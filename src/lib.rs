//! Simple, CPU cache-friendly epoch-based reclamation (EBR).
//!
//! ```rust
//! use ebr::Ebr;
//!
//! let ebr: Ebr<Box<u64>> = Ebr::default();
//!
//! let mut guard = ebr.pin();
//!
//! guard.defer_drop(Box::new(1));
//! ```

use std::{
    cell::RefCell,
    collections::VecDeque,
    mem::{take, MaybeUninit},
    num::NonZeroU64,
    sync::{
        atomic::{fence, AtomicU64, Ordering},
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
};

use shared_local_state::SharedLocalState;

const BUMP_EPOCH_OPS: u64 = 128;
const BUMP_EPOCH_TRAILING_ZEROS: u32 = BUMP_EPOCH_OPS.trailing_zeros();
const INACTIVE_BIT: u64 = 0b1;

#[cfg(not(feature = "lock_free_delays"))]
#[inline]
fn debug_delay() {}

#[cfg(feature = "lock_free_delays")]
fn debug_delay() {
    for i in 0..2 {
        std::thread::yield_now();
    }
}

fn _test_impls() {
    fn send<T: Send>() {}

    send::<Ebr<()>>();
    send::<Inner<(), 128>>();
}

/// Epoch-based garbage collector with extremely efficient
/// single-threaded operations in the hot-path.
///
/// The `SLOTS` const generic specifies how large the local
/// garbage bags should be allowed to grow before marking
/// them with the current monotonic timestamp and placing
/// them into a local queue for destruction.
///
/// If an `Ebr` is dropped while it has un-reclaimed garbage,
/// it sends its current garbage bag to another thread
/// to cooperatively reclaim after any potentially witnessing
/// thread has finished its work.
#[derive(Debug)]
pub struct Ebr<T: Send + 'static, const SLOTS: usize = 128> {
    inner: RefCell<Inner<T, SLOTS>>,
}

impl<T: Send + 'static, const SLOTS: usize> Ebr<T, SLOTS> {
    pub fn pin(&self) -> Guard<'_, T, SLOTS> {
        let mut inner = self.inner.borrow_mut();
        inner.pins += 1;
        inner.local_concurrent_pins += 1;

        if inner.local_concurrent_pins == 1 {
            let mut global_current_epoch = inner.global_current_epoch.load(Ordering::Acquire);
            loop {
                debug_delay();
                inner
                    .local_progress_registry
                    .access_without_notification(|lqe| {
                        lqe.store(global_current_epoch, Ordering::Release);
                    });

                // here we optimistically linearize w/ the global_current_epoch
                // by doing a second read and repeating the store until what we
                // stored equals the current epoch after storage.
                let second_read = inner.global_current_epoch.load(Ordering::Acquire);

                if second_read == global_current_epoch {
                    break;
                } else {
                    global_current_epoch = second_read;
                }
            }
        }

        let should_bump_epoch = inner.pins.trailing_zeros() >= BUMP_EPOCH_TRAILING_ZEROS;
        if should_bump_epoch {
            inner.maintenance();
        }

        Guard { ebr: &self.inner }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Clone for Ebr<T, SLOTS> {
    fn clone(&self) -> Self {
        Ebr {
            inner: RefCell::new(self.inner.borrow().clone()),
        }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Default for Ebr<T, SLOTS> {
    fn default() -> Ebr<T, SLOTS> {
        Ebr {
            inner: Default::default(),
        }
    }
}

#[derive(Debug)]
pub struct Inner<T: Send + 'static, const SLOTS: usize> {
    // shared quiescent epochs
    local_progress_registry: SharedLocalState<AtomicU64>,

    // the highest epoch that gc is safe for
    global_minimum_epoch: Arc<AtomicU64>,

    // new garbage gets assigned this epoch
    global_current_epoch: Arc<AtomicU64>,

    // epoch-tagged garbage waiting to be safely dropped
    garbage_queue: VecDeque<Bag<T, SLOTS>>,

    // new garbage accumulates here first
    current_garbage_bag: Bag<T, SLOTS>,

    // receives garbage from terminated threads
    maintenance_lock: Arc<Mutex<Receiver<Bag<T, SLOTS>>>>,

    // send outstanding garbage here when this Ebr drops
    orphan_tx: Sender<Bag<T, SLOTS>>,

    // count of pin attempts from this collector
    pins: u64,

    // pins on this thread
    local_concurrent_pins: usize,
}

impl<T: Send + 'static, const SLOTS: usize> Drop for Inner<T, SLOTS> {
    fn drop(&mut self) {
        // Send all outstanding garbage to the orphan queue.
        for old_bag in take(&mut self.garbage_queue) {
            self.orphan_tx.send(old_bag).unwrap();
        }

        if self.current_garbage_bag.len > 0 {
            let mut full_bag = take(&mut self.current_garbage_bag);
            debug_delay();
            let mut global_current_epoch = self.global_current_epoch.load(Ordering::Acquire);

            loop {
                // We use optimistic double-reading here to ensure our seal is linearized
                // with the current epoch.
                full_bag.seal(global_current_epoch);

                debug_delay();

                let second_read = self.global_current_epoch.load(Ordering::Acquire);

                if second_read == global_current_epoch {
                    break;
                } else {
                    global_current_epoch = second_read;
                }
            }
            self.orphan_tx.send(full_bag).unwrap();
        }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Default for Inner<T, SLOTS> {
    fn default() -> Inner<T, SLOTS> {
        let current_epoch = 2;

        let local_epoch = AtomicU64::new(3);
        let local_progress_registry = SharedLocalState::new(local_epoch);

        let (orphan_tx, orphan_rx) = channel();

        Inner {
            local_progress_registry,
            global_current_epoch: Arc::new(AtomicU64::new(current_epoch)),
            global_minimum_epoch: Arc::new(AtomicU64::new(current_epoch)),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: Arc::new(Mutex::new(orphan_rx)),
            orphan_tx,
            pins: 0,
            local_concurrent_pins: 0,
        }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Clone for Inner<T, SLOTS> {
    fn clone(&self) -> Inner<T, SLOTS> {
        let local_quiescent_epoch =
            AtomicU64::new(self.global_current_epoch.load(Ordering::Acquire) + 1);

        let local_progress_registry = self.local_progress_registry.insert(local_quiescent_epoch);

        Inner {
            local_progress_registry,
            global_minimum_epoch: self.global_minimum_epoch.clone(),
            global_current_epoch: self.global_current_epoch.clone(),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: self.maintenance_lock.clone(),
            orphan_tx: self.orphan_tx.clone(),
            pins: 0,
            local_concurrent_pins: 0,
        }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Inner<T, SLOTS> {
    #[cold]
    fn maintenance(&mut self) {
        let orphan_rx = if let Ok(orphan_rx) = self.maintenance_lock.try_lock() {
            orphan_rx
        } else {
            return;
        };

        debug_delay();
        // we bump by 2 because 1 represents the inactive bit.
        let last_epoch = self.global_current_epoch.fetch_add(2, Ordering::Release);

        // we have now been "elected" global maintainer,
        // which has responsibility for:
        // * bumping the global quiescent epoch
        // * clearing the orphan garbage queue

        debug_delay();

        let minimum_fold_closure = |min: u64, this: &AtomicU64| {
            let value = this.load(Ordering::Acquire);
            let is_inactive = value & INACTIVE_BIT == INACTIVE_BIT;

            // NB we can just ignore inactives
            if is_inactive {
                min
            } else {
                min.min(value)
            }
        };

        let global_minimum_epoch = self
            .local_progress_registry
            .fold(last_epoch, minimum_fold_closure);

        fence(Ordering::Release);

        assert_ne!(global_minimum_epoch, u64::MAX);

        debug_delay();
        self.global_minimum_epoch
            .fetch_max(global_minimum_epoch, Ordering::Release);

        while let Ok(bag) = orphan_rx.try_recv() {
            self.garbage_queue.push_back(bag);
        }
    }
}

pub struct Guard<'a, T: Send + 'static, const SLOTS: usize> {
    ebr: &'a RefCell<Inner<T, SLOTS>>,
}

impl<'a, T: Send + 'static, const SLOTS: usize> Drop for Guard<'a, T, SLOTS> {
    fn drop(&mut self) {
        // set this to a large number to ensure it is not counted by `min_epoch()`
        let mut inner = self.ebr.borrow_mut();

        debug_delay();

        inner.local_concurrent_pins = inner.local_concurrent_pins.checked_sub(1).unwrap();

        if inner.local_concurrent_pins == 0 {
            debug_delay();
            // adding 1 sets the INACTIVE_BIT
            inner
                .local_progress_registry
                .access_without_notification(|lqe| lqe.fetch_add(1, Ordering::Release));
        }
    }
}

impl<'a, T: Send + 'static, const SLOTS: usize> Guard<'a, T, SLOTS> {
    pub fn defer_drop(&mut self, item: T) {
        let mut ebr = self.ebr.borrow_mut();
        ebr.current_garbage_bag.push(item);

        if ebr.current_garbage_bag.is_full() {
            let mut full_bag = take(&mut ebr.current_garbage_bag);

            debug_delay();
            let mut global_current_epoch = ebr.global_current_epoch.load(Ordering::Acquire);

            loop {
                // We use optimistic double-reading here to ensure our seal is linearized
                // with the current epoch.
                full_bag.seal(global_current_epoch);

                debug_delay();

                let second_read = ebr.global_current_epoch.load(Ordering::Acquire);

                if second_read == global_current_epoch {
                    break;
                } else {
                    global_current_epoch = second_read;
                }
            }

            ebr.garbage_queue.push_back(full_bag);

            debug_delay();
            let global_minimum_epoch = ebr.global_minimum_epoch.load(Ordering::Acquire);
            let quiescent = global_minimum_epoch.checked_sub(2).unwrap();

            assert!(
                global_current_epoch > quiescent,
                "expected global_current_epoch \
                {} to be > quiescent {}",
                global_current_epoch,
                quiescent
            );

            while let Some(front) = ebr.garbage_queue.front() {
                if front.final_epoch.unwrap().get() > quiescent {
                    break;
                }

                let bag = ebr.garbage_queue.pop_front().unwrap();
                drop(bag);
            }
        }
    }
}

#[derive(Debug)]
struct Bag<T, const SLOTS: usize> {
    garbage: [MaybeUninit<T>; SLOTS],
    final_epoch: Option<NonZeroU64>,
    len: usize,
}

impl<T, const SLOTS: usize> Drop for Bag<T, SLOTS> {
    fn drop(&mut self) {
        for index in 0..self.len {
            unsafe {
                self.garbage[index].as_mut_ptr().drop_in_place();
            }
        }
    }
}

impl<T, const SLOTS: usize> Bag<T, SLOTS> {
    fn push(&mut self, item: T) {
        debug_assert!(self.len < SLOTS);
        unsafe {
            self.garbage[self.len].as_mut_ptr().write(item);
        }
        self.len += 1;
    }

    const fn is_full(&self) -> bool {
        self.len == SLOTS
    }

    fn seal(&mut self, epoch: u64) {
        self.final_epoch = Some(NonZeroU64::new(epoch).unwrap());
    }
}

impl<T: Send + 'static, const SLOTS: usize> Default for Bag<T, SLOTS> {
    fn default() -> Bag<T, SLOTS> {
        Bag {
            final_epoch: None,
            len: 0,
            garbage: unsafe { MaybeUninit::<[MaybeUninit<T>; SLOTS]>::uninit().assume_init() },
        }
    }
}
