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
        atomic::{
            fence, AtomicU64,
            Ordering::{Acquire, Relaxed, Release},
        },
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
};

const BUMP_EPOCH_OPS: u64 = 128;
const BUMP_EPOCH_TRAILING_ZEROS: u32 = BUMP_EPOCH_OPS.trailing_zeros();
const INACTIVE_BIT: u64 = 0b1;
const INACTIVE_MASK: u64 = u64::MAX - INACTIVE_BIT;

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
            assert_eq!(inner.pinned_epoch, None);
            debug_delay();
            let pinned_epoch = inner.epoch_tracker.pin();
            inner.pinned_epoch = Some(pinned_epoch);
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

const fn epoch_pin(current: u64) -> u64 {
    let shift = match current & 0b11 {
        0b00 => 2,
        0b01 => 2 + 20,
        0b10 => 2 + 40,
        _ => unreachable!(),
    };

    current + (1 << shift)
}

const fn epoch_unpin(current: u64, epoch: Epoch) -> u64 {
    let shift = match epoch {
        Epoch::E0 => 2,
        Epoch::E1 => 2 + 20,
        Epoch::E2 => 2 + 40,
        _ => unreachable!(),
    };

    current - (1 << shift)
}

const fn current_tenancy(current: u64) -> usize {
    let shift = match current & 0b11 {
        0b00 => 2,
        0b01 => 2 + 20,
        0b10 => 2 + 40,
        _ => unreachable!(),
    };

    (current >> shift) as usize & 0xFFFFF
}

const fn advance_epoch(current: u64) -> u64 {
    let advanced_low_bits = match current & 0b11 {
        0b00 => 0b01,
        0b01 => 0b10,
        0b10 => 0b00,
        _ => unreachable!(),
    };

    const EPOCH_MASK: u64 = u64::MAX - 0b11;

    (current & EPOCH_MASK) + advanced_low_bits
}

const fn previous_tenancy(current: u64) -> usize {
    let shift = match current & 0b11 {
        0b00 => 2 + 40,
        0b01 => 2,
        0b10 => 2 + 20,
        _ => unreachable!(),
    };

    (current >> shift) as usize & 0xFFFFF
}

const fn inactive_tenancy(current: u64) -> usize {
    let shift = match current & 0b11 {
        0b00 => 2 + 20,
        0b01 => 2 + 40,
        0b10 => 2,
        _ => unreachable!(),
    };

    (current >> shift) as usize & 0xFFFFF
}

const fn epoch_to_enum(current: u64) -> Epoch {
    match current & 0b11 {
        0b00 => Epoch::E0,
        0b01 => Epoch::E1,
        0b10 => Epoch::E2,
        _ => unreachable!(),
    }
}

#[derive(Debug, Default, Clone)]
struct EpochTracker(Arc<AtomicU64>);

impl EpochTracker {
    // returns the epoch that we are pinned into, must
    // be passed to unpin later on
    fn pin(&self) -> Epoch {
        let mut current = self.0.load(Relaxed);

        loop {
            let pinned = epoch_pin(current);
            match self
                .0
                .compare_exchange_weak(current, pinned, Release, Acquire)
            {
                Ok(_) => return epoch_to_enum(pinned),
                Err(c) => current = c,
            }
        }
    }

    fn unpin(&self, epoch: Epoch) {
        let mut current = self.0.load(Relaxed);

        loop {
            let unpinned = epoch_unpin(current, epoch);
            match self
                .0
                .compare_exchange_weak(current, unpinned, Release, Acquire)
            {
                Ok(_) => return,
                Err(c) => current = c,
            }
        }
    }
}

#[derive(Debug)]
pub struct Inner<T: Send + 'static, const SLOTS: usize> {
    // shared state for doing epoch maintenance
    epoch_tracker: EpochTracker,

    // incremented after successfully advancing the epoch,
    epoch_counter: Arc<AtomicU64>,

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

    // the epoch we're checked into
    pinned_epoch: Option<Epoch>,
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
            full_bag.seal(self.epoch_counter.load(Acquire));
            self.orphan_tx.send(full_bag).unwrap();
        }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Default for Inner<T, SLOTS> {
    fn default() -> Inner<T, SLOTS> {
        let (orphan_tx, orphan_rx) = channel();

        Inner {
            epoch_tracker: EpochTracker::default(),
            epoch_counter: Arc::new(AtomicU64::new(2)),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: Arc::new(Mutex::new(orphan_rx)),
            orphan_tx,
            pins: 0,
            local_concurrent_pins: 0,
            pinned_epoch: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
enum Epoch {
    E0 = 0,
    E1 = 1,
    E2 = 2,
}

impl Epoch {
    fn successor(&self) -> Epoch {
        use Epoch::*;
        match self {
            E0 => E1,
            E1 => E2,
            E2 => E0,
        }
    }

    fn predecessor(&self) -> Epoch {
        use Epoch::*;
        match self {
            E0 => E2,
            E1 => E0,
            E2 => E1,
        }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Clone for Inner<T, SLOTS> {
    fn clone(&self) -> Inner<T, SLOTS> {
        Inner {
            epoch_tracker: self.epoch_tracker.clone(),
            epoch_counter: self.epoch_counter.clone(),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: self.maintenance_lock.clone(),
            orphan_tx: self.orphan_tx.clone(),
            pins: 0,
            local_concurrent_pins: 0,
            pinned_epoch: None,
        }
    }
}

impl<T: Send + 'static, const SLOTS: usize> Inner<T, SLOTS> {
    #[cold]
    fn maintenance(&mut self) {
        debug_delay();
        let orphan_rx = if let Ok(orphan_rx) = self.maintenance_lock.try_lock() {
            orphan_rx
        } else {
            return;
        };

        // we have now been "elected" global maintainer,
        // which has responsibility for:
        // * bumping the global quiescent epoch
        // * clearing the orphan garbage queue

        let mut current = self.epoch_tracker.0.load(Relaxed);

        let advanced = loop {
            if previous_tenancy(current) > 0 {
                break false;
            }

            let bumped_epoch = advance_epoch(current);
            match self.epoch_tracker.0.compare_exchange_weak(
                current,
                bumped_epoch,
                Release,
                Relaxed,
            ) {
                Ok(_) => break true,
                Err(c) => current = c,
            }
        };

        if advanced {
            debug_delay();
            self.epoch_counter.fetch_add(1, Release);
        }

        debug_delay();

        fence(Release);

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
            let pinned_epoch = inner.pinned_epoch.take().unwrap();
            inner.epoch_tracker.unpin(pinned_epoch);
            let safe_to_gc = inner.epoch_counter.load(Acquire).checked_sub(2).unwrap();

            while let Some(front) = inner.garbage_queue.front() {
                if front.final_epoch.unwrap().get() > safe_to_gc {
                    break;
                }

                let bag = inner.garbage_queue.pop_front().unwrap();
                drop(bag);
            }
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
            let global_current_epoch = ebr.epoch_counter.load(Acquire);
            full_bag.seal(global_current_epoch);

            ebr.garbage_queue.push_back(full_bag);

            debug_delay();
            let quiescent = global_current_epoch.checked_sub(2).unwrap();

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
