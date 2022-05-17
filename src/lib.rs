//! Simple, CPU cache-friendly epoch-based reclamation (EBR).
//!
//! ```rust
//! use ebr::Ebr;
//!
//! let mut ebr: Ebr<Box<u64>> = Ebr::default();
//!
//! let mut guard = ebr.pin();
//!
//! guard.defer_drop(Box::new(1));
//! ```

use std::{
    collections::VecDeque,
    mem::{take, MaybeUninit},
    num::NonZeroU64,
    sync::{
        atomic::{
            fence, AtomicU64,
            Ordering::{Acquire, Release},
        },
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
};

use shared_local_state::SharedLocalState;

const BUMP_EPOCH_OPS: u64 = 1024;
const BUMP_EPOCH_TRAILING_ZEROS: u32 = BUMP_EPOCH_OPS.trailing_zeros();
const GARBAGE_SLOTS_PER_BAG: usize = 128;

#[derive(Debug)]
pub struct Ebr<T: Send + 'static> {
    // shared quiescent epochs
    local_progress_registry: SharedLocalState<AtomicU64>,

    // the highest epoch that gc is safe for
    global_quiescent_epoch: Arc<AtomicU64>,

    // new garbage gets assigned this epoch
    global_current_epoch: Arc<AtomicU64>,

    // epoch-tagged garbage waiting to be safely dropped
    garbage_queue: VecDeque<Bag<T>>,

    // new garbage accumulates here first
    current_garbage_bag: Bag<T>,

    // receives garbage from terminated threads
    maintenance_lock: Arc<Mutex<Receiver<Bag<T>>>>,

    // send outstanding garbage here when this Ebr drops
    orphan_tx: Sender<Bag<T>>,

    // count of pin attempts from this collector
    pins: u64,
}

impl<T: Send + 'static> Drop for Ebr<T> {
    fn drop(&mut self) {
        // Send all outstanding garbage to the orphan queue.
        for old_bag in take(&mut self.garbage_queue) {
            self.orphan_tx.send(old_bag).unwrap();
        }

        if self.current_garbage_bag.len > 0 {
            let mut full_bag = take(&mut self.current_garbage_bag);
            full_bag.seal(self.global_current_epoch.load(Acquire));
            self.orphan_tx.send(full_bag).unwrap();
        }
    }
}

impl<T: Send + 'static> Default for Ebr<T> {
    fn default() -> Ebr<T> {
        let current_epoch = 1;
        let quiescent_epoch = current_epoch - 1;

        let local_quiescent_epoch = AtomicU64::new(quiescent_epoch);
        let local_progress_registry = SharedLocalState::new(local_quiescent_epoch);

        let (orphan_tx, orphan_rx) = channel();

        Ebr {
            local_progress_registry,
            global_current_epoch: Arc::new(AtomicU64::new(current_epoch)),
            global_quiescent_epoch: Arc::new(AtomicU64::new(quiescent_epoch)),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: Arc::new(Mutex::new(orphan_rx)),
            orphan_tx,
            pins: 0,
        }
    }
}

impl<T: Send + 'static> Clone for Ebr<T> {
    fn clone(&self) -> Ebr<T> {
        let global_current_epoch = self.global_current_epoch.load(Acquire);

        let local_quiescent_epoch = AtomicU64::new(global_current_epoch);

        let local_progress_registry = self.local_progress_registry.insert(local_quiescent_epoch);

        Ebr {
            local_progress_registry,
            global_quiescent_epoch: self.global_quiescent_epoch.clone(),
            global_current_epoch: self.global_current_epoch.clone(),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: self.maintenance_lock.clone(),
            orphan_tx: self.orphan_tx.clone(),
            pins: 0,
        }
    }
}

impl<T: Send + 'static> Ebr<T> {
    pub fn pin(&mut self) -> Guard<'_, T> {
        self.pins += 1;

        let global_current_epoch = self.global_current_epoch.load(Acquire);
        self.local_progress_registry
            .access_without_notification(|lqe| lqe.store(global_current_epoch, Release));

        let should_bump_epoch = self.pins.trailing_zeros() == BUMP_EPOCH_TRAILING_ZEROS;
        if should_bump_epoch {
            self.maintenance();
        }

        Guard { ebr: self }
    }

    #[cold]
    fn maintenance(&mut self) {
        self.global_current_epoch.fetch_add(1, Release);

        let orphan_rx = if let Ok(orphan_rx) = self.maintenance_lock.try_lock() {
            orphan_rx
        } else {
            return;
        };

        // we have now been "elected" global maintainer,
        // which has responsibility for:
        // * bumping the global quiescent epoch
        // * clearing the orphan garbage queue

        let global_quiescent_epoch = self
            .local_progress_registry
            .fold(0, |min, this| min.min(this.load(Acquire)));

        fence(Release);

        assert_ne!(global_quiescent_epoch, u64::MAX);

        self.global_quiescent_epoch
            .fetch_max(global_quiescent_epoch, Release);

        while let Ok(bag) = orphan_rx.try_recv() {
            if bag.final_epoch.unwrap().get() < global_quiescent_epoch {
                drop(bag)
            } else {
                self.garbage_queue.push_back(bag);
            }
        }
    }
}

pub struct Guard<'a, T: Send + 'static> {
    ebr: &'a mut Ebr<T>,
}

impl<'a, T: Send + 'static> Drop for Guard<'a, T> {
    fn drop(&mut self) {
        // set this to a large number to ensure it is not counted by `min_epoch()`
        self.ebr
            .local_progress_registry
            .access_without_notification(|lqe| lqe.store(u64::MAX, Release));
    }
}

impl<'a, T: Send + 'static> Guard<'a, T> {
    pub fn defer_drop(&mut self, item: T) {
        self.ebr.current_garbage_bag.push(item);

        if self.ebr.current_garbage_bag.is_full() {
            let mut full_bag = take(&mut self.ebr.current_garbage_bag);
            let global_current_epoch = self.ebr.global_current_epoch.load(Acquire);
            full_bag.seal(global_current_epoch);
            self.ebr.garbage_queue.push_back(full_bag);

            let quiescent = self.ebr.global_quiescent_epoch.load(Acquire);

            assert!(global_current_epoch > quiescent);

            while self
                .ebr
                .garbage_queue
                .front()
                .unwrap()
                .final_epoch
                .unwrap()
                .get()
                < quiescent
            {
                let bag = self.ebr.garbage_queue.pop_front().unwrap();
                drop(bag);
            }
        }
    }
}

#[derive(Debug)]
struct Bag<T> {
    garbage: [MaybeUninit<T>; GARBAGE_SLOTS_PER_BAG],
    final_epoch: Option<NonZeroU64>,
    len: usize,
}

impl<T> Drop for Bag<T> {
    fn drop(&mut self) {
        for index in 0..self.len {
            unsafe {
                self.garbage[index].as_mut_ptr().drop_in_place();
            }
        }
    }
}

impl<T> Bag<T> {
    fn push(&mut self, item: T) {
        debug_assert!(self.len < GARBAGE_SLOTS_PER_BAG);
        unsafe {
            self.garbage[self.len].as_mut_ptr().write(item);
        }
        self.len += 1;
    }

    const fn is_full(&self) -> bool {
        self.len == GARBAGE_SLOTS_PER_BAG
    }

    fn seal(&mut self, epoch: u64) {
        self.final_epoch = Some(NonZeroU64::new(epoch).unwrap());
    }
}

impl<T: Send + 'static> Default for Bag<T> {
    fn default() -> Bag<T> {
        Bag {
            final_epoch: None,
            len: 0,
            garbage: unsafe {
                MaybeUninit::<[MaybeUninit<T>; GARBAGE_SLOTS_PER_BAG]>::uninit().assume_init()
            },
        }
    }
}
