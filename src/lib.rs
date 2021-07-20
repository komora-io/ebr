//! Epoch-based reclamation.

use std::{
    collections::{BTreeMap, VecDeque},
    mem::{take, MaybeUninit},
    num::NonZeroU64,
    sync::RwLock,
    sync::{
        atomic::{
            fence, AtomicU64,
            Ordering::{Acquire, Relaxed, Release},
        },
        mpsc::{channel, Receiver, Sender},
        Arc, Mutex,
    },
};

const BUMP_EPOCH_OPS: u64 = 1024;
const BUMP_EPOCH_TRAILING_ZEROS: u32 = BUMP_EPOCH_OPS.trailing_zeros();
const GARBAGE_SLOTS_PER_BAG: usize = 128;

pub struct Guard<'a, T: Send + 'static> {
    ebr: &'a mut Ebr<T>,
}

impl<'a, T: Send + 'static> Drop for Guard<'a, T> {
    fn drop(&mut self) {
        self.ebr.local_quiescent_epoch.store(u64::MAX, Release);
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

/// A registry for tracking tenancy in various epochs.
#[derive(Debug, Clone)]
pub struct Registry {
    tenants: Arc<RwLock<BTreeMap<u64, Arc<AtomicU64>>>>,
}

impl Default for Registry {
    fn default() -> Registry {
        Registry {
            tenants: Default::default(),
        }
    }
}

impl Registry {
    fn register(&self, id: u64, quiescent_epoch: u64) -> Arc<AtomicU64> {
        let epoch_tracker = Arc::new(AtomicU64::new(quiescent_epoch));
        self.tenants
            .write()
            .unwrap()
            .insert(id, epoch_tracker.clone());
        epoch_tracker
    }

    fn deregister(&self, id: u64) {
        self.tenants
            .write()
            .unwrap()
            .remove(&id)
            .expect("unknown id deregistered from Ebr");
    }

    fn min_epoch(&self) -> u64 {
        let min = self
            .tenants
            .read()
            .unwrap()
            .values()
            .map(|v| v.load(Relaxed))
            .min()
            .unwrap();

        fence(Release);

        min
    }
}

#[derive(Debug)]
pub struct Ebr<T: Send + 'static> {
    // total collector count
    collectors: Arc<AtomicU64>,
    // the unique ID for this Ebr handle
    local_id: u64,
    // the quiescent epoch for this Ebr handle
    local_quiescent_epoch: Arc<AtomicU64>,
    registry: Registry,
    // the global minimum quiescent epoch across
    // all registered Ebr handles
    global_quiescent_epoch: Arc<AtomicU64>,
    global_current_epoch: Arc<AtomicU64>,
    garbage_queue: VecDeque<Bag<T>>,
    current_garbage_bag: Bag<T>,
    maintenance_lock: Arc<Mutex<Receiver<Bag<T>>>>,
    orphan_sender: Sender<Bag<T>>,
    pins: u64,
}

impl<T: Send + 'static> Default for Ebr<T> {
    fn default() -> Ebr<T> {
        let registry = Registry::default();
        let collectors = Arc::new(AtomicU64::new(0));
        let local_id = collectors.fetch_add(1, Relaxed);
        let current_epoch = 1;
        let quiescent_epoch = current_epoch - 1;
        let local_quiescent_epoch = registry.register(local_id, quiescent_epoch);

        let (tx, rx) = channel();

        Ebr {
            collectors,
            registry,
            local_id,
            local_quiescent_epoch,
            global_current_epoch: Arc::new(AtomicU64::new(current_epoch)),
            global_quiescent_epoch: Arc::new(AtomicU64::new(quiescent_epoch)),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: Arc::new(Mutex::new(rx)),
            orphan_sender: tx,
            pins: 0,
        }
    }
}

impl<T: Send + 'static> Ebr<T> {
    pub fn pin(&mut self) -> Guard<'_, T> {
        self.pins += 1;

        let global_current_epoch = self.global_current_epoch.load(Relaxed);
        self.local_quiescent_epoch
            .store(global_current_epoch, Release);

        let should_bump_epoch = self.pins.trailing_zeros() == BUMP_EPOCH_TRAILING_ZEROS;
        if should_bump_epoch {
            self.maintenance();
        }

        Guard { ebr: self }
    }

    #[cold]
    fn maintenance(&mut self) {
        self.global_current_epoch.fetch_add(1, Relaxed);

        let orphans_rx = if let Ok(orphans_rx) = self.maintenance_lock.try_lock() {
            orphans_rx
        } else {
            return;
        };

        // we have now been "elected" global maintainer,
        // which has responsibility for:
        // * incrementing the global epoch
        // * bumping the global quiescent epoch
        // * clearing the orphan garbage queue

        let global_quiescent_epoch = self.registry.min_epoch();

        assert_ne!(global_quiescent_epoch, u64::MAX);

        self.global_quiescent_epoch
            .fetch_max(global_quiescent_epoch, Release);

        while let Ok(bag) = orphans_rx.try_recv() {
            if bag.final_epoch.unwrap().get() < global_quiescent_epoch {
                drop(bag)
            } else {
                self.garbage_queue.push_back(bag);
            }
        }
    }
}

impl<T: Send + 'static> Clone for Ebr<T> {
    fn clone(&self) -> Ebr<T> {
        let local_id = self.collectors.fetch_add(1, Relaxed);
        let global_current_epoch = self.global_current_epoch.load(Acquire);
        let local_quiescent_epoch = self.registry.register(local_id, global_current_epoch - 1);

        Ebr {
            collectors: self.collectors.clone(),
            registry: self.registry.clone(),
            local_id,
            local_quiescent_epoch,
            global_quiescent_epoch: self.global_quiescent_epoch.clone(),
            global_current_epoch: self.global_current_epoch.clone(),
            garbage_queue: Default::default(),
            current_garbage_bag: Bag::default(),
            maintenance_lock: self.maintenance_lock.clone(),
            orphan_sender: self.orphan_sender.clone(),
            pins: 0,
        }
    }
}

impl<T: Send + 'static> Drop for Ebr<T> {
    fn drop(&mut self) {
        // Send all outstanding garbage to the orphan queue.
        // This is safe to do even if we're the only
        // instance, because everything in the queue will
        // be dropped at the end of this call to drop anyway.
        for old_bag in take(&mut self.garbage_queue) {
            self.orphan_sender.send(old_bag).unwrap();
        }

        if self.current_garbage_bag.len > 0 {
            let mut full_bag = take(&mut self.current_garbage_bag);
            full_bag.seal(self.global_current_epoch.load(Acquire));
            self.orphan_sender.send(full_bag).unwrap();
        }

        self.registry.deregister(self.local_id);
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

fn uninit_array<T, const LEN: usize>() -> [MaybeUninit<T>; LEN] {
    unsafe { MaybeUninit::<[MaybeUninit<T>; LEN]>::uninit().assume_init() }
}

impl<T> Bag<T> {
    fn push(&mut self, item: T) {
        assert!(self.len < GARBAGE_SLOTS_PER_BAG);
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
            garbage: uninit_array::<T, GARBAGE_SLOTS_PER_BAG>(),
        }
    }
}
