#![feature(test)]

extern crate test;

use std::thread::scope;

use test::Bencher;

use crossbeam_epoch::{pin as crossbeam_pin, Owned};

const THREADS: usize = 16;
const STEPS: usize = 10_000;

#[bench]
fn ebr_single_alloc_defer_free(b: &mut Bencher) {
    let ebr: ebr::Ebr<Box<u64>> = ebr::Ebr::default();

    b.iter(|| {
        let mut guard = ebr.pin();
        guard.defer_drop(Box::new(1));
    });
}

#[bench]
fn ebr_single_defer(b: &mut Bencher) {
    let ebr: ebr::Ebr<()> = ebr::Ebr::default();

    b.iter(|| {
        let mut guard = ebr.pin();
        guard.defer_drop(());
    });
}

#[bench]
fn ebr_multi_alloc_defer_free(b: &mut Bencher) {
    b.iter(|| {
        scope(|s| {
            let ebr: ebr::Ebr<Box<u64>> = ebr::Ebr::default();

            for _ in 0..THREADS {
                let ebr = ebr.clone();
                s.spawn(move || {
                    for _ in 0..STEPS {
                        let mut guard = ebr.pin();
                        guard.defer_drop(Box::new(1));
                    }
                });
            }
        })
    });
}

#[bench]
fn ebr_multi_defer(b: &mut Bencher) {
    b.iter(|| {
        let ebr: ebr::Ebr<()> = ebr::Ebr::default();

        scope(|s| {
            for _ in 0..THREADS {
                let ebr = ebr.clone();
                s.spawn(move || {
                    for _ in 0..STEPS {
                        let mut guard = ebr.pin();
                        guard.defer_drop(());
                    }
                });
            }
        })
    });
}

#[bench]
fn crossbeam_single_alloc_defer_free(b: &mut Bencher) {
    b.iter(|| {
        let guard = crossbeam_pin();
        unsafe {
            guard.defer_destroy(Owned::new(1).into_shared(&guard));
        }
    });
}

#[bench]
fn crossbeam_single_defer(b: &mut Bencher) {
    b.iter(|| {
        let guard = crossbeam_pin();
        unsafe {
            guard.defer_destroy(Owned::new(()).into_shared(&guard));
        }
    });
}

#[bench]
fn crossbeam_multi_alloc_defer_free(b: &mut Bencher) {
    b.iter(|| {
        scope(|s| {
            for _ in 0..THREADS {
                s.spawn(move || {
                    for _ in 0..STEPS {
                        let guard = crossbeam_pin();
                        unsafe {
                            guard.defer_destroy(Owned::new(1).into_shared(&guard));
                        }
                    }
                });
            }
        })
    });
}

#[bench]
fn crossbeam_multi_defer(b: &mut Bencher) {
    b.iter(|| {
        scope(|s| {
            for _ in 0..THREADS {
                s.spawn(move || {
                    for _ in 0..STEPS {
                        let guard = crossbeam_pin();
                        unsafe {
                            guard.defer_destroy(Owned::new(()).into_shared(&guard));
                        }
                    }
                });
            }
        })
    });
}
