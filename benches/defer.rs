#![feature(test)]

extern crate test;

use test::Bencher;

use crossbeam_utils::thread::scope;

#[bench]
fn single_alloc_defer_free(b: &mut Bencher) {
    let mut ebr: ebr::Ebr<Box<u64>> = ebr::Ebr::default();

    b.iter(|| {
        let mut guard = ebr.pin();
        guard.defer_drop(Box::new(1));
    });
}

#[bench]
fn single_defer(b: &mut Bencher) {
    let mut ebr: ebr::Ebr<()> = ebr::Ebr::default();

    b.iter(|| {
        let mut guard = ebr.pin();
        guard.defer_drop(());
    });
}

#[bench]
fn multi_alloc_defer_free(b: &mut Bencher) {
    const THREADS: usize = 16;
    const STEPS: usize = 10_000;

    b.iter(|| {
        let ebr: ebr::Ebr<Box<u64>> = ebr::Ebr::default();

        scope(|s| {
            for _ in 0..THREADS {
                let mut ebr = ebr.clone();
                s.spawn(move |_| {
                    for _ in 0..STEPS {
                        let mut guard = ebr.pin();
                        guard.defer_drop(Box::new(1));
                    }
                });
            }
        })
        .unwrap();
    });
}

#[bench]
fn multi_defer(b: &mut Bencher) {
    const THREADS: usize = 16;
    const STEPS: usize = 10_000;

    b.iter(|| {
        let ebr: ebr::Ebr<()> = ebr::Ebr::default();

        scope(|s| {
            for _ in 0..THREADS {
                let mut ebr = ebr.clone();
                s.spawn(move |_| {
                    for _ in 0..STEPS {
                        let mut guard = ebr.pin();
                        guard.defer_drop(());
                    }
                });
            }
        })
        .unwrap();
    });
}
