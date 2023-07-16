use std::alloc::{Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::scope;
use std::time::Instant;

const THREADS: usize = 8;
const STEPS: usize = 100_000_000;

#[global_allocator]
static ALLOCATOR: Alloc = Alloc;

static ALLOCATED: AtomicUsize = AtomicUsize::new(0);
static FREED: AtomicUsize = AtomicUsize::new(0);
static RESIDENT: AtomicUsize = AtomicUsize::new(0);

fn allocated() -> usize {
    ALLOCATED.swap(0, Ordering::Relaxed)
}

fn freed() -> usize {
    FREED.swap(0, Ordering::Relaxed)
}

fn resident() -> usize {
    RESIDENT.load(Ordering::Relaxed)
}

#[derive(Default, Debug, Clone, Copy)]
struct Alloc;

unsafe impl std::alloc::GlobalAlloc for Alloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ret = System.alloc(layout);
        assert_ne!(ret, std::ptr::null_mut());
        ALLOCATED.fetch_add(layout.size(), Ordering::Relaxed);
        RESIDENT.fetch_add(layout.size(), Ordering::Relaxed);
        std::ptr::write_bytes(ret, 0xa1, layout.size());
        ret
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        std::ptr::write_bytes(ptr, 0xde, layout.size());
        FREED.fetch_add(layout.size(), Ordering::Relaxed);
        RESIDENT.fetch_sub(layout.size(), Ordering::Relaxed);
        System.dealloc(ptr, layout)
    }
}

fn main() {
    let before = Instant::now();

    let ebr: ebr::Ebr<u32> = ebr::Ebr::default();

    scope(|s| {
        for _ in 0..THREADS {
            let ebr = ebr.clone();
            s.spawn(move || {
                for _ in 0..STEPS {
                    let mut guard = ebr.pin();
                    guard.defer_drop(77);
                }
            });
        }
    });

    dbg!(before.elapsed());

    dbg!(allocated());
    dbg!(freed());
    dbg!(resident());

    let mps = STEPS as u128 * THREADS as u128 / before.elapsed().as_micros();
    println!("{} million pins + defer drops per second", mps);

    println!("dropping final EBR");
    drop(ebr);

    dbg!(allocated());
    dbg!(freed());
    dbg!(resident());
}
