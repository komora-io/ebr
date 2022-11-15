# ebr

a simple epoch-based reclamation (EBR) library with low cacheline ping-pong.

```rust
use ebr::Ebr;

let ebr: Ebr<Box<u64>> = Ebr::default();

let mut guard = ebr.pin();

guard.defer_drop(Box::new(1));
```
