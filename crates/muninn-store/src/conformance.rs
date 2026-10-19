//! Backend conformance suite: the observable behaviour every [`Store`] must share.
//!
//! Call [`run_all`] with a factory that returns a *fresh, empty* store each time; each
//! check panics with a descriptive message on failure. The suite is public so that future
//! backends (S3/Azure adapters, caches) can run it from their own tests.

use std::collections::BTreeSet;
use std::sync::Barrier;
use std::thread;

use crate::{Key, ObjectMeta, PutOutcome, Store, StoreErrorKind};

fn key(s: &str) -> Key {
    Key::new(s).unwrap_or_else(|e| panic!("bad test key {s:?}: {e}"))
}

fn keys(store: &dyn Store, prefix: &str) -> Vec<String> {
    store
        .list(prefix)
        .unwrap_or_else(|e| panic!("list({prefix:?}) failed: {e}"))
        .into_iter()
        .map(|k| k.as_str().to_owned())
        .collect()
}

/// A named conformance check.
pub type Check = (&'static str, fn(&dyn Store));

/// Every check, by name.
pub const CHECKS: &[Check] = &[
    ("missing_objects", missing_objects),
    ("put_get_overwrite", put_get_overwrite),
    ("empty_and_large_objects", empty_and_large_objects),
    ("get_range", get_range),
    ("put_if_absent", put_if_absent),
    ("list_prefix_semantics", list_prefix_semantics),
    ("delete", delete),
    ("delete_many", delete_many),
    ("path_conflicts", path_conflicts),
    (
        "concurrent_put_if_absent_single_winner",
        concurrent_put_if_absent_single_winner,
    ),
    ("concurrent_puts_are_atomic", concurrent_puts_are_atomic),
    ("concurrent_distinct_keys", concurrent_distinct_keys),
];

/// Runs every check against fresh stores from `make`.
pub fn run_all<S: Store>(make: impl Fn() -> S) {
    for (name, check) in CHECKS {
        let store = make();
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| check(&store)));
        if let Err(panic) = result {
            let msg = panic
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| panic.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            panic!("store conformance check `{name}` failed: {msg}");
        }
    }
}

/// `get`/`get_range`/`head` of an absent key are `NotFound`; `delete` succeeds; `list` is
/// empty.
pub fn missing_objects(s: &dyn Store) {
    let k = key("nope/x");
    assert_eq!(s.get(&k).unwrap_err().kind(), StoreErrorKind::NotFound);
    assert_eq!(
        s.get_range(&k, 0..0).unwrap_err().kind(),
        StoreErrorKind::NotFound
    );
    assert_eq!(s.head(&k).unwrap_err().kind(), StoreErrorKind::NotFound);
    s.delete(&k).expect("deleting an absent key succeeds");
    assert!(keys(s, "").is_empty());
    assert!(keys(s, "nope/").is_empty());
}

/// Round trip and overwrite.
pub fn put_get_overwrite(s: &dyn Store) {
    let k = key("a/b/c.bin");
    s.put(&k, b"hello").unwrap();
    assert_eq!(s.get(&k).unwrap(), b"hello");
    assert_eq!(s.head(&k).unwrap(), ObjectMeta { size: 5 });
    s.put(&k, b"bye").unwrap();
    assert_eq!(s.get(&k).unwrap(), b"bye");
    assert_eq!(s.head(&k).unwrap().size, 3);
    // Parent "directories" are not objects.
    assert_eq!(
        s.get(&key("a/b")).unwrap_err().kind(),
        StoreErrorKind::NotFound
    );
    assert_eq!(
        s.head(&key("a")).unwrap_err().kind(),
        StoreErrorKind::NotFound
    );
    assert_eq!(keys(s, ""), ["a/b/c.bin"]);
}

/// Zero-byte and multi-MiB objects.
pub fn empty_and_large_objects(s: &dyn Store) {
    let e = key("empty");
    s.put(&e, b"").unwrap();
    assert_eq!(s.get(&e).unwrap(), b"");
    assert_eq!(s.head(&e).unwrap().size, 0);
    assert_eq!(s.get_range(&e, 0..0).unwrap(), b"");
    assert_eq!(
        s.get_range(&e, 0..1).unwrap_err().kind(),
        StoreErrorKind::InvalidRange
    );
    let big: Vec<u8> = (0..3_000_017u32)
        .map(|i| (i.wrapping_mul(2_654_435_761) >> 13) as u8)
        .collect();
    let b = key("big");
    s.put(&b, &big).unwrap();
    assert_eq!(s.get(&b).unwrap(), big);
    assert_eq!(
        s.get_range(&b, 1_000_000..1_000_100).unwrap(),
        &big[1_000_000..1_000_100]
    );
    let n = big.len() as u64;
    assert_eq!(s.get_range(&b, n - 7..n).unwrap(), &big[big.len() - 7..]);
    assert_eq!(
        s.put_if_absent(&key("empty2"), b"").unwrap(),
        PutOutcome::Created
    );
    assert_eq!(
        s.put_if_absent(&key("empty2"), b"x").unwrap(),
        PutOutcome::AlreadyExists
    );
    assert_eq!(s.get(&key("empty2")).unwrap(), b"");
}

/// Strict range semantics: `start <= end <= size`.
pub fn get_range(s: &dyn Store) {
    let k = key("r");
    s.put(&k, b"0123456789").unwrap();
    assert_eq!(s.get_range(&k, 0..10).unwrap(), b"0123456789");
    assert_eq!(s.get_range(&k, 2..5).unwrap(), b"234");
    assert_eq!(s.get_range(&k, 0..0).unwrap(), b"");
    assert_eq!(s.get_range(&k, 9..10).unwrap(), b"9");
    assert_eq!(
        s.get_range(&k, 10..10).unwrap(),
        b"",
        "empty range at the end is valid"
    );
    #[allow(clippy::reversed_empty_ranges)] // deliberately invalid input
    let bad_ranges = [10..11, 11..11, 5..4, 0..u64::MAX, u64::MAX..u64::MAX];
    for bad in bad_ranges {
        assert_eq!(
            s.get_range(&k, bad.clone()).unwrap_err().kind(),
            StoreErrorKind::InvalidRange,
            "range {bad:?}"
        );
    }
}

/// Creates once, never overwrites, works again after delete.
pub fn put_if_absent(s: &dyn Store) {
    let k = key("log/00000000000000000001.json");
    assert_eq!(s.put_if_absent(&k, b"first").unwrap(), PutOutcome::Created);
    assert_eq!(
        s.put_if_absent(&k, b"second").unwrap(),
        PutOutcome::AlreadyExists
    );
    assert_eq!(s.get(&k).unwrap(), b"first");
    s.put(&k, b"over").unwrap();
    assert_eq!(
        s.put_if_absent(&k, b"third").unwrap(),
        PutOutcome::AlreadyExists
    );
    assert_eq!(s.get(&k).unwrap(), b"over");
    s.delete(&k).unwrap();
    assert_eq!(s.put_if_absent(&k, b"again").unwrap(), PutOutcome::Created);
    assert_eq!(s.get(&k).unwrap(), b"again");
    assert_eq!(keys(s, ""), ["log/00000000000000000001.json"]);
}

/// Plain string-prefix matching, sorted output.
pub fn list_prefix_semantics(s: &dyn Store) {
    for k in [
        "b", "a/x", "a/y/z", "ab", "a-b", "log/0002", "log/0001", "log/0010", "logs/1", "z/z/z/z",
    ] {
        s.put(&key(k), k.as_bytes()).unwrap();
    }
    assert_eq!(
        keys(s, ""),
        [
            "a-b", "a/x", "a/y/z", "ab", "b", "log/0001", "log/0002", "log/0010", "logs/1",
            "z/z/z/z"
        ]
    );
    assert_eq!(keys(s, "a"), ["a-b", "a/x", "a/y/z", "ab"]);
    assert_eq!(keys(s, "a/"), ["a/x", "a/y/z"]);
    assert_eq!(keys(s, "a/y"), ["a/y/z"]);
    assert_eq!(keys(s, "a/y/z"), ["a/y/z"]);
    assert_eq!(keys(s, "log/"), ["log/0001", "log/0002", "log/0010"]);
    assert_eq!(keys(s, "log/000"), ["log/0001", "log/0002"]);
    assert_eq!(
        keys(s, "log"),
        ["log/0001", "log/0002", "log/0010", "logs/1"]
    );
    assert_eq!(keys(s, "z/z/"), ["z/z/z/z"]);
    assert!(keys(s, "nothing").is_empty());
    assert!(keys(s, "nothing/here/").is_empty());
    assert!(keys(s, "b/").is_empty(), "an object is not a directory");
    assert!(keys(s, "a/x/").is_empty());
    for bad in ["/", "/a", "a//", "../"] {
        assert_eq!(
            s.list(bad).unwrap_err().kind(),
            StoreErrorKind::InvalidKey,
            "prefix {bad:?}"
        );
    }
}

/// Delete removes exactly one object.
pub fn delete(s: &dyn Store) {
    for k in ["d/1", "d/2", "d/sub/3"] {
        s.put(&key(k), b"x").unwrap();
    }
    s.delete(&key("d/2")).unwrap();
    assert_eq!(
        s.get(&key("d/2")).unwrap_err().kind(),
        StoreErrorKind::NotFound
    );
    assert_eq!(keys(s, "d/"), ["d/1", "d/sub/3"]);
    s.delete(&key("d/2")).unwrap();
    s.delete(&key("d/sub/3")).unwrap();
    s.delete(&key("d/sub")).unwrap(); // not an object: no-op
    assert_eq!(keys(s, ""), ["d/1"]);
}

/// `delete_many` removes exactly the given objects (absent ones are fine) and frees
/// their paths.
pub fn delete_many(s: &dyn Store) {
    for k in ["m/1", "m/2", "m/sub/3", "m/sub/4", "n/5"] {
        s.put(&key(k), b"x").unwrap();
    }
    s.delete_many(&[key("m/sub/3"), key("m/2"), key("m/nope"), key("m/sub/4")])
        .unwrap();
    assert_eq!(keys(s, ""), ["m/1", "n/5"]);
    s.delete_many(&[]).unwrap();
    // The emptied directory can hold an object again.
    s.put(&key("m/sub"), b"y").unwrap();
    assert_eq!(s.get(&key("m/sub")).unwrap(), b"y");
}

/// A key may not be a path-prefix of another key, in either direction; freed paths can be
/// reused.
pub fn path_conflicts(s: &dyn Store) {
    s.put(&key("a"), b"file").unwrap();
    assert_eq!(
        s.put(&key("a/b"), b"x").unwrap_err().kind(),
        StoreErrorKind::Conflict
    );
    assert_eq!(
        s.put_if_absent(&key("a/b/c"), b"x").unwrap_err().kind(),
        StoreErrorKind::Conflict
    );
    s.put(&key("x/y/z"), b"deep").unwrap();
    assert_eq!(
        s.put(&key("x"), b"x").unwrap_err().kind(),
        StoreErrorKind::Conflict
    );
    assert_eq!(
        s.put(&key("x/y"), b"x").unwrap_err().kind(),
        StoreErrorKind::Conflict
    );
    assert_eq!(
        s.put_if_absent(&key("x/y"), b"x").unwrap_err().kind(),
        StoreErrorKind::Conflict
    );
    assert_eq!(keys(s, ""), ["a", "x/y/z"]);
    // After deleting the deep key the path can hold an object again.
    s.delete(&key("x/y/z")).unwrap();
    assert_eq!(
        s.put_if_absent(&key("x"), b"now a file").unwrap(),
        PutOutcome::Created
    );
    assert_eq!(s.get(&key("x")).unwrap(), b"now a file");
    s.delete(&key("a")).unwrap();
    s.put(&key("a/b"), b"now a dir").unwrap();
    assert_eq!(keys(s, ""), ["a/b", "x"]);
}

/// Many threads race `put_if_absent` on the same keys: exactly one winner per key, and
/// the stored bytes are the winner's.
pub fn concurrent_put_if_absent_single_winner(s: &dyn Store) {
    const THREADS: usize = 8;
    const KEYS: usize = 16;
    let barrier = Barrier::new(THREADS);
    let wins: Vec<Vec<usize>> = thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|t| {
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let mut won = Vec::new();
                    for k in 0..KEYS {
                        let data = format!("thread-{t}").into_bytes();
                        match s
                            .put_if_absent(&key(&format!("race/{k:03}")), &data)
                            .unwrap()
                        {
                            PutOutcome::Created => won.push(k),
                            PutOutcome::AlreadyExists => {}
                        }
                    }
                    won
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });
    for k in 0..KEYS {
        let winners: Vec<usize> = (0..THREADS).filter(|t| wins[*t].contains(&k)).collect();
        assert_eq!(winners.len(), 1, "key {k}: winners {winners:?}");
        let got = s.get(&key(&format!("race/{k:03}"))).unwrap();
        assert_eq!(got, format!("thread-{}", winners[0]).into_bytes());
    }
    assert_eq!(keys(s, "race/").len(), KEYS);
}

/// Concurrent overwrites of one key: readers only ever see a complete version.
pub fn concurrent_puts_are_atomic(s: &dyn Store) {
    let k = key("hot");
    s.put(&k, &[0u8; 10]).unwrap();
    thread::scope(|scope| {
        for t in 1..=4u8 {
            let k = &k;
            scope.spawn(move || {
                for i in 0..50usize {
                    // Each version is `len` copies of one byte; lengths differ per write.
                    let len = 1 + (usize::from(t) * 7919 + i * 104_729) % 20_000;
                    s.put(k, &vec![t; len]).unwrap();
                }
            });
        }
        for _ in 0..2 {
            let k = &k;
            scope.spawn(move || {
                for _ in 0..200 {
                    let v = s.get(k).unwrap();
                    assert!(
                        !v.is_empty() && v.iter().all(|&b| b == v[0]),
                        "torn read: {} bytes",
                        v.len()
                    );
                }
            });
        }
    });
    assert_eq!(keys(s, ""), ["hot"]);
}

/// Concurrent writers to distinct keys all land.
pub fn concurrent_distinct_keys(s: &dyn Store) {
    thread::scope(|scope| {
        for t in 0..6 {
            scope.spawn(move || {
                for i in 0..20 {
                    s.put(&key(&format!("p/{t}/{i:02}")), &[t as u8, i as u8])
                        .unwrap();
                }
            });
        }
    });
    let all: BTreeSet<String> = keys(s, "p/").into_iter().collect();
    assert_eq!(all.len(), 120);
    assert_eq!(s.get(&key("p/3/07")).unwrap(), [3, 7]);
}
