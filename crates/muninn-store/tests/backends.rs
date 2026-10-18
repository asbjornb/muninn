//! Runs the shared backend conformance suite against every backend, plus backend-specific
//! checks.

use std::path::PathBuf;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use muninn_store::{FaultyStore, Key, LocalFsStore, MemoryStore, PutOutcome, Store, conformance};

fn fresh_dir(tag: &str) -> PathBuf {
    // One root per test binary run, wiped on first use so runs do not accumulate.
    static ROOT: OnceLock<PathBuf> = OnceLock::new();
    static N: AtomicUsize = AtomicUsize::new(0);
    let root = ROOT.get_or_init(|| {
        let root = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join("muninn-store-tests");
        if std::env::var_os("MUNINN_CRASH_CHILD_DIR").is_none() {
            let _ = std::fs::remove_dir_all(&root);
        }
        root
    });
    root.join(format!("{tag}-{}", N.fetch_add(1, Ordering::Relaxed)))
}

fn key(s: &str) -> Key {
    Key::new(s).unwrap()
}

#[test]
fn memory_store_conformance() {
    conformance::run_all(MemoryStore::new);
}

#[test]
fn local_fs_store_conformance() {
    conformance::run_all(|| LocalFsStore::open(fresh_dir("conf")).unwrap());
}

#[test]
fn faulty_store_without_faults_conformance() {
    conformance::run_all(|| FaultyStore::new(MemoryStore::new(), 42));
    conformance::run_all(|| FaultyStore::new(LocalFsStore::open(fresh_dir("faulty")).unwrap(), 42));
}

#[test]
fn arc_and_box_forward() {
    conformance::run_all(|| Arc::new(MemoryStore::new()));
    conformance::run_all(|| Box::new(MemoryStore::new()) as Box<dyn Store>);
}

#[test]
fn local_fs_persists_across_reopen() {
    let dir = fresh_dir("reopen");
    {
        let s = LocalFsStore::open(&dir).unwrap();
        s.put(&key("a/b"), b"1").unwrap();
        assert_eq!(
            s.put_if_absent(&key("log/1"), b"2").unwrap(),
            PutOutcome::Created
        );
    }
    let s = LocalFsStore::open(&dir).unwrap();
    assert_eq!(s.get(&key("a/b")).unwrap(), b"1");
    assert_eq!(s.get(&key("log/1")).unwrap(), b"2");
    assert_eq!(
        std::fs::read(dir.join("a").join("b")).unwrap(),
        b"1",
        "key a/b is file <root>/a/b"
    );
}

#[test]
fn local_fs_ignores_and_cleans_temp_files() {
    let dir = fresh_dir("temp");
    let s = LocalFsStore::open(&dir).unwrap();
    s.put(&key("log/1"), b"committed").unwrap();
    // Simulate crashed puts: temp files left next to objects and at the root.
    std::fs::write(dir.join("log").join(".muninn-tmp-999-1"), b"torn").unwrap();
    std::fs::write(dir.join(".muninn-tmp-999-2"), b"torn").unwrap();
    std::fs::write(dir.join("log").join(".other-hidden"), b"x").unwrap();
    assert_eq!(s.list("").unwrap(), vec![key("log/1")]);
    assert_eq!(s.list("log/").unwrap(), vec![key("log/1")]);
    assert_eq!(s.remove_stale_temp_files().unwrap(), 2);
    assert_eq!(s.remove_stale_temp_files().unwrap(), 0);
    assert!(
        dir.join("log").join(".other-hidden").exists(),
        "only our temp files are removed"
    );
    // No temp files survive successful operations.
    for i in 0..10 {
        s.put(&key(&format!("x/{i}")), b"v").unwrap();
        s.put_if_absent(&key(&format!("x/{i}")), b"w").unwrap();
    }
    assert_eq!(s.remove_stale_temp_files().unwrap(), 0);
}

#[test]
fn local_fs_put_if_absent_never_exposes_partial_object() {
    // A stale temp file with the same content prefix must not be visible under the key.
    let dir = fresh_dir("partial");
    let s = LocalFsStore::open(&dir).unwrap();
    std::fs::create_dir_all(dir.join("log")).unwrap();
    std::fs::write(dir.join("log").join(".muninn-tmp-1-1"), b"{\"half").unwrap();
    assert!(s.get(&key("log/1")).is_err());
    assert_eq!(
        s.put_if_absent(&key("log/1"), b"{\"whole\":1}").unwrap(),
        PutOutcome::Created
    );
    assert_eq!(s.get(&key("log/1")).unwrap(), b"{\"whole\":1}");
}

#[test]
fn faulty_store_over_local_fs_ambiguous_put() {
    let s = FaultyStore::new(LocalFsStore::open(fresh_dir("ambig")).unwrap(), 9);
    s.set_fail_after(1.0);
    assert!(
        s.put_if_absent(&key("log/1"), b"entry")
            .unwrap_err()
            .is_injected()
    );
    s.restart();
    // Recovery by reading back: the entry is there in full.
    assert_eq!(s.get(&key("log/1")).unwrap(), b"entry");
    assert_eq!(
        s.put_if_absent(&key("log/1"), b"entry").unwrap(),
        PutOutcome::AlreadyExists
    );
}

/// Child half of `local_fs_survives_kill_9`: when `MUNINN_CRASH_CHILD_DIR` is set, writes
/// self-describing objects forever (until killed). Without the variable it does nothing.
#[test]
fn crash_child() {
    let Some(dir) = std::env::var_os("MUNINN_CRASH_CHILD_DIR") else {
        return;
    };
    let s = LocalFsStore::open(PathBuf::from(dir)).unwrap();
    for i in 0u64..u64::MAX {
        // Object = 8-byte length header + that many bytes of a fill pattern.
        let len = 1 + (i * 7919) % 300_000;
        let mut data = len.to_le_bytes().to_vec();
        data.extend(std::iter::repeat_n((i % 251) as u8, len as usize));
        s.put(&key(&format!("obj/{}", i % 13)), &data).unwrap();
        let _ = s
            .put_if_absent(&key(&format!("log/{i:08}")), &data)
            .unwrap();
    }
}

#[test]
fn local_fs_survives_kill_9() {
    let dir = fresh_dir("kill9");
    std::fs::create_dir_all(&dir).unwrap();
    for round in 0..3u64 {
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--nocapture", "--test-threads=1"])
            .env("MUNINN_CRASH_CHILD_DIR", &dir)
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .unwrap();
        std::thread::sleep(std::time::Duration::from_millis(150 + 70 * round));
        child.kill().unwrap(); // SIGKILL
        child.wait().unwrap();
        let s = LocalFsStore::open(&dir).unwrap();
        let keys = s.list("").unwrap();
        for k in &keys {
            let data = s.get(k).unwrap();
            assert!(data.len() >= 8, "{k}: torn object of {} bytes", data.len());
            let len = u64::from_le_bytes(data[..8].try_into().unwrap()) as usize;
            assert_eq!(data.len(), 8 + len, "{k}: torn object");
            assert!(
                data[8..].iter().all(|&b| b == data[8]),
                "{k}: mixed content"
            );
        }
        s.remove_stale_temp_files().unwrap();
    }
}
