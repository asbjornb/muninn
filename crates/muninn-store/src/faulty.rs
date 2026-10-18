//! Deterministic fault injection.
//!
//! [`FaultyStore`] wraps any store and, driven by a seeded PRNG of its own (SplitMix64),
//! makes operations fail in the three ways that matter for crash consistency:
//!
//! - **fail before**: the operation does not reach the inner store; the caller gets `Io`.
//! - **fail after** (mutating ops only): the operation *is applied* to the inner store, but
//!   the caller still gets `Io` — the ambiguous failure every caller must survive.
//! - **crash at op N**: operation number `N` (0-based, counting every call) fails — with or
//!   without taking effect — and every later operation fails until [`FaultyStore::restart`],
//!   which models the process coming back up over the same durable state.
//!
//! The same seed and the same sequence of calls give the same faults. Every call is
//! recorded in an op log for debugging failing simulations.

use std::fmt;
use std::ops::Range;
use std::sync::{Mutex, PoisonError};

use crate::{Key, ObjectMeta, PutOutcome, Result, Store, StoreError};

/// A store operation, for the op log.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum OpKind {
    /// [`Store::get`].
    Get,
    /// [`Store::get_range`].
    GetRange,
    /// [`Store::head`].
    Head,
    /// [`Store::put`].
    Put,
    /// [`Store::put_if_absent`].
    PutIfAbsent,
    /// [`Store::list`].
    List,
    /// [`Store::delete`].
    Delete,
}

impl OpKind {
    /// Does this operation change the store?
    pub fn is_mutating(self) -> bool {
        matches!(self, OpKind::Put | OpKind::PutIfAbsent | OpKind::Delete)
    }
}

/// What the wrapper did to an operation.
#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum FaultKind {
    /// Passed through untouched.
    None,
    /// Failed without reaching the inner store.
    FailedBefore,
    /// Applied to the inner store, then reported as failed.
    FailedAfter,
    /// The crash point: failed without taking effect; the store is now crashed.
    CrashBefore,
    /// The crash point: applied, then failed; the store is now crashed.
    CrashAfter,
    /// Rejected because the store is crashed.
    Crashed,
}

impl FaultKind {
    /// Did the operation reach the inner store?
    pub fn took_effect(self) -> bool {
        matches!(
            self,
            FaultKind::None | FaultKind::FailedAfter | FaultKind::CrashAfter
        )
    }
}

/// One entry of the op log.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OpRecord {
    /// 0-based index of the call.
    pub index: u64,
    /// The operation.
    pub op: OpKind,
    /// The key or list prefix.
    pub key: String,
    /// Payload size for puts.
    pub len: Option<usize>,
    /// What was injected.
    pub fault: FaultKind,
    /// The inner store's outcome when it was called (`"ok"`, `"ok: AlreadyExists"`, or the
    /// error); `None` if it was not called.
    pub inner: Option<String>,
}

impl fmt::Display for OpRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "#{} {:?} {:?}", self.index, self.op, self.key)?;
        if let Some(len) = self.len {
            write!(f, " ({len} bytes)")?;
        }
        if self.fault != FaultKind::None {
            write!(f, " [{:?}]", self.fault)?;
        }
        if let Some(inner) = &self.inner {
            write!(f, " -> {inner}")?;
        }
        Ok(())
    }
}

/// SplitMix64: tiny, fast, good enough for fault schedules.
#[derive(Clone, Debug)]
struct SplitMix64(u64);

impl SplitMix64 {
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// A uniform draw in `[0, 1)`.
    fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[derive(Debug)]
struct State {
    rng: SplitMix64,
    next_index: u64,
    fail_before: f64,
    fail_after: f64,
    crash_at: Option<(u64, bool)>,
    crashed: bool,
    log: Vec<OpRecord>,
}

/// A deterministic fault-injecting wrapper; see the module docs.
#[derive(Debug)]
pub struct FaultyStore<S> {
    inner: S,
    state: Mutex<State>,
}

impl<S: Store> FaultyStore<S> {
    /// Wraps `inner` with no faults configured.
    pub fn new(inner: S, seed: u64) -> FaultyStore<S> {
        FaultyStore {
            inner,
            state: Mutex::new(State {
                rng: SplitMix64(seed),
                next_index: 0,
                fail_before: 0.0,
                fail_after: 0.0,
                crash_at: None,
                crashed: false,
                log: Vec::new(),
            }),
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// The wrapped store (ground truth, bypassing faults).
    pub fn inner(&self) -> &S {
        &self.inner
    }

    /// Probability that any operation fails before taking effect.
    pub fn set_fail_before(&self, p: f64) {
        self.state().fail_before = p;
    }

    /// Probability that a mutating operation (that did not fail before) takes effect and
    /// then reports failure.
    pub fn set_fail_after(&self, p: f64) {
        self.state().fail_after = p;
    }

    /// Crash at operation `index` (0-based over all calls). With `takes_effect`, that
    /// operation is applied before the crash (only meaningful for mutating ops).
    pub fn set_crash_at(&self, index: u64, takes_effect: bool) {
        self.state().crash_at = Some((index, takes_effect));
    }

    /// Crash now: every subsequent operation fails until [`FaultyStore::restart`].
    pub fn crash(&self) {
        self.state().crashed = true;
    }

    /// Ends a crash, clears any pending crash point and turns off random faults.
    pub fn restart(&self) {
        let mut s = self.state();
        s.crashed = false;
        s.crash_at = None;
        s.fail_before = 0.0;
        s.fail_after = 0.0;
    }

    /// Is the store currently crashed?
    pub fn is_crashed(&self) -> bool {
        self.state().crashed
    }

    /// Number of operations attempted so far.
    pub fn op_count(&self) -> u64 {
        self.state().next_index
    }

    /// A copy of the op log.
    pub fn op_log(&self) -> Vec<OpRecord> {
        self.state().log.clone()
    }

    /// The op log rendered one operation per line.
    pub fn op_log_text(&self) -> String {
        self.state().log.iter().map(|r| format!("{r}\n")).collect()
    }

    fn decide(&self, op: OpKind) -> (u64, FaultKind) {
        let mut s = self.state();
        let index = s.next_index;
        s.next_index += 1;
        if s.crashed {
            return (index, FaultKind::Crashed);
        }
        if let Some((at, effect)) = s.crash_at
            && at == index
        {
            s.crashed = true;
            s.crash_at = None;
            let fault = if effect && op.is_mutating() {
                FaultKind::CrashAfter
            } else {
                FaultKind::CrashBefore
            };
            return (index, fault);
        }
        // Always draw both numbers so the schedule does not depend on configuration order.
        let (a, b) = (s.rng.next_f64(), s.rng.next_f64());
        let fault = if a < s.fail_before {
            FaultKind::FailedBefore
        } else if op.is_mutating() && b < s.fail_after {
            FaultKind::FailedAfter
        } else {
            FaultKind::None
        };
        (index, fault)
    }

    fn run<T>(
        &self,
        op: OpKind,
        key: &str,
        len: Option<usize>,
        call: impl FnOnce(&S) -> Result<T>,
        describe: impl Fn(&T) -> String,
    ) -> Result<T> {
        let (index, fault) = self.decide(op);
        let (inner, result) = if fault.took_effect() {
            let r = call(&self.inner);
            let text = match &r {
                Ok(v) => describe(v),
                Err(e) => format!("error: {e}"),
            };
            (Some(text), Some(r))
        } else {
            (None, None)
        };
        self.state().log.push(OpRecord {
            index,
            op,
            key: key.to_owned(),
            len,
            fault,
            inner,
        });
        match (fault, result) {
            (FaultKind::None, Some(r)) => r,
            _ => Err(StoreError::injected(format!(
                "injected fault {fault:?} on op #{index} {op:?} {key:?}"
            ))),
        }
    }
}

fn ok<T>(_: &T) -> String {
    "ok".to_owned()
}

impl<S: Store> Store for FaultyStore<S> {
    fn get(&self, key: &Key) -> Result<Vec<u8>> {
        self.run(
            OpKind::Get,
            key.as_str(),
            None,
            |s| s.get(key),
            |v| format!("ok: {} bytes", v.len()),
        )
    }

    fn get_range(&self, key: &Key, range: Range<u64>) -> Result<Vec<u8>> {
        let r = range.clone();
        self.run(
            OpKind::GetRange,
            key.as_str(),
            None,
            move |s| s.get_range(key, r),
            |v| format!("ok: {} bytes", v.len()),
        )
    }

    fn head(&self, key: &Key) -> Result<ObjectMeta> {
        self.run(
            OpKind::Head,
            key.as_str(),
            None,
            |s| s.head(key),
            |m| format!("ok: {} bytes", m.size),
        )
    }

    fn put(&self, key: &Key, data: &[u8]) -> Result<()> {
        self.run(
            OpKind::Put,
            key.as_str(),
            Some(data.len()),
            |s| s.put(key, data),
            ok,
        )
    }

    fn put_if_absent(&self, key: &Key, data: &[u8]) -> Result<PutOutcome> {
        self.run(
            OpKind::PutIfAbsent,
            key.as_str(),
            Some(data.len()),
            |s| s.put_if_absent(key, data),
            |o| format!("ok: {o:?}"),
        )
    }

    fn list(&self, prefix: &str) -> Result<Vec<Key>> {
        self.run(
            OpKind::List,
            prefix,
            None,
            |s| s.list(prefix),
            |v| format!("ok: {} keys", v.len()),
        )
    }

    fn delete(&self, key: &Key) -> Result<()> {
        self.run(OpKind::Delete, key.as_str(), None, |s| s.delete(key), ok)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryStore;

    fn key(s: &str) -> Key {
        Key::new(s).unwrap()
    }

    fn workload(store: &FaultyStore<MemoryStore>) -> Vec<bool> {
        (0..200)
            .map(|i| {
                let k = key(&format!("k/{}", i % 17));
                match i % 4 {
                    0 => store.put(&k, &[i as u8]).is_ok(),
                    1 => store.get(&k).is_ok(),
                    2 => store.put_if_absent(&k, b"x").is_ok(),
                    _ => store.delete(&k).is_ok(),
                }
            })
            .collect()
    }

    #[test]
    fn deterministic_for_a_seed() {
        let run = |seed| {
            let s = FaultyStore::new(MemoryStore::new(), seed);
            s.set_fail_before(0.2);
            s.set_fail_after(0.2);
            (workload(&s), s.op_log(), s.inner().list("").unwrap())
        };
        assert_eq!(run(7), run(7));
        assert_ne!(run(7).0, run(8).0);
        let (outcomes, log, _) = run(7);
        assert!(outcomes.iter().any(|ok| !ok) && outcomes.iter().any(|ok| *ok));
        assert!(log.iter().any(|r| r.fault == FaultKind::FailedBefore));
        assert!(log.iter().any(|r| r.fault == FaultKind::FailedAfter));
        assert!(
            log.iter()
                .all(|r| r.fault != FaultKind::FailedAfter || r.op.is_mutating())
        );
    }

    #[test]
    fn fail_before_has_no_effect_and_fail_after_does() {
        let s = FaultyStore::new(MemoryStore::new(), 1);
        s.set_fail_before(1.0);
        let e = s.put(&key("a"), b"1").unwrap_err();
        assert!(e.is_injected());
        assert!(s.inner().is_empty());
        s.set_fail_before(0.0);
        s.set_fail_after(1.0);
        assert!(s.put(&key("a"), b"1").unwrap_err().is_injected());
        assert_eq!(s.inner().get(&key("a")).unwrap(), b"1");
        // Reads are never "after" failures.
        assert_eq!(s.get(&key("a")).unwrap(), b"1");
        let log = s.op_log();
        assert_eq!(
            log.iter().map(|r| r.fault).collect::<Vec<_>>(),
            [
                FaultKind::FailedBefore,
                FaultKind::FailedAfter,
                FaultKind::None
            ]
        );
        assert!(
            s.op_log_text()
                .contains("#1 Put \"a\" (1 bytes) [FailedAfter] -> ok")
        );
    }

    #[test]
    fn crash_at_op_n() {
        for takes_effect in [false, true] {
            let s = FaultyStore::new(MemoryStore::new(), 3);
            s.set_crash_at(2, takes_effect);
            s.put(&key("a"), b"1").unwrap();
            s.put(&key("b"), b"2").unwrap();
            assert!(s.put_if_absent(&key("c"), b"3").is_err());
            assert!(s.is_crashed());
            assert!(s.get(&key("a")).is_err());
            assert!(s.list("").is_err());
            assert_eq!(s.inner().head(&key("c")).is_ok(), takes_effect);
            s.restart();
            assert!(!s.is_crashed());
            assert_eq!(s.get(&key("a")).unwrap(), b"1");
            assert_eq!(s.op_count(), 6);
            let faults: Vec<FaultKind> = s.op_log().iter().map(|r| r.fault).collect();
            let crash = if takes_effect {
                FaultKind::CrashAfter
            } else {
                FaultKind::CrashBefore
            };
            assert_eq!(
                faults,
                [
                    FaultKind::None,
                    FaultKind::None,
                    crash,
                    FaultKind::Crashed,
                    FaultKind::Crashed,
                    FaultKind::None
                ]
            );
        }
    }

    #[test]
    fn crash_point_on_a_read_never_takes_effect() {
        let s = FaultyStore::new(MemoryStore::new(), 3);
        s.set_crash_at(0, true);
        assert!(s.get(&key("a")).is_err());
        assert_eq!(s.op_log()[0].fault, FaultKind::CrashBefore);
    }
}
