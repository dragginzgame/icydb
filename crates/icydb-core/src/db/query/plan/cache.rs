//! Plan cache for deterministic logical plans; private to the planning boundary.

use super::{LogicalPlan, PlanFingerprint};
use std::{
    cell::Cell,
    collections::BTreeMap,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
};

///
/// PlanCache
///

struct PlanCache {
    plans: BTreeMap<PlanFingerprint, Arc<LogicalPlan>>,
}

// NOTE:
// Although the IC executes canister code single-threaded, this cache is still
// protected by a Mutex to make shared mutability explicit and to preserve
// correctness if this code is reused in multithreaded contexts (tests, tooling,
// or non-IC backends).
static CACHE: OnceLock<Mutex<PlanCache>> = OnceLock::new();
static HITS: AtomicUsize = AtomicUsize::new(0);
static MISSES: AtomicUsize = AtomicUsize::new(0);

const DEFAULT_CACHE_DISABLED: bool = cfg!(test) || !cfg!(feature = "plan-cache");

// Thread-local cache disable flag.
// This is intentionally thread-local so tests can safely toggle cache behavior
// without affecting other threads or global state.
thread_local! {
    static CACHE_DISABLED: std::cell::Cell<bool> = const { std::cell::Cell::new(DEFAULT_CACHE_DISABLED) };
}

///
/// CacheStats
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CacheStats {
    pub hits: usize,
    pub misses: usize,
    pub size: usize,
}

pub fn get(fingerprint: &PlanFingerprint) -> Option<Arc<LogicalPlan>> {
    if cache_disabled() {
        return None;
    }
    let cache = CACHE.get_or_init(|| {
        Mutex::new(PlanCache {
            plans: BTreeMap::new(),
        })
    });
    cache
        .lock()
        .expect("plan cache lock poisoned")
        .plans
        .get(fingerprint)
        .cloned()
}

pub fn insert(fingerprint: PlanFingerprint, plan: Arc<LogicalPlan>) {
    if cache_disabled() {
        return;
    }
    let cache = CACHE.get_or_init(|| {
        Mutex::new(PlanCache {
            plans: BTreeMap::new(),
        })
    });
    cache
        .lock()
        .expect("plan cache lock poisoned")
        .plans
        .insert(fingerprint, plan);
}

// Cache statistics are best-effort only.
// Relaxed atomics are sufficient because stats are not used for correctness.
pub fn record_hit() {
    if cache_disabled() {
        return;
    }
    HITS.fetch_add(1, Ordering::Relaxed);
}

pub fn record_miss() {
    if cache_disabled() {
        return;
    }
    MISSES.fetch_add(1, Ordering::Relaxed);
}

pub fn stats() -> CacheStats {
    let size = CACHE
        .get()
        .and_then(|cache| cache.lock().ok())
        .map_or(0, |cache| cache.plans.len());

    CacheStats {
        hits: HITS.load(Ordering::Relaxed),
        misses: MISSES.load(Ordering::Relaxed),
        size,
    }
}

#[allow(dead_code)]
pub fn reset() {
    if let Some(cache) = CACHE.get()
        && let Ok(mut guard) = cache.lock()
    {
        guard.plans.clear();
    }
    HITS.store(0, Ordering::Relaxed);
    MISSES.store(0, Ordering::Relaxed);
}

// Temporarily override cache behavior for the current thread.
// Intended for tests and benchmarking only.
#[expect(dead_code)]
pub fn with_cache_disabled<R>(f: impl FnOnce() -> R) -> R {
    CACHE_DISABLED.with(|flag| {
        let prev = flag.replace(true);
        let out = f();
        flag.set(prev);
        out
    })
}

// Temporarily override cache behavior for the current thread.
// Intended for tests and benchmarking only.
#[allow(dead_code)]
pub fn with_cache_enabled<R>(f: impl FnOnce() -> R) -> R {
    CACHE_DISABLED.with(|flag| {
        let prev = flag.replace(false);
        let out = f();
        flag.set(prev);
        out
    })
}

fn cache_disabled() -> bool {
    CACHE_DISABLED.with(Cell::get)
}
