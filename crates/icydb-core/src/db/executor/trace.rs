//! Executor query tracing boundary.
//!
//! Tracing is optional, injected by the caller, and must not affect execution semantics.

use crate::{
    db::query::plan::{AccessPath, AccessPlan, ExecutablePlan, PlanFingerprint},
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
};
use sha2::{Digest, Sha256};

///
/// QueryTraceSink
///

pub trait QueryTraceSink: Send + Sync {
    fn on_event(&self, event: QueryTraceEvent);
}

///
/// TraceExecutorKind
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceExecutorKind {
    Load,
    Save,
    Delete,
}

///
/// TraceAccess
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TraceAccess {
    ByKey,
    ByKeys { count: u32 },
    KeyRange,
    IndexPrefix { name: &'static str, prefix_len: u32 },
    FullScan,
    UniqueIndex { name: &'static str },
    Union { branches: u32 },
    Intersection { branches: u32 },
}

///
/// TracePhase
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TracePhase {
    Access,
    Filter,
    Order,
    Page,
    DeleteLimit,
}

///
/// QueryTraceEvent
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum QueryTraceEvent {
    Start {
        fingerprint: PlanFingerprint,
        executor: TraceExecutorKind,
        access: Option<TraceAccess>,
    },
    Phase {
        fingerprint: PlanFingerprint,
        executor: TraceExecutorKind,
        access: Option<TraceAccess>,
        phase: TracePhase,
        rows: u64,
    },
    Finish {
        fingerprint: PlanFingerprint,
        executor: TraceExecutorKind,
        access: Option<TraceAccess>,
        rows: u64,
    },
    Error {
        fingerprint: PlanFingerprint,
        executor: TraceExecutorKind,
        access: Option<TraceAccess>,
        class: ErrorClass,
        origin: ErrorOrigin,
    },
}

///
/// TraceScope
///

pub struct TraceScope {
    sink: &'static dyn QueryTraceSink,
    fingerprint: PlanFingerprint,
    executor: TraceExecutorKind,
    access: Option<TraceAccess>,
}

impl TraceScope {
    fn new(
        sink: &'static dyn QueryTraceSink,
        fingerprint: PlanFingerprint,
        executor: TraceExecutorKind,
        access: Option<TraceAccess>,
    ) -> Self {
        sink.on_event(QueryTraceEvent::Start {
            fingerprint,
            executor,
            access,
        });
        Self {
            sink,
            fingerprint,
            executor,
            access,
        }
    }

    pub(crate) fn finish(self, rows: u64) {
        self.sink.on_event(QueryTraceEvent::Finish {
            fingerprint: self.fingerprint,
            executor: self.executor,
            access: self.access,
            rows,
        });
    }

    pub(crate) fn phase(&self, phase: TracePhase, rows: u64) {
        self.sink.on_event(QueryTraceEvent::Phase {
            fingerprint: self.fingerprint,
            executor: self.executor,
            access: self.access,
            phase,
            rows,
        });
    }

    pub(crate) fn error(self, err: &InternalError) {
        self.sink.on_event(QueryTraceEvent::Error {
            fingerprint: self.fingerprint,
            executor: self.executor,
            access: self.access,
            class: err.class,
            origin: err.origin,
        });
    }
}

pub fn start_plan_trace<E: EntityKind>(
    sink: Option<&'static dyn QueryTraceSink>,
    executor: TraceExecutorKind,
    plan: &ExecutablePlan<E>,
) -> Option<TraceScope> {
    let sink = sink?;
    let access = Some(trace_access_from_plan(plan.access()));
    let fingerprint = plan.fingerprint();
    Some(TraceScope::new(sink, fingerprint, executor, access))
}

pub fn start_exec_trace(
    sink: Option<&'static dyn QueryTraceSink>,
    executor: TraceExecutorKind,
    entity_path: &'static str,
    access: Option<TraceAccess>,
    detail: Option<&'static str>,
) -> Option<TraceScope> {
    let sink = sink?;
    let fingerprint = exec_fingerprint(executor, entity_path, detail);
    Some(TraceScope::new(sink, fingerprint, executor, access))
}

fn trace_access_from_plan<K>(plan: &AccessPlan<K>) -> TraceAccess {
    match plan {
        AccessPlan::Path(path) => trace_access_from_path(path),
        AccessPlan::Union(children) => TraceAccess::Union {
            branches: u32::try_from(children.len()).unwrap_or(u32::MAX),
        },
        AccessPlan::Intersection(children) => TraceAccess::Intersection {
            branches: u32::try_from(children.len()).unwrap_or(u32::MAX),
        },
    }
}

fn trace_access_from_path<K>(path: &AccessPath<K>) -> TraceAccess {
    match path {
        AccessPath::ByKey(_) => TraceAccess::ByKey,
        AccessPath::ByKeys(keys) => TraceAccess::ByKeys {
            count: u32::try_from(keys.len()).unwrap_or(u32::MAX),
        },
        AccessPath::KeyRange { .. } => TraceAccess::KeyRange,
        AccessPath::IndexPrefix { index, values } => TraceAccess::IndexPrefix {
            name: index.name,
            prefix_len: u32::try_from(values.len()).unwrap_or(u32::MAX),
        },
        AccessPath::FullScan => TraceAccess::FullScan,
    }
}

fn exec_fingerprint(
    executor: TraceExecutorKind,
    entity_path: &'static str,
    detail: Option<&'static str>,
) -> PlanFingerprint {
    let mut hasher = Sha256::new();
    hasher.update(b"execfp:v1");
    hasher.update([executor_tag(executor)]);
    write_str(&mut hasher, entity_path);
    match detail {
        Some(detail) => {
            hasher.update([1u8]);
            write_str(&mut hasher, detail);
        }
        None => {
            hasher.update([0u8]);
        }
    }
    let digest = hasher.finalize();
    let mut out = [0u8; 32];
    out.copy_from_slice(&digest);
    PlanFingerprint::from_bytes(out)
}

const fn executor_tag(executor: TraceExecutorKind) -> u8 {
    match executor {
        TraceExecutorKind::Load => 0x01,
        TraceExecutorKind::Save => 0x02,
        TraceExecutorKind::Delete => 0x03,
    }
}

fn write_str(hasher: &mut Sha256, value: &str) {
    let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
    hasher.update(len.to_be_bytes());
    hasher.update(value.as_bytes());
}
