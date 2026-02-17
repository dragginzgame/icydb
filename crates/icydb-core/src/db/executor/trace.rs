//! Executor query tracing boundary.
//!
//! Tracing is optional, injected by the caller, and must not affect execution semantics.

use crate::{
    db::query::plan::{
        AccessPlan, AccessPlanProjection, ExecutablePlan, PlanFingerprint, project_access_plan,
        validate::{PushdownSurfaceEligibility, SecondaryOrderPushdownRejection},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    traits::EntityKind,
    value::Value,
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

///
/// QueryTraceSink
///

pub(crate) trait QueryTraceSink: Send + Sync {
    fn on_event(&self, event: QueryTraceEvent);
}

///
/// TraceExecutorKind
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TraceExecutorKind {
    Load,
    Save,
    Delete,
}

///
/// TraceAccess
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TraceAccess {
    ByKey,
    ByKeys { count: u32 },
    KeyRange,
    IndexPrefix { name: &'static str, prefix_len: u32 },
    IndexRange { name: &'static str, prefix_len: u32 },
    FullScan,
    Union { branches: u32 },
    Intersection { branches: u32 },
}

///
/// TracePhase
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TracePhase {
    Access,
    PostAccess,
}

///
/// TracePushdownRejectionReason
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TracePushdownRejectionReason {
    NoOrderBy,
    AccessPathNotSingleIndexPrefix,
    AccessPathIndexRangeUnsupported,
    InvalidIndexPrefixBounds,
    MissingPrimaryKeyTieBreak,
    PrimaryKeyDirectionNotAscending,
    NonAscendingDirection,
    OrderFieldsDoNotMatchIndex,
}

impl From<&SecondaryOrderPushdownRejection> for TracePushdownRejectionReason {
    fn from(value: &SecondaryOrderPushdownRejection) -> Self {
        match value {
            SecondaryOrderPushdownRejection::NoOrderBy => Self::NoOrderBy,
            SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix => {
                Self::AccessPathNotSingleIndexPrefix
            }
            SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported { .. } => {
                Self::AccessPathIndexRangeUnsupported
            }
            SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds { .. } => {
                Self::InvalidIndexPrefixBounds
            }
            SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak { .. } => {
                Self::MissingPrimaryKeyTieBreak
            }
            SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending { .. } => {
                Self::PrimaryKeyDirectionNotAscending
            }
            SecondaryOrderPushdownRejection::NonAscendingDirection { .. } => {
                Self::NonAscendingDirection
            }
            SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex { .. } => {
                Self::OrderFieldsDoNotMatchIndex
            }
        }
    }
}

///
/// TracePushdownDecision
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TracePushdownDecision {
    AcceptedSecondaryIndexOrder,
    RejectedSecondaryIndexOrder {
        reason: TracePushdownRejectionReason,
    },
}

impl From<PushdownSurfaceEligibility<'_>> for TracePushdownDecision {
    fn from(value: PushdownSurfaceEligibility<'_>) -> Self {
        match value {
            PushdownSurfaceEligibility::EligibleSecondaryIndex { .. } => {
                Self::AcceptedSecondaryIndexOrder
            }
            PushdownSurfaceEligibility::Rejected { reason } => Self::RejectedSecondaryIndexOrder {
                reason: TracePushdownRejectionReason::from(reason),
            },
        }
    }
}

///
/// QueryTraceEvent
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum QueryTraceEvent {
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
    Pushdown {
        fingerprint: PlanFingerprint,
        executor: TraceExecutorKind,
        access: Option<TraceAccess>,
        decision: TracePushdownDecision,
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

pub(crate) struct TraceScope {
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

    pub(crate) fn pushdown(&self, decision: TracePushdownDecision) {
        self.sink.on_event(QueryTraceEvent::Pushdown {
            fingerprint: self.fingerprint,
            executor: self.executor,
            access: self.access,
            decision,
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

pub(crate) fn start_plan_trace<E: EntityKind>(
    sink: Option<&'static dyn QueryTraceSink>,
    executor: TraceExecutorKind,
    plan: &ExecutablePlan<E>,
) -> Option<TraceScope> {
    let sink = sink?;
    let access = Some(trace_access_from_plan(plan.access()));
    let fingerprint = plan.fingerprint();
    Some(TraceScope::new(sink, fingerprint, executor, access))
}

pub(crate) fn start_exec_trace(
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

/// Convert a `usize` count to `u64` with saturation.
#[must_use]
pub(crate) fn saturating_rows(value: usize) -> u64 {
    u64::try_from(value).unwrap_or(u64::MAX)
}

/// Emit canonical access/post-access trace phases with saturated row counts.
pub(crate) fn emit_access_post_access_phases(
    trace: Option<&TraceScope>,
    access_rows: usize,
    post_access_rows: usize,
) {
    let Some(trace) = trace else {
        return;
    };

    trace.phase(TracePhase::Access, saturating_rows(access_rows));
    trace.phase(TracePhase::PostAccess, saturating_rows(post_access_rows));
}

/// Finish or error a trace scope from an executor result.
pub(crate) fn finish_trace_from_result<T>(
    trace: Option<TraceScope>,
    result: &Result<T, InternalError>,
    ok_rows: impl Fn(&T) -> usize,
) {
    let Some(trace) = trace else {
        return;
    };

    match result {
        Ok(ok) => trace.finish(saturating_rows(ok_rows(ok))),
        Err(err) => trace.error(err),
    }
}

fn trace_access_from_plan<K>(plan: &AccessPlan<K>) -> TraceAccess {
    let mut projection = TraceAccessProjection;
    project_access_plan(plan, &mut projection)
}

struct TraceAccessProjection;

impl<K> AccessPlanProjection<K> for TraceAccessProjection {
    type Output = TraceAccess;

    fn by_key(&mut self, _key: &K) -> Self::Output {
        TraceAccess::ByKey
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        // NOTE: Diagnostics are best-effort; overflow saturates to preserve determinism.
        TraceAccess::ByKeys {
            count: u32::try_from(keys.len()).unwrap_or(u32::MAX),
        }
    }

    fn key_range(&mut self, _start: &K, _end: &K) -> Self::Output {
        TraceAccess::KeyRange
    }

    fn index_prefix(
        &mut self,
        index_name: &'static str,
        _index_fields: &[&'static str],
        _prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        // NOTE: Diagnostics are best-effort; overflow saturates to preserve determinism.
        TraceAccess::IndexPrefix {
            name: index_name,
            prefix_len: u32::try_from(values.len()).unwrap_or(u32::MAX),
        }
    }

    fn index_range(
        &mut self,
        index_name: &'static str,
        _index_fields: &[&'static str],
        prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        // NOTE: Diagnostics are best-effort; overflow saturates to preserve determinism.
        TraceAccess::IndexRange {
            name: index_name,
            prefix_len: u32::try_from(prefix_len).unwrap_or(u32::MAX),
        }
    }

    fn full_scan(&mut self) -> Self::Output {
        TraceAccess::FullScan
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        // NOTE: Diagnostics are best-effort; overflow saturates to preserve determinism.
        TraceAccess::Union {
            branches: u32::try_from(children.len()).unwrap_or(u32::MAX),
        }
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        // NOTE: Diagnostics are best-effort; overflow saturates to preserve determinism.
        TraceAccess::Intersection {
            branches: u32::try_from(children.len()).unwrap_or(u32::MAX),
        }
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
    // NOTE: Diagnostics-only fingerprinting saturates on overflow to avoid panics.
    let len = u32::try_from(value.len()).unwrap_or(u32::MAX);
    hasher.update(len.to_be_bytes());
    hasher.update(value.as_bytes());
}
