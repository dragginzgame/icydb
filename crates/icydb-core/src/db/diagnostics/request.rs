//! Module: diagnostics::request
//! Responsibility: bounded request-wide query-shape and repeated-key summaries.
//! Does not own: query planning, execution budgets, or application authorization.
//! Boundary: consumes literal-free plan/resource observations and exposes bounded DTO snapshots.

use candid::CandidType;
use serde::Deserialize;

const MAX_REQUEST_DIAGNOSTIC_SHAPES: usize = 32;
const MAX_REQUEST_DIAGNOSTIC_KEY_IDENTITIES: usize = 128;
const MAX_REQUEST_DIAGNOSTIC_FIELDS: usize = 8;
const MAX_REQUEST_DIAGNOSTIC_LABEL_BYTES: usize = 128;
const REPEATED_SHAPE_WARNING_THRESHOLD: u64 = 8;

/// Coarse access-path family selected for one normalized request query shape.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum RequestDiagnosticAccessPath {
    /// One primary-key lookup.
    ByKey,
    /// One bounded set of primary-key lookups.
    ByKeys,
    /// One primary-key range traversal.
    KeyRange,
    /// One secondary-index prefix traversal.
    IndexPrefix,
    /// Multiple exact lookups through one secondary index.
    IndexMultiLookup,
    /// One bounded set of secondary-index prefix branches.
    IndexBranchSet,
    /// One secondary-index range traversal.
    IndexRange,
    /// One full entity traversal.
    FullScan,
    /// One union of access children.
    Union,
    /// One intersection of access children.
    Intersection,
    /// A planner-free direct terminal without a prepared access plan.
    Direct,
}

/// Actionable warning class derived from one bounded request summary.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, PartialEq)]
pub enum RequestDiagnosticWarningKind {
    /// One normalized query shape executed repeatedly in the request.
    RepeatedQueryShape,
    /// One hashed exact-key identity was looked up repeatedly.
    RepeatedExactKey,
    /// Residual equality work suggests a useful compound index prefix.
    CompoundIndexCandidate,
}

/// One bounded warning without query literals or raw key material.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RequestDiagnosticWarning {
    /// Warning family.
    pub kind: RequestDiagnosticWarningKind,
    /// Literal-free normalized query-shape fingerprint prefix.
    pub normalized_shape_fingerprint_prefix: u64,
    /// Relevant observed count, when the warning is count-based.
    pub observed: u64,
    /// Bounded human-readable action.
    pub message: String,
}

/// Aggregated work for one literal-free normalized query shape.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RequestQueryShapeDiagnostic {
    /// Literal-free normalized query-shape fingerprint prefix.
    pub normalized_shape_fingerprint_prefix: u64,
    /// Bounded accepted entity path, or an empty string for a direct terminal.
    pub entity: String,
    /// Selected coarse access-path family.
    pub access_path: RequestDiagnosticAccessPath,
    /// Selected accepted secondary-index name, when applicable.
    pub selected_index: Option<String>,
    /// Executions attempted for this shape.
    pub executions: u64,
    /// Shared plan-cache hits observed while preparing this shape.
    pub plan_cache_hits: u64,
    /// Shared plan-cache misses observed while preparing this shape.
    pub plan_cache_misses: u64,
    /// Plan compilations observed for this shape.
    pub plan_compilations: u64,
    /// Physical primary/index keys visited.
    pub keys_visited: u64,
    /// Stored rows visited.
    pub rows_visited: u64,
    /// Logical rows returned.
    pub rows_returned: u64,
    /// Stored row bytes read.
    pub stored_bytes_read: u64,
    /// Persisted bytes decoded.
    pub decoded_bytes: u64,
    /// Runtime value bytes materialized.
    pub materialized_bytes: u64,
    /// Logical result bytes produced.
    pub result_bytes: u64,
    /// Exact-key input positions observed, including duplicates.
    pub exact_key_lookups: u64,
    /// Exact-key positions beyond the first occurrence of retained identities.
    pub repeated_key_lookups: u64,
    /// Largest retained lookup count for one hashed key identity.
    pub hottest_key_lookups: u64,
    /// Bounded residual predicate field names.
    pub residual_fields: Vec<String>,
    /// Bounded diagnostic-only compound-index field candidate.
    pub compound_index_candidate: Vec<String>,
}

/// Bounded request-wide query diagnostics snapshot.
#[derive(CandidType, Clone, Debug, Deserialize, Eq, PartialEq)]
pub struct RequestDiagnostics {
    /// Retained normalized query shapes.
    pub shapes: Vec<RequestQueryShapeDiagnostic>,
    /// Actionable warnings derived from retained evidence.
    pub warnings: Vec<RequestDiagnosticWarning>,
    /// Shape observations omitted after the shape capacity was reached.
    pub overflowed_shapes: u64,
    /// New hashed key identities omitted after the key capacity was reached.
    pub overflowed_key_identities: u64,
    /// Observations or fields suppressed by capacity or fail-soft accounting.
    pub suppressed_observations: u64,
}

#[derive(Clone, Debug)]
pub(in crate::db) struct RequestQueryPlanEvidence {
    pub(in crate::db) normalized_shape_fingerprint_prefix: u64,
    pub(in crate::db) entity: String,
    pub(in crate::db) access_path: RequestDiagnosticAccessPath,
    pub(in crate::db) selected_index: Option<String>,
    pub(in crate::db) residual_fields: Vec<String>,
    pub(in crate::db) compound_index_candidate: Vec<String>,
    pub(in crate::db) exact_key_hashes: Vec<[u8; 16]>,
    pub(in crate::db) suppressed_fields: u64,
}

impl RequestQueryPlanEvidence {
    pub(in crate::db) fn bounded(
        normalized_shape_fingerprint_prefix: u64,
        entity: &str,
        access_path: RequestDiagnosticAccessPath,
        selected_index: Option<&str>,
        residual_fields: Vec<&str>,
        compound_index_candidate: Vec<&str>,
        exact_key_hashes: Vec<[u8; 16]>,
    ) -> Self {
        let mut suppressed_fields = 0_u64;
        let entity = bounded_label(entity, &mut suppressed_fields).unwrap_or_default();
        let selected_index =
            selected_index.and_then(|index| bounded_label(index, &mut suppressed_fields));
        let residual_fields = bounded_labels(residual_fields, &mut suppressed_fields);
        let compound_index_candidate =
            bounded_labels(compound_index_candidate, &mut suppressed_fields);

        Self {
            normalized_shape_fingerprint_prefix,
            entity,
            access_path,
            selected_index,
            residual_fields,
            compound_index_candidate,
            exact_key_hashes,
            suppressed_fields,
        }
    }

    pub(in crate::db) fn retained_bytes_estimate(&self) -> u64 {
        let labels = self
            .entity
            .len()
            .saturating_add(self.selected_index.as_ref().map_or(0, String::len))
            .saturating_add(self.residual_fields.iter().map(String::len).sum::<usize>())
            .saturating_add(
                self.compound_index_candidate
                    .iter()
                    .map(String::len)
                    .sum::<usize>(),
            );
        u64::try_from(labels).unwrap_or(u64::MAX).saturating_add(
            u64::try_from(self.exact_key_hashes.len())
                .unwrap_or(u64::MAX)
                .saturating_mul(16),
        )
    }

    pub(in crate::db) fn work_steps_estimate(&self) -> u64 {
        1_u64
            .saturating_add(u64::try_from(self.exact_key_hashes.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(self.residual_fields.len()).unwrap_or(u64::MAX))
            .saturating_add(u64::try_from(self.compound_index_candidate.len()).unwrap_or(u64::MAX))
    }
}

fn bounded_label(value: &str, suppressed: &mut u64) -> Option<String> {
    if value.len() > MAX_REQUEST_DIAGNOSTIC_LABEL_BYTES {
        *suppressed = suppressed.saturating_add(1);
        return None;
    }

    Some(value.to_string())
}

fn bounded_labels(values: Vec<&str>, suppressed: &mut u64) -> Vec<String> {
    let mut bounded = Vec::new();
    for value in values {
        if bounded.len() >= MAX_REQUEST_DIAGNOSTIC_FIELDS {
            *suppressed = suppressed.saturating_add(1);
            continue;
        }
        let Some(value) = bounded_label(value, suppressed) else {
            continue;
        };
        if !bounded.contains(&value) {
            bounded.push(value);
        }
    }
    bounded
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::db) struct RequestDiagnosticResourceUsage {
    pub(in crate::db) keys_visited: u64,
    pub(in crate::db) rows_visited: u64,
    pub(in crate::db) rows_returned: u64,
    pub(in crate::db) stored_bytes_read: u64,
    pub(in crate::db) decoded_bytes: u64,
    pub(in crate::db) materialized_bytes: u64,
    pub(in crate::db) result_bytes: u64,
}

#[derive(Debug)]
struct ShapeAccumulator {
    public: RequestQueryShapeDiagnostic,
    key_counts: Vec<([u8; 16], u64)>,
}

impl ShapeAccumulator {
    const fn new(
        normalized_shape_fingerprint_prefix: u64,
        access_path: RequestDiagnosticAccessPath,
    ) -> Self {
        Self {
            public: RequestQueryShapeDiagnostic {
                normalized_shape_fingerprint_prefix,
                entity: String::new(),
                access_path,
                selected_index: None,
                executions: 0,
                plan_cache_hits: 0,
                plan_cache_misses: 0,
                plan_compilations: 0,
                keys_visited: 0,
                rows_visited: 0,
                rows_returned: 0,
                stored_bytes_read: 0,
                decoded_bytes: 0,
                materialized_bytes: 0,
                result_bytes: 0,
                exact_key_lookups: 0,
                repeated_key_lookups: 0,
                hottest_key_lookups: 0,
                residual_fields: Vec::new(),
                compound_index_candidate: Vec::new(),
            },
            key_counts: Vec::new(),
        }
    }

    fn apply_plan_evidence(&mut self, evidence: &RequestQueryPlanEvidence) {
        self.public.entity.clone_from(&evidence.entity);
        self.public.access_path = evidence.access_path;
        self.public
            .selected_index
            .clone_from(&evidence.selected_index);
        self.public
            .residual_fields
            .clone_from(&evidence.residual_fields);
        self.public
            .compound_index_candidate
            .clone_from(&evidence.compound_index_candidate);
    }

    const fn apply_usage(&mut self, usage: RequestDiagnosticResourceUsage) {
        self.public.executions = self.public.executions.saturating_add(1);
        self.public.keys_visited = self.public.keys_visited.saturating_add(usage.keys_visited);
        self.public.rows_visited = self.public.rows_visited.saturating_add(usage.rows_visited);
        self.public.rows_returned = self
            .public
            .rows_returned
            .saturating_add(usage.rows_returned);
        self.public.stored_bytes_read = self
            .public
            .stored_bytes_read
            .saturating_add(usage.stored_bytes_read);
        self.public.decoded_bytes = self
            .public
            .decoded_bytes
            .saturating_add(usage.decoded_bytes);
        self.public.materialized_bytes = self
            .public
            .materialized_bytes
            .saturating_add(usage.materialized_bytes);
        self.public.result_bytes = self.public.result_bytes.saturating_add(usage.result_bytes);
    }
}

#[derive(Debug, Default)]
pub(in crate::db) struct RequestDiagnosticsState {
    shapes: Vec<ShapeAccumulator>,
    retained_key_identities: usize,
    overflowed_shapes: u64,
    overflowed_key_identities: u64,
    suppressed_observations: u64,
}

impl RequestDiagnosticsState {
    fn shape_mut(
        &mut self,
        prefix: u64,
        access_path: RequestDiagnosticAccessPath,
    ) -> Option<&mut ShapeAccumulator> {
        if let Some(index) = self
            .shapes
            .iter()
            .position(|shape| shape.public.normalized_shape_fingerprint_prefix == prefix)
        {
            return self.shapes.get_mut(index);
        }
        if self.shapes.len() >= MAX_REQUEST_DIAGNOSTIC_SHAPES {
            self.overflowed_shapes = self.overflowed_shapes.saturating_add(1);
            self.suppressed_observations = self.suppressed_observations.saturating_add(1);
            return None;
        }

        self.shapes.push(ShapeAccumulator::new(prefix, access_path));
        self.shapes.last_mut()
    }

    pub(in crate::db) const fn suppress(&mut self, count: u64) {
        self.suppressed_observations = self.suppressed_observations.saturating_add(count);
    }

    pub(in crate::db) fn observe_plan(
        &mut self,
        evidence: RequestQueryPlanEvidence,
        cache_hits: u64,
        cache_misses: u64,
    ) {
        self.suppress(evidence.suppressed_fields);
        let key_hashes = evidence.exact_key_hashes.clone();
        let prefix = evidence.normalized_shape_fingerprint_prefix;
        let Some(shape) = self.shape_mut(prefix, evidence.access_path) else {
            return;
        };
        shape.apply_plan_evidence(&evidence);
        shape.public.plan_cache_hits = shape.public.plan_cache_hits.saturating_add(cache_hits);
        shape.public.plan_cache_misses =
            shape.public.plan_cache_misses.saturating_add(cache_misses);
        shape.public.plan_compilations =
            shape.public.plan_compilations.saturating_add(cache_misses);
        self.observe_exact_key_hashes(prefix, key_hashes.as_slice());
    }

    pub(in crate::db) fn observe_execution(
        &mut self,
        prefix: u64,
        usage: RequestDiagnosticResourceUsage,
    ) {
        let Some(shape) = self.shape_mut(prefix, RequestDiagnosticAccessPath::Direct) else {
            return;
        };
        shape.apply_usage(usage);
    }

    pub(in crate::db) fn observe_exact_key_hashes(&mut self, prefix: u64, hashes: &[[u8; 16]]) {
        let Some(shape_index) = self
            .shapes
            .iter()
            .position(|shape| shape.public.normalized_shape_fingerprint_prefix == prefix)
        else {
            let Some(_) = self.shape_mut(prefix, RequestDiagnosticAccessPath::ByKeys) else {
                return;
            };
            return self.observe_exact_key_hashes(prefix, hashes);
        };

        for hash in hashes {
            let existing = self.shapes[shape_index]
                .key_counts
                .iter()
                .position(|(candidate, _)| candidate == hash);
            if let Some(key_index) = existing {
                let shape = &mut self.shapes[shape_index];
                let count = shape.key_counts[key_index].1.saturating_add(1);
                shape.key_counts[key_index].1 = count;
                shape.public.repeated_key_lookups =
                    shape.public.repeated_key_lookups.saturating_add(1);
                shape.public.hottest_key_lookups = shape.public.hottest_key_lookups.max(count);
            } else if self.retained_key_identities < MAX_REQUEST_DIAGNOSTIC_KEY_IDENTITIES {
                let shape = &mut self.shapes[shape_index];
                shape.key_counts.push((*hash, 1));
                shape.public.hottest_key_lookups = shape.public.hottest_key_lookups.max(1);
                self.retained_key_identities = self.retained_key_identities.saturating_add(1);
            } else {
                self.overflowed_key_identities = self.overflowed_key_identities.saturating_add(1);
                self.suppressed_observations = self.suppressed_observations.saturating_add(1);
            }
            self.shapes[shape_index].public.exact_key_lookups = self.shapes[shape_index]
                .public
                .exact_key_lookups
                .saturating_add(1);
        }
    }

    pub(in crate::db) fn snapshot(&self) -> RequestDiagnostics {
        let shapes = self
            .shapes
            .iter()
            .map(|shape| shape.public.clone())
            .collect::<Vec<_>>();
        let warnings = shapes
            .iter()
            .flat_map(warnings_for_shape)
            .collect::<Vec<_>>();

        RequestDiagnostics {
            shapes,
            warnings,
            overflowed_shapes: self.overflowed_shapes,
            overflowed_key_identities: self.overflowed_key_identities,
            suppressed_observations: self.suppressed_observations,
        }
    }
}

fn warnings_for_shape(shape: &RequestQueryShapeDiagnostic) -> Vec<RequestDiagnosticWarning> {
    let mut warnings = Vec::new();
    if shape.executions >= REPEATED_SHAPE_WARNING_THRESHOLD {
        let action = if shape.exact_key_lookups != 0 {
            " Consider one bounded get_many preload and reuse its rows within the request."
        } else {
            " Consider hoisting or batching this repeated query."
        };
        warnings.push(RequestDiagnosticWarning {
            kind: RequestDiagnosticWarningKind::RepeatedQueryShape,
            normalized_shape_fingerprint_prefix: shape.normalized_shape_fingerprint_prefix,
            observed: shape.executions,
            message: format!(
                "Normalized {} query shape executed {} times.{}",
                diagnostic_entity_label(shape),
                shape.executions,
                action,
            ),
        });
    }
    if shape.hottest_key_lookups > 1 {
        warnings.push(RequestDiagnosticWarning {
            kind: RequestDiagnosticWarningKind::RepeatedExactKey,
            normalized_shape_fingerprint_prefix: shape.normalized_shape_fingerprint_prefix,
            observed: shape.hottest_key_lookups,
            message: format!(
                "The same hashed {} key was looked up {} times; preload shared rows once.",
                diagnostic_entity_label(shape),
                shape.hottest_key_lookups,
            ),
        });
    }
    if !shape.compound_index_candidate.is_empty() {
        warnings.push(RequestDiagnosticWarning {
            kind: RequestDiagnosticWarningKind::CompoundIndexCandidate,
            normalized_shape_fingerprint_prefix: shape.normalized_shape_fingerprint_prefix,
            observed: shape.rows_visited,
            message: format!(
                "Residual work on {} suggests the compound index prefix [{}].",
                diagnostic_entity_label(shape),
                shape.compound_index_candidate.join(", "),
            ),
        });
    }

    warnings
}

const fn diagnostic_entity_label(shape: &RequestQueryShapeDiagnostic) -> &str {
    if shape.entity.is_empty() {
        "direct"
    } else {
        shape.entity.as_str()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn evidence(prefix: u64, key: [u8; 16]) -> RequestQueryPlanEvidence {
        RequestQueryPlanEvidence::bounded(
            prefix,
            "GeneratorExportSnapshot",
            RequestDiagnosticAccessPath::ByKey,
            None,
            Vec::new(),
            Vec::new(),
            vec![key],
        )
    }

    #[test]
    fn repeated_point_lookups_are_bounded_and_actionable_without_raw_keys() {
        let mut state = RequestDiagnosticsState::default();
        for _ in 0..200 {
            state.observe_plan(evidence(7, [3; 16]), 1, 0);
            state.observe_execution(7, RequestDiagnosticResourceUsage::default());
        }

        let snapshot = state.snapshot();
        assert_eq!(snapshot.shapes[0].executions, 200);
        assert_eq!(snapshot.shapes[0].hottest_key_lookups, 200);
        assert!(
            snapshot
                .warnings
                .iter()
                .any(|warning| warning.message.contains("get_many"))
        );
        let debug = format!("{snapshot:?}");
        assert!(!debug.contains("token-secret-literal"));
    }

    #[test]
    fn shape_and_key_overflow_remain_visible() {
        let mut state = RequestDiagnosticsState::default();
        for shape in 0..=MAX_REQUEST_DIAGNOSTIC_SHAPES {
            state.observe_execution(
                u64::try_from(shape).unwrap_or(u64::MAX),
                RequestDiagnosticResourceUsage::default(),
            );
        }
        for key in 0..=MAX_REQUEST_DIAGNOSTIC_KEY_IDENTITIES {
            let mut hash = [0_u8; 16];
            hash[..8].copy_from_slice(&u64::try_from(key).unwrap_or(u64::MAX).to_be_bytes());
            state.observe_exact_key_hashes(0, &[hash]);
        }

        let snapshot = state.snapshot();
        assert_eq!(snapshot.shapes.len(), MAX_REQUEST_DIAGNOSTIC_SHAPES);
        assert_eq!(snapshot.overflowed_shapes, 1);
        assert_eq!(snapshot.overflowed_key_identities, 1);
        assert!(snapshot.suppressed_observations >= 2);
    }

    #[test]
    fn compound_index_warning_names_only_bounded_fields() {
        let mut state = RequestDiagnosticsState::default();
        state.observe_plan(
            RequestQueryPlanEvidence::bounded(
                9,
                "Token",
                RequestDiagnosticAccessPath::IndexPrefix,
                Some("token_collection"),
                vec!["stage"],
                vec!["collection_id", "stage"],
                Vec::new(),
            ),
            0,
            1,
        );
        state.observe_execution(
            9,
            RequestDiagnosticResourceUsage {
                rows_visited: 600,
                ..RequestDiagnosticResourceUsage::default()
            },
        );

        let snapshot = state.snapshot();
        assert!(snapshot.warnings.iter().any(|warning| {
            warning.kind == RequestDiagnosticWarningKind::CompoundIndexCandidate
                && warning.message.contains("collection_id, stage")
        }));
    }
}
