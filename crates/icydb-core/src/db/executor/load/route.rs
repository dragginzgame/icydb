use crate::{
    db::{
        executor::{
            fold::{AggregateFoldMode, AggregateKind},
            load::{IndexRangeLimitSpec, LoadExecutor},
        },
        index::RawIndexKey,
        query::plan::{
            AccessPath, AccessPlan, CursorBoundary, Direction, LogicalPlan, compute_page_window,
            validate::PushdownApplicability,
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// ExecutionMode
///
/// Canonical route-level execution shape selected by the routing gate.
/// Keeps streaming-vs-materialized decisions explicit and testable.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum ExecutionMode {
    Streaming,
    Materialized,
}

///
/// ScanHintPlan
///
/// Canonical scan-hint payload produced by route planning.
/// Keeps bounded fetch/budget hints under one boundary.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(super) struct ScanHintPlan {
    pub(super) physical_fetch_hint: Option<usize>,
    pub(super) load_scan_budget_hint: Option<usize>,
}

///
/// ExecutionRoutePlan
///
/// Canonical route decision payload for load/aggregate execution.
/// This is the single boundary that owns execution mode, pushdown eligibility,
/// DESC physical reverse-traversal capability, and scan-hint decisions.
///

#[derive(Clone)]
pub(super) struct ExecutionRoutePlan {
    pub(super) execution_mode: ExecutionMode,
    pub(super) secondary_pushdown_applicability: PushdownApplicability,
    pub(super) index_range_limit_spec: Option<IndexRangeLimitSpec>,
    capabilities: RouteCapabilities,
    pub(super) scan_hints: ScanHintPlan,
    pub(super) aggregate_fold_mode: AggregateFoldMode,
}

impl ExecutionRoutePlan {
    // Return the effective physical fetch hint for fallback stream resolution.
    // DESC fallback must disable bounded hints when reverse traversal is unavailable.
    pub(super) const fn fallback_physical_fetch_hint(&self, direction: Direction) -> Option<usize> {
        if direction_allows_physical_fetch_hint(direction, self.desc_physical_reverse_supported()) {
            self.scan_hints.physical_fetch_hint
        } else {
            None
        }
    }

    // True when DESC execution can traverse the physical access path in reverse.
    pub(super) const fn desc_physical_reverse_supported(&self) -> bool {
        self.capabilities.desc_physical_reverse_supported
    }

    // True when secondary-prefix pushdown is enabled for this route.
    pub(super) const fn secondary_fast_path_eligible(&self) -> bool {
        self.secondary_pushdown_applicability.is_eligible()
    }

    // True when index-range limit pushdown is enabled for this route.
    pub(super) const fn index_range_limit_fast_path_enabled(&self) -> bool {
        self.index_range_limit_spec.is_some()
    }

    // True when composite aggregate fast-path execution is shape-safe.
    pub(super) const fn composite_aggregate_fast_path_eligible(&self) -> bool {
        self.capabilities.composite_aggregate_fast_path_eligible
    }
}

///
/// FastPathOrder
///
/// Shared fast-path precedence model used by load and aggregate routing.
/// Routing implementations remain separate, but they iterate one canonical order.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum FastPathOrder {
    PrimaryKey,
    SecondaryPrefix,
    PrimaryScan,
    IndexRange,
    Composite,
}

pub(super) const LOAD_FAST_PATH_ORDER: [FastPathOrder; 3] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::IndexRange,
];

pub(super) const AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 5] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::PrimaryScan,
    FastPathOrder::IndexRange,
    FastPathOrder::Composite,
];

///
/// RouteIntent
///

enum RouteIntent {
    Load {
        direction: Direction,
    },
    Aggregate {
        direction: Direction,
        kind: AggregateKind,
    },
}

///
/// RouteCapabilities
///
/// Canonical derived capability snapshot for one logical plan and direction.
/// Route planning derives this once, then consumes it for eligibility and hint
/// decisions to reduce drift across helpers.
///

#[allow(clippy::struct_excessive_bools)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RouteCapabilities {
    streaming_access_shape_safe: bool,
    desc_physical_reverse_supported: bool,
    count_pushdown_access_shape_supported: bool,
    index_range_limit_pushdown_shape_eligible: bool,
    composite_aggregate_fast_path_eligible: bool,
    bounded_probe_hint_safe: bool,
}

const fn direction_allows_physical_fetch_hint(
    direction: Direction,
    desc_physical_reverse_supported: bool,
) -> bool {
    !matches!(direction, Direction::Desc) || desc_physical_reverse_supported
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Derive a canonical route capability snapshot for one plan + direction.
    fn derive_route_capabilities(
        plan: &LogicalPlan<E::Key>,
        direction: Direction,
    ) -> RouteCapabilities {
        RouteCapabilities {
            streaming_access_shape_safe: plan.is_streaming_access_shape_safe::<E>(),
            desc_physical_reverse_supported: Self::is_desc_physical_reverse_traversal_supported(
                &plan.access,
                direction,
            ),
            count_pushdown_access_shape_supported: Self::count_pushdown_access_shape_supported(
                &plan.access,
            ),
            index_range_limit_pushdown_shape_eligible:
                Self::is_index_range_limit_pushdown_shape_eligible(plan),
            composite_aggregate_fast_path_eligible: Self::is_composite_aggregate_fast_path_eligible(
                plan,
            ),
            bounded_probe_hint_safe: Self::bounded_probe_hint_is_safe(plan),
        }
    }

    // Build canonical execution routing for load execution.
    pub(super) fn build_execution_route_plan_for_load(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
        direction: Direction,
    ) -> Result<ExecutionRoutePlan, InternalError> {
        Self::validate_pk_fast_path_boundary_if_applicable(plan, cursor_boundary)?;

        Ok(Self::build_execution_route_plan(
            plan,
            cursor_boundary,
            index_range_anchor,
            probe_fetch_hint,
            RouteIntent::Load { direction },
        ))
    }

    // Build canonical execution routing for aggregate execution.
    pub(super) fn build_execution_route_plan_for_aggregate(
        plan: &LogicalPlan<E::Key>,
        kind: AggregateKind,
        direction: Direction,
    ) -> ExecutionRoutePlan {
        Self::build_execution_route_plan(
            plan,
            None,
            None,
            None,
            RouteIntent::Aggregate { direction, kind },
        )
    }

    // Shared route gate for load + aggregate execution.
    fn build_execution_route_plan(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
        intent: RouteIntent,
    ) -> ExecutionRoutePlan {
        let secondary_pushdown_applicability =
            crate::db::query::plan::validate::assess_secondary_order_pushdown_if_applicable_validated(
                E::MODEL,
                plan,
            );
        let (direction, kind) = match intent {
            RouteIntent::Load { direction } => (direction, None),
            RouteIntent::Aggregate { direction, kind } => (direction, Some(kind)),
        };
        let capabilities = Self::derive_route_capabilities(plan, direction);
        let count_pushdown_eligible = kind.is_some_and(|aggregate_kind| {
            Self::is_count_pushdown_eligible(aggregate_kind, capabilities)
        });

        // Aggregate probes must not assume DESC physical reverse traversal
        // when the access shape cannot emit descending order natively.
        let count_pushdown_probe_fetch_hint = if count_pushdown_eligible {
            Self::count_pushdown_fetch_hint(plan, capabilities)
        } else {
            None
        };
        let aggregate_terminal_probe_fetch_hint = kind.and_then(|aggregate_kind| {
            Self::aggregate_probe_fetch_hint(plan, aggregate_kind, direction, capabilities)
        });
        let aggregate_physical_fetch_hint =
            count_pushdown_probe_fetch_hint.or(aggregate_terminal_probe_fetch_hint);
        let physical_fetch_hint = match kind {
            Some(_) => aggregate_physical_fetch_hint,
            None => probe_fetch_hint,
        };
        let load_scan_budget_hint = match intent {
            RouteIntent::Load { .. } => {
                Self::load_scan_budget_hint(plan, cursor_boundary, capabilities)
            }
            RouteIntent::Aggregate { .. } => None,
        };

        let index_range_limit_spec = Self::assess_index_range_limit_pushdown(
            plan,
            cursor_boundary,
            index_range_anchor,
            physical_fetch_hint,
            capabilities,
        );
        debug_assert!(
            index_range_limit_spec.is_none()
                || capabilities.index_range_limit_pushdown_shape_eligible,
            "route invariant: index-range limit spec requires pushdown-eligible shape",
        );
        debug_assert!(
            !count_pushdown_eligible
                || matches!(kind, Some(AggregateKind::Count))
                    && capabilities.streaming_access_shape_safe
                    && capabilities.count_pushdown_access_shape_supported,
            "route invariant: COUNT pushdown eligibility must match COUNT-safe capability set",
        );
        debug_assert!(
            load_scan_budget_hint.is_none()
                || cursor_boundary.is_none() && capabilities.streaming_access_shape_safe,
            "route invariant: load scan-budget hints require non-continuation streaming-safe shape",
        );
        let aggregate_fold_mode = if count_pushdown_eligible {
            AggregateFoldMode::KeysOnly
        } else {
            AggregateFoldMode::ExistingRows
        };
        let execution_mode = if let Some(_aggregate_kind) = kind {
            if count_pushdown_eligible
                || capabilities.streaming_access_shape_safe
                || secondary_pushdown_applicability.is_eligible()
                || index_range_limit_spec.is_some()
            {
                ExecutionMode::Streaming
            } else {
                ExecutionMode::Materialized
            }
        } else if capabilities.streaming_access_shape_safe {
            ExecutionMode::Streaming
        } else {
            ExecutionMode::Materialized
        };
        debug_assert!(
            kind.is_none()
                || index_range_limit_spec.is_none()
                || matches!(execution_mode, ExecutionMode::Streaming),
            "route invariant: aggregate index-range limit pushdown must execute in streaming mode",
        );

        ExecutionRoutePlan {
            execution_mode,
            secondary_pushdown_applicability,
            index_range_limit_spec,
            capabilities,
            scan_hints: ScanHintPlan {
                physical_fetch_hint,
                load_scan_budget_hint,
            },
            aggregate_fold_mode,
        }
    }

    // Assess index-range limit pushdown once for this execution and produce
    // the bounded fetch spec when all eligibility gates pass.
    fn assess_index_range_limit_pushdown(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        index_range_anchor: Option<&RawIndexKey>,
        probe_fetch_hint: Option<usize>,
        capabilities: RouteCapabilities,
    ) -> Option<IndexRangeLimitSpec> {
        if !capabilities.index_range_limit_pushdown_shape_eligible {
            return None;
        }
        if cursor_boundary.is_some() && index_range_anchor.is_none() {
            return None;
        }
        if let Some(fetch) = probe_fetch_hint {
            return Some(IndexRangeLimitSpec { fetch });
        }

        let page = plan.page.as_ref()?;
        let limit = page.limit?;
        if limit == 0 {
            return Some(IndexRangeLimitSpec { fetch: 0 });
        }

        let fetch = Self::page_window_fetch_count(plan, cursor_boundary, true)?;

        Some(IndexRangeLimitSpec { fetch })
    }

    // Shared load-page scan-budget hint gate.
    fn load_scan_budget_hint(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if cursor_boundary.is_some() {
            return None;
        }
        if !capabilities.streaming_access_shape_safe {
            return None;
        }

        Self::page_window_fetch_count(plan, cursor_boundary, true)
    }

    // Shared bounded-probe safety gate for aggregate key-stream hints.
    // DISTINCT + offset must remain unbounded so deduplication happens before
    // offset consumption without risking short windows.
    fn bounded_probe_hint_is_safe(plan: &LogicalPlan<E::Key>) -> bool {
        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        !(plan.distinct && offset > 0)
    }

    const fn count_pushdown_path_shape_supported(path: &AccessPath<E::Key>) -> bool {
        matches!(path, AccessPath::FullScan | AccessPath::KeyRange { .. })
    }

    fn count_pushdown_access_shape_supported(access: &AccessPlan<E::Key>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::count_pushdown_path_shape_supported(path),
            AccessPlan::Union(_) | AccessPlan::Intersection(_) => false,
        }
    }

    const fn is_count_pushdown_eligible(
        kind: AggregateKind,
        capabilities: RouteCapabilities,
    ) -> bool {
        matches!(kind, AggregateKind::Count)
            && capabilities.streaming_access_shape_safe
            && capabilities.count_pushdown_access_shape_supported
    }

    fn count_pushdown_fetch_hint(
        plan: &LogicalPlan<E::Key>,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        Self::page_window_fetch_count(plan, None, false)
    }

    fn aggregate_probe_fetch_hint(
        plan: &LogicalPlan<E::Key>,
        kind: AggregateKind,
        direction: Direction,
        capabilities: RouteCapabilities,
    ) -> Option<usize> {
        if !matches!(
            kind,
            AggregateKind::Exists
                | AggregateKind::Min
                | AggregateKind::Max
                | AggregateKind::First
                | AggregateKind::Last
        ) {
            return None;
        }
        if plan.page.as_ref().is_some_and(|page| page.limit == Some(0)) {
            return Some(0);
        }
        if !direction_allows_physical_fetch_hint(
            direction,
            capabilities.desc_physical_reverse_supported,
        ) {
            return None;
        }
        if !capabilities.bounded_probe_hint_safe {
            return None;
        }

        let offset = usize::try_from(plan.effective_page_offset(None)).unwrap_or(usize::MAX);
        let page_limit = plan
            .page
            .as_ref()
            .and_then(|page| page.limit)
            .map(|limit| usize::try_from(limit).unwrap_or(usize::MAX));

        match kind {
            AggregateKind::Exists | AggregateKind::First => Some(offset.saturating_add(1)),
            AggregateKind::Min if direction == Direction::Asc => Some(offset.saturating_add(1)),
            AggregateKind::Max if direction == Direction::Desc => Some(offset.saturating_add(1)),
            AggregateKind::Last => page_limit.map(|limit| offset.saturating_add(limit)),
            _ => None,
        }
    }

    fn is_desc_physical_reverse_traversal_supported(
        access: &AccessPlan<E::Key>,
        direction: Direction,
    ) -> bool {
        if !matches!(direction, Direction::Desc) {
            return false;
        }

        Self::access_supports_reverse_traversal(access)
    }

    fn access_supports_reverse_traversal(access: &AccessPlan<E::Key>) -> bool {
        match access {
            AccessPlan::Path(path) => Self::path_supports_reverse_traversal(path),
            AccessPlan::Union(children) | AccessPlan::Intersection(children) => {
                children.iter().all(Self::access_supports_reverse_traversal)
            }
        }
    }

    // Composite aggregate fast-path eligibility must stay explicit:
    // - composite access shape only (`Union` / `Intersection`)
    // - no residual predicate filtering
    // - no post-access reordering
    fn is_composite_aggregate_fast_path_eligible(plan: &LogicalPlan<E::Key>) -> bool {
        if !Self::is_composite_access_shape(&plan.access) {
            return false;
        }

        let metadata = plan.budget_safety_metadata::<E>();
        if metadata.has_residual_filter {
            return false;
        }
        if metadata.requires_post_access_sort {
            return false;
        }

        true
    }

    // Shared page-window fetch computation for bounded routing hints.
    fn page_window_fetch_count(
        plan: &LogicalPlan<E::Key>,
        cursor_boundary: Option<&CursorBoundary>,
        needs_extra: bool,
    ) -> Option<usize> {
        let page = plan.page.as_ref()?;
        let limit = page.limit?;

        Some(
            compute_page_window(
                plan.effective_page_offset(cursor_boundary),
                limit,
                needs_extra,
            )
            .fetch_count,
        )
    }

    const fn path_supports_reverse_traversal(path: &AccessPath<E::Key>) -> bool {
        matches!(
            path,
            AccessPath::ByKey(_)
                | AccessPath::KeyRange { .. }
                | AccessPath::IndexPrefix { .. }
                | AccessPath::IndexRange { .. }
                | AccessPath::FullScan
        )
    }

    const fn is_composite_access_shape(access: &AccessPlan<E::Key>) -> bool {
        matches!(access, AccessPlan::Union(_) | AccessPlan::Intersection(_))
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{AGGREGATE_FAST_PATH_ORDER, ExecutionMode, FastPathOrder, LOAD_FAST_PATH_ORDER};
    use crate::{
        db::{
            executor::{
                fold::{AggregateFoldMode, AggregateKind},
                load::LoadExecutor,
            },
            query::{
                ReadConsistency,
                plan::{
                    AccessPath, CursorBoundary, Direction, LogicalPlan, OrderDirection, OrderSpec,
                    PageSpec,
                },
            },
        },
        model::{field::FieldKind, index::IndexModel},
        traits::Path,
        types::Ulid,
        value::Value,
    };
    use icydb_derive::FieldValues;
    use serde::{Deserialize, Serialize};
    use std::ops::Bound;

    crate::test_canister! {
        ident = RouteMatrixCanister,
    }

    crate::test_store! {
        ident = RouteMatrixStore,
        canister = RouteMatrixCanister,
    }

    static ROUTE_MATRIX_INDEX_FIELDS: [&str; 1] = ["rank"];
    static ROUTE_MATRIX_INDEX_MODELS: [IndexModel; 1] = [IndexModel::new(
        "rank_idx",
        RouteMatrixStore::PATH,
        &ROUTE_MATRIX_INDEX_FIELDS,
        false,
    )];

    #[derive(Clone, Debug, Default, Deserialize, FieldValues, PartialEq, Serialize)]
    struct RouteMatrixEntity {
        id: Ulid,
        rank: u32,
        label: String,
    }

    crate::test_entity_schema! {
        ident = RouteMatrixEntity,
        id = Ulid,
        id_field = id,
        entity_name = "RouteMatrixEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("rank", FieldKind::Uint),
            ("label", FieldKind::Text),
        ],
        indexes = [&ROUTE_MATRIX_INDEX_MODELS[0]],
        store = RouteMatrixStore,
        canister = RouteMatrixCanister,
    }

    #[test]
    fn load_fast_path_order_matches_expected_precedence() {
        assert_eq!(
            LOAD_FAST_PATH_ORDER,
            [
                FastPathOrder::PrimaryKey,
                FastPathOrder::SecondaryPrefix,
                FastPathOrder::IndexRange,
            ],
            "load fast-path precedence must stay stable"
        );
    }

    #[test]
    fn aggregate_fast_path_order_matches_expected_precedence() {
        assert_eq!(
            AGGREGATE_FAST_PATH_ORDER,
            [
                FastPathOrder::PrimaryKey,
                FastPathOrder::SecondaryPrefix,
                FastPathOrder::PrimaryScan,
                FastPathOrder::IndexRange,
                FastPathOrder::Composite,
            ],
            "aggregate fast-path precedence must stay stable"
        );
    }

    #[test]
    fn route_capabilities_full_scan_desc_pk_order_reflect_expected_flags() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 2,
        });
        let capabilities =
            LoadExecutor::<RouteMatrixEntity>::derive_route_capabilities(&plan, Direction::Desc);

        assert!(capabilities.streaming_access_shape_safe);
        assert!(capabilities.desc_physical_reverse_supported);
        assert!(capabilities.count_pushdown_access_shape_supported);
        assert!(!capabilities.index_range_limit_pushdown_shape_eligible);
        assert!(!capabilities.composite_aggregate_fast_path_eligible);
        assert!(capabilities.bounded_probe_hint_safe);
    }

    #[test]
    fn route_capabilities_by_keys_desc_distinct_offset_disable_probe_hint() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::ByKeys(vec![
                Ulid::from_u128(7303),
                Ulid::from_u128(7301),
                Ulid::from_u128(7302),
            ]),
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.distinct = true;
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });
        let capabilities =
            LoadExecutor::<RouteMatrixEntity>::derive_route_capabilities(&plan, Direction::Desc);

        assert!(capabilities.streaming_access_shape_safe);
        assert!(!capabilities.desc_physical_reverse_supported);
        assert!(!capabilities.count_pushdown_access_shape_supported);
        assert!(!capabilities.index_range_limit_pushdown_shape_eligible);
        assert!(!capabilities.composite_aggregate_fast_path_eligible);
        assert!(!capabilities.bounded_probe_hint_safe);
    }

    #[test]
    fn route_matrix_load_pk_desc_with_page_uses_streaming_budget_and_reverse() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 2,
        });
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            None,
            None,
            None,
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(route_plan.desc_physical_reverse_supported());
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
        assert_eq!(route_plan.scan_hints.load_scan_budget_hint, Some(6));
        assert!(route_plan.index_range_limit_spec.is_none());
    }

    #[test]
    fn route_matrix_load_index_range_cursor_without_anchor_disables_pushdown() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexRange {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                prefix: vec![],
                lower: Bound::Included(Value::Uint(10)),
                upper: Bound::Excluded(Value::Uint(20)),
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Desc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 0,
        });
        let cursor = CursorBoundary { slots: Vec::new() };
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            Some(&cursor),
            None,
            None,
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
        assert!(route_plan.desc_physical_reverse_supported());
        assert!(route_plan.index_range_limit_spec.is_none());
        assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    }

    #[test]
    fn route_matrix_load_non_pk_order_disables_scan_budget_hint() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("rank".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 2,
        });
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            None,
            None,
            None,
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.execution_mode, ExecutionMode::Materialized);
        assert_eq!(route_plan.scan_hints.load_scan_budget_hint, None);
    }

    #[test]
    fn route_matrix_load_by_keys_desc_disables_fallback_fetch_hint_without_reverse_support() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::ByKeys(vec![
                Ulid::from_u128(7203),
                Ulid::from_u128(7201),
                Ulid::from_u128(7202),
            ]),
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        let route_plan = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_load(
            &plan,
            None,
            None,
            Some(4),
            Direction::Desc,
        )
        .expect("load route plan should build");

        assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(4));
        assert_eq!(
            route_plan.fallback_physical_fetch_hint(Direction::Desc),
            None
        );
        assert_eq!(
            route_plan.fallback_physical_fetch_hint(Direction::Asc),
            Some(4)
        );
    }

    #[test]
    fn route_matrix_aggregate_count_pk_order_is_streaming_keys_only() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(4),
            offset: 2,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Count,
                Direction::Asc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::KeysOnly
        ));
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(6));
    }

    #[test]
    fn route_matrix_aggregate_distinct_offset_last_disables_probe_hint() {
        let mut plan = LogicalPlan::new(AccessPath::<Ulid>::FullScan, ReadConsistency::MissingOk);
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.distinct = true;
        plan.page = Some(PageSpec {
            limit: Some(3),
            offset: 1,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Last,
                Direction::Desc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(matches!(
            route_plan.aggregate_fold_mode,
            AggregateFoldMode::ExistingRows
        ));
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    }

    #[test]
    fn route_matrix_aggregate_by_keys_desc_disables_probe_hint_without_reverse_support() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::ByKeys(vec![
                Ulid::from_u128(7103),
                Ulid::from_u128(7101),
                Ulid::from_u128(7102),
            ]),
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::First,
                Direction::Desc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(!route_plan.desc_physical_reverse_supported());
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, None);
    }

    #[test]
    fn route_matrix_aggregate_secondary_extrema_probe_hints_lock_offset_plus_one() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexPrefix {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                values: vec![Value::Uint(7)],
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Asc),
                ("id".to_string(), OrderDirection::Asc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: None,
            offset: 2,
        });

        let min_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
            Direction::Asc,
        );
        let max_asc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Max,
            Direction::Asc,
        );
        assert_eq!(min_asc.scan_hints.physical_fetch_hint, Some(3));
        assert_eq!(max_asc.scan_hints.physical_fetch_hint, None);

        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Desc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });
        let max_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Max,
            Direction::Desc,
        );
        let min_desc = LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
            &plan,
            AggregateKind::Min,
            Direction::Desc,
        );
        assert_eq!(max_desc.scan_hints.physical_fetch_hint, Some(3));
        assert_eq!(min_desc.scan_hints.physical_fetch_hint, None);
    }

    #[test]
    fn route_matrix_aggregate_index_range_desc_with_window_enables_pushdown_hint() {
        let mut plan = LogicalPlan::new(
            AccessPath::<Ulid>::IndexRange {
                index: ROUTE_MATRIX_INDEX_MODELS[0],
                prefix: vec![],
                lower: Bound::Included(Value::Uint(10)),
                upper: Bound::Excluded(Value::Uint(30)),
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![
                ("rank".to_string(), OrderDirection::Desc),
                ("id".to_string(), OrderDirection::Desc),
            ],
        });
        plan.page = Some(PageSpec {
            limit: Some(2),
            offset: 1,
        });
        let route_plan =
            LoadExecutor::<RouteMatrixEntity>::build_execution_route_plan_for_aggregate(
                &plan,
                AggregateKind::Last,
                Direction::Desc,
            );

        assert_eq!(route_plan.execution_mode, ExecutionMode::Streaming);
        assert!(route_plan.desc_physical_reverse_supported());
        assert_eq!(route_plan.scan_hints.physical_fetch_hint, Some(3));
        assert_eq!(
            route_plan.index_range_limit_spec.map(|spec| spec.fetch),
            Some(3)
        );
    }
}
