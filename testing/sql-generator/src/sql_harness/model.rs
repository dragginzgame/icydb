//! Module: sql_harness::model
//!
//! Responsibility: typed, test-only scenario and route facts shared by SQL evidence runners.
//! Does not own: SQL semantics, production planning, or classification derived from SQL text.
//! Boundary: requires scenario producers to declare evidence intent independently of SQL payloads.

use crate::{
    EligibleProvider, EvidenceStrength, MutationKind, NullabilityClass, PredicateFamily,
    QueryShape, RouteFamily, StatementFamily, ValueTypeFamily, WindowBehavior,
};

///
/// RowOrder
///
/// Whether row position is part of a normalized result contract.
/// Owned by the shared SQL harness and consumed by result normalization.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RowOrder {
    Ordered,
    Unordered,
}

///
/// ExpectedAcceptance
///
/// Expected admission outcome and typed rejection identity for one scenario.
/// Owned by the shared SQL harness and interpreted by correctness verdicts.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExpectedAcceptance {
    Accepted,
    Rejected {
        error_code: u16,
        diagnostic_code: u16,
    },
}

///
/// WindowSpec
///
/// Declared ordering and read-window facts used to classify one SQL scenario.
/// Owned by the shared SQL harness and consumed by route evidence and selection.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WindowSpec {
    /// Coarse ordering and bound behavior.
    pub behavior: WindowBehavior,

    /// Requested row limit, when the scenario is bounded.
    pub limit: Option<usize>,

    /// Number of ordered rows skipped before the requested window.
    pub offset: usize,

    /// Human-readable declared order used only in evidence reports.
    pub order_hint: Option<&'static str>,
}

impl WindowSpec {
    /// Unordered and unbounded window declaration.
    pub const NONE: Self = Self {
        behavior: WindowBehavior::None,
        limit: None,
        offset: 0,
        order_hint: None,
    };

    /// Build an unordered bounded window declaration.
    #[must_use]
    pub const fn limit(limit: usize) -> Self {
        Self {
            behavior: WindowBehavior::Limit,
            limit: Some(limit),
            offset: 0,
            order_hint: None,
        }
    }

    /// Build an ordered bounded window declaration.
    #[must_use]
    pub const fn ordered(limit: usize, offset: usize, order_hint: &'static str) -> Self {
        Self {
            behavior: if offset == 0 {
                WindowBehavior::OrderedLimit
            } else {
                WindowBehavior::OrderedLimitOffset
            },
            limit: Some(limit),
            offset,
            order_hint: Some(order_hint),
        }
    }

    /// Build an ordered window without a row bound.
    #[must_use]
    pub const fn ordered_unbounded(order_hint: &'static str) -> Self {
        Self {
            behavior: WindowBehavior::Ordered,
            limit: None,
            offset: 0,
            order_hint: Some(order_hint),
        }
    }

    /// Return the maximum successful row reads that prove bounded early stopping.
    #[must_use]
    pub fn read_bound(self) -> Option<u64> {
        let limit = self.limit?;
        u64::try_from(limit.saturating_add(self.offset).saturating_add(1)).ok()
    }
}

///
/// RouteOutcome
///
/// Result of applying an execution route to the declared query window.
/// Owned by the shared SQL harness and used in route evidence signatures.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RouteOutcome {
    EligibleButNotPushed,
    Materialized,
    MissingTieBreaker,
    Pushed,
    ResidualUnbounded,
    UnchangedOrNotApplicable,
    Unsupported,
}

impl RouteOutcome {
    /// Return the stable report code for this route outcome.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::EligibleButNotPushed => "eligible_but_not_pushed",
            Self::Materialized => "materialized",
            Self::MissingTieBreaker => "missing_tie_breaker",
            Self::Pushed => "pushed",
            Self::ResidualUnbounded => "residual_unbounded",
            Self::UnchangedOrNotApplicable => "unchanged_or_not_applicable",
            Self::Unsupported => "unsupported",
        }
    }
}

///
/// RouteReason
///
/// Typed reason explaining why a route produced its observed outcome.
/// Owned by the shared SQL harness and serialized into runner diagnostics.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum RouteReason {
    EqualityPrefixOrderedSuffixCandidate,
    EqualityPrefixOrderedSuffixLimitStopProven,
    FilterOrderMismatch,
    GroupedAggregateOwnsExecution,
    IndexOrderSuffixGap,
    NoOrderBy,
    NotAPaginatedSelect,
    OrderExpressionNotClassified,
    PrimaryOrderCandidate,
    PrimaryOrderLimitStopProven,
    RequiresMaterializedSort,
    ResidualFilterRequiresCandidateScan,
    SecondaryOrderCandidate,
    SecondaryOrderLimitStopProven,
    StorageMirrorHasPrimaryIndexOnly,
    StorageMirrorPrimaryOrderCandidate,
}

impl RouteReason {
    /// Return the stable report code for this route reason.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::EqualityPrefixOrderedSuffixCandidate => {
                "equality_prefix_ordered_suffix_candidate"
            }
            Self::EqualityPrefixOrderedSuffixLimitStopProven => {
                "equality_prefix_ordered_suffix_limit_stop_proven"
            }
            Self::FilterOrderMismatch => "filter_order_mismatch",
            Self::GroupedAggregateOwnsExecution => "grouped_aggregate_owns_execution",
            Self::IndexOrderSuffixGap => "index_order_suffix_gap",
            Self::NoOrderBy => "no_order_by",
            Self::NotAPaginatedSelect => "not_a_paginated_select",
            Self::OrderExpressionNotClassified => "order_expression_not_classified",
            Self::PrimaryOrderCandidate => "primary_order_candidate",
            Self::PrimaryOrderLimitStopProven => "primary_order_limit_stop_proven",
            Self::RequiresMaterializedSort => "requires_materialized_sort",
            Self::ResidualFilterRequiresCandidateScan => "residual_filter_requires_candidate_scan",
            Self::SecondaryOrderCandidate => "secondary_order_candidate",
            Self::SecondaryOrderLimitStopProven => "secondary_order_limit_stop_proven",
            Self::StorageMirrorHasPrimaryIndexOnly => "storage_mirror_has_primary_index_only",
            Self::StorageMirrorPrimaryOrderCandidate => "storage_mirror_primary_order_candidate",
        }
    }
}

///
/// RouteFact
///
/// Complete typed route identity used by scenario expectations and observations.
/// Owned by the shared SQL harness and consumed by verdict and report generation.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct RouteFact {
    /// Route family selected by the declared or observed execution facts.
    pub family: RouteFamily,

    /// Outcome of applying the route to the scenario window.
    pub outcome: RouteOutcome,

    /// Typed reason for the route outcome.
    pub reason: RouteReason,
}

impl RouteFact {
    /// Build one complete typed route identity.
    #[must_use]
    pub const fn new(family: RouteFamily, outcome: RouteOutcome, reason: RouteReason) -> Self {
        Self {
            family,
            outcome,
            reason,
        }
    }
}

///
/// RouteObservation
///
/// Execution metrics needed to classify the route used by one scenario.
/// Owned by the shared SQL harness and populated by the performance runner.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RouteObservation {
    /// Whether execution required a materialized ordering stage.
    pub materialized_order: bool,

    /// Number of data-store lookups performed by the observed execution.
    pub data_store_get_calls: u64,

    /// Number of index entries read by the observed execution.
    pub index_store_entry_reads: u64,
}

///
/// RouteExpectation
///
/// Typed rule for deriving a route fact from declared scenario and runtime facts.
/// Owned by the shared SQL harness and applied by correctness-aware runners.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RouteExpectation {
    Fixed(RouteFact),
    IndexOrder {
        family: RouteFamily,
        candidate_reason: RouteReason,
        pushed_reason: RouteReason,
    },
    PrimaryOrder {
        candidate_reason: RouteReason,
        residual_filter: bool,
    },
}

impl RouteExpectation {
    /// Return the declared route family before execution metrics are observed.
    #[must_use]
    pub const fn family(self) -> RouteFamily {
        match self {
            Self::Fixed(fact) => fact.family,
            Self::IndexOrder { family, .. } => family,
            Self::PrimaryOrder { .. } => RouteFamily::PrimaryOrder,
        }
    }

    /// Classify runtime route evidence without inspecting the scenario SQL text.
    #[must_use]
    pub fn classify(self, window: WindowSpec, observation: RouteObservation) -> RouteFact {
        match self {
            Self::Fixed(fact) => fact,
            Self::PrimaryOrder {
                candidate_reason,
                residual_filter,
            } => {
                if observation.materialized_order {
                    return RouteFact::new(
                        RouteFamily::MaterializedOrder,
                        RouteOutcome::Materialized,
                        RouteReason::RequiresMaterializedSort,
                    );
                }
                if residual_filter {
                    return RouteFact::new(
                        RouteFamily::ResidualFilterOrderedScan,
                        RouteOutcome::ResidualUnbounded,
                        RouteReason::ResidualFilterRequiresCandidateScan,
                    );
                }
                if window
                    .read_bound()
                    .is_some_and(|bound| observation.data_store_get_calls <= bound)
                {
                    return RouteFact::new(
                        RouteFamily::PrimaryOrder,
                        RouteOutcome::Pushed,
                        RouteReason::PrimaryOrderLimitStopProven,
                    );
                }
                RouteFact::new(
                    RouteFamily::PrimaryOrder,
                    RouteOutcome::EligibleButNotPushed,
                    candidate_reason,
                )
            }
            Self::IndexOrder {
                family,
                candidate_reason,
                pushed_reason,
            } => {
                if observation.materialized_order {
                    return RouteFact::new(
                        RouteFamily::MaterializedOrder,
                        RouteOutcome::Materialized,
                        RouteReason::RequiresMaterializedSort,
                    );
                }
                if window.read_bound().is_some_and(|bound| {
                    observation.data_store_get_calls <= bound
                        && observation.index_store_entry_reads <= bound
                }) {
                    return RouteFact::new(family, RouteOutcome::Pushed, pushed_reason);
                }
                RouteFact::new(family, RouteOutcome::EligibleButNotPushed, candidate_reason)
            }
        }
    }
}

///
/// ScenarioMetadata
///
/// Authoritative typed intent and evidence contract attached to one SQL payload.
/// Owned by the shared SQL harness and consumed by selection, verdict, and reporting code.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ScenarioMetadata {
    /// SQL contract feature identifiers evidenced by the scenario.
    pub contract_features: &'static [&'static str],

    /// Stable identity of the scenario's declared oracle or invariant provider.
    pub provider_id: &'static str,

    /// Provider class eligible to judge the scenario.
    pub provider: EligibleProvider,

    /// Strength of evidence supplied by the declared provider.
    pub evidence_strength: EvidenceStrength,

    /// Top-level SQL statement family.
    pub statement: StatementFamily,

    /// Semantic result shape.
    pub shape: QueryShape,

    /// Coarse value family exercised by the scenario.
    pub value_type: ValueTypeFamily,

    /// Nullability contract exercised by the scenario.
    pub nullability: NullabilityClass,

    /// Semantic predicate family, declared independently of SQL rendering.
    pub predicate: PredicateFamily,

    /// Declared ordering and row-window contract.
    pub window: WindowSpec,

    /// Mutation family, or `None` for a read scenario.
    pub mutation: MutationKind,

    /// Whether row position is part of the expected result contract.
    pub row_order: RowOrder,

    /// Rule used to classify observed route metrics.
    pub route: RouteExpectation,

    /// Exact route required for correctness, when route identity is contractual.
    pub required_route: Option<RouteFact>,

    /// Expected admission outcome and typed rejection identity.
    pub expected: ExpectedAcceptance,
}

///
/// CorrectnessScenario
///
/// SQL payload plus typed metadata and runner-specific surface selection.
/// Owned by the shared SQL harness and instantiated by correctness and performance runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CorrectnessScenario<S> {
    /// Stable scenario identity used for selection and reporting.
    pub key: String,

    /// Runner-specific execution surface.
    pub surface: S,

    /// Human-readable scenario family used in reports.
    pub family: String,

    /// SQL payload executed by the runner, never classification authority.
    pub sql: String,

    /// Authoritative typed scenario and evidence facts.
    pub metadata: ScenarioMetadata,
}
