//! Module: sql_harness::model
//! Responsibility: typed, test-only scenario and route facts shared by SQL evidence runners.
//! Does not own: SQL semantics, production planning, or classification derived from SQL text.
//! Boundary: requires scenario producers to declare evidence intent independently of SQL payloads.

///
/// EvidenceClass
///
/// SQL contract layer exercised by an evidence provider.
/// Owned by the shared SQL harness and consumed by coverage and runner metadata.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum EvidenceClass {
    Boundary,
    Execute,
    Lower,
    Parse,
    ReferenceDifferential,
    Regression,
    Route,
    State,
}

///
/// EvidenceStrength
///
/// Strength of the oracle or assertion supplied by one SQL evidence provider.
/// Owned by the shared SQL harness and used by coverage validation and selection.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum EvidenceStrength {
    BoundaryAssertion,
    ContractAssertion,
    MetamorphicInvariant,
    ReferenceOracle,
}

///
/// EligibleProvider
///
/// Reference or invariant provider eligible to judge one SQL scenario.
/// Owned by the shared SQL harness and declared by correctness and performance runners.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum EligibleProvider {
    ExecutionModeEquivalent,
    FrontendEquivalent,
    IcyDbContractOnly,
    RejectionInvariant,
    SqliteReference,
    StateModelReference,
}

///
/// StatementFamily
///
/// Top-level SQL statement family represented by a scenario.
/// Owned by the shared SQL harness and used to stratify bounded test selections.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum StatementFamily {
    Delete,
    Describe,
    Explain,
    Insert,
    Select,
    Show,
    Update,
}

///
/// QueryShape
///
/// Semantic result shape exercised by a SQL scenario.
/// Owned by the shared SQL harness and consumed by coverage and selection logic.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum QueryShape {
    GlobalAggregate,
    Grouped,
    Metadata,
    Mutation,
    Scalar,
}

///
/// ValueTypeFamily
///
/// Coarse value family exercised by a SQL scenario or projection.
/// Owned by the shared SQL harness and used as a declared coverage stratum.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum ValueTypeFamily {
    Blob,
    Boolean,
    Catalog,
    Mixed,
    Numeric,
    Text,
}

///
/// NullabilityClass
///
/// Nullability contract exercised by a scenario.
/// Owned by the shared SQL harness and consumed by stratified selection.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum NullabilityClass {
    NotApplicable,
    NonNullable,
    Nullable,
}

///
/// PredicateFamily
///
/// Semantic predicate family declared for a SQL scenario.
/// Owned by the shared SQL harness so runners never infer coverage from SQL text.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum PredicateFamily {
    Boolean,
    CasefoldPrefix,
    Compound,
    FieldComparison,
    Membership,
    None,
    Prefix,
    PrimaryKey,
    Range,
    SparseMembership,
}

///
/// WindowBehavior
///
/// Ordering and bounding shape declared for a SQL scenario.
/// Owned by the shared SQL harness and used by selection and route classification.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum WindowBehavior {
    None,
    Limit,
    Ordered,
    OrderedLimit,
    OrderedLimitOffset,
}

///
/// MutationKind
///
/// Mutation family exercised by a scenario, or `None` for reads.
/// Owned by the shared SQL harness and used as a selection stratum.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum MutationKind {
    Delete,
    Insert,
    None,
    Update,
}

///
/// RowOrder
///
/// Whether row position is part of a normalized result contract.
/// Owned by the shared SQL harness and consumed by result normalization.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RowOrder {
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
pub(crate) enum ExpectedAcceptance {
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
pub(crate) struct WindowSpec {
    /// Coarse ordering and bound behavior.
    pub(crate) behavior: WindowBehavior,

    /// Requested row limit, when the scenario is bounded.
    pub(crate) limit: Option<usize>,

    /// Number of ordered rows skipped before the requested window.
    pub(crate) offset: usize,

    /// Human-readable declared order used only in evidence reports.
    pub(crate) order_hint: Option<&'static str>,
}

impl WindowSpec {
    /// Unordered and unbounded window declaration.
    pub(crate) const NONE: Self = Self {
        behavior: WindowBehavior::None,
        limit: None,
        offset: 0,
        order_hint: None,
    };

    /// Build an unordered bounded window declaration.
    pub(crate) const fn limit(limit: usize) -> Self {
        Self {
            behavior: WindowBehavior::Limit,
            limit: Some(limit),
            offset: 0,
            order_hint: None,
        }
    }

    /// Build an ordered bounded window declaration.
    pub(crate) const fn ordered(limit: usize, offset: usize, order_hint: &'static str) -> Self {
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
    pub(crate) const fn ordered_unbounded(order_hint: &'static str) -> Self {
        Self {
            behavior: WindowBehavior::Ordered,
            limit: None,
            offset: 0,
            order_hint: Some(order_hint),
        }
    }

    /// Return the maximum successful row reads that prove bounded early stopping.
    pub(crate) fn read_bound(self) -> Option<u64> {
        let limit = self.limit?;
        u64::try_from(limit.saturating_add(self.offset).saturating_add(1)).ok()
    }
}

///
/// RouteFamily
///
/// Coarse execution-route family observed or required by a SQL scenario.
/// Owned by the shared SQL harness and emitted in correctness and performance evidence.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RouteFamily {
    EqualityPrefixOrderedSuffix,
    IncompatibleFilterFirstOrder,
    MaterializedOrder,
    NotOrderedOrNotPaginated,
    PrimaryOrder,
    ResidualFilterOrderedScan,
    SecondaryOrder,
    UnsupportedAccessKind,
}

impl RouteFamily {
    /// Return the stable report code for this route family.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::EqualityPrefixOrderedSuffix => "equality_prefix_ordered_suffix",
            Self::IncompatibleFilterFirstOrder => "incompatible_filter_first_order",
            Self::MaterializedOrder => "materialized_order",
            Self::NotOrderedOrNotPaginated => "not_ordered_or_not_paginated",
            Self::PrimaryOrder => "primary_order",
            Self::ResidualFilterOrderedScan => "residual_filter_ordered_scan",
            Self::SecondaryOrder => "secondary_order",
            Self::UnsupportedAccessKind => "unsupported_access_kind",
        }
    }
}

///
/// RouteOutcome
///
/// Result of applying an execution route to the declared query window.
/// Owned by the shared SQL harness and used in route evidence signatures.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum RouteOutcome {
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
    pub(crate) const fn code(self) -> &'static str {
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
pub(crate) enum RouteReason {
    EqualityPrefixOrderedSuffixCandidate,
    EqualityPrefixOrderedSuffixLimitStopProven,
    FilterOrderMismatch,
    GroupedAggregateMaterialized,
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
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::EqualityPrefixOrderedSuffixCandidate => {
                "equality_prefix_ordered_suffix_candidate"
            }
            Self::EqualityPrefixOrderedSuffixLimitStopProven => {
                "equality_prefix_ordered_suffix_limit_stop_proven"
            }
            Self::FilterOrderMismatch => "filter_order_mismatch",
            Self::GroupedAggregateMaterialized => "grouped_aggregate_materialized",
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
pub(crate) struct RouteFact {
    /// Route family selected by the declared or observed execution facts.
    pub(crate) family: RouteFamily,

    /// Outcome of applying the route to the scenario window.
    pub(crate) outcome: RouteOutcome,

    /// Typed reason for the route outcome.
    pub(crate) reason: RouteReason,
}

impl RouteFact {
    /// Build one complete typed route identity.
    pub(crate) const fn new(
        family: RouteFamily,
        outcome: RouteOutcome,
        reason: RouteReason,
    ) -> Self {
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
pub(crate) struct RouteObservation {
    /// Whether execution required a materialized ordering stage.
    pub(crate) materialized_order: bool,

    /// Number of data-store lookups performed by the observed execution.
    pub(crate) data_store_get_calls: u64,

    /// Number of index entries read by the observed execution.
    pub(crate) index_store_entry_reads: u64,
}

///
/// RouteExpectation
///
/// Typed rule for deriving a route fact from declared scenario and runtime facts.
/// Owned by the shared SQL harness and applied by correctness-aware runners.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum RouteExpectation {
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
    pub(crate) const fn family(self) -> RouteFamily {
        match self {
            Self::Fixed(fact) => fact.family,
            Self::IndexOrder { family, .. } => family,
            Self::PrimaryOrder { .. } => RouteFamily::PrimaryOrder,
        }
    }

    /// Classify runtime route evidence without inspecting the scenario SQL text.
    pub(crate) fn classify(self, window: WindowSpec, observation: RouteObservation) -> RouteFact {
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
/// ScenarioSource
///
/// Reproduction identity for a deterministic or generated SQL scenario.
/// Owned by the shared SQL harness and included in evidence reports.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ScenarioSource {
    Deterministic,
    Generated { root_seed: u64, case_index: u64 },
}

impl ScenarioSource {
    /// Return the stable report label for the source class.
    pub(crate) const fn label(self) -> &'static str {
        match self {
            Self::Deterministic => "deterministic",
            Self::Generated { .. } => "random",
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
pub(crate) struct ScenarioMetadata {
    /// SQL contract feature identifiers evidenced by the scenario.
    pub(crate) contract_features: &'static [&'static str],

    /// Stable identity of the scenario's declared oracle or invariant provider.
    pub(crate) provider_id: &'static str,

    /// Provider class eligible to judge the scenario.
    pub(crate) provider: EligibleProvider,

    /// Strength of evidence supplied by the declared provider.
    pub(crate) evidence_strength: EvidenceStrength,

    /// Top-level SQL statement family.
    pub(crate) statement: StatementFamily,

    /// Semantic result shape.
    pub(crate) shape: QueryShape,

    /// Coarse value family exercised by the scenario.
    pub(crate) value_type: ValueTypeFamily,

    /// Nullability contract exercised by the scenario.
    pub(crate) nullability: NullabilityClass,

    /// Semantic predicate family, declared independently of SQL rendering.
    pub(crate) predicate: PredicateFamily,

    /// Declared ordering and row-window contract.
    pub(crate) window: WindowSpec,

    /// Mutation family, or `None` for a read scenario.
    pub(crate) mutation: MutationKind,

    /// Whether row position is part of the expected result contract.
    pub(crate) row_order: RowOrder,

    /// Rule used to classify observed route metrics.
    pub(crate) route: RouteExpectation,

    /// Exact route required for correctness, when route identity is contractual.
    pub(crate) required_route: Option<RouteFact>,

    /// Expected admission outcome and typed rejection identity.
    pub(crate) expected: ExpectedAcceptance,
}

///
/// CorrectnessScenario
///
/// SQL payload plus typed metadata and runner-specific surface selection.
/// Owned by the shared SQL harness and instantiated by correctness and performance runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CorrectnessScenario<S> {
    /// Stable scenario identity used for selection and reporting.
    pub(crate) key: String,

    /// Deterministic or generated reproduction identity.
    pub(crate) source: ScenarioSource,

    /// Runner-specific execution surface.
    pub(crate) surface: S,

    /// Human-readable scenario family used in reports.
    pub(crate) family: String,

    /// SQL payload executed by the runner, never classification authority.
    pub(crate) sql: String,

    /// Authoritative typed scenario and evidence facts.
    pub(crate) metadata: ScenarioMetadata,
}
