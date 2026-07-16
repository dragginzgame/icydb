//! Module: sql_generator::coverage
//! Responsibility: typed Tier C scenario declarations and strict coverage-distribution evidence.
//! Does not own: scenario generation, execution verdicts, shard membership, or CI policy.
//! Boundary: projects source-owned typed facts only after exact merged receipt validation.

use crate::{
    GeneratedMutationSequence, GeneratedSelectCase, MutationOperation, MutationPredicate,
    MutationSqliteEligibility, SelectExpectedOutcome, SelectFeature, SelectProvider,
    SelectQueryShape, SelectValueKind, SqlGeneratorError, TierCEvidenceError, TierCMergedReport,
    TierCScenarioOutcome, TierCShardReport, replay::canonical_json_bytes,
    scheduled::TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES, scheduled_sql_scenario_shard,
};

use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display},
};

use serde::{Deserialize, Serialize};

/// Current hard-cut Tier C coverage-distribution artifact format.
pub const TIER_C_DISTRIBUTION_FORMAT_VERSION: u32 = 1;

// -----------------------------------------------------------------------------
// Shared coverage taxonomy
// -----------------------------------------------------------------------------

///
/// EvidenceStrength
///
/// Strength of the oracle or invariant supplied by one SQL evidence provider.
/// The shared correctness harness and scheduled evidence declarations consume this vocabulary.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EvidenceStrength {
    /// Direct assertion at an admission, parsing, or other contract boundary.
    BoundaryAssertion,

    /// Product-owned typed semantic or rejection assertion.
    ContractAssertion,

    /// Relationship that must remain true across equivalent executions.
    MetamorphicInvariant,

    /// Independent model or database result oracle.
    ReferenceOracle,
}

impl EvidenceStrength {
    /// Return the stable machine-readable evidence-strength identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::BoundaryAssertion => "boundary_assertion",
            Self::ContractAssertion => "contract_assertion",
            Self::MetamorphicInvariant => "metamorphic_invariant",
            Self::ReferenceOracle => "reference_oracle",
        }
    }
}

///
/// EligibleProvider
///
/// Reference or invariant provider eligible to judge one SQL scenario.
/// This is evidence taxonomy, not a runtime execution-mode selector.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EligibleProvider {
    /// Equivalent cold, warm, heap, journaled, or other maintained execution modes.
    ExecutionModeEquivalent,

    /// Equivalent fluent and SQL frontend semantics.
    FrontendEquivalent,

    /// IcyDB's typed contract is the only fair provider.
    IcyDbContractOnly,

    /// A typed rejection invariant is the required verdict.
    RejectionInvariant,

    /// Bundled SQLite provides the maintained overlap oracle.
    SqliteReference,

    /// The independent mutation state model provides the transition oracle.
    StateModelReference,
}

impl EligibleProvider {
    /// Return the stable machine-readable provider-class identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::ExecutionModeEquivalent => "execution_mode_equivalent",
            Self::FrontendEquivalent => "frontend_equivalent",
            Self::IcyDbContractOnly => "icydb_contract_only",
            Self::RejectionInvariant => "rejection_invariant",
            Self::SqliteReference => "sqlite_reference",
            Self::StateModelReference => "state_model_reference",
        }
    }
}

///
/// StatementFamily
///
/// Top-level SQL statement family represented by a correctness scenario.
/// Mixed mutation sequences declare every family they contain rather than one representative.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum StatementFamily {
    /// `DELETE` mutation.
    Delete,

    /// `DESCRIBE` metadata statement.
    Describe,

    /// `EXPLAIN` diagnostic statement.
    Explain,

    /// `INSERT` or `INSERT ... SELECT` mutation.
    Insert,

    /// `SELECT` query.
    Select,

    /// `SHOW` metadata statement.
    Show,

    /// `UPDATE` mutation.
    Update,
}

impl StatementFamily {
    /// Return the stable machine-readable statement-family identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Describe => "describe",
            Self::Explain => "explain",
            Self::Insert => "insert",
            Self::Select => "select",
            Self::Show => "show",
            Self::Update => "update",
        }
    }
}

///
/// QueryShape
///
/// Semantic result shape exercised by a SQL scenario.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryShape {
    /// Aggregate over the entire admitted input.
    GlobalAggregate,

    /// Explicit grouping and grouped projection.
    Grouped,

    /// Metadata result shape.
    Metadata,

    /// Mutation result or state-transition shape.
    Mutation,

    /// Non-aggregate scalar row projection.
    Scalar,
}

impl QueryShape {
    /// Return the stable machine-readable query-shape identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::GlobalAggregate => "global_aggregate",
            Self::Grouped => "grouped",
            Self::Metadata => "metadata",
            Self::Mutation => "mutation",
            Self::Scalar => "scalar",
        }
    }
}

///
/// ValueTypeFamily
///
/// Coarse value family exercised by a scenario or projection.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueTypeFamily {
    /// Arbitrary byte values.
    Blob,

    /// Strict boolean values.
    Boolean,

    /// Catalog or metadata values.
    Catalog,

    /// More than one value family in one result or transition.
    Mixed,

    /// Integer or exact decimal values.
    Numeric,

    /// UTF-8 text values.
    Text,
}

impl ValueTypeFamily {
    /// Return the stable machine-readable value-family identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Blob => "blob",
            Self::Boolean => "boolean",
            Self::Catalog => "catalog",
            Self::Mixed => "mixed",
            Self::Numeric => "numeric",
            Self::Text => "text",
        }
    }
}

///
/// NullabilityClass
///
/// Nullability contract exercised by a scenario.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum NullabilityClass {
    /// Nullability does not participate in the statement contract.
    NotApplicable,

    /// The exercised path contains no nullable values.
    NonNullable,

    /// The exercised path can produce or consume SQL `NULL`.
    Nullable,
}

impl NullabilityClass {
    /// Return the stable machine-readable nullability identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::NotApplicable => "not_applicable",
            Self::NonNullable => "non_nullable",
            Self::Nullable => "nullable",
        }
    }
}

///
/// PredicateFamily
///
/// Semantic predicate family declared independently of rendered SQL text.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum PredicateFamily {
    /// Boolean truth or match-all expression.
    Boolean,

    /// Case-insensitive prefix comparison.
    CasefoldPrefix,

    /// More than one predicate shape participates.
    Compound,

    /// Comparison between field-derived values.
    FieldComparison,

    /// Membership in an explicit value set.
    Membership,

    /// No predicate participates.
    None,

    /// Case-sensitive prefix comparison.
    Prefix,

    /// Exact primary-key comparison.
    PrimaryKey,

    /// Bounded value range.
    Range,

    /// Membership over sparse or optional values.
    SparseMembership,
}

impl PredicateFamily {
    /// Return the stable machine-readable predicate-family identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Boolean => "boolean",
            Self::CasefoldPrefix => "casefold_prefix",
            Self::Compound => "compound",
            Self::FieldComparison => "field_comparison",
            Self::Membership => "membership",
            Self::None => "none",
            Self::Prefix => "prefix",
            Self::PrimaryKey => "primary_key",
            Self::Range => "range",
            Self::SparseMembership => "sparse_membership",
        }
    }
}

///
/// WindowBehavior
///
/// Ordering and bounding shape declared for a SQL scenario.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum WindowBehavior {
    /// Unordered explicit result bound.
    Limit,

    /// No ordering or explicit bound.
    None,

    /// Deterministic ordering without an explicit bound.
    Ordered,

    /// Deterministic ordering with an explicit limit.
    OrderedLimit,

    /// Deterministic ordering with explicit limit and offset.
    OrderedLimitOffset,
}

impl WindowBehavior {
    /// Return the stable machine-readable window-behavior identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Limit => "limit",
            Self::None => "none",
            Self::Ordered => "ordered",
            Self::OrderedLimit => "ordered_limit",
            Self::OrderedLimitOffset => "ordered_limit_offset",
        }
    }
}

///
/// MutationKind
///
/// Mutation family exercised by a scenario, or `None` for non-mutation statements.
/// Mixed mutation sequences declare every kind they contain.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MutationKind {
    /// `DELETE` transition.
    Delete,

    /// `INSERT` or `INSERT ... SELECT` transition.
    Insert,

    /// No mutation participates.
    None,

    /// `UPDATE` transition.
    Update,
}

impl MutationKind {
    /// Return the stable machine-readable mutation-family identity.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::Delete => "delete",
            Self::Insert => "insert",
            Self::None => "none",
            Self::Update => "update",
        }
    }
}

///
/// RouteFamily
///
/// Coarse execution-route family declared or observed by SQL correctness evidence.
/// `NotContractual` records scenarios whose verdict deliberately does not constrain routing.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RouteFamily {
    /// Equality-prefix filtering followed by compatible ordered suffix traversal.
    EqualityPrefixOrderedSuffix,

    /// Filter-first access whose order is incompatible with the requested order.
    IncompatibleFilterFirstOrder,

    /// Ordering produced through materialization.
    MaterializedOrder,

    /// The scenario verdict deliberately does not constrain execution routing.
    NotContractual,

    /// The statement is not both ordered and paginated.
    NotOrderedOrNotPaginated,

    /// Primary-key order traversal.
    PrimaryOrder,

    /// Ordered scan followed by residual filtering.
    ResidualFilterOrderedScan,

    /// Secondary-index order traversal.
    SecondaryOrder,

    /// Access kind outside the maintained routed-query contract.
    UnsupportedAccessKind,
}

impl RouteFamily {
    /// Return the stable report code for this route family.
    #[must_use]
    pub const fn code(self) -> &'static str {
        match self {
            Self::EqualityPrefixOrderedSuffix => "equality_prefix_ordered_suffix",
            Self::IncompatibleFilterFirstOrder => "incompatible_filter_first_order",
            Self::MaterializedOrder => "materialized_order",
            Self::NotContractual => "not_contractual",
            Self::NotOrderedOrNotPaginated => "not_ordered_or_not_paginated",
            Self::PrimaryOrder => "primary_order",
            Self::ResidualFilterOrderedScan => "residual_filter_ordered_scan",
            Self::SecondaryOrder => "secondary_order",
            Self::UnsupportedAccessKind => "unsupported_access_kind",
        }
    }
}

// -----------------------------------------------------------------------------
// Scenario declarations
// -----------------------------------------------------------------------------

///
/// TierCExpectedAcceptance
///
/// Scenario-level admission contract used to align a declaration with its receipt outcome.
/// Step-local expected rejections inside a passing mutation sequence remain step evidence.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TierCExpectedAcceptance {
    /// The scenario-level execution must complete successfully.
    Accepted,

    /// The scenario-level execution must reach its declared typed rejection.
    Rejected,
}

impl TierCExpectedAcceptance {
    const fn code(self) -> &'static str {
        match self {
            Self::Accepted => "accepted",
            Self::Rejected => "rejected",
        }
    }
}

///
/// TierCCoverageLabels
///
/// Typed multi-label strata carried by one scheduled correctness scenario.
/// Sets are intentional: one deterministic mutation sequence may exercise several facts per dimension.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TierCCoverageLabels {
    evidence_strength: BTreeSet<EvidenceStrength>,
    mutation: BTreeSet<MutationKind>,
    nullability: BTreeSet<NullabilityClass>,
    predicate: BTreeSet<PredicateFamily>,
    provider: BTreeSet<EligibleProvider>,
    route: BTreeSet<RouteFamily>,
    shape: BTreeSet<QueryShape>,
    statement: BTreeSet<StatementFamily>,
    value_type: BTreeSet<ValueTypeFamily>,
    window: BTreeSet<WindowBehavior>,
}

impl TierCCoverageLabels {
    /// Build one complete set of typed coverage labels.
    ///
    /// # Errors
    ///
    /// Returns a typed distribution error when any dimension is empty or when
    /// statement and mutation labels describe incompatible operations.
    #[expect(
        clippy::too_many_arguments,
        reason = "the constructor makes all ten report dimensions explicit"
    )]
    pub fn try_new(
        evidence_strength: BTreeSet<EvidenceStrength>,
        mutation: BTreeSet<MutationKind>,
        nullability: BTreeSet<NullabilityClass>,
        predicate: BTreeSet<PredicateFamily>,
        provider: BTreeSet<EligibleProvider>,
        route: BTreeSet<RouteFamily>,
        shape: BTreeSet<QueryShape>,
        statement: BTreeSet<StatementFamily>,
        value_type: BTreeSet<ValueTypeFamily>,
        window: BTreeSet<WindowBehavior>,
    ) -> Result<Self, TierCDistributionError> {
        let labels = Self {
            evidence_strength,
            mutation,
            nullability,
            predicate,
            provider,
            route,
            shape,
            statement,
            value_type,
            window,
        };
        labels.validate()?;

        Ok(labels)
    }

    fn validate(&self) -> Result<(), TierCDistributionError> {
        let complete = !self.evidence_strength.is_empty()
            && !self.mutation.is_empty()
            && !self.nullability.is_empty()
            && !self.predicate.is_empty()
            && !self.provider.is_empty()
            && !self.route.is_empty()
            && !self.shape.is_empty()
            && !self.statement.is_empty()
            && !self.value_type.is_empty()
            && !self.window.is_empty();
        if !complete {
            return Err(TierCDistributionError::IncompleteCoverageLabels);
        }

        let statements = self
            .statement
            .iter()
            .filter_map(|statement| match statement {
                StatementFamily::Delete => Some(MutationKind::Delete),
                StatementFamily::Insert => Some(MutationKind::Insert),
                StatementFamily::Update => Some(MutationKind::Update),
                StatementFamily::Describe
                | StatementFamily::Explain
                | StatementFamily::Select
                | StatementFamily::Show => None,
            })
            .collect::<BTreeSet<_>>();
        let mutations = self
            .mutation
            .iter()
            .copied()
            .filter(|mutation| *mutation != MutationKind::None)
            .collect::<BTreeSet<_>>();
        let has_non_mutation_statement = self.statement.iter().any(|statement| {
            matches!(
                statement,
                StatementFamily::Describe
                    | StatementFamily::Explain
                    | StatementFamily::Select
                    | StatementFamily::Show
            )
        });
        let valid = if statements.is_empty() {
            self.mutation == BTreeSet::from([MutationKind::None])
        } else {
            !has_non_mutation_statement
                && statements == mutations
                && !self.mutation.contains(&MutationKind::None)
        };
        if !valid {
            return Err(TierCDistributionError::InconsistentMutationLabels);
        }

        Ok(())
    }
}

///
/// TierCScenarioDeclaration
///
/// Stable scenario identity plus source-owned semantic labels used only after receipt merge.
/// It carries no execution verdict and cannot substitute for the exact shard observations.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TierCScenarioDeclaration {
    scenario_id: String,
    contract_features: BTreeSet<String>,
    provider_ids: BTreeSet<String>,
    expected: TierCExpectedAcceptance,
    labels: TierCCoverageLabels,
}

impl TierCScenarioDeclaration {
    /// Build and validate one current scheduled scenario declaration.
    ///
    /// # Errors
    ///
    /// Returns a typed distribution error for an invalid scenario identity,
    /// empty or malformed stable labels, or inconsistent typed strata.
    pub fn try_new(
        scenario_id: impl Into<String>,
        contract_features: BTreeSet<String>,
        provider_ids: BTreeSet<String>,
        expected: TierCExpectedAcceptance,
        labels: TierCCoverageLabels,
    ) -> Result<Self, TierCDistributionError> {
        let declaration = Self {
            scenario_id: scenario_id.into(),
            contract_features,
            provider_ids,
            expected,
            labels,
        };
        declaration.validate()?;

        Ok(declaration)
    }

    /// Borrow the stable scenario identity shared with exact receipts.
    #[must_use]
    pub const fn scenario_id(&self) -> &str {
        self.scenario_id.as_str()
    }

    fn validate(&self) -> Result<(), TierCDistributionError> {
        scheduled_sql_scenario_shard(self.scenario_id.as_str()).map_err(|source| {
            TierCDistributionError::InvalidScenarioId {
                scenario_id: self.scenario_id.clone(),
                source,
            }
        })?;
        self.labels.validate()?;
        let stable = |label: &str| {
            !label.is_empty()
                && label.len() <= 256
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        };
        if self.contract_features.is_empty()
            || self.provider_ids.is_empty()
            || !self
                .contract_features
                .iter()
                .all(|feature| stable(feature.as_str()))
            || !self
                .provider_ids
                .iter()
                .all(|provider| stable(provider.as_str()))
        {
            return Err(TierCDistributionError::InvalidStableLabel(
                self.scenario_id.clone(),
            ));
        }

        Ok(())
    }
}

// -----------------------------------------------------------------------------
// Strict distribution artifact
// -----------------------------------------------------------------------------

///
/// TierCStratumDistribution
///
/// Scenario counts across the exact ten typed distribution dimensions.
/// A multi-label scenario contributes once to each distinct label it declares.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct TierCStratumDistribution {
    evidence_strength: BTreeMap<String, u32>,
    mutation: BTreeMap<String, u32>,
    nullability: BTreeMap<String, u32>,
    predicate: BTreeMap<String, u32>,
    provider: BTreeMap<String, u32>,
    route: BTreeMap<String, u32>,
    shape: BTreeMap<String, u32>,
    statement: BTreeMap<String, u32>,
    value_type: BTreeMap<String, u32>,
    window: BTreeMap<String, u32>,
}

impl TierCStratumDistribution {
    const fn empty() -> Self {
        Self {
            evidence_strength: BTreeMap::new(),
            mutation: BTreeMap::new(),
            nullability: BTreeMap::new(),
            predicate: BTreeMap::new(),
            provider: BTreeMap::new(),
            route: BTreeMap::new(),
            shape: BTreeMap::new(),
            statement: BTreeMap::new(),
            value_type: BTreeMap::new(),
            window: BTreeMap::new(),
        }
    }
}

///
/// TierCCoverageDistributionReport
///
/// Strict diagnostic projection of complete Tier C declarations after exact receipt merge.
/// Decoding recomputes every count from the same typed declarations used to execute the catalog.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TierCCoverageDistributionReport {
    format_version: u32,
    manifest_revision: String,
    scenario_set_hash: String,
    scenario_count: u32,
    contract_feature_counts: BTreeMap<String, u32>,
    provider_id_counts: BTreeMap<String, u32>,
    expected_acceptance_counts: BTreeMap<String, u32>,
    strata: TierCStratumDistribution,
    complete: bool,
}

impl TierCCoverageDistributionReport {
    /// Validate declarations, exact receipt labels, and the clean merged verdict together.
    ///
    /// # Errors
    ///
    /// Returns a typed distribution or receipt error when membership, labels,
    /// outcomes, counts, or the final clean-evidence gate disagree.
    pub fn try_from_clean_evidence(
        declarations: &[TierCScenarioDeclaration],
        merged: &TierCMergedReport,
    ) -> Result<Self, TierCDistributionError> {
        let report = Self::try_new(declarations, merged)?;
        merged
            .require_clean()
            .map_err(TierCDistributionError::FailedEvidence)?;

        Ok(report)
    }

    /// Project one complete distribution from typed declarations and exact merged receipts.
    ///
    /// # Errors
    ///
    /// Returns a typed distribution error for duplicate declarations, invalid
    /// labels, receipt/declaration drift, outcome drift, or count overflow.
    pub fn try_new(
        declarations: &[TierCScenarioDeclaration],
        merged: &TierCMergedReport,
    ) -> Result<Self, TierCDistributionError> {
        validate_declarations(declarations)?;
        validate_membership(declarations, merged)?;
        validate_outcome_expectations(declarations, merged)?;
        let mut contract_feature_counts = BTreeMap::new();
        let mut provider_id_counts = BTreeMap::new();
        let mut expected_acceptance_counts = BTreeMap::new();
        let mut strata = TierCStratumDistribution::empty();

        for declaration in declarations {
            for feature_id in &declaration.contract_features {
                increment_count(&mut contract_feature_counts, feature_id)?;
            }
            for provider_id in &declaration.provider_ids {
                increment_count(&mut provider_id_counts, provider_id)?;
            }
            increment_count(&mut expected_acceptance_counts, declaration.expected.code())?;
            increment_labels(
                &mut strata.evidence_strength,
                &declaration.labels.evidence_strength,
                |label| label.code(),
            )?;
            increment_labels(
                &mut strata.mutation,
                &declaration.labels.mutation,
                |label| label.code(),
            )?;
            increment_labels(
                &mut strata.nullability,
                &declaration.labels.nullability,
                |label| label.code(),
            )?;
            increment_labels(
                &mut strata.predicate,
                &declaration.labels.predicate,
                |label| label.code(),
            )?;
            increment_labels(
                &mut strata.provider,
                &declaration.labels.provider,
                |label| label.code(),
            )?;
            increment_labels(&mut strata.route, &declaration.labels.route, |label| {
                label.code()
            })?;
            increment_labels(&mut strata.shape, &declaration.labels.shape, |label| {
                label.code()
            })?;
            increment_labels(
                &mut strata.statement,
                &declaration.labels.statement,
                |label| label.code(),
            )?;
            increment_labels(
                &mut strata.value_type,
                &declaration.labels.value_type,
                |label| label.code(),
            )?;
            increment_labels(&mut strata.window, &declaration.labels.window, |label| {
                label.code()
            })?;
        }

        Ok(Self {
            format_version: TIER_C_DISTRIBUTION_FORMAT_VERSION,
            manifest_revision: crate::TIER_C_SQL_COVERAGE_MANIFEST_REVISION.to_string(),
            scenario_set_hash: merged.expected_scenario_set_hash().to_string(),
            scenario_count: bounded_count(declarations.len())?,
            contract_feature_counts,
            provider_id_counts,
            expected_acceptance_counts,
            strata,
            complete: true,
        })
    }

    /// Return the complete declared scenario count.
    #[must_use]
    pub const fn scenario_count(&self) -> u32 {
        self.scenario_count
    }

    /// Return how many scenarios declare one statement family.
    #[must_use]
    pub fn statement_count(&self, family: StatementFamily) -> u32 {
        self.strata
            .statement
            .get(family.code())
            .copied()
            .unwrap_or_default()
    }

    /// Return how many scenarios declare one mutation family.
    #[must_use]
    pub fn mutation_count(&self, kind: MutationKind) -> u32 {
        self.strata
            .mutation
            .get(kind.code())
            .copied()
            .unwrap_or_default()
    }

    /// Borrow counts keyed by stable provider identity.
    #[must_use]
    pub const fn provider_id_counts(&self) -> &BTreeMap<String, u32> {
        &self.provider_id_counts
    }

    /// Borrow counts keyed by stable contract feature identity.
    #[must_use]
    pub const fn contract_feature_counts(&self) -> &BTreeMap<String, u32> {
        &self.contract_feature_counts
    }

    /// Encode this report using the sole current bounded canonical JSON format.
    ///
    /// # Errors
    ///
    /// Returns a typed error when recomputed evidence drifts, encoding fails,
    /// or the artifact exceeds the fixed byte bound.
    pub fn to_canonical_json(
        &self,
        declarations: &[TierCScenarioDeclaration],
        merged: &TierCMergedReport,
    ) -> Result<Vec<u8>, TierCDistributionError> {
        self.validate(declarations, merged)?;
        let bytes = canonical_json_bytes(self).map_err(TierCDistributionError::Serialization)?;
        validate_artifact_size(bytes.len())?;

        Ok(bytes)
    }

    /// Decode one strict bounded current-format distribution artifact.
    ///
    /// # Errors
    ///
    /// Returns a typed error before oversized input is decoded, or for malformed,
    /// non-canonical, stale, tampered, or membership-inconsistent evidence.
    pub fn from_canonical_json(
        bytes: &[u8],
        declarations: &[TierCScenarioDeclaration],
        merged: &TierCMergedReport,
    ) -> Result<Self, TierCDistributionError> {
        validate_artifact_size(bytes.len())?;
        let report = serde_json::from_slice::<Self>(bytes)
            .map_err(|source| TierCDistributionError::Decode { source })?;
        report.validate(declarations, merged)?;
        let canonical =
            canonical_json_bytes(&report).map_err(TierCDistributionError::Serialization)?;
        if canonical != bytes {
            return Err(TierCDistributionError::NonCanonicalArtifact);
        }

        Ok(report)
    }

    fn validate(
        &self,
        declarations: &[TierCScenarioDeclaration],
        merged: &TierCMergedReport,
    ) -> Result<(), TierCDistributionError> {
        if self.format_version != TIER_C_DISTRIBUTION_FORMAT_VERSION {
            return Err(TierCDistributionError::InvalidArtifactVersion {
                expected: TIER_C_DISTRIBUTION_FORMAT_VERSION,
                actual: self.format_version,
            });
        }
        let expected = Self::try_new(declarations, merged)?;
        if &expected != self {
            return Err(TierCDistributionError::ReportDrift);
        }

        Ok(())
    }
}

///
/// TierCDistributionError
///
/// Typed declaration, projection, encoding, decoding, or validation failure for Tier C coverage.
///

#[derive(Debug)]
pub enum TierCDistributionError {
    /// Input or output exceeded the fixed current artifact byte bound.
    ArtifactTooLarge {
        /// Observed byte count.
        observed_bytes: usize,

        /// Maximum admitted byte count.
        maximum_bytes: usize,
    },

    /// Strict current JSON could not be decoded.
    Decode {
        /// Original JSON decoding cause.
        source: serde_json::Error,
    },

    /// More than one declaration used one stable scenario identity.
    DuplicateScenarioId(String),

    /// Exact receipt evidence contained one or more failed scenarios.
    FailedEvidence(TierCEvidenceError),

    /// Embedded generated case facts could not be projected without loss.
    GeneratedCase(SqlGeneratorError),

    /// At least one required typed distribution dimension was empty.
    IncompleteCoverageLabels,

    /// The current distribution format version did not match the artifact.
    InvalidArtifactVersion {
        /// Sole current format version.
        expected: u32,

        /// Decoded artifact version.
        actual: u32,
    },

    /// One declaration used an invalid shared scheduled scenario identity.
    InvalidScenarioId {
        /// Invalid scenario identity.
        scenario_id: String,

        /// Shared shard-contract cause.
        source: crate::ScenarioShardError,
    },

    /// A contract-feature or provider identity was empty, oversized, or unstable.
    InvalidStableLabel(String),

    /// Statement-family and mutation-family labels described different operations.
    InconsistentMutationLabels,

    /// Typed declarations and exact receipt observations had different members.
    MembershipMismatch {
        /// Declarations absent from exact receipt observations.
        missing: Vec<String>,

        /// Receipt observations absent from declarations.
        unexpected: Vec<String>,
    },

    /// A valid JSON artifact did not use deterministic current encoding.
    NonCanonicalArtifact,

    /// A receipt outcome label disagreed with source-owned expected acceptance.
    OutcomeExpectationMismatch(String),

    /// A decoded distribution disagreed with recomputed declaration facts.
    ReportDrift,

    /// One bounded distribution count overflowed its current `u32` representation.
    ScenarioCountOverflow,

    /// Canonical JSON materialization or encoding failed.
    Serialization(SqlGeneratorError),
}

impl Display for TierCDistributionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ArtifactTooLarge {
                observed_bytes,
                maximum_bytes,
            } => write!(
                formatter,
                "Tier C distribution has {observed_bytes} bytes, exceeding the {maximum_bytes}-byte bound",
            ),
            Self::Decode { .. } => formatter.write_str("failed to decode Tier C distribution"),
            Self::DuplicateScenarioId(scenario_id) => {
                write!(formatter, "duplicate Tier C declaration {scenario_id:?}")
            }
            Self::FailedEvidence(source) => {
                write!(formatter, "Tier C receipt evidence is not clean: {source}")
            }
            Self::GeneratedCase(source) => {
                write!(
                    formatter,
                    "invalid generated Tier C coverage facts: {source}"
                )
            }
            Self::IncompleteCoverageLabels => {
                formatter.write_str("Tier C declaration omitted a required coverage dimension")
            }
            Self::InvalidArtifactVersion { expected, actual } => write!(
                formatter,
                "Tier C distribution version {actual} does not match current version {expected}",
            ),
            Self::InvalidScenarioId {
                scenario_id,
                source,
            } => write!(
                formatter,
                "invalid Tier C scenario ID {scenario_id:?}: {source}"
            ),
            Self::InvalidStableLabel(scenario_id) => write!(
                formatter,
                "Tier C scenario {scenario_id:?} contains an invalid stable coverage label",
            ),
            Self::InconsistentMutationLabels => formatter.write_str(
                "Tier C statement and mutation coverage labels describe different operations",
            ),
            Self::MembershipMismatch {
                missing,
                unexpected,
            } => write!(
                formatter,
                "Tier C distribution membership drifted: missing {missing:?}, unexpected {unexpected:?}",
            ),
            Self::NonCanonicalArtifact => {
                formatter.write_str("Tier C distribution is not deterministic current JSON")
            }
            Self::OutcomeExpectationMismatch(scenario_id) => write!(
                formatter,
                "Tier C outcome disagrees with expected acceptance for {scenario_id:?}",
            ),
            Self::ReportDrift => formatter
                .write_str("Tier C distribution disagrees with recomputed declaration evidence"),
            Self::ScenarioCountOverflow => {
                formatter.write_str("Tier C distribution scenario count overflowed")
            }
            Self::Serialization(source) => {
                write!(
                    formatter,
                    "failed to serialize Tier C distribution: {source}"
                )
            }
        }
    }
}

impl Error for TierCDistributionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Decode { source } => Some(source),
            Self::FailedEvidence(source) => Some(source),
            Self::GeneratedCase(source) | Self::Serialization(source) => Some(source),
            Self::InvalidScenarioId { source, .. } => Some(source),
            _ => None,
        }
    }
}

// -----------------------------------------------------------------------------
// Source-owned declaration projections
// -----------------------------------------------------------------------------

/// Derive one scheduled declaration from a generated SELECT without inspecting rendered SQL.
///
/// # Errors
///
/// Returns a typed distribution or generator error when the supplied scenario
/// identity or embedded accepted-snapshot facts are invalid.
pub fn generated_select_tier_c_declaration(
    scenario_id: impl Into<String>,
    case: &GeneratedSelectCase,
) -> Result<TierCScenarioDeclaration, TierCDistributionError> {
    let scenario_id = scenario_id.into();
    let expected = match case.expected() {
        SelectExpectedOutcome::Accepted => TierCExpectedAcceptance::Accepted,
        SelectExpectedOutcome::Rejected(_) => TierCExpectedAcceptance::Rejected,
    };
    let (provider, provider_id, evidence_strength) = match case.provider() {
        SelectProvider::RejectionInvariant => (
            EligibleProvider::RejectionInvariant,
            "icydb.typed_rejection",
            EvidenceStrength::ContractAssertion,
        ),
        SelectProvider::SqliteReference => (
            EligibleProvider::SqliteReference,
            "sqlite.generated_select",
            EvidenceStrength::ReferenceOracle,
        ),
    };
    let query = case.query();
    let value_type = select_value_type(case).map_err(TierCDistributionError::GeneratedCase)?;
    let nullability = if case.features().contains(&SelectFeature::Null)
        || case.snapshot().fields().iter().any(|field| {
            case.fixture().rows().iter().any(|row| {
                row.value_by_field_id(field.id())
                    .is_some_and(crate::GeneratedValue::is_null)
            })
        }) {
        NullabilityClass::Nullable
    } else {
        NullabilityClass::NonNullable
    };
    let predicates = select_predicates(case);
    let window = select_window(query.order_term_count(), query.limit(), query.offset());
    let shape = match query.shape() {
        SelectQueryShape::GlobalAggregate => QueryShape::GlobalAggregate,
        SelectQueryShape::GroupedAggregate => QueryShape::Grouped,
        SelectQueryShape::Scalar => QueryShape::Scalar,
    };
    let labels = TierCCoverageLabels::try_new(
        BTreeSet::from([evidence_strength]),
        BTreeSet::from([MutationKind::None]),
        BTreeSet::from([nullability]),
        predicates,
        BTreeSet::from([provider]),
        BTreeSet::from([RouteFamily::NotContractual]),
        BTreeSet::from([shape]),
        BTreeSet::from([StatementFamily::Select]),
        BTreeSet::from([value_type]),
        BTreeSet::from([window]),
    )?;

    TierCScenarioDeclaration::try_new(
        scenario_id,
        case.family()
            .contract_features()
            .iter()
            .map(|feature| (*feature).to_string())
            .collect(),
        BTreeSet::from([provider_id.to_string()]),
        expected,
        labels,
    )
}

/// Derive one scheduled declaration from a generated mutation state machine.
///
/// # Errors
///
/// Returns a typed distribution error when the supplied scenario identity or
/// derived multi-label declaration violates the current evidence contract.
pub fn generated_mutation_tier_c_declaration(
    scenario_id: impl Into<String>,
    sequence: &GeneratedMutationSequence,
) -> Result<TierCScenarioDeclaration, TierCDistributionError> {
    let scenario_id = scenario_id.into();
    let mut contract_features = BTreeSet::new();
    let mut mutations = BTreeSet::new();
    let mut predicates = BTreeSet::new();
    let mut providers = BTreeSet::from([EligibleProvider::StateModelReference]);
    let mut provider_ids = BTreeSet::from(["state_model.generated_mutation".to_string()]);
    let mut statements = BTreeSet::new();
    let mut windows = BTreeSet::new();

    for step in sequence.steps() {
        if matches!(
            step.sqlite_eligibility(),
            MutationSqliteEligibility::Eligible
        ) {
            providers.insert(EligibleProvider::SqliteReference);
            provider_ids.insert("sqlite.generated_mutation".to_string());
        }
        let statement = step.statement();
        if statement.returning() {
            contract_features.insert("mutation.returning".to_string());
            contract_features.insert("returning.fields".to_string());
        }
        match statement.operation() {
            MutationOperation::Delete { predicate, window } => {
                contract_features.insert("mutation.delete".to_string());
                mutations.insert(MutationKind::Delete);
                statements.insert(StatementFamily::Delete);
                collect_mutation_predicates(predicate, &mut predicates);
                windows.insert(mutation_window(*window));
            }
            MutationOperation::Insert { .. } => {
                contract_features.insert("mutation.insert".to_string());
                mutations.insert(MutationKind::Insert);
                statements.insert(StatementFamily::Insert);
                predicates.insert(PredicateFamily::None);
                windows.insert(WindowBehavior::None);
            }
            MutationOperation::InsertFromQuery { predicate, .. } => {
                contract_features.insert("mutation.insert".to_string());
                mutations.insert(MutationKind::Insert);
                statements.insert(StatementFamily::Insert);
                collect_mutation_predicates(predicate, &mut predicates);
                windows.insert(WindowBehavior::Ordered);
            }
            MutationOperation::Update {
                predicate, window, ..
            } => {
                contract_features.insert("mutation.update".to_string());
                mutations.insert(MutationKind::Update);
                statements.insert(StatementFamily::Update);
                collect_mutation_predicates(predicate, &mut predicates);
                windows.insert(mutation_window(*window));
                if window.is_some() {
                    contract_features.insert("mutation.trusted_update_window".to_string());
                }
            }
        }
    }
    let labels = TierCCoverageLabels::try_new(
        BTreeSet::from([EvidenceStrength::ReferenceOracle]),
        mutations,
        BTreeSet::from([NullabilityClass::NonNullable]),
        predicates,
        providers,
        BTreeSet::from([RouteFamily::NotContractual]),
        BTreeSet::from([QueryShape::Mutation]),
        statements,
        BTreeSet::from([ValueTypeFamily::Mixed]),
        windows,
    )?;

    TierCScenarioDeclaration::try_new(
        scenario_id,
        contract_features,
        provider_ids,
        TierCExpectedAcceptance::Accepted,
        labels,
    )
}

fn select_value_type(case: &GeneratedSelectCase) -> Result<ValueTypeFamily, SqlGeneratorError> {
    let kinds = case
        .query()
        .projection_kinds(case.snapshot())?
        .into_iter()
        .map(|kind| match kind {
            SelectValueKind::Boolean => ValueTypeFamily::Boolean,
            SelectValueKind::Decimal | SelectValueKind::Integer => ValueTypeFamily::Numeric,
            SelectValueKind::Text => ValueTypeFamily::Text,
        })
        .collect::<BTreeSet<_>>();
    match kinds.len() {
        0 => Err(SqlGeneratorError::new(
            crate::SqlGeneratorErrorKind::InvalidCase,
            "generated Tier C SELECT must project at least one typed value",
        )),
        1 => kinds.first().copied().ok_or_else(|| {
            SqlGeneratorError::new(
                crate::SqlGeneratorErrorKind::InvalidCase,
                "generated Tier C SELECT lost its sole typed projection",
            )
        }),
        _ => Ok(ValueTypeFamily::Mixed),
    }
}

fn select_predicates(case: &GeneratedSelectCase) -> BTreeSet<PredicateFamily> {
    if !case.query().has_predicate() && !case.query().has_having() {
        return BTreeSet::from([PredicateFamily::None]);
    }

    let mut predicates = BTreeSet::new();
    if case.features().contains(&SelectFeature::Boolean) {
        predicates.insert(PredicateFamily::Compound);
    }
    if case.features().contains(&SelectFeature::Comparison) {
        predicates.insert(PredicateFamily::FieldComparison);
    }
    if case.features().contains(&SelectFeature::Text) {
        predicates.insert(PredicateFamily::Prefix);
    }
    if case.features().contains(&SelectFeature::Null) {
        predicates.insert(PredicateFamily::Boolean);
    }
    if predicates.is_empty() {
        predicates.insert(PredicateFamily::FieldComparison);
    }

    predicates
}

fn select_window(
    order_term_count: usize,
    limit: Option<u32>,
    offset: Option<u32>,
) -> WindowBehavior {
    let has_offset = offset.is_some_and(|offset| offset > 0);
    match (order_term_count > 0, limit, has_offset) {
        (false, None, _) => WindowBehavior::None,
        (false, Some(_), _) => WindowBehavior::Limit,
        (true, None, _) => WindowBehavior::Ordered,
        (true, Some(_), false) => WindowBehavior::OrderedLimit,
        (true, Some(_), true) => WindowBehavior::OrderedLimitOffset,
    }
}

fn collect_mutation_predicates(
    predicate: &MutationPredicate,
    predicates: &mut BTreeSet<PredicateFamily>,
) {
    match predicate {
        MutationPredicate::All => {
            predicates.insert(PredicateFamily::Boolean);
        }
        MutationPredicate::And { left, right } => {
            predicates.insert(PredicateFamily::Compound);
            collect_mutation_predicates(left, predicates);
            collect_mutation_predicates(right, predicates);
        }
        MutationPredicate::KeyEqual { .. } => {
            predicates.insert(PredicateFamily::PrimaryKey);
        }
        MutationPredicate::NumberRange { .. } => {
            predicates.insert(PredicateFamily::Range);
        }
        MutationPredicate::TextEqual { .. } => {
            predicates.insert(PredicateFamily::FieldComparison);
        }
    }
}

const fn mutation_window(window: Option<crate::MutationWindow>) -> WindowBehavior {
    match window {
        None => WindowBehavior::None,
        Some(window) if window.offset() == 0 => WindowBehavior::OrderedLimit,
        Some(_) => WindowBehavior::OrderedLimitOffset,
    }
}

// -----------------------------------------------------------------------------
// Distribution validation
// -----------------------------------------------------------------------------

fn validate_declarations(
    declarations: &[TierCScenarioDeclaration],
) -> Result<(), TierCDistributionError> {
    let mut declared = BTreeSet::new();
    for declaration in declarations {
        declaration.validate()?;
        if !declared.insert(declaration.scenario_id()) {
            return Err(TierCDistributionError::DuplicateScenarioId(
                declaration.scenario_id.clone(),
            ));
        }
    }

    Ok(())
}

fn validate_membership(
    declarations: &[TierCScenarioDeclaration],
    merged: &TierCMergedReport,
) -> Result<(), TierCDistributionError> {
    let declared = declarations
        .iter()
        .map(TierCScenarioDeclaration::scenario_id)
        .collect::<BTreeSet<_>>();
    let observed = merged
        .shard_reports()
        .iter()
        .flat_map(TierCShardReport::observations)
        .map(crate::TierCScenarioObservation::scenario_id)
        .collect::<BTreeSet<_>>();
    if declared != observed {
        return Err(TierCDistributionError::MembershipMismatch {
            missing: declared
                .difference(&observed)
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
            unexpected: observed
                .difference(&declared)
                .map(|scenario_id| (*scenario_id).to_string())
                .collect(),
        });
    }

    Ok(())
}

fn validate_outcome_expectations(
    declarations: &[TierCScenarioDeclaration],
    merged: &TierCMergedReport,
) -> Result<(), TierCDistributionError> {
    let outcomes = merged
        .shard_reports()
        .iter()
        .flat_map(TierCShardReport::observations)
        .map(|observation| (observation.scenario_id(), observation.outcome()))
        .collect::<BTreeMap<_, _>>();
    for declaration in declarations {
        let outcome = outcomes
            .get(declaration.scenario_id())
            .copied()
            .ok_or_else(|| TierCDistributionError::MembershipMismatch {
                missing: vec![declaration.scenario_id.clone()],
                unexpected: Vec::new(),
            })?;
        let aligned = matches!(
            (declaration.expected, outcome),
            (
                TierCExpectedAcceptance::Accepted,
                TierCScenarioOutcome::Passed | TierCScenarioOutcome::Failed(_)
            ) | (
                TierCExpectedAcceptance::Rejected,
                TierCScenarioOutcome::ExpectedRejection | TierCScenarioOutcome::Failed(_)
            )
        );
        if !aligned {
            return Err(TierCDistributionError::OutcomeExpectationMismatch(
                declaration.scenario_id.clone(),
            ));
        }
    }

    Ok(())
}

fn increment_labels<T: Ord>(
    counts: &mut BTreeMap<String, u32>,
    labels: &BTreeSet<T>,
    code: impl Fn(&T) -> &'static str,
) -> Result<(), TierCDistributionError> {
    for label in labels {
        increment_count(counts, code(label))?;
    }

    Ok(())
}

fn increment_count(
    counts: &mut BTreeMap<String, u32>,
    key: &str,
) -> Result<(), TierCDistributionError> {
    let count = counts.entry(key.to_string()).or_default();
    *count = count
        .checked_add(1)
        .ok_or(TierCDistributionError::ScenarioCountOverflow)?;

    Ok(())
}

fn bounded_count(count: usize) -> Result<u32, TierCDistributionError> {
    u32::try_from(count).map_err(|_| TierCDistributionError::ScenarioCountOverflow)
}

const fn validate_artifact_size(byte_count: usize) -> Result<(), TierCDistributionError> {
    if byte_count > TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES {
        return Err(TierCDistributionError::ArtifactTooLarge {
            observed_bytes: byte_count,
            maximum_bytes: TIER_C_EVIDENCE_MAX_ARTIFACT_BYTES,
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        EligibleProvider, EvidenceStrength, MutationKind, NullabilityClass, PredicateFamily,
        QueryShape, RouteFamily, SQL_SCHEDULED_SHARD_COUNT, StatementFamily,
        TierCCoverageDistributionReport, TierCCoverageLabels, TierCDistributionError,
        TierCExpectedAcceptance, TierCMergedReport, TierCScenarioDeclaration,
        TierCScenarioObservation, TierCScenarioOutcome, TierCShardReport, ValueTypeFamily,
        WindowBehavior, scheduled_sql_scenario_shard,
    };
    use std::collections::BTreeSet;

    #[test]
    fn multi_label_declarations_project_each_exercised_mutation_family() {
        let scenario_ids = one_scenario_per_shard();
        let declarations = scenario_ids
            .iter()
            .map(|scenario_id| mutation_declaration(scenario_id))
            .collect::<Vec<_>>();
        let merged = merged_receipts(&declarations);
        let report =
            TierCCoverageDistributionReport::try_from_clean_evidence(&declarations, &merged)
                .expect("exact clean declarations should project");

        assert_eq!(report.scenario_count(), 8);
        assert_eq!(report.statement_count(StatementFamily::Insert), 8);
        assert_eq!(report.statement_count(StatementFamily::Update), 8);
        assert_eq!(report.statement_count(StatementFamily::Delete), 8);
        assert_eq!(report.mutation_count(MutationKind::Insert), 8);
        assert_eq!(report.mutation_count(MutationKind::Update), 8);
        assert_eq!(report.mutation_count(MutationKind::Delete), 8);

        let encoded = report
            .to_canonical_json(&declarations, &merged)
            .expect("distribution should encode canonically");
        let decoded = TierCCoverageDistributionReport::from_canonical_json(
            encoded.as_slice(),
            &declarations,
            &merged,
        )
        .expect("distribution should decode canonically");
        assert_eq!(decoded, report);
    }

    #[test]
    fn distribution_rejects_inconsistent_operation_labels() {
        let labels = TierCCoverageLabels::try_new(
            BTreeSet::from([EvidenceStrength::ReferenceOracle]),
            BTreeSet::from([MutationKind::Insert]),
            BTreeSet::from([NullabilityClass::NonNullable]),
            BTreeSet::from([PredicateFamily::None]),
            BTreeSet::from([EligibleProvider::StateModelReference]),
            BTreeSet::from([RouteFamily::NotContractual]),
            BTreeSet::from([QueryShape::Mutation]),
            BTreeSet::from([StatementFamily::Delete]),
            BTreeSet::from([ValueTypeFamily::Mixed]),
            BTreeSet::from([WindowBehavior::None]),
        );

        assert!(matches!(
            labels,
            Err(TierCDistributionError::InconsistentMutationLabels)
        ));
    }

    fn mutation_declaration(scenario_id: &str) -> TierCScenarioDeclaration {
        let labels = TierCCoverageLabels::try_new(
            BTreeSet::from([EvidenceStrength::ReferenceOracle]),
            BTreeSet::from([
                MutationKind::Delete,
                MutationKind::Insert,
                MutationKind::Update,
            ]),
            BTreeSet::from([NullabilityClass::NonNullable]),
            BTreeSet::from([PredicateFamily::PrimaryKey, PredicateFamily::Range]),
            BTreeSet::from([
                EligibleProvider::SqliteReference,
                EligibleProvider::StateModelReference,
            ]),
            BTreeSet::from([RouteFamily::NotContractual]),
            BTreeSet::from([QueryShape::Mutation]),
            BTreeSet::from([
                StatementFamily::Delete,
                StatementFamily::Insert,
                StatementFamily::Update,
            ]),
            BTreeSet::from([ValueTypeFamily::Mixed]),
            BTreeSet::from([WindowBehavior::None, WindowBehavior::OrderedLimit]),
        )
        .expect("test labels should align");
        TierCScenarioDeclaration::try_new(
            scenario_id,
            BTreeSet::from([
                "mutation.delete".to_string(),
                "mutation.insert".to_string(),
                "mutation.update".to_string(),
            ]),
            BTreeSet::from([
                "sqlite.generated_mutation".to_string(),
                "state_model.generated_mutation".to_string(),
            ]),
            TierCExpectedAcceptance::Accepted,
            labels,
        )
        .expect("test declaration should validate")
    }

    fn merged_receipts(declarations: &[TierCScenarioDeclaration]) -> TierCMergedReport {
        let declared = declarations
            .iter()
            .map(TierCScenarioDeclaration::scenario_id)
            .collect::<Vec<_>>();
        let reports = (0..SQL_SCHEDULED_SHARD_COUNT)
            .map(|shard_index| {
                let observations = declared
                    .iter()
                    .filter(|scenario_id| {
                        scheduled_sql_scenario_shard(scenario_id)
                            .expect("test scenario should shard")
                            == shard_index
                    })
                    .map(|scenario_id| {
                        TierCScenarioObservation::try_new(
                            *scenario_id,
                            TierCScenarioOutcome::Passed,
                        )
                        .expect("test observation should validate")
                    })
                    .collect();
                TierCShardReport::try_new(shard_index, &declared, observations)
                    .expect("test shard should validate")
            })
            .collect();
        TierCMergedReport::try_merge(&declared, reports).expect("test merge should validate")
    }

    fn one_scenario_per_shard() -> Vec<String> {
        let mut by_shard = vec![None; usize::from(SQL_SCHEDULED_SHARD_COUNT)];
        for candidate in 0_u32..100_000 {
            let scenario_id = format!("distribution.scenario.{candidate}");
            let shard = scheduled_sql_scenario_shard(scenario_id.as_str())
                .expect("test scenario should shard");
            by_shard[usize::from(shard)].get_or_insert(scenario_id);
            if by_shard.iter().all(Option::is_some) {
                break;
            }
        }

        by_shard
            .into_iter()
            .map(|scenario_id| scenario_id.expect("search should populate every shard"))
            .collect()
    }
}
