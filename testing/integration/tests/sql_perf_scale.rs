//! Module: sql_perf_scale
//! Responsibility: typed normalized-cost and adjacent-cardinality slope arithmetic.
//! Does not own: fixture loading, scenario execution, candidate policy, or regression verdicts.
//! Boundary: derives only from measured counters and exact declared fixture cardinalities.

use crate::{
    MatrixSample, MatrixScenario, MatrixSurface, QueryShape, RouteFamily, ScaleFixtureFacts,
    ScalePayloadProfile,
    sql_perf_profile::{PerformanceProfile, PerformanceProfileError, scenario_set_hash},
};

use std::{
    cmp::Ordering,
    collections::{BTreeMap, BTreeSet},
    error::Error,
    fmt::{self, Display},
    num::NonZeroU64,
};

use serde::{Deserialize, Serialize};

///
/// NormalizedDenominator
///
/// Typed measured or declared unit eligible for instruction normalization.
/// Owned by scale arithmetic and consumed by P1 ranking and report construction.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum NormalizedDenominator {
    /// Declared and realized rows in the selected fixture surface.
    FixtureRow,

    /// Rows returned by the query result.
    ReturnedRow,

    /// Rows ingested by scalar aggregate reducers.
    AggregateRowIngested,

    /// Data-store get calls.
    DataStoreGet,

    /// Index range-scan calls.
    IndexRangeScan,

    /// Index entries read.
    IndexEntryRead,

    /// Projected output bytes.
    OutputByte,
}

impl NormalizedDenominator {
    /// Return the nonzero typed unit measured for one sample.
    pub(crate) fn measured_units(self, sample: &MatrixSample) -> Option<NonZeroU64> {
        let value = match self {
            Self::FixtureRow => sample.fixture_row_count,
            Self::ReturnedRow => u64::try_from(sample.outcome.row_count).ok()?,
            Self::AggregateRowIngested => sample.scalar_aggregate_rows_ingested,
            Self::DataStoreGet => sample.data_store_get_calls,
            Self::IndexRangeScan => sample.index_store_range_scan_calls,
            Self::IndexEntryRead => sample.index_store_entry_reads,
            Self::OutputByte => sample.output_blob_bytes,
        };

        NonZeroU64::new(value)
    }
}

/// Every denominator maintained by the current performance profile.
pub(crate) const NORMALIZED_DENOMINATORS: &[NormalizedDenominator] = &[
    NormalizedDenominator::FixtureRow,
    NormalizedDenominator::ReturnedRow,
    NormalizedDenominator::AggregateRowIngested,
    NormalizedDenominator::DataStoreGet,
    NormalizedDenominator::IndexRangeScan,
    NormalizedDenominator::IndexEntryRead,
    NormalizedDenominator::OutputByte,
];

/// Expected grouped live-state shape for one scale sentinel.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum GroupedScaleStateExpectation {
    /// Ordered execution retains only the group currently receiving rows.
    SingleActiveGroup,

    /// Hash execution retains every observed group until finalization.
    RetainedGroupTable,
}

/// Grouped physical-state and semantic-pair contract for one scale sentinel.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct GroupedScaleExpectation {
    /// Stable ordered/hash pair identity, when an equivalent control exists.
    pub(crate) pair_id: Option<&'static str>,

    /// Required physical live-state shape.
    pub(crate) state: GroupedScaleStateExpectation,
}

impl GroupedScaleExpectation {
    /// Construct one grouped physical-state expectation.
    const fn new(pair_id: Option<&'static str>, state: GroupedScaleStateExpectation) -> Self {
        Self { pair_id, state }
    }
}

///
/// ScaleSentinelSpec
///
/// Checked-in identity and expected facts for one scale-sentinel family.
/// Owned by the scale profile; execution clones the named P1 scenario metadata.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ScaleSentinelSpec {
    /// Stable identity shared by all cardinalities in this sentinel family.
    pub(crate) sentinel_id: &'static str,

    /// Existing P1 scenario whose typed metadata and confirmation represent the stratum.
    pub(crate) p1_scenario_id: &'static str,

    /// Optional scale-only SQL payload when exact selectivity needs a narrower predicate.
    pub(crate) sql_override: Option<&'static str>,

    /// Surface whose otherwise-empty fixture is loaded for measurement.
    pub(crate) surface: MatrixSurface,

    /// Required typed route family at every scale cardinality.
    pub(crate) route_family: RouteFamily,

    /// Reviewed exact selectivity class.
    pub(crate) selectivity: ScaleSelectivity,

    /// Declared result window, or no window for aggregate sentinels.
    pub(crate) result_window: Option<u32>,

    /// Required blob-payload distribution for the surface.
    pub(crate) payload_profile: ScalePayloadProfile,

    /// Grouped physical-state and semantic-pair contract, when applicable.
    pub(crate) grouped: Option<GroupedScaleExpectation>,
}

const SCALE_SENTINEL_SPECS: &[ScaleSentinelSpec] = &[
    ScaleSentinelSpec {
        sentinel_id: "user.primary_order.all.window1",
        p1_scenario_id: "user.select.pk.all.pk_asc.limit1",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::PrimaryOrder,
        selectivity: ScaleSelectivity::All,
        result_window: Some(1),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.primary_order.one.window1",
        p1_scenario_id: "user.select.pk.pk_range.pk_asc.limit1",
        sql_override: Some("SELECT id FROM PerfAuditUser WHERE id = 1 ORDER BY id ASC LIMIT 1"),
        surface: MatrixSurface::User,
        route_family: RouteFamily::PrimaryOrder,
        selectivity: ScaleSelectivity::One,
        result_window: Some(1),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.secondary_order.quarter.window10",
        p1_scenario_id: "user.select.pk.age_range.age_asc.limit10",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::SecondaryOrder,
        selectivity: ScaleSelectivity::Quarter,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.secondary_order.zero.window1",
        p1_scenario_id: "user.select.pk.name_prefix.name_asc.limit1",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::SecondaryOrder,
        selectivity: ScaleSelectivity::Zero,
        result_window: Some(1),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "token.equality_prefix.quarter.window50",
        p1_scenario_id: "token.collection_id.sparse_in.page_only.limit50",
        sql_override: None,
        surface: MatrixSurface::Token,
        route_family: RouteFamily::EqualityPrefixOrderedSuffix,
        selectivity: ScaleSelectivity::Quarter,
        result_window: Some(50),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.incompatible_filter_order.quarter.window10",
        p1_scenario_id: "user.select.pk.age_range.name_asc.limit10",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::IncompatibleFilterFirstOrder,
        selectivity: ScaleSelectivity::Quarter,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "heap_user.materialized_order.all.window10",
        p1_scenario_id: "heap_user.select.pk.all.age_asc.limit10",
        sql_override: None,
        surface: MatrixSurface::HeapUser,
        route_family: RouteFamily::MaterializedOrder,
        selectivity: ScaleSelectivity::All,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.not_paginated.aggregate_all",
        p1_scenario_id: "user.aggregate.count_all",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::NotOrderedOrNotPaginated,
        selectivity: ScaleSelectivity::All,
        result_window: None,
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.not_paginated.aggregate_quarter",
        p1_scenario_id: "user.aggregate.count_active",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::NotOrderedOrNotPaginated,
        selectivity: ScaleSelectivity::Quarter,
        result_window: None,
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.residual_primary.quarter.window10",
        p1_scenario_id: "user.select.pk.active_true.pk_desc.limit10",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::ResidualFilterOrderedScan,
        selectivity: ScaleSelectivity::Quarter,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.unsupported_order.all.window10",
        p1_scenario_id: "user.select.pk.all.numeric_expr_asc.limit10",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::UnsupportedAccessKind,
        selectivity: ScaleSelectivity::All,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "blob.secondary_payload.quarter.window10",
        p1_scenario_id: "blob.select.payload.bucket_eq.bucket_label_asc.limit10",
        sql_override: None,
        surface: MatrixSurface::Blob,
        route_family: RouteFamily::SecondaryOrder,
        selectivity: ScaleSelectivity::Quarter,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::BlobCycleV1,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "heap_user.primary_order.all.window10",
        p1_scenario_id: "heap_user.select.pk.all.pk_asc.limit10",
        sql_override: None,
        surface: MatrixSurface::HeapUser,
        route_family: RouteFamily::PrimaryOrder,
        selectivity: ScaleSelectivity::All,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "journaled_user.primary_order.all.window10",
        p1_scenario_id: "journaled_user.select.pk.all.pk_asc.limit10",
        sql_override: None,
        surface: MatrixSurface::JournaledUser,
        route_family: RouteFamily::PrimaryOrder,
        selectivity: ScaleSelectivity::All,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: None,
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_hash.few_groups.sum.window1",
        p1_scenario_id: "user.grouped_baseline.hash_sum_age_control",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::Quarter,
        result_window: Some(1),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("few_groups.sum.window1"),
            GroupedScaleStateExpectation::RetainedGroupTable,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_hash.few_groups.count.window10",
        p1_scenario_id: "user.aggregate.group_age_count",
        sql_override: Some(
            "SELECT age, COUNT(*) FROM PerfAuditUser \
             WHERE age >= 0 AND age < 100 AND age = age \
             GROUP BY age ORDER BY age ASC LIMIT 10",
        ),
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("few_groups.count.window10"),
            GroupedScaleStateExpectation::RetainedGroupTable,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_ordered.few_groups.count.window10",
        p1_scenario_id: "user.aggregate.group_age_count",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(10),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("few_groups.count.window10"),
            GroupedScaleStateExpectation::SingleActiveGroup,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_ordered.few_groups.sum.window1",
        p1_scenario_id: "user.grouped_baseline.ordered_sum_age",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::Quarter,
        result_window: Some(1),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("few_groups.sum.window1"),
            GroupedScaleStateExpectation::SingleActiveGroup,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_hash.many_groups.sum.window16",
        p1_scenario_id: "user.grouped_scale.hash_name_sum_window16",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(16),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("many_groups.sum.window16"),
            GroupedScaleStateExpectation::RetainedGroupTable,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_ordered.many_groups.sum.window16",
        p1_scenario_id: "user.grouped_scale.ordered_name_sum_window16",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(16),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("many_groups.sum.window16"),
            GroupedScaleStateExpectation::SingleActiveGroup,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_hash.many_groups.having_sum.window16",
        p1_scenario_id: "user.grouped_scale.hash_name_having_sum_window16",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(16),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("many_groups.having_sum.window16"),
            GroupedScaleStateExpectation::RetainedGroupTable,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_ordered.many_groups.having_sum.window16",
        p1_scenario_id: "user.grouped_scale.ordered_name_having_sum_window16",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(16),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            Some("many_groups.having_sum.window16"),
            GroupedScaleStateExpectation::SingleActiveGroup,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_hash.few_groups.distinct_nat.window16",
        p1_scenario_id: "user.grouped_scale.hash_age_distinct_nat_window16",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(16),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            None,
            GroupedScaleStateExpectation::RetainedGroupTable,
        )),
    },
    ScaleSentinelSpec {
        sentinel_id: "user.grouped_ordered.many_groups.count.window100",
        p1_scenario_id: "user.grouped_scale.ordered_name_count_window100",
        sql_override: None,
        surface: MatrixSurface::User,
        route_family: RouteFamily::GroupedAggregate,
        selectivity: ScaleSelectivity::All,
        result_window: Some(100),
        payload_profile: ScalePayloadProfile::NotApplicable,
        grouped: Some(GroupedScaleExpectation::new(
            None,
            GroupedScaleStateExpectation::SingleActiveGroup,
        )),
    },
];

/// Return the complete reviewed scale-sentinel profile.
pub(crate) const fn scale_sentinel_specs() -> &'static [ScaleSentinelSpec] {
    SCALE_SENTINEL_SPECS
}

///
/// ScaleScenarioDeclaration
///
/// One exact scale-sentinel execution declared at one reviewed cardinality.
/// Owned by the scale profile and executed independently in its assigned shard.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ScaleScenarioDeclaration {
    /// Sentinel-family facts shared by all three cardinalities.
    pub(crate) spec: ScaleSentinelSpec,

    /// Exact fixture cardinality required for this execution.
    pub(crate) fixture_rows: u32,

    /// P1-derived typed scenario with the scale-specific stable identity.
    pub(crate) scenario: MatrixScenario,
}

/// Construct and validate the complete exact scale scenario set.
///
/// # Errors
///
/// Returns a typed profile error for missing or duplicated P1 ownership,
/// cardinality/window drift, a surface mismatch, or incomplete route/selectivity coverage.
pub(crate) fn scale_scenario_declarations(
    profile: PerformanceProfile,
    p1_scenarios: &[MatrixScenario],
) -> Result<Vec<ScaleScenarioDeclaration>, ScaleProfileError> {
    profile
        .validate()
        .map_err(ScaleProfileError::InvalidProfile)?;
    let p1_by_id = p1_scenarios
        .iter()
        .map(|scenario| (scenario.key.as_str(), scenario))
        .collect::<BTreeMap<_, _>>();
    if p1_by_id.len() != p1_scenarios.len() {
        return Err(ScaleProfileError::DuplicateP1ScenarioId);
    }

    validate_scale_spec_coverage(profile)?;
    let mut declarations = Vec::new();
    let mut declaration_ids = BTreeSet::new();
    for spec in scale_sentinel_specs() {
        let p1_scenario = p1_by_id
            .get(spec.p1_scenario_id)
            .copied()
            .ok_or_else(|| ScaleProfileError::MissingP1Scenario(spec.p1_scenario_id.to_string()))?;
        if p1_scenario.surface != spec.surface {
            return Err(ScaleProfileError::P1SurfaceDrift {
                scenario_id: spec.p1_scenario_id.to_string(),
                expected: spec.surface.label().to_string(),
                actual: p1_scenario.surface.label().to_string(),
            });
        }
        let p1_window = p1_scenario
            .metadata
            .window
            .limit
            .map(u32::try_from)
            .transpose()
            .map_err(|_| ScaleProfileError::WindowOverflow(spec.p1_scenario_id.to_string()))?;
        if p1_window != spec.result_window {
            return Err(ScaleProfileError::P1WindowDrift {
                scenario_id: spec.p1_scenario_id.to_string(),
                expected: spec.result_window,
                actual: p1_window,
            });
        }

        for fixture_rows in profile.scale_row_cardinalities() {
            let mut scenario = p1_scenario.clone();
            scenario.key = format!("scale.{}.rows{fixture_rows}", spec.sentinel_id);
            scenario.family = format!("scale.{}", spec.sentinel_id);
            if let Some(sql) = spec.sql_override {
                scenario.sql = sql.to_string();
            }
            if !declaration_ids.insert(scenario.key.clone()) {
                return Err(ScaleProfileError::DuplicateScaleScenarioId(scenario.key));
            }
            declarations.push(ScaleScenarioDeclaration {
                spec: *spec,
                fixture_rows: *fixture_rows,
                scenario,
            });
        }
    }
    declarations.sort_by(|left, right| {
        left.spec
            .sentinel_id
            .cmp(right.spec.sentinel_id)
            .then_with(|| left.fixture_rows.cmp(&right.fixture_rows))
    });
    if declarations.len() != profile.expected_scale_scenario_count() {
        return Err(ScaleProfileError::ScaleScenarioCountDrift {
            expected: profile.expected_scale_scenario_count(),
            actual: declarations.len(),
        });
    }
    let observed_hash = scenario_set_hash(
        declarations
            .iter()
            .map(|declaration| declaration.scenario.key.as_str()),
    )
    .map_err(ScaleProfileError::InvalidScaleScenarioSet)?;
    if observed_hash != profile.expected_scale_scenario_set_hash() {
        return Err(ScaleProfileError::ScaleScenarioSetHashDrift {
            expected: profile.expected_scale_scenario_set_hash(),
            actual: observed_hash,
        });
    }
    let mut shard_counts = vec![0_usize; usize::from(profile.shard_count())];
    for declaration in &declarations {
        let shard_index = profile
            .scenario_shard(&declaration.scenario.key)
            .map_err(ScaleProfileError::InvalidScaleScenarioSet)?;
        shard_counts[usize::from(shard_index)] += 1;
    }
    if shard_counts != profile.expected_scale_shard_counts() {
        return Err(ScaleProfileError::ScaleShardCountDrift {
            expected: profile.expected_scale_shard_counts().to_vec(),
            actual: shard_counts,
        });
    }

    Ok(declarations)
}

fn validate_scale_spec_coverage(profile: PerformanceProfile) -> Result<(), ScaleProfileError> {
    let sentinel_ids = scale_sentinel_specs()
        .iter()
        .map(|spec| spec.sentinel_id)
        .collect::<BTreeSet<_>>();
    if sentinel_ids.len() != scale_sentinel_specs().len() {
        return Err(ScaleProfileError::DuplicateSentinelId);
    }
    let observed_routes = scale_sentinel_specs()
        .iter()
        .map(|spec| spec.route_family)
        .collect::<BTreeSet<_>>();
    let expected_routes = [
        RouteFamily::EqualityPrefixOrderedSuffix,
        RouteFamily::GroupedAggregate,
        RouteFamily::IncompatibleFilterFirstOrder,
        RouteFamily::MaterializedOrder,
        RouteFamily::NotOrderedOrNotPaginated,
        RouteFamily::PrimaryOrder,
        RouteFamily::ResidualFilterOrderedScan,
        RouteFamily::SecondaryOrder,
        RouteFamily::UnsupportedAccessKind,
    ]
    .into_iter()
    .collect::<BTreeSet<_>>();
    if observed_routes != expected_routes {
        return Err(ScaleProfileError::RouteCoverageDrift);
    }
    let observed_selectivity = scale_sentinel_specs()
        .iter()
        .map(|spec| spec.selectivity)
        .collect::<BTreeSet<_>>();
    if observed_selectivity
        != [
            ScaleSelectivity::Zero,
            ScaleSelectivity::One,
            ScaleSelectivity::Quarter,
            ScaleSelectivity::All,
        ]
        .into_iter()
        .collect()
    {
        return Err(ScaleProfileError::SelectivityCoverageDrift);
    }
    let observed_windows = scale_sentinel_specs()
        .iter()
        .filter_map(|spec| spec.result_window)
        .collect::<BTreeSet<_>>();
    let expected_windows = profile
        .result_window_sizes()
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    if observed_windows != expected_windows {
        return Err(ScaleProfileError::WindowCoverageDrift);
    }
    if scale_sentinel_specs().iter().any(|spec| {
        (spec.surface == MatrixSurface::Blob)
            != (spec.payload_profile == ScalePayloadProfile::BlobCycleV1)
    }) {
        return Err(ScaleProfileError::PayloadProfileDrift);
    }
    let mut grouped_pairs = BTreeMap::<&str, Vec<GroupedScaleStateExpectation>>::new();
    for spec in scale_sentinel_specs() {
        if (spec.route_family == RouteFamily::GroupedAggregate) != spec.grouped.is_some() {
            return Err(ScaleProfileError::GroupedContractDrift);
        }
        if let Some(grouped) = spec.grouped
            && let Some(pair_id) = grouped.pair_id
        {
            grouped_pairs
                .entry(pair_id)
                .or_default()
                .push(grouped.state);
        }
    }
    if grouped_pairs.values().any(|states| {
        states.as_slice()
            != [
                GroupedScaleStateExpectation::SingleActiveGroup,
                GroupedScaleStateExpectation::RetainedGroupTable,
            ]
            && states.as_slice()
                != [
                    GroupedScaleStateExpectation::RetainedGroupTable,
                    GroupedScaleStateExpectation::SingleActiveGroup,
                ]
    }) {
        return Err(ScaleProfileError::GroupedContractDrift);
    }

    Ok(())
}

///
/// ScaleProfileError
///
/// Typed failure while materializing the reviewed scale profile from P1 authority.
/// Owned by scale declaration construction and preserved by scheduled runners.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScaleProfileError {
    /// More than one P1 declaration has the same stable scenario identity.
    DuplicateP1ScenarioId,

    /// More than one scale scenario has the same stable exact-cardinality identity.
    DuplicateScaleScenarioId(String),

    /// More than one sentinel family has the same stable identity.
    DuplicateSentinelId,

    /// The checked-in performance profile is invalid.
    InvalidProfile(PerformanceProfileError),

    /// Grouped scale sentinels lack one exact physical-state or pair contract.
    GroupedContractDrift,

    /// The materialized scale scenario set has an invalid canonical identity.
    InvalidScaleScenarioSet(PerformanceProfileError),

    /// A scale sentinel references no current P1 scenario.
    MissingP1Scenario(String),

    /// Blob payload-profile ownership drifted from the blob surface.
    PayloadProfileDrift,

    /// One P1 scenario belongs to a surface other than its scale declaration.
    P1SurfaceDrift {
        /// Stable P1 scenario identity.
        scenario_id: String,
        /// Expected scale surface.
        expected: String,
        /// Actual P1 surface.
        actual: String,
    },

    /// One P1 scenario's result window differs from its scale declaration.
    P1WindowDrift {
        /// Stable P1 scenario identity.
        scenario_id: String,
        /// Expected result window.
        expected: Option<u32>,
        /// Actual P1 result window.
        actual: Option<u32>,
    },

    /// The reviewed route-family set is incomplete or contains an undeclared route.
    RouteCoverageDrift,

    /// The materialized scale scenario count differs from the checked-in profile.
    ScaleScenarioCountDrift {
        /// Checked-in exact scenario count.
        expected: usize,
        /// Materialized scenario count.
        actual: usize,
    },

    /// The materialized scale scenario-set identity differs from the checked-in profile.
    ScaleScenarioSetHashDrift {
        /// Checked-in scale scenario-set identity.
        expected: &'static str,
        /// Materialized scale scenario-set identity.
        actual: String,
    },

    /// Deterministic scale shard membership counts drifted.
    ScaleShardCountDrift {
        /// Checked-in count for each zero-based shard.
        expected: Vec<usize>,
        /// Materialized count for each zero-based shard.
        actual: Vec<usize>,
    },

    /// The reviewed zero/one/quarter/all selectivity set is incomplete.
    SelectivityCoverageDrift,

    /// One P1 result window cannot be represented by the scale artifact.
    WindowOverflow(String),

    /// The reviewed result-window set is incomplete.
    WindowCoverageDrift,
}

impl Display for ScaleProfileError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DuplicateP1ScenarioId => {
                formatter.write_str("P1 scenario declarations contain a duplicate identity")
            }
            Self::DuplicateScaleScenarioId(scenario_id) => {
                write!(
                    formatter,
                    "duplicate scale scenario identity {scenario_id:?}"
                )
            }
            Self::DuplicateSentinelId => {
                formatter.write_str("scale sentinel declarations contain a duplicate identity")
            }
            Self::InvalidProfile(error) => {
                write!(formatter, "invalid performance profile: {error}")
            }
            Self::GroupedContractDrift => {
                formatter.write_str("grouped scale physical-state contract drifted")
            }
            Self::InvalidScaleScenarioSet(error) => {
                write!(formatter, "invalid scale scenario set: {error}")
            }
            Self::MissingP1Scenario(scenario_id) => {
                write!(
                    formatter,
                    "scale sentinel references missing P1 scenario {scenario_id:?}"
                )
            }
            Self::PayloadProfileDrift => {
                formatter.write_str("scale blob payload-profile ownership drifted")
            }
            Self::P1SurfaceDrift {
                scenario_id,
                expected,
                actual,
            } => write!(
                formatter,
                "scale P1 scenario {scenario_id:?} surface drifted: expected {expected:?}, observed {actual:?}",
            ),
            Self::P1WindowDrift {
                scenario_id,
                expected,
                actual,
            } => write!(
                formatter,
                "scale P1 scenario {scenario_id:?} window drifted: expected {expected:?}, observed {actual:?}",
            ),
            Self::RouteCoverageDrift => formatter.write_str("scale route-family coverage drifted"),
            Self::ScaleScenarioCountDrift { expected, actual } => write!(
                formatter,
                "scale scenario count drifted: expected {expected}, observed {actual}",
            ),
            Self::ScaleScenarioSetHashDrift { expected, actual } => write!(
                formatter,
                "scale scenario-set hash drifted: expected {expected}, observed {actual}",
            ),
            Self::ScaleShardCountDrift { expected, actual } => write!(
                formatter,
                "scale shard counts drifted: expected {expected:?}, observed {actual:?}",
            ),
            Self::SelectivityCoverageDrift => {
                formatter.write_str("scale selectivity coverage drifted")
            }
            Self::WindowOverflow(scenario_id) => write!(
                formatter,
                "scale P1 scenario {scenario_id:?} window cannot be represented",
            ),
            Self::WindowCoverageDrift => {
                formatter.write_str("scale result-window coverage drifted")
            }
        }
    }
}

impl Error for ScaleProfileError {}

///
/// ScaleSelectivity
///
/// Reviewed predicate selectivity represented by one scale sentinel.
/// Owned by the scale profile and resolved only from canister-returned fixture facts.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ScaleSelectivity {
    /// The declared predicate matches no fixture rows.
    Zero,

    /// The declared exact-key predicate matches one fixture row.
    One,

    /// The declared predicate matches exactly one quarter of fixture rows.
    Quarter,

    /// The declared predicate matches every fixture row.
    All,
}

impl ScaleSelectivity {
    /// Return the exact realized match count from typed fixture facts.
    pub(crate) const fn realized_rows(self, facts: &ScaleFixtureFacts) -> u32 {
        match self {
            Self::Zero => facts.zero_match_rows,
            Self::One => facts.one_match_rows,
            Self::Quarter => facts.quarter_match_rows,
            Self::All => facts.all_match_rows,
        }
    }

    /// Return the stable report code for this selectivity class.
    pub(crate) const fn code(self) -> &'static str {
        match self {
            Self::Zero => "zero",
            Self::One => "one",
            Self::Quarter => "quarter",
            Self::All => "all",
        }
    }
}

///
/// NormalizedCost
///
/// Exact instruction numerator and nonzero typed-unit denominator.
/// Owned by scale arithmetic; consumers compare the rational value without floats.
///

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct NormalizedCost {
    /// Measured local instructions.
    pub(crate) local_instructions: u64,

    /// Nonzero measured or declared units.
    pub(crate) units: NonZeroU64,
}

///
/// ScaleNormalizedObservation
///
/// One exact normalized-cost projection from a retained scale sample.
/// Owned by scale evidence and emitted only for a nonzero typed denominator.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleNormalizedObservation {
    /// Stable exact-cardinality scale scenario identity.
    pub(crate) scenario_id: String,

    /// Typed measured or declared normalization unit.
    pub(crate) denominator: NormalizedDenominator,

    /// Exact instruction numerator and nonzero unit denominator.
    pub(crate) cost: NormalizedCost,
}

/// Derive an exact normalized cost when the typed denominator is nonzero.
pub(crate) fn normalized_cost(
    denominator: NormalizedDenominator,
    sample: &MatrixSample,
) -> Option<NormalizedCost> {
    Some(NormalizedCost {
        local_instructions: sample.total_local_instructions,
        units: denominator.measured_units(sample)?,
    })
}

/// Project every eligible exact normalized cost from complete scale observations.
pub(crate) fn scale_normalized_costs(
    observations: &[ScaleObservation],
) -> Vec<ScaleNormalizedObservation> {
    let mut normalized = observations
        .iter()
        .flat_map(|observation| {
            NORMALIZED_DENOMINATORS.iter().filter_map(|denominator| {
                Some(ScaleNormalizedObservation {
                    scenario_id: observation.scenario_id.clone(),
                    denominator: *denominator,
                    cost: normalized_cost(*denominator, &observation.sample)?,
                })
            })
        })
        .collect::<Vec<_>>();
    normalized.sort_by(|left, right| {
        left.scenario_id
            .cmp(&right.scenario_id)
            .then_with(|| left.denominator.cmp(&right.denominator))
    });

    normalized
}

/// Compare two samples by descending exact normalized cost, then stable ID.
pub(crate) fn compare_normalized_cost(
    denominator: NormalizedDenominator,
    left: &MatrixSample,
    right: &MatrixSample,
) -> Ordering {
    match (
        normalized_cost(denominator, left),
        normalized_cost(denominator, right),
    ) {
        (Some(left_cost), Some(right_cost)) => {
            let left_scaled = u128::from(left_cost.local_instructions)
                .saturating_mul(u128::from(right_cost.units.get()));
            let right_scaled = u128::from(right_cost.local_instructions)
                .saturating_mul(u128::from(left_cost.units.get()));

            right_scaled
                .cmp(&left_scaled)
                .then_with(|| left.key.cmp(&right.key))
        }
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => left.key.cmp(&right.key),
    }
}

///
/// ScaleObservation
///
/// One exact scale-sentinel result at a declared fixture cardinality.
/// Owned by scale evidence and grouped by stable sentinel identity for slopes.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ScaleObservation {
    /// Stable scale-sentinel family identity shared across cardinalities.
    pub(crate) sentinel_id: String,

    /// Stable scenario identity for this exact cardinality.
    pub(crate) scenario_id: String,

    /// Stable P1 scenario selected when this scale stratum requires confirmation.
    pub(crate) p1_scenario_id: String,

    /// Reviewed predicate selectivity class.
    pub(crate) selectivity: ScaleSelectivity,

    /// Exact rows matching the predicate under the realized fixture.
    pub(crate) predicate_match_rows: u32,

    /// Declared result window, or no window for aggregates.
    pub(crate) result_window: Option<u32>,

    /// Complete realized scale-fixture facts returned by the canister.
    pub(crate) fixture: ScaleFixtureFacts,

    /// Complete attributed SQL sample produced against the realized fixture.
    pub(crate) sample: MatrixSample,
}

/// Build one validated exact-cardinality scale observation from live fixture evidence.
///
/// # Errors
///
/// Returns a typed evidence error when the loader facts, retained sample,
/// route, result cardinality, or declaration identity drift.
pub(crate) fn build_scale_observation(
    declaration: &ScaleScenarioDeclaration,
    fixture: ScaleFixtureFacts,
    mut sample: MatrixSample,
) -> Result<ScaleObservation, ScaleEvidenceError> {
    sample.fixture_row_count = u64::from(declaration.fixture_rows);
    let expected_route_family = declaration.spec.route_family.code();
    if sample.key != declaration.scenario.key
        || sample.surface != declaration.spec.surface.label()
        || sample.family != declaration.scenario.family
        || sample.sql != declaration.scenario.sql
    {
        return Err(ScaleEvidenceError::DeclarationDrift(
            declaration.scenario.key.clone(),
        ));
    }
    if sample.route_family != expected_route_family {
        return Err(ScaleEvidenceError::RouteFamilyDrift {
            scenario_id: declaration.scenario.key.clone(),
            expected: expected_route_family.to_string(),
            actual: sample.route_family,
        });
    }
    validate_grouped_scale_state(declaration, &sample)?;
    if fixture.profile_version != 1
        || fixture.surface != declaration.spec.surface.label()
        || fixture.fixture_rows != declaration.fixture_rows
        || fixture.payload_profile != declaration.spec.payload_profile
    {
        return Err(ScaleEvidenceError::FixtureFactDrift(
            declaration.scenario.key.clone(),
        ));
    }
    let predicate_match_rows = declaration.spec.selectivity.realized_rows(&fixture);
    let expected_result_rows = match declaration.scenario.metadata.shape {
        QueryShape::Scalar => Some(
            declaration
                .spec
                .result_window
                .map_or(predicate_match_rows, |window| {
                    predicate_match_rows.min(window)
                }),
        ),
        // A global aggregate ingests its matched inputs but emits one result row.
        QueryShape::GlobalAggregate => Some(1),
        QueryShape::Grouped | QueryShape::Metadata | QueryShape::Mutation => None,
    };
    if let Some(expected_result_rows) = expected_result_rows {
        let actual_result_rows = u32::try_from(sample.outcome.row_count).map_err(|_| {
            ScaleEvidenceError::ResultCardinalityDrift {
                scenario_id: declaration.scenario.key.clone(),
                expected: expected_result_rows,
                actual: sample.outcome.row_count,
            }
        })?;
        if actual_result_rows != expected_result_rows {
            return Err(ScaleEvidenceError::ResultCardinalityDrift {
                scenario_id: declaration.scenario.key.clone(),
                expected: expected_result_rows,
                actual: sample.outcome.row_count,
            });
        }
    }

    let observation = ScaleObservation {
        sentinel_id: declaration.spec.sentinel_id.to_string(),
        scenario_id: declaration.scenario.key.clone(),
        p1_scenario_id: declaration.spec.p1_scenario_id.to_string(),
        selectivity: declaration.spec.selectivity,
        predicate_match_rows,
        result_window: declaration.spec.result_window,
        fixture,
        sample,
    };
    validate_observation_facts(&observation)?;

    Ok(observation)
}

fn validate_grouped_scale_state(
    declaration: &ScaleScenarioDeclaration,
    sample: &MatrixSample,
) -> Result<(), ScaleEvidenceError> {
    let Some(grouped) = declaration.spec.grouped else {
        return Ok(());
    };
    let valid = sample.grouped_groups_observed > 0
        && sample.grouped_groups_finalized == sample.grouped_groups_observed
        && match grouped.state {
            GroupedScaleStateExpectation::SingleActiveGroup => {
                sample.grouped_peak_live_groups == 1
                    && sample.grouped_peak_live_aggregate_states <= 1
            }
            GroupedScaleStateExpectation::RetainedGroupTable => {
                sample.grouped_groups_observed > 1
                    && sample.grouped_peak_live_groups == sample.grouped_groups_observed
                    && sample.grouped_peak_live_aggregate_states <= sample.grouped_groups_observed
            }
        };
    if !valid {
        return Err(ScaleEvidenceError::GroupedStateDrift {
            scenario_id: declaration.scenario.key.clone(),
            expected: grouped.state,
            groups_observed: sample.grouped_groups_observed,
            groups_finalized: sample.grouped_groups_finalized,
            peak_live_groups: sample.grouped_peak_live_groups,
            peak_live_aggregate_states: sample.grouped_peak_live_aggregate_states,
        });
    }

    Ok(())
}

/// Validate every declared ordered/hash scale pair against merged observations.
///
/// # Errors
///
/// Returns a typed error when a pair is incomplete or produces different
/// public grouped results at the same fixture cardinality.
pub(crate) fn validate_grouped_scale_pairs(
    declarations: &[ScaleScenarioDeclaration],
    observations: &[ScaleObservation],
) -> Result<(), ScaleEvidenceError> {
    let observed_by_id = observations
        .iter()
        .map(|observation| (observation.scenario_id.as_str(), observation))
        .collect::<BTreeMap<_, _>>();
    let mut pairs = BTreeMap::<(&str, u32), Vec<&ScaleObservation>>::new();
    for declaration in declarations {
        let Some(pair_id) = declaration.spec.grouped.and_then(|grouped| grouped.pair_id) else {
            continue;
        };
        let observation = observed_by_id
            .get(declaration.scenario.key.as_str())
            .copied()
            .ok_or_else(|| ScaleEvidenceError::GroupedPairDrift {
                pair_id: pair_id.to_string(),
                fixture_rows: declaration.fixture_rows,
            })?;
        pairs
            .entry((pair_id, declaration.fixture_rows))
            .or_default()
            .push(observation);
    }
    for ((pair_id, fixture_rows), pair) in pairs {
        let same_result = pair
            .first()
            .and_then(|observation| observation.sample.result_signature.as_ref())
            .is_some_and(|expected| {
                pair.len() == 2
                    && pair.iter().all(|observation| {
                        observation.sample.result_signature.as_ref() == Some(expected)
                    })
            });
        if !same_result {
            return Err(ScaleEvidenceError::GroupedPairDrift {
                pair_id: pair_id.to_string(),
                fixture_rows,
            });
        }
    }

    Ok(())
}

/// Validate one retained observation against its current scale declaration.
///
/// # Errors
///
/// Returns a typed evidence error when any serialized fixture, sample, route,
/// selectivity, or declaration fact differs from freshly derived evidence.
pub(crate) fn validate_scale_observation(
    declaration: &ScaleScenarioDeclaration,
    observation: &ScaleObservation,
) -> Result<(), ScaleEvidenceError> {
    let expected = build_scale_observation(
        declaration,
        observation.fixture.clone(),
        observation.sample.clone(),
    )?;
    if &expected != observation {
        return Err(ScaleEvidenceError::DeclarationDrift(
            observation.scenario_id.clone(),
        ));
    }

    Ok(())
}

///
/// AdjacentScaleSlope
///
/// Exact signed instruction change per positive row-cardinality change.
/// Owned by scale evidence and never interpreted as a cost slope across route changes.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AdjacentScaleSlope {
    /// Stable scale-sentinel family identity.
    pub(crate) sentinel_id: String,

    /// Lower adjacent fixture cardinality.
    pub(crate) from_fixture_rows: u32,

    /// Higher adjacent fixture cardinality.
    pub(crate) to_fixture_rows: u32,

    /// Signed instruction change between observations.
    pub(crate) instruction_delta: i128,

    /// Positive fixture-row change between observations.
    pub(crate) row_delta: u32,

    /// Route at the lower cardinality.
    pub(crate) from_route_family: String,

    /// Route at the higher cardinality.
    pub(crate) to_route_family: String,

    /// Whether route identity changed across the adjacent pair.
    pub(crate) route_changed: bool,
}

/// Derive exact slopes for every sentinel with the complete cardinality ladder.
///
/// # Errors
///
/// Returns a typed error for an invalid ladder, empty identity, duplicate or
/// unexpected observation, or missing required cardinality.
pub(crate) fn adjacent_scale_slopes(
    expected_cardinalities: &[u32],
    observations: &[ScaleObservation],
) -> Result<Vec<AdjacentScaleSlope>, ScaleEvidenceError> {
    validate_cardinality_ladder(expected_cardinalities)?;
    let expected = expected_cardinalities
        .iter()
        .copied()
        .collect::<BTreeSet<_>>();
    let mut grouped = BTreeMap::<&str, BTreeMap<u32, &ScaleObservation>>::new();
    for observation in observations {
        if observation.sentinel_id.is_empty() {
            return Err(ScaleEvidenceError::EmptySentinelId);
        }
        if observation.scenario_id.is_empty() {
            return Err(ScaleEvidenceError::EmptyScenarioId);
        }
        if observation.p1_scenario_id.is_empty() {
            return Err(ScaleEvidenceError::EmptyP1ScenarioId);
        }
        validate_observation_facts(observation)?;
        if !expected.contains(&observation.fixture.fixture_rows) {
            return Err(ScaleEvidenceError::UnexpectedCardinality {
                sentinel_id: observation.sentinel_id.clone(),
                fixture_rows: observation.fixture.fixture_rows,
            });
        }
        let by_cardinality = grouped.entry(&observation.sentinel_id).or_default();
        if by_cardinality
            .insert(observation.fixture.fixture_rows, observation)
            .is_some()
        {
            return Err(ScaleEvidenceError::DuplicateCardinality {
                sentinel_id: observation.sentinel_id.clone(),
                fixture_rows: observation.fixture.fixture_rows,
            });
        }
    }

    let mut slopes = Vec::new();
    for (sentinel_id, by_cardinality) in grouped {
        for fixture_rows in expected_cardinalities {
            if !by_cardinality.contains_key(fixture_rows) {
                return Err(ScaleEvidenceError::MissingCardinality {
                    sentinel_id: sentinel_id.to_string(),
                    fixture_rows: *fixture_rows,
                });
            }
        }
        for pair in expected_cardinalities.windows(2) {
            let from = by_cardinality[&pair[0]];
            let to = by_cardinality[&pair[1]];
            let row_delta = to
                .fixture
                .fixture_rows
                .checked_sub(from.fixture.fixture_rows)
                .ok_or(ScaleEvidenceError::InvalidCardinalityLadder)?;
            let instruction_delta = i128::from(to.sample.total_local_instructions)
                - i128::from(from.sample.total_local_instructions);

            slopes.push(AdjacentScaleSlope {
                sentinel_id: sentinel_id.to_string(),
                from_fixture_rows: from.fixture.fixture_rows,
                to_fixture_rows: to.fixture.fixture_rows,
                instruction_delta,
                row_delta,
                from_route_family: from.sample.route_family.clone(),
                to_route_family: to.sample.route_family.clone(),
                route_changed: from.sample.route_family != to.sample.route_family,
            });
        }
    }

    Ok(slopes)
}

fn validate_observation_facts(observation: &ScaleObservation) -> Result<(), ScaleEvidenceError> {
    let fixture_rows = observation.fixture.fixture_rows;
    let expected_payload = if observation.sample.surface == "blob" {
        ScalePayloadProfile::BlobCycleV1
    } else {
        ScalePayloadProfile::NotApplicable
    };
    if observation.fixture.profile_version != 1
        || observation.fixture.surface != observation.sample.surface
        || fixture_rows == 0
        || observation.fixture.zero_match_rows != 0
        || observation.fixture.one_match_rows != 1
        || observation.fixture.quarter_match_rows != fixture_rows / 4
        || observation.fixture.all_match_rows != fixture_rows
        || observation.fixture.payload_profile != expected_payload
        || observation.predicate_match_rows
            != observation.selectivity.realized_rows(&observation.fixture)
        || observation.sample.key != observation.scenario_id
        || observation.sample.fixture_row_count != u64::from(fixture_rows)
    {
        return Err(ScaleEvidenceError::FixtureFactDrift(
            observation.scenario_id.clone(),
        ));
    }

    Ok(())
}

fn validate_cardinality_ladder(cardinalities: &[u32]) -> Result<(), ScaleEvidenceError> {
    if cardinalities.len() < 2
        || cardinalities[0] == 0
        || cardinalities.windows(2).any(|pair| pair[0] >= pair[1])
    {
        return Err(ScaleEvidenceError::InvalidCardinalityLadder);
    }

    Ok(())
}

///
/// ScaleEvidenceError
///
/// Typed failure while deriving normalized scale evidence.
/// Owned by scale arithmetic and preserved by later report or verdict boundaries.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ScaleEvidenceError {
    /// One retained sample differs from its typed scale declaration.
    DeclarationDrift(String),

    /// One scale observation lacks its P1 confirmation identity.
    EmptyP1ScenarioId,

    /// One scale observation lacks a stable scenario identity.
    EmptyScenarioId,

    /// One scale observation lacks a stable sentinel-family identity.
    EmptySentinelId,

    /// Realized fixture facts or their retained sample projection drifted.
    FixtureFactDrift(String),

    /// One grouped scale sample violated its declared live-state shape.
    GroupedStateDrift {
        /// Stable exact-cardinality scenario identity.
        scenario_id: String,
        /// Required grouped live-state shape.
        expected: GroupedScaleStateExpectation,
        /// Number of groups observed by the executor-owned runtime counter.
        groups_observed: u64,
        /// Number of groups finalized by the executor-owned runtime counter.
        groups_finalized: u64,
        /// Peak concurrently live grouped-key states.
        peak_live_groups: u64,
        /// Peak concurrently live aggregate states.
        peak_live_aggregate_states: u64,
    },

    /// One ordered/hash semantic pair is incomplete or returned different rows.
    GroupedPairDrift {
        /// Stable pair identity shared by both physical controls.
        pair_id: String,
        /// Exact fixture cardinality at which the pair drifted.
        fixture_rows: u32,
    },

    /// One result returned a row count other than its declared exact expectation.
    ResultCardinalityDrift {
        /// Stable exact-cardinality scenario identity.
        scenario_id: String,
        /// Exact expected result row count.
        expected: u32,
        /// Actual result row count.
        actual: usize,
    },

    /// One live sample observed a route other than the declared route family.
    RouteFamilyDrift {
        /// Stable exact-cardinality scenario identity.
        scenario_id: String,
        /// Required route-family code.
        expected: String,
        /// Observed route-family code.
        actual: String,
    },

    /// One sentinel has more than one observation at a required cardinality.
    DuplicateCardinality {
        /// Stable sentinel-family identity.
        sentinel_id: String,
        /// Duplicated fixture cardinality.
        fixture_rows: u32,
    },

    /// The checked-in cardinality ladder is empty, zero, duplicated, or unordered.
    InvalidCardinalityLadder,

    /// One sentinel lacks an observation at a required cardinality.
    MissingCardinality {
        /// Stable sentinel-family identity.
        sentinel_id: String,
        /// Missing fixture cardinality.
        fixture_rows: u32,
    },

    /// One observation names a cardinality outside the checked-in ladder.
    UnexpectedCardinality {
        /// Stable sentinel-family identity.
        sentinel_id: String,
        /// Unexpected fixture cardinality.
        fixture_rows: u32,
    },
}

impl Display for ScaleEvidenceError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeclarationDrift(scenario_id) => write!(
                formatter,
                "scale observation {scenario_id:?} differs from its declaration",
            ),
            Self::EmptyP1ScenarioId => {
                formatter.write_str("scale observation P1 scenario ID is empty")
            }
            Self::EmptyScenarioId => formatter.write_str("scale observation scenario ID is empty"),
            Self::EmptySentinelId => formatter.write_str("scale observation sentinel ID is empty"),
            Self::FixtureFactDrift(scenario_id) => write!(
                formatter,
                "scale observation {scenario_id:?} carries inconsistent fixture facts",
            ),
            Self::GroupedStateDrift {
                scenario_id,
                expected,
                groups_observed,
                groups_finalized,
                peak_live_groups,
                peak_live_aggregate_states,
            } => write!(
                formatter,
                "scale observation {scenario_id:?} violated grouped state {expected:?}: \
                 observed={groups_observed}, finalized={groups_finalized}, \
                 peak_groups={peak_live_groups}, peak_aggregate_states={peak_live_aggregate_states}",
            ),
            Self::GroupedPairDrift {
                pair_id,
                fixture_rows,
            } => write!(
                formatter,
                "grouped scale pair {pair_id:?} drifted at {fixture_rows} fixture rows",
            ),
            Self::ResultCardinalityDrift {
                scenario_id,
                expected,
                actual,
            } => write!(
                formatter,
                "scale observation {scenario_id:?} result cardinality drifted: expected {expected}, observed {actual}",
            ),
            Self::RouteFamilyDrift {
                scenario_id,
                expected,
                actual,
            } => write!(
                formatter,
                "scale observation {scenario_id:?} route drifted: expected {expected:?}, observed {actual:?}",
            ),
            Self::DuplicateCardinality {
                sentinel_id,
                fixture_rows,
            } => write!(
                formatter,
                "scale sentinel {sentinel_id:?} duplicates fixture cardinality {fixture_rows}",
            ),
            Self::InvalidCardinalityLadder => {
                formatter.write_str("scale cardinality ladder must be positive and increasing")
            }
            Self::MissingCardinality {
                sentinel_id,
                fixture_rows,
            } => write!(
                formatter,
                "scale sentinel {sentinel_id:?} lacks fixture cardinality {fixture_rows}",
            ),
            Self::UnexpectedCardinality {
                sentinel_id,
                fixture_rows,
            } => write!(
                formatter,
                "scale sentinel {sentinel_id:?} has unexpected fixture cardinality {fixture_rows}",
            ),
        }
    }
}

impl Error for ScaleEvidenceError {}

#[cfg(test)]
mod tests {
    use crate::{deterministic_matrix, report_matrix_sample};

    use super::*;

    fn sample(key: &str, total: u64, fixture_rows: u64, returned_rows: usize) -> MatrixSample {
        let mut sample = report_matrix_sample(
            key,
            "user",
            total,
            10,
            "SELECT id FROM PerfAuditUser ORDER BY id ASC LIMIT 10",
        );
        sample.fixture_row_count = fixture_rows;
        sample.outcome.row_count = returned_rows;
        sample
    }

    fn observation(
        fixture_rows: u32,
        total_local_instructions: u64,
        route_family: &str,
    ) -> ScaleObservation {
        let scenario_id = format!("user.primary_order.window10.rows{fixture_rows}");
        let mut sample = sample(
            &scenario_id,
            total_local_instructions,
            u64::from(fixture_rows),
            10,
        );
        sample.route_family = route_family.to_string();
        ScaleObservation {
            sentinel_id: "user.primary_order.window10".to_string(),
            scenario_id,
            p1_scenario_id: "user.select.pk.all.pk_asc.limit10".to_string(),
            selectivity: ScaleSelectivity::All,
            predicate_match_rows: fixture_rows,
            result_window: Some(10),
            fixture: ScaleFixtureFacts {
                profile_version: 1,
                surface: "user".to_string(),
                fixture_rows,
                zero_match_rows: 0,
                one_match_rows: 1,
                quarter_match_rows: fixture_rows / 4,
                all_match_rows: fixture_rows,
                payload_profile: ScalePayloadProfile::NotApplicable,
            },
            sample,
        }
    }

    #[test]
    fn normalization_uses_exact_nonzero_typed_denominators() {
        let higher_ratio = sample("higher", 100, 10, 0);
        let lower_ratio = sample("lower", 150, 20, 0);

        assert_eq!(
            normalized_cost(NormalizedDenominator::FixtureRow, &higher_ratio),
            Some(NormalizedCost {
                local_instructions: 100,
                units: NonZeroU64::new(10).expect("ten is nonzero"),
            }),
        );
        assert_eq!(
            normalized_cost(NormalizedDenominator::ReturnedRow, &higher_ratio),
            None,
            "zero denominators must remain ineligible",
        );
        assert_eq!(
            compare_normalized_cost(
                NormalizedDenominator::FixtureRow,
                &higher_ratio,
                &lower_ratio,
            ),
            Ordering::Less,
            "10 instructions per row should rank ahead of 7.5",
        );
    }

    #[test]
    fn scale_profile_has_every_declared_cardinality_route_selectivity_and_window() {
        let declarations = scale_scenario_declarations(
            crate::sql_perf_profile::SQL_PERFORMANCE_PROFILE,
            &deterministic_matrix(),
        )
        .expect("current P1 declarations should materialize the reviewed scale profile");

        assert_eq!(
            declarations.len(),
            scale_sentinel_specs().len()
                * crate::sql_perf_profile::SQL_PERFORMANCE_PROFILE
                    .scale_row_cardinalities()
                    .len(),
        );
        for spec in scale_sentinel_specs() {
            let family = declarations
                .iter()
                .filter(|declaration| declaration.spec.sentinel_id == spec.sentinel_id)
                .collect::<Vec<_>>();
            assert_eq!(family.len(), 3, "{} scale ladder", spec.sentinel_id);
            assert_eq!(
                family
                    .iter()
                    .map(|declaration| declaration.fixture_rows)
                    .collect::<Vec<_>>(),
                vec![16, 256, 2_048],
            );
        }
        let exact_key = declarations
            .iter()
            .find(|declaration| {
                declaration.spec.sentinel_id == "user.primary_order.one.window1"
                    && declaration.fixture_rows == 16
            })
            .expect("exact-key scale sentinel should exist");
        assert!(exact_key.scenario.sql.contains("WHERE id = 1"));
    }

    #[test]
    fn adjacent_slopes_have_golden_signed_arithmetic_and_route_changes() {
        let slopes = adjacent_scale_slopes(
            &[16, 256, 2_048],
            &[
                observation(2_048, 23_000, "secondary_order"),
                observation(16, 1_000, "primary_order"),
                observation(256, 5_000, "primary_order"),
            ],
        )
        .expect("complete scale ladder should derive adjacent slopes");

        assert_eq!(slopes.len(), 2);
        assert_eq!(slopes[0].instruction_delta, 4_000);
        assert_eq!(slopes[0].row_delta, 240);
        assert!(!slopes[0].route_changed);
        assert_eq!(slopes[1].instruction_delta, 18_000);
        assert_eq!(slopes[1].row_delta, 1_792);
        assert!(slopes[1].route_changed);
        assert_eq!(slopes[1].from_route_family, "primary_order");
        assert_eq!(slopes[1].to_route_family, "secondary_order");
    }

    #[test]
    fn scale_slopes_reject_incomplete_duplicate_and_unknown_cardinalities() {
        let incomplete = [observation(16, 1_000, "primary_order")];
        assert!(matches!(
            adjacent_scale_slopes(&[16, 256], &incomplete),
            Err(ScaleEvidenceError::MissingCardinality {
                fixture_rows: 256,
                ..
            })
        ));

        let duplicate = [
            observation(16, 1_000, "primary_order"),
            observation(16, 1_001, "primary_order"),
        ];
        assert!(matches!(
            adjacent_scale_slopes(&[16, 256], &duplicate),
            Err(ScaleEvidenceError::DuplicateCardinality {
                fixture_rows: 16,
                ..
            })
        ));

        assert!(matches!(
            adjacent_scale_slopes(
                &[16, 256],
                &[
                    observation(16, 1_000, "primary_order"),
                    observation(2_048, 2_000, "primary_order"),
                ],
            ),
            Err(ScaleEvidenceError::UnexpectedCardinality {
                fixture_rows: 2_048,
                ..
            })
        ));
    }
}
