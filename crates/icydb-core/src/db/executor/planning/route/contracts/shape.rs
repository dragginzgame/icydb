//! Module: db::executor::planning::route::contracts::shape
//! Defines the structural route-shape contracts produced by executor planning.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        executor::aggregate::capability::field_kind_supports_aggregate_ordering,
        query::plan::AggregateKind,
    },
    model::field::FieldModel,
};

///
/// FastPathOrder
///
/// Shared fast-path precedence model used by load and aggregate routing.
/// Routing implementations remain separate, but they iterate one canonical order.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum FastPathOrder {
    PrimaryKey,
    SecondaryPrefix,
    PrimaryScan,
    IndexRange,
    Composite,
}

// Contract: fast-path precedence is a stability boundary. Any change here must
// be intentional, accompanied by route-order tests, and called out in changelog.
pub(in crate::db::executor) const LOAD_FAST_PATH_ORDER: [FastPathOrder; 3] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::IndexRange,
];

// Contract: aggregate dispatch precedence is ordered for semantic and
// performance stability. Do not reorder casually.
pub(in crate::db::executor) const AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 5] = [
    FastPathOrder::PrimaryKey,
    FastPathOrder::SecondaryPrefix,
    FastPathOrder::PrimaryScan,
    FastPathOrder::IndexRange,
    FastPathOrder::Composite,
];

// Contract: grouped aggregate routes are materialized-only in this audit pass
// and must not participate in scalar aggregate fast-path dispatch.
pub(in crate::db::executor) const GROUPED_AGGREGATE_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

// Contract: mutation routes are materialized-only and do not participate in
// load/aggregate fast-path precedence.
pub(in crate::db::executor) const MUTATION_FAST_PATH_ORDER: [FastPathOrder; 0] = [];

///
/// AggregateRouteShape
///
/// Borrowed aggregate semantic shape consumed by route planning for scalar
/// aggregate routing.
/// This route-owned contract keeps kind plus planner-resolved target-field
/// metadata available without requiring route policy to rediscover field-table
/// semantics for field existence or orderability checks.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AggregateRouteShape<'a> {
    kind: AggregateKind,
    target_field: Option<&'a str>,
    target_field_known: bool,
    target_field_orderable: bool,
    target_field_is_primary_key: bool,
}

impl<'a> AggregateRouteShape<'a> {
    /// Construct one route-owned aggregate shape from already-resolved
    /// planner/prepared metadata.
    #[must_use]
    pub(in crate::db) const fn new_resolved(
        kind: AggregateKind,
        target_field: Option<&'a str>,
        target_field_known: bool,
        target_field_orderable: bool,
        target_field_is_primary_key: bool,
    ) -> Self {
        Self {
            kind,
            target_field,
            target_field_known,
            target_field_orderable,
            target_field_is_primary_key,
        }
    }

    /// Construct one route-owned aggregate shape from field-table semantics.
    #[must_use]
    pub(in crate::db) fn new_from_fields(
        kind: AggregateKind,
        target_field: Option<&'a str>,
        fields: &[FieldModel],
        primary_key_name: &str,
    ) -> Self {
        let target_field_known = target_field.is_none_or(|target_field| {
            fields
                .iter()
                .any(|field_model| field_model.name() == target_field)
        });
        let target_field_orderable = target_field.is_some_and(|target_field| {
            fields
                .iter()
                .find(|field_model| field_model.name() == target_field)
                .is_some_and(|field_model| {
                    field_kind_supports_aggregate_ordering(&field_model.kind())
                })
        });
        let target_field_is_primary_key =
            target_field.is_some_and(|target_field| target_field == primary_key_name);

        Self::new_resolved(
            kind,
            target_field,
            target_field_known,
            target_field_orderable,
            target_field_is_primary_key,
        )
    }

    /// Return aggregate kind.
    #[must_use]
    pub(in crate::db) const fn kind(self) -> AggregateKind {
        self.kind
    }

    /// Return optional target field name.
    #[must_use]
    pub(in crate::db) const fn target_field(self) -> Option<&'a str> {
        self.target_field
    }

    /// Return whether the optional target field resolved against schema authority.
    #[must_use]
    pub(in crate::db) const fn target_field_known(self) -> bool {
        self.target_field_known
    }

    /// Return whether the optional target field supports aggregate ordering.
    #[must_use]
    pub(in crate::db) const fn target_field_orderable(self) -> bool {
        self.target_field_orderable
    }

    /// Return whether the optional target field is the entity primary key.
    #[must_use]
    pub(in crate::db) const fn target_field_is_primary_key(self) -> bool {
        self.target_field_is_primary_key
    }
}

///
/// RouteShapeKind
///
/// Planner-to-router semantic execution shape contract.
/// This shape is independent from streaming/materialized execution policy and
/// allows route dispatch migration away from feature-combination branching.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::executor) enum RouteShapeKind {
    LoadScalar,
    AggregateCount,
    AggregateNonCount,
    AggregateGrouped,
    MutationDelete,
}
