//! Module: db::query::plan::access_planner
//! Responsibility: prepare canonical predicate and order inputs for access planning.
//! Does not own: access selection, executor routing, or final access-choice scoring.
//! Boundary: projects borrowed intent under accepted schema authority.

use crate::db::{
    QueryError,
    predicate::{Predicate, normalize, normalize_enum_literals},
    query::{
        plan::{OrderSpec, canonicalize_order_spec_for_grouping},
        preparation::PreparationWork,
    },
    schema::SchemaInfo,
};

///
/// AccessPlanningInputs
///
/// Access-planning input contract projected from query intent.
/// Carries the optional predicate and raw order shape.
/// Raw order remains available for allocation-free shape checks; the pipeline
/// supplies one canonical order to both access selection and logical assembly.
///

#[derive(Debug)]
pub(in crate::db::query) struct AccessPlanningInputs<'a> {
    predicate: Option<&'a Predicate>,
    order: Option<&'a OrderSpec>,
}

impl<'a> AccessPlanningInputs<'a> {
    /// Build access-planning inputs from intent-projected values.
    #[must_use]
    pub(in crate::db::query) const fn new(
        predicate: Option<&'a Predicate>,
        order: Option<&'a OrderSpec>,
    ) -> Self {
        Self { predicate, order }
    }

    /// Borrow predicate input for normalization and planner analysis.
    #[must_use]
    pub(in crate::db::query) const fn predicate(&self) -> Option<&'a Predicate> {
        self.predicate
    }

    /// Borrow raw ORDER BY input for planner-side canonicalization.
    #[must_use]
    pub(in crate::db::query) const fn order(&self) -> Option<&'a OrderSpec> {
        self.order
    }

    /// Materialize order only when constructing a plan. Access selection borrows
    /// this result before logical assembly takes ownership of it.
    pub(in crate::db::query) fn canonical_order(
        &self,
        schema: &SchemaInfo,
        grouped: bool,
        work: &PreparationWork<'_>,
    ) -> Result<Option<OrderSpec>, QueryError> {
        canonicalize_order_spec_for_grouping(
            schema.primary_key_names(),
            self.order
                .map(|order| work.copy_order_spec(order))
                .transpose()?,
            grouped,
            work,
        )
    }
}

// Normalize one optional predicate into canonical planner form.
pub(in crate::db::query) fn normalize_query_predicate(
    schema_info: &SchemaInfo,
    predicate: Option<&Predicate>,
    work: &PreparationWork<'_>,
) -> Result<Option<Predicate>, QueryError> {
    predicate
        .map(|predicate| {
            let predicate = normalize_enum_literals(schema_info, predicate, work)?;

            Ok::<Predicate, QueryError>(normalize(predicate))
        })
        .transpose()
}
