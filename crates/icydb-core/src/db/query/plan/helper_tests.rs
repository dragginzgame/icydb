#![cfg(feature = "sql")]
//! Direct planner-contract fixtures keep strict execution qualification separate
//! from frontend literal-coercion policy. No authored query bypass is exposed.

use crate::db::{
    QueryError,
    predicate::{MissingRowPolicy, Predicate},
    query::{
        intent::QueryModel,
        plan::{
            AccessPlannedQuery, OrderSpec, VisibleIndexes, expr::Expr,
            pipeline::assemble_query_model_plan,
            plan_access_selection_with_order_and_semantic_indexes,
        },
        preparation::PreparationWork,
    },
    schema::SchemaInfo,
};
use std::rc::Rc;

pub(in crate::db) fn strict_covering_plan(
    schema: Rc<SchemaInfo>,
    expression: Expr,
    predicate: Predicate,
    fields: Vec<String>,
    work: &PreparationWork<'_>,
) -> Result<AccessPlannedQuery, QueryError> {
    let order = OrderSpec {
        fields: vec![crate::db::asc("id").lower()],
    };
    let query = QueryModel::new(MissingRowPolicy::Ignore)
        .filter_expr(&schema, expression, work)?
        .select_fields(fields)
        .order_spec(order.clone());
    let visible =
        VisibleIndexes::accepted_schema_visible(&schema, work).map_err(QueryError::execute)?;
    let indexes = visible.accepted_semantic_index_contracts();
    let (access, reason) = plan_access_selection_with_order_and_semantic_indexes(
        indexes,
        &schema,
        Some(&predicate),
        Some(&order),
        false,
        work,
    )?
    .into_access_and_non_index_reason();
    assemble_query_model_plan(
        &query,
        indexes,
        schema,
        Some(predicate),
        Some(order),
        None,
        access,
        reason,
        work,
    )
}
