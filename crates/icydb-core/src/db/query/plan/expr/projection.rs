//! Module: db::query::plan::expr::projection
//! Defines the planner-owned projection selection and projection field shapes
//! that flow into structural execution.

use crate::{
    db::query::plan::expr::ast::{Alias, BinaryOp, Expr, FieldId, parse_supported_order_expr},
    model::entity::{EntityModel, resolve_field_slot},
    value::Value,
};

///
/// ProjectionSelection
///
/// Planner-owned projection selection contract for scalar query shapes.
/// `All` projects the full entity model field list.
/// `Fields` projects one explicit field subset in declaration order.
/// Invariant: projection order is planner-authoritative and must remain stable
/// through executor/materialization boundaries.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionSelection {
    All,
    Fields(Vec<FieldId>),
    Exprs(Vec<ProjectionField>),
}

///
/// ProjectionField
///
/// One canonical projection output field in declaration order.
/// This remains planner-owned semantic shape and is executor-agnostic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ProjectionField {
    Scalar { expr: Expr, alias: Option<Alias> },
}

///
/// ProjectionSpec
///
/// Canonical projection semantic contract emitted by planner.
/// Construction remains planner-only; consumers borrow read-only views.
/// Invariant: `fields` order is canonical output order and must not be
/// reordered by executor/output layers.
///

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct ProjectionSpec {
    fields: Vec<ProjectionField>,
}

impl ProjectionSpec {
    /// Build one projection semantic contract from planner-lowered fields.
    #[must_use]
    pub(in crate::db::query::plan) const fn new(fields: Vec<ProjectionField>) -> Self {
        Self { fields }
    }

    /// Build one projection semantic contract for tests outside planner modules.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn from_fields_for_test(fields: Vec<ProjectionField>) -> Self {
        Self::new(fields)
    }

    /// Return the declared output field count.
    #[must_use]
    pub(crate) const fn len(&self) -> usize {
        self.fields.len()
    }

    /// Borrow declared projection fields in canonical order.
    pub(crate) fn fields(&self) -> std::slice::Iter<'_, ProjectionField> {
        self.fields.iter()
    }
}

/// Borrow the canonical expression owned by one projection field.
#[must_use]
pub(crate) const fn projection_field_expr(field: &ProjectionField) -> &Expr {
    match field {
        ProjectionField::Scalar { expr, .. } => expr,
    }
}

/// Return one direct projected field name when the output stays on one field
/// leaf under optional alias wrappers.
#[must_use]
#[cfg(not(test))]
pub(in crate::db) const fn projection_field_direct_field_name(
    field: &ProjectionField,
) -> Option<&str> {
    direct_projection_expr_field_name(projection_field_expr(field))
}

/// Return one direct projected field name when the output stays on one field
/// leaf under optional alias wrappers.
#[must_use]
#[cfg(test)]
pub(in crate::db) fn projection_field_direct_field_name(field: &ProjectionField) -> Option<&str> {
    direct_projection_expr_field_name(projection_field_expr(field))
}

/// Return one direct field name when the expression is only a field leaf plus
/// optional alias wrappers.
#[must_use]
#[cfg(not(test))]
pub(in crate::db) const fn direct_projection_expr_field_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Field(field) => Some(field.as_str()),
        Expr::Literal(_) | Expr::FunctionCall { .. } | Expr::Aggregate(_) | Expr::Binary { .. } => {
            None
        }
    }
}

#[must_use]
#[cfg(test)]
pub(in crate::db) fn direct_projection_expr_field_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Field(field) => Some(field.as_str()),
        Expr::Alias { expr, .. } => direct_projection_expr_field_name(expr.as_ref()),
        Expr::Literal(_)
        | Expr::FunctionCall { .. }
        | Expr::Aggregate(_)
        | Expr::Unary { .. }
        | Expr::Binary { .. } => None,
    }
}

/// Resolve one unique direct field-slot layout from canonical field names.
///
/// This helper centralizes the executor/planner rule for direct slot-copy
/// projections: every projected output must map to one canonical field slot,
/// and no source slot may be repeated because retained-slot readers consume
/// values with `Option::take()`.
#[must_use]
pub(crate) fn collect_unique_direct_projection_slots<'a>(
    model: &EntityModel,
    field_names: impl IntoIterator<Item = &'a str>,
) -> Option<Vec<usize>> {
    let mut field_slots = Vec::new();

    for field_name in field_names {
        let slot = resolve_field_slot(model, field_name)?;
        if field_slots.contains(&slot) {
            return None;
        }

        field_slots.push(slot);
    }

    Some(field_slots)
}

/// Return true when one expression references only fields in one allowed set.
///
/// Semantic contract:
/// - field leaves must be present in `allowed`
/// - aggregate/literal leaves are always admissible
/// - alias and unary wrappers recurse into inner expression
/// - binary expressions require both sides to be admissible
#[must_use]
pub(crate) fn expr_references_only_fields(expr: &Expr, allowed: &[&str]) -> bool {
    match expr {
        Expr::Field(field) => allowed.iter().any(|allowed| *allowed == field.as_str()),
        Expr::Aggregate(_) => true,
        Expr::Literal(_) => true,
        Expr::FunctionCall { args, .. } => args
            .iter()
            .all(|arg| expr_references_only_fields(arg, allowed)),
        #[cfg(test)]
        Expr::Alias { expr, .. } => expr_references_only_fields(expr.as_ref(), allowed),
        #[cfg(test)]
        Expr::Unary { expr, .. } => expr_references_only_fields(expr.as_ref(), allowed),
        Expr::Binary { left, right, .. } => {
            expr_references_only_fields(left.as_ref(), allowed)
                && expr_references_only_fields(right.as_ref(), allowed)
        }
    }
}

/// Return true when one canonical `ORDER BY` term preserves the same
/// lexicographic order as one grouped key field.
///
/// This intentionally stays narrower than the full supported computed-order
/// family. Grouped pagination and continuation still resume on canonical group
/// keys, so grouped `ORDER BY` can only admit expressions that are proven to
/// preserve the underlying grouped-key order contract rather than merely
/// reference grouped fields.
#[must_use]
pub(crate) fn order_term_preserves_group_field_order(
    term: &str,
    expected_group_field: &str,
) -> bool {
    parse_supported_order_expr(term)
        .is_some_and(|expr| order_expr_preserves_group_field_order(&expr, expected_group_field))
}

// Keep grouped-order proof intentionally syntactic and fail closed. The
// current grouped runtime orders and resumes on canonical group keys, so only
// one exact grouped field or one additive constant offset over that field may
// reuse the same ordered-group contract.
fn order_expr_preserves_group_field_order(expr: &Expr, expected_group_field: &str) -> bool {
    match expr {
        Expr::Field(field) => field.as_str() == expected_group_field,
        Expr::Binary { op, left, right }
            if matches!(op, BinaryOp::Add | BinaryOp::Sub)
                && matches!(
                    left.as_ref(),
                    Expr::Field(field) if field.as_str() == expected_group_field
                )
                && is_numeric_order_offset_literal(right.as_ref()) =>
        {
            true
        }
        Expr::Literal(_) | Expr::FunctionCall { .. } | Expr::Aggregate(_) | Expr::Binary { .. } => {
            false
        }
        #[cfg(test)]
        Expr::Alias { .. } | Expr::Unary { .. } => false,
    }
}

// Additive constant offsets preserve both ascending and descending order for
// the underlying grouped key while avoiding the tie/collapse behavior of the
// broader computed-order family.
const fn is_numeric_order_offset_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            Value::Int(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Uint(_)
                | Value::Uint128(_)
                | Value::UintBig(_)
                | Value::Decimal(_)
                | Value::Float32(_)
                | Value::Float64(_)
        )
    )
}
