//! Module: db::query::plan::expr::projection
//! Defines the planner-owned projection selection and projection field shapes
//! that flow into structural execution.

use crate::{
    db::query::plan::expr::ast::{
        Alias, BinaryOp, Expr, FieldId, parse_grouped_post_aggregate_order_expr,
        parse_supported_order_expr,
    },
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

///
/// GroupedOrderExprClass
///
/// Planner-local grouped `ORDER BY` proof result for one expression against
/// one expected grouped key field. This keeps grouped order admission explicit:
/// the grouped validator and grouped strategy logic consume one shared proof
/// contract instead of open-coding additive-order special cases separately.
///
///
/// GroupedOrderExprClass
///
/// Classifies the small grouped `ORDER BY` expression family that the planner
/// can prove preserves canonical grouped-key order in the current grouped
/// execution model. This stays intentionally narrower than the broader scalar
/// computed-order surface because grouped pagination still resumes on grouped
/// keys rather than on arbitrary computed order values.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedOrderExprClass {
    CanonicalGroupField,
    GroupFieldPlusConstant,
    GroupFieldMinusConstant,
}

///
/// GroupedOrderTermAdmissibility
///
/// One planner-local admission result for a grouped `ORDER BY` term against
/// one expected grouped key. The grouped cursor validator uses this to keep
/// plain prefix mismatch separate from expressions that parse and evaluate but
/// still are not order-admissible under the grouped boundedness contract.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedOrderTermAdmissibility {
    Preserves(GroupedOrderExprClass),
    PrefixMismatch,
    UnsupportedExpression,
}

///
/// GroupedTopKOrderTermAdmissibility
///
/// Planner-local grouped Top-K admission result for one `ORDER BY` term.
/// This keeps the `0.88` aggregate-order lane explicit without widening the
/// older canonical grouped-key proof helper into a catch-all classifier.
///
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum GroupedTopKOrderTermAdmissibility {
    Admissible,
    NonGroupFieldReference,
    UnsupportedExpression,
}

/// Return true when one canonical `ORDER BY` term preserves the same
/// lexicographic order as one grouped key field.
///
/// This intentionally stays narrower than the full supported computed-order
/// family. Grouped pagination and continuation still resume on canonical group
/// keys, so grouped `ORDER BY` can only admit expressions that are proven to
/// preserve the underlying grouped-key order contract rather than merely
/// reference grouped fields.
#[cfg(test)]
#[must_use]
pub(crate) fn order_term_preserves_group_field_order(
    term: &str,
    expected_group_field: &str,
) -> bool {
    matches!(
        classify_grouped_order_term_for_field(term, expected_group_field),
        GroupedOrderTermAdmissibility::Preserves(_)
    )
}

// Classify one grouped ORDER BY term against one expected grouped key field
// so grouped validation can distinguish prefix mismatch from unsupported-but-
// evaluable grouped order expressions.
#[must_use]
pub(crate) fn classify_grouped_order_term_for_field(
    term: &str,
    expected_group_field: &str,
) -> GroupedOrderTermAdmissibility {
    parse_supported_order_expr(term).map_or(
        GroupedOrderTermAdmissibility::UnsupportedExpression,
        |expr| classify_grouped_order_expr_for_field(&expr, expected_group_field),
    )
}

// Keep grouped-order proof intentionally syntactic and fail closed. The
// current grouped runtime orders and resumes on canonical group keys, so only
// one exact grouped field or one additive/subtractive constant offset over
// that field may reuse the same ordered-group contract.
fn classify_grouped_order_expr_for_field(
    expr: &Expr,
    expected_group_field: &str,
) -> GroupedOrderTermAdmissibility {
    match expr {
        Expr::Field(field) if field.as_str() == expected_group_field => {
            GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::CanonicalGroupField)
        }
        Expr::Field(_) => GroupedOrderTermAdmissibility::PrefixMismatch,
        Expr::Binary {
            op: BinaryOp::Add,
            left,
            right,
        } if matches!(
            left.as_ref(),
            Expr::Field(field) if field.as_str() == expected_group_field
        ) && is_numeric_order_offset_literal(right.as_ref()) =>
        {
            GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldPlusConstant)
        }
        Expr::Binary {
            op: BinaryOp::Sub,
            left,
            right,
        } if matches!(
            left.as_ref(),
            Expr::Field(field) if field.as_str() == expected_group_field
        ) && is_numeric_order_offset_literal(right.as_ref()) =>
        {
            GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldMinusConstant)
        }
        Expr::Binary {
            op: BinaryOp::Add | BinaryOp::Sub,
            left,
            right,
        } if matches!(left.as_ref(), Expr::Field(_))
            && is_numeric_order_offset_literal(right.as_ref()) =>
        {
            GroupedOrderTermAdmissibility::PrefixMismatch
        }
        Expr::Literal(_) | Expr::FunctionCall { .. } | Expr::Aggregate(_) | Expr::Binary { .. } => {
            GroupedOrderTermAdmissibility::UnsupportedExpression
        }
        #[cfg(test)]
        Expr::Alias { .. } | Expr::Unary { .. } => {
            GroupedOrderTermAdmissibility::UnsupportedExpression
        }
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

/// Return true when one grouped `ORDER BY` term is admissible for the
/// aggregate/post-aggregate Top-K lane over the declared grouped key set.
#[must_use]
pub(crate) fn classify_grouped_top_k_order_term(
    term: &str,
    group_fields: &[&str],
) -> GroupedTopKOrderTermAdmissibility {
    let Some(expr) = parse_grouped_post_aggregate_order_expr(term) else {
        return GroupedTopKOrderTermAdmissibility::UnsupportedExpression;
    };

    if expr_references_only_fields(&expr, group_fields) {
        return GroupedTopKOrderTermAdmissibility::Admissible;
    }

    GroupedTopKOrderTermAdmissibility::NonGroupFieldReference
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        GroupedOrderExprClass, GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility,
        classify_grouped_order_term_for_field, classify_grouped_top_k_order_term,
        order_term_preserves_group_field_order,
    };
    use crate::db::query::plan::expr::ast::{
        Expr, parse_grouped_post_aggregate_order_expr, parse_supported_order_expr,
    };

    fn parse(expr: &str) -> Expr {
        parse_supported_order_expr(expr)
            .expect("supported grouped ORDER BY test expression should parse")
    }

    fn parse_top_k(expr: &str) -> Expr {
        parse_grouped_post_aggregate_order_expr(expr)
            .expect("supported grouped Top-K ORDER BY test expression should parse")
    }

    #[test]
    fn grouped_order_classifier_accepts_canonical_group_field() {
        let _expr = parse("score");

        assert_eq!(
            classify_grouped_order_term_for_field("score", "score"),
            GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::CanonicalGroupField),
        );
        assert!(order_term_preserves_group_field_order("score", "score"));
    }

    #[test]
    fn grouped_order_classifier_accepts_group_field_plus_constant() {
        let _expr = parse("score + 1");

        assert_eq!(
            classify_grouped_order_term_for_field("score + 1", "score"),
            GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldPlusConstant),
        );
        assert!(order_term_preserves_group_field_order("score + 1", "score"));
    }

    #[test]
    fn grouped_order_classifier_accepts_group_field_minus_constant() {
        let _expr = parse("score - 2");

        assert_eq!(
            classify_grouped_order_term_for_field("score - 2", "score"),
            GroupedOrderTermAdmissibility::Preserves(
                GroupedOrderExprClass::GroupFieldMinusConstant
            ),
        );
        assert!(order_term_preserves_group_field_order("score - 2", "score"));
    }

    #[test]
    fn grouped_order_classifier_rejects_non_preserving_computed_order() {
        let _expr = parse("score + score");

        assert_eq!(
            classify_grouped_order_term_for_field("score + score", "score"),
            GroupedOrderTermAdmissibility::UnsupportedExpression,
        );
        assert!(!order_term_preserves_group_field_order(
            "score + score",
            "score"
        ));
    }

    #[test]
    fn grouped_order_classifier_reports_prefix_mismatch_for_other_field() {
        let _expr = parse("other_score + 1");

        assert_eq!(
            classify_grouped_order_term_for_field("other_score + 1", "score"),
            GroupedOrderTermAdmissibility::PrefixMismatch,
        );
        assert!(!order_term_preserves_group_field_order(
            "other_score + 1",
            "score"
        ));
    }

    #[test]
    fn grouped_order_classifier_rejects_wrapper_function_without_proof() {
        let _expr = parse("ROUND(score, 2)");

        assert_eq!(
            classify_grouped_order_term_for_field("ROUND(score, 2)", "score"),
            GroupedOrderTermAdmissibility::UnsupportedExpression,
        );
        assert!(!order_term_preserves_group_field_order(
            "ROUND(score, 2)",
            "score"
        ));
    }

    #[test]
    fn grouped_top_k_classifier_accepts_aggregate_leaf_terms() {
        let _expr = parse_top_k("AVG(score)");

        assert_eq!(
            classify_grouped_top_k_order_term("AVG(score)", &["score"]),
            GroupedTopKOrderTermAdmissibility::Admissible,
        );
    }

    #[test]
    fn grouped_top_k_classifier_accepts_post_aggregate_round_terms() {
        let _expr = parse_top_k("ROUND(AVG(score), 2)");

        assert_eq!(
            classify_grouped_top_k_order_term("ROUND(AVG(score), 2)", &["score"]),
            GroupedTopKOrderTermAdmissibility::Admissible,
        );
    }

    #[test]
    fn grouped_top_k_classifier_accepts_group_field_scalar_composition() {
        let _expr = parse_top_k("score + score");

        assert_eq!(
            classify_grouped_top_k_order_term("score + score", &["score"]),
            GroupedTopKOrderTermAdmissibility::Admissible,
        );
    }

    #[test]
    fn grouped_top_k_classifier_rejects_non_group_field_leaves() {
        let _expr = parse_top_k("AVG(score) + other_score");

        assert_eq!(
            classify_grouped_top_k_order_term("AVG(score) + other_score", &["score"]),
            GroupedTopKOrderTermAdmissibility::NonGroupFieldReference,
        );
    }

    #[test]
    fn grouped_top_k_classifier_rejects_unsupported_wrapper_functions() {
        assert_eq!(
            classify_grouped_top_k_order_term("LOWER(score)", &["score"]),
            GroupedTopKOrderTermAdmissibility::UnsupportedExpression,
        );
    }
}
