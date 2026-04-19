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

impl ProjectionSelection {
    /// Build one planner-owned scalar projection selection from already-lowered fields.
    #[must_use]
    pub(in crate::db) const fn from_scalar_fields(fields: Vec<ProjectionField>) -> Self {
        Self::Exprs(fields)
    }
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
pub(in crate::db) fn projection_field_direct_field_name(field: &ProjectionField) -> Option<&str> {
    direct_projection_expr_field_name(projection_field_expr(field))
}

/// Return one direct field name when the expression is only a field leaf plus
/// optional alias wrappers.
#[must_use]
#[allow(
    clippy::missing_const_for_fn,
    reason = "alias unwrapping touches boxed expression refs that are not const-callable on stable"
)]
pub(in crate::db) fn direct_projection_expr_field_name(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Field(field) => Some(field.as_str()),
        #[cfg(test)]
        Expr::Alias { expr, .. } => direct_projection_expr_field_name(expr.as_ref()),
        Expr::Unary { .. } => None,
        Expr::Literal(_)
        | Expr::FunctionCall { .. }
        | Expr::Aggregate(_)
        | Expr::Case { .. }
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
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            when_then_arms.iter().all(|arm| {
                expr_references_only_fields(arm.condition(), allowed)
                    && expr_references_only_fields(arm.result(), allowed)
            }) && expr_references_only_fields(else_expr.as_ref(), allowed)
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => expr_references_only_fields(expr.as_ref(), allowed),
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

///
/// GroupedTopKOrderAnalysis
///
/// One local grouped Top-K order proof summary for one already-parsed
/// expression.
/// This exists so grouped Top-K admission and heap-selection checks share one
/// traversal without widening the broader planner/shared analysis surfaces.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct GroupedTopKOrderAnalysis {
    references_only_fields: bool,
    contains_aggregate: bool,
    contains_non_aggregate_wrapper_fn: bool,
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
        Expr::Literal(_)
        | Expr::FunctionCall { .. }
        | Expr::Aggregate(_)
        | Expr::Case { .. }
        | Expr::Binary { .. } => GroupedOrderTermAdmissibility::UnsupportedExpression,
        #[cfg(test)]
        Expr::Alias { .. } => GroupedOrderTermAdmissibility::UnsupportedExpression,
        Expr::Unary { .. } => GroupedOrderTermAdmissibility::UnsupportedExpression,
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
    let analysis = analyze_grouped_top_k_order_expr(&expr, group_fields);

    if analysis.references_only_fields {
        if !analysis.contains_aggregate && analysis.contains_non_aggregate_wrapper_fn {
            return GroupedTopKOrderTermAdmissibility::UnsupportedExpression;
        }

        return GroupedTopKOrderTermAdmissibility::Admissible;
    }

    GroupedTopKOrderTermAdmissibility::NonGroupFieldReference
}

/// Return true when one grouped post-aggregate order expression depends on at
/// least one aggregate leaf and therefore cannot stay on the canonical grouped-
/// key ordered lane.
#[must_use]
pub(crate) fn grouped_top_k_order_term_requires_heap(term: &str) -> bool {
    parse_grouped_post_aggregate_order_expr(term)
        .is_some_and(|expr| analyze_grouped_top_k_order_expr(&expr, &[]).contains_aggregate)
}

fn analyze_grouped_top_k_order_expr(
    expr: &Expr,
    group_fields: &[&str],
) -> GroupedTopKOrderAnalysis {
    match expr {
        Expr::Field(field) => GroupedTopKOrderAnalysis {
            references_only_fields: group_fields
                .iter()
                .any(|allowed| *allowed == field.as_str()),
            contains_aggregate: false,
            contains_non_aggregate_wrapper_fn: false,
        },
        Expr::Aggregate(_) => GroupedTopKOrderAnalysis {
            references_only_fields: true,
            contains_aggregate: true,
            contains_non_aggregate_wrapper_fn: false,
        },
        Expr::Literal(_) => GroupedTopKOrderAnalysis {
            references_only_fields: true,
            contains_aggregate: false,
            contains_non_aggregate_wrapper_fn: false,
        },
        Expr::FunctionCall { args, .. } => {
            let child = args.iter().fold(
                GroupedTopKOrderAnalysis {
                    references_only_fields: true,
                    ..GroupedTopKOrderAnalysis::default()
                },
                |mut current, arg| {
                    let child = analyze_grouped_top_k_order_expr(arg, group_fields);
                    current.references_only_fields &= child.references_only_fields;
                    current.contains_aggregate |= child.contains_aggregate;
                    current.contains_non_aggregate_wrapper_fn |=
                        child.contains_non_aggregate_wrapper_fn;

                    current
                },
            );

            GroupedTopKOrderAnalysis {
                contains_non_aggregate_wrapper_fn: !child.contains_aggregate
                    || child.contains_non_aggregate_wrapper_fn,
                ..child
            }
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => when_then_arms.iter().fold(
            analyze_grouped_top_k_order_expr(else_expr.as_ref(), group_fields),
            |mut current, arm| {
                let condition = analyze_grouped_top_k_order_expr(arm.condition(), group_fields);
                let result = analyze_grouped_top_k_order_expr(arm.result(), group_fields);
                current.references_only_fields &=
                    condition.references_only_fields && result.references_only_fields;
                current.contains_aggregate |=
                    condition.contains_aggregate || result.contains_aggregate;
                current.contains_non_aggregate_wrapper_fn |= condition
                    .contains_non_aggregate_wrapper_fn
                    || result.contains_non_aggregate_wrapper_fn;

                current
            },
        ),
        Expr::Binary { left, right, .. } => {
            let left = analyze_grouped_top_k_order_expr(left.as_ref(), group_fields);
            let right = analyze_grouped_top_k_order_expr(right.as_ref(), group_fields);

            GroupedTopKOrderAnalysis {
                references_only_fields: left.references_only_fields && right.references_only_fields,
                contains_aggregate: left.contains_aggregate || right.contains_aggregate,
                contains_non_aggregate_wrapper_fn: left.contains_non_aggregate_wrapper_fn
                    || right.contains_non_aggregate_wrapper_fn,
            }
        }
        #[cfg(test)]
        Expr::Alias { expr, .. } => analyze_grouped_top_k_order_expr(expr.as_ref(), group_fields),
        Expr::Unary { expr, .. } => analyze_grouped_top_k_order_expr(expr.as_ref(), group_fields),
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        GroupedOrderExprClass, GroupedOrderTermAdmissibility, GroupedTopKOrderTermAdmissibility,
        classify_grouped_order_term_for_field, classify_grouped_top_k_order_term,
        grouped_top_k_order_term_requires_heap,
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
        assert!(matches!(
            classify_grouped_order_term_for_field("score", "score"),
            GroupedOrderTermAdmissibility::Preserves(_)
        ));
    }

    #[test]
    fn grouped_order_classifier_accepts_group_field_plus_constant() {
        let _expr = parse("score + 1");

        assert_eq!(
            classify_grouped_order_term_for_field("score + 1", "score"),
            GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::GroupFieldPlusConstant),
        );
        assert!(matches!(
            classify_grouped_order_term_for_field("score + 1", "score"),
            GroupedOrderTermAdmissibility::Preserves(_)
        ));
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
        assert!(matches!(
            classify_grouped_order_term_for_field("score - 2", "score"),
            GroupedOrderTermAdmissibility::Preserves(_)
        ));
    }

    #[test]
    fn grouped_order_classifier_rejects_non_preserving_computed_order() {
        let _expr = parse("score + score");

        assert_eq!(
            classify_grouped_order_term_for_field("score + score", "score"),
            GroupedOrderTermAdmissibility::UnsupportedExpression,
        );
        assert!(!matches!(
            classify_grouped_order_term_for_field("score + score", "score"),
            GroupedOrderTermAdmissibility::Preserves(_)
        ));
    }

    #[test]
    fn grouped_order_classifier_reports_prefix_mismatch_for_other_field() {
        let _expr = parse("other_score + 1");

        assert_eq!(
            classify_grouped_order_term_for_field("other_score + 1", "score"),
            GroupedOrderTermAdmissibility::PrefixMismatch,
        );
        assert!(!matches!(
            classify_grouped_order_term_for_field("other_score + 1", "score"),
            GroupedOrderTermAdmissibility::Preserves(_)
        ));
    }

    #[test]
    fn grouped_order_classifier_rejects_wrapper_function_without_proof() {
        let _expr = parse("ROUND(score, 2)");

        assert_eq!(
            classify_grouped_order_term_for_field("ROUND(score, 2)", "score"),
            GroupedOrderTermAdmissibility::UnsupportedExpression,
        );
        assert!(!matches!(
            classify_grouped_order_term_for_field("ROUND(score, 2)", "score"),
            GroupedOrderTermAdmissibility::Preserves(_)
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

    #[test]
    fn grouped_top_k_classifier_accepts_filtered_aggregate_null_test_terms() {
        let _expr = parse_top_k("COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank))");

        assert_eq!(
            classify_grouped_top_k_order_term(
                "COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank))",
                &["class_name", "guild_rank"],
            ),
            GroupedTopKOrderTermAdmissibility::Admissible,
        );
    }

    #[test]
    fn grouped_top_k_classifier_accepts_filtered_aggregate_null_test_boolean_compositions() {
        let _expr = parse_top_k("COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank) AND level >= 10)");

        assert_eq!(
            classify_grouped_top_k_order_term(
                "COUNT(*) FILTER (WHERE IS_NOT_NULL(guild_rank) AND level >= 10)",
                &["class_name", "guild_rank", "level"],
            ),
            GroupedTopKOrderTermAdmissibility::Admissible,
        );
    }

    #[test]
    fn grouped_top_k_heap_gate_requires_aggregate_leaf() {
        assert!(grouped_top_k_order_term_requires_heap("AVG(score)"));
        assert!(grouped_top_k_order_term_requires_heap(
            "ROUND(AVG(score), 2)"
        ));
        assert!(!grouped_top_k_order_term_requires_heap("score + score"));
        assert!(!grouped_top_k_order_term_requires_heap("score"));
    }
}
