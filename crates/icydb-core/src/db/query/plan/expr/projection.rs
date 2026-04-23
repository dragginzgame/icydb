//! Module: db::query::plan::expr::projection
//! Defines the planner-owned projection selection and projection field shapes
//! that flow into structural execution.

use crate::{
    db::query::plan::expr::ast::{
        Alias, BinaryOp, Expr, FieldId, parse_grouped_post_aggregate_order_expr,
        parse_supported_order_expr,
    },
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel},
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

    /// Return whether this projection preserves the model's canonical identity
    /// field order without aliases or computed expressions.
    #[must_use]
    pub(in crate::db) fn is_model_identity_for(&self, model: &EntityModel) -> bool {
        if self.len() != model.fields().len() {
            return false;
        }

        for (field_model, projected_field) in model.fields().iter().zip(self.fields()) {
            if !projected_field.is_identity_field_projection(field_model) {
                return false;
            }
        }

        true
    }

    /// Return the set of model field slots referenced anywhere inside this
    /// projection's canonical expression tree.
    pub(in crate::db) fn referenced_slots_for(
        &self,
        model: &EntityModel,
    ) -> Result<Vec<usize>, InternalError> {
        let mut referenced = vec![false; model.fields().len()];

        for field in self.fields() {
            mark_projection_expr_slots(model, field.expr(), referenced.as_mut_slice())?;
        }

        Ok(referenced
            .into_iter()
            .enumerate()
            .filter_map(|(slot, required)| required.then_some(slot))
            .collect())
    }
}

impl ProjectionField {
    /// Borrow the canonical expression owned by this projection field.
    #[must_use]
    pub(crate) const fn expr(&self) -> &Expr {
        match self {
            Self::Scalar { expr, .. } => expr,
        }
    }

    /// Return one direct projected field name when this output stays on one
    /// field leaf under optional alias wrappers.
    #[must_use]
    pub(in crate::db) fn direct_field_name(&self) -> Option<&str> {
        direct_projection_expr_field_name(self.expr())
    }

    // Identity projection stays on the direct field leaf with no alias or
    // computed wrapper, and must preserve the model-declared field name.
    fn is_identity_field_projection(&self, field_model: &FieldModel) -> bool {
        match self {
            Self::Scalar {
                expr: Expr::Field(field_id),
                alias: None,
            } => field_id.as_str() == field_model.name(),
            Self::Scalar { .. } => false,
        }
    }
}

// Walk one canonical projection expression and mark every referenced field slot
// against the resolved model layout. This stays on the projection boundary so
// static-planning consumers do not open-code expression slot scans locally.
fn mark_projection_expr_slots(
    model: &EntityModel,
    expr: &Expr,
    referenced: &mut [bool],
) -> Result<(), InternalError> {
    expr.try_for_each_tree_expr(&mut |node| match node {
        Expr::Field(field_id) => {
            let field_name = field_id.as_str();
            let slot = model.resolve_field_slot(field_name).ok_or_else(|| {
                InternalError::query_invalid_logical_plan(format!(
                    "projection expression references unknown field '{field_name}'",
                ))
            })?;
            referenced[slot] = true;
            Ok(())
        }
        _ => Ok(()),
    })
}

/// Return one direct field name when the expression is only a field leaf plus
/// optional alias wrappers.
#[must_use]
#[cfg_attr(
    not(test),
    expect(
        clippy::missing_const_for_fn,
        reason = "test-only alias traversal keeps the shared helper non-const across the full target matrix"
    )
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
        let slot = model.resolve_field_slot(field_name)?;
        if field_slots.contains(&slot) {
            return None;
        }

        field_slots.push(slot);
    }

    Some(field_slots)
}

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
/// GroupedCanonicalOrderShape
///
/// One local grouped canonical-order proof shape for one already-parsed
/// expression.
/// This exists so canonical grouped-key prefix proof and broader grouped Top-K
/// admission can read one shared analysis result instead of reclassifying the
/// same expression separately.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GroupedCanonicalOrderShape {
    CanonicalGroupField,
    GroupFieldPlusConstant,
    GroupFieldMinusConstant,
    OtherField,
    OtherFieldOffset,
    Unsupported,
}

///
/// GroupedOrderExprAnalysis
///
/// One shared grouped-order proof summary for one already-parsed planner
/// expression.
/// This exists so canonical grouped-key validation, grouped Top-K admission,
/// and grouped heap selection all read one recursive ownership seam.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct GroupedOrderExprAnalysis {
    canonical_shape: GroupedCanonicalOrderShape,
    references_only_group_fields: bool,
    contains_aggregate: bool,
    contains_non_aggregate_wrapper_fn: bool,
}

impl GroupedOrderExprAnalysis {
    // Build the shared grouped-order proof summary for one expression tree
    // while keeping canonical grouped-key proof and broader Top-K admission on
    // the same recursive owner.
    fn from_expr(expr: &Expr, group_fields: &[&str], expected_group_field: Option<&str>) -> Self {
        match expr {
            Expr::Field(field) => Self {
                canonical_shape: expected_group_field.map_or(
                    GroupedCanonicalOrderShape::Unsupported,
                    |expected_group_field| {
                        if field.as_str() == expected_group_field {
                            GroupedCanonicalOrderShape::CanonicalGroupField
                        } else {
                            GroupedCanonicalOrderShape::OtherField
                        }
                    },
                ),
                references_only_group_fields: group_fields
                    .iter()
                    .any(|allowed| *allowed == field.as_str()),
                contains_aggregate: false,
                contains_non_aggregate_wrapper_fn: false,
            },
            Expr::Aggregate(_) => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                references_only_group_fields: true,
                contains_aggregate: true,
                contains_non_aggregate_wrapper_fn: false,
            },
            Expr::Literal(_) => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                references_only_group_fields: true,
                contains_aggregate: false,
                contains_non_aggregate_wrapper_fn: false,
            },
            Expr::FunctionCall { args, .. } => {
                let child = args.iter().fold(
                    Self {
                        canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                        references_only_group_fields: true,
                        contains_aggregate: false,
                        contains_non_aggregate_wrapper_fn: false,
                    },
                    |current, arg| current.merge_with(Self::from_expr(arg, group_fields, None)),
                );

                Self {
                    canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                    contains_non_aggregate_wrapper_fn: !child.contains_aggregate
                        || child.contains_non_aggregate_wrapper_fn,
                    ..child
                }
            }
            Expr::Case {
                when_then_arms,
                else_expr,
            } => when_then_arms.iter().fold(
                Self::from_expr(else_expr.as_ref(), group_fields, None),
                |current, arm| {
                    current
                        .merge_with(Self::from_expr(arm.condition(), group_fields, None))
                        .merge_with(Self::from_expr(arm.result(), group_fields, None))
                },
            ),
            Expr::Binary { op, left, right } => {
                let left_expr = left.as_ref();
                let right_expr = right.as_ref();
                let left = Self::from_expr(left_expr, group_fields, None);
                let right = Self::from_expr(right_expr, group_fields, None);

                Self {
                    canonical_shape: classify_grouped_canonical_order_shape(
                        *op,
                        left_expr,
                        right_expr,
                        expected_group_field,
                    ),
                    ..left.merge_with(right)
                }
            }
            #[cfg(test)]
            Expr::Alias { expr, .. } => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                ..Self::from_expr(expr.as_ref(), group_fields, None)
            },
            Expr::Unary { expr, .. } => Self {
                canonical_shape: GroupedCanonicalOrderShape::Unsupported,
                ..Self::from_expr(expr.as_ref(), group_fields, None)
            },
        }
    }

    // Merge one child analysis into the current grouped-order proof summary
    // without widening canonical grouped-key proof beyond the parent node.
    const fn merge_with(self, other: Self) -> Self {
        Self {
            canonical_shape: GroupedCanonicalOrderShape::Unsupported,
            references_only_group_fields: self.references_only_group_fields
                && other.references_only_group_fields,
            contains_aggregate: self.contains_aggregate || other.contains_aggregate,
            contains_non_aggregate_wrapper_fn: self.contains_non_aggregate_wrapper_fn
                || other.contains_non_aggregate_wrapper_fn,
        }
    }

    // Convert the shared canonical grouped-key proof shape into the legacy
    // caller-facing admissibility contract used by grouped validation.
    const fn canonical_admissibility(self) -> GroupedOrderTermAdmissibility {
        match self.canonical_shape {
            GroupedCanonicalOrderShape::CanonicalGroupField => {
                GroupedOrderTermAdmissibility::Preserves(GroupedOrderExprClass::CanonicalGroupField)
            }
            GroupedCanonicalOrderShape::GroupFieldPlusConstant => {
                GroupedOrderTermAdmissibility::Preserves(
                    GroupedOrderExprClass::GroupFieldPlusConstant,
                )
            }
            GroupedCanonicalOrderShape::GroupFieldMinusConstant => {
                GroupedOrderTermAdmissibility::Preserves(
                    GroupedOrderExprClass::GroupFieldMinusConstant,
                )
            }
            GroupedCanonicalOrderShape::OtherField
            | GroupedCanonicalOrderShape::OtherFieldOffset => {
                GroupedOrderTermAdmissibility::PrefixMismatch
            }
            GroupedCanonicalOrderShape::Unsupported => {
                GroupedOrderTermAdmissibility::UnsupportedExpression
            }
        }
    }
}

// Keep canonical grouped-key proof intentionally syntactic and fail closed so
// only the admitted field-preserving offset family can reuse the resumable
// grouped-order lane.
fn classify_grouped_canonical_order_shape(
    op: BinaryOp,
    left: &Expr,
    right: &Expr,
    expected_group_field: Option<&str>,
) -> GroupedCanonicalOrderShape {
    let Some(expected_group_field) = expected_group_field else {
        return GroupedCanonicalOrderShape::Unsupported;
    };

    match (op, left, right) {
        (BinaryOp::Add, Expr::Field(field), right)
            if field.as_str() == expected_group_field && is_numeric_order_offset_literal(right) =>
        {
            GroupedCanonicalOrderShape::GroupFieldPlusConstant
        }
        (BinaryOp::Sub, Expr::Field(field), right)
            if field.as_str() == expected_group_field && is_numeric_order_offset_literal(right) =>
        {
            GroupedCanonicalOrderShape::GroupFieldMinusConstant
        }
        (BinaryOp::Add | BinaryOp::Sub, Expr::Field(_), right)
            if is_numeric_order_offset_literal(right) =>
        {
            GroupedCanonicalOrderShape::OtherFieldOffset
        }
        _ => GroupedCanonicalOrderShape::Unsupported,
    }
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
        |expr| {
            GroupedOrderExprAnalysis::from_expr(&expr, &[], Some(expected_group_field))
                .canonical_admissibility()
        },
    )
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
    let analysis = GroupedOrderExprAnalysis::from_expr(&expr, group_fields, None);

    if analysis.references_only_group_fields {
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
    parse_grouped_post_aggregate_order_expr(term).is_some_and(|expr| {
        GroupedOrderExprAnalysis::from_expr(&expr, &[], None).contains_aggregate
    })
}
