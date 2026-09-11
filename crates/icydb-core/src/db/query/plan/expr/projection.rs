//! Module: db::query::plan::expr::projection
//! Defines the planner-owned projection selection and projection field shapes
//! that flow into structural execution.

#[cfg(test)]
mod grouped_order_tests;
#[cfg(test)]
mod slot_tests;

use crate::{
    db::{
        QueryError,
        query::plan::{
            GroupFieldRef, GroupFieldSet,
            expr::ast::{Alias, BinaryOp, Expr, FieldId},
        },
        query::preparation::PreparationWork,
        schema::SchemaInfo,
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::cmp::Ordering;

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
pub(in crate::db) enum ProjectionSelection {
    All,
    Fields(Vec<FieldId>),
    Exprs(Vec<ProjectionField>),
}

impl ProjectionSelection {
    /// Retain authored projection operands under the caller's request budget.
    /// Declaration order, duplicates and aliases are preserved exactly.
    pub(in crate::db) fn copy_for_preparation(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        Ok(match self {
            Self::All => Self::All,
            Self::Fields(fields) => Self::Fields(work.copy_slice(fields, |field| {
                Ok(FieldId::new(work.copy_text(field.as_str())?))
            })?),
            Self::Exprs(fields) => {
                Self::Exprs(work.copy_slice(fields, |field| field.copy_for_preparation(work))?)
            }
        })
    }
}

///
/// ProjectionField
///
/// One canonical projection output field in declaration order.
/// This remains planner-owned semantic shape and is executor-agnostic.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ProjectionField {
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
pub(in crate::db) struct ProjectionSpec {
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
    pub(in crate::db) const fn len(&self) -> usize {
        self.fields.len()
    }

    /// Borrow declared projection fields in canonical order.
    pub(in crate::db) fn fields(&self) -> std::slice::Iter<'_, ProjectionField> {
        self.fields.iter()
    }

    /// Return referenced slots using the caller-selected schema authority.
    pub(in crate::db) fn referenced_slots_for_schema(
        &self,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<Vec<usize>, QueryError> {
        let mut referenced = Vec::new();

        for field in self.fields() {
            mark_projection_expr_slots(schema, field.expr(), &mut referenced, work)?;
        }

        Ok(referenced)
    }

    /// Return whether this projection preserves accepted physical field order.
    pub(in crate::db) fn is_schema_identity_for(
        &self,
        schema: &SchemaInfo,
        work: &PreparationWork<'_>,
    ) -> Result<bool, QueryError> {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        if self.len() != schema.field_count() {
            return Ok(false);
        }
        // Accepted live fields have distinct physical slots, which need not be
        // contiguous. Full cardinality plus strict slot order proves identity
        // without collecting or sorting another schema-name vector.
        let mut previous = None;
        for field in self.fields() {
            work.charge(Resource::PredicateExpressionSteps, 1)?;
            let ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } = field
            else {
                return Ok(false);
            };
            work.charge(
                Resource::PredicateExpressionSteps,
                field.as_str().len() as u64,
            )?;
            let Some(slot) = schema.field_slot_index(field.as_str()) else {
                return Ok(false);
            };
            if let Some(previous) = previous {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                if slot <= previous {
                    return Ok(false);
                }
            }
            previous = Some(slot);
        }
        Ok(true)
    }
}

impl ProjectionField {
    /// Retain one complete output expression and alias under the current budget.
    pub(in crate::db) fn copy_for_preparation(
        &self,
        work: &PreparationWork<'_>,
    ) -> Result<Self, QueryError> {
        let Self::Scalar { expr, alias } = self;
        Ok(Self::Scalar {
            expr: work.copy_expr(expr)?,
            alias: alias
                .as_ref()
                .map(|alias| work.copy_text(alias.as_str()).map(Alias::new))
                .transpose()?,
        })
    }

    /// Borrow the canonical expression owned by this projection field.
    #[must_use]
    pub(in crate::db) const fn expr(&self) -> &Expr {
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
}

// Walk one canonical projection expression and mark every referenced field slot
// against the resolved model layout. This stays on the projection boundary so
// static-planning consumers do not open-code expression slot scans locally.
fn mark_projection_expr_slots(
    schema: &SchemaInfo,
    expr: &Expr,
    referenced: &mut Vec<usize>,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    // Projection inputs are depth-admitted before this recursive visitor.
    // Aggregate leaves retain their existing reachability semantics.
    expr.try_for_each_tree_expr(&mut |node| {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let field_name = match node {
            Expr::Field(field) => field.as_str(),
            Expr::FieldPath(path) => path.root().as_str(),
            _ => return Ok(()),
        };
        work.charge(Resource::PredicateExpressionSteps, field_name.len() as u64)?;
        let slot = schema
            .field_slot_index(field_name)
            .ok_or_else(|| QueryError::execute(InternalError::query_invalid_logical_plan()))?;
        insert_projection_slot(referenced, slot, work)
    })
}

// Keep slots sorted and unique during construction, eliminating a separate
// unmetered final sort. Charge every comparison, growth and shifted element
// before mutation; a failed insertion leaves the existing list unchanged.
fn insert_projection_slot(
    referenced: &mut Vec<usize>,
    slot: usize,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    let (mut low, mut high) = (0, referenced.len());
    while low < high {
        work.charge(Resource::PredicateExpressionSteps, 1)?;
        let mid = low + (high - low) / 2;
        match referenced[mid].cmp(&slot) {
            Ordering::Less => low = mid + 1,
            Ordering::Greater => high = mid,
            Ordering::Equal => return Ok(()),
        }
    }
    work.charge(
        Resource::PredicateExpressionSteps,
        (referenced.len() - low) as u64,
    )?;
    work.reserve_vec(referenced, 1)?;
    referenced.insert(low, slot);
    Ok(())
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
        Expr::FieldPath(_)
        | Expr::Literal(_)
        | Expr::FunctionCall { .. }
        | Expr::Aggregate(_)
        | Expr::Case { .. }
        | Expr::Binary { .. } => None,
    }
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
pub(in crate::db) enum GroupedOrderExprClass {
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
pub(in crate::db) enum GroupedOrderTermAdmissibility {
    Preserves(GroupedOrderExprClass),
    PrefixMismatch,
    UnsupportedExpression,
}

///
/// GroupedTopKOrderTermAdmissibility
///
/// Planner-local grouped Top-K admission result for one `ORDER BY` term.
/// This keeps the aggregate-order lane explicit without widening the narrow
/// canonical grouped-key proof helper into a catch-all classifier.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum GroupedTopKOrderTermAdmissibility {
    Admissible,
    NonGroupFieldReference,
    UnsupportedExpression,
}

// Classify one grouped ORDER BY term against one expected grouped key field
// so grouped validation can distinguish prefix mismatch from unsupported-but-
// evaluable grouped order expressions.
#[must_use]
pub(in crate::db) fn classify_grouped_order_term_for_field(
    expr: &Expr,
    expected_group_field: GroupFieldRef<'_>,
) -> GroupedOrderTermAdmissibility {
    match try_classify_grouped_order_term_for_field(expr, expected_group_field, &mut |_| {
        Ok::<_, std::convert::Infallible>(())
    }) {
        Ok(result) => result,
        Err(never) => match never {},
    }
}

/// Classify the same canonical shape with observation before inspections.
pub(in crate::db) fn try_classify_grouped_order_term_for_field<E>(
    expr: &Expr,
    expected_group_field: GroupFieldRef<'_>,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<GroupedOrderTermAdmissibility, E> {
    observe(1)?;
    // The canonical proof admits only a field or field +/- numeric literal.
    // Reject other roots without walking descendants that cannot change that
    // syntactic proof. Keep mismatched fields distinct from unsupported shapes.
    let (field, class) = match expr {
        Expr::Field(_) | Expr::FieldPath(_) => (expr, GroupedOrderExprClass::CanonicalGroupField),
        Expr::Binary { op, left, right }
            if matches!(left.as_ref(), Expr::Field(_) | Expr::FieldPath(_))
                && is_numeric_order_offset_literal(right) =>
        {
            let class = match op {
                BinaryOp::Add => GroupedOrderExprClass::GroupFieldPlusConstant,
                BinaryOp::Sub => GroupedOrderExprClass::GroupFieldMinusConstant,
                _ => return Ok(GroupedOrderTermAdmissibility::UnsupportedExpression),
            };
            (left.as_ref(), class)
        }
        _ => return Ok(GroupedOrderTermAdmissibility::UnsupportedExpression),
    };

    Ok(
        if try_group_field_matches_expr(expected_group_field, field, observe)? {
            GroupedOrderTermAdmissibility::Preserves(class)
        } else {
            GroupedOrderTermAdmissibility::PrefixMismatch
        },
    )
}

// A stored label covers its root, path component bytes and separators. Its
// length bounds the equal-prefix work of the shared borrowed leaf comparator;
// mismatched representation/length may consume less. No label is constructed.
fn try_group_field_matches_expr<E>(
    field: GroupFieldRef<'_>,
    expr: &Expr,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<bool, E> {
    observe(1_u64.saturating_add(field.field().len() as u64))?;
    Ok(field.matches_expr(expr))
}

// Additive constant offsets preserve both ascending and descending order for
// the underlying grouped key while avoiding the tie/collapse behavior of the
// broader computed-order family.
const fn is_numeric_order_offset_literal(expr: &Expr) -> bool {
    matches!(
        expr,
        Expr::Literal(
            Value::Int64(_)
                | Value::Int128(_)
                | Value::IntBig(_)
                | Value::Nat64(_)
                | Value::Nat128(_)
                | Value::NatBig(_)
                | Value::Decimal(_)
                | Value::Float32(_)
                | Value::Float64(_)
        )
    )
}

/// Return true when one grouped `ORDER BY` term is admissible for the
/// aggregate/post-aggregate Top-K lane over the declared grouped key set.
#[must_use]
pub(in crate::db) fn classify_grouped_top_k_order_term(
    expr: &Expr,
    group_fields: &GroupFieldSet,
) -> GroupedTopKOrderTermAdmissibility {
    match try_classify_grouped_top_k_order_term(expr, group_fields, &mut |_| {
        Ok::<_, std::convert::Infallible>(())
    }) {
        Ok(result) => result,
        Err(never) => match never {},
    }
}

/// Observe each Top-K expression visit and candidate field comparison before work.
pub(in crate::db) fn try_classify_grouped_top_k_order_term<E>(
    expr: &Expr,
    group_fields: &GroupFieldSet,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<GroupedTopKOrderTermAdmissibility, E> {
    // A non-group field always rejects, regardless of later aggregates. When
    // there is no aggregate, any function is an unsupported scalar wrapper.
    // Aggregate inputs/filters remain leaves owned by pre-group validation.
    let mut contains_aggregate = false;
    let mut contains_function = false;
    let only_group_fields = expr.try_all_tree_expr(&mut |node| {
        observe(1)?;
        Ok(match node {
            Expr::Field(_) | Expr::FieldPath(_) => {
                for field in group_fields.iter() {
                    if try_group_field_matches_expr(field, node, observe)? {
                        return Ok(true);
                    }
                }
                false
            }
            Expr::Aggregate(_) => {
                contains_aggregate = true;
                true
            }
            Expr::FunctionCall { .. } => {
                contains_function = true;
                true
            }
            Expr::Literal(_) | Expr::Binary { .. } | Expr::Unary { .. } | Expr::Case { .. } => true,
            #[cfg(test)]
            Expr::Alias { .. } => true,
        })
    })?;

    if !only_group_fields {
        return Ok(GroupedTopKOrderTermAdmissibility::NonGroupFieldReference);
    }
    if !contains_aggregate && contains_function {
        return Ok(GroupedTopKOrderTermAdmissibility::UnsupportedExpression);
    }

    Ok(GroupedTopKOrderTermAdmissibility::Admissible)
}

/// Return true when one grouped post-aggregate order expression must leave the
/// canonical grouped-key ordered lane for bounded Top-K finalization.
#[must_use]
pub(in crate::db) fn grouped_top_k_order_term_requires_heap(expr: &Expr) -> bool {
    match try_grouped_top_k_order_term_requires_heap(expr, &mut |_| {
        Ok::<_, std::convert::Infallible>(())
    }) {
        Ok(result) => result,
        Err(never) => match never {},
    }
}

/// Observe the shared short-circuit heap search before inspecting each node.
pub(in crate::db) fn try_grouped_top_k_order_term_requires_heap<E>(
    expr: &Expr,
    observe: &mut impl FnMut(u64) -> Result<(), E>,
) -> Result<bool, E> {
    Ok(!expr.try_all_tree_expr(&mut |node| {
        observe(1)?;
        Ok(!matches!(node, Expr::Aggregate(_) | Expr::Case { .. }))
    })?)
}

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(ProjectionField {
Self::Scalar{expr,alias} => [expr,alias],
});
crate::retained::retained_fields!(ProjectionSelection {
Self::All => [],
Self::Fields(field_0) => [field_0],
Self::Exprs(field_0) => [field_0],
});
crate::retained::retained_fields!(ProjectionSpec {
Self{fields} => [fields],
});
