//! Deterministic, read-only explanation of logical plans; must not execute or validate.

use crate::{
    db::{
        access::{
            AccessPlan, PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection,
        },
        contracts::{CoercionSpec, CompareOp, ComparePredicate, Predicate, ReadConsistency},
        query::{
            intent::QueryMode,
            plan::{
                AccessPlanProjection, AccessPlannedQuery, DeleteLimitSpec, LogicalPlan,
                OrderDirection, OrderSpec, PageSpec, ScalarPlan,
                assess_secondary_order_pushdown_from_parts, project_access_plan,
            },
            predicate::normalize,
        },
    },
    model::entity::EntityModel,
    traits::FieldValue,
    value::Value,
};
use std::ops::Bound;

///
/// ExplainPlan
///
/// Stable, deterministic representation of a planned query for observability.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainPlan {
    pub mode: QueryMode,
    pub access: ExplainAccessPath,
    pub predicate: ExplainPredicate,
    pub order_by: ExplainOrderBy,
    pub distinct: bool,
    pub order_pushdown: ExplainOrderPushdown,
    pub page: ExplainPagination,
    pub delete_limit: ExplainDeleteLimit,
    pub consistency: ReadConsistency,
}

///
/// ExplainOrderPushdown
///
/// Deterministic ORDER BY pushdown eligibility reported by explain.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainOrderPushdown {
    MissingModelContext,
    EligibleSecondaryIndex {
        index: &'static str,
        prefix_len: usize,
    },
    Rejected(SecondaryOrderPushdownRejection),
}

///
/// ExplainAccessPath
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainAccessPath {
    ByKey {
        key: Value,
    },
    ByKeys {
        keys: Vec<Value>,
    },
    KeyRange {
        start: Value,
        end: Value,
    },
    IndexPrefix {
        name: &'static str,
        fields: Vec<&'static str>,
        prefix_len: usize,
        values: Vec<Value>,
    },
    IndexRange {
        name: &'static str,
        fields: Vec<&'static str>,
        prefix_len: usize,
        prefix: Vec<Value>,
        lower: Bound<Value>,
        upper: Bound<Value>,
    },
    FullScan,
    Union(Vec<Self>),
    Intersection(Vec<Self>),
}

///
/// ExplainPredicate
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainPredicate {
    None,
    True,
    False,
    And(Vec<Self>),
    Or(Vec<Self>),
    Not(Box<Self>),
    Compare {
        field: String,
        op: CompareOp,
        value: Value,
        coercion: CoercionSpec,
    },
    IsNull {
        field: String,
    },
    IsMissing {
        field: String,
    },
    IsEmpty {
        field: String,
    },
    IsNotEmpty {
        field: String,
    },
    TextContains {
        field: String,
        value: Value,
    },
    TextContainsCi {
        field: String,
        value: Value,
    },
}

///
/// ExplainOrderBy
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainOrderBy {
    None,
    Fields(Vec<ExplainOrder>),
}

///
/// ExplainOrder
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainOrder {
    pub field: String,
    pub direction: OrderDirection,
}

///
/// ExplainPagination
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainPagination {
    None,
    Page { limit: Option<u32>, offset: u32 },
}

///
/// ExplainDeleteLimit
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainDeleteLimit {
    None,
    Limit { max_rows: u32 },
}

impl<K> AccessPlannedQuery<K>
where
    K: FieldValue,
{
    /// Produce a stable, deterministic explanation of this logical plan.
    #[must_use]
    pub(crate) fn explain(&self) -> ExplainPlan {
        self.explain_inner(None)
    }

    /// Produce a stable, deterministic explanation of this logical plan
    /// with model-aware pushdown eligibility diagnostics.
    #[must_use]
    pub(crate) fn explain_with_model(&self, model: &EntityModel) -> ExplainPlan {
        self.explain_inner(Some(model))
    }

    fn explain_inner(&self, model: Option<&EntityModel>) -> ExplainPlan {
        let logical = match &self.logical {
            LogicalPlan::Scalar(logical) => logical,
            LogicalPlan::Grouped(logical) => &logical.scalar,
        };

        explain_scalar_inner(logical, model, &self.access)
    }
}

fn explain_scalar_inner<K>(
    logical: &ScalarPlan,
    model: Option<&EntityModel>,
    access: &AccessPlan<K>,
) -> ExplainPlan
where
    K: FieldValue,
{
    let predicate = match &logical.predicate {
        Some(predicate) => ExplainPredicate::from_predicate(&normalize(predicate)),
        None => ExplainPredicate::None,
    };

    let order_by = explain_order(logical.order.as_ref());
    let order_pushdown = explain_order_pushdown(model, logical, access);
    let page = explain_page(logical.page.as_ref());
    let delete_limit = explain_delete_limit(logical.delete_limit.as_ref());

    ExplainPlan {
        mode: logical.mode,
        access: ExplainAccessPath::from_access_plan(access),
        predicate,
        order_by,
        distinct: logical.distinct,
        order_pushdown,
        page,
        delete_limit,
        consistency: logical.consistency,
    }
}

fn explain_order_pushdown<K>(
    model: Option<&EntityModel>,
    logical: &ScalarPlan,
    access: &AccessPlan<K>,
) -> ExplainOrderPushdown {
    let Some(model) = model else {
        return ExplainOrderPushdown::MissingModelContext;
    };
    assess_secondary_order_pushdown_from_parts(model, logical, access).into()
}

impl From<SecondaryOrderPushdownEligibility> for ExplainOrderPushdown {
    fn from(value: SecondaryOrderPushdownEligibility) -> Self {
        Self::from(PushdownSurfaceEligibility::from(&value))
    }
}

impl From<PushdownSurfaceEligibility<'_>> for ExplainOrderPushdown {
    fn from(value: PushdownSurfaceEligibility<'_>) -> Self {
        match value {
            PushdownSurfaceEligibility::EligibleSecondaryIndex { index, prefix_len } => {
                Self::EligibleSecondaryIndex { index, prefix_len }
            }
            PushdownSurfaceEligibility::Rejected { reason } => Self::Rejected(reason.clone()),
        }
    }
}

struct ExplainAccessProjection;

impl<K> AccessPlanProjection<K> for ExplainAccessProjection
where
    K: FieldValue,
{
    type Output = ExplainAccessPath;

    fn by_key(&mut self, key: &K) -> Self::Output {
        ExplainAccessPath::ByKey {
            key: key.to_value(),
        }
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        ExplainAccessPath::ByKeys {
            keys: keys.iter().map(FieldValue::to_value).collect(),
        }
    }

    fn key_range(&mut self, start: &K, end: &K) -> Self::Output {
        ExplainAccessPath::KeyRange {
            start: start.to_value(),
            end: end.to_value(),
        }
    }

    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        ExplainAccessPath::IndexPrefix {
            name: index_name,
            fields: index_fields.to_vec(),
            prefix_len,
            values: values.to_vec(),
        }
    }

    fn index_range(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        ExplainAccessPath::IndexRange {
            name: index_name,
            fields: index_fields.to_vec(),
            prefix_len,
            prefix: prefix.to_vec(),
            lower: lower.clone(),
            upper: upper.clone(),
        }
    }

    fn full_scan(&mut self) -> Self::Output {
        ExplainAccessPath::FullScan
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        ExplainAccessPath::Union(children)
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        ExplainAccessPath::Intersection(children)
    }
}

impl ExplainAccessPath {
    fn from_access_plan<K>(access: &AccessPlan<K>) -> Self
    where
        K: FieldValue,
    {
        let mut projection = ExplainAccessProjection;
        project_access_plan(access, &mut projection)
    }
}

impl ExplainPredicate {
    fn from_predicate(predicate: &Predicate) -> Self {
        match predicate {
            Predicate::True => Self::True,
            Predicate::False => Self::False,
            Predicate::And(children) => {
                Self::And(children.iter().map(Self::from_predicate).collect())
            }
            Predicate::Or(children) => {
                Self::Or(children.iter().map(Self::from_predicate).collect())
            }
            Predicate::Not(inner) => Self::Not(Box::new(Self::from_predicate(inner))),
            Predicate::Compare(compare) => Self::from_compare(compare),
            Predicate::IsNull { field } => Self::IsNull {
                field: field.clone(),
            },
            Predicate::IsMissing { field } => Self::IsMissing {
                field: field.clone(),
            },
            Predicate::IsEmpty { field } => Self::IsEmpty {
                field: field.clone(),
            },
            Predicate::IsNotEmpty { field } => Self::IsNotEmpty {
                field: field.clone(),
            },
            Predicate::TextContains { field, value } => Self::TextContains {
                field: field.clone(),
                value: value.clone(),
            },
            Predicate::TextContainsCi { field, value } => Self::TextContainsCi {
                field: field.clone(),
                value: value.clone(),
            },
        }
    }

    fn from_compare(compare: &ComparePredicate) -> Self {
        Self::Compare {
            field: compare.field.clone(),
            op: compare.op,
            value: compare.value.clone(),
            coercion: compare.coercion.clone(),
        }
    }
}

fn explain_order(order: Option<&OrderSpec>) -> ExplainOrderBy {
    let Some(order) = order else {
        return ExplainOrderBy::None;
    };

    if order.fields.is_empty() {
        return ExplainOrderBy::None;
    }

    ExplainOrderBy::Fields(
        order
            .fields
            .iter()
            .map(|(field, direction)| ExplainOrder {
                field: field.clone(),
                direction: *direction,
            })
            .collect(),
    )
}

const fn explain_page(page: Option<&PageSpec>) -> ExplainPagination {
    match page {
        Some(page) => ExplainPagination::Page {
            limit: page.limit,
            offset: page.offset,
        },
        None => ExplainPagination::None,
    }
}

const fn explain_delete_limit(limit: Option<&DeleteLimitSpec>) -> ExplainDeleteLimit {
    match limit {
        Some(limit) => ExplainDeleteLimit::Limit {
            max_rows: limit.max_rows,
        },
        None => ExplainDeleteLimit::None,
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
