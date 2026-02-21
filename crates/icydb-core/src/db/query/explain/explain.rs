//! Deterministic, read-only explanation of logical plans; must not execute or validate.

use super::{
    AccessPlan, AccessPlanProjection, DeleteLimitSpec, LogicalPlan, OrderDirection, OrderSpec,
    PageSpec, project_access_plan,
};
use crate::{
    db::query::{
        ReadConsistency,
        intent::QueryMode,
        plan::validate::{
            PushdownSurfaceEligibility, SecondaryOrderPushdownEligibility,
            SecondaryOrderPushdownRejection, assess_secondary_order_pushdown,
        },
        predicate::{CompareOp, ComparePredicate, Predicate, coercion::CoercionSpec, normalize},
    },
    model::entity::EntityModel,
    traits::FieldValue,
    value::Value,
};
use std::ops::Bound;

///
/// ExplainPlan
///
/// Stable, deterministic representation of a `LogicalPlan` for observability.
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

impl<K> LogicalPlan<K>
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
        let predicate = match &self.predicate {
            Some(predicate) => ExplainPredicate::from_predicate(&normalize(predicate)),
            None => ExplainPredicate::None,
        };

        let order_by = explain_order(self.order.as_ref());
        let order_pushdown = explain_order_pushdown(model, self);
        let page = explain_page(self.page.as_ref());
        let delete_limit = explain_delete_limit(self.delete_limit.as_ref());

        ExplainPlan {
            mode: self.mode,
            access: ExplainAccessPath::from_access_plan(&self.access),
            predicate,
            order_by,
            distinct: self.distinct,
            order_pushdown,
            page,
            delete_limit,
            consistency: self.consistency,
        }
    }
}

fn explain_order_pushdown<K>(
    model: Option<&EntityModel>,
    plan: &LogicalPlan<K>,
) -> ExplainOrderPushdown {
    let Some(model) = model else {
        return ExplainOrderPushdown::MissingModelContext;
    };

    assess_secondary_order_pushdown(model, plan).into()
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
mod tests {
    use super::*;
    use crate::db::query::intent::{KeyAccess, LoadSpec, QueryMode, access_plan_from_keys_value};
    use crate::db::query::plan::{AccessPath, AccessPlan, LogicalPlan, OrderDirection, OrderSpec};
    use crate::db::query::predicate::Predicate;
    use crate::db::query::{ReadConsistency, builder::field::FieldRef};
    use crate::model::{field::FieldKind, index::IndexModel};
    use crate::traits::EntitySchema;
    use crate::types::Ulid;
    use crate::value::Value;

    const PUSHDOWN_INDEX_FIELDS: [&str; 1] = ["tag"];
    const PUSHDOWN_INDEX: IndexModel = IndexModel::new(
        "explain::pushdown_tag",
        "explain::pushdown_store",
        &PUSHDOWN_INDEX_FIELDS,
        false,
    );

    crate::test_entity! {
    ident = ExplainPushdownEntity,
        id = Ulid,
        entity_name = "PushdownEntity",
        primary_key = "id",
        pk_index = 0,
        fields = [
            ("id", FieldKind::Ulid),
            ("tag", FieldKind::Text),
            ("rank", FieldKind::Int),
        ],
        indexes = [&PUSHDOWN_INDEX],
    }

    #[test]
    fn explain_is_deterministic_for_same_query() {
        let predicate = FieldRef::new("id").eq(Ulid::default());
        let mut plan: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        plan.predicate = Some(predicate);

        assert_eq!(plan.explain(), plan.explain());
    }

    #[test]
    fn explain_is_deterministic_for_equivalent_predicates() {
        let id = Ulid::default();

        let predicate_a = Predicate::And(vec![
            FieldRef::new("id").eq(id),
            FieldRef::new("other").eq(Value::Text("x".to_string())),
        ]);
        let predicate_b = Predicate::And(vec![
            FieldRef::new("other").eq(Value::Text("x".to_string())),
            FieldRef::new("id").eq(id),
        ]);

        let mut plan_a: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        plan_a.predicate = Some(predicate_a);

        let mut plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        plan_b.predicate = Some(predicate_b);

        assert_eq!(plan_a.explain(), plan_b.explain());
    }

    #[test]
    fn explain_is_deterministic_for_by_keys() {
        let a = Ulid::from_u128(1);
        let b = Ulid::from_u128(2);

        let access_a = access_plan_from_keys_value(&KeyAccess::Many(vec![a, b, a]));
        let access_b = access_plan_from_keys_value(&KeyAccess::Many(vec![b, a]));

        let plan_a: LogicalPlan<Value> = LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            access: access_a,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        };
        let plan_b: LogicalPlan<Value> = LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            access: access_b,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        };

        assert_eq!(plan_a.explain(), plan_b.explain());
    }

    #[test]
    fn explain_reports_deterministic_index_choice() {
        const INDEX_FIELDS: [&str; 1] = ["idx_a"];
        const INDEX_A: IndexModel =
            IndexModel::new("explain::idx_a", "explain::store", &INDEX_FIELDS, false);
        const INDEX_B: IndexModel =
            IndexModel::new("explain::idx_a_alt", "explain::store", &INDEX_FIELDS, false);

        let mut indexes = [INDEX_B, INDEX_A];
        indexes.sort_by(|left, right| left.name.cmp(right.name));
        let chosen = indexes[0];

        let plan: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexPrefix {
                index: chosen,
                values: vec![Value::Text("alpha".to_string())],
            },
            crate::db::query::ReadConsistency::MissingOk,
        );

        let explain = plan.explain();
        match explain.access {
            ExplainAccessPath::IndexPrefix {
                name,
                fields,
                prefix_len,
                ..
            } => {
                assert_eq!(name, "explain::idx_a");
                assert_eq!(fields, vec!["idx_a"]);
                assert_eq!(prefix_len, 1);
            }
            _ => panic!("expected index prefix"),
        }
    }

    #[test]
    fn explain_differs_for_semantic_changes() {
        let plan_a: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::ByKey(Value::Ulid(Ulid::from_u128(1))),
            ReadConsistency::MissingOk,
        );
        let plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);

        assert_ne!(plan_a.explain(), plan_b.explain());
    }

    #[test]
    fn explain_with_model_reports_eligible_order_pushdown() {
        let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
        let mut plan: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_INDEX,
                values: vec![Value::Text("alpha".to_string())],
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });

        assert_eq!(
            plan.explain_with_model(model).order_pushdown,
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index: PUSHDOWN_INDEX.name,
                prefix_len: 1,
            }
        );
    }

    #[test]
    fn explain_with_model_reports_descending_pushdown_eligibility() {
        let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
        let mut plan: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_INDEX,
                values: vec![Value::Text("alpha".to_string())],
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Desc)],
        });

        assert_eq!(
            plan.explain_with_model(model).order_pushdown,
            ExplainOrderPushdown::EligibleSecondaryIndex {
                index: PUSHDOWN_INDEX.name,
                prefix_len: 1,
            }
        );
    }

    #[test]
    fn explain_with_model_reports_composite_index_range_pushdown_rejection_reason() {
        let model = <ExplainPushdownEntity as EntitySchema>::MODEL;
        let plan: LogicalPlan<Value> = LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            access: AccessPlan::Union(vec![
                AccessPlan::path(AccessPath::IndexRange {
                    index: PUSHDOWN_INDEX,
                    prefix: vec![],
                    lower: Bound::Included(Value::Text("alpha".to_string())),
                    upper: Bound::Excluded(Value::Text("omega".to_string())),
                }),
                AccessPlan::path(AccessPath::FullScan),
            ]),
            predicate: None,
            order: Some(OrderSpec {
                fields: vec![("id".to_string(), OrderDirection::Asc)],
            }),
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        };

        assert_eq!(
            plan.explain_with_model(model).order_pushdown,
            ExplainOrderPushdown::Rejected(
                SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                    index: PUSHDOWN_INDEX.name,
                    prefix_len: 0,
                }
            )
        );
    }

    #[test]
    fn explain_without_model_reports_missing_model_context() {
        let mut plan: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexPrefix {
                index: PUSHDOWN_INDEX,
                values: vec![Value::Text("alpha".to_string())],
            },
            ReadConsistency::MissingOk,
        );
        plan.order = Some(OrderSpec {
            fields: vec![("id".to_string(), OrderDirection::Asc)],
        });

        assert_eq!(
            plan.explain().order_pushdown,
            ExplainOrderPushdown::MissingModelContext
        );
    }

    #[test]
    #[expect(clippy::too_many_lines)]
    fn explain_pushdown_conversion_covers_all_variants() {
        let cases = vec![
            (
                SecondaryOrderPushdownEligibility::Eligible {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
                ExplainOrderPushdown::EligibleSecondaryIndex {
                    index: "explain::pushdown_tag",
                    prefix_len: 1,
                },
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::NoOrderBy,
                ),
                ExplainOrderPushdown::Rejected(SecondaryOrderPushdownRejection::NoOrderBy),
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
                ),
                ExplainOrderPushdown::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathNotSingleIndexPrefix,
                ),
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: "explain::pushdown_tag",
                        prefix_len: 1,
                    },
                ),
                ExplainOrderPushdown::Rejected(
                    SecondaryOrderPushdownRejection::AccessPathIndexRangeUnsupported {
                        index: "explain::pushdown_tag",
                        prefix_len: 1,
                    },
                ),
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                        prefix_len: 3,
                        index_field_len: 2,
                    },
                ),
                ExplainOrderPushdown::Rejected(
                    SecondaryOrderPushdownRejection::InvalidIndexPrefixBounds {
                        prefix_len: 3,
                        index_field_len: 2,
                    },
                ),
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                        field: "id".to_string(),
                    },
                ),
                ExplainOrderPushdown::Rejected(
                    SecondaryOrderPushdownRejection::MissingPrimaryKeyTieBreak {
                        field: "id".to_string(),
                    },
                ),
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                        field: "id".to_string(),
                    },
                ),
                ExplainOrderPushdown::Rejected(
                    SecondaryOrderPushdownRejection::PrimaryKeyDirectionNotAscending {
                        field: "id".to_string(),
                    },
                ),
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                        field: "rank".to_string(),
                    },
                ),
                ExplainOrderPushdown::Rejected(
                    SecondaryOrderPushdownRejection::MixedDirectionNotEligible {
                        field: "rank".to_string(),
                    },
                ),
            ),
            (
                SecondaryOrderPushdownEligibility::Rejected(
                    SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                        index: "explain::pushdown_tag",
                        prefix_len: 1,
                        expected_suffix: vec!["rank".to_string()],
                        expected_full: vec!["group".to_string(), "rank".to_string()],
                        actual: vec!["other".to_string()],
                    },
                ),
                ExplainOrderPushdown::Rejected(
                    SecondaryOrderPushdownRejection::OrderFieldsDoNotMatchIndex {
                        index: "explain::pushdown_tag",
                        prefix_len: 1,
                        expected_suffix: vec!["rank".to_string()],
                        expected_full: vec!["group".to_string(), "rank".to_string()],
                        actual: vec!["other".to_string()],
                    },
                ),
            ),
        ];

        for (input, expected) in cases {
            assert_eq!(ExplainOrderPushdown::from(input), expected);
        }
    }
}
