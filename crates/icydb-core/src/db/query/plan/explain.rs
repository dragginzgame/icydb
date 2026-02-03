//! Deterministic, read-only explanation of logical plans; must not execute or validate.

use super::{
    AccessPath, AccessPlan, DeleteLimitSpec, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
};
use crate::db::query::QueryMode;
use crate::db::query::predicate::{
    CompareOp, ComparePredicate, Predicate, coercion::CoercionSpec, normalize,
};
use crate::{db::query::ReadConsistency, traits::FieldValue, value::Value};

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
    pub page: ExplainPagination,
    pub delete_limit: ExplainDeleteLimit,
    pub consistency: ReadConsistency,
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
    MapContainsKey {
        field: String,
        key: Value,
        coercion: CoercionSpec,
    },
    MapContainsValue {
        field: String,
        value: Value,
        coercion: CoercionSpec,
    },
    MapContainsEntry {
        field: String,
        key: Value,
        value: Value,
        coercion: CoercionSpec,
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
    pub fn explain(&self) -> ExplainPlan {
        let predicate = match &self.predicate {
            Some(predicate) => ExplainPredicate::from_predicate(&normalize(predicate)),
            None => ExplainPredicate::None,
        };

        let order_by = explain_order(self.order.as_ref());
        let page = explain_page(self.page.as_ref());
        let delete_limit = explain_delete_limit(self.delete_limit.as_ref());

        ExplainPlan {
            mode: self.mode,
            access: ExplainAccessPath::from_access_plan(&self.access),
            predicate,
            order_by,
            page,
            delete_limit,
            consistency: self.consistency,
        }
    }
}

impl ExplainAccessPath {
    fn from_access_plan<K>(access: &AccessPlan<K>) -> Self
    where
        K: FieldValue,
    {
        match access {
            AccessPlan::Path(path) => Self::from_path(path),
            AccessPlan::Union(children) => {
                Self::Union(children.iter().map(Self::from_access_plan).collect())
            }
            AccessPlan::Intersection(children) => {
                Self::Intersection(children.iter().map(Self::from_access_plan).collect())
            }
        }
    }

    fn from_path<K>(path: &AccessPath<K>) -> Self
    where
        K: FieldValue,
    {
        match path {
            AccessPath::ByKey(key) => Self::ByKey {
                key: key.to_value(),
            },
            AccessPath::ByKeys(keys) => Self::ByKeys {
                keys: keys.iter().map(FieldValue::to_value).collect(),
            },
            AccessPath::KeyRange { start, end } => Self::KeyRange {
                start: start.to_value(),
                end: end.to_value(),
            },
            AccessPath::IndexPrefix { index, values } => Self::IndexPrefix {
                name: index.name,
                fields: index.fields.to_vec(),
                prefix_len: values.len(),
                values: values.clone(),
            },
            AccessPath::FullScan => Self::FullScan,
        }
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
            Predicate::MapContainsKey {
                field,
                key,
                coercion,
            } => Self::MapContainsKey {
                field: field.clone(),
                key: key.clone(),
                coercion: coercion.clone(),
            },
            Predicate::MapContainsValue {
                field,
                value,
                coercion,
            } => Self::MapContainsValue {
                field: field.clone(),
                value: value.clone(),
                coercion: coercion.clone(),
            },
            Predicate::MapContainsEntry {
                field,
                key,
                value,
                coercion,
            } => Self::MapContainsEntry {
                field: field.clone(),
                key: key.clone(),
                value: value.clone(),
                coercion: coercion.clone(),
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
    use crate::db::query::intent::{KeyAccess, access_plan_from_keys_value};
    use crate::db::query::plan::{AccessPath, LogicalPlan};
    use crate::db::query::predicate::Predicate;
    use crate::db::query::{FieldRef, LoadSpec, QueryMode, ReadConsistency};
    use crate::model::index::IndexModel;
    use crate::types::Ulid;
    use crate::value::Value;

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
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        };
        let plan_b: LogicalPlan<Value> = LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            access: access_b,
            predicate: None,
            order: None,
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
}
