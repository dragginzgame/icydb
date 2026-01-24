//! Deterministic, read-only explanation of logical plans; must not execute or validate.

use super::{AccessPath, LogicalPlan, OrderDirection, OrderSpec, PageSpec};
use crate::db::query::predicate::{
    CompareOp, ComparePredicate, Predicate, coercion::CoercionSpec, normalize,
};
use crate::{key::Key, value::Value};

///
/// ExplainPlan
///
/// Stable, deterministic projection of a `LogicalPlan` for observability.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainPlan {
    pub access: ExplainAccessPath,
    pub predicate: ExplainPredicate,
    pub order_by: ExplainOrderBy,
    pub page: ExplainPagination,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainAccessPath {
    ByKey {
        key: Key,
    },
    ByKeys {
        keys: Vec<Key>,
    },
    KeyRange {
        start: Key,
        end: Key,
    },
    IndexPrefix {
        name: &'static str,
        fields: Vec<&'static str>,
        prefix_len: usize,
        values: Vec<Value>,
    },
    FullScan,
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

impl LogicalPlan {
    /// Produce a stable, deterministic explanation of this logical plan.
    #[must_use]
    pub fn explain(&self) -> ExplainPlan {
        let predicate = match &self.predicate {
            Some(predicate) => ExplainPredicate::from_predicate(&normalize(predicate)),
            None => ExplainPredicate::None,
        };

        let order_by = explain_order(self.order.as_ref());
        let page = explain_page(self.page.as_ref());

        ExplainPlan {
            access: ExplainAccessPath::from_access(&self.access),
            predicate,
            order_by,
            page,
        }
    }
}

impl ExplainAccessPath {
    fn from_access(access: &AccessPath) -> Self {
        match access {
            AccessPath::ByKey(key) => Self::ByKey { key: *key },
            AccessPath::ByKeys(keys) => Self::ByKeys { keys: keys.clone() },
            AccessPath::KeyRange { start, end } => Self::KeyRange {
                start: *start,
                end: *end,
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::builder::{QueryBuilder, eq};
    use crate::db::query::plan::planner::PlannerEntity;
    use crate::db::query::plan::{AccessPath, LogicalPlan};
    use crate::key::Key;
    use crate::model::index::IndexModel;
    use crate::types::Ulid;
    use crate::value::Value;
    use icydb_schema::node::Schema;

    #[test]
    fn explain_is_deterministic_for_same_query() {
        let spec = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("id", Ulid::default()))
            .build();
        let schema = Schema::new();

        let plan_a = spec.plan::<PlannerEntity>(&schema).expect("plan a");
        let plan_b = spec.plan::<PlannerEntity>(&schema).expect("plan b");

        assert_eq!(plan_a.explain(), plan_b.explain());
    }

    #[test]
    fn explain_is_deterministic_for_equivalent_predicates() {
        let id = Ulid::default();
        let spec_a = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("id", id))
            .and(eq("other", "x"))
            .build();
        let spec_b = QueryBuilder::<PlannerEntity>::new()
            .filter(eq("other", "x"))
            .and(eq("id", id))
            .build();

        let schema = Schema::new();
        let plan_a = spec_a.plan::<PlannerEntity>(&schema).expect("plan a");
        let plan_b = spec_b.plan::<PlannerEntity>(&schema).expect("plan b");

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

        let plan = LogicalPlan::new(AccessPath::IndexPrefix {
            index: chosen,
            values: vec![Value::Text("alpha".to_string())],
        });

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
        let plan_a = LogicalPlan::new(AccessPath::ByKey(Key::Ulid(Ulid::from_u128(1))));
        let plan_b = LogicalPlan::new(AccessPath::FullScan);

        assert_ne!(plan_a.explain(), plan_b.explain());
    }
}
