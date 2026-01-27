//! Deterministic, read-only explanation of logical plans; must not execute or validate.

use super::{
    AccessPath, AccessPlan, DeleteLimitSpec, LogicalPlan, OrderDirection, OrderSpec, PageSpec,
    ProjectionSpec,
};
use crate::db::query::QueryMode;
use crate::db::query::predicate::{
    CompareOp, ComparePredicate, Predicate, coercion::CoercionSpec, normalize,
};
use crate::{db::query::ReadConsistency, key::Key, value::Value};

///
/// ExplainPlan
///
/// Stable, deterministic projection of a `LogicalPlan` for observability.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExplainPlan {
    pub mode: QueryMode,
    pub access: ExplainAccessPath,
    pub predicate: ExplainPredicate,
    pub order_by: ExplainOrderBy,
    pub page: ExplainPagination,
    pub delete_limit: ExplainDeleteLimit,
    pub projection: ExplainProjection,
    pub consistency: ReadConsistency,
}

///
/// ExplainAccessPath
///

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
    Page { limit: Option<u32>, offset: u64 },
}

///
/// ExplainDeleteLimit
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainDeleteLimit {
    None,
    Limit { max_rows: u32 },
}

///
/// ExplainProjection
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExplainProjection {
    All,
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
        let delete_limit = explain_delete_limit(self.delete_limit.as_ref());
        let projection = ExplainProjection::from_spec(&self.projection);

        ExplainPlan {
            mode: self.mode,
            access: ExplainAccessPath::from_access_plan(&self.access),
            predicate,
            order_by,
            page,
            delete_limit,
            projection,
            consistency: self.consistency,
        }
    }
}

impl ExplainAccessPath {
    fn from_access_plan(access: &AccessPlan) -> Self {
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

    fn from_path(path: &AccessPath) -> Self {
        match path {
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

const fn explain_delete_limit(limit: Option<&DeleteLimitSpec>) -> ExplainDeleteLimit {
    match limit {
        Some(limit) => ExplainDeleteLimit::Limit {
            max_rows: limit.max_rows,
        },
        None => ExplainDeleteLimit::None,
    }
}

impl ExplainProjection {
    const fn from_spec(spec: &ProjectionSpec) -> Self {
        match spec {
            ProjectionSpec::All => Self::All,
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::query::{Query, ReadConsistency, eq};
    use crate::model::index::IndexModel;
    use crate::types::Ulid;
    use crate::value::Value;
    use crate::{
        db::query::plan::{AccessPath, LogicalPlan, planner::PlannerEntity},
        key::Key,
    };

    #[test]
    fn explain_is_deterministic_for_same_query() {
        let query = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
            .filter(eq("id", Ulid::default()));

        let plan_a = query.plan().expect("plan a");
        let plan_b = query.plan().expect("plan b");

        assert_eq!(plan_a.explain(), plan_b.explain());
    }

    #[test]
    fn explain_is_deterministic_for_equivalent_predicates() {
        let id = Ulid::default();
        let query_a = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
            .filter(eq("id", id))
            .filter(eq("other", "x"));
        let query_b = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
            .filter(eq("other", "x"))
            .filter(eq("id", id));

        let plan_a = query_a.plan().expect("plan a");
        let plan_b = query_b.plan().expect("plan b");

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

        let plan = LogicalPlan::new(
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
        let plan_a = LogicalPlan::new(
            AccessPath::ByKey(Key::Ulid(Ulid::from_u128(1))),
            crate::db::query::ReadConsistency::MissingOk,
        );
        let plan_b = LogicalPlan::new(
            AccessPath::FullScan,
            crate::db::query::ReadConsistency::MissingOk,
        );

        assert_ne!(plan_a.explain(), plan_b.explain());
    }
}
