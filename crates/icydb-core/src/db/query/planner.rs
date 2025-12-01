use crate::{
    IndexSpec, Key, Value,
    db::primitives::filter::{Cmp, FilterExpr},
    obs::metrics,
    traits::EntityKind,
};
use std::fmt::{self, Display};

///
/// QueryPlan
///

#[derive(Debug)]
pub enum QueryPlan {
    FullScan,
    Index(IndexPlan),
    Keys(Vec<Key>),
    Range(Key, Key),
}

impl fmt::Display for QueryPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Index(plan) => write!(f, "Index({plan})"),

            Self::Keys(keys) => {
                // Show up to 5 keys, then ellipsize
                let preview: Vec<String> = keys.iter().take(5).map(|k| format!("{k:?}")).collect();

                if keys.len() > 5 {
                    write!(f, "Keys[{}… total {}]", preview.join(", "), keys.len())
                } else {
                    write!(f, "Keys[{}]", preview.join(", "))
                }
            }

            Self::Range(start, end) => {
                write!(f, "Range({start:?} → {end:?})")
            }

            Self::FullScan => write!(f, "FullScan"),
        }
    }
}

///
/// IndexPlan
///

#[derive(Debug)]
pub struct IndexPlan {
    pub index: &'static IndexSpec,
    pub values: Vec<Value>,
}

impl Display for IndexPlan {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let values: Vec<String> = self.values.iter().map(|v| format!("{v:?}")).collect();
        write!(f, "index={} values=[{}]", self.index, values.join(", "))
    }
}

///
/// QueryPlanner
///

#[derive(Debug)]
pub struct QueryPlanner {
    pub filter: Option<FilterExpr>,
}

impl QueryPlanner {
    #[must_use]
    pub fn new(filter: Option<&FilterExpr>) -> Self {
        Self {
            filter: filter.cloned(),
        }
    }

    #[must_use]
    pub fn plan<E: EntityKind>(&self) -> QueryPlan {
        // If filter is a primary key match
        // this would handle One and Many queries
        if let Some(plan) = self.extract_from_filter::<E>() {
            metrics::with_state_mut(|m| match plan {
                QueryPlan::Keys(_) => m.ops.plan_keys += 1,
                QueryPlan::Index(_) => m.ops.plan_index += 1,
                QueryPlan::Range(_, _) | QueryPlan::FullScan => m.ops.plan_range += 1,
            });
            return plan;
        }

        // check for index matches
        // THIS WILL DO THE INDEX LOOKUPS
        if !E::INDEXES.is_empty()
            && let Some(plan) = self.extract_from_index::<E>()
        {
            metrics::with_state_mut(|m| m.ops.plan_index += 1);
            return plan;
        }

        // Fallback: do a full scan
        metrics::with_state_mut(|m| m.ops.plan_range += 1);

        QueryPlan::FullScan
    }

    // extract_from_filter
    fn extract_from_filter<E: EntityKind>(&self) -> Option<QueryPlan> {
        let Some(filter) = &self.filter else {
            return None;
        };

        match filter {
            FilterExpr::Clause(clause) if clause.field == E::PRIMARY_KEY => match clause.cmp {
                Cmp::Eq => clause.value.as_key().map(|key| QueryPlan::Keys(vec![key])),

                Cmp::In => {
                    if let Value::List(values) = &clause.value {
                        let keys = values.iter().filter_map(Value::as_key).collect::<Vec<_>>();

                        if keys.is_empty() {
                            None
                        } else {
                            Some(QueryPlan::Keys(keys))
                        }
                    } else {
                        None
                    }
                }

                _ => None,
            },

            _ => None,
        }
    }

    // extract_from_index: build a leftmost equality prefix in terms of Value
    fn extract_from_index<E: EntityKind>(&self) -> Option<QueryPlan> {
        let Some(filter) = &self.filter else {
            return None;
        };

        let mut best: Option<(usize, IndexPlan)> = None;

        for index in E::INDEXES {
            // Build leftmost equality prefix (only == supported for hashed indexes)
            let mut values: Vec<Value> = Vec::with_capacity(index.fields.len());

            for field in index.fields {
                if let Some(v) = Self::find_eq_value(filter, field) {
                    values.push(v);
                } else {
                    break; // stop at first non-match
                }
            }

            // Skip indexes that produced no equality prefix
            if values.is_empty() {
                continue;
            }

            let score = values.len();
            let cand = (score, IndexPlan { index, values });

            match &best {
                Some((best_score, _)) if *best_score >= score => { /* keep current best */ }
                _ => best = Some(cand),
            }
        }

        best.map(|(_, plan)| QueryPlan::Index(plan))
    }

    /// Find an equality clause (`field == ?`) anywhere in the filter tree and return the Value.
    fn find_eq_value(filter: &FilterExpr, field: &str) -> Option<Value> {
        match filter {
            FilterExpr::Clause(c) if c.field == field && matches!(c.cmp, Cmp::Eq) => {
                Some(c.value.clone())
            }
            // Walk conjunctive subtrees
            FilterExpr::And(list) => list.iter().find_map(|f| Self::find_eq_value(f, field)),
            _ => None,
        }
    }
}
