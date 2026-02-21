use crate::db::{
    executor::load::ExecutionAccessPathVariant,
    query::plan::{AccessPath, AccessPlan, Direction, OrderDirection},
};

pub(super) fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    match access {
        AccessPlan::Path(path) => match path.as_ref() {
            AccessPath::ByKey(_) => ExecutionAccessPathVariant::ByKey,
            AccessPath::ByKeys(_) => ExecutionAccessPathVariant::ByKeys,
            AccessPath::KeyRange { .. } => ExecutionAccessPathVariant::KeyRange,
            AccessPath::IndexPrefix { .. } => ExecutionAccessPathVariant::IndexPrefix,
            AccessPath::IndexRange { .. } => ExecutionAccessPathVariant::IndexRange,
            AccessPath::FullScan => ExecutionAccessPathVariant::FullScan,
        },
        AccessPlan::Union(_) => ExecutionAccessPathVariant::Union,
        AccessPlan::Intersection(_) => ExecutionAccessPathVariant::Intersection,
    }
}

pub(super) const fn execution_order_direction(direction: Direction) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}
