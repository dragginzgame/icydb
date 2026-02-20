use crate::{
    db::{
        executor::load::ExecutionAccessPathVariant,
        query::plan::{
            AccessPlan, AccessPlanProjection, Direction, OrderDirection, project_access_plan,
        },
    },
    value::Value,
};
use std::ops::Bound;

// Trace-only projection from plan access shapes to coarse execution trace variants.
struct ExecutionAccessProjection;

impl<K> AccessPlanProjection<K> for ExecutionAccessProjection {
    type Output = ExecutionAccessPathVariant;

    fn by_key(&mut self, _key: &K) -> Self::Output {
        ExecutionAccessPathVariant::ByKey
    }

    fn by_keys(&mut self, _keys: &[K]) -> Self::Output {
        ExecutionAccessPathVariant::ByKeys
    }

    fn key_range(&mut self, _start: &K, _end: &K) -> Self::Output {
        ExecutionAccessPathVariant::KeyRange
    }

    fn index_prefix(
        &mut self,
        _index_name: &'static str,
        _index_fields: &[&'static str],
        _prefix_len: usize,
        _values: &[Value],
    ) -> Self::Output {
        ExecutionAccessPathVariant::IndexPrefix
    }

    fn index_range(
        &mut self,
        _index_name: &'static str,
        _index_fields: &[&'static str],
        _prefix_len: usize,
        _prefix: &[Value],
        _lower: &Bound<Value>,
        _upper: &Bound<Value>,
    ) -> Self::Output {
        ExecutionAccessPathVariant::IndexRange
    }

    fn full_scan(&mut self) -> Self::Output {
        ExecutionAccessPathVariant::FullScan
    }

    fn union(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        ExecutionAccessPathVariant::Union
    }

    fn intersection(&mut self, _children: Vec<Self::Output>) -> Self::Output {
        ExecutionAccessPathVariant::Intersection
    }
}

pub(super) fn access_path_variant<K>(access: &AccessPlan<K>) -> ExecutionAccessPathVariant {
    let mut projection = ExecutionAccessProjection;
    project_access_plan(access, &mut projection)
}

pub(super) const fn execution_order_direction(direction: Direction) -> OrderDirection {
    match direction {
        Direction::Asc => OrderDirection::Asc,
        Direction::Desc => OrderDirection::Desc,
    }
}
