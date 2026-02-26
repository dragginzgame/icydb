use crate::{
    db::{
        executor::{
            ExecutablePlan,
            aggregate_model::field::{FieldSlot, extract_orderable_field_value},
            load::LoadExecutor,
        },
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    value::Value,
};
use std::{cmp::Ordering, collections::BTreeSet};

///
/// CanonicalDistinctValue
///
/// Canonical set key wrapper for `count_distinct_by` value deduplication.
/// Uses `Value::canonical_cmp` to provide a total ordering for `BTreeSet`.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct CanonicalDistinctValue(Value);

impl Ord for CanonicalDistinctValue {
    fn cmp(&self, other: &Self) -> Ordering {
        let ordering = Value::canonical_cmp(&self.0, &other.0);
        debug_assert!(
            (ordering == Ordering::Equal) == (self.0 == other.0),
            "canonical distinct ordering must preserve Value equality semantics",
        );

        ordering
    }
}

impl PartialOrd for CanonicalDistinctValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db) fn aggregate_count_distinct_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<u32, InternalError> {
        let target_field = target_field.into();
        let field_slot = Self::resolve_any_field_slot(target_field.as_str())?;
        let response = self.execute(plan)?;

        Self::aggregate_count_distinct_field_from_materialized(
            response,
            target_field.as_str(),
            field_slot,
        )
    }

    // Reduce one materialized response into `count_distinct(field)` by
    // counting unique typed field values across the effective response window.
    fn aggregate_count_distinct_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<u32, InternalError> {
        let mut distinct_values: BTreeSet<CanonicalDistinctValue> = BTreeSet::new();
        let mut distinct_count = 0u32;
        for (_, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if distinct_values.insert(CanonicalDistinctValue(value)) {
                distinct_count = distinct_count.saturating_add(1);
            }
        }

        Ok(distinct_count)
    }
}
