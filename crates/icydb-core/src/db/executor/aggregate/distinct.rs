use crate::{
    db::{
        executor::{
            ExecutablePlan,
            aggregate::field::{FieldSlot, extract_orderable_field_value},
            load::LoadExecutor,
        },
        group_key::{GroupKeySet, KeyCanonicalError},
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

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
    // This is value DISTINCT semantics and intentionally uses canonical
    // `GroupKey` equality (not row/DataKey identity).
    fn aggregate_count_distinct_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<u32, InternalError> {
        let mut distinct_values = GroupKeySet::default();
        let mut distinct_count = 0u32;
        for (_, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(Self::map_aggregate_field_value_error)?;
            if distinct_values
                .insert_value(&value)
                .map_err(KeyCanonicalError::into_internal_error)?
            {
                distinct_count = distinct_count.saturating_add(1);
            }
        }

        Ok(distinct_count)
    }
}
