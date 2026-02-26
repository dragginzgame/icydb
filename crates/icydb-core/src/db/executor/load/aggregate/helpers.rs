use crate::{
    db::{
        Context,
        executor::{
            ExecutablePlan,
            aggregate::AggregateKind,
            aggregate::field::{
                AggregateFieldValueError, FieldSlot, compare_orderable_field_values,
                extract_orderable_field_value,
            },
            load::{LoadExecutor, aggregate::MinMaxByIds},
        },
        query::{ReadConsistency, plan::Direction},
        response::Response,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};
use std::cmp::Ordering;

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    // Canonical precedence predicate for field projections under deterministic
    // field ordering with primary-key ascending tie-break.
    fn field_projection_candidate_precedes(
        target_field: &str,
        candidate_id: &Id<E>,
        candidate_value: &Value,
        current_id: &Id<E>,
        current_value: &Value,
        field_preference: Ordering,
    ) -> Result<bool, InternalError> {
        let field_order =
            compare_orderable_field_values(target_field, candidate_value, current_value)
                .map_err(Self::map_aggregate_field_value_error)?;
        if field_order == field_preference {
            return Ok(true);
        }

        Ok(field_order == Ordering::Equal && candidate_id.key() < current_id.key())
    }

    // Execute one field-target nth aggregate (`nth(field, n)`) via canonical
    // materialized fallback semantics.
    pub(in crate::db::executor::load::aggregate) fn execute_nth_field_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let field_slot = Self::resolve_orderable_field_slot(target_field)?;
        let response = self.execute(plan)?;

        Self::aggregate_nth_field_from_materialized(response, target_field, field_slot, nth)
    }

    // Execute one field-target median aggregate (`median(field)`) via
    // canonical materialized fallback semantics.
    pub(in crate::db::executor::load::aggregate) fn execute_median_field_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
    ) -> Result<Option<Id<E>>, InternalError> {
        let field_slot = Self::resolve_orderable_field_slot(target_field)?;
        let response = self.execute(plan)?;

        Self::aggregate_median_field_from_materialized(response, target_field, field_slot)
    }

    // Execute one field-target paired extrema aggregate (`min_max(field)`)
    // via canonical materialized fallback semantics.
    pub(in crate::db::executor::load::aggregate) fn execute_min_max_field_aggregate(
        &self,
        plan: ExecutablePlan<E>,
        target_field: &str,
    ) -> Result<MinMaxByIds<E>, InternalError> {
        let field_slot = Self::resolve_orderable_field_slot(target_field)?;
        let response = self.execute(plan)?;

        Self::aggregate_min_max_field_from_materialized(response, target_field, field_slot)
    }

    // Reduce one materialized response into `nth(field, n)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    fn aggregate_nth_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let ordered_rows =
            Self::ordered_field_projection_from_materialized(response, target_field, field_slot)?;

        // Phase 2: project the requested ordinal position.
        if nth >= ordered_rows.len() {
            return Ok(None);
        }

        Ok(ordered_rows.into_iter().nth(nth).map(|(id, _)| id))
    }

    // Reduce one materialized response into `median(field)` using deterministic
    // ordering `(field_value_asc, primary_key_asc)`.
    // Even-length windows select the lower median for type-agnostic stability.
    fn aggregate_median_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Option<Id<E>>, InternalError> {
        let ordered_rows =
            Self::ordered_field_projection_from_materialized(response, target_field, field_slot)?;
        if ordered_rows.is_empty() {
            return Ok(None);
        }

        let median_index = if ordered_rows.len() % 2 == 0 {
            ordered_rows.len() / 2 - 1
        } else {
            ordered_rows.len() / 2
        };

        Ok(ordered_rows.into_iter().nth(median_index).map(|(id, _)| id))
    }

    // Reduce one materialized response into `(min_by(field), max_by(field))`
    // using one pass over the response window.
    fn aggregate_min_max_field_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<MinMaxByIds<E>, InternalError> {
        let mut min_candidate: Option<(Id<E>, Value)> = None;
        let mut max_candidate: Option<(Id<E>, Value)> = None;
        for (id, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let replace_min = match min_candidate.as_ref() {
                Some((current_id, current_value)) => Self::field_projection_candidate_precedes(
                    target_field,
                    &id,
                    &value,
                    current_id,
                    current_value,
                    Ordering::Less,
                )?,
                None => true,
            };
            if replace_min {
                min_candidate = Some((id, value.clone()));
            }

            let replace_max = match max_candidate.as_ref() {
                Some((current_id, current_value)) => Self::field_projection_candidate_precedes(
                    target_field,
                    &id,
                    &value,
                    current_id,
                    current_value,
                    Ordering::Greater,
                )?,
                None => true,
            };
            if replace_max {
                max_candidate = Some((id, value));
            }
        }

        let Some((min_id, _)) = min_candidate else {
            return Ok(None);
        };
        let Some((max_id, _)) = max_candidate else {
            return Err(InternalError::query_executor_invariant(
                "min_max(field) reduction produced a min id without a max id",
            ));
        };

        Ok(Some((min_id, max_id)))
    }

    // Project one response window into deterministic field ordering
    // `(field_value_asc, primary_key_asc)`.
    fn ordered_field_projection_from_materialized(
        response: Response<E>,
        target_field: &str,
        field_slot: FieldSlot,
    ) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        let mut ordered_rows: Vec<(Id<E>, Value)> = Vec::new();
        for (id, entity) in response {
            let value = extract_orderable_field_value(&entity, target_field, field_slot)
                .map_err(AggregateFieldValueError::into_internal_error)?;
            let mut insert_index = ordered_rows.len();
            for (index, (current_id, current_value)) in ordered_rows.iter().enumerate() {
                let candidate_precedes = Self::field_projection_candidate_precedes(
                    target_field,
                    &id,
                    &value,
                    current_id,
                    current_value,
                    Ordering::Less,
                )?;
                if candidate_precedes {
                    insert_index = index;
                    break;
                }
            }

            ordered_rows.insert(insert_index, (id, value));
        }

        Ok(ordered_rows)
    }

    // Load one entity for field-extrema stream folding while preserving read
    // consistency classification behavior.
    pub(in crate::db::executor) fn read_entity_for_field_extrema(
        ctx: &Context<'_, E>,
        consistency: ReadConsistency,
        key: &crate::db::data::DataKey,
    ) -> Result<Option<E>, InternalError> {
        let decode_row = |row| {
            let mut decoded = Context::<E>::deserialize_rows(vec![(key.clone(), row)])?;
            let Some((_, entity)) = decoded.pop() else {
                return Err(InternalError::query_executor_invariant(
                    "field-extrema row decode expected one decoded entity",
                ));
            };

            Ok(entity)
        };
        match consistency {
            ReadConsistency::Strict => {
                let row = ctx.read_strict(key)?;
                Ok(Some(decode_row(row)?))
            }
            ReadConsistency::MissingOk => match ctx.read(key) {
                Ok(row) => Ok(Some(decode_row(row)?)),
                Err(err) if err.is_not_found() => Ok(None),
                Err(err) => Err(err),
            },
        }
    }

    pub(in crate::db::executor) fn field_extrema_aggregate_direction(
        kind: AggregateKind,
    ) -> Result<Direction, InternalError> {
        match kind {
            AggregateKind::Min => Ok(Direction::Asc),
            AggregateKind::Max => Ok(Direction::Desc),
            AggregateKind::Count
            | AggregateKind::Exists
            | AggregateKind::First
            | AggregateKind::Last => Err(InternalError::query_executor_invariant(
                "field-target aggregate direction requires MIN/MAX terminal",
            )),
        }
    }

    // Adapter so aggregate submodules keep one internal mapping entrypoint while
    // taxonomy mapping ownership remains centralized in aggregate field semantics.
    pub(in crate::db::executor::load::aggregate) fn map_aggregate_field_value_error(
        err: AggregateFieldValueError,
    ) -> InternalError {
        err.into_internal_error()
    }
}
