use crate::{
    db::{
        cursor::{CursorBoundary, CursorBoundarySlot},
        query::{
            plan::{CursorPlanError, OrderPlanError, OrderSpec, PlanError},
            predicate::SchemaInfo,
        },
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, FieldValue},
    value::Value,
};

///
/// PrimaryKeyCursorSlotDecodeError
///
/// Typed primary-key cursor-slot decode failures.
///
#[derive(Clone, Debug)]
struct PrimaryKeyCursorSlotDecodeError {
    mismatch_value: Option<Value>,
}

impl PrimaryKeyCursorSlotDecodeError {
    // Construct a missing-slot decode failure.
    #[must_use]
    const fn missing() -> Self {
        Self {
            mismatch_value: None,
        }
    }

    // Construct a type-mismatch decode failure.
    #[must_use]
    const fn type_mismatch(value: Value) -> Self {
        Self {
            mismatch_value: Some(value),
        }
    }

    // Return the optional offending value for cursor error mapping.
    #[must_use]
    fn into_mismatch_value(self) -> Option<Value> {
        self.mismatch_value
    }
}

// Decode one primary-key cursor slot into a typed key value.
fn decode_primary_key_cursor_slot<K: FieldValue>(
    slot: &CursorBoundarySlot,
) -> Result<K, PrimaryKeyCursorSlotDecodeError> {
    match slot {
        CursorBoundarySlot::Missing => Err(PrimaryKeyCursorSlotDecodeError::missing()),
        CursorBoundarySlot::Present(value) => K::from_value(value)
            .ok_or_else(|| PrimaryKeyCursorSlotDecodeError::type_mismatch(value.clone())),
    }
}

/// Decode the primary-key slot from a validated cursor boundary using typed key semantics.
pub(in crate::db) fn decode_typed_primary_key_cursor_slot<K: FieldValue>(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<K, PlanError> {
    let pk_field = model.primary_key.name;
    let pk_index = order
        .fields
        .iter()
        .position(|(field, _)| field == pk_field)
        .ok_or_else(|| {
            PlanError::from(OrderPlanError::MissingPrimaryKeyTieBreak {
                field: pk_field.to_string(),
            })
        })?;

    let schema = SchemaInfo::from_entity_model(model).map_err(PlanError::from)?;
    let expected = schema
        .field(pk_field)
        .expect("primary key exists by model contract")
        .to_string();
    let pk_slot = &boundary.slots[pk_index];

    decode_primary_key_cursor_slot::<K>(pk_slot).map_err(|err| {
        PlanError::from(CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
            field: pk_field.to_string(),
            expected,
            value: err.into_mismatch_value(),
        })
    })
}

/// Decode a typed primary-key cursor boundary for PK-ordered executor paths.
pub(in crate::db) fn decode_pk_cursor_boundary<E>(
    boundary: Option<&CursorBoundary>,
) -> Result<Option<E::Key>, InternalError>
where
    E: EntityKind,
{
    let Some(boundary) = boundary else {
        return Ok(None);
    };

    debug_assert_eq!(
        boundary.slots.len(),
        1,
        "pk-ordered continuation boundaries are validated by the cursor spine",
    );
    let slot = boundary
        .slots
        .first()
        .unwrap_or(&CursorBoundarySlot::Missing);

    decode_primary_key_cursor_slot::<E::Key>(slot)
        .map(Some)
        .map_err(|err| {
            if err.into_mismatch_value().is_none() {
                InternalError::query_executor_invariant("pk cursor slot must be present")
            } else {
                InternalError::query_executor_invariant("pk cursor slot type mismatch")
            }
        })
}
