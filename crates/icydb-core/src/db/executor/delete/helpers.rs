use crate::{
    db::{
        data::{DataKey, DataRow, RawRow, decode_and_validate_entity_key},
        executor::ExecutorError,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};

///
/// DeleteRow
/// Row wrapper used during delete planning and execution.
///

pub(super) struct DeleteRow<E>
where
    E: EntityKind,
{
    pub(super) key: DataKey,
    pub(super) raw: Option<RawRow>,
    pub(super) entity: E,
}

impl<E: EntityKind> crate::db::query::plan::logical::PlanRow<E> for DeleteRow<E> {
    fn entity(&self) -> &E {
        &self.entity
    }
}

/// Decode raw access rows into typed delete rows with key/entity checks.
pub(super) fn decode_rows<E: EntityKind + EntityValue>(
    rows: Vec<DataRow>,
) -> Result<Vec<DeleteRow<E>>, InternalError> {
    rows.into_iter()
        .map(|(dk, raw)| {
            let expected = dk.try_key::<E>()?;
            let entity = decode_and_validate_entity_key::<E, _, _, _, _>(
                expected,
                || raw.try_decode::<E>(),
                |err| {
                    ExecutorError::serialize_corruption(format!(
                        "failed to deserialize row: {dk} ({err})"
                    ))
                    .into()
                },
                |expected, actual| {
                    ExecutorError::store_corruption(format!(
                        "row key mismatch: expected {expected:?}, found {actual:?}"
                    ))
                    .into()
                },
            )?;

            Ok(DeleteRow {
                key: dk,
                raw: Some(raw),
                entity,
            })
        })
        .collect()
}
