use crate::{
    db::{
        executor::ExecutorError,
        store::{DataKey, DataRow, RawRow},
    },
    error::{ErrorOrigin, InternalError},
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
            let dk_for_err = dk.clone();
            let entity = raw.try_decode::<E>().map_err(|err| {
                ExecutorError::corruption(
                    ErrorOrigin::Serialize,
                    format!("failed to deserialize row: {dk_for_err} ({err})"),
                )
            })?;

            let expected = dk.try_key::<E>()?;
            let actual = entity.id().key();
            if expected != actual {
                return Err(ExecutorError::corruption(
                    ErrorOrigin::Store,
                    format!("row key mismatch: expected {expected:?}, found {actual:?}"),
                )
                .into());
            }

            Ok(DeleteRow {
                key: dk,
                raw: Some(raw),
                entity,
            })
        })
        .collect()
}
