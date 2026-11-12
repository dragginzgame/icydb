//! Module: db::session::sql::execute::write::authority
//! Responsibility: accepted-schema authority helpers for SQL write execution.
//! Does not own: INSERT/UPDATE/DELETE execution or candidate-row collection.
//! Boundary: keeps key decoding, field normalization, descriptor validation,
//! and save-contract projection in one accepted-schema owner.

use crate::{
    db::{
        DbSession, QueryError,
        data::{AcceptedMutationIntentPatch, FieldSlot},
        executor::AcceptedSchemaCatalogContext,
        schema::{
            AcceptedFieldKind, AcceptedRowLayoutRuntimeContract, AcceptedRowLayoutRuntimeField,
            ValidateError, input_value_from_sql_assignment_result_for_persisted_kind,
            input_value_from_strict_sql_literal_for_persisted_kind,
        },
        session::sql::execute::write_returning::SqlReturningProjectionPlan,
        sql::parser::SqlReturningProjection,
    },
    traits::CanisterKind,
    value::{InputValue, Value},
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

fn checked_accepted_write_descriptor(
    catalog: &AcceptedSchemaCatalogContext,
) -> Result<AcceptedRowLayoutRuntimeContract<'_>, QueryError> {
    AcceptedRowLayoutRuntimeContract::from_accepted_schema(catalog.snapshot())
        .map_err(QueryError::execute)
}

fn checked_accepted_write_descriptor_for_returning<'a>(
    catalog: &'a AcceptedSchemaCatalogContext,
    returning: Option<&SqlReturningProjection>,
) -> Result<
    (
        AcceptedRowLayoutRuntimeContract<'a>,
        Option<SqlReturningProjectionPlan>,
    ),
    QueryError,
> {
    let descriptor = checked_accepted_write_descriptor(catalog)?;
    let projection = returning
        .map(|returning| SqlReturningProjectionPlan::from_descriptor(&descriptor, returning))
        .transpose()?;
    Ok((descriptor, projection))
}

// Unknown authored names are caller validation failures on every write route,
// including DEFAULT and resumable preparation; they are never engine damage.
fn unknown_sql_write_field(field_name: &str) -> QueryError {
    QueryError::validate(ValidateError::UnknownField {
        field: field_name.to_string(),
    })
}

// Resolve once, then retain the accepted field through policy, conversion and
// patch construction. The borrowed field cannot drift to another descriptor.
pub(super) fn resolve_sql_write_field<'a, 'schema>(
    descriptor: &'a AcceptedRowLayoutRuntimeContract<'schema>,
    field_name: &str,
) -> Result<&'a AcceptedRowLayoutRuntimeField<'schema>, QueryError> {
    descriptor
        .field_by_name(field_name)
        .ok_or_else(|| unknown_sql_write_field(field_name))
}

pub(super) fn accepted_write_field_slot(field: &AcceptedRowLayoutRuntimeField<'_>) -> FieldSlot {
    FieldSlot::from_validated_index(usize::from(field.slot().get()))
}

pub(super) fn sql_write_patch_set_insert_default(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: AcceptedMutationIntentPatch,
    field_name: &str,
) -> Result<AcceptedMutationIntentPatch, QueryError> {
    let slot = accepted_write_field_slot(resolve_sql_write_field(descriptor, field_name)?);

    Ok(patch.set_explicit_insert_default(slot))
}

pub(super) fn sql_write_input_for_accepted_field(
    accepted_field: &AcceptedRowLayoutRuntimeField<'_>,
    value: &Value,
) -> Result<InputValue, QueryError> {
    if matches!(value, Value::Null) {
        return accepted_field
            .decode_contract()
            .nullable()
            .then_some(InputValue::null())
            .ok_or_else(invalid_sql_write_field_literal);
    }

    sql_write_input_for_accepted_kind(accepted_field.kind(), value)
}

fn invalid_sql_write_field_literal() -> QueryError {
    QueryError::sql_write_boundary(SqlWriteBoundaryCode::InvalidFieldLiteral)
}

pub(super) fn sql_write_input_for_assignment_result(
    accepted_field: &AcceptedRowLayoutRuntimeField<'_>,
    value: &Value,
) -> Result<InputValue, QueryError> {
    if matches!(value, Value::Null) {
        return sql_write_input_for_accepted_field(accepted_field, value);
    }
    input_value_from_sql_assignment_result_for_persisted_kind(accepted_field.kind(), value)
        .ok_or_else(invalid_sql_write_field_literal)
}

fn sql_write_input_for_accepted_kind(
    accepted_kind: &AcceptedFieldKind,
    value: &Value,
) -> Result<InputValue, QueryError> {
    input_value_from_strict_sql_literal_for_persisted_kind(accepted_kind, value)
        .ok_or_else(invalid_sql_write_field_literal)
}

pub(super) fn reject_explicit_sql_write_to_managed_field(
    field: &AcceptedRowLayoutRuntimeField<'_>,
) -> Result<(), QueryError> {
    let policy = field.write_policy();

    if policy.write_management().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitManagedField,
        ));
    }

    Ok(())
}

pub(super) fn reject_explicit_sql_write_to_generated_field(
    field: &AcceptedRowLayoutRuntimeField<'_>,
) -> Result<(), QueryError> {
    let policy = field.write_policy();

    if policy.insert_generation().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitGeneratedField,
        ));
    }

    Ok(())
}

impl<C: CanisterKind> DbSession<C> {
    pub(in crate::db::session::sql) fn with_checked_accepted_write_descriptor_for_returning<T>(
        &self,
        catalog: Option<&AcceptedSchemaCatalogContext>,
        entity_name: Option<&str>,
        returning: Option<&SqlReturningProjection>,
        run: impl for<'a> FnOnce(
            &'a AcceptedSchemaCatalogContext,
            AcceptedRowLayoutRuntimeContract<'a>,
            Option<SqlReturningProjectionPlan>,
        ) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        if let Some(catalog) = catalog {
            let (descriptor, projection) =
                checked_accepted_write_descriptor_for_returning(catalog, returning)?;
            return run(catalog, descriptor, projection);
        }

        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(entity_name)
            .map_err(QueryError::execute)?;
        let (descriptor, projection) =
            checked_accepted_write_descriptor_for_returning(&catalog, returning)?;

        run(&catalog, descriptor, projection)
    }
}

#[cfg(test)]
mod tests;
