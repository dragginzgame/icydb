//! Module: db::session::sql::execute::write::authority
//! Responsibility: accepted-schema authority helpers for SQL write execution.
//! Does not own: INSERT/UPDATE/DELETE execution or candidate-row collection.
//! Boundary: keeps key decoding, field normalization, descriptor validation,
//! and save-contract projection in one accepted-schema owner.

use crate::{
    db::{
        DbSession, QueryError,
        data::{AcceptedMutationIntentPatch, FieldSlot},
        executor::EntityAuthority,
        schema::{
            AcceptedFieldKind, AcceptedRowLayoutRuntimeContract, SchemaFieldWritePolicy,
            SchemaInfo, input_value_from_strict_sql_literal_for_persisted_kind,
        },
        session::{
            AcceptedSchemaCatalogContext,
            sql::execute::write_returning::validate_sql_returning_projection_fields,
        },
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
) -> Result<AcceptedRowLayoutRuntimeContract<'a>, QueryError> {
    let descriptor = checked_accepted_write_descriptor(catalog)?;
    validate_sql_returning_projection_fields(&descriptor, returning)?;

    Ok(descriptor)
}

fn accepted_write_field_slot(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<FieldSlot, QueryError> {
    let accepted_slot = descriptor
        .field_slot_index_by_name(field_name)
        .ok_or_else(QueryError::invariant)?;

    Ok(FieldSlot::from_validated_index(accepted_slot))
}

pub(super) fn sql_write_patch_set_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: AcceptedMutationIntentPatch,
    field_name: &str,
    value: InputValue,
) -> Result<AcceptedMutationIntentPatch, QueryError> {
    let slot = accepted_write_field_slot(descriptor, field_name)?;

    Ok(patch.set_authored(slot, value))
}

pub(super) fn sql_write_patch_set_insert_default(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: AcceptedMutationIntentPatch,
    field_name: &str,
) -> Result<AcceptedMutationIntentPatch, QueryError> {
    let slot = accepted_write_field_slot(descriptor, field_name)?;

    Ok(patch.set_explicit_insert_default(slot))
}

pub(super) fn sql_write_patch_set_update_default(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    patch: AcceptedMutationIntentPatch,
    field_name: &str,
) -> Result<AcceptedMutationIntentPatch, QueryError> {
    let slot = accepted_write_field_slot(descriptor, field_name)?;

    Ok(patch.set_explicit_update_default(slot))
}

fn write_policy_for_accepted_name(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<SchemaFieldWritePolicy, QueryError> {
    let Some(field) = descriptor.field_by_name(field_name) else {
        return Err(QueryError::invariant());
    };

    Ok(field.write_policy())
}

pub(super) fn sql_write_input_for_accepted_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
    value: &Value,
) -> Result<InputValue, QueryError> {
    let accepted_field = descriptor
        .field_by_name(field_name)
        .ok_or_else(QueryError::invariant)?;
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

fn sql_write_input_for_accepted_kind(
    accepted_kind: &AcceptedFieldKind,
    value: &Value,
) -> Result<InputValue, QueryError> {
    input_value_from_strict_sql_literal_for_persisted_kind(accepted_kind, value)
        .ok_or_else(invalid_sql_write_field_literal)
}

pub(super) fn reject_explicit_sql_write_to_managed_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.write_management().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitManagedField,
        ));
    }

    Ok(())
}

pub(super) fn reject_explicit_sql_write_to_generated_field(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    field_name: &str,
) -> Result<(), QueryError> {
    let Ok(policy) = write_policy_for_accepted_name(descriptor, field_name) else {
        return Ok(());
    };

    if policy.insert_generation().is_some() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::ExplicitGeneratedField,
        ));
    }

    Ok(())
}

impl<C: CanisterKind> DbSession<C> {
    pub(super) fn accepted_sql_write_authority_schema_info(
        catalog: &AcceptedSchemaCatalogContext,
    ) -> (EntityAuthority, SchemaInfo) {
        let schema_info = catalog.accepted_schema_info();
        let authority = catalog.accepted_entity_authority();
        (authority, schema_info.clone())
    }

    pub(in crate::db::session::sql) fn with_checked_accepted_write_descriptor_for_returning<T>(
        &self,
        catalog: Option<&AcceptedSchemaCatalogContext>,
        entity_name: Option<&str>,
        returning: Option<&SqlReturningProjection>,
        run: impl for<'a> FnOnce(
            &'a AcceptedSchemaCatalogContext,
            AcceptedRowLayoutRuntimeContract<'a>,
        ) -> Result<T, QueryError>,
    ) -> Result<T, QueryError> {
        if let Some(catalog) = catalog {
            let descriptor = checked_accepted_write_descriptor_for_returning(catalog, returning)?;
            return run(catalog, descriptor);
        }

        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(entity_name)
            .map_err(QueryError::execute)?;
        let descriptor = checked_accepted_write_descriptor_for_returning(&catalog, returning)?;

        run(&catalog, descriptor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::query::intent::QueryExecutionError,
        error::{ErrorDetail, QueryErrorDetail},
    };

    const fn enum_kind() -> AcceptedFieldKind {
        AcceptedFieldKind::Enum {
            type_id: crate::value::EnumTypeId::new(1).expect("test enum type ID should be valid"),
        }
    }

    fn assert_invalid_enum_sql_literal(error: QueryError) {
        let QueryError::Execute(QueryExecutionError::Unsupported(internal)) = error else {
            panic!("expected unsupported SQL write boundary error");
        };

        assert!(matches!(
            internal.detail(),
            Some(ErrorDetail::Query(QueryErrorDetail::SqlWriteBoundary { boundary }))
                if *boundary == SqlWriteBoundaryCode::InvalidFieldLiteral
        ));
    }

    #[test]
    fn sql_enum_string_literal_remains_unresolved_until_accepted_patch_admission() {
        let input =
            sql_write_input_for_accepted_kind(&enum_kind(), &Value::Text("Active".to_string()))
                .expect("target-typed enum string should become authored input");

        assert_eq!(input, InputValue::loose_enum("Active"));
    }

    #[test]
    fn sql_enum_target_rejects_non_label_scalar_literals() {
        let err = sql_write_input_for_accepted_kind(&enum_kind(), &Value::Nat64(7))
            .expect_err("numeric literal must not author an enum label");

        assert_invalid_enum_sql_literal(err);
    }

    #[test]
    fn sql_enum_target_defers_label_validation_to_accepted_patch_admission() {
        for variant in ["Missing", "Loaded"] {
            let input =
                sql_write_input_for_accepted_kind(&enum_kind(), &Value::Text(variant.to_string()))
                    .expect("enum text should remain unresolved until catalog admission");

            assert_eq!(input, InputValue::loose_enum(variant));
        }
    }
}
