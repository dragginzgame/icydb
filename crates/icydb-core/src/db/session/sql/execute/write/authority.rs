//! Module: db::session::sql::execute::write::authority
//! Responsibility: accepted-schema authority helpers for SQL write execution.
//! Does not own: INSERT/UPDATE/DELETE execution or candidate-row collection.
//! Boundary: keeps key decoding, field normalization, descriptor validation,
//! and save-contract projection in one accepted-schema owner.

use crate::{
    db::{
        DbSession, KeyValueCodec, PersistedRow, QueryError,
        data::{AuthoredStructuralPatch, FieldSlot},
        executor::EntityAuthority,
        schema::{
            AcceptedFieldKind, AcceptedRowLayoutRuntimeContract, AcceptedSchemaSnapshot,
            SchemaFieldWritePolicy, SchemaInfo, canonicalize_strict_sql_literal_for_persisted_kind,
            input_value_from_strict_sql_literal_for_persisted_kind,
        },
        session::{
            AcceptedSchemaCatalogContext,
            accepted_schema::{AcceptedSaveContract, accepted_save_contract_for_catalog_context},
            sql::execute::write_returning::validate_sql_returning_projection_fields,
        },
        sql::parser::SqlReturningProjection,
    },
    entity::EntityKind,
    traits::CanisterKind,
    value::{InputValue, Value},
};
use icydb_diagnostic_code::SqlWriteBoundaryCode;

pub(super) fn sql_write_key_from_literal<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    value: &Value,
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    if descriptor.primary_key_names().len() > 1 {
        let Value::List(values) = value else {
            return Err(QueryError::sql_write_boundary(
                SqlWriteBoundaryCode::PrimaryKeyLiteralShape,
            ));
        };

        return sql_write_key_from_component_literals::<E>(descriptor, values.as_slice());
    }

    if let Some(key) = <E::Key as KeyValueCodec>::from_key_value(value) {
        return Ok(key);
    }

    let primary_key_kind = descriptor.first_primary_key_kind();
    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(primary_key_kind, value)
        .unwrap_or_else(|| value.clone());

    <E::Key as KeyValueCodec>::from_key_value(&normalized).ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible)
    })
}

pub(super) fn sql_write_key_from_component_literals<E>(
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
    values: &[Value],
) -> Result<E::Key, QueryError>
where
    E: EntityKind,
{
    let primary_key_names = descriptor.primary_key_names();
    let primary_key_kinds = descriptor.primary_key_kinds();
    if values.len() != primary_key_names.len() {
        return Err(QueryError::sql_write_boundary(
            SqlWriteBoundaryCode::PrimaryKeyLiteralShape,
        ));
    }

    let mut normalized = Vec::with_capacity(values.len());
    for ((_field_name, kind), value) in primary_key_names
        .iter()
        .zip(primary_key_kinds.iter())
        .zip(values.iter())
    {
        let value = canonicalize_strict_sql_literal_for_persisted_kind(kind, value)
            .unwrap_or_else(|| value.clone());

        normalized.push(value);
    }

    let key_value = if normalized.len() == 1 {
        normalized
            .into_iter()
            .next()
            .ok_or_else(QueryError::invariant)?
    } else {
        Value::List(normalized)
    };

    <E::Key as KeyValueCodec>::from_key_value(&key_value).ok_or_else(|| {
        QueryError::sql_write_boundary(SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible)
    })
}

fn checked_accepted_write_descriptor<E>(
    schema: &AcceptedSchemaSnapshot,
) -> Result<AcceptedRowLayoutRuntimeContract<'_>, QueryError>
where
    E: EntityKind,
{
    let (descriptor, _) =
        AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(schema, E::MODEL)
            .map_err(QueryError::execute)?;

    Ok(descriptor)
}

fn checked_accepted_write_descriptor_for_returning<'a, E>(
    schema: &'a AcceptedSchemaSnapshot,
    returning: Option<&SqlReturningProjection>,
) -> Result<AcceptedRowLayoutRuntimeContract<'a>, QueryError>
where
    E: EntityKind,
{
    let descriptor = checked_accepted_write_descriptor::<E>(schema)?;
    validate_sql_returning_projection_fields(&descriptor, returning)?;

    Ok(descriptor)
}

pub(super) fn require_sql_write_policy_plan<T>(plan: Option<T>) -> Result<T, QueryError> {
    plan.ok_or_else(QueryError::unsupported_query)
}

pub(super) fn accepted_sql_write_save_contract<E>(
    catalog: &AcceptedSchemaCatalogContext,
    descriptor: &AcceptedRowLayoutRuntimeContract<'_>,
) -> AcceptedSaveContract
where
    E: EntityKind,
{
    let contract = accepted_save_contract_for_catalog_context::<E>(catalog, descriptor);
    debug_assert_eq!(contract.0.accepted_schema_revision(), catalog.revision());
    debug_assert!(std::ptr::eq(
        contract.0.enum_catalog(),
        catalog.enum_catalog(),
    ));

    contract
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
    patch: AuthoredStructuralPatch,
    field_name: &str,
    value: InputValue,
) -> Result<AuthoredStructuralPatch, QueryError> {
    let slot = accepted_write_field_slot(descriptor, field_name)?;

    Ok(patch.set(slot, value))
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
    let accepted_kind = descriptor
        .field_kind_by_name(field_name)
        .ok_or_else(QueryError::invariant)?;

    sql_write_input_for_accepted_kind(accepted_kind, value)
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
    pub(super) fn accepted_sql_write_authority_schema_info<E>(
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<(EntityAuthority, SchemaInfo), QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        catalog
            .accepted_entity_authority_and_schema_info_for::<E>()
            .map_err(QueryError::execute)
    }

    pub(super) fn with_checked_accepted_write_descriptor_for_returning<E, T>(
        &self,
        catalog: Option<&AcceptedSchemaCatalogContext>,
        returning: Option<&SqlReturningProjection>,
        run: impl for<'a> FnOnce(
            &'a AcceptedSchemaCatalogContext,
            AcceptedRowLayoutRuntimeContract<'a>,
        ) -> Result<T, QueryError>,
    ) -> Result<T, QueryError>
    where
        E: PersistedRow<Canister = C>,
    {
        if let Some(catalog) = catalog {
            let descriptor = checked_accepted_write_descriptor_for_returning::<E>(
                catalog.snapshot(),
                returning,
            )?;
            return run(catalog, descriptor);
        }

        let catalog = self
            .accepted_schema_catalog_context_for_query::<E>()
            .map_err(QueryError::execute)?;
        let descriptor =
            checked_accepted_write_descriptor_for_returning::<E>(catalog.snapshot(), returning)?;

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

        assert_eq!(
            input,
            InputValue::Enum(crate::value::InputValueEnum::loose("Active"))
        );
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

            assert_eq!(
                input,
                InputValue::Enum(crate::value::InputValueEnum::loose(variant))
            );
        }
    }
}
