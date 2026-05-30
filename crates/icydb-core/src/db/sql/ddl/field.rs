use super::{
    BoundSqlDdlNoOpRequest, BoundSqlDdlRequest, BoundSqlDdlStatement, SqlDdlBindError,
    SqlDdlMutationKind,
};
use crate::db::{
    data::encode_runtime_value_for_accepted_field_contract,
    schema::{
        AcceptedFieldDecodeContract, AcceptedSchemaSnapshot, FieldId, PersistedFieldKind,
        PersistedFieldOrigin, PersistedFieldSnapshot, SchemaFieldDefault, SchemaFieldSlot,
        SchemaFieldWritePolicy, SchemaInfo, canonicalize_strict_sql_literal_for_persisted_kind,
        resolve_sql_ddl_field_drop_dependent_index,
    },
    sql::{
        identifier::identifiers_tail_match,
        parser::{
            SqlAlterColumnAction, SqlAlterTableAddColumnStatement,
            SqlAlterTableAlterColumnStatement, SqlAlterTableDropColumnStatement,
            SqlAlterTableRenameColumnStatement,
        },
    },
};
use crate::model::field::{DEFAULT_BIG_INT_MAX_BYTES, FieldStorageDecode, LeafCodec, ScalarCodec};

///
/// BoundSqlAddColumnRequest
///
/// Catalog-resolved additive field DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlAddColumnRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
}

impl BoundSqlAddColumnRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted DDL-owned field snapshot to publish.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }
}

///
/// BoundSqlAlterColumnDefaultRequest
///
/// Catalog-resolved field-default metadata DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlAlterColumnDefaultRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
    default: SchemaFieldDefault,
    mutation_kind: SqlDdlMutationKind,
}

impl BoundSqlAlterColumnDefaultRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field.name()
    }

    /// Borrow the accepted field whose default will change.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }

    /// Borrow the default contract to publish.
    #[must_use]
    pub(in crate::db) const fn default(&self) -> &SchemaFieldDefault {
        &self.default
    }

    /// Return the field-default mutation kind.
    #[must_use]
    pub(in crate::db) const fn mutation_kind(&self) -> SqlDdlMutationKind {
        self.mutation_kind
    }
}

///
/// BoundSqlAlterColumnNullabilityRequest
///
/// Catalog-resolved field-nullability metadata DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlAlterColumnNullabilityRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
    nullable: bool,
    mutation_kind: SqlDdlMutationKind,
}

impl BoundSqlAlterColumnNullabilityRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field.name()
    }

    /// Borrow the accepted field whose nullability will change.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }

    /// Return the nullable contract to publish.
    #[must_use]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }

    /// Return the field-nullability mutation kind.
    #[must_use]
    pub(in crate::db) const fn mutation_kind(&self) -> SqlDdlMutationKind {
        self.mutation_kind
    }
}

///
/// BoundSqlDropColumnRequest
///
/// Catalog-resolved retained-slot field removal DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDropColumnRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
}

impl BoundSqlDropColumnRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn field_name(&self) -> &str {
        self.field.name()
    }

    /// Borrow the accepted DDL-owned field that will be retired.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }
}

///
/// BoundSqlRenameColumnRequest
///
/// Catalog-resolved field-rename metadata DDL request.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlRenameColumnRequest {
    entity_name: String,
    field: PersistedFieldSnapshot,
    new_name: String,
}

impl BoundSqlRenameColumnRequest {
    /// Borrow the accepted entity name.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted source field name.
    #[must_use]
    pub(in crate::db) const fn old_name(&self) -> &str {
        self.field.name()
    }

    /// Borrow the accepted target field name.
    #[must_use]
    pub(in crate::db) const fn new_name(&self) -> &str {
        self.new_name.as_str()
    }

    /// Borrow the accepted source field.
    #[must_use]
    pub(in crate::db) const fn field(&self) -> &PersistedFieldSnapshot {
        &self.field
    }
}

pub(super) fn bind_alter_table_add_column_statement(
    statement: &SqlAlterTableAddColumnStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    if schema
        .field_nullable(statement.column_name.as_str())
        .is_some()
    {
        return Err(SqlDdlBindError::DuplicateColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }

    let (kind, storage_decode, leaf_codec) = persisted_field_contract_for_sql_column_type(
        statement.column_type.as_str(),
    )
    .ok_or_else(|| SqlDdlBindError::UnsupportedAlterTableAddColumnType {
        entity_name: entity_name.to_string(),
        column_name: statement.column_name.clone(),
        column_type: statement.column_type.clone(),
    })?;
    let default = schema_field_default_for_sql_default(
        entity_name,
        statement.column_name.as_str(),
        statement.default.as_ref(),
        &kind,
        statement.nullable,
        storage_decode,
        leaf_codec,
    )?;
    if !statement.nullable && default.is_none() {
        return Err(SqlDdlBindError::UnsupportedAlterTableAddColumnNotNull {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }
    let field = PersistedFieldSnapshot::new_with_write_policy_and_origin(
        next_sql_ddl_field_id(accepted_before),
        statement.column_name.clone(),
        next_sql_ddl_field_slot(accepted_before),
        kind,
        Vec::new(),
        statement.nullable,
        default,
        SchemaFieldWritePolicy::from_model_policies(None, None),
        PersistedFieldOrigin::SqlDdl,
        storage_decode,
        leaf_codec,
    );

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::AddColumn(BoundSqlAddColumnRequest {
            entity_name: entity_name.to_string(),
            field,
        }),
    })
}

pub(super) fn bind_alter_table_alter_column_statement(
    statement: &SqlAlterTableAlterColumnStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let Some(entity_name) = schema.entity_name() else {
        return Err(SqlDdlBindError::MissingEntityName);
    };

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    let field = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.name() == statement.column_name)
        .ok_or_else(|| SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        })?;

    match &statement.action {
        SqlAlterColumnAction::SetDefault(default) => {
            reject_generated_field_default_change(entity_name, field)?;
            let default =
                schema_field_default_for_alter_column_default(entity_name, field, default)?;
            Ok(bind_alter_table_alter_column_default(
                entity_name,
                field,
                default,
                SqlDdlMutationKind::SetFieldDefault,
            ))
        }
        SqlAlterColumnAction::DropDefault => {
            if !field.default().is_none() {
                reject_generated_field_default_change(entity_name, field)?;
            }
            if !field.nullable() && !field.default().is_none() {
                return Err(SqlDdlBindError::UnsupportedAlterTableDropDefaultRequired {
                    entity_name: entity_name.to_string(),
                    column_name: statement.column_name.clone(),
                });
            }
            Ok(bind_alter_table_alter_column_default(
                entity_name,
                field,
                SchemaFieldDefault::None,
                SqlDdlMutationKind::DropFieldDefault,
            ))
        }
        SqlAlterColumnAction::SetNotNull => bind_alter_table_alter_column_nullability(
            entity_name,
            field,
            false,
            SqlDdlMutationKind::SetFieldNotNull,
        ),
        SqlAlterColumnAction::DropNotNull => bind_alter_table_alter_column_nullability(
            entity_name,
            field,
            true,
            SqlDdlMutationKind::DropFieldNotNull,
        ),
    }
}

pub(super) fn bind_alter_table_drop_column_statement(
    statement: &SqlAlterTableDropColumnStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    let accepted = accepted_before.persisted_snapshot();
    let Some(field) = accepted
        .fields()
        .iter()
        .find(|field| field.name() == statement.column_name)
    else {
        if statement.if_exists {
            return Ok(BoundSqlDdlRequest {
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::DropField,
                    index_name: statement.column_name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: "-".to_string(),
                    field_path: vec![statement.column_name.clone()],
                }),
            });
        }

        return Err(SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    };

    if accepted.primary_key_field_ids().contains(&field.id()) {
        return Err(SqlDdlBindError::PrimaryKeyFieldDropRejected {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }

    if field.generated() {
        return Err(SqlDdlBindError::GeneratedFieldDropRejected {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
        });
    }

    if let Some(index_name) =
        resolve_sql_ddl_field_drop_dependent_index(accepted_before, field.id())
    {
        return Err(SqlDdlBindError::IndexedFieldDropRejected {
            entity_name: entity_name.to_string(),
            column_name: statement.column_name.clone(),
            index_name,
        });
    }

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::DropColumn(BoundSqlDropColumnRequest {
            entity_name: entity_name.to_string(),
            field: field.clone(),
        }),
    })
}

pub(super) fn bind_alter_table_rename_column_statement(
    statement: &SqlAlterTableRenameColumnStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if !identifiers_tail_match(statement.entity.as_str(), entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: statement.entity.clone(),
            expected_entity: entity_name.to_string(),
        });
    }

    let accepted = accepted_before.persisted_snapshot();
    let Some(field) = accepted
        .fields()
        .iter()
        .find(|field| field.name() == statement.old_column_name)
    else {
        return Err(SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.old_column_name.clone(),
        });
    };

    if statement.old_column_name == statement.new_column_name {
        return Ok(BoundSqlDdlRequest {
            statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                mutation_kind: SqlDdlMutationKind::RenameField,
                index_name: statement.old_column_name.clone(),
                entity_name: entity_name.to_string(),
                target_store: "-".to_string(),
                field_path: vec![statement.old_column_name.clone()],
            }),
        });
    }

    if accepted
        .fields()
        .iter()
        .any(|field| field.name() == statement.new_column_name)
    {
        return Err(SqlDdlBindError::DuplicateColumn {
            entity_name: entity_name.to_string(),
            column_name: statement.new_column_name.clone(),
        });
    }

    if field.generated() {
        return Err(SqlDdlBindError::GeneratedFieldRenameRejected {
            entity_name: entity_name.to_string(),
            column_name: statement.old_column_name.clone(),
        });
    }

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::RenameColumn(BoundSqlRenameColumnRequest {
            entity_name: entity_name.to_string(),
            field: field.clone(),
            new_name: statement.new_column_name.clone(),
        }),
    })
}

fn bind_alter_table_alter_column_default(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    default: SchemaFieldDefault,
    mutation_kind: SqlDdlMutationKind,
) -> BoundSqlDdlRequest {
    if field.default() == &default {
        return BoundSqlDdlRequest {
            statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                mutation_kind,
                index_name: field.name().to_string(),
                entity_name: entity_name.to_string(),
                target_store: entity_name.to_string(),
                field_path: vec![field.name().to_string()],
            }),
        };
    }

    BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::AlterColumnDefault(BoundSqlAlterColumnDefaultRequest {
            entity_name: entity_name.to_string(),
            field: field.clone(),
            default,
            mutation_kind,
        }),
    }
}

fn reject_generated_field_default_change(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
) -> Result<(), SqlDdlBindError> {
    if field.generated() {
        return Err(SqlDdlBindError::GeneratedFieldDefaultChangeRejected {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
        });
    }

    Ok(())
}

fn bind_alter_table_alter_column_nullability(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    nullable: bool,
    mutation_kind: SqlDdlMutationKind,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    if field.nullable() == nullable {
        return Ok(BoundSqlDdlRequest {
            statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                mutation_kind,
                index_name: field.name().to_string(),
                entity_name: entity_name.to_string(),
                target_store: entity_name.to_string(),
                field_path: vec![field.name().to_string()],
            }),
        });
    }

    reject_generated_field_nullability_change(entity_name, field)?;

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::AlterColumnNullability(
            BoundSqlAlterColumnNullabilityRequest {
                entity_name: entity_name.to_string(),
                field: field.clone(),
                nullable,
                mutation_kind,
            },
        ),
    })
}

fn reject_generated_field_nullability_change(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
) -> Result<(), SqlDdlBindError> {
    if field.generated() {
        return Err(SqlDdlBindError::GeneratedFieldNullabilityChangeRejected {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
        });
    }

    Ok(())
}

fn schema_field_default_for_sql_default(
    entity_name: &str,
    column_name: &str,
    default: Option<&crate::value::Value>,
    kind: &PersistedFieldKind,
    nullable: bool,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
) -> Result<SchemaFieldDefault, SqlDdlBindError> {
    let Some(default) = default else {
        return Ok(SchemaFieldDefault::None);
    };
    if matches!(default, crate::value::Value::Null) {
        return Err(SqlDdlBindError::InvalidAlterTableAddColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: column_name.to_string(),
            detail: "NULL cannot be used as an accepted database default".to_string(),
        });
    }

    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(kind, default)
        .unwrap_or_else(|| default.clone());
    let contract =
        AcceptedFieldDecodeContract::new(column_name, kind, nullable, storage_decode, leaf_codec);
    let payload = encode_runtime_value_for_accepted_field_contract(contract, &normalized).map_err(
        |error| SqlDdlBindError::InvalidAlterTableAddColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: column_name.to_string(),
            detail: error.to_string(),
        },
    )?;

    Ok(SchemaFieldDefault::SlotPayload(payload))
}

fn schema_field_default_for_alter_column_default(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    default: &crate::value::Value,
) -> Result<SchemaFieldDefault, SqlDdlBindError> {
    if matches!(default, crate::value::Value::Null) {
        return Err(SqlDdlBindError::InvalidAlterTableAlterColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
            detail: "NULL cannot be used as an accepted database default".to_string(),
        });
    }

    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(field.kind(), default)
        .unwrap_or_else(|| default.clone());
    let contract = AcceptedFieldDecodeContract::new(
        field.name(),
        field.kind(),
        field.nullable(),
        field.storage_decode(),
        field.leaf_codec(),
    );
    let payload = encode_runtime_value_for_accepted_field_contract(contract, &normalized).map_err(
        |error| SqlDdlBindError::InvalidAlterTableAlterColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
            detail: error.to_string(),
        },
    )?;

    Ok(SchemaFieldDefault::SlotPayload(payload))
}

fn next_sql_ddl_field_id(accepted_before: &AcceptedSchemaSnapshot) -> FieldId {
    let snapshot = accepted_before.persisted_snapshot();
    let next = snapshot
        .fields()
        .iter()
        .map(|field| field.id().get())
        .chain(
            snapshot
                .row_layout()
                .retired_field_slots()
                .iter()
                .map(|(field_id, _)| field_id.get()),
        )
        .max()
        .unwrap_or(0)
        .checked_add(1)
        .expect("accepted field IDs should not be exhausted");

    FieldId::new(next)
}

fn next_sql_ddl_field_slot(accepted_before: &AcceptedSchemaSnapshot) -> SchemaFieldSlot {
    accepted_before
        .persisted_snapshot()
        .row_layout()
        .next_unallocated_slot()
}

fn persisted_field_contract_for_sql_column_type(
    column_type: &str,
) -> Option<(PersistedFieldKind, FieldStorageDecode, LeafCodec)> {
    let normalized = column_type.trim().to_ascii_lowercase();
    match normalized.as_str() {
        "bool" | "boolean" => Some((
            PersistedFieldKind::Bool,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Bool),
        )),
        "int8" => Some((
            PersistedFieldKind::Int8,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int16" => Some((
            PersistedFieldKind::Int16,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int32" => Some((
            PersistedFieldKind::Int32,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int64" => Some((
            PersistedFieldKind::Int64,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Int64),
        )),
        "int128" => Some((
            PersistedFieldKind::Int128,
            FieldStorageDecode::ByKind,
            LeafCodec::StructuralFallback,
        )),
        "nat8" => Some((
            PersistedFieldKind::Nat8,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat16" => Some((
            PersistedFieldKind::Nat16,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat32" => Some((
            PersistedFieldKind::Nat32,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat64" => Some((
            PersistedFieldKind::Nat64,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )),
        "nat128" => Some((
            PersistedFieldKind::Nat128,
            FieldStorageDecode::ByKind,
            LeafCodec::StructuralFallback,
        )),
        "text" | "string" => Some((
            PersistedFieldKind::Text { max_len: None },
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Text),
        )),
        _ => persisted_big_int_contract_for_sql_column_type(&normalized),
    }
}

fn persisted_big_int_contract_for_sql_column_type(
    normalized: &str,
) -> Option<(PersistedFieldKind, FieldStorageDecode, LeafCodec)> {
    if let Some(max_bytes) = sql_big_int_type_max_bytes(normalized, "int_big") {
        return Some((
            PersistedFieldKind::IntBig { max_bytes },
            FieldStorageDecode::ByKind,
            LeafCodec::StructuralFallback,
        ));
    }

    sql_big_int_type_max_bytes(normalized, "nat_big").map(|max_bytes| {
        (
            PersistedFieldKind::NatBig { max_bytes },
            FieldStorageDecode::ByKind,
            LeafCodec::StructuralFallback,
        )
    })
}

fn sql_big_int_type_max_bytes(normalized: &str, type_name: &str) -> Option<u32> {
    if normalized == type_name {
        return Some(DEFAULT_BIG_INT_MAX_BYTES);
    }

    let inner = normalized
        .strip_prefix(type_name)?
        .strip_prefix("(max_bytes=")?
        .strip_suffix(')')?;
    let max_bytes = inner.parse::<u32>().ok()?;
    if max_bytes == 0 {
        return None;
    }

    Some(max_bytes)
}
