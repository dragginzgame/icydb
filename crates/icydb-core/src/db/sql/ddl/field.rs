use super::{
    BoundSqlDdlNoOpRequest, BoundSqlDdlRequest, BoundSqlDdlSchemaVersionContract,
    BoundSqlDdlStatement, SqlDdlBindError, SqlDdlMutationKind,
};
use crate::db::{
    schema::{
        AcceptedSchemaSnapshot, PersistedFieldKind, PersistedFieldSnapshot,
        SchemaDdlFieldDefaultCandidateError, SchemaDdlFieldDropCandidateError,
        SchemaDdlFieldNullabilityCandidateError, SchemaDdlFieldRenameCandidateError,
        SchemaFieldDefault, SchemaInfo, build_sql_ddl_field_addition_candidate,
        encode_sql_ddl_add_column_default, encode_sql_ddl_alter_column_default,
        resolve_sql_ddl_field_drop_candidate, resolve_sql_ddl_field_drop_default_candidate,
        resolve_sql_ddl_field_nullability_candidate, resolve_sql_ddl_field_rename_candidate,
        resolve_sql_ddl_field_set_default_candidate,
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
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
    #[cfg(test)]
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
    let field = build_sql_ddl_field_addition_candidate(
        accepted_before,
        statement.column_name.clone(),
        kind,
        statement.nullable,
        default,
        storage_decode,
        leaf_codec,
    );

    Ok(BoundSqlDdlRequest {
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
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

    match &statement.action {
        SqlAlterColumnAction::SetDefault(default) => {
            let field = resolve_sql_ddl_field_set_default_candidate(
                accepted_before,
                statement.column_name.as_str(),
            )
            .map_err(|error| {
                sql_field_default_candidate_error(
                    entity_name,
                    statement.column_name.as_str(),
                    error,
                )
            })?;
            let default =
                schema_field_default_for_alter_column_default(entity_name, &field, default)?;
            Ok(bind_alter_table_alter_column_default(
                entity_name,
                &field,
                default,
                SqlDdlMutationKind::SetFieldDefault,
            ))
        }
        SqlAlterColumnAction::DropDefault => {
            let field = resolve_sql_ddl_field_drop_default_candidate(
                accepted_before,
                statement.column_name.as_str(),
            )
            .map_err(|error| {
                sql_field_default_candidate_error(
                    entity_name,
                    statement.column_name.as_str(),
                    error,
                )
            })?;
            Ok(bind_alter_table_alter_column_default(
                entity_name,
                &field,
                SchemaFieldDefault::None,
                SqlDdlMutationKind::DropFieldDefault,
            ))
        }
        SqlAlterColumnAction::SetNotNull => {
            let field = resolve_sql_ddl_field_nullability_candidate(
                accepted_before,
                statement.column_name.as_str(),
                false,
            )
            .map_err(|error| {
                sql_field_nullability_candidate_error(
                    entity_name,
                    statement.column_name.as_str(),
                    error,
                )
            })?;
            Ok(bind_alter_table_alter_column_nullability(
                entity_name,
                &field,
                false,
                SqlDdlMutationKind::SetFieldNotNull,
            ))
        }
        SqlAlterColumnAction::DropNotNull => {
            let field = resolve_sql_ddl_field_nullability_candidate(
                accepted_before,
                statement.column_name.as_str(),
                true,
            )
            .map_err(|error| {
                sql_field_nullability_candidate_error(
                    entity_name,
                    statement.column_name.as_str(),
                    error,
                )
            })?;
            Ok(bind_alter_table_alter_column_nullability(
                entity_name,
                &field,
                true,
                SqlDdlMutationKind::DropFieldNotNull,
            ))
        }
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

    let field =
        match resolve_sql_ddl_field_drop_candidate(accepted_before, statement.column_name.as_str())
        {
            Ok(field) => field,
            Err(SchemaDdlFieldDropCandidateError::Unknown) if statement.if_exists => {
                return Ok(BoundSqlDdlRequest {
                    schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
                    statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                        mutation_kind: SqlDdlMutationKind::DropField,
                        index_name: statement.column_name.clone(),
                        entity_name: entity_name.to_string(),
                        target_store: "-".to_string(),
                        field_path: vec![statement.column_name.clone()],
                    }),
                });
            }
            Err(SchemaDdlFieldDropCandidateError::Unknown) => {
                return Err(SqlDdlBindError::UnknownColumn {
                    entity_name: entity_name.to_string(),
                    column_name: statement.column_name.clone(),
                });
            }
            Err(SchemaDdlFieldDropCandidateError::PrimaryKey) => {
                return Err(SqlDdlBindError::PrimaryKeyFieldDropRejected {
                    entity_name: entity_name.to_string(),
                    column_name: statement.column_name.clone(),
                });
            }
            Err(SchemaDdlFieldDropCandidateError::Generated) => {
                return Err(SqlDdlBindError::GeneratedFieldDropRejected {
                    entity_name: entity_name.to_string(),
                    column_name: statement.column_name.clone(),
                });
            }
            Err(SchemaDdlFieldDropCandidateError::Indexed(index_name)) => {
                return Err(SqlDdlBindError::IndexedFieldDropRejected {
                    entity_name: entity_name.to_string(),
                    column_name: statement.column_name.clone(),
                    index_name,
                });
            }
        };

    Ok(BoundSqlDdlRequest {
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
        statement: BoundSqlDdlStatement::DropColumn(BoundSqlDropColumnRequest {
            entity_name: entity_name.to_string(),
            field,
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

    let field = resolve_sql_ddl_field_rename_candidate(
        accepted_before,
        statement.old_column_name.as_str(),
        statement.new_column_name.as_str(),
    )
    .map_err(|error| {
        sql_field_rename_candidate_error(
            entity_name,
            statement.old_column_name.as_str(),
            statement.new_column_name.as_str(),
            error,
        )
    })?;

    if statement.old_column_name == statement.new_column_name {
        return Ok(BoundSqlDdlRequest {
            schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
            statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                mutation_kind: SqlDdlMutationKind::RenameField,
                index_name: statement.old_column_name.clone(),
                entity_name: entity_name.to_string(),
                target_store: "-".to_string(),
                field_path: vec![statement.old_column_name.clone()],
            }),
        });
    }

    Ok(BoundSqlDdlRequest {
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
        statement: BoundSqlDdlStatement::RenameColumn(BoundSqlRenameColumnRequest {
            entity_name: entity_name.to_string(),
            field,
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
            schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
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
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
        statement: BoundSqlDdlStatement::AlterColumnDefault(BoundSqlAlterColumnDefaultRequest {
            entity_name: entity_name.to_string(),
            field: field.clone(),
            default,
            mutation_kind,
        }),
    }
}

fn bind_alter_table_alter_column_nullability(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    nullable: bool,
    mutation_kind: SqlDdlMutationKind,
) -> BoundSqlDdlRequest {
    if field.nullable() == nullable {
        return BoundSqlDdlRequest {
            schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
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
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
        statement: BoundSqlDdlStatement::AlterColumnNullability(
            BoundSqlAlterColumnNullabilityRequest {
                entity_name: entity_name.to_string(),
                field: field.clone(),
                nullable,
                mutation_kind,
            },
        ),
    }
}

fn sql_field_default_candidate_error(
    entity_name: &str,
    column_name: &str,
    error: SchemaDdlFieldDefaultCandidateError,
) -> SqlDdlBindError {
    match error {
        SchemaDdlFieldDefaultCandidateError::Unknown => SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: column_name.to_string(),
        },
        SchemaDdlFieldDefaultCandidateError::Generated => {
            SqlDdlBindError::GeneratedFieldDefaultChangeRejected {
                entity_name: entity_name.to_string(),
                column_name: column_name.to_string(),
            }
        }
        SchemaDdlFieldDefaultCandidateError::Required => {
            SqlDdlBindError::UnsupportedAlterTableDropDefaultRequired {
                entity_name: entity_name.to_string(),
                column_name: column_name.to_string(),
            }
        }
    }
}

fn sql_field_nullability_candidate_error(
    entity_name: &str,
    column_name: &str,
    error: SchemaDdlFieldNullabilityCandidateError,
) -> SqlDdlBindError {
    match error {
        SchemaDdlFieldNullabilityCandidateError::Unknown => SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: column_name.to_string(),
        },
        SchemaDdlFieldNullabilityCandidateError::Generated => {
            SqlDdlBindError::GeneratedFieldNullabilityChangeRejected {
                entity_name: entity_name.to_string(),
                column_name: column_name.to_string(),
            }
        }
    }
}

fn sql_field_rename_candidate_error(
    entity_name: &str,
    old_column_name: &str,
    new_column_name: &str,
    error: SchemaDdlFieldRenameCandidateError,
) -> SqlDdlBindError {
    match error {
        SchemaDdlFieldRenameCandidateError::Unknown => SqlDdlBindError::UnknownColumn {
            entity_name: entity_name.to_string(),
            column_name: old_column_name.to_string(),
        },
        SchemaDdlFieldRenameCandidateError::Duplicate => SqlDdlBindError::DuplicateColumn {
            entity_name: entity_name.to_string(),
            column_name: new_column_name.to_string(),
        },
        SchemaDdlFieldRenameCandidateError::Generated => {
            SqlDdlBindError::GeneratedFieldRenameRejected {
                entity_name: entity_name.to_string(),
                column_name: old_column_name.to_string(),
            }
        }
    }
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
    encode_sql_ddl_add_column_default(
        column_name,
        default,
        kind,
        nullable,
        storage_decode,
        leaf_codec,
    )
    .map_err(|error| SqlDdlBindError::InvalidAlterTableAddColumnDefault {
        entity_name: entity_name.to_string(),
        column_name: column_name.to_string(),
        detail: error.to_string(),
    })
}

fn schema_field_default_for_alter_column_default(
    entity_name: &str,
    field: &PersistedFieldSnapshot,
    default: &crate::value::Value,
) -> Result<SchemaFieldDefault, SqlDdlBindError> {
    encode_sql_ddl_alter_column_default(field, default).map_err(|error| {
        SqlDdlBindError::InvalidAlterTableAlterColumnDefault {
            entity_name: entity_name.to_string(),
            column_name: field.name().to_string(),
            detail: error.to_string(),
        }
    })
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
