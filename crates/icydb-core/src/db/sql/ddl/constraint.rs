use super::{
    BoundSqlDdlNoOpRequest, BoundSqlDdlRequest, BoundSqlDdlSchemaVersionContract,
    BoundSqlDdlStatement, SqlDdlBindError, SqlDdlMutationKind,
};
use crate::db::{
    schema::{
        AcceptedCheckExprV1, AcceptedConstraintKind, AcceptedSchemaSnapshot,
        ConstraintActivationKind, ConstraintId, ConstraintOrigin, SchemaInfo, bind_sql_check_expr,
        validate_constraint_name,
    },
    sql::{
        identifier::identifiers_tail_match,
        parser::{
            SqlAlterTableAddCheckConstraintStatement, SqlAlterTableDropConstraintStatement,
            SqlAlterTableValidateConstraintStatement,
        },
    },
};

/// Accepted-catalog-bound `ADD CONSTRAINT ... CHECK` request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlAddCheckConstraintRequest {
    entity_name: String,
    constraint_name: String,
    expression: AcceptedCheckExprV1,
    not_valid: bool,
}

impl BoundSqlAddCheckConstraintRequest {
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn constraint_name(&self) -> &str {
        self.constraint_name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn expression(&self) -> &AcceptedCheckExprV1 {
        &self.expression
    }

    #[must_use]
    pub(in crate::db) const fn not_valid(&self) -> bool {
        self.not_valid
    }
}

/// Accepted-catalog-bound SQL-DDL check removal request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDropConstraintRequest {
    entity_name: String,
    constraint_name: String,
    constraint_id: ConstraintId,
    activation: bool,
}

impl BoundSqlDropConstraintRequest {
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn constraint_name(&self) -> &str {
        self.constraint_name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn constraint_id(&self) -> ConstraintId {
        self.constraint_id
    }

    #[must_use]
    pub(in crate::db) const fn is_activation(&self) -> bool {
        self.activation
    }
}

/// Accepted-catalog-bound bounded check-validation request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlValidateConstraintRequest {
    entity_name: String,
    constraint_name: String,
    constraint_id: ConstraintId,
    kind: BoundSqlValidationConstraintKind,
    activation_epoch: Option<u64>,
    after_page_sequence: Option<u64>,
    already_validated: bool,
}

impl BoundSqlValidateConstraintRequest {
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn constraint_name(&self) -> &str {
        self.constraint_name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn constraint_id(&self) -> ConstraintId {
        self.constraint_id
    }

    #[must_use]
    pub(in crate::db) const fn activation_epoch(&self) -> Option<u64> {
        self.activation_epoch
    }

    #[must_use]
    pub(in crate::db) const fn kind(&self) -> BoundSqlValidationConstraintKind {
        self.kind
    }

    #[must_use]
    pub(in crate::db) const fn after_page_sequence(&self) -> Option<u64> {
        self.after_page_sequence
    }

    #[must_use]
    pub(in crate::db) const fn already_validated(&self) -> bool {
        self.already_validated
    }
}

/// Accepted constraint family selected for one bounded SQL validation call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum BoundSqlValidationConstraintKind {
    Check,
    NotNull,
    Unique,
}

pub(super) fn bind_alter_table_add_check_constraint_statement(
    statement: &SqlAlterTableAddCheckConstraintStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = bound_entity_name(statement.entity.as_str(), schema)?;
    validate_constraint_name(statement.constraint_name.as_str()).map_err(|_| {
        SqlDdlBindError::InvalidConstraintName {
            constraint_name: statement.constraint_name.clone(),
        }
    })?;
    let catalog = accepted_before.persisted_snapshot().constraint_catalog();
    if catalog
        .constraints()
        .iter()
        .any(|constraint| constraint.name() == statement.constraint_name)
        || catalog
            .activations()
            .iter()
            .any(|activation| activation.name() == statement.constraint_name)
    {
        return Err(SqlDdlBindError::DuplicateConstraintName {
            constraint_name: statement.constraint_name.clone(),
        });
    }
    let value_catalog = schema
        .value_catalog_handle()
        .ok_or(SqlDdlBindError::AcceptedValueCatalogRequired)?;
    let expression = bind_sql_check_expr(
        &statement.expression,
        accepted_before.persisted_snapshot(),
        value_catalog.enum_catalog(),
        value_catalog.composite_catalog(),
    )
    .map_err(SqlDdlBindError::InvalidCheckExpression)?;

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::AddCheckConstraint(BoundSqlAddCheckConstraintRequest {
            entity_name: entity_name.to_string(),
            constraint_name: statement.constraint_name.clone(),
            expression,
            not_valid: statement.not_valid,
        }),
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
    })
}

pub(super) fn bind_alter_table_drop_constraint_statement(
    statement: &SqlAlterTableDropConstraintStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = bound_entity_name(statement.entity.as_str(), schema)?;
    let catalog = accepted_before.persisted_snapshot().constraint_catalog();
    if let Some(constraint) = catalog
        .constraints()
        .iter()
        .find(|constraint| constraint.name() == statement.constraint_name)
    {
        if constraint.origin() != ConstraintOrigin::SqlDdl
            || !matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
        {
            return Err(SqlDdlBindError::ConstraintOwnershipRejected {
                constraint_name: statement.constraint_name.clone(),
            });
        }
        return Ok(bound_drop(
            entity_name,
            statement.constraint_name.clone(),
            constraint.id(),
            false,
        ));
    }
    if let Some(activation) = catalog
        .activations()
        .iter()
        .find(|activation| activation.name() == statement.constraint_name)
    {
        if activation.origin() != ConstraintOrigin::SqlDdl
            || !matches!(activation.kind(), ConstraintActivationKind::Check { .. })
        {
            return Err(SqlDdlBindError::ConstraintOwnershipRejected {
                constraint_name: statement.constraint_name.clone(),
            });
        }
        return Ok(bound_drop(
            entity_name,
            statement.constraint_name.clone(),
            activation.id(),
            true,
        ));
    }
    if statement.if_exists {
        return Ok(BoundSqlDdlRequest {
            statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                mutation_kind: SqlDdlMutationKind::DropCheckConstraint,
                index_name: statement.constraint_name.clone(),
                entity_name: entity_name.to_string(),
                target_store: entity_name.to_string(),
                field_path: Vec::new(),
            }),
            schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
        });
    }

    Err(SqlDdlBindError::UnknownConstraint {
        constraint_name: statement.constraint_name.clone(),
    })
}

pub(super) fn bind_alter_table_validate_constraint_statement(
    statement: &SqlAlterTableValidateConstraintStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = bound_entity_name(statement.entity.as_str(), schema)?;
    let catalog = accepted_before.persisted_snapshot().constraint_catalog();
    let (constraint_id, kind, activation_epoch, already_validated) = if let Some(activation) =
        catalog
            .activations()
            .iter()
            .find(|activation| activation.name() == statement.constraint_name)
    {
        let kind = match activation.kind() {
            ConstraintActivationKind::Check { .. } => BoundSqlValidationConstraintKind::Check,
            ConstraintActivationKind::NotNull { .. } => BoundSqlValidationConstraintKind::NotNull,
            ConstraintActivationKind::Unique { .. } => BoundSqlValidationConstraintKind::Unique,
            ConstraintActivationKind::Relation { .. } => {
                return Err(SqlDdlBindError::ConstraintOwnershipRejected {
                    constraint_name: statement.constraint_name.clone(),
                });
            }
        };
        if activation.origin() != ConstraintOrigin::SqlDdl {
            return Err(SqlDdlBindError::ConstraintOwnershipRejected {
                constraint_name: statement.constraint_name.clone(),
            });
        }
        (
            activation.id(),
            kind,
            Some(activation.activation_epoch()),
            false,
        )
    } else if let Some(constraint) = catalog
        .constraints()
        .iter()
        .find(|constraint| constraint.name() == statement.constraint_name)
    {
        let kind = match constraint.kind() {
            AcceptedConstraintKind::Check { .. } => BoundSqlValidationConstraintKind::Check,
            AcceptedConstraintKind::NotNull { .. } => BoundSqlValidationConstraintKind::NotNull,
            AcceptedConstraintKind::Unique { .. } => BoundSqlValidationConstraintKind::Unique,
            AcceptedConstraintKind::PrimaryKey | AcceptedConstraintKind::Relation { .. } => {
                return Err(SqlDdlBindError::ConstraintOwnershipRejected {
                    constraint_name: statement.constraint_name.clone(),
                });
            }
        };
        if constraint.origin() != ConstraintOrigin::SqlDdl {
            return Err(SqlDdlBindError::ConstraintOwnershipRejected {
                constraint_name: statement.constraint_name.clone(),
            });
        }
        (constraint.id(), kind, None, true)
    } else {
        return Err(SqlDdlBindError::UnknownConstraint {
            constraint_name: statement.constraint_name.clone(),
        });
    };

    Ok(BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::ValidateConstraint(BoundSqlValidateConstraintRequest {
            entity_name: entity_name.to_string(),
            constraint_name: statement.constraint_name.clone(),
            constraint_id,
            kind,
            activation_epoch,
            after_page_sequence: statement.after_page_sequence,
            already_validated,
        }),
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
    })
}

fn bound_drop(
    entity_name: &str,
    constraint_name: String,
    constraint_id: ConstraintId,
    activation: bool,
) -> BoundSqlDdlRequest {
    BoundSqlDdlRequest {
        statement: BoundSqlDdlStatement::DropConstraint(BoundSqlDropConstraintRequest {
            entity_name: entity_name.to_string(),
            constraint_name,
            constraint_id,
            activation,
        }),
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
    }
}

fn bound_entity_name<'a>(
    sql_entity: &str,
    schema: &'a SchemaInfo,
) -> Result<&'a str, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;
    if !identifiers_tail_match(sql_entity, entity_name) {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: sql_entity.to_string(),
            expected_entity: entity_name.to_string(),
        });
    }
    Ok(entity_name)
}
