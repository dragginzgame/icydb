use super::{
    BoundSqlDdlNoOpRequest, BoundSqlDdlRequest, BoundSqlDdlSchemaVersionContract,
    BoundSqlDdlStatement, SqlDdlBindError, SqlDdlMutationKind,
};
use crate::db::{
    predicate::parse_sql_predicate,
    query::predicate::validate_predicate,
    schema::{
        AcceptedSchemaSnapshot, ConstraintId, PersistedIndexSnapshot,
        SchemaDdlIndexDropCandidateError, SchemaDdlSecondaryIndexAdditionCandidate,
        SchemaDdlSecondaryIndexAdditionCandidateError, SchemaDdlSecondaryIndexExpressionIntent,
        SchemaDdlSecondaryIndexExpressionOpIntent, SchemaDdlSecondaryIndexFieldPathIntent,
        SchemaDdlSecondaryIndexKeyCandidateError, SchemaDdlSecondaryIndexKeyIntent, SchemaInfo,
        build_sql_ddl_secondary_index_candidate,
        resolve_sql_ddl_secondary_index_addition_candidate,
        resolve_sql_ddl_secondary_index_drop_candidate,
    },
    sql::{
        identifier::identifiers_tail_match,
        parser::{
            SqlCreateIndexExpressionKey, SqlCreateIndexKeyItem, SqlCreateIndexStatement,
            SqlCreateIndexUniqueness, SqlDropIndexStatement,
        },
    },
};

///
/// BoundSqlCreateIndexRequest
///
/// Catalog-resolved request for adding one secondary index.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlCreateIndexRequest {
    index_name: String,
    entity_name: String,
    key_items: Vec<BoundSqlDdlCreateIndexKey>,
    field_paths: Vec<BoundSqlDdlFieldPath>,
    candidate_index: PersistedIndexSnapshot,
}

impl BoundSqlCreateIndexRequest {
    /// Borrow the requested index name.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    /// Borrow the accepted entity name that owns this request.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field-path targets.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn field_paths(&self) -> &[BoundSqlDdlFieldPath] {
        self.field_paths.as_slice()
    }

    /// Borrow the accepted key targets in DDL key order.
    #[must_use]
    pub(in crate::db) const fn key_items(&self) -> &[BoundSqlDdlCreateIndexKey] {
        self.key_items.as_slice()
    }

    /// Borrow the candidate accepted index snapshot for mutation admission.
    #[must_use]
    pub(in crate::db) const fn candidate_index(&self) -> &PersistedIndexSnapshot {
        &self.candidate_index
    }
}

///
/// BoundSqlDropIndexRequest
///
/// Catalog-resolved request for dropping one DDL-published secondary index.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDropIndexRequest {
    index_name: String,
    dropped_index: PersistedIndexSnapshot,
    field_path: Vec<String>,
    pending_activation_id: Option<ConstraintId>,
}

impl BoundSqlDropIndexRequest {
    /// Borrow the requested index name.
    #[must_use]
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    /// Borrow the accepted index snapshot that will be removed.
    #[must_use]
    pub(in crate::db) const fn dropped_index(&self) -> &PersistedIndexSnapshot {
        &self.dropped_index
    }

    /// Borrow the dropped field-path target.
    #[must_use]
    pub(in crate::db) const fn field_path(&self) -> &[String] {
        self.field_path.as_slice()
    }

    /// Return the live unique activation retired by this drop, when the index
    /// has not yet become planner-visible.
    #[must_use]
    pub(in crate::db) const fn pending_activation_id(&self) -> Option<ConstraintId> {
        self.pending_activation_id
    }
}

///
/// BoundSqlDdlFieldPath
///
/// Accepted field-path target for SQL DDL binding.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDdlFieldPath {
    root: String,
    segments: Vec<String>,
    accepted_path: Vec<String>,
}

impl BoundSqlDdlFieldPath {
    /// Borrow the top-level field name.
    #[must_use]
    pub(in crate::db) const fn root(&self) -> &str {
        self.root.as_str()
    }

    /// Borrow nested path segments below the top-level field.
    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.segments.as_slice()
    }

    /// Borrow the full accepted field path used by index metadata.
    #[must_use]
    pub(in crate::db) const fn accepted_path(&self) -> &[String] {
        self.accepted_path.as_slice()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum BoundSqlDdlCreateIndexKey {
    FieldPath(BoundSqlDdlFieldPath),
    Expression(BoundSqlDdlExpressionKey),
}

///
/// BoundSqlDdlExpressionKey
///
/// Accepted expression-index key target for SQL DDL binding.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct BoundSqlDdlExpressionKey {
    op: SchemaDdlSecondaryIndexExpressionOpIntent,
    source: BoundSqlDdlFieldPath,
    canonical_sql: String,
}

impl BoundSqlDdlExpressionKey {
    /// Return the accepted expression operation.
    #[must_use]
    pub(in crate::db) const fn op(&self) -> SchemaDdlSecondaryIndexExpressionOpIntent {
        self.op
    }

    /// Borrow the accepted source field path.
    #[must_use]
    pub(in crate::db) const fn source(&self) -> &BoundSqlDdlFieldPath {
        &self.source
    }

    /// Borrow the SQL-facing canonical expression text.
    #[must_use]
    pub(in crate::db) const fn canonical_sql(&self) -> &str {
        self.canonical_sql.as_str()
    }
}

pub(super) fn bind_create_index_statement(
    statement: &SqlCreateIndexStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
    index_store_path: &'static str,
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

    let key_items = statement
        .key_items
        .iter()
        .map(|key_item| bind_create_index_key_item(key_item, entity_name, schema))
        .collect::<Result<Vec<_>, _>>()?;
    let field_paths = create_index_field_path_report_items(key_items.as_slice());
    let predicate_sql =
        validated_create_index_predicate_sql(statement.predicate_sql.as_deref(), schema)?;
    let candidate_index = candidate_index_snapshot(
        accepted_before,
        statement.name.as_str(),
        key_items.as_slice(),
        predicate_sql.as_deref(),
        statement.uniqueness,
        index_store_path,
    )?;
    let addition_candidate =
        resolve_sql_ddl_secondary_index_addition_candidate(accepted_before, candidate_index)
            .map_err(|error| {
                sql_secondary_index_addition_candidate_error(
                    statement.name.as_str(),
                    &key_items,
                    &field_paths,
                    error,
                )
            })?;
    let candidate_index = match addition_candidate {
        SchemaDdlSecondaryIndexAdditionCandidate::Add(candidate_index) => candidate_index,
        SchemaDdlSecondaryIndexAdditionCandidate::Existing(existing_index)
            if statement.if_not_exists =>
        {
            return Ok(BoundSqlDdlRequest {
                schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: create_index_mutation_kind(key_items.as_slice()),
                    index_name: statement.name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: existing_index.store().to_string(),
                    field_path: create_index_key_report(
                        key_items.as_slice(),
                        field_paths.as_slice(),
                    ),
                }),
            });
        }
        SchemaDdlSecondaryIndexAdditionCandidate::Existing(_) => {
            return Err(SqlDdlBindError::DuplicateIndexName {
                index_name: statement.name.clone(),
            });
        }
    };

    Ok(BoundSqlDdlRequest {
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
        statement: BoundSqlDdlStatement::CreateIndex(BoundSqlCreateIndexRequest {
            index_name: statement.name.clone(),
            entity_name: entity_name.to_string(),
            key_items,
            field_paths,
            candidate_index,
        }),
    })
}

pub(super) fn bind_drop_index_statement(
    statement: &SqlDropIndexStatement,
    accepted_before: &AcceptedSchemaSnapshot,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlRequest, SqlDdlBindError> {
    let entity_name = schema
        .entity_name()
        .ok_or(SqlDdlBindError::MissingEntityName)?;

    if let Some(sql_entity) = statement.entity.as_deref()
        && !identifiers_tail_match(sql_entity, entity_name)
    {
        return Err(SqlDdlBindError::EntityMismatch {
            sql_entity: sql_entity.to_string(),
            expected_entity: entity_name.to_string(),
        });
    }
    let drop_candidate = resolve_sql_ddl_secondary_index_drop_candidate(
        accepted_before,
        &statement.name,
    )
    .map_err(|error| match error {
        SchemaDdlIndexDropCandidateError::Generated => {
            SqlDdlBindError::GeneratedIndexDropRejected {
                index_name: statement.name.clone(),
            }
        }
        SchemaDdlIndexDropCandidateError::Unknown => SqlDdlBindError::UnknownIndex {
            entity_name: entity_name.to_string(),
            index_name: statement.name.clone(),
        },
        SchemaDdlIndexDropCandidateError::Unsupported => SqlDdlBindError::UnsupportedDropIndex {
            index_name: statement.name.clone(),
        },
    });
    let (dropped_index, field_path, pending_activation_id) = match drop_candidate {
        Ok((dropped_index, field_path, pending_activation_id)) => {
            (dropped_index, field_path, pending_activation_id)
        }
        Err(SqlDdlBindError::UnknownIndex { .. }) if statement.if_exists => {
            return Ok(BoundSqlDdlRequest {
                schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::DropSecondaryIndex,
                    index_name: statement.name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: "-".to_string(),
                    field_path: Vec::new(),
                }),
            });
        }
        Err(error) => return Err(error),
    };
    Ok(BoundSqlDdlRequest {
        schema_version_contract: BoundSqlDdlSchemaVersionContract::default(),
        statement: BoundSqlDdlStatement::DropIndex(BoundSqlDropIndexRequest {
            index_name: statement.name.clone(),
            dropped_index,
            field_path,
            pending_activation_id,
        }),
    })
}

fn bind_create_index_key_item(
    key_item: &SqlCreateIndexKeyItem,
    entity_name: &str,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlCreateIndexKey, SqlDdlBindError> {
    match key_item {
        SqlCreateIndexKeyItem::FieldPath(field_path) => {
            bind_create_index_field_path(field_path.as_str(), entity_name, schema)
                .map(BoundSqlDdlCreateIndexKey::FieldPath)
        }
        SqlCreateIndexKeyItem::Expression(expression) => {
            bind_create_index_expression_key(expression, entity_name, schema)
        }
    }
}

fn bind_create_index_expression_key(
    expression: &SqlCreateIndexExpressionKey,
    entity_name: &str,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlCreateIndexKey, SqlDdlBindError> {
    let source = bind_create_index_field_path(expression.field_path.as_str(), entity_name, schema)?;

    Ok(BoundSqlDdlCreateIndexKey::Expression(
        BoundSqlDdlExpressionKey {
            op: expression_op_from_sql_function(expression.function),
            source,
            canonical_sql: expression.canonical_sql(),
        },
    ))
}

const fn expression_op_from_sql_function(
    function: crate::db::sql::parser::SqlCreateIndexExpressionFunction,
) -> SchemaDdlSecondaryIndexExpressionOpIntent {
    match function {
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Lower => {
            SchemaDdlSecondaryIndexExpressionOpIntent::Lower
        }
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Upper => {
            SchemaDdlSecondaryIndexExpressionOpIntent::Upper
        }
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Trim => {
            SchemaDdlSecondaryIndexExpressionOpIntent::Trim
        }
    }
}

fn key_items_are_field_path_only(key_items: &[BoundSqlDdlCreateIndexKey]) -> bool {
    key_items
        .iter()
        .all(|key_item| matches!(key_item, BoundSqlDdlCreateIndexKey::FieldPath(_)))
}

fn create_index_field_path_report_items(
    key_items: &[BoundSqlDdlCreateIndexKey],
) -> Vec<BoundSqlDdlFieldPath> {
    key_items
        .iter()
        .map(|key_item| match key_item {
            BoundSqlDdlCreateIndexKey::FieldPath(field_path) => field_path.clone(),
            BoundSqlDdlCreateIndexKey::Expression(expression) => expression.source().clone(),
        })
        .collect()
}

fn bind_create_index_field_path(
    field_path: &str,
    entity_name: &str,
    schema: &SchemaInfo,
) -> Result<BoundSqlDdlFieldPath, SqlDdlBindError> {
    let mut path = field_path
        .split('.')
        .map(str::trim)
        .filter(|segment| !segment.is_empty());
    let Some(root) = path.next() else {
        return Err(SqlDdlBindError::UnknownFieldPath {
            entity_name: entity_name.to_string(),
            field_path: field_path.to_string(),
        });
    };
    let segments = path.map(str::to_string).collect::<Vec<_>>();

    let capabilities = if segments.is_empty() {
        schema.sql_capabilities(root)
    } else {
        schema.nested_sql_capabilities(root, segments.as_slice())
    }
    .ok_or_else(|| SqlDdlBindError::UnknownFieldPath {
        entity_name: entity_name.to_string(),
        field_path: field_path.to_string(),
    })?;

    // Catalog evidence is recorded now, but enum index routing remains closed
    // until runtime values and index bytes use canonical IDs end to end.
    if capabilities.enum_equality().is_some() || !capabilities.orderable() {
        return Err(SqlDdlBindError::FieldPathNotIndexable {
            field_path: field_path.to_string(),
        });
    }

    let mut accepted_path = Vec::with_capacity(segments.len() + 1);
    accepted_path.push(root.to_string());
    accepted_path.extend(segments.iter().cloned());

    Ok(BoundSqlDdlFieldPath {
        root: root.to_string(),
        segments,
        accepted_path,
    })
}

fn sql_secondary_index_addition_candidate_error(
    index_name: &str,
    key_items: &[BoundSqlDdlCreateIndexKey],
    field_paths: &[BoundSqlDdlFieldPath],
    error: SchemaDdlSecondaryIndexAdditionCandidateError,
) -> SqlDdlBindError {
    match error {
        SchemaDdlSecondaryIndexAdditionCandidateError::DuplicateName => {
            SqlDdlBindError::DuplicateIndexName {
                index_name: index_name.to_string(),
            }
        }
        SchemaDdlSecondaryIndexAdditionCandidateError::DuplicateContract { existing_index } => {
            SqlDdlBindError::DuplicateFieldPathIndex {
                field_path: create_index_key_report(key_items, field_paths).join(","),
                existing_index,
            }
        }
    }
}

fn create_index_mutation_kind(key_items: &[BoundSqlDdlCreateIndexKey]) -> SqlDdlMutationKind {
    if key_items_are_field_path_only(key_items) {
        SqlDdlMutationKind::AddFieldPathIndex
    } else {
        SqlDdlMutationKind::AddExpressionIndex
    }
}

fn create_index_key_report(
    key_items: &[BoundSqlDdlCreateIndexKey],
    field_paths: &[BoundSqlDdlFieldPath],
) -> Vec<String> {
    if key_items_are_field_path_only(key_items) {
        ddl_field_path_report(field_paths)
    } else {
        ddl_key_item_report(key_items)
    }
}

fn candidate_index_snapshot(
    accepted_before: &AcceptedSchemaSnapshot,
    index_name: &str,
    key_items: &[BoundSqlDdlCreateIndexKey],
    predicate_sql: Option<&str>,
    uniqueness: SqlCreateIndexUniqueness,
    index_store_path: &'static str,
) -> Result<PersistedIndexSnapshot, SqlDdlBindError> {
    let key_intents = schema_secondary_index_key_intents(key_items);

    build_sql_ddl_secondary_index_candidate(
        accepted_before,
        index_name.to_string(),
        index_store_path.to_string(),
        matches!(uniqueness, SqlCreateIndexUniqueness::Unique),
        key_intents.as_slice(),
        predicate_sql.map(str::to_string),
    )
    .map_err(sql_secondary_index_key_candidate_error)
}

fn schema_secondary_index_key_intents(
    key_items: &[BoundSqlDdlCreateIndexKey],
) -> Vec<SchemaDdlSecondaryIndexKeyIntent> {
    key_items
        .iter()
        .map(|key_item| match key_item {
            BoundSqlDdlCreateIndexKey::FieldPath(field_path) => {
                SchemaDdlSecondaryIndexKeyIntent::FieldPath(schema_index_field_path_intent(
                    field_path,
                ))
            }
            BoundSqlDdlCreateIndexKey::Expression(expression) => {
                SchemaDdlSecondaryIndexKeyIntent::Expression(Box::new(
                    SchemaDdlSecondaryIndexExpressionIntent::new(
                        expression.op(),
                        schema_index_field_path_intent(expression.source()),
                        expression.canonical_sql().to_string(),
                    ),
                ))
            }
        })
        .collect()
}

fn schema_index_field_path_intent(
    field_path: &BoundSqlDdlFieldPath,
) -> SchemaDdlSecondaryIndexFieldPathIntent {
    SchemaDdlSecondaryIndexFieldPathIntent::new(
        field_path.root().to_string(),
        field_path.segments().to_vec(),
    )
}

fn sql_secondary_index_key_candidate_error(
    error: SchemaDdlSecondaryIndexKeyCandidateError,
) -> SqlDdlBindError {
    match error {
        SchemaDdlSecondaryIndexKeyCandidateError::IndexIdentityExhausted => {
            SqlDdlBindError::IndexIdentityExhausted
        }
        SchemaDdlSecondaryIndexKeyCandidateError::FieldPathNotAcceptedCatalogBacked {
            field_path,
        } => SqlDdlBindError::FieldPathNotAcceptedCatalogBacked { field_path },
        SchemaDdlSecondaryIndexKeyCandidateError::FieldPathNotIndexable { field_path } => {
            SqlDdlBindError::FieldPathNotIndexable { field_path }
        }
    }
}

fn validated_create_index_predicate_sql(
    predicate_sql: Option<&str>,
    schema: &SchemaInfo,
) -> Result<Option<String>, SqlDdlBindError> {
    let Some(predicate_sql) = predicate_sql else {
        return Ok(None);
    };
    let predicate = parse_sql_predicate(predicate_sql)
        .map_err(|_| SqlDdlBindError::InvalidFilteredIndexPredicate)?;
    validate_predicate(schema, &predicate)
        .map_err(|_| SqlDdlBindError::InvalidFilteredIndexPredicate)?;

    Ok(Some(predicate_sql.to_string()))
}

fn ddl_field_path_report(field_paths: &[BoundSqlDdlFieldPath]) -> Vec<String> {
    match field_paths {
        [field_path] => field_path.accepted_path().to_vec(),
        _ => vec![
            field_paths
                .iter()
                .map(|field_path| field_path.accepted_path().join("."))
                .collect::<Vec<_>>()
                .join(","),
        ],
    }
}

pub(super) fn ddl_key_item_report(key_items: &[BoundSqlDdlCreateIndexKey]) -> Vec<String> {
    match key_items {
        [key_item] => vec![ddl_key_item_text(key_item)],
        _ => vec![
            key_items
                .iter()
                .map(ddl_key_item_text)
                .collect::<Vec<_>>()
                .join(","),
        ],
    }
}

fn ddl_key_item_text(key_item: &BoundSqlDdlCreateIndexKey) -> String {
    match key_item {
        BoundSqlDdlCreateIndexKey::FieldPath(field_path) => field_path.accepted_path().join("."),
        BoundSqlDdlCreateIndexKey::Expression(expression) => expression.canonical_sql().to_string(),
    }
}
