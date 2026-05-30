use super::{
    BoundSqlDdlNoOpRequest, BoundSqlDdlRequest, BoundSqlDdlStatement, SqlDdlBindError,
    SqlDdlMutationKind,
};
use crate::db::{
    predicate::parse_sql_predicate,
    query::predicate::validate_predicate,
    schema::{
        AcceptedSchemaSnapshot, PersistedFieldKind, PersistedIndexExpressionOp,
        PersistedIndexExpressionSnapshot, PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot,
        PersistedIndexSnapshot, SchemaDdlIndexDropCandidateError, SchemaExpressionIndexInfo,
        SchemaExpressionIndexKeyItemInfo, SchemaInfo,
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
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    /// Borrow the accepted entity name that owns this request.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
    }

    /// Borrow the accepted field-path targets.
    #[must_use]
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
    entity_name: String,
    dropped_index: PersistedIndexSnapshot,
    field_path: Vec<String>,
}

impl BoundSqlDropIndexRequest {
    /// Borrow the requested index name.
    #[must_use]
    pub(in crate::db) const fn index_name(&self) -> &str {
        self.index_name.as_str()
    }

    /// Borrow the accepted entity name that owns this request.
    #[must_use]
    pub(in crate::db) const fn entity_name(&self) -> &str {
        self.entity_name.as_str()
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
    op: PersistedIndexExpressionOp,
    source: BoundSqlDdlFieldPath,
    canonical_sql: String,
}

impl BoundSqlDdlExpressionKey {
    /// Return the accepted expression operation.
    #[must_use]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
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
    if let Some(existing_index) = find_field_path_index_by_name(schema, statement.name.as_str()) {
        if key_items_are_field_path_only(key_items.as_slice())
            && statement.if_not_exists
            && existing_field_path_index_matches_request(
                existing_index,
                field_paths.as_slice(),
                statement.predicate_sql.as_deref(),
                statement.uniqueness,
            )
        {
            return Ok(BoundSqlDdlRequest {
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::AddFieldPathIndex,
                    index_name: statement.name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: existing_index.store().to_string(),
                    field_path: ddl_field_path_report(field_paths.as_slice()),
                }),
            });
        }

        return Err(SqlDdlBindError::DuplicateIndexName {
            index_name: statement.name.clone(),
        });
    }
    let predicate_sql =
        validated_create_index_predicate_sql(statement.predicate_sql.as_deref(), schema)?;
    if let Some(existing_index) = find_expression_index_by_name(schema, statement.name.as_str()) {
        if statement.if_not_exists
            && existing_expression_index_matches_request(
                existing_index,
                key_items.as_slice(),
                predicate_sql.as_deref(),
                statement.uniqueness,
            )
        {
            return Ok(BoundSqlDdlRequest {
                statement: BoundSqlDdlStatement::NoOp(BoundSqlDdlNoOpRequest {
                    mutation_kind: SqlDdlMutationKind::AddExpressionIndex,
                    index_name: statement.name.clone(),
                    entity_name: entity_name.to_string(),
                    target_store: existing_index.store().to_string(),
                    field_path: ddl_key_item_report(key_items.as_slice()),
                }),
            });
        }

        return Err(SqlDdlBindError::DuplicateIndexName {
            index_name: statement.name.clone(),
        });
    }
    if key_items_are_field_path_only(key_items.as_slice()) {
        reject_duplicate_field_path_index(
            field_paths.as_slice(),
            predicate_sql.as_deref(),
            schema,
        )?;
    } else {
        reject_duplicate_expression_index(key_items.as_slice(), predicate_sql.as_deref(), schema)?;
    }
    let candidate_index = candidate_index_snapshot(
        statement.name.as_str(),
        key_items.as_slice(),
        predicate_sql.as_deref(),
        statement.uniqueness,
        schema,
        index_store_path,
    )?;

    Ok(BoundSqlDdlRequest {
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
    let (dropped_index, field_path) = match drop_candidate {
        Ok((dropped_index, field_path)) => (dropped_index, field_path),
        Err(SqlDdlBindError::UnknownIndex { .. }) if statement.if_exists => {
            return Ok(BoundSqlDdlRequest {
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
        statement: BoundSqlDdlStatement::DropIndex(BoundSqlDropIndexRequest {
            index_name: statement.name.clone(),
            entity_name: entity_name.to_string(),
            dropped_index,
            field_path,
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
) -> PersistedIndexExpressionOp {
    match function {
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Lower => {
            PersistedIndexExpressionOp::Lower
        }
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Upper => {
            PersistedIndexExpressionOp::Upper
        }
        crate::db::sql::parser::SqlCreateIndexExpressionFunction::Trim => {
            PersistedIndexExpressionOp::Trim
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

    if !capabilities.orderable() {
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

fn find_field_path_index_by_name<'a>(
    schema: &'a SchemaInfo,
    index_name: &str,
) -> Option<&'a crate::db::schema::SchemaIndexInfo> {
    schema
        .field_path_indexes()
        .iter()
        .find(|index| index.name() == index_name)
}

fn existing_field_path_index_matches_request(
    index: &crate::db::schema::SchemaIndexInfo,
    field_paths: &[BoundSqlDdlFieldPath],
    predicate_sql: Option<&str>,
    uniqueness: SqlCreateIndexUniqueness,
) -> bool {
    let fields = index.fields();

    index.unique() == matches!(uniqueness, SqlCreateIndexUniqueness::Unique)
        && index.predicate_sql() == predicate_sql
        && fields.len() == field_paths.len()
        && fields
            .iter()
            .zip(field_paths)
            .all(|(field, requested)| field.path() == requested.accepted_path())
}

fn find_expression_index_by_name<'a>(
    schema: &'a SchemaInfo,
    index_name: &str,
) -> Option<&'a SchemaExpressionIndexInfo> {
    schema
        .expression_indexes()
        .iter()
        .find(|index| index.name() == index_name)
}

fn existing_expression_index_matches_request(
    index: &SchemaExpressionIndexInfo,
    key_items: &[BoundSqlDdlCreateIndexKey],
    predicate_sql: Option<&str>,
    uniqueness: SqlCreateIndexUniqueness,
) -> bool {
    let existing_key_items = index.key_items();

    index.unique() == matches!(uniqueness, SqlCreateIndexUniqueness::Unique)
        && index.predicate_sql() == predicate_sql
        && existing_key_items.len() == key_items.len()
        && existing_key_items
            .iter()
            .zip(key_items)
            .all(existing_expression_key_item_matches_request)
}

fn existing_expression_key_item_matches_request(
    existing: (
        &SchemaExpressionIndexKeyItemInfo,
        &BoundSqlDdlCreateIndexKey,
    ),
) -> bool {
    let (existing, requested) = existing;
    match (existing, requested) {
        (
            SchemaExpressionIndexKeyItemInfo::FieldPath(existing),
            BoundSqlDdlCreateIndexKey::FieldPath(requested),
        ) => existing.path() == requested.accepted_path(),
        (
            SchemaExpressionIndexKeyItemInfo::Expression(existing),
            BoundSqlDdlCreateIndexKey::Expression(requested),
        ) => existing_expression_component_matches_request(
            existing.op(),
            existing.source().path(),
            existing.canonical_text(),
            requested,
        ),
        _ => false,
    }
}

fn existing_expression_component_matches_request(
    existing_op: PersistedIndexExpressionOp,
    existing_path: &[String],
    existing_canonical_text: &str,
    requested: &BoundSqlDdlExpressionKey,
) -> bool {
    let requested_path = requested.source().accepted_path();
    let requested_canonical_text = format!("expr:v1:{}", requested.canonical_sql());

    existing_op == requested.op()
        && existing_path == requested_path
        && existing_canonical_text == requested_canonical_text
}

fn reject_duplicate_expression_index(
    key_items: &[BoundSqlDdlCreateIndexKey],
    predicate_sql: Option<&str>,
    schema: &SchemaInfo,
) -> Result<(), SqlDdlBindError> {
    let Some(existing_index) = schema.expression_indexes().iter().find(|index| {
        existing_expression_index_matches_request(
            index,
            key_items,
            predicate_sql,
            if index.unique() {
                SqlCreateIndexUniqueness::Unique
            } else {
                SqlCreateIndexUniqueness::NonUnique
            },
        )
    }) else {
        return Ok(());
    };

    Err(SqlDdlBindError::DuplicateFieldPathIndex {
        field_path: ddl_key_item_report(key_items).join(","),
        existing_index: existing_index.name().to_string(),
    })
}

fn reject_duplicate_field_path_index(
    field_paths: &[BoundSqlDdlFieldPath],
    predicate_sql: Option<&str>,
    schema: &SchemaInfo,
) -> Result<(), SqlDdlBindError> {
    let Some(existing_index) = schema.field_path_indexes().iter().find(|index| {
        let fields = index.fields();
        index.predicate_sql() == predicate_sql
            && fields.len() == field_paths.len()
            && fields
                .iter()
                .zip(field_paths)
                .all(|(field, requested)| field.path() == requested.accepted_path())
    }) else {
        return Ok(());
    };

    Err(SqlDdlBindError::DuplicateFieldPathIndex {
        field_path: ddl_field_path_report(field_paths).join(","),
        existing_index: existing_index.name().to_string(),
    })
}

fn candidate_index_snapshot(
    index_name: &str,
    key_items: &[BoundSqlDdlCreateIndexKey],
    predicate_sql: Option<&str>,
    uniqueness: SqlCreateIndexUniqueness,
    schema: &SchemaInfo,
    index_store_path: &'static str,
) -> Result<PersistedIndexSnapshot, SqlDdlBindError> {
    let key = if key_items_are_field_path_only(key_items) {
        PersistedIndexKeySnapshot::FieldPath(
            key_items
                .iter()
                .map(|key_item| {
                    let BoundSqlDdlCreateIndexKey::FieldPath(field_path) = key_item else {
                        unreachable!("field-path-only index checked before field-path lowering");
                    };

                    accepted_index_field_path_snapshot(schema, field_path)
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    } else {
        PersistedIndexKeySnapshot::Items(
            key_items
                .iter()
                .map(|key_item| match key_item {
                    BoundSqlDdlCreateIndexKey::FieldPath(field_path) => {
                        accepted_index_field_path_snapshot(schema, field_path)
                            .map(PersistedIndexKeyItemSnapshot::FieldPath)
                    }
                    BoundSqlDdlCreateIndexKey::Expression(expression) => {
                        accepted_index_expression_snapshot(schema, expression)
                    }
                })
                .collect::<Result<Vec<_>, _>>()?,
        )
    };

    Ok(PersistedIndexSnapshot::new_sql_ddl(
        schema.next_secondary_index_ordinal(),
        index_name.to_string(),
        index_store_path.to_string(),
        matches!(uniqueness, SqlCreateIndexUniqueness::Unique),
        key,
        predicate_sql.map(str::to_string),
    ))
}

fn accepted_index_field_path_snapshot(
    schema: &SchemaInfo,
    field_path: &BoundSqlDdlFieldPath,
) -> Result<crate::db::schema::PersistedIndexFieldPathSnapshot, SqlDdlBindError> {
    schema
        .accepted_index_field_path_snapshot(field_path.root(), field_path.segments())
        .ok_or_else(|| SqlDdlBindError::FieldPathNotAcceptedCatalogBacked {
            field_path: field_path.accepted_path().join("."),
        })
}

fn accepted_index_expression_snapshot(
    schema: &SchemaInfo,
    expression: &BoundSqlDdlExpressionKey,
) -> Result<PersistedIndexKeyItemSnapshot, SqlDdlBindError> {
    let source = accepted_index_field_path_snapshot(schema, expression.source())?;
    let Some(output_kind) = expression_output_kind(expression.op(), source.kind()) else {
        return Err(SqlDdlBindError::FieldPathNotIndexable {
            field_path: expression.source().accepted_path().join("."),
        });
    };

    Ok(PersistedIndexKeyItemSnapshot::Expression(Box::new(
        PersistedIndexExpressionSnapshot::new(
            expression.op(),
            source.clone(),
            source.kind().clone(),
            output_kind,
            format!("expr:v1:{}", expression.canonical_sql()),
        ),
    )))
}

fn expression_output_kind(
    op: PersistedIndexExpressionOp,
    source_kind: &PersistedFieldKind,
) -> Option<PersistedFieldKind> {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            if matches!(source_kind, PersistedFieldKind::Text { .. }) {
                Some(source_kind.clone())
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Date => {
            if matches!(
                source_kind,
                PersistedFieldKind::Date | PersistedFieldKind::Timestamp
            ) {
                Some(PersistedFieldKind::Date)
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            if matches!(
                source_kind,
                PersistedFieldKind::Date | PersistedFieldKind::Timestamp
            ) {
                Some(PersistedFieldKind::Int64)
            } else {
                None
            }
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
    let predicate = parse_sql_predicate(predicate_sql).map_err(|error| {
        SqlDdlBindError::InvalidFilteredIndexPredicate {
            detail: error.to_string(),
        }
    })?;
    validate_predicate(schema, &predicate).map_err(|error| {
        SqlDdlBindError::InvalidFilteredIndexPredicate {
            detail: error.to_string(),
        }
    })?;

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
