//! Schema-owned secondary-index candidate helpers for SQL DDL.

use crate::db::schema::{
    AcceptedFieldKind, AcceptedSchemaSnapshot, PersistedIndexExpressionOp,
    PersistedIndexExpressionSnapshot, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
};

/// Schema-owned outcome for resolving one SQL DDL secondary-index addition
/// against accepted catalog authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlSecondaryIndexAdditionCandidate {
    /// No accepted index conflicts with the DDL-authored candidate.
    Add(PersistedIndexSnapshot),
    /// An accepted index with the same name already has the same contract.
    Existing(PersistedIndexSnapshot),
}

/// Secondary-index addition candidate resolution failures for SQL DDL-authored
/// schema mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlSecondaryIndexAdditionCandidateError {
    /// An accepted index already uses the requested SQL index name.
    DuplicateName,
    /// An accepted index already has the requested key and predicate contract.
    DuplicateContract { existing_index: String },
}

/// Schema-owned key intent for one SQL DDL secondary index candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlSecondaryIndexKeyIntent {
    FieldPath(SchemaDdlSecondaryIndexFieldPathIntent),
    Expression(Box<SchemaDdlSecondaryIndexExpressionIntent>),
}

/// Schema-owned SQL DDL deterministic-expression operation intent.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlSecondaryIndexExpressionOpIntent {
    Lower,
    Upper,
    Trim,
}

/// Schema-owned field-path intent for one SQL DDL index key item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaDdlSecondaryIndexFieldPathIntent {
    root: String,
    segments: Vec<String>,
}

impl SchemaDdlSecondaryIndexFieldPathIntent {
    /// Build one SQL DDL field-path index key intent.
    #[must_use]
    pub(in crate::db) const fn new(root: String, segments: Vec<String>) -> Self {
        Self { root, segments }
    }

    #[must_use]
    pub(in crate::db) const fn root(&self) -> &str {
        self.root.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn segments(&self) -> &[String] {
        self.segments.as_slice()
    }
}

/// Schema-owned deterministic-expression intent for one SQL DDL index key
/// item.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaDdlSecondaryIndexExpressionIntent {
    op: SchemaDdlSecondaryIndexExpressionOpIntent,
    source: SchemaDdlSecondaryIndexFieldPathIntent,
    canonical_sql: String,
}

impl SchemaDdlSecondaryIndexExpressionIntent {
    /// Build one SQL DDL expression-index key intent.
    #[must_use]
    pub(in crate::db) const fn new(
        op: SchemaDdlSecondaryIndexExpressionOpIntent,
        source: SchemaDdlSecondaryIndexFieldPathIntent,
        canonical_sql: String,
    ) -> Self {
        Self {
            op,
            source,
            canonical_sql,
        }
    }

    #[must_use]
    pub(in crate::db) const fn op(&self) -> SchemaDdlSecondaryIndexExpressionOpIntent {
        self.op
    }

    #[must_use]
    pub(in crate::db) const fn source(&self) -> &SchemaDdlSecondaryIndexFieldPathIntent {
        &self.source
    }

    #[must_use]
    pub(in crate::db) const fn canonical_sql(&self) -> &str {
        self.canonical_sql.as_str()
    }
}

/// Secondary-index key candidate construction failures for SQL DDL-authored
/// schema mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlSecondaryIndexKeyCandidateError {
    FieldPathNotAcceptedCatalogBacked { field_path: String },
    FieldPathNotIndexable { field_path: String },
}

/// Resolve one accepted SQL DDL secondary-index addition candidate. SQL DDL
/// supplies the already-bound key contract and frontend `IF NOT EXISTS`
/// policy; schema mutation owns accepted name and key-contract comparison.
pub(in crate::db) fn resolve_sql_ddl_secondary_index_addition_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    candidate: PersistedIndexSnapshot,
) -> Result<SchemaDdlSecondaryIndexAdditionCandidate, SchemaDdlSecondaryIndexAdditionCandidateError>
{
    let accepted = accepted_before.persisted_snapshot();

    if let Some(existing) = accepted
        .indexes()
        .iter()
        .find(|index| index.name() == candidate.name())
    {
        if secondary_index_exact_addition_match(existing, &candidate) {
            return Ok(SchemaDdlSecondaryIndexAdditionCandidate::Existing(
                existing.clone(),
            ));
        }

        return Err(SchemaDdlSecondaryIndexAdditionCandidateError::DuplicateName);
    }

    if let Some(existing) = accepted
        .indexes()
        .iter()
        .find(|index| secondary_index_duplicate_contract_match(index, &candidate))
    {
        return Err(
            SchemaDdlSecondaryIndexAdditionCandidateError::DuplicateContract {
                existing_index: existing.name().to_string(),
            },
        );
    }

    Ok(SchemaDdlSecondaryIndexAdditionCandidate::Add(candidate))
}

/// Build one SQL DDL-owned secondary-index candidate with schema-owned ordinal
/// allocation. SQL DDL supplies author intent and schema mutation derives the
/// accepted key metadata and durable catalog identity.
pub(in crate::db) fn build_sql_ddl_secondary_index_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    name: String,
    store: String,
    unique: bool,
    key_items: &[SchemaDdlSecondaryIndexKeyIntent],
    predicate_sql: Option<String>,
) -> Result<PersistedIndexSnapshot, SchemaDdlSecondaryIndexKeyCandidateError> {
    let key = sql_ddl_secondary_index_key_snapshot(accepted_before, key_items)?;

    Ok(PersistedIndexSnapshot::new_sql_ddl(
        next_sql_ddl_secondary_index_ordinal(accepted_before),
        name,
        store,
        unique,
        key,
        predicate_sql,
    ))
}

fn next_sql_ddl_secondary_index_ordinal(accepted_before: &AcceptedSchemaSnapshot) -> u16 {
    u16::try_from(accepted_before.persisted_snapshot().indexes().len())
        .ok()
        .and_then(|count| count.checked_add(1))
        .expect("accepted index ordinals should not be exhausted")
}

fn sql_ddl_secondary_index_key_snapshot(
    accepted_before: &AcceptedSchemaSnapshot,
    key_items: &[SchemaDdlSecondaryIndexKeyIntent],
) -> Result<PersistedIndexKeySnapshot, SchemaDdlSecondaryIndexKeyCandidateError> {
    if key_items
        .iter()
        .all(|key_item| matches!(key_item, SchemaDdlSecondaryIndexKeyIntent::FieldPath(_)))
    {
        return key_items
            .iter()
            .map(|key_item| {
                let SchemaDdlSecondaryIndexKeyIntent::FieldPath(field_path) = key_item else {
                    unreachable!("schema mutation invariant");
                };
                sql_ddl_index_field_path_snapshot(accepted_before, field_path)
            })
            .collect::<Result<Vec<_>, _>>()
            .map(PersistedIndexKeySnapshot::FieldPath);
    }

    key_items
        .iter()
        .map(|key_item| match key_item {
            SchemaDdlSecondaryIndexKeyIntent::FieldPath(field_path) => {
                sql_ddl_index_field_path_snapshot(accepted_before, field_path)
                    .map(PersistedIndexKeyItemSnapshot::FieldPath)
            }
            SchemaDdlSecondaryIndexKeyIntent::Expression(expression) => {
                sql_ddl_index_expression_snapshot(accepted_before, expression)
            }
        })
        .collect::<Result<Vec<_>, _>>()
        .map(PersistedIndexKeySnapshot::Items)
}

fn sql_ddl_index_field_path_snapshot(
    accepted_before: &AcceptedSchemaSnapshot,
    field_path: &SchemaDdlSecondaryIndexFieldPathIntent,
) -> Result<PersistedIndexFieldPathSnapshot, SchemaDdlSecondaryIndexKeyCandidateError> {
    let field = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.name() == field_path.root())
        .ok_or_else(|| {
            SchemaDdlSecondaryIndexKeyCandidateError::FieldPathNotAcceptedCatalogBacked {
                field_path: sql_ddl_index_field_path_text(field_path),
            }
        })?;

    let (kind, nullable) = if field_path.segments().is_empty() {
        (field.kind(), field.nullable())
    } else {
        let leaf = field
            .nested_leaves()
            .iter()
            .find(|leaf| leaf.path() == field_path.segments())
            .ok_or_else(|| {
                SchemaDdlSecondaryIndexKeyCandidateError::FieldPathNotAcceptedCatalogBacked {
                    field_path: sql_ddl_index_field_path_text(field_path),
                }
            })?;
        (leaf.kind(), leaf.nullable())
    };

    let mut path = Vec::with_capacity(field_path.segments().len() + 1);
    path.push(field_path.root().to_string());
    path.extend(field_path.segments().iter().cloned());

    Ok(PersistedIndexFieldPathSnapshot::new(
        field.id(),
        field.slot(),
        path,
        kind.clone(),
        nullable,
    ))
}

fn sql_ddl_index_expression_snapshot(
    accepted_before: &AcceptedSchemaSnapshot,
    expression: &SchemaDdlSecondaryIndexExpressionIntent,
) -> Result<PersistedIndexKeyItemSnapshot, SchemaDdlSecondaryIndexKeyCandidateError> {
    let source = sql_ddl_index_field_path_snapshot(accepted_before, expression.source())?;
    let persisted_op = persisted_expression_op_for_sql_ddl_intent(expression.op());
    let Some(output_kind) = sql_ddl_index_expression_output_kind(persisted_op, source.kind())
    else {
        return Err(
            SchemaDdlSecondaryIndexKeyCandidateError::FieldPathNotIndexable {
                field_path: source.path().join("."),
            },
        );
    };

    Ok(PersistedIndexKeyItemSnapshot::Expression(Box::new(
        PersistedIndexExpressionSnapshot::new(
            persisted_op,
            source.clone(),
            source.kind().clone(),
            output_kind,
            format!("expr:v1:{}", expression.canonical_sql()),
        ),
    )))
}

const fn persisted_expression_op_for_sql_ddl_intent(
    intent: SchemaDdlSecondaryIndexExpressionOpIntent,
) -> PersistedIndexExpressionOp {
    match intent {
        SchemaDdlSecondaryIndexExpressionOpIntent::Lower => PersistedIndexExpressionOp::Lower,
        SchemaDdlSecondaryIndexExpressionOpIntent::Upper => PersistedIndexExpressionOp::Upper,
        SchemaDdlSecondaryIndexExpressionOpIntent::Trim => PersistedIndexExpressionOp::Trim,
    }
}

fn sql_ddl_index_expression_output_kind(
    op: PersistedIndexExpressionOp,
    source_kind: &AcceptedFieldKind,
) -> Option<AcceptedFieldKind> {
    match op {
        PersistedIndexExpressionOp::Lower
        | PersistedIndexExpressionOp::Upper
        | PersistedIndexExpressionOp::Trim
        | PersistedIndexExpressionOp::LowerTrim => {
            if matches!(source_kind, AcceptedFieldKind::Text { .. }) {
                Some(source_kind.clone())
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Date => {
            if matches!(
                source_kind,
                AcceptedFieldKind::Date | AcceptedFieldKind::Timestamp
            ) {
                Some(AcceptedFieldKind::Date)
            } else {
                None
            }
        }
        PersistedIndexExpressionOp::Year
        | PersistedIndexExpressionOp::Month
        | PersistedIndexExpressionOp::Day => {
            if matches!(
                source_kind,
                AcceptedFieldKind::Date | AcceptedFieldKind::Timestamp
            ) {
                Some(AcceptedFieldKind::Int64)
            } else {
                None
            }
        }
    }
}

fn sql_ddl_index_field_path_text(field_path: &SchemaDdlSecondaryIndexFieldPathIntent) -> String {
    let mut path = Vec::with_capacity(field_path.segments().len() + 1);
    path.push(field_path.root().to_string());
    path.extend(field_path.segments().iter().cloned());
    path.join(".")
}

fn secondary_index_exact_addition_match(
    existing: &PersistedIndexSnapshot,
    candidate: &PersistedIndexSnapshot,
) -> bool {
    existing.unique() == candidate.unique()
        && secondary_index_duplicate_contract_match(existing, candidate)
}

fn secondary_index_duplicate_contract_match(
    existing: &PersistedIndexSnapshot,
    candidate: &PersistedIndexSnapshot,
) -> bool {
    existing.predicate_sql() == candidate.predicate_sql() && existing.key() == candidate.key()
}
