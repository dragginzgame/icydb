use super::{AcceptedSchemaMutationError, SchemaMutationRequest};
#[cfg(feature = "sql")]
use super::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlIndexDropCandidateError,
    SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError, SchemaDdlMutationTarget,
    schema_mutation_request_for_snapshots,
};
use crate::db::schema::{
    AcceptedFieldKind, FieldId, PersistedIndexExpressionOp, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    SchemaFieldSlot,
};
#[cfg(feature = "sql")]
use crate::db::schema::{AcceptedSchemaSnapshot, PersistedSchemaSnapshot};

///
/// SchemaFieldPathIndexRebuildTarget
///
/// Accepted schema-owned rebuild target for a field-path index. It carries the
/// persisted index store identity and key-slot contract consumed by the
/// physical runner before the index becomes runtime-visible.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldPathIndexRebuildTarget {
    pub(in crate::db::schema::mutation) ordinal: u16,
    pub(in crate::db::schema::mutation) name: String,
    pub(in crate::db::schema::mutation) store: String,
    pub(in crate::db::schema::mutation) unique: bool,
    pub(in crate::db::schema::mutation) predicate_sql: Option<String>,
    pub(in crate::db::schema::mutation) key_paths: Vec<SchemaFieldPathIndexRebuildKey>,
}

impl SchemaFieldPathIndexRebuildTarget {
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(predicate_sql) => Some(predicate_sql.as_str()),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn key_paths(&self) -> &[SchemaFieldPathIndexRebuildKey] {
        self.key_paths.as_slice()
    }
}

///
/// SchemaFieldPathIndexRebuildKey
///
/// One accepted field-path key component required to rebuild a secondary index
/// from accepted row-layout slots.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldPathIndexRebuildKey {
    pub(in crate::db::schema::mutation) field_id: FieldId,
    pub(in crate::db::schema::mutation) slot: SchemaFieldSlot,
    pub(in crate::db::schema::mutation) path: Vec<String>,
    pub(in crate::db::schema::mutation) kind: AcceptedFieldKind,
    pub(in crate::db::schema::mutation) nullable: bool,
}

impl SchemaFieldPathIndexRebuildKey {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }

    #[must_use]
    pub(in crate::db) const fn path(&self) -> &[String] {
        self.path.as_slice()
    }

    #[must_use]
    pub(in crate::db) fn field_name(&self) -> &str {
        self.path.first().map_or("", String::as_str)
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn nullable(&self) -> bool {
        self.nullable
    }
}

///
/// SchemaExpressionIndexRebuildTarget
///
/// Accepted schema-owned rebuild target for a deterministic expression index.
/// It preserves accepted key order across field-path and expression components
/// so the physical runner does not need generated `IndexModel` metadata to
/// derive key shape.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaExpressionIndexRebuildTarget {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    predicate_sql: Option<String>,
    key_items: Vec<SchemaExpressionIndexRebuildKey>,
}

impl SchemaExpressionIndexRebuildTarget {
    /// Return the accepted dense index ordinal used to encode physical keys.
    #[must_use]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    /// Borrow the accepted index name used for derivation diagnostics.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(predicate_sql) => Some(predicate_sql.as_str()),
            None => None,
        }
    }

    /// Borrow the accepted ordered key-item contract.
    #[must_use]
    pub(in crate::db) const fn key_items(&self) -> &[SchemaExpressionIndexRebuildKey] {
        self.key_items.as_slice()
    }
}

///
/// SchemaExpressionIndexRebuildKey
///
/// Accepted key component required to rebuild deterministic expression indexes.
/// Mixed indexes retain their exact accepted item order.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaExpressionIndexRebuildKey {
    FieldPath(SchemaFieldPathIndexRebuildKey),
    Expression(Box<SchemaExpressionIndexRebuildExpression>),
}

///
/// SchemaExpressionIndexRebuildExpression
///
/// One accepted deterministic expression key component.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaExpressionIndexRebuildExpression {
    op: PersistedIndexExpressionOp,
    source: SchemaFieldPathIndexRebuildKey,
    input_kind: AcceptedFieldKind,
    output_kind: AcceptedFieldKind,
    canonical_text: String,
}

impl SchemaExpressionIndexRebuildExpression {
    /// Return the accepted expression operation.
    #[must_use]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    /// Borrow the accepted source field-path contract.
    #[must_use]
    pub(in crate::db) const fn source(&self) -> &SchemaFieldPathIndexRebuildKey {
        &self.source
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn input_kind(&self) -> &AcceptedFieldKind {
        &self.input_kind
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn output_kind(&self) -> &AcceptedFieldKind {
        &self.output_kind
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn canonical_text(&self) -> &str {
        self.canonical_text.as_str()
    }
}

impl SchemaMutationRequest<'_> {
    /// Lower one accepted field-path index snapshot into a mutation request.
    /// Expression/mixed indexes stay on their dedicated lowering path.
    pub(in crate::db::schema) fn from_accepted_field_path_index(
        index: &PersistedIndexSnapshot,
    ) -> Result<Self, AcceptedSchemaMutationError> {
        let PersistedIndexKeySnapshot::FieldPath(paths) = index.key() else {
            return Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape);
        };

        if paths.is_empty() {
            return Err(AcceptedSchemaMutationError::EmptyIndexKey);
        }

        let key_paths = paths.iter().map(field_path_rebuild_key).collect();

        Ok(Self::AddFieldPathIndex {
            target: SchemaFieldPathIndexRebuildTarget {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store: index.store().to_string(),
                unique: index.unique(),
                predicate_sql: index.predicate_sql().map(str::to_string),
                key_paths,
            },
        })
    }

    /// Lower one accepted deterministic expression index snapshot into a
    /// mutation request. Field-path-only keys and empty keys fail closed
    /// because this path exists only for expression-backed index contracts.
    pub(in crate::db::schema) fn from_accepted_expression_index(
        index: &PersistedIndexSnapshot,
    ) -> Result<Self, AcceptedSchemaMutationError> {
        let PersistedIndexKeySnapshot::Items(items) = index.key() else {
            return Err(AcceptedSchemaMutationError::UnsupportedIndexKeyShape);
        };

        if items.is_empty() {
            return Err(AcceptedSchemaMutationError::EmptyIndexKey);
        }

        let mut has_expression = false;
        let key_items = items
            .iter()
            .map(|item| match item {
                PersistedIndexKeyItemSnapshot::FieldPath(path) => {
                    SchemaExpressionIndexRebuildKey::FieldPath(field_path_rebuild_key(path))
                }
                PersistedIndexKeyItemSnapshot::Expression(expression) => {
                    has_expression = true;
                    SchemaExpressionIndexRebuildKey::Expression(Box::new(
                        SchemaExpressionIndexRebuildExpression {
                            op: expression.op(),
                            source: field_path_rebuild_key(expression.source()),
                            input_kind: expression.input_kind().clone(),
                            output_kind: expression.output_kind().clone(),
                            canonical_text: expression.canonical_text().to_string(),
                        },
                    ))
                }
            })
            .collect();

        if !has_expression {
            return Err(AcceptedSchemaMutationError::ExpressionIndexRequiresExpressionKey);
        }

        Ok(Self::AddExpressionIndex {
            target: SchemaExpressionIndexRebuildTarget {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store: index.store().to_string(),
                unique: index.unique(),
                predicate_sql: index.predicate_sql().map(str::to_string),
                key_items,
            },
        })
    }
}

/// Admit one SQL DDL field-path index candidate through the schema-owned
/// mutation request and supported-runner path.
#[cfg(all(test, feature = "sql"))]
pub(in crate::db) fn admit_sql_ddl_field_path_index_candidate(
    index: &PersistedIndexSnapshot,
) -> Result<SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError> {
    let request = SchemaMutationRequest::from_accepted_field_path_index(index)
        .map_err(SchemaDdlMutationAdmissionError::AcceptedIndex)?;
    let SchemaMutationRequest::AddFieldPathIndex { target } = request else {
        return Err(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath);
    };

    Ok(SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::FieldPathAddition(target),
    })
}

/// Admit one SQL DDL expression index candidate through the schema-owned
/// mutation request boundary.
#[cfg(feature = "sql")]
pub(in crate::db) fn admit_sql_ddl_expression_index_candidate(
    index: &PersistedIndexSnapshot,
) -> Result<SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError> {
    let request = SchemaMutationRequest::from_accepted_expression_index(index)
        .map_err(SchemaDdlMutationAdmissionError::AcceptedIndex)?;
    let SchemaMutationRequest::AddExpressionIndex { target } = request else {
        return Err(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath);
    };

    Ok(SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::ExpressionAddition(target),
    })
}

/// Admit one SQL DDL secondary-index drop candidate through the schema-owned
/// mutation request boundary.
#[cfg(feature = "sql")]
pub(in crate::db) const fn admit_sql_ddl_secondary_index_drop_candidate()
-> SchemaDdlMutationAdmission {
    SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::SecondaryDrop,
    }
}

/// Resolve one accepted SQL DDL index-drop candidate and reject generated
/// accepted indexes before the frontend can derive a catalog mutation.
#[cfg(feature = "sql")]
pub(in crate::db) fn resolve_sql_ddl_secondary_index_drop_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    index_name: &str,
) -> Result<(PersistedIndexSnapshot, Vec<String>), SchemaDdlIndexDropCandidateError> {
    let index = accepted_before
        .persisted_snapshot()
        .indexes()
        .iter()
        .find(|index| index.name() == index_name)
        .cloned()
        .ok_or(SchemaDdlIndexDropCandidateError::Unknown)?;
    if index.generated() {
        return Err(SchemaDdlIndexDropCandidateError::Generated);
    }
    let field_path = ddl_drop_index_key_report(index.key())
        .ok_or(SchemaDdlIndexDropCandidateError::Unsupported)?;

    Ok((index, field_path))
}

#[cfg(any(test, feature = "sql"))]
fn ddl_drop_index_key_report(key: &PersistedIndexKeySnapshot) -> Option<Vec<String>> {
    match key {
        PersistedIndexKeySnapshot::FieldPath(field_paths) => match field_paths.as_slice() {
            [] => None,
            [field_path] => Some(field_path.path().to_vec()),
            _ => Some(vec![
                field_paths
                    .iter()
                    .map(|field_path| field_path.path().join("."))
                    .collect::<Vec<_>>()
                    .join(","),
            ]),
        },
        PersistedIndexKeySnapshot::Items(items) => match items.as_slice() {
            [] => None,
            [item] => Some(vec![ddl_drop_index_key_item_text(item)]),
            _ => Some(vec![
                items
                    .iter()
                    .map(ddl_drop_index_key_item_text)
                    .collect::<Vec<_>>()
                    .join(","),
            ]),
        },
    }
}

#[cfg(any(test, feature = "sql"))]
fn ddl_drop_index_key_item_text(item: &PersistedIndexKeyItemSnapshot) -> String {
    match item {
        PersistedIndexKeyItemSnapshot::FieldPath(field_path) => field_path.path().join("."),
        PersistedIndexKeyItemSnapshot::Expression(expression) => expression
            .canonical_text()
            .trim_start_matches("expr:v1:")
            .to_string(),
    }
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// field-path index candidate.
#[cfg(feature = "sql")]
pub(in crate::db) fn derive_sql_ddl_field_path_index_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    index: PersistedIndexSnapshot,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let mut indexes = before.indexes().to_vec();
    indexes.push(index);
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        before.fields().to_vec(),
        indexes,
    )
    .with_relations(before.relations().to_vec());
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let request = schema_mutation_request_for_snapshots(
        accepted_before.persisted_snapshot(),
        accepted_after.persisted_snapshot(),
    )
    .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;
    let SchemaMutationRequest::AddFieldPathIndex { target } = request else {
        return Err(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath);
    };

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission: SchemaDdlMutationAdmission {
            target: SchemaDdlMutationTarget::FieldPathAddition(target),
        },
    })
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// expression index candidate.
#[cfg(feature = "sql")]
pub(in crate::db) fn derive_sql_ddl_expression_index_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    index: PersistedIndexSnapshot,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let mut indexes = before.indexes().to_vec();
    indexes.push(index.clone());
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        before.fields().to_vec(),
        indexes,
    )
    .with_relations(before.relations().to_vec());
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_expression_index_candidate(&index)?;

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// secondary-index drop candidate.
#[cfg(feature = "sql")]
pub(in crate::db) fn derive_sql_ddl_secondary_index_drop_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    index: &PersistedIndexSnapshot,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let indexes = before
        .indexes()
        .iter()
        .filter(|candidate| candidate.name() != index.name())
        .enumerate()
        .map(|(offset, candidate)| {
            let ordinal = u16::try_from(offset)
                .ok()
                .and_then(|offset| offset.checked_add(1))
                .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;
            candidate
                .clone_with_dense_identities(ordinal, |field_id, slot| Some((field_id, slot)))
                .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)
        })
        .collect::<Result<Vec<_>, _>>()?;
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        before.fields().to_vec(),
        indexes,
    )
    .with_relations(before.relations().to_vec());
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_secondary_index_drop_candidate();

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}

pub(super) fn field_path_rebuild_key(
    path: &PersistedIndexFieldPathSnapshot,
) -> SchemaFieldPathIndexRebuildKey {
    SchemaFieldPathIndexRebuildKey {
        field_id: path.field_id(),
        slot: path.slot(),
        path: path.path().to_vec(),
        kind: path.kind().clone(),
        nullable: path.nullable(),
    }
}
