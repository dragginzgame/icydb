#[cfg(test)]
use super::write_hash_bool;
use super::{AcceptedSchemaMutationError, SchemaMutationRequest};
#[cfg(feature = "sql")]
use super::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlIndexDropCandidateError,
    SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError, SchemaDdlMutationTarget,
    schema_mutation_request_for_snapshots,
};
#[cfg(test)]
use crate::db::codec::{write_hash_str_u32, write_hash_tag_u8, write_hash_u32};
#[cfg(feature = "sql")]
use crate::db::schema::{AcceptedSchemaSnapshot, PersistedSchemaSnapshot};
use crate::db::schema::{
    FieldId, PersistedFieldKind, PersistedIndexExpressionOp, PersistedIndexFieldPathSnapshot,
    PersistedIndexKeyItemSnapshot, PersistedIndexKeySnapshot, PersistedIndexSnapshot,
    SchemaFieldSlot,
};

///
/// SchemaFieldPathIndexRebuildTarget
///
/// Accepted schema-owned rebuild target for a field-path index. It carries the
/// persisted index store identity and key-slot contract that a later physical
/// rebuild runner must consume before the index can become runtime-visible.
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
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    #[must_use]
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
    pub(in crate::db::schema::mutation) kind: PersistedFieldKind,
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
    pub(in crate::db) const fn kind(&self) -> &PersistedFieldKind {
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
/// so a later physical rebuild runner does not need generated `IndexModel`
/// metadata to derive key shape.
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
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn store(&self) -> &str {
        self.store.as_str()
    }

    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn unique(&self) -> bool {
        self.unique
    }

    #[cfg(any(test, feature = "sql"))]
    #[must_use]
    pub(in crate::db) const fn predicate_sql(&self) -> Option<&str> {
        match &self.predicate_sql {
            Some(predicate_sql) => Some(predicate_sql.as_str()),
            None => None,
        }
    }

    #[must_use]
    #[cfg(any(test, feature = "sql"))]
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
    input_kind: PersistedFieldKind,
    output_kind: PersistedFieldKind,
    canonical_text: String,
}

impl SchemaExpressionIndexRebuildExpression {
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn op(&self) -> PersistedIndexExpressionOp {
        self.op
    }

    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn source(&self) -> &SchemaFieldPathIndexRebuildKey {
        &self.source
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn input_kind(&self) -> &PersistedFieldKind {
        &self.input_kind
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn output_kind(&self) -> &PersistedFieldKind {
        &self.output_kind
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn canonical_text(&self) -> &str {
        self.canonical_text.as_str()
    }
}

///
/// SchemaSecondaryIndexDropCleanupTarget
///
/// Accepted schema-owned cleanup target for dropping a secondary index. It
/// carries the persisted store identity that must be cleaned before a later
/// mutation can publish a snapshot without the index.
///

#[derive(Clone, Debug, Eq, PartialEq)]
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) struct SchemaSecondaryIndexDropCleanupTarget {
    ordinal: u16,
    name: String,
    store: String,
    unique: bool,
    predicate_sql: Option<String>,
}

#[cfg(any(test, feature = "sql"))]
impl SchemaSecondaryIndexDropCleanupTarget {
    #[must_use]
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db) const fn ordinal(&self) -> u16 {
        self.ordinal
    }

    #[must_use]
    #[cfg(test)]
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

    /// Lower one accepted secondary index snapshot into a cleanup request.
    #[cfg(any(test, feature = "sql"))]
    pub(in crate::db::schema) fn from_accepted_secondary_index_drop(
        index: &PersistedIndexSnapshot,
    ) -> Self {
        Self::DropNonRequiredSecondaryIndex {
            target: SchemaSecondaryIndexDropCleanupTarget {
                ordinal: index.ordinal(),
                name: index.name().to_string(),
                store: index.store().to_string(),
                unique: index.unique(),
                predicate_sql: index.predicate_sql().map(str::to_string),
            },
        }
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
    let plan = request.lower_to_plan();
    let supported = plan
        .supported_developer_physical_path()
        .map_err(|_| SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;

    Ok(SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::FieldPathAddition(supported.target().clone()),
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
pub(in crate::db) fn admit_sql_ddl_secondary_index_drop_candidate(
    index: &PersistedIndexSnapshot,
) -> Result<SchemaDdlMutationAdmission, SchemaDdlMutationAdmissionError> {
    let request = SchemaMutationRequest::from_accepted_secondary_index_drop(index);
    let SchemaMutationRequest::DropNonRequiredSecondaryIndex { target } = request else {
        return Err(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath);
    };

    Ok(SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::SecondaryDrop(target),
    })
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
    );
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let request = schema_mutation_request_for_snapshots(
        accepted_before.persisted_snapshot(),
        accepted_after.persisted_snapshot(),
    );
    let plan = request.lower_to_plan();
    let supported = plan
        .supported_developer_physical_path()
        .map_err(|_| SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission: SchemaDdlMutationAdmission {
            target: SchemaDdlMutationTarget::FieldPathAddition(supported.target().clone()),
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
    );
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
        .cloned()
        .collect::<Vec<_>>();
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        before.fields().to_vec(),
        indexes,
    );
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_secondary_index_drop_candidate(index)?;

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}

impl SchemaFieldPathIndexRebuildTarget {
    #[cfg(test)]
    pub(super) fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, u32::from(self.ordinal));
        write_hash_str_u32(hasher, &self.name);
        write_hash_str_u32(hasher, &self.store);
        write_hash_bool(hasher, self.unique);
        match &self.predicate_sql {
            Some(predicate_sql) => {
                write_hash_tag_u8(hasher, 1);
                write_hash_str_u32(hasher, predicate_sql);
            }
            None => write_hash_tag_u8(hasher, 0),
        }
        write_hash_u32(
            hasher,
            u32::try_from(self.key_paths.len()).unwrap_or(u32::MAX),
        );
        for key_path in &self.key_paths {
            key_path.hash_into(hasher);
        }
    }
}

impl SchemaFieldPathIndexRebuildKey {
    #[cfg(test)]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, self.field_id.get());
        write_hash_u32(hasher, u32::from(self.slot.get()));
        write_hash_u32(hasher, u32::try_from(self.path.len()).unwrap_or(u32::MAX));
        for segment in &self.path {
            write_hash_str_u32(hasher, segment);
        }
        write_hash_str_u32(hasher, &format!("{:?}", self.kind));
        write_hash_bool(hasher, self.nullable);
    }
}

impl SchemaExpressionIndexRebuildTarget {
    #[cfg(test)]
    pub(super) fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, u32::from(self.ordinal));
        write_hash_str_u32(hasher, &self.name);
        write_hash_str_u32(hasher, &self.store);
        write_hash_bool(hasher, self.unique);
        match &self.predicate_sql {
            Some(predicate_sql) => {
                write_hash_tag_u8(hasher, 1);
                write_hash_str_u32(hasher, predicate_sql);
            }
            None => write_hash_tag_u8(hasher, 0),
        }
        write_hash_u32(
            hasher,
            u32::try_from(self.key_items.len()).unwrap_or(u32::MAX),
        );
        for key_item in &self.key_items {
            key_item.hash_into(hasher);
        }
    }
}

impl SchemaExpressionIndexRebuildKey {
    #[cfg(test)]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        match self {
            Self::FieldPath(path) => {
                write_hash_tag_u8(hasher, 1);
                path.hash_into(hasher);
            }
            Self::Expression(expression) => {
                write_hash_tag_u8(hasher, 2);
                expression.hash_into(hasher);
            }
        }
    }
}

impl SchemaExpressionIndexRebuildExpression {
    #[cfg(test)]
    fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, self.op as u32);
        self.source.hash_into(hasher);
        write_hash_str_u32(hasher, &format!("{:?}", self.input_kind));
        write_hash_str_u32(hasher, &format!("{:?}", self.output_kind));
        write_hash_str_u32(hasher, &self.canonical_text);
    }
}

#[cfg(test)]
impl SchemaSecondaryIndexDropCleanupTarget {
    pub(super) fn hash_into(&self, hasher: &mut sha2::Sha256) {
        write_hash_u32(hasher, u32::from(self.ordinal));
        write_hash_str_u32(hasher, &self.name);
        write_hash_str_u32(hasher, &self.store);
        write_hash_bool(hasher, self.unique);
        match &self.predicate_sql {
            Some(predicate_sql) => {
                write_hash_tag_u8(hasher, 1);
                write_hash_str_u32(hasher, predicate_sql);
            }
            None => write_hash_tag_u8(hasher, 0),
        }
    }
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
