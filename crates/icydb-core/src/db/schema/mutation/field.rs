use super::{
    SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmission,
    SchemaDdlMutationAdmissionError, SchemaDdlMutationTarget,
};
use crate::db::schema::{
    AcceptedSchemaSnapshot, FieldId, PersistedFieldSnapshot, PersistedSchemaSnapshot,
    SchemaFieldDefault, SchemaFieldSlot, SchemaRowLayout,
};

///
/// SchemaFieldAdditionTarget
///
/// Accepted additive-field target admitted for SQL DDL publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldAdditionTarget {
    field_id: FieldId,
    name: String,
    slot: SchemaFieldSlot,
}

impl SchemaFieldAdditionTarget {
    /// Build one additive-field DDL target from accepted field metadata.
    #[must_use]
    fn from_field(field: &PersistedFieldSnapshot) -> Self {
        Self {
            field_id: field.id(),
            name: field.name().to_string(),
            slot: field.slot(),
        }
    }

    /// Return the accepted field ID.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the accepted row slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }
}

///
/// SchemaFieldDropTarget
///
/// Accepted retained-slot field removal target admitted for SQL DDL publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldDropTarget {
    field_id: FieldId,
    name: String,
    slot: SchemaFieldSlot,
}

impl SchemaFieldDropTarget {
    /// Build one field-drop DDL target from accepted field metadata.
    #[must_use]
    fn from_field(field: &PersistedFieldSnapshot) -> Self {
        Self {
            field_id: field.id(),
            name: field.name().to_string(),
            slot: field.slot(),
        }
    }

    /// Return the accepted field ID.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the retired accepted row slot.
    #[must_use]
    pub(in crate::db) const fn slot(&self) -> SchemaFieldSlot {
        self.slot
    }
}

///
/// SchemaFieldDefaultTarget
///
/// Accepted field-default metadata target admitted for SQL DDL publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldDefaultTarget {
    field_id: FieldId,
    name: String,
}

impl SchemaFieldDefaultTarget {
    /// Build one field-default DDL target from accepted field metadata.
    #[must_use]
    fn from_field(field: &PersistedFieldSnapshot) -> Self {
        Self {
            field_id: field.id(),
            name: field.name().to_string(),
        }
    }

    /// Return the accepted field ID.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }
}

///
/// SchemaFieldNullabilityTarget
///
/// Accepted field-nullability metadata target admitted for SQL DDL publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldNullabilityTarget {
    field_id: FieldId,
    name: String,
}

impl SchemaFieldNullabilityTarget {
    /// Build one field-nullability DDL target from accepted field metadata.
    #[must_use]
    fn from_field(field: &PersistedFieldSnapshot) -> Self {
        Self {
            field_id: field.id(),
            name: field.name().to_string(),
        }
    }

    /// Return the accepted field ID.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Borrow the accepted field name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }
}

///
/// SchemaFieldRenameTarget
///
/// Accepted field-name metadata target admitted for SQL DDL publication.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct SchemaFieldRenameTarget {
    field_id: FieldId,
    old_name: String,
    new_name: String,
}

impl SchemaFieldRenameTarget {
    /// Build one field-rename DDL target from accepted before metadata and
    /// schema-owned target naming.
    #[must_use]
    fn from_field_name(before: &PersistedFieldSnapshot, new_name: &str) -> Self {
        Self {
            field_id: before.id(),
            old_name: before.name().to_string(),
            new_name: new_name.to_string(),
        }
    }

    /// Return the accepted field ID.
    #[must_use]
    pub(in crate::db) const fn field_id(&self) -> FieldId {
        self.field_id
    }

    /// Borrow the accepted source field name.
    #[must_use]
    pub(in crate::db) const fn old_name(&self) -> &str {
        self.old_name.as_str()
    }

    /// Borrow the accepted target field name.
    #[must_use]
    pub(in crate::db) const fn new_name(&self) -> &str {
        self.new_name.as_str()
    }
}

/// Field drop candidate resolution failures for SQL DDL-authored schema
/// mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlFieldDropCandidateError {
    /// No accepted field matches the requested SQL column name.
    Unknown,
    /// The requested accepted field is part of the primary key.
    PrimaryKey,
    /// The requested accepted field is generated-owned.
    Generated,
    /// The requested accepted field is still referenced by an accepted index.
    Indexed(String),
}

/// Field default candidate resolution failures for SQL DDL-authored schema
/// mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlFieldDefaultCandidateError {
    /// No accepted field matches the requested SQL column name.
    Unknown,
    /// The requested accepted field is generated-owned.
    Generated,
    /// The requested accepted field is required and currently has a default.
    Required,
}

/// Field nullability candidate resolution failures for SQL DDL-authored schema
/// mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlFieldNullabilityCandidateError {
    /// No accepted field matches the requested SQL column name.
    Unknown,
    /// The requested accepted field is generated-owned.
    Generated,
}

/// Field rename candidate resolution failures for SQL DDL-authored schema
/// mutations.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum SchemaDdlFieldRenameCandidateError {
    /// No accepted source field matches the requested SQL column name.
    Unknown,
    /// An accepted field already uses the requested target column name.
    Duplicate,
    /// The requested accepted field is generated-owned.
    Generated,
}

/// Admit one SQL DDL field addition through the schema-owned mutation request
/// boundary. Publication policy is validated against the full accepted-before
/// and accepted-after snapshots before execution stores the derived snapshot.
pub(in crate::db) fn admit_sql_ddl_field_addition_candidate(
    field: &PersistedFieldSnapshot,
) -> SchemaDdlMutationAdmission {
    SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::FieldAddition(SchemaFieldAdditionTarget::from_field(
            field,
        )),
    }
}

/// Admit one SQL DDL retained-slot field drop.
#[must_use]
pub(in crate::db) fn admit_sql_ddl_field_drop_candidate(
    field: &PersistedFieldSnapshot,
) -> SchemaDdlMutationAdmission {
    SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::FieldDrop(SchemaFieldDropTarget::from_field(field)),
    }
}

/// Admit one SQL DDL field-default metadata change.
pub(in crate::db) fn admit_sql_ddl_field_default_candidate(
    field: &PersistedFieldSnapshot,
) -> SchemaDdlMutationAdmission {
    SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::FieldDefaultChange(SchemaFieldDefaultTarget::from_field(
            field,
        )),
    }
}

/// Admit one SQL DDL field-nullability metadata candidate.
#[must_use]
pub(in crate::db) fn admit_sql_ddl_field_nullability_candidate(
    field: &PersistedFieldSnapshot,
) -> SchemaDdlMutationAdmission {
    SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::FieldNullabilityChange(
            SchemaFieldNullabilityTarget::from_field(field),
        ),
    }
}

/// Admit one SQL DDL field-rename metadata candidate.
#[must_use]
pub(in crate::db) fn admit_sql_ddl_field_rename_candidate(
    before: &PersistedFieldSnapshot,
    new_name: &str,
) -> SchemaDdlMutationAdmission {
    SchemaDdlMutationAdmission {
        target: SchemaDdlMutationTarget::FieldRename(SchemaFieldRenameTarget::from_field_name(
            before, new_name,
        )),
    }
}

fn resolve_sql_ddl_field_drop_dependent_index(
    accepted_before: &AcceptedSchemaSnapshot,
    field_id: FieldId,
) -> Option<String> {
    accepted_before
        .persisted_snapshot()
        .indexes()
        .iter()
        .find(|index| index.key().references_field(field_id))
        .map(|index| index.name().to_string())
}

/// Resolve one accepted SQL DDL field-drop candidate and reject primary-key,
/// generated-owned, or index-referenced fields before the frontend can derive a
/// catalog mutation.
pub(in crate::db) fn resolve_sql_ddl_field_drop_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    field_name: &str,
) -> Result<PersistedFieldSnapshot, SchemaDdlFieldDropCandidateError> {
    let accepted = accepted_before.persisted_snapshot();
    let field = accepted
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(SchemaDdlFieldDropCandidateError::Unknown)?;

    if accepted.primary_key_field_ids().contains(&field.id()) {
        return Err(SchemaDdlFieldDropCandidateError::PrimaryKey);
    }

    if field.generated() {
        return Err(SchemaDdlFieldDropCandidateError::Generated);
    }

    if let Some(index_name) =
        resolve_sql_ddl_field_drop_dependent_index(accepted_before, field.id())
    {
        return Err(SchemaDdlFieldDropCandidateError::Indexed(index_name));
    }

    Ok(field.clone())
}

/// Resolve one accepted SQL DDL SET DEFAULT candidate and reject
/// generated-owned fields before default encoding or mutation derivation.
pub(in crate::db) fn resolve_sql_ddl_field_set_default_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    field_name: &str,
) -> Result<PersistedFieldSnapshot, SchemaDdlFieldDefaultCandidateError> {
    let field = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(SchemaDdlFieldDefaultCandidateError::Unknown)?;

    if field.generated() {
        return Err(SchemaDdlFieldDefaultCandidateError::Generated);
    }

    Ok(field.clone())
}

/// Resolve one accepted SQL DDL DROP DEFAULT candidate. Missing defaults are
/// returned so SQL can report the existing true no-op behavior, while real
/// generated-owned or required-field changes reject before mutation derivation.
pub(in crate::db) fn resolve_sql_ddl_field_drop_default_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    field_name: &str,
) -> Result<PersistedFieldSnapshot, SchemaDdlFieldDefaultCandidateError> {
    let field = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(SchemaDdlFieldDefaultCandidateError::Unknown)?;

    if field.default().is_none() {
        return Ok(field.clone());
    }

    if field.generated() {
        return Err(SchemaDdlFieldDefaultCandidateError::Generated);
    }

    if !field.nullable() {
        return Err(SchemaDdlFieldDefaultCandidateError::Required);
    }

    Ok(field.clone())
}

/// Resolve one accepted SQL DDL nullability candidate. Matching nullability is
/// returned so SQL can preserve true no-op behavior before generated ownership
/// rejects only actual changes.
pub(in crate::db) fn resolve_sql_ddl_field_nullability_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    field_name: &str,
    nullable: bool,
) -> Result<PersistedFieldSnapshot, SchemaDdlFieldNullabilityCandidateError> {
    let field = accepted_before
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(SchemaDdlFieldNullabilityCandidateError::Unknown)?;

    if field.nullable() != nullable && field.generated() {
        return Err(SchemaDdlFieldNullabilityCandidateError::Generated);
    }

    Ok(field.clone())
}

/// Resolve one accepted SQL DDL field-rename candidate. Same-name renames are
/// returned as true no-ops before generated ownership rejects actual renames.
pub(in crate::db) fn resolve_sql_ddl_field_rename_candidate(
    accepted_before: &AcceptedSchemaSnapshot,
    old_name: &str,
    new_name: &str,
) -> Result<PersistedFieldSnapshot, SchemaDdlFieldRenameCandidateError> {
    let accepted = accepted_before.persisted_snapshot();
    let field = accepted
        .fields()
        .iter()
        .find(|field| field.name() == old_name)
        .ok_or(SchemaDdlFieldRenameCandidateError::Unknown)?;

    if old_name == new_name {
        return Ok(field.clone());
    }

    if accepted
        .fields()
        .iter()
        .any(|field| field.name() == new_name)
    {
        return Err(SchemaDdlFieldRenameCandidateError::Duplicate);
    }

    if field.generated() {
        return Err(SchemaDdlFieldRenameCandidateError::Generated);
    }

    Ok(field.clone())
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// additive field candidate.
pub(in crate::db) fn derive_sql_ddl_field_addition_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    field: PersistedFieldSnapshot,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let mut fields = before.fields().to_vec();
    fields.push(field.clone());
    let mut field_to_slot = before.row_layout().field_to_slot().to_vec();
    field_to_slot.push((field.id(), field.slot()));
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        SchemaRowLayout::new_with_retired_slots(
            before.row_layout().version(),
            field_to_slot,
            before.row_layout().retired_field_slots().to_vec(),
        ),
        fields,
        before.indexes().to_vec(),
    );
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_field_addition_candidate(&field);

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// retained-slot field drop.
pub(in crate::db) fn derive_sql_ddl_field_drop_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    field_name: &str,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let before_field = before
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;
    let fields = before
        .fields()
        .iter()
        .filter(|field| field.id() != before_field.id())
        .cloned()
        .collect();
    let row_layout = before
        .row_layout()
        .clone_retiring_field(before_field.id())
        .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        row_layout,
        fields,
        before.indexes().to_vec(),
    );
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_field_drop_candidate(before_field);

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// field-default metadata change.
pub(in crate::db) fn derive_sql_ddl_field_default_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    field_name: &str,
    default: SchemaFieldDefault,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let before_field = before
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;
    let fields = before
        .fields()
        .iter()
        .map(|field| {
            if field.id() == before_field.id() {
                field.clone_with_default(default.clone())
            } else {
                field.clone()
            }
        })
        .collect();
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        fields,
        before.indexes().to_vec(),
    );
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let after_field = accepted_after
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.id() == before_field.id())
        .ok_or(SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_field_default_candidate(after_field);

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// field-nullability metadata change.
pub(in crate::db) fn derive_sql_ddl_field_nullability_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    field_name: &str,
    nullable: bool,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let before_field = before
        .fields()
        .iter()
        .find(|field| field.name() == field_name)
        .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;
    let fields = before
        .fields()
        .iter()
        .map(|field| {
            if field.id() == before_field.id() {
                field.clone_with_nullable(nullable)
            } else {
                field.clone()
            }
        })
        .collect();
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        fields,
        before.indexes().to_vec(),
    );
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let after_field = accepted_after
        .persisted_snapshot()
        .fields()
        .iter()
        .find(|field| field.id() == before_field.id())
        .ok_or(SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_field_nullability_candidate(after_field);

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}

/// Derive and admit the accepted-after schema snapshot for one SQL DDL
/// field-rename metadata change.
pub(in crate::db) fn derive_sql_ddl_field_rename_accepted_after(
    accepted_before: &AcceptedSchemaSnapshot,
    old_name: &str,
    new_name: &str,
) -> Result<SchemaDdlAcceptedSnapshotDerivation, SchemaDdlMutationAdmissionError> {
    let before = accepted_before.persisted_snapshot();
    let before_field = before
        .fields()
        .iter()
        .find(|field| field.name() == old_name)
        .ok_or(SchemaDdlMutationAdmissionError::UnsupportedExecutionPath)?;
    let fields = before
        .fields()
        .iter()
        .map(|field| {
            if field.id() == before_field.id() {
                field.clone_with_name(new_name.to_string())
            } else {
                field.clone()
            }
        })
        .collect();
    let indexes = before
        .indexes()
        .iter()
        .map(|index| {
            index.clone_with_renamed_field_path_root(
                before_field.id(),
                before_field.name(),
                new_name,
            )
        })
        .collect();
    let persisted_after = PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        before.version(),
        before.entity_path().to_string(),
        before.entity_name().to_string(),
        before.primary_key_field_ids().to_vec(),
        before.row_layout().clone(),
        fields,
        indexes,
    );
    let accepted_after = AcceptedSchemaSnapshot::try_new(persisted_after)
        .map_err(|_| SchemaDdlMutationAdmissionError::AcceptedAfterRejected)?;
    let admission = admit_sql_ddl_field_rename_candidate(before_field, new_name);

    Ok(SchemaDdlAcceptedSnapshotDerivation {
        accepted_after,
        admission,
    })
}
