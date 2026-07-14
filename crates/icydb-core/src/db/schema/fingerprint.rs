//! Module: db::schema::fingerprint
//! Responsibility: deterministic schema-contract hashing for commit compatibility checks.
//! Does not own: commit marker persistence or recovery orchestration.
//! Boundary: schema identity hashing consumed by commit preparation and replay guards.

use crate::{
    db::{
        codec::{finalize_hash_sha256, new_hash_sha256},
        commit::CommitSchemaFingerprint,
        schema::{
            AcceptedSchemaSnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaRowLayout, SchemaVersion, encode_persisted_schema_snapshot,
        },
    },
    error::InternalError,
};
use sha2::{Digest, Sha256};
const ACCEPTED_SCHEMA_RUNTIME_FINGERPRINT_DOMAIN: &[u8] = b"icydb.accepted-schema.runtime";
const ACCEPTED_SCHEMA_RUNTIME_FINGERPRINT_VERSION: u8 = 1;
const ACCEPTED_SCHEMA_ADMISSION_FINGERPRINT_VERSION: u8 = 1;

/// Compute one accepted-schema fingerprint for runtime cache identity.
///
/// This cache fingerprint follows the accepted persisted snapshot that planning
/// and SQL admission consume at runtime, including accepted index contracts.
/// It intentionally excludes the source-declared schema version; cache callers
/// carry that version beside the fingerprint as a separate identity fact.
pub(in crate::db) fn accepted_schema_cache_fingerprint(
    schema: &AcceptedSchemaSnapshot,
) -> Result<CommitSchemaFingerprint, InternalError> {
    accepted_schema_cache_fingerprint_for_persisted_snapshot(schema.persisted_snapshot())
}

/// Compute one accepted-schema fingerprint for commit marker validation.
///
/// Commit markers must follow the same accepted persisted schema authority as
/// row decode, write validation, and index planning.
pub(in crate::db) fn accepted_commit_schema_fingerprint(
    schema: &AcceptedSchemaSnapshot,
) -> Result<CommitSchemaFingerprint, InternalError> {
    accepted_schema_cache_fingerprint_for_persisted_snapshot(schema.persisted_snapshot())
}

/// Compute the accepted runtime-shape fingerprint for one persisted snapshot.
///
/// Storage uses this while inserting the raw schema payload so later query
/// cache identity can read a method-qualified fingerprint header without
/// decoding the full snapshot.
pub(in crate::db) fn accepted_schema_cache_fingerprint_for_persisted_snapshot(
    schema: &PersistedSchemaSnapshot,
) -> Result<CommitSchemaFingerprint, InternalError> {
    let normalized_schema = schema_with_cache_fingerprint_version(schema);
    let encoded_snapshot = encode_persisted_schema_snapshot(&normalized_schema)?;

    Ok(accepted_schema_cache_fingerprint_from_raw(
        normalized_schema.entity_path(),
        &encoded_snapshot,
    ))
}

/// Compute the accepted-shape fingerprint used by schema-version admission.
///
/// Unlike the runtime cache fingerprint, this intentionally normalizes the
/// declared schema version out of the accepted snapshot before hashing. The
/// version is compared as an adjacent identity fact by admission policy.
pub(in crate::db::schema) fn accepted_schema_admission_fingerprint(
    schema: &PersistedSchemaSnapshot,
) -> Result<CommitSchemaFingerprint, InternalError> {
    let normalized_schema = schema_with_admission_fingerprint_version(schema);
    let encoded_snapshot = encode_persisted_schema_snapshot(&normalized_schema)?;

    Ok(accepted_schema_admission_fingerprint_from_raw(
        normalized_schema.entity_path(),
        &encoded_snapshot,
    ))
}

#[must_use]
pub(in crate::db) const fn accepted_schema_cache_fingerprint_method_version() -> u8 {
    ACCEPTED_SCHEMA_RUNTIME_FINGERPRINT_VERSION
}

#[must_use]
pub(in crate::db::schema) const fn accepted_schema_admission_fingerprint_method_version() -> u8 {
    ACCEPTED_SCHEMA_ADMISSION_FINGERPRINT_VERSION
}

#[must_use]
pub(in crate::db) fn accepted_schema_cache_fingerprint_from_raw(
    entity_path: &str,
    encoded_version_normalized_snapshot: &[u8],
) -> CommitSchemaFingerprint {
    let mut hasher = new_hash_sha256();
    hasher.update(ACCEPTED_SCHEMA_RUNTIME_FINGERPRINT_DOMAIN);
    hasher.update([ACCEPTED_SCHEMA_RUNTIME_FINGERPRINT_VERSION]);
    hash_labeled_str(&mut hasher, "entity_path", entity_path);
    hash_labeled_len(
        &mut hasher,
        "accepted_schema_snapshot_len",
        encoded_version_normalized_snapshot.len(),
    );
    hasher.update(encoded_version_normalized_snapshot);

    truncate_sha256_commit_schema_fingerprint(hasher)
}

#[must_use]
fn accepted_schema_admission_fingerprint_from_raw(
    entity_path: &str,
    encoded_snapshot: &[u8],
) -> CommitSchemaFingerprint {
    let mut hasher = new_hash_sha256();
    hasher.update([ACCEPTED_SCHEMA_ADMISSION_FINGERPRINT_VERSION]);
    hash_labeled_str(&mut hasher, "entity_path", entity_path);
    hash_labeled_len(
        &mut hasher,
        "accepted_schema_admission_snapshot_len",
        encoded_snapshot.len(),
    );
    hasher.update(encoded_snapshot);

    truncate_sha256_commit_schema_fingerprint(hasher)
}

fn schema_with_cache_fingerprint_version(
    schema: &PersistedSchemaSnapshot,
) -> PersistedSchemaSnapshot {
    schema_with_fingerprint_version_and_indexes(schema, schema.indexes().to_vec())
}

fn schema_with_admission_fingerprint_version(
    schema: &PersistedSchemaSnapshot,
) -> PersistedSchemaSnapshot {
    schema_with_fingerprint_version_and_indexes(
        schema,
        schema
            .indexes()
            .iter()
            .map(index_with_admission_fingerprint_name)
            .collect(),
    )
}

fn schema_with_fingerprint_version_and_indexes(
    schema: &PersistedSchemaSnapshot,
    indexes: Vec<PersistedIndexSnapshot>,
) -> PersistedSchemaSnapshot {
    // Canonical hash sentinel only: this is not an inferred persisted version.
    let version = SchemaVersion::initial();
    let row_layout = SchemaRowLayout::new(version, schema.row_layout().field_to_slot().to_vec());

    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        version,
        schema.entity_path().to_string(),
        schema.entity_name().to_string(),
        schema.primary_key_field_ids().to_vec(),
        row_layout,
        schema.fields().to_vec(),
        indexes,
    )
    .with_relations(schema.relations().to_vec())
}

fn index_with_admission_fingerprint_name(index: &PersistedIndexSnapshot) -> PersistedIndexSnapshot {
    let name = if index.generated() {
        format!("generated-index-{}", index.ordinal())
    } else {
        index.name().to_string()
    };

    PersistedIndexSnapshot::new_with_origin(
        index.ordinal(),
        name,
        index.store().to_string(),
        index.unique(),
        index.origin(),
        index.key().clone(),
        index.predicate_sql().map(str::to_string),
    )
}

fn hash_labeled_str(hasher: &mut Sha256, label: &str, value: &str) {
    hash_labeled_len(hasher, label, value.len());
    hasher.update(value.as_bytes());
}

fn hash_labeled_len(hasher: &mut Sha256, label: &str, len: usize) {
    hasher.update(label.as_bytes());
    hasher.update(u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
}

fn truncate_sha256_commit_schema_fingerprint(hasher: Sha256) -> CommitSchemaFingerprint {
    // Keep the persisted commit-marker width stable while moving the contract
    // onto the shared SHA-256 family used by the other semantic fingerprints.
    let digest = finalize_hash_sha256(hasher);
    let mut fingerprint = [0u8; 16];
    let width = fingerprint.len();
    fingerprint.copy_from_slice(&digest[..width]);

    fingerprint
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::{
            AcceptedSchemaSnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot,
            SchemaRowLayout, SchemaVersion, compiled_schema_proposal_for_model,
        },
        model::{
            EntityModel,
            field::{FieldKind, FieldModel},
            index::IndexModel,
        },
    };

    const CONTRACT_INDEX_FIELDS: [&str; 1] = ["value"];

    static CONTRACT_BASE_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("value", FieldKind::Text { max_len: None }),
    ];
    static CONTRACT_EXTRA_FIELDS: [FieldModel; 3] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("value", FieldKind::Text { max_len: None }),
        FieldModel::generated("enabled", FieldKind::Bool),
    ];
    static CONTRACT_INDEX_MODEL: IndexModel = IndexModel::generated(
        "idx_entity__value",
        "entity::value_index",
        &CONTRACT_INDEX_FIELDS,
        false,
    );
    static EMPTY_INDEX_REFS: [&IndexModel; 0] = [];
    static CONTRACT_INDEX_REFS: [&IndexModel; 1] = [&CONTRACT_INDEX_MODEL];
    static CONTRACT_BASE_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        1,
        &CONTRACT_BASE_FIELDS[0],
        0,
        &CONTRACT_BASE_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_EXTRA_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        1,
        &CONTRACT_EXTRA_FIELDS[0],
        0,
        &CONTRACT_EXTRA_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_INDEXED_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        1,
        &CONTRACT_BASE_FIELDS[0],
        0,
        &CONTRACT_BASE_FIELDS,
        &CONTRACT_INDEX_REFS,
    );

    fn snapshot_for_model(model: &EntityModel) -> PersistedSchemaSnapshot {
        compiled_schema_proposal_for_model(model).initial_persisted_schema_snapshot()
    }

    fn snapshot_with_version(
        snapshot: &PersistedSchemaSnapshot,
        version: SchemaVersion,
    ) -> PersistedSchemaSnapshot {
        let row_layout =
            SchemaRowLayout::new(version, snapshot.row_layout().field_to_slot().to_vec());

        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            version,
            snapshot.entity_path().to_string(),
            snapshot.entity_name().to_string(),
            snapshot.primary_key_field_ids().to_vec(),
            row_layout,
            snapshot.fields().to_vec(),
            snapshot.indexes().to_vec(),
        )
        .with_relations(snapshot.relations().to_vec())
    }

    fn snapshot_with_generated_index_name(
        snapshot: &PersistedSchemaSnapshot,
        index_name: &str,
    ) -> PersistedSchemaSnapshot {
        let mut indexes = snapshot.indexes().to_vec();
        let index = &indexes[0];
        indexes[0] = PersistedIndexSnapshot::new_with_origin(
            index.ordinal(),
            index_name.to_string(),
            index.store().to_string(),
            index.unique(),
            index.origin(),
            index.key().clone(),
            index.predicate_sql().map(str::to_string),
        );

        PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
            snapshot.version(),
            snapshot.entity_path().to_string(),
            snapshot.entity_name().to_string(),
            snapshot.primary_key_field_ids().to_vec(),
            snapshot.row_layout().clone(),
            snapshot.fields().to_vec(),
            indexes,
        )
        .with_relations(snapshot.relations().to_vec())
    }

    #[test]
    fn schema_admission_fingerprint_ignores_declared_schema_version() {
        let stored = snapshot_for_model(&CONTRACT_BASE_MODEL);
        let candidate = snapshot_with_version(&stored, SchemaVersion::new(2));

        assert_ne!(stored.version(), candidate.version());
        assert_eq!(
            super::accepted_schema_admission_fingerprint(&stored)
                .expect("stored admission fingerprint should hash"),
            super::accepted_schema_admission_fingerprint(&candidate)
                .expect("candidate admission fingerprint should hash"),
            "declared schema_version is compared beside the admission fingerprint, not inside it",
        );
    }

    #[test]
    fn accepted_schema_cache_fingerprint_ignores_declared_schema_version() {
        let stored = snapshot_for_model(&CONTRACT_BASE_MODEL);
        let candidate = snapshot_with_version(&stored, SchemaVersion::new(2));
        let accepted_stored =
            AcceptedSchemaSnapshot::try_new(stored).expect("stored snapshot should be accepted");
        let accepted_candidate = AcceptedSchemaSnapshot::try_new(candidate)
            .expect("candidate snapshot should be accepted");

        assert_ne!(
            accepted_stored.persisted_snapshot().version(),
            accepted_candidate.persisted_snapshot().version()
        );
        assert_eq!(
            super::accepted_schema_cache_fingerprint(&accepted_stored)
                .expect("stored cache fingerprint should hash"),
            super::accepted_schema_cache_fingerprint(&accepted_candidate)
                .expect("candidate cache fingerprint should hash"),
            "schema_version is carried beside the accepted cache fingerprint, not inside it",
        );
    }

    #[test]
    fn schema_admission_fingerprint_tracks_accepted_shape_contracts() {
        assert_ne!(
            super::accepted_schema_admission_fingerprint(&snapshot_for_model(&CONTRACT_BASE_MODEL))
                .expect("base admission fingerprint should hash"),
            super::accepted_schema_admission_fingerprint(&snapshot_for_model(
                &CONTRACT_EXTRA_MODEL
            ))
            .expect("extra-field admission fingerprint should hash"),
            "field-count changes must change the admission shape fingerprint",
        );
        assert_ne!(
            super::accepted_schema_admission_fingerprint(&snapshot_for_model(&CONTRACT_BASE_MODEL))
                .expect("base admission fingerprint should hash"),
            super::accepted_schema_admission_fingerprint(&snapshot_for_model(
                &CONTRACT_INDEXED_MODEL
            ))
            .expect("indexed admission fingerprint should hash"),
            "accepted index contract changes must change the admission shape fingerprint",
        );
    }

    #[test]
    fn schema_admission_fingerprint_ignores_generated_index_display_name() {
        let indexed = snapshot_for_model(&CONTRACT_INDEXED_MODEL);
        let renamed = snapshot_with_generated_index_name(&indexed, "renamed_generated_index");

        assert_eq!(
            super::accepted_schema_admission_fingerprint(&indexed)
                .expect("indexed admission fingerprint should hash"),
            super::accepted_schema_admission_fingerprint(&renamed)
                .expect("renamed generated-index admission fingerprint should hash"),
            "generated index names remain metadata-only for admission fingerprinting",
        );
    }
}
