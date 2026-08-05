//! Module: db::schema::fingerprint
//! Responsibility: deterministic schema-contract hashing for commit compatibility checks.
//! Does not own: commit marker persistence or recovery orchestration.
//! Boundary: schema identity hashing consumed by commit preparation and replay guards.

use crate::{
    db::{
        codec::{finalize_hash_sha256, new_hash_sha256},
        commit::CommitSchemaFingerprint,
        schema::{
            AcceptedSchemaSnapshot, PersistedIndexSnapshot, PersistedSchemaSnapshot, SchemaVersion,
            encode_persisted_schema_snapshot,
        },
    },
    error::InternalError,
};
use sha2::{Digest, Sha256};
#[cfg(test)]
use std::cell::Cell;
const ACCEPTED_SCHEMA_RUNTIME_FINGERPRINT_DOMAIN: &[u8] = b"icydb.accepted-schema.runtime";
const ACCEPTED_SCHEMA_RUNTIME_FINGERPRINT_VERSION: u8 = 1;
const ACCEPTED_SCHEMA_ADMISSION_FINGERPRINT_VERSION: u8 = 1;

#[cfg(test)]
thread_local! {
    static ACCEPTED_SCHEMA_SNAPSHOT_FINGERPRINT_BUILDS: Cell<u64> = const { Cell::new(0) };
}

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
    #[cfg(test)]
    ACCEPTED_SCHEMA_SNAPSHOT_FINGERPRINT_BUILDS.with(|builds| {
        builds.set(builds.get().saturating_add(1));
    });
    let normalized_schema = schema_with_cache_fingerprint_version(schema);
    let encoded_snapshot = encode_persisted_schema_snapshot(&normalized_schema)?;

    Ok(accepted_schema_cache_fingerprint_from_raw(
        normalized_schema.entity_path(),
        &encoded_snapshot,
    ))
}

#[cfg(test)]
pub(in crate::db) fn reset_accepted_schema_snapshot_fingerprint_builds_for_tests() {
    ACCEPTED_SCHEMA_SNAPSHOT_FINGERPRINT_BUILDS.with(|builds| builds.set(0));
}

#[cfg(test)]
#[must_use]
pub(in crate::db) fn accepted_schema_snapshot_fingerprint_builds_for_tests() -> u64 {
    ACCEPTED_SCHEMA_SNAPSHOT_FINGERPRINT_BUILDS.with(Cell::get)
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
    let row_layout = schema.row_layout().clone();

    PersistedSchemaSnapshot::new_with_primary_key_fields_and_indexes(
        version,
        schema.entity_path().to_string(),
        schema.entity_name().to_string(),
        schema.primary_key_field_ids().to_vec(),
        row_layout,
        schema.fields().to_vec(),
        indexes,
    )
    .with_constraint_catalog(schema.constraint_catalog().clone())
    .with_relations(schema.relations().to_vec())
    .with_constraint_candidates(
        schema.candidate_indexes().to_vec(),
        schema.candidate_relations().to_vec(),
    )
}

fn index_with_admission_fingerprint_name(index: &PersistedIndexSnapshot) -> PersistedIndexSnapshot {
    let name = if index.generated() {
        format!("generated-index-{}", index.ordinal())
    } else {
        index.name().to_string()
    };

    index_with_identity_and_name(index, index.schema_id(), name)
}

fn index_with_identity_and_name(
    index: &PersistedIndexSnapshot,
    schema_id: crate::db::schema::SchemaIndexId,
    name: String,
) -> PersistedIndexSnapshot {
    let renamed = match index.origin() {
        crate::db::schema::PersistedIndexOrigin::Generated => PersistedIndexSnapshot::new(
            schema_id,
            index.ordinal(),
            name,
            index.store().to_string(),
            index.unique(),
            index.key().clone(),
            index.predicate_sql().map(str::to_string),
        ),
        crate::db::schema::PersistedIndexOrigin::SqlDdl => PersistedIndexSnapshot::new_sql_ddl(
            schema_id,
            index.ordinal(),
            name,
            index.store().to_string(),
            index.unique(),
            index.key().clone(),
            index.predicate_sql().map(str::to_string),
        ),
    };
    renamed.clone_with_schema_identity(schema_id, index.ordinal(), index.physical_generation())
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
