//! Module: db::schema::fingerprint
//! Responsibility: deterministic schema-contract hashing for commit compatibility checks.
//! Does not own: commit marker persistence or recovery orchestration.
//! Boundary: schema identity hashing consumed by commit preparation and replay guards.

use crate::{db::commit::CommitSchemaFingerprint, model::EntityModel, traits::EntityKind};
use canic_utils::hash::Xxh3;

const COMMIT_SCHEMA_FINGERPRINT_VERSION: u8 = 1;

/// Compute one deterministic schema/index fingerprint for an entity commit planner.
#[must_use]
pub(in crate::db) fn commit_schema_fingerprint_for_entity<E: EntityKind>() -> CommitSchemaFingerprint
{
    // Phase 1: version the fingerprint contract and hash top-level identity.
    let mut hasher = Xxh3::with_seed(0);
    hasher.update(&[COMMIT_SCHEMA_FINGERPRINT_VERSION]);
    hash_labeled_str(&mut hasher, "entity_path", E::PATH);

    // Phase 2: hash the macro-generated entity schema contract consumed by
    // prepare/replay planning (field slot order + index definitions).
    hash_entity_model_for_commit(&mut hasher, E::MODEL);

    hasher.digest128().to_be_bytes()
}

fn hash_entity_model_for_commit(hasher: &mut Xxh3, model: &EntityModel) {
    // Phase 1: hash core entity identity and field-shape contract.
    hash_labeled_str(hasher, "model_path", model.path);
    hash_labeled_str(hasher, "entity_name", model.entity_name);
    hash_labeled_str(hasher, "primary_key", model.primary_key.name);
    hash_labeled_len(hasher, "field_count", model.fields.len());

    for field in model.fields {
        hash_labeled_str(hasher, "field_name", field.name);
    }

    // Phase 2: hash index contract details (names, stores, uniqueness, fields).
    hash_labeled_len(hasher, "index_count", model.indexes.len());
    for index in model.indexes {
        hash_labeled_str(hasher, "index_name", index.name());
        hash_labeled_str(hasher, "index_store", index.store());
        hasher.update(&[u8::from(index.is_unique())]);
        hash_labeled_len(hasher, "index_field_count", index.fields().len());
        for field in index.fields() {
            hash_labeled_str(hasher, "index_field_name", field);
        }
    }
}

fn hash_labeled_str(hasher: &mut Xxh3, label: &str, value: &str) {
    hash_labeled_len(hasher, label, value.len());
    hasher.update(value.as_bytes());
}

fn hash_labeled_len(hasher: &mut Xxh3, label: &str, len: usize) {
    hasher.update(label.as_bytes());
    hasher.update(&u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
}
