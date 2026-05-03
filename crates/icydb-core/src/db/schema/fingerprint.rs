//! Module: db::schema::fingerprint
//! Responsibility: deterministic schema-contract hashing for commit compatibility checks.
//! Does not own: commit marker persistence or recovery orchestration.
//! Boundary: schema identity hashing consumed by commit preparation and replay guards.

use crate::{
    db::{
        codec::{finalize_hash_sha256, new_hash_sha256},
        commit::CommitSchemaFingerprint,
        index::canonical_index_predicate,
        predicate::hash_predicate,
        schema::{
            AcceptedSchemaSnapshot, compiled_schema_proposal_for_model,
            encode_persisted_schema_snapshot,
        },
    },
    error::InternalError,
    model::{
        EntityModel,
        field::FieldKind,
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    traits::EntityKind,
};
use sha2::{Digest, Sha256};
use std::{any::TypeId, cell::RefCell, collections::HashMap};

thread_local! {
    static ENTITY_COMMIT_SCHEMA_FINGERPRINT_CACHE:
        RefCell<HashMap<TypeId, CommitSchemaFingerprint>> = RefCell::new(HashMap::new());
}

const COMMIT_SCHEMA_FINGERPRINT_VERSION: u8 = 2;
const ACCEPTED_SCHEMA_CACHE_FINGERPRINT_VERSION: u8 = 1;

const INDEX_KEY_ITEM_FIELD_TAG: u8 = 0x00;
const INDEX_KEY_ITEM_EXPRESSION_TAG: u8 = 0x01;

const INDEX_PREDICATE_NONE_TAG: u8 = 0x00;
const INDEX_PREDICATE_SEMANTIC_TAG: u8 = 0x01;

/// SchemaFingerprint
///
/// This structural identity fingerprint represents:
/// - entity identity
/// - field layout (names + count)
/// - index definitions
///
/// It does NOT represent:
/// - field type compatibility
/// - nullability
/// - decimal scale
/// - storage encoding
///
/// It does represent schema-level bounded-text limits because those affect
/// write admissibility without changing field order or storage encoding.
///
/// Therefore, this is a structural identity fingerprint, not a full
/// compatibility hash.
///
/// Compute one deterministic schema/index fingerprint for an entity commit planner.
#[must_use]
pub(crate) fn commit_schema_fingerprint_for_entity<E: EntityKind + 'static>()
-> CommitSchemaFingerprint {
    // Phase 1: check the per-entity memoized fingerprint so repeated SQL
    // compile/cache lookups inside one canister lifetime do not keep rehashing
    // the same schema contract.
    let cache_key = TypeId::of::<E>();
    let cached = ENTITY_COMMIT_SCHEMA_FINGERPRINT_CACHE
        .with(|cache| cache.borrow().get(&cache_key).copied());

    if let Some(fingerprint) = cached {
        return fingerprint;
    }

    // Phase 2: compute the deterministic schema contract fingerprint once,
    // then retain it for future lookups of the same entity type.
    let fingerprint = commit_schema_fingerprint_for_model(E::PATH, E::MODEL);
    ENTITY_COMMIT_SCHEMA_FINGERPRINT_CACHE.with(|cache| {
        cache.borrow_mut().insert(cache_key, fingerprint);
    });

    fingerprint
}

/// Compute one deterministic schema/index fingerprint from resolved authority.
#[must_use]
pub(crate) fn commit_schema_fingerprint_for_model(
    entity_path: &'static str,
    model: &'static EntityModel,
) -> CommitSchemaFingerprint {
    // Phase 1: version the fingerprint contract and hash top-level identity.
    let mut hasher = new_hash_sha256();
    hasher.update([COMMIT_SCHEMA_FINGERPRINT_VERSION]);
    hash_labeled_str(&mut hasher, "entity_path", entity_path);

    // Phase 2: hash the macro-generated entity schema contract consumed by
    // prepare/replay planning. Today's contract tracks entity identity, primary
    // key name, field slot names/count, bounded-text limits, and index definitions.
    // It intentionally does not track general field kind, nullability, decimal
    // scale, storage decode, or write-management metadata; tests below pin that
    // behavior before any future contract expansion.
    hash_entity_model_for_commit(&mut hasher, model);

    truncate_sha256_commit_schema_fingerprint(hasher)
}

/// Compute one accepted-schema fingerprint for runtime cache identity.
///
/// Unlike the commit fingerprint, this cache fingerprint follows the accepted
/// persisted snapshot that planning and SQL admission consume at runtime. It
/// intentionally includes the encoded accepted schema payload and generated
/// index contract so in-heap SQL/query caches miss when either the live schema
/// authority or planner-visible index metadata changes.
pub(in crate::db) fn accepted_schema_cache_fingerprint_for_model(
    model: &'static EntityModel,
    schema: &AcceptedSchemaSnapshot,
) -> Result<CommitSchemaFingerprint, InternalError> {
    let mut hasher = new_hash_sha256();
    hasher.update([ACCEPTED_SCHEMA_CACHE_FINGERPRINT_VERSION]);
    hash_labeled_str(&mut hasher, "model_path", model.path());
    let encoded_snapshot = encode_persisted_schema_snapshot(schema.persisted_snapshot())?;
    hash_labeled_len(
        &mut hasher,
        "accepted_schema_snapshot_len",
        encoded_snapshot.len(),
    );
    hasher.update(encoded_snapshot);
    hash_model_index_contract_for_cache(&mut hasher, model);

    Ok(truncate_sha256_commit_schema_fingerprint(hasher))
}

fn hash_entity_model_for_commit(hasher: &mut Sha256, model: &EntityModel) {
    let proposal = compiled_schema_proposal_for_model(model);

    // Phase 1: hash core entity identity and field-shape contract.
    hash_labeled_str(hasher, "model_path", proposal.entity_path());
    hash_labeled_str(hasher, "entity_name", proposal.entity_name());
    hash_labeled_str(hasher, "primary_key", proposal.primary_key_name());
    hash_labeled_len(hasher, "field_count", proposal.fields().len());

    for field in proposal.fields() {
        hash_labeled_str(hasher, "field_name", field.name());
        hash_field_text_max_len_contract(hasher, &field.kind());
    }

    // Phase 2: hash index contract details (names, stores, uniqueness, fields).
    hash_model_index_contract_for_cache(hasher, model);
}

fn hash_model_index_contract_for_cache(hasher: &mut Sha256, model: &EntityModel) {
    hash_labeled_len(hasher, "index_count", model.indexes.len());
    for index in model.indexes {
        hash_labeled_str(hasher, "index_name", index.name());
        hash_labeled_str(hasher, "index_store", index.store());
        hasher.update([u8::from(index.is_unique())]);
        hash_index_key_items_contract(hasher, index);
        hash_index_predicate_contract(hasher, index);
    }
}

fn hash_field_text_max_len_contract(hasher: &mut Sha256, kind: &FieldKind) {
    match kind {
        FieldKind::Text {
            max_len: Some(max_len),
        } => {
            hash_labeled_tag(hasher, "field_text_max_len_kind", 1);
            hasher.update(max_len.to_be_bytes());
        }
        FieldKind::Text { max_len: None } => {
            hash_labeled_tag(hasher, "field_text_max_len_kind", 0);
        }
        _ => hash_labeled_tag(hasher, "field_text_max_len_kind", 0),
    }
}

fn hash_index_key_items_contract(hasher: &mut Sha256, index: &IndexModel) {
    match index.key_items() {
        IndexKeyItemsRef::Fields(fields) => {
            hash_labeled_len(hasher, "index_field_count", fields.len());
            for field in fields {
                hash_labeled_tag(hasher, "index_key_item_kind", INDEX_KEY_ITEM_FIELD_TAG);
                hash_labeled_str(hasher, "index_field_name", field);
            }
        }
        IndexKeyItemsRef::Items(items) => {
            hash_labeled_len(hasher, "index_field_count", items.len());
            for item in items {
                match item {
                    IndexKeyItem::Field(field) => {
                        hash_labeled_tag(hasher, "index_key_item_kind", INDEX_KEY_ITEM_FIELD_TAG);
                        hash_labeled_str(hasher, "index_field_name", field);
                    }
                    IndexKeyItem::Expression(expression) => {
                        hash_labeled_tag(
                            hasher,
                            "index_key_item_kind",
                            INDEX_KEY_ITEM_EXPRESSION_TAG,
                        );
                        hash_labeled_tag(hasher, "index_expression_kind", expression.kind_tag());
                        hash_labeled_str(hasher, "index_expression_field", expression.field());
                    }
                }
            }
        }
    }
}

fn hash_index_predicate_contract(hasher: &mut Sha256, index: &IndexModel) {
    match canonical_index_predicate(index) {
        None => hash_labeled_tag(hasher, "index_predicate_kind", INDEX_PREDICATE_NONE_TAG),
        Some(predicate) => {
            hash_labeled_tag(hasher, "index_predicate_kind", INDEX_PREDICATE_SEMANTIC_TAG);
            let mut predicate_hasher = new_hash_sha256();
            hash_predicate(&mut predicate_hasher, predicate);
            let digest = finalize_hash_sha256(predicate_hasher);
            hash_labeled_len(hasher, "index_predicate_semantic_hash_len", digest.len());
            hasher.update(digest);
        }
    }
}

fn hash_labeled_tag(hasher: &mut Sha256, label: &str, tag: u8) {
    hasher.update(label.as_bytes());
    hasher.update([tag]);
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
        db::Predicate,
        db::schema::fingerprint::{hash_entity_model_for_commit, hash_labeled_str},
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode},
            index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
        },
    };
    use sha2::Digest;
    use std::sync::LazyLock;

    const INDEX_FIELDS: [&str; 1] = ["active"];
    const CONTRACT_INDEX_FIELDS: [&str; 1] = ["value"];

    static FIELD_MODELS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("active", FieldKind::Bool),
    ];
    static CONTRACT_BASE_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("value", FieldKind::Text { max_len: None }),
    ];
    static CONTRACT_RENAMED_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("label", FieldKind::Text { max_len: None }),
    ];
    static CONTRACT_EXTRA_FIELDS: [FieldModel; 3] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("value", FieldKind::Text { max_len: None }),
        FieldModel::generated("enabled", FieldKind::Bool),
    ];
    static CONTRACT_TYPE_CHANGED_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("value", FieldKind::Bool),
    ];
    static CONTRACT_NULLABLE_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated_with_storage_decode_and_nullability(
            "value",
            FieldKind::Text { max_len: None },
            FieldStorageDecode::ByKind,
            true,
        ),
    ];
    static CONTRACT_TEXT_MAX_LEN_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("value", FieldKind::Text { max_len: Some(16) }),
    ];
    static CONTRACT_DECIMAL_SCALE_2_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("amount", FieldKind::Decimal { scale: 2 }),
    ];
    static CONTRACT_DECIMAL_SCALE_4_FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("amount", FieldKind::Decimal { scale: 4 }),
    ];
    static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
        LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));
    static ACTIVE_FALSE_PREDICATE: LazyLock<Predicate> =
        LazyLock::new(|| Predicate::eq("active".to_string(), false.into()));

    fn active_true_predicate() -> &'static Predicate {
        &ACTIVE_TRUE_PREDICATE
    }

    fn active_false_predicate() -> &'static Predicate {
        &ACTIVE_FALSE_PREDICATE
    }

    const fn active_true_predicate_metadata(sql: &'static str) -> IndexPredicateMetadata {
        IndexPredicateMetadata::generated(sql, active_true_predicate)
    }

    const fn active_false_predicate_metadata() -> IndexPredicateMetadata {
        IndexPredicateMetadata::generated("active = false", active_false_predicate)
    }

    static INDEX_MODEL_PRED_TRUE_A: IndexModel = IndexModel::generated_with_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        false,
        Some(active_true_predicate_metadata("active = true")),
    );
    static INDEX_MODEL_PRED_TRUE_B: IndexModel = IndexModel::generated_with_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        false,
        Some(active_true_predicate_metadata("active=true")),
    );
    static INDEX_MODEL_PRED_FALSE: IndexModel = IndexModel::generated_with_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        false,
        Some(active_false_predicate_metadata()),
    );
    static INDEX_KEY_ITEMS_FIELD: [IndexKeyItem; 1] = [IndexKeyItem::Field("active")];
    static INDEX_MODEL_KEY_ITEMS_FIELD: IndexModel =
        IndexModel::generated_with_key_items_and_predicate(
            "entity|active",
            "entity::store",
            &INDEX_FIELDS,
            Some(&INDEX_KEY_ITEMS_FIELD),
            false,
            Some(active_true_predicate_metadata("active=true")),
        );
    static INDEX_KEY_ITEMS_EXPR: [IndexKeyItem; 1] =
        [IndexKeyItem::Expression(IndexExpression::Lower("active"))];
    static INDEX_MODEL_KEY_ITEMS_EXPR: IndexModel =
        IndexModel::generated_with_key_items_and_predicate(
            "entity|active",
            "entity::store",
            &INDEX_FIELDS,
            Some(&INDEX_KEY_ITEMS_EXPR),
            false,
            Some(active_true_predicate_metadata("active=true")),
        );
    static CONTRACT_INDEX_MODEL: IndexModel = IndexModel::generated(
        "entity|value",
        "entity::value_index",
        &CONTRACT_INDEX_FIELDS,
        false,
    );

    static EMPTY_INDEX_REFS: [&IndexModel; 0] = [];
    static INDEX_REFS_TRUE_A: [&IndexModel; 1] = [&INDEX_MODEL_PRED_TRUE_A];
    static INDEX_REFS_TRUE_B: [&IndexModel; 1] = [&INDEX_MODEL_PRED_TRUE_B];
    static INDEX_REFS_FALSE: [&IndexModel; 1] = [&INDEX_MODEL_PRED_FALSE];
    static INDEX_REFS_KEY_ITEMS_FIELD: [&IndexModel; 1] = [&INDEX_MODEL_KEY_ITEMS_FIELD];
    static INDEX_REFS_KEY_ITEMS_EXPR: [&IndexModel; 1] = [&INDEX_MODEL_KEY_ITEMS_EXPR];
    static CONTRACT_INDEX_REFS: [&IndexModel; 1] = [&CONTRACT_INDEX_MODEL];

    static MODEL_TRUE_A: EntityModel = EntityModel::generated(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        0,
        &FIELD_MODELS,
        &INDEX_REFS_TRUE_A,
    );
    static MODEL_TRUE_B: EntityModel = EntityModel::generated(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        0,
        &FIELD_MODELS,
        &INDEX_REFS_TRUE_B,
    );
    static MODEL_FALSE: EntityModel = EntityModel::generated(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        0,
        &FIELD_MODELS,
        &INDEX_REFS_FALSE,
    );
    static MODEL_KEY_ITEMS_FIELD: EntityModel = EntityModel::generated(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        0,
        &FIELD_MODELS,
        &INDEX_REFS_KEY_ITEMS_FIELD,
    );
    static MODEL_KEY_ITEMS_EXPR: EntityModel = EntityModel::generated(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        0,
        &FIELD_MODELS,
        &INDEX_REFS_KEY_ITEMS_EXPR,
    );
    static CONTRACT_BASE_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_BASE_FIELDS[0],
        0,
        &CONTRACT_BASE_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_RENAMED_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_RENAMED_FIELDS[0],
        0,
        &CONTRACT_RENAMED_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_EXTRA_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_EXTRA_FIELDS[0],
        0,
        &CONTRACT_EXTRA_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_TYPE_CHANGED_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_TYPE_CHANGED_FIELDS[0],
        0,
        &CONTRACT_TYPE_CHANGED_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_NULLABLE_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_NULLABLE_FIELDS[0],
        0,
        &CONTRACT_NULLABLE_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_TEXT_MAX_LEN_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_TEXT_MAX_LEN_FIELDS[0],
        0,
        &CONTRACT_TEXT_MAX_LEN_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_DECIMAL_SCALE_2_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_DECIMAL_SCALE_2_FIELDS[0],
        0,
        &CONTRACT_DECIMAL_SCALE_2_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_DECIMAL_SCALE_4_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_DECIMAL_SCALE_4_FIELDS[0],
        0,
        &CONTRACT_DECIMAL_SCALE_4_FIELDS,
        &EMPTY_INDEX_REFS,
    );
    static CONTRACT_INDEXED_MODEL: EntityModel = EntityModel::generated(
        "fingerprint::ContractEntity",
        "ContractEntity",
        &CONTRACT_BASE_FIELDS[0],
        0,
        &CONTRACT_BASE_FIELDS,
        &CONTRACT_INDEX_REFS,
    );

    fn fingerprint_for_model(model: &EntityModel) -> [u8; 16] {
        let mut hasher = crate::db::codec::new_hash_sha256();
        hasher.update([super::COMMIT_SCHEMA_FINGERPRINT_VERSION]);
        hash_labeled_str(&mut hasher, "entity_path", model.path());
        hash_entity_model_for_commit(&mut hasher, model);
        super::truncate_sha256_commit_schema_fingerprint(hasher)
    }

    #[test]
    fn schema_fingerprint_current_contract_tracks_field_names_counts_and_indexes() {
        assert_ne!(
            fingerprint_for_model(&CONTRACT_BASE_MODEL),
            fingerprint_for_model(&CONTRACT_RENAMED_MODEL),
            "field-name changes are part of today's commit schema fingerprint contract",
        );
        assert_ne!(
            fingerprint_for_model(&CONTRACT_BASE_MODEL),
            fingerprint_for_model(&CONTRACT_EXTRA_MODEL),
            "field-count changes are part of today's commit schema fingerprint contract",
        );
        assert_ne!(
            fingerprint_for_model(&CONTRACT_BASE_MODEL),
            fingerprint_for_model(&CONTRACT_INDEXED_MODEL),
            "index contract changes are part of today's commit schema fingerprint contract",
        );
    }

    #[test]
    fn schema_fingerprint_current_contract_ignores_field_type_nullability_and_scale() {
        assert_eq!(
            fingerprint_for_model(&CONTRACT_BASE_MODEL),
            fingerprint_for_model(&CONTRACT_TYPE_CHANGED_MODEL),
            "field-kind changes are intentionally documented as outside today's fingerprint contract",
        );
        assert_eq!(
            fingerprint_for_model(&CONTRACT_BASE_MODEL),
            fingerprint_for_model(&CONTRACT_NULLABLE_MODEL),
            "nullability changes are intentionally documented as outside today's fingerprint contract",
        );
        assert_eq!(
            fingerprint_for_model(&CONTRACT_DECIMAL_SCALE_2_MODEL),
            fingerprint_for_model(&CONTRACT_DECIMAL_SCALE_4_MODEL),
            "decimal scale changes are intentionally documented as outside today's fingerprint contract",
        );
    }

    #[test]
    fn schema_fingerprint_tracks_text_max_len_contract() {
        assert_ne!(
            fingerprint_for_model(&CONTRACT_BASE_MODEL),
            fingerprint_for_model(&CONTRACT_TEXT_MAX_LEN_MODEL),
            "text max_len changes write admissibility and must change commit schema fingerprint",
        );
    }

    #[test]
    fn schema_fingerprint_changes_when_index_predicate_semantics_change() {
        assert_ne!(
            fingerprint_for_model(&MODEL_TRUE_A),
            fingerprint_for_model(&MODEL_FALSE),
            "semantic predicate changes must change commit schema fingerprint",
        );
    }

    #[test]
    fn schema_fingerprint_is_stable_for_equivalent_index_predicate_sql_text() {
        assert_eq!(
            fingerprint_for_model(&MODEL_TRUE_A),
            fingerprint_for_model(&MODEL_TRUE_B),
            "equivalent predicate SQL text should hash to the same semantic schema fingerprint",
        );
    }

    #[test]
    fn schema_fingerprint_preserves_field_only_parity_for_key_item_metadata() {
        assert_eq!(
            fingerprint_for_model(&MODEL_TRUE_A),
            fingerprint_for_model(&MODEL_KEY_ITEMS_FIELD),
            "field-only key-item metadata should hash identically to field metadata",
        );
    }

    #[test]
    fn schema_fingerprint_changes_when_expression_key_item_semantics_change() {
        assert_ne!(
            fingerprint_for_model(&MODEL_TRUE_A),
            fingerprint_for_model(&MODEL_KEY_ITEMS_EXPR),
            "expression key-item metadata must contribute to schema fingerprint semantics",
        );
    }
}
