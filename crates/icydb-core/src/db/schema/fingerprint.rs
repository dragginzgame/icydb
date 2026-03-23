//! Module: db::schema::fingerprint
//! Responsibility: deterministic schema-contract hashing for commit compatibility checks.
//! Does not own: commit marker persistence or recovery orchestration.
//! Boundary: schema identity hashing consumed by commit preparation and replay guards.

use crate::{
    db::{
        commit::CommitSchemaFingerprint, index::canonical_index_predicate,
        predicate::hash_predicate,
    },
    model::{
        EntityModel,
        index::{IndexKeyItem, IndexKeyItemsRef, IndexModel},
    },
    traits::EntityKind,
};
use canic_utils::hash::Xxh3;
use sha2::{Digest, Sha256};

const COMMIT_SCHEMA_FINGERPRINT_VERSION: u8 = 2;

const INDEX_KEY_ITEM_FIELD_TAG: u8 = 0x00;
const INDEX_KEY_ITEM_EXPRESSION_TAG: u8 = 0x01;

const INDEX_PREDICATE_NONE_TAG: u8 = 0x00;
const INDEX_PREDICATE_SEMANTIC_TAG: u8 = 0x01;
const INDEX_PREDICATE_RAW_SQL_FALLBACK_TAG: u8 = 0x02;

/// Compute one deterministic schema/index fingerprint for an entity commit planner.
#[must_use]
pub(crate) fn commit_schema_fingerprint_for_entity<E: EntityKind>() -> CommitSchemaFingerprint {
    commit_schema_fingerprint_for_model(E::PATH, E::MODEL)
}

/// Compute one deterministic schema/index fingerprint from structural authority.
#[must_use]
pub(crate) fn commit_schema_fingerprint_for_model(
    entity_path: &'static str,
    model: &'static EntityModel,
) -> CommitSchemaFingerprint {
    // Phase 1: version the fingerprint contract and hash top-level identity.
    let mut hasher = Xxh3::with_seed(0);
    hasher.update(&[COMMIT_SCHEMA_FINGERPRINT_VERSION]);
    hash_labeled_str(&mut hasher, "entity_path", entity_path);

    // Phase 2: hash the macro-generated entity schema contract consumed by
    // prepare/replay planning (field slot order + index definitions).
    hash_entity_model_for_commit(&mut hasher, model);

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
        hash_index_key_items_contract(hasher, index);
        hash_index_predicate_contract(hasher, index);
    }
}

fn hash_index_key_items_contract(hasher: &mut Xxh3, index: &IndexModel) {
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

fn hash_index_predicate_contract(hasher: &mut Xxh3, index: &IndexModel) {
    match canonical_index_predicate(index) {
        Ok(None) => hash_labeled_tag(hasher, "index_predicate_kind", INDEX_PREDICATE_NONE_TAG),
        Ok(Some(predicate)) => {
            hash_labeled_tag(hasher, "index_predicate_kind", INDEX_PREDICATE_SEMANTIC_TAG);
            let mut predicate_hasher = Sha256::new();
            hash_predicate(&mut predicate_hasher, predicate);
            let digest = predicate_hasher.finalize();
            hash_labeled_len(hasher, "index_predicate_semantic_hash_len", digest.len());
            hasher.update(digest.as_slice());
        }
        Err(_) => {
            // Defensive fallback for malformed static schema metadata.
            // This keeps commit fingerprinting deterministic even when predicate
            // parsing fails unexpectedly before schema validation catches it.
            hash_labeled_tag(
                hasher,
                "index_predicate_kind",
                INDEX_PREDICATE_RAW_SQL_FALLBACK_TAG,
            );
            if let Some(predicate_sql) = index.predicate() {
                hash_labeled_str(hasher, "index_predicate_raw_sql", predicate_sql);
            }
        }
    }
}

fn hash_labeled_tag(hasher: &mut Xxh3, label: &str, tag: u8) {
    hasher.update(label.as_bytes());
    hasher.update(&[tag]);
}

fn hash_labeled_str(hasher: &mut Xxh3, label: &str, value: &str) {
    hash_labeled_len(hasher, label, value.len());
    hasher.update(value.as_bytes());
}

fn hash_labeled_len(hasher: &mut Xxh3, label: &str, len: usize) {
    hasher.update(label.as_bytes());
    hasher.update(&u64::try_from(len).unwrap_or(u64::MAX).to_be_bytes());
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::schema::fingerprint::{hash_entity_model_for_commit, hash_labeled_str},
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel},
            index::{IndexExpression, IndexKeyItem, IndexModel},
        },
    };
    use canic_utils::hash::Xxh3;

    const INDEX_FIELDS: [&str; 1] = ["active"];

    static FIELD_MODELS: [FieldModel; 2] = [
        FieldModel::new("id", FieldKind::Ulid),
        FieldModel::new("active", FieldKind::Bool),
    ];

    static INDEX_MODEL_PRED_TRUE_A: IndexModel = IndexModel::new_with_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        false,
        Some("active = true"),
    );
    static INDEX_MODEL_PRED_TRUE_B: IndexModel = IndexModel::new_with_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        false,
        Some("active=true"),
    );
    static INDEX_MODEL_PRED_FALSE: IndexModel = IndexModel::new_with_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        false,
        Some("active = false"),
    );
    static INDEX_KEY_ITEMS_FIELD: [IndexKeyItem; 1] = [IndexKeyItem::Field("active")];
    static INDEX_MODEL_KEY_ITEMS_FIELD: IndexModel = IndexModel::new_with_key_items_and_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        Some(&INDEX_KEY_ITEMS_FIELD),
        false,
        Some("active=true"),
    );
    static INDEX_KEY_ITEMS_EXPR: [IndexKeyItem; 1] =
        [IndexKeyItem::Expression(IndexExpression::Lower("active"))];
    static INDEX_MODEL_KEY_ITEMS_EXPR: IndexModel = IndexModel::new_with_key_items_and_predicate(
        "entity|active",
        "entity::store",
        &INDEX_FIELDS,
        Some(&INDEX_KEY_ITEMS_EXPR),
        false,
        Some("active=true"),
    );

    static INDEX_REFS_TRUE_A: [&IndexModel; 1] = [&INDEX_MODEL_PRED_TRUE_A];
    static INDEX_REFS_TRUE_B: [&IndexModel; 1] = [&INDEX_MODEL_PRED_TRUE_B];
    static INDEX_REFS_FALSE: [&IndexModel; 1] = [&INDEX_MODEL_PRED_FALSE];
    static INDEX_REFS_KEY_ITEMS_FIELD: [&IndexModel; 1] = [&INDEX_MODEL_KEY_ITEMS_FIELD];
    static INDEX_REFS_KEY_ITEMS_EXPR: [&IndexModel; 1] = [&INDEX_MODEL_KEY_ITEMS_EXPR];

    static MODEL_TRUE_A: EntityModel = EntityModel::new(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        &FIELD_MODELS,
        &INDEX_REFS_TRUE_A,
    );
    static MODEL_TRUE_B: EntityModel = EntityModel::new(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        &FIELD_MODELS,
        &INDEX_REFS_TRUE_B,
    );
    static MODEL_FALSE: EntityModel = EntityModel::new(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        &FIELD_MODELS,
        &INDEX_REFS_FALSE,
    );
    static MODEL_KEY_ITEMS_FIELD: EntityModel = EntityModel::new(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        &FIELD_MODELS,
        &INDEX_REFS_KEY_ITEMS_FIELD,
    );
    static MODEL_KEY_ITEMS_EXPR: EntityModel = EntityModel::new(
        "fingerprint::Entity",
        "Entity",
        &FIELD_MODELS[0],
        &FIELD_MODELS,
        &INDEX_REFS_KEY_ITEMS_EXPR,
    );

    fn fingerprint_for_model(model: &EntityModel) -> [u8; 16] {
        let mut hasher = Xxh3::with_seed(0);
        hasher.update(&[super::COMMIT_SCHEMA_FINGERPRINT_VERSION]);
        hash_labeled_str(&mut hasher, "entity_path", model.path());
        hash_entity_model_for_commit(&mut hasher, model);
        hasher.digest128().to_be_bytes()
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
