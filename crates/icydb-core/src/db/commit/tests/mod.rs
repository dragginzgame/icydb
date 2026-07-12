//! Module: db::commit::tests
//! Covers commit application and persistence invariants for the write path.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::{
    db::{
        Db, EntityRuntimeHooks, Predicate,
        codec::{
            ROW_FORMAT_VERSION_CURRENT, decode_row_payload_bytes, serialize_row_payload,
            serialize_row_payload_with_version,
        },
        commit::{
            CommitFailpoint, CommitFailpointFailureClass, CommitFailpointMode,
            CommitFailpointRecoveryAuthority, CommitFailpointSnapshotOracle, CommitMarker,
            CommitRowOp, arm_commit_failpoint_for_tests, begin_commit,
            clear_commit_failpoint_for_tests, clear_recovery_runtime_state_for_tests,
            commit_marker_present, ensure_recovered, finish_commit, init_commit_store_for_tests,
            mark_schema_reconciliation_dirty_for_tests,
            marker::{
                COMMIT_MARKER_FORMAT_VERSION_CURRENT, encode_commit_marker_payload,
                encode_single_row_commit_marker_payload,
            },
            prepare_row_commit_for_entity_with_structural_readers,
            reset_commit_marker_test_journal_sequence, rollback_prepared_row_ops_reverse, store,
        },
        data::{
            CanonicalRow, DataStore, DecodedDataStoreKey, RawDataStoreKey, RawRow, StoreVisit,
            encode_value_with_model_proposal_for_test,
        },
        executor::SaveExecutor,
        index::{
            IndexEntryValue, IndexKey, IndexState, IndexStore, IndexStoreVisit, RawIndexStoreKey,
        },
        journal::{FoldWatermark, JournalTailStore},
        registry::{StoreHandle, StoreRegistry},
        relation::validate_delete_strong_relations_for_source,
        schema::{
            AcceptedSchemaSnapshot, FieldId, PersistedSchemaSnapshot, SchemaFieldSlot,
            SchemaRowLayout, SchemaStore, SchemaVersion, accepted_commit_schema_fingerprint,
            compiled_schema_proposal_for_model, publish_test_accepted_schema_snapshot,
        },
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::{
        field::{EnumVariantModel, FieldKind, FieldStorageDecode},
        index::{IndexExpression, IndexKeyItem, IndexModel, IndexPredicateMetadata},
    },
    testing::test_memory,
    traits::{
        CanisterKind, EntityKind, EntitySchema, FieldTypeMeta, Path, PersistedFieldSlotCodec,
        RuntimeValueDecode, RuntimeValueEncode,
    },
    types::{EntityTag, Ulid},
    value::{Value, ValueEnum},
};
use icydb_derive::{FieldProjection, PersistedRow};
use serde::Deserialize;
use std::{
    cell::RefCell,
    collections::{BTreeMap, BTreeSet},
    panic::{AssertUnwindSafe, catch_unwind},
    sync::LazyLock,
};

type RecoveryStoreSnapshot = (Vec<(Vec<u8>, Vec<u8>)>, Vec<(Vec<u8>, Vec<u8>)>);
struct RecoveryFailpointCase {
    marker: CommitMarker,
    pre_snapshot: RecoveryStoreSnapshot,
    post_snapshot: RecoveryStoreSnapshot,
    pre_watermark: FoldWatermark,
    post_watermark: FoldWatermark,
}

impl RecoveryFailpointCase {
    fn classified_state_for(
        &self,
        snapshot: CommitFailpointSnapshotOracle,
    ) -> (&RecoveryStoreSnapshot, FoldWatermark) {
        match snapshot {
            CommitFailpointSnapshotOracle::PreCommit => (&self.pre_snapshot, self.pre_watermark),
            CommitFailpointSnapshotOracle::MarkerAuthorizedPostCommit => {
                (&self.post_snapshot, self.post_watermark)
            }
            CommitFailpointSnapshotOracle::RecoveryIntermediate => {
                panic!("intermediate recovery state must be asserted by failpoint-specific tests")
            }
        }
    }

    fn interruption_state_for(
        &self,
        site: CommitFailpoint,
    ) -> (&RecoveryStoreSnapshot, FoldWatermark) {
        self.classified_state_for(site.recovery_oracle().snapshot())
    }

    fn retry_state_for(&self, site: CommitFailpoint) -> (&RecoveryStoreSnapshot, FoldWatermark) {
        let oracle = site.recovery_oracle();
        if oracle.marker_present() {
            (&self.post_snapshot, self.post_watermark)
        } else {
            self.classified_state_for(oracle.snapshot())
        }
    }
}

static ACTIVE_TRUE_PREDICATE: LazyLock<Predicate> =
    LazyLock::new(|| Predicate::eq("active".to_string(), true.into()));

fn active_true_predicate() -> &'static Predicate {
    &ACTIVE_TRUE_PREDICATE
}

const fn active_true_predicate_metadata() -> IndexPredicateMetadata {
    IndexPredicateMetadata::generated("active = true", active_true_predicate)
}

//
// RecoveryTestCanister
//

crate::test_canister! {
    ident = RecoveryTestCanister,
    commit_memory_id = crate::testing::test_commit_memory_id(),
}

struct RecoveryPeerCanister;

impl Path for RecoveryPeerCanister {
    const PATH: &'static str = concat!(module_path!(), "::", stringify!(RecoveryPeerCanister));
}

impl CanisterKind for RecoveryPeerCanister {
    const COMMIT_MEMORY_ID: u8 = 30;
    const COMMIT_STABLE_KEY: &'static str = "icydb.test.peer.commit.v1";
}

//
// RecoveryTestDataStore
//

crate::test_store! {
    ident = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_store! {
    ident = HeapRecoveryTestDataStore,
    canister = RecoveryTestCanister,
}

crate::test_store! {
    ident = RecoveryPeerDataStore,
    canister = RecoveryPeerCanister,
}

///
/// RecoveryTestEntity
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryTestEntity {
    id: Ulid,
}

crate::test_entity! {
    ident = RecoveryTestEntity,
    entity_name = "RecoveryTestEntity",
    tag = crate::testing::RECOVERY_TEST_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
    ],
    indexes = [],
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryPayloadEntity {
    id: Ulid,
    name: String,
}

crate::test_entity! {
    ident = RecoveryPayloadEntity,
    entity_name = "RecoveryPayloadEntity",
    tag = crate::testing::RECOVERY_PAYLOAD_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { name: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [],
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryPeerEntity {
    id: Ulid,
    group: u32,
}

crate::test_entity! {
    ident = RecoveryPeerEntity,
    entity_name = "RecoveryPeerEntity",
    tag = EntityTag::new(0x1713),
    store = RecoveryPeerDataStore,
    canister = RecoveryPeerCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u32 => FieldKind::Nat64 },
    ],
    indexes = [],
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryIndexedEntity {
    id: Ulid,
    group: u32,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct HeapRecoveryIndexedEntity {
    id: Ulid,
    group: u32,
}

///
/// RecoveryNullableIndexedEntity
///
/// Nullable additive-transition fixture used by startup recovery tests.
/// It gives index rebuild one current generated model with an appended nullable
/// field while the seeded stored rows still carry the shorter old layout.
///

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryNullableIndexedEntity {
    id: Ulid,
    group: u32,
    nickname: Option<String>,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryUniqueEntity {
    id: Ulid,
    email: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryUniqueCasefoldEntity {
    id: Ulid,
    email: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryUpperExpressionEntity {
    id: Ulid,
    email: String,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalEntity {
    id: Ulid,
    group: u32,
    active: bool,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalUniqueEntity {
    id: Ulid,
    email: String,
    active: bool,
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalUniqueCasefoldEntity {
    id: Ulid,
    email: String,
    active: bool,
}

///
/// RecoveryStatus
///
/// RecoveryStatus is the typed persisted wrapper for the runtime enum value
/// used by conditional unique index recovery tests.
/// It preserves enum-value index behavior without making the dynamic `Value`
/// union itself persistable.
///

#[derive(Clone, Debug, Deserialize, PartialEq)]
struct RecoveryStatus(ValueEnum);

impl FieldTypeMeta for RecoveryStatus {
    const KIND: FieldKind = FieldKind::Enum {
        path: RECOVERY_STATUS_ENUM_PATH,
        variants: &RECOVERY_STATUS_VARIANTS,
    };
    const STORAGE_DECODE: FieldStorageDecode = FieldStorageDecode::Value;
}

impl RuntimeValueEncode for RecoveryStatus {
    fn to_value(&self) -> Value {
        Value::Enum(self.0.clone())
    }
}

impl RuntimeValueDecode for RecoveryStatus {
    fn from_value(value: &Value) -> Option<Self> {
        let Value::Enum(value) = value else {
            return None;
        };

        Some(Self(value.clone()))
    }
}

impl PersistedFieldSlotCodec for RecoveryStatus {
    fn encode_persisted_slot(&self, field_name: &'static str) -> Result<Vec<u8>, InternalError> {
        crate::db::encode_persisted_slot_payload_by_meta(self, field_name)
    }

    fn decode_persisted_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Self, InternalError> {
        crate::db::decode_persisted_slot_payload_by_meta(bytes, field_name)
    }

    fn encode_persisted_option_slot(
        value: &Option<Self>,
        field_name: &'static str,
    ) -> Result<Vec<u8>, InternalError> {
        crate::db::encode_persisted_option_slot_payload_by_meta(value, field_name)
    }

    fn decode_persisted_option_slot(
        bytes: &[u8],
        field_name: &'static str,
    ) -> Result<Option<Self>, InternalError> {
        crate::db::decode_persisted_option_slot_payload_by_meta(bytes, field_name)
    }
}

#[derive(Clone, Debug, Deserialize, FieldProjection, PartialEq, PersistedRow)]
struct RecoveryConditionalUniqueEnumEntity {
    id: Ulid,
    status: RecoveryStatus,
    active: bool,
}

impl Default for RecoveryConditionalUniqueEnumEntity {
    fn default() -> Self {
        Self {
            id: Ulid::from_u128(0),
            status: enum_status("Pending"),
            active: false,
        }
    }
}

static RECOVERY_INDEXED_INDEX_FIELDS: [&str; 1] = ["group"];
static RECOVERY_INDEXED_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "group",
    RecoveryTestDataStore::PATH,
    &RECOVERY_INDEXED_INDEX_FIELDS,
    false,
)];
static HEAP_RECOVERY_INDEXED_INDEX_FIELDS: [&str; 1] = ["group"];
static HEAP_RECOVERY_INDEXED_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "group",
    HeapRecoveryTestDataStore::PATH,
    &HEAP_RECOVERY_INDEXED_INDEX_FIELDS,
    false,
)];
static RECOVERY_NULLABLE_INDEXED_INDEX_FIELDS: [&str; 1] = ["group"];
static RECOVERY_NULLABLE_INDEXED_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "group_nullable",
    RecoveryTestDataStore::PATH,
    &RECOVERY_NULLABLE_INDEXED_INDEX_FIELDS,
    false,
)];
static RECOVERY_UNIQUE_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_UNIQUE_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated(
    "email_unique",
    RecoveryTestDataStore::PATH,
    &RECOVERY_UNIQUE_INDEX_FIELDS,
    true,
)];
static RECOVERY_UNIQUE_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static RECOVERY_UNIQUE_CASEFOLD_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items(
        "email_unique_casefold",
        RecoveryTestDataStore::PATH,
        &RECOVERY_UNIQUE_CASEFOLD_INDEX_FIELDS,
        &RECOVERY_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS,
        true,
    )];
static RECOVERY_UPPER_EXPRESSION_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_UPPER_EXPRESSION_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Upper("email"))];
static RECOVERY_UPPER_EXPRESSION_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items(
        "email_upper",
        RecoveryTestDataStore::PATH,
        &RECOVERY_UPPER_EXPRESSION_INDEX_FIELDS,
        &RECOVERY_UPPER_EXPRESSION_INDEX_KEY_ITEMS,
        false,
    )];
static RECOVERY_CONDITIONAL_INDEX_FIELDS: [&str; 1] = ["group"];
static RECOVERY_CONDITIONAL_INDEX_MODELS: [IndexModel; 1] = [IndexModel::generated_with_predicate(
    "group_active_only",
    RecoveryTestDataStore::PATH,
    &RECOVERY_CONDITIONAL_INDEX_FIELDS,
    false,
    Some(active_true_predicate_metadata()),
)];
static RECOVERY_CONDITIONAL_UNIQUE_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_CONDITIONAL_UNIQUE_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_predicate(
        "email_unique_active_only",
        RecoveryTestDataStore::PATH,
        &RECOVERY_CONDITIONAL_UNIQUE_INDEX_FIELDS,
        true,
        Some(active_true_predicate_metadata()),
    )];
static RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_FIELDS: [&str; 1] = ["email"];
static RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS: [IndexKeyItem; 1] =
    [IndexKeyItem::Expression(IndexExpression::Lower("email"))];
static RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_key_items_and_predicate(
        "email_unique_casefold_active_only",
        RecoveryTestDataStore::PATH,
        &RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_FIELDS,
        Some(&RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_KEY_ITEMS),
        true,
        Some(active_true_predicate_metadata()),
    )];
static RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_FIELDS: [&str; 1] = ["status"];
static RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_MODELS: [IndexModel; 1] =
    [IndexModel::generated_with_predicate(
        "status_unique_active_only",
        RecoveryTestDataStore::PATH,
        &RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_FIELDS,
        true,
        Some(active_true_predicate_metadata()),
    )];
static RECOVERY_INDEXED_MISSING_FIELD_INDEX_FIELDS: [&str; 1] = ["missing_group"];
static RECOVERY_INDEXED_MISSING_FIELD_INDEX_MODEL: IndexModel = IndexModel::generated(
    "missing_group",
    RecoveryTestDataStore::PATH,
    &RECOVERY_INDEXED_MISSING_FIELD_INDEX_FIELDS,
    false,
);

crate::test_entity! {
    ident = RecoveryIndexedEntity,
    entity_name = "RecoveryIndexedEntity",
    tag = crate::testing::RECOVERY_INDEXED_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u32 => FieldKind::Nat64 },
    ],
    indexes = [&RECOVERY_INDEXED_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = HeapRecoveryIndexedEntity,
    entity_name = "HeapRecoveryIndexedEntity",
    tag = EntityTag::new(0x1712),
    store = HeapRecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u32 => FieldKind::Nat64 },
    ],
    indexes = [&HEAP_RECOVERY_INDEXED_INDEX_MODELS[0]],
}

const RECOVERY_NULLABLE_INDEXED_ENTITY_TAG: EntityTag = EntityTag::new(0x103A);

crate::test_entity! {
    ident = RecoveryNullableIndexedEntity,
    entity_name = "RecoveryNullableIndexedEntity",
    tag = RECOVERY_NULLABLE_INDEXED_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    version = 2,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u32 => FieldKind::Nat64 },
        crate::test_field! {
            nickname: Option<String> => FieldKind::Text { max_len: None },
            options = crate::testing::TestFieldModelOptions::DEFAULT.with_nullable(true),
        },
    ],
    indexes = [&RECOVERY_NULLABLE_INDEXED_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = RecoveryUniqueEntity,
    entity_name = "RecoveryUniqueEntity",
    tag = crate::testing::RECOVERY_UNIQUE_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { email: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&RECOVERY_UNIQUE_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = RecoveryUniqueCasefoldEntity,
    entity_name = "RecoveryUniqueCasefoldEntity",
    tag = crate::testing::RECOVERY_UNIQUE_CASEFOLD_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { email: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&RECOVERY_UNIQUE_CASEFOLD_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = RecoveryUpperExpressionEntity,
    entity_name = "RecoveryUpperExpressionEntity",
    tag = crate::testing::RECOVERY_UPPER_EXPRESSION_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { email: String => FieldKind::Text { max_len: None } },
    ],
    indexes = [&RECOVERY_UPPER_EXPRESSION_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = RecoveryConditionalEntity,
    entity_name = "RecoveryConditionalEntity",
    tag = crate::testing::RECOVERY_CONDITIONAL_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { group: u32 => FieldKind::Nat64 },
        crate::test_field! { active: bool => FieldKind::Bool },
    ],
    indexes = [&RECOVERY_CONDITIONAL_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = RecoveryConditionalUniqueEntity,
    entity_name = "RecoveryConditionalUniqueEntity",
    tag = crate::testing::RECOVERY_CONDITIONAL_UNIQUE_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { email: String => FieldKind::Text { max_len: None } },
        crate::test_field! { active: bool => FieldKind::Bool },
    ],
    indexes = [&RECOVERY_CONDITIONAL_UNIQUE_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = RecoveryConditionalUniqueCasefoldEntity,
    entity_name = "RecoveryConditionalUniqueCasefoldEntity",
    tag = crate::testing::RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { email: String => FieldKind::Text { max_len: None } },
        crate::test_field! { active: bool => FieldKind::Bool },
    ],
    indexes = [&RECOVERY_CONDITIONAL_UNIQUE_CASEFOLD_INDEX_MODELS[0]],
}

crate::test_entity! {
    ident = RecoveryConditionalUniqueEnumEntity,
    entity_name = "RecoveryConditionalUniqueEnumEntity",
    tag = crate::testing::RECOVERY_CONDITIONAL_UNIQUE_ENUM_ENTITY_TAG,
    store = RecoveryTestDataStore,
    canister = RecoveryTestCanister,
    key_type = Ulid,
    primary_key = [id],
    fields = [
        crate::test_field! { id: Ulid => FieldKind::Ulid },
        crate::test_field! { status: RecoveryStatus => FieldKind::Enum {
            path: RECOVERY_STATUS_ENUM_PATH,
            variants: &RECOVERY_STATUS_VARIANTS,
        }, options = crate::testing::TestFieldModelOptions::DEFAULT
            .with_storage_decode(crate::model::field::FieldStorageDecode::Value) },
        crate::test_field! { active: bool => FieldKind::Bool },
    ],
    indexes = [&RECOVERY_CONDITIONAL_UNIQUE_ENUM_INDEX_MODELS[0]],
}

static ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryIndexedEntity::ENTITY_TAG,
        <RecoveryIndexedEntity as EntitySchema>::MODEL,
        RecoveryIndexedEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        HeapRecoveryIndexedEntity::ENTITY_TAG,
        <HeapRecoveryIndexedEntity as EntitySchema>::MODEL,
        HeapRecoveryIndexedEntity::PATH,
        HeapRecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<HeapRecoveryIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryNullableIndexedEntity::ENTITY_TAG,
        <RecoveryNullableIndexedEntity as EntitySchema>::MODEL,
        RecoveryNullableIndexedEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryNullableIndexedEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryUniqueEntity::ENTITY_TAG,
        <RecoveryUniqueEntity as EntitySchema>::MODEL,
        RecoveryUniqueEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryUniqueEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryUniqueCasefoldEntity::ENTITY_TAG,
        <RecoveryUniqueCasefoldEntity as EntitySchema>::MODEL,
        RecoveryUniqueCasefoldEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryUniqueCasefoldEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryUpperExpressionEntity::ENTITY_TAG,
        <RecoveryUpperExpressionEntity as EntitySchema>::MODEL,
        RecoveryUpperExpressionEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryUpperExpressionEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalEntity::ENTITY_TAG,
        <RecoveryConditionalEntity as EntitySchema>::MODEL,
        RecoveryConditionalEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryConditionalEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalUniqueEntity::ENTITY_TAG,
        <RecoveryConditionalUniqueEntity as EntitySchema>::MODEL,
        RecoveryConditionalUniqueEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryConditionalUniqueEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalUniqueCasefoldEntity::ENTITY_TAG,
        <RecoveryConditionalUniqueCasefoldEntity as EntitySchema>::MODEL,
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryConditionalUniqueCasefoldEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryConditionalUniqueEnumEntity::ENTITY_TAG,
        <RecoveryConditionalUniqueEnumEntity as EntitySchema>::MODEL,
        RecoveryConditionalUniqueEnumEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryConditionalUniqueEnumEntity>,
    ),
];

static PEER_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryPeerCanister>] =
    &[EntityRuntimeHooks::new(
        RecoveryPeerEntity::ENTITY_TAG,
        <RecoveryPeerEntity as EntitySchema>::MODEL,
        RecoveryPeerEntity::PATH,
        RecoveryPeerDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryPeerEntity>,
    )];

thread_local! {
    static RECOVERY_DATA_STORE: RefCell<DataStore> = RefCell::new(DataStore::init_journaled(test_memory(19)));
    static RECOVERY_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(20)));
    static RECOVERY_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(21)));
    static RECOVERY_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(22)));
    static HEAP_RECOVERY_DATA_STORE: RefCell<DataStore> =
        const { RefCell::new(DataStore::init_heap()) };
    static HEAP_RECOVERY_INDEX_STORE: RefCell<IndexStore> =
        const { RefCell::new(IndexStore::init_heap()) };
    static HEAP_RECOVERY_SCHEMA_STORE: RefCell<SchemaStore> =
        const { RefCell::new(SchemaStore::init_heap()) };
    static PEER_RECOVERY_DATA_STORE: RefCell<DataStore> =
        RefCell::new(DataStore::init_journaled(test_memory(23)));
    static PEER_RECOVERY_INDEX_STORE: RefCell<IndexStore> =
        RefCell::new(IndexStore::init_journaled(test_memory(24)));
    static PEER_RECOVERY_SCHEMA_STORE: RefCell<SchemaStore> =
        RefCell::new(SchemaStore::init_journaled(test_memory(25)));
    static PEER_RECOVERY_JOURNAL_STORE: RefCell<JournalTailStore> =
        RefCell::new(JournalTailStore::init(test_memory(26)));
    static STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            RecoveryTestDataStore::PATH,
            &RECOVERY_DATA_STORE,
            &RECOVERY_INDEX_STORE,
            &RECOVERY_SCHEMA_STORE,
            &RECOVERY_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(19, "icydb.test.recovery.data.v1"),
                crate::db::StoreAllocationIdentity::new(20, "icydb.test.recovery.index.v1"),
                crate::db::StoreAllocationIdentity::new(21, "icydb.test.recovery.schema.v1"),
                crate::db::StoreAllocationIdentity::new(22, "icydb.test.recovery.journal.v1"),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
            .expect("test store registration should succeed");
        reg.register_store(
            HeapRecoveryTestDataStore::PATH,
            &HEAP_RECOVERY_DATA_STORE,
            &HEAP_RECOVERY_INDEX_STORE,
            &HEAP_RECOVERY_SCHEMA_STORE,
            crate::db::StoreAllocationIdentities::absent(),
            crate::db::StoreRuntimeStorageCapabilities::heap(),
        )
            .expect("heap recovery test store registration should succeed");
        reg
    };
    static PEER_STORE_REGISTRY: StoreRegistry = {
        let mut reg = StoreRegistry::new();
        reg.register_journaled_store(
            RecoveryPeerDataStore::PATH,
            &PEER_RECOVERY_DATA_STORE,
            &PEER_RECOVERY_INDEX_STORE,
            &PEER_RECOVERY_SCHEMA_STORE,
            &PEER_RECOVERY_JOURNAL_STORE,
            crate::db::StoreAllocationIdentities::new_journaled(
                crate::db::StoreAllocationIdentity::new(23, "icydb.test.peer-recovery.data.v1"),
                crate::db::StoreAllocationIdentity::new(24, "icydb.test.peer-recovery.index.v1"),
                crate::db::StoreAllocationIdentity::new(25, "icydb.test.peer-recovery.schema.v1"),
                crate::db::StoreAllocationIdentity::new(26, "icydb.test.peer-recovery.journal.v1"),
            ),
            crate::db::StoreRuntimeStorageCapabilities::journaled(),
        )
        .expect("peer recovery test store registration should succeed");
        reg
    };
}

static DB: Db<RecoveryTestCanister> = Db::new_with_hooks(&STORE_REGISTRY, ENTITY_RUNTIME_HOOKS);
static PEER_DB: Db<RecoveryPeerCanister> =
    Db::new_with_hooks(&PEER_STORE_REGISTRY, PEER_ENTITY_RUNTIME_HOOKS);

static DUPLICATE_NAME_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryIndexedEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
];

static DUPLICATE_PATH_ENTITY_RUNTIME_HOOKS: &[EntityRuntimeHooks<RecoveryTestCanister>] = &[
    EntityRuntimeHooks::new(
        RecoveryTestEntity::ENTITY_TAG,
        <RecoveryTestEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryTestEntity>,
    ),
    EntityRuntimeHooks::new(
        RecoveryIndexedEntity::ENTITY_TAG,
        <RecoveryIndexedEntity as EntitySchema>::MODEL,
        RecoveryTestEntity::PATH,
        RecoveryTestDataStore::PATH,
        validate_delete_strong_relations_for_source::<RecoveryIndexedEntity>,
    ),
];

static DUPLICATE_PATH_DB: Db<RecoveryTestCanister> =
    Db::new_with_hooks(&STORE_REGISTRY, DUPLICATE_PATH_ENTITY_RUNTIME_HOOKS);

fn duplicate_name_db() -> Db<RecoveryTestCanister> {
    Db::new_with_hooks(&STORE_REGISTRY, DUPLICATE_NAME_ENTITY_RUNTIME_HOOKS)
}

fn with_recovery_store<R>(f: impl FnOnce(StoreHandle) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(RecoveryTestDataStore::PATH).map(f))
        .expect("recovery test store access should succeed")
}

fn with_peer_recovery_store<R>(f: impl FnOnce(StoreHandle) -> R) -> R {
    PEER_DB
        .with_store_registry(|reg| reg.try_get_store(RecoveryPeerDataStore::PATH).map(f))
        .expect("peer recovery test store access should succeed")
}

fn with_heap_recovery_store<R>(f: impl FnOnce(StoreHandle) -> R) -> R {
    DB.with_store_registry(|reg| reg.try_get_store(HeapRecoveryTestDataStore::PATH).map(f))
        .expect("heap recovery test store access should succeed")
}

fn init_peer_commit_store_for_tests() -> Result<(), InternalError> {
    super::memory::configure_commit_memory_id(
        RecoveryPeerCanister::COMMIT_MEMORY_ID,
        RecoveryPeerCanister::COMMIT_STABLE_KEY,
    )?;
    let allocation = super::memory::current_commit_memory_allocation()?;
    let control_memory = super::memory::commit_memory_handle(allocation)?;
    crate::db::database_format::initialize_current_database_control_for_tests(&control_memory);
    store::with_commit_store(|_| Ok(()))
}

// Reset marker + data store to isolate recovery tests.
fn reset_recovery_state() {
    clear_commit_failpoint_for_tests();
    init_commit_store_for_tests().expect("commit store init should succeed");
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker reset should succeed");

    with_recovery_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
        store.with_schema_mut(SchemaStore::clear);
        if let Some(journal_store) = store.journal_tail_store() {
            journal_store.with_borrow_mut(JournalTailStore::clear);
        }
    });
    reset_commit_marker_test_journal_sequence();
    with_heap_recovery_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
        store.with_schema_mut(SchemaStore::clear);
    });
    clear_recovery_runtime_state_for_tests(&DB)
        .expect("recovery fixture reset should clear volatile schema authority");
    ensure_recovered(&DB).expect("recovery fixture reset should publish accepted schema root");
}

fn reset_peer_recovery_state() {
    init_peer_commit_store_for_tests().expect("peer commit store init should succeed");
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("peer commit marker reset should succeed");

    with_peer_recovery_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
        store.with_schema_mut(SchemaStore::clear);
        if let Some(journal_store) = store.journal_tail_store() {
            journal_store.with_borrow_mut(JournalTailStore::clear);
        }
    });
    clear_recovery_runtime_state_for_tests(&PEER_DB)
        .expect("peer recovery fixture reset should clear volatile schema authority");
    ensure_recovered(&PEER_DB)
        .expect("peer recovery fixture reset should publish accepted schema root");
}

fn row_op_for_path_with_schema(
    path: &'static str,
    data_key: Vec<u8>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
    schema_fingerprint: [u8; 16],
) -> CommitRowOp {
    CommitRowOp::try_new_bytes(path, &data_key, before, after, schema_fingerprint)
        .expect("recovery test row op key bytes should decode")
}

fn seed_canonical_data_row_for_recovery(
    data_store: &mut DataStore,
    key: RawDataStoreKey,
    row: RawRow,
) {
    data_store
        .fold_recovered_journal_put(key, row)
        .expect("canonical recovery row seed should succeed");
}

fn row_op_for_path(
    path: &'static str,
    data_key: Vec<u8>,
    before: Option<Vec<u8>>,
    after: Option<Vec<u8>>,
) -> CommitRowOp {
    let schema_fingerprint = match path {
        RecoveryTestEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryTestEntity>()
        }
        RecoveryIndexedEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryIndexedEntity>()
        }
        RecoveryNullableIndexedEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryNullableIndexedEntity>()
        }
        RecoveryUniqueEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>()
        }
        RecoveryUniqueCasefoldEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueCasefoldEntity>()
        }
        RecoveryConditionalEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryConditionalEntity>()
        }
        RecoveryConditionalUniqueEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryConditionalUniqueEntity>(
            )
        }
        RecoveryConditionalUniqueCasefoldEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<
                RecoveryConditionalUniqueCasefoldEntity,
            >()
        }
        RecoveryConditionalUniqueEnumEntity::PATH => {
            initial_accepted_commit_schema_fingerprint_for_entity::<
                RecoveryConditionalUniqueEnumEntity,
            >()
        }
        _ => [0u8; 16],
    };
    row_op_for_path_with_schema(path, data_key, before, after, schema_fingerprint)
}

fn initial_accepted_commit_schema_fingerprint_for_entity<E: EntityKind + 'static>() -> [u8; 16] {
    let proposal = compiled_schema_proposal_for_model(E::MODEL);
    let accepted = AcceptedSchemaSnapshot::try_new(proposal.initial_persisted_schema_snapshot())
        .expect("initial recovery test schema snapshot should be accepted");

    accepted_commit_schema_fingerprint(&accepted)
        .expect("initial recovery test schema fingerprint should derive")
}

fn row_bytes_for(key: &RawDataStoreKey) -> Option<Vec<u8>> {
    with_recovery_store(|store| {
        store.with_data(|data_store| data_store.get(key).map(|row| row.as_bytes().to_vec()))
    })
}

fn indexed_ids_for(entity: &RecoveryIndexedEntity) -> Option<BTreeSet<Ulid>> {
    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let index_key = IndexKey::new(entity, index)
        .expect("index key build should succeed")
        .expect("index key should exist")
        .to_raw()
        .expect("test index key should encode");

    with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store.get(&index_key).map(|entry| {
                let primary_key_component = entry
                    .decode_row_identity(&index_key)
                    .expect("index entry decode should succeed")
                    .primary_key_value()
                    .scalar_component()
                    .expect("decoded index row identity should be scalar");
                let Value::Ulid(value) = primary_key_component.as_runtime_value() else {
                    panic!("decoded index key should be a Ulid");
                };
                BTreeSet::from([value])
            })
        })
    })
}

fn nullable_indexed_ids_for(entity: &RecoveryNullableIndexedEntity) -> Option<BTreeSet<Ulid>> {
    let index = RecoveryNullableIndexedEntity::MODEL.indexes()[0];
    let index_key = IndexKey::new(entity, index)
        .expect("nullable index key build should succeed")
        .expect("nullable index key should exist")
        .to_raw()
        .expect("test index key should encode");

    with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store.get(&index_key).map(|entry| {
                let primary_key_component = entry
                    .decode_row_identity(&index_key)
                    .expect("nullable index entry decode should succeed")
                    .primary_key_value()
                    .scalar_component()
                    .expect("decoded nullable index row identity should be scalar");
                let Value::Ulid(value) = primary_key_component.as_runtime_value() else {
                    panic!("decoded nullable index key should be a Ulid");
                };
                BTreeSet::from([value])
            })
        })
    })
}

// Encode one old physical row for `RecoveryNullableIndexedEntity` with only
// the pre-transition `id` and `group` slots. Startup recovery uses this to
// prove that current accepted schema can rebuild indexes from older rows.
fn old_nullable_indexed_raw_row_for_test(id: Ulid, group: u32) -> RawRow {
    let id_payload = encode_value_with_model_proposal_for_test(
        RecoveryNullableIndexedEntity::MODEL,
        0,
        &Value::Ulid(id),
    )
    .expect("old nullable indexed id payload should encode");
    let group_payload = encode_value_with_model_proposal_for_test(
        RecoveryNullableIndexedEntity::MODEL,
        1,
        &Value::Nat64(u64::from(group)),
    )
    .expect("old nullable indexed group payload should encode");
    let slot_payload =
        encode_commit_test_slot_payload(&[id_payload.as_slice(), group_payload.as_slice()]);

    RawRow::try_new(
        serialize_row_payload(slot_payload).expect("old nullable indexed row should serialize"),
    )
    .expect("old nullable indexed row should be valid raw row bytes")
}

// Build one dense slot-framed payload for owner-local row-layout fixtures.
// Production writers continue to own canonical complete-row serialization.
fn encode_commit_test_slot_payload(slots: &[&[u8]]) -> Vec<u8> {
    let field_count =
        u16::try_from(slots.len()).expect("commit slot fixture count should fit in u16");
    let mut row_payload = Vec::new();
    let mut payload_bytes = Vec::new();

    row_payload.extend_from_slice(&field_count.to_be_bytes());
    for bytes in slots {
        let start = u32::try_from(payload_bytes.len())
            .expect("commit slot fixture start should fit in u32");
        let len = u32::try_from(bytes.len()).expect("commit slot fixture length should fit in u32");
        row_payload.extend_from_slice(&start.to_be_bytes());
        row_payload.extend_from_slice(&len.to_be_bytes());
        payload_bytes.extend_from_slice(bytes);
    }
    row_payload.extend_from_slice(payload_bytes.as_slice());

    row_payload
}

// Install the accepted schema snapshot that represents
// `RecoveryNullableIndexedEntity` before `nickname` was added. Reconciliation
// must accept this as an append-only nullable transition during startup.
fn install_nullable_indexed_old_accepted_schema_prefix() {
    let proposal =
        compiled_schema_proposal_for_model(<RecoveryNullableIndexedEntity as EntitySchema>::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let stored_version = SchemaVersion::new(expected.version().get().saturating_sub(1));
    let stored_prefix = PersistedSchemaSnapshot::new(
        stored_version,
        expected.entity_path().to_string(),
        expected.entity_name().to_string(),
        expected.first_primary_key_field_id(),
        SchemaRowLayout::new(
            stored_version,
            vec![
                (FieldId::new(1), SchemaFieldSlot::new(0)),
                (FieldId::new(2), SchemaFieldSlot::new(1)),
            ],
        ),
        expected.fields()[..2].to_vec(),
    );

    with_recovery_store(|store| {
        store.with_schema_mut(|schema_store| {
            schema_store
                .insert_persisted_snapshot(
                    RecoveryNullableIndexedEntity::ENTITY_TAG,
                    &stored_prefix,
                )
                .expect("old nullable indexed schema prefix should persist");
        });
    });
}

fn conditional_indexed_ids_for(entity: &RecoveryConditionalEntity) -> Option<BTreeSet<Ulid>> {
    let index = RecoveryConditionalEntity::MODEL.indexes()[0];
    let index_key = IndexKey::new(entity, index)
        .expect("conditional index key build should succeed")
        .expect("conditional index key should exist")
        .to_raw()
        .expect("test index key should encode");

    with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store.get(&index_key).map(|entry| {
                let primary_key_component = entry
                    .decode_row_identity(&index_key)
                    .expect("conditional index entry decode should succeed")
                    .primary_key_value()
                    .scalar_component()
                    .expect("decoded conditional index row identity should be scalar");
                let Value::Ulid(value) = primary_key_component.as_runtime_value() else {
                    panic!("decoded conditional index key should be a Ulid");
                };
                BTreeSet::from([value])
            })
        })
    })
}

fn upper_expression_indexed_ids_for(
    entity: &RecoveryUpperExpressionEntity,
) -> Option<BTreeSet<Ulid>> {
    let index = RecoveryUpperExpressionEntity::MODEL.indexes()[0];
    let index_key = IndexKey::new(entity, index)
        .expect("expression index key build should succeed")
        .expect("expression index key should exist")
        .to_raw()
        .expect("test index key should encode");

    with_recovery_store(|store| {
        store.with_index(|index_store| {
            index_store.get(&index_key).map(|entry| {
                let primary_key_component = entry
                    .decode_row_identity(&index_key)
                    .expect("expression index entry decode should succeed")
                    .primary_key_value()
                    .scalar_component()
                    .expect("decoded expression index row identity should be scalar");
                let Value::Ulid(value) = primary_key_component.as_runtime_value() else {
                    panic!("decoded expression index key should be a Ulid");
                };
                BTreeSet::from([value])
            })
        })
    })
}

fn encoded_index_key_bytes<E>(entity: &E, index: &IndexModel) -> Vec<u8>
where
    E: crate::traits::EntityKind + crate::traits::EntityValue,
{
    encoded_index_key(entity, index).as_bytes().to_vec()
}

fn encoded_index_key<E>(entity: &E, index: &IndexModel) -> RawIndexStoreKey
where
    E: crate::traits::EntityKind + crate::traits::EntityValue,
{
    IndexKey::new(entity, index)
        .expect("characterization index key should build")
        .expect("characterization index key should exist")
        .to_raw()
        .expect("characterization index key should encode")
}

fn seed_indexed_recovery_entity(data_store: &mut DataStore, entity: &RecoveryIndexedEntity) {
    let data_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(entity.id)
        .expect("indexed characterization data key should build")
        .to_raw()
        .expect("indexed characterization data key should encode");
    let raw_row =
        RawRow::try_new(indexed_row_bytes(entity)).expect("indexed raw row should construct");

    seed_canonical_data_row_for_recovery(data_store, data_key, raw_row);
}

fn seed_conditional_recovery_entity(
    data_store: &mut DataStore,
    entity: &RecoveryConditionalEntity,
) {
    let data_key = DecodedDataStoreKey::try_new::<RecoveryConditionalEntity>(entity.id)
        .expect("conditional characterization data key should build")
        .to_raw()
        .expect("conditional characterization data key should encode");
    let raw_row = RawRow::try_new(conditional_row_bytes(entity))
        .expect("conditional raw row should construct");

    seed_canonical_data_row_for_recovery(data_store, data_key, raw_row);
}

fn seed_upper_expression_recovery_entity(
    data_store: &mut DataStore,
    entity: &RecoveryUpperExpressionEntity,
) {
    let data_key = DecodedDataStoreKey::try_new::<RecoveryUpperExpressionEntity>(entity.id)
        .expect("expression characterization data key should build")
        .to_raw()
        .expect("expression characterization data key should encode");
    let raw_row =
        RawRow::try_new(canonical_row_bytes(entity)).expect("expression raw row should construct");

    seed_canonical_data_row_for_recovery(data_store, data_key, raw_row);
}

fn mixed_index_shape_stale_keys(
    indexed: &IndexModel,
    conditional: &IndexModel,
    expression: &IndexModel,
) -> [RawIndexStoreKey; 3] {
    let stale_indexed = RecoveryIndexedEntity {
        id: Ulid::from_u128(29_999),
        group: 999,
    };
    let stale_conditional = RecoveryConditionalEntity {
        id: Ulid::from_u128(39_999),
        group: 999,
        active: false,
    };
    let stale_expression = RecoveryUpperExpressionEntity {
        id: Ulid::from_u128(49_999),
        email: "stale@example.com".to_string(),
    };

    [
        encoded_index_key(&stale_indexed, indexed),
        encoded_index_key(&stale_conditional, conditional),
        encoded_index_key(&stale_expression, expression),
    ]
}

fn index_key_bytes_snapshot() -> Vec<Vec<u8>> {
    let mut keys = with_recovery_store(|store| {
        store.with_index(|index_store| {
            let mut keys = Vec::new();
            let _: Result<(), std::convert::Infallible> =
                index_store.visit_entries(|raw_key, _| {
                    keys.push(raw_key.as_bytes().to_vec());
                    Ok(IndexStoreVisit::Continue)
                });
            keys
        })
    });
    keys.sort();
    keys
}

// Capture one deterministic snapshot of row-store and index-store raw bytes.
fn recovery_store_snapshot() -> RecoveryStoreSnapshot {
    with_recovery_store(|store| {
        let mut data_rows = store.with_data(|data_store| {
            let mut rows = Vec::new();
            let _: Result<(), crate::error::InternalError> =
                data_store.visit_entries(|raw_key, raw_row| {
                    rows.push((raw_key.as_bytes().to_vec(), raw_row.as_bytes().to_vec()));
                    Ok(StoreVisit::Continue)
                });
            rows
        });
        let mut index_rows = store.with_index(|index_store| {
            let mut rows = Vec::new();
            let _: Result<(), std::convert::Infallible> =
                index_store.visit_entries(|raw_key, raw_entry| {
                    rows.push((raw_key.as_bytes().to_vec(), raw_entry.as_bytes().to_vec()));
                    Ok(IndexStoreVisit::Continue)
                });
            rows
        });
        data_rows.sort();
        index_rows.sort();

        (data_rows, index_rows)
    })
}

fn peer_recovery_store_snapshot() -> RecoveryStoreSnapshot {
    with_peer_recovery_store(|store| {
        let mut data_rows = store.with_data(|data_store| {
            let mut rows = Vec::new();
            let _: Result<(), crate::error::InternalError> =
                data_store.visit_entries(|raw_key, raw_row| {
                    rows.push((raw_key.as_bytes().to_vec(), raw_row.as_bytes().to_vec()));
                    Ok(StoreVisit::Continue)
                });
            rows
        });
        let mut index_rows = store.with_index(|index_store| {
            let mut rows = Vec::new();
            let _: Result<(), std::convert::Infallible> =
                index_store.visit_entries(|raw_key, raw_entry| {
                    rows.push((raw_key.as_bytes().to_vec(), raw_entry.as_bytes().to_vec()));
                    Ok(IndexStoreVisit::Continue)
                });
            rows
        });
        data_rows.sort();
        index_rows.sort();

        (data_rows, index_rows)
    })
}

// Apply prepared row operations through the forward (non-recovery) apply path.
fn apply_row_ops_forward(row_ops: &[CommitRowOp]) -> Result<(), InternalError> {
    for row_op in row_ops {
        DB.prepare_row_commit_op(row_op)?.apply();
    }

    Ok(())
}

fn mixed_recovery_marker_failpoint_case() -> RecoveryFailpointCase {
    let seed_ops = mixed_recovery_seed_ops();
    let marker_ops = mixed_recovery_marker_ops();

    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice()).expect("seed state should apply for oracle");
    let pre_snapshot = recovery_store_snapshot();
    apply_row_ops_forward(marker_ops.as_slice()).expect("marker ops should apply for oracle");
    let post_snapshot = recovery_store_snapshot();

    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice()).expect("seed state should apply before failpoint");
    let pre_watermark = recovery_journal_fold_watermark();
    assert_eq!(
        recovery_store_snapshot(),
        pre_snapshot,
        "failpoint case must begin from the oracle pre-state",
    );
    let marker = CommitMarker::new(marker_ops).expect("mixed marker should build");
    let post_watermark = recovery_post_watermark(&marker, pre_watermark);

    RecoveryFailpointCase {
        marker,
        pre_snapshot,
        post_snapshot,
        pre_watermark,
        post_watermark,
    }
}

fn recovery_journal_tail_batch_count() -> u64 {
    with_recovery_store(|store| {
        store
            .journal_tail_store()
            .expect("recovery store should expose a journal tail")
            .with_borrow(JournalTailStore::len)
    })
}

fn recovery_journal_fold_watermark() -> FoldWatermark {
    with_recovery_store(|store| {
        store
            .journal_tail_store()
            .expect("recovery store should expose a journal tail")
            .with_borrow(JournalTailStore::fold_watermark)
            .expect("journal fold watermark should decode")
    })
}

fn recovery_index_state() -> IndexState {
    with_recovery_store(|store| store.index_state())
}

fn recovery_post_watermark(marker: &CommitMarker, pre_watermark: FoldWatermark) -> FoldWatermark {
    marker
        .journal_batches()
        .last()
        .map_or(pre_watermark, |batch| {
            FoldWatermark::new(
                batch.journal_sequence(),
                pre_watermark
                    .fold_epoch()
                    .checked_add(1)
                    .expect("test fold epoch should advance"),
            )
        })
}

fn assert_begin_commit_failpoint(marker: &CommitMarker, mode: CommitFailpointMode) {
    match mode.failure_class() {
        CommitFailpointFailureClass::StructuredReturnedError => {
            assert!(
                begin_commit(marker.clone()).is_err(),
                "begin_commit failpoint should return error",
            );
        }
        CommitFailpointFailureClass::HostUnwindInterruption => {
            let result = catch_unwind(AssertUnwindSafe(|| begin_commit(marker.clone())));
            assert!(
                result.is_err(),
                "begin_commit failpoint should unwind at the armed site",
            );
        }
    }
}

fn assert_recovery_failpoint(mode: CommitFailpointMode) {
    match mode.failure_class() {
        CommitFailpointFailureClass::StructuredReturnedError => {
            ensure_recovered(&DB).expect_err("recovery failpoint should return error");
        }
        CommitFailpointFailureClass::HostUnwindInterruption => {
            let result = catch_unwind(AssertUnwindSafe(|| ensure_recovered(&DB)));
            assert!(
                result.is_err(),
                "recovery failpoint should unwind at the armed site",
            );
        }
    }
}

fn assert_failpoint_interruption_oracle(site: CommitFailpoint, case: &RecoveryFailpointCase) {
    let oracle = site.recovery_oracle();
    let (expected_snapshot, expected_watermark) = case.interruption_state_for(site);

    assert_eq!(
        &recovery_store_snapshot(),
        expected_snapshot,
        "failpoint site should expose the classified recovery snapshot oracle",
    );
    assert_eq!(
        commit_marker_present().expect("commit marker check should succeed"),
        oracle.marker_present(),
        "failpoint site should expose the classified marker-presence oracle",
    );
    assert_eq!(
        recovery_journal_tail_batch_count(),
        oracle.journal_tail_batches(),
        "failpoint site should expose the classified journal-tail oracle",
    );
    assert_eq!(
        recovery_journal_fold_watermark(),
        expected_watermark,
        "failpoint site should expose the classified fold-watermark oracle",
    );
}

fn assert_journal_tail_fold_interruption_oracle(
    site: CommitFailpoint,
    case: &RecoveryFailpointCase,
) {
    let oracle = site.recovery_oracle();
    assert_eq!(
        oracle.snapshot(),
        CommitFailpointSnapshotOracle::RecoveryIntermediate,
        "journal-tail fold failpoint should declare an intermediate recovery snapshot",
    );
    assert!(
        oracle.marker_present(),
        "journal-tail fold interruption should leave marker authority present",
    );
    assert_eq!(
        recovery_journal_tail_batch_count(),
        oracle.journal_tail_batches(),
        "journal-tail fold interruption should keep the tail visible until cleanup",
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "journal-tail fold interruption should leave the marker visible",
    );

    let (data_rows, index_rows) = recovery_store_snapshot();
    assert_eq!(
        data_rows, case.post_snapshot.0,
        "journal-tail fold should make canonical rows reflect the marker-authorized state",
    );
    assert_eq!(
        index_rows, case.pre_snapshot.1,
        "journal-tail fold should not mark derived index state rebuilt",
    );

    assert_eq!(
        recovery_journal_fold_watermark(),
        case.post_watermark,
        "journal-tail fold interruption should expose the classified fold watermark",
    );
}

fn assert_secondary_index_rebuild_clear_interruption_oracle(
    case: &RecoveryFailpointCase,
    mode: CommitFailpointMode,
) {
    let oracle = CommitFailpoint::AfterSecondaryIndexRebuildClear.recovery_oracle();
    assert_eq!(
        oracle.snapshot(),
        CommitFailpointSnapshotOracle::RecoveryIntermediate,
        "secondary-index rebuild clear should declare an intermediate recovery snapshot",
    );
    assert!(
        oracle.marker_present(),
        "secondary-index rebuild interruption should leave marker authority present",
    );
    assert_eq!(
        commit_marker_present().expect("commit marker check should succeed"),
        oracle.marker_present(),
        "secondary-index rebuild interruption should leave marker authority visible",
    );
    assert_eq!(
        recovery_journal_tail_batch_count(),
        oracle.journal_tail_batches(),
        "secondary-index rebuild interruption should occur after folded tail cleanup",
    );
    assert_eq!(
        recovery_journal_fold_watermark(),
        case.post_watermark,
        "secondary-index rebuild interruption should keep the folded watermark",
    );

    let (data_rows, index_rows) = recovery_store_snapshot();
    assert_eq!(
        data_rows, case.post_snapshot.0,
        "secondary-index rebuild should run after canonical rows reach post-state",
    );

    match mode.failure_class() {
        CommitFailpointFailureClass::StructuredReturnedError => {
            assert_eq!(
                index_rows, case.pre_snapshot.1,
                "returned rebuild errors should restore the pre-rebuild index snapshot",
            );
            assert_eq!(
                recovery_index_state(),
                IndexState::Ready,
                "returned rebuild errors should restore the pre-rebuild index state",
            );
        }
        CommitFailpointFailureClass::HostUnwindInterruption => {
            assert!(
                index_rows.is_empty(),
                "host unwind should leave the cleared derived index state for guarded retry",
            );
            assert_eq!(
                recovery_index_state(),
                IndexState::Building,
                "host unwind should leave indexes non-ready until guarded retry",
            );
        }
    }
}

fn assert_journaled_index_fold_interruption_oracle(case: &RecoveryFailpointCase) {
    let oracle = CommitFailpoint::AfterJournaledIndexMaterializedViewFold.recovery_oracle();
    assert_eq!(
        oracle.snapshot(),
        CommitFailpointSnapshotOracle::RecoveryIntermediate,
        "journaled index fold should declare an intermediate recovery snapshot",
    );
    assert!(
        oracle.marker_present(),
        "journaled index fold interruption should leave marker authority present",
    );
    assert_eq!(
        commit_marker_present().expect("commit marker check should succeed"),
        oracle.marker_present(),
        "journaled index fold interruption should leave marker authority visible",
    );
    assert_eq!(
        recovery_journal_tail_batch_count(),
        oracle.journal_tail_batches(),
        "journaled index fold interruption should occur after folded tail cleanup",
    );
    assert_eq!(
        recovery_journal_fold_watermark(),
        case.post_watermark,
        "journaled index fold interruption should keep the folded watermark",
    );
    assert_eq!(
        recovery_store_snapshot(),
        case.post_snapshot,
        "journaled index fold should expose marker-authorized row/index bytes",
    );
    assert_eq!(
        recovery_index_state(),
        IndexState::Building,
        "journaled index fold should not mark indexes ready before recovery closes",
    );
}

fn assert_markerless_journaled_index_fold_reentry_oracle(case: &RecoveryFailpointCase) {
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "marker-cleared reentry should keep marker authority absent",
    );
    assert_eq!(
        recovery_journal_tail_batch_count(),
        0,
        "marker-cleared reentry should not resurrect journal tail batches",
    );
    assert_eq!(
        recovery_journal_fold_watermark(),
        case.post_watermark,
        "marker-cleared reentry should keep the post-recovery fold watermark",
    );
    assert_eq!(
        recovery_store_snapshot(),
        case.post_snapshot,
        "marker-cleared reentry should keep marker-authorized row/index bytes",
    );
    assert_eq!(
        recovery_index_state(),
        IndexState::Building,
        "marker-cleared reentry should not mark indexes ready before recovery closes",
    );
}

fn indexed_data_key(id: Ulid) -> RawDataStoreKey {
    DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(id)
        .expect("indexed key should build")
        .to_raw()
        .expect("indexed key should encode")
}

fn peer_data_key(id: Ulid) -> RawDataStoreKey {
    DecodedDataStoreKey::try_new::<RecoveryPeerEntity>(id)
        .expect("peer key should build")
        .to_raw()
        .expect("peer key should encode")
}

fn unique_data_key(id: Ulid) -> RawDataStoreKey {
    DecodedDataStoreKey::try_new::<RecoveryUniqueEntity>(id)
        .expect("unique key should build")
        .to_raw()
        .expect("unique key should encode")
}

fn conditional_data_key(id: Ulid) -> RawDataStoreKey {
    DecodedDataStoreKey::try_new::<RecoveryConditionalEntity>(id)
        .expect("conditional key should build")
        .to_raw()
        .expect("conditional key should encode")
}

fn conditional_unique_data_key(id: Ulid) -> RawDataStoreKey {
    DecodedDataStoreKey::try_new::<RecoveryConditionalUniqueEntity>(id)
        .expect("conditional-unique key should build")
        .to_raw()
        .expect("conditional-unique key should encode")
}

fn conditional_unique_casefold_data_key(id: Ulid) -> RawDataStoreKey {
    DecodedDataStoreKey::try_new::<RecoveryConditionalUniqueCasefoldEntity>(id)
        .expect("conditional-unique-casefold key should build")
        .to_raw()
        .expect("conditional-unique-casefold key should encode")
}

fn indexed_row_bytes(entity: &RecoveryIndexedEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn peer_row_bytes(entity: &RecoveryPeerEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn unique_row_bytes(entity: &RecoveryUniqueEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn conditional_row_bytes(entity: &RecoveryConditionalEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn conditional_unique_row_bytes(entity: &RecoveryConditionalUniqueEntity) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn conditional_unique_casefold_row_bytes(
    entity: &RecoveryConditionalUniqueCasefoldEntity,
) -> Vec<u8> {
    canonical_row_bytes(entity)
}

fn canonical_row_bytes<E: crate::db::PersistedRow + crate::traits::EntityValue>(
    entity: &E,
) -> Vec<u8> {
    CanonicalRow::from_entity_with_model_proposal_for_test(entity)
        .expect("canonical row encoding should succeed")
        .into_raw_row()
        .as_bytes()
        .to_vec()
}

fn canonical_row_payload_bytes<E: crate::db::PersistedRow + crate::traits::EntityValue>(
    entity: &E,
) -> Vec<u8> {
    let row = CanonicalRow::from_entity_with_model_proposal_for_test(entity)
        .expect("canonical row encoding should succeed")
        .into_raw_row();

    decode_row_payload_bytes(row.as_bytes())
        .expect("canonical row payload should decode")
        .into_owned()
}

fn peer_insert_marker(entity: &RecoveryPeerEntity) -> CommitMarker {
    let schema_fingerprint =
        initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryPeerEntity>();
    let row_op = row_op_for_path_with_schema(
        RecoveryPeerEntity::PATH,
        peer_data_key(entity.id).as_bytes().to_vec(),
        None,
        Some(peer_row_bytes(entity)),
        schema_fingerprint,
    );

    CommitMarker::new(vec![row_op]).expect("peer recovery marker should build")
}

fn next_model_recovery_seed(seed: &mut u64) -> u64 {
    *seed = seed
        .wrapping_mul(6_364_136_223_846_793_005)
        .wrapping_add(1_442_695_040_888_963_407);
    *seed
}

fn model_recovery_indexed_entity(slot: u64, value: u64) -> RecoveryIndexedEntity {
    RecoveryIndexedEntity {
        id: Ulid::from_u128(18_000 + u128::from(slot)),
        group: u32::try_from(value % 97).expect("modulo value should fit in u32") + 1,
    }
}

fn deterministic_model_recovery_batches() -> Vec<Vec<CommitRowOp>> {
    let mut seed = 0x4D4F_4445_4C52_4543_u64;
    let mut live = BTreeMap::<u64, RecoveryIndexedEntity>::new();
    let mut batches = Vec::new();

    for _ in 0..12 {
        let mut batch = Vec::new();
        for _ in 0..4 {
            let value = next_model_recovery_seed(&mut seed);
            let slot = value % 7;
            let before = live.get(&slot).map(indexed_row_bytes);
            let should_delete = before.is_some() && value.is_multiple_of(5);
            let key = indexed_data_key(Ulid::from_u128(18_000 + u128::from(slot)));
            let after = if should_delete {
                live.remove(&slot);
                None
            } else {
                let entity = model_recovery_indexed_entity(slot, value);
                live.insert(slot, entity.clone());
                Some(indexed_row_bytes(&entity))
            };

            batch.push(row_op_for_path(
                RecoveryIndexedEntity::PATH,
                key.as_bytes().to_vec(),
                before,
                after,
            ));
        }
        batches.push(batch);
    }

    batches
}

fn model_recovery_failpoint_case(target_batch_index: usize) -> RecoveryFailpointCase {
    let batches = deterministic_model_recovery_batches();
    let (seed_batches, target_batches) = batches.split_at(target_batch_index);
    let marker_ops = target_batches
        .first()
        .expect("model recovery target batch should exist")
        .clone();

    reset_recovery_state();
    for batch in seed_batches {
        apply_row_ops_forward(batch).expect("model seed state should apply for oracle");
    }
    let pre_snapshot = recovery_store_snapshot();
    apply_row_ops_forward(marker_ops.as_slice())
        .expect("model marker batch should apply for oracle");
    let post_snapshot = recovery_store_snapshot();

    reset_recovery_state();
    for batch in seed_batches {
        let marker = CommitMarker::new(batch.clone()).expect("model seed marker should build");
        begin_commit(marker).expect("model seed marker should persist before recovery");
        ensure_recovered(&DB).expect("model seed marker should recover");
    }
    assert_eq!(
        recovery_store_snapshot(),
        pre_snapshot,
        "model failpoint case must begin from the oracle pre-state",
    );
    let pre_watermark = recovery_journal_fold_watermark();
    let marker = CommitMarker::new(marker_ops).expect("model recovery marker should build");
    let post_watermark = recovery_post_watermark(&marker, pre_watermark);

    RecoveryFailpointCase {
        marker,
        pre_snapshot,
        post_snapshot,
        pre_watermark,
        post_watermark,
    }
}

const RECOVERY_STATUS_ENUM_PATH: &str = "db::commit::tests::RecoveryConditionalStatus";
static RECOVERY_STATUS_VARIANTS: [EnumVariantModel; 1] = [EnumVariantModel::new(
    "Pending",
    None,
    FieldStorageDecode::ByKind,
)];

fn enum_status(_variant: &str) -> RecoveryStatus {
    RecoveryStatus(ValueEnum::test_unit(1, 1))
}

// Build one deterministic seed snapshot used by forward/replay equivalence checks.
fn mixed_recovery_seed_ops() -> Vec<CommitRowOp> {
    let indexed_first_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9301),
        group: 41,
    };
    let indexed_second_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9302),
        group: 41,
    };
    let unique_first_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9303),
        email: "case-a@example.com".to_string(),
    };
    let unique_second_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9304),
        email: "case-b@example.com".to_string(),
    };

    vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_first_v1.id).as_bytes().to_vec(),
            None,
            Some(indexed_row_bytes(&indexed_first_v1)),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_second_v1.id).as_bytes().to_vec(),
            None,
            Some(indexed_row_bytes(&indexed_second_v1)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_first_v1.id).as_bytes().to_vec(),
            None,
            Some(unique_row_bytes(&unique_first_v1)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_second_v1.id).as_bytes().to_vec(),
            None,
            Some(unique_row_bytes(&unique_second_v1)),
        ),
    ]
}

// Build one mixed marker sequence with one operation per key over the seeded snapshot.
fn mixed_recovery_marker_ops() -> Vec<CommitRowOp> {
    let indexed_first_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9301),
        group: 41,
    };
    let indexed_first_v2 = RecoveryIndexedEntity {
        id: indexed_first_v1.id,
        group: 42,
    };
    let indexed_second_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9302),
        group: 41,
    };
    let indexed_third_v1 = RecoveryIndexedEntity {
        id: Ulid::from_u128(9305),
        group: 42,
    };
    let unique_first_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9303),
        email: "case-a@example.com".to_string(),
    };
    let unique_first_v2 = RecoveryUniqueEntity {
        id: unique_first_v1.id,
        email: "case-a2@example.com".to_string(),
    };
    let unique_second_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9304),
        email: "case-b@example.com".to_string(),
    };
    let unique_third_v1 = RecoveryUniqueEntity {
        id: Ulid::from_u128(9306),
        email: "case-c@example.com".to_string(),
    };

    vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_first_v1.id).as_bytes().to_vec(),
            Some(indexed_row_bytes(&indexed_first_v1)),
            Some(indexed_row_bytes(&indexed_first_v2)),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_second_v1.id).as_bytes().to_vec(),
            Some(indexed_row_bytes(&indexed_second_v1)),
            None,
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_first_v1.id).as_bytes().to_vec(),
            Some(unique_row_bytes(&unique_first_v1)),
            Some(unique_row_bytes(&unique_first_v2)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_second_v1.id).as_bytes().to_vec(),
            Some(unique_row_bytes(&unique_second_v1)),
            None,
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            indexed_data_key(indexed_third_v1.id).as_bytes().to_vec(),
            None,
            Some(indexed_row_bytes(&indexed_third_v1)),
        ),
        row_op_for_path(
            RecoveryUniqueEntity::PATH,
            unique_data_key(unique_third_v1.id).as_bytes().to_vec(),
            None,
            Some(unique_row_bytes(&unique_third_v1)),
        ),
    ]
}

// Build one deterministic conditional-index seed snapshot used by forward/replay checks.
fn conditional_recovery_seed_ops() -> Vec<CommitRowOp> {
    let activate_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9401),
        group: 31,
        active: false,
    };
    let deactivate_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9402),
        group: 32,
        active: true,
    };
    let move_key_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 33,
        active: true,
    };
    let delete_active_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9404),
        group: 35,
        active: true,
    };
    let delete_inactive_later = RecoveryConditionalEntity {
        id: Ulid::from_u128(9405),
        group: 36,
        active: false,
    };

    vec![
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(activate_later.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&activate_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(deactivate_later.id)
                .as_bytes()
                .to_vec(),
            None,
            Some(conditional_row_bytes(&deactivate_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(move_key_later.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&move_key_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_active_later.id)
                .as_bytes()
                .to_vec(),
            None,
            Some(conditional_row_bytes(&delete_active_later)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_inactive_later.id)
                .as_bytes()
                .to_vec(),
            None,
            Some(conditional_row_bytes(&delete_inactive_later)),
        ),
    ]
}

// Build one deterministic conditional-index marker sequence that spans membership transitions.
fn conditional_recovery_marker_ops() -> Vec<CommitRowOp> {
    let activate_before = RecoveryConditionalEntity {
        id: Ulid::from_u128(9401),
        group: 31,
        active: false,
    };
    let activate_after = RecoveryConditionalEntity {
        id: activate_before.id,
        group: activate_before.group,
        active: true,
    };
    let deactivate_before = RecoveryConditionalEntity {
        id: Ulid::from_u128(9402),
        group: 32,
        active: true,
    };
    let deactivate_after = RecoveryConditionalEntity {
        id: deactivate_before.id,
        group: deactivate_before.group,
        active: false,
    };
    let move_before = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 33,
        active: true,
    };
    let move_after = RecoveryConditionalEntity {
        id: move_before.id,
        group: 34,
        active: true,
    };
    let delete_active = RecoveryConditionalEntity {
        id: Ulid::from_u128(9404),
        group: 35,
        active: true,
    };
    let delete_inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(9405),
        group: 36,
        active: false,
    };
    let insert_inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(9406),
        group: 37,
        active: false,
    };
    let insert_active = RecoveryConditionalEntity {
        id: Ulid::from_u128(9407),
        group: 38,
        active: true,
    };

    vec![
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(activate_before.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&activate_before)),
            Some(conditional_row_bytes(&activate_after)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(deactivate_before.id)
                .as_bytes()
                .to_vec(),
            Some(conditional_row_bytes(&deactivate_before)),
            Some(conditional_row_bytes(&deactivate_after)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(move_before.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&move_before)),
            Some(conditional_row_bytes(&move_after)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_active.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&delete_active)),
            None,
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(delete_inactive.id).as_bytes().to_vec(),
            Some(conditional_row_bytes(&delete_inactive)),
            None,
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(insert_inactive.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&insert_inactive)),
        ),
        row_op_for_path(
            RecoveryConditionalEntity::PATH,
            conditional_data_key(insert_active.id).as_bytes().to_vec(),
            None,
            Some(conditional_row_bytes(&insert_active)),
        ),
    ]
}

#[test]
fn commit_forward_apply_and_replay_preserve_identical_store_state_for_mixed_marker_sequence() {
    let seed_ops = mixed_recovery_seed_ops();
    let marker_ops = mixed_recovery_marker_ops();

    // Phase 1: seed one shared pre-commit snapshot and apply forward marker mutations.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("seed state apply should succeed for mixed fixture");
    apply_row_ops_forward(marker_ops.as_slice())
        .expect("forward apply should succeed for deterministic mixed marker sequence");
    let forward_snapshot = recovery_store_snapshot();

    // Phase 2: replay the same marker from the same seeded snapshot and compare outcomes.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("seed state apply should succeed before replay marker");
    let marker =
        CommitMarker::new(marker_ops).expect("mixed marker sequence should build for replay path");
    begin_commit(marker).expect("replay marker begin_commit should persist marker");
    ensure_recovered(&DB).unwrap_or_else(|err| {
        panic!(
            "replay marker should recover successfully: class={:?} origin={:?} detail={:?}",
            err.class, err.origin, err.detail,
        )
    });
    let replay_snapshot = recovery_store_snapshot();

    assert_eq!(
        replay_snapshot, forward_snapshot,
        "forward apply and replay must converge on identical data/index store state"
    );
    assert!(
        !commit_marker_present().expect("commit marker presence check should succeed"),
        "successful replay must clear the persisted marker"
    );
}

#[test]
fn recovery_replays_deterministic_model_batches_through_guarded_reentry() {
    let batches = deterministic_model_recovery_batches();

    reset_recovery_state();
    for batch in &batches {
        apply_row_ops_forward(batch).expect("model batch forward apply should succeed");
    }
    let forward_snapshot = recovery_store_snapshot();

    reset_recovery_state();
    for batch in batches {
        let marker = CommitMarker::new(batch).expect("model recovery marker should build");
        begin_commit(marker).expect("model marker should persist before guarded recovery");
        assert!(
            commit_marker_present().expect("commit marker check should succeed"),
            "model marker must be visible before guarded recovery",
        );

        ensure_recovered(&DB).expect("guarded recovery should replay model marker");
        assert!(
            !commit_marker_present().expect("commit marker check should succeed"),
            "guarded recovery must clear each model marker",
        );
        assert_eq!(
            recovery_journal_tail_batch_count(),
            0,
            "guarded recovery should leave no model journal tail batches",
        );
    }

    assert_eq!(
        recovery_store_snapshot(),
        forward_snapshot,
        "deterministic model recovery should converge with forward apply",
    );
}

#[test]
fn recovery_model_batch_failpoints_follow_classified_interruption_oracles() {
    for site in [
        CommitFailpoint::BeforeMarkerWrite,
        CommitFailpoint::AfterMarkerWrite,
        CommitFailpoint::BeforeMarkerBoundJournalAppend,
        CommitFailpoint::AfterMarkerBoundJournalAppend,
        CommitFailpoint::BeforeMarkerClear,
        CommitFailpoint::AfterMarkerClear,
    ] {
        for mode in [
            CommitFailpointMode::ReturnError,
            CommitFailpointMode::PanicUnwind,
        ] {
            let case = model_recovery_failpoint_case(5);

            match site {
                CommitFailpoint::BeforeMarkerWrite | CommitFailpoint::AfterMarkerWrite => {
                    arm_commit_failpoint_for_tests(site, mode);
                    assert_begin_commit_failpoint(&case.marker, mode);
                }
                CommitFailpoint::BeforeMarkerBoundJournalAppend
                | CommitFailpoint::AfterMarkerBoundJournalAppend
                | CommitFailpoint::BeforeMarkerClear
                | CommitFailpoint::AfterMarkerClear => {
                    begin_commit(case.marker.clone())
                        .expect("model marker should persist before failpoint");
                    arm_commit_failpoint_for_tests(site, mode);
                    assert_recovery_failpoint(mode);
                }
                CommitFailpoint::BeforeJournalTailFoldBatch
                | CommitFailpoint::AfterJournalTailFoldWatermarkPersist
                | CommitFailpoint::AfterSecondaryIndexRebuildClear
                | CommitFailpoint::AfterJournaledIndexMaterializedViewFold => {
                    panic!("expanded recovery failpoints are covered by dedicated tests")
                }
            }

            assert_failpoint_interruption_oracle(site, &case);

            ensure_recovered(&DB).expect("model failpoint retry should converge");
            let (expected_final, expected_watermark) = case.retry_state_for(site);
            assert_eq!(
                &recovery_store_snapshot(),
                expected_final,
                "model failpoint retry must converge to the classified final snapshot",
            );
            assert_eq!(
                recovery_journal_fold_watermark(),
                expected_watermark,
                "model failpoint retry must converge to the classified fold watermark",
            );
            assert!(
                !commit_marker_present().expect("commit marker check should succeed"),
                "model failpoint retry should leave no marker",
            );
        }
    }
}

#[test]
fn recovery_journal_tail_fold_failpoints_are_retryable_for_error_and_unwind() {
    for site in [
        CommitFailpoint::BeforeJournalTailFoldBatch,
        CommitFailpoint::AfterJournalTailFoldWatermarkPersist,
    ] {
        for mode in [
            CommitFailpointMode::ReturnError,
            CommitFailpointMode::PanicUnwind,
        ] {
            let case = model_recovery_failpoint_case(5);
            begin_commit(case.marker.clone()).expect("model marker should persist before recovery");

            arm_commit_failpoint_for_tests(site, mode);
            assert_recovery_failpoint(mode);
            match site {
                CommitFailpoint::BeforeJournalTailFoldBatch => {
                    assert_failpoint_interruption_oracle(site, &case);
                }
                CommitFailpoint::AfterJournalTailFoldWatermarkPersist => {
                    assert_journal_tail_fold_interruption_oracle(site, &case);
                }
                _ => panic!("unexpected journal-tail fold failpoint"),
            }

            if site == CommitFailpoint::BeforeJournalTailFoldBatch {
                arm_commit_failpoint_for_tests(site, mode);
                assert_recovery_failpoint(mode);
                assert_failpoint_interruption_oracle(site, &case);
            }

            ensure_recovered(&DB).expect("journal-tail fold retry should converge");
            assert_eq!(recovery_store_snapshot(), case.post_snapshot);
            assert_eq!(recovery_journal_tail_batch_count(), 0);
            assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
            assert!(
                !commit_marker_present().expect("commit marker check should succeed"),
                "successful fold retry should clear marker authority",
            );

            ensure_recovered(&DB).expect("second guarded recovery should be a no-op");
            assert_eq!(recovery_store_snapshot(), case.post_snapshot);
            assert_eq!(recovery_journal_tail_batch_count(), 0);
            assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
        }
    }
}

#[test]
fn recovery_secondary_index_rebuild_clear_failpoint_is_retryable_for_error_and_unwind() {
    for mode in [
        CommitFailpointMode::ReturnError,
        CommitFailpointMode::PanicUnwind,
    ] {
        let case = model_recovery_failpoint_case(5);
        assert!(
            !case.pre_snapshot.1.is_empty(),
            "secondary-index rebuild fixture should begin with non-empty pre-state indexes",
        );
        assert!(
            !case.post_snapshot.1.is_empty(),
            "secondary-index rebuild fixture should expect non-empty rebuilt indexes",
        );
        begin_commit(case.marker.clone()).expect("model marker should persist before recovery");

        arm_commit_failpoint_for_tests(CommitFailpoint::AfterSecondaryIndexRebuildClear, mode);
        assert_recovery_failpoint(mode);
        assert_secondary_index_rebuild_clear_interruption_oracle(&case, mode);

        ensure_recovered(&DB).expect("secondary-index rebuild retry should converge");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_journal_tail_batch_count(), 0);
        assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
        assert_eq!(recovery_index_state(), IndexState::Ready);
        assert!(
            !commit_marker_present().expect("commit marker check should succeed"),
            "successful rebuild retry should clear marker authority",
        );

        ensure_recovered(&DB).expect("second guarded recovery should be a no-op");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_index_state(), IndexState::Ready);
    }
}

#[test]
fn recovery_journaled_index_fold_failpoint_is_retryable_for_error_and_unwind() {
    for mode in [
        CommitFailpointMode::ReturnError,
        CommitFailpointMode::PanicUnwind,
    ] {
        let case = model_recovery_failpoint_case(5);
        begin_commit(case.marker.clone()).expect("model marker should persist before recovery");

        arm_commit_failpoint_for_tests(
            CommitFailpoint::AfterJournaledIndexMaterializedViewFold,
            mode,
        );
        assert_recovery_failpoint(mode);
        assert_journaled_index_fold_interruption_oracle(&case);

        ensure_recovered(&DB).expect("journaled index fold retry should converge");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_journal_tail_batch_count(), 0);
        assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
        assert_eq!(recovery_index_state(), IndexState::Ready);
        assert!(
            !commit_marker_present().expect("commit marker check should succeed"),
            "successful journaled index fold retry should clear marker authority",
        );

        ensure_recovered(&DB).expect("second guarded recovery should be a no-op");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_index_state(), IndexState::Ready);
    }
}

#[test]
fn recovery_repeated_interruption_across_fold_and_index_phases_converges() {
    for mode in [
        CommitFailpointMode::ReturnError,
        CommitFailpointMode::PanicUnwind,
    ] {
        let case = model_recovery_failpoint_case(5);
        begin_commit(case.marker.clone()).expect("model marker should persist before recovery");

        arm_commit_failpoint_for_tests(CommitFailpoint::AfterJournalTailFoldWatermarkPersist, mode);
        assert_recovery_failpoint(mode);
        assert_journal_tail_fold_interruption_oracle(
            CommitFailpoint::AfterJournalTailFoldWatermarkPersist,
            &case,
        );

        arm_commit_failpoint_for_tests(
            CommitFailpoint::AfterJournaledIndexMaterializedViewFold,
            mode,
        );
        assert_recovery_failpoint(mode);
        assert_journaled_index_fold_interruption_oracle(&case);

        ensure_recovered(&DB).expect("repeated interrupted recovery should converge");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_journal_tail_batch_count(), 0);
        assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
        assert_eq!(recovery_index_state(), IndexState::Ready);
        assert!(
            !commit_marker_present().expect("commit marker check should succeed"),
            "repeated interrupted recovery should clear marker authority",
        );

        ensure_recovered(&DB).expect("post-convergence guarded recovery should be a no-op");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_index_state(), IndexState::Ready);
    }
}

#[test]
fn recovery_repeated_interruption_after_marker_clear_converges() {
    for mode in [
        CommitFailpointMode::ReturnError,
        CommitFailpointMode::PanicUnwind,
    ] {
        let case = model_recovery_failpoint_case(5);
        begin_commit(case.marker.clone()).expect("model marker should persist before recovery");

        arm_commit_failpoint_for_tests(CommitFailpoint::AfterMarkerClear, mode);
        assert_recovery_failpoint(mode);
        assert_failpoint_interruption_oracle(CommitFailpoint::AfterMarkerClear, &case);
        assert_eq!(
            recovery_index_state(),
            IndexState::Building,
            "marker-clear interruption should leave indexes non-ready until guarded retry",
        );

        arm_commit_failpoint_for_tests(
            CommitFailpoint::AfterJournaledIndexMaterializedViewFold,
            mode,
        );
        assert_recovery_failpoint(mode);
        assert_markerless_journaled_index_fold_reentry_oracle(&case);

        ensure_recovered(&DB).expect("marker-cleared repeated recovery should converge");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_journal_tail_batch_count(), 0);
        assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
        assert_eq!(recovery_index_state(), IndexState::Ready);
        assert!(
            !commit_marker_present().expect("commit marker check should succeed"),
            "marker-cleared repeated recovery should leave no marker",
        );
    }
}

#[test]
fn recovery_upgrade_reentry_after_marker_clear_restores_readiness() {
    let case = model_recovery_failpoint_case(5);
    begin_commit(case.marker.clone()).expect("model marker should persist before recovery");

    arm_commit_failpoint_for_tests(
        CommitFailpoint::AfterMarkerClear,
        CommitFailpointMode::PanicUnwind,
    );
    assert_recovery_failpoint(CommitFailpointMode::PanicUnwind);
    assert_failpoint_interruption_oracle(CommitFailpoint::AfterMarkerClear, &case);
    assert_eq!(recovery_store_snapshot(), case.post_snapshot);
    assert_eq!(recovery_journal_tail_batch_count(), 0);
    assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
    assert_eq!(recovery_index_state(), IndexState::Building);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "marker-clear interruption should leave no marker for upgrade reentry",
    );

    clear_recovery_runtime_state_for_tests(&DB)
        .expect("test should clear volatile recovery runtime state");

    ensure_recovered(&DB).expect("upgrade-style markerless recovery should restore readiness");
    assert_eq!(recovery_store_snapshot(), case.post_snapshot);
    assert_eq!(recovery_journal_tail_batch_count(), 0);
    assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
    assert_eq!(recovery_index_state(), IndexState::Ready);
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "upgrade-style markerless recovery should leave no marker",
    );
}

#[test]
fn commit_failpoint_modes_have_explicit_failure_classification() {
    assert_eq!(
        CommitFailpointMode::ReturnError.failure_class(),
        CommitFailpointFailureClass::StructuredReturnedError,
    );
    assert_eq!(
        CommitFailpointMode::PanicUnwind.failure_class(),
        CommitFailpointFailureClass::HostUnwindInterruption,
    );
}

#[test]
fn commit_failpoint_sites_have_explicit_recovery_authority() {
    assert_eq!(
        CommitFailpoint::BeforeMarkerWrite.recovery_authority(),
        CommitFailpointRecoveryAuthority::NoCommitAuthority,
    );
    assert_eq!(
        CommitFailpoint::AfterMarkerWrite.recovery_authority(),
        CommitFailpointRecoveryAuthority::MarkerPayload,
    );
    assert_eq!(
        CommitFailpoint::BeforeMarkerBoundJournalAppend.recovery_authority(),
        CommitFailpointRecoveryAuthority::MarkerPayload,
    );
    assert_eq!(
        CommitFailpoint::AfterMarkerBoundJournalAppend.recovery_authority(),
        CommitFailpointRecoveryAuthority::MarkerPayloadAndJournalPrefix,
    );
    assert_eq!(
        CommitFailpoint::BeforeJournalTailFoldBatch.recovery_authority(),
        CommitFailpointRecoveryAuthority::JournalTailFoldReady,
    );
    assert_eq!(
        CommitFailpoint::AfterJournalTailFoldWatermarkPersist.recovery_authority(),
        CommitFailpointRecoveryAuthority::FoldWatermarkPersisted,
    );
    assert_eq!(
        CommitFailpoint::AfterSecondaryIndexRebuildClear.recovery_authority(),
        CommitFailpointRecoveryAuthority::SecondaryIndexRebuildCleared,
    );
    assert_eq!(
        CommitFailpoint::AfterJournaledIndexMaterializedViewFold.recovery_authority(),
        CommitFailpointRecoveryAuthority::JournaledIndexMaterializedViewFolded,
    );
    assert_eq!(
        CommitFailpoint::BeforeMarkerClear.recovery_authority(),
        CommitFailpointRecoveryAuthority::RecoveredStateWithMarker,
    );
    assert_eq!(
        CommitFailpoint::AfterMarkerClear.recovery_authority(),
        CommitFailpointRecoveryAuthority::RecoveredStateWithoutMarker,
    );
}

#[test]
fn commit_failpoint_sites_have_explicit_recovery_oracles() {
    let cases = [
        (
            CommitFailpoint::BeforeMarkerWrite,
            CommitFailpointSnapshotOracle::PreCommit,
            false,
            0,
        ),
        (
            CommitFailpoint::AfterMarkerWrite,
            CommitFailpointSnapshotOracle::PreCommit,
            true,
            0,
        ),
        (
            CommitFailpoint::BeforeMarkerBoundJournalAppend,
            CommitFailpointSnapshotOracle::PreCommit,
            true,
            0,
        ),
        (
            CommitFailpoint::AfterMarkerBoundJournalAppend,
            CommitFailpointSnapshotOracle::PreCommit,
            true,
            1,
        ),
        (
            CommitFailpoint::BeforeJournalTailFoldBatch,
            CommitFailpointSnapshotOracle::PreCommit,
            true,
            1,
        ),
        (
            CommitFailpoint::AfterJournalTailFoldWatermarkPersist,
            CommitFailpointSnapshotOracle::RecoveryIntermediate,
            true,
            1,
        ),
        (
            CommitFailpoint::AfterSecondaryIndexRebuildClear,
            CommitFailpointSnapshotOracle::RecoveryIntermediate,
            true,
            0,
        ),
        (
            CommitFailpoint::AfterJournaledIndexMaterializedViewFold,
            CommitFailpointSnapshotOracle::RecoveryIntermediate,
            true,
            0,
        ),
        (
            CommitFailpoint::BeforeMarkerClear,
            CommitFailpointSnapshotOracle::MarkerAuthorizedPostCommit,
            true,
            0,
        ),
        (
            CommitFailpoint::AfterMarkerClear,
            CommitFailpointSnapshotOracle::MarkerAuthorizedPostCommit,
            false,
            0,
        ),
    ];

    for (site, snapshot, marker_present, journal_tail_batches) in cases {
        let oracle = site.recovery_oracle();
        assert_eq!(oracle.snapshot(), snapshot);
        assert_eq!(oracle.marker_present(), marker_present);
        assert_eq!(oracle.journal_tail_batches(), journal_tail_batches);
    }
}

#[test]
fn recovery_domain_key_allows_peer_marker_after_primary_noop_recovery() {
    reset_recovery_state();
    reset_peer_recovery_state();

    ensure_recovered(&DB).expect("primary no-op recovery should succeed");

    let peer = RecoveryPeerEntity {
        id: Ulid::from_u128(17_130),
        group: 41,
    };
    let expected_row = (
        peer_data_key(peer.id).as_bytes().to_vec(),
        peer_row_bytes(&peer),
    );

    init_peer_commit_store_for_tests().expect("peer commit store should configure");
    begin_commit(peer_insert_marker(&peer)).expect("peer marker should persist");
    ensure_recovered(&DB).expect("primary follow-up recovery should not consume peer marker");
    assert!(
        !peer_recovery_store_snapshot().0.contains(&expected_row),
        "peer row must not be applied by the primary recovery domain",
    );

    ensure_recovered(&PEER_DB).expect("peer marker should recover after primary domain recovered");
    assert!(
        peer_recovery_store_snapshot().0.contains(&expected_row),
        "peer marker replay must apply the peer row",
    );
    assert!(
        !commit_marker_present().expect("peer marker presence check should succeed"),
        "peer recovery must clear the peer commit marker",
    );
}

#[test]
fn recovery_domain_key_keeps_primary_marker_after_peer_noop_recovery() {
    reset_recovery_state();
    reset_peer_recovery_state();

    let primary = RecoveryIndexedEntity {
        id: Ulid::from_u128(17_131),
        group: 42,
    };
    let expected_row = (
        indexed_data_key(primary.id).as_bytes().to_vec(),
        indexed_row_bytes(&primary),
    );
    let marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryIndexedEntity::PATH,
        indexed_data_key(primary.id).as_bytes().to_vec(),
        None,
        Some(indexed_row_bytes(&primary)),
    )])
    .expect("primary marker should build");

    init_commit_store_for_tests().expect("primary commit store should configure");
    begin_commit(marker).expect("primary marker should persist");
    ensure_recovered(&PEER_DB).expect("peer no-op recovery should not consume primary marker");

    init_commit_store_for_tests().expect("primary commit store should reconfigure");
    assert!(
        commit_marker_present().expect("primary marker presence check should succeed"),
        "primary marker must remain present after peer recovery",
    );

    ensure_recovered(&DB).expect("primary marker should recover after peer domain recovered");
    assert!(
        recovery_store_snapshot().0.contains(&expected_row),
        "primary marker replay must apply the primary row",
    );
    assert!(
        !commit_marker_present().expect("primary marker presence check should succeed"),
        "primary recovery must clear the primary commit marker",
    );
}

#[test]
fn failpoint_before_marker_write_preserves_pre_state_for_error_and_unwind() {
    for mode in [
        CommitFailpointMode::ReturnError,
        CommitFailpointMode::PanicUnwind,
    ] {
        let case = mixed_recovery_marker_failpoint_case();

        arm_commit_failpoint_for_tests(CommitFailpoint::BeforeMarkerWrite, mode);
        assert_begin_commit_failpoint(&case.marker, mode);
        assert_failpoint_interruption_oracle(CommitFailpoint::BeforeMarkerWrite, &case);
    }
}

#[test]
fn failpoint_after_marker_write_recovers_marker_authorized_state_for_error_and_unwind() {
    for mode in [
        CommitFailpointMode::ReturnError,
        CommitFailpointMode::PanicUnwind,
    ] {
        let case = mixed_recovery_marker_failpoint_case();

        arm_commit_failpoint_for_tests(CommitFailpoint::AfterMarkerWrite, mode);
        assert_begin_commit_failpoint(&case.marker, mode);
        assert_failpoint_interruption_oracle(CommitFailpoint::AfterMarkerWrite, &case);

        ensure_recovered(&DB).expect("recovery should replay the persisted marker");
        assert_eq!(recovery_store_snapshot(), case.post_snapshot);
        assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
        assert!(
            !commit_marker_present().expect("commit marker check should succeed"),
            "successful recovery must clear the marker",
        );
    }
}

#[test]
fn failpoint_marker_bound_journal_append_is_retryable_for_error_and_unwind() {
    for site in [
        CommitFailpoint::BeforeMarkerBoundJournalAppend,
        CommitFailpoint::AfterMarkerBoundJournalAppend,
    ] {
        for mode in [
            CommitFailpointMode::ReturnError,
            CommitFailpointMode::PanicUnwind,
        ] {
            let case = mixed_recovery_marker_failpoint_case();
            begin_commit(case.marker.clone())
                .expect("marker should persist before recovery failpoint");

            arm_commit_failpoint_for_tests(site, mode);
            assert_recovery_failpoint(mode);
            assert_failpoint_interruption_oracle(site, &case);

            ensure_recovered(&DB).expect("journal append retry should recover");
            assert_eq!(recovery_store_snapshot(), case.post_snapshot);
            assert_eq!(recovery_journal_tail_batch_count(), 0);
            assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
            assert!(
                !commit_marker_present().expect("commit marker check should succeed"),
                "successful retry must clear the marker",
            );
        }
    }
}

#[test]
fn failpoint_marker_clear_preserves_recovered_state_for_error_and_unwind() {
    for site in [
        CommitFailpoint::BeforeMarkerClear,
        CommitFailpoint::AfterMarkerClear,
    ] {
        for mode in [
            CommitFailpointMode::ReturnError,
            CommitFailpointMode::PanicUnwind,
        ] {
            let case = mixed_recovery_marker_failpoint_case();
            begin_commit(case.marker.clone())
                .expect("marker should persist before recovery failpoint");

            arm_commit_failpoint_for_tests(site, mode);
            assert_recovery_failpoint(mode);
            assert_failpoint_interruption_oracle(site, &case);
            assert_eq!(
                recovery_index_state(),
                IndexState::Building,
                "marker-clear interruption should leave indexes non-ready until guarded retry",
            );

            ensure_recovered(&DB).expect("marker-clear retry should finish recovery");
            assert_eq!(recovery_store_snapshot(), case.post_snapshot);
            assert_eq!(recovery_journal_tail_batch_count(), 0);
            assert_eq!(recovery_journal_fold_watermark(), case.post_watermark);
            assert_eq!(recovery_index_state(), IndexState::Ready);
            assert!(
                !commit_marker_present().expect("commit marker check should succeed"),
                "successful retry must leave no marker",
            );
        }
    }
}

#[test]
fn conditional_index_forward_apply_and_replay_preserve_identical_store_state_for_membership_matrix()
{
    let seed_ops = conditional_recovery_seed_ops();
    let marker_ops = conditional_recovery_marker_ops();

    // Phase 1: apply the full conditional-membership transition matrix through live apply.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("conditional seed state apply should succeed for matrix fixture");
    apply_row_ops_forward(marker_ops.as_slice())
        .expect("forward apply should succeed for conditional membership transition matrix");
    let forward_snapshot = recovery_store_snapshot();

    // Phase 2: replay the same marker from the same seeded snapshot and compare outcomes.
    reset_recovery_state();
    apply_row_ops_forward(seed_ops.as_slice())
        .expect("conditional seed state apply should succeed before replay marker");
    let marker = CommitMarker::new(marker_ops)
        .expect("conditional membership transition marker should build for replay");
    begin_commit(marker).expect("conditional replay marker begin_commit should persist marker");
    ensure_recovered(&DB).expect("conditional marker replay should recover successfully");
    let replay_snapshot = recovery_store_snapshot();

    assert_eq!(
        replay_snapshot, forward_snapshot,
        "conditional-index forward apply and replay must converge on identical store state"
    );
    assert!(
        !commit_marker_present().expect("commit marker presence check should succeed"),
        "successful conditional replay must clear the persisted marker"
    );

    // Phase 3: lock the final membership shape for representative transition outcomes.
    let activated = RecoveryConditionalEntity {
        id: Ulid::from_u128(9401),
        group: 31,
        active: true,
    };
    let deactivated = RecoveryConditionalEntity {
        id: Ulid::from_u128(9402),
        group: 32,
        active: false,
    };
    let moved_old_key = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 33,
        active: true,
    };
    let moved_new_key = RecoveryConditionalEntity {
        id: Ulid::from_u128(9403),
        group: 34,
        active: true,
    };
    let inserted_active = RecoveryConditionalEntity {
        id: Ulid::from_u128(9407),
        group: 38,
        active: true,
    };
    assert_eq!(
        conditional_indexed_ids_for(&activated),
        Some(BTreeSet::from([activated.id])),
        "false->true transitions must create conditional index membership",
    );
    assert!(
        conditional_indexed_ids_for(&deactivated).is_none(),
        "true->false transitions must remove conditional index membership",
    );
    assert!(
        conditional_indexed_ids_for(&moved_old_key).is_none(),
        "true->true key-move transitions must remove old conditional index key membership",
    );
    assert_eq!(
        conditional_indexed_ids_for(&moved_new_key),
        Some(BTreeSet::from([moved_new_key.id])),
        "true->true key-move transitions must create new conditional index key membership",
    );
    assert_eq!(
        conditional_indexed_ids_for(&inserted_active),
        Some(BTreeSet::from([inserted_active.id])),
        "none->true inserts must publish conditional index membership",
    );
}

#[test]
fn index_key_new_rejects_missing_index_field_on_entity_model() {
    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(9901),
        group: 7,
    };

    let err = IndexKey::new(&entity, &RECOVERY_INDEXED_MISSING_FIELD_INDEX_MODEL)
        .expect_err("index fields missing from the entity model must fail as invariants");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn conditional_index_mutation_tracks_false_true_false_membership_transitions() {
    reset_recovery_state();

    let inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(9_931),
        group: 7,
        active: false,
    };
    let active = RecoveryConditionalEntity {
        id: inactive.id,
        group: inactive.group,
        active: true,
    };
    let inactive_again = RecoveryConditionalEntity {
        id: inactive.id,
        group: inactive.group,
        active: false,
    };
    let key = conditional_data_key(inactive.id);

    // Phase 1: inactive insert must not create a conditional index entry.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalEntity::PATH,
        key.as_bytes().to_vec(),
        None,
        Some(conditional_row_bytes(&inactive)),
    )])
    .expect("inactive conditional row insert should succeed");
    assert!(
        conditional_indexed_ids_for(&inactive).is_none(),
        "inactive conditional rows must stay absent from the index",
    );

    // Phase 2: false -> true transition must insert one entry.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalEntity::PATH,
        key.as_bytes().to_vec(),
        Some(conditional_row_bytes(&inactive)),
        Some(conditional_row_bytes(&active)),
    )])
    .expect("conditional false->true transition should succeed");
    assert_eq!(
        conditional_indexed_ids_for(&active),
        Some(BTreeSet::from([active.id])),
        "active conditional rows must be present in the index",
    );

    // Phase 3: true -> false transition must remove that entry.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalEntity::PATH,
        key.as_bytes().to_vec(),
        Some(conditional_row_bytes(&active)),
        Some(conditional_row_bytes(&inactive_again)),
    )])
    .expect("conditional true->false transition should succeed");
    assert!(
        conditional_indexed_ids_for(&inactive_again).is_none(),
        "inactive conditional rows must be removed from the index",
    );
}

#[test]
fn conditional_unique_index_skips_unique_validation_when_predicate_is_false() {
    reset_recovery_state();

    let first_active = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9_941),
        email: "conditional-unique@example.com".to_string(),
        active: true,
    };
    let second_inactive = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9_942),
        email: first_active.email.clone(),
        active: false,
    };
    let second_active = RecoveryConditionalUniqueEntity {
        id: second_inactive.id,
        email: second_inactive.email.clone(),
        active: true,
    };

    // Phase 1: baseline active row reserves the unique conditional slot.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        conditional_unique_data_key(first_active.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_row_bytes(&first_active)),
    )])
    .expect("active conditional-unique insert should succeed");

    // Phase 2: duplicate email with inactive predicate must bypass unique checks.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        conditional_unique_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_row_bytes(&second_inactive)),
    )])
    .expect("inactive duplicate should bypass conditional unique validation");

    // Phase 3: activating the duplicate row must enforce unique ownership.
    let err = apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        conditional_unique_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        Some(conditional_unique_row_bytes(&second_inactive)),
        Some(conditional_unique_row_bytes(&second_active)),
    )])
    .expect_err("active duplicate should violate conditional unique index");
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn conditional_unique_expression_index_skips_unique_validation_when_predicate_is_false() {
    reset_recovery_state();

    let first_active = RecoveryConditionalUniqueCasefoldEntity {
        id: Ulid::from_u128(9_946),
        email: "Conditional-CaseFold@example.com".to_string(),
        active: true,
    };
    let second_inactive = RecoveryConditionalUniqueCasefoldEntity {
        id: Ulid::from_u128(9_947),
        email: "conditional-casefold@example.com".to_string(),
        active: false,
    };
    let second_active = RecoveryConditionalUniqueCasefoldEntity {
        id: second_inactive.id,
        email: second_inactive.email.clone(),
        active: true,
    };

    // Phase 1: baseline active row reserves the conditional+expression unique slot.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        conditional_unique_casefold_data_key(first_active.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_casefold_row_bytes(&first_active)),
    )])
    .expect("active conditional expression-unique insert should succeed");

    // Phase 2: inactive duplicate bypasses unique validation while predicate=false.
    apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        conditional_unique_casefold_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        None,
        Some(conditional_unique_casefold_row_bytes(&second_inactive)),
    )])
    .expect("inactive duplicate should bypass conditional expression-unique validation");

    // Phase 3: activating the duplicate row must enforce canonical LOWER(email) uniqueness.
    let err = apply_row_ops_forward(&[row_op_for_path(
        RecoveryConditionalUniqueCasefoldEntity::PATH,
        conditional_unique_casefold_data_key(second_inactive.id)
            .as_bytes()
            .to_vec(),
        Some(conditional_unique_casefold_row_bytes(&second_inactive)),
        Some(conditional_unique_casefold_row_bytes(&second_active)),
    )])
    .expect_err("active duplicate should violate conditional expression-unique index");
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Index);
}

#[test]
fn commit_marker_round_trip_clears_after_finish() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    let marker = CommitMarker::new(Vec::new()).expect("commit marker creation should succeed");

    let guard = begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present after begin_commit"
    );

    finish_commit(guard, |_| Ok(())).expect("finish_commit should clear marker");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after finish_commit"
    );
}

#[test]
fn finish_commit_error_keeps_marker_for_recovery() {
    init_commit_store_for_tests().expect("commit store init should succeed");
    let marker = CommitMarker::new(Vec::new()).expect("commit marker creation should succeed");

    let guard = begin_commit(marker).expect("begin_commit should persist marker");
    let err = finish_commit(guard, |_| Err(InternalError::executor_invariant()))
        .expect_err("failed finish_commit should surface apply error");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed finish_commit should keep marker persisted for recovery replay"
    );

    // Cleanup so unrelated tests do not observe this intentionally-persisted marker.
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
    with_recovery_store(|store| {
        store.with_data_mut(DataStore::clear);
        store.with_index_mut(IndexStore::clear);
    });
}

#[test]
fn finish_commit_mixed_state_failure_rolls_back_index_prefix_without_row_visibility() {
    reset_recovery_state();

    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(915),
        group: 19,
    };
    let data_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = canonical_row_bytes(&entity);
    let row_op = row_op_for_path(
        RecoveryIndexedEntity::PATH,
        data_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
    );
    let marker =
        CommitMarker::new(vec![row_op.clone()]).expect("commit marker creation should succeed");
    let guard = begin_commit(marker).expect("begin_commit should persist marker");

    // Simulate a mixed-state apply edge:
    // - apply index mutations
    // - fail before row write
    // - rollback must remove the applied index mutation
    let err = finish_commit(guard, |_| {
        let context = DB.context::<RecoveryIndexedEntity>();
        let prepared = prepare_row_commit_for_entity_with_structural_readers::<
            RecoveryIndexedEntity,
        >(&DB, &row_op, &context, &context)?;
        let rollback = prepared.snapshot_rollback();
        for index_op in prepared.index_ops {
            index_op.index_store.with_borrow_mut(|store| {
                if let Some(value) = index_op.value {
                    store.insert(index_op.key, value);
                } else {
                    store.remove(&index_op.key);
                }
            });
        }
        rollback_prepared_row_ops_reverse(vec![rollback]);

        Err(InternalError::executor_invariant())
    })
    .expect_err("mixed-state finish_commit path should surface apply error");
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Executor);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed mixed-state apply must keep marker persisted for recovery replay"
    );
    assert_eq!(
        row_bytes_for(&data_key),
        None,
        "mixed-state apply failure must not leave row bytes visible"
    );
    assert!(
        indexed_ids_for(&entity).is_none(),
        "mixed-state apply failure must not leave index membership visible"
    );

    // Cleanup so unrelated tests do not observe this intentionally-persisted marker.
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_replay_is_idempotent() {
    reset_recovery_state();

    let entity = RecoveryTestEntity {
        id: Ulid::from_u128(901),
    };
    let raw_key = DecodedDataStoreKey::try_new::<RecoveryTestEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = canonical_row_bytes(&entity);
    let marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryTestEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes.clone()),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay"
    );

    // First replay applies marker operations and clears the marker.
    ensure_recovered(&DB).expect("first recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after first replay"
    );
    let first = row_bytes_for(&raw_key);
    assert_eq!(first, Some(row_bytes));

    // Second replay is a no-op on already recovered state.
    ensure_recovered(&DB).expect("second recovery replay should be a no-op");
    let second = row_bytes_for(&raw_key);
    assert_eq!(second, first);
}

#[test]
fn recovery_rejects_corrupt_marker_data_key_decode() {
    reset_recovery_state();

    let row_bytes = canonical_row_bytes(&RecoveryTestEntity {
        id: Ulid::from_u128(902),
    });
    let malformed_key = vec![0u8; RawDataStoreKey::MAX_STORED_SIZE_USIZE.saturating_sub(1)];
    let mut marker_payload = Vec::new();
    marker_payload.extend_from_slice(&[0u8; 16]);
    marker_payload.extend_from_slice(&1u32.to_le_bytes());
    marker_payload.extend_from_slice(
        &u32::try_from(RecoveryTestEntity::PATH.len())
            .expect("entity path length should fit u32")
            .to_le_bytes(),
    );
    marker_payload.extend_from_slice(RecoveryTestEntity::PATH.as_bytes());
    marker_payload.extend_from_slice(
        &u32::try_from(malformed_key.len())
            .expect("data key length should fit u32")
            .to_le_bytes(),
    );
    marker_payload.extend_from_slice(&malformed_key);
    marker_payload.push(0b0000_0010);
    marker_payload.extend_from_slice(
        &u32::try_from(row_bytes.len())
            .expect("after payload length should fit u32")
            .to_le_bytes(),
    );
    marker_payload.extend_from_slice(&row_bytes);
    marker_payload.extend_from_slice(&initial_accepted_commit_schema_fingerprint_for_entity::<
        RecoveryTestEntity,
    >());
    marker_payload.extend_from_slice(&0u32.to_le_bytes());

    let marker_bytes = store::CommitStore::encode_raw_marker_envelope_for_tests(
        COMMIT_MARKER_FORMAT_VERSION_CURRENT,
        marker_payload,
    )
    .expect("raw marker envelope encode should succeed");
    let control_slot_bytes = store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes)
        .expect("raw control-slot encode should succeed");
    store::with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot_bytes);
        Ok(())
    })
    .expect("corrupt test marker should persist raw bytes");

    let err = ensure_recovered(&DB).expect_err("recovery should reject corrupt marker bytes");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    let marker_still_present = store::with_commit_store(|store| Ok(!store.is_empty()))
        .expect("raw commit marker presence check should succeed");
    assert!(
        marker_still_present,
        "marker should remain present when recovery prevalidation fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-corrupt marker.
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_rejects_incompatible_marker_format_version_fail_closed() {
    reset_recovery_state();

    let marker = CommitMarker {
        id: [0xAB; 16],
        row_ops: Vec::new(),
        journal_batches: Vec::new(),
    };
    let marker_payload =
        encode_commit_marker_payload(&marker).expect("marker payload encode should succeed");
    let future_version = COMMIT_MARKER_FORMAT_VERSION_CURRENT.saturating_add(1);
    let marker_bytes =
        store::CommitStore::encode_raw_marker_envelope_for_tests(future_version, marker_payload)
            .expect("future-version marker envelope encode should succeed");
    let control_slot_bytes = store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes)
        .expect("control-slot envelope encode should succeed");
    store::with_commit_store(|store| {
        store.set_raw_marker_bytes_for_tests(control_slot_bytes);
        Ok(())
    })
    .expect("test helper should persist raw marker bytes");

    let err =
        ensure_recovered(&DB).expect_err("recovery should reject incompatible marker versions");
    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    let marker_still_present = store::with_commit_store(|store| Ok(!store.is_empty()))
        .expect("raw commit marker presence check should succeed");
    assert!(
        marker_still_present,
        "marker should remain present when recovery decode fails compatibility checks"
    );

    // Cleanup so unrelated tests do not observe this intentionally-incompatible marker.
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn single_row_control_slot_direct_encoder_matches_canonical_two_stage_encoding() {
    let marker_id = [0x5A; 16];
    let raw_key = DecodedDataStoreKey::try_new::<RecoveryPayloadEntity>(Ulid::from_u128(111))
        .expect("single-row encoder test data key should build")
        .to_raw()
        .expect("single-row encoder test data key should encode");
    let row_op = row_op_for_path(
        RecoveryPayloadEntity::PATH,
        raw_key.as_bytes().to_vec(),
        Some(canonical_row_bytes(&RecoveryPayloadEntity {
            id: Ulid::from_u128(111),
            name: "before".to_string(),
        })),
        Some(canonical_row_bytes(&RecoveryPayloadEntity {
            id: Ulid::from_u128(111),
            name: "after".to_string(),
        })),
    );
    let marker_payload = encode_single_row_commit_marker_payload(marker_id, &row_op)
        .expect("single-row marker payload encode should succeed");
    let marker_bytes = store::CommitStore::encode_raw_marker_envelope_for_tests(
        COMMIT_MARKER_FORMAT_VERSION_CURRENT,
        marker_payload,
    )
    .expect("single-row marker envelope encode should succeed");
    let canonical = store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes)
        .expect("canonical control-slot encode should succeed");
    let direct =
        store::CommitStore::encode_raw_single_row_control_slot_for_tests(marker_id, &row_op)
            .expect("direct single-row control-slot encode should succeed");

    assert_eq!(
        direct, canonical,
        "single-row direct control-slot encoding must stay byte-for-byte canonical"
    );
}

#[test]
fn multi_row_control_slot_direct_encoder_matches_canonical_two_stage_encoding() {
    let marker = CommitMarker {
        id: [0x6B; 16],
        row_ops: vec![
            row_op_for_path(
                RecoveryPayloadEntity::PATH,
                DecodedDataStoreKey::try_new::<RecoveryPayloadEntity>(Ulid::from_u128(211))
                    .expect("multi-row encoder first key should build")
                    .to_raw()
                    .expect("multi-row encoder first key should encode")
                    .as_bytes()
                    .to_vec(),
                Some(canonical_row_bytes(&RecoveryPayloadEntity {
                    id: Ulid::from_u128(211),
                    name: "before-a".to_string(),
                })),
                Some(canonical_row_bytes(&RecoveryPayloadEntity {
                    id: Ulid::from_u128(211),
                    name: "after-a".to_string(),
                })),
            ),
            row_op_for_path(
                RecoveryPayloadEntity::PATH,
                DecodedDataStoreKey::try_new::<RecoveryPayloadEntity>(Ulid::from_u128(212))
                    .expect("multi-row encoder second key should build")
                    .to_raw()
                    .expect("multi-row encoder second key should encode")
                    .as_bytes()
                    .to_vec(),
                None,
                Some(canonical_row_bytes(&RecoveryPayloadEntity {
                    id: Ulid::from_u128(212),
                    name: "after-b".to_string(),
                })),
            ),
        ],
        journal_batches: Vec::new(),
    };
    let marker_payload = encode_commit_marker_payload(&marker)
        .expect("multi-row marker payload encode should succeed");
    let marker_bytes = store::CommitStore::encode_raw_marker_envelope_for_tests(
        COMMIT_MARKER_FORMAT_VERSION_CURRENT,
        marker_payload,
    )
    .expect("multi-row marker envelope encode should succeed");
    let canonical = store::CommitStore::encode_raw_control_slot_for_tests(marker_bytes)
        .expect("canonical multi-row control-slot encode should succeed");
    let direct = store::CommitStore::encode_raw_direct_control_slot_for_tests(&marker)
        .expect("direct multi-row control-slot encode should succeed");

    assert_eq!(
        direct, canonical,
        "multi-row direct control-slot encoding must stay byte-for-byte canonical"
    );
}

#[test]
fn recovery_replay_rolls_back_applied_prefix_when_later_marker_op_fails_prepare() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(913),
        group: 17,
    };
    let first_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let first_row = canonical_row_bytes(&first);

    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(914),
        group: 18,
    };
    let second_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let second_row = canonical_row_bytes(&second);
    let unsupported_path = "commit_tests::UnknownEntity";
    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
        ),
        row_op_for_path(
            unsupported_path,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row),
        ),
    ])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered(&DB).expect_err(
        "recovery should fail when a later marker op has an unsupported entity path during replay",
    );
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay should keep marker persisted for later recovery attempts"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        None,
        "recovery must roll back the already-applied prefix row when a later marker op fails"
    );
    assert!(
        indexed_ids_for(&first).is_none(),
        "recovery must roll back the already-applied prefix index mutation when a later marker op fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-corrupt marker.
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_rejects_unsupported_entity_path_without_fallback() {
    reset_recovery_state();

    let raw_key = DecodedDataStoreKey::try_new::<RecoveryTestEntity>(Ulid::from_u128(911))
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row_bytes = canonical_row_bytes(&RecoveryTestEntity {
        id: Ulid::from_u128(911),
    });
    let unsupported_path = "commit_tests::UnknownEntity";
    let marker = CommitMarker::new(vec![row_op_for_path(
        unsupported_path,
        raw_key.as_bytes().to_vec(),
        None,
        Some(row_bytes),
    )])
    .expect("commit marker creation should succeed");

    begin_commit(marker).expect("begin_commit should persist marker");

    let err =
        ensure_recovered(&DB).expect_err("recovery should reject unsupported entity path markers");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery dispatch fails"
    );
    assert_eq!(
        row_bytes_for(&raw_key),
        None,
        "recovery must not partially apply rows when dispatch fails"
    );

    // Cleanup so unrelated tests do not observe this intentionally-unsupported marker.
    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn runtime_hook_lookup_rejects_duplicate_entity_tags() {
    #[cfg(debug_assertions)]
    {
        let Err(_) = std::panic::catch_unwind(duplicate_name_db) else {
            panic!("duplicate entity tags must fail during hook table construction");
        };
    }

    #[cfg(not(debug_assertions))]
    {
        let duplicate_name_db = duplicate_name_db();
        let Err(err) =
            duplicate_name_db.runtime_hook_for_entity_tag(RecoveryTestEntity::ENTITY_TAG)
        else {
            panic!("duplicate entity tags must fail runtime-hook lookup")
        };
        assert_eq!(err.class, ErrorClass::InvariantViolation);
        assert_eq!(err.origin, ErrorOrigin::Store);
    }
}

#[test]
fn prepare_row_commit_rejects_duplicate_entity_paths() {
    let raw_key = DecodedDataStoreKey::try_new::<RecoveryTestEntity>(Ulid::from_u128(9_991))
        .expect("duplicate-path test data key should build")
        .to_raw()
        .expect("duplicate-path test data key should encode");
    let op = row_op_for_path(
        RecoveryTestEntity::PATH,
        raw_key.as_bytes().to_vec(),
        None,
        None,
    );
    let Err(err) = DUPLICATE_PATH_DB.prepare_row_commit_op(&op) else {
        panic!("duplicate entity paths must fail prepare dispatch")
    };
    assert_eq!(err.class, ErrorClass::InvariantViolation);
    assert_eq!(err.origin, ErrorOrigin::Store);
}

#[test]
fn recovery_replay_rejects_schema_fingerprint_mismatch() {
    reset_recovery_state();

    let entity = RecoveryTestEntity {
        id: Ulid::from_u128(9801),
    };
    let key = DecodedDataStoreKey::try_new::<RecoveryTestEntity>(entity.id)
        .expect("data key should build")
        .to_raw()
        .expect("data key should encode");
    let row = canonical_row_bytes(&entity);

    let marker = CommitMarker::new(vec![row_op_for_path_with_schema(
        RecoveryTestEntity::PATH,
        key.as_bytes().to_vec(),
        None,
        Some(row),
        initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryIndexedEntity>(),
    )])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered(&DB)
        .expect_err("recovery should reject mismatched commit schema fingerprint");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when replay rejects schema fingerprint mismatch"
    );
    assert_eq!(
        row_bytes_for(&key),
        None,
        "row bytes must remain absent when replay fails before apply"
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_replay_merges_multi_row_shared_index_key() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(903),
        group: 7,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(904),
        group: 7,
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    ensure_recovered(&DB).expect("recovery replay should succeed");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after replay"
    );
    assert_eq!(row_bytes_for(&first_key), Some(first_row));
    assert_eq!(row_bytes_for(&second_key), Some(second_row));

    let indexed_ids_first = indexed_ids_for(&first).expect("first index entry should exist");
    let indexed_ids_second = indexed_ids_for(&second).expect("second index entry should exist");
    assert_eq!(
        indexed_ids_first,
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        indexed_ids_second,
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_replays_interrupted_atomic_batch_marker_and_is_idempotent() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(907),
        group: 9,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(908),
        group: 9,
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    // Simulate an interrupted atomic batch by persisting the marker without apply.
    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be present before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        None,
        "interrupted batch rows must not be visible before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&second_key),
        None,
        "interrupted batch rows must not be visible before recovery replay"
    );
    assert!(
        indexed_ids_for(&first).is_none(),
        "interrupted batch index state must not be visible before recovery replay"
    );

    // First replay applies marker row ops and clears the marker.
    ensure_recovered(&DB).expect("first recovery replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after first replay"
    );
    let first_after = row_bytes_for(&first_key);
    let second_after = row_bytes_for(&second_key);
    assert_eq!(first_after, Some(first_row));
    assert_eq!(second_after, Some(second_row));

    let indexed_after_first = indexed_ids_for(&first).expect("first index entry should exist");
    let indexed_after_second = indexed_ids_for(&second).expect("second index entry should exist");
    let expected_first = std::iter::once(first.id).collect::<BTreeSet<_>>();
    let expected_second = std::iter::once(second.id).collect::<BTreeSet<_>>();
    assert_eq!(indexed_after_first, expected_first);
    assert_eq!(indexed_after_second, expected_second);

    // Second replay is a no-op on already recovered state.
    ensure_recovered(&DB).expect("second recovery replay should be a no-op");
    assert_eq!(row_bytes_for(&first_key), first_after);
    assert_eq!(row_bytes_for(&second_key), second_after);
    let indexed_second_first =
        indexed_ids_for(&first).expect("first index entry should remain after idempotent replay");
    let indexed_second_second =
        indexed_ids_for(&second).expect("second index entry should remain after idempotent replay");
    assert_eq!(indexed_second_first, expected_first);
    assert_eq!(indexed_second_second, expected_second);
}

#[test]
fn recovery_replay_interrupted_conflicting_unique_batch_fails_closed() {
    reset_recovery_state();

    let first = RecoveryUniqueEntity {
        id: Ulid::from_u128(911),
        email: "dup@example.com".to_string(),
    };
    let second = RecoveryUniqueEntity {
        id: Ulid::from_u128(912),
        email: "dup@example.com".to_string(),
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryUniqueEntity>(first.id)
        .expect("first unique data key should build")
        .to_raw()
        .expect("first unique data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryUniqueEntity>(second.id)
        .expect("second unique data key should build")
        .to_raw()
        .expect("second unique data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    // Simulate interrupted atomic marker persistence for two writes that conflict
    // on one unique secondary index value.
    let marker = CommitMarker::new(vec![
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row),
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
    ])
    .expect("conflicting unique marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist conflicting unique marker");

    let err = ensure_recovered(&DB)
        .expect_err("recovery should fail closed on conflicting unique replay marker");
    assert_eq!(err.class, ErrorClass::Conflict);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed unique replay must keep marker persisted for retry",
    );
    assert!(
        index_key_bytes_snapshot().is_empty(),
        "failed rebuild must not leave partially rebuilt unique index state",
    );

    let retry_err = ensure_recovered(&DB)
        .expect_err("repeated recovery attempts should remain fail-closed until marker is fixed");
    assert_eq!(retry_err.class, ErrorClass::Conflict);
    assert_eq!(retry_err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "retry failure must keep marker persisted",
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn unique_conflict_classification_parity_holds_between_live_apply_and_replay() {
    reset_recovery_state();

    // Phase 1: capture live save-path unique conflict classification.
    let save = SaveExecutor::<RecoveryUniqueEntity>::new(DB, false);
    save.insert(RecoveryUniqueEntity {
        id: Ulid::from_u128(9211),
        email: "dup-live-replay@example.com".to_string(),
    })
    .expect("seed unique row should save in live path");
    let live_err = save
        .insert(RecoveryUniqueEntity {
            id: Ulid::from_u128(9212),
            email: "dup-live-replay@example.com".to_string(),
        })
        .expect_err("live save path should reject duplicate unique value");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);

    // Phase 2: capture replay-path unique conflict classification for the same semantic conflict.
    reset_recovery_state();
    let replay_first = RecoveryUniqueEntity {
        id: Ulid::from_u128(9221),
        email: "dup-live-replay@example.com".to_string(),
    };
    let replay_second = RecoveryUniqueEntity {
        id: Ulid::from_u128(9222),
        email: "dup-live-replay@example.com".to_string(),
    };

    let replay_first_key = DecodedDataStoreKey::try_new::<RecoveryUniqueEntity>(replay_first.id)
        .expect("first replay key should build")
        .to_raw()
        .expect("first replay key should encode");
    let replay_second_key = DecodedDataStoreKey::try_new::<RecoveryUniqueEntity>(replay_second.id)
        .expect("second replay key should build")
        .to_raw()
        .expect("second replay key should encode");

    let replay_first_row = canonical_row_bytes(&replay_first);
    let replay_second_row = canonical_row_bytes(&replay_second);

    let replay_marker = CommitMarker::new(vec![
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            replay_first_key.as_bytes().to_vec(),
            None,
            Some(replay_first_row),
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
        row_op_for_path_with_schema(
            RecoveryUniqueEntity::PATH,
            replay_second_key.as_bytes().to_vec(),
            None,
            Some(replay_second_row),
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueEntity>(),
        ),
    ])
    .expect("replay unique-conflict marker should build");
    begin_commit(replay_marker).expect("begin_commit should persist replay conflict marker");

    let replay_err = ensure_recovered(&DB)
        .expect_err("replay recovery should reject duplicate unique value marker");
    assert_eq!(replay_err.class, ErrorClass::Conflict);
    assert_eq!(replay_err.class, live_err.class);
    assert_eq!(replay_err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay unique conflict must keep marker persisted for retry",
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn unique_expression_index_enforces_casefolded_conflicts_on_live_saves() {
    reset_recovery_state();

    // Phase 1: seed one row with mixed-case email.
    let save = SaveExecutor::<RecoveryUniqueCasefoldEntity>::new(DB, false);
    save.insert(RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9311),
        email: "CaseFold@Test.example".to_string(),
    })
    .expect("seed casefold unique row should save");

    // Phase 2: inserting a case-variant duplicate must violate unique ownership.
    let conflicting = RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9312),
        email: "casefold@test.example".to_string(),
    };
    let live_err = save
        .insert(conflicting.clone())
        .expect_err("casefold duplicate should violate unique expression index");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);

    // Phase 3: rejected insert must not leave a persisted primary row.
    let conflicting_key =
        DecodedDataStoreKey::try_new::<RecoveryUniqueCasefoldEntity>(conflicting.id)
            .expect("conflicting casefold key should build")
            .to_raw()
            .expect("conflicting casefold key should encode");
    assert!(
        row_bytes_for(&conflicting_key).is_none(),
        "conflicting casefold insert should not persist primary row",
    );
}

#[test]
fn unique_expression_conflict_classification_parity_holds_between_live_apply_and_replay() {
    reset_recovery_state();

    // Phase 1: capture live save-path casefold expression-unique conflict classification.
    let save = SaveExecutor::<RecoveryUniqueCasefoldEntity>::new(DB, false);
    save.insert(RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9313),
        email: "CaseFold-Replay@Test.example".to_string(),
    })
    .expect("seed casefold replay row should save in live path");
    let live_err = save
        .insert(RecoveryUniqueCasefoldEntity {
            id: Ulid::from_u128(9314),
            email: "casefold-replay@test.example".to_string(),
        })
        .expect_err("live save path should reject casefold duplicate unique value");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);

    // Phase 2: capture replay-path classification for the same casefold semantic conflict.
    reset_recovery_state();
    let replay_first = RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9315),
        email: "CaseFold-Replay@Test.example".to_string(),
    };
    let replay_second = RecoveryUniqueCasefoldEntity {
        id: Ulid::from_u128(9316),
        email: "casefold-replay@test.example".to_string(),
    };

    let replay_marker = CommitMarker::new(vec![
        row_op_for_path_with_schema(
            RecoveryUniqueCasefoldEntity::PATH,
            DecodedDataStoreKey::try_new::<RecoveryUniqueCasefoldEntity>(replay_first.id)
                .expect("first casefold replay key should build")
                .to_raw()
                .expect("first casefold replay key should encode")
                .as_bytes()
                .to_vec(),
            None,
            Some(canonical_row_bytes(&replay_first)),
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueCasefoldEntity>(),
        ),
        row_op_for_path_with_schema(
            RecoveryUniqueCasefoldEntity::PATH,
            DecodedDataStoreKey::try_new::<RecoveryUniqueCasefoldEntity>(replay_second.id)
                .expect("second casefold replay key should build")
                .to_raw()
                .expect("second casefold replay key should encode")
                .as_bytes()
                .to_vec(),
            None,
            Some(canonical_row_bytes(&replay_second)),
            initial_accepted_commit_schema_fingerprint_for_entity::<RecoveryUniqueCasefoldEntity>(),
        ),
    ])
    .expect("replay casefold unique-conflict marker should build");
    begin_commit(replay_marker)
        .expect("begin_commit should persist replay casefold conflict marker");

    let replay_err = ensure_recovered(&DB)
        .expect_err("replay recovery should reject casefold duplicate unique value marker");
    assert_eq!(replay_err.class, ErrorClass::Conflict);
    assert_eq!(replay_err.class, live_err.class);
    assert_eq!(replay_err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay casefold unique conflict must keep marker persisted for retry",
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn conditional_unique_conflict_classification_parity_holds_between_live_update_and_replay() {
    reset_recovery_state();

    let first_active = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9231),
        email: "dup-conditional-live-replay@example.com".to_string(),
        active: true,
    };
    let second_inactive = RecoveryConditionalUniqueEntity {
        id: Ulid::from_u128(9232),
        email: first_active.email.clone(),
        active: false,
    };
    let second_active = RecoveryConditionalUniqueEntity {
        id: second_inactive.id,
        email: second_inactive.email.clone(),
        active: true,
    };

    // Phase 1: capture live save-path conditional-unique conflict classification.
    let save = SaveExecutor::<RecoveryConditionalUniqueEntity>::new(DB, false);
    save.insert(first_active.clone())
        .expect("seed active conditional-unique row should save in live path");
    save.insert(second_inactive.clone())
        .expect("inactive duplicate should save in live path");
    let live_err = save
        .update(second_active.clone())
        .expect_err("live update path should reject duplicate conditional-unique activation");
    assert_eq!(live_err.class, ErrorClass::Conflict);
    assert_eq!(live_err.origin, ErrorOrigin::Index);

    // Phase 2: capture replay-path conditional-unique conflict for the same activation conflict.
    reset_recovery_state();
    let first_key = conditional_unique_data_key(first_active.id);
    let second_key = conditional_unique_data_key(second_inactive.id);
    let first_row = conditional_unique_row_bytes(&first_active);
    let second_inactive_row = conditional_unique_row_bytes(&second_inactive);
    let second_active_row = conditional_unique_row_bytes(&second_active);

    apply_row_ops_forward(&[
        row_op_for_path(
            RecoveryConditionalUniqueEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
        ),
        row_op_for_path(
            RecoveryConditionalUniqueEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_inactive_row.clone()),
        ),
    ])
    .expect("seed state apply should succeed before replay conflict marker");

    let replay_marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryConditionalUniqueEntity::PATH,
        second_key.as_bytes().to_vec(),
        Some(second_inactive_row.clone()),
        Some(second_active_row),
    )])
    .expect("replay conditional-unique conflict marker should build");
    begin_commit(replay_marker).expect("begin_commit should persist replay conflict marker");

    let replay_err = ensure_recovered(&DB)
        .expect_err("replay recovery should reject duplicate conditional-unique activation");
    assert_eq!(replay_err.class, ErrorClass::Conflict);
    assert_eq!(replay_err.class, live_err.class);
    assert_eq!(replay_err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed replay conditional-unique conflict must keep marker persisted for retry",
    );
    assert_eq!(
        row_bytes_for(&second_key),
        Some(second_inactive_row),
        "failed replay must keep the prior predicate-false row state visible",
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[expect(clippy::too_many_lines)]
#[test]
fn recovery_replays_interrupted_atomic_update_batch_marker_and_is_idempotent() {
    reset_recovery_state();

    let old_first = RecoveryIndexedEntity {
        id: Ulid::from_u128(909),
        group: 10,
    };
    let old_second = RecoveryIndexedEntity {
        id: Ulid::from_u128(910),
        group: 10,
    };
    let new_first = RecoveryIndexedEntity {
        id: old_first.id,
        group: 11,
    };
    let new_second = RecoveryIndexedEntity {
        id: old_second.id,
        group: 11,
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(old_first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(old_second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");

    let old_first_row = canonical_row_bytes(&old_first);
    let old_second_row = canonical_row_bytes(&old_second);
    let new_first_row = canonical_row_bytes(&new_first);
    let new_second_row = canonical_row_bytes(&new_second);

    // Phase 1: establish the pre-update durable state (group=10).
    let seed_marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(old_first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(old_second_row.clone()),
        ),
    ])
    .expect("seed marker creation should succeed");
    begin_commit(seed_marker).expect("seed begin_commit should persist marker");
    ensure_recovered(&DB).expect("seed replay should succeed");

    let old_indexed_ids_first =
        indexed_ids_for(&old_first).expect("old first index entry should exist after seed replay");
    let old_indexed_ids_second = indexed_ids_for(&old_second)
        .expect("old second index entry should exist after seed replay");
    assert_eq!(
        old_indexed_ids_first,
        std::iter::once(old_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        old_indexed_ids_second,
        std::iter::once(old_second.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(row_bytes_for(&first_key), Some(old_first_row.clone()));
    assert_eq!(row_bytes_for(&second_key), Some(old_second_row.clone()));

    // Phase 2: simulate an interrupted atomic update marker (group=10 -> group=11).
    let update_marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            Some(old_first_row),
            Some(new_first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            Some(old_second_row),
            Some(new_second_row.clone()),
        ),
    ])
    .expect("update marker creation should succeed");
    begin_commit(update_marker).expect("update begin_commit should persist marker");

    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "update marker should be present before recovery replay"
    );
    assert_eq!(
        row_bytes_for(&first_key),
        Some(canonical_row_bytes(&old_first)),
        "pre-recovery row bytes should still reflect old update state"
    );
    assert_eq!(
        row_bytes_for(&second_key),
        Some(canonical_row_bytes(&old_second)),
        "pre-recovery row bytes should still reflect old update state"
    );
    let pre_update_old_indexed_first =
        indexed_ids_for(&old_first).expect("old first index entry should still exist pre-recovery");
    let pre_update_old_indexed_second = indexed_ids_for(&old_second)
        .expect("old second index entry should still exist pre-recovery");
    assert_eq!(
        pre_update_old_indexed_first,
        std::iter::once(old_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        pre_update_old_indexed_second,
        std::iter::once(old_second.id).collect::<BTreeSet<_>>()
    );
    assert!(
        indexed_ids_for(&new_first).is_none(),
        "new index entry must not be visible before update replay"
    );

    // First replay applies update row ops and clears the marker.
    ensure_recovered(&DB).expect("update replay should succeed");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "update marker should be cleared after replay"
    );
    let first_after = row_bytes_for(&first_key);
    let second_after = row_bytes_for(&second_key);
    assert_eq!(first_after, Some(new_first_row));
    assert_eq!(second_after, Some(new_second_row));
    assert!(
        indexed_ids_for(&old_first).is_none(),
        "old index key should be removed after update replay"
    );
    let new_indexed_ids_first = indexed_ids_for(&new_first)
        .expect("new first index entry should exist after update replay");
    let new_indexed_ids_second = indexed_ids_for(&new_second)
        .expect("new second index entry should exist after update replay");
    assert_eq!(
        new_indexed_ids_first,
        std::iter::once(new_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        new_indexed_ids_second,
        std::iter::once(new_second.id).collect::<BTreeSet<_>>()
    );

    // Second replay is a no-op on already recovered state.
    ensure_recovered(&DB).expect("second update replay should be a no-op");
    assert_eq!(row_bytes_for(&first_key), first_after);
    assert_eq!(row_bytes_for(&second_key), second_after);
    assert!(
        indexed_ids_for(&old_first).is_none(),
        "old index key should remain absent after idempotent replay"
    );
    let new_indexed_second_first = indexed_ids_for(&new_first)
        .expect("new first index entry should remain after idempotent replay");
    let new_indexed_second_second = indexed_ids_for(&new_second)
        .expect("new second index entry should remain after idempotent replay");
    assert_eq!(
        new_indexed_second_first,
        std::iter::once(new_first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        new_indexed_second_second,
        std::iter::once(new_second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_replay_mixed_save_save_delete_sequence_preserves_final_index_state() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(905),
        group: 8,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(906),
        group: 8,
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    // Phase 1: replay two inserts sharing the same index key.
    let save_marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row.clone()),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row.clone()),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(save_marker).expect("begin_commit should persist marker");

    ensure_recovered(&DB).expect("recovery replay should succeed");
    assert_eq!(row_bytes_for(&first_key), Some(first_row.clone()));
    assert_eq!(row_bytes_for(&second_key), Some(second_row.clone()));

    let inserted_indexed_ids_first =
        indexed_ids_for(&first).expect("first index entry should exist after insert replay");
    let inserted_indexed_ids_second =
        indexed_ids_for(&second).expect("second index entry should exist after insert replay");
    assert_eq!(
        inserted_indexed_ids_first,
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        inserted_indexed_ids_second,
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );

    // Phase 2: replay a delete that removes one of the inserted rows.
    let delete_marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryIndexedEntity::PATH,
        second_key.as_bytes().to_vec(),
        Some(second_row),
        None,
    )])
    .expect("delete marker creation should succeed");
    begin_commit(delete_marker).expect("delete begin_commit should persist marker");

    ensure_recovered(&DB).expect("delete recovery replay should succeed");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after replay"
    );
    assert_eq!(row_bytes_for(&first_key), Some(first_row));
    assert_eq!(row_bytes_for(&second_key), None);

    let indexed_ids = indexed_ids_for(&first).expect("index entry should exist after replay");
    let expected_ids = std::iter::once(first.id).collect::<BTreeSet<_>>();
    assert_eq!(indexed_ids, expected_ids);
}

#[test]
fn recovery_ignores_live_only_heap_marker_rows_and_indexes() {
    reset_recovery_state();

    let entity = HeapRecoveryIndexedEntity {
        id: Ulid::from_u128(917),
        group: 70,
    };
    let data_key = DecodedDataStoreKey::try_new::<HeapRecoveryIndexedEntity>(entity.id)
        .expect("heap recovery data key should build")
        .to_raw()
        .expect("heap recovery data key should encode");
    let row = canonical_row_bytes(&entity);

    let marker = CommitMarker::from_parts(
        [0x77; 16],
        vec![row_op_for_path(
            HeapRecoveryIndexedEntity::PATH,
            data_key.as_bytes().to_vec(),
            None,
            Some(row),
        )],
        Vec::new(),
    )
    .expect("heap row-op marker creation should succeed");
    begin_commit(marker).expect("heap marker should persist through the generic commit gate");

    ensure_recovered(&DB).expect("recovery should skip live-only heap marker rows");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "heap-only marker should be cleared after recovery proves no durable replay",
    );
    with_heap_recovery_store(|store| {
        assert_eq!(
            store.with_data(DataStore::len),
            0,
            "recovery must not replay live-only heap data rows",
        );
        assert_eq!(
            store.with_index(IndexStore::len),
            0,
            "recovery must not rebuild live-only heap index entries",
        );
    });
}

#[test]
fn recovery_replay_preserves_index_key_raw_bytes_across_reloads() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(913),
        group: 20,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(914),
        group: 21,
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first index key build should succeed")
            .expect("first index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second index key build should succeed")
            .expect("second index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    let marker = CommitMarker::new(vec![
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            first_key.as_bytes().to_vec(),
            None,
            Some(first_row),
        ),
        row_op_for_path(
            RecoveryIndexedEntity::PATH,
            second_key.as_bytes().to_vec(),
            None,
            Some(second_row),
        ),
    ])
    .expect("commit marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    ensure_recovered(&DB).expect("first recovery replay should succeed");
    let first_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        first_snapshot, expected,
        "index key bytes after replay should match precomputed canonical bytes"
    );

    ensure_recovered(&DB).expect("second recovery replay should be no-op");
    let second_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        second_snapshot, expected,
        "index key bytes should remain stable after subsequent replay"
    );
    assert_eq!(second_snapshot, first_snapshot);
}

#[test]
fn recovery_startup_gate_rebuilds_secondary_indexes_from_authoritative_rows() {
    reset_recovery_state();

    let first = RecoveryIndexedEntity {
        id: Ulid::from_u128(920),
        group: 30,
    };
    let second = RecoveryIndexedEntity {
        id: Ulid::from_u128(921),
        group: 31,
    };
    let stale = RecoveryIndexedEntity {
        id: Ulid::from_u128(999),
        group: 99,
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(first.id)
        .expect("first data key should build")
        .to_raw()
        .expect("first data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(second.id)
        .expect("second data key should build")
        .to_raw()
        .expect("second data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let stale_key = IndexKey::new(&stale, index)
        .expect("stale key build should succeed")
        .expect("stale key should exist")
        .to_raw()
        .expect("test index key should encode");
    let stale_entry = IndexEntryValue::presence();

    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(
                data_store,
                first_key,
                RawRow::try_new(first_row).expect("first row raw construction should succeed"),
            );
            seed_canonical_data_row_for_recovery(
                data_store,
                second_key,
                RawRow::try_new(second_row).expect("second row raw construction should succeed"),
            );
        });
        store.with_index_mut(|index_store| {
            index_store.insert(stale_key, stale_entry);
        });
    });
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild indexes from data rows");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after startup recovery"
    );

    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first index key build should succeed")
            .expect("first index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second index key build should succeed")
            .expect("second index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should drop stale index entries and recreate canonical entries from rows"
    );
    assert_eq!(
        indexed_ids_for(&first).expect("first index entry should exist"),
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        indexed_ids_for(&second).expect("second index entry should exist"),
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_startup_rebuilds_secondary_index_characterization_window() {
    run_recovery_startup_rebuilds_secondary_index_characterization_window(256);
}

#[test]
fn recovery_startup_rebuilds_secondary_index_large_host_floor() {
    run_recovery_startup_rebuilds_secondary_index_characterization_window(1_024);
}

#[test]
fn recovery_startup_rebuilds_mixed_index_shapes_host_floor() {
    const ROWS_PER_SHAPE: u32 = 128;

    reset_recovery_state();

    let indexed = RecoveryIndexedEntity::MODEL.indexes()[0];
    let conditional = RecoveryConditionalEntity::MODEL.indexes()[0];
    let expression = RecoveryUpperExpressionEntity::MODEL.indexes()[0];
    let mut expected_index_keys = Vec::new();
    let mut active_sample = None;
    let mut inactive_sample = None;
    let mut expression_sample = None;

    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            for row in 0..ROWS_PER_SHAPE {
                let plain = RecoveryIndexedEntity {
                    id: Ulid::from_u128(20_000 + u128::from(row)),
                    group: row,
                };
                seed_indexed_recovery_entity(data_store, &plain);
                expected_index_keys.push(encoded_index_key_bytes(&plain, indexed));

                let active = row % 2 == 0;
                let conditional_row = RecoveryConditionalEntity {
                    id: Ulid::from_u128(30_000 + u128::from(row)),
                    group: row,
                    active,
                };
                seed_conditional_recovery_entity(data_store, &conditional_row);
                if active {
                    expected_index_keys
                        .push(encoded_index_key_bytes(&conditional_row, conditional));
                    active_sample.get_or_insert(conditional_row);
                } else {
                    inactive_sample.get_or_insert(conditional_row);
                }

                let expression_row = RecoveryUpperExpressionEntity {
                    id: Ulid::from_u128(40_000 + u128::from(row)),
                    email: format!("User{row}@Example.Com"),
                };
                seed_upper_expression_recovery_entity(data_store, &expression_row);
                expected_index_keys.push(encoded_index_key_bytes(&expression_row, expression));
                if row == ROWS_PER_SHAPE / 2 {
                    expression_sample = Some(expression_row);
                }
            }
        });
        store.with_index_mut(|index_store| {
            for stale_key in mixed_index_shape_stale_keys(indexed, conditional, expression) {
                index_store.insert(stale_key, IndexEntryValue::presence());
            }
        });
    });
    expected_index_keys.sort();

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild mixed index shapes from rows");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after mixed-shape characterization rebuild",
    );
    assert_eq!(
        index_key_bytes_snapshot(),
        expected_index_keys,
        "startup rebuild should recreate exactly the mixed-shape row-derived index set",
    );

    let (data_rows, index_rows) = recovery_store_snapshot();
    let expected_data_rows =
        usize::try_from(ROWS_PER_SHAPE.saturating_mul(3)).expect("row count should fit usize");
    assert_eq!(data_rows.len(), expected_data_rows);
    assert_eq!(index_rows.len(), expected_index_keys.len());

    let active = active_sample.expect("active sample should exist");
    let inactive = inactive_sample.expect("inactive sample should exist");
    let expression = expression_sample.expect("expression sample should exist");
    assert_eq!(
        conditional_indexed_ids_for(&active).expect("active conditional index entry should exist"),
        std::iter::once(active.id).collect::<BTreeSet<_>>(),
    );
    assert!(conditional_indexed_ids_for(&inactive).is_none());
    assert_eq!(
        upper_expression_indexed_ids_for(&expression).expect("expression index entry should exist"),
        std::iter::once(expression.id).collect::<BTreeSet<_>>(),
    );
}

fn run_recovery_startup_rebuilds_secondary_index_characterization_window(
    characterization_rows: u32,
) {
    reset_recovery_state();

    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let mut expected_index_keys = Vec::new();
    let mut sample_entities = Vec::new();

    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            for row in 0..characterization_rows {
                let entity = RecoveryIndexedEntity {
                    id: Ulid::from_u128(10_000 + u128::from(row)),
                    group: row,
                };
                let data_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(entity.id)
                    .expect("characterization data key should build")
                    .to_raw()
                    .expect("characterization data key should encode");
                let raw_row = RawRow::try_new(indexed_row_bytes(&entity))
                    .expect("characterization raw row should construct");
                seed_canonical_data_row_for_recovery(data_store, data_key, raw_row);

                expected_index_keys.push(
                    IndexKey::new(&entity, index)
                        .expect("characterization index key should build")
                        .expect("characterization index key should exist")
                        .to_raw()
                        .expect("characterization index key should encode")
                        .as_bytes()
                        .to_vec(),
                );
                if row == 0 || row == characterization_rows / 2 || row + 1 == characterization_rows
                {
                    sample_entities.push(entity);
                }
            }
        });
        store.with_index_mut(|index_store| {
            let stale = RecoveryIndexedEntity {
                id: Ulid::from_u128(19_999),
                group: characterization_rows.saturating_add(1),
            };
            let stale_key = IndexKey::new(&stale, index)
                .expect("stale characterization index key should build")
                .expect("stale characterization index key should exist")
                .to_raw()
                .expect("stale characterization index key should encode");
            index_store.insert(stale_key, IndexEntryValue::presence());
        });
    });
    expected_index_keys.sort();

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild characterization index window from rows");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after characterization rebuild",
    );
    assert_eq!(
        index_key_bytes_snapshot(),
        expected_index_keys,
        "startup rebuild should recreate exactly the characterized row-derived index set",
    );
    let expected_count = usize::try_from(characterization_rows)
        .expect("characterization row count should fit usize");
    let (data_rows, index_rows) = recovery_store_snapshot();
    assert_eq!(data_rows.len(), expected_count);
    assert_eq!(index_rows.len(), expected_count);

    for entity in sample_entities {
        assert_eq!(
            indexed_ids_for(&entity).expect("sample index entry should exist"),
            std::iter::once(entity.id).collect::<BTreeSet<_>>(),
            "sample index entry should decode back to its row id",
        );
    }
}

#[test]
fn recovery_startup_gate_rebuilds_secondary_indexes_from_old_nullable_rows() {
    reset_recovery_state();
    install_nullable_indexed_old_accepted_schema_prefix();

    let first = RecoveryNullableIndexedEntity {
        id: Ulid::from_u128(12_020),
        group: 30,
        nickname: None,
    };
    let second = RecoveryNullableIndexedEntity {
        id: Ulid::from_u128(12_021),
        group: 31,
        nickname: None,
    };
    let stale = RecoveryNullableIndexedEntity {
        id: Ulid::from_u128(12_099),
        group: 99,
        nickname: None,
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryNullableIndexedEntity>(first.id)
        .expect("first nullable data key should build")
        .to_raw()
        .expect("first nullable data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryNullableIndexedEntity>(second.id)
        .expect("second nullable data key should build")
        .to_raw()
        .expect("second nullable data key should encode");
    let first_row = old_nullable_indexed_raw_row_for_test(first.id, first.group);
    let second_row = old_nullable_indexed_raw_row_for_test(second.id, second.group);

    let index = RecoveryNullableIndexedEntity::MODEL.indexes()[0];
    let stale_key = IndexKey::new(&stale, index)
        .expect("stale nullable key build should succeed")
        .expect("stale nullable key should exist")
        .to_raw()
        .expect("test index key should encode");
    let stale_entry = IndexEntryValue::presence();

    // Phase 1: seed old two-slot rows and intentionally stale secondary index
    // state, then force startup recovery through the empty-marker rebuild gate.
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(data_store, first_key, first_row);
            seed_canonical_data_row_for_recovery(data_store, second_key, second_row);
        });
        store.with_index_mut(|index_store| {
            index_store.insert(stale_key, stale_entry);
        });
    });

    // Phase 2: recovery must reconcile the append-only nullable schema before
    // rebuilding indexes so old shorter rows decode under accepted contracts.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB)
        .expect("recovery should rebuild indexes from old nullable-layout data rows");

    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after nullable startup recovery"
    );

    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first nullable index key build should succeed")
            .expect("first nullable index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second nullable index key build should succeed")
            .expect("second nullable index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should drop stale entries and index old rows through accepted schema",
    );
    assert_eq!(
        nullable_indexed_ids_for(&first).expect("first nullable index entry should exist"),
        std::iter::once(first.id).collect::<BTreeSet<_>>()
    );
    assert_eq!(
        nullable_indexed_ids_for(&second).expect("second nullable index entry should exist"),
        std::iter::once(second.id).collect::<BTreeSet<_>>()
    );
    assert!(
        nullable_indexed_ids_for(&stale).is_none(),
        "stale nullable index-only rows must be dropped during startup rebuild",
    );
}

#[test]
fn recovery_replay_updates_old_nullable_row_before_image_with_accepted_contract() {
    reset_recovery_state();
    install_nullable_indexed_old_accepted_schema_prefix();

    let old = RecoveryNullableIndexedEntity {
        id: Ulid::from_u128(12_120),
        group: 30,
        nickname: None,
    };
    let new = RecoveryNullableIndexedEntity {
        id: old.id,
        group: 31,
        nickname: Some("accepted".to_string()),
    };

    let data_key = DecodedDataStoreKey::try_new::<RecoveryNullableIndexedEntity>(old.id)
        .expect("nullable update data key should build")
        .to_raw()
        .expect("nullable update data key should encode");
    let old_raw_row = old_nullable_indexed_raw_row_for_test(old.id, old.group);
    let old_row_bytes = old_raw_row.as_bytes().to_vec();
    let new_row_bytes = canonical_row_bytes(&new);

    let index = RecoveryNullableIndexedEntity::MODEL.indexes()[0];
    let old_index_key = IndexKey::new(&old, index)
        .expect("old nullable index key build should succeed")
        .expect("old nullable index key should exist")
        .to_raw()
        .expect("test index key should encode");
    let old_entry = IndexEntryValue::presence();

    // Phase 1: seed an old-layout authoritative row and matching old index
    // entry, then persist a marker that updates the row to current layout.
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(data_store, data_key.clone(), old_raw_row);
        });
        store.with_index_mut(|index_store| {
            index_store.insert(old_index_key, old_entry);
        });
    });

    let marker = CommitMarker::new(vec![row_op_for_path(
        RecoveryNullableIndexedEntity::PATH,
        data_key.as_bytes().to_vec(),
        Some(old_row_bytes),
        Some(new_row_bytes.clone()),
    )])
    .expect("nullable update marker creation should succeed");
    begin_commit(marker).expect("nullable update begin_commit should persist marker");

    // Phase 2: replay must decode the old before-image through the accepted
    // contract while writing the current-layout replacement row and index.
    ensure_recovered(&DB).expect("nullable update replay should decode old before-image");

    assert_eq!(
        row_bytes_for(&data_key),
        Some(new_row_bytes),
        "nullable update replay should write the current-layout row",
    );
    assert!(
        nullable_indexed_ids_for(&old).is_none(),
        "old nullable index key should be removed after update replay",
    );
    assert_eq!(
        nullable_indexed_ids_for(&new).expect("new nullable index entry should exist"),
        std::iter::once(new.id).collect::<BTreeSet<_>>()
    );
}

#[test]
fn recovery_startup_gate_rebuilds_conditional_indexes_from_authoritative_rows() {
    reset_recovery_state();

    let active = RecoveryConditionalEntity {
        id: Ulid::from_u128(926),
        group: 61,
        active: true,
    };
    let inactive = RecoveryConditionalEntity {
        id: Ulid::from_u128(927),
        group: 62,
        active: false,
    };
    let stale = RecoveryConditionalEntity {
        id: Ulid::from_u128(928),
        group: 63,
        active: true,
    };

    let active_key = DecodedDataStoreKey::try_new::<RecoveryConditionalEntity>(active.id)
        .expect("active data key should build")
        .to_raw()
        .expect("active data key should encode");
    let inactive_key = DecodedDataStoreKey::try_new::<RecoveryConditionalEntity>(inactive.id)
        .expect("inactive data key should build")
        .to_raw()
        .expect("inactive data key should encode");
    let active_row = canonical_row_bytes(&active);
    let inactive_row = canonical_row_bytes(&inactive);

    let index = RecoveryConditionalEntity::MODEL.indexes()[0];
    let inactive_index_key = IndexKey::new(&inactive, index)
        .expect("inactive index key build should succeed")
        .expect("inactive index key should exist")
        .to_raw()
        .expect("test index key should encode");
    let stale_index_key = IndexKey::new(&stale, index)
        .expect("stale index key build should succeed")
        .expect("stale index key should exist")
        .to_raw()
        .expect("test index key should encode");
    let inactive_entry = IndexEntryValue::presence();
    let stale_entry = IndexEntryValue::presence();

    // Phase 1: seed authoritative rows and intentionally stale conditional index state.
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(
                data_store,
                active_key,
                RawRow::try_new(active_row).expect("active raw row construction should succeed"),
            );
            seed_canonical_data_row_for_recovery(
                data_store,
                inactive_key,
                RawRow::try_new(inactive_row)
                    .expect("inactive raw row construction should succeed"),
            );
        });
        store.with_index_mut(|index_store| {
            index_store.insert(inactive_index_key, inactive_entry);
            index_store.insert(stale_index_key, stale_entry);
        });
    });

    // Phase 2: startup recovery must rebuild conditional index state from row truth only.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild conditional indexes from row truth");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after conditional startup recovery",
    );

    let mut expected = vec![
        IndexKey::new(&active, index)
            .expect("active conditional index key build should succeed")
            .expect("active conditional index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();
    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should keep predicate-true rows only and purge stale/predicate-false entries",
    );
    assert_eq!(
        conditional_indexed_ids_for(&active).expect("active conditional index entry should exist"),
        std::iter::once(active.id).collect::<BTreeSet<_>>(),
        "predicate-true rows must remain indexed after startup rebuild",
    );
    assert!(
        conditional_indexed_ids_for(&inactive).is_none(),
        "predicate-false rows must remain absent from the conditional index after rebuild",
    );
    assert!(
        conditional_indexed_ids_for(&stale).is_none(),
        "stale index-only rows must be dropped during conditional rebuild",
    );
}

#[test]
fn recovery_startup_gate_rebuilds_upper_expression_indexes_from_authoritative_rows() {
    reset_recovery_state();

    let first = RecoveryUpperExpressionEntity {
        id: Ulid::from_u128(940),
        email: "Alice@Example.Com".to_string(),
    };
    let second = RecoveryUpperExpressionEntity {
        id: Ulid::from_u128(941),
        email: "bob@example.com".to_string(),
    };
    let stale = RecoveryUpperExpressionEntity {
        id: Ulid::from_u128(999),
        email: "stale@example.com".to_string(),
    };

    let first_key = DecodedDataStoreKey::try_new::<RecoveryUpperExpressionEntity>(first.id)
        .expect("first expression data key should build")
        .to_raw()
        .expect("first expression data key should encode");
    let second_key = DecodedDataStoreKey::try_new::<RecoveryUpperExpressionEntity>(second.id)
        .expect("second expression data key should build")
        .to_raw()
        .expect("second expression data key should encode");
    let first_row = canonical_row_bytes(&first);
    let second_row = canonical_row_bytes(&second);

    let index = RecoveryUpperExpressionEntity::MODEL.indexes()[0];
    let stale_key = IndexKey::new(&stale, index)
        .expect("stale expression index key build should succeed")
        .expect("stale expression index key should exist")
        .to_raw()
        .expect("test index key should encode");
    let stale_entry = IndexEntryValue::presence();

    // Phase 1: seed authoritative rows and intentionally stale expression-index state.
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(
                data_store,
                first_key,
                RawRow::try_new(first_row)
                    .expect("first expression raw row construction should succeed"),
            );
            seed_canonical_data_row_for_recovery(
                data_store,
                second_key,
                RawRow::try_new(second_row)
                    .expect("second expression raw row construction should succeed"),
            );
        });
        store.with_index_mut(|index_store| {
            index_store.insert(stale_key, stale_entry);
        });
    });

    // Phase 2: startup recovery must rebuild expression index state from row truth only.
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    ensure_recovered(&DB).expect("recovery should rebuild expression indexes from row truth");
    assert!(
        !commit_marker_present().expect("commit marker check should succeed"),
        "commit marker should be cleared after expression startup recovery",
    );

    let mut expected = vec![
        IndexKey::new(&first, index)
            .expect("first expression index key build should succeed")
            .expect("first expression index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
        IndexKey::new(&second, index)
            .expect("second expression index key build should succeed")
            .expect("second expression index key should exist")
            .to_raw()
            .expect("test index key should encode")
            .as_bytes()
            .to_vec(),
    ];
    expected.sort();

    assert_eq!(
        index_key_bytes_snapshot(),
        expected,
        "startup rebuild should drop stale expression index entries and recreate canonical UPPER(email) keys from rows",
    );
}

#[test]
fn recovery_startup_rebuild_rejects_future_row_format_fail_closed() {
    reset_recovery_state();

    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(925),
        group: 34,
    };
    let raw_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(entity.id)
        .expect("row key should build")
        .to_raw()
        .expect("row key should encode");
    let payload = canonical_row_payload_bytes(&entity);
    let future_version = ROW_FORMAT_VERSION_CURRENT.saturating_add(1);
    let future_version_row = serialize_row_payload_with_version(payload, future_version)
        .expect("future-version row envelope should encode");

    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(
                data_store,
                raw_key.clone(),
                RawRow::try_new(future_version_row)
                    .expect("future-version row should fit raw row bounds"),
            );
        });
    });
    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    let err = ensure_recovered(&DB).expect_err("recovery should reject future row formats");

    assert_eq!(err.class, ErrorClass::IncompatiblePersistedFormat);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when recovery rejects incompatible row format",
    );
    assert!(
        row_bytes_for(&raw_key).is_some(),
        "failed recovery must not discard persisted rows",
    );
    assert!(
        indexed_ids_for(&entity).is_none(),
        "failed recovery must not publish index state for incompatible rows",
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}

#[test]
fn recovery_reconciles_schema_before_rebuilding_indexes_from_rows() {
    reset_recovery_state();

    let entity = RecoveryIndexedEntity {
        id: Ulid::from_u128(926),
        group: 35,
    };
    let raw_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(entity.id)
        .expect("row key should build")
        .to_raw()
        .expect("row key should encode");
    let payload = canonical_row_payload_bytes(&entity);
    let future_version = ROW_FORMAT_VERSION_CURRENT.saturating_add(1);
    let future_version_row = serialize_row_payload_with_version(payload, future_version)
        .expect("future-version row envelope should encode");

    let proposal = compiled_schema_proposal_for_model(RecoveryIndexedEntity::MODEL);
    let expected = proposal.initial_persisted_schema_snapshot();
    let changed = PersistedSchemaSnapshot::new(
        expected.version(),
        expected.entity_path().to_string(),
        "ChangedRecoveryIndexedEntity".to_string(),
        expected.first_primary_key_field_id(),
        SchemaRowLayout::new(
            SchemaVersion::initial(),
            expected.row_layout().field_to_slot().to_vec(),
        ),
        expected.fields().to_vec(),
    );

    with_recovery_store(|store| {
        store.with_schema_mut(|schema_store| {
            publish_test_accepted_schema_snapshot(
                schema_store,
                RecoveryIndexedEntity::ENTITY_TAG,
                RecoveryIndexedEntity::PATH,
                RecoveryTestDataStore::PATH,
                RecoveryIndexedEntity::MODEL,
                changed,
            )
            .expect("changed schema snapshot should publish");
        });
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(
                data_store,
                raw_key.clone(),
                RawRow::try_new(future_version_row)
                    .expect("future-version row should fit raw row bounds"),
            );
        });
    });
    mark_schema_reconciliation_dirty_for_tests(&DB);

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");
    let err = ensure_recovered(&DB)
        .expect_err("schema reconciliation should run before startup index rebuild row decode");

    assert_eq!(err.class, ErrorClass::Unsupported);
    assert_eq!(err.origin, ErrorOrigin::Recovery);
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "marker should remain present when schema reconciliation rejects before recovery"
    );
    assert!(
        row_bytes_for(&raw_key).is_some(),
        "failed schema reconciliation must not discard persisted rows",
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
    with_recovery_store(|store| {
        store.with_schema_mut(SchemaStore::clear);
    });
}

#[test]
fn recovery_startup_rebuild_fail_closed_restores_previous_index_state_on_corrupt_row() {
    reset_recovery_state();

    let sentinel = RecoveryIndexedEntity {
        id: Ulid::from_u128(922),
        group: 77,
    };
    let index = RecoveryIndexedEntity::MODEL.indexes()[0];
    let sentinel_key = IndexKey::new(&sentinel, index)
        .expect("sentinel key build should succeed")
        .expect("sentinel key should exist")
        .to_raw()
        .expect("test index key should encode");
    let sentinel_entry = IndexEntryValue::presence();

    with_recovery_store(|store| {
        store.with_index_mut(|index_store| {
            index_store.insert(sentinel_key.clone(), sentinel_entry);
        });
    });
    let before_snapshot = index_key_bytes_snapshot();

    let bad_key = DecodedDataStoreKey::try_new::<RecoveryIndexedEntity>(Ulid::from_u128(923))
        .expect("bad data key should build")
        .to_raw()
        .expect("bad data key should encode");
    with_recovery_store(|store| {
        store.with_data_mut(|data_store| {
            seed_canonical_data_row_for_recovery(
                data_store,
                bad_key,
                RawRow::try_new(vec![0xFF, 0x00, 0xAA]).expect("bad row raw construction"),
            );
        });
    });

    let marker = CommitMarker::new(Vec::new()).expect("marker creation should succeed");
    begin_commit(marker).expect("begin_commit should persist marker");

    let err = ensure_recovered(&DB).expect_err("startup rebuild should reject corrupted row bytes");
    assert_eq!(err.class, ErrorClass::Corruption);
    assert_eq!(err.origin, ErrorOrigin::Recovery);

    let after_snapshot = index_key_bytes_snapshot();
    assert_eq!(
        after_snapshot, before_snapshot,
        "failed startup rebuild must restore the prior index snapshot"
    );
    assert!(
        commit_marker_present().expect("commit marker check should succeed"),
        "failed startup rebuild must keep marker persisted for retry"
    );

    store::with_commit_store(|store| {
        store.clear_raw_for_tests();
        Ok(())
    })
    .expect("commit marker cleanup should succeed");
}
