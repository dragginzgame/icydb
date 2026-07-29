//! Module: db::runtime_entity_catalog
//! Responsibility: enumerate and resolve runtime entities from accepted store catalogs.
//! Does not own: authored schema registration, proposal construction, or accepted mutation.
//! Boundary: joins registry-owned stores to their current accepted entity snapshots.

use crate::{
    db::{
        Db,
        commit::{
            CommitRowOp, PreparedRowCommitOp, prepare_commit_context_for_runtime_entity,
            prepare_row_commit_with_context,
        },
        data::RawDataStoreKey,
        registry::StoreHandle,
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
};
use std::{collections::BTreeSet, rc::Rc};

/// One accepted entity joined to its registry-owned runtime store.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedRuntimeEntity {
    entity_tag: EntityTag,
    entity_path: Rc<str>,
    entity_name: Rc<str>,
    store_path: &'static str,
}

impl AcceptedRuntimeEntity {
    /// Build one runtime route from an entity snapshot in the current accepted bundle.
    fn new(
        entity_tag: EntityTag,
        entity_path: impl Into<Rc<str>>,
        entity_name: impl Into<Rc<str>>,
        store_path: &'static str,
    ) -> Self {
        Self {
            entity_tag,
            entity_path: entity_path.into(),
            entity_name: entity_name.into(),
            store_path,
        }
    }

    /// Project one source-bound accepted snapshot onto its registry-owned store.
    pub(in crate::db) fn from_accepted_snapshot(
        bundle: &crate::db::schema::AcceptedSchemaRevisionBundle,
        entity_tag: EntityTag,
        snapshot: &crate::db::schema::PersistedSchemaSnapshot,
        registered_store_path: &'static str,
    ) -> Result<Self, InternalError> {
        let source = icydb_schema::EntitySourceKey::try_new(snapshot.entity_path())
            .map_err(|_| InternalError::store_corruption())?;
        if bundle.store_path() != registered_store_path
            || bundle.source_bindings().entity(&source) != Some(entity_tag)
        {
            return Err(InternalError::store_corruption());
        }
        Ok(Self::new(
            entity_tag,
            snapshot.entity_path(),
            snapshot.entity_name(),
            registered_store_path,
        ))
    }

    #[must_use]
    pub(in crate::db) const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    pub(in crate::db) fn entity_path(&self) -> &str {
        &self.entity_path
    }

    #[must_use]
    pub(in crate::db) fn entity_path_handle(&self) -> Rc<str> {
        self.entity_path.clone()
    }

    #[must_use]
    pub(in crate::db) fn entity_name(&self) -> &str {
        &self.entity_name
    }

    #[must_use]
    pub(in crate::db) const fn store_path(&self) -> &'static str {
        self.store_path
    }

    pub(in crate::db) fn store(
        &self,
        db: &Db<impl CanisterKind>,
    ) -> Result<StoreHandle, InternalError> {
        db.store_handle(self.store_path)
    }

    /// Resolve accepted commit authority for this accepted runtime entity.
    pub(in crate::db) fn prepare_commit_context<C: CanisterKind>(
        &self,
        db: &Db<C>,
        schema_fingerprint: crate::db::commit::CommitSchemaFingerprint,
        mode: crate::db::commit::CommitPrepareMode,
    ) -> Result<crate::db::commit::CommitPrepareContext, InternalError> {
        prepare_commit_context_for_runtime_entity(
            db,
            self.entity_path_handle(),
            self.entity_tag,
            self.store_path,
            schema_fingerprint,
            mode,
        )
    }
}

/// Enumerate every entity from the current accepted bundles of registered stores.
pub(in crate::db) fn accepted_runtime_entities<C: CanisterKind>(
    db: &Db<C>,
) -> Result<Vec<AcceptedRuntimeEntity>, InternalError> {
    let mut entities = Vec::new();
    for (store_path, store) in sorted_registered_stores(db) {
        entities.extend(
            store.with_schema(|schema| schema.current_accepted_runtime_entities(store_path))?,
        );
    }
    entities.sort_by(|left, right| {
        left.store_path()
            .cmp(right.store_path())
            .then_with(|| left.entity_tag().cmp(&right.entity_tag()))
            .then_with(|| left.entity_path().cmp(right.entity_path()))
    });

    let mut tags = BTreeSet::new();
    let mut paths = BTreeSet::new();
    for entity in &entities {
        if !tags.insert(entity.entity_tag()) || !paths.insert(entity.entity_path()) {
            return Err(InternalError::store_corruption());
        }
    }

    Ok(entities)
}

pub(in crate::db) fn accepted_runtime_entity_for_tag<C: CanisterKind>(
    db: &Db<C>,
    entity_tag: EntityTag,
) -> Result<AcceptedRuntimeEntity, InternalError> {
    let mut matched = None;
    for (store_path, store) in sorted_registered_stores(db) {
        let entity = store.with_schema(|schema| {
            schema.current_accepted_runtime_entity_for_tag(store_path, entity_tag)
        })?;
        merge_unique_entity_match(&mut matched, entity)?;
    }
    matched.ok_or_else(|| InternalError::unsupported_entity_tag_in_data_store(entity_tag))
}

pub(in crate::db) fn accepted_runtime_entity_for_path<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &str,
) -> Result<AcceptedRuntimeEntity, InternalError> {
    find_accepted_runtime_entity_for_path(db, entity_path)?
        .ok_or_else(|| InternalError::unsupported_entity_path(entity_path))
}

/// Find an exact immutable accepted entity path without classifying absence as
/// an invalid dynamic-name request.
pub(in crate::db) fn find_accepted_runtime_entity_for_path<C: CanisterKind>(
    db: &Db<C>,
    entity_path: &str,
) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
    let mut matched = None;
    for (store_path, store) in sorted_registered_stores(db) {
        let entity = store.with_schema(|schema| {
            schema.current_accepted_runtime_entity_for_path(store_path, entity_path)
        })?;
        merge_unique_entity_match(&mut matched, entity)?;
    }
    Ok(matched)
}

pub(in crate::db) fn accepted_runtime_entity_for_name<C: CanisterKind>(
    db: &Db<C>,
    entity_name: &str,
) -> Result<Option<AcceptedRuntimeEntity>, InternalError> {
    let mut matched = None;
    for (store_path, store) in sorted_registered_stores(db) {
        let entity = store.with_schema(|schema| {
            schema.current_accepted_runtime_entity_for_name(store_path, entity_name)
        })?;
        merge_unique_entity_match(&mut matched, entity)?;
    }
    Ok(matched)
}

fn sorted_registered_stores<C: CanisterKind>(db: &Db<C>) -> Vec<(&'static str, StoreHandle)> {
    let mut stores = db.with_store_registry(|registry| registry.iter().collect::<Vec<_>>());
    stores.sort_unstable_by_key(|(store_path, _)| *store_path);
    stores
}

fn merge_unique_entity_match(
    matched: &mut Option<AcceptedRuntimeEntity>,
    candidate: Option<AcceptedRuntimeEntity>,
) -> Result<(), InternalError> {
    let Some(candidate) = candidate else {
        return Ok(());
    };
    if matched.replace(candidate).is_some() {
        return Err(InternalError::store_corruption());
    }
    Ok(())
}

/// Resolve an entity from an unpublished candidate during marker-bound recovery.
pub(in crate::db) fn candidate_runtime_entity_for_path(
    candidate: &crate::db::schema::CandidateSchemaRevision,
    registered_store_path: &'static str,
    entity_path: &str,
) -> Result<AcceptedRuntimeEntity, InternalError> {
    if candidate.store_path() != registered_store_path {
        return Err(InternalError::store_corruption());
    }
    let mut matched = None;
    for (entity_tag, snapshot) in candidate.bundle().entity_snapshots() {
        if snapshot.entity_path() != entity_path {
            continue;
        }
        let entity = AcceptedRuntimeEntity::from_accepted_snapshot(
            candidate.bundle(),
            *entity_tag,
            snapshot,
            registered_store_path,
        )?;
        if matched.replace(entity).is_some() {
            return Err(InternalError::store_corruption());
        }
    }

    matched.ok_or_else(InternalError::store_corruption)
}

pub(in crate::db) fn prepare_row_commit<C: CanisterKind>(
    db: &Db<C>,
    op: &CommitRowOp,
    mode: crate::db::commit::CommitPrepareMode,
) -> Result<PreparedRowCommitOp, InternalError> {
    let entity = accepted_runtime_entity_for_path(db, op.entity_path.as_ref())?;
    let store = entity.store(db)?;
    let context = entity.prepare_commit_context(db, op.schema_fingerprint, mode)?;

    prepare_row_commit_with_context(db, op, &context, db, &store)
}

pub(in crate::db) fn validate_delete_relations<C: CanisterKind>(
    db: &Db<C>,
    target_path: &str,
    deleted_target_keys: &BTreeSet<RawDataStoreKey>,
) -> Result<(), InternalError> {
    if deleted_target_keys.is_empty() {
        return Ok(());
    }

    crate::db::relation::validate_candidate_relation_target_delete_barrier(
        db,
        target_path,
        deleted_target_keys,
    )?;

    for source in accepted_runtime_entities(db)? {
        let source_store = source.store(db)?;
        if !source_store.with_schema(|schema_store| {
            schema_store.entity_has_relation_to_target(source.entity_tag(), target_path)
        })? {
            continue;
        }
        crate::db::relation::validate_delete_relations_for_accepted_source(
            db,
            source.entity_tag(),
            source.entity_path(),
            source.store_path(),
            target_path,
            deleted_target_keys,
        )?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::{
            data::DataStore,
            index::IndexStore,
            integrity::DatabaseIncarnationId,
            registry::{StoreAllocationIdentities, StoreRegistry, StoreRuntimeStorageCapabilities},
            schema::{
                AcceptedFieldKind, AcceptedSchemaRevision, FieldId, FieldStorageDecode, LeafCodec,
                PersistedFieldSnapshot, PersistedSchemaSnapshot, ScalarCodec, SchemaFieldSlot,
                SchemaInsertDefault, SchemaRowLayout, SchemaStore, SchemaVersion,
                accepted_schema_candidate_for_tests,
            },
        },
        traits::Path,
    };
    use std::cell::RefCell;

    const STORE_PATH: &str = "runtime_entity_catalog::tests::Store";
    const ENTITY_PATH: &str = "runtime_entity_catalog::tests::Entity";

    struct TestCanister;

    impl Path for TestCanister {
        const PATH: &'static str = "runtime_entity_catalog::tests::Canister";
    }

    impl CanisterKind for TestCanister {
        const COMMIT_MEMORY_ID: u8 = 1;
        const COMMIT_STABLE_KEY: &'static str = "icydb.runtime_entity_catalog.commit.v1";
        const INTEGRITY_PROGRESS_MEMORY_ID: u8 = 2;
        const INTEGRITY_PROGRESS_STABLE_KEY: &'static str =
            "icydb.runtime_entity_catalog.integrity.progress.v1";
    }

    thread_local! {
        static DATA_STORE: RefCell<DataStore> = const { RefCell::new(DataStore::init_heap()) };
        static INDEX_STORE: RefCell<IndexStore> = const { RefCell::new(IndexStore::init_heap()) };
        static SCHEMA_STORE: RefCell<SchemaStore> =
            const { RefCell::new(SchemaStore::init_heap()) };
        static STORE_REGISTRY: StoreRegistry = {
            let mut registry = StoreRegistry::new();
            registry.register_store(
                STORE_PATH,
                &DATA_STORE,
                &INDEX_STORE,
                &SCHEMA_STORE,
                StoreAllocationIdentities::absent(),
                StoreRuntimeStorageCapabilities::heap(),
            ).expect("runtime entity test store should register");
            registry
        };
    }

    #[test]
    fn accepted_bundle_alone_supplies_runtime_entity_routing() {
        let entity_tag = EntityTag::new(91);
        let snapshot = PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            ENTITY_PATH.to_string(),
            "Entity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]),
            vec![PersistedFieldSnapshot::new_initial(
                FieldId::new(1),
                "id".to_string(),
                SchemaFieldSlot::new(0),
                AcceptedFieldKind::Ulid,
                Vec::new(),
                false,
                SchemaInsertDefault::None,
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Ulid),
            )],
        );
        let candidate = accepted_schema_candidate_for_tests(
            STORE_PATH,
            AcceptedSchemaRevision::INITIAL,
            std::collections::BTreeMap::from([(entity_tag, snapshot)]),
        );
        SCHEMA_STORE.with(|store| {
            let mut store = store.borrow_mut();
            *store = SchemaStore::init_heap();
            store
                .publish_accepted_schema_candidate(
                    DatabaseIncarnationId::for_tests(0x62),
                    AcceptedSchemaRevision::NONE,
                    &candidate,
                )
                .expect("accepted runtime candidate should publish");
        });

        let db = Db::<TestCanister>::new(&STORE_REGISTRY);
        let by_tag =
            accepted_runtime_entity_for_tag(&db, entity_tag).expect("accepted tag should route");
        let by_path =
            accepted_runtime_entity_for_path(&db, ENTITY_PATH).expect("accepted path should route");
        let by_name = accepted_runtime_entity_for_name(&db, "Entity")
            .expect("accepted name lookup should succeed")
            .expect("accepted name should route");
        let enumerated =
            accepted_runtime_entities(&db).expect("accepted entity catalog should enumerate");

        assert_eq!(by_tag, by_path);
        assert_eq!(by_path, by_name);
        assert_eq!(by_tag.entity_path(), ENTITY_PATH);
        assert_eq!(by_tag.store_path(), STORE_PATH);
        assert_eq!(enumerated, vec![by_tag]);
    }
}
