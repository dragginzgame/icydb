//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedSchemaRevisionBundle {
    /// Build one bundle whose accepted declarations do not yet carry immutable
    /// proposal source identities.
    pub(in crate::db::schema) fn new(
        revision: AcceptedSchemaRevision,
        store_path: impl Into<String>,
        enum_catalog: AcceptedEnumCatalog,
        composite_catalog: AcceptedCompositeCatalog,
        entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    ) -> Result<Self, InternalError> {
        Self::new_with_source_bindings(
            revision,
            store_path,
            enum_catalog,
            composite_catalog,
            AcceptedSourceBindingCatalog::default(),
            entity_snapshots,
        )
    }

    #[must_use]
    pub(in crate::db) const fn source_bindings_for_tests(&self) -> &AcceptedSourceBindingCatalog {
        &self.source_bindings
    }
}

pub(in crate::db) fn empty_accepted_schema_candidate_for_tests(
    store_path: &str,
    revision: AcceptedSchemaRevision,
) -> CandidateSchemaRevision {
    accepted_schema_candidate_for_tests(store_path, revision, BTreeMap::new())
}

pub(in crate::db) fn accepted_schema_candidate_for_tests(
    store_path: &str,
    revision: AcceptedSchemaRevision,
    entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
) -> CandidateSchemaRevision {
    accepted_schema_candidate_with_field_bindings_for_tests(
        store_path,
        revision,
        entity_snapshots,
        BTreeMap::new(),
    )
}

pub(in crate::db) fn accepted_schema_candidate_with_field_bindings_for_tests(
    store_path: &str,
    revision: AcceptedSchemaRevision,
    entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
    fields: BTreeMap<(EntityTag, icydb_schema::FieldSourceKey), crate::db::schema::FieldId>,
) -> CandidateSchemaRevision {
    let entity_bindings = entity_snapshots
        .iter()
        .map(|(entity_tag, snapshot)| {
            let source = icydb_schema::EntitySourceKey::try_new(snapshot.entity_path())
                .expect("accepted entity fixture source should admit");
            (source, *entity_tag)
        })
        .collect();
    let source_bindings = AcceptedSourceBindingCatalog::initial(
        entity_bindings,
        fields,
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
    accepted_schema_candidate_with_catalogs_for_tests(
        store_path,
        revision,
        AcceptedEnumCatalog {
            by_id: BTreeMap::new(),
            id_by_path: BTreeMap::new(),
        },
        AcceptedCompositeCatalog::empty(),
        source_bindings,
        entity_snapshots,
    )
}

pub(in crate::db) fn accepted_schema_candidate_with_catalogs_for_tests(
    store_path: &str,
    revision: AcceptedSchemaRevision,
    enum_catalog: AcceptedEnumCatalog,
    composite_catalog: AcceptedCompositeCatalog,
    source_bindings: AcceptedSourceBindingCatalog,
    entity_snapshots: BTreeMap<EntityTag, PersistedSchemaSnapshot>,
) -> CandidateSchemaRevision {
    let bundle = AcceptedSchemaRevisionBundle::new_with_source_bindings(
        revision,
        store_path,
        enum_catalog,
        composite_catalog,
        source_bindings,
        entity_snapshots,
    )
    .expect("accepted schema candidate fixture should build");
    CandidateSchemaRevision::new(bundle).expect("accepted schema candidate fixture should encode")
}
