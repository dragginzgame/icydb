//! Module: db::schema::enum_catalog
//! Responsibility: canonicalize source or generated enum proposals into ID-backed catalog candidates.
//! Does not own: durable catalog publication, runtime value admission, or enum key encoding.
//! Boundary: exact source/generated enum definitions -> deterministic accepted enum catalog candidate.

mod admission;
pub(super) mod codec;
mod equality_key;
mod output;
mod publication;
mod value_wire;

use crate::{
    db::schema::FieldStorageDecode,
    db::schema::{
        AcceptedFieldKind, MAX_ACCEPTED_RECURSIVE_DEPTH,
        composite_catalog::AcceptedCompositeCatalog,
    },
    value::{CanonicalEnumBody, CanonicalEnumValue},
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(in crate::db) use crate::value::{EnumTypeId, EnumVariantId};
pub(in crate::db::schema) use admission::normalize_and_admit_nullable_value;
pub(in crate::db) use admission::normalize_candidate_value;
pub(in crate::db) use admission::validate_decoded_persisted_field_value_in_catalog;
pub(in crate::db) use admission::{
    AcceptedValueRef, AdmittedOwnedValue, CanonicalValue, MAX_ACCEPTED_VALUE_BYTES,
    ValueAdmissionBudget, ValueAdmissionError,
};
pub(in crate::db::schema) use admission::{
    admit_canonical_value, validate_nullable_canonical_value, with_normalized_accepted_value,
};
pub(in crate::db::schema) use codec::{decode_accepted_enum_catalog, encode_accepted_enum_catalog};
pub(in crate::db) use equality_key::encode_unit_enum_equality_key;
#[cfg(feature = "sql")]
pub(in crate::db) use equality_key::{EqualityCapability, enum_equality_capability};
pub(in crate::db) use output::output_value_from_runtime;
pub(in crate::db) use publication::AcceptedSchemaRevisionBundle;
#[cfg(test)]
pub(in crate::db::schema) use publication::decode_accepted_schema_revision_bundle;
pub(in crate::db::schema) use publication::{
    ACCEPTED_SCHEMA_ROOT_BYTES, AcceptedSchemaBundleKey, AcceptedSchemaPublicationError,
    AcceptedSchemaRoot, MAX_ACCEPTED_SCHEMA_BUNDLE_BYTES, MAX_SCHEMA_STORE_PATH_BYTES,
    decode_verified_accepted_schema_revision_bundle, prepare_accepted_schema_root_publication,
    select_current_accepted_schema_root,
};
pub(in crate::db) use publication::{
    AcceptedSchemaFingerprint, AcceptedSchemaRevision, AcceptedSchemaRootSelection,
    CandidateSchemaRevision,
};
#[cfg(test)]
pub(in crate::db) use publication::{
    accepted_schema_candidate_for_tests, accepted_schema_candidate_with_catalogs_for_tests,
    accepted_schema_candidate_with_field_bindings_for_tests,
    empty_accepted_schema_candidate_for_tests,
};
pub(in crate::db) use value_wire::{
    CanonicalEnumWireError, decode_canonical_enum_value, encode_canonical_enum_value,
};

/// Canonical enum ordering contract.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum EnumOrderingPolicy {
    EqualityOnly,
}

/// Canonical accepted enum definitions for one store-local catalog candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedEnumCatalog {
    by_id: BTreeMap<EnumTypeId, AcceptedEnumType>,
    id_by_path: BTreeMap<String, EnumTypeId>,
}

/// Opaque process-local identity for one store's accepted catalog domain.
#[derive(Clone)]
pub(in crate::db) struct AcceptedStoreCatalogScope(Arc<()>);

impl AcceptedStoreCatalogScope {
    #[must_use]
    pub(in crate::db::schema) fn new() -> Self {
        Self(Arc::new(()))
    }
}

impl std::fmt::Debug for AcceptedStoreCatalogScope {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("AcceptedStoreCatalogScope(..)")
    }
}

impl PartialEq for AcceptedStoreCatalogScope {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

impl Eq for AcceptedStoreCatalogScope {}

/// Store-local provenance retained by admitted values and execution plans.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedSchemaAuthority {
    store_scope: AcceptedStoreCatalogScope,
    revision: AcceptedSchemaRevision,
    fingerprint: AcceptedSchemaFingerprint,
}

impl AcceptedSchemaAuthority {
    #[must_use]
    pub(in crate::db) const fn revision(&self) -> AcceptedSchemaRevision {
        self.revision
    }

    /// Return whether this authority belongs to the supplied store-local
    /// catalog domain and still matches its current immutable root.
    #[must_use]
    pub(in crate::db::schema) fn matches_store_root(
        &self,
        store_scope: &AcceptedStoreCatalogScope,
        revision: AcceptedSchemaRevision,
        fingerprint: AcceptedSchemaFingerprint,
    ) -> bool {
        &self.store_scope == store_scope
            && self.revision == revision
            && self.fingerprint == fingerprint
    }

    #[must_use]
    pub(in crate::db) const fn fingerprint(&self) -> AcceptedSchemaFingerprint {
        self.fingerprint
    }
}

/// Shared immutable enum/composite catalog authority retained by one accepted revision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedValueCatalogHandle {
    enum_catalog: Arc<AcceptedEnumCatalog>,
    composite_catalog: Arc<AcceptedCompositeCatalog>,
    authority: AcceptedSchemaAuthority,
}

impl AcceptedValueCatalogHandle {
    #[must_use]
    pub(in crate::db::schema) fn new(
        enum_catalog: AcceptedEnumCatalog,
        composite_catalog: AcceptedCompositeCatalog,
        store_scope: AcceptedStoreCatalogScope,
        revision: AcceptedSchemaRevision,
        fingerprint: AcceptedSchemaFingerprint,
    ) -> Self {
        Self {
            enum_catalog: Arc::new(enum_catalog),
            composite_catalog: Arc::new(composite_catalog),
            authority: AcceptedSchemaAuthority {
                store_scope,
                revision,
                fingerprint,
            },
        }
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn new_for_tests(
        enum_catalog: AcceptedEnumCatalog,
        composite_catalog: AcceptedCompositeCatalog,
        revision: AcceptedSchemaRevision,
    ) -> Self {
        Self::new(
            enum_catalog,
            composite_catalog,
            AcceptedStoreCatalogScope::new(),
            revision,
            AcceptedSchemaFingerprint::new([0xA5; 32]),
        )
    }

    #[must_use]
    pub(in crate::db) fn enum_catalog(&self) -> &AcceptedEnumCatalog {
        self.enum_catalog.as_ref()
    }

    #[must_use]
    pub(in crate::db) fn composite_catalog(&self) -> &AcceptedCompositeCatalog {
        self.composite_catalog.as_ref()
    }

    #[must_use]
    pub(in crate::db) const fn authority(&self) -> &AcceptedSchemaAuthority {
        &self.authority
    }

    #[must_use]
    pub(in crate::db) const fn revision(&self) -> AcceptedSchemaRevision {
        self.authority.revision()
    }
}

pub(in crate::db::schema) type InitialEnumVariantDefinitions =
    BTreeMap<EnumVariantId, (String, Option<(AcceptedFieldKind, FieldStorageDecode)>)>;
type InitialEnumDefinitions = BTreeMap<EnumTypeId, (String, InitialEnumVariantDefinitions)>;

fn variant_payload_matches(
    accepted: &AcceptedEnumVariantBody,
    proposed: Option<&(AcceptedFieldKind, FieldStorageDecode)>,
) -> bool {
    match (accepted, proposed) {
        (AcceptedEnumVariantBody::Unit, None) => true,
        (AcceptedEnumVariantBody::Payload { contract }, Some((kind, storage_decode))) => {
            contract.kind() == kind && contract.storage_decode() == *storage_decode
        }
        (AcceptedEnumVariantBody::Unit, Some(_))
        | (AcceptedEnumVariantBody::Payload { .. }, None) => false,
    }
}

impl AcceptedEnumCatalog {
    /// Rename one accepted enum type path without changing its identity or
    /// value contract. Source-migration planning applies this only after an
    /// explicit old/new binding has resolved.
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn with_renamed_type(
        mut self,
        type_id: EnumTypeId,
        new_path: String,
    ) -> Result<Self, EnumCatalogBuildError> {
        if new_path.is_empty() || self.id_by_path.contains_key(&new_path) {
            return Err(EnumCatalogBuildError::LookupMapInvariant);
        }
        let definition = self
            .by_id
            .get_mut(&type_id)
            .ok_or(EnumCatalogBuildError::LookupMapInvariant)?;
        if self.id_by_path.remove(definition.path.as_str()) != Some(type_id) {
            return Err(EnumCatalogBuildError::LookupMapInvariant);
        }
        definition.path.clone_from(&new_path);
        self.id_by_path.insert(new_path, type_id);
        Ok(self)
    }

    /// Rename one accepted enum variant without changing its accepted ID or
    /// payload contract.
    #[cfg(any(test, feature = "migration"))]
    pub(in crate::db::schema) fn with_renamed_variant(
        mut self,
        type_id: EnumTypeId,
        variant_id: EnumVariantId,
        new_name: String,
    ) -> Result<Self, EnumCatalogBuildError> {
        let definition = self
            .by_id
            .get_mut(&type_id)
            .ok_or(EnumCatalogBuildError::LookupMapInvariant)?;
        if new_name.is_empty() || definition.variant_id_by_name.contains_key(&new_name) {
            return Err(EnumCatalogBuildError::LookupMapInvariant);
        }
        let variant = definition
            .variants_by_id
            .get_mut(&variant_id)
            .ok_or(EnumCatalogBuildError::LookupMapInvariant)?;
        if definition.variant_id_by_name.remove(variant.name.as_str()) != Some(variant_id) {
            return Err(EnumCatalogBuildError::LookupMapInvariant);
        }
        variant.name.clone_from(&new_name);
        definition.variant_id_by_name.insert(new_name, variant_id);
        Ok(self)
    }

    /// Construct one initial enum catalog from already allocated store-local
    /// identities and exact payload contracts.
    pub(in crate::db::schema) fn from_initial_definitions(
        definitions: InitialEnumDefinitions,
    ) -> Result<Self, EnumCatalogBuildError> {
        let mut by_id = BTreeMap::new();
        let mut id_by_path = BTreeMap::new();
        for (type_id, (path, variants)) in definitions {
            if path.is_empty()
                || variants.is_empty()
                || id_by_path.insert(path.clone(), type_id).is_some()
            {
                return Err(EnumCatalogBuildError::LookupMapInvariant);
            }
            let mut variants_by_id = BTreeMap::new();
            let mut variant_id_by_name = BTreeMap::new();
            for (variant_id, (name, payload)) in variants {
                if name.is_empty()
                    || variant_id_by_name
                        .insert(name.clone(), variant_id)
                        .is_some()
                {
                    return Err(EnumCatalogBuildError::LookupMapInvariant);
                }
                variants_by_id.insert(
                    variant_id,
                    AcceptedEnumVariant {
                        name,
                        body: payload.map_or(
                            AcceptedEnumVariantBody::Unit,
                            |(kind, storage_decode)| AcceptedEnumVariantBody::Payload {
                                contract: AcceptedValueContract {
                                    kind,
                                    storage_decode,
                                },
                            },
                        ),
                    },
                );
            }
            by_id.insert(
                type_id,
                AcceptedEnumType {
                    path,
                    variants_by_id,
                    variant_id_by_name,
                    ordering: EnumOrderingPolicy::EqualityOnly,
                },
            );
        }
        let catalog = Self { by_id, id_by_path };
        if !catalog.validate() {
            return Err(EnumCatalogBuildError::LookupMapInvariant);
        }
        Ok(catalog)
    }

    /// Remove an exact set of accepted enum definitions.
    ///
    /// The application lowerer resolves the immutable source identity and
    /// removes dependent source bindings separately. Catalog and bundle
    /// validation reject any retained definition that still refers to a
    /// removed type. Validating after the complete set is absent permits an
    /// unreferenced recursive component to be removed atomically.
    pub(in crate::db::schema) fn with_removed_types(
        mut self,
        type_ids: &BTreeSet<EnumTypeId>,
    ) -> Result<Self, EnumCatalogBuildError> {
        for type_id in type_ids {
            let definition = self
                .by_id
                .remove(type_id)
                .ok_or(EnumCatalogBuildError::LookupMapInvariant)?;
            if self.id_by_path.remove(definition.path.as_str()) != Some(*type_id) {
                return Err(EnumCatalogBuildError::LookupMapInvariant);
            }
        }
        if !self.validate() {
            return Err(EnumCatalogBuildError::LookupMapInvariant);
        }
        Ok(self)
    }

    fn validate(&self) -> bool {
        self.lookup_maps_are_bijective()
            && self.by_id.values().all(|definition| {
                definition.variants_by_id.values().all(|variant| {
                    let AcceptedEnumVariantBody::Payload { contract } = &variant.body else {
                        return true;
                    };
                    accepted_kind_matches_catalog(self, &contract.kind, 0)
                })
            })
    }

    fn lookup_maps_are_bijective(&self) -> bool {
        self.by_id.len() == self.id_by_path.len()
            && self.id_by_path.iter().all(|(path, type_id)| {
                self.by_id
                    .get(type_id)
                    .is_some_and(|definition| definition.path == *path)
            })
            && self.by_id.iter().all(|(type_id, definition)| {
                self.id_by_path.get(definition.path.as_str()) == Some(type_id)
                    && definition.lookup_maps_are_bijective()
            })
    }

    #[must_use]
    pub(in crate::db) fn type_id(&self, path: &str) -> Option<EnumTypeId> {
        self.id_by_path.get(path).copied()
    }

    #[must_use]
    pub(in crate::db) fn enum_type(&self, id: EnumTypeId) -> Option<&AcceptedEnumType> {
        self.by_id.get(&id)
    }

    /// Resolve one canonical enum value against this exact catalog authority.
    pub(in crate::db) fn resolve_value<'catalog, 'value, V>(
        &'catalog self,
        value: &'value CanonicalEnumValue<V>,
    ) -> Result<AcceptedEnumValueSelection<'catalog, 'value, V>, EnumValueResolutionError> {
        let definition = self
            .enum_type(value.type_id())
            .ok_or(EnumValueResolutionError::UnknownType)?;
        let variant = definition
            .variant(value.variant_id())
            .ok_or(EnumValueResolutionError::UnknownVariant)?;

        Ok(AcceptedEnumValueSelection {
            type_id: value.type_id(),
            variant_id: value.variant_id(),
            definition,
            variant,
            body: value.body(),
        })
    }

    pub(super) fn matches_accepted_kind(&self, kind: &AcceptedFieldKind) -> bool {
        accepted_kind_matches_catalog(self, kind, 0)
    }

    /// Verify that every composite identity reachable from an enum payload is
    /// owned by the supplied store-local composite catalog.
    pub(in crate::db::schema) fn composite_references_are_resolved(
        &self,
        composite_catalog: &AcceptedCompositeCatalog,
    ) -> bool {
        self.by_id.values().all(|definition| {
            definition.variants_by_id.values().all(|variant| {
                if let AcceptedEnumVariantBody::Payload { contract } = &variant.body {
                    accepted_kind_composites_are_resolved(&contract.kind, composite_catalog, 0)
                } else {
                    true
                }
            })
        })
    }
}

/// Catalog-backed view of one canonical enum value.
///
/// This keeps ID resolution, schema-visible names, the accepted variant
/// contract, and the runtime body attached to one catalog borrow.
pub(in crate::db) struct AcceptedEnumValueSelection<'catalog, 'value, V> {
    type_id: EnumTypeId,
    variant_id: EnumVariantId,
    definition: &'catalog AcceptedEnumType,
    variant: &'catalog AcceptedEnumVariant,
    body: &'value CanonicalEnumBody<V>,
}

impl<V> AcceptedEnumValueSelection<'_, '_, V> {
    #[must_use]
    pub(in crate::db) const fn type_id(&self) -> EnumTypeId {
        self.type_id
    }

    #[must_use]
    pub(in crate::db) const fn variant_id(&self) -> EnumVariantId {
        self.variant_id
    }

    #[must_use]
    pub(in crate::db) const fn path(&self) -> &str {
        self.definition.path.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn variant_name(&self) -> &str {
        self.variant.name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn accepted_body(&self) -> &AcceptedEnumVariantBody {
        self.variant.body()
    }

    #[must_use]
    pub(in crate::db) const fn value_body(&self) -> &CanonicalEnumBody<V> {
        self.body
    }
}

/// Failure to resolve canonical store-local enum IDs in one accepted catalog.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum EnumValueResolutionError {
    UnknownType,
    UnknownVariant,
}

/// One canonical accepted enum type definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedEnumType {
    path: String,
    variants_by_id: BTreeMap<EnumVariantId, AcceptedEnumVariant>,
    variant_id_by_name: BTreeMap<String, EnumVariantId>,
    ordering: EnumOrderingPolicy,
}

impl AcceptedEnumType {
    #[must_use]
    pub(in crate::db::schema) const fn path(&self) -> &str {
        self.path.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) fn variant_count(&self) -> usize {
        self.variants_by_id.len()
    }

    /// Iterate accepted variants in stable identity order.
    pub(in crate::db::schema) fn variants(&self) -> impl Iterator<Item = &AcceptedEnumVariant> {
        self.variants_by_id.values()
    }

    /// Return whether one source-bound declaration exactly matches this
    /// accepted type without interpreting name changes as metadata edits.
    pub(in crate::db::schema) fn matches_exact_definition(
        &self,
        path: &str,
        variants: &InitialEnumVariantDefinitions,
    ) -> bool {
        self.path == path
            && self.variants_by_id.len() == variants.len()
            && variants.iter().all(|(variant_id, (name, payload))| {
                self.variants_by_id.get(variant_id).is_some_and(|variant| {
                    variant.name == *name
                        && variant_payload_matches(&variant.body, payload.as_ref())
                })
            })
    }

    fn lookup_maps_are_bijective(&self) -> bool {
        self.variants_by_id.len() == self.variant_id_by_name.len()
            && self.variant_id_by_name.iter().all(|(name, variant_id)| {
                self.variants_by_id
                    .get(variant_id)
                    .is_some_and(|variant| variant.name == *name)
            })
            && self.variants_by_id.iter().all(|(variant_id, variant)| {
                self.variant_id_by_name.get(variant.name.as_str()) == Some(variant_id)
            })
    }

    #[must_use]
    pub(in crate::db) fn variant_id(&self, name: &str) -> Option<EnumVariantId> {
        self.variant_id_by_name.get(name).copied()
    }

    #[must_use]
    pub(in crate::db) fn variant(&self, id: EnumVariantId) -> Option<&AcceptedEnumVariant> {
        self.variants_by_id.get(&id)
    }
}

/// One accepted enum variant with structurally valid unit/payload state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedEnumVariant {
    name: String,
    body: AcceptedEnumVariantBody,
}

impl AcceptedEnumVariant {
    /// Borrow the accepted schema-visible variant name.
    #[must_use]
    pub(in crate::db::schema) const fn name(&self) -> &str {
        self.name.as_str()
    }

    #[must_use]
    pub(in crate::db) const fn body(&self) -> &AcceptedEnumVariantBody {
        &self.body
    }
}

/// Accepted unit or payload-bearing enum variant contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedEnumVariantBody {
    Unit,
    Payload { contract: AcceptedValueContract },
}

/// Accepted payload kind and storage decoder as one inseparable contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedValueContract {
    kind: AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
}

impl AcceptedValueContract {
    pub(in crate::db) fn from_accepted_field(
        catalog: &AcceptedValueCatalogHandle,
        kind: &AcceptedFieldKind,
        storage_decode: FieldStorageDecode,
    ) -> Result<Self, EnumCatalogBuildError> {
        Self::from_candidate_catalogs(
            catalog.enum_catalog(),
            catalog.composite_catalog(),
            kind,
            storage_decode,
        )
    }

    pub(in crate::db) fn from_candidate_catalogs(
        enum_catalog: &AcceptedEnumCatalog,
        composite_catalog: &AcceptedCompositeCatalog,
        kind: &AcceptedFieldKind,
        storage_decode: FieldStorageDecode,
    ) -> Result<Self, EnumCatalogBuildError> {
        if !composite_catalog.matches_kind(enum_catalog, kind) {
            return Err(EnumCatalogBuildError::LookupMapInvariant);
        }
        Ok(Self {
            kind: kind.clone(),
            storage_decode,
        })
    }

    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    /// Derive the accepted element contract for a list or set value.
    #[must_use]
    pub(in crate::db) fn collection_element_contract(&self) -> Option<Self> {
        match &self.kind {
            AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => Some(Self {
                kind: inner.as_ref().clone(),
                storage_decode: FieldStorageDecode::ByKind,
            }),
            _ => None,
        }
    }
}

/// Typed failure while canonicalizing one generated enum catalog candidate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum EnumCatalogBuildError {
    #[cfg(test)]
    EmptyTypePath,
    #[cfg(test)]
    EmptyVariantName {
        path: String,
    },
    #[cfg(test)]
    DuplicateVariantName {
        path: String,
        name: String,
    },
    #[cfg(test)]
    ConflictingDefinition {
        path: String,
    },
    #[cfg(test)]
    EnumTypeIdExhausted,
    #[cfg(test)]
    EnumVariantIdExhausted {
        path: String,
    },
    #[cfg(test)]
    UnknownEnumPath {
        path: String,
    },
    LookupMapInvariant,
}

#[cfg(test)]
struct RawEnumVariantProposal {
    payload_kind: Option<AcceptedFieldKind>,
    payload_storage_decode: FieldStorageDecode,
}

#[cfg(test)]
struct RawEnumDefinitionProposal {
    variants: BTreeMap<String, RawEnumVariantProposal>,
}

/// Accepted-native enum definition fixture for catalog codec and boundary tests.
#[cfg(test)]
pub(in crate::db) struct TestEnumDefinition {
    path: &'static str,
    variants: Vec<TestEnumVariant>,
}

#[cfg(test)]
impl TestEnumDefinition {
    pub(in crate::db) fn new(path: &'static str, variants: Vec<TestEnumVariant>) -> Self {
        Self { path, variants }
    }
}

/// Accepted-native enum variant fixture.
#[cfg(test)]
pub(in crate::db) struct TestEnumVariant {
    name: &'static str,
    payload: Option<(AcceptedFieldKind, FieldStorageDecode)>,
}

#[cfg(test)]
impl TestEnumVariant {
    pub(in crate::db) const fn unit(name: &'static str) -> Self {
        Self {
            name,
            payload: None,
        }
    }

    pub(in crate::db) const fn payload(
        name: &'static str,
        kind: AcceptedFieldKind,
        storage_decode: FieldStorageDecode,
    ) -> Self {
        Self {
            name,
            payload: Some((kind, storage_decode)),
        }
    }
}

#[cfg(test)]
pub(in crate::db) fn empty_accepted_enum_catalog_for_tests() -> AcceptedEnumCatalog {
    AcceptedEnumCatalog::from_initial_definitions(BTreeMap::new())
        .expect("empty accepted enum catalog is valid")
}

#[cfg(test)]
pub(in crate::db) fn build_accepted_enum_catalog_for_tests(
    definitions: &[TestEnumDefinition],
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    let mut proposals = BTreeMap::<String, Vec<RawEnumDefinitionProposal>>::new();
    for definition in definitions {
        if definition.path.is_empty() {
            return Err(EnumCatalogBuildError::EmptyTypePath);
        }
        let mut variants = BTreeMap::new();
        for variant in &definition.variants {
            if variant.name.is_empty() {
                return Err(EnumCatalogBuildError::EmptyVariantName {
                    path: definition.path.to_string(),
                });
            }
            let proposal = match &variant.payload {
                Some((kind, storage_decode)) => RawEnumVariantProposal {
                    payload_kind: Some(kind.clone()),
                    payload_storage_decode: *storage_decode,
                },
                None => RawEnumVariantProposal {
                    payload_kind: None,
                    payload_storage_decode: FieldStorageDecode::ByKind,
                },
            };
            if variants
                .insert(variant.name.to_string(), proposal)
                .is_some()
            {
                return Err(EnumCatalogBuildError::DuplicateVariantName {
                    path: definition.path.to_string(),
                    name: variant.name.to_string(),
                });
            }
        }
        proposals
            .entry(definition.path.to_string())
            .or_default()
            .push(RawEnumDefinitionProposal { variants });
    }

    build_catalog_from_definitions(proposals)
}

#[cfg(test)]
fn build_catalog_from_definitions(
    definitions: BTreeMap<String, Vec<RawEnumDefinitionProposal>>,
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    let mut id_by_path = BTreeMap::new();
    let mut last_type_id = None;
    for path in definitions.keys() {
        let type_id = next_type_id(last_type_id)?;
        id_by_path.insert(path.clone(), type_id);
        last_type_id = Some(type_id);
    }

    let mut by_id = BTreeMap::new();
    for (path, proposals) in definitions {
        let type_id = id_by_path
            .get(path.as_str())
            .copied()
            .ok_or_else(|| EnumCatalogBuildError::UnknownEnumPath { path: path.clone() })?;
        let variant_ids = allocate_variant_ids(&path, &proposals)?;
        let accepted_definition =
            accepted_enum_type_from_proposals(&path, proposals, &variant_ids)?;
        by_id.insert(type_id, accepted_definition);
    }

    let catalog = AcceptedEnumCatalog { by_id, id_by_path };
    if !catalog.validate() {
        return Err(EnumCatalogBuildError::LookupMapInvariant);
    }

    Ok(catalog)
}

#[cfg(test)]
fn allocate_variant_ids(
    path: &str,
    proposals: &[RawEnumDefinitionProposal],
) -> Result<BTreeMap<String, EnumVariantId>, EnumCatalogBuildError> {
    let mut ids = BTreeMap::new();
    let mut last_variant_id = None;
    let proposed_names = proposals
        .iter()
        .flat_map(|proposal| proposal.variants.keys())
        .cloned()
        .collect::<BTreeSet<_>>();
    for name in proposed_names {
        if ids.contains_key(name.as_str()) {
            continue;
        }
        let variant_id = next_variant_id(path, last_variant_id)?;
        ids.insert(name, variant_id);
        last_variant_id = Some(variant_id);
    }

    Ok(ids)
}

#[cfg(test)]
fn accepted_enum_type_from_proposals(
    path: &str,
    proposals: Vec<RawEnumDefinitionProposal>,
    variant_id_by_name: &BTreeMap<String, EnumVariantId>,
) -> Result<AcceptedEnumType, EnumCatalogBuildError> {
    let mut accepted_definition = None;
    for proposal in proposals {
        let candidate = accepted_enum_type_from_proposal(path, proposal, variant_id_by_name)?;
        if let Some(accepted) = accepted_definition.as_ref()
            && accepted != &candidate
        {
            return Err(EnumCatalogBuildError::ConflictingDefinition {
                path: path.to_string(),
            });
        }
        accepted_definition = Some(candidate);
    }

    accepted_definition.ok_or_else(|| EnumCatalogBuildError::ConflictingDefinition {
        path: path.to_string(),
    })
}

#[cfg(test)]
fn accepted_enum_type_from_proposal(
    path: &str,
    proposal: RawEnumDefinitionProposal,
    variant_id_by_name: &BTreeMap<String, EnumVariantId>,
) -> Result<AcceptedEnumType, EnumCatalogBuildError> {
    let mut variants_by_id = BTreeMap::new();
    let mut candidate_variant_id_by_name = BTreeMap::new();
    for (name, proposal) in proposal.variants {
        let variant_id = variant_id_by_name
            .get(name.as_str())
            .copied()
            .ok_or_else(|| EnumCatalogBuildError::ConflictingDefinition {
                path: path.to_string(),
            })?;
        let body = match proposal.payload_kind {
            Some(kind) => AcceptedEnumVariantBody::Payload {
                contract: AcceptedValueContract {
                    kind,
                    storage_decode: proposal.payload_storage_decode,
                },
            },
            None => AcceptedEnumVariantBody::Unit,
        };
        variants_by_id.insert(
            variant_id,
            AcceptedEnumVariant {
                name: name.clone(),
                body,
            },
        );
        candidate_variant_id_by_name.insert(name, variant_id);
    }

    Ok(AcceptedEnumType {
        path: path.to_string(),
        variants_by_id,
        variant_id_by_name: candidate_variant_id_by_name,
        ordering: EnumOrderingPolicy::EqualityOnly,
    })
}

fn accepted_kind_matches_catalog(
    catalog: &AcceptedEnumCatalog,
    kind: &AcceptedFieldKind,
    depth: usize,
) -> bool {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return false;
    }
    match kind {
        AcceptedFieldKind::Enum { type_id } => catalog.enum_type(*type_id).is_some(),
        AcceptedFieldKind::Relation { key_kind, .. }
        | AcceptedFieldKind::List(key_kind)
        | AcceptedFieldKind::Set(key_kind) => {
            accepted_kind_matches_catalog(catalog, key_kind, depth.saturating_add(1))
        }
        AcceptedFieldKind::Map { key, value } => {
            accepted_kind_matches_catalog(catalog, key, depth.saturating_add(1))
                && accepted_kind_matches_catalog(catalog, value, depth.saturating_add(1))
        }
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. }
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::Composite { .. }
        | AcceptedFieldKind::U256 => true,
    }
}

#[cfg(test)]
fn next_type_id(last: Option<EnumTypeId>) -> Result<EnumTypeId, EnumCatalogBuildError> {
    let value = match last {
        Some(last) => last
            .get()
            .checked_add(1)
            .ok_or(EnumCatalogBuildError::EnumTypeIdExhausted)?,
        None => 1,
    };
    EnumTypeId::new(value).ok_or(EnumCatalogBuildError::EnumTypeIdExhausted)
}

#[cfg(test)]
fn next_variant_id(
    path: &str,
    last: Option<EnumVariantId>,
) -> Result<EnumVariantId, EnumCatalogBuildError> {
    let exhausted = || EnumCatalogBuildError::EnumVariantIdExhausted {
        path: path.to_string(),
    };
    let value = match last {
        Some(last) => last.get().checked_add(1).ok_or_else(exhausted)?,
        None => 1,
    };
    EnumVariantId::new(value).ok_or_else(exhausted)
}

fn accepted_kind_composites_are_resolved(
    kind: &AcceptedFieldKind,
    composite_catalog: &AcceptedCompositeCatalog,
    depth: usize,
) -> bool {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return false;
    }
    match kind {
        AcceptedFieldKind::Composite { type_id } => {
            composite_catalog.composite_type(*type_id).is_some()
        }
        AcceptedFieldKind::Relation { key_kind, .. }
        | AcceptedFieldKind::List(key_kind)
        | AcceptedFieldKind::Set(key_kind) => accepted_kind_composites_are_resolved(
            key_kind,
            composite_catalog,
            depth.saturating_add(1),
        ),
        AcceptedFieldKind::Map { key, value } => {
            accepted_kind_composites_are_resolved(key, composite_catalog, depth.saturating_add(1))
                && accepted_kind_composites_are_resolved(
                    value,
                    composite_catalog,
                    depth.saturating_add(1),
                )
        }
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::NatBig { .. }
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::U256 => true,
    }
}
