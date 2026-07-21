//! Module: db::schema::enum_catalog
//! Responsibility: canonicalize generated enum proposals into ID-backed catalog candidates.
//! Does not own: durable catalog publication, runtime value admission, or enum key encoding.
//! Boundary: generated entity models -> deterministic accepted enum catalog candidate.

mod admission;
pub(super) mod codec;
mod equality_key;
mod output;
mod publication;
#[cfg(test)]
mod tests;
mod value_wire;

use crate::{
    db::schema::{
        AcceptedFieldKind, MAX_ACCEPTED_RECURSIVE_DEPTH,
        composite_catalog::{AcceptedCompositeCatalog, CompositeTypeId},
    },
    model::{
        entity::EntityModel,
        field::{EnumVariantModel, FieldKind, FieldStorageDecode},
    },
    value::{CanonicalEnumBody, CanonicalEnumValue},
    value::{RuntimeEnumContext, RuntimeEnumSelection},
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

pub(in crate::db) use crate::value::{EnumTypeId, EnumVariantId};
pub(in crate::db) use admission::validate_decoded_persisted_field_value_in_catalog;
pub(in crate::db) use admission::{
    AcceptedValueRef, AdmittedOwnedValue, CanonicalValue, ValueAdmissionBudget,
    ValueAdmissionError, encode_unit_enum_default_in_catalog,
};
pub(in crate::db::schema) use admission::{
    admit_canonical_value, normalize_and_admit_nullable_value, validate_nullable_canonical_value,
    with_normalized_accepted_value,
};
pub(in crate::db::schema) use codec::{decode_accepted_enum_catalog, encode_accepted_enum_catalog};
pub(in crate::db) use equality_key::encode_unit_enum_equality_key;
#[cfg(feature = "sql")]
pub(in crate::db) use equality_key::{EqualityCapability, enum_equality_capability};
pub(in crate::db) use output::output_value_from_runtime;
#[cfg(test)]
pub(in crate::db::schema) use publication::decode_accepted_schema_revision_bundle;
#[cfg(test)]
pub(in crate::db) use publication::empty_accepted_schema_candidate_for_tests;
pub(in crate::db::schema) use publication::{
    AcceptedSchemaBundleKey, AcceptedSchemaPublicationError, AcceptedSchemaRevisionBundle,
    AcceptedSchemaRootSelection, decode_verified_accepted_schema_revision_bundle,
    prepare_accepted_schema_root_publication, select_current_accepted_schema_root,
};
pub(in crate::db) use publication::{
    AcceptedSchemaFingerprint, AcceptedSchemaRevision, CandidateSchemaRevision,
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

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn matches(&self, other: &Self) -> bool {
        self.matches_store_root(&other.store_scope, other.revision, other.fingerprint)
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

impl AcceptedEnumCatalog {
    fn validate(&self) -> bool {
        self.lookup_maps_are_bijective() && self.contract_graph_is_valid()
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

    fn contract_graph_is_valid(&self) -> bool {
        let mut visited = BTreeSet::new();
        let mut active = BTreeSet::new();
        self.by_id
            .keys()
            .copied()
            .all(|type_id| validate_enum_type_graph(self, type_id, &mut visited, &mut active, 0))
    }

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn len(&self) -> usize {
        self.by_id.len()
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

    pub(in crate::db::schema) fn collect_composite_references(
        &self,
        kind: &AcceptedFieldKind,
        references: &mut BTreeSet<CompositeTypeId>,
    ) -> bool {
        collect_composite_type_references(
            self,
            kind,
            references,
            &mut BTreeSet::new(),
            &mut BTreeSet::new(),
            0,
        )
    }
}

impl RuntimeEnumContext for AcceptedEnumCatalog {
    fn resolve_enum<'a>(
        &'a self,
        value: &'a crate::value::ValueEnum,
    ) -> Option<RuntimeEnumSelection<'a>> {
        let definition = self.enum_type(value.type_id())?;
        let variant = definition.variant(value.variant_id())?;
        Some(RuntimeEnumSelection {
            path: definition.path.as_str(),
            variant: variant.name.as_str(),
            payload: value.payload(),
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

    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn ordering(&self) -> EnumOrderingPolicy {
        self.ordering
    }
}

/// One accepted enum variant with structurally valid unit/payload state.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedEnumVariant {
    name: String,
    body: AcceptedEnumVariantBody,
}

impl AcceptedEnumVariant {
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

    pub(in crate::db::schema) fn from_candidate_catalogs(
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
    EmptyTypePath,
    EmptyVariantName { path: String },
    DuplicateVariantName { path: String, name: String },
    ConflictingDefinition { path: String },
    RecursiveEnumContract { cycle: Vec<String> },
    ContractDepthExceeded,
    EnumTypeIdExhausted,
    EnumVariantIdExhausted { path: String },
    ExistingTypeIdentityChanged { path: String },
    ExistingVariantIdentityChanged { path: String, name: String },
    ExistingVariantContractChanged { path: String, name: String },
    UnknownEnumPath { path: String },
    CompositeCatalogRequired { path: String },
    LookupMapInvariant,
}

struct RawEnumVariantProposal {
    payload_kind: Option<FieldKind>,
    payload_storage_decode: FieldStorageDecode,
}

struct RawEnumDefinitionProposal {
    variants: BTreeMap<String, RawEnumVariantProposal>,
}

/// Build one deterministic initial catalog candidate from all generated models
/// belonging to the same store.
#[cfg(test)]
pub(in crate::db) fn build_initial_accepted_enum_catalog(
    models: &[&EntityModel],
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    build_initial_accepted_enum_catalog_with_composite_ids(models, &BTreeMap::new())
}

pub(in crate::db::schema) fn build_initial_accepted_enum_catalog_with_composite_ids(
    models: &[&EntityModel],
    composite_ids: &BTreeMap<String, CompositeTypeId>,
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    build_catalog_from_definitions(collect_enum_definitions_from_models(models)?, composite_ids)
}

pub(in crate::db::schema) fn reconcile_accepted_enum_catalog_with_composite_ids(
    accepted: &AcceptedEnumCatalog,
    models: &[&EntityModel],
    composite_ids: &BTreeMap<String, CompositeTypeId>,
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    reconcile_catalog_from_definitions(
        accepted,
        collect_enum_definitions_from_models(models)?,
        composite_ids,
    )
}

fn collect_enum_definitions_from_models(
    models: &[&EntityModel],
) -> Result<BTreeMap<String, Vec<RawEnumDefinitionProposal>>, EnumCatalogBuildError> {
    let mut definitions = BTreeMap::<String, Vec<RawEnumDefinitionProposal>>::new();
    for model in models {
        for field in model.fields() {
            collect_enum_definitions_from_kind(field.kind(), &mut definitions, &mut Vec::new(), 0)?;
        }
    }

    Ok(definitions)
}

#[cfg(test)]
fn build_initial_accepted_enum_catalog_from_kinds(
    kinds: &[FieldKind],
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    build_initial_accepted_enum_catalog_from_kinds_with_composite_ids(kinds, &BTreeMap::new())
}

#[cfg(test)]
pub(in crate::db::schema) fn build_initial_accepted_enum_catalog_from_kinds_with_composite_ids(
    kinds: &[FieldKind],
    composite_ids: &BTreeMap<String, CompositeTypeId>,
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    let mut definitions = BTreeMap::<String, Vec<RawEnumDefinitionProposal>>::new();
    for kind in kinds {
        collect_enum_definitions_from_kind(*kind, &mut definitions, &mut Vec::new(), 0)?;
    }

    build_catalog_from_definitions(definitions, composite_ids)
}

#[cfg(test)]
pub(in crate::db) fn build_initial_accepted_enum_catalog_from_kinds_for_tests(
    kinds: &[FieldKind],
) -> Result<AcceptedEnumCatalog, ()> {
    build_initial_accepted_enum_catalog_from_kinds(kinds).map_err(|_| ())
}

#[cfg(test)]
fn reconcile_accepted_enum_catalog_from_kinds(
    accepted: &AcceptedEnumCatalog,
    kinds: &[FieldKind],
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    let mut definitions = BTreeMap::<String, Vec<RawEnumDefinitionProposal>>::new();
    for kind in kinds {
        collect_enum_definitions_from_kind(*kind, &mut definitions, &mut Vec::new(), 0)?;
    }

    reconcile_catalog_from_definitions(accepted, definitions, &BTreeMap::new())
}

fn collect_enum_definitions_from_kind(
    kind: FieldKind,
    definitions: &mut BTreeMap<String, Vec<RawEnumDefinitionProposal>>,
    active_paths: &mut Vec<String>,
    depth: usize,
) -> Result<(), EnumCatalogBuildError> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(EnumCatalogBuildError::ContractDepthExceeded);
    }

    match kind {
        FieldKind::Enum { path, variants } => {
            collect_enum_definition(path, variants, definitions, active_paths, depth)?;
        }
        FieldKind::Relation { key_kind, .. }
        | FieldKind::List(key_kind)
        | FieldKind::Set(key_kind) => collect_enum_definitions_from_kind(
            *key_kind,
            definitions,
            active_paths,
            depth.saturating_add(1),
        )?,
        FieldKind::Map { key, value } => {
            collect_enum_definitions_from_kind(
                *key,
                definitions,
                active_paths,
                depth.saturating_add(1),
            )?;
            collect_enum_definitions_from_kind(
                *value,
                definitions,
                active_paths,
                depth.saturating_add(1),
            )?;
        }
        FieldKind::Composite { shape, .. } => match shape {
            crate::model::field::CompositeShapeModel::Record(fields) => {
                for field in *fields {
                    collect_enum_definitions_from_kind(
                        field.kind(),
                        definitions,
                        active_paths,
                        depth.saturating_add(1),
                    )?;
                }
            }
            crate::model::field::CompositeShapeModel::Tuple(elements) => {
                for element in *elements {
                    collect_enum_definitions_from_kind(
                        element.kind(),
                        definitions,
                        active_paths,
                        depth.saturating_add(1),
                    )?;
                }
            }
            crate::model::field::CompositeShapeModel::Newtype(inner) => {
                collect_enum_definitions_from_kind(
                    inner.kind(),
                    definitions,
                    active_paths,
                    depth.saturating_add(1),
                )?;
            }
        },
        FieldKind::Account
        | FieldKind::Blob { .. }
        | FieldKind::Bool
        | FieldKind::Date
        | FieldKind::Decimal { .. }
        | FieldKind::Duration
        | FieldKind::Float32
        | FieldKind::Float64
        | FieldKind::Int8
        | FieldKind::Int16
        | FieldKind::Int32
        | FieldKind::Int64
        | FieldKind::Int128
        | FieldKind::IntBig { .. }
        | FieldKind::Principal
        | FieldKind::Subaccount
        | FieldKind::Text { .. }
        | FieldKind::Timestamp
        | FieldKind::Nat8
        | FieldKind::Nat16
        | FieldKind::Nat32
        | FieldKind::Nat64
        | FieldKind::Nat128
        | FieldKind::NatBig { .. }
        | FieldKind::Ulid
        | FieldKind::Unit => {}
    }

    Ok(())
}

fn collect_enum_definition(
    path: &str,
    variants: &[EnumVariantModel],
    definitions: &mut BTreeMap<String, Vec<RawEnumDefinitionProposal>>,
    active_paths: &mut Vec<String>,
    depth: usize,
) -> Result<(), EnumCatalogBuildError> {
    if path.is_empty() {
        return Err(EnumCatalogBuildError::EmptyTypePath);
    }
    if let Some(cycle_start) = active_paths.iter().position(|active| active == path) {
        let mut cycle = active_paths[cycle_start..].to_vec();
        cycle.push(path.to_string());
        return Err(EnumCatalogBuildError::RecursiveEnumContract { cycle });
    }

    let mut variant_names = BTreeSet::new();
    for variant in variants {
        if variant.ident().is_empty() {
            return Err(EnumCatalogBuildError::EmptyVariantName {
                path: path.to_string(),
            });
        }
        if !variant_names.insert(variant.ident()) {
            return Err(EnumCatalogBuildError::DuplicateVariantName {
                path: path.to_string(),
                name: variant.ident().to_string(),
            });
        }
    }

    active_paths.push(path.to_string());
    for variant in variants {
        if let Some(payload_kind) = variant.payload_kind() {
            collect_enum_definitions_from_kind(
                payload_kind,
                definitions,
                active_paths,
                depth.saturating_add(1),
            )?;
        }
    }
    active_paths.pop();

    let proposal_variants = variants
        .iter()
        .map(|variant| {
            (
                variant.ident().to_string(),
                RawEnumVariantProposal {
                    payload_kind: variant.payload_kind(),
                    payload_storage_decode: variant.payload_storage_decode(),
                },
            )
        })
        .collect();
    definitions
        .entry(path.to_string())
        .or_default()
        .push(RawEnumDefinitionProposal {
            variants: proposal_variants,
        });

    Ok(())
}

fn build_catalog_from_definitions(
    definitions: BTreeMap<String, Vec<RawEnumDefinitionProposal>>,
    composite_ids: &BTreeMap<String, CompositeTypeId>,
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
        let accepted_definition = accepted_enum_type_from_proposals(
            &path,
            proposals,
            &id_by_path,
            composite_ids,
            &variant_ids,
        )?;
        by_id.insert(type_id, accepted_definition);
    }

    let catalog = AcceptedEnumCatalog { by_id, id_by_path };
    if !catalog.validate() {
        return Err(EnumCatalogBuildError::LookupMapInvariant);
    }

    Ok(catalog)
}

fn reconcile_catalog_from_definitions(
    accepted: &AcceptedEnumCatalog,
    definitions: BTreeMap<String, Vec<RawEnumDefinitionProposal>>,
    composite_ids: &BTreeMap<String, CompositeTypeId>,
) -> Result<AcceptedEnumCatalog, EnumCatalogBuildError> {
    if !accepted.validate() {
        return Err(EnumCatalogBuildError::LookupMapInvariant);
    }

    let candidate = build_catalog_from_definitions(definitions, composite_ids)?;
    validate_surviving_enum_identities(accepted, &candidate)?;

    Ok(candidate)
}

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

fn accepted_enum_type_from_proposals(
    path: &str,
    proposals: Vec<RawEnumDefinitionProposal>,
    id_by_path: &BTreeMap<String, EnumTypeId>,
    composite_ids: &BTreeMap<String, CompositeTypeId>,
    variant_id_by_name: &BTreeMap<String, EnumVariantId>,
) -> Result<AcceptedEnumType, EnumCatalogBuildError> {
    let mut accepted_definition = None;
    for proposal in proposals {
        let candidate = accepted_enum_type_from_proposal(
            path,
            proposal,
            id_by_path,
            composite_ids,
            variant_id_by_name,
        )?;
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

fn validate_surviving_enum_identities(
    accepted: &AcceptedEnumCatalog,
    candidate: &AcceptedEnumCatalog,
) -> Result<(), EnumCatalogBuildError> {
    for (path, candidate_type_id) in &candidate.id_by_path {
        let Some(accepted_type_id) = accepted.id_by_path.get(path) else {
            continue;
        };
        if accepted_type_id != candidate_type_id {
            return Err(EnumCatalogBuildError::ExistingTypeIdentityChanged { path: path.clone() });
        }
        let accepted_definition = accepted
            .enum_type(*accepted_type_id)
            .ok_or(EnumCatalogBuildError::LookupMapInvariant)?;
        let candidate_definition = candidate
            .enum_type(*candidate_type_id)
            .ok_or(EnumCatalogBuildError::LookupMapInvariant)?;
        for (name, candidate_variant_id) in &candidate_definition.variant_id_by_name {
            let Some(accepted_variant_id) = accepted_definition.variant_id_by_name.get(name) else {
                continue;
            };
            if accepted_variant_id != candidate_variant_id {
                return Err(EnumCatalogBuildError::ExistingVariantIdentityChanged {
                    path: path.clone(),
                    name: name.clone(),
                });
            }
            if accepted_definition.variant(*accepted_variant_id)
                != candidate_definition.variant(*candidate_variant_id)
            {
                return Err(EnumCatalogBuildError::ExistingVariantContractChanged {
                    path: path.clone(),
                    name: name.clone(),
                });
            }
        }
    }

    Ok(())
}

fn accepted_enum_type_from_proposal(
    path: &str,
    proposal: RawEnumDefinitionProposal,
    id_by_path: &BTreeMap<String, EnumTypeId>,
    composite_ids: &BTreeMap<String, CompositeTypeId>,
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
                    kind: accepted_field_kind_from_model(kind, id_by_path, composite_ids, 0)?,
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

fn accepted_field_kind_from_model(
    kind: FieldKind,
    id_by_path: &BTreeMap<String, EnumTypeId>,
    composite_id_by_path: &BTreeMap<String, CompositeTypeId>,
    depth: usize,
) -> Result<AcceptedFieldKind, EnumCatalogBuildError> {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return Err(EnumCatalogBuildError::ContractDepthExceeded);
    }

    Ok(match kind {
        FieldKind::Account => AcceptedFieldKind::Account,
        FieldKind::Blob { max_len } => AcceptedFieldKind::Blob { max_len },
        FieldKind::Bool => AcceptedFieldKind::Bool,
        FieldKind::Date => AcceptedFieldKind::Date,
        FieldKind::Decimal { scale } => AcceptedFieldKind::Decimal { scale },
        FieldKind::Duration => AcceptedFieldKind::Duration,
        FieldKind::Enum { path, .. } => AcceptedFieldKind::Enum {
            type_id: id_by_path.get(path).copied().ok_or_else(|| {
                EnumCatalogBuildError::UnknownEnumPath {
                    path: path.to_string(),
                }
            })?,
        },
        FieldKind::Float32 => AcceptedFieldKind::Float32,
        FieldKind::Float64 => AcceptedFieldKind::Float64,
        FieldKind::Int8 => AcceptedFieldKind::Int8,
        FieldKind::Int16 => AcceptedFieldKind::Int16,
        FieldKind::Int32 => AcceptedFieldKind::Int32,
        FieldKind::Int64 => AcceptedFieldKind::Int64,
        FieldKind::Int128 => AcceptedFieldKind::Int128,
        FieldKind::IntBig { max_bytes } => AcceptedFieldKind::IntBig { max_bytes },
        FieldKind::Principal => AcceptedFieldKind::Principal,
        FieldKind::Subaccount => AcceptedFieldKind::Subaccount,
        FieldKind::Text { max_len } => AcceptedFieldKind::Text { max_len },
        FieldKind::Timestamp => AcceptedFieldKind::Timestamp,
        FieldKind::Nat8 => AcceptedFieldKind::Nat8,
        FieldKind::Nat16 => AcceptedFieldKind::Nat16,
        FieldKind::Nat32 => AcceptedFieldKind::Nat32,
        FieldKind::Nat64 => AcceptedFieldKind::Nat64,
        FieldKind::Nat128 => AcceptedFieldKind::Nat128,
        FieldKind::NatBig { max_bytes } => AcceptedFieldKind::NatBig { max_bytes },
        FieldKind::Ulid => AcceptedFieldKind::Ulid,
        FieldKind::Unit => AcceptedFieldKind::Unit,
        FieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } => AcceptedFieldKind::Relation {
            target_path: target_path.to_string(),
            target_entity_name: target_entity_name.to_string(),
            target_entity_tag,
            target_store_path: target_store_path.to_string(),
            key_kind: Box::new(accepted_field_kind_from_model(
                *key_kind,
                id_by_path,
                composite_id_by_path,
                depth.saturating_add(1),
            )?),
        },
        FieldKind::List(inner) => {
            AcceptedFieldKind::List(Box::new(accepted_field_kind_from_model(
                *inner,
                id_by_path,
                composite_id_by_path,
                depth.saturating_add(1),
            )?))
        }
        FieldKind::Set(inner) => AcceptedFieldKind::Set(Box::new(accepted_field_kind_from_model(
            *inner,
            id_by_path,
            composite_id_by_path,
            depth.saturating_add(1),
        )?)),
        FieldKind::Map { key, value } => AcceptedFieldKind::Map {
            key: Box::new(accepted_field_kind_from_model(
                *key,
                id_by_path,
                composite_id_by_path,
                depth.saturating_add(1),
            )?),
            value: Box::new(accepted_field_kind_from_model(
                *value,
                id_by_path,
                composite_id_by_path,
                depth.saturating_add(1),
            )?),
        },
        FieldKind::Composite { path, .. } => AcceptedFieldKind::Composite {
            type_id: composite_id_by_path.get(path).copied().ok_or_else(|| {
                EnumCatalogBuildError::CompositeCatalogRequired {
                    path: path.to_string(),
                }
            })?,
        },
    })
}

#[cfg(test)]
pub(in crate::db::schema) fn resolve_model_field_kind(
    catalog: &AcceptedEnumCatalog,
    kind: FieldKind,
) -> Result<AcceptedFieldKind, EnumCatalogBuildError> {
    accepted_field_kind_from_model(kind, &catalog.id_by_path, &BTreeMap::new(), 0)
}

pub(in crate::db::schema) fn resolve_model_field_kind_with_composites(
    catalog: &AcceptedEnumCatalog,
    composite_id_by_path: &BTreeMap<String, CompositeTypeId>,
    kind: FieldKind,
) -> Result<AcceptedFieldKind, EnumCatalogBuildError> {
    accepted_field_kind_from_model(kind, &catalog.id_by_path, composite_id_by_path, 0)
}

pub(in crate::db::schema) fn resolve_model_field_kind_with_composite_catalog(
    catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
    kind: FieldKind,
) -> Result<AcceptedFieldKind, EnumCatalogBuildError> {
    accepted_field_kind_from_model(kind, &catalog.id_by_path, composite_catalog.id_by_path(), 0)
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
        | AcceptedFieldKind::Composite { .. } => true,
    }
}

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

fn validate_enum_type_graph(
    catalog: &AcceptedEnumCatalog,
    type_id: EnumTypeId,
    visited: &mut BTreeSet<EnumTypeId>,
    active: &mut BTreeSet<EnumTypeId>,
    depth: usize,
) -> bool {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return false;
    }
    if visited.contains(&type_id) {
        return true;
    }
    if !active.insert(type_id) {
        return false;
    }
    let Some(definition) = catalog.by_id.get(&type_id) else {
        return false;
    };
    let mut references = BTreeSet::new();
    for variant in definition.variants_by_id.values() {
        if let AcceptedEnumVariantBody::Payload { contract } = &variant.body
            && !collect_enum_type_references(&contract.kind, &mut references, 0)
        {
            return false;
        }
    }
    for referenced_type in references {
        if !catalog.by_id.contains_key(&referenced_type)
            || !validate_enum_type_graph(
                catalog,
                referenced_type,
                visited,
                active,
                depth.saturating_add(1),
            )
        {
            return false;
        }
    }
    active.remove(&type_id);
    visited.insert(type_id);
    true
}

fn collect_enum_type_references(
    kind: &AcceptedFieldKind,
    references: &mut BTreeSet<EnumTypeId>,
    depth: usize,
) -> bool {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return false;
    }
    match kind {
        AcceptedFieldKind::Enum { type_id } => {
            references.insert(*type_id);
        }
        AcceptedFieldKind::Relation { key_kind, .. }
        | AcceptedFieldKind::List(key_kind)
        | AcceptedFieldKind::Set(key_kind) => {
            return collect_enum_type_references(key_kind, references, depth.saturating_add(1));
        }
        AcceptedFieldKind::Map { key, value } => {
            return collect_enum_type_references(key, references, depth.saturating_add(1))
                && collect_enum_type_references(value, references, depth.saturating_add(1));
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
        | AcceptedFieldKind::Composite { .. } => {}
    }
    true
}

fn collect_composite_type_references(
    catalog: &AcceptedEnumCatalog,
    kind: &AcceptedFieldKind,
    references: &mut BTreeSet<CompositeTypeId>,
    visited_enums: &mut BTreeSet<EnumTypeId>,
    active_enums: &mut BTreeSet<EnumTypeId>,
    depth: usize,
) -> bool {
    if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
        return false;
    }
    let nested_depth = depth.saturating_add(1);
    match kind {
        AcceptedFieldKind::Composite { type_id } => {
            references.insert(*type_id);
        }
        AcceptedFieldKind::Enum { type_id } => {
            if visited_enums.contains(type_id) {
                return true;
            }
            if !active_enums.insert(*type_id) {
                return false;
            }
            let Some(definition) = catalog.by_id.get(type_id) else {
                return false;
            };
            for variant in definition.variants_by_id.values() {
                if let AcceptedEnumVariantBody::Payload { contract } = &variant.body
                    && !collect_composite_type_references(
                        catalog,
                        &contract.kind,
                        references,
                        visited_enums,
                        active_enums,
                        nested_depth,
                    )
                {
                    return false;
                }
            }
            active_enums.remove(type_id);
            visited_enums.insert(*type_id);
        }
        AcceptedFieldKind::Relation { key_kind, .. }
        | AcceptedFieldKind::List(key_kind)
        | AcceptedFieldKind::Set(key_kind) => {
            return collect_composite_type_references(
                catalog,
                key_kind,
                references,
                visited_enums,
                active_enums,
                nested_depth,
            );
        }
        AcceptedFieldKind::Map { key, value } => {
            return collect_composite_type_references(
                catalog,
                key,
                references,
                visited_enums,
                active_enums,
                nested_depth,
            ) && collect_composite_type_references(
                catalog,
                value,
                references,
                visited_enums,
                active_enums,
                nested_depth,
            );
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
        | AcceptedFieldKind::Unit => {}
    }
    true
}
