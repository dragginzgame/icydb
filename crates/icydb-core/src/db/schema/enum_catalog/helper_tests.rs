//! Test-only constructors and adapters for the parent module.

use super::*;

impl AcceptedValueCatalogHandle {
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
}

pub(in crate::db) fn empty_accepted_enum_catalog_for_tests() -> AcceptedEnumCatalog {
    AcceptedEnumCatalog::from_initial_definitions(BTreeMap::new())
        .expect("empty accepted enum catalog is valid")
}

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

pub(super) fn build_catalog_from_definitions(
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

pub(super) fn allocate_variant_ids(
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

pub(super) fn accepted_enum_type_from_proposals(
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

pub(super) fn accepted_enum_type_from_proposal(
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

    Ok(AcceptedEnumType::new(
        path.to_string(),
        variants_by_id,
        candidate_variant_id_by_name,
        EnumOrderingPolicy::EqualityOnly,
    ))
}

pub(super) fn next_type_id(last: Option<EnumTypeId>) -> Result<EnumTypeId, EnumCatalogBuildError> {
    let value = match last {
        Some(last) => last
            .get()
            .checked_add(1)
            .ok_or(EnumCatalogBuildError::EnumTypeIdExhausted)?,
        None => 1,
    };
    EnumTypeId::new(value).ok_or(EnumCatalogBuildError::EnumTypeIdExhausted)
}

pub(super) fn next_variant_id(
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

pub(super) struct RawEnumVariantProposal {
    pub(super) payload_kind: Option<AcceptedFieldKind>,
    pub(super) payload_storage_decode: FieldStorageDecode,
}

pub(super) struct RawEnumDefinitionProposal {
    pub(super) variants: BTreeMap<String, RawEnumVariantProposal>,
}

/// Accepted-native enum definition fixture for catalog codec and boundary tests.
pub(in crate::db) struct TestEnumDefinition {
    pub(super) path: &'static str,
    pub(super) variants: Vec<TestEnumVariant>,
}

impl TestEnumDefinition {
    pub(in crate::db) fn new(path: &'static str, variants: Vec<TestEnumVariant>) -> Self {
        Self { path, variants }
    }
}

/// Accepted-native enum variant fixture.
pub(in crate::db) struct TestEnumVariant {
    pub(super) name: &'static str,
    pub(super) payload: Option<(AcceptedFieldKind, FieldStorageDecode)>,
}

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
