//! Module: db::schema::composite_catalog
//! Responsibility: canonicalize generated composite proposals into ID-backed accepted definitions.
//! Does not own: generated codecs, enum definitions, or accepted-schema publication.
//! Boundary: exact generated record/tuple/newtype shapes -> store-local composite catalog candidate.

mod codec;
#[cfg(test)]
mod tests;

use crate::{
    db::schema::{AcceptedFieldKind, enum_catalog::AcceptedEnumCatalog},
    model::{
        entity::EntityModel,
        field::{CompositeCodec, CompositeShapeModel, EnumVariantModel, FieldKind},
    },
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
};

pub(in crate::db::schema) use codec::{
    decode_accepted_composite_catalog, encode_accepted_composite_catalog,
};

const MAX_COMPOSITE_CONTRACT_DEPTH: usize = 64;

///
/// CompositeTypeId
///
/// Stable non-zero identity owned by one store-local accepted composite
/// catalog.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct CompositeTypeId(NonZeroU32);

impl CompositeTypeId {
    #[must_use]
    pub(in crate::db) const fn new(value: u32) -> Option<Self> {
        match NonZeroU32::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    #[must_use]
    pub(in crate::db) const fn get(self) -> u32 {
        self.0.get()
    }
}

///
/// AcceptedCompositeCatalog
///
/// Canonical nominal composite definitions owned by one accepted store
/// revision.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCompositeCatalog {
    by_id: BTreeMap<CompositeTypeId, AcceptedCompositeType>,
    id_by_path: BTreeMap<String, CompositeTypeId>,
}

impl AcceptedCompositeCatalog {
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn empty() -> Self {
        Self {
            by_id: BTreeMap::new(),
            id_by_path: BTreeMap::new(),
        }
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) fn type_id(&self, path: &str) -> Option<CompositeTypeId> {
        self.id_by_path.get(path).copied()
    }

    #[must_use]
    pub(in crate::db::schema) const fn id_by_path(&self) -> &BTreeMap<String, CompositeTypeId> {
        &self.id_by_path
    }

    #[must_use]
    pub(in crate::db::schema) fn composite_type(
        &self,
        id: CompositeTypeId,
    ) -> Option<&AcceptedCompositeType> {
        self.by_id.get(&id)
    }

    #[must_use]
    pub(in crate::db::schema) fn matches_kind(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        kind: &AcceptedFieldKind,
    ) -> bool {
        self.matches_kind_at_depth(enum_catalog, kind, 0)
    }

    #[must_use]
    pub(in crate::db::schema) fn matches_generated_composite(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        type_id: CompositeTypeId,
        path: &str,
        codec: CompositeCodec,
        shape: &CompositeShapeModel,
    ) -> bool {
        self.by_id.get(&type_id).is_some_and(|definition| {
            definition.path == path
                && definition.codec == codec
                && definition.matches_generated_shape(self, enum_catalog, shape)
        })
    }

    fn matches_kind_at_depth(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        kind: &AcceptedFieldKind,
        depth: usize,
    ) -> bool {
        if depth > MAX_COMPOSITE_CONTRACT_DEPTH {
            return false;
        }
        let nested_depth = depth.saturating_add(1);
        match kind {
            AcceptedFieldKind::Composite { type_id } => self.by_id.contains_key(type_id),
            AcceptedFieldKind::Relation { key_kind, .. }
            | AcceptedFieldKind::List(key_kind)
            | AcceptedFieldKind::Set(key_kind) => {
                self.matches_kind_at_depth(enum_catalog, key_kind, nested_depth)
            }
            AcceptedFieldKind::Map { key, value } => {
                self.matches_kind_at_depth(enum_catalog, key, nested_depth)
                    && self.matches_kind_at_depth(enum_catalog, value, nested_depth)
            }
            _ => enum_catalog.matches_accepted_kind(kind),
        }
    }

    pub(in crate::db::schema) fn validate(&self, enum_catalog: &AcceptedEnumCatalog) -> bool {
        self.by_id.len() == self.id_by_path.len()
            && self.id_by_path.iter().all(|(path, type_id)| {
                self.by_id
                    .get(type_id)
                    .is_some_and(|definition| definition.path == *path)
            })
            && self.by_id.iter().all(|(type_id, definition)| {
                self.id_by_path.get(definition.path.as_str()) == Some(type_id)
                    && definition.validate(self, enum_catalog)
            })
            && self.contract_graph_is_acyclic(enum_catalog)
    }

    fn contract_graph_is_acyclic(&self, enum_catalog: &AcceptedEnumCatalog) -> bool {
        let mut visited = BTreeSet::new();
        let mut active = BTreeSet::new();
        self.by_id.keys().copied().all(|type_id| {
            validate_composite_type_graph(self, enum_catalog, type_id, &mut visited, &mut active, 0)
        })
    }
}

///
/// AcceptedCompositeType
///
/// One exact nominal composite definition owned by accepted schema.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct AcceptedCompositeType {
    path: String,
    codec: CompositeCodec,
    shape: AcceptedCompositeShape,
}

impl AcceptedCompositeType {
    #[must_use]
    pub(in crate::db::schema) const fn shape(&self) -> &AcceptedCompositeShape {
        &self.shape
    }

    fn validate(
        &self,
        composite_catalog: &AcceptedCompositeCatalog,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> bool {
        !self.path.is_empty()
            && match &self.shape {
                AcceptedCompositeShape::Record(fields) => {
                    fields.windows(2).all(|pair| pair[0].name < pair[1].name)
                        && fields.iter().all(|field| {
                            !field.name.is_empty()
                                && composite_catalog
                                    .matches_kind(enum_catalog, &field.contract.kind)
                        })
                }
                AcceptedCompositeShape::Tuple(elements) => elements
                    .iter()
                    .all(|element| composite_catalog.matches_kind(enum_catalog, &element.kind)),
                AcceptedCompositeShape::Newtype(inner) => {
                    composite_catalog.matches_kind(enum_catalog, &inner.kind)
                }
            }
    }

    fn matches_generated_shape(
        &self,
        composite_catalog: &AcceptedCompositeCatalog,
        enum_catalog: &AcceptedEnumCatalog,
        generated: &CompositeShapeModel,
    ) -> bool {
        match (&self.shape, generated) {
            (AcceptedCompositeShape::Record(accepted), CompositeShapeModel::Record(generated)) => {
                accepted.len() == generated.len()
                    && accepted.iter().all(|accepted_field| {
                        generated
                            .iter()
                            .find(|field| field.name() == accepted_field.name)
                            .is_some_and(|generated_field| {
                                accepted_field.contract.nullable == generated_field.nullable()
                                    && accepted_field
                                        .contract
                                        .kind
                                        .matches_generated_storage_shape(
                                            generated_field.kind(),
                                            enum_catalog,
                                            composite_catalog,
                                        )
                            })
                    })
            }
            (AcceptedCompositeShape::Tuple(accepted), CompositeShapeModel::Tuple(generated)) => {
                accepted.len() == generated.len()
                    && accepted
                        .iter()
                        .zip(*generated)
                        .all(|(accepted, generated)| {
                            accepted.nullable == generated.nullable()
                                && accepted.kind.matches_generated_storage_shape(
                                    generated.kind(),
                                    enum_catalog,
                                    composite_catalog,
                                )
                        })
            }
            (
                AcceptedCompositeShape::Newtype(accepted),
                CompositeShapeModel::Newtype(generated),
            ) => {
                accepted.nullable == generated.nullable()
                    && accepted.kind.matches_generated_storage_shape(
                        generated.kind(),
                        enum_catalog,
                        composite_catalog,
                    )
            }
            _ => false,
        }
    }
}

///
/// AcceptedCompositeShape
///
/// Exact member layout owned by one accepted nominal composite definition.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum AcceptedCompositeShape {
    /// Named members in canonical field-name order.
    Record(Vec<AcceptedCompositeField>),
    /// Positional members in declaration order.
    Tuple(Vec<AcceptedCompositeElement>),
    /// One nominally wrapped member.
    Newtype(AcceptedCompositeElement),
}

///
/// AcceptedCompositeField
///
/// One named record member and its inseparable accepted value contract.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct AcceptedCompositeField {
    name: String,
    contract: AcceptedCompositeElement,
}

impl AcceptedCompositeField {
    #[must_use]
    pub(in crate::db::schema) fn name(&self) -> &str {
        &self.name
    }

    #[must_use]
    pub(in crate::db::schema) const fn contract(&self) -> &AcceptedCompositeElement {
        &self.contract
    }
}

///
/// AcceptedCompositeElement
///
/// One positional payload kind and its accepted explicit-null policy.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct AcceptedCompositeElement {
    kind: AcceptedFieldKind,
    nullable: bool,
}

impl AcceptedCompositeElement {
    #[must_use]
    pub(in crate::db::schema) const fn kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    #[must_use]
    pub(in crate::db::schema) const fn nullable(&self) -> bool {
        self.nullable
    }
}

///
/// CompositeCatalogBuildError
///
/// Typed rejection owned by accepted composite proposal canonicalization.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum CompositeCatalogBuildError {
    ConflictingDefinition { path: String },
    ContractDepthExceeded,

    DuplicateRecordField { path: String, name: String },

    EmptyTypePath,

    ExistingTypeContractChanged { path: String },

    ExistingTypeIdentityChanged { path: String },

    FieldKindResolution,

    RecursiveContract { cycle: Vec<String> },

    TypeIdExhausted,
}

#[derive(Clone, Copy)]
struct RawCompositeDefinitionProposal {
    codec: CompositeCodec,
    shape: &'static CompositeShapeModel,
}

/// Build the initial accepted composite authority from generated proposals.
pub(in crate::db::schema) fn build_initial_accepted_composite_catalog(
    models: &[&EntityModel],
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<AcceptedCompositeCatalog, CompositeCatalogBuildError> {
    let definitions = collect_composite_definitions_from_models(models)?;
    build_catalog_from_definitions(definitions, enum_catalog)
}

/// Allocate deterministic store-local IDs for generated composite paths.
pub(in crate::db::schema) fn generated_composite_type_ids(
    models: &[&EntityModel],
) -> Result<BTreeMap<String, CompositeTypeId>, CompositeCatalogBuildError> {
    let definitions = collect_composite_definitions_from_models(models)?;
    allocate_type_ids(&definitions)
}

#[cfg(test)]
pub(in crate::db) fn build_initial_accepted_catalogs_for_tests(
    models: &[&EntityModel],
) -> Result<(AcceptedEnumCatalog, AcceptedCompositeCatalog), ()> {
    let composite_ids = generated_composite_type_ids(models).map_err(|_| ())?;
    let enum_catalog = super::enum_catalog::build_initial_accepted_enum_catalog_with_composite_ids(
        models,
        &composite_ids,
    )
    .map_err(|_| ())?;
    let composite_catalog =
        build_initial_accepted_composite_catalog(models, &enum_catalog).map_err(|_| ())?;
    Ok((enum_catalog, composite_catalog))
}

#[cfg(test)]
pub(in crate::db) fn build_initial_accepted_catalogs_from_kinds_for_tests(
    kinds: &[FieldKind],
) -> Result<(AcceptedEnumCatalog, AcceptedCompositeCatalog), ()> {
    let definitions = collect_composite_definitions_from_kinds(kinds).map_err(|_| ())?;
    let composite_ids = allocate_type_ids(&definitions).map_err(|_| ())?;
    let enum_catalog =
        super::enum_catalog::build_initial_accepted_enum_catalog_from_kinds_with_composite_ids(
            kinds,
            &composite_ids,
        )
        .map_err(|_| ())?;
    let composite_catalog =
        build_catalog_from_definitions(definitions, &enum_catalog).map_err(|_| ())?;
    Ok((enum_catalog, composite_catalog))
}

/// Reconcile proposals without changing an existing nominal identity or shape.
pub(in crate::db::schema) fn reconcile_accepted_composite_catalog(
    accepted: &AcceptedCompositeCatalog,
    models: &[&EntityModel],
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<AcceptedCompositeCatalog, CompositeCatalogBuildError> {
    let candidate = build_initial_accepted_composite_catalog(models, enum_catalog)?;
    for (path, accepted_id) in &accepted.id_by_path {
        let Some(candidate_id) = candidate.id_by_path.get(path) else {
            continue;
        };
        if candidate_id != accepted_id {
            return Err(CompositeCatalogBuildError::ExistingTypeIdentityChanged {
                path: path.clone(),
            });
        }
        if candidate.by_id.get(candidate_id) != accepted.by_id.get(accepted_id) {
            return Err(CompositeCatalogBuildError::ExistingTypeContractChanged {
                path: path.clone(),
            });
        }
    }
    Ok(candidate)
}

fn collect_composite_definitions_from_models(
    models: &[&EntityModel],
) -> Result<BTreeMap<String, Vec<RawCompositeDefinitionProposal>>, CompositeCatalogBuildError> {
    let mut definitions = BTreeMap::new();
    for model in models {
        for field in model.fields() {
            collect_composite_definitions_from_kind(
                field.kind(),
                &mut definitions,
                &mut Vec::new(),
                0,
            )?;
        }
    }
    Ok(definitions)
}

#[cfg(test)]
fn collect_composite_definitions_from_kinds(
    kinds: &[FieldKind],
) -> Result<BTreeMap<String, Vec<RawCompositeDefinitionProposal>>, CompositeCatalogBuildError> {
    let mut definitions = BTreeMap::new();
    for kind in kinds {
        collect_composite_definitions_from_kind(*kind, &mut definitions, &mut Vec::new(), 0)?;
    }
    Ok(definitions)
}

fn collect_composite_definitions_from_kind(
    kind: FieldKind,
    definitions: &mut BTreeMap<String, Vec<RawCompositeDefinitionProposal>>,
    active_paths: &mut Vec<String>,
    depth: usize,
) -> Result<(), CompositeCatalogBuildError> {
    if depth > MAX_COMPOSITE_CONTRACT_DEPTH {
        return Err(CompositeCatalogBuildError::ContractDepthExceeded);
    }
    let nested_depth = depth.saturating_add(1);
    match kind {
        FieldKind::Composite { path, codec, shape } => {
            if path.is_empty() {
                return Err(CompositeCatalogBuildError::EmptyTypePath);
            }
            if let Some(cycle_start) = active_paths.iter().position(|active| active == path) {
                let mut cycle = active_paths[cycle_start..].to_vec();
                cycle.push(path.to_string());
                return Err(CompositeCatalogBuildError::RecursiveContract { cycle });
            }
            definitions
                .entry(path.to_string())
                .or_default()
                .push(RawCompositeDefinitionProposal { codec, shape });
            active_paths.push(path.to_string());
            match shape {
                CompositeShapeModel::Record(fields) => {
                    for field in *fields {
                        collect_composite_definitions_from_kind(
                            field.kind(),
                            definitions,
                            active_paths,
                            nested_depth,
                        )?;
                    }
                }
                CompositeShapeModel::Tuple(elements) => {
                    for element in *elements {
                        collect_composite_definitions_from_kind(
                            element.kind(),
                            definitions,
                            active_paths,
                            nested_depth,
                        )?;
                    }
                }
                CompositeShapeModel::Newtype(inner) => {
                    collect_composite_definitions_from_kind(
                        inner.kind(),
                        definitions,
                        active_paths,
                        nested_depth,
                    )?;
                }
            }
            active_paths.pop();
        }
        FieldKind::Enum { variants, .. } => {
            collect_composites_from_enum_variants(
                variants,
                definitions,
                active_paths,
                nested_depth,
            )?;
        }
        FieldKind::Relation { key_kind, .. }
        | FieldKind::List(key_kind)
        | FieldKind::Set(key_kind) => collect_composite_definitions_from_kind(
            *key_kind,
            definitions,
            active_paths,
            nested_depth,
        )?,
        FieldKind::Map { key, value } => {
            collect_composite_definitions_from_kind(*key, definitions, active_paths, nested_depth)?;
            collect_composite_definitions_from_kind(
                *value,
                definitions,
                active_paths,
                nested_depth,
            )?;
        }
        _ => {}
    }
    Ok(())
}

fn collect_composites_from_enum_variants(
    variants: &[EnumVariantModel],
    definitions: &mut BTreeMap<String, Vec<RawCompositeDefinitionProposal>>,
    active_paths: &mut Vec<String>,
    depth: usize,
) -> Result<(), CompositeCatalogBuildError> {
    for variant in variants {
        if let Some(kind) = variant.payload_kind() {
            collect_composite_definitions_from_kind(kind, definitions, active_paths, depth)?;
        }
    }
    Ok(())
}

fn build_catalog_from_definitions(
    definitions: BTreeMap<String, Vec<RawCompositeDefinitionProposal>>,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<AcceptedCompositeCatalog, CompositeCatalogBuildError> {
    let id_by_path = allocate_type_ids(&definitions)?;

    let mut by_id = BTreeMap::new();
    for (path, proposals) in definitions {
        let type_id = id_by_path
            .get(&path)
            .copied()
            .ok_or(CompositeCatalogBuildError::FieldKindResolution)?;
        let mut accepted_definition = None;
        for proposal in proposals {
            let candidate =
                accepted_definition_from_model(path.clone(), proposal, enum_catalog, &id_by_path)?;
            if accepted_definition
                .as_ref()
                .is_some_and(|accepted| accepted != &candidate)
            {
                return Err(CompositeCatalogBuildError::ConflictingDefinition { path });
            }
            accepted_definition = Some(candidate);
        }
        let definition =
            accepted_definition.ok_or(CompositeCatalogBuildError::FieldKindResolution)?;
        by_id.insert(type_id, definition);
    }

    let catalog = AcceptedCompositeCatalog { by_id, id_by_path };
    if !catalog.validate(enum_catalog) {
        return Err(CompositeCatalogBuildError::FieldKindResolution);
    }
    Ok(catalog)
}

fn allocate_type_ids(
    definitions: &BTreeMap<String, Vec<RawCompositeDefinitionProposal>>,
) -> Result<BTreeMap<String, CompositeTypeId>, CompositeCatalogBuildError> {
    let mut id_by_path = BTreeMap::new();
    for (index, path) in definitions.keys().enumerate() {
        let value = u32::try_from(index)
            .ok()
            .and_then(|index| index.checked_add(1))
            .ok_or(CompositeCatalogBuildError::TypeIdExhausted)?;
        let id = CompositeTypeId::new(value).ok_or(CompositeCatalogBuildError::TypeIdExhausted)?;
        id_by_path.insert(path.clone(), id);
    }
    Ok(id_by_path)
}

fn accepted_definition_from_model(
    path: String,
    proposal: RawCompositeDefinitionProposal,
    enum_catalog: &AcceptedEnumCatalog,
    composite_ids: &BTreeMap<String, CompositeTypeId>,
) -> Result<AcceptedCompositeType, CompositeCatalogBuildError> {
    let shape = match proposal.shape {
        CompositeShapeModel::Record(fields) => {
            let mut accepted = fields
                .iter()
                .map(|field| {
                    Ok(AcceptedCompositeField {
                        name: field.name().to_string(),
                        contract: AcceptedCompositeElement {
                            kind: super::enum_catalog::resolve_model_field_kind_with_composites(
                                enum_catalog,
                                composite_ids,
                                field.kind(),
                            )
                            .map_err(|_| CompositeCatalogBuildError::FieldKindResolution)?,
                            nullable: field.nullable(),
                        },
                    })
                })
                .collect::<Result<Vec<_>, CompositeCatalogBuildError>>()?;
            accepted.sort_by(|left, right| left.name.cmp(&right.name));
            if let Some(pair) = accepted
                .windows(2)
                .find(|pair| pair[0].name == pair[1].name)
            {
                return Err(CompositeCatalogBuildError::DuplicateRecordField {
                    path,
                    name: pair[0].name.clone(),
                });
            }
            AcceptedCompositeShape::Record(accepted)
        }
        CompositeShapeModel::Tuple(elements) => AcceptedCompositeShape::Tuple(
            elements
                .iter()
                .map(|element| {
                    Ok(AcceptedCompositeElement {
                        kind: super::enum_catalog::resolve_model_field_kind_with_composites(
                            enum_catalog,
                            composite_ids,
                            element.kind(),
                        )
                        .map_err(|_| CompositeCatalogBuildError::FieldKindResolution)?,
                        nullable: element.nullable(),
                    })
                })
                .collect::<Result<Vec<_>, CompositeCatalogBuildError>>()?,
        ),
        CompositeShapeModel::Newtype(inner) => {
            AcceptedCompositeShape::Newtype(AcceptedCompositeElement {
                kind: super::enum_catalog::resolve_model_field_kind_with_composites(
                    enum_catalog,
                    composite_ids,
                    inner.kind(),
                )
                .map_err(|_| CompositeCatalogBuildError::FieldKindResolution)?,
                nullable: inner.nullable(),
            })
        }
    };
    Ok(AcceptedCompositeType {
        path,
        codec: proposal.codec,
        shape,
    })
}

fn validate_composite_type_graph(
    composite_catalog: &AcceptedCompositeCatalog,
    enum_catalog: &AcceptedEnumCatalog,
    type_id: CompositeTypeId,
    visited: &mut BTreeSet<CompositeTypeId>,
    active: &mut BTreeSet<CompositeTypeId>,
    depth: usize,
) -> bool {
    if depth > MAX_COMPOSITE_CONTRACT_DEPTH {
        return false;
    }
    if visited.contains(&type_id) {
        return true;
    }
    if !active.insert(type_id) {
        return false;
    }
    let Some(definition) = composite_catalog.by_id.get(&type_id) else {
        return false;
    };
    let mut references = BTreeSet::new();
    let valid_shape = match &definition.shape {
        AcceptedCompositeShape::Record(fields) => fields.iter().all(|field| {
            enum_catalog.collect_composite_references(&field.contract.kind, &mut references)
        }),
        AcceptedCompositeShape::Tuple(elements) => elements.iter().all(|element| {
            enum_catalog.collect_composite_references(&element.kind, &mut references)
        }),
        AcceptedCompositeShape::Newtype(inner) => {
            enum_catalog.collect_composite_references(&inner.kind, &mut references)
        }
    };
    if !valid_shape {
        return false;
    }
    for referenced_type in references {
        if !composite_catalog.by_id.contains_key(&referenced_type)
            || !validate_composite_type_graph(
                composite_catalog,
                enum_catalog,
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
