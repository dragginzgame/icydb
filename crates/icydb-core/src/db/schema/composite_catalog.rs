//! Module: db::schema::composite_catalog
//! Responsibility: canonicalize source or generated composite proposals into ID-backed accepted definitions.
//! Does not own: generated codecs, enum definitions, or accepted-schema publication.
//! Boundary: exact source/generated composite shapes -> store-local composite catalog candidate.

mod codec;
use crate::{
    db::schema::{
        AcceptedFieldKind, MAX_ACCEPTED_RECURSIVE_DEPTH, enum_catalog::AcceptedEnumCatalog,
    },
    model::field::CompositeCodec,
};
use std::{
    collections::{BTreeMap, BTreeSet},
    num::NonZeroU32,
};

pub(in crate::db::schema) use codec::{
    decode_accepted_composite_catalog, encode_accepted_composite_catalog,
};

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
/// CompositeFieldId
///
/// Stable non-zero member identity owned by one accepted record composite.
///

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::db) struct CompositeFieldId(NonZeroU32);

impl CompositeFieldId {
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
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn empty() -> Self {
        Self {
            by_id: BTreeMap::new(),
            id_by_path: BTreeMap::new(),
        }
    }

    /// Construct one initial composite catalog from already allocated
    /// store-local identities and canonical shapes.
    pub(in crate::db::schema) fn from_initial_definitions(
        definitions: BTreeMap<CompositeTypeId, (String, AcceptedCompositeShape)>,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<Self, CompositeCatalogBuildError> {
        let mut by_id = BTreeMap::new();
        let mut id_by_path = BTreeMap::new();
        for (type_id, (path, shape)) in definitions {
            if path.is_empty() || id_by_path.insert(path.clone(), type_id).is_some() {
                return Err(CompositeCatalogBuildError::FieldKindResolution);
            }
            by_id.insert(
                type_id,
                AcceptedCompositeType {
                    path,
                    codec: CompositeCodec::StructuralV1,
                    shape,
                },
            );
        }
        let catalog = Self { by_id, id_by_path };
        if !catalog.validate(enum_catalog) {
            return Err(CompositeCatalogBuildError::FieldKindResolution);
        }
        Ok(catalog)
    }

    /// Re-declare an editable composite path under one accepted ID.
    ///
    /// Record member names are part of canonical record values and are not
    /// metadata-only. The full shape and structural codec therefore remain
    /// exact while only the nominal type path changes.
    pub(in crate::db::schema) fn with_redeclared_path(
        mut self,
        type_id: CompositeTypeId,
        path: String,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<Self, CompositeCatalogBuildError> {
        let accepted = self
            .by_id
            .get(&type_id)
            .ok_or(CompositeCatalogBuildError::FieldKindResolution)?;
        if path.is_empty() {
            return Err(CompositeCatalogBuildError::FieldKindResolution);
        }
        let old_path = accepted.path.clone();
        let codec = accepted.codec;
        let shape = accepted.shape.clone();
        self.id_by_path.remove(old_path.as_str());
        if self.id_by_path.insert(path.clone(), type_id).is_some() {
            return Err(CompositeCatalogBuildError::ConflictingDefinition { path });
        }
        self.by_id
            .insert(type_id, AcceptedCompositeType { path, codec, shape });
        if !self.validate(enum_catalog) {
            return Err(CompositeCatalogBuildError::FieldKindResolution);
        }
        Ok(self)
    }

    /// Re-declare one record's editable path and member names.
    ///
    /// Stable member IDs and their accepted contracts must remain exact. The
    /// application boundary separately proves that no persisted row or
    /// derived key can still contain the previous canonical member names.
    pub(in crate::db::schema) fn with_redeclared_record_metadata(
        mut self,
        type_id: CompositeTypeId,
        path: String,
        fields: Vec<AcceptedCompositeField>,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<Self, CompositeCatalogBuildError> {
        if path.is_empty() {
            return Err(CompositeCatalogBuildError::FieldKindResolution);
        }
        let accepted = self
            .by_id
            .get(&type_id)
            .ok_or(CompositeCatalogBuildError::FieldKindResolution)?;
        let AcceptedCompositeShape::Record(accepted_fields) = &accepted.shape else {
            return Err(CompositeCatalogBuildError::ExistingTypeContractChanged { path });
        };
        if accepted_fields.len() != fields.len()
            || accepted_fields.iter().any(|accepted_field| {
                fields.iter().all(|candidate_field| {
                    candidate_field.id != accepted_field.id
                        || candidate_field.contract != accepted_field.contract
                })
            })
        {
            return Err(CompositeCatalogBuildError::ExistingTypeContractChanged { path });
        }

        let old_path = accepted.path.clone();
        let codec = accepted.codec;
        self.id_by_path.remove(old_path.as_str());
        if self.id_by_path.insert(path.clone(), type_id).is_some() {
            return Err(CompositeCatalogBuildError::ConflictingDefinition { path });
        }
        self.by_id.insert(
            type_id,
            AcceptedCompositeType {
                path,
                codec,
                shape: AcceptedCompositeShape::Record(fields),
            },
        );
        if !self.validate(enum_catalog) {
            return Err(CompositeCatalogBuildError::FieldKindResolution);
        }
        Ok(self)
    }

    /// Remove an exact set of accepted composite definitions.
    ///
    /// Validation rejects retained composite definitions that still refer to
    /// a removed identity. Validating after the complete set is absent permits
    /// an unreferenced recursive component to be removed atomically.
    /// Entity-field closure remains owned by the accepted revision bundle.
    pub(in crate::db::schema) fn with_removed_types(
        mut self,
        type_ids: &BTreeSet<CompositeTypeId>,
        enum_catalog: &AcceptedEnumCatalog,
    ) -> Result<Self, CompositeCatalogBuildError> {
        for type_id in type_ids {
            let definition = self
                .by_id
                .remove(type_id)
                .ok_or(CompositeCatalogBuildError::FieldKindResolution)?;
            if self.id_by_path.remove(definition.path.as_str()) != Some(*type_id) {
                return Err(CompositeCatalogBuildError::FieldKindResolution);
            }
        }
        if !self.validate(enum_catalog) {
            return Err(CompositeCatalogBuildError::FieldKindResolution);
        }
        Ok(self)
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

    /// Resolve an exact chain of nominal newtype wrappers to the canonical
    /// value kind observed by row-local accepted checks.
    ///
    /// Records, tuples, missing definitions, and recursive wrapper-only
    /// cycles have no scalar/collection operand contract and return `None`.
    #[must_use]
    pub(in crate::db::schema) fn resolve_newtype_value_kind(
        &self,
        kind: &AcceptedFieldKind,
    ) -> Option<AcceptedFieldKind> {
        let mut current = kind;
        let mut visited = BTreeSet::new();
        loop {
            let AcceptedFieldKind::Composite { type_id } = current else {
                return Some(current.clone());
            };
            if !visited.insert(*type_id) {
                return None;
            }
            let accepted = self.composite_type(*type_id)?;
            let AcceptedCompositeShape::Newtype(inner) = accepted.shape() else {
                return None;
            };
            current = inner.kind();
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn matches_kind(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        kind: &AcceptedFieldKind,
    ) -> bool {
        self.matches_kind_at_depth(enum_catalog, kind, 0)
    }

    fn matches_kind_at_depth(
        &self,
        enum_catalog: &AcceptedEnumCatalog,
        kind: &AcceptedFieldKind,
        depth: usize,
    ) -> bool {
        if depth >= MAX_ACCEPTED_RECURSIVE_DEPTH {
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
            && enum_catalog.composite_references_are_resolved(self)
            && self.id_by_path.iter().all(|(path, type_id)| {
                self.by_id
                    .get(type_id)
                    .is_some_and(|definition| definition.path == *path)
            })
            && self.by_id.iter().all(|(type_id, definition)| {
                self.id_by_path.get(definition.path.as_str()) == Some(type_id)
                    && definition.validate(self, enum_catalog)
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
    pub(in crate::db::schema) const fn path(&self) -> &str {
        self.path.as_str()
    }

    #[must_use]
    pub(in crate::db::schema) const fn codec(&self) -> CompositeCodec {
        self.codec
    }

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
                        && unique_values(fields.iter().map(|field| field.id))
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
    id: CompositeFieldId,
    name: String,
    contract: AcceptedCompositeElement,
}

impl AcceptedCompositeField {
    /// Construct one canonical record member.
    #[must_use]
    pub(in crate::db::schema) const fn new(
        id: CompositeFieldId,
        name: String,
        contract: AcceptedCompositeElement,
    ) -> Self {
        Self { id, name, contract }
    }

    #[must_use]
    pub(in crate::db::schema) const fn id(&self) -> CompositeFieldId {
        self.id
    }

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
    /// Construct one canonical positional or wrapped member.
    #[must_use]
    pub(in crate::db::schema) const fn new(kind: AcceptedFieldKind, nullable: bool) -> Self {
        Self { kind, nullable }
    }

    #[must_use]
    pub(in crate::db::schema) const fn kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    #[must_use]
    pub(in crate::db::schema) const fn nullable(&self) -> bool {
        self.nullable
    }
}

/// Typed rejection while constructing or editing an accepted composite catalog.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum CompositeCatalogBuildError {
    ConflictingDefinition { path: String },
    ExistingTypeContractChanged { path: String },
    FieldKindResolution,
}

fn unique_values<T: Ord>(values: impl Iterator<Item = T>) -> bool {
    let mut seen = BTreeSet::new();
    values.into_iter().all(|value| seen.insert(value))
}
