//! Module: relation::metadata
//! Responsibility: extract and expose canonical relation metadata descriptors from entity models.
//! Does not own: relation validation execution or reverse-index mutation application.
//! Boundary: defines lightweight relation metadata contracts consumed by schema
//! describe, relation validators, and reverse-index maintenance.

use crate::{
    db::{Db, identity::EntityName},
    error::InternalError,
    model::entity::EntityModel,
    model::field::{FieldKind, RelationStrength},
    traits::CanisterKind,
    types::EntityTag,
};

///
/// RelationDescriptorCardinality
///
/// Canonical cardinality for a relation field declared directly or through a
/// supported collection wrapper. Relation-owned descriptor extraction uses this
/// enum so schema describe and relation validation do not separately classify
/// single/list/set relation shapes.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum RelationDescriptorCardinality {
    Single,
    List,
    Set,
}

///
/// RelationDescriptor
///
/// Canonical relation metadata shape extracted from an entity model field.
/// The descriptor is intentionally semantic: it describes relation target
/// identity, key kind, strength, and cardinality without performing save,
/// delete, reverse-index, or storage execution behavior.
///

#[derive(Clone, Copy)]
pub(in crate::db) struct RelationDescriptor<'model> {
    field_index: usize,
    field_name: &'static str,
    field_kind: &'model FieldKind,
    target_path: &'static str,
    target_entity_name: &'static str,
    target_entity_tag: EntityTag,
    target_store_path: &'static str,
    key_kind: &'model FieldKind,
    strength: RelationStrength,
    cardinality: RelationDescriptorCardinality,
}

impl<'model> RelationDescriptor<'model> {
    // Build one canonical relation descriptor from already-classified target
    // metadata and the source field metadata that owns it.
    const fn new(
        field_index: usize,
        field_name: &'static str,
        field_kind: &'model FieldKind,
        target: RelationTargetInfo<'model>,
    ) -> Self {
        Self {
            field_index,
            field_name,
            field_kind,
            target_path: target.path,
            target_entity_name: target.entity_name,
            target_entity_tag: target.entity_tag,
            target_store_path: target.store_path,
            key_kind: target.key_kind,
            strength: target.strength,
            cardinality: target.cardinality,
        }
    }

    /// Return the stable source field index for this relation.
    #[must_use]
    pub(in crate::db) const fn field_index(self) -> usize {
        self.field_index
    }

    /// Return the stable source field name for this relation.
    #[must_use]
    pub(in crate::db) const fn field_name(self) -> &'static str {
        self.field_name
    }

    /// Return the owning source field kind for this relation.
    #[must_use]
    pub(in crate::db) const fn field_kind(self) -> &'model FieldKind {
        self.field_kind
    }

    /// Return the declared target entity path.
    #[must_use]
    pub(in crate::db) const fn target_path(self) -> &'static str {
        self.target_path
    }

    /// Return the declared target entity name.
    #[must_use]
    pub(in crate::db) const fn target_entity_name(self) -> &'static str {
        self.target_entity_name
    }

    /// Return the declared target entity tag.
    #[must_use]
    pub(in crate::db) const fn target_entity_tag(self) -> EntityTag {
        self.target_entity_tag
    }

    /// Return the declared target store path.
    #[must_use]
    pub(in crate::db) const fn target_store_path(self) -> &'static str {
        self.target_store_path
    }

    /// Return the declared target key kind.
    #[must_use]
    pub(in crate::db) const fn key_kind(self) -> &'model FieldKind {
        self.key_kind
    }

    /// Return the declared relation strength.
    #[must_use]
    pub(in crate::db) const fn strength(self) -> RelationStrength {
        self.strength
    }

    /// Return the declared relation cardinality.
    #[must_use]
    pub(in crate::db) const fn cardinality(self) -> RelationDescriptorCardinality {
        self.cardinality
    }
}

///
/// StrongRelationMetadataError
///
/// Error payload emitted when static relation metadata cannot be lowered into
/// the validated target identity consumed by save/delete/reverse-index code.
/// The payload keeps source-field context beside the raw target metadata so
/// callers can classify the failure at their executor-facing boundary.
///

pub(super) struct StrongRelationMetadataError {
    field_name: &'static str,
    target_path: &'static str,
    target_entity_name: &'static str,
    source: crate::db::identity::EntityNameError,
}

impl StrongRelationMetadataError {
    /// Return the source field that declared invalid relation target metadata.
    #[must_use]
    pub(super) const fn field_name(&self) -> &'static str {
        self.field_name
    }

    /// Return the declared target path for the invalid relation metadata.
    #[must_use]
    pub(super) const fn target_path(&self) -> &'static str {
        self.target_path
    }

    /// Return the raw target entity name that failed identity validation.
    #[must_use]
    pub(super) const fn target_entity_name(&self) -> &'static str {
        self.target_entity_name
    }

    /// Return the identity-layer validation error for the target entity name.
    #[must_use]
    pub(super) const fn source(&self) -> &crate::db::identity::EntityNameError {
        &self.source
    }
}

///
/// StrongRelationTargetIdentity
///
/// Validated target identity carried by strong-relation execution paths.
/// It seals the target path, entity name, entity tag, store path, and key kind
/// into one descriptor so relation callers cannot validate one component and
/// then build keys or stores from a drifting component.
///

#[derive(Clone, Copy)]
pub(super) struct StrongRelationTargetIdentity {
    path: &'static str,
    entity_name: EntityName,
    entity_tag: EntityTag,
    store_path: &'static str,
    key_kind: &'static FieldKind,
}

impl StrongRelationTargetIdentity {
    // Build the validated target identity from static field metadata.
    fn try_from_descriptor(
        descriptor: RelationDescriptor<'static>,
    ) -> Result<Self, StrongRelationMetadataError> {
        let entity_name =
            EntityName::try_from_str(descriptor.target_entity_name()).map_err(|source| {
                StrongRelationMetadataError {
                    field_name: descriptor.field_name(),
                    target_path: descriptor.target_path(),
                    target_entity_name: descriptor.target_entity_name(),
                    source,
                }
            })?;

        Ok(Self {
            path: descriptor.target_path(),
            entity_name,
            entity_tag: descriptor.target_entity_tag(),
            store_path: descriptor.target_store_path(),
            key_kind: descriptor.key_kind(),
        })
    }

    /// Return the target entity path declared by the relation.
    #[must_use]
    pub(super) const fn path(self) -> &'static str {
        self.path
    }

    /// Return the canonical target entity name declared by the relation.
    #[must_use]
    pub(super) const fn entity_name(self) -> EntityName {
        self.entity_name
    }

    /// Return the target entity tag used by target raw-key construction.
    #[must_use]
    pub(super) const fn entity_tag(self) -> EntityTag {
        self.entity_tag
    }

    /// Return the target store path used by target-store lookup.
    #[must_use]
    pub(super) const fn store_path(self) -> &'static str {
        self.store_path
    }

    /// Return the target storage-key kind declared by the relation.
    #[must_use]
    pub(super) const fn key_kind(self) -> &'static FieldKind {
        self.key_kind
    }

    /// Validate this target identity against runtime hook metadata when the
    /// current database has enough typed runtime context to perform the check.
    pub(super) fn validate_against_db<C>(
        self,
        db: &Db<C>,
        source_path: &str,
        field_name: &str,
    ) -> Result<(), InternalError>
    where
        C: CanisterKind,
    {
        if !db.has_runtime_hooks() {
            return Ok(());
        }

        let hook = db
            .runtime_hook_for_entity_tag(self.entity_tag)
            .map_err(|err| {
                InternalError::strong_relation_target_identity_mismatch(
                    source_path,
                    field_name,
                    self.path,
                    format!(
                        "target_entity_tag={} is not registered: {err}",
                        self.entity_tag.value()
                    ),
                )
            })?;

        if hook.entity_path != self.path {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path,
                format!(
                    "target_entity_tag={} resolves to entity_path={} but relation declares {}",
                    self.entity_tag.value(),
                    hook.entity_path,
                    self.path,
                ),
            ));
        }

        if hook.model.name() != self.entity_name.as_str() {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path,
                format!(
                    "target_entity_tag={} resolves to entity_name={} but relation declares {}",
                    self.entity_tag.value(),
                    hook.model.name(),
                    self.entity_name.as_str(),
                ),
            ));
        }

        if hook.store_path != self.store_path {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path,
                format!(
                    "target_store_path={} does not match runtime store {} for target_entity_tag={}",
                    self.store_path,
                    hook.store_path,
                    self.entity_tag.value(),
                ),
            ));
        }

        Ok(())
    }
}

///
/// StrongRelationInfo
///
/// Lightweight relation descriptor extracted from runtime field metadata.
///

#[derive(Clone, Copy)]
pub(super) struct StrongRelationInfo {
    pub(super) field_index: usize,
    pub(super) field_name: &'static str,
    pub(super) field_kind: &'static FieldKind,
    target: StrongRelationTargetIdentity,
}

impl StrongRelationInfo {
    /// Return the sealed target identity for this strong relation.
    #[must_use]
    pub(super) const fn target(self) -> StrongRelationTargetIdentity {
        self.target
    }

    /// Validate this relation target identity against runtime hook metadata.
    pub(super) fn validate_target_identity<C>(
        self,
        db: &Db<C>,
        source_path: &str,
    ) -> Result<(), InternalError>
    where
        C: CanisterKind,
    {
        self.target
            .validate_against_db(db, source_path, self.field_name)
    }
}

///
/// StrongRelationTargetInfo
///
/// Raw target descriptor for relation fields before canonical descriptor
/// construction. It exists only to keep the `FieldKind` pattern match small
/// and lets the canonical public-in-db descriptor remain the only exported
/// relation metadata shape.
///

#[derive(Clone, Copy)]
struct RelationTargetInfo<'model> {
    path: &'static str,
    entity_name: &'static str,
    entity_tag: EntityTag,
    store_path: &'static str,
    key_kind: &'model FieldKind,
    strength: RelationStrength,
    cardinality: RelationDescriptorCardinality,
}

/// Resolve a model field-kind into strong relation target metadata (if applicable).
const fn relation_target_from_kind(kind: &FieldKind) -> Option<RelationTargetInfo<'_>> {
    match kind {
        FieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
            strength,
            ..
        } => Some(RelationTargetInfo {
            path: target_path,
            entity_name: target_entity_name,
            entity_tag: *target_entity_tag,
            store_path: target_store_path,
            key_kind,
            strength: *strength,
            cardinality: RelationDescriptorCardinality::Single,
        }),
        FieldKind::List(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
            strength,
            ..
        }) => Some(RelationTargetInfo {
            path: target_path,
            entity_name: target_entity_name,
            entity_tag: *target_entity_tag,
            store_path: target_store_path,
            key_kind,
            strength: *strength,
            cardinality: RelationDescriptorCardinality::List,
        }),
        FieldKind::Set(FieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
            strength,
            ..
        }) => Some(RelationTargetInfo {
            path: target_path,
            entity_name: target_entity_name,
            entity_tag: *target_entity_tag,
            store_path: target_store_path,
            key_kind,
            strength: *strength,
            cardinality: RelationDescriptorCardinality::Set,
        }),
        _ => None,
    }
}

/// Resolve a model field into canonical relation metadata (if applicable).
const fn relation_descriptor_from_field<'model>(
    field_index: usize,
    field_name: &'static str,
    kind: &'model FieldKind,
) -> Option<RelationDescriptor<'model>> {
    let Some(target) = relation_target_from_kind(kind) else {
        return None;
    };

    Some(RelationDescriptor::new(
        field_index,
        field_name,
        kind,
        target,
    ))
}

/// Resolve canonical relation descriptors for one source model.
pub(in crate::db) fn relation_descriptors_for_model_iter(
    model: &EntityModel,
) -> impl Iterator<Item = RelationDescriptor<'_>> + '_ {
    model
        .fields
        .iter()
        .enumerate()
        .filter_map(|(field_index, field)| {
            relation_descriptor_from_field(field_index, field.name, &field.kind)
        })
}

/// Resolve a strong relation descriptor into validated relation metadata.
fn strong_relation_from_descriptor(
    descriptor: RelationDescriptor<'static>,
) -> Result<Option<StrongRelationInfo>, StrongRelationMetadataError> {
    if descriptor.strength() != RelationStrength::Strong {
        return Ok(None);
    }

    Ok(Some(StrongRelationInfo {
        field_index: descriptor.field_index(),
        field_name: descriptor.field_name(),
        field_kind: descriptor.field_kind(),
        target: StrongRelationTargetIdentity::try_from_descriptor(descriptor)?,
    }))
}

/// Resolve strong relation descriptors for one source model, optionally filtered by target path.
pub(super) fn strong_relations_for_model_iter<'a>(
    model: &'static EntityModel,
    target_path_filter: Option<&'a str>,
) -> impl Iterator<Item = Result<StrongRelationInfo, StrongRelationMetadataError>> + 'a {
    relation_descriptors_for_model_iter(model)
        .filter_map(move |descriptor| {
            if descriptor.strength() != RelationStrength::Strong {
                return None;
            }
            if target_path_filter.is_some_and(|target_path| descriptor.target_path() != target_path)
            {
                return None;
            }

            Some(strong_relation_from_descriptor(descriptor))
        })
        .map(|relation| relation.map(|relation| relation.expect("strong relation filtered")))
}

impl EntityModel {
    /// Return `true` when this model declares any strong relation field.
    #[must_use]
    pub(in crate::db) fn has_any_strong_relations(&'static self) -> bool {
        relation_descriptors_for_model_iter(self)
            .any(|descriptor| descriptor.strength() == RelationStrength::Strong)
    }
}

/// Return `true` when one source model declares any strong relation to the
/// target entity path under delete-side validation.
pub(in crate::db) fn model_has_strong_relations_to_target(
    model: &'static EntityModel,
    target_path: &str,
) -> bool {
    relation_descriptors_for_model_iter(model).any(|descriptor| {
        descriptor.strength() == RelationStrength::Strong && descriptor.target_path() == target_path
    })
}
