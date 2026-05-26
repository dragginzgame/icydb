//! Module: relation
//! Responsibility: relation-domain validation and reverse-index mutation helpers.
//! Does not own: query planning, executor routing, or storage codec policy.
//! Boundary: executor/commit paths delegate relation semantics to this module.

mod metadata;
mod reverse_index;
mod save_validate;
mod validate;

use crate::{
    db::{
        Db,
        data::RawDataStoreKey,
        identity::EntityName,
        schema::{PersistedFieldKind, PersistedRelationStrength},
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
    value::Value,
};
use std::{collections::BTreeSet, fmt::Display};

pub(in crate::db) use metadata::{
    RelationDescriptor, RelationDescriptorCardinality, relation_descriptors_for_model_iter,
};
pub(crate) use reverse_index::{
    ReverseRelationSourceInfo, prepare_reverse_relation_index_mutations_for_source_slot_readers,
};
pub(in crate::db) use save_validate::validate_save_strong_relations_with_accepted_contract;
pub(in crate::db) use validate::validate_delete_strong_relations_for_source;

///
/// StrongRelationDeleteValidateFn
///
/// Function-pointer contract for delete-side strong relation validators.
///

pub(crate) type StrongRelationDeleteValidateFn<C> =
    fn(&Db<C>, &str, &BTreeSet<RawDataStoreKey>) -> Result<(), InternalError>;

///
/// RelationTargetDecodeContext
/// Call-site context labels for relation target key decode diagnostics.
///

#[derive(Clone, Copy, Debug)]
enum RelationTargetDecodeContext {
    DeleteValidation,
    ReverseIndexPrepare,
}

///
/// RelationTargetMismatchPolicy
/// Defines whether relation target entity mismatches are skipped or rejected.
///

#[derive(Clone, Copy, Debug)]
enum RelationTargetMismatchPolicy {
    Skip,
    Reject,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum AcceptedRelationCardinality {
    Single,
    List,
    Set,
}

///
/// AcceptedRelationTargetDescriptor
///
/// Accepted-schema relation target metadata projected from a relation field
/// or a supported collection wrapper. This is intentionally field-shape
/// metadata only; save validation and reverse-index preparation add their
/// own execution-specific source slot context.
///

#[derive(Clone, Copy)]
struct AcceptedRelationTargetDescriptor<'a> {
    target_path: &'a str,
    target_entity_name: &'a str,
    target_entity_tag: EntityTag,
    target_store_path: &'a str,
    target_key_kind: &'a PersistedFieldKind,
    strength: PersistedRelationStrength,
    cardinality: AcceptedRelationCardinality,
}

fn accepted_relation_target_descriptor_from_kind(
    kind: &PersistedFieldKind,
) -> Option<AcceptedRelationTargetDescriptor<'_>> {
    fn relation_target(
        kind: &PersistedFieldKind,
        cardinality: AcceptedRelationCardinality,
    ) -> Option<AcceptedRelationTargetDescriptor<'_>> {
        let PersistedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
            strength,
        } = kind
        else {
            return None;
        };

        Some(AcceptedRelationTargetDescriptor {
            target_path,
            target_entity_name,
            target_entity_tag: *target_entity_tag,
            target_store_path,
            target_key_kind: key_kind.as_ref(),
            strength: *strength,
            cardinality,
        })
    }

    match kind {
        PersistedFieldKind::Relation { .. } => {
            relation_target(kind, AcceptedRelationCardinality::Single)
        }
        PersistedFieldKind::List(inner) | PersistedFieldKind::Set(inner) => {
            let cardinality = match kind {
                PersistedFieldKind::List(_) => AcceptedRelationCardinality::List,
                PersistedFieldKind::Set(_) => AcceptedRelationCardinality::Set,
                _ => unreachable!("outer relation collection shape was already matched"),
            };

            relation_target(inner.as_ref(), cardinality)
        }
        _ => None,
    }
}

#[derive(Clone, Debug)]
struct AcceptedRelationTargetAuthority {
    path: String,
    entity_name: EntityName,
    entity_tag: EntityTag,
    store_path: String,
}

impl AcceptedRelationTargetAuthority {
    fn try_new(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        target_entity_tag: EntityTag,
        target_store_path: &str,
    ) -> Result<Self, InternalError> {
        let entity_name = EntityName::try_from_str(target_entity_name).map_err(|err| {
            InternalError::strong_relation_target_name_invalid(
                source_path,
                field_name,
                target_path,
                target_entity_name,
                err,
            )
        })?;

        Ok(Self {
            path: target_path.to_string(),
            entity_name,
            entity_tag: target_entity_tag,
            store_path: target_store_path.to_string(),
        })
    }

    #[must_use]
    const fn path(&self) -> &str {
        self.path.as_str()
    }

    #[must_use]
    const fn entity_name(&self) -> EntityName {
        self.entity_name
    }

    #[must_use]
    const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    const fn store_path(&self) -> &str {
        self.store_path.as_str()
    }

    fn validate_against_db<C>(
        &self,
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
                    self.path.as_str(),
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
                self.path.as_str(),
                format!(
                    "target_entity_tag={} resolves to entity_path={} but relation declares {}",
                    self.entity_tag.value(),
                    hook.entity_path,
                    self.path
                ),
            ));
        }

        if hook.model.name() != self.entity_name.as_str() {
            return Err(InternalError::strong_relation_target_identity_mismatch(
                source_path,
                field_name,
                self.path.as_str(),
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
                self.path.as_str(),
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

impl InternalError {
    /// Map a relation-target key normalization failure into a typed `InternalError`.
    pub(in crate::db::relation) fn relation_target_raw_key_error(
        source_path: &'static str,
        field_name: &str,
        target_path: &str,
        value: &Value,
        message: &'static str,
    ) -> Self {
        Self::executor_unsupported(format!(
            "{message}: source={source_path} field={field_name} target={target_path} value={value:?}",
        ))
    }

    /// Construct the canonical strong-relation invalid target-name error.
    pub(in crate::db) fn strong_relation_target_name_invalid(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        target_entity_name: &str,
        err: impl Display,
    ) -> Self {
        Self::executor_internal(format!(
            "strong relation target name invalid: source={source_path} field={field_name} target={target_path} name={target_entity_name} ({err})",
        ))
    }

    /// Construct the canonical strong-relation target identity mismatch error.
    pub(in crate::db) fn strong_relation_target_identity_mismatch(
        source_path: &str,
        field_name: &str,
        target_path: &str,
        detail: impl Display,
    ) -> Self {
        Self::executor_internal(format!(
            "strong relation target identity mismatch: source={source_path} field={field_name} target={target_path} ({detail})",
        ))
    }

    /// Construct the canonical save-time strong-relation missing-target error.
    pub(crate) fn strong_relation_target_missing(
        source_path: &'static str,
        field_name: &str,
        target_path: &str,
        value: &Value,
    ) -> Self {
        Self::executor_unsupported(format!(
            "strong relation missing: source={source_path} field={field_name} target={target_path} key={value:?}",
        ))
    }

    /// Construct the canonical save-time strong-relation missing-store error.
    pub(crate) fn strong_relation_target_store_missing(
        source_path: &'static str,
        field_name: &str,
        target_path: &str,
        target_store_path: &str,
        value: &Value,
        err: impl Display,
    ) -> Self {
        Self::executor_internal(format!(
            "strong relation target store missing: source={source_path} field={field_name} target={target_path} store={target_store_path} key={value:?} ({err})",
        ))
    }
}

/// Visit concrete relation target values for one relation field payload.
///
/// Runtime relation List/Set shapes are represented as `Value::List`, and
/// optional relation slots may be explicit `Value::Null`.
pub(super) fn for_each_relation_target_value(
    value: &Value,
    mut visit: impl FnMut(&Value) -> Result<(), InternalError>,
) -> Result<(), InternalError> {
    match value {
        Value::List(items) => {
            for item in items {
                if matches!(item, Value::Null) {
                    continue;
                }
                visit(item)?;
            }
        }
        Value::Null => {}
        _ => visit(value)?,
    }

    Ok(())
}
