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
        Db, EntityRuntimeHooks,
        data::RawDataStoreKey,
        identity::EntityName,
        schema::{
            AcceptedFieldKind, classify_accepted_field_kind, ensure_accepted_schema_snapshot,
        },
    },
    error::InternalError,
    traits::CanisterKind,
    types::EntityTag,
    value::Value,
};
use std::{
    collections::BTreeSet,
    fmt::{Debug, Display},
};

pub(in crate::db) use metadata::{
    RelationFieldCardinality, RelationFieldMetadata, relation_field_metadata_for_model_iter,
};
pub(crate) use reverse_index::{
    ReverseRelationSourceInfo, prepare_reverse_relation_index_mutations_for_source_slot_readers,
};
pub(in crate::db) use save_validate::validate_save_relations_with_accepted_contract;
pub(in crate::db) use validate::validate_delete_relations_for_source;

///
/// RelationDeleteValidateFn
///
/// Function-pointer contract for delete-side relation validators.
///

pub(in crate::db) type RelationDeleteValidateFn<C> =
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
/// AcceptedRelationTargetMetadata
///
/// Accepted-schema relation target metadata projected from a relation field
/// or a supported collection wrapper. This is intentionally field-shape
/// metadata only; save validation and reverse-index preparation add their
/// own execution-specific source slot context.
///

#[derive(Clone, Copy)]
struct AcceptedRelationTargetMetadata<'a> {
    target_path: &'a str,
    target_entity_name: &'a str,
    target_entity_tag: EntityTag,
    target_store_path: &'a str,
    scalar_target_key_kind: &'a AcceptedFieldKind,
    cardinality: AcceptedRelationCardinality,
}

#[derive(Clone, Debug)]
struct AcceptedRelationTargetContract {
    target: AcceptedRelationTargetAuthority,
    primary_key_kinds: Vec<AcceptedFieldKind>,
}

impl AcceptedRelationTargetContract {
    #[must_use]
    const fn primary_key_kinds(&self) -> &[AcceptedFieldKind] {
        self.primary_key_kinds.as_slice()
    }

    fn into_target(self) -> AcceptedRelationTargetAuthority {
        self.target
    }
}

#[derive(Clone, Copy)]
struct AcceptedRelationTupleEdgeLocalComponent<'a> {
    field_name: &'a str,
    kind: &'a AcceptedFieldKind,
}

impl<'a> AcceptedRelationTupleEdgeLocalComponent<'a> {
    const fn new(field_name: &'a str, kind: &'a AcceptedFieldKind) -> Self {
        Self { field_name, kind }
    }
}

struct AcceptedRelationTupleEdgeDescriptor {
    target_contract: AcceptedRelationTargetContract,
}

impl AcceptedRelationTupleEdgeDescriptor {
    fn into_target_contract(self) -> AcceptedRelationTargetContract {
        self.target_contract
    }
}

fn accepted_relation_tuple_edge_descriptor<C>(
    db: &Db<C>,
    source_path: &str,
    relation_name: &str,
    target_path: &str,
    local_components: &[AcceptedRelationTupleEdgeLocalComponent<'_>],
) -> Result<AcceptedRelationTupleEdgeDescriptor, InternalError>
where
    C: CanisterKind,
{
    let target_contract =
        accepted_relation_target_contract(db, source_path, relation_name, target_path)?;
    let target_kinds = target_contract.primary_key_kinds();
    if local_components.len() != target_kinds.len() {
        return Err(InternalError::relation_target_identity_mismatch(
            source_path,
            relation_name,
            target_path,
            format!(
                "relation edge local component count {} does not match accepted target primary-key component count {}",
                local_components.len(),
                target_kinds.len()
            ),
        ));
    }

    for (local, target_kind) in local_components.iter().zip(target_kinds) {
        let local_kind = relation_local_component_key_kind(local.kind);
        if local_kind != target_kind {
            return Err(InternalError::relation_target_identity_mismatch(
                source_path,
                relation_name,
                target_path,
                format!(
                    "local field '{}' kind {local_kind:?} does not match accepted target primary-key kind {target_kind:?}",
                    local.field_name,
                ),
            ));
        }
        validate_relation_primary_key_component_kind(local_kind)?;
    }

    Ok(AcceptedRelationTupleEdgeDescriptor { target_contract })
}

struct AcceptedRelationScalarTargetDescriptor {
    target_contract: AcceptedRelationTargetContract,
    cardinality: AcceptedRelationCardinality,
}

impl AcceptedRelationScalarTargetDescriptor {
    const fn cardinality(&self) -> AcceptedRelationCardinality {
        self.cardinality
    }

    fn into_target_contract(self) -> AcceptedRelationTargetContract {
        self.target_contract
    }
}

fn accepted_scalar_relation_target_descriptor<C>(
    db: &Db<C>,
    source_path: &str,
    diagnostic_relation_name: &str,
    authority_relation_name: &str,
    kind: &AcceptedFieldKind,
    expected_edge_target_path: Option<&str>,
) -> Result<Option<AcceptedRelationScalarTargetDescriptor>, InternalError>
where
    C: CanisterKind,
{
    let Some(target) = accepted_relation_target_metadata_from_kind(kind) else {
        return Ok(None);
    };
    if let Some(edge_target_path) = expected_edge_target_path
        && target.target_path != edge_target_path
    {
        return Err(InternalError::store_invariant());
    }
    validate_relation_primary_key_component_kind(target.scalar_target_key_kind)?;
    let declared_target = AcceptedRelationTargetAuthority::try_new(
        source_path,
        authority_relation_name,
        target.target_path,
        target.target_entity_name,
        target.target_entity_tag,
        target.target_store_path,
    )?;
    let target_hook =
        declared_target.validate_against_db(db, source_path, diagnostic_relation_name)?;
    let target_contract = accepted_relation_target_contract_for_hook(
        db,
        source_path,
        diagnostic_relation_name,
        target_hook,
    )?;
    validate_accepted_relation_primary_key_kinds(
        source_path,
        diagnostic_relation_name,
        target.target_path,
        std::slice::from_ref(target.scalar_target_key_kind),
        target_contract.primary_key_kinds(),
    )?;

    Ok(Some(AcceptedRelationScalarTargetDescriptor {
        target_contract,
        cardinality: target.cardinality,
    }))
}

fn accepted_relation_target_contract<C>(
    db: &Db<C>,
    source_path: &str,
    relation_name: &str,
    target_path: &str,
) -> Result<AcceptedRelationTargetContract, InternalError>
where
    C: CanisterKind,
{
    let target_hook = db.runtime_hook_for_entity_path(target_path)?;
    accepted_relation_target_contract_for_hook(db, source_path, relation_name, target_hook)
}

fn accepted_relation_target_contract_for_hook<C>(
    db: &Db<C>,
    source_path: &str,
    relation_name: &str,
    target_hook: &EntityRuntimeHooks<C>,
) -> Result<AcceptedRelationTargetContract, InternalError>
where
    C: CanisterKind,
{
    let target_store = db.store_handle(target_hook.store_path)?;
    let accepted = target_store.with_schema_mut(|schema_store| {
        ensure_accepted_schema_snapshot(
            schema_store,
            target_hook.entity_tag,
            target_hook.entity_path,
            target_hook.store_path,
            target_hook.model,
        )
    })?;
    let primary_key_kinds = accepted
        .primary_key_field_kinds()
        .into_iter()
        .cloned()
        .collect();
    let target = AcceptedRelationTargetAuthority::try_new(
        source_path,
        relation_name,
        target_hook.entity_path,
        accepted.entity_name(),
        target_hook.entity_tag,
        target_hook.store_path,
    )?;

    Ok(AcceptedRelationTargetContract {
        target,
        primary_key_kinds,
    })
}

fn validate_accepted_relation_primary_key_kinds(
    source_path: &str,
    relation_name: &str,
    target_path: &str,
    relation_key_kinds: &[AcceptedFieldKind],
    accepted_key_kinds: &[AcceptedFieldKind],
) -> Result<(), InternalError> {
    if accepted_key_kinds.len() != relation_key_kinds.len() {
        return Err(InternalError::relation_target_identity_mismatch(
            source_path,
            relation_name,
            target_path,
            format!(
                "target accepted primary-key component count {} does not match relation component count {}",
                accepted_key_kinds.len(),
                relation_key_kinds.len(),
            ),
        ));
    }

    for (accepted_key_kind, relation_key_kind) in accepted_key_kinds.iter().zip(relation_key_kinds)
    {
        if accepted_key_kind != relation_key_kind {
            return Err(InternalError::relation_target_identity_mismatch(
                source_path,
                relation_name,
                target_path,
                format!(
                    "target accepted primary-key kind {accepted_key_kind:?} does not match relation key kind {relation_key_kind:?}"
                ),
            ));
        }
    }

    Ok(())
}

fn accepted_relation_target_metadata_from_kind(
    kind: &AcceptedFieldKind,
) -> Option<AcceptedRelationTargetMetadata<'_>> {
    fn relation_target(
        kind: &AcceptedFieldKind,
        cardinality: AcceptedRelationCardinality,
    ) -> Option<AcceptedRelationTargetMetadata<'_>> {
        let AcceptedFieldKind::Relation {
            target_path,
            target_entity_name,
            target_entity_tag,
            target_store_path,
            key_kind,
        } = kind
        else {
            return None;
        };

        Some(AcceptedRelationTargetMetadata {
            target_path,
            target_entity_name,
            target_entity_tag: *target_entity_tag,
            target_store_path,
            scalar_target_key_kind: key_kind.as_ref(),
            cardinality,
        })
    }

    match kind {
        AcceptedFieldKind::Relation { .. } => {
            relation_target(kind, AcceptedRelationCardinality::Single)
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            let cardinality = match kind {
                AcceptedFieldKind::List(_) => AcceptedRelationCardinality::List,
                AcceptedFieldKind::Set(_) => AcceptedRelationCardinality::Set,
                _ => unreachable!("relation invariant"),
            };

            relation_target(inner.as_ref(), cardinality)
        }
        _ => None,
    }
}

fn validate_relation_primary_key_component_kind(
    key_kind: &AcceptedFieldKind,
) -> Result<(), InternalError> {
    if let AcceptedFieldKind::Relation { key_kind, .. } = key_kind {
        return validate_relation_primary_key_component_kind(key_kind);
    }

    if classify_accepted_field_kind(key_kind).is_relation_key_eligible() {
        Ok(())
    } else {
        Err(InternalError::relation_source_row_unsupported_key_kind(
            key_kind,
        ))
    }
}

fn relation_local_component_key_kind(kind: &AcceptedFieldKind) -> &AcceptedFieldKind {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => key_kind,
        other => other,
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
            InternalError::relation_target_name_invalid(
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

    fn validate_against_db<'db, C>(
        &self,
        db: &'db Db<C>,
        source_path: &str,
        field_name: &str,
    ) -> Result<&'db EntityRuntimeHooks<C>, InternalError>
    where
        C: CanisterKind,
    {
        let hook = db
            .runtime_hook_for_entity_tag(self.entity_tag)
            .map_err(|err| {
                InternalError::relation_target_identity_mismatch(
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
            return Err(InternalError::relation_target_identity_mismatch(
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
            return Err(InternalError::relation_target_identity_mismatch(
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
            return Err(InternalError::relation_target_identity_mismatch(
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

        Ok(hook)
    }
}

impl InternalError {
    /// Map a relation-target key normalization failure into a typed `InternalError`.
    pub(in crate::db::relation) fn relation_target_raw_key_error(
        _source_path: &'static str,
        _field_name: &str,
        _target_path: &str,
        _value: &Value,
        _message: &'static str,
    ) -> Self {
        Self::executor_unsupported()
    }

    /// Construct the canonical relation invalid target-name error.
    pub(in crate::db::relation) fn relation_target_name_invalid(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _target_entity_name: &str,
        _err: impl Debug,
    ) -> Self {
        Self::executor_internal()
    }

    /// Construct the canonical relation target identity mismatch error.
    pub(in crate::db::relation) fn relation_target_identity_mismatch(
        _source_path: &str,
        _field_name: &str,
        _target_path: &str,
        _detail: impl Display,
    ) -> Self {
        Self::executor_internal()
    }

    /// Construct the canonical save-time relation missing-target error.
    pub(in crate::db::relation) fn relation_target_missing(
        _source_path: &'static str,
        _field_name: &str,
        _target_path: &str,
        _value: &Value,
    ) -> Self {
        Self::executor_unsupported()
    }

    /// Construct the canonical capability-based relation target policy error.
    pub(in crate::db::relation) fn relation_volatile_target_unsupported(
        _source_path: &'static str,
        _field_name: &str,
        _target_path: &str,
        _source_store_path: &'static str,
        _target_store_path: &str,
    ) -> Self {
        Self::executor_unsupported()
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

#[cfg(test)]
mod tests {
    use super::{
        validate_accepted_relation_primary_key_kinds, validate_relation_primary_key_component_kind,
    };
    use crate::{db::schema::AcceptedFieldKind, error::ErrorClass, types::EntityTag};

    fn relation_key_kind(key_kind: AcceptedFieldKind) -> AcceptedFieldKind {
        AcceptedFieldKind::Relation {
            target_path: "Target".to_string(),
            target_entity_name: "Target".to_string(),
            target_entity_tag: EntityTag::new(11),
            target_store_path: "TargetStore".to_string(),
            key_kind: Box::new(key_kind),
        }
    }

    #[test]
    fn relation_primary_key_component_kind_accepts_admitted_scalar_lanes() {
        for kind in [
            AcceptedFieldKind::Account,
            AcceptedFieldKind::Int64,
            AcceptedFieldKind::Int128,
            AcceptedFieldKind::Nat64,
            AcceptedFieldKind::Nat128,
            AcceptedFieldKind::Principal,
            AcceptedFieldKind::Subaccount,
            AcceptedFieldKind::Timestamp,
            AcceptedFieldKind::Ulid,
            AcceptedFieldKind::Unit,
        ] {
            validate_relation_primary_key_component_kind(&kind)
                .expect("admitted relation primary-key component kind should validate");
        }
    }

    #[test]
    fn relation_primary_key_component_kind_unwraps_relation_key_kind() {
        let kind = relation_key_kind(AcceptedFieldKind::Nat128);

        validate_relation_primary_key_component_kind(&kind)
            .expect("relation field wrapper should validate through its key kind");
    }

    #[test]
    fn relation_primary_key_component_kind_rejects_non_admitted_bigints() {
        for kind in [
            AcceptedFieldKind::IntBig { max_bytes: 32 },
            AcceptedFieldKind::NatBig { max_bytes: 32 },
            relation_key_kind(AcceptedFieldKind::IntBig { max_bytes: 32 }),
            relation_key_kind(AcceptedFieldKind::NatBig { max_bytes: 32 }),
        ] {
            validate_relation_primary_key_component_kind(&kind)
                .expect_err("big integer relation primary-key components must reject");
        }
    }

    #[test]
    fn accepted_relation_target_authority_rejects_primary_key_arity_drift() {
        let err = validate_accepted_relation_primary_key_kinds(
            "Source",
            "target_id",
            "Target",
            &[AcceptedFieldKind::Nat64],
            &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
        )
        .expect_err("relation target authority must reject primary-key arity drift");

        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(
            err.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInternal,
        );
    }

    #[test]
    fn accepted_relation_target_authority_rejects_primary_key_kind_drift() {
        let err = validate_accepted_relation_primary_key_kinds(
            "Source",
            "target_id",
            "Target",
            &[AcceptedFieldKind::Nat64],
            &[AcceptedFieldKind::Nat128],
        )
        .expect_err("relation target authority must reject primary-key kind drift");

        assert_eq!(err.class, ErrorClass::Internal);
        assert_eq!(
            err.diagnostic_code(),
            icydb_diagnostic_code::DiagnosticCode::RuntimeInternal,
        );
    }

    #[test]
    fn accepted_relation_target_authority_accepts_matching_ordered_primary_key_kinds() {
        validate_accepted_relation_primary_key_kinds(
            "Source",
            "author",
            "Target",
            &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
            &[AcceptedFieldKind::Nat64, AcceptedFieldKind::Ulid],
        )
        .expect("matching ordered relation target authority should validate");
    }
}
