//! Module: db::schema::constraint
//! Responsibility: accepted structural constraint identity and closure state.
//! Does not own: nullability, uniqueness, primary-key, or relation enforcement.
//! Boundary: records stable catalog identity for existing structural owners.

use std::collections::BTreeSet;

use crate::db::codec::{finalize_hash_sha256, new_hash_sha256};
use crate::db::schema::composite_catalog::AcceptedCompositeShape;
use crate::db::schema::enum_catalog::AcceptedEnumVariantBody;
use crate::db::schema::{
    AcceptedCheckExprV1, AcceptedCheckLiteralV1, AcceptedCompositeCatalog, AcceptedEnumCatalog,
    AcceptedFieldKind, AcceptedNamedTypeIdentity, AcceptedSchemaFingerprint, ConstraintId,
    ConstraintIdAllocator, FieldId, PersistedFieldOrigin, PersistedFieldSnapshot,
    PersistedIndexOrigin, PersistedIndexSnapshot, PersistedRelationEdgeSnapshot,
    PersistedSchemaSnapshot, RelationId, SchemaIndexId,
};
use sha2::Digest;

const PRIMARY_KEY_CONSTRAINT_NAME: &str = "__icydb_primary_key";
const MAX_ACCEPTED_CONSTRAINT_NAME_BYTES: usize = 256;
const CONSTRAINT_ACTIVATION_FINGERPRINT_DOMAIN: &[u8] = b"icydb.constraint-activation.v1";

/// Return the deterministic first-publication primary-key constraint name.
#[must_use]
pub(in crate::db) const fn primary_key_constraint_name() -> &'static str {
    PRIMARY_KEY_CONSTRAINT_NAME
}

/// Build the deterministic first-publication not-null constraint name.
#[must_use]
pub(in crate::db) fn not_null_constraint_name(field_id: FieldId) -> String {
    format!("__icydb_not_null_{}", field_id.get())
}

/// Durable origin of one accepted constraint catalog entry.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ConstraintOrigin {
    /// Proposed by generated schema metadata.
    Generated,
    /// Created through catalog-native SQL DDL.
    SqlDdl,
}

impl ConstraintOrigin {
    /// Map one accepted field owner into constraint ownership.
    #[must_use]
    pub(in crate::db) const fn from_field_origin(origin: PersistedFieldOrigin) -> Self {
        match origin {
            PersistedFieldOrigin::Generated => Self::Generated,
            PersistedFieldOrigin::SqlDdl => Self::SqlDdl,
        }
    }

    /// Map one accepted index owner into constraint ownership.
    #[must_use]
    pub(in crate::db) const fn from_index_origin(origin: PersistedIndexOrigin) -> Self {
        match origin {
            PersistedIndexOrigin::Generated => Self::Generated,
            PersistedIndexOrigin::SqlDdl => Self::SqlDdl,
        }
    }
}

/// Accepted identity of every ruled nominal value below one persisted root.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedRuleTarget {
    root_field_id: FieldId,
    target_type: AcceptedNamedTypeIdentity,
}

impl AcceptedRuleTarget {
    /// Build one target already resolved through accepted source bindings.
    #[must_use]
    pub(in crate::db) const fn new(
        root_field_id: FieldId,
        target_type: AcceptedNamedTypeIdentity,
    ) -> Self {
        Self {
            root_field_id,
            target_type,
        }
    }

    /// Return the persisted root whose finite value is traversed.
    #[must_use]
    pub(in crate::db) const fn root_field_id(self) -> FieldId {
        self.root_field_id
    }

    /// Return the accepted nominal identity selected during traversal.
    #[must_use]
    pub(in crate::db) const fn target_type(self) -> AcceptedNamedTypeIdentity {
        self.target_type
    }
}

/// Closed accepted operation owned by one targeted durable rule.
///
/// Numeric operands use the same canonical accepted literal payload as row
/// checks. Length bounds are already exact Nat64 values and need no second
/// encoded literal representation.
#[derive(Clone, Debug, Eq, PartialEq)]
#[expect(
    clippy::enum_variant_names,
    reason = "the inclusive suffix is part of each closed operation's precise semantics"
)]
pub(in crate::db) enum AcceptedRuleOperation {
    /// Inclusive Unicode-scalar, octet, or collection-cardinality range.
    LengthRangeInclusive { min: u64, max: u64 },
    /// Inclusive exact numeric minimum.
    NumericMinimumInclusive { value: AcceptedCheckLiteralV1 },
    /// Inclusive exact numeric range.
    NumericRangeInclusive {
        min: AcceptedCheckLiteralV1,
        max: AcceptedCheckLiteralV1,
    },
}

pub(in crate::db::schema) fn accepted_rule_target_is_reachable(
    root: &AcceptedFieldKind,
    target: AcceptedNamedTypeIdentity,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> bool {
    let mut pending = vec![root.clone()];
    let mut visited = BTreeSet::new();
    while let Some(kind) = pending.pop() {
        match kind {
            AcceptedFieldKind::Enum { type_id } => {
                let identity = AcceptedNamedTypeIdentity::Enum(type_id);
                if identity == target {
                    return true;
                }
                if !visited.insert(identity) {
                    continue;
                }
                let Some(definition) = enum_catalog.enum_type(type_id) else {
                    return false;
                };
                pending.extend(definition.variants().filter_map(|variant| {
                    let AcceptedEnumVariantBody::Payload { contract } = variant.body() else {
                        return None;
                    };
                    Some(contract.kind().clone())
                }));
            }
            AcceptedFieldKind::Composite { type_id } => {
                let identity = AcceptedNamedTypeIdentity::Composite(type_id);
                if identity == target {
                    return true;
                }
                if !visited.insert(identity) {
                    continue;
                }
                let Some(definition) = composite_catalog.composite_type(type_id) else {
                    return false;
                };
                match definition.shape() {
                    AcceptedCompositeShape::Record(fields) => {
                        pending.extend(fields.iter().map(|field| field.contract().kind().clone()));
                    }
                    AcceptedCompositeShape::Tuple(elements) => {
                        pending.extend(elements.iter().map(|element| element.kind().clone()));
                    }
                    AcceptedCompositeShape::Newtype(inner) => {
                        pending.push(inner.kind().clone());
                    }
                }
            }
            AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
                pending.push(*inner);
            }
            AcceptedFieldKind::Map { key, value } => {
                pending.push(*value);
                pending.push(*key);
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
            | AcceptedFieldKind::Relation { .. } => {}
        }
    }
    false
}

pub(in crate::db::schema) fn validate_accepted_targeted_rules(
    snapshot: &PersistedSchemaSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> bool {
    let accepted = snapshot.constraints().iter().filter_map(|constraint| {
        let AcceptedConstraintKind::TargetedRule { target, operation } = constraint.kind() else {
            return None;
        };
        Some((*target, operation.as_ref()))
    });
    let activating = snapshot
        .constraint_activations()
        .iter()
        .filter_map(|activation| {
            let ConstraintActivationKind::TargetedRule { target, operation } = activation.kind()
            else {
                return None;
            };
            Some((*target, operation.as_ref()))
        });
    accepted.chain(activating).all(|(target, operation)| {
        let Some(root) = snapshot
            .fields()
            .iter()
            .find(|field| field.id() == target.root_field_id())
        else {
            return false;
        };
        if !accepted_rule_target_is_reachable(
            root.kind(),
            target.target_type(),
            enum_catalog,
            composite_catalog,
        ) {
            return false;
        }
        let target_kind = match target.target_type() {
            AcceptedNamedTypeIdentity::Enum(type_id) => AcceptedFieldKind::Enum { type_id },
            AcceptedNamedTypeIdentity::Composite(type_id) => {
                AcceptedFieldKind::Composite { type_id }
            }
        };
        let Some(resolved_kind) = composite_catalog.resolve_newtype_value_kind(&target_kind) else {
            return false;
        };
        let kind_is_supported = match operation {
            AcceptedRuleOperation::LengthRangeInclusive { .. } => {
                accepted_rule_length_kind_is_supported(&resolved_kind)
            }
            AcceptedRuleOperation::NumericMinimumInclusive { .. }
            | AcceptedRuleOperation::NumericRangeInclusive { .. } => {
                accepted_rule_numeric_kind_is_supported(&resolved_kind)
            }
        };
        kind_is_supported
            && crate::db::schema::check::validate_accepted_rule_operation_literals(
                operation,
                &resolved_kind,
                enum_catalog,
                composite_catalog,
            )
            .is_ok()
    })
}

pub(in crate::db::schema) const fn accepted_rule_numeric_kind_is_supported(
    kind: &AcceptedFieldKind,
) -> bool {
    matches!(
        kind,
        AcceptedFieldKind::Decimal { .. }
            | AcceptedFieldKind::Float32
            | AcceptedFieldKind::Float64
            | AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64
            | AcceptedFieldKind::Int128
            | AcceptedFieldKind::IntBig { .. }
            | AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64
            | AcceptedFieldKind::Nat128
            | AcceptedFieldKind::NatBig { .. }
    )
}

pub(in crate::db::schema) const fn accepted_rule_length_kind_is_supported(
    kind: &AcceptedFieldKind,
) -> bool {
    matches!(
        kind,
        AcceptedFieldKind::Blob { .. }
            | AcceptedFieldKind::Text { .. }
            | AcceptedFieldKind::List(_)
            | AcceptedFieldKind::Set(_)
            | AcceptedFieldKind::Map { .. }
    )
}

/// Structural owner referenced by one accepted constraint catalog entry.
///
/// The referenced field, index, or relation remains the sole execution
/// authority. This enum deliberately does not repeat its semantic contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedConstraintKind {
    /// The entity's ordered accepted primary-key fields.
    PrimaryKey,
    /// One accepted field whose field contract rejects `NULL`.
    NotNull { field_id: FieldId },
    /// One accepted secondary index whose index contract is unique.
    Unique { index_id: SchemaIndexId },
    /// One accepted relation whose relation contract owns referential checks.
    Relation { relation_id: RelationId },
    /// One accepted row-local expression that owns its complete semantics.
    Check {
        expression: Box<AcceptedCheckExprV1>,
    },
    /// One accepted root-plus-nominal target and its closed durable operation.
    TargetedRule {
        target: AcceptedRuleTarget,
        operation: Box<AcceptedRuleOperation>,
    },
}

/// Stable accepted identity projected into one runtime enforcement contract.
///
/// This compact value deliberately excludes constraint semantics. The
/// accepted field, index, relation, or row program remains the evaluator;
/// specialized runtime paths carry this value only so every violation reaches
/// the public boundary with the catalog-owned identity.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedConstraintIdentity {
    id: ConstraintId,
    name: String,
}

impl AcceptedConstraintIdentity {
    /// Build one identity already proven by accepted catalog closure.
    #[must_use]
    pub(in crate::db) const fn new(id: ConstraintId, name: String) -> Self {
        Self { id, name }
    }

    /// Project stable identity from one accepted constraint catalog entry.
    #[must_use]
    pub(in crate::db::schema) fn from_constraint(constraint: &AcceptedConstraintSnapshot) -> Self {
        Self::new(constraint.id(), constraint.name().to_string())
    }

    /// Return the stable entity-local constraint identity.
    #[must_use]
    pub(in crate::db) const fn id(&self) -> ConstraintId {
        self.id
    }

    /// Borrow the stable accepted constraint name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }
}

/// Current lifecycle state of one accepted constraint activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum ConstraintActivationState {
    /// New writes are gated while historical validation has not started.
    EnforcingNewWrites,
    /// A schema-owned bounded validation job is proving historical rows.
    Validating,
}

impl ConstraintActivationState {
    const fn fingerprint_tag(self) -> u8 {
        match self {
            Self::EnforcingNewWrites => 1,
            Self::Validating => 2,
        }
    }
}

/// Candidate semantics owned by one accepted activation record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ConstraintActivationKind {
    /// A currently nullable field proposed to become non-null.
    NotNull { field_id: FieldId },
    /// A reserved candidate unique index that is not planner-visible.
    Unique { index_id: SchemaIndexId },
    /// A reserved candidate relation that is not delete-authoritative.
    Relation { relation_id: RelationId },
    /// One canonical row-local expression used by the new-write gate.
    Check {
        expression: Box<AcceptedCheckExprV1>,
    },
    /// One source-bound durable rule awaiting historical proof.
    TargetedRule {
        target: AcceptedRuleTarget,
        operation: Box<AcceptedRuleOperation>,
    },
}

/// Semantic fingerprint binding a validation job to one exact activation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ConstraintActivationFingerprint([u8; 32]);

impl ConstraintActivationFingerprint {
    /// Reconstruct one persisted activation fingerprint.
    #[must_use]
    pub(in crate::db) const fn new(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    /// Return the stable persisted fingerprint bytes.
    #[must_use]
    pub(in crate::db) const fn as_bytes(self) -> [u8; 32] {
        self.0
    }
}

/// Accepted-schema migration authority for one not-yet-validated constraint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ConstraintActivationSnapshot {
    id: ConstraintId,
    name: String,
    origin: ConstraintOrigin,
    kind: ConstraintActivationKind,
    state: ConstraintActivationState,
    base_schema_fingerprint: AcceptedSchemaFingerprint,
    activation_epoch: u64,
    fingerprint: ConstraintActivationFingerprint,
}

impl ConstraintActivationSnapshot {
    /// Build one candidate activation and derive its complete semantic fingerprint.
    #[must_use]
    pub(in crate::db) fn new(
        id: ConstraintId,
        name: String,
        origin: ConstraintOrigin,
        kind: ConstraintActivationKind,
        state: ConstraintActivationState,
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
    ) -> Self {
        let fingerprint = constraint_activation_fingerprint(
            id,
            name.as_str(),
            origin,
            &kind,
            state,
            base_schema_fingerprint,
            activation_epoch,
        );
        Self {
            id,
            name,
            origin,
            kind,
            state,
            base_schema_fingerprint,
            activation_epoch,
            fingerprint,
        }
    }

    /// Reconstruct persisted activation parts before catalog closure validation.
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "the persisted activation boundary keeps every fingerprinted field explicit"
    )]
    pub(in crate::db) const fn from_persisted_parts(
        id: ConstraintId,
        name: String,
        origin: ConstraintOrigin,
        kind: ConstraintActivationKind,
        state: ConstraintActivationState,
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
        fingerprint: ConstraintActivationFingerprint,
    ) -> Self {
        Self {
            id,
            name,
            origin,
            kind,
            state,
            base_schema_fingerprint,
            activation_epoch,
            fingerprint,
        }
    }

    /// Return the stable constraint identity reserved through promotion.
    #[must_use]
    pub(in crate::db) const fn id(&self) -> ConstraintId {
        self.id
    }

    /// Borrow the reserved accepted constraint name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return candidate ownership.
    #[must_use]
    pub(in crate::db) const fn origin(&self) -> ConstraintOrigin {
        self.origin
    }

    /// Borrow the candidate semantics.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &ConstraintActivationKind {
        &self.kind
    }

    /// Return the live activation lifecycle state.
    #[must_use]
    pub(in crate::db) const fn state(&self) -> ConstraintActivationState {
        self.state
    }

    /// Return the accepted root fingerprint against which validation began.
    #[must_use]
    pub(in crate::db) const fn base_schema_fingerprint(&self) -> AcceptedSchemaFingerprint {
        self.base_schema_fingerprint
    }

    /// Return the non-zero activation epoch.
    #[must_use]
    pub(in crate::db) const fn activation_epoch(&self) -> u64 {
        self.activation_epoch
    }

    /// Return the exact candidate semantic fingerprint.
    #[must_use]
    pub(in crate::db) const fn fingerprint(&self) -> ConstraintActivationFingerprint {
        self.fingerprint
    }

    pub(in crate::db::schema) fn has_valid_fingerprint(&self) -> bool {
        self.activation_epoch != 0
            && self.fingerprint
                == constraint_activation_fingerprint(
                    self.id,
                    self.name.as_str(),
                    self.origin,
                    &self.kind,
                    self.state,
                    self.base_schema_fingerprint,
                    self.activation_epoch,
                )
    }

    fn clone_with_state(&self, state: ConstraintActivationState) -> Self {
        Self::new(
            self.id,
            self.name.clone(),
            self.origin,
            self.kind.clone(),
            state,
            self.base_schema_fingerprint,
            self.activation_epoch,
        )
    }
}

/// Stable accepted identity and display metadata for one structural constraint.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedConstraintSnapshot {
    id: ConstraintId,
    name: String,
    origin: ConstraintOrigin,
    kind: AcceptedConstraintKind,
}

impl AcceptedConstraintSnapshot {
    /// Build one accepted structural constraint from validated catalog pieces.
    #[must_use]
    pub(in crate::db) const fn new(
        id: ConstraintId,
        name: String,
        origin: ConstraintOrigin,
        kind: AcceptedConstraintKind,
    ) -> Self {
        Self {
            id,
            name,
            origin,
            kind,
        }
    }

    /// Return the stable entity-local constraint identity.
    #[must_use]
    pub(in crate::db) const fn id(&self) -> ConstraintId {
        self.id
    }

    /// Borrow the stable accepted constraint name.
    #[must_use]
    pub(in crate::db) const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return accepted constraint ownership.
    #[must_use]
    pub(in crate::db) const fn origin(&self) -> ConstraintOrigin {
        self.origin
    }

    /// Return the referenced structural owner kind.
    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &AcceptedConstraintKind {
        &self.kind
    }

    fn clone_with_kind(&self, kind: AcceptedConstraintKind) -> Self {
        Self {
            id: self.id,
            name: self.name.clone(),
            origin: self.origin,
            kind,
        }
    }
}

/// Failure to derive a closed structural constraint candidate.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedConstraintCatalogError {
    /// The entity-local non-reusing ID space is exhausted.
    IdentityExhausted,
    /// An accepted constraint name is empty.
    EmptyName,
    /// An accepted constraint name violates the accepted schema-name policy.
    InvalidName,
    /// An accepted constraint name exceeds the persisted metadata bound.
    NameTooLong,
    /// A new stable name collides with an existing constraint.
    DuplicateName,
    /// An activation epoch is zero and cannot bind durable validation state.
    InvalidActivationEpoch,
    /// An unrelated schema change was attempted while activation is live.
    LiveActivation,
    /// The requested activation identity is not live.
    ActivationNotFound,
    /// The requested activation transition is not valid from its current state.
    InvalidActivationState,
    /// A requested structural owner does not have one matching catalog entry.
    OwnerMismatch,
}

/// Invariant-bearing accepted constraint registry and non-reusing allocator.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct AcceptedConstraintCatalog {
    allocator: ConstraintIdAllocator,
    constraints: Vec<AcceptedConstraintSnapshot>,
    activations: Vec<ConstraintActivationSnapshot>,
}

impl AcceptedConstraintCatalog {
    /// Reconstruct persisted catalog parts before closure validation.
    #[must_use]
    pub(in crate::db) const fn from_persisted_parts(
        allocator: ConstraintIdAllocator,
        constraints: Vec<AcceptedConstraintSnapshot>,
        activations: Vec<ConstraintActivationSnapshot>,
    ) -> Self {
        Self {
            allocator,
            constraints,
            activations,
        }
    }

    /// Build deterministic structural entries for authoritative first creation.
    pub(in crate::db) fn initial(
        fields: &[PersistedFieldSnapshot],
        indexes: &[PersistedIndexSnapshot],
        relations: &[PersistedRelationEdgeSnapshot],
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        let mut catalog = Self::default();
        catalog.push(
            primary_key_constraint_name().to_string(),
            ConstraintOrigin::Generated,
            AcceptedConstraintKind::PrimaryKey,
        )?;
        for field in fields.iter().filter(|field| !field.nullable()) {
            catalog = catalog.with_added_not_null(field)?;
        }
        for index in indexes.iter().filter(|index| index.unique()) {
            catalog = catalog.with_added_unique(index)?;
        }
        for relation in relations {
            catalog = catalog.with_added_relation(relation)?;
        }
        Ok(catalog)
    }

    /// Return persisted non-reusing allocator state.
    #[must_use]
    pub(in crate::db) const fn allocator(&self) -> ConstraintIdAllocator {
        self.allocator
    }

    /// Borrow accepted constraints ordered by stable identity.
    #[must_use]
    pub(in crate::db) const fn constraints(&self) -> &[AcceptedConstraintSnapshot] {
        self.constraints.as_slice()
    }

    /// Borrow live activations ordered by reserved constraint identity.
    #[must_use]
    pub(in crate::db) const fn activations(&self) -> &[ConstraintActivationSnapshot] {
        self.activations.as_slice()
    }

    /// Borrow one live activation by its stable identity.
    #[must_use]
    pub(in crate::db) fn activation(
        &self,
        id: ConstraintId,
    ) -> Option<&ConstraintActivationSnapshot> {
        self.activations
            .iter()
            .find(|activation| activation.id() == id)
    }

    /// Add the structural registry entry required by one new non-null field.
    pub(in crate::db) fn with_added_not_null(
        mut self,
        field: &PersistedFieldSnapshot,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        if field.nullable() {
            return Ok(self);
        }
        self.push(
            not_null_constraint_name(field.id()),
            ConstraintOrigin::from_field_origin(field.origin()),
            AcceptedConstraintKind::NotNull {
                field_id: field.id(),
            },
        )?;
        Ok(self)
    }

    /// Remove the structural registry entry for one field made nullable.
    pub(in crate::db) fn with_removed_not_null(
        mut self,
        field_id: FieldId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        let before = self.constraints.len();
        self.constraints.retain(|constraint| {
            !matches!(
                constraint.kind(),
                AcceptedConstraintKind::NotNull {
                    field_id: constrained
                } if *constrained == field_id
            )
        });
        if self.constraints.len().saturating_add(1) != before {
            return Err(AcceptedConstraintCatalogError::OwnerMismatch);
        }
        Ok(self)
    }

    /// Add the registry entry paired with one new unique index.
    pub(in crate::db) fn with_added_unique(
        mut self,
        index: &PersistedIndexSnapshot,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        if !index.unique() {
            return Ok(self);
        }
        self.push(
            index.name().to_string(),
            ConstraintOrigin::from_index_origin(index.origin()),
            AcceptedConstraintKind::Unique {
                index_id: index.schema_id(),
            },
        )?;
        Ok(self)
    }

    /// Add one fully validated row-local check candidate.
    pub(in crate::db) fn with_added_check(
        mut self,
        name: String,
        origin: ConstraintOrigin,
        expression: AcceptedCheckExprV1,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        self.push(
            name,
            origin,
            AcceptedConstraintKind::Check {
                expression: Box::new(expression),
            },
        )?;
        Ok(self)
    }

    /// Add one fully bound targeted durable rule during initial publication.
    pub(in crate::db) fn with_added_targeted_rule(
        mut self,
        name: String,
        origin: ConstraintOrigin,
        target: AcceptedRuleTarget,
        operation: AcceptedRuleOperation,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        self.push(
            name,
            origin,
            AcceptedConstraintKind::TargetedRule {
                target,
                operation: Box::new(operation),
            },
        )?;
        Ok(self)
    }

    /// Reserve one generated or SQL-DDL check activation in accepted schema.
    pub(in crate::db) fn with_added_check_activation(
        self,
        name: String,
        origin: ConstraintOrigin,
        expression: AcceptedCheckExprV1,
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.with_added_activation(
            name,
            origin,
            ConstraintActivationKind::Check {
                expression: Box::new(expression),
            },
            base_schema_fingerprint,
            activation_epoch,
        )
    }

    /// Reserve one generated targeted-rule activation in accepted schema.
    pub(in crate::db) fn with_added_targeted_rule_activation(
        self,
        name: String,
        origin: ConstraintOrigin,
        target: AcceptedRuleTarget,
        operation: AcceptedRuleOperation,
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.with_added_activation(
            name,
            origin,
            ConstraintActivationKind::TargetedRule {
                target,
                operation: Box::new(operation),
            },
            base_schema_fingerprint,
            activation_epoch,
        )
    }

    /// Reserve one not-null activation while the accepted field remains nullable.
    #[cfg(any(test, feature = "query"))]
    pub(in crate::db) fn with_added_not_null_activation(
        self,
        field: &PersistedFieldSnapshot,
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        if !field.nullable() {
            return Err(AcceptedConstraintCatalogError::OwnerMismatch);
        }
        self.with_added_activation(
            not_null_constraint_name(field.id()),
            ConstraintOrigin::from_field_origin(field.origin()),
            ConstraintActivationKind::NotNull {
                field_id: field.id(),
            },
            base_schema_fingerprint,
            activation_epoch,
        )
    }

    /// Reserve one unique-index activation beside its planner-invisible owner.
    #[cfg(any(test, feature = "query"))]
    pub(in crate::db) fn with_added_unique_activation(
        self,
        index: &PersistedIndexSnapshot,
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        if !index.unique() {
            return Err(AcceptedConstraintCatalogError::OwnerMismatch);
        }
        self.with_added_activation(
            index.name().to_string(),
            ConstraintOrigin::from_index_origin(index.origin()),
            ConstraintActivationKind::Unique {
                index_id: index.schema_id(),
            },
            base_schema_fingerprint,
            activation_epoch,
        )
    }

    fn with_added_activation(
        mut self,
        name: String,
        origin: ConstraintOrigin,
        kind: ConstraintActivationKind,
        base_schema_fingerprint: AcceptedSchemaFingerprint,
        activation_epoch: u64,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        validate_constraint_name(name.as_str())?;
        if activation_epoch == 0 {
            return Err(AcceptedConstraintCatalogError::InvalidActivationEpoch);
        }
        if self.name_is_reserved(name.as_str()) {
            return Err(AcceptedConstraintCatalogError::DuplicateName);
        }
        let (allocator, id) = self
            .allocator
            .checked_reserve()
            .ok_or(AcceptedConstraintCatalogError::IdentityExhausted)?;
        self.allocator = allocator;
        self.activations.push(ConstraintActivationSnapshot::new(
            id,
            name,
            origin,
            kind,
            ConstraintActivationState::EnforcingNewWrites,
            base_schema_fingerprint,
            activation_epoch,
        ));
        Ok(self)
    }

    /// Move one write-gated activation into schema-owned validation.
    pub(in crate::db) fn with_validation_started(
        mut self,
        id: ConstraintId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        let activation = self
            .activations
            .iter_mut()
            .find(|activation| activation.id() == id)
            .ok_or(AcceptedConstraintCatalogError::ActivationNotFound)?;
        if activation.state() != ConstraintActivationState::EnforcingNewWrites {
            return Err(AcceptedConstraintCatalogError::InvalidActivationState);
        }
        *activation = activation.clone_with_state(ConstraintActivationState::Validating);
        Ok(self)
    }

    /// Promote one completely validated activation while preserving its identity.
    pub(in crate::db) fn with_promoted_activation(
        self,
        id: ConstraintId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.with_promoted_activation_from_state(id, ConstraintActivationState::Validating)
    }

    /// Promote one activation proved completely within one bounded message.
    pub(in crate::db) fn with_directly_validated_activation(
        self,
        id: ConstraintId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.with_promoted_activation_from_state(id, ConstraintActivationState::EnforcingNewWrites)
    }

    fn with_promoted_activation_from_state(
        mut self,
        id: ConstraintId,
        required_state: ConstraintActivationState,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        let position = self
            .activations
            .iter()
            .position(|activation| activation.id() == id)
            .ok_or(AcceptedConstraintCatalogError::ActivationNotFound)?;
        let activation = self
            .activations
            .get(position)
            .cloned()
            .ok_or(AcceptedConstraintCatalogError::ActivationNotFound)?;
        if activation.state() != required_state {
            return Err(AcceptedConstraintCatalogError::InvalidActivationState);
        }
        let kind = match activation.kind {
            ConstraintActivationKind::NotNull { field_id } => {
                AcceptedConstraintKind::NotNull { field_id }
            }
            ConstraintActivationKind::Unique { index_id } => {
                AcceptedConstraintKind::Unique { index_id }
            }
            ConstraintActivationKind::Relation { relation_id } => {
                AcceptedConstraintKind::Relation { relation_id }
            }
            ConstraintActivationKind::Check { expression } => {
                AcceptedConstraintKind::Check { expression }
            }
            ConstraintActivationKind::TargetedRule { target, operation } => {
                AcceptedConstraintKind::TargetedRule { target, operation }
            }
        };
        self.activations.remove(position);
        self.constraints.push(AcceptedConstraintSnapshot::new(
            activation.id,
            activation.name,
            activation.origin,
            kind,
        ));
        self.constraints.sort_by_key(AcceptedConstraintSnapshot::id);
        Ok(self)
    }

    /// Abort one live activation while permanently retiring its reserved identity.
    pub(in crate::db) fn with_aborted_activation(
        mut self,
        id: ConstraintId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        let before = self.activations.len();
        self.activations.retain(|activation| activation.id() != id);
        if self.activations.len().saturating_add(1) != before {
            return Err(AcceptedConstraintCatalogError::ActivationNotFound);
        }
        Ok(self)
    }

    /// Remove one accepted generated row-local declaration while retiring its identity.
    pub(in crate::db) fn with_removed_generated_constraint(
        self,
        id: ConstraintId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.with_removed_constraint(id, ConstraintOrigin::Generated, true)
    }

    /// Remove one accepted SQL-DDL-owned check while retiring its identity.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn with_removed_sql_ddl_check(
        self,
        id: ConstraintId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.with_removed_constraint(id, ConstraintOrigin::SqlDdl, false)
    }

    fn with_removed_constraint(
        mut self,
        id: ConstraintId,
        origin: ConstraintOrigin,
        targeted_rule_allowed: bool,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        let position = self
            .constraints
            .iter()
            .position(|constraint| constraint.id() == id)
            .ok_or(AcceptedConstraintCatalogError::OwnerMismatch)?;
        let constraint = self
            .constraints
            .get(position)
            .ok_or(AcceptedConstraintCatalogError::OwnerMismatch)?;
        let kind_matches = matches!(constraint.kind(), AcceptedConstraintKind::Check { .. })
            || targeted_rule_allowed
                && matches!(
                    constraint.kind(),
                    AcceptedConstraintKind::TargetedRule { .. }
                );
        if constraint.origin() != origin || !kind_matches {
            return Err(AcceptedConstraintCatalogError::OwnerMismatch);
        }
        self.constraints.remove(position);
        Ok(self)
    }

    /// Return whether `after` is one exact lifecycle transition from this live catalog.
    pub(in crate::db) fn permits_live_activation_transition_to(&self, after: &Self) -> bool {
        if self == after {
            return true;
        }
        self.activations.iter().any(|activation| {
            let id = activation.id();
            (activation.state() == ConstraintActivationState::EnforcingNewWrites
                && self
                    .clone()
                    .with_validation_started(id)
                    .is_ok_and(|candidate| candidate == *after))
                || self
                    .clone()
                    .with_aborted_activation(id)
                    .is_ok_and(|candidate| candidate == *after)
                || match activation.state() {
                    ConstraintActivationState::EnforcingNewWrites => self
                        .clone()
                        .with_directly_validated_activation(id)
                        .is_ok_and(|candidate| candidate == *after),
                    ConstraintActivationState::Validating => self
                        .clone()
                        .with_promoted_activation(id)
                        .is_ok_and(|candidate| candidate == *after),
                }
        })
    }

    /// Remove the registry entry paired with one dropped unique index.
    pub(in crate::db) fn with_removed_unique(
        mut self,
        index_id: SchemaIndexId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        let before = self.constraints.len();
        self.constraints.retain(|constraint| {
            !matches!(
                constraint.kind(),
                AcceptedConstraintKind::Unique {
                    index_id: constrained
                } if *constrained == index_id
            )
        });
        if self.constraints.len().saturating_add(1) != before {
            return Err(AcceptedConstraintCatalogError::OwnerMismatch);
        }
        Ok(self)
    }

    /// Remove the generated registry entry paired with one dropped relation.
    pub(in crate::db) fn with_removed_relation(
        mut self,
        relation_id: RelationId,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        let position = self
            .constraints
            .iter()
            .position(|constraint| {
                constraint.origin() == ConstraintOrigin::Generated
                    && matches!(
                        constraint.kind(),
                        AcceptedConstraintKind::Relation {
                            relation_id: constrained
                        } if *constrained == relation_id
                    )
            })
            .ok_or(AcceptedConstraintCatalogError::OwnerMismatch)?;
        self.constraints.remove(position);
        Ok(self)
    }

    /// Rewrite field references after an exact dense-layout rewrite.
    pub(in crate::db) fn with_mapped_field_ids(
        mut self,
        map: impl Copy + Fn(FieldId) -> Option<FieldId>,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        self.constraints = self
            .constraints
            .iter()
            .map(|constraint| match constraint.kind() {
                AcceptedConstraintKind::NotNull { field_id } => map(*field_id)
                    .map(|field_id| {
                        constraint.clone_with_kind(AcceptedConstraintKind::NotNull { field_id })
                    })
                    .ok_or(AcceptedConstraintCatalogError::OwnerMismatch),
                AcceptedConstraintKind::Check { expression } => expression
                    .clone_with_mapped_field_ids(map)
                    .map(|expression| {
                        constraint.clone_with_kind(AcceptedConstraintKind::Check {
                            expression: Box::new(expression),
                        })
                    })
                    .map_err(|_| AcceptedConstraintCatalogError::OwnerMismatch),
                AcceptedConstraintKind::TargetedRule { target, operation } => {
                    map(target.root_field_id())
                        .map(|root_field_id| {
                            constraint.clone_with_kind(AcceptedConstraintKind::TargetedRule {
                                target: AcceptedRuleTarget::new(
                                    root_field_id,
                                    target.target_type(),
                                ),
                                operation: operation.clone(),
                            })
                        })
                        .ok_or(AcceptedConstraintCatalogError::OwnerMismatch)
                }
                AcceptedConstraintKind::PrimaryKey
                | AcceptedConstraintKind::Unique { .. }
                | AcceptedConstraintKind::Relation { .. } => Ok(constraint.clone()),
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(self)
    }

    fn with_added_relation(
        mut self,
        relation: &PersistedRelationEdgeSnapshot,
    ) -> Result<Self, AcceptedConstraintCatalogError> {
        self.reject_live_activation()?;
        self.push(
            relation.name().to_string(),
            ConstraintOrigin::Generated,
            AcceptedConstraintKind::Relation {
                relation_id: relation.id(),
            },
        )?;
        Ok(self)
    }

    fn push(
        &mut self,
        name: String,
        origin: ConstraintOrigin,
        kind: AcceptedConstraintKind,
    ) -> Result<(), AcceptedConstraintCatalogError> {
        validate_constraint_name(name.as_str())?;
        if self
            .constraints
            .iter()
            .any(|constraint| constraint.name() == name)
        {
            return Err(AcceptedConstraintCatalogError::DuplicateName);
        }
        let (allocator, id) = self
            .allocator
            .checked_reserve()
            .ok_or(AcceptedConstraintCatalogError::IdentityExhausted)?;
        self.allocator = allocator;
        self.constraints
            .push(AcceptedConstraintSnapshot::new(id, name, origin, kind));
        Ok(())
    }

    const fn reject_live_activation(&self) -> Result<(), AcceptedConstraintCatalogError> {
        if self.activations.is_empty() {
            Ok(())
        } else {
            Err(AcceptedConstraintCatalogError::LiveActivation)
        }
    }

    fn name_is_reserved(&self, name: &str) -> bool {
        self.constraints
            .iter()
            .any(|constraint| constraint.name() == name)
            || self
                .activations
                .iter()
                .any(|activation| activation.name() == name)
    }
}

fn constraint_activation_fingerprint(
    id: ConstraintId,
    name: &str,
    origin: ConstraintOrigin,
    kind: &ConstraintActivationKind,
    state: ConstraintActivationState,
    base_schema_fingerprint: AcceptedSchemaFingerprint,
    activation_epoch: u64,
) -> ConstraintActivationFingerprint {
    let mut hasher = new_hash_sha256();
    hasher.update(CONSTRAINT_ACTIVATION_FINGERPRINT_DOMAIN);
    hasher.update(id.get().to_be_bytes());
    hasher.update(u64::try_from(name.len()).unwrap_or(u64::MAX).to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update([match origin {
        ConstraintOrigin::Generated => 1,
        ConstraintOrigin::SqlDdl => 2,
    }]);
    match kind {
        ConstraintActivationKind::NotNull { field_id } => {
            hasher.update([1]);
            hasher.update(field_id.get().to_be_bytes());
        }
        ConstraintActivationKind::Unique { index_id } => {
            hasher.update([2]);
            hasher.update(index_id.get().to_be_bytes());
        }
        ConstraintActivationKind::Relation { relation_id } => {
            hasher.update([3]);
            hasher.update(relation_id.get().to_be_bytes());
        }
        ConstraintActivationKind::Check { expression } => {
            hasher.update([4]);
            let expression = expression.canonical_key();
            hasher.update(
                u64::try_from(expression.len())
                    .unwrap_or(u64::MAX)
                    .to_be_bytes(),
            );
            hasher.update(expression);
        }
        ConstraintActivationKind::TargetedRule { target, operation } => {
            hasher.update([5]);
            hasher.update(target.root_field_id().get().to_be_bytes());
            match target.target_type() {
                AcceptedNamedTypeIdentity::Enum(type_id) => {
                    hasher.update([1]);
                    hasher.update(type_id.get().to_be_bytes());
                }
                AcceptedNamedTypeIdentity::Composite(type_id) => {
                    hasher.update([2]);
                    hasher.update(type_id.get().to_be_bytes());
                }
            }
            update_targeted_rule_operation_fingerprint(&mut hasher, operation);
        }
    }
    hasher.update([state.fingerprint_tag()]);
    hasher.update(base_schema_fingerprint.as_bytes());
    hasher.update(activation_epoch.to_be_bytes());
    ConstraintActivationFingerprint::new(finalize_hash_sha256(hasher))
}

fn update_targeted_rule_operation_fingerprint(
    hasher: &mut sha2::Sha256,
    operation: &AcceptedRuleOperation,
) {
    match operation {
        AcceptedRuleOperation::LengthRangeInclusive { min, max } => {
            hasher.update([1]);
            hasher.update(min.to_be_bytes());
            hasher.update(max.to_be_bytes());
        }
        AcceptedRuleOperation::NumericMinimumInclusive { value } => {
            hasher.update([2]);
            let value = value.canonical_key();
            hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
            hasher.update(value);
        }
        AcceptedRuleOperation::NumericRangeInclusive { min, max } => {
            hasher.update([3]);
            for value in [min.canonical_key(), max.canonical_key()] {
                hasher.update(u64::try_from(value.len()).unwrap_or(u64::MAX).to_be_bytes());
                hasher.update(value);
            }
        }
    }
}

pub(in crate::db::schema) fn accepted_constraint_name_is_valid(name: &str) -> bool {
    validate_constraint_name(name).is_ok()
}

/// Validate one generated constraint name at macro expansion.
///
/// Accepted catalog admission repeats this check and remains authoritative.
#[doc(hidden)]
pub fn validate_generated_constraint_name(name: &str) -> Result<(), String> {
    validate_constraint_name(name).map_err(|error| format!("invalid constraint name: {error:?}"))
}

pub(in crate::db) fn validate_constraint_name(
    name: &str,
) -> Result<(), AcceptedConstraintCatalogError> {
    let bytes = name.as_bytes();
    if bytes.is_empty() {
        return Err(AcceptedConstraintCatalogError::EmptyName);
    }
    if bytes.len() > MAX_ACCEPTED_CONSTRAINT_NAME_BYTES {
        return Err(AcceptedConstraintCatalogError::NameTooLong);
    }
    if name
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(AcceptedConstraintCatalogError::InvalidName);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AcceptedConstraintCatalog, AcceptedConstraintCatalogError, AcceptedConstraintKind,
        ConstraintActivationState, ConstraintOrigin,
    };
    use crate::db::schema::{AcceptedCheckExprV1, AcceptedSchemaFingerprint};

    #[test]
    fn accepted_constraint_catalog_rejects_noncanonical_or_unbounded_names() {
        for (name, expected) in [
            ("", AcceptedConstraintCatalogError::EmptyName),
            (
                "contains space",
                AcceptedConstraintCatalogError::InvalidName,
            ),
            ("contains\0nul", AcceptedConstraintCatalogError::InvalidName),
        ] {
            let mut catalog = AcceptedConstraintCatalog::default();
            assert_eq!(
                catalog.push(
                    name.to_string(),
                    ConstraintOrigin::Generated,
                    AcceptedConstraintKind::PrimaryKey,
                ),
                Err(expected),
            );
        }

        let mut catalog = AcceptedConstraintCatalog::default();
        assert_eq!(
            catalog.push(
                "a".repeat(257),
                ConstraintOrigin::Generated,
                AcceptedConstraintKind::PrimaryKey,
            ),
            Err(AcceptedConstraintCatalogError::NameTooLong),
        );
    }

    #[test]
    fn check_activation_preserves_identity_through_validation_and_promotion() {
        let catalog = AcceptedConstraintCatalog::default()
            .with_added_check_activation(
                "pending_check".to_string(),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
                AcceptedSchemaFingerprint::new([0xA5; 32]),
                7,
            )
            .expect("activation should reserve identity");
        let allocator = catalog.allocator();
        let activation = catalog.activations()[0].clone();
        let validating = catalog
            .with_validation_started(activation.id())
            .expect("activation should enter validation");
        assert_eq!(
            validating.activations()[0].state(),
            ConstraintActivationState::Validating
        );
        assert_ne!(
            validating.activations()[0].fingerprint(),
            activation.fingerprint(),
        );

        let promoted = validating
            .with_promoted_activation(activation.id())
            .expect("validated check should promote");
        assert!(promoted.activations().is_empty());
        assert_eq!(promoted.constraints()[0].id(), activation.id());
        assert_eq!(promoted.constraints()[0].name(), activation.name());
        assert_eq!(promoted.allocator(), allocator);
    }

    #[test]
    fn aborted_activation_retires_its_identity_without_reuse() {
        let catalog = AcceptedConstraintCatalog::default()
            .with_added_check_activation(
                "first".to_string(),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
                AcceptedSchemaFingerprint::new([0xA5; 32]),
                7,
            )
            .expect("activation should reserve identity");
        let retired = catalog.activations()[0].id();
        let aborted = catalog
            .with_aborted_activation(retired)
            .expect("activation should abort");
        let next = aborted
            .with_added_check_activation(
                "second".to_string(),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
                AcceptedSchemaFingerprint::new([0xB5; 32]),
                8,
            )
            .expect("new activation should reserve a new identity");
        assert!(next.activations()[0].id() > retired);
    }

    #[test]
    fn removed_generated_check_retires_its_identity_without_reuse() {
        let catalog = AcceptedConstraintCatalog::default()
            .with_added_check(
                "first".to_string(),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
            )
            .expect("generated check should admit");
        let retired = catalog.constraints()[0].id();
        let allocator = catalog.allocator();
        let removed = catalog
            .with_removed_generated_constraint(retired)
            .expect("generated check should be removable");

        assert!(removed.constraints().is_empty());
        assert_eq!(removed.allocator(), allocator);

        let next = removed
            .with_added_check(
                "second".to_string(),
                ConstraintOrigin::Generated,
                AcceptedCheckExprV1::True,
            )
            .expect("new generated check should admit");
        assert!(next.constraints()[0].id() > retired);

        let sql_owned = AcceptedConstraintCatalog::default()
            .with_added_check(
                "sql_owned".to_string(),
                ConstraintOrigin::SqlDdl,
                AcceptedCheckExprV1::True,
            )
            .expect("SQL-owned check should admit");
        let sql_owned_id = sql_owned.constraints()[0].id();
        assert_eq!(
            sql_owned.with_removed_generated_constraint(sql_owned_id),
            Err(AcceptedConstraintCatalogError::OwnerMismatch),
            "generated removal must not take ownership of SQL DDL state",
        );
    }
}
