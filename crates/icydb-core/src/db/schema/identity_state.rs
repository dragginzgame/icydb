//! Module: db::schema::identity_state
//! Responsibility: bounded current-form identity control state and statement-local allocation.
//! Does not own: range commit records or schema publication ordering.
//! Boundary: accepted identity owner -> control-state bytes and tentative generated values.

use crate::{
    db::{
        codec::{ByteDecodeError, ByteReader},
        database_format::{crc32c, verify_crc32c_trailer},
        integrity::DatabaseIncarnationId,
        schema::{AcceptedFieldKind, FieldId},
    },
    error::InternalError,
    types::EntityTag,
    value::Value,
};
use std::collections::BTreeMap;

/// Lifetime bound across active and retired identity owners in one database.
pub(in crate::db) const MAX_IDENTITY_STATE_RECORDS_PER_DATABASE: usize = 65_536;

const IDENTITY_STATE_MAGIC: &[u8; 8] = b"ICYIDST!";
const IDENTITY_STATE_VERSION: u8 = 1;
const IDENTITY_STATE_LIFECYCLE_ACTIVE: u8 = 1;
const IDENTITY_STATE_LIFECYCLE_RETIRED: u8 = 2;
const IDENTITY_STATE_KIND_NAT8: u8 = 1;
const IDENTITY_STATE_KIND_NAT16: u8 = 2;
const IDENTITY_STATE_KIND_NAT32: u8 = 3;
const IDENTITY_STATE_KIND_NAT64: u8 = 4;
const IDENTITY_STATE_KIND_NAT128: u8 = 5;
const IDENTITY_STATE_ADVANCE_ABSENT: u8 = 0;
const IDENTITY_STATE_ADVANCE_PRESENT: u8 = 1;
const IDENTITY_ADVANCE_ID_BYTES: usize = 16 + 16 + 8 + 4;
const IDENTITY_STATE_BODY_BYTES: usize =
    8 + 1 + 16 + 8 + 4 + 1 + 1 + 16 + 1 + IDENTITY_ADVANCE_ID_BYTES;
pub(in crate::db::schema) const IDENTITY_STATE_RECORD_BYTES: usize =
    IDENTITY_STATE_BODY_BYTES + size_of::<u32>();

/// Accepted schema key for one identity allocation domain.
/// Schema publication may rebind the field ID through accepted source lineage.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IdentityStateOwner {
    database_incarnation_id: DatabaseIncarnationId,
    entity_tag: EntityTag,
    field_id: FieldId,
}

impl IdentityStateOwner {
    pub(in crate::db) fn try_new(
        database_incarnation_id: DatabaseIncarnationId,
        entity_tag: EntityTag,
        field_id: FieldId,
    ) -> Result<Self, InternalError> {
        if entity_tag.value() == 0 || field_id.get() == 0 {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(Self {
            database_incarnation_id,
            entity_tag,
            field_id,
        })
    }

    #[must_use]
    pub(in crate::db) const fn database_incarnation_id(self) -> DatabaseIncarnationId {
        self.database_incarnation_id
    }

    #[must_use]
    pub(in crate::db) const fn entity_tag(self) -> EntityTag {
        self.entity_tag
    }

    #[must_use]
    pub(in crate::db) const fn field_id(self) -> FieldId {
        self.field_id
    }
}

/// Active accepted owner or retained allocation-history tombstone.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IdentityStateLifecycle {
    Active,
    Retired,
}

/// Exact journal-envelope identity of one applied range advance.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IdentityAdvanceId {
    commit_marker_id: [u8; 16],
    journal_batch_id: [u8; 16],
    journal_sequence: u64,
    record_ordinal: u32,
}

impl IdentityAdvanceId {
    pub(in crate::db) fn try_new(
        commit_marker_id: [u8; 16],
        journal_batch_id: [u8; 16],
        journal_sequence: u64,
        record_ordinal: u32,
    ) -> Result<Self, InternalError> {
        if commit_marker_id == [0; 16] || journal_batch_id == [0; 16] {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(Self {
            commit_marker_id,
            journal_batch_id,
            journal_sequence,
            record_ordinal,
        })
    }

    #[must_use]
    pub(in crate::db) const fn commit_marker_id(self) -> [u8; 16] {
        self.commit_marker_id
    }

    #[must_use]
    pub(in crate::db) const fn journal_batch_id(self) -> [u8; 16] {
        self.journal_batch_id
    }

    #[must_use]
    pub(in crate::db) const fn journal_sequence(self) -> u64 {
        self.journal_sequence
    }

    #[must_use]
    pub(in crate::db) const fn record_ordinal(self) -> u32 {
        self.record_ordinal
    }
}

/// One contiguous marker-owned advance for an immutable identity owner.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct IdentityRangeAdvance {
    owner: IdentityStateOwner,
    expected_high_water: u128,
    new_high_water: u128,
    allocation_count: u32,
}

impl IdentityRangeAdvance {
    pub(in crate::db) fn try_new(
        owner: IdentityStateOwner,
        expected_high_water: u128,
        new_high_water: u128,
        allocation_count: u32,
    ) -> Result<Self, InternalError> {
        if allocation_count == 0
            || expected_high_water.checked_add(u128::from(allocation_count)) != Some(new_high_water)
        {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(Self {
            owner,
            expected_high_water,
            new_high_water,
            allocation_count,
        })
    }

    #[must_use]
    pub(in crate::db) const fn owner(self) -> IdentityStateOwner {
        self.owner
    }

    #[must_use]
    pub(in crate::db) const fn expected_high_water(self) -> u128 {
        self.expected_high_water
    }

    #[must_use]
    pub(in crate::db) const fn new_high_water(self) -> u128 {
        self.new_high_water
    }

    #[must_use]
    pub(in crate::db) const fn allocation_count(self) -> u32 {
        self.allocation_count
    }
}

/// Current-form operational state for one accepted identity allocation domain.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IdentityState {
    owner: IdentityStateOwner,
    accepted_kind: AcceptedFieldKind,
    lifecycle: IdentityStateLifecycle,
    materialized_high_water: u128,
    last_applied_advance: Option<IdentityAdvanceId>,
}

/// Marker-derived relationship between committed and materialized state.
///
/// This is never persisted. A durable marker makes `committed_high_water`
/// authoritative while its state record may still contain the expected
/// pre-commit value. Once marker recovery completes, both values are equal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum IdentityRangeCommitState {
    PendingMaterialization {
        materialized_high_water: u128,
        committed_high_water: u128,
    },
    Materialized {
        high_water: u128,
    },
}

impl IdentityRangeCommitState {
    #[must_use]
    pub(in crate::db) const fn materialized_high_water(self) -> u128 {
        match self {
            Self::PendingMaterialization {
                materialized_high_water,
                ..
            } => materialized_high_water,
            Self::Materialized { high_water } => high_water,
        }
    }

    #[must_use]
    pub(in crate::db) const fn committed_high_water(self) -> u128 {
        match self {
            Self::PendingMaterialization {
                committed_high_water,
                ..
            } => committed_high_water,
            Self::Materialized { high_water } => high_water,
        }
    }

    #[must_use]
    pub(in crate::db) const fn is_materialized(self) -> bool {
        matches!(self, Self::Materialized { .. })
    }
}

impl IdentityState {
    pub(in crate::db) fn new_active(
        owner: IdentityStateOwner,
        accepted_kind: AcceptedFieldKind,
    ) -> Result<Self, InternalError> {
        Self::try_new(
            owner,
            accepted_kind,
            IdentityStateLifecycle::Active,
            0,
            None,
        )
    }

    pub(in crate::db) fn try_new(
        owner: IdentityStateOwner,
        accepted_kind: AcceptedFieldKind,
        lifecycle: IdentityStateLifecycle,
        materialized_high_water: u128,
        last_applied_advance: Option<IdentityAdvanceId>,
    ) -> Result<Self, InternalError> {
        let maximum = identity_kind_maximum(&accepted_kind)
            .ok_or_else(InternalError::identity_state_corruption)?;
        if materialized_high_water > maximum
            || (materialized_high_water == 0) != last_applied_advance.is_none()
        {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(Self {
            owner,
            accepted_kind,
            lifecycle,
            materialized_high_water,
            last_applied_advance,
        })
    }

    pub(in crate::db) fn retire(&self) -> Result<Self, InternalError> {
        if self.lifecycle != IdentityStateLifecycle::Active {
            return Err(InternalError::identity_state_corruption());
        }
        Self::try_new(
            self.owner,
            self.accepted_kind.clone(),
            IdentityStateLifecycle::Retired,
            self.materialized_high_water,
            self.last_applied_advance,
        )
    }

    #[must_use]
    pub(in crate::db) const fn owner(&self) -> IdentityStateOwner {
        self.owner
    }

    #[must_use]
    pub(in crate::db) const fn accepted_kind(&self) -> &AcceptedFieldKind {
        &self.accepted_kind
    }

    #[must_use]
    pub(in crate::db) const fn lifecycle(&self) -> IdentityStateLifecycle {
        self.lifecycle
    }

    #[must_use]
    pub(in crate::db) const fn materialized_high_water(&self) -> u128 {
        self.materialized_high_water
    }

    #[must_use]
    pub(in crate::db) const fn last_applied_advance(&self) -> Option<IdentityAdvanceId> {
        self.last_applied_advance
    }

    /// Revalidate one tentative allocation range against this quiescent state.
    pub(in crate::db) fn preflight_range_advance(
        &self,
        range: IdentityRangeAdvance,
    ) -> Result<(), InternalError> {
        if self.lifecycle != IdentityStateLifecycle::Active
            || self.owner != range.owner()
            || range.new_high_water()
                > identity_kind_maximum(&self.accepted_kind)
                    .ok_or_else(InternalError::identity_state_corruption)?
        {
            return Err(InternalError::identity_state_corruption());
        }
        if self.materialized_high_water != range.expected_high_water() {
            return Err(InternalError::identity_state_conflict());
        }
        Ok(())
    }

    pub(in crate::db) fn apply_range_advance(
        &self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<Self, InternalError> {
        if self
            .range_commit_state(range, advance_id)?
            .is_materialized()
        {
            return Ok(self.clone());
        }
        Self::try_new(
            self.owner,
            self.accepted_kind.clone(),
            self.lifecycle,
            range.new_high_water(),
            Some(advance_id),
        )
    }

    /// Resolve one marker-owned range against the possibly lagging state
    /// record without treating the committed marker value as materialized.
    pub(in crate::db) fn range_commit_state(
        &self,
        range: IdentityRangeAdvance,
        advance_id: IdentityAdvanceId,
    ) -> Result<IdentityRangeCommitState, InternalError> {
        if self.lifecycle != IdentityStateLifecycle::Active
            || self.owner != range.owner()
            || range.new_high_water()
                > identity_kind_maximum(&self.accepted_kind)
                    .ok_or_else(InternalError::identity_state_corruption)?
        {
            return Err(InternalError::identity_state_corruption());
        }
        if self.materialized_high_water == range.new_high_water()
            && self.last_applied_advance == Some(advance_id)
        {
            return Ok(IdentityRangeCommitState::Materialized {
                high_water: range.new_high_water(),
            });
        }
        if self.materialized_high_water != range.expected_high_water()
            || self.last_applied_advance == Some(advance_id)
        {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(IdentityRangeCommitState::PendingMaterialization {
            materialized_high_water: self.materialized_high_water,
            committed_high_water: range.new_high_water(),
        })
    }
}

/// One tentative generated value bound to its accepted owner and input order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedIdentityAllocation {
    owner: IdentityStateOwner,
    field_slot: usize,
    input_ordinal: u32,
    value: Value,
}

impl AcceptedIdentityAllocation {
    #[must_use]
    pub(in crate::db) const fn owner(&self) -> IdentityStateOwner {
        self.owner
    }

    #[must_use]
    pub(in crate::db) const fn field_slot(&self) -> usize {
        self.field_slot
    }

    #[must_use]
    pub(in crate::db) const fn input_ordinal(&self) -> u32 {
        self.input_ordinal
    }

    #[must_use]
    pub(in crate::db) const fn value(&self) -> &Value {
        &self.value
    }
}

/// Statement-local cursor over one quiescent active Identity owner.
#[derive(Debug)]
pub(in crate::db) struct IdentityStatementCursor {
    owner: IdentityStateOwner,
    accepted_kind: AcceptedFieldKind,
    expected_high_water: u128,
    tentative_high_water: u128,
    allocation_count: u32,
}

impl IdentityStatementCursor {
    pub(in crate::db) fn from_active_state(state: &IdentityState) -> Result<Self, InternalError> {
        if state.lifecycle() != IdentityStateLifecycle::Active {
            return Err(InternalError::identity_state_corruption());
        }
        Ok(Self {
            owner: state.owner(),
            accepted_kind: state.accepted_kind().clone(),
            expected_high_water: state.materialized_high_water(),
            tentative_high_water: state.materialized_high_water(),
            allocation_count: 0,
        })
    }

    pub(in crate::db) fn allocate(
        &mut self,
        field_slot: usize,
        input_ordinal: u32,
    ) -> Result<AcceptedIdentityAllocation, InternalError> {
        if input_ordinal != self.allocation_count {
            return Err(InternalError::store_invariant());
        }
        let next = self
            .tentative_high_water
            .checked_add(1)
            .ok_or_else(InternalError::identity_exhausted)?;
        let value = identity_runtime_value(&self.accepted_kind, next)?;
        self.allocation_count = self
            .allocation_count
            .checked_add(1)
            .ok_or_else(InternalError::identity_candidate_count_exhausted)?;
        self.tentative_high_water = next;
        Ok(AcceptedIdentityAllocation {
            owner: self.owner,
            field_slot,
            input_ordinal,
            value,
        })
    }

    #[must_use]
    pub(in crate::db) const fn has_allocations(&self) -> bool {
        self.tentative_high_water != self.expected_high_water
    }

    pub(in crate::db) fn into_range_advance(
        self,
    ) -> Result<Option<IdentityRangeAdvance>, InternalError> {
        if !self.has_allocations() {
            return Ok(None);
        }
        IdentityRangeAdvance::try_new(
            self.owner,
            self.expected_high_water,
            self.tentative_high_water,
            self.allocation_count,
        )
        .map(Some)
    }
}

/// Convert one canonical u128 identity into its exact runtime scalar shape.
pub(in crate::db) fn identity_runtime_value(
    kind: &AcceptedFieldKind,
    value: u128,
) -> Result<Value, InternalError> {
    let maximum =
        identity_kind_maximum(kind).ok_or_else(InternalError::identity_state_corruption)?;
    if value == 0 || value > maximum {
        return Err(InternalError::identity_exhausted());
    }
    match kind {
        AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => u64::try_from(value)
            .map(Value::Nat64)
            .map_err(|_| InternalError::identity_exhausted()),
        AcceptedFieldKind::Nat128 => Ok(Value::Nat128(value)),
        _ => Err(InternalError::identity_state_corruption()),
    }
}

pub(in crate::db::schema) type IdentityStateInventory =
    BTreeMap<(EntityTag, FieldId), IdentityState>;

pub(in crate::db::schema) struct IdentityStateTransition {
    updates: Vec<IdentityState>,
    removals: Vec<IdentityStateOwner>,
    projected_inventory: IdentityStateInventory,
}

impl IdentityStateTransition {
    #[must_use]
    pub(in crate::db::schema) const fn is_empty(&self) -> bool {
        self.updates.is_empty() && self.removals.is_empty()
    }

    pub(in crate::db::schema) fn into_effects(
        self,
    ) -> (Vec<IdentityState>, Vec<IdentityStateOwner>) {
        (self.updates, self.removals)
    }

    #[must_use]
    pub(in crate::db::schema) fn projected_inventory_len(&self) -> usize {
        self.projected_inventory.len()
    }

    pub(in crate::db::schema) fn into_projected_inventory(self) -> IdentityStateInventory {
        self.projected_inventory
    }
}

pub(in crate::db::schema) fn prepare_identity_state_transition(
    incarnation: DatabaseIncarnationId,
    current: Option<&crate::db::schema::AcceptedSchemaRevisionBundle>,
    candidate: &crate::db::schema::AcceptedSchemaRevisionBundle,
    inventory: IdentityStateInventory,
) -> Result<IdentityStateTransition, InternalError> {
    let mut current_declarations = current
        .map(identity_declarations)
        .transpose()?
        .unwrap_or_default();
    let candidate_declarations = identity_declarations(candidate)?;

    if inventory.iter().any(|(key, state)| {
        state.owner().database_incarnation_id() != incarnation
            || state.owner().entity_tag() != key.0
            || state.owner().field_id() != key.1
    }) {
        return Err(InternalError::identity_state_corruption());
    }
    if let Some(current) = current {
        validate_identity_state_closure(current, &inventory)?;
    } else if inventory
        .values()
        .any(|state| state.lifecycle() == IdentityStateLifecycle::Active)
    {
        return Err(InternalError::identity_state_corruption());
    }
    let mut transition = IdentityStateTransition {
        updates: Vec::new(),
        removals: Vec::new(),
        projected_inventory: inventory,
    };
    if let Some(current) = current {
        rebind_retained_identity_states(
            current,
            candidate,
            &mut current_declarations,
            &candidate_declarations,
            &mut transition,
        )?;
    }
    let IdentityStateTransition {
        updates,
        projected_inventory,
        ..
    } = &mut transition;
    for (key, current_kind) in &current_declarations {
        let state = projected_inventory
            .get(key)
            .ok_or_else(InternalError::identity_state_corruption)?;
        if state.lifecycle() != IdentityStateLifecycle::Active
            || state.accepted_kind() != current_kind
        {
            return Err(InternalError::identity_state_corruption());
        }
        match candidate_declarations.get(key) {
            Some(candidate_kind) if candidate_kind == current_kind => {}
            Some(_) => return Err(InternalError::identity_state_corruption()),
            None => {
                let retired = state.retire()?;
                projected_inventory.insert(*key, retired.clone());
                updates.push(retired);
            }
        }
    }

    for ((entity_tag, field_id), kind) in &candidate_declarations {
        if current_declarations.contains_key(&(*entity_tag, *field_id)) {
            continue;
        }
        if projected_inventory.contains_key(&(*entity_tag, *field_id)) {
            return Err(InternalError::identity_state_corruption());
        }
        if projected_inventory.len() >= MAX_IDENTITY_STATE_RECORDS_PER_DATABASE {
            return Err(InternalError::identity_state_capacity_exhausted());
        }
        let owner = IdentityStateOwner::try_new(incarnation, *entity_tag, *field_id)?;
        let state = IdentityState::new_active(owner, kind.clone())?;
        projected_inventory.insert((*entity_tag, *field_id), state.clone());
        updates.push(state);
    }

    Ok(transition)
}

// Source bindings prove that a changed numeric field ID is the same logical
// allocator. Move its complete state, including replay identity and exhaustion;
// retirement is reserved for an actually removed owner.
fn rebind_retained_identity_states(
    current: &crate::db::schema::AcceptedSchemaRevisionBundle,
    candidate: &crate::db::schema::AcceptedSchemaRevisionBundle,
    current_declarations: &mut BTreeMap<(EntityTag, FieldId), AcceptedFieldKind>,
    candidate_declarations: &BTreeMap<(EntityTag, FieldId), AcceptedFieldKind>,
    transition: &mut IdentityStateTransition,
) -> Result<(), InternalError> {
    for (key, kind) in std::mem::take(current_declarations) {
        if candidate_declarations.contains_key(&key)
            || candidate_declarations
                .range((key.0, FieldId::new(1))..)
                .next()
                .is_none_or(|((entity, _), _)| *entity != key.0)
        {
            current_declarations.insert(key, kind);
            continue;
        }
        let after = current
            .source_bindings()
            .field_source(key.0, key.1)
            .and_then(|source| candidate.source_bindings().field(key.0, source))
            .ok_or_else(InternalError::store_unsupported)?;
        let after_key = (key.0, after);
        if candidate_declarations.get(&after_key) != Some(&kind)
            || transition.projected_inventory.contains_key(&after_key)
        {
            return Err(InternalError::identity_state_corruption());
        }
        let mut state = transition
            .projected_inventory
            .remove(&key)
            .ok_or_else(InternalError::identity_state_corruption)?;
        transition.removals.push(state.owner());
        state.owner.field_id = after;
        transition.updates.push(state.clone());
        transition.projected_inventory.insert(after_key, state);
        current_declarations.insert(after_key, kind);
    }
    Ok(())
}

pub(in crate::db::schema) fn validate_identity_state_closure(
    bundle: &crate::db::schema::AcceptedSchemaRevisionBundle,
    inventory: &IdentityStateInventory,
) -> Result<(), InternalError> {
    let declarations = identity_declarations(bundle)?;
    for (key, state) in inventory {
        if state.owner().entity_tag() != key.0 || state.owner().field_id() != key.1 {
            return Err(InternalError::identity_state_corruption());
        }
        match state.lifecycle() {
            IdentityStateLifecycle::Active
                if declarations.get(key) == Some(state.accepted_kind()) => {}
            IdentityStateLifecycle::Retired if !declarations.contains_key(key) => {}
            IdentityStateLifecycle::Active | IdentityStateLifecycle::Retired => {
                return Err(InternalError::identity_state_corruption());
            }
        }
    }
    for key in declarations.keys() {
        if inventory
            .get(key)
            .is_none_or(|state| state.lifecycle() != IdentityStateLifecycle::Active)
        {
            return Err(InternalError::identity_state_corruption());
        }
    }
    Ok(())
}

fn identity_declarations(
    bundle: &crate::db::schema::AcceptedSchemaRevisionBundle,
) -> Result<BTreeMap<(EntityTag, FieldId), AcceptedFieldKind>, InternalError> {
    let mut declarations = BTreeMap::new();
    for (entity_tag, snapshot) in bundle.entity_snapshots() {
        for field in snapshot.fields().iter().filter(|field| {
            field.write_policy().insert_generation()
                == Some(crate::db::schema::FieldInsertGeneration::Identity)
        }) {
            if identity_kind_maximum(field.kind()).is_none()
                || declarations
                    .insert((*entity_tag, field.id()), field.kind().clone())
                    .is_some()
            {
                return Err(InternalError::identity_state_corruption());
            }
        }
    }
    Ok(declarations)
}

pub(in crate::db) fn identity_kind_maximum(kind: &AcceptedFieldKind) -> Option<u128> {
    match kind {
        AcceptedFieldKind::Nat8 => Some(u128::from(u8::MAX)),
        AcceptedFieldKind::Nat16 => Some(u128::from(u16::MAX)),
        AcceptedFieldKind::Nat32 => Some(u128::from(u32::MAX)),
        AcceptedFieldKind::Nat64 => Some(u128::from(u64::MAX)),
        AcceptedFieldKind::Nat128 => Some(u128::MAX),
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Composite { .. }
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::Set(_)
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => None,
    }
}

pub(in crate::db::schema) fn encode_identity_state(
    state: &IdentityState,
) -> Result<Vec<u8>, InternalError> {
    let kind = encode_identity_kind(state.accepted_kind())?;
    let mut bytes = Vec::with_capacity(IDENTITY_STATE_RECORD_BYTES);
    bytes.extend_from_slice(IDENTITY_STATE_MAGIC);
    bytes.push(IDENTITY_STATE_VERSION);
    bytes.extend_from_slice(&state.owner().database_incarnation_id().to_bytes());
    bytes.extend_from_slice(&state.owner().entity_tag().value().to_be_bytes());
    bytes.extend_from_slice(&state.owner().field_id().get().to_be_bytes());
    bytes.push(kind);
    bytes.push(match state.lifecycle() {
        IdentityStateLifecycle::Active => IDENTITY_STATE_LIFECYCLE_ACTIVE,
        IdentityStateLifecycle::Retired => IDENTITY_STATE_LIFECYCLE_RETIRED,
    });
    bytes.extend_from_slice(&state.materialized_high_water().to_be_bytes());
    match state.last_applied_advance() {
        None => {
            bytes.push(IDENTITY_STATE_ADVANCE_ABSENT);
            bytes.resize(bytes.len().saturating_add(IDENTITY_ADVANCE_ID_BYTES), 0);
        }
        Some(advance) => {
            bytes.push(IDENTITY_STATE_ADVANCE_PRESENT);
            bytes.extend_from_slice(&advance.commit_marker_id());
            bytes.extend_from_slice(&advance.journal_batch_id());
            bytes.extend_from_slice(&advance.journal_sequence().to_be_bytes());
            bytes.extend_from_slice(&advance.record_ordinal().to_be_bytes());
        }
    }
    if bytes.len() != IDENTITY_STATE_BODY_BYTES {
        return Err(InternalError::store_invariant());
    }
    bytes.extend_from_slice(&crc32c(&bytes).to_be_bytes());
    Ok(bytes)
}

pub(in crate::db::schema) fn decode_identity_state(
    bytes: &[u8],
) -> Result<IdentityState, InternalError> {
    if bytes.len() != IDENTITY_STATE_RECORD_BYTES {
        return Err(InternalError::identity_state_corruption());
    }
    let body = verify_crc32c_trailer(bytes).ok_or_else(InternalError::identity_state_corruption)?;

    let mut reader = ByteReader::new(body);
    if reader.read_array::<8>().map_err(primitive_decode_error)? != *IDENTITY_STATE_MAGIC
        || reader.read_u8().map_err(primitive_decode_error)? != IDENTITY_STATE_VERSION
    {
        return Err(InternalError::identity_state_corruption());
    }
    let database_incarnation_id = DatabaseIncarnationId::try_from_bytes(
        reader.read_array::<16>().map_err(primitive_decode_error)?,
    )
    .map_err(|_| InternalError::identity_state_corruption())?;
    let entity_tag = EntityTag::new(reader.read_u64().map_err(primitive_decode_error)?);
    let field_id = FieldId::new(reader.read_u32().map_err(primitive_decode_error)?);
    let accepted_kind = decode_identity_kind(reader.read_u8().map_err(primitive_decode_error)?)?;
    let lifecycle = match reader.read_u8().map_err(primitive_decode_error)? {
        IDENTITY_STATE_LIFECYCLE_ACTIVE => IdentityStateLifecycle::Active,
        IDENTITY_STATE_LIFECYCLE_RETIRED => IdentityStateLifecycle::Retired,
        _ => return Err(InternalError::identity_state_corruption()),
    };
    let materialized_high_water = reader.read_u128().map_err(primitive_decode_error)?;
    let advance_tag = reader.read_u8().map_err(primitive_decode_error)?;
    let commit_marker_id = reader.read_array::<16>().map_err(primitive_decode_error)?;
    let journal_batch_id = reader.read_array::<16>().map_err(primitive_decode_error)?;
    let journal_sequence = reader.read_u64().map_err(primitive_decode_error)?;
    let record_ordinal = reader.read_u32().map_err(primitive_decode_error)?;
    reader.finish().map_err(primitive_decode_error)?;
    let last_applied_advance = match advance_tag {
        IDENTITY_STATE_ADVANCE_ABSENT
            if commit_marker_id == [0; 16]
                && journal_batch_id == [0; 16]
                && journal_sequence == 0
                && record_ordinal == 0 =>
        {
            None
        }
        IDENTITY_STATE_ADVANCE_PRESENT => Some(IdentityAdvanceId::try_new(
            commit_marker_id,
            journal_batch_id,
            journal_sequence,
            record_ordinal,
        )?),
        _ => {
            return Err(InternalError::identity_state_corruption());
        }
    };
    let owner = IdentityStateOwner::try_new(database_incarnation_id, entity_tag, field_id)?;
    IdentityState::try_new(
        owner,
        accepted_kind,
        lifecycle,
        materialized_high_water,
        last_applied_advance,
    )
}

fn encode_identity_kind(kind: &AcceptedFieldKind) -> Result<u8, InternalError> {
    match kind {
        AcceptedFieldKind::Nat8 => Ok(IDENTITY_STATE_KIND_NAT8),
        AcceptedFieldKind::Nat16 => Ok(IDENTITY_STATE_KIND_NAT16),
        AcceptedFieldKind::Nat32 => Ok(IDENTITY_STATE_KIND_NAT32),
        AcceptedFieldKind::Nat64 => Ok(IDENTITY_STATE_KIND_NAT64),
        AcceptedFieldKind::Nat128 => Ok(IDENTITY_STATE_KIND_NAT128),
        _ => Err(InternalError::store_invariant()),
    }
}

fn decode_identity_kind(tag: u8) -> Result<AcceptedFieldKind, InternalError> {
    match tag {
        IDENTITY_STATE_KIND_NAT8 => Ok(AcceptedFieldKind::Nat8),
        IDENTITY_STATE_KIND_NAT16 => Ok(AcceptedFieldKind::Nat16),
        IDENTITY_STATE_KIND_NAT32 => Ok(AcceptedFieldKind::Nat32),
        IDENTITY_STATE_KIND_NAT64 => Ok(AcceptedFieldKind::Nat64),
        IDENTITY_STATE_KIND_NAT128 => Ok(AcceptedFieldKind::Nat128),
        _ => Err(InternalError::identity_state_corruption()),
    }
}

// This codec retains its domain classification for every primitive failure.
fn primitive_decode_error(_: ByteDecodeError) -> InternalError {
    InternalError::identity_state_corruption()
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod helper_tests;
