//! Module: db::schema::identity_state
//! Responsibility: bounded current-form identity control state and statement-local allocation.
//! Does not own: range commit records or schema publication ordering.
//! Boundary: accepted identity owner -> control-state bytes and tentative generated values.

use crate::{
    db::{
        database_format::crc32c,
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

/// Immutable accepted owner of one identity allocation domain.
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

/// Current-form operational state for one immutable identity owner.
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
    #[cfg(test)]
    pub(in crate::db) const fn owner(&self) -> IdentityStateOwner {
        self.owner
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn expected_high_water(&self) -> u128 {
        self.expected_high_water
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn tentative_high_water(&self) -> u128 {
        self.tentative_high_water
    }

    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn allocation_count(&self) -> u32 {
        self.allocation_count
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
    projected_inventory: IdentityStateInventory,
}

impl IdentityStateTransition {
    #[must_use]
    pub(in crate::db::schema) const fn is_empty(&self) -> bool {
        self.updates.is_empty()
    }

    pub(in crate::db::schema) fn into_updates(self) -> Vec<IdentityState> {
        self.updates
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
    let current_declarations = current
        .map(identity_declarations)
        .transpose()?
        .unwrap_or_default();
    let candidate_declarations = identity_declarations(candidate)?;

    for (key, state) in &inventory {
        if state.owner().database_incarnation_id() != incarnation
            || state.owner().entity_tag() != key.0
            || state.owner().field_id() != key.1
        {
            return Err(InternalError::identity_state_corruption());
        }
        match state.lifecycle() {
            IdentityStateLifecycle::Active => {
                let Some(kind) = current_declarations.get(key) else {
                    return Err(InternalError::identity_state_corruption());
                };
                if kind != state.accepted_kind() {
                    return Err(InternalError::identity_state_corruption());
                }
            }
            IdentityStateLifecycle::Retired => {
                if current_declarations.contains_key(key) {
                    return Err(InternalError::identity_state_corruption());
                }
            }
        }
    }

    let mut updates = Vec::new();
    let mut projected_inventory = inventory;
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

    Ok(IdentityStateTransition {
        updates,
        projected_inventory,
    })
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
        | AcceptedFieldKind::IntBig { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::NatBig { .. }
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
    let body = bytes
        .get(..IDENTITY_STATE_BODY_BYTES)
        .ok_or_else(InternalError::identity_state_corruption)?;
    let checksum_bytes = bytes
        .get(IDENTITY_STATE_BODY_BYTES..)
        .ok_or_else(InternalError::identity_state_corruption)?;
    let expected_checksum = u32::from_be_bytes(
        checksum_bytes
            .try_into()
            .map_err(|_| InternalError::identity_state_corruption())?,
    );
    if crc32c(body) != expected_checksum {
        return Err(InternalError::identity_state_corruption());
    }

    let mut reader = IdentityStateReader::new(body);
    if reader.read_array::<8>()? != *IDENTITY_STATE_MAGIC
        || reader.read_u8()? != IDENTITY_STATE_VERSION
    {
        return Err(InternalError::identity_state_corruption());
    }
    let database_incarnation_id = DatabaseIncarnationId::try_from_bytes(reader.read_array::<16>()?)
        .map_err(|_| InternalError::identity_state_corruption())?;
    let entity_tag = EntityTag::new(reader.read_u64()?);
    let field_id = FieldId::new(reader.read_u32()?);
    let accepted_kind = decode_identity_kind(reader.read_u8()?)?;
    let lifecycle = match reader.read_u8()? {
        IDENTITY_STATE_LIFECYCLE_ACTIVE => IdentityStateLifecycle::Active,
        IDENTITY_STATE_LIFECYCLE_RETIRED => IdentityStateLifecycle::Retired,
        _ => return Err(InternalError::identity_state_corruption()),
    };
    let materialized_high_water = reader.read_u128()?;
    let advance_tag = reader.read_u8()?;
    let commit_marker_id = reader.read_array::<16>()?;
    let journal_batch_id = reader.read_array::<16>()?;
    let journal_sequence = reader.read_u64()?;
    let record_ordinal = reader.read_u32()?;
    if !reader.is_exhausted() {
        return Err(InternalError::identity_state_corruption());
    }
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

struct IdentityStateReader<'a> {
    bytes: &'a [u8],
    cursor: usize,
}

impl<'a> IdentityStateReader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, cursor: 0 }
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N], InternalError> {
        let end = self
            .cursor
            .checked_add(N)
            .ok_or_else(InternalError::identity_state_corruption)?;
        let bytes = self
            .bytes
            .get(self.cursor..end)
            .ok_or_else(InternalError::identity_state_corruption)?;
        self.cursor = end;
        bytes
            .try_into()
            .map_err(|_| InternalError::identity_state_corruption())
    }

    fn read_u8(&mut self) -> Result<u8, InternalError> {
        Ok(self.read_array::<1>()?[0])
    }

    fn read_u32(&mut self) -> Result<u32, InternalError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    fn read_u64(&mut self) -> Result<u64, InternalError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    fn read_u128(&mut self) -> Result<u128, InternalError> {
        Ok(u128::from_be_bytes(self.read_array()?))
    }

    const fn is_exhausted(&self) -> bool {
        self.cursor == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{ErrorClass, ErrorOrigin};

    fn owner() -> IdentityStateOwner {
        IdentityStateOwner::try_new(
            DatabaseIncarnationId::for_tests(0x21),
            EntityTag::new(7),
            FieldId::new(3),
        )
        .expect("identity owner should admit")
    }

    fn advance() -> IdentityAdvanceId {
        IdentityAdvanceId::try_new([0x31; 16], [0x41; 16], 19, 5)
            .expect("nonzero advance identity should admit")
    }

    fn assert_identity_corruption(bytes: &[u8]) {
        let error = decode_identity_state(bytes).expect_err("malformed state must reject");
        assert_eq!(error.class(), ErrorClass::Corruption);
        assert_eq!(error.origin(), ErrorOrigin::Identity);
    }

    #[test]
    fn identity_state_codec_is_fixed_width_checksummed_and_canonical() {
        let state = IdentityState::try_new(
            owner(),
            AcceptedFieldKind::Nat128,
            IdentityStateLifecycle::Retired,
            u128::MAX,
            Some(advance()),
        )
        .expect("max-width retired state should admit");
        let encoded = encode_identity_state(&state).expect("identity state should encode");

        assert_eq!(encoded.len(), IDENTITY_STATE_RECORD_BYTES);
        assert_eq!(
            decode_identity_state(&encoded).expect("identity state should decode"),
            state,
        );
        assert_eq!(
            encoded
                .get(39..55)
                .expect("high-water bytes should occupy the fixed field"),
            &u128::MAX.to_be_bytes(),
        );
    }

    #[test]
    fn identity_state_codec_rejects_bad_envelope_and_noncanonical_absent_advance() {
        let state = IdentityState::new_active(owner(), AcceptedFieldKind::Nat64)
            .expect("zero state should admit");
        let encoded = encode_identity_state(&state).expect("identity state should encode");

        assert_identity_corruption(&encoded[..encoded.len().saturating_sub(1)]);

        for offset in [0, 8, 37, 38, 100] {
            let mut malformed = encoded.clone();
            malformed[offset] ^= 0xFF;
            assert_identity_corruption(&malformed);
        }

        let mut absent_with_payload = encoded;
        absent_with_payload[56] = 1;
        let checksum = crc32c(&absent_with_payload[..IDENTITY_STATE_BODY_BYTES]);
        absent_with_payload[IDENTITY_STATE_BODY_BYTES..].copy_from_slice(&checksum.to_be_bytes());
        assert_identity_corruption(&absent_with_payload);

        let mut present_without_identity =
            encode_identity_state(&state).expect("identity state should encode");
        present_without_identity[55] = IDENTITY_STATE_ADVANCE_PRESENT;
        let checksum = crc32c(&present_without_identity[..IDENTITY_STATE_BODY_BYTES]);
        present_without_identity[IDENTITY_STATE_BODY_BYTES..]
            .copy_from_slice(&checksum.to_be_bytes());
        assert_identity_corruption(&present_without_identity);
    }

    #[test]
    fn identity_state_enforces_exact_unsigned_width_bounds() {
        for (kind, maximum) in [
            (AcceptedFieldKind::Nat8, u128::from(u8::MAX)),
            (AcceptedFieldKind::Nat16, u128::from(u16::MAX)),
            (AcceptedFieldKind::Nat32, u128::from(u32::MAX)),
            (AcceptedFieldKind::Nat64, u128::from(u64::MAX)),
            (AcceptedFieldKind::Nat128, u128::MAX),
        ] {
            let state = IdentityState::try_new(
                owner(),
                kind.clone(),
                IdentityStateLifecycle::Active,
                maximum,
                Some(advance()),
            )
            .expect("exact kind maximum should admit");
            assert_eq!(
                decode_identity_state(
                    &encode_identity_state(&state).expect("bounded state should encode")
                )
                .expect("bounded state should decode"),
                state,
            );

            if let Some(overflow) = maximum.checked_add(1) {
                assert!(
                    IdentityState::try_new(
                        owner(),
                        kind,
                        IdentityStateLifecycle::Active,
                        overflow,
                        None,
                    )
                    .is_err()
                );
            }
        }
    }

    #[test]
    fn identity_state_rejects_non_identity_kind_and_zero_owner_components() {
        assert!(
            IdentityState::new_active(owner(), AcceptedFieldKind::Int64).is_err(),
            "signed state must reject",
        );
        assert!(
            IdentityStateOwner::try_new(
                DatabaseIncarnationId::for_tests(0x21),
                EntityTag::new(0),
                FieldId::new(3),
            )
            .is_err(),
        );
        assert!(
            IdentityStateOwner::try_new(
                DatabaseIncarnationId::for_tests(0x21),
                EntityTag::new(7),
                FieldId::new(0),
            )
            .is_err(),
        );
        assert!(
            IdentityState::try_new(
                owner(),
                AcceptedFieldKind::Nat64,
                IdentityStateLifecycle::Active,
                1,
                None,
            )
            .is_err(),
            "nonzero materialized state requires exact applied-advance evidence",
        );
        assert!(
            IdentityState::try_new(
                owner(),
                AcceptedFieldKind::Nat64,
                IdentityStateLifecycle::Active,
                0,
                Some(advance()),
            )
            .is_err(),
            "zero materialized state cannot claim an applied advance",
        );
    }

    #[test]
    fn statement_cursor_allocates_exact_runtime_shapes_in_input_order() {
        for (kind, first, second) in [
            (AcceptedFieldKind::Nat8, Value::Nat64(1), Value::Nat64(2)),
            (AcceptedFieldKind::Nat16, Value::Nat64(1), Value::Nat64(2)),
            (AcceptedFieldKind::Nat32, Value::Nat64(1), Value::Nat64(2)),
            (AcceptedFieldKind::Nat64, Value::Nat64(1), Value::Nat64(2)),
            (
                AcceptedFieldKind::Nat128,
                Value::Nat128(1),
                Value::Nat128(2),
            ),
        ] {
            let state =
                IdentityState::new_active(owner(), kind).expect("zero active state should admit");
            let mut cursor = IdentityStatementCursor::from_active_state(&state)
                .expect("active state should open a cursor");

            let allocation_0 = cursor
                .allocate(4, 0)
                .expect("first allocation should succeed");
            let allocation_1 = cursor
                .allocate(4, 1)
                .expect("second allocation should succeed");

            assert_eq!(allocation_0.owner(), owner());
            assert_eq!(allocation_0.field_slot(), 4);
            assert_eq!(allocation_0.input_ordinal(), 0);
            assert_eq!(allocation_0.value(), &first);
            assert_eq!(allocation_1.input_ordinal(), 1);
            assert_eq!(allocation_1.value(), &second);
            assert_eq!(cursor.owner(), owner());
            assert_eq!(cursor.expected_high_water(), 0);
            assert_eq!(cursor.tentative_high_water(), 2);
            assert_eq!(cursor.allocation_count(), 2);
            assert!(cursor.has_allocations());
        }
    }

    #[test]
    fn statement_cursor_rejects_exhaustion_and_order_without_advancing() {
        for (kind, maximum) in [
            (AcceptedFieldKind::Nat8, u128::from(u8::MAX)),
            (AcceptedFieldKind::Nat16, u128::from(u16::MAX)),
            (AcceptedFieldKind::Nat32, u128::from(u32::MAX)),
            (AcceptedFieldKind::Nat64, u128::from(u64::MAX)),
            (AcceptedFieldKind::Nat128, u128::MAX),
        ] {
            let state = IdentityState::try_new(
                owner(),
                kind,
                IdentityStateLifecycle::Active,
                maximum,
                Some(advance()),
            )
            .expect("exact maximum should admit");
            let mut exhausted = IdentityStatementCursor::from_active_state(&state)
                .expect("active state should open a cursor");
            let error = exhausted
                .allocate(0, 0)
                .expect_err("allocation beyond the exact width must reject");
            assert_eq!(error.class(), ErrorClass::Unsupported);
            assert_eq!(error.origin(), ErrorOrigin::Identity);
            assert_eq!(exhausted.expected_high_water(), maximum);
            assert_eq!(exhausted.tentative_high_water(), maximum);
            assert_eq!(exhausted.allocation_count(), 0);
            assert!(!exhausted.has_allocations());
        }

        let state = IdentityState::new_active(owner(), AcceptedFieldKind::Nat64)
            .expect("zero active state should admit");
        let mut out_of_order = IdentityStatementCursor::from_active_state(&state)
            .expect("active state should open a cursor");
        let error = out_of_order
            .allocate(0, 1)
            .expect_err("the first candidate must retain ordinal zero");
        assert_eq!(error.class(), ErrorClass::InvariantViolation);
        assert_eq!(error.origin(), ErrorOrigin::Store);
        assert_eq!(out_of_order.tentative_high_water(), 0);
        assert_eq!(out_of_order.allocation_count(), 0);
    }

    #[test]
    fn marker_range_distinguishes_committed_from_materialized_state() {
        let state = IdentityState::new_active(owner(), AcceptedFieldKind::Nat64)
            .expect("zero active state should admit");
        let range = IdentityRangeAdvance::try_new(owner(), 0, 2, 2).expect("range should admit");
        let advance = advance();

        let pending = state
            .range_commit_state(range, advance)
            .expect("the marker should commit the pending range");
        assert_eq!(pending.materialized_high_water(), 0);
        assert_eq!(pending.committed_high_water(), 2);
        assert!(!pending.is_materialized());

        let materialized = state
            .apply_range_advance(range, advance)
            .expect("the committed range should materialize");
        let complete = materialized
            .range_commit_state(range, advance)
            .expect("exact replay should remain materialized");
        assert_eq!(complete.materialized_high_water(), 2);
        assert_eq!(complete.committed_high_water(), 2);
        assert!(complete.is_materialized());

        let stale_advance = IdentityAdvanceId::try_new([0x32; 16], [0x42; 16], 20, 6)
            .expect("second advance identity should admit");
        assert!(
            materialized
                .range_commit_state(range, stale_advance)
                .is_err(),
            "numeric equality without the exact advance identity is corruption",
        );
    }
}
