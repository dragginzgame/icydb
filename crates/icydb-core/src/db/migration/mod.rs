//! Module: db::migration
//! Responsibility: explicit migration plan contracts and commit-marker-backed execution.
//! Does not own: migration row-op derivation policy or schema transformation design.
//! Boundary: callers provide explicit row-op steps; this module executes them durably.

#[cfg(test)]
mod tests;

use crate::{
    db::{
        Db,
        commit::{
            CommitMarker, CommitRowOp, begin_commit_with_migration_state,
            clear_migration_state_bytes, finish_commit, load_migration_state_bytes,
        },
    },
    error::InternalError,
    traits::CanisterKind,
};

const MAX_MIGRATION_STATE_BYTES: usize = 64 * 1024;
const MIGRATION_STATE_MAGIC: [u8; 2] = *b"MS";
const MIGRATION_STATE_VERSION_CURRENT: u8 = 1;
const MIGRATION_STATE_NONE_ROW_KEY_TAG: u8 = 0;
const MIGRATION_STATE_SOME_ROW_KEY_TAG: u8 = 1;

///
/// MigrationCursor
///
/// Explicit migration resume cursor.
/// This cursor tracks the next step index to execute in one migration plan.
/// The migration runtime persists this cursor durably between executions.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct MigrationCursor {
    next_step: usize,
}

impl MigrationCursor {
    /// Construct the starting migration cursor.
    #[must_use]
    pub const fn start() -> Self {
        Self { next_step: 0 }
    }

    /// Return the next migration step index to execute.
    #[must_use]
    pub const fn next_step(self) -> usize {
        self.next_step
    }

    const fn from_step(step_index: usize) -> Self {
        Self {
            next_step: step_index,
        }
    }

    // Advance one step after successful migration-step execution.
    const fn advance(self) -> Self {
        Self {
            next_step: self.next_step.saturating_add(1),
        }
    }
}

///
/// PersistedMigrationState
///
/// Durable migration-progress record stored in commit control state.
/// `step_index` stores the next step to execute for `migration_id` and
/// `migration_version`.
/// `last_applied_row_key` records the last row key from the last successful
/// migration step for diagnostics and recovery observability.
///

#[derive(Clone, Debug, Eq, PartialEq)]
struct PersistedMigrationState {
    migration_id: String,
    migration_version: u64,
    step_index: u64,
    last_applied_row_key: Option<Vec<u8>>,
}

///
/// MigrationRowOp
///
/// Public migration row operation DTO used to build explicit migration steps.
/// This DTO mirrors commit row-op payload shape without exposing commit internals.
/// Migration execution converts these DTOs into commit marker row operations.
///

#[derive(Clone, Debug)]
pub struct MigrationRowOp {
    /// Runtime entity path resolved by commit runtime hooks during execution.
    pub entity_path: String,
    /// Encoded raw data key bytes for target row identity.
    pub key: Vec<u8>,
    /// Optional encoded before-image row payload.
    pub before: Option<Vec<u8>>,
    /// Optional encoded after-image row payload.
    pub after: Option<Vec<u8>>,
    /// Schema fingerprint expected by commit prepare/replay for this row op.
    pub schema_fingerprint: [u8; 16],
}

impl MigrationRowOp {
    /// Construct one migration row operation DTO.
    #[must_use]
    pub fn new(
        entity_path: impl Into<String>,
        key: Vec<u8>,
        before: Option<Vec<u8>>,
        after: Option<Vec<u8>>,
        schema_fingerprint: [u8; 16],
    ) -> Self {
        Self {
            entity_path: entity_path.into(),
            key,
            before,
            after,
            schema_fingerprint,
        }
    }
}

impl TryFrom<MigrationRowOp> for CommitRowOp {
    type Error = InternalError;

    fn try_from(op: MigrationRowOp) -> Result<Self, Self::Error> {
        Self::try_new_bytes(
            op.entity_path,
            op.key.as_slice(),
            op.before,
            op.after,
            op.schema_fingerprint,
        )
    }
}

///
/// MigrationStep
///
/// One explicit migration step represented as ordered commit row operations.
/// Step ordering is deterministic and preserved exactly at execution time.
/// Empty step names and empty row-op vectors are rejected by constructor.
///

#[derive(Clone, Debug)]
pub struct MigrationStep {
    name: String,
    row_ops: Vec<CommitRowOp>,
}

impl MigrationStep {
    /// Build one validated migration step from public migration row-op DTOs.
    pub fn from_row_ops(
        name: impl Into<String>,
        row_ops: Vec<MigrationRowOp>,
    ) -> Result<Self, InternalError> {
        let commit_row_ops = row_ops
            .into_iter()
            .map(CommitRowOp::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(name, commit_row_ops)
    }

    /// Build one validated migration step.
    pub(in crate::db) fn new(
        name: impl Into<String>,
        row_ops: Vec<CommitRowOp>,
    ) -> Result<Self, InternalError> {
        let name = name.into();
        validate_non_empty_label(name.as_str(), "migration step name")?;

        if row_ops.is_empty() {
            return Err(InternalError::migration_step_row_ops_required(&name));
        }

        Ok(Self { name, row_ops })
    }

    /// Return this step's stable display name.
    #[must_use]
    pub const fn name(&self) -> &str {
        self.name.as_str()
    }

    /// Return the number of row operations in this step.
    #[must_use]
    pub const fn row_op_count(&self) -> usize {
        self.row_ops.len()
    }
}

///
/// MigrationPlan
///
/// Explicit, ordered migration contract composed of named row-op steps.
/// The plan id is stable caller-owned metadata for observability and retries.
/// The plan version is caller-owned monotonic metadata for upgrade safety.
/// Steps are executed sequentially in insertion order and never reordered.
///

#[derive(Clone, Debug)]
pub struct MigrationPlan {
    id: String,
    version: u64,
    steps: Vec<MigrationStep>,
}

impl MigrationPlan {
    /// Build one validated migration plan.
    pub fn new(
        id: impl Into<String>,
        version: u64,
        steps: Vec<MigrationStep>,
    ) -> Result<Self, InternalError> {
        let id = id.into();
        validate_non_empty_label(id.as_str(), "migration plan id")?;
        if version == 0 {
            return Err(InternalError::migration_plan_version_required(&id));
        }

        if steps.is_empty() {
            return Err(InternalError::migration_plan_steps_required(&id));
        }

        Ok(Self { id, version, steps })
    }

    /// Return this plan's stable id.
    #[must_use]
    pub const fn id(&self) -> &str {
        self.id.as_str()
    }

    /// Return this plan's stable version.
    #[must_use]
    pub const fn version(&self) -> u64 {
        self.version
    }

    /// Return the number of steps in this plan.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.steps.len()
    }

    /// Return whether this plan has no steps.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.steps.is_empty()
    }

    fn step_at(&self, index: usize) -> Result<&MigrationStep, InternalError> {
        self.steps.get(index).ok_or_else(|| {
            InternalError::migration_cursor_out_of_bounds(
                self.id(),
                self.version(),
                index,
                self.len(),
            )
        })
    }
}

///
/// MigrationRunState
///
/// Bounded migration-execution completion status.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MigrationRunState {
    /// No remaining steps; migration plan is complete at returned cursor.
    Complete,
    /// Remaining steps exist; rerun the same plan to resume from durable state.
    NeedsResume,
}

///
/// MigrationRunOutcome
///
/// Summary of one bounded migration-execution run.
/// This captures the next cursor plus applied-step/row-op counters.
/// Durable cursor persistence is internal to migration runtime state.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MigrationRunOutcome {
    cursor: MigrationCursor,
    applied_steps: usize,
    applied_row_ops: usize,
    state: MigrationRunState,
}

impl MigrationRunOutcome {
    const fn new(
        cursor: MigrationCursor,
        applied_steps: usize,
        applied_row_ops: usize,
        state: MigrationRunState,
    ) -> Self {
        Self {
            cursor,
            applied_steps,
            applied_row_ops,
            state,
        }
    }

    /// Return the next migration cursor.
    #[must_use]
    pub const fn cursor(self) -> MigrationCursor {
        self.cursor
    }

    /// Return the number of steps applied in this bounded run.
    #[must_use]
    pub const fn applied_steps(self) -> usize {
        self.applied_steps
    }

    /// Return the number of row ops applied in this bounded run.
    #[must_use]
    pub const fn applied_row_ops(self) -> usize {
        self.applied_row_ops
    }

    /// Return bounded-run completion state.
    #[must_use]
    pub const fn state(self) -> MigrationRunState {
        self.state
    }
}

/// Execute one bounded migration run from durable internal cursor state.
///
/// Contract:
/// - always runs commit recovery before applying migration steps
/// - executes at most `max_steps` deterministic steps in-order
/// - each step is persisted through commit-marker protocol
/// - migration cursor progress is atomically persisted with step marker writes
/// - step failures preserve marker authority for explicit fail-closed recovery
pub(in crate::db) fn execute_migration_plan<C: CanisterKind>(
    db: &Db<C>,
    plan: &MigrationPlan,
    max_steps: usize,
) -> Result<MigrationRunOutcome, InternalError> {
    // Phase 1: validate run-shape controls before touching commit state.
    if max_steps == 0 {
        return Err(InternalError::migration_execution_requires_max_steps(
            plan.id(),
        ));
    }

    // Phase 2: recover any in-flight commit marker before migration execution.
    db.ensure_recovered_state()?;

    // Phase 3: load durable migration cursor state from commit control storage.
    let mut next_cursor = load_durable_cursor_for_plan(plan)?;

    // Phase 4: execute a bounded number of deterministic migration steps.
    let mut applied_steps = 0usize;
    let mut applied_row_ops = 0usize;
    while applied_steps < max_steps {
        if next_cursor.next_step() >= plan.len() {
            break;
        }

        let step_index = next_cursor.next_step();
        let step = plan.step_at(step_index)?;
        let next_cursor_after_step = next_cursor.advance();
        let next_state_bytes =
            encode_durable_cursor_state(plan, next_cursor_after_step, step.row_ops.last())?;
        execute_migration_step(db, plan, step_index, step, next_state_bytes)?;

        applied_steps = applied_steps.saturating_add(1);
        applied_row_ops = applied_row_ops.saturating_add(step.row_op_count());
        next_cursor = next_cursor_after_step;
    }

    let state = if next_cursor.next_step() == plan.len() {
        clear_migration_state_bytes()?;
        MigrationRunState::Complete
    } else {
        MigrationRunState::NeedsResume
    };

    Ok(MigrationRunOutcome::new(
        next_cursor,
        applied_steps,
        applied_row_ops,
        state,
    ))
}

fn load_durable_cursor_for_plan(plan: &MigrationPlan) -> Result<MigrationCursor, InternalError> {
    let Some(bytes) = load_migration_state_bytes()? else {
        return Ok(MigrationCursor::start());
    };
    let state = decode_persisted_migration_state(&bytes)?;
    if state.migration_id != plan.id() || state.migration_version != plan.version() {
        return Err(InternalError::migration_in_progress_conflict(
            plan.id(),
            plan.version(),
            &state.migration_id,
            state.migration_version,
        ));
    }

    let step_index = usize::try_from(state.step_index).map_err(|_| {
        InternalError::migration_persisted_step_index_invalid_usize(
            plan.id(),
            plan.version(),
            state.step_index,
        )
    })?;
    if step_index > plan.len() {
        return Err(InternalError::migration_persisted_step_index_out_of_bounds(
            plan.id(),
            plan.version(),
            step_index,
            plan.len(),
        ));
    }

    if step_index == plan.len() {
        clear_migration_state_bytes()?;
    }

    Ok(MigrationCursor::from_step(step_index))
}

fn encode_durable_cursor_state(
    plan: &MigrationPlan,
    cursor: MigrationCursor,
    last_applied_row_op: Option<&CommitRowOp>,
) -> Result<Vec<u8>, InternalError> {
    let step_index = u64::try_from(cursor.next_step()).map_err(|_| {
        InternalError::migration_next_step_index_u64_required(plan.id(), plan.version())
    })?;
    let state = PersistedMigrationState {
        migration_id: plan.id().to_string(),
        migration_version: plan.version(),
        step_index,
        last_applied_row_key: last_applied_row_op.map(|row_op| row_op.key.as_bytes().to_vec()),
    };

    encode_persisted_migration_state(&state)
}

fn decode_persisted_migration_state(
    bytes: &[u8],
) -> Result<PersistedMigrationState, InternalError> {
    // Phase 1: reject oversized payloads before any structural decode work.
    if bytes.len() > MAX_MIGRATION_STATE_BYTES {
        return Err(InternalError::serialize_corruption(format!(
            "migration state decode failed: payload size {} exceeds limit {MAX_MIGRATION_STATE_BYTES}",
            bytes.len(),
        )));
    }

    // Phase 2: validate the fixed header and read the fixed-width fields.
    let mut cursor = bytes;
    decode_migration_state_magic(&mut cursor)?;
    let format_version = decode_migration_state_u8(&mut cursor, "format version")?;
    validate_migration_state_format_version(format_version)?;
    let migration_id = decode_migration_state_string(&mut cursor, "migration_id")?;
    let migration_version = decode_migration_state_u64(&mut cursor, "migration_version")?;
    let step_index = decode_migration_state_u64(&mut cursor, "step_index")?;
    let last_applied_row_key =
        decode_migration_state_optional_bytes(&mut cursor, "last_applied_row_key")?;

    // Phase 3: reject trailing bytes so the codec stays single-version and exact.
    if !cursor.is_empty() {
        return Err(InternalError::serialize_corruption(
            "migration state decode failed: trailing bytes",
        ));
    }

    Ok(PersistedMigrationState {
        migration_id,
        migration_version,
        step_index,
        last_applied_row_key,
    })
}

fn encode_persisted_migration_state(
    state: &PersistedMigrationState,
) -> Result<Vec<u8>, InternalError> {
    // Phase 1: pre-compute the bounded payload size so overflow and policy
    // violations fail before any bytes are emitted.
    let row_key_len = state.last_applied_row_key.as_ref().map_or(0usize, Vec::len);
    let encoded_len = MIGRATION_STATE_MAGIC
        .len()
        .saturating_add(1)
        .saturating_add(4)
        .saturating_add(state.migration_id.len())
        .saturating_add(8)
        .saturating_add(8)
        .saturating_add(1)
        .saturating_add(if state.last_applied_row_key.is_some() {
            4usize.saturating_add(row_key_len)
        } else {
            0
        });
    if encoded_len > MAX_MIGRATION_STATE_BYTES {
        return Err(InternalError::migration_state_serialize_failed(format!(
            "payload size {encoded_len} exceeds limit {MAX_MIGRATION_STATE_BYTES}",
        )));
    }

    let migration_id_len = u32::try_from(state.migration_id.len()).map_err(|_| {
        InternalError::migration_state_serialize_failed("migration_id exceeds u32 length")
    })?;
    let row_key_len_u32 = u32::try_from(row_key_len).map_err(|_| {
        InternalError::migration_state_serialize_failed("last_applied_row_key exceeds u32 length")
    })?;

    // Phase 2: write the fixed binary payload in one exact stable order.
    let mut encoded = Vec::with_capacity(encoded_len);
    encoded.extend_from_slice(&MIGRATION_STATE_MAGIC);
    encoded.push(MIGRATION_STATE_VERSION_CURRENT);
    encoded.extend_from_slice(&migration_id_len.to_be_bytes());
    encoded.extend_from_slice(state.migration_id.as_bytes());
    encoded.extend_from_slice(&state.migration_version.to_be_bytes());
    encoded.extend_from_slice(&state.step_index.to_be_bytes());

    match state.last_applied_row_key.as_ref() {
        Some(row_key) => {
            encoded.push(MIGRATION_STATE_SOME_ROW_KEY_TAG);
            encoded.extend_from_slice(&row_key_len_u32.to_be_bytes());
            encoded.extend_from_slice(row_key);
        }
        None => encoded.push(MIGRATION_STATE_NONE_ROW_KEY_TAG),
    }

    Ok(encoded)
}

// Decode and validate the fixed migration-state magic prefix.
fn decode_migration_state_magic(bytes: &mut &[u8]) -> Result<(), InternalError> {
    let magic = take_migration_state_bytes(bytes, MIGRATION_STATE_MAGIC.len(), "magic")?;
    if magic != MIGRATION_STATE_MAGIC {
        return Err(InternalError::serialize_corruption(
            "migration state decode failed: invalid magic",
        ));
    }

    Ok(())
}

// Decode one fixed-width u8 field from the migration-state payload.
fn decode_migration_state_u8(bytes: &mut &[u8], label: &'static str) -> Result<u8, InternalError> {
    Ok(take_migration_state_bytes(bytes, 1, label)?[0])
}

// Decode one fixed-width u64 field from the migration-state payload.
fn decode_migration_state_u64(
    bytes: &mut &[u8],
    label: &'static str,
) -> Result<u64, InternalError> {
    let raw = take_migration_state_bytes(bytes, 8, label)?;

    Ok(u64::from_be_bytes([
        raw[0], raw[1], raw[2], raw[3], raw[4], raw[5], raw[6], raw[7],
    ]))
}

// Decode one length-prefixed UTF-8 string field from the migration-state payload.
fn decode_migration_state_string(
    bytes: &mut &[u8],
    label: &'static str,
) -> Result<String, InternalError> {
    let raw = decode_migration_state_length_prefixed_bytes(bytes, label)?;

    String::from_utf8(raw.to_vec()).map_err(|_| {
        InternalError::serialize_corruption(format!(
            "migration state decode failed: {label} is not valid UTF-8",
        ))
    })
}

// Decode one tagged optional byte vector from the migration-state payload.
fn decode_migration_state_optional_bytes(
    bytes: &mut &[u8],
    label: &'static str,
) -> Result<Option<Vec<u8>>, InternalError> {
    let tag = decode_migration_state_u8(bytes, label)?;

    match tag {
        MIGRATION_STATE_NONE_ROW_KEY_TAG => Ok(None),
        MIGRATION_STATE_SOME_ROW_KEY_TAG => Ok(Some(
            decode_migration_state_length_prefixed_bytes(bytes, label)?.to_vec(),
        )),
        _ => Err(InternalError::serialize_corruption(format!(
            "migration state decode failed: invalid {label} tag {tag}",
        ))),
    }
}

// Decode one length-prefixed byte slice from the migration-state payload.
fn decode_migration_state_length_prefixed_bytes<'a>(
    bytes: &mut &'a [u8],
    label: &'static str,
) -> Result<&'a [u8], InternalError> {
    let raw_len = take_migration_state_bytes(bytes, 4, label)?;
    let len = usize::try_from(u32::from_be_bytes([
        raw_len[0], raw_len[1], raw_len[2], raw_len[3],
    ]))
    .map_err(|_| {
        InternalError::serialize_corruption(format!(
            "migration state decode failed: {label} length out of range",
        ))
    })?;

    take_migration_state_bytes(bytes, len, label)
}

// Borrow one exact byte span from the migration-state payload.
fn take_migration_state_bytes<'a>(
    bytes: &mut &'a [u8],
    len: usize,
    label: &'static str,
) -> Result<&'a [u8], InternalError> {
    if bytes.len() < len {
        return Err(InternalError::serialize_corruption(format!(
            "migration state decode failed: truncated {label}",
        )));
    }

    let (head, tail) = bytes.split_at(len);
    *bytes = tail;

    Ok(head)
}

// Validate the single supported migration-state format version.
fn validate_migration_state_format_version(format_version: u8) -> Result<(), InternalError> {
    if format_version == MIGRATION_STATE_VERSION_CURRENT {
        return Ok(());
    }

    Err(InternalError::serialize_incompatible_persisted_format(
        format!(
            "migration state format version {format_version} is unsupported by runtime version {MIGRATION_STATE_VERSION_CURRENT}",
        ),
    ))
}

fn execute_migration_step<C: CanisterKind>(
    db: &Db<C>,
    plan: &MigrationPlan,
    step_index: usize,
    step: &MigrationStep,
    next_state_bytes: Vec<u8>,
) -> Result<(), InternalError> {
    // Phase 1: persist marker authority + next-step cursor state atomically.
    let marker = CommitMarker::new(step.row_ops.clone())
        .map_err(|err| annotate_step_error(plan, step_index, step.name(), err))?;
    let commit = begin_commit_with_migration_state(marker, next_state_bytes)
        .map_err(|err| annotate_step_error(plan, step_index, step.name(), err))?;

    // Phase 2: apply step row ops under commit-window durability semantics.
    finish_commit(commit, |_| apply_marker_row_ops(db, &step.row_ops))
        .map_err(|err| annotate_step_error(plan, step_index, step.name(), err))?;

    Ok(())
}

fn apply_marker_row_ops<C: CanisterKind>(
    db: &Db<C>,
    row_ops: &[CommitRowOp],
) -> Result<(), InternalError> {
    // Phase 1: pre-prepare all row operations before mutating stores.
    let mut prepared = Vec::with_capacity(row_ops.len());
    for row_op in row_ops {
        prepared.push(db.prepare_row_commit_op(row_op)?);
    }

    // Phase 2: apply the prepared operations in stable marker order.
    for prepared_op in prepared {
        prepared_op.apply();
    }

    Ok(())
}

fn annotate_step_error(
    plan: &MigrationPlan,
    step_index: usize,
    step_name: &str,
    err: InternalError,
) -> InternalError {
    let source_message = err.message().to_string();

    err.with_message(format!(
        "migration '{}' step {} ('{}') failed: {}",
        plan.id(),
        step_index,
        step_name,
        source_message,
    ))
}

fn validate_non_empty_label(value: &str, label: &str) -> Result<(), InternalError> {
    if value.trim().is_empty() {
        return Err(InternalError::migration_label_empty(label));
    }

    Ok(())
}
