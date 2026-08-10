//! Module: db::mutation_job::intent
//! Responsibility: current canonical mutation-intent custody and bounded codec.
//! Does not own: SQL parsing, target traversal, row mutation, or progress transitions.
//! Boundary: accepted catalog-native scope/fixed patch -> private durable intent bytes.

use crate::{
    db::{
        MutationJobError, MutationJobPayloadKind,
        codec::{
            finalize_hash_sha256, new_hash_sha256_prefixed, write_hash_str_u32, write_hash_u32,
            write_hash_u64,
        },
        cursor::{decode_current_value_payload, encode_current_value_payload},
        data::{AcceptedFixedUpdatePatch, FieldSlot},
        database_format::crc32c,
        query::{
            plan::expr::{BinaryOp, CaseWhenArm, Expr, FieldId, FieldPath, Function, UnaryOp},
            resumable_update_scope_fingerprint,
        },
    },
    types::Timestamp,
};
use sha2::Digest;

use super::MAX_MUTATION_JOB_INTENT_BYTES;

const INTENT_MAGIC: &[u8; 8] = b"ICYMINT1";
const INTENT_FORMAT_VERSION: u8 = 1;
const SCOPE_MAGIC: &[u8; 8] = b"ICYMSCP1";
const SCOPE_FORMAT_VERSION: u8 = 1;
const PATCH_MAGIC: &[u8; 8] = b"ICYMPTC1";
const PATCH_FORMAT_VERSION: u8 = 1;
const START_REQUEST_FINGERPRINT_DOMAIN: &[u8] = b"icydb.mutation-job.start-request.v1";
const TARGET_ENTITY_IDENTITY_DOMAIN: &[u8] = b"icydb.mutation-job.target-entity.v1";
const MAX_CANONICAL_SCOPE_BYTES: usize = 8 * 1024;
const MAX_CANONICAL_PATCH_BYTES: usize = 8 * 1024;
const MAX_CANONICAL_PATH_BYTES: usize = 4 * 1024;
const MAX_CANONICAL_EXPR_DEPTH: usize = 32;
const MAX_CANONICAL_EXPR_NODES: usize = 256;
const MAX_CANONICAL_PATH_SEGMENTS: usize = 64;
const MAX_CANONICAL_CASE_ARMS: usize = 64;
const MAX_CANONICAL_PATCH_FIELDS: usize = 256;

/// Current private catalog-native meaning retained by one durable mutation job.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct CanonicalMutationIntent {
    database_incarnation: [u8; 16],
    target_store_identity: [u8; 32],
    target_entity_identity: [u8; 32],
    target_store_path: String,
    target_entity_path: String,
    target_entity_tag: u64,
    accepted_schema_revision: u64,
    accepted_schema_fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
    canonical_scope: Vec<u8>,
    canonical_fixed_patch: Vec<u8>,
    start_request_fingerprint: [u8; 32],
    scope_fingerprint: [u8; 32],
    patch_fingerprint: [u8; 32],
    operation_timestamp: Timestamp,
    batch_policy_identity: u32,
}

impl CanonicalMutationIntent {
    #[expect(
        clippy::too_many_arguments,
        reason = "the constructor binds every immutable accepted-authority component explicitly"
    )]
    pub(in crate::db) fn new(
        database_incarnation: [u8; 16],
        target_store_identity: [u8; 32],
        target_store_path: String,
        target_entity_path: String,
        target_entity_tag: u64,
        accepted_schema_revision: u64,
        accepted_schema_fingerprint_method: u8,
        accepted_schema_fingerprint: [u8; 16],
        scope: &Expr,
        fixed_patch: &AcceptedFixedUpdatePatch,
        operation_timestamp: Timestamp,
        batch_policy_identity: u32,
    ) -> Result<Self, MutationJobError> {
        validate_path(&target_store_path)?;
        validate_path(&target_entity_path)?;
        let canonical_scope = encode_scope(scope)?;
        let canonical_fixed_patch = encode_fixed_patch(fixed_patch)?;
        let scope_fingerprint = resumable_update_scope_fingerprint(scope);
        let patch_fingerprint = fixed_patch.fingerprint();
        let target_entity_identity = target_entity_identity(&target_entity_path, target_entity_tag);
        let start_request_fingerprint = start_request_fingerprint(
            database_incarnation,
            target_store_identity,
            target_entity_identity,
            &target_store_path,
            &target_entity_path,
            target_entity_tag,
            accepted_schema_revision,
            accepted_schema_fingerprint_method,
            accepted_schema_fingerprint,
            scope_fingerprint,
            patch_fingerprint,
            batch_policy_identity,
        );
        let intent = Self {
            database_incarnation,
            target_store_identity,
            target_entity_identity,
            target_store_path,
            target_entity_path,
            target_entity_tag,
            accepted_schema_revision,
            accepted_schema_fingerprint_method,
            accepted_schema_fingerprint,
            canonical_scope,
            canonical_fixed_patch,
            start_request_fingerprint,
            scope_fingerprint,
            patch_fingerprint,
            operation_timestamp,
            batch_policy_identity,
        };
        intent.validate()?;
        Ok(intent)
    }

    /// Encode one complete current-form private intent envelope.
    pub(in crate::db) fn encode(&self) -> Result<Vec<u8>, MutationJobError> {
        self.validate()?;
        let mut writer = Writer::new();
        writer.raw(INTENT_MAGIC);
        writer.u8(INTENT_FORMAT_VERSION);
        writer.raw(&self.database_incarnation);
        writer.raw(&self.target_store_identity);
        writer.raw(&self.target_entity_identity);
        writer.string(&self.target_store_path)?;
        writer.string(&self.target_entity_path)?;
        writer.u64(self.target_entity_tag);
        writer.u64(self.accepted_schema_revision);
        writer.u8(self.accepted_schema_fingerprint_method);
        writer.raw(&self.accepted_schema_fingerprint);
        writer.bytes(&self.canonical_scope)?;
        writer.bytes(&self.canonical_fixed_patch)?;
        writer.raw(&self.start_request_fingerprint);
        writer.raw(&self.scope_fingerprint);
        writer.raw(&self.patch_fingerprint);
        writer.i64(self.operation_timestamp.as_millis());
        writer.u32(self.batch_policy_identity);
        let checksum = crc32c(writer.as_slice());
        writer.u32(checksum);
        let bytes = writer.finish();
        if bytes.len() > MAX_MUTATION_JOB_INTENT_BYTES {
            return Err(intent_too_large(MAX_MUTATION_JOB_INTENT_BYTES, bytes.len()));
        }
        Ok(bytes)
    }

    /// Decode and fully validate one current-form private intent envelope.
    pub(in crate::db) fn decode(bytes: &[u8]) -> Result<Self, MutationJobError> {
        if bytes.len() > MAX_MUTATION_JOB_INTENT_BYTES {
            return Err(intent_too_large(MAX_MUTATION_JOB_INTENT_BYTES, bytes.len()));
        }
        if bytes.len() < INTENT_MAGIC.len() + 1 + size_of::<u32>()
            || bytes.get(..INTENT_MAGIC.len()) != Some(INTENT_MAGIC)
        {
            return Err(MutationJobError::IncompatibleProgressFormat);
        }
        let checksum_offset = bytes
            .len()
            .checked_sub(size_of::<u32>())
            .ok_or(MutationJobError::CorruptProgressStore)?;
        let payload = bytes
            .get(..checksum_offset)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        let checksum = u32::from_be_bytes(
            bytes
                .get(checksum_offset..)
                .ok_or(MutationJobError::CorruptProgressStore)?
                .try_into()
                .map_err(|_| MutationJobError::CorruptProgressStore)?,
        );
        if crc32c(payload) != checksum {
            return Err(MutationJobError::CorruptProgressStore);
        }
        let mut reader = Reader::new(payload);
        if reader.array::<8>()? != *INTENT_MAGIC {
            return Err(MutationJobError::IncompatibleProgressFormat);
        }
        if reader.u8()? != INTENT_FORMAT_VERSION {
            return Err(MutationJobError::IncompatibleProgressFormat);
        }
        let intent = Self {
            database_incarnation: reader.array()?,
            target_store_identity: reader.array()?,
            target_entity_identity: reader.array()?,
            target_store_path: reader.string(MAX_CANONICAL_PATH_BYTES)?,
            target_entity_path: reader.string(MAX_CANONICAL_PATH_BYTES)?,
            target_entity_tag: reader.u64()?,
            accepted_schema_revision: reader.u64()?,
            accepted_schema_fingerprint_method: reader.u8()?,
            accepted_schema_fingerprint: reader.array()?,
            canonical_scope: reader.bytes(MAX_CANONICAL_SCOPE_BYTES)?.to_vec(),
            canonical_fixed_patch: reader.bytes(MAX_CANONICAL_PATCH_BYTES)?.to_vec(),
            start_request_fingerprint: reader.array()?,
            scope_fingerprint: reader.array()?,
            patch_fingerprint: reader.array()?,
            operation_timestamp: Timestamp::from_millis(reader.i64()?),
            batch_policy_identity: reader.u32()?,
        };
        if !reader.is_empty() {
            return Err(MutationJobError::CorruptProgressStore);
        }
        intent.validate()?;
        Ok(intent)
    }

    /// Return whether two starts have exactly the same immutable meaning.
    #[must_use]
    pub(in crate::db) fn same_start_request(&self, other: &Self) -> bool {
        self.start_request_fingerprint == other.start_request_fingerprint
    }

    /// Return whether current accepted and physical authority are unchanged.
    #[must_use]
    pub(in crate::db) fn same_authority(&self, other: &Self) -> bool {
        self.database_incarnation == other.database_incarnation
            && self.target_store_identity == other.target_store_identity
            && self.target_entity_identity == other.target_entity_identity
            && self.accepted_schema_revision == other.accepted_schema_revision
            && self.accepted_schema_fingerprint_method == other.accepted_schema_fingerprint_method
            && self.accepted_schema_fingerprint == other.accepted_schema_fingerprint
    }

    /// Return the database incarnation frozen at start.
    #[must_use]
    pub(in crate::db) const fn database_incarnation(&self) -> [u8; 16] {
        self.database_incarnation
    }

    /// Return the complete physical target identity frozen at start.
    #[must_use]
    pub(in crate::db) const fn target_store_identity(&self) -> [u8; 32] {
        self.target_store_identity
    }

    /// Borrow the accepted target store path frozen at start.
    #[must_use]
    pub(in crate::db) const fn target_store_path(&self) -> &str {
        self.target_store_path.as_str()
    }

    /// Borrow the accepted target entity path frozen at start.
    #[must_use]
    pub(in crate::db) const fn target_entity_path(&self) -> &str {
        self.target_entity_path.as_str()
    }

    /// Return the accepted target entity tag frozen at start.
    #[must_use]
    pub(in crate::db) const fn target_entity_tag(&self) -> u64 {
        self.target_entity_tag
    }

    /// Return the accepted-schema revision frozen at start.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_revision(&self) -> u64 {
        self.accepted_schema_revision
    }

    /// Return the accepted-schema fingerprint method frozen at start.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_fingerprint_method(&self) -> u8 {
        self.accepted_schema_fingerprint_method
    }

    /// Return the accepted-schema fingerprint frozen at start.
    #[must_use]
    pub(in crate::db) const fn accepted_schema_fingerprint(&self) -> [u8; 16] {
        self.accepted_schema_fingerprint
    }

    /// Return the engine-owned batch-policy identity frozen at start.
    #[must_use]
    pub(in crate::db) const fn batch_policy_identity(&self) -> u32 {
        self.batch_policy_identity
    }

    /// Borrow the frozen server-generated operation timestamp.
    #[must_use]
    pub(in crate::db) const fn operation_timestamp(&self) -> Timestamp {
        self.operation_timestamp
    }

    /// Decode the current canonical scope for later accepted-authority binding.
    pub(in crate::db) fn decode_scope(&self) -> Result<Expr, MutationJobError> {
        decode_scope(&self.canonical_scope)
    }

    /// Decode the current canonical fixed patch for later row convergence.
    pub(in crate::db) fn decode_fixed_patch(
        &self,
    ) -> Result<AcceptedFixedUpdatePatch, MutationJobError> {
        decode_fixed_patch(&self.canonical_fixed_patch)
    }

    fn validate(&self) -> Result<(), MutationJobError> {
        validate_path(&self.target_store_path)?;
        validate_path(&self.target_entity_path)?;
        let scope = decode_scope(&self.canonical_scope)?;
        let patch = decode_fixed_patch(&self.canonical_fixed_patch)?;
        if self.target_entity_identity
            != target_entity_identity(&self.target_entity_path, self.target_entity_tag)
            || self.scope_fingerprint != resumable_update_scope_fingerprint(&scope)
            || self.patch_fingerprint != patch.fingerprint()
            || self.start_request_fingerprint
                != start_request_fingerprint(
                    self.database_incarnation,
                    self.target_store_identity,
                    self.target_entity_identity,
                    &self.target_store_path,
                    &self.target_entity_path,
                    self.target_entity_tag,
                    self.accepted_schema_revision,
                    self.accepted_schema_fingerprint_method,
                    self.accepted_schema_fingerprint,
                    self.scope_fingerprint,
                    self.patch_fingerprint,
                    self.batch_policy_identity,
                )
        {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(())
    }
}

const fn validate_path(path: &str) -> Result<(), MutationJobError> {
    if path.is_empty() || path.len() > MAX_CANONICAL_PATH_BYTES {
        return Err(MutationJobError::IneligibleIntent);
    }
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "the fingerprint explicitly binds every immutable start authority"
)]
fn start_request_fingerprint(
    database_incarnation: [u8; 16],
    target_store_identity: [u8; 32],
    target_entity_identity: [u8; 32],
    target_store_path: &str,
    target_entity_path: &str,
    target_entity_tag: u64,
    accepted_schema_revision: u64,
    accepted_schema_fingerprint_method: u8,
    accepted_schema_fingerprint: [u8; 16],
    scope_fingerprint: [u8; 32],
    patch_fingerprint: [u8; 32],
    batch_policy_identity: u32,
) -> [u8; 32] {
    let mut hasher = new_hash_sha256_prefixed(START_REQUEST_FINGERPRINT_DOMAIN);
    hasher.update(database_incarnation);
    hasher.update(target_store_identity);
    hasher.update(target_entity_identity);
    write_hash_str_u32(&mut hasher, target_store_path);
    write_hash_str_u32(&mut hasher, target_entity_path);
    write_hash_u64(&mut hasher, target_entity_tag);
    write_hash_u64(&mut hasher, accepted_schema_revision);
    hasher.update([accepted_schema_fingerprint_method]);
    hasher.update(accepted_schema_fingerprint);
    hasher.update(scope_fingerprint);
    hasher.update(patch_fingerprint);
    write_hash_u32(&mut hasher, batch_policy_identity);
    finalize_hash_sha256(hasher)
}

fn target_entity_identity(entity_path: &str, entity_tag: u64) -> [u8; 32] {
    let mut hasher = new_hash_sha256_prefixed(TARGET_ENTITY_IDENTITY_DOMAIN);
    write_hash_str_u32(&mut hasher, entity_path);
    write_hash_u64(&mut hasher, entity_tag);
    finalize_hash_sha256(hasher)
}

fn encode_scope(scope: &Expr) -> Result<Vec<u8>, MutationJobError> {
    let mut writer = Writer::new();
    writer.raw(SCOPE_MAGIC);
    writer.u8(SCOPE_FORMAT_VERSION);
    let mut nodes = 0usize;
    encode_expr(&mut writer, scope, 0, &mut nodes)?;
    let bytes = writer.finish();
    if bytes.len() > MAX_CANONICAL_SCOPE_BYTES {
        return Err(intent_too_large(MAX_CANONICAL_SCOPE_BYTES, bytes.len()));
    }
    Ok(bytes)
}

fn decode_scope(bytes: &[u8]) -> Result<Expr, MutationJobError> {
    if bytes.len() > MAX_CANONICAL_SCOPE_BYTES {
        return Err(MutationJobError::CorruptProgressStore);
    }
    let mut reader = Reader::new(bytes);
    if reader.array::<8>()? != *SCOPE_MAGIC || reader.u8()? != SCOPE_FORMAT_VERSION {
        return Err(MutationJobError::IncompatibleProgressFormat);
    }
    let mut nodes = 0usize;
    let expr = decode_expr(&mut reader, 0, &mut nodes)?;
    if !reader.is_empty() {
        return Err(MutationJobError::CorruptProgressStore);
    }
    Ok(expr)
}

fn encode_expr(
    writer: &mut Writer,
    expr: &Expr,
    depth: usize,
    nodes: &mut usize,
) -> Result<(), MutationJobError> {
    count_expr_node_for_encode(depth, nodes)?;
    match expr {
        Expr::Field(field) => {
            writer.u8(0);
            writer.string(field.as_str())?;
        }
        Expr::FieldPath(path) => {
            writer.u8(1);
            writer.string(path.root().as_str())?;
            writer.count(path.segments().len())?;
            if path.segments().is_empty() || path.segments().len() > MAX_CANONICAL_PATH_SEGMENTS {
                return Err(MutationJobError::IneligibleIntent);
            }
            for segment in path.segments() {
                writer.string(segment)?;
            }
        }
        Expr::Literal(value) => {
            writer.u8(2);
            let payload = encode_current_value_payload(value)
                .map_err(|_| MutationJobError::IneligibleIntent)?;
            writer.bytes(&payload)?;
        }
        Expr::FunctionCall { function, args } => {
            writer.u8(3);
            writer.u8(function_tag(*function));
            writer.count(args.len())?;
            for arg in args {
                encode_expr(writer, arg, depth + 1, nodes)?;
            }
        }
        Expr::Unary { op, expr } => {
            writer.u8(4);
            writer.u8(unary_tag(*op));
            encode_expr(writer, expr, depth + 1, nodes)?;
        }
        Expr::Binary { op, left, right } => {
            writer.u8(5);
            writer.u8(binary_tag(*op));
            encode_expr(writer, left, depth + 1, nodes)?;
            encode_expr(writer, right, depth + 1, nodes)?;
        }
        Expr::Case {
            when_then_arms,
            else_expr,
        } => {
            if when_then_arms.is_empty() || when_then_arms.len() > MAX_CANONICAL_CASE_ARMS {
                return Err(MutationJobError::IneligibleIntent);
            }
            writer.u8(6);
            writer.count(when_then_arms.len())?;
            for arm in when_then_arms {
                encode_expr(writer, arm.condition(), depth + 1, nodes)?;
                encode_expr(writer, arm.result(), depth + 1, nodes)?;
            }
            encode_expr(writer, else_expr, depth + 1, nodes)?;
        }
        Expr::Aggregate(_) => return Err(MutationJobError::IneligibleIntent),
        #[cfg(test)]
        Expr::Alias { .. } => return Err(MutationJobError::IneligibleIntent),
    }
    Ok(())
}

fn decode_expr(
    reader: &mut Reader<'_>,
    depth: usize,
    nodes: &mut usize,
) -> Result<Expr, MutationJobError> {
    count_expr_node_for_decode(depth, nodes)?;
    match reader.u8()? {
        0 => Ok(Expr::Field(FieldId::new(
            reader.string(MAX_CANONICAL_PATH_BYTES)?,
        ))),
        1 => {
            let root = reader.string(MAX_CANONICAL_PATH_BYTES)?;
            let count = reader.bounded_count(MAX_CANONICAL_PATH_SEGMENTS)?;
            if count == 0 {
                return Err(MutationJobError::CorruptProgressStore);
            }
            let mut segments = Vec::with_capacity(count);
            for _ in 0..count {
                segments.push(reader.string(MAX_CANONICAL_PATH_BYTES)?);
            }
            Ok(Expr::FieldPath(FieldPath::new(root, segments)))
        }
        2 => {
            let payload = reader.bytes(MAX_CANONICAL_SCOPE_BYTES)?;
            decode_current_value_payload(payload)
                .map(Expr::Literal)
                .map_err(|_| MutationJobError::CorruptProgressStore)
        }
        3 => {
            let function = function_from_tag(reader.u8()?)?;
            let count = reader.bounded_count(MAX_CANONICAL_EXPR_NODES)?;
            let mut args = Vec::with_capacity(count);
            for _ in 0..count {
                args.push(decode_expr(reader, depth + 1, nodes)?);
            }
            Ok(Expr::FunctionCall { function, args })
        }
        4 => Ok(Expr::Unary {
            op: unary_from_tag(reader.u8()?)?,
            expr: Box::new(decode_expr(reader, depth + 1, nodes)?),
        }),
        5 => Ok(Expr::Binary {
            op: binary_from_tag(reader.u8()?)?,
            left: Box::new(decode_expr(reader, depth + 1, nodes)?),
            right: Box::new(decode_expr(reader, depth + 1, nodes)?),
        }),
        6 => {
            let count = reader.bounded_count(MAX_CANONICAL_CASE_ARMS)?;
            if count == 0 {
                return Err(MutationJobError::CorruptProgressStore);
            }
            let mut when_then_arms = Vec::with_capacity(count);
            for _ in 0..count {
                let condition = decode_expr(reader, depth + 1, nodes)?;
                let result = decode_expr(reader, depth + 1, nodes)?;
                when_then_arms.push(CaseWhenArm::new(condition, result));
            }
            let else_expr = Box::new(decode_expr(reader, depth + 1, nodes)?);
            Ok(Expr::Case {
                when_then_arms,
                else_expr,
            })
        }
        _ => Err(MutationJobError::CorruptProgressStore),
    }
}

fn count_expr_node_for_encode(depth: usize, nodes: &mut usize) -> Result<(), MutationJobError> {
    if depth > MAX_CANONICAL_EXPR_DEPTH {
        return Err(MutationJobError::IneligibleIntent);
    }
    *nodes = nodes
        .checked_add(1)
        .ok_or(MutationJobError::CounterOverflow)?;
    if *nodes > MAX_CANONICAL_EXPR_NODES {
        return Err(MutationJobError::IneligibleIntent);
    }
    Ok(())
}

fn count_expr_node_for_decode(depth: usize, nodes: &mut usize) -> Result<(), MutationJobError> {
    if depth > MAX_CANONICAL_EXPR_DEPTH {
        return Err(MutationJobError::CorruptProgressStore);
    }
    *nodes = nodes
        .checked_add(1)
        .ok_or(MutationJobError::CorruptProgressStore)?;
    if *nodes > MAX_CANONICAL_EXPR_NODES {
        return Err(MutationJobError::CorruptProgressStore);
    }
    Ok(())
}

fn encode_fixed_patch(patch: &AcceptedFixedUpdatePatch) -> Result<Vec<u8>, MutationJobError> {
    if patch.fields().is_empty() || patch.fields().len() > MAX_CANONICAL_PATCH_FIELDS {
        return Err(MutationJobError::IneligibleIntent);
    }
    let mut writer = Writer::new();
    writer.raw(PATCH_MAGIC);
    writer.u8(PATCH_FORMAT_VERSION);
    writer.count(patch.fields().len())?;
    for field in patch.fields() {
        writer.count(field.slot().index())?;
        writer.bytes(field.payload())?;
    }
    let bytes = writer.finish();
    if bytes.len() > MAX_CANONICAL_PATCH_BYTES {
        return Err(intent_too_large(MAX_CANONICAL_PATCH_BYTES, bytes.len()));
    }
    Ok(bytes)
}

fn decode_fixed_patch(bytes: &[u8]) -> Result<AcceptedFixedUpdatePatch, MutationJobError> {
    if bytes.len() > MAX_CANONICAL_PATCH_BYTES {
        return Err(MutationJobError::CorruptProgressStore);
    }
    let mut reader = Reader::new(bytes);
    if reader.array::<8>()? != *PATCH_MAGIC || reader.u8()? != PATCH_FORMAT_VERSION {
        return Err(MutationJobError::IncompatibleProgressFormat);
    }
    let count = reader.bounded_count(MAX_CANONICAL_PATCH_FIELDS)?;
    if count == 0 {
        return Err(MutationJobError::CorruptProgressStore);
    }
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let slot = reader.bounded_count(u32::MAX as usize)?;
        let payload = reader.bytes(MAX_CANONICAL_PATCH_BYTES)?.to_vec();
        fields.push((FieldSlot::from_validated_index(slot), payload));
    }
    if !reader.is_empty() {
        return Err(MutationJobError::CorruptProgressStore);
    }
    AcceptedFixedUpdatePatch::from_canonical_fields(fields)
        .map_err(|_| MutationJobError::CorruptProgressStore)
}

const fn unary_tag(op: UnaryOp) -> u8 {
    match op {
        UnaryOp::Not => 0,
    }
}

const fn unary_from_tag(tag: u8) -> Result<UnaryOp, MutationJobError> {
    match tag {
        0 => Ok(UnaryOp::Not),
        _ => Err(MutationJobError::CorruptProgressStore),
    }
}

const fn binary_tag(op: BinaryOp) -> u8 {
    match op {
        BinaryOp::Or => 0,
        BinaryOp::And => 1,
        BinaryOp::Eq => 2,
        BinaryOp::Ne => 3,
        BinaryOp::Lt => 4,
        BinaryOp::Lte => 5,
        BinaryOp::Gt => 6,
        BinaryOp::Gte => 7,
        BinaryOp::Add => 8,
        BinaryOp::Sub => 9,
        BinaryOp::Mul => 10,
        BinaryOp::Div => 11,
    }
}

const fn binary_from_tag(tag: u8) -> Result<BinaryOp, MutationJobError> {
    match tag {
        0 => Ok(BinaryOp::Or),
        1 => Ok(BinaryOp::And),
        2 => Ok(BinaryOp::Eq),
        3 => Ok(BinaryOp::Ne),
        4 => Ok(BinaryOp::Lt),
        5 => Ok(BinaryOp::Lte),
        6 => Ok(BinaryOp::Gt),
        7 => Ok(BinaryOp::Gte),
        8 => Ok(BinaryOp::Add),
        9 => Ok(BinaryOp::Sub),
        10 => Ok(BinaryOp::Mul),
        11 => Ok(BinaryOp::Div),
        _ => Err(MutationJobError::CorruptProgressStore),
    }
}

const fn function_tag(function: Function) -> u8 {
    match function {
        Function::Abs => 0,
        Function::Cbrt => 1,
        Function::Ceiling => 2,
        Function::Coalesce => 3,
        Function::CollectionContains => 4,
        Function::Contains => 5,
        Function::EndsWith => 6,
        Function::Exp => 7,
        Function::Floor => 8,
        Function::InList => 9,
        Function::IsEmpty => 10,
        Function::IsMissing => 11,
        Function::IsNotEmpty => 12,
        Function::IsNotNull => 13,
        Function::IsNull => 14,
        Function::Left => 15,
        Function::Length => 16,
        Function::Ln => 17,
        Function::Log => 18,
        Function::Log2 => 19,
        Function::Log10 => 20,
        Function::Lower => 21,
        Function::Ltrim => 22,
        Function::Mod => 23,
        Function::NullIf => 24,
        Function::OctetLength => 25,
        Function::Position => 26,
        Function::Power => 27,
        Function::Replace => 28,
        Function::Right => 29,
        Function::Round => 30,
        Function::Rtrim => 31,
        Function::Sign => 32,
        Function::Sqrt => 33,
        Function::StartsWith => 34,
        Function::Substring => 35,
        Function::Trim => 36,
        Function::Trunc => 37,
        Function::Upper => 38,
    }
}

const fn function_from_tag(tag: u8) -> Result<Function, MutationJobError> {
    let function = match tag {
        0 => Function::Abs,
        1 => Function::Cbrt,
        2 => Function::Ceiling,
        3 => Function::Coalesce,
        4 => Function::CollectionContains,
        5 => Function::Contains,
        6 => Function::EndsWith,
        7 => Function::Exp,
        8 => Function::Floor,
        9 => Function::InList,
        10 => Function::IsEmpty,
        11 => Function::IsMissing,
        12 => Function::IsNotEmpty,
        13 => Function::IsNotNull,
        14 => Function::IsNull,
        15 => Function::Left,
        16 => Function::Length,
        17 => Function::Ln,
        18 => Function::Log,
        19 => Function::Log2,
        20 => Function::Log10,
        21 => Function::Lower,
        22 => Function::Ltrim,
        23 => Function::Mod,
        24 => Function::NullIf,
        25 => Function::OctetLength,
        26 => Function::Position,
        27 => Function::Power,
        28 => Function::Replace,
        29 => Function::Right,
        30 => Function::Round,
        31 => Function::Rtrim,
        32 => Function::Sign,
        33 => Function::Sqrt,
        34 => Function::StartsWith,
        35 => Function::Substring,
        36 => Function::Trim,
        37 => Function::Trunc,
        38 => Function::Upper,
        _ => return Err(MutationJobError::CorruptProgressStore),
    };
    Ok(function)
}

fn intent_too_large(limit: usize, observed: usize) -> MutationJobError {
    MutationJobError::PayloadTooLarge {
        kind: MutationJobPayloadKind::Intent,
        limit: u64::try_from(limit).map_or(u64::MAX, |value| value),
        observed: u64::try_from(observed).map_or(u64::MAX, |value| value),
    }
}

struct Writer {
    bytes: Vec<u8>,
}

impl Writer {
    const fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    fn raw(&mut self, value: &[u8]) {
        self.bytes.extend_from_slice(value);
    }

    fn u8(&mut self, value: u8) {
        self.bytes.push(value);
    }

    fn u32(&mut self, value: u32) {
        self.raw(&value.to_be_bytes());
    }

    fn u64(&mut self, value: u64) {
        self.raw(&value.to_be_bytes());
    }

    fn i64(&mut self, value: i64) {
        self.raw(&value.to_be_bytes());
    }

    fn count(&mut self, value: usize) -> Result<(), MutationJobError> {
        self.u32(u32::try_from(value).map_err(|_| MutationJobError::IneligibleIntent)?);
        Ok(())
    }

    fn bytes(&mut self, value: &[u8]) -> Result<(), MutationJobError> {
        self.count(value.len())?;
        self.raw(value);
        Ok(())
    }

    fn string(&mut self, value: &str) -> Result<(), MutationJobError> {
        if value.len() > MAX_CANONICAL_PATH_BYTES {
            return Err(MutationJobError::IneligibleIntent);
        }
        self.bytes(value.as_bytes())
    }

    const fn as_slice(&self) -> &[u8] {
        self.bytes.as_slice()
    }

    fn finish(self) -> Vec<u8> {
        self.bytes
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    fn exact(&mut self, len: usize) -> Result<&'a [u8], MutationJobError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(MutationJobError::CorruptProgressStore)?;
        self.offset = end;
        Ok(value)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], MutationJobError> {
        self.exact(N)?
            .try_into()
            .map_err(|_| MutationJobError::CorruptProgressStore)
    }

    fn u8(&mut self) -> Result<u8, MutationJobError> {
        Ok(self.exact(1)?[0])
    }

    fn u32(&mut self) -> Result<u32, MutationJobError> {
        Ok(u32::from_be_bytes(self.array()?))
    }

    fn u64(&mut self) -> Result<u64, MutationJobError> {
        Ok(u64::from_be_bytes(self.array()?))
    }

    fn i64(&mut self) -> Result<i64, MutationJobError> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    fn bounded_count(&mut self, max: usize) -> Result<usize, MutationJobError> {
        let value =
            usize::try_from(self.u32()?).map_err(|_| MutationJobError::CorruptProgressStore)?;
        if value > max {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(value)
    }

    fn bytes(&mut self, max: usize) -> Result<&'a [u8], MutationJobError> {
        let len = self.bounded_count(max)?;
        self.exact(len)
    }

    fn string(&mut self, max: usize) -> Result<String, MutationJobError> {
        let bytes = self.bytes(max)?;
        let value =
            std::str::from_utf8(bytes).map_err(|_| MutationJobError::CorruptProgressStore)?;
        if value.is_empty() {
            return Err(MutationJobError::CorruptProgressStore);
        }
        Ok(value.to_string())
    }

    const fn is_empty(&self) -> bool {
        self.offset == self.bytes.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::value::Value;

    fn fixed_patch() -> AcceptedFixedUpdatePatch {
        AcceptedFixedUpdatePatch::from_canonical_fields(vec![
            (FieldSlot::from_validated_index(1), vec![1, 2, 3]),
            (FieldSlot::from_validated_index(4), vec![4, 5]),
        ])
        .expect("strict canonical fixed fields should admit")
    }

    fn scope(value: u64) -> Expr {
        Expr::Binary {
            op: BinaryOp::And,
            left: Box::new(Expr::Binary {
                op: BinaryOp::Eq,
                left: Box::new(Expr::Field(FieldId::new("collection_id"))),
                right: Box::new(Expr::Literal(Value::Nat64(value))),
            }),
            right: Box::new(Expr::FunctionCall {
                function: Function::IsNotNull,
                args: vec![Expr::Field(FieldId::new("tier"))],
            }),
        }
    }

    fn intent(value: u64, timestamp: i64) -> CanonicalMutationIntent {
        CanonicalMutationIntent::new(
            [1; 16],
            [2; 32],
            "journaled".to_string(),
            "schema::Token".to_string(),
            7,
            11,
            1,
            [3; 16],
            &scope(value),
            &fixed_patch(),
            Timestamp::from_millis(timestamp),
            17,
        )
        .expect("bounded canonical intent should admit")
    }

    #[test]
    fn current_intent_round_trip_preserves_scope_patch_and_request_identity() {
        let current = intent(7, 100);
        let bytes = current.encode().expect("current intent should encode");
        let decoded =
            CanonicalMutationIntent::decode(&bytes).expect("current intent should decode");

        assert_eq!(decoded, current);
        assert_eq!(decoded.decode_scope(), Ok(scope(7)));
        assert_eq!(decoded.decode_fixed_patch(), Ok(fixed_patch()));
        assert!(decoded.same_start_request(&intent(7, 200)));
        assert!(!decoded.same_start_request(&intent(8, 200)));
        assert_eq!(decoded.operation_timestamp(), Timestamp::from_millis(100));
    }

    #[test]
    fn current_intent_rejects_corruption_future_format_and_noncanonical_patch() {
        let current = intent(7, 100);
        let mut corrupt = current.encode().expect("current intent should encode");
        corrupt[40] ^= 1;
        assert_eq!(
            CanonicalMutationIntent::decode(&corrupt),
            Err(MutationJobError::CorruptProgressStore),
        );

        let mut future = current.encode().expect("current intent should encode");
        future[INTENT_MAGIC.len()] = INTENT_FORMAT_VERSION + 1;
        let checksum_offset = future.len() - size_of::<u32>();
        let checksum = crc32c(&future[..checksum_offset]);
        future[checksum_offset..].copy_from_slice(&checksum.to_be_bytes());
        assert_eq!(
            CanonicalMutationIntent::decode(&future),
            Err(MutationJobError::IncompatibleProgressFormat),
        );

        assert!(
            AcceptedFixedUpdatePatch::from_canonical_fields(vec![
                (FieldSlot::from_validated_index(2), vec![1]),
                (FieldSlot::from_validated_index(2), vec![2]),
            ])
            .is_err()
        );
    }
}
