//! Continuation signature for cursor pagination compatibility checks.
use super::{
    CursorBoundary, CursorBoundarySlot, CursorPlanError, Direction, ExplainPlan, OrderPlanError,
    OrderSpec, PlanError, encode_plan_hex,
};
use crate::{
    db::{
        index::RawIndexKey,
        query::{plan::hash_parts, predicate::SchemaInfo},
    },
    error::InternalError,
    model::entity::EntityModel,
    serialize::{deserialize_bounded, serialize},
    traits::{EntityKind, FieldValue, Storable},
    value::Value,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::borrow::Cow;
use thiserror::Error as ThisError;

///
/// ContinuationSignature
///
/// Stable, deterministic hash of continuation-relevant plan semantics.
/// Excludes windowing state (`limit`, `offset`) and cursor boundaries.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ContinuationSignature([u8; 32]);

impl ContinuationSignature {
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
    }

    pub(crate) const fn into_bytes(self) -> [u8; 32] {
        self.0
    }

    #[must_use]
    pub fn as_hex(&self) -> String {
        encode_plan_hex(&self.0)
    }
}

impl std::fmt::Display for ContinuationSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

const MAX_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;

///
/// CursorTokenVersion
///
/// CursorTokenVersion
///
/// Wire-level cursor token version owned by the cursor protocol boundary.
/// This keeps version parsing and compatibility behavior centralized.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::query) enum CursorTokenVersion {
    V1,
    V2,
}

impl CursorTokenVersion {
    const V1_TAG: u8 = 1;
    const V2_TAG: u8 = 2;

    // Decode one raw wire version into the protocol enum.
    const fn decode(raw: u8) -> Result<Self, ContinuationTokenError> {
        match raw {
            Self::V1_TAG => Ok(Self::V1),
            Self::V2_TAG => Ok(Self::V2),
            version => Err(ContinuationTokenError::UnsupportedVersion { version }),
        }
    }

    // Encode this protocol version for wire format output.
    const fn encode(self) -> u8 {
        match self {
            Self::V1 => Self::V1_TAG,
            Self::V2 => Self::V2_TAG,
        }
    }

    // Apply version compatibility behavior for initial offset.
    // V1 tokens did not carry offset and must decode as zero.
    const fn decode_initial_offset(self, wire_initial_offset: u32) -> u32 {
        match self {
            Self::V1 => 0,
            Self::V2 => wire_initial_offset,
        }
    }
}

/// Decode errors for typed primary-key cursor slot extraction.
#[derive(Clone, Debug)]
pub(crate) enum PrimaryKeyCursorSlotDecodeError {
    Missing,
    TypeMismatch { value: Value },
}

impl PrimaryKeyCursorSlotDecodeError {
    /// Convert this decode failure into the optional offending value shape.
    #[must_use]
    pub(crate) fn into_mismatch_value(self) -> Option<Value> {
        match self {
            Self::Missing => None,
            Self::TypeMismatch { value } => Some(value),
        }
    }
}

// Decode one primary-key cursor slot into a typed key value.
pub(crate) fn decode_primary_key_cursor_slot<K: FieldValue>(
    slot: &CursorBoundarySlot,
) -> Result<K, PrimaryKeyCursorSlotDecodeError> {
    match slot {
        CursorBoundarySlot::Missing => Err(PrimaryKeyCursorSlotDecodeError::Missing),
        CursorBoundarySlot::Present(value) => {
            K::from_value(value).ok_or_else(|| PrimaryKeyCursorSlotDecodeError::TypeMismatch {
                value: value.clone(),
            })
        }
    }
}

/// Decode the primary-key slot from a validated cursor boundary using typed key semantics.
pub(crate) fn decode_typed_primary_key_cursor_slot<K: FieldValue>(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<K, PlanError> {
    let pk_field = model.primary_key.name;
    let pk_index = order
        .fields
        .iter()
        .position(|(field, _)| field == pk_field)
        .ok_or_else(|| {
            PlanError::from(OrderPlanError::MissingPrimaryKeyTieBreak {
                field: pk_field.to_string(),
            })
        })?;

    let schema = SchemaInfo::from_entity_model(model).map_err(PlanError::from)?;
    let expected = schema
        .field(pk_field)
        .expect("primary key exists by model contract")
        .to_string();
    let pk_slot = &boundary.slots[pk_index];

    decode_primary_key_cursor_slot::<K>(pk_slot).map_err(|err| {
        PlanError::from(CursorPlanError::ContinuationCursorPrimaryKeyTypeMismatch {
            field: pk_field.to_string(),
            expected,
            value: err.into_mismatch_value(),
        })
    })
}

/// Decode a typed primary-key cursor boundary for PK-ordered executor paths.
pub(crate) fn decode_pk_cursor_boundary<E>(
    boundary: Option<&CursorBoundary>,
) -> Result<Option<E::Key>, InternalError>
where
    E: EntityKind,
{
    let Some(boundary) = boundary else {
        return Ok(None);
    };

    debug_assert_eq!(
        boundary.slots.len(),
        1,
        "pk-ordered continuation boundaries are validated by the cursor spine",
    );
    let slot = boundary
        .slots
        .first()
        .unwrap_or(&CursorBoundarySlot::Missing);

    decode_primary_key_cursor_slot::<E::Key>(slot)
        .map(Some)
        .map_err(|err| match err {
            PrimaryKeyCursorSlotDecodeError::Missing => {
                InternalError::query_executor_invariant("pk cursor slot must be present")
            }
            PrimaryKeyCursorSlotDecodeError::TypeMismatch { .. } => {
                InternalError::query_executor_invariant("pk cursor slot type mismatch")
            }
        })
}

///
/// ContinuationToken
/// Opaque cursor payload bound to a continuation signature.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct ContinuationToken {
    signature: ContinuationSignature,
    boundary: CursorBoundary,
    direction: Direction,
    initial_offset: u32,
    index_range_anchor: Option<IndexRangeCursorAnchor>,
}

impl ContinuationToken {
    pub(in crate::db) const fn new_with_direction(
        signature: ContinuationSignature,
        boundary: CursorBoundary,
        direction: Direction,
        initial_offset: u32,
    ) -> Self {
        Self {
            signature,
            boundary,
            direction,
            initial_offset,
            index_range_anchor: None,
        }
    }

    pub(in crate::db) const fn new_index_range_with_direction(
        signature: ContinuationSignature,
        boundary: CursorBoundary,
        index_range_anchor: IndexRangeCursorAnchor,
        direction: Direction,
        initial_offset: u32,
    ) -> Self {
        Self {
            signature,
            boundary,
            direction,
            initial_offset,
            index_range_anchor: Some(index_range_anchor),
        }
    }

    pub(crate) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    pub(crate) const fn boundary(&self) -> &CursorBoundary {
        &self.boundary
    }

    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    pub(in crate::db) const fn initial_offset(&self) -> u32 {
        self.initial_offset
    }

    pub(in crate::db) const fn index_range_anchor(&self) -> Option<&IndexRangeCursorAnchor> {
        self.index_range_anchor.as_ref()
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, ContinuationTokenError> {
        let index_range_anchor = self
            .index_range_anchor()
            .map(IndexRangeCursorAnchorWire::from);
        let wire = ContinuationTokenWire {
            version: CursorTokenVersion::V2.encode(),
            signature: self.signature.into_bytes(),
            boundary: self.boundary.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
            index_range_anchor,
        };

        serialize(&wire).map_err(|err| ContinuationTokenError::Encode(err.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ContinuationTokenError> {
        let wire: ContinuationTokenWire = deserialize_bounded(bytes, MAX_CONTINUATION_TOKEN_BYTES)
            .map_err(|err| ContinuationTokenError::Decode(err.to_string()))?;

        // Decode the protocol version first so compatibility behavior remains centralized.
        let version = CursorTokenVersion::decode(wire.version)?;
        let signature = ContinuationSignature::from_bytes(wire.signature);
        let boundary = wire.boundary;
        let direction = wire.direction;
        let initial_offset = version.decode_initial_offset(wire.initial_offset);

        match wire
            .index_range_anchor
            .map(IndexRangeCursorAnchorWire::into_anchor)
        {
            Some(anchor) => Ok(Self::new_index_range_with_direction(
                signature,
                boundary,
                anchor,
                direction,
                initial_offset,
            )),
            None => Ok(Self::new_with_direction(
                signature,
                boundary,
                direction,
                initial_offset,
            )),
        }
    }

    #[cfg(test)]
    pub(crate) fn encode_with_version_for_test(
        &self,
        version: u8,
    ) -> Result<Vec<u8>, ContinuationTokenError> {
        let index_range_anchor = self
            .index_range_anchor()
            .map(IndexRangeCursorAnchorWire::from);
        let wire = ContinuationTokenWire {
            version,
            signature: self.signature.into_bytes(),
            boundary: self.boundary.clone(),
            direction: self.direction,
            initial_offset: self.initial_offset,
            index_range_anchor,
        };

        serialize(&wire).map_err(|err| ContinuationTokenError::Encode(err.to_string()))
    }
}

///
/// ContinuationTokenError
/// Cursor token encoding/decoding failures.
///

#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
pub(crate) enum ContinuationTokenError {
    #[error("failed to encode continuation token: {0}")]
    Encode(String),

    #[error("failed to decode continuation token: {0}")]
    Decode(String),

    #[error("unsupported continuation token version: {version}")]
    UnsupportedVersion { version: u8 },
}

///
/// ContinuationTokenWire
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ContinuationTokenWire {
    version: u8,
    signature: [u8; 32],
    boundary: CursorBoundary,
    #[serde(default)]
    direction: Direction,
    #[serde(default)]
    initial_offset: u32,
    #[serde(default)]
    index_range_anchor: Option<IndexRangeCursorAnchorWire>,
}

///
/// IndexRangeCursorAnchor
/// Dedicated continuation anchor for `AccessPath::IndexRange`.
///
/// This tracks the exact raw index key of the last emitted row so continuation
/// can resume from `Bound::Excluded(last_raw_key)` in store traversal space.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct IndexRangeCursorAnchor {
    last_raw_key: RawIndexKey,
}

impl IndexRangeCursorAnchor {
    pub(in crate::db) const fn new(last_raw_key: RawIndexKey) -> Self {
        Self { last_raw_key }
    }

    pub(in crate::db) const fn last_raw_key(&self) -> &RawIndexKey {
        &self.last_raw_key
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct IndexRangeCursorAnchorWire {
    last_raw_key: Vec<u8>,
}

impl From<&IndexRangeCursorAnchor> for IndexRangeCursorAnchorWire {
    fn from(anchor: &IndexRangeCursorAnchor) -> Self {
        Self {
            last_raw_key: anchor.last_raw_key().as_bytes().to_vec(),
        }
    }
}

impl IndexRangeCursorAnchorWire {
    fn into_anchor(self) -> IndexRangeCursorAnchor {
        IndexRangeCursorAnchor::new(<RawIndexKey as Storable>::from_bytes(Cow::Owned(
            self.last_raw_key,
        )))
    }
}

///
/// LogicalPlan
///

impl<K> super::LogicalPlan<K>
where
    K: FieldValue,
{
    /// Compute a continuation signature bound to the entity path.
    ///
    /// This is used to validate that a continuation token belongs to the
    /// same canonical query shape.
    #[must_use]
    pub(crate) fn continuation_signature(
        &self,
        entity_path: &'static str,
    ) -> ContinuationSignature {
        self.explain().continuation_signature(entity_path)
    }
}

impl ExplainPlan {
    /// Compute the continuation signature for this explain plan.
    ///
    /// Included fields:
    /// - entity path
    /// - mode (load/delete)
    /// - access path
    /// - normalized predicate
    /// - canonical order-by (including implicit PK tie-break)
    /// - distinct flag
    /// - projection marker (currently full entity row projection)
    ///
    /// Excluded fields:
    /// - pagination window (`limit`, `offset`)
    /// - delete limits
    /// - cursor boundary/token state
    #[must_use]
    pub fn continuation_signature(&self, entity_path: &'static str) -> ContinuationSignature {
        let mut hasher = Sha256::new();
        hasher.update(b"contsig:v1");
        hash_parts::hash_explain_plan_profile(
            &mut hasher,
            self,
            hash_parts::ExplainHashProfile::ContinuationV1 { entity_path },
        );

        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        ContinuationSignature::from_bytes(out)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        ContinuationSignature, ContinuationToken, ContinuationTokenError, IndexRangeCursorAnchor,
    };
    use crate::db::query::intent::{KeyAccess, LoadSpec, access_plan_from_keys_value};
    use crate::db::query::plan::{
        AccessPath, CursorBoundary, CursorBoundarySlot, Direction, LogicalPlan, OrderDirection,
        OrderSpec, PageSpec,
    };
    use crate::db::query::predicate::Predicate;
    use crate::db::query::{ReadConsistency, builder::field::FieldRef, intent::QueryMode};
    use crate::types::Ulid;
    use crate::value::Value;
    use crate::{db::index::RawIndexKey, traits::Storable};
    use std::borrow::Cow;

    #[test]
    fn signature_is_deterministic_for_equivalent_predicates() {
        let id = Ulid::default();

        let predicate_a = Predicate::And(vec![
            FieldRef::new("id").eq(id),
            FieldRef::new("other").eq(Value::Text("x".to_string())),
        ]);
        let predicate_b = Predicate::And(vec![
            FieldRef::new("other").eq(Value::Text("x".to_string())),
            FieldRef::new("id").eq(id),
        ]);

        let mut plan_a: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        plan_a.predicate = Some(predicate_a);

        let mut plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        plan_b.predicate = Some(predicate_b);

        assert_eq!(
            plan_a.continuation_signature("tests::Entity"),
            plan_b.continuation_signature("tests::Entity")
        );
    }

    #[test]
    fn signature_is_deterministic_for_by_keys() {
        let a = Ulid::from_u128(1);
        let b = Ulid::from_u128(2);

        let access_a = access_plan_from_keys_value(&KeyAccess::Many(vec![a, b, a]));
        let access_b = access_plan_from_keys_value(&KeyAccess::Many(vec![b, a]));

        let plan_a: LogicalPlan<Value> = LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            access: access_a,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        };
        let plan_b: LogicalPlan<Value> = LogicalPlan {
            mode: QueryMode::Load(LoadSpec::new()),
            access: access_b,
            predicate: None,
            order: None,
            distinct: false,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        };

        assert_eq!(
            plan_a.continuation_signature("tests::Entity"),
            plan_b.continuation_signature("tests::Entity")
        );
    }

    #[test]
    fn signature_excludes_pagination_window_state() {
        let mut plan_a: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        let mut plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);

        plan_a.page = Some(PageSpec {
            limit: Some(10),
            offset: 0,
        });
        plan_b.page = Some(PageSpec {
            limit: Some(10),
            offset: 999,
        });

        assert_eq!(
            plan_a.continuation_signature("tests::Entity"),
            plan_b.continuation_signature("tests::Entity")
        );
    }

    #[test]
    fn signature_changes_when_order_changes() {
        let mut plan_a: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        let mut plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);

        plan_a.order = Some(OrderSpec {
            fields: vec![("name".to_string(), OrderDirection::Asc)],
        });
        plan_b.order = Some(OrderSpec {
            fields: vec![("name".to_string(), OrderDirection::Desc)],
        });

        assert_ne!(
            plan_a.continuation_signature("tests::Entity"),
            plan_b.continuation_signature("tests::Entity")
        );
    }

    #[test]
    fn signature_changes_when_distinct_flag_changes() {
        let plan_a: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        let mut plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        plan_b.distinct = true;

        assert_ne!(
            plan_a.continuation_signature("tests::Entity"),
            plan_b.continuation_signature("tests::Entity")
        );
    }

    #[test]
    fn signature_changes_with_entity_path() {
        let plan: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);

        assert_ne!(
            plan.continuation_signature("tests::EntityA"),
            plan.continuation_signature("tests::EntityB")
        );
    }

    #[test]
    fn continuation_token_round_trips_index_range_anchor() {
        let raw_key = <RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![0xAA, 0xBB, 0xCC]));
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Uint(42))],
        };
        let signature = ContinuationSignature::from_bytes([7u8; 32]);

        let token = ContinuationToken::new_index_range_with_direction(
            signature,
            boundary.clone(),
            IndexRangeCursorAnchor::new(raw_key.clone()),
            Direction::Asc,
            3,
        );

        let encoded = token
            .encode()
            .expect("token with index-range anchor encodes");
        let decoded =
            ContinuationToken::decode(&encoded).expect("token with index-range anchor decodes");

        assert_eq!(decoded.signature(), signature);
        assert_eq!(decoded.boundary(), &boundary);
        assert_eq!(decoded.initial_offset(), 3);
        let decoded_anchor = decoded
            .index_range_anchor()
            .expect("decoded token should include index-range anchor");
        assert_eq!(decoded_anchor.last_raw_key().as_bytes(), raw_key.as_bytes());
    }

    #[test]
    fn continuation_token_decode_rejects_unknown_version() {
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Uint(1))],
        };
        let signature = ContinuationSignature::from_bytes([3u8; 32]);
        let token = ContinuationToken::new_with_direction(signature, boundary, Direction::Asc, 9);
        let encoded = token
            .encode_with_version_for_test(99)
            .expect("unknown-version wire token should encode");

        let err = ContinuationToken::decode(&encoded).expect_err("unknown version must fail");
        assert_eq!(
            err,
            ContinuationTokenError::UnsupportedVersion { version: 99 }
        );
    }

    #[test]
    fn continuation_token_v1_decodes_initial_offset_as_zero() {
        let boundary = CursorBoundary {
            slots: vec![CursorBoundarySlot::Present(Value::Uint(1))],
        };
        let signature = ContinuationSignature::from_bytes([4u8; 32]);
        let token = ContinuationToken::new_with_direction(signature, boundary, Direction::Desc, 11);
        let encoded = token
            .encode_with_version_for_test(1)
            .expect("v1 wire token should encode");

        let decoded = ContinuationToken::decode(&encoded).expect("v1 wire token should decode");
        assert_eq!(
            decoded.initial_offset(),
            0,
            "v1 must decode with zero offset"
        );
        assert_eq!(decoded.direction(), Direction::Desc);
    }
}
