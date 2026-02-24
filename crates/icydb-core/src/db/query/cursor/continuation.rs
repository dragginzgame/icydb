//! Continuation signature and typed cursor-boundary decoding helpers.

use crate::{
    db::query::{
        contracts::cursor::{ContinuationSignature, CursorBoundary, CursorBoundarySlot},
        explain::ExplainPlan,
        fingerprint::hash_parts,
        plan::{CursorPlanError, LogicalPlan, OrderPlanError, OrderSpec, PlanError},
        predicate::SchemaInfo,
    },
    error::InternalError,
    model::entity::EntityModel,
    traits::{EntityKind, FieldValue},
    value::Value,
};
use sha2::{Digest, Sha256};

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
/// LogicalPlan
///

impl<K> LogicalPlan<K>
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
    use crate::{
        db::query::{
            ReadConsistency,
            builder::field::FieldRef,
            contracts::cursor::{
                ContinuationSignature, ContinuationToken, ContinuationTokenError, CursorBoundary,
                CursorBoundarySlot, IndexRangeCursorAnchor,
            },
            intent::{KeyAccess, LoadSpec, QueryMode, access_plan_from_keys_value},
            plan::{AccessPath, Direction, LogicalPlan, OrderDirection, OrderSpec, PageSpec},
            predicate::Predicate,
        },
        traits::Storable,
        types::Ulid,
        value::Value,
    };
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
    fn signature_changes_when_order_field_set_changes() {
        let mut plan_a: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        let mut plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);

        plan_a.order = Some(OrderSpec {
            fields: vec![("name".to_string(), OrderDirection::Asc)],
        });
        plan_b.order = Some(OrderSpec {
            fields: vec![("rank".to_string(), OrderDirection::Asc)],
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
        let raw_key = <crate::db::index::RawIndexKey as Storable>::from_bytes(Cow::Owned(vec![
            0xAA, 0xBB, 0xCC,
        ]));
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
