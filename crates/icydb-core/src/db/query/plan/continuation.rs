//! Continuation signature for cursor pagination compatibility checks.
#![allow(clippy::cast_possible_truncation)]

use super::{CursorBoundary, CursorBoundarySlot, ExplainPlan, OrderSpec, PlanError};
use crate::{
    db::query::{
        plan::hash_parts,
        predicate::{SchemaInfo, validate::literal_matches_type},
    },
    error::{ErrorClass, ErrorOrigin, InternalError},
    model::entity::EntityModel,
    serialize::{deserialize_bounded, serialize},
    traits::{EntityKind, FieldValue},
    value::Value,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
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
        crate::db::cursor::encode_cursor(&self.0)
    }
}

impl std::fmt::Display for ContinuationSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

const CONTINUATION_TOKEN_VERSION_V1: u8 = 1;
const MAX_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;

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
        .ok_or_else(|| PlanError::MissingPrimaryKeyTieBreak {
            field: pk_field.to_string(),
        })?;

    let schema = SchemaInfo::from_entity_model(model).map_err(PlanError::PredicateInvalid)?;
    let expected = schema
        .field(pk_field)
        .expect("primary key exists by model contract")
        .to_string();
    let pk_slot = &boundary.slots[pk_index];

    decode_primary_key_cursor_slot::<K>(pk_slot).map_err(|err| {
        PlanError::ContinuationCursorPrimaryKeyTypeMismatch {
            field: pk_field.to_string(),
            expected,
            value: err.into_mismatch_value(),
        }
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

    if boundary.slots.len() != 1 {
        return Err(InternalError::new(
            ErrorClass::InvariantViolation,
            ErrorOrigin::Query,
            format!(
                "executor invariant violated: pk-ordered continuation boundary must contain exactly 1 slot, found {}",
                boundary.slots.len()
            ),
        ));
    }

    decode_primary_key_cursor_slot::<E::Key>(&boundary.slots[0])
        .map(Some)
        .map_err(|err| match err {
            PrimaryKeyCursorSlotDecodeError::Missing => InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                "executor invariant violated: pk cursor slot must be present",
            ),
            PrimaryKeyCursorSlotDecodeError::TypeMismatch { .. } => InternalError::new(
                ErrorClass::InvariantViolation,
                ErrorOrigin::Query,
                "executor invariant violated: pk cursor slot type mismatch",
            ),
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
}

impl ContinuationToken {
    pub(crate) const fn new(signature: ContinuationSignature, boundary: CursorBoundary) -> Self {
        Self {
            signature,
            boundary,
        }
    }

    pub(crate) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    pub(crate) const fn boundary(&self) -> &CursorBoundary {
        &self.boundary
    }

    pub(crate) fn encode(&self) -> Result<Vec<u8>, ContinuationTokenError> {
        let wire = ContinuationTokenWire {
            version: CONTINUATION_TOKEN_VERSION_V1,
            signature: self.signature.into_bytes(),
            boundary: self.boundary.clone(),
        };

        serialize(&wire).map_err(|err| ContinuationTokenError::Encode(err.to_string()))
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<Self, ContinuationTokenError> {
        let wire: ContinuationTokenWire = deserialize_bounded(bytes, MAX_CONTINUATION_TOKEN_BYTES)
            .map_err(|err| ContinuationTokenError::Decode(err.to_string()))?;

        if wire.version != CONTINUATION_TOKEN_VERSION_V1 {
            return Err(ContinuationTokenError::UnsupportedVersion {
                version: wire.version,
            });
        }

        Ok(Self {
            signature: ContinuationSignature::from_bytes(wire.signature),
            boundary: wire.boundary,
        })
    }

    #[cfg(test)]
    pub(crate) fn encode_with_version_for_test(
        &self,
        version: u8,
    ) -> Result<Vec<u8>, ContinuationTokenError> {
        let wire = ContinuationTokenWire {
            version,
            signature: self.signature.into_bytes(),
            boundary: self.boundary.clone(),
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
}

// Decode and validate one continuation cursor against a canonical plan surface.
pub(crate) fn decode_validated_cursor_boundary(
    cursor: &[u8],
    entity_path: &'static str,
    model: &EntityModel,
    order: &OrderSpec,
    expected_signature: ContinuationSignature,
) -> Result<CursorBoundary, PlanError> {
    let token = ContinuationToken::decode(cursor).map_err(|err| match err {
        ContinuationTokenError::Encode(message) | ContinuationTokenError::Decode(message) => {
            PlanError::InvalidContinuationCursor { reason: message }
        }
        ContinuationTokenError::UnsupportedVersion { version } => {
            PlanError::ContinuationCursorVersionMismatch { version }
        }
    })?;

    if token.signature() != expected_signature {
        return Err(PlanError::ContinuationCursorSignatureMismatch {
            entity_path,
            expected: expected_signature.to_string(),
            actual: token.signature().to_string(),
        });
    }

    if token.boundary().slots.len() != order.fields.len() {
        return Err(PlanError::ContinuationCursorBoundaryArityMismatch {
            expected: order.fields.len(),
            found: token.boundary().slots.len(),
        });
    }

    validate_cursor_boundary_types(model, order, token.boundary())?;

    Ok(token.boundary().clone())
}

// Validate decoded cursor boundary slot types against canonical order fields.
fn validate_cursor_boundary_types(
    model: &EntityModel,
    order: &OrderSpec,
    boundary: &CursorBoundary,
) -> Result<(), PlanError> {
    let schema = SchemaInfo::from_entity_model(model).map_err(PlanError::PredicateInvalid)?;

    for ((field, _), slot) in order.fields.iter().zip(boundary.slots.iter()) {
        let field_type = schema
            .field(field)
            .ok_or_else(|| PlanError::UnknownOrderField {
                field: field.clone(),
            })?;

        match slot {
            CursorBoundarySlot::Missing => {
                if field == model.primary_key.name {
                    return Err(PlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                        field: field.clone(),
                        expected: field_type.to_string(),
                        value: None,
                    });
                }
            }
            CursorBoundarySlot::Present(value) => {
                if !literal_matches_type(value, field_type) {
                    if field == model.primary_key.name {
                        return Err(PlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                            field: field.clone(),
                            expected: field_type.to_string(),
                            value: Some(value.clone()),
                        });
                    }

                    return Err(PlanError::ContinuationCursorBoundaryTypeMismatch {
                        field: field.clone(),
                        expected: field_type.to_string(),
                        value: value.clone(),
                    });
                }

                // Primary-key slots must also satisfy key decoding semantics.
                if field == model.primary_key.name && Value::as_storage_key(value).is_none() {
                    return Err(PlanError::ContinuationCursorPrimaryKeyTypeMismatch {
                        field: field.clone(),
                        expected: field_type.to_string(),
                        value: Some(value.clone()),
                    });
                }
            }
        }
    }

    Ok(())
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
    pub fn continuation_signature(&self, entity_path: &'static str) -> ContinuationSignature {
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
    use crate::db::query::intent::{KeyAccess, access_plan_from_keys_value};
    use crate::db::query::plan::{AccessPath, LogicalPlan};
    use crate::db::query::predicate::Predicate;
    use crate::db::query::{FieldRef, QueryMode, ReadConsistency};
    use crate::types::Ulid;
    use crate::value::Value;

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
            mode: QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: access_a,
            predicate: None,
            order: None,
            delete_limit: None,
            page: None,
            consistency: ReadConsistency::MissingOk,
        };
        let plan_b: LogicalPlan<Value> = LogicalPlan {
            mode: QueryMode::Load(crate::db::query::LoadSpec::new()),
            access: access_b,
            predicate: None,
            order: None,
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

        plan_a.page = Some(crate::db::query::plan::PageSpec {
            limit: Some(10),
            offset: 0,
        });
        plan_b.page = Some(crate::db::query::plan::PageSpec {
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

        plan_a.order = Some(crate::db::query::plan::OrderSpec {
            fields: vec![(
                "name".to_string(),
                crate::db::query::plan::OrderDirection::Asc,
            )],
        });
        plan_b.order = Some(crate::db::query::plan::OrderSpec {
            fields: vec![(
                "name".to_string(),
                crate::db::query::plan::OrderDirection::Desc,
            )],
        });

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
}
