//! Continuation signature for cursor pagination compatibility checks.
#![allow(clippy::cast_possible_truncation)]

use super::{
    CursorBoundary, CursorBoundarySlot, ExplainAccessPath, ExplainOrderBy, ExplainPlan,
    ExplainPredicate, OrderSpec, PlanError,
};
use crate::{
    db::{
        index::fingerprint::hash_value,
        query::{
            QueryMode,
            predicate::{SchemaInfo, coercion::CoercionId, validate::literal_matches_type},
        },
    },
    model::entity::EntityModel,
    serialize::{deserialize_bounded, serialize},
    traits::FieldValue,
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
        let mut out = String::with_capacity(64);
        for byte in self.0 {
            use std::fmt::Write as _;
            let _ = write!(out, "{byte:02x}");
        }
        out
    }
}

impl std::fmt::Display for ContinuationSignature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

const CONTINUATION_TOKEN_VERSION_V1: u8 = 1;
#[cfg_attr(not(test), allow(dead_code))]
const MAX_CONTINUATION_TOKEN_BYTES: usize = 8 * 1024;

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

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) const fn signature(&self) -> ContinuationSignature {
        self.signature
    }

    #[cfg_attr(not(test), allow(dead_code))]
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

    #[cfg_attr(not(test), allow(dead_code))]
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
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) enum ContinuationTokenError {
    #[error("failed to encode continuation token: {0}")]
    Encode(String),

    #[error("failed to decode continuation token: {0}")]
    Decode(String),

    #[error("unsupported continuation token version: {version}")]
    UnsupportedVersion { version: u8 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
struct ContinuationTokenWire {
    version: u8,
    signature: [u8; 32],
    boundary: CursorBoundary,
}

// Decode and validate one continuation cursor against a canonical plan surface.
#[cfg_attr(not(test), allow(dead_code))]
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
#[cfg_attr(not(test), allow(dead_code))]
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

        write_tag(&mut hasher, 0x01);
        write_str(&mut hasher, entity_path);

        write_tag(&mut hasher, 0x02);
        hash_mode(&mut hasher, self.mode);

        write_tag(&mut hasher, 0x03);
        hash_access(&mut hasher, &self.access);

        write_tag(&mut hasher, 0x04);
        hash_predicate(&mut hasher, &self.predicate);

        write_tag(&mut hasher, 0x05);
        hash_order(&mut hasher, &self.order_by);

        write_tag(&mut hasher, 0x06);
        hash_projection(&mut hasher);

        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        ContinuationSignature::from_bytes(out)
    }
}

// Phase 1 projection surface is always full row `(Id<E>, E)`.
fn hash_projection(hasher: &mut Sha256) {
    write_tag(hasher, 0x70);
}

fn hash_access(hasher: &mut Sha256, access: &ExplainAccessPath) {
    match access {
        ExplainAccessPath::ByKey { key } => {
            write_tag(hasher, 0x10);
            write_value(hasher, key);
        }
        ExplainAccessPath::ByKeys { keys } => {
            write_tag(hasher, 0x11);
            write_u32(hasher, keys.len() as u32);
            for key in keys {
                write_value(hasher, key);
            }
        }
        ExplainAccessPath::KeyRange { start, end } => {
            write_tag(hasher, 0x12);
            write_value(hasher, start);
            write_value(hasher, end);
        }
        ExplainAccessPath::IndexPrefix {
            name,
            fields,
            prefix_len,
            values,
        } => {
            write_tag(hasher, 0x13);
            write_str(hasher, name);
            write_u32(hasher, fields.len() as u32);
            for field in fields {
                write_str(hasher, field);
            }
            write_u32(hasher, *prefix_len as u32);
            write_u32(hasher, values.len() as u32);
            for value in values {
                write_value(hasher, value);
            }
        }
        ExplainAccessPath::FullScan => {
            write_tag(hasher, 0x14);
        }
        ExplainAccessPath::Union(children) => {
            write_tag(hasher, 0x15);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_access(hasher, child);
            }
        }
        ExplainAccessPath::Intersection(children) => {
            write_tag(hasher, 0x16);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_access(hasher, child);
            }
        }
    }
}

fn hash_predicate(hasher: &mut Sha256, predicate: &ExplainPredicate) {
    match predicate {
        ExplainPredicate::None => write_tag(hasher, 0x20),
        ExplainPredicate::True => write_tag(hasher, 0x21),
        ExplainPredicate::False => write_tag(hasher, 0x22),
        ExplainPredicate::And(children) => {
            write_tag(hasher, 0x23);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_predicate(hasher, child);
            }
        }
        ExplainPredicate::Or(children) => {
            write_tag(hasher, 0x24);
            write_u32(hasher, children.len() as u32);
            for child in children {
                hash_predicate(hasher, child);
            }
        }
        ExplainPredicate::Not(inner) => {
            write_tag(hasher, 0x25);
            hash_predicate(hasher, inner);
        }
        ExplainPredicate::Compare {
            field,
            op,
            value,
            coercion,
        } => {
            write_tag(hasher, 0x26);
            write_str(hasher, field);
            write_tag(hasher, op.tag());
            write_value(hasher, value);
            hash_coercion(hasher, coercion.id, &coercion.params);
        }
        ExplainPredicate::IsNull { field } => {
            write_tag(hasher, 0x27);
            write_str(hasher, field);
        }
        ExplainPredicate::IsMissing { field } => {
            write_tag(hasher, 0x28);
            write_str(hasher, field);
        }
        ExplainPredicate::IsEmpty { field } => {
            write_tag(hasher, 0x29);
            write_str(hasher, field);
        }
        ExplainPredicate::IsNotEmpty { field } => {
            write_tag(hasher, 0x2a);
            write_str(hasher, field);
        }
        ExplainPredicate::TextContains { field, value } => {
            write_tag(hasher, 0x2e);
            write_str(hasher, field);
            write_value(hasher, value);
        }
        ExplainPredicate::TextContainsCi { field, value } => {
            write_tag(hasher, 0x2f);
            write_str(hasher, field);
            write_value(hasher, value);
        }
    }
}

fn hash_order(hasher: &mut Sha256, order: &ExplainOrderBy) {
    match order {
        ExplainOrderBy::None => write_tag(hasher, 0x30),
        ExplainOrderBy::Fields(fields) => {
            write_tag(hasher, 0x31);
            write_u32(hasher, fields.len() as u32);
            for field in fields {
                write_str(hasher, &field.field);
                write_tag(hasher, order_direction_tag(field.direction));
            }
        }
    }
}

fn hash_mode(hasher: &mut Sha256, mode: QueryMode) {
    match mode {
        QueryMode::Load(_) => write_tag(hasher, 0x60),
        QueryMode::Delete(_) => write_tag(hasher, 0x61),
    }
}

fn hash_coercion(
    hasher: &mut Sha256,
    id: CoercionId,
    params: &std::collections::BTreeMap<String, String>,
) {
    write_tag(hasher, coercion_id_tag(id));
    write_u32(hasher, params.len() as u32);
    for (key, value) in params {
        write_str(hasher, key);
        write_str(hasher, value);
    }
}

fn write_value(hasher: &mut Sha256, value: &crate::value::Value) {
    match hash_value(value) {
        Ok(digest) => hasher.update(digest),
        Err(err) => {
            write_tag(hasher, 0xEE);
            write_str(hasher, &err.display_with_class());
        }
    }
}

fn write_str(hasher: &mut Sha256, value: &str) {
    write_u32(hasher, value.len() as u32);
    hasher.update(value.as_bytes());
}

fn write_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

fn write_tag(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}

const fn order_direction_tag(direction: crate::db::query::plan::OrderDirection) -> u8 {
    match direction {
        crate::db::query::plan::OrderDirection::Asc => 0x01,
        crate::db::query::plan::OrderDirection::Desc => 0x02,
    }
}

const fn coercion_id_tag(id: CoercionId) -> u8 {
    match id {
        CoercionId::Strict => 0x01,
        CoercionId::NumericWiden => 0x02,
        CoercionId::TextCasefold => 0x04,
        CoercionId::CollectionElement => 0x05,
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
