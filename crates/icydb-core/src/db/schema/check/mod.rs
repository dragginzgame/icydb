//! Module: db::schema::check
//! Responsibility: accepted `CheckExprV1` binding, canonical form, and evaluation.
//! Does not own: SQL parsing, generated declarations, mutation routing, or activation jobs.
//! Boundary: turns bounded row-local proposals into field-ID-bound accepted semantics.

mod bind;
mod compile;
mod render;

#[cfg(test)]
mod tests;

use crate::{
    db::schema::{AcceptedFieldKind, FieldId, PersistedSchemaSnapshot, SchemaFieldSlot},
    model::field::{FieldStorageDecode, LeafCodec},
};

pub(in crate::db) use bind::bind_generated_check_predicate;
#[cfg(feature = "sql")]
pub(in crate::db) use bind::bind_sql_check_expr;
#[cfg(test)]
pub(in crate::db) use bind::{CheckExprV1Input, CheckValueExprV1Input, bind_check_expr_v1};
pub(in crate::db) use compile::AcceptedRowConstraintViolationKind;
pub(in crate::db::schema) use compile::validate_accepted_check_literals;
pub(in crate::db) use compile::{
    AcceptedRowConstraintEvaluationError, CompiledAcceptedRowConstraints,
};
pub(in crate::db) use render::render_accepted_check_expr_sql;

/// Maximum root-relative nesting accepted by one V1 check expression.
pub(in crate::db) const MAX_CHECK_EXPR_V1_DEPTH: u16 = 32;
/// Maximum nodes accepted by one V1 check expression.
pub(in crate::db) const MAX_CHECK_EXPR_V1_NODES: u16 = 256;
/// Maximum direct children accepted by one canonical `AND` or `OR`.
pub(in crate::db) const MAX_CHECK_EXPR_V1_CHILDREN: usize = 64;
/// Maximum encoded bytes retained by all literals in one expression.
pub(in crate::db) const MAX_CHECK_EXPR_V1_LITERAL_BYTES: usize = 4 * 1024;
/// Maximum deterministic canonical-key bytes retained by one expression.
pub(in crate::db) const MAX_CHECK_EXPR_V1_BYTES: usize = 16 * 1024;
/// Maximum enum members accepted by frontend `IN` sugar before lowering.
pub(in crate::db) const MAX_CHECK_EXPR_V1_MEMBERSHIP_ITEMS: usize = 64;

/// Exact comparison operation admitted by `CheckExprV1`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedCheckCompareOpV1 {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl AcceptedCheckCompareOpV1 {
    const fn tag(self) -> u8 {
        match self {
            Self::Eq => 0,
            Self::Ne => 1,
            Self::Lt => 2,
            Self::Lte => 3,
            Self::Gt => 4,
            Self::Gte => 5,
        }
    }

    pub(in crate::db) const fn requires_ordering(self) -> bool {
        !matches!(self, Self::Eq | Self::Ne)
    }
}

/// Canonical non-null literal admitted through one exact accepted field codec.
///
/// Runtime values are deliberately not persisted here. The payload is the same
/// bounded canonical slot encoding used by the accepted field contract.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedCheckLiteralV1 {
    kind: AcceptedFieldKind,
    storage_decode: FieldStorageDecode,
    leaf_codec: LeafCodec,
    payload: Vec<u8>,
}

impl AcceptedCheckLiteralV1 {
    #[must_use]
    pub(in crate::db) const fn from_accepted_parts(
        kind: AcceptedFieldKind,
        storage_decode: FieldStorageDecode,
        leaf_codec: LeafCodec,
        payload: Vec<u8>,
    ) -> Self {
        Self {
            kind,
            storage_decode,
            leaf_codec,
            payload,
        }
    }

    #[must_use]
    pub(in crate::db) const fn kind(&self) -> &AcceptedFieldKind {
        &self.kind
    }

    #[must_use]
    pub(in crate::db) const fn storage_decode(&self) -> FieldStorageDecode {
        self.storage_decode
    }

    #[must_use]
    pub(in crate::db) const fn leaf_codec(&self) -> LeafCodec {
        self.leaf_codec
    }

    #[must_use]
    pub(in crate::db) const fn payload(&self) -> &[u8] {
        self.payload.as_slice()
    }
}

/// Canonical scalar or bounded-length operand used by `CheckExprV1`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedCheckValueExprV1 {
    Field(FieldId),
    Literal(AcceptedCheckLiteralV1),
    CharLength(FieldId),
    OctetLength(FieldId),
    Cardinality(FieldId),
}

/// Canonical, field-ID-bound accepted row-local check expression.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedCheckExprV1 {
    True,
    False,
    Not(Box<Self>),
    And(Vec<Self>),
    Or(Vec<Self>),
    Compare {
        left: AcceptedCheckValueExprV1,
        op: AcceptedCheckCompareOpV1,
        right: AcceptedCheckValueExprV1,
    },
    IsNull(AcceptedCheckValueExprV1),
    IsNotNull(AcceptedCheckValueExprV1),
}

/// Typed rejection while binding or validating one V1 expression.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AcceptedCheckExprV1Error {
    UnknownField,
    UnsupportedFieldKind,
    UnsupportedOperator,
    OperandKindMismatch,
    LiteralRequiresExpectedKind,
    LiteralAdmissionRejected,
    NullLiteralUnsupported,
    LengthOperationKindMismatch,
    EmptyBoolean,
    MembershipEmpty,
    MembershipTooWide,
    MembershipRequiresEnumField,
    DepthExceeded,
    NodeCountExceeded,
    ChildCountExceeded,
    LiteralBytesExceeded,
    EncodedBytesExceeded,
    NonCanonical,
    #[cfg(feature = "sql")]
    FieldMappingRejected,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
struct CheckExprV1Bounds {
    nodes: u16,
    literal_bytes: usize,
}

impl AcceptedCheckExprV1 {
    /// Return sorted unique accepted field dependencies.
    #[must_use]
    pub(in crate::db) fn dependencies(&self) -> Vec<FieldId> {
        let mut dependencies = Vec::new();
        self.collect_dependencies(&mut dependencies);
        dependencies.sort_unstable();
        dependencies.dedup();
        dependencies
    }

    /// Validate bounds, owner resolution, capabilities, and canonical shape.
    pub(in crate::db) fn validate(
        &self,
        snapshot: &PersistedSchemaSnapshot,
    ) -> Result<(), AcceptedCheckExprV1Error> {
        self.validate_snapshot_local(snapshot.fields())
    }

    pub(in crate::db::schema) fn validate_snapshot_local(
        &self,
        fields: &[crate::db::schema::PersistedFieldSnapshot],
    ) -> Result<(), AcceptedCheckExprV1Error> {
        let mut bounds = CheckExprV1Bounds::default();
        self.validate_at(fields, 0, &mut bounds)?;
        if self.canonical_key().len() > MAX_CHECK_EXPR_V1_BYTES {
            return Err(AcceptedCheckExprV1Error::EncodedBytesExceeded);
        }
        Ok(())
    }

    /// Rewrite field identities during an exact dense-layout mutation.
    #[cfg(feature = "sql")]
    pub(in crate::db) fn clone_with_mapped_field_ids(
        &self,
        map: impl Copy + Fn(FieldId) -> Option<FieldId>,
    ) -> Result<Self, AcceptedCheckExprV1Error> {
        let map_value = |value: &AcceptedCheckValueExprV1| match value {
            AcceptedCheckValueExprV1::Field(field_id) => map(*field_id)
                .map(AcceptedCheckValueExprV1::Field)
                .ok_or(AcceptedCheckExprV1Error::FieldMappingRejected),
            AcceptedCheckValueExprV1::Literal(literal) => {
                Ok(AcceptedCheckValueExprV1::Literal(literal.clone()))
            }
            AcceptedCheckValueExprV1::CharLength(field_id) => map(*field_id)
                .map(AcceptedCheckValueExprV1::CharLength)
                .ok_or(AcceptedCheckExprV1Error::FieldMappingRejected),
            AcceptedCheckValueExprV1::OctetLength(field_id) => map(*field_id)
                .map(AcceptedCheckValueExprV1::OctetLength)
                .ok_or(AcceptedCheckExprV1Error::FieldMappingRejected),
            AcceptedCheckValueExprV1::Cardinality(field_id) => map(*field_id)
                .map(AcceptedCheckValueExprV1::Cardinality)
                .ok_or(AcceptedCheckExprV1Error::FieldMappingRejected),
        };

        match self {
            Self::True => Ok(Self::True),
            Self::False => Ok(Self::False),
            Self::Not(inner) => Ok(Self::Not(Box::new(inner.clone_with_mapped_field_ids(map)?))),
            Self::And(children) => children
                .iter()
                .map(|child| child.clone_with_mapped_field_ids(map))
                .collect::<Result<Vec<_>, _>>()
                .map(Self::And),
            Self::Or(children) => children
                .iter()
                .map(|child| child.clone_with_mapped_field_ids(map))
                .collect::<Result<Vec<_>, _>>()
                .map(Self::Or),
            Self::Compare { left, op, right } => Ok(Self::Compare {
                left: map_value(left)?,
                op: *op,
                right: map_value(right)?,
            }),
            Self::IsNull(value) => map_value(value).map(Self::IsNull),
            Self::IsNotNull(value) => map_value(value).map(Self::IsNotNull),
        }
    }

    pub(super) fn canonicalized_and(children: Vec<Self>) -> Result<Self, AcceptedCheckExprV1Error> {
        canonicalized_boolean(children, true)
    }

    pub(super) fn canonicalized_or(children: Vec<Self>) -> Result<Self, AcceptedCheckExprV1Error> {
        canonicalized_boolean(children, false)
    }

    pub(in crate::db::schema) fn canonical_key(&self) -> Vec<u8> {
        let mut bytes = Vec::new();
        self.write_canonical_key(&mut bytes);
        bytes
    }

    fn collect_dependencies(&self, dependencies: &mut Vec<FieldId>) {
        let mut collect_value = |value: &AcceptedCheckValueExprV1| match value {
            AcceptedCheckValueExprV1::Field(field_id)
            | AcceptedCheckValueExprV1::CharLength(field_id)
            | AcceptedCheckValueExprV1::OctetLength(field_id)
            | AcceptedCheckValueExprV1::Cardinality(field_id) => dependencies.push(*field_id),
            AcceptedCheckValueExprV1::Literal(_) => {}
        };
        match self {
            Self::True | Self::False => {}
            Self::Not(inner) => inner.collect_dependencies(dependencies),
            Self::And(children) | Self::Or(children) => {
                for child in children {
                    child.collect_dependencies(dependencies);
                }
            }
            Self::Compare { left, right, .. } => {
                collect_value(left);
                collect_value(right);
            }
            Self::IsNull(value) | Self::IsNotNull(value) => {
                collect_value(value);
            }
        }
    }

    fn validate_at(
        &self,
        fields: &[crate::db::schema::PersistedFieldSnapshot],
        depth: u16,
        bounds: &mut CheckExprV1Bounds,
    ) -> Result<(), AcceptedCheckExprV1Error> {
        if depth >= MAX_CHECK_EXPR_V1_DEPTH {
            return Err(AcceptedCheckExprV1Error::DepthExceeded);
        }
        bounds.nodes = bounds
            .nodes
            .checked_add(1)
            .ok_or(AcceptedCheckExprV1Error::NodeCountExceeded)?;
        if bounds.nodes > MAX_CHECK_EXPR_V1_NODES {
            return Err(AcceptedCheckExprV1Error::NodeCountExceeded);
        }

        match self {
            Self::True | Self::False => Ok(()),
            Self::Not(inner) => inner.validate_at(fields, depth + 1, bounds),
            Self::And(children) | Self::Or(children) => {
                validate_boolean_children(self, children, fields, depth, bounds)
            }
            Self::Compare { left, op, right } => {
                let left_kind = validate_value_expr(left, fields, bounds)?;
                let right_kind = validate_value_expr(right, fields, bounds)?;
                if left_kind != right_kind {
                    return Err(AcceptedCheckExprV1Error::OperandKindMismatch);
                }
                validate_compare_capability(left_kind, *op)
            }
            Self::IsNull(value) | Self::IsNotNull(value) => {
                let _ = validate_value_expr(value, fields, bounds)?;
                Ok(())
            }
        }
    }

    fn write_canonical_key(&self, bytes: &mut Vec<u8>) {
        match self {
            Self::True => bytes.push(0),
            Self::False => bytes.push(1),
            Self::Not(inner) => {
                bytes.push(2);
                inner.write_canonical_key(bytes);
            }
            Self::And(children) => write_children_key(bytes, 3, children),
            Self::Or(children) => write_children_key(bytes, 4, children),
            Self::Compare { left, op, right } => {
                bytes.extend_from_slice(&[5, op.tag()]);
                write_value_key(bytes, left);
                write_value_key(bytes, right);
            }
            Self::IsNull(value) => {
                bytes.push(6);
                write_value_key(bytes, value);
            }
            Self::IsNotNull(value) => {
                bytes.push(7);
                write_value_key(bytes, value);
            }
        }
    }
}

fn canonicalized_boolean(
    children: Vec<AcceptedCheckExprV1>,
    and: bool,
) -> Result<AcceptedCheckExprV1, AcceptedCheckExprV1Error> {
    if children.is_empty() {
        return Err(AcceptedCheckExprV1Error::EmptyBoolean);
    }
    let mut flattened = Vec::new();
    for child in children {
        match (and, child) {
            (true, AcceptedCheckExprV1::And(nested)) | (false, AcceptedCheckExprV1::Or(nested)) => {
                flattened.extend(nested);
            }
            (_, child) => flattened.push(child),
        }
    }
    if flattened.len() > MAX_CHECK_EXPR_V1_CHILDREN {
        return Err(AcceptedCheckExprV1Error::ChildCountExceeded);
    }
    flattened.sort_by_key(AcceptedCheckExprV1::canonical_key);
    flattened.dedup();
    if flattened.len() == 1 {
        return flattened
            .pop()
            .ok_or(AcceptedCheckExprV1Error::EmptyBoolean);
    }
    Ok(if and {
        AcceptedCheckExprV1::And(flattened)
    } else {
        AcceptedCheckExprV1::Or(flattened)
    })
}

fn validate_boolean_children(
    parent: &AcceptedCheckExprV1,
    children: &[AcceptedCheckExprV1],
    fields: &[crate::db::schema::PersistedFieldSnapshot],
    depth: u16,
    bounds: &mut CheckExprV1Bounds,
) -> Result<(), AcceptedCheckExprV1Error> {
    if children.len() < 2 || children.len() > MAX_CHECK_EXPR_V1_CHILDREN {
        return Err(AcceptedCheckExprV1Error::NonCanonical);
    }
    let nested_is_same = |child: &AcceptedCheckExprV1| {
        matches!(
            (parent, child),
            (AcceptedCheckExprV1::And(_), AcceptedCheckExprV1::And(_))
                | (AcceptedCheckExprV1::Or(_), AcceptedCheckExprV1::Or(_))
        )
    };
    let mut prior: Option<Vec<u8>> = None;
    for child in children {
        if nested_is_same(child) {
            return Err(AcceptedCheckExprV1Error::NonCanonical);
        }
        let key = child.canonical_key();
        if prior.as_ref().is_some_and(|prior| prior >= &key) {
            return Err(AcceptedCheckExprV1Error::NonCanonical);
        }
        prior = Some(key);
        child.validate_at(fields, depth + 1, bounds)?;
    }
    Ok(())
}

fn validate_value_expr<'a>(
    value: &'a AcceptedCheckValueExprV1,
    fields: &'a [crate::db::schema::PersistedFieldSnapshot],
    bounds: &mut CheckExprV1Bounds,
) -> Result<&'a AcceptedFieldKind, AcceptedCheckExprV1Error> {
    match value {
        AcceptedCheckValueExprV1::Field(field_id) => {
            let field = field_for_id_in_fields(fields, *field_id)
                .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
            if matches!(
                field.kind(),
                AcceptedFieldKind::Relation { .. } | AcceptedFieldKind::Composite { .. }
            ) {
                return Err(AcceptedCheckExprV1Error::UnsupportedFieldKind);
            }
            Ok(field.kind())
        }
        AcceptedCheckValueExprV1::Literal(literal) => {
            bounds.literal_bytes = bounds
                .literal_bytes
                .checked_add(literal.payload().len())
                .ok_or(AcceptedCheckExprV1Error::LiteralBytesExceeded)?;
            if literal.payload().is_empty()
                || bounds.literal_bytes > MAX_CHECK_EXPR_V1_LITERAL_BYTES
            {
                return Err(AcceptedCheckExprV1Error::LiteralBytesExceeded);
            }
            Ok(literal.kind())
        }
        AcceptedCheckValueExprV1::CharLength(field_id) => {
            let field = field_for_id_in_fields(fields, *field_id)
                .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
            if !matches!(field.kind(), AcceptedFieldKind::Text { .. }) {
                return Err(AcceptedCheckExprV1Error::LengthOperationKindMismatch);
            }
            Ok(nat64_kind())
        }
        AcceptedCheckValueExprV1::OctetLength(field_id) => {
            let field = field_for_id_in_fields(fields, *field_id)
                .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
            if !matches!(field.kind(), AcceptedFieldKind::Blob { .. }) {
                return Err(AcceptedCheckExprV1Error::LengthOperationKindMismatch);
            }
            Ok(nat64_kind())
        }
        AcceptedCheckValueExprV1::Cardinality(field_id) => {
            let field = field_for_id_in_fields(fields, *field_id)
                .ok_or(AcceptedCheckExprV1Error::UnknownField)?;
            if !matches!(
                field.kind(),
                AcceptedFieldKind::List(_)
                    | AcceptedFieldKind::Set(_)
                    | AcceptedFieldKind::Map { .. }
            ) {
                return Err(AcceptedCheckExprV1Error::LengthOperationKindMismatch);
            }
            Ok(nat64_kind())
        }
    }
}

const fn validate_compare_capability(
    kind: &AcceptedFieldKind,
    op: AcceptedCheckCompareOpV1,
) -> Result<(), AcceptedCheckExprV1Error> {
    if matches!(kind, AcceptedFieldKind::Relation { .. }) {
        return Err(AcceptedCheckExprV1Error::UnsupportedFieldKind);
    }
    let semantics = crate::db::schema::classify_accepted_field_kind(kind);
    if !semantics.is_scalar() || !semantics.is_sql_comparable() {
        return Err(AcceptedCheckExprV1Error::UnsupportedOperator);
    }
    if op.requires_ordering() && !semantics.is_orderable() {
        return Err(AcceptedCheckExprV1Error::UnsupportedOperator);
    }
    Ok(())
}

pub(super) fn field_for_id(
    snapshot: &PersistedSchemaSnapshot,
    field_id: FieldId,
) -> Option<&crate::db::schema::PersistedFieldSnapshot> {
    snapshot
        .fields()
        .iter()
        .find(|field| field.id() == field_id)
}

fn field_for_id_in_fields(
    fields: &[crate::db::schema::PersistedFieldSnapshot],
    field_id: FieldId,
) -> Option<&crate::db::schema::PersistedFieldSnapshot> {
    fields.iter().find(|field| field.id() == field_id)
}

pub(super) const fn nat64_kind() -> &'static AcceptedFieldKind {
    &AcceptedFieldKind::Nat64
}

pub(super) const fn nat64_codec() -> (FieldStorageDecode, LeafCodec) {
    (
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(crate::model::field::ScalarCodec::Nat64),
    )
}

pub(super) fn slot_for_field(
    snapshot: &PersistedSchemaSnapshot,
    field_id: FieldId,
) -> Result<SchemaFieldSlot, AcceptedCheckExprV1Error> {
    field_for_id(snapshot, field_id)
        .map(crate::db::schema::PersistedFieldSnapshot::slot)
        .ok_or(AcceptedCheckExprV1Error::UnknownField)
}

fn write_children_key(bytes: &mut Vec<u8>, tag: u8, children: &[AcceptedCheckExprV1]) {
    bytes.push(tag);
    write_len(bytes, children.len());
    for child in children {
        let key = child.canonical_key();
        write_len(bytes, key.len());
        bytes.extend_from_slice(&key);
    }
}

fn write_value_key(bytes: &mut Vec<u8>, value: &AcceptedCheckValueExprV1) {
    match value {
        AcceptedCheckValueExprV1::Field(field_id) => {
            bytes.push(0);
            bytes.extend_from_slice(&field_id.get().to_be_bytes());
        }
        AcceptedCheckValueExprV1::Literal(literal) => {
            bytes.push(1);
            write_literal_key(bytes, literal);
        }
        AcceptedCheckValueExprV1::CharLength(field_id) => {
            bytes.push(2);
            bytes.extend_from_slice(&field_id.get().to_be_bytes());
        }
        AcceptedCheckValueExprV1::OctetLength(field_id) => {
            bytes.push(3);
            bytes.extend_from_slice(&field_id.get().to_be_bytes());
        }
        AcceptedCheckValueExprV1::Cardinality(field_id) => {
            bytes.push(4);
            bytes.extend_from_slice(&field_id.get().to_be_bytes());
        }
    }
}

fn write_literal_key(bytes: &mut Vec<u8>, literal: &AcceptedCheckLiteralV1) {
    write_kind_key(bytes, literal.kind());
    bytes.push(storage_decode_tag(literal.storage_decode()));
    write_leaf_codec_key(bytes, literal.leaf_codec());
    write_len(bytes, literal.payload().len());
    bytes.extend_from_slice(literal.payload());
}

fn write_kind_key(bytes: &mut Vec<u8>, kind: &AcceptedFieldKind) {
    use AcceptedFieldKind as K;
    match kind {
        K::Account => bytes.push(0),
        K::Blob { max_len } => write_optional_u32(bytes, 1, *max_len),
        K::Bool => bytes.push(2),
        K::Date => bytes.push(3),
        K::Decimal { scale } => write_tag_u32(bytes, 4, *scale),
        K::Duration => bytes.push(5),
        K::Enum { type_id } => write_tag_u32(bytes, 6, type_id.get()),
        K::Float32 => bytes.push(7),
        K::Float64 => bytes.push(8),
        K::Int8 => bytes.push(9),
        K::Int16 => bytes.push(10),
        K::Int32 => bytes.push(11),
        K::Int64 => bytes.push(12),
        K::Int128 => bytes.push(13),
        K::IntBig { max_bytes } => write_tag_u32(bytes, 14, *max_bytes),
        K::Principal => bytes.push(15),
        K::Subaccount => bytes.push(16),
        K::Text { max_len } => write_optional_u32(bytes, 17, *max_len),
        K::Timestamp => bytes.push(18),
        K::Nat8 => bytes.push(19),
        K::Nat16 => bytes.push(20),
        K::Nat32 => bytes.push(21),
        K::Nat64 => bytes.push(22),
        K::Nat128 => bytes.push(23),
        K::NatBig { max_bytes } => write_tag_u32(bytes, 24, *max_bytes),
        K::Ulid => bytes.push(25),
        K::Unit => bytes.push(26),
        K::Relation { .. } => bytes.push(27),
        K::List(inner) => {
            bytes.push(28);
            write_kind_key(bytes, inner);
        }
        K::Set(inner) => {
            bytes.push(29);
            write_kind_key(bytes, inner);
        }
        K::Map { key, value } => {
            bytes.push(30);
            write_kind_key(bytes, key);
            write_kind_key(bytes, value);
        }
        K::Composite { type_id } => write_tag_u32(bytes, 31, type_id.get()),
    }
}

const fn storage_decode_tag(storage_decode: FieldStorageDecode) -> u8 {
    match storage_decode {
        FieldStorageDecode::ByKind => 0,
        FieldStorageDecode::CatalogValue => 1,
    }
}

fn write_leaf_codec_key(bytes: &mut Vec<u8>, codec: LeafCodec) {
    match codec {
        LeafCodec::Structural => bytes.push(0),
        LeafCodec::Scalar(codec) => {
            bytes.push(1);
            bytes.push(scalar_codec_tag(codec));
        }
    }
}

const fn scalar_codec_tag(codec: crate::model::field::ScalarCodec) -> u8 {
    use crate::model::field::ScalarCodec as C;
    match codec {
        C::Blob => 0,
        C::Bool => 1,
        C::Date => 2,
        C::Duration => 3,
        C::Float32 => 4,
        C::Float64 => 5,
        C::Int64 => 6,
        C::Principal => 7,
        C::Subaccount => 8,
        C::Text => 9,
        C::Timestamp => 10,
        C::Nat64 => 11,
        C::Ulid => 12,
        C::Unit => 13,
    }
}

fn write_optional_u32(bytes: &mut Vec<u8>, tag: u8, value: Option<u32>) {
    bytes.push(tag);
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend_from_slice(&value.to_be_bytes());
        }
        None => bytes.push(0),
    }
}

fn write_tag_u32(bytes: &mut Vec<u8>, tag: u8, value: u32) {
    bytes.push(tag);
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn write_len(bytes: &mut Vec<u8>, len: usize) {
    bytes.extend_from_slice(&(len as u64).to_be_bytes());
}
