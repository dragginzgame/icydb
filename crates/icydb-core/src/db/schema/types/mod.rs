//! Module: db::schema::types
//! Responsibility: compact predicate-schema type system for validation and coercion checks.
//! Does not own: planner route selection or runtime predicate execution behavior.
//! Boundary: defines scalar/field type compatibility surfaces used by predicate validation.

#[cfg(test)]
mod filter_tests;
mod query_projection;

#[cfg(any(test, feature = "sql"))]
use crate::value::InputValue;
use crate::{
    db::{
        QueryError,
        codec::hex::decode_hex_bounded,
        predicate::CoercionId,
        query::preparation::PreparationWork,
        schema::{
            AcceptedFieldKind, AcceptedFieldKindCategory, classify_accepted_field_kind,
            field_kind_semantics::scalar_kind_is_sql_comparable,
        },
    },
    types::{
        Account, Date, Decimal, Duration, Float32, Float64, Principal, Subaccount, Timestamp, U256,
        Ulid,
    },
    value::{CoercionFamily, Value},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use icydb_schema::{ScalarCoercionFamily, ScalarKind};
use std::borrow::Cow;
use std::fmt;
use std::str::FromStr;

pub(in crate::db::schema) use query_projection::validate_query_projections;
pub(in crate::db) use query_projection::{
    query_field_is_queryable, query_field_kind_from_persisted_kind,
    query_field_type_from_persisted_kind,
};

const fn scalar_coercion_family(kind: ScalarKind) -> CoercionFamily {
    match kind.coercion_family() {
        ScalarCoercionFamily::Numeric => CoercionFamily::Numeric,
        ScalarCoercionFamily::Textual => CoercionFamily::Textual,
        ScalarCoercionFamily::Identifier => CoercionFamily::Identifier,
        ScalarCoercionFamily::Enum => CoercionFamily::Enum,
        ScalarCoercionFamily::Blob => CoercionFamily::Blob,
        ScalarCoercionFamily::Bool => CoercionFamily::Bool,
        ScalarCoercionFamily::Unit => CoercionFamily::Unit,
    }
}

macro_rules! scalar_kind_matches_value_from_registry {
    ( @args $kind:expr, $value:expr; @entries $( ($scalar:ident, $coercion_family:expr, $value_pat:pat, is_numeric_value = $is_numeric:expr, supports_numeric_coercion = $supports_numeric_coercion:expr, supports_arithmetic = $supports_arithmetic:expr, supports_equality = $supports_equality:expr, supports_ordering = $supports_ordering:expr, is_keyable = $is_keyable:expr, is_primary_key_component_encodable = $is_primary_key_component_encodable:expr) ),* $(,)? ) => {
        matches!(
            ($kind, $value),
            $( (ScalarKind::$scalar, $value_pat) )|*
        )
    };
}

const fn scalar_kind_matches_value(kind: ScalarKind, value: &Value) -> bool {
    scalar_registry!(scalar_kind_matches_value_from_registry, kind, value)
}

///
/// FieldType
///
/// Reduced runtime type representation used exclusively for predicate validation.
/// This intentionally drops:
/// - record structure
/// - tuple structure
/// - validator/normalizer metadata
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum FieldType {
    Scalar(ScalarKind),
    List(Box<Self>),
    Set(Box<Self>),
    Map { key: Box<Self>, value: Box<Self> },
    Composite,
}

impl FieldType {
    /// Select the existing comparer for two SQL-comparable field operands.
    /// Numeric pairs widen; other orderable pairs require the same scalar kind.
    #[must_use]
    pub(in crate::db) fn field_ordering_coercion(&self, right: &Self) -> Option<CoercionId> {
        let (Self::Scalar(left), Self::Scalar(right)) = (self, right) else {
            return None;
        };
        if left.supports_numeric_coercion() && right.supports_numeric_coercion() {
            Some(CoercionId::NumericWiden)
        } else if left == right && left.supports_ordering() && scalar_kind_is_sql_comparable(*left)
        {
            Some(CoercionId::Strict)
        } else {
            None
        }
    }

    #[must_use]
    pub(crate) const fn is_queryable(&self) -> bool {
        matches!(self, Self::Scalar(_) | Self::List(_) | Self::Set(_))
    }

    #[must_use]
    pub(crate) const fn coercion_family(&self) -> Option<CoercionFamily> {
        match self {
            Self::Scalar(inner) => Some(scalar_coercion_family(*inner)),
            Self::List(_) | Self::Set(_) | Self::Map { .. } => Some(CoercionFamily::Collection),
            Self::Composite => None,
        }
    }

    #[must_use]
    pub(crate) const fn is_text(&self) -> bool {
        matches!(self, Self::Scalar(ScalarKind::Text))
    }

    #[must_use]
    pub(crate) const fn is_collection(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_) | Self::Map { .. })
    }

    #[must_use]
    pub(crate) const fn is_list_like(&self) -> bool {
        matches!(self, Self::List(_) | Self::Set(_))
    }

    #[must_use]
    pub(crate) const fn is_orderable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.supports_ordering(),
            _ => false,
        }
    }

    #[must_use]
    pub(crate) const fn is_keyable(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.is_keyable(),
            _ => false,
        }
    }

    #[must_use]
    pub(crate) const fn supports_numeric_coercion(&self) -> bool {
        match self {
            Self::Scalar(inner) => inner.supports_numeric_coercion(),
            _ => false,
        }
    }
}

pub(crate) fn literal_matches_type(literal: &Value, field_type: &FieldType) -> bool {
    match field_type {
        FieldType::Scalar(inner) => scalar_kind_matches_value(*inner, literal),
        FieldType::List(element) | FieldType::Set(element) => match literal {
            Value::List(items) => items.iter().all(|item| literal_matches_type(item, element)),
            _ => false,
        },
        FieldType::Map { key, value } => match literal {
            Value::Map(entries) => {
                if Value::validate_map_entries(entries.as_slice()).is_err() {
                    return false;
                }

                entries.iter().all(|(entry_key, entry_value)| {
                    literal_matches_type(entry_key, key) && literal_matches_type(entry_value, value)
                })
            }
            _ => false,
        },
        FieldType::Composite => {
            // NOTE: exact composite field types never match predicate literals.
            false
        }
    }
}

/// Canonicalize one strict SQL literal against accepted persisted field metadata.
#[must_use]
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn canonicalize_strict_sql_literal_for_persisted_kind(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    let semantics = classify_accepted_field_kind(kind);
    match semantics.category() {
        AcceptedFieldKindCategory::Relation(_) => {
            let AcceptedFieldKind::Relation { key_kind, .. } = kind else {
                return None;
            };

            canonicalize_strict_sql_literal_for_persisted_kind(key_kind, value)
        }
        AcceptedFieldKindCategory::Collection => match kind {
            AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => match value {
                Value::List(values) => values
                    .iter()
                    .map(|item| canonicalize_strict_sql_literal_for_persisted_kind(inner, item))
                    .collect::<Option<Vec<_>>>()
                    .map(Value::List),
                _ => None,
            },
            _ => None,
        },
        AcceptedFieldKindCategory::Composite
        | AcceptedFieldKindCategory::Scalar(
            ScalarKind::Account
            | ScalarKind::Blob
            | ScalarKind::Bool
            | ScalarKind::Date
            | ScalarKind::Duration
            | ScalarKind::Enum
            | ScalarKind::Float32
            | ScalarKind::Float64
            | ScalarKind::Subaccount
            | ScalarKind::Text
            | ScalarKind::Timestamp
            | ScalarKind::Unit,
        ) => None,
        AcceptedFieldKindCategory::Scalar(ScalarKind::Decimal) => {
            let AcceptedFieldKind::Decimal { scale } = kind else {
                return None;
            };
            // Integer SQL spellings lose Decimal display identity. Resolve them
            // exactly against the accepted field scale; other literal domains
            // retain their existing admission rules.
            if !matches!(
                value,
                Value::Int64(_) | Value::Int128(_) | Value::Nat64(_) | Value::Nat128(_)
            ) {
                return None;
            }
            let mantissa =
                crate::db::numeric::coerce_numeric_decimal(value)?.scale_to_integer(*scale)?;
            Decimal::try_from_i128_with_scale(mantissa, *scale).map(Value::Decimal)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::Int) => {
            canonicalize_signed64_persisted_literal(kind, value)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::Nat) => {
            canonicalize_unsigned64_persisted_literal(kind, value)
        }
        AcceptedFieldKindCategory::Scalar(ScalarKind::Int128) => {
            canonicalize_int128_persisted_literal(value)
        }

        AcceptedFieldKindCategory::Scalar(ScalarKind::Nat128) => {
            canonicalize_nat128_persisted_literal(value)
        }

        AcceptedFieldKindCategory::Scalar(ScalarKind::U256) => canonicalize_u256_literal(value),
        AcceptedFieldKindCategory::Scalar(ScalarKind::Principal) => match value {
            Value::Text(inner) => Principal::from_text(inner).ok().map(Value::Principal),
            _ => None,
        },
        AcceptedFieldKindCategory::Scalar(ScalarKind::Ulid) => match value {
            Value::Text(inner) => inner.parse::<Ulid>().ok().map(Value::Ulid),
            _ => None,
        },
    }
}

/// Canonicalize one string-backed public filter literal through accepted kind
/// authority.
///
/// Unlike strict SQL literals, public filter numerics arrive as text so their
/// Wire shape stays stable across narrow and wide numeric field kinds.
pub(in crate::db) fn canonicalize_filter_literal_for_persisted_kind<'a>(
    kind: &AcceptedFieldKind,
    value: &'a Value,
    work: &PreparationWork<'_>,
) -> Result<Option<Cow<'a, Value>>, QueryError> {
    work.charge(Resource::NestedValueSteps, 1)?;
    Ok(match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => {
            canonicalize_filter_literal_for_persisted_kind(key_kind, value, work)?
        }
        AcceptedFieldKind::List(inner) | AcceptedFieldKind::Set(inner) => {
            canonicalize_filter_list(inner, value, work)?
        }
        AcceptedFieldKind::Map { .. } | AcceptedFieldKind::Composite { .. } => None,
        AcceptedFieldKind::Text { .. } => match value {
            Value::Text(_) => Some(Cow::Borrowed(value)),
            _ => None,
        },
        AcceptedFieldKind::Blob { max_len } => {
            let max_len = max_len.map_or(usize::MAX, |len| len as usize);
            match value {
                Value::Blob(bytes) if bytes.len() <= max_len => Some(Cow::Borrowed(value)),
                Value::Text(text) => {
                    charge_filter_hex_decode(text, max_len, work)?;
                    decode_hex_bounded(text.as_bytes(), max_len)
                        .map(|bytes| Cow::Owned(Value::Blob(bytes)))
                }
                _ => None,
            }
        }
        _ => {
            if let Value::Text(text) = value {
                // Charge a conservative input-byte visit allowance before parsing,
                // including rejected text. Parser-internal allocations are separate.
                if matches!(kind, AcceptedFieldKind::Subaccount) {
                    charge_filter_hex_decode(text, 32, work)?;
                } else {
                    work.charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
                }
            }
            canonicalize_filter_scalar_literal(kind, value).map(Cow::Owned)
        }
    })
}

// Keep unchanged collection storage borrowed. Only the first converted child
// requires a separate result; never mutate the source before every child admits.
fn canonicalize_filter_list<'a>(
    inner: &AcceptedFieldKind,
    value: &'a Value,
    work: &PreparationWork<'_>,
) -> Result<Option<Cow<'a, Value>>, QueryError> {
    let Value::List(values) = value else {
        return Ok(None);
    };
    let mut canonical: Option<Vec<Value>> = None;
    for (index, item) in values.iter().enumerate() {
        let Some(item) = canonicalize_filter_literal_for_persisted_kind(inner, item, work)? else {
            return Ok(None);
        };
        if canonical.is_none() && matches!(item, Cow::Owned(_)) {
            work.charge(
                Resource::TemporaryBytes,
                (values.len() as u64).saturating_mul(size_of::<Value>() as u64),
            )?;
            let mut output = Vec::with_capacity(values.len());
            for prefix in &values[..index] {
                output.push(materialize_filter_literal(Cow::Borrowed(prefix), work)?);
            }
            canonical = Some(output);
        }
        if let Some(output) = &mut canonical {
            output.push(materialize_filter_literal(item, work)?);
        }
    }
    Ok(Some(match canonical {
        Some(values) => Cow::Owned(Value::List(values)),
        None => Cow::Borrowed(value),
    }))
}

// Match the shared decoder's pre-allocation length checks; malformed digits can
// fail only after its output buffer has been allocated, so they still charge.
fn charge_filter_hex_decode(
    text: &str,
    max_len: usize,
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    work.charge(Resource::PredicateExpressionSteps, text.len() as u64)?;
    if text.len().is_multiple_of(2) && text.len() / 2 <= max_len {
        work.charge(Resource::TemporaryBytes, (text.len() / 2) as u64)?;
    }
    Ok(())
}

/// Materialize a normalized filter literal only when a new container needs an
/// owned child. Borrowed storage contains unchanged text/blob
/// scalars or lists composed of borrowed children.
fn materialize_filter_literal(
    value: Cow<'_, Value>,
    work: &PreparationWork<'_>,
) -> Result<Value, QueryError> {
    match value {
        Cow::Owned(value) => Ok(value),
        Cow::Borrowed(value) => work.copy_value(value),
    }
}

/// Canonicalize one collection-containment literal through the field's
/// accepted element kind.
pub(in crate::db) fn canonicalize_filter_collection_element_for_persisted_kind<'a>(
    field_kind: &AcceptedFieldKind,
    value: &'a Value,
    work: &PreparationWork<'_>,
) -> Result<Option<Cow<'a, Value>>, QueryError> {
    match field_kind {
        AcceptedFieldKind::List(element_kind) | AcceptedFieldKind::Set(element_kind) => {
            canonicalize_filter_literal_for_persisted_kind(element_kind, value, work)
        }
        _ => Ok(None),
    }
}

fn canonicalize_filter_scalar_literal(kind: &AcceptedFieldKind, value: &Value) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Account => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Account(inner) => Some(*inner),
                _ => None,
            },
            Account::from_str,
            Value::Account,
        ),
        AcceptedFieldKind::Bool => match value {
            Value::Bool(inner) => Some(Value::Bool(*inner)),
            _ => None,
        },
        AcceptedFieldKind::Decimal { .. } => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Decimal(inner) => Some(*inner),
                _ => None,
            },
            Decimal::from_str,
            Value::Decimal,
        ),
        AcceptedFieldKind::Float32 => match value {
            Value::Float32(inner) => Some(Value::Float32(*inner)),
            Value::Text(inner) => inner
                .parse::<f32>()
                .ok()
                .and_then(Float32::try_new)
                .map(Value::Float32),
            _ => None,
        },
        AcceptedFieldKind::Float64 => match value {
            Value::Float64(inner) => Some(Value::Float64(*inner)),
            Value::Text(inner) => inner
                .parse::<f64>()
                .ok()
                .and_then(Float64::try_new)
                .map(Value::Float64),
            _ => None,
        },
        AcceptedFieldKind::Principal => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Principal(inner) => Some(*inner),
                _ => None,
            },
            Principal::from_str,
            Value::Principal,
        ),
        AcceptedFieldKind::Ulid => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Ulid(inner) => Some(*inner),
                _ => None,
            },
            Ulid::from_str,
            Value::Ulid,
        ),
        AcceptedFieldKind::Unit => match value {
            Value::Null | Value::Unit => Some(Value::Unit),
            _ => None,
        },
        AcceptedFieldKind::U256 => canonicalize_u256_literal(value),
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64
        | AcceptedFieldKind::Int128
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128 => canonicalize_filter_numeric_literal(kind, value),
        AcceptedFieldKind::Date
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Timestamp => canonicalize_filter_string_backed_atom(kind, value),
        AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Set(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Composite { .. } => None,
    }
}

fn canonicalize_filter_string_backed_atom(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Date => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Date(inner) => Some(*inner),
                _ => None,
            },
            |value| Date::parse(value).ok_or(()),
            Value::Date,
        ),
        AcceptedFieldKind::Duration => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Duration(inner) => Some(*inner),
                _ => None,
            },
            Duration::parse_flexible,
            Value::Duration,
        ),
        AcceptedFieldKind::Subaccount => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Subaccount(inner) => Some(*inner),
                _ => None,
            },
            |value| {
                let bytes = decode_hex_bounded(value.as_bytes(), 32).ok_or(())?;
                <[u8; 32]>::try_from(bytes)
                    .map(Subaccount::from_array)
                    .map_err(|_| ())
            },
            Value::Subaccount,
        ),
        AcceptedFieldKind::Timestamp => canonicalize_text_or_exact(
            value,
            |value| match value {
                Value::Timestamp(inner) => Some(*inner),
                _ => None,
            },
            Timestamp::parse_flexible,
            Value::Timestamp,
        ),
        _ => None,
    }
}

fn canonicalize_filter_numeric_literal(kind: &AcceptedFieldKind, value: &Value) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Int8
        | AcceptedFieldKind::Int16
        | AcceptedFieldKind::Int32
        | AcceptedFieldKind::Int64 => canonicalize_filter_int(kind, value),
        AcceptedFieldKind::Int128 => match value {
            Value::Int128(inner) => Some(Value::Int128(*inner)),
            Value::Text(inner) => inner.parse::<i128>().ok().map(Value::Int128),
            _ => None,
        },
        AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64 => canonicalize_filter_nat(kind, value),
        AcceptedFieldKind::Nat128 => match value {
            Value::Nat128(inner) => Some(Value::Nat128(*inner)),
            Value::Text(inner) => inner.parse::<u128>().ok().map(Value::Nat128),
            _ => None,
        },
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
        | AcceptedFieldKind::Date
        | AcceptedFieldKind::Decimal { .. }
        | AcceptedFieldKind::Duration
        | AcceptedFieldKind::Enum { .. }
        | AcceptedFieldKind::Float32
        | AcceptedFieldKind::Float64
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256
        | AcceptedFieldKind::Relation { .. }
        | AcceptedFieldKind::List(_)
        | AcceptedFieldKind::Set(_)
        | AcceptedFieldKind::Map { .. }
        | AcceptedFieldKind::Composite { .. } => None,
    }
}

fn canonicalize_u256_literal(value: &Value) -> Option<Value> {
    match value {
        Value::U256(value) => Some(Value::U256(*value)),
        Value::Nat64(value) => Some(Value::U256(U256::from(*value))),
        Value::Nat128(value) => Some(Value::U256(U256::from(*value))),
        Value::Text(value) => value.parse().ok().map(Value::U256),
        _ => None,
    }
}

fn canonicalize_text_or_exact<T, E>(
    value: &Value,
    exact: impl FnOnce(&Value) -> Option<T>,
    parse: impl FnOnce(&str) -> Result<T, E>,
    wrap: impl FnOnce(T) -> Value,
) -> Option<Value> {
    if let Some(exact) = exact(value) {
        return Some(wrap(exact));
    }
    match value {
        Value::Text(inner) => parse(inner).ok().map(wrap),
        _ => None,
    }
}

fn canonicalize_filter_int(kind: &AcceptedFieldKind, value: &Value) -> Option<Value> {
    // Text parsing belongs to filter admission; typed range checks are shared
    // with strict persisted literals, which must continue to reject text.
    if let Value::Text(inner) = value {
        return canonicalize_signed64_persisted_literal(kind, &Value::Int64(inner.parse().ok()?));
    }
    canonicalize_signed64_persisted_literal(kind, value)
}

fn canonicalize_filter_nat(kind: &AcceptedFieldKind, value: &Value) -> Option<Value> {
    if let Value::Text(inner) = value {
        return canonicalize_unsigned64_persisted_literal(kind, &Value::Nat64(inner.parse().ok()?));
    }
    canonicalize_unsigned64_persisted_literal(kind, value)
}

/// Target-type an evaluated SQL assignment without widening literal admission.
///
/// Scalar arithmetic returns Decimal. Integral results may enter integer fields
/// only after exact, checked conversion; the existing literal owner still checks
/// narrow widths, relation keys and all other accepted field families.
#[must_use]
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn input_value_from_sql_assignment_result_for_persisted_kind(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<InputValue> {
    if let AcceptedFieldKind::Relation { key_kind, .. } = kind {
        return input_value_from_sql_assignment_result_for_persisted_kind(key_kind, value);
    }
    if let Value::Decimal(decimal) = value {
        let converted = match kind {
            AcceptedFieldKind::Int8
            | AcceptedFieldKind::Int16
            | AcceptedFieldKind::Int32
            | AcceptedFieldKind::Int64 => Some(Value::Int64(decimal.to_i64()?)),
            AcceptedFieldKind::Nat8
            | AcceptedFieldKind::Nat16
            | AcceptedFieldKind::Nat32
            | AcceptedFieldKind::Nat64 => Some(Value::Nat64(decimal.to_u64()?)),
            _ => None,
        };
        if let Some(converted) = converted {
            let normalized = canonicalize_strict_sql_literal_for_persisted_kind(kind, &converted)?;
            return InputValue::try_from_runtime_non_enum(&normalized);
        }
    }
    input_value_from_strict_sql_literal_for_persisted_kind(kind, value)
}

/// Target-type one strict SQL literal against accepted persisted metadata.
///
/// Enum labels remain unresolved authored input until catalog admission. Other
/// field kinds retain the existing strict SQL canonicalization rules.
#[must_use]
#[cfg(any(test, feature = "sql"))]
pub(in crate::db) fn input_value_from_strict_sql_literal_for_persisted_kind(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<InputValue> {
    if matches!(kind, AcceptedFieldKind::Enum { .. }) {
        let Value::Text(variant_name) = value else {
            return None;
        };
        return Some(InputValue::loose_enum(variant_name.clone()));
    }

    let normalized = canonicalize_strict_sql_literal_for_persisted_kind(kind, value)
        .unwrap_or_else(|| value.clone());
    literal_matches_type(&normalized, &field_type_from_persisted_kind(kind))
        .then(|| InputValue::try_from_runtime_non_enum(&normalized))
        .flatten()
}

fn canonicalize_signed64_persisted_literal(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Int64 => canonicalize_int_persisted_literal(value, i64::MIN, i64::MAX),
        AcceptedFieldKind::Int8 => {
            canonicalize_int_persisted_literal(value, i64::from(i8::MIN), i64::from(i8::MAX))
        }
        AcceptedFieldKind::Int16 => {
            canonicalize_int_persisted_literal(value, i64::from(i16::MIN), i64::from(i16::MAX))
        }
        AcceptedFieldKind::Int32 => {
            canonicalize_int_persisted_literal(value, i64::from(i32::MIN), i64::from(i32::MAX))
        }
        _ => None,
    }
}

fn canonicalize_unsigned64_persisted_literal(
    kind: &AcceptedFieldKind,
    value: &Value,
) -> Option<Value> {
    match kind {
        AcceptedFieldKind::Nat64 => canonicalize_nat_persisted_literal(value, u64::MAX),
        AcceptedFieldKind::Nat8 => canonicalize_nat_persisted_literal(value, u64::from(u8::MAX)),
        AcceptedFieldKind::Nat16 => canonicalize_nat_persisted_literal(value, u64::from(u16::MAX)),
        AcceptedFieldKind::Nat32 => canonicalize_nat_persisted_literal(value, u64::from(u32::MAX)),
        _ => None,
    }
}

pub(in crate::db) fn field_type_from_persisted_kind(kind: &AcceptedFieldKind) -> FieldType {
    match kind {
        AcceptedFieldKind::Relation { key_kind, .. } => field_type_from_persisted_kind(key_kind),
        AcceptedFieldKind::List(inner) => {
            FieldType::List(Box::new(field_type_from_persisted_kind(inner)))
        }
        AcceptedFieldKind::Set(inner) => {
            FieldType::Set(Box::new(field_type_from_persisted_kind(inner)))
        }
        AcceptedFieldKind::Map { key, value } => FieldType::Map {
            key: Box::new(field_type_from_persisted_kind(key)),
            value: Box::new(field_type_from_persisted_kind(value)),
        },
        AcceptedFieldKind::Composite { .. } => FieldType::Composite,
        AcceptedFieldKind::Account
        | AcceptedFieldKind::Blob { .. }
        | AcceptedFieldKind::Bool
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
        | AcceptedFieldKind::Principal
        | AcceptedFieldKind::Subaccount
        | AcceptedFieldKind::Text { .. }
        | AcceptedFieldKind::Timestamp
        | AcceptedFieldKind::Nat8
        | AcceptedFieldKind::Nat16
        | AcceptedFieldKind::Nat32
        | AcceptedFieldKind::Nat64
        | AcceptedFieldKind::Nat128
        | AcceptedFieldKind::Ulid
        | AcceptedFieldKind::Unit
        | AcceptedFieldKind::U256 => scalar_field_type_from_persisted_kind(kind),
    }
}

fn scalar_field_type_from_persisted_kind(kind: &AcceptedFieldKind) -> FieldType {
    let AcceptedFieldKindCategory::Scalar(kind) = classify_accepted_field_kind(kind).category()
    else {
        debug_assert!(false, "scalar accepted field kind must classify as scalar");
        return FieldType::Composite;
    };

    FieldType::Scalar(kind)
}

fn canonicalize_int_persisted_literal(value: &Value, min: i64, max: i64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => *inner,
        Value::Nat64(inner) => i64::try_from(*inner).ok()?,
        _ => return None,
    };

    (min..=max).contains(&value).then_some(Value::Int64(value))
}

fn canonicalize_nat_persisted_literal(value: &Value, max: u64) -> Option<Value> {
    let value = match value {
        Value::Int64(inner) => u64::try_from(*inner).ok()?,
        Value::Nat64(inner) => *inner,
        _ => return None,
    };

    (value <= max).then_some(Value::Nat64(value))
}

#[cfg(any(test, feature = "sql"))]
fn canonicalize_int128_persisted_literal(value: &Value) -> Option<Value> {
    let value = match value {
        // Bare integers above u64 within Decimal's range arrive as scale-zero atoms.
        Value::Decimal(inner) => inner.scale_to_integer(0)?,
        Value::Int64(inner) => i128::from(*inner),
        Value::Nat64(inner) => i128::from(*inner),
        Value::Int128(inner) => *inner,
        Value::Nat128(inner) => i128::try_from(*inner).ok()?,
        Value::Text(inner) => inner.parse::<i128>().ok()?,
        _ => return None,
    };

    Some(Value::Int128(value))
}

#[cfg(any(test, feature = "sql"))]
fn canonicalize_nat128_persisted_literal(value: &Value) -> Option<Value> {
    let value = match value {
        Value::Decimal(inner) => u128::try_from(inner.scale_to_integer(0)?).ok()?,
        Value::Int64(inner) => u128::try_from(*inner).ok()?,
        Value::Nat64(inner) => u128::from(*inner),
        Value::Int128(inner) => u128::try_from(*inner).ok()?,
        Value::Nat128(inner) => *inner,
        Value::Text(inner) => inner.parse::<u128>().ok()?,
        _ => return None,
    };

    Some(Value::Nat128(value))
}

impl fmt::Display for FieldType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Scalar(inner) => write!(f, "{inner:?}"),
            Self::List(inner) => write!(f, "List<{inner}>"),
            Self::Set(inner) => write!(f, "Set<{inner}>"),
            Self::Map { key, value } => write!(f, "Map<{key}, {value}>"),
            Self::Composite => f.write_str("Composite"),
        }
    }
}

#[cfg(all(test, feature = "sql"))]
mod tests;

// Exhaustive cache-retention coverage; new owned fields require accounting.
crate::retained::retained_fields!(FieldType {
Self::Scalar(field_0) => [field_0],
Self::List(field_0) => [field_0],
Self::Set(field_0) => [field_0],
Self::Map{key,value} => [key,value],
Self::Composite => [],
});
