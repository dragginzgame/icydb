//! Module: query::fingerprint::hash_sections
//! Responsibility: canonical field/tag encoding for plan-hash profiles.
//! Does not own: plan explain projection or token transport.
//! Boundary: reusable hash primitives for fingerprints and continuation signatures.
#![expect(clippy::cast_possible_truncation)]

mod access;
mod grouping;
mod profile;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        codec::{write_hash_str_u32, write_hash_tag_u8, write_hash_u32},
        predicate::{Predicate, hash_predicate as hash_model_predicate},
        query::{
            fingerprint::projection_hash::hash_scalar_filter_expr_structural_fingerprint,
            plan::{OrderDirection, OrderSpec, QueryMode, expr::Expr},
        },
    },
    error::InternalError,
    value::{Value, hash_value},
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

pub(in crate::db::query) use profile::hash_continuation_with_projection;

const ACCESS_TAG_BY_KEY: u8 = 0x10;
const ACCESS_TAG_BY_KEYS: u8 = 0x11;
const ACCESS_TAG_KEY_RANGE: u8 = 0x12;
const ACCESS_TAG_INDEX_PREFIX: u8 = 0x13;
const ACCESS_TAG_FULL_SCAN: u8 = 0x14;
const ACCESS_TAG_UNION: u8 = 0x15;
const ACCESS_TAG_INTERSECTION: u8 = 0x16;
const ACCESS_TAG_INDEX_RANGE: u8 = 0x17;
const ACCESS_TAG_INDEX_MULTI_LOOKUP: u8 = 0x18;
const ACCESS_TAG_INDEX_BRANCH_SET: u8 = 0x19;

const PREDICATE_ABSENT_TAG: u8 = 0x20;
const FILTER_EXPR_PRESENT_TAG: u8 = 0x21;

const ORDER_NONE_TAG: u8 = 0x30;
const ORDER_FIELDS_TAG: u8 = 0x31;

const DISTINCT_ENABLED_TAG: u8 = 0x44;
const DISTINCT_DISABLED_TAG: u8 = 0x45;

const QUERY_MODE_LOAD_TAG: u8 = 0x60;
const QUERY_MODE_DELETE_TAG: u8 = 0x61;

const GROUPING_NONE_TAG: u8 = 0x70;
const GROUPING_PRESENT_TAG: u8 = 0x71;
const GROUPING_STRATEGY_HASH_TAG: u8 = 0x72;
const GROUPING_STRATEGY_ORDERED_TAG: u8 = 0x73;
const GROUP_HAVING_ABSENT_TAG: u8 = 0x74;
const GROUP_HAVING_PRESENT_TAG: u8 = 0x75;
const GROUP_HAVING_COMPARE_TAG: u8 = 0x76;
const GROUP_HAVING_AND_TAG: u8 = 0x77;
const GROUP_HAVING_VALUE_GROUP_FIELD_TAG: u8 = 0x78;
const GROUP_HAVING_VALUE_AGGREGATE_INDEX_TAG: u8 = 0x79;
const GROUP_HAVING_VALUE_LITERAL_TAG: u8 = 0x7A;
const GROUP_HAVING_VALUE_FUNCTION_TAG: u8 = 0x7B;
const GROUP_HAVING_VALUE_BINARY_TAG: u8 = 0x7C;
const GROUP_HAVING_VALUE_UNARY_TAG: u8 = 0x7D;
const GROUP_HAVING_VALUE_CASE_TAG: u8 = 0x7E;
const GROUP_HAVING_VALUE_CASE_ARM_TAG: u8 = 0x7F;
const GROUP_HAVING_VALUE_EXPR_TAG: u8 = 0x80;
const GROUP_HAVING_VALUE_FIELD_PATH_TAG: u8 = 0x81;
const GROUP_FIELD_DIRECT_TAG: u8 = 0x82;
const GROUP_FIELD_SCALAR_PATH_TAG: u8 = 0x83;

const VALUE_BOUND_UNBOUNDED_TAG: u8 = 0x00;
const VALUE_BOUND_INCLUDED_TAG: u8 = 0x01;
const VALUE_BOUND_EXCLUDED_TAG: u8 = 0x02;

const ORDER_DIRECTION_ASC_TAG: u8 = 0x01;
const ORDER_DIRECTION_DESC_TAG: u8 = 0x02;

pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_ENTITY_PATH_TAG:
    u8 = 0x01;
pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_MODE_TAG: u8 = 0x02;
pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_ACCESS_TAG: u8 =
    0x03;
pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_PREDICATE_TAG: u8 =
    0x04;
pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_ORDER_TAG: u8 =
    0x05;
pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_DISTINCT_TAG: u8 =
    0x06;
pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_GROUPING_SHAPE_TAG:
    u8 = 0x07;
pub(in crate::db::query::fingerprint::hash_sections) const CONTINUATION_SECTION_PROJECTION_SPEC_TAG:
    u8 = 0x08;

///
/// Hash canonical predicate model structure into the plan hash stream.
///
pub(super) fn hash_predicate(hasher: &mut Sha256, predicate: Option<&Predicate>) {
    let Some(predicate) = predicate else {
        write_tag(hasher, PREDICATE_ABSENT_TAG);
        return;
    };

    hash_model_predicate(hasher, predicate);
}

///
/// Hash one scalar semantic filter component into the shared identity stream.
///
/// Canonical scalar `filter_expr` owns semantic identity when present; the
/// predicate hash is used only for plans that still have no planner-owned
/// scalar filter expression.
///
pub(super) fn hash_scalar_semantic_filter(
    hasher: &mut Sha256,
    filter_expr: Option<&Expr>,
    predicate: Option<&Predicate>,
) -> Result<(), InternalError> {
    if let Some(filter_expr) = filter_expr {
        write_tag(hasher, FILTER_EXPR_PRESENT_TAG);
        hash_scalar_filter_expr_structural_fingerprint(hasher, filter_expr)?;

        return Ok(());
    }

    hash_predicate(hasher, predicate);
    Ok(())
}

// Render only the order term currently being hashed.
pub(super) fn hash_order_spec(hasher: &mut Sha256, order: Option<&OrderSpec>) {
    match order {
        Some(order) if !order.fields.is_empty() => hash_order_fields(
            hasher,
            order
                .fields
                .iter()
                .map(|term| (term.rendered_label(), term.direction())),
        ),
        Some(_) | None => write_tag(hasher, ORDER_NONE_TAG),
    }
}

fn hash_order_fields<S: AsRef<str>>(
    hasher: &mut Sha256,
    fields: impl ExactSizeIterator<Item = (S, OrderDirection)>,
) {
    write_tag(hasher, ORDER_FIELDS_TAG);
    write_u32(hasher, fields.len() as u32);
    for (field, direction) in fields {
        write_str(hasher, field.as_ref());
        write_tag(hasher, order_direction_tag(direction));
    }
}

///
/// Hash query mode into the plan hash stream.
///

pub(super) fn hash_mode(hasher: &mut Sha256, mode: QueryMode) {
    match mode {
        QueryMode::Load(_) => write_tag(hasher, QUERY_MODE_LOAD_TAG),
        QueryMode::Delete(_) => write_tag(hasher, QUERY_MODE_DELETE_TAG),
    }
}

///
/// Encode one value digest into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_value(
    hasher: &mut Sha256,
    value: &Value,
) -> Result<(), InternalError> {
    // An incomplete value hash must never become a usable identity.
    hasher.update(hash_value(value)?);
    Ok(())
}

///
/// Encode one value bound into the plan hash stream.
///
pub(super) fn write_value_bound(
    hasher: &mut Sha256,
    bound: &Bound<Value>,
) -> Result<(), InternalError> {
    match bound {
        Bound::Unbounded => write_tag(hasher, VALUE_BOUND_UNBOUNDED_TAG),
        Bound::Included(value) => {
            write_tag(hasher, VALUE_BOUND_INCLUDED_TAG);
            write_value(hasher, value)?;
        }
        Bound::Excluded(value) => {
            write_tag(hasher, VALUE_BOUND_EXCLUDED_TAG);
            write_value(hasher, value)?;
        }
    }
    Ok(())
}

///
/// Encode one string with length prefix into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_str(hasher: &mut Sha256, value: &str) {
    write_hash_str_u32(hasher, value);
}

///
/// Encode one u32 in network byte order into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_u32(hasher: &mut Sha256, value: u32) {
    write_hash_u32(hasher, value);
}

///
/// Encode one tag byte into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_tag(hasher: &mut Sha256, tag: u8) {
    write_hash_tag_u8(hasher, tag);
}

const fn order_direction_tag(direction: OrderDirection) -> u8 {
    match direction {
        OrderDirection::Asc => ORDER_DIRECTION_ASC_TAG,
        OrderDirection::Desc => ORDER_DIRECTION_DESC_TAG,
    }
}

pub(super) fn hash_distinct(hasher: &mut Sha256, distinct: bool) {
    if distinct {
        write_tag(hasher, DISTINCT_ENABLED_TAG);
    } else {
        write_tag(hasher, DISTINCT_DISABLED_TAG);
    }
}
