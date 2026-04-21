//! Module: query::fingerprint::hash_parts
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
        predicate::{MissingRowPolicy, Predicate, hash_predicate as hash_model_predicate},
        query::{
            explain::{ExplainDeleteLimit, ExplainOrderBy, ExplainPagination},
            fingerprint::projection_hash::hash_scalar_filter_expr_structural_fingerprint,
            plan::{DeleteLimitSpec, OrderDirection, OrderSpec, PageSpec, QueryMode, expr::Expr},
        },
    },
    value::{Value, hash_value},
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

pub(in crate::db::query) use profile::{
    ExplainHashProfile, hash_explain_plan_profile, hash_planned_query_profile_with_projection,
};

const ACCESS_TAG_BY_KEY: u8 = 0x10;
const ACCESS_TAG_BY_KEYS: u8 = 0x11;
const ACCESS_TAG_KEY_RANGE: u8 = 0x12;
const ACCESS_TAG_INDEX_PREFIX: u8 = 0x13;
const ACCESS_TAG_FULL_SCAN: u8 = 0x14;
const ACCESS_TAG_UNION: u8 = 0x15;
const ACCESS_TAG_INTERSECTION: u8 = 0x16;
const ACCESS_TAG_INDEX_RANGE: u8 = 0x17;
const ACCESS_TAG_INDEX_MULTI_LOOKUP: u8 = 0x18;

const PREDICATE_ABSENT_TAG: u8 = 0x20;
const FILTER_EXPR_PRESENT_TAG: u8 = 0x21;

const ORDER_NONE_TAG: u8 = 0x30;
const ORDER_FIELDS_TAG: u8 = 0x31;

const PAGE_NONE_TAG: u8 = 0x40;
const PAGE_PRESENT_TAG: u8 = 0x41;
const DELETE_LIMIT_NONE_TAG: u8 = 0x42;
const DELETE_LIMIT_PRESENT_TAG: u8 = 0x43;
const DISTINCT_ENABLED_TAG: u8 = 0x44;
const DISTINCT_DISABLED_TAG: u8 = 0x45;

const CONSISTENCY_IGNORE_TAG: u8 = 0x50;
const CONSISTENCY_ERROR_TAG: u8 = 0x51;

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

const HASH_VALUE_ERROR_TAG: u8 = 0xEE;

const VALUE_BOUND_UNBOUNDED_TAG: u8 = 0x00;
const VALUE_BOUND_INCLUDED_TAG: u8 = 0x01;
const VALUE_BOUND_EXCLUDED_TAG: u8 = 0x02;

const OPTIONAL_VALUE_ABSENT_TAG: u8 = 0x00;
const OPTIONAL_VALUE_PRESENT_TAG: u8 = 0x01;

const ORDER_DIRECTION_ASC_TAG: u8 = 0x01;
const ORDER_DIRECTION_DESC_TAG: u8 = 0x02;

pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_ACCESS_TAG: u8 = 0x01;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_PREDICATE_TAG: u8 =
    0x02;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_ORDER_TAG: u8 = 0x03;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_DISTINCT_TAG: u8 = 0x04;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_PAGE_TAG: u8 = 0x05;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_DELETE_LIMIT_TAG: u8 =
    0x06;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_CONSISTENCY_TAG: u8 =
    0x07;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_MODE_TAG: u8 = 0x08;
pub(in crate::db::query::fingerprint::hash_parts) const FINGERPRINT_SECTION_PROJECTION_SPEC_TAG:
    u8 = 0x09;

pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_ENTITY_PATH_TAG: u8 =
    0x01;
pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_MODE_TAG: u8 = 0x02;
pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_ACCESS_TAG: u8 = 0x03;
pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_PREDICATE_TAG: u8 =
    0x04;
pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_ORDER_TAG: u8 = 0x05;
pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_DISTINCT_TAG: u8 =
    0x06;
pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_GROUPING_SHAPE_TAG:
    u8 = 0x07;
pub(in crate::db::query::fingerprint::hash_parts) const CONTINUATION_SECTION_PROJECTION_SPEC_TAG:
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
/// older predicate hash remains the fallback only for plans that still have no
/// planner-owned scalar filter expression.
///
pub(super) fn hash_scalar_semantic_filter(
    hasher: &mut Sha256,
    filter_expr: Option<&Expr>,
    predicate: Option<&Predicate>,
) {
    if let Some(filter_expr) = filter_expr {
        write_tag(hasher, FILTER_EXPR_PRESENT_TAG);
        hash_scalar_filter_expr_structural_fingerprint(hasher, filter_expr);

        return;
    }

    hash_predicate(hasher, predicate);
}

///
/// Hash explain order specs into the plan hash stream.
///

///
/// ProjectedOrderShape
///
/// Canonical order projection shared by logical-plan and explain hashing.
/// Both surfaces normalize into the same ordered field list before the
/// canonical order hash is written.
///

enum ProjectedOrderShape {
    None,
    Fields(Vec<(String, OrderDirection)>),
}

impl ProjectedOrderShape {
    fn from_explain(order: &ExplainOrderBy) -> Self {
        match order {
            ExplainOrderBy::None => Self::None,
            ExplainOrderBy::Fields(fields) => Self::Fields(
                fields
                    .iter()
                    .map(|field| (field.field().to_owned(), field.direction()))
                    .collect(),
            ),
        }
    }

    fn from_plan(order: Option<&OrderSpec>) -> Self {
        match order {
            Some(order) if !order.fields.is_empty() => Self::Fields(
                order
                    .fields
                    .iter()
                    .map(|term| (term.rendered_label(), term.direction()))
                    .collect(),
            ),
            Some(_) | None => Self::None,
        }
    }
}

pub(super) fn hash_order(hasher: &mut Sha256, order: &ExplainOrderBy) {
    hash_projected_order_shape(hasher, &ProjectedOrderShape::from_explain(order));
}

pub(super) fn hash_order_spec(hasher: &mut Sha256, order: Option<&OrderSpec>) {
    hash_projected_order_shape(hasher, &ProjectedOrderShape::from_plan(order));
}

fn hash_projected_order_shape(hasher: &mut Sha256, order: &ProjectedOrderShape) {
    match order {
        ProjectedOrderShape::None => write_tag(hasher, ORDER_NONE_TAG),
        ProjectedOrderShape::Fields(fields) => {
            write_tag(hasher, ORDER_FIELDS_TAG);
            write_u32(hasher, fields.len() as u32);
            for (field, direction) in fields {
                write_str(hasher, field);
                write_tag(hasher, order_direction_tag(*direction));
            }
        }
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

pub(in crate::db) fn write_value(hasher: &mut Sha256, value: &Value) {
    match hash_value(value) {
        Ok(digest) => hasher.update(digest),
        Err(err) => {
            write_tag(hasher, HASH_VALUE_ERROR_TAG);
            write_str(hasher, &err.display_with_class());
        }
    }
}

///
/// Encode one value bound into the plan hash stream.
///
pub(super) fn write_value_bound(hasher: &mut Sha256, bound: &Bound<Value>) {
    match bound {
        Bound::Unbounded => write_tag(hasher, VALUE_BOUND_UNBOUNDED_TAG),
        Bound::Included(value) => {
            write_tag(hasher, VALUE_BOUND_INCLUDED_TAG);
            write_value(hasher, value);
        }
        Bound::Excluded(value) => {
            write_tag(hasher, VALUE_BOUND_EXCLUDED_TAG);
            write_value(hasher, value);
        }
    }
}

///
/// Encode one string with length prefix into the plan hash stream.
///

pub(in crate::db) fn write_str(hasher: &mut Sha256, value: &str) {
    write_hash_str_u32(hasher, value);
}

///
/// Encode one u32 in network byte order into the plan hash stream.
///

pub(in crate::db) fn write_u32(hasher: &mut Sha256, value: u32) {
    write_hash_u32(hasher, value);
}

///
/// Encode one optional `u32` into the plan hash stream.
///

pub(in crate::db) fn write_optional_u32(hasher: &mut Sha256, value: Option<u32>) {
    match value {
        Some(value) => {
            write_tag(hasher, 1);
            write_u32(hasher, value);
        }
        None => write_tag(hasher, 0),
    }
}

///
/// Encode one tag byte into the plan hash stream.
///

pub(in crate::db) fn write_tag(hasher: &mut Sha256, tag: u8) {
    write_hash_tag_u8(hasher, tag);
}

const fn order_direction_tag(direction: OrderDirection) -> u8 {
    match direction {
        OrderDirection::Asc => ORDER_DIRECTION_ASC_TAG,
        OrderDirection::Desc => ORDER_DIRECTION_DESC_TAG,
    }
}

///
/// ProjectedPageWindow
///
/// Canonical pagination projection shared by logical-plan and explain hashing.
/// Both surfaces normalize into the same optional-limit plus offset shape
/// before the canonical page hash is written.
///

enum ProjectedPageWindow {
    None,
    Page { limit: Option<u32>, offset: u32 },
}

///
/// ProjectedDeleteWindow
///
/// Canonical delete-limit projection shared by logical-plan and explain hashing.
/// Explain-only fixed-row delete limits normalize into the same limit/offset
/// window shape used by planner-owned delete limit specs.
///

enum ProjectedDeleteWindow {
    None,
    Window { limit: Option<u32>, offset: u32 },
}

impl ProjectedPageWindow {
    const fn from_explain(page: &ExplainPagination) -> Self {
        match page {
            ExplainPagination::None => Self::None,
            ExplainPagination::Page { limit, offset } => Self::Page {
                limit: *limit,
                offset: *offset,
            },
        }
    }

    const fn from_plan(page: Option<&PageSpec>) -> Self {
        match page {
            Some(page) => Self::Page {
                limit: page.limit,
                offset: page.offset,
            },
            None => Self::None,
        }
    }
}

impl ProjectedDeleteWindow {
    const fn from_explain(limit: &ExplainDeleteLimit) -> Self {
        match limit {
            ExplainDeleteLimit::None => Self::None,
            ExplainDeleteLimit::Limit { max_rows } => Self::Window {
                limit: Some(*max_rows),
                offset: 0,
            },
            ExplainDeleteLimit::Window { limit, offset } => Self::Window {
                limit: *limit,
                offset: *offset,
            },
        }
    }

    const fn from_plan(limit: Option<&DeleteLimitSpec>) -> Self {
        match limit {
            Some(limit) => Self::Window {
                limit: limit.limit,
                offset: limit.offset,
            },
            None => Self::None,
        }
    }
}

pub(super) fn hash_page(hasher: &mut Sha256, page: &ExplainPagination) {
    hash_projected_page_window(hasher, &ProjectedPageWindow::from_explain(page));
}

pub(super) fn hash_page_spec(hasher: &mut Sha256, page: Option<&PageSpec>) {
    hash_projected_page_window(hasher, &ProjectedPageWindow::from_plan(page));
}

fn hash_projected_page_window(hasher: &mut Sha256, page: &ProjectedPageWindow) {
    match page {
        ProjectedPageWindow::None => write_tag(hasher, PAGE_NONE_TAG),
        ProjectedPageWindow::Page { limit, offset } => {
            write_tag(hasher, PAGE_PRESENT_TAG);
            match limit {
                Some(limit) => {
                    write_tag(hasher, OPTIONAL_VALUE_PRESENT_TAG);
                    write_u32(hasher, *limit);
                }
                None => write_tag(hasher, OPTIONAL_VALUE_ABSENT_TAG),
            }
            write_u32(hasher, *offset);
        }
    }
}

pub(super) fn hash_distinct(hasher: &mut Sha256, distinct: bool) {
    if distinct {
        write_tag(hasher, DISTINCT_ENABLED_TAG);
    } else {
        write_tag(hasher, DISTINCT_DISABLED_TAG);
    }
}

pub(super) fn hash_delete_limit(hasher: &mut Sha256, limit: &ExplainDeleteLimit) {
    hash_projected_delete_window(hasher, &ProjectedDeleteWindow::from_explain(limit));
}

pub(super) fn hash_delete_limit_spec(hasher: &mut Sha256, limit: Option<&DeleteLimitSpec>) {
    hash_projected_delete_window(hasher, &ProjectedDeleteWindow::from_plan(limit));
}

fn hash_projected_delete_window(hasher: &mut Sha256, limit: &ProjectedDeleteWindow) {
    match limit {
        ProjectedDeleteWindow::None => write_tag(hasher, DELETE_LIMIT_NONE_TAG),
        ProjectedDeleteWindow::Window { limit, offset } => {
            write_tag(hasher, DELETE_LIMIT_PRESENT_TAG);
            write_u32(hasher, *offset);
            write_optional_u32(hasher, *limit);
        }
    }
}

pub(super) fn hash_consistency(hasher: &mut Sha256, consistency: MissingRowPolicy) {
    match consistency {
        MissingRowPolicy::Ignore => write_tag(hasher, CONSISTENCY_IGNORE_TAG),
        MissingRowPolicy::Error => write_tag(hasher, CONSISTENCY_ERROR_TAG),
    }
}

#[cfg_attr(not(test), allow(dead_code))]
pub(in crate::db::query::fingerprint) fn hash_explain_plan_profile_internal(
    hasher: &mut Sha256,
    plan: &crate::db::query::explain::ExplainPlan,
    profile: ExplainHashProfile<'_>,
    projection: Option<&crate::db::query::plan::expr::ProjectionSpec>,
) {
    profile::hash_explain_plan_profile_internal(hasher, plan, profile, projection);
}
