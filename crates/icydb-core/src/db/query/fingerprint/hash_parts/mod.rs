//! Module: query::fingerprint::hash_parts
//! Responsibility: canonical field/tag encoding for plan-hash profiles.
//! Does not own: plan explain projection or token transport.
//! Boundary: reusable hash primitives for fingerprints and continuation signatures.
#![expect(clippy::cast_possible_truncation)]

mod access;
mod grouping;
#[cfg(test)]
mod tests;

use crate::{
    db::{
        codec::{write_hash_str_u32, write_hash_tag_u8, write_hash_u32},
        predicate::{MissingRowPolicy, Predicate, hash_predicate as hash_model_predicate},
        query::{
            explain::{ExplainDeleteLimit, ExplainOrderBy, ExplainPagination, ExplainPlan},
            plan::{
                AccessPlannedQuery, DeleteLimitSpec, OrderDirection, OrderSpec, PageSpec,
                QueryMode, expr::ProjectionSpec,
            },
        },
    },
    value::{Value, hash_value},
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

use crate::db::query::fingerprint::hash_parts::{
    access::{hash_access, hash_access_plan},
    grouping::{GroupingFingerprintSource, hash_grouping_shape_v1, hash_projection_spec_v1},
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

const HASH_VALUE_ERROR_TAG: u8 = 0xEE;

const VALUE_BOUND_UNBOUNDED_TAG: u8 = 0x00;
const VALUE_BOUND_INCLUDED_TAG: u8 = 0x01;
const VALUE_BOUND_EXCLUDED_TAG: u8 = 0x02;

const OPTIONAL_VALUE_ABSENT_TAG: u8 = 0x00;
const OPTIONAL_VALUE_PRESENT_TAG: u8 = 0x01;

const ORDER_DIRECTION_ASC_TAG: u8 = 0x01;
const ORDER_DIRECTION_DESC_TAG: u8 = 0x02;

const FINGERPRINT_SECTION_ACCESS_TAG: u8 = 0x01;
const FINGERPRINT_SECTION_PREDICATE_TAG: u8 = 0x02;
const FINGERPRINT_SECTION_ORDER_TAG: u8 = 0x03;
const FINGERPRINT_SECTION_DISTINCT_TAG: u8 = 0x04;
const FINGERPRINT_SECTION_PAGE_TAG: u8 = 0x05;
const FINGERPRINT_SECTION_DELETE_LIMIT_TAG: u8 = 0x06;
const FINGERPRINT_SECTION_CONSISTENCY_TAG: u8 = 0x07;
const FINGERPRINT_SECTION_MODE_TAG: u8 = 0x08;
const FINGERPRINT_SECTION_PROJECTION_SPEC_TAG: u8 = 0x09;

const CONTINUATION_SECTION_ENTITY_PATH_TAG: u8 = 0x01;
const CONTINUATION_SECTION_MODE_TAG: u8 = 0x02;
const CONTINUATION_SECTION_ACCESS_TAG: u8 = 0x03;
const CONTINUATION_SECTION_PREDICATE_TAG: u8 = 0x04;
const CONTINUATION_SECTION_ORDER_TAG: u8 = 0x05;
const CONTINUATION_SECTION_DISTINCT_TAG: u8 = 0x06;
const CONTINUATION_SECTION_GROUPING_SHAPE_TAG: u8 = 0x07;
const CONTINUATION_SECTION_PROJECTION_SPEC_TAG: u8 = 0x08;

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
/// Hash explain order specs into the plan hash stream.
///

///
/// ProjectedOrderShape
///
/// Canonical order projection shared by logical-plan and explain hashing.
/// Both surfaces normalize into the same ordered field list before the
/// canonical order hash is written.
///

enum ProjectedOrderShape<'a> {
    None,
    Fields(Vec<(&'a str, OrderDirection)>),
}

impl<'a> ProjectedOrderShape<'a> {
    fn from_explain(order: &'a ExplainOrderBy) -> Self {
        match order {
            ExplainOrderBy::None => Self::None,
            ExplainOrderBy::Fields(fields) => Self::Fields(
                fields
                    .iter()
                    .map(|field| (field.field(), field.direction()))
                    .collect(),
            ),
        }
    }

    fn from_plan(order: Option<&'a OrderSpec>) -> Self {
        match order {
            Some(order) if !order.fields.is_empty() => Self::Fields(
                order
                    .fields
                    .iter()
                    .map(|(field, direction)| (field.as_str(), *direction))
                    .collect(),
            ),
            Some(_) | None => Self::None,
        }
    }
}

pub(super) fn hash_order(hasher: &mut Sha256, order: &ExplainOrderBy) {
    hash_projected_order_shape(hasher, &ProjectedOrderShape::from_explain(order));
}

fn hash_order_spec(hasher: &mut Sha256, order: Option<&OrderSpec>) {
    hash_projected_order_shape(hasher, &ProjectedOrderShape::from_plan(order));
}

fn hash_projected_order_shape(hasher: &mut Sha256, order: &ProjectedOrderShape<'_>) {
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
/// ExplainHashProfile
///
/// Hashing profiles that select canonical explain-surface fields.
///

pub(in crate::db::query) enum ExplainHashProfile<'a> {
    Fingerprint,
    Continuation { entity_path: &'a str },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ExplainHashField {
    EntityPath,
    Mode,
    Access,
    Predicate,
    Order,
    Distinct,
    Page,
    DeleteLimit,
    Consistency,
    GroupingShape,
    ProjectionSpec,
}

///
/// ExplainHashSource
///
/// Canonical hash-profile source shared by explain and planner-owned query
/// hashing. This keeps the per-field profile walk on one owner-local seam
/// instead of maintaining parallel match trees for the two input surfaces.
///

#[allow(dead_code)]
#[derive(Clone, Copy)]
enum ExplainHashSource<'a> {
    Explain(&'a ExplainPlan),
    Planned(&'a AccessPlannedQuery),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ExplainHashStep {
    section_tag: u8,
    field: ExplainHashField,
}

struct ExplainHashProfileSpec<'a> {
    entity_path: Option<&'a str>,
    steps: &'static [ExplainHashStep],
}

const FINGERPRINT_STEPS: [ExplainHashStep; 9] = [
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_ACCESS_TAG,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_PREDICATE_TAG,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_ORDER_TAG,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_DISTINCT_TAG,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_PAGE_TAG,
        field: ExplainHashField::Page,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_DELETE_LIMIT_TAG,
        field: ExplainHashField::DeleteLimit,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_CONSISTENCY_TAG,
        field: ExplainHashField::Consistency,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_MODE_TAG,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_SECTION_PROJECTION_SPEC_TAG,
        field: ExplainHashField::ProjectionSpec,
    },
];

const CONTINUATION_STEPS: [ExplainHashStep; 8] = [
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_ENTITY_PATH_TAG,
        field: ExplainHashField::EntityPath,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_MODE_TAG,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_ACCESS_TAG,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_PREDICATE_TAG,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_ORDER_TAG,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_DISTINCT_TAG,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_GROUPING_SHAPE_TAG,
        field: ExplainHashField::GroupingShape,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_SECTION_PROJECTION_SPEC_TAG,
        field: ExplainHashField::ProjectionSpec,
    },
];

impl<'a> ExplainHashProfile<'a> {
    const fn spec(self) -> ExplainHashProfileSpec<'a> {
        match self {
            Self::Fingerprint => ExplainHashProfileSpec {
                entity_path: None,
                steps: &FINGERPRINT_STEPS,
            },
            Self::Continuation { entity_path } => ExplainHashProfileSpec {
                entity_path: Some(entity_path),
                steps: &CONTINUATION_STEPS,
            },
        }
    }
}

impl<'a> ExplainHashSource<'a> {
    const fn grouping_source(self) -> GroupingFingerprintSource<'a> {
        match self {
            Self::Explain(plan) => GroupingFingerprintSource::Explain(plan.grouping()),
            Self::Planned(plan) => GroupingFingerprintSource::Plan(plan),
        }
    }

    fn hash_field(
        self,
        hasher: &mut Sha256,
        field: ExplainHashField,
        entity_path: Option<&str>,
        projection: Option<&ProjectionSpec>,
        include_group_strategy: bool,
    ) {
        match self {
            Self::Explain(plan) => self.hash_explain_field(
                hasher,
                plan,
                field,
                entity_path,
                projection,
                include_group_strategy,
            ),
            Self::Planned(plan) => self.hash_planned_field(
                hasher,
                plan,
                field,
                entity_path,
                projection,
                include_group_strategy,
            ),
        }
    }

    fn hash_explain_field(
        self,
        hasher: &mut Sha256,
        plan: &'a ExplainPlan,
        field: ExplainHashField,
        entity_path: Option<&str>,
        projection: Option<&ProjectionSpec>,
        include_group_strategy: bool,
    ) {
        match field {
            ExplainHashField::EntityPath => {
                let entity_path = entity_path.expect("entity path required by hash profile");
                write_str(hasher, entity_path);
            }
            ExplainHashField::Mode => hash_mode(hasher, plan.mode()),
            ExplainHashField::Access => hash_access(hasher, plan.access()),
            ExplainHashField::Predicate => hash_predicate(hasher, plan.predicate_model_for_hash()),
            ExplainHashField::Order => hash_order(hasher, plan.order_by()),
            ExplainHashField::Distinct => hash_distinct(hasher, plan.distinct()),
            ExplainHashField::Page => hash_page(hasher, plan.page()),
            ExplainHashField::DeleteLimit => hash_delete_limit(hasher, plan.delete_limit()),
            ExplainHashField::Consistency => hash_consistency(hasher, plan.consistency()),
            ExplainHashField::GroupingShape => {
                hash_grouping_shape_v1(hasher, self.grouping_source(), include_group_strategy);
            }
            ExplainHashField::ProjectionSpec => {
                hash_projection_spec_v1(
                    hasher,
                    projection,
                    self.grouping_source(),
                    include_group_strategy,
                );
            }
        }
    }

    fn hash_planned_field(
        self,
        hasher: &mut Sha256,
        plan: &'a AccessPlannedQuery,
        field: ExplainHashField,
        entity_path: Option<&str>,
        projection: Option<&ProjectionSpec>,
        include_group_strategy: bool,
    ) {
        let scalar = plan.scalar_plan();

        match field {
            ExplainHashField::EntityPath => {
                let entity_path = entity_path.expect("entity path required by hash profile");
                write_str(hasher, entity_path);
            }
            ExplainHashField::Mode => hash_mode(hasher, scalar.mode),
            ExplainHashField::Access => hash_access_plan(hasher, &plan.access),
            ExplainHashField::Predicate => hash_predicate(hasher, scalar.predicate.as_ref()),
            ExplainHashField::Order => hash_order_spec(hasher, scalar.order.as_ref()),
            ExplainHashField::Distinct => hash_distinct(hasher, scalar.distinct),
            ExplainHashField::Page => hash_page_spec(hasher, scalar.page.as_ref()),
            ExplainHashField::DeleteLimit => {
                hash_delete_limit_spec(hasher, scalar.delete_limit.as_ref());
            }
            ExplainHashField::Consistency => hash_consistency(hasher, scalar.consistency),
            ExplainHashField::GroupingShape => {
                hash_grouping_shape_v1(hasher, self.grouping_source(), include_group_strategy);
            }
            ExplainHashField::ProjectionSpec => {
                hash_projection_spec_v1(
                    hasher,
                    projection,
                    self.grouping_source(),
                    include_group_strategy,
                );
            }
        }
    }
}

/// Hash a planner-owned query with an explicit semantic projection section.
pub(in crate::db::query) fn hash_planned_query_profile_with_projection(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
    profile: ExplainHashProfile<'_>,
    projection: &ProjectionSpec,
) {
    hash_planned_query_profile_internal(hasher, plan, profile, Some(projection));
}

fn hash_planned_query_profile_internal(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
    profile: ExplainHashProfile<'_>,
    projection: Option<&ProjectionSpec>,
) {
    let spec = profile.spec();
    let include_group_strategy = spec.entity_path.is_some();
    let source = ExplainHashSource::Planned(plan);

    for step in spec.steps {
        write_tag(hasher, step.section_tag);
        source.hash_field(
            hasher,
            step.field,
            spec.entity_path,
            projection,
            include_group_strategy,
        );
    }
}

/// Hash an `ExplainPlan` using a profile-specific canonical field set.
pub(in crate::db::query) fn hash_explain_plan_profile(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: ExplainHashProfile<'_>,
) {
    hash_explain_plan_profile_internal(hasher, plan, profile, None);
}

pub(in crate::db::query::fingerprint) fn hash_explain_plan_profile_internal(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: ExplainHashProfile<'_>,
    projection: Option<&ProjectionSpec>,
) {
    // Apply selected hash profile in declared order to preserve determinism.
    let spec = profile.spec();
    let include_group_strategy = spec.entity_path.is_some();
    let source = ExplainHashSource::Explain(plan);

    for step in spec.steps {
        write_tag(hasher, step.section_tag);
        source.hash_field(
            hasher,
            step.field,
            spec.entity_path,
            projection,
            include_group_strategy,
        );
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

fn hash_page(hasher: &mut Sha256, page: &ExplainPagination) {
    hash_projected_page_window(hasher, &ProjectedPageWindow::from_explain(page));
}

fn hash_page_spec(hasher: &mut Sha256, page: Option<&PageSpec>) {
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

fn hash_distinct(hasher: &mut Sha256, distinct: bool) {
    if distinct {
        write_tag(hasher, DISTINCT_ENABLED_TAG);
    } else {
        write_tag(hasher, DISTINCT_DISABLED_TAG);
    }
}

fn hash_delete_limit(hasher: &mut Sha256, limit: &ExplainDeleteLimit) {
    hash_projected_delete_window(hasher, &ProjectedDeleteWindow::from_explain(limit));
}

fn hash_delete_limit_spec(hasher: &mut Sha256, limit: Option<&DeleteLimitSpec>) {
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

fn hash_consistency(hasher: &mut Sha256, consistency: MissingRowPolicy) {
    match consistency {
        MissingRowPolicy::Ignore => write_tag(hasher, CONSISTENCY_IGNORE_TAG),
        MissingRowPolicy::Error => write_tag(hasher, CONSISTENCY_ERROR_TAG),
    }
}
