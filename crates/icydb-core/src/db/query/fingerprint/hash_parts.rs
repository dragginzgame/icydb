//! Module: query::fingerprint::hash_parts
//! Responsibility: canonical field/tag encoding for plan-hash profiles.
//! Does not own: plan explain projection or token transport.
//! Boundary: reusable hash primitives for fingerprints and continuation signatures.
#![expect(clippy::cast_possible_truncation)]

use crate::{
    db::{
        access::AccessPlan,
        codec::{write_hash_str_u32, write_hash_tag_u8, write_hash_u32, write_hash_u64},
        predicate::{MissingRowPolicy, Predicate, hash_predicate as hash_model_predicate},
        query::{
            explain::{
                ExplainAccessPath, ExplainDeleteLimit, ExplainGroupHavingSymbol,
                ExplainGroupedStrategy, ExplainGrouping, ExplainOrderBy, ExplainPagination,
                ExplainPlan,
            },
            fingerprint::aggregate_hash::{
                AggregateHashShape, hash_group_aggregate_structural_fingerprint_v1,
            },
            fingerprint::projection_hash::hash_projection_structural_fingerprint_v1,
            plan::{
                AccessPlanProjection, AccessPlannedQuery, DeleteLimitSpec, GroupAggregateSpec,
                GroupHavingSymbol, OrderDirection, OrderSpec, PageSpec, QueryMode,
                expr::ProjectionSpec, grouped_plan_aggregate_family, grouped_plan_strategy,
                project_access_plan, project_explain_access_path,
            },
        },
    },
    traits::FieldValue,
    value::{Value, hash_value},
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

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
const GROUP_HAVING_GROUP_FIELD_TAG: u8 = 0x76;
const GROUP_HAVING_AGGREGATE_INDEX_TAG: u8 = 0x77;

const HASH_VALUE_ERROR_TAG: u8 = 0xEE;

const VALUE_BOUND_UNBOUNDED_TAG: u8 = 0x00;
const VALUE_BOUND_INCLUDED_TAG: u8 = 0x01;
const VALUE_BOUND_EXCLUDED_TAG: u8 = 0x02;

const OPTIONAL_VALUE_ABSENT_TAG: u8 = 0x00;
const OPTIONAL_VALUE_PRESENT_TAG: u8 = 0x01;

const ORDER_DIRECTION_ASC_TAG: u8 = 0x01;
const ORDER_DIRECTION_DESC_TAG: u8 = 0x02;

const FINGERPRINT_V1_SECTION_ACCESS_TAG: u8 = 0x01;
const FINGERPRINT_V1_SECTION_PREDICATE_TAG: u8 = 0x02;
const FINGERPRINT_V1_SECTION_ORDER_TAG: u8 = 0x03;
const FINGERPRINT_V1_SECTION_DISTINCT_TAG: u8 = 0x04;
const FINGERPRINT_V1_SECTION_PAGE_TAG: u8 = 0x05;
const FINGERPRINT_V1_SECTION_DELETE_LIMIT_TAG: u8 = 0x06;
const FINGERPRINT_V1_SECTION_CONSISTENCY_TAG: u8 = 0x07;
const FINGERPRINT_V1_SECTION_MODE_TAG: u8 = 0x08;
const FINGERPRINT_V1_SECTION_PROJECTION_SPEC_TAG: u8 = 0x09;

const CONTINUATION_V1_SECTION_ENTITY_PATH_TAG: u8 = 0x01;
const CONTINUATION_V1_SECTION_MODE_TAG: u8 = 0x02;
const CONTINUATION_V1_SECTION_ACCESS_TAG: u8 = 0x03;
const CONTINUATION_V1_SECTION_PREDICATE_TAG: u8 = 0x04;
const CONTINUATION_V1_SECTION_ORDER_TAG: u8 = 0x05;
const CONTINUATION_V1_SECTION_DISTINCT_TAG: u8 = 0x06;
const CONTINUATION_V1_SECTION_GROUPING_SHAPE_TAG: u8 = 0x07;
const CONTINUATION_V1_SECTION_PROJECTION_SPEC_TAG: u8 = 0x08;

///
/// Hash explain access paths into the plan hash stream.
///

pub(super) fn hash_access(hasher: &mut Sha256, access: &ExplainAccessPath) {
    let mut visitor = FingerprintVisitor { hasher };
    project_explain_access_path(access, &mut visitor);
}

///
/// FingerprintVisitor
///
/// Explain-access hash visitor that preserves canonical child-before-parent
/// token ordering used by structural fingerprinting.
///

struct FingerprintVisitor<'a> {
    hasher: &'a mut Sha256,
}

fn write_access_fields(hasher: &mut Sha256, tag: u8, name: &'static str, fields: &[&'static str]) {
    write_tag(hasher, tag);
    write_str(hasher, name);
    write_u32(hasher, fields.len() as u32);
    for field in fields {
        write_str(hasher, field);
    }
}

fn write_values(hasher: &mut Sha256, values: &[Value]) {
    write_u32(hasher, values.len() as u32);
    for value in values {
        write_value(hasher, value);
    }
}

fn write_field_values<K>(hasher: &mut Sha256, values: &[K])
where
    K: FieldValue,
{
    write_u32(hasher, values.len() as u32);
    for value in values {
        write_value(hasher, &value.to_value());
    }
}

impl AccessPlanProjection<Value> for FingerprintVisitor<'_> {
    type Output = ();

    fn by_key(&mut self, key: &Value) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEY);
        write_value(self.hasher, key);
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEYS);
        write_u32(self.hasher, keys.len() as u32);
        for key in keys {
            write_value(self.hasher, key);
        }
    }

    fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_KEY_RANGE);
        write_value(self.hasher, start);
        write_value(self.hasher, end);
    }

    fn index_prefix(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_PREFIX, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, values);
    }

    fn index_multi_lookup(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_MULTI_LOOKUP, name, fields);
        write_values(self.hasher, values);
    }

    fn index_range(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_RANGE, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, prefix);
        write_value_bound(self.hasher, lower);
        write_value_bound(self.hasher, upper);
    }

    fn full_scan(&mut self) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_FULL_SCAN);
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_UNION);
        write_u32(self.hasher, children.len() as u32);
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_INTERSECTION);
        write_u32(self.hasher, children.len() as u32);
    }
}

///
/// PlanFingerprintVisitor
///
/// Access-plan hash visitor over planner-owned canonical access contracts.
/// This keeps identity hashing independent from explain DTO projection.
///

struct PlanFingerprintVisitor<'a> {
    hasher: &'a mut Sha256,
}

impl<K> AccessPlanProjection<K> for PlanFingerprintVisitor<'_>
where
    K: FieldValue,
{
    type Output = ();

    fn by_key(&mut self, key: &K) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEY);
        write_value(self.hasher, &key.to_value());
    }

    fn by_keys(&mut self, keys: &[K]) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_BY_KEYS);
        write_field_values(self.hasher, keys);
    }

    fn key_range(&mut self, start: &K, end: &K) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_KEY_RANGE);
        write_value(self.hasher, &start.to_value());
        write_value(self.hasher, &end.to_value());
    }

    fn index_prefix(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_PREFIX, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, values);
    }

    fn index_multi_lookup(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_MULTI_LOOKUP, name, fields);
        write_values(self.hasher, values);
    }

    fn index_range(
        &mut self,
        name: &'static str,
        fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        write_access_fields(self.hasher, ACCESS_TAG_INDEX_RANGE, name, fields);
        write_u32(self.hasher, prefix_len as u32);
        write_values(self.hasher, prefix);
        write_value_bound(self.hasher, lower);
        write_value_bound(self.hasher, upper);
    }

    fn full_scan(&mut self) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_FULL_SCAN);
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_UNION);
        write_u32(self.hasher, children.len() as u32);
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, ACCESS_TAG_INTERSECTION);
        write_u32(self.hasher, children.len() as u32);
    }
}

///
/// Hash planner-owned access contracts into the plan hash stream.
///

pub(super) fn hash_access_plan<K>(hasher: &mut Sha256, access: &AccessPlan<K>)
where
    K: FieldValue,
{
    let mut visitor = PlanFingerprintVisitor { hasher };
    project_access_plan(access, &mut visitor);
}

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

pub(super) fn hash_order(hasher: &mut Sha256, order: &ExplainOrderBy) {
    match order {
        ExplainOrderBy::None => write_tag(hasher, ORDER_NONE_TAG),
        ExplainOrderBy::Fields(fields) => {
            write_tag(hasher, ORDER_FIELDS_TAG);
            write_u32(hasher, fields.len() as u32);
            for field in fields {
                write_str(hasher, field.field());
                write_tag(hasher, order_direction_tag(field.direction()));
            }
        }
    }
}

fn hash_order_spec(hasher: &mut Sha256, order: Option<&OrderSpec>) {
    let Some(order) = order else {
        write_tag(hasher, ORDER_NONE_TAG);
        return;
    };
    if order.fields.is_empty() {
        write_tag(hasher, ORDER_NONE_TAG);
        return;
    }

    write_tag(hasher, ORDER_FIELDS_TAG);
    write_u32(hasher, order.fields.len() as u32);
    for (field, direction) in &order.fields {
        write_str(hasher, field);
        write_tag(hasher, order_direction_tag(*direction));
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

pub(in crate::db::query::fingerprint) fn write_value(hasher: &mut Sha256, value: &Value) {
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

///
/// ExplainHashProfile
///
/// Hashing profiles that select canonical explain-surface fields.
///

pub(in crate::db::query) enum ExplainHashProfile<'a> {
    FingerprintV1,
    ContinuationV1 { entity_path: &'a str },
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
    GroupingShapeV1,
    ProjectionSpecV1,
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

const FINGERPRINT_V1_STEPS: [ExplainHashStep; 9] = [
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_ACCESS_TAG,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_PREDICATE_TAG,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_ORDER_TAG,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_DISTINCT_TAG,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_PAGE_TAG,
        field: ExplainHashField::Page,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_DELETE_LIMIT_TAG,
        field: ExplainHashField::DeleteLimit,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_CONSISTENCY_TAG,
        field: ExplainHashField::Consistency,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_MODE_TAG,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: FINGERPRINT_V1_SECTION_PROJECTION_SPEC_TAG,
        field: ExplainHashField::ProjectionSpecV1,
    },
];

const CONTINUATION_V1_STEPS: [ExplainHashStep; 8] = [
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_ENTITY_PATH_TAG,
        field: ExplainHashField::EntityPath,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_MODE_TAG,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_ACCESS_TAG,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_PREDICATE_TAG,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_ORDER_TAG,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_DISTINCT_TAG,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_GROUPING_SHAPE_TAG,
        field: ExplainHashField::GroupingShapeV1,
    },
    ExplainHashStep {
        section_tag: CONTINUATION_V1_SECTION_PROJECTION_SPEC_TAG,
        field: ExplainHashField::ProjectionSpecV1,
    },
];

impl<'a> ExplainHashProfile<'a> {
    const fn spec(self) -> ExplainHashProfileSpec<'a> {
        match self {
            Self::FingerprintV1 => ExplainHashProfileSpec {
                entity_path: None,
                steps: &FINGERPRINT_V1_STEPS,
            },
            Self::ContinuationV1 { entity_path } => ExplainHashProfileSpec {
                entity_path: Some(entity_path),
                steps: &CONTINUATION_V1_STEPS,
            },
        }
    }
}

fn hash_explain_field(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
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
        ExplainHashField::GroupingShapeV1 => {
            hash_grouping_shape_v1(hasher, plan.grouping(), include_group_strategy);
        }
        ExplainHashField::ProjectionSpecV1 => {
            hash_projection_spec_v1(hasher, projection, plan.grouping(), include_group_strategy);
        }
    }
}

fn hash_planned_query_field(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
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
        ExplainHashField::GroupingShapeV1 => {
            hash_grouping_shape_v1_from_plan(hasher, plan, include_group_strategy);
        }
        ExplainHashField::ProjectionSpecV1 => {
            hash_projection_spec_v1_for_plan(hasher, projection, plan, include_group_strategy);
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
    for step in spec.steps {
        write_tag(hasher, step.section_tag);
        hash_planned_query_field(
            hasher,
            plan,
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
    for step in spec.steps {
        write_tag(hasher, step.section_tag);
        hash_explain_field(
            hasher,
            plan,
            step.field,
            spec.entity_path,
            projection,
            include_group_strategy,
        );
    }
}

fn hash_page(hasher: &mut Sha256, page: &ExplainPagination) {
    match page {
        ExplainPagination::None => write_tag(hasher, PAGE_NONE_TAG),
        ExplainPagination::Page { limit, offset } => {
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

fn hash_page_spec(hasher: &mut Sha256, page: Option<&PageSpec>) {
    let Some(page) = page else {
        write_tag(hasher, PAGE_NONE_TAG);
        return;
    };

    write_tag(hasher, PAGE_PRESENT_TAG);
    match page.limit {
        Some(limit) => {
            write_tag(hasher, OPTIONAL_VALUE_PRESENT_TAG);
            write_u32(hasher, limit);
        }
        None => write_tag(hasher, OPTIONAL_VALUE_ABSENT_TAG),
    }
    write_u32(hasher, page.offset);
}

fn hash_distinct(hasher: &mut Sha256, distinct: bool) {
    if distinct {
        write_tag(hasher, DISTINCT_ENABLED_TAG);
    } else {
        write_tag(hasher, DISTINCT_DISABLED_TAG);
    }
}

fn hash_delete_limit(hasher: &mut Sha256, limit: &ExplainDeleteLimit) {
    match limit {
        ExplainDeleteLimit::None => write_tag(hasher, DELETE_LIMIT_NONE_TAG),
        ExplainDeleteLimit::Limit { max_rows } => {
            write_tag(hasher, DELETE_LIMIT_PRESENT_TAG);
            write_u32(hasher, *max_rows);
        }
    }
}

fn hash_delete_limit_spec(hasher: &mut Sha256, limit: Option<&DeleteLimitSpec>) {
    let Some(limit) = limit else {
        write_tag(hasher, DELETE_LIMIT_NONE_TAG);
        return;
    };

    write_tag(hasher, DELETE_LIMIT_PRESENT_TAG);
    write_u32(hasher, limit.max_rows);
}

fn hash_consistency(hasher: &mut Sha256, consistency: MissingRowPolicy) {
    match consistency {
        MissingRowPolicy::Ignore => write_tag(hasher, CONSISTENCY_IGNORE_TAG),
        MissingRowPolicy::Error => write_tag(hasher, CONSISTENCY_ERROR_TAG),
    }
}

///
/// GroupedFingerprintShape
///
/// Canonical grouped fingerprint projection shared by logical-plan and explain
/// hashing callsites. Both surfaces project into this neutral grouped shape so
/// hashing does not keep parallel semantic projection seams.
///

struct GroupedFingerprintShape<'a> {
    ordered_group: bool,
    aggregate_family_code: Option<&'a str>,
    group_fields: Vec<(u32, &'a str)>,
    aggregates: Vec<AggregateHashShape<'a>>,
    having: Option<Vec<GroupHavingFingerprintClause<'a>>>,
    max_groups: u64,
    max_group_bytes: u64,
}

/// Canonical grouped fingerprint projection state shared by plan and explain hashing.
enum ProjectedGroupingShape<'a> {
    None,
    Grouped(GroupedFingerprintShape<'a>),
}

/// Canonical grouped HAVING clause projection shared by plan and explain hashing.
enum GroupHavingFingerprintClause<'a> {
    GroupField {
        slot_index: u32,
        field: &'a str,
        op_tag: u8,
        value: &'a Value,
    },
    AggregateIndex {
        index: u32,
        op_tag: u8,
        value: &'a Value,
    },
}

impl<'a> ProjectedGroupingShape<'a> {
    fn from_explain(grouping: &'a ExplainGrouping) -> Self {
        match grouping {
            ExplainGrouping::None => Self::None,
            ExplainGrouping::Grouped {
                strategy,
                fallback_reason: _,
                group_fields,
                aggregates,
                having,
                max_groups,
                max_group_bytes,
            } => {
                let aggregate_family = grouped_plan_aggregate_family(
                    &aggregates
                        .iter()
                        .map(|aggregate| GroupAggregateSpec {
                            kind: aggregate.kind(),
                            target_field: aggregate.target_field().map(str::to_string),
                            distinct: aggregate.distinct(),
                        })
                        .collect::<Vec<_>>(),
                );

                Self::Grouped(GroupedFingerprintShape {
                    ordered_group: matches!(strategy, ExplainGroupedStrategy::OrderedGroup),
                    aggregate_family_code: Some(aggregate_family.code()),
                    group_fields: group_fields
                        .iter()
                        .map(|field| (field.slot_index() as u32, field.field()))
                        .collect(),
                    aggregates: aggregates
                        .iter()
                        .map(|aggregate| {
                            AggregateHashShape::semantic(
                                aggregate.kind(),
                                aggregate.target_field(),
                                aggregate.distinct(),
                            )
                        })
                        .collect(),
                    having: having.as_ref().map(|having| {
                        having
                            .clauses()
                            .iter()
                            .map(|clause| match clause.symbol() {
                                ExplainGroupHavingSymbol::GroupField { slot_index, field } => {
                                    GroupHavingFingerprintClause::GroupField {
                                        slot_index: *slot_index as u32,
                                        field,
                                        op_tag: clause.op().tag(),
                                        value: clause.value(),
                                    }
                                }
                                ExplainGroupHavingSymbol::AggregateIndex { index } => {
                                    GroupHavingFingerprintClause::AggregateIndex {
                                        index: *index as u32,
                                        op_tag: clause.op().tag(),
                                        value: clause.value(),
                                    }
                                }
                            })
                            .collect()
                    }),
                    max_groups: *max_groups,
                    max_group_bytes: *max_group_bytes,
                })
            }
        }
    }

    fn from_plan(plan: &'a AccessPlannedQuery) -> Self {
        let Some(grouped) = plan.grouped_plan() else {
            return Self::None;
        };
        let strategy = grouped_plan_strategy(plan)
            .expect("grouped grouping-shape hashing requires planner-owned grouped strategy");

        Self::Grouped(GroupedFingerprintShape {
            ordered_group: strategy.is_ordered_group(),
            aggregate_family_code: Some(strategy.aggregate_family().code()),
            group_fields: grouped
                .group
                .group_fields
                .iter()
                .map(|field| (field.index as u32, field.field.as_str()))
                .collect(),
            aggregates: grouped
                .group
                .aggregates
                .iter()
                .map(|aggregate| {
                    AggregateHashShape::semantic(
                        aggregate.kind,
                        aggregate.target_field.as_deref(),
                        aggregate.distinct,
                    )
                })
                .collect(),
            having: grouped.having.as_ref().map(|having| {
                having
                    .clauses
                    .iter()
                    .map(|clause| match &clause.symbol {
                        GroupHavingSymbol::GroupField(field_slot) => {
                            GroupHavingFingerprintClause::GroupField {
                                slot_index: field_slot.index as u32,
                                field: &field_slot.field,
                                op_tag: clause.op.tag(),
                                value: &clause.value,
                            }
                        }
                        GroupHavingSymbol::AggregateIndex(index) => {
                            GroupHavingFingerprintClause::AggregateIndex {
                                index: *index as u32,
                                op_tag: clause.op.tag(),
                                value: &clause.value,
                            }
                        }
                    })
                    .collect()
            }),
            max_groups: grouped.group.execution.max_groups,
            max_group_bytes: grouped.group.execution.max_group_bytes,
        })
    }
}

// Grouped shape semantics that remain part of continuation identity independent
// from projection expression hashing.
fn hash_grouping_shape_v1(
    hasher: &mut Sha256,
    grouping: &ExplainGrouping,
    include_group_strategy: bool,
) {
    let grouping = ProjectedGroupingShape::from_explain(grouping);

    hash_projected_grouping_shape_v1(hasher, &grouping, include_group_strategy);
}

fn hash_grouping_shape_v1_from_plan(
    hasher: &mut Sha256,
    plan: &AccessPlannedQuery,
    include_group_strategy: bool,
) {
    let grouping = ProjectedGroupingShape::from_plan(plan);

    hash_projected_grouping_shape_v1(hasher, &grouping, include_group_strategy);
}

fn hash_projection_spec_v1(
    hasher: &mut Sha256,
    projection: Option<&ProjectionSpec>,
    grouping: &ExplainGrouping,
    include_group_strategy: bool,
) {
    // Explain-only hashing callsites may not have planner projection semantics.
    // In that case, preserve grouped-shape identity semantics.
    if let Some(projection) = projection {
        hash_projection_structural_fingerprint_v1(hasher, projection);
        return;
    }

    hash_grouping_shape_v1(hasher, grouping, include_group_strategy);
}

fn hash_projection_spec_v1_for_plan(
    hasher: &mut Sha256,
    projection: Option<&ProjectionSpec>,
    plan: &AccessPlannedQuery,
    include_group_strategy: bool,
) {
    if let Some(projection) = projection {
        hash_projection_structural_fingerprint_v1(hasher, projection);
        return;
    }

    hash_grouping_shape_v1_from_plan(hasher, plan, include_group_strategy);
}

// Hash the canonical grouped identity payload after plan/explain have already
// projected onto the shared grouped fingerprint shape.
fn hash_projected_grouping_shape_v1(
    hasher: &mut Sha256,
    grouping: &ProjectedGroupingShape<'_>,
    include_group_strategy: bool,
) {
    match grouping {
        ProjectedGroupingShape::None => write_tag(hasher, GROUPING_NONE_TAG),
        ProjectedGroupingShape::Grouped(grouped) => {
            write_tag(hasher, GROUPING_PRESENT_TAG);
            if include_group_strategy {
                hash_grouped_strategy_projection(
                    hasher,
                    grouped.ordered_group,
                    grouped.aggregate_family_code,
                );
            }

            hash_group_field_slots(
                hasher,
                grouped.group_fields.len(),
                grouped
                    .group_fields
                    .iter()
                    .map(|(slot_index, field)| (*slot_index, *field)),
            );
            hash_group_aggregate_shapes(
                hasher,
                grouped.aggregates.len(),
                grouped.aggregates.iter().copied(),
            );
            hash_group_having_projection(hasher, grouped.having.as_deref());

            write_hash_u64(hasher, grouped.max_groups);
            write_hash_u64(hasher, grouped.max_group_bytes);
        }
    }
}

// Hash grouped key order using stable slot identity first, then the canonical
// field label as a guardrail against grouped projection drift.
fn hash_group_field_slots<'a, I>(hasher: &mut Sha256, field_count: usize, fields: I)
where
    I: IntoIterator<Item = (u32, &'a str)>,
{
    write_u32(hasher, field_count as u32);
    for (slot_index, field) in fields {
        write_u32(hasher, slot_index);
        write_str(hasher, field);
    }
}

// Hash grouped aggregate semantics from one already-lowered aggregate shape stream.
fn hash_group_aggregate_shapes<'a, I>(hasher: &mut Sha256, aggregate_count: usize, aggregates: I)
where
    I: IntoIterator<Item = AggregateHashShape<'a>>,
{
    write_u32(hasher, aggregate_count as u32);
    for aggregate in aggregates {
        hash_group_aggregate_structural_fingerprint_v1(hasher, &aggregate);
    }
}

fn hash_grouped_strategy_projection(
    hasher: &mut Sha256,
    ordered_group: bool,
    aggregate_family_code: Option<&str>,
) {
    if ordered_group {
        write_tag(hasher, GROUPING_STRATEGY_ORDERED_TAG);
    } else {
        write_tag(hasher, GROUPING_STRATEGY_HASH_TAG);
    }

    if let Some(aggregate_family_code) = aggregate_family_code {
        write_str(hasher, aggregate_family_code);
    }
}

// Hash one grouped HAVING clause after the caller has already projected it onto
// the canonical grouped symbol/op/value shape.
fn hash_group_having_projection_clause(
    hasher: &mut Sha256,
    clause: &GroupHavingFingerprintClause<'_>,
) {
    match clause {
        GroupHavingFingerprintClause::GroupField {
            slot_index,
            field,
            op_tag,
            value,
        } => {
            write_tag(hasher, GROUP_HAVING_GROUP_FIELD_TAG);
            write_u32(hasher, *slot_index);
            write_str(hasher, field);
            write_tag(hasher, *op_tag);
            write_value(hasher, value);
        }
        GroupHavingFingerprintClause::AggregateIndex {
            index,
            op_tag,
            value,
        } => {
            write_tag(hasher, GROUP_HAVING_AGGREGATE_INDEX_TAG);
            write_u32(hasher, *index);
            write_tag(hasher, *op_tag);
            write_value(hasher, value);
        }
    }
}

fn hash_group_having_projection(
    hasher: &mut Sha256,
    clauses: Option<&[GroupHavingFingerprintClause<'_>]>,
) {
    let Some(clauses) = clauses else {
        write_tag(hasher, GROUP_HAVING_ABSENT_TAG);
        return;
    };

    write_tag(hasher, GROUP_HAVING_PRESENT_TAG);
    write_u32(hasher, clauses.len() as u32);
    for clause in clauses {
        hash_group_having_projection_clause(hasher, clause);
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        CONTINUATION_V1_STEPS, ExplainHashField, ExplainHashProfile, FINGERPRINT_V1_STEPS,
    };

    #[test]
    fn fingerprint_v1_profile_excludes_grouping_shape_field() {
        let has_grouping_shape = FINGERPRINT_V1_STEPS
            .iter()
            .any(|step| step.field == ExplainHashField::GroupingShapeV1);

        assert!(
            !has_grouping_shape,
            "FingerprintV1 must remain semantic and exclude grouped strategy/handoff metadata fields",
        );
    }

    #[test]
    fn continuation_v1_profile_includes_grouping_shape_field() {
        let has_grouping_shape = CONTINUATION_V1_STEPS
            .iter()
            .any(|step| step.field == ExplainHashField::GroupingShapeV1);

        assert!(
            has_grouping_shape,
            "ContinuationV1 must remain grouped-shape aware for resume compatibility",
        );
    }

    #[test]
    fn fingerprint_v1_profile_projection_slot_is_stable() {
        let projection_slots = FINGERPRINT_V1_STEPS
            .iter()
            .filter(|step| step.field == ExplainHashField::ProjectionSpecV1)
            .count();

        assert_eq!(
            projection_slots, 1,
            "FingerprintV1 must keep exactly one projection-semantic hash slot",
        );
    }

    #[test]
    fn continuation_v1_profile_declares_entity_path_contract_slot() {
        let spec = ExplainHashProfile::ContinuationV1 {
            entity_path: "tests::Entity",
        }
        .spec();

        assert!(
            spec.entity_path.is_some(),
            "ContinuationV1 must remain entity-path aware for cursor signature isolation",
        );
    }
}
