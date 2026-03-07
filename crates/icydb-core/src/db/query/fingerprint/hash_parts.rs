//! Module: query::fingerprint::hash_parts
//! Responsibility: canonical field/tag encoding for plan-hash profiles.
//! Does not own: plan explain projection or token transport.
//! Boundary: reusable hash primitives for fingerprints and continuation signatures.
#![expect(clippy::cast_possible_truncation)]

use crate::{
    db::{
        predicate::{MissingRowPolicy, Predicate, hash_predicate as hash_model_predicate},
        query::{
            explain::{
                ExplainAccessPath, ExplainDeleteLimit, ExplainGroupHaving,
                ExplainGroupHavingClause, ExplainGroupHavingSymbol, ExplainGroupedStrategy,
                ExplainGrouping, ExplainOrderBy, ExplainPagination, ExplainPlan,
            },
            fingerprint::aggregate_hash::{
                AggregateHashShape, hash_group_aggregate_structural_fingerprint_v1,
            },
            fingerprint::projection_hash::hash_projection_structural_fingerprint_v1,
            plan::{
                AccessPlanProjection, OrderDirection, QueryMode, expr::ProjectionSpec,
                project_explain_access_path,
            },
        },
    },
    value::{Value, hash_value},
};
use sha2::{Digest, Sha256};
use std::ops::Bound;

///
/// Hash explain access paths into the plan hash stream.
///

pub(super) fn hash_access(hasher: &mut Sha256, access: &ExplainAccessPath) {
    let mut projection = HashAccessProjection { hasher };
    project_explain_access_path(access, &mut projection);
}

struct HashAccessProjection<'a> {
    hasher: &'a mut Sha256,
}

impl AccessPlanProjection<Value> for HashAccessProjection<'_> {
    type Output = ();

    fn by_key(&mut self, key: &Value) -> Self::Output {
        write_tag(self.hasher, 0x10);
        write_value(self.hasher, key);
    }

    fn by_keys(&mut self, keys: &[Value]) -> Self::Output {
        write_tag(self.hasher, 0x11);
        write_u32(self.hasher, keys.len() as u32);
        for key in keys {
            write_value(self.hasher, key);
        }
    }

    fn key_range(&mut self, start: &Value, end: &Value) -> Self::Output {
        write_tag(self.hasher, 0x12);
        write_value(self.hasher, start);
        write_value(self.hasher, end);
    }

    fn index_prefix(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        values: &[Value],
    ) -> Self::Output {
        write_tag(self.hasher, 0x13);
        write_str(self.hasher, index_name);
        write_u32(self.hasher, index_fields.len() as u32);
        for field in index_fields {
            write_str(self.hasher, field);
        }
        write_u32(self.hasher, prefix_len as u32);
        write_u32(self.hasher, values.len() as u32);
        for value in values {
            write_value(self.hasher, value);
        }
    }

    fn index_multi_lookup(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        values: &[Value],
    ) -> Self::Output {
        write_tag(self.hasher, 0x18);
        write_str(self.hasher, index_name);
        write_u32(self.hasher, index_fields.len() as u32);
        for field in index_fields {
            write_str(self.hasher, field);
        }
        write_u32(self.hasher, values.len() as u32);
        for value in values {
            write_value(self.hasher, value);
        }
    }

    fn index_range(
        &mut self,
        index_name: &'static str,
        index_fields: &[&'static str],
        prefix_len: usize,
        prefix: &[Value],
        lower: &Bound<Value>,
        upper: &Bound<Value>,
    ) -> Self::Output {
        write_tag(self.hasher, 0x17);
        write_str(self.hasher, index_name);
        write_u32(self.hasher, index_fields.len() as u32);
        for field in index_fields {
            write_str(self.hasher, field);
        }
        write_u32(self.hasher, prefix_len as u32);
        write_u32(self.hasher, prefix.len() as u32);
        for value in prefix {
            write_value(self.hasher, value);
        }
        write_value_bound(self.hasher, lower);
        write_value_bound(self.hasher, upper);
    }

    fn full_scan(&mut self) -> Self::Output {
        write_tag(self.hasher, 0x14);
    }

    fn union(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, 0x15);
        write_u32(self.hasher, children.len() as u32);
    }

    fn intersection(&mut self, children: Vec<Self::Output>) -> Self::Output {
        write_tag(self.hasher, 0x16);
        write_u32(self.hasher, children.len() as u32);
    }
}

///
/// Hash canonical predicate model structure into the plan hash stream.
///
pub(super) fn hash_predicate(hasher: &mut Sha256, predicate: Option<&Predicate>) {
    let Some(predicate) = predicate else {
        write_tag(hasher, 0x20);
        return;
    };

    hash_model_predicate(hasher, predicate);
}

///
/// Hash explain order specs into the plan hash stream.
///

pub(super) fn hash_order(hasher: &mut Sha256, order: &ExplainOrderBy) {
    match order {
        ExplainOrderBy::None => write_tag(hasher, 0x30),
        ExplainOrderBy::Fields(fields) => {
            write_tag(hasher, 0x31);
            write_u32(hasher, fields.len() as u32);
            for field in fields {
                write_str(hasher, field.field());
                write_tag(hasher, order_direction_tag(field.direction()));
            }
        }
    }
}

///
/// Hash query mode into the plan hash stream.
///

pub(super) fn hash_mode(hasher: &mut Sha256, mode: QueryMode) {
    match mode {
        QueryMode::Load(_) => write_tag(hasher, 0x60),
        QueryMode::Delete(_) => write_tag(hasher, 0x61),
    }
}

///
/// Encode one value digest into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_value(hasher: &mut Sha256, value: &Value) {
    match hash_value(value) {
        Ok(digest) => hasher.update(digest),
        Err(err) => {
            write_tag(hasher, 0xEE);
            write_str(hasher, &err.display_with_class());
        }
    }
}

///
/// Encode one value bound into the plan hash stream.
///
pub(super) fn write_value_bound(hasher: &mut Sha256, bound: &Bound<Value>) {
    match bound {
        Bound::Unbounded => write_tag(hasher, 0x00),
        Bound::Included(value) => {
            write_tag(hasher, 0x01);
            write_value(hasher, value);
        }
        Bound::Excluded(value) => {
            write_tag(hasher, 0x02);
            write_value(hasher, value);
        }
    }
}

///
/// Encode one string with length prefix into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_str(hasher: &mut Sha256, value: &str) {
    write_u32(hasher, value.len() as u32);
    hasher.update(value.as_bytes());
}

///
/// Encode one u32 in network byte order into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_u32(hasher: &mut Sha256, value: u32) {
    hasher.update(value.to_be_bytes());
}

///
/// Encode one tag byte into the plan hash stream.
///

pub(in crate::db::query::fingerprint) fn write_tag(hasher: &mut Sha256, tag: u8) {
    hasher.update([tag]);
}

const fn order_direction_tag(direction: OrderDirection) -> u8 {
    match direction {
        OrderDirection::Asc => 0x01,
        OrderDirection::Desc => 0x02,
    }
}

///
/// ExplainHashProfile
///
/// Hashing profiles that select canonical explain-surface fields.
///

pub(in crate::db::query) enum ExplainHashProfile<'a> {
    FingerprintV2,
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

const FINGERPRINT_V2_STEPS: [ExplainHashStep; 9] = [
    ExplainHashStep {
        section_tag: 0x01,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: 0x02,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: 0x03,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: 0x04,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: 0x05,
        field: ExplainHashField::Page,
    },
    ExplainHashStep {
        section_tag: 0x06,
        field: ExplainHashField::DeleteLimit,
    },
    ExplainHashStep {
        section_tag: 0x07,
        field: ExplainHashField::Consistency,
    },
    ExplainHashStep {
        section_tag: 0x08,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: 0x09,
        field: ExplainHashField::ProjectionSpecV1,
    },
];

const CONTINUATION_V1_STEPS: [ExplainHashStep; 8] = [
    ExplainHashStep {
        section_tag: 0x01,
        field: ExplainHashField::EntityPath,
    },
    ExplainHashStep {
        section_tag: 0x02,
        field: ExplainHashField::Mode,
    },
    ExplainHashStep {
        section_tag: 0x03,
        field: ExplainHashField::Access,
    },
    ExplainHashStep {
        section_tag: 0x04,
        field: ExplainHashField::Predicate,
    },
    ExplainHashStep {
        section_tag: 0x05,
        field: ExplainHashField::Order,
    },
    ExplainHashStep {
        section_tag: 0x06,
        field: ExplainHashField::Distinct,
    },
    ExplainHashStep {
        section_tag: 0x07,
        field: ExplainHashField::GroupingShapeV1,
    },
    ExplainHashStep {
        section_tag: 0x08,
        field: ExplainHashField::ProjectionSpecV1,
    },
];

impl<'a> ExplainHashProfile<'a> {
    const fn spec(self) -> ExplainHashProfileSpec<'a> {
        match self {
            Self::FingerprintV2 => ExplainHashProfileSpec {
                entity_path: None,
                steps: &FINGERPRINT_V2_STEPS,
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

/// Hash an `ExplainPlan` using a profile-specific canonical field set.
pub(in crate::db::query) fn hash_explain_plan_profile(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: ExplainHashProfile<'_>,
) {
    hash_explain_plan_profile_internal(hasher, plan, profile, None);
}

/// Hash an `ExplainPlan` with one explicit semantic projection section.
pub(in crate::db::query) fn hash_explain_plan_profile_with_projection(
    hasher: &mut Sha256,
    plan: &ExplainPlan,
    profile: ExplainHashProfile<'_>,
    projection: &ProjectionSpec,
) {
    hash_explain_plan_profile_internal(hasher, plan, profile, Some(projection));
}

fn hash_explain_plan_profile_internal(
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
        ExplainPagination::None => write_tag(hasher, 0x40),
        ExplainPagination::Page { limit, offset } => {
            write_tag(hasher, 0x41);
            match limit {
                Some(limit) => {
                    write_tag(hasher, 0x01);
                    write_u32(hasher, *limit);
                }
                None => write_tag(hasher, 0x00),
            }
            write_u32(hasher, *offset);
        }
    }
}

fn hash_distinct(hasher: &mut Sha256, distinct: bool) {
    if distinct {
        write_tag(hasher, 0x44);
    } else {
        write_tag(hasher, 0x45);
    }
}

fn hash_delete_limit(hasher: &mut Sha256, limit: &ExplainDeleteLimit) {
    match limit {
        ExplainDeleteLimit::None => write_tag(hasher, 0x42),
        ExplainDeleteLimit::Limit { max_rows } => {
            write_tag(hasher, 0x43);
            write_u32(hasher, *max_rows);
        }
    }
}

fn hash_consistency(hasher: &mut Sha256, consistency: MissingRowPolicy) {
    match consistency {
        MissingRowPolicy::Ignore => write_tag(hasher, 0x50),
        MissingRowPolicy::Error => write_tag(hasher, 0x51),
    }
}

// Grouped shape semantics that remain part of continuation identity independent
// from projection expression hashing.
fn hash_grouping_shape_v1(
    hasher: &mut Sha256,
    grouping: &ExplainGrouping,
    include_group_strategy: bool,
) {
    match grouping {
        ExplainGrouping::None => write_tag(hasher, 0x70),
        ExplainGrouping::Grouped {
            strategy,
            group_fields,
            aggregates,
            having,
            max_groups,
            max_group_bytes,
        } => {
            // Grouped identity includes grouped key/aggregate ordering, grouped
            // HAVING semantics, and grouped budget policy.
            write_tag(hasher, 0x71);
            if include_group_strategy {
                hash_grouped_strategy(hasher, *strategy);
            }
            write_u32(hasher, group_fields.len() as u32);
            for field in group_fields {
                // Hash declared group field order using stable slot identity first,
                // then canonical field label as an additional guardrail.
                write_u32(hasher, field.slot_index() as u32);
                write_str(hasher, field.field());
            }

            write_u32(hasher, aggregates.len() as u32);
            for aggregate in aggregates {
                hash_group_aggregate_structural_fingerprint_v1(
                    hasher,
                    &AggregateHashShape::semantic(
                        aggregate.kind(),
                        aggregate.target_field(),
                        aggregate.distinct(),
                    ),
                );
            }
            hash_group_having(hasher, having.as_ref());

            write_u64(hasher, *max_groups);
            write_u64(hasher, *max_group_bytes);
        }
    }
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

fn hash_grouped_strategy(hasher: &mut Sha256, strategy: ExplainGroupedStrategy) {
    match strategy {
        ExplainGroupedStrategy::HashGroup => write_tag(hasher, 0x72),
        ExplainGroupedStrategy::OrderedGroup => write_tag(hasher, 0x73),
    }
}

fn hash_group_having(hasher: &mut Sha256, having: Option<&ExplainGroupHaving>) {
    let Some(having) = having else {
        write_tag(hasher, 0x74);
        return;
    };

    write_tag(hasher, 0x75);
    write_u32(hasher, having.clauses().len() as u32);
    for clause in having.clauses() {
        hash_group_having_clause(hasher, clause);
    }
}

fn hash_group_having_clause(hasher: &mut Sha256, clause: &ExplainGroupHavingClause) {
    match clause.symbol() {
        ExplainGroupHavingSymbol::GroupField { slot_index, field } => {
            write_tag(hasher, 0x76);
            write_u32(hasher, *slot_index as u32);
            write_str(hasher, field);
        }
        ExplainGroupHavingSymbol::AggregateIndex { index } => {
            write_tag(hasher, 0x77);
            write_u32(hasher, *index as u32);
        }
    }
    write_tag(hasher, clause.op().tag());
    write_value(hasher, clause.value());
}

fn write_u64(hasher: &mut Sha256, value: u64) {
    hasher.update(value.to_be_bytes());
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{
        CONTINUATION_V1_STEPS, ExplainHashField, ExplainHashProfile, FINGERPRINT_V2_STEPS,
    };

    #[test]
    fn fingerprint_v2_profile_excludes_grouping_shape_field() {
        let has_grouping_shape = FINGERPRINT_V2_STEPS
            .iter()
            .any(|step| step.field == ExplainHashField::GroupingShapeV1);

        assert!(
            !has_grouping_shape,
            "FingerprintV2 must remain semantic and exclude grouped strategy/handoff metadata fields",
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
    fn fingerprint_v2_profile_projection_slot_is_stable() {
        let projection_slots = FINGERPRINT_V2_STEPS
            .iter()
            .filter(|step| step.field == ExplainHashField::ProjectionSpecV1)
            .count();

        assert_eq!(
            projection_slots, 1,
            "FingerprintV2 must keep exactly one projection-semantic hash slot",
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
