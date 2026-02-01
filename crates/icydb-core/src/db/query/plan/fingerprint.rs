//! Deterministic plan fingerprinting derived from the explain projection.
#![allow(clippy::cast_possible_truncation)]

use super::{
    ExplainAccessPath, ExplainDeleteLimit, ExplainOrderBy, ExplainPagination, ExplainPlan,
    ExplainPredicate,
};
use crate::db::index::fingerprint::hash_value;
use crate::db::query::QueryMode;
use crate::db::query::{ReadConsistency, predicate::coercion::CoercionId};
use crate::traits::FieldValue;
use sha2::{Digest, Sha256};

///
/// PlanFingerprint
///
/// Stable, deterministic fingerprint for logical plans.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlanFingerprint([u8; 32]);

impl PlanFingerprint {
    pub(crate) const fn from_bytes(bytes: [u8; 32]) -> Self {
        Self(bytes)
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

impl std::fmt::Display for PlanFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

impl<K> super::LogicalPlan<K>
where
    K: Copy,
{
    /// Compute a stable fingerprint for this logical plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        self.explain().fingerprint()
    }
}

impl ExplainPlan {
    /// Compute a stable fingerprint for this explain plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        let mut hasher = Sha256::new();
        hasher.update(b"planfp:v2");
        hash_explain_plan(&mut hasher, self);
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        PlanFingerprint(out)
    }
}

fn hash_explain_plan(hasher: &mut Sha256, plan: &ExplainPlan) {
    write_tag(hasher, 0x01);
    hash_access(hasher, &plan.access);

    write_tag(hasher, 0x02);
    hash_predicate(hasher, &plan.predicate);

    write_tag(hasher, 0x03);
    hash_order(hasher, &plan.order_by);

    write_tag(hasher, 0x04);
    hash_page(hasher, &plan.page);

    write_tag(hasher, 0x05);
    hash_delete_limit(hasher, &plan.delete_limit);

    write_tag(hasher, 0x06);
    hash_consistency(hasher, plan.consistency);

    write_tag(hasher, 0x07);
    hash_mode(hasher, plan.mode);
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
        ExplainPredicate::MapContainsKey {
            field,
            key,
            coercion,
        } => {
            write_tag(hasher, 0x2b);
            write_str(hasher, field);
            write_value(hasher, key);
            hash_coercion(hasher, coercion.id, &coercion.params);
        }
        ExplainPredicate::MapContainsValue {
            field,
            value,
            coercion,
        } => {
            write_tag(hasher, 0x2c);
            write_str(hasher, field);
            write_value(hasher, value);
            hash_coercion(hasher, coercion.id, &coercion.params);
        }
        ExplainPredicate::MapContainsEntry {
            field,
            key,
            value,
            coercion,
        } => {
            write_tag(hasher, 0x2d);
            write_str(hasher, field);
            write_value(hasher, key);
            write_value(hasher, value);
            hash_coercion(hasher, coercion.id, &coercion.params);
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

fn hash_delete_limit(hasher: &mut Sha256, limit: &ExplainDeleteLimit) {
    match limit {
        ExplainDeleteLimit::None => write_tag(hasher, 0x42),
        ExplainDeleteLimit::Limit { max_rows } => {
            write_tag(hasher, 0x43);
            write_u32(hasher, *max_rows);
        }
    }
}

fn hash_consistency(hasher: &mut Sha256, consistency: ReadConsistency) {
    match consistency {
        ReadConsistency::MissingOk => write_tag(hasher, 0x50),
        ReadConsistency::Strict => write_tag(hasher, 0x51),
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

#[cfg(test)]
mod tests {
    use crate::db::query::plan::{AccessPath, DeleteLimitSpec, LogicalPlan};
    use crate::db::query::{FieldRef, plan::planner::PlannerEntity};
    use crate::db::query::{Query, QueryMode, ReadConsistency};
    use crate::model::index::IndexModel;
    use crate::types::{Ref, Ulid};
    use crate::value::Value;

    #[test]
    fn fingerprint_is_deterministic_for_equivalent_predicates() {
        let id = Ulid::default();

        let query_a = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
            .filter(FieldRef::new("id").eq(id))
            .filter(FieldRef::new("other").eq("x"));

        let query_b = Query::<PlannerEntity>::new(ReadConsistency::MissingOk)
            .filter(FieldRef::new("other").eq("x"))
            .filter(FieldRef::new("id").eq(id));

        let plan_a = query_a.plan().expect("plan a");
        let plan_b = query_b.plan().expect("plan b");

        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_index_choice() {
        const INDEX_FIELDS: [&str; 1] = ["idx_a"];
        const INDEX_A: IndexModel = IndexModel::new(
            "fingerprint::idx_a",
            "fingerprint::store",
            &INDEX_FIELDS,
            false,
        );
        const INDEX_B: IndexModel = IndexModel::new(
            "fingerprint::idx_b",
            "fingerprint::store",
            &INDEX_FIELDS,
            false,
        );

        let plan_a = LogicalPlan::<Ref<PlannerEntity>>::new(
            AccessPath::IndexPrefix {
                index: INDEX_A,
                values: vec![Value::Text("alpha".to_string())],
            },
            crate::db::query::ReadConsistency::MissingOk,
        );
        let plan_b = LogicalPlan::<Ref<PlannerEntity>>::new(
            AccessPath::IndexPrefix {
                index: INDEX_B,
                values: vec![Value::Text("alpha".to_string())],
            },
            crate::db::query::ReadConsistency::MissingOk,
        );

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_pagination() {
        let mut plan_a = LogicalPlan::<Ref<PlannerEntity>>::new(
            AccessPath::FullScan,
            crate::db::query::ReadConsistency::MissingOk,
        );
        let mut plan_b = LogicalPlan::<Ref<PlannerEntity>>::new(
            AccessPath::FullScan,
            crate::db::query::ReadConsistency::MissingOk,
        );
        plan_a.page = Some(crate::db::query::plan::PageSpec {
            limit: Some(10),
            offset: 0,
        });
        plan_b.page = Some(crate::db::query::plan::PageSpec {
            limit: Some(10),
            offset: 1,
        });

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_delete_limit() {
        let mut plan_a = LogicalPlan::<Ref<PlannerEntity>>::new(
            AccessPath::FullScan,
            crate::db::query::ReadConsistency::MissingOk,
        );
        let mut plan_b = LogicalPlan::<Ref<PlannerEntity>>::new(
            AccessPath::FullScan,
            crate::db::query::ReadConsistency::MissingOk,
        );
        plan_a.mode = QueryMode::Delete(crate::db::query::DeleteSpec::new());
        plan_b.mode = QueryMode::Delete(crate::db::query::DeleteSpec::new());
        plan_a.delete_limit = Some(DeleteLimitSpec { max_rows: 2 });
        plan_b.delete_limit = Some(DeleteLimitSpec { max_rows: 3 });

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_is_stable_for_full_scan() {
        let plan = LogicalPlan::<Ref<PlannerEntity>>::new(
            AccessPath::FullScan,
            crate::db::query::ReadConsistency::MissingOk,
        );
        let fingerprint_a = plan.fingerprint();
        let fingerprint_b = plan.fingerprint();
        assert_eq!(fingerprint_a, fingerprint_b);
    }
}
