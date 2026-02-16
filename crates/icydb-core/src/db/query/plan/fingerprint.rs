//! Deterministic plan fingerprinting derived from the explain projection.
#![allow(clippy::cast_possible_truncation)]

use super::ExplainPlan;
use crate::{db::query::plan::hash_parts, traits::FieldValue};
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
        crate::db::cursor::encode_cursor(&self.0)
    }
}

impl std::fmt::Display for PlanFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

impl<K> super::LogicalPlan<K>
where
    K: FieldValue,
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
        hash_parts::hash_explain_plan_profile(
            &mut hasher,
            self,
            hash_parts::ExplainHashProfile::FingerprintV2,
        );
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);
        PlanFingerprint(out)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use std::ops::Bound;

    use crate::db::query::intent::{KeyAccess, access_plan_from_keys_value};
    use crate::db::query::plan::{AccessPath, DeleteLimitSpec, LogicalPlan};
    use crate::db::query::predicate::Predicate;
    use crate::db::query::{FieldRef, QueryMode, ReadConsistency};
    use crate::model::index::IndexModel;
    use crate::types::Ulid;
    use crate::value::Value;

    #[test]
    fn fingerprint_is_deterministic_for_equivalent_predicates() {
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

        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_is_deterministic_for_by_keys() {
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

        let plan_a: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexPrefix {
                index: INDEX_A,
                values: vec![Value::Text("alpha".to_string())],
            },
            crate::db::query::ReadConsistency::MissingOk,
        );
        let plan_b: LogicalPlan<Value> = LogicalPlan::new(
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
            offset: 1,
        });

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_delete_limit() {
        let mut plan_a: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        let mut plan_b: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        plan_a.mode = QueryMode::Delete(crate::db::query::DeleteSpec::new());
        plan_b.mode = QueryMode::Delete(crate::db::query::DeleteSpec::new());
        plan_a.delete_limit = Some(DeleteLimitSpec { max_rows: 2 });
        plan_b.delete_limit = Some(DeleteLimitSpec { max_rows: 3 });

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_is_stable_for_full_scan() {
        let plan: LogicalPlan<Value> =
            LogicalPlan::new(AccessPath::<Value>::FullScan, ReadConsistency::MissingOk);
        let fingerprint_a = plan.fingerprint();
        let fingerprint_b = plan.fingerprint();
        assert_eq!(fingerprint_a, fingerprint_b);
    }

    #[test]
    fn fingerprint_is_stable_for_equivalent_index_range_bounds() {
        const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
        const INDEX: IndexModel = IndexModel::new(
            "fingerprint::group_rank",
            "fingerprint::store",
            &INDEX_FIELDS,
            false,
        );

        let plan_a: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexRange {
                index: INDEX,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(100)),
                upper: Bound::Excluded(Value::Uint(200)),
            },
            ReadConsistency::MissingOk,
        );
        let plan_b: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexRange {
                index: INDEX,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(100)),
                upper: Bound::Excluded(Value::Uint(200)),
            },
            ReadConsistency::MissingOk,
        );

        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_when_index_range_bound_discriminant_changes() {
        const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
        const INDEX: IndexModel = IndexModel::new(
            "fingerprint::group_rank",
            "fingerprint::store",
            &INDEX_FIELDS,
            false,
        );

        let plan_included: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexRange {
                index: INDEX,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(100)),
                upper: Bound::Excluded(Value::Uint(200)),
            },
            ReadConsistency::MissingOk,
        );
        let plan_excluded: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexRange {
                index: INDEX,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Excluded(Value::Uint(100)),
                upper: Bound::Excluded(Value::Uint(200)),
            },
            ReadConsistency::MissingOk,
        );

        assert_ne!(plan_included.fingerprint(), plan_excluded.fingerprint());
    }

    #[test]
    fn fingerprint_changes_when_index_range_bound_value_changes() {
        const INDEX_FIELDS: [&str; 2] = ["group", "rank"];
        const INDEX: IndexModel = IndexModel::new(
            "fingerprint::group_rank",
            "fingerprint::store",
            &INDEX_FIELDS,
            false,
        );

        let plan_low_100: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexRange {
                index: INDEX,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(100)),
                upper: Bound::Excluded(Value::Uint(200)),
            },
            ReadConsistency::MissingOk,
        );
        let plan_low_101: LogicalPlan<Value> = LogicalPlan::new(
            AccessPath::IndexRange {
                index: INDEX,
                prefix: vec![Value::Uint(7)],
                lower: Bound::Included(Value::Uint(101)),
                upper: Bound::Excluded(Value::Uint(200)),
            },
            ReadConsistency::MissingOk,
        );

        assert_ne!(plan_low_100.fingerprint(), plan_low_101.fingerprint());
    }
}
