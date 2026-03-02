//! Module: query::fingerprint::fingerprint
//! Responsibility: deterministic plan fingerprint derivation from explain models.
//! Does not own: explain projection assembly or execution-plan compilation.
//! Boundary: stable plan identity hash surface for diagnostics/caching.

use crate::{
    db::{
        codec::cursor::encode_cursor,
        query::plan::AccessPlannedQuery,
        query::{explain::ExplainPlan, fingerprint::hash_parts},
    },
    traits::FieldValue,
};
use sha2::{Digest, Sha256};

///
/// PlanFingerprint
///
/// Stable, deterministic fingerprint for logical plans.
///

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct PlanFingerprint([u8; 32]);

impl PlanFingerprint {
    #[must_use]
    pub fn as_hex(&self) -> String {
        encode_cursor(&self.0)
    }
}

impl std::fmt::Display for PlanFingerprint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.as_hex())
    }
}

impl<K> AccessPlannedQuery<K>
where
    K: FieldValue,
{
    /// Compute a stable fingerprint for this logical plan.
    #[must_use]
    #[cfg(test)]
    pub(crate) fn fingerprint(&self) -> PlanFingerprint {
        let explain = self.explain();
        let projection = self.projection_spec_for_identity();
        let mut hasher = Sha256::new();
        hasher.update(b"planfp:v2");
        hash_parts::hash_explain_plan_profile_with_projection(
            &mut hasher,
            &explain,
            hash_parts::ExplainHashProfile::FingerprintV2,
            &projection,
        );
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);

        PlanFingerprint(out)
    }
}

impl ExplainPlan {
    /// Compute a stable fingerprint for this explain plan.
    #[must_use]
    pub fn fingerprint(&self) -> PlanFingerprint {
        // Phase 1: hash canonical explain fields under the current fingerprint profile.
        let mut hasher = Sha256::new();
        hasher.update(b"planfp:v2");
        hash_parts::hash_explain_plan_profile(
            &mut hasher,
            self,
            hash_parts::ExplainHashProfile::FingerprintV2,
        );

        // Phase 2: finalize into the fixed-width fingerprint payload.
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

    use crate::db::access::AccessPath;
    use crate::db::contracts::{MissingRowPolicy, Predicate};
    use crate::db::query::fingerprint::hash_parts;
    use crate::db::query::intent::{DeleteSpec, KeyAccess, LoadSpec, access_plan_from_keys_value};
    use crate::db::query::plan::expr::{
        Alias, BinaryOp, Expr, FieldId, ProjectionField, ProjectionSpec,
    };
    use crate::db::query::plan::{AccessPlannedQuery, DeleteLimitSpec, LogicalPlan, PageSpec};
    use crate::db::query::{builder::field::FieldRef, builder::sum, intent::QueryMode};
    use crate::model::index::IndexModel;
    use crate::types::{Decimal, Ulid};
    use crate::value::Value;
    use sha2::{Digest, Sha256};

    fn fingerprint_with_projection(
        plan: &AccessPlannedQuery<Value>,
        projection: &ProjectionSpec,
    ) -> super::PlanFingerprint {
        let explain = plan.explain();
        let mut hasher = Sha256::new();
        hasher.update(b"planfp:v2");
        hash_parts::hash_explain_plan_profile_with_projection(
            &mut hasher,
            &explain,
            hash_parts::ExplainHashProfile::FingerprintV2,
            projection,
        );
        let digest = hasher.finalize();
        let mut out = [0u8; 32];
        out.copy_from_slice(&digest);

        super::PlanFingerprint(out)
    }

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

        let mut plan_a: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        plan_a.scalar_plan_mut().predicate = Some(predicate_a);

        let mut plan_b: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        plan_b.scalar_plan_mut().predicate = Some(predicate_b);

        assert_eq!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_is_deterministic_for_by_keys() {
        let a = Ulid::from_u128(1);
        let b = Ulid::from_u128(2);

        let access_a = access_plan_from_keys_value(&KeyAccess::Many(vec![a, b, a]));
        let access_b = access_plan_from_keys_value(&KeyAccess::Many(vec![b, a]));

        let plan_a: AccessPlannedQuery<Value> = AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency: MissingRowPolicy::Ignore,
            }),
            access: access_a,
        };
        let plan_b: AccessPlannedQuery<Value> = AccessPlannedQuery {
            logical: LogicalPlan::Scalar(crate::db::query::plan::ScalarPlan {
                mode: QueryMode::Load(LoadSpec::new()),
                predicate: None,
                order: None,
                distinct: false,
                delete_limit: None,
                page: None,
                consistency: MissingRowPolicy::Ignore,
            }),
            access: access_b,
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

        let plan_a: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: INDEX_A,
                values: vec![Value::Text("alpha".to_string())],
            },
            MissingRowPolicy::Ignore,
        );
        let plan_b: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::IndexPrefix {
                index: INDEX_B,
                values: vec![Value::Text("alpha".to_string())],
            },
            MissingRowPolicy::Ignore,
        );

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_pagination() {
        let mut plan_a: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let mut plan_b: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        plan_a.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(10),
            offset: 0,
        });
        plan_b.scalar_plan_mut().page = Some(PageSpec {
            limit: Some(10),
            offset: 1,
        });

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_delete_limit() {
        let mut plan_a: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let mut plan_b: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        plan_a.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
        plan_b.scalar_plan_mut().mode = QueryMode::Delete(DeleteSpec::new());
        plan_a.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec { max_rows: 2 });
        plan_b.scalar_plan_mut().delete_limit = Some(DeleteLimitSpec { max_rows: 3 });

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_changes_with_distinct_flag() {
        let plan_a: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let mut plan_b: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        plan_b.scalar_plan_mut().distinct = true;

        assert_ne!(plan_a.fingerprint(), plan_b.fingerprint());
    }

    #[test]
    fn fingerprint_numeric_projection_alias_only_change_does_not_invalidate() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let numeric_projection =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                },
                alias: None,
            }]);
        let alias_only_numeric_projection =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Alias {
                    expr: Box::new(Expr::Binary {
                        op: crate::db::query::plan::expr::BinaryOp::Add,
                        left: Box::new(Expr::Field(FieldId::new("rank"))),
                        right: Box::new(Expr::Literal(Value::Int(1))),
                    }),
                    name: Alias::new("rank_plus_one_expr"),
                },
                alias: Some(Alias::new("rank_plus_one")),
            }]);

        let semantic_fingerprint = fingerprint_with_projection(&plan, &numeric_projection);
        let alias_fingerprint = fingerprint_with_projection(&plan, &alias_only_numeric_projection);

        assert_eq!(
            semantic_fingerprint, alias_fingerprint,
            "numeric projection alias wrappers must not affect fingerprint identity",
        );
    }

    #[test]
    fn fingerprint_numeric_projection_semantic_change_invalidates() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let projection_add_one =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Add,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                },
                alias: None,
            }]);
        let projection_mul_one =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: crate::db::query::plan::expr::BinaryOp::Mul,
                    left: Box::new(Expr::Field(FieldId::new("rank"))),
                    right: Box::new(Expr::Literal(Value::Int(1))),
                },
                alias: None,
            }]);

        let add_fingerprint = fingerprint_with_projection(&plan, &projection_add_one);
        let mul_fingerprint = fingerprint_with_projection(&plan, &projection_mul_one);

        assert_ne!(
            add_fingerprint, mul_fingerprint,
            "numeric projection semantic changes must invalidate fingerprint identity",
        );
    }

    #[test]
    fn fingerprint_numeric_literal_decimal_scale_is_canonicalized() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let decimal_one_scale_1 =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
                alias: None,
            }]);
        let decimal_one_scale_2 =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Literal(Value::Decimal(Decimal::new(100, 2))),
                alias: None,
            }]);

        assert_eq!(
            fingerprint_with_projection(&plan, &decimal_one_scale_1),
            fingerprint_with_projection(&plan, &decimal_one_scale_2),
            "decimal scale-only literal changes must not fragment fingerprint identity",
        );
    }

    #[test]
    fn fingerprint_literal_numeric_subtype_remains_significant_when_observable() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let int_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Literal(Value::Int(1)),
            alias: None,
        }]);
        let decimal_literal = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Literal(Value::Decimal(Decimal::new(10, 1))),
            alias: None,
        }]);

        assert_ne!(
            fingerprint_with_projection(&plan, &int_literal),
            fingerprint_with_projection(&plan, &decimal_literal),
            "top-level literal subtype remains observable and identity-significant",
        );
    }

    #[test]
    fn fingerprint_numeric_promotion_paths_do_not_fragment() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let int_plus_int = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Binary {
                op: BinaryOp::Add,
                left: Box::new(Expr::Literal(Value::Int(1))),
                right: Box::new(Expr::Literal(Value::Int(2))),
            },
            alias: None,
        }]);
        let int_plus_decimal =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Literal(Value::Int(1))),
                    right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(20, 1)))),
                },
                alias: None,
            }]);
        let decimal_plus_int =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Literal(Value::Decimal(Decimal::new(10, 1)))),
                    right: Box::new(Expr::Literal(Value::Int(2))),
                },
                alias: None,
            }]);

        let fingerprint_int_plus_int = fingerprint_with_projection(&plan, &int_plus_int);
        let fingerprint_int_plus_decimal = fingerprint_with_projection(&plan, &int_plus_decimal);
        let fingerprint_decimal_plus_int = fingerprint_with_projection(&plan, &decimal_plus_int);

        assert_eq!(fingerprint_int_plus_int, fingerprint_int_plus_decimal);
        assert_eq!(fingerprint_int_plus_int, fingerprint_decimal_plus_int);
    }

    #[test]
    fn fingerprint_aggregate_numeric_target_field_remains_significant() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let sum_rank = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Aggregate(sum("rank")),
            alias: None,
        }]);
        let sum_score = ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
            expr: Expr::Aggregate(sum("score")),
            alias: None,
        }]);

        assert_ne!(
            fingerprint_with_projection(&plan, &sum_rank),
            fingerprint_with_projection(&plan, &sum_score),
            "aggregate target field changes must invalidate fingerprint identity",
        );
    }

    #[test]
    fn fingerprint_distinct_numeric_noop_paths_stay_stable() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
        let sum_distinct_plus_int_zero =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Aggregate(sum("rank").distinct())),
                    right: Box::new(Expr::Literal(Value::Int(0))),
                },
                alias: None,
            }]);
        let sum_distinct_plus_decimal_zero =
            ProjectionSpec::from_fields_for_test(vec![ProjectionField::Scalar {
                expr: Expr::Binary {
                    op: BinaryOp::Add,
                    left: Box::new(Expr::Aggregate(sum("rank").distinct())),
                    right: Box::new(Expr::Literal(Value::Decimal(Decimal::new(0, 1)))),
                },
                alias: None,
            }]);

        assert_eq!(
            fingerprint_with_projection(&plan, &sum_distinct_plus_int_zero),
            fingerprint_with_projection(&plan, &sum_distinct_plus_decimal_zero),
            "distinct numeric no-op literal subtype differences must not fragment fingerprint identity",
        );
    }

    #[test]
    fn fingerprint_is_stable_for_full_scan() {
        let plan: AccessPlannedQuery<Value> =
            AccessPlannedQuery::new(AccessPath::<Value>::FullScan, MissingRowPolicy::Ignore);
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

        let plan_a: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEX,
                vec![Value::Uint(7)],
                Bound::Included(Value::Uint(100)),
                Bound::Excluded(Value::Uint(200)),
            ),
            MissingRowPolicy::Ignore,
        );
        let plan_b: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEX,
                vec![Value::Uint(7)],
                Bound::Included(Value::Uint(100)),
                Bound::Excluded(Value::Uint(200)),
            ),
            MissingRowPolicy::Ignore,
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

        let plan_included: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEX,
                vec![Value::Uint(7)],
                Bound::Included(Value::Uint(100)),
                Bound::Excluded(Value::Uint(200)),
            ),
            MissingRowPolicy::Ignore,
        );
        let plan_excluded: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEX,
                vec![Value::Uint(7)],
                Bound::Excluded(Value::Uint(100)),
                Bound::Excluded(Value::Uint(200)),
            ),
            MissingRowPolicy::Ignore,
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

        let plan_low_100: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEX,
                vec![Value::Uint(7)],
                Bound::Included(Value::Uint(100)),
                Bound::Excluded(Value::Uint(200)),
            ),
            MissingRowPolicy::Ignore,
        );
        let plan_low_101: AccessPlannedQuery<Value> = AccessPlannedQuery::new(
            AccessPath::index_range(
                INDEX,
                vec![Value::Uint(7)],
                Bound::Included(Value::Uint(101)),
                Bound::Excluded(Value::Uint(200)),
            ),
            MissingRowPolicy::Ignore,
        );

        assert_ne!(plan_low_100.fingerprint(), plan_low_101.fingerprint());
    }
}
