/*

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        db::query::predicate::SchemaInfo,
        db::query::predicate::coercion::CoercionSpec,
        model::index::IndexModel,
        traits::{
            EntityKind, FieldValues, SanitizeAuto, SanitizeCustom, ValidateAuto, ValidateCustom,
            View, Visitable,
        },
        types::Ulid,
        value::Value,
    };
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    pub struct PlannerEntity {
        id: Ulid,
        idx_a: String,
        idx_b: String,
        other: String,
    }

    crate::test_entity! {
        entity PlannerEntity {
            path: "planner_test::PlannerEntity",
            pk: id: Ulid,

            fields { id: Ulid, idx_a: Text, idx_b: Text, other: Text }

            indexes { index idx_a_idx_b(idx_a, idx_b); }
        }
    }

    impl View for PlannerEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for PlannerEntity {}
    impl SanitizeCustom for PlannerEntity {}
    impl ValidateAuto for PlannerEntity {}
    impl ValidateCustom for PlannerEntity {}
    impl Visitable for PlannerEntity {}

    impl FieldValues for PlannerEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Ulid(self.id)),
                "idx_a" => Some(Value::Text(self.idx_a.clone())),
                "idx_b" => Some(Value::Text(self.idx_b.clone())),
                "other" => Some(Value::Text(self.other.clone())),
                _ => None,
            }
        }
    }

    #[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
    struct MultiIndexEntity {
        id: Ulid,
        idx_a: String,
        idx_b: String,
    }

    crate::test_entity! {
        entity MultiIndexEntity {
            path: "planner_test::MultiIndexEntity",
            pk: id: Ulid,

            fields { id: Ulid, idx_a: Text, idx_b: Text }

            indexes {
                index idx_a_b(idx_a, idx_b);
                index idx_a_alt(idx_a);
                index idx_a(idx_a);
            }
        }
    }

    impl View for MultiIndexEntity {
        type ViewType = Self;

        fn to_view(&self) -> Self::ViewType {
            self.clone()
        }

        fn from_view(view: Self::ViewType) -> Self {
            view
        }
    }

    impl SanitizeAuto for MultiIndexEntity {}
    impl SanitizeCustom for MultiIndexEntity {}
    impl ValidateAuto for MultiIndexEntity {}
    impl ValidateCustom for MultiIndexEntity {}
    impl Visitable for MultiIndexEntity {}

    impl FieldValues for MultiIndexEntity {
        fn get_value(&self, field: &str) -> Option<Value> {
            match field {
                "id" => Some(Value::Ulid(self.id)),
                "idx_a" => Some(Value::Text(self.idx_a.clone())),
                "idx_b" => Some(Value::Text(self.idx_b.clone())),
                _ => None,
            }
        }
    }

    fn model_schema() -> SchemaInfo {
        SchemaInfo::from_entity_model(PlannerEntity::MODEL).expect("valid model")
    }

    fn planner_index() -> IndexModel {
        *<PlannerEntity as EntityKind>::INDEXES[0]
    }

    fn strict() -> CoercionSpec {
        CoercionSpec::new(CoercionId::Strict)
    }

    fn non_strict() -> CoercionSpec {
        CoercionSpec::new(CoercionId::TextCasefold)
    }

    fn eq(field: &str, value: Value, coercion: CoercionSpec) -> Predicate {
        Predicate::Compare(ComparePredicate {
            field: field.to_string(),
            op: CompareOp::Eq,
            value,
            coercion,
        })
    }

    fn in_list(field: &str, values: Vec<Value>, coercion: CoercionSpec) -> Predicate {
        Predicate::Compare(ComparePredicate {
            field: field.to_string(),
            op: CompareOp::In,
            value: Value::List(values),
            coercion,
        })
    }

    fn v_text(s: &str) -> Value {
        Value::Text(s.to_string())
    }

    #[test]
    fn pk_eq_strict_plans_by_key() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = eq("id", Value::Ulid(id), strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(id)));
    }

    #[test]
    fn pk_in_strict_plans_by_keys() {
        let schema = model_schema();
        let a = Ulid::default();
        let b = Ulid::from_bytes([1u8; 16]);
        let predicate = in_list("id", vec![Value::Ulid(a), Value::Ulid(b)], strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::ByKeys(vec![a, b])));
    }

    #[test]
    fn pk_eq_non_strict_rejected() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = eq("id", Value::Ulid(id), non_strict());

        assert!(plan_access::<PlannerEntity>(&schema, Some(&predicate)).is_err());
    }

    #[test]
    fn index_eq_strict_plans_prefix() {
        let schema = model_schema();
        let predicate = eq("idx_a", v_text("alpha"), strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Path(AccessPath::IndexPrefix {
                index: planner_index(),
                values: vec![v_text("alpha")],
            })
        );
    }

    #[test]
    fn index_in_strict_plans_union_of_prefixes() {
        let schema = model_schema();
        let predicate = in_list("idx_a", vec![v_text("a"), v_text("b")], strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Union(vec![
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: planner_index(),
                    values: vec![v_text("a")],
                }),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: planner_index(),
                    values: vec![v_text("b")],
                }),
            ])
        );
    }

    #[test]
    fn index_non_first_field_falls_back_to_full_scan() {
        let schema = model_schema();
        let predicate = eq("idx_b", v_text("beta"), strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn index_non_strict_falls_back_to_full_scan() {
        let schema = model_schema();
        let predicate = eq("idx_a", v_text("alpha"), non_strict());
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn and_two_indexable_predicates_intersect() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Intersection(vec![
                AccessPlan::Path(AccessPath::ByKey(id)),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: planner_index(),
                    values: vec![v_text("alpha")],
                }),
            ])
        );
    }

    #[test]
    fn and_indexable_with_non_indexable_normalizes_to_indexable() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::ByKey(id)));
    }

    #[test]
    fn mixed_pk_non_strict_and_index_strict_rejected() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::And(vec![
            eq("id", Value::Ulid(id), non_strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);

        assert!(plan_access::<PlannerEntity>(&schema, Some(&predicate)).is_err());
    }

    #[test]
    fn and_non_indexable_predicates_fall_back_to_full_scan() {
        let schema = model_schema();
        let predicate = Predicate::And(vec![
            eq("idx_b", v_text("beta"), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn composite_prefix_requires_strict_coercions() {
        let schema = model_schema();
        let predicate = Predicate::And(vec![
            eq("idx_a", v_text("a"), strict()),
            eq("idx_b", v_text("b"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Intersection(vec![
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: planner_index(),
                    values: vec![v_text("a"), v_text("b")],
                }),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: planner_index(),
                    values: vec![v_text("a")],
                }),
            ])
        );

        let non_strict_predicate = Predicate::And(vec![
            eq("idx_a", v_text("a"), strict()),
            eq("idx_b", v_text("b"), non_strict()),
        ]);
        let non_strict_plan =
            plan_access::<PlannerEntity>(&schema, Some(&non_strict_predicate)).unwrap();

        assert_eq!(
            non_strict_plan,
            AccessPlan::Path(AccessPath::IndexPrefix {
                index: planner_index(),
                values: vec![v_text("a")],
            })
        );
    }

    #[test]
    fn index_prefix_from_and_prefers_longest_prefix_then_name() {
        let schema = SchemaInfo::from_entity_model(MultiIndexEntity::MODEL).expect("valid model");

        let children = vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("idx_b", v_text("beta"), strict()),
        ];
        let first = index_prefix_from_and::<MultiIndexEntity>(&schema, &children)
            .expect("index prefix")
            .expect("index prefix missing");
        let second = index_prefix_from_and::<MultiIndexEntity>(&schema, &children)
            .expect("index prefix")
            .expect("index prefix missing");
        assert_eq!(first, second);

        let AccessPath::IndexPrefix { index, values } = first else {
            panic!("expected index prefix path");
        };
        assert_eq!(index.name, "planner_test::MultiIndexEntity::idx_a_b");
        assert_eq!(values, vec![v_text("alpha"), v_text("beta")]);

        let children = vec![eq("idx_a", v_text("alpha"), strict())];
        let AccessPath::IndexPrefix { index, .. } =
            index_prefix_from_and::<MultiIndexEntity>(&schema, &children)
                .expect("index prefix")
                .expect("index prefix missing")
        else {
            panic!("expected index prefix path");
        };
        assert_eq!(index.name, "planner_test::MultiIndexEntity::idx_a");
    }

    #[test]
    fn or_two_indexable_predicates_union() {
        let schema = model_schema();
        let id = Ulid::default();
        let predicate = Predicate::Or(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(
            plan,
            AccessPlan::Union(vec![
                AccessPlan::Path(AccessPath::ByKey(id)),
                AccessPlan::Path(AccessPath::IndexPrefix {
                    index: planner_index(),
                    values: vec![v_text("alpha")],
                }),
            ])
        );
    }

    #[test]
    fn or_indexable_with_non_indexable_normalizes_to_full_scan() {
        let schema = model_schema();
        let predicate = Predicate::Or(vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("other", v_text("x"), strict()),
        ]);
        let plan = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn empty_and_or_normalize_to_full_scan() {
        let schema = model_schema();
        let empty_and = Predicate::And(Vec::new());
        let empty_or = Predicate::Or(Vec::new());

        let and_plan = plan_access::<PlannerEntity>(&schema, Some(&empty_and)).unwrap();
        let or_plan = plan_access::<PlannerEntity>(&schema, Some(&empty_or)).unwrap();

        assert_eq!(and_plan, AccessPlan::Path(AccessPath::FullScan));
        assert_eq!(or_plan, AccessPlan::Path(AccessPath::FullScan));
    }

    #[test]
    fn nested_or_and_flatten_deterministically() {
        let schema = model_schema();
        let id = Ulid::default();
        let nested = Predicate::Or(vec![
            Predicate::Or(vec![eq("idx_a", v_text("alpha"), strict())]),
            Predicate::Or(vec![eq("id", Value::Ulid(id), strict())]),
        ]);
        let direct = Predicate::Or(vec![
            eq("id", Value::Ulid(id), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);

        let nested_plan = plan_access::<PlannerEntity>(&schema, Some(&nested)).unwrap();
        let direct_plan = plan_access::<PlannerEntity>(&schema, Some(&direct)).unwrap();

        assert_eq!(nested_plan, direct_plan);
    }

    #[test]
    fn predicate_order_does_not_change_access_plan() {
        let schema = model_schema();
        let a = Predicate::And(vec![
            eq("id", Value::Ulid(Ulid::default()), strict()),
            eq("idx_a", v_text("alpha"), strict()),
        ]);
        let b = Predicate::And(vec![
            eq("idx_a", v_text("alpha"), strict()),
            eq("id", Value::Ulid(Ulid::default()), strict()),
        ]);

        let plan_a = plan_access::<PlannerEntity>(&schema, Some(&a)).unwrap();
        let plan_b = plan_access::<PlannerEntity>(&schema, Some(&b)).unwrap();

        assert_eq!(plan_a, plan_b);
    }

    #[test]
    fn deterministic_output_across_runs() {
        let schema = model_schema();
        let predicate = eq("idx_a", v_text("alpha"), strict());

        let plan_a = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();
        let plan_b = plan_access::<PlannerEntity>(&schema, Some(&predicate)).unwrap();

        assert_eq!(plan_a, plan_b);
    }
}
*/
