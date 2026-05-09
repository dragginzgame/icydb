use super::support::*;

// Assert one delete-window intent shape remains fail-closed until an explicit
// ORDER BY is present.
fn assert_delete_window_requires_order(label: &str, limit: Option<u32>, offset: Option<u32>) {
    let model = basic_model();
    let mut intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore).delete();
    if let Some(limit) = limit {
        intent = intent.limit(limit);
    }
    if let Some(offset) = offset {
        intent = intent.offset(offset);
    }

    assert!(
        matches!(
            intent.build_plan_model(),
            Err(QueryError::Intent(IntentError::PlanShape(
                crate::db::query::plan::validate::PolicyPlanError::DeleteWindowRequiresOrder
            )))
        ),
        "{label}: delete window without order should be rejected",
    );
}

// Assert one load pagination shape remains fail-closed until an explicit
// ORDER BY is present.
fn assert_unordered_pagination_rejects(label: &str, limit: Option<u32>, offset: Option<u32>) {
    let mut query = Query::<PlanEntity>::new(MissingRowPolicy::Ignore);
    if let Some(limit) = limit {
        query = query.limit(limit);
    }
    if let Some(offset) = offset {
        query = query.offset(offset);
    }

    let err = query
        .plan()
        .expect_err("unordered pagination shape must fail");

    assert!(
        query_error_is_policy_plan_error(&err, |inner| {
            matches!(
                inner,
                crate::db::query::plan::validate::PolicyPlanError::UnorderedPagination
            )
        }),
        "{label}: unordered pagination should map to PolicyPlanError::UnorderedPagination",
    );
}

#[test]
fn intent_rejects_by_ids_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .by_ids([Ulid::generate()])
        .filter_predicate(Predicate::True);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::ByIdsWithPredicate))
    ));
}

#[test]
fn intent_rejects_only_with_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .only(Ulid::generate())
        .filter_predicate(Predicate::True);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::OnlyWithPredicate))
    ));
}

#[test]
fn intent_rejects_delete_window_without_order_matrix() {
    for (label, limit, offset) in [
        ("delete limit without order", Some(1), None),
        ("delete offset without order", None, Some(10)),
    ] {
        assert_delete_window_requires_order(label, limit, offset);
    }
}

#[test]
fn intent_accepts_ordered_delete_offset_shape() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .offset(10)
        .delete()
        .order_term(crate::db::asc("id"));

    intent
        .build_plan_model()
        .expect("ordered delete with offset should pass intent validation");
}

#[test]
fn delete_query_rejects_grouped_shape_during_intent_validation() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .delete()
        .group_by("name")
        .expect("group field should resolve")
        .plan()
        .expect_err("delete queries must reject grouped logical shape during intent validation");

    assert!(matches!(
        err,
        QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::DeletePlanWithGrouping
        ))
    ));
}

#[test]
fn load_rejects_duplicate_non_primary_order_field() {
    let err = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("name"))
        .order_term(crate::db::desc("name"))
        .limit(1)
        .plan()
        .expect_err("duplicate non-primary order field must fail");

    assert!(query_error_is_order_plan_error(&err, |inner| {
        matches!(
            inner,
            crate::db::query::plan::validate::OrderPlanError::DuplicateOrderField { field }
                if field == "name"
        )
    }));
}

#[test]
fn load_unordered_pagination_rejects_matrix() {
    for (label, limit, offset) in [
        ("limit without order", Some(1), None),
        ("offset without order", None, Some(1)),
        ("limit+offset without order", Some(10), Some(2)),
    ] {
        assert_unordered_pagination_rejects(label, limit, offset);
    }
}

#[test]
fn load_ordered_pagination_is_allowed() {
    Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("name"))
        .limit(10)
        .offset(2)
        .plan()
        .expect("ordered pagination should plan");
}

#[test]
fn ordered_plan_appends_primary_key_tie_break() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("name"))
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("ordered query should carry order spec");

    assert_eq!(
        order.fields,
        vec![
            crate::db::query::plan::OrderTerm::field("name", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Asc),
        ],
        "canonical order should append primary key as terminal tie-break"
    );
}

#[test]
fn ordered_plan_moves_primary_key_to_terminal_position() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .order_term(crate::db::desc("id"))
        .order_term(crate::db::asc("name"))
        .plan()
        .expect("ordered plan should build")
        .into_inner();
    let order = plan
        .scalar_plan()
        .order
        .as_ref()
        .expect("ordered query should carry order spec");

    assert_eq!(
        order.fields,
        vec![
            crate::db::query::plan::OrderTerm::field("name", OrderDirection::Asc),
            crate::db::query::plan::OrderTerm::field("id", OrderDirection::Desc),
        ],
        "canonical order must keep exactly one terminal PK tie-break with requested direction"
    );
}

#[test]
fn typed_order_terms_preserve_expression_shape_without_sort_parsing() {
    let plain = crate::db::OrderTerm::asc(crate::db::field("name")).lower();
    let lowered = crate::db::OrderTerm::desc(crate::db::lower("name")).lower();

    assert_eq!(plain.rendered_label(), "name");
    assert!(matches!(
        plain.expr(),
        crate::db::query::plan::expr::Expr::Field(field) if field.as_str() == "name"
    ));
    assert_eq!(lowered.rendered_label(), "LOWER(name)");
    assert!(matches!(
        lowered.expr(),
        crate::db::query::plan::expr::Expr::FunctionCall {
            function: crate::db::query::plan::expr::Function::Lower,
            args,
        } if matches!(args.as_slice(), [crate::db::query::plan::expr::Expr::Field(field)] if field.as_str() == "name")
    ));
}

#[test]
fn intent_rejects_empty_order_spec() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .order_spec(OrderSpec { fields: Vec::new() });

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::PlanShape(
            crate::db::query::plan::validate::PolicyPlanError::EmptyOrderSpec
        )))
    ));
}

#[test]
fn intent_rejects_conflicting_key_access() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore)
        .by_id(Ulid::generate())
        .by_ids([Ulid::generate()]);

    assert!(matches!(
        intent.build_plan_model(),
        Err(QueryError::Intent(IntentError::KeyAccessConflict))
    ));
}

#[test]
fn typed_by_ids_matches_by_id_access() {
    let key = Ulid::generate();

    let by_id = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_id(key)
        .plan()
        .expect("by_id plan")
        .into_inner();
    let by_ids = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_ids([key])
        .plan()
        .expect("by_ids plan")
        .into_inner();

    assert_eq!(by_id, by_ids);
}

#[test]
fn explicit_key_access_override_keeps_generic_planner_owned_reason() {
    let key = Ulid::generate();

    let by_id = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_id(key)
        .plan()
        .expect("by_id plan")
        .into_inner();
    let by_ids = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_ids([key])
        .plan()
        .expect("by_ids plan")
        .into_inner();
    let only = Query::<PlanSingleton>::new(MissingRowPolicy::Ignore)
        .only()
        .plan()
        .expect("only plan")
        .into_inner();

    assert_eq!(
        by_id.access_choice().chosen_reason.code(),
        "intent_key_access_override",
        "explicit by_id access should keep one stored planner-owned override reason instead of falling back to raw access-shape projection",
    );
    assert_eq!(
        by_ids.access_choice().chosen_reason.code(),
        "intent_key_access_override",
        "explicit by_ids access should share the same generic override reason so one-key by_ids stays identical to by_id",
    );
    assert_eq!(
        only.access_choice().chosen_reason.code(),
        "intent_key_access_override",
        "only() should keep the same generic override reason because it is also an explicit fluent key-access override",
    );
}

#[test]
fn by_id_limit_one_without_order_simplifies_paging_shape() {
    let key = Ulid::generate();
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .by_id(key)
        .limit(1)
        .plan()
        .expect("by_id + limit(1) plan should build")
        .into_inner();

    assert!(
        plan.scalar_plan().page.is_none(),
        "by_id + limit(1) with no offset should remove redundant page metadata"
    );
    assert!(
        matches!(
            plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKey(by_key) if *by_key == Value::Ulid(key))
        ),
        "by_id + limit(1) should keep exact ByKey access",
    );
}

#[test]
fn by_key_access_strips_redundant_primary_key_equality_predicate() {
    let key = Ulid::generate();
    let model_plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .by_id(key)
        .filter(FieldRef::new("id").eq(key))
        .build_plan_model()
        .expect("model by_id + id == literal plan should build");
    let AccessPlannedQuery {
        logical,
        access,
        projection_selection: _projection_selection,
        ..
    } = model_plan;
    let typed_plan = AccessPlannedQuery::from_parts(logical, access);

    assert!(
        typed_plan.scalar_plan().predicate.is_none(),
        "by_id + id == literal should strip redundant scalar predicate"
    );
    assert!(
        matches!(
            typed_plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKey(by_key) if *by_key == Value::Ulid(key))
        ),
        "redundant predicate stripping must keep the exact ByKey path"
    );
}

#[test]
fn by_keys_access_strips_redundant_primary_key_in_predicate() {
    let key1 = Ulid::from_u128(9_811);
    let key2 = Ulid::from_u128(9_813);
    let model_plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(key2),
                Value::Ulid(key1),
                Value::Ulid(key2),
            ]),
            CoercionId::Strict,
        )))
        .build_plan_model()
        .expect("model id IN literal-set plan should build");
    let AccessPlannedQuery {
        logical,
        access,
        projection_selection: _projection_selection,
        ..
    } = model_plan;
    let typed_plan = AccessPlannedQuery::from_parts(logical, access);

    assert!(
        typed_plan.scalar_plan().predicate.is_none(),
        "exact primary-key IN sets should strip redundant scalar predicates",
    );
    assert!(
        matches!(
            typed_plan.access,
            AccessPlan::Path(path)
                if matches!(
                    path.as_ref(),
                    AccessPath::ByKeys(keys)
                        if keys == &vec![Value::Ulid(key1), Value::Ulid(key2)]
                )
        ),
        "redundant predicate stripping must keep the canonical ByKeys path",
    );
}

#[test]
fn key_range_access_strips_redundant_primary_key_half_open_bounds() {
    let lower = Ulid::from_u128(9_811);
    let upper = Ulid::from_u128(9_813);
    let model_plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "id",
                CompareOp::Gte,
                Value::Ulid(lower),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "id",
                CompareOp::Lt,
                Value::Ulid(upper),
                CoercionId::Strict,
            )),
        ]))
        .build_plan_model()
        .expect("model id half-open range plan should build");
    let AccessPlannedQuery {
        logical,
        access,
        projection_selection: _projection_selection,
        ..
    } = model_plan;
    let typed_plan = AccessPlannedQuery::from_parts(logical, access);

    assert!(
        typed_plan.scalar_plan().predicate.is_none(),
        "exact primary-key half-open ranges should strip redundant scalar predicates",
    );
    assert!(
        matches!(
            typed_plan.access,
            AccessPlan::Path(path)
                if matches!(
                    path.as_ref(),
                    AccessPath::KeyRange { start, end }
                        if *start == Value::Ulid(lower) && *end == Value::Ulid(upper)
                )
        ),
        "redundant predicate stripping must keep the exact KeyRange path",
    );
}

#[test]
fn singleton_only_uses_default_key() {
    let plan = Query::<PlanSingleton>::new(MissingRowPolicy::Ignore)
        .only()
        .plan()
        .expect("singleton plan")
        .into_inner();

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::ByKey(Value::Unit))
    ));
}

#[test]
fn build_plan_model_full_scan_without_predicate() {
    let model = basic_model();
    let intent = QueryModel::<Ulid>::new(model, MissingRowPolicy::Ignore);
    let plan = intent.build_plan_model().expect("model plan should build");

    assert!(matches!(
        plan.access,
        AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan)
    ));
}

#[test]
fn build_plan_model_limit_zero_lowers_to_empty_by_keys() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .order_term(crate::db::asc("id"))
        .limit(0)
        .build_plan_model()
        .expect("ordered limit(0) plan should build");

    assert!(matches!(
        &plan.access,
        AccessPlan::Path(path)
            if matches!(path.as_ref(), AccessPath::ByKeys(keys) if keys.is_empty())
    ));
    assert_eq!(
        plan.access_choice().chosen_reason.code(),
        "limit_zero_window",
        "limit-zero access short-circuit should keep its own builder-owned chosen reason instead of falling back to the generic empty by-keys label",
    );
}

#[test]
fn build_plan_model_constant_false_lowers_to_empty_by_keys() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::False)
        .build_plan_model()
        .expect("constant false plan should build");

    assert!(
        matches!(
            &plan.access,
            AccessPlan::Path(path)
                if matches!(path.as_ref(), AccessPath::ByKeys(keys) if keys.is_empty())
        ),
        "constant-false filter should lower to empty by-keys access"
    );
    assert_eq!(
        plan.access_choice().chosen_reason.code(),
        "constant_false_predicate",
        "constant-false access short-circuit should keep its own builder-owned chosen reason instead of falling back to the generic empty by-keys label",
    );
    assert!(
        matches!(plan.scalar_plan().predicate, Some(Predicate::False)),
        "constant-false filter should remain visible in logical predicate for explain stability"
    );
}

#[test]
fn build_plan_model_constant_true_elides_logical_predicate() {
    let plan = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::True)
        .build_plan_model()
        .expect("constant true plan should build");

    assert!(
        plan.scalar_plan().predicate.is_none(),
        "constant-true filter should be folded away before logical planning"
    );
    assert!(
        matches!(
            &plan.access,
            AccessPlan::Path(path) if matches!(path.as_ref(), AccessPath::FullScan)
        ),
        "constant-true filter should not force access routing changes",
    );
}

#[test]
fn typed_plan_matches_model_plan_for_same_intent() {
    let predicate = FieldRef::new("id").eq(Ulid::default());

    let model_intent = QueryModel::<Ulid>::new(PlanEntity::MODEL, MissingRowPolicy::Ignore)
        .filter(predicate.clone())
        .order_term(crate::db::asc("name"))
        .limit(10)
        .offset(2);

    let model_plan = model_intent.build_plan_model().expect("model plan");
    let AccessPlannedQuery {
        logical: _model_logical,
        access: _model_access,
        projection_selection: _projection_selection,
        ..
    } = model_plan.clone();
    let mut model_as_typed = model_plan;
    model_as_typed.finalize_planner_route_profile_for_model(PlanEntity::MODEL);
    model_as_typed
        .finalize_static_planning_shape_for_model_only(PlanEntity::MODEL)
        .expect("model-backed parity plan should freeze static planning shape");

    let typed_plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .filter(predicate)
        .order_term(crate::db::asc("name"))
        .limit(10)
        .offset(2)
        .plan()
        .expect("typed plan")
        .into_inner();

    assert_eq!(model_as_typed, typed_plan);
}

#[test]
fn query_distinct_defaults_to_false() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        !plan.scalar_plan().distinct,
        "distinct should default to false for new query intents"
    );
}

#[test]
fn query_distinct_sets_logical_plan_flag() {
    let plan = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .distinct()
        .plan()
        .expect("typed plan")
        .into_inner();

    assert!(
        plan.scalar_plan().distinct,
        "distinct should be true when query intent enables distinct"
    );
}

#[cfg(feature = "sql")]
#[test]
fn compiled_query_projection_spec_lowers_scalar_fields_in_model_order() {
    let compiled = Query::<PlanEntity>::new(MissingRowPolicy::Ignore)
        .plan()
        .expect("plan should build");
    let field_names = compiled
        .projection_spec()
        .fields()
        .map(|field| match field {
            ProjectionField::Scalar {
                expr: Expr::Field(field),
                alias: None,
            } => field.as_str().to_string(),
            other @ ProjectionField::Scalar { .. } => {
                panic!("scalar projection should lower to plain field exprs: {other:?}")
            }
        })
        .collect::<Vec<_>>();

    assert_eq!(field_names, vec!["id".to_string(), "name".to_string()]);
}
