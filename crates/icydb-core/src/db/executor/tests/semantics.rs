//! Module: executor::tests::semantics
//! Responsibility: executor semantic correctness across load, aggregate, and delete behavior.
//! Does not own: executor implementation or planner lowering.
//! Boundary: verifies executor behavior through stable query and route fixtures.

use super::support::*;

#[cfg(feature = "sql")]
use crate::db::query::plan::expr::{ProjectionField, ProjectionSpec};
use crate::{
    db::{
        executor::{
            EntityAuthority,
            route::{RoutePlanRequest, build_execution_route_plan},
            validate_executor_plan_for_authority,
        },
        predicate::{CoercionId, CompareOp, ComparePredicate, Predicate},
    },
    value::Value,
};

fn query_execution_pipeline_snapshot<E>(query: &Query<E>) -> String
where
    E: EntityKind + EntityValue,
{
    // Phase 1: build query intent into one executor-owned executable plan contract.
    let plan = query
        .access_plan_for_test()
        .expect("execution pipeline snapshot should build access plan");
    let executable = crate::db::executor::PreparedExecutionPlan::<E>::from(plan);

    // Phase 2: derive canonical execution descriptor JSON from executable-plan contracts.
    let descriptor_json = executable
        .explain_load_execution_node_descriptor()
        .expect("execution pipeline snapshot should build execution descriptor")
        .render_json_canonical();

    // Phase 3: join executable-plan and explain-descriptor snapshots into one payload.
    [
        executable
            .render_snapshot_canonical()
            .expect("execution pipeline snapshot should render executable snapshot"),
        format!("execution_descriptor_json={descriptor_json}"),
    ]
    .join("\n")
}

#[cfg(feature = "sql")]
fn projection_columns_snapshot(projection: &ProjectionSpec) -> Vec<String> {
    projection
        .fields()
        .enumerate()
        .map(|(index, field)| match field {
            ProjectionField::Scalar { expr, alias } => {
                let alias_label = alias.as_ref().map_or("none", |value| value.as_str());
                format!("column[{index}]::expr={expr:?}::alias={alias_label}")
            }
        })
        .collect()
}

#[cfg(feature = "sql")]
fn query_execution_pipeline_projection_snapshot<E>(query: &Query<E>) -> String
where
    E: EntityKind + EntityValue,
{
    // Phase 1: build query intent into one executable plan + canonical projection columns.
    let plan = query
        .access_plan_for_test()
        .expect("execution pipeline projection snapshot should build access plan");
    let projection_columns = projection_columns_snapshot(&plan.projection_spec(E::MODEL));
    let executable = crate::db::executor::PreparedExecutionPlan::<E>::from(plan);

    // Phase 2: derive canonical execution descriptor JSON from executable-plan contracts.
    let descriptor_json = executable
        .explain_load_execution_node_descriptor()
        .expect("execution pipeline projection snapshot should build execution descriptor")
        .render_json_canonical();

    // Phase 3: join executable-plan, explain-descriptor, and projection-column snapshots.
    [
        executable
            .render_snapshot_canonical()
            .expect("execution pipeline projection snapshot should render executable snapshot"),
        format!("projection_columns={projection_columns:?}"),
        format!("execution_descriptor_json={descriptor_json}"),
    ]
    .join("\n")
}

fn query_grouped_execution_pipeline_snapshot<E>(query: &Query<E>) -> String
where
    E: EntityKind + EntityValue,
{
    // Phase 1: build grouped query intent into one executor-owned executable plan contract.
    let plan = query
        .access_plan_for_test()
        .expect("grouped execution pipeline snapshot should build access plan");
    let executable = crate::db::executor::PreparedExecutionPlan::<E>::from(plan);
    let authority = EntityAuthority::for_generated_type_for_test::<E>()
        .with_cursor_schema_info_for_test(
            crate::db::schema::SchemaInfo::cached_for_generated_entity_model(E::MODEL).clone(),
        );
    validate_executor_plan_for_authority(&authority, executable.logical_plan())
        .expect("grouped execution pipeline snapshot should validate executor boundary");
    let grouped_handoff =
        crate::db::query::plan::grouped_executor_handoff(executable.logical_plan())
            .expect("grouped execution pipeline snapshot should project grouped handoff");

    // Phase 2: derive grouped execution truth from grouped handoff contracts.
    let route_plan = build_execution_route_plan(
        grouped_handoff.base(),
        RoutePlanRequest::Grouped {
            grouped_plan_strategy: grouped_handoff.grouped_plan_strategy(),
        },
    )
    .expect("grouped execution pipeline snapshot should build grouped route plan");
    let grouped_execution_mode = route_plan
        .grouped_execution_mode()
        .expect("grouped execution pipeline snapshot should carry grouped execution mode");
    let descriptor_json = executable
        .explain_load_execution_node_descriptor()
        .expect("grouped execution pipeline snapshot should build grouped execution descriptor")
        .render_json_canonical();

    // Phase 3: join executable snapshot, grouped route truth, and grouped descriptor.
    [
        executable
            .render_snapshot_canonical()
            .expect("grouped execution pipeline snapshot should render executable snapshot"),
        format!("route_shape_kind={:?}", route_plan.route_shape_kind()),
        format!("route_execution_mode={:?}", route_plan.execution_mode()),
        format!(
            "route_continuation_mode={:?}",
            route_plan.continuation().mode()
        ),
        format!(
            "grouped_planner_fallback_reason={:?}",
            route_plan.grouped_plan_fallback_reason()
        ),
        format!("grouped_execution_mode={grouped_execution_mode:?}"),
        format!("execution_descriptor_json={descriptor_json}"),
    ]
    .join("\n")
}

#[cfg(feature = "sql")]
#[test]
fn query_execution_pipeline_snapshot_for_by_key_shape_with_projection_columns_is_stable() {
    let query =
        Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore).by_id(Ulid::from_u128(9_101));
    let actual = query_execution_pipeline_projection_snapshot(&query);
    let expected = r#"snapshot_version=1
plan_hash=40d5eb26f22d778191a8bc90093b37816260dd633f0baf3c5cb64dae0e337033
mode=Load(LoadSpec { limit: None, offset: 0 })
is_grouped=false
execution_family=PrimaryKey
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=All
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("id")), alias: None }, Scalar { expr: Field(FieldId("group")), alias: None }, Scalar { expr: Field(FieldId("rank")), alias: None }, Scalar { expr: Field(FieldId("label")), alias: None }] }
order_spec=None
page_spec=None
projection_coverage_flag=false
continuation_signature=9b5b6c1d12f6762a96a60204b72228ffc7bc544b972f3ad25955b448e46109c9
index_prefix_specs=[]
index_range_specs=[]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: None, offset: 0 }), access: ByKey { key: Ulid("000000000000000000000008WD") }, filter_expr: None, filter_expr_model: None, predicate: None, predicate_model: None, order_by: None, distinct: false, grouping: None, order_pushdown: MissingModelContext, page: None, delete_limit: None, consistency: Ignore }
projection_columns=["column[0]::expr=Field(FieldId(\"id\"))::alias=none", "column[1]::expr=Field(FieldId(\"group\"))::alias=none", "column[2]::expr=Field(FieldId(\"rank\"))::alias=none", "column[3]::expr=Field(FieldId(\"label\"))::alias=none"]
execution_descriptor_json={"node_id":0,"node_type":"ByKeyLookup","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"ByKey","key":"Ulid(\"000000000000000000000008WD\")"},"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":false,"fast_path_reason":"mat_fallback","residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"ByKey\")","acc_reason":"Text(\"intent_key_access_override\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_kind":"Text(\"materialized\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"access_not_cov\")","fast_path":"Text(\"none\")","fast_reason":"Text(\"mat_fallback\")","fast_reject":"List([Text(\"primary_key=pk_fast_no\"), Text(\"secondary_prefix=sec_order_na\"), Text(\"index_range=idx_limit_no\")])","ord_route_mode":"Text(\"direct_streaming\")","ord_route_reason":"Text(\"none\")","proj_fields":"List([Text(\"id\"), Text(\"group\"), Text(\"rank\"), Text(\"label\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","route_family":"Text(\"not_ordered_or_not_paginated\")","route_outcome":"Text(\"unchanged_or_not_applicable\")","route_reason":"Text(\"no_limit\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "execution pipeline + projection-column snapshot drifted; query->executable->explain->projection-columns is a stabilized 0.51 surface",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_secondary_index_ordered_shape_is_stable() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Nat64(7),
            CoercionId::Strict,
        )))
        .order_term(crate::db::asc("rank"))
        .limit(5);
    let actual = query_execution_pipeline_snapshot(&query);
    let expected = r#"snapshot_version=1
plan_hash=29d574e586051144dfd9cf8d13dfcec2344cb5f775e85d305a6f7e343afad0a4
mode=Load(LoadSpec { limit: Some(5), offset: 0 })
is_grouped=false
execution_family=Ordered
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=All
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("id")), alias: None }, Scalar { expr: Field(FieldId("group")), alias: None }, Scalar { expr: Field(FieldId("rank")), alias: None }, Scalar { expr: Field(FieldId("label")), alias: None }] }
order_spec=Some(OrderSpec { fields: [OrderTerm { label: "rank", expr: Field(FieldId("rank")), direction: Asc }, OrderTerm { label: "id", expr: Field(FieldId("id")), direction: Asc }] })
page_spec=Some(PageSpec { limit: Some(5), offset: 0 })
projection_coverage_flag=false
continuation_signature=e68dd9416ecde94e9597289b518a24e60d3fd96d2b8c6dd121e71c2f358df70d
index_prefix_specs=[{index:group_rank,bound_type:equality,lower:included(len:29:head:0000000000000010:tail:0007000100000100),upper:included(len:4186:head:0000000000000010:tail:ffffffffffffffff)}]
index_range_specs=[]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(5), offset: 0 }), access: IndexPrefix { name: "group_rank", fields: ["group", "rank"], prefix_len: 1, values: [Nat64(7)] }, filter_expr: None, filter_expr_model: None, predicate: Compare { field: "group", op: Eq, value: Nat64(7), coercion: CoercionSpec { id: Strict, params: {} } }, predicate_model: Some(Compare(ComparePredicate { field: "group", op: Eq, value: Nat64(7), coercion: CoercionSpec { id: Strict, params: {} } })), order_by: Fields([ExplainOrder { field: "rank", direction: Asc }, ExplainOrder { field: "id", direction: Asc }]), distinct: false, grouping: None, order_pushdown: MissingModelContext, page: Page { limit: Some(5), offset: 0 }, delete_limit: None, consistency: Ignore }
execution_descriptor_json={"node_id":0,"node_type":"IndexPrefixScan","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"IndexPrefix","name":"group_rank","fields":["group","rank"],"prefix_len":1,"values":["Nat64(7)"]},"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":true,"fast_path_reason":"sec_order_ok","residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"IndexPredicatePrefilter","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"full","predicate_pushdown":"strict_all_or_none","filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"pushdown":"Text(\"group=Nat64(7)\")"}},{"node_id":2,"node_type":"SecondaryOrderPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"group_rank\")","prefix_len":"Nat64(1)"}},{"node_id":3,"node_type":"TopNSeek","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"fetch":"Nat64(6)"}},{"node_id":4,"node_type":"OrderByAccessSatisfied","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"order_by_idx":"Bool(true)"}},{"node_id":5,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":5,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Nat64(0)"}}],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"IndexPrefix(group_rank)\")","acc_reason":"Text(\"selected_index_not_projected\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_kind":"Text(\"materialized\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"order_mat\")","fast_path":"Text(\"secondary_prefix\")","fast_reason":"Text(\"sec_order_ok\")","fast_reject":"List([Text(\"primary_key=pk_fast_no\"), Text(\"index_range=idx_limit_no\")])","fetch":"Nat64(6)","limit_stop_after":"Text(\"possible(limit=5,lookahead=1,fetch=6)\")","ord_route_mode":"Text(\"direct_streaming\")","ord_route_reason":"Text(\"none\")","pred_idx_cap":"Text(\"fully_indexable\")","prefix_len":"Nat64(1)","prefix_values":"List([Nat64(7)])","proj_fields":"List([Text(\"id\"), Text(\"group\"), Text(\"rank\"), Text(\"label\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","route_family":"Text(\"secondary_order\")","route_outcome":"Text(\"pushed\")","route_reason":"Text(\"secondary_order_limit_stop_proven\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "secondary-index ordered execution pipeline snapshot drifted; planner/executor boundary must remain stable",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_index_range_shape_is_stable() {
    let query = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Nat64(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lt,
                Value::Nat64(200),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Eq,
                Value::Text("keep".to_string()),
                CoercionId::Strict,
            )),
        ]))
        .order_term(crate::db::asc("code"))
        .order_term(crate::db::asc("id"))
        .limit(2);
    let actual = query_execution_pipeline_snapshot(&query);
    let expected = r#"snapshot_version=1
plan_hash=97d43812f398767b138f27387ee55f08a38747a22c695cd489f6ac078e30eb44
mode=Load(LoadSpec { limit: Some(2), offset: 0 })
is_grouped=false
execution_family=Ordered
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=All
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("id")), alias: None }, Scalar { expr: Field(FieldId("code")), alias: None }, Scalar { expr: Field(FieldId("label")), alias: None }] }
order_spec=Some(OrderSpec { fields: [OrderTerm { label: "code", expr: Field(FieldId("code")), direction: Asc }, OrderTerm { label: "id", expr: Field(FieldId("id")), direction: Asc }] })
page_spec=Some(PageSpec { limit: Some(2), offset: 0 })
projection_coverage_flag=false
continuation_signature=9e754919e6a73f6672599091b55443eaebc7355a95943c21ca7f557d1c9e4cf2
index_prefix_specs=[]
index_range_specs=[{index:code_unique,lower:included(len:26:head:0000000000000010:tail:0000000064000100),upper:excluded(len:26:head:0000000000000010:tail:00000000c8000100)}]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(2), offset: 0 }), access: IndexRange { name: "code_unique", fields: ["code"], prefix_len: 0, prefix: [], lower: Included(Nat64(100)), upper: Excluded(Nat64(200)) }, filter_expr: None, filter_expr_model: None, predicate: And([Compare { field: "code", op: Lt, value: Nat64(200), coercion: CoercionSpec { id: Strict, params: {} } }, Compare { field: "code", op: Gte, value: Nat64(100), coercion: CoercionSpec { id: Strict, params: {} } }, Compare { field: "label", op: Eq, value: Text("keep"), coercion: CoercionSpec { id: Strict, params: {} } }]), predicate_model: Some(And([Compare(ComparePredicate { field: "code", op: Lt, value: Nat64(200), coercion: CoercionSpec { id: Strict, params: {} } }), Compare(ComparePredicate { field: "code", op: Gte, value: Nat64(100), coercion: CoercionSpec { id: Strict, params: {} } }), Compare(ComparePredicate { field: "label", op: Eq, value: Text("keep"), coercion: CoercionSpec { id: Strict, params: {} } })])), order_by: Fields([ExplainOrder { field: "code", direction: Asc }, ExplainOrder { field: "id", direction: Asc }]), distinct: false, grouping: None, order_pushdown: MissingModelContext, page: Page { limit: Some(2), offset: 0 }, delete_limit: None, consistency: Ignore }
execution_descriptor_json={"node_id":0,"node_type":"IndexRangeScan","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"IndexRange","name":"code_unique","fields":["code"],"prefix_len":0,"prefix":[],"lower":"Included(Nat64(100))","upper":"Excluded(Nat64(200))"},"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":true,"fast_path_reason":"sec_order_ok","residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"ResidualFilter","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"partial","predicate_pushdown":"code>=Nat64(100) AND code<Nat64(200)","filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":"Compare { field: \"label\", op: Eq, value: Text(\"keep\"), coercion: CoercionSpec { id: Strict, params: {} } }","projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"residual_filter_shape":"Text(\"predicate\")"}},{"node_id":2,"node_type":"SecondaryOrderPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"code_unique\")","prefix_len":"Nat64(0)"}},{"node_id":3,"node_type":"IndexRangeLimitPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"fetch":"Nat64(3)"}},{"node_id":4,"node_type":"OrderByAccessSatisfied","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"order_by_idx":"Bool(true)"}},{"node_id":5,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":2,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Nat64(0)"}}],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"IndexRange(code_unique)\")","acc_reason":"Text(\"selected_index_not_projected\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_kind":"Text(\"materialized\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"order_mat\")","fast_path":"Text(\"secondary_prefix\")","fast_reason":"Text(\"sec_order_ok\")","fast_reject":"List([Text(\"primary_key=pk_fast_no\")])","fetch":"Nat64(3)","limit_stop_after":"Text(\"disabled(residual_filter_blocks_direct_streaming)\")","ord_route_mode":"Text(\"materialized_fallback\")","ord_route_reason":"Text(\"residual_filter_blocks_direct_streaming\")","pred_idx_cap":"Text(\"partially_indexable\")","prefix_len":"Nat64(0)","prefix_values":"List([])","proj_fields":"List([Text(\"id\"), Text(\"code\"), Text(\"label\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","route_family":"Text(\"residual_filter_ordered_scan\")","route_outcome":"Text(\"residual_unbounded\")","route_reason":"Text(\"residual_filter_blocks_direct_streaming\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "index-range execution pipeline snapshot drifted; planner/executor boundary must remain stable",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_grouped_aggregate_shape_is_stable() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Nat64(7),
            CoercionId::Strict,
        )))
        .group_by("group")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(2);
    let actual = query_grouped_execution_pipeline_snapshot(&query);
    let expected = r#"snapshot_version=1
plan_hash=268e62959e8d32cda18e6d35449cf8f853d66e93b72226b27de717eda4d490a8
mode=Load(LoadSpec { limit: Some(2), offset: 0 })
is_grouped=true
execution_family=Grouped
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=Declared
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("group")), alias: None }, Scalar { expr: Aggregate(AggregateExpr { kind: Count, input_expr: None, filter_expr: None, distinct: false }), alias: None }] }
order_spec=None
page_spec=Some(PageSpec { limit: Some(2), offset: 0 })
projection_coverage_flag=true
continuation_signature=174e44364ab233ae12166525253e45b0deec90d373707fac8d588972988c977a
index_prefix_specs=[{index:group_rank,bound_type:equality,lower:included(len:29:head:0000000000000010:tail:0007000100000100),upper:included(len:4186:head:0000000000000010:tail:ffffffffffffffff)}]
index_range_specs=[]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(2), offset: 0 }), access: IndexPrefix { name: "group_rank", fields: ["group", "rank"], prefix_len: 1, values: [Nat64(7)] }, filter_expr: None, filter_expr_model: None, predicate: Compare { field: "group", op: Eq, value: Nat64(7), coercion: CoercionSpec { id: Strict, params: {} } }, predicate_model: Some(Compare(ComparePredicate { field: "group", op: Eq, value: Nat64(7), coercion: CoercionSpec { id: Strict, params: {} } })), order_by: None, distinct: false, grouping: Grouped { strategy: "ordered_group", fallback_reason: None, group_fields: [ExplainGroupField { slot_index: 1, field: "group" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, input_expr: None, filter_expr: None, distinct: false }], having: None, max_groups: 18446744073709551615, max_group_bytes: 18446744073709551615 }, order_pushdown: MissingModelContext, page: Page { limit: Some(2), offset: 0 }, delete_limit: None, consistency: Ignore }
route_shape_kind=AggregateGrouped
route_execution_mode=Materialized
route_continuation_mode=Initial
grouped_planner_fallback_reason=None
grouped_execution_mode=OrderedStreaming
execution_descriptor_json={"node_id":0,"node_type":"IndexPrefixScan","layer":"scan","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":{"type":"IndexPrefix","name":"group_rank","fields":["group","rank"],"prefix_len":1,"values":["Nat64(7)"]},"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":false,"fast_path_reason":"mat_fallback","residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"IndexPredicatePrefilter","layer":"pipeline","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"full","predicate_pushdown":"strict_all_or_none","filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"pushdown":"Text(\"group=Nat64(7)\")"}},{"node_id":2,"node_type":"GroupedAggregateOrderedStreaming","layer":"aggregate","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"aggregate_contract":"Text(\"grouped\")","aggregate_physical":"Text(\"ordered_streaming\")","grouped_execution_mode":"Text(\"ordered_streaming\")","grouped_plan_fallback_reason":"Text(\"none\")"}},{"node_id":3,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"filter_expr":null,"residual_filter_expr":null,"fast_path_selected":null,"fast_path_reason":null,"residual_filter_predicate":null,"projection":null,"ordering_source":null,"limit":2,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Nat64(0)"}}],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"IndexPrefix(group_rank)\")","acc_reason":"Text(\"selected_index_not_projected\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_kind":"Text(\"materialized\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"proj_not_cov\")","fast_path":"Text(\"none\")","fast_reason":"Text(\"mat_fallback\")","fast_reject":"List([])","grouped_execution_mode":"Text(\"ordered_streaming\")","grouped_plan_fallback_reason":"Text(\"none\")","ord_route_mode":"Text(\"direct_streaming\")","ord_route_reason":"Text(\"none\")","pred_idx_cap":"Text(\"fully_indexable\")","prefix_len":"Nat64(1)","prefix_values":"List([Nat64(7)])","proj_fields":"List([Text(\"group\"), Text(\"aggregate\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "grouped aggregate execution pipeline snapshot drifted; grouped planner/executor boundary must remain stable",
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_execution_pipeline_snapshot_marks_covering_read_route_for_coverable_projection() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Nat64(7),
            CoercionId::Strict,
        )))
        .select_fields(["id", "group"])
        .order_term(crate::db::asc("id"))
        .limit(1);
    let actual = query_execution_pipeline_projection_snapshot(&query);

    assert!(
        actual.contains("load_terminal_fast_path=CoveringRead"),
        "executor snapshot should surface the explicit covering-read route",
    );
    assert!(
        actual.contains("\"cov_read_route\":\"Text(\\\"covering_read\\\")\""),
        "execution descriptor should surface the same covering-read route label",
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_execution_pipeline_snapshot_marks_covering_read_route_for_pk_by_key_projection() {
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_511)),
            CoercionId::Strict,
        )))
        .select_fields(["id"])
        .order_term(crate::db::asc("id"));
    let actual = query_execution_pipeline_projection_snapshot(&query);

    assert!(
        actual.contains("load_terminal_fast_path=CoveringRead"),
        "PK by-key projection snapshot should surface the explicit covering-read route",
    );
    assert!(
        actual.contains("\"node_type\":\"ByKeyLookup\""),
        "PK by-key projection snapshot should keep the by-key access root",
    );
    assert!(
        actual.contains("\"existing_row_mode\":\"Text(\\\"row_check_required\\\")\""),
        "PK by-key projection snapshot should expose the explicit row-check mode",
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_execution_pipeline_snapshot_marks_covering_read_route_for_pk_by_keys_projection() {
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter_predicate(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(Ulid::from_u128(9_511)),
                Value::Ulid(Ulid::from_u128(9_513)),
            ]),
            CoercionId::Strict,
        )))
        .select_fields(["id"])
        .order_term(crate::db::asc("id"));
    let actual = query_execution_pipeline_projection_snapshot(&query);

    assert!(
        actual.contains("load_terminal_fast_path=CoveringRead"),
        "PK by-keys projection snapshot should surface the explicit covering-read route",
    );
    assert!(
        actual.contains("\"node_type\":\"ByKeysLookup\""),
        "PK by-keys projection snapshot should keep the by-keys access root",
    );
    assert!(
        actual.contains("\"existing_row_mode\":\"Text(\\\"row_check_required\\\")\""),
        "PK by-keys projection snapshot should expose the explicit row-check mode",
    );
}
