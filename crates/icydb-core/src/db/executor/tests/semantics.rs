//! Module: db::executor::tests::semantics
//! Responsibility: module-local ownership and contracts for db::executor::tests::semantics.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use super::*;
#[cfg(feature = "sql")]
use crate::db::query::plan::expr::{ProjectionField, ProjectionSpec};
use crate::{
    db::{
        executor::{
            EntityAuthority, route::build_execution_route_plan_for_grouped_plan,
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
    // Phase 1: compile query intent into one executor-owned executable plan contract.
    let compiled = query
        .plan()
        .expect("execution pipeline snapshot should build compiled query");
    let executable = crate::db::executor::ExecutablePlan::from(compiled);

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
    // Phase 1: compile query intent into one executable plan + canonical projection columns.
    let compiled = query
        .plan()
        .expect("execution pipeline projection snapshot should build compiled query");
    let projection_columns = projection_columns_snapshot(&compiled.projection_spec());
    let executable = crate::db::executor::ExecutablePlan::from(compiled);

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
    // Phase 1: compile grouped query intent into one executor-owned executable plan contract.
    let compiled = query
        .plan()
        .expect("grouped execution pipeline snapshot should build compiled query");
    let executable = crate::db::executor::ExecutablePlan::from(compiled);
    validate_executor_plan_for_authority(
        EntityAuthority::for_type::<E>(),
        executable.logical_plan(),
    )
    .expect("grouped execution pipeline snapshot should validate executor boundary");
    let grouped_handoff =
        crate::db::query::plan::grouped_executor_handoff(executable.logical_plan())
            .expect("grouped execution pipeline snapshot should project grouped handoff");

    // Phase 2: derive grouped route observability from grouped handoff contracts.
    let route_plan = build_execution_route_plan_for_grouped_plan(
        E::MODEL,
        grouped_handoff.base(),
        grouped_handoff.grouped_plan_strategy(),
    );
    let grouped_observability = route_plan
        .grouped_observability()
        .expect("grouped execution pipeline snapshot should project grouped observability");
    let descriptor_json = executable
        .explain_load_execution_node_descriptor()
        .expect("grouped execution pipeline snapshot should build grouped execution descriptor")
        .render_json_canonical();

    // Phase 3: join executable snapshot, grouped route observability, and grouped descriptor.
    [
        executable
            .render_snapshot_canonical()
            .expect("grouped execution pipeline snapshot should render executable snapshot"),
        format!(
            "route_execution_mode_case={:?}",
            route_plan.shape().execution_mode_case()
        ),
        format!(
            "route_execution_mode={:?}",
            route_plan.shape().execution_mode(),
        ),
        format!(
            "route_continuation_mode={:?}",
            route_plan.continuation().capabilities().mode()
        ),
        format!("grouped_outcome={:?}", grouped_observability.outcome()),
        format!(
            "grouped_rejection={:?}",
            grouped_observability.rejection_reason()
        ),
        format!(
            "grouped_planner_fallback_reason={:?}",
            grouped_observability.planner_fallback_reason()
        ),
        format!("grouped_eligible={}", grouped_observability.eligible()),
        format!(
            "grouped_execution_mode={:?}",
            grouped_observability.execution_mode()
        ),
        format!(
            "grouped_execution_strategy={:?}",
            grouped_observability.grouped_execution_strategy()
        ),
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
plan_hash=411b8c1bc919db245b2598c8d016182acc7ac2b0f9b5c41b493706c5526090c1
mode=Load(LoadSpec { limit: None, offset: 0 })
is_grouped=false
execution_strategy=PrimaryKey
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=All
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("id")), alias: None }, Scalar { expr: Field(FieldId("group")), alias: None }, Scalar { expr: Field(FieldId("rank")), alias: None }, Scalar { expr: Field(FieldId("label")), alias: None }] }
order_spec=None
page_spec=None
projection_coverage_flag=false
continuation_signature=355c1739abb9dd4cd89e22d9ac3902c76e6054c27f51684814f299061274e637
index_prefix_specs=[]
index_range_specs=[]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: None, offset: 0 }), access: ByKey { key: Ulid(Ulid(Ulid(9101))) }, predicate: None, predicate_model: None, order_by: None, distinct: false, grouping: None, order_pushdown: MissingModelContext, page: None, delete_limit: None, consistency: Ignore }
projection_columns=["column[0]::expr=Field(FieldId(\"id\"))::alias=none", "column[1]::expr=Field(FieldId(\"group\"))::alias=none", "column[2]::expr=Field(FieldId(\"rank\"))::alias=none", "column[3]::expr=Field(FieldId(\"label\"))::alias=none"]
execution_descriptor_json={"node_id":0,"node_type":"ByKeyLookup","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"ByKey","key":"Ulid(Ulid(Ulid(9101)))"},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"ByKey\")","acc_reason":"Text(\"non_index_access\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"access_not_cov\")","fast_path":"Text(\"none\")","fast_reason":"Text(\"mat_fallback\")","fast_reject":"List([Text(\"primary_key=pk_fast_no\"), Text(\"secondary_prefix=sec_order_na\"), Text(\"index_range=idx_limit_no\")])","ord_route_contract":"Text(\"direct_streaming\")","ord_route_reason":"Text(\"none\")","proj_fields":"List([Text(\"id\"), Text(\"group\"), Text(\"rank\"), Text(\"label\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "execution pipeline + projection-column snapshot drifted; query->executable->explain->projection-columns is a stabilized 0.51 surface",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_secondary_index_ordered_shape_is_stable() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .order_by("rank")
        .limit(5);
    let actual = query_execution_pipeline_snapshot(&query);
    let expected = r#"snapshot_version=1
plan_hash=8af939186fa06a6eb38eb60248e6ae6e7e58fe94fce3fe3b41feaf1796624c8d
mode=Load(LoadSpec { limit: Some(5), offset: 0 })
is_grouped=false
execution_strategy=Ordered
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=All
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("id")), alias: None }, Scalar { expr: Field(FieldId("group")), alias: None }, Scalar { expr: Field(FieldId("rank")), alias: None }, Scalar { expr: Field(FieldId("label")), alias: None }] }
order_spec=Some(OrderSpec { fields: [("rank", Asc), ("id", Asc)] })
page_spec=Some(PageSpec { limit: Some(5), offset: 0 })
projection_coverage_flag=false
continuation_signature=39fc8a0c3f8a1b09f24c9c77e093f31bafbbb678aa0158e45c7be3225b730b40
index_prefix_specs=[{index:group_rank,bound_type:equality,lower:included(len:29:head:0000000000000010:tail:0007000100000100),upper:included(len:4187:head:0000000000000010:tail:ffffffffffffffff)}]
index_range_specs=[]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(5), offset: 0 }), access: IndexPrefix { name: "group_rank", fields: ["group", "rank"], prefix_len: 1, values: [Uint(7)] }, predicate: Compare { field: "group", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } }, predicate_model: Some(Compare(ComparePredicate { field: "group", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } })), order_by: Fields([ExplainOrder { field: "rank", direction: Asc }, ExplainOrder { field: "id", direction: Asc }]), distinct: false, grouping: None, order_pushdown: MissingModelContext, page: Page { limit: Some(5), offset: 0 }, delete_limit: None, consistency: Ignore }
execution_descriptor_json={"node_id":0,"node_type":"IndexPrefixScan","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"IndexPrefix","name":"group_rank","fields":["group","rank"],"prefix_len":1,"values":["Uint(7)"]},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"IndexPredicatePrefilter","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"full","predicate_pushdown":"strict_all_or_none","fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"pushdown":"Text(\"group=Uint(7)\")"}},{"node_id":2,"node_type":"SecondaryOrderPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"group_rank\")","prefix_len":"Uint(1)"}},{"node_id":3,"node_type":"TopNSeek","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"fetch":"Uint(6)"}},{"node_id":4,"node_type":"OrderByAccessSatisfied","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"order_by_idx":"Bool(true)"}},{"node_id":5,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":5,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Uint(0)"}}],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"IndexPrefix(group_rank)\")","acc_reason":"Text(\"single_candidate\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"order_mat\")","fast_path":"Text(\"secondary_prefix\")","fast_reason":"Text(\"sec_order_ok\")","fast_reject":"List([Text(\"primary_key=pk_fast_no\"), Text(\"index_range=idx_limit_no\")])","fetch":"Uint(6)","ord_route_contract":"Text(\"direct_streaming\")","ord_route_reason":"Text(\"none\")","pred_idx_cap":"Text(\"fully_indexable\")","prefix_len":"Uint(1)","prefix_values":"List([Uint(7)])","proj_fields":"List([Text(\"id\"), Text(\"group\"), Text(\"rank\"), Text(\"label\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "secondary-index ordered execution pipeline snapshot drifted; planner/executor boundary must remain stable",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_index_range_shape_is_stable() {
    let query = Query::<UniqueIndexRangeEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::And(vec![
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Gte,
                Value::Uint(100),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "code",
                CompareOp::Lt,
                Value::Uint(200),
                CoercionId::Strict,
            )),
            Predicate::Compare(ComparePredicate::with_coercion(
                "label",
                CompareOp::Eq,
                Value::Text("keep".to_string()),
                CoercionId::Strict,
            )),
        ]))
        .order_by("code")
        .order_by("id")
        .limit(2);
    let actual = query_execution_pipeline_snapshot(&query);
    let expected = r#"snapshot_version=1
plan_hash=2e29ba4a86e6dc8a607003423193134041e1cf39f5c7e19367f9a07fbdc252e3
mode=Load(LoadSpec { limit: Some(2), offset: 0 })
is_grouped=false
execution_strategy=Ordered
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=All
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("id")), alias: None }, Scalar { expr: Field(FieldId("code")), alias: None }, Scalar { expr: Field(FieldId("label")), alias: None }] }
order_spec=Some(OrderSpec { fields: [("code", Asc), ("id", Asc)] })
page_spec=Some(PageSpec { limit: Some(2), offset: 0 })
projection_coverage_flag=false
continuation_signature=12eac166be92fff10fe514b841eee9f53407f59d29bb67631aa80ebb3488fdc3
index_prefix_specs=[]
index_range_specs=[{index:code_unique,lower:included(len:26:head:0000000000000010:tail:0000000064000100),upper:excluded(len:26:head:0000000000000010:tail:00000000c8000100)}]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(2), offset: 0 }), access: IndexRange { name: "code_unique", fields: ["code"], prefix_len: 0, prefix: [], lower: Included(Uint(100)), upper: Excluded(Uint(200)) }, predicate: And([Compare { field: "code", op: Lt, value: Uint(200), coercion: CoercionSpec { id: Strict, params: {} } }, Compare { field: "code", op: Gte, value: Uint(100), coercion: CoercionSpec { id: Strict, params: {} } }, Compare { field: "label", op: Eq, value: Text("keep"), coercion: CoercionSpec { id: Strict, params: {} } }]), predicate_model: Some(And([Compare(ComparePredicate { field: "code", op: Lt, value: Uint(200), coercion: CoercionSpec { id: Strict, params: {} } }), Compare(ComparePredicate { field: "code", op: Gte, value: Uint(100), coercion: CoercionSpec { id: Strict, params: {} } }), Compare(ComparePredicate { field: "label", op: Eq, value: Text("keep"), coercion: CoercionSpec { id: Strict, params: {} } })])), order_by: Fields([ExplainOrder { field: "code", direction: Asc }, ExplainOrder { field: "id", direction: Asc }]), distinct: false, grouping: None, order_pushdown: MissingModelContext, page: Page { limit: Some(2), offset: 0 }, delete_limit: None, consistency: Ignore }
execution_descriptor_json={"node_id":0,"node_type":"IndexRangeScan","layer":"scan","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":{"type":"IndexRange","name":"code_unique","fields":["code"],"prefix_len":0,"prefix":[],"lower":"Included(Uint(100))","upper":"Excluded(Uint(200))"},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"ResidualPredicateFilter","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"partial","predicate_pushdown":"code>=Uint(100) AND code<Uint(200)","fast_path_selected":null,"fast_path_reason":null,"residual_predicate":"And([Compare { field: \"code\", op: Lt, value: Uint(200), coercion: CoercionSpec { id: Strict, params: {} } }, Compare { field: \"code\", op: Gte, value: Uint(100), coercion: CoercionSpec { id: Strict, params: {} } }, Compare { field: \"label\", op: Eq, value: Text(\"keep\"), coercion: CoercionSpec { id: Strict, params: {} } }])","projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{}},{"node_id":2,"node_type":"SecondaryOrderPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"index":"Text(\"code_unique\")","prefix_len":"Uint(0)"}},{"node_id":3,"node_type":"IndexRangeLimitPushdown","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"fetch":"Uint(3)"}},{"node_id":4,"node_type":"OrderByAccessSatisfied","layer":"pipeline","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"order_by_idx":"Bool(true)"}},{"node_id":5,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Streaming","execution_mode_detail":"streaming","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":2,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Uint(0)"}}],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"IndexRange(code_unique)\")","acc_reason":"Text(\"single_candidate\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"order_mat\")","fast_path":"Text(\"secondary_prefix\")","fast_reason":"Text(\"sec_order_ok\")","fast_reject":"List([Text(\"primary_key=pk_fast_no\")])","fetch":"Uint(3)","ord_route_contract":"Text(\"materialized_fallback\")","ord_route_reason":"Text(\"residual_predicate_blocks_direct_streaming\")","pred_idx_cap":"Text(\"partially_indexable\")","prefix_len":"Uint(0)","prefix_values":"List([])","proj_fields":"List([Text(\"id\"), Text(\"code\"), Text(\"label\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "index-range execution pipeline snapshot drifted; planner/executor boundary must remain stable",
    );
}

#[test]
fn query_execution_pipeline_snapshot_for_grouped_aggregate_shape_is_stable() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .group_by("group")
        .expect("group field should resolve")
        .aggregate(crate::db::count())
        .limit(2);
    let actual = query_grouped_execution_pipeline_snapshot(&query);
    let expected = r#"snapshot_version=1
plan_hash=952ac7a21d2bd69ffea5cfaec34cc05364637ed571a2b1cd7246c22c80c291f3
mode=Load(LoadSpec { limit: Some(2), offset: 0 })
is_grouped=true
execution_strategy=Grouped
load_terminal_fast_path=Materialized
ordering_direction=Asc
distinct_execution_strategy=None
projection_selection=Declared
projection_spec=ProjectionSpec { fields: [Scalar { expr: Field(FieldId("group")), alias: None }, Scalar { expr: Aggregate(AggregateExpr { kind: Count, target_field: None, distinct: false }), alias: None }] }
order_spec=None
page_spec=Some(PageSpec { limit: Some(2), offset: 0 })
projection_coverage_flag=true
continuation_signature=efde04b0a75f535f33ab3b39b7dc0b26a4f06e1350681ad1e5a3f334e6600785
index_prefix_specs=[{index:group_rank,bound_type:equality,lower:included(len:29:head:0000000000000010:tail:0007000100000100),upper:included(len:4187:head:0000000000000010:tail:ffffffffffffffff)}]
index_range_specs=[]
explain_plan=ExplainPlan { mode: Load(LoadSpec { limit: Some(2), offset: 0 }), access: IndexPrefix { name: "group_rank", fields: ["group", "rank"], prefix_len: 1, values: [Uint(7)] }, predicate: Compare { field: "group", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } }, predicate_model: Some(Compare(ComparePredicate { field: "group", op: Eq, value: Uint(7), coercion: CoercionSpec { id: Strict, params: {} } })), order_by: None, distinct: false, grouping: Grouped { strategy: OrderedGroup, fallback_reason: None, group_fields: [ExplainGroupField { slot_index: 1, field: "group" }], aggregates: [ExplainGroupAggregate { kind: Count, target_field: None, distinct: false }], having: None, max_groups: 18446744073709551615, max_group_bytes: 18446744073709551615 }, order_pushdown: MissingModelContext, page: Page { limit: Some(2), offset: 0 }, delete_limit: None, consistency: Ignore }
route_execution_mode_case=AggregateGrouped
route_execution_mode=Materialized
route_continuation_mode=Initial
grouped_outcome=MaterializedFallback
grouped_rejection=None
grouped_planner_fallback_reason=None
grouped_eligible=true
grouped_execution_mode=Materialized
grouped_execution_strategy=OrderedMaterialized
execution_descriptor_json={"node_id":0,"node_type":"IndexPrefixScan","layer":"scan","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":{"type":"IndexPrefix","name":"group_rank","fields":["group","rank"],"prefix_len":1,"values":["Uint(7)"]},"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":false,"rows_expected":null,"children":[{"node_id":1,"node_type":"IndexPredicatePrefilter","layer":"pipeline","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"full","predicate_pushdown":"strict_all_or_none","fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"pushdown":"Text(\"group=Uint(7)\")"}},{"node_id":2,"node_type":"GroupedAggregateOrderedMaterialized","layer":"aggregate","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":null,"cursor":null,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"grouped_execution_strategy":"Text(\"ordered_materialized\")","grouped_plan_fallback_reason":"Text(\"none\")","grouped_route_eligible":"Bool(true)","grouped_route_outcome":"Text(\"materialized_fallback\")","grouped_route_rejection_reason":"Text(\"none\")"}},{"node_id":3,"node_type":"LimitOffset","layer":"terminal","execution_mode":"Materialized","execution_mode_detail":"materialized","access_strategy":null,"predicate_pushdown_mode":"none","predicate_pushdown":null,"fast_path_selected":null,"fast_path_reason":null,"residual_predicate":null,"projection":null,"ordering_source":null,"limit":2,"cursor":false,"covering_scan":null,"rows_expected":null,"children":[],"node_properties":{"offset":"Uint(0)"}}],"node_properties":{"acc_alts":"List([])","acc_choice":"Text(\"IndexPrefix(group_rank)\")","acc_reason":"Text(\"single_candidate\")","acc_reject":"List([])","cont_mode":"Text(\"initial\")","cov_read_route":"Text(\"materialized\")","cov_scan_reason":"Text(\"proj_not_cov\")","fast_path":"Text(\"none\")","fast_reason":"Text(\"mat_fallback\")","fast_reject":"List([])","grouped_execution_strategy":"Text(\"ordered_materialized\")","grouped_plan_fallback_reason":"Text(\"none\")","grouped_route_eligible":"Bool(true)","grouped_route_outcome":"Text(\"materialized_fallback\")","grouped_route_rejection_reason":"Text(\"none\")","ord_route_contract":"Text(\"direct_streaming\")","ord_route_reason":"Text(\"none\")","pred_idx_cap":"Text(\"fully_indexable\")","prefix_len":"Uint(1)","prefix_values":"List([Uint(7)])","proj_fields":"List([Text(\"group\"), Text(\"aggregate\")])","proj_pushdown":"Bool(false)","resume_from":"Text(\"none\")","scan_dir":"Text(\"asc\")"}}"#.to_string();

    assert_eq!(
        actual, expected,
        "grouped aggregate execution pipeline snapshot drifted; grouped planner/executor boundary must remain stable",
    );
}

#[cfg(feature = "sql")]
#[test]
fn query_execution_pipeline_snapshot_marks_covering_read_route_for_coverable_projection() {
    let query = Query::<PushdownParityEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "group",
            CompareOp::Eq,
            Value::Uint(7),
            CoercionId::Strict,
        )))
        .select_fields(["id", "group"])
        .order_by("id")
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

#[test]
fn query_execution_pipeline_snapshot_marks_covering_read_route_for_pk_by_key_projection() {
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::Eq,
            Value::Ulid(Ulid::from_u128(9_511)),
            CoercionId::Strict,
        )))
        .select_fields(["id"])
        .order_by("id");
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

#[test]
fn query_execution_pipeline_snapshot_marks_covering_read_route_for_pk_by_keys_projection() {
    let query = Query::<SimpleEntity>::new(MissingRowPolicy::Ignore)
        .filter(Predicate::Compare(ComparePredicate::with_coercion(
            "id",
            CompareOp::In,
            Value::List(vec![
                Value::Ulid(Ulid::from_u128(9_511)),
                Value::Ulid(Ulid::from_u128(9_513)),
            ]),
            CoercionId::Strict,
        )))
        .select_fields(["id"])
        .order_by("id");
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
