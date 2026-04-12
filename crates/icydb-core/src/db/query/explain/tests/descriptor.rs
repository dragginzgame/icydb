use super::*;

#[test]
fn explain_execution_public_node_type_vocabulary_stays_stable() {
    let actual = [
        ExplainExecutionNodeType::ByKeyLookup.as_str(),
        ExplainExecutionNodeType::IndexPrefixScan.as_str(),
        ExplainExecutionNodeType::IndexRangeScan.as_str(),
        ExplainExecutionNodeType::OrderByAccessSatisfied.as_str(),
        ExplainExecutionNodeType::OrderByMaterializedSort.as_str(),
        ExplainExecutionNodeType::CoveringRead.as_str(),
        ExplainExecutionNodeType::AggregateCount.as_str(),
        ExplainExecutionNodeType::GroupedAggregateHashMaterialized.as_str(),
        ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized.as_str(),
    ];
    let expected = [
        "ByKeyLookup",
        "IndexPrefixScan",
        "IndexRangeScan",
        "OrderByAccessSatisfied",
        "OrderByMaterializedSort",
        "CoveringRead",
        "AggregateCount",
        "GroupedAggregateHashMaterialized",
        "GroupedAggregateOrderedMaterialized",
    ];

    assert_eq!(
        actual, expected,
        "high-level public execution-node vocabulary drifted",
    );
}

#[test]
fn execution_descriptor_verbose_text_renders_all_optional_fields() {
    let mut node_properties = ExplainPropertyMap::new();
    node_properties.insert("fetch", Value::from(7_u64));
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::TopNSeek,
        execution_mode: ExplainExecutionMode::Streaming,
        access_strategy: Some(ExplainAccessPath::FullScan),
        predicate_pushdown: Some("strict_all_or_none".to_string()),
        residual_predicate: Some(ExplainPredicate::IsNull {
            field: "rank".to_string(),
        }),
        projection: Some("index_only".to_string()),
        ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
        limit: Some(3),
        cursor: Some(false),
        covering_scan: Some(true),
        rows_expected: Some(3),
        children: Vec::new(),
        node_properties,
    };

    let verbose = descriptor.render_text_tree_verbose();
    assert!(
        verbose.contains("TopNSeek execution_mode=Streaming"),
        "verbose execution text should render root node heading",
    );
    assert!(
        verbose.contains("access_strategy=FullScan"),
        "verbose execution text should render access strategy details",
    );
    assert!(
        verbose.contains("predicate_pushdown=strict_all_or_none"),
        "verbose execution text should render predicate pushdown details",
    );
    assert!(
        verbose.contains("node_properties=fetch="),
        "verbose execution text should render node properties",
    );
}

fn assert_execution_json_public_contract_fields(
    json: &str,
    expected_node_type: &str,
    expected_layer: &str,
    expected_execution_mode: &str,
    expected_execution_mode_detail: &str,
    expected_pushdown_mode: &str,
) {
    let required_fields = [
        "\"node_type\":",
        "\"layer\":",
        "\"execution_mode\":",
        "\"execution_mode_detail\":",
        "\"predicate_pushdown_mode\":",
        "\"children\":",
    ];

    for field in required_fields {
        assert!(
            json.contains(field),
            "canonical execution JSON missing stable public field `{field}`",
        );
    }

    assert!(
        json.contains(&format!("\"node_type\":\"{expected_node_type}\"")),
        "canonical execution JSON must expose one stable node type",
    );
    assert!(
        json.contains(&format!("\"layer\":\"{expected_layer}\"")),
        "canonical execution JSON must expose one stable node family layer",
    );
    assert!(
        json.contains(&format!("\"execution_mode\":\"{expected_execution_mode}\"")),
        "canonical execution JSON must expose one stable execution mode",
    );
    assert!(
        json.contains(&format!(
            "\"execution_mode_detail\":\"{expected_execution_mode_detail}\""
        )),
        "canonical execution JSON must expose one stable execution mode detail",
    );
    assert!(
        json.contains(&format!(
            "\"predicate_pushdown_mode\":\"{expected_pushdown_mode}\""
        )),
        "canonical execution JSON must expose one stable pushdown classification",
    );
}

#[test]
fn execution_descriptor_canonical_json_public_contract_is_stable() {
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::TopNSeek,
        execution_mode: ExplainExecutionMode::Streaming,
        access_strategy: Some(ExplainAccessPath::FullScan),
        predicate_pushdown: None,
        residual_predicate: None,
        projection: Some("index_only".to_string()),
        ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
        limit: Some(3),
        cursor: Some(false),
        covering_scan: Some(true),
        rows_expected: Some(3),
        children: vec![ExplainExecutionNodeDescriptor {
            node_type: ExplainExecutionNodeType::LimitOffset,
            execution_mode: ExplainExecutionMode::Materialized,
            access_strategy: None,
            predicate_pushdown: None,
            residual_predicate: None,
            projection: None,
            ordering_source: None,
            limit: Some(1),
            cursor: None,
            covering_scan: None,
            rows_expected: None,
            children: Vec::new(),
            node_properties: ExplainPropertyMap::new(),
        }],
        node_properties: ExplainPropertyMap::new(),
    };

    let json = descriptor.render_json_canonical();

    assert_execution_json_public_contract_fields(
        &json,
        "TopNSeek",
        "pipeline",
        "Streaming",
        "streaming",
        "none",
    );
    assert!(
        json.contains("\"access_strategy\":{\"type\":\"FullScan\"}"),
        "canonical execution JSON should expose one stable access family",
    );
    assert!(
        json.contains("\"projection\":\"index_only\""),
        "canonical execution JSON should expose one stable projection contract when present",
    );
    assert!(
        json.contains("\"ordering_source\":\"AccessOrder\""),
        "canonical execution JSON should expose one stable ordering-source contract when present",
    );
    assert!(
        json.contains("\"limit\":3"),
        "canonical execution JSON should expose one stable limit contract when present",
    );
    assert!(
        json.contains("\"cursor\":false"),
        "canonical execution JSON should expose one stable cursor contract when present",
    );
    assert!(
        json.contains("\"covering_scan\":true"),
        "canonical execution JSON should expose one stable covering contract when present",
    );
    assert!(
        json.contains("\"rows_expected\":3"),
        "canonical execution JSON should expose one stable row estimate when present",
    );
    assert!(
        json.contains("\"children\":[{")
            && json.contains("\"node_type\":\"LimitOffset\"")
            && json.contains("\"layer\":\"terminal\""),
        "canonical execution JSON should keep child node family semantics stable",
    );
}

#[test]
fn execution_descriptor_canonical_json_schema_is_consistent_across_node_families() {
    let cases = [
        (
            "scan",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::IndexRangeScan,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
        (
            "pipeline",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::TopNSeek,
                execution_mode: ExplainExecutionMode::Streaming,
                access_strategy: None,
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
        (
            "aggregate",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::AggregateCount,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
        (
            "terminal",
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::LimitOffset,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: None,
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: None,
                limit: None,
                cursor: None,
                covering_scan: None,
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
        ),
    ];

    for (expected_layer, descriptor) in cases {
        let json = descriptor.render_json_canonical();
        assert_execution_json_public_contract_fields(
            &json,
            descriptor.node_type().as_str(),
            expected_layer,
            if descriptor.execution_mode() == ExplainExecutionMode::Streaming {
                "Streaming"
            } else {
                "Materialized"
            },
            if descriptor.execution_mode() == ExplainExecutionMode::Streaming {
                "streaming"
            } else {
                "materialized"
            },
            "none",
        );
    }
}

#[test]
fn execution_descriptor_canonical_json_missing_optional_fields_keep_public_contract_intact() {
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::LimitOffset,
        execution_mode: ExplainExecutionMode::Materialized,
        access_strategy: None,
        predicate_pushdown: None,
        residual_predicate: None,
        projection: None,
        ordering_source: None,
        limit: None,
        cursor: None,
        covering_scan: None,
        rows_expected: None,
        children: Vec::new(),
        node_properties: ExplainPropertyMap::new(),
    };

    let json = descriptor.render_json_canonical();

    assert_execution_json_public_contract_fields(
        &json,
        "LimitOffset",
        "terminal",
        "Materialized",
        "materialized",
        "none",
    );
    assert!(
        json.contains("\"children\":[]"),
        "minimal canonical execution JSON should keep an explicit child list",
    );
}

fn assert_execution_public_metadata_parity(
    descriptor: &ExplainExecutionNodeDescriptor,
    expected_layer: &str,
    expected_execution_mode_detail: &str,
    expected_pushdown_mode: &str,
    expected_fast_path_selected: Option<bool>,
    expected_fast_path_reason: Option<&str>,
) {
    let text = descriptor.render_text_tree();
    let json = descriptor.render_json_canonical();

    assert!(
        text.contains(&format!("layer={expected_layer}")),
        "text execution explain must expose stable layer ownership",
    );
    assert!(
        json.contains(&format!("\"layer\":\"{expected_layer}\"")),
        "JSON execution explain must expose stable layer ownership",
    );
    assert!(
        text.contains(&format!(
            "execution_mode_detail={expected_execution_mode_detail}"
        )),
        "text execution explain must expose execution_mode_detail",
    );
    assert!(
        json.contains(&format!(
            "\"execution_mode_detail\":\"{expected_execution_mode_detail}\""
        )),
        "JSON execution explain must expose execution_mode_detail",
    );
    assert!(
        text.contains(&format!("predicate_pushdown_mode={expected_pushdown_mode}")),
        "text execution explain must expose predicate pushdown mode",
    );
    assert!(
        json.contains(&format!(
            "\"predicate_pushdown_mode\":\"{expected_pushdown_mode}\""
        )),
        "JSON execution explain must expose predicate pushdown mode",
    );

    if let Some(selected) = expected_fast_path_selected {
        assert!(
            text.contains(&format!("fast_path_selected={selected}")),
            "text execution explain must expose fast-path selection when present",
        );
        assert!(
            json.contains(&format!("\"fast_path_selected\":{selected}")),
            "JSON execution explain must expose fast-path selection when present",
        );
    }

    if let Some(reason) = expected_fast_path_reason {
        assert!(
            text.contains(&format!("fast_path_reason={reason}")),
            "text execution explain must expose fast-path reason when present",
        );
        assert!(
            json.contains(&format!("\"fast_path_reason\":\"{reason}\"")),
            "JSON execution explain must expose fast-path reason when present",
        );
    }
}

#[test]
fn execution_descriptor_text_json_additive_metadata_parity_is_stable_for_route_shapes() {
    let mut fast_path_properties = ExplainPropertyMap::new();
    fast_path_properties.insert(
        "fast_path_selected",
        Value::Text("secondary_index".to_string()),
    );
    fast_path_properties.insert(
        "fast_path_selected_reason",
        Value::Text("topn_eligible".to_string()),
    );

    let cases = [
        (
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::AggregateSeekFirst,
                execution_mode: ExplainExecutionMode::Materialized,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: Some(ExplainExecutionOrderingSource::IndexSeekFirst { fetch: 1 }),
                limit: None,
                cursor: Some(false),
                covering_scan: Some(false),
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
            "aggregate",
            "materialized",
            "none",
            None,
            None,
        ),
        (
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::AggregateExists,
                execution_mode: ExplainExecutionMode::Streaming,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: None,
                residual_predicate: None,
                projection: None,
                ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
                limit: Some(3),
                cursor: Some(true),
                covering_scan: Some(false),
                rows_expected: None,
                children: Vec::new(),
                node_properties: ExplainPropertyMap::new(),
            },
            "aggregate",
            "streaming",
            "none",
            None,
            None,
        ),
        (
            ExplainExecutionNodeDescriptor {
                node_type: ExplainExecutionNodeType::TopNSeek,
                execution_mode: ExplainExecutionMode::Streaming,
                access_strategy: Some(ExplainAccessPath::FullScan),
                predicate_pushdown: Some("strict_all_or_none".to_string()),
                residual_predicate: None,
                projection: None,
                ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
                limit: Some(5),
                cursor: Some(false),
                covering_scan: Some(false),
                rows_expected: Some(5),
                children: Vec::new(),
                node_properties: fast_path_properties.clone(),
            },
            "pipeline",
            "streaming",
            "full",
            Some(true),
            Some("topn_eligible"),
        ),
    ];

    for (
        descriptor,
        expected_layer,
        expected_execution_mode_detail,
        expected_pushdown_mode,
        expected_fast_path_selected,
        expected_fast_path_reason,
    ) in cases
    {
        assert_execution_public_metadata_parity(
            &descriptor,
            expected_layer,
            expected_execution_mode_detail,
            expected_pushdown_mode,
            expected_fast_path_selected,
            expected_fast_path_reason,
        );
    }
}

#[test]
fn execution_descriptor_pushdown_mode_projection_is_stable() {
    let mut descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::IndexPredicatePrefilter,
        execution_mode: ExplainExecutionMode::Materialized,
        access_strategy: None,
        predicate_pushdown: None,
        residual_predicate: None,
        projection: None,
        ordering_source: None,
        limit: None,
        cursor: None,
        covering_scan: None,
        rows_expected: None,
        children: Vec::new(),
        node_properties: ExplainPropertyMap::new(),
    };

    let none_mode = descriptor.render_json_canonical();
    assert!(
        none_mode.contains("\"predicate_pushdown_mode\":\"none\""),
        "missing pushdown mode `none` projection",
    );

    descriptor.predicate_pushdown = Some("strict_all_or_none".to_string());
    let full_mode = descriptor.render_json_canonical();
    assert!(
        full_mode.contains("\"predicate_pushdown_mode\":\"full\""),
        "missing pushdown mode `full` projection",
    );

    descriptor.predicate_pushdown = Some("index_predicate".to_string());
    descriptor.residual_predicate = Some(ExplainPredicate::True);
    let partial_mode = descriptor.render_json_canonical();
    assert!(
        partial_mode.contains("\"predicate_pushdown_mode\":\"partial\""),
        "missing pushdown mode `partial` projection",
    );
}
