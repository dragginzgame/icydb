use super::*;

#[test]
fn explain_execution_node_type_vocabulary_is_frozen() {
    let actual = [
        ExplainExecutionNodeType::ByKeyLookup.as_str(),
        ExplainExecutionNodeType::ByKeysLookup.as_str(),
        ExplainExecutionNodeType::PrimaryKeyRangeScan.as_str(),
        ExplainExecutionNodeType::IndexPrefixScan.as_str(),
        ExplainExecutionNodeType::IndexRangeScan.as_str(),
        ExplainExecutionNodeType::IndexMultiLookup.as_str(),
        ExplainExecutionNodeType::FullScan.as_str(),
        ExplainExecutionNodeType::Union.as_str(),
        ExplainExecutionNodeType::Intersection.as_str(),
        ExplainExecutionNodeType::IndexPredicatePrefilter.as_str(),
        ExplainExecutionNodeType::ResidualPredicateFilter.as_str(),
        ExplainExecutionNodeType::OrderByAccessSatisfied.as_str(),
        ExplainExecutionNodeType::OrderByMaterializedSort.as_str(),
        ExplainExecutionNodeType::DistinctPreOrdered.as_str(),
        ExplainExecutionNodeType::DistinctMaterialized.as_str(),
        ExplainExecutionNodeType::ProjectionMaterialized.as_str(),
        ExplainExecutionNodeType::CoveringRead.as_str(),
        ExplainExecutionNodeType::LimitOffset.as_str(),
        ExplainExecutionNodeType::CursorResume.as_str(),
        ExplainExecutionNodeType::IndexRangeLimitPushdown.as_str(),
        ExplainExecutionNodeType::TopNSeek.as_str(),
        ExplainExecutionNodeType::AggregateCount.as_str(),
        ExplainExecutionNodeType::AggregateExists.as_str(),
        ExplainExecutionNodeType::AggregateMin.as_str(),
        ExplainExecutionNodeType::AggregateMax.as_str(),
        ExplainExecutionNodeType::AggregateFirst.as_str(),
        ExplainExecutionNodeType::AggregateLast.as_str(),
        ExplainExecutionNodeType::AggregateSum.as_str(),
        ExplainExecutionNodeType::AggregateSeekFirst.as_str(),
        ExplainExecutionNodeType::AggregateSeekLast.as_str(),
        ExplainExecutionNodeType::GroupedAggregateHashMaterialized.as_str(),
        ExplainExecutionNodeType::GroupedAggregateOrderedMaterialized.as_str(),
        ExplainExecutionNodeType::SecondaryOrderPushdown.as_str(),
    ];
    let expected = [
        "ByKeyLookup",
        "ByKeysLookup",
        "PrimaryKeyRangeScan",
        "IndexPrefixScan",
        "IndexRangeScan",
        "IndexMultiLookup",
        "FullScan",
        "Union",
        "Intersection",
        "IndexPredicatePrefilter",
        "ResidualPredicateFilter",
        "OrderByAccessSatisfied",
        "OrderByMaterializedSort",
        "DistinctPreOrdered",
        "DistinctMaterialized",
        "ProjectionMaterialized",
        "CoveringRead",
        "LimitOffset",
        "CursorResume",
        "IndexRangeLimitPushdown",
        "TopNSeek",
        "AggregateCount",
        "AggregateExists",
        "AggregateMin",
        "AggregateMax",
        "AggregateFirst",
        "AggregateLast",
        "AggregateSum",
        "AggregateSeekFirst",
        "AggregateSeekLast",
        "GroupedAggregateHashMaterialized",
        "GroupedAggregateOrderedMaterialized",
        "SecondaryOrderPushdown",
    ];

    assert_eq!(
        actual, expected,
        "execution-node vocabulary drifted; node names are a stable EXPLAIN contract",
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

#[test]
fn execution_descriptor_canonical_json_shape_is_stable() {
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
    let expected = "{\"node_id\":0,\"node_type\":\"TopNSeek\",\"layer\":\"pipeline\",\"execution_mode\":\"Streaming\",\"execution_mode_detail\":\"streaming\",\"access_strategy\":{\"type\":\"FullScan\"},\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":\"index_only\",\"ordering_source\":\"AccessOrder\",\"limit\":3,\"cursor\":false,\"covering_scan\":true,\"rows_expected\":3,\"children\":[{\"node_id\":1,\"node_type\":\"LimitOffset\",\"layer\":\"terminal\",\"execution_mode\":\"Materialized\",\"execution_mode_detail\":\"materialized\",\"access_strategy\":null,\"predicate_pushdown_mode\":\"none\",\"predicate_pushdown\":null,\"fast_path_selected\":null,\"fast_path_reason\":null,\"residual_predicate\":null,\"projection\":null,\"ordering_source\":null,\"limit\":1,\"cursor\":null,\"covering_scan\":null,\"rows_expected\":null,\"children\":[],\"node_properties\":{}}],\"node_properties\":{}}";

    assert_eq!(
        json, expected,
        "canonical execution-node JSON shape drifted",
    );
}

#[test]
fn execution_descriptor_canonical_json_field_order_is_stable() {
    let descriptor = ExplainExecutionNodeDescriptor {
        node_type: ExplainExecutionNodeType::IndexPrefixScan,
        execution_mode: ExplainExecutionMode::Materialized,
        access_strategy: Some(ExplainAccessPath::IndexPrefix {
            name: "users_by_email",
            fields: vec!["email"],
            prefix_len: 1,
            values: vec![Value::Text("alpha@example.com".to_string())],
        }),
        predicate_pushdown: Some("strict_all_or_none".to_string()),
        residual_predicate: None,
        projection: None,
        ordering_source: Some(ExplainExecutionOrderingSource::AccessOrder),
        limit: Some(5),
        cursor: Some(true),
        covering_scan: Some(false),
        rows_expected: Some(5),
        children: Vec::new(),
        node_properties: ExplainPropertyMap::new(),
    };
    let json = descriptor.render_json_canonical();
    let ordered_fields = [
        "\"node_id\":",
        "\"node_type\":",
        "\"layer\":",
        "\"execution_mode\":",
        "\"execution_mode_detail\":",
        "\"access_strategy\":",
        "\"predicate_pushdown_mode\":",
        "\"predicate_pushdown\":",
        "\"fast_path_selected\":",
        "\"fast_path_reason\":",
        "\"residual_predicate\":",
        "\"projection\":",
        "\"ordering_source\":",
        "\"limit\":",
        "\"cursor\":",
        "\"covering_scan\":",
        "\"rows_expected\":",
        "\"children\":",
        "\"node_properties\":",
    ];

    let mut last_position = 0usize;
    for (index, field) in ordered_fields.iter().enumerate() {
        let position = json.find(field).unwrap_or_else(|| {
            panic!("canonical execution JSON missing expected field at index {index}: {field}")
        });
        if index > 0 {
            assert!(
                position > last_position,
                "canonical execution JSON field ordering drifted at field `{field}`",
            );
        }
        last_position = position;
    }
}

fn assert_execution_json_top_level_field_order(json: &str) {
    let ordered_fields = [
        "\"node_id\":",
        "\"node_type\":",
        "\"layer\":",
        "\"execution_mode\":",
        "\"execution_mode_detail\":",
        "\"access_strategy\":",
        "\"predicate_pushdown_mode\":",
        "\"predicate_pushdown\":",
        "\"fast_path_selected\":",
        "\"fast_path_reason\":",
        "\"residual_predicate\":",
        "\"projection\":",
        "\"ordering_source\":",
        "\"limit\":",
        "\"cursor\":",
        "\"covering_scan\":",
        "\"rows_expected\":",
        "\"children\":",
        "\"node_properties\":",
    ];

    let mut last_position = 0usize;
    for (index, field) in ordered_fields.iter().enumerate() {
        let position = json.find(field).unwrap_or_else(|| {
            panic!("canonical execution JSON missing expected field at index {index}: {field}")
        });
        if index > 0 {
            assert!(
                position > last_position,
                "canonical execution JSON field ordering drifted at field `{field}`",
            );
        }
        last_position = position;
    }
}

fn assert_execution_json_top_level_field_names_are_unique(json: &str) {
    let field_tokens = [
        "\"node_id\":",
        "\"node_type\":",
        "\"layer\":",
        "\"execution_mode\":",
        "\"execution_mode_detail\":",
        "\"access_strategy\":",
        "\"predicate_pushdown_mode\":",
        "\"predicate_pushdown\":",
        "\"fast_path_selected\":",
        "\"fast_path_reason\":",
        "\"residual_predicate\":",
        "\"projection\":",
        "\"ordering_source\":",
        "\"limit\":",
        "\"cursor\":",
        "\"covering_scan\":",
        "\"rows_expected\":",
        "\"children\":",
        "\"node_properties\":",
    ];

    for field_token in field_tokens {
        let occurrences = json.match_indices(field_token).count();
        assert_eq!(
            occurrences, 1,
            "canonical execution JSON field naming drifted: expected exactly one top-level `{field_token}` token"
        );
    }
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
        assert_execution_json_top_level_field_order(&json);
        assert_execution_json_top_level_field_names_are_unique(&json);
        assert!(
            json.contains(&format!("\"layer\":\"{expected_layer}\"")),
            "canonical execution JSON must expose stable layer ownership for each node family",
        );
    }
}

#[test]
fn execution_descriptor_canonical_json_missing_optional_fields_render_explicit_nulls() {
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
    let expected_null_fields = [
        "\"access_strategy\":null",
        "\"predicate_pushdown\":null",
        "\"fast_path_selected\":null",
        "\"fast_path_reason\":null",
        "\"residual_predicate\":null",
        "\"projection\":null",
        "\"ordering_source\":null",
        "\"limit\":null",
        "\"cursor\":null",
        "\"covering_scan\":null",
        "\"rows_expected\":null",
    ];
    for expected_null in expected_null_fields {
        assert!(
            json.contains(expected_null),
            "canonical execution JSON optional/null projection drifted: missing `{expected_null}`",
        );
    }
}

fn assert_execution_additive_metadata_parity(
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
        text.contains("node_id=0"),
        "text execution explain must expose deterministic node_id",
    );
    assert!(
        json.contains("\"node_id\":0"),
        "JSON execution explain must expose deterministic node_id",
    );
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
    } else {
        assert!(
            !text.contains("fast_path_selected="),
            "text execution explain must omit fast-path selection when absent",
        );
        assert!(
            json.contains("\"fast_path_selected\":null"),
            "JSON execution explain must project null fast-path selection when absent",
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
    } else {
        assert!(
            !text.contains("fast_path_reason="),
            "text execution explain must omit fast-path reason when absent",
        );
        assert!(
            json.contains("\"fast_path_reason\":null"),
            "JSON execution explain must project null fast-path reason when absent",
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
        assert_execution_additive_metadata_parity(
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
