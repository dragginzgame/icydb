use super::{LiteralSlots, normalize_select_value, projection_alias_slots};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[test]
fn canonical_field_slots_preserve_reference_topology_across_local_id_changes() {
    let repeated_a = json!({
        "projections": [
            {"alias": null, "expression": {"field_id": "field:0000000a"}},
            {"alias": null, "expression": {"field_id": "field:0000000a"}}
        ]
    });
    let repeated_b = json!({
        "projections": [
            {"alias": null, "expression": {"field_id": "field:00000384"}},
            {"alias": null, "expression": {"field_id": "field:00000384"}}
        ]
    });
    let distinct = json!({
        "projections": [
            {"alias": null, "expression": {"field_id": "field:0000000a"}},
            {"alias": null, "expression": {"field_id": "field:00000014"}}
        ]
    });
    let fields_a = BTreeMap::from([(10, "field:0".to_string()), (20, "field:1".to_string())]);
    let fields_b = BTreeMap::from([(900, "field:0".to_string()), (901, "field:1".to_string())]);

    assert_eq!(
        normalize(repeated_a, &fields_a),
        normalize(repeated_b, &fields_b),
    );
    assert_ne!(
        normalize(
            json!({
                "projections": [
                    {"alias": null, "expression": {"field_id": "field:0000000a"}},
                    {"alias": null, "expression": {"field_id": "field:0000000a"}}
                ]
            }),
            &fields_a,
        ),
        normalize(distinct, &fields_a),
    );
}

#[test]
fn alias_names_are_erased_while_alias_binding_topology_remains() {
    let fields = BTreeMap::new();
    let first = json!({
        "projections": [
            {"alias": "first_name", "expression": {"node": "literal", "value": "alpha"}},
            {"alias": "second_name", "expression": {"node": "literal", "value": "beta"}}
        ],
        "order": [{"target": {"alias": "first_name"}}]
    });
    let renamed = json!({
        "projections": [
            {"alias": "x", "expression": {"node": "literal", "value": "alpha"}},
            {"alias": "y", "expression": {"node": "literal", "value": "beta"}}
        ],
        "order": [{"target": {"alias": "x"}}]
    });
    let rebound = json!({
        "projections": [
            {"alias": "x", "expression": {"node": "literal", "value": "alpha"}},
            {"alias": "y", "expression": {"node": "literal", "value": "beta"}}
        ],
        "order": [{"target": {"alias": "y"}}]
    });

    assert_eq!(normalize(first, &fields), normalize(renamed, &fields),);
    assert_ne!(
        normalize(
            json!({
                "projections": [
                    {"alias": "x", "expression": {"node": "literal", "value": "alpha"}},
                    {"alias": "y", "expression": {"node": "literal", "value": "beta"}}
                ],
                "order": [{"target": {"alias": "x"}}]
            }),
            &fields,
        ),
        normalize(rebound, &fields),
    );
}

#[test]
fn numeric_literal_slots_follow_numeric_not_lexical_order() {
    let fields = BTreeMap::new();
    let low_high = json!({
        "projections": [],
        "predicate": ["i64:2", "i64:10"]
    });
    let shifted = json!({
        "projections": [],
        "predicate": ["i64:20", "i64:30"]
    });

    assert_eq!(normalize(low_high, &fields), normalize(shifted, &fields),);

    let unsigned_low_high = json!({
        "projections": [],
        "predicate": ["u64:0000000000000002", "u64:0000000000000010"]
    });
    let unsigned_shifted = json!({
        "projections": [],
        "predicate": ["u64:0000000000000020", "u64:0000000000000030"]
    });
    assert_eq!(
        normalize(unsigned_low_high, &fields),
        normalize(unsigned_shifted, &fields),
    );
}

#[test]
fn typed_null_kind_remains_structurally_significant() {
    let fields = BTreeMap::new();
    let integer = json!({
        "projections": [],
        "expression": {"kind": "null", "value": "integer"}
    });
    let text = json!({
        "projections": [],
        "expression": {"kind": "null", "value": "text"}
    });

    assert_ne!(normalize(integer, &fields), normalize(text, &fields));
}

#[test]
fn text_literal_classes_preserve_empty_and_prefix_roles() {
    let fields = BTreeMap::new();
    let empty = json!({
        "projections": [],
        "expression": {"kind": "text", "value": ""}
    });
    let ordinary = json!({
        "projections": [],
        "expression": {"kind": "text", "value": "alpha"}
    });
    let prefix = json!({
        "projections": [],
        "predicate": {"prefix": "alpha"}
    });

    assert_ne!(
        normalize(empty, &fields),
        normalize(ordinary.clone(), &fields)
    );
    assert_ne!(normalize(ordinary, &fields), normalize(prefix, &fields));
}

fn normalize(value: Value, fields: &BTreeMap<u32, String>) -> Value {
    let aliases = projection_alias_slots(&value).expect("test aliases should derive");
    let literals = LiteralSlots::from_value(&value).expect("test literals should derive");
    normalize_select_value(value, fields, &aliases, &literals, None)
        .expect("test structural value should normalize")
}
