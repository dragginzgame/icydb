use super::{GroupedQueryOutput, GroupedRow};
use crate::value::OutputValue;

#[derive(serde::Serialize)]
struct FrozenGroupedRowWire {
    group_key: Vec<OutputValue>,
    aggregate_values: Vec<OutputValue>,
}

#[derive(serde::Serialize)]
struct FrozenGroupedQueryOutputWire {
    entity: String,
    rows: Vec<FrozenGroupedRowWire>,
    row_count: u32,
    next_cursor: Option<String>,
}

#[test]
fn grouped_query_output_preserves_its_initial_wire_record_shape() {
    let current = GroupedQueryOutput {
        entity: "Example".to_string(),
        rows: vec![GroupedRow::new(
            [OutputValue::nat64(7)],
            [OutputValue::nat64(1)],
        )],
        row_count: 1,
        next_cursor: Some("abcd".to_string()),
    };
    let frozen = FrozenGroupedQueryOutputWire {
        entity: current.entity.clone(),
        rows: vec![FrozenGroupedRowWire {
            group_key: vec![OutputValue::nat64(7)],
            aggregate_values: vec![OutputValue::nat64(1)],
        }],
        row_count: current.row_count,
        next_cursor: current.next_cursor.clone(),
    };

    assert_eq!(
        icydb_schema::wire::encode_one(&current).expect("current grouped output should encode"),
        icydb_schema::wire::encode_one(&frozen).expect("frozen grouped output should encode"),
    );
}
