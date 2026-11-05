use super::*;

#[derive(serde::Serialize)]
struct FrozenRowProjectionOutputWire {
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<OutputValue>>,
    row_count: u32,
}

#[test]
fn row_projection_output_preserves_the_frozen_wire_record_shape() {
    let current = RowProjectionOutput {
        entity: "example".to_string(),
        columns: vec!["id".to_string()],
        rows: Vec::new(),
        row_count: 0,
    };
    let frozen = FrozenRowProjectionOutputWire {
        entity: current.entity.clone(),
        columns: current.columns.clone(),
        rows: current.rows.clone(),
        row_count: current.row_count,
    };

    assert_eq!(current.len(), 0);
    assert!(current.is_empty());

    assert_eq!(
        icydb_schema::wire::encode_one(&current).expect("current row projection should encode"),
        icydb_schema::wire::encode_one(&frozen).expect("frozen row projection should encode"),
    );
}
