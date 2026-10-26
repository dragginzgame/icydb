//! Module: schema report rendering.
//! Responsibility: render accepted schema reports into CLI tables.
//! Does not own: endpoint calls, Wire decoding, or endpoint publication.
//! Boundary: receives decoded schema descriptions and returns user-facing text.

use icydb::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationDescription,
    EntitySchemaDescription, encode_hex_lower,
};

use crate::{
    observability::render::{render_field_list, schema_field_cells, yes_no},
    table::{ColumnAlign, append_report_section},
};

type SchemaEntityRow = [String; 11];
type SchemaFieldRow = [String; 16];
type SchemaIndexRow = [String; 5];
type SchemaRelationRow = [String; 4];

pub(super) fn render_schema_report(report: &[EntitySchemaDescription]) -> String {
    let mut output = String::new();
    let entity_rows = report.iter().map(schema_entity_row).collect::<Vec<_>>();
    let field_rows = report
        .iter()
        .flat_map(|entity| {
            entity
                .fields()
                .iter()
                .map(|field| schema_field_row(entity.entity_name(), field))
        })
        .collect::<Vec<_>>();
    let index_rows = report
        .iter()
        .flat_map(|entity| {
            entity
                .indexes()
                .iter()
                .map(|index| schema_index_row(entity.entity_name(), index))
        })
        .collect::<Vec<_>>();
    let relation_rows = report
        .iter()
        .flat_map(|entity| {
            entity
                .relations()
                .iter()
                .map(|relation| schema_relation_row(entity.entity_name(), relation))
        })
        .collect::<Vec<_>>();

    output.push_str("IcyDB schema\n");
    output.push_str(
        format!(
            "  entities: {}\n  fields: {}\n  indexes: {}\n  relations: {}\n\n",
            report.len(),
            field_rows.len(),
            index_rows.len(),
            relation_rows.len(),
        )
        .as_str(),
    );
    append_schema_entity_table(&mut output, entity_rows.as_slice());
    output.push('\n');
    append_schema_field_table(&mut output, field_rows.as_slice());
    output.push('\n');
    append_schema_index_table(&mut output, index_rows.as_slice());
    output.push('\n');
    append_schema_relation_table(&mut output, relation_rows.as_slice());

    output
}

fn schema_entity_row(entity: &EntitySchemaDescription) -> SchemaEntityRow {
    [
        entity.entity_name().to_string(),
        entity.entity_tag().to_string(),
        entity.accepted_schema_fingerprint_method().to_string(),
        encode_hex_lower(&entity.accepted_schema_fingerprint()),
        entity.fields().len().to_string(),
        entity.indexes().len().to_string(),
        entity.relations().len().to_string(),
        render_field_list(entity.primary_key_fields()),
        entity.row_layout_current().to_string(),
        entity.row_layout_history_floor().to_string(),
        entity.entity_path().to_string(),
    ]
}

fn schema_field_row(entity_name: &str, field: &EntityFieldDescription) -> SchemaFieldRow {
    let mut row = std::array::from_fn(|_| String::new());
    row[0] = entity_name.to_string();
    for (cell, value) in row[1..].iter_mut().zip(schema_field_cells(field)) {
        *cell = value;
    }
    row
}

fn schema_index_row(entity_name: &str, index: &EntityIndexDescription) -> SchemaIndexRow {
    [
        entity_name.to_string(),
        index.name().to_string(),
        render_field_list(index.fields()),
        yes_no(index.unique()).to_string(),
        index.origin().to_string(),
    ]
}

fn schema_relation_row(
    entity_name: &str,
    relation: &EntityRelationDescription,
) -> SchemaRelationRow {
    [
        entity_name.to_string(),
        relation.field().to_string(),
        relation.target_entity_name().to_string(),
        format!("{:?}", relation.cardinality()),
    ]
}

fn append_schema_entity_table(output: &mut String, rows: &[SchemaEntityRow]) {
    append_report_section(
        output,
        "entities",
        &[
            "entity",
            "tag",
            "fingerprint method",
            "accepted fingerprint",
            "fields",
            "indexes",
            "relations",
            "primary key",
            "layout",
            "history floor",
            "path",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_field_table(output: &mut String, rows: &[SchemaFieldRow]) {
    append_report_section(
        output,
        "fields",
        &[
            "entity",
            "field",
            "slot",
            "type",
            "nullable",
            "pk",
            "queryable",
            "origin",
            "insert omission",
            "insert default",
            "default bytes",
            "default hash",
            "introduced layout",
            "historical fill",
            "fill bytes",
            "fill hash",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_index_table(output: &mut String, rows: &[SchemaIndexRow]) {
    append_report_section(
        output,
        "indexes",
        &["entity", "index", "fields", "unique", "origin"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_relation_table(output: &mut String, rows: &[SchemaRelationRow]) {
    append_report_section(
        output,
        "relations",
        &["entity", "field", "target", "cardinality"],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}
