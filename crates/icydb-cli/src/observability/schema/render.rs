//! Module: schema report rendering.
//! Responsibility: render accepted schema reports into CLI tables.
//! Does not own: endpoint calls, candid decoding, or config surface gating.
//! Boundary: receives decoded schema descriptions and returns user-facing text.

use icydb::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationDescription,
    EntitySchemaDescription,
};

use crate::{
    observability::render::{render_field_list, yes_no},
    table::{ColumnAlign, append_indented_table},
};

type SchemaEntityRow = [String; 6];
type SchemaFieldRow = [String; 8];
type SchemaIndexRow = [String; 5];
type SchemaRelationRow = [String; 5];

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
        entity.fields().len().to_string(),
        entity.indexes().len().to_string(),
        entity.relations().len().to_string(),
        render_field_list(entity.primary_key_fields()),
        entity.entity_path().to_string(),
    ]
}

fn schema_field_row(entity_name: &str, field: &EntityFieldDescription) -> SchemaFieldRow {
    [
        entity_name.to_string(),
        field.name().to_string(),
        field
            .slot()
            .map_or_else(|| "-".to_string(), |slot| slot.to_string()),
        field.kind().to_string(),
        yes_no(field.nullable()).to_string(),
        yes_no(field.primary_key()).to_string(),
        yes_no(field.queryable()).to_string(),
        field.origin().to_string(),
    ]
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
        format!("{:?}", relation.enforcement()),
        format!("{:?}", relation.cardinality()),
    ]
}

fn append_schema_entity_table(output: &mut String, rows: &[SchemaEntityRow]) {
    append_schema_table(
        output,
        "entities",
        &[
            "entity",
            "fields",
            "indexes",
            "relations",
            "primary key",
            "path",
        ],
        rows,
        &[
            ColumnAlign::Left,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Right,
            ColumnAlign::Left,
            ColumnAlign::Left,
        ],
    );
}

fn append_schema_field_table(output: &mut String, rows: &[SchemaFieldRow]) {
    append_schema_table(
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
        ],
    );
}

fn append_schema_index_table(output: &mut String, rows: &[SchemaIndexRow]) {
    append_schema_table(
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
    append_schema_table(
        output,
        "relations",
        &["entity", "field", "target", "enforcement", "cardinality"],
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

fn append_schema_table<const N: usize>(
    output: &mut String,
    title: &str,
    headers: &[&str; N],
    rows: &[[String; N]],
    alignments: &[ColumnAlign; N],
) {
    output.push_str(title);
    output.push('\n');
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(output, "  ", headers, rows, alignments);
}
