//! Module: schema report command handling.
//! Responsibility: call the generated accepted-schema endpoint and render schema reports.
//! Does not own: schema reconciliation, config surface gating, or generic ICP command construction.
//! Boundary: exposes the schema show command and test-covered report rendering through observability.

use candid::Decode;
use icydb::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationDescription,
    EntitySchemaDescription,
};

use crate::{
    cli::CanisterTarget,
    config::{SCHEMA_ENDPOINT, require_configured_endpoint},
    icp::require_created_canister,
    table::{ColumnAlign, append_indented_table},
};

use super::{
    call_query,
    render::{render_field_list, yes_no},
};

type SchemaEntityRow = [String; 6];
type SchemaFieldRow = [String; 8];
type SchemaIndexRow = [String; 5];
type SchemaRelationRow = [String; 5];

/// Read and print the generated accepted-schema endpoint.
pub(super) fn run_schema_show_command(target: CanisterTarget) -> Result<(), String> {
    require_configured_endpoint(target.canister_name(), SCHEMA_ENDPOINT)?;
    require_created_canister(target.environment(), target.canister_name())?;
    let candid_bytes = call_query(
        target.environment(),
        target.canister_name(),
        SCHEMA_ENDPOINT.method(),
        "()",
    )?;
    let response = Decode!(
        candid_bytes.as_slice(),
        Result<Vec<icydb::db::EntitySchemaDescription>, icydb::Error>
    )
    .map_err(|err| err.to_string())?;

    match response {
        Ok(report) => {
            print!("{}", render_schema_report(report.as_slice()));

            Ok(())
        }
        Err(err) => Err(format!(
            "IcyDB schema method '{}' failed on canister '{}' in environment '{}': {err}",
            SCHEMA_ENDPOINT.method(),
            target.canister_name(),
            target.environment(),
        )),
    }
}

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
        entity.primary_key().to_string(),
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
        format!("{:?}", relation.strength()),
        format!("{:?}", relation.cardinality()),
    ]
}

fn append_schema_entity_table(output: &mut String, rows: &[SchemaEntityRow]) {
    output.push_str("entities\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
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
    output.push_str("fields\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
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
    output.push_str("indexes\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
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
    output.push_str("relations\n");
    if rows.is_empty() {
        output.push_str("  None\n");
        return;
    }

    append_indented_table(
        output,
        "  ",
        &["entity", "field", "target", "strength", "cardinality"],
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
