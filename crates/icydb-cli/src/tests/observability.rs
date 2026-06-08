//! Module: Observability payload and rendering tests.
//! Responsibility: exercise metrics, schema, schema-check, and snapshot decoding/rendering.
//! Does not own: ICP process commands or SQL shell routing.
//! Boundary: test-only assertions over generated canister response shapes and reports.

mod schema_check;

use candid::Encode;
use icydb::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntityRelationStrength, EntitySchemaDescription,
};

use crate::observability::test_support::{
    decode_extended_metrics_report, decode_metrics_report, decode_metrics_reset_response,
    decode_schema_report, decode_snapshot_report, method_error, metrics_candid_arg,
    render_extended_metrics_report, render_field_list, render_metrics_report, render_schema_report,
    render_snapshot_report, yes_no,
};

#[test]
fn metrics_candid_arg_renders_optional_window() {
    assert_eq!(metrics_candid_arg(None), "(null)");
    assert_eq!(metrics_candid_arg(Some(123)), "(opt (123 : nat64))");
}

#[test]
fn decode_metrics_report_accepts_generated_response_shape() {
    let response: Result<icydb::metrics::CompactMetricsReport, icydb::Error> =
        Ok(icydb::metrics::CompactMetricsReport::default());
    let candid_bytes = Encode!(&response).expect("metrics response should encode");
    let decoded = decode_metrics_report(candid_bytes.as_slice())
        .expect("metrics response should decode")
        .expect("metrics response should be ok");

    assert_eq!(decoded.entity_counters().len(), 0);
    assert_eq!(decoded.requested_window_start_ms(), None);
}

#[test]
fn decode_extended_metrics_report_accepts_generated_response_shape() {
    let response: Result<icydb::metrics::EventReport, icydb::Error> =
        Ok(icydb::metrics::EventReport::default());
    let candid_bytes = Encode!(&response).expect("extended metrics response should encode");
    let decoded = decode_extended_metrics_report(candid_bytes.as_slice())
        .expect("extended metrics response should decode")
        .expect("extended metrics response should be ok");

    assert_eq!(decoded.entity_counters().len(), 0);
    assert_eq!(decoded.requested_window_start_ms(), None);
}

#[test]
fn decode_metrics_reset_response_accepts_generated_response_shape() {
    let response: Result<(), icydb::Error> = Ok(());
    let candid_bytes = Encode!(&response).expect("metrics reset response should encode");

    decode_metrics_reset_response(candid_bytes.as_slice())
        .expect("metrics reset response should decode")
        .expect("metrics reset response should be ok");
}

#[test]
fn decode_snapshot_report_accepts_generated_response_shape() {
    let response: Result<icydb::db::StorageReport, icydb::Error> =
        Ok(icydb::db::StorageReport::default());
    let candid_bytes = Encode!(&response).expect("snapshot response should encode");
    let decoded = decode_snapshot_report(candid_bytes.as_slice())
        .expect("snapshot response should decode")
        .expect("snapshot response should be ok");

    assert_eq!(decoded.storage_data().len(), 0);
    assert_eq!(decoded.storage_index().len(), 0);
}

#[test]
fn decode_schema_report_accepts_generated_response_shape() {
    let response: Result<Vec<EntitySchemaDescription>, icydb::Error> = Ok(Vec::new());
    let candid_bytes = Encode!(&response).expect("schema response should encode");
    let decoded = decode_schema_report(candid_bytes.as_slice())
        .expect("schema response should decode")
        .expect("schema response should be ok");

    assert_eq!(decoded.len(), 0);
}

#[test]
fn observability_render_helpers_format_common_values() {
    assert_eq!(render_field_list(&[]), "-");
    assert_eq!(
        render_field_list(&["id".to_string(), "tenant".to_string()]),
        "id, tenant",
    );
    assert_eq!(yes_no(true), "yes");
    assert_eq!(yes_no(false), "no");
}

#[test]
fn observability_call_errors_include_call_target_context() {
    assert_eq!(
        method_error(
            "schema check",
            "demo",
            "demo_rpg",
            "__icydb_schema_check",
            "schema drift",
        ),
        "IcyDB schema check method '__icydb_schema_check' failed on canister 'demo_rpg' in environment 'demo': schema drift",
    );
}

#[test]
fn snapshot_report_rendering_uses_human_tables() {
    let text = render_snapshot_report(&icydb::db::StorageReport::default());

    assert!(text.contains("IcyDB storage snapshot"));
    assert!(text.contains("data stores\n  None"));
    assert!(text.contains("index stores\n  None"));
    assert!(text.contains("schema stores\n  None"));
    assert!(text.contains("entities\n  None"));
}

#[test]
fn schema_report_rendering_uses_human_tables() {
    let text = render_schema_report(&[]);

    assert!(text.contains("IcyDB schema"));
    assert!(text.contains("entities: 0"));
    assert!(text.contains("fields: 0"));
    assert!(text.contains("indexes: 0"));
    assert!(text.contains("relations: 0"));
    assert!(text.contains("entities\n  None"));
    assert!(text.contains("fields\n  None"));
    assert!(text.contains("indexes\n  None"));
    assert!(text.contains("relations\n  None"));
}

#[test]
fn schema_report_renders_aligned_summary_and_index_tables() {
    let fields = (0..35)
        .map(|_| {
            EntityFieldDescription::new(
                "field".to_string(),
                None,
                "Text".to_string(),
                false,
                false,
                true,
                "generated".to_string(),
            )
        })
        .collect();
    let indexes = vec![
        EntityIndexDescription::new(
            "idx_character__name".to_string(),
            false,
            vec!["name".to_string()],
            "generated".to_string(),
        ),
        EntityIndexDescription::new(
            "character_level_idx".to_string(),
            false,
            vec!["level".to_string()],
            "ddl".to_string(),
        ),
    ];
    let report = [EntitySchemaDescription::new(
        "icydb_testing_demo_rpg_fixtures::schema::character::Character".to_string(),
        "Character".to_string(),
        "id".to_string(),
        fields,
        indexes,
        vec![EntityRelationDescription::new(
            "account_id".to_string(),
            "icydb_testing_demo_rpg_fixtures::schema::account::Account".to_string(),
            "Account".to_string(),
            "accounts".to_string(),
            EntityRelationStrength::Strong,
            EntityRelationCardinality::Single,
        )],
    )];
    let text = render_schema_report(&report);

    assert!(text.contains("entities: 1"));
    assert!(text.contains("fields: 35"));
    assert!(text.contains("indexes: 2"));
    assert!(text.contains("relations: 1"));
    assert!(text.contains("  entity      fields   indexes   relations   primary key   path\n"));
    assert!(
        text.lines().any(
            |line| line.starts_with("  ---------   ------   -------   ---------   -----------")
        )
    );
    assert!(text.contains(
        "  Character       35         2           1   id            icydb_testing_demo_rpg_fixtures::schema::character::Character\n"
    ));
    assert!(text.contains("fields\n"));
    assert!(
        text.contains("  entity      field   slot   type   nullable   pk   queryable   origin\n")
    );
    assert!(
        text.contains(
            "  Character   field      -   Text   no         no   yes         generated\n"
        )
    );
    assert!(text.contains("indexes\n"));
    assert!(text.contains("  entity      index                 fields   unique   origin\n"));
    assert!(text.contains("  Character   idx_character__name   name     no       generated\n"));
    assert!(text.contains("  Character   character_level_idx   level    no       ddl\n"));
    assert!(text.contains("relations\n"));
    assert!(text.contains("  entity      field        target    strength   cardinality\n"));
    assert!(text.contains("  Character   account_id   Account   Strong     Single\n"));
}

#[test]
fn schema_report_renders_composite_primary_key_fields() {
    let report = [EntitySchemaDescription::new_with_primary_key_fields(
        "demo::Placement".to_string(),
        "Placement".to_string(),
        "tenant_id, local_id".to_string(),
        vec!["tenant_id".to_string(), "local_id".to_string()],
        vec![
            EntityFieldDescription::new(
                "tenant_id".to_string(),
                Some(0),
                "nat".to_string(),
                false,
                true,
                true,
                "generated".to_string(),
            ),
            EntityFieldDescription::new(
                "local_id".to_string(),
                Some(1),
                "ulid".to_string(),
                false,
                true,
                true,
                "generated".to_string(),
            ),
        ],
        Vec::new(),
        Vec::new(),
    )];
    let text = render_schema_report(&report);

    assert!(text.contains("Placement"));
    assert!(text.contains("tenant_id, local_id"));
    assert!(text.lines().any(|line| {
        line.contains("Placement")
            && line.contains("tenant_id, local_id")
            && line.contains("demo::Placement")
    }));
}

#[test]
fn metrics_report_rendering_uses_human_summary() {
    let text = render_metrics_report(&icydb::metrics::CompactMetricsReport::default());

    assert!(text.contains("IcyDB metrics"));
    assert!(text.contains("requested window start ms: none"));
    assert!(text.contains("counters: none"));
    assert!(text.contains("entities\n  None"));
}

#[test]
fn extended_metrics_report_rendering_uses_human_summary() {
    let text = render_extended_metrics_report(&icydb::metrics::EventReport::default());

    assert!(text.contains("IcyDB metrics"));
    assert!(text.contains("requested window start ms: none"));
    assert!(text.contains("counters: none"));
    assert!(text.contains("entities\n  None"));
}
