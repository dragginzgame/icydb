//! Module: Observability payload and rendering tests.
//! Responsibility: exercise metrics, schema, and snapshot decoding/rendering.
//! Does not own: ICP process commands or SQL shell routing.
//! Boundary: test-only assertions over generated canister response shapes and reports.

use crate::{icp::IcpCallKind, observability::decode_endpoint_result};

use icydb::db::{
    EntityFieldDescription, EntityIndexDescription, EntityRelationCardinality,
    EntityRelationDescription, EntitySchemaDescription,
};

use crate::observability::test_support::{
    method_error, render_field_list, render_metrics_report, render_schema_report,
    render_snapshot_report, yes_no,
};

#[test]
fn decode_metrics_report_accepts_generated_response_shape() {
    let response: Result<icydb::metrics::MetricsReport, icydb::Error> =
        Ok(icydb::metrics::MetricsReport::default());
    let wire_bytes = canic_icydb::wire::Endpoint::Metrics
        .encode_response(&response, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .expect("metrics response should encode");
    let decoded = decode_endpoint_result::<icydb::metrics::MetricsReport>(
        wire_bytes.as_slice(),
        "metrics",
        "demo",
        "demo_rpg",
        canic_icydb::wire::Endpoint::Metrics,
        IcpCallKind::Query,
    )
    .expect("metrics response should be ok");

    assert!(decoded.entities().is_empty());
}

#[test]
fn decode_metrics_reset_response_accepts_generated_response_shape() {
    let response: Result<(), icydb::Error> = Ok(());
    let wire_bytes = canic_icydb::wire::Endpoint::MetricsReset
        .encode_response(&response, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .expect("metrics reset response should encode");

    decode_endpoint_result::<()>(
        wire_bytes.as_slice(),
        "metrics reset",
        "demo",
        "demo_rpg",
        canic_icydb::wire::Endpoint::MetricsReset,
        IcpCallKind::Update,
    )
    .expect("metrics reset response should be ok");
}

#[test]
fn decode_snapshot_report_accepts_generated_response_shape() {
    let response: Result<icydb::db::StorageReport, icydb::Error> =
        Ok(icydb::db::StorageReport::default());
    let wire_bytes = canic_icydb::wire::Endpoint::Snapshot
        .encode_response(&response, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .expect("snapshot response should encode");
    let decoded = decode_endpoint_result::<icydb::db::StorageReport>(
        wire_bytes.as_slice(),
        "snapshot",
        "demo",
        "demo_rpg",
        canic_icydb::wire::Endpoint::Snapshot,
        IcpCallKind::Query,
    )
    .expect("snapshot response should be ok");

    assert_eq!(decoded.storage_data().len(), 0);
    assert_eq!(decoded.storage_index().len(), 0);
}

#[test]
fn decode_schema_report_accepts_generated_response_shape() {
    let response: Result<Vec<EntitySchemaDescription>, icydb::Error> = Ok(Vec::new());
    let wire_bytes = canic_icydb::wire::Endpoint::Schema
        .encode_response(&response, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .expect("schema response should encode");
    let decoded = decode_endpoint_result::<Vec<EntitySchemaDescription>>(
        wire_bytes.as_slice(),
        "schema",
        "demo",
        "demo_rpg",
        canic_icydb::wire::Endpoint::Schema,
        IcpCallKind::Query,
    )
    .expect("schema response should be ok");

    assert_eq!(decoded.len(), 0);
}

#[test]
fn endpoint_decoder_rejects_malformed_wrong_shape_and_application_errors() {
    let wrong_shape = canic_icydb::wire::Endpoint::Snapshot
        .encode_response(42_u64, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .unwrap();
    let response: Result<icydb::db::StorageReport, icydb::Error> =
        Err(icydb::Error::from_runtime_boundary(
            icydb::diagnostic::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
            icydb::ErrorOrigin::Runtime,
        ));
    let application_error = canic_icydb::wire::Endpoint::Snapshot
        .encode_response(&response, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .unwrap();
    let valid: Result<icydb::db::StorageReport, icydb::Error> =
        Ok(icydb::db::StorageReport::default());
    let wrong_endpoint = canic_icydb::wire::Endpoint::Metrics
        .encode_response(&valid, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
        .unwrap();
    let wrong_direction = canic_icydb::wire::Endpoint::Snapshot
        .encode_request(&valid, icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES))
        .unwrap();
    for bytes in [
        b"not Wire".as_slice(),
        &wrong_shape,
        &application_error,
        &wrong_endpoint,
        &wrong_direction,
    ] {
        assert!(
            decode_endpoint_result::<icydb::db::StorageReport>(
                bytes,
                "snapshot",
                "demo",
                "demo_rpg",
                canic_icydb::wire::Endpoint::Snapshot,
                IcpCallKind::Query
            )
            .is_err()
        );
    }
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
            "schema",
            "demo",
            "demo_rpg",
            "icydb_schema",
            "schema unavailable",
        ),
        "IcyDB schema method 'icydb_schema' failed on canister 'demo_rpg' in environment 'demo': schema unavailable",
    );
}

#[test]
fn snapshot_report_rendering_uses_human_tables() {
    let text = render_snapshot_report(&icydb::db::StorageReport::default());

    assert!(text.contains("IcyDB storage snapshot"));
    assert!(text.contains("unavailable: no default memory runtime"));
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
                None,
                None,
                None,
                None,
                None,
                None,
                None,
                None,
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
        7,
        1,
        [0x11; 16],
        "id".to_string(),
        vec!["id".to_string()],
        fields,
        indexes,
        vec![EntityRelationDescription::new(
            "account_id".to_string(),
            "icydb_testing_demo_rpg_fixtures::schema::account::Account".to_string(),
            "Account".to_string(),
            "accounts".to_string(),
            EntityRelationCardinality::Single,
        )],
        Vec::new(),
        1,
        1,
    )];
    let text = render_schema_report(&report);

    assert!(text.contains("entities: 1"));
    assert!(text.contains("fields: 35"));
    assert!(text.contains("indexes: 2"));
    assert!(text.contains("relations: 1"));
    assert!(text.contains("fingerprint method"));
    assert!(text.contains("accepted fingerprint"));
    assert!(text.contains("11111111111111111111111111111111"));
    assert!(text.lines().any(|line| line.starts_with("  ---------")));
    assert!(text.lines().any(|line| {
        line.contains("Character")
            && line.contains("35")
            && line.contains("icydb_testing_demo_rpg_fixtures::schema::character::Character")
    }));
    assert!(text.contains("fields\n"));
    assert!(text.contains("insert omission"));
    assert!(text.contains("insert default"));
    assert!(text.contains("introduced layout"));
    assert!(text.contains("historical fill"));
    assert!(text.lines().any(|line| {
        line.contains("Character") && line.contains("field") && line.contains("generated")
    }));
    assert!(text.contains("indexes\n"));
    assert!(text.contains("  entity      index                 fields   unique   origin\n"));
    assert!(text.contains("  Character   idx_character__name   name     no       generated\n"));
    assert!(text.contains("  Character   character_level_idx   level    no       ddl\n"));
    assert!(text.contains("relations\n"));
    assert!(text.contains("  entity      field        target    cardinality\n"));
    assert!(text.contains("  Character   account_id   Account   Single\n"));
}

#[test]
fn schema_report_renders_composite_primary_key_fields() {
    let report = [EntitySchemaDescription::new(
        "demo::Placement".to_string(),
        "Placement".to_string(),
        8,
        1,
        [0x22; 16],
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
                Some("required".to_string()),
                None,
                None,
                None,
                Some(1),
                Some("reject".to_string()),
                None,
                None,
            ),
            EntityFieldDescription::new(
                "local_id".to_string(),
                Some(1),
                "ulid".to_string(),
                false,
                true,
                true,
                "generated".to_string(),
                Some("required".to_string()),
                None,
                None,
                None,
                Some(1),
                Some("reject".to_string()),
                None,
                None,
            ),
        ],
        Vec::new(),
        Vec::new(),
        Vec::new(),
        1,
        1,
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
    let text = render_metrics_report(&icydb::metrics::MetricsReport::default());

    assert!(text.contains("IcyDB metrics"));
    assert!(text.contains("entities: 0 of 0 (bounded path prefix, sorted by cost)"));
    assert!(text.contains("heap-local window ID: unavailable"));
    assert!(text.contains("canonical debt: 0 batches, 0 records, 0 encoded batch bytes"));
    assert!(text.contains("window appended: 0 batches, 0 records, 0 encoded batch bytes"));
    assert!(text.contains("window retired: 0 batches, 0 records, 0 encoded batch bytes"));
    assert!(text.contains("movement overflowed: false"));
    assert!(text.contains("instruction spans may nest; totals are not additive"));
    assert!(text.contains("entities\n  None"));
}

#[test]
fn schema_report_keeps_unicode_columns_aligned_and_controls_on_one_line() {
    let report = ["界", "e\u{301}", "line\n\x1b[31m\u{009b}\u{202e}\u{2028}"].map(|name| {
        EntitySchemaDescription::new(
            "demo::Entity".into(),
            name.into(),
            8,
            1,
            [0x22; 16],
            "id".into(),
            vec!["id".into()],
            Vec::new(),
            Vec::new(),
            Vec::new(),
            Vec::new(),
            1,
            1,
        )
    });
    let text = render_schema_report(&report);
    let wide = text.lines().find(|line| line.starts_with("  界")).unwrap();
    let combining = text
        .lines()
        .find(|line| line.starts_with("  e\u{301}"))
        .unwrap();
    // CJK occupies two terminal columns; an accented combining sequence one.
    // The following numeric column must start at the same display position.
    let wide_padding = wide
        .strip_prefix("  界")
        .unwrap()
        .chars()
        .take_while(|ch| *ch == ' ')
        .count();
    let combining_padding = combining
        .strip_prefix("  e\u{301}")
        .unwrap()
        .chars()
        .take_while(|ch| *ch == ' ')
        .count();
    assert_eq!(combining_padding, wide_padding + 1);
    assert!(text.contains("line\\n\\u{1b}[31m\\u{9b}\\u{202e}\\u{2028}"));
    assert!(!text.contains(['\x1b', '\u{009b}', '\u{202e}', '\u{2028}']));
    assert_eq!(
        text.lines()
            .filter(|line| line.contains("demo::Entity"))
            .count(),
        3
    );
}
