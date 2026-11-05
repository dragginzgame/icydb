use super::{
    MAX_DIAGNOSTIC_ERROR_BYTES, RawDiagnosticFact, artifact::DiagnosticSchemaArtifact,
    diagnostic_command_report, diagnostic_fact_schema_mismatch, parse_error_code, read_error_json,
    render_error, render_error_code_report, render_error_code_report_with_facts, render_fact_value,
};
use crate::cli::{CliArgs, CliCommand};
use clap::Parser;
use std::io::{Cursor, Write as _};

#[test]
fn typed_adapter_public_errors_render_from_codes_and_wire_json() {
    use icydb::db::TypedAdapterError;

    for (cause, label) in [
        (TypedAdapterError::BatchHandleMismatch, "typed batch handle"),
        (TypedAdapterError::BatchRowConsumed, "typed batch row"),
        (TypedAdapterError::EntityMismatch, "typed row belongs"),
        (TypedAdapterError::FieldUnavailable, "typed field source"),
        (
            TypedAdapterError::IncompatibleField,
            "generated field contract",
        ),
        (
            TypedAdapterError::RowFieldUnavailable,
            "typed output projection",
        ),
        (TypedAdapterError::RowShapeMismatch, "typed output column"),
        (TypedAdapterError::StaleBinding, "typed binding is stale"),
        (TypedAdapterError::ValueShapeMismatch, "typed output value"),
    ] {
        let error = icydb::Error::from(cause);
        let json = serde_json::to_vec(&error).unwrap();
        let decoded = read_error_json(json.as_slice()).unwrap();
        assert_eq!(decoded, error);
        let reason = render_error(&decoded);
        // Rendering retains the existing broad diagnostic label before
        // the specific adapter reason.
        assert!(reason.contains(label), "{cause:?}: {reason}");
        let report = render_error_code_report(error.code().raw());
        assert!(report.contains(&reason));
        assert_eq!(
            diagnostic_fact_schema_mismatch(error.code().raw(), &[]),
            None
        );
    }
    // BindingUnavailable carries an opaque producer-owned context; qualify
    // its registered projection without constructing one in host tooling.
    let binding = icydb::Error::from_error_code(
        icydb::ErrorCode::RUNTIME_BOUNDARY_TYPED_ADAPTER_BINDING_UNAVAILABLE,
        icydb::ErrorOrigin::Interface,
    );
    assert!(render_error(&binding).contains(": generated source descriptor"));
    assert_eq!(
        read_error_json(serde_json::to_vec(&binding).unwrap().as_slice()).unwrap(),
        binding
    );
}

#[test]
fn oversized_row_json_preserves_limit_facts_without_schema_metadata() {
    use icydb::diagnostic::{DiagnosticFactTag, ErrorClass, ErrorOrigin};

    let input = serde_json::to_vec(&serde_json::json!({
        "code": icydb::ErrorCode::RUNTIME_UNSUPPORTED.raw(),
        "class": ErrorClass::Unsupported.wire_code(),
        "origin": ErrorOrigin::Serialize.wire_code(),
        "facts": [
            { "tag": DiagnosticFactTag::ActualLength.raw(), "value": 4_194_305 },
            { "tag": DiagnosticFactTag::Limit.raw(), "value": 4_194_304 },
        ],
    }))
    .unwrap();
    let error = read_error_json(input.as_slice()).unwrap();
    assert_eq!(error.class(), ErrorClass::Unsupported);
    assert_eq!(error.origin(), icydb::ErrorOrigin::Serialize);
    assert_eq!(error.facts().len(), 2);
    assert_eq!(error.facts()[0].value(), 4_194_305);
    assert_eq!(error.facts()[1].value(), 4_194_304);
    let facts = vec![
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ActualLength.raw(),
            value: 4_194_305,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::Limit.raw(),
            value: 4_194_304,
        },
    ];
    assert_eq!(diagnostic_fact_schema_mismatch(22, &facts), None);
    let mut notes = Vec::new();
    render_error_code_report_with_facts(22, &facts, &[], &mut notes);
}

#[test]
fn error_json_preserves_public_payload_and_large_numeric_values() {
    let input = serde_json::to_vec(&serde_json::json!({
        "code": u16::MAX, "class": 0, "origin": 0,
        "facts": [{ "tag": 255, "value": u64::MAX }], "query_field": null,
    }))
    .unwrap();
    let error = read_error_json(input.as_slice()).expect("public payload");
    assert_eq!(error.code().raw(), u16::MAX);
    assert_eq!(error.facts()[0].tag(), 255);
    assert_eq!(error.facts()[0].value(), u64::MAX);
    assert!(error.query_field().is_none());
}

#[test]
fn error_json_bounds_reads_and_accepts_the_exact_byte_limit() {
    let mut input = br#"{"code":7,"class":0,"origin":0,"facts":[],"query_field":null}"#.to_vec();
    input.resize(MAX_DIAGNOSTIC_ERROR_BYTES, b' ');
    assert!(read_error_json(input.as_slice()).is_ok());
    input.extend_from_slice(&[b' '; 32]);
    let mut reader = Cursor::new(input);
    assert!(read_error_json(&mut reader).is_err());
    assert_eq!(reader.position(), (MAX_DIAGNOSTIC_ERROR_BYTES + 1) as u64);
}

#[test]
fn error_json_rejects_malformed_wrong_shape_and_trailing_data() {
    for input in [
        "",
        "   ",
        "{",
        "[]",
        "{}",
        "[7,0,0,[],null]",
        r#"{"Err":{"code":7,"class":0,"origin":0,"facts":[]}}"#,
        r#"{"code":7,"class":0,"origin":0,"facts":"invalid"}"#,
        r#"{"code":7,"class":0,"origin":0,"facts":[]} {}"#,
    ] {
        assert!(read_error_json(input.as_bytes()).is_err());
    }
}

#[test]
fn error_json_retries_interrupted_reads_and_preserves_io_failures() {
    struct Reader<'a> {
        bytes: &'a [u8],
        error: Option<std::io::ErrorKind>,
    }

    impl std::io::Read for Reader<'_> {
        fn read(&mut self, buffer: &mut [u8]) -> std::io::Result<usize> {
            if let Some(kind) = self.error.take() {
                return Err(std::io::Error::from(kind));
            }
            self.bytes.read(buffer)
        }
    }

    for kind in [std::io::ErrorKind::Interrupted, std::io::ErrorKind::Other] {
        let result = read_error_json(Reader {
            bytes: br#"{"code":7,"class":0,"origin":0,"facts":[],"query_field":null}"#,
            error: Some(kind),
        });
        assert_eq!(result.is_ok(), kind == std::io::ErrorKind::Interrupted);
    }
}

#[test]
fn diagnostic_json_file_reaches_the_shared_context_renderer() {
    let input = serde_json::json!({
        "code": icydb::ErrorCode::QUERY_PLAN.raw(),
        "class": icydb::ErrorCode::QUERY_PLAN.class().wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Query.wire_code(),
        "facts": [{ "tag": icydb::diagnostic::DiagnosticFactTag::TermIndex.raw(), "value": 1 }],
        "query_field": { "role": icydb::diagnostic::QueryFieldRole::OrderBy.raw(), "field": "line\n\u{1b}`" },
    });
    let bytes = serde_json::to_vec(&input).unwrap();
    let expected = read_error_json(bytes.as_slice()).unwrap();
    let root = ic_host_fs::testing::create_fixture_directory(
        &std::env::temp_dir(),
        "icydb-diagnostic-input",
    )
    .unwrap();
    let path = root.join("input.json");
    let mut file = std::fs::File::create_new(&path).expect("new test input");
    file.write_all(&bytes).unwrap();
    drop(file);
    let args = CliArgs::try_parse_from([
        "icydb",
        "diagnostic",
        "--error-json",
        path.to_str().unwrap(),
    ])
    .unwrap();
    let CliCommand::Diagnostic(args) = args.into_command() else {
        panic!("diagnostic command")
    };
    let report = diagnostic_command_report(&args);
    std::fs::remove_dir_all(root).expect("remove this test's input");
    let report = report.expect("structured report");
    assert!(report.ends_with(&format!("payload: {}", render_error(&expected))));
    assert!(!report.contains('\u{1b}'));
    assert!(expected.validated_query_field().unwrap().is_some());
}

fn decoded_query_field_error(
    code: icydb::ErrorCode,
    role: u8,
    field: &str,
    facts: serde_json::Value,
) -> icydb::Error {
    serde_json::from_value(serde_json::json!({
        "code": code.raw(),
        "class": code.class().wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Query.wire_code(),
        "facts": facts,
        "query_field": {
            "role": role,
            "field": field,
        },
    }))
    .expect("query-field error should decode")
}

#[test]
fn renders_every_valid_query_field_role_from_shared_typed_context() {
    let cases = [
        (icydb::diagnostic::QueryFieldRole::Predicate, "predicate"),
        (icydb::diagnostic::QueryFieldRole::Projection, "projection"),
        (icydb::diagnostic::QueryFieldRole::GroupBy, "group_by"),
        (icydb::diagnostic::QueryFieldRole::Having, "having"),
        (icydb::diagnostic::QueryFieldRole::OrderBy, "order_by"),
        (
            icydb::diagnostic::QueryFieldRole::AggregateTarget,
            "aggregate_target",
        ),
    ];

    for (role, label) in cases {
        let error = decoded_query_field_error(
            icydb::ErrorCode::QUERY_PLAN,
            role.raw(),
            "missing",
            serde_json::json!([]),
        );

        assert_eq!(
            render_error(&error),
            format!("E_QUERY_PLAN: query planning failed; {label} field `missing`")
        );
    }
}

#[test]
fn renders_reference_order_field_with_its_numeric_term_position() {
    let error = decoded_query_field_error(
        icydb::ErrorCode::QUERY_PLAN,
        icydb::diagnostic::QueryFieldRole::OrderBy.raw(),
        "id",
        serde_json::json!([{
            "tag": icydb::diagnostic::DiagnosticFactTag::TermIndex.raw(),
            "value": 1,
        }]),
    );

    assert_eq!(
        render_error(&error),
        "E_QUERY_PLAN: query planning failed; order_by field `id`; facts term_index=1"
    );
}

#[test]
fn query_field_rendering_escapes_terminal_control_and_delimiter_characters() {
    let error = decoded_query_field_error(
        icydb::ErrorCode::QUERY_PLAN,
        icydb::diagnostic::QueryFieldRole::Projection.raw(),
        "line\n\0`\"'\u{1b}\\\u{202e}",
        serde_json::json!([]),
    );
    let rendered = render_error(&error);

    assert_eq!(
        rendered,
        "E_QUERY_PLAN: query planning failed; projection field `line\\n\\u{0}\\`\\\"\\'\\u{1b}\\\\\\u{202e}`"
    );
    assert!(!rendered.contains('\n'));
    assert!(!rendered.contains('\0'));
    assert!(!rendered.contains('\u{1b}'));
}

#[test]
fn malformed_decoded_query_field_context_is_withheld_and_reported() {
    let overbound = "x".repeat(icydb::diagnostic::MAX_PUBLIC_QUERY_FIELD_BYTES + 1);
    let cases = [
        (
            decoded_query_field_error(
                icydb::ErrorCode::QUERY_PLAN,
                0,
                "unknown-role-secret",
                serde_json::json!([]),
            ),
            "unknown role",
            "unknown-role-secret",
        ),
        (
            decoded_query_field_error(
                icydb::ErrorCode::QUERY_VALIDATE,
                icydb::diagnostic::QueryFieldRole::Predicate.raw(),
                "wrong-code-secret",
                serde_json::json!([]),
            ),
            "role is not allowed for this E-code",
            "wrong-code-secret",
        ),
        (
            decoded_query_field_error(
                icydb::ErrorCode::QUERY_PLAN,
                icydb::diagnostic::QueryFieldRole::OrderBy.raw(),
                overbound.as_str(),
                serde_json::json!([]),
            ),
            "field exceeds the 256-byte bound",
            overbound.as_str(),
        ),
    ];

    for (error, mismatch, untrusted_field) in cases {
        let rendered = render_error(&error);
        assert!(
            rendered.contains(format!("query field context mismatch: {mismatch}").as_str()),
            "{rendered}"
        );
        assert!(!rendered.contains(untrusted_field), "{rendered}");
    }
}

#[test]
fn invalid_numeric_facts_withhold_otherwise_valid_query_field_context() {
    let error = decoded_query_field_error(
        icydb::ErrorCode::QUERY_PLAN,
        icydb::diagnostic::QueryFieldRole::OrderBy.raw(),
        "fact-mismatch-secret",
        serde_json::json!([
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::TermIndex.raw(),
                "value": 1,
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::TermIndex.raw(),
                "value": 2,
            },
        ]),
    );
    let rendered = render_error(&error);

    assert!(!rendered.contains("fact-mismatch-secret"), "{rendered}");
    assert!(
        rendered.contains("query field context mismatch"),
        "{rendered}"
    );
    assert!(rendered.contains("fact context mismatch"), "{rendered}");
    assert!(rendered.contains("term_index=1 term_index=2"), "{rendered}");
}

#[test]
fn renders_compact_query_not_found_code_report() {
    let report = render_error_code_report(6);

    assert!(report.contains("IcyDB diagnostic E6"), "{report}");
    assert!(report.contains("known: yes"), "{report}");
    assert!(report.contains("class: not-found"), "{report}");
    assert!(report.contains("default origin: query"), "{report}");
    assert!(
        report.contains("E_QUERY_NOT_FOUND: query expected one row but found none"),
        "{report}"
    );
}

#[test]
fn renders_known_and_unknown_numeric_facts() {
    let err: icydb::Error = serde_json::from_value(serde_json::json!({
        "code": icydb::ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS.raw(),
        "class": icydb::diagnostic::ErrorClass::Unsupported.wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Executor.wire_code(),
        "facts": [
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ActualCount.raw(),
                "value": 5_000,
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::Limit.raw(),
                "value": 4_096,
            },
            { "tag": 250, "value": 7 },
        ],
    }))
    .expect("numeric fact error should decode");

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: structural mutation batch exceeds the operation-count bound; facts actual_count=5000 limit=4096 tag#250=7; fact context mismatch: fact count exceeds the E-code maximum",
    );
}

#[test]
fn grouped_limit_labels_are_available_to_structured_and_manual_diagnostics() {
    assert_eq!(
        super::parse_fact_tag("grouped_limit").unwrap(),
        icydb::diagnostic::DiagnosticFactTag::GroupedLimit.raw()
    );
    for (value, label) in [
        (1, "distinct-values-per-group"),
        (2, "distinct-values-total"),
        (3, "estimated-bytes"),
        (4, "groups"),
    ] {
        assert_eq!(super::grouped_limit_text(value), Some(label));
        assert_eq!(
            super::render_fact_value(
                icydb::diagnostic::DiagnosticFactTag::GroupedLimit,
                value,
                None
            ),
            format!("{value}({label})")
        );
    }
    assert_eq!(super::grouped_limit_text(0), None);
    assert_eq!(super::grouped_limit_text(5), None);
}

#[test]
fn renders_hard_execution_budget_attribution() {
    let err: icydb::Error = serde_json::from_value(serde_json::json!({
        "code": icydb::ErrorCode::RUNTIME_BOUNDARY_EXECUTION_BUDGET_EXCEEDED.raw(),
        "class": icydb::diagnostic::ErrorClass::Unsupported.wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Executor.wire_code(),
        "facts": [
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::BudgetResource.raw(),
                "value": icydb::diagnostic::DiagnosticExecutionBudgetResource::StoredBytesRead.raw(),
            },
            { "tag": icydb::diagnostic::DiagnosticFactTag::Limit.raw(), "value": 4096 },
            { "tag": icydb::diagnostic::DiagnosticFactTag::Actual.raw(), "value": 4097 },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ExecutionBudgetScope.raw(),
                "value": icydb::diagnostic::DiagnosticExecutionBudgetScope::Request.raw(),
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ExecutionLane.raw(),
                "value": icydb::diagnostic::DiagnosticExecutionLane::TrustedRead.raw(),
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::QueryShapeFingerprintPrefix.raw(),
                "value": 17,
            },
        ],
    }))
    .expect("execution budget fact error should decode");

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: charged database work exceeds its hard execution budget; facts budget_resource=6(stored-bytes-read) limit=4096 actual=4097 execution_budget_scope=2(request) execution_lane=2(trusted-read) query_shape_fingerprint_prefix=17",
    );
}

#[test]
fn renders_convergence_backlog_pressure_attribution() {
    let err: icydb::Error = serde_json::from_value(serde_json::json!({
        "code": icydb::ErrorCode::RUNTIME_BOUNDARY_CONVERGENCE_BACKLOG_PRESSURE.raw(),
        "class": icydb::diagnostic::ErrorClass::Conflict.wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Executor.wire_code(),
        "facts": [
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::BacklogResource.raw(),
                "value": icydb::diagnostic::DiagnosticBacklogResource::Batches.raw(),
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::CurrentCount.raw(),
                "value": 38,
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ProposedCount.raw(),
                "value": 1,
            },
            { "tag": icydb::diagnostic::DiagnosticFactTag::Limit.raw(), "value": 38 },
        ],
    }))
    .expect("backlog-pressure fact error should decode");

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_CONFLICT: journal convergence backlog exceeds its cumulative admission limit; facts backlog_resource=1(batches) current_count=38 proposed_count=1 limit=38",
    );
}

#[test]
fn mutation_field_facts_resolve_only_with_exact_schema_identity() {
    use icydb::diagnostic::DiagnosticFactTag as Tag;

    let artifact = DiagnosticSchemaArtifact::test_fixture();
    let half = u64::from_be_bytes([7; 8]);
    let mut facts = vec![
        (Tag::AcceptedSchemaFingerprintMethod, 1),
        (Tag::AcceptedSchemaFingerprintHigh, half),
        (Tag::AcceptedSchemaFingerprintLow, half),
        (Tag::EntityTag, 42),
        (Tag::FieldId, 1),
        (Tag::MutationOperation, 1),
    ]
    .into_iter()
    .map(|(tag, value)| RawDiagnosticFact {
        tag: tag.raw(),
        value,
    })
    .collect::<Vec<_>>();

    // Operation-only admission and row-specific rejection share one identity.
    for batch_position in [false, true] {
        if batch_position {
            facts.push(RawDiagnosticFact {
                tag: Tag::BatchPosition.raw(),
                value: 3,
            });
        }
        let mut notes = Vec::new();
        let report = render_error_code_report_with_facts(221, &facts, &[&artifact], &mut notes);
        assert!(report.contains("entity_tag=42(Account)"), "{report}");
        assert!(report.contains("field_id=1(id)"), "{report}");
        assert!(notes.is_empty(), "{notes:?}");

        // Every authority component participates; unrelated schemas may reuse IDs.
        for index in 0..4 {
            let mut mismatched = facts.clone();
            mismatched[index].value += 1;
            let report = render_error_code_report_with_facts(
                221,
                &mismatched,
                &[&artifact],
                &mut Vec::new(),
            );
            assert!(!report.contains("Account"), "{report}");
            assert!(!report.contains("field_id=1(id)"), "{report}");
            assert!(report.contains("names withheld"), "{report}");
        }
        let report = render_error_code_report_with_facts(221, &facts, &[], &mut Vec::new());
        assert!(report.contains("field_id=1"), "{report}");
        assert!(!report.contains("field_id=1(id)"), "{report}");
    }
}

#[test]
fn resolved_fact_labels_escape_each_identity_family_at_presentation() {
    use icydb::diagnostic::DiagnosticFactTag;

    let label = "界-e\u{301}-\\n\t\r\n\x1b[2J\u{0085}\u{202e}\u{2028}";
    let mut json = serde_json::to_value(DiagnosticSchemaArtifact::test_fixture()).unwrap();
    let entity = &mut json["entities"][0];
    entity["entity_name"] = label.into();
    entity["fields"][0]["name"] = label.into();
    entity["constraints"][0]["name"] = label.into();
    entity["constraints"][0]["kind"] = label.into();
    entity["indexes"] = serde_json::json!([{"id": 2, "name": label}]);
    entity["relations"] = serde_json::json!([{"id": 4, "name": label}]);
    let artifact: DiagnosticSchemaArtifact = serde_json::from_value(json).unwrap();
    let resolved = artifact.resolve(1, [7; 16], 42, Some(3)).unwrap();
    for (tag, value) in [
        (DiagnosticFactTag::EntityTag, 42),
        (DiagnosticFactTag::ConstraintId, 3),
        (DiagnosticFactTag::FieldId, 1),
        (DiagnosticFactTag::RootField, 1),
        (DiagnosticFactTag::IndexId, 2),
        (DiagnosticFactTag::RelationId, 4),
        (DiagnosticFactTag::ConstraintKind, 255),
    ] {
        assert_eq!(
            render_fact_value(tag, value, Some(&resolved)),
            format!("{value}({})", ic_host_text::escape::terminal_text(label))
        );
    }
    assert_eq!(
        render_fact_value(DiagnosticFactTag::ConstraintKind, 5, Some(&resolved)),
        "5(unique)"
    );
}

#[test]
fn exact_artifact_humanizes_constraint_facts_and_stale_artifact_does_not() {
    use icydb::diagnostic::DiagnosticFactTag;

    let high = u64::from_be_bytes([7; 8]);
    let facts = [
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintMethod.raw(),
            value: 1,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintHigh.raw(),
            value: high,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintLow.raw(),
            value: high,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::EntityTag.raw(),
            value: 42,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintId.raw(),
            value: 3,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintKind.raw(),
            value: 5,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintContext.raw(),
            value: icydb::diagnostic::DiagnosticConstraintContext::WriteAdmission.raw(),
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::MutationOperation.raw(),
            value: 1,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::BatchPosition.raw(),
            value: 0,
        },
    ];
    let artifact = DiagnosticSchemaArtifact::test_fixture();
    let mut notes = Vec::new();
    let report =
        render_error_code_report_with_facts(210, facts.as_slice(), &[&artifact], &mut notes);
    assert!(report.contains("entity_tag=42(Account)"), "{report}");
    assert!(
        report.contains("constraint_id=3(account_name_unique)"),
        "{report}"
    );
    assert!(report.contains("constraint_kind=5(unique)"), "{report}");
    assert!(report.contains("mutation_operation=1(insert)"), "{report}");
    assert!(
        report.contains("accepted entity: Account (schema::Account)"),
        "{report}"
    );

    let mut stale_facts = facts;
    stale_facts[1].value = u64::from_be_bytes([8; 8]);
    let mut notes = Vec::new();
    let report =
        render_error_code_report_with_facts(210, stale_facts.as_slice(), &[&artifact], &mut notes);
    assert!(!report.contains("Account"), "{report}");
    assert!(report.contains("names withheld"), "{report}");

    let mut malformed_facts = facts;
    malformed_facts.swap(4, 5);
    let mut notes = Vec::new();
    let report = render_error_code_report_with_facts(
        210,
        malformed_facts.as_slice(),
        &[&artifact],
        &mut notes,
    );
    assert!(!report.contains("Account"), "{report}");
    assert!(report.contains("fact context mismatch"), "{report}");
}

#[test]
fn exact_schema_facts_without_resolver_explain_numeric_fallback() {
    use icydb::diagnostic::DiagnosticFactTag;

    let facts = [
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintMethod.raw(),
            value: 1,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintHigh.raw(),
            value: 1,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintLow.raw(),
            value: 2,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::EntityTag.raw(),
            value: 3,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintId.raw(),
            value: 4,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintKind.raw(),
            value: icydb::diagnostic::DiagnosticConstraintKind::Unique.raw(),
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintContext.raw(),
            value: icydb::diagnostic::DiagnosticConstraintContext::WriteAdmission.raw(),
        },
    ];
    let mut notes = Vec::new();
    let report = render_error_code_report_with_facts(210, facts.as_slice(), &[], &mut notes);

    assert!(report.contains("entity_tag=3"), "{report}");
    assert!(
        report.contains("supply --artifact, --canister, or --source-metadata"),
        "{report}"
    );
}

#[test]
fn exact_source_metadata_humanizes_only_its_bound_schema_identity() {
    use icydb::diagnostic::DiagnosticFactTag;

    let high = u64::from_be_bytes([7; 8]);
    let facts = [
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintMethod.raw(),
            value: 1,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintHigh.raw(),
            value: high,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::AcceptedSchemaFingerprintLow.raw(),
            value: high,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::EntityTag.raw(),
            value: 42,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintId.raw(),
            value: 3,
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintKind.raw(),
            value: icydb::diagnostic::DiagnosticConstraintKind::Unique.raw(),
        },
        RawDiagnosticFact {
            tag: DiagnosticFactTag::ConstraintContext.raw(),
            value: icydb::diagnostic::DiagnosticConstraintContext::WriteAdmission.raw(),
        },
    ];
    let metadata = DiagnosticSchemaArtifact::test_source_fixture();
    let mut notes = Vec::new();
    let report =
        render_error_code_report_with_facts(210, facts.as_slice(), &[&metadata], &mut notes);

    assert!(report.contains("entity_tag=42(SourceAccount)"), "{report}");
    assert!(
        report.contains("constraint_id=3(source_account_name_unique)"),
        "{report}"
    );

    let deployment = DiagnosticSchemaArtifact::test_fixture();
    let mut notes = Vec::new();
    let report = render_error_code_report_with_facts(
        210,
        facts.as_slice(),
        &[&deployment, &metadata],
        &mut notes,
    );
    assert!(report.contains("entity_tag=42(Account)"), "{report}");
    assert!(!report.contains("SourceAccount"), "{report}");

    let mut stale_facts = facts;
    stale_facts[2].value = u64::from_be_bytes([8; 8]);
    let mut notes = Vec::new();
    let report =
        render_error_code_report_with_facts(210, stale_facts.as_slice(), &[&metadata], &mut notes);
    assert!(!report.contains("SourceAccount"), "{report}");
    assert!(report.contains("names withheld"), "{report}");
}

#[test]
fn renders_cursor_and_recovery_fact_tags_without_canister_prose() {
    let cursor: icydb::Error = serde_json::from_value(serde_json::json!({
        "code": icydb::ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR.raw(),
        "class": icydb::diagnostic::ErrorClass::Unsupported.wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Cursor.wire_code(),
        "facts": [
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ComponentIndex.raw(),
                "value": 1,
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::DecodeReason.raw(),
                "value": icydb::diagnostic::DiagnosticDecodeReason::CursorInvalidBase64.raw(),
            },
        ],
    }))
    .expect("cursor fact error should decode");
    assert!(render_error(&cursor).ends_with("facts component_index=1 decode_reason=4"),);

    let recovery: icydb::Error = serde_json::from_value(serde_json::json!({
        "code": icydb::ErrorCode::RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT.raw(),
        "class": icydb::diagnostic::ErrorClass::IncompatiblePersistedFormat.wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Recovery.wire_code(),
        "facts": [
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ExpectedVersion.raw(),
                "value": 9,
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ActualVersion.raw(),
                "value": 7,
            },
        ],
    }))
    .expect("recovery fact error should decode");
    assert!(render_error(&recovery).ends_with("facts expected_version=9 actual_version=7"),);

    let component: icydb::Error = serde_json::from_value(serde_json::json!({
        "code": icydb::ErrorCode::STORE_CORRUPTION.raw(),
        "class": icydb::diagnostic::ErrorClass::Corruption.wire_code(),
        "origin": icydb::diagnostic::ErrorOrigin::Store.wire_code(),
        "facts": [
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ComponentKind.raw(),
                "value": icydb::diagnostic::DiagnosticComponentKind::CommitDataKey.raw(),
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::ActualLength.raw(),
                "value": 513,
            },
            {
                "tag": icydb::diagnostic::DiagnosticFactTag::Limit.raw(),
                "value": 512,
            },
        ],
    }))
    .expect("component fact error should decode");
    assert!(
        render_error(&component)
            .ends_with("facts component_kind=1(commit-data-key) actual_length=513 limit=512"),
    );
}

#[test]
fn renders_compact_read_admission_code_report() {
    let report = render_error_code_report(173);

    assert!(report.contains("IcyDB diagnostic E173"), "{report}");
    assert!(report.contains("E_QUERY_READ_ADMISSION"), "{report}");
    assert!(
        report.contains("public read queries cannot execute an unbounded full scan"),
        "{report}"
    );
    assert!(
        report.contains("add a suitable index"),
        "read-admission report should include fix guidance: {report}"
    );
}

#[test]
fn diagnostic_code_parser_accepts_quoted_e_prefix() {
    assert_eq!(parse_error_code("\"e7\""), Ok(7));
}

#[test]
fn diagnostic_code_parser_rejects_non_numeric_input() {
    let err = parse_error_code("banana").expect_err("non-code input should fail");

    assert!(err.contains("expected E7"), "{err}");
}

#[test]
fn unknown_compact_code_report_is_explicit() {
    let report = render_error_code_report(9999);

    assert!(report.contains("known: no"), "{report}");
    assert!(
        report.contains("reason: unknown compact error code"),
        "{report}"
    );
    assert!(
        report.contains("registry fallback: E_RUNTIME_INTERNAL"),
        "{report}"
    );
}

#[test]
fn renders_schema_ddl_admission_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::SchemaDdlAdmission,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::SchemaDdlAdmission {
            reason: icydb::diagnostic::SchemaDdlAdmissionCode::PublicationRaceLost,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_SCHEMA_DDL_ADMISSION: SQL DDL admission rejected: accepted schema changed after DDL binding",
    );
}

#[test]
fn renders_unsupported_sql_feature_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QueryUnsupportedSqlFeature,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::UnsupportedSqlFeature {
            feature: icydb::diagnostic::SqlFeatureCode::Join,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_UNSUPPORTED_SQL_FEATURE: unsupported SQL feature: JOIN",
    );
}

#[test]
fn renders_sql_surface_mismatch_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QuerySqlSurfaceMismatch,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::SqlSurfaceMismatch {
            mismatch: icydb::diagnostic::SqlSurfaceMismatchCode::QueryRejectsInsert,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_SQL_SURFACE_MISMATCH: execute_trusted_sql_query rejects INSERT; use execute_trusted_sql_mutation()",
    );
}

#[test]
fn renders_sql_write_boundary_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QuerySqlWriteBoundary,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb::diagnostic::SqlWriteBoundaryCode::MissingPrimaryKey,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_SQL_WRITE_BOUNDARY: SQL write rejected: INSERT is missing required primary key fields",
    );
}

#[test]
fn renders_sql_write_staged_row_boundary_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QuerySqlWriteBoundary,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::SqlWriteBoundary {
            boundary: icydb::diagnostic::SqlWriteBoundaryCode::StagedRowsTooMany,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_SQL_WRITE_BOUNDARY: SQL write rejected: SQL write stages more rows than this endpoint's row budget",
    );
}

#[test]
fn renders_query_projection_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QueryUnsupportedProjection,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::QueryProjection {
            reason: icydb::diagnostic::QueryProjectionCode::NumericScaleArguments,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_UNSUPPORTED_PROJECTION: query projection rejected: scale-taking numeric projections require a non-negative integer scale",
    );
}

#[test]
fn renders_query_read_admission_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission {
            reason: icydb::diagnostic::QueryReadAdmissionCode::PublicQueryRequiresLimit,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_READ_ADMISSION: query read admission rejected: public read queries require a bounded read intent; fix: add a positive limit within policy or use exact selected primary-key access",
    );
}

#[test]
fn renders_query_read_admission_fix_hints_for_common_public_read_rejections() {
    let cases = [
        (
            icydb::diagnostic::QueryReadAdmissionCode::UnboundedFullScanRejected,
            "E_QUERY_READ_ADMISSION: query read admission rejected: public read queries cannot execute an unbounded full scan; fix: add a suitable index, tighten the predicate, or move the query behind a trusted admin endpoint",
        ),
        (
            icydb::diagnostic::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
            "E_QUERY_READ_ADMISSION: query read admission rejected: grouped reads require explicit group and memory budgets; fix: add grouped_limits(max_groups, max_group_bytes) and keep DISTINCT aggregates within policy",
        ),
        (
            icydb::diagnostic::QueryReadAdmissionCode::SortRequiresMaterialization,
            "E_QUERY_READ_ADMISSION: query read admission rejected: this read requires materializing rows for ORDER BY; fix: order by the selected index order, remove the sort, or keep the query on a trusted admin path",
        ),
        (
            icydb::diagnostic::QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy,
            "E_QUERY_READ_ADMISSION: query read admission rejected: primary-key input literals exceed this endpoint's read budget; fix: reduce the primary-key IN list or move the read behind a trusted admin endpoint",
        ),
    ];

    for (reason, expected) in cases {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission { reason }),
        ));

        assert_eq!(render_error(&err), expected);
    }
}

#[test]
fn renders_query_read_admission_fix_hint_for_every_rejection_code() {
    let reasons = [
        icydb::diagnostic::QueryReadAdmissionCode::PublicQueryRequiresLimit,
        icydb::diagnostic::QueryReadAdmissionCode::PublicQueryRequiresIndex,
        icydb::diagnostic::QueryReadAdmissionCode::UnboundedFullScanRejected,
        icydb::diagnostic::QueryReadAdmissionCode::SortRequiresMaterialization,
        icydb::diagnostic::QueryReadAdmissionCode::GroupedQueryRequiresLimits,
        icydb::diagnostic::QueryReadAdmissionCode::GroupedQueryExceedsBudget,
        icydb::diagnostic::QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute,
        icydb::diagnostic::QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy,
        icydb::diagnostic::QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy,
        icydb::diagnostic::QueryReadAdmissionCode::InputDepthExceeded,
        icydb::diagnostic::QueryReadAdmissionCode::InputNodesExceeded,
        icydb::diagnostic::QueryReadAdmissionCode::InputBytesExceeded,
        icydb::diagnostic::QueryReadAdmissionCode::ExplainDoesNotAcceptCursor,
    ];

    for reason in reasons {
        let rendered = render_query_read_admission_error(reason);
        let (_, fix) = rendered
            .split_once("; fix: ")
            .expect("read-admission diagnostics should render a fix hint");

        assert!(
            !fix.is_empty(),
            "read-admission diagnostics should render a non-empty fix hint: {rendered}",
        );
    }
}

#[test]
fn renders_unknown_aggregate_target_field_code() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::from_code(
        icydb::diagnostic::DiagnosticCode::QueryUnknownAggregateTargetField,
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD: unknown aggregate target field",
    );
}

#[test]
fn renders_sql_lowering_detail() {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QueryUnsupportedSqlFeature,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::SqlLowering {
            reason: icydb::diagnostic::SqlLoweringCode::DistinctOrderByProjection,
        }),
    ));

    assert_eq!(
        render_error(&err),
        "E_QUERY_UNSUPPORTED_SQL_FEATURE: unsupported SQL lowering: SELECT DISTINCT ORDER BY terms must be derivable from the projected tuple",
    );
}

#[test]
fn renders_runtime_boundary_details() {
    let cases = [
        (
            icydb::diagnostic::RuntimeBoundaryCode::SqlQueryEntityNotFound,
            "E_RUNTIME_NOT_FOUND: SQL query target entity was not found in the accepted schema",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
            "E_RUNTIME_UNSUPPORTED: operational endpoint requires controller access",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::SqlDdlTargetRequired,
            "E_RUNTIME_UNSUPPORTED: SQL DDL requires one target entity",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::MutationRequiredFieldMissing,
            "E_RUNTIME_UNSUPPORTED: mutation is missing one or more required fields",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::MutationManagedTimestampRegression,
            "E_RUNTIME_INVARIANT_VIOLATION: mutation operation time precedes an accepted managed timestamp",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::MutationDatabaseOwnedFieldExplicit,
            "E_RUNTIME_UNSUPPORTED: mutation explicitly authors a database-owned field",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::MutationBatchEmpty,
            "E_RUNTIME_UNSUPPORTED: structural mutation batch is empty",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::MutationBatchDuplicateKey,
            "E_RUNTIME_CONFLICT: structural mutation batch targets the same accepted key more than once",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::PersistedRowLayoutOutsideAcceptedWindow,
            "E_RUNTIME_CORRUPTION: persisted row layout is outside the accepted layout window",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::PersistedRowSlotCountMismatch,
            "E_RUNTIME_CORRUPTION: persisted row slot count does not match its stamped layout",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::GeneratedFieldAfterDdlField,
            "E_RUNTIME_UNSUPPORTED: generated field would collide with an accepted SQL DDL field slot",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::ConstraintViolation,
            "E_RUNTIME_INVARIANT_VIOLATION: mutation violates an accepted constraint or activation gate",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::AcceptedRowConstraintProgramCorrupt,
            "E_RUNTIME_CORRUPTION: accepted row-constraint program is corrupt",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::ConstraintActivationWriteBlocked,
            "E_RUNTIME_CONFLICT: write conflicts with an incomplete constraint activation",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::GeneratedConstraintActivationStale,
            "E_RUNTIME_CONFLICT: generated constraint proposal no longer matches its live activation",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::ExactKeyBatchTooManyItems,
            "E_RUNTIME_UNSUPPORTED: exact-key batch exceeds the input item-count bound",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::ExactKeyBatchInputBytesExceeded,
            "E_RUNTIME_UNSUPPORTED: exact-key batch exceeds the encoded input-key byte bound",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::ExactKeyBatchStoredBytesExceeded,
            "E_RUNTIME_UNSUPPORTED: exact-key batch exceeds the distinct stored-row byte bound",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::ExactKeyBatchResultBytesExceeded,
            "E_RUNTIME_UNSUPPORTED: exact-key batch exceeds the logical result byte bound",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::ExecutionBudgetExceeded,
            "E_RUNTIME_UNSUPPORTED: charged database work exceeds its hard execution budget",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::PageUnitTooLarge,
            "E_RUNTIME_UNSUPPORTED: one scalar-page unit exceeds its resumable page-work envelope",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::RequestExecutionScopeRequired,
            "E_RUNTIME_UNSUPPORTED: no IcyDB request-execution scope is active; wrap the entry point with #[icydb::request_execution], #[icydb::test], or with_request_execution",
        ),
        (
            icydb::diagnostic::RuntimeBoundaryCode::RequestExecutionRootMismatch,
            "E_RUNTIME_CONFLICT: explicit IcyDB request root conflicts with the active request root",
        ),
    ];

    for (boundary, expected) in cases {
        let err = icydb::Error::from_runtime_boundary(boundary, icydb::ErrorOrigin::Interface);
        assert_eq!(render_error(&err), expected);
    }
}

#[test]
fn renders_mutation_batch_commit_work_boundary_detail() {
    let err = icydb::Error::from_runtime_boundary(
        icydb::diagnostic::RuntimeBoundaryCode::MutationBatchCommitWorkExceeded,
        icydb::ErrorOrigin::Executor,
    );

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: structural mutation batch exceeds the prepared-commit work bound",
    );
}

#[test]
fn renders_sql_surface_policy_denial() {
    let err = icydb::Error::from_runtime_boundary(
        icydb::diagnostic::RuntimeBoundaryCode::SqlSurfacePolicyDenied,
        icydb::ErrorOrigin::Interface,
    );

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: application policy denied access to the SQL endpoint",
    );
}

#[test]
fn renders_schema_surface_policy_denial() {
    let err = icydb::Error::from_runtime_boundary(
        icydb::diagnostic::RuntimeBoundaryCode::SchemaSurfacePolicyDenied,
        icydb::ErrorOrigin::Interface,
    );

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: application policy denied access to the schema endpoint",
    );
}

#[test]
fn renders_sql_query_reply_bytes_boundary_detail() {
    let err = icydb::Error::from_runtime_boundary(
        icydb::diagnostic::RuntimeBoundaryCode::SqlQueryReplyBytesExceeded,
        icydb::ErrorOrigin::Response,
    );

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: SQL query result exceeds the public reply byte limit",
    );
}

#[test]
fn renders_logical_explain_output_boundary_detail() {
    let err = icydb::Error::from_runtime_boundary(
        icydb::diagnostic::RuntimeBoundaryCode::QueryExplainOutputExceeded,
        icydb::ErrorOrigin::Query,
    );
    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: logical explain output exceeds the render byte limit",
    );
}

#[test]
fn renders_logical_explain_depth_boundary_detail() {
    let err = icydb::Error::from_runtime_boundary(
        icydb::diagnostic::RuntimeBoundaryCode::QueryExplainDepthExceeded,
        icydb::ErrorOrigin::Query,
    );
    assert_eq!(
        render_error(&err),
        "E_RUNTIME_UNSUPPORTED: logical explain access tree exceeds the diagnostic depth limit",
    );
}

#[test]
fn renders_database_startup_recovery_pending_boundary_detail() {
    let err = icydb::Error::from_runtime_boundary(
        icydb::diagnostic::RuntimeBoundaryCode::DatabaseStartupRecoveryPending,
        icydb::ErrorOrigin::Recovery,
    );

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_CONFLICT: database startup recovery is still in progress",
    );
}

#[test]
fn renders_bucket_mismatch_with_requested_and_persisted_pages() {
    let facts = [
        RawDiagnosticFact {
            tag: icydb::diagnostic::DiagnosticFactTag::Expected.raw(),
            value: 16,
        },
        RawDiagnosticFact {
            tag: icydb::diagnostic::DiagnosticFactTag::Actual.raw(),
            value: 128,
        },
    ];
    let mut notes = Vec::new();
    let report = render_error_code_report_with_facts(274, &facts, &[], &mut notes);
    assert!(report.contains("memory bucket size mismatch"), "{report}");
    assert!(
        report.contains("expected=requested, actual=persisted"),
        "{report}"
    );
    assert!(report.contains("expected=16"), "{report}");
    assert!(report.contains("actual=128"), "{report}");
}

#[test]
fn memory_admission_codes_render_without_schema_artifacts() {
    for (code, hint) in [
        (276, "Allowed"),
        (277, "namespace"),
        (278, "roles"),
        (279, "declaration"),
        (280, "established"),
        (281, "journal"),
    ] {
        let mut notes = Vec::new();
        let facts = if code == 278 {
            vec![RawDiagnosticFact {
                tag: icydb::diagnostic::DiagnosticFactTag::ExpectedCount.raw(),
                value: 3,
            }]
        } else {
            Vec::new()
        };
        let report = render_error_code_report_with_facts(code, &facts, &[], &mut notes);
        assert!(report.contains(hint), "{code}: {report}");
        assert_eq!(diagnostic_fact_schema_mismatch(code, &facts), None);
        if code == 278 {
            assert!(report.contains("expected_count=3"), "{report}");
        }
    }
}

#[test]
fn falls_back_to_code_text_without_detail() {
    let err = icydb::Error::from_code(
        icydb::diagnostic::DiagnosticCode::RuntimeInternal,
        icydb::ErrorOrigin::Runtime,
    );

    assert_eq!(
        render_error(&err),
        "E_RUNTIME_INTERNAL: internal runtime failure"
    );
}

fn render_query_read_admission_error(reason: icydb::diagnostic::QueryReadAdmissionCode) -> String {
    let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
        icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
        icydb::diagnostic::ErrorOrigin::Query,
        Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission { reason }),
    ));

    render_error(&err)
}
