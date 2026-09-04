//! Module: diagnostic rendering.
//! Responsibility: render compact IcyDB diagnostic payloads for host/CLI users.
//! Does not own: canister wire shape, core error classification, or recovery policy.
//! Boundary: keeps rich diagnostic prose out of production canister crates.

pub(crate) mod artifact;

use icydb::diagnostic::{
    DiagnosticBacklogResource, DiagnosticCode, DiagnosticComponentKind,
    DiagnosticConstraintContext, DiagnosticConstraintKind, DiagnosticDetail,
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
    DiagnosticFactTag, DiagnosticMutationOperation, ErrorClass, ErrorCode, ErrorOrigin,
    QueryErrorKind, QueryFieldRole, QueryFieldSchemaMismatch, QueryProjectionCode,
    QueryReadAdmissionCode, RuntimeBoundaryCode, RuntimeErrorKind, SchemaDdlAdmissionCode,
    SchemaMigrationCode, SqlFeatureCode, SqlLoweringCode, SqlSurfaceMismatchCode,
    SqlWriteBoundaryCode,
};
use std::fmt::Write as _;

use crate::{
    cli::DiagnosticArgs,
    diagnostic::artifact::{DiagnosticSchemaArtifact, ResolvedDiagnosticEntity},
    observability::{load_schema_report, render_hex_lower},
};

#[derive(Clone, Copy)]
struct RawDiagnosticFact {
    tag: u8,
    value: u64,
}

#[derive(Clone, Copy)]
struct DiagnosticSchemaIdentity {
    fingerprint_method: u8,
    fingerprint: [u8; 16],
    entity_tag: u64,
    constraint_id: Option<u32>,
}

/// Resolve and print one compact diagnostic entirely from host-side authority.
pub(crate) fn run_diagnostic_command(args: DiagnosticArgs) -> Result<(), String> {
    if args.facts().is_empty()
        && args.artifact().is_none()
        && args.source_metadata().is_none()
        && args.canister_name().is_none()
    {
        println!("{}", render_error_code_report(args.code())?);
        return Ok(());
    }

    let facts = parse_facts(args.facts())?;
    let explicit_artifact = args
        .artifact()
        .map(DiagnosticSchemaArtifact::read_deployment)
        .transpose()?;
    let source_metadata = args
        .source_metadata()
        .map(DiagnosticSchemaArtifact::read_source_metadata)
        .transpose()?;

    let mut notes = Vec::new();
    let explicit_artifact = explicit_artifact.as_ref().filter(|artifact| {
        let Some(canister) = args.canister_name() else {
            return true;
        };
        let matches = artifact.provenance_matches(args.environment(), canister);
        if !matches {
            notes.push(
                "artifact provenance does not match the selected deployment; names withheld"
                    .to_string(),
            );
        }
        matches
    });
    let fact_schema_is_valid =
        diagnostic_fact_schema_mismatch(args.code(), facts.as_slice())?.is_none();
    let identity = fact_schema_is_valid
        .then(|| DiagnosticSchemaIdentity::from_facts(facts.as_slice()))
        .flatten();
    let exact_artifact_found = identity.is_some_and(|identity| {
        explicit_artifact.is_some_and(|artifact| {
            artifact
                .resolve(
                    identity.fingerprint_method,
                    identity.fingerprint,
                    identity.entity_tag,
                    identity.constraint_id,
                )
                .is_some()
        })
    });
    let live_artifact = if !fact_schema_is_valid || exact_artifact_found {
        None
    } else if let Some(canister) = args.canister_name() {
        match load_schema_report(args.environment(), canister) {
            Ok(report) => Some(DiagnosticSchemaArtifact::from_report(
                args.environment(),
                canister,
                report.as_slice(),
            )?),
            Err(err) => {
                notes.push(format!(
                    "live schema introspection unavailable ({err}); continuing with offline resolvers"
                ));
                None
            }
        }
    } else {
        None
    };
    let source_metadata = source_metadata.as_ref().filter(|metadata| {
        let exact = identity.is_some_and(|identity| {
            metadata
                .resolve(
                    identity.fingerprint_method,
                    identity.fingerprint,
                    identity.entity_tag,
                    identity.constraint_id,
                )
                .is_some()
        });
        if identity.is_some() && !exact {
            notes.push(
                "source metadata does not prove the exact accepted fingerprint and entity identity; names withheld"
                    .to_string(),
            );
        }
        exact
    });
    let artifacts = [explicit_artifact, live_artifact.as_ref(), source_metadata]
        .into_iter()
        .flatten()
        .collect::<Vec<_>>();

    let report = render_error_code_report_with_facts(
        args.code(),
        facts.as_slice(),
        artifacts.as_slice(),
        &mut notes,
    )?;
    println!("{report}");
    Ok(())
}

/// Render one compact public IcyDB error code for CLI lookup.
pub(crate) fn render_error_code_report(input: &str) -> Result<String, String> {
    let mut notes = Vec::new();
    render_error_code_report_with_facts(input, &[], &[], &mut notes)
}

fn render_error_code_report_with_facts(
    input: &str,
    facts: &[RawDiagnosticFact],
    artifacts: &[&DiagnosticSchemaArtifact],
    notes: &mut Vec<String>,
) -> Result<String, String> {
    let raw = parse_error_code(input)?;
    let code = ErrorCode::from_raw(raw);
    let diagnostic_code = code.diagnostic_code();
    let default_origin = diagnostic_code.origin();
    let facade_error = icydb::Error::from_error_code(code, default_origin.into());

    let mut lines = Vec::with_capacity(7);
    lines.push(format!("IcyDB diagnostic E{}", code.raw()));
    lines.push(format!("raw code: {}", code.raw()));
    lines.push(format!(
        "known: {}",
        if code.is_known() { "yes" } else { "no" }
    ));
    lines.push(format!("class: {}", class_text(code.class())));
    lines.push(format!("default origin: {}", origin_text(default_origin)));

    if code.is_known() {
        lines.push(format!("reason: {}", render_error(&facade_error)));
    } else {
        lines.push("reason: unknown compact error code".to_string());
        lines.push(format!(
            "registry fallback: {}",
            render_error(&facade_error)
        ));
    }

    if !facts.is_empty() {
        let schema_mismatch = diagnostic_fact_schema_mismatch(input, facts)?;
        let identity = schema_mismatch
            .is_none()
            .then(|| DiagnosticSchemaIdentity::from_facts(facts))
            .flatten();
        let resolved = identity.and_then(|identity| {
            artifacts.iter().find_map(|artifact| {
                artifact.resolve(
                    identity.fingerprint_method,
                    identity.fingerprint,
                    identity.entity_tag,
                    identity.constraint_id,
                )
            })
        });
        lines.push(format!(
            "facts: {}",
            render_raw_facts(facts, resolved.as_ref())
        ));
        if let Some(mismatch) = schema_mismatch {
            notes.push(format!(
                "fact context mismatch: {}",
                fact_schema_mismatch_text(mismatch)
            ));
        }
        if let Some(identity) = identity {
            lines.push(format!(
                "accepted schema identity: method={} fingerprint={} entity_tag={}",
                identity.fingerprint_method,
                render_hex_lower(&identity.fingerprint),
                identity.entity_tag
            ));
        }
        match (artifacts.is_empty(), identity, resolved.as_ref()) {
            (_, None, _) => notes.push(
                "numeric fallback: exact fingerprint method, fingerprint, and entity tag facts are required"
                    .to_string(),
            ),
            (true, Some(_), _) => notes.push(
                "numeric fallback: supply --artifact, --canister, or --source-metadata for exact schema labels"
                    .to_string(),
            ),
            (false, Some(_), None) => notes.push(
                "numeric fallback: schema artifact has no exact fingerprint/entity match; names withheld"
                    .to_string(),
            ),
            (false, Some(_), Some(resolved)) => lines.push(format!(
                "accepted entity: {} ({})",
                resolved.entity_name(),
                resolved.entity_path()
            )),
        }
    }
    lines.extend(notes.iter().map(|note| format!("note: {note}")));

    Ok(lines.join("\n"))
}

fn diagnostic_fact_schema_mismatch(
    input: &str,
    facts: &[RawDiagnosticFact],
) -> Result<Option<icydb::diagnostic::DiagnosticFactSchemaMismatch>, String> {
    let code = ErrorCode::from_raw(parse_error_code(input)?);
    let raw = facts
        .iter()
        .map(|fact| (fact.tag, fact.value))
        .collect::<Vec<_>>();
    Ok(icydb::diagnostic::validate_raw_diagnostic_fact_schema(code, raw.as_slice()).err())
}

const fn fact_schema_mismatch_text(
    mismatch: icydb::diagnostic::DiagnosticFactSchemaMismatch,
) -> &'static str {
    use icydb::diagnostic::DiagnosticFactSchemaMismatch;
    match mismatch {
        DiagnosticFactSchemaMismatch::GlobalMaximumExceeded => {
            "fact count exceeds the global maximum"
        }
        DiagnosticFactSchemaMismatch::CodeMaximumExceeded => {
            "fact count exceeds the E-code maximum"
        }
        DiagnosticFactSchemaMismatch::InvalidSequence => {
            "required, allowed, repeated, or ordered tags do not match the E-code"
        }
        DiagnosticFactSchemaMismatch::InvalidValue => {
            "a known tag carries an invalid compact value"
        }
    }
}

impl DiagnosticSchemaIdentity {
    fn from_facts(facts: &[RawDiagnosticFact]) -> Option<Self> {
        let fingerprint_method = u8::try_from(single_fact(
            facts,
            DiagnosticFactTag::AcceptedSchemaFingerprintMethod,
        )?)
        .ok()?;
        let high = single_fact(facts, DiagnosticFactTag::AcceptedSchemaFingerprintHigh)?;
        let low = single_fact(facts, DiagnosticFactTag::AcceptedSchemaFingerprintLow)?;
        let entity_tag = single_fact(facts, DiagnosticFactTag::EntityTag)?;
        let mut fingerprint = [0_u8; 16];
        fingerprint[..8].copy_from_slice(high.to_be_bytes().as_slice());
        fingerprint[8..].copy_from_slice(low.to_be_bytes().as_slice());
        let constraint_id = optional_single_fact(facts, DiagnosticFactTag::ConstraintId)
            .and_then(|value| u32::try_from(value).ok());
        Some(Self {
            fingerprint_method,
            fingerprint,
            entity_tag,
            constraint_id,
        })
    }
}

fn parse_facts(inputs: &[String]) -> Result<Vec<RawDiagnosticFact>, String> {
    if inputs.len() > icydb::diagnostic::MAX_PUBLIC_DIAGNOSTIC_FACTS {
        return Err(format!(
            "diagnostic input has {} facts; maximum is {}",
            inputs.len(),
            icydb::diagnostic::MAX_PUBLIC_DIAGNOSTIC_FACTS
        ));
    }
    inputs.iter().map(|input| parse_fact(input)).collect()
}

fn parse_fact(input: &str) -> Result<RawDiagnosticFact, String> {
    let (tag, value) = input
        .split_once('=')
        .ok_or_else(|| format!("invalid diagnostic fact `{input}`; expected TAG=VALUE"))?;
    let tag = parse_fact_tag(tag)?;
    let value = value.parse::<u64>().map_err(|_| {
        format!("invalid diagnostic fact `{input}`; VALUE must be an unsigned integer")
    })?;
    Ok(RawDiagnosticFact { tag, value })
}

fn parse_fact_tag(input: &str) -> Result<u8, String> {
    if let Ok(raw) = input.parse::<u8>() {
        return Ok(raw);
    }
    for raw in u8::MIN..=u8::MAX {
        let Some(tag) = DiagnosticFactTag::known(raw) else {
            continue;
        };
        if fact_tag_text(tag) == input {
            return Ok(raw);
        }
    }
    Err(format!(
        "unknown diagnostic fact tag `{input}`; use a numeric tag or maintained label"
    ))
}

fn single_fact(facts: &[RawDiagnosticFact], tag: DiagnosticFactTag) -> Option<u64> {
    let mut matching = facts.iter().filter(|fact| fact.tag == tag.raw());
    let value = matching.next()?.value;
    matching.next().is_none().then_some(value)
}

fn optional_single_fact(facts: &[RawDiagnosticFact], tag: DiagnosticFactTag) -> Option<u64> {
    single_fact(facts, tag)
}

fn render_raw_facts(
    facts: &[RawDiagnosticFact],
    resolved: Option<&ResolvedDiagnosticEntity<'_>>,
) -> String {
    let mut rendered = String::new();
    for (index, fact) in facts.iter().enumerate() {
        if index != 0 {
            rendered.push(' ');
        }
        let Some(tag) = DiagnosticFactTag::known(fact.tag) else {
            let _ = write!(rendered, "tag#{}={}", fact.tag, fact.value);
            continue;
        };
        let _ = write!(
            rendered,
            "{}={}",
            fact_tag_text(tag),
            render_fact_value(tag, fact.value, resolved)
        );
    }
    rendered
}

fn render_fact_value(
    tag: DiagnosticFactTag,
    value: u64,
    resolved: Option<&ResolvedDiagnosticEntity<'_>>,
) -> String {
    let label = match tag {
        DiagnosticFactTag::EntityTag => resolved.map(ResolvedDiagnosticEntity::entity_name),
        DiagnosticFactTag::ConstraintId => {
            resolved.and_then(ResolvedDiagnosticEntity::constraint_name)
        }
        DiagnosticFactTag::FieldId | DiagnosticFactTag::RootField => u32::try_from(value)
            .ok()
            .and_then(|id| resolved.and_then(|resolved| resolved.field_name(id))),
        DiagnosticFactTag::IndexId => u32::try_from(value)
            .ok()
            .and_then(|id| resolved.and_then(|resolved| resolved.index_name(id))),
        DiagnosticFactTag::RelationId => u32::try_from(value)
            .ok()
            .and_then(|id| resolved.and_then(|resolved| resolved.relation_name(id))),
        DiagnosticFactTag::ConstraintKind => constraint_kind_text(value)
            .or_else(|| resolved.and_then(ResolvedDiagnosticEntity::constraint_kind)),
        DiagnosticFactTag::ConstraintContext => constraint_context_text(value),
        DiagnosticFactTag::MutationOperation => mutation_operation_text(value),
        DiagnosticFactTag::ComponentKind => component_kind_text(value),
        DiagnosticFactTag::BudgetResource => execution_budget_resource_text(value),
        DiagnosticFactTag::BacklogResource => backlog_resource_text(value),
        DiagnosticFactTag::ExecutionBudgetScope => execution_budget_scope_text(value),
        DiagnosticFactTag::ExecutionLane => execution_lane_text(value),
        _ => None,
    };
    label.map_or_else(|| value.to_string(), |label| format!("{value}({label})"))
}

const fn backlog_resource_text(value: u64) -> Option<&'static str> {
    match DiagnosticBacklogResource::known(value) {
        Some(DiagnosticBacklogResource::Batches) => Some("batches"),
        Some(DiagnosticBacklogResource::Records) => Some("records"),
        Some(DiagnosticBacklogResource::EncodedBytes) => Some("encoded-bytes"),
        None => None,
    }
}

const fn execution_budget_resource_text(value: u64) -> Option<&'static str> {
    match DiagnosticExecutionBudgetResource::known(value) {
        Some(DiagnosticExecutionBudgetResource::QueryExecutions) => Some("query-executions"),
        Some(DiagnosticExecutionBudgetResource::PlanningSteps) => Some("planning-steps"),
        Some(DiagnosticExecutionBudgetResource::PlanCompilations) => Some("plan-compilations"),
        Some(DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited) => {
            Some("key-index-entries-visited")
        }
        Some(DiagnosticExecutionBudgetResource::RowsVisited) => Some("rows-visited"),
        Some(DiagnosticExecutionBudgetResource::StoredBytesRead) => Some("stored-bytes-read"),
        Some(DiagnosticExecutionBudgetResource::PredicateExpressionSteps) => {
            Some("predicate-expression-steps")
        }
        Some(DiagnosticExecutionBudgetResource::NestedValueSteps) => Some("nested-value-steps"),
        Some(DiagnosticExecutionBudgetResource::DecodedBytes) => Some("decoded-bytes"),
        Some(DiagnosticExecutionBudgetResource::MaterializedBytes) => Some("materialized-bytes"),
        Some(DiagnosticExecutionBudgetResource::SortEntries) => Some("sort-entries"),
        Some(DiagnosticExecutionBudgetResource::SortComparisons) => Some("sort-comparisons"),
        Some(DiagnosticExecutionBudgetResource::SortTemporaryBytes) => Some("sort-temporary-bytes"),
        Some(DiagnosticExecutionBudgetResource::GroupDistinctEntries) => {
            Some("group-distinct-entries")
        }
        Some(DiagnosticExecutionBudgetResource::GroupDistinctStateBytes) => {
            Some("group-distinct-state-bytes")
        }
        Some(DiagnosticExecutionBudgetResource::CursorSteps) => Some("cursor-steps"),
        Some(DiagnosticExecutionBudgetResource::TemporaryBytes) => Some("temporary-bytes"),
        Some(DiagnosticExecutionBudgetResource::ResultRows) => Some("result-rows"),
        Some(DiagnosticExecutionBudgetResource::ResultBytes) => Some("result-bytes"),
        Some(DiagnosticExecutionBudgetResource::InstructionUnits) => Some("instruction-units"),
        None => None,
    }
}

const fn execution_budget_scope_text(value: u64) -> Option<&'static str> {
    match DiagnosticExecutionBudgetScope::known(value) {
        Some(DiagnosticExecutionBudgetScope::Execution) => Some("execution"),
        Some(DiagnosticExecutionBudgetScope::Request) => Some("request"),
        None => None,
    }
}

const fn execution_lane_text(value: u64) -> Option<&'static str> {
    match DiagnosticExecutionLane::known(value) {
        Some(DiagnosticExecutionLane::PublicRead) => Some("public-read"),
        Some(DiagnosticExecutionLane::TrustedRead) => Some("trusted-read"),
        Some(DiagnosticExecutionLane::Diagnostic) => Some("diagnostic"),
        Some(DiagnosticExecutionLane::Mutation) => Some("mutation"),
        Some(DiagnosticExecutionLane::Recovery) => Some("recovery"),
        None => None,
    }
}

const fn component_kind_text(value: u64) -> Option<&'static str> {
    match DiagnosticComponentKind::known(value) {
        Some(DiagnosticComponentKind::CommitDataKey) => Some("commit-data-key"),
        Some(DiagnosticComponentKind::IndexKey) => Some("index-key"),
        Some(DiagnosticComponentKind::IndexKeyComponent) => Some("index-key-component"),
        Some(DiagnosticComponentKind::RelationTargetPrimaryKey) => {
            Some("relation-target-primary-key")
        }
        None => None,
    }
}

const fn constraint_kind_text(value: u64) -> Option<&'static str> {
    match DiagnosticConstraintKind::known(value) {
        Some(DiagnosticConstraintKind::Check) => Some("check"),
        Some(DiagnosticConstraintKind::NotNull) => Some("not-null"),
        Some(DiagnosticConstraintKind::Relation) => Some("relation"),
        Some(DiagnosticConstraintKind::TargetedRule) => Some("targeted-rule"),
        Some(DiagnosticConstraintKind::Unique) => Some("unique"),
        None => None,
    }
}

const fn constraint_context_text(value: u64) -> Option<&'static str> {
    match DiagnosticConstraintContext::known(value) {
        Some(DiagnosticConstraintContext::Integrity) => Some("integrity"),
        Some(DiagnosticConstraintContext::MigrationValidation) => Some("migration-validation"),
        Some(DiagnosticConstraintContext::WriteAdmission) => Some("write-admission"),
        None => None,
    }
}

const fn mutation_operation_text(value: u64) -> Option<&'static str> {
    match DiagnosticMutationOperation::known(value) {
        Some(DiagnosticMutationOperation::Insert) => Some("insert"),
        Some(DiagnosticMutationOperation::Replace) => Some("replace"),
        Some(DiagnosticMutationOperation::Update) => Some("update"),
        Some(DiagnosticMutationOperation::Delete) => Some("delete"),
        None => None,
    }
}

fn parse_error_code(input: &str) -> Result<u16, String> {
    let trimmed = input.trim().trim_matches(['"', '\'']);
    let digits = match trimmed
        .strip_prefix('E')
        .or_else(|| trimmed.strip_prefix('e'))
    {
        Some(rest) => rest,
        None => trimmed,
    };

    if digits.is_empty() || !digits.chars().all(|ch| ch.is_ascii_digit()) {
        return Err(format!(
            "invalid IcyDB diagnostic code `{input}`; expected E7, 7, E190, or 190"
        ));
    }

    digits
        .parse::<u16>()
        .map_err(|_| format!("invalid IcyDB diagnostic code `{input}`; code does not fit u16"))
}

/// Render one compact public IcyDB error for CLI output.
pub(crate) fn render_error(err: &icydb::Error) -> String {
    let diagnostic = err.diagnostic();
    let code = diagnostic.code();
    let detail = diagnostic
        .detail()
        .copied()
        .map_or_else(|| code_text(code).to_string(), diagnostic_detail_text);
    let mut rendered_error = format!("{}: {detail}", code_label(code));

    let raw_facts = err
        .facts()
        .iter()
        .map(|fact| RawDiagnosticFact {
            tag: fact.tag(),
            value: fact.value(),
        })
        .collect::<Vec<_>>();
    let rendered = render_raw_facts(raw_facts.as_slice(), None);
    let raw_pairs = raw_facts
        .iter()
        .map(|fact| (fact.tag, fact.value))
        .collect::<Vec<_>>();
    let fact_mismatch = if raw_facts.is_empty() {
        None
    } else {
        icydb::diagnostic::validate_raw_diagnostic_fact_schema(err.code(), raw_pairs.as_slice())
            .err()
    };

    match (fact_mismatch, err.validated_query_field()) {
        (None, Ok(Some((role, field)))) => {
            rendered_error.push_str("; ");
            rendered_error.push_str(query_field_role_text(role));
            rendered_error.push_str(" field `");
            rendered_error.push_str(escape_query_field(field).as_str());
            rendered_error.push('`');
        }
        (_, Ok(None)) => {}
        (Some(_), Ok(Some(_))) => {
            rendered_error.push_str(
                "; query field context mismatch: diagnostic facts do not match the E-code",
            );
        }
        (_, Err(mismatch)) => {
            rendered_error.push_str("; query field context mismatch: ");
            rendered_error.push_str(query_field_schema_mismatch_text(mismatch));
        }
    }

    if !raw_facts.is_empty() {
        rendered_error.push_str("; facts ");
        rendered_error.push_str(rendered.as_str());
        if let Some(mismatch) = fact_mismatch {
            rendered_error.push_str("; fact context mismatch: ");
            rendered_error.push_str(fact_schema_mismatch_text(mismatch));
        }
    }

    rendered_error
}

const fn query_field_role_text(role: QueryFieldRole) -> &'static str {
    match role {
        QueryFieldRole::Predicate => "predicate",
        QueryFieldRole::Projection => "projection",
        QueryFieldRole::GroupBy => "group_by",
        QueryFieldRole::Having => "having",
        QueryFieldRole::OrderBy => "order_by",
        QueryFieldRole::AggregateTarget => "aggregate_target",
    }
}

const fn query_field_schema_mismatch_text(mismatch: QueryFieldSchemaMismatch) -> &'static str {
    match mismatch {
        QueryFieldSchemaMismatch::UnknownRole => "unknown role",
        QueryFieldSchemaMismatch::DisallowedCodeRole => "role is not allowed for this E-code",
        QueryFieldSchemaMismatch::EmptyField => "field is empty",
        QueryFieldSchemaMismatch::FieldTooLong => "field exceeds the 256-byte bound",
    }
}

fn escape_query_field(field: &str) -> String {
    let mut escaped = String::with_capacity(field.len());
    for character in field.chars() {
        match character {
            '\\' => escaped.push_str("\\\\"),
            '`' => escaped.push_str("\\`"),
            '\'' => escaped.push_str("\\'"),
            '"' => escaped.push_str("\\\""),
            '\n' => escaped.push_str("\\n"),
            '\r' => escaped.push_str("\\r"),
            '\t' => escaped.push_str("\\t"),
            character if character.is_control() => {
                for escaped_character in character.escape_unicode() {
                    escaped.push(escaped_character);
                }
            }
            character => escaped.push(character),
        }
    }
    escaped
}

const fn fact_tag_text(tag: icydb::diagnostic::DiagnosticFactTag) -> &'static str {
    use icydb::diagnostic::DiagnosticFactTag;

    match tag {
        DiagnosticFactTag::AcceptedSchemaFingerprintMethod => "accepted_schema_fingerprint_method",
        DiagnosticFactTag::AcceptedSchemaFingerprintHigh => "accepted_schema_fingerprint_high",
        DiagnosticFactTag::AcceptedSchemaFingerprintLow => "accepted_schema_fingerprint_low",
        DiagnosticFactTag::ExpectedFingerprintPrefix => "expected_fingerprint_prefix",
        DiagnosticFactTag::ActualFingerprintPrefix => "actual_fingerprint_prefix",
        DiagnosticFactTag::EntityTag => "entity_tag",
        DiagnosticFactTag::ExpectedEntityTag => "expected_entity_tag",
        DiagnosticFactTag::ActualEntityTag => "actual_entity_tag",
        DiagnosticFactTag::ConstraintId => "constraint_id",
        DiagnosticFactTag::FieldId => "field_id",
        DiagnosticFactTag::IndexId => "index_id",
        DiagnosticFactTag::RelationId => "relation_id",
        DiagnosticFactTag::MutationOperation => "mutation_operation",
        DiagnosticFactTag::RowOperation => "row_operation",
        DiagnosticFactTag::BatchPosition => "batch_position",
        DiagnosticFactTag::FirstBatchPosition => "first_batch_position",
        DiagnosticFactTag::DuplicateBatchPosition => "duplicate_batch_position",
        DiagnosticFactTag::ClauseIndex => "clause_index",
        DiagnosticFactTag::TermIndex => "term_index",
        DiagnosticFactTag::FirstTermIndex => "first_term_index",
        DiagnosticFactTag::DuplicateTermIndex => "duplicate_term_index",
        DiagnosticFactTag::ProjectionIndex => "projection_index",
        DiagnosticFactTag::GroupIndex => "group_index",
        DiagnosticFactTag::AggregateIndex => "aggregate_index",
        DiagnosticFactTag::ArgumentIndex => "argument_index",
        DiagnosticFactTag::BranchIndex => "branch_index",
        DiagnosticFactTag::ComponentIndex => "component_index",
        DiagnosticFactTag::ParameterIndex => "parameter_index",
        DiagnosticFactTag::SourceSpanStart => "source_span_start",
        DiagnosticFactTag::SourceSpanEnd => "source_span_end",
        DiagnosticFactTag::Expected => "expected",
        DiagnosticFactTag::Actual => "actual",
        DiagnosticFactTag::Minimum => "minimum",
        DiagnosticFactTag::Maximum => "maximum",
        DiagnosticFactTag::Limit => "limit",
        DiagnosticFactTag::ExpectedCount => "expected_count",
        DiagnosticFactTag::ActualCount => "actual_count",
        DiagnosticFactTag::CurrentCount => "current_count",
        DiagnosticFactTag::ProposedCount => "proposed_count",
        DiagnosticFactTag::ExpectedRevision => "expected_revision",
        DiagnosticFactTag::ActualRevision => "actual_revision",
        DiagnosticFactTag::CurrentRevision => "current_revision",
        DiagnosticFactTag::RequestedRevision => "requested_revision",
        DiagnosticFactTag::ExpectedVersion => "expected_version",
        DiagnosticFactTag::ActualVersion => "actual_version",
        DiagnosticFactTag::CurrentVersion => "current_version",
        DiagnosticFactTag::RequestedVersion => "requested_version",
        DiagnosticFactTag::ExpectedOffset => "expected_offset",
        DiagnosticFactTag::ActualOffset => "actual_offset",
        DiagnosticFactTag::ExpectedArity => "expected_arity",
        DiagnosticFactTag::ActualArity => "actual_arity",
        DiagnosticFactTag::ExpectedLength => "expected_length",
        DiagnosticFactTag::ActualLength => "actual_length",
        DiagnosticFactTag::ExpectedSlotCount => "expected_slot_count",
        DiagnosticFactTag::ActualSlotCount => "actual_slot_count",
        DiagnosticFactTag::RowLayout => "row_layout",
        DiagnosticFactTag::HistoryFloor => "history_floor",
        DiagnosticFactTag::CurrentLayout => "current_layout",
        DiagnosticFactTag::PhysicalSlot => "physical_slot",
        DiagnosticFactTag::PhysicalGeneration => "physical_generation",
        DiagnosticFactTag::ExpectedMemoryId => "expected_memory_id",
        DiagnosticFactTag::ActualMemoryId => "actual_memory_id",
        DiagnosticFactTag::ConstraintKind => "constraint_kind",
        DiagnosticFactTag::ConstraintContext => "constraint_context",
        DiagnosticFactTag::FieldKind => "field_kind",
        DiagnosticFactTag::ValueKind => "value_kind",
        DiagnosticFactTag::TypeFamily => "type_family",
        DiagnosticFactTag::FunctionKind => "function_kind",
        DiagnosticFactTag::OperatorKind => "operator_kind",
        DiagnosticFactTag::AggregateKind => "aggregate_kind",
        DiagnosticFactTag::KeyNamespaceKind => "key_namespace_kind",
        DiagnosticFactTag::ComponentKind => "component_kind",
        DiagnosticFactTag::MismatchKind => "mismatch_kind",
        DiagnosticFactTag::DecodeReason => "decode_reason",
        DiagnosticFactTag::BudgetResource => "budget_resource",
        DiagnosticFactTag::BacklogResource => "backlog_resource",
        DiagnosticFactTag::MigrationPhase => "migration_phase",
        DiagnosticFactTag::DatabaseControlRecordKind => "database_control_record_kind",
        DiagnosticFactTag::StateKind => "state_kind",
        DiagnosticFactTag::PayloadComponent => "payload_component",
        DiagnosticFactTag::ExpectedSignaturePrefix => "expected_signature_prefix",
        DiagnosticFactTag::ActualSignaturePrefix => "actual_signature_prefix",
        DiagnosticFactTag::FindingPosition => "finding_position",
        DiagnosticFactTag::RootField => "root_field",
        DiagnosticFactTag::RecordMember => "record_member",
        DiagnosticFactTag::TupleElement => "tuple_element",
        DiagnosticFactTag::Newtype => "newtype",
        DiagnosticFactTag::EnumVariant => "enum_variant",
        DiagnosticFactTag::ListElement => "list_element",
        DiagnosticFactTag::SetElement => "set_element",
        DiagnosticFactTag::MapEntryKey => "map_entry_key",
        DiagnosticFactTag::MapEntryValue => "map_entry_value",
        DiagnosticFactTag::ExecutionBudgetScope => "execution_budget_scope",
        DiagnosticFactTag::ExecutionLane => "execution_lane",
        DiagnosticFactTag::QueryShapeFingerprintPrefix => "query_shape_fingerprint_prefix",
    }
}

const fn class_text(class: ErrorClass) -> &'static str {
    match class {
        ErrorClass::Conflict => "conflict",
        ErrorClass::Corruption => "corruption",
        ErrorClass::IncompatiblePersistedFormat => "incompatible-persisted-format",
        ErrorClass::Internal => "internal",
        ErrorClass::InvariantViolation => "invariant-violation",
        ErrorClass::NotFound => "not-found",
        ErrorClass::Query => "query",
        ErrorClass::Unsupported => "unsupported",
    }
}

const fn origin_text(origin: ErrorOrigin) -> &'static str {
    match origin {
        ErrorOrigin::Cursor => "cursor",
        ErrorOrigin::Executor => "executor",
        ErrorOrigin::Identity => "identity",
        ErrorOrigin::Index => "index",
        ErrorOrigin::Interface => "interface",
        ErrorOrigin::Planner => "planner",
        ErrorOrigin::Query => "query",
        ErrorOrigin::Recovery => "recovery",
        ErrorOrigin::Response => "response",
        ErrorOrigin::Runtime => "runtime",
        ErrorOrigin::Serialize => "serialize",
        ErrorOrigin::Store => "store",
    }
}

fn diagnostic_detail_text(detail: DiagnosticDetail) -> String {
    match detail {
        DiagnosticDetail::QueryKind { kind } => query_kind_text(kind).to_string(),
        DiagnosticDetail::RuntimeKind { kind } => runtime_kind_text(kind).to_string(),
        DiagnosticDetail::RuntimeBoundary { boundary } => {
            runtime_boundary_text(boundary).to_string()
        }
        DiagnosticDetail::SchemaDdlAdmission { reason } => {
            format!("SQL DDL admission rejected: {}", schema_ddl_text(reason))
        }
        DiagnosticDetail::SchemaMigration { reason } => {
            format!(
                "schema migration rejected: {}",
                schema_migration_text(reason)
            )
        }
        DiagnosticDetail::UnsupportedSqlFeature { feature } => {
            format!("unsupported SQL feature: {}", sql_feature_text(feature))
        }
        DiagnosticDetail::SqlSurfaceMismatch { mismatch } => {
            sql_surface_mismatch_text(mismatch).to_string()
        }
        DiagnosticDetail::SqlWriteBoundary { boundary } => {
            format!("SQL write rejected: {}", sql_write_boundary_text(boundary))
        }
        DiagnosticDetail::QueryProjection { reason } => {
            format!(
                "query projection rejected: {}",
                query_projection_text(reason)
            )
        }
        DiagnosticDetail::QueryReadAdmission { reason } => {
            format!(
                "query read admission rejected: {}",
                query_read_admission_text(reason)
            )
        }
        DiagnosticDetail::SqlLowering { reason } => {
            format!("unsupported SQL lowering: {}", sql_lowering_text(reason))
        }
    }
}

const fn code_label(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "E_QUERY_VALIDATE",
        DiagnosticCode::QueryIntent => "E_QUERY_INTENT",
        DiagnosticCode::QueryPlan => "E_QUERY_PLAN",
        DiagnosticCode::QueryReadAdmission => "E_QUERY_READ_ADMISSION",
        DiagnosticCode::QueryUnorderedPagination => "E_QUERY_UNORDERED_PAGINATION",
        DiagnosticCode::QueryInvalidContinuationCursor => "E_QUERY_INVALID_CONTINUATION_CURSOR",
        DiagnosticCode::QueryNotFound => "E_QUERY_NOT_FOUND",
        DiagnosticCode::QueryNotUnique => "E_QUERY_NOT_UNIQUE",
        DiagnosticCode::QueryNumericOverflow => "E_QUERY_NUMERIC_OVERFLOW",
        DiagnosticCode::QueryNumericNotRepresentable => "E_QUERY_NUMERIC_NOT_REPRESENTABLE",
        DiagnosticCode::QueryUnknownAggregateTargetField => {
            "E_QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD"
        }
        DiagnosticCode::QueryUnsupportedProjection => "E_QUERY_UNSUPPORTED_PROJECTION",
        DiagnosticCode::QueryUnsupportedSqlFeature => "E_QUERY_UNSUPPORTED_SQL_FEATURE",
        DiagnosticCode::QuerySqlSurfaceMismatch => "E_QUERY_SQL_SURFACE_MISMATCH",
        DiagnosticCode::QuerySqlWriteBoundary => "E_QUERY_SQL_WRITE_BOUNDARY",
        DiagnosticCode::SchemaDdlAdmission => "E_SCHEMA_DDL_ADMISSION",
        DiagnosticCode::StoreNotFound => "E_STORE_NOT_FOUND",
        DiagnosticCode::StoreCorruption => "E_STORE_CORRUPTION",
        DiagnosticCode::StoreInvariantViolation => "E_STORE_INVARIANT_VIOLATION",
        DiagnosticCode::RuntimeCorruption => "E_RUNTIME_CORRUPTION",
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => {
            "E_RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT"
        }
        DiagnosticCode::RuntimeInvariantViolation => "E_RUNTIME_INVARIANT_VIOLATION",
        DiagnosticCode::RuntimeConflict => "E_RUNTIME_CONFLICT",
        DiagnosticCode::RuntimeNotFound => "E_RUNTIME_NOT_FOUND",
        DiagnosticCode::RuntimeUnsupported => "E_RUNTIME_UNSUPPORTED",
        DiagnosticCode::RuntimeInternal => "E_RUNTIME_INTERNAL",
    }
}

const fn code_text(code: DiagnosticCode) -> &'static str {
    match code {
        DiagnosticCode::QueryValidate => "query validation failed",
        DiagnosticCode::QueryIntent => "query intent is invalid",
        DiagnosticCode::QueryPlan => "query planning failed",
        DiagnosticCode::QueryReadAdmission => "query read admission rejected",
        DiagnosticCode::QueryUnorderedPagination => "pagination requires deterministic ordering",
        DiagnosticCode::QueryInvalidContinuationCursor => "continuation cursor is invalid",
        DiagnosticCode::QueryNotFound => "query expected one row but found none",
        DiagnosticCode::QueryNotUnique => "query expected one row but found multiple rows",
        DiagnosticCode::QueryNumericOverflow => "numeric operation overflowed",
        DiagnosticCode::QueryNumericNotRepresentable => "numeric result is not representable",
        DiagnosticCode::QueryUnknownAggregateTargetField => "unknown aggregate target field",
        DiagnosticCode::QueryUnsupportedProjection => "query projection is not supported",
        DiagnosticCode::QueryUnsupportedSqlFeature => "SQL feature is not supported",
        DiagnosticCode::QuerySqlSurfaceMismatch => "SQL statement used the wrong endpoint surface",
        DiagnosticCode::QuerySqlWriteBoundary => "SQL write boundary rejected",
        DiagnosticCode::SchemaDdlAdmission => "SQL DDL admission rejected",
        DiagnosticCode::StoreNotFound => "store key was not found",
        DiagnosticCode::StoreCorruption => "store corruption detected",
        DiagnosticCode::StoreInvariantViolation => "store invariant was violated",
        DiagnosticCode::RuntimeCorruption => "runtime corruption detected",
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => {
            "persisted data format is incompatible"
        }
        DiagnosticCode::RuntimeInvariantViolation => "runtime invariant was violated",
        DiagnosticCode::RuntimeConflict => "runtime conflict detected",
        DiagnosticCode::RuntimeNotFound => "runtime item was not found",
        DiagnosticCode::RuntimeUnsupported => "operation is not supported",
        DiagnosticCode::RuntimeInternal => "internal runtime failure",
    }
}

const fn query_kind_text(kind: QueryErrorKind) -> &'static str {
    match kind {
        QueryErrorKind::Validate => "query validation failed",
        QueryErrorKind::Intent => "query intent is invalid",
        QueryErrorKind::Plan => "query planning failed",
        QueryErrorKind::UnorderedPagination => "pagination requires deterministic ordering",
        QueryErrorKind::InvalidContinuationCursor => "continuation cursor is invalid",
        QueryErrorKind::NotFound => "query expected one row but found none",
        QueryErrorKind::NotUnique => "query expected one row but found multiple rows",
    }
}

const fn query_projection_text(reason: QueryProjectionCode) -> &'static str {
    match reason {
        QueryProjectionCode::NumericLiteralRequired => {
            "scalar numeric projection requires a numeric literal"
        }
        QueryProjectionCode::NumericScaleArguments => {
            "scale-taking numeric projections require a non-negative integer scale"
        }
        QueryProjectionCode::NestedFieldPathPreview => {
            "nested field-path projection preview is not supported"
        }
        QueryProjectionCode::CaseConditionBooleanRequired => {
            "CASE projection conditions must evaluate to boolean values"
        }
        QueryProjectionCode::NumericInputRequired => {
            "numeric projection functions require numeric inputs"
        }
        QueryProjectionCode::TextOrBlobInputRequired => {
            "this projection function requires text or blob input"
        }
        QueryProjectionCode::TextInputRequired => "text projection functions require text input",
        QueryProjectionCode::TextOrNullArgumentRequired => {
            "this projection function requires a text or NULL literal argument"
        }
        QueryProjectionCode::IntegerOrNullArgumentRequired => {
            "this projection function requires an integer or NULL literal argument"
        }
        QueryProjectionCode::UnaryOperandIncompatible => {
            "projection unary operator operand is incompatible"
        }
        QueryProjectionCode::BinaryOperandsIncompatible => {
            "projection binary operator operands are incompatible"
        }
    }
}

fn query_read_admission_text(reason: QueryReadAdmissionCode) -> String {
    format!(
        "{}; fix: {}",
        query_read_admission_reason_text(reason),
        query_read_admission_fix_text(reason),
    )
}

const fn query_read_admission_reason_text(reason: QueryReadAdmissionCode) -> &'static str {
    match reason {
        QueryReadAdmissionCode::PublicQueryRequiresLimit => {
            "public read queries require a bounded read intent"
        }
        QueryReadAdmissionCode::PublicQueryRequiresIndex => {
            "public read queries require an index-backed access path"
        }
        QueryReadAdmissionCode::UnboundedFullScanRejected => {
            "public read queries cannot execute an unbounded full scan"
        }
        QueryReadAdmissionCode::SortRequiresMaterialization => {
            "this read requires materializing rows for ORDER BY"
        }
        QueryReadAdmissionCode::GroupedQueryRequiresLimits => {
            "grouped reads require explicit group and memory budgets"
        }
        QueryReadAdmissionCode::GroupedQueryExceedsBudget => {
            "grouped read planning exceeds this endpoint's group budget"
        }
        QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute => {
            "diagnostic EXPLAIN lanes cannot execute rows"
        }
        QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy => {
            "the returned-row bound exceeds this endpoint's read budget"
        }
        QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy => {
            "primary-key input literals exceed this endpoint's read budget"
        }
    }
}

const fn query_read_admission_fix_text(reason: QueryReadAdmissionCode) -> &'static str {
    match reason {
        QueryReadAdmissionCode::PublicQueryRequiresLimit => {
            "add a positive limit within policy or use exact selected primary-key access"
        }
        QueryReadAdmissionCode::PublicQueryRequiresIndex
        | QueryReadAdmissionCode::UnboundedFullScanRejected => {
            "add a suitable index, tighten the predicate, or move the query behind a trusted admin endpoint"
        }
        QueryReadAdmissionCode::SortRequiresMaterialization => {
            "order by the selected index order, remove the sort, or keep the query on a trusted admin path"
        }
        QueryReadAdmissionCode::GroupedQueryRequiresLimits => {
            "add grouped_limits(max_groups, max_group_bytes) and keep DISTINCT aggregates within policy"
        }
        QueryReadAdmissionCode::GroupedQueryExceedsBudget => {
            "lower grouped_limits or split the report into a trusted/admin query"
        }
        QueryReadAdmissionCode::DiagnosticLaneDoesNotExecute => {
            "run EXPLAIN for diagnostics only, then execute through an admitted ordinary or trusted lane"
        }
        QueryReadAdmissionCode::ReturnedRowBoundExceedsPolicy => {
            "lower LIMIT or split the query into smaller cursor-paged reads"
        }
        QueryReadAdmissionCode::PrimaryKeyInputExceedsPolicy => {
            "reduce the primary-key IN list or move the read behind a trusted admin endpoint"
        }
    }
}

const fn sql_lowering_text(reason: SqlLoweringCode) -> &'static str {
    match reason {
        SqlLoweringCode::EntityMismatch => {
            "statement target entity does not match the requested entity"
        }
        SqlLoweringCode::SelectProjectionShape => "unsupported SELECT projection shape",
        SqlLoweringCode::SelectDistinct => "unsupported SELECT DISTINCT shape",
        SqlLoweringCode::DistinctOrderByProjection => {
            "SELECT DISTINCT ORDER BY terms must be derivable from the projected tuple"
        }
        SqlLoweringCode::GlobalAggregateProjection => {
            "unsupported global aggregate projection shape"
        }
        SqlLoweringCode::GlobalAggregateGroupBy => "global aggregate SQL does not support GROUP BY",
        SqlLoweringCode::SelectGroupByShape => "unsupported SELECT GROUP BY shape",
        SqlLoweringCode::GroupedProjectionExplicitListRequired => {
            "grouped SELECT requires an explicit projection list"
        }
        SqlLoweringCode::GroupedProjectionAggregateRequired => {
            "grouped SELECT projection must include at least one aggregate expression"
        }
        SqlLoweringCode::GroupedProjectionNonGroupField => {
            "grouped projection references fields outside GROUP BY keys"
        }
        SqlLoweringCode::GroupedProjectionScalarAfterAggregate => {
            "grouped projection scalar expression appears after aggregate expressions"
        }
        SqlLoweringCode::HavingRequiresGroupBy => "HAVING requires GROUP BY",
        SqlLoweringCode::SelectHavingShape => "unsupported SQL HAVING shape",
        SqlLoweringCode::AggregateInputExpressions => {
            "aggregate input expressions are not executable in this release"
        }
        SqlLoweringCode::WhereExpressionShape => "unsupported SQL WHERE expression shape",
        SqlLoweringCode::ParameterPlacement => "unsupported SQL parameter placement",
        SqlLoweringCode::SqlDdlExecutionUnsupported => {
            "SQL DDL execution is not supported in this release"
        }
    }
}

const fn runtime_kind_text(kind: RuntimeErrorKind) -> &'static str {
    match kind {
        RuntimeErrorKind::Corruption => "runtime corruption detected",
        RuntimeErrorKind::IncompatiblePersistedFormat => "persisted data format is incompatible",
        RuntimeErrorKind::InvariantViolation => "runtime invariant was violated",
        RuntimeErrorKind::Conflict => "runtime conflict detected",
        RuntimeErrorKind::NotFound => "runtime item was not found",
        RuntimeErrorKind::Unsupported => "operation is not supported",
        RuntimeErrorKind::Internal => "internal runtime failure",
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the exhaustive public runtime-boundary vocabulary is clearer in one match"
)]
const fn runtime_boundary_text(boundary: RuntimeBoundaryCode) -> &'static str {
    match boundary {
        RuntimeBoundaryCode::SqlSurfaceControllerRequired => {
            "SQL endpoint requires controller access"
        }
        RuntimeBoundaryCode::SchemaSurfaceControllerRequired => {
            "schema endpoint requires controller access"
        }
        RuntimeBoundaryCode::OperationalSurfaceControllerRequired => {
            "operational endpoint requires controller access"
        }
        RuntimeBoundaryCode::SqlQueryNoConfiguredEntities => {
            "SQL query endpoint has no configured entities"
        }
        RuntimeBoundaryCode::SqlQueryEntityNotFound => {
            "SQL query target entity was not found in the accepted schema"
        }
        RuntimeBoundaryCode::SqlQueryReplyBytesExceeded => {
            "SQL query result exceeds the public reply byte limit"
        }
        RuntimeBoundaryCode::DatabaseStartupRecoveryPending => {
            "database startup recovery is still in progress"
        }
        RuntimeBoundaryCode::ConvergenceBacklogPressure => {
            "journal convergence backlog exceeds its cumulative admission limit"
        }
        RuntimeBoundaryCode::SqlSurfacePolicyDenied => {
            "application policy denied access to the SQL endpoint"
        }
        RuntimeBoundaryCode::SchemaSurfacePolicyDenied => {
            "application policy denied access to the schema endpoint"
        }
        RuntimeBoundaryCode::SqlDdlTargetRequired => "SQL DDL requires one target entity",
        RuntimeBoundaryCode::SqlDdlEntityNotConfigured => {
            "SQL DDL target entity is not configured for this canister"
        }
        RuntimeBoundaryCode::SqlIntrospectionDisabled => {
            "SQL introspection is disabled for this canister build target"
        }
        RuntimeBoundaryCode::MutationRequiredFieldMissing => {
            "mutation is missing one or more required fields"
        }
        RuntimeBoundaryCode::MutationManagedTimestampRegression => {
            "mutation operation time precedes an accepted managed timestamp"
        }
        RuntimeBoundaryCode::PersistedRowLayoutOutsideAcceptedWindow => {
            "persisted row layout is outside the accepted layout window"
        }
        RuntimeBoundaryCode::PersistedRowSlotCountMismatch => {
            "persisted row slot count does not match its stamped layout"
        }
        RuntimeBoundaryCode::GeneratedFieldAfterDdlField => {
            "generated field would collide with an accepted SQL DDL field slot"
        }
        RuntimeBoundaryCode::JournalMutationRevisionExhausted => {
            "journaled mutation revision space is exhausted"
        }
        RuntimeBoundaryCode::ConstraintViolation => {
            "mutation violates an accepted constraint or activation gate"
        }
        RuntimeBoundaryCode::AcceptedRowConstraintProgramCorrupt => {
            "accepted row-constraint program is corrupt"
        }
        RuntimeBoundaryCode::ConstraintActivationWriteBlocked => {
            "write conflicts with an incomplete constraint activation"
        }
        RuntimeBoundaryCode::GeneratedConstraintActivationStale => {
            "generated constraint proposal no longer matches its live activation"
        }
        RuntimeBoundaryCode::MutationDatabaseOwnedFieldExplicit => {
            "mutation explicitly authors a database-owned field"
        }
        RuntimeBoundaryCode::MutationBatchEmpty => "structural mutation batch is empty",
        RuntimeBoundaryCode::MutationBatchTooManyItems => {
            "structural mutation batch exceeds the operation-count bound"
        }
        RuntimeBoundaryCode::MutationBatchStagedBytesExceeded => {
            "structural mutation batch exceeds the staged-byte bound"
        }
        RuntimeBoundaryCode::MutationBatchResultBytesExceeded => {
            "structural mutation result exceeds the encoded response bound"
        }
        RuntimeBoundaryCode::MutationBatchCommitWorkExceeded => {
            "structural mutation batch exceeds the prepared-commit work bound"
        }
        RuntimeBoundaryCode::MutationBatchStoreMismatch => {
            "structural mutation batch crosses an accepted store boundary"
        }
        RuntimeBoundaryCode::MutationBatchTooManyEntities => {
            "structural mutation batch exceeds the distinct-entity bound"
        }
        RuntimeBoundaryCode::MutationBatchDuplicateKey => {
            "structural mutation batch targets the same accepted key more than once"
        }
        RuntimeBoundaryCode::ExactKeyBatchTooManyItems => {
            "exact-key batch exceeds the input item-count bound"
        }
        RuntimeBoundaryCode::ExactKeyBatchInputBytesExceeded => {
            "exact-key batch exceeds the encoded input-key byte bound"
        }
        RuntimeBoundaryCode::ExactKeyBatchStoredBytesExceeded => {
            "exact-key batch exceeds the distinct stored-row byte bound"
        }
        RuntimeBoundaryCode::ExactKeyBatchResultBytesExceeded => {
            "exact-key batch exceeds the logical result byte bound"
        }
        RuntimeBoundaryCode::ExecutionBudgetExceeded => {
            "charged database work exceeds its hard execution budget"
        }
        RuntimeBoundaryCode::PageUnitTooLarge => {
            "one scalar-page unit exceeds its resumable page-work envelope"
        }
        RuntimeBoundaryCode::RequestExecutionScopeRequired => {
            "no IcyDB request-execution scope is active; wrap the entry point with #[icydb::request_execution], #[icydb::test], or with_request_execution"
        }
        RuntimeBoundaryCode::RequestExecutionRootMismatch => {
            "explicit IcyDB request root conflicts with the active request root"
        }
    }
}

const fn schema_ddl_text(reason: SchemaDdlAdmissionCode) -> &'static str {
    match reason {
        SchemaDdlAdmissionCode::MissingExpectedSchemaVersion => "missing EXPECT SCHEMA VERSION",
        SchemaDdlAdmissionCode::MissingNextSchemaVersion => "missing SET SCHEMA VERSION",
        SchemaDdlAdmissionCode::StaleExpectedSchemaVersion => "expected schema version is stale",
        SchemaDdlAdmissionCode::InvalidExpectedSchemaVersion => {
            "expected schema version is invalid"
        }
        SchemaDdlAdmissionCode::InvalidNextSchemaVersion => "next schema version is invalid",
        SchemaDdlAdmissionCode::AcceptedSchemaChangeWithoutVersionBump => {
            "accepted schema changed without a version bump"
        }
        SchemaDdlAdmissionCode::EmptyVersionBump => "schema version bump has no schema change",
        SchemaDdlAdmissionCode::VersionGap => "schema version gap is not allowed",
        SchemaDdlAdmissionCode::VersionRollback => "schema version rollback is not allowed",
        SchemaDdlAdmissionCode::FingerprintMethodMismatch => {
            "schema fingerprint method versions do not match"
        }
        SchemaDdlAdmissionCode::UnsupportedTransitionClass => {
            "DDL transition class is not supported"
        }
        SchemaDdlAdmissionCode::PhysicalRunnerMissing => {
            "required physical runner capability is missing"
        }
        SchemaDdlAdmissionCode::ValidationFailed => "candidate schema validation failed",
        SchemaDdlAdmissionCode::PublicationRaceLost => "accepted schema changed after DDL binding",
        SchemaDdlAdmissionCode::InvalidAddColumnDefault => {
            "ADD COLUMN default value is not encodable"
        }
        SchemaDdlAdmissionCode::InvalidAlterColumnDefault => {
            "ALTER COLUMN SET DEFAULT value is not encodable"
        }
        SchemaDdlAdmissionCode::GeneratedIndexDropRejected => {
            "generated index cannot be dropped by SQL DDL"
        }
        SchemaDdlAdmissionCode::SchemaRewriteRequiresMigration => {
            "nonempty physical schema rewrite requires a migration"
        }
        SchemaDdlAdmissionCode::SchemaTransitionBudgetExceeded => {
            "schema transition exceeded its bounded resource budget"
        }
        SchemaDdlAdmissionCode::GeneratedFieldDefaultChangeRejected => {
            "generated field default cannot be changed by SQL DDL"
        }
        SchemaDdlAdmissionCode::GeneratedFieldNullabilityChangeRejected => {
            "generated field nullability cannot be changed by SQL DDL"
        }
        SchemaDdlAdmissionCode::RowLayoutVersionExhausted => {
            "row-layout version space is exhausted"
        }
    }
}

const fn schema_migration_text(reason: SchemaMigrationCode) -> &'static str {
    match reason {
        SchemaMigrationCode::Unadopted => "accepted generated entities are not adopted",
        SchemaMigrationCode::MissingMigration => "a required immediate migration is missing",
        SchemaMigrationCode::VersionGap => "an entity source version skips its predecessor",
        SchemaMigrationCode::Downgrade => "an entity source version moves backward",
        SchemaMigrationCode::EmptyEntityVersionBump => {
            "an entity source version changed without a schema change"
        }
        SchemaMigrationCode::StaleAcceptedHead => "the accepted schema head changed",
        SchemaMigrationCode::PlanChanged => "the deployed migration plan changed",
        SchemaMigrationCode::UnknownFromObject => "a rename source is not accepted",
        SchemaMigrationCode::UnknownToObject => "a rename target is not declared",
        SchemaMigrationCode::KindMismatch => "a migration object kind does not match",
        SchemaMigrationCode::IdentityConflict => "accepted migration identity conflicts",
        SchemaMigrationCode::UnexplainedSchemaDifference => {
            "the proposal contains an unexplained schema difference"
        }
        SchemaMigrationCode::UnsupportedTransform => "the declared transform is unsupported",
        SchemaMigrationCode::PhysicalRunnerMissing => {
            "the required physical migration runner is unavailable"
        }
        SchemaMigrationCode::MigrationInProgress => "a schema migration is already in progress",
        SchemaMigrationCode::AbortTooLate => "row rewriting has begun and abort is no longer safe",
        SchemaMigrationCode::ProgressCorrupt => "durable migration progress is corrupt",
        SchemaMigrationCode::CandidateMismatch => {
            "durable candidate state does not match the deployed plan"
        }
        SchemaMigrationCode::PublicationRaceLost => {
            "accepted migration authority changed before publication"
        }
    }
}

const fn sql_surface_mismatch_text(mismatch: SqlSurfaceMismatchCode) -> &'static str {
    match mismatch {
        SqlSurfaceMismatchCode::QueryRejectsInsert => {
            "execute_trusted_sql_query rejects INSERT; use execute_trusted_sql_mutation()"
        }
        SqlSurfaceMismatchCode::QueryRejectsUpdate => {
            "execute_trusted_sql_query rejects UPDATE; use execute_trusted_sql_exact_update() or execute_trusted_sql_prefix_update()"
        }
        SqlSurfaceMismatchCode::QueryRejectsDelete => {
            "execute_trusted_sql_query rejects DELETE; use execute_trusted_sql_mutation()"
        }
        SqlSurfaceMismatchCode::MutationRejectsSelect => {
            "execute_trusted_sql_mutation rejects SELECT; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsExplain => {
            "execute_trusted_sql_mutation rejects EXPLAIN; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsDescribe => {
            "execute_trusted_sql_mutation rejects DESCRIBE; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsShowIndexes => {
            "execute_trusted_sql_mutation rejects SHOW INDEXES; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsShowConstraints => {
            "execute_trusted_sql_mutation rejects SHOW CONSTRAINTS; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsShowRelations => {
            "execute_trusted_sql_mutation rejects SHOW RELATIONS; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsShowColumns => {
            "execute_trusted_sql_mutation rejects SHOW COLUMNS; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsShowEntities => {
            "execute_trusted_sql_mutation rejects SHOW ENTITIES; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsShowStores => {
            "execute_trusted_sql_mutation rejects SHOW STORES; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRejectsShowMemory => {
            "execute_trusted_sql_mutation rejects SHOW MEMORY; use execute_trusted_sql_query()"
        }
        SqlSurfaceMismatchCode::MutationRequiresExplicitUpdateIntent => {
            "execute_trusted_sql_mutation rejects UPDATE; use execute_trusted_sql_exact_update() or execute_trusted_sql_prefix_update()"
        }
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the exhaustive governed SQL write-code map is clearer as one typed lookup"
)]
const fn sql_write_boundary_text(boundary: SqlWriteBoundaryCode) -> &'static str {
    match boundary {
        SqlWriteBoundaryCode::PrimaryKeyLiteralIncompatible => {
            "primary key literal is not compatible with the entity key type"
        }
        SqlWriteBoundaryCode::MissingPrimaryKey => "INSERT is missing required primary key fields",
        SqlWriteBoundaryCode::MissingRequiredFields => {
            "INSERT is missing required non-generated fields"
        }
        SqlWriteBoundaryCode::ExplicitManagedField => {
            "explicit writes to managed fields are not allowed"
        }
        SqlWriteBoundaryCode::ExplicitGeneratedField => {
            "explicit writes to generated fields are not allowed"
        }
        SqlWriteBoundaryCode::InsertSelectRequiresScalar => {
            "INSERT SELECT requires a scalar SELECT source"
        }
        SqlWriteBoundaryCode::InsertSelectAggregateProjection => {
            "INSERT SELECT does not support aggregate source projections"
        }
        SqlWriteBoundaryCode::InsertSelectWidthMismatch => {
            "INSERT SELECT projection width must match the target column list"
        }
        SqlWriteBoundaryCode::UpdatePrimaryKeyMutation => "UPDATE cannot mutate primary key fields",
        SqlWriteBoundaryCode::InvalidFieldLiteral => {
            "SQL write literal is not compatible with the target field type"
        }
        SqlWriteBoundaryCode::UnknownReturningField => {
            "RETURNING references a field that does not exist on the target entity"
        }
        SqlWriteBoundaryCode::DuplicateReturningField => {
            "RETURNING field lists cannot repeat the same target field"
        }
        SqlWriteBoundaryCode::UpdateMissingWherePredicate => "UPDATE requires a WHERE predicate",
        SqlWriteBoundaryCode::WriteOrderByUnsupportedShape => {
            "SQL write ORDER BY only supports direct field targets"
        }
        SqlWriteBoundaryCode::ReturningResponseTooLarge => {
            "UPDATE RETURNING response exceeds this endpoint's response-size budget"
        }
        SqlWriteBoundaryCode::ReturningRowsTooMany => {
            "UPDATE RETURNING emits more rows than this endpoint's row budget"
        }
        SqlWriteBoundaryCode::StagedRowsTooMany => {
            "SQL write stages more rows than this endpoint's row budget"
        }
        SqlWriteBoundaryCode::InsertDefaultRequiredField => {
            "INSERT DEFAULT cannot resolve a required ordinary field"
        }
        SqlWriteBoundaryCode::UpdateDefaultRequiredField => {
            "UPDATE DEFAULT cannot resolve a required ordinary field"
        }
        SqlWriteBoundaryCode::UpdateDefaultDatabaseOwnedField => {
            "UPDATE DEFAULT cannot assign a generated or managed field"
        }
        SqlWriteBoundaryCode::ExactUpdateAssertionRequired => {
            "exact UPDATE requires a positive require_affected_at_most assertion"
        }
        SqlWriteBoundaryCode::ExactUpdateAssertionTooHigh => {
            "exact UPDATE assertion exceeds the engine row ceiling"
        }
        SqlWriteBoundaryCode::ExactUpdateAffectedRowsExceeded => {
            "exact UPDATE matched more rows than require_affected_at_most"
        }
        SqlWriteBoundaryCode::ExactUpdateWindowUnsupported => {
            "exact UPDATE rejects LIMIT, OFFSET, and non-primary-key ordering"
        }
        SqlWriteBoundaryCode::ExactUpdateScanBudgetExceeded => {
            "exact UPDATE selection exceeded the engine scan budget"
        }
        SqlWriteBoundaryCode::ResumableUpdateWindowUnsupported => {
            "resumable UPDATE rejects LIMIT, OFFSET, and non-primary-key ordering"
        }
        SqlWriteBoundaryCode::ResumableUpdateReturningUnsupported => {
            "resumable UPDATE does not support row RETURNING"
        }
        SqlWriteBoundaryCode::ResumableUpdateRequiresJournaledStore => {
            "resumable UPDATE requires a journaled store"
        }
        SqlWriteBoundaryCode::ResumableUpdateAssignedFieldHasGlobalConstraint => {
            "resumable UPDATE cannot assign a unique- or relation-owned field"
        }
        SqlWriteBoundaryCode::ResumableUpdateScopeDependsOnAssignedField => {
            "resumable UPDATE scope cannot depend on an assigned field"
        }
        SqlWriteBoundaryCode::ResumableUpdateScopeDependencyUnknown => {
            "resumable UPDATE scope dependencies could not be proven"
        }
        SqlWriteBoundaryCode::ResumableUpdateContinuationMalformed => {
            "resumable UPDATE continuation is malformed or not current"
        }
        SqlWriteBoundaryCode::ResumableUpdateContinuationTargetMismatch => {
            "resumable UPDATE continuation belongs to another target"
        }
        SqlWriteBoundaryCode::ResumableUpdateContinuationSchemaMismatch => {
            "resumable UPDATE continuation belongs to another accepted schema"
        }
        SqlWriteBoundaryCode::ResumableUpdateContinuationScopeMismatch => {
            "resumable UPDATE continuation belongs to another scope"
        }
        SqlWriteBoundaryCode::ResumableUpdateContinuationPatchMismatch => {
            "resumable UPDATE continuation belongs to another fixed patch"
        }
        SqlWriteBoundaryCode::ResumableUpdateContinuationBatchPolicyMismatch => {
            "resumable UPDATE continuation uses another engine batch policy"
        }
        SqlWriteBoundaryCode::ResumableUpdateManagedFieldHasGlobalConstraint => {
            "resumable UPDATE cannot refresh a globally constrained managed field"
        }
    }
}

const fn sql_feature_text(feature: SqlFeatureCode) -> &'static str {
    match feature {
        SqlFeatureCode::AggregateFilterClause => "aggregate FILTER clauses",
        SqlFeatureCode::AlterStatementBeyondAlterTable
        | SqlFeatureCode::AlterTableAddColumnDuplicateDefault
        | SqlFeatureCode::AlterTableAddColumnModifiers
        | SqlFeatureCode::AlterTableAddStatementBeyondAddColumn
        | SqlFeatureCode::AlterTableAddConstraintBeyondCheck
        | SqlFeatureCode::AlterTableAddConstraintModifiers
        | SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction
        | SqlFeatureCode::AlterTableAlterColumnModifiers
        | SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction
        | SqlFeatureCode::AlterTableAlterColumnUnsupportedAction
        | SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn
        | SqlFeatureCode::AlterTableDropColumnIfExistsSyntax
        | SqlFeatureCode::AlterTableDropColumnModifiers
        | SqlFeatureCode::AlterTableDropStatementBeyondDropColumn
        | SqlFeatureCode::AlterTableDropConstraintIfExistsSyntax
        | SqlFeatureCode::AlterTableDropConstraintModifiers
        | SqlFeatureCode::AlterTableRenameColumnMissingTo
        | SqlFeatureCode::AlterTableRenameColumnModifiers
        | SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn
        | SqlFeatureCode::AlterTableValidateBeyondConstraint
        | SqlFeatureCode::AlterTableValidateConstraintModifiers
        | SqlFeatureCode::AlterTableUnsupportedOperation
        | SqlFeatureCode::CreateIndexIfNotExistsSyntax
        | SqlFeatureCode::CreateIndexKeyOrderingModifiers
        | SqlFeatureCode::CreateIndexModifiers
        | SqlFeatureCode::CreateStatementBeyondCreateIndex
        | SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause
        | SqlFeatureCode::DdlSchemaVersionDuplicateSetClause
        | SqlFeatureCode::DropIndexModifiers
        | SqlFeatureCode::DropIndexIfExistsSyntax
        | SqlFeatureCode::DropStatementBeyondDropIndex
        | SqlFeatureCode::ExpressionIndexUnsupportedFunction => sql_ddl_feature_text(feature),
        SqlFeatureCode::ColumnAlias => "column or expression aliases",
        SqlFeatureCode::DescribeModifier => "DESCRIBE modifiers",
        SqlFeatureCode::Having => "HAVING",
        SqlFeatureCode::Insert => "INSERT",
        SqlFeatureCode::Join => "JOIN",
        SqlFeatureCode::LikePatternBeyondTrailingPrefix => {
            "LIKE patterns beyond trailing '%' prefix form"
        }
        SqlFeatureCode::LowerFieldPredicateUnsupported => {
            "LOWER(field) predicate forms beyond LIKE 'prefix%' or ordered text bounds"
        }
        SqlFeatureCode::MultiStatementSql => "multi-statement SQL input",
        SqlFeatureCode::NestedAggregateInput => {
            "nested aggregate references inside aggregate input expressions"
        }
        SqlFeatureCode::NestedProjectionFunctionInArithmetic => {
            "nested projection functions inside arithmetic expressions"
        }
        SqlFeatureCode::NumericScaleFunctionArguments => {
            "scale-taking numeric function arguments beyond supported literal integer scale"
        }
        SqlFeatureCode::OrderByFieldNotOrderable => {
            "ORDER BY fields whose accepted catalog type is not orderable"
        }
        SqlFeatureCode::OrderByUnsupportedForm => "unsupported ORDER BY expression form",
        SqlFeatureCode::Other => "unsupported SQL feature",
        SqlFeatureCode::PredicateStartsWithFirstArgument => {
            "STARTS_WITH first argument forms beyond plain or LOWER field wrappers"
        }
        SqlFeatureCode::QuotedIdentifiers => "quoted identifiers",
        SqlFeatureCode::ReturningUnsupportedShape => "unsupported RETURNING shape",
        SqlFeatureCode::ScalarFunctionExpressionPosition => {
            "functions beyond supported scalar forms in this expression position"
        }
        SqlFeatureCode::ScaleTakingNumericFunctionExpressionPosition => {
            "scale-taking numeric functions in this expression position"
        }
        SqlFeatureCode::ShowColumnsModifiers => "SHOW COLUMNS modifiers",
        SqlFeatureCode::ShowConstraintsModifiers => "SHOW CONSTRAINTS modifiers",
        SqlFeatureCode::ShowEntitiesModifiers => "SHOW ENTITIES modifiers",
        SqlFeatureCode::ShowIndexesModifiers => "SHOW INDEXES modifiers",
        SqlFeatureCode::ShowMemoryModifiers => "SHOW MEMORY modifiers",
        SqlFeatureCode::ShowRelationsModifiers => "SHOW RELATIONS modifiers",
        SqlFeatureCode::ShowStoresModifiers => "SHOW STORES modifiers",
        SqlFeatureCode::ShowUnsupportedCommand => "unsupported SHOW command",
        SqlFeatureCode::SimpleCaseExpression => "simple CASE expressions",
        SqlFeatureCode::StandaloneLiteralProjectionItem => "standalone literal projection items",
        SqlFeatureCode::UnionIntersectExcept => "UNION, INTERSECT, or EXCEPT",
        SqlFeatureCode::UnsupportedFunctionNamespace => "unsupported SQL function namespace",
        SqlFeatureCode::Update => "UPDATE",
        SqlFeatureCode::UpperFieldPredicateUnsupported => {
            "UPPER(field) in reduced predicate-only contracts"
        }
        SqlFeatureCode::WindowFunction => "window functions",
        SqlFeatureCode::With => "WITH",
    }
}

const fn sql_ddl_feature_text(feature: SqlFeatureCode) -> &'static str {
    match feature {
        SqlFeatureCode::AlterStatementBeyondAlterTable => "ALTER statements beyond ALTER TABLE",
        SqlFeatureCode::AlterTableAddColumnDuplicateDefault => {
            "duplicate ALTER TABLE ADD COLUMN DEFAULT clauses"
        }
        SqlFeatureCode::AlterTableAddColumnModifiers => "ALTER TABLE ADD COLUMN modifiers",
        SqlFeatureCode::AlterTableAddStatementBeyondAddColumn => {
            "ALTER TABLE ADD statements beyond ADD COLUMN"
        }
        SqlFeatureCode::AlterTableAddConstraintBeyondCheck => {
            "ALTER TABLE ADD CONSTRAINT kinds beyond CHECK"
        }
        SqlFeatureCode::AlterTableAddConstraintModifiers => "ALTER TABLE ADD CONSTRAINT modifiers",
        SqlFeatureCode::AlterTableAlterColumnDropUnsupportedAction => {
            "ALTER TABLE ALTER COLUMN DROP actions beyond DEFAULT and NOT NULL"
        }
        SqlFeatureCode::AlterTableAlterColumnModifiers => "ALTER TABLE ALTER COLUMN modifiers",
        SqlFeatureCode::AlterTableAlterColumnSetUnsupportedAction => {
            "ALTER TABLE ALTER COLUMN SET actions beyond DEFAULT and NOT NULL"
        }
        SqlFeatureCode::AlterTableAlterColumnUnsupportedAction => {
            "ALTER TABLE ALTER COLUMN actions beyond SET/DROP DEFAULT and SET/DROP NOT NULL"
        }
        SqlFeatureCode::AlterTableAlterStatementBeyondAlterColumn => {
            "ALTER TABLE ALTER statements beyond ALTER COLUMN"
        }
        SqlFeatureCode::AlterTableDropColumnIfExistsSyntax => {
            "ALTER TABLE DROP COLUMN IF EXISTS syntax"
        }
        SqlFeatureCode::AlterTableDropColumnModifiers => "ALTER TABLE DROP COLUMN modifiers",
        SqlFeatureCode::AlterTableDropStatementBeyondDropColumn => {
            "ALTER TABLE DROP statements beyond DROP COLUMN"
        }
        SqlFeatureCode::AlterTableDropConstraintIfExistsSyntax => {
            "ALTER TABLE DROP CONSTRAINT IF EXISTS syntax"
        }
        SqlFeatureCode::AlterTableDropConstraintModifiers => {
            "ALTER TABLE DROP CONSTRAINT modifiers"
        }
        SqlFeatureCode::AlterTableRenameColumnMissingTo => "ALTER TABLE RENAME COLUMN without TO",
        SqlFeatureCode::AlterTableRenameColumnModifiers => "ALTER TABLE RENAME COLUMN modifiers",
        SqlFeatureCode::AlterTableRenameStatementBeyondRenameColumn => {
            "ALTER TABLE RENAME statements beyond RENAME COLUMN"
        }
        SqlFeatureCode::AlterTableValidateBeyondConstraint => {
            "ALTER TABLE VALIDATE operations beyond constraints"
        }
        SqlFeatureCode::AlterTableValidateConstraintModifiers => {
            "ALTER TABLE VALIDATE CONSTRAINT modifiers"
        }
        SqlFeatureCode::AlterTableUnsupportedOperation => "unsupported ALTER TABLE operation",
        SqlFeatureCode::CreateIndexIfNotExistsSyntax => "CREATE INDEX IF NOT EXISTS syntax",
        SqlFeatureCode::CreateIndexKeyOrderingModifiers => "CREATE INDEX key ordering modifiers",
        SqlFeatureCode::CreateIndexModifiers => "CREATE INDEX modifiers",
        SqlFeatureCode::CreateStatementBeyondCreateIndex => "CREATE statements beyond CREATE INDEX",
        SqlFeatureCode::DdlSchemaVersionDuplicateExpectedClause => {
            "duplicate EXPECT SCHEMA VERSION clauses"
        }
        SqlFeatureCode::DdlSchemaVersionDuplicateSetClause => {
            "duplicate SET SCHEMA VERSION clauses"
        }
        SqlFeatureCode::DropIndexModifiers => "DROP INDEX modifiers",
        SqlFeatureCode::DropIndexIfExistsSyntax => "DROP INDEX IF EXISTS syntax",
        SqlFeatureCode::DropStatementBeyondDropIndex => "DROP statements beyond DROP INDEX",
        SqlFeatureCode::ExpressionIndexUnsupportedFunction => {
            "expression index functions beyond LOWER, UPPER, and TRIM"
        }
        _ => "unsupported SQL feature",
    }
}

#[cfg(test)]
mod tests {
    use super::{
        RawDiagnosticFact, artifact::DiagnosticSchemaArtifact, parse_error_code, render_error,
        render_error_code_report, render_error_code_report_with_facts,
    };

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
            "line\n\0`\"'\u{1b}\\",
            serde_json::json!([]),
        );
        let rendered = render_error(&error);

        assert_eq!(
            rendered,
            "E_QUERY_PLAN: query planning failed; projection field `line\\n\\u{0}\\`\\\"\\'\\u{1b}\\\\`"
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
        let report = render_error_code_report("E6").expect("E6 should parse");

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
            render_error_code_report_with_facts("E210", facts.as_slice(), &[&artifact], &mut notes)
                .expect("exact diagnostic should render");
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
        let report = render_error_code_report_with_facts(
            "E210",
            stale_facts.as_slice(),
            &[&artifact],
            &mut notes,
        )
        .expect("stale diagnostic should render numerically");
        assert!(!report.contains("Account"), "{report}");
        assert!(report.contains("names withheld"), "{report}");

        let mut malformed_facts = facts;
        malformed_facts.swap(4, 5);
        let mut notes = Vec::new();
        let report = render_error_code_report_with_facts(
            "E210",
            malformed_facts.as_slice(),
            &[&artifact],
            &mut notes,
        )
        .expect("malformed known context should remain numerically renderable");
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
        let report = render_error_code_report_with_facts("E210", facts.as_slice(), &[], &mut notes)
            .expect("numeric diagnostic should render");

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
            render_error_code_report_with_facts("E210", facts.as_slice(), &[&metadata], &mut notes)
                .expect("exact source-bound diagnostic should render");

        assert!(report.contains("entity_tag=42(SourceAccount)"), "{report}");
        assert!(
            report.contains("constraint_id=3(source_account_name_unique)"),
            "{report}"
        );

        let deployment = DiagnosticSchemaArtifact::test_fixture();
        let mut notes = Vec::new();
        let report = render_error_code_report_with_facts(
            "E210",
            facts.as_slice(),
            &[&deployment, &metadata],
            &mut notes,
        )
        .expect("higher-priority deployment metadata should render");
        assert!(report.contains("entity_tag=42(Account)"), "{report}");
        assert!(!report.contains("SourceAccount"), "{report}");

        let mut stale_facts = facts;
        stale_facts[2].value = u64::from_be_bytes([8; 8]);
        let mut notes = Vec::new();
        let report = render_error_code_report_with_facts(
            "E210",
            stale_facts.as_slice(),
            &[&metadata],
            &mut notes,
        )
        .expect("stale source metadata should fall back numerically");
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
                    "value": icydb::diagnostic::DiagnosticDecodeReason::CursorInvalidHex.raw(),
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
        let report = render_error_code_report("173").expect("173 should parse");

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
        let report = render_error_code_report("9999").expect("numeric code should parse");

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

    fn render_query_read_admission_error(
        reason: icydb::diagnostic::QueryReadAdmissionCode,
    ) -> String {
        let err = icydb::Error::from_diagnostic(icydb::diagnostic::Diagnostic::new(
            icydb::diagnostic::DiagnosticCode::QueryReadAdmission,
            icydb::diagnostic::ErrorOrigin::Query,
            Some(icydb::diagnostic::DiagnosticDetail::QueryReadAdmission { reason }),
        ));

        render_error(&err)
    }
}
