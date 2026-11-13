//! Module: diagnostic rendering.
//! Responsibility: render compact IcyDB diagnostic payloads for host/CLI users.
//! Does not own: canister wire shape, core error classification, or recovery policy.
//! Boundary: keeps rich diagnostic prose out of production canister crates.

pub(crate) mod artifact;

use icydb::db::encode_hex_lower;
use icydb::diagnostic::{
    DiagnosticBacklogResource, DiagnosticCode, DiagnosticComponentKind,
    DiagnosticConstraintContext, DiagnosticConstraintKind, DiagnosticDetail,
    DiagnosticExecutionBudgetResource, DiagnosticExecutionBudgetScope, DiagnosticExecutionLane,
    DiagnosticFactTag, DiagnosticGroupBudgetResource, DiagnosticMutationOperation, ErrorClass,
    ErrorCode, ErrorOrigin, QueryErrorKind, QueryFieldRole, QueryFieldSchemaMismatch,
    QueryProjectionCode, QueryReadAdmissionCode, RuntimeBoundaryCode, RuntimeErrorKind,
    SchemaDdlAdmissionCode, SchemaMigrationCode, SqlFeatureCode, SqlLoweringCode,
    SqlSurfaceMismatchCode, SqlWriteBoundaryCode,
};
use std::{fmt::Write as _, io::Read, path::Path};

use crate::{
    cli::DiagnosticArgs,
    diagnostic::artifact::{DiagnosticSchemaArtifact, ResolvedDiagnosticEntity},
    observability::load_schema_report,
};

use ic_host_artifacts::artifact::{ArtifactError, read_reader};
use ic_host_fs::read::open_regular_file;
use ic_host_text::escape::terminal_text;

// Decode failures cannot prove version skew: corruption and a wrong endpoint
// can fail identically. Preserve the original diagnostic and offer lockstep advice.
pub(crate) fn render_wire_decode_error(
    endpoint: icydb::__macro::RpcMethod,
    error: icydb::wire::Error,
) -> String {
    format!(
        "cannot decode {} reply: {error}. Check that the CLI and canister use matching IcyDB contracts; a different release, wrong endpoint, or malformed reply can cause this failure. No compatibility fallback was attempted.",
        endpoint.name(),
    )
}

/// One parsed numeric diagnostic fact, including forward-compatible unknown tags.
#[derive(Clone, Copy, Debug)]
pub(crate) struct RawDiagnosticFact {
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

const MAX_DIAGNOSTIC_ERROR_BYTES: usize = 64 * 1024;

// Bound bytes before deserializing the existing public payload. JSON's default
// recursion bound stays enabled; no CLI-owned transport shape is introduced.
fn read_error_json(reader: impl Read) -> Result<icydb::Error, String> {
    let bytes = read_reader(reader, MAX_DIAGNOSTIC_ERROR_BYTES).map_err(|error| match error {
        ArtifactError::LimitExceeded { .. } => "diagnostic error JSON exceeds 64 KiB".to_string(),
        other => format!(
            "cannot read diagnostic error JSON: {}",
            terminal_text(&other.to_string())
        ),
    })?;
    if bytes.iter().find(|byte| !byte.is_ascii_whitespace()) != Some(&b'{') {
        return Err("diagnostic error JSON must be an object".to_string());
    }
    serde_json::from_slice(&bytes).map_err(|err| {
        format!(
            "invalid public IcyDB Error JSON: {}",
            terminal_text(&err.to_string())
        )
    })
}

fn load_error_json(path: &Path) -> Result<icydb::Error, String> {
    if path == Path::new("-") {
        read_error_json(std::io::stdin().lock())
    } else {
        // Admit the selected descriptor before reading so a FIFO cannot wait
        // for a writer during open; intentional stream input uses `-` above.
        let file = open_regular_file(path)
            .map_err(|err| format!("cannot open diagnostic error JSON: {err}"))?;
        read_error_json(file)
    }
}

/// Resolve and print one compact diagnostic entirely from host-side authority.
pub(crate) fn run_diagnostic_command(args: &DiagnosticArgs) -> Result<(), String> {
    println!("{}", diagnostic_command_report(args)?);
    Ok(())
}

fn diagnostic_command_report(args: &DiagnosticArgs) -> Result<String, String> {
    let payload = args.error_json().map(load_error_json).transpose()?;
    let code = payload
        .as_ref()
        .map(|error| error.code().raw())
        .or_else(|| args.code())
        .ok_or("a diagnostic input is required")?;
    if payload.is_none()
        && args.facts().is_empty()
        && args.artifact().is_none()
        && args.source_metadata().is_none()
        && args.canister_name().is_none()
    {
        return Ok(render_error_code_report(code));
    }

    let facts = if let Some(error) = &payload {
        error
            .facts()
            .iter()
            .map(|fact| RawDiagnosticFact {
                tag: fact.tag(),
                value: fact.value(),
            })
            .collect()
    } else {
        validate_fact_count(args.facts())?;
        args.facts().to_vec()
    };
    let mut report = resolved_diagnostic_report(args, code, &facts)?;
    if let Some(error) = payload {
        // Preserve actual origin and validated caller field context alongside
        // the registry's default origin and exact-schema resolution.
        write!(
            report,
            "\norigin: {}\npayload: {}",
            origin_text(error.origin().into()),
            render_error(&error)
        )
        .map_err(|err| format!("cannot render diagnostic error: {err}"))?;
    }
    Ok(report)
}

// Both CLI input sources share the same fingerprint/provenance checks and
// resolver order. Payload decoding never supplies schema authority itself.
fn resolved_diagnostic_report(
    args: &DiagnosticArgs,
    code: u16,
    facts: &[RawDiagnosticFact],
) -> Result<String, String> {
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
    let fact_schema_is_valid = diagnostic_fact_schema_mismatch(code, facts).is_none();
    let identity = fact_schema_is_valid
        .then(|| DiagnosticSchemaIdentity::from_facts(facts))
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
        // A decoded response may still be unusable as diagnostic authority.
        // Both failures belong to optional live resolution, not offline admission.
        match load_schema_report(args.environment(), canister).and_then(|report| {
            DiagnosticSchemaArtifact::from_report(args.environment(), canister, &report)
        }) {
            Ok(artifact) => Some(artifact),
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

    Ok(render_error_code_report_with_facts(
        code,
        facts,
        artifacts.as_slice(),
        &mut notes,
    ))
}

/// Render one compact public IcyDB error code for CLI lookup.
pub(crate) fn render_error_code_report(code: u16) -> String {
    let mut notes = Vec::new();
    render_error_code_report_with_facts(code, &[], &[], &mut notes)
}

fn render_error_code_report_with_facts(
    raw: u16,
    facts: &[RawDiagnosticFact],
    artifacts: &[&DiagnosticSchemaArtifact],
    notes: &mut Vec<String>,
) -> String {
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
        let schema_mismatch = diagnostic_fact_schema_mismatch(raw, facts);
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
                encode_hex_lower(&identity.fingerprint),
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
                terminal_text(resolved.entity_name()),
                terminal_text(resolved.entity_path())
            )),
        }
    }
    lines.extend(notes.iter().map(|note| format!("note: {note}")));

    lines.join("\n")
}

fn diagnostic_fact_schema_mismatch(
    code: u16,
    facts: &[RawDiagnosticFact],
) -> Option<icydb::diagnostic::DiagnosticFactSchemaMismatch> {
    let code = ErrorCode::from_raw(code);
    let raw = facts
        .iter()
        .map(|fact| (fact.tag, fact.value))
        .collect::<Vec<_>>();
    icydb::diagnostic::validate_raw_diagnostic_fact_schema(code, raw.as_slice()).err()
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

// Clap owns individual values; aggregate/domain admission remains independent.
fn validate_fact_count(inputs: &[RawDiagnosticFact]) -> Result<(), String> {
    if inputs.len() > icydb::diagnostic::MAX_PUBLIC_DIAGNOSTIC_FACTS {
        return Err(format!(
            "diagnostic input has {} facts; maximum is {}",
            inputs.len(),
            icydb::diagnostic::MAX_PUBLIC_DIAGNOSTIC_FACTS
        ));
    }
    Ok(())
}

/// Parse one numeric diagnostic fact without interpreting its schema context.
pub(crate) fn parse_fact(input: &str) -> Result<RawDiagnosticFact, String> {
    let display = terminal_text(input);
    let (tag, value) = input
        .split_once('=')
        .ok_or_else(|| format!("invalid diagnostic fact `{display}`; expected TAG=VALUE"))?;
    let tag = parse_fact_tag(tag)?;
    let value = value.parse::<u64>().map_err(|_| {
        format!("invalid diagnostic fact `{display}`; VALUE must be an unsigned integer")
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
        "unknown diagnostic fact tag `{}`; use a numeric tag or maintained label",
        terminal_text(input)
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
        DiagnosticFactTag::GroupedLimit => grouped_limit_text(value),
        DiagnosticFactTag::ExecutionBudgetScope => execution_budget_scope_text(value),
        DiagnosticFactTag::ExecutionLane => execution_lane_text(value),
        _ => None,
    };
    label.map_or_else(
        || value.to_string(),
        |label| format!("{value}({})", terminal_text(label)),
    )
}

const fn grouped_limit_text(value: u64) -> Option<&'static str> {
    match DiagnosticGroupBudgetResource::known(value) {
        Some(DiagnosticGroupBudgetResource::Groups) => Some("groups"),
        Some(DiagnosticGroupBudgetResource::DistinctValuesPerGroup) => {
            Some("distinct-values-per-group")
        }
        Some(DiagnosticGroupBudgetResource::DistinctValuesTotal) => Some("distinct-values-total"),
        Some(DiagnosticGroupBudgetResource::EstimatedBytes) => Some("estimated-bytes"),
        None => None,
    }
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

/// Parse a pasted compact code while preserving unknown numeric identities.
pub(crate) fn parse_error_code(input: &str) -> Result<u16, String> {
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
            "invalid IcyDB diagnostic code `{}`; expected E7, 7, E190, or 190",
            terminal_text(input)
        ));
    }

    digits.parse::<u16>().map_err(|_| {
        format!(
            "invalid IcyDB diagnostic code `{}`; code does not fit u16",
            terminal_text(input)
        )
    })
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
    // Quote the diagnostic grammar first, so Host's visible control escapes
    // keep their single backslash while authored backslashes remain doubled.
    let quoted = field
        .replace('\\', "\\\\")
        .replace('`', "\\`")
        .replace('\'', "\\'")
        .replace('"', "\\\"");
    ic_host_text::escape::terminal_text(&quoted).into_owned()
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
        DiagnosticFactTag::GroupedLimit => "grouped_limit",
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
    code_description(code).0
}

const fn code_text(code: DiagnosticCode) -> &'static str {
    code_description(code).1
}

// Keep stable labels and their fallback sentences together so a new code has
// one exhaustive presentation entry. Detailed diagnostics retain their own text.
const fn code_description(code: DiagnosticCode) -> (&'static str, &'static str) {
    match code {
        DiagnosticCode::QueryExactCountMetadataUnavailable => (
            "E_QUERY_EXACT_COUNT_METADATA_UNAVAILABLE",
            "exact-count metadata is unavailable; no rows were scanned",
        ),
        DiagnosticCode::QueryValidate => ("E_QUERY_VALIDATE", "query validation failed"),
        DiagnosticCode::QueryIntent => ("E_QUERY_INTENT", "query intent is invalid"),
        DiagnosticCode::QueryPlan => ("E_QUERY_PLAN", "query planning failed"),
        DiagnosticCode::QueryReadAdmission => {
            ("E_QUERY_READ_ADMISSION", "query read admission rejected")
        }
        DiagnosticCode::QueryUnorderedPagination => (
            "E_QUERY_UNORDERED_PAGINATION",
            "pagination requires deterministic ordering",
        ),
        DiagnosticCode::QueryInvalidContinuationCursor => (
            "E_QUERY_INVALID_CONTINUATION_CURSOR",
            "continuation cursor is invalid",
        ),
        DiagnosticCode::QueryNotFound => {
            ("E_QUERY_NOT_FOUND", "query expected one row but found none")
        }
        DiagnosticCode::QueryNotUnique => (
            "E_QUERY_NOT_UNIQUE",
            "query expected one row but found multiple rows",
        ),
        DiagnosticCode::QueryNumericOverflow => {
            ("E_QUERY_NUMERIC_OVERFLOW", "numeric operation overflowed")
        }
        DiagnosticCode::QueryNumericNotRepresentable => (
            "E_QUERY_NUMERIC_NOT_REPRESENTABLE",
            "numeric result is not representable",
        ),
        DiagnosticCode::QueryUnknownAggregateTargetField => (
            "E_QUERY_UNKNOWN_AGGREGATE_TARGET_FIELD",
            "unknown aggregate target field",
        ),
        DiagnosticCode::QueryUnsupportedProjection => (
            "E_QUERY_UNSUPPORTED_PROJECTION",
            "query projection is not supported",
        ),
        DiagnosticCode::QueryUnsupportedSqlFeature => (
            "E_QUERY_UNSUPPORTED_SQL_FEATURE",
            "SQL feature is not supported",
        ),
        DiagnosticCode::QuerySqlSurfaceMismatch => (
            "E_QUERY_SQL_SURFACE_MISMATCH",
            "SQL statement used the wrong endpoint surface",
        ),
        DiagnosticCode::QuerySqlWriteBoundary => {
            ("E_QUERY_SQL_WRITE_BOUNDARY", "SQL write boundary rejected")
        }
        DiagnosticCode::SchemaDdlAdmission => {
            ("E_SCHEMA_DDL_ADMISSION", "SQL DDL admission rejected")
        }
        DiagnosticCode::StoreNotFound => ("E_STORE_NOT_FOUND", "store key was not found"),
        DiagnosticCode::StoreCorruption => ("E_STORE_CORRUPTION", "store corruption detected"),
        DiagnosticCode::StoreInvariantViolation => (
            "E_STORE_INVARIANT_VIOLATION",
            "store invariant was violated",
        ),
        DiagnosticCode::RuntimeCorruption => {
            ("E_RUNTIME_CORRUPTION", "runtime corruption detected")
        }
        DiagnosticCode::RuntimeIncompatiblePersistedFormat => (
            "E_RUNTIME_INCOMPATIBLE_PERSISTED_FORMAT",
            "persisted data format is incompatible",
        ),
        DiagnosticCode::RuntimeInvariantViolation => (
            "E_RUNTIME_INVARIANT_VIOLATION",
            "runtime invariant was violated",
        ),
        DiagnosticCode::RuntimeConflict => ("E_RUNTIME_CONFLICT", "runtime conflict detected"),
        DiagnosticCode::RuntimeNotFound => ("E_RUNTIME_NOT_FOUND", "runtime item was not found"),
        DiagnosticCode::RuntimeUnsupported => {
            ("E_RUNTIME_UNSUPPORTED", "operation is not supported")
        }
        DiagnosticCode::RuntimeInternal => ("E_RUNTIME_INTERNAL", "internal runtime failure"),
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
        QueryReadAdmissionCode::InputDepthExceeded => {
            "query input nesting exceeds the preparation limit"
        }
        QueryReadAdmissionCode::InputNodesExceeded => "query input contains too many components",
        QueryReadAdmissionCode::InputBytesExceeded => {
            "query input payload exceeds the preparation limit"
        }
        QueryReadAdmissionCode::ExplainDoesNotAcceptCursor => {
            "logical explain does not accept a continuation cursor"
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
        QueryReadAdmissionCode::InputDepthExceeded => "reduce expression and value nesting",
        QueryReadAdmissionCode::InputNodesExceeded => {
            "reduce query width or split large operand lists"
        }
        QueryReadAdmissionCode::InputBytesExceeded => "reduce literal and identifier payload sizes",
        QueryReadAdmissionCode::ExplainDoesNotAcceptCursor => {
            "remove the cursor before explaining the logical query"
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
        SqlLoweringCode::BindingCount => "SQL binding count does not match parameter slots",
        SqlLoweringCode::BindingFamily => "unsupported SQL binding value family",
        SqlLoweringCode::BindingLimit => "SQL binding input limit exceeded",
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
        RuntimeBoundaryCode::SchemaReplyBytesExceeded => {
            "schema report exceeds the public reply byte limit"
        }
        RuntimeBoundaryCode::QueryExplainOutputExceeded => {
            "logical explain output exceeds the render byte limit"
        }
        RuntimeBoundaryCode::QueryExplainDepthExceeded => {
            "logical explain access tree exceeds the diagnostic depth limit"
        }
        RuntimeBoundaryCode::DatabaseStartupRecoveryPending => {
            "database startup recovery is still in progress"
        }
        RuntimeBoundaryCode::MemoryAllocationResolutionFailed => {
            "memory allocation cannot resolve under current host grants; fresh logical allocations require mode = Allowed and an eligible free slot"
        }
        RuntimeBoundaryCode::MemoryNamespaceRemoved => {
            "current declarations omitted a historical IcyDB namespace; restore its permanent namespace and controls"
        }
        RuntimeBoundaryCode::MemoryAllocationRolesIncomplete => {
            "memory declarations lack required roles (expected_count: 3 database controls or 4 store roles)"
        }
        RuntimeBoundaryCode::MemoryDeclarationInvalid => {
            "memory declarations violate the current identity, logical-request or role contract"
        }
        RuntimeBoundaryCode::MemoryDeclarationSnapshotMismatch => {
            "current memory declarations differ from established host allocation authority; expected_memory_id=requested, actual_memory_id=committed when present"
        }
        RuntimeBoundaryCode::MemoryHistoricalJournalUnavailable => {
            "historical journal allocation cannot be selected for recovery; restore current host authority and resolve its allocation lifecycle"
        }
        RuntimeBoundaryCode::TypedAdapterBatchHandleMismatch => {
            "typed batch handle belongs to another result; use the handle with its original result owner"
        }
        RuntimeBoundaryCode::TypedAdapterBatchRowConsumed => {
            "typed batch row has already been consumed; retain its decoded value instead of decoding twice"
        }
        RuntimeBoundaryCode::TypedAdapterBindingUnavailable => {
            "generated source descriptor cannot bind to accepted schema; compare the generated declaration with current accepted authority"
        }
        RuntimeBoundaryCode::TypedAdapterEntityMismatch => {
            "typed row belongs to another accepted entity; use the binding for that entity"
        }
        RuntimeBoundaryCode::TypedAdapterFieldUnavailable => {
            "typed field source is absent from the binding; rebuild the adapter against accepted schema"
        }
        RuntimeBoundaryCode::TypedAdapterIncompatibleField => {
            "generated field contract disagrees with accepted schema; review and regenerate the typed declaration"
        }
        RuntimeBoundaryCode::TypedAdapterRowFieldUnavailable => {
            "typed output projection omits a required field; include it or select the matching output adapter"
        }
        RuntimeBoundaryCode::TypedAdapterRowShapeMismatch => {
            "typed output column and value counts disagree; inspect the producer's row contract"
        }
        RuntimeBoundaryCode::TypedAdapterStaleBinding => {
            "typed binding is stale; acquire a binding from current accepted schema before retrying"
        }
        RuntimeBoundaryCode::TypedAdapterValueShapeMismatch => {
            "typed output value does not match its generated Rust field shape; use the matching adapter contract"
        }
        RuntimeBoundaryCode::MemoryBucketSizeMismatch => {
            "memory bucket size mismatch (pages: expected=requested, actual=persisted); retain the existing size or recreate the database"
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
        SqlWriteBoundaryCode::WriteScanBudgetExceeded => {
            "SQL write selection exceeded the engine scan budget"
        }
        SqlWriteBoundaryCode::ResumableUpdateWindowUnsupported => {
            "resumable UPDATE rejects LIMIT, OFFSET, and non-primary-key ordering"
        }
        SqlWriteBoundaryCode::ResumableUpdateReturningUnsupported => {
            "resumable UPDATE does not support row RETURNING"
        }
        SqlWriteBoundaryCode::ResumableUpdateExpressionUnsupported => {
            "resumable UPDATE requires fixed literal or DEFAULT assignments"
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
        SqlFeatureCode::QuotedIdentifiers => "backtick-quoted identifiers",
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
mod tests;
