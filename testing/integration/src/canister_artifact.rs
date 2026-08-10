//! Built-canister Candid and Wasm inspection support.
//!
//! This module owns artifact evidence only. It does not decide which endpoint
//! should exist and cannot generate an IC export.

use std::{collections::BTreeSet, fs, path::Path, process::Command};

use candid::{
    CandidType,
    pretty::candid::compile,
    types::{FuncMode, Function, Type, TypeInner, internal::TypeContainer},
};

use icydb::{
    Error,
    db::{
        EntitySchemaDescription, IntegrityCheckResult, SchemaMigrationCommand,
        SchemaMigrationStatusPage, SchemaMigrationStatusRequest, SqlIntegrityError, StorageReport,
        sql::{SqlQueryPerfResult, SqlQueryResult},
    },
    metrics::{CompactMetricsReport, EventReport},
};

/// Query/update mode encoded by both an IC Wasm export and Candid service.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CanisterMethodMode {
    /// Ordinary replicated update method.
    Update,
    /// Non-composite query method.
    Query,
    /// Composite query method.
    CompositeQuery,
}

/// One method observed in a built canister artifact.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CanisterMethod {
    /// Fixed public method name.
    pub name: String,
    /// IC execution mode.
    pub mode: CanisterMethodMode,
}

impl CanisterMethod {
    fn new(name: impl Into<String>, mode: CanisterMethodMode) -> Self {
        Self {
            name: name.into(),
            mode,
        }
    }
}

/// One frozen expected method without allocating policy state.
pub type ExpectedCanisterMethod = (&'static str, CanisterMethodMode);

/// Frozen production/local policy for one maintained generated actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintainedCanisterPolicy {
    /// Integration-harness canister name.
    pub canister: &'static str,
    /// Cargo package producing the actor Wasm.
    pub package: &'static str,
    /// Exact future production feature set, in deterministic lexical order.
    pub production_features: &'static [&'static str],
    /// Exact future local/test feature set, in deterministic lexical order.
    pub local_test_features: &'static [&'static str],
    /// IcyDB-prefixed methods exported by the maintained production build.
    pub production_icydb_methods: &'static [ExpectedCanisterMethod],
    /// IcyDB-prefixed methods exported by the maintained local/test build.
    pub local_test_icydb_methods: &'static [ExpectedCanisterMethod],
}

const NO_METHODS: &[ExpectedCanisterMethod] = &[];
const METRICS_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
];
const SQL_PERF_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
];
const TEST_SQL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
    ("icydb_integrity", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_query", CanisterMethodMode::Query),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const TEST_SQL_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_integrity", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const TEST_SQL_BOUNDED_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_query", CanisterMethodMode::Query),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const TEST_SQL_BOUNDED_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
    ("icydb_update", CanisterMethodMode::Update),
];
const RPG_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
];
const RPG_LOCAL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", CanisterMethodMode::Update),
    ("icydb_fixtures_load", CanisterMethodMode::Update),
    ("icydb_fixtures_reset", CanisterMethodMode::Update),
    ("icydb_metrics", CanisterMethodMode::Query),
    ("icydb_metrics_extended", CanisterMethodMode::Query),
    ("icydb_metrics_reset", CanisterMethodMode::Update),
    ("icydb_query", CanisterMethodMode::Query),
    ("icydb_schema", CanisterMethodMode::Query),
    ("icydb_snapshot", CanisterMethodMode::Query),
];

/// Frozen build and pre-cut export policy for all ten maintained actors.
pub const MAINTAINED_CANISTER_POLICIES: &[MaintainedCanisterPolicy] = &[
    MaintainedCanisterPolicy {
        canister: "default_empty",
        package: "canister_audit_default_empty",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "default_empty_metrics",
        package: "canister_audit_default_empty_metrics",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: METRICS_METHODS,
        local_test_icydb_methods: METRICS_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_dynamic_query",
        package: "canister_audit_one_entity_dynamic_query",
        production_features: &["candid-export", "request-diagnostics"],
        local_test_features: &["candid-export", "request-diagnostics"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_sql_query",
        package: "canister_audit_one_entity_sql_query",
        production_features: &["candid-export", "sql"],
        local_test_features: &["candid-export", "sql"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_typed_query",
        package: "canister_audit_one_entity_typed_query",
        production_features: &["candid-export"],
        local_test_features: &["candid-export", "lifecycle-audit"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_perf",
        package: "canister_audit_sql_perf",
        production_features: &["candid-export", "diagnostics", "sql"],
        local_test_features: &["candid-export", "diagnostics", "sql", "test-admin-api"],
        production_icydb_methods: METRICS_METHODS,
        local_test_icydb_methods: SQL_PERF_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "ten_entity_typed_query",
        package: "canister_audit_ten_entity_typed_query",
        production_features: &["candid-export"],
        local_test_features: &["candid-export"],
        production_icydb_methods: NO_METHODS,
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql",
        package: "canister_test_sql",
        production_features: &["candid-export", "sql"],
        local_test_features: &[
            "candid-export",
            "diagnostics",
            "local-sql-query",
            "metrics-context-audit",
            "test-admin-api",
        ],
        production_icydb_methods: TEST_SQL_PRODUCTION_METHODS,
        local_test_icydb_methods: TEST_SQL_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_bounded",
        package: "canister_test_sql_bounded",
        production_features: &["candid-export", "sql"],
        local_test_features: &[
            "candid-export",
            "diagnostics",
            "local-sql-query",
            "test-admin-api",
        ],
        production_icydb_methods: TEST_SQL_BOUNDED_PRODUCTION_METHODS,
        local_test_icydb_methods: TEST_SQL_BOUNDED_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "demo_rpg",
        package: "canister_demo_rpg",
        production_features: &["candid-export", "sql"],
        local_test_features: &[
            "candid-export",
            "diagnostics",
            "local-extended-metrics",
            "local-sql-query",
            "test-admin-api",
        ],
        production_icydb_methods: RPG_PRODUCTION_METHODS,
        local_test_icydb_methods: RPG_LOCAL_METHODS,
    },
];

/// Candid and raw-Wasm method manifests for one built actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanisterArtifactManifest {
    /// Methods registered in generated Candid.
    pub candid_methods: BTreeSet<CanisterMethod>,
    /// IC query/update exports present in raw Wasm, including reserved runtime
    /// self-call entrypoints that are intentionally absent from Candid.
    pub wasm_methods: BTreeSet<CanisterMethod>,
}

impl CanisterArtifactManifest {
    /// Return only methods in IcyDB's maintained public namespace.
    #[must_use]
    pub fn icydb_methods(&self) -> BTreeSet<CanisterMethod> {
        self.wasm_methods
            .iter()
            .filter(|method| method.name.starts_with("icydb_"))
            .cloned()
            .collect()
    }
}

/// Inspect one Candid-exporting canister and require Candid/application-Wasm
/// agreement while retaining reserved CDK runtime exports in the raw manifest.
///
/// # Errors
///
/// Returns an error for unreadable or malformed Wasm, unavailable Candid
/// extraction, malformed Candid service declarations, or a method/mode drift
/// between the two artifacts.
pub fn inspect_canister_artifacts(wasm_path: &Path) -> Result<CanisterArtifactManifest, String> {
    let wasm = fs::read(wasm_path)
        .map_err(|error| format!("failed to read {}: {error}", wasm_path.display()))?;
    let wasm_methods = inspect_wasm_methods(&wasm)?;

    let candid_output = Command::new("candid-extractor")
        .arg(wasm_path)
        .output()
        .map_err(|error| format!("failed to invoke candid-extractor: {error}"))?;
    if !candid_output.status.success() {
        return Err(format!(
            "candid-extractor failed for {}: {}",
            wasm_path.display(),
            String::from_utf8_lossy(&candid_output.stderr).trim_end()
        ));
    }
    let candid = std::str::from_utf8(&candid_output.stdout)
        .map_err(|error| format!("candid-extractor returned non-UTF-8 output: {error}"))?;
    let candid_methods = inspect_candid_methods(candid)?;
    let application_wasm_methods = candid_visible_wasm_methods(&wasm_methods);
    if candid_methods != application_wasm_methods {
        let runtime_methods = wasm_methods
            .difference(&application_wasm_methods)
            .cloned()
            .collect::<BTreeSet<_>>();
        return Err(format!(
            "Candid/Wasm method drift for {}: Candid {candid_methods:?}, application Wasm {application_wasm_methods:?}, reserved runtime Wasm {runtime_methods:?}",
            wasm_path.display()
        ));
    }

    Ok(CanisterArtifactManifest {
        candid_methods,
        wasm_methods,
    })
}

/// Read IC method exports directly from a raw Wasm module.
///
/// # Errors
///
/// Returns an error for invalid framing, overflowing lengths, malformed UTF-8,
/// duplicate IC method exports, or a truncated export section.
pub fn inspect_wasm_methods(wasm: &[u8]) -> Result<BTreeSet<CanisterMethod>, String> {
    const WASM_HEADER: &[u8; 8] = b"\0asm\x01\0\0\0";
    if !wasm.starts_with(WASM_HEADER) {
        return Err("invalid Wasm header or unsupported binary version".to_string());
    }

    let mut input = &wasm[WASM_HEADER.len()..];
    let mut methods = BTreeSet::new();
    while !input.is_empty() {
        let section_id = take_byte(&mut input, "section id")?;
        let section_len = read_u32_leb(&mut input, "section length")?;
        let mut section = take_bytes(
            &mut input,
            usize::try_from(section_len).map_err(|_| "section length does not fit usize")?,
            "section payload",
        )?;
        if section_id != 7 {
            continue;
        }

        let export_count = read_u32_leb(&mut section, "export count")?;
        for _ in 0..export_count {
            let name = read_name(&mut section)?;
            let kind = take_byte(&mut section, "export kind")?;
            let _index = read_u32_leb(&mut section, "export index")?;
            if kind != 0 {
                continue;
            }
            let Some(method) = method_from_wasm_export(name) else {
                continue;
            };
            if !methods.insert(method.clone()) {
                return Err(format!("duplicate IC method export {method:?}"));
            }
        }
        if !section.is_empty() {
            return Err("Wasm export section contains trailing bytes".to_string());
        }
    }

    Ok(methods)
}

/// Read method names and modes from one generated Candid service.
///
/// # Errors
///
/// Returns an error for a missing or unterminated service, malformed method
/// declaration, unsupported mode suffix, or duplicate method/mode pair.
pub fn inspect_candid_methods(candid: &str) -> Result<BTreeSet<CanisterMethod>, String> {
    let candid = strip_candid_line_comments(candid);
    let service_offset = candid
        .rfind("service :")
        .ok_or_else(|| "Candid contract has no service declaration".to_string())?;
    let service = &candid[service_offset..];
    let open = service
        .find('{')
        .ok_or_else(|| "Candid service has no opening brace".to_string())?;

    let mut methods = BTreeSet::new();
    let mut depth = 1_u32;
    let mut statement = String::new();
    let mut quoted = false;
    let mut escaped = false;
    for character in service[open + 1..].chars() {
        if quoted {
            statement.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                quoted = false;
            }
            continue;
        }

        match character {
            '"' => {
                quoted = true;
                statement.push(character);
            }
            '{' => {
                depth = depth
                    .checked_add(1)
                    .ok_or_else(|| "Candid service nesting overflow".to_string())?;
                statement.push(character);
            }
            '}' => {
                depth = depth
                    .checked_sub(1)
                    .ok_or_else(|| "Candid service nesting underflow".to_string())?;
                if depth == 0 {
                    if !statement.trim().is_empty() {
                        let method = parse_candid_method(statement.trim())?;
                        if !methods.insert(method.clone()) {
                            return Err(format!("duplicate Candid method {method:?}"));
                        }
                    }
                    return Ok(methods);
                }
                statement.push(character);
            }
            ';' if depth == 1 => {
                let method = parse_candid_method(statement.trim())?;
                if !methods.insert(method.clone()) {
                    return Err(format!("duplicate Candid method {method:?}"));
                }
                statement.clear();
            }
            _ => statement.push(character),
        }
    }

    Err("unterminated Candid service declaration".to_string())
}

/// Render the normative 0.217 endpoint ABI foundation from maintained Rust
/// DTOs.
///
/// All transitive records are derived from their maintained public Rust DTOs.
#[must_use]
pub fn render_endpoint_abi_foundation() -> String {
    let mut container = TypeContainer::new();
    let methods = vec![
        endpoint_method::<String, Result<SqlQueryPerfResult, Error>>(
            &mut container,
            "icydb_query",
            CanisterMethodMode::Query,
        ),
        endpoint_method::<String, Result<SqlQueryResult, Error>>(
            &mut container,
            "icydb_ddl",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<String, Result<SqlQueryResult, Error>>(
            &mut container,
            "icydb_update",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<String, Result<IntegrityCheckResult, SqlIntegrityError>>(
            &mut container,
            "icydb_integrity",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<(), Result<(), Error>>(
            &mut container,
            "icydb_fixtures_reset",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<(), Result<(), Error>>(
            &mut container,
            "icydb_fixtures_load",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<Option<u64>, Result<CompactMetricsReport, Error>>(
            &mut container,
            "icydb_metrics",
            CanisterMethodMode::Query,
        ),
        endpoint_method::<Option<u64>, Result<EventReport, Error>>(
            &mut container,
            "icydb_metrics_extended",
            CanisterMethodMode::Query,
        ),
        endpoint_method::<(), Result<(), Error>>(
            &mut container,
            "icydb_metrics_reset",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<(), Result<StorageReport, Error>>(
            &mut container,
            "icydb_snapshot",
            CanisterMethodMode::Query,
        ),
        endpoint_method::<(), Result<Vec<EntitySchemaDescription>, Error>>(
            &mut container,
            "icydb_schema",
            CanisterMethodMode::Query,
        ),
    ];
    let actor: Type = TypeInner::Service(methods).into();
    format!("{}\n", compile(&container.env, &Some(actor)))
}

/// Render the normative 0.218 migration endpoint ABI from public Rust DTOs.
#[must_use]
pub fn render_schema_migration_endpoint_abi() -> String {
    let mut container = TypeContainer::new();
    let methods = vec![
        endpoint_method::<SchemaMigrationCommand, Result<SchemaMigrationStatusPage, Error>>(
            &mut container,
            "icydb_schema_migrate",
            CanisterMethodMode::Update,
        ),
        endpoint_method::<SchemaMigrationStatusRequest, Result<SchemaMigrationStatusPage, Error>>(
            &mut container,
            "icydb_schema_migration",
            CanisterMethodMode::Query,
        ),
    ];
    let actor: Type = TypeInner::Service(methods).into();
    format!("{}\n", compile(&container.env, &Some(actor)))
}

fn endpoint_method<A: CandidType + 'static, R: CandidType>(
    container: &mut TypeContainer,
    name: &str,
    mode: CanisterMethodMode,
) -> (String, Type) {
    let args = if std::any::TypeId::of::<A>() == std::any::TypeId::of::<()>() {
        Vec::new()
    } else {
        vec![container.add::<A>()]
    };
    let mode = match mode {
        CanisterMethodMode::Update => Vec::new(),
        CanisterMethodMode::Query => vec![FuncMode::Query],
        CanisterMethodMode::CompositeQuery => vec![FuncMode::CompositeQuery],
    };
    let function = Function {
        modes: mode,
        args,
        rets: vec![container.add::<R>()],
    };
    (name.to_string(), TypeInner::Func(function).into())
}

fn method_from_wasm_export(name: &str) -> Option<CanisterMethod> {
    [
        ("canister_update ", CanisterMethodMode::Update),
        ("canister_query ", CanisterMethodMode::Query),
        (
            "canister_composite_query ",
            CanisterMethodMode::CompositeQuery,
        ),
    ]
    .into_iter()
    .find_map(|(prefix, mode)| {
        name.strip_prefix(prefix)
            .map(|method| CanisterMethod::new(method, mode))
    })
}

fn candid_visible_wasm_methods(methods: &BTreeSet<CanisterMethod>) -> BTreeSet<CanisterMethod> {
    methods
        .iter()
        .filter(|method| {
            method.name != "<ic-cdk internal> timer_executor"
                || method.mode != CanisterMethodMode::Update
        })
        .cloned()
        .collect()
}

fn parse_candid_method(statement: &str) -> Result<CanisterMethod, String> {
    let (name, signature) = statement
        .split_once(':')
        .ok_or_else(|| format!("malformed Candid method declaration '{statement}'"))?;
    let name = name.trim().trim_matches('"');
    if name.is_empty() {
        return Err("Candid method name is empty".to_string());
    }
    let signature = signature.trim_end();
    let mode = if signature.ends_with(" composite_query") {
        CanisterMethodMode::CompositeQuery
    } else if signature.ends_with(" query") {
        CanisterMethodMode::Query
    } else if signature.ends_with(')') {
        CanisterMethodMode::Update
    } else {
        return Err(format!(
            "unsupported Candid method mode in declaration '{statement}'"
        ));
    };

    Ok(CanisterMethod::new(name, mode))
}

fn strip_candid_line_comments(candid: &str) -> String {
    let mut output = String::with_capacity(candid.len());
    for line in candid.lines() {
        let mut quoted = false;
        let mut escaped = false;
        let mut chars = line.char_indices().peekable();
        let mut end = line.len();
        while let Some((index, character)) = chars.next() {
            if quoted {
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    quoted = false;
                }
                continue;
            }
            if character == '"' {
                quoted = true;
                continue;
            }
            if character == '/' && chars.peek().is_some_and(|(_, next)| *next == '/') {
                end = index;
                break;
            }
        }
        output.push_str(&line[..end]);
        output.push('\n');
    }
    output
}

fn read_name<'a>(input: &mut &'a [u8]) -> Result<&'a str, String> {
    let len = read_u32_leb(input, "export name length")?;
    let bytes = take_bytes(
        input,
        usize::try_from(len).map_err(|_| "export name length does not fit usize")?,
        "export name",
    )?;
    std::str::from_utf8(bytes).map_err(|error| format!("invalid UTF-8 export name: {error}"))
}

fn read_u32_leb(input: &mut &[u8], label: &str) -> Result<u32, String> {
    let mut value = 0_u32;
    for shift in [0_u32, 7, 14, 21, 28] {
        let byte = take_byte(input, label)?;
        let low = u32::from(byte & 0x7f);
        if shift == 28 && low > 0x0f {
            return Err(format!("{label} overflows u32"));
        }
        value |= low << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
    }
    Err(format!("{label} uses an overlong u32 LEB"))
}

fn take_byte(input: &mut &[u8], label: &str) -> Result<u8, String> {
    let (byte, rest) = input
        .split_first()
        .ok_or_else(|| format!("truncated {label}"))?;
    *input = rest;
    Ok(*byte)
}

fn take_bytes<'a>(input: &mut &'a [u8], len: usize, label: &str) -> Result<&'a [u8], String> {
    if input.len() < len {
        return Err(format!(
            "truncated {label}: need {len} bytes, have {}",
            input.len()
        ));
    }
    let (taken, rest) = input.split_at(len);
    *input = rest;
    Ok(taken)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use super::{
        CanisterMethod, CanisterMethodMode, MAINTAINED_CANISTER_POLICIES,
        candid_visible_wasm_methods, inspect_candid_methods, inspect_wasm_methods,
        render_endpoint_abi_foundation, render_schema_migration_endpoint_abi,
    };

    #[test]
    fn candid_inspection_preserves_names_and_modes_through_nested_types() {
        let candid = r"
            type Nested = record { value : text; callback : func () -> () };
            service : {
                // Comments and nested records do not split declarations.
                read : (record { nested : Nested }) -> (variant { Ok; Err : text }) query;
                write : (text) -> (record { nested : record { value : nat64 } });
                composed : () -> (Nested) composite_query
            }
        ";

        let observed = inspect_candid_methods(candid).expect("Candid should inspect");
        let expected = BTreeSet::from([
            CanisterMethod::new("composed", CanisterMethodMode::CompositeQuery),
            CanisterMethod::new("read", CanisterMethodMode::Query),
            CanisterMethod::new("write", CanisterMethodMode::Update),
        ]);
        assert_eq!(observed, expected);
    }

    #[test]
    fn raw_wasm_inspection_reads_only_ic_function_exports() {
        let wasm = wasm_with_exports(&[
            ("canister_query read", 0),
            ("canister_update write", 0),
            ("canister_update <ic-cdk internal> timer_executor", 0),
            ("get_candid_pointer", 0),
            ("canister_query not_a_function", 3),
        ]);

        let observed = inspect_wasm_methods(&wasm).expect("Wasm should inspect");
        let expected = BTreeSet::from([
            CanisterMethod::new(
                "<ic-cdk internal> timer_executor",
                CanisterMethodMode::Update,
            ),
            CanisterMethod::new("read", CanisterMethodMode::Query),
            CanisterMethod::new("write", CanisterMethodMode::Update),
        ]);
        assert_eq!(observed, expected);
    }

    #[test]
    fn candid_agreement_excludes_only_reserved_cdk_runtime_exports() {
        let methods = BTreeSet::from([
            CanisterMethod::new(
                "<ic-cdk internal> timer_executor",
                CanisterMethodMode::Update,
            ),
            CanisterMethod::new("<ic-cdk internal> unexpected", CanisterMethodMode::Update),
            CanisterMethod::new("read", CanisterMethodMode::Query),
            CanisterMethod::new("write", CanisterMethodMode::Update),
        ]);

        assert_eq!(
            candid_visible_wasm_methods(&methods),
            BTreeSet::from([
                CanisterMethod::new("<ic-cdk internal> unexpected", CanisterMethodMode::Update,),
                CanisterMethod::new("read", CanisterMethodMode::Query),
                CanisterMethod::new("write", CanisterMethodMode::Update),
            ]),
        );
    }

    #[test]
    fn maintained_policy_is_complete_unique_and_deterministic() {
        assert_eq!(MAINTAINED_CANISTER_POLICIES.len(), 10);
        let names = MAINTAINED_CANISTER_POLICIES
            .iter()
            .map(|policy| policy.canister)
            .collect::<BTreeSet<_>>();
        let packages = MAINTAINED_CANISTER_POLICIES
            .iter()
            .map(|policy| policy.package)
            .collect::<BTreeSet<_>>();
        assert_eq!(names.len(), MAINTAINED_CANISTER_POLICIES.len());
        assert_eq!(packages.len(), MAINTAINED_CANISTER_POLICIES.len());
        for policy in MAINTAINED_CANISTER_POLICIES {
            assert!(policy.production_features.is_sorted());
            assert!(policy.local_test_features.is_sorted());
            assert!(policy.local_test_icydb_methods.is_sorted());
            assert!(policy.production_icydb_methods.is_sorted());
        }
    }

    #[test]
    fn endpoint_abi_foundation_matches_golden() {
        assert_eq!(
            render_endpoint_abi_foundation(),
            include_str!("contracts/0.217/endpoint-abi-foundation.did")
        );
    }

    #[test]
    fn schema_migration_endpoint_abi_matches_golden() {
        assert_eq!(
            render_schema_migration_endpoint_abi(),
            include_str!("contracts/0.218/schema-migration-endpoints.did")
        );
    }

    fn wasm_with_exports(exports: &[(&str, u8)]) -> Vec<u8> {
        let mut section = Vec::new();
        push_u32_leb(
            &mut section,
            u32::try_from(exports.len()).expect("fixture count fits"),
        );
        for (index, (name, kind)) in exports.iter().enumerate() {
            push_u32_leb(
                &mut section,
                u32::try_from(name.len()).expect("fixture name length fits"),
            );
            section.extend_from_slice(name.as_bytes());
            section.push(*kind);
            push_u32_leb(
                &mut section,
                u32::try_from(index).expect("fixture index fits"),
            );
        }

        let mut wasm = b"\0asm\x01\0\0\0".to_vec();
        wasm.push(7);
        push_u32_leb(
            &mut wasm,
            u32::try_from(section.len()).expect("fixture section length fits"),
        );
        wasm.extend_from_slice(&section);
        wasm
    }

    fn push_u32_leb(output: &mut Vec<u8>, mut value: u32) {
        loop {
            let mut byte = u8::try_from(value & 0x7f).expect("seven bits fit u8");
            value >>= 7;
            if value != 0 {
                byte |= 0x80;
            }
            output.push(byte);
            if value == 0 {
                return;
            }
        }
    }
}
