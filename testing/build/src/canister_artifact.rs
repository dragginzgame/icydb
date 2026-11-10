//! Module: build::canister_artifact
//! Responsibility: actor policy and inspection.
//! Does not own: builds or runtime.
//! Boundary: checks artifacts against policy.

use std::{collections::BTreeSet, fs, path::Path};

use ic_host_artifacts::{
    artifact::ArtifactError,
    wasm::{InspectionLimits, inspect},
};
use ic_host_fs::read::read_opened_file;
use ic_host_tools::interface;
use icydb::__macro::RpcMethod;
use icydb_testing_wasm_helpers::wire::DynamicRowsQuery;

pub use ic_host_tools::interface::MethodMode;

/// One method observed in a built canister artifact.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct CanisterMethod {
    /// Fixed public method name.
    pub name: String,
    /// IC execution mode.
    pub mode: MethodMode,
}

impl CanisterMethod {
    fn new(name: impl Into<String>, mode: MethodMode) -> Self {
        Self {
            name: name.into(),
            mode,
        }
    }
}

/// One frozen expected method without allocating policy state.
pub type ExpectedCanisterMethod = (&'static str, MethodMode);

/// Frozen production/local policy for one maintained generated actor.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct MaintainedCanisterPolicy {
    /// Integration-harness canister name.
    pub canister: &'static str,
    /// Cargo package producing the actor Wasm.
    pub package: &'static str,
    /// Exact production feature set, in deterministic lexical order.
    pub production_features: &'static [&'static str],
    /// Exact local/test feature set, in deterministic lexical order.
    pub local_test_features: &'static [&'static str],
    /// IcyDB-prefixed methods exported by the maintained production build.
    pub production_icydb_methods: &'static [ExpectedCanisterMethod],
    /// Explicit non-IcyDB update probes retained in the production audit actor.
    /// All other application update exports are refused by artifact qualification.
    pub production_fixture_updates: &'static [&'static str],
    /// IcyDB-prefixed methods exported by the maintained local/test build.
    pub local_test_icydb_methods: &'static [ExpectedCanisterMethod],
}

const NO_METHODS: &[ExpectedCanisterMethod] = &[];
const METRICS_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_metrics", MethodMode::Query),
    ("icydb_metrics_reset", MethodMode::Update),
];
const SQL_PERF_LOCAL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", MethodMode::Update),
    ("icydb_fixtures_load", MethodMode::Update),
    ("icydb_fixtures_reset", MethodMode::Update),
];
const TEST_SQL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", MethodMode::Update),
    ("icydb_fixtures_load", MethodMode::Update),
    ("icydb_fixtures_reset", MethodMode::Update),
    ("icydb_integrity", MethodMode::Update),
    ("icydb_metrics", MethodMode::Query),
    ("icydb_metrics_reset", MethodMode::Update),
    ("icydb_query", MethodMode::Query),
    ("icydb_schema", MethodMode::Query),
    ("icydb_snapshot", MethodMode::Query),
    ("icydb_update", MethodMode::Update),
];
const TEST_SQL_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", MethodMode::Update),
    ("icydb_integrity", MethodMode::Update),
    ("icydb_metrics", MethodMode::Query),
    ("icydb_metrics_reset", MethodMode::Update),
    ("icydb_schema", MethodMode::Query),
    ("icydb_snapshot", MethodMode::Query),
    ("icydb_update", MethodMode::Update),
];
const TEST_SQL_BOUNDED_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", MethodMode::Update),
    ("icydb_fixtures_load", MethodMode::Update),
    ("icydb_fixtures_reset", MethodMode::Update),
    ("icydb_metrics", MethodMode::Query),
    ("icydb_metrics_reset", MethodMode::Update),
    ("icydb_query", MethodMode::Query),
    ("icydb_schema", MethodMode::Query),
    ("icydb_snapshot", MethodMode::Query),
    ("icydb_update", MethodMode::Update),
];
const TEST_SQL_BOUNDED_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", MethodMode::Update),
    ("icydb_metrics", MethodMode::Query),
    ("icydb_metrics_reset", MethodMode::Update),
    ("icydb_schema", MethodMode::Query),
    ("icydb_snapshot", MethodMode::Query),
    ("icydb_update", MethodMode::Update),
];
const TEST_SQL_GUARD_METHODS: &[ExpectedCanisterMethod] = &[("icydb_query", MethodMode::Query)];
const TEST_READ_AUTHORITY_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_query", MethodMode::Query),
    ("icydb_schema", MethodMode::Query),
];
const TEST_SCHEMA_METHODS: &[ExpectedCanisterMethod] = &[("icydb_schema", MethodMode::Query)];
const RPG_PRODUCTION_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", MethodMode::Update),
    ("icydb_metrics", MethodMode::Query),
    ("icydb_metrics_reset", MethodMode::Update),
    ("icydb_schema", MethodMode::Query),
    ("icydb_snapshot", MethodMode::Query),
];
const RPG_LOCAL_METHODS: &[ExpectedCanisterMethod] = &[
    ("icydb_ddl", MethodMode::Update),
    ("icydb_fixtures_load", MethodMode::Update),
    ("icydb_fixtures_reset", MethodMode::Update),
    ("icydb_metrics", MethodMode::Query),
    ("icydb_metrics_reset", MethodMode::Update),
    ("icydb_query", MethodMode::Query),
    ("icydb_schema", MethodMode::Query),
    ("icydb_snapshot", MethodMode::Query),
];

/// Frozen current build and export policy for maintained and evidence actors.
pub const MAINTAINED_CANISTER_POLICIES: &[MaintainedCanisterPolicy] = &[
    MaintainedCanisterPolicy {
        canister: "default_empty",
        package: "canister_audit_default_empty",
        production_features: &["sql"],
        local_test_features: &["sql"],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "default_empty_metrics",
        package: "canister_audit_default_empty_metrics",
        production_features: &["sql"],
        local_test_features: &["sql"],
        production_icydb_methods: METRICS_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: METRICS_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "group_path_sql_query",
        package: "canister_audit_group_path_sql_query",
        production_features: &["sql"],
        local_test_features: &["sql"],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[
            "ddl_group_path",
            "load_group_path_fixture",
            "mutate_group_path",
        ],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_none",
        package: "canister_audit_nested_relation_none",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &["exercise_relation_flow", "measure_relation_calibration"],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_direct",
        package: "canister_audit_nested_relation_direct",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &["exercise_relation_flow", "measure_relation_calibration"],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_shallow",
        package: "canister_audit_nested_relation_shallow",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[
            "exercise_relation_flow",
            "exercise_shallow_relation_flow",
            "measure_relation_calibration",
        ],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "nested_relation_repeated",
        package: "canister_audit_nested_relation_repeated",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[
            "commit_repeated_relation_boundary",
            "exercise_relation_flow",
            "exercise_repeated_relation_flow",
            "measure_relation_calibration",
        ],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_dynamic_query",
        package: "canister_audit_one_entity_dynamic_query",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_reachable_operations",
        package: "canister_audit_one_entity_reachable_operations",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &["exercise_reachable_entity_write"],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_sql_query",
        package: "canister_audit_one_entity_sql_query",
        production_features: &["sql"],
        local_test_features: &["sql"],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "one_entity_typed_query",
        package: "canister_audit_one_entity_typed_query",
        production_features: &["u256-audit"],
        local_test_features: &["lifecycle-audit", "u256-audit"],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "request_future_scale",
        package: "canister_audit_request_future_scale",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_perf",
        package: "canister_audit_sql_perf",
        production_features: &["sql"],
        local_test_features: &["sql", "test-admin-api"],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: SQL_PERF_LOCAL_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "ten_entity_typed_query",
        package: "canister_audit_ten_entity_typed_query",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "ten_entity_reachable_operations",
        package: "canister_audit_ten_entity_reachable_operations",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &["exercise_reachable_entity_write"],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql",
        package: "canister_test_sql",
        production_features: &["sql"],
        local_test_features: &["local-sql-query", "test-admin-api"],
        production_icydb_methods: TEST_SQL_PRODUCTION_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: TEST_SQL_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_bounded",
        package: "canister_test_sql_bounded",
        production_features: &["sql"],
        local_test_features: &["local-sql-query", "test-admin-api"],
        production_icydb_methods: TEST_SQL_BOUNDED_PRODUCTION_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: TEST_SQL_BOUNDED_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "lifecycle_participant",
        package: "canister_test_lifecycle_participant",
        production_features: &[],
        local_test_features: &["population-seed"],
        production_icydb_methods: NO_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: NO_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "read_authority",
        package: "canister_test_read_authority",
        production_features: &["sql"],
        local_test_features: &["guarded-reads", "sql"],
        production_icydb_methods: TEST_READ_AUTHORITY_METHODS,
        production_fixture_updates: &["set_schema_reader_enabled", "set_sql_reader_enabled"],
        local_test_icydb_methods: TEST_READ_AUTHORITY_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "schema_guard",
        package: "canister_test_schema_guard",
        production_features: &[],
        local_test_features: &["guarded-schema"],
        production_icydb_methods: TEST_SCHEMA_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: TEST_SCHEMA_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "schema_public",
        package: "canister_test_schema_public",
        production_features: &[],
        local_test_features: &[],
        production_icydb_methods: TEST_SCHEMA_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: TEST_SCHEMA_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "sql_guard",
        package: "canister_test_sql_guard",
        production_features: &["sql"],
        local_test_features: &["guarded-sql-query", "sql"],
        production_icydb_methods: TEST_SQL_GUARD_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: TEST_SQL_GUARD_METHODS,
    },
    MaintainedCanisterPolicy {
        canister: "demo_rpg",
        package: "canister_demo_rpg",
        production_features: &["sql"],
        local_test_features: &["local-sql-query", "test-admin-api"],
        production_icydb_methods: RPG_PRODUCTION_METHODS,
        production_fixture_updates: &[],
        local_test_icydb_methods: RPG_LOCAL_METHODS,
    },
];

/// IC Wire declarations and raw-Wasm method manifests for one built actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CanisterArtifactManifest {
    /// IC Wire methods declared by the artifact's current contract identity.
    pub wire_methods: BTreeSet<CanisterMethod>,
    /// IC query/update exports present in raw Wasm, including reserved runtime
    /// self-call entrypoints outside the application Wire roster.
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

/// Inspect one canister and require its IC Wire declarations to
/// agree with application Wasm exports, retaining reserved timer runtime exports.
///
/// # Errors
///
/// Returns an error for unreadable or malformed Wasm, or a method/mode drift
/// between declarations and exports, or an unknown/duplicated Wire identity.
pub fn inspect_canister_artifacts(wasm_path: &Path) -> Result<CanisterArtifactManifest, String> {
    let wasm = read_wasm_artifact(wasm_path)
        .map_err(|error| format!("failed to read {}: {error}", wasm_path.display()))?;
    let wasm_methods = inspect_wasm_methods(&wasm)?;

    let wire_methods = inspect_wire_methods(&wasm)?;
    let application_wasm_methods = application_wasm_methods(&wasm_methods);
    if wire_methods != application_wasm_methods {
        let runtime_methods = wasm_methods
            .difference(&application_wasm_methods)
            .cloned()
            .collect::<BTreeSet<_>>();
        return Err(format!(
            "declaration/Wasm method drift for {}: Wire {wire_methods:?}, application Wasm {application_wasm_methods:?}, reserved runtime Wasm {runtime_methods:?}",
            wasm_path.display()
        ));
    }

    Ok(CanisterArtifactManifest {
        wire_methods,
        wasm_methods,
    })
}

// Every raw method declares its current identity. Protocol ownership is never
// inferred from an export's spelling, and undeclared hidden methods reject.
fn inspect_wire_methods(wasm: &[u8]) -> Result<BTreeSet<CanisterMethod>, String> {
    let facts = inspect(
        wasm,
        InspectionLimits {
            module_bytes: wasm.len(),
            sections: wasm.len(),
            exports: u32::try_from(wasm.len()).unwrap_or(u32::MAX),
            custom_sections: wasm.len(),
        },
    )
    .map_err(|error| format!("failed to inspect Wire declarations: {error}"))?;
    let mut methods = BTreeSet::new();
    let mut identities = BTreeSet::new();
    for declaration in facts
        .custom_sections
        .iter()
        .filter(|section| section.name == "icydb:wire")
    {
        let (chunks, remainder) = declaration.data.as_chunks::<8>();
        for bytes in chunks {
            let id = u64::from_le_bytes(*bytes);
            if !identities.insert(id) {
                return Err("duplicated IC Wire contract identity".into());
            }
            let (name, query) = if let Some(endpoint) = RpcMethod::from_id(id) {
                (
                    endpoint.name(),
                    matches!(
                        endpoint,
                        RpcMethod::Query
                            | RpcMethod::Metrics
                            | RpcMethod::Snapshot
                            | RpcMethod::Schema
                            | RpcMethod::SchemaMigration
                    ),
                )
            } else if id == DynamicRowsQuery::ID {
                (DynamicRowsQuery::NAME, true)
            } else {
                *icydb_testing_wasm_helpers::fixture_wire::METHODS
                    .iter()
                    .find(|(name, _)| {
                        icydb_testing_wasm_helpers::fixture_wire::method_id(name) == id
                    })
                    .ok_or("unknown IC Wire contract identity")?
            };
            methods.insert(CanisterMethod::new(
                name,
                if query {
                    MethodMode::Query
                } else {
                    MethodMode::Update
                },
            ));
        }
        if !remainder.is_empty() {
            return Err("invalid IC Wire identity width".into());
        }
    }
    Ok(methods)
}

/// Read a complete regular Wasm artifact through its selected descriptor.
///
/// The observed descriptor length bounds allocation and stream growth without
/// imposing a fixed cap on compiler or report artifacts. Symbolic links remain
/// allowed in caller-controlled paths. Retained build ownership, immutable
/// inputs and Wasm validity remain caller responsibilities.
///
/// # Errors
/// Returns typed open, metadata, non-regular-file, size, read or allocation
/// failures, including growth beyond the observed artifact length.
pub fn read_wasm_artifact(path: &Path) -> Result<Vec<u8>, ArtifactError> {
    let file = fs::File::open(path)?;
    let maximum_bytes = usize::try_from(file.metadata()?.len())
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    read_opened_file(file, maximum_bytes)
}

// The setup catalog owns version selection. Capture the installed executable's
// identity once, then let shared admission/extraction detect changes during use.
// This is a local installed-tool observation, not an upstream binary digest pin.

/// Read IC method exports directly from a raw Wasm module.
///
/// # Errors
///
/// Returns an error for invalid framing, overflowing lengths, malformed UTF-8,
/// duplicate IC method exports, or a truncated export section.
pub fn inspect_wasm_methods(wasm: &[u8]) -> Result<BTreeSet<CanisterMethod>, String> {
    // Natural byte-derived ceilings preserve whole-module inspection. Shared
    // facts own framing and method decoding; IcyDB owns admission and CDK policy.
    let facts = inspect(
        wasm,
        InspectionLimits {
            module_bytes: wasm.len(),
            sections: wasm.len(),
            exports: u32::try_from(wasm.len()).unwrap_or(u32::MAX),
            custom_sections: wasm.len(),
        },
    )
    .map_err(|error| format!("failed to inspect canister Wasm: {error}"))?;
    Ok(interface::exported_methods(&facts)
        .map(|method| CanisterMethod::new(method.name, method.mode))
        .collect())
}

/// Render the current migration endpoints' Wire identities and resource limits.
#[must_use]
pub fn render_schema_migration_endpoint_contracts() -> String {
    let contracts: Vec<_> = [RpcMethod::SchemaMigrate, RpcMethod::SchemaMigration]
        .into_iter()
        .map(|endpoint| {
            serde_json::json!({ "method": endpoint.name(), "id": endpoint.id(),
            "request_bytes": icydb::wire::MAX_REQUEST_BYTES,
            "response_bytes": icydb::wire::MAX_REPLY_BYTES,
            "nodes": icydb_schema::wire::MAX_NODES, "depth": icydb_schema::wire::MAX_DEPTH })
        })
        .collect();
    format!(
        "{}\n",
        serde_json::to_string_pretty(&contracts).unwrap_or_default()
    )
}

fn application_wasm_methods(methods: &BTreeSet<CanisterMethod>) -> BTreeSet<CanisterMethod> {
    methods
        .iter()
        .filter(|method| {
            method.name != "<ic-timers internal> dispatch" || method.mode != MethodMode::Update
        })
        .cloned()
        .collect()
}

#[cfg(test)]
mod tests;
