//! Deployed-only source-migration command execution and rendering.

use canic_icydb::wire::Endpoint;
use icydb::db::{
    SchemaMigrationCommand as RuntimeMigrationCommand, SchemaMigrationPhase,
    SchemaMigrationStatusPage, SchemaMigrationStatusRequest, encode_hex_lower,
};

use crate::{
    cli::{CanisterTarget, ConfirmedMigrationTarget, SchemaMigrationCommand},
    icp::{IcpCallKind, require_created_canister},
    observability::call_endpoint,
};

// Public status omits private migration and cleanup cursors, so identical pages
// cannot prove a stall. Bound paid calls per invocation; retained work can resume.
const MAX_MIGRATION_UPDATE_CALLS: usize = 1_024;

pub(super) fn run_schema_migration_command(command: SchemaMigrationCommand) -> Result<(), String> {
    let status = match &command {
        SchemaMigrationCommand::Status(target) => load_status(target),
        SchemaMigrationCommand::Advance(target) => {
            let status = load_status(target)?;
            advance(target, &status)
        }
        SchemaMigrationCommand::Run(target) => run_to_terminal(target),
        SchemaMigrationCommand::Abort(args) => run_confirmed_abort(args),
        SchemaMigrationCommand::Adopt(args) => run_confirmed(args, "adopt", adopt),
    }?;
    print_status(&status);

    // A successful endpoint call is not proof that the requested operation
    // succeeded. Preserve its status and findings before reporting CLI failure.
    let phase = status.phase();
    let (operation, succeeded) = match command {
        SchemaMigrationCommand::Status(_) | SchemaMigrationCommand::Adopt(_) => return Ok(()),
        SchemaMigrationCommand::Advance(_) => (
            "advance",
            !matches!(
                phase,
                SchemaMigrationPhase::Rejected | SchemaMigrationPhase::Aborted
            ),
        ),
        SchemaMigrationCommand::Run(_) => ("run", phase == SchemaMigrationPhase::Applied),
        SchemaMigrationCommand::Abort(_) => ("abort", phase == SchemaMigrationPhase::Aborted),
    };
    if !succeeded {
        return Err(format!(
            "schema migration {operation} did not succeed: phase {phase:?}"
        ));
    }

    Ok(())
}

fn run_confirmed_abort(
    args: &ConfirmedMigrationTarget,
) -> Result<SchemaMigrationStatusPage, String> {
    if !args.confirmed() {
        return Err("schema migration abort requires explicit --yes confirmation".to_string());
    }
    let target = args.target();
    let mut status = load_status(target)?;
    let database = status.database_identity();
    let plan = status
        .plan_digest()
        .ok_or_else(|| "deployed schema has no migration plan to abort".to_string())?;
    for completed_calls in 1..=MAX_MIGRATION_UPDATE_CALLS {
        let next = abort(target, &status)?;
        if next.database_identity() != database || next.plan_digest() != Some(plan) {
            return Err(
                "deployed migration database or plan identity changed while aborting".into(),
            );
        }
        if matches!(
            next.phase(),
            SchemaMigrationPhase::Aborted | SchemaMigrationPhase::Applied
        ) {
            return Ok(next);
        }
        status = next;
        if completed_calls % 128 == 0 {
            println!(
                "schema migration abort: {completed_calls} update calls completed; phase {:?}",
                status.phase()
            );
        }
    }
    print_status(&status);
    Err(format!(
        "schema migration abort reached its limit of {MAX_MIGRATION_UPDATE_CALLS} update calls; inspect migration status, then rerun abort --yes to continue cleanup"
    ))
}

fn run_confirmed(
    args: &ConfirmedMigrationTarget,
    operation: &str,
    run: impl FnOnce(
        &CanisterTarget,
        &SchemaMigrationStatusPage,
    ) -> Result<SchemaMigrationStatusPage, String>,
) -> Result<SchemaMigrationStatusPage, String> {
    if !args.confirmed() {
        return Err(format!(
            "schema migration {operation} requires explicit --yes confirmation"
        ));
    }
    let status = load_status(args.target())?;
    run(args.target(), &status)
}

fn run_to_terminal(target: &CanisterTarget) -> Result<SchemaMigrationStatusPage, String> {
    let mut status = load_status(target)?;
    let database = status.database_identity();
    let plan = status
        .plan_digest()
        .ok_or_else(|| "deployed schema has no migration plan to run".to_string())?;
    if terminal(status.phase()) {
        return Ok(status);
    }
    for completed_calls in 1..=MAX_MIGRATION_UPDATE_CALLS {
        let next = advance(target, &status)?;
        if next.database_identity() != database || next.plan_digest() != Some(plan) {
            return Err(
                "deployed migration database or plan identity changed while running".into(),
            );
        }
        status = next;
        if terminal(status.phase()) {
            return Ok(status);
        }
        if completed_calls % 128 == 0 {
            println!(
                "schema migration run: {completed_calls} update calls completed; phase {:?}",
                status.phase()
            );
        }
    }
    print_status(&status);
    Err(format!(
        "schema migration run reached its limit of {MAX_MIGRATION_UPDATE_CALLS} update calls; inspect migration status, then invoke run again to continue migration"
    ))
}

const fn terminal(phase: SchemaMigrationPhase) -> bool {
    matches!(
        phase,
        SchemaMigrationPhase::Applied
            | SchemaMigrationPhase::Rejected
            | SchemaMigrationPhase::Aborted
    )
}

fn load_status(target: &CanisterTarget) -> Result<SchemaMigrationStatusPage, String> {
    require_created_canister(target.environment(), target.canister_name())?;
    call_endpoint(
        target.environment(),
        target.canister_name(),
        Endpoint::SchemaMigration,
        SchemaMigrationStatusRequest::default(),
        "schema migration status",
        IcpCallKind::Query,
    )
}

fn advance(
    target: &CanisterTarget,
    status: &SchemaMigrationStatusPage,
) -> Result<SchemaMigrationStatusPage, String> {
    let plan = status
        .plan_digest()
        .ok_or_else(|| "deployed schema has no migration plan to advance".to_string())?;
    let command = RuntimeMigrationCommand::Advance {
        expected_database: status.database_identity(),
        expected_head: status.accepted_head().clone(),
        expected_plan: plan,
        acknowledged_finding_page: None,
    };
    call_command(target, &command)
}

fn abort(
    target: &CanisterTarget,
    status: &SchemaMigrationStatusPage,
) -> Result<SchemaMigrationStatusPage, String> {
    let plan = status
        .plan_digest()
        .ok_or_else(|| "deployed schema has no migration plan to abort".to_string())?;
    call_command(
        target,
        &RuntimeMigrationCommand::Abort {
            expected_database: status.database_identity(),
            expected_head: status.accepted_head().clone(),
            expected_plan: plan,
        },
    )
}

fn adopt(
    target: &CanisterTarget,
    status: &SchemaMigrationStatusPage,
) -> Result<SchemaMigrationStatusPage, String> {
    call_command(
        target,
        &RuntimeMigrationCommand::Adopt {
            expected_database: status.database_identity(),
            expected_head: status.accepted_head().clone(),
        },
    )
}

fn call_command(
    target: &CanisterTarget,
    command: &RuntimeMigrationCommand,
) -> Result<SchemaMigrationStatusPage, String> {
    call_endpoint(
        target.environment(),
        target.canister_name(),
        Endpoint::SchemaMigrate,
        command,
        "schema migration",
        IcpCallKind::Update,
    )
}

fn render_accepted_head(head: &icydb::db::ExpectedAcceptedHead) -> String {
    match head {
        icydb::db::ExpectedAcceptedHead::Empty => "variant { Empty }".to_string(),
        icydb::db::ExpectedAcceptedHead::Exact {
            revision,
            fingerprint,
        } => format!(
            "variant {{ Exact = record {{ revision = {revision}; fingerprint = {} }} }}",
            blob(fingerprint.to_bytes()),
        ),
    }
}

fn blob(bytes: [u8; 32]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::from("blob \"");
    for byte in bytes {
        value.push('\\');
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value.push('"');
    value
}

fn print_status(status: &SchemaMigrationStatusPage) {
    println!("IcyDB schema migration");
    println!("  phase: {:?}", status.phase());
    println!(
        "  database: {}",
        encode_hex_lower(&status.database_identity().to_bytes())
    );
    println!(
        "  accepted head: {}",
        render_accepted_head(status.accepted_head())
    );
    println!(
        "  plan: {}",
        status.plan_digest().map_or_else(
            || "None".to_string(),
            |digest| encode_hex_lower(&digest.to_bytes()),
        )
    );
    println!("  rows validated: {}", status.rows_validated());
    println!("  rows rewritten: {}", status.rows_rewritten());
    println!("  indexes rebuilt: {}", status.indexes_rebuilt());
    println!("  findings: {}", status.findings().len());
    for finding in status.findings() {
        println!(
            "    {:?}: entity {} key {}",
            finding.kind(),
            finding.entity_tag(),
            encode_hex_lower(finding.primary_key()),
        );
        if let Some(field) = finding.source_field() {
            println!("      source field: {field}");
        }
        if let Some(field) = finding.target_field() {
            println!("      target field: {field}");
        }
        if let Some(reason) = finding.transform_reason() {
            println!("      transform reason: {reason:?}");
        }
    }
}

#[cfg(test)]
mod tests {
    use canic_icydb::wire::Endpoint;
    use icydb::db::{ExpectedAcceptedHead, SchemaMigrationCommand, TargetDatabaseIdentity};
    use icydb_schema::{ExpectedSchemaFingerprint, SchemaMigrationPlanDigest};

    #[test]
    fn migration_commands_decode_without_canister_type_metadata() {
        for revision in [1, u64::MAX] {
            for head in [
                ExpectedAcceptedHead::Empty,
                ExpectedAcceptedHead::Exact {
                    revision,
                    fingerprint: ExpectedSchemaFingerprint::from_bytes([3; 32]),
                },
            ] {
                let database = TargetDatabaseIdentity::from_bytes([1; 32]);
                let plan = SchemaMigrationPlanDigest::from_bytes([2; 32]);
                for command in [
                    SchemaMigrationCommand::Adopt {
                        expected_database: database,
                        expected_head: head.clone(),
                    },
                    SchemaMigrationCommand::Abort {
                        expected_database: database,
                        expected_head: head.clone(),
                        expected_plan: plan,
                    },
                    SchemaMigrationCommand::Advance {
                        expected_database: database,
                        expected_head: head.clone(),
                        expected_plan: plan,
                        acknowledged_finding_page: None,
                    },
                    SchemaMigrationCommand::Advance {
                        expected_database: database,
                        expected_head: head.clone(),
                        expected_plan: plan,
                        acknowledged_finding_page: Some(u64::MAX),
                    },
                ] {
                    // The argument carries its own types, without service metadata.
                    let bytes = Endpoint::SchemaMigrate
                        .encode_request(
                            &command,
                            icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES),
                        )
                        .unwrap();
                    assert_eq!(
                        canic_icydb::wire::Endpoint::SchemaMigrate
                            .decode_request::<SchemaMigrationCommand>(
                                &bytes,
                                icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES)
                            )
                            .unwrap(),
                        command
                    );
                }
            }
        }
    }
}
