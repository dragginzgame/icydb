//! Deployed-only source-migration command execution and rendering.

use candid::Decode;
use icydb::db::{SchemaMigrationPhase, SchemaMigrationStatusPage};
use icydb::{Error, db::SchemaMigrationCommand as RuntimeMigrationCommand};

use crate::{
    cli::{CanisterTarget, ConfirmedMigrationTarget, SchemaMigrationCommand},
    endpoint::{SCHEMA_MIGRATE_ENDPOINT, SCHEMA_MIGRATION_ENDPOINT},
    icp::require_created_canister,
    observability::{call_query, call_update, endpoint_result_error},
};

pub(super) fn run_schema_migration_command(command: SchemaMigrationCommand) -> Result<(), String> {
    match command {
        SchemaMigrationCommand::Status(target) => {
            print_status(&load_status(&target)?);
            Ok(())
        }
        SchemaMigrationCommand::Advance(target) => {
            let status = load_status(&target)?;
            print_status(&advance(&target, &status)?);
            Ok(())
        }
        SchemaMigrationCommand::Run(target) => run_to_terminal(&target),
        SchemaMigrationCommand::Abort(args) => run_confirmed_abort(&args),
        SchemaMigrationCommand::Adopt(args) => run_confirmed(&args, "adopt", adopt),
    }
}

fn run_confirmed_abort(args: &ConfirmedMigrationTarget) -> Result<(), String> {
    if !args.confirmed() {
        return Err("schema migration abort requires explicit --yes confirmation".to_string());
    }
    let target = args.target();
    let mut status = load_status(target)?;
    let database = status.database_identity();
    let plan = status
        .plan_digest()
        .ok_or_else(|| "deployed schema has no migration plan to abort".to_string())?;
    loop {
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
            print_status(&next);
            return Ok(());
        }
        // Candidate-generation cleanup is deliberately bounded and its
        // private scan cursor is not part of the public status contract. Keep
        // issuing exact-plan abort pages until the terminal receipt publishes.
        status = next;
    }
}

fn run_confirmed(
    args: &ConfirmedMigrationTarget,
    operation: &str,
    run: impl FnOnce(
        &CanisterTarget,
        &SchemaMigrationStatusPage,
    ) -> Result<SchemaMigrationStatusPage, String>,
) -> Result<(), String> {
    if !args.confirmed() {
        return Err(format!(
            "schema migration {operation} requires explicit --yes confirmation"
        ));
    }
    let status = load_status(args.target())?;
    print_status(&run(args.target(), &status)?);
    Ok(())
}

fn run_to_terminal(target: &CanisterTarget) -> Result<(), String> {
    let mut status = load_status(target)?;
    let database = status.database_identity();
    let plan = status
        .plan_digest()
        .ok_or_else(|| "deployed schema has no migration plan to run".to_string())?;
    loop {
        if terminal(status.phase()) {
            print_status(&status);
            return Ok(());
        }
        let next = advance(target, &status)?;
        if next.database_identity() != database || next.plan_digest() != Some(plan) {
            return Err(
                "deployed migration database or plan identity changed while running".into(),
            );
        }
        if next == status {
            return Err("deployed migration made no bounded progress".into());
        }
        status = next;
    }
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
    let candid = call_query(
        target.environment(),
        target.canister_name(),
        SCHEMA_MIGRATION_ENDPOINT.method(),
        "(record { cursor = null })",
    )?;
    decode_response(
        &candid,
        "schema migration status",
        target,
        SCHEMA_MIGRATION_ENDPOINT.method(),
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
    let candid = call_update(
        target.environment(),
        target.canister_name(),
        SCHEMA_MIGRATE_ENDPOINT.method(),
        &command_candid(command),
    )?;
    decode_response(
        &candid,
        "schema migration",
        target,
        SCHEMA_MIGRATE_ENDPOINT.method(),
    )
}

fn decode_response(
    candid: &[u8],
    label: &str,
    target: &CanisterTarget,
    method: &str,
) -> Result<SchemaMigrationStatusPage, String> {
    let response = Decode!(candid, Result<SchemaMigrationStatusPage, Error>)
        .map_err(|error| error.to_string())?;
    response.map_err(|error| endpoint_result_error(label, target, method, error))
}

fn command_candid(command: &RuntimeMigrationCommand) -> String {
    match command {
        RuntimeMigrationCommand::Adopt {
            expected_database,
            expected_head,
        } => format!(
            "(variant {{ Adopt = record {{ expected_database = {}; expected_head = {} }} }})",
            blob(expected_database.to_bytes()),
            head(expected_head),
        ),
        RuntimeMigrationCommand::Advance {
            expected_database,
            expected_head,
            expected_plan,
            acknowledged_finding_page,
        } => format!(
            "(variant {{ Advance = record {{ expected_database = {}; expected_head = {}; expected_plan = {}; acknowledged_finding_page = {} }} }})",
            blob(expected_database.to_bytes()),
            head(expected_head),
            blob(expected_plan.to_bytes()),
            acknowledged_finding_page
                .map_or_else(|| "null".to_string(), |value| format!("opt {value}")),
        ),
        RuntimeMigrationCommand::Abort {
            expected_database,
            expected_head,
            expected_plan,
        } => format!(
            "(variant {{ Abort = record {{ expected_database = {}; expected_head = {}; expected_plan = {} }} }})",
            blob(expected_database.to_bytes()),
            head(expected_head),
            blob(expected_plan.to_bytes()),
        ),
    }
}

fn head(head: &icydb::db::ExpectedAcceptedHead) -> String {
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
    println!("  database: {}", hex(status.database_identity().to_bytes()));
    println!("  accepted head: {}", head(status.accepted_head()));
    println!(
        "  plan: {}",
        status
            .plan_digest()
            .map_or_else(|| "None".to_string(), |digest| hex(digest.to_bytes()))
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
            hex_slice(finding.primary_key()),
        );
    }
}

fn hex(bytes: [u8; 32]) -> String {
    hex_slice(bytes.as_slice())
}

fn hex_slice(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut value = String::with_capacity(64);
    for byte in bytes {
        value.push(char::from(HEX[usize::from(byte >> 4)]));
        value.push(char::from(HEX[usize::from(byte & 0x0f)]));
    }
    value
}

#[cfg(test)]
mod tests {
    use super::{blob, command_candid};
    use icydb::db::{ExpectedAcceptedHead, SchemaMigrationCommand, TargetDatabaseIdentity};

    #[test]
    fn deployed_migration_command_candid_is_exact_and_local_source_free() {
        let candid = command_candid(&SchemaMigrationCommand::Adopt {
            expected_database: TargetDatabaseIdentity::from_bytes([1; 32]),
            expected_head: ExpectedAcceptedHead::Empty,
        });
        assert!(candid.contains("variant { Adopt"));
        assert!(candid.contains(&blob([1; 32])));
        assert!(candid.contains("variant { Empty }"));
    }
}
