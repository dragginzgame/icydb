//! Module: CLI config commands and endpoint surface gates.
//! Responsibility: create, render, validate, and enforce `icydb.toml` CLI config.
//! Does not own: config file parsing, ICP project discovery, or command execution.
//! Boundary: exposes config command handlers plus test-covered config diagnostics and endpoint constants.

use std::path::PathBuf;

mod init;
mod report;
mod resolution;
mod surface;

pub(crate) use init::init_config;
#[cfg(test)]
pub(crate) use init::init_config_with_existing_config_for_test;
#[cfg(test)]
pub(crate) use init::init_config_without_existing_config;
#[cfg(test)]
pub(crate) use surface::ConfigSurface;
pub(crate) use surface::{
    ConfiguredEndpoint, FIXTURES_LOAD_ENDPOINT, METRICS_ENDPOINT, METRICS_EXTENDED_ENDPOINT,
    METRICS_RESET_ENDPOINT, SCHEMA_CHECK_ENDPOINT, SCHEMA_ENDPOINT, SNAPSHOT_ENDPOINT,
    SQL_DDL_ENDPOINT, SQL_QUERY_ENDPOINT, SQL_UPDATE_ENDPOINT,
};

use crate::{
    cli::ConfigArgs,
    icp::{build_target_for_environment, known_canisters},
};

use resolution::load_resolved_config;

type ResolvedConfig = icydb_config::ResolvedIcydbConfig;

struct ConfigContext {
    environment: Option<String>,
    known_canisters: Vec<String>,
    start_dir: PathBuf,
    resolved: ResolvedConfig,
}

/// Resolve, validate, and display the IcyDB config visible from one directory.
pub(crate) fn show_config(args: ConfigArgs) -> Result<(), String> {
    let context = load_config_context(args)?;

    print!(
        "{}",
        report::render_config_report(
            context.start_dir.as_path(),
            context.environment.as_deref(),
            context.known_canisters.as_slice(),
            &context.resolved,
        )
    );

    Ok(())
}

/// Resolve, validate, and fail when the config is not synced with ICP metadata.
pub(crate) fn check_config(args: ConfigArgs) -> Result<(), String> {
    let context = load_config_context(args)?;
    let issues = report::config_sync_issues(
        context.environment.as_deref(),
        context.known_canisters.as_slice(),
        &context.resolved,
    );
    if issues.is_empty() {
        print_config_check_passed(context.environment.as_deref());

        return Ok(());
    }

    Err(config_check_failed_message(issues.as_slice()))
}

/// Return whether the current `icydb.toml` enables one generated endpoint family.
pub(crate) fn configured_endpoint_enabled(
    canister: &str,
    endpoint: ConfiguredEndpoint,
) -> Result<bool, String> {
    let (_, resolved) = load_resolved_config(None, &[])?;

    Ok(surface::configured_endpoint_enabled_for_resolved(
        &resolved, canister, endpoint,
    ))
}

/// Fail with a local config diagnostic before calling a generated endpoint.
pub(crate) fn require_configured_endpoint(
    canister: &str,
    endpoint: ConfiguredEndpoint,
) -> Result<(), String> {
    let (_, resolved) = load_resolved_config(None, &[])?;

    let surface = endpoint.surface();
    if surface::config_surface_enabled_for_resolved(&resolved, canister, surface) {
        return Ok(());
    }

    Err(surface::disabled_config_surface_message(
        &resolved, canister, surface,
    ))
}

/// Fail before calling an environment-sensitive generated endpoint.
pub(crate) fn require_configured_endpoint_for_environment(
    environment: &str,
    canister: &str,
    endpoint: ConfiguredEndpoint,
) -> Result<(), String> {
    let (_, resolved) = load_resolved_config(None, &[])?;
    let build_target = build_target_for_environment(environment);

    let surface = endpoint.surface();
    if surface::config_surface_enabled_for_resolved_build_target(
        &resolved,
        canister,
        surface,
        build_target,
    ) {
        return Ok(());
    }

    Err(surface::disabled_config_surface_message_for_build_target(
        &resolved,
        canister,
        surface,
        build_target,
    ))
}

fn print_config_check_passed(environment: Option<&str>) {
    println!("IcyDB config check passed");
    if environment.is_none() {
        println!("ICP sync check not run; pass --environment <name>");
    }
}

fn config_check_failed_message(issues: &[String]) -> String {
    let mut message = String::from("IcyDB config check failed");
    for issue in issues {
        message.push_str("\n- ");
        message.push_str(issue.as_str());
    }

    message
}

fn load_config_context(args: ConfigArgs) -> Result<ConfigContext, String> {
    let environment = args.environment().map(str::to_string);
    let known_canisters = if let Some(environment) = &environment {
        known_canisters(environment.as_str())?
    } else {
        Vec::new()
    };
    let (start_dir, resolved) = load_resolved_config(args.start_dir(), known_canisters.as_slice())?;

    Ok(ConfigContext {
        environment,
        known_canisters,
        start_dir,
        resolved,
    })
}

#[cfg(test)]
pub(crate) mod test_support {
    use crate::config::{ConfigSurface, ConfiguredEndpoint, report, surface};

    pub(crate) fn disabled_config_surface_message(
        resolved: &icydb_config::ResolvedIcydbConfig,
        canister: &str,
        config_surface: ConfigSurface,
    ) -> String {
        surface::disabled_config_surface_message(resolved, canister, config_surface)
    }

    pub(crate) fn config_surface_enabled_for_resolved(
        resolved: &icydb_config::ResolvedIcydbConfig,
        canister: &str,
        config_surface: ConfigSurface,
    ) -> bool {
        surface::config_surface_enabled_for_resolved(resolved, canister, config_surface)
    }

    pub(crate) fn configured_endpoint_enabled_for_resolved(
        resolved: &icydb_config::ResolvedIcydbConfig,
        canister: &str,
        endpoint: ConfiguredEndpoint,
    ) -> bool {
        surface::configured_endpoint_enabled_for_resolved(resolved, canister, endpoint)
    }

    pub(crate) fn configured_endpoint_enabled_for_resolved_build_target(
        resolved: &icydb_config::ResolvedIcydbConfig,
        canister: &str,
        endpoint: ConfiguredEndpoint,
        build_target: icydb_config::GeneratedBuildTarget,
    ) -> bool {
        surface::configured_endpoint_enabled_for_resolved_build_target(
            resolved,
            canister,
            endpoint,
            build_target,
        )
    }

    pub(crate) fn render_config_report(
        start_dir: &std::path::Path,
        environment: Option<&str>,
        known_canisters: &[String],
        resolved: &icydb_config::ResolvedIcydbConfig,
    ) -> String {
        report::render_config_report(start_dir, environment, known_canisters, resolved)
    }

    pub(crate) fn config_sync_issues(
        environment: Option<&str>,
        known_canisters: &[String],
        resolved: &icydb_config::ResolvedIcydbConfig,
    ) -> Vec<String> {
        report::config_sync_issues(environment, known_canisters, resolved)
    }
}
