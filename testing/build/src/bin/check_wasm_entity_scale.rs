//! Fail closed when generated entity cardinality causes excessive raw-Wasm growth.

use std::{
    fs,
    path::{Path, PathBuf},
};

use clap::Parser;

use icydb_testing_build::wasm_measurement::{
    ENTITY_SCALE_ADDED_ENTITIES, entity_scale_raw_bytes_per_added_entity,
    validate_reachable_entity_scale_raw_wasm, validate_schema_entity_scale_raw_wasm,
};

#[derive(Parser)]
#[command(about = "Check raw Wasm growth from one to ten entities")]
struct Args {
    baseline: PathBuf,
    candidate: PathBuf,
    #[arg(long)]
    reachable: bool,
}

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let args = Args::parse();
    let baseline_bytes = artifact_bytes(&args.baseline, "one-entity baseline")?;
    let candidate_bytes = artifact_bytes(&args.candidate, "ten-entity candidate")?;
    let growth = candidate_bytes.saturating_sub(baseline_bytes);
    let per_entity = entity_scale_raw_bytes_per_added_entity(baseline_bytes, candidate_bytes);
    let (comparison, validation) = if args.reachable {
        (
            "reachable",
            validate_reachable_entity_scale_raw_wasm(baseline_bytes, candidate_bytes),
        )
    } else {
        (
            "schema",
            validate_schema_entity_scale_raw_wasm(baseline_bytes, candidate_bytes),
        )
    };
    let status = if validation.is_ok() {
        "pass"
    } else {
        "exceeded"
    };

    println!(
        "[wasm-size] {comparison} entity scale: {growth} final raw bytes across \
         {ENTITY_SCALE_ADDED_ENTITIES} added entities ({per_entity} bytes/entity; status={status})"
    );
    validation.map_err(|error| error.to_string())?;
    Ok(())
}

fn artifact_bytes(path: &Path, label: &str) -> Result<u64, String> {
    let metadata = fs::metadata(path)
        .map_err(|error| format!("failed to inspect {label} '{}': {error}", path.display()))?;
    if !metadata.is_file() {
        return Err(format!("{label} is not a file: '{}'", path.display()));
    }
    Ok(metadata.len())
}

///
/// TESTS
///
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn scale_arguments_preserve_paths_and_select_reachable_contract() {
        let args = Args::try_parse_from([
            "check_wasm_entity_scale",
            "one.wasm",
            "ten.wasm",
            "--reachable",
        ])
        .unwrap();
        assert_eq!(args.baseline, Path::new("one.wasm"));
        assert_eq!(args.candidate, Path::new("ten.wasm"));
        assert!(args.reachable);
        assert!(Args::try_parse_from(["check_wasm_entity_scale", "one.wasm"]).is_err());
    }
}
