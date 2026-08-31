//! Fail closed when generated entity cardinality causes excessive raw-Wasm growth.

use std::{env, fs, path::Path};

use icydb_testing_integration::wasm_measurement::{
    ENTITY_SCALE_ADDED_ENTITIES, entity_scale_raw_bytes_per_added_entity,
    validate_entity_scale_raw_wasm,
};

fn main() {
    if let Err(error) = run() {
        eprintln!("{error}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut arguments = env::args_os().skip(1);
    let baseline = arguments.next().ok_or_else(usage)?;
    let candidate = arguments.next().ok_or_else(usage)?;
    let report_only = match arguments.next() {
        None => false,
        Some(flag) if flag == "--report-only" => true,
        Some(_) => return Err(usage()),
    };
    if arguments.next().is_some() {
        return Err(usage());
    }

    let baseline_bytes = artifact_bytes(Path::new(&baseline), "one-entity baseline")?;
    let candidate_bytes = artifact_bytes(Path::new(&candidate), "ten-entity candidate")?;
    let growth = candidate_bytes.saturating_sub(baseline_bytes);
    let per_entity = entity_scale_raw_bytes_per_added_entity(baseline_bytes, candidate_bytes);
    let validation = validate_entity_scale_raw_wasm(baseline_bytes, candidate_bytes);
    let status = if validation.is_ok() {
        "pass"
    } else {
        "exceeded"
    };

    println!(
        "[wasm-size] entity scale: {growth} final raw bytes across \
         {ENTITY_SCALE_ADDED_ENTITIES} added entities ({per_entity} bytes/entity; status={status})"
    );
    if !report_only {
        validation.map_err(|error| error.to_string())?;
    }
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

fn usage() -> String {
    "usage: check_wasm_entity_scale <one-entity-final.wasm> <ten-entity-final.wasm> [--report-only]"
        .to_string()
}
