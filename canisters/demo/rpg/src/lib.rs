//!
//! Character-only RPG demo canister used by local demos and fixture loading.
//!

use icydb_testing_demo_rpg_fixtures::fixtures;

icydb::start!();

/// Load one deterministic baseline fixture dataset.
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    db().insert_many_atomic(fixtures::characters())?;

    Ok(())
}

ic_cdk::export_candid!();
