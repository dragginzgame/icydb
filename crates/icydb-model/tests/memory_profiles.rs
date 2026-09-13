//! Macro-to-model coverage for canister-owned memory profiles.

use icydb_model::{build::get_schema, canister, node::Canister};

macro_rules! profile_fixture {
    ($name:ident, $namespace:literal $(, $profile:literal)?) => {
        #[canister(
            memory_namespace = $namespace,
            memory_min = 100, memory_max = 110,
            commit_memory_id = 109, startup_memory_id = 108
            $(, memory_profile = $profile)?
        )]
        pub struct $name;
    };
}

profile_fixture!(DefaultCanister, "profile_default");
profile_fixture!(CompactCanister, "profile_compact", "compact");
profile_fixture!(GeneralCanister, "profile_general", "general");
profile_fixture!(HighHeadroomCanister, "profile_high", "high_headroom");

#[test]
fn macro_profiles_reach_the_schema_model() {
    let schema = get_schema().expect("profile fixture schema should validate and seal");
    for (name, pages) in [
        ("DefaultCanister", 16),
        ("CompactCanister", 4),
        ("GeneralCanister", 16),
        ("HighHeadroomCanister", 128),
    ] {
        let (_, canister) = schema
            .get_nodes::<Canister>()
            .find(|(_, canister)| canister.def().ident() == name)
            .expect("macro declaration should register its model");
        assert_eq!(canister.memory_profile().bucket_size_pages(), pages);
    }
    drop(schema);
}
