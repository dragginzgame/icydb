mod actor_schema {
    include!("src/actor_schema.rs");
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    runtime_api::build::build_canister!(actor_schema::ActorHygieneCanister)
}
