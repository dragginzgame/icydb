use icydb_testing_integration::stage_canister_for_icp;

fn main() {
    let canister_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "demo_rpg".to_string());

    match stage_canister_for_icp(canister_name.as_str()) {
        Ok((wasm_path, did_path)) => {
            println!("staged wasm: {}", wasm_path.display());
            match did_path {
                Some(did_path) => println!("staged did:  {}", did_path.display()),
                None => println!("staged did:  unavailable"),
            }
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
