use icydb_testing_integration::stage_sql_test_canister_for_dfx;

fn main() {
    let canister_name = std::env::args()
        .nth(1)
        .unwrap_or_else(|| "sql_test".to_string());

    match stage_sql_test_canister_for_dfx(canister_name.as_str()) {
        Ok((wasm_path, did_path)) => {
            println!("staged wasm: {}", wasm_path.display());
            println!("staged did:  {}", did_path.display());
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
