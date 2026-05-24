use icydb_testing_integration::{
    CanisterBuildOptions, CanisterSqlMode, CanisterWasmProfile, stage_canister_for_icp_with_options,
};

fn main() {
    let (canister_name, options) = match parse_args(std::env::args().skip(1)) {
        Ok(parsed) => parsed,
        Err(err) => {
            eprintln!("{err}");
            eprintln!(
                "usage: build_fixture_canister [canister] [--profile debug|release|wasm-release] [--sql-mode on|off]"
            );
            std::process::exit(2);
        }
    };

    match stage_canister_for_icp_with_options(canister_name.as_str(), options) {
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

fn parse_args(
    args: impl IntoIterator<Item = String>,
) -> Result<(String, CanisterBuildOptions), String> {
    let mut canister_name = None;
    let mut options = CanisterBuildOptions::default();
    let mut args = args.into_iter();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--profile" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--profile requires a value".to_string())?;
                options.profile = CanisterWasmProfile::parse(value.as_str())?;
            }
            "--sql-mode" => {
                let value = args
                    .next()
                    .ok_or_else(|| "--sql-mode requires a value".to_string())?;
                options.sql_mode = CanisterSqlMode::parse(value.as_str())?;
            }
            "--help" | "-h" => {
                return Err(
                    "build_fixture_canister stages a supported fixture canister".to_string()
                );
            }
            value if value.starts_with('-') => {
                return Err(format!("unknown option '{value}'"));
            }
            value => {
                if canister_name.replace(value.to_string()).is_some() {
                    return Err("only one canister name may be supplied".to_string());
                }
            }
        }
    }

    Ok((
        canister_name.unwrap_or_else(|| "demo_rpg".to_string()),
        options,
    ))
}
