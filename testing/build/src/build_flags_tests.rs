//! Exercise Cargo's effective flag precedence in isolated child processes.

use crate::combined_encoded_rustflags;
use std::{env, fs, process::Command};

const CASE_ENV: &str = "ICYDB_BUILD_FLAGS_TEST_CASE";

#[test]
fn caller_flags_and_release_remaps_reach_cargo() {
    let Ok(case) = env::var(CASE_ENV) else {
        for case in ["encoded", "empty", "ordinary", "absent", "no_additions"] {
            let mut child = Command::new(env::current_exe().unwrap());
            child
                .args([
                    "--exact",
                    "build_flags_tests::caller_flags_and_release_remaps_reach_cargo",
                    "--nocapture",
                ])
                .env(CASE_ENV, case)
                .env_remove("RUSTFLAGS")
                .env_remove("CARGO_ENCODED_RUSTFLAGS");
            if case != "absent" {
                child.env("RUSTFLAGS", "--cfg caller_ordinary");
            }
            match case {
                "encoded" | "no_additions" => {
                    child.env("CARGO_ENCODED_RUSTFLAGS", "--cfg\x1fcaller_encoded");
                }
                "empty" => {
                    child.env("CARGO_ENCODED_RUSTFLAGS", "");
                }
                _ => {}
            }
            let output = child.output().unwrap();
            assert!(
                output.status.success(),
                "{case}: {}\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        return;
    };

    // Spaces must remain inside the remap argument, not become new rustc tokens.
    let root = env::temp_dir().join(format!("icydb flags {} {case}", std::process::id()));
    fs::create_dir_all(root.join("src")).unwrap();
    fs::write(
        root.join("Cargo.toml"),
        "[package]\nname = \"flags_probe\"\nversion = \"0.0.0\"\nedition = \"2024\"\n[workspace]\n",
    )
    .unwrap();
    let source = root.join("src/absolute.rs");
    fs::write(&source, "const SOURCE: &str = file!();\n").unwrap();
    fs::write(
        root.join("src/main.rs"),
        format!(
            "#![allow(unexpected_cfgs)]\ninclude!({source:?});\nfn main() {{ println!(\"{{}}|{{}}|{{}}\", SOURCE, cfg!(caller_encoded), cfg!(caller_ordinary)); }}\n"
        ),
    )
    .unwrap();
    let additions = if case == "no_additions" {
        vec![]
    } else {
        vec![format!(
            "--remap-path-prefix={}=/trimmed flags",
            root.display()
        )]
    };
    let flags = combined_encoded_rustflags(&additions);
    assert_eq!(flags.is_none(), case == "no_additions");
    let mut cargo = Command::new("cargo");
    cargo
        .current_dir(&root)
        .args(["run", "--offline", "--quiet"])
        .env("CARGO_TARGET_DIR", root.join("target"))
        .env_remove("CARGO_BUILD_TARGET")
        .env_remove("RUSTC_WRAPPER")
        .env_remove("RUSTC_WORKSPACE_WRAPPER");
    if let Some(flags) = flags {
        cargo.env("CARGO_ENCODED_RUSTFLAGS", flags);
    }
    let output = cargo.output().unwrap();
    assert!(
        output.status.success(),
        "{case}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let path = if case == "no_additions" {
        source.to_string_lossy().into_owned()
    } else {
        "/trimmed flags/src/absolute.rs".to_string()
    };
    assert_eq!(
        String::from_utf8(output.stdout).unwrap().trim(),
        format!(
            "{path}|{}|{}",
            matches!(case.as_str(), "encoded" | "no_additions"),
            case == "ordinary"
        )
    );
    fs::remove_dir_all(&root).unwrap();
}
