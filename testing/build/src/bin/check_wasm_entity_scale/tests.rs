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
