use super::parse_icp_canister_list;

#[test]
fn inventory_requires_the_complete_canister_array() {
    for source in ["{}", r#"{"canisters":42}"#, r#"{"canisters":["a",42]}"#] {
        assert!(parse_icp_canister_list(source.as_bytes()).is_err());
    }
    assert!(parse_icp_canister_list(b"{\"canisters\":[\"\xff\"]}").is_err());
}

#[test]
fn inventory_retains_empty_valid_lists_sorting_and_unknown_fields() {
    assert_eq!(
        parse_icp_canister_list(br#"{"canisters":[]}"#).unwrap(),
        Vec::<String>::new()
    );
    assert_eq!(
        parse_icp_canister_list(br#"{"canisters":["b","a"],"extra":true}"#).unwrap(),
        ["a", "b"]
    );
    assert_eq!(
        parse_icp_canister_list(br#"{"canisters":["b","a","b"]}"#).unwrap(),
        ["a", "b", "b"]
    );
}
