//! Qualify the actual fixture startup adapter against an explicitly prepared server.

#[test]
#[ignore = "requires prepared POCKET_IC_BIN or IC_TESTKIT_POCKET_IC_URL"]
fn governed_fixture_instance_is_usable() {
    let pic = crate::start_fixture_pocket_ic();
    let canister = pic.create_canister();
    let before = pic.cycle_balance(canister);
    pic.add_cycles(canister, 1_000_000);
    assert_eq!(pic.cycle_balance(canister) - before, 1_000_000);
}
