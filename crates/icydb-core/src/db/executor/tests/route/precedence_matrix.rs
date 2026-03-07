use super::*;

#[test]
fn load_fast_path_order_matches_expected_precedence() {
    assert_eq!(
        LOAD_FAST_PATH_ORDER,
        [
            FastPathOrder::PrimaryKey,
            FastPathOrder::SecondaryPrefix,
            FastPathOrder::IndexRange,
        ],
        "load fast-path precedence must stay stable"
    );
}

#[test]
fn aggregate_fast_path_order_matches_expected_precedence() {
    assert_eq!(
        AGGREGATE_FAST_PATH_ORDER,
        [
            FastPathOrder::PrimaryKey,
            FastPathOrder::SecondaryPrefix,
            FastPathOrder::PrimaryScan,
            FastPathOrder::IndexRange,
            FastPathOrder::Composite,
        ],
        "aggregate fast-path precedence must stay stable"
    );
}

#[test]
fn aggregate_fast_path_order_starts_with_load_contract_prefix() {
    assert!(
        AGGREGATE_FAST_PATH_ORDER
            .starts_with(&[FastPathOrder::PrimaryKey, FastPathOrder::SecondaryPrefix]),
        "aggregate precedence must preserve load-first prefix to avoid subtle route drift"
    );
}
