//! Raw identity projection filters before materialized ordering and pagination.

use super::{initialize, insert_exact_key_fixture, sql_projection_rows};
use crate::value::OutputValue;

#[test]
fn raw_identity_order_applies_residual_filter_before_offset_and_limit() {
    let session = initialize();
    for payload in [30, 10, 20, 20, 40] {
        insert_exact_key_fixture(&session, payload);
    }
    for (sql, expected) in [
        (
            "SELECT * FROM IdentityRow WHERE payload > 10 ORDER BY payload ASC, id ASC LIMIT 2 OFFSET 1",
            vec![(4, 20), (1, 30)],
        ),
        (
            "SELECT * FROM IdentityRow WHERE payload > 10 ORDER BY payload DESC, id DESC LIMIT 2",
            vec![(5, 40), (1, 30)],
        ),
        (
            "SELECT * FROM IdentityRow WHERE payload < 10 ORDER BY payload ASC, id ASC",
            vec![],
        ),
    ] {
        let expected = expected
            .into_iter()
            .map(|(id, payload)| vec![OutputValue::nat64(id), OutputValue::nat64(payload)])
            .collect::<Vec<_>>();
        assert_eq!(sql_projection_rows(&session, sql), expected);
    }
}
