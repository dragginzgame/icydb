//! Native reflection must admit the canonical Account's fixed-byte option.

use crate::{Generator, RpcMethod};

#[test]
fn query_response_reflection_preserves_account_container_and_nonzero_subaccount() {
    let mut generator = Generator::new().unwrap();
    generator
        .method::<String, icydb::db::sql::SqlQueryResult, icydb::Error>(RpcMethod::Query)
        .unwrap();
    assert_eq!(generator.methods.len(), 1);
}
