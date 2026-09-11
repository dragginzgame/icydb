use crate::db::{
    codec::new_hash_sha256,
    query::{
        fingerprint::{
            finalize_sha256_digest,
            hash_sections::{hash_order_fields, hash_order_spec},
        },
        plan::{OrderDirection, OrderSpec, OrderTerm},
    },
};

#[test]
fn order_identity_preserves_empty_source_and_term_order_contracts() {
    let hash_spec = |order: Option<&OrderSpec>| {
        let mut hasher = new_hash_sha256();
        hash_order_spec(&mut hasher, order);
        finalize_sha256_digest(hasher)
    };
    let empty = OrderSpec { fields: vec![] };
    assert_eq!(hash_spec(None), hash_spec(Some(&empty)));
    let mut order = OrderSpec {
        fields: vec![
            OrderTerm::field("owner_λ", OrderDirection::Asc),
            OrderTerm::field("amount", OrderDirection::Desc),
        ],
    };
    let mut hasher = new_hash_sha256();
    hash_order_fields(
        &mut hasher,
        [
            ("owner_λ", OrderDirection::Asc),
            ("amount", OrderDirection::Desc),
        ]
        .into_iter(),
    );
    let expected = finalize_sha256_digest(hasher);
    assert_eq!(hash_spec(Some(&order)), expected);
    order.fields.reverse();
    assert_ne!(hash_spec(Some(&order)), expected);
    order.fields.reverse();
    order.fields[1] = OrderTerm::field("amount", OrderDirection::Asc);
    assert_ne!(hash_spec(Some(&order)), expected);
}
