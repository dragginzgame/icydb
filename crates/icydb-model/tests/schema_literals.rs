//! Macro-admitted literals preserve canonical defaults and public source programs.

use icydb_model::{build::get_schema, node::Entity, prelude::*};
use icydb_schema::{Account, Principal, ScalarLiteral, SourceCheckInstruction, Subaccount, Ulid};

#[canister(memory_namespace = "schema_literals")]
pub struct LiteralCanister;

#[store(canister = "LiteralCanister", storage(journaled(key = "main")))]
pub struct LiteralStore;

#[entity(
    store = "LiteralStore",
    version = 1,
    pk(field = "id"),
    constraint(
        name = "max_qty",
        check = "qty <= 340282366920938463463374607431768211455"
    ),
    index(
        field = "qty",
        predicate = "qty IN (0, 340282366920938463463374607431768211455)"
    ),
    fields(
        field(name = "id", value(item(prim = "Nat64")), default = 0u64),
        field(name = "qty", value(item(prim = "Nat128")), default = 0u128),
        field(name = "account", value(item(prim = "Account")), default = "2vxsx-fae"),
        field(
            name = "subaccount_owner",
            value(item(prim = "Account")),
            default = "k2t6j-2nvnp-4zjm3-25dtz-6xhaa-c7boj-5gayf-oj3xs-i43lp-teztq-6ae-6cc627i.1"
        ),
        field(name = "owner", value(item(prim = "Principal")), default = "aaaaa-aa"),
        field(
            name = "sub",
            value(item(prim = "Subaccount")),
            default = "0xabababababababababababababababababababababababababababababababab"
        ),
        field(
            name = "ulid",
            value(item(prim = "Ulid")),
            default = "7ZZZZZZZZZZZZZZZZZZZZZZZZZ"
        )
    ),
    traits(add(Default))
)]
pub struct LiteralRow;

#[test]
fn generated_identity_defaults_construct_exact_canonical_values() {
    let row = LiteralRow::default();
    assert_eq!(
        row.account,
        Account::from_owner_and_subaccount(Principal::anonymous(), None)
    );
    assert_eq!(
        row.subaccount_owner,
        "k2t6j-2nvnp-4zjm3-25dtz-6xhaa-c7boj-5gayf-oj3xs-i43lp-teztq-6ae-6cc627i.1"
            .parse::<Account>()
            .unwrap()
    );
    assert_eq!(row.owner, Principal::from_slice(&[]));
    assert_eq!(row.sub, Subaccount::from_array([0xab; 32]));
    assert_eq!(row.ulid, Ulid::from_bytes([0xff; 16]));
}

#[test]
fn generated_numeric_predicates_and_defaults_lower_into_the_sealed_fragment() {
    let schema = get_schema().expect("valid literals must seal");
    schema
        .schema_fragment_for_canister(&format!("{}::LiteralCanister", module_path!()))
        .expect("valid defaults must lower into a fragment");
    let (_, entity) = schema
        .get_nodes::<Entity>()
        .find(|(_, entity)| entity.def().ident() == "LiteralRow")
        .unwrap();
    let check = entity.constraints()[0].source_expression(&schema).unwrap();
    let index = entity.indexes()[0]
        .source_predicate(&schema)
        .unwrap()
        .unwrap();
    for expression in [check, index] {
        assert!(expression.instructions().iter().any(|instruction| matches!(instruction, SourceCheckInstruction::Literal(ScalarLiteral::Nat(value)) if *value == u128::MAX)));
    }
    drop(schema);
}
