//! Schema-only consumer of the standalone `icydb-model` package.
//!
//! This crate intentionally has no dependency on `icydb` or `icydb-core`.

use model_api::{
    base::types::{num::Degrees, web::Url},
    prelude::*,
};

// Generated code must not resolve standard types, traits or variants through local aliases.
use std::{
    cmp::Ordering::{Equal as None, Less as Ok},
    collections::VecDeque as Vec,
    fmt::Debug as Clone,
    io::Result,
    marker::{PhantomData as Option, Send as PartialEq, Sync as PartialOrd},
    ops::{
        ControlFlow::{Break as Err, Continue as Some},
        Range as Into,
    },
    rc::Rc as Box,
};
const _: Into<u8> = 0..0;
const _: ::std::option::Option<&dyn Clone> = ::std::option::Option::None;
const _: Result<()> = ::std::result::Result::Ok(());
const _: ::std::option::Option<&(dyn PartialEq + PartialOrd)> = ::std::option::Option::None;
const _: Option<()> = std::marker::PhantomData;
const _: ::std::option::Option<Vec<()>> = ::std::option::Option::None;
const _: ::std::option::Option<Box<()>> = ::std::option::Option::None;

const _: std::cmp::Ordering = None;
const _: std::cmp::Ordering = Ok;
const _: std::ops::ControlFlow<(), ()> = Err(());
const _: std::ops::ControlFlow<(), ()> = Some(());

#[canister(memory_namespace = "model_schema_only")]
pub struct SchemaOnlyCanister {}

#[store(canister = "SchemaOnlyCanister", storage(heap()))]
pub struct SchemaOnlyStore {}

#[record(fields(field(name = "website", value(item(is = "Url")))))]
pub struct SchemaOnlyProfile {}

// Application names must not collide with private schema registration items.
#[cfg(all(test, feature = "build"))]
const SCHEMA_ONLY_PROFILE_CONST: u8 = 7;

#[cfg(all(test, feature = "build"))]
const fn __icydb_register_schema_only_profile_const() -> u8 {
    SCHEMA_ONLY_PROFILE_CONST
}

#[enum_(variant(name = "Active"), variant(name = "Disabled"))]
pub struct SchemaOnlyState {}

#[newtype(item(prim = "Text", max_len = 64))]
pub struct FieldKey {}

#[map(key(is = "FieldKey"), value(item(is = "FieldValue")))]
pub struct Values {}

#[enum_(
    variant(name = "One", value(item(indirect, is = "Value"))),
    variant(name = "Many", value(many, item(indirect, is = "Value")))
)]
pub struct FieldValue {}

#[enum_(
    name = "SchemaOnlyValue",
    variant(name = "Text", value(item(prim = "Text", max_len = 128))),
    variant(name = "Degrees", value(item(is = "Degrees"))),
    variant(name = "Record", value(item(indirect, is = "Values")))
)]
pub struct Value {}

#[newtype(item(prim = "Nat64"))]
pub struct Tokens {}

#[newtype(name = "SchemaOnlyTokenAmount", item(prim = "Nat64"))]
pub struct TokenAmount {}

#[newtype(item(prim = "Text", max_len = 32))]
pub struct Tier {}

#[enum_(
    variant(name = "Free"),
    variant(name = "Icp", value(item(is = "crate::Tokens"))),
    variant(name = "Icrc1", value(item(is = "crate::TokenAmount")))
)]
pub struct ClaimCost {}

#[map(key(is = "Tier"), value(item(is = "crate::ClaimCost")))]
pub struct ClaimCostTiers {}

#[record(fields(
    field(name = "values", value(item(is = "Values"))),
    field(name = "fallback", value(opt, item(is = "Value"))),
    field(name = "claim_cost_tiers", value(item(is = "ClaimCostTiers")))
))]
pub struct CollectionPolicy {}

#[newtype(
    item(prim = "Text", unbounded),
    ty(
        normalizer(path = "base::normalizer::text::Trim"),
        validator(path = "base::validator::len::Max", args(40)),
        rule(name = "length", length_range_inclusive(min = 1, max = 40)),
    )
)]
pub struct RuleText {}

#[newtype(
    item(prim = "Decimal", scale = 2),
    ty(
        rule(name = "minimum", numeric_minimum_inclusive(value = 0)),
        rule(name = "maximum", numeric_maximum_inclusive(value = 100)),
        rule(name = "range", numeric_range_inclusive(min = 0, max = 100)),
        rule(name = "step", multiple_of(divisor = 0.25)),
    )
)]
pub struct RuleNumber {}

#[entity(
    store = "SchemaOnlyStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["rule_text"], unique),
    index(fields = ["rule_text", "id"], unique),
    index(fields = ["rule_number"], predicate = "id > 0"),
    index(fields = ["rule_number", "id"]),
    fields(
        field(
            name = "id",
            value(item(prim = "Nat64")),
            generated(insert = "Identity::next")
        ),
        field(
            name = "state",
            value(item(is = "SchemaOnlyState"))
        ),
        field(
            name = "profile",
            value(item(is = "SchemaOnlyProfile"))
        ),
        field(
            name = "degrees",
            value(item(is = "Degrees"))
        ),
        field(
            name = "policy",
            value(item(is = "CollectionPolicy"))
        ),
        field(
            name = "rule_text",
            value(item(is = "RuleText"))
        ),
        field(
            name = "rule_number",
            value(item(is = "RuleNumber"))
        )
    )
)]
pub struct SchemaOnlyEntity {}

#[cfg(all(test, feature = "build"))]
mod tests;

/// An authored module whose name also happens to be a framework package name.
pub mod icydb {
    #[model_api::newtype(item(prim = "Nat64"))]
    pub struct Money {}
}

/// Explicit macro path through a consumer-owned facade.
pub use model_api as model_bridge;

#[record(
    model_crate = "crate::model_bridge",
    fields(
        field(name = "icydb", value(item(is = "crate::icydb::Money"))),
        field(
            name = "icydb_model",
            value(item(prim = "Text", max_len = 64)),
            default = "authored"
        )
    )
)]
pub struct AuthoredPackageNames {}

#[cfg(test)]
mod crate_path_tests;

#[cfg(test)]
mod default_hygiene_tests;

// Authored Rust attributes must reach the generated main type. This module
// consumes the deprecated fixture deliberately; downstream users still receive
// Rust's ordinary deprecation diagnostic outside this allowed scope.
#[expect(
    deprecated,
    reason = "the fixture verifies authored deprecation reaches generated records"
)]
mod authored_attributes {
    use model_api::prelude::*;

    #[record(fields(field(name = "value", value(item(prim = "Nat64")))))]
    #[deprecated(note = "use the current record")]
    #[repr(C)]
    pub struct LegacyRecord {}

    #[test]
    fn generated_record_preserves_rust_attributes_and_fields() {
        let record = LegacyRecord { value: 7 };
        assert_eq!(record.value, 7);
    }
}
