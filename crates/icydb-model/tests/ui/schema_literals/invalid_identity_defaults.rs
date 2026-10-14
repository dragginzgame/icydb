use icydb_model::prelude::*;

#[record(fields(field(name = "account", value(item(prim = "Account")), default = "zz")), traits(add(Default)))]
pub struct BadAccount;

#[record(fields(field(name = "owner", value(item(prim = "Principal")), default = "zz")), traits(add(Default)))]
pub struct BadPrincipal;

#[record(fields(field(name = "sub", value(item(prim = "Subaccount")), default = "aé0000000000000000000000000000000000000000000000000000000000000")), traits(add(Default)))]
pub struct BadSubaccount;

#[record(fields(field(name = "id", value(item(prim = "Ulid")), default = "zz")), traits(add(Default)))]
pub struct BadUlid;

fn main() {}
