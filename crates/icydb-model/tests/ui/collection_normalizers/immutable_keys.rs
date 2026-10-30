use icydb_model::prelude::*;

#[set(item(
    prim = "Text", unbounded,
    normalizer(path = "base::normalizer::text::Trim")
))]
pub struct SetKeys;

#[map(
    key(prim = "Text", unbounded, normalizer(path = "base::normalizer::text::Trim")),
    value(item(prim = "Text", unbounded))
)]
pub struct MapKeys;

fn main() {}
