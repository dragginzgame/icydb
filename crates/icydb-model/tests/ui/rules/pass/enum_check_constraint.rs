use icydb_model::prelude::*;

#[canister(
    memory_namespace = "enum_check",
    memory_min = 100,
    memory_max = 106,
    commit_memory_id = 104,
    startup_memory_id = 106,
    integrity_progress_memory_id = 105
)]
pub struct EnumCheckCanister {}

#[store(
    canister = "EnumCheckCanister",
    storage(journaled(
        data_memory_id = 100,
        index_memory_id = 101,
        schema_memory_id = 102,
        journal_memory_id = 103,
    ))
)]
pub struct EnumCheckStore {}

#[enum_(variant(name = "Rocky"), variant(name = "GasGiant"))]
pub struct BodyClass {}

#[enum_(variant(name = "Dry"), variant(name = "Ocean"), variant(name = "NotApplicable"))]
pub struct SurfaceWater {}

#[entity(
    store = "EnumCheckStore",
    version = 1,
    pk(field = "id"),
    constraint(
        name = "body_surface_water_pair",
        check = "(body_class = 'Rocky' AND surface_water != 'NotApplicable') OR (body_class = 'GasGiant' AND surface_water = 'NotApplicable')"
    ),
    fields(
        field(name = "id", value(item(prim = "Ulid"))),
        field(name = "body_class", value(item(is = "BodyClass"))),
        field(name = "surface_water", value(item(is = "SurfaceWater")))
    )
)]
pub struct Planet {}

fn main() {}
