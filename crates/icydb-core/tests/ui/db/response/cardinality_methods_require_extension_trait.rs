use icydb_core::{
    db::EntityResponse,
    entity::EntityKind,
};

fn dto_boundary_guard<E: EntityKind>(response: EntityResponse<E>) {
    let _ = response.require_one();
}

fn main() {}
