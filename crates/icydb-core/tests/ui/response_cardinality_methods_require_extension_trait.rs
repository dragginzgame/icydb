use icydb_core::{
    db::EntityResponse,
    traits::EntityKind,
};

fn dto_boundary_guard<E: EntityKind>(response: EntityResponse<E>) {
    let _ = response.require_one();
}

fn main() {}
