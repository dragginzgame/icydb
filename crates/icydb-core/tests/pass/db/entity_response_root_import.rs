use icydb_core::{
    db::EntityResponse,
    entity::EntityKind,
};

fn accept_response<E: EntityKind>(_response: Option<EntityResponse<E>>) {}

fn main() {}
