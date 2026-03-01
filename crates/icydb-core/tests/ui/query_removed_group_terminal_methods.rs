use icydb_core::{
    db::{MissingRowPolicy, Query},
    traits::EntityKind,
};

fn assert_removed_group_methods<E: EntityKind>() {
    let _ = Query::<E>::new(MissingRowPolicy::Ignore).group_count();
}

fn main() {}
