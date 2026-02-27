use icydb_core::{
    db::DbSession,
    traits::{CanisterKind, EntityKind},
};

fn assert_load_group_by_missing<C, E>(session: &DbSession<C>)
where
    C: CanisterKind,
    E: EntityKind<Canister = C>,
{
    let _ = session.load::<E>().group_by("rank");
}

fn main() {}
