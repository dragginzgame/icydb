use icydb_core::{
    db::DbSession,
    traits::{CanisterKind, EntityKind, EntityValue},
};

fn assert_group_min_by_requires_result_handling<C, E>(session: &DbSession<C>)
where
    C: CanisterKind,
    E: EntityKind<Canister = C> + EntityValue,
{
    let _ = session
        .load::<E>()
        .group_by("rank")
        .unwrap()
        .group_min_by("rank")
        .group_count();
}

fn main() {}
