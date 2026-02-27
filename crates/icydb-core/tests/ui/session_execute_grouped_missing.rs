use icydb_core::{
    db::DbSession,
    traits::CanisterKind,
};

fn assert_execute_grouped_missing<C>(session: &DbSession<C>)
where
    C: CanisterKind,
{
    let _ = session.execute_grouped();
}

fn main() {}
