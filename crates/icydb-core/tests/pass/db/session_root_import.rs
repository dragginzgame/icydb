use icydb_core::{
    db::DbSession,
    traits::CanisterKind,
};

fn accept_session<C: CanisterKind>(_session: Option<DbSession<C>>) {}

fn main() {}
