use icydb_core::db::executor::LoadExecutor;
use icydb_core::db::query::{Query, ReadConsistency};
use icydb_core::traits::EntityKind;

fn bad<E: EntityKind>(executor: LoadExecutor<E>) {
    let query = Query::<E>::new(ReadConsistency::MissingOk);
    let _ = executor.execute(query);
}

fn main() {}
