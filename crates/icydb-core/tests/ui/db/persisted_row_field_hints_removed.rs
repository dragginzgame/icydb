use icydb_derive::PersistedRow;

#[derive(PersistedRow)]
struct RemovedHintEntity {
    #[icydb(meta)]
    payload: String,
}

fn main() {}
