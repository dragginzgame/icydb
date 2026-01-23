mod save;
pub mod v2;

pub use save::*;

// create
#[must_use]
/// Build an insert `SaveQuery`.
pub fn insert() -> SaveQuery {
    SaveQuery::new(SaveMode::Insert)
}

// update
#[must_use]
/// Build an update `SaveQuery`.
pub fn update() -> SaveQuery {
    SaveQuery::new(SaveMode::Update)
}

// replace
#[must_use]
/// Build a replace `SaveQuery`.
pub fn replace() -> SaveQuery {
    SaveQuery::new(SaveMode::Replace)
}
