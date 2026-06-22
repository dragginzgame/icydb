//! Module: db::session::sql::projection::contract
//! Responsibility: carry outward SQL projection labels and display scales.
//! Does not own: projection execution, prepared-plan cache state, or payload
//! assembly.
//! Boundary: keeps the session SQL result contract with the projection helpers
//! that derive it.

///
/// SqlProjectionContract
///
/// SqlProjectionContract is the outward SQL projection contract derived from
/// one shared lower prepared plan. SQL execution keeps this wrapper so
/// statement shaping stays owner-local while prepared-plan reuse lives below it.
///

#[derive(Clone, Debug)]
pub(in crate::db) struct SqlProjectionContract {
    columns: Vec<String>,
    fixed_scales: Vec<Option<u32>>,
}

impl SqlProjectionContract {
    #[must_use]
    pub(in crate::db) const fn new(columns: Vec<String>, fixed_scales: Vec<Option<u32>>) -> Self {
        Self {
            columns,
            fixed_scales,
        }
    }

    #[must_use]
    pub(in crate::db) fn into_components(self) -> (Vec<String>, Vec<Option<u32>>) {
        (self.columns, self.fixed_scales)
    }
}
