//! Renamed-dependency typed-adapter compile fixture.

#[cfg(test)]
mod actor_hygiene;
#[cfg(test)]
mod actor_schema;
mod schema;

#[cfg(test)]
use actor_hygiene::icydb_storage_grants;

#[cfg(test)]
mod tests;

#[cfg(test)]
mod crate_path_tests;
