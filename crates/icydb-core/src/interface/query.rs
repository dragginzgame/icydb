use crate::{
    Error, Key,
    db::query::{DeleteQuery, LoadQuery, SaveQuery},
    interface::InterfaceError,
};
use candid::Principal;
use canic::{Error as CanicError, cdk::call::Call};
use thiserror::Error as ThisError;

///
/// QueryError
///

#[derive(Debug, ThisError)]
pub enum QueryError {
    #[error("entity not found: {0}")]
    EntityNotFound(String),
}

impl From<QueryError> for Error {
    fn from(err: QueryError) -> Self {
        InterfaceError::from(err).into()
    }
}

// query_load
pub async fn query_load(pid: Principal, query: LoadQuery) -> Result<Vec<Key>, Error> {
    query_call(pid, "icydb_query_load", query).await
}

// query_save
pub async fn query_save(pid: Principal, query: SaveQuery) -> Result<Key, Error> {
    query_call(pid, "icydb_query_save", query).await
}

// query_delete
pub async fn query_delete(pid: Principal, query: DeleteQuery) -> Result<Vec<Key>, Error> {
    query_call(pid, "icydb_query_delete", query).await
}

// query_call
// private helper method
async fn query_call<T: candid::CandidType + for<'de> candid::Deserialize<'de>>(
    pid: Principal,
    method: &str,
    arg: impl candid::CandidType,
) -> Result<T, Error> {
    let result = Call::unbounded_wait(pid, method)
        .with_arg(arg)
        .await
        .map_err(CanicError::from)?;

    let response = result.candid::<T>().map_err(CanicError::from)?;

    Ok(response)
}
