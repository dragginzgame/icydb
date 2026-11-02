use icydb::db::{RequestExecutionFuture, with_request_execution_async};
use std::{
    future::Future,
    task::{Context, Poll, Waker},
};

#[icydb::request_execution]
async fn entry() -> Result<u8, icydb::Error> {
    Ok(7)
}

#[derive(Debug, Eq, PartialEq)]
struct ApplicationError(icydb::Error);

impl From<icydb::Error> for ApplicationError {
    fn from(error: icydb::Error) -> Self {
        Self(error)
    }
}

#[icydb::request_execution]
async fn application_entry() -> Result<u8, ApplicationError> {
    Ok(entry().await?)
}

fn main() {
    let mut context = Context::from_waker(Waker::noop());
    let mut attributed = Box::pin(application_entry());
    assert_eq!(attributed.as_mut().poll(&mut context), Poll::Ready(Ok(7)));
    let erased: RequestExecutionFuture<'_, u8> = with_request_execution_async(async { Ok(11) });
    assert_eq!(
        Box::pin(erased).as_mut().poll(&mut context),
        Poll::Ready(Ok(11))
    );
}
