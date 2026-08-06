#![allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]

use std::future::Future as _;

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    pub(crate) mod endpoint_handlers {
        pub(crate) fn metrics(
            _: Option<u64>,
        ) -> Result<icydb::metrics::CompactMetricsReport, icydb::Error> {
            Ok(icydb::metrics::CompactMetricsReport::default())
        }

        pub(crate) fn metrics_extended(
            _: Option<u64>,
        ) -> Result<icydb::metrics::EventReport, icydb::Error> {
            Ok(icydb::metrics::EventReport::default())
        }

        pub(crate) fn metrics_reset() -> Result<(), icydb::Error> {
            Ok(())
        }
    }

    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_operational_controller() -> Result<(), icydb::Error> {
            Ok(())
        }
    }
}

icydb::endpoints! {
    #[cfg(any())]
    icydb_ddl;
    icydb_metrics(authorization = public);
    icydb_metrics_extended(authorization = public);
    icydb_metrics_reset;
}

#[icydb::request_execution]
fn attributed_sync_entry() -> u8 {
    7
}

#[icydb::request_execution]
#[ic_cdk::query]
fn attributed_ic_cdk_entry() -> u8 {
    attributed_sync_entry()
}

#[icydb::request_execution]
async fn attributed_async_entry() -> u8 {
    std::future::ready(()).await;
    9
}

#[test]
fn public_endpoint_facade_compile_contract() {
    assert_eq!(attributed_sync_entry(), 7);
    assert_eq!(attributed_ic_cdk_entry(), 7);
    assert_eq!(
        icydb::db::with_request_execution(|| 7),
        attributed_sync_entry()
    );

    let mut future = Box::pin(attributed_async_entry());
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());

    assert_eq!(
        future.as_mut().poll(&mut context),
        std::task::Poll::Ready(9)
    );
}

#[icydb::test]
fn request_test_attribute_uses_the_runtime_boundary() {
    assert_eq!(attributed_sync_entry(), 7);
}

#[allow(dead_code)]
fn main() {}
