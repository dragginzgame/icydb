#![allow(clippy::missing_const_for_fn, clippy::unnecessary_wraps)]

mod __icydb_generated {
    pub(crate) const __ICYDB_START_BINDING: () = ();

    #[cfg(feature = "metrics")]
    pub(crate) mod endpoint_handlers {
        pub(crate) fn metrics() -> Result<icydb::metrics::MetricsReport, icydb::Error> {
            Ok(icydb::metrics::MetricsReport::default())
        }

        pub(crate) fn metrics_reset() -> Result<(), icydb::Error> {
            Ok(())
        }
    }

    #[cfg(feature = "metrics")]
    pub(crate) mod endpoint_authorization {
        pub(crate) fn require_operational_controller() -> Result<(), icydb::Error> {
            Ok(())
        }
    }
}

icydb::endpoints! {
    #[cfg(any())]
    icydb_ddl;
    #[cfg(feature = "metrics")]
    icydb_metrics(authorization = public);
    #[cfg(feature = "metrics")]
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

    let erased: icydb::db::RequestExecutionFuture<'_, u8> =
        icydb::db::with_request_execution_async(async { 11 });
    let mut erased = Box::pin(erased);
    assert_eq!(
        erased.as_mut().poll(&mut context),
        std::task::Poll::Ready(11)
    );
}

#[icydb::test]
fn request_test_attribute_uses_the_runtime_boundary() {
    assert_eq!(attributed_sync_entry(), 7);
}

#[allow(dead_code)]
fn main() {}
