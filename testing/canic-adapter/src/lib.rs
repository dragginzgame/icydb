//! Canic-dependent fixture serving and qualification; never a database dependency.

use icydb::wire::{MAX_REPLY_BYTES, MAX_REQUEST_BYTES, limits};
use serde::{Serialize, de::DeserializeOwned};

#[doc(hidden)]
pub mod __private {
    pub use icydb_testing_wasm_helpers::fixture_wire;
}

/// Serve a fixture method through the same raw admission owner as generated endpoints.
///
/// # Panics
/// Panics when called outside an IC canister entry point.
pub fn serve<Q, R>(id: u64, handler: impl FnOnce(Q) -> R)
where
    Q: DeserializeOwned,
    R: Serialize,
{
    // Keep runtime name dispatch on the native caller side. Each Wasm export
    // instantiates only its own request decoder and identity prefix.
    canic_icydb::serve_raw(
        MAX_REQUEST_BYTES,
        |bytes| ic_wire::native::decode_request(id, bytes, limits(MAX_REQUEST_BYTES)),
        |response| ic_wire::native::encode_response(id, &response, limits(MAX_REPLY_BYTES)),
        handler,
    );
}

/// Serve an asynchronous fixture handler using Canic's bounded transport.
///
/// # Panics
/// Panics outside an IC entrypoint; reply failure traps the current message.
pub async fn serve_async<Q, R, F>(id: u64, handler: impl FnOnce(Q) -> F)
where
    Q: DeserializeOwned,
    R: Serialize,
    F: std::future::Future<Output = R>,
{
    canic_icydb::serve_raw_async(
        MAX_REQUEST_BYTES,
        |bytes| ic_wire::native::decode_request(id, bytes, limits(MAX_REQUEST_BYTES)),
        |response| ic_wire::native::encode_response(id, &response, limits(MAX_REPLY_BYTES)),
        handler,
    )
    .await;
}

/// Declare one raw fixture export alongside its ordinary native Rust handler.
#[macro_export]
macro_rules! export_wire {
    ($(#[$cfg:meta])* $mode:ident, $name:literal, $marker:ident, $function:ident, ($($arg:ident: $ty:ty),* $(,)?), $return:ty) => {
        canic_icydb::wire_endpoint! {
            $(#[$cfg])*
            $mode, $name,
            $crate::__private::fixture_wire::$marker::ID,
            {
                 $crate::serve::<_, _>($crate::__private::fixture_wire::$marker::ID, |($($arg,)*): ($($ty,)*)| -> $return { $function($($arg),*) });
            }
        }
    };
}
