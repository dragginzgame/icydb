//! Finite workload and raw IC adapter for the isolated Wire actor.
//! This fixture owns no database, codec, CDK or simulator lifecycle.

/// Maximum argument/reply bytes admitted by the actor.
pub const MAX_FRAME_BYTES: usize = 64;

/// Local adapter failure; application results remain ordinary row counts.
#[derive(Debug, Eq, PartialEq)]
pub enum QueryCodecError {
    /// Request framing, identity, value or canonical encoding is invalid.
    InvalidRequest,
    /// The selected response encoder cannot produce the reply.
    Encoding,
}

/// Count matching keys in the finite row fixture.
#[must_use]
pub fn row_count(key: u64) -> u32 {
    [0, 7, 7, u64::MAX]
        .into_iter()
        .fold(0, |count, row| count + u32::from(row == key))
}

/// Serve one synchronous query through raw IC calls without a CDK dependency.
#[cfg(target_arch = "wasm32")]
pub fn serve_query(handler: fn(&[u8]) -> Result<Vec<u8>, QueryCodecError>) {
    let mut buffer = [0; MAX_FRAME_BYTES];
    let Some(request) = buffer.get_mut(..ic_platform::ic0::msg_arg_data_size()) else {
        ic_platform::ic0::msg_reject(b"codec query request limit");
        return;
    };
    ic_platform::ic0::msg_arg_data_copy(request, 0);
    match handler(request) {
        Ok(reply) if reply.len() <= MAX_FRAME_BYTES => {
            ic_platform::ic0::msg_reply_data_append(&reply);
            ic_platform::ic0::msg_reply();
        }
        Ok(_) | Err(_) => ic_platform::ic0::msg_reject(b"codec query rejected"),
    }
}
