//! Native callers for the maintained raw Wire fixture contracts.

use ic_platform::Principal;
use ic_testkit::pocket_ic::PocketIc;
use icydb_testing_wasm_helpers::fixture_wire;
use serde::{Serialize, de::DeserializeOwned};

/// Raw PocketIC transport for generated and fixture-owned Wire methods.
pub trait WireCallExt {
    /// Query as the anonymous principal.
    fn query_wire<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String>;
    /// Query with an explicit caller identity.
    fn query_wire_as<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        caller: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String>;
    /// Update as the anonymous principal.
    fn update_wire<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String>;
    /// Update with an explicit caller identity.
    fn update_wire_as<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        caller: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String>;
}

impl WireCallExt for PocketIc {
    fn query_wire<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String> {
        self.query_wire_as(canister, Principal::anonymous(), method, arguments)
    }
    fn query_wire_as<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        caller: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String> {
        let request =
            fixture_wire::encode_request(method, arguments).map_err(|error| error.to_string())?;
        let reply = self
            .query_call(canister, caller, method, request)
            .map_err(|error| format!("{error:?}"))?;
        fixture_wire::decode_response(method, &reply).map_err(|error| error.to_string())
    }
    fn update_wire<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String> {
        self.update_wire_as(canister, Principal::anonymous(), method, arguments)
    }
    fn update_wire_as<T: Serialize + DeserializeOwned, A: Serialize>(
        &self,
        canister: Principal,
        caller: Principal,
        method: &str,
        arguments: A,
    ) -> Result<T, String> {
        let request =
            fixture_wire::encode_request(method, arguments).map_err(|error| error.to_string())?;
        let reply = self
            .update_call(canister, caller, method, request)
            .map_err(|error| format!("{error:?}"))?;
        fixture_wire::decode_response(method, &reply).map_err(|error| error.to_string())
    }
}
