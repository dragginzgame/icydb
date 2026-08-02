//! Module: build::actor::endpoint
//! Responsibility: emit private authorization helpers used by source-declared endpoints.
//! Does not own: endpoint declaration syntax, public wrappers, or private handler bodies.
//! Boundary: package-private actor-codegen support for the public source macro.

use proc_macro2::TokenStream;
use quote::quote;

/// Emit the private controller-authorization helpers used by the fixed wrappers.
pub(crate) fn emit_endpoint_authorization_helpers() -> TokenStream {
    quote! {
        pub(crate) mod endpoint_authorization {
            fn require_controller(
                boundary: ::icydb::diagnostic::RuntimeBoundaryCode,
            ) -> Result<(), ::icydb::Error> {
                let caller = ::icydb::__reexports::ic_cdk::api::msg_caller();
                if !::icydb::__reexports::ic_cdk::api::is_controller(&caller) {
                    return Err(::icydb::Error::from_runtime_boundary(
                        boundary,
                        ::icydb::ErrorOrigin::Interface,
                    ));
                }

                Ok(())
            }

            pub(crate) fn require_sql_controller() -> Result<(), ::icydb::Error> {
                require_controller(
                    ::icydb::diagnostic::RuntimeBoundaryCode::SqlSurfaceControllerRequired,
                )
            }

            pub(crate) fn require_operational_controller() -> Result<(), ::icydb::Error> {
                require_controller(
                    ::icydb::diagnostic::RuntimeBoundaryCode::OperationalSurfaceControllerRequired,
                )
            }

            pub(crate) fn require_schema_controller() -> Result<(), ::icydb::Error> {
                require_controller(
                    ::icydb::diagnostic::RuntimeBoundaryCode::SchemaSurfaceControllerRequired,
                )
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::emit_endpoint_authorization_helpers;

    #[test]
    fn authorization_helpers_preserve_the_three_frozen_controller_boundaries() {
        let tokens = emit_endpoint_authorization_helpers().to_string();

        for boundary in [
            "SqlSurfaceControllerRequired",
            "OperationalSurfaceControllerRequired",
            "SchemaSurfaceControllerRequired",
        ] {
            assert!(tokens.contains(boundary));
        }
        assert!(tokens.contains("ErrorOrigin :: Interface"));
        assert!(tokens.contains("is_controller"));
    }
}
