use proc_macro2::TokenStream;
use quote::quote;
use syn::Path;

const INTERNAL_CRATES: &[&str] = &[
    "icydb",
    "icydb-base",
    "icydb-build",
    "icydb-core",
    "icydb-error",
    "icydb-macros",
    "icydb-paths",
    "icydb-schema",
];

fn env_path(name: &str) -> Option<TokenStream> {
    std::env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .and_then(|value| syn::parse_str::<Path>(&value).ok())
        .map(|path| quote!(#path))
}

///
/// CratePaths
///
/// Resolves crate roots for generated code. Internal icydb crates default to
/// direct crate names to avoid meta-crate cycles; other crates prefer the
/// public `icydb::` facade. Env vars allow overrides:
/// `ICYDB_CORE_CRATE`, `ICYDB_SCHEMA_CRATE`, `ICYDB_ERROR_CRATE`.
///

#[derive(Clone, Debug, Default)]
pub struct CratePaths {
    pub core: TokenStream,
    pub schema: TokenStream,
    pub error: TokenStream,
}

impl CratePaths {
    #[must_use]
    pub fn new() -> Self {
        let pkg = std::env::var("CARGO_PKG_NAME").unwrap_or_default();
        let use_meta_paths = !INTERNAL_CRATES.contains(&pkg.as_str());

        let core = if use_meta_paths {
            quote!(icydb::core)
        } else {
            quote!(icydb_core)
        };

        let schema = if use_meta_paths {
            quote!(icydb::schema)
        } else {
            quote!(icydb_schema)
        };

        let error = if use_meta_paths {
            quote!(icydb::error)
        } else {
            quote!(icydb_error)
        };

        Self {
            core: env_path("ICYDB_CORE_CRATE").unwrap_or(core),
            schema: env_path("ICYDB_SCHEMA_CRATE").unwrap_or(schema),
            error: env_path("ICYDB_ERROR_CRATE").unwrap_or(error),
        }
    }
}

/// Singleton accessor for proc-macro contexts.
#[must_use]
pub fn paths() -> CratePaths {
    CratePaths::new()
}
