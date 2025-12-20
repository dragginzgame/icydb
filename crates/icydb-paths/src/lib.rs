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
    /// Resolve crate paths for generated code, honoring environment overrides.
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

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use quote::quote;
    use std::env;
    use std::sync::Mutex;

    static ENV_LOCK: Mutex<()> = Mutex::new(());

    struct TempEnv {
        key: &'static str,
        prev: Option<String>,
    }

    impl TempEnv {
        fn set(key: &'static str, value: Option<&str>) -> Self {
            let prev = env::var(key).ok();
            unsafe {
                match value {
                    Some(v) => env::set_var(key, v),
                    None => env::remove_var(key),
                }
            }
            Self { key, prev }
        }
    }

    impl Drop for TempEnv {
        fn drop(&mut self) {
            unsafe {
                match &self.prev {
                    Some(value) => env::set_var(self.key, value),
                    None => env::remove_var(self.key),
                }
            }
        }
    }

    #[test]
    fn uses_internal_crate_names_inside_workspace() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _pkg = TempEnv::set("CARGO_PKG_NAME", Some("icydb-paths"));
        let _core = TempEnv::set("ICYDB_CORE_CRATE", None);
        let _schema = TempEnv::set("ICYDB_SCHEMA_CRATE", None);
        let _error = TempEnv::set("ICYDB_ERROR_CRATE", None);

        let paths = CratePaths::new();

        assert_eq!(paths.core.to_string(), quote!(icydb_core).to_string());
        assert_eq!(paths.schema.to_string(), quote!(icydb_schema).to_string());
        assert_eq!(paths.error.to_string(), quote!(icydb_error).to_string());
    }

    #[test]
    fn honors_env_overrides_for_external_consumers() {
        let _lock = ENV_LOCK.lock().unwrap();
        let _pkg = TempEnv::set("CARGO_PKG_NAME", Some("external-app"));
        let _core = TempEnv::set("ICYDB_CORE_CRATE", Some("custom::core"));
        let _schema = TempEnv::set("ICYDB_SCHEMA_CRATE", Some("custom::schema"));
        let _error = TempEnv::set("ICYDB_ERROR_CRATE", Some("custom::error"));

        let paths = CratePaths::new();

        assert_eq!(paths.core.to_string(), quote!(custom::core).to_string());
        assert_eq!(paths.schema.to_string(), quote!(custom::schema).to_string());
        assert_eq!(paths.error.to_string(), quote!(custom::error).to_string());
    }
}
