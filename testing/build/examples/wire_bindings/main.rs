//! Generate browser bindings from the maintained native endpoint types.

mod bulk;
mod metadata;
mod samples;
mod vectors;

use std::{error::Error, fs, path::PathBuf};

use clap::Parser;
use icydb::__macro::RpcMethod;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use serde_reflection::{Samples, Tracer, TracerConfig};

#[derive(Parser)]
#[command(
    name = "wire_bindings",
    about = "Generate browser IC Wire bindings and exact vectors"
)]
struct Args {
    /// Directory receiving generated bindings, metadata and vectors.
    #[arg(value_name = "OUTPUT", default_value = "target/wire-browser")]
    out: PathBuf,
}

struct Generator {
    tracer: Tracer,
    registry: ic_wire::typescript::SchemaRegistry,
    samples: Samples,
    methods: Vec<Value>,
    vectors: Vec<(RpcMethod, bool, Vec<u8>, serde_reflection::Value)>,
}

impl Generator {
    fn new() -> Result<Self, Box<dyn Error>> {
        let mut value = Self {
            tracer: Tracer::new(
                TracerConfig::default()
                    .is_human_readable(false)
                    .record_samples_for_structs(true)
                    .default_string_value("0".into())
                    .default_borrowed_str_value("0"),
            ),
            registry: metadata::registry()?,
            samples: Samples::new(),
            methods: Vec::new(),
            vectors: Vec::new(),
        };
        samples::seed(&mut value.tracer, &mut value.samples)?;
        Ok(value)
    }

    fn vector<T: Serialize>(
        &mut self,
        endpoint: RpcMethod,
        response: bool,
        value: &T,
    ) -> Result<(), Box<dyn Error>> {
        // A fresh tracer records the value without merging generic Result
        // instances with different native success/error types.
        let (_, sample) =
            Tracer::new(TracerConfig::default()).trace_value(&mut Samples::new(), value)?;
        let bytes = if response {
            endpoint
                .encode_response(value, icydb::wire::limits(icydb::wire::MAX_REPLY_BYTES))
                .map_err(|error| format!("{error:?}"))?
        } else {
            endpoint
                .encode_request(value, icydb::wire::limits(icydb::wire::MAX_REQUEST_BYTES))
                .map_err(|error| format!("{error:?}"))?
        };
        self.vectors.push((endpoint, response, bytes, sample));
        Ok(())
    }

    fn method<Q, R, E>(&mut self, endpoint: RpcMethod) -> Result<(), Box<dyn Error>>
    where
        Q: Serialize + DeserializeOwned + ic_wire::typescript::Wire,
        R: Serialize + DeserializeOwned + ic_wire::typescript::Wire,
        E: Serialize + DeserializeOwned + ic_wire::typescript::Wire,
    {
        let (_, requests) = self.tracer.trace_type::<Q>(&self.samples)?;
        let (_, responses) = self.tracer.trace_type::<R>(&self.samples)?;
        let (_, errors) = self.tracer.trace_type::<E>(&self.samples)?;
        let mode = match endpoint {
            RpcMethod::Query
            | RpcMethod::Metrics
            | RpcMethod::Snapshot
            | RpcMethod::Schema
            | RpcMethod::SchemaMigration => "query",
            RpcMethod::Ddl
            | RpcMethod::Integrity
            | RpcMethod::Update
            | RpcMethod::FixturesReset
            | RpcMethod::FixturesLoad
            | RpcMethod::MetricsReset
            | RpcMethod::SchemaMigrate => "update",
        };
        let browser_limits = |bytes| ic_wire::Limits {
            nodes: icydb_schema::wire::BROWSER_MAX_NODES,
            depth: icydb_schema::wire::BROWSER_MAX_DEPTH,
            ..icydb_schema::wire::limits(bytes)
        };
        // Wire validates directional budget projection; IcyDB retains its
        // intentionally smaller browser policy and semantic endpoint identity.
        let mut method = ic_wire::typescript::method_limits(
            browser_limits(icydb::wire::MAX_REQUEST_BYTES),
            browser_limits(icydb::wire::MAX_REPLY_BYTES),
        )
        .map_err(|error| format!("Wire browser budget projection failed: {error:?}"))?;
        method["name"] = json!(endpoint.name());
        method["id"] = json!(endpoint.id().to_string());
        method["mode"] = json!(mode);
        method["request"] = self
            .registry
            .register::<Q>(&format!("{}_Request", endpoint.name()))
            .map_err(|error| format!("Wire request metadata: {error:?}"))?;
        method["response"] = self
            .registry
            .register::<Result<R, E>>(&format!("{}_Response", endpoint.name()))
            .map_err(|error| format!("Wire reply metadata: {error:?}"))?;
        self.methods.push(method);
        for value in requests {
            self.vector(endpoint, false, &value)?;
        }
        for value in responses {
            self.vector(endpoint, true, &Ok::<_, E>(value))?;
        }
        for value in errors {
            self.vector(endpoint, true, &Err::<R, _>(value))?;
        }
        Ok(())
    }

    fn generate(mut self, out: &std::path::Path) -> Result<(), Box<dyn Error>> {
        self.method::<String, icydb::db::sql::SqlQueryResult, icydb::Error>(RpcMethod::Query)?;
        self.method::<String, icydb::db::sql::SqlQueryResult, icydb::Error>(RpcMethod::Ddl)?;
        self.method::<String, icydb::db::sql::SqlQueryResult, icydb::Error>(RpcMethod::Update)?;
        self.method::<String, icydb::db::IntegrityCheckResult, icydb::db::SqlIntegrityError>(
            RpcMethod::Integrity,
        )?;
        self.method::<(), (), icydb::Error>(RpcMethod::FixturesReset)?;
        self.method::<(), (), icydb::Error>(RpcMethod::FixturesLoad)?;
        self.method::<(), icydb::metrics::MetricsReport, icydb::Error>(RpcMethod::Metrics)?;
        self.method::<(), (), icydb::Error>(RpcMethod::MetricsReset)?;
        self.method::<(), icydb::db::StorageReport, icydb::Error>(RpcMethod::Snapshot)?;
        self.method::<(), Vec<icydb::db::EntitySchemaDescription>, icydb::Error>(
            RpcMethod::Schema,
        )?;
        self.method::<icydb::db::SchemaMigrationCommand, icydb::db::SchemaMigrationStatusPage, icydb::Error>(RpcMethod::SchemaMigrate)?;
        self.method::<icydb::db::SchemaMigrationStatusRequest, icydb::db::SchemaMigrationStatusPage, icydb::Error>(RpcMethod::SchemaMigration)?;
        vectors::add(&mut self)?;
        let registry = self
            .registry
            .finish()
            .map_err(|error| format!("Wire registry: {error:?}"))?;
        fs::create_dir_all(out)?;
        metadata::write(out, &registry, &self.methods)?;
        vectors::write(out, &registry, &self.methods, &self.vectors)?;
        println!(
            "Generated {} methods, {} native types and {} exact vectors",
            self.methods.len(),
            registry.len(),
            self.vectors.len()
        );
        Ok(())
    }
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    Generator::new()?.generate(&args.out)
}

#[cfg(test)]
mod tests {
    use super::Args;
    use clap::{Parser as _, error::ErrorKind};
    use std::path::PathBuf;

    #[test]
    fn output_argument_retains_default_and_explicit_paths() {
        assert_eq!(
            Args::try_parse_from(["wire_bindings"]).unwrap().out,
            PathBuf::from("target/wire-browser")
        );
        assert_eq!(
            Args::try_parse_from(["wire_bindings", "a directory"])
                .unwrap()
                .out,
            PathBuf::from("a directory")
        );
        assert_eq!(
            Args::try_parse_from(["wire_bindings", "--", "-output"])
                .unwrap()
                .out,
            PathBuf::from("-output")
        );
    }

    #[test]
    fn help_and_invalid_arguments_exit_before_generation() {
        for (args, kind) in [
            (vec!["wire_bindings", "--help"], ErrorKind::DisplayHelp),
            (
                vec!["wire_bindings", "--unknown"],
                ErrorKind::UnknownArgument,
            ),
            (
                vec!["wire_bindings", "first", "second"],
                ErrorKind::UnknownArgument,
            ),
        ] {
            assert_eq!(Args::try_parse_from(args).err().unwrap().kind(), kind);
        }
    }

    #[cfg(unix)]
    #[test]
    fn native_output_path_bytes_are_preserved() {
        use std::{ffi::OsString, os::unix::ffi::OsStringExt as _};
        let path = OsString::from_vec(b"output-\xff".to_vec());
        let args = Args::try_parse_from([OsString::from("wire_bindings"), path.clone()]).unwrap();
        assert_eq!(args.out, PathBuf::from(path));
    }
}
