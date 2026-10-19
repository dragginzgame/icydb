//! Generate browser bindings from the maintained native endpoint types.

mod metadata;
mod samples;
mod vectors;

use std::{env, error::Error, fs, path::PathBuf};

use icydb::wire::Endpoint;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use serde_reflection::{Samples, Tracer, TracerConfig};

struct Generator {
    tracer: Tracer,
    samples: Samples,
    methods: Vec<Value>,
    vectors: Vec<(Endpoint, bool, Vec<u8>, serde_reflection::Value)>,
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
            samples: Samples::new(),
            methods: Vec::new(),
            vectors: Vec::new(),
        };
        samples::seed(&mut value.tracer, &mut value.samples)?;
        Ok(value)
    }

    fn vector<T: Serialize>(
        &mut self,
        endpoint: Endpoint,
        response: bool,
        value: &T,
    ) -> Result<(), Box<dyn Error>> {
        // A fresh tracer records the value without merging generic Result
        // instances with different native success/error types.
        let (_, sample) =
            Tracer::new(TracerConfig::default()).trace_value(&mut Samples::new(), value)?;
        let bytes = if response {
            endpoint
                .encode_response(value)
                .map_err(|error| format!("{error:?}"))?
        } else {
            endpoint
                .encode_request(value)
                .map_err(|error| format!("{error:?}"))?
        };
        self.vectors.push((endpoint, response, bytes, sample));
        Ok(())
    }

    fn method<Q, R, E>(&mut self, endpoint: Endpoint) -> Result<(), Box<dyn Error>>
    where
        Q: Serialize + DeserializeOwned,
        R: Serialize + DeserializeOwned,
        E: Serialize + DeserializeOwned,
    {
        let (request, requests) = self.tracer.trace_type::<Q>(&self.samples)?;
        let (response, responses) = self.tracer.trace_type::<R>(&self.samples)?;
        let (error, errors) = self.tracer.trace_type::<E>(&self.samples)?;
        let mode = match endpoint {
            Endpoint::Query
            | Endpoint::Metrics
            | Endpoint::Snapshot
            | Endpoint::Schema
            | Endpoint::SchemaMigration => "query",
            Endpoint::Ddl
            | Endpoint::Integrity
            | Endpoint::Update
            | Endpoint::FixturesReset
            | Endpoint::FixturesLoad
            | Endpoint::MetricsReset
            | Endpoint::SchemaMigrate => "update",
        };
        self.methods.push(json!({
            "requestNodes":32768, "responseNodes":32768, "requestDepth":32, "responseDepth":32,
            "name": endpoint.name(), "id": endpoint.id().to_string(), "mode":mode,
            "requestBytes":icydb::wire::MAX_REQUEST_BYTES, "responseBytes":icydb::wire::MAX_REPLY_BYTES,
            "request": metadata::format(&request)?,
            "response": {"k":"result", "ok":metadata::format(&response)?, "err":metadata::format(&error)?},
        }));
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
        self.method::<String, icydb::db::sql::SqlQueryResult, icydb::Error>(Endpoint::Query)?;
        self.method::<String, icydb::db::sql::SqlQueryResult, icydb::Error>(Endpoint::Ddl)?;
        self.method::<String, icydb::db::sql::SqlQueryResult, icydb::Error>(Endpoint::Update)?;
        self.method::<String, icydb::db::IntegrityCheckResult, icydb::db::SqlIntegrityError>(
            Endpoint::Integrity,
        )?;
        self.method::<(), (), icydb::Error>(Endpoint::FixturesReset)?;
        self.method::<(), (), icydb::Error>(Endpoint::FixturesLoad)?;
        self.method::<(), icydb::metrics::MetricsReport, icydb::Error>(Endpoint::Metrics)?;
        self.method::<(), (), icydb::Error>(Endpoint::MetricsReset)?;
        self.method::<(), icydb::db::StorageReport, icydb::Error>(Endpoint::Snapshot)?;
        self.method::<(), Vec<icydb::db::EntitySchemaDescription>, icydb::Error>(Endpoint::Schema)?;
        self.method::<icydb::db::SchemaMigrationCommand, icydb::db::SchemaMigrationStatusPage, icydb::Error>(Endpoint::SchemaMigrate)?;
        self.method::<icydb::db::SchemaMigrationStatusRequest, icydb::db::SchemaMigrationStatusPage, icydb::Error>(Endpoint::SchemaMigration)?;
        samples::complete(&mut self.tracer, &self.samples)?;
        vectors::add(&mut self)?;
        let registry = self.tracer.registry()?;
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
    let out = env::args_os()
        .nth(1)
        .map_or_else(|| PathBuf::from("target/wire-browser"), PathBuf::from);
    Generator::new()?.generate(&out)
}
