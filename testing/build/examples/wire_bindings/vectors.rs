//! Actual native encoders are the byte oracle for generated browser codecs.

use crate::{Generator, samples};
use icydb::__macro::RpcMethod;
use std::{collections::BTreeMap, error::Error, fmt::Write as _, fs, path::Path};

use icydb::{
    Error as PublicError, ErrorKind, ErrorOrigin, RuntimeErrorKind,
    db::{
        RowProjectionOutput,
        sql::{SqlGroupedRowsOutput, SqlQueryResult},
    },
    types::{Float32, Float64},
    value::{OutputValue, PublicValue},
};
use serde_json::Value;
use serde_reflection::Value as Sample;

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub(super) fn add(generator: &mut Generator) -> Result<()> {
    let mut values = samples::values()?;
    values.extend([
        PublicValue::Int64(i64::MIN),
        PublicValue::Int64(i64::MAX),
        PublicValue::Int128(i128::MIN),
        PublicValue::Int128(i128::MAX),
        PublicValue::Nat64(u64::MAX),
        PublicValue::Nat128(u128::MAX),
        PublicValue::Float32(Float32::try_new(-0.0).ok_or("float sample")?),
        PublicValue::Float32(Float32::try_new(f32::from_bits(1)).ok_or("float sample")?),
        PublicValue::Float64(Float64::try_new(-0.0).ok_or("float sample")?),
        PublicValue::Float64(Float64::try_new(f64::from_bits(1)).ok_or("float sample")?),
        PublicValue::Text("\u{feff}é🦀NULL".into()),
        PublicValue::List(vec![PublicValue::Map(vec![(
            PublicValue::Null,
            PublicValue::Text("NULL".into()),
        )])]),
    ]);
    for value in values {
        // The sealed output root deliberately shares native PublicValue bytes.
        let bytes = icydb::wire::encode_one(value).map_err(|e| format!("{e:?}"))?;
        let value: OutputValue = icydb::wire::decode_one(&bytes).map_err(|e| format!("{e:?}"))?;
        let reply: std::result::Result<_, PublicError> =
            Ok(SqlQueryResult::Projection(RowProjectionOutput {
                entity: "Entry".into(),
                columns: vec!["value".into()],
                rows: vec![vec![value]],
                row_count: 1,
            }));
        generator.vector(RpcMethod::Query, true, &reply)?;
    }
    let grouped: std::result::Result<_, PublicError> =
        Ok(SqlQueryResult::Grouped(SqlGroupedRowsOutput {
            entity: "Entry".into(),
            columns: vec!["value".into(), "count".into()],
            rows: vec![
                vec!["NULL".into(), "7".into()],
                vec!["\"NULL\"".into(), "2".into()],
            ],
            row_count: 2,
            next_cursor: Some("opaque-authenticated-cursor".into()),
        }));
    generator.vector(RpcMethod::Query, true, &grouped)?;
    let reply: std::result::Result<SqlQueryResult, _> = Err(PublicError::from_kind(
        ErrorKind::Runtime(RuntimeErrorKind::Internal),
        ErrorOrigin::Response,
    ));
    generator.vector(RpcMethod::Query, true, &reply)?;
    generator.vector(
        RpcMethod::Query,
        false,
        &"SELECT value, COUNT(*) FROM Entry GROUP BY value /* é🦀 */",
    )?;
    Ok(())
}

fn js(node: &Value, sample: &Sample, types: &BTreeMap<String, Value>) -> Result<String> {
    let kind = node["k"].as_str().ok_or("missing vector schema")?;
    if kind == "ref" {
        let name = node["name"].as_str().ok_or("missing vector type")?;
        return js(types.get(name).ok_or("unknown vector type")?, sample, types);
    }
    if kind == "newtype" {
        return js(&node["item"], sample, types);
    }
    Ok(match sample {
        Sample::Unit | Sample::Option(None) => "null".into(),
        Sample::Bool(value) => value.to_string(),
        Sample::I8(value) => value.to_string(),
        Sample::I16(value) => value.to_string(),
        Sample::I32(value) => value.to_string(),
        Sample::U8(value) => value.to_string(),
        Sample::U16(value) => value.to_string(),
        Sample::U32(value) => value.to_string(),
        Sample::I64(value) => format!("{value}n"),
        Sample::I128(value) => format!("{value}n"),
        Sample::U64(value) => format!("{value}n"),
        Sample::U128(value) => format!("{value}n"),
        Sample::F32(value) => format!("{:?}", f64::from(*value)),
        Sample::F64(value) => format!("{value:?}"),
        Sample::Char(_) => return Err("unsupported char vector".into()),
        Sample::Str(value) => serde_json::to_string(value)?,
        Sample::Bytes(value) => format!("new Uint8Array({})", serde_json::to_string(value)?),
        Sample::Option(Some(value)) => format!("{{some:{}}}", js(&node["item"], value, types)?),
        Sample::Variant(id, value) if kind == "result" => {
            let key = if *id == 0 {
                "ok"
            } else if *id == 1 {
                "err"
            } else {
                return Err("invalid result vector".into());
            };
            format!("{{{key}:{}}}", js(&node[key], value, types)?)
        }
        Sample::Variant(id, value) => {
            let variant = node["variants"]
                .as_array()
                .ok_or("missing vector variants")?
                .iter()
                .find(|v| v["id"] == *id)
                .ok_or("unknown vector variant")?;
            format!(
                "{{tag:{},value:{}}}",
                variant["name"],
                js(&variant["value"], value, types)?
            )
        }
        Sample::Seq(values) if kind == "record" => {
            let fields = node["fields"].as_array().ok_or("missing vector fields")?;
            if fields.len() != values.len() {
                return Err("native vector field mismatch".into());
            }
            format!(
                "{{{}}}",
                fields
                    .iter()
                    .zip(values)
                    .map(|(field, value)| Ok(format!(
                        "{}:{}",
                        field["name"],
                        js(&field["value"], value, types)?
                    )))
                    .collect::<Result<Vec<String>>>()?
                    .join(",")
            )
        }
        Sample::Seq(values) => {
            let items = values
                .iter()
                .enumerate()
                .map(|(i, value)| {
                    js(
                        if kind == "tuple" {
                            &node["items"][i]
                        } else {
                            &node["item"]
                        },
                        value,
                        types,
                    )
                })
                .collect::<Result<Vec<_>>>()?;
            format!("[{}]", items.join(","))
        }
    })
}

pub(super) fn write(
    out: &Path,
    types: &BTreeMap<String, Value>,
    methods: &[Value],
    vectors: &[(RpcMethod, bool, Vec<u8>, Sample)],
) -> Result<()> {
    let mut source = String::from(
        "// Exact frames emitted by the current native IcyDB encoder.\nexport const vectors = [\n",
    );
    for (endpoint, response, bytes, sample) in vectors {
        let method = methods
            .iter()
            .find(|m| m["name"] == endpoint.name())
            .ok_or("missing vector method")?;
        let value = js(
            &method[if *response { "response" } else { "request" }],
            sample,
            types,
        )?;
        writeln!(
            source,
            "{{method:{},response:{response},bytes:new Uint8Array({}),value:{value}}},",
            serde_json::to_string(endpoint.name())?,
            serde_json::to_string(bytes)?
        )?;
    }
    source.push_str("];\n");
    fs::write(out.join("vectors.mjs"), source)?;
    Ok(())
}
