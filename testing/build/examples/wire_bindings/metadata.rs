//! Native Serde metadata and TypeScript declarations, separate from runtime admission.

use std::{collections::BTreeMap, error::Error, fs, path::Path};

use ic_wire::typescript::{RUNTIME, generate_registry};
use serde_json::{Value, json};
use serde_reflection::{ContainerFormat, Format, Named, Registry, VariantFormat};

type Result<T> = std::result::Result<T, Box<dyn Error>>;

pub(super) fn format(value: &Format) -> Result<Value> {
    Ok(match value {
        Format::Variable(_) => return Err("incomplete native metadata".into()),
        Format::TypeName(name) => json!({"k":"ref","name":name}),
        Format::Unit => json!({"k":"unit"}),
        Format::Bool => json!({"k":"bool"}),
        Format::I8 => json!({"k":"i","bits":8}),
        Format::I16 => json!({"k":"i","bits":16}),
        Format::I32 => json!({"k":"i","bits":32}),
        Format::I64 => json!({"k":"i","bits":64}),
        Format::I128 => json!({"k":"i","bits":128}),
        Format::U8 => json!({"k":"u","bits":8}),
        Format::U16 => json!({"k":"u","bits":16}),
        Format::U32 => json!({"k":"u","bits":32}),
        Format::U64 => json!({"k":"u","bits":64}),
        Format::U128 => json!({"k":"u","bits":128}),
        Format::F32 => json!({"k":"float","bits":32}),
        Format::F64 => json!({"k":"float","bits":64}),
        Format::Char => return Err("native char contract is unsupported".into()),
        Format::Str => json!({"k":"text"}),
        Format::Bytes => json!({"k":"bytes"}),
        Format::Option(item) => json!({"k":"option","item":format(item)?}),
        Format::Seq(item) => json!({"k":"list","item":format(item)?}),
        Format::Map { .. } => {
            return Err("native map contract needs an explicit ordering owner".into());
        }
        Format::Tuple(items) => tuple(items)?,
        Format::TupleArray { content, size } => {
            json!({"k":"array","max":size,"item":format(content)?})
        }
    })
}

fn tuple(items: &[Format]) -> Result<Value> {
    Ok(json!({"k":"tuple","items":items.iter().map(format).collect::<Result<Vec<_>>>()?}))
}

fn record(fields: &[Named<Format>]) -> Result<Value> {
    Ok(
        json!({"k":"record","fields":fields.iter().map(|field| Ok(json!({"name":field.name,"value":format(&field.value)?}))).collect::<Result<Vec<Value>>>()?}),
    )
}

fn variant(value: &VariantFormat) -> Result<Value> {
    match value {
        VariantFormat::Variable(_) => Err("incomplete native variant".into()),
        VariantFormat::Unit => Ok(json!({"k":"unit"})),
        VariantFormat::NewType(item) => format(item),
        VariantFormat::Tuple(items) => tuple(items),
        VariantFormat::Struct(fields) => record(fields),
    }
}

fn container(value: &ContainerFormat) -> Result<Value> {
    match value {
        ContainerFormat::UnitStruct => Ok(json!({"k":"unit"})),
        ContainerFormat::NewTypeStruct(item) => Ok(json!({"k":"newtype","item":format(item)?})),
        ContainerFormat::TupleStruct(items) => tuple(items),
        ContainerFormat::Struct(fields) => record(fields),
        ContainerFormat::Enum(variants) => Ok(
            json!({"k":"enum","variants":variants.iter().map(|(id, v)| Ok(json!({"id":id,"name":v.name,"value":variant(&v.value)?}))).collect::<Result<Vec<Value>>>()?}),
        ),
    }
}

pub(super) fn registry(value: &Registry) -> Result<BTreeMap<String, Value>> {
    let mut types: BTreeMap<String, Value> = value
        .iter()
        .map(|(name, value)| Ok((name.clone(), container(value)?)))
        .collect::<Result<_>>()?;
    // Reflection describes Serde shape, not custom scalar validation. Preserve
    // these concrete native atoms explicitly without inferring arbitrary bounds.
    if let Some(variants) = types
        .get_mut("PublicValue")
        .and_then(|v| v["variants"].as_array_mut())
    {
        for variant in variants {
            match variant["name"].as_str() {
                Some("Decimal") => variant["value"]["check"] = json!("decimal"),
                Some("Date") => {
                    variant["value"]["min"] = json!(icydb::types::Date::MIN.as_days_since_epoch());
                    variant["value"]["max"] = json!(icydb::types::Date::MAX.as_days_since_epoch());
                }
                Some("Principal") => variant["value"]["max"] = json!(29),
                Some("Subaccount" | "U256") => {
                    variant["value"]["min"] = json!(32);
                    variant["value"]["max"] = json!(32);
                }
                Some("Ulid") => {
                    variant["value"]["min"] = json!(16);
                    variant["value"]["max"] = json!(16);
                }
                _ => {}
            }
        }
    }
    if let Some(fields) = types
        .get_mut("Account")
        .and_then(|v| v["fields"].as_array_mut())
    {
        for field in fields {
            match field["name"].as_str() {
                Some("owner") => field["value"]["max"] = json!(29),
                Some("subaccount") => {
                    field["value"]["item"]["min"] = json!(32);
                    field["value"]["item"]["max"] = json!(32);
                }
                _ => {}
            }
        }
    }
    Ok(types)
}

pub(super) fn write(out: &Path, types: &Registry, methods: &[Value]) -> Result<()> {
    let types = registry(types)?;
    let source = generate_registry(&types, methods, "./native-runtime.js")
        .map_err(|error| format!("Wire TypeScript generation failed: {error:?}"))?;
    fs::write(out.join("icydb.ts"), source)?;
    fs::write(
        out.join("native-runtime.ts"),
        include_str!("../../../wire-browser/native-runtime.ts"),
    )?;
    fs::write(out.join("wire-runtime.ts"), RUNTIME)?;
    fs::write(
        out.join("contracts.json"),
        serde_json::to_vec_pretty(&json!({"types":types,"methods":methods}))?,
    )?;
    Ok(())
}
