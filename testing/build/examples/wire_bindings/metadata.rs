//! Native Serde metadata and TypeScript declarations, separate from runtime admission.

use std::{collections::BTreeMap, error::Error, fmt::Write as _, fs, path::Path};

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

fn ty(value: &Value) -> Result<String> {
    Ok(match value["k"].as_str().ok_or("missing metadata kind")? {
        "ref" => value["name"].as_str().ok_or("missing native name")?.into(),
        "unit" => "null".into(),
        "bool" => "boolean".into(),
        "text" => "string".into(),
        "bytes" => "Uint8Array".into(),
        "u" | "i" => if value["bits"].as_u64().ok_or("missing integer width")? > 32 {
            "bigint"
        } else {
            "number"
        }
        .into(),
        "float" => "number".into(),
        "newtype" => ty(&value["item"])?,
        "list" | "array" => format!("ReadonlyArray<{}>", ty(&value["item"])?),
        "tuple" => format!(
            "readonly [{}]",
            value["items"]
                .as_array()
                .ok_or("missing tuple items")?
                .iter()
                .map(ty)
                .collect::<Result<Vec<_>>>()?
                .join(", ")
        ),
        "option" => format!("null | {{ readonly some: {} }}", ty(&value["item"])?),
        "result" => format!(
            "{{ readonly ok: {} }} | {{ readonly err: {} }}",
            ty(&value["ok"])?,
            ty(&value["err"])?
        ),
        "record" => format!(
            "{{ {} }}",
            value["fields"]
                .as_array()
                .ok_or("missing record fields")?
                .iter()
                .map(|f| Ok(format!("readonly {}: {}", f["name"], ty(&f["value"])?)))
                .collect::<Result<Vec<String>>>()?
                .join("; ")
        ),
        "enum" => value["variants"]
            .as_array()
            .ok_or("missing variants")?
            .iter()
            .map(|v| {
                Ok(format!(
                    "{{ readonly tag: {}; readonly value: {} }}",
                    v["name"],
                    ty(&v["value"])?
                ))
            })
            .collect::<Result<Vec<String>>>()?
            .join(" | "),
        _ => return Err("unsupported native metadata".into()),
    })
}

pub(super) fn write(out: &Path, types: &Registry, methods: &[Value]) -> Result<()> {
    let types = registry(types)?;
    let mut source = String::from(
        "// Generated from native IcyDB Serde contracts.\nimport { createCodec } from './native-runtime.js';\n",
    );
    for (name, node) in &types {
        // Serde names become executable type identifiers only after validation.
        if name.is_empty()
            || !name
                .bytes()
                .enumerate()
                .all(|(i, b)| b.is_ascii_alphabetic() || b == b'_' || (i > 0 && b.is_ascii_digit()))
        {
            return Err(format!("invalid TypeScript native type name: {name}").into());
        }
        writeln!(source, "export type {name} = {};", ty(node)?)?;
    }
    writeln!(source, "const types = {types};", types = json!(types))?;
    for method in methods {
        let name = method["name"].as_str().ok_or("missing endpoint name")?;
        writeln!(
            source,
            "export const {name} = (() => {{ const codec = createCodec({method}, types); return {{ methodName: {name_json}, mode: {mode} as const, encodeRequest: (value: {request}): Uint8Array => codec.encodeRequest(value), decodeRequest: (bytes: Uint8Array): {request} => codec.decodeRequest(bytes) as {request}, encodeResponse: (value: {response}): Uint8Array => codec.encodeResponse(value), decodeResponse: (bytes: Uint8Array): {response} => codec.decodeResponse(bytes) as {response} }}; }})();",
            name_json = json!(name),
            mode = method["mode"],
            request = ty(&method["request"])?,
            response = ty(&method["response"])?,
        )?;
    }
    fs::write(out.join("icydb.ts"), source)?;
    fs::write(
        out.join("native-runtime.ts"),
        include_str!("../../../wire-browser/native-runtime.ts"),
    )?;
    fs::write(out.join("wire-runtime.ts"), ic_wire::typescript::RUNTIME)?;
    fs::write(
        out.join("contracts.json"),
        serde_json::to_vec_pretty(&json!({"types":types,"methods":methods}))?,
    )?;
    Ok(())
}
