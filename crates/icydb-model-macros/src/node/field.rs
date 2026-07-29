//! Module: node::field
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

#[cfg(test)]
mod tests;

use crate::case::{Case, Casing};
use crate::{prelude::*, validate::reserved::is_reserved_word};
use darling::ast::NestedMeta;
use std::slice::Iter;

///
/// FieldList
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct FieldList {
    #[darling(multiple, rename = "field")]
    pub(crate) fields: Vec<Field>,
}

impl FieldList {
    pub fn get(&self, name: &Ident) -> Option<&Field> {
        self.fields.iter().find(|field| field.name == *name)
    }

    pub const fn is_empty(&self) -> bool {
        self.fields.is_empty()
    }

    pub const fn len(&self) -> usize {
        self.fields.len()
    }

    pub fn iter(&self) -> std::slice::Iter<'_, Field> {
        self.fields.iter()
    }

    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Field> {
        self.fields.iter_mut()
    }

    pub fn has_default(&self) -> bool {
        self.fields.iter().any(|f| f.default.is_some())
    }

    pub fn push(&mut self, field: Field) {
        self.fields.push(field);
    }

    pub fn validate(&self) -> Result<(), DarlingError> {
        for field in &self.fields {
            field.validate()?;
        }
        Ok(())
    }

    /// Generate default assignments for struct initialization.
    pub fn default_assignments(&self) -> Vec<(Ident, TokenStream)> {
        self.iter()
            .filter_map(|field| {
                field
                    .rust_default_expr()
                    .map(|expr| (field.name.clone(), expr))
            })
            .collect()
    }
}

impl HasSchemaPart for FieldList {
    fn schema_part(&self) -> TokenStream {
        let fields = quote_slice(&self.fields, Field::schema_part);

        quote! {
            {
                const __FIELDS: &'static [::icydb_model::node::Field] = #fields;

                ::icydb_model::node::FieldList::new(__FIELDS)
            }
        }
    }
}

impl HasTypeExpr for FieldList {
    fn type_expr(&self) -> TokenStream {
        let fields = self.fields.iter().map(HasTypeExpr::type_expr);

        quote!(#(#fields),*)
    }
}

impl<'a> IntoIterator for &'a FieldList {
    type Item = &'a Field;
    type IntoIter = Iter<'a, Field>;

    fn into_iter(self) -> Self::IntoIter {
        self.fields.iter()
    }
}

///
/// Field
///

#[derive(Clone, Debug)]
pub(crate) enum FieldGeneration {
    Insert(Arg),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum FieldWriteManagement {
    CreatedAt,
    UpdatedAt,
}

impl FromMeta for FieldGeneration {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut insert = None;

        for item in items {
            let NestedMeta::Meta(syn::Meta::NameValue(name_value)) = item else {
                return Err(DarlingError::custom(
                    "generated(...) currently requires insert = \"...\"",
                ));
            };

            if !name_value.path.is_ident("insert") {
                return Err(DarlingError::custom(
                    "generated(...) currently supports only insert = \"...\"",
                ));
            }

            let syn::Expr::Lit(expr_lit) = &name_value.value else {
                return Err(DarlingError::custom(
                    "generated(insert = ...) currently requires a quoted generator path",
                ));
            };

            let syn::Lit::Str(generator) = &expr_lit.lit else {
                return Err(DarlingError::custom(
                    "generated(insert = ...) currently requires a quoted generator path",
                ));
            };
            let arg = Arg::FuncPath(syn::parse_str(&generator.value()).map_err(|_| {
                DarlingError::custom(
                    "generated(insert = ...) currently requires a quoted generator path",
                )
            })?);
            if insert.replace(arg).is_some() {
                return Err(DarlingError::custom(
                    "generated(...) currently accepts only one insert = \"...\" argument",
                ));
            }
        }

        let Some(insert) = insert else {
            return Err(DarlingError::custom(
                "generated(...) currently requires insert = \"...\"",
            ));
        };

        Ok(Self::Insert(insert))
    }
}

impl HasSchemaPart for FieldGeneration {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Insert(arg) => {
                let arg = quote_one(arg, Arg::schema_part);
                quote!(::icydb_model::node::FieldGeneration::Insert(#arg))
            }
        }
    }
}

impl HasSchemaPart for FieldWriteManagement {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::CreatedAt => quote!(::icydb_model::node::FieldWriteManagement::CreatedAt),
            Self::UpdatedAt => quote!(::icydb_model::node::FieldWriteManagement::UpdatedAt),
        }
    }
}

#[derive(Clone, Debug, FromMeta)]
pub struct Field {
    pub(crate) name: Ident,
    pub(crate) value: Value,

    #[darling(default)]
    pub(crate) default: Option<Arg>,

    #[darling(default)]
    pub(crate) generated: Option<FieldGeneration>,

    #[darling(default, skip)]
    pub(crate) write_management: Option<FieldWriteManagement>,
}

// Canonical relation identity suffixes.
const RELATION_ONE_SUFFIX: &str = "_id";
const RELATION_MANY_SUFFIX: &str = "_ids";

impl Field {
    pub fn validate(&self) -> Result<(), DarlingError> {
        // Name validation.
        let name = self.name.to_string();

        if name.len() > MAX_FIELD_NAME_LEN {
            return Err(DarlingError::custom(format!(
                "field name '{name}' exceeds max length {MAX_FIELD_NAME_LEN}"
            ))
            .with_span(&self.name));
        }

        if is_reserved_word(&name) {
            return Err(
                DarlingError::custom(format!("the word '{name}' is reserved"))
                    .with_span(&self.name),
            );
        }

        if !name.is_case(Case::Snake) {
            return Err(
                DarlingError::custom(format!("field name '{name}' must be snake_case"))
                    .with_span(&self.name),
            );
        }

        // Value validation.
        self.value.validate()?;

        // Relation fields encode identity semantics and must use canonical suffixes.
        if self.value.item.is_relation() {
            let required_suffix = match self.value.cardinality() {
                Cardinality::Many => RELATION_MANY_SUFFIX,
                Cardinality::One | Cardinality::Opt => RELATION_ONE_SUFFIX,
            };
            if !name.ends_with(required_suffix) {
                return Err(DarlingError::custom(format!(
                    "relation field name '{name}' must end with '{required_suffix}'"
                ))
                .with_span(&self.name));
            }
        }

        // Insert-generation stays schema-owned and explicit instead of making
        // SQL omission inferable from general Rust defaults.
        self.validate_generated()?;
        self.validate_database_default()?;

        Ok(())
    }

    /// Return true when the field's Rust construction value is identical to the
    /// generated Rust field type's implicit `Default` value.
    pub fn default_matches_implicit_default(&self) -> bool {
        let Some(default) = &self.default else {
            return match self.value.cardinality() {
                Cardinality::One => self.write_management.is_some(),
                Cardinality::Opt | Cardinality::Many => self.generated.is_none(),
            };
        };

        match self.value.cardinality() {
            Cardinality::One => self.one_default_matches_implicit_default(default),
            Cardinality::Opt => option_default_matches(default),
            Cardinality::Many => vec_default_matches(default),
        }
    }

    /// Generate the Rust `Default` construction expression for this field.
    ///
    /// This is downstream of the schema contract. It never decides whether the
    /// field has a database default.
    pub fn rust_default_expr(&self) -> Option<TokenStream> {
        if let Some(FieldGeneration::Insert(generator)) = &self.generated {
            return Some(quote!(#generator.into()));
        }

        match (&self.default, self.value.cardinality()) {
            (Some(default), _) => Some(schema_default_rust_expr(default, &self.value)),
            (None, Cardinality::One) if self.write_management.is_some() => {
                Some(quote!(Default::default()))
            }
            (None, Cardinality::One) => None,
            (None, Cardinality::Opt) => Some(quote!(None)),
            (None, Cardinality::Many) => Some(quote!(Vec::default())),
        }
    }

    pub fn has_rust_default(&self) -> bool {
        self.rust_default_expr().is_some()
    }

    pub fn const_ident(&self) -> Ident {
        let constant = self.name.to_string().to_case(Case::Constant);
        format_ident!("{constant}")
    }

    pub(crate) fn managed_timestamp(name: Ident, write_management: FieldWriteManagement) -> Self {
        Self {
            name,
            value: Value {
                item: Item {
                    primitive: Some(Primitive::Timestamp),
                    ..Default::default()
                },
                ..Default::default()
            },
            default: None,
            generated: None,
            write_management: Some(write_management),
        }
    }

    // One-cardinality fields can only use the implicit derive path when their
    // explicit default lowers to the same value as the generated field type.
    fn one_default_matches_implicit_default(&self, default: &Arg) -> bool {
        if let Some(path) = self.value.item.is.as_ref() {
            return custom_type_default_matches(path, default);
        }

        let Some(primitive) = self.value.item.primitive else {
            return false;
        };

        primitive_default_matches(primitive, default)
    }

    // `generated(insert = "...")` stays schema-owned and explicit. Only one
    // small allowlist of write-time generators is admitted in this release.
    fn validate_generated(&self) -> Result<(), DarlingError> {
        let Some(FieldGeneration::Insert(generator)) = self.generated.as_ref() else {
            return Ok(());
        };

        if self.write_management.is_some() {
            return Err(DarlingError::custom(
                "generated(insert = ...) cannot be combined with auto-managed write fields",
            )
            .with_span(&self.name));
        }

        if self.value.cardinality() != Cardinality::One {
            return Err(DarlingError::custom(
                "generated(insert = ...) currently supports only single-value fields",
            )
            .with_span(&self.name));
        }

        if self.value.item.is.is_some() || self.value.item.relation.is_some() {
            return Err(DarlingError::custom(
                "generated(insert = ...) currently supports only primitive Ulid or Timestamp fields",
            )
            .with_span(&self.name));
        }

        let Some(contract) = generated_insert_contract(generator) else {
            return Err(DarlingError::custom(
                "generated(insert = ...) currently supports only Ulid::generate or Timestamp::now",
            )
            .with_span(&self.name));
        };

        match (self.value.item.primitive, contract) {
            (Some(Primitive::Ulid), GeneratedInsertContract::Ulid)
            | (Some(Primitive::Timestamp), GeneratedInsertContract::Timestamp) => {}
            (Some(_), GeneratedInsertContract::Ulid) => {
                return Err(DarlingError::custom(
                    "generated(insert = \"Ulid::generate\") requires a primitive Ulid field",
                )
                .with_span(&self.name));
            }
            (Some(_), GeneratedInsertContract::Timestamp) => {
                return Err(DarlingError::custom(
                    "generated(insert = \"Timestamp::now\") requires a primitive Timestamp field",
                )
                .with_span(&self.name));
            }
            (None, _) => {
                return Err(DarlingError::custom(
                    "generated(insert = ...) currently supports only primitive Ulid or Timestamp fields",
                )
                .with_span(&self.name));
            }
        }

        if self.default.is_some() {
            return Err(DarlingError::custom(
                "generated(insert = ...) cannot be combined with default = ...; default is a database/schema default",
            )
            .with_span(&self.name));
        }

        Ok(())
    }

    fn validate_database_default(&self) -> Result<(), DarlingError> {
        let Some(default) = self.default.as_ref() else {
            return Ok(());
        };

        if authored_unit_enum_default(default, &self.value)
            .map_err(|message| DarlingError::custom(message).with_span(&self.name))?
            .is_some()
        {
            return Ok(());
        }

        validate_database_default_shape(default, &self.value)
            .map_err(|message| DarlingError::custom(message).with_span(&self.name))
    }
}

fn authored_unit_enum_default<'a>(
    default: &'a Arg,
    value: &'a Value,
) -> Result<Option<(&'a Path, &'a Ident)>, String> {
    let Some(enum_path) = value.item.is.as_ref() else {
        return Ok(None);
    };
    if value.many {
        return Err("default currently supports only single-value fields".to_string());
    }
    let Arg::ConstPath(default_path) = default else {
        return Err(
            "custom-type defaults must name a unit enum variant such as Status::Active".to_string(),
        );
    };
    let mut segments = default_path.segments.iter().rev();
    let variant = &segments
        .next()
        .ok_or_else(|| "enum default path is empty".to_string())?
        .ident;
    let default_type = &segments
        .next()
        .ok_or_else(|| "enum defaults must include the enum type and unit variant".to_string())?
        .ident;
    let declared_type = &enum_path
        .segments
        .last()
        .ok_or_else(|| "enum field type path is empty".to_string())?
        .ident;
    if default_type != declared_type {
        return Err(format!(
            "enum default type {default_type} does not match field type {declared_type}"
        ));
    }

    Ok(Some((enum_path, variant)))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum GeneratedInsertContract {
    Ulid,
    Timestamp,
}

fn generated_insert_contract(generator: &Arg) -> Option<GeneratedInsertContract> {
    match generator {
        Arg::FuncPath(path) if path_ends_with_segments(path, &["Ulid", "generate"]) => {
            Some(GeneratedInsertContract::Ulid)
        }
        Arg::FuncPath(path) if path_ends_with_segments(path, &["Timestamp", "now"]) => {
            Some(GeneratedInsertContract::Timestamp)
        }
        Arg::Bool(_)
        | Arg::Char(_)
        | Arg::ConstPath(_)
        | Arg::Number(_)
        | Arg::String(_)
        | Arg::FuncPath(_) => None,
    }
}

fn schema_default_rust_expr(default: &Arg, value: &Value) -> TokenStream {
    match (value.item.primitive, default) {
        (Some(Primitive::Account), Arg::String(value)) => {
            quote!(<::icydb_model::schema::Account as ::core::str::FromStr>::from_str(#value)
                .expect("validated Account schema default should parse")
                .into())
        }
        (Some(Primitive::Principal), Arg::String(value)) => {
            quote!(::icydb_model::schema::Principal::from_text(#value)
                .expect("validated Principal schema default should parse")
                .into())
        }
        (Some(Primitive::Subaccount), Arg::String(value)) => {
            let bytes = parse_subaccount_hex(value.value().as_str())
                .expect("validated Subaccount schema default should parse");
            let byte_tokens = bytes.iter().map(|byte| quote!(#byte));
            quote!(::icydb_model::schema::Subaccount::from_array([#(#byte_tokens),*]).into())
        }
        (Some(Primitive::Ulid), Arg::String(value)) => {
            quote!(<::icydb_model::schema::Ulid as ::core::str::FromStr>::from_str(#value)
                .expect("validated Ulid schema default should parse")
                .into())
        }
        _ => quote!(#default.into()),
    }
}

fn validate_database_default_shape(default: &Arg, value: &Value) -> Result<(), String> {
    if value.many {
        return Err("default currently supports only single-value fields".to_string());
    }
    let Some(primitive) = value.item.primitive else {
        return Err(
            "default currently supports only primitive fields or unit enum variants".to_string(),
        );
    };
    if let Some(message) = identity_like_default_constructor_error(default, primitive) {
        return Err(message);
    }

    let compatible = match (primitive, default) {
        (Primitive::Bool, Arg::Bool(_))
        | (
            Primitive::Account
            | Primitive::Blob
            | Primitive::Principal
            | Primitive::Subaccount
            | Primitive::Text
            | Primitive::Ulid,
            Arg::String(_),
        )
        | (
            Primitive::Float32
            | Primitive::Float64
            | Primitive::Int8
            | Primitive::Int16
            | Primitive::Int32
            | Primitive::Int64
            | Primitive::Int128
            | Primitive::Nat8
            | Primitive::Nat16
            | Primitive::Nat32
            | Primitive::Nat64
            | Primitive::Nat128,
            Arg::Number(_),
        )
        | (
            Primitive::Date
            | Primitive::Decimal
            | Primitive::Duration
            | Primitive::IntBig
            | Primitive::NatBig
            | Primitive::Timestamp,
            Arg::Number(_) | Arg::String(_),
        ) => true,
        (Primitive::Unit, Arg::ConstPath(path)) => path_ends_with_segments(path, &["Unit"]),
        (_, Arg::FuncPath(path)) => primitive_default_fn_matches(primitive, path),
        _ => false,
    };

    compatible.then_some(()).ok_or_else(|| {
        format!("default value {default:?} is not compatible with primitive {primitive:?}")
    })
}

fn identity_like_default_constructor_error(default: &Arg, primitive: Primitive) -> Option<String> {
    let Arg::FuncPath(path) = default else {
        return None;
    };
    let type_name = match primitive {
        Primitive::Account => "Account",
        Primitive::Principal => "Principal",
        Primitive::Subaccount => "Subaccount",
        Primitive::Ulid => "Ulid",
        _ => return None,
    };

    path_ends_with_segments(path, &[type_name, "default"]).then(|| {
        format!(
            "identity-like {type_name}::default() constructors are not valid schema/database \
             defaults; use an explicit persisted literal if this default is intentional"
        )
    })
}

fn parse_subaccount_hex(value: &str) -> Result<[u8; 32], String> {
    let value = value.strip_prefix("0x").unwrap_or(value);
    if value.len() != 64 {
        return Err(format!(
            "default for primitive Subaccount requires 64 hex characters, got {}",
            value.len()
        ));
    }

    let mut bytes = [0u8; 32];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let start = index * 2;
        *byte = u8::from_str_radix(&value[start..start + 2], 16).map_err(|_| {
            format!("default for primitive Subaccount has invalid hex at byte {index}")
        })?;
    }

    Ok(bytes)
}

// Explicit `None` or `Option::default()` matches the implicit optional default.
fn option_default_matches(default: &Arg) -> bool {
    matches!(default, Arg::ConstPath(path) if path_ends_with_segments(path, &["None"]))
        || matches!(default, Arg::FuncPath(path) if path_ends_with_segments(path, &["Option", "default"]))
}

// Explicit empty vectors still match the derived default for repeated fields.
fn vec_default_matches(default: &Arg) -> bool {
    matches!(default, Arg::FuncPath(path)
        if path_ends_with_segments(path, &["Vec", "new"])
            || path_ends_with_segments(path, &["Vec", "default"]))
}

// Custom `is = "Type"` fields only match when the default is `Type::default()`.
fn custom_type_default_matches(field_type: &Path, default: &Arg) -> bool {
    matches!(default, Arg::FuncPath(path) if path_matches_type_default(path, field_type))
}

// Primitive defaults can use deterministic zero-literals, empty-string/vec
// constructors, or the field type's own `default()` constructor when that
// constructor is a meaningful database value. Identity-like constructors are
// rejected separately during schema-default validation.
fn primitive_default_matches(primitive: Primitive, default: &Arg) -> bool {
    match default {
        Arg::Bool(value) => primitive == Primitive::Bool && !value,
        Arg::Number(value) => {
            primitive_supports_zero_literal(primitive) && arg_number_is_zero(value)
        }
        Arg::String(value) => primitive == Primitive::Text && value.value().is_empty(),
        Arg::FuncPath(path) => primitive_default_fn_matches(primitive, path),
        Arg::Char(_) | Arg::ConstPath(_) => false,
    }
}

fn primitive_default_fn_matches(primitive: Primitive, path: &Path) -> bool {
    if matches!(primitive, Primitive::Text)
        && (path_ends_with_segments(path, &["String", "new"])
            || path_ends_with_segments(path, &["String", "default"]))
    {
        return true;
    }

    if matches!(primitive, Primitive::Blob)
        && (path_ends_with_segments(path, &["Vec", "new"])
            || path_ends_with_segments(path, &["Vec", "default"]))
    {
        return true;
    }

    primitive_default_type_names(primitive)
        .iter()
        .any(|type_name| path_ends_with_segments(path, &[type_name, "default"]))
}

const fn primitive_default_type_names(primitive: Primitive) -> &'static [&'static str] {
    match primitive {
        Primitive::Account => &["Account"],
        Primitive::Blob => &["Blob"],
        Primitive::Bool => &["Bool", "bool"],
        Primitive::Date => &["Date", "i32"],
        Primitive::Decimal => &["Decimal", "f64"],
        Primitive::Duration => &["Duration", "u64"],
        Primitive::Float32 => &["Float32", "f32"],
        Primitive::Float64 => &["Float64", "f64"],
        Primitive::IntBig => &["IntBig"],
        Primitive::Int8 => &["Int8", "i8"],
        Primitive::Int16 => &["Int16", "i16"],
        Primitive::Int32 => &["Int32", "i32"],
        Primitive::Int64 => &["Int64", "i64"],
        Primitive::Int128 => &["Int128", "i128"],
        Primitive::NatBig => &["NatBig"],
        Primitive::Nat8 => &["Nat8", "u8"],
        Primitive::Nat16 => &["Nat16", "u16"],
        Primitive::Nat32 => &["Nat32", "u32"],
        Primitive::Nat64 => &["Nat64", "u64"],
        Primitive::Nat128 => &["Nat128", "u128"],
        Primitive::Principal => &["Principal"],
        Primitive::Subaccount => &["Subaccount"],
        Primitive::Text => &["Text", "String"],
        Primitive::Timestamp => &["Timestamp", "u64"],
        Primitive::Ulid => &["Ulid"],
        Primitive::Unit => &["Unit"],
    }
}

const fn primitive_supports_zero_literal(primitive: Primitive) -> bool {
    matches!(
        primitive,
        Primitive::Date
            | Primitive::Decimal
            | Primitive::Duration
            | Primitive::Float32
            | Primitive::Float64
            | Primitive::IntBig
            | Primitive::Int8
            | Primitive::Int16
            | Primitive::Int32
            | Primitive::Int64
            | Primitive::Int128
            | Primitive::NatBig
            | Primitive::Nat8
            | Primitive::Nat16
            | Primitive::Nat32
            | Primitive::Nat64
            | Primitive::Nat128
            | Primitive::Timestamp
    )
}

const fn arg_number_is_zero(number: &ArgNumber) -> bool {
    match number {
        ArgNumber::Float32(value) => value.to_bits() == 0.0f32.to_bits(),
        ArgNumber::Float64(value) => value.to_bits() == 0.0f64.to_bits(),
        ArgNumber::Int8(value) => *value == 0,
        ArgNumber::Int16(value) => *value == 0,
        ArgNumber::Int32(value) => *value == 0,
        ArgNumber::Int64(value) => *value == 0,
        ArgNumber::Int128(value) => *value == 0,
        ArgNumber::Nat8(value) => *value == 0,
        ArgNumber::Nat16(value) => *value == 0,
        ArgNumber::Nat32(value) => *value == 0,
        ArgNumber::Nat64(value) => *value == 0,
        ArgNumber::Nat128(value) => *value == 0,
    }
}

fn path_matches_type_default(default_path: &Path, field_type: &Path) -> bool {
    let default_segments: Vec<_> = default_path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();
    let type_segments: Vec<_> = field_type
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();

    default_segments.len() == type_segments.len() + 1
        && default_segments
            .last()
            .is_some_and(|segment| segment == "default")
        && default_segments[..type_segments.len()] == type_segments[..]
}

fn path_ends_with_segments(path: &Path, expected: &[&str]) -> bool {
    let segments: Vec<_> = path
        .segments
        .iter()
        .map(|segment| segment.ident.to_string())
        .collect();

    segments.len() >= expected.len()
        && segments[segments.len() - expected.len()..]
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
}

impl HasSchemaPart for Field {
    fn schema_part(&self) -> TokenStream {
        let name = quote_one(&self.name, to_str_lit);
        let value = self.value.schema_part();
        let default = quote_option(self.default.as_ref(), Arg::schema_part);
        let generated = quote_option(self.generated.as_ref(), FieldGeneration::schema_part);
        let write_management = quote_option(
            self.write_management.as_ref(),
            FieldWriteManagement::schema_part,
        );

        quote! {
            ::icydb_model::node::Field::new(
                #name,
                #value,
                #default,
                #generated,
                #write_management,
            )
        }
    }
}

impl HasTypeExpr for Field {
    fn type_expr(&self) -> TokenStream {
        let name = &self.name;
        let value = self.value.type_expr();

        quote! {
            #name: #value
        }
    }
}
