use crate::{prelude::*, validate::reserved::is_reserved_word};
use darling::ast::NestedMeta;
use icydb_utils::{Case, Casing};
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
    pub fn get(&self, ident: &Ident) -> Option<&Field> {
        self.fields.iter().find(|f| f.ident == *ident)
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
            .map(|f| (f.ident.clone(), f.default_expr()))
            .collect()
    }
}

impl HasSchemaPart for FieldList {
    fn schema_part(&self) -> TokenStream {
        let fields = quote_slice(&self.fields, Field::schema_part);

        quote! {
            {
                const __FIELDS: &'static [::icydb::schema::node::Field] = #fields;

                ::icydb::schema::node::FieldList::new(__FIELDS)
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
                quote!(::icydb::schema::node::FieldGeneration::Insert(#arg))
            }
        }
    }
}

impl HasSchemaPart for FieldWriteManagement {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::CreatedAt => quote!(::icydb::schema::node::FieldWriteManagement::CreatedAt),
            Self::UpdatedAt => quote!(::icydb::schema::node::FieldWriteManagement::UpdatedAt),
        }
    }
}

#[derive(Clone, Debug, FromMeta)]
pub struct Field {
    pub(crate) ident: Ident,
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
        // Identifier validation.
        let ident_str = self.ident.to_string();

        if ident_str.len() > MAX_FIELD_NAME_LEN {
            return Err(DarlingError::custom(format!(
                "field name '{ident_str}' exceeds max length {MAX_FIELD_NAME_LEN}"
            ))
            .with_span(&self.ident));
        }

        if is_reserved_word(&ident_str) {
            return Err(
                DarlingError::custom(format!("the word '{ident_str}' is reserved"))
                    .with_span(&self.ident),
            );
        }

        if !ident_str.is_case(Case::Snake) {
            return Err(DarlingError::custom(format!(
                "field ident '{ident_str}' must be snake_case"
            ))
            .with_span(&self.ident));
        }

        // Value validation.
        self.value.validate()?;

        // Relation fields encode identity semantics and must use canonical suffixes.
        if self.value.item.is_relation() {
            let required_suffix = match self.value.cardinality() {
                Cardinality::Many => RELATION_MANY_SUFFIX,
                Cardinality::One | Cardinality::Opt => RELATION_ONE_SUFFIX,
            };
            if !ident_str.ends_with(required_suffix) {
                return Err(DarlingError::custom(format!(
                    "relation field ident '{ident_str}' must end with '{required_suffix}'"
                ))
                .with_span(&self.ident));
            }
        }

        // Insert-generation stays schema-owned and explicit instead of making
        // SQL omission inferable from general Rust defaults.
        self.validate_generated()?;

        Ok(())
    }

    /// Return true when the declared field default is identical to the
    /// generated Rust field type's implicit `Default` value.
    pub fn default_matches_implicit_default(&self) -> bool {
        let Some(default) = &self.default else {
            return true;
        };

        match self.value.cardinality() {
            Cardinality::One => self.one_default_matches_implicit_default(default),
            Cardinality::Opt => option_default_matches(default),
            Cardinality::Many => vec_default_matches(default),
        }
    }

    /// Generate the default expression for this field.
    pub fn default_expr(&self) -> TokenStream {
        match (&self.default, self.value.cardinality()) {
            (Some(default), _) => quote!(#default.into()),
            (None, Cardinality::One) => quote!(Default::default()),
            (None, Cardinality::Opt) => quote!(None),
            (None, Cardinality::Many) => quote!(Vec::default()),
        }
    }

    pub fn const_ident(&self) -> Ident {
        let constant = self.ident.to_string().to_case(Case::Constant);
        format_ident!("{constant}")
    }

    pub fn insert_generation_expr(&self) -> TokenStream {
        match self.generated.as_ref().and_then(|generated| {
            let FieldGeneration::Insert(generator) = generated;
            generated_insert_contract(generator)
        }) {
            Some(GeneratedInsertContract::Ulid) => {
                quote!(Some(::icydb::model::field::FieldInsertGeneration::Ulid))
            }
            Some(GeneratedInsertContract::Timestamp) => {
                quote!(Some(
                    ::icydb::model::field::FieldInsertGeneration::Timestamp
                ))
            }
            None => quote!(None),
        }
    }

    pub fn write_management_expr(&self) -> TokenStream {
        match self.write_management {
            Some(FieldWriteManagement::CreatedAt) => {
                quote!(Some(::icydb::model::field::FieldWriteManagement::CreatedAt))
            }
            Some(FieldWriteManagement::UpdatedAt) => {
                quote!(Some(::icydb::model::field::FieldWriteManagement::UpdatedAt))
            }
            None => quote!(None),
        }
    }

    pub fn created_at() -> Self {
        Self {
            ident: format_ident!("created_at"),
            value: Value {
                item: Item::created_at(),
                ..Default::default()
            },
            default: None,
            generated: None,
            write_management: Some(FieldWriteManagement::CreatedAt),
        }
    }

    pub fn updated_at() -> Self {
        Self {
            ident: format_ident!("updated_at"),
            value: Value {
                item: Item::updated_at(),
                ..Default::default()
            },
            default: None,
            generated: None,
            write_management: Some(FieldWriteManagement::UpdatedAt),
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
            .with_span(&self.ident));
        }

        if self.value.cardinality() != Cardinality::One {
            return Err(DarlingError::custom(
                "generated(insert = ...) currently supports only single-value fields",
            )
            .with_span(&self.ident));
        }

        if self.value.item.is.is_some() || self.value.item.relation.is_some() {
            return Err(DarlingError::custom(
                "generated(insert = ...) currently supports only primitive Ulid or Timestamp fields",
            )
            .with_span(&self.ident));
        }

        let Some(contract) = generated_insert_contract(generator) else {
            return Err(DarlingError::custom(
                "generated(insert = ...) currently supports only Ulid::generate or Timestamp::now",
            )
            .with_span(&self.ident));
        };

        match (self.value.item.primitive, contract) {
            (Some(Primitive::Ulid), GeneratedInsertContract::Ulid)
            | (Some(Primitive::Timestamp), GeneratedInsertContract::Timestamp) => {}
            (Some(_), GeneratedInsertContract::Ulid) => {
                return Err(DarlingError::custom(
                    "generated(insert = \"Ulid::generate\") requires a primitive Ulid field",
                )
                .with_span(&self.ident));
            }
            (Some(_), GeneratedInsertContract::Timestamp) => {
                return Err(DarlingError::custom(
                    "generated(insert = \"Timestamp::now\") requires a primitive Timestamp field",
                )
                .with_span(&self.ident));
            }
            (None, _) => {
                return Err(DarlingError::custom(
                    "generated(insert = ...) currently supports only primitive Ulid or Timestamp fields",
                )
                .with_span(&self.ident));
            }
        }

        if self
            .default
            .as_ref()
            .is_some_and(|default| !generated_insert_default_matches(default, contract))
        {
            return Err(DarlingError::custom(
                "generated(insert = ...) and default = ... must use the same supported generator path when both are present",
            )
            .with_span(&self.ident));
        }

        Ok(())
    }
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

fn generated_insert_default_matches(default: &Arg, contract: GeneratedInsertContract) -> bool {
    match contract {
        GeneratedInsertContract::Ulid => {
            matches!(default, Arg::FuncPath(path) if path_ends_with_segments(path, &["Ulid", "generate"]))
        }
        GeneratedInsertContract::Timestamp => {
            matches!(default, Arg::FuncPath(path) if path_ends_with_segments(path, &["Timestamp", "now"]))
        }
    }
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

// Primitive defaults can use zero-literals, empty-string/vec constructors, or
// the field type's own `default()` constructor.
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
        Primitive::Int => &["Int"],
        Primitive::Int8 => &["Int8", "i8"],
        Primitive::Int16 => &["Int16", "i16"],
        Primitive::Int32 => &["Int32", "i32"],
        Primitive::Int64 => &["Int64", "i64"],
        Primitive::Int128 => &["Int128", "i128"],
        Primitive::Nat => &["Nat"],
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
            | Primitive::Int
            | Primitive::Int8
            | Primitive::Int16
            | Primitive::Int32
            | Primitive::Int64
            | Primitive::Int128
            | Primitive::Nat
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
        let ident = quote_one(&self.ident, to_str_lit);
        let value = self.value.schema_part();
        let default = quote_option(self.default.as_ref(), Arg::schema_part);
        let generated = quote_option(self.generated.as_ref(), FieldGeneration::schema_part);
        let write_management = quote_option(
            self.write_management.as_ref(),
            FieldWriteManagement::schema_part,
        );

        quote! {
            ::icydb::schema::node::Field::new(
                #ident,
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
        let ident = &self.ident;
        let value = self.value.type_expr();

        quote! {
            #ident: #value
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::{Field, FieldGeneration, FieldWriteManagement, Value};
    use crate::node::{Arg, Item};
    use darling::{FromMeta, ast::NestedMeta};
    use icydb_schema::types::Primitive;
    use quote::format_ident;
    use quote::quote;
    use syn::parse_quote;

    fn relation_field(ident: &str, many: bool) -> Field {
        Field {
            ident: format_ident!("{ident}"),
            value: Value {
                opt: false,
                many,
                item: Item {
                    relation: Some(syn::parse_quote!(User)),
                    primitive: Some(Primitive::Ulid),
                    ..Item::default()
                },
            },
            default: None,
            generated: None,
            write_management: None,
        }
    }

    #[test]
    fn relation_one_suffix_is_required() {
        let field = relation_field("user", false);
        let err = field
            .validate()
            .expect_err("one relation field without _id suffix must fail");
        assert!(
            err.to_string().contains("must end with '_id'"),
            "unexpected validation error: {err}",
        );
    }

    #[test]
    fn relation_many_suffix_is_required() {
        let field = relation_field("users", true);
        let err = field
            .validate()
            .expect_err("many relation field without _ids suffix must fail");
        assert!(
            err.to_string().contains("must end with '_ids'"),
            "unexpected validation error: {err}",
        );
    }

    #[test]
    fn relation_suffix_validation_accepts_canonical_idents() {
        relation_field("user_id", false)
            .validate()
            .expect("one relation field with _id suffix should pass");
        relation_field("user_ids", true)
            .validate()
            .expect("many relation field with _ids suffix should pass");
    }

    #[test]
    fn default_match_detects_primitive_default_constructors() {
        let field = Field {
            ident: format_ident!("name"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Text),
                    ..Item::default()
                },
            },
            default: Some(Arg::FuncPath(parse_quote!(String::new))),
            generated: None,
            write_management: None,
        };

        assert!(
            field.default_matches_implicit_default(),
            "String::new should not force a manual Default impl",
        );
    }

    #[test]
    fn default_match_detects_custom_type_default_constructors() {
        let field = Field {
            ident: format_ident!("profile"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    is: Some(parse_quote!(crate::Profile)),
                    ..Item::default()
                },
            },
            default: Some(Arg::FuncPath(parse_quote!(crate::Profile::default))),
            generated: None,
            write_management: None,
        };

        assert!(
            field.default_matches_implicit_default(),
            "custom type default() should not force a manual Default impl",
        );
    }

    #[test]
    fn default_match_rejects_custom_non_default_constructors() {
        let field = Field {
            ident: format_ident!("id"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Ulid),
                    ..Item::default()
                },
            },
            default: Some(Arg::FuncPath(parse_quote!(Ulid::generate))),
            generated: None,
            write_management: None,
        };

        assert!(
            !field.default_matches_implicit_default(),
            "custom constructors must still force an explicit Default impl",
        );
    }

    #[test]
    fn generated_clause_accepts_single_value_primitive_ulid_fields() {
        let field = Field {
            ident: format_ident!("id"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Ulid),
                    ..Item::default()
                },
            },
            default: None,
            generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
                Ulid::generate
            )))),
            write_management: None,
        };

        field
            .validate()
            .expect("generated(insert = ...) should be admitted for primitive Ulid fields");
    }

    #[test]
    fn generated_clause_parser_accepts_arbitrary_quoted_generator_paths() {
        let generated = FieldGeneration::from_list(&[NestedMeta::Meta(syn::Meta::NameValue(
            parse_quote!(insert = "Id::generate"),
        ))])
        .expect("generated(insert = \"...\") should parse any quoted generator path");

        let FieldGeneration::Insert(Arg::FuncPath(path)) = generated else {
            panic!("generated(insert = \"...\") should lower to a function path");
        };

        assert_eq!(
            path.segments
                .iter()
                .map(|segment| segment.ident.to_string())
                .collect::<Vec<_>>(),
            vec!["Id".to_string(), "generate".to_string()],
            "generated(insert = \"...\") should preserve the quoted path segments",
        );
    }

    #[test]
    fn from_list_parses_generated_insert_clause() {
        let args = NestedMeta::parse_meta_list(quote!(
            ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ))
        .expect("field args should parse");

        let field = Field::from_list(&args).expect("field meta should lower");

        assert!(
            matches!(field.generated, Some(FieldGeneration::Insert(_))),
            "generated(insert = ...) should parse into FieldGeneration::Insert",
        );
        assert_eq!(field.value.item.primitive, Some(Primitive::Ulid));
    }

    #[test]
    fn generated_clause_accepts_single_value_primitive_timestamp_fields() {
        let field = Field {
            ident: format_ident!("created_on_insert"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Timestamp),
                    ..Item::default()
                },
            },
            default: Some(Arg::FuncPath(parse_quote!(Timestamp::now))),
            generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
                Timestamp::now
            )))),
            write_management: None,
        };

        field
            .validate()
            .expect("generated(insert = ...) should be admitted for primitive Timestamp fields");
    }

    #[test]
    fn generated_clause_rejects_mismatched_field_and_generator_contracts() {
        let field = Field {
            ident: format_ident!("name"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Text),
                    ..Item::default()
                },
            },
            default: None,
            generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
                Ulid::generate
            )))),
            write_management: None,
        };

        let err = field
            .validate()
            .expect_err("generated(insert = ...) should stay fail-closed on mismatched fields");
        assert!(
            err.to_string()
                .contains("generated(insert = \"Ulid::generate\") requires a primitive Ulid field"),
            "unexpected generated(insert = ...) validation error: {err}",
        );
    }

    #[test]
    fn generated_clause_rejects_non_ulid_generators() {
        let field = Field {
            ident: format_ident!("id"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Ulid),
                    ..Item::default()
                },
            },
            default: None,
            generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
                Id::generate
            )))),
            write_management: None,
        };

        let err = field
            .validate()
            .expect_err("generated(insert = ...) should stay fail-closed on non-Ulid generators");
        assert!(
            err.to_string().contains(
                "generated(insert = ...) currently supports only Ulid::generate or Timestamp::now"
            ),
            "unexpected generated(insert = ...) validation error: {err}",
        );
    }

    #[test]
    fn generated_clause_rejects_mismatched_default_contracts() {
        let field = Field {
            ident: format_ident!("created_on_insert"),
            value: Value {
                opt: false,
                many: false,
                item: Item {
                    primitive: Some(Primitive::Timestamp),
                    ..Item::default()
                },
            },
            default: Some(Arg::ConstPath(parse_quote!(Timestamp::EPOCH))),
            generated: Some(FieldGeneration::Insert(Arg::FuncPath(parse_quote!(
                Timestamp::now
            )))),
            write_management: None,
        };

        let err = field
            .validate()
            .expect_err("generated(insert = ...) should reject conflicting default contracts");
        assert!(
            err.to_string().contains(
                "generated(insert = ...) and default = ... must use the same supported generator path when both are present"
            ),
            "unexpected generated/default conflict validation error: {err}",
        );
    }

    #[test]
    fn created_and_updated_fields_emit_write_management_metadata() {
        assert_eq!(
            Field::created_at().write_management,
            Some(FieldWriteManagement::CreatedAt),
            "created_at helper should mark the field as insert-managed",
        );
        assert_eq!(
            Field::updated_at().write_management,
            Some(FieldWriteManagement::UpdatedAt),
            "updated_at helper should mark the field as update-managed",
        );
    }
}
