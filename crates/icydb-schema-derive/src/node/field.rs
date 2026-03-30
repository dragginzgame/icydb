use crate::{prelude::*, validate::reserved::is_reserved_word};
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

#[derive(Clone, Debug, FromMeta)]
pub struct Field {
    pub(crate) ident: Ident,
    pub(crate) value: Value,

    #[darling(default)]
    pub(crate) default: Option<Arg>,
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

    pub fn created_at() -> Self {
        Self {
            ident: format_ident!("created_at"),
            value: Value {
                item: Item::created_at(),
                ..Default::default()
            },
            default: None,
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

        quote! {
            ::icydb::schema::node::Field::new(#ident, #value, #default)
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
    use super::{Field, Value};
    use crate::node::{Arg, Item};
    use icydb_schema::types::Primitive;
    use quote::format_ident;
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
        };

        assert!(
            !field.default_matches_implicit_default(),
            "custom constructors must still force an explicit Default impl",
        );
    }
}
