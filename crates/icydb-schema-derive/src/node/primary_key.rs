use crate::prelude::*;
use darling::ast::NestedMeta;

///
/// PrimaryKey
///

#[derive(Debug)]
pub struct PrimaryKey {
    pub(crate) field: Ident,

    pub(crate) source: PrimaryKeySource,
}

impl FromMeta for PrimaryKey {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut fields = None;
        let mut source = PrimaryKeySource::default();

        for item in items {
            let NestedMeta::Meta(syn::Meta::NameValue(name_value)) = item else {
                return Err(DarlingError::custom(
                    "pk(...) supports only fields = [...] and source = \"...\"",
                ));
            };

            if name_value.path.is_ident("field") {
                return Err(DarlingError::custom(
                    "pk(field = ...) was removed; use pk(fields = [\"id\"])",
                )
                .with_span(&name_value.path));
            }

            if name_value.path.is_ident("fields") {
                if fields
                    .replace(parse_primary_key_fields(&name_value.value)?)
                    .is_some()
                {
                    return Err(DarlingError::custom(
                        "pk(...) accepts only one fields = [...] argument",
                    )
                    .with_span(&name_value.path));
                }
                continue;
            }

            if name_value.path.is_ident("source") {
                source = parse_primary_key_source(&name_value.value)?;
                continue;
            }

            return Err(DarlingError::custom(
                "pk(...) supports only fields = [...] and source = \"...\"",
            )
            .with_span(&name_value.path));
        }

        let Some(fields) = fields else {
            return Err(DarlingError::custom("pk(...) requires fields = [\"id\"]"));
        };

        if fields.is_empty() {
            return Err(DarlingError::custom(
                "pk(fields = []) must contain one field in this release",
            ));
        }
        if fields.len() > 1 {
            return Err(DarlingError::custom(
                "composite primary keys are not implemented yet; use one primary-key field",
            )
            .with_span(&fields[1]));
        }

        let field = parse_primary_key_field(&fields[0])?;

        Ok(Self { field, source })
    }
}

impl HasSchemaPart for PrimaryKey {
    fn schema_part(&self) -> TokenStream {
        let field = quote_one(&self.field, to_str_lit);
        let source = self.source.schema_part();

        quote! {
            ::icydb::schema::node::PrimaryKey::new(&[#field], #source)
        }
    }
}

fn parse_primary_key_fields(expr: &syn::Expr) -> Result<Vec<LitStr>, DarlingError> {
    match expr {
        syn::Expr::Array(array) => array
            .elems
            .iter()
            .map(|element| {
                let syn::Expr::Lit(expr_lit) = element else {
                    return Err(DarlingError::custom(
                        "pk(fields = [...]) requires string literal field names",
                    )
                    .with_span(element));
                };
                let syn::Lit::Str(literal) = &expr_lit.lit else {
                    return Err(DarlingError::custom(
                        "pk(fields = [...]) requires string literal field names",
                    )
                    .with_span(element));
                };
                Ok(literal.clone())
            })
            .collect(),
        syn::Expr::Lit(expr_lit) if matches!(expr_lit.lit, syn::Lit::Str(_)) => {
            Err(DarlingError::custom(
                "pk(fields = ...) must be a Rust array literal of string literals, not a comma-string",
            )
            .with_span(expr))
        }
        _ => Err(DarlingError::custom(
            "pk(fields = ...) must be a Rust array literal of string literals",
        )
        .with_span(expr)),
    }
}

fn parse_primary_key_field(literal: &LitStr) -> Result<Ident, DarlingError> {
    let value = literal.value();
    if value.is_empty() {
        return Err(DarlingError::custom("primary key field cannot be empty").with_span(literal));
    }

    syn::parse_str::<Ident>(value.as_str()).map_err(|_| {
        DarlingError::custom(format!(
            "primary key field '{value}' is not a valid Rust field identifier"
        ))
        .with_span(literal)
    })
}

fn parse_primary_key_source(expr: &syn::Expr) -> Result<PrimaryKeySource, DarlingError> {
    let syn::Expr::Lit(expr_lit) = expr else {
        return Err(
            DarlingError::custom("pk(source = ...) requires \"internal\" or \"external\"")
                .with_span(expr),
        );
    };

    PrimaryKeySource::from_value(&expr_lit.lit)
}

///
/// PrimaryKeySource
///

#[derive(Clone, Copy, Debug, Default, Eq, FromMeta, PartialEq)]
pub enum PrimaryKeySource {
    #[default]
    #[darling(rename = "internal")]
    Internal,

    #[darling(rename = "external")]
    External,
}

impl HasSchemaPart for PrimaryKeySource {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Internal => quote!(::icydb::schema::node::PrimaryKeySource::Internal),
            Self::External => quote!(::icydb::schema::node::PrimaryKeySource::External),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{PrimaryKey, PrimaryKeySource};
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    fn parse_primary_key(tokens: proc_macro2::TokenStream) -> Result<PrimaryKey, darling::Error> {
        let args = NestedMeta::parse_meta_list(tokens).expect("test meta should parse");
        PrimaryKey::from_list(&args)
    }

    #[test]
    fn from_list_parses_scalar_fields_syntax() {
        let primary_key = parse_primary_key(quote!(fields = ["id"]))
            .expect("scalar primary-key fields syntax should parse");

        assert_eq!(primary_key.field.to_string(), "id");
        assert_eq!(primary_key.source, PrimaryKeySource::Internal);
    }

    #[test]
    fn from_list_parses_explicit_external_source() {
        let primary_key = parse_primary_key(quote!(fields = ["pid"], source = "external"))
            .expect("explicit external primary-key source should parse");

        assert_eq!(primary_key.field.to_string(), "pid");
        assert_eq!(primary_key.source, PrimaryKeySource::External);
    }

    #[test]
    fn from_list_rejects_removed_field_syntax() {
        let err = parse_primary_key(quote!(field = "id"))
            .expect_err("old primary-key field syntax should reject");

        assert!(
            err.to_string()
                .contains("pk(field = ...) was removed; use pk(fields = [\"id\"])"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_comma_string_fields() {
        let err = parse_primary_key(quote!(fields = "id, name"))
            .expect_err("primary-key fields should require array syntax");

        assert!(
            err.to_string().contains("not a comma-string"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_empty_fields() {
        let err = parse_primary_key(quote!(fields = []))
            .expect_err("empty primary-key fields should reject");

        assert!(
            err.to_string().contains("must contain one field"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_composite_until_runtime_support_lands() {
        let err = parse_primary_key(quote!(fields = ["tenant_id", "local_id"]))
            .expect_err("composite primary-key syntax should reject until runtime support lands");

        assert!(
            err.to_string()
                .contains("composite primary keys are not implemented yet"),
            "unexpected error: {err}",
        );
    }
}
