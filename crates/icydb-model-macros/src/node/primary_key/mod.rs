//! Module: node::primary_key
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{
    node::field_list_arg::{
        field_or_fields_duplicate_message, parse_field_list_arg, parse_scalar_field_arg,
        reject_duplicate_fields,
    },
    prelude::*,
};
use darling::ast::NestedMeta;

const MAX_PRIMARY_KEY_FIELDS: usize = 4;

///
/// PrimaryKey
///

#[derive(Debug)]
pub struct PrimaryKey {
    pub(crate) fields: Vec<Ident>,
    pub(crate) source: PrimaryKeySource,
}

impl PrimaryKey {
    pub(crate) const fn fields(&self) -> &[Ident] {
        self.fields.as_slice()
    }

    pub(crate) fn scalar_field(&self) -> &Ident {
        self.fields
            .first()
            .expect("primary-key parsing must reject empty field lists")
    }
}

impl FromMeta for PrimaryKey {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut fields = None;
        let mut source = None;

        for item in items {
            let NestedMeta::Meta(syn::Meta::NameValue(name_value)) = item else {
                return Err(DarlingError::custom(
                    "pk(...) supports only field = \"...\", fields = [...], and source = \"...\"",
                ));
            };

            if name_value.path.is_ident("field") {
                let field = parse_scalar_field_arg("pk", &name_value.value)?;
                if fields.replace(vec![field]).is_some() {
                    return Err(
                        DarlingError::custom(field_or_fields_duplicate_message("pk"))
                            .with_span(&name_value.path),
                    );
                }
                continue;
            }

            if name_value.path.is_ident("fields") {
                if fields
                    .replace(parse_field_list_arg("pk", &name_value.value)?)
                    .is_some()
                {
                    return Err(
                        DarlingError::custom(field_or_fields_duplicate_message("pk"))
                            .with_span(&name_value.path),
                    );
                }
                continue;
            }

            if name_value.path.is_ident("source") {
                if source
                    .replace(parse_primary_key_source(&name_value.value)?)
                    .is_some()
                {
                    return Err(DarlingError::custom(
                        "pk(...) accepts only one source = \"...\" argument",
                    )
                    .with_span(&name_value.path));
                }
                continue;
            }

            return Err(DarlingError::custom(
                "pk(...) supports only field = \"...\", fields = [...], and source = \"...\"",
            )
            .with_span(&name_value.path));
        }

        let Some(fields) = fields else {
            return Err(DarlingError::custom(
                "pk(...) requires field = \"id\" or fields = [\"id\"]",
            ));
        };

        if fields.is_empty() {
            return Err(DarlingError::custom(
                "pk(fields = []) must contain at least one field",
            ));
        }
        if fields.len() > MAX_PRIMARY_KEY_FIELDS {
            return Err(DarlingError::custom(format!(
                "pk(fields = [...]) supports at most {MAX_PRIMARY_KEY_FIELDS} fields"
            ))
            .with_span(&fields[MAX_PRIMARY_KEY_FIELDS]));
        }

        let fields = fields
            .iter()
            .map(parse_primary_key_field)
            .collect::<Result<Vec<_>, _>>()?;
        reject_duplicate_fields(&fields, ToString::to_string, "primary key")?;
        let source = match (source, fields.len()) {
            (Some(PrimaryKeySource::Internal), len) if len > 1 => {
                return Err(DarlingError::custom(
                    "composite primary keys are external-only; remove source = \"internal\" or use source = \"external\"",
                )
                .with_span(&fields[1]));
            }
            (Some(source), _) => source,
            (None, len) if len > 1 => PrimaryKeySource::External,
            (None, _) => PrimaryKeySource::default(),
        };

        Ok(Self { fields, source })
    }
}

impl HasSchemaPart for PrimaryKey {
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        let fields = quote_slice(self.fields(), to_str_lit);
        let source = self.source.schema_part(paths);

        quote! {
            #model::node::PrimaryKey::new(#fields, #source)
        }
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
    fn schema_part(&self, paths: &CratePaths) -> TokenStream {
        let model = &paths.model;
        match self {
            Self::Internal => quote!(#model::node::PrimaryKeySource::Internal),
            Self::External => quote!(#model::node::PrimaryKeySource::External),
        }
    }
}

#[cfg(test)]
mod tests;
