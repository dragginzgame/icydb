//! Module: node::relation
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{
    node::field_list_arg::{
        field_or_fields_duplicate_message, parse_field_list_arg, parse_scalar_field_arg,
    },
    prelude::*,
};
use darling::ast::NestedMeta;
use std::collections::HashSet;

///
/// Relation
///
/// Derive-side relation-edge declaration. This is proposal metadata only; the
/// generated schema node performs graph-aware validation against accepted
/// source/target field metadata.
///

#[derive(Clone, Debug)]
pub struct Relation {
    pub(crate) ident: LitStr,
    pub(crate) target: Path,
    pub(crate) fields: Vec<LitStr>,
}

impl FromMeta for Relation {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut ident = None;
        let mut target = None;
        let mut fields = None;

        for item in items {
            let NestedMeta::Meta(syn::Meta::NameValue(name_value)) = item else {
                return Err(DarlingError::custom(
                    "relation(...) supports ident = \"...\", rel = \"...\", field = \"...\", and fields = [...]",
                ));
            };

            if name_value.path.is_ident("ident") {
                if ident
                    .replace(parse_relation_ident(&name_value.value)?)
                    .is_some()
                {
                    return Err(DarlingError::custom(
                        "relation(...) accepts only one ident = \"...\" argument",
                    )
                    .with_span(&name_value.path));
                }
                continue;
            }

            if name_value.path.is_ident("rel") {
                if target
                    .replace(parse_relation_target(&name_value.value)?)
                    .is_some()
                {
                    return Err(DarlingError::custom(
                        "relation(...) accepts only one rel = \"...\" argument",
                    )
                    .with_span(&name_value.path));
                }
                continue;
            }

            if name_value.path.is_ident("field") {
                let field = parse_scalar_field_arg("relation", &name_value.value)?;
                if fields.replace(vec![field]).is_some() {
                    return Err(DarlingError::custom(field_or_fields_duplicate_message(
                        "relation",
                    ))
                    .with_span(&name_value.path));
                }
                continue;
            }

            if name_value.path.is_ident("fields") {
                if fields
                    .replace(parse_field_list_arg("relation", &name_value.value)?)
                    .is_some()
                {
                    return Err(DarlingError::custom(field_or_fields_duplicate_message(
                        "relation",
                    ))
                    .with_span(&name_value.path));
                }
                continue;
            }

            return Err(DarlingError::custom(
                "relation(...) supports ident = \"...\", rel = \"...\", field = \"...\", and fields = [...]",
            )
            .with_span(&name_value.path));
        }

        let Some(ident) = ident else {
            return Err(DarlingError::custom(
                "relation(...) requires ident = \"...\"",
            ));
        };
        let Some(target) = target else {
            return Err(DarlingError::custom("relation(...) requires rel = \"...\""));
        };
        let Some(fields) = fields else {
            return Err(DarlingError::custom(
                "relation(...) requires field = \"...\" or fields = [...]",
            ));
        };

        if fields.is_empty() {
            return Err(DarlingError::custom(
                "relation(fields = []) must contain at least one field",
            ));
        }
        reject_duplicate_relation_fields(fields.as_slice())?;

        Ok(Self {
            ident,
            target,
            fields,
        })
    }
}

impl Relation {
    pub(crate) fn validate(&self, fields: &FieldList) -> Result<(), DarlingError> {
        let mut local_component_cardinality = None;
        for field in &self.fields {
            let field_ident = relation_field_ident(field)?;
            let Some(local_field) = fields.get(&field_ident) else {
                return Err(DarlingError::custom(format!(
                    "relation field '{}' not found",
                    field.value()
                ))
                .with_span(field));
            };

            let local_cardinality = local_field.value.cardinality();
            if local_cardinality == Cardinality::Many {
                return Err(DarlingError::custom(
                    "relation tuple component fields cannot have many cardinality",
                )
                .with_span(field));
            }
            match local_component_cardinality {
                Some(expected) if expected != local_cardinality => {
                    return Err(DarlingError::custom(
                        "relation tuple component fields must be all required or all optional",
                    )
                    .with_span(field));
                }
                Some(_) => {}
                None => local_component_cardinality = Some(local_cardinality),
            }
            if local_field.generated.is_some() {
                return Err(DarlingError::custom(
                    "relation tuple component fields cannot be generated",
                )
                .with_span(field));
            }
        }

        Ok(())
    }
}

impl HasSchemaPart for Relation {
    fn schema_part(&self) -> TokenStream {
        let ident = quote_one(&self.ident, to_str_lit);
        let target = quote_one(&self.target, to_path);
        let fields = quote_slice(&self.fields, to_str_lit);

        quote! {
            ::icydb::schema::node::RelationEdge::new(#ident, #target, #fields)
        }
    }
}

fn parse_relation_ident(expr: &syn::Expr) -> Result<LitStr, DarlingError> {
    let literal = parse_relation_string_arg("ident", expr)?;
    if literal.value().is_empty() {
        return Err(DarlingError::custom("relation ident cannot be empty").with_span(&literal));
    }

    Ok(literal)
}

fn parse_relation_target(expr: &syn::Expr) -> Result<Path, DarlingError> {
    let literal = parse_relation_string_arg("rel", expr)?;
    syn::parse_str::<Path>(literal.value().as_str()).map_err(|_| {
        DarlingError::custom(format!(
            "relation target '{}' is not a valid Rust path",
            literal.value()
        ))
        .with_span(&literal)
    })
}

fn parse_relation_string_arg(name: &str, expr: &syn::Expr) -> Result<LitStr, DarlingError> {
    let syn::Expr::Lit(expr_lit) = expr else {
        return Err(DarlingError::custom(format!(
            "relation({name} = ...) requires a string literal"
        ))
        .with_span(expr));
    };
    let syn::Lit::Str(literal) = &expr_lit.lit else {
        return Err(DarlingError::custom(format!(
            "relation({name} = ...) requires a string literal"
        ))
        .with_span(expr));
    };

    Ok(literal.clone())
}

fn reject_duplicate_relation_fields(fields: &[LitStr]) -> Result<(), DarlingError> {
    let mut seen = HashSet::new();
    for field in fields {
        let field_name = field.value();
        if !seen.insert(field_name.clone()) {
            return Err(DarlingError::custom(format!(
                "relation field '{field_name}' is declared more than once"
            ))
            .with_span(field));
        }
    }

    Ok(())
}

fn relation_field_ident(field: &LitStr) -> Result<Ident, DarlingError> {
    syn::parse_str::<Ident>(field.value().as_str()).map_err(|_| {
        DarlingError::custom(format!(
            "relation field '{}' is not a valid Rust field identifier",
            field.value()
        ))
        .with_span(field)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(tokens: TokenStream) -> Vec<NestedMeta> {
        NestedMeta::parse_meta_list(tokens).expect("relation args should parse")
    }

    fn field_with_cardinality(ident: &str, opt: bool, many: bool) -> Field {
        Field {
            ident: format_ident!("{ident}"),
            value: Value {
                opt,
                many,
                item: Item {
                    primitive: Some(Primitive::Ulid),
                    ..Item::default()
                },
            },
            default: None,
            generated: None,
            write_management: None,
        }
    }

    fn scalar_field(ident: &str) -> Field {
        field_with_cardinality(ident, false, false)
    }

    fn optional_field(ident: &str) -> Field {
        field_with_cardinality(ident, true, false)
    }

    #[test]
    fn from_list_accepts_scalar_field_shorthand() {
        let relation = Relation::from_list(&args(quote!(
            ident = "author",
            rel = "User",
            field = "author_id"
        )))
        .expect("scalar relation field shorthand should parse");

        assert_eq!(relation.ident.value(), "author");
        assert_eq!(relation.fields.len(), 1);
        assert_eq!(relation.fields[0].value(), "author_id");
    }

    #[test]
    fn from_list_accepts_ordered_field_list() {
        let relation = Relation::from_list(&args(quote!(
            ident = "author",
            rel = "User",
            fields = ["tenant_id", "author_id"]
        )))
        .expect("relation field list should parse");

        assert_eq!(
            relation
                .fields
                .iter()
                .map(LitStr::value)
                .collect::<Vec<_>>(),
            ["tenant_id", "author_id"],
        );
    }

    #[test]
    fn from_list_rejects_duplicate_field_and_fields() {
        let err = Relation::from_list(&args(quote!(
            ident = "author",
            rel = "User",
            field = "author_id",
            fields = ["tenant_id", "author_id"]
        )))
        .expect_err("relation should reject field and fields together");

        assert!(
            err.to_string().contains("field") && err.to_string().contains("fields"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_comma_string_fields() {
        let err = Relation::from_list(&args(quote!(
            ident = "author",
            rel = "User",
            fields = "tenant_id, author_id"
        )))
        .expect_err("relation fields must use array syntax");

        assert!(
            err.to_string().contains("fields"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn validate_rejects_missing_local_field() {
        let relation = Relation::from_list(&args(quote!(
            ident = "author",
            rel = "User",
            field = "author_id"
        )))
        .expect("relation should parse");
        let fields = FieldList {
            fields: vec![scalar_field("id")],
        };

        let err = relation
            .validate(&fields)
            .expect_err("relation should reject missing local component field");

        assert!(
            err.to_string()
                .contains("relation field 'author_id' not found"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn validate_rejects_mixed_local_field_cardinality() {
        let relation = Relation::from_list(&args(quote!(
            ident = "author",
            rel = "User",
            fields = ["tenant_id", "author_id"]
        )))
        .expect("relation should parse");
        let fields = FieldList {
            fields: vec![scalar_field("tenant_id"), optional_field("author_id")],
        };

        let err = relation
            .validate(&fields)
            .expect_err("relation should reject mixed required/optional tuple components");

        assert!(
            err.to_string().contains("all required or all optional"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn schema_part_lowers_to_relation_edge() {
        let relation = Relation::from_list(&args(quote!(
            ident = "author",
            rel = "User",
            fields = ["tenant_id", "author_id"]
        )))
        .expect("relation should parse");

        let tokens = relation.schema_part().to_string();

        assert!(
            tokens.contains("RelationEdge :: new"),
            "unexpected schema tokens: {tokens}",
        );
        assert!(
            tokens.contains("< User as :: icydb :: __macro :: Path > :: PATH"),
            "unexpected schema tokens: {tokens}",
        );
    }
}
