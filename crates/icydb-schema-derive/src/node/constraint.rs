//! Module: node::constraint
//! Responsibility: generated named-check parsing and build-time validation.
//! Does not own: accepted field identity, literal admission, or runtime enforcement.
//! Boundary: entity macro metadata to one structured check proposal.

use crate::{
    node::index::{
        generated_check_field_models_for_predicate, predicate_runtime_tokens,
        referenced_predicate_fields,
    },
    prelude::*,
};
use darling::ast::NestedMeta;
use icydb_core::db::{
    Predicate as CorePredicate, parse_generated_index_predicate_sql,
    validate_generated_check_predicate_fields, validate_generated_constraint_name,
};

/// Parsed `constraint(name = "...", check = "...")` declaration.
#[derive(Debug)]
pub(crate) struct Constraint {
    pub(crate) name: LitStr,
    pub(crate) check: LitStr,
}

impl Constraint {
    /// Parse and validate the generated check against declared entity fields.
    pub(crate) fn validated_predicate(
        &self,
        entity: &Entity,
    ) -> Result<CorePredicate, DarlingError> {
        validate_generated_constraint_name(self.name.value().as_str())
            .map_err(DarlingError::custom)
            .map_err(|error| error.with_span(&self.name))?;
        let predicate = parse_generated_index_predicate_sql(self.check.value().as_str())
            .map_err(DarlingError::custom)
            .map_err(|error| error.with_span(&self.check))?;
        let field_models = generated_check_field_models_for_predicate(entity, &predicate)?;
        validate_generated_check_predicate_fields(field_models.as_slice(), &predicate)
            .map_err(DarlingError::custom)
            .map_err(|error| error.with_span(&self.check))?;

        Ok(predicate)
    }

    /// Emit one static structured predicate resolver and generated model entry.
    pub(crate) fn runtime_model_tokens(
        &self,
        entity: &Entity,
        ordinal: usize,
    ) -> Result<(Vec<TokenStream>, TokenStream), DarlingError> {
        let predicate = self.validated_predicate(entity)?;
        let entity_ident = entity.def.ident().to_string().to_ascii_uppercase();
        let predicate_static_ident =
            format_ident!("__{}_CHECK_CONSTRAINT_{}", entity_ident, ordinal);
        let predicate_resolver_ident = format_ident!(
            "__{}_check_constraint_{}_resolver",
            entity_ident.to_ascii_lowercase(),
            ordinal
        );
        let predicate_tokens = predicate_runtime_tokens(&predicate)?;
        let name = &self.name;
        let source_sql = &self.check;
        let mut support_items = generated_custom_kind_assertions(entity, &predicate, ordinal);
        support_items.extend([
            quote! {
                static #predicate_static_ident:
                    ::std::sync::LazyLock<::icydb::db::Predicate> =
                    ::std::sync::LazyLock::new(|| #predicate_tokens);
            },
            quote! {
                fn #predicate_resolver_ident() -> &'static ::icydb::db::Predicate {
                    &#predicate_static_ident
                }
            },
        ]);

        Ok((
            support_items,
            quote! {
                ::icydb::model::entity::CheckConstraintModel::generated(
                    #name,
                    #source_sql,
                    #predicate_resolver_ident,
                )
            },
        ))
    }
}

fn generated_custom_kind_assertions(
    entity: &Entity,
    predicate: &CorePredicate,
    constraint_ordinal: usize,
) -> Vec<TokenStream> {
    referenced_predicate_fields(predicate)
        .into_iter()
        .filter_map(|field_name| {
            let field = entity
                .fields
                .iter()
                .find(|candidate| candidate.ident == field_name)?;
            let path = field.value.item.is.as_ref()?;
            if matches!(field.value.cardinality(), Cardinality::Many) {
                return None;
            }
            let entity_name = entity.def.ident().to_string().to_ascii_uppercase();
            let field_name = field.ident.to_string().to_ascii_uppercase();
            let assertion_ident = format_ident!(
                "__{}_CHECK_CONSTRAINT_{}_{}_KIND_ASSERT",
                entity_name,
                constraint_ordinal,
                field_name,
            );

            Some(quote! {
                const #assertion_ident: () = {
                    if !matches!(
                        <#path as ::icydb::__macro::FieldTypeMeta>::KIND,
                        ::icydb::model::field::FieldKind::Enum { .. }
                    ) {
                        panic!("direct custom fields in generated checks must be exact enums");
                    }
                };
            })
        })
        .collect()
}

impl FromMeta for Constraint {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut name = None;
        let mut check = None;

        for item in items {
            let NestedMeta::Meta(syn::Meta::NameValue(name_value)) = item else {
                return Err(DarlingError::custom(
                    "constraint(...) requires name = \"...\" and check = \"...\"",
                ));
            };
            let target = if name_value.path.is_ident("name") {
                &mut name
            } else if name_value.path.is_ident("check") {
                &mut check
            } else {
                return Err(DarlingError::custom(
                    "constraint(...) supports only name = \"...\" and check = \"...\"",
                )
                .with_span(&name_value.path));
            };
            let syn::Expr::Lit(expr_lit) = &name_value.value else {
                return Err(
                    DarlingError::custom("constraint arguments must be string literals")
                        .with_span(&name_value.value),
                );
            };
            let syn::Lit::Str(literal) = &expr_lit.lit else {
                return Err(
                    DarlingError::custom("constraint arguments must be string literals")
                        .with_span(&name_value.value),
                );
            };
            if target.replace(literal.clone()).is_some() {
                return Err(DarlingError::custom(
                    "constraint(...) accepts each argument exactly once",
                )
                .with_span(&name_value.path));
            }
        }

        Ok(Self {
            name: name
                .ok_or_else(|| DarlingError::custom("constraint(...) requires name = \"...\""))?,
            check: check
                .ok_or_else(|| DarlingError::custom("constraint(...) requires check = \"...\""))?,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::Constraint;
    use darling::{FromMeta, ast::NestedMeta};
    use quote::quote;

    fn parse(tokens: proc_macro2::TokenStream) -> Result<Constraint, darling::Error> {
        let args = NestedMeta::parse_meta_list(tokens)?;
        Constraint::from_list(args.as_slice())
    }

    #[test]
    fn parses_named_check_declaration() {
        let constraint = parse(quote!(name = "positive_balance", check = "balance >= 0"))
            .expect("named check should parse");

        assert_eq!(constraint.name.value(), "positive_balance");
        assert_eq!(constraint.check.value(), "balance >= 0");
    }

    #[test]
    fn rejects_missing_or_duplicate_arguments() {
        assert!(parse(quote!(name = "missing_check")).is_err());
        assert!(parse(quote!(name = "a", name = "b", check = "active = true")).is_err());
    }
}
