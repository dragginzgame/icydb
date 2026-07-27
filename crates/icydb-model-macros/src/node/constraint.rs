//! Module: node::constraint
//! Responsibility: generated named-check parsing and build-time validation.
//! Does not own: accepted field identity, literal admission, or runtime enforcement.
//! Boundary: entity macro metadata to one structured check proposal.

use crate::{
    node::index::validate_predicate_fields,
    predicate::{self, Predicate},
    prelude::*,
};
use darling::ast::NestedMeta;

/// Parsed `constraint(source_key = "...", name = "...", check = "...")` declaration.
#[derive(Debug)]
pub(crate) struct Constraint {
    pub(crate) source_key: LitStr,
    pub(crate) name: LitStr,
    pub(crate) check: LitStr,
}

impl Constraint {
    /// Parse and validate the generated check against declared entity fields.
    pub(crate) fn validated_predicate(&self, entity: &Entity) -> Result<Predicate, DarlingError> {
        validate_constraint_name(self.name.value().as_str())
            .map_err(|error| error.with_span(&self.name))?;
        let predicate = predicate::parse(self.check.value().as_str())
            .map_err(|error| error.with_span(&self.check))?;
        validate_predicate_fields(entity, &predicate, true)
            .map_err(|error| error.with_span(&self.check))?;

        Ok(predicate)
    }

    pub(crate) fn schema_part_for_entity(
        &self,
        entity: &Entity,
    ) -> Result<TokenStream, DarlingError> {
        let source_key = &self.source_key;
        let name = &self.name;
        let check = &self.check;
        let predicate = self.validated_predicate(entity)?;
        let expression =
            crate::node::index::predicate_source_expression_tokens(&predicate, entity)?;

        Ok(quote! {
            ::icydb_model::node::CheckConstraint::new(
                #source_key,
                #name,
                #check,
                |_schema| #expression,
            )
        })
    }
}

impl HasSchemaPart for Constraint {
    fn schema_part(&self) -> TokenStream {
        TokenStream::new()
    }
}

fn validate_constraint_name(name: &str) -> Result<(), DarlingError> {
    if name.is_empty() {
        return Err(DarlingError::custom(
            "generated constraint name must not be empty",
        ));
    }
    if name.len() > icydb_schema::MAX_SCHEMA_NAME_BYTES {
        return Err(DarlingError::custom(
            "generated constraint name exceeds its byte bound",
        ));
    }
    if name
        .chars()
        .any(|character| character.is_whitespace() || character.is_control())
    {
        return Err(DarlingError::custom(
            "generated constraint name cannot contain whitespace or control characters",
        ));
    }
    Ok(())
}

impl FromMeta for Constraint {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut name = None;
        let mut source_key = None;
        let mut check = None;

        for item in items {
            let NestedMeta::Meta(syn::Meta::NameValue(name_value)) = item else {
                return Err(DarlingError::custom(
                    "constraint(...) requires name = \"...\" and check = \"...\"",
                ));
            };
            let target = if name_value.path.is_ident("source_key") {
                &mut source_key
            } else if name_value.path.is_ident("name") {
                &mut name
            } else if name_value.path.is_ident("check") {
                &mut check
            } else {
                return Err(DarlingError::custom(
                    "constraint(...) supports only source_key = \"...\", name = \"...\", and check = \"...\"",
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
            source_key: source_key.ok_or_else(|| {
                DarlingError::custom("constraint(...) requires source_key = \"...\"")
            })?,
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
        let constraint = parse(quote!(
            source_key = "positive_balance",
            name = "positive_balance",
            check = "balance >= 0"
        ))
        .expect("named check should parse");

        assert_eq!(constraint.source_key.value(), "positive_balance");
        assert_eq!(constraint.name.value(), "positive_balance");
        assert_eq!(constraint.check.value(), "balance >= 0");
    }

    #[test]
    fn rejects_missing_or_duplicate_arguments() {
        assert!(parse(quote!(name = "missing_check")).is_err());
        assert!(
            parse(quote!(
                source_key = "active",
                name = "a",
                name = "b",
                check = "active = true"
            ))
            .is_err()
        );
    }
}
