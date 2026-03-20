use crate::{prelude::*, validate::reserved::is_reserved_word};
use canic_utils::case::{Case, Casing};
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
            ::icydb::schema::node::FieldList::new(#fields)
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
    use crate::node::Item;
    use icydb_schema::types::Primitive;
    use quote::format_ident;

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
}
