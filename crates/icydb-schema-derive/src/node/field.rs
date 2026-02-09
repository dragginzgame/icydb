use crate::{prelude::*, validate::reserved::is_reserved_word};
use canic_utils::case::{Case, Casing};
use std::slice::Iter;

///
/// FieldList
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct FieldList {
    #[darling(multiple, rename = "field")]
    pub fields: Vec<Field>,
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
            ::icydb::schema::node::FieldList {
                fields: #fields,
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
    pub ident: Ident,
    pub value: Value,

    #[darling(default)]
    pub default: Option<Arg>,

    #[darling(skip, default)]
    pub is_system: bool,
}

// Storage suffixes are forbidden on relation field names.
const BANNED_SUFFIXES: [&str; 6] = ["_id", "_ids", "_ref", "_refs", "_key", "_keys"];

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

        // Enforce suffix bans only for relation fields.
        if self.value.item.is_relation()
            && BANNED_SUFFIXES
                .iter()
                .any(|suffix| ident_str.ends_with(suffix))
        {
            let suffixes = BANNED_SUFFIXES
                .iter()
                .map(|suffix| format!("'{suffix}'"))
                .collect::<Vec<_>>()
                .join(", ");

            return Err(DarlingError::custom(format!(
                "relation field ident '{ident_str}' must not end with {suffixes}"
            ))
            .with_span(&self.ident));
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
            is_system: true,
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
            is_system: true,
        }
    }
}

impl HasSchemaPart for Field {
    fn schema_part(&self) -> TokenStream {
        let ident = quote_one(&self.ident, to_str_lit);
        let value = self.value.schema_part();
        let default = quote_option(self.default.as_ref(), Arg::schema_part);

        quote! {
            ::icydb::schema::node::Field {
                ident: #ident,
                value: #value,
                default: #default,
            }
        }
    }
}

impl HasTypeExpr for Field {
    fn type_expr(&self) -> TokenStream {
        let ident = &self.ident;
        let value = self.value.type_expr();

        quote! {
            pub(crate) #ident: #value
        }
    }
}
