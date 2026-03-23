use crate::prelude::*;
use icydb_schema::types::Cardinality;

// Generates typed Id accessors for every relation-backed field.
pub fn relation_accessor_tokens<'a>(fields: impl Iterator<Item = &'a Field>) -> Vec<TokenStream> {
    fields
        .filter_map(relation_accessor_tokens_for_field)
        .collect()
}

fn relation_accessor_tokens_for_field(field: &Field) -> Option<TokenStream> {
    let relation = field.value.item.relation.as_ref()?;
    Some(match field.value.cardinality() {
        Cardinality::One => one_relation_accessor_tokens(field, relation),
        Cardinality::Opt => optional_relation_accessor_tokens(field, relation),
        Cardinality::Many => many_relation_accessor_tokens(field, relation),
    })
}

fn one_relation_accessor_tokens(field: &Field, relation: &Path) -> TokenStream {
    let field_ident = &field.ident;
    let accessor_ident = accessor_ident(field);
    let setter_ident = setter_ident(field);

    quote! {
        /// Returns the typed relation ID for this field.
        #[must_use]
        pub fn #accessor_ident(&self) -> ::icydb::types::Id<#relation> {
            ::icydb::types::Id::from_key(self.#field_ident)
        }

        /// Replaces this relation field using a typed relation ID.
        pub fn #setter_ident(&mut self, value: ::icydb::types::Id<#relation>) {
            self.#field_ident = value.key();
        }
    }
}

fn optional_relation_accessor_tokens(field: &Field, relation: &Path) -> TokenStream {
    let field_ident = &field.ident;
    let accessor_ident = accessor_ident(field);
    let setter_ident = setter_ident(field);

    quote! {
        /// Returns the typed relation ID for this field, if present.
        #[must_use]
        pub fn #accessor_ident(&self) -> Option<::icydb::types::Id<#relation>> {
            self.#field_ident.map(::icydb::types::Id::from_key)
        }

        /// Replaces this optional relation field using typed relation IDs.
        pub fn #setter_ident(&mut self, value: Option<::icydb::types::Id<#relation>>) {
            self.#field_ident = value.map(|id| id.key());
        }
    }
}

fn many_relation_accessor_tokens(field: &Field, relation: &Path) -> TokenStream {
    let field_ident = &field.ident;
    let accessor_ident = accessor_ident(field);
    let add_ident = add_many_ident(field);
    let remove_ident = remove_many_ident(field);

    quote! {
        /// Returns typed relation IDs for all values in this field.
        #[must_use]
        pub fn #accessor_ident(&self) -> impl Iterator<Item = ::icydb::types::Id<#relation>> + '_ {
            self.#field_ident
                .iter()
                .copied()
                .map(::icydb::types::Id::from_key)
        }

        /// Appends one typed relation ID to this many-relation field.
        pub fn #add_ident(&mut self, value: ::icydb::types::Id<#relation>) {
            self.#field_ident.push(value.key());
        }

        /// Removes one typed relation ID from this many-relation field.
        ///
        /// Returns `true` when one matching relation ID was removed.
        pub fn #remove_ident(&mut self, value: ::icydb::types::Id<#relation>) -> bool {
            let key = value.key();
            if let Some(position) = self.#field_ident.iter().position(|existing| *existing == key) {
                self.#field_ident.remove(position);
                return true;
            }

            false
        }
    }
}

fn accessor_ident(field: &Field) -> Ident {
    // Relation field names already encode `_id` / `_ids`; accessors mirror the field.
    field.ident.clone()
}

fn setter_ident(field: &Field) -> Ident {
    format_ident!("set_{}", field.ident)
}

fn add_many_ident(field: &Field) -> Ident {
    let item_ident = many_item_ident(field);
    format_ident!("add_{item_ident}")
}

fn remove_many_ident(field: &Field) -> Ident {
    let item_ident = many_item_ident(field);
    format_ident!("remove_{item_ident}")
}

fn many_item_ident(field: &Field) -> Ident {
    let field_name = field.ident.to_string();
    let item_name = field_name
        .strip_suffix("_ids")
        .unwrap_or(field_name.as_str());

    format_ident!("{item_name}_id")
}
