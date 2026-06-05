use super::{default_strategy_entity, record_default_strategy};
use crate::{
    node::{
        Arg, Def, Entity, Field, FieldList, Item, PrimaryKey, PrimaryKeySource, Record, Type, Value,
    },
    trait_kind::{TraitBuilder, TraitKind},
};
use icydb_schema::types::Primitive;
use quote::format_ident;
use syn::parse_quote;

fn field_with_primitive_default(ident: &str, primitive: Primitive, default: Arg) -> Field {
    Field {
        ident: format_ident!("{ident}"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(primitive),
                ..Item::default()
            },
        },
        default: Some(default),
        generated: None,
        write_management: None,
    }
}

fn required_field_without_default(ident: &str, primitive: Primitive) -> Field {
    Field {
        ident: format_ident!("{ident}"),
        value: Value {
            opt: false,
            many: false,
            item: Item {
                primitive: Some(primitive),
                ..Item::default()
            },
        },
        default: None,
        generated: None,
        write_management: None,
    }
}

fn redundant_default_entity() -> Entity {
    Entity {
        def: Def::new(syn::parse_quote!(
            struct RedundantDefaultEntity;
        )),
        store: syn::parse_quote!(UiDataStore),
        schema_version: 1,
        primary_key: PrimaryKey {
            fields: vec![format_ident!("id")],
            source: PrimaryKeySource::Internal,
        },
        name: None,
        indexes: vec![],
        relations: vec![],
        fields: FieldList {
            fields: vec![
                field_with_primitive_default(
                    "id",
                    Primitive::Nat64,
                    Arg::FuncPath(parse_quote!(u64::default)),
                ),
                field_with_primitive_default(
                    "name",
                    Primitive::Text,
                    Arg::FuncPath(parse_quote!(String::new)),
                ),
            ],
        },
        ty: Type::default(),
        traits: TraitBuilder::default(),
    }
}

#[test]
fn entity_defaults_derive_when_explicit_defaults_match_implicit_defaults() {
    let strategy = default_strategy_entity(&redundant_default_entity());

    assert_eq!(strategy.derive, Some(TraitKind::Default));
    assert!(
        strategy.imp.is_none(),
        "redundant defaults should not force a manual Default impl",
    );
}

#[test]
fn entity_defaults_are_not_generated_for_required_fields_without_defaults() {
    let mut entity = redundant_default_entity();
    entity
        .fields
        .fields
        .push(required_field_without_default("score", Primitive::Int32));

    let strategy = default_strategy_entity(&entity);

    assert!(strategy.derive.is_none());
    assert!(strategy.imp.is_none());
}

#[test]
fn entity_defaults_keep_manual_impl_for_custom_default_constructors() {
    let mut entity = redundant_default_entity();
    entity.fields.fields[0].default = Some(Arg::FuncPath(parse_quote!(Ulid::generate)));
    entity.fields.fields[0].value.item.primitive = Some(Primitive::Ulid);

    let strategy = default_strategy_entity(&entity);

    assert!(
        strategy.derive.is_none(),
        "custom defaults must still bypass derive(Default)",
    );
    assert!(
        strategy.imp.is_some(),
        "custom defaults still require an explicit Default impl",
    );
}

#[test]
fn records_follow_the_same_redundant_default_rule() {
    let fields = FieldList {
        fields: vec![
            field_with_primitive_default(
                "enabled",
                Primitive::Bool,
                Arg::FuncPath(parse_quote!(bool::default)),
            ),
            field_with_primitive_default(
                "name",
                Primitive::Text,
                Arg::FuncPath(parse_quote!(String::new)),
            ),
        ],
    };
    let record = Record {
        def: Def::new(syn::parse_quote!(
            struct RedundantDefaultRecord;
        )),
        fields,
        traits: TraitBuilder::default(),
        ty: Type::default(),
    };

    let strategy = record_default_strategy(&record.def, &record.fields);

    assert_eq!(strategy.derive, Some(TraitKind::Default));
    assert!(
        strategy.imp.is_none(),
        "records with redundant defaults should derive Default",
    );
}

#[test]
fn record_defaults_are_not_generated_for_required_fields_without_defaults() {
    let fields = FieldList {
        fields: vec![required_field_without_default("score", Primitive::Int32)],
    };
    let record = Record {
        def: Def::new(syn::parse_quote!(
            struct RequiredRecord;
        )),
        fields,
        traits: TraitBuilder::default(),
        ty: Type::default(),
    };

    let strategy = record_default_strategy(&record.def, &record.fields);

    assert!(strategy.derive.is_none());
    assert!(strategy.imp.is_none());
}
