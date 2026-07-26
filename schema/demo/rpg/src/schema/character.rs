use crate::schema::DemoRpgStore;
use icydb::design::prelude::*;

///
/// CharacterMentor
///
/// Nested mentor profile embedded in demo RPG characters.
/// The record gives SQL field-path examples a stable structural value with
/// scalar leaves for text, numeric, and principal projection tests.
///

#[record(
    source_key = "schema/demo/rpg/src/schema/character.rs::record::1",
    fields(
        field(
            source_key = "name",
            ident = "name",
            value(item(prim = "Text", unbounded)),
            default = "String::new"
        ),
        field(
            source_key = "level",
            ident = "level",
            value(item(prim = "Nat16")),
            default = 0u16
        ),
        field(
            source_key = "pid",
            ident = "pid",
            value(item(prim = "Principal")),
            default = "2vxsx-fae"
        )
    )
)]
pub struct CharacterMentor {}

///
/// Character
///
/// Fixture RPG character entity used by SQL endpoint and integration harnesses.
///

#[entity(source_key = "schema/demo/rpg/src/schema/character.rs::entity::1",
    audit_timestamps(
        created_at(source_key = "created_at", ident = "created_at"),
        updated_at(source_key = "updated_at", ident = "updated_at")
    ),
    store = "DemoRpgStore",
    version = 1,
    pk(fields = ["id"]),
    index(source_key = "index.1", fields = ["name"]),
    index(source_key = "index.2", fields = ["level", "class_name"]),
    fields(
        field(source_key = "id", ident = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(source_key = "name", ident = "name", value(item(prim = "Text", unbounded))),
        field(source_key = "description", ident = "description", value(item(prim = "Text", unbounded))),
        field(source_key = "class_name", ident = "class_name", value(item(prim = "Text", unbounded))),
        field(source_key = "background", ident = "background", value(item(prim = "Text", unbounded))),
        field(source_key = "homeland", ident = "homeland", value(item(prim = "Text", unbounded))),
        field(source_key = "level", ident = "level", value(item(prim = "Nat16"))),
        field(source_key = "experience", ident = "experience", value(item(prim = "Nat64"))),
        field(source_key = "renown", ident = "renown", value(item(prim = "Int16"))),
        field(source_key = "strength", ident = "strength", value(item(prim = "Int16"))),
        field(source_key = "dexterity", ident = "dexterity", value(item(prim = "Int16"))),
        field(source_key = "constitution", ident = "constitution", value(item(prim = "Int16"))),
        field(source_key = "intelligence", ident = "intelligence", value(item(prim = "Int16"))),
        field(source_key = "wisdom", ident = "wisdom", value(item(prim = "Int16"))),
        field(source_key = "charisma", ident = "charisma", value(item(prim = "Int16"))),
        field(source_key = "hit_points", ident = "hit_points", value(item(prim = "Int32"))),
        field(source_key = "armor_class", ident = "armor_class", value(item(prim = "Nat8"))),
        field(source_key = "spell_slots", ident = "spell_slots", value(item(prim = "Nat8"))),
        field(source_key = "initiative_bonus", ident = "initiative_bonus", value(item(prim = "Int8"))),
        field(source_key = "gold_pieces", ident = "gold_pieces", value(item(prim = "Nat32"))),
        field(source_key = "critical_chance", ident = "critical_chance", value(item(prim = "Decimal", scale = 2))),
        field(source_key = "dodge_chance", ident = "dodge_chance", value(item(prim = "Float64"))),
        field(source_key = "is_npc", ident = "is_npc", value(item(prim = "Bool"))),
        field(source_key = "guild_rank", ident = "guild_rank", value(opt, item(prim = "Text", unbounded))),
        field(source_key = "mentor", ident = "mentor", value(item(is = "CharacterMentor")),),
        field(source_key = "resistances", ident = "resistances", value(many, item(prim = "Text", unbounded))),
        field(source_key = "inventory_weights", ident = "inventory_weights", value(many, item(prim = "Nat16"))),
        field(source_key = "portrait", ident = "portrait", value(item(prim = "Blob", unbounded))),
        field(source_key = "last_rest_at", ident = "last_rest_at", value(item(prim = "Timestamp"))),
        field(source_key = "respawn_cooldown", ident = "respawn_cooldown", value(item(prim = "Duration")))
    )
)]
pub struct Character {}
