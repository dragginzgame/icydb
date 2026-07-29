use crate::schema::DemoRpgStore;
use icydb_model::prelude::*;

///
/// CharacterMentor
///
/// Nested mentor profile embedded in demo RPG characters.
/// The record gives SQL field-path examples a stable structural value with
/// scalar leaves for text, numeric, and principal projection tests.
///

#[record(fields(
    field(
        name = "name",
        value(item(prim = "Text", unbounded)),
        default = "String::new"
    ),
    field(name = "level", value(item(prim = "Nat16")), default = 0u16),
    field(name = "pid", value(item(prim = "Principal")), default = "2vxsx-fae")
))]
pub struct CharacterMentor {}

///
/// Character
///
/// Fixture RPG character entity used by SQL endpoint and integration harnesses.
///

#[entity(store = "DemoRpgStore",
    version = 1,
    pk(fields = ["id"]),
    index(fields = ["name"]),
    index(fields = ["level", "class_name"]),
    fields(
        field(name = "id",
            value(item(prim = "Ulid")),
            generated(insert = "Ulid::generate")
        ),
        field(name = "name", value(item(prim = "Text", unbounded))),
        field(name = "description", value(item(prim = "Text", unbounded))),
        field(name = "class_name", value(item(prim = "Text", unbounded))),
        field(name = "background", value(item(prim = "Text", unbounded))),
        field(name = "homeland", value(item(prim = "Text", unbounded))),
        field(name = "level", value(item(prim = "Nat16"))),
        field(name = "experience", value(item(prim = "Nat64"))),
        field(name = "renown", value(item(prim = "Int16"))),
        field(name = "strength", value(item(prim = "Int16"))),
        field(name = "dexterity", value(item(prim = "Int16"))),
        field(name = "constitution", value(item(prim = "Int16"))),
        field(name = "intelligence", value(item(prim = "Int16"))),
        field(name = "wisdom", value(item(prim = "Int16"))),
        field(name = "charisma", value(item(prim = "Int16"))),
        field(name = "hit_points", value(item(prim = "Int32"))),
        field(name = "armor_class", value(item(prim = "Nat8"))),
        field(name = "spell_slots", value(item(prim = "Nat8"))),
        field(name = "initiative_bonus", value(item(prim = "Int8"))),
        field(name = "gold_pieces", value(item(prim = "Nat32"))),
        field(name = "critical_chance", value(item(prim = "Decimal", scale = 2))),
        field(name = "dodge_chance", value(item(prim = "Float64"))),
        field(name = "is_npc", value(item(prim = "Bool"))),
        field(name = "guild_rank", value(opt, item(prim = "Text", unbounded))),
        field(name = "mentor", value(item(is = "CharacterMentor")),),
        field(name = "resistances", value(many, item(prim = "Text", unbounded))),
        field(name = "inventory_weights", value(many, item(prim = "Nat16"))),
        field(name = "portrait", value(item(prim = "Blob", unbounded))),
        field(name = "last_rest_at", value(item(prim = "Timestamp"))),
        field(name = "respawn_cooldown", value(item(prim = "Duration")))
    ),
    timestamps
)]
pub struct Character {}
