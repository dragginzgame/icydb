//!
//! RPG demo canister used by local demos and fixture loading.
//!

#[cfg(feature = "test-admin-api")]
use icydb::{
    db::{StructuralPatch, WriteCell},
    value::InputValue,
};
#[cfg(feature = "test-admin-api")]
use icydb_testing_demo_rpg_fixtures::{
    fixtures,
    schema::{Character, CharacterMentor, Grid},
};

icydb::start!();

icydb::endpoints! {
    #[cfg(feature = "local-sql-query")]
    icydb_sql_query(introspection = true);
    #[cfg(feature = "sql")]
    icydb_ddl;
    icydb_metrics(authorization = public);
    #[cfg(feature = "local-extended-metrics")]
    icydb_metrics_extended(authorization = public);
    icydb_metrics_reset;
    icydb_snapshot;
    icydb_schema(authorization = controller);
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_reset;
    #[cfg(feature = "test-admin-api")]
    icydb_fixtures_load(handler = icydb_fixtures_load);
}

/// Load one deterministic baseline fixture dataset.
#[cfg(feature = "test-admin-api")]
fn icydb_fixtures_load() -> Result<(), icydb::Error> {
    db()?.execute_trusted_structural_insert_batch(
        "Character",
        fixtures::characters()
            .into_iter()
            .map(character_patch)
            .collect(),
    )?;
    db()?.execute_trusted_structural_insert_batch(
        "Grid",
        fixtures::grid().into_iter().map(grid_patch).collect(),
    )?;

    Ok(())
}

#[cfg(feature = "test-admin-api")]
fn authored(value: impl Into<InputValue>) -> WriteCell<InputValue> {
    WriteCell::Value(value.into())
}

#[cfg(feature = "test-admin-api")]
fn character_patch(character: Character) -> StructuralPatch {
    StructuralPatch::new()
        .field("name", authored(character.name))
        .field("description", authored(character.description))
        .field("class_name", authored(character.class_name))
        .field("background", authored(character.background))
        .field("homeland", authored(character.homeland))
        .field("level", authored(character.level))
        .field("experience", authored(character.experience))
        .field("renown", authored(character.renown))
        .field("strength", authored(character.strength))
        .field("dexterity", authored(character.dexterity))
        .field("constitution", authored(character.constitution))
        .field("intelligence", authored(character.intelligence))
        .field("wisdom", authored(character.wisdom))
        .field("charisma", authored(character.charisma))
        .field("hit_points", authored(character.hit_points))
        .field("armor_class", authored(character.armor_class))
        .field("spell_slots", authored(character.spell_slots))
        .field("initiative_bonus", authored(character.initiative_bonus))
        .field("gold_pieces", authored(character.gold_pieces))
        .field("critical_chance", authored(character.critical_chance))
        .field("dodge_chance", authored(character.dodge_chance))
        .field("is_npc", authored(character.is_npc))
        .field("guild_rank", authored(character.guild_rank))
        .field("mentor", authored(mentor_input(character.mentor)))
        .field(
            "resistances",
            authored(InputValue::List(
                character
                    .resistances
                    .into_iter()
                    .map(InputValue::from)
                    .collect(),
            )),
        )
        .field(
            "inventory_weights",
            authored(InputValue::List(
                character
                    .inventory_weights
                    .into_iter()
                    .map(InputValue::from)
                    .collect(),
            )),
        )
        .field("portrait", authored(character.portrait))
        .field("last_rest_at", authored(character.last_rest_at))
        .field("respawn_cooldown", authored(character.respawn_cooldown))
}

#[cfg(feature = "test-admin-api")]
fn mentor_input(mentor: CharacterMentor) -> InputValue {
    InputValue::Map(vec![
        ("name".into(), mentor.name.into()),
        ("level".into(), mentor.level.into()),
        ("pid".into(), mentor.pid.into()),
    ])
}

#[cfg(feature = "test-admin-api")]
fn grid_patch(cell: Grid) -> StructuralPatch {
    StructuralPatch::new()
        .field("x", authored(cell.x))
        .field("y", authored(cell.y))
        .field("terrain", authored(cell.terrain))
        .field("elevation", authored(cell.elevation))
        .field("danger_level", authored(cell.danger_level))
        .field("discovered", authored(cell.discovered))
}

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
