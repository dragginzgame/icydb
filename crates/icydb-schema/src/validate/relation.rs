use crate::{
    node::{DataStore, Entity, Schema, SchemaNode},
    prelude::*,
};
use std::collections::{BTreeMap, BTreeSet};

///
/// EntityInfo
/// Entity metadata needed for same-canister validation.
///

pub struct EntityInfo {
    name: String,
    canister: String,
}

///
/// RelationEdge
/// Relation occurrence captured during entity value-graph traversal.
///

pub struct RelationEdge {
    source_entity: String,
    target_entity: String,
    field: String,
}

// Validate that all relations reachable from an entity's value graph stay within its canister.
pub fn validate_same_canister_relations(schema: &Schema, errs: &mut ErrorTree) {
    // Phase 1: collect relation edges for each entity.
    let mut edges = Vec::new();
    for (entity_path, entity) in schema.get_nodes::<Entity>() {
        collect_entity_relations(schema, entity_path, entity, &mut edges);
    }

    // Phase 2: resolve canisters and enforce locality.
    let entity_info = build_entity_info_map(schema, errs);
    for edge in edges {
        let Some(source) = entity_info.get(&edge.source_entity) else {
            continue;
        };
        let Some(target) = entity_info.get(&edge.target_entity) else {
            continue;
        };
        if source.canister != target.canister {
            err!(
                errs,
                "entity '{0}' (canister '{1}'), field '{2}', has a relation to entity '{3}' (canister '{4}'), which is not allowed",
                source.name,
                source.canister,
                edge.field,
                target.name,
                target.canister
            );
        }
    }
}

// Build a map of entity path -> (resolved name, canister path) for validation.
fn build_entity_info_map(schema: &Schema, errs: &mut ErrorTree) -> BTreeMap<String, EntityInfo> {
    let mut entity_info = BTreeMap::new();
    for (entity_path, entity) in schema.get_nodes::<Entity>() {
        let store = match schema.cast_node::<DataStore>(entity.store) {
            Ok(store) => store,
            Err(e) => {
                errs.add(e);
                continue;
            }
        };

        entity_info.insert(
            entity_path.to_string(),
            EntityInfo {
                name: entity.resolved_name().to_string(),
                canister: store.canister.to_string(),
            },
        );
    }

    entity_info
}

// Collect all relation edges reachable from a single entity's value graph.
fn collect_entity_relations(
    schema: &Schema,
    entity_path: &str,
    entity: &Entity,
    edges: &mut Vec<RelationEdge>,
) {
    let mut visiting = BTreeSet::new();

    for field in entity.fields.fields {
        let mut field_path = vec![field.ident.to_string()];
        collect_value_relations(
            schema,
            entity_path,
            &field.value,
            &mut field_path,
            &mut visiting,
            edges,
        );
    }
}

// Walk a Value node and collect relation edges from its Item and nested shapes.
fn collect_value_relations(
    schema: &Schema,
    entity_path: &str,
    value: &Value,
    field_path: &mut Vec<String>,
    visiting: &mut BTreeSet<String>,
    edges: &mut Vec<RelationEdge>,
) {
    collect_item_relations(
        schema,
        entity_path,
        &value.item,
        field_path,
        visiting,
        edges,
    );
}

// Walk an Item node, recording relations and recursing into referenced type nodes.
fn collect_item_relations(
    schema: &Schema,
    entity_path: &str,
    item: &Item,
    field_path: &mut Vec<String>,
    visiting: &mut BTreeSet<String>,
    edges: &mut Vec<RelationEdge>,
) {
    if let Some(relation) = item.relation {
        edges.push(RelationEdge {
            source_entity: entity_path.to_string(),
            target_entity: relation.to_string(),
            field: format_field_path(field_path),
        });
    }

    if let ItemTarget::Is(path) = &item.target {
        traverse_type_node(schema, entity_path, path, field_path, visiting, edges);
    }
}

// Traverse a type node referenced from ItemTarget::Is, collecting relation edges.
fn traverse_type_node(
    schema: &Schema,
    entity_path: &str,
    type_path: &str,
    field_path: &mut Vec<String>,
    visiting: &mut BTreeSet<String>,
    edges: &mut Vec<RelationEdge>,
) {
    if !visiting.insert(type_path.to_string()) {
        return;
    }

    let Some(node) = schema.get_node(type_path) else {
        visiting.remove(type_path);
        return;
    };

    match node {
        SchemaNode::Record(record) => {
            for field in record.fields.fields {
                field_path.push(field.ident.to_string());
                collect_value_relations(
                    schema,
                    entity_path,
                    &field.value,
                    field_path,
                    visiting,
                    edges,
                );
                field_path.pop();
            }
        }
        SchemaNode::Enum(enumeration) => {
            for variant in enumeration.variants {
                let Some(value) = &variant.value else {
                    continue;
                };
                field_path.push(variant.ident.to_string());
                collect_value_relations(schema, entity_path, value, field_path, visiting, edges);
                field_path.pop();
            }
        }
        SchemaNode::Tuple(tuple) => {
            for (index, value) in tuple.values.iter().enumerate() {
                field_path.push(format!("[{index}]"));
                collect_value_relations(schema, entity_path, value, field_path, visiting, edges);
                field_path.pop();
            }
        }
        SchemaNode::List(list) => {
            field_path.push("item".to_string());
            collect_item_relations(schema, entity_path, &list.item, field_path, visiting, edges);
            field_path.pop();
        }
        SchemaNode::Set(set) => {
            field_path.push("item".to_string());
            collect_item_relations(schema, entity_path, &set.item, field_path, visiting, edges);
            field_path.pop();
        }
        SchemaNode::Map(map) => {
            field_path.push("key".to_string());
            collect_item_relations(schema, entity_path, &map.key, field_path, visiting, edges);
            field_path.pop();

            field_path.push("value".to_string());
            collect_value_relations(schema, entity_path, &map.value, field_path, visiting, edges);
            field_path.pop();
        }
        SchemaNode::Newtype(newtype) => {
            field_path.push("item".to_string());
            collect_item_relations(
                schema,
                entity_path,
                &newtype.item,
                field_path,
                visiting,
                edges,
            );
            field_path.pop();
        }
        _ => {}
    }

    visiting.remove(type_path);
}

// Render a dotted field path used in validation errors.
fn format_field_path(field_path: &[String]) -> String {
    field_path.join(".")
}
