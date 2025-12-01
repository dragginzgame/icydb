use crate::{Error, prelude::*};
use std::{any::Any, collections::BTreeMap};

///
/// SchemaNode
///

#[remain::sorted]
#[derive(Clone, Debug, Serialize)]
pub enum SchemaNode {
    Canister(Canister),
    Entity(Entity),
    Enum(Enum),
    List(List),
    Map(Map),
    Newtype(Newtype),
    Record(Record),
    Sanitizer(Sanitizer),
    Set(Set),
    Store(Store),
    Tuple(Tuple),
    Validator(Validator),
}

impl SchemaNode {
    #[must_use]
    pub fn get_type(&self) -> Option<Box<dyn TypeNode>> {
        match self {
            Self::Entity(n) => Some(Box::new(n.clone())),
            Self::Enum(n) => Some(Box::new(n.clone())),
            Self::List(n) => Some(Box::new(n.clone())),
            Self::Map(n) => Some(Box::new(n.clone())),
            Self::Newtype(n) => Some(Box::new(n.clone())),
            Self::Record(n) => Some(Box::new(n.clone())),
            Self::Set(n) => Some(Box::new(n.clone())),
            Self::Tuple(n) => Some(Box::new(n.clone())),
            _ => None,
        }
    }
}

impl SchemaNode {
    const fn def(&self) -> &Def {
        match self {
            Self::Canister(n) => &n.def,
            Self::Entity(n) => &n.def,
            Self::Enum(n) => &n.def,
            Self::List(n) => &n.def,
            Self::Map(n) => &n.def,
            Self::Newtype(n) => &n.def,
            Self::Record(n) => &n.def,
            Self::Sanitizer(n) => &n.def,
            Self::Set(n) => &n.def,
            Self::Store(n) => &n.def,
            Self::Tuple(n) => &n.def,
            Self::Validator(n) => &n.def,
        }
    }
}

impl MacroNode for SchemaNode {
    fn as_any(&self) -> &dyn Any {
        match self {
            Self::Canister(n) => n.as_any(),
            Self::Entity(n) => n.as_any(),
            Self::Enum(n) => n.as_any(),
            Self::List(n) => n.as_any(),
            Self::Map(n) => n.as_any(),
            Self::Newtype(n) => n.as_any(),
            Self::Record(n) => n.as_any(),
            Self::Sanitizer(n) => n.as_any(),
            Self::Set(n) => n.as_any(),
            Self::Store(n) => n.as_any(),
            Self::Tuple(n) => n.as_any(),
            Self::Validator(n) => n.as_any(),
        }
    }
}

impl ValidateNode for SchemaNode {}

impl VisitableNode for SchemaNode {
    fn drive<V: Visitor>(&self, v: &mut V) {
        match self {
            Self::Canister(n) => n.accept(v),
            Self::Entity(n) => n.accept(v),
            Self::Enum(n) => n.accept(v),
            Self::List(n) => n.accept(v),
            Self::Map(n) => n.accept(v),
            Self::Newtype(n) => n.accept(v),
            Self::Record(n) => n.accept(v),
            Self::Sanitizer(n) => n.accept(v),
            Self::Set(n) => n.accept(v),
            Self::Store(n) => n.accept(v),
            Self::Tuple(n) => n.accept(v),
            Self::Validator(n) => n.accept(v),
        }
    }
}

///
/// Schema
///

#[derive(Clone, Debug, Serialize)]
pub struct Schema {
    pub nodes: BTreeMap<String, SchemaNode>,
    pub hash: &'static str,
    pub timestamp: u64,
}

impl Schema {
    #[must_use]
    pub fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            hash: "",
            timestamp: canic::utils::time::now_secs(),
        }
    }

    // insert_node
    pub fn insert_node(&mut self, node: SchemaNode) {
        self.nodes.insert(node.def().path(), node);
    }

    // get_node
    #[must_use]
    pub fn get_node<'a>(&'a self, path: &str) -> Option<&'a SchemaNode> {
        self.nodes.get(path)
    }

    // try_get_node
    pub fn try_get_node<'a>(&'a self, path: &str) -> Result<&'a SchemaNode, Error> {
        let node = self
            .get_node(path)
            .ok_or_else(|| NodeError::PathNotFound(path.to_string()))?;

        Ok(node)
    }

    // cast_node
    pub fn cast_node<'a, T: 'static>(&'a self, path: &str) -> Result<&'a T, Error> {
        let node = self.try_get_node(path)?;

        node.as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| NodeError::IncorrectNodeType(path.to_string()).into())
    }

    // check_node_as
    pub fn check_node_as<T: 'static>(&self, path: &str) -> Result<(), Error> {
        self.cast_node::<T>(path).map(|_| ())
    }

    // get_nodes
    pub fn get_nodes<T: 'static>(&self) -> impl Iterator<Item = (&str, &T)> {
        self.nodes
            .iter()
            .filter_map(|(key, node)| node.as_any().downcast_ref::<T>().map(|n| (key.as_str(), n)))
    }

    // get_node_values
    pub fn get_node_values<T: 'static>(&'_ self) -> impl Iterator<Item = &'_ T> + '_ {
        self.nodes
            .values()
            .filter_map(|node| node.as_any().downcast_ref::<T>())
    }

    // filter_nodes
    // Generic method to filter key, and nodes of any type with a predicate
    pub fn filter_nodes<'a, T: 'static>(
        &'a self,
        predicate: impl Fn(&T) -> bool + 'a,
    ) -> impl Iterator<Item = (&'a str, &'a T)> + 'a {
        self.nodes.iter().filter_map(move |(key, node)| {
            node.as_any()
                .downcast_ref::<T>()
                .filter(|target| predicate(target))
                .map(|target| (key.as_str(), target))
        })
    }
}

impl Default for Schema {
    fn default() -> Self {
        Self::new()
    }
}

impl ValidateNode for Schema {}

impl VisitableNode for Schema {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.nodes.values() {
            node.accept(v);
        }
    }
}
