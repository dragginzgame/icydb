use crate::{Error, prelude::*};
use icydb_schema::{ScalarLiteral, SchemaContractError, TypeSourceKey};
use std::{any::Any, collections::BTreeMap};

use sha2::{Digest, Sha256};

///
/// SchemaNode
///

#[remain::sorted]
#[derive(Clone, Debug, Serialize)]
pub enum SchemaNode {
    /// A registered [`Canister`] authoring declaration.
    Canister(Canister),
    /// A registered [`Entity`] authoring declaration.
    Entity(Entity),
    /// A registered [`Enum`] authoring declaration.
    Enum(Enum),
    /// A registered [`List`] authoring declaration.
    List(List),
    /// A registered [`Map`] authoring declaration.
    Map(Map),
    /// A registered [`Newtype`] authoring declaration.
    Newtype(Newtype),
    /// A registered [`Normalizer`] authoring declaration.
    Normalizer(Normalizer),
    /// A registered [`Record`] authoring declaration.
    Record(Record),
    /// A registered [`Set`] authoring declaration.
    Set(Set),
    /// A registered [`Store`] authoring declaration.
    Store(Store),
    /// A registered [`Tuple`] authoring declaration.
    Tuple(Tuple),
    /// A registered [`Validator`] authoring declaration.
    Validator(Validator),
}

impl SchemaNode {
    const fn def(&self) -> &Def {
        match self {
            Self::Canister(n) => n.def(),
            Self::Entity(n) => n.def(),
            Self::Enum(n) => n.def(),
            Self::List(n) => n.def(),
            Self::Map(n) => n.def(),
            Self::Newtype(n) => n.def(),
            Self::Normalizer(n) => n.def(),
            Self::Record(n) => n.def(),
            Self::Set(n) => n.def(),
            Self::Store(n) => n.def(),
            Self::Tuple(n) => n.def(),
            Self::Validator(n) => n.def(),
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
            Self::Normalizer(n) => n.as_any(),
            Self::Record(n) => n.as_any(),
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
            Self::Normalizer(n) => n.accept(v),
            Self::Record(n) => n.accept(v),
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
    nodes: BTreeMap<String, SchemaNode>,
    #[serde(skip)]
    state: SchemaState,
    #[serde(skip)]
    registration_error: Option<SchemaGraphError>,
}

impl Schema {
    #[must_use]
    /// Create an empty collecting graph; registration and sealing happen separately.
    pub const fn new() -> Self {
        Self {
            nodes: BTreeMap::new(),
            state: SchemaState::Collecting,
            registration_error: None,
        }
    }

    /// Register one constructor-produced node while the graph is collecting.
    ///
    /// Duplicate and late registration are retained as graph errors rather
    /// than replacing an earlier declaration. [`crate::build::get_schema`]
    /// reports the lexically canonical retained error before exposing a sealed
    /// snapshot.
    pub fn insert_node(&mut self, node: SchemaNode) {
        let path = node.def().path();
        if self.state.is_sealed() {
            self.record_registration_error(SchemaGraphError::LateRegistration(path));
            return;
        }
        if self.nodes.contains_key(path.as_str()) {
            self.record_registration_error(SchemaGraphError::DuplicateRegistration(path));
            return;
        }
        self.nodes.insert(path, node);
    }

    /// Seal the complete graph after whole-graph validation.
    ///
    /// # Errors
    ///
    /// Returns the canonical duplicate or late-registration failure retained
    /// while constructors populated the graph.
    pub(crate) fn seal(&mut self) -> Result<SchemaGraphDigest, SchemaGraphError> {
        if let Some(error) = self.registration_error.clone() {
            return Err(error);
        }
        if let SchemaState::Sealed(digest) = self.state {
            return Ok(digest);
        }

        let mut hasher = Sha256::new();
        for (path, node) in &self.nodes {
            hash_bounded_bytes(&mut hasher, path.as_bytes());
            let encoded =
                serde_json::to_vec(node).map_err(|_| SchemaGraphError::SnapshotEncoding)?;
            hash_bounded_bytes(&mut hasher, encoded.as_slice());
        }
        let digest = SchemaGraphDigest(hasher.finalize().into());
        self.state = SchemaState::Sealed(digest);

        Ok(digest)
    }

    /// Return whether whole-graph validation has sealed this graph.
    #[must_use]
    pub const fn is_sealed(&self) -> bool {
        self.state.is_sealed()
    }

    /// Return the immutable digest of a sealed graph.
    #[must_use]
    pub const fn digest(&self) -> Option<SchemaGraphDigest> {
        match self.state {
            SchemaState::Collecting => None,
            SchemaState::Sealed(digest) => Some(digest),
        }
    }

    fn record_registration_error(&mut self, error: SchemaGraphError) {
        match self.registration_error.as_ref() {
            Some(current) if current <= &error => {}
            Some(_) | None => self.registration_error = Some(error),
        }
    }

    // get_node
    #[must_use]
    /// Look up a declaration by its fully qualified Rust path.
    pub fn get_node<'a>(&'a self, path: &str) -> Option<&'a SchemaNode> {
        self.nodes.get(path)
    }

    // try_get_node
    /// Look up a declaration, reporting a missing-path error when absent.
    pub fn try_get_node<'a>(&'a self, path: &str) -> Result<&'a SchemaNode, Error> {
        let node = self
            .get_node(path)
            .ok_or_else(|| NodeError::PathNotFound(path.to_string()))?;

        Ok(node)
    }

    // cast_node
    /// Look up and downcast a declaration, reporting missing paths or mismatched node kinds.
    pub fn cast_node<'a, T: 'static>(&'a self, path: &str) -> Result<&'a T, Error> {
        let node = self.try_get_node(path)?;

        node.as_any()
            .downcast_ref::<T>()
            .ok_or_else(|| NodeError::IncorrectNodeType(path.to_string()).into())
    }

    // check_node_as
    pub(crate) fn check_node_as<T: 'static>(&self, path: &str) -> Result<(), Error> {
        self.cast_node::<T>(path).map(|_| ())
    }

    // get_nodes
    /// Iterate declarations of the requested concrete node type with their Rust paths.
    pub fn get_nodes<T: 'static>(&self) -> impl Iterator<Item = (&str, &T)> {
        self.nodes
            .iter()
            .filter_map(|(key, node)| node.as_any().downcast_ref::<T>().map(|n| (key.as_str(), n)))
    }

    // filter_nodes
    // Generic method to filter key, and nodes of any type with a predicate
    /// Iterate nodes of the requested type that satisfy the supplied predicate.
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

    /// Borrow all schema nodes indexed by path.
    #[must_use]
    pub const fn nodes(&self) -> &BTreeMap<String, SchemaNode> {
        &self.nodes
    }

    /// Resolve one authored unit-enum literal through current declared names.
    ///
    /// # Errors
    ///
    /// Returns an invalid-enum-literal error when the path is not an enum, the
    /// variant is absent, or either maintained name is malformed.
    pub fn enum_unit_literal(
        &self,
        enum_path: &str,
        variant_name: &str,
    ) -> Result<ScalarLiteral, SchemaContractError> {
        let r#enum = self
            .get_node(enum_path)
            .and_then(|node| node.as_any().downcast_ref::<Enum>())
            .ok_or(SchemaContractError::InvalidEnumLiteral)?;
        let variant = r#enum
            .variants()
            .iter()
            .find(|variant| variant.name() == variant_name && variant.value().is_none())
            .ok_or(SchemaContractError::InvalidEnumLiteral)?;
        Ok(ScalarLiteral::EnumUnit {
            enum_type: TypeSourceKey::try_new(r#enum.name())?,
            variant: TypeSourceKey::try_new(variant.name())?,
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

fn hash_bounded_bytes(hasher: &mut Sha256, bytes: &[u8]) {
    hasher.update((bytes.len() as u64).to_be_bytes());
    hasher.update(bytes);
}

///
/// SchemaGraphDigest
///
/// Deterministic identity of one validated, sealed host authoring graph.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SchemaGraphDigest([u8; 32]);

impl SchemaGraphDigest {
    /// Return the digest bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

///
/// SchemaState
///
/// Construction phase of one host authoring graph.
///

#[derive(Clone, Copy, Debug, Default)]
enum SchemaState {
    #[default]
    Collecting,
    Sealed(SchemaGraphDigest),
}

impl SchemaState {
    const fn is_sealed(self) -> bool {
        matches!(self, Self::Sealed(_))
    }
}

///
/// SchemaGraphError
///
/// Deterministic graph-construction failure retained until sealing.
///

#[derive(Clone, Debug, Eq, thiserror::Error, Ord, PartialEq, PartialOrd)]
pub enum SchemaGraphError {
    /// A second constructor declared the same complete Rust path.
    #[error("duplicate authoring-graph registration for '{0}'")]
    DuplicateRegistration(String),

    /// A constructor attempted to mutate an already sealed graph.
    #[error("late authoring-graph registration for '{0}'")]
    LateRegistration(String),

    /// The validated graph could not be encoded for deterministic identity.
    #[error("authoring graph could not be encoded for deterministic identity")]
    SnapshotEncoding,
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
