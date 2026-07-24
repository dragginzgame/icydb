//! Frozen bounds for the version-1 proposal transport.

/// Maximum encoded size of one reusable schema fragment.
pub const MAX_SCHEMA_FRAGMENT_BYTES: usize = 512 * 1024;

/// Maximum encoded size of one database-scoped schema proposal.
pub const MAX_SCHEMA_PROPOSAL_BYTES: usize = 2 * 1024 * 1024;

/// Maximum byte length of one immutable source key.
pub const MAX_SOURCE_KEY_BYTES: usize = 128;

/// Maximum byte length of one editable schema/display name.
pub const MAX_SCHEMA_NAME_BYTES: usize = 256;

/// Maximum byte length of one caller-generated submission key.
pub const MAX_SCHEMA_SUBMISSION_KEY_BYTES: usize = 128;

/// Maximum byte length of one text or blob proposal literal.
pub const MAX_PROPOSAL_LITERAL_BYTES: usize = 64 * 1024;

/// Maximum number of reusable fragments in one proposal.
pub const MAX_SCHEMA_PROPOSAL_FRAGMENTS: usize = 1_024;

/// Maximum number of entity definitions in one fragment.
pub const MAX_FRAGMENT_ENTITIES: usize = 1_024;

/// Maximum number of named type definitions in one fragment.
pub const MAX_FRAGMENT_TYPES: usize = 1_024;

/// Maximum number of fields in one entity or record definition.
pub const MAX_FRAGMENT_FIELDS: usize = 256;

/// Maximum number of indexes in one entity definition.
pub const MAX_FRAGMENT_INDEXES: usize = 64;

/// Maximum number of relations in one entity definition.
pub const MAX_FRAGMENT_RELATIONS: usize = 64;

/// Maximum number of accepted constraints in one entity definition.
pub const MAX_FRAGMENT_CONSTRAINTS: usize = 256;

/// Maximum number of instructions in one source-level check expression.
pub const MAX_SOURCE_CHECK_INSTRUCTIONS: usize = 1_024;

/// Maximum resolved depth of one named-type graph.
///
/// The root named type occupies depth one. References that must be resolved
/// against the expected accepted head are checked during application.
pub const MAX_SCHEMA_TYPE_DEPTH: usize = 64;

/// Maximum number of explicit removals in one proposal.
pub const MAX_SCHEMA_REMOVALS: usize = 4_096;

/// Maximum number of entity-to-store assignments in one proposal.
pub const MAX_SCHEMA_ASSIGNMENTS: usize = 4_096;

/// Maximum number of required capabilities in one proposal.
pub const MAX_SCHEMA_CAPABILITIES: usize = 32;
