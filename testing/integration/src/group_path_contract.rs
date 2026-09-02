//! Frozen scalar record-path grouping measurement contract.

/// Measurement contract version. This is not a persisted or wire format.
pub const GROUP_PATH_CONTRACT_VERSION: u32 = 1;

/// Maximum deterministic fixture cardinality.
pub const GROUP_PATH_FIXTURE_ROWS: u32 = 2_048;

/// Exact retained actor source identity.
pub const GROUP_PATH_ACTOR_SOURCE_SHA256: &str =
    "e014f79760c79259e58630ee514b74174dddb7175480dd906ca77907f8f56283";

/// Exact generated schema source identity.
pub const GROUP_PATH_SCHEMA_SOURCE_SHA256: &str =
    "b33429d5140d8c53d06e79da8fa3ddec471dd8cc746888904d9c49f3434f3b6c";

/// Direct mirrored control query.
pub const GROUP_PATH_DIRECT_QUERY: &str = "SELECT direct_rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY direct_rank \
ORDER BY direct_rank ASC \
LIMIT 127";

/// Required scalar record-path query admitted by Landing Slice 3.
pub const GROUP_PATH_REQUIRED_QUERY: &str = "SELECT profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY profile.rank \
ORDER BY profile.rank ASC \
LIMIT 127";

/// Required scalar record-path query without final ordering or a row limit.
pub const GROUP_PATH_REQUIRED_COUNT_QUERY: &str = "SELECT profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY profile.rank";

/// Optional scalar record-path query covering present, missing, and null.
pub const GROUP_PATH_OPTIONAL_QUERY: &str = "SELECT optional_profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY optional_profile.rank \
ORDER BY optional_profile.rank ASC \
LIMIT 128";

/// Nullable-terminal scalar path restricted to rows represented by its index.
pub const GROUP_PATH_OPTIONAL_NON_NULL_QUERY: &str = "SELECT profile.optional_rank, COUNT(*) \
FROM GroupPathAuditRow \
WHERE profile.optional_rank >= 0 \
GROUP BY profile.optional_rank \
ORDER BY profile.optional_rank ASC \
LIMIT 127";

/// Nullable-terminal path without the excluding predicate required by its index.
pub const GROUP_PATH_NULLABLE_TERMINAL_QUERY: &str = "SELECT profile.optional_rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY profile.optional_rank \
ORDER BY profile.optional_rank ASC \
LIMIT 127";

/// Equality-prefix grouping that must reject an incomplete nullable suffix.
pub const GROUP_PATH_OMISSION_PREFIX_QUERY: &str = "SELECT optional_profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
WHERE direct_rank = 0 \
GROUP BY optional_profile.rank \
ORDER BY optional_profile.rank ASC \
LIMIT 2";

/// Equality-prefix grouping whose predicate excludes the omitted suffix rows.
pub const GROUP_PATH_OMISSION_PREFIX_NON_NULL_QUERY: &str = "SELECT optional_profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
WHERE direct_rank = 0 AND optional_profile.rank >= 0 \
GROUP BY optional_profile.rank \
ORDER BY optional_profile.rank ASC \
LIMIT 1";

/// Mixed direct/path grouping query.
pub const GROUP_PATH_MIXED_QUERY: &str = "SELECT direct_rank, profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY direct_rank, profile.rank \
ORDER BY direct_rank ASC, profile.rank ASC \
LIMIT 127";

/// Raw path HAVING over the exact declared key.
pub const GROUP_PATH_HAVING_QUERY: &str = "SELECT profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY profile.rank \
HAVING profile.rank >= 120 \
ORDER BY profile.rank ASC \
LIMIT 7";

/// Scalar expression evaluated from the exact declared path group key.
pub const GROUP_PATH_EXPRESSION_QUERY: &str = "SELECT profile.rank + 1, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY profile.rank \
ORDER BY profile.rank + 1 ASC \
LIMIT 127";

/// Path aggregate input remains row-local when a direct key owns grouping.
pub const GROUP_PATH_AGGREGATE_INPUT_QUERY: &str = "SELECT direct_rank, SUM(profile.rank) \
FROM GroupPathAuditRow \
GROUP BY direct_rank \
ORDER BY direct_rank ASC \
LIMIT 127";

/// Bounded scalar record-path page used to verify grouped cursor emission.
pub const GROUP_PATH_PAGED_QUERY: &str = "SELECT profile.rank, COUNT(*) \
FROM GroupPathAuditRow \
GROUP BY profile.rank \
ORDER BY profile.rank ASC \
LIMIT 17";

/// A sibling raw path is not admitted by grouping another member.
pub const GROUP_PATH_SIBLING_PROJECTION_QUERY: &str =
    "SELECT profile.optional_rank, COUNT(*) FROM GroupPathAuditRow GROUP BY profile.rank";

/// A record-valued terminal remains outside scalar grouping.
pub const GROUP_PATH_RECORD_TERMINAL_QUERY: &str =
    "SELECT profile, COUNT(*) FROM GroupPathAuditRow GROUP BY profile";

/// Unknown accepted members fail before route planning.
pub const GROUP_PATH_UNKNOWN_MEMBER_QUERY: &str =
    "SELECT profile.missing, COUNT(*) FROM GroupPathAuditRow GROUP BY profile.missing";

/// Complete required-path index used by the ordered comparison.
pub const GROUP_PATH_COMPLETE_INDEX_DDL: &str = "CREATE INDEX group_path_profile_rank_idx \
ON GroupPathAuditRow (profile.rank) \
EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2";

/// Mirrored direct-field index used by the ordered instruction comparison.
pub const GROUP_PATH_DIRECT_INDEX_DDL: &str = "CREATE INDEX group_path_direct_rank_idx \
ON GroupPathAuditRow (direct_rank) \
EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2";

/// Complete mixed direct/path prefix used by ordered admission tests.
pub const GROUP_PATH_MIXED_INDEX_DDL: &str = "CREATE INDEX group_path_mixed_rank_idx \
ON GroupPathAuditRow (direct_rank, profile.rank) \
EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2";

/// Omission-capable optional-path index used by completeness rejection tests.
pub const GROUP_PATH_OMISSION_INDEX_DDL: &str = "CREATE INDEX group_path_optional_rank_idx \
ON GroupPathAuditRow (profile.optional_rank) \
EXPECT SCHEMA VERSION 2 SET SCHEMA VERSION 3";

/// Composite prefix whose nullable path suffix can omit matching rows.
pub const GROUP_PATH_OMISSION_PREFIX_INDEX_DDL: &str = "CREATE INDEX group_path_optional_prefix_idx \
ON GroupPathAuditRow (direct_rank, optional_profile.rank) \
EXPECT SCHEMA VERSION 1 SET SCHEMA VERSION 2";
