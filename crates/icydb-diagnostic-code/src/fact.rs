//! Module: fact
//!
//! Responsibility: production-safe numeric diagnostic-fact identities.
//! Does not own: Candid records, rich labels, prose, or subsystem projections.
//! Boundary: freezes the numeric vocabulary shared by public errors and host tooling.

use std::fmt;

use crate::ErrorCode;

/// Maximum number of numeric facts carried by one public error.
pub const MAX_PUBLIC_DIAGNOSTIC_FACTS: usize = 80;

macro_rules! define_fact_tag_registry {
    ($($name:ident = $raw:literal;)+) => {
        /// Stable semantic identity for one numeric public diagnostic fact.
        #[derive(Clone, Copy, Eq, Hash, PartialEq)]
        pub enum DiagnosticFactTag {
            $(
                #[doc = concat!("Public fact tag ", stringify!($raw), ".")]
                $name,
            )+
        }

        impl DiagnosticFactTag {
            /// Return the fixed public wire value.
            #[must_use]
            pub const fn raw(self) -> u8 {
                match self {
                    $(Self::$name => $raw,)+
                }
            }

            /// Recover a known tag from its public wire value.
            #[must_use]
            pub const fn known(raw: u8) -> Option<Self> {
                match raw {
                    $($raw => Some(Self::$name),)+
                    _ => None,
                }
            }
        }

        #[cfg(test)]
        const ORDERED_FACT_TAGS: &[DiagnosticFactTag] = &[
            $(DiagnosticFactTag::$name,)+
        ];
    };
}

// This table is a public numeric registry. Append only within a released
// major-version contract; do not reuse or reinterpret an assigned value.
define_fact_tag_registry! {
    AcceptedSchemaFingerprintMethod = 1;
    AcceptedSchemaFingerprintHigh = 2;
    AcceptedSchemaFingerprintLow = 3;
    ExpectedFingerprintPrefix = 4;
    ActualFingerprintPrefix = 5;
    EntityTag = 6;
    ExpectedEntityTag = 7;
    ActualEntityTag = 8;
    ConstraintId = 9;
    FieldId = 10;
    IndexId = 11;
    RelationId = 12;
    MutationOperation = 13;
    RowOperation = 14;
    BatchPosition = 15;
    FirstBatchPosition = 16;
    DuplicateBatchPosition = 17;
    ClauseIndex = 18;
    TermIndex = 19;
    FirstTermIndex = 20;
    DuplicateTermIndex = 21;
    ProjectionIndex = 22;
    GroupIndex = 23;
    AggregateIndex = 24;
    ArgumentIndex = 25;
    BranchIndex = 26;
    ComponentIndex = 27;
    ParameterIndex = 28;
    SourceSpanStart = 29;
    SourceSpanEnd = 30;
    Expected = 31;
    Actual = 32;
    Minimum = 33;
    Maximum = 34;
    Limit = 35;
    ExpectedCount = 36;
    ActualCount = 37;
    ExpectedRevision = 38;
    ActualRevision = 39;
    CurrentRevision = 40;
    RequestedRevision = 41;
    ExpectedVersion = 42;
    ActualVersion = 43;
    CurrentVersion = 44;
    RequestedVersion = 45;
    ExpectedOffset = 46;
    ActualOffset = 47;
    ExpectedArity = 48;
    ActualArity = 49;
    ExpectedLength = 50;
    ActualLength = 51;
    ExpectedSlotCount = 52;
    ActualSlotCount = 53;
    RowLayout = 54;
    HistoryFloor = 55;
    CurrentLayout = 56;
    PhysicalSlot = 57;
    PhysicalGeneration = 58;
    ExpectedMemoryId = 59;
    ActualMemoryId = 60;
    ConstraintKind = 61;
    ConstraintContext = 62;
    FieldKind = 63;
    ValueKind = 64;
    TypeFamily = 65;
    FunctionKind = 66;
    OperatorKind = 67;
    AggregateKind = 68;
    KeyNamespaceKind = 69;
    ComponentKind = 70;
    MismatchKind = 71;
    DecodeReason = 72;
    BudgetResource = 73;
    MigrationPhase = 74;
    DatabaseControlRecordKind = 75;
    StateKind = 76;
    PayloadComponent = 77;
    ExpectedSignaturePrefix = 78;
    ActualSignaturePrefix = 79;
    FindingPosition = 80;
    RootField = 81;
    RecordMember = 82;
    TupleElement = 83;
    Newtype = 84;
    EnumVariant = 85;
    ListElement = 86;
    SetElement = 87;
    MapEntryKey = 88;
    MapEntryValue = 89;
}

/// Compact reason carried by [`DiagnosticFactTag::DecodeReason`].
///
/// Values are global within that fact tag. They identify only bounded decode
/// or validation categories and never retain rejected payload bytes.
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum DiagnosticDecodeReason {
    CursorEmpty,
    CursorTooLong,
    CursorOddLength,
    CursorInvalidHex,
    CursorGroupedDirectionMismatch,
    CursorTokenEncode,
    CursorTokenDecode,
    RecoveryMarkerMagic,
    RecoveryMarkerChecksum,
    RecoveryMarkerState,
}

/// Compact operation carried by [`DiagnosticFactTag::MutationOperation`].
#[derive(Clone, Copy, Eq, Hash, PartialEq)]
pub enum DiagnosticMutationOperation {
    Insert,
    Replace,
    Update,
    Delete,
}

macro_rules! define_numeric_fact_value_registry {
    (
        $(#[$enum_meta:meta])*
        pub enum $name:ident {
            $($variant:ident = $raw:literal;)+
        }
    ) => {
        $(#[$enum_meta])*
        #[derive(Clone, Copy, Eq, Hash, PartialEq)]
        pub enum $name {
            $(
                #[doc = concat!("Compact diagnostic value ", stringify!($raw), ".")]
                $variant,
            )+
        }

        impl $name {
            /// Return the fixed numeric fact value.
            #[must_use]
            pub const fn raw(self) -> u64 {
                match self {
                    $(Self::$variant => $raw,)+
                }
            }

            /// Recover a known compact value from its public numeric identity.
            #[must_use]
            pub const fn known(raw: u64) -> Option<Self> {
                match raw {
                    $($raw => Some(Self::$variant),)+
                    _ => None,
                }
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.raw())
            }
        }
    };
}

define_numeric_fact_value_registry! {
    /// Compact accepted constraint family carried by [`DiagnosticFactTag::ConstraintKind`].
    pub enum DiagnosticConstraintKind {
        Check = 1;
        NotNull = 2;
        Relation = 3;
        TargetedRule = 4;
        Unique = 5;
    }
}

define_numeric_fact_value_registry! {
    /// Compact enforcement boundary carried by [`DiagnosticFactTag::ConstraintContext`].
    pub enum DiagnosticConstraintContext {
        Integrity = 1;
        MigrationValidation = 2;
        WriteAdmission = 3;
    }
}

define_numeric_fact_value_registry! {
    /// Compact storage component carried by [`DiagnosticFactTag::ComponentKind`].
    pub enum DiagnosticComponentKind {
        CommitDataKey = 1;
        IndexKey = 2;
        IndexKeyComponent = 3;
        RelationTargetPrimaryKey = 4;
    }
}

define_numeric_fact_value_registry! {
    /// Compact semantic family carried by [`DiagnosticFactTag::TypeFamily`].
    pub enum DiagnosticTypeFamily {
        Blob = 1;
        Bool = 2;
        Collection = 3;
        Null = 4;
        Numeric = 5;
        Opaque = 6;
        Structured = 7;
        Text = 8;
        Unknown = 9;
    }
}

define_numeric_fact_value_registry! {
    /// Compact function identity carried by [`DiagnosticFactTag::FunctionKind`].
    pub enum DiagnosticFunctionKind {
        Abs = 1;
        Cbrt = 2;
        Ceiling = 3;
        Coalesce = 4;
        CollectionContains = 5;
        Contains = 6;
        EndsWith = 7;
        Exp = 8;
        Floor = 9;
        IsEmpty = 10;
        IsMissing = 11;
        IsNotEmpty = 12;
        IsNotNull = 13;
        IsNull = 14;
        Left = 15;
        Length = 16;
        Ln = 17;
        Log = 18;
        Log2 = 19;
        Log10 = 20;
        Lower = 21;
        Ltrim = 22;
        Mod = 23;
        NullIf = 24;
        OctetLength = 25;
        Position = 26;
        Power = 27;
        Replace = 28;
        Right = 29;
        Round = 30;
        Rtrim = 31;
        Sign = 32;
        Sqrt = 33;
        StartsWith = 34;
        Substring = 35;
        Trim = 36;
        Trunc = 37;
        Upper = 38;
        InList = 39;
    }
}

define_numeric_fact_value_registry! {
    /// Compact operator identity carried by [`DiagnosticFactTag::OperatorKind`].
    pub enum DiagnosticOperatorKind {
        Not = 1;
        Add = 2;
        And = 3;
        Div = 4;
        Eq = 5;
        Gt = 6;
        Gte = 7;
        Lt = 8;
        Lte = 9;
        Mul = 10;
        Ne = 11;
        Or = 12;
        Sub = 13;
        In = 14;
        NotIn = 15;
        Contains = 16;
        StartsWith = 17;
        EndsWith = 18;
    }
}

define_numeric_fact_value_registry! {
    /// Compact aggregate identity carried by [`DiagnosticFactTag::AggregateKind`].
    pub enum DiagnosticAggregateKind {
        Count = 1;
        Sum = 2;
        Avg = 3;
        Exists = 4;
        Min = 5;
        Max = 6;
        First = 7;
        Last = 8;
    }
}

impl DiagnosticDecodeReason {
    /// Return the fixed numeric fact value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        match self {
            Self::CursorEmpty => 1,
            Self::CursorTooLong => 2,
            Self::CursorOddLength => 3,
            Self::CursorInvalidHex => 4,
            Self::CursorGroupedDirectionMismatch => 5,
            Self::CursorTokenEncode => 6,
            Self::CursorTokenDecode => 7,
            Self::RecoveryMarkerMagic => 8,
            Self::RecoveryMarkerChecksum => 9,
            Self::RecoveryMarkerState => 10,
        }
    }

    /// Recover a known compact decode reason.
    #[must_use]
    pub const fn known(raw: u64) -> Option<Self> {
        match raw {
            1 => Some(Self::CursorEmpty),
            2 => Some(Self::CursorTooLong),
            3 => Some(Self::CursorOddLength),
            4 => Some(Self::CursorInvalidHex),
            5 => Some(Self::CursorGroupedDirectionMismatch),
            6 => Some(Self::CursorTokenEncode),
            7 => Some(Self::CursorTokenDecode),
            8 => Some(Self::RecoveryMarkerMagic),
            9 => Some(Self::RecoveryMarkerChecksum),
            10 => Some(Self::RecoveryMarkerState),
            _ => None,
        }
    }
}

impl DiagnosticMutationOperation {
    /// Return the fixed numeric fact value.
    #[must_use]
    pub const fn raw(self) -> u64 {
        match self {
            Self::Insert => 1,
            Self::Replace => 2,
            Self::Update => 3,
            Self::Delete => 4,
        }
    }

    /// Recover a known compact mutation operation.
    #[must_use]
    pub const fn known(raw: u64) -> Option<Self> {
        match raw {
            1 => Some(Self::Insert),
            2 => Some(Self::Replace),
            3 => Some(Self::Update),
            4 => Some(Self::Delete),
            _ => None,
        }
    }
}

impl fmt::Debug for DiagnosticFactTag {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw())
    }
}

impl fmt::Debug for DiagnosticDecodeReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw())
    }
}

impl fmt::Debug for DiagnosticMutationOperation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw())
    }
}

/// Pack two accepted `u32` identities into one fact value without narrowing.
#[must_use]
pub const fn pack_u32_pair(high: u32, low: u32) -> u64 {
    (high as u64) << 32 | low as u64
}

/// Recover the two accepted identities from one packed fact value.
#[must_use]
pub const fn unpack_u32_pair(value: u64) -> (u32, u32) {
    let bytes = value.to_be_bytes();
    (
        u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]),
        u32::from_be_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]),
    )
}

/// Why one numeric fact sequence does not satisfy its owning E-code schema.
///
/// This taxonomy intentionally carries no prose. Host tooling owns rendering.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DiagnosticFactSchemaMismatch {
    /// The sequence exceeds the global public fact ceiling.
    GlobalMaximumExceeded,
    /// The sequence exceeds the tighter ceiling for its E-code.
    CodeMaximumExceeded,
    /// Required, allowed, repeated, or ordered tags do not match the E-code.
    InvalidSequence,
    /// One known compact tag carries a value outside its frozen numeric registry.
    InvalidValue,
}

/// Validate facts already expressed with known production tag identities.
///
/// The validator allocates nothing. It is the runtime-side entry point used
/// immediately before facts cross the public facade.
pub fn validate_known_diagnostic_fact_schema(
    code: ErrorCode,
    facts: &[(DiagnosticFactTag, u64)],
) -> Result<(), DiagnosticFactSchemaMismatch> {
    validate_diagnostic_fact_schema(code, facts.len(), |index| {
        let (tag, value) = facts[index];
        (tag.raw(), value)
    })
}

/// Validate raw host/tooling facts against their owning E-code schema.
///
/// Unknown tags remain renderable by callers, but make a known E-code context
/// invalid instead of being heuristically reinterpreted.
pub fn validate_raw_diagnostic_fact_schema(
    code: ErrorCode,
    facts: &[(u8, u64)],
) -> Result<(), DiagnosticFactSchemaMismatch> {
    validate_diagnostic_fact_schema(code, facts.len(), |index| facts[index])
}

#[expect(
    clippy::too_many_lines,
    reason = "the frozen E-code schema registry keeps every numeric owner visible in one exhaustive dispatch"
)]
fn validate_diagnostic_fact_schema(
    code: ErrorCode,
    fact_count: usize,
    fact_at: impl Fn(usize) -> (u8, u64),
) -> Result<(), DiagnosticFactSchemaMismatch> {
    if fact_count > MAX_PUBLIC_DIAGNOSTIC_FACTS {
        return Err(DiagnosticFactSchemaMismatch::GlobalMaximumExceeded);
    }

    let maximum = diagnostic_fact_maximum(code);
    if fact_count > maximum {
        return Err(DiagnosticFactSchemaMismatch::CodeMaximumExceeded);
    }

    let valid_sequence = match code.raw() {
        3 => query_plan_schema(fact_count, &fact_at),
        6 => cursor_schema(fact_count, &fact_at),
        16 => store_corruption_schema(fact_count, &fact_at),
        18 => runtime_corruption_schema(fact_count, &fact_at),
        19 => incompatible_format_schema(fact_count, &fact_at),
        20 => runtime_invariant_schema(fact_count, &fact_at),
        21 => runtime_conflict_schema(fact_count, &fact_at),
        23 => runtime_unsupported_schema(fact_count, &fact_at),
        24 => runtime_internal_schema(fact_count, &fact_at),
        138 => tags_match(
            fact_count,
            &fact_at,
            &[
                DiagnosticFactTag::ExpectedArity,
                DiagnosticFactTag::ActualArity,
            ],
        ),
        141 | 142 | 169 | 170 => {
            tags_match(fact_count, &fact_at, &[DiagnosticFactTag::ProjectionIndex])
        }
        175 => tags_match(fact_count, &fact_at, &[DiagnosticFactTag::ParameterIndex]),
        177 | 237 => {
            tags_match(fact_count, &fact_at, &[DiagnosticFactTag::Limit])
                || tags_match(
                    fact_count,
                    &fact_at,
                    &[DiagnosticFactTag::ActualLength, DiagnosticFactTag::Limit],
                )
        }
        178 | 180 | 202 | 203 | 205 | 236 => tags_match(
            fact_count,
            &fact_at,
            &[DiagnosticFactTag::ActualCount, DiagnosticFactTag::Limit],
        ),
        196 | 234 => tags_match(
            fact_count,
            &fact_at,
            &[
                DiagnosticFactTag::EntityTag,
                DiagnosticFactTag::FieldId,
                DiagnosticFactTag::MutationOperation,
                DiagnosticFactTag::BatchPosition,
            ],
        ),
        197 => tags_match(
            fact_count,
            &fact_at,
            &[
                DiagnosticFactTag::RowLayout,
                DiagnosticFactTag::HistoryFloor,
                DiagnosticFactTag::CurrentLayout,
            ],
        ),
        198 => tags_match(
            fact_count,
            &fact_at,
            &[
                DiagnosticFactTag::RowLayout,
                DiagnosticFactTag::ExpectedSlotCount,
                DiagnosticFactTag::ActualSlotCount,
            ],
        ),
        201 => tags_match(
            fact_count,
            &fact_at,
            &[DiagnosticFactTag::ActualCount, DiagnosticFactTag::Minimum],
        ),
        223 => constraint_schema(fact_count, &fact_at, true),
        225 => constraint_schema(fact_count, &fact_at, false),
        233 => tags_match(
            fact_count,
            &fact_at,
            &[
                DiagnosticFactTag::EntityTag,
                DiagnosticFactTag::MutationOperation,
                DiagnosticFactTag::BatchPosition,
            ],
        ),
        235 => {
            tags_match(fact_count, &fact_at, &[DiagnosticFactTag::ActualCount]) && fact_at(0).1 == 0
        }
        238 => tags_match(
            fact_count,
            &fact_at,
            &[DiagnosticFactTag::ActualLength, DiagnosticFactTag::Limit],
        ),
        239 => tags_match(
            fact_count,
            &fact_at,
            &[
                DiagnosticFactTag::BatchPosition,
                DiagnosticFactTag::ExpectedEntityTag,
                DiagnosticFactTag::ActualEntityTag,
            ],
        ),
        240 => tags_match(
            fact_count,
            &fact_at,
            &[
                DiagnosticFactTag::EntityTag,
                DiagnosticFactTag::FirstBatchPosition,
                DiagnosticFactTag::DuplicateBatchPosition,
            ],
        ),
        _ => fact_count == 0,
    };
    if !valid_sequence {
        return Err(DiagnosticFactSchemaMismatch::InvalidSequence);
    }

    for index in 0..fact_count {
        let (raw_tag, value) = fact_at(index);
        let Some(tag) = DiagnosticFactTag::known(raw_tag) else {
            return Err(DiagnosticFactSchemaMismatch::InvalidSequence);
        };
        if !diagnostic_fact_value_is_valid(tag, value) {
            return Err(DiagnosticFactSchemaMismatch::InvalidValue);
        }
    }
    Ok(())
}

const fn diagnostic_fact_maximum(code: ErrorCode) -> usize {
    match code.raw() {
        6 | 16 | 18 | 24 | 197 | 198 | 233 | 239 | 240 => 3,
        141 | 142 | 169 | 170 | 175 | 235 => 1,
        19 | 21 | 138 | 177 | 178 | 180 | 201 | 202 | 203 | 205 | 236 | 237 | 238 => 2,
        3 | 20 => 5,
        23 => 6,
        196 | 234 => 4,
        223 => 73,
        225 => 9,
        _ => 0,
    }
}

#[expect(
    clippy::too_many_lines,
    reason = "the query-plan E-code deliberately owns several exact finite fact sequences"
)]
fn query_plan_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    fact_count == 0
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::TermIndex])
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::ComponentIndex])
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::GroupIndex])
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::ClauseIndex])
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::AggregateIndex])
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::AggregateKind])
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::ProjectionIndex])
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::FirstTermIndex,
                DiagnosticFactTag::DuplicateTermIndex,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ClauseIndex,
                DiagnosticFactTag::OperatorKind,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::AggregateIndex,
                DiagnosticFactTag::AggregateKind,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::AggregateKind,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::OperatorKind,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::BranchIndex,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[DiagnosticFactTag::TypeFamily, DiagnosticFactTag::TypeFamily],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ClauseIndex,
                DiagnosticFactTag::AggregateIndex,
                DiagnosticFactTag::ActualCount,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::FunctionKind,
                DiagnosticFactTag::ExpectedArity,
                DiagnosticFactTag::ActualArity,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::FunctionKind,
                DiagnosticFactTag::ArgumentIndex,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::OperatorKind,
                DiagnosticFactTag::TypeFamily,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::BranchIndex,
                DiagnosticFactTag::TypeFamily,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::TypeFamily,
                DiagnosticFactTag::BranchIndex,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::BranchIndex,
                DiagnosticFactTag::TypeFamily,
                DiagnosticFactTag::BranchIndex,
                DiagnosticFactTag::TypeFamily,
            ],
        )
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::FunctionKind,
                DiagnosticFactTag::ArgumentIndex,
                DiagnosticFactTag::TypeFamily,
                DiagnosticFactTag::ArgumentIndex,
                DiagnosticFactTag::TypeFamily,
            ],
        )
}

fn cursor_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    if fact_count == 0 {
        return true;
    }
    if tags_match(fact_count, fact_at, &[DiagnosticFactTag::DecodeReason]) {
        return matches!(fact_at(0).1, 1 | 3 | 5 | 6 | 7);
    }
    if tags_match(
        fact_count,
        fact_at,
        &[
            DiagnosticFactTag::ActualLength,
            DiagnosticFactTag::Maximum,
            DiagnosticFactTag::DecodeReason,
        ],
    ) {
        return fact_at(2).1 == DiagnosticDecodeReason::CursorTooLong.raw();
    }
    if tags_match(
        fact_count,
        fact_at,
        &[
            DiagnosticFactTag::ComponentIndex,
            DiagnosticFactTag::DecodeReason,
        ],
    ) {
        return matches!(fact_at(1).1, 4 | 6 | 7);
    }
    tags_match(
        fact_count,
        fact_at,
        &[
            DiagnosticFactTag::ExpectedSignaturePrefix,
            DiagnosticFactTag::ActualSignaturePrefix,
        ],
    ) || tags_match(
        fact_count,
        fact_at,
        &[
            DiagnosticFactTag::ExpectedOffset,
            DiagnosticFactTag::ActualOffset,
        ],
    )
}

fn store_corruption_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    fact_count == 0
        || (tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ComponentKind,
                DiagnosticFactTag::ActualLength,
                DiagnosticFactTag::Limit,
            ],
        ) && fact_at(0).1 == DiagnosticComponentKind::CommitDataKey.raw())
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ExpectedEntityTag,
                DiagnosticFactTag::ActualEntityTag,
            ],
        )
}

fn runtime_corruption_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    store_corruption_schema(fact_count, fact_at)
        || (tags_match(fact_count, fact_at, &[DiagnosticFactTag::DecodeReason])
            && matches!(fact_at(0).1, 8..=10))
}

fn incompatible_format_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    fact_count == 0
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::ExpectedVersion])
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ExpectedVersion,
                DiagnosticFactTag::ActualVersion,
            ],
        )
}

fn runtime_invariant_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    fact_count == 0
        || (tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::EntityTag,
                DiagnosticFactTag::PhysicalGeneration,
                DiagnosticFactTag::ComponentKind,
                DiagnosticFactTag::ActualArity,
                DiagnosticFactTag::Maximum,
            ],
        ) && fact_at(2).1 == DiagnosticComponentKind::IndexKey.raw())
}

fn runtime_conflict_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    fact_count == 0
        || tags_match(fact_count, fact_at, &[DiagnosticFactTag::ExpectedRevision])
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ExpectedRevision,
                DiagnosticFactTag::CurrentRevision,
            ],
        )
}

fn runtime_unsupported_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    fact_count == 0
        || (tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::EntityTag,
                DiagnosticFactTag::PhysicalGeneration,
                DiagnosticFactTag::ComponentIndex,
                DiagnosticFactTag::ComponentKind,
                DiagnosticFactTag::ActualLength,
                DiagnosticFactTag::Limit,
            ],
        ) && fact_at(3).1 == DiagnosticComponentKind::IndexKeyComponent.raw())
}

fn runtime_internal_schema(fact_count: usize, fact_at: &impl Fn(usize) -> (u8, u64)) -> bool {
    fact_count == 0
        || tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ExpectedMemoryId,
                DiagnosticFactTag::ActualMemoryId,
            ],
        )
        || (tags_match(
            fact_count,
            fact_at,
            &[
                DiagnosticFactTag::ComponentKind,
                DiagnosticFactTag::ExpectedArity,
                DiagnosticFactTag::ActualArity,
            ],
        ) && fact_at(0).1 == DiagnosticComponentKind::RelationTargetPrimaryKey.raw())
}

fn constraint_schema(
    fact_count: usize,
    fact_at: &impl Fn(usize) -> (u8, u64),
    allow_targeted_path: bool,
) -> bool {
    const COMMON: &[DiagnosticFactTag] = &[
        DiagnosticFactTag::AcceptedSchemaFingerprintMethod,
        DiagnosticFactTag::AcceptedSchemaFingerprintHigh,
        DiagnosticFactTag::AcceptedSchemaFingerprintLow,
        DiagnosticFactTag::EntityTag,
        DiagnosticFactTag::ConstraintId,
        DiagnosticFactTag::ConstraintKind,
        DiagnosticFactTag::ConstraintContext,
    ];
    if fact_count < COMMON.len()
        || !tags_prefix_matches(fact_count, fact_at, COMMON)
        || fact_at(6).1 != DiagnosticConstraintContext::WriteAdmission.raw()
    {
        return false;
    }

    let constraint_kind = fact_at(5).1;
    if DiagnosticConstraintKind::known(constraint_kind).is_none() {
        return false;
    }
    let mut index = COMMON.len();
    if index < fact_count && fact_at(index).0 == DiagnosticFactTag::MutationOperation.raw() {
        index += 1;
        if index < fact_count && fact_at(index).0 == DiagnosticFactTag::BatchPosition.raw() {
            index += 1;
        }
    }

    let path_len = fact_count - index;
    if path_len == 0 {
        return !allow_targeted_path
            || constraint_kind != DiagnosticConstraintKind::TargetedRule.raw();
    }
    allow_targeted_path
        && constraint_kind == DiagnosticConstraintKind::TargetedRule.raw()
        && path_len <= 64
        && (index..fact_count).all(|position| {
            DiagnosticFactTag::known(fact_at(position).0).is_some_and(is_value_path_tag)
        })
}

fn tags_match(
    fact_count: usize,
    fact_at: &impl Fn(usize) -> (u8, u64),
    expected: &[DiagnosticFactTag],
) -> bool {
    fact_count == expected.len() && tags_prefix_matches(fact_count, fact_at, expected)
}

fn tags_prefix_matches(
    fact_count: usize,
    fact_at: &impl Fn(usize) -> (u8, u64),
    expected: &[DiagnosticFactTag],
) -> bool {
    fact_count >= expected.len()
        && expected
            .iter()
            .enumerate()
            .all(|(index, tag)| fact_at(index).0 == tag.raw())
}

const fn is_value_path_tag(tag: DiagnosticFactTag) -> bool {
    matches!(
        tag,
        DiagnosticFactTag::RootField
            | DiagnosticFactTag::RecordMember
            | DiagnosticFactTag::TupleElement
            | DiagnosticFactTag::Newtype
            | DiagnosticFactTag::EnumVariant
            | DiagnosticFactTag::ListElement
            | DiagnosticFactTag::SetElement
            | DiagnosticFactTag::MapEntryKey
            | DiagnosticFactTag::MapEntryValue
    )
}

const fn diagnostic_fact_value_is_valid(tag: DiagnosticFactTag, value: u64) -> bool {
    match tag {
        DiagnosticFactTag::AcceptedSchemaFingerprintMethod
        | DiagnosticFactTag::ExpectedMemoryId
        | DiagnosticFactTag::ActualMemoryId => value <= u8::MAX as u64,
        DiagnosticFactTag::ConstraintId
        | DiagnosticFactTag::FieldId
        | DiagnosticFactTag::IndexId
        | DiagnosticFactTag::RelationId
        | DiagnosticFactTag::BatchPosition
        | DiagnosticFactTag::FirstBatchPosition
        | DiagnosticFactTag::DuplicateBatchPosition
        | DiagnosticFactTag::RowLayout
        | DiagnosticFactTag::HistoryFloor
        | DiagnosticFactTag::CurrentLayout
        | DiagnosticFactTag::RootField
        | DiagnosticFactTag::Newtype
        | DiagnosticFactTag::ListElement
        | DiagnosticFactTag::SetElement
        | DiagnosticFactTag::MapEntryKey
        | DiagnosticFactTag::MapEntryValue => value <= u32::MAX as u64,
        DiagnosticFactTag::ConstraintKind => DiagnosticConstraintKind::known(value).is_some(),
        DiagnosticFactTag::ConstraintContext => DiagnosticConstraintContext::known(value).is_some(),
        DiagnosticFactTag::TypeFamily => DiagnosticTypeFamily::known(value).is_some(),
        DiagnosticFactTag::FunctionKind => DiagnosticFunctionKind::known(value).is_some(),
        DiagnosticFactTag::OperatorKind => DiagnosticOperatorKind::known(value).is_some(),
        DiagnosticFactTag::AggregateKind => DiagnosticAggregateKind::known(value).is_some(),
        DiagnosticFactTag::ComponentKind => DiagnosticComponentKind::known(value).is_some(),
        DiagnosticFactTag::DecodeReason => DiagnosticDecodeReason::known(value).is_some(),
        DiagnosticFactTag::MutationOperation => DiagnosticMutationOperation::known(value).is_some(),
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        DiagnosticAggregateKind, DiagnosticComponentKind, DiagnosticConstraintContext,
        DiagnosticConstraintKind, DiagnosticDecodeReason, DiagnosticFactSchemaMismatch,
        DiagnosticFactTag, DiagnosticFunctionKind, DiagnosticMutationOperation,
        DiagnosticOperatorKind, DiagnosticTypeFamily, ORDERED_FACT_TAGS, pack_u32_pair,
        unpack_u32_pair, validate_known_diagnostic_fact_schema,
        validate_raw_diagnostic_fact_schema,
    };
    use crate::ErrorCode;

    #[test]
    fn fact_tag_registry_is_fixed_unique_and_contiguous() {
        for (index, tag) in ORDERED_FACT_TAGS.iter().copied().enumerate() {
            let expected = u8::try_from(index + 1).expect("fact-tag index fits u8");
            assert_eq!(tag.raw(), expected);
            assert_eq!(DiagnosticFactTag::known(expected), Some(tag));
        }

        assert_eq!(DiagnosticFactTag::known(0), None);
        assert_eq!(DiagnosticFactTag::known(90), None);
        assert_eq!(DiagnosticFactTag::known(u8::MAX), None);
    }

    #[test]
    fn accepted_identity_pair_packing_is_exact() {
        for pair in [
            (0, 0),
            (1, 2),
            (u32::MAX, 0),
            (0, u32::MAX),
            (u32::MAX, u32::MAX),
        ] {
            assert_eq!(unpack_u32_pair(pack_u32_pair(pair.0, pair.1)), pair);
        }
    }

    #[test]
    fn constraint_fact_value_registries_are_fixed() {
        assert_eq!(DiagnosticConstraintKind::Check.raw(), 1);
        assert_eq!(DiagnosticConstraintKind::NotNull.raw(), 2);
        assert_eq!(DiagnosticConstraintKind::Relation.raw(), 3);
        assert_eq!(DiagnosticConstraintKind::TargetedRule.raw(), 4);
        assert_eq!(DiagnosticConstraintKind::Unique.raw(), 5);
        assert_eq!(DiagnosticConstraintKind::known(0), None);
        assert_eq!(DiagnosticConstraintKind::known(6), None);

        assert_eq!(DiagnosticConstraintContext::Integrity.raw(), 1);
        assert_eq!(DiagnosticConstraintContext::MigrationValidation.raw(), 2);
        assert_eq!(DiagnosticConstraintContext::WriteAdmission.raw(), 3);
        assert_eq!(DiagnosticConstraintContext::known(0), None);
        assert_eq!(DiagnosticConstraintContext::known(4), None);
    }

    #[test]
    fn component_kind_registry_is_fixed_and_numeric() {
        let kinds = [
            DiagnosticComponentKind::CommitDataKey,
            DiagnosticComponentKind::IndexKey,
            DiagnosticComponentKind::IndexKeyComponent,
            DiagnosticComponentKind::RelationTargetPrimaryKey,
        ];

        for (index, kind) in kinds.iter().copied().enumerate() {
            let expected = (index + 1) as u64;
            assert_eq!(kind.raw(), expected);
            assert_eq!(DiagnosticComponentKind::known(expected), Some(kind));
            assert_eq!(format!("{kind:?}"), expected.to_string());
        }
        assert_eq!(DiagnosticComponentKind::known(0), None);
        assert_eq!(DiagnosticComponentKind::known(5), None);
    }

    #[test]
    fn decode_reason_registry_is_fixed_and_numeric() {
        let reasons = [
            DiagnosticDecodeReason::CursorEmpty,
            DiagnosticDecodeReason::CursorTooLong,
            DiagnosticDecodeReason::CursorOddLength,
            DiagnosticDecodeReason::CursorInvalidHex,
            DiagnosticDecodeReason::CursorGroupedDirectionMismatch,
            DiagnosticDecodeReason::CursorTokenEncode,
            DiagnosticDecodeReason::CursorTokenDecode,
            DiagnosticDecodeReason::RecoveryMarkerMagic,
            DiagnosticDecodeReason::RecoveryMarkerChecksum,
            DiagnosticDecodeReason::RecoveryMarkerState,
        ];

        for (index, reason) in reasons.iter().copied().enumerate() {
            let expected = (index + 1) as u64;
            assert_eq!(reason.raw(), expected);
            assert_eq!(DiagnosticDecodeReason::known(expected), Some(reason));
            assert_eq!(format!("{reason:?}"), expected.to_string());
        }

        assert_eq!(DiagnosticDecodeReason::known(0), None);
        assert_eq!(DiagnosticDecodeReason::known(11), None);
    }

    #[test]
    fn mutation_operation_registry_is_fixed_and_numeric() {
        let operations = [
            DiagnosticMutationOperation::Insert,
            DiagnosticMutationOperation::Replace,
            DiagnosticMutationOperation::Update,
            DiagnosticMutationOperation::Delete,
        ];

        for (index, operation) in operations.iter().copied().enumerate() {
            let expected = (index + 1) as u64;
            assert_eq!(operation.raw(), expected);
            assert_eq!(
                DiagnosticMutationOperation::known(expected),
                Some(operation)
            );
            assert_eq!(format!("{operation:?}"), expected.to_string());
        }

        assert_eq!(DiagnosticMutationOperation::known(0), None);
        assert_eq!(DiagnosticMutationOperation::known(5), None);
    }

    #[test]
    fn query_kind_registries_are_fixed_contiguous_and_numeric() {
        for raw in 1..=9 {
            let value = DiagnosticTypeFamily::known(raw).expect("type family should be known");
            assert_eq!(value.raw(), raw);
            assert_eq!(format!("{value:?}"), raw.to_string());
        }
        assert_eq!(DiagnosticTypeFamily::known(0), None);
        assert_eq!(DiagnosticTypeFamily::known(10), None);

        for raw in 1..=39 {
            let value = DiagnosticFunctionKind::known(raw).expect("function kind should be known");
            assert_eq!(value.raw(), raw);
            assert_eq!(format!("{value:?}"), raw.to_string());
        }
        assert_eq!(DiagnosticFunctionKind::known(0), None);
        assert_eq!(DiagnosticFunctionKind::known(40), None);

        for raw in 1..=18 {
            let value = DiagnosticOperatorKind::known(raw).expect("operator kind should be known");
            assert_eq!(value.raw(), raw);
            assert_eq!(format!("{value:?}"), raw.to_string());
        }
        assert_eq!(DiagnosticOperatorKind::known(0), None);
        assert_eq!(DiagnosticOperatorKind::known(19), None);

        for raw in 1..=8 {
            let value =
                DiagnosticAggregateKind::known(raw).expect("aggregate kind should be known");
            assert_eq!(value.raw(), raw);
            assert_eq!(format!("{value:?}"), raw.to_string());
        }
        assert_eq!(DiagnosticAggregateKind::known(0), None);
        assert_eq!(DiagnosticAggregateKind::known(9), None);
    }

    #[test]
    fn per_code_schema_rejects_missing_disallowed_and_noncanonical_tags() {
        let valid = [
            (DiagnosticFactTag::ActualCount, 5),
            (DiagnosticFactTag::Limit, 4),
        ];
        assert_eq!(
            validate_known_diagnostic_fact_schema(
                ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS,
                &valid,
            ),
            Ok(())
        );
        assert_eq!(
            validate_known_diagnostic_fact_schema(
                ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS,
                &valid[..1],
            ),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
        assert_eq!(
            validate_known_diagnostic_fact_schema(
                ErrorCode::RUNTIME_BOUNDARY_MUTATION_BATCH_TOO_MANY_ITEMS,
                &[valid[1], valid[0]],
            ),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
        assert_eq!(
            validate_known_diagnostic_fact_schema(ErrorCode::QUERY_VALIDATE, &valid),
            Err(DiagnosticFactSchemaMismatch::CodeMaximumExceeded)
        );
    }

    #[test]
    fn raw_schema_keeps_unknown_context_numeric_but_marks_it_invalid() {
        assert_eq!(
            validate_raw_diagnostic_fact_schema(
                ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
                &[(u8::MAX, 17)],
            ),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
        assert_eq!(
            validate_raw_diagnostic_fact_schema(
                ErrorCode::QUERY_INVALID_CONTINUATION_CURSOR,
                &[(DiagnosticFactTag::DecodeReason.raw(), u64::MAX)],
            ),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
    }

    #[test]
    fn constraint_schema_enforces_authority_operation_and_bounded_path_suffix() {
        let mut targeted = vec![
            (DiagnosticFactTag::AcceptedSchemaFingerprintMethod, 1),
            (DiagnosticFactTag::AcceptedSchemaFingerprintHigh, 2),
            (DiagnosticFactTag::AcceptedSchemaFingerprintLow, 3),
            (DiagnosticFactTag::EntityTag, 17),
            (DiagnosticFactTag::ConstraintId, 4),
            (
                DiagnosticFactTag::ConstraintKind,
                DiagnosticConstraintKind::TargetedRule.raw(),
            ),
            (
                DiagnosticFactTag::ConstraintContext,
                DiagnosticConstraintContext::WriteAdmission.raw(),
            ),
            (
                DiagnosticFactTag::MutationOperation,
                DiagnosticMutationOperation::Insert.raw(),
            ),
            (DiagnosticFactTag::BatchPosition, 0),
        ];
        targeted.extend((0..64).map(|index| (DiagnosticFactTag::ListElement, index)));
        assert_eq!(targeted.len(), 73);
        assert_eq!(
            validate_known_diagnostic_fact_schema(
                ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
                targeted.as_slice(),
            ),
            Ok(())
        );

        let mut overlong = targeted.clone();
        overlong.push((DiagnosticFactTag::ListElement, 64));
        assert_eq!(
            validate_known_diagnostic_fact_schema(
                ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
                overlong.as_slice(),
            ),
            Err(DiagnosticFactSchemaMismatch::CodeMaximumExceeded)
        );

        let mut non_targeted_path = targeted;
        non_targeted_path[5].1 = DiagnosticConstraintKind::Unique.raw();
        assert_eq!(
            validate_known_diagnostic_fact_schema(
                ErrorCode::RUNTIME_BOUNDARY_CONSTRAINT_VIOLATION,
                non_targeted_path.as_slice(),
            ),
            Err(DiagnosticFactSchemaMismatch::InvalidSequence)
        );
    }

    #[test]
    fn schema_enforces_global_ceiling_before_per_code_ceiling() {
        let facts = vec![(DiagnosticFactTag::ActualCount.raw(), 0); 81];
        assert_eq!(
            validate_raw_diagnostic_fact_schema(ErrorCode::QUERY_PLAN, facts.as_slice()),
            Err(DiagnosticFactSchemaMismatch::GlobalMaximumExceeded)
        );
    }
}
