//! Module: fact
//!
//! Responsibility: production-safe numeric diagnostic-fact identities.
//! Does not own: Candid records, rich labels, prose, or subsystem projections.
//! Boundary: freezes the numeric vocabulary shared by public errors and host tooling.

use std::fmt;

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

#[cfg(test)]
mod tests {
    use super::{
        DiagnosticAggregateKind, DiagnosticComponentKind, DiagnosticConstraintContext,
        DiagnosticConstraintKind, DiagnosticDecodeReason, DiagnosticFactTag,
        DiagnosticFunctionKind, DiagnosticMutationOperation, DiagnosticOperatorKind,
        DiagnosticTypeFamily, ORDERED_FACT_TAGS, pack_u32_pair, unpack_u32_pair,
    };

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
}
