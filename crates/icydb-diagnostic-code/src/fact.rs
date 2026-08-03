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

impl fmt::Debug for DiagnosticFactTag {
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
    use super::{DiagnosticFactTag, ORDERED_FACT_TAGS, pack_u32_pair, unpack_u32_pair};

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
}
