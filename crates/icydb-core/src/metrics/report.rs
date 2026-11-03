//! Service-owned instruction report shape; Metrics retains numeric authority.

use ic_metrics::MeasurementAggregate;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Read-only instruction observations at IcyDB's frontend boundary.
///
/// Canonical Metrics values own validation and arithmetic. IcyDB owns this
/// report's field order and native schema independently of Metrics metadata.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InstructionMetrics(MeasurementAggregate);

impl InstructionMetrics {
    /// Borrow the admitted canonical observation without reconstructing it.
    pub(crate) const fn measurement(&self) -> &MeasurementAggregate {
        &self.0
    }
}

impl<'de> Deserialize<'de> for InstructionMetrics {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        MeasurementAggregate::deserialize(deserializer).map(Self)
    }
}

impl From<MeasurementAggregate> for InstructionMetrics {
    fn from(value: MeasurementAggregate) -> Self {
        Self(value)
    }
}

impl Serialize for InstructionMetrics {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        AggregateFields {
            samples: self.0.samples(),
            total: self.0.total(),
            maximum: self.0.maximum(),
            samples_overflowed: self.0.samples_overflowed(),
            total_overflowed: self.0.total_overflowed(),
        }
        .serialize(serializer)
    }
}

#[cfg(not(target_family = "wasm"))]
impl ic_wire::typescript::Wire for InstructionMetrics {
    fn native_schema(
        context: &mut ic_wire::typescript::SchemaContext,
    ) -> Result<serde_json::Value, ic_wire::typescript::GenerationError> {
        context.describe::<AggregateFields>()
    }
}

// Preserve the existing native/reflection name as well as the five field bytes.
// Deserialization delegates to Metrics instead of duplicating its admission.
#[derive(Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
struct AggregateFields {
    samples: u64,
    total: u128,
    maximum: Option<u64>,
    samples_overflowed: bool,
    total_overflowed: bool,
}

#[cfg(test)]
mod tests {
    use super::InstructionMetrics;
    use ic_metrics::MeasurementAggregate;
    use icydb_schema::wire::{decode_one, encode_one};

    #[test]
    fn service_report_preserves_canonical_bytes_and_complete_numeric_admission() {
        for fields in [
            (0, 0, None, false, false),
            (1, 0, Some(0), false, false),
            (u64::MAX, 0, Some(0), false, false),
            (u64::MAX, u128::MAX, Some(u64::MAX), true, true),
        ] {
            let canonical =
                MeasurementAggregate::from_parts(fields.0, fields.1, fields.2, fields.3, fields.4)
                    .unwrap();
            let report = InstructionMetrics::from(canonical);
            let bytes = encode_one(&report).unwrap();
            assert_eq!(bytes, encode_one(fields).unwrap());
            assert_eq!(bytes, encode_one(canonical).unwrap());
            let decoded: InstructionMetrics = decode_one(&bytes).unwrap();
            assert_eq!(decoded, report);
            assert_eq!(decoded.measurement(), &canonical);
            for end in 0..bytes.len() {
                assert!(decode_one::<InstructionMetrics>(&bytes[..end]).is_err());
            }
        }
    }
}
