//! IcyDB-owned report admission; recorders remain internal instrumentation.

use ic_metrics::MeasurementAggregate;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// Read-only instruction observations at IcyDB's frontend boundary.
///
/// IcyDB admits this report's numeric consistency and owns its transport fields.
/// Canonical Metrics recorders own instrumentation, never report decoding.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InstructionMetrics(AggregateFields);

impl InstructionMetrics {
    /// Recorded sample count; inspect its overflow flag before comparisons.
    #[must_use]
    pub const fn samples(&self) -> u64 {
        self.0.samples
    }

    /// Recorded instruction total; inspect its overflow flag before comparisons.
    #[must_use]
    pub const fn total(&self) -> u128 {
        self.0.total
    }

    /// Largest individual observation, absent only for an empty report.
    #[must_use]
    pub const fn maximum(&self) -> Option<u64> {
        self.0.maximum
    }

    /// Whether the producer actually overflowed its sample count.
    #[must_use]
    pub const fn samples_overflowed(&self) -> bool {
        self.0.samples_overflowed
    }

    /// Whether the producer actually overflowed its instruction total.
    #[must_use]
    pub const fn total_overflowed(&self) -> bool {
        self.0.total_overflowed
    }
}

impl<'de> Deserialize<'de> for InstructionMetrics {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let fields = AggregateFields::deserialize(deserializer)?;
        if !fields.valid() {
            return Err(serde::de::Error::custom("invalid IcyDB instruction report"));
        }
        Ok(Self(fields))
    }
}

impl From<MeasurementAggregate> for InstructionMetrics {
    fn from(value: MeasurementAggregate) -> Self {
        Self(AggregateFields {
            samples: value.samples(),
            total: value.total(),
            maximum: value.maximum(),
            samples_overflowed: value.samples_overflowed(),
            total_overflowed: value.total_overflowed(),
        })
    }
}

impl Serialize for InstructionMetrics {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.serialize(serializer)
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

// Preserve the existing reflection name, field order and bytes. These are
// report facts, not a mutable recorder or a governed persistence adapter.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[cfg_attr(not(target_family = "wasm"), derive(ic_wire::Wire))]
struct AggregateFields {
    samples: u64,
    total: u128,
    maximum: Option<u64>,
    samples_overflowed: bool,
    total_overflowed: bool,
}

impl AggregateFields {
    // u64 count times u64 maximum fits in u128. Exact caps remain distinct from
    // actual overflow; a clipped count cannot establish an upper total bound.
    fn valid(&self) -> bool {
        if self.samples == 0 {
            return self.total == 0
                && self.maximum.is_none()
                && !self.samples_overflowed
                && !self.total_overflowed;
        }
        let Some(maximum) = self.maximum else {
            return false;
        };
        if (self.samples_overflowed && self.samples != u64::MAX)
            || (self.total_overflowed && (self.total != u128::MAX || !self.samples_overflowed))
        {
            return false;
        }
        self.total >= u128::from(maximum)
            && (maximum != 0 || self.total == 0)
            && (self.samples_overflowed
                || self.total <= u128::from(self.samples) * u128::from(maximum))
    }
}

#[cfg(test)]
mod tests;
