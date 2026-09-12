//! Canonical fixed-width subaccount atom.

use crate::Principal;
use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};
use std::fmt::{self, Display};

//
// Subaccount
//

type SubaccountBytes = [u8; 32];

#[derive(CandidType, Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
/// A canonical 32-byte ICRC account subaccount.
pub struct Subaccount(SubaccountBytes);

impl Subaccount {
    /// The lexicographically smallest subaccount.
    pub const MIN: Self = Self::from_array([0x00; 32]);
    /// The lexicographically largest subaccount.
    pub const MAX: Self = Self::from_array([0xFF; 32]);

    /// Return the fixed-width byte array.
    #[must_use]
    pub const fn to_array(&self) -> [u8; 32] {
        self.0
    }

    /// Construct from the exact fixed-width byte array.
    #[must_use]
    pub const fn from_array(array: [u8; 32]) -> Self {
        Self(array)
    }

    /// Borrow the fixed-width bytes.
    #[must_use]
    pub const fn as_slice(&self) -> &[u8] {
        &self.0
    }

    /// Consume the value and return its fixed-width bytes.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 32] {
        self.0
    }
}

impl<'de> Deserialize<'de> for Subaccount {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Candid validates nat8 elements through sequence ingress. Read into a
        // fixed array and inspect at most one excess byte before rejecting.
        struct SubaccountBytesVisitor;

        impl<'de> serde::de::Visitor<'de> for SubaccountBytesVisitor {
            type Value = Subaccount;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("exactly 32 subaccount bytes")
            }

            fn visit_seq<A: serde::de::SeqAccess<'de>>(
                self,
                mut sequence: A,
            ) -> Result<Subaccount, A::Error> {
                let mut bytes = [0; 32];
                for (index, byte) in bytes.iter_mut().enumerate() {
                    *byte = sequence
                        .next_element()?
                        .ok_or_else(|| A::Error::invalid_length(index, &self))?;
                }
                if sequence.next_element::<u8>()?.is_some() {
                    return Err(A::Error::invalid_length(33, &self));
                }
                Ok(Subaccount::from_array(bytes))
            }
        }

        if deserializer.is_human_readable() {
            return SubaccountBytes::deserialize(deserializer).map(Self);
        }

        deserializer.deserialize_seq(SubaccountBytesVisitor)
    }
}

impl Display for Subaccount {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.0 {
            write!(f, "{byte:02x}")?;
        }

        Ok(())
    }
}

// code taken from
// <https://docs.rs/ic-ledger-types/latest/src/ic_ledger_types/lib.rs.html#140-148>
#[expect(clippy::cast_possible_truncation)]
impl From<Principal> for Subaccount {
    fn from(principal: Principal) -> Self {
        let mut bytes = [0u8; 32];
        let p = principal.as_slice();

        // Defensive check: Principals are currently <= 29 bytes
        let len = p.len().min(31); // reserve 1 byte for the length prefix
        bytes[0] = len as u8;

        // Copy safely without panic risk
        bytes[1..=len].copy_from_slice(&p[..len]);

        Self(bytes)
    }
}

impl Serialize for Subaccount {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        if serializer.is_human_readable() {
            self.0.serialize(serializer)
        } else {
            serializer.serialize_bytes(&self.0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Account;

    #[test]
    fn binary_serde_and_candid_preserve_exact_subaccount_bytes() {
        for byte in [0, 23, 24, 128, 255] {
            let value = Subaccount::from_array([byte; 32]);
            let mut encoded = Vec::new();
            ciborium::into_writer(&value, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 34);
            let wire: ciborium::Value = ciborium::from_reader(encoded.as_slice()).unwrap();
            assert_eq!(wire, ciborium::Value::Bytes(value.to_bytes().to_vec()));
            assert_eq!(
                ciborium::from_reader::<Subaccount, _>(encoded.as_slice()).unwrap(),
                value
            );

            let json = serde_json::to_string(&value).unwrap();
            assert_eq!(json, serde_json::to_string(&value.to_array()).unwrap());
            assert_eq!(serde_json::from_str::<Subaccount>(&json).unwrap(), value);

            let candid = candid::encode_one(value).unwrap();
            assert_eq!(candid, candid::encode_one(value.to_array()).unwrap());
            assert_eq!(candid.len(), 42);
            assert_eq!(candid::decode_one::<Subaccount>(&candid).unwrap(), value);

            let batch = vec![value; 1_000];
            encoded.clear();
            ciborium::into_writer(&batch, &mut encoded).unwrap();
            assert_eq!(encoded.len(), 34_003);
            assert_eq!(
                ciborium::from_reader::<Vec<Subaccount>, _>(encoded.as_slice()).unwrap(),
                batch
            );
        }
    }

    #[test]
    fn binary_subaccount_ingress_enforces_width_and_byte_elements() {
        for len in [0, 31, 33, 1_024] {
            let wire = candid::encode_one(vec![0_u8; len]).unwrap();
            assert!(candid::decode_one::<Subaccount>(&wire).is_err());
            let mut encoded = Vec::new();
            ciborium::into_writer(&serde_bytes::Bytes::new(&vec![0; len]), &mut encoded).unwrap();
            assert!(ciborium::from_reader::<Subaccount, _>(encoded.as_slice()).is_err());
        }
        let wire = candid::encode_one(vec![0_u16; 32]).unwrap();
        assert!(candid::decode_one::<Subaccount>(&wire).is_err());

        let mut encoded = Vec::new();
        ciborium::into_writer(&Subaccount::MAX, &mut encoded).unwrap();
        for end in 0..encoded.len() {
            assert!(ciborium::from_reader::<Subaccount, _>(&encoded[..end]).is_err());
        }
    }

    #[test]
    fn accounts_roundtrip_optional_subaccounts_in_all_transports() {
        for subaccount in [None, Some(Subaccount::MIN), Some(Subaccount::MAX)] {
            let account = Account::from_owner_and_subaccount(Principal::anonymous(), subaccount);
            let mut encoded = Vec::new();
            ciborium::into_writer(&account, &mut encoded).unwrap();
            assert_eq!(encoded.len(), if subaccount.is_some() { 54 } else { 21 });
            assert_eq!(
                ciborium::from_reader::<Account, _>(encoded.as_slice()).unwrap(),
                account
            );
            let json = serde_json::to_string(&account).unwrap();
            assert_eq!(serde_json::from_str::<Account>(&json).unwrap(), account);
            let candid = candid::encode_one(account).unwrap();
            assert_eq!(candid, candid::encode_one(account.to_icrc_type()).unwrap());
            assert_eq!(candid::decode_one::<Account>(&candid).unwrap(), account);
        }
    }
}
