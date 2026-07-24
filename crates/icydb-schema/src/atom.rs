//! Engine-neutral scalar atoms used by proposal literals and facade re-exports.

use std::{
    cmp::Ordering,
    fmt::{self, Display, Formatter},
    hash::{Hash, Hasher},
    str::FromStr,
};

use candid::CandidType;
use serde::{Deserialize, Deserializer, Serialize, de::Error as DeError};

use crate::{MAX_PROPOSAL_LITERAL_BYTES, SchemaContractError};

/// Failure while parsing a canonical principal.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalError {
    /// The textual principal is invalid.
    InvalidText,
}

impl Display for PrincipalError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("principal text is invalid")
    }
}

/// Failure while decoding canonical principal bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalDecodeError {
    /// A principal cannot exceed 29 bytes.
    TooLarge {
        /// Actual byte length.
        len: usize,
    },
}

/// Failure while exposing canonical principal bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrincipalEncodeError {
    /// A principal cannot exceed its canonical maximum.
    TooLarge {
        /// Actual byte length.
        len: usize,
        /// Maximum canonical byte length.
        max: usize,
    },
}

/// Canonical principal atom.
#[derive(CandidType, Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
#[serde(transparent)]
pub struct Principal(candid::Principal);

impl Principal {
    /// Maximum canonical principal byte length.
    pub const MAX_LENGTH_IN_BYTES: u32 = 29;

    /// Minimum byte-ordered principal.
    pub const MIN: Self = Self::from_slice(&[0x00; 29]);

    /// Maximum byte-ordered principal.
    pub const MAX: Self = Self::from_slice(&[0xFF; 29]);

    /// Return the anonymous principal.
    #[must_use]
    pub const fn anonymous() -> Self {
        Self(candid::Principal::anonymous())
    }

    /// Parse canonical textual principal form.
    ///
    /// # Errors
    ///
    /// Returns [`PrincipalError::InvalidText`] for invalid text.
    pub fn from_text(text: &str) -> Result<Self, PrincipalError> {
        candid::Principal::from_text(text)
            .map(Self)
            .map_err(|_| PrincipalError::InvalidText)
    }

    /// Construct from canonical principal bytes.
    ///
    /// # Panics
    ///
    /// Panics when `bytes` exceeds the canonical 29-byte principal limit.
    /// Use [`Principal::try_from_bytes`] for untrusted input.
    #[must_use]
    pub const fn from_slice(bytes: &[u8]) -> Self {
        Self(candid::Principal::from_slice(bytes))
    }

    /// Borrow canonical principal bytes.
    #[must_use]
    pub const fn as_slice(&self) -> &[u8] {
        self.0.as_slice()
    }

    /// Borrow bounded canonical bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed error if an upstream value violates the canonical
    /// principal length.
    pub const fn stored_bytes(&self) -> Result<&[u8], PrincipalEncodeError> {
        let bytes = self.as_slice();
        if bytes.len() > Self::MAX_LENGTH_IN_BYTES as usize {
            return Err(PrincipalEncodeError::TooLarge {
                len: bytes.len(),
                max: Self::MAX_LENGTH_IN_BYTES as usize,
            });
        }
        Ok(bytes)
    }

    /// Copy bounded canonical bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed error for an invalid upstream representation.
    pub fn to_bytes(self) -> Result<Vec<u8>, PrincipalEncodeError> {
        Ok(self.stored_bytes()?.to_vec())
    }

    /// Decode bounded canonical bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed error when `bytes` exceeds the principal bound.
    pub const fn try_from_bytes(bytes: &[u8]) -> Result<Self, PrincipalDecodeError> {
        if bytes.len() > Self::MAX_LENGTH_IN_BYTES as usize {
            return Err(PrincipalDecodeError::TooLarge { len: bytes.len() });
        }
        Ok(Self::from_slice(bytes))
    }
}

impl Display for Principal {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl From<candid::Principal> for Principal {
    fn from(value: candid::Principal) -> Self {
        Self(value)
    }
}

impl From<Principal> for candid::Principal {
    fn from(value: Principal) -> Self {
        value.0
    }
}

impl FromStr for Principal {
    type Err = PrincipalError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        Self::from_text(input)
    }
}

impl Serialize for Principal {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.0.serialize(serializer)
    }
}

impl TryFrom<&[u8]> for Principal {
    type Error = PrincipalDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

/// Canonical bounded binary scalar.
#[derive(Clone, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub struct Blob(Vec<u8>);

impl Blob {
    /// Construct a bounded canonical blob.
    ///
    /// # Errors
    ///
    /// Returns [`SchemaContractError::InvalidLiteral`] when the value exceeds
    /// the proposal scalar bound.
    pub fn try_new(bytes: Vec<u8>) -> Result<Self, SchemaContractError> {
        if bytes.len() > MAX_PROPOSAL_LITERAL_BYTES {
            return Err(SchemaContractError::InvalidLiteral);
        }
        Ok(Self(bytes))
    }

    /// Borrow the canonical bytes.
    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    /// Consume the atom and return its bytes.
    #[must_use]
    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }

    /// Clone the canonical bytes.
    #[must_use]
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.clone()
    }

    /// Return the byte length.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.0.len()
    }

    /// Return whether the value is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Display for Blob {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(formatter, "[blob ({} bytes)]", self.len())
    }
}

impl From<Vec<u8>> for Blob {
    fn from(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }
}

impl From<&[u8]> for Blob {
    fn from(bytes: &[u8]) -> Self {
        Self(bytes.to_vec())
    }
}

impl<const N: usize> From<&[u8; N]> for Blob {
    fn from(bytes: &[u8; N]) -> Self {
        Self(bytes.to_vec())
    }
}

impl<'de> Deserialize<'de> for Blob {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        Self::try_new(bytes).map_err(D::Error::custom)
    }
}

impl CandidType for Blob {
    fn _ty() -> candid::types::Type {
        <Vec<u8> as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_blob(self.as_bytes())
    }
}

/// Failure while parsing a canonical ULID.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UlidParseError {
    /// The input is not canonical ULID text.
    InvalidString,
}

impl Display for UlidParseError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str("ULID text is invalid")
    }
}

/// Failure while decoding canonical ULID bytes.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UlidDecodeError {
    /// A ULID must contain exactly 16 bytes.
    InvalidSize {
        /// Actual byte length.
        len: usize,
    },
}

/// Canonical ULID atom without generation authority.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[repr(transparent)]
pub struct Ulid([u8; 16]);

impl Ulid {
    /// Canonical byte length.
    pub const STORED_SIZE: u32 = 16;

    /// Minimum ULID.
    pub const MIN: Self = Self::from_bytes([0x00; 16]);

    /// Maximum ULID.
    pub const MAX: Self = Self::from_bytes([0xFF; 16]);

    /// Nil ULID.
    #[must_use]
    pub const fn nil() -> Self {
        Self::MIN
    }

    /// Construct from the canonical 16-byte representation.
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    /// Construct deterministically from the canonical unsigned integer form.
    #[must_use]
    pub const fn from_u128(value: u128) -> Self {
        Self::from_bytes(value.to_be_bytes())
    }

    /// Return the canonical 16-byte representation.
    #[must_use]
    pub const fn to_bytes(self) -> [u8; 16] {
        self.0
    }

    /// Decode exactly 16 canonical bytes.
    ///
    /// # Errors
    ///
    /// Returns a typed size error for every other length.
    pub const fn try_from_bytes(bytes: &[u8]) -> Result<Self, UlidDecodeError> {
        if bytes.len() != Self::STORED_SIZE as usize {
            return Err(UlidDecodeError::InvalidSize { len: bytes.len() });
        }
        let mut value = [0; 16];
        value.copy_from_slice(bytes);
        Ok(Self::from_bytes(value))
    }
}

impl Display for Ulid {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&ulid::Ulid::from_bytes(self.0), formatter)
    }
}

impl fmt::Debug for Ulid {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.to_string(), formatter)
    }
}

impl FromStr for Ulid {
    type Err = UlidParseError;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        let value = input
            .parse::<ulid::Ulid>()
            .map_err(|_| UlidParseError::InvalidString)?;
        if value.to_string() != input {
            return Err(UlidParseError::InvalidString);
        }
        Ok(Self::from_bytes(value.to_bytes()))
    }
}

impl CandidType for Ulid {
    fn _ty() -> candid::types::Type {
        <String as CandidType>::_ty()
    }

    fn idl_serialize<S>(&self, serializer: S) -> Result<(), S::Error>
    where
        S: candid::types::Serializer,
    {
        serializer.serialize_text(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Ulid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(D::Error::custom)
    }
}

impl Serialize for Ulid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl TryFrom<&[u8]> for Ulid {
    type Error = UlidDecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

/// Failure while decoding one finite 32-bit floating-point atom.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Float32DecodeError {
    /// The input did not contain exactly four bytes.
    InvalidSize {
        /// Actual byte length.
        len: usize,
    },
    /// The bytes represented NaN or infinity.
    NonFinite,
}

/// Finite canonical `f32` atom.
#[derive(CandidType, Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct Float32(f32);

impl Float32 {
    /// Construct a finite float and normalize negative zero.
    #[must_use]
    pub fn try_new(value: f32) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        Some(Self(if value == 0.0 { 0.0 } else { value }))
    }

    /// Return the finite primitive value.
    #[must_use]
    pub const fn get(self) -> f32 {
        self.0
    }

    /// Return the stable big-endian IEEE-754 representation.
    #[must_use]
    pub const fn to_be_bytes(&self) -> [u8; 4] {
        self.0.to_bits().to_be_bytes()
    }

    /// Decode one stable big-endian IEEE-754 representation.
    ///
    /// # Errors
    ///
    /// Returns a typed error for the wrong byte length or a non-finite value.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, Float32DecodeError> {
        let bytes: [u8; 4] = bytes
            .try_into()
            .map_err(|_| Float32DecodeError::InvalidSize { len: bytes.len() })?;
        Self::try_new(f32::from_bits(u32::from_be_bytes(bytes)))
            .ok_or(Float32DecodeError::NonFinite)
    }

    /// Convert a finite, in-range `f64`.
    #[must_use]
    #[expect(clippy::cast_possible_truncation)]
    pub fn try_from_f64(value: f64) -> Option<Self> {
        if !value.is_finite() || value < f64::from(f32::MIN) || value > f64::from(f32::MAX) {
            return None;
        }
        Self::try_new(value as f32)
    }
}

impl Display for Float32 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl<'de> Deserialize<'de> for Float32 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(f32::deserialize(deserializer)?)
            .ok_or_else(|| D::Error::custom("Float32 must be finite"))
    }
}

impl Serialize for Float32 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_f32(self.0)
    }
}

impl Eq for Float32 {}

impl From<Float32> for f32 {
    fn from(value: Float32) -> Self {
        value.0
    }
}

#[expect(clippy::cast_precision_loss)]
impl From<i32> for Float32 {
    fn from(value: i32) -> Self {
        Self(value as f32)
    }
}

impl Hash for Float32 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u32(self.0.to_bits());
    }
}

impl Ord for Float32 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialEq for Float32 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd for Float32 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&[u8]> for Float32 {
    type Error = Float32DecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl TryFrom<f32> for Float32 {
    type Error = ();

    fn try_from(value: f32) -> Result<Self, Self::Error> {
        Self::try_new(value).ok_or(())
    }
}

/// Failure while decoding one finite 64-bit floating-point atom.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Float64DecodeError {
    /// The input did not contain exactly eight bytes.
    InvalidSize {
        /// Actual byte length.
        len: usize,
    },
    /// The bytes represented NaN or infinity.
    NonFinite,
}

/// Finite canonical `f64` atom.
#[derive(CandidType, Clone, Copy, Debug, Default)]
#[repr(transparent)]
pub struct Float64(f64);

impl Float64 {
    /// Construct a finite float and normalize negative zero.
    #[must_use]
    pub fn try_new(value: f64) -> Option<Self> {
        if !value.is_finite() {
            return None;
        }
        Some(Self(if value == 0.0 { 0.0 } else { value }))
    }

    /// Return the finite primitive value.
    #[must_use]
    pub const fn get(self) -> f64 {
        self.0
    }

    /// Return the stable big-endian IEEE-754 representation.
    #[must_use]
    pub const fn to_be_bytes(&self) -> [u8; 8] {
        self.0.to_bits().to_be_bytes()
    }

    /// Decode one stable big-endian IEEE-754 representation.
    ///
    /// # Errors
    ///
    /// Returns a typed error for the wrong byte length or a non-finite value.
    pub fn try_from_bytes(bytes: &[u8]) -> Result<Self, Float64DecodeError> {
        let bytes: [u8; 8] = bytes
            .try_into()
            .map_err(|_| Float64DecodeError::InvalidSize { len: bytes.len() })?;
        Self::try_new(f64::from_bits(u64::from_be_bytes(bytes)))
            .ok_or(Float64DecodeError::NonFinite)
    }
}

impl Display for Float64 {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        Display::fmt(&self.0, formatter)
    }
}

impl<'de> Deserialize<'de> for Float64 {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::try_new(f64::deserialize(deserializer)?)
            .ok_or_else(|| D::Error::custom("Float64 must be finite"))
    }
}

impl Serialize for Float64 {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_f64(self.0)
    }
}

impl Eq for Float64 {}

impl From<Float64> for f64 {
    fn from(value: Float64) -> Self {
        value.0
    }
}

impl From<i32> for Float64 {
    fn from(value: i32) -> Self {
        Self(f64::from(value))
    }
}

impl Hash for Float64 {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.0.to_bits());
    }
}

impl Ord for Float64 {
    fn cmp(&self, other: &Self) -> Ordering {
        self.0.total_cmp(&other.0)
    }
}

impl PartialEq for Float64 {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl PartialOrd for Float64 {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TryFrom<&[u8]> for Float64 {
    type Error = Float64DecodeError;

    fn try_from(bytes: &[u8]) -> Result<Self, Self::Error> {
        Self::try_from_bytes(bytes)
    }
}

impl TryFrom<f64> for Float64 {
    type Error = ();

    fn try_from(value: f64) -> Result<Self, Self::Error> {
        Self::try_new(value).ok_or(())
    }
}
