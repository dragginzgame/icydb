//! Canonical account atom without storage authority.

#[cfg(test)]
mod tests;

use std::{
    fmt::{self, Display, Formatter},
    str::FromStr,
};

use serde::{Deserialize, Serialize};

use crate::{Principal, Subaccount};

/// Canonical ICRC account atom.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub struct Account {
    owner: Principal,
    subaccount: Option<Subaccount>,
}

impl Account {
    const PRINCIPAL_MAX_LEN: usize = Principal::MAX_LENGTH_IN_BYTES as usize;
    // A maximum principal occupies 63 text bytes, followed by '-', seven
    // checksum characters, '.', and at most 64 subaccount hex digits.
    const MAX_TEXT_LEN: usize = 136;
    const TAG_SUBACCOUNT: u8 = 0x80;

    /// Construct from convertible owner and optional subaccount values.
    pub fn new<P: Into<Principal>, S: Into<Subaccount>>(owner: P, subaccount: Option<S>) -> Self {
        Self {
            owner: owner.into(),
            subaccount: subaccount.map(Into::into),
        }
    }

    /// Construct from canonical components.
    #[must_use]
    pub const fn from_owner_and_subaccount(
        owner: Principal,
        subaccount: Option<Subaccount>,
    ) -> Self {
        Self { owner, subaccount }
    }

    /// Return the owner.
    #[must_use]
    pub const fn owner(&self) -> Principal {
        self.owner
    }

    /// Return the optional subaccount.
    #[must_use]
    pub const fn subaccount(&self) -> Option<Subaccount> {
        self.subaccount
    }

    // ICRC-1 text uses the CRC-32 of owner || subaccount in unpadded base32.
    fn checksum(owner: Principal, subaccount: Subaccount) -> String {
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(owner.as_slice());
        hasher.update(subaccount.as_slice());
        data_encoding::BASE32_NOPAD
            .encode(&hasher.finalize().to_be_bytes())
            .to_ascii_lowercase()
    }

    #[expect(clippy::cast_possible_truncation)]
    fn ordering_tag(&self) -> u8 {
        let mut tag = self.owner.as_slice().len().min(u8::MAX as usize) as u8;
        if self.subaccount.is_some() {
            tag |= Self::TAG_SUBACCOUNT;
        }
        tag
    }
}

impl Display for Account {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        let Some(subaccount) = self.subaccount.filter(|value| *value != Subaccount::MIN) else {
            return Display::fmt(&self.owner, formatter);
        };
        let hex = subaccount.to_string();
        write!(
            formatter,
            "{}-{}.{}",
            self.owner,
            Self::checksum(self.owner, subaccount),
            hex.trim_start_matches('0')
        )
    }
}

impl From<Principal> for Account {
    fn from(owner: Principal) -> Self {
        Self {
            owner,
            subaccount: None,
        }
    }
}

impl FromStr for Account {
    type Err = String;

    fn from_str(input: &str) -> Result<Self, Self::Err> {
        if input.len() > Self::MAX_TEXT_LEN || !input.is_ascii() {
            return Err("invalid account text width or alphabet".into());
        }
        let Some((prefix, hex)) = input.split_once('.') else {
            return input
                .parse::<Principal>()
                .map(Self::from)
                .map_err(|error| error.to_string());
        };
        let (owner, checksum) = prefix
            .rsplit_once('-')
            .filter(|(_, checksum)| checksum.len() == 7)
            .ok_or("missing account checksum")?;
        if hex.is_empty() || hex.len() > 64 || hex.starts_with('0') {
            return Err("invalid account subaccount width or leading zero".into());
        }
        let owner = owner
            .parse::<Principal>()
            .map_err(|error| error.to_string())?;
        // Left padding admits odd hex widths without allocating from input length.
        let mut padded = [b'0'; 64];
        padded[64 - hex.len()..].copy_from_slice(hex.as_bytes());
        let hex = std::str::from_utf8(&padded).map_err(|error| error.to_string())?;
        let subaccount = Subaccount::from_hex(hex).map_err(|error| error.to_string())?;
        if checksum != Self::checksum(owner, subaccount) {
            return Err("invalid account checksum".into());
        }
        Ok(Self::from_owner_and_subaccount(owner, Some(subaccount)))
    }
}

impl Ord for Account {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.ordering_tag()
            .cmp(&other.ordering_tag())
            .then_with(|| {
                let mut left = [0; Self::PRINCIPAL_MAX_LEN];
                let left_bytes = self.owner.as_slice();
                left[..left_bytes.len()].copy_from_slice(left_bytes);
                let mut right = [0; Self::PRINCIPAL_MAX_LEN];
                let right_bytes = other.owner.as_slice();
                right[..right_bytes.len()].copy_from_slice(right_bytes);
                left.cmp(&right)
            })
            .then_with(|| {
                self.subaccount
                    .unwrap_or(Subaccount::MIN)
                    .cmp(&other.subaccount.unwrap_or(Subaccount::MIN))
            })
    }
}

impl PartialOrd for Account {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
