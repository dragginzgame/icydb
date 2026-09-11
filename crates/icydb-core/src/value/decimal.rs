//! Shared decimal magnitude conversion. Callers own allocation/work admission
//! and outward grammar; this owner does not know query budgets or key formats.

#[cfg(test)]
mod tests;

use crate::types::{IntBig, NatBig};
use std::fmt::{self, Write};

pub(crate) const DECIMAL_CHUNK_WIDTH: usize = 9;
const DECIMAL_CHUNK_BASE: u64 = 1_000_000_000;

/// A formatter must admit hidden conversion scratch as well as output writes.
/// Ordinary String formatting is infallible; bounded callers supply their own
/// authority and preserve its error when mapping through fmt::Error.
pub(crate) trait ValueFormatWriter: Write {
    fn admit_scratch(&mut self, bytes: u64, steps: u64) -> fmt::Result;
}

impl ValueFormatWriter for String {
    fn admit_scratch(&mut self, _bytes: u64, _steps: u64) -> fmt::Result {
        Ok(())
    }
}

/// Admit the full scratch and conservative conversion work before reading limbs.
/// The conversion itself has no budget callbacks or partial failure states.
pub(crate) fn signed_chunks<E>(
    value: &IntBig,
    admit: impl FnOnce(u64, u64) -> Result<(), E>,
) -> Result<(bool, Vec<u32>), E> {
    let (negative, limbs) = value.sign_and_u32_digits();
    Ok((negative, chunks_from_limbs(limbs, admit)?))
}

/// Copy an unsigned magnitude only after admitting its initialized limb backing.
pub(crate) fn unsigned_chunks<E>(
    value: &NatBig,
    admit: impl FnOnce(u64, u64) -> Result<(), E>,
) -> Result<Vec<u32>, E> {
    chunks_from_limbs(value.u32_digits(), admit)
}

fn chunks_from_limbs<E>(
    limbs: impl ExactSizeIterator<Item = u32>,
    admit: impl FnOnce(u64, u64) -> Result<(), E>,
) -> Result<Vec<u32>, E> {
    let len = limbs.len();
    // 1e9 > 2^29, so c = ceil(32*n/29) bounds the number of chunks/passes.
    // Both backing vectors total 4*(n+c) bytes. Copying visits n limbs;
    // division visits at most n*c, and trimming visits at most n+c (each
    // zero limb is removed once, plus at most one nonzero check per pass).
    // These are conservative work units, not an instruction or live-heap count.
    let capacity = (len / 29) * 32 + ((len % 29) * 32).div_ceil(29);
    let n = len as u64;
    let c = capacity as u64;
    admit(
        n.saturating_add(c).saturating_mul(4),
        n.saturating_mul(c)
            .saturating_add(n.saturating_mul(2))
            .saturating_add(c),
    )?;
    let mut quotient = Vec::with_capacity(len);
    quotient.extend(limbs);
    trim_zero_limbs(&mut quotient);
    if quotient.is_empty() {
        return Ok(Vec::new());
    }
    let mut chunks = Vec::with_capacity(capacity);
    while !quotient.is_empty() {
        let mut remainder = 0_u64;
        for limb in quotient.iter_mut().rev() {
            let value = (remainder << 32) | u64::from(*limb);
            // remainder < 1e9, so the quotient fits one base-2^32 limb.
            *limb = u32::try_from(value / DECIMAL_CHUNK_BASE).unwrap_or_default();
            remainder = value % DECIMAL_CHUNK_BASE;
        }
        chunks.push(u32::try_from(remainder).unwrap_or_default());
        trim_zero_limbs(&mut quotient);
    }
    Ok(chunks)
}

fn trim_zero_limbs(limbs: &mut Vec<u32>) {
    while limbs.last() == Some(&0) {
        limbs.pop();
    }
}

/// Count output digits without constructing or walking the digit buffer.
pub(crate) fn digit_count(chunks: &[u32]) -> Option<usize> {
    let Some(&leading) = chunks.last() else {
        return Some(1);
    };
    (chunks.len() - 1)
        .checked_mul(DECIMAL_CHUNK_WIDTH)
        .and_then(|padded| padded.checked_add(chunk_width(leading)))
}

/// Stream canonical ASCII chunks through fixed stack scratch, most significant
/// first. The callback owns output admission and stops subsequent emission.
pub(crate) fn visit_digits<E>(
    chunks: &[u32],
    mut emit: impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    let Some((&leading, remaining)) = chunks.split_last() else {
        return emit(b"0");
    };
    emit_chunk(leading, chunk_width(leading), &mut emit)?;
    for &chunk in remaining.iter().rev() {
        emit_chunk(chunk, DECIMAL_CHUNK_WIDTH, &mut emit)?;
    }
    Ok(())
}

fn chunk_width(chunk: u32) -> usize {
    chunk.checked_ilog10().unwrap_or(0) as usize + 1
}

fn emit_chunk<E>(
    mut chunk: u32,
    width: usize,
    emit: &mut impl FnMut(&[u8]) -> Result<(), E>,
) -> Result<(), E> {
    // Ten bytes cover every u32 leading chunk; canonical converted chunks
    // use at most nine. Neither caller-controlled digits nor width allocate.
    let mut scratch = [b'0'; 10];
    for digit in scratch[..width].iter_mut().rev() {
        *digit = b'0' + u8::try_from(chunk % 10).unwrap_or_default();
        chunk /= 10;
    }
    emit(&scratch[..width])
}

/// Preserve the signed Candid literal spelling without decimal/grouping Strings.
pub(crate) fn write_signed_literal(
    value: &IntBig,
    out: &mut (impl ValueFormatWriter + ?Sized),
) -> fmt::Result {
    let (negative, chunks) = signed_chunks(value, |bytes, steps| out.admit_scratch(bytes, steps))?;
    if negative {
        out.write_char('-')?;
    }
    write_grouped_digits(&chunks, out)
}

/// Preserve the unsigned Candid literal spelling under the same admission owner.
pub(crate) fn write_unsigned_literal(
    value: &NatBig,
    out: &mut (impl ValueFormatWriter + ?Sized),
) -> fmt::Result {
    let chunks = unsigned_chunks(value, |bytes, steps| out.admit_scratch(bytes, steps))?;
    write_grouped_digits(&chunks, out)
}

fn write_grouped_digits(chunks: &[u32], out: &mut (impl Write + ?Sized)) -> fmt::Result {
    let mut remaining = digit_count(chunks).ok_or(fmt::Error)?;
    visit_digits(chunks, |digits| {
        for &digit in digits {
            out.write_char(char::from(digit))?;
            remaining -= 1;
            if remaining != 0 && remaining.is_multiple_of(3) {
                out.write_char('_')?;
            }
        }
        Ok(())
    })
}
