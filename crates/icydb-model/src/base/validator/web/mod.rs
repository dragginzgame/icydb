//! Module: base::validator::web
//!
//! Responsibility: base validator definitions.
//! Does not own: normalization policy, persistence, or schema mutation semantics.
//! Boundary: reports typed visitor issues for facade schema values.

use crate::{prelude::*, visitor::Validator};

fn is_mime_token_part(part: &str) -> bool {
    let Some(first) = part.chars().next() else {
        return false;
    };
    let Some(last) = part.chars().next_back() else {
        return false;
    };

    !part.is_empty()
        && first.is_ascii_alphanumeric()
        && last.is_ascii_alphanumeric()
        && part
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || "+.-".contains(c))
}

///
/// MimeType
///
/// Validates a basic MIME type token pair in the form `type/subtype`.
/// Each token must start and end with ASCII alphanumeric characters and may
/// contain `+`, `-`, or `.` internally.
///

#[validator]
pub struct MimeType;

impl Validator<str> for MimeType {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        // Split into at most three slash segments so we can enforce exactly one '/'.
        let mut slash_segments = s.split('/');
        let type_part = slash_segments.next();
        let subtype_part = slash_segments.next();
        let extra_part = slash_segments.next();

        // Must contain exactly one '/'
        if type_part.is_none() || subtype_part.is_none() || extra_part.is_some() {
            ctx.issue("MIME type must contain exactly one slash");
            return;
        }

        let (Some(type_part), Some(subtype_part)) = (type_part, subtype_part) else {
            return;
        };

        if !is_mime_token_part(type_part) || !is_mime_token_part(subtype_part) {
            ctx.issue("MIME type contains invalid token characters");
        }
    }
}

fn url_has_forbidden_chars(s: &str) -> bool {
    s.chars()
        .any(|ch| ch.is_ascii_control() || ch.is_ascii_whitespace())
}

fn split_http_url_rest(s: &str) -> Option<&str> {
    if let Some(rest) = s.strip_prefix("http://") {
        Some(rest)
    } else {
        s.strip_prefix("https://")
    }
}

fn url_host_end(rest: &str) -> usize {
    rest.find(['/', '?', '#']).unwrap_or(rest.len())
}

fn url_host_and_port_are_valid(host: &str) -> bool {
    if host.is_empty() || host.contains('@') {
        return false;
    }

    if let Some(bracketed) = host.strip_prefix('[') {
        let Some(end) = bracketed.find(']') else {
            return false;
        };
        let address = &bracketed[..end];
        let suffix = &bracketed[end + 1..];

        return !address.is_empty()
            && address
                .chars()
                .all(|ch| ch.is_ascii_hexdigit() || ch == ':' || ch == '.')
            && url_port_suffix_is_valid(suffix);
    }

    let hostname = match host.rsplit_once(':') {
        Some((hostname, port)) => {
            if hostname.contains(':')
                || port.is_empty()
                || !port.chars().all(|ch| ch.is_ascii_digit())
            {
                return false;
            }
            hostname
        }
        None => host,
    };

    url_hostname_is_valid(hostname)
}

fn url_port_suffix_is_valid(suffix: &str) -> bool {
    if suffix.is_empty() {
        return true;
    }
    let Some(port) = suffix.strip_prefix(':') else {
        return false;
    };

    !port.is_empty() && port.chars().all(|ch| ch.is_ascii_digit())
}

fn url_hostname_is_valid(hostname: &str) -> bool {
    if hostname.is_empty() || hostname == "." || hostname == ".." {
        return false;
    }

    hostname.split('.').all(|label| {
        let Some(first) = label.chars().next() else {
            return false;
        };
        let Some(last) = label.chars().next_back() else {
            return false;
        };

        !label.is_empty()
            && first.is_ascii_alphanumeric()
            && last.is_ascii_alphanumeric()
            && label
                .chars()
                .all(|ch| ch.is_ascii_alphanumeric() || ch == '-')
    })
}

///
/// Url
///
/// Validates that the value uses `http://` or `https://` and has a non-empty
/// host without whitespace, control characters, userinfo, or malformed ports.
///

#[validator]
pub struct Url;

impl Validator<str> for Url {
    fn validate(&self, s: &str, ctx: &mut dyn VisitorContext) {
        let Some(rest) = split_http_url_rest(s) else {
            ctx.issue("URL must start with http:// or https://");
            return;
        };

        if url_has_forbidden_chars(s) {
            ctx.issue("URL must not contain whitespace or control characters");
            return;
        }

        let host_end = url_host_end(rest);
        let host = &rest[..host_end];
        if !url_host_and_port_are_valid(host) {
            ctx.issue("URL host is malformed");
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
