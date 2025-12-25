use crate::prelude::*;

///
/// Iso3166_1A2
///
/// ISO 3166-1 alpha-2 country codes
/// <https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>
///

#[validator]
pub struct Iso3166_1A2;

pub const VALID_ENTRIES_3166_1A2: &[&str] = &[
    "AD", "AE", "AF", "AG", "AI", "AL", "AM", "AO", "AQ", "AR", "AS", "AT", "AU", "AW", "AX", "AZ",
    "BA", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BL", "BM", "BN", "BO", "BQ", "BR", "BS",
    "BT", "BV", "BW", "BY", "BZ", "CA", "CC", "CD", "CF", "CG", "CH", "CI", "CK", "CL", "CM", "CN",
    "CO", "CR", "CU", "CV", "CW", "CX", "CY", "CZ", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE",
    "EG", "EH", "ER", "ES", "ET", "FI", "FJ", "FM", "FO", "FR", "GA", "GB", "GD", "GE", "GF", "GG",
    "GH", "GI", "GL", "GM", "GN", "GP", "GQ", "GR", "GS", "GT", "GU", "GW", "GY", "HK", "HM", "HN",
    "HR", "HT", "HU", "ID", "IE", "IL", "IM", "IN", "IO", "IQ", "IR", "IS", "IT", "JE", "JM", "JO",
    "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB", "LC", "LI",
    "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "ME", "MF", "MG", "MH", "MK", "ML",
    "MM", "MN", "MO", "MP", "MQ", "MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NC",
    "NE", "NF", "NG", "NI", "NL", "NO", "NP", "NR", "NU", "NZ", "OM", "PA", "PE", "PF", "PG", "PH",
    "PK", "PL", "PM", "PN", "PR", "PS", "PT", "PW", "PY", "QA", "RE", "RO", "RS", "RU", "RW", "SA",
    "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SJ", "SK", "SL", "SM", "SN", "SO", "SR", "SS", "ST",
    "SV", "SX", "SY", "SZ", "TC", "TD", "TF", "TG", "TH", "TJ", "TK", "TL", "TM", "TN", "TO", "TR",
    "TT", "TV", "TZ", "UA", "UG", "UM", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VI", "VN", "VU",
    "WF", "WS", "YE", "YT", "ZA", "ZM", "ZW",
];

impl Validator<str> for Iso3166_1A2 {
    fn validate(&self, s: &str) -> Result<(), ValidateIssue> {
        if VALID_ENTRIES_3166_1A2.contains(&s) {
            Ok(())
        } else {
            Err(ValidateIssue::validation(format!(
                "unknown ISO 3166-1 alpha-2 country code: {s}"
            )))
        }
    }
}

///
/// Iso639_1
///
/// language code
/// <https://en.wikipedia.org/wiki/List_of_ISO_639-1_codes>
///

#[validator]
pub struct Iso639_1;

pub const VALID_ENTRIES_639_1: &[&str] = &[
    "aa", "ab", "ae", "af", "ak", "am", "an", "ar", "as", "av", "ay", "az", "ba", "be", "bg", "bh",
    "bi", "bm", "bn", "bo", "br", "bs", "ca", "ce", "ch", "co", "cr", "cs", "cu", "cv", "cy", "da",
    "de", "dv", "dz", "ee", "el", "en", "eo", "es", "et", "eu", "fa", "ff", "fi", "fj", "fo", "fr",
    "fy", "ga", "gd", "gl", "gn", "gu", "gv", "ha", "he", "hi", "ho", "hr", "ht", "hu", "hy", "hz",
    "ia", "id", "ie", "ig", "ii", "ik", "io", "is", "it", "iu", "ja", "jv", "ka", "kg", "ki", "kj",
    "kk", "kl", "km", "kn", "ko", "kr", "ks", "ku", "kv", "kw", "ky", "la", "lb", "lg", "li", "ln",
    "lo", "lt", "lu", "lv", "mg", "mh", "mi", "mk", "ml", "mn", "mr", "ms", "mt", "my", "na", "nb",
    "nd", "ne", "ng", "nl", "nn", "no", "nr", "nv", "ny", "oc", "oj", "om", "or", "os", "pa", "pi",
    "pl", "ps", "pt", "qu", "rm", "rn", "ro", "ru", "rw", "sa", "sc", "sd", "se", "sg", "si", "sk",
    "sl", "sm", "sn", "so", "sq", "sr", "ss", "st", "su", "sv", "sw", "ta", "te", "tg", "th", "ti",
    "tk", "tl", "tn", "to", "tr", "ts", "tt", "tw", "ty", "ug", "uk", "ur", "uz", "ve", "vi", "vo",
    "wa", "wo", "xh", "yi", "yo", "za", "zh", "zu",
];

impl Validator<str> for Iso639_1 {
    fn validate(&self, s: &str) -> Result<(), ValidateIssue> {
        if VALID_ENTRIES_639_1.contains(&s) {
            Ok(())
        } else {
            Err(ValidateIssue::validation(format!(
                "unknown ISO 639-1 language code: {s}"
            )))
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::traits::Validator;

    // -------------------------------------------------------------------------
    // ISO 3166-1 alpha-2 country codes
    // -------------------------------------------------------------------------
    #[test]
    fn test_iso31661alpha2_valid() {
        let v = Iso3166_1A2 {};
        assert!(v.validate("US").is_ok());
        assert!(v.validate("DE").is_ok());
        assert!(v.validate("JP").is_ok());
    }

    #[test]
    fn test_iso31661alpha2_invalid_format() {
        let v = Iso3166_1A2 {};
        assert!(v.validate("us").is_err()); // lowercase not allowed
        assert!(v.validate("USA").is_err()); // too long
        assert!(v.validate("U").is_err()); // too short
    }

    #[test]
    fn test_iso31661alpha2_unknown_code() {
        let v = Iso3166_1A2 {};
        assert!(v.validate("ZZ").is_err()); // not a real country
    }

    // -------------------------------------------------------------------------
    // ISO 639-1 language codes
    // -------------------------------------------------------------------------
    #[test]
    fn test_iso6391_valid() {
        let v = Iso639_1 {};
        assert!(v.validate("en").is_ok());
        assert!(v.validate("fr").is_ok());
        assert!(v.validate("zh").is_ok());
    }

    #[test]
    fn test_iso6391_invalid_format() {
        let v = Iso639_1 {};
        assert!(v.validate("EN").is_err()); // uppercase not allowed
        assert!(v.validate("eng").is_err()); // too long
        assert!(v.validate("e").is_err()); // too short
    }

    #[test]
    fn test_iso6391_unknown_code() {
        let v = Iso639_1 {};
        assert!(v.validate("xx").is_err()); // not a real language
    }
}
