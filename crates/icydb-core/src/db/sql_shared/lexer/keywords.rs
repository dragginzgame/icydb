use crate::db::sql_shared::Keyword;

const KEYWORDS_LEN_2: &[(&str, Keyword)] = &[
    ("AS", Keyword::As),
    ("BY", Keyword::By),
    ("IN", Keyword::In),
    ("IS", Keyword::Is),
    ("OR", Keyword::Or),
];

const KEYWORDS_LEN_3: &[(&str, Keyword)] = &[
    ("AND", Keyword::And),
    ("ASC", Keyword::Asc),
    ("AVG", Keyword::Avg),
    ("END", Keyword::End),
    ("MAX", Keyword::Max),
    ("MIN", Keyword::Min),
    ("NOT", Keyword::Not),
    ("SUM", Keyword::Sum),
];

const KEYWORDS_LEN_4: &[(&str, Keyword)] = &[
    ("CASE", Keyword::Case),
    ("DESC", Keyword::Desc),
    ("ELSE", Keyword::Else),
    ("FROM", Keyword::From),
    ("JOIN", Keyword::Join),
    ("JSON", Keyword::Json),
    ("NULL", Keyword::Null),
    ("OVER", Keyword::Over),
    ("SHOW", Keyword::Show),
    ("THEN", Keyword::Then),
    ("TRUE", Keyword::True),
    ("WHEN", Keyword::When),
    ("WITH", Keyword::With),
];

const KEYWORDS_LEN_5: &[(&str, Keyword)] = &[
    ("COUNT", Keyword::Count),
    ("FALSE", Keyword::False),
    ("GROUP", Keyword::Group),
    ("LIMIT", Keyword::Limit),
    ("ORDER", Keyword::Order),
    ("UNION", Keyword::Union),
    ("WHERE", Keyword::Where),
];

const KEYWORDS_LEN_6: &[(&str, Keyword)] = &[
    ("DELETE", Keyword::Delete),
    ("EXCEPT", Keyword::Except),
    ("FILTER", Keyword::Filter),
    ("HAVING", Keyword::Having),
    ("INSERT", Keyword::Insert),
    ("OFFSET", Keyword::Offset),
    ("SELECT", Keyword::Select),
    ("TABLES", Keyword::Tables),
    ("UPDATE", Keyword::Update),
];

const KEYWORDS_LEN_7: &[(&str, Keyword)] = &[
    ("BETWEEN", Keyword::Between),
    ("COLUMNS", Keyword::Columns),
    ("EXPLAIN", Keyword::Explain),
    ("INDEXES", Keyword::Indexes),
];

const KEYWORDS_LEN_8: &[(&str, Keyword)] = &[
    ("DESCRIBE", Keyword::Describe),
    ("DISTINCT", Keyword::Distinct),
    ("ENTITIES", Keyword::Entities),
];

const KEYWORDS_LEN_9: &[(&str, Keyword)] = &[
    ("EXECUTION", Keyword::Execution),
    ("INTERSECT", Keyword::Intersect),
    ("RETURNING", Keyword::Returning),
];

pub(super) const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

pub(super) const fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub(super) fn keyword_from_ident_bytes(value: &[u8]) -> Option<Keyword> {
    match value.len() {
        2 => find_keyword_by_length(value, KEYWORDS_LEN_2),
        3 => find_keyword_by_length(value, KEYWORDS_LEN_3),
        4 => find_keyword_by_length(value, KEYWORDS_LEN_4),
        5 => find_keyword_by_length(value, KEYWORDS_LEN_5),
        6 => find_keyword_by_length(value, KEYWORDS_LEN_6),
        7 => find_keyword_by_length(value, KEYWORDS_LEN_7),
        8 => find_keyword_by_length(value, KEYWORDS_LEN_8),
        9 => find_keyword_by_length(value, KEYWORDS_LEN_9),
        _ => None,
    }
}

// Keep keyword classification flat and table-driven so adding one keyword does
// not grow another long branch ladder in the shared lexer boundary.
// The lexer calls this on borrowed token bytes first so keyword hits do not
// allocate a temporary identifier string only to discard it again.
fn find_keyword_by_length(value: &[u8], keywords: &[(&str, Keyword)]) -> Option<Keyword> {
    for (keyword, token) in keywords {
        if value.eq_ignore_ascii_case(keyword.as_bytes()) {
            return Some(*token);
        }
    }

    None
}
