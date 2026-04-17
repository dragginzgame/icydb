use crate::db::sql_shared::Keyword;

pub(super) const fn is_identifier_start(byte: u8) -> bool {
    byte.is_ascii_alphabetic() || byte == b'_'
}

pub(super) const fn is_identifier_continue(byte: u8) -> bool {
    byte.is_ascii_alphanumeric() || byte == b'_'
}

pub(super) const fn keyword_from_ident(value: &str) -> Option<Keyword> {
    match value.len() {
        2 if value.eq_ignore_ascii_case("AS") => Some(Keyword::As),
        2 if value.eq_ignore_ascii_case("BY") => Some(Keyword::By),
        2 if value.eq_ignore_ascii_case("IN") => Some(Keyword::In),
        2 if value.eq_ignore_ascii_case("IS") => Some(Keyword::Is),
        2 if value.eq_ignore_ascii_case("OR") => Some(Keyword::Or),
        3 if value.eq_ignore_ascii_case("AND") => Some(Keyword::And),
        3 if value.eq_ignore_ascii_case("ASC") => Some(Keyword::Asc),
        3 if value.eq_ignore_ascii_case("AVG") => Some(Keyword::Avg),
        3 if value.eq_ignore_ascii_case("MAX") => Some(Keyword::Max),
        3 if value.eq_ignore_ascii_case("MIN") => Some(Keyword::Min),
        3 if value.eq_ignore_ascii_case("NOT") => Some(Keyword::Not),
        3 if value.eq_ignore_ascii_case("SUM") => Some(Keyword::Sum),
        4 if value.eq_ignore_ascii_case("DESC") => Some(Keyword::Desc),
        4 if value.eq_ignore_ascii_case("FROM") => Some(Keyword::From),
        4 if value.eq_ignore_ascii_case("JOIN") => Some(Keyword::Join),
        4 if value.eq_ignore_ascii_case("JSON") => Some(Keyword::Json),
        4 if value.eq_ignore_ascii_case("NULL") => Some(Keyword::Null),
        4 if value.eq_ignore_ascii_case("SHOW") => Some(Keyword::Show),
        4 if value.eq_ignore_ascii_case("TRUE") => Some(Keyword::True),
        4 if value.eq_ignore_ascii_case("WITH") => Some(Keyword::With),
        5 if value.eq_ignore_ascii_case("COUNT") => Some(Keyword::Count),
        5 if value.eq_ignore_ascii_case("FALSE") => Some(Keyword::False),
        5 if value.eq_ignore_ascii_case("GROUP") => Some(Keyword::Group),
        5 if value.eq_ignore_ascii_case("LIMIT") => Some(Keyword::Limit),
        5 if value.eq_ignore_ascii_case("ORDER") => Some(Keyword::Order),
        5 if value.eq_ignore_ascii_case("UNION") => Some(Keyword::Union),
        5 if value.eq_ignore_ascii_case("WHERE") => Some(Keyword::Where),
        9 if value.eq_ignore_ascii_case("RETURNING") => Some(Keyword::Returning),
        6 if value.eq_ignore_ascii_case("DELETE") => Some(Keyword::Delete),
        6 if value.eq_ignore_ascii_case("EXCEPT") => Some(Keyword::Except),
        6 if value.eq_ignore_ascii_case("HAVING") => Some(Keyword::Having),
        6 if value.eq_ignore_ascii_case("INSERT") => Some(Keyword::Insert),
        6 if value.eq_ignore_ascii_case("OFFSET") => Some(Keyword::Offset),
        6 if value.eq_ignore_ascii_case("SELECT") => Some(Keyword::Select),
        6 if value.eq_ignore_ascii_case("UPDATE") => Some(Keyword::Update),
        7 if value.eq_ignore_ascii_case("BETWEEN") => Some(Keyword::Between),
        7 if value.eq_ignore_ascii_case("COLUMNS") => Some(Keyword::Columns),
        7 if value.eq_ignore_ascii_case("EXPLAIN") => Some(Keyword::Explain),
        7 if value.eq_ignore_ascii_case("INDEXES") => Some(Keyword::Indexes),
        8 if value.eq_ignore_ascii_case("DESCRIBE") => Some(Keyword::Describe),
        8 if value.eq_ignore_ascii_case("DISTINCT") => Some(Keyword::Distinct),
        8 if value.eq_ignore_ascii_case("ENTITIES") => Some(Keyword::Entities),
        6 if value.eq_ignore_ascii_case("TABLES") => Some(Keyword::Tables),
        9 if value.eq_ignore_ascii_case("EXECUTION") => Some(Keyword::Execution),
        9 if value.eq_ignore_ascii_case("INTERSECT") => Some(Keyword::Intersect),
        _ => None,
    }
}
