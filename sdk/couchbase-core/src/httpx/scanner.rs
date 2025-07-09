use crate::httpx::error;
use std::str;

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) enum ScanState {
    Continue = 0,
    BeginLiteral = 1,
    BeginObject = 2,
    ObjectKey = 3,
    ObjectValue = 4,
    EndObject = 5,
    BeginArray = 6,
    ArrayValue = 7,
    EndArray = 8,
    SkipSpace = 9,

    // Stop.
    End = 10,
    Error = 11,
}

#[derive(Copy, Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub(crate) enum ParseState {
    ObjectKey = 0,
    ObjectValue = 1,
    ArrayValue = 2,
}

pub(crate) struct Scanner {
    step: fn(&mut Scanner, u8) -> ScanState,
    end_top: bool,
    parse_state: Vec<ParseState>,
    err: Option<error::Error>,
    bytes: usize,
}

impl Scanner {
    pub fn new() -> Self {
        Scanner {
            step: Scanner::state_begin_value,
            end_top: false,
            parse_state: Vec::new(),
            err: None,
            bytes: 0,
        }
    }

    pub fn step(&mut self, step: u8) -> ScanState {
        (self.step)(self, step)
    }

    pub fn incr_bytes(&mut self, incr: isize) {
        self.bytes = (self.bytes as isize + incr) as usize;
    }

    pub fn err(&self) -> Option<&error::Error> {
        self.err.as_ref()
    }

    pub fn reset(&mut self) {
        self.step = Scanner::state_begin_value;
        self.parse_state.clear();
        self.err = None;
        self.end_top = false;
    }

    fn eof(&mut self) -> ScanState {
        if self.err.is_some() {
            return ScanState::Error;
        }
        if self.end_top {
            return ScanState::End;
        }
        (self.step)(self, b' ')
    }

    fn state_begin_value(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_whitespace() {
            return ScanState::SkipSpace;
        }
        match c {
            b'{' => {
                s.step = Scanner::state_begin_string_or_empty;
                s.push_parse_state(ParseState::ObjectKey, ScanState::BeginObject)
            }
            b'[' => {
                s.step = Scanner::state_begin_value_or_empty;
                s.push_parse_state(ParseState::ArrayValue, ScanState::BeginArray)
            }
            b'"' => {
                s.step = Scanner::state_in_string;
                ScanState::BeginLiteral
            }
            b'-' => {
                s.step = Scanner::state_neg;
                ScanState::BeginLiteral
            }
            b'0' => {
                s.step = Scanner::state0;
                ScanState::BeginLiteral
            }
            b't' => {
                s.step = Scanner::state_t;
                ScanState::BeginLiteral
            }
            b'f' => {
                s.step = Scanner::state_f;
                ScanState::BeginLiteral
            }
            b'n' => {
                s.step = Scanner::state_n;
                ScanState::BeginLiteral
            }
            _ if c.is_ascii_digit() => {
                s.step = Scanner::state1;
                ScanState::BeginLiteral
            }
            _ => s.error(c, "looking for beginning of value"),
        }
    }

    fn state_begin_value_or_empty(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_whitespace() {
            return ScanState::SkipSpace;
        }
        if c == b']' {
            return Scanner::state_end_value(s, c);
        }
        Scanner::state_begin_value(s, c)
    }

    fn state_begin_string_or_empty(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_whitespace() {
            return ScanState::SkipSpace;
        }
        if c == b'}' {
            let n = s.parse_state.len();
            s.parse_state[n - 1] = ParseState::ObjectValue;
            return Scanner::state_end_value(s, c);
        }
        Scanner::state_begin_string(s, c)
    }

    fn state_begin_string(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_whitespace() {
            return ScanState::SkipSpace;
        }
        if c == b'"' {
            s.step = Scanner::state_in_string;
            return ScanState::BeginLiteral;
        }
        s.error(c, "looking for beginning of object key string")
    }

    fn state_end_value(s: &mut Scanner, c: u8) -> ScanState {
        let n = s.parse_state.len();
        if n == 0 {
            s.step = Scanner::state_end_top;
            s.end_top = true;
            return Scanner::state_end_top(s, c);
        }
        if c.is_ascii_whitespace() {
            s.step = Scanner::state_end_value;
            return ScanState::SkipSpace;
        }
        let ps = s.parse_state[n - 1];
        match ps {
            ParseState::ObjectKey => {
                if c == b':' {
                    s.parse_state[n - 1] = ParseState::ObjectValue;
                    s.step = Scanner::state_begin_value;
                    return ScanState::ObjectKey;
                }
                s.error(c, "after object key")
            }
            ParseState::ObjectValue => {
                if c == b',' {
                    s.parse_state[n - 1] = ParseState::ObjectKey;
                    s.step = Scanner::state_begin_string;
                    return ScanState::ObjectValue;
                }
                if c == b'}' {
                    s.pop_parse_state();
                    return ScanState::EndObject;
                }
                s.error(c, "after object key:value pair")
            }
            ParseState::ArrayValue => {
                if c == b',' {
                    s.step = Scanner::state_begin_value;
                    return ScanState::ArrayValue;
                }
                if c == b']' {
                    s.pop_parse_state();
                    return ScanState::EndArray;
                }
                s.error(c, "after array element")
            }
        }
    }

    fn state_end_top(s: &mut Scanner, c: u8) -> ScanState {
        if !c.is_ascii_whitespace() {
            s.error(c, "after top-level value");
        }

        ScanState::End
    }

    fn state_in_string(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'"' {
            s.step = Scanner::state_end_value;
            return ScanState::Continue;
        }
        if c == b'\\' {
            s.step = Scanner::state_in_string_esc;
            return ScanState::Continue;
        }
        if c < 0x20 {
            return s.error(c, "in string literal");
        }
        ScanState::Continue
    }

    fn state_in_string_esc(s: &mut Scanner, c: u8) -> ScanState {
        match c {
            b'b' | b'f' | b'n' | b'r' | b't' | b'\\' | b'/' | b'"' => {
                s.step = Scanner::state_in_string;
                ScanState::Continue
            }
            b'u' => {
                s.step = Scanner::state_in_string_esc_u;
                ScanState::Continue
            }
            _ => s.error(c, "in string escape code"),
        }
    }

    fn state_in_string_esc_u(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_hexdigit() {
            s.step = Scanner::state_in_string_esc_u1;
            ScanState::Continue
        } else {
            s.error(c, "in \\u hexadecimal character escape")
        }
    }

    fn state_in_string_esc_u1(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_hexdigit() {
            s.step = Scanner::state_in_string_esc_u12;
            ScanState::Continue
        } else {
            s.error(c, "in \\u hexadecimal character escape")
        }
    }

    fn state_in_string_esc_u12(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_hexdigit() {
            s.step = Scanner::state_in_string_esc_u123;
            ScanState::Continue
        } else {
            s.error(c, "in \\u hexadecimal character escape")
        }
    }

    fn state_in_string_esc_u123(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_hexdigit() {
            s.step = Scanner::state_in_string;
            ScanState::Continue
        } else {
            s.error(c, "in \\u hexadecimal character escape")
        }
    }

    fn state_neg(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'0' {
            s.step = Scanner::state0;
            ScanState::Continue
        } else if c.is_ascii_digit() {
            s.step = Scanner::state1;
            ScanState::Continue
        } else {
            s.error(c, "in numeric literal")
        }
    }

    fn state1(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_digit() {
            s.step = Scanner::state1;
            ScanState::Continue
        } else {
            Scanner::state0(s, c)
        }
    }

    fn state0(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'.' {
            s.step = Scanner::state_dot;
            ScanState::Continue
        } else if c == b'e' || c == b'E' {
            s.step = Scanner::state_e;
            ScanState::Continue
        } else {
            Scanner::state_end_value(s, c)
        }
    }

    fn state_dot(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_digit() {
            s.step = Scanner::state_dot0;
            ScanState::Continue
        } else {
            s.error(c, "after decimal point in numeric literal")
        }
    }

    fn state_dot0(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_digit() {
            ScanState::Continue
        } else if c == b'e' || c == b'E' {
            s.step = Scanner::state_e;
            ScanState::Continue
        } else {
            Scanner::state_end_value(s, c)
        }
    }

    fn state_e(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'+' || c == b'-' {
            s.step = Scanner::state_e_sign;
            ScanState::Continue
        } else {
            Scanner::state_e_sign(s, c)
        }
    }

    fn state_e_sign(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_digit() {
            s.step = Scanner::state_e0;
            ScanState::Continue
        } else {
            s.error(c, "in exponent of numeric literal")
        }
    }

    fn state_e0(s: &mut Scanner, c: u8) -> ScanState {
        if c.is_ascii_digit() {
            ScanState::Continue
        } else {
            Scanner::state_end_value(s, c)
        }
    }

    fn state_t(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'r' {
            s.step = Scanner::state_tr;
            ScanState::Continue
        } else {
            s.error(c, "in literal true (expecting 'r')")
        }
    }

    fn state_tr(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'u' {
            s.step = Scanner::state_tru;
            ScanState::Continue
        } else {
            s.error(c, "in literal true (expecting 'u')")
        }
    }

    fn state_tru(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'e' {
            s.step = Scanner::state_end_value;
            ScanState::Continue
        } else {
            s.error(c, "in literal true (expecting 'e')")
        }
    }

    fn state_f(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'a' {
            s.step = Scanner::state_fa;
            ScanState::Continue
        } else {
            s.error(c, "in literal false (expecting 'a')")
        }
    }

    fn state_fa(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'l' {
            s.step = Scanner::state_fal;
            ScanState::Continue
        } else {
            s.error(c, "in literal false (expecting 'l')")
        }
    }

    fn state_fal(s: &mut Scanner, c: u8) -> ScanState {
        if c == b's' {
            s.step = Scanner::state_fals;
            ScanState::Continue
        } else {
            s.error(c, "in literal false (expecting 's')")
        }
    }

    fn state_fals(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'e' {
            s.step = Scanner::state_end_value;
            ScanState::Continue
        } else {
            s.error(c, "in literal false (expecting 'e')")
        }
    }

    fn state_n(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'u' {
            s.step = Scanner::state_nu;
            ScanState::Continue
        } else {
            s.error(c, "in literal null (expecting 'u')")
        }
    }

    fn state_nu(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'l' {
            s.step = Scanner::state_nul;
            ScanState::Continue
        } else {
            s.error(c, "in literal null (expecting 'l')")
        }
    }

    fn state_nul(s: &mut Scanner, c: u8) -> ScanState {
        if c == b'l' {
            s.step = Scanner::state_end_value;
            ScanState::Continue
        } else {
            s.error(c, "in literal null (expecting 'l')")
        }
    }

    fn error(&mut self, c: u8, context: &str) -> ScanState {
        self.step = Scanner::state_error;
        self.err = Some(error::Error::new_message_error(format!(
            "invalid character {} {}",
            Scanner::quote_char(c),
            context
        )));
        ScanState::Error
    }

    fn state_error(_s: &mut Scanner, _c: u8) -> ScanState {
        ScanState::Error
    }

    pub fn quote_char(c: u8) -> String {
        match c {
            b'\'' => "'\\''".to_string(),
            b'"' => "'\"'".to_string(),
            _ => format!("'{}'", c as char),
        }
    }

    fn push_parse_state(
        &mut self,
        new_parse_state: ParseState,
        success_state: ScanState,
    ) -> ScanState {
        self.parse_state.push(new_parse_state);
        success_state
    }

    fn pop_parse_state(&mut self) {
        self.parse_state.pop();
        if self.parse_state.is_empty() {
            self.step = Scanner::state_end_top;
            self.end_top = true;
        } else {
            self.step = Scanner::state_end_value;
        }
    }
}

fn valid(data: &[u8]) -> bool {
    let mut scan = Scanner::new();
    for &c in data {
        scan.bytes += 1;
        if (scan.step)(&mut scan, c) == ScanState::Error {
            return false;
        }
    }
    scan.eof() != ScanState::Error
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_valid() {
        let tests = vec![
            ("foo", false),
            ("}{", false),
            ("{]", false),
            ("{}", true),
            ("[]", true),
            ("[1,2,3]", true),
            ("[1,2,3,]", false),
            (r#"{"foo":"bar"}"#, true),
            (r#"{"foo": "bar",}"#, false),
            (r#"{"foo": "bar", "baz":}"#, false),
            (r#"{"foo": "bar", "baz": 123,}"#, false),
            (r#"{"foo":"bar","bar":{"baz":["qux"]}}"#, true),
            ("{\"foo\": \"bar\", \"baz\": 123, \"qux\":}", false),
        ];

        for (data, expected) in tests {
            assert_eq!(valid(data.as_bytes()), expected, "data: {data}");
        }
    }
}
