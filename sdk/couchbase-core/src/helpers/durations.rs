use std::collections::HashMap;
use std::ops::Add;
use std::time::Duration;

use lazy_static::lazy_static;

lazy_static! {
    static ref UNIT_MAP: HashMap<&'static str, u64> = {
        let mut m = HashMap::new();
        m.insert("ns", 1);
        m.insert("us", 1_000);
        m.insert("µs", 1_000);
        m.insert("μs", 1_000);
        m.insert("ms", 1_000_000);
        m.insert("s", 1_000_000_000);
        m.insert("m", 60_000_000_000);
        m.insert("h", 3_600_000_000_000);
        m
    };
}

// serialize_duration_to_golang_string creates a golang formatted string to use with timeouts.
// Unlike Golang strings it does not deal with fractional seconds, we do not need that accuracy.
pub fn duration_to_golang_string(duration: &Duration) -> String {
    let mut total_secs = duration.as_secs();
    let secs = total_secs % 60;
    total_secs /= 60;
    let mut golang_string = format!("{}s", secs);
    if total_secs > 0 {
        let minutes = total_secs % 60;
        total_secs /= 60;
        golang_string = format!("{}m{}", minutes, golang_string);
        if total_secs > 0 {
            golang_string = format!("{}h{}", total_secs, golang_string)
        }
    }

    golang_string
}

// Note that this will not parse negative durations, rust Durations do not support negative values.
pub fn parse_duration_from_golang_string(s: &str) -> Result<Duration, String> {
    let orig = s;
    let mut d: u64 = 0;
    let mut s = s;

    // Consume [-+]?
    if !s.is_empty() {
        let c = s.chars().next().unwrap();
        if c == '-' || c == '+' {
            if c == '-' {
                return Err(format!("invalid negative duration {}", quote(orig)));
            }
            s = &s[1..];
        }
    }

    // Special case: if all that is left is "0", this is zero.
    if s == "0" {
        return Ok(Duration::from_nanos(0));
    }
    if s.is_empty() {
        return Err(format!("invalid duration {}", quote(orig)));
    }

    while !s.is_empty() {
        let mut v: u64 = 0;
        let mut f: u64 = 0;
        let mut scale: f64 = 1.0;

        // The next character must be [0-9.]
        if !s.starts_with(|c: char| c.is_ascii_digit() || c == '.') {
            return Err(format!("invalid duration {}", quote(orig)));
        }

        // Consume [0-9]*
        let pl = s.len();
        let (v_temp, s_temp) = leading_int(s)?;
        v = v_temp;
        s = s_temp;
        let pre = pl != s.len();

        // Consume (\.[0-9]*)?
        let mut post = false;
        if s.starts_with('.') {
            s = &s[1..];
            let pl = s.len();
            let (f_temp, scale_temp, s_temp) = leading_fraction(s);
            f = f_temp;
            scale = scale_temp;
            s = s_temp;
            post = pl != s.len();
        }

        if !pre && !post {
            return Err(format!("invalid duration {}", quote(orig)));
        }

        // Consume unit.
        let i = s
            .find(|c: char| c == '.' || c.is_ascii_digit())
            .unwrap_or(s.len());
        if i == 0 {
            return Err(format!("missing unit in duration {}", quote(orig)));
        }
        let u = &s[..i];
        s = &s[i..];
        let unit = UNIT_MAP
            .get(u)
            .ok_or_else(|| format!("unknown unit {} in duration {}", quote(u), quote(orig)))?;

        if v > u64::MAX / unit {
            return Err(format!("invalid duration {}", quote(orig)));
        }
        v *= unit;

        if f > 0 {
            let vf = f as f64 * (*unit as f64 / scale);
            if vf > u64::MAX as f64 {
                return Err(format!("invalid duration {}", quote(orig)));
            }

            v += vf as u64
        }

        d = d
            .checked_add(v)
            .ok_or_else(|| format!("invalid duration {}", quote(orig)))?;
    }

    Ok(Duration::from_nanos(d))
}

fn leading_int(s: &str) -> Result<(u64, &str), String> {
    let mut x: u64 = 0;
    let mut i = 0;
    for c in s.chars() {
        if !c.is_ascii_digit() {
            break;
        }
        if x > u64::MAX / 10 {
            return Err("bad [0-9]*".to_string());
        }
        x = x * 10 + (c as u64 - '0' as u64);
        i += 1;
    }
    Ok((x, &s[i..]))
}

fn leading_fraction(s: &str) -> (u64, f64, &str) {
    let mut x: u64 = 0;
    let mut scale: f64 = 1.0;
    let mut overflow = false;
    let mut i = 0;
    for c in s.chars() {
        if !c.is_ascii_digit() {
            break;
        }
        if overflow {
            i += 1;
            continue;
        }
        if x > (u64::MAX - 1) / 10 {
            overflow = true;
            i += 1;
            continue;
        }
        let y = x * 10 + (c as u64 - '0' as u64);
        x = y;
        scale *= 10.0;
        i += 1;
    }
    (x, scale, &s[i..])
}

fn quote(s: &str) -> String {
    format!("\"{}\"", s)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::helpers::durations::parse_duration_from_golang_string;

    #[test]
    fn test_parse_duration() {
        let parse_duration_tests = vec![
            // simple
            ("0", Duration::from_secs(0)),
            ("5s", Duration::from_secs(5)),
            ("30s", Duration::from_secs(30)),
            ("1478s", Duration::from_secs(1478)),
            ("+5s", Duration::from_secs(5)),
            ("+0", Duration::from_secs(0)),
            // decimal
            ("5.0s", Duration::from_secs(5)),
            ("5.6s", Duration::from_millis(5600)),
            ("5.s", Duration::from_secs(5)),
            (".5s", Duration::from_millis(500)),
            ("1.0s", Duration::from_secs(1)),
            ("1.00s", Duration::from_secs(1)),
            ("1.004s", Duration::from_millis(1004)),
            ("1.0040s", Duration::from_millis(1004)),
            ("100.00100s", Duration::from_millis(100001)),
            // different units
            ("10ns", Duration::from_nanos(10)),
            ("11us", Duration::from_micros(11)),
            ("12µs", Duration::from_micros(12)),
            ("12μs", Duration::from_micros(12)),
            ("13ms", Duration::from_millis(13)),
            ("14s", Duration::from_secs(14)),
            ("15m", Duration::from_secs(15 * 60)),
            ("16h", Duration::from_secs(16 * 3600)),
            // composite durations
            ("3h30m", Duration::from_secs(3 * 3600 + 30 * 60)),
            ("10.5s4m", Duration::from_millis(4 * 60 * 1000 + 10500)),
            (
                "1h2m3s4ms5us6ns",
                Duration::from_secs(60 * 60)
                    + Duration::from_secs(2 * 60)
                    + Duration::from_secs(3)
                    + Duration::from_millis(4)
                    + Duration::from_micros(5)
                    + Duration::from_nanos(6),
            ),
            (
                "39h9m14.425s",
                Duration::from_secs(39 * 60 * 60)
                    + Duration::from_secs(9 * 60)
                    + Duration::from_millis(14425),
            ),
            // large value
            ("52763797000ns", Duration::from_nanos(52_763_797_000)),
            // more than 9 digits after decimal point
            ("0.3333333333333333333h", Duration::from_secs(20 * 60)),
            // 9007199254740993 = 1<<53+1 cannot be stored precisely in a float64
            (
                "9007199254740993ns",
                Duration::from_nanos(9_007_199_254_740_993),
            ),
            // largest duration that can be represented by i64 in nanoseconds
            (
                "9223372036854775807ns",
                Duration::from_nanos(9_223_372_036_854_775_807),
            ),
            (
                "9223372036854775.807us",
                Duration::from_nanos(9_223_372_036_854_775_807),
            ),
            (
                "9223372036s854ms775us807ns",
                Duration::from_nanos(9_223_372_036_854_775_807),
            ),
            // huge string
            ("0.100000000000000000000h", Duration::from_secs(6 * 60)),
            // This value tests the first overflow check in leadingFraction
            ("0.830103483285477580700h", Duration::new(2988, 372_539_827)),
        ];

        for (input, expected) in parse_duration_tests {
            match parse_duration_from_golang_string(input) {
                Ok(duration) => {
                    assert_eq!(
                        duration, expected,
                        "ParseDuration({}) = {:?}, want {:?}",
                        input, duration, expected
                    );
                }
                Err(e) => {
                    panic!("ParseDuration({}) returned error: {}", input, e);
                }
            }
        }
    }
}
