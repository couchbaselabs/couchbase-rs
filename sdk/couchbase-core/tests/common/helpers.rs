use rand::distr::Alphanumeric;
use rand::{rng, Rng};

pub fn generate_key() -> Vec<u8> {
    rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect::<String>()
        .into_bytes()
}

pub fn generate_string_value(len: usize) -> Vec<u8> {
    rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
        .into_bytes()
}
