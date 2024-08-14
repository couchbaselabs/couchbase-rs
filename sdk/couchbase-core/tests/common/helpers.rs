use rand::{Rng, thread_rng};
use rand::distr::Alphanumeric;

pub fn generate_key() -> Vec<u8> {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(30)
        .map(char::from)
        .collect::<String>()
        .into_bytes()
}

pub fn generate_string_value(len: usize) -> Vec<u8> {
    thread_rng()
        .sample_iter(&Alphanumeric)
        .take(len)
        .map(char::from)
        .collect::<String>()
        .into_bytes()
}
