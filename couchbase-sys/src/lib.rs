#![doc(html_root_url = "https://docs.rs/couchbase-sys/1.0.0-alpha.5")]
#![allow(non_camel_case_types)]
#![allow(non_upper_case_globals)]
#![allow(non_snake_case)]

#[cfg(feature = "link-static")]
#[link(name = "openssl", kind = "static")]
extern crate openssl_sys;

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

// Function to test (Keep this function as is)
pub fn add(a: i32, b: i32) -> i32 {
    a + b
}

// Unit test
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_add_positive_numbers() {
        assert_eq!(add(2, 3), 5);
    }

    #[test]
    fn test_add_negative_numbers() {
        assert_eq!(add(-2, -3), -5);
    }
}
