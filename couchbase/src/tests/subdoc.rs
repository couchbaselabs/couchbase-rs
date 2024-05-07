use crate::tests::mock::{MockCore, NAME};
use crate::{
    ArrayAddUniqueSpecOptions, ArrayAppendSpecOptions, ArrayInsertSpecOptions,
    ArrayPrependSpecOptions, Cluster, CouchbaseError, DecrementSpecOptions, IncrementSpecOptions,
    InsertSpecOptions, MutateInSpec, RemoveSpecOptions, ReplaceSpecOptions, UpsertSpecOptions,
};
use mockall::predicate::eq;
use serde_json::to_vec;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

macro_rules! treemap {
    () => {
        BTreeMap::new()
    };
    ($($k:expr => $v:expr),+ $(,)?) => {
        {
            let mut m = BTreeMap::new();
            $(
                m.insert($k, $v);
            )+
            m
        }
    };
}

#[test]
fn replace_works() {
    let result =
        MutateInSpec::replace("name", "52-Mile Air", ReplaceSpecOptions::default()).unwrap();

    let actual = MutateInSpec::Replace {
        path: "name".into(),
        value: "52-Mile Air".as_bytes().to_vec(),
        xattr: ReplaceSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
#[should_panic]
fn replace_panic_for_wrong_input() {
    let map = treemap!(
        Some("a") => 2,
        Some("b") => 4,
        None => 6,
    );
    let _ = MutateInSpec::replace("name", map, ReplaceSpecOptions::default()).unwrap();
}

#[test]
fn insert_works() {
    let result = MutateInSpec::insert("name", "52-Mile Air", InsertSpecOptions::default()).unwrap();

    let actual = MutateInSpec::Insert {
        path: "name".into(),
        value: "52-Mile Air".as_bytes().to_vec(),
        create_path: InsertSpecOptions::default().create_path,
        xattr: InsertSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
#[should_panic]
fn insert_panic_for_wrong_input() {
    let map = treemap!(
        Some("a") => 2,
        Some("b") => 4,
        None => 6,
    );
    let _ = MutateInSpec::insert("name", map, InsertSpecOptions::default()).unwrap();
}

#[test]
fn upsert_works() {
    let result = MutateInSpec::upsert("name", "52-Mile Air", UpsertSpecOptions::default()).unwrap();

    let actual = MutateInSpec::Upsert {
        path: "name".into(),
        value: "52-Mile Air".as_bytes().to_vec(),
        create_path: UpsertSpecOptions::default().create_path,
        xattr: UpsertSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
#[should_panic]
fn upsert_panic_for_wrong_input() {
    let map = treemap!(
        Some("a") => 2,
        Some("b") => 4,
        None => 6,
    );
    let _ = MutateInSpec::upsert("name", map, UpsertSpecOptions::default()).unwrap();
}

#[test]
fn array_add_unique_works() {
    let result =
        MutateInSpec::array_add_unique("name", "52-Mile Air", ArrayAddUniqueSpecOptions::default())
            .unwrap();

    let actual = MutateInSpec::ArrayAddUnique {
        path: "name".into(),
        value: "52-Mile Air".as_bytes().to_vec(),
        create_path: ArrayAddUniqueSpecOptions::default().create_path,
        xattr: ArrayAddUniqueSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
#[should_panic]
fn array_add_unique_panic_for_wrong_input() {
    let map = treemap!(
        Some("a") => 2,
        Some("b") => 4,
        None => 6,
    );
    let _ =
        MutateInSpec::array_add_unique("name", map, ArrayAddUniqueSpecOptions::default()).unwrap();
}

#[test]
fn remove_works() {
    let result = MutateInSpec::remove("name", RemoveSpecOptions::default()).unwrap();

    let actual = MutateInSpec::Remove {
        path: "name".into(),
        xattr: ReplaceSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
fn increment_works() {
    let result = MutateInSpec::increment("name", 10, IncrementSpecOptions::default()).unwrap();

    let actual = MutateInSpec::Counter {
        path: "name".into(),
        delta: 10i64,
        create_path: IncrementSpecOptions::default().create_path,
        xattr: IncrementSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
fn decrement_works() {
    let result = MutateInSpec::decrement("name", 10, DecrementSpecOptions::default()).unwrap();

    let actual = MutateInSpec::Counter {
        path: "name".into(),
        delta: -10i64,
        create_path: IncrementSpecOptions::default().create_path,
        xattr: IncrementSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
#[should_panic]
fn array_append_panic_for_wrong_input() {
    let map = treemap!(
        Some("a") => 2,
        Some("b") => 4,
        None => 6,
    );
    let _ =
        MutateInSpec::array_append("fish", vec![map], ArrayAppendSpecOptions::default()).unwrap();
}
#[test]
#[should_panic]
fn array_append_panic_for_no_content() {
    let empty_content: Vec<&str> = vec![];
    let _ = MutateInSpec::array_append("fish", empty_content, ArrayAppendSpecOptions::default())
        .unwrap();
}

#[test]
fn array_append_works() {
    let result =
        MutateInSpec::array_append("fish", vec!["clownfish"], ArrayAppendSpecOptions::default())
            .unwrap();

    let actual = MutateInSpec::ArrayAppend {
        path: "fish".into(),
        value: "clownfish".as_bytes().to_vec(),
        create_path: ArrayAppendSpecOptions::default().create_path,
        xattr: ArrayAppendSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}
#[test]
#[should_panic]
fn array_prepend_panic_for_wrong_input() {
    let map = treemap!(
        Some("a") => 2,
        Some("b") => 4,
        None => 6,
    );
    let _ =
        MutateInSpec::array_prepend("fish", vec![map], ArrayPrependSpecOptions::default()).unwrap();
}
#[test]
#[should_panic]
fn array_prepend_panic_for_no_content() {
    let empty_content: Vec<&str> = vec![];
    let _ = MutateInSpec::array_prepend("fish", empty_content, ArrayPrependSpecOptions::default())
        .unwrap();
}

#[test]
fn array_prepend_works() {
    let result = MutateInSpec::array_prepend(
        "fish",
        vec!["clownfish"],
        ArrayPrependSpecOptions::default(),
    )
    .unwrap();

    let actual = MutateInSpec::ArrayPrepend {
        path: "fish".into(),
        value: "clownfish".as_bytes().to_vec(),
        create_path: ArrayPrependSpecOptions::default().create_path,
        xattr: ArrayPrependSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}

#[test]
#[should_panic]
fn array_insert_panic_for_wrong_input() {
    let map = treemap!(
        Some("a") => 2,
        Some("b") => 4,
        None => 6,
    );
    let _ =
        MutateInSpec::array_insert("fish", vec![map], ArrayInsertSpecOptions::default()).unwrap();
}
#[test]
#[should_panic]
fn array_insert_panic_for_no_content() {
    let empty_content: Vec<&str> = vec![];
    let _ = MutateInSpec::array_insert("fish", empty_content, ArrayInsertSpecOptions::default())
        .unwrap();
}

#[test]
fn array_insert_works() {
    let result =
        MutateInSpec::array_insert("fish", vec!["clownfish"], ArrayInsertSpecOptions::default())
            .unwrap();

    let actual = MutateInSpec::ArrayPrepend {
        path: "fish".into(),
        value: "clownfish".as_bytes().to_vec(),
        create_path: ArrayInsertSpecOptions::default().create_path,
        xattr: ArrayInsertSpecOptions::default().xattr,
    };
    assert_eq!(result, actual);
}
