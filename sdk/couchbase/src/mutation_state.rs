use couchbase_core::queryx::query_options::{ScanVectorEntry, SparseScanVectors};
use serde::de::{MapAccess, Visitor};
use serde::ser::{SerializeMap, SerializeStruct};
use serde::{de, Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::fmt;
use std::fmt::Write;

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct MutationToken {
    pub(crate) token: couchbase_core::mutationtoken::MutationToken,
    pub(crate) bucket_name: String,
}

impl MutationToken {
    pub(crate) fn new(
        token: couchbase_core::mutationtoken::MutationToken,
        bucket_name: String,
    ) -> Self {
        Self { token, bucket_name }
    }

    pub fn partition_id(&self) -> u16 {
        self.token.vbid()
    }

    pub fn partition_uuid(&self) -> u64 {
        self.token.vbuuid()
    }

    pub fn sequence_number(&self) -> u64 {
        self.token.seqno()
    }

    pub fn bucket_name(&self) -> &str {
        &self.bucket_name
    }
}

#[derive(Default, Clone, Debug, Eq, PartialEq)]
pub struct MutationState {
    // There's a bit of duplication between key and value here but that allows us to recreate
    // the tokens easily.
    tokens: HashMap<MutationStateKey, couchbase_core::mutationtoken::MutationToken>,
}

#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash)]
struct MutationStateKey {
    bucket_name: String,
    vbid: u16,
}

impl Serialize for MutationStateKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.bucket_name, &self.vbid)?;
        map.end()
    }
}

impl MutationState {
    pub fn new() -> Self {
        Self {
            tokens: HashMap::default(),
        }
    }

    pub fn new_with_tokens(tokens: Vec<MutationToken>) -> Self {
        let mut state = Self {
            tokens: HashMap::default(),
        };
        for token in tokens {
            state.push_token(token);
        }

        state
    }

    pub fn push_token(&mut self, token: MutationToken) {
        let key = MutationStateKey {
            bucket_name: token.bucket_name,
            vbid: token.token.vbid(),
        };

        if let Some(entry) = self.tokens.get(&key) {
            if entry.seqno() < token.token.seqno() {
                self.tokens.insert(key, token.token);
            }
        } else {
            self.tokens.insert(key, token.token);
        }
    }

    pub fn tokens(&self) -> Vec<MutationToken> {
        self.tokens
            .iter()
            .map(|(key, token)| MutationToken::new(token.clone(), key.bucket_name.clone()))
            .collect()
    }
}

impl From<MutationState> for HashMap<String, SparseScanVectors> {
    fn from(value: MutationState) -> Self {
        let mut buckets: HashMap<String, SparseScanVectors> = HashMap::default();
        for (key, token) in value.tokens {
            let bucket = buckets.entry(key.bucket_name.clone()).or_default();
            bucket.insert(
                key.vbid.to_string(),
                ScanVectorEntry::new(token.seqno(), token.vbuuid().to_string()),
            );
        }

        buckets
    }
}

impl Serialize for MutationState {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_map(None)?;
        let mut buckets: HashMap<String, HashMap<String, (u64, String)>> = HashMap::new();

        for (key, token) in &self.tokens {
            let bucket = buckets.entry(key.bucket_name.clone()).or_default();
            bucket.insert(
                key.vbid.to_string(),
                (token.seqno(), token.vbuuid().to_string()),
            );
        }

        for (bucket_name, vbuckets) in buckets {
            state.serialize_entry(&bucket_name, &vbuckets)?;
        }

        state.end()
    }
}

impl<'de> Deserialize<'de> for MutationState {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct MutationStateVisitor;

        impl<'de> Visitor<'de> for MutationStateVisitor {
            type Value = MutationState;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("a map of bucket names to vbucket entries")
            }

            fn visit_map<M>(self, mut map: M) -> Result<Self::Value, M::Error>
            where
                M: MapAccess<'de>,
            {
                let mut tokens = HashMap::new();

                while let Some((bucket_name, vbuckets)) =
                    map.next_entry::<String, HashMap<String, (u64, String)>>()?
                {
                    for (vbid, (seqno, vbuuid)) in vbuckets {
                        let key = MutationStateKey {
                            bucket_name: bucket_name.clone(),
                            vbid: vbid.parse().map_err(de::Error::custom)?,
                        };
                        let token = couchbase_core::mutationtoken::MutationToken::new(
                            key.vbid,
                            vbuuid.parse().map_err(de::Error::custom)?,
                            seqno,
                        );
                        tokens.insert(key, token);
                    }
                }

                Ok(MutationState { tokens })
            }
        }

        deserializer.deserialize_map(MutationStateVisitor)
    }
}

#[macro_export]
macro_rules! mutation_state {
    ( $($x:expr),+ ) => {
        {
            let mut state = MutationState::new();
            $(
                state.push_token($x);
            )*
            state
        }
    };
}

#[cfg(test)]
mod tests {
    use crate::mutation_state::MutationState;
    use crate::mutation_state::MutationToken;

    #[test]
    fn serialization() {
        let mutation_state = mutation_state! {
             MutationToken::new(
                couchbase_core::mutationtoken::MutationToken::new(1, 1234, 1),
                "default".to_string(),
                ),
             MutationToken::new(
                couchbase_core::mutationtoken::MutationToken::new(25, 5678, 10),
                "beer-sample".to_string(),
                )
        };

        let serialized = serde_json::to_string(&mutation_state).unwrap();
        assert!(serialized.contains(r#""default":{"1":[1,"1234"]}"#));
        assert!(serialized.contains(r#""beer-sample":{"25":[10,"5678"]}"#));
    }

    #[test]
    fn deserialization() {
        let json = r#"{"default":{"1":[1,"1234"]},"beer-sample":{"25":[10,"5678"]}}"#;

        let mutation_state: MutationState = serde_json::from_str(json).unwrap();
        let tokens = mutation_state.tokens();
        assert!(tokens.contains(&MutationToken::new(
            couchbase_core::mutationtoken::MutationToken::new(1, 1234, 1),
            "default".to_string(),
        )));
        assert!(tokens.contains(&MutationToken::new(
            couchbase_core::mutationtoken::MutationToken::new(25, 5678, 10),
            "beer-sample".to_string(),
        )));
    }
}
