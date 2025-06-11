use serde::{Serialize, Serializer};

#[derive(Debug, Clone, Eq, PartialEq)]
#[non_exhaustive]
pub enum LookupInMacros {
    Document,
    ExpiryTime,
    CAS,
    SeqNo,
    LastModified,
    IsDeleted,
    ValueSizeBytes,
    RevId,
}

impl From<LookupInMacros> for String {
    fn from(macro_: LookupInMacros) -> Self {
        match macro_ {
            LookupInMacros::Document => "$document".to_string(),
            LookupInMacros::ExpiryTime => "$document.exptime".to_string(),
            LookupInMacros::CAS => "$document.CAS".to_string(),
            LookupInMacros::SeqNo => "$document.seqno".to_string(),
            LookupInMacros::LastModified => "$document.last_modified".to_string(),
            LookupInMacros::IsDeleted => "$document.deleted".to_string(),
            LookupInMacros::ValueSizeBytes => "$document.value_bytes".to_string(),
            LookupInMacros::RevId => "$document.revid".to_string(),
        }
    }
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
#[non_exhaustive]
pub enum MutateInMacros {
    #[serde(rename = "${Mutation.CAS}")]
    CAS,
    #[serde(rename = "${Mutation.seqno}")]
    SeqNo,
    #[serde(rename = "${Mutation.value_crc32c}")]
    ValueCrc32c,
}

pub(crate) static MUTATE_IN_MACROS: &[&[u8]] = &[
    br#""${Mutation.CAS}""#,
    br#""${Mutation.seqno}""#,
    br#""${Mutation.value_crc32c}""#,
];
