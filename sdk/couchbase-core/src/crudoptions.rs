use std::sync::Arc;

use crate::memdx::datatype::DataTypeFlag;
use crate::memdx::durability_level::DurabilityLevel;
use crate::memdx::subdoc::{LookupInOp, MutateInOp, SubdocDocFlag};
use crate::retry::{RetryStrategy, DEFAULT_RETRY_STRATEGY};

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetMetaOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetMetaOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct UpsertOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) value: &'a [u8],
    pub(crate) flags: u32,
    pub(crate) datatype: DataTypeFlag,
    pub(crate) expiry: Option<u32>,
    pub(crate) preserve_expiry: Option<bool>,
    pub(crate) cas: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UpsertOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            flags: 0,
            datatype: DataTypeFlag::default(),
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn datatype(mut self, datatype: DataTypeFlag) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DeleteOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) cas: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DeleteOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetAndLockOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) lock_time: u32,
    pub(crate) collection_id: Option<u32>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetAndLockOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        lock_time: u32,
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            lock_time,
            collection_id: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn collection_id(mut self, collection_id: impl Into<Option<u32>>) -> Self {
        self.collection_id = collection_id.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetAndTouchOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) expiry: u32,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetAndTouchOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str, expiry: u32) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            expiry,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct UnlockOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) cas: u64,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> UnlockOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str, cas: u64) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            cas,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct TouchOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) expiry: u32,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> TouchOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str, expiry: u32) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            expiry,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AddOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) value: &'a [u8],
    pub(crate) flags: u32,
    pub(crate) datatype: DataTypeFlag,
    pub(crate) expiry: Option<u32>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> AddOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            flags: 0,
            datatype: DataTypeFlag::default(),
            expiry: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn datatype(mut self, datatype: DataTypeFlag) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct ReplaceOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) value: &'a [u8],
    pub(crate) flags: u32,
    pub(crate) datatype: DataTypeFlag,
    pub(crate) expiry: Option<u32>,
    pub(crate) preserve_expiry: Option<bool>,
    pub(crate) cas: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> ReplaceOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            flags: 0,
            datatype: DataTypeFlag::default(),
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: u32) -> Self {
        self.flags = flags;
        self
    }

    pub fn datatype(mut self, datatype: DataTypeFlag) -> Self {
        self.datatype = datatype;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct AppendOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) value: &'a [u8],
    pub(crate) cas: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> AppendOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct PrependOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) value: &'a [u8],
    pub(crate) cas: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> PrependOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        value: &'a [u8],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            value,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct IncrementOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) initial: Option<u64>,
    pub(crate) delta: Option<u64>,
    pub(crate) expiry: Option<u32>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> IncrementOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            initial: None,
            delta: None,
            expiry: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn initial(mut self, initial: impl Into<Option<u64>>) -> Self {
        self.initial = initial.into();
        self
    }

    pub fn delta(mut self, delta: impl Into<Option<u64>>) -> Self {
        self.delta = delta.into();
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct DecrementOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) initial: Option<u64>,
    pub(crate) delta: Option<u64>,
    pub(crate) expiry: Option<u32>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> DecrementOptions<'a> {
    pub fn new(key: &'a [u8], scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            initial: None,
            delta: None,
            expiry: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn initial(mut self, initial: impl Into<Option<u64>>) -> Self {
        self.initial = initial.into();
        self
    }

    pub fn delta(mut self, delta: impl Into<Option<u64>>) -> Self {
        self.delta = delta.into();
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct LookupInOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) ops: &'a [LookupInOp<'a>],
    pub(crate) flags: SubdocDocFlag,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> LookupInOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        ops: &'a [LookupInOp<'a>],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            ops,
            flags: SubdocDocFlag::empty(),
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: SubdocDocFlag) -> Self {
        self.flags = flags;
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct MutateInOptions<'a> {
    pub(crate) key: &'a [u8],
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) ops: &'a [MutateInOp<'a>],
    pub(crate) flags: SubdocDocFlag,
    pub(crate) expiry: Option<u32>,
    pub(crate) preserve_expiry: Option<bool>,
    pub(crate) cas: Option<u64>,
    pub(crate) durability_level: Option<DurabilityLevel>,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> MutateInOptions<'a> {
    pub fn new(
        key: &'a [u8],
        scope_name: &'a str,
        collection_name: &'a str,
        ops: &'a [MutateInOp<'a>],
    ) -> Self {
        Self {
            key,
            scope_name,
            collection_name,
            ops,
            flags: SubdocDocFlag::empty(),
            expiry: None,
            preserve_expiry: None,
            cas: None,
            durability_level: None,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn flags(mut self, flags: SubdocDocFlag) -> Self {
        self.flags = flags;
        self
    }

    pub fn expiry(mut self, expiry: impl Into<Option<u32>>) -> Self {
        self.expiry = expiry.into();
        self
    }

    pub fn preserve_expiry(mut self, preserve_expiry: impl Into<Option<bool>>) -> Self {
        self.preserve_expiry = preserve_expiry.into();
        self
    }

    pub fn cas(mut self, cas: impl Into<Option<u64>>) -> Self {
        self.cas = cas.into();
        self
    }

    pub fn durability_level(
        mut self,
        durability_level: impl Into<Option<DurabilityLevel>>,
    ) -> Self {
        self.durability_level = durability_level.into();
        self
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}

#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct GetCollectionIdOptions<'a> {
    pub(crate) scope_name: &'a str,
    pub(crate) collection_name: &'a str,
    pub(crate) retry_strategy: Arc<dyn RetryStrategy>,
}

impl<'a> GetCollectionIdOptions<'a> {
    pub fn new(scope_name: &'a str, collection_name: &'a str) -> Self {
        Self {
            scope_name,
            collection_name,
            retry_strategy: DEFAULT_RETRY_STRATEGY.clone(),
        }
    }

    pub fn retry_strategy(mut self, retry_strategy: Arc<dyn RetryStrategy>) -> Self {
        self.retry_strategy = retry_strategy;
        self
    }
}
