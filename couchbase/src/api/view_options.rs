use crate::CouchbaseResult;
use serde_derive::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Duration;

#[derive(Debug, Clone, Copy)]
pub enum ViewScanConsistency {
    NotBounded,
    RequestPlus,
    UpdateAfter,
}

#[derive(Debug, Clone, Copy)]
pub enum ViewOrdering {
    Ascending,
    Descending,
}

#[derive(Debug, Clone, Copy)]
pub enum ViewErrorMode {
    Continue,
    Stop,
}

#[derive(Debug, Clone, Deserialize, Serialize, Eq, PartialEq)]
pub enum DesignDocumentNamespace {
    Production,
    Development,
}

#[derive(Debug, Default)]
pub struct ViewOptions {
    pub(crate) timeout: Option<Duration>,
    pub(crate) scan_consistency: Option<ViewScanConsistency>,
    pub(crate) skip: Option<u32>,
    pub(crate) limit: Option<u32>,
    pub(crate) order: Option<ViewOrdering>,
    pub(crate) reduce: Option<bool>,
    pub(crate) group: Option<bool>,
    pub(crate) group_level: Option<u32>,
    pub(crate) key: Option<Value>,
    pub(crate) keys: Option<Value>,
    pub(crate) start_key: Option<Value>,
    pub(crate) end_key: Option<Value>,
    pub(crate) inclusive_end: Option<bool>,
    pub(crate) start_key_doc_id: Option<String>,
    pub(crate) end_key_doc_id: Option<String>,
    pub(crate) on_error: Option<ViewErrorMode>,
    pub(crate) debug: Option<bool>,
    pub(crate) namespace: Option<DesignDocumentNamespace>,
    pub(crate) raw: Option<serde_json::Map<String, Value>>,
}

impl ViewOptions {
    timeout!();

    pub(crate) fn form_data(&self) -> CouchbaseResult<Vec<(&str, String)>> {
        let mut form = vec![];
        if let Some(s) = self.scan_consistency {
            match s {
                ViewScanConsistency::NotBounded => form.push(("stale", "ok".into())),
                ViewScanConsistency::RequestPlus => form.push(("stale", "false".into())),
                ViewScanConsistency::UpdateAfter => form.push(("stale", "update_after".into())),
            }
        }
        if let Some(s) = self.skip {
            form.push(("skip", s.to_string()));
        }
        if let Some(l) = self.limit {
            form.push(("limit", l.to_string()));
        }
        if let Some(o) = self.order {
            match o {
                ViewOrdering::Ascending => form.push(("descending", "false".into())),
                ViewOrdering::Descending => form.push(("descending", "falstruee".into())),
            }
        }
        if let Some(r) = self.reduce {
            if r {
                form.push(("reduce", "true".into()));

                if let Some(g) = self.group {
                    if g {
                        form.push(("group", "true".into()));
                    }
                }
                if let Some(g) = self.group_level {
                    form.push(("group_level", g.to_string()));
                }
            } else {
                form.push(("reduce", "false".into()));
            }
        }
        if let Some(k) = &self.key {
            form.push(("key", k.to_string()));
        }
        if let Some(ks) = &self.keys {
            form.push(("keys", ks.to_string()));
        }
        if let Some(k) = &self.start_key {
            form.push(("start_key", k.to_string()));
        }
        if let Some(k) = &self.end_key {
            form.push(("end_key", k.to_string()));
        }
        if self.start_key.is_some() || self.end_key.is_some() {
            if let Some(i) = self.inclusive_end {
                match i {
                    true => form.push(("inclusive_end", "true".into())),
                    false => form.push(("inclusive_end", "false".into())),
                }
            }
        }
        if let Some(k) = &self.start_key_doc_id {
            form.push(("startkey_docid", k.into()));
        }
        if let Some(k) = &self.end_key_doc_id {
            form.push(("endkey_docid", k.into()));
        }
        if let Some(o) = &self.on_error {
            match o {
                ViewErrorMode::Continue => form.push(("on_error", "continue".into())),
                ViewErrorMode::Stop => form.push(("on_error", "stop".into())),
            }
        }
        if let Some(d) = self.debug {
            if d {
                form.push(("debug", "true".into()));
            }
        }
        if let Some(r) = &self.raw {
            for item in r {
                form.push((item.0, item.1.to_string()));
            }
        }

        Ok(form)
    }

    pub fn scan_consistency(mut self, consistency: ViewScanConsistency) -> Self {
        self.scan_consistency = Some(consistency);
        self
    }
    pub fn skip(mut self, skip: u32) -> Self {
        self.skip = Some(skip);
        self
    }
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }
    pub fn order(mut self, ordering: ViewOrdering) -> Self {
        self.order = Some(ordering);
        self
    }
    pub fn reduce(mut self, enabled: bool) -> Self {
        self.reduce = Some(enabled);
        self
    }
    pub fn group(mut self, enabled: bool) -> Self {
        self.group = Some(enabled);
        self
    }
    pub fn group_level(mut self, level: u32) -> Self {
        self.group_level = Some(level);
        self
    }
    pub fn key<T>(mut self, key: T) -> Self
    where
        T: serde::Serialize,
    {
        let k = match serde_json::to_value(key) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode key"),
        };
        self.key = Some(k);
        self
    }
    pub fn keys<T>(mut self, keys: Vec<T>) -> Self
    where
        T: serde::Serialize,
    {
        let ks = match serde_json::to_value(keys) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode keys"),
        };
        self.keys = Some(ks);
        self
    }
    pub fn start_key<T>(mut self, key: T) -> Self
    where
        T: serde::Serialize,
    {
        let k = match serde_json::to_value(key) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode start_key"),
        };
        self.start_key = Some(k);
        self
    }
    pub fn end_key<T>(mut self, key: T) -> Self
    where
        T: serde::Serialize,
    {
        let k = match serde_json::to_value(key) {
            Ok(val) => val,
            Err(_e) => panic!("Could not encode end_key"),
        };
        self.end_key = Some(k);
        self
    }
    pub fn inclusive_end(mut self, inclusive_end: bool) -> Self {
        self.inclusive_end = Some(inclusive_end);
        self
    }
    pub fn start_key_doc_id(mut self, doc_id: impl Into<String>) -> Self {
        self.start_key_doc_id = Some(doc_id.into());
        self
    }
    pub fn end_key_doc_id(mut self, doc_id: impl Into<String>) -> Self {
        self.end_key_doc_id = Some(doc_id.into());
        self
    }
    pub fn on_error(mut self, error_mode: ViewErrorMode) -> Self {
        self.on_error = Some(error_mode);
        self
    }
    pub fn debug(mut self, enabled: bool) -> Self {
        self.debug = Some(enabled);
        self
    }
    pub fn namespace(mut self, namespace: DesignDocumentNamespace) -> Self {
        self.namespace = Some(namespace);
        self
    }
    pub fn raw<T>(mut self, raw: T) -> Self
    where
        T: serde::Serialize,
    {
        let raw = match serde_json::to_value(raw) {
            Ok(Value::Object(a)) => a,
            Ok(_) => panic!("Only objects are allowed"),
            _ => panic!("Could not encode raw parameters"),
        };
        self.raw = Some(raw);
        self
    }
}
