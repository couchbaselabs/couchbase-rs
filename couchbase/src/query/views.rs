use std::fmt;
use url::form_urlencoded;

#[derive(Debug)]
pub enum ViewResult {
    Meta(ViewMeta),
    Row(ViewRow),
}

#[derive(Debug)]
pub struct ViewMeta {
    pub inner: String,
}

#[derive(Debug)]
pub struct ViewRow {
    pub id: String,
    pub value: String,
    pub key: String,
}

pub enum Stale {
    True,
    False,
    UpdateAfter,
}

impl fmt::Display for Stale {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Stale::True => write!(f, "ok"),
            Stale::False => write!(f, "false"),
            Stale::UpdateAfter => write!(f, "update_after"),
        }

    }
}

pub enum OnError {
    Stop,
    Continue,
}

impl fmt::Display for OnError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            OnError::Stop => write!(f, "stop"),
            OnError::Continue => write!(f, "continue"),
        }

    }
}

const PARAM_REDUCE_OFFSET: usize = 0;
const PARAM_LIMIT_OFFSET: usize = 1;
const PARAM_SKIP_OFFSET: usize = 2;
const PARAM_STALE_OFFSET: usize = 3;
const PARAM_GROUPLEVEL_OFFSET: usize = 4;
const PARAM_GROUP_OFFSET: usize = 5;
const PARAM_ONERROR_OFFSET: usize = 6;
const PARAM_DEBUG_OFFSET: usize = 7;
const PARAM_DESCENDING_OFFSET: usize = 8;
const PARAM_INCLUSIVEEND_OFFSET: usize = 9;
// TODO const PARAM_STARTKEY_OFFSET: usize = 10;
const PARAM_STARTKEYDOCID_OFFSET: usize = 11;
// TODO const PARAM_ENDKEY_OFFSET: usize = 12;
const PARAM_ENDKEYDOCID_OFFSET: usize = 13;
// TODO const PARAM_KEY_OFFSET: usize = 14;

const NUM_PARAMS: usize = 15;

pub struct ViewQuery {
    design: String,
    view: String,
    development: bool,
    params: Vec<Option<(&'static str, String)>>,
}

impl ViewQuery {
    pub fn from<S>(design: S, view: S) -> ViewQuery
        where S: Into<String>
    {
        ViewQuery {
            design: design.into(),
            view: view.into(),
            development: false,
            params: vec![None; NUM_PARAMS],
        }
    }

    pub fn development(mut self, development: bool) -> ViewQuery {
        self.development = development;
        self
    }

    pub fn reduce(mut self, reduce: bool) -> ViewQuery {
        self.params[PARAM_REDUCE_OFFSET] = Some(("reduce", format!("{}", reduce)));
        self
    }

    pub fn limit(mut self, limit: u32) -> ViewQuery {
        self.params[PARAM_LIMIT_OFFSET] = Some(("limit", format!("{}", limit)));
        self
    }

    pub fn skip(mut self, skip: u32) -> ViewQuery {
        self.params[PARAM_SKIP_OFFSET] = Some(("skip", format!("{}", skip)));
        self
    }

    pub fn group(mut self, group: bool) -> ViewQuery {
        self.params[PARAM_GROUP_OFFSET] = Some(("group", format!("{}", group)));
        self
    }

    pub fn group_level(mut self, group_level: usize) -> ViewQuery {
        self.params[PARAM_GROUPLEVEL_OFFSET] = Some(("group_level", format!("{}", group_level)));
        self
    }

    pub fn inclusive_end(mut self, inclusive_end: bool) -> ViewQuery {
        self.params[PARAM_INCLUSIVEEND_OFFSET] = Some(("inclusive_end",
                                                       format!("{}", inclusive_end)));
        self
    }

    pub fn stale(mut self, stale: Stale) -> ViewQuery {
        self.params[PARAM_STALE_OFFSET] = Some(("stale", format!("{}", stale)));
        self
    }

    pub fn on_error(mut self, on_error: OnError) -> ViewQuery {
        self.params[PARAM_ONERROR_OFFSET] = Some(("on_error", format!("{}", on_error)));
        self
    }

    pub fn debug(mut self, debug: bool) -> ViewQuery {
        self.params[PARAM_DEBUG_OFFSET] = Some(("debug", format!("{}", debug)));
        self
    }

    pub fn descending(mut self, descending: bool) -> ViewQuery {
        self.params[PARAM_DESCENDING_OFFSET] = Some(("descending", format!("{}", descending)));
        self
    }

    pub fn startkey_docid<S>(mut self, id: S) -> ViewQuery
        where S: Into<String>
    {
        self.params[PARAM_STARTKEYDOCID_OFFSET] = Some(("startkey_docid",
                                                        format!("{}", id.into())));
        self
    }

    pub fn endkey_docid<S>(mut self, id: S) -> ViewQuery
        where S: Into<String>
    {
        self.params[PARAM_ENDKEYDOCID_OFFSET] = Some(("endkey_docid", format!("{}", id.into())));
        self
    }

    pub fn design(&self) -> &str {
        &self.design
    }

    pub fn view(&self) -> &str {
        &self.view
    }

    pub fn params(&self) -> String {
        self.params
            .iter()
            .filter(|v| v.is_some())
            .fold(form_urlencoded::Serializer::new(String::new()),
                  |mut acc, v| {
                      let &(name, ref value) = v.as_ref().unwrap();
                      acc.append_pair(name, &value);
                      acc
                  })
            .finish()
    }
}
