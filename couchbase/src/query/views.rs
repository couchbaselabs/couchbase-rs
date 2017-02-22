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

    pub fn is_development(&self) -> bool {
        self.development
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

#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_empty_query() {
        let query = ViewQuery::from("design", "view");
        assert_eq!("design", query.design());
        assert_eq!("view", query.view());
        assert_eq!("", query.params());
        assert_eq!(false, query.is_development());
    }

    #[test]
    fn test_development_enabled() {
        assert_eq!(true,
                   ViewQuery::from("foo", "bar").development(true).is_development());
    }

    #[test]
    fn test_limit() {
        assert_eq!("limit=10", ViewQuery::from("foo", "bar").limit(10).params());
    }

    #[test]
    fn test_skip() {
        assert_eq!("skip=2", ViewQuery::from("foo", "bar").skip(2).params());
    }

    #[test]
    fn test_reduce() {
        assert_eq!("reduce=true",
                   ViewQuery::from("foo", "bar").reduce(true).params());
    }

    #[test]
    fn test_group() {
        assert_eq!("group=true",
                   ViewQuery::from("foo", "bar").group(true).params());
    }

    #[test]
    fn test_debug() {
        assert_eq!("debug=true",
                   ViewQuery::from("foo", "bar").debug(true).params());
    }

    #[test]
    fn test_descending() {
        assert_eq!("descending=true",
                   ViewQuery::from("foo", "bar").descending(true).params());
    }

    #[test]
    fn test_group_level() {
        assert_eq!("group_level=3",
                   ViewQuery::from("foo", "bar").group_level(3).params());
    }

    #[test]
    fn test_stale() {
        assert_eq!("stale=ok",
                   ViewQuery::from("foo", "bar").stale(Stale::True).params());
        assert_eq!("stale=false",
                   ViewQuery::from("foo", "bar").stale(Stale::False).params());
        assert_eq!("stale=update_after",
                   ViewQuery::from("foo", "bar").stale(Stale::UpdateAfter).params());
    }

    #[test]
    fn test_on_error() {
        assert_eq!("on_error=stop",
                   ViewQuery::from("foo", "bar").on_error(OnError::Stop).params());
        assert_eq!("on_error=continue",
                   ViewQuery::from("foo", "bar").on_error(OnError::Continue).params());
    }

    #[test]
    fn test_parameter_combination() {
        assert_eq!("limit=5&skip=3",
                   ViewQuery::from("foo", "bar").skip(3).limit(5).params());
    }

    #[test]
    fn test_startkey_docid() {
        assert_eq!("startkey_docid=somedoc",
                   ViewQuery::from("foo", "bar").startkey_docid("somedoc").params());
    }

    #[test]
    fn test_endkey_docid() {
        assert_eq!("endkey_docid=somedoc",
                   ViewQuery::from("foo", "bar").endkey_docid("somedoc").params());
    }

    #[test]
    fn test_urlencoding() {
        let query = ViewQuery::from("foo", "bar")
            .startkey_docid("??>>what?)")
            .endkey_docid("some!doc)");
        assert_eq!("startkey_docid=%3F%3F%3E%3Ewhat%3F%29&endkey_docid=some%21doc%29",
                   query.params());
    }

}
