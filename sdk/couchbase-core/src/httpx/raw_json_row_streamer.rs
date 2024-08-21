use crate::httpx::error::ErrorKind::Generic;
use crate::httpx::error::Result as HttpxResult;
use crate::httpx::json_row_stream::JsonRowStream;
use bytes::Bytes;
use futures::{Stream, StreamExt, TryStreamExt};
use serde_json::Value;
use std::cmp::{PartialEq, PartialOrd};
use std::collections::HashMap;

#[derive(PartialEq, Eq, PartialOrd, Debug)]
enum RowStreamState {
    Start = 0,
    Rows = 1,
    PostRows = 2,
    End = 3,
}

pub struct RawJsonRowStreamer<S>
where
    S: Stream,
{
    stream: JsonRowStream<S>,
    rows_attrib: String,
    buffered_row: String,
    attribs: HashMap<String, Value>,
    state: RowStreamState,
}

impl<S> RawJsonRowStreamer<S>
where
    S: Stream<Item = HttpxResult<Bytes>> + Unpin,
{
    pub fn new(stream: JsonRowStream<S>, rows_attrib: String) -> Self {
        Self {
            stream,
            rows_attrib: rows_attrib.to_string(),
            buffered_row: String::new(),
            attribs: HashMap::new(),
            state: RowStreamState::Start,
        }
    }

    async fn begin(&mut self) -> HttpxResult<()> {
        if self.state != RowStreamState::Start {
            return Err(Generic {
                msg: "Unexpected parsing state during begin".to_string(),
            }
            .into());
        }

        let first = match self.stream.next().await {
            Some(result) => result?,
            None => {
                return Err(Generic {
                    msg: "Expected first line to be non-empty".to_string(),
                }
                .into())
            }
        };

        if &first[..] != b"{" {
            return Err(Generic {
                msg: "Expected an opening brace for the result".to_string(),
            }
            .into());
        }
        loop {
            match self.stream.next().await {
                Some(mut item) => {
                    let mut item =
                        String::from_utf8(item?).map_err(|e| Generic { msg: e.to_string() })?;
                    if item.is_empty() || item == "}" {
                        self.state = RowStreamState::End;
                        break;
                    }
                    if item.contains(&self.rows_attrib) {
                        if let Some(mut maybe_row) = self.stream.next().await {
                            let maybe_row = String::from_utf8(maybe_row?)
                                .map_err(|e| Generic { msg: e.to_string() })?;
                            // if there are no more rows, immediately move to post-rows
                            if maybe_row == "]" {
                                self.state = RowStreamState::PostRows;
                                break;
                            } else {
                                // We can't peek, so buffer the first row
                                self.buffered_row = maybe_row;
                            }
                        }
                        self.state = RowStreamState::Rows;
                        break;
                    }

                    // Wrap the line in a JSON object to deserialize
                    item = format!("{{{}}}", item);
                    let json_value: HashMap<String, Value> =
                        serde_json::from_str(&item).map_err(|e| Generic { msg: e.to_string() })?;

                    // Save the attribute for the metadata
                    for (k, v) in json_value {
                        self.attribs.insert(k, v);
                    }
                }
                None => {
                    self.state = RowStreamState::End;
                    break;
                }
            }
        }

        Ok(())
    }

    pub fn has_more_rows(&mut self) -> bool {
        if self.state < RowStreamState::Rows {
            return false;
        }

        if self.state > RowStreamState::Rows {
            return false;
        }

        !self.buffered_row.is_empty()
    }

    pub async fn read_row(&mut self) -> HttpxResult<Option<String>> {
        if self.state < RowStreamState::Rows {
            return Err(Generic {
                msg: "Unexpected parsing state during read rows".to_string(),
            }
            .into());
        }

        // If we've already read all rows or rows is null, we return None
        if self.state > RowStreamState::Rows {
            return Ok(None);
        }

        let row = self.buffered_row.clone();

        if let Some(mut maybe_row) = self.stream.next().await {
            let maybe_row =
                String::from_utf8(maybe_row?).map_err(|e| Generic { msg: e.to_string() })?;
            if maybe_row == "]" {
                self.state = RowStreamState::PostRows;
            } else {
                self.buffered_row = maybe_row;
            }
        }

        Ok(Some(row))
    }

    async fn end(&mut self) -> HttpxResult<()> {
        if self.state < RowStreamState::PostRows {
            return Err(Generic {
                msg: "Unexpected parsing state during end".to_string(),
            }
            .into());
        }

        // Check if we've already read everything
        if self.state > RowStreamState::PostRows {
            return Ok(());
        }

        loop {
            match self.stream.next().await {
                Some(item) => {
                    let mut item =
                        String::from_utf8(item?).map_err(|e| Generic { msg: e.to_string() })?;

                    if item == "}" || item.is_empty() {
                        self.state = RowStreamState::End;
                        break;
                    }
                    item = format!("{{{}}}", item);
                    let json_value: HashMap<String, Value> =
                        serde_json::from_str(&item).map_err(|e| Generic { msg: e.to_string() })?;
                    for (k, v) in json_value {
                        self.attribs.insert(k, v);
                    }
                }
                None => {
                    self.state = RowStreamState::End;
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn read_prelude(&mut self) -> HttpxResult<String> {
        self.begin().await?;
        let as_string = serde_json::to_string(&self.attribs)?;
        Ok(as_string)
    }

    pub async fn read_epilog(&mut self) -> HttpxResult<String> {
        self.end().await?;
        let as_string = serde_json::to_string(&self.attribs)?;
        Ok(as_string)
    }
}
