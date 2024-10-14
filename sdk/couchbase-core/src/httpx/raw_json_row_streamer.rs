use crate::httpx::error::ErrorKind::Generic;
use crate::httpx::error::Result as HttpxResult;
use crate::httpx::json_row_stream::JsonRowStream;
use futures::{Stream, StreamExt, TryStreamExt};
use futures_core::FusedStream;
use serde_json::Value;
use std::cmp::{PartialEq, PartialOrd};
use std::collections::HashMap;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(PartialEq, Eq, PartialOrd, Debug)]
enum RowStreamState {
    Start = 0,
    Rows = 1,
    PostRows = 2,
    End = 3,
}

pub struct RawJsonRowStreamer {
    stream: JsonRowStream,
    rows_attrib: String,
    buffered_row: Vec<u8>,
    attribs: HashMap<String, Value>,
    state: RowStreamState,
}

impl RawJsonRowStreamer {
    pub fn new(stream: JsonRowStream, rows_attrib: impl Into<String>) -> Self {
        Self {
            stream,
            rows_attrib: rows_attrib.into(),
            buffered_row: Vec::new(),
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
                            let maybe_row = maybe_row?;
                            let str_row = std::str::from_utf8(&maybe_row)
                                .map_err(|e| Generic { msg: e.to_string() })?;
                            // if there are no more rows, immediately move to post-rows
                            if str_row == "]" {
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

    pub fn has_more_rows(&self) -> bool {
        if self.state < RowStreamState::Rows {
            return false;
        }

        if self.state > RowStreamState::Rows {
            return false;
        }

        !self.buffered_row.is_empty()
    }

    pub async fn read_row(&mut self) -> HttpxResult<Option<Vec<u8>>> {
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
            let maybe_row = maybe_row?;
            let str_row =
                std::str::from_utf8(&maybe_row).map_err(|e| Generic { msg: e.to_string() })?;
            if str_row == "]" {
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

    pub async fn read_prelude(&mut self) -> HttpxResult<Vec<u8>> {
        self.begin().await?;
        Ok(serde_json::to_vec(&self.attribs)?)
    }

    pub async fn read_epilog(&mut self) -> HttpxResult<Vec<u8>> {
        self.end().await?;
        Ok(serde_json::to_vec(&self.attribs)?)
    }
}

impl FusedStream for RawJsonRowStreamer {
    fn is_terminated(&self) -> bool {
        matches!(self.state, RowStreamState::End) || matches!(self.state, RowStreamState::PostRows)
    }
}

impl Stream for RawJsonRowStreamer {
    type Item = HttpxResult<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.state < RowStreamState::Rows {
            return Poll::Ready(Some(Err(Generic {
                msg: "Unexpected parsing state during read rows".to_string(),
            }
            .into())));
        }

        // Check if we've already read everything
        if self.state >= RowStreamState::PostRows {
            return Poll::Ready(None);
        }

        let row = self.buffered_row.clone();

        let this = self.get_mut();

        match this.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(stream_row))) => {
                let str_row =
                    std::str::from_utf8(&stream_row).map_err(|e| Generic { msg: e.to_string() })?;
                if str_row == "]" {
                    this.state = RowStreamState::PostRows;
                } else {
                    this.buffered_row = stream_row;
                }
                Poll::Ready(Some(Ok(row)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                this.state = RowStreamState::End;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}
