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
                    if self.state < RowStreamState::PostRows && item.contains(&self.rows_attrib) {
                        if let Some(mut maybe_row) = self.stream.next().await {
                            let maybe_row = maybe_row?;
                            let str_row = std::str::from_utf8(&maybe_row)
                                .map_err(|e| Generic { msg: e.to_string() })?;
                            // if there are no more rows, immediately move to post-rows
                            if str_row == "]" {
                                self.state = RowStreamState::PostRows;
                                // Read the rest of the metadata.
                                continue;
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
        if self.state == RowStreamState::Rows {
            return true;
        }

        !self.buffered_row.is_empty()
    }

    pub async fn read_prelude(&mut self) -> HttpxResult<Vec<u8>> {
        self.begin().await?;
        Ok(serde_json::to_vec(&self.attribs)?)
    }

    pub fn epilog(&mut self) -> HttpxResult<Vec<u8>> {
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
                msg: "unexpected parsing state during read rows".to_string(),
            }
            .into())));
        }

        let row = self.buffered_row.clone();

        let this = self.get_mut();

        match this.stream.poll_next_unpin(cx) {
            Poll::Ready(Some(Ok(stream_row))) => {
                if this.state == RowStreamState::PostRows {
                    let mut item = String::from_utf8(stream_row).map_err(|e| Generic {
                        msg: format!("failed to parse stream epilogue to string {}", &e),
                    })?;

                    if item == "}" || item.is_empty() {
                        this.state = RowStreamState::End;
                        return Poll::Ready(None);
                    }

                    item = format!("{{{}}}", item);
                    let json_value: HashMap<String, Value> =
                        serde_json::from_str(&item).map_err(|e| Generic {
                            msg: format!("failed to parse stream epilogue to value {}", &e),
                        })?;
                    for (k, v) in json_value {
                        this.attribs.insert(k, v);
                    }

                    // TODO: I'm very suspicious of this.
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }

                let str_row = std::str::from_utf8(&stream_row).map_err(|e| Generic {
                    msg: format!("failed to parse stream row {}", &e),
                })?;
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
