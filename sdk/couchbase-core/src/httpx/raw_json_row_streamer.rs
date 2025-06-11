use crate::httpx::decoder::{Decoder, Token};
use crate::httpx::error::Error;
use crate::httpx::error::Result as HttpxResult;
use futures::{stream, FutureExt, Stream, TryStreamExt};
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

pub struct RawJsonRowStreamer {
    stream: Decoder,
    rows_attrib: String,
    attribs: HashMap<String, Value>,
    state: RowStreamState,
}

pub enum RawJsonRowItem {
    Row(Vec<u8>),
    Metadata(Vec<u8>),
}

impl RawJsonRowStreamer {
    pub fn new(stream: Decoder, rows_attrib: impl Into<String>) -> Self {
        Self {
            stream,
            rows_attrib: rows_attrib.into(),
            attribs: HashMap::new(),
            state: RowStreamState::Start,
        }
    }

    async fn begin(&mut self) -> HttpxResult<()> {
        if self.state != RowStreamState::Start {
            return Err(Error::new_message_error(
                "unexpected parsing state during begin",
            ));
        }

        let first = self.stream.token().await?;

        if first != Token::Delim('{') {
            return Err(Error::new_message_error(
                "expected an opening brace for the result",
            ));
        }

        loop {
            if !self.stream.more().await {
                self.state = RowStreamState::End;
                break;
            }

            let token = self.stream.token().await?;
            let key = match token {
                Token::String(s) => s,
                _ => {
                    return Err(Error::new_message_error(
                        "expected a string key for the result",
                    ));
                }
            };

            if key == self.rows_attrib.as_str() {
                let token = self.stream.token().await?;
                match token {
                    Token::Delim('[') => {
                        self.state = RowStreamState::Rows;
                    }
                    Token::Value(v) => {
                        if &v == b"null" {
                            continue;
                        }

                        return Err(Error::new_message_error(
                            "expected an opening bracket for the rows",
                        ));
                    }
                    _ => {
                        return Err(Error::new_message_error(
                            "expected an opening bracket for the rows",
                        ));
                    }
                }

                if self.stream.more().await {
                    self.state = RowStreamState::Rows;
                    break;
                }

                // There are no rows so we can just read the remaining metadata now.
                let token = match self.stream.token().await {
                    Ok(t) => t,
                    Err(e) => return Err(e),
                };

                match token {
                    Token::Delim(']') => {}
                    _ => {
                        return Err(Error::new_message_error(
                            "expected closing ] for the result",
                        ));
                    }
                }

                self.state = RowStreamState::PostRows;
                continue;
            }

            let value = self.stream.decode().await?;
            let value = serde_json::from_slice(&value)
                .map_err(|e| Error::new_message_error(format!("failed to parse value: {}", e)))?;

            self.attribs.insert(key, value);
        }

        Ok(())
    }

    pub async fn has_more_rows(&mut self) -> bool {
        if self.state != RowStreamState::Rows {
            return false;
        }

        self.stream.more().await
    }

    pub async fn read_prelude(&mut self) -> HttpxResult<Vec<u8>> {
        self.begin().await?;
        serde_json::to_vec(&self.attribs)
            .map_err(|e| Error::new_message_error(format!("failed to read prelude: {}", e)))
    }

    pub fn epilog(&mut self) -> HttpxResult<Vec<u8>> {
        serde_json::to_vec(&self.attribs)
            .map_err(|e| Error::new_message_error(format!("failed to read epilogue: {}", e)))
    }

    pub async fn next(&mut self) -> Option<HttpxResult<RawJsonRowItem>> {
        if self.state == RowStreamState::End {
            return None;
        }

        loop {
            if self.state == RowStreamState::PostRows {
                let token = match self.stream.token().await {
                    Ok(t) => t,
                    Err(e) => return Some(Err(e)),
                };

                let key = match token {
                    Token::String(s) => s,
                    Token::Delim('}') => {
                        self.state = RowStreamState::End;

                        let metadata = match serde_json::to_vec(&self.attribs).map_err(|e| {
                            Error::new_message_error(format!("failed to encode metadata: {}", e))
                        }) {
                            Ok(m) => m,
                            Err(e) => return Some(Err(e)),
                        };

                        return Some(Ok(RawJsonRowItem::Metadata(metadata)));
                    }
                    _ => {
                        return Some(Err(Error::new_message_error(
                            "expected a string key for the result",
                        )));
                    }
                };

                let value = match self.stream.decode().await {
                    Ok(v) => v,
                    Err(e) => return Some(Err(e)),
                };

                let value = match serde_json::from_slice::<Value>(&value) {
                    Ok(v) => v,
                    Err(e) => {
                        return Some(Err(Error::new_message_error(format!(
                            "failed to parse value: {}",
                            e
                        ))))
                    }
                };

                self.attribs.insert(key, value);
                continue;
            }

            let row = match self.stream.decode().await {
                Ok(v) => v,
                Err(e) => return Some(Err(e)),
            };

            if !self.stream.more().await {
                let token = match self.stream.token().await {
                    Ok(t) => t,
                    Err(e) => return Some(Err(e)),
                };

                match token {
                    Token::Delim(']') => {}
                    _ => {
                        return Some(Err(Error::new_message_error(
                            "expected closing ] for the result",
                        )));
                    }
                }

                self.state = RowStreamState::PostRows;
            }

            return Some(Ok(RawJsonRowItem::Row(row)));
        }
    }

    pub fn into_stream(self) -> impl Stream<Item = HttpxResult<RawJsonRowItem>> {
        stream::unfold(self, |mut stream| async move {
            stream.next().await.map(|row| (row, stream))
        })
    }
}
