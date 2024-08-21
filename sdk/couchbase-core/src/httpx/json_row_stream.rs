use crate::httpx::error::Result as HttpxResult;
use crate::httpx::json_row_parser::JsonRowParser;
use bytes::Bytes;
use futures::stream::{FusedStream, Stream};
use futures::StreamExt;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Debug)]
pub struct JsonRowStream<S>
where
    S: Stream,
{
    state: State,
    parser: JsonRowParser,
    stream: S,
}
#[derive(Debug, Eq, PartialEq)]
enum State {
    Collecting,
    Done,
}

impl<S> JsonRowStream<S>
where
    S: Stream<Item = HttpxResult<Bytes>> + Unpin,
{
    pub fn new(stream: S) -> Self {
        Self {
            state: State::Collecting,
            parser: JsonRowParser::new(2),
            stream,
        }
    }
}

impl<S> FusedStream for JsonRowStream<S>
where
    S: Stream<Item = HttpxResult<Bytes>> + Unpin,
{
    fn is_terminated(&self) -> bool {
        matches!(self.state, State::Done)
    }
}

impl<S> Stream for JsonRowStream<S>
where
    S: Stream<Item = HttpxResult<Bytes>> + Unpin,
{
    type Item = HttpxResult<Vec<u8>>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<HttpxResult<Vec<u8>>>> {
        let this = self.get_mut();
        loop {
            match this.state {
                State::Collecting => match this.parser.next() {
                    // We got a value from the parser, propagate it
                    Ok(Some(value)) => return Poll::Ready(Some(Ok(value))),
                    // The parser didn't return a value, so poll the I/O stream to see if it's done
                    Ok(None) => match Pin::new(&mut this.stream).poll_next(cx) {
                        // The I/O stream isn't done yet, but no data is available
                        Poll::Pending => return Poll::Pending,
                        // A chunk is ready from the I/O stream, push it to the parser
                        Poll::Ready(Some(Ok(chunk))) => {
                            this.parser.push(&chunk[..]);
                            continue;
                        }
                        // The I/O Stream is finished, and the parser returned None, we're done.
                        Poll::Ready(None) => return Poll::Ready(None),
                        // The I/O stream errored, propagate the error
                        Poll::Ready(Some(Err(e))) => {
                            this.state = State::Done;
                            return Poll::Ready(Some(Err(e)));
                        }
                    },
                    Err(e) => {
                        this.state = State::Done;
                        return Poll::Ready(Some(Err(e)));
                    }
                },
                State::Done => return Poll::Ready(None),
            }
        }
    }
}
