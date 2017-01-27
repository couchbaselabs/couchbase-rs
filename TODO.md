# TODO

This list is actually much bigger than what can be found here, since there is so
much work to do ;-)

This is the must-have list before a 0.1 I think...


**API**
 - add cluster abstraction with open bucket and everything
 - Return Result for cluster and open bucket if it actually worked out
 - add all kinds of ops to the API

**Perf**
 - right now we don't unlock in the callback, which makes it serialized,
   go fix that so its faster
