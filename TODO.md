# TODO

This list is actually much bigger than what can be found here, since there is so
much work to do ;-)

This is the must-have list before a 0.1 I think...

**Build Process**
 - let user choose between statically compile or use from system by specifying
   a flag. Also figure out the right default (system?)

**LCB Integration**
 - Right now the FFI is generated via libbindgen on demand. pretty neat but
   the names suck (reexport?) and cargo test doesn't work since it gets hung
   up on some docs in lcb.. make this interface/abstraction more stable and
   supportable while still not having to fix up everything every time we switch
   to a new version

**Testing**
 - See how we can utilize the mock to test this stuff, since lcb also uses it
   for testing.

**Actual API**
 - Make the very basic KV ops work
