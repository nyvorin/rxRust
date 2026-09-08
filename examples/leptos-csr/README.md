# rx-leptos CSR example

A Leptos 0.8 client-side app that drives its UI with rxRust pipelines through
[`rx-leptos`](../../crates/rx-leptos).

| Panel | Pipeline |
|-------|----------|
| Typeahead | `from_signal(query)` → `debounce` → `distinct_until_changed` → `switch_map(backend)` → `to_signal(results)`. Typing again cancels the in-flight search; a `pending` signal drives the spinner. |
| Stopwatch | `from_signal(reset)` → `switch_map(from_signal(running) → switch_map(interval or empty) → scan)` → `to_signal(ticks)`. Pausing stops the interval; reset restarts the fold. |
| Mouse tracker | `from_event(document, "mousemove")` → `throttle_time` → `map` → `to_signal(position)`. |

The reactive logic lives in `src/model.rs` with no view code, so it is tested
natively (`cargo test -p leptos-csr-example`) under a tokio local runtime, the
same way the app runs under the browser's microtask executor.

## Run it

```sh
cargo install trunk            # once
rustup target add wasm32-unknown-unknown
cd examples/leptos-csr
trunk serve --open
```

`cargo check -p leptos-csr-example --target wasm32-unknown-unknown` is what CI
runs; the app itself needs a browser.

Server-side rendering is deferred; see issue `rxRust-bnd` in `bd`.
