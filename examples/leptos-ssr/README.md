# rx-leptos SSR example

The [CSR example](../leptos-csr) rendered on the server with `leptos_axum` and
hydrated in the browser, built with `cargo leptos`.

## The one rule

Leptos's server integrations have no tokio `LocalSet`, so `spawn_local`
panics during server render. rxRust's `LocalScheduler` timers (`debounce`,
`delay`, `interval`, ...) and `from_signal`'s change delivery both use it.
So every model here:

1. creates its signals immediately, which is what the server renders, and
2. wires its rx pipeline inside `Effect::new`, which runs only in the browser,
   feeding those signals with `.feed(write_signal)`.

`tests/ssr.rs` renders `<App/>` to a string on a multi-threaded tokio runtime
(the shape of an axum handler) and checks the initial markup; a `spawn_local`
on that path would panic the test. `tests/model.rs` runs the `wire_*`
pipelines natively.

## Run it

```sh
cargo install cargo-leptos   # once
rustup target add wasm32-unknown-unknown
cd examples/leptos-ssr
cargo leptos serve           # http://127.0.0.1:3000
```

This package is excluded from the repository workspace because Leptos's `csr`
and `ssr` features must not be unified with the CSR example; build it from
this directory.
