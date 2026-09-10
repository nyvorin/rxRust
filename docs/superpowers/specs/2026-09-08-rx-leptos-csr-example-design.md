# rx-leptos CSR example — design

**Status:** approved by the user on 2026-09-08 ("roll with CSR for now, note SSR for down the road"). SSR tracked as `bd` issue rxRust-bnd; this work is rxRust-gzn.

## Goal

A Leptos 0.8 client-side app that shows rxRust pipelines driving Leptos UI
through `crates/rx-leptos`, with the reactive logic testable natively.

## Shape

`examples/leptos-csr/` is a workspace package (`leptos-csr-example`,
`publish = false`) with a lib and a bin:

- `src/model.rs` — view-independent reactive models. Each returns plain signals
  and owns its subscriptions through `to_signal`, so they die with the reactive
  owner (a component) that created them.
  - `typeahead(debounce, backend) -> Typeahead { query, results, pending, searches, delivered }`
    `from_signal(query) → debounce → distinct_until_changed → tap(pending on, searches += 1) → switch_map(backend) → tap(pending off, delivered += 1) → to_signal`.
    `backend: FnMut(String) -> Stream<Vec<String>>` where `Stream<T>` is a boxed `Local` observable, so the app and the tests plug in different backends without generic bounds leaking into the model signature.
  - `stopwatch(tick) -> Stopwatch { running, reset, ticks }`
    `from_signal(reset) → switch_map(from_signal(running) → switch_map(interval | nothing) → scan(+) → start_with(0)) → to_signal`.
  - `mouse_position(target, throttle) -> ReadSignal<(i32, i32)>` (wasm only)
    `from_event(target, "mousemove") → throttle_time → map(client_x, client_y) → to_signal`.
- `src/app.rs` — `App`, `TypeaheadPanel`, `StopwatchPanel`, `MousePanel` components. The fake search backend delays 600 ms so cancellation is visible.
- `src/main.rs` — `mount_to_body(App)` on wasm; a one-line hint on native.
- `index.html` — trunk entry with minimal styling.
- `tests/model.rs` — native tests on a tokio local runtime with
  `any_spawner::Executor::init_tokio()`, the same executor shape the browser
  gives Leptos: typeahead debounce + cancellation + pending flag; stopwatch
  run/pause/resume/reset.

## Scheduling

Signal changes reach rxRust on the executor's next tick (`from_signal`);
rxRust timers run on the same executor (`LocalScheduler` uses
`wasm_bindgen_futures`/gloo timers on wasm and tokio local tasks natively).
No synchronous cross-talk between the two schedulers is assumed.

## Verification

- `cargo test -p leptos-csr-example` (native model tests)
- `cargo check -p leptos-csr-example --target wasm32-unknown-unknown`
- `cargo clippy -p leptos-csr-example --all-targets -- -D warnings`
- Manual: `trunk serve` in `examples/leptos-csr`.

CI adds the check and the tests to the `rx-leptos` job.

## Out of scope

SSR/hydration (rxRust-bnd), DOM-level component tests (need a browser
runner), publishing.
