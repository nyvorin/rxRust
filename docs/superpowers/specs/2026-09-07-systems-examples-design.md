# Systems Examples: Design

Date: 2026-09-07
Status: approved in conversation (sub-project 2 of the fork roadmap)
Branch: `feat/systems-examples`, stacked on `feat/operator-parity-tier2b`

## Goal

Show rxRust where it earns its keep in real systems, with runnable programs under `examples/` that double as tests. Each example is self-contained, uses only the crate's dev-dependencies, prints a readable trace when run with `cargo run --example <name>`, and carries a `#[cfg(test)]` module so `cargo test --examples` proves its behavior.

## Examples

1. **`tcp_line_pipeline`** (Shared context, multi-thread tokio). A TCP server accepts one connection and turns the socket's lines into an observable with `from_stream`, so the socket is read at the pace the pipeline consumes it. The pipeline parses `name=value` metrics, drops malformed lines, batches with `buffer_count`, and emits per-batch averages. A client task writes a fixed script. Asserts the batch averages.
2. **`debounced_search`** (Local context, tokio local runtime). Keystrokes arrive on a `Subject`; the pipeline `debounce`s, skips repeats with `distinct_until_changed`, and `switch_map`s into a simulated async search so stale searches are cancelled. Asserts that only the final query produced results and that earlier searches were abandoned.
3. **`retry_backoff_poller`** (Local context). A flaky fetch fails twice then succeeds. A custom `RetryPolicy` implements exponential backoff with a cap; `timeout` guards a hung fetch and `catch_error` supplies a fallback. Asserts the value, the attempt count, and the delays the policy chose.
4. **`state_store`** (Local context). A Redux-style store: actions on a `Subject`, a `scan` reducer, `share_replay(1)` so late subscribers get the current state, and selectors with `distinct_until_changed`. Asserts the selector streams for a scripted action sequence.
5. **`event_sourcing`** (Local context). Account events are routed with `group_by` into per-aggregate folds (`scan`) whose `last` values are the rebuilt balances, while `materialize` produces an audit log of every notification. Asserts the balances and the audit log.

## Conventions

- Each example exposes `pub fn run() -> ...` (or `async fn run()`), a `main` that prints the result, and tests in the same file that call `run()`.
- No new runtime dependencies; tokio's `net` and `io-util` features are enabled for dev-dependencies only, for the socket example.
- Timing-based examples use generous windows and assert only on ordering and counts, never on elapsed wall-clock durations.
- CI runs `cargo test --examples` in the stable and nightly test jobs.
- `README.md` gains an "Examples" section listing the five programs.
