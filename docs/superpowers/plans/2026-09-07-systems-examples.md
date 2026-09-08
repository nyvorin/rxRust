# Systems Examples Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Five runnable examples under `examples/` with embedded tests, wired into CI, per `docs/superpowers/specs/2026-09-07-systems-examples-design.md`.

**Architecture:** Each example is one file exposing `run()`, a printing `main`, and a `#[cfg(test)]` module. Behavior is proven by `cargo test --examples`; compile hygiene by the existing clippy `--all-targets` gate.

**Tech Stack:** rxrust from this branch, tokio (dev, with `net` + `io-util` added), futures (dev).

## Global Constraints

- Branch `feat/systems-examples`, based on `feat/operator-parity-tier2b`.
- Gate per task: `cargo test --examples`, `cargo +nightly clippy --all-targets --all-features -- -D warnings`, `cargo +nightly fmt --all`.
- Assertions on ordering/counts only; no elapsed-time assertions.

## Tasks

- [ ] **Task 0: dev features and CI.** Add `net`, `io-util` to the non-wasm tokio dev-dependency; add `cargo test --examples` to the `test` and `test-nightly` jobs in `.github/workflows/main.yml`. Commit `chore: run examples in CI`.
- [ ] **Task 1: `examples/tcp_line_pipeline.rs`.** Server on `127.0.0.1:0`, client script `cpu=10`, `bad line`, `cpu=20`, `mem=5`, `mem=7`, `cpu=30`, `mem=9`; pipeline `from_stream(lines).filter_map(parse).buffer_count(3).map(average)`; assert averages `[12.0 (10,20,5 -> 11.666..)]`, computed in the test from the same script. Commit `feat(examples): add tcp line pipeline example`.
- [ ] **Task 2: `examples/debounced_search.rs`.** Keystrokes `r`, `rx`, `rxr`, `rxru`, `rxrust` 5ms apart, `debounce(30ms)`, `distinct_until_changed`, `switch_map(from_future(search))` with a 40ms simulated search; assert results only for `rxrust` and that `searches_started == 1`. Commit `feat(examples): add debounced search example`.
- [ ] **Task 3: `examples/retry_backoff_poller.rs`.** `ExponentialBackoff { base: 10ms, factor: 2, max_attempts: 5 }` implementing `RetryPolicy`; fetch fails twice; assert value `"ok"`, attempts `3`, delays `[10ms, 20ms]`; a second run with a permanently failing fetch hits `catch_error`'s fallback after 5 attempts. Commit `feat(examples): add retry with backoff example`.
- [ ] **Task 4: `examples/state_store.rs`.** `Action::{Increment, Decrement, SetName(String)}`, `State { count, name }`; store via `scan` + `share_replay(1)`; selectors `count` and `name` with `distinct_until_changed`; late subscriber sees the current state. Assert selector sequences. Commit `feat(examples): add state store example`.
- [ ] **Task 5: `examples/event_sourcing.rs`.** `Event::{Deposited, Withdrawn}` with account ids; `group_by(account)` + per-group `scan` + `last`; `materialize` audit log; assert balances `{a: 70, b: 25}` and the audit log length. Commit `feat(examples): add event sourcing example`.
- [ ] **Task 6: README "Examples" section, full matrix, push, PR against `feat/operator-parity-tier2b`.**
