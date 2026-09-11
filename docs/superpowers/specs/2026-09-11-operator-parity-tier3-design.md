# Operator parity tier 3 — design

**Status:** in progress 2026-09-11 (bd rxRust-bom). Continues the tier 1/2 spec
(`2026-09-07-operator-parity-tier1-design.md`).

## Why

`missing_features.md` is organised by the ReactiveX categories and is now
essentially complete. Compared against RxJS 7's actual operator list, a few
real operators and several RxJS-named entry points are still missing.

## Real operators

| Operator | RxJS | Semantics |
|---|---|---|
| `switch_scan(seed, f)` | `switchScan` | Like `merge_scan`, but each source item unsubscribes the previous inner; inner emissions become the accumulator. Completes when the source and the current inner complete. |
| `window_toggle(openings, closing_selector)` | `windowToggle` | `buffer_toggle` emitting a `Subject` window per opening instead of a `Vec`; closing emission or completion closes its window; source completion completes open windows. |
| `window_when(closing_selector)` | `windowWhen` | `buffer_when` with windows: the first opens at subscribe; a closing emission completes the window and opens the next (closing completion keeps the window open, as in RxJS). |
| `debounce_when(selector)` | `debounce(durationSelector)` | Each item starts `selector(&item)`, cancelling the previous one; the item is emitted when its duration emits or completes, unless a newer item arrived. Source completion flushes the pending item. |
| `sample_time(period)` / `sample_time_with` | `sampleTime` | `sample` driven by an interval on the context's scheduler. |
| `replay_subject_with_window(capacity, window)` | `ReplaySubject(n, windowTime)` | Replay buffer that also drops items older than `window`. |

## RxJS-named entry points

Thin methods delegating to existing operators: `merge_map` (= `flat_map`),
`merge_with`, `zip_with`, `race_with`, `switch_all`, `exhaust_all`,
`combine_latest_with` (tuple form), `to_vec` (= `collect::<Vec<_>>()`). Plus
`#[doc(alias = "...")]` RxJS names on new items so rustdoc search finds them.
`range` is not added: `from_iter(a..b)` is idiomatic Rust (doc alias only).

## Verification

Unit tests per operator in the module, following tiers 1/2 (`Local`
subjects, `TestCtx` for time), doctests on every method, the full matrix
(stable, nightly all-features, clippy on both, fmt, wasm), and
`missing_features.md` / `guide/operators.md` / CHANGELOG updates.
