# Operator Parity, Tier 1: Design

Date: 2026-09-07
Status: approved in conversation, pending implementation plan
Branch: `feat/operator-parity-tier1`

## Goal

Bring rxRust's operator surface to parity with the everyday RxJS 7 operators that people reach for first, fix one correctness bug in `BehaviorSubject` that blocks a faithful `ReplaySubject`, and leave `missing_features.md` accurate.

## Non-goals

Deferred to tier 2: `window`, `window_count`, `window_time`, `buffer_toggle`, `buffer_when`, `delay_when`, `merge_scan`, `expand`, `partition`, `sequence_equal`, `generate`, `iif`, `from_callback`, `using`, `single`, `on_error_resume_next`, time-windowed replay. Out of scope entirely: backpressure operators (RxJS has none; `Stream` interop covers pull) and `from_event` (belongs in the Leptos bridge).

## Conventions for every operator

These follow `guide/advanced/custom_operators.md` and the existing code.

- **Location.** One file per operator in `src/ops/<name>.rs`, flat. Register with `pub mod` and `pub use` in `src/ops.rs`. New public types (`Notification`, `Timestamped`, `TimeInterval`, `TimeoutError`, `ReplaySubject`, `AsyncSubject`) are re-exported from `src/prelude.rs`.
- **Surface.** Instance operators are methods on the `Observable` trait in `src/observable.rs`, built with `self.transform(|source| Op { .. })`. Static creators are methods on `ObservableFactory` in `src/factory.rs`, built with `Self::lift`.
- **Environment agnostic.** Implement `ObservableType` and `CoreObservable<C>` generic over `Context`. Shared mutable state goes through `C::RcMut<T>`. Timing operators take the context scheduler by default and offer a `_with(.., scheduler)` variant, matching `delay` / `debounce`.
- **Owned items.** Operators that buffer or replay items require owned items, following the `buffer_count` precedent for bounds on `Item<'a>`.
- **Naming.** RxJS names in snake_case. Where RxJS overloads one name, use the repo's existing suffix conventions: `_with` for explicit scheduler, `_or` for a default value, `_or_else` for a lazy value, `_observables` for N-ary static forms.
- **Docs.** Rustdoc with a `#[doc(alias = "rxjsName")]`, a marble sketch where it helps, and a doctest. Add the operator to `guide/operators.md`. Tick or correct its row in `missing_features.md`. Add a line under Unreleased in `CHANGELOG.md`.
- **Tests.** In the operator file, using `#[rxrust_macro::test(local)]` and a `Shared` case where the operator holds state. Time-based operators use `TestCtx` and `TestScheduler` virtual time, never wall-clock. Each operator covers: basic behavior, empty source, error propagation, completion, unsubscribe and resource cleanup.
- **Gate.** `cargo test`, `cargo +nightly test --all-features`, `cargo +nightly clippy --all-targets --all-features -- -D warnings`, `cargo +nightly fmt --all -- --check`.
- **Delivery.** Two pull requests against the fork's `master`, in this order: PR 1a (utility and filtering) then PR 1b (subjects, multicast, error handling, timing). Commit style `feat(ops.<name>): ...`.

## PR 1a: utility and filtering operators

Each entry gives the signature shape, semantics, and edge cases.

### `every(predicate) -> bool`

Alias `all`. Emits `false` and completes at the first item failing the predicate, unsubscribing the source. Emits `true` on source completion. Empty source emits `true`. Errors propagate.

### `ignore_elements()`

Drops every item and forwards only error and completion. The item type is unchanged so the operator composes without turbofish.

### `element_at(index)` and `element_at_or(index, default)`

Emits the item at the zero-based index, then completes. If the source is shorter, `element_at` completes empty and `element_at_or` emits the default. Composed from `skip`, `take`, and `default_if_empty`; the return types are the composed operator types.

### `is_empty() -> bool`

Emits `false` and completes on the first item, unsubscribing the source. Emits `true` on completion of an empty source.

### `find(predicate)` and `find_index(predicate) -> usize`

`find` emits the first matching item then completes; composed from `filter` and `take`. `find_index` is its own operator that tracks the zero-based position and emits the index of the first match. Both complete empty when nothing matches.

### `end_with(values: Vec<Item>)`

Mirror of `start_with`. After the source completes, emits the values in order, then completes. Not emitted on error.

### `materialize()` and `dematerialize()`

`Notification<Item, Err>` is an enum with `Next(Item)`, `Error(Err)`, and `Complete`, deriving `Debug`, `Clone`, `PartialEq`, `Eq`. `materialize` emits every event as a `Notification` and completes after the terminal one; its error type is `Infallible`. `dematerialize` takes a source whose items are `Notification<T, E>` and replays them as real events, stopping at the first `Error` or `Complete` and unsubscribing the source.

### `timestamp()` and `time_interval()`

`timestamp` wraps each item as `Timestamped<Item> { value, timestamp: Instant }` using `crate::scheduler::Instant` so it works on wasm. `time_interval` wraps each item as `Elapsed<Item> { value, interval: Duration }`, where the interval is the time since the previous emission, or since subscription for the first. The value struct is named `Elapsed` rather than `TimeInterval` so the operator struct can keep the conventional operator name. Both structs derive `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`.

### `race(other)` and `race_observables(iter)`

Subscribes to all sources. The first source to emit any event wins: the others are unsubscribed and the winner is mirrored from that event on. `race` is the binary instance method; `race_observables` is the N-ary factory over homogeneous boxed observables, following `merge_observables`.

### `fork_join_observables(iter) -> Vec<Item>`

Subscribes to all sources and, once every source has completed, emits one `Vec` holding each source's last value in input order, then completes. If any source completes without emitting, the result completes empty. An error from any source is forwarded immediately and the rest are unsubscribed.

### `combine_latest_observables(iter) -> Vec<Item>` and `zip_observables(iter) -> Vec<Item>`

N-ary factory forms of the existing binary operators, over homogeneous sources. `combine_latest_observables` emits a snapshot `Vec` of the latest value from each source whenever any source emits, once every source has emitted at least once; items must be `Clone`. `zip_observables` emits the nth item from each source as a `Vec` and completes when any source completes with no pending pair.

### `throw_if_empty(err_fn: FnOnce() -> Err)`

If the source completes without emitting, calls `err_fn` and emits that error. Otherwise transparent.

## PR 1b: subjects, multicast, error handling, timing

### Fix: `BehaviorSubject` value must be shared across clones

Today `BehaviorSubject` stores `value: Item` inline, so every clone carries its own copy. A clone taken before `next(5)` still replays `0` to late subscribers, which is wrong and breaks the state-store pattern the cookbook recommends. Move the value into shared state behind the context's `RcMut`, so the struct becomes `BehaviorSubject<P, V>` with `V: RcDerefMut<Target = Item>`. `Behavior::peek` and `next_by` read and write the shared cell. This changes the public type parameters and is recorded as a breaking change in the changelog. A regression test subscribes through a clone taken before an emission and expects the latest value.

### `ReplaySubject`

`ReplaySubject<P, B>` wraps `Subject<P>` plus shared state `B` holding a `VecDeque<Item>` and an optional capacity. On `next`, push and trim to capacity, then forward. On subscribe, replay the buffer in order, then attach; if the subject has already terminated, forward the terminal event after replaying. Factories: `replay_subject(capacity)` and `replay_subject_unbounded()`, both on `ObservableFactory`. Items must be `Clone`. No `_mut_ref` variant.

### `AsyncSubject`

`AsyncSubject<P, V>` wraps `Subject<P>` plus shared `Option<Item>`. `next` stores the value without forwarding. `complete` forwards the stored value, if any, to all subscribers and then completes them. `error` forwards the error and discards the value. Subscribers arriving after completion receive the value and completion. Factory: `async_subject()`.

### Generalize `ConnectableObservable` over the subject type

`ConnectableObservable<S, P>` and `RefCount<S, P, ConnPtr>` hardcode `Subject<P>`. Generalize both over a subject type `Sub` that is `Observer + CoreObservable + Clone`, so any subject can back multicasting. `publish()` keeps returning the plain `Subject` form and its public type alias stays stable. Add `publish_replay(capacity)`, `publish_behavior(initial)`, and `publish_last()` on top of the generalized `multicast`.

### `share()` and `share_replay(capacity)`

`share` is `publish().ref_count()`. `share_replay` is `publish_replay(capacity).ref_count()`. With `ref_count`, the source is unsubscribed when the last subscriber leaves and re-subscribed for the next one; the same `ReplaySubject` persists across reconnects, so late subscribers still see the last `capacity` values. This matches RxJS `share` with a replay connector and `resetOnRefCountZero: true`, and the doc states it plainly.

### `catch_error(f: FnMut(Err) -> Fallback)`

On error, calls `f` and subscribes the fallback observable with the downstream observer, mirroring it from then on. The fallback must have the same item type; the output error type is the fallback's. The subscription holds the source handle plus a dynamic slot for the fallback, so unsubscribing during the fallback unsubscribes it. Modeled on `retry`.

### `timeout` family

Core operator `Timeout<S, Sch, F>` with an error factory `F: FnOnce() -> Err`. A timer starts at subscribe and is reset on every item. If it fires, the source is unsubscribed and the error is emitted. Completion or error from the source cancels the timer. Public surface:

- `timeout(duration)` requires `Err: From<TimeoutError>`.
- `timeout_or_else(duration, f)` takes an explicit error factory.
- `timeout_with(duration, scheduler)` and `timeout_or_else_with(duration, f, scheduler)` for an explicit scheduler.

`TimeoutError` is a unit struct deriving `Debug`, `Clone`, `Copy`, `PartialEq`, `Eq`, `Default`, implementing `Display` and `std::error::Error`.

### `repeat(count)` and `repeat_forever()`

Resubscribes to the source on completion. `count` is the total number of subscriptions; `repeat(0)` completes immediately and `repeat(1)` is transparent. Errors propagate. Unsubscribing stops further resubscription. The source must be `Clone`, as with `retry`. `repeat_forever` on a synchronous source loops until unsubscribed, as in RxJS.

### `exhaust_map(f)`

Maps each outer item to an inner observable. While an inner is active, outer items are dropped. When the inner completes, the next outer item starts a new one. Completes when the outer has completed and no inner is active. Errors from either side propagate. Modeled on `switch_map`.

### `audit(selector)`, `audit_time(duration)`, `audit_time_with(duration, scheduler)`

`audit` emits the most recent item when the duration observable returned by `selector` emits, then waits for the next item to start a new window. If the existing `throttle` with a trailing-only `ThrottleEdge` has exactly these semantics, `audit` is a thin wrapper over it; the tests below decide that. If throttle's trailing edge also emits a leading value, `audit` gets its own implementation. Tests: an item at t=0 with a 100ms audit emits that item, or a later one, at t=100; silence emits nothing; completion during an open window waits for the window to end, then emits the pending item and completes, as in RxJS.

## Documentation and bookkeeping

- `missing_features.md`: correct the rows that claim `every`/`all`, `ignore_elements`, and `element_at` exist; add rows for every operator above.
- `guide/operators.md`: add each operator to its category table.
- `CHANGELOG.md`: list additions under Unreleased and call out the `BehaviorSubject` type change.
- `guide/cookbook/state_store.md`: re-run its doctests after the `BehaviorSubject` fix.

## Testing strategy

Unit tests live in each operator file per the conventions above. `tests/v1_integration.rs` gains a few end-to-end chains that combine new operators, such as `share_replay` feeding two subscribers, `timeout` with `catch_error` fallback, and `materialize` piped into `dematerialize`. The `BehaviorSubject` regression test lives in `src/subject/behavior_subject.rs`.

## Risks

- Generalizing `ConnectableObservable` may ripple through the `Connectable` trait and its type aliases. Mitigation: land the generalization as its own commit with the existing tests green before adding new subjects.
- The `Item<'a>` generic associated type makes buffering operators need owned-item bounds. Mitigation: copy the bounds from `buffer_count` rather than inventing new ones.
- `repeat_forever` on synchronous sources is an intentional infinite loop; the doc and test make that explicit.
