# rx-leptos

Bridges between [rxRust](https://docs.rs/rxrust) observables and Leptos 0.8
signals, built on `reactive_graph` (the reactive core Leptos re-exports).

| Function | Direction | Notes |
|----------|-----------|-------|
| `from_signal(signal)` | signal → observable | Emits the current value on subscribe, then every change on the next executor tick (writes within a tick coalesce). Works with signals, memos and anything else implementing `Get`. No `effects` feature needed. |
| `to_signal(obs, initial)` / `to_signal_local` | observable → signal | The signal and the subscription belong to the current reactive owner and are disposed with it. |
| `use_observable(obs)` | observable → `ReadSignal<Option<T>>` | `None` until the first item. |
| `from_event(target, "click")` | DOM → observable | wasm only; removes the listener on unsubscribe. |

Everything uses rxRust's `Local` context: signals are single-threaded UI state.

## Outside Leptos

Leptos initialises the global `any_spawner` executor when it mounts or serves an
app. In plain tests, do it yourself:

```rust
let _ = any_spawner::Executor::init_futures_executor();
// ... write signals ...
any_spawner::Executor::poll_local(); // deliver pending changes
```

## Status

Spike. Not published; API may change.
