//! View-independent reactive models, structured for server rendering.
//!
//! Each model creates its signals immediately, so the server renders the
//! initial state, and wires its rxRust pipeline inside `Effect::new`, which
//! runs only in the browser. That matters: Leptos's tokio integration has no
//! `LocalSet`, so `spawn_local` panics on the server, and both rxRust's
//! `LocalScheduler` timers and `from_signal` change delivery use it.
//!
//! The `wire_*` functions are public so tests (and custom effects) can run
//! the pipelines directly.

use std::{convert::Infallible, time::Duration};

use leptos::prelude::*;
use rx_leptos::prelude::*;
use rxrust::{context::Context, prelude::*};

/// A boxed, single-threaded observable of `T` that never errors.
pub type Stream<T> = Local<<Local<()> as Context>::BoxedCoreObservable<'static, T, Infallible>>;

/// Signals of a debounced, cancelling search box.
pub struct Typeahead {
  pub query: RwSignal<String>,
  pub results: ReadSignal<Vec<String>>,
  pub pending: ReadSignal<bool>,
  pub searches: ReadSignal<u32>,
  pub delivered: ReadSignal<u32>,
}

/// Write side of a [`Typeahead`].
#[derive(Clone, Copy)]
pub struct TypeaheadSinks {
  pub results: WriteSignal<Vec<String>>,
  pub pending: WriteSignal<bool>,
  pub searches: WriteSignal<u32>,
  pub delivered: WriteSignal<u32>,
}

/// Create the signals now and wire the pipeline in the browser.
pub fn typeahead(
  debounce: Duration, backend: impl FnMut(String) -> Stream<Vec<String>> + Clone + 'static,
) -> Typeahead {
  let query = RwSignal::new(String::new());
  let (results, set_results) = signal(Vec::new());
  let (pending, set_pending) = signal(false);
  let (searches, set_searches) = signal(0u32);
  let (delivered, set_delivered) = signal(0u32);
  let sinks = TypeaheadSinks {
    results: set_results,
    pending: set_pending,
    searches: set_searches,
    delivered: set_delivered,
  };

  Effect::new(move |_| wire_typeahead(query, debounce, backend.clone(), sinks));

  Typeahead { query, results, pending, searches, delivered }
}

/// Debounce `query`, drop repeats, run `backend` per settled query and cancel
/// the previous search when a new one starts. Lives as long as the current
/// reactive owner.
pub fn wire_typeahead(
  query: RwSignal<String>, debounce: Duration,
  backend: impl FnMut(String) -> Stream<Vec<String>> + 'static, sinks: TypeaheadSinks,
) {
  query
    .to_observable()
    .debounce(debounce)
    .distinct_until_changed()
    .tap(move |_| {
      sinks.pending.set(true);
      sinks.searches.update(|n| *n += 1);
    })
    .switch_map(backend)
    .tap(move |_| {
      sinks.pending.set(false);
      sinks.delivered.update(|n| *n += 1);
    })
    .feed(sinks.results);
}

/// Signals of a stopwatch.
pub struct Stopwatch {
  pub running: RwSignal<bool>,
  pub reset: RwSignal<u32>,
  pub ticks: ReadSignal<u64>,
}

/// Create the signals now and wire the pipeline in the browser.
pub fn stopwatch(tick: Duration) -> Stopwatch {
  let running = RwSignal::new(false);
  let reset = RwSignal::new(0u32);
  let (ticks, set_ticks) = signal(0u64);

  Effect::new(move |_| wire_stopwatch(running, reset, tick, set_ticks));

  Stopwatch { running, reset, ticks }
}

/// Count `tick` periods while `running`; restart on `reset`.
pub fn wire_stopwatch(
  running: RwSignal<bool>, reset: RwSignal<u32>, tick: Duration, set_ticks: WriteSignal<u64>,
) {
  reset
    .to_observable()
    .switch_map(move |_| {
      running
        .to_observable()
        .switch_map(move |on| {
          if on {
            Local::interval(tick).map(|_| 1u64).box_it()
          } else {
            Local::from_iter(Vec::<u64>::new()).box_it()
          }
        })
        .scan(0u64, |total, n| total + n)
        .start_with(vec![0])
    })
    .feed(set_ticks);
}

/// Feed the throttled pointer position over `target` into `set_position`.
#[cfg(target_arch = "wasm32")]
pub fn wire_mouse(
  target: &web_sys::EventTarget, throttle: Duration, set_position: WriteSignal<(i32, i32)>,
) {
  use wasm_bindgen::JsCast;

  from_event(target, "mousemove")
    .throttle_time(throttle, ThrottleEdge::leading())
    .map(|event: web_sys::Event| {
      let mouse: web_sys::MouseEvent = event.unchecked_into();
      (mouse.client_x(), mouse.client_y())
    })
    .feed(set_position);
}
