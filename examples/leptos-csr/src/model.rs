//! View-independent reactive models.
//!
//! Each function wires an rxRust pipeline between Leptos signals and returns
//! the signals. The subscriptions belong to the reactive owner that called
//! the function (`to_signal`), so a component that creates a model also
//! disposes it.

use std::{convert::Infallible, time::Duration};

use leptos::prelude::*;
use rx_leptos::prelude::*;
use rxrust::{context::Context, prelude::*};

/// A boxed, single-threaded observable of `T` that never errors. Backends
/// return this so the models stay free of generic bounds.
pub type Stream<T> = Local<<Local<()> as Context>::BoxedCoreObservable<'static, T, Infallible>>;

/// Signals of a debounced, cancelling search box.
pub struct Typeahead {
  /// What the user typed. Write this from the input.
  pub query: RwSignal<String>,
  /// Results of the most recent settled query.
  pub results: ReadSignal<Vec<String>>,
  /// `true` while a search is in flight.
  pub pending: ReadSignal<bool>,
  /// Searches started so far.
  pub searches: ReadSignal<u32>,
  /// Searches whose results arrived (the rest were cancelled).
  pub delivered: ReadSignal<u32>,
}

/// Debounce `query`, drop repeats, run `backend` for each settled query and
/// cancel the previous search when a new one starts.
pub fn typeahead(
  debounce: Duration, backend: impl FnMut(String) -> Stream<Vec<String>> + 'static,
) -> Typeahead {
  let query = RwSignal::new(String::new());
  let (pending, set_pending) = signal(false);
  let (searches, set_searches) = signal(0u32);
  let (delivered, set_delivered) = signal(0u32);

  let results = to_signal(
    from_signal(query)
      .debounce(debounce)
      .distinct_until_changed()
      .tap(move |_| {
        set_pending.set(true);
        set_searches.update(|n| *n += 1);
      })
      .switch_map(backend)
      .tap(move |_| {
        set_pending.set(false);
        set_delivered.update(|n| *n += 1);
      }),
    Vec::new(),
  );

  Typeahead { query, results, pending, searches, delivered }
}

/// Signals of a stopwatch.
pub struct Stopwatch {
  /// Set `true` to run, `false` to pause.
  pub running: RwSignal<bool>,
  /// Bump to restart from zero.
  pub reset: RwSignal<u32>,
  /// Ticks counted since the last reset.
  pub ticks: ReadSignal<u64>,
}

/// Count `tick` periods while `running` is `true`; restart on `reset`.
pub fn stopwatch(tick: Duration) -> Stopwatch {
  let running = RwSignal::new(false);
  let reset = RwSignal::new(0u32);

  let ticks = to_signal(
    from_signal(reset).switch_map(move |_| {
      from_signal(running)
        .switch_map(move |on| {
          if on {
            Local::interval(tick).map(|_| 1u64).box_it()
          } else {
            Local::from_iter(Vec::<u64>::new()).box_it()
          }
        })
        .scan(0u64, |total, n| total + n)
        .start_with(vec![0])
    }),
    0,
  );

  Stopwatch { running, reset, ticks }
}

/// Latest pointer position over `target`, throttled to one update per
/// `throttle`.
#[cfg(target_arch = "wasm32")]
pub fn mouse_position(target: &web_sys::EventTarget, throttle: Duration) -> ReadSignal<(i32, i32)> {
  use wasm_bindgen::JsCast;

  to_signal(
    from_event(target, "mousemove")
      .throttle_time(throttle, ThrottleEdge::leading())
      .map(|event: web_sys::Event| {
        let mouse: web_sys::MouseEvent = event.unchecked_into();
        (mouse.client_x(), mouse.client_y())
      }),
    (0, 0),
  )
}
