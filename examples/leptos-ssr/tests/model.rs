//! The `wire_*` pipelines run natively on a tokio local runtime with
//! `any_spawner` pointed at tokio, mirroring the browser's executor.

#![cfg(not(target_arch = "wasm32"))]

use std::{cell::RefCell, rc::Rc, time::Duration};

use any_spawner::Executor;
use leptos_ssr_example::model::{TypeaheadSinks, wire_stopwatch, wire_typeahead};
use reactive_graph::{
  owner::Owner,
  signal::{RwSignal, signal},
  traits::{GetUntracked, Set, Update},
};
use rxrust::prelude::*;
use tokio::time::sleep;

fn setup() -> Owner {
  let _ = Executor::init_tokio();
  Owner::new()
}

#[tokio::test(flavor = "local")]
async fn typeahead_debounces_typing_and_cancels_the_stale_search() {
  let owner = setup();
  let started = Rc::new(RefCell::new(Vec::new()));
  let log = started.clone();
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

  owner.with(|| {
    wire_typeahead(
      query,
      Duration::from_millis(40),
      move |q| {
        log.borrow_mut().push(q.clone());
        Local::of(vec![format!("hit:{q}")])
          .delay(Duration::from_millis(100))
          .box_it()
      },
      sinks,
    )
  });

  query.set("r".into());
  sleep(Duration::from_millis(10)).await;
  query.set("rx".into());
  sleep(Duration::from_millis(10)).await;
  query.set("rxr".into());
  sleep(Duration::from_millis(65)).await;
  assert_eq!(*started.borrow(), vec!["rxr"]);
  assert!(pending.get_untracked());

  query.set("rust".into());
  sleep(Duration::from_millis(55)).await;
  assert_eq!(*started.borrow(), vec!["rxr", "rust"]);
  assert!(results.get_untracked().is_empty(), "cancelled search must not deliver");

  sleep(Duration::from_millis(120)).await;
  assert_eq!(results.get_untracked(), vec!["hit:rust"]);
  assert!(!pending.get_untracked());
  assert_eq!(searches.get_untracked(), 2);
  assert_eq!(delivered.get_untracked(), 1);

  owner.cleanup();
}

#[tokio::test(flavor = "local")]
async fn stopwatch_runs_pauses_and_resets() {
  let owner = setup();
  let running = RwSignal::new(false);
  let reset = RwSignal::new(0u32);
  let (ticks, set_ticks) = signal(0u64);
  owner.with(|| wire_stopwatch(running, reset, Duration::from_millis(20), set_ticks));
  Executor::tick().await;
  assert_eq!(ticks.get_untracked(), 0);

  running.set(true);
  sleep(Duration::from_millis(110)).await;
  let after_run = ticks.get_untracked();
  assert!((3..=6).contains(&after_run), "expected about 5 ticks, got {after_run}");

  running.set(false);
  sleep(Duration::from_millis(30)).await;
  let paused = ticks.get_untracked();
  sleep(Duration::from_millis(80)).await;
  assert_eq!(ticks.get_untracked(), paused, "no ticks while paused");

  reset.update(|n| *n += 1);
  Executor::tick().await;
  assert_eq!(ticks.get_untracked(), 0, "reset restarts from zero");

  owner.cleanup();
}
