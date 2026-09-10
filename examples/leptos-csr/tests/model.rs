//! The models run natively on a tokio local runtime, with `any_spawner`
//! pointed at tokio the way Leptos points it at the browser's microtasks.

#![cfg(not(target_arch = "wasm32"))]

use std::{cell::RefCell, rc::Rc, time::Duration};

use any_spawner::Executor;
use leptos_csr_example::model::{stopwatch, typeahead};
use reactive_graph::{
  owner::Owner,
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

  let model = owner.with(|| {
    typeahead(Duration::from_millis(40), move |q| {
      log.borrow_mut().push(q.clone());
      Local::of(vec![format!("hit:{q}")])
        .delay(Duration::from_millis(100))
        .box_it()
    })
  });

  // Three keystrokes inside the debounce window: one search.
  model.query.set("r".into());
  sleep(Duration::from_millis(10)).await;
  model.query.set("rx".into());
  sleep(Duration::from_millis(10)).await;
  model.query.set("rxr".into());
  sleep(Duration::from_millis(65)).await;
  assert_eq!(*started.borrow(), vec!["rxr"]);
  assert!(model.pending.get_untracked());
  assert!(model.results.get_untracked().is_empty());

  // A new query while "rxr" is in flight cancels it.
  model.query.set("rust".into());
  sleep(Duration::from_millis(55)).await;
  assert_eq!(*started.borrow(), vec!["rxr", "rust"]);
  assert!(model.results.get_untracked().is_empty(), "cancelled search must not deliver");

  sleep(Duration::from_millis(120)).await;
  assert_eq!(model.results.get_untracked(), vec!["hit:rust"]);
  assert!(!model.pending.get_untracked());
  assert_eq!(model.searches.get_untracked(), 2);
  assert_eq!(model.delivered.get_untracked(), 1);

  owner.cleanup();
}

#[tokio::test(flavor = "local")]
async fn typeahead_ignores_a_repeated_query() {
  let owner = setup();
  let started = Rc::new(RefCell::new(Vec::new()));
  let log = started.clone();

  let model = owner.with(|| {
    typeahead(Duration::from_millis(20), move |q| {
      log.borrow_mut().push(q.clone());
      Local::of(vec![q]).box_it()
    })
  });

  model.query.set("a".into());
  sleep(Duration::from_millis(40)).await;
  model.query.set("ab".into());
  sleep(Duration::from_millis(5)).await;
  model.query.set("a".into());
  sleep(Duration::from_millis(40)).await;
  assert_eq!(*started.borrow(), vec!["a"], "same settled query is not searched twice");
  assert_eq!(model.results.get_untracked(), vec!["a"]);

  owner.cleanup();
}

#[tokio::test(flavor = "local")]
async fn stopwatch_runs_pauses_resumes_and_resets() {
  let owner = setup();
  let model = owner.with(|| stopwatch(Duration::from_millis(20)));
  Executor::tick().await;
  assert_eq!(model.ticks.get_untracked(), 0);

  model.running.set(true);
  sleep(Duration::from_millis(110)).await;
  let after_run = model.ticks.get_untracked();
  assert!((3..=6).contains(&after_run), "expected about 5 ticks, got {after_run}");

  model.running.set(false);
  sleep(Duration::from_millis(30)).await;
  let paused = model.ticks.get_untracked();
  sleep(Duration::from_millis(80)).await;
  assert_eq!(model.ticks.get_untracked(), paused, "no ticks while paused");

  model.running.set(true);
  sleep(Duration::from_millis(70)).await;
  assert!(model.ticks.get_untracked() > paused, "resumes counting");

  model.reset.update(|n| *n += 1);
  Executor::tick().await;
  assert_eq!(model.ticks.get_untracked(), 0, "reset restarts from zero");
  sleep(Duration::from_millis(70)).await;
  let after_reset = model.ticks.get_untracked();
  assert!((1..=5).contains(&after_reset), "keeps running after reset, got {after_reset}");

  owner.cleanup();
}
