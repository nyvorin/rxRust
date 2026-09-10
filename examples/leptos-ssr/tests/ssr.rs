//! Server rendering must produce the initial state without spawning any
//! task: there is no `LocalSet` in Leptos's axum integration, and a
//! `spawn_local` here would panic.

#![cfg(feature = "ssr")]

use leptos::prelude::*;
use leptos_ssr_example::app::App;
use rx_leptos::prelude::*;
use rxrust::prelude::*;

#[tokio::test(flavor = "multi_thread")]
async fn server_renders_initial_state_without_spawning_tasks() {
  let _ = any_spawner::Executor::init_tokio();
  let owner = Owner::new_root(None);
  let html = owner.with(|| App().to_html());
  owner.cleanup();

  assert!(html.contains("Search crates"), "typeahead rendered");
  assert!(html.contains("0.0 s"), "stopwatch initial state rendered");
  assert!(html.contains("x = 0, y = 0"), "mouse panel initial state rendered");
  assert!(html.contains("0 searches started, 0 delivered"), "no search ran on the server");
}

/// Leptos's server features enable `sandboxed-arenas`: signal access outside
/// an owner panics. rx-leptos re-enters the subscribing owner from its
/// executor tasks and observer writes, so a pipeline wired inside an owner
/// keeps working (for instance inside a server-side `LocalSet`).
#[tokio::test(flavor = "local")]
async fn pipelines_run_under_sandboxed_arenas_when_wired_inside_an_owner() {
  let _ = any_spawner::Executor::init_tokio();
  let owner = Owner::new_root(None);

  let (input, doubled) = owner.with(|| {
    let input = RwSignal::new(1);
    let (doubled, set_doubled) = signal(0);
    input
      .to_observable()
      .map(|v: i32| v * 2)
      .feed(set_doubled);
    (input, doubled)
  });
  assert_eq!(owner.with(|| doubled.get_untracked()), 2);

  owner.with(|| input.set(5));
  any_spawner::Executor::tick().await;
  assert_eq!(owner.with(|| doubled.get_untracked()), 10);

  owner.cleanup();
}
