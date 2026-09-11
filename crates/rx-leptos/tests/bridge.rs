//! The spike's questions: can a rxRust `Local` observable be owned by a Leptos
//! reactive scope and disposed on cleanup, without leaks or borrow panics?
//! And can a signal drive an observable through Leptos's own scheduling?

use std::{cell::RefCell, convert::Infallible, rc::Rc};

use any_spawner::Executor;
use reactive_graph::{
  computed::Memo,
  owner::Owner,
  signal::{RwSignal, signal},
  traits::{Get, GetUntracked, Set},
};
use rx_leptos::prelude::*;
use rxrust::prelude::*;

/// Leptos initialises the executor when mounting; tests do it by hand.
fn executor() { let _ = Executor::init_futures_executor(); }

#[test]
fn to_signal_updates_until_owner_cleanup() {
  let owner = Owner::new();
  let mut source = Local::subject::<i32, Infallible>();

  let count = owner.with(|| to_signal(source.clone(), 0));
  assert_eq!(count.get_untracked(), 0);
  assert_eq!(source.inner.subscriber_count(), 1);

  source.next(5);
  assert_eq!(count.get_untracked(), 5);

  // The signal belongs to the owner and is disposed with it; the
  // subscription must go too, and later items must be dropped quietly.
  owner.cleanup();
  assert_eq!(source.inner.subscriber_count(), 0, "cleanup must unsubscribe");
  source.next(6);
}

#[test]
fn to_signal_local_accepts_non_send_items() {
  let owner = Owner::new();
  let mut source = Local::subject::<Rc<i32>, Infallible>();
  let latest = owner.with(|| to_signal_local(source.clone(), Rc::new(0)));
  source.next(Rc::new(3));
  assert_eq!(*latest.get_untracked(), 3);
  owner.cleanup();
  assert_eq!(source.inner.subscriber_count(), 0);
}

#[test]
fn use_observable_is_none_until_first_item() {
  let owner = Owner::new();
  let mut source = Local::subject::<&'static str, Infallible>();
  let latest = owner.with(|| use_observable(source.clone()));
  assert_eq!(latest.get_untracked(), None);
  source.next("hello");
  assert_eq!(latest.get_untracked(), Some("hello"));
  owner.cleanup();
}

#[test]
fn from_signal_emits_current_value_synchronously_and_changes_per_tick() {
  executor();
  let count = RwSignal::new(1);
  let seen = Rc::new(RefCell::new(Vec::new()));
  let sink = seen.clone();

  let sub = from_signal(count).subscribe(move |v| sink.borrow_mut().push(v));
  assert_eq!(*seen.borrow(), vec![1], "current value arrives on subscribe");

  count.set(2);
  assert_eq!(*seen.borrow(), vec![1], "changes wait for the executor");
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![1, 2]);

  // Writes within one tick coalesce, like a Leptos effect.
  count.set(3);
  count.set(4);
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![1, 2, 4]);

  assert!(!sub.is_closed());
  sub.unsubscribe();
  count.set(5);
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![1, 2, 4], "no emissions after unsubscribe");
}

#[test]
fn from_signal_guard_unsubscribes_on_drop() {
  executor();
  let count = RwSignal::new(1);
  let seen = Rc::new(RefCell::new(Vec::new()));
  let sink = seen.clone();
  {
    let _guard = from_signal(count)
      .subscribe(move |v| sink.borrow_mut().push(v))
      .unsubscribe_when_dropped();
    count.set(2);
    Executor::poll_local();
  }
  count.set(3);
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![1, 2]);
}

#[test]
fn from_signal_detaches_when_downstream_unsubscribes_mid_emission() {
  executor();
  let count = RwSignal::new(1);
  let seen = Rc::new(RefCell::new(Vec::new()));
  let sink = seen.clone();

  // `take(1)` unsubscribes upstream from inside `next`.
  let sub = from_signal(count)
    .take(1)
    .subscribe(move |v| sink.borrow_mut().push(v));
  assert!(sub.is_closed());
  count.set(2);
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![1]);
}

#[test]
fn from_signal_follows_a_memo_and_skips_unchanged_values() {
  executor();
  let input = RwSignal::new(1);
  let doubled = Memo::new(move |_| input.get() * 2);
  let seen = Rc::new(RefCell::new(Vec::new()));
  let sink = seen.clone();

  let _sub = from_signal(doubled).subscribe(move |v| sink.borrow_mut().push(v));
  input.set(1); // memo value unchanged: no emission
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![2]);
  input.set(2);
  Executor::poll_local();
  input.set(3);
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![2, 4, 6]);
}

#[test]
fn from_signal_writing_back_to_the_signal_reschedules() {
  executor();
  let count = RwSignal::new(0);
  let seen = Rc::new(RefCell::new(Vec::new()));
  let sink = seen.clone();

  let _sub = from_signal(count).subscribe(move |v| {
    sink.borrow_mut().push(v);
    if v < 3 {
      count.set(v + 1);
    }
  });
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![0, 1, 2, 3]);
  assert_eq!(count.get_untracked(), 3);
}

#[test]
fn signal_to_observable_to_signal_round_trip() {
  executor();
  let owner = Owner::new();
  let input = RwSignal::new(1);
  let emissions = Rc::new(RefCell::new(0));
  let counter = emissions.clone();

  let doubled = owner.with(|| {
    to_signal(
      from_signal(input)
        .tap(move |_| *counter.borrow_mut() += 1)
        .map(|v: i32| v * 2),
      0,
    )
  });
  assert_eq!(doubled.get_untracked(), 2);

  input.set(5);
  Executor::poll_local();
  assert_eq!(doubled.get_untracked(), 10);
  assert_eq!(*emissions.borrow(), 2);

  // Cleanup disposes `doubled` and must tear down the whole chain.
  owner.cleanup();
  input.set(6);
  Executor::poll_local();
  assert_eq!(*emissions.borrow(), 2, "no emissions after cleanup");
}

#[test]
fn operators_apply_between_signal_and_signal() {
  executor();
  let owner = Owner::new();
  let input = RwSignal::new(0);
  let changes = Rc::new(RefCell::new(0));
  let changes_c = changes.clone();

  let even = owner.with(|| {
    to_signal(
      from_signal(input)
        .filter(|v| v % 2 == 0)
        .distinct_until_changed()
        .tap(move |_| *changes_c.borrow_mut() += 1),
      -1,
    )
  });

  for v in [1, 2, 2, 3, 4] {
    input.set(v);
    Executor::poll_local();
  }
  assert_eq!(even.get_untracked(), 4);
  // initial 0, then 2 and 4: three distinct even values
  assert_eq!(*changes.borrow(), 3);
  owner.cleanup();
}

#[test]
fn signal_ext_gives_from_signal_a_method_form() {
  executor();
  let count = RwSignal::new(7);
  let seen = Rc::new(RefCell::new(Vec::new()));
  let sink = seen.clone();
  let _sub = count
    .to_observable()
    .subscribe(move |v| sink.borrow_mut().push(v));
  count.set(8);
  Executor::poll_local();
  assert_eq!(*seen.borrow(), vec![7, 8]);
}

#[test]
fn observable_ext_gives_to_signal_method_forms() {
  executor();
  let owner = Owner::new();
  let mut source = Local::subject::<i32, Infallible>();
  let input = RwSignal::new(1);

  let (latest, doubled, first) = owner.with(|| {
    (
      source.clone().to_option_signal(),
      input
        .to_observable()
        .map(|v: i32| v * 2)
        .to_signal(0),
      source
        .clone()
        .map(|v: i32| Rc::new(v))
        .to_signal_local(Rc::new(0)),
    )
  });
  assert_eq!(latest.get_untracked(), None);
  assert_eq!(doubled.get_untracked(), 2);

  source.next(3);
  input.set(5);
  Executor::poll_local();
  assert_eq!(latest.get_untracked(), Some(3));
  assert_eq!(doubled.get_untracked(), 10);
  assert_eq!(*first.get_untracked(), 3);

  owner.cleanup();
  assert_eq!(source.inner.subscriber_count(), 0);
}

#[test]
fn use_subscription_unsubscribes_on_owner_cleanup() {
  let owner = Owner::new();
  let source = Local::subject::<i32, Infallible>();
  owner.with(|| use_subscription(source.clone().subscribe(|_| {})));
  assert_eq!(source.inner.subscriber_count(), 1);
  owner.cleanup();
  assert_eq!(source.inner.subscriber_count(), 0);
}

#[test]
fn use_subject_completes_subscribers_on_owner_cleanup() {
  let owner = Owner::new();
  let mut clicks = owner.with(use_subject::<u32>);
  let seen = Rc::new(RefCell::new(Vec::new()));
  let completed = Rc::new(RefCell::new(false));
  let (sink, done) = (seen.clone(), completed.clone());

  clicks
    .clone()
    .on_complete(move || *done.borrow_mut() = true)
    .subscribe(move |v| sink.borrow_mut().push(v));

  clicks.next(1);
  clicks.next(2);
  assert_eq!(*seen.borrow(), vec![1, 2]);
  assert!(!*completed.borrow());

  owner.cleanup();
  assert!(*completed.borrow(), "cleanup completes the subject");
  assert_eq!(clicks.inner.subscriber_count(), 0);
  clicks.next(3); // completed subject: dropped quietly
  assert_eq!(*seen.borrow(), vec![1, 2]);
}

#[test]
fn feed_signal_writes_into_an_existing_signal_until_cleanup() {
  executor();
  let owner = Owner::new();
  let input = RwSignal::new(1);
  let (doubled, set_doubled) = signal(0);

  owner.with(|| {
    input
      .to_observable()
      .map(|v: i32| v * 2)
      .feed(set_doubled)
  });
  assert_eq!(doubled.get_untracked(), 2, "feeds synchronously on subscribe");

  input.set(4);
  Executor::poll_local();
  assert_eq!(doubled.get_untracked(), 8);

  owner.cleanup();
  input.set(5);
  Executor::poll_local();
  assert_eq!(doubled.get_untracked(), 8, "the signal outlives the owner but stops updating");
}
