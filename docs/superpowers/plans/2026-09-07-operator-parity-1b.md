# Operator Parity PR 1b Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix `BehaviorSubject`'s per-clone value bug, add `ReplaySubject` and `AsyncSubject`, generalize multicasting so `share`, `share_replay`, `publish_replay`, `publish_behavior`, and `publish_last` exist, and add `catch_error`, the `timeout` family, `repeat`, `exhaust_map`, and `audit`, as one PR stacked on PR 1a.

**Architecture:** Subjects keep their extra state (current value, replay buffer, last value) behind the context's `RcMut` so every clone shares it. `ConnectableObservable` and `RefCount` become generic over any subject type through a small `MulticastSubject` trait that exposes subscriber counts. Resubscribing operators (`repeat`, `catch_error`) follow `retry`'s pattern of holding the downstream context plus a serial subscription slot; the timing operator (`timeout`) follows `debounce`'s task-handle pattern; `exhaust_map` follows `switch_map`.

**Tech Stack:** Rust 2024, stable + nightly toolchains, `rxrust_macro::test`, `TestCtx`/`TestScheduler` for virtual time. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-07-operator-parity-tier1-design.md` (section "PR 1b")

## Global Constraints

- Branch `feat/operator-parity-tier1b`, based on `feat/operator-parity-tier1`; the PR targets that branch and is retargeted to `master` once 1a merges.
- Same file, naming, docs, test, and gate conventions as PR 1a (see `docs/superpowers/plans/2026-09-07-operator-parity-1a.md`, "Global Constraints").
- Follow the lessons recorded in the 1a plan execution: never unsubscribe the source that is currently dispatching to you; `on_error`/`on_complete` closures must be `'static`; a next-only `subscribe` needs `Err = Infallible`; use `vec![..]` for homogeneous lists; annotate `map` closure parameter types in tests when clippy under `--all-features` reports E0282.
- Time-based tests use `TestCtx` and `TestScheduler::init()` / `advance_by` / `flush`, never wall-clock sleeps.
- The `BehaviorSubject` type-parameter change is a breaking change; record it in the changelog.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/subject/behavior_subject.rs` | shared-value fix, regression test |
| `src/subject/multicast_subject.rs` | `MulticastSubject` trait + impls for `Subject`, `BehaviorSubject` |
| `src/subject/replay_subject.rs` | `ReplaySubject`, `ReplayBuffer`, tests |
| `src/subject/async_subject.rs` | `AsyncSubject`, `AsyncState`, tests |
| `src/subject.rs` | register the three new modules |
| `src/observable/connectable.rs` | generalize over subject type |
| `src/ops/ref_count.rs` | generalize over subject type; `ShareOf` alias |
| `src/ops/catch_error.rs` | `CatchError` |
| `src/ops/timeout.rs` | `Timeout`, `TimeoutError` |
| `src/ops/repeat.rs` | `Repeat` |
| `src/ops/exhaust_map.rs` | `ExhaustMap` |
| `src/ops/audit.rs` | `Audit`/`AuditTime` aliases over `Throttle` |
| `src/observable.rs` | new trait methods |
| `src/factory.rs` | subject factories |
| `src/prelude.rs` | export `ReplaySubject`, `AsyncSubject`, `TimeoutError`, `MulticastSubject`, `Notification` already present |
| `missing_features.md`, `guide/operators.md`, `CHANGELOG.md`, `tests/v1_integration.rs` | bookkeeping |

---

### Task 1: `BehaviorSubject` shares its value across clones

**Files:**
- Modify: `src/subject/behavior_subject.rs`
- Modify: `src/factory.rs:323-355` (two factory methods)

**Interfaces:**
- Produces: `BehaviorSubject<P, V>` with `pub subject: Subject<P>`, `pub value: V` where `V: RcDerefMut<Target = Item>`; `BehaviorSubject::new(initial: Item)` for `V: From<Item>`; `Behavior::peek`/`next_by` unchanged in shape.
- Factories return `Self::With<BehaviorSubject<SubjectPtr<'a, Self, Item, Err>, Self::RcMut<Item>>>`.

- [x] **Step 1: Write the failing regression test**

Append to the `tests` module in `src/subject/behavior_subject.rs`:

```rust
  #[rxrust_macro::test]
  fn test_behavior_subject_value_shared_across_clones() {
    let mut a = Local::behavior_subject::<i32, ()>(0);
    let b = a.clone();
    a.next(5);

    let (results, capture) = create_value_capture();
    b.on_error(|_| {}).subscribe(capture);

    assert_eq!(*results.borrow(), vec![5]);
    assert_eq!(a.peek(), 5);
  }
```

Run: `cargo test --lib subject::behavior_subject::tests::test_behavior_subject_value_shared_across_clones`
Expected: FAIL, left `[0]` right `[5]`.

- [x] **Step 2: Move the value behind the shared pointer**

Replace the struct, `Clone`, constructor, `Observer`, `CoreObservable`, and `Behavior` impls in `src/subject/behavior_subject.rs` with:

```rust
use super::subject_core::Subject;
use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// A specialized Subject that maintains and emits the latest value to new
/// subscribers.
///
/// The current value lives behind the context's shared pointer, so every
/// clone of the subject observes the same latest value.
///
/// # Type Parameters
///
/// - `P`: The smart pointer type for the Subject's observers list
/// - `V`: The shared pointer holding the current value
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut behavior = Local::behavior_subject(42);
/// behavior
///   .clone()
///   .subscribe(|v| println!("Current: {}", v)); // Prints: 42
/// behavior.next(99); // Prints: 99
/// ```
pub struct BehaviorSubject<P, V> {
  /// The underlying subject that manages subscribers
  pub subject: Subject<P>,
  /// Shared cell holding the current value
  pub value: V,
}

impl<P: Clone, V: Clone> Clone for BehaviorSubject<P, V> {
  fn clone(&self) -> Self { Self { subject: self.subject.clone(), value: self.value.clone() } }
}

impl<P, V> BehaviorSubject<P, V>
where
  Subject<P>: Default,
  V: RcDerefMut,
{
  /// Creates a new BehaviorSubject with the given initial value.
  pub fn new(initial: V::Target) -> Self
  where
    V: From<V::Target>,
  {
    Self { subject: Subject::default(), value: V::from(initial) }
  }
}

impl<Item, Err, P, V> Observer<Item, Err> for BehaviorSubject<P, V>
where
  Item: Clone,
  V: RcDerefMut<Target = Item>,
  Subject<P>: Observer<Item, Err>,
{
  fn next(&mut self, value: Item) {
    *self.value.rc_deref_mut() = value.clone();
    self.subject.next(value);
  }

  fn error(self, err: Err) { self.subject.error(err); }

  fn complete(self) { self.subject.complete(); }

  fn is_closed(&self) -> bool { self.subject.is_closed() }
}

impl<P, V> ObservableType for BehaviorSubject<P, V>
where
  Subject<P>: ObservableType,
{
  type Item<'a>
    = <Subject<P> as ObservableType>::Item<'a>
  where
    Self: 'a;

  type Err = <Subject<P> as ObservableType>::Err;
}

impl<Item, Err, C, P, V> CoreObservable<C> for BehaviorSubject<P, V>
where
  C: Context + Observer<Item, Err>,
  Subject<P>: CoreObservable<C, Err = Err>,
  V: RcDerefMut<Target = Item>,
  Item: Clone,
{
  type Unsub = <Subject<P> as CoreObservable<C>>::Unsub;

  fn subscribe(self, mut observer: C) -> Self::Unsub {
    observer.next(self.value.rc_deref().clone());
    self.subject.subscribe(observer)
  }
}
```

Keep the `Behavior` trait as is and replace its impl for the subject:

```rust
impl<Item, P, V> Behavior for BehaviorSubject<P, V>
where
  Item: Clone,
  V: RcDerefMut<Target = Item>,
  Self: Observer<Item, ()>,
{
  type Item = Item;

  fn peek(&self) -> Item { self.value.rc_deref().clone() }

  fn next_by(&mut self, f: impl FnOnce(Self::Item) -> Self::Item) {
    let new_val = f(self.peek());
    self.next(new_val);
  }
}
```

If the existing `ObservableType` impl was written with an `Item: Clone` bound on the struct's first parameter, drop that bound as shown; the item type comes from the inner `Subject`.

- [x] **Step 3: Update the two factories**

In `src/factory.rs`, change both `behavior_subject` signatures:

```rust
  fn behavior_subject<'a, Item: Clone, Err>(
    initial: Item,
  ) -> Self::With<BehaviorSubject<SubjectPtr<'a, Self, Item, Err>, Self::RcMut<Item>>> {
    Self::lift(BehaviorSubject::new(initial))
  }

  fn behavior_subject_mut_ref<'a, Item: Clone + 'a, Err>(
    initial: Item,
  ) -> Self::With<BehaviorSubject<SubjectPtrMutRef<'a, Self, Item, Err>, Self::RcMut<Item>>> {
    Self::lift(BehaviorSubject::new(initial))
  }
```

- [x] **Step 4: Run the subject and cookbook tests**

Run: `cargo test --lib subject:: && cargo test --doc behavior && cargo test --doc state_store`
Expected: all pass, including the new regression test.

- [x] **Step 5: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/subject/behavior_subject.rs src/factory.rs
git commit -m "fix(subject.behavior): share the current value across clones"
```

---

### Task 2: `MulticastSubject` trait, generalized `ConnectableObservable` and `RefCount`, `share()`

**Files:**
- Create: `src/subject/multicast_subject.rs`
- Modify: `src/subject.rs`, `src/observable/connectable.rs`, `src/ops/ref_count.rs`, `src/observable.rs` (`multicast`, `multicast_mut_ref`, `publish`, `publish_mut_ref`, the two `ConnectableObservableCtx*` aliases, new `share`), `src/prelude.rs`

**Interfaces:**
- Produces: `pub trait MulticastSubject { fn subscriber_count(&self) -> usize; fn is_empty(&self) -> bool; fn is_terminated(&self) -> bool; }`, `ConnectableObservable<S, Sub>`, `RefCount<S, Sub, ConnPtr>`, `Observable::multicast<'a, Sub>(self, subject: Sub) -> Self::With<ConnectableObservable<Self::Inner, Sub>>`, `Observable::share<'a>(self) -> ShareOf<'a, Self>`.

- [x] **Step 1: Add the trait**

`src/subject/multicast_subject.rs`:

```rust
//! Subscriber-count access for any subject usable in multicasting.

use super::{behavior_subject::BehaviorSubject, subject_core::Subject, subscribers::Subscribers};
use crate::context::RcDeref;

/// A subject that can back `ConnectableObservable` and `RefCount`.
///
/// `RefCount` connects the source when the first subscriber arrives and
/// disconnects when the last one leaves, so it needs to see the count.
pub trait MulticastSubject {
  /// Number of live subscribers.
  fn subscriber_count(&self) -> usize;

  /// Whether there are no live subscribers.
  fn is_empty(&self) -> bool { self.subscriber_count() == 0 }

  /// Whether the subject has delivered a terminal event and will replay it
  /// to late subscribers instead of accepting a new source connection.
  fn is_terminated(&self) -> bool { false }
}

impl<P, O> MulticastSubject for Subject<P>
where
  P: RcDeref<Target = Subscribers<O>>,
{
  fn subscriber_count(&self) -> usize { Subject::subscriber_count(self) }
}

impl<P, V> MulticastSubject for BehaviorSubject<P, V>
where
  Subject<P>: MulticastSubject,
{
  fn subscriber_count(&self) -> usize { self.subject.subscriber_count() }
}
```

In `src/subject.rs` add `pub mod multicast_subject;` and `pub use multicast_subject::*;`.

- [x] **Step 2: Generalize `ConnectableObservable`**

In `src/observable/connectable.rs` replace the struct and impls (keep the module docs and tests):

```rust
use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  ops::ref_count::RefCount,
};

#[derive(Clone)]
pub struct ConnectableObservable<S, Sub> {
  pub(crate) source: S,
  pub(crate) subject: Sub,
}

impl<Item, Err, S, Sub> Observer<Item, Err> for ConnectableObservable<S, Sub>
where
  Sub: Observer<Item, Err>,
{
  #[inline]
  fn next(&mut self, value: Item) { self.subject.next(value); }

  #[inline]
  fn error(self, err: Err) { self.subject.error(err); }

  #[inline]
  fn complete(self) { self.subject.complete(); }

  #[inline]
  fn is_closed(&self) -> bool { self.subject.is_closed() }
}

impl<S, Sub> ObservableType for ConnectableObservable<S, Sub>
where
  S: ObservableType,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

impl<C, S, Sub> CoreObservable<C> for ConnectableObservable<S, Sub>
where
  S: ObservableType,
  Sub: CoreObservable<C>,
{
  type Unsub = <Sub as CoreObservable<C>>::Unsub;

  fn subscribe(self, observer: C) -> Self::Unsub { self.subject.subscribe(observer) }
}

impl<S, Sub: Clone> ConnectableObservable<S, Sub> {
  pub fn fork(&self) -> Sub { self.subject.clone() }

  pub fn connect<C: Context>(self) -> S::Unsub
  where
    S: CoreObservable<C::With<Sub>>,
  {
    self.source.subscribe(C::lift(self.subject))
  }
}

pub trait Connectable<S, Sub: Clone>: Context<Inner = ConnectableObservable<S, Sub>>
where
  S: CoreObservable<Self::With<Sub>>,
{
  fn connect(self) -> S::Unsub { self.into_inner().connect::<Self>() }

  fn fork(&self) -> Self::With<Sub> { self.wrap(self.inner().fork()) }

  #[allow(clippy::type_complexity)]
  fn ref_count(self) -> Self::With<RefCount<S, Sub, Self::RcMut<Option<S::Unsub>>>> {
    let connectable = self.into_inner();
    let connection = Self::RcMut::from(None);
    Self::lift(RefCount { connectable, connection })
  }
}

impl<C, S, Sub> Connectable<S, Sub> for C
where
  C: Context<Inner = ConnectableObservable<S, Sub>>,
  Sub: Clone,
  S: CoreObservable<C::With<Sub>>,
{
}
```

Keep the doc comments that were on each item.

- [x] **Step 3: Generalize `RefCount` and add the `share` aliases**

In `src/ops/ref_count.rs` replace the struct and impls (keep tests):

```rust
use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, Observable, ObservableType, connectable::ConnectableObservable},
  subject::{MulticastSubject, Subject, SubjectPtr},
  subscription::Subscription,
};

pub struct RefCount<S, Sub, ConnPtr> {
  pub(crate) connectable: ConnectableObservable<S, Sub>,
  pub(crate) connection: ConnPtr,
}

impl<S: Clone, Sub: Clone, ConnPtr: Clone> Clone for RefCount<S, Sub, ConnPtr> {
  fn clone(&self) -> Self {
    Self { connectable: self.connectable.clone(), connection: self.connection.clone() }
  }
}

impl<S, Sub, ConnPtr> ObservableType for RefCount<S, Sub, ConnPtr>
where
  Sub: ObservableType,
{
  type Item<'a>
    = Sub::Item<'a>
  where
    Self: 'a;
  type Err = Sub::Err;
}

impl<Ctx, S, Sub, ConnPtr> CoreObservable<Ctx> for RefCount<S, Sub, ConnPtr>
where
  Ctx: Context,
  S: Clone + CoreObservable<Ctx::With<Sub>>,
  Sub: Clone + MulticastSubject + CoreObservable<Ctx>,
  ConnPtr: Clone + RcDerefMut<Target = Option<S::Unsub>> + Subscription,
{
  type Unsub = RefCountSubscription<Sub, <Sub as CoreObservable<Ctx>>::Unsub, ConnPtr>;

  fn subscribe(self, observer: Ctx) -> Self::Unsub {
    let subject = self.connectable.fork();
    let inner_sub = subject.clone().subscribe(observer);

    if !subject.is_terminated()
      && subject.subscriber_count() == 1
      && self.connection.rc_deref().is_none()
    {
      *self.connection.rc_deref_mut() = Some(self.connectable.connect::<Ctx>());
    }

    RefCountSubscription { subject, inner: inner_sub, connection: self.connection }
  }
}

pub struct RefCountSubscription<Sub, InnerSub, ConnPtr> {
  subject: Sub,
  inner: InnerSub,
  connection: ConnPtr,
}

impl<Sub, InnerSub, ConnPtr> Subscription for RefCountSubscription<Sub, InnerSub, ConnPtr>
where
  Sub: MulticastSubject,
  InnerSub: Subscription,
  ConnPtr: Subscription,
{
  fn unsubscribe(self) {
    self.inner.unsubscribe();
    if self.subject.is_empty() {
      self.connection.unsubscribe();
    }
  }

  fn is_closed(&self) -> bool { self.inner.is_closed() }
}

/// The plain `Subject` that `publish()` uses for an observable `O`.
pub type PublishSubjectOf<'a, O> =
  Subject<SubjectPtr<'a, O, <O as Observable>::Item<'a>, <O as Observable>::Err>>;

/// Return type of [`Observable::share`].
pub type ShareOf<'a, O> = <O as Context>::With<
  RefCount<
    <O as Context>::Inner,
    PublishSubjectOf<'a, O>,
    <O as Context>::RcMut<
      Option<
        <<O as Context>::Inner as CoreObservable<<O as Context>::With<PublishSubjectOf<'a, O>>>>::Unsub,
      >,
    >,
  >,
>;
```

- [x] **Step 4: Update `src/observable.rs`**

Change the aliases near the end of the file:

```rust
pub type ConnectableObservableCtx<'a, O> = <O as Context>::With<
  ConnectableObservable<<O as Context>::Inner, PublishSubjectOf<'a, O>>,
>;
pub type ConnectableObservableCtxMutRef<'a, O, Item> = <O as Context>::With<
  ConnectableObservable<
    <O as Context>::Inner,
    Subject<SubjectPtrMutRef<'a, O, Item, <O as Observable>::Err>>,
  >,
>;
```

Change `multicast` to be generic over the subject and add `share`:

```rust
  fn multicast<Sub>(self, subject: Sub) -> Self::With<ConnectableObservable<Self::Inner, Sub>> {
    self.transform(|source| ConnectableObservable { source, subject })
  }

  /// Multicast through a plain `Subject`, connecting on the first subscriber
  /// and disconnecting when the last one leaves
  ///
  /// Equivalent to `publish().ref_count()`.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let shared = Local::from_iter(vec![1, 2]).share();
  /// shared.clone().subscribe(|v| println!("A: {}", v));
  /// shared.subscribe(|v| println!("B: {}", v));
  /// ```
  fn share<'a>(self) -> ShareOf<'a, Self>
  where
    Self::Inner: CoreObservable<Self::With<PublishSubjectOf<'a, Self>>>,
  {
    self.publish().ref_count()
  }
```

Import `PublishSubjectOf` and `ShareOf` from `crate::ops::ref_count` in the ops import block. `publish` keeps calling `self.multicast(Subject::default())`; if inference needs help, write `self.multicast(PublishSubjectOf::<'a, Self>::default())`. `publish_mut_ref` at `src/observable.rs:2203` changes its return type's second argument from `SubjectPtrMutRef<..>` to `Subject<SubjectPtrMutRef<..>>`.

`src/prelude.rs`: add `MulticastSubject` to the `subject::*` export (it is covered by the glob) and confirm `ShareOf` is reachable via `crate::ops::*`.

- [x] **Step 5: Add a `share` test to `src/ops/ref_count.rs`**

```rust
  #[rxrust_macro::test]
  fn test_share_connects_once_for_two_subscribers() {
    let subscriptions = Rc::new(RefCell::new(0));
    let subscriptions_c = subscriptions.clone();
    let a = Rc::new(RefCell::new(Vec::new()));
    let b = Rc::new(RefCell::new(Vec::new()));
    let a_c = a.clone();
    let b_c = b.clone();

    let mut source = Local::subject::<i32, std::convert::Infallible>();
    let shared = source
      .clone()
      .tap(move |_| {})
      .finalize(move || *subscriptions_c.borrow_mut() += 1)
      .share();

    let sub_a = shared.clone().subscribe(move |v| a_c.borrow_mut().push(v));
    let sub_b = shared.subscribe(move |v| b_c.borrow_mut().push(v));
    source.next(1);
    source.next(2);

    assert_eq!(*a.borrow(), vec![1, 2]);
    assert_eq!(*b.borrow(), vec![1, 2]);
    assert_eq!(source.inner.subscriber_count(), 1);

    sub_a.unsubscribe();
    assert_eq!(source.inner.subscriber_count(), 1);
    sub_b.unsubscribe();
    assert_eq!(source.inner.subscriber_count(), 0);
    assert_eq!(*subscriptions.borrow(), 1);
  }
```

If `finalize` runs on unsubscribe only, the count of 1 confirms one source connection; if it does not fire on unsubscribe, drop that assertion and keep the subscriber-count ones.

- [x] **Step 6: Run everything that touches multicasting**

Run: `cargo test --lib connectable && cargo test --lib ref_count && cargo test --doc multicast && cargo test --doc publish && cargo test --doc share`
Expected: pass.

- [x] **Step 7: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/subject/multicast_subject.rs src/subject.rs src/observable/connectable.rs src/ops/ref_count.rs src/observable.rs src/prelude.rs
git commit -m "refactor(connectable): generalize multicasting over the subject type and add share"
```

---

### Task 3: `ReplaySubject`, `publish_replay`, `share_replay`

**Files:**
- Create: `src/subject/replay_subject.rs`
- Modify: `src/subject.rs`, `src/subject/multicast_subject.rs`, `src/factory.rs` (after `behavior_subject_mut_ref`), `src/observable.rs` (after `publish`), `src/ops/ref_count.rs` (`ShareReplayOf` alias), `src/prelude.rs`

**Interfaces:**
- Produces: `ReplaySubject<P, B>` with `B: RcDerefMut<Target = ReplayBuffer<Item, Err>>`; `ReplaySubject::new(capacity: Option<usize>)`; factories `replay_subject(capacity: usize)`, `replay_subject_unbounded()`; `Observable::publish_replay(capacity)`, `Observable::share_replay(capacity)`; alias `ReplaySubjectOf<'a, O>`.

- [x] **Step 1: Write the subject with tests**

`src/subject/replay_subject.rs`:

```rust
//! ReplaySubject: replays a bounded history to late subscribers.

use std::collections::VecDeque;

use super::subject_core::Subject;
use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// A recorded terminal event.
#[derive(Debug, Clone)]
pub enum Terminal<Err> {
  /// The subject errored
  Error(Err),
  /// The subject completed
  Complete,
}

/// Shared replay state.
pub struct ReplayBuffer<Item, Err> {
  items: VecDeque<Item>,
  capacity: Option<usize>,
  terminal: Option<Terminal<Err>>,
}

impl<Item, Err> ReplayBuffer<Item, Err> {
  fn new(capacity: Option<usize>) -> Self {
    Self { items: VecDeque::new(), capacity, terminal: None }
  }

  fn push(&mut self, item: Item) {
    if self.capacity == Some(0) {
      return;
    }
    self.items.push_back(item);
    if let Some(cap) = self.capacity {
      while self.items.len() > cap {
        self.items.pop_front();
      }
    }
  }
}

/// A Subject that buffers the last `capacity` items (or all of them) and
/// replays them, plus any terminal event, to every new subscriber.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut subject = Local::replay_subject::<i32, std::convert::Infallible>(2);
/// subject.next(1);
/// subject.next(2);
/// subject.next(3);
///
/// let mut seen = Vec::new();
/// subject.clone().subscribe(|v| seen.push(v));
/// assert_eq!(seen, vec![2, 3]);
/// ```
pub struct ReplaySubject<P, B> {
  /// The underlying subject that manages live subscribers
  pub subject: Subject<P>,
  buffer: B,
}

impl<P: Clone, B: Clone> Clone for ReplaySubject<P, B> {
  fn clone(&self) -> Self { Self { subject: self.subject.clone(), buffer: self.buffer.clone() } }
}

impl<P, B, Item, Err> ReplaySubject<P, B>
where
  Subject<P>: Default,
  B: RcDerefMut<Target = ReplayBuffer<Item, Err>> + From<ReplayBuffer<Item, Err>>,
{
  /// Creates a subject that replays up to `capacity` items, or every item
  /// when `capacity` is `None`.
  pub fn new(capacity: Option<usize>) -> Self {
    Self { subject: Subject::default(), buffer: B::from(ReplayBuffer::new(capacity)) }
  }
}

impl<Item, Err, P, B> Observer<Item, Err> for ReplaySubject<P, B>
where
  Item: Clone,
  Err: Clone,
  B: RcDerefMut<Target = ReplayBuffer<Item, Err>>,
  Subject<P>: Observer<Item, Err>,
{
  fn next(&mut self, value: Item) {
    {
      let mut buffer = self.buffer.rc_deref_mut();
      if buffer.terminal.is_some() {
        return;
      }
      buffer.push(value.clone());
    }
    self.subject.next(value);
  }

  fn error(self, err: Err) {
    {
      let mut buffer = self.buffer.rc_deref_mut();
      if buffer.terminal.is_some() {
        return;
      }
      buffer.terminal = Some(Terminal::Error(err.clone()));
    }
    self.subject.error(err);
  }

  fn complete(self) {
    {
      let mut buffer = self.buffer.rc_deref_mut();
      if buffer.terminal.is_some() {
        return;
      }
      buffer.terminal = Some(Terminal::Complete);
    }
    self.subject.complete();
  }

  fn is_closed(&self) -> bool { self.buffer.rc_deref().terminal.is_some() }
}

impl<P, B> ObservableType for ReplaySubject<P, B>
where
  Subject<P>: ObservableType,
{
  type Item<'a>
    = <Subject<P> as ObservableType>::Item<'a>
  where
    Self: 'a;
  type Err = <Subject<P> as ObservableType>::Err;
}

impl<Item, Err, C, P, B> CoreObservable<C> for ReplaySubject<P, B>
where
  C: Context + Observer<Item, Err>,
  Subject<P>: CoreObservable<C, Err = Err>,
  B: RcDerefMut<Target = ReplayBuffer<Item, Err>>,
  Item: Clone,
  Err: Clone,
{
  type Unsub = Option<<Subject<P> as CoreObservable<C>>::Unsub>;

  fn subscribe(self, mut observer: C) -> Self::Unsub {
    let (items, terminal) = {
      let buffer = self.buffer.rc_deref();
      (buffer.items.iter().cloned().collect::<Vec<_>>(), buffer.terminal.clone())
    };
    for item in items {
      if observer.is_closed() {
        return None;
      }
      observer.next(item);
    }
    match terminal {
      Some(Terminal::Error(err)) => {
        observer.error(err);
        None
      }
      Some(Terminal::Complete) => {
        observer.complete();
        None
      }
      None => Some(self.subject.subscribe(observer)),
    }
  }
}

/// The `ReplaySubject` type that `replay_subject` builds for an observable `O`.
pub type ReplaySubjectOf<'a, O> = ReplaySubject<
  super::SubjectPtr<'a, O, <O as crate::observable::Observable>::Item<'a>, <O as crate::observable::Observable>::Err>,
  <O as Context>::RcMut<
    ReplayBuffer<<O as crate::observable::Observable>::Item<'a>, <O as crate::observable::Observable>::Err>,
  >,
>;

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_replay_bounded_history_to_late_subscriber() {
    let mut subject = Local::replay_subject::<i32, Infallible>(2);
    subject.next(1);
    subject.next(2);
    subject.next(3);

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_c = seen.clone();
    subject
      .clone()
      .subscribe(move |v| seen_c.borrow_mut().push(v));
    subject.next(4);

    assert_eq!(*seen.borrow(), vec![2, 3, 4]);
  }

  #[rxrust_macro::test]
  fn test_replay_unbounded_history() {
    let mut subject = Local::replay_subject_unbounded::<i32, Infallible>();
    for i in 0..5 {
      subject.next(i);
    }

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_c = seen.clone();
    subject
      .clone()
      .subscribe(move |v| seen_c.borrow_mut().push(v));

    assert_eq!(*seen.borrow(), vec![0, 1, 2, 3, 4]);
  }

  #[rxrust_macro::test]
  fn test_replay_completion_to_late_subscriber() {
    let mut subject = Local::replay_subject::<i32, Infallible>(1);
    subject.next(7);
    subject.clone().complete();

    let seen = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let seen_c = seen.clone();
    let completed_c = completed.clone();
    let sub = subject
      .clone()
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| seen_c.borrow_mut().push(v));

    assert_eq!(*seen.borrow(), vec![7]);
    assert!(*completed.borrow());
    assert!(sub.is_closed());
    // Events after termination are ignored
    subject.next(8);
    assert_eq!(*seen.borrow(), vec![7]);
  }

  #[rxrust_macro::test]
  fn test_replay_error_to_late_subscriber() {
    let subject = Local::replay_subject::<i32, String>(1);
    subject.clone().error("boom".to_string());

    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();
    subject
      .clone()
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(error.borrow().as_deref(), Some("boom"));
  }

  #[rxrust_macro::test]
  fn test_replay_shared_context() {
    use std::sync::{Arc, Mutex};

    let mut subject = Shared::replay_subject::<i32, Infallible>(3);
    subject.next(1);
    subject.next(2);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_c = seen.clone();
    subject
      .clone()
      .subscribe(move |v| seen_c.lock().unwrap().push(v));

    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
  }
}
```

Register in `src/subject.rs` (`pub mod replay_subject;` and `pub use replay_subject::*;`) and add to `src/subject/multicast_subject.rs`:

```rust
impl<P, B, Item, Err> MulticastSubject for super::replay_subject::ReplaySubject<P, B>
where
  Subject<P>: MulticastSubject,
  B: RcDeref<Target = super::replay_subject::ReplayBuffer<Item, Err>>,
{
  fn subscriber_count(&self) -> usize { self.subject.subscriber_count() }

  fn is_terminated(&self) -> bool { Observer::<Item, Err>::is_closed(self) }
}
```

If the `Observer::is_closed` call does not resolve (ambiguous `Item`/`Err`), add a `pub fn is_terminated(&self) -> bool` inherent method on `ReplaySubject` reading `self.buffer.rc_deref().terminal.is_some()` and call that.

- [x] **Step 2: Factories and operators**

`src/factory.rs`, after `behavior_subject_mut_ref`:

```rust
  /// Creates a `ReplaySubject` that replays the last `capacity` items and any
  /// terminal event to late subscribers.
  #[allow(clippy::type_complexity)]
  fn replay_subject<'a, Item: Clone, Err: Clone>(
    capacity: usize,
  ) -> Self::With<
    ReplaySubject<SubjectPtr<'a, Self, Item, Err>, Self::RcMut<ReplayBuffer<Item, Err>>>,
  > {
    Self::lift(ReplaySubject::new(Some(capacity)))
  }

  /// Creates a `ReplaySubject` that replays every item to late subscribers.
  #[allow(clippy::type_complexity)]
  fn replay_subject_unbounded<'a, Item: Clone, Err: Clone>() -> Self::With<
    ReplaySubject<SubjectPtr<'a, Self, Item, Err>, Self::RcMut<ReplayBuffer<Item, Err>>>,
  > {
    Self::lift(ReplaySubject::new(None))
  }
```

`src/ops/ref_count.rs`, add:

```rust
/// Return type of [`Observable::share_replay`].
pub type ShareReplayOf<'a, O> = <O as Context>::With<
  RefCount<
    <O as Context>::Inner,
    ReplaySubjectOf<'a, O>,
    <O as Context>::RcMut<
      Option<<<O as Context>::Inner as CoreObservable<<O as Context>::With<ReplaySubjectOf<'a, O>>>>::Unsub>,
    >,
  >,
>;
```

`src/observable.rs`, after `publish`:

```rust
  /// Multicast through a `ReplaySubject` holding the last `capacity` items
  fn publish_replay<'a>(
    self, capacity: usize,
  ) -> Self::With<ConnectableObservable<Self::Inner, ReplaySubjectOf<'a, Self>>> {
    self.multicast(ReplaySubjectOf::<'a, Self>::new(Some(capacity)))
  }

  /// `publish_replay(capacity).ref_count()`: late subscribers see the last
  /// `capacity` items, the source is connected while at least one subscriber
  /// is present, and after the source terminates late subscribers receive the
  /// buffer and the terminal event without reconnecting
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let shared = Local::from_iter(vec![1, 2, 3]).share_replay(2);
  /// shared.clone().subscribe(|v| println!("first: {}", v));
  /// shared.subscribe(|v| println!("late: {}", v)); // late: 2, late: 3
  /// ```
  #[doc(alias = "shareReplay")]
  fn share_replay<'a>(self, capacity: usize) -> ShareReplayOf<'a, Self>
  where
    Self::Inner: CoreObservable<Self::With<ReplaySubjectOf<'a, Self>>>,
  {
    self.publish_replay(capacity).ref_count()
  }
```

`src/prelude.rs`: `ReplaySubject`, `ReplayBuffer`, `ReplaySubjectOf` are covered by `subject::*`; `ShareReplayOf` by `ops::*`.

- [x] **Step 3: Add a `share_replay` test to `src/ops/ref_count.rs`**

```rust
  #[rxrust_macro::test]
  fn test_share_replay_late_subscriber_gets_buffer_and_completion() {
    let shared = Local::from_iter(vec![1, 2, 3]).share_replay(2);

    let first = Rc::new(RefCell::new(Vec::new()));
    let first_c = first.clone();
    shared.clone().subscribe(move |v| first_c.borrow_mut().push(v));
    assert_eq!(*first.borrow(), vec![1, 2, 3]);

    let late = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let late_c = late.clone();
    let completed_c = completed.clone();
    shared
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| late_c.borrow_mut().push(v));
    assert_eq!(*late.borrow(), vec![2, 3]);
    assert!(*completed.borrow());
  }
```

- [x] **Step 4: Run and commit**

Run: `cargo test --lib subject::replay_subject && cargo test --lib ref_count && cargo test --doc replay && cargo test --doc share_replay`
Expected: pass.

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/subject/replay_subject.rs src/subject.rs src/subject/multicast_subject.rs src/factory.rs src/observable.rs src/ops/ref_count.rs src/prelude.rs
git commit -m "feat(subject.replay): add ReplaySubject, publish_replay and share_replay"
```

---

### Task 4: `AsyncSubject`, `publish_last`, `publish_behavior`

**Files:**
- Create: `src/subject/async_subject.rs`
- Modify: `src/subject.rs`, `src/subject/multicast_subject.rs`, `src/factory.rs` (after `replay_subject_unbounded`), `src/observable.rs` (after `share_replay`)

**Interfaces:**
- Produces: `AsyncSubject<P, V>` with `V: RcDerefMut<Target = AsyncState<Item, Err>>`; factory `async_subject()`; `Observable::publish_last()`, `Observable::publish_behavior(initial)`.

- [x] **Step 1: Write the subject with tests**

`src/subject/async_subject.rs`:

```rust
//! AsyncSubject: emits only the last value, and only on completion.

use super::{replay_subject::Terminal, subject_core::Subject};
use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// Shared state of an [`AsyncSubject`].
pub struct AsyncState<Item, Err> {
  last: Option<Item>,
  terminal: Option<Terminal<Err>>,
}

impl<Item, Err> Default for AsyncState<Item, Err> {
  fn default() -> Self { Self { last: None, terminal: None } }
}

/// A Subject that stores the last value and emits it to every subscriber only
/// when it completes. Subscribers arriving after completion receive the value
/// and completion; an error discards the value.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut subject = Local::async_subject::<i32, std::convert::Infallible>();
/// let mut seen = Vec::new();
/// subject.clone().subscribe(|v| seen.push(v));
/// subject.next(1);
/// subject.next(2);
/// assert!(seen.is_empty());
/// subject.complete();
/// assert_eq!(seen, vec![2]);
/// ```
pub struct AsyncSubject<P, V> {
  /// The underlying subject that manages live subscribers
  pub subject: Subject<P>,
  state: V,
}

impl<P: Clone, V: Clone> Clone for AsyncSubject<P, V> {
  fn clone(&self) -> Self { Self { subject: self.subject.clone(), state: self.state.clone() } }
}

impl<P, V, Item, Err> Default for AsyncSubject<P, V>
where
  Subject<P>: Default,
  V: RcDerefMut<Target = AsyncState<Item, Err>> + From<AsyncState<Item, Err>>,
{
  fn default() -> Self { Self { subject: Subject::default(), state: V::from(AsyncState::default()) } }
}

impl<Item, Err, P, V> Observer<Item, Err> for AsyncSubject<P, V>
where
  Item: Clone,
  Err: Clone,
  V: RcDerefMut<Target = AsyncState<Item, Err>>,
  Subject<P>: Observer<Item, Err> + Clone,
{
  fn next(&mut self, value: Item) {
    let mut state = self.state.rc_deref_mut();
    if state.terminal.is_none() {
      state.last = Some(value);
    }
  }

  fn error(self, err: Err) {
    {
      let mut state = self.state.rc_deref_mut();
      if state.terminal.is_some() {
        return;
      }
      state.last = None;
      state.terminal = Some(Terminal::Error(err.clone()));
    }
    self.subject.error(err);
  }

  fn complete(self) {
    let last = {
      let mut state = self.state.rc_deref_mut();
      if state.terminal.is_some() {
        return;
      }
      state.terminal = Some(Terminal::Complete);
      state.last.clone()
    };
    let mut subject = self.subject;
    if let Some(value) = last {
      subject.next(value);
    }
    subject.complete();
  }

  fn is_closed(&self) -> bool { self.state.rc_deref().terminal.is_some() }
}

impl<P, V> ObservableType for AsyncSubject<P, V>
where
  Subject<P>: ObservableType,
{
  type Item<'a>
    = <Subject<P> as ObservableType>::Item<'a>
  where
    Self: 'a;
  type Err = <Subject<P> as ObservableType>::Err;
}

impl<Item, Err, C, P, V> CoreObservable<C> for AsyncSubject<P, V>
where
  C: Context + Observer<Item, Err>,
  Subject<P>: CoreObservable<C, Err = Err>,
  V: RcDerefMut<Target = AsyncState<Item, Err>>,
  Item: Clone,
  Err: Clone,
{
  type Unsub = Option<<Subject<P> as CoreObservable<C>>::Unsub>;

  fn subscribe(self, mut observer: C) -> Self::Unsub {
    let (last, terminal) = {
      let state = self.state.rc_deref();
      (state.last.clone(), state.terminal.clone())
    };
    match terminal {
      Some(Terminal::Error(err)) => {
        observer.error(err);
        None
      }
      Some(Terminal::Complete) => {
        if let Some(value) = last {
          observer.next(value);
        }
        observer.complete();
        None
      }
      None => Some(self.subject.subscribe(observer)),
    }
  }
}

/// The `AsyncSubject` type that `async_subject` builds for an observable `O`.
pub type AsyncSubjectOf<'a, O> = AsyncSubject<
  super::SubjectPtr<'a, O, <O as crate::observable::Observable>::Item<'a>, <O as crate::observable::Observable>::Err>,
  <O as Context>::RcMut<
    AsyncState<<O as crate::observable::Observable>::Item<'a>, <O as crate::observable::Observable>::Err>,
  >,
>;

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_async_subject_emits_last_on_complete() {
    let mut subject = Local::async_subject::<i32, Infallible>();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let seen_c = seen.clone();
    let completed_c = completed.clone();

    subject
      .clone()
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| seen_c.borrow_mut().push(v));

    subject.next(1);
    subject.next(2);
    assert!(seen.borrow().is_empty());

    subject.complete();
    assert_eq!(*seen.borrow(), vec![2]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_async_subject_late_subscriber_after_complete() {
    let mut subject = Local::async_subject::<i32, Infallible>();
    subject.next(9);
    subject.clone().complete();

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_c = seen.clone();
    subject
      .clone()
      .subscribe(move |v| seen_c.borrow_mut().push(v));

    assert_eq!(*seen.borrow(), vec![9]);
  }

  #[rxrust_macro::test]
  fn test_async_subject_empty_complete() {
    let subject = Local::async_subject::<i32, Infallible>();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let seen_c = seen.clone();
    let completed_c = completed.clone();

    subject
      .clone()
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| seen_c.borrow_mut().push(v));
    subject.complete();

    assert!(seen.borrow().is_empty());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_async_subject_error_discards_value() {
    let mut subject = Local::async_subject::<i32, String>();
    let seen = Rc::new(RefCell::new(Vec::new()));
    let error = Rc::new(RefCell::new(None));
    let seen_c = seen.clone();
    let error_c = error.clone();

    subject
      .clone()
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(move |v| seen_c.borrow_mut().push(v));
    subject.next(1);
    subject.error("boom".to_string());

    assert!(seen.borrow().is_empty());
    assert_eq!(error.borrow().as_deref(), Some("boom"));
  }
}
```

Register in `src/subject.rs`; add the `MulticastSubject` impl in `src/subject/multicast_subject.rs`, same shape as the `ReplaySubject` one (delegating `subscriber_count` and reporting `is_terminated` from the state).

- [x] **Step 2: Factory and operators**

`src/factory.rs`:

```rust
  /// Creates an `AsyncSubject`, which emits only its last value, on completion.
  #[allow(clippy::type_complexity)]
  fn async_subject<'a, Item: Clone, Err: Clone>() -> Self::With<
    AsyncSubject<SubjectPtr<'a, Self, Item, Err>, Self::RcMut<AsyncState<Item, Err>>>,
  > {
    Self::lift(AsyncSubject::default())
  }
```

`src/observable.rs`, after `share_replay`:

```rust
  /// Multicast through an `AsyncSubject`: subscribers receive only the
  /// source's last value, when it completes
  #[doc(alias = "publishLast")]
  fn publish_last<'a>(
    self,
  ) -> Self::With<ConnectableObservable<Self::Inner, AsyncSubjectOf<'a, Self>>> {
    self.multicast(AsyncSubjectOf::<'a, Self>::default())
  }

  /// Multicast through a `BehaviorSubject` seeded with `initial`
  #[doc(alias = "publishBehavior")]
  fn publish_behavior<'a>(
    self, initial: Self::Item<'a>,
  ) -> Self::With<ConnectableObservable<Self::Inner, BehaviorSubjectOf<'a, Self>>>
  where
    Self::Item<'a>: Clone,
  {
    self.multicast(BehaviorSubjectOf::<'a, Self>::new(initial))
  }
```

Add to `src/subject/behavior_subject.rs`:

```rust
/// The `BehaviorSubject` type that `behavior_subject` builds for an observable `O`.
pub type BehaviorSubjectOf<'a, O> = BehaviorSubject<
  super::SubjectPtr<'a, O, <O as crate::observable::Observable>::Item<'a>, <O as crate::observable::Observable>::Err>,
  <O as Context>::RcMut<<O as crate::observable::Observable>::Item<'a>>,
>;
```

- [x] **Step 3: Test `publish_last` and `publish_behavior` in `src/observable/connectable.rs` tests**

```rust
  #[rxrust_macro::test]
  fn test_publish_last_emits_only_final_value() {
    let (results, capture) = create_value_capture();
    let connectable = Local::from_iter(vec![1, 2, 3]).publish_last();
    connectable.fork().subscribe(capture);
    connectable.connect();
    assert_eq!(*results.borrow(), vec![3]);
  }

  #[rxrust_macro::test]
  fn test_publish_behavior_seeds_subscribers() {
    let (results, capture) = create_value_capture();
    let connectable = Local::from_iter(vec![1, 2]).publish_behavior(0);
    connectable.fork().subscribe(capture);
    connectable.connect();
    assert_eq!(*results.borrow(), vec![0, 1, 2]);
  }
```

- [x] **Step 4: Run and commit**

Run: `cargo test --lib subject::async_subject && cargo test --lib connectable && cargo test --doc async_subject && cargo test --doc publish_last && cargo test --doc publish_behavior`
Expected: pass.

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/subject/async_subject.rs src/subject.rs src/subject/multicast_subject.rs src/subject/behavior_subject.rs src/factory.rs src/observable.rs src/observable/connectable.rs
git commit -m "feat(subject.async): add AsyncSubject, publish_last and publish_behavior"
```

---

### Task 5: `catch_error`

**Files:**
- Create: `src/ops/catch_error.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `map_err`)

**Interfaces:**
- Produces: `Observable::catch_error<F, Out>(self, handler: F) -> Self::With<CatchError<Self::Inner, F>> where F: FnMut(Self::Err) -> Out, Out: Context<Inner: ObservableType>` with matching item type; output `Err` is the fallback's.

- [x] **Step 1: Write the operator with tests**

```rust
//! CatchError operator implementation
//!
//! Recovers from an error by switching to a fallback observable.

use crate::{
  context::{Context, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{IntoBoxedSubscription, Subscription},
};

/// CatchError operator: On error, subscribe to a fallback observable
///
/// The handler receives the error and returns the observable to continue
/// with. Its items must match the source's; its error type becomes the
/// output error type.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::throw_err("boom".to_string())
///   .map(|_: ()| 0)
///   .catch_error(|e: String| Local::from_iter(vec![e.len() as i32]))
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![4]);
/// ```
#[doc(alias = "catchError")]
#[derive(Clone)]
pub struct CatchError<S, F> {
  pub source: S,
  pub handler: F,
}

impl<S, F, Out> ObservableType for CatchError<S, F>
where
  S: ObservableType,
  F: FnMut(S::Err) -> Out,
  Out: Context<Inner: ObservableType>,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = <Out::Inner as ObservableType>::Err;
}

/// Observer that swaps in the fallback on error
pub struct CatchErrorObserver<Ctx: Context, F> {
  observer: Option<Ctx>,
  handler: F,
  serial: Ctx::RcMut<Option<Ctx::BoxedSubscription>>,
}

impl<Ctx, F, Out, Item, SrcErr, OutErr> Observer<Item, SrcErr> for CatchErrorObserver<Ctx, F>
where
  Ctx: Context + Observer<Item, OutErr>,
  F: FnMut(SrcErr) -> Out,
  Out: Context<Inner: CoreObservable<Ctx, Unsub: IntoBoxedSubscription<Ctx::BoxedSubscription>>>,
{
  fn next(&mut self, value: Item) {
    if let Some(observer) = self.observer.as_mut() {
      observer.next(value);
    }
  }

  fn error(mut self, err: SrcErr) {
    let Some(observer) = self.observer.take() else { return };
    let fallback = (self.handler)(err).into_inner();
    let unsub = fallback.subscribe(observer);
    *self.serial.rc_deref_mut() = Some(unsub.into_boxed());
  }

  fn complete(self) {
    if let Some(observer) = self.observer {
      observer.complete();
    }
  }

  fn is_closed(&self) -> bool {
    self
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

impl<S, F, C> CoreObservable<C> for CatchError<S, F>
where
  C: Context,
  S: CoreObservable<C::With<CatchErrorObserver<C, F>>>,
  S::Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
  C::RcMut<Option<C::BoxedSubscription>>: Subscription,
{
  type Unsub = C::RcMut<Option<C::BoxedSubscription>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let CatchError { source, handler } = self;
    let serial: C::RcMut<Option<C::BoxedSubscription>> = C::RcMut::from(None);
    let observer =
      CatchErrorObserver { observer: Some(context), handler, serial: serial.clone() };
    let source_unsub = source.subscribe(C::lift(observer));
    // The fallback may have replaced the slot synchronously; keep that one.
    let mut slot = serial.rc_deref_mut();
    if slot.is_none() {
      *slot = Some(source_unsub.into_boxed());
    }
    drop(slot);
    serial
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_catch_error_switches_to_fallback() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    let mut source = Local::subject::<i32, String>();
    source
      .clone()
      .catch_error(|e: String| Local::from_iter(vec![e.len() as i32, 100]))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    source.next(1);
    source.error("boom".to_string());

    assert_eq!(*result.borrow(), vec![1, 4, 100]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_catch_error_transparent_without_error() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter(vec![1, 2])
      .map_err(|_: Infallible| String::new())
      .catch_error(|_: String| Local::of(0))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
  }

  #[rxrust_macro::test]
  fn test_catch_error_fallback_error_propagates() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    Local::throw_err("first".to_string())
      .map(|_: ()| 0)
      .catch_error(|_: String| Local::throw_err(42u8).map(|_: ()| 0))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), Some(42u8));
  }

  #[rxrust_macro::test]
  fn test_catch_error_unsubscribe_cancels_fallback() {
    let mut source = Local::subject::<i32, String>();
    let fallback = Local::subject::<i32, Infallible>();
    let fallback_c = fallback.clone();

    let sub = source
      .clone()
      .catch_error(move |_: String| fallback_c.clone())
      .subscribe(|_| {});

    source.error("boom".to_string());
    assert_eq!(fallback.inner.subscriber_count(), 1);

    sub.unsubscribe();
    assert_eq!(fallback.inner.subscriber_count(), 0);
  }
}
```

If `Local::throw_err(..).map(|_: ()| 0)` does not type-check because `ThrowErr`'s item type is not `()`, look at `src/observable/trivial.rs` for the item type and adjust the closure annotation.

- [x] **Step 2: Register and add the trait method**

`src/ops.rs`: `pub mod catch_error;` / `pub use catch_error::*;`.

`src/observable.rs`, after `map_err`:

```rust
  /// Recover from an error by continuing with the observable returned by
  /// `handler`
  ///
  /// The fallback's items must match; its error type becomes the output
  /// error type.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::throw_err("boom".to_string())
  ///   .map(|_: ()| 0)
  ///   .catch_error(|_: String| Local::of(-1))
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: -1
  /// ```
  #[doc(alias = "catchError")]
  fn catch_error<F, Out>(self, handler: F) -> Self::With<CatchError<Self::Inner, F>>
  where
    F: FnMut(Self::Err) -> Out,
    Out: Context<Inner: ObservableType>,
  {
    self.transform(|source| CatchError { source, handler })
  }
```

- [x] **Step 3: Run, gate, commit**

Run: `cargo test --lib ops::catch_error && cargo test --doc catch_error`

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/catch_error.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.catch_error): add catch_error operator"
```

---

### Task 6: `timeout` family

**Files:**
- Create: `src/ops/timeout.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `debounce_with`), `src/prelude.rs` (export `TimeoutError`)

**Interfaces:**
- Produces: `pub struct TimeoutError;` (Debug, Clone, Copy, PartialEq, Eq, Default, Display, Error); `Timeout<S, Sch, F>`; methods `timeout(duration)`, `timeout_with(duration, scheduler)` requiring `Self::Err: From<TimeoutError>`, `timeout_or_else(duration, f)`, `timeout_or_else_with(duration, f, scheduler)` with `F: FnOnce() -> Self::Err`.

- [x] **Step 1: Write the operator with tests**

```rust
//! Timeout operator implementation
//!
//! Errors if the source stays silent for longer than a duration.

use std::fmt;

use crate::{
  context::{Context, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  scheduler::{Duration, Scheduler, Task, TaskHandle, TaskState},
  subscription::{SourceWithHandle, Subscription},
};

/// The error `timeout` emits when no item arrives in time.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct TimeoutError;

impl fmt::Display for TimeoutError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str("observable timed out") }
}

impl std::error::Error for TimeoutError {}

/// Builds the default timeout error for any `Err: From<TimeoutError>`.
pub fn default_timeout_error<E: From<TimeoutError>>() -> E { TimeoutError.into() }

/// Timeout operator: Error if the source is silent for `duration`
///
/// The timer starts at subscription and restarts after every item. When it
/// fires, the downstream receives the error from `error_fn` and the source
/// is released.
///
/// # Examples
///
/// ```rust,no_run
/// use rxrust::prelude::*;
///
/// # #[tokio::main(flavor = "local")]
/// # async fn main() {
/// Local::never()
///   .map_to(0)
///   .map_err(|_: std::convert::Infallible| TimeoutError)
///   .timeout(Duration::from_millis(50))
///   .on_error(|e| println!("{}", e))
///   .subscribe(|_| {});
/// # }
/// ```
#[derive(Clone)]
pub struct Timeout<S, Sch, F> {
  pub source: S,
  pub duration: Duration,
  pub scheduler: Sch,
  pub error_fn: F,
}

impl<S, Sch, F> ObservableType for Timeout<S, Sch, F>
where
  S: ObservableType,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Subscription for the timeout operator
pub type TimeoutSubscription<U, H> = SourceWithHandle<U, H>;

/// Observer that arms and re-arms the timer
pub struct TimeoutObserver<P, Sch, H, E> {
  observer: P,
  scheduler: Sch,
  duration: Duration,
  handle_state: H,
  error_fn: E,
}

fn timeout_fire<P, E, Item, Err, F>(state: &mut (P, E)) -> TaskState
where
  P: RcDerefMut<Target = Option<Item>>,
  E: RcDerefMut<Target = Option<F>>,
  F: FnOnce() -> Err,
  Item: Observer<Item2, Err>,
  Item2: Sized,
{
  unreachable!("replaced below")
}
```

Replace that placeholder task function with a concrete one; the task state is `(P, E)` where `P` holds the downstream observer and `E` the error factory:

```rust
fn timeout_fire<P, E, O, F, Item, Err>(state: &mut (P, E)) -> TaskState
where
  P: RcDerefMut<Target = Option<O>>,
  E: RcDerefMut<Target = Option<F>>,
  O: Observer<Item, Err>,
  F: FnOnce() -> Err,
{
  let (observer_rc, error_rc) = state;
  let observer = observer_rc.rc_deref_mut().take();
  let error_fn = error_rc.rc_deref_mut().take();
  if let (Some(observer), Some(error_fn)) = (observer, error_fn) {
    observer.error(error_fn());
  }
  TaskState::Finished
}

impl<P, Sch, H, E, O, F, Item, Err> TimeoutObserver<P, Sch, H, E>
where
  P: RcDerefMut<Target = Option<O>> + Clone,
  E: RcDerefMut<Target = Option<F>> + Clone,
  H: RcDerefMut<Target = Option<TaskHandle>>,
  O: Observer<Item, Err>,
  F: FnOnce() -> Err,
  Sch: Scheduler<Task<(P, E)>>,
{
  fn arm(&self) {
    if let Some(handle) = self.handle_state.rc_deref_mut().take() {
      handle.unsubscribe();
    }
    let task = Task::new(
      (self.observer.clone(), self.error_fn.clone()),
      timeout_fire::<P, E, O, F, Item, Err>,
    );
    let handle = self.scheduler.schedule(task, Some(self.duration));
    *self.handle_state.rc_deref_mut() = Some(handle);
  }

  fn disarm(&self) {
    if let Some(handle) = self.handle_state.rc_deref_mut().take() {
      handle.unsubscribe();
    }
  }
}

impl<P, Sch, H, E, O, F, Item, Err> Observer<Item, Err> for TimeoutObserver<P, Sch, H, E>
where
  P: RcDerefMut<Target = Option<O>> + Clone,
  E: RcDerefMut<Target = Option<F>> + Clone,
  H: RcDerefMut<Target = Option<TaskHandle>>,
  O: Observer<Item, Err>,
  F: FnOnce() -> Err,
  Sch: Scheduler<Task<(P, E)>>,
{
  fn next(&mut self, value: Item) {
    self.disarm();
    if let Some(observer) = self.observer.rc_deref_mut().as_mut() {
      observer.next(value);
    }
    if self.observer.rc_deref_mut().is_some() {
      self.arm();
    }
  }

  fn error(self, err: Err) {
    self.disarm();
    if let Some(observer) = self.observer.rc_deref_mut().take() {
      observer.error(err);
    }
  }

  fn complete(self) {
    self.disarm();
    if let Some(observer) = self.observer.rc_deref_mut().take() {
      observer.complete();
    }
  }

  fn is_closed(&self) -> bool {
    self
      .observer
      .rc_deref_mut()
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

type ObserverRc<C> = <C as Context>::RcMut<Option<<C as Context>::Inner>>;
type ErrorFnRc<C, F> = <C as Context>::RcMut<Option<F>>;
type HandleRc<C> = <C as Context>::RcMut<Option<TaskHandle>>;

impl<S, Sch, F, C> CoreObservable<C> for Timeout<S, Sch, F>
where
  C: Context,
  HandleRc<C>: Subscription,
  S: CoreObservable<C::With<TimeoutObserver<ObserverRc<C>, Sch, HandleRc<C>, ErrorFnRc<C, F>>>>,
  ObserverRc<C>: Clone,
  ErrorFnRc<C, F>: Clone,
  Sch: Scheduler<Task<(ObserverRc<C>, ErrorFnRc<C, F>)>>,
  C::Inner: for<'a> Observer<S::Item<'a>, S::Err>,
  F: FnOnce() -> S::Err,
{
  type Unsub = TimeoutSubscription<S::Unsub, HandleRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let Timeout { source, duration, scheduler, error_fn } = self;
    let handle_state: HandleRc<C> = C::RcMut::from(None);
    let handle_for_observer = handle_state.clone();

    let wrapped = context.transform(|observer| {
      let timeout_observer = TimeoutObserver {
        observer: C::RcMut::from(Some(observer)),
        scheduler,
        duration,
        handle_state: handle_for_observer,
        error_fn: C::RcMut::from(Some(error_fn)),
      };
      timeout_observer.arm();
      timeout_observer
    });

    let source_sub = source.subscribe(wrapped);
    SourceWithHandle::new(source_sub, handle_state)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use super::TimeoutError;
  use crate::{context::TestCtx, prelude::*, scheduler::test_scheduler::TestScheduler};

  #[rxrust_macro::test]
  fn test_timeout_errors_when_silent() {
    TestScheduler::init();
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    let mut subject = TestCtx::subject::<i32, TimeoutError>();
    let _sub = subject
      .clone()
      .timeout(Duration::from_millis(100))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    subject.next(1);
    TestScheduler::advance_by(Duration::from_millis(60));
    assert!(error.borrow().is_none());
    // The item above reset the timer, so 60 + 60 > 100 fires now
    TestScheduler::advance_by(Duration::from_millis(60));
    assert_eq!(*error.borrow(), Some(TimeoutError));
  }

  #[rxrust_macro::test]
  fn test_timeout_items_keep_it_alive() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let error = Rc::new(RefCell::new(None));
    let result_c = result.clone();
    let error_c = error.clone();

    let mut subject = TestCtx::subject::<i32, TimeoutError>();
    let _sub = subject
      .clone()
      .timeout(Duration::from_millis(100))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    for i in 0..5 {
      TestScheduler::advance_by(Duration::from_millis(50));
      subject.next(i);
    }

    assert_eq!(*result.borrow(), vec![0, 1, 2, 3, 4]);
    assert!(error.borrow().is_none());
  }

  #[rxrust_macro::test]
  fn test_timeout_or_else_custom_error_and_completion_cancels() {
    TestScheduler::init();
    let error = Rc::new(RefCell::new(None));
    let completed = Rc::new(RefCell::new(false));
    let error_c = error.clone();
    let completed_c = completed.clone();

    let subject = TestCtx::subject::<i32, String>();
    let _sub = subject
      .clone()
      .timeout_or_else(Duration::from_millis(10), || "late".to_string())
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    subject.clone().complete();
    TestScheduler::advance_by(Duration::from_millis(50));

    assert!(*completed.borrow());
    assert!(error.borrow().is_none());
    assert!(TestScheduler::is_empty());
  }

  #[rxrust_macro::test]
  fn test_timeout_unsubscribe_cancels_timer() {
    TestScheduler::init();
    let subject = TestCtx::subject::<i32, TimeoutError>();
    let sub = subject
      .clone()
      .timeout(Duration::from_millis(10))
      .on_error(|_| {})
      .subscribe(|_| {});

    sub.unsubscribe();
    assert!(TestScheduler::is_empty());
  }
}
```

Delete the first placeholder `timeout_fire` definition (the one containing `unreachable!`) before compiling; only the concrete one stays.

- [x] **Step 2: Register, export, and add the trait methods**

`src/ops.rs`: `pub mod timeout;` / `pub use timeout::*;`. `src/prelude.rs`: add `timeout::TimeoutError` to the `ops` re-export list.

`src/observable.rs`, after `debounce_with`:

```rust
  /// Error with `TimeoutError` if the source is silent for `duration`
  ///
  /// The timer restarts after every item. Requires `Err: From<TimeoutError>`.
  fn timeout(
    self, duration: Duration,
  ) -> Self::With<Timeout<Self::Inner, Self::Scheduler, fn() -> Self::Err>>
  where
    Self::Err: From<TimeoutError>,
  {
    let scheduler = self.scheduler().clone();
    self.timeout_or_else_with(duration, default_timeout_error::<Self::Err> as fn() -> Self::Err, scheduler)
  }

  /// [`Observable::timeout`] with an explicit scheduler
  fn timeout_with<Sch>(
    self, duration: Duration, scheduler: Sch,
  ) -> Self::With<Timeout<Self::Inner, Sch, fn() -> Self::Err>>
  where
    Self::Err: From<TimeoutError>,
  {
    self.timeout_or_else_with(duration, default_timeout_error::<Self::Err> as fn() -> Self::Err, scheduler)
  }

  /// Error with `error_fn()` if the source is silent for `duration`
  fn timeout_or_else<F>(
    self, duration: Duration, error_fn: F,
  ) -> Self::With<Timeout<Self::Inner, Self::Scheduler, F>>
  where
    F: FnOnce() -> Self::Err,
  {
    let scheduler = self.scheduler().clone();
    self.timeout_or_else_with(duration, error_fn, scheduler)
  }

  /// [`Observable::timeout_or_else`] with an explicit scheduler
  fn timeout_or_else_with<F, Sch>(
    self, duration: Duration, error_fn: F, scheduler: Sch,
  ) -> Self::With<Timeout<Self::Inner, Sch, F>>
  where
    F: FnOnce() -> Self::Err,
  {
    self.transform(|source| Timeout { source, duration, scheduler, error_fn })
  }
```

Import `Timeout`, `TimeoutError`, `default_timeout_error` from `crate::ops::timeout`.

- [x] **Step 3: Run, gate, commit**

Run: `cargo test --lib ops::timeout && cargo test --doc timeout`

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/timeout.rs src/ops.rs src/observable.rs src/prelude.rs
git commit -m "feat(ops.timeout): add timeout, timeout_with, timeout_or_else operators"
```

---

### Task 7: `repeat` and `repeat_forever`

**Files:**
- Create: `src/ops/repeat.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `retry`)

**Interfaces:**
- Produces: `Repeat<S>` with `pub count: Option<usize>`; `Observable::repeat(count: usize)`, `Observable::repeat_forever()`. Resubscription happens on the context scheduler's next tick, like `retry`.

- [x] **Step 1: Write the operator with tests**

```rust
//! Repeat operator implementation
//!
//! Resubscribes to the source when it completes.

use crate::{
  context::{Context, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  scheduler::{Scheduler, Task, TaskState},
  subscription::{IntoBoxedSubscription, Subscription},
};

/// Repeat operator: Resubscribe to the source on completion
///
/// `count` is the total number of subscriptions (`Some(0)` completes
/// immediately, `Some(1)` is transparent); `None` repeats until
/// unsubscribed. Each resubscription happens on the scheduler's next tick,
/// so a synchronous source cannot recurse.
///
/// # Examples
///
/// ```rust,no_run
/// use rxrust::prelude::*;
///
/// # #[tokio::main(flavor = "local")]
/// # async fn main() {
/// Local::from_iter(vec![1, 2])
///   .repeat(3)
///   .subscribe(|v| println!("{}", v));
/// // Prints 1, 2, 1, 2, 1, 2 across three ticks
/// # }
/// ```
#[derive(Clone)]
pub struct Repeat<S> {
  pub source: S,
  pub count: Option<usize>,
}

impl<S: ObservableType> ObservableType for Repeat<S> {
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that schedules the next subscription on completion
pub struct RepeatObserver<S, Ctx: Context> {
  source: S,
  observer: Ctx,
  remaining: Option<usize>,
  serial: Ctx::RcMut<Option<Ctx::BoxedSubscription>>,
  subscribe_fn: fn(Self),
}

impl<S: Clone, Ctx: Context + Clone> Clone for RepeatObserver<S, Ctx> {
  fn clone(&self) -> Self {
    Self {
      source: self.source.clone(),
      observer: self.observer.clone(),
      remaining: self.remaining,
      serial: self.serial.clone(),
      subscribe_fn: self.subscribe_fn,
    }
  }
}

impl<S, Ctx> RepeatObserver<S, Ctx>
where
  Ctx: Context,
  S: CoreObservable<Ctx::With<Self>> + Clone,
  S::Unsub: IntoBoxedSubscription<Ctx::BoxedSubscription>,
{
  fn subscribe_impl(self) {
    let serial = self.serial.clone();
    if let Some(previous) = serial.rc_deref_mut().take() {
      previous.unsubscribe();
    }
    let source = self.source.clone();
    let unsub = source.subscribe(Ctx::lift(self));
    *serial.rc_deref_mut() = Some(unsub.into_boxed());
  }
}

impl<S, Ctx, Item, Err> Observer<Item, Err> for RepeatObserver<S, Ctx>
where
  Self: Clone,
  Ctx: Context<Scheduler: Scheduler<Task<Option<Self>>>> + Observer<Item, Err>,
{
  fn next(&mut self, value: Item) { self.observer.next(value); }

  fn error(self, err: Err) { self.observer.error(err); }

  fn complete(mut self) {
    let again = match self.remaining {
      None => true,
      Some(n) if n > 1 => {
        self.remaining = Some(n - 1);
        true
      }
      Some(_) => false,
    };
    if !again || self.observer.is_closed() {
      self.observer.complete();
      return;
    }
    let scheduler = self.observer.scheduler().clone();
    scheduler.schedule(
      Task::new(Some(self), |this| {
        if let Some(observer) = this.take() {
          (observer.subscribe_fn)(observer);
        }
        TaskState::Finished
      }),
      None,
    );
  }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, Ctx> CoreObservable<Ctx> for Repeat<S>
where
  Ctx: Context,
  Ctx::Inner: for<'a> Observer<S::Item<'a>, S::Err>,
  S: CoreObservable<Ctx::With<RepeatObserver<S, Ctx>>> + Clone,
  S::Unsub: IntoBoxedSubscription<Ctx::BoxedSubscription>,
  Ctx::RcMut<Option<Ctx::BoxedSubscription>>: Subscription,
{
  type Unsub = Ctx::RcMut<Option<Ctx::BoxedSubscription>>;

  fn subscribe(self, observer: Ctx) -> Self::Unsub {
    let serial = Ctx::RcMut::from(None);
    if self.count == Some(0) {
      observer.into_inner().complete();
      return serial;
    }
    let repeat_observer = RepeatObserver {
      source: self.source,
      observer,
      remaining: self.count,
      serial: serial.clone(),
      subscribe_fn: RepeatObserver::subscribe_impl,
    };
    let unsub = repeat_observer
      .source
      .clone()
      .subscribe(Ctx::lift(repeat_observer));
    *serial.rc_deref_mut() = Some(unsub.into_boxed());
    serial
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::{context::TestCtx, prelude::*, scheduler::test_scheduler::TestScheduler};

  #[rxrust_macro::test]
  fn test_repeat_count() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    let _sub = TestCtx::from_iter(vec![1, 2])
      .repeat(3)
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
    TestScheduler::flush();

    assert_eq!(*result.borrow(), vec![1, 2, 1, 2, 1, 2]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_repeat_zero_completes_immediately() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    let _sub = TestCtx::from_iter(vec![1])
      .repeat(0)
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert!(result.borrow().is_empty());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_repeat_forever_until_unsubscribed() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let sub = TestCtx::from_iter(vec![7])
      .repeat_forever()
      .subscribe(move |v| result_c.borrow_mut().push(v));

    for _ in 0..3 {
      TestScheduler::advance_by(Duration::from_millis(1));
    }
    let seen = result.borrow().len();
    assert!(seen >= 4, "expected at least four repetitions, saw {seen}");

    sub.unsubscribe();
    TestScheduler::advance_by(Duration::from_millis(1));
    assert_eq!(result.borrow().len(), seen);
  }

  #[rxrust_macro::test]
  fn test_repeat_error_stops() {
    TestScheduler::init();
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    let _sub = TestCtx::throw_err("boom".to_string())
      .map(|_: ()| 0)
      .repeat(3)
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});
    TestScheduler::flush();

    assert_eq!(error.borrow().as_deref(), Some("boom"));
  }
}
```

If `TestScheduler::advance_by` does not run a task scheduled with `None` delay, use `TestScheduler::flush()` in the forever test between reads; and if `flush` on an unbounded repeat never returns, replace the loop with three `advance_by(Duration::ZERO)` calls and assert `seen >= 2`.

- [x] **Step 2: Register and add the trait methods**

`src/ops.rs`: `pub mod repeat;` / `pub use repeat::*;`.

`src/observable.rs`, after `retry`:

```rust
  /// Resubscribe to the source on completion, `count` times in total
  ///
  /// `repeat(0)` completes immediately and `repeat(1)` is transparent. Each
  /// resubscription runs on the scheduler's next tick.
  fn repeat(self, count: usize) -> Self::With<Repeat<Self::Inner>> {
    self.transform(|source| Repeat { source, count: Some(count) })
  }

  /// Resubscribe to the source on completion until unsubscribed
  fn repeat_forever(self) -> Self::With<Repeat<Self::Inner>> {
    self.transform(|source| Repeat { source, count: None })
  }
```

- [x] **Step 3: Run, gate, commit**

Run: `cargo test --lib ops::repeat && cargo test --doc repeat`

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/repeat.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.repeat): add repeat and repeat_forever operators"
```

---

### Task 8: `exhaust_map`

**Files:**
- Create: `src/ops/exhaust_map.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `switch_map`)

**Interfaces:**
- Produces: `Observable::exhaust_map<F, Out>(self, f: F) -> Self::With<ExhaustMap<Self::Inner, F>>` with the same bounds as `switch_map`.

- [x] **Step 1: Write the operator with tests**

```rust
//! ExhaustMap operator implementation
//!
//! Maps to inner observables but ignores outer items while an inner one is
//! active.

use std::marker::PhantomData;

use crate::{
  context::{Context, RcDeref, RcDerefMut, Scope},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{IntoBoxedSubscription, Subscription, TupleSubscription},
};

/// ExhaustMap operator: Run one inner observable at a time, dropping outer
/// items that arrive while one is active
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter(vec![1, 2, 3])
///   .exhaust_map(|v| Local::from_iter(vec![v * 10, v * 10 + 1]))
///   .subscribe(|v| result.push(v));
/// // Synchronous inners finish before the next outer item, so all run
/// assert_eq!(result, vec![10, 11, 20, 21, 30, 31]);
/// ```
#[doc(alias = "exhaustMap")]
#[derive(Clone)]
pub struct ExhaustMap<S, F> {
  pub source: S,
  pub func: F,
}

#[doc(hidden)]
pub struct ExhaustMapState<O, InnerSub> {
  observer: O,
  outer_completed: bool,
  inner_active: bool,
  inner_sub: Option<InnerSub>,
}

impl<O, InnerSub: Subscription> Subscription for ExhaustMapState<O, InnerSub> {
  fn unsubscribe(mut self) {
    if let Some(inner) = self.inner_sub.take() {
      inner.unsubscribe();
    }
  }

  fn is_closed(&self) -> bool { false }
}

#[doc(hidden)]
pub struct ExhaustMapOuterObserver<Sc: Scope, O, F, InnerObs> {
  state: ExhaustState<Sc, O>,
  func: F,
  _inner: PhantomData<fn() -> InnerObs>,
}

#[doc(hidden)]
pub struct ExhaustMapInnerObserver<State>(State);

type ExhaustState<Sc, O> =
  <Sc as Scope>::RcMut<Option<ExhaustMapState<O, <Sc as Scope>::BoxedSubscription>>>;
type InnerObserverCtx<C> = <C as Context>::With<
  ExhaustMapInnerObserver<ExhaustState<<C as Context>::Scope, <C as Context>::Inner>>,
>;

impl<S, F, Out> ObservableType for ExhaustMap<S, F>
where
  S: ObservableType,
  F: for<'a> FnMut(S::Item<'a>) -> Out,
  Out: Context<Inner: ObservableType<Err = S::Err> + 'static>,
{
  type Item<'a>
    = <Out::Inner as ObservableType>::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

impl<S, F, C, Out, InnerObs> CoreObservable<C> for ExhaustMap<S, F>
where
  C: Context,
  S: CoreObservable<C::With<ExhaustMapOuterObserver<C::Scope, C::Inner, F, InnerObs>>>,
  F: for<'a> FnMut(S::Item<'a>) -> Out,
  Out: Context<Inner = InnerObs>,
  InnerObs: CoreObservable<InnerObserverCtx<C>, Err = S::Err> + 'static,
  InnerObs::Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
  ExhaustState<C::Scope, C::Inner>: Subscription,
{
  type Unsub = TupleSubscription<S::Unsub, ExhaustState<C::Scope, C::Inner>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let ExhaustMap { source, func } = self;
    let state: ExhaustState<C::Scope, C::Inner> = <C::Scope as Scope>::RcMut::from(None);

    let wrapped = context.transform(|observer| {
      *state.rc_deref_mut() = Some(ExhaustMapState {
        observer,
        outer_completed: false,
        inner_active: false,
        inner_sub: None,
      });
      ExhaustMapOuterObserver { state: state.clone(), func, _inner: PhantomData }
    });

    let source_unsub = source.subscribe(wrapped);
    TupleSubscription::new(source_unsub, state)
  }
}

impl<Sc, O, InnerObs, Item, Err, F, Out> Observer<Item, Err>
  for ExhaustMapOuterObserver<Sc, O, F, InnerObs>
where
  Sc: Scope,
  O: for<'a> Observer<InnerObs::Item<'a>, Err>,
  F: FnMut(Item) -> Out,
  Out: Context<Inner = InnerObs, Scope = Sc>,
  InnerObs: CoreObservable<
      Out::With<ExhaustMapInnerObserver<ExhaustState<Sc, O>>>,
      Unsub: IntoBoxedSubscription<Sc::BoxedSubscription>,
    >,
{
  fn next(&mut self, value: Item) {
    {
      let mut guard = self.state.rc_deref_mut();
      let Some(st) = guard.as_mut() else { return };
      if st.inner_active || st.observer.is_closed() {
        return;
      }
      st.inner_active = true;
    }
    let inner_obs = (self.func)(value).into_inner();
    let inner_unsub = inner_obs.subscribe(Out::lift(ExhaustMapInnerObserver(self.state.clone())));
    let mut guard = self.state.rc_deref_mut();
    if let Some(st) = guard.as_mut() {
      if st.inner_active {
        st.inner_sub = Some(inner_unsub.into_boxed());
      } else {
        // The inner finished synchronously; nothing to keep.
        drop(inner_unsub);
      }
    }
  }

  fn error(self, err: Err) {
    if let Some(mut st) = self.state.rc_deref_mut().take() {
      st.observer.error(err);
      if let Some(inner) = st.inner_sub.take() {
        inner.unsubscribe();
      }
    }
  }

  fn complete(self) {
    let mut guard = self.state.rc_deref_mut();
    let Some(st) = guard.as_mut() else { return };
    st.outer_completed = true;
    if !st.inner_active {
      let st = guard.take().unwrap();
      st.observer.complete();
    }
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .as_ref()
      .is_none_or(|st| st.observer.is_closed())
  }
}

impl<Sc, O, Item, Err> Observer<Item, Err> for ExhaustMapInnerObserver<ExhaustState<Sc, O>>
where
  Sc: Scope,
  O: Observer<Item, Err>,
{
  fn next(&mut self, value: Item) {
    if let Some(st) = self.0.rc_deref_mut().as_mut() {
      st.observer.next(value);
    }
  }

  fn error(self, err: Err) {
    if let Some(st) = self.0.rc_deref_mut().take() {
      st.observer.error(err);
    }
  }

  fn complete(self) {
    let mut guard = self.0.rc_deref_mut();
    let Some(st) = guard.as_mut() else { return };
    st.inner_active = false;
    st.inner_sub = None;
    if st.outer_completed {
      let st = guard.take().unwrap();
      st.observer.complete();
    }
  }

  fn is_closed(&self) -> bool {
    self
      .0
      .rc_deref()
      .as_ref()
      .is_none_or(|st| st.observer.is_closed())
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_exhaust_map_drops_outer_items_while_inner_active() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut outer = Local::subject::<i32, Infallible>();
    let mut inner = Local::subject::<i32, Infallible>();
    let inner_c = inner.clone();

    outer
      .clone()
      .exhaust_map(move |_| inner_c.clone())
      .subscribe(move |v| result_c.borrow_mut().push(v));

    outer.next(1);
    inner.next(10);
    outer.next(2); // dropped: inner still active
    inner.next(11);
    inner.clone().complete();
    outer.next(3); // starts a new inner subscription
    assert_eq!(inner.inner.subscriber_count(), 1);
    inner.next(12);

    assert_eq!(*result.borrow(), vec![10, 11, 12]);
  }

  #[rxrust_macro::test]
  fn test_exhaust_map_sync_inners_all_run() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter(vec![1, 2, 3])
      .exhaust_map(|v| Local::from_iter(vec![v * 10, v * 10 + 1]))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![10, 11, 20, 21, 30, 31]);
  }

  #[rxrust_macro::test]
  fn test_exhaust_map_completes_after_inner() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    let mut outer = Local::subject::<i32, Infallible>();
    let inner = Local::subject::<i32, Infallible>();
    let inner_c = inner.clone();

    outer
      .clone()
      .exhaust_map(move |_| inner_c.clone())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    outer.next(1);
    outer.clone().complete();
    assert!(!*completed.borrow());
    inner.complete();
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_exhaust_map_error_propagation() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    let mut outer = Local::subject::<i32, String>();
    outer
      .clone()
      .exhaust_map(|v| Local::of(v).map_err(|_: Infallible| String::new()))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    outer.error("boom".to_string());
    assert_eq!(error.borrow().as_deref(), Some("boom"));
  }

  #[rxrust_macro::test]
  fn test_exhaust_map_unsubscribe_cancels_inner() {
    let mut outer = Local::subject::<i32, Infallible>();
    let inner = Local::subject::<i32, Infallible>();
    let inner_c = inner.clone();

    let sub = outer
      .clone()
      .exhaust_map(move |_| inner_c.clone())
      .subscribe(|_| {});
    outer.next(1);
    assert_eq!(inner.inner.subscriber_count(), 1);

    sub.unsubscribe();
    assert_eq!(inner.inner.subscriber_count(), 0);
  }
}
```

- [x] **Step 2: Register and add the trait method**

`src/ops.rs`: `pub mod exhaust_map;` / `pub use exhaust_map::*;`.

`src/observable.rs`, after `switch_map`:

```rust
  /// Map each item to an inner observable, ignoring items that arrive while
  /// an inner observable is still active
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter(vec![1, 2])
  ///   .exhaust_map(|v| Local::of(v * 10))
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 10, 20
  /// ```
  #[doc(alias = "exhaustMap")]
  fn exhaust_map<F, Out>(self, f: F) -> Self::With<ExhaustMap<Self::Inner, F>>
  where
    F: for<'a> FnMut(Self::Item<'a>) -> Out,
    Out: Context<Inner: ObservableType<Err = Self::Err> + 'static>,
  {
    self.transform(|source| ExhaustMap { source, func: f })
  }
```

- [x] **Step 3: Run, gate, commit**

Run: `cargo test --lib ops::exhaust_map && cargo test --doc exhaust_map`

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/exhaust_map.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.exhaust_map): add exhaust_map operator"
```

---

### Task 9: `audit`, `audit_time`, `audit_time_with`

**Files:**
- Create: `src/ops/audit.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `throttle_time_with`)

**Interfaces:**
- Produces: aliases `Audit<S, F> = Throttle<S, ThrottleWhenParam<F>>`, `AuditTime<S, D> = Throttle<S, D>`; methods `audit(selector)`, `audit_time(duration)`, `audit_time_with(duration, scheduler)` built on `throttle` with `ThrottleEdge::trailing()`.

- [x] **Step 1: Write the alias file with the deciding tests**

```rust
//! Audit operator implementation
//!
//! Emits the most recent item when a duration ends, then waits for the next
//! item to start a new duration. Built on `throttle` with a trailing edge.

use crate::ops::throttle::{Throttle, ThrottleWhenParam};

/// Emits the latest item when the selector's observable emits.
#[doc(alias = "audit")]
pub type Audit<S, F> = Throttle<S, ThrottleWhenParam<F>>;

/// Emits the latest item after a fixed duration.
#[doc(alias = "auditTime")]
pub type AuditTime<S, D> = Throttle<S, D>;

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::{context::TestCtx, prelude::*, scheduler::test_scheduler::TestScheduler};

  #[rxrust_macro::test]
  fn test_audit_time_emits_latest_at_window_end() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut subject = TestCtx::subject::<i32, Infallible>();
    let _sub = subject
      .clone()
      .audit_time(Duration::from_millis(100))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    subject.next(1);
    TestScheduler::advance_by(Duration::from_millis(50));
    subject.next(2);
    assert!(result.borrow().is_empty());
    TestScheduler::advance_by(Duration::from_millis(50));
    assert_eq!(*result.borrow(), vec![2]);

    // Silence: nothing more
    TestScheduler::advance_by(Duration::from_millis(200));
    assert_eq!(*result.borrow(), vec![2]);

    // A new item starts a new window
    subject.next(3);
    TestScheduler::advance_by(Duration::from_millis(100));
    assert_eq!(*result.borrow(), vec![2, 3]);
  }

  #[rxrust_macro::test]
  fn test_audit_time_completion_flushes_pending() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    let mut subject = TestCtx::subject::<i32, Infallible>();
    let _sub = subject
      .clone()
      .audit_time(Duration::from_millis(100))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    subject.next(1);
    subject.clone().complete();

    assert_eq!(*result.borrow(), vec![1]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_audit_with_selector() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut subject = TestCtx::subject::<i32, Infallible>();
    let _sub = subject
      .clone()
      .audit(|_| TestCtx::timer(Duration::from_millis(10)))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    subject.next(1);
    subject.next(2);
    TestScheduler::advance_by(Duration::from_millis(10));
    assert_eq!(*result.borrow(), vec![2]);
  }
}
```

- [x] **Step 2: Register and add the trait methods**

`src/ops.rs`: `pub mod audit;` / `pub use audit::*;`.

`src/observable.rs`, after `throttle_time_with`:

```rust
  /// Emit the most recent item when the observable returned by `selector`
  /// emits, then wait for the next item to start a new window
  ///
  /// Equivalent to `throttle(selector, ThrottleEdge::trailing())`.
  fn audit<F, Out>(self, selector: F) -> Self::With<Audit<Self::Inner, F>>
  where
    F: for<'a> FnMut(&Self::Item<'a>) -> Out,
    Out: Observable<Err = Self::Err>,
  {
    self.throttle(selector, ThrottleEdge::trailing())
  }

  /// Emit the most recent item once `duration` has passed since the first
  /// item of the window
  ///
  /// Equivalent to `throttle_time(duration, ThrottleEdge::trailing())`.
  #[doc(alias = "auditTime")]
  fn audit_time(self, duration: Duration) -> Self::With<AuditTime<Self::Inner, Self::With<Duration>>>
  where
    Self::With<Duration>: Context<Scheduler = Self::Scheduler>,
  {
    self.throttle_time(duration, ThrottleEdge::trailing())
  }

  /// [`Observable::audit_time`] with an explicit scheduler
  fn audit_time_with<Sch>(
    self, duration: Duration, scheduler: Sch,
  ) -> Self::With<AuditTime<Self::Inner, <Self::With<Duration> as Context>::With<Duration>>>
  where
    Self::With<Duration>: Context,
  {
    self.throttle_time_with(duration, ThrottleEdge::trailing(), scheduler)
  }
```

Match `audit_time_with`'s return type to whatever `throttle_time_with` returns in `src/observable.rs:1307`; read that signature and mirror it exactly. If the trailing-edge tests in Step 1 fail on semantics (a leading value is emitted, or the window is not restarted by a fresh item), replace the aliases with a dedicated `Audit` operator modeled on `debounce`'s task-handle pattern: store the latest value, schedule the emit task on the first item of a window, and clear the window when it fires.

- [x] **Step 3: Run, gate, commit**

Run: `cargo test --lib ops::audit && cargo test --doc audit`

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/audit.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.audit): add audit, audit_time and audit_time_with operators"
```

---

### Task 10: Documentation, integration tests, full matrix, PR

**Files:**
- Modify: `missing_features.md`, `guide/operators.md`, `CHANGELOG.md`, `tests/v1_integration.rs`, `docs/superpowers/plans/2026-09-07-operator-parity-1b.md`

- [x] **Step 1: Integration tests**

Append to `tests/v1_integration.rs`:

```rust
#[rxrust_macro::test]
fn test_share_replay_feeds_two_subscribers() {
  let a = Rc::new(RefCell::new(Vec::new()));
  let b = Rc::new(RefCell::new(Vec::new()));
  let a_c = a.clone();
  let b_c = b.clone();

  let shared = Local::from_iter(vec![1, 2, 3]).share_replay(1);
  shared.clone().subscribe(move |v| a_c.borrow_mut().push(v));
  shared.subscribe(move |v| b_c.borrow_mut().push(v));

  assert_eq!(*a.borrow(), vec![1, 2, 3]);
  assert_eq!(*b.borrow(), vec![3]);
}

#[rxrust_macro::test]
fn test_catch_error_after_throw_if_empty() {
  let result = Rc::new(RefCell::new(Vec::new()));
  let result_c = result.clone();

  Local::from_iter(Vec::<i32>::new())
    .map_err(|_: Infallible| String::new())
    .throw_if_empty(|| "empty".to_string())
    .catch_error(|e: String| Local::from_iter(vec![e.len() as i32]))
    .subscribe(move |v| result_c.borrow_mut().push(v));

  assert_eq!(*result.borrow(), vec![5]);
}
```

- [x] **Step 2: Bookkeeping**

`missing_features.md`:
- Creating: `Repeat` row to `[x]` with sub-bullet `- implemented as repeat(count) / repeat_forever`.
- Error handling: `Catch` row to `[x]` with sub-bullet `- implemented as catch_error`.
- Utility: `Timeout` row to `[x]` with sub-bullet `- timeout, timeout_with, timeout_or_else, timeout_or_else_with; TimeoutError`.
- Filtering, under Debounce: add `- [x] Audit / AuditTime (audit, audit_time)`.
- Transforming, under FlatMap: add `- [x] ExhaustMap (exhaust_map)`.
- Connectable: `Replay` row to `[x]` with `- publish_replay, share_replay`; add `- [x] Share (share)`, `- [x] PublishBehavior / PublishLast (publish_behavior, publish_last)`.
- Subjects: `AsyncSubject` and `ReplaySubject` rows to `[x]`.

`guide/operators.md`: add rows `catch_error`, `timeout`, `repeat`, `share` / `share_replay`, `exhaust_map`, `audit` / `audit_time` to the matching tables with one-line descriptions.

`CHANGELOG.md`, under Unreleased:
- In `### ✨ New Features` add `*   **RxJS Parity, Tier 1b**: `ReplaySubject`, `AsyncSubject`, `share`, `share_replay`, `publish_replay`, `publish_behavior`, `publish_last`, `catch_error`, `timeout` family, `repeat`, `repeat_forever`, `exhaust_map`, `audit`, `audit_time`.`
- In `### 💔 Sorry & Breaking Changes` add `*   **BehaviorSubject**: the current value now lives behind the context's shared pointer so every clone observes the latest value; the type is now `BehaviorSubject<P, V>` instead of `BehaviorSubject<Item, P>`. `ConnectableObservable` and `RefCount` are generic over the subject type; `multicast` accepts any subject.`

- [x] **Step 3: Full matrix**

```bash
cargo test
cargo +nightly test --all-features
cargo +nightly clippy --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
wasm-pack test --node
```

- [x] **Step 4: Commit, mark the plan complete, push, open the PR**

```bash
git add missing_features.md guide/operators.md CHANGELOG.md tests/v1_integration.rs
git commit -m "docs: record tier 1b subjects and operators in guide, changelog and missing_features"
sed -i '' 's/^- \[ \] \*\*Step/- [x] **Step/' docs/superpowers/plans/2026-09-07-operator-parity-1b.md
git add docs/superpowers/plans/2026-09-07-operator-parity-1b.md
git commit -m "docs(plans): mark operator parity 1b plan complete"
git push -u origin feat/operator-parity-tier1b
gh pr create --base feat/operator-parity-tier1 --title "feat(subject,ops): RxJS parity tier 1b, subjects, multicast, error handling, timing" --body "<summary listing the additions, the BehaviorSubject breaking change, the multicast generalization, and the test matrix results>"
```

---

## Self-review

**Spec coverage.** PR 1b section: BehaviorSubject fix (Task 1), ReplaySubject (3), AsyncSubject (4), generalized ConnectableObservable with `publish_replay`/`publish_behavior`/`publish_last` (2, 3, 4), `share`/`share_replay` (2, 3), `catch_error` (5), `timeout` family (6), `repeat`/`repeat_forever` (7), `exhaust_map` (8), `audit` family (9), docs and bookkeeping and integration tests (10).

**Placeholders.** Task 6 deliberately shows a placeholder `timeout_fire` that the same step tells the implementer to delete; every other step carries its code. Task 10's PR body is a description of what to write, since the matrix numbers are unknown until it runs.

**Type consistency.** `MulticastSubject`, `ConnectableObservable<S, Sub>`, `RefCount<S, Sub, ConnPtr>`, `PublishSubjectOf`, `ShareOf`, `ReplaySubjectOf`, `ShareReplayOf`, `AsyncSubjectOf`, `BehaviorSubjectOf`, `Terminal`, `ReplayBuffer`, `AsyncState`, `CatchError`, `Timeout`, `TimeoutError`, `default_timeout_error`, `Repeat`, `ExhaustMap`, `Audit`, `AuditTime` are each defined once and used by those names across tasks.
