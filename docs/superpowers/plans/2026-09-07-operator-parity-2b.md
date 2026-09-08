# Operator Parity PR 2b Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the windowing and flattening operators `window`, `window_count`, `window_time`, `buffer_when`, `buffer_toggle`, `delay_when`, `merge_scan`, `expand`, with docs and tests, as one PR stacked on tier 2a.

**Architecture:** `window`/`window_count` follow `group_by`'s context-marker pattern to emit `Subject`s as items and `buffer`'s two-observer subscription wiring. `buffer_when` re-arms its closing observable through a function pointer and a generation counter so stale closings are ignored. `buffer_toggle`, `delay_when`, `merge_scan` and `expand` keep one shared state plus a `DynamicSubscriptions` of boxed inner handles, like the N-ary operators of tier 1a; `expand` spawns inner subscriptions through a function pointer so its observer's bounds are not self-referential. All buffering operators require owned items via `S: for<'a> ObservableType<Item<'a> = Item>`.

**Tech Stack:** Rust 2024, stable + nightly, `rxrust_macro::test`, `TestCtx` virtual time. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-07-operator-parity-tier1-design.md`, section "Tier 2 / PR 2b".

## Global Constraints

- Branch `feat/operator-parity-tier2b`, based on `feat/operator-parity-tier2`; PR targets that branch until it merges.
- All conventions and lessons of the earlier plans apply. In particular: never unsubscribe the observable currently dispatching to you (drop the handle, rely on `is_closed`); skip inserting a handle that is already closed after a synchronous subscribe; hide tokio-based doctests from wasm.
- The closing observable of `buffer_when` must not emit synchronously while being subscribed (documented; RxJS has the same restriction).

## File Structure

| File | Responsibility |
| --- | --- |
| `src/ops/window.rs` | `Window`, `WindowState`, both observers, `never_errors`, `WindowSubjectOf`, `WindowTimer`, tests for window/window_count/window_time |
| `src/ops/window_count.rs` | `WindowCount` |
| `src/ops/buffer_when.rs` | `BufferWhen` |
| `src/ops/buffer_toggle.rs` | `BufferToggle` |
| `src/ops/delay_when.rs` | `DelayWhen` |
| `src/ops/merge_scan.rs` | `MergeScan` |
| `src/ops/expand.rs` | `Expand` |
| `src/observable.rs`, `src/ops.rs` | methods and registration |
| bookkeeping | `missing_features.md`, `guide/operators.md`, `CHANGELOG.md`, `tests/v1_integration.rs` |

---

### Task 1: `window`, `window_count`, `window_time`

**Files:** src/ops/window.rs (plus `WindowSubjectOf` and `WindowTimer` aliases) and src/ops/window_count.rs; methods after `buffer_time_max_with`.

Add to `src/ops/window.rs`: `pub type WindowSubjectOf<'a, O> = <O as Context>::With<PublishSubjectOf<'a, O>>;` and `pub type WindowTimer<Sch, Err> = MapErr<Interval<Sch>, fn(Infallible) -> Err>;`. Register `Window`, `WindowCount`, `WindowSubjectOf`, `WindowTimer`, `never_errors`, plus `Interval` and `MapErr` (already imported) in `src/observable.rs`.

- [ ] **Step 1: Operator file(s)**

`window.rs`:

```rust
//! Window operator implementation
//!
//! Splits the source into consecutive windows, each an observable of its own,
//! delimited by a notifier.

use std::{convert::Infallible, marker::PhantomData};

use crate::{
  context::{Context, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{IntoBoxedSubscription, Subscription, TupleSubscription},
};

/// Maps an impossible error into any error type; used to feed infallible
/// notifiers (timers) into operators that expect the source's error type.
pub fn never_errors<E>(never: Infallible) -> E { match never {} }

/// Window operator: Emit consecutive windows delimited by `notifier`
///
/// Each window is a `Subject` wrapped in the context, emitted as soon as it
/// opens; the first window opens at subscribe. When `notifier` emits, the
/// current window completes and a new one opens. Source completion or error
/// terminates the open window and then the outer stream. Items must be
/// `Clone`.
///
/// # Examples
///
/// ```rust
/// use std::{cell::RefCell, convert::Infallible, rc::Rc};
///
/// use rxrust::prelude::*;
///
/// let windows = Rc::new(RefCell::new(Vec::new()));
/// let mut source = Local::subject::<i32, Infallible>();
/// let mut boundary = Local::subject::<(), Infallible>();
///
/// let sink = windows.clone();
/// source
///   .clone()
///   .window(boundary.clone())
///   .subscribe(move |w: Local<_>| {
///     let bucket = Rc::new(RefCell::new(Vec::new()));
///     sink.borrow_mut().push(bucket.clone());
///     w.subscribe(move |v| bucket.borrow_mut().push(v));
///   });
///
/// source.next(1);
/// source.next(2);
/// boundary.next(());
/// source.next(3);
/// source.complete();
///
/// let seen: Vec<Vec<i32>> = windows.borrow().iter().map(|b| b.borrow().clone()).collect();
/// assert_eq!(seen, vec![vec![1, 2], vec![3]]);
/// ```
pub struct Window<S, N, CtxMarker> {
  pub source: S,
  pub notifier: N,
  _marker: PhantomData<fn() -> CtxMarker>,
}

impl<S: Clone, N: Clone, CtxMarker> Clone for Window<S, N, CtxMarker> {
  fn clone(&self) -> Self {
    Self { source: self.source.clone(), notifier: self.notifier.clone(), _marker: PhantomData }
  }
}

impl<S, N, CtxMarker> Window<S, N, CtxMarker> {
  /// Pairs a source with its boundary notifier.
  pub fn new(source: S, notifier: N) -> Self { Self { source, notifier, _marker: PhantomData } }
}

impl<S, N, CtxMarker> ObservableType for Window<S, N, CtxMarker>
where
  S: ObservableType,
  CtxMarker: Context,
{
  type Item<'m>
    = CtxMarker::With<CtxMarker::Inner>
  where
    Self: 'm;
  type Err = S::Err;
}

/// State shared by the source and notifier observers
pub struct WindowState<O, Sub> {
  observer: Option<O>,
  current: Option<Sub>,
}

/// Completes the open window, if any, and opens the next one.
pub(crate) fn rotate_window<CtxMarker, O, Item, Err>(state: &mut WindowState<O, CtxMarker::Inner>)
where
  CtxMarker: Context<Inner: Default + Clone + Observer<Item, Err>>,
  O: Observer<CtxMarker::With<CtxMarker::Inner>, Err>,
{
  if let Some(window) = state.current.take() {
    window.complete();
  }
  let Some(observer) = state.observer.as_mut() else { return };
  if observer.is_closed() {
    return;
  }
  let subject = CtxMarker::Inner::default();
  observer.next(CtxMarker::lift(subject.clone()));
  state.current = Some(subject);
}

/// Observer for the source
pub struct WindowSourceObserver<R, NotifierUnsub, CtxMarker> {
  state: R,
  notifier_unsub: NotifierUnsub,
  _marker: PhantomData<fn() -> CtxMarker>,
}

impl<R, NotifierUnsub, CtxMarker, O, Item, Err> Observer<Item, Err>
  for WindowSourceObserver<R, NotifierUnsub, CtxMarker>
where
  CtxMarker: Context<Inner: Observer<Item, Err> + Clone>,
  R: RcDerefMut<Target = WindowState<O, CtxMarker::Inner>>,
  O: Observer<CtxMarker::With<CtxMarker::Inner>, Err>,
  NotifierUnsub: Subscription,
  Err: Clone,
{
  fn next(&mut self, value: Item) {
    if let Some(window) = self.state.rc_deref_mut().current.as_mut() {
      window.next(value);
    }
  }

  fn error(self, err: Err) {
    self.notifier_unsub.unsubscribe();
    let mut state = self.state.rc_deref_mut();
    if let Some(window) = state.current.take() {
      window.error(err.clone());
    }
    if let Some(observer) = state.observer.take() {
      observer.error(err);
    }
  }

  fn complete(self) {
    self.notifier_unsub.unsubscribe();
    let mut state = self.state.rc_deref_mut();
    if let Some(window) = state.current.take() {
      window.complete();
    }
    if let Some(observer) = state.observer.take() {
      observer.complete();
    }
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref_mut()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

/// Observer for the boundary notifier
pub struct WindowNotifierObserver<R, SourceUnsub, CtxMarker, Item> {
  state: R,
  source_unsub: SourceUnsub,
  _marker: PhantomData<fn() -> (CtxMarker, Item)>,
}

impl<R, SourceUnsub, CtxMarker, Item, O, NotifyItem, Err> Observer<NotifyItem, Err>
  for WindowNotifierObserver<R, SourceUnsub, CtxMarker, Item>
where
  CtxMarker: Context<Inner: Default + Clone + Observer<Item, Err>>,
  R: RcDerefMut<Target = WindowState<O, CtxMarker::Inner>>,
  O: Observer<CtxMarker::With<CtxMarker::Inner>, Err>,
  SourceUnsub: Subscription,
  Err: Clone,
{
  fn next(&mut self, _value: NotifyItem) {
    rotate_window::<CtxMarker, O, Item, Err>(&mut self.state.rc_deref_mut());
  }

  fn error(self, err: Err) {
    self.source_unsub.unsubscribe();
    let mut state = self.state.rc_deref_mut();
    if let Some(window) = state.current.take() {
      window.error(err.clone());
    }
    if let Some(observer) = state.observer.take() {
      observer.error(err);
    }
  }

  fn complete(self) {
    // A completed boundary keeps the current window open until the source
    // ends, as in RxJS.
  }

  fn is_closed(&self) -> bool { self.state.rc_deref_mut().observer.is_none() }
}

type WState<C, CtxMarker> =
  <C as Context>::RcMut<WindowState<<C as Context>::Inner, <CtxMarker as Context>::Inner>>;
type WSourceObserver<C, CtxMarker> = WindowSourceObserver<
  WState<C, CtxMarker>,
  <C as Context>::RcMut<Option<<C as Context>::BoxedSubscription>>,
  CtxMarker,
>;
type WNotifierObserver<C, CtxMarker, SourceUnsub, Item> = WindowNotifierObserver<
  WState<C, CtxMarker>,
  <C as Context>::RcMut<Option<SourceUnsub>>,
  CtxMarker,
  Item,
>;

impl<S, N, CtxMarker, C, Item, SourceUnsub, NotifierUnsub> CoreObservable<C>
  for Window<S, N, CtxMarker>
where
  C: Context,
  CtxMarker: Context<Inner: Default + Clone + Observer<Item, S::Err>>,
  C::Inner: Observer<CtxMarker::With<CtxMarker::Inner>, S::Err>,
  S: for<'a> ObservableType<Item<'a> = Item>
    + CoreObservable<C::With<WSourceObserver<C, CtxMarker>>, Unsub = SourceUnsub>,
  N: CoreObservable<C::With<WNotifierObserver<C, CtxMarker, SourceUnsub, Item>>, Unsub = NotifierUnsub>,
  SourceUnsub: Subscription,
  NotifierUnsub: IntoBoxedSubscription<C::BoxedSubscription>,
  C::RcMut<Option<SourceUnsub>>: Subscription,
  C::RcMut<Option<C::BoxedSubscription>>: Subscription,
{
  type Unsub =
    TupleSubscription<C::RcMut<Option<SourceUnsub>>, C::RcMut<Option<C::BoxedSubscription>>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let Window { source, notifier, .. } = self;

    let state: WState<C, CtxMarker> =
      C::RcMut::from(WindowState { observer: Some(context.into_inner()), current: None });
    rotate_window::<CtxMarker, C::Inner, Item, S::Err>(&mut state.rc_deref_mut());

    let source_unsub_proxy: C::RcMut<Option<SourceUnsub>> = C::RcMut::from(None);
    let notifier_unsub_proxy: C::RcMut<Option<C::BoxedSubscription>> = C::RcMut::from(None);

    let source_observer = WindowSourceObserver {
      state: state.clone(),
      notifier_unsub: notifier_unsub_proxy.clone(),
      _marker: PhantomData,
    };
    let source_sub = source.subscribe(C::lift(source_observer));
    *source_unsub_proxy.rc_deref_mut() = Some(source_sub);

    let notifier_observer = WindowNotifierObserver {
      state: state.clone(),
      source_unsub: source_unsub_proxy.clone(),
      _marker: PhantomData,
    };
    let notifier_sub = notifier.subscribe(C::lift(notifier_observer));
    *notifier_unsub_proxy.rc_deref_mut() = Some(notifier_sub.into_boxed());

    TupleSubscription::new(source_unsub_proxy, notifier_unsub_proxy)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::{context::TestCtx, prelude::*, scheduler::test_scheduler::TestScheduler};

  type Buckets = Rc<RefCell<Vec<Rc<RefCell<Vec<i32>>>>>>;

  fn collect_windows<O>(windows: O) -> Buckets
  where
    O: Observable<Err = Infallible>,
    for<'a> O::Item<'a>: Observable<Err = Infallible> + 'static,
    for<'a, 'b> <O::Item<'a> as Observable>::Item<'b>: Into<i32>,
  {
    let buckets: Buckets = Rc::new(RefCell::new(Vec::new()));
    let sink = buckets.clone();
    windows.subscribe(move |w| {
      let bucket = Rc::new(RefCell::new(Vec::new()));
      sink.borrow_mut().push(bucket.clone());
      w.subscribe(move |v| bucket.borrow_mut().push(v.into()));
    });
    buckets
  }

  fn snapshot(buckets: &Buckets) -> Vec<Vec<i32>> {
    buckets.borrow().iter().map(|b| b.borrow().clone()).collect()
  }

  #[rxrust_macro::test]
  fn test_window_rotates_on_notifier() {
    let mut source = Local::subject::<i32, Infallible>();
    let mut boundary = Local::subject::<(), Infallible>();
    let buckets = collect_windows(source.clone().window(boundary.clone()));

    // The first window opens at subscribe
    assert_eq!(snapshot(&buckets), vec![Vec::<i32>::new()]);
    source.next(1);
    source.next(2);
    boundary.next(());
    source.next(3);
    boundary.next(());
    source.complete();

    assert_eq!(snapshot(&buckets), vec![vec![1, 2], vec![3], vec![]]);
  }

  #[rxrust_macro::test]
  fn test_window_completes_open_window_on_source_completion() {
    let completed = Rc::new(RefCell::new(0));
    let outer_done = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();
    let outer_c = outer_done.clone();
    let mut source = Local::subject::<i32, Infallible>();
    let boundary = Local::subject::<(), Infallible>();

    source
      .clone()
      .window(boundary.clone())
      .on_complete(move || *outer_c.borrow_mut() = true)
      .subscribe(move |w: Local<_>| {
        let c = completed_c.clone();
        w.on_complete(move || *c.borrow_mut() += 1).subscribe(|_| {});
      });

    source.next(1);
    source.complete();

    assert_eq!(*completed.borrow(), 1);
    assert!(*outer_done.borrow());
    assert_eq!(boundary.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_window_count_groups() {
    let buckets = collect_windows(Local::from_iter(vec![1, 2, 3, 4, 5]).window_count(2));
    assert_eq!(snapshot(&buckets), vec![vec![1, 2], vec![3, 4], vec![5]]);
  }

  #[rxrust_macro::test]
  fn test_window_time_rotates_on_virtual_clock() {
    TestScheduler::init();
    let mut source = TestCtx::subject::<i32, Infallible>();
    let buckets = collect_windows(source.clone().window_time(Duration::from_millis(100)));

    source.next(1);
    TestScheduler::advance_by(Duration::from_millis(100));
    source.next(2);
    source.next(3);
    TestScheduler::advance_by(Duration::from_millis(100));
    source.clone().complete();

    assert_eq!(snapshot(&buckets), vec![vec![1], vec![2, 3], vec![]]);
  }
}
```

`window_count.rs`:

```rust
//! WindowCount operator implementation
//!
//! Splits the source into windows of a fixed number of items.

use std::marker::PhantomData;

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// WindowCount operator: Emit a new window every `count` items
///
/// Each window is a `Subject` wrapped in the context, emitted when it opens;
/// the first window opens at subscribe. A `count` of zero behaves like one.
///
/// # Examples
///
/// ```rust
/// use std::{cell::RefCell, rc::Rc};
///
/// use rxrust::prelude::*;
///
/// let windows = Rc::new(RefCell::new(Vec::new()));
/// let sink = windows.clone();
/// Local::from_iter(vec![1, 2, 3])
///   .window_count(2)
///   .subscribe(move |w: Local<_>| {
///     let bucket = Rc::new(RefCell::new(Vec::new()));
///     sink.borrow_mut().push(bucket.clone());
///     w.subscribe(move |v| bucket.borrow_mut().push(v));
///   });
/// let seen: Vec<Vec<i32>> = windows.borrow().iter().map(|b| b.borrow().clone()).collect();
/// assert_eq!(seen, vec![vec![1, 2], vec![3]]);
/// ```
#[doc(alias = "windowCount")]
pub struct WindowCount<S, CtxMarker> {
  pub source: S,
  pub count: usize,
  _marker: PhantomData<fn() -> CtxMarker>,
}

impl<S: Clone, CtxMarker> Clone for WindowCount<S, CtxMarker> {
  fn clone(&self) -> Self {
    Self { source: self.source.clone(), count: self.count, _marker: PhantomData }
  }
}

impl<S, CtxMarker> WindowCount<S, CtxMarker> {
  /// Windows of `count` items over `source`.
  pub fn new(source: S, count: usize) -> Self {
    Self { source, count: count.max(1), _marker: PhantomData }
  }
}

impl<S, CtxMarker> ObservableType for WindowCount<S, CtxMarker>
where
  S: ObservableType,
  CtxMarker: Context,
{
  type Item<'m>
    = CtxMarker::With<CtxMarker::Inner>
  where
    Self: 'm;
  type Err = S::Err;
}

/// Observer that counts items into windows
pub struct WindowCountObserver<O, Sub, CtxMarker> {
  observer: O,
  current: Option<Sub>,
  count: usize,
  seen: usize,
  _marker: PhantomData<fn() -> CtxMarker>,
}

impl<O, CtxMarker> WindowCountObserver<O, CtxMarker::Inner, CtxMarker>
where
  CtxMarker: Context<Inner: Default + Clone>,
{
  fn open<Err>(&mut self)
  where
    O: Observer<CtxMarker::With<CtxMarker::Inner>, Err>,
  {
    if self.observer.is_closed() {
      return;
    }
    let subject = CtxMarker::Inner::default();
    self.observer.next(CtxMarker::lift(subject.clone()));
    self.current = Some(subject);
    self.seen = 0;
  }
}

impl<O, CtxMarker, Item, Err> Observer<Item, Err>
  for WindowCountObserver<O, CtxMarker::Inner, CtxMarker>
where
  CtxMarker: Context<Inner: Default + Clone + Observer<Item, Err>>,
  O: Observer<CtxMarker::With<CtxMarker::Inner>, Err>,
  Err: Clone,
{
  fn next(&mut self, value: Item) {
    if self.current.is_none() {
      self.open::<Err>();
    }
    if let Some(window) = self.current.as_mut() {
      window.next(value);
    }
    self.seen += 1;
    if self.seen >= self.count {
      if let Some(window) = self.current.take() {
        window.complete();
      }
    }
  }

  fn error(self, err: Err) {
    if let Some(window) = self.current {
      window.error(err.clone());
    }
    self.observer.error(err);
  }

  fn complete(self) {
    if let Some(window) = self.current {
      window.complete();
    }
    self.observer.complete();
  }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, CtxMarker, C, Item> CoreObservable<C> for WindowCount<S, CtxMarker>
where
  C: Context,
  CtxMarker: Context<Inner: Default + Clone + Observer<Item, S::Err>>,
  C::Inner: Observer<CtxMarker::With<CtxMarker::Inner>, S::Err>,
  S: for<'a> ObservableType<Item<'a> = Item>
    + CoreObservable<C::With<WindowCountObserver<C::Inner, CtxMarker::Inner, CtxMarker>>>,
  S::Err: Clone,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let WindowCount { source, count, .. } = self;
    let wrapped = context.transform(|observer| {
      let mut o = WindowCountObserver { observer, current: None, count, seen: 0, _marker: PhantomData };
      o.open::<S::Err>();
      o
    });
    source.subscribe(wrapped)
  }
}
```

- [ ] **Step 2: Trait methods**

```rust
  /// Split the source into consecutive windows delimited by `notifier`
  ///
  /// Each window is a `Subject` wrapped in the context and is emitted as it
  /// opens; the first opens at subscribe. Items must be `Clone`.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use std::convert::Infallible;
  ///
  /// use rxrust::prelude::*;
  ///
  /// let source = Local::subject::<i32, Infallible>();
  /// let boundary = Local::subject::<(), Infallible>();
  /// source
  ///   .clone()
  ///   .window(boundary.clone())
  ///   .subscribe(|w: Local<_>| {
  ///     w.subscribe(|v| println!("{}", v));
  ///   });
  /// ```
  fn window<'a, N>(
    self, notifier: N,
  ) -> Self::With<Window<Self::Inner, N::Inner, WindowSubjectOf<'a, Self>>>
  where
    N: Observable<Err = Self::Err, Inner: ObservableType>,
  {
    self.transform(|source| Window::new(source, notifier.into_inner()))
  }

  /// Split the source into windows of `count` items
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter(vec![1, 2, 3])
  ///   .window_count(2)
  ///   .subscribe(|w: Local<_>| {
  ///     w.subscribe(|v| println!("{}", v));
  ///   });
  /// ```
  #[doc(alias = "windowCount")]
  fn window_count<'a>(
    self, count: usize,
  ) -> Self::With<WindowCount<Self::Inner, WindowSubjectOf<'a, Self>>> {
    self.transform(|source| WindowCount::new(source, count))
  }

  /// Split the source into windows of `duration`
  ///
  /// # Examples
  ///
  /// ```rust,no_run
  /// use rxrust::prelude::*;
  ///
  /// # #[cfg(not(target_arch = "wasm32"))]
  /// # {
  /// # #[tokio::main(flavor = "local")]
  /// # async fn main() {
  /// Local::interval(Duration::from_millis(10))
  ///   .window_time(Duration::from_millis(100))
  ///   .subscribe(|w: Local<_>| {
  ///     w.subscribe(|v| println!("{}", v));
  ///   });
  /// # }
  /// # }
  /// ```
  #[doc(alias = "windowTime")]
  #[allow(clippy::type_complexity)]
  fn window_time<'a>(
    self, duration: Duration,
  ) -> Self::With<
    Window<Self::Inner, WindowTimer<Self::Scheduler, Self::Err>, WindowSubjectOf<'a, Self>>,
  > {
    let scheduler = self.scheduler().clone();
    self.window_time_with(duration, scheduler)
  }

  /// [`Observable::window_time`] with an explicit scheduler
  #[allow(clippy::type_complexity)]
  fn window_time_with<'a, Sch>(
    self, duration: Duration, scheduler: Sch,
  ) -> Self::With<Window<Self::Inner, WindowTimer<Sch, Self::Err>, WindowSubjectOf<'a, Self>>> {
    let timer = MapErr {
      source: Interval { period: duration, scheduler },
      func: never_errors::<Self::Err> as fn(std::convert::Infallible) -> Self::Err,
    };
    self.transform(|source| Window::new(source, timer))
  }
```

- [ ] **Step 3:** `lib_tests ops::window and ops::window_count`, `doc_tests window`, gate, commit `feat(ops.window): ...`.

---

### Task 2: `buffer_when` and `buffer_toggle`

**Files:** src/ops/buffer_when.rs, src/ops/buffer_toggle.rs; methods after `buffer_time_max_with`.

- [ ] **Step 1: Operator file(s)**

`buffer_when.rs`:

```rust
//! BufferWhen operator implementation
//!
//! Buffers items until a closing observable, obtained from a selector for
//! every buffer, emits.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{IntoBoxedSubscription, Subscription, TupleSubscription},
};

/// BufferWhen operator: Emit buffers closed by `closing_selector()`
///
/// A buffer opens at subscribe together with a closing observable from the
/// selector. When it emits, the buffer is emitted and a new buffer and
/// closing observable start. The closing observable's completion is ignored.
/// Source completion emits the last buffer. The closing observable must not
/// emit synchronously while it is being subscribed.
///
/// # Examples
///
/// ```rust
/// use std::{cell::RefCell, convert::Infallible, rc::Rc};
///
/// use rxrust::prelude::*;
///
/// let result = Rc::new(RefCell::new(Vec::new()));
/// let sink = result.clone();
/// let mut source = Local::subject::<i32, Infallible>();
/// let mut closing = Local::subject::<(), Infallible>();
/// let closing_c = closing.clone();
///
/// source
///   .clone()
///   .buffer_when(move || closing_c.clone())
///   .subscribe(move |b| sink.borrow_mut().push(b));
///
/// source.next(1);
/// source.next(2);
/// closing.next(());
/// source.next(3);
/// source.complete();
/// assert_eq!(*result.borrow(), vec![vec![1, 2], vec![3]]);
/// ```
#[doc(alias = "bufferWhen")]
#[derive(Clone)]
pub struct BufferWhen<S, F> {
  pub source: S,
  pub closing_selector: F,
}

impl<S, F> ObservableType for BufferWhen<S, F>
where
  S: ObservableType,
{
  type Item<'a>
    = Vec<S::Item<'a>>
  where
    Self: 'a;
  type Err = S::Err;
}

/// State shared by the source and closing observers
pub struct BufferWhenState<O, Item> {
  observer: Option<O>,
  buffer: Vec<Item>,
  generation: usize,
}

/// Observer for a closing observable
pub struct BufferWhenClosingObserver<StateRc, SelRc, SlotRc> {
  state: StateRc,
  selector: SelRc,
  slot: SlotRc,
  generation: usize,
  reopen: fn(StateRc, SelRc, SlotRc),
}

/// Opens the next buffer's closing observable.
fn open_closing<Out, StateRc, SelRc, SlotRc, O, Item, F, BoxedSub>(
  state: StateRc, selector: SelRc, slot: SlotRc,
) where
  StateRc: RcDerefMut<Target = BufferWhenState<O, Item>> + Clone,
  SelRc: RcDerefMut<Target = F> + Clone,
  SlotRc: RcDerefMut<Target = Option<BoxedSub>> + Clone,
  F: FnMut() -> Out,
  Out: Context<Inner: CoreObservable<Out::With<BufferWhenClosingObserver<StateRc, SelRc, SlotRc>>, Unsub: IntoBoxedSubscription<BoxedSub>>>,
{
  if state.rc_deref().observer.is_none() {
    return;
  }
  let generation = {
    let mut st = state.rc_deref_mut();
    st.generation += 1;
    st.generation
  };
  let closing = (selector.rc_deref_mut())().into_inner();
  let observer = BufferWhenClosingObserver {
    state: state.clone(),
    selector: selector.clone(),
    slot: slot.clone(),
    generation,
    reopen: open_closing::<Out, StateRc, SelRc, SlotRc, O, Item, F, BoxedSub>,
  };
  let unsub = closing.subscribe(Out::lift(observer));
  // A stale handle is simply replaced; the old closing sees `is_closed`.
  *slot.rc_deref_mut() = Some(unsub.into_boxed());
}

impl<StateRc, SelRc, SlotRc, O, Item, Err, NotifyItem> Observer<NotifyItem, Err>
  for BufferWhenClosingObserver<StateRc, SelRc, SlotRc>
where
  StateRc: RcDerefMut<Target = BufferWhenState<O, Item>> + Clone,
  O: Observer<Vec<Item>, Err>,
  SelRc: Clone,
  SlotRc: Clone,
{
  fn next(&mut self, _value: NotifyItem) {
    let buffer = {
      let mut st = self.state.rc_deref_mut();
      if st.generation != self.generation || st.observer.is_none() {
        return;
      }
      std::mem::take(&mut st.buffer)
    };
    if let Some(observer) = self.state.rc_deref_mut().observer.as_mut() {
      observer.next(buffer);
    }
    (self.reopen)(self.state.clone(), self.selector.clone(), self.slot.clone());
  }

  fn error(self, err: Err) {
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
  }

  fn complete(self) {
    // Closing completion without a value keeps the buffer open, as in RxJS.
  }

  fn is_closed(&self) -> bool {
    let st = self.state.rc_deref();
    st.generation != self.generation || st.observer.as_ref().is_none_or(|o| o.is_closed())
  }
}

/// Observer for the source
pub struct BufferWhenSourceObserver<StateRc, SlotRc> {
  state: StateRc,
  slot: SlotRc,
}

impl<StateRc, SlotRc, O, Item, Err> Observer<Item, Err> for BufferWhenSourceObserver<StateRc, SlotRc>
where
  StateRc: RcDerefMut<Target = BufferWhenState<O, Item>>,
  O: Observer<Vec<Item>, Err>,
  SlotRc: Subscription,
{
  fn next(&mut self, value: Item) { self.state.rc_deref_mut().buffer.push(value); }

  fn error(self, err: Err) {
    self.slot.unsubscribe();
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
  }

  fn complete(self) {
    self.slot.unsubscribe();
    let mut st = self.state.rc_deref_mut();
    let buffer = std::mem::take(&mut st.buffer);
    st.generation += 1;
    if let Some(mut observer) = st.observer.take() {
      observer.next(buffer);
      observer.complete();
    }
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

type StateRc<C, Item> = <C as Context>::RcMut<BufferWhenState<<C as Context>::Inner, Item>>;
type SelRc<C, F> = <C as Context>::RcMut<F>;
type SlotRc<C> = <C as Context>::RcMut<Option<<C as Context>::BoxedSubscription>>;

impl<S, F, C, Item, Out> CoreObservable<C> for BufferWhen<S, F>
where
  C: Context,
  S: for<'a> ObservableType<Item<'a> = Item>
    + CoreObservable<C::With<BufferWhenSourceObserver<StateRc<C, Item>, SlotRc<C>>>>,
  F: FnMut() -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<BufferWhenClosingObserver<StateRc<C, Item>, SelRc<C, F>, SlotRc<C>>>,
        Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
      >,
    >,
  C::Inner: Observer<Vec<Item>, S::Err>,
  SlotRc<C>: Subscription,
{
  type Unsub = TupleSubscription<S::Unsub, SlotRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let BufferWhen { source, closing_selector } = self;
    let state: StateRc<C, Item> = C::RcMut::from(BufferWhenState {
      observer: Some(context.into_inner()),
      buffer: Vec::new(),
      generation: 0,
    });
    let selector: SelRc<C, F> = C::RcMut::from(closing_selector);
    let slot: SlotRc<C> = C::RcMut::from(None);

    open_closing::<Out, _, _, _, C::Inner, Item, F, C::BoxedSubscription>(
      state.clone(),
      selector,
      slot.clone(),
    );

    let source_observer = BufferWhenSourceObserver { state, slot: slot.clone() };
    let source_unsub = source.subscribe(C::lift(source_observer));
    TupleSubscription::new(source_unsub, slot)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::{context::TestCtx, prelude::*, scheduler::test_scheduler::TestScheduler};

  #[rxrust_macro::test]
  fn test_buffer_when_closes_on_selector_emission() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let mut source = Local::subject::<i32, Infallible>();
    let mut closing = Local::subject::<(), Infallible>();
    let closing_c = closing.clone();

    source
      .clone()
      .buffer_when(move || closing_c.clone())
      .subscribe(move |b| result_c.borrow_mut().push(b));

    source.next(1);
    source.next(2);
    closing.next(());
    source.next(3);
    closing.next(());
    closing.next(());
    source.next(4);
    source.complete();

    assert_eq!(*result.borrow(), vec![vec![1, 2], vec![3], vec![], vec![4]]);
  }

  #[rxrust_macro::test]
  fn test_buffer_when_with_timer_on_virtual_clock() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let mut source = TestCtx::subject::<i32, Infallible>();

    let _sub = source
      .clone()
      .buffer_when(|| TestCtx::timer(Duration::from_millis(100)))
      .subscribe(move |b| result_c.borrow_mut().push(b));

    source.next(1);
    TestScheduler::advance_by(Duration::from_millis(100));
    source.next(2);
    source.next(3);
    TestScheduler::advance_by(Duration::from_millis(100));

    assert_eq!(*result.borrow(), vec![vec![1], vec![2, 3]]);
  }

  #[rxrust_macro::test]
  fn test_buffer_when_error_propagation_and_cleanup() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();
    let source = Local::subject::<i32, String>();
    let closing = Local::subject::<(), String>();
    let closing_c = closing.clone();

    source
      .clone()
      .buffer_when(move || closing_c.clone())
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});
    assert_eq!(closing.inner.subscriber_count(), 1);

    source.error("boom".to_string());
    assert_eq!(error.borrow().as_deref(), Some("boom"));
    assert_eq!(closing.inner.subscriber_count(), 0);
  }
}
```

`buffer_toggle.rs`:

```rust
//! BufferToggle operator implementation
//!
//! Opens a buffer for every item of an `openings` observable and closes each
//! with its own closing observable; buffers may overlap.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription, TupleSubscription},
};

/// BufferToggle operator: Overlapping buffers driven by openings and closings
///
/// Every item from `openings` starts a buffer, closed when
/// `closing_selector(opening_item)` emits or completes. Source items go into
/// every open buffer, so items must be `Clone`. Source completion emits the
/// open buffers in opening order.
///
/// # Examples
///
/// ```rust
/// use std::{cell::RefCell, convert::Infallible, rc::Rc};
///
/// use rxrust::prelude::*;
///
/// let result = Rc::new(RefCell::new(Vec::new()));
/// let sink = result.clone();
/// let mut source = Local::subject::<i32, Infallible>();
/// let mut openings = Local::subject::<(), Infallible>();
/// let mut closing = Local::subject::<(), Infallible>();
/// let closing_c = closing.clone();
///
/// source
///   .clone()
///   .buffer_toggle(openings.clone(), move |_| closing_c.clone())
///   .subscribe(move |b| sink.borrow_mut().push(b));
///
/// source.next(0); // no buffer open yet
/// openings.next(());
/// source.next(1);
/// source.next(2);
/// closing.next(());
/// source.complete();
/// assert_eq!(*result.borrow(), vec![vec![1, 2]]);
/// ```
#[doc(alias = "bufferToggle")]
#[derive(Clone)]
pub struct BufferToggle<S, Op, F> {
  pub source: S,
  pub openings: Op,
  pub closing_selector: F,
}

impl<S, Op, F> ObservableType for BufferToggle<S, Op, F>
where
  S: ObservableType,
{
  type Item<'a>
    = Vec<S::Item<'a>>
  where
    Self: 'a;
  type Err = S::Err;
}

/// State shared by every observer of the operator
pub struct BufferToggleState<O, Item> {
  observer: Option<O>,
  buffers: Vec<(usize, Vec<Item>)>,
}

impl<O, Item> BufferToggleState<O, Item> {
  fn close<Err>(&mut self, id: usize)
  where
    O: Observer<Vec<Item>, Err>,
  {
    let Some(pos) = self.buffers.iter().position(|(bid, _)| *bid == id) else { return };
    let (_, buffer) = self.buffers.remove(pos);
    if let Some(observer) = self.observer.as_mut() {
      observer.next(buffer);
    }
  }
}

/// Observer for one closing observable
pub struct ToggleClosingObserver<StateRc, SubsRc> {
  state: StateRc,
  subs: SubsRc,
  id: usize,
}

impl<StateRc, SubsRc, O, Item, Err, NotifyItem, U> Observer<NotifyItem, Err>
  for ToggleClosingObserver<StateRc, SubsRc>
where
  StateRc: RcDerefMut<Target = BufferToggleState<O, Item>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  O: Observer<Vec<Item>, Err>,
  U: Subscription,
{
  fn next(&mut self, _value: NotifyItem) {
    self.state.rc_deref_mut().close::<Err>(self.id);
    // Drop our own handle rather than unsubscribing mid-dispatch.
    self.subs.rc_deref_mut().remove(self.id);
  }

  fn error(self, err: Err) {
    self.subs.rc_deref_mut().remove(self.id);
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(self) {
    self.state.rc_deref_mut().close::<Err>(self.id);
    self.subs.rc_deref_mut().remove(self.id);
  }

  fn is_closed(&self) -> bool {
    let st = self.state.rc_deref();
    st.observer.as_ref().is_none_or(|o| o.is_closed())
      || !st.buffers.iter().any(|(bid, _)| *bid == self.id)
  }
}

/// Observer for the openings observable
pub struct ToggleOpeningsObserver<StateRc, SubsRc, F> {
  state: StateRc,
  subs: SubsRc,
  closing_selector: F,
}

impl<StateRc, SubsRc, F, O, Item, Err, OpenItem, Out, U> Observer<OpenItem, Err>
  for ToggleOpeningsObserver<StateRc, SubsRc, F>
where
  StateRc: RcDerefMut<Target = BufferToggleState<O, Item>> + Clone,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>> + Clone,
  O: Observer<Vec<Item>, Err>,
  F: FnMut(OpenItem) -> Out,
  Out: Context<Inner: CoreObservable<Out::With<ToggleClosingObserver<StateRc, SubsRc>>, Unsub: IntoBoxedSubscription<U>>>,
  U: Subscription,
{
  fn next(&mut self, opening: OpenItem) {
    if self.state.rc_deref().observer.is_none() {
      return;
    }
    let id = self.subs.rc_deref_mut().reserve_id();
    self.state.rc_deref_mut().buffers.push((id, Vec::new()));
    let closing = (self.closing_selector)(opening).into_inner();
    let observer = ToggleClosingObserver { state: self.state.clone(), subs: self.subs.clone(), id };
    let unsub = closing.subscribe(Out::lift(observer)).into_boxed();
    let still_open = self.state.rc_deref().buffers.iter().any(|(bid, _)| *bid == id);
    if still_open {
      self.subs.rc_deref_mut().insert(id, unsub);
    }
  }

  fn error(self, err: Err) {
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(self) {
    // Openings ending does not end the buffers already open.
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

/// Observer for the source
pub struct ToggleSourceObserver<StateRc, SubsRc, OpUnsub> {
  state: StateRc,
  subs: SubsRc,
  openings_unsub: OpUnsub,
}

impl<StateRc, SubsRc, OpUnsub, O, Item, Err, U> Observer<Item, Err>
  for ToggleSourceObserver<StateRc, SubsRc, OpUnsub>
where
  StateRc: RcDerefMut<Target = BufferToggleState<O, Item>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  O: Observer<Vec<Item>, Err>,
  OpUnsub: Subscription,
  U: Subscription,
  Item: Clone,
{
  fn next(&mut self, value: Item) {
    let mut st = self.state.rc_deref_mut();
    for (_, buffer) in st.buffers.iter_mut() {
      buffer.push(value.clone());
    }
  }

  fn error(self, err: Err) {
    self.openings_unsub.unsubscribe();
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(self) {
    self.openings_unsub.unsubscribe();
    let (buffers, observer) = {
      let mut st = self.state.rc_deref_mut();
      (std::mem::take(&mut st.buffers), st.observer.take())
    };
    if let Some(mut observer) = observer {
      for (_, buffer) in buffers {
        observer.next(buffer);
      }
      observer.complete();
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

type StateRc<C, Item> = <C as Context>::RcMut<BufferToggleState<<C as Context>::Inner, Item>>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;
type OpProxy<C> = <C as Context>::RcMut<Option<<C as Context>::BoxedSubscription>>;

impl<S, Op, F, C, Item, OpenItem, Out> CoreObservable<C> for BufferToggle<S, Op, F>
where
  C: Context,
  S: for<'a> ObservableType<Item<'a> = Item>
    + CoreObservable<C::With<ToggleSourceObserver<StateRc<C, Item>, SubsRc<C>, OpProxy<C>>>>,
  Op: for<'a> ObservableType<Item<'a> = OpenItem, Err = S::Err>
    + CoreObservable<
      C::With<ToggleOpeningsObserver<StateRc<C, Item>, SubsRc<C>, F>>,
      Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
    >,
  F: FnMut(OpenItem) -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<ToggleClosingObserver<StateRc<C, Item>, SubsRc<C>>>,
        Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
      >,
    >,
  C::Inner: Observer<Vec<Item>, S::Err>,
  OpProxy<C>: Subscription,
  Item: Clone,
{
  type Unsub = TupleSubscription<SourceWithDynamicSubs<S::Unsub, SubsRc<C>>, OpProxy<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let BufferToggle { source, openings, closing_selector } = self;
    let state: StateRc<C, Item> = C::RcMut::from(BufferToggleState {
      observer: Some(context.into_inner()),
      buffers: Vec::new(),
    });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());
    let op_proxy: OpProxy<C> = C::RcMut::from(None);

    let openings_observer =
      ToggleOpeningsObserver { state: state.clone(), subs: subs.clone(), closing_selector };
    let op_unsub = openings.subscribe(C::lift(openings_observer)).into_boxed();
    *op_proxy.rc_deref_mut() = Some(op_unsub);

    let source_observer =
      ToggleSourceObserver { state, subs: subs.clone(), openings_unsub: op_proxy.clone() };
    let source_unsub = source.subscribe(C::lift(source_observer));

    TupleSubscription::new(SourceWithDynamicSubs::new(source_unsub, subs), op_proxy)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_buffer_toggle_overlapping_buffers() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let mut source = Local::subject::<i32, Infallible>();
    let mut openings = Local::subject::<u8, Infallible>();
    let mut close_a = Local::subject::<(), Infallible>();
    let mut close_b = Local::subject::<(), Infallible>();
    let (ca, cb) = (close_a.clone(), close_b.clone());

    source
      .clone()
      .buffer_toggle(openings.clone(), move |which| if which == 0 { ca.clone() } else { cb.clone() })
      .subscribe(move |b| result_c.borrow_mut().push(b));

    source.next(0);
    openings.next(0);
    source.next(1);
    openings.next(1);
    source.next(2);
    close_a.next(());
    source.next(3);
    close_b.next(());
    source.next(4);
    source.complete();

    assert_eq!(*result.borrow(), vec![vec![1, 2], vec![2, 3]]);
    assert_eq!(openings.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_buffer_toggle_flushes_open_buffers_on_completion() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let mut source = Local::subject::<i32, Infallible>();
    let mut openings = Local::subject::<(), Infallible>();
    let closing = Local::subject::<(), Infallible>();
    let closing_c = closing.clone();

    source
      .clone()
      .buffer_toggle(openings.clone(), move |_| closing_c.clone())
      .subscribe(move |b| result_c.borrow_mut().push(b));

    openings.next(());
    source.next(1);
    openings.next(());
    source.next(2);
    source.complete();

    assert_eq!(*result.borrow(), vec![vec![1, 2], vec![2]]);
    assert_eq!(closing.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_buffer_toggle_closing_completion_closes_buffer() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let mut source = Local::subject::<i32, Infallible>();
    let mut openings = Local::subject::<(), Infallible>();

    source
      .clone()
      .buffer_toggle(openings.clone(), |_| Local::from_iter(Vec::<()>::new()))
      .subscribe(move |b| result_c.borrow_mut().push(b));

    openings.next(());
    source.next(1);

    // The closing completed synchronously, so the buffer closed empty
    assert_eq!(*result.borrow(), vec![Vec::<i32>::new()]);
  }

  #[rxrust_macro::test]
  fn test_buffer_toggle_error_propagation() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();
    let source = Local::subject::<i32, String>();
    let openings = Local::subject::<(), String>();

    source
      .clone()
      .buffer_toggle(openings.clone(), |_| Local::of(()).map_err(|_: Infallible| String::new()))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    source.error("boom".to_string());
    assert_eq!(error.borrow().as_deref(), Some("boom"));
    assert_eq!(openings.inner.subscriber_count(), 0);
  }
}
```

- [ ] **Step 2: Trait methods**

```rust
  /// Buffer items until the observable returned by `closing_selector` emits,
  /// then start a new buffer with a fresh closing observable
  ///
  /// # Examples
  ///
  /// ```rust,no_run
  /// use rxrust::prelude::*;
  ///
  /// # #[cfg(not(target_arch = "wasm32"))]
  /// # {
  /// # #[tokio::main(flavor = "local")]
  /// # async fn main() {
  /// Local::interval(Duration::from_millis(10))
  ///   .buffer_when(|| Local::timer(Duration::from_millis(100)))
  ///   .subscribe(|b| println!("{:?}", b));
  /// # }
  /// # }
  /// ```
  #[doc(alias = "bufferWhen")]
  fn buffer_when<F, Out>(self, closing_selector: F) -> Self::With<BufferWhen<Self::Inner, F>>
  where
    F: FnMut() -> Out,
    Out: Context<Inner: ObservableType>,
  {
    self.transform(|source| BufferWhen { source, closing_selector })
  }

  /// Open a buffer for every item of `openings`, closed by
  /// `closing_selector(item)`; buffers may overlap
  ///
  /// # Examples
  ///
  /// ```rust
  /// use std::convert::Infallible;
  ///
  /// use rxrust::prelude::*;
  ///
  /// let source = Local::subject::<i32, Infallible>();
  /// let openings = Local::subject::<(), Infallible>();
  /// source
  ///   .clone()
  ///   .buffer_toggle(openings.clone(), |_| Local::of(()))
  ///   .subscribe(|b| println!("{:?}", b));
  /// ```
  #[doc(alias = "bufferToggle")]
  fn buffer_toggle<Op, F, Out>(
    self, openings: Op, closing_selector: F,
  ) -> Self::With<BufferToggle<Self::Inner, Op::Inner, F>>
  where
    Op: Observable<Err = Self::Err, Inner: ObservableType>,
    F: for<'a> FnMut(Op::Item<'a>) -> Out,
    Out: Context<Inner: ObservableType>,
  {
    self.transform(|source| BufferToggle { source, openings: openings.into_inner(), closing_selector })
  }
```

- [ ] **Step 3:** `lib_tests ops::buffer_when and ops::buffer_toggle`, `doc_tests buffer_when`, gate, commit `feat(ops.buffer_when): ...`.

---

### Task 3: `delay_when`

**Files:** src/ops/delay_when.rs; method after `delay_subscription_with`.

- [ ] **Step 1: Operator file(s)**

`delay_when.rs`:

```rust
//! DelayWhen operator implementation
//!
//! Delays each item until an observable chosen for that item emits or
//! completes.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription},
};

/// DelayWhen operator: Delay every item by its own duration observable
///
/// For each item, `selector(&item)` returns an observable; the item is
/// emitted when that observable first emits or completes, as in RxJS 7.
/// Items may be reordered if their delays differ. Completes once the source
/// has completed and every pending delay has resolved.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter(vec![1, 2])
///   .delay_when(|_| Local::of(()))
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 2]);
/// ```
#[doc(alias = "delayWhen")]
#[derive(Clone)]
pub struct DelayWhen<S, F> {
  pub source: S,
  pub selector: F,
}

impl<S, F> ObservableType for DelayWhen<S, F>
where
  S: ObservableType,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// State shared by the source and delay observers
pub struct DelayWhenState<O> {
  observer: Option<O>,
  pending: usize,
  outer_done: bool,
}

impl<O> DelayWhenState<O> {
  fn complete_if_done<Item, Err>(&mut self)
  where
    O: Observer<Item, Err>,
  {
    if self.outer_done && self.pending == 0
      && let Some(observer) = self.observer.take()
    {
      observer.complete();
    }
  }
}

/// Observer for one item's delay observable
pub struct DelayWhenInnerObserver<StateRc, SubsRc, Item> {
  state: StateRc,
  subs: SubsRc,
  id: usize,
  value: Option<Item>,
}

impl<StateRc, SubsRc, Item, O, Err, U> DelayWhenInnerObserver<StateRc, SubsRc, Item>
where
  StateRc: RcDerefMut<Target = DelayWhenState<O>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  O: Observer<Item, Err>,
  U: Subscription,
{
  fn fire<E>(&mut self) {
    let Some(value) = self.value.take() else { return };
    let mut st = self.state.rc_deref_mut();
    if let Some(observer) = st.observer.as_mut() {
      observer.next(value);
    }
    st.pending -= 1;
    st.complete_if_done::<Item, Err>();
    drop(st);
    // Drop our own handle rather than unsubscribing mid-dispatch.
    self.subs.rc_deref_mut().remove(self.id);
  }
}

impl<StateRc, SubsRc, Item, O, Err, DelayItem, U> Observer<DelayItem, Err>
  for DelayWhenInnerObserver<StateRc, SubsRc, Item>
where
  StateRc: RcDerefMut<Target = DelayWhenState<O>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  O: Observer<Item, Err>,
  U: Subscription,
{
  fn next(&mut self, _value: DelayItem) { self.fire::<Err>(); }

  fn error(self, err: Err) {
    self.subs.rc_deref_mut().remove(self.id);
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(mut self) { self.fire::<Err>(); }

  fn is_closed(&self) -> bool {
    self.value.is_none()
      || self
        .state
        .rc_deref()
        .observer
        .as_ref()
        .is_none_or(|o| o.is_closed())
  }
}

/// Observer for the source
pub struct DelayWhenSourceObserver<StateRc, SubsRc, F> {
  state: StateRc,
  subs: SubsRc,
  selector: F,
}

impl<StateRc, SubsRc, F, O, Item, Err, Out, U> Observer<Item, Err>
  for DelayWhenSourceObserver<StateRc, SubsRc, F>
where
  StateRc: RcDerefMut<Target = DelayWhenState<O>> + Clone,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>> + Clone,
  O: Observer<Item, Err>,
  F: FnMut(&Item) -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<DelayWhenInnerObserver<StateRc, SubsRc, Item>>,
        Unsub: IntoBoxedSubscription<U>,
      >,
    >,
  U: Subscription,
{
  fn next(&mut self, value: Item) {
    if self.state.rc_deref().observer.is_none() {
      return;
    }
    let delay = (self.selector)(&value).into_inner();
    let id = self.subs.rc_deref_mut().reserve_id();
    self.state.rc_deref_mut().pending += 1;
    let observer =
      DelayWhenInnerObserver { state: self.state.clone(), subs: self.subs.clone(), id, value: Some(value) };
    let unsub = delay.subscribe(Out::lift(observer)).into_boxed();
    // A delay that resolved synchronously has nothing left to cancel.
    if !unsub.is_closed() {
      self.subs.rc_deref_mut().insert(id, unsub);
    }
  }

  fn error(self, err: Err) {
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(self) {
    let mut st = self.state.rc_deref_mut();
    st.outer_done = true;
    st.complete_if_done::<Item, Err>();
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

type StateRc<C> = <C as Context>::RcMut<DelayWhenState<<C as Context>::Inner>>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;

impl<S, F, C, Item, Out> CoreObservable<C> for DelayWhen<S, F>
where
  C: Context,
  S: for<'a> ObservableType<Item<'a> = Item>
    + CoreObservable<C::With<DelayWhenSourceObserver<StateRc<C>, SubsRc<C>, F>>>,
  F: FnMut(&Item) -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<DelayWhenInnerObserver<StateRc<C>, SubsRc<C>, Item>>,
        Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
      >,
    >,
  C::Inner: Observer<Item, S::Err>,
{
  type Unsub = SourceWithDynamicSubs<S::Unsub, SubsRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let DelayWhen { source, selector } = self;
    let state: StateRc<C> = C::RcMut::from(DelayWhenState {
      observer: Some(context.into_inner()),
      pending: 0,
      outer_done: false,
    });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());
    let observer = DelayWhenSourceObserver { state, subs: subs.clone(), selector };
    let source_unsub = source.subscribe(C::lift(observer));
    SourceWithDynamicSubs::new(source_unsub, subs)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::{context::TestCtx, prelude::*, scheduler::test_scheduler::TestScheduler};

  #[rxrust_macro::test]
  fn test_delay_when_reorders_by_delay() {
    TestScheduler::init();
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    let _sub = TestCtx::from_iter(vec![30u64, 10, 20])
      .delay_when(|ms| TestCtx::timer(Duration::from_millis(*ms)))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert!(result.borrow().is_empty());
    TestScheduler::advance_by(Duration::from_millis(10));
    assert_eq!(*result.borrow(), vec![10]);
    assert!(!*completed.borrow());
    TestScheduler::advance_by(Duration::from_millis(20));
    assert_eq!(*result.borrow(), vec![10, 20, 30]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_delay_when_completion_of_duration_emits() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter(vec![1, 2])
      .delay_when(|_| Local::from_iter(Vec::<()>::new()))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
  }

  #[rxrust_macro::test]
  fn test_delay_when_error_from_duration() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    Local::from_iter(vec![1])
      .map_err(|_: Infallible| String::new())
      .delay_when(|_| Local::throw_err("late".to_string()).map(|_| ()))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(error.borrow().as_deref(), Some("late"));
  }

  #[rxrust_macro::test]
  fn test_delay_when_unsubscribe_cancels_pending() {
    let source = Local::subject::<i32, Infallible>();
    let delay = Local::subject::<(), Infallible>();
    let delay_c = delay.clone();
    let mut source_emitter = source.clone();

    let sub = source
      .clone()
      .delay_when(move |_| delay_c.clone())
      .subscribe(|_| {});
    source_emitter.next(1);
    assert_eq!(delay.inner.subscriber_count(), 1);

    sub.unsubscribe();
    assert_eq!(delay.inner.subscriber_count(), 0);
    assert_eq!(source.inner.subscriber_count(), 0);
  }
}
```

- [ ] **Step 2: Trait methods**

```rust
  /// Delay each item until the observable returned by `selector(&item)`
  /// first emits or completes
  ///
  /// # Examples
  ///
  /// ```rust,no_run
  /// use rxrust::prelude::*;
  ///
  /// # #[cfg(not(target_arch = "wasm32"))]
  /// # {
  /// # #[tokio::main(flavor = "local")]
  /// # async fn main() {
  /// Local::from_iter(vec![30u64, 10])
  ///   .delay_when(|ms| Local::timer(Duration::from_millis(*ms)))
  ///   .subscribe(|v| println!("{}", v)); // 10, then 30
  /// # }
  /// # }
  /// ```
  #[doc(alias = "delayWhen")]
  fn delay_when<F, Out>(self, selector: F) -> Self::With<DelayWhen<Self::Inner, F>>
  where
    F: for<'a> FnMut(&Self::Item<'a>) -> Out,
    Out: Context<Inner: ObservableType>,
  {
    self.transform(|source| DelayWhen { source, selector })
  }
```

- [ ] **Step 3:** `lib_tests ops::delay_when`, `doc_tests delay_when`, gate, commit `feat(ops.delay_when): ...`.

---

### Task 4: `merge_scan`

**Files:** src/ops/merge_scan.rs; method after `scan_map`.

- [ ] **Step 1: Operator file(s)**

`merge_scan.rs`:

```rust
//! MergeScan operator implementation
//!
//! Like `scan`, but the accumulator function returns an observable whose
//! emissions become the new accumulator.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription},
};

/// MergeScan operator: Accumulate through observables
///
/// For each item, `f(acc, item)` returns an observable of accumulators; each
/// of its emissions becomes the current accumulator and is emitted. Inner
/// observables run concurrently. Completes when the source and every inner
/// observable have completed.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter(vec![1, 2, 3])
///   .merge_scan(0, |acc, v| Local::of(acc + v))
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 3, 6]);
/// ```
#[doc(alias = "mergeScan")]
#[derive(Clone)]
pub struct MergeScan<S, F, Acc> {
  pub source: S,
  pub func: F,
  pub seed: Acc,
}

impl<S, F, Acc> ObservableType for MergeScan<S, F, Acc>
where
  S: ObservableType,
{
  type Item<'a>
    = Acc
  where
    Self: 'a;
  type Err = S::Err;
}

/// State shared by the source and inner observers
pub struct MergeScanState<O, Acc> {
  observer: Option<O>,
  acc: Acc,
  active: usize,
  outer_done: bool,
}

impl<O, Acc> MergeScanState<O, Acc> {
  fn complete_if_done<Err>(&mut self)
  where
    O: Observer<Acc, Err>,
  {
    if self.outer_done && self.active == 0
      && let Some(observer) = self.observer.take()
    {
      observer.complete();
    }
  }
}

/// Observer for one inner observable
pub struct MergeScanInnerObserver<StateRc, SubsRc> {
  state: StateRc,
  subs: SubsRc,
  id: usize,
}

impl<StateRc, SubsRc, O, Acc, Err, U> Observer<Acc, Err> for MergeScanInnerObserver<StateRc, SubsRc>
where
  StateRc: RcDerefMut<Target = MergeScanState<O, Acc>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  O: Observer<Acc, Err>,
  Acc: Clone,
  U: Subscription,
{
  fn next(&mut self, value: Acc) {
    let mut st = self.state.rc_deref_mut();
    if st.observer.is_none() {
      return;
    }
    st.acc = value.clone();
    if let Some(observer) = st.observer.as_mut() {
      observer.next(value);
    }
  }

  fn error(self, err: Err) {
    self.subs.rc_deref_mut().remove(self.id);
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(self) {
    self.subs.rc_deref_mut().remove(self.id);
    let mut st = self.state.rc_deref_mut();
    st.active = st.active.saturating_sub(1);
    st.complete_if_done::<Err>();
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

/// Observer for the source
pub struct MergeScanSourceObserver<StateRc, SubsRc, F> {
  state: StateRc,
  subs: SubsRc,
  func: F,
}

impl<StateRc, SubsRc, F, O, Acc, Item, Err, Out, U> Observer<Item, Err>
  for MergeScanSourceObserver<StateRc, SubsRc, F>
where
  StateRc: RcDerefMut<Target = MergeScanState<O, Acc>> + Clone,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>> + Clone,
  O: Observer<Acc, Err>,
  Acc: Clone,
  F: FnMut(Acc, Item) -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<MergeScanInnerObserver<StateRc, SubsRc>>,
        Unsub: IntoBoxedSubscription<U>,
      >,
    >,
  U: Subscription,
{
  fn next(&mut self, value: Item) {
    let acc = {
      let st = self.state.rc_deref();
      if st.observer.is_none() {
        return;
      }
      st.acc.clone()
    };
    let inner = (self.func)(acc, value).into_inner();
    let id = self.subs.rc_deref_mut().reserve_id();
    self.state.rc_deref_mut().active += 1;
    let observer = MergeScanInnerObserver { state: self.state.clone(), subs: self.subs.clone(), id };
    let unsub = inner.subscribe(Out::lift(observer)).into_boxed();
    if !unsub.is_closed() {
      self.subs.rc_deref_mut().insert(id, unsub);
    }
  }

  fn error(self, err: Err) {
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(self) {
    let mut st = self.state.rc_deref_mut();
    st.outer_done = true;
    st.complete_if_done::<Err>();
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

type StateRc<C, Acc> = <C as Context>::RcMut<MergeScanState<<C as Context>::Inner, Acc>>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;

impl<S, F, Acc, C, Item, Out> CoreObservable<C> for MergeScan<S, F, Acc>
where
  C: Context,
  S: for<'a> ObservableType<Item<'a> = Item>
    + CoreObservable<C::With<MergeScanSourceObserver<StateRc<C, Acc>, SubsRc<C>, F>>>,
  F: FnMut(Acc, Item) -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<MergeScanInnerObserver<StateRc<C, Acc>, SubsRc<C>>>,
        Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
      >,
    >,
  C::Inner: Observer<Acc, S::Err>,
  Acc: Clone,
{
  type Unsub = SourceWithDynamicSubs<S::Unsub, SubsRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let MergeScan { source, func, seed } = self;
    let state: StateRc<C, Acc> = C::RcMut::from(MergeScanState {
      observer: Some(context.into_inner()),
      acc: seed,
      active: 0,
      outer_done: false,
    });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());
    let observer = MergeScanSourceObserver { state, subs: subs.clone(), func };
    let source_unsub = source.subscribe(C::lift(observer));
    SourceWithDynamicSubs::new(source_unsub, subs)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_merge_scan_sync_accumulates() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::from_iter(vec![1, 2, 3])
      .merge_scan(0, |acc, v| Local::of(acc + v))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 3, 6]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_merge_scan_multiple_inner_emissions() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter(vec![1, 2])
      .merge_scan(0, |acc, v| Local::from_iter(vec![acc + v, acc + v * 10]))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    // 0+1=1, 0+10=10; then acc=10: 10+2=12, 10+20=30
    assert_eq!(*result.borrow(), vec![1, 10, 12, 30]);
  }

  #[rxrust_macro::test]
  fn test_merge_scan_waits_for_inner_completion() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();
    let mut source = Local::subject::<i32, Infallible>();
    let inner = Local::subject::<i32, Infallible>();
    let inner_c = inner.clone();

    source
      .clone()
      .merge_scan(0, move |_, _| inner_c.clone())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    source.next(1);
    source.clone().complete();
    assert!(!*completed.borrow());
    inner.complete();
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_merge_scan_inner_error() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    Local::from_iter(vec![1])
      .map_err(|_: Infallible| String::new())
      .merge_scan(0, |_, _| Local::throw_err("boom".to_string()).map(|_| 0))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(error.borrow().as_deref(), Some("boom"));
  }
}
```

- [ ] **Step 2: Trait methods**

```rust
  /// Accumulate through observables: `f(acc, item)` returns an observable
  /// whose emissions become the new accumulator and are emitted
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter(vec![1, 2, 3])
  ///   .merge_scan(0, |acc, v| Local::of(acc + v))
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 1, 3, 6
  /// ```
  #[doc(alias = "mergeScan")]
  fn merge_scan<Acc, F, Out>(self, seed: Acc, f: F) -> Self::With<MergeScan<Self::Inner, F, Acc>>
  where
    Acc: Clone,
    F: for<'a> FnMut(Acc, Self::Item<'a>) -> Out,
    Out: Context<Inner: ObservableType>,
  {
    self.transform(|source| MergeScan { source, func: f, seed })
  }
```

- [ ] **Step 3:** `lib_tests ops::merge_scan`, `doc_tests merge_scan`, gate, commit `feat(ops.merge_scan): ...`.

---

### Task 5: `expand`

**Files:** src/ops/expand.rs; method after `exhaust_map`.

- [ ] **Step 1: Operator file(s)**

`expand.rs`:

```rust
//! Expand operator implementation
//!
//! Recursively projects every emitted item into an observable and merges
//! the results.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription},
};

/// Expand operator: Recursive `flat_map`
///
/// Emits every source item, then feeds each emitted item (from the source or
/// from an inner observable) to `f` and merges the results, recursively.
/// Items must be `Clone`. Completes when the source and every inner
/// observable have completed; an inner that never completes keeps the
/// expansion alive, and unbounded recursion must be cut with an empty inner.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::of(1)
///   .expand(|v| if v < 8 { Local::from_iter(vec![v * 2]) } else { Local::from_iter(vec![]) })
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 2, 4, 8]);
/// ```
#[derive(Clone)]
pub struct Expand<S, F> {
  pub source: S,
  pub func: F,
}

impl<S, F> ObservableType for Expand<S, F>
where
  S: ObservableType,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// State shared by every observer of the expansion
pub struct ExpandState<O> {
  observer: Option<O>,
  active: usize,
  outer_done: bool,
}

impl<O> ExpandState<O> {
  fn complete_if_done<Item, Err>(&mut self)
  where
    O: Observer<Item, Err>,
  {
    if self.outer_done && self.active == 0
      && let Some(observer) = self.observer.take()
    {
      observer.complete();
    }
  }
}

/// Observer for the source and for every inner observable
///
/// `spawn` is a function pointer instantiated where the inner observable's
/// bounds are known, so this observer's own bounds stay non-recursive.
pub struct ExpandObserver<StateRc, SubsRc, FuncRc, Item> {
  state: StateRc,
  subs: SubsRc,
  func: FuncRc,
  /// `None` for the source observer, `Some(id)` for an inner one
  id: Option<usize>,
  spawn: fn(&StateRc, &SubsRc, &FuncRc, Item),
}

/// Emits `value` downstream and subscribes its expansion.
fn spawn_expansion<StateRc, SubsRc, FuncRc, O, F, Item, Err, Out, U>(
  state: &StateRc, subs: &SubsRc, func: &FuncRc, value: Item,
) where
  StateRc: RcDerefMut<Target = ExpandState<O>> + Clone,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>> + Clone,
  FuncRc: RcDerefMut<Target = F> + Clone,
  O: Observer<Item, Err>,
  F: FnMut(Item) -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<ExpandObserver<StateRc, SubsRc, FuncRc, Item>>,
        Unsub: IntoBoxedSubscription<U>,
      >,
    >,
  U: Subscription,
  Item: Clone,
{
  {
    let mut st = state.rc_deref_mut();
    let Some(observer) = st.observer.as_mut() else { return };
    observer.next(value.clone());
    st.active += 1;
  }
  let inner = (func.rc_deref_mut())(value).into_inner();
  let id = subs.rc_deref_mut().reserve_id();
  let observer = ExpandObserver {
    state: state.clone(),
    subs: subs.clone(),
    func: func.clone(),
    id: Some(id),
    spawn: spawn_expansion::<StateRc, SubsRc, FuncRc, O, F, Item, Err, Out, U>,
  };
  let unsub = inner.subscribe(Out::lift(observer)).into_boxed();
  if !unsub.is_closed() {
    subs.rc_deref_mut().insert(id, unsub);
  }
}

impl<StateRc, SubsRc, FuncRc, O, Item, Err, U> Observer<Item, Err>
  for ExpandObserver<StateRc, SubsRc, FuncRc, Item>
where
  StateRc: RcDerefMut<Target = ExpandState<O>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  O: Observer<Item, Err>,
  U: Subscription,
{
  fn next(&mut self, value: Item) { (self.spawn)(&self.state, &self.subs, &self.func, value); }

  fn error(self, err: Err) {
    if let Some(id) = self.id {
      self.subs.rc_deref_mut().remove(id);
    }
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.subs.rc_deref_mut().unsubscribe_all();
  }

  fn complete(self) {
    if let Some(id) = self.id {
      self.subs.rc_deref_mut().remove(id);
    }
    let mut st = self.state.rc_deref_mut();
    match self.id {
      None => st.outer_done = true,
      Some(_) => st.active = st.active.saturating_sub(1),
    }
    st.complete_if_done::<Item, Err>();
  }

  fn is_closed(&self) -> bool {
    self
      .state
      .rc_deref()
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

type StateRc<C> = <C as Context>::RcMut<ExpandState<<C as Context>::Inner>>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;
type FuncRc<C, F> = <C as Context>::RcMut<F>;
type Obs<C, F, Item> = ExpandObserver<StateRc<C>, SubsRc<C>, FuncRc<C, F>, Item>;

impl<S, F, C, Item, Out> CoreObservable<C> for Expand<S, F>
where
  C: Context,
  S: for<'a> ObservableType<Item<'a> = Item> + CoreObservable<C::With<Obs<C, F, Item>>>,
  F: FnMut(Item) -> Out,
  Out: Context<
      Inner: CoreObservable<
        Out::With<Obs<C, F, Item>>,
        Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
      >,
    >,
  C::Inner: Observer<Item, S::Err>,
  Item: Clone,
{
  type Unsub = SourceWithDynamicSubs<S::Unsub, SubsRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let Expand { source, func } = self;
    let state: StateRc<C> = C::RcMut::from(ExpandState {
      observer: Some(context.into_inner()),
      active: 0,
      outer_done: false,
    });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());
    let func: FuncRc<C, F> = C::RcMut::from(func);
    let observer = ExpandObserver {
      state,
      subs: subs.clone(),
      func,
      id: None,
      spawn: spawn_expansion::<
        StateRc<C>,
        SubsRc<C>,
        FuncRc<C, F>,
        C::Inner,
        F,
        Item,
        S::Err,
        Out,
        C::BoxedSubscription,
      >,
    };
    let source_unsub = source.subscribe(C::lift(observer));
    SourceWithDynamicSubs::new(source_unsub, subs)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_expand_recurses_until_empty() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::of(1)
      .expand(|v| if v < 8 { Local::from_iter(vec![v * 2]) } else { Local::from_iter(vec![]) })
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2, 4, 8]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_expand_fans_out_multiple_children() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::of(1)
      .expand(|v| if v < 4 { Local::from_iter(vec![v * 2, v * 2 + 1]) } else { Local::from_iter(vec![]) })
      .subscribe(move |v| result_c.borrow_mut().push(v));

    // Depth-first because inner observables are synchronous
    assert_eq!(*result.borrow(), vec![1, 2, 4, 5, 3, 6, 7]);
  }

  #[rxrust_macro::test]
  fn test_expand_waits_for_async_inner() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();
    let inner = Local::subject::<i32, Infallible>();
    let inner_c = inner.clone();

    Local::of(1)
      .expand(move |_| inner_c.clone())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    assert!(!*completed.borrow());
    inner.complete();
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_expand_unsubscribe_cancels_inner() {
    let inner = Local::subject::<i32, Infallible>();
    let inner_c = inner.clone();

    let sub = Local::of(1).expand(move |_| inner_c.clone()).subscribe(|_| {});
    assert_eq!(inner.inner.subscriber_count(), 1);
    sub.unsubscribe();
    assert_eq!(inner.inner.subscriber_count(), 0);
  }
}
```

- [ ] **Step 2: Trait methods**

```rust
  /// Recursively project every emitted item through `f` and merge the results
  ///
  /// Return an empty observable from `f` to stop the recursion.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::of(1)
  ///   .expand(|v| if v < 4 { Local::of(v * 2) } else { Local::from_iter(vec![]) })
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 1, 2, 4
  /// ```
  fn expand<F, Out>(self, f: F) -> Self::With<Expand<Self::Inner, F>>
  where
    F: for<'a> FnMut(Self::Item<'a>) -> Out,
    Out: Context<Inner: ObservableType>,
  {
    self.transform(|source| Expand { source, func: f })
  }
```

- [ ] **Step 3:** `lib_tests ops::expand`, `doc_tests expand`, gate, commit `feat(ops.expand): ...`.

---

### Task 6: Bookkeeping, matrix, PR

- Integration test in `tests/v1_integration.rs`: `window_count(2)` over `from_iter(vec![1, 2, 3, 4])` collecting each window through `collect::<Vec<_>>()`, then `merge_scan(0, |acc, v| Local::of(acc + v))` over the flattened sums; assert `[3, 10]` then the running totals `[3, 10]`.
- `missing_features.md`: `Window` to `[x]` (`window`, `window_count`, `window_time`); Buffer sub-bullets `buffer_when`, `buffer_toggle`; Delay sub-bullet `delay_when`; Transforming rows `MergeScan`, `Expand`.
- `guide/operators.md` rows; `CHANGELOG.md` line `RxJS Parity, Tier 2b`.
- Full matrix (stable, nightly, clippy, fmt, wasm), push, `gh pr create --base feat/operator-parity-tier2`.

## Self-review

Spec coverage: window family (Task 1), buffer_when/buffer_toggle (2), delay_when (3), merge_scan (4), expand (5), bookkeeping (6). No placeholders. Names used consistently: `Window`, `WindowCount`, `WindowSubjectOf`, `WindowTimer`, `never_errors`, `BufferWhen`, `BufferToggle`, `DelayWhen`, `MergeScan`, `Expand`.
