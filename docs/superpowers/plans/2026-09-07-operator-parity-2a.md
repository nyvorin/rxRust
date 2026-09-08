# Operator Parity PR 2a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `partition`, `sequence_equal`, `single`, `on_error_resume_next`, and the factories `generate`, `iif`, `from_callback`, `using`, with docs and tests, as one PR stacked on tier 1b.

**Architecture:** Same operator shape as tiers 1a/1b (`ObservableType` + `CoreObservable<C>` generic over `Context`, methods on `Observable`, factories on `ObservableFactory`). `partition` is a single filter-with-flag operator applied twice to a cloned source. `sequence_equal` follows the binary `zip` layout with two buffers. `on_error_resume_next` follows `catch_error`'s serial-slot layout. `iif` uses `EitherSubscription`; `using` holds the resource behind `C::RcMut<Option<Res>>` and clears it on termination or unsubscribe.

**Tech Stack:** Rust 2024, stable + nightly, `rxrust_macro::test`. No new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-07-operator-parity-tier1-design.md`, section "Tier 2 / PR 2a".

## Global Constraints

- Branch `feat/operator-parity-tier2`, based on `feat/operator-parity-tier1b`; PR targets that branch until it merges.
- All conventions and lessons from the 1a and 1b plans apply (strict gate, `vec![..]` sources, `'static` lifecycle closures, `on_error` before next-only `subscribe` when `Err != Infallible`, never unsubscribe the currently dispatching source, resubscribing operators need a `Clone` downstream).
- Every new public type is re-exported from the prelude: `SingleError`, `Generate`.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/ops/partition.rs` | `Partition` op (filter with `keep` flag) + tests |
| `src/ops/sequence_equal.rs` | `SequenceEqual` binary op + tests |
| `src/ops/single.rs` | `SingleError`, `Single` op + tests |
| `src/ops/on_error_resume_next.rs` | `OnErrorResumeNext` op + tests |
| `src/observable/generate.rs` | `Generate` iterator; factory uses `FromIter` |
| `src/observable/iif.rs` | `Iif` observable + tests |
| `src/observable/from_callback.rs` | `FromCallback` observable + tests |
| `src/observable/using.rs` | `Using` observable + `UsingSubscription` + tests |
| `src/observable.rs` | trait methods; register the four new observable modules |
| `src/factory.rs` | four factory methods |
| `src/ops.rs`, `src/prelude.rs` | registration and exports |
| bookkeeping | `missing_features.md`, `guide/operators.md`, `CHANGELOG.md`, `tests/v1_integration.rs` |

Check how `src/observable.rs` declares its submodules (`pub mod create;` etc. near the top, or via `src/observable/mod.rs`) and register the new files the same way.

---

### Task 1: `partition`

**Files:** create `src/ops/partition.rs`; modify `src/ops.rs`, `src/observable.rs` (after `filter`).

**Interfaces:** `Observable::partition<F>(self, predicate: F) -> (Self::With<Partition<Self::Inner, F>>, Self::With<Partition<Self::Inner, F>>) where Self::Inner: Clone, F: Clone + for<'a> FnMut(&Self::Item<'a>) -> bool`.

- [x] **Step 1: Operator file**

```rust
//! Partition operator implementation
//!
//! Splits a source into the items that satisfy a predicate and the rest.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// Partition operator: One half of a `partition` pair
///
/// Forwards items whose predicate result equals `keep`. `partition` builds
/// two of these over a cloned source: one with `keep = true`, one with
/// `keep = false`. Each half subscribes to the source on its own, as in
/// RxJS; use `share()` first if the source must be subscribed once.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let (evens, odds) = Local::from_iter(vec![1, 2, 3, 4]).partition(|v| v % 2 == 0);
/// let mut seen_even = Vec::new();
/// let mut seen_odd = Vec::new();
/// evens.subscribe(|v| seen_even.push(v));
/// odds.subscribe(|v| seen_odd.push(v));
/// assert_eq!(seen_even, vec![2, 4]);
/// assert_eq!(seen_odd, vec![1, 3]);
/// ```
#[derive(Clone)]
pub struct Partition<S, F> {
  pub source: S,
  pub predicate: F,
  pub keep: bool,
}

impl<S: ObservableType, F> ObservableType for Partition<S, F> {
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that keeps items matching the wanted predicate result
pub struct PartitionObserver<O, F> {
  observer: O,
  predicate: F,
  keep: bool,
}

impl<O, F, Item, Err> Observer<Item, Err> for PartitionObserver<O, F>
where
  O: Observer<Item, Err>,
  F: FnMut(&Item) -> bool,
{
  fn next(&mut self, value: Item) {
    if (self.predicate)(&value) == self.keep {
      self.observer.next(value);
    }
  }

  fn error(self, e: Err) { self.observer.error(e); }

  fn complete(self) { self.observer.complete(); }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, F, C> CoreObservable<C> for Partition<S, F>
where
  C: Context,
  S: CoreObservable<C::With<PartitionObserver<C::Inner, F>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let Partition { source, predicate, keep } = self;
    let wrapped = context.transform(|observer| PartitionObserver { observer, predicate, keep });
    source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_partition_splits_items() {
    let evens = Rc::new(RefCell::new(Vec::new()));
    let odds = Rc::new(RefCell::new(Vec::new()));
    let evens_c = evens.clone();
    let odds_c = odds.clone();

    let (matching, rest) = Local::from_iter(vec![1, 2, 3, 4, 5]).partition(|v| v % 2 == 0);
    matching.subscribe(move |v| evens_c.borrow_mut().push(v));
    rest.subscribe(move |v| odds_c.borrow_mut().push(v));

    assert_eq!(*evens.borrow(), vec![2, 4]);
    assert_eq!(*odds.borrow(), vec![1, 3, 5]);
  }

  #[rxrust_macro::test]
  fn test_partition_each_half_subscribes_independently() {
    let mut source = Local::subject::<i32, Infallible>();
    let (matching, rest) = source.clone().partition(|v| *v > 0);
    let _a = matching.subscribe(|_| {});
    let _b = rest.subscribe(|_| {});
    assert_eq!(source.inner.subscriber_count(), 2);
    source.next(1);
  }

  #[rxrust_macro::test]
  fn test_partition_error_reaches_both() {
    let errors = Rc::new(RefCell::new(0));
    let e1 = errors.clone();
    let e2 = errors.clone();

    let (matching, rest) = Local::throw_err("boom".to_string())
      .map(|_| 0)
      .partition(|v| *v > 0);
    matching
      .on_error(move |_| *e1.borrow_mut() += 1)
      .subscribe(|_| {});
    rest
      .on_error(move |_| *e2.borrow_mut() += 1)
      .subscribe(|_| {});

    assert_eq!(*errors.borrow(), 2);
  }
}
```

- [x] **Step 2: Trait method** (after `filter`)

```rust
  /// Split the source into the items that satisfy `predicate` and the rest
  ///
  /// Returns `(matching, rest)`. Each half subscribes to the source on its
  /// own; apply `share()` first if the source must be subscribed once.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let (evens, odds) = Local::from_iter(vec![1, 2, 3]).partition(|v| v % 2 == 0);
  /// evens.subscribe(|v| println!("even {}", v));
  /// odds.subscribe(|v| println!("odd {}", v));
  /// ```
  fn partition<F>(
    self, predicate: F,
  ) -> (Self::With<Partition<Self::Inner, F>>, Self::With<Partition<Self::Inner, F>>)
  where
    Self::Inner: Clone,
    F: Clone + for<'a> FnMut(&Self::Item<'a>) -> bool,
  {
    let matching = self.wrap(Partition {
      source: self.inner().clone(),
      predicate: predicate.clone(),
      keep: true,
    });
    let rest = self.transform(|source| Partition { source, predicate, keep: false });
    (matching, rest)
  }
```

- [x] **Step 3:** `lib_tests ops::partition`, `doc_tests partition`, gate, commit `feat(ops.partition): add partition operator`.

---

### Task 2: `sequence_equal`

**Files:** create `src/ops/sequence_equal.rs`; modify `src/ops.rs`, `src/observable.rs` (after `contains`).

**Interfaces:** `Observable::sequence_equal<'a, S2>(self, other: S2) -> Self::With<SequenceEqual<Self::Inner, S2::Inner>>` with `merge`-style bounds plus `Self::Item<'a>: PartialEq`.

- [x] **Step 1: Operator file**

```rust
//! SequenceEqual operator implementation
//!
//! Compares two observables item by item and emits one `bool`.

use std::collections::VecDeque;

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{IntoBoxedSubscription, Subscription, TupleSubscription},
};

/// SequenceEqual operator: Emits whether two sources emit equal sequences
///
/// Items are compared pairwise with `PartialEq`. Emits `false` and completes
/// at the first mismatch, or when one side completes while the other still
/// has unmatched items. Emits `true` when both complete with every item
/// matched; two empty sources are equal.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = None;
/// Local::from_iter(vec![1, 2, 3])
///   .sequence_equal(Local::from_iter(vec![1, 2, 3]))
///   .subscribe(|v| result = Some(v));
/// assert_eq!(result, Some(true));
/// ```
#[doc(alias = "sequenceEqual")]
#[derive(Clone)]
pub struct SequenceEqual<A, B> {
  pub source_a: A,
  pub source_b: B,
}

impl<A, B> ObservableType for SequenceEqual<A, B>
where
  A: ObservableType,
{
  type Item<'a>
    = bool
  where
    Self: 'a;
  type Err = A::Err;
}

/// State shared by both sides
pub struct SequenceEqualState<O, Item> {
  observer: Option<O>,
  buffer_a: VecDeque<Item>,
  buffer_b: VecDeque<Item>,
  done_a: bool,
  done_b: bool,
}

impl<O, Item: PartialEq> SequenceEqualState<O, Item> {
  /// Compare as far as both buffers allow; returns `Some(false)` on a
  /// mismatch, `Some(true)` when both sides are done and drained, else
  /// `None`.
  fn verdict(&mut self) -> Option<bool> {
    while let (Some(a), Some(b)) = (self.buffer_a.front(), self.buffer_b.front()) {
      if a != b {
        return Some(false);
      }
      self.buffer_a.pop_front();
      self.buffer_b.pop_front();
    }
    match (self.done_a, self.done_b) {
      (true, true) => Some(self.buffer_a.is_empty() && self.buffer_b.is_empty()),
      (true, false) if self.buffer_a.is_empty() && !self.buffer_b.is_empty() => Some(false),
      (false, true) if self.buffer_b.is_empty() && !self.buffer_a.is_empty() => Some(false),
      _ => None,
    }
  }
}

/// Which side an observer feeds
#[derive(Clone, Copy)]
pub enum Side {
  /// The receiver of `sequence_equal`
  A,
  /// The argument of `sequence_equal`
  B,
}

/// Observer for one side
pub struct SequenceEqualObserver<StateRc, OtherProxy> {
  state: StateRc,
  other: OtherProxy,
  side: Side,
}

impl<StateRc, OtherProxy, O, Item> SequenceEqualObserver<StateRc, OtherProxy>
where
  StateRc: RcDerefMut<Target = SequenceEqualState<O, Item>>,
  OtherProxy: Subscription + Clone,
  Item: PartialEq,
{
  fn settle<Err>(&self)
  where
    O: Observer<bool, Err>,
  {
    let verdict = self.state.rc_deref_mut().verdict();
    if let Some(equal) = verdict {
      let observer = self.state.rc_deref_mut().observer.take();
      if let Some(mut observer) = observer {
        observer.next(equal);
        observer.complete();
      }
      // Cancel the other side; our own side stops through `is_closed`.
      self.other.clone().unsubscribe();
    }
  }
}

impl<Item, Err, O, StateRc, OtherProxy> Observer<Item, Err>
  for SequenceEqualObserver<StateRc, OtherProxy>
where
  Item: PartialEq,
  O: Observer<bool, Err>,
  StateRc: RcDerefMut<Target = SequenceEqualState<O, Item>>,
  OtherProxy: Subscription + Clone,
{
  fn next(&mut self, value: Item) {
    {
      let mut state = self.state.rc_deref_mut();
      if state.observer.is_none() {
        return;
      }
      match self.side {
        Side::A => state.buffer_a.push_back(value),
        Side::B => state.buffer_b.push_back(value),
      }
    }
    self.settle::<Err>();
  }

  fn error(self, err: Err) {
    let observer = self.state.rc_deref_mut().observer.take();
    if let Some(observer) = observer {
      observer.error(err);
    }
    self.other.unsubscribe();
  }

  fn complete(self) {
    {
      let mut state = self.state.rc_deref_mut();
      if state.observer.is_none() {
        return;
      }
      match self.side {
        Side::A => state.done_a = true,
        Side::B => state.done_b = true,
      }
    }
    self.settle::<Err>();
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

type StateRc<'a, C, A> = <C as Context>::RcMut<
  SequenceEqualState<<C as Context>::Inner, <A as ObservableType>::Item<'a>>,
>;
type BoxedProxy<C> = <C as Context>::RcMut<Option<<C as Context>::BoxedSubscription>>;
type Proxy<C, U> = <C as Context>::RcMut<Option<U>>;

impl<A, B, C, BUnsub> CoreObservable<C> for SequenceEqual<A, B>
where
  C: Context,
  A: ObservableType
    + for<'a> CoreObservable<
      C::With<SequenceEqualObserver<StateRc<'a, C, A>, Proxy<C, BUnsub>>>,
      Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
    >,
  B: for<'a> CoreObservable<
      C::With<SequenceEqualObserver<StateRc<'a, C, A>, BoxedProxy<C>>>,
      Unsub = BUnsub,
    >,
  BoxedProxy<C>: Subscription,
  Proxy<C, BUnsub>: Subscription,
{
  type Unsub = TupleSubscription<BoxedProxy<C>, Proxy<C, BUnsub>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let SequenceEqual { source_a, source_b } = self;
    let state: StateRc<C, A> = C::RcMut::from(SequenceEqualState {
      observer: Some(context.into_inner()),
      buffer_a: VecDeque::new(),
      buffer_b: VecDeque::new(),
      done_a: false,
      done_b: false,
    });
    let a_proxy: BoxedProxy<C> = C::RcMut::from(None);
    let b_proxy: Proxy<C, BUnsub> = C::RcMut::from(None);

    let a_observer =
      SequenceEqualObserver { state: state.clone(), other: b_proxy.clone(), side: Side::A };
    let a_unsub = source_a.subscribe(C::lift(a_observer));
    *a_proxy.rc_deref_mut() = Some(a_unsub.into_boxed());

    if state.rc_deref().observer.is_none() {
      // A alone decided the outcome (for example it errored); B is never subscribed.
      return TupleSubscription::new(a_proxy, b_proxy);
    }

    let b_observer =
      SequenceEqualObserver { state: state.clone(), other: a_proxy.clone(), side: Side::B };
    let b_unsub = source_b.subscribe(C::lift(b_observer));
    *b_proxy.rc_deref_mut() = Some(b_unsub);

    TupleSubscription::new(a_proxy, b_proxy)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  fn run(a: Vec<i32>, b: Vec<i32>) -> Vec<bool> {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    Local::from_iter(a)
      .sequence_equal(Local::from_iter(b))
      .subscribe(move |v| result_c.borrow_mut().push(v));
    let out = result.borrow().clone();
    out
  }

  #[rxrust_macro::test]
  fn test_sequence_equal_true() { assert_eq!(run(vec![1, 2, 3], vec![1, 2, 3]), vec![true]); }

  #[rxrust_macro::test]
  fn test_sequence_equal_mismatch() { assert_eq!(run(vec![1, 2, 3], vec![1, 9, 3]), vec![false]); }

  #[rxrust_macro::test]
  fn test_sequence_equal_length_differs() {
    assert_eq!(run(vec![1, 2], vec![1, 2, 3]), vec![false]);
    assert_eq!(run(vec![1, 2, 3], vec![1, 2]), vec![false]);
  }

  #[rxrust_macro::test]
  fn test_sequence_equal_both_empty() { assert_eq!(run(vec![], vec![]), vec![true]); }

  #[rxrust_macro::test]
  fn test_sequence_equal_interleaved_and_short_circuit() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();

    a.clone()
      .sequence_equal(b.clone())
      .subscribe(move |v| result_c.borrow_mut().push(v));

    a.next(1);
    b.next(1);
    a.next(2);
    assert!(result.borrow().is_empty());
    b.next(3);
    assert_eq!(*result.borrow(), vec![false]);
    // The other side was cancelled at the verdict
    assert_eq!(a.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_sequence_equal_error_propagation() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();
    let a = Local::subject::<i32, String>();
    let b = Local::subject::<i32, String>();

    a.clone()
      .sequence_equal(b.clone())
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});
    b.error("boom".to_string());

    assert_eq!(error.borrow().as_deref(), Some("boom"));
    assert_eq!(a.inner.subscriber_count(), 0);
  }
}
```

If `error(self, err)` unsubscribing `self.other` trips the mid-dispatch rule for the erroring side, note that `other` is always the opposite source, which is not dispatching, so it is safe.

- [x] **Step 2: Trait method** (after `contains`)

```rust
  /// Emit whether this observable and `other` emit equal sequences
  ///
  /// Compares items pairwise; emits `false` and completes at the first
  /// mismatch or length difference, `true` when both complete matched.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let observable = Local::from_iter(vec![1, 2]).sequence_equal(Local::from_iter(vec![1, 2]));
  /// // Emits: true
  /// ```
  #[doc(alias = "sequenceEqual")]
  fn sequence_equal<'a, S2>(self, other: S2) -> Self::With<SequenceEqual<Self::Inner, S2::Inner>>
  where
    Self: 'a,
    Self::Item<'a>: PartialEq,
    S2: Observable<Inner: ObservableType<Item<'a> = Self::Item<'a>, Err = Self::Err>> + 'a,
  {
    self.transform(|source_a| SequenceEqual { source_a, source_b: other.into_inner() })
  }
```

- [x] **Step 3:** `lib_tests ops::sequence_equal`, `doc_tests sequence_equal`, gate, commit `feat(ops.sequence_equal): add sequence_equal operator`.

---

### Task 3: `single`

**Files:** create `src/ops/single.rs`; modify `src/ops.rs`, `src/observable.rs` (after `last_or`), `src/prelude.rs` (export `SingleError`).

- [x] **Step 1: Operator file**

```rust
//! Single operator implementation
//!
//! Emits the only item of a source, or errors if there is none or more than
//! one.

use std::fmt;

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// Why `single` could not produce exactly one item.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SingleError {
  /// The source completed without emitting
  Empty,
  /// The source emitted a second item
  TooMany,
}

impl fmt::Display for SingleError {
  fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
    match self {
      SingleError::Empty => f.write_str("expected exactly one item, got none"),
      SingleError::TooMany => f.write_str("expected exactly one item, got more"),
    }
  }
}

impl std::error::Error for SingleError {}

/// Single operator: Emit the only item, or error
///
/// Emits the item on completion. Errors with [`SingleError::Empty`] if the
/// source completes without items and with [`SingleError::TooMany`] as soon
/// as a second item arrives, releasing the source.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = None;
/// Local::from_iter(vec![42])
///   .map_err(|_: std::convert::Infallible| SingleError::Empty)
///   .single()
///   .on_error(|_| {})
///   .subscribe(|v| result = Some(v));
/// assert_eq!(result, Some(42));
/// ```
#[derive(Clone)]
pub struct Single<S> {
  pub source: S,
}

impl<S: ObservableType> ObservableType for Single<S> {
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that holds the first item and rejects a second
pub struct SingleObserver<O, Item> {
  observer: Option<O>,
  first: Option<Item>,
}

impl<O, Item, Err> Observer<Item, Err> for SingleObserver<O, Item>
where
  O: Observer<Item, Err>,
  Err: From<SingleError>,
{
  fn next(&mut self, value: Item) {
    if self.observer.is_none() {
      return;
    }
    if self.first.is_some() {
      self.first = None;
      if let Some(observer) = self.observer.take() {
        observer.error(SingleError::TooMany.into());
      }
    } else {
      self.first = Some(value);
    }
  }

  fn error(self, e: Err) {
    if let Some(observer) = self.observer {
      observer.error(e);
    }
  }

  fn complete(self) {
    let SingleObserver { observer, first } = self;
    let Some(mut observer) = observer else { return };
    match first {
      Some(value) => {
        observer.next(value);
        observer.complete();
      }
      None => observer.error(SingleError::Empty.into()),
    }
  }

  fn is_closed(&self) -> bool {
    self
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

impl<S, C> CoreObservable<C> for Single<S>
where
  C: Context,
  S: for<'a> CoreObservable<C::With<SingleObserver<C::Inner, S::Item<'a>>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let wrapped =
      context.transform(|observer| SingleObserver { observer: Some(observer), first: None });
    self.source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use super::SingleError;
  use crate::prelude::*;

  fn run(items: Vec<i32>) -> (Vec<i32>, Option<SingleError>, bool) {
    let result = Rc::new(RefCell::new(Vec::new()));
    let error = Rc::new(RefCell::new(None));
    let completed = Rc::new(RefCell::new(false));
    let (r, e, c) = (result.clone(), error.clone(), completed.clone());
    Local::from_iter(items)
      .map_err(|_: Infallible| SingleError::Empty)
      .single()
      .on_error(move |err| *e.borrow_mut() = Some(err))
      .on_complete(move || *c.borrow_mut() = true)
      .subscribe(move |v| r.borrow_mut().push(v));
    let out = (result.borrow().clone(), *error.borrow(), *completed.borrow());
    out
  }

  #[rxrust_macro::test]
  fn test_single_one_item() { assert_eq!(run(vec![7]), (vec![7], None, true)); }

  #[rxrust_macro::test]
  fn test_single_empty_errors() { assert_eq!(run(vec![]), (vec![], Some(SingleError::Empty), false)); }

  #[rxrust_macro::test]
  fn test_single_too_many_errors_early() {
    let seen = Rc::new(RefCell::new(0));
    let seen_c = seen.clone();
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    Local::from_iter(vec![1, 2, 3, 4])
      .map_err(|_: Infallible| SingleError::Empty)
      .tap(move |_| *seen_c.borrow_mut() += 1)
      .single()
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), Some(SingleError::TooMany));
    // The source was released after the second item
    assert_eq!(*seen.borrow(), 2);
  }

  #[rxrust_macro::test]
  fn test_single_source_error_propagates() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    Local::throw_err(SingleError::TooMany)
      .map(|_| 0)
      .single()
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), Some(SingleError::TooMany));
  }
}
```

- [x] **Step 2: Trait method** (after `last_or`) and prelude export

```rust
  /// Emit the only item on completion, or error
  ///
  /// Errors with `SingleError::Empty` on an empty source and with
  /// `SingleError::TooMany` when a second item arrives. Requires
  /// `Err: From<SingleError>`.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter(vec![1])
  ///   .map_err(|_: std::convert::Infallible| SingleError::Empty)
  ///   .single()
  ///   .on_error(|e| println!("{}", e))
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 1
  /// ```
  fn single(self) -> Self::With<Single<Self::Inner>>
  where
    Self::Err: From<SingleError>,
  {
    self.transform(|source| Single { source })
  }
```

Add `single::SingleError` to the prelude's `ops` re-export list.

- [x] **Step 3:** `lib_tests ops::single`, `doc_tests single`, gate, commit `feat(ops.single): add single operator`.

---

### Task 4: `on_error_resume_next`

**Files:** create `src/ops/on_error_resume_next.rs`; modify `src/ops.rs`, `src/observable.rs` (after `catch_error`).

- [x] **Step 1: Operator file**

```rust
//! OnErrorResumeNext operator implementation
//!
//! Continues with another observable when the source errors or completes.

use crate::{
  context::{Context, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{IntoBoxedSubscription, Subscription},
};

/// OnErrorResumeNext operator: Continue with `next` after the source ends
///
/// When the source errors (the error is discarded) or completes, `next` is
/// subscribed and mirrored. The output error type is `next`'s.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::throw_err("boom".to_string())
///   .map(|_| 0)
///   .on_error_resume_next(Local::from_iter(vec![1, 2]))
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 2]);
/// ```
#[doc(alias = "onErrorResumeNext")]
#[derive(Clone)]
pub struct OnErrorResumeNext<S, N> {
  pub source: S,
  pub next: N,
}

impl<S, N> ObservableType for OnErrorResumeNext<S, N>
where
  S: ObservableType,
  N: ObservableType,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = N::Err;
}

/// Observer that switches to `next` on any terminal event
pub struct OnErrorResumeNextObserver<Ctx: Context, N> {
  observer: Option<Ctx>,
  next: Option<N>,
  serial: Ctx::RcMut<Option<Ctx::BoxedSubscription>>,
}

impl<Ctx: Context, N> OnErrorResumeNextObserver<Ctx, N> {
  fn resume(mut self)
  where
    N: CoreObservable<Ctx, Unsub: IntoBoxedSubscription<Ctx::BoxedSubscription>>,
  {
    if let (Some(observer), Some(next)) = (self.observer.take(), self.next.take()) {
      let unsub = next.subscribe(observer);
      *self.serial.rc_deref_mut() = Some(unsub.into_boxed());
    }
  }
}

impl<Ctx, N, Item, SrcErr, OutErr> Observer<Item, SrcErr> for OnErrorResumeNextObserver<Ctx, N>
where
  Ctx: Context + Observer<Item, OutErr>,
  N: ObservableType<Err = OutErr>
    + CoreObservable<Ctx, Unsub: IntoBoxedSubscription<Ctx::BoxedSubscription>>,
{
  fn next(&mut self, value: Item) {
    if let Some(observer) = self.observer.as_mut() {
      observer.next(value);
    }
  }

  fn error(self, _err: SrcErr) { self.resume(); }

  fn complete(self) { self.resume(); }

  fn is_closed(&self) -> bool {
    self
      .observer
      .as_ref()
      .is_none_or(|o| o.is_closed())
  }
}

impl<S, N, C> CoreObservable<C> for OnErrorResumeNext<S, N>
where
  C: Context,
  S: CoreObservable<C::With<OnErrorResumeNextObserver<C, N>>>,
  S::Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
  N: ObservableType,
  C::RcMut<Option<C::BoxedSubscription>>: Subscription,
{
  type Unsub = C::RcMut<Option<C::BoxedSubscription>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let OnErrorResumeNext { source, next } = self;
    let serial: C::RcMut<Option<C::BoxedSubscription>> = C::RcMut::from(None);
    let observer = OnErrorResumeNextObserver {
      observer: Some(context),
      next: Some(next),
      serial: serial.clone(),
    };
    let source_unsub = source.subscribe(C::lift(observer));
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
  fn test_resume_after_error() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    let mut source = Local::subject::<i32, String>();
    source
      .clone()
      .on_error_resume_next(Local::from_iter(vec![10, 11]))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    source.next(1);
    source.error("boom".to_string());

    assert_eq!(*result.borrow(), vec![1, 10, 11]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_resume_after_completion() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter(vec![1])
      .on_error_resume_next(Local::from_iter(vec![2]))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
  }

  #[rxrust_macro::test]
  fn test_next_error_propagates() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    Local::from_iter(vec![1])
      .on_error_resume_next(Local::throw_err("later".to_string()).map(|_| 0))
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(error.borrow().as_deref(), Some("later"));
  }

  #[rxrust_macro::test]
  fn test_unsubscribe_cancels_next() {
    let source = Local::subject::<i32, String>();
    let next = Local::subject::<i32, Infallible>();

    let sub = source
      .clone()
      .on_error_resume_next(next.clone())
      .subscribe(|_| {});
    source.clone().complete();
    assert_eq!(next.inner.subscriber_count(), 1);
    sub.unsubscribe();
    assert_eq!(next.inner.subscriber_count(), 0);
  }
}
```

- [x] **Step 2: Trait method** (after `catch_error`)

```rust
  /// Continue with `next` when the source errors or completes
  ///
  /// A source error is discarded. The output error type is `next`'s.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::throw_err("boom".to_string())
  ///   .map(|_| 0)
  ///   .on_error_resume_next(Local::of(1))
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 1
  /// ```
  #[doc(alias = "onErrorResumeNext")]
  fn on_error_resume_next<'a, N>(self, next: N) -> Self::With<OnErrorResumeNext<Self::Inner, N::Inner>>
  where
    Self: 'a,
    N: Observable<Inner: ObservableType<Item<'a> = Self::Item<'a>>> + 'a,
  {
    self.transform(|source| OnErrorResumeNext { source, next: next.into_inner() })
  }
```

- [x] **Step 3:** `lib_tests ops::on_error_resume_next`, `doc_tests on_error_resume_next`, gate, commit `feat(ops.on_error_resume_next): add on_error_resume_next operator`.

---

### Task 5: `generate` factory

**Files:** create `src/observable/generate.rs`; register in `src/observable.rs`; modify `src/factory.rs` (after `from_fn`), `src/prelude.rs`.

- [x] **Step 1: Iterator file**

```rust
//! Generate: a lazy state-machine iterator for `ObservableFactory::generate`.

/// Iterator yielding `initial`, then `iterate(&state)` while
/// `condition(&state)` holds.
#[derive(Clone)]
pub struct Generate<T, C, I> {
  state: Option<T>,
  condition: C,
  iterate: I,
}

impl<T, C, I> Generate<T, C, I> {
  /// Creates the generator.
  pub fn new(initial: T, condition: C, iterate: I) -> Self {
    Self { state: Some(initial), condition, iterate }
  }
}

impl<T, C, I> Iterator for Generate<T, C, I>
where
  C: FnMut(&T) -> bool,
  I: FnMut(&T) -> T,
{
  type Item = T;

  fn next(&mut self) -> Option<T> {
    let current = self.state.take()?;
    if !(self.condition)(&current) {
      return None;
    }
    self.state = Some((self.iterate)(&current));
    Some(current)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_generate_counts() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::generate(1, |v| *v <= 4, |v| v + 1).subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2, 3, 4]);
  }

  #[rxrust_macro::test]
  fn test_generate_empty_when_condition_false() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::generate(0, |_| false, |v| v + 1)
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert!(result.borrow().is_empty());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_generate_is_lazy_with_take() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::generate(1u64, |_| true, |v| v * 2)
      .take(5)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2, 4, 8, 16]);
  }
}
```

- [x] **Step 2: Factory** (after `from_fn`)

```rust
  /// Emit `initial`, then `iterate(&state)` while `condition(&state)` holds.
  ///
  /// Lazy: pairs well with `take` for unbounded generators.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::generate(1, |v| *v <= 3, |v| v + 1).subscribe(|v| println!("{}", v));
  /// // Prints: 1, 2, 3
  /// ```
  fn generate<T, Cond, Iter>(
    initial: T, condition: Cond, iterate: Iter,
  ) -> Self::With<FromIter<Generate<T, Cond, Iter>>>
  where
    Cond: FnMut(&T) -> bool,
    Iter: FnMut(&T) -> T,
  {
    Self::from_iter(Generate::new(initial, condition, iterate))
  }
```

Import `Generate` in `src/factory.rs` and export it from the prelude's `observable::{..}` list.

- [x] **Step 3:** `lib_tests observable::generate`, `doc_tests generate`, gate, commit `feat(factory.generate): add generate factory`.

---

### Task 6: `iif` factory

**Files:** create `src/observable/iif.rs`; register; modify `src/factory.rs` (after `defer`), `src/prelude.rs`.

- [x] **Step 1: Observable file**

```rust
//! Iif: choose one of two observables at subscribe time.

use crate::{
  observable::{CoreObservable, ObservableType},
  subscription::EitherSubscription,
};

/// Subscribes `then_source` when `condition()` is true, else `else_source`.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::iif(|| 2 > 1, Local::of("yes"), Local::of("no")).subscribe(|v| result.push(v));
/// assert_eq!(result, vec!["yes"]);
/// ```
#[derive(Clone)]
pub struct Iif<F, A, B> {
  pub condition: F,
  pub then_source: A,
  pub else_source: B,
}

impl<F, A, B> ObservableType for Iif<F, A, B>
where
  A: ObservableType,
{
  type Item<'a>
    = A::Item<'a>
  where
    Self: 'a;
  type Err = A::Err;
}

impl<F, A, B, C> CoreObservable<C> for Iif<F, A, B>
where
  F: FnOnce() -> bool,
  A: CoreObservable<C>,
  B: CoreObservable<C>,
{
  type Unsub = EitherSubscription<A::Unsub, B::Unsub>;

  fn subscribe(self, context: C) -> Self::Unsub {
    if (self.condition)() {
      EitherSubscription::Left(self.then_source.subscribe(context))
    } else {
      EitherSubscription::Right(self.else_source.subscribe(context))
    }
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  fn run(flag: bool) -> Vec<i32> {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    Local::iif(move || flag, Local::from_iter(vec![1, 2]), Local::from_iter(vec![9]))
      .subscribe(move |v| result_c.borrow_mut().push(v));
    let out = result.borrow().clone();
    out
  }

  #[rxrust_macro::test]
  fn test_iif_then() { assert_eq!(run(true), vec![1, 2]); }

  #[rxrust_macro::test]
  fn test_iif_else() { assert_eq!(run(false), vec![9]); }

  #[rxrust_macro::test]
  fn test_iif_evaluates_at_subscribe() {
    let calls = Rc::new(RefCell::new(0));
    let calls_c = calls.clone();
    let observable = Local::iif(
      move || {
        *calls_c.borrow_mut() += 1;
        true
      },
      Local::of(1),
      Local::of(2),
    );
    assert_eq!(*calls.borrow(), 0);
    observable.subscribe(|_| {});
    assert_eq!(*calls.borrow(), 1);
  }
}
```

- [x] **Step 2: Factory** (after `defer`)

```rust
  /// Subscribe to `then_source` when `condition()` is true at subscribe
  /// time, otherwise to `else_source`.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::iif(|| true, Local::of(1), Local::of(2)).subscribe(|v| println!("{}", v));
  /// // Prints: 1
  /// ```
  fn iif<F, A, B>(
    condition: F, then_source: Self::With<A>, else_source: Self::With<B>,
  ) -> Self::With<Iif<F, A, B>>
  where
    F: FnOnce() -> bool,
    A: ObservableType,
    B: for<'a> ObservableType<Item<'a> = A::Item<'a>, Err = A::Err>,
  {
    Self::lift(Iif {
      condition,
      then_source: then_source.into_inner(),
      else_source: else_source.into_inner(),
    })
  }
```

If the `for<'a>` equality on `B` rejects ordinary sources, drop that bound; the `CoreObservable` impl already forces both branches to accept the same observer.

- [x] **Step 3:** `lib_tests observable::iif`, `doc_tests iif`, gate, commit `feat(factory.iif): add iif factory`.

---

### Task 7: `from_callback` factory

**Files:** create `src/observable/from_callback.rs`; register; modify `src/factory.rs` (after `from_fn`), `src/prelude.rs`.

- [x] **Step 1: Observable file**

```rust
//! FromCallback: emit the values handed to a callback, then complete.

use std::{convert::Infallible, marker::PhantomData};

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// Runs `f` with an emit callback at subscribe; each value passed to the
/// callback is emitted, and the observable completes when `f` returns.
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_callback(|emit: &mut dyn FnMut(i32)| {
///   emit(1);
///   emit(2);
/// })
/// .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 2]);
/// ```
#[doc(alias = "bindCallback")]
pub struct FromCallback<F, Item> {
  pub f: F,
  _marker: PhantomData<fn() -> Item>,
}

impl<F: Clone, Item> Clone for FromCallback<F, Item> {
  fn clone(&self) -> Self { Self { f: self.f.clone(), _marker: PhantomData } }
}

impl<F, Item> FromCallback<F, Item> {
  /// Wraps the callback-taking function.
  pub fn new(f: F) -> Self { Self { f, _marker: PhantomData } }
}

impl<F, Item> ObservableType for FromCallback<F, Item> {
  type Item<'a>
    = Item
  where
    Self: 'a;
  type Err = Infallible;
}

impl<F, Item, C> CoreObservable<C> for FromCallback<F, Item>
where
  C: Context,
  C::Inner: Observer<Item, Infallible>,
  F: FnOnce(&mut dyn FnMut(Item)),
{
  type Unsub = ();

  fn subscribe(self, context: C) -> Self::Unsub {
    let mut observer = context.into_inner();
    (self.f)(&mut |value| {
      if !observer.is_closed() {
        observer.next(value);
      }
    });
    observer.complete();
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_from_callback_emits_and_completes() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::from_callback(|emit: &mut dyn FnMut(i32)| {
      emit(1);
      emit(2);
    })
    .on_complete(move || *completed_c.borrow_mut() = true)
    .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_from_callback_respects_take() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_callback(|emit: &mut dyn FnMut(i32)| {
      for i in 0..10 {
        emit(i);
      }
    })
    .take(2)
    .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![0, 1]);
  }
}
```

- [x] **Step 2: Factory** (after `from_fn`)

```rust
  /// Emit every value handed to the callback, then complete when `f` returns.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_callback(|emit: &mut dyn FnMut(i32)| emit(42)).subscribe(|v| println!("{}", v));
  /// // Prints: 42
  /// ```
  #[doc(alias = "bindCallback")]
  fn from_callback<F, Item>(f: F) -> Self::With<FromCallback<F, Item>>
  where
    F: FnOnce(&mut dyn FnMut(Item)),
  {
    Self::lift(FromCallback::new(f))
  }
```

- [x] **Step 3:** `lib_tests observable::from_callback`, `doc_tests from_callback`, gate, commit `feat(factory.from_callback): add from_callback factory`.

---

### Task 8: `using` factory

**Files:** create `src/observable/using.rs`; register; modify `src/factory.rs` (after `defer`), `src/prelude.rs`.

- [x] **Step 1: Observable file**

```rust
//! Using: tie a resource's lifetime to a subscription.

use std::marker::PhantomData;

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::Subscription,
};

/// Creates a resource per subscription, builds the observable from it, and
/// drops the resource when the subscription terminates or is unsubscribed.
///
/// # Examples
///
/// ```rust
/// use std::{cell::RefCell, rc::Rc};
///
/// use rxrust::prelude::*;
///
/// let dropped = Rc::new(RefCell::new(false));
/// struct Guard(Rc<RefCell<bool>>);
/// impl Drop for Guard {
///   fn drop(&mut self) { *self.0.borrow_mut() = true; }
/// }
///
/// let flag = dropped.clone();
/// Local::using(move || Guard(flag.clone()), |_guard| Local::from_iter(vec![1, 2]))
///   .subscribe(|v| println!("{}", v));
/// assert!(*dropped.borrow());
/// ```
pub struct Using<RF, OF, Res, OutCtx> {
  pub resource_factory: RF,
  pub observable_factory: OF,
  _marker: PhantomData<fn() -> (Res, OutCtx)>,
}

impl<RF, OF, Res, OutCtx> Using<RF, OF, Res, OutCtx> {
  /// Pairs a resource factory with an observable factory.
  pub fn new(resource_factory: RF, observable_factory: OF) -> Self {
    Self { resource_factory, observable_factory, _marker: PhantomData }
  }
}

impl<RF, OF, Res, OutCtx> ObservableType for Using<RF, OF, Res, OutCtx>
where
  OutCtx: Context<Inner: ObservableType>,
{
  type Item<'a>
    = <OutCtx::Inner as ObservableType>::Item<'a>
  where
    Self: 'a;
  type Err = <OutCtx::Inner as ObservableType>::Err;
}

/// Observer that releases the resource on a terminal event
pub struct UsingObserver<O, H> {
  observer: O,
  holder: H,
}

impl<O, H, Res, Item, Err> Observer<Item, Err> for UsingObserver<O, H>
where
  O: Observer<Item, Err>,
  H: RcDerefMut<Target = Option<Res>>,
{
  fn next(&mut self, value: Item) { self.observer.next(value); }

  fn error(self, err: Err) {
    self.holder.rc_deref_mut().take();
    self.observer.error(err);
  }

  fn complete(self) {
    self.holder.rc_deref_mut().take();
    self.observer.complete();
  }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

/// Subscription that releases the resource when unsubscribed
pub struct UsingSubscription<U, H> {
  inner: U,
  holder: H,
}

impl<U, H, Res> Subscription for UsingSubscription<U, H>
where
  U: Subscription,
  H: RcDerefMut<Target = Option<Res>>,
{
  fn unsubscribe(self) {
    self.inner.unsubscribe();
    self.holder.rc_deref_mut().take();
  }

  fn is_closed(&self) -> bool { self.inner.is_closed() || self.holder.rc_deref().is_none() }
}

impl<RF, OF, Res, OutCtx, C> CoreObservable<C> for Using<RF, OF, Res, OutCtx>
where
  C: Context,
  RF: FnOnce() -> Res,
  OF: FnOnce(&Res) -> OutCtx,
  OutCtx: Context<Inner: CoreObservable<C::With<UsingObserver<C::Inner, C::RcMut<Option<Res>>>>>>,
  C::RcMut<Option<Res>>: RcDerefMut<Target = Option<Res>>,
{
  type Unsub = UsingSubscription<
    <OutCtx::Inner as CoreObservable<C::With<UsingObserver<C::Inner, C::RcMut<Option<Res>>>>>>::Unsub,
    C::RcMut<Option<Res>>,
  >;

  fn subscribe(self, context: C) -> Self::Unsub {
    let resource = (self.resource_factory)();
    let source = (self.observable_factory)(&resource).into_inner();
    let holder: C::RcMut<Option<Res>> = C::RcMut::from(Some(resource));
    let holder_for_observer = holder.clone();
    let wrapped =
      context.transform(|observer| UsingObserver { observer, holder: holder_for_observer });
    let inner = source.subscribe(wrapped);
    UsingSubscription { inner, holder }
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  struct Guard(Rc<RefCell<bool>>);
  impl Drop for Guard {
    fn drop(&mut self) { *self.0.borrow_mut() = true; }
  }

  #[rxrust_macro::test]
  fn test_using_drops_resource_on_complete() {
    let dropped = Rc::new(RefCell::new(false));
    let result = Rc::new(RefCell::new(Vec::new()));
    let flag = dropped.clone();
    let result_c = result.clone();

    Local::using(move || Guard(flag.clone()), |_g| Local::from_iter(vec![1, 2]))
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
    assert!(*dropped.borrow());
  }

  #[rxrust_macro::test]
  fn test_using_drops_resource_on_unsubscribe() {
    let dropped = Rc::new(RefCell::new(false));
    let flag = dropped.clone();
    let source = Local::subject::<i32, Infallible>();
    let source_c = source.clone();

    let sub = Local::using(move || Guard(flag.clone()), move |_g| source_c.clone()).subscribe(|_| {});
    assert!(!*dropped.borrow());
    assert_eq!(source.inner.subscriber_count(), 1);

    sub.unsubscribe();
    assert!(*dropped.borrow());
    assert_eq!(source.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_using_resource_available_to_factory() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::using(|| 21, |base| Local::of(*base * 2)).subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![42]);
  }
}
```

- [x] **Step 2: Factory** (after `defer`)

```rust
  /// Create a resource per subscription and an observable from it; the
  /// resource is dropped when the subscription terminates or is
  /// unsubscribed.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::using(|| String::from("res"), |r| Local::of(r.len())).subscribe(|v| println!("{}", v));
  /// // Prints: 3
  /// ```
  fn using<RF, OF, Res, Out>(
    resource_factory: RF, observable_factory: OF,
  ) -> Self::With<Using<RF, OF, Res, Self::With<Out>>>
  where
    RF: FnOnce() -> Res,
    OF: FnOnce(&Res) -> Self::With<Out>,
    Out: ObservableType,
  {
    Self::lift(Using::new(resource_factory, observable_factory))
  }
```

- [x] **Step 3:** `lib_tests observable::using`, `doc_tests using`, gate, commit `feat(factory.using): add using factory`.

---

### Task 9: Bookkeeping, matrix, PR

- Integration tests in `tests/v1_integration.rs`:

```rust
#[rxrust_macro::test]
fn test_partition_then_sequence_equal() {
  let result = Rc::new(RefCell::new(Vec::new()));
  let result_c = result.clone();

  let (evens, _odds) = Local::from_iter(vec![1, 2, 3, 4]).partition(|v| v % 2 == 0);
  evens
    .sequence_equal(Local::generate(2, |v| *v <= 4, |v| v + 2))
    .subscribe(move |v| result_c.borrow_mut().push(v));

  assert_eq!(*result.borrow(), vec![true]);
}
```

- `missing_features.md`: `From` row `from_callback` to `[x]`; `Using` to `[x]`; `SequenceEqual` to `[x]`; add `Single`, `Iif`, `Generate`, `Partition`, `OnErrorResumeNext` rows as `[x]` under their categories.
- `guide/operators.md`: rows for `partition`, `sequence_equal`, `single`, `on_error_resume_next`; creation rows for `generate`, `iif`, `from_callback`, `using`.
- `CHANGELOG.md`: `*   **RxJS Parity, Tier 2a**: ...` under New Features.
- Full matrix, then push and `gh pr create --base feat/operator-parity-tier1b`.

## Self-review

Spec coverage: every PR 2a bullet has a task (1 partition, 2 sequence_equal, 5 generate, 6 iif, 7 from_callback, 8 using, 3 single, 4 on_error_resume_next). No placeholders. Names used consistently: `Partition`, `SequenceEqual`, `Single`/`SingleError`, `OnErrorResumeNext`, `Generate`, `Iif`, `FromCallback`, `Using`/`UsingSubscription`.
