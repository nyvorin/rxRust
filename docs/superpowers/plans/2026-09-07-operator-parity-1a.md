# Operator Parity PR 1a Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add the utility and filtering operators from the tier-1 spec (`every`, `ignore_elements`, `is_empty`, `element_at`, `find`, `find_index`, `end_with`, `throw_if_empty`, `materialize`, `dematerialize`, `timestamp`, `time_interval`, `race`, `race_observables`, `fork_join_observables`, `combine_latest_observables`, `zip_observables`) with docs and tests, as one PR.

**Architecture:** Every operator is a struct in `src/ops/<name>.rs` implementing `ObservableType` and `CoreObservable<C>` generically over the `Context`, wrapping the downstream observer via `context.transform(..)`. Instance operators become methods on the `Observable` trait in `src/observable.rs`; N-ary creators become methods on `ObservableFactory` in `src/factory.rs`. State shared between observers lives behind `C::RcMut<T>` so the same code runs on `Local` (Rc/RefCell) and `Shared` (Arc/Mutex).

**Tech Stack:** Rust 2024 edition, stable toolchain for default features and nightly for `--all-features`; `rxrust_macro::test` for tests (sync, or `local`/`shared` async flavors); no new dependencies.

**Spec:** `docs/superpowers/specs/2026-09-07-operator-parity-tier1-design.md`

## Global Constraints

- One file per operator in `src/ops/`, flat; register with `pub mod` and `pub use` in `src/ops.rs` (keep both lists alphabetical).
- Rustdoc on every public operator with `#[doc(alias = "<rxjsName>")]` where the RxJS name differs, and a doctest.
- Tests in the operator file; cover basic behavior, empty source, error propagation, completion, unsubscribe/cleanup; a `Shared` case where the operator holds state.
- Gate before every commit: `cargo +nightly fmt --all` then `cargo +nightly clippy --all-targets --all-features -- -D warnings`.
- Commit style: `feat(ops.<name>): add <name> operator` with the session trailers.
- Branch: `feat/operator-parity-tier1` (already created, spec committed).
- Do not modify `map`, `nightly` feature code, or the `Subject` internals in this PR.

## File Structure

| File | Responsibility |
| --- | --- |
| `src/ops/every.rs` | `Every` op + observer + tests |
| `src/ops/ignore_elements.rs` | `IgnoreElements` op + tests |
| `src/ops/is_empty.rs` | `IsEmpty` op + tests |
| `src/ops/element_at.rs` | `ElementAt` / `ElementAtOr` type aliases over `Skip`/`Take`/`DefaultIfEmpty` + tests |
| `src/ops/find.rs` | `Find` alias over `Filter`/`Take`, `FindIndex` op + tests |
| `src/ops/end_with.rs` | `EndWith` op + tests |
| `src/ops/throw_if_empty.rs` | `ThrowIfEmpty` op + tests |
| `src/ops/materialize.rs` | `Notification` enum, `Materialize`, `Dematerialize` + tests |
| `src/ops/timestamp.rs` | `Timestamped<T>` value, `Timestamp` op + tests |
| `src/ops/time_interval.rs` | `Elapsed<T>` value, `TimeInterval` op + tests |
| `src/ops/race.rs` | binary `Race` op + tests |
| `src/ops/race_all.rs` | N-ary `RaceAll` op + tests (factory: `race_observables`) |
| `src/ops/fork_join.rs` | N-ary `ForkJoin` op + tests (factory: `fork_join_observables`) |
| `src/ops/combine_latest_all.rs` | N-ary `CombineLatestAll` op + tests (factory: `combine_latest_observables`) |
| `src/ops/zip_all.rs` | N-ary `ZipAll` op + tests (factory: `zip_observables`) |
| `src/observable.rs` | one trait method per instance operator |
| `src/factory.rs` | four N-ary factory methods |
| `src/prelude.rs` | re-export `Notification`, `Timestamped`, `Elapsed` |
| `missing_features.md`, `guide/operators.md`, `CHANGELOG.md` | bookkeeping |
| `tests/v1_integration.rs` | two end-to-end chains |

Shared shape used by Tasks 12 through 14 (N-ary with owned items): the op holds `Vec<O>`; `subscribe` builds one shared state behind `C::RcMut`, a `C::RcMut<DynamicSubscriptions<C::BoxedSubscription>>` for the inner subscriptions, and subscribes each source in order with an indexed observer, boxing each inner subscription with `IntoBoxedSubscription` so the subscription type cannot depend on itself. The returned subscription is `SourceWithDynamicSubs<(), C::RcMut<DynamicSubscriptions<C::BoxedSubscription>>>`.

---

### Task 1: `every`

**Files:**
- Create: `src/ops/every.rs`
- Modify: `src/ops.rs` (add `pub mod every;` and `pub use every::*;`)
- Modify: `src/observable.rs` (add method after `contains`, around line 692)

**Interfaces:**
- Produces: `Observable::every<F>(self, predicate: F) -> Self::With<Every<Self::Inner, F>> where F: for<'a> FnMut(&Self::Item<'a>) -> bool`, emitting one `bool`.

- [x] **Step 1: Write the operator file with failing tests**

```rust
//! Every operator implementation
//!
//! Emits `true` if every item satisfies a predicate, or `false` as soon as
//! one item does not.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// Every operator: Checks whether every item satisfies a predicate
///
/// Emits `false` and completes as soon as one item fails the predicate,
/// unsubscribing from the source. Emits `true` when the source completes and
/// every item passed. An empty source emits `true`.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = None;
/// Local::from_iter([2, 4, 6])
///   .every(|v| v % 2 == 0)
///   .subscribe(|v| result = Some(v));
/// assert_eq!(result, Some(true));
///
/// let mut result = None;
/// Local::from_iter([2, 3, 6])
///   .every(|v| v % 2 == 0)
///   .subscribe(|v| result = Some(v));
/// assert_eq!(result, Some(false));
/// ```
#[doc(alias = "all")]
#[derive(Clone)]
pub struct Every<S, F> {
  pub source: S,
  pub predicate: F,
}

impl<S, F> ObservableType for Every<S, F>
where
  S: ObservableType,
{
  type Item<'a>
    = bool
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that evaluates the predicate and short-circuits on the first
/// failure
pub struct EveryObserver<O, F> {
  observer: Option<O>,
  predicate: F,
}

impl<O, F, Item, Err> Observer<Item, Err> for EveryObserver<O, F>
where
  O: Observer<bool, Err>,
  F: FnMut(&Item) -> bool,
{
  fn next(&mut self, v: Item) {
    if self.observer.is_none() {
      return;
    }
    if !(self.predicate)(&v)
      && let Some(mut observer) = self.observer.take()
    {
      observer.next(false);
      observer.complete();
    }
  }

  fn error(self, e: Err) {
    if let Some(observer) = self.observer {
      observer.error(e);
    }
  }

  fn complete(self) {
    if let Some(mut observer) = self.observer {
      observer.next(true);
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

impl<S, F, C> CoreObservable<C> for Every<S, F>
where
  C: Context,
  S: CoreObservable<C::With<EveryObserver<C::Inner, F>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let Every { source, predicate } = self;
    let wrapped =
      context.transform(|observer| EveryObserver { observer: Some(observer), predicate });
    source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    cell::RefCell,
    rc::Rc,
    sync::{Arc, Mutex},
  };

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_every_all_pass() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_clone = result.clone();

    Local::from_iter([2, 4, 6])
      .every(|v| v % 2 == 0)
      .subscribe(move |v| result_clone.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![true]);
  }

  #[rxrust_macro::test]
  fn test_every_short_circuits_on_first_failure() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::new(RefCell::new(0));
    let result_clone = result.clone();
    let seen_clone = seen.clone();

    Local::from_iter([2, 3, 4, 5])
      .tap(move |_| *seen_clone.borrow_mut() += 1)
      .every(|v| v % 2 == 0)
      .subscribe(move |v| result_clone.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![false]);
    // 2 passes, 3 fails; 4 and 5 are never pulled from the source
    assert_eq!(*seen.borrow(), 2);
  }

  #[rxrust_macro::test]
  fn test_every_empty_is_true() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_clone = result.clone();
    let completed_clone = completed.clone();

    Local::from_iter(std::iter::empty::<i32>())
      .every(|_| false)
      .on_complete(move || *completed_clone.borrow_mut() = true)
      .subscribe(move |v| result_clone.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![true]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_every_error_propagation() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let error = Rc::new(RefCell::new(String::new()));
    let result_clone = result.clone();
    let error_clone = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .every(|v| *v == 0)
      .on_error(move |e| *error_clone.borrow_mut() = e)
      .subscribe(move |v| result_clone.borrow_mut().push(v));

    assert_eq!(*result.borrow(), Vec::<bool>::new());
    assert_eq!(*error.borrow(), "boom");
  }

  #[rxrust_macro::test]
  fn test_every_shared() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result_clone = result.clone();

    Shared::from_iter([1, 2, 3])
      .every(|v| *v > 0)
      .subscribe(move |v| result_clone.lock().unwrap().push(v));

    assert_eq!(*result.lock().unwrap(), vec![true]);
  }
}
```

- [x] **Step 2: Register the module and add the trait method**

In `src/ops.rs`, add `pub mod every;` to the module list and `pub use every::*;` to the re-exports, keeping alphabetical order.

In `src/observable.rs`, directly after the `contains` method, add:

```rust
  /// Emit whether every item satisfies a predicate
  ///
  /// Emits `false` and completes as soon as an item fails the predicate,
  /// unsubscribing the source. Emits `true` on completion when every item
  /// passed, including for an empty source.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let observable = Local::from_iter([1, 2, 3]).every(|v| *v > 0);
  /// // Emits: true
  /// ```
  #[doc(alias = "all")]
  fn every<F>(self, predicate: F) -> Self::With<Every<Self::Inner, F>>
  where
    F: for<'a> FnMut(&Self::Item<'a>) -> bool,
  {
    self.transform(|source| Every { source, predicate })
  }
```

Check that `Every` is in scope at the top of `src/observable.rs`; the file imports operators via `use crate::ops::*;` or an explicit list. If explicit, add `Every` to that list.

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::every && cargo test --doc every`
Expected: 5 unit tests pass, doctests pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/every.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.every): add every operator"
```

---

### Task 2: `ignore_elements`

**Files:**
- Create: `src/ops/ignore_elements.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (add method after `every`)

**Interfaces:**
- Produces: `Observable::ignore_elements(self) -> Self::With<IgnoreElements<Self::Inner>>`, same `Item` and `Err` as the source.

- [x] **Step 1: Write the operator file with tests**

```rust
//! IgnoreElements operator implementation
//!
//! Drops every item and forwards only the terminal notification.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// IgnoreElements operator: Suppresses all items, mirrors error and completion
///
/// Useful when only the outcome of a stream matters, for example waiting for
/// a write to finish.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut items = Vec::new();
/// let mut completed = false;
/// Local::from_iter([1, 2, 3])
///   .ignore_elements()
///   .on_complete(|| completed = true)
///   .subscribe(|v| items.push(v));
/// assert!(items.is_empty());
/// assert!(completed);
/// ```
#[derive(Clone)]
pub struct IgnoreElements<S> {
  pub source: S,
}

impl<S: ObservableType> ObservableType for IgnoreElements<S> {
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that discards items
pub struct IgnoreElementsObserver<O> {
  observer: O,
}

impl<O, Item, Err> Observer<Item, Err> for IgnoreElementsObserver<O>
where
  O: Observer<Item, Err>,
{
  fn next(&mut self, _value: Item) {}

  fn error(self, e: Err) { self.observer.error(e); }

  fn complete(self) { self.observer.complete(); }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, C> CoreObservable<C> for IgnoreElements<S>
where
  C: Context,
  S: CoreObservable<C::With<IgnoreElementsObserver<C::Inner>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let wrapped = context.transform(|observer| IgnoreElementsObserver { observer });
    self.source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_ignore_elements_drops_items_and_completes() {
    let items = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let seen = Rc::new(RefCell::new(0));
    let items_c = items.clone();
    let completed_c = completed.clone();
    let seen_c = seen.clone();

    Local::from_iter([1, 2, 3])
      .tap(move |_| *seen_c.borrow_mut() += 1)
      .ignore_elements()
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| items_c.borrow_mut().push(v));

    assert_eq!(*items.borrow(), Vec::<i32>::new());
    assert!(*completed.borrow());
    // The source still produced every item; only the downstream was shielded
    assert_eq!(*seen.borrow(), 3);
  }

  #[rxrust_macro::test]
  fn test_ignore_elements_error_propagation() {
    let error = Rc::new(RefCell::new(String::new()));
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .ignore_elements()
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), "boom");
  }
}
```

- [x] **Step 2: Register and add the trait method**

`src/ops.rs`: add `pub mod ignore_elements;` and `pub use ignore_elements::*;`.

`src/observable.rs`, after `every`:

```rust
  /// Drop every item and mirror only the terminal notification
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter([1, 2, 3])
  ///   .ignore_elements()
  ///   .on_complete(|| println!("done"))
  ///   .subscribe(|_| unreachable!());
  /// ```
  fn ignore_elements(self) -> Self::With<IgnoreElements<Self::Inner>> {
    self.transform(|source| IgnoreElements { source })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::ignore_elements && cargo test --doc ignore_elements`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/ignore_elements.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.ignore_elements): add ignore_elements operator"
```

---

### Task 3: `is_empty`

**Files:**
- Create: `src/ops/is_empty.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `ignore_elements`)

**Interfaces:**
- Produces: `Observable::is_empty(self) -> Self::With<IsEmpty<Self::Inner>>`, emitting one `bool`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! IsEmpty operator implementation
//!
//! Emits `true` if the source completes without emitting, `false` on the
//! first item.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// IsEmpty operator: Reports whether the source emitted anything
///
/// Emits `false` and completes on the first item, unsubscribing the source.
/// Emits `true` and completes when the source completes without items.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = None;
/// Local::from_iter([1, 2])
///   .is_empty()
///   .subscribe(|v| result = Some(v));
/// assert_eq!(result, Some(false));
///
/// let mut result = None;
/// Local::from_iter(std::iter::empty::<i32>())
///   .is_empty()
///   .subscribe(|v| result = Some(v));
/// assert_eq!(result, Some(true));
/// ```
#[derive(Clone)]
pub struct IsEmpty<S> {
  pub source: S,
}

impl<S: ObservableType> ObservableType for IsEmpty<S> {
  type Item<'a>
    = bool
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that reports emptiness and short-circuits on the first item
pub struct IsEmptyObserver<O> {
  observer: Option<O>,
}

impl<O, Item, Err> Observer<Item, Err> for IsEmptyObserver<O>
where
  O: Observer<bool, Err>,
{
  fn next(&mut self, _value: Item) {
    if let Some(mut observer) = self.observer.take() {
      observer.next(false);
      observer.complete();
    }
  }

  fn error(self, e: Err) {
    if let Some(observer) = self.observer {
      observer.error(e);
    }
  }

  fn complete(self) {
    if let Some(mut observer) = self.observer {
      observer.next(true);
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

impl<S, C> CoreObservable<C> for IsEmpty<S>
where
  C: Context,
  S: CoreObservable<C::With<IsEmptyObserver<C::Inner>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let wrapped = context.transform(|observer| IsEmptyObserver { observer: Some(observer) });
    self.source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_is_empty_false_short_circuits() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::new(RefCell::new(0));
    let result_c = result.clone();
    let seen_c = seen.clone();

    Local::from_iter([1, 2, 3])
      .tap(move |_| *seen_c.borrow_mut() += 1)
      .is_empty()
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![false]);
    assert_eq!(*seen.borrow(), 1);
  }

  #[rxrust_macro::test]
  fn test_is_empty_true_on_empty() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter(std::iter::empty::<i32>())
      .is_empty()
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![true]);
  }

  #[rxrust_macro::test]
  fn test_is_empty_error_propagation() {
    let error = Rc::new(RefCell::new(String::new()));
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .is_empty()
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), "boom");
  }
}
```

- [x] **Step 2: Register and add the trait method**

`src/ops.rs`: add `pub mod is_empty;` and `pub use is_empty::*;`.

`src/observable.rs`, after `ignore_elements`:

```rust
  /// Emit whether the source completed without emitting any item
  ///
  /// Emits `false` and completes on the first item, unsubscribing the
  /// source; emits `true` when an empty source completes.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let observable = Local::from_iter([1, 2, 3]).is_empty();
  /// // Emits: false
  /// ```
  fn is_empty(self) -> Self::With<IsEmpty<Self::Inner>> {
    self.transform(|source| IsEmpty { source })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::is_empty && cargo test --doc is_empty`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/is_empty.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.is_empty): add is_empty operator"
```

---

### Task 4: `element_at` and `element_at_or`

**Files:**
- Create: `src/ops/element_at.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `first_or`, around line 520)

**Interfaces:**
- Consumes: `Skip { source, count }`, `Take { source, count }`, `DefaultIfEmpty::new(source, default_value)` (all existing).
- Produces: `Observable::element_at(self, index: usize) -> Self::With<ElementAt<Self::Inner>>` and `Observable::element_at_or<'a>(self, index: usize, default_value: Self::Item<'a>) -> Self::With<ElementAtOr<Self::Inner, Self::Item<'a>>>`.

- [x] **Step 1: Write the alias file with tests**

```rust
//! ElementAt operator implementation
//!
//! Emits the item at a zero-based index. Composed from `skip` and `take`,
//! with `default_if_empty` for the `_or` form.

use crate::ops::{default_if_empty::DefaultIfEmpty, skip::Skip, take::Take};

/// Emits only the item at `index`, then completes. Completes empty if the
/// source has fewer items.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([10, 20, 30])
///   .element_at(1)
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![20]);
/// ```
#[doc(alias = "elementAt")]
pub type ElementAt<S> = Take<Skip<S>>;

/// Like [`ElementAt`] but emits a default when the source is too short.
pub type ElementAtOr<S, Item> = DefaultIfEmpty<Take<Skip<S>>, Item>;

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_element_at_emits_index() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::new(RefCell::new(0));
    let result_c = result.clone();
    let seen_c = seen.clone();

    Local::from_iter([10, 20, 30, 40])
      .tap(move |_| *seen_c.borrow_mut() += 1)
      .element_at(2)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![30]);
    // Stops pulling after the wanted item
    assert_eq!(*seen.borrow(), 3);
  }

  #[rxrust_macro::test]
  fn test_element_at_out_of_range_completes_empty() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::from_iter([10, 20])
      .element_at(5)
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), Vec::<i32>::new());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_element_at_or_default() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([10, 20])
      .element_at_or(5, -1)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![-1]);
  }

  #[rxrust_macro::test]
  fn test_element_at_or_present() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([10, 20])
      .element_at_or(0, -1)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![10]);
  }
}
```

- [x] **Step 2: Register and add the trait methods**

`src/ops.rs`: add `pub mod element_at;` and `pub use element_at::*;`.

`src/observable.rs`, after `first_or`:

```rust
  /// Emit only the item at the zero-based `index`, then complete
  ///
  /// Completes without emitting if the source has fewer than `index + 1`
  /// items. Use [`Observable::element_at_or`] to supply a fallback.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let observable = Local::from_iter([10, 20, 30]).element_at(1);
  /// // Emits: 20
  /// ```
  #[doc(alias = "elementAt")]
  fn element_at(self, index: usize) -> Self::With<ElementAt<Self::Inner>> {
    self.transform(|source| Take { source: Skip { source, count: index }, count: 1 })
  }

  /// Emit the item at `index`, or `default_value` if the source is too short
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let observable = Local::from_iter([10, 20]).element_at_or(5, 0);
  /// // Emits: 0
  /// ```
  fn element_at_or<'a>(
    self, index: usize, default_value: Self::Item<'a>,
  ) -> Self::With<ElementAtOr<Self::Inner, Self::Item<'a>>> {
    self.transform(|source| {
      DefaultIfEmpty::new(Take { source: Skip { source, count: index }, count: 1 }, default_value)
    })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::element_at && cargo test --doc element_at`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/element_at.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.element_at): add element_at and element_at_or operators"
```

---

### Task 5: `find` and `find_index`

**Files:**
- Create: `src/ops/find.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `element_at_or`)

**Interfaces:**
- Consumes: `Filter { source, filter }`, `Take { source, count }` (existing).
- Produces: `Observable::find<F>(self, predicate: F) -> Self::With<Find<Self::Inner, F>>` and `Observable::find_index<F>(self, predicate: F) -> Self::With<FindIndex<Self::Inner, F>>` (emits `usize`), both with `F: for<'a> FnMut(&Self::Item<'a>) -> bool`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! Find operators implementation
//!
//! `find` emits the first item matching a predicate; `find_index` emits its
//! zero-based position.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  ops::{filter::Filter, take::Take},
};

/// Emits the first item satisfying the predicate, then completes. Completes
/// empty when nothing matches. Composed from `filter` and `take`.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([1, 4, 6, 8])
///   .find(|v| v % 2 == 0)
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![4]);
/// ```
pub type Find<S, F> = Take<Filter<S, F>>;

/// FindIndex operator: Emits the zero-based index of the first match
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([1, 4, 6, 8])
///   .find_index(|v| v % 2 == 0)
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1]);
/// ```
#[doc(alias = "findIndex")]
#[derive(Clone)]
pub struct FindIndex<S, F> {
  pub source: S,
  pub predicate: F,
}

impl<S, F> ObservableType for FindIndex<S, F>
where
  S: ObservableType,
{
  type Item<'a>
    = usize
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that counts positions until the predicate matches
pub struct FindIndexObserver<O, F> {
  observer: Option<O>,
  predicate: F,
  index: usize,
}

impl<O, F, Item, Err> Observer<Item, Err> for FindIndexObserver<O, F>
where
  O: Observer<usize, Err>,
  F: FnMut(&Item) -> bool,
{
  fn next(&mut self, v: Item) {
    if self.observer.is_none() {
      return;
    }
    if (self.predicate)(&v) {
      if let Some(mut observer) = self.observer.take() {
        observer.next(self.index);
        observer.complete();
      }
    } else {
      self.index += 1;
    }
  }

  fn error(self, e: Err) {
    if let Some(observer) = self.observer {
      observer.error(e);
    }
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

impl<S, F, C> CoreObservable<C> for FindIndex<S, F>
where
  C: Context,
  S: CoreObservable<C::With<FindIndexObserver<C::Inner, F>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let FindIndex { source, predicate } = self;
    let wrapped = context
      .transform(|observer| FindIndexObserver { observer: Some(observer), predicate, index: 0 });
    source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_find_first_match_and_stop() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let seen = Rc::new(RefCell::new(0));
    let result_c = result.clone();
    let seen_c = seen.clone();

    Local::from_iter([1, 3, 4, 6])
      .tap(move |_| *seen_c.borrow_mut() += 1)
      .find(|v| v % 2 == 0)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![4]);
    assert_eq!(*seen.borrow(), 3);
  }

  #[rxrust_macro::test]
  fn test_find_no_match_completes_empty() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::from_iter([1, 3, 5])
      .find(|v| v % 2 == 0)
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), Vec::<i32>::new());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_find_index_first_match() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([1, 3, 4, 6])
      .find_index(|v| v % 2 == 0)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![2]);
  }

  #[rxrust_macro::test]
  fn test_find_index_no_match_completes_empty() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::from_iter([1, 3, 5])
      .find_index(|v| v % 2 == 0)
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), Vec::<usize>::new());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_find_index_error_propagation() {
    let error = Rc::new(RefCell::new(String::new()));
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .find_index(|v| *v == 1)
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), "boom");
  }
}
```

- [x] **Step 2: Register and add the trait methods**

`src/ops.rs`: add `pub mod find;` and `pub use find::*;`.

`src/observable.rs`, after `element_at_or`:

```rust
  /// Emit the first item that satisfies `predicate`, then complete
  ///
  /// Completes without emitting when nothing matches.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let observable = Local::from_iter([1, 4, 6]).find(|v| v % 2 == 0);
  /// // Emits: 4
  /// ```
  fn find<F>(self, predicate: F) -> Self::With<Find<Self::Inner, F>>
  where
    F: for<'a> FnMut(&Self::Item<'a>) -> bool,
  {
    self.transform(|source| Take { source: Filter { source, filter: predicate }, count: 1 })
  }

  /// Emit the zero-based index of the first item that satisfies `predicate`
  ///
  /// Completes without emitting when nothing matches.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// let observable = Local::from_iter([1, 4, 6]).find_index(|v| v % 2 == 0);
  /// // Emits: 1
  /// ```
  #[doc(alias = "findIndex")]
  fn find_index<F>(self, predicate: F) -> Self::With<FindIndex<Self::Inner, F>>
  where
    F: for<'a> FnMut(&Self::Item<'a>) -> bool,
  {
    self.transform(|source| FindIndex { source, predicate })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::find && cargo test --doc find`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/find.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.find): add find and find_index operators"
```

---

### Task 6: `end_with`

**Files:**
- Create: `src/ops/end_with.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `start_with`)

**Interfaces:**
- Produces: `Observable::end_with<Item>(self, values: Vec<Item>) -> Self::With<EndWith<Self::Inner, Item>>`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! EndWith operator implementation
//!
//! Emits specified items after the source completes.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// EndWith operator: Appends values after the source completes
///
/// The values are emitted in order when the source completes, then the
/// stream completes. They are not emitted if the source errors.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([1, 2])
///   .end_with(vec![3, 4])
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 2, 3, 4]);
/// ```
#[doc(alias = "endWith")]
#[derive(Clone)]
pub struct EndWith<S, Item> {
  pub source: S,
  pub values: Vec<Item>,
}

impl<S, Item> ObservableType for EndWith<S, Item>
where
  S: ObservableType,
{
  type Item<'a>
    = Item
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that flushes the trailing values on completion
pub struct EndWithObserver<O, Item> {
  observer: O,
  values: Vec<Item>,
}

impl<O, Item, Err> Observer<Item, Err> for EndWithObserver<O, Item>
where
  O: Observer<Item, Err>,
{
  fn next(&mut self, value: Item) { self.observer.next(value); }

  fn error(self, e: Err) { self.observer.error(e); }

  fn complete(self) {
    let EndWithObserver { mut observer, values } = self;
    for value in values {
      if observer.is_closed() {
        break;
      }
      observer.next(value);
    }
    observer.complete();
  }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, C, Item> CoreObservable<C> for EndWith<S, Item>
where
  C: Context,
  S: CoreObservable<C::With<EndWithObserver<C::Inner, Item>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let EndWith { source, values } = self;
    let wrapped = context.transform(|observer| EndWithObserver { observer, values });
    source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_end_with_appends_values() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([1, 2])
      .end_with(vec![3, 4])
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2, 3, 4]);
  }

  #[rxrust_macro::test]
  fn test_end_with_on_empty_source() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter(std::iter::empty::<i32>())
      .end_with(vec![9])
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![9]);
  }

  #[rxrust_macro::test]
  fn test_end_with_not_emitted_on_error() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let error = Rc::new(RefCell::new(String::new()));
    let result_c = result.clone();
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .end_with(vec![1])
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), Vec::<i32>::new());
    assert_eq!(*error.borrow(), "boom");
  }

  #[rxrust_macro::test]
  fn test_end_with_respects_take() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([1])
      .end_with(vec![2, 3, 4])
      .take(2)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
  }
}
```

- [x] **Step 2: Register and add the trait method**

`src/ops.rs`: add `pub mod end_with;` and `pub use end_with::*;`.

`src/observable.rs`, after `start_with`:

```rust
  /// Emit `values` after the source completes, then complete
  ///
  /// Mirror of [`Observable::start_with`]. The values are not emitted if the
  /// source errors.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter([1, 2])
  ///   .end_with(vec![3])
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 1, 2, 3
  /// ```
  #[doc(alias = "endWith")]
  fn end_with<Item>(self, values: Vec<Item>) -> Self::With<EndWith<Self::Inner, Item>> {
    self.transform(|source| EndWith { source, values })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::end_with && cargo test --doc end_with`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/end_with.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.end_with): add end_with operator"
```

---

### Task 7: `throw_if_empty`

**Files:**
- Create: `src/ops/throw_if_empty.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `default_if_empty`)

**Interfaces:**
- Produces: `Observable::throw_if_empty<F>(self, error_fn: F) -> Self::With<ThrowIfEmpty<Self::Inner, F>> where F: FnOnce() -> Self::Err`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! ThrowIfEmpty operator implementation
//!
//! Errors instead of completing when the source emits nothing.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// ThrowIfEmpty operator: Turns an empty completion into an error
///
/// If the source completes without emitting, `error_fn` is called and its
/// result is emitted as the error. Otherwise the operator is transparent.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut error = None;
/// Local::from_iter(std::iter::empty::<i32>())
///   .map_err(|_: std::convert::Infallible| String::new())
///   .throw_if_empty(|| "empty".to_string())
///   .on_error(|e| error = Some(e))
///   .subscribe(|_| {});
/// assert_eq!(error.as_deref(), Some("empty"));
/// ```
#[doc(alias = "throwIfEmpty")]
#[derive(Clone)]
pub struct ThrowIfEmpty<S, F> {
  pub source: S,
  pub error_fn: F,
}

impl<S, F> ObservableType for ThrowIfEmpty<S, F>
where
  S: ObservableType,
{
  type Item<'a>
    = S::Item<'a>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that tracks whether anything was emitted
pub struct ThrowIfEmptyObserver<O, F> {
  observer: O,
  error_fn: F,
  is_empty: bool,
}

impl<O, F, Item, Err> Observer<Item, Err> for ThrowIfEmptyObserver<O, F>
where
  O: Observer<Item, Err>,
  F: FnOnce() -> Err,
{
  fn next(&mut self, value: Item) {
    self.is_empty = false;
    self.observer.next(value);
  }

  fn error(self, e: Err) { self.observer.error(e); }

  fn complete(self) {
    let ThrowIfEmptyObserver { observer, error_fn, is_empty } = self;
    if is_empty {
      observer.error(error_fn());
    } else {
      observer.complete();
    }
  }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, F, C> CoreObservable<C> for ThrowIfEmpty<S, F>
where
  C: Context,
  S: CoreObservable<C::With<ThrowIfEmptyObserver<C::Inner, F>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let ThrowIfEmpty { source, error_fn } = self;
    let wrapped =
      context.transform(|observer| ThrowIfEmptyObserver { observer, error_fn, is_empty: true });
    source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_throw_if_empty_errors_on_empty() {
    let error = Rc::new(RefCell::new(None));
    let completed = Rc::new(RefCell::new(false));
    let error_c = error.clone();
    let completed_c = completed.clone();

    Local::from_iter(std::iter::empty::<i32>())
      .map_err(|_: Infallible| String::new())
      .throw_if_empty(|| "empty".to_string())
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    assert_eq!(error.borrow().as_deref(), Some("empty"));
    assert!(!*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_throw_if_empty_transparent_when_items() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::from_iter([1, 2])
      .map_err(|_: Infallible| String::new())
      .throw_if_empty(|| "empty".to_string())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_throw_if_empty_forwards_source_error() {
    let error = Rc::new(RefCell::new(String::new()));
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .throw_if_empty(|| "empty".to_string())
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), "boom");
  }
}
```

If `map_err` does not accept a closure over `Infallible` the way the tests assume, replace `.map_err(|_: Infallible| String::new())` with `Local::create::<i32, String, _, _>(|_| {})` style construction; check `src/ops/map_err.rs` for its exact bound first.

- [x] **Step 2: Register and add the trait method**

`src/ops.rs`: add `pub mod throw_if_empty;` and `pub use throw_if_empty::*;`.

`src/observable.rs`, after `default_if_empty`:

```rust
  /// Error with `error_fn()` instead of completing when the source is empty
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter(std::iter::empty::<i32>())
  ///   .map_err(|_: std::convert::Infallible| String::new())
  ///   .throw_if_empty(|| "nothing".to_string())
  ///   .on_error(|e| println!("{}", e))
  ///   .subscribe(|_| {});
  /// // Prints: nothing
  /// ```
  #[doc(alias = "throwIfEmpty")]
  fn throw_if_empty<F>(self, error_fn: F) -> Self::With<ThrowIfEmpty<Self::Inner, F>>
  where
    F: FnOnce() -> Self::Err,
  {
    self.transform(|source| ThrowIfEmpty { source, error_fn })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::throw_if_empty && cargo test --doc throw_if_empty`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/throw_if_empty.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.throw_if_empty): add throw_if_empty operator"
```

---

### Task 8: `materialize` and `dematerialize`

**Files:**
- Create: `src/ops/materialize.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `finalize`), `src/prelude.rs` (re-export `Notification`)

**Interfaces:**
- Produces: `pub enum Notification<Item, Err> { Next(Item), Error(Err), Complete }`; `Observable::materialize(self) -> Self::With<Materialize<Self::Inner>>` (Item `Notification<Item, Err>`, Err `Infallible`); `Observable::dematerialize<Item, Err>(self) -> Self::With<Dematerialize<Self::Inner, Item, Err>>` requiring `Self: Observable<Err = Infallible>` and `for<'a> Self::Item<'a>: Into<Notification<Item, Err>>`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! Materialize and Dematerialize operator implementations
//!
//! `materialize` turns every event into a [`Notification`] item;
//! `dematerialize` turns [`Notification`] items back into events.

use std::{convert::Infallible, marker::PhantomData};

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// A reified observable event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Notification<Item, Err> {
  /// An item was emitted
  Next(Item),
  /// The stream errored
  Error(Err),
  /// The stream completed
  Complete,
}

/// Materialize operator: Emits every event as a [`Notification`]
///
/// Errors and completion become items, after which the stream completes.
/// The resulting error type is [`Infallible`].
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([1, 2])
///   .materialize()
///   .subscribe(|n| result.push(n));
/// assert_eq!(
///   result,
///   vec![Notification::Next(1), Notification::Next(2), Notification::Complete]
/// );
/// ```
#[derive(Clone)]
pub struct Materialize<S> {
  pub source: S,
}

impl<S: ObservableType> ObservableType for Materialize<S> {
  type Item<'a>
    = Notification<S::Item<'a>, S::Err>
  where
    Self: 'a;
  type Err = Infallible;
}

/// Observer that wraps events into notifications
pub struct MaterializeObserver<O> {
  observer: O,
}

impl<O, Item, Err> Observer<Item, Err> for MaterializeObserver<O>
where
  O: Observer<Notification<Item, Err>, Infallible>,
{
  fn next(&mut self, v: Item) { self.observer.next(Notification::Next(v)); }

  fn error(self, e: Err) {
    let mut observer = self.observer;
    observer.next(Notification::Error(e));
    observer.complete();
  }

  fn complete(self) {
    let mut observer = self.observer;
    observer.next(Notification::Complete);
    observer.complete();
  }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, C> CoreObservable<C> for Materialize<S>
where
  C: Context,
  S: CoreObservable<C::With<MaterializeObserver<C::Inner>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let wrapped = context.transform(|observer| MaterializeObserver { observer });
    self.source.subscribe(wrapped)
  }
}

/// Dematerialize operator: Replays [`Notification`] items as real events
///
/// The source must be infallible; its items are converted into
/// notifications. The first `Error` or `Complete` notification terminates
/// the stream and unsubscribes the source.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// let mut error = None;
/// Local::from_iter(vec![
///   Notification::Next(1),
///   Notification::Error("boom".to_string()),
///   Notification::Next(2),
/// ])
/// .dematerialize()
/// .on_error(|e| error = Some(e))
/// .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1]);
/// assert_eq!(error.as_deref(), Some("boom"));
/// ```
pub struct Dematerialize<S, Item, Err> {
  pub source: S,
  _marker: PhantomData<fn() -> (Item, Err)>,
}

impl<S: Clone, Item, Err> Clone for Dematerialize<S, Item, Err> {
  fn clone(&self) -> Self { Self { source: self.source.clone(), _marker: PhantomData } }
}

impl<S, Item, Err> Dematerialize<S, Item, Err> {
  /// Wraps a source of notifications.
  pub fn new(source: S) -> Self { Self { source, _marker: PhantomData } }
}

impl<S, Item, Err> ObservableType for Dematerialize<S, Item, Err>
where
  S: ObservableType,
{
  type Item<'a>
    = Item
  where
    Self: 'a;
  type Err = Err;
}

/// Observer that unwraps notifications into events
pub struct DematerializeObserver<O, Item, Err> {
  observer: Option<O>,
  _marker: PhantomData<fn() -> (Item, Err)>,
}

impl<O, SrcItem, Item, Err> Observer<SrcItem, Infallible> for DematerializeObserver<O, Item, Err>
where
  O: Observer<Item, Err>,
  SrcItem: Into<Notification<Item, Err>>,
{
  fn next(&mut self, v: SrcItem) {
    match v.into() {
      Notification::Next(item) => {
        if let Some(observer) = self.observer.as_mut() {
          observer.next(item);
        }
      }
      Notification::Error(e) => {
        if let Some(observer) = self.observer.take() {
          observer.error(e);
        }
      }
      Notification::Complete => {
        if let Some(observer) = self.observer.take() {
          observer.complete();
        }
      }
    }
  }

  fn error(self, e: Infallible) { match e {} }

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

impl<S, C, Item, Err> CoreObservable<C> for Dematerialize<S, Item, Err>
where
  C: Context,
  S: CoreObservable<C::With<DematerializeObserver<C::Inner, Item, Err>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let wrapped = context
      .transform(|observer| DematerializeObserver { observer: Some(observer), _marker: PhantomData });
    self.source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use super::Notification;
  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_materialize_items_and_complete() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([1, 2])
      .materialize()
      .subscribe(move |n| result_c.borrow_mut().push(n));

    assert_eq!(
      *result.borrow(),
      vec![Notification::Next(1), Notification::Next(2), Notification::Complete]
    );
  }

  #[rxrust_macro::test]
  fn test_materialize_error_becomes_item() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .materialize()
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |n| result_c.borrow_mut().push(n));

    assert_eq!(*result.borrow(), vec![Notification::Error("boom".to_string())]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_dematerialize_replays_events() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::from_iter(vec![
      Notification::<i32, String>::Next(1),
      Notification::Next(2),
      Notification::Complete,
      Notification::Next(3),
    ])
    .dematerialize()
    .on_complete(move || *completed_c.borrow_mut() = true)
    .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_dematerialize_error_notification() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();

    Local::from_iter(vec![Notification::<i32, String>::Error("boom".to_string())])
      .dematerialize()
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(error.borrow().as_deref(), Some("boom"));
  }

  #[rxrust_macro::test]
  fn test_round_trip() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([1, 2, 3])
      .materialize()
      .dematerialize()
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2, 3]);
  }
}
```

If type inference for `dematerialize()` fails in the tests, write `dematerialize::<i32, String>()` and keep the turbofish in the doctest as well; do not change the operator's signature.

- [x] **Step 2: Register, export, and add the trait methods**

`src/ops.rs`: add `pub mod materialize;` and `pub use materialize::*;`.

`src/prelude.rs`: extend the `ops` re-export to `pub use crate::ops::{into_future::*, into_stream::*, materialize::Notification};`.

`src/observable.rs`, after `finalize`:

```rust
  /// Emit every event as a [`Notification`] item and complete afterwards
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter([1])
  ///   .materialize()
  ///   .subscribe(|n| println!("{:?}", n));
  /// // Prints: Next(1), Complete
  /// ```
  fn materialize(self) -> Self::With<Materialize<Self::Inner>> {
    self.transform(|source| Materialize { source })
  }

  /// Replay [`Notification`] items as real events
  ///
  /// The source must be infallible. Stops at the first `Error` or
  /// `Complete` notification.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter(vec![Notification::<i32, String>::Next(1), Notification::Complete])
  ///   .dematerialize()
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 1
  /// ```
  fn dematerialize<Item, Err>(self) -> Self::With<Dematerialize<Self::Inner, Item, Err>>
  where
    Self: Observable<Err = std::convert::Infallible>,
    for<'a> Self::Item<'a>: Into<Notification<Item, Err>>,
  {
    self.transform(Dematerialize::new)
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::materialize && cargo test --doc materialize && cargo test --doc dematerialize`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/materialize.rs src/ops.rs src/observable.rs src/prelude.rs
git commit -m "feat(ops.materialize): add materialize and dematerialize operators"
```

---

### Task 9: `timestamp` and `time_interval`

**Files:**
- Create: `src/ops/timestamp.rs`, `src/ops/time_interval.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `dematerialize`), `src/prelude.rs` (re-export `Timestamped`, `Elapsed`)

**Interfaces:**
- Produces: `pub struct Timestamped<T> { pub value: T, pub timestamp: Instant }`, `pub struct Elapsed<T> { pub value: T, pub interval: Duration }`; `Observable::timestamp(self) -> Self::With<Timestamp<Self::Inner>>`; `Observable::time_interval(self) -> Self::With<TimeInterval<Self::Inner>>`.

- [x] **Step 1: Write `src/ops/timestamp.rs`**

```rust
//! Timestamp operator implementation
//!
//! Attaches the wall-clock instant of emission to each item.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  scheduler::Instant,
};

/// An item together with the instant it was emitted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Timestamped<T> {
  /// The emitted value
  pub value: T,
  /// When the value passed through the `timestamp` operator
  pub timestamp: Instant,
}

/// Timestamp operator: Wraps each item in a [`Timestamped`]
///
/// Uses [`Instant::now`] from the scheduler module, so it works on wasm.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([1, 2])
///   .timestamp()
///   .subscribe(|t| result.push(t.value));
/// assert_eq!(result, vec![1, 2]);
/// ```
#[derive(Clone)]
pub struct Timestamp<S> {
  pub source: S,
}

impl<S: ObservableType> ObservableType for Timestamp<S> {
  type Item<'a>
    = Timestamped<S::Item<'a>>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that stamps each item
pub struct TimestampObserver<O> {
  observer: O,
}

impl<O, Item, Err> Observer<Item, Err> for TimestampObserver<O>
where
  O: Observer<Timestamped<Item>, Err>,
{
  fn next(&mut self, value: Item) {
    self
      .observer
      .next(Timestamped { value, timestamp: Instant::now() });
  }

  fn error(self, e: Err) { self.observer.error(e); }

  fn complete(self) { self.observer.complete(); }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, C> CoreObservable<C> for Timestamp<S>
where
  C: Context,
  S: CoreObservable<C::With<TimestampObserver<C::Inner>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let wrapped = context.transform(|observer| TimestampObserver { observer });
    self.source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_timestamp_preserves_values_and_orders_time() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let before = Instant::now();

    Local::from_iter([1, 2, 3])
      .timestamp()
      .subscribe(move |t| result_c.borrow_mut().push(t));

    let after = Instant::now();
    let stamped = result.borrow();
    assert_eq!(stamped.iter().map(|t| t.value).collect::<Vec<_>>(), vec![1, 2, 3]);
    for pair in stamped.windows(2) {
      assert!(pair[0].timestamp <= pair[1].timestamp);
    }
    assert!(stamped[0].timestamp >= before);
    assert!(stamped[2].timestamp <= after);
  }

  #[rxrust_macro::test]
  fn test_timestamp_error_propagation() {
    let error = Rc::new(RefCell::new(String::new()));
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .timestamp()
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), "boom");
  }
}
```

- [x] **Step 2: Write `src/ops/time_interval.rs`**

```rust
//! TimeInterval operator implementation
//!
//! Attaches the time elapsed since the previous emission to each item.

use crate::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  scheduler::{Duration, Instant},
};

/// An item together with the time elapsed since the previous item, or since
/// subscription for the first item.
#[doc(alias = "TimeInterval")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Elapsed<T> {
  /// The emitted value
  pub value: T,
  /// Time since the previous emission (or subscription)
  pub interval: Duration,
}

/// TimeInterval operator: Wraps each item in an [`Elapsed`]
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([1, 2])
///   .time_interval()
///   .subscribe(|e| result.push(e.value));
/// assert_eq!(result, vec![1, 2]);
/// ```
#[doc(alias = "timeInterval")]
#[derive(Clone)]
pub struct TimeInterval<S> {
  pub source: S,
}

impl<S: ObservableType> ObservableType for TimeInterval<S> {
  type Item<'a>
    = Elapsed<S::Item<'a>>
  where
    Self: 'a;
  type Err = S::Err;
}

/// Observer that measures the gap between emissions
pub struct TimeIntervalObserver<O> {
  observer: O,
  last: Instant,
}

impl<O, Item, Err> Observer<Item, Err> for TimeIntervalObserver<O>
where
  O: Observer<Elapsed<Item>, Err>,
{
  fn next(&mut self, value: Item) {
    let now = Instant::now();
    let interval = now.duration_since(self.last);
    self.last = now;
    self.observer.next(Elapsed { value, interval });
  }

  fn error(self, e: Err) { self.observer.error(e); }

  fn complete(self) { self.observer.complete(); }

  fn is_closed(&self) -> bool { self.observer.is_closed() }
}

impl<S, C> CoreObservable<C> for TimeInterval<S>
where
  C: Context,
  S: CoreObservable<C::With<TimeIntervalObserver<C::Inner>>>,
{
  type Unsub = S::Unsub;

  fn subscribe(self, context: C) -> Self::Unsub {
    let wrapped =
      context.transform(|observer| TimeIntervalObserver { observer, last: Instant::now() });
    self.source.subscribe(wrapped)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_time_interval_preserves_values() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::from_iter([1, 2, 3])
      .time_interval()
      .subscribe(move |e| result_c.borrow_mut().push(e.value));

    assert_eq!(*result.borrow(), vec![1, 2, 3]);
  }

  #[cfg(not(target_arch = "wasm32"))]
  #[rxrust_macro::test(local)]
  async fn test_time_interval_measures_delay() {
    let result = Local::timer(Duration::from_millis(20))
      .time_interval()
      .into_future()
      .await;

    let elapsed = result.unwrap().unwrap();
    // A timer never fires early, so this lower bound is deterministic
    assert!(elapsed.interval >= Duration::from_millis(20));
  }

  #[rxrust_macro::test]
  fn test_time_interval_error_propagation() {
    let error = Rc::new(RefCell::new(String::new()));
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .time_interval()
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), "boom");
  }
}
```

If `Local::timer` emits a type other than `()` and `into_future` returns a different nesting of `Result`, adjust the unwraps to match `IntoFutureResult<T, E>` defined in `src/ops/into_future.rs` (it is `Result<Result<T, E>, IntoFutureError>`).

- [x] **Step 3: Register, export, and add the trait methods**

`src/ops.rs`: add `pub mod time_interval;`, `pub mod timestamp;` and the matching `pub use` lines.

`src/prelude.rs`: extend to `pub use crate::ops::{into_future::*, into_stream::*, materialize::Notification, time_interval::Elapsed, timestamp::Timestamped};`.

`src/observable.rs`, after `dematerialize`:

```rust
  /// Wrap each item with the [`Instant`] it was emitted
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter([1])
  ///   .timestamp()
  ///   .subscribe(|t| println!("{} at {:?}", t.value, t.timestamp));
  /// ```
  fn timestamp(self) -> Self::With<Timestamp<Self::Inner>> {
    self.transform(|source| Timestamp { source })
  }

  /// Wrap each item with the time elapsed since the previous emission
  ///
  /// The first item measures from subscription.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter([1])
  ///   .time_interval()
  ///   .subscribe(|e| println!("{} after {:?}", e.value, e.interval));
  /// ```
  #[doc(alias = "timeInterval")]
  fn time_interval(self) -> Self::With<TimeInterval<Self::Inner>> {
    self.transform(|source| TimeInterval { source })
  }
```

- [x] **Step 4: Run the tests**

Run: `cargo test --lib ops::timestamp && cargo test --lib ops::time_interval && cargo test --doc timestamp && cargo test --doc time_interval`
Expected: pass.

- [x] **Step 5: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/timestamp.rs src/ops/time_interval.rs src/ops.rs src/observable.rs src/prelude.rs
git commit -m "feat(ops.timestamp): add timestamp and time_interval operators"
```

---

### Task 10: binary `race`

**Files:**
- Create: `src/ops/race.rs`
- Modify: `src/ops.rs`, `src/observable.rs` (after `merge`)

**Interfaces:**
- Consumes: `TupleSubscription::new`, `IntoBoxedSubscription::into_boxed`, `RcDeref`/`RcDerefMut` (existing).
- Produces: `Observable::race<'a, S2>(self, other: S2) -> Self::With<Race<Self::Inner, S2::Inner>>` with the same bounds as `merge`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! Race operator implementation
//!
//! Mirrors whichever of two sources emits first and drops the other.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{IntoBoxedSubscription, Subscription, TupleSubscription},
};

/// Race operator: Mirrors the first source to emit any event
///
/// Both sources are subscribed. The first one to emit an item, error, or
/// completion wins; the other is unsubscribed and the winner is mirrored
/// from that event on. See [`crate::factory::ObservableFactory::race_observables`]
/// for the N-ary form.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::from_iter([1, 2])
///   .race(Local::from_iter([3, 4]))
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 2]);
/// ```
#[doc(alias = "raceWith")]
#[derive(Clone)]
pub struct Race<A, B> {
  pub source_a: A,
  pub source_b: B,
}

impl<A, B> ObservableType for Race<A, B>
where
  A: ObservableType,
{
  type Item<'a>
    = A::Item<'a>
  where
    Self: 'a;
  type Err = A::Err;
}

/// Which side of the race an observer belongs to
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RaceSide {
  /// The receiver of `race`
  A,
  /// The argument of `race`
  B,
}

/// State shared by both race observers
pub struct RaceState<O> {
  observer: Option<O>,
  winner: Option<RaceSide>,
}

/// Observer for one side of the race
pub struct RaceObserver<StateRc, OtherProxy> {
  state: StateRc,
  other: OtherProxy,
  side: RaceSide,
}

impl<StateRc, OtherProxy, O> RaceObserver<StateRc, OtherProxy>
where
  StateRc: RcDerefMut<Target = RaceState<O>>,
  OtherProxy: Subscription + Clone,
{
  /// Returns true when this side is, or just became, the winner. Claiming
  /// unsubscribes the other side.
  fn claim(&mut self) -> bool {
    let mut state = self.state.rc_deref_mut();
    match state.winner {
      None => {
        state.winner = Some(self.side);
        drop(state);
        self.other.clone().unsubscribe();
        true
      }
      Some(side) => side == self.side,
    }
  }
}

impl<Item, Err, O, StateRc, OtherProxy> Observer<Item, Err> for RaceObserver<StateRc, OtherProxy>
where
  O: Observer<Item, Err>,
  StateRc: RcDerefMut<Target = RaceState<O>>,
  OtherProxy: Subscription + Clone,
{
  fn next(&mut self, value: Item) {
    if self.claim() {
      let mut state = self.state.rc_deref_mut();
      if let Some(observer) = state.observer.as_mut() {
        observer.next(value);
      }
    }
  }

  fn error(mut self, err: Err) {
    if self.claim() {
      let observer = self.state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.error(err);
      }
    }
  }

  fn complete(mut self) {
    if self.claim() {
      let observer = self.state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.complete();
      }
    }
  }

  fn is_closed(&self) -> bool {
    let state = self.state.rc_deref();
    state.winner.is_some_and(|w| w != self.side)
      || state.observer.as_ref().is_none_or(|o| o.is_closed())
  }
}

type StateRc<C> = <C as Context>::RcMut<RaceState<<C as Context>::Inner>>;
type BoxedProxy<C> = <C as Context>::RcMut<Option<<C as Context>::BoxedSubscription>>;
type Proxy<C, U> = <C as Context>::RcMut<Option<U>>;

impl<A, B, C, BUnsub> CoreObservable<C> for Race<A, B>
where
  C: Context,
  A: CoreObservable<C::With<RaceObserver<StateRc<C>, Proxy<C, BUnsub>>>>,
  A::Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
  B: CoreObservable<C::With<RaceObserver<StateRc<C>, BoxedProxy<C>>>, Unsub = BUnsub>,
  BoxedProxy<C>: Subscription,
  Proxy<C, BUnsub>: Subscription,
{
  type Unsub = TupleSubscription<BoxedProxy<C>, Proxy<C, BUnsub>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let Race { source_a, source_b } = self;

    let state: StateRc<C> =
      C::RcMut::from(RaceState { observer: Some(context.into_inner()), winner: None });
    let a_proxy: BoxedProxy<C> = C::RcMut::from(None);
    let b_proxy: Proxy<C, BUnsub> = C::RcMut::from(None);

    let a_observer = RaceObserver { state: state.clone(), other: b_proxy.clone(), side: RaceSide::A };
    let a_unsub = source_a.subscribe(C::lift(a_observer));
    *a_proxy.rc_deref_mut() = Some(a_unsub.into_boxed());

    // A synchronous source may have already won; then B is never subscribed.
    if state.rc_deref().winner.is_some() {
      return TupleSubscription::new(a_proxy, b_proxy);
    }

    let b_observer = RaceObserver { state: state.clone(), other: a_proxy.clone(), side: RaceSide::B };
    let b_unsub = source_b.subscribe(C::lift(b_observer));
    *b_proxy.rc_deref_mut() = Some(b_unsub);

    // A may have won on another thread while B was subscribing.
    if state.rc_deref().winner == Some(RaceSide::A) {
      b_proxy.clone().unsubscribe();
    }

    TupleSubscription::new(a_proxy, b_proxy)
  }
}

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_race_first_side_wins() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();

    a.clone()
      .race(b.clone())
      .subscribe(move |v| result_c.borrow_mut().push(v));

    a.next(1);
    b.next(10);
    a.next(2);

    assert_eq!(*result.borrow(), vec![1, 2]);
    // The loser was unsubscribed
    assert_eq!(b.inner.subscriber_count(), 0);
    assert_eq!(a.inner.subscriber_count(), 1);
  }

  #[rxrust_macro::test]
  fn test_race_second_side_wins() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();

    a.clone()
      .race(b.clone())
      .subscribe(move |v| result_c.borrow_mut().push(v));

    b.next(10);
    a.next(1);
    b.next(20);

    assert_eq!(*result.borrow(), vec![10, 20]);
    assert_eq!(a.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_race_sync_source_wins_without_subscribing_other() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();
    let b = Local::subject::<i32, Infallible>();

    Local::from_iter([1, 2])
      .race(b.clone())
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
    assert_eq!(b.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_race_completion_counts_as_winning() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::empty()
      .map_to(0)
      .race(Local::of(1))
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), Vec::<i32>::new());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_race_error_counts_as_winning() {
    let error = Rc::new(RefCell::new(String::new()));
    let error_c = error.clone();

    Local::throw_err("boom".to_string())
      .map(|_| 0)
      .race(Local::of(1).map_err(|_: Infallible| String::new()))
      .on_error(move |e| *error_c.borrow_mut() = e)
      .subscribe(|_| {});

    assert_eq!(*error.borrow(), "boom");
  }

  #[rxrust_macro::test]
  fn test_race_unsubscribe_cancels_both() {
    let a = Local::subject::<i32, Infallible>();
    let b = Local::subject::<i32, Infallible>();

    let sub = a.clone().race(b.clone()).subscribe(|_| {});
    assert_eq!(a.inner.subscriber_count(), 1);
    assert_eq!(b.inner.subscriber_count(), 1);

    sub.unsubscribe();
    assert_eq!(a.inner.subscriber_count(), 0);
    assert_eq!(b.inner.subscriber_count(), 0);
  }
}
```

If `Local::empty().map_to(0)` does not unify item types with `Local::of(1)`, use `Local::from_iter(std::iter::empty::<i32>())` instead.

- [x] **Step 2: Register and add the trait method**

`src/ops.rs`: add `pub mod race;` and `pub use race::*;`.

`src/observable.rs`, after `merge`:

```rust
  /// Mirror whichever of two observables emits first
  ///
  /// Both are subscribed; the first to emit an item, error, or completion
  /// wins and the other is unsubscribed.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::from_iter([1, 2])
  ///   .race(Local::from_iter([3, 4]))
  ///   .subscribe(|v| println!("{}", v));
  /// // Prints: 1, 2
  /// ```
  #[doc(alias = "raceWith")]
  fn race<'a, S2>(self, other: S2) -> Self::With<Race<Self::Inner, S2::Inner>>
  where
    Self: 'a,
    S2: Observable<Inner: ObservableType<Item<'a> = Self::Item<'a>, Err = Self::Err>> + 'a,
  {
    self.transform(|source_a| Race { source_a, source_b: other.into_inner() })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::race:: && cargo test --doc race`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/race.rs src/ops.rs src/observable.rs
git commit -m "feat(ops.race): add binary race operator"
```

---

### Task 11: `race_observables`

**Files:**
- Create: `src/ops/race_all.rs`
- Modify: `src/ops.rs`, `src/factory.rs` (after `concat_observables`)

**Interfaces:**
- Consumes: `DynamicSubscriptions` (`reserve_id`, `insert`, `remove`, `unsubscribe_all`), `SourceWithDynamicSubs::new`, `IntoBoxedSubscription`.
- Produces: `ObservableFactory::race_observables<O, I>(observables: I) -> Self::With<RaceAll<O>> where O: ObservableType, I: IntoIterator<Item = Self::With<O>>`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! RaceAll operator implementation
//!
//! N-ary form of `race`: mirrors the first of many sources to emit.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription},
};

/// RaceAll operator: Mirrors the first of many sources to emit any event
///
/// Created with [`crate::factory::ObservableFactory::race_observables`].
/// Sources are subscribed in order; once one emits, the rest are
/// unsubscribed (or never subscribed) and the winner is mirrored. An empty
/// list completes immediately.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::race_observables([Local::from_iter([1, 2]), Local::from_iter([3])])
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![1, 2]);
/// ```
#[doc(alias = "race")]
pub struct RaceAll<O> {
  pub sources: Vec<O>,
}

impl<O: ObservableType> ObservableType for RaceAll<O> {
  type Item<'a>
    = O::Item<'a>
  where
    Self: 'a;
  type Err = O::Err;
}

/// State shared by every race observer
pub struct RaceAllState<Obs> {
  observer: Option<Obs>,
  winner: Option<usize>,
}

/// Observer for one indexed source
pub struct RaceAllObserver<StateRc, SubsRc> {
  state: StateRc,
  subs: SubsRc,
  index: usize,
}

impl<StateRc, SubsRc, Obs, U> RaceAllObserver<StateRc, SubsRc>
where
  StateRc: RcDerefMut<Target = RaceAllState<Obs>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  U: Subscription,
{
  /// Returns true when this source is, or just became, the winner. Claiming
  /// unsubscribes every other source and keeps our own subscription so the
  /// downstream can still cancel it.
  fn claim(&mut self) -> bool {
    let mut state = self.state.rc_deref_mut();
    match state.winner {
      None => {
        state.winner = Some(self.index);
        drop(state);
        let mut subs = self.subs.rc_deref_mut();
        let own = subs.remove(self.index);
        subs.unsubscribe_all();
        if let Some(own) = own {
          subs.insert(self.index, own);
        }
        true
      }
      Some(w) => w == self.index,
    }
  }
}

impl<Item, Err, Obs, StateRc, SubsRc, U> Observer<Item, Err> for RaceAllObserver<StateRc, SubsRc>
where
  Obs: Observer<Item, Err>,
  StateRc: RcDerefMut<Target = RaceAllState<Obs>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  U: Subscription,
{
  fn next(&mut self, value: Item) {
    if self.claim() {
      let mut state = self.state.rc_deref_mut();
      if let Some(observer) = state.observer.as_mut() {
        observer.next(value);
      }
    }
  }

  fn error(mut self, err: Err) {
    if self.claim() {
      let observer = self.state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.error(err);
      }
    }
  }

  fn complete(mut self) {
    if self.claim() {
      let observer = self.state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.complete();
      }
    }
  }

  fn is_closed(&self) -> bool {
    let state = self.state.rc_deref();
    state.winner.is_some_and(|w| w != self.index)
      || state.observer.as_ref().is_none_or(|o| o.is_closed())
  }
}

type StateRc<C> = <C as Context>::RcMut<RaceAllState<<C as Context>::Inner>>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;

impl<O, C> CoreObservable<C> for RaceAll<O>
where
  C: Context,
  O: CoreObservable<
      C::With<RaceAllObserver<StateRc<C>, SubsRc<C>>>,
      Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
    >,
{
  type Unsub = SourceWithDynamicSubs<(), SubsRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let state: StateRc<C> =
      C::RcMut::from(RaceAllState { observer: Some(context.into_inner()), winner: None });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());

    let mut subscribed_any = false;
    for source in self.sources {
      if state.rc_deref().winner.is_some() {
        break;
      }
      subscribed_any = true;
      let id = subs.rc_deref_mut().reserve_id();
      let observer = RaceAllObserver { state: state.clone(), subs: subs.clone(), index: id };
      let unsub = source.subscribe(C::lift(observer)).into_boxed();
      let winner = state.rc_deref().winner;
      match winner {
        Some(w) if w != id => unsub.unsubscribe(),
        _ => subs.rc_deref_mut().insert(id, unsub),
      }
    }

    if !subscribed_any {
      let observer = state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.complete();
      }
    }

    SourceWithDynamicSubs::new((), subs)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    cell::RefCell,
    convert::Infallible,
    rc::Rc,
    sync::{Arc, Mutex},
  };

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_race_observables_sync_first_wins() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::race_observables([
      Local::from_iter([1, 2]),
      Local::from_iter([3, 4]),
      Local::from_iter([5]),
    ])
    .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![1, 2]);
  }

  #[rxrust_macro::test]
  fn test_race_observables_later_source_wins() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();
    let mut c = Local::subject::<i32, Infallible>();

    Local::race_observables([a.clone(), b.clone(), c.clone()])
      .subscribe(move |v| result_c.borrow_mut().push(v));

    c.next(30);
    a.next(1);
    b.next(2);
    c.next(31);

    assert_eq!(*result.borrow(), vec![30, 31]);
    assert_eq!(a.inner.subscriber_count(), 0);
    assert_eq!(b.inner.subscriber_count(), 0);
    assert_eq!(c.inner.subscriber_count(), 1);
  }

  #[rxrust_macro::test]
  fn test_race_observables_empty_list_completes() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    Local::race_observables(Vec::<Local<Of<i32>>>::new())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_race_observables_unsubscribe_cancels_all() {
    let a = Local::subject::<i32, Infallible>();
    let b = Local::subject::<i32, Infallible>();

    let sub = Local::race_observables([a.clone(), b.clone()]).subscribe(|_| {});
    assert_eq!(a.inner.subscriber_count(), 1);
    assert_eq!(b.inner.subscriber_count(), 1);

    sub.unsubscribe();
    assert_eq!(a.inner.subscriber_count(), 0);
    assert_eq!(b.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_race_observables_shared() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result_c = result.clone();

    Shared::race_observables([Shared::from_iter([1, 2]), Shared::from_iter([3])])
      .subscribe(move |v| result_c.lock().unwrap().push(v));

    assert_eq!(*result.lock().unwrap(), vec![1, 2]);
  }
}
```

If `Vec::<Local<Of<i32>>>::new()` does not name the type correctly, use `Local::race_observables(std::iter::empty::<Local<Of<i32>>>())`; `Of` is exported from the prelude.

- [x] **Step 2: Register and add the factory method**

`src/ops.rs`: add `pub mod race_all;` and `pub use race_all::*;`.

`src/factory.rs`, after `concat_observables`:

```rust
  /// Mirror whichever of many observables emits first.
  ///
  /// Subscribes in order; the first to emit an item, error, or completion
  /// wins and the rest are unsubscribed (or never subscribed). An empty
  /// iterator completes immediately.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::race_observables([Local::from_iter([1, 2]), Local::from_iter([3])])
  ///   .subscribe(|v| println!("Got: {}", v));
  /// // Prints: 1, 2
  /// ```
  ///
  /// # See Also
  ///
  /// * [`Observable::race`] - Binary instance method
  #[doc(alias = "race")]
  fn race_observables<O, I>(observables: I) -> Self::With<crate::ops::race_all::RaceAll<O>>
  where
    O: ObservableType,
    I: IntoIterator<Item = Self::With<O>>,
  {
    let sources = observables
      .into_iter()
      .map(Context::into_inner)
      .collect();
    Self::lift(crate::ops::race_all::RaceAll { sources })
  }
```

Check the `use` block at the top of `src/factory.rs` includes `Context` (it must, since `Self::lift` is a `Context` method); if `Context::into_inner` cannot be used as a path, write `.map(|o| o.into_inner())`.

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::race_all && cargo test --doc race_observables`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/race_all.rs src/ops.rs src/factory.rs
git commit -m "feat(factory.race_observables): add N-ary race factory"
```

---

### Task 12: `fork_join_observables`

**Files:**
- Create: `src/ops/fork_join.rs`
- Modify: `src/ops.rs`, `src/factory.rs` (after `race_observables`)

**Interfaces:**
- Produces: `ObservableFactory::fork_join_observables<O, I>(observables: I) -> Self::With<ForkJoin<O>>`, emitting one `Vec<Item>`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! ForkJoin operator implementation
//!
//! Waits for every source to complete, then emits their last values.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription},
};

/// ForkJoin operator: Emits the last value of every source once all complete
///
/// Created with [`crate::factory::ObservableFactory::fork_join_observables`].
/// Emits one `Vec` holding each source's last value in input order, then
/// completes. If any source completes without emitting, the result completes
/// without emitting. An error from any source is forwarded immediately and
/// the rest are unsubscribed. An empty list completes immediately.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::fork_join_observables([Local::from_iter([1, 2]), Local::from_iter([3])])
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![vec![2, 3]]);
/// ```
#[doc(alias = "forkJoin")]
pub struct ForkJoin<O> {
  pub sources: Vec<O>,
}

impl<O: ObservableType> ObservableType for ForkJoin<O> {
  type Item<'a>
    = Vec<O::Item<'a>>
  where
    Self: 'a;
  type Err = O::Err;
}

/// State shared by every fork-join observer
pub struct ForkJoinState<Obs, Item> {
  observer: Option<Obs>,
  values: Vec<Option<Item>>,
  remaining: usize,
}

impl<Obs, Item> ForkJoinState<Obs, Item> {
  fn finish_if_done<Err>(&mut self)
  where
    Obs: Observer<Vec<Item>, Err>,
  {
    if self.remaining != 0 {
      return;
    }
    if let Some(mut observer) = self.observer.take() {
      if self.values.iter().all(Option::is_some) {
        let values = self.values.drain(..).flatten().collect();
        observer.next(values);
      }
      observer.complete();
    }
  }
}

/// Observer for one indexed source
pub struct ForkJoinObserver<StateRc, SubsRc> {
  state: StateRc,
  subs: SubsRc,
  index: usize,
}

impl<Item, Err, Obs, StateRc, SubsRc, U> Observer<Item, Err> for ForkJoinObserver<StateRc, SubsRc>
where
  Obs: Observer<Vec<Item>, Err>,
  StateRc: RcDerefMut<Target = ForkJoinState<Obs, Item>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  U: Subscription,
{
  fn next(&mut self, value: Item) {
    let mut state = self.state.rc_deref_mut();
    if state.observer.is_some() {
      state.values[self.index] = Some(value);
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
    self.subs.rc_deref_mut().remove(self.index);
    let mut state = self.state.rc_deref_mut();
    if state.observer.is_none() {
      return;
    }
    if state.values[self.index].is_none() {
      // This source can never contribute, so the join can never emit.
      let observer = state.observer.take();
      drop(state);
      if let Some(observer) = observer {
        observer.complete();
      }
      self.subs.rc_deref_mut().unsubscribe_all();
      return;
    }
    state.remaining -= 1;
    state.finish_if_done::<Err>();
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

type StateRc<'a, C, O> =
  <C as Context>::RcMut<ForkJoinState<<C as Context>::Inner, <O as ObservableType>::Item<'a>>>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;

impl<O, C> CoreObservable<C> for ForkJoin<O>
where
  C: Context,
  O: ObservableType
    + for<'a> CoreObservable<
      C::With<ForkJoinObserver<StateRc<'a, C, O>, SubsRc<C>>>,
      Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
    >,
{
  type Unsub = SourceWithDynamicSubs<(), SubsRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let total = self.sources.len();
    let state: StateRc<C, O> = C::RcMut::from(ForkJoinState {
      observer: Some(context.into_inner()),
      values: (0..total).map(|_| None).collect(),
      remaining: total,
    });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());

    if total == 0 {
      let observer = state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.complete();
      }
      return SourceWithDynamicSubs::new((), subs);
    }

    for source in self.sources {
      if state.rc_deref().observer.is_none() {
        break;
      }
      let id = subs.rc_deref_mut().reserve_id();
      let observer = ForkJoinObserver { state: state.clone(), subs: subs.clone(), index: id };
      let unsub = source.subscribe(C::lift(observer)).into_boxed();
      if state.rc_deref().observer.is_none() {
        unsub.unsubscribe();
      } else {
        subs.rc_deref_mut().insert(id, unsub);
      }
    }

    SourceWithDynamicSubs::new((), subs)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    cell::RefCell,
    convert::Infallible,
    rc::Rc,
    sync::{Arc, Mutex},
  };

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_fork_join_last_values_in_order() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::fork_join_observables([
      Local::from_iter([1, 2]),
      Local::from_iter([3]),
      Local::from_iter([4, 5, 6]),
    ])
    .on_complete(move || *completed_c.borrow_mut() = true)
    .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![vec![2, 3, 6]]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_fork_join_waits_for_async_sources() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();

    Local::fork_join_observables([a.clone(), b.clone()])
      .subscribe(move |v| result_c.borrow_mut().push(v));

    a.next(1);
    b.next(10);
    a.next(2);
    a.clone().complete();
    assert!(result.borrow().is_empty());

    b.next(20);
    b.complete();
    assert_eq!(*result.borrow(), vec![vec![2, 20]]);
  }

  #[rxrust_macro::test]
  fn test_fork_join_empty_source_completes_without_value() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::fork_join_observables([Local::from_iter([1]), Local::from_iter(std::iter::empty())])
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), Vec::<Vec<i32>>::new());
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_fork_join_empty_list_completes() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    Local::fork_join_observables(std::iter::empty::<Local<Of<i32>>>())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_fork_join_error_unsubscribes_rest() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();
    let mut a = Local::subject::<i32, String>();
    let b = Local::subject::<i32, String>();

    Local::fork_join_observables([a.clone(), b.clone()])
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    a.error("boom".to_string());

    assert_eq!(error.borrow().as_deref(), Some("boom"));
    assert_eq!(b.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_fork_join_shared() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result_c = result.clone();

    Shared::fork_join_observables([Shared::from_iter([1, 2]), Shared::from_iter([3])])
      .subscribe(move |v| result_c.lock().unwrap().push(v));

    assert_eq!(*result.lock().unwrap(), vec![vec![2, 3]]);
  }
}
```

If `a.clone().complete()` on a subject clone is not how the crate completes a subject while keeping a handle, look at how `src/subject/subject_core.rs` tests call `complete` and mirror that.

- [x] **Step 2: Register and add the factory method**

`src/ops.rs`: add `pub mod fork_join;` and `pub use fork_join::*;`.

`src/factory.rs`, after `race_observables`:

```rust
  /// Wait for every observable to complete, then emit their last values.
  ///
  /// Emits one `Vec` of last values in input order and completes. If any
  /// source completes without emitting, completes without a value. Errors
  /// are forwarded immediately. An empty iterator completes immediately.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::fork_join_observables([Local::from_iter([1, 2]), Local::from_iter([3])])
  ///   .subscribe(|v| println!("{:?}", v));
  /// // Prints: [2, 3]
  /// ```
  #[doc(alias = "forkJoin")]
  fn fork_join_observables<O, I>(observables: I) -> Self::With<crate::ops::fork_join::ForkJoin<O>>
  where
    O: ObservableType,
    I: IntoIterator<Item = Self::With<O>>,
  {
    let sources = observables
      .into_iter()
      .map(Context::into_inner)
      .collect();
    Self::lift(crate::ops::fork_join::ForkJoin { sources })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::fork_join && cargo test --doc fork_join`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/fork_join.rs src/ops.rs src/factory.rs
git commit -m "feat(factory.fork_join_observables): add N-ary fork_join factory"
```

---

### Task 13: `combine_latest_observables`

**Files:**
- Create: `src/ops/combine_latest_all.rs`
- Modify: `src/ops.rs`, `src/factory.rs` (after `fork_join_observables`)

**Interfaces:**
- Produces: `ObservableFactory::combine_latest_observables<O, I>(observables: I) -> Self::With<CombineLatestAll<O>>`, emitting `Vec<Item>` snapshots; items must be `Clone`.

- [x] **Step 1: Write the operator file with tests**

```rust
//! CombineLatestAll operator implementation
//!
//! N-ary form of `combine_latest`: emits a snapshot of the latest value from
//! every source whenever any source emits.

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription},
};

/// CombineLatestAll operator: Emits the latest values of every source
///
/// Created with
/// [`crate::factory::ObservableFactory::combine_latest_observables`]. Once
/// every source has emitted at least once, each new item from any source
/// emits a `Vec` with the latest value of every source in input order.
/// Completes when all sources complete, or as soon as a source completes
/// without ever emitting (the result can then never emit). An empty list
/// completes immediately.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::combine_latest_observables([Local::from_iter([1, 2]), Local::from_iter([3])])
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![vec![2, 3]]);
/// ```
#[doc(alias = "combineLatest")]
pub struct CombineLatestAll<O> {
  pub sources: Vec<O>,
}

impl<O: ObservableType> ObservableType for CombineLatestAll<O> {
  type Item<'a>
    = Vec<O::Item<'a>>
  where
    Self: 'a;
  type Err = O::Err;
}

/// State shared by every combine-latest observer
pub struct CombineLatestAllState<Obs, Item> {
  observer: Option<Obs>,
  latest: Vec<Option<Item>>,
  remaining: usize,
}

/// Observer for one indexed source
pub struct CombineLatestAllObserver<StateRc, SubsRc> {
  state: StateRc,
  subs: SubsRc,
  index: usize,
}

impl<Item, Err, Obs, StateRc, SubsRc, U> Observer<Item, Err>
  for CombineLatestAllObserver<StateRc, SubsRc>
where
  Item: Clone,
  Obs: Observer<Vec<Item>, Err>,
  StateRc: RcDerefMut<Target = CombineLatestAllState<Obs, Item>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  U: Subscription,
{
  fn next(&mut self, value: Item) {
    let mut state = self.state.rc_deref_mut();
    if state.observer.is_none() {
      return;
    }
    state.latest[self.index] = Some(value);
    if state.latest.iter().all(Option::is_some) {
      let snapshot: Vec<Item> = state.latest.iter().flatten().cloned().collect();
      if let Some(observer) = state.observer.as_mut() {
        observer.next(snapshot);
      }
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
    self.subs.rc_deref_mut().remove(self.index);
    let mut state = self.state.rc_deref_mut();
    if state.observer.is_none() {
      return;
    }
    let never_emitted = state.latest[self.index].is_none();
    state.remaining -= 1;
    if never_emitted || state.remaining == 0 {
      let observer = state.observer.take();
      drop(state);
      if let Some(observer) = observer {
        observer.complete();
      }
      self.subs.rc_deref_mut().unsubscribe_all();
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

type StateRc<'a, C, O> = <C as Context>::RcMut<
  CombineLatestAllState<<C as Context>::Inner, <O as ObservableType>::Item<'a>>,
>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;

impl<O, C> CoreObservable<C> for CombineLatestAll<O>
where
  C: Context,
  O: ObservableType
    + for<'a> CoreObservable<
      C::With<CombineLatestAllObserver<StateRc<'a, C, O>, SubsRc<C>>>,
      Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
    >,
{
  type Unsub = SourceWithDynamicSubs<(), SubsRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let total = self.sources.len();
    let state: StateRc<C, O> = C::RcMut::from(CombineLatestAllState {
      observer: Some(context.into_inner()),
      latest: (0..total).map(|_| None).collect(),
      remaining: total,
    });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());

    if total == 0 {
      let observer = state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.complete();
      }
      return SourceWithDynamicSubs::new((), subs);
    }

    for source in self.sources {
      if state.rc_deref().observer.is_none() {
        break;
      }
      let id = subs.rc_deref_mut().reserve_id();
      let observer =
        CombineLatestAllObserver { state: state.clone(), subs: subs.clone(), index: id };
      let unsub = source.subscribe(C::lift(observer)).into_boxed();
      if state.rc_deref().observer.is_none() {
        unsub.unsubscribe();
      } else {
        subs.rc_deref_mut().insert(id, unsub);
      }
    }

    SourceWithDynamicSubs::new((), subs)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    cell::RefCell,
    convert::Infallible,
    rc::Rc,
    sync::{Arc, Mutex},
  };

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_combine_latest_observables_interleaved() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();
    let mut c = Local::subject::<i32, Infallible>();

    Local::combine_latest_observables([a.clone(), b.clone(), c.clone()])
      .subscribe(move |v| result_c.borrow_mut().push(v));

    a.next(1);
    b.next(10);
    assert!(result.borrow().is_empty());
    c.next(100);
    a.next(2);
    b.next(20);

    assert_eq!(*result.borrow(), vec![vec![1, 10, 100], vec![2, 10, 100], vec![2, 20, 100]]);
  }

  #[rxrust_macro::test]
  fn test_combine_latest_observables_sync_sources() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    Local::combine_latest_observables([Local::from_iter([1, 2]), Local::from_iter([3, 4])])
      .subscribe(move |v| result_c.borrow_mut().push(v));

    // The first source finishes before the second subscribes
    assert_eq!(*result.borrow(), vec![vec![2, 3], vec![2, 4]]);
  }

  #[rxrust_macro::test]
  fn test_combine_latest_observables_completes_when_all_complete() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();

    Local::combine_latest_observables([a.clone(), b.clone()])
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    a.next(1);
    b.next(2);
    a.complete();
    assert!(!*completed.borrow());
    b.complete();
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_combine_latest_observables_silent_source_completes_early() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    let a = Local::subject::<i32, Infallible>();
    let b = Local::subject::<i32, Infallible>();

    Local::combine_latest_observables([a.clone(), b.clone()])
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    a.clone().complete();

    assert!(*completed.borrow());
    assert_eq!(b.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_combine_latest_observables_empty_list_completes() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    Local::combine_latest_observables(std::iter::empty::<Local<Of<i32>>>())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_combine_latest_observables_shared() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result_c = result.clone();

    Shared::combine_latest_observables([Shared::from_iter([1, 2]), Shared::from_iter([3])])
      .subscribe(move |v| result_c.lock().unwrap().push(v));

    assert_eq!(*result.lock().unwrap(), vec![vec![2, 3]]);
  }
}
```

- [x] **Step 2: Register and add the factory method**

`src/ops.rs`: add `pub mod combine_latest_all;` and `pub use combine_latest_all::*;`.

`src/factory.rs`, after `fork_join_observables`:

```rust
  /// Combine the latest values of many observables.
  ///
  /// Once every source has emitted, each new item emits a `Vec` of the
  /// latest value from every source in input order. Items must be `Clone`.
  /// Completes when all sources complete, or as soon as one completes
  /// without emitting. An empty iterator completes immediately.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::combine_latest_observables([Local::from_iter([1, 2]), Local::from_iter([3])])
  ///   .subscribe(|v| println!("{:?}", v));
  /// // Prints: [2, 3]
  /// ```
  ///
  /// # See Also
  ///
  /// * [`Observable::combine_latest`] - Binary instance method with a combiner
  #[doc(alias = "combineLatest")]
  fn combine_latest_observables<O, I>(
    observables: I,
  ) -> Self::With<crate::ops::combine_latest_all::CombineLatestAll<O>>
  where
    O: ObservableType,
    I: IntoIterator<Item = Self::With<O>>,
  {
    let sources = observables
      .into_iter()
      .map(Context::into_inner)
      .collect();
    Self::lift(crate::ops::combine_latest_all::CombineLatestAll { sources })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::combine_latest_all && cargo test --doc combine_latest_observables`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/combine_latest_all.rs src/ops.rs src/factory.rs
git commit -m "feat(factory.combine_latest_observables): add N-ary combine_latest factory"
```

---

### Task 14: `zip_observables`

**Files:**
- Create: `src/ops/zip_all.rs`
- Modify: `src/ops.rs`, `src/factory.rs` (after `combine_latest_observables`)

**Interfaces:**
- Produces: `ObservableFactory::zip_observables<O, I>(observables: I) -> Self::With<ZipAll<O>>`, emitting `Vec<Item>` rows.

- [x] **Step 1: Write the operator file with tests**

```rust
//! ZipAll operator implementation
//!
//! N-ary form of `zip`: emits the nth item of every source as one `Vec`.

use std::collections::VecDeque;

use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  subscription::{DynamicSubscriptions, IntoBoxedSubscription, SourceWithDynamicSubs, Subscription},
};

/// ZipAll operator: Pairs the nth items of every source
///
/// Created with [`crate::factory::ObservableFactory::zip_observables`].
/// Buffers items per source and emits a `Vec` in input order each time every
/// source has an item waiting. Completes when a completed source has no
/// buffered items left, since no further row can be formed. An empty list
/// completes immediately.
///
/// # Examples
///
/// ```
/// use rxrust::prelude::*;
///
/// let mut result = Vec::new();
/// Local::zip_observables([Local::from_iter([1, 2, 3]), Local::from_iter([10, 20])])
///   .subscribe(|v| result.push(v));
/// assert_eq!(result, vec![vec![1, 10], vec![2, 20]]);
/// ```
#[doc(alias = "zip")]
pub struct ZipAll<O> {
  pub sources: Vec<O>,
}

impl<O: ObservableType> ObservableType for ZipAll<O> {
  type Item<'a>
    = Vec<O::Item<'a>>
  where
    Self: 'a;
  type Err = O::Err;
}

/// State shared by every zip observer
pub struct ZipAllState<Obs, Item> {
  observer: Option<Obs>,
  buffers: Vec<VecDeque<Item>>,
  completed: Vec<bool>,
}

impl<Obs, Item> ZipAllState<Obs, Item> {
  /// A completed source with an empty buffer means no more rows can form.
  fn exhausted(&self) -> bool {
    self
      .buffers
      .iter()
      .zip(&self.completed)
      .any(|(buffer, done)| *done && buffer.is_empty())
  }
}

/// Observer for one indexed source
pub struct ZipAllObserver<StateRc, SubsRc> {
  state: StateRc,
  subs: SubsRc,
  index: usize,
}

impl<Item, Err, Obs, StateRc, SubsRc, U> Observer<Item, Err> for ZipAllObserver<StateRc, SubsRc>
where
  Obs: Observer<Vec<Item>, Err>,
  StateRc: RcDerefMut<Target = ZipAllState<Obs, Item>>,
  SubsRc: RcDerefMut<Target = DynamicSubscriptions<U>>,
  U: Subscription,
{
  fn next(&mut self, value: Item) {
    let mut state = self.state.rc_deref_mut();
    if state.observer.is_none() {
      return;
    }
    state.buffers[self.index].push_back(value);
    if state.buffers.iter().all(|b| !b.is_empty()) {
      let row: Vec<Item> = state
        .buffers
        .iter_mut()
        .map(|b| b.pop_front().expect("checked non-empty"))
        .collect();
      if let Some(observer) = state.observer.as_mut() {
        observer.next(row);
      }
      if state.exhausted() {
        let observer = state.observer.take();
        drop(state);
        if let Some(observer) = observer {
          observer.complete();
        }
        self.subs.rc_deref_mut().unsubscribe_all();
      }
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
    self.subs.rc_deref_mut().remove(self.index);
    let mut state = self.state.rc_deref_mut();
    if state.observer.is_none() {
      return;
    }
    state.completed[self.index] = true;
    if state.exhausted() {
      let observer = state.observer.take();
      drop(state);
      if let Some(observer) = observer {
        observer.complete();
      }
      self.subs.rc_deref_mut().unsubscribe_all();
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

type StateRc<'a, C, O> =
  <C as Context>::RcMut<ZipAllState<<C as Context>::Inner, <O as ObservableType>::Item<'a>>>;
type SubsRc<C> = <C as Context>::RcMut<DynamicSubscriptions<<C as Context>::BoxedSubscription>>;

impl<O, C> CoreObservable<C> for ZipAll<O>
where
  C: Context,
  O: ObservableType
    + for<'a> CoreObservable<
      C::With<ZipAllObserver<StateRc<'a, C, O>, SubsRc<C>>>,
      Unsub: IntoBoxedSubscription<C::BoxedSubscription>,
    >,
{
  type Unsub = SourceWithDynamicSubs<(), SubsRc<C>>;

  fn subscribe(self, context: C) -> Self::Unsub {
    let total = self.sources.len();
    let state: StateRc<C, O> = C::RcMut::from(ZipAllState {
      observer: Some(context.into_inner()),
      buffers: (0..total).map(|_| VecDeque::new()).collect(),
      completed: vec![false; total],
    });
    let subs: SubsRc<C> = C::RcMut::from(DynamicSubscriptions::default());

    if total == 0 {
      let observer = state.rc_deref_mut().observer.take();
      if let Some(observer) = observer {
        observer.complete();
      }
      return SourceWithDynamicSubs::new((), subs);
    }

    for source in self.sources {
      if state.rc_deref().observer.is_none() {
        break;
      }
      let id = subs.rc_deref_mut().reserve_id();
      let observer = ZipAllObserver { state: state.clone(), subs: subs.clone(), index: id };
      let unsub = source.subscribe(C::lift(observer)).into_boxed();
      if state.rc_deref().observer.is_none() {
        unsub.unsubscribe();
      } else {
        subs.rc_deref_mut().insert(id, unsub);
      }
    }

    SourceWithDynamicSubs::new((), subs)
  }
}

#[cfg(test)]
mod tests {
  use std::{
    cell::RefCell,
    convert::Infallible,
    rc::Rc,
    sync::{Arc, Mutex},
  };

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_zip_observables_sync_sources() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let result_c = result.clone();
    let completed_c = completed.clone();

    Local::zip_observables([
      Local::from_iter([1, 2, 3]),
      Local::from_iter([10, 20]),
      Local::from_iter([100, 200, 300]),
    ])
    .on_complete(move || *completed_c.borrow_mut() = true)
    .subscribe(move |v| result_c.borrow_mut().push(v));

    assert_eq!(*result.borrow(), vec![vec![1, 10, 100], vec![2, 20, 200]]);
    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_zip_observables_interleaved() {
    let result = Rc::new(RefCell::new(Vec::new()));
    let result_c = result.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let mut b = Local::subject::<i32, Infallible>();

    Local::zip_observables([a.clone(), b.clone()])
      .subscribe(move |v| result_c.borrow_mut().push(v));

    a.next(1);
    a.next(2);
    assert!(result.borrow().is_empty());
    b.next(10);
    assert_eq!(*result.borrow(), vec![vec![1, 10]]);
    b.next(20);
    assert_eq!(*result.borrow(), vec![vec![1, 10], vec![2, 20]]);
  }

  #[rxrust_macro::test]
  fn test_zip_observables_completes_when_a_source_is_exhausted() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    let mut a = Local::subject::<i32, Infallible>();
    let b = Local::subject::<i32, Infallible>();

    Local::zip_observables([a.clone(), b.clone()])
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    a.next(1);
    a.clone().complete();
    // `a` still has a buffered item, so a row could still form
    assert!(!*completed.borrow());
    assert_eq!(b.inner.subscriber_count(), 1);

    b.clone().next(10);
    // Row emitted, `a` is now exhausted
    assert!(*completed.borrow());
    assert_eq!(b.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_zip_observables_empty_list_completes() {
    let completed = Rc::new(RefCell::new(false));
    let completed_c = completed.clone();

    Local::zip_observables(std::iter::empty::<Local<Of<i32>>>())
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(|_| {});

    assert!(*completed.borrow());
  }

  #[rxrust_macro::test]
  fn test_zip_observables_error_propagation() {
    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();
    let mut a = Local::subject::<i32, String>();
    let b = Local::subject::<i32, String>();

    Local::zip_observables([a.clone(), b.clone()])
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    a.error("boom".to_string());

    assert_eq!(error.borrow().as_deref(), Some("boom"));
    assert_eq!(b.inner.subscriber_count(), 0);
  }

  #[rxrust_macro::test]
  fn test_zip_observables_shared() {
    let result = Arc::new(Mutex::new(Vec::new()));
    let result_c = result.clone();

    Shared::zip_observables([Shared::from_iter([1, 2]), Shared::from_iter([3, 4])])
      .subscribe(move |v| result_c.lock().unwrap().push(v));

    assert_eq!(*result.lock().unwrap(), vec![vec![1, 3], vec![2, 4]]);
  }
}
```

- [x] **Step 2: Register and add the factory method**

`src/ops.rs`: add `pub mod zip_all;` and `pub use zip_all::*;`.

`src/factory.rs`, after `combine_latest_observables`:

```rust
  /// Zip many observables, emitting the nth item of each as one `Vec`.
  ///
  /// Completes as soon as a completed source has no buffered item left,
  /// because no further row can be formed. An empty iterator completes
  /// immediately.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use rxrust::prelude::*;
  ///
  /// Local::zip_observables([Local::from_iter([1, 2, 3]), Local::from_iter([10, 20])])
  ///   .subscribe(|v| println!("{:?}", v));
  /// // Prints: [1, 10], [2, 20]
  /// ```
  ///
  /// # See Also
  ///
  /// * [`Observable::zip`] - Binary instance method emitting tuples
  #[doc(alias = "zip")]
  fn zip_observables<O, I>(observables: I) -> Self::With<crate::ops::zip_all::ZipAll<O>>
  where
    O: ObservableType,
    I: IntoIterator<Item = Self::With<O>>,
  {
    let sources = observables
      .into_iter()
      .map(Context::into_inner)
      .collect();
    Self::lift(crate::ops::zip_all::ZipAll { sources })
  }
```

- [x] **Step 3: Run the tests**

Run: `cargo test --lib ops::zip_all && cargo test --doc zip_observables`
Expected: pass.

- [x] **Step 4: Gate and commit**

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add src/ops/zip_all.rs src/ops.rs src/factory.rs
git commit -m "feat(factory.zip_observables): add N-ary zip factory"
```

---

### Task 15: Documentation, bookkeeping, integration tests

**Files:**
- Modify: `missing_features.md`, `guide/operators.md`, `CHANGELOG.md`, `tests/v1_integration.rs`

- [x] **Step 1: Add two integration tests to `tests/v1_integration.rs`**

Append at the end of the file:

```rust
#[rxrust_macro::test]
fn test_materialize_round_trip_with_race_and_end_with() {
  let result = Rc::new(RefCell::new(Vec::new()));
  let result_clone = result.clone();

  Local::from_iter([1, 2])
    .race(Local::from_iter([9]))
    .end_with(vec![3])
    .materialize()
    .dematerialize()
    .subscribe(move |v| result_clone.borrow_mut().push(v));

  assert_eq!(*result.borrow(), vec![1, 2, 3]);
}

#[rxrust_macro::test]
fn test_fork_join_feeds_every() {
  let result = Rc::new(RefCell::new(Vec::new()));
  let result_clone = result.clone();

  Local::fork_join_observables([
    Local::from_iter([1, 2, 3]),
    Local::from_iter([4, 5]),
  ])
  .map(|last_values| last_values.into_iter().sum::<i32>())
  .every(|sum| *sum == 8)
  .subscribe(move |v| result_clone.borrow_mut().push(v));

  assert_eq!(*result.borrow(), vec![true]);
}
```

Run: `cargo test --test v1_integration`
Expected: pass.

- [x] **Step 2: Correct and extend `missing_features.md`**

Make these edits:

- Under "Creating Observables", no change.
- Under "Transforming Observables", after the `Scan` row add:
  `- [x] Materialize/Dematerialize — represent events as items and back` and remove the `Materialize/Dematerialize` row from the Utility section.
- Under "Filtering Observables": change `ElementAt` to `- [x] ElementAt — emit only item n emitted by an Observable` with sub-bullet `- implemented as element_at / element_at_or`; change `IgnoreElements` to `[x]` with sub-bullet `- implemented as ignore_elements`; change `First` sub-bullet to `- via take(1), first, first_or, find, find_index`.
- Under "Combining Observables": add `- [x] Race/Amb — mirror the first source to emit` with sub-bullet `- race (binary), race_observables (N-ary)`; add `- [x] ForkJoin — emit the last values of all sources once all complete (fork_join_observables)`; extend `CombineLatest` and `Zip` rows with sub-bullets `- N-ary: combine_latest_observables` and `- N-ary: zip_observables`.
- Under "Observable Utility Operators": change `TimeInterval` to `[x]` with `- implemented as time_interval, emits Elapsed { value, interval }`; change `Timestamp` to `[x]` with `- implemented as timestamp, emits Timestamped { value, timestamp }`; remove the `Materialize/Dematerialize` row (moved above).
- Under "Conditional and Boolean Operators": change `All` sub-bullet to `- implemented as every (doc alias all)`; change `Amb` to `[x]` with `- see Race`; add `- [x] IsEmpty — emit whether the source was empty (is_empty)`; add `- [x] ThrowIfEmpty — error instead of completing on an empty source (throw_if_empty)`; add `- [x] EndWith — emit values after the source completes (end_with)`.

- [x] **Step 3: Add rows to `guide/operators.md`**

Filtering table, after `contains`:

```markdown
| `every` | Emits `true` if every item satisfies a predicate, `false` on the first that does not. |
| `is_empty` | Emits `true` if the source completes without items. |
| `find` / `find_index` | Emits the first item (or its index) matching a predicate. |
| `element_at` / `element_at_or` | Emits the item at a zero-based index, with an optional default. |
| `ignore_elements` | Drops every item, mirrors only error and completion. |
```

Combination table, at the end:

```markdown
| `race` / `race_observables` | Mirrors the first source to emit; the others are unsubscribed. |
| `fork_join_observables` | Emits the last value of every source once all complete. |
| `combine_latest_observables` | N-ary `combine_latest` emitting a `Vec` snapshot. |
| `zip_observables` | N-ary `zip` emitting a `Vec` row. |
| `end_with` | Emits given values after the source completes. |
```

Utility table, at the end:

```markdown
| `materialize` / `dematerialize` | Converts events to `Notification` items and back. |
| `timestamp` | Wraps each item with the `Instant` it was emitted. |
| `time_interval` | Wraps each item with the time since the previous emission. |
| `throw_if_empty` | Errors instead of completing when the source is empty. |
```

- [x] **Step 4: Update `CHANGELOG.md`**

Under `## [Unreleased]`, in the `### ✨ New Features` list, add a bullet:

```markdown
*   **RxJS Parity, Tier 1a**: `every`, `ignore_elements`, `is_empty`, `element_at`, `element_at_or`, `find`, `find_index`, `end_with`, `throw_if_empty`, `materialize`, `dematerialize`, `timestamp`, `time_interval`, `race`, and the N-ary factories `race_observables`, `fork_join_observables`, `combine_latest_observables`, `zip_observables`.
```

- [x] **Step 5: Run the doc-driven tests and commit**

Run: `cargo test --doc && cargo test --test v1_integration`
Expected: pass (guide markdown is compiled as doctests, so table edits must not break code fences).

```bash
cargo +nightly fmt --all && cargo +nightly clippy --all-targets --all-features -- -D warnings
git add missing_features.md guide/operators.md CHANGELOG.md tests/v1_integration.rs
git commit -m "docs: record tier 1a operators in guide, changelog and missing_features"
```

---

### Task 16: Full gate and PR

- [x] **Step 1: Run the full matrix**

```bash
cargo test
cargo +nightly test --all-features
cargo +nightly clippy --all-targets --all-features -- -D warnings
cargo +nightly fmt --all -- --check
wasm-pack test --node
```

Expected: every suite green. If `wasm-pack test --node` fails on a pre-existing issue unrelated to the new operators, note it in the PR body rather than fixing it here.

- [x] **Step 2: Push and open the PR**

```bash
git push -u origin feat/operator-parity-tier1
gh pr create --base master --title "feat(ops): RxJS parity tier 1a, utility and filtering operators" --body "$(cat <<'EOF'
## Summary
- Adds `every`, `ignore_elements`, `is_empty`, `element_at`/`element_at_or`, `find`/`find_index`, `end_with`, `throw_if_empty`, `materialize`/`dematerialize`, `timestamp`, `time_interval`, and binary `race`.
- Adds N-ary factories `race_observables`, `fork_join_observables`, `combine_latest_observables`, `zip_observables`.
- Corrects `missing_features.md` rows that claimed `every`/`all`, `ignore_elements`, and `element_at` already existed.
- Design: `docs/superpowers/specs/2026-09-07-operator-parity-tier1-design.md`; plan: `docs/superpowers/plans/2026-09-07-operator-parity-1a.md`.

## Test plan
- [ ] `cargo test` (stable, default features)
- [ ] `cargo +nightly test --all-features`
- [ ] `cargo +nightly clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo +nightly fmt --all -- --check`
- [ ] `wasm-pack test --node`

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01HZvqSeEXabgPF9ygvc6wAf
EOF
)"
```

---

## Self-review

**Spec coverage.** PR 1a section of the spec: `every` (Task 1), `ignore_elements` (2), `element_at`/`element_at_or` (4), `is_empty` (3), `find`/`find_index` (5), `end_with` (6), `materialize`/`dematerialize` (8), `timestamp`/`time_interval` (9), `race`/`race_observables` (10, 11), `fork_join_observables` (12), `combine_latest_observables`/`zip_observables` (13, 14), `throw_if_empty` (7). Docs and bookkeeping (15), integration tests (15), gate (16). One naming deviation from the spec: the `time_interval` value struct is `Elapsed<T>` rather than `TimeInterval<T>` so the operator struct can keep the conventional operator name; the spec is updated to match.

**Placeholders.** None; every step carries its code and command.

**Type consistency.** `Every`, `IgnoreElements`, `IsEmpty`, `ElementAt`, `ElementAtOr`, `Find`, `FindIndex`, `EndWith`, `ThrowIfEmpty`, `Notification`, `Materialize`, `Dematerialize`, `Timestamped`, `Timestamp`, `Elapsed`, `TimeInterval`, `Race`, `RaceAll`, `ForkJoin`, `CombineLatestAll`, `ZipAll` are each defined once and referenced by the same name in `src/observable.rs` / `src/factory.rs` and the docs task. The four N-ary tasks share identical `StateRc`/`SubsRc` alias shapes and the same subscribe loop.
