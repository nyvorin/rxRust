//! Observable to signal.

use std::convert::Infallible;

use reactive_graph::{
  owner::{LocalStorage, Owner},
  signal::{ReadSignal, WriteSignal, signal, signal_local},
  traits::{IsDisposed, Set},
};
use rxrust::{
  observable::{CoreObservable, Observable},
  observer::Observer,
};

use crate::hooks::use_subscription;

/// Run `f` under `owner`, if there is one.
///
/// Items often arrive from executor tasks or timers with no reactive owner
/// on the stack. Under Leptos's server features (`sandboxed-arenas`) signal
/// access outside an owner panics, so every write re-enters the owner that
/// was current when the subscription was made.
pub(crate) fn with_owner<R>(owner: &Option<Owner>, f: impl FnOnce() -> R) -> R {
  match owner {
    Some(owner) => owner.with(f),
    None => f(),
  }
}

/// Observer that writes every item into a signal.
pub struct SetSignalObserver<T, St = reactive_graph::owner::SyncStorage> {
  write: WriteSignal<T, St>,
  owner: Option<Owner>,
}

impl<T, St> Observer<T, Infallible> for SetSignalObserver<T, St>
where
  WriteSignal<T, St>: Set<Value = T> + IsDisposed,
{
  // A disposed signal (its owner was cleaned up) silently drops the item.
  fn next(&mut self, value: T) {
    let write = &self.write;
    with_owner(&self.owner, || {
      let _ = write.try_set(value);
    });
  }

  fn error(self, never: Infallible) { match never {} }

  fn complete(self) {}

  fn is_closed(&self) -> bool { with_owner(&self.owner, || self.write.is_disposed()) }
}

/// Observer that writes every item into an `Option` signal.
pub struct SetSomeObserver<T> {
  write: WriteSignal<Option<T>>,
  owner: Option<Owner>,
}

impl<T> Observer<T, Infallible> for SetSomeObserver<T>
where
  WriteSignal<Option<T>>: Set<Value = Option<T>> + IsDisposed,
{
  fn next(&mut self, value: T) {
    let write = &self.write;
    with_owner(&self.owner, || {
      let _ = write.try_set(Some(value));
    });
  }

  fn error(self, never: Infallible) { match never {} }

  fn complete(self) {}

  fn is_closed(&self) -> bool { with_owner(&self.owner, || self.write.is_disposed()) }
}

/// Subscribe `observable` and write every item into `write`, for as long as
/// the current reactive owner lives.
///
/// This is the building block for server-rendered apps: create the signal
/// at component level (so the server renders its initial value) and call
/// `feed_signal` inside `Effect::new`, which only runs in the browser.
///
/// ```
/// use std::convert::Infallible;
///
/// use reactive_graph::{owner::Owner, signal::signal, traits::GetUntracked};
/// use rx_leptos::feed_signal;
/// use rxrust::prelude::*;
///
/// let owner = Owner::new();
/// let mut source = Local::subject::<i32, Infallible>();
/// let (count, set_count) = signal(0);
/// owner.with(|| feed_signal(source.clone(), set_count));
/// source.next(7);
/// assert_eq!(count.get_untracked(), 7);
/// owner.cleanup();
/// assert_eq!(source.inner.subscriber_count(), 0);
/// ```
pub fn feed_signal<O, T, St>(observable: O, write: WriteSignal<T, St>)
where
  O: Observable<Err = Infallible>,
  O::Inner: CoreObservable<O::With<SetSignalObserver<T, St>>, Unsub: 'static>,
  WriteSignal<T, St>: Set<Value = T> + IsDisposed,
{
  let owner = Owner::current();
  use_subscription(observable.subscribe_with(SetSignalObserver { write, owner }));
}

/// Turn an observable into a read signal that starts at `initial`.
///
/// The signal and the subscription both belong to the reactive owner that
/// called this function: when that owner is cleaned up the observable is
/// unsubscribed and the signal is disposed, so read it only while the owner
/// lives. Items must be `Send + Sync`; use [`to_signal_local`] otherwise.
///
/// # Examples
///
/// ```
/// use std::convert::Infallible;
///
/// use reactive_graph::{owner::Owner, traits::GetUntracked};
/// use rx_leptos::to_signal;
/// use rxrust::prelude::*;
///
/// let owner = Owner::new();
/// let mut source = Local::subject::<i32, Infallible>();
/// let count = owner.with(|| to_signal(source.clone(), 0));
/// source.next(7);
/// assert_eq!(count.get_untracked(), 7);
/// owner.cleanup();
/// assert_eq!(source.inner.subscriber_count(), 0);
/// ```
pub fn to_signal<O, T>(observable: O, initial: T) -> ReadSignal<T>
where
  T: Send + Sync + 'static,
  O: Observable<Err = Infallible>,
  O::Inner: CoreObservable<O::With<SetSignalObserver<T>>, Unsub: 'static>,
{
  let (read, write) = signal(initial);
  feed_signal(observable, write);
  read
}

/// [`to_signal`] for items that are not `Send + Sync`.
pub fn to_signal_local<O, T>(observable: O, initial: T) -> ReadSignal<T, LocalStorage>
where
  T: 'static,
  O: Observable<Err = Infallible>,
  O::Inner: CoreObservable<O::With<SetSignalObserver<T, LocalStorage>>, Unsub: 'static>,
{
  let (read, write) = signal_local(initial);
  feed_signal(observable, write);
  read
}

/// Turn an observable into a `ReadSignal<Option<T>>` that is `None` until
/// the first item arrives. Cleanup follows the current owner like
/// [`to_signal`].
pub fn use_observable<O, T>(observable: O) -> ReadSignal<Option<T>>
where
  T: Send + Sync + 'static,
  O: Observable<Err = Infallible>,
  O::Inner: CoreObservable<O::With<SetSomeObserver<T>>, Unsub: 'static>,
{
  let (read, write) = signal(None);
  let owner = Owner::current();
  use_subscription(observable.subscribe_with(SetSomeObserver { write, owner }));
  read
}
