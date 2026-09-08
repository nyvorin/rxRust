//! Observable to signal.

use std::convert::Infallible;

use reactive_graph::{
  owner::{LocalStorage, StoredValue},
  signal::{ReadSignal, WriteSignal, signal, signal_local},
  traits::{IsDisposed, Set},
};
use rxrust::{
  observable::{CoreObservable, Observable},
  observer::Observer,
  subscription::{Subscription, SubscriptionGuard},
};

/// Observer that writes every item into a signal.
pub struct SetSignalObserver<T, St = reactive_graph::owner::SyncStorage> {
  write: WriteSignal<T, St>,
}

impl<T, St> Observer<T, Infallible> for SetSignalObserver<T, St>
where
  WriteSignal<T, St>: Set<Value = T> + IsDisposed,
{
  // A disposed signal (its owner was cleaned up) silently drops the item.
  fn next(&mut self, value: T) { let _ = self.write.try_set(value); }

  fn error(self, never: Infallible) { match never {} }

  fn complete(self) {}

  fn is_closed(&self) -> bool { self.write.is_disposed() }
}

/// Observer that writes every item into an `Option` signal.
pub struct SetSomeObserver<T> {
  write: WriteSignal<Option<T>>,
}

impl<T> Observer<T, Infallible> for SetSomeObserver<T>
where
  WriteSignal<Option<T>>: Set<Value = Option<T>> + IsDisposed,
{
  fn next(&mut self, value: T) { let _ = self.write.try_set(Some(value)); }

  fn error(self, never: Infallible) { match never {} }

  fn complete(self) {}

  fn is_closed(&self) -> bool { self.write.is_disposed() }
}

/// Ties a subscription to the current reactive owner: the guard is stored in
/// the owner's arena and dropped, unsubscribing, when the owner cleans up.
fn keep_until_cleanup<U: Subscription + 'static>(subscription: U) {
  let guard: SubscriptionGuard<U> = subscription.unsubscribe_when_dropped();
  let _stored: StoredValue<SubscriptionGuard<U>, LocalStorage> = StoredValue::new_local(guard);
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
  let subscription = observable.subscribe_with(SetSignalObserver { write });
  keep_until_cleanup(subscription);
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
  let subscription = observable.subscribe_with(SetSignalObserver { write });
  keep_until_cleanup(subscription);
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
  let subscription = observable.subscribe_with(SetSomeObserver { write });
  keep_until_cleanup(subscription);
  read
}
