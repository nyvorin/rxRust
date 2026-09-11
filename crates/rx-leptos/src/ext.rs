//! Method forms of the bridge functions, so pipelines read left to right.

use std::convert::Infallible;

use reactive_graph::{owner::LocalStorage, signal::ReadSignal, traits::Get};
use rxrust::{
  observable::{CoreObservable, Observable},
  prelude::Local,
};

use crate::{
  from_signal::{FromSignal, from_signal},
  to_signal::{SetSignalObserver, SetSomeObserver, to_signal, to_signal_local, use_observable},
};

/// `signal.to_observable()`, the method form of [`from_signal`].
pub trait SignalExt: Get + Sized + 'static {
  /// Mirror this signal as a `Local` observable. See [`FromSignal`].
  fn to_observable(self) -> Local<FromSignal<Self>> { from_signal(self) }
}

impl<S: Get + 'static> SignalExt for S {}

/// `observable.to_signal(initial)` and friends, the method forms of
/// [`to_signal`], [`to_signal_local`] and [`use_observable`].
pub trait ObservableExt: Observable<Err = Infallible> + Sized {
  /// See [`to_signal`].
  fn to_signal<T>(self, initial: T) -> ReadSignal<T>
  where
    T: Send + Sync + 'static,
    Self::Inner: CoreObservable<Self::With<SetSignalObserver<T>>, Unsub: 'static>,
  {
    to_signal(self, initial)
  }

  /// See [`to_signal_local`].
  fn to_signal_local<T>(self, initial: T) -> ReadSignal<T, LocalStorage>
  where
    T: 'static,
    Self::Inner: CoreObservable<Self::With<SetSignalObserver<T, LocalStorage>>, Unsub: 'static>,
  {
    to_signal_local(self, initial)
  }

  /// See [`use_observable`]: `None` until the first item.
  fn to_option_signal<T>(self) -> ReadSignal<Option<T>>
  where
    T: Send + Sync + 'static,
    Self::Inner: CoreObservable<Self::With<SetSomeObserver<T>>, Unsub: 'static>,
  {
    use_observable(self)
  }
}

impl<O: Observable<Err = Infallible>> ObservableExt for O {}
