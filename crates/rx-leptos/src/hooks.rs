//! Owner-scoped helpers: things that live exactly as long as the current
//! reactive owner (a component, an effect, a route).

use std::convert::Infallible;

use reactive_graph::owner::{LocalStorage, StoredValue};
use rxrust::{
  observer::Observer,
  prelude::{Local, LocalSubject, ObservableFactory},
  subscription::{Subscription, SubscriptionGuard},
};

/// Tie a subscription to the current reactive owner: it is unsubscribed when
/// the owner is cleaned up.
///
/// Use it for side-effect subscriptions inside a component the way you would
/// use `Effect::new` for signals:
///
/// ```
/// use std::convert::Infallible;
///
/// use reactive_graph::owner::Owner;
/// use rx_leptos::use_subscription;
/// use rxrust::prelude::*;
///
/// let owner = Owner::new();
/// let source = Local::subject::<i32, Infallible>();
/// owner.with(|| use_subscription(source.clone().subscribe(|v| println!("{v}"))));
/// assert_eq!(source.inner.subscriber_count(), 1);
/// owner.cleanup();
/// assert_eq!(source.inner.subscriber_count(), 0);
/// ```
///
/// Outside any owner the subscription is never unsubscribed.
pub fn use_subscription<U: Subscription + 'static>(subscription: U) {
  let guard: SubscriptionGuard<U> = subscription.unsubscribe_when_dropped();
  let _stored: StoredValue<SubscriptionGuard<U>, LocalStorage> = StoredValue::new_local(guard);
}

/// Completes a subject when dropped.
struct CompleteOnDrop<T: Clone + 'static>(Option<LocalSubject<'static, T, Infallible>>);

impl<T: Clone + 'static> Drop for CompleteOnDrop<T> {
  fn drop(&mut self) {
    if let Some(subject) = self.0.take() {
      <LocalSubject<'static, T, Infallible> as Observer<T, Infallible>>::complete(subject);
    }
  }
}

/// A `Subject` owned by the current reactive owner: it completes, releasing
/// its subscribers, when the owner is cleaned up. Items are cloned to each
/// subscriber, as with any rxRust subject.
///
/// Feed it from event handlers and build pipelines on clones of it:
///
/// ```
/// use std::{cell::Cell, rc::Rc};
///
/// use reactive_graph::owner::Owner;
/// use rx_leptos::use_subject;
/// use rxrust::prelude::*;
///
/// let owner = Owner::new();
/// let mut clicks = owner.with(|| use_subject::<u32>());
/// let done = Rc::new(Cell::new(false));
/// let flag = done.clone();
/// clicks
///   .clone()
///   .on_complete(move || flag.set(true))
///   .subscribe(|n| println!("click {n}"));
///
/// clicks.next(1);
/// owner.cleanup();
/// assert!(done.get());
/// ```
pub fn use_subject<T: Clone + 'static>() -> LocalSubject<'static, T, Infallible> {
  let subject = Local::subject::<T, Infallible>();
  let _stored: StoredValue<CompleteOnDrop<T>, LocalStorage> =
    StoredValue::new_local(CompleteOnDrop(Some(subject.clone())));
  subject
}
