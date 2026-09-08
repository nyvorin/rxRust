//! DOM events to observable (wasm only).

use std::convert::Infallible;

use rxrust::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  prelude::Local,
  subscription::Subscription,
};
use wasm_bindgen::{JsCast, closure::Closure};
use web_sys::{Event, EventTarget};

/// An observable of DOM events of one name on one target.
#[derive(Clone)]
pub struct FromEvent {
  target: EventTarget,
  name: String,
}

impl ObservableType for FromEvent {
  type Item<'a>
    = Event
  where
    Self: 'a;
  type Err = Infallible;
}

/// Subscription handle: unsubscribing removes the listener.
pub struct EventListenerSubscription {
  target: EventTarget,
  name: String,
  listener: Option<Closure<dyn FnMut(Event)>>,
}

impl Subscription for EventListenerSubscription {
  fn unsubscribe(mut self) {
    if let Some(listener) = self.listener.take() {
      let _ = self
        .target
        .remove_event_listener_with_callback(&self.name, listener.as_ref().unchecked_ref());
    }
  }

  fn is_closed(&self) -> bool { self.listener.is_none() }
}

impl<C> CoreObservable<C> for FromEvent
where
  C: Context,
  C::Inner: Observer<Event, Infallible> + 'static,
{
  type Unsub = EventListenerSubscription;

  fn subscribe(self, context: C) -> Self::Unsub {
    let mut observer = context.into_inner();
    let listener = Closure::wrap(Box::new(move |event: Event| {
      if !observer.is_closed() {
        observer.next(event);
      }
    }) as Box<dyn FnMut(Event)>);
    let _ = self
      .target
      .add_event_listener_with_callback(&self.name, listener.as_ref().unchecked_ref());
    EventListenerSubscription { target: self.target, name: self.name, listener: Some(listener) }
  }
}

/// Observe DOM events named `name` on `target`.
pub fn from_event(target: &EventTarget, name: &str) -> Local<FromEvent> {
  Local::<()>::lift(FromEvent { target: target.clone(), name: name.to_string() })
}
