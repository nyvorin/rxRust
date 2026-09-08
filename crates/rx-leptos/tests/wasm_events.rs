//! DOM event bridging, run under `wasm-pack test --node` where `EventTarget`
//! and `Event` exist as globals.
#![cfg(target_arch = "wasm32")]

use std::{cell::RefCell, rc::Rc};

use rx_leptos::prelude::*;
use rxrust::prelude::*;
use wasm_bindgen_test::wasm_bindgen_test;
use web_sys::{Event, EventTarget};

#[wasm_bindgen_test]
fn from_event_delivers_events_until_unsubscribed() {
  let target = EventTarget::new().unwrap();
  let seen = Rc::new(RefCell::new(0));
  let sink = seen.clone();

  let sub = from_event(&target, "ping").subscribe(move |_| *sink.borrow_mut() += 1);
  target
    .dispatch_event(&Event::new("ping").unwrap())
    .unwrap();
  target
    .dispatch_event(&Event::new("pong").unwrap())
    .unwrap();
  assert_eq!(*seen.borrow(), 1);

  sub.unsubscribe();
  target
    .dispatch_event(&Event::new("ping").unwrap())
    .unwrap();
  assert_eq!(*seen.borrow(), 1, "listener removed on unsubscribe");
}
