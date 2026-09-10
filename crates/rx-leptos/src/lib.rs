//! # rx-leptos
//!
//! Bridges between [rxRust](https://docs.rs/rxrust) observables and Leptos
//! 0.8 signals, built on `reactive_graph`, the reactive core that Leptos
//! re-exports.
//!
//! - [`from_signal`] turns a signal (or memo) into an observable that emits the
//!   current value on subscribe and every later change on the next tick of the
//!   app's executor, exactly when a Leptos effect would run. It subscribes to
//!   the reactive graph directly, so it does not need the `effects` feature.
//! - [`to_signal`] and [`use_observable`] turn an observable into a read signal
//!   whose subscription lives exactly as long as the current reactive owner (a
//!   component, an effect, a route).
//! - [`from_event`] (wasm only) turns DOM events on an `EventTarget` into an
//!   observable that removes its listener when unsubscribed.
//! - [`use_subject`] creates a `Subject` that completes when the current
//!   reactive owner is cleaned up; [`use_subscription`] ties any subscription
//!   to the owner the same way.
//! - [`SignalExt`] and [`ObservableExt`] give the bridges method forms:
//!   `query.to_observable().debounce(d).to_signal(String::new())`.
//!
//! Everything uses rxRust's `Local` context: signals are single-threaded UI
//! state, so there is no locking overhead.

pub mod ext;
pub mod from_signal;
pub mod hooks;
pub mod to_signal;

#[cfg(target_arch = "wasm32")]
pub mod from_event;

pub use ext::{ObservableExt, SignalExt};
#[cfg(target_arch = "wasm32")]
pub use from_event::{FromEvent, from_event};
pub use from_signal::{FromSignal, SignalSubscription, from_signal};
pub use hooks::{use_subject, use_subscription};
/// The reactive core this crate is built on, re-exported so downstream code
/// can name the exact version (`leptos::prelude` re-exports the same types).
pub use reactive_graph;
/// The observable library this crate bridges, re-exported for the same reason.
pub use rxrust;
pub use to_signal::{to_signal, to_signal_local, use_observable};

/// Convenient imports: the bridge functions, their method forms, the
/// owner-scoped hooks, and the `reactive_graph` access traits (`Get`, `Set`,
/// ...). These traits are the ones `leptos::prelude` exports, so glob
/// importing both is fine.
pub mod prelude {
  pub use reactive_graph::traits::{Get, GetUntracked, Set, Update, With, WithUntracked};

  #[cfg(target_arch = "wasm32")]
  pub use crate::from_event::{FromEvent, from_event};
  pub use crate::{
    ext::{ObservableExt, SignalExt},
    from_signal::{FromSignal, from_signal},
    hooks::{use_subject, use_subscription},
    to_signal::{to_signal, to_signal_local, use_observable},
  };
}
