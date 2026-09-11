//! Leptos 0.8 server-rendered example for `rx-leptos`.
//!
//! The server renders each component's initial state; rx pipelines are wired
//! inside `Effect::new` so they exist only in the browser. See `README.md`.

pub mod app;
pub mod model;

#[cfg(feature = "hydrate")]
#[wasm_bindgen::prelude::wasm_bindgen]
pub fn hydrate() {
  console_error_panic_hook::set_once();
  leptos::mount::hydrate_body(app::App);
}
