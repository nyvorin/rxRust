//! Components and the HTML shell. All reactive wiring comes from
//! [`crate::model`].

use std::time::Duration;

use leptos::prelude::*;
use rxrust::prelude::*;

use crate::model::{Stream, stopwatch, typeahead};

const STYLE: &str = "
body { font-family: system-ui, sans-serif; margin: 2rem auto; max-width: 42rem; line-height: 1.5; }
section { border: 1px solid #ccc; border-radius: 8px; padding: 1rem 1.25rem; margin-bottom: \
                     1.25rem; }
h2 { margin-top: 0; font-size: 1.1rem; }
input { font: inherit; padding: 0.4rem; width: 100%; box-sizing: border-box; }
button { font: inherit; padding: 0.4rem 0.9rem; margin-right: 0.5rem; }
.muted { color: #777; }
code { background: #f4f4f4; padding: 0 0.25rem; }
";

const CRATES: [&str; 8] =
  ["rxrust", "rx-leptos", "reactive_graph", "leptos", "rust", "regex", "rayon", "ring"];

/// A slow, fake search backend: prefix matches after 600 ms.
fn search_crates(query: String) -> Stream<Vec<String>> {
  let hits: Vec<String> = CRATES
    .iter()
    .filter(|name| name.starts_with(query.as_str()))
    .map(|name| name.to_string())
    .collect();
  if query.is_empty() {
    Local::of(hits).box_it()
  } else {
    Local::of(hits)
      .delay(Duration::from_millis(600))
      .box_it()
  }
}

/// The HTML document the server sends; the client hydrates `<App/>` inside.
pub fn shell(options: LeptosOptions) -> impl IntoView {
  view! {
    <!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="utf-8"/>
        <meta name="viewport" content="width=device-width, initial-scale=1"/>
        <title>"rx-leptos · SSR example"</title>
        <AutoReload options=options.clone() />
        <HydrationScripts options/>
        <style>{STYLE}</style>
      </head>
      <body>
        <App/>
      </body>
    </html>
  }
}

#[component]
pub fn App() -> impl IntoView {
  view! {
    <h1>"rx-leptos (server-rendered)"</h1>
    <p class="muted">
      "The server renders the initial state; rx pipelines are wired in the browser inside "
      <code>"Effect::new"</code>". Source: "
      <a href="https://github.com/nyvorin/rxRust/tree/master/examples/leptos-ssr">"examples/leptos-ssr"</a>
    </p>
    <TypeaheadPanel/>
    <StopwatchPanel/>
    <MousePanel/>
  }
}

#[component]
fn TypeaheadPanel() -> impl IntoView {
  let model = typeahead(Duration::from_millis(300), search_crates);
  let query = model.query;
  let results = model.results;
  let pending = model.pending;
  let searches = model.searches;
  let delivered = model.delivered;

  view! {
    <section>
      <h2>"Typeahead: " <code>"debounce → distinct_until_changed → switch_map"</code></h2>
      <input
        type="search"
        placeholder="Search crates (try r, re, rx)"
        prop:value=move || query.get()
        on:input=move |ev| query.set(event_target_value(&ev))
      />
      <p class="muted">
        {move || if pending.get() { "Searching…".to_string() } else {
          format!("{} searches started, {} delivered, {} cancelled",
            searches.get(), delivered.get(), searches.get() - delivered.get())
        }}
      </p>
      <ul>
        {move || results.get().into_iter().map(|name| view! { <li>{name}</li> }).collect_view()}
      </ul>
    </section>
  }
}

#[component]
fn StopwatchPanel() -> impl IntoView {
  let model = stopwatch(Duration::from_millis(100));
  let running = model.running;
  let reset = model.reset;
  let ticks = model.ticks;

  view! {
    <section>
      <h2>"Stopwatch: " <code>"switch_map(interval) → scan"</code></h2>
      <p style="font-size: 2rem; margin: 0.25rem 0;">
        {move || format!("{:.1} s", ticks.get() as f64 / 10.0)}
      </p>
      <button on:click=move |_| running.update(|on| *on = !*on)>
        {move || if running.get() { "Pause" } else { "Start" }}
      </button>
      <button on:click=move |_| reset.update(|n| *n += 1)>"Reset"</button>
    </section>
  }
}

#[component]
fn MousePanel() -> impl IntoView {
  let (position, set_position) = signal((0, 0));
  let section = NodeRef::<leptos::html::Section>::new();

  // Runs in the browser once the element exists; the server renders (0, 0).
  Effect::new(move |_| {
    #[cfg(target_arch = "wasm32")]
    if let Some(element) = section.get() {
      crate::model::wire_mouse(&element, Duration::from_millis(50), set_position);
    }
    #[cfg(not(target_arch = "wasm32"))]
    let _ = (section, set_position);
  });

  view! {
    <section node_ref=section>
      <h2>"Mouse: " <code>"from_event → throttle_time"</code></h2>
      <p>{move || { let (x, y) = position.get(); format!("x = {x}, y = {y} (move the pointer over this box)") }}</p>
    </section>
  }
}
