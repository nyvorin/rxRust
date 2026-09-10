#[cfg(feature = "ssr")]
#[tokio::main]
async fn main() {
  use axum::{Router, routing::get};
  use leptos::prelude::*;
  use leptos_ssr_example::app::shell;

  // `leptos_routes` would do this; with a hand-written route it is on us.
  let _ = any_spawner::Executor::init_tokio();

  let conf = get_configuration(None).expect("Cargo.toml [package.metadata.leptos]");
  let addr = conf.leptos_options.site_addr;
  let leptos_options = conf.leptos_options;

  let app = Router::new()
    .route(
      "/",
      get(leptos_axum::render_app_to_stream({
        let options = leptos_options.clone();
        move || shell(options.clone())
      })),
    )
    .fallback(leptos_axum::file_and_error_handler(shell))
    .with_state(leptos_options);

  println!("listening on http://{addr}");
  let listener = tokio::net::TcpListener::bind(&addr)
    .await
    .expect("bind");
  axum::serve(listener, app.into_make_service())
    .await
    .expect("serve");
}

#[cfg(not(feature = "ssr"))]
fn main() {}
