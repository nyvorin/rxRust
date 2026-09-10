#[cfg(target_arch = "wasm32")]
fn main() {
  console_error_panic_hook::set_once();
  leptos::mount::mount_to_body(leptos_csr_example::app::App);
}

#[cfg(not(target_arch = "wasm32"))]
fn main() {
  eprintln!(
    "This example runs in the browser: `cd examples/leptos-csr && trunk serve` (see README.md)."
  );
}
