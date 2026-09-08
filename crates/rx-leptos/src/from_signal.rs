//! Signal to observable.

use std::{
  cell::RefCell,
  convert::Infallible,
  sync::{Arc, RwLock, RwLockWriteGuard, Weak},
};

use any_spawner::Executor;
use reactive_graph::{
  graph::{
    AnySource, AnySubscriber, ReactiveNode, Source, Subscriber, ToAnySubscriber, WithObserver,
  },
  traits::Get,
};
use rxrust::{
  context::Context,
  observable::{CoreObservable, ObservableType},
  observer::Observer,
  prelude::Local,
  subscription::Subscription,
};
use send_wrapper::SendWrapper;

/// An observable that mirrors a signal (or memo, or any other `Get` type).
///
/// Subscribing registers a node in the reactive graph and reads the signal
/// once, so the observer receives the current value synchronously. Every
/// later change is delivered on the next tick of the global `any_spawner`
/// executor, the same scheduling Leptos uses for its effects: writes within
/// one tick coalesce into a single emission of the latest value, and a memo
/// that recomputes to an equal value emits nothing.
///
/// Leptos initialises the executor when it mounts or serves an app. Outside
/// Leptos (plain unit tests, for instance) call
/// `any_spawner::Executor::init_futures_executor()` once and
/// `Executor::poll_local()` to deliver pending changes.
///
/// The observable completes only if the signal is disposed; otherwise
/// unsubscribe to detach it. Values are delivered on the thread that
/// subscribed; the signal must not be written from another thread.
pub struct FromSignal<S> {
  signal: S,
}

impl<S: Clone> Clone for FromSignal<S> {
  fn clone(&self) -> Self { Self { signal: self.signal.clone() } }
}

impl<S: Get> ObservableType for FromSignal<S> {
  type Item<'a>
    = S::Value
  where
    Self: 'a;
  type Err = Infallible;
}

/// Subscription handle returned by [`FromSignal`].
///
/// Like other rxRust subscriptions, dropping the handle does not
/// unsubscribe; call `unsubscribe` or use `unsubscribe_when_dropped`.
pub struct SignalSubscription {
  node: Weak<dyn Detach + Send + Sync>,
}

impl Subscription for SignalSubscription {
  fn unsubscribe(self) {
    if let Some(node) = self.node.upgrade() {
      node.detach();
    }
  }

  fn is_closed(&self) -> bool {
    self
      .node
      .upgrade()
      .is_none_or(|node| node.is_detached())
  }
}

/// Graph bookkeeping. Shared with `reactive_graph`, so it must be `Sync`.
#[derive(Default)]
struct Graph {
  /// A source marked us dirty; the next run must re-read.
  dirty: bool,
  /// A run is queued on the executor.
  scheduled: bool,
  /// A notification arrived while a run was in progress.
  pending: bool,
  /// Unsubscribed, or the signal was disposed.
  closed: bool,
  /// The sources tracked by the last read.
  sources: Vec<AnySource>,
}

/// The single-threaded half: the signal read and the observer fed.
/// Holding the node keeps it alive until it is detached.
struct Bridge<S, O> {
  signal: S,
  observer: O,
  _keep_alive: Arc<Node<S, O>>,
}

struct Node<S, O> {
  weak: Weak<Node<S, O>>,
  graph: RwLock<Graph>,
  /// `None` while a run is in progress and once detached.
  bridge: SendWrapper<RefCell<Option<Bridge<S, O>>>>,
}

trait Detach {
  fn detach(&self);
  fn is_detached(&self) -> bool;
}

impl<S, O> Node<S, O>
where
  S: Get + 'static,
  O: Observer<S::Value, Infallible> + 'static,
{
  fn start(signal: S, observer: O) -> Weak<dyn Detach + Send + Sync> {
    let node = Arc::new_cyclic(|weak| Node {
      weak: weak.clone(),
      graph: RwLock::new(Graph { dirty: true, ..Graph::default() }),
      bridge: SendWrapper::new(RefCell::new(None)),
    });
    *node.bridge.borrow_mut() = Some(Bridge { signal, observer, _keep_alive: node.clone() });
    node.run();
    let weak: Weak<Node<S, O>> = Arc::downgrade(&node);
    weak
  }

  fn graph(&self) -> RwLockWriteGuard<'_, Graph> {
    self
      .graph
      .write()
      .unwrap_or_else(|poisoned| poisoned.into_inner())
  }

  fn any_subscriber(&self) -> AnySubscriber {
    AnySubscriber(self.weak.as_ptr() as usize, self.weak.clone())
  }

  /// Queue a run on the executor, unless one is already queued.
  ///
  /// Sources notify us while they may hold their own locks (a memo notifies
  /// under its read lock), so the re-read must not happen here.
  fn schedule(&self) {
    {
      let mut graph = self.graph();
      if graph.closed || graph.scheduled {
        return;
      }
      graph.scheduled = true;
    }
    let weak = self.weak.clone();
    Executor::spawn_local(async move {
      if let Some(node) = weak.upgrade() {
        node.graph().scheduled = false;
        node.run();
      }
    });
  }

  /// Re-read the signal if the graph says it may have changed, and emit.
  fn run(&self) {
    let Some(mut bridge) = self.bridge.borrow_mut().take() else {
      // Detached, or re-entered from inside `next`.
      self.graph().pending = true;
      return;
    };
    let subscriber = self.any_subscriber();
    if subscriber.with_observer(|| subscriber.update_if_necessary()) {
      self.clear_sources(&subscriber);
      match subscriber.with_observer(|| bridge.signal.try_get()) {
        Some(value) => bridge.observer.next(value),
        None => {
          // The signal was disposed: nothing can ever be emitted again.
          self.graph().closed = true;
          let Bridge { observer, .. } = bridge;
          observer.complete();
          return;
        }
      }
    }
    if bridge.observer.is_closed() || self.graph().closed {
      self.graph().closed = true;
      self.clear_sources(&subscriber);
      return;
    }
    *self.bridge.borrow_mut() = Some(bridge);
    let pending = std::mem::take(&mut self.graph().pending);
    if pending {
      self.schedule();
    }
  }
}

impl<S, O> Detach for Node<S, O>
where
  S: Get + 'static,
  O: Observer<S::Value, Infallible> + 'static,
{
  fn detach(&self) {
    self.graph().closed = true;
    // `None` if detached from inside `next`; that run drops the bridge.
    let bridge = self.bridge.borrow_mut().take();
    self.clear_sources(&self.any_subscriber());
    drop(bridge);
  }

  fn is_detached(&self) -> bool { self.graph().closed }
}

impl<S, O> ReactiveNode for Node<S, O>
where
  S: Get + 'static,
  O: Observer<S::Value, Infallible> + 'static,
{
  fn mark_dirty(&self) {
    self.graph().dirty = true;
    self.schedule();
  }

  fn mark_check(&self) { self.schedule(); }

  fn mark_subscribers_check(&self) {}

  fn update_if_necessary(&self) -> bool {
    let mut graph = self.graph();
    if graph.dirty {
      graph.dirty = false;
      return true;
    }
    let sources = graph.sources.clone();
    drop(graph);
    sources
      .iter()
      .any(|source| source.update_if_necessary())
  }
}

impl<S, O> Subscriber for Node<S, O>
where
  S: Get + 'static,
  O: Observer<S::Value, Infallible> + 'static,
{
  fn add_source(&self, source: AnySource) {
    let mut graph = self.graph();
    if !graph.sources.contains(&source) {
      graph.sources.push(source);
    }
  }

  fn clear_sources(&self, subscriber: &AnySubscriber) {
    let sources = std::mem::take(&mut self.graph().sources);
    for source in sources {
      source.remove_subscriber(subscriber);
    }
  }
}

impl<S, O> ToAnySubscriber for Node<S, O>
where
  S: Get + 'static,
  O: Observer<S::Value, Infallible> + 'static,
{
  fn to_any_subscriber(&self) -> AnySubscriber { self.any_subscriber() }
}

impl<S, C> CoreObservable<C> for FromSignal<S>
where
  C: Context,
  C::Inner: Observer<S::Value, Infallible> + 'static,
  S: Get + 'static,
{
  type Unsub = SignalSubscription;

  fn subscribe(self, context: C) -> Self::Unsub {
    let node = Node::start(self.signal, context.into_inner());
    SignalSubscription { node }
  }
}

/// Mirror a signal as a `Local` observable. See [`FromSignal`].
///
/// # Examples
///
/// ```
/// use std::{cell::RefCell, rc::Rc};
///
/// use any_spawner::Executor;
/// use reactive_graph::{signal::RwSignal, traits::Set};
/// use rx_leptos::from_signal;
/// use rxrust::prelude::*;
///
/// // Leptos does this for you when it mounts the app.
/// let _ = Executor::init_futures_executor();
///
/// let count = RwSignal::new(1);
/// let seen = Rc::new(RefCell::new(Vec::new()));
/// let sink = seen.clone();
/// let sub = from_signal(count).subscribe(move |v| sink.borrow_mut().push(v));
/// count.set(2);
/// Executor::poll_local();
/// sub.unsubscribe();
/// count.set(3);
/// Executor::poll_local();
/// assert_eq!(*seen.borrow(), vec![1, 2]);
/// ```
pub fn from_signal<S>(signal: S) -> Local<FromSignal<S>>
where
  S: Get + 'static,
{
  Local::<()>::lift(FromSignal { signal })
}
