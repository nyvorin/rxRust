//! ReplaySubject: replays a bounded history to late subscribers.

use std::collections::VecDeque;

use super::subject_core::Subject;
use crate::{
  context::{Context, RcDeref, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// A recorded terminal event.
#[derive(Debug, Clone)]
pub enum Terminal<Err> {
  /// The subject errored
  Error(Err),
  /// The subject completed
  Complete,
}

/// Shared replay state.
pub struct ReplayBuffer<Item, Err> {
  items: VecDeque<Item>,
  capacity: Option<usize>,
  terminal: Option<Terminal<Err>>,
}

impl<Item, Err> ReplayBuffer<Item, Err> {
  fn new(capacity: Option<usize>) -> Self {
    Self { items: VecDeque::new(), capacity, terminal: None }
  }

  fn push(&mut self, item: Item) {
    if self.capacity == Some(0) {
      return;
    }
    self.items.push_back(item);
    if let Some(cap) = self.capacity {
      while self.items.len() > cap {
        self.items.pop_front();
      }
    }
  }
}

/// A Subject that buffers the last `capacity` items (or all of them) and
/// replays them, plus any terminal event, to every new subscriber.
///
/// # Examples
///
/// ```rust
/// use std::{cell::RefCell, rc::Rc};
///
/// use rxrust::prelude::*;
///
/// let mut subject = Local::replay_subject::<i32, std::convert::Infallible>(2);
/// subject.next(1);
/// subject.next(2);
/// subject.next(3);
///
/// let seen = Rc::new(RefCell::new(Vec::new()));
/// let sink = seen.clone();
/// subject
///   .clone()
///   .subscribe(move |v| sink.borrow_mut().push(v));
/// assert_eq!(*seen.borrow(), vec![2, 3]);
/// ```
pub struct ReplaySubject<P, B> {
  /// The underlying subject that manages live subscribers
  pub subject: Subject<P>,
  buffer: B,
}

impl<P: Clone, B: Clone> Clone for ReplaySubject<P, B> {
  fn clone(&self) -> Self { Self { subject: self.subject.clone(), buffer: self.buffer.clone() } }
}

impl<P, B, Item, Err> ReplaySubject<P, B>
where
  Subject<P>: Default,
  B: RcDerefMut<Target = ReplayBuffer<Item, Err>> + From<ReplayBuffer<Item, Err>>,
{
  /// Creates a subject that replays up to `capacity` items, or every item
  /// when `capacity` is `None`.
  pub fn new(capacity: Option<usize>) -> Self {
    Self { subject: Subject::default(), buffer: B::from(ReplayBuffer::new(capacity)) }
  }
}

impl<P, B, Item, Err> ReplaySubject<P, B>
where
  B: RcDeref<Target = ReplayBuffer<Item, Err>>,
{
  /// Whether a terminal event has been recorded.
  pub fn is_terminated(&self) -> bool { self.buffer.rc_deref().terminal.is_some() }
}

impl<Item, Err, P, B> Observer<Item, Err> for ReplaySubject<P, B>
where
  Item: Clone,
  Err: Clone,
  B: RcDerefMut<Target = ReplayBuffer<Item, Err>>,
  Subject<P>: Observer<Item, Err>,
{
  fn next(&mut self, value: Item) {
    {
      let mut buffer = self.buffer.rc_deref_mut();
      if buffer.terminal.is_some() {
        return;
      }
      buffer.push(value.clone());
    }
    self.subject.next(value);
  }

  fn error(self, err: Err) {
    {
      let mut buffer = self.buffer.rc_deref_mut();
      if buffer.terminal.is_some() {
        return;
      }
      buffer.terminal = Some(Terminal::Error(err.clone()));
    }
    self.subject.error(err);
  }

  fn complete(self) {
    {
      let mut buffer = self.buffer.rc_deref_mut();
      if buffer.terminal.is_some() {
        return;
      }
      buffer.terminal = Some(Terminal::Complete);
    }
    self.subject.complete();
  }

  fn is_closed(&self) -> bool { self.buffer.rc_deref().terminal.is_some() }
}

impl<P, B> ObservableType for ReplaySubject<P, B>
where
  Subject<P>: ObservableType,
{
  type Item<'a>
    = <Subject<P> as ObservableType>::Item<'a>
  where
    Self: 'a;
  type Err = <Subject<P> as ObservableType>::Err;
}

impl<Item, Err, C, P, B> CoreObservable<C> for ReplaySubject<P, B>
where
  C: Context + Observer<Item, Err>,
  Subject<P>: CoreObservable<C, Err = Err>,
  B: RcDerefMut<Target = ReplayBuffer<Item, Err>>,
  Item: Clone,
  Err: Clone,
{
  type Unsub = Option<<Subject<P> as CoreObservable<C>>::Unsub>;

  fn subscribe(self, mut observer: C) -> Self::Unsub {
    let (items, terminal) = {
      let buffer = self.buffer.rc_deref();
      (buffer.items.iter().cloned().collect::<Vec<_>>(), buffer.terminal.clone())
    };
    for item in items {
      if observer.is_closed() {
        return None;
      }
      observer.next(item);
    }
    match terminal {
      Some(Terminal::Error(err)) => {
        observer.error(err);
        None
      }
      Some(Terminal::Complete) => {
        observer.complete();
        None
      }
      None => Some(self.subject.subscribe(observer)),
    }
  }
}

/// The `ReplaySubject` type that `replay_subject` builds for an observable
/// `O`.
pub type ReplaySubjectOf<'a, O> = ReplaySubject<
  super::SubjectPtr<
    'a,
    O,
    <O as crate::observable::Observable>::Item<'a>,
    <O as crate::observable::Observable>::Err,
  >,
  <O as Context>::RcMut<
    ReplayBuffer<
      <O as crate::observable::Observable>::Item<'a>,
      <O as crate::observable::Observable>::Err,
    >,
  >,
>;

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, convert::Infallible, rc::Rc};

  use crate::prelude::*;

  #[rxrust_macro::test]
  fn test_replay_bounded_history_to_late_subscriber() {
    let mut subject = Local::replay_subject::<i32, Infallible>(2);
    subject.next(1);
    subject.next(2);
    subject.next(3);

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_c = seen.clone();
    subject
      .clone()
      .subscribe(move |v| seen_c.borrow_mut().push(v));
    subject.next(4);

    assert_eq!(*seen.borrow(), vec![2, 3, 4]);
  }

  #[rxrust_macro::test]
  fn test_replay_unbounded_history() {
    let mut subject = Local::replay_subject_unbounded::<i32, Infallible>();
    for i in 0..5 {
      subject.next(i);
    }

    let seen = Rc::new(RefCell::new(Vec::new()));
    let seen_c = seen.clone();
    subject
      .clone()
      .subscribe(move |v| seen_c.borrow_mut().push(v));

    assert_eq!(*seen.borrow(), vec![0, 1, 2, 3, 4]);
  }

  #[rxrust_macro::test]
  fn test_replay_completion_to_late_subscriber() {
    let mut subject = Local::replay_subject::<i32, Infallible>(1);
    subject.next(7);
    subject.clone().complete();

    let seen = Rc::new(RefCell::new(Vec::new()));
    let completed = Rc::new(RefCell::new(false));
    let seen_c = seen.clone();
    let completed_c = completed.clone();
    let sub = subject
      .clone()
      .on_complete(move || *completed_c.borrow_mut() = true)
      .subscribe(move |v| seen_c.borrow_mut().push(v));

    assert_eq!(*seen.borrow(), vec![7]);
    assert!(*completed.borrow());
    assert!(sub.is_closed());
    // Events after termination are ignored
    subject.next(8);
    assert_eq!(*seen.borrow(), vec![7]);
  }

  #[rxrust_macro::test]
  fn test_replay_error_to_late_subscriber() {
    let subject = Local::replay_subject::<i32, String>(1);
    subject.clone().error("boom".to_string());

    let error = Rc::new(RefCell::new(None));
    let error_c = error.clone();
    subject
      .clone()
      .on_error(move |e| *error_c.borrow_mut() = Some(e))
      .subscribe(|_| {});

    assert_eq!(error.borrow().as_deref(), Some("boom"));
  }

  #[rxrust_macro::test]
  fn test_replay_shared_context() {
    use std::sync::{Arc, Mutex};

    let mut subject = Shared::replay_subject::<i32, Infallible>(3);
    subject.next(1);
    subject.next(2);

    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_c = seen.clone();
    subject
      .clone()
      .subscribe(move |v| seen_c.lock().unwrap().push(v));

    assert_eq!(*seen.lock().unwrap(), vec![1, 2]);
  }
}
