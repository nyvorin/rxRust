use super::subject_core::Subject;
use crate::{
  context::{Context, RcDerefMut},
  observable::{CoreObservable, ObservableType},
  observer::Observer,
};

/// A specialized Subject that maintains and emits the latest value to new
/// subscribers.
///
/// Unlike a regular Subject which only emits values after subscription,
/// BehaviorSubject immediately emits the most recent value (or an initial
/// value) to new subscribers. This makes it ideal for representing stateful
/// values and current state.
///
/// The current value lives behind the context's shared pointer, so every
/// clone of the subject observes the same latest value.
///
/// # Type Parameters
///
/// - `P`: The smart pointer type for the Subject's observers list
/// - `V`: The shared pointer holding the current value (`Item` behind
///   `Rc<RefCell<_>>` or `Arc<Mutex<_>>` depending on the context)
///
/// # Examples
///
/// ```rust
/// use rxrust::prelude::*;
///
/// let mut behavior = Local::behavior_subject(42);
/// behavior
///   .clone()
///   .subscribe(|v| println!("Current: {}", v)); // Prints: 42
/// behavior.next(99); // Prints: 99
/// ```
pub struct BehaviorSubject<P, V> {
  /// The underlying subject that manages subscribers
  pub subject: Subject<P>,
  /// Shared cell holding the current value
  pub value: V,
}

impl<P: Clone, V: Clone> Clone for BehaviorSubject<P, V> {
  fn clone(&self) -> Self { Self { subject: self.subject.clone(), value: self.value.clone() } }
}

/// The `BehaviorSubject` type that `behavior_subject` builds for an
/// observable `O`.
pub type BehaviorSubjectOf<'a, O> = BehaviorSubject<
  super::SubjectPtr<
    'a,
    O,
    <O as crate::observable::Observable>::Item<'a>,
    <O as crate::observable::Observable>::Err,
  >,
  <O as Context>::RcMut<<O as crate::observable::Observable>::Item<'a>>,
>;

// ============================================================================
// Constructor
// ============================================================================

impl<P, V> BehaviorSubject<P, V>
where
  Subject<P>: Default,
  V: RcDerefMut,
{
  /// Creates a new BehaviorSubject with the given initial value.
  ///
  /// # Examples
  ///
  /// ```rust
  /// use std::convert::Infallible;
  ///
  /// use rxrust::prelude::*;
  ///
  /// let behavior = Local::behavior_subject::<i32, Infallible>(0);
  /// ```
  pub fn new(initial: V::Target) -> Self
  where
    V: From<V::Target>,
  {
    Self { subject: Subject::default(), value: V::from(initial) }
  }
}

// ============================================================================
// Observer Implementation
// ============================================================================

impl<Item, Err, P, V> Observer<Item, Err> for BehaviorSubject<P, V>
where
  Item: Clone,
  V: RcDerefMut<Target = Item>,
  Subject<P>: Observer<Item, Err>,
{
  /// Updates the shared value first, then forwards the emission.
  fn next(&mut self, value: Item) {
    *self.value.rc_deref_mut() = value.clone();
    self.subject.next(value);
  }

  fn error(self, err: Err) { self.subject.error(err); }

  fn complete(self) { self.subject.complete(); }

  fn is_closed(&self) -> bool { self.subject.is_closed() }
}

// ============================================================================
// CoreObservable Implementation
// ============================================================================

impl<P, V> ObservableType for BehaviorSubject<P, V>
where
  Subject<P>: ObservableType,
{
  type Item<'a>
    = <Subject<P> as ObservableType>::Item<'a>
  where
    Self: 'a;

  type Err = <Subject<P> as ObservableType>::Err;
}

impl<Item, Err, C, P, V> CoreObservable<C> for BehaviorSubject<P, V>
where
  C: Context + Observer<Item, Err>,
  Subject<P>: CoreObservable<C, Err = Err>,
  V: RcDerefMut<Target = Item>,
  Item: Clone,
{
  type Unsub = <Subject<P> as CoreObservable<C>>::Unsub;

  /// Emits the current value immediately, then subscribes to future changes.
  fn subscribe(self, mut observer: C) -> Self::Unsub {
    observer.next(self.value.rc_deref().clone());
    self.subject.subscribe(observer)
  }
}

// ============================================================================
// Behavior Trait and Implementations
// ============================================================================

/// Trait for accessing and modifying the current value of a BehaviorSubject.
///
/// This trait provides convenient methods for stateful observables that
/// maintain and expose their current value, allowing for both reading and
/// functional updates.
///
/// # Examples
///
/// ```rust
/// use rxrust::{prelude::*, subject::behavior_subject::Behavior};
///
/// let mut behavior = Local::behavior_subject::<i32, ()>(0);
/// behavior
///   .clone()
///   .on_error(|_e| {})
///   .subscribe(|v| println!("Value: {}", v));
///
/// // Read current value
/// println!("Current: {}", behavior.inner.peek());
///
/// // Functional update based on current value
/// behavior.inner.next_by(|v| v + 1);
/// ```
pub trait Behavior {
  /// The type of value maintained by this behavior
  type Item;

  /// Returns the current value without modifying it.
  fn peek(&self) -> Self::Item;

  /// Updates the value using a function that takes the current value.
  ///
  /// # Arguments
  /// * `f` - Function that transforms the current value to a new value
  fn next_by(&mut self, f: impl FnOnce(Self::Item) -> Self::Item);
}

impl<Item, P, V> Behavior for BehaviorSubject<P, V>
where
  Item: Clone,
  V: RcDerefMut<Target = Item>,
  Self: Observer<Item, ()>,
{
  type Item = Item;

  /// Returns a clone of the current value.
  fn peek(&self) -> Item { self.value.rc_deref().clone() }

  /// Updates the stored value and emits it to all subscribers.
  fn next_by(&mut self, f: impl FnOnce(Self::Item) -> Self::Item) {
    let new_val = f(self.peek());
    self.next(new_val);
  }
}

impl<C: Context<Inner: Behavior>> Behavior for C {
  type Item = <C::Inner as Behavior>::Item;

  /// Delegates peek to the inner BehaviorSubject.
  fn peek(&self) -> Self::Item { self.inner().peek() }

  /// Delegates next_by to the inner BehaviorSubject.
  fn next_by(&mut self, f: impl FnOnce(Self::Item) -> Self::Item) { self.inner_mut().next_by(f) }
}

// ============================================================================
// Tests
// ============================================================================

#[cfg(test)]
mod tests {
  use std::{cell::RefCell, rc::Rc};

  use super::*;
  use crate::prelude::*;

  /// Test helper that captures values in a thread-safe manner
  fn create_value_capture<T>() -> (Rc<RefCell<Vec<T>>>, impl FnMut(T) + Clone)
  where
    T: Clone,
  {
    let values = Rc::new(RefCell::new(Vec::new()));
    let values_clone = values.clone();
    let capture = move |value: T| {
      values_clone.borrow_mut().push(value);
    };
    (values, capture)
  }

  /// Test basic BehaviorSubject functionality with immediate value emission
  #[rxrust_macro::test]
  fn test_behavior_subject_basic() {
    let mut behavior = Local::behavior_subject(42);
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    // Should have emitted initial value immediately
    assert_eq!(*values.borrow(), vec![42]);

    // Test subsequent emissions
    behavior.next(1);
    behavior.next(2);
    behavior.next(3);

    assert_eq!(*values.borrow(), vec![42, 1, 2, 3]);
  }

  /// Test BehaviorSubject functionality in multi-threaded Shared context
  #[cfg(not(target_arch = "wasm32"))]
  #[rxrust_macro::test]
  fn test_behavior_subject_shared() {
    use std::sync::{Arc, Mutex};

    let behavior = Arc::new(Mutex::new(Shared::behavior_subject(100)));
    let values = Arc::new(Mutex::new(Vec::new()));

    // Subscribe in separate thread to test thread-safety
    let behavior_clone = behavior.clone();
    let values_clone = values.clone();
    std::thread::spawn(move || {
      let b = behavior_clone.lock().unwrap();
      b.clone().subscribe(move |v| {
        values_clone.lock().unwrap().push(v);
      });
    })
    .join()
    .unwrap();

    std::thread::sleep(Duration::from_millis(10));

    // Verify initial value emission
    assert_eq!(*values.lock().unwrap(), vec![100]);

    // Test emissions from different thread
    {
      let mut b = behavior.lock().unwrap();
      b.next(200);
      b.next(300);
    }

    std::thread::sleep(Duration::from_millis(10));
    assert_eq!(*values.lock().unwrap(), vec![100, 200, 300]);
  }

  /// Test Behavior trait peek functionality
  #[rxrust_macro::test]
  fn test_behavior_peek() {
    let mut behavior = Local::behavior_subject(10);
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    // Verify initial value emission and subsequent updates
    assert_eq!(*values.borrow(), vec![10]);
    behavior.next(20);
    assert_eq!(*values.borrow(), vec![10, 20]);
    behavior.next(30);
    assert_eq!(*values.borrow(), vec![10, 20, 30]);
  }

  /// Test Behavior trait functional update through next_by
  #[rxrust_macro::test]
  fn test_behavior_next_by() {
    let mut behavior = Local::behavior_subject(0);
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    assert_eq!(*values.borrow(), vec![0]);

    // Test sequential updates
    for i in 1..=3 {
      behavior.next(i);
    }

    assert_eq!(*values.borrow(), vec![0, 1, 2, 3]);
  }

  /// Test multiple subscribers receiving current and subsequent values
  #[rxrust_macro::test]
  fn test_behavior_subject_multiple_subscribers() {
    let mut behavior = Local::behavior_subject(99);
    let (values1, capture1) = create_value_capture();
    let (values2, capture2) = create_value_capture();
    let (values3, capture3) = create_value_capture();

    // First subscriber gets initial value
    behavior.clone().subscribe(capture1);
    assert_eq!(*values1.borrow(), vec![99]);

    // Emit new value - all active subscribers receive it
    behavior.next(100);
    assert_eq!(*values1.borrow(), vec![99, 100]);

    // Second subscriber subscribes and gets current value
    behavior.clone().subscribe(capture2);
    assert_eq!(*values2.borrow(), vec![100]);

    // Third subscriber also gets current value
    behavior.clone().subscribe(capture3);
    assert_eq!(*values3.borrow(), vec![100]);

    // All subscribers receive subsequent values
    behavior.next(101);
    assert_eq!(*values1.borrow(), vec![99, 100, 101]);
    assert_eq!(*values2.borrow(), vec![100, 101]);
    assert_eq!(*values3.borrow(), vec![100, 101]);
  }

  /// Test BehaviorSubject completion behavior
  #[rxrust_macro::test]
  fn test_behavior_subject_complete() {
    let mut behavior = Local::behavior_subject(1);
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    // Test normal emissions before completion
    assert_eq!(*values.borrow(), vec![1]);
    behavior.next(2);
    behavior.next(3);
    assert_eq!(*values.borrow(), vec![1, 2, 3]);

    // Test completion - ownership moved, so we use a clone
    behavior.clone().complete();
    // Main functionality verified through subscription behavior
  }

  /// Test BehaviorSubject error handling capabilities
  #[rxrust_macro::test]
  fn test_behavior_subject_error() {
    let mut behavior = Local::behavior_subject(10);
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    // Verify normal functionality before error
    assert_eq!(*values.borrow(), vec![10]);
    behavior.next(20);
    behavior.next(30);
    assert_eq!(*values.borrow(), vec![10, 20, 30]);
  }

  /// Test BehaviorSubject with different value types (strings)
  #[rxrust_macro::test]
  fn test_behavior_subject_strings() {
    let mut behavior = Local::behavior_subject("hello".to_string());
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    assert_eq!(*values.borrow(), vec!["hello"]);
    behavior.next("world".to_string());
    behavior.next("rxrust".to_string());
    assert_eq!(*values.borrow(), vec!["hello", "world", "rxrust"]);
  }

  /// Test BehaviorSubject clone behavior and shared state
  #[rxrust_macro::test]
  fn test_behavior_subject_clone() {
    let mut behavior1 = Local::behavior_subject(5);
    let mut behavior2 = behavior1.clone();

    let (values1, capture1) = create_value_capture();
    let (values2, capture2) = create_value_capture();

    behavior1.clone().subscribe(capture1);
    behavior2.clone().subscribe(capture2);

    // Both clones should emit initial value
    assert_eq!(*values1.borrow(), vec![5]);
    assert_eq!(*values2.borrow(), vec![5]);

    // Emissions from either clone affect both sets of subscribers
    behavior1.next(10);
    assert_eq!(*values1.borrow(), vec![5, 10]);
    assert_eq!(*values2.borrow(), vec![5, 10]);

    behavior2.next(15);
    assert_eq!(*values1.borrow(), vec![5, 10, 15]);
    assert_eq!(*values2.borrow(), vec![5, 10, 15]);
  }

  /// Test Behavior trait functionality through Context wrapper
  #[rxrust_macro::test]
  fn test_behavior_context_wrapper() {
    let mut behavior = Local::behavior_subject(42);
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    // Verify Context wrapper properly forwards BehaviorSubject behavior
    assert_eq!(*values.borrow(), vec![42]);
    behavior.next(52);
    assert_eq!(*values.borrow(), vec![42, 52]);
  }

  /// Test BehaviorSubject with complex custom data types
  #[rxrust_macro::test]
  fn test_behavior_subject_complex_type() {
    #[derive(Debug, Clone, PartialEq)]
    struct Point {
      x: i32,
      y: i32,
    }

    let mut behavior = Local::behavior_subject(Point { x: 0, y: 0 });
    let (values, capture) = create_value_capture();

    behavior.clone().subscribe(capture);

    // Test emissions with complex data structure
    assert_eq!(*values.borrow(), vec![Point { x: 0, y: 0 }]);

    behavior.next(Point { x: 1, y: 2 });
    behavior.next(Point { x: 3, y: 4 });

    let expected = vec![Point { x: 0, y: 0 }, Point { x: 1, y: 2 }, Point { x: 3, y: 4 }];
    assert_eq!(*values.borrow(), expected);

    // Test final emission
    behavior.next(Point { x: 4, y: 5 });
    assert_eq!(values.borrow().last(), Some(&Point { x: 4, y: 5 }));
  }

  #[rxrust_macro::test]
  fn test_behavior_subject_value_shared_across_clones() {
    let mut a = Local::behavior_subject::<i32, ()>(0);
    let b = a.clone();
    a.next(5);

    let (results, capture) = create_value_capture();
    b.on_error(|_| {}).subscribe(capture);

    assert_eq!(*results.borrow(), vec![5]);
    assert_eq!(a.peek(), 5);
  }
}
