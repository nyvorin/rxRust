//! # Operators
//!
//! All operator implementations for transforming, filtering, and combining
//! observable streams.
//!
//! ## Operator Categories
//!
//! ### Transformation
//! - [`map`] - Transform each item
//! - [`filter_map`] - Transform and filter in one step
//! - [`scan`] - Accumulate with intermediate emissions
//! - [`scan_map`] - Transform each item while accumulating a value
//!
//! ### Filtering
//! - [`filter`] - Pass items matching a predicate
//! - [`take`] / [`skip`] - Limit emissions
//! - [`distinct`] - Remove duplicates
//!
//! ### Combination
//! - [`merge`] - Interleave multiple streams
//! - [`zip`] - Pair items from multiple streams
//! - [`combine_latest`] - Combine latest values
//!
//! ### Timing
//! - [`debounce`] - Emit after quiet period
//! - [`throttle`] - Rate limit emissions
//! - [`delay`] - Shift emissions in time
//!
//! For interactive marble diagrams, see [ReactiveX.io](http://reactivex.io/documentation/operators.html).
//! For Rust-specific usage, see the [Operators Guide](https://rxrust.github.io/rxRust/operators.html).

pub mod audit;
pub mod average;
pub mod box_it;
pub mod buffer;
pub mod buffer_count;
pub mod buffer_time;
pub mod catch_error;
pub mod collect;
pub mod combine_latest;
pub mod combine_latest_all;
pub mod contains;
pub mod debounce;
pub mod default_if_empty;
pub mod delay;
pub mod distinct;
pub mod distinct_until_changed;
pub mod element_at;
pub mod end_with;
pub mod every;
pub mod exhaust_map;
pub mod filter;
pub mod filter_map;
pub mod finalize;
pub mod find;
pub mod flat_map;
pub mod fork_join;
pub mod group_by;
pub mod ignore_elements;
pub mod into_future;
pub mod into_stream;
pub mod is_empty;
pub mod last;
pub mod lifecycle;
pub mod map;
pub mod map_err;
pub mod map_to;
pub mod materialize;
pub mod merge;
pub mod merge_all;
pub mod observe_on;
pub mod pairwise;
pub mod race;
pub mod race_all;
pub mod reduce;
pub mod ref_count;
pub mod repeat;
pub mod retry;
pub mod sample;
pub mod scan;
pub mod scan_map;
pub mod skip;
pub mod skip_last;
pub mod skip_until;
pub mod skip_while;
pub mod start_with;
pub mod subscribe_on;
pub mod switch_map;
pub mod take;
pub mod take_last;
pub mod take_until;
pub mod take_while;
pub mod tap;
pub mod throttle;
pub mod throw_if_empty;
pub mod time_interval;
pub mod timeout;
pub mod timestamp;
pub mod with_latest_from;
pub mod zip;
pub mod zip_all;

// Re-exports
pub use audit::*;
pub use average::*;
pub use box_it::*;
pub use buffer::*;
pub use buffer_count::*;
pub use buffer_time::*;
pub use catch_error::*;
pub use collect::*;
pub use combine_latest::*;
pub use combine_latest_all::*;
pub use contains::*;
pub use debounce::*;
pub use default_if_empty::*;
pub use delay::*;
pub use distinct::*;
pub use distinct_until_changed::*;
pub use element_at::*;
pub use end_with::*;
pub use every::*;
pub use exhaust_map::*;
pub use filter::*;
pub use filter_map::*;
pub use finalize::*;
pub use find::*;
pub use flat_map::*;
pub use fork_join::*;
pub use group_by::*;
pub use ignore_elements::*;
pub use into_future::*;
pub use into_stream::*;
pub use is_empty::*;
pub use last::*;
pub use lifecycle::*;
pub use map::*;
pub use map_err::*;
pub use map_to::*;
pub use materialize::*;
pub use merge::*;
pub use merge_all::*;
pub use observe_on::*;
pub use pairwise::*;
pub use race::*;
pub use race_all::*;
pub use reduce::*;
pub use ref_count::*;
pub use repeat::*;
pub use retry::*;
pub use sample::*;
pub use scan::*;
pub use scan_map::*;
pub use skip::*;
pub use skip_last::*;
pub use skip_until::*;
pub use skip_while::*;
pub use start_with::*;
pub use subscribe_on::*;
pub use switch_map::*;
pub use take::*;
pub use take_last::*;
pub use take_until::*;
pub use take_while::*;
pub use tap::*;
pub use throttle::*;
pub use throw_if_empty::*;
pub use time_interval::*;
pub use timeout::*;
pub use timestamp::*;
pub use with_latest_from::*;
pub use zip::*;
pub use zip_all::*;

#[cfg(test)]
mod aggregation_tests;
