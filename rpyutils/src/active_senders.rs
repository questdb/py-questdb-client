/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2025 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/

use std::{
    collections::VecDeque, ffi::c_int, fmt::Debug, ops::Sub, sync::{LazyLock, Mutex}, time::{Duration, Instant}
};

type Slot = u32;

struct Slots {
    /// Next available slot ID in the linear range.
    next_slot: Slot,

    /// I.e. "holes" in the range `0..self.next_slot`.
    returned: VecDeque<Slot>,
}

impl Slots {
    fn new() -> Self {
        Self {
            next_slot: 0,
            returned: VecDeque::new(),
        }
    }

    fn next(&mut self) -> Slot {
        if let Some(returned) = self.returned.pop_front() {
            returned
        } else {
            let slot = self.next_slot;
            self.next_slot += 1;
            slot
        }
    }

    fn restore(&mut self, slot_id: Slot) {
        if slot_id == self.next_slot - 1 {
            self.next_slot -= 1;
            while let Some(&last) = self.returned.back() {
                if last == self.next_slot - 1 {
                    self.returned.pop_back();
                    self.next_slot -= 1;
                } else {
                    break;
                }
            }
        } else {
            self.returned.push_back(slot_id);
            self.returned.make_contiguous().sort_unstable();
        }
    }
}

#[cfg(test)]
impl Debug for Slots {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Slots")
            .field("next_slot", &self.next_slot)
            .field("returned", &self.returned)
            .finish()
    }
}

trait InstantLike: PartialEq + PartialOrd + Copy + Sub<std::time::Duration> + Debug {
    fn now() -> Self;
    fn duration_since(&self, earlier: Self) -> Duration;
}

impl InstantLike for Instant {
    fn now() -> Self {
        Instant::now()
    }

    fn duration_since(&self, earlier: Self) -> Duration {
        self.duration_since(earlier)
    }
}

struct ActiveSenders<InstantType: InstantLike = Instant> {
    slots: Slots,

    /// Tracked established connection events.
    /// Keys are slot IDs, which are always non-negative integers.
    /// Values are `VecDeque<u64>` containing established connection `Instant` timestamps.
    series: std::collections::HashMap<Slot, std::collections::VecDeque<InstantType>>,

    /// Timestamp of last issued warning
    last_warning: Option<InstantType>,

    /// Window for counting recent reconnections.
    reconnect_warn_window: Duration,

    /// Threshold for issuing a warning when the number of recent reconnections exceeds this value.
    reconnect_warn_threshold: usize,

    /// Window to suppress warnings after the last warning.
    quiet_window: Duration,
}

#[cfg(test)]
impl<I: InstantLike> Debug for ActiveSenders<I> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut series = self.series.iter().collect::<Vec<_>>();
        series.sort_by(|(k1, _v1), (k2, _v2)| k1.cmp(k2));
        f.debug_struct("ActiveSenders")
            .field("slots", &self.slots)
            .field("series", &series)
            .field("last_warning", &self.last_warning)
            .finish()
    }
}

impl<I: InstantLike<Output = I>> ActiveSenders<I> {
    fn new(
        reconnect_warn_window: Duration,
        reconnect_warn_threshold: usize,
        quiet_window: Duration,
    ) -> Self {
        Self {
            slots: Slots::new(),
            series: std::collections::HashMap::new(),
            last_warning: None,
            reconnect_warn_window,
            reconnect_warn_threshold,
            quiet_window,
        }
    }

    fn count_recent_reconnections(&mut self) -> usize {
        let now = I::now();
        let cutoff: I = now - self.reconnect_warn_window;
        let mut max_count = 0;
        let mut to_delete = Vec::new();

        for (&slot_id, serie) in &mut self.series {
            while let Some(&established) = serie.front() {
                if established < cutoff {
                    serie.pop_front();
                } else {
                    break;
                }
            }
            let count = serie.len();
            if count == 0 {
                to_delete.push(slot_id);
            } else if count > max_count {
                max_count = count;
            }
        }

        for slot_id in to_delete {
            self.series.remove(&slot_id);
        }

        max_count
    }

    fn track_established(&mut self) -> (Slot, bool) {
        let slot_id = self.slots.next();
        let serie = self
            .series
            .entry(slot_id)
            .or_insert_with(|| VecDeque::with_capacity(2 * self.reconnect_warn_threshold));
        serie.push_back(I::now());

        let max_recent_reconnections = self.count_recent_reconnections();

        let mut warning = false;

        if max_recent_reconnections >= self.reconnect_warn_threshold {
            let now = I::now();
            if self.last_warning.is_none()
                || now.duration_since(self.last_warning.unwrap()) > self.quiet_window
            {
                warning = true;
                self.last_warning = Some(now);
            }
        }
        (slot_id, warning)
    }

    fn track_closed(&mut self, slot_id: Slot) {
        self.slots.restore(slot_id);
    }
}

static ACTIVE_SENDERS: LazyLock<Mutex<ActiveSenders>> = LazyLock::new(|| {
    Mutex::new(ActiveSenders::new(
        Duration::from_secs(5),
        25, // reconnections
        Duration::from_secs(10 * 60),
    ))
});

#[no_mangle]
pub extern "C" fn qdb_active_senders_track_established(warn: *mut c_int) -> Slot {
    let mut active_senders = ACTIVE_SENDERS.lock().unwrap();
    let (slot_id, warning) = active_senders.track_established();
    unsafe {
        *warn = warning as c_int;
    }
    slot_id
}

#[no_mangle]
pub extern "C" fn qdb_active_senders_track_closed(slot_id: Slot) {
    let mut active_senders = ACTIVE_SENDERS.lock().unwrap();
    active_senders.track_closed(slot_id);
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;
    use rand::seq::SliceRandom;

    fn assert_slots_state(slots: &Slots, next_id: Slot, returned: &[Slot]) {
        assert_eq!(slots.next_slot, next_id);
        assert_eq!(slots.returned.len(), returned.len());
        for (i, &slot) in returned.iter().enumerate() {
            assert_eq!(slots.returned[i], slot);
        }
    }

    /// Test the slots, last-out-first-in usage pattern.
    #[test]
    fn test_slots_lofi() {
        let mut slots = Slots::new();
        assert_slots_state(&slots, 0, &[]);
        assert_eq!(slots.next(), 0);
        assert_slots_state(&slots, 1, &[]);
        assert_eq!(slots.next(), 1);
        assert_slots_state(&slots, 2, &[]);
        assert_eq!(slots.next(), 2);
        assert_slots_state(&slots, 3, &[]);
        assert_eq!(slots.next(), 3);
        assert_slots_state(&slots, 4, &[]);
        slots.restore(3);
        assert_slots_state(&slots, 3, &[]);
        slots.restore(2);
        assert_slots_state(&slots, 2, &[]);
        slots.restore(1);
        assert_slots_state(&slots, 1, &[]);
        slots.restore(0);
        assert_slots_state(&slots, 0, &[]);
    }

    /// Test the slots, last-out-last-in usage pattern.
    #[test]
    fn test_slots_loli() {
        let mut slots = Slots::new();
        assert_eq!(slots.next(), 0);
        assert_eq!(slots.next(), 1);
        assert_eq!(slots.next(), 2);
        assert_eq!(slots.next(), 3);

        slots.restore(0);
        assert_slots_state(&slots, 4, &[0]);
        slots.restore(1);
        assert_slots_state(&slots, 4, &[0, 1]);
        slots.restore(2);
        assert_slots_state(&slots, 4, &[0, 1, 2]);
        slots.restore(3);
        assert_slots_state(&slots, 0, &[]);
    }

    /// Tests the slots in twos.
    #[test]
    fn test_slot_gaps() {
        let mut slots = Slots::new();

        assert_eq!(slots.next(), 0);
        assert_eq!(slots.next(), 1);
        assert_slots_state(&slots, 2, &[]);
        slots.restore(0);
        assert_slots_state(&slots, 2, &[0]);

        assert_eq!(slots.next(), 0);
        assert_eq!(slots.next(), 2);
        assert_eq!(slots.next(), 3);
        assert_eq!(slots.next(), 4);
        assert_slots_state(&slots, 5, &[]);
        slots.restore(1);
        assert_slots_state(&slots, 5, &[1]);
        slots.restore(3);
        assert_slots_state(&slots, 5, &[1, 3]); // gap in the returned sequence

        slots.restore(4);
        assert_slots_state(&slots, 3, &[1]);

        slots.restore(2);
        assert_slots_state(&slots, 1, &[]);

        slots.restore(0);
    }

    #[test]
    fn test_slots_random() {
        for _ in 0..100 {
            let mut slots = Slots::new();

            let mut acquired = (0..50).map(|_| slots.next()).collect::<Vec<_>>();
            assert_slots_state(&slots, 50, &[]);
            assert_eq!(acquired.len(), 50);

            let mut rng = rand::rng();
            acquired.shuffle(&mut rng);

            for &slot in &acquired {
                slots.restore(slot);
            }
            assert_slots_state(&slots, 0, &[]);
        }
    }

    thread_local! {
        // Storing time as milliseconds
        static NEXT_MOCK_INSTANT_VALUE: Cell<u64> = const { Cell::new(0) };
    }

    fn reset_mock_instant() {
        // We initialize with a large enough value to avoid subtraction underflow
        // issues where `ActiveSenders` needs to calculate a duration in the past.
        NEXT_MOCK_INSTANT_VALUE.set(1000000000u64);
    }

    fn advance_mock_instant(time: Duration) {
        NEXT_MOCK_INSTANT_VALUE.with(|v| {
            let new_value = v.get() + time.as_millis() as u64;
            v.set(new_value);
        });
    }

    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    struct MockInstant {
        millis: u64,
    }

    impl Debug for MockInstant {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}ms", self.millis)
        }
    }

    impl Sub<Duration> for MockInstant {
        type Output = Self;

        fn sub(self, rhs: Duration) -> Self::Output {
            MockInstant {
                millis: self.millis - rhs.as_millis() as u64,
            }
        }
    }

    impl InstantLike for MockInstant {
        fn now() -> Self {
            MockInstant {
                millis: NEXT_MOCK_INSTANT_VALUE.get(),
            }
        }

        fn duration_since(&self, earlier: Self) -> Duration {
            Duration::from_millis(self.millis - earlier.millis)
        }
    }

    #[test]
    fn test_active_senders_4_independent() {
        // We connect 4 independent senders in a 300ms window.
        // This will not trigger a warning.

        reset_mock_instant();
        let mut active_senders =
            ActiveSenders::<MockInstant>::new(Duration::from_secs(5), 3, Duration::from_secs(60));
        assert_eq!(active_senders.track_established(), (0, false));

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (1, false));

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (2, false));

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (3, false));

        active_senders.track_closed(1);
        active_senders.track_closed(2);

        advance_mock_instant(Duration::from_millis(100)); // first reconnection, no trigger
        assert_eq!(active_senders.track_established(), (1, false));

        active_senders.track_closed(3);
        active_senders.track_closed(4);
        active_senders.track_closed(1);
        active_senders.track_closed(2);
    }

    #[test]
    fn test_active_senders_fast_reconnect() {
        reset_mock_instant();
        let mut active_senders =
            ActiveSenders::<MockInstant>::new(Duration::from_secs(5), 3, Duration::from_secs(60));

        assert_eq!(active_senders.track_established(), (0, false));
        active_senders.track_closed(0);

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (0, false));
        active_senders.track_closed(0);

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (0, true)); // warn, 3rd reconnect within 5s
        active_senders.track_closed(0);

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (0, false)); // suppress warning
        active_senders.track_closed(0);

        advance_mock_instant(active_senders.quiet_window);

        assert_eq!(active_senders.track_established(), (0, false));

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (1, false)); // new slot ID should not affect logic!

        active_senders.track_closed(0);

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (0, false));
        active_senders.track_closed(0);

        advance_mock_instant(Duration::from_millis(100));
        assert_eq!(active_senders.track_established(), (0, true)); // warn, 3rd reconnect within 5s
    }

    #[test]
    fn test_active_senders_slow_reconnect() {
        reset_mock_instant();
        let mut active_senders =
            ActiveSenders::<MockInstant>::new(Duration::from_secs(5), 3, Duration::from_secs(60));

        // Ten times: Two reconnects, then a big pause.
        for _ in 0..10 {
            assert_eq!(active_senders.track_established(), (0, false));
            active_senders.track_closed(0);

            advance_mock_instant(Duration::from_millis(100));
            assert_eq!(active_senders.track_established(), (0, false));
            active_senders.track_closed(0);

            advance_mock_instant(active_senders.reconnect_warn_window);
        }
    }
}
