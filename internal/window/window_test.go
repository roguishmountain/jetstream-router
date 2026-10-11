package window

import (
	"testing"
	"time"
)

// Times are in microseconds, like Jetstream's time_us. t0 is an arbitrary
// non-zero start time.
const (
	t0     = int64(1_000_000_000_000)
	sec    = int64(time.Second / time.Microsecond)
	minute = 60 * sec
)

// add calls Add and fails the test if the result isn't what's expected.
func add(t *testing.T, d *Detector, key, member string, at int64, wantCount int, wantFire bool) {
	t.Helper()
	count, fire := d.Add(key, member, at)
	if count != wantCount || fire != wantFire {
		t.Fatalf("Add(%q, %q, t0%+ds) = (%d, %v), want (%d, %v)",
			key, member, (at-t0)/sec, count, fire, wantCount, wantFire)
	}
}

func TestCountsUniqueMembers(t *testing.T) {
	d := New(10*time.Minute, 30*time.Second, 5)
	add(t, d, "X", "A", t0, 1, false)
	add(t, d, "X", "A", t0+1*sec, 1, false)
	add(t, d, "X", "B", t0+2*sec, 2, false)
}

func TestFiresOnceAtThreshold(t *testing.T) {
	d := New(10*time.Minute, 30*time.Second, 2)
	add(t, d, "X", "A", t0, 1, false)
	add(t, d, "X", "B", t0+1*sec, 2, true)
	add(t, d, "X", "B", t0+2*sec, 2, false) // repeat member at the threshold
	add(t, d, "X", "C", t0+3*sec, 3, false) // past the threshold
}

func TestOldEventsExpireAndCanFireAgain(t *testing.T) {
	d := New(10*time.Minute, 30*time.Second, 2)
	add(t, d, "X", "A", t0, 1, false)
	add(t, d, "X", "B", t0+1*minute, 2, true)

	// 12 minutes later A and B are outside the window.
	add(t, d, "X", "C", t0+12*minute, 1, false)
	add(t, d, "X", "D", t0+12*minute+1*sec, 2, true)
}

func TestRepeatMemberStaysUntilTheirLatestEventExpires(t *testing.T) {
	d := New(10*time.Minute, time.Hour, 10)
	add(t, d, "X", "A", t0, 1, false)
	add(t, d, "X", "A", t0+5*minute, 1, false)

	// At 11 minutes A's first event has expired but the one at 5 minutes
	// hasn't, so A still counts.
	add(t, d, "X", "B", t0+11*minute, 2, false)

	// At 16 minutes A's latest event has expired too.
	add(t, d, "X", "C", t0+16*minute, 2, false) // B and C
}

func TestLateEventsAreIgnored(t *testing.T) {
	d := New(10*time.Minute, time.Hour, 2)
	add(t, d, "Y", "A", t0+20*minute, 1, false)

	// An event from 20 minutes before the newest one is outside the window.
	add(t, d, "X", "B", t0, 0, false)
	if keys, _ := size(d); keys != 1 {
		t.Errorf("late event created a key: got %d keys, want 1", keys)
	}
}

func TestKeysAreIndependent(t *testing.T) {
	d := New(10*time.Minute, 30*time.Second, 2)
	add(t, d, "X", "A", t0, 1, false)
	add(t, d, "Y", "B", t0+1*sec, 1, false)
	add(t, d, "X", "C", t0+2*sec, 2, true)
}

func TestSweepRemovesQuietKeys(t *testing.T) {
	d := New(10*time.Minute, 30*time.Second, 5)
	add(t, d, "X", "A", t0, 1, false)

	// 11 minutes later X has had no new events. Adding to Y triggers a
	// sweep, which finds X empty and deletes it.
	add(t, d, "Y", "B", t0+11*minute, 1, false)
	if keys, entries := size(d); keys != 1 || entries != 1 {
		t.Errorf("size() = (%d keys, %d entries), want (1, 1)", keys, entries)
	}
}

func TestQuietKeysStayUntilSweep(t *testing.T) {
	// Same as above, but the sweep isn't due yet, so X is still held.
	d := New(10*time.Minute, time.Hour, 5)
	add(t, d, "X", "A", t0, 1, false)
	add(t, d, "Y", "B", t0+11*minute, 1, false)
	if keys, _ := size(d); keys != 2 {
		t.Errorf("got %d keys, want 2", keys)
	}
}

// size reports how many keys and queue entries d holds, so tests can check
// that expiry and the sweep free memory.
func size(d *Detector) (keys, entries int) {
	for _, ks := range d.keys {
		entries += len(ks.queue)
	}
	return len(d.keys), entries
}
