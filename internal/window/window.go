// Package window counts distinct members per key over a sliding time window
// and reports when a key reaches a threshold, e.g. "account X gained 50
// distinct followers in 5 minutes".
//
// Time is always the events' own timestamps, never the clock, so replaying a
// backlog behaves the same as live traffic and tests are deterministic. A
// Detector is not safe for concurrent use; the worker calls it from one
// goroutine.
package window

import "time"

// Detector tracks, for each key, which members were seen within the window.
type Detector struct {
	windowUS     int64
	sweepEveryUS int64
	threshold    int

	keys      map[string]*keyState
	watermark int64 // newest event time seen; event-time "now"
	lastSweep int64 // watermark when the last sweep ran
}

type keyState struct {
	// members maps each member to the time of their latest event, so a
	// member counts once however many times they appear.
	members map[string]int64

	// queue holds events oldest first, so expiry only has to look at the
	// front. A member seen again later leaves an older, stale entry behind;
	// expire skips those.
	queue []entry
}

type entry struct {
	member string
	timeUS int64
}

// New returns a Detector that counts members seen within window and fires
// when a key's count reaches threshold. Every sweepEvery of event time it
// drops keys that have gone quiet, to free memory.
func New(window, sweepEvery time.Duration, threshold int) *Detector {
	return &Detector{
		windowUS:     window.Microseconds(),
		sweepEveryUS: sweepEvery.Microseconds(),
		threshold:    threshold,
		keys:         map[string]*keyState{},
	}
}

// Add records that member was seen for key at timeUS (Unix microseconds) and
// returns how many distinct members key has within the window. fire is true
// only when this call brought the count up to exactly the threshold, so each
// burst is reported once. Events already older than the window are ignored.
func (d *Detector) Add(key, member string, timeUS int64) (count int, fire bool) {
	if timeUS > d.watermark {
		d.watermark = timeUS
	}
	if d.lastSweep == 0 {
		d.lastSweep = d.watermark // first event: start sweeping from here
	} else if d.watermark-d.lastSweep >= d.sweepEveryUS {
		d.sweep()
	}

	cutoff := d.watermark - d.windowUS
	if timeUS <= cutoff {
		return 0, false // too late to fall inside the window
	}

	ks := d.keys[key]
	if ks == nil {
		ks = &keyState{members: map[string]int64{}}
		d.keys[key] = ks
	}
	ks.expire(cutoff)

	before := len(ks.members)
	// Only record the event if it's the member's newest. An exact duplicate
	// (e.g. a redelivered message) changes nothing and adds no queue entry.
	if prev, ok := ks.members[member]; !ok || timeUS > prev {
		ks.members[member] = timeUS
		ks.queue = append(ks.queue, entry{member: member, timeUS: timeUS})
	}

	count = len(ks.members)
	// count > before: a repeat member must not re-fire while sitting at the
	// threshold.
	return count, count == d.threshold && count > before
}

// sweep expires every key and deletes the ones left empty. Without it, a key
// that stops receiving events would keep its old entries forever.
func (d *Detector) sweep() {
	cutoff := d.watermark - d.windowUS
	for key, ks := range d.keys {
		ks.expire(cutoff)
		if len(ks.members) == 0 {
			delete(d.keys, key)
		}
	}
	d.lastSweep = d.watermark
}

// expire pops queue entries at or before cutoff. A popped entry removes its
// member only if it is that member's latest event; otherwise the member was
// seen again later and stays.
func (ks *keyState) expire(cutoff int64) {
	i := 0
	for i < len(ks.queue) && ks.queue[i].timeUS <= cutoff {
		e := ks.queue[i]
		if t, ok := ks.members[e.member]; ok && t == e.timeUS {
			delete(ks.members, e.member)
		}
		i++
	}
	ks.queue = ks.queue[i:]
	if len(ks.queue) == 0 {
		ks.queue = nil // release the old backing array
	}
}
