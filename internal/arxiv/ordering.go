package arxiv

import "time"

// After returns true if this entry is strictly after the (lastTime, lastID) composite cursor.
// If cursor is empty (no prior row), all entries are considered new.
func (e Entry) After(lastTime *time.Time, lastID *string) bool {
	if lastTime == nil || lastID == nil || *lastID == "" {
		return true
	}
	if e.Updated.After(*lastTime) {
		return true
	}
	if e.Updated.Before(*lastTime) {
		return false
	}
	return e.ArxivID > *lastID
}

// AdvanceComposite picks the max (Updated, id) in a list (non-empty) for the cursor.
func MaxComposite(entries []Entry) (t time.Time, id string) {
	if len(entries) == 0 {
		return
	}
	best := entries[0]
	for _, e := range entries[1:] {
		if e.Updated.After(best.Updated) {
			best = e
			continue
		}
		if e.Updated.Equal(best.Updated) && e.ArxivID > best.ArxivID {
			best = e
		}
	}
	return best.Updated, best.ArxivID
}
