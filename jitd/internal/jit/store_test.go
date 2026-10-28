package jit

import (
	"errors"
	"slices"
	"testing"
)

func mustUpdate(t *testing.T, s *Store, fn func(d *Data) error) {
	t.Helper()
	if err := s.Update(fn); err != nil {
		t.Fatal(err)
	}
}

func TestStoreWritesOnlyChangedRows(t *testing.T) {
	s := OpenMemory()
	mustUpdate(t, s, func(d *Data) error {
		d.Workflows["wf_1"] = &Workflow{ID: "wf_1", Name: "one"}
		d.Workflows["wf_2"] = &Workflow{ID: "wf_2", Name: "two"}
		d.Requests["req_1"] = &AccessRequest{ID: "req_1", WorkflowID: "wf_1", Status: StatusPending}
		d.Requests["req_2"] = &AccessRequest{ID: "req_2", WorkflowID: "wf_1", Status: StatusPending}
		return nil
	})
	if s.Changes() != 1 {
		t.Errorf("changes = %d, want 1", s.Changes())
	}

	// Change one request, delete the other and delete a flow.
	s.mu.Lock()
	s.d.Requests["req_1"].Status = StatusApproved
	delete(s.d.Requests, "req_2")
	delete(s.d.Workflows, "wf_2")
	rows, _ := s.diff()
	s.mu.Unlock()
	var got []string
	for _, r := range rows {
		what := " written"
		if r.data == nil {
			what = " deleted"
		}
		got = append(got, r.key.id+what)
	}
	slices.Sort(got)
	if want := []string{"req_1 written", "req_2 deleted", "wf_2 deleted"}; !slices.Equal(got, want) {
		t.Errorf("diff = %v, want %v", got, want)
	}
	mustUpdate(t, s, func(d *Data) error { return nil })
	if len(s.saved) != 2 {
		t.Errorf("rows left: %d, want 2", len(s.saved))
	}

	// Nothing changed: nothing written, and no change for pages to see.
	before := s.Changes()
	mustUpdate(t, s, func(d *Data) error { d.Requests["req_1"].Status = StatusApproved; return nil })
	mustUpdate(t, s, func(d *Data) error { return errNoChange })
	if s.Changes() != before {
		t.Error("a no-op update counted as a change")
	}
}

func TestStoreUpdateQuiet(t *testing.T) {
	s := OpenMemory()
	mustUpdate(t, s, func(d *Data) error {
		d.Requests["req_1"] = &AccessRequest{ID: "req_1", WorkflowID: "wf_1", Status: StatusApproved}
		return nil
	})
	before := s.Changes()
	changed := s.Changed()
	err := s.UpdateQuiet(func(d *Data) (bool, error) {
		d.Requests["req_1"].LastError = "bookkeeping"
		return false, nil
	})
	if err != nil || s.Changes() != before {
		t.Errorf("quiet update: err %v, changes %d -> %d", err, before, s.Changes())
	}
	select {
	case <-changed:
		t.Error("a quiet update signalled Changed")
	default:
	}
	s.View(func(d *Data) {
		if d.Requests["req_1"].LastError != "bookkeeping" {
			t.Error("quiet update was not kept")
		}
	})
	err = s.UpdateQuiet(func(d *Data) (bool, error) {
		d.Requests["req_1"].Status = StatusRevoked
		return true, nil
	})
	if err != nil || s.Changes() != before+1 {
		t.Errorf("a visible quiet update: err %v, changes %d -> %d", err, before, s.Changes())
	}
	select {
	case <-changed:
	default:
		t.Error("a visible update did not signal Changed")
	}
}

func TestStoreUpdateErrorRollsBack(t *testing.T) {
	s := OpenMemory()
	mustUpdate(t, s, func(d *Data) error {
		d.Workflows["wf_1"] = &Workflow{ID: "wf_1", Name: "one"}
		return nil
	})
	unchanged := func(when string) {
		t.Helper()
		s.View(func(d *Data) {
			if len(d.Workflows) != 1 || d.Workflows["wf_1"].Name != "one" || len(d.Requests) != 0 {
				t.Errorf("%s: the update was kept: %+v %+v", when, d.Workflows, d.Requests)
			}
		})
	}
	change := func(d *Data) {
		d.Workflows["wf_1"].Name = "changed"
		d.Workflows["wf_2"] = &Workflow{ID: "wf_2"}
	}
	before := s.Changes()

	boom := errors.New("boom")
	if err := s.Update(func(d *Data) error { change(d); return boom }); !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	unchanged("after an error")

	func() {
		defer func() { recover() }()
		s.Update(func(d *Data) error { change(d); panic("boom") })
	}()
	unchanged("after a panic")

	if err := s.Update(func(d *Data) error { d.Requests["req_x"] = nil; return nil }); err == nil {
		t.Error("a nil request was accepted")
	}
	unchanged("after a nil request")
	if s.Changes() != before {
		t.Error("a failed update counted as a change")
	}
}
