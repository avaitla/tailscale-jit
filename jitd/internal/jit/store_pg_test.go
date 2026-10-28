package jit

import (
	"context"
	"database/sql"
	"errors"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

// The Postgres tests need a database to create schemas in, for example:
//
//	JITD_TEST_POSTGRES_URL='postgres://jitd@127.0.0.1:5432/jitdtest?sslmode=disable' go test ./jitd/...
//
// Each test gets a schema of its own, dropped afterwards. With
// JITD_TEST_STORE=postgres as well, every service test runs on Postgres
// instead of in memory.

// pgTestURL returns a URL for a new, empty schema, or skips the test.
func pgTestURL(t *testing.T) string {
	t.Helper()
	base := os.Getenv("JITD_TEST_POSTGRES_URL")
	if base == "" {
		t.Skip("JITD_TEST_POSTGRES_URL is not set")
	}
	db, err := sql.Open("pgx", base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	schema := "jitd_test_" + strings.ToLower(NewID(""))
	if _, err := db.Exec("CREATE SCHEMA " + schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Exec("DROP SCHEMA " + schema + " CASCADE") })
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	q := u.Query()
	q.Set("search_path", schema)
	u.RawQuery = q.Encode()
	return u.String()
}

// openTestStore opens an empty store for a test: in memory, or Postgres
// when JITD_TEST_STORE=postgres.
func openTestStore(t *testing.T) *Store {
	t.Helper()
	if os.Getenv("JITD_TEST_STORE") != "postgres" {
		return OpenMemory()
	}
	return openStore(t, pgTestURL(t))
}

func openStore(t *testing.T, dsn string) *Store {
	t.Helper()
	s, err := Open(dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func queryString(t *testing.T, s *Store, q string) string {
	t.Helper()
	var v string
	if err := s.db.QueryRow(q).Scan(&v); err != nil {
		t.Fatalf("%s: %v", q, err)
	}
	return v
}

func TestPostgresStore(t *testing.T) {
	dsn := pgTestURL(t)
	s := openStore(t, dsn)
	if strings.Contains(s.Path, "password") {
		t.Fatalf("Path %q", s.Path)
	}
	mustUpdate(t, s, func(d *Data) error {
		d.Workflows["wf_1"] = &Workflow{ID: "wf_1", Name: "one", MaxDuration: DefaultMaxDuration}
		d.Requests["req_1"] = &AccessRequest{ID: "req_1", WorkflowID: "wf_1", Status: StatusPending, CreatedAt: t0,
			Requester: Identity{Login: "alice@example.com"}}
		d.Requests["req_2"] = &AccessRequest{ID: "req_2", WorkflowID: "wf_1", Status: StatusPending}
		return nil
	})
	if typ := queryString(t, s, `SELECT jsonb_typeof(data) FROM requests WHERE id = 'req_1'`); typ != "object" {
		t.Errorf("data is %s, want a jsonb object", typ)
	}
	if got := queryString(t, s, `SELECT requester || ' ' || to_char(created_at AT TIME ZONE 'UTC', 'YYYY-MM-DD HH24:MI') FROM requests WHERE id = 'req_1'`); got != "alice@example.com 2026-10-01 12:00" {
		t.Errorf("columns: %q", got)
	}
	if s.Changes() != 1 {
		t.Errorf("changes = %d, want 1", s.Changes())
	}

	// Nothing changed: nothing written, and no change for pages to see,
	// although jsonb keeps the JSON in another form than jitd writes it.
	gen := queryString(t, s, `SELECT generation::text FROM jitd_meta`)
	mustUpdate(t, s, func(d *Data) error { d.Requests["req_1"].Status = StatusPending; return nil })
	if g := queryString(t, s, `SELECT generation::text FROM jitd_meta`); g != gen || s.Changes() != 1 {
		t.Errorf("a no-op update wrote: generation %s -> %s, changes %d", gen, g, s.Changes())
	}

	// A quiet update saves but does not count.
	if err := s.UpdateQuiet(func(d *Data) (bool, error) { d.Requests["req_1"].LastError = "bookkeeping"; return false, nil }); err != nil {
		t.Fatal(err)
	}
	if s.Changes() != 1 || !strings.Contains(queryString(t, s, `SELECT data->>'last_error' FROM requests WHERE id = 'req_1'`), "bookkeeping") {
		t.Errorf("quiet update: changes %d", s.Changes())
	}

	// A failed update leaves both the database and the working copy alone.
	if err := s.Update(func(d *Data) error { delete(d.Requests, "req_2"); return errNoChange }); err != nil {
		t.Fatal(err)
	}
	if err := s.Update(func(d *Data) error { delete(d.Requests, "req_2"); return io.EOF }); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v", err)
	}
	s.View(func(d *Data) {
		if d.Requests["req_2"] == nil {
			t.Error("a failed update was kept in the working copy")
		}
	})
	mustUpdate(t, s, func(d *Data) error { delete(d.Requests, "req_2"); return nil })
	if n := queryString(t, s, `SELECT count(*)::text FROM requests`); n != "1" {
		t.Errorf("requests left: %s", n)
	}
	s.Close()

	// Reopening finds everything, and does not migrate again.
	s2 := openStore(t, dsn)
	s2.View(func(d *Data) {
		if d.Workflows["wf_1"].Name != "one" || d.Requests["req_1"].LastError != "bookkeeping" || !d.Requests["req_1"].CreatedAt.Equal(t0) {
			t.Errorf("reloaded data = %+v %+v", d.Workflows, d.Requests)
		}
	})
	ro, err := OpenReadOnly(dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := ro.Update(func(d *Data) error { d.Workflows["wf_1"].Name = "x"; return nil }); err == nil {
		t.Error("wrote through a read-only store")
	}
	ro.Close()

	if _, err := s2.db.Exec(`UPDATE jitd_meta SET schema_version = 99`); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dsn); err == nil || !strings.Contains(err.Error(), "newer jitd") {
		t.Errorf("a newer schema: %v", err)
	}

	// The write itself fails: the working copy goes back, and nothing counts.
	before := s2.changes // Changes would ask the closed database
	s2.db.Close()
	if err := s2.Update(func(d *Data) error { d.Workflows["wf_1"].Name = "changed"; return nil }); err == nil {
		t.Fatal("expected an error from a closed database")
	}
	s2.View(func(d *Data) {
		if d.Workflows["wf_1"].Name != "one" {
			t.Error("a failed write was kept in the working copy")
		}
	})
	if s2.changes != before {
		t.Error("a failed write counted as a change")
	}
}

func TestOpenRefusesOtherURLs(t *testing.T) {
	for _, dsn := range []string{"jit.db", "mysql://x@y/z", ""} {
		if _, err := Open(dsn); err == nil || !strings.Contains(err.Error(), "postgres://") {
			t.Errorf("Open(%q): %v", dsn, err)
		}
	}
	if got := RedactURL("postgres://jitd:secret@db:5432/jitd"); strings.Contains(got, "secret") {
		t.Errorf("RedactURL = %q", got)
	}
}

func TestPostgresRefusesOtherTables(t *testing.T) {
	dsn := pgTestURL(t)
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err := db.Exec(`CREATE TABLE requests (id int)`); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dsn); err == nil || !strings.Contains(err.Error(), "not a jitd database") {
		t.Errorf("err = %v", err)
	}
	if _, err := OpenReadOnly(dsn); err == nil {
		t.Error("read-only open of a database jitd never set up worked")
	}
}

// Two jitds on one database see each other's changes and never lose one.
func TestPostgresSharedStore(t *testing.T) {
	dsn := pgTestURL(t)
	a, b := openStore(t, dsn), openStore(t, dsn)

	mustUpdate(t, a, func(d *Data) error {
		d.Workflows["wf_1"] = &Workflow{ID: "wf_1", Name: "from a"}
		return nil
	})
	changed := b.Changed()
	b.View(func(d *Data) {
		if d.Workflows["wf_1"] == nil || d.Workflows["wf_1"].Name != "from a" {
			t.Errorf("b does not see a's write: %+v", d.Workflows)
		}
	})
	select {
	case <-changed:
	default:
		t.Error("picking up a's write did not signal Changed")
	}
	if a.Changes() != b.Changes() || a.Changes() != 1 {
		t.Errorf("changes: a %d, b %d", a.Changes(), b.Changes())
	}

	// Concurrent read-modify-writes from both: none is lost.
	const n = 20
	var wg sync.WaitGroup
	for i := range 2 * n {
		s := a
		if i%2 == 1 {
			s = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.Update(func(d *Data) error {
				d.Workflows["wf_1"].ApprovalsRequired++
				return nil
			}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	a.View(func(d *Data) {
		if got := d.Workflows["wf_1"].ApprovalsRequired; got != 2*n {
			t.Errorf("after %d increments from two stores: %d", 2*n, got)
		}
	})
}

func TestPostgresLock(t *testing.T) {
	dsn := pgTestURL(t)
	a, b := openStore(t, dsn), openStore(t, dsn)
	unlock, err := a.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	got := make(chan struct{})
	go func() {
		unlockB, err := b.Lock(context.Background())
		if err != nil {
			t.Error(err)
			close(got)
			return
		}
		unlockB()
		close(got)
	}()
	select {
	case <-got:
		t.Fatal("b took the lock while a held it")
	case <-time.After(300 * time.Millisecond):
	}
	unlock()
	select {
	case <-got:
	case <-time.After(10 * time.Second):
		t.Fatal("b did not get the lock after a released it")
	}

	// A cancelled wait gives up.
	unlock, err = a.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if _, err := b.Lock(ctx); err == nil {
		t.Error("b took the lock while a held it")
	}
}

// Two jitds on one database apply the service's rules as one: a device gets
// one open request per flow, and one approval grants once.
func TestPostgresTwoServices(t *testing.T) {
	dsn := pgTestURL(t)
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	newSvc := func() (*Service, *fakeTS) {
		ts := newFakeTS()
		svc := &Service{Store: openStore(t, dsn), TS: ts, Log: log, Now: func() time.Time { return t0 }}
		svc.PolicyCache = &PolicyCache{TS: ts, TTL: time.Minute, Log: log, Now: func() time.Time { return t0 }}
		return svc, ts
	}
	a, tsA := newSvc()
	b, tsB := newSvc()
	w, err := a.CreateWorkflow(rdpFlow(false))
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	results := make([]error, 10)
	for i := range results {
		svc := a
		if i%2 == 1 {
			svc = b
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, results[i] = svc.Submit(context.Background(), alice, NewRequest{WorkflowID: w.ID, Reason: "x"})
		}()
	}
	wg.Wait()
	ok := 0
	for _, err := range results {
		if err == nil {
			ok++
		}
	}
	reqs := b.ListRequests(context.Background(), ListFilter{Viewer: alice, Scope: ScopeMine})
	if ok != 1 || len(reqs) != 1 {
		t.Fatalf("%d of %d concurrent submits succeeded, %d requests stored; want 1", ok, len(results), len(reqs))
	}

	// Bob approves on both at once: the attribute is set once.
	for _, svc := range []*Service{a, b} {
		wg.Add(1)
		go func() {
			defer wg.Done()
			svc.Approve(context.Background(), bob, reqs[0].ID, "")
		}()
	}
	wg.Wait()
	sets := 0
	for _, c := range append(tsA.calls, tsB.calls...) {
		if strings.HasPrefix(c, "SET ") {
			sets++
		}
	}
	r, err := a.GetRequest(context.Background(), alice, reqs[0].ID)
	if err != nil {
		t.Fatal(err)
	}
	if sets != 1 || r.Status != StatusApproved || len(r.Approvals) != 1 {
		t.Errorf("after two concurrent approvals: %d SETs, status %s, %d approvals", sets, r.Status, len(r.Approvals))
	}
}
