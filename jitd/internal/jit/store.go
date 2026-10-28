package jit

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite" // pure Go, so jitd still builds with CGO_ENABLED=0
)

// Data is everything jitd persists.
type Data struct {
	Workflows map[string]*Workflow      `json:"workflows"`
	Requests  map[string]*AccessRequest `json:"requests"`
}

// schemaVersion is kept in PRAGMA user_version. To change the tables, add a
// step to migrate and bump it; never edit schemaV1 or an existing step.
const schemaVersion = 2

const schemaV1 = `
CREATE TABLE workflows (
	id   TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	data TEXT NOT NULL -- the Workflow as JSON
);
CREATE TABLE requests (
	id          TEXT PRIMARY KEY,
	workflow_id TEXT NOT NULL,
	requester   TEXT NOT NULL,
	status      TEXT NOT NULL,
	created_at  TEXT NOT NULL,
	data        TEXT NOT NULL -- the AccessRequest as JSON
);
-- jitd reads everything at start; these are for people with the sqlite3 command line.
CREATE INDEX requests_status ON requests(status);
CREATE INDEX requests_workflow ON requests(workflow_id);
CREATE INDEX requests_created ON requests(created_at);
`

// timeColumn formats created_at with a fixed width, so the column sorts in
// time order as text (RFC 3339 with trimmed fractions does not).
func timeColumn(t time.Time) string {
	return t.UTC().Format("2006-01-02T15:04:05.000000000Z")
}

// errNoChange, returned by an Update function that changed nothing, skips
// comparing every row with the database. Update then returns nil.
var errNoChange = errors.New("no change")

// Store keeps approval flows and access requests in a SQLite or Postgres
// database, with a working copy in memory. Each row holds the object as JSON,
// plus a few columns for ad-hoc queries. An Update writes only the rows that
// changed, in one transaction.
//
// With SQLite, jitd must be the only writer: it does not see changes made to
// the database by anything else while it runs. With Postgres, every read
// first checks whether another jitd changed the database and reloads if so,
// and every write locks the database first (see store_postgres.go).
type Store struct {
	db      *sql.DB
	pg      bool // Postgres; else SQLite
	mu      sync.Mutex
	d       Data
	saved   map[rowKey][]byte // each row's JSON as it is in the database
	changes uint64
	gen     int64         // Postgres: the generation the working copy was loaded at
	notify  chan struct{} // closed at the next counted change; see Changed
	broken  error         // set if the working copy could not be put back after a failed update

	// Path is the database file, or the Postgres URL without its password.
	Path string
	// Mode is the database file's permissions when it already existed (jitd
	// creates it 0600), so jitd can warn if others can read it.
	Mode fs.FileMode
	// Log reports a failed reload from Postgres; nil discards it.
	Log *slog.Logger
}

type rowKey struct {
	table string // "workflows" or "requests"
	id    string
}

// Open opens the SQLite database at path, creating it if needed. A
// postgres:// or postgresql:// URL opens that Postgres database instead.
func Open(path string) (*Store, error) {
	if IsPostgres(path) {
		return openPostgres(path, false)
	}
	if fi, err := os.Stat(path); err == nil {
		s, err := openDB(path, false)
		if s != nil {
			s.Mode = fi.Mode().Perm()
		}
		return s, err
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, err
	}
	if err := createPrivate(path); err != nil {
		return nil, err
	}
	s, err := openDB(path, false)
	if err != nil {
		removeDB(path)
	}
	return s, err
}

// OpenReadOnly opens an existing database without changing it.
func OpenReadOnly(path string) (*Store, error) {
	if IsPostgres(path) {
		return openPostgres(path, true)
	}
	if _, err := os.Stat(path); err != nil {
		return nil, err
	}
	return openDB(path, true)
}

func openDB(path string, readOnly bool) (*Store, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, err
	}
	// A file: URI, so the path is escaped: '?', '#' and '%' in it must not
	// be read as URI syntax.
	q := "_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=synchronous(FULL)&_txlock=immediate"
	if readOnly {
		q = "mode=ro&_pragma=busy_timeout(5000)"
	}
	dsn := (&url.URL{Scheme: "file", Path: filepath.ToSlash(abs)}).String() + "?" + q
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// jitd is the only user, and Store.mu orders every access anyway.
	db.SetMaxOpenConns(1)
	s := &Store{db: db, Path: path}
	if readOnly {
		err = s.checkVersion()
	} else {
		err = s.migrate()
	}
	if err == nil {
		err = s.load()
	}
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	return s, nil
}

func (s *Store) version() (int, error) {
	var v int
	err := s.db.QueryRow("PRAGMA user_version").Scan(&v)
	return v, err
}

func (s *Store) checkVersion() error {
	v, err := s.version()
	if err == nil && v != schemaVersion {
		err = fmt.Errorf("the database is at schema %d; this jitd needs %d (start jitd once to upgrade it)", v, schemaVersion)
	}
	return err
}

// migrate brings the database to schemaVersion, in one transaction.
func (s *Store) migrate() error {
	v, err := s.version()
	if err != nil {
		return err
	}
	switch {
	case v == schemaVersion:
		return nil
	case v > schemaVersion:
		return fmt.Errorf("the database was written by a newer jitd (schema %d; this jitd knows up to %d): run the newer jitd, or restore a backup", v, schemaVersion)
	}
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if v < 1 {
		var n int
		if err := tx.QueryRow("SELECT count(*) FROM sqlite_master").Scan(&n); err != nil {
			return err
		}
		if n > 0 {
			return errors.New("this is not a jitd database (it has tables of its own)")
		}
		if _, err := tx.Exec(schemaV1); err != nil {
			return err
		}
	}
	if v < 2 {
		if err := migrateV2(tx); err != nil {
			return fmt.Errorf("upgrading to schema 2: %w", err)
		}
	}
	if _, err := tx.Exec(fmt.Sprintf("PRAGMA user_version = %d", schemaVersion)); err != nil {
		return err
	}
	return tx.Commit()
}

// migrateV2 stores data as text rather than as a blob, and created_at with a
// fixed width (see timeColumn).
func migrateV2(tx *sql.Tx) error {
	if _, err := tx.Exec(`UPDATE workflows SET data = CAST(data AS TEXT)`); err != nil {
		return err
	}
	rows, err := tx.Query(`SELECT id, data FROM requests`)
	if err != nil {
		return err
	}
	created := map[string]string{}
	for rows.Next() {
		var id string
		var b []byte
		if err := rows.Scan(&id, &b); err != nil {
			rows.Close()
			return err
		}
		var r AccessRequest
		if err := json.Unmarshal(b, &r); err != nil {
			rows.Close()
			return fmt.Errorf("request %s: %w", id, err)
		}
		created[id] = timeColumn(r.CreatedAt)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return err
	}
	for id, c := range created {
		if _, err := tx.Exec(`UPDATE requests SET data = CAST(data AS TEXT), created_at = ? WHERE id = ?`, c, id); err != nil {
			return err
		}
	}
	return nil
}

// querier is a database or a transaction.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// load reads every row into the working copy.
func (s *Store) load() error {
	return s.loadFrom(context.Background(), s.db)
}

// loadFrom reads every row through q into the working copy. If that fails,
// the working copy stays as it was.
func (s *Store) loadFrom(ctx context.Context, q querier) error {
	query := "SELECT 'workflows', id, data FROM workflows UNION ALL SELECT 'requests', id, data FROM requests"
	if s.pg {
		query = "SELECT 'workflows', id, data::text FROM workflows UNION ALL SELECT 'requests', id, data::text FROM requests"
	}
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()
	saved := map[rowKey][]byte{}
	for rows.Next() {
		var k rowKey
		var b []byte
		if err := rows.Scan(&k.table, &k.id, &b); err != nil {
			return err
		}
		saved[k] = b
	}
	if err := rows.Err(); err != nil {
		return err
	}
	old := s.saved
	s.saved = saved
	d, err := s.decode()
	if err != nil {
		s.saved = old
		return err
	}
	if s.pg {
		// jsonb stores JSON its own way (key order, spacing): keep each row
		// as jitd writes it, so diff only finds real changes.
		if err := s.normalize(d); err != nil {
			s.saved = old
			return err
		}
	}
	s.d = d
	return nil
}

// decode builds a working copy from the stored rows.
func (s *Store) decode() (Data, error) {
	d := Data{Workflows: map[string]*Workflow{}, Requests: map[string]*AccessRequest{}}
	for k, b := range s.saved {
		var err error
		if k.table == "workflows" {
			w := new(Workflow)
			err = json.Unmarshal(b, w)
			d.Workflows[k.id] = w
		} else {
			r := new(AccessRequest)
			err = json.Unmarshal(b, r)
			d.Requests[k.id] = r
		}
		if err != nil {
			return d, fmt.Errorf("%s %s: %w", singular(k.table), k.id, err)
		}
	}
	return d, nil
}

func singular(table string) string {
	if table == "workflows" {
		return "flow"
	}
	return "request"
}

// Close closes the database.
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.db.Close()
}

// View runs fn with read access under the lock. fn must not keep references.
// With Postgres it first picks up changes made by another jitd; if that
// fails, fn sees the working copy as it was.
func (s *Store) View(fn func(d *Data)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pg {
		if err := s.refresh(context.Background()); err != nil && s.Log != nil {
			s.Log.Error("reading the database", "err", err)
		}
	}
	fn(&s.d)
}

// Update runs fn under the lock and, if it returns nil, saves the rows it
// changed. If fn fails or panics, or saving fails, the working copy goes back
// to what is in the database. A no-op update (for example a verification
// that found nothing new) neither writes nor counts as a change, so pages do
// not reload for it.
func (s *Store) Update(fn func(d *Data) error) error {
	return s.update(fn, func() bool { return true })
}

// UpdateQuiet is Update for bookkeeping nobody is waiting to see (for
// example the time of the last verification): it saves but does not count
// as a change. fn returns whether it changed something user-visible; if so
// the change is counted after all.
func (s *Store) UpdateQuiet(fn func(d *Data) (visible bool, err error)) error {
	visible := false
	return s.update(func(d *Data) error {
		var err error
		visible, err = fn(d)
		return err
	}, func() bool { return visible })
}

// row is one row to write; data is nil to delete it.
type row struct {
	key  rowKey
	data []byte
	w    *Workflow
	r    *AccessRequest
}

func (s *Store) update(fn func(d *Data) error, count func() bool) (err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.broken != nil {
		return s.broken
	}
	// With Postgres the transaction starts first: it locks out other jitds
	// and brings the working copy up to date before fn sees it.
	var tx *sql.Tx
	if s.pg {
		if tx, err = s.beginPG(context.Background()); err != nil {
			return err
		}
		defer tx.Rollback()
	}
	defer func() {
		if p := recover(); p != nil {
			s.rollback()
			panic(p)
		}
	}()
	err = fn(&s.d)
	if errors.Is(err, errNoChange) {
		return nil
	}
	var changed []row
	if err == nil {
		changed, err = s.diff()
	}
	counted := len(changed) > 0 && count()
	if err == nil && len(changed) > 0 {
		if tx != nil {
			err = s.commitPG(tx, changed, counted)
		} else {
			err = s.write(changed)
		}
	}
	if err != nil {
		return errors.Join(err, s.rollback())
	}
	for _, c := range changed {
		if c.data == nil {
			delete(s.saved, c.key)
		} else {
			s.saved[c.key] = c.data
		}
	}
	if counted {
		s.changes++
		s.signal()
	}
	return nil
}

// signal wakes everyone waiting in Changed. The caller holds s.mu.
func (s *Store) signal() {
	if s.notify != nil {
		close(s.notify)
		s.notify = nil
	}
}

// Changed returns a channel that is closed at the next change this jitd
// makes or (with Postgres) picks up from another jitd. Waiters should still
// re-check now and then: a change another jitd made is only noticed when
// something reads the store.
func (s *Store) Changed() <-chan struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.notify == nil {
		s.notify = make(chan struct{})
	}
	return s.notify
}

// rollback puts the working copy back to what is in the database. If that
// fails, the two could differ, so the store refuses further updates.
func (s *Store) rollback() error {
	d, err := s.decode()
	if err != nil {
		s.broken = fmt.Errorf("the store stopped saving after an error it could not undo (restart jitd): %w", err)
		return s.broken
	}
	s.d = d
	return nil
}

// diff returns the rows where the working copy differs from the database.
func (s *Store) diff() ([]row, error) {
	var out []row
	seen := make(map[rowKey]bool, len(s.d.Workflows)+len(s.d.Requests))
	add := func(k rowKey, v any, w *Workflow, r *AccessRequest) error {
		seen[k] = true
		b, err := json.Marshal(v)
		if err != nil {
			return err
		}
		if old, ok := s.saved[k]; !ok || !bytes.Equal(old, b) {
			out = append(out, row{key: k, data: b, w: w, r: r})
		}
		return nil
	}
	for id, w := range s.d.Workflows {
		if w == nil {
			return nil, fmt.Errorf("flow %s is nil", id)
		}
		if err := add(rowKey{"workflows", id}, w, w, nil); err != nil {
			return nil, err
		}
	}
	for id, r := range s.d.Requests {
		if r == nil {
			return nil, fmt.Errorf("request %s is nil", id)
		}
		if err := add(rowKey{"requests", id}, r, nil, r); err != nil {
			return nil, err
		}
	}
	for k := range s.saved {
		if !seen[k] {
			out = append(out, row{key: k})
		}
	}
	return out, nil
}

func (s *Store) write(changed []row) error {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := s.writeRows(ctx, tx, changed); err != nil {
		return err
	}
	return tx.Commit()
}

// writeRows writes changed through q.
func (s *Store) writeRows(ctx context.Context, q querier, changed []row) error {
	for _, c := range changed {
		var err error
		switch {
		case c.data == nil:
			// The table name is one of two constants, never input.
			_, err = q.ExecContext(ctx, s.sql("DELETE FROM "+c.key.table+" WHERE id = ?"), c.key.id)
		case c.w != nil:
			_, err = q.ExecContext(ctx, s.sql(`INSERT INTO workflows (id, name, data) VALUES (?, ?, ?)
				ON CONFLICT (id) DO UPDATE SET name = excluded.name, data = excluded.data`),
				c.key.id, c.w.Name, string(c.data))
		default:
			var created any = timeColumn(c.r.CreatedAt)
			if s.pg {
				created = c.r.CreatedAt.UTC() // a timestamptz column
			}
			_, err = q.ExecContext(ctx, s.sql(`INSERT INTO requests (id, workflow_id, requester, status, created_at, data)
				VALUES (?, ?, ?, ?, ?, ?)
				ON CONFLICT (id) DO UPDATE SET workflow_id = excluded.workflow_id, requester = excluded.requester,
					status = excluded.status, created_at = excluded.created_at, data = excluded.data`),
				c.key.id, c.r.WorkflowID, c.r.Requester.Login, string(c.r.Status),
				created, string(c.data))
		}
		if err != nil {
			return fmt.Errorf("saving %s %s: %w", singular(c.key.table), c.key.id, err)
		}
	}
	return nil
}

// Changes counts the changes pages should see. Pages poll it to learn that
// something changed without reloading blindly. With SQLite it counts since
// the store was opened; with Postgres it is shared by every jitd on the
// database.
func (s *Store) Changes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.pg {
		if n, err := s.changesPG(context.Background()); err == nil {
			return n
		}
	}
	return s.changes
}

// createPrivate creates an empty file readable only by its owner. SQLite would
// create the database readable by everyone; its WAL and shared-memory files
// take their mode from the database file.
func createPrivate(path string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	return f.Close()
}

// removeDB removes a database file and its WAL and shared-memory files.
func removeDB(path string) {
	for _, p := range []string{path, path + "-wal", path + "-shm"} {
		os.Remove(p)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// clone deep-copies v through JSON so callers never share store memory.
func clone[T any](v *T) *T {
	b, _ := json.Marshal(v)
	var out T
	json.Unmarshal(b, &out)
	return &out
}
