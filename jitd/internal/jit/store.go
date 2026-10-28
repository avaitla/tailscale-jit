package jit

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"sync"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// Data is everything jitd persists.
type Data struct {
	Workflows map[string]*Workflow      `json:"workflows"`
	Requests  map[string]*AccessRequest `json:"requests"`
}

// The database is Postgres. Each row holds the object as JSON (jsonb), plus a
// few columns for ad-hoc queries, and one more table, jitd_meta, has a single
// row:
//
//   - schema_version: the version of these tables.
//   - generation: bumped by every write. A jitd whose working copy is at an
//     older generation reloads it before reading or writing, so several jitds
//     can share the database.
//   - changes: the count of user-visible changes, shared by every jitd, for
//     pages polling /v1/changes.
//
// Every write locks the jitd_meta row first (SELECT ... FOR UPDATE), so
// writes from different jitds take turns, and each one starts from what the
// previous one wrote.

// schemaVersion is jitd_meta.schema_version. To change the tables, add a step
// to migrate and bump it; never edit schemaV1.
const schemaVersion = 1

const schemaV1 = `
CREATE TABLE workflows (
	id   TEXT PRIMARY KEY,
	name TEXT NOT NULL,
	data JSONB NOT NULL -- the Workflow
);
CREATE TABLE requests (
	id          TEXT PRIMARY KEY,
	workflow_id TEXT NOT NULL,
	requester   TEXT NOT NULL,
	status      TEXT NOT NULL,
	created_at  TIMESTAMPTZ NOT NULL,
	data        JSONB NOT NULL -- the AccessRequest
);
-- jitd reads everything into memory; these are for people with psql.
CREATE INDEX requests_status ON requests(status);
CREATE INDEX requests_workflow ON requests(workflow_id);
CREATE INDEX requests_created ON requests(created_at);
CREATE TABLE jitd_meta (
	id             INT PRIMARY KEY CHECK (id = 1),
	schema_version INT NOT NULL,
	generation     BIGINT NOT NULL,
	changes        BIGINT NOT NULL
);
`

// Advisory lock keys ("jitd" and a number). They are per database, not per
// schema: jitds on different schemas of one database only wait for each
// other a little more than needed.
const (
	lockMigrate int64 = 0x6a697464_0001 // while creating or upgrading the tables
	lockService int64 = 0x6a697464_0002 // around each state change; see Store.Lock
)

// errNoChange, returned by an Update function that changed nothing, skips
// comparing every row with the database. Update then returns nil.
var errNoChange = errors.New("no change")

// Store keeps approval flows and access requests in a Postgres database, with
// a working copy in memory. An Update writes only the rows that changed, in
// one transaction. Every read first checks whether another jitd changed the
// database and reloads if so, and every write locks the database first.
//
// A Store from OpenMemory has no database: tests and the preview use it.
type Store struct {
	db       *sql.DB // nil: in memory only
	readOnly bool
	mu       sync.Mutex
	d        Data
	saved    map[rowKey][]byte // each row's JSON as it is in the database
	changes  uint64            // in memory only; with a database, jitd_meta.changes
	gen      int64             // the generation the working copy was loaded at
	notify   chan struct{}     // closed at the next counted change; see Changed
	broken   error             // set if the working copy could not be put back after a failed update

	// Path is the database URL without its password.
	Path string
	// Log reports a failed reload; nil discards it.
	Log *slog.Logger
}

type rowKey struct {
	table string // "workflows" or "requests"
	id    string
}

// IsPostgres reports whether s is a Postgres URL.
func IsPostgres(s string) bool {
	return strings.HasPrefix(s, "postgres://") || strings.HasPrefix(s, "postgresql://")
}

// RedactURL hides the password in a database URL, for logs.
func RedactURL(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return "(a database URL that does not parse)"
	}
	return u.Redacted()
}

// Open opens the Postgres database at dsn (a postgres:// URL), creating or
// upgrading jitd's tables if needed.
func Open(dsn string) (*Store, error) {
	return open(dsn, false)
}

// OpenReadOnly opens a database jitd has set up, without changing it.
func OpenReadOnly(dsn string) (*Store, error) {
	return open(dsn, true)
}

// OpenMemory returns a store without a database, which forgets everything
// when the process ends. Tests and the preview use it.
func OpenMemory() *Store {
	return &Store{
		d:     Data{Workflows: map[string]*Workflow{}, Requests: map[string]*AccessRequest{}},
		saved: map[rowKey][]byte{},
		Path:  "(in memory)",
	}
}

func open(dsn string, readOnly bool) (*Store, error) {
	if !IsPostgres(dsn) {
		return nil, fmt.Errorf("%s: not a postgres:// URL", RedactURL(dsn))
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	// Store.mu orders the store's own queries; Store.Lock holds one more
	// connection for as long as a state change takes.
	db.SetMaxOpenConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	s := &Store{db: db, readOnly: readOnly, Path: RedactURL(dsn), gen: -1}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if readOnly {
		err = s.checkVersion(ctx)
	} else {
		err = s.migrate(ctx)
	}
	if err == nil {
		err = s.refresh(ctx)
	}
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("%s: %w", s.Path, err)
	}
	return s, nil
}

// querier is a database or a transaction.
type querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

func version(ctx context.Context, q querier) (int, error) {
	var exists bool
	if err := q.QueryRowContext(ctx, `SELECT to_regclass('jitd_meta') IS NOT NULL`).Scan(&exists); err != nil {
		return 0, err
	}
	if !exists {
		return 0, nil
	}
	var v int
	err := q.QueryRowContext(ctx, `SELECT schema_version FROM jitd_meta WHERE id = 1`).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, errors.New("jitd_meta has no row: this database was not set up by jitd")
	}
	return v, err
}

func (s *Store) checkVersion(ctx context.Context) error {
	v, err := version(ctx, s.db)
	if err == nil && v != schemaVersion {
		err = fmt.Errorf("the database is at schema %d; this jitd needs %d (start jitd once to upgrade it)", v, schemaVersion)
	}
	return err
}

// migrate brings the database to schemaVersion, in one transaction. Two jitds
// starting at once take turns.
func (s *Store) migrate(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, lockMigrate); err != nil {
		return err
	}
	v, err := version(ctx, tx)
	if err != nil {
		return err
	}
	switch {
	case v == schemaVersion:
		return nil
	case v > schemaVersion:
		return fmt.Errorf("the database was written by a newer jitd (schema %d; this jitd knows up to %d): run the newer jitd, or restore a backup", v, schemaVersion)
	}
	if v < 1 {
		var taken bool
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass('workflows') IS NOT NULL OR to_regclass('requests') IS NOT NULL`).Scan(&taken); err != nil {
			return err
		}
		if taken {
			return errors.New("this is not a jitd database (it has a workflows or requests table of its own); use another database, or another schema with ?search_path=")
		}
		if _, err := tx.ExecContext(ctx, schemaV1); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO jitd_meta (id, schema_version, generation, changes) VALUES (1, 0, 0, 0)`); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jitd_meta SET schema_version = $1 WHERE id = 1`, schemaVersion); err != nil {
		return err
	}
	return tx.Commit()
}

// loadFrom reads every row through q into the working copy. If that fails,
// the working copy stays as it was.
func (s *Store) loadFrom(ctx context.Context, q querier) error {
	rows, err := q.QueryContext(ctx, "SELECT 'workflows', id, data::text FROM workflows UNION ALL SELECT 'requests', id, data::text FROM requests")
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
	if err == nil {
		// jsonb stores JSON its own way (key order, spacing): keep each row
		// as jitd writes it, so diff only finds real changes.
		err = s.normalize(d)
	}
	if err != nil {
		s.saved = old
		return err
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

// normalize sets each saved row to the JSON jitd writes for d's object.
func (s *Store) normalize(d Data) error {
	for k := range s.saved {
		var v any = d.Requests[k.id]
		if k.table == "workflows" {
			v = d.Workflows[k.id]
		}
		b, err := json.Marshal(v)
		if err != nil {
			return fmt.Errorf("%s %s: %w", singular(k.table), k.id, err)
		}
		s.saved[k] = b
	}
	return nil
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
	if s.db == nil {
		return nil
	}
	return s.db.Close()
}

// refresh reloads the working copy if another jitd changed the database since
// it was loaded. The caller holds s.mu.
func (s *Store) refresh(ctx context.Context) error {
	if s.db == nil {
		return nil
	}
	var gen int64
	if err := s.db.QueryRowContext(ctx, `SELECT generation FROM jitd_meta WHERE id = 1`).Scan(&gen); err != nil {
		return err
	}
	if gen == s.gen {
		return nil
	}
	// One snapshot, so the rows match the generation read with them.
	tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `SELECT generation FROM jitd_meta WHERE id = 1`).Scan(&gen); err != nil {
		return err
	}
	if err := s.loadFrom(ctx, tx); err != nil {
		return err
	}
	s.gen = gen
	s.signal()
	return nil
}

// View runs fn with read access under the lock. fn must not keep references.
// It first picks up changes made by another jitd; if that fails, fn sees the
// working copy as it was.
func (s *Store) View(fn func(d *Data)) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.refresh(context.Background()); err != nil && s.Log != nil {
		s.Log.Error("reading the database", "err", err)
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
	if s.readOnly {
		return errors.New("the store is read-only")
	}
	// The transaction starts first: it locks out other jitds and brings the
	// working copy up to date before fn sees it.
	var tx *sql.Tx
	if s.db != nil {
		if tx, err = s.begin(context.Background()); err != nil {
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
	if err == nil && len(changed) > 0 && tx != nil {
		err = s.commit(tx, changed, counted)
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

// begin starts a write: it locks the jitd_meta row, which makes other jitds'
// writes wait until this one commits, and reloads the working copy if it is
// behind. The caller holds s.mu, and commits with commit or rolls back.
func (s *Store) begin(ctx context.Context) (*sql.Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	var gen int64
	if err := tx.QueryRowContext(ctx, `SELECT generation FROM jitd_meta WHERE id = 1 FOR UPDATE`).Scan(&gen); err != nil {
		tx.Rollback()
		return nil, err
	}
	if gen != s.gen {
		// Every writer holds the row lock, so nothing commits while this
		// reads: the rows match gen.
		if err := s.loadFrom(ctx, tx); err != nil {
			tx.Rollback()
			return nil, err
		}
		s.gen = gen
		s.signal()
	}
	return tx, nil
}

// commit writes changed in tx, bumps the generation (and the change count if
// counted) and commits.
func (s *Store) commit(tx *sql.Tx, changed []row, counted bool) error {
	ctx := context.Background()
	if err := writeRows(ctx, tx, changed); err != nil {
		return err
	}
	n := 0
	if counted {
		n = 1
	}
	var gen int64
	if err := tx.QueryRowContext(ctx, `UPDATE jitd_meta SET generation = generation + 1, changes = changes + $1 WHERE id = 1 RETURNING generation`, n).Scan(&gen); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		// It may have committed anyway; then the generation moved and the
		// next read reloads.
		return err
	}
	s.gen = gen
	return nil
}

// writeRows writes changed through q.
func writeRows(ctx context.Context, q querier, changed []row) error {
	for _, c := range changed {
		var err error
		switch {
		case c.data == nil:
			// The table name is one of two constants, never input.
			_, err = q.ExecContext(ctx, "DELETE FROM "+c.key.table+" WHERE id = $1", c.key.id)
		case c.w != nil:
			_, err = q.ExecContext(ctx, `INSERT INTO workflows (id, name, data) VALUES ($1, $2, $3)
				ON CONFLICT (id) DO UPDATE SET name = excluded.name, data = excluded.data`,
				c.key.id, c.w.Name, string(c.data))
		default:
			_, err = q.ExecContext(ctx, `INSERT INTO requests (id, workflow_id, requester, status, created_at, data)
				VALUES ($1, $2, $3, $4, $5, $6)
				ON CONFLICT (id) DO UPDATE SET workflow_id = excluded.workflow_id, requester = excluded.requester,
					status = excluded.status, created_at = excluded.created_at, data = excluded.data`,
				c.key.id, c.r.WorkflowID, c.r.Requester.Login, string(c.r.Status),
				c.r.CreatedAt.UTC(), string(c.data))
		}
		if err != nil {
			return fmt.Errorf("saving %s %s: %w", singular(c.key.table), c.key.id, err)
		}
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
// makes or picks up from another jitd. Waiters should still re-check now and
// then: a change another jitd made is only noticed when something reads the
// store.
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

// Changes counts the changes pages should see. Pages poll it to learn that
// something changed without reloading blindly. It is shared by every jitd on
// the database.
func (s *Store) Changes() uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db == nil {
		return s.changes
	}
	var n int64
	if err := s.db.QueryRowContext(context.Background(), `SELECT changes FROM jitd_meta WHERE id = 1`).Scan(&n); err != nil {
		return s.changes
	}
	return uint64(n)
}

// Lock takes the lock that orders state changes across every jitd sharing
// the database: a change reads a request, calls the Tailscale API and writes
// the request back, and two jitds must not interleave those (two approvals
// of one request, two submits for one device). It returns the function that
// releases it. Without a database there is one jitd, and Service.mu already
// orders its changes, so Lock does nothing.
func (s *Store) Lock(ctx context.Context) (unlock func(), err error) {
	if s.db == nil {
		return func() {}, nil
	}
	c, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := c.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, lockService); err != nil {
		c.Raw(func(any) error { return driver.ErrBadConn }) // may hold the lock: don't reuse it
		c.Close()
		return nil, err
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := c.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, lockService); err != nil {
			// Closing the connection ends the session, which releases the
			// lock; returning it to the pool would keep it held.
			c.Raw(func(any) error { return driver.ErrBadConn })
		}
		c.Close()
	}, nil
}

// clone deep-copies v through JSON so callers never share store memory.
func clone[T any](v *T) *T {
	b, _ := json.Marshal(v)
	var out T
	json.Unmarshal(b, &out)
	return &out
}
