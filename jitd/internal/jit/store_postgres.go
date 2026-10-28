package jit

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver
)

// Postgres keeps the same rows as SQLite, with the JSON in jsonb columns, and
// one more table, jitd_meta, with a single row:
//
//   - schema_version: Postgres has no PRAGMA user_version.
//   - generation: bumped by every write. A jitd whose working copy is at an
//     older generation reloads it before reading or writing, so several jitds
//     can share the database.
//   - changes: the count of user-visible changes, shared by every jitd, for
//     pages polling /v1/changes.
//
// Every write locks the jitd_meta row first (SELECT ... FOR UPDATE), so
// writes from different jitds take turns, and each one starts from what the
// previous one wrote.

// pgSchemaVersion is jitd_meta.schema_version. To change the tables, add a
// step to migratePG and bump it; never edit pgSchemaV1.
const pgSchemaVersion = 1

const pgSchemaV1 = `
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
	pgLockMigrate int64 = 0x6a697464_0001 // while creating or upgrading the tables
	pgLockService int64 = 0x6a697464_0002 // around each state change; see Store.Lock
)

// IsPostgres reports whether data (the -data flag) names a Postgres database
// rather than a SQLite file.
func IsPostgres(data string) bool {
	return strings.HasPrefix(data, "postgres://") || strings.HasPrefix(data, "postgresql://")
}

// RedactURL hides the password in a database URL, for logs.
func RedactURL(s string) string {
	u, err := url.Parse(s)
	if err != nil {
		return "(a database URL that does not parse)"
	}
	return u.Redacted()
}

func openPostgres(dsn string, readOnly bool) (*Store, error) {
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	// Store.mu orders the store's own queries; Store.Lock holds one more
	// connection for as long as a state change takes.
	db.SetMaxOpenConns(4)
	db.SetConnMaxIdleTime(5 * time.Minute)
	s := &Store{db: db, pg: true, Path: RedactURL(dsn), gen: -1}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if readOnly {
		err = s.checkVersionPG(ctx)
	} else {
		err = s.migratePG(ctx)
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

// sql turns a query written with ? placeholders into one for this database.
// The queries are jitd's own and have no ? anywhere else.
func (s *Store) sql(q string) string {
	if !s.pg {
		return q
	}
	var b strings.Builder
	n := 0
	for _, c := range q {
		if c == '?' {
			n++
			b.WriteString("$" + strconv.Itoa(n))
			continue
		}
		b.WriteRune(c)
	}
	return b.String()
}

func (s *Store) versionPG(ctx context.Context, q querier) (int, error) {
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

func (s *Store) checkVersionPG(ctx context.Context) error {
	v, err := s.versionPG(ctx, s.db)
	if err == nil && v != pgSchemaVersion {
		err = fmt.Errorf("the database is at schema %d; this jitd needs %d (start jitd once to upgrade it)", v, pgSchemaVersion)
	}
	return err
}

// migratePG brings the database to pgSchemaVersion, in one transaction. Two
// jitds starting at once take turns.
func (s *Store) migratePG(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock($1)`, pgLockMigrate); err != nil {
		return err
	}
	v, err := s.versionPG(ctx, tx)
	if err != nil {
		return err
	}
	switch {
	case v == pgSchemaVersion:
		return nil
	case v > pgSchemaVersion:
		return fmt.Errorf("the database was written by a newer jitd (schema %d; this jitd knows up to %d): run the newer jitd, or restore a backup", v, pgSchemaVersion)
	}
	if v < 1 {
		var taken bool
		if err := tx.QueryRowContext(ctx, `SELECT to_regclass('workflows') IS NOT NULL OR to_regclass('requests') IS NOT NULL`).Scan(&taken); err != nil {
			return err
		}
		if taken {
			return errors.New("this is not a jitd database (it has a workflows or requests table of its own); use another database, or another schema with ?search_path=")
		}
		if _, err := tx.ExecContext(ctx, pgSchemaV1); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO jitd_meta (id, schema_version, generation, changes) VALUES (1, 0, 0, 0)`); err != nil {
			return err
		}
	}
	if _, err := tx.ExecContext(ctx, `UPDATE jitd_meta SET schema_version = $1 WHERE id = 1`, pgSchemaVersion); err != nil {
		return err
	}
	return tx.Commit()
}

// refresh reloads the working copy if another jitd changed the database since
// it was loaded. The caller holds s.mu.
func (s *Store) refresh(ctx context.Context) error {
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

// beginPG starts a write: it locks the jitd_meta row, which makes other jitds'
// writes wait until this one commits, and reloads the working copy if it is
// behind. The caller holds s.mu, and commits with commitPG or rolls back.
func (s *Store) beginPG(ctx context.Context) (*sql.Tx, error) {
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

// commitPG writes changed in tx, bumps the generation (and the change count if
// counted) and commits.
func (s *Store) commitPG(tx *sql.Tx, changed []row, counted bool) error {
	ctx := context.Background()
	if err := s.writeRows(ctx, tx, changed); err != nil {
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

func (s *Store) changesPG(ctx context.Context) (uint64, error) {
	var n int64
	err := s.db.QueryRowContext(ctx, `SELECT changes FROM jitd_meta WHERE id = 1`).Scan(&n)
	return uint64(n), err
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

// Lock takes the lock that orders state changes across every jitd sharing a
// Postgres database: a change reads a request, calls the Tailscale API and
// writes the request back, and two jitds must not interleave those (two
// approvals of one request, two submits for one device). It returns the
// function that releases it. With SQLite there is one jitd per file, and
// Service.mu already orders its changes, so Lock does nothing.
func (s *Store) Lock(ctx context.Context) (unlock func(), err error) {
	if !s.pg {
		return func() {}, nil
	}
	c, err := s.db.Conn(ctx)
	if err != nil {
		return nil, err
	}
	if _, err := c.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, pgLockService); err != nil {
		c.Raw(func(any) error { return driver.ErrBadConn }) // may hold the lock: don't reuse it
		c.Close()
		return nil, err
	}
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if _, err := c.ExecContext(ctx, `SELECT pg_advisory_unlock($1)`, pgLockService); err != nil {
			// Closing the connection ends the session, which releases the
			// lock; returning it to the pool would keep it held.
			c.Raw(func(any) error { return driver.ErrBadConn })
		}
		c.Close()
	}, nil
}
