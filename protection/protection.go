// Package protection keeps the list of protected transaction names in a
// place that every replica of your app can read.
//
// A protected name (like "deposit") runs at SERIALIZABLE and is retried by
// capture.Monitor.Do. Without a store, protection lives in the memory of one
// process: a replica only protects what its own checker saw, and a restart
// forgets everything. With a store, every replica reads the same list, and
// the IsoWatch collector (which sees the traffic of all replicas) can add
// names to it.
//
// Use it through capture.Config.ProtectionStore:
//
//	store, err := protection.NewPostgres(pool, "")  // table isowatch_protected
//	mon, err := capture.New(ctx, pool, capture.Config{
//		Tables:          []string{"accounts"},
//		Sink:            sink,
//		ProtectionStore: store,
//	})
package protection

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Store keeps the set of protected names.
//
// Implementations must be safe for concurrent use. Add of a name that is
// already there and Remove of a name that is not there are not errors.
type Store interface {
	// Add protects a name. Reason is a short note for people, such as the
	// anomaly that caused it. It may be empty.
	Add(ctx context.Context, name, reason string) error
	// Remove stops protecting a name.
	Remove(ctx context.Context, name string) error
	// List returns every protected name.
	List(ctx context.Context) ([]string, error)
}

// DefaultTable is the table Postgres uses when none is given.
const DefaultTable = "isowatch_protected"

// DB is the part of pgx that Postgres needs. *pgxpool.Pool and *pgx.Conn
// both work.
//
// Give it a plain pool, not one wrapped by IsoWatch, so the store's own
// queries are never tracked.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// Postgres is a Store kept in a PostgreSQL table:
//
//	CREATE TABLE IF NOT EXISTS isowatch_protected (
//	  name     text PRIMARY KEY,
//	  reason   text NOT NULL DEFAULT '',
//	  added_at timestamptz NOT NULL DEFAULT now()
//	);
//
// The IsoWatch collector can write to the same table, so a name it protects
// reaches every replica.
type Postgres struct {
	db    DB
	table string
}

// tablePart is one part of a table name. Only lower case plain identifiers
// are allowed, so the name means the same table whether a tool writes it
// with or without double quotes.
var tablePart = regexp.MustCompile(`^[a-z_][a-z0-9_$]{0,62}$`)

// NewPostgres returns a store that uses the given table, or DefaultTable if
// table is empty. The table may be schema qualified, like
// "isowatch.protected". It does not touch the database.
func NewPostgres(db DB, table string) (*Postgres, error) {
	if db == nil {
		return nil, errors.New("isowatch: protection store needs a database")
	}
	if table == "" {
		table = DefaultTable
	}
	if err := ValidTable(table); err != nil {
		return nil, err
	}
	return &Postgres{db: db, table: table}, nil
}

// ValidTable reports whether a table name can be used: one or two parts
// ("table" or "schema.table"), each a lower case identifier of letters,
// digits, "_" and "$", not starting with a digit, at most 63 characters.
func ValidTable(table string) error {
	parts := strings.Split(table, ".")
	if len(parts) > 2 {
		return fmt.Errorf("isowatch: protection table %q has too many parts", table)
	}
	for _, p := range parts {
		if !tablePart.MatchString(p) {
			return fmt.Errorf("isowatch: protection table %q must be a lower case name like %q or %q", table, DefaultTable, "isowatch."+DefaultTable)
		}
	}
	return nil
}

// Table returns the table name.
func (p *Postgres) Table() string { return p.table }

// CreateTableSQL returns the statement that creates the table.
func CreateTableSQL(table string) string {
	return `CREATE TABLE IF NOT EXISTS ` + table + ` (
  name     text PRIMARY KEY,
  reason   text NOT NULL DEFAULT '',
  added_at timestamptz NOT NULL DEFAULT now()
)`
}

// CreateTable creates the table if it does not exist. Add also does this
// when it finds no table, so calling it is only needed when you want the
// table to exist before the first name is protected.
func (p *Postgres) CreateTable(ctx context.Context) error {
	if _, err := p.db.Exec(ctx, CreateTableSQL(p.table)); err != nil {
		return fmt.Errorf("isowatch: creating %s: %w", p.table, err)
	}
	return nil
}

// Add protects a name. If the table does not exist yet, it is created.
func (p *Postgres) Add(ctx context.Context, name, reason string) error {
	if name == "" {
		return nil
	}
	q := `INSERT INTO ` + p.table + ` (name, reason) VALUES ($1, $2) ON CONFLICT (name) DO NOTHING`
	_, err := p.db.Exec(ctx, q, name, reason)
	if undefinedTable(err) {
		if err := p.CreateTable(ctx); err != nil {
			return err
		}
		_, err = p.db.Exec(ctx, q, name, reason)
	}
	if err != nil {
		return fmt.Errorf("isowatch: protecting %q in %s: %w", name, p.table, err)
	}
	return nil
}

// Remove stops protecting a name.
func (p *Postgres) Remove(ctx context.Context, name string) error {
	_, err := p.db.Exec(ctx, `DELETE FROM `+p.table+` WHERE name = $1`, name)
	if err != nil && !undefinedTable(err) {
		return fmt.Errorf("isowatch: unprotecting %q in %s: %w", name, p.table, err)
	}
	return nil
}

// List returns every protected name. A table that does not exist yet means
// that nothing is protected, so it returns no names and no error.
func (p *Postgres) List(ctx context.Context) ([]string, error) {
	rows, err := p.db.Query(ctx, `SELECT name FROM `+p.table)
	if err == nil {
		var names []string
		names, err = pgx.CollectRows(rows, pgx.RowTo[string])
		if err == nil {
			return names, nil
		}
	}
	if undefinedTable(err) {
		return nil, nil
	}
	return nil, fmt.Errorf("isowatch: reading %s: %w", p.table, err)
}

// undefinedTable reports PostgreSQL error 42P01 (the table does not exist).
func undefinedTable(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "42P01"
}
