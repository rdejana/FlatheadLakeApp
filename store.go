package main

import (
	"database/sql"
	"fmt"
	"log"
	"sort"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// BoatLog represents a record of launching or retrieving a boat on the lift.
type BoatLog struct {
	ID        string    `json:"id"`
	Action    string    `json:"action"` // "in" (launch / season start) or "out" (haul / season end)
	Rating    string    `json:"rating"` // "green" (no issues), "yellow" (minor issue / close), "red" (challenging)
	LakeLevel float64   `json:"lake_level"`
	Unit      string    `json:"unit"`
	FullPool  float64   `json:"full_pool"`
	Delta     float64   `json:"delta"`
	LoggedAt  time.Time `json:"logged_at"` // Timestamp when the action occurred
	Notes     string    `json:"notes,omitempty"`
	CreatedAt time.Time `json:"created_at"`
}

// BoatStore defines the interface for boat log storage.
type BoatStore interface {
	GetAll() []BoatLog
	Add(log BoatLog) BoatLog
	Delete(id string) bool
}

// SQLiteBoatStore is the SQLite implementation of BoatStore.
type SQLiteBoatStore struct {
	db *sql.DB
}

func NewSQLiteBoatStore(dbPath string) (*SQLiteBoatStore, error) {
	db, err := sql.Open("sqlite", dbPath)
	if err != nil {
		return nil, fmt.Errorf("open sqlite db: %w", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS boat_logs (
		id TEXT PRIMARY KEY,
		action TEXT NOT NULL,
		rating TEXT NOT NULL,
		lake_level REAL NOT NULL,
		unit TEXT NOT NULL,
		full_pool REAL NOT NULL,
		delta REAL NOT NULL,
		logged_at TEXT NOT NULL,
		notes TEXT,
		created_at TEXT NOT NULL
	);`
	if _, err := db.Exec(createTableSQL); err != nil {
		db.Close()
		return nil, fmt.Errorf("create boat_logs table: %w", err)
	}

	return &SQLiteBoatStore{db: db}, nil
}

func (s *SQLiteBoatStore) Close() error {
	return s.db.Close()
}

func (s *SQLiteBoatStore) GetAll() []BoatLog {
	rows, err := s.db.Query(`
		SELECT id, action, rating, lake_level, unit, full_pool, delta, logged_at, COALESCE(notes, ''), created_at
		FROM boat_logs
		ORDER BY logged_at DESC
	`)
	if err != nil {
		log.Printf("[sqlite] query error: %v", err)
		return []BoatLog{}
	}
	defer rows.Close()

	var logs []BoatLog
	for rows.Next() {
		var l BoatLog
		var loggedAtStr, createdAtStr string
		if err := rows.Scan(&l.ID, &l.Action, &l.Rating, &l.LakeLevel, &l.Unit, &l.FullPool, &l.Delta, &loggedAtStr, &l.Notes, &createdAtStr); err != nil {
			log.Printf("[sqlite] scan error: %v", err)
			continue
		}
		if t, err := time.Parse(time.RFC3339, loggedAtStr); err == nil {
			l.LoggedAt = t
		}
		if t, err := time.Parse(time.RFC3339, createdAtStr); err == nil {
			l.CreatedAt = t
		}
		logs = append(logs, l)
	}
	return logs
}

func (s *SQLiteBoatStore) Add(l BoatLog) BoatLog {
	if l.ID == "" {
		l.ID = fmt.Sprintf("log-%d", time.Now().UnixNano())
	}
	if l.CreatedAt.IsZero() {
		l.CreatedAt = time.Now()
	}

	query := `
		INSERT INTO boat_logs (id, action, rating, lake_level, unit, full_pool, delta, logged_at, notes, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			action = excluded.action,
			rating = excluded.rating,
			lake_level = excluded.lake_level,
			unit = excluded.unit,
			full_pool = excluded.full_pool,
			delta = excluded.delta,
			logged_at = excluded.logged_at,
			notes = excluded.notes,
			created_at = excluded.created_at
	`
	_, err := s.db.Exec(query,
		l.ID,
		l.Action,
		l.Rating,
		l.LakeLevel,
		l.Unit,
		l.FullPool,
		l.Delta,
		l.LoggedAt.Format(time.RFC3339),
		l.Notes,
		l.CreatedAt.Format(time.RFC3339),
	)
	if err != nil {
		log.Printf("[sqlite] insert error: %v", err)
	}
	return l
}

func (s *SQLiteBoatStore) Delete(id string) bool {
	res, err := s.db.Exec("DELETE FROM boat_logs WHERE id = ?", id)
	if err != nil {
		log.Printf("[sqlite] delete error: %v", err)
		return false
	}
	n, err := res.RowsAffected()
	return err == nil && n > 0
}

// MemoryBoatStore is the in-memory implementation of BoatStore.
type MemoryBoatStore struct {
	mu   sync.RWMutex
	logs map[string]BoatLog
}

func NewMemoryBoatStore() *MemoryBoatStore {
	return &MemoryBoatStore{
		logs: make(map[string]BoatLog),
	}
}

func (s *MemoryBoatStore) GetAll() []BoatLog {
	s.mu.RLock()
	defer s.mu.RUnlock()

	res := make([]BoatLog, 0, len(s.logs))
	for _, l := range s.logs {
		res = append(res, l)
	}
	// Sort newest first
	sort.Slice(res, func(i, j int) bool {
		return res[i].LoggedAt.After(res[j].LoggedAt)
	})
	return res
}

func (s *MemoryBoatStore) Add(l BoatLog) BoatLog {
	s.mu.Lock()
	defer s.mu.Unlock()

	if l.ID == "" {
		l.ID = fmt.Sprintf("log-%d", time.Now().UnixNano())
	}
	if l.CreatedAt.IsZero() {
		l.CreatedAt = time.Now()
	}
	s.logs[l.ID] = l
	return l
}

func (s *MemoryBoatStore) Delete(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, exists := s.logs[id]; exists {
		delete(s.logs, id)
		return true
	}
	return false
}
