package main

import (
	"context"
	"embed"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

//go:embed html/*.html
var htmlFS embed.FS

func main() {
	client := NewClient(os.Getenv("USGS_API_KEY"))

	updates := make(chan Reading, 1)
	queries := make(chan chan<- Reading, 1)
	done := make(chan struct{})

	// State goroutine: single owner of the latest reading.
	go stateLoop(updates, queries)

	// Fetch goroutine: polls USGS and pushes readings to state.
	go fetchLoop(client, updates, done)

	// SQLite Boat store.
	dbPath := os.Getenv("DB_PATH")
	if dbPath == "" {
		dbPath = "boat_tracker.db"
	}
	sqliteStore, err := NewSQLiteBoatStore(dbPath)
	var boatStore BoatStore = sqliteStore
	if err != nil {
		log.Printf("[sqlite] failed to initialize SQLite store (%v), falling back to in-memory store", err)
		boatStore = NewMemoryBoatStore()
	} else {
		defer sqliteStore.Close()
		log.Printf("[sqlite] initialized SQLite store at %s", dbPath)
	}

	// HTTP server.
	srv := &http.Server{
		Addr:    ":8080",
		Handler: newMux(client, queries, boatStore),
	}
	go func() {
		log.Printf("[http] listening on http://localhost:8080")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("[http] server error: %v", err)
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	fmt.Println("Application running... press Ctrl+C or send SIGTERM to stop.")
	<-sigs
	fmt.Println()

	close(done)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
	fmt.Println("Application stopped")
}
