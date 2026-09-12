package main

import (
	"context"
	"log"
	"time"
)

// Reading holds the latest pool level measurement.
type Reading struct {
	Station    string    `json:"station"`
	Value      float64   `json:"value"`
	Unit       string    `json:"unit"`
	FullPool   float64   `json:"full_pool"`
	Delta      float64   `json:"delta"`
	MeasuredAt time.Time `json:"measured_at"`
	FetchedAt  time.Time `json:"fetched_at"`
}

type HistoricalPoint struct {
	Time  time.Time `json:"time"`
	Value float64   `json:"value"`
}

type HistoryResponse struct {
	Station  string            `json:"station"`
	Unit     string            `json:"unit"`
	FullPool float64           `json:"full_pool"`
	Points   []HistoricalPoint `json:"points"`
}

// ---- State goroutine -------------------------------------------------------

// stateLoop owns the latest Reading. Other goroutines communicate with it
// via channels only — no shared state, no mutexes.
func stateLoop(updates <-chan Reading, queries <-chan chan<- Reading) {
	var latest Reading
	for {
		select {
		case r := <-updates:
			latest = r
			log.Printf("[state] updated: %.2f %s at %s", r.Value, r.Unit, r.MeasuredAt.Format(time.RFC3339))
		case reply := <-queries:
			reply <- latest
		}
	}
}

// ---- Fetch goroutine -------------------------------------------------------

func fetchLoop(client *Client, updates chan<- Reading, done <-chan struct{}) {
	ticker := time.NewTicker(10 * time.Minute)
	defer ticker.Stop()

	fetch := make(chan struct{}, 1)
	fetch <- struct{}{} // immediate fetch on startup

	doFetch := func() {
		fc, err := client.GetLatestContinuous(context.Background(), defaultStationID, defaultParameter)
		if err != nil {
			log.Printf("[fetch] error: %v", err)
			return
		}
		if len(fc.Features) == 0 {
			log.Printf("[fetch] no features returned")
			return
		}
		p := fc.Features[0].Properties
		value := float64(p.Value)
		updates <- Reading{
			Station:    p.MonitoringLocationID,
			Value:      value,
			Unit:       p.UnitOfMeasure,
			FullPool:   summerFullPool,
			Delta:      value - summerFullPool,
			MeasuredAt: p.Time,
			FetchedAt:  time.Now(),
		}
	}

	for {
		select {
		case <-done:
			return
		case <-fetch:
			doFetch()
		case <-ticker.C:
			doFetch()
		}
	}
}
