package main

import (
	"bytes"
	"context"
	"embed"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

//go:embed html/*.html
var htmlFS embed.FS

const (
	latestContinuousURL = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/latest-continuous/items"
	continuousURL       = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/continuous/items"
	defaultStationID    = "USGS-12371550"
	defaultParameter    = "00062"
	summerFullPool      = 2893.00
)

// ---- USGS API types --------------------------------------------------------

type Float64Value float64

func (f *Float64Value) UnmarshalJSON(data []byte) error {
	var n float64
	if err := json.Unmarshal(data, &n); err == nil {
		*f = Float64Value(n)
		return nil
	}

	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return err
	}

	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return err
	}

	*f = Float64Value(v)
	return nil
}

type FeatureCollection struct {
	Type           string    `json:"type"`
	Features       []Feature `json:"features"`
	NumberReturned int       `json:"numberReturned"`
	Links          []Link    `json:"links"`
	TimeStamp      time.Time `json:"timeStamp"`
}

type Feature struct {
	Type       string                `json:"type"`
	ID         string                `json:"id"`
	Properties MeasurementProperties `json:"properties"`
	Geometry   Geometry              `json:"geometry"`
}

type MeasurementProperties struct {
	TimeSeriesID         string       `json:"time_series_id"`
	MonitoringLocationID string       `json:"monitoring_location_id"`
	ParameterCode        string       `json:"parameter_code"`
	StatisticID          string       `json:"statistic_id"`
	Time                 time.Time    `json:"time"`
	Value                Float64Value `json:"value"`
	UnitOfMeasure        string       `json:"unit_of_measure"`
	ApprovalStatus       string       `json:"approval_status"`
	Qualifier            *string      `json:"qualifier"`
	LastModified         time.Time    `json:"last_modified"`
}

type Geometry struct {
	Type        string    `json:"type"`
	Coordinates []float64 `json:"coordinates"`
}

type Link struct {
	Type  string `json:"type"`
	Rel   string `json:"rel"`
	Title string `json:"title"`
	Href  string `json:"href"`
}

// ---- Client ----------------------------------------------------------------

type Client struct {
	apiKey string
	client *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey: apiKey,
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (c *Client) GetLatestContinuous(ctx context.Context, locationID, parameterCode string) (*FeatureCollection, error) {
	u, err := url.Parse(latestContinuousURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	u.RawQuery = q.Encode()

	return c.fetchFeatureCollection(ctx, u.String())
}

func (c *Client) GetContinuousHistory(ctx context.Context, locationID, parameterCode, start, end string) (*FeatureCollection, error) {
	u, err := url.Parse(continuousURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	q.Set("datetime", fmt.Sprintf("%s/%s", start, end))
	q.Set("limit", "10000")
	u.RawQuery = q.Encode()

	return c.fetchFeatureCollection(ctx, u.String())
}

func (c *Client) GetPointContinuous(ctx context.Context, locationID, parameterCode string, point time.Time) (*FeatureCollection, error) {
	u, err := url.Parse(continuousURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	// Query starting at the requested timestamp with a 2-hour forward window to find the closest reading
	endWindow := point.Add(2 * time.Hour)
	q.Set("datetime", fmt.Sprintf("%s/%s", point.UTC().Format(time.RFC3339), endWindow.UTC().Format(time.RFC3339)))
	q.Set("limit", "1")
	u.RawQuery = q.Encode()

	return c.fetchFeatureCollection(ctx, u.String())
}

func (c *Client) fetchFeatureCollection(ctx context.Context, reqURL string) (*FeatureCollection, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Set("Accept", "application/json")
	if c.apiKey != "" {
		req.Header.Set("X-Api-Key", c.apiKey)
	}

	resp, err := c.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http %d: %s", resp.StatusCode, string(body))
	}

	var result FeatureCollection
	if err := json.Unmarshal(body, &result); err != nil {
		return nil, err
	}

	return &result, nil
}

// ---- Domain ----------------------------------------------------------------

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

// BoatStore defines the interface for boat log storage (ready for SQLite implementation later).
type BoatStore interface {
	GetAll() []BoatLog
	Add(log BoatLog) BoatLog
	Delete(id string) bool
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
	l.CreatedAt = time.Now()
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

// ---- HTTP handlers ---------------------------------------------------------

func serveEmbeddedHTML(w http.ResponseWriter, filename string) {
	data, err := htmlFS.ReadFile("html/" + filename)
	if err != nil {
		http.Error(w, "file not found", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(data)
}

func newMux(client *Client, queries chan<- chan<- Reading, boatStore BoatStore) *http.ServeMux {
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		serveEmbeddedHTML(w, "index.html")
	})

	mux.HandleFunc("/history", func(w http.ResponseWriter, r *http.Request) {
		serveEmbeddedHTML(w, "history.html")
	})

	mux.HandleFunc("/boat", func(w http.ResponseWriter, r *http.Request) {
		serveEmbeddedHTML(w, "boat.html")
	})

	mux.HandleFunc("/data", func(w http.ResponseWriter, r *http.Request) {
		reply := make(chan Reading, 1)
		queries <- reply
		reading := <-reply
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(reading)
	})

	mux.HandleFunc("/data/history", func(w http.ResponseWriter, r *http.Request) {
		start := r.URL.Query().Get("start")
		end := r.URL.Query().Get("end")

		if start == "" || end == "" {
			http.Error(w, "start and end query parameters are required", http.StatusBadRequest)
			return
		}

		startTime, err := time.Parse(time.RFC3339, start)
		if err != nil {
			http.Error(w, "invalid start time format, expected RFC3339", http.StatusBadRequest)
			return
		}

		endTime, err := time.Parse(time.RFC3339, end)
		if err != nil {
			http.Error(w, "invalid end time format, expected RFC3339", http.StatusBadRequest)
			return
		}

		fc, err := client.GetContinuousHistory(r.Context(), defaultStationID, defaultParameter, startTime.Format(time.RFC3339), endTime.Format(time.RFC3339))
		if err != nil {
			log.Printf("[history] error fetching USGS data: %v", err)
			http.Error(w, fmt.Sprintf("failed to fetch historical data: %v", err), http.StatusInternalServerError)
			return
		}

		points := make([]HistoricalPoint, 0, len(fc.Features))
		unit := "ft"
		station := defaultStationID

		for _, feat := range fc.Features {
			if feat.Properties.UnitOfMeasure != "" {
				unit = feat.Properties.UnitOfMeasure
			}
			if feat.Properties.MonitoringLocationID != "" {
				station = feat.Properties.MonitoringLocationID
			}
			points = append(points, HistoricalPoint{
				Time:  feat.Properties.Time,
				Value: float64(feat.Properties.Value),
			})
		}

		resp := HistoryResponse{
			Station:  station,
			Unit:     unit,
			FullPool: summerFullPool,
			Points:   points,
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/data/point", func(w http.ResponseWriter, r *http.Request) {
		timeParam := r.URL.Query().Get("time")
		if timeParam == "" {
			http.Error(w, "time query parameter is required", http.StatusBadRequest)
			return
		}

		pointTime, err := time.Parse(time.RFC3339, timeParam)
		if err != nil {
			http.Error(w, "invalid time format, expected RFC3339", http.StatusBadRequest)
			return
		}

		fc, err := client.GetPointContinuous(r.Context(), defaultStationID, defaultParameter, pointTime)
		if err != nil {
			log.Printf("[point] error fetching USGS data: %v", err)
			http.Error(w, fmt.Sprintf("failed to fetch data: %v", err), http.StatusInternalServerError)
			return
		}

		if len(fc.Features) == 0 {
			http.Error(w, "no reading found for the specified point in time", http.StatusNotFound)
			return
		}

		p := fc.Features[0].Properties
		value := float64(p.Value)
		resp := Reading{
			Station:    p.MonitoringLocationID,
			Value:      value,
			Unit:       p.UnitOfMeasure,
			FullPool:   summerFullPool,
			Delta:      value - summerFullPool,
			MeasuredAt: p.Time,
			FetchedAt:  time.Now(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("/api/boat-logs", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			logs := boatStore.GetAll()
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(logs)

		case http.MethodPost:
			var req struct {
				Action    string    `json:"action"`
				Rating    string    `json:"rating"`
				LoggedAt  time.Time `json:"logged_at"`
				LakeLevel *float64  `json:"lake_level,omitempty"`
				Notes     string    `json:"notes"`
			}
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				http.Error(w, "invalid request body", http.StatusBadRequest)
				return
			}
			if req.Action != "in" && req.Action != "out" {
				http.Error(w, "action must be 'in' or 'out'", http.StatusBadRequest)
				return
			}
			if req.Rating != "green" && req.Rating != "yellow" && req.Rating != "red" {
				http.Error(w, "rating must be 'green', 'yellow', or 'red'", http.StatusBadRequest)
				return
			}
			if req.LoggedAt.IsZero() {
				req.LoggedAt = time.Now()
			}

			lakeLevel := 0.0
			unit := "ft"

			if req.LakeLevel != nil {
				lakeLevel = *req.LakeLevel
			} else {
				// Query USGS API for the lake level at this timestamp
				fc, err := client.GetPointContinuous(r.Context(), defaultStationID, defaultParameter, req.LoggedAt)
				if err == nil && len(fc.Features) > 0 {
					p := fc.Features[0].Properties
					lakeLevel = float64(p.Value)
					if p.UnitOfMeasure != "" {
						unit = p.UnitOfMeasure
					}
				} else {
					// Fallback to latest reading
					reply := make(chan Reading, 1)
					queries <- reply
					cur := <-reply
					if cur.Value > 0 {
						lakeLevel = cur.Value
						unit = cur.Unit
					}
				}
			}

			entry := BoatLog{
				Action:    req.Action,
				Rating:    req.Rating,
				LakeLevel: lakeLevel,
				Unit:      unit,
				FullPool:  summerFullPool,
				Delta:     lakeLevel - summerFullPool,
				LoggedAt:  req.LoggedAt,
				Notes:     req.Notes,
			}

			created := boatStore.Add(entry)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusCreated)
			json.NewEncoder(w).Encode(created)

		case http.MethodDelete:
			id := r.URL.Query().Get("id")
			if id == "" {
				http.Error(w, "id parameter is required", http.StatusBadRequest)
				return
			}
			if !boatStore.Delete(id) {
				http.Error(w, "log not found", http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusNoContent)

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}
	})

	mux.HandleFunc("/api/boat-logs/export", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		logs := boatStore.GetAll()
		var buf bytes.Buffer
		cw := csv.NewWriter(&buf)

		// Header
		_ = cw.Write([]string{"id", "action", "rating", "lake_level", "unit", "full_pool", "delta", "logged_at", "notes"})

		for _, l := range logs {
			_ = cw.Write([]string{
				l.ID,
				l.Action,
				l.Rating,
				fmt.Sprintf("%.2f", l.LakeLevel),
				l.Unit,
				fmt.Sprintf("%.2f", l.FullPool),
				fmt.Sprintf("%.2f", l.Delta),
				l.LoggedAt.Format(time.RFC3339),
				l.Notes,
			})
		}
		cw.Flush()

		filename := fmt.Sprintf("boat-lift-logs-%s.csv", time.Now().Format("20060102-150405"))
		w.Header().Set("Content-Type", "text/csv; charset=utf-8")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", filename))
		w.Write(buf.Bytes())
	})

	mux.HandleFunc("/api/boat-logs/import", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}

		// Support both multipart file upload and raw CSV body
		var reader io.Reader = r.Body
		if strings.HasPrefix(r.Header.Get("Content-Type"), "multipart/form-data") {
			if err := r.ParseMultipartForm(10 << 20); err == nil {
				file, _, err := r.FormFile("file")
				if err == nil {
					defer file.Close()
					reader = file
				}
			}
		}

		cr := csv.NewReader(reader)
		cr.FieldsPerRecord = -1
		records, err := cr.ReadAll()
		if err != nil {
			http.Error(w, fmt.Sprintf("failed to parse CSV: %v", err), http.StatusBadRequest)
			return
		}

		if len(records) == 0 {
			http.Error(w, "CSV file is empty", http.StatusBadRequest)
			return
		}

		// Detect header indices for input fields: action, rating, logged_at, notes, id (optional)
		colIdx := map[string]int{
			"id":        -1,
			"action":    -1,
			"rating":    -1,
			"logged_at": -1,
			"date":      -1,
			"time":      -1,
			"datetime":  -1,
			"notes":     -1,
			"note":      -1,
		}

		hasHeader := false
		headerRow := records[0]
		for i, h := range headerRow {
			hClean := strings.ToLower(strings.TrimSpace(h))
			if _, exists := colIdx[hClean]; exists {
				colIdx[hClean] = i
				hasHeader = true
			}
		}

		startRow := 0
		if hasHeader {
			startRow = 1
		} else {
			// default column positions if no named header:
			// action, rating, logged_at, notes
			colIdx["action"] = 0
			colIdx["rating"] = 1
			colIdx["logged_at"] = 2
			colIdx["notes"] = 3
		}

		imported := 0
		for rowIdx := startRow; rowIdx < len(records); rowIdx++ {
			row := records[rowIdx]
			if len(row) == 0 || (len(row) == 1 && strings.TrimSpace(row[0]) == "") {
				continue
			}

			getVal := func(names ...string) string {
				for _, name := range names {
					idx := colIdx[name]
					if idx >= 0 && idx < len(row) {
						v := strings.TrimSpace(row[idx])
						if v != "" {
							return v
						}
					}
				}
				return ""
			}

			action := strings.ToLower(getVal("action"))
			if action != "in" && action != "out" {
				continue // skip invalid records
			}

			rating := strings.ToLower(getVal("rating"))
			if rating != "green" && rating != "yellow" && rating != "red" {
				rating = "green"
			}

			loggedAtStr := getVal("logged_at", "datetime", "date", "time")
			loggedAt := time.Now()
			if loggedAtStr != "" {
				// Try RFC3339, standard date formats
				for _, layout := range []string{
					time.RFC3339,
					"2006-01-02T15:04",
					"2006-01-02 15:04:05",
					"2006-01-02 15:04",
					"2006-01-02",
					"01/02/2006 15:04",
					"01/02/2006",
				} {
					if t, err := time.Parse(layout, loggedAtStr); err == nil {
						loggedAt = t
						break
					}
				}
			}

			// Pool level always retrieved from the USGS API for this point in time
			lakeLevel := 0.0
			unit := "ft"
			fc, err := client.GetPointContinuous(r.Context(), defaultStationID, defaultParameter, loggedAt)
			if err == nil && len(fc.Features) > 0 {
				p := fc.Features[0].Properties
				lakeLevel = float64(p.Value)
				if p.UnitOfMeasure != "" {
					unit = p.UnitOfMeasure
				}
			} else {
				// Fallback to latest reading if point not available
				reply := make(chan Reading, 1)
				queries <- reply
				cur := <-reply
				if cur.Value > 0 {
					lakeLevel = cur.Value
					unit = cur.Unit
				}
			}

			fullPool := summerFullPool
			delta := lakeLevel - fullPool

			notes := getVal("notes", "note")
			id := getVal("id")

			boatStore.Add(BoatLog{
				ID:        id,
				Action:    action,
				Rating:    rating,
				LakeLevel: lakeLevel,
				Unit:      unit,
				FullPool:  fullPool,
				Delta:     delta,
				LoggedAt:  loggedAt,
				Notes:     notes,
			})
			imported++
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]interface{}{
			"imported": imported,
			"total":    len(boatStore.GetAll()),
		})
	})

	return mux
}

// ---- Main ------------------------------------------------------------------

func main() {
	client := NewClient(os.Getenv("USGS_API_KEY"))

	updates := make(chan Reading, 1)
	queries := make(chan chan<- Reading, 1)
	done := make(chan struct{})

	// State goroutine: single owner of the latest reading.
	go stateLoop(updates, queries)

	// Fetch goroutine: polls USGS and pushes readings to state.
	go fetchLoop(client, updates, done)

	// HTTP server.
	boatStore := NewMemoryBoatStore()

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
