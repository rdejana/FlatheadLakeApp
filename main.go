package main

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strconv"
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

func newMux(client *Client, queries chan<- chan<- Reading) *http.ServeMux {
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
	srv := &http.Server{
		Addr:    ":8080",
		Handler: newMux(client, queries),
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
