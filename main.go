package main

import (
	"context"
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

const baseURL = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/latest-continuous/items"

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
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, err
	}

	q := u.Query()
	q.Set("monitoring_location_id", locationID)
	q.Set("parameter_code", parameterCode)
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
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

func main() {
	client := NewClient(os.Getenv("USGS_API_KEY"))

	ticker := time.NewTicker(10 * time.Minute)
	done := make(chan bool)
	fetch := make(chan struct{}, 1)
	fetch <- struct{}{} // trigger an immediate fetch on startup

	fetchData := func() {
		resp, err := client.GetLatestContinuous(
			context.Background(),
			"USGS-12371550",
			"00062",
		)
		if err != nil {
			log.Printf("Error getting latest continuous items: %v", err)
			return
		}
		fmt.Printf("[%s] Retrieved %d result(s)\n", time.Now().Format("01/02/06 03:04:05 PM"), resp.NumberReturned)
		for _, feature := range resp.Features {
			p := feature.Properties
			fmt.Printf("Station: %s\n", p.MonitoringLocationID)
			fmt.Printf("Value: %.2f %s\n", float64(p.Value), p.UnitOfMeasure)
			fmt.Printf("Time: %s\n", p.Time.Format(time.RFC3339))
		}
	}

	go func() {
		for {
			select {
			case <-done:
				return
			case <-fetch:
				fetchData()
			case <-ticker.C:
				fetchData()
			}
		}
	}()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	fmt.Println("Application running... press Ctrl+C or send SIGTERM to stop.")
	<-sigs
	fmt.Println()

	ticker.Stop()
	done <- true
	fmt.Println("Application stopped")

}
