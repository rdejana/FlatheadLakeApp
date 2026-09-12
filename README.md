# FlatheadLakeApp

A lightweight web application to track Flathead Lake water levels and log boat launch/retrieval operations relative to summer full pool (2,893.00 ft). Water level data is sourced from the USGS Water Data API.

## Features

- **Real-Time Monitoring**: Current lake level and delta from full pool.
- **Historical Trends**: Interactive water level charts over selectable time ranges.
- **Boat Lift Logs**: Track boat put-in / take-out actions, rating conditions (green/yellow/red), notes, and automatic water level association at time of log.
- **CSV Export & Import**: Export and bulk import boat lift logs.
- **SQLite Persistence**: Stores boat lift logs in SQLite with automatic fallback to an in-memory store if SQLite is unavailable.

---

## Configuration

The application is configured using environment variables:

| Variable | Default | Description |
|---|---|---|
| `DB_PATH` | `boat_tracker.db` | File path for SQLite database. |
| `USGS_API_KEY` | *(empty)* | Optional USGS Water Data API key. |

---

## Running Locally

```bash
# Run tests
go test ./...

# Start the application
go run .
```

The application will be available at `http://localhost:8080`.

---

## Running with Docker / Podman

### 1. Build the Image

```bash
docker build -t flathead-lake-app -f Containerfile .
```

### 2. Run with Host SQLite Persistence

Mounting a host directory to store the SQLite database ensures data persists across container restarts:

```bash
# Create directory on host
mkdir -p data

# Run container with volume mount
docker run -d \
  --name flathead-lake-app \
  -p 8080:8080 \
  -v "$(pwd)/data:/app/data" \
  -e DB_PATH=/app/data/boat_tracker.db \
  -e USGS_API_KEY="your-optional-api-key" \
  flathead-lake-app
```

> **Note for Podman / SELinux (Fedora/RHEL/CentOS):**
> Append `:Z` to the volume mount: `-v "$(pwd)/data:/app/data:Z"`

### Alternative: Single File Mount

```bash
touch "$(pwd)/boat_tracker.db"

docker run -d \
  --name flathead-lake-app \
  -p 8080:8080 \
  -v "$(pwd)/boat_tracker.db:/app/boat_tracker.db" \
  flathead-lake-app
```
