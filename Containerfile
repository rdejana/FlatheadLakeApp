# ---- Build stage -----------------------------------------------------------
FROM registry.access.redhat.com/hi/go:latest AS builder

WORKDIR /build

COPY go.mod .
RUN go mod download

COPY . .
RUN rm -f boat_tracker.db boat_tracker.db*
RUN mkdir /data && chmod 777 /data

RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o pool .

# ---- App stage -------------------------------------------------------------
FROM registry.access.redhat.com/hi/static:latest

WORKDIR /app

COPY --from=builder /build/pool /app/pool
COPY --from=builder /data /data

ENV DB_PATH=/data/boat_tracker.db

EXPOSE 8080

ENTRYPOINT ["/app/pool"]
