FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN mkdir -p /out \
 && go build -o /out/dedup ./cmd/dedup \
 && go build -o /out/keyword ./cmd/keyword \
 && go build -o /out/pipeline ./cmd/pipeline \
 && go build -o /out/arxiv-watcher ./cmd/arxiv-watcher \
 && go build -o /out/rss-watcher ./cmd/rss-watcher \
 && go build -o /out/api ./cmd/api \
 && go build -o /out/worker ./cmd/worker

FROM alpine:3.20
WORKDIR /app
RUN adduser -D appuser
RUN mkdir -p /data/pdfs && chown -R appuser /data
COPY --from=build /out/* /app/
COPY web /app/web
USER appuser
