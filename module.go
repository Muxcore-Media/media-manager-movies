package movies

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/Muxcore-Media/core/pkg/contracts"
	"github.com/google/uuid"
)

// RequestPayload is the JSON payload for media.requested events.
type RequestPayload struct {
	MediaType string `json:"media_type"`
	Title     string `json:"title"`
	Year      int    `json:"year,omitempty"`
	TmdbID    string `json:"tmdb_id,omitempty"`
	Quality   string `json:"quality,omitempty"`
}

type Module struct {
	reg contracts.ServiceRegistry
	bus contracts.EventBus
	mu  sync.RWMutex
	requests map[string]string
}

func NewModule(reg contracts.ServiceRegistry, bus contracts.EventBus) *Module {
	return &Module{
		reg:      reg,
		bus:      bus,
		requests: make(map[string]string),
	}
}

func (m *Module) Info() contracts.ModuleInfo {
	return contracts.ModuleInfo{
		ID:           "media-manager-movies",
		Name:         "Movie Manager",
		Version:      "1.0.0",
		Kinds:        []contracts.ModuleKind{contracts.ModuleKindMediaManager},
		Description:  "Handles movie media requests, search, and download coordination",
		Author:       "MuxCore",
		Capabilities: []string{"media.movie", "media.search", "media.request"},
	}
}

func (m *Module) Init(ctx context.Context) error {
	if err := m.reg.RegisterMediaSchema(m.MediaTypeSchema()); err != nil {
		return fmt.Errorf("register movie schema: %w", err)
	}
	return nil
}

func (m *Module) MediaTypeSchema() contracts.MediaTypeSchema {
	return contracts.MediaTypeSchema{
		MediaType: contracts.MediaTypeMovie,
		ModuleID:  m.Info().ID,
		Fields: []contracts.MediaFieldSchema{
			{Key: "tmdb_id", Type: contracts.FieldTypeString, Description: "TheMovieDB ID"},
			{Key: "imdb_id", Type: contracts.FieldTypeString, Description: "IMDB ID"},
			{Key: "year", Type: contracts.FieldTypeInt, Description: "Release year"},
			{Key: "quality", Type: contracts.FieldTypeString, Description: "Quality profile used for the request"},
		},
	}
}

func (m *Module) Start(ctx context.Context) error {
	err := m.bus.Subscribe(ctx, contracts.EventMediaRequested, m.handleMediaRequested)
	if err != nil {
		return fmt.Errorf("subscribe media.requested: %w", err)
	}
	slog.Info("movie manager listening for media requests")
	return nil
}

func (m *Module) Stop(ctx context.Context) error {
	return m.bus.Unsubscribe(ctx, contracts.EventMediaRequested, m.handleMediaRequested)
}

func (m *Module) Health(ctx context.Context) error { return nil }

func (m *Module) handleMediaRequested(ctx context.Context, event contracts.Event) error {
	var payload RequestPayload
	if err := json.Unmarshal(event.Payload, &payload); err != nil {
		slog.Error("invalid media request payload", "error", err)
		return fmt.Errorf("invalid payload: %w", err)
	}

	if payload.MediaType != string(contracts.MediaTypeMovie) {
		return nil // not our media type
	}

	requestID := uuid.New().String()
	m.mu.Lock()
	m.requests[requestID] = "searching"
	m.mu.Unlock()

	slog.Info("movie requested", "request_id", requestID, "title", payload.Title, "year", payload.Year)

	// 1. Search indexers
	results, err := m.searchIndexers(ctx, payload)
	if err != nil {
		slog.Error("indexer search failed", "request_id", requestID, "error", err)
		return fmt.Errorf("search indexers: %w", err)
	}
	if len(results) == 0 {
		slog.Warn("no results found", "request_id", requestID, "title", payload.Title)
		return nil
	}

	// 2. Pick best result (preferring most seeders on first match)
	best := results[0]
	slog.Info("best result chosen", "request_id", requestID, "title", best.Title, "seeders", best.Seeders)

	// 3. Submit to downloader
	dlID, err := m.startDownload(ctx, best)
	if err != nil {
		slog.Error("download start failed", "request_id", requestID, "error", err)
		return fmt.Errorf("start download: %w", err)
	}

	m.mu.Lock()
	m.requests[requestID] = "downloading"
	m.mu.Unlock()

	// 4. Emit download started event
	dlEvent := contracts.Event{
		ID:     uuid.New().String(),
		Type:   contracts.EventDownloadStarted,
		Source: "media-manager-movies",
		Payload: mustMarshal(map[string]string{
			"request_id":  requestID,
			"download_id": dlID,
			"title":       best.Title,
		}),
		Metadata: map[string]string{
			"indexer":    best.Source,
			"media_type": "movie",
		},
	}
	if err := m.bus.Publish(ctx, dlEvent); err != nil {
		slog.Error("publish download.started failed", "error", err)
	}

	return nil
}

func (m *Module) searchIndexers(ctx context.Context, payload RequestPayload) ([]contracts.IndexerResult, error) {
	entries := m.reg.FindByKind(contracts.ModuleKindIndexer)
	if len(entries) == 0 {
		return nil, fmt.Errorf("no indexer modules registered")
	}

	query := contracts.SearchQuery{
		Query:      payload.Title,
		Type:       "movie",
		Categories: []string{"movie"},
		Limit:      10,
	}
	if payload.Year > 0 {
		query.Year = payload.Year
	}

	var allResults []contracts.IndexerResult
	for _, entry := range entries {
		indexer, ok := entry.Module.(contracts.Indexer)
		if !ok {
			continue
		}
		results, err := indexer.Search(ctx, query)
		if err != nil {
			slog.Warn("indexer search error", "indexer", entry.Info.ID, "error", err)
			continue
		}
		allResults = append(allResults, results...)
	}
	return allResults, nil
}

func (m *Module) startDownload(ctx context.Context, result contracts.IndexerResult) (string, error) {
	entries := m.reg.FindByKind(contracts.ModuleKindDownloader)
	if len(entries) == 0 {
		return "", fmt.Errorf("no downloader modules registered")
	}

	task := contracts.DownloadTask{
		MagnetURI:  result.MagnetURI,
		TorrentURL: result.Link,
		Label:      "movies",
	}

	for _, entry := range entries {
		dl, ok := entry.Module.(contracts.Downloader)
		if !ok {
			continue
		}
		id, err := dl.Add(ctx, task)
		if err != nil {
			slog.Warn("downloader add error", "downloader", entry.Info.ID, "error", err)
			continue
		}
		return id, nil
	}
	return "", fmt.Errorf("all downloaders failed")
}

func mustMarshal(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}
