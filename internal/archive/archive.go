// Package archive exports events to object storage before the pruner
// deletes them, so pruned ranges remain recoverable.
package archive

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/klauspost/compress/gzip"
	"github.com/sorotrail/sorotrail/internal/store"
)

// Manifest records metadata about one archived ledger-range chunk.
type Manifest struct {
	SchemaVersion int       `json:"schema_version"`
	FromLedger    int64     `json:"from_ledger"`
	ToLedger      int64     `json:"to_ledger"`
	EventCount    int       `json:"event_count"`
	FirstEventID  string    `json:"first_event_id"`
	LastEventID   string    `json:"last_event_id"`
	ArchivedAt    time.Time `json:"archived_at"`
	Producer      string    `json:"producer"`
}

// Archiver exports events before deletion.
type Archiver interface {
	ArchiveEvents(ctx context.Context, events []store.Event, fromLedger, toLedger int64) (objectURI string, manifest Manifest, err error)
}

// Options configures a filesystem archiver.
type Options struct {
	Dir    string
	Prefix string
}

// FSArchiver writes compressed NDJSON to a local directory tree.
type FSArchiver struct {
	dir    string
	prefix string
	log    *slog.Logger
	mu     sync.Mutex
}

// NewFS creates a filesystem archiver.
func NewFS(opts Options, log *slog.Logger) *FSArchiver {
	return &FSArchiver{
		dir:    opts.Dir,
		prefix: opts.Prefix,
		log:    log.With("component", "archive"),
	}
}

// ArchiveEvents writes events as gzip-compressed NDJSON.
func (a *FSArchiver) ArchiveEvents(_ context.Context, events []store.Event, fromLedger, toLedger int64) (string, Manifest, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(events) == 0 {
		return "", Manifest{}, nil
	}

	manifest := Manifest{
		SchemaVersion: 1,
		FromLedger:    fromLedger,
		ToLedger:      toLedger,
		EventCount:    len(events),
		FirstEventID:  events[0].ID,
		LastEventID:   events[len(events)-1].ID,
		ArchivedAt:    time.Now().UTC(),
		Producer:      "sorotrail",
	}

	chunkDir := filepath.Join(a.dir, a.prefix, "events", "schema=v1",
		fmt.Sprintf("ledger_start=%07d", fromLedger))
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return "", manifest, fmt.Errorf("creating archive dir: %w", err)
	}

	payloadPath := filepath.Join(chunkDir, "data.ndjson.gz")
	if err := a.writeNDJSONGzip(payloadPath, events); err != nil {
		return "", manifest, fmt.Errorf("writing NDJSON: %w", err)
	}

	manifestPath := filepath.Join(chunkDir, "manifest.json")
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", manifest, fmt.Errorf("marshalling manifest: %w", err)
	}
	if err := os.WriteFile(manifestPath, manifestJSON, 0o644); err != nil {
		return "", manifest, fmt.Errorf("writing manifest: %w", err)
	}

	objectURI := filepath.Join(a.prefix, "events", "schema=v1",
		fmt.Sprintf("ledger_start=%07d", fromLedger), "data.ndjson.gz")

	a.log.Info("archived events",
		"from_ledger", fromLedger,
		"to_ledger", toLedger,
		"event_count", len(events),
		"object_uri", objectURI,
	)

	return objectURI, manifest, nil
}

func (a *FSArchiver) writeNDJSONGzip(path string, events []store.Event) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gz := gzip.NewWriter(f)
	defer gz.Close()

	enc := json.NewEncoder(gz)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			return fmt.Errorf("encoding event %s: %w", e.ID, err)
		}
	}
	return nil
}
