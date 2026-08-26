// Package archive exports events to object storage before the pruner
// deletes them, so pruned ranges remain recoverable. The interface is
// deliberately minimal: callers hand over a batch of events and a
// ledger range, and the implementation writes compressed NDJSON to
// whatever backend is configured (local filesystem, S3, etc.).
//
// Archival is optional — without ARCHIVE_ENABLED the pruner deletes
// directly. When enabled, each batch is written before deletion so
// a crash mid-prune never loses data that was supposed to be archived.
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
// It is persisted alongside the compressed NDJSON so consumers can
// locate and verify archived data without reading the payload.
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

// Archiver is the minimal interface the pruner uses to export events
// before deleting them. Implementations must be safe for concurrent
// use from a single goroutine (the pruner runs on one goroutine).
type Archiver interface {
	// ArchiveEvents writes the given events to a compressed NDJSON file
	// for the inclusive ledger range [fromLedger, toLedger]. The manifest
	// is written alongside the payload. Returns the object URI (a path
	// or URL the caller can use to locate the archived data).
	ArchiveEvents(ctx context.Context, events []store.Event, fromLedger, toLedger int64) (objectURI string, manifest Manifest, err error)
}

// Options configures a filesystem-based archiver.
type Options struct {
	// Dir is the root directory where archived chunks are written.
	// Each chunk is stored under Dir/events/schema=v1/ledger_start=$N/.
	Dir string
	// Prefix is prepended to the directory path within Dir (e.g.
	// "sorotrail/"). Defaults to empty.
	Prefix string
}

// FSArchiver writes compressed NDJSON to a local directory tree.
// It implements Archiver and is suitable for single-instance
// deployments or testing. For multi-instance or cloud deployments,
// replace with an S3-backed implementation that satisfies the same
// interface.
type FSArchiver struct {
	dir    string
	prefix string
	log    *slog.Logger

	// mu serialises writes so two concurrent ArchiveEvents calls
	// don't interleave partial files.
	mu sync.Mutex
}

// NewFS creates a filesystem archiver rooted at opts.Dir.
func NewFS(opts Options, log *slog.Logger) *FSArchiver {
	return &FSArchiver{
		dir:    opts.Dir,
		prefix: opts.Prefix,
		log:    log.With("component", "archive"),
	}
}

// ArchiveEvents writes events as gzip-compressed NDJSON to the
// directory tree under the configured root. The object URI is the
// path to the gzip file relative to the archive root.
func (a *FSArchiver) ArchiveEvents(_ context.Context, events []store.Event, fromLedger, toLedger int64) (string, Manifest, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if len(events) == 0 {
		return "", Manifest{}, nil
	}

	// Build manifest.
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

	// Determine output path.
	chunkDir := filepath.Join(a.dir, a.prefix, "events", "schema=v1",
		fmt.Sprintf("ledger_start=%07d", fromLedger))
	if err := os.MkdirAll(chunkDir, 0o755); err != nil {
		return "", manifest, fmt.Errorf("creating archive dir: %w", err)
	}

	// Write compressed NDJSON payload.
	payloadPath := filepath.Join(chunkDir, "data.ndjson.gz")
	if err := a.writeNDJSONGzip(payloadPath, events); err != nil {
		return "", manifest, fmt.Errorf("writing NDJSON: %w", err)
	}

	// Write manifest.
	manifestPath := filepath.Join(chunkDir, "manifest.json")
	manifestJSON, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return "", manifest, fmt.Errorf("marshalling manifest: %w", err)
	}
	if err := os.WriteFile(manifestPath, manifestJSON, 0o644); err != nil {
		return "", manifest, fmt.Errorf("writing manifest: %w", err)
	}

	// Object URI is relative to the archive root.
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

// writeNDJSONGzip writes events as newline-delimited JSON, compressed
// with gzip. Each line is one JSON-serialized event.
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
