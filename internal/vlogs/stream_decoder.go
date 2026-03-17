package vlogs

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
)

// ErrResponseTooLarge is returned by StreamDecoder when the VictoriaLogs
// response body exceeds the configured maxBytes limit.
var ErrResponseTooLarge = errors.New("vlogs response exceeded maximum allowed bytes")

// defaultLineBufferSize is the scanner line buffer: 1 MiB covers the vast
// majority of real-world log lines while bounding per-request memory use.
const defaultLineBufferSize = 1 * 1024 * 1024 // 1 MiB

// StreamDecoder reads an NDJSON stream from r and calls fn for each decoded
// Record. It is the core of the proxy's streaming pipeline.
//
// Guarantees:
//   - Never reads the full body into memory; each line is decoded and
//     immediately forwarded to fn.
//   - Respects ctx cancellation: if the caller's context is cancelled (e.g.
//     Grafana user navigates away), streaming stops immediately.
//   - Enforces maxBytes: if more than maxBytes are consumed from r, streaming
//     stops and ErrResponseTooLarge is returned. This protects against
//     unexpectedly large VL responses causing OOM.
//   - Empty lines are silently skipped (NDJSON permits them).
//   - If fn returns a non-nil error, streaming stops and that error is returned.
func StreamDecoder(ctx context.Context, r io.Reader, maxBytes int64, fn func(Record) error) error {
	// Wrap the reader in a LimitedReader with capacity maxBytes+1.
	// If LimitedReader.N reaches 0 after reading, we consumed all maxBytes+1
	// bytes, which proves the response is larger than maxBytes.
	lr := &io.LimitedReader{R: r, N: maxBytes + 1}

	scanner := bufio.NewScanner(lr)
	scanner.Buffer(make([]byte, defaultLineBufferSize), defaultLineBufferSize)

	for scanner.Scan() {
		// Check context cancellation before processing each record.
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		line := scanner.Bytes()
		if len(line) == 0 {
			continue // skip blank lines
		}

		// Check byte limit: if LimitedReader exhausted its budget the last
		// read returned io.EOF, the scanner will have stopped. But we can
		// also detect it here after the scan in case the limit landed exactly
		// on a line boundary.
		if lr.N == 0 {
			return ErrResponseTooLarge
		}

		var rec Record
		if err := json.Unmarshal(line, &rec); err != nil {
			return fmt.Errorf("decode NDJSON record: %w", err)
		}

		if err := fn(rec); err != nil {
			return err
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan NDJSON stream: %w", err)
	}

	// If the LimitedReader used its full budget the response was truncated.
	if lr.N == 0 {
		return ErrResponseTooLarge
	}

	return nil
}
