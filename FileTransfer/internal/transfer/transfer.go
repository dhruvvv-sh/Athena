// Package transfer holds the chunking, checksum and metadata-handshake logic shared by
// workers. A transfer is planned into fixed-size chunks; a manifest (metadata + checksums)
// is exchanged with the receiver before any bytes move; then each chunk is streamed and
// verified. This mirrors a multi-part upload.
package transfer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"filetransfer/internal/model"
)

// ChunkPlan describes one chunk's position and (once read) its checksum.
type ChunkPlan struct {
	Seq      int    `json:"seq"`
	Offset   int64  `json:"offset"`
	Size     int64  `json:"size"`
	Checksum string `json:"checksum"` // sha-256 hex, filled once the chunk is read
}

// Manifest is the metadata file shared with the receiver before transfer. The receiver
// ACKs it (agreeing on size/chunking/checksums) before the sender streams chunks.
type Manifest struct {
	TransferID string      `json:"transfer_id"`
	FlowID     string      `json:"flow_id,omitempty"`
	Source     string      `json:"source"`
	Target     string      `json:"target"`
	SizeBytes  int64       `json:"size_bytes"`
	ChunkSize  int64       `json:"chunk_size"`
	Checksum   string      `json:"checksum"` // whole-file sha-256 hex
	Chunks     []ChunkPlan `json:"chunks"`
}

// Kind classifies a source/target URI as a local file path or an s3://bucket/key URI.
func Kind(uri string) string {
	if strings.HasPrefix(strings.ToLower(uri), "s3://") {
		return model.KindS3
	}
	return model.KindFile
}

// PlanChunks divides a file of size bytes into ceil(size/chunkSize) chunks.
func PlanChunks(size, chunkSize int64) []ChunkPlan {
	if chunkSize <= 0 {
		chunkSize = 8 << 20
	}
	var plans []ChunkPlan
	seq := 0
	for off := int64(0); off < size; off += chunkSize {
		sz := chunkSize
		if off+sz > size {
			sz = size - off
		}
		plans = append(plans, ChunkPlan{Seq: seq, Offset: off, Size: sz})
		seq++
	}
	if size == 0 { // an empty file is still one zero-length chunk
		plans = append(plans, ChunkPlan{Seq: 0, Offset: 0, Size: 0})
	}
	return plans
}

// BuildManifest reads the (local-file) source, computing the whole-file checksum and each
// chunk's checksum. S3 sources are a TODO.
func BuildManifest(t *model.Transfer) (*Manifest, error) {
	if Kind(t.Source) != model.KindFile {
		return nil, fmt.Errorf("BuildManifest: source kind %q not yet supported", Kind(t.Source))
	}
	f, err := os.Open(t.Source)
	if err != nil {
		return nil, fmt.Errorf("open source: %w", err)
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return nil, err
	}
	size := fi.Size()
	plans := PlanChunks(size, t.ChunkSize)

	whole := sha256.New()
	buf := make([]byte, 0, t.ChunkSize)
	if cap(buf) == 0 {
		buf = make([]byte, 8<<20)
	} else {
		buf = buf[:cap(buf)]
	}
	for i := range plans {
		n := plans[i].Size
		h := sha256.New()
		if _, err := io.CopyBuffer(io.MultiWriter(h, whole), io.LimitReader(f, n), buf); err != nil {
			return nil, fmt.Errorf("hash chunk %d: %w", plans[i].Seq, err)
		}
		plans[i].Checksum = hex.EncodeToString(h.Sum(nil))
	}
	return &Manifest{
		TransferID: t.ID,
		FlowID:     t.FlowID,
		Source:     t.Source,
		Target:     t.Target,
		SizeBytes:  size,
		ChunkSize:  t.ChunkSize,
		Checksum:   hex.EncodeToString(whole.Sum(nil)),
		Chunks:     plans,
	}, nil
}

// ProgressFn is called after each chunk is written on the target side.
type ProgressFn func(ch ChunkPlan) error

// Execute performs the transfer for the local-file → local-file case (the MVP path):
// it streams each planned chunk from source to target, verifying the per-chunk checksum,
// and invokes progress after each chunk. Returns the assembled file's checksum.
//
// Cross-cluster streaming (sender node → receiver node over the load balancer, with the
// receiver assembling chunks that may arrive at different nodes) is the next build-out and
// is described in the README; S3 targets use multi-part upload (TODO).
func Execute(ctx context.Context, m *Manifest, progress ProgressFn) (string, error) {
	if Kind(m.Source) != model.KindFile || Kind(m.Target) != model.KindFile {
		return "", fmt.Errorf("Execute: only file→file is implemented (got %s→%s)", Kind(m.Source), Kind(m.Target))
	}
	src, err := os.Open(m.Source)
	if err != nil {
		return "", fmt.Errorf("open source: %w", err)
	}
	defer src.Close()

	if dir := filepath.Dir(m.Target); dir != "" {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return "", fmt.Errorf("create target dir: %w", err)
		}
	}
	dst, err := os.Create(m.Target)
	if err != nil {
		return "", fmt.Errorf("create target: %w", err)
	}
	defer dst.Close()

	whole := sha256.New()
	buf := make([]byte, 32<<10)
	for _, ch := range m.Chunks {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
		h := sha256.New()
		w := io.MultiWriter(dst, whole, h)
		if _, err := io.CopyBuffer(w, io.LimitReader(src, ch.Size), buf); err != nil {
			return "", fmt.Errorf("copy chunk %d: %w", ch.Seq, err)
		}
		if got := hex.EncodeToString(h.Sum(nil)); ch.Checksum != "" && got != ch.Checksum {
			return "", fmt.Errorf("chunk %d checksum mismatch: want %s got %s", ch.Seq, ch.Checksum, got)
		}
		if progress != nil {
			if err := progress(ch); err != nil {
				return "", err
			}
		}
	}
	if err := dst.Sync(); err != nil {
		return "", err
	}
	final := hex.EncodeToString(whole.Sum(nil))
	if m.Checksum != "" && final != m.Checksum {
		return "", fmt.Errorf("assembled checksum mismatch: want %s got %s", m.Checksum, final)
	}
	return final, nil
}
