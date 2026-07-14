// Package worker runs a worker node: it registers with the master (or the load balancer
// in front of it), atomically claims transfer tasks, and executes them in chunks.
package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/google/uuid"

	"filetransfer/internal/apps"
	"filetransfer/internal/config"
	"filetransfer/internal/flows"
	"filetransfer/internal/logger"
	"filetransfer/internal/model"
	"filetransfer/internal/transfer"
)

type Worker struct {
	cfg    *config.Config
	nodeID string
	master string
	client *http.Client
	apps   apps.Registry // application name -> endpoint (resolved from a flow's app names)
	flows  *flows.Set
}

// Run starts the worker's claim/execute loop until ctx is cancelled.
func Run(ctx context.Context, cfg *config.Config) error {
	if err := logger.Init(cfg, "worker"); err != nil {
		return err
	}
	nodeID := cfg.Worker.NodeID
	if nodeID == "" {
		nodeID = "worker-" + uuid.NewString()[:8]
	}
	client := &http.Client{Timeout: 30 * time.Second}
	// When talking to an HTTPS master, trust the CA(s) in the configured trust store and
	// present our client certificate (required when the master enforces mTLS).
	if strings.HasPrefix(strings.ToLower(cfg.Worker.MasterURL), "https://") {
		tlsConf := &tls.Config{MinVersion: tls.VersionTLS12}
		if pool, err := loadCAPool(cfg.TLS.CADir); err != nil {
			log.Printf("worker: trust store: %v (falling back to system roots)", err)
		} else {
			tlsConf.RootCAs = pool
		}
		if cfg.TLS.ClientCert != "" {
			if cert, err := tls.LoadX509KeyPair(cfg.TLS.ClientCert, cfg.TLS.ClientKey); err != nil {
				log.Printf("worker: client cert (%s): %v", cfg.TLS.ClientCert, err)
			} else {
				tlsConf.Certificates = []tls.Certificate{cert}
			}
		}
		client.Transport = &http.Transport{TLSClientConfig: tlsConf}
	}
	// Load the application registry so the worker can map a flow's application names to
	// their endpoints (a flow refers to applications only by name).
	appReg, err := apps.Load(cfg.Paths.AppsFile)
	if err != nil {
		return fmt.Errorf("applications: %w", err)
	}
	flowSet, err := flows.Load(cfg.Paths.FlowsDir, filepath.Dir(cfg.Home))
	if err != nil {
		return fmt.Errorf("flows: %w", err)
	}
	w := &Worker{
		cfg:    cfg,
		nodeID: nodeID,
		master: cfg.Worker.MasterURL,
		client: client,
		apps:   appReg,
		flows:  flowSet,
	}
	log.Printf("worker %s: master=%s poll=%ds apps=%d flows=%d", w.nodeID, w.master, cfg.Worker.PollInterval, len(appReg), flowSet.Count())

	if err := w.register(ctx); err != nil {
		log.Printf("worker %s: register failed (will retry): %v", w.nodeID, err)
	}
	// Keep this node's registration fresh so the master lists it as active; when the
	// worker stops, its last_seen goes stale and the master drops it from the UI.
	go w.nodeHeartbeat(ctx)

	poll := time.Duration(cfg.Worker.PollInterval) * time.Second
	for {
		select {
		case <-ctx.Done():
			log.Printf("worker %s: shutting down", w.nodeID)
			return nil
		default:
		}
		worked, err := w.claimAndRun(ctx)
		if err != nil {
			log.Printf("worker %s: %v", w.nodeID, err)
		}
		if !worked {
			select {
			case <-ctx.Done():
				return nil
			case <-time.After(poll):
			}
		}
	}
}

func (w *Worker) register(ctx context.Context) error {
	return w.post(ctx, "/api/nodes/register", model.Node{ID: w.nodeID, Role: "worker"}, nil)
}

// nodeHeartbeat periodically re-registers so the master keeps this node marked active.
func (w *Worker) nodeHeartbeat(ctx context.Context) {
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = w.register(ctx)
		}
	}
}

// loadCAPool builds a trust store from every *.crt / *.pem file in dir.
func loadCAPool(dir string) (*x509.CertPool, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	pool := x509.NewCertPool()
	added := 0
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		ext := strings.ToLower(filepath.Ext(e.Name()))
		if ext != ".crt" && ext != ".pem" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(dir, e.Name()))
		if err != nil {
			continue
		}
		if pool.AppendCertsFromPEM(b) {
			added++
		}
	}
	if added == 0 {
		return nil, fmt.Errorf("no CA certificates found in %s", dir)
	}
	return pool, nil
}

// claimAndRun claims one task and executes it. Returns worked=true if a task was handled.
func (w *Worker) claimAndRun(ctx context.Context) (bool, error) {
	var claim struct {
		Task     *model.Task     `json:"task"`
		Transfer *model.Transfer `json:"transfer"`
	}
	status, err := w.postRaw(ctx, "/api/tasks/claim", map[string]string{"node_id": w.nodeID}, &claim)
	if err != nil {
		return false, fmt.Errorf("claim: %w", err)
	}
	if status == http.StatusNoContent || claim.Task == nil {
		return false, nil // nothing to do
	}
	t := claim.Transfer
	log.Printf("worker %s: claimed task %d — %s → %s", w.nodeID, claim.Task.ID, t.Source, t.Target)

	// Heartbeat in the background while the transfer runs.
	hbCtx, stopHB := context.WithCancel(ctx)
	go w.heartbeat(hbCtx, claim.Task.ID)
	defer stopHB()

	if err := w.execute(ctx, claim.Task, t); err != nil {
		log.Printf("worker %s: task %d failed: %v", w.nodeID, claim.Task.ID, err)
		_ = w.post(ctx, fmt.Sprintf("/api/tasks/%d/complete", claim.Task.ID),
			map[string]any{"transfer_id": t.ID, "ok": false, "error": err.Error()}, nil)
		return true, nil
	}
	return true, nil
}

func (w *Worker) execute(ctx context.Context, task *model.Task, t *model.Transfer) error {
	// 1. Build the manifest (size, per-chunk + whole-file checksums) from the source.
	m, err := transfer.BuildManifest(t)
	if err != nil {
		return fmt.Errorf("build manifest: %w", err)
	}
	// 2. Share the manifest with the master (receiver ACK point). Any receiver node can
	//    then take part in assembly because chunk state is in Postgres.
	if err := w.post(ctx, fmt.Sprintf("/api/tasks/%d/manifest", task.ID), m, nil); err != nil {
		return fmt.Errorf("upload manifest: %w", err)
	}
	if endpoint, remote := w.remoteEndpoint(t); remote {
		checksum, err := w.executeRemote(ctx, task, t, m, endpoint)
		if err != nil {
			return err
		}
		if err := w.post(ctx, fmt.Sprintf("/api/tasks/%d/complete", task.ID),
			map[string]any{"transfer_id": t.ID, "ok": true, "checksum": checksum}, nil); err != nil {
			return fmt.Errorf("complete: %w", err)
		}
		log.Printf("worker %s: remote task %d complete — checksum %s", w.nodeID, task.ID, checksum)
		logger.Transfer("SENT     id=%s node=%s bytes=%d chunks=%d checksum=%s remote=%s", t.ID, w.nodeID, m.SizeBytes, len(m.Chunks), checksum, endpoint)
		return nil
	}
	// 3. Stream chunks, reporting each chunk's status as it lands.
	checksum, err := transfer.Execute(ctx, m, func(ch transfer.ChunkPlan) error {
		return w.post(ctx, fmt.Sprintf("/api/tasks/%d/progress", task.ID), map[string]any{
			"transfer_id": t.ID, "seq": ch.Seq, "status": model.ChunkWritten, "node_id": w.nodeID,
		}, nil)
	})
	if err != nil {
		return err
	}
	// 4. Final ACK with the assembled checksum.
	if err := w.post(ctx, fmt.Sprintf("/api/tasks/%d/complete", task.ID),
		map[string]any{"transfer_id": t.ID, "ok": true, "checksum": checksum}, nil); err != nil {
		return fmt.Errorf("complete: %w", err)
	}
	log.Printf("worker %s: task %d complete — checksum %s", w.nodeID, task.ID, checksum)
	logger.Transfer("SENT     id=%s node=%s bytes=%d chunks=%d checksum=%s", t.ID, w.nodeID, m.SizeBytes, len(m.Chunks), checksum)
	return nil
}

func (w *Worker) remoteEndpoint(t *model.Transfer) (string, bool) {
	if t.FlowID == "" || w.flows == nil {
		return "", false
	}
	f := w.flows.ByID(t.FlowID)
	if f == nil {
		return "", false
	}
	self := filepath.Base(w.cfg.Home)
	if strings.EqualFold(f.Receiver.Application, self) {
		return "", false
	}
	endpoint := strings.TrimRight(w.apps.Endpoint(f.Receiver.Application), "/")
	if endpoint == "" || strings.EqualFold(endpoint, strings.TrimRight(w.master, "/")) {
		return "", false
	}
	return endpoint, true
}

func (w *Worker) executeRemote(ctx context.Context, task *model.Task, t *model.Transfer, m *transfer.Manifest, endpoint string) (string, error) {
	var ack struct {
		Chunks []model.Chunk `json:"chunks"`
	}
	if err := w.postTo(ctx, endpoint, "/api/receive/manifest", m, &ack); err != nil {
		return "", fmt.Errorf("remote manifest: %w", err)
	}
	done := map[int]bool{}
	for _, ch := range ack.Chunks {
		if ch.Status == model.ChunkWritten || ch.Status == model.ChunkAcked {
			done[ch.Seq] = true
		}
	}
	src, err := os.Open(t.Source)
	if err != nil {
		return "", fmt.Errorf("open source: %w", err)
	}
	defer src.Close()
	for _, ch := range m.Chunks {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		default:
		}
		if done[ch.Seq] {
			if err := w.post(ctx, fmt.Sprintf("/api/tasks/%d/progress", task.ID), map[string]any{
				"transfer_id": t.ID, "seq": ch.Seq, "status": model.ChunkAcked, "node_id": w.nodeID,
			}, nil); err != nil {
				return "", err
			}
			continue
		}
		if _, err := src.Seek(ch.Offset, io.SeekStart); err != nil {
			return "", fmt.Errorf("seek chunk %d: %w", ch.Seq, err)
		}
		path := fmt.Sprintf("/api/receive/chunks/%s/%d", t.ID, ch.Seq)
		if err := w.putChunk(ctx, endpoint, path, io.LimitReader(src, ch.Size)); err != nil {
			return "", fmt.Errorf("remote chunk %d: %w", ch.Seq, err)
		}
		if err := w.post(ctx, fmt.Sprintf("/api/tasks/%d/progress", task.ID), map[string]any{
			"transfer_id": t.ID, "seq": ch.Seq, "status": model.ChunkAcked, "node_id": w.nodeID,
		}, nil); err != nil {
			return "", err
		}
	}
	var out struct {
		Checksum string `json:"checksum"`
	}
	if err := w.postTo(ctx, endpoint, fmt.Sprintf("/api/receive/%s/complete", t.ID), map[string]string{"node_id": w.nodeID}, &out); err != nil {
		return "", fmt.Errorf("remote complete: %w", err)
	}
	if out.Checksum == "" {
		out.Checksum = m.Checksum
	}
	return out.Checksum, nil
}

func (w *Worker) heartbeat(ctx context.Context, taskID int64) {
	t := time.NewTicker(20 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = w.post(ctx, fmt.Sprintf("/api/tasks/%d/heartbeat", taskID), map[string]string{"node_id": w.nodeID}, nil)
		}
	}
}

// ── HTTP helpers ──

func (w *Worker) post(ctx context.Context, path string, body, out any) error {
	_, err := w.postRaw(ctx, path, body, out)
	return err
}

func (w *Worker) postTo(ctx context.Context, base, path string, body, out any) error {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(base, "/")+path, &buf)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-FT-Node-ID", w.nodeID)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, bytes.TrimSpace(b))
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
			return err
		}
	}
	return nil
}

func (w *Worker) putChunk(ctx context.Context, base, path string, body io.Reader) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, strings.TrimRight(base, "/")+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/octet-stream")
	req.Header.Set("X-FT-Node-ID", w.nodeID)
	resp, err := w.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, bytes.TrimSpace(b))
	}
	return nil
}

func (w *Worker) postRaw(ctx context.Context, path string, body, out any) (int, error) {
	var buf bytes.Buffer
	if err := json.NewEncoder(&buf).Encode(body); err != nil {
		return 0, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, w.master+path, &buf)
	if err != nil {
		return 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := w.client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 400 {
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, fmt.Errorf("%s -> %d: %s", path, resp.StatusCode, bytes.TrimSpace(b))
	}
	if out != nil && resp.StatusCode != http.StatusNoContent {
		if err := json.NewDecoder(resp.Body).Decode(out); err != nil && err != io.EOF {
			return resp.StatusCode, err
		}
	}
	return resp.StatusCode, nil
}
