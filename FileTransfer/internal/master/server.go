// Package master runs the tracking UI, REST API and task scheduler.
package master

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"filetransfer/internal/apps"
	"filetransfer/internal/config"
	"filetransfer/internal/db"
	"filetransfer/internal/flows"
	"filetransfer/internal/logger"
	"filetransfer/internal/model"
	"filetransfer/internal/transfer"
)

//go:embed all:ui
var uiFS embed.FS

type server struct {
	cfg   *config.Config
	store *Store
	flows *flows.Set
	apps  apps.Registry
	peer  *http.Client // for polling peer masters' health/nodes (uses the CA trust store)
}

// ctxCN carries the authenticated client-certificate CN (mTLS) down the request chain.
type ctxKey string

const ctxCN ctxKey = "client_cn"

// Run starts the master: sets up logging, connects to Postgres, applies the schema,
// and serves HTTP(S).
func Run(ctx context.Context, cfg *config.Config) error {
	if err := logger.Init(cfg, "master"); err != nil {
		return err
	}
	pool, err := db.Open(ctx, cfg.Database.DSN)
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := db.Migrate(ctx, pool); err != nil {
		return err
	}
	log.Printf("master: schema ready")

	// A relative flow file_path for application X resolves under <parent>/X/, where parent
	// is the deployments' shared parent directory (sibling deployments named per app).
	flowSet, err := flows.Load(cfg.Paths.FlowsDir, filepath.Dir(cfg.Home))
	if err != nil {
		return fmt.Errorf("flows: %w", err)
	}
	log.Printf("master: loaded %d flow(s) from %s", flowSet.Count(), cfg.Paths.FlowsDir)

	appReg, err := apps.Load(cfg.Paths.AppsFile)
	if err != nil {
		return fmt.Errorf("applications: %w", err)
	}
	log.Printf("master: loaded %d application endpoint(s) from %s", len(appReg), cfg.Paths.AppsFile)
	// Warn if a flow references an application missing from the registry.
	for _, a := range flowSet.Apps() {
		if !appReg.Has(a) {
			log.Printf("master: WARNING flow application %q has no endpoint in %s", a, cfg.Paths.AppsFile)
		}
	}

	// Client for polling peer masters (health + their workers). Trust the same CA store so
	// an https peer presenting its identity cert verifies.
	peerTLS := &tls.Config{MinVersion: tls.VersionTLS12}
	if capool, perr := loadCAPool(cfg.TLS.CADir); perr == nil {
		peerTLS.RootCAs = capool
	}
	peer := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{TLSClientConfig: peerTLS}}

	s := &server{cfg: cfg, store: NewStore(pool), flows: flowSet, apps: appReg, peer: peer}
	go s.requeueLoop(ctx)

	srv := &http.Server{Addr: cfg.Master.Addr, Handler: s.routes()}
	go func() {
		<-ctx.Done()
		sh, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sh)
	}()

	scheme := "http"
	if cfg.TLS.Enabled {
		scheme = "https"
		tlsCfg, terr := buildServerTLS(cfg)
		if terr != nil {
			return terr
		}
		srv.TLSConfig = tlsCfg
		if cfg.TLS.MTLS {
			scheme = "https+mtls"
		}
	}
	log.Printf("master: listening on %s (%s, chunk size %d bytes)", cfg.Master.Addr, scheme, cfg.Master.ChunkSize)
	if cfg.TLS.Enabled {
		err = srv.ListenAndServeTLS("", "") // certs come from srv.TLSConfig
	} else {
		err = srv.ListenAndServe()
	}
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// buildServerTLS loads the server cert and, when mTLS is on, requires and verifies a
// client certificate against the client-CA trust store.
func buildServerTLS(cfg *config.Config) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(cfg.TLS.CertFile, cfg.TLS.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server cert: %w", err)
	}
	t := &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12}
	if cfg.TLS.MTLS {
		pool, err := loadCAPool(cfg.TLS.ClientCADir)
		if err != nil {
			return nil, fmt.Errorf("client trust store: %w", err)
		}
		t.ClientCAs = pool
		// Verify a client cert *if one is presented*, but don't require it at the
		// handshake — that lets a browser load the read-only UI over server-TLS, while
		// the API/coordination endpoints still enforce a client cert (see server.mtls).
		t.ClientAuth = tls.VerifyClientCertIfGiven
	}
	return t, nil
}

// loadCAPool builds a cert pool from every *.crt / *.pem file in dir.
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
		switch strings.ToLower(filepath.Ext(e.Name())) {
		case ".crt", ".pem":
			if b, err := os.ReadFile(filepath.Join(dir, e.Name())); err == nil && pool.AppendCertsFromPEM(b) {
				added++
			}
		}
	}
	if added == 0 {
		return nil, fmt.Errorf("no CA certificates in %s", dir)
	}
	return pool, nil
}

func (s *server) routes() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })

	// Read-only (UI-facing): open over server-TLS so the dashboard works in a browser.
	mux.HandleFunc("GET /api/transfers", s.listTransfers)
	mux.HandleFunc("GET /api/transfers/{id}", s.getTransfer)
	mux.HandleFunc("GET /api/permissions", s.listPermissions)
	mux.HandleFunc("GET /api/nodes", s.listNodes)
	mux.HandleFunc("GET /api/worker-transfers", s.listWorkerTransfers)
	mux.HandleFunc("GET /api/requests", s.listRequests)
	mux.HandleFunc("GET /api/flows", s.listFlows)
	mux.HandleFunc("GET /api/flows/browse", s.browseFlow)
	mux.HandleFunc("GET /api/applications", s.listApplications)
	mux.HandleFunc("GET /api/cluster", s.clusterHealth)

	// Initiating a transfer REGISTERS it in the DB; a worker (mTLS-authenticated) then
	// claims and performs it. Creation is allowed from the UI (no client cert) — the sender
	// is taken from the request's sender_app; a presented client-cert CN takes precedence.
	mux.HandleFunc("POST /api/transfers", s.createTransfer)

	// Other mutations + cluster coordination: require a verified client cert when mTLS is on.
	mux.HandleFunc("POST /api/permissions", s.mtls(s.upsertPermission))
	mux.HandleFunc("POST /api/nodes/register", s.mtls(s.registerNode))
	mux.HandleFunc("POST /api/tasks/claim", s.mtls(s.claimTask))
	mux.HandleFunc("POST /api/tasks/{id}/manifest", s.mtls(s.taskManifest))
	mux.HandleFunc("POST /api/tasks/{id}/progress", s.mtls(s.taskProgress))
	mux.HandleFunc("POST /api/tasks/{id}/complete", s.mtls(s.taskComplete))
	mux.HandleFunc("POST /api/tasks/{id}/heartbeat", s.mtls(s.taskHeartbeat))
	mux.HandleFunc("DELETE /api/flows/file", s.mtls(s.deleteFile))
	mux.HandleFunc("POST /api/receive/manifest", s.mtls(s.receiveManifest))
	mux.HandleFunc("PUT /api/receive/chunks/{id}/{seq}", s.mtls(s.receiveChunk))
	mux.HandleFunc("POST /api/receive/{id}/complete", s.mtls(s.receiveComplete))

	// UI (embedded ui/ dir served at /)
	sub, _ := fs.Sub(uiFS, "ui")
	mux.Handle("GET /", http.FileServer(http.FS(sub)))

	return logRequests(s.withClientCN(mux))
}

// withClientCN records the verified client-cert CN (mTLS) in the request context so
// handlers can map it to a flow. With mTLS off it is a no-op.
func (s *server) withClientCN(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS != nil && len(r.TLS.PeerCertificates) > 0 {
			cn := r.TLS.PeerCertificates[0].Subject.CommonName
			r = r.WithContext(context.WithValue(r.Context(), ctxCN, cn))
		}
		next.ServeHTTP(w, r)
	})
}

func clientCN(r *http.Request) string {
	cn, _ := r.Context().Value(ctxCN).(string)
	return cn
}

// mtls guards an endpoint: when mTLS is enabled it requires a verified client cert
// (rejecting cert-less callers with 401). Read-only/UI endpoints are left unguarded so
// the dashboard loads in a browser.
func (s *server) mtls(h http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if s.cfg.TLS.MTLS && (r.TLS == nil || len(r.TLS.PeerCertificates) == 0) {
			httpError(w, http.StatusUnauthorized, "client certificate required")
			return
		}
		h(w, r)
	}
}

// ── Handlers ──

func (s *server) createTransfer(w http.ResponseWriter, r *http.Request) {
	var req model.CreateTransferReq
	cn := clientCN(r)
	// Every request is audited to ft_requests, accepted or rejected. `rec` accumulates the
	// requested details; `reject` logs it as rejected and writes the error response.
	rec := &model.TransferRequest{ClientCN: cn, RemoteAddr: r.RemoteAddr}
	reject := func(code int, msg string) {
		rec.Outcome, rec.StatusCode, rec.Error = model.RequestRejected, code, msg
		if err := s.store.LogRequest(r.Context(), rec); err != nil {
			log.Printf("master: log request: %v", err)
		}
		httpError(w, code, msg)
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		reject(http.StatusBadRequest, "invalid JSON body")
		return
	}
	// Capture the requested values (relative paths, as received) for the audit log.
	rec.FlowID, rec.Source, rec.Target = req.FlowID, req.Source, req.Target
	rec.SenderApp, rec.TargetApp = req.SenderApp, req.TargetApp

	if req.Source == "" {
		reject(http.StatusBadRequest, "source is required")
		return
	}

	// Every transfer belongs to a flow. Resolve it — preferably by flow_id, else by the
	// (sender, receiver) pair from a client-cert CN or sender_app + target_app. This
	// endpoint only REGISTERS the transfer; a worker (mTLS-authenticated) then performs it.
	var flow *flows.Flow
	if req.FlowID != "" {
		if flow = s.flows.ByID(req.FlowID); flow == nil {
			reject(http.StatusBadRequest, fmt.Sprintf("no flow with identifier %q", req.FlowID))
			return
		}
	} else {
		if flow = s.flows.Match(cn, req.SenderApp, req.TargetApp); flow == nil {
			reject(http.StatusBadRequest, "no matching flow (provide flow_id, or sender_app + target_app / a client certificate)")
			return
		}
	}
	// Record the resolved flow + its applications on the audit row.
	rec.FlowID, rec.SenderApp, rec.TargetApp = flow.Identifier, flow.Sender.Application, flow.Receiver.Application

	// If a client certificate is presented, it must be the flow's sender.
	if cn != "" && cn != flow.Sender.CN {
		reject(http.StatusForbidden, fmt.Sprintf("client CN %q is not the sender of flow %q", cn, flow.Identifier))
		return
	}
	// Permission gate: sending reads the source and delivers (writes) to the destination,
	// so the sender endpoint must be granted BOTH 'read' and 'write' in the flow config.
	if !flow.Sender.Can(flows.PermRead) {
		reject(http.StatusForbidden, fmt.Sprintf("sender %q lacks 'read' permission in flow %q", flow.Sender.Application, flow.Identifier))
		return
	}
	if !flow.Sender.Can(flows.PermWrite) {
		reject(http.StatusForbidden, fmt.Sprintf("sender %q lacks 'write' permission in flow %q", flow.Sender.Application, flow.Identifier))
		return
	}
	// Target is optional: default to the source's filename inside the receiver sandbox.
	if req.Target == "" {
		req.Target = filepath.Base(req.Source)
	}
	srcAbs, err := flow.Sender.Resolve(req.Source)
	if err != nil {
		reject(http.StatusForbidden, "source: "+err.Error())
		return
	}
	tgtAbs, err := flow.Receiver.Resolve(req.Target)
	if err != nil {
		reject(http.StatusForbidden, "target: "+err.Error())
		return
	}
	req.Source, req.Target = srcAbs, tgtAbs
	req.FlowID = flow.Identifier
	if req.RequestedBy == "" {
		req.RequestedBy = flow.Sender.Application
	}
	via := "ui"
	if cn != "" {
		via = "cn=" + cn
	}
	logger.Transfer("AUTH     flow=%s sender=%s(%s) -> receiver=%s@%s", flow.Identifier,
		flow.Sender.Application, via, flow.Receiver.Application, s.apps.Endpoint(flow.Receiver.Application))

	t, err := s.store.CreateTransfer(r.Context(), req, s.cfg.Master.ChunkSize)
	if err != nil {
		reject(http.StatusInternalServerError, err.Error())
		return
	}
	// Accepted: record the audit row with the created transfer id.
	rec.Outcome, rec.StatusCode, rec.TransferID = model.RequestAccepted, http.StatusCreated, t.ID
	if err := s.store.LogRequest(r.Context(), rec); err != nil {
		log.Printf("master: log request: %v", err)
	}
	logger.Transfer("CREATED  id=%s src=%q dst=%q chunk=%d by=%q", t.ID, t.Source, t.Target, t.ChunkSize, t.RequestedBy)
	writeJSON(w, http.StatusCreated, t)
}

func (s *server) listRequests(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := s.store.ListRequests(r.Context(), limit)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *server) listTransfers(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := s.store.ListTransfers(r.Context(), limit)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *server) getTransfer(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("id")
	t, err := s.store.GetTransfer(r.Context(), id)
	if err != nil {
		httpError(w, http.StatusNotFound, "transfer not found")
		return
	}
	chunks, _ := s.store.ListChunks(r.Context(), id)
	writeJSON(w, http.StatusOK, map[string]any{"transfer": t, "chunks": chunks})
}

func (s *server) listPermissions(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListPermissions(r.Context())
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *server) upsertPermission(w http.ResponseWriter, r *http.Request) {
	var p model.Permission
	if !readJSON(w, r, &p) {
		return
	}
	if p.Principal == "" || p.PathPrefix == "" {
		httpError(w, http.StatusBadRequest, "principal and path_prefix are required")
		return
	}
	out, err := s.store.UpsertPermission(r.Context(), p)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, out)
}

// nodeActiveWindow: a node is "active" (shown in the UI) if it has heartbeated within this
// window. Workers re-register every ~15s, so a stopped worker drops off within it.
const nodeActiveWindow = 45 * time.Second

func (s *server) listNodes(w http.ResponseWriter, r *http.Request) {
	list, err := s.store.ListNodes(r.Context(), time.Now().Add(-nodeActiveWindow))
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

// listWorkerTransfers returns transfers joined to the worker node that processed them,
// for the "transfers per worker node" page (the UI groups by node_id).
func (s *server) listWorkerTransfers(w http.ResponseWriter, r *http.Request) {
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	list, err := s.store.ListTransfersByWorker(r.Context(), limit)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, list)
}

// browseFlow lists files under a flow's sandbox (by cert CN), for the flow-details view
// and the Initiate file picker. Path traversal is rejected by Flow.Browse/Resolve.
func (s *server) browseFlow(w http.ResponseWriter, r *http.Request) {
	f := s.flows.ByID(r.URL.Query().Get("flow"))
	if f == nil {
		httpError(w, http.StatusNotFound, fmt.Sprintf("no flow with identifier %q", r.URL.Query().Get("flow")))
		return
	}
	role := r.URL.Query().Get("role")
	if role == "" {
		role = flows.Sender
	}
	ep := f.EndpointFor(role)
	// Listing a folder requires the 'list' permission on that endpoint.
	if !ep.Can(flows.PermList) {
		httpError(w, http.StatusForbidden, fmt.Sprintf("%s %q lacks 'list' permission in flow %q", role, ep.Application, f.Identifier))
		return
	}
	rel := r.URL.Query().Get("path")
	dir, entries, err := ep.Browse(rel)
	if err != nil {
		httpError(w, http.StatusBadRequest, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"path": rel, "dir": dir, "entries": entries})
}

// deleteFile removes a file within a flow endpoint's sandbox. It requires mTLS, the
// endpoint's 'delete' permission, and (when mTLS is on) a client cert whose CN matches
// the endpoint operating on its own folder.
func (s *server) deleteFile(w http.ResponseWriter, r *http.Request) {
	f := s.flows.ByID(r.URL.Query().Get("flow"))
	if f == nil {
		httpError(w, http.StatusNotFound, fmt.Sprintf("no flow with identifier %q", r.URL.Query().Get("flow")))
		return
	}
	role := r.URL.Query().Get("role")
	if role == "" {
		role = flows.Receiver
	}
	ep := f.EndpointFor(role)
	if !ep.Can(flows.PermDelete) {
		httpError(w, http.StatusForbidden, fmt.Sprintf("%s %q lacks 'delete' permission in flow %q", role, ep.Application, f.Identifier))
		return
	}
	if cn := clientCN(r); cn != "" && cn != ep.CN {
		httpError(w, http.StatusForbidden, fmt.Sprintf("client CN %q is not authorized to delete in %s of flow %q", cn, role, f.Identifier))
		return
	}
	abs, err := ep.Resolve(r.URL.Query().Get("path"))
	if err != nil {
		httpError(w, http.StatusForbidden, err.Error())
		return
	}
	info, err := os.Stat(abs)
	if err != nil || info.IsDir() {
		httpError(w, http.StatusNotFound, "file not found")
		return
	}
	if err := os.Remove(abs); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Transfer("DELETE   flow=%s role=%s path=%q", f.Identifier, role, abs)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted", "path": abs})
}

// listApplications exposes the application name -> endpoint registry.
func (s *server) listApplications(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.apps)
}

// masterHealth is one master node's health plus its worker nodes.
type masterHealth struct {
	Application string       `json:"application"`
	Endpoint    string       `json:"endpoint"`
	Healthy     bool         `json:"healthy"`
	Workers     []model.Node `json:"workers"`
}

// partnerFlow is one flow connecting this application to a partner, with direction from
// THIS application's point of view.
type partnerFlow struct {
	Identifier string `json:"flow_identifier"`
	Direction  string `json:"direction"` // outbound (we send) | inbound (we receive)
}

// partner is another application this one exchanges files with (via one or more flows).
type partner struct {
	Application string        `json:"application"`
	Endpoint    string        `json:"endpoint"`
	Healthy     bool          `json:"healthy"`
	Flows       []partnerFlow `json:"flows"`
}

// partners derives this application's partners from the loaded flows. `self` is this
// deployment's application (the FT_HOME basename); `healthy` maps app -> reachability.
func (s *server) partners(self string, healthy map[string]bool) []partner {
	byApp := map[string]*partner{}
	add := func(app, id, dir string) {
		if strings.EqualFold(app, self) {
			return
		}
		p := byApp[app]
		if p == nil {
			p = &partner{Application: app, Endpoint: s.apps.Endpoint(app), Healthy: healthy[app]}
			byApp[app] = p
		}
		p.Flows = append(p.Flows, partnerFlow{Identifier: id, Direction: dir})
	}
	for _, f := range s.flows.All() {
		if strings.EqualFold(f.Sender.Application, self) {
			add(f.Receiver.Application, f.Identifier, "outbound")
		}
		if strings.EqualFold(f.Receiver.Application, self) {
			add(f.Sender.Application, f.Identifier, "inbound")
		}
	}
	out := make([]partner, 0, len(byApp))
	for _, p := range byApp {
		sort.Slice(p.Flows, func(i, j int) bool { return p.Flows[i].Identifier < p.Flows[j].Identifier })
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Application < out[j].Application })
	return out
}

// clusterHealth polls every application endpoint (from applications.yml) for master
// health and its active workers, so the Overview can show the whole cluster.
func (s *server) clusterHealth(w http.ResponseWriter, r *http.Request) {
	names := s.apps.Names()
	out := make([]masterHealth, len(names))
	var wg sync.WaitGroup
	for i, name := range names {
		i, name := i, name
		endpoint := s.apps[name]
		wg.Add(1)
		go func() {
			defer wg.Done()
			mh := masterHealth{Application: name, Endpoint: endpoint, Workers: []model.Node{}}
			mh.Healthy = s.peerOK(r.Context(), endpoint+"/healthz")
			if mh.Healthy {
				if nodes := s.peerNodes(r.Context(), endpoint+"/api/nodes"); nodes != nil {
					mh.Workers = nodes
				}
			}
			out[i] = mh
		}()
	}
	wg.Wait()

	// Derive this application's partners (other apps it shares a flow with) from the
	// health just gathered. `self` is this deployment's application name.
	healthy := make(map[string]bool, len(out))
	for _, m := range out {
		healthy[m.Application] = m.Healthy
	}
	self := filepath.Base(s.cfg.Home)
	writeJSON(w, http.StatusOK, map[string]any{
		"self":     self,
		"masters":  out,
		"partners": s.partners(self, healthy),
	})
}

// peerOK reports whether a GET to url returns 2xx.
func (s *server) peerOK(ctx context.Context, url string) bool {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return false
	}
	resp, err := s.peer.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode >= 200 && resp.StatusCode < 300
}

// peerNodes fetches a peer master's active worker nodes (nil on error).
func (s *server) peerNodes(ctx context.Context, url string) []model.Node {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil
	}
	resp, err := s.peer.Do(req)
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	var nodes []model.Node
	if err := json.NewDecoder(resp.Body).Decode(&nodes); err != nil {
		return nil
	}
	return nodes
}

// listFlows exposes the configured sender/receiver flows so the UI can drive the
// transfer form (and permission model) from the actual flow definitions.
func (s *server) listFlows(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, http.StatusOK, s.flows.List())
}

func (s *server) registerNode(w http.ResponseWriter, r *http.Request) {
	var n model.Node
	if !readJSON(w, r, &n) {
		return
	}
	if n.ID == "" {
		httpError(w, http.StatusBadRequest, "id is required")
		return
	}
	if n.Role == "" {
		n.Role = "worker"
	}
	if err := s.store.RegisterNode(r.Context(), n); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "registered"})
}

func (s *server) claimTask(w http.ResponseWriter, r *http.Request) {
	var body struct {
		NodeID string `json:"node_id"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	task, tr, err := s.store.ClaimTask(r.Context(), body.NodeID)
	if errors.Is(err, ErrNoTask) {
		w.WriteHeader(http.StatusNoContent)
		return
	}
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Transfer("ASSIGNED id=%s task=%d node=%s", tr.ID, task.ID, body.NodeID)
	writeJSON(w, http.StatusOK, map[string]any{"task": task, "transfer": tr})
}

func (s *server) taskManifest(w http.ResponseWriter, r *http.Request) {
	var m transfer.Manifest
	if !readJSON(w, r, &m) {
		return
	}
	if m.TransferID == "" {
		httpError(w, http.StatusBadRequest, "transfer_id is required")
		return
	}
	if err := s.store.SaveManifest(r.Context(), &m); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "manifest saved"})
}

func (s *server) taskProgress(w http.ResponseWriter, r *http.Request) {
	var body struct {
		TransferID string `json:"transfer_id"`
		Seq        int    `json:"seq"`
		Status     string `json:"status"`
		NodeID     string `json:"node_id"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	if err := s.store.SetChunkStatus(r.Context(), body.TransferID, body.Seq, body.Status, body.NodeID); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) taskComplete(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	var body struct {
		TransferID string `json:"transfer_id"`
		OK         bool   `json:"ok"`
		Checksum   string `json:"checksum"`
		Error      string `json:"error"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	latencyMs, err := s.store.CompleteTask(r.Context(), id, body.TransferID, body.Checksum, body.OK, body.Error)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if body.OK {
		logger.Transfer("COMPLETED id=%s task=%d checksum=%s latency_ms=%d", body.TransferID, id, body.Checksum, latencyMs)
	} else {
		logger.Transfer("FAILED   id=%s task=%d error=%q latency_ms=%d", body.TransferID, id, body.Error, latencyMs)
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "recorded"})
}

func (s *server) taskHeartbeat(w http.ResponseWriter, r *http.Request) {
	id, _ := strconv.ParseInt(r.PathValue("id"), 10, 64)
	var body struct {
		NodeID string `json:"node_id"`
	}
	if !readJSON(w, r, &body) {
		return
	}
	_ = s.store.Heartbeat(r.Context(), id, body.NodeID)
	w.WriteHeader(http.StatusNoContent)
}

func (s *server) receiveManifest(w http.ResponseWriter, r *http.Request) {
	var m transfer.Manifest
	if !readJSON(w, r, &m) {
		return
	}
	if m.TransferID == "" || m.FlowID == "" {
		httpError(w, http.StatusBadRequest, "transfer_id and flow_id are required")
		return
	}
	f, ok := s.authorizeIncoming(w, r, m.FlowID, m.Target)
	if !ok {
		return
	}
	if err := s.store.SaveIncomingManifest(r.Context(), &m, f.Sender.Application); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	chunks, err := s.store.ListChunks(r.Context(), m.TransferID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Transfer("RECEIVE  manifest id=%s flow=%s chunks=%d sender=%s", m.TransferID, m.FlowID, len(m.Chunks), f.Sender.Application)
	writeJSON(w, http.StatusOK, map[string]any{"status": "manifest accepted", "chunks": chunks})
}

func (s *server) receiveChunk(w http.ResponseWriter, r *http.Request) {
	transferID := r.PathValue("id")
	seq, err := strconv.Atoi(r.PathValue("seq"))
	if err != nil {
		httpError(w, http.StatusBadRequest, "invalid chunk sequence")
		return
	}
	t, err := s.store.GetTransfer(r.Context(), transferID)
	if err != nil {
		httpError(w, http.StatusNotFound, "transfer not found")
		return
	}
	if _, ok := s.authorizeIncoming(w, r, t.FlowID, t.Target); !ok {
		return
	}
	ch, err := s.store.GetChunk(r.Context(), transferID, seq)
	if err != nil {
		httpError(w, http.StatusNotFound, "chunk not found")
		return
	}
	if err := os.MkdirAll(filepath.Dir(t.Target), 0o755); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	part := stagingPath(t.Target, transferID)
	f, err := os.OpenFile(part, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	defer f.Close()
	if _, err := f.Seek(ch.Offset, io.SeekStart); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	h := sha256.New()
	written, err := io.Copy(io.MultiWriter(f, h), io.LimitReader(r.Body, ch.Size+1))
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if written != ch.Size {
		_ = s.store.SetChunkStatus(r.Context(), transferID, seq, model.ChunkFailed, r.Header.Get("X-FT-Node-ID"))
		httpError(w, http.StatusBadRequest, fmt.Sprintf("chunk %d size mismatch: want %d got %d", seq, ch.Size, written))
		return
	}
	if got := hex.EncodeToString(h.Sum(nil)); ch.Checksum != "" && got != ch.Checksum {
		_ = s.store.SetChunkStatus(r.Context(), transferID, seq, model.ChunkFailed, r.Header.Get("X-FT-Node-ID"))
		httpError(w, http.StatusBadRequest, fmt.Sprintf("chunk %d checksum mismatch", seq))
		return
	}
	if err := f.Sync(); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	nodeID := r.Header.Get("X-FT-Node-ID")
	if err := s.store.SetChunkStatus(r.Context(), transferID, seq, model.ChunkWritten, nodeID); err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": model.ChunkWritten})
}

func (s *server) receiveComplete(w http.ResponseWriter, r *http.Request) {
	transferID := r.PathValue("id")
	t, err := s.store.GetTransfer(r.Context(), transferID)
	if err != nil {
		httpError(w, http.StatusNotFound, "transfer not found")
		return
	}
	if _, ok := s.authorizeIncoming(w, r, t.FlowID, t.Target); !ok {
		return
	}
	chunks, err := s.store.ListChunks(r.Context(), transferID)
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	for _, ch := range chunks {
		if ch.Status != model.ChunkWritten && ch.Status != model.ChunkAcked {
			httpError(w, http.StatusConflict, fmt.Sprintf("chunk %d is %s", ch.Seq, ch.Status))
			return
		}
	}
	part := stagingPath(t.Target, transferID)
	f, err := os.Open(part)
	if err != nil {
		_, _ = s.store.CompleteIncomingTransfer(r.Context(), transferID, "", false, err.Error())
		httpError(w, http.StatusNotFound, "staged file not found")
		return
	}
	h := sha256.New()
	n, err := io.Copy(h, f)
	_ = f.Close()
	if err != nil {
		_, _ = s.store.CompleteIncomingTransfer(r.Context(), transferID, "", false, err.Error())
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	if n != t.SizeBytes {
		msg := fmt.Sprintf("assembled size mismatch: want %d got %d", t.SizeBytes, n)
		_, _ = s.store.CompleteIncomingTransfer(r.Context(), transferID, "", false, msg)
		httpError(w, http.StatusBadRequest, msg)
		return
	}
	checksum := hex.EncodeToString(h.Sum(nil))
	if t.Checksum != "" && checksum != t.Checksum {
		msg := "assembled checksum mismatch"
		_, _ = s.store.CompleteIncomingTransfer(r.Context(), transferID, "", false, msg)
		httpError(w, http.StatusBadRequest, msg)
		return
	}
	if err := os.Rename(part, t.Target); err != nil {
		_, _ = s.store.CompleteIncomingTransfer(r.Context(), transferID, "", false, err.Error())
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	latencyMs, err := s.store.CompleteIncomingTransfer(r.Context(), transferID, checksum, true, "")
	if err != nil {
		httpError(w, http.StatusInternalServerError, err.Error())
		return
	}
	logger.Transfer("RECEIVED id=%s checksum=%s latency_ms=%d target=%q", transferID, checksum, latencyMs, t.Target)
	writeJSON(w, http.StatusOK, map[string]string{"status": model.TransferCompleted, "checksum": checksum})
}

func (s *server) authorizeIncoming(w http.ResponseWriter, r *http.Request, flowID, target string) (*flows.Flow, bool) {
	f := s.flows.ByID(flowID)
	if f == nil {
		httpError(w, http.StatusBadRequest, fmt.Sprintf("no flow with identifier %q", flowID))
		return nil, false
	}
	if cn := clientCN(r); cn != "" && cn != f.Sender.CN {
		httpError(w, http.StatusForbidden, fmt.Sprintf("client CN %q is not the sender of flow %q", cn, flowID))
		return nil, false
	}
	if !f.Sender.Can(flows.PermWrite) {
		httpError(w, http.StatusForbidden, fmt.Sprintf("sender %q lacks 'write' permission in flow %q", f.Sender.Application, flowID))
		return nil, false
	}
	if !f.Receiver.Contains(target) {
		httpError(w, http.StatusForbidden, fmt.Sprintf("target %q escapes receiver sandbox", target))
		return nil, false
	}
	return f, true
}

func stagingPath(target, transferID string) string {
	dir := filepath.Dir(target)
	base := filepath.Base(target)
	return filepath.Join(dir, "."+base+"."+transferID+".part")
}

// requeueLoop periodically returns tasks whose worker died (no heartbeat) to the pool.
func (s *server) requeueLoop(ctx context.Context) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := s.store.RequeueStale(ctx, 2*time.Minute); err == nil && n > 0 {
				log.Printf("master: requeued %d stale task(s)", n)
			}
			// Drop node rows for workers that stopped heartbeating a while ago.
			if n, err := s.store.PurgeStaleNodes(ctx, time.Now().Add(-5*time.Minute)); err == nil && n > 0 {
				log.Printf("master: purged %d stale node(s)", n)
			}
		}
	}
}

// ── HTTP helpers ──

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func httpError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}

func readJSON(w http.ResponseWriter, r *http.Request, dst any) bool {
	if err := json.NewDecoder(r.Body).Decode(dst); err != nil {
		httpError(w, http.StatusBadRequest, "invalid JSON body")
		return false
	}
	return true
}

func logRequests(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		if r.URL.Path != "/" && r.URL.Path != "/healthz" {
			log.Printf("%s %s %s", r.Method, r.URL.Path, time.Since(start).Round(time.Millisecond))
		}
	})
}
