package master

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/google/uuid"

	"filetransfer/internal/model"
	"filetransfer/internal/transfer"
)

// ErrNoTask is returned by ClaimTask when no pending task is available.
var ErrNoTask = errors.New("no task available")

type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }

// CreateTransfer records a new transfer (status pending) and its claimable task.
// The chunk list/size/checksum are filled in later when a worker uploads the manifest.
func (s *Store) CreateTransfer(ctx context.Context, req model.CreateTransferReq, defChunk int64) (*model.Transfer, error) {
	chunk := req.ChunkSize
	if chunk <= 0 {
		chunk = defChunk
	}
	t := &model.Transfer{
		ID:          uuid.NewString(),
		FlowID:      req.FlowID,
		Source:      req.Source,
		SourceKind:  transfer.Kind(req.Source),
		Target:      req.Target,
		TargetKind:  transfer.Kind(req.Target),
		ChunkSize:   chunk,
		Status:      model.TransferPending,
		Priority:    req.Priority,
		RequestedBy: req.RequestedBy,
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if err := tx.QueryRowContext(ctx, `
		INSERT INTO ft_transfers (id, flow_id, source, source_kind, target, target_kind, chunk_size, status, priority, requested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		RETURNING created_at, updated_at`,
		t.ID, t.FlowID, t.Source, t.SourceKind, t.Target, t.TargetKind, t.ChunkSize, t.Status, t.Priority, t.RequestedBy,
	).Scan(&t.CreatedAt, &t.UpdatedAt); err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`INSERT INTO ft_tasks (transfer_id, status) VALUES ($1,'pending')`, t.ID); err != nil {
		return nil, err
	}
	return t, tx.Commit()
}

// LogRequest records one POST /api/transfers request (accepted or rejected). It never
// fails the request path — errors are returned for the caller to log, not surface.
func (s *Store) LogRequest(ctx context.Context, rq *model.TransferRequest) error {
	return s.db.QueryRowContext(ctx, `
		INSERT INTO ft_requests
		  (flow_id, source, target, sender_app, target_app, client_cn, remote_addr, outcome, status_code, error, transfer_id)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		RETURNING id, created_at`,
		rq.FlowID, rq.Source, rq.Target, rq.SenderApp, rq.TargetApp, rq.ClientCN, rq.RemoteAddr,
		rq.Outcome, rq.StatusCode, rq.Error, rq.TransferID,
	).Scan(&rq.ID, &rq.CreatedAt)
}

// ListRequests returns recent transfer requests, newest first.
func (s *Store) ListRequests(ctx context.Context, limit int) ([]model.TransferRequest, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, created_at, flow_id, source, target, sender_app, target_app, client_cn,
		       remote_addr, outcome, status_code, error, transfer_id
		FROM ft_requests ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.TransferRequest{}
	for rows.Next() {
		var q model.TransferRequest
		if err := rows.Scan(&q.ID, &q.CreatedAt, &q.FlowID, &q.Source, &q.Target, &q.SenderApp,
			&q.TargetApp, &q.ClientCN, &q.RemoteAddr, &q.Outcome, &q.StatusCode, &q.Error, &q.TransferID); err != nil {
			return nil, err
		}
		out = append(out, q)
	}
	return out, rows.Err()
}

func (s *Store) ListTransfers(ctx context.Context, limit int) ([]model.Transfer, error) {
	if limit <= 0 {
		limit = 100
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, flow_id, source, source_kind, target, target_kind, size_bytes, chunk_size, checksum,
		       status, error, priority, requested_by, latency_ms, created_at, updated_at
		FROM ft_transfers ORDER BY created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Transfer{}
	for rows.Next() {
		var t model.Transfer
		if err := rows.Scan(&t.ID, &t.FlowID, &t.Source, &t.SourceKind, &t.Target, &t.TargetKind, &t.SizeBytes,
			&t.ChunkSize, &t.Checksum, &t.Status, &t.Error, &t.Priority, &t.RequestedBy, &t.LatencyMs,
			&t.CreatedAt, &t.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

// TransferWithNode is a transfer plus the worker node that claimed/processed it
// (from its task). NodeID is empty while the transfer is still pending.
type TransferWithNode struct {
	model.Transfer
	NodeID string `json:"node_id"`
}

// ListTransfersByWorker returns transfers joined to the worker node that processed
// them, so the UI can group transfers per node.
func (s *Store) ListTransfersByWorker(ctx context.Context, limit int) ([]TransferWithNode, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT tr.id, tr.flow_id, tr.source, tr.source_kind, tr.target, tr.target_kind, tr.size_bytes,
		       tr.chunk_size, tr.checksum, tr.status, tr.error, tr.priority, tr.requested_by, tr.latency_ms,
		       tr.created_at, tr.updated_at, COALESCE(t.node_id,'')
		FROM ft_transfers tr LEFT JOIN ft_tasks t ON t.transfer_id = tr.id
		ORDER BY tr.created_at DESC LIMIT $1`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []TransferWithNode{}
	for rows.Next() {
		var t TransferWithNode
		if err := rows.Scan(&t.ID, &t.FlowID, &t.Source, &t.SourceKind, &t.Target, &t.TargetKind, &t.SizeBytes,
			&t.ChunkSize, &t.Checksum, &t.Status, &t.Error, &t.Priority, &t.RequestedBy, &t.LatencyMs,
			&t.CreatedAt, &t.UpdatedAt, &t.NodeID); err != nil {
			return nil, err
		}
		out = append(out, t)
	}
	return out, rows.Err()
}

func (s *Store) GetTransfer(ctx context.Context, id string) (*model.Transfer, error) {
	var t model.Transfer
	err := s.db.QueryRowContext(ctx, `
		SELECT id, flow_id, source, source_kind, target, target_kind, size_bytes, chunk_size, checksum,
		       status, error, priority, requested_by, latency_ms, created_at, updated_at
		FROM ft_transfers WHERE id=$1`, id).Scan(
		&t.ID, &t.FlowID, &t.Source, &t.SourceKind, &t.Target, &t.TargetKind, &t.SizeBytes, &t.ChunkSize,
		&t.Checksum, &t.Status, &t.Error, &t.Priority, &t.RequestedBy, &t.LatencyMs, &t.CreatedAt, &t.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &t, nil
}

func (s *Store) ListChunks(ctx context.Context, transferID string) ([]model.Chunk, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT transfer_id, seq, "offset", size, checksum, status, COALESCE(node_id,''), updated_at
		FROM ft_chunks WHERE transfer_id=$1 ORDER BY seq`, transferID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Chunk{}
	for rows.Next() {
		var c model.Chunk
		if err := rows.Scan(&c.TransferID, &c.Seq, &c.Offset, &c.Size, &c.Checksum, &c.Status, &c.NodeID, &c.UpdatedAt); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, rows.Err()
}

// ClaimTask atomically claims the next pending task for nodeID and marks its transfer
// "assigned". SELECT ... FOR UPDATE SKIP LOCKED lets many workers claim concurrently.
func (s *Store) ClaimTask(ctx context.Context, nodeID string) (*model.Task, *model.Transfer, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()

	var task model.Task
	err = tx.QueryRowContext(ctx, `
		SELECT t.id, t.transfer_id
		FROM ft_tasks t JOIN ft_transfers tr ON tr.id = t.transfer_id
		WHERE t.status='pending'
		ORDER BY tr.priority DESC, t.created_at ASC
		FOR UPDATE OF t SKIP LOCKED
		LIMIT 1`).Scan(&task.ID, &task.TransferID)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil, ErrNoTask
	}
	if err != nil {
		return nil, nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE ft_tasks SET status='claimed', node_id=$1, claimed_at=now(), heartbeat_at=now() WHERE id=$2`,
		nodeID, task.ID); err != nil {
		return nil, nil, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE ft_transfers SET status=$1, updated_at=now() WHERE id=$2 AND status='pending'`,
		model.TransferAssigned, task.TransferID); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, err
	}
	task.Status = "claimed"
	task.NodeID = nodeID
	tr, err := s.GetTransfer(ctx, task.TransferID)
	return &task, tr, err
}

// SaveManifest persists the worker-computed size/checksum/chunk plan and flips the
// transfer to in_progress.
func (s *Store) SaveManifest(ctx context.Context, m *transfer.Manifest) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx,
		`UPDATE ft_transfers SET size_bytes=$1, checksum=$2, chunk_size=$3, status=$4, updated_at=now() WHERE id=$5`,
		m.SizeBytes, m.Checksum, m.ChunkSize, model.TransferInProgress, m.TransferID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM ft_chunks WHERE transfer_id=$1`, m.TransferID); err != nil {
		return err
	}
	for _, c := range m.Chunks {
		if _, err := tx.ExecContext(ctx,
			`INSERT INTO ft_chunks (transfer_id, seq, "offset", size, checksum, status) VALUES ($1,$2,$3,$4,$5,'pending')`,
			m.TransferID, c.Seq, c.Offset, c.Size, c.Checksum); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SaveIncomingManifest records a transfer created by a peer sender and persists its
// chunk plan in this receiver's database. The transfer id is shared across both sides.
func (s *Store) SaveIncomingManifest(ctx context.Context, m *transfer.Manifest, requestedBy string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `
		INSERT INTO ft_transfers
		  (id, flow_id, source, source_kind, target, target_kind, size_bytes, chunk_size, checksum, status, requested_by)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		ON CONFLICT (id) DO UPDATE SET
		  flow_id=EXCLUDED.flow_id,
		  source=EXCLUDED.source,
		  source_kind=EXCLUDED.source_kind,
		  target=EXCLUDED.target,
		  target_kind=EXCLUDED.target_kind,
		  size_bytes=EXCLUDED.size_bytes,
		  chunk_size=EXCLUDED.chunk_size,
		  checksum=EXCLUDED.checksum,
		  status=EXCLUDED.status,
		  requested_by=EXCLUDED.requested_by,
		  error='',
		  updated_at=now()`,
		m.TransferID, m.FlowID, m.Source, transfer.Kind(m.Source), m.Target, transfer.Kind(m.Target),
		m.SizeBytes, m.ChunkSize, m.Checksum, model.TransferInProgress, requestedBy)
	if err != nil {
		return err
	}
	for _, c := range m.Chunks {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO ft_chunks (transfer_id, seq, "offset", size, checksum, status)
			VALUES ($1,$2,$3,$4,$5,'pending')
			ON CONFLICT (transfer_id, seq) DO UPDATE SET
			  "offset"=EXCLUDED."offset",
			  size=EXCLUDED.size,
			  checksum=EXCLUDED.checksum,
			  updated_at=now()`,
			m.TransferID, c.Seq, c.Offset, c.Size, c.Checksum); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) GetChunk(ctx context.Context, transferID string, seq int) (*model.Chunk, error) {
	var c model.Chunk
	err := s.db.QueryRowContext(ctx, `
		SELECT transfer_id, seq, "offset", size, checksum, status, COALESCE(node_id,''), updated_at
		FROM ft_chunks WHERE transfer_id=$1 AND seq=$2`, transferID, seq).Scan(
		&c.TransferID, &c.Seq, &c.Offset, &c.Size, &c.Checksum, &c.Status, &c.NodeID, &c.UpdatedAt)
	if err != nil {
		return nil, err
	}
	return &c, nil
}

func (s *Store) SetChunkStatus(ctx context.Context, transferID string, seq int, status, nodeID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE ft_chunks SET status=$1, node_id=$2, updated_at=now() WHERE transfer_id=$3 AND seq=$4`,
		status, nodeID, transferID, seq)
	return err
}

func (s *Store) CompleteIncomingTransfer(ctx context.Context, transferID, checksum string, ok bool, errMsg string) (int64, error) {
	status := model.TransferCompleted
	if !ok {
		status = model.TransferFailed
	}
	var createdAt time.Time
	if err := s.db.QueryRowContext(ctx, `SELECT created_at FROM ft_transfers WHERE id=$1`, transferID).Scan(&createdAt); err != nil {
		return 0, err
	}
	latencyMs := model.LatencyMilliseconds(createdAt, time.Now())
	_, err := s.db.ExecContext(ctx, `
		UPDATE ft_transfers
		SET status=$1, checksum=COALESCE(NULLIF($2,''), checksum), error=$3, latency_ms=$4, updated_at=now()
		WHERE id=$5`, status, checksum, errMsg, latencyMs, transferID)
	return latencyMs, err
}

func (s *Store) Heartbeat(ctx context.Context, taskID int64, nodeID string) error {
	_, err := s.db.ExecContext(ctx,
		`UPDATE ft_tasks SET heartbeat_at=now(), node_id=$1 WHERE id=$2`, nodeID, taskID)
	return err
}

// CompleteTask finalizes a task and its transfer (completed or failed).
func (s *Store) CompleteTask(ctx context.Context, taskID int64, transferID, checksum string, ok bool, errMsg string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	taskStatus, trStatus := "done", model.TransferCompleted
	if !ok {
		taskStatus, trStatus = "failed", model.TransferFailed
	}
	var createdAt time.Time
	if err := tx.QueryRowContext(ctx, `SELECT created_at FROM ft_transfers WHERE id=$1`, transferID).Scan(&createdAt); err != nil {
		return 0, err
	}
	latencyMs := model.LatencyMilliseconds(createdAt, time.Now())
	if _, err := tx.ExecContext(ctx, `UPDATE ft_tasks SET status=$1 WHERE id=$2`, taskStatus, taskID); err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx,
		`UPDATE ft_transfers SET status=$1, checksum=COALESCE(NULLIF($2,''), checksum), error=$3, latency_ms=$5, updated_at=now() WHERE id=$4`,
		trStatus, checksum, errMsg, transferID, latencyMs); err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return latencyMs, nil
}

// ── Permissions ──

func (s *Store) ListPermissions(ctx context.Context) ([]model.Permission, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, principal, path_prefix, can_read, can_write, can_delete, created_at
		FROM ft_permissions ORDER BY principal, path_prefix`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Permission{}
	for rows.Next() {
		var p model.Permission
		if err := rows.Scan(&p.ID, &p.Principal, &p.PathPrefix, &p.CanRead, &p.CanWrite, &p.CanDelete, &p.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

func (s *Store) UpsertPermission(ctx context.Context, p model.Permission) (*model.Permission, error) {
	err := s.db.QueryRowContext(ctx, `
		INSERT INTO ft_permissions (principal, path_prefix, can_read, can_write, can_delete)
		VALUES ($1,$2,$3,$4,$5)
		ON CONFLICT (principal, path_prefix) DO UPDATE
		  SET can_read=EXCLUDED.can_read, can_write=EXCLUDED.can_write, can_delete=EXCLUDED.can_delete
		RETURNING id, created_at`,
		p.Principal, p.PathPrefix, p.CanRead, p.CanWrite, p.CanDelete).Scan(&p.ID, &p.CreatedAt)
	if err != nil {
		return nil, err
	}
	return &p, nil
}

// ── Nodes ──

func (s *Store) RegisterNode(ctx context.Context, n model.Node) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO ft_nodes (id, role, endpoint, last_seen) VALUES ($1,$2,$3,now())
		ON CONFLICT (id) DO UPDATE SET endpoint=EXCLUDED.endpoint, role=EXCLUDED.role, last_seen=now()`,
		n.ID, n.Role, n.Endpoint)
	return err
}

// ListNodes returns registered nodes. When `since` is non-zero, only nodes that have
// heartbeated at or after that time are returned (i.e. the workers currently associated
// with this master), so dead workers don't linger in the UI.
func (s *Store) ListNodes(ctx context.Context, since time.Time) ([]model.Node, error) {
	var sinceArg interface{}
	if !since.IsZero() {
		sinceArg = since
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, role, endpoint, last_seen, created_at FROM ft_nodes
		WHERE ($1::timestamptz IS NULL OR last_seen >= $1)
		ORDER BY last_seen DESC`, sinceArg)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []model.Node{}
	for rows.Next() {
		var n model.Node
		if err := rows.Scan(&n.ID, &n.Role, &n.Endpoint, &n.LastSeen, &n.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// PurgeStaleNodes deletes node rows not seen since the cutoff (called periodically).
func (s *Store) PurgeStaleNodes(ctx context.Context, olderThan time.Time) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM ft_nodes WHERE last_seen < $1`, olderThan)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// RequeueStale returns pending any task whose worker stopped heartbeating past the TTL,
// so another worker can pick it up (call periodically from the master).
func (s *Store) RequeueStale(ctx context.Context, ttl time.Duration) (int64, error) {
	res, err := s.db.ExecContext(ctx, `
		UPDATE ft_tasks SET status='pending', node_id=NULL
		WHERE status='claimed' AND heartbeat_at < now() - $1::interval`,
		ttl.String())
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return n, nil
}
