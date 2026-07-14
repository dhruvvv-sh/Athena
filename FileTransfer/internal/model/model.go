// Package model holds the shared domain types and status enums used by both the
// master and the worker (persisted in Postgres, exchanged over the REST API).
package model

import "time"

// Transfer status lifecycle.
const (
	TransferPending    = "pending"     // created, awaiting a worker
	TransferAssigned   = "assigned"    // claimed by a worker (task created)
	TransferInProgress = "in_progress" // chunks moving
	TransferCompleted  = "completed"   // assembled + checksum verified
	TransferFailed     = "failed"
	TransferCanceled   = "canceled"
)

// Chunk status lifecycle (per chunk, tracked in Postgres so any receiver can assemble).
const (
	ChunkPending = "pending"
	ChunkSent    = "sent"
	ChunkAcked   = "acked"
	ChunkWritten = "written"
	ChunkFailed  = "failed"
)

// Permission operations.
const (
	PermRead   = "read"
	PermWrite  = "write"
	PermDelete = "delete"
)

// Endpoint kinds for a transfer's source/target.
const (
	KindFile = "file" // local / mounted filesystem path
	KindS3   = "s3"   // s3://bucket/key
)

// LatencyMilliseconds returns transfer duration in milliseconds, clamped to zero for invalid ranges.
func LatencyMilliseconds(start, end time.Time) int64 {
	if end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

// Transfer is a single file-move job.
type Transfer struct {
	ID          string    `json:"id"`
	FlowID      string    `json:"flow_id,omitempty"` // flow identifier this transfer belongs to
	Source      string    `json:"source"`            // file path or s3://bucket/key
	SourceKind  string    `json:"source_kind"`       // KindFile | KindS3
	Target      string    `json:"target"`
	TargetKind  string    `json:"target_kind"`
	SizeBytes   int64     `json:"size_bytes"`
	ChunkSize   int64     `json:"chunk_size"`
	Checksum    string    `json:"checksum"` // sha-256 of the whole file (hex)
	Status      string    `json:"status"`
	Error       string    `json:"error,omitempty"`
	Priority    int       `json:"priority"`
	RequestedBy string    `json:"requested_by,omitempty"`
	LatencyMs   int64     `json:"latency_ms"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

// Task is a claimable unit of work for a worker node (1:1 with a transfer for now).
type Task struct {
	ID          int64      `json:"id"`
	TransferID  string     `json:"transfer_id"`
	Status      string     `json:"status"` // pending | claimed | done | failed
	NodeID      string     `json:"node_id,omitempty"`
	ClaimedAt   *time.Time `json:"claimed_at,omitempty"`
	HeartbeatAt *time.Time `json:"heartbeat_at,omitempty"`
	CreatedAt   time.Time  `json:"created_at"`
}

// Chunk is one piece of a transfer; state lives in Postgres so any receiver node
// behind the load balancer can accept it and take part in assembly.
type Chunk struct {
	TransferID string    `json:"transfer_id"`
	Seq        int       `json:"seq"`
	Offset     int64     `json:"offset"`
	Size       int64     `json:"size"`
	Checksum   string    `json:"checksum"` // sha-256 of this chunk (hex)
	Status     string    `json:"status"`
	NodeID     string    `json:"node_id,omitempty"`
	UpdatedAt  time.Time `json:"updated_at"`
}

// Permission grants read/write/delete on a path prefix to a principal.
type Permission struct {
	ID         int64     `json:"id"`
	Principal  string    `json:"principal"`   // user / service account
	PathPrefix string    `json:"path_prefix"` // e.g. "/data/incoming" or "s3://bucket/"
	CanRead    bool      `json:"can_read"`
	CanWrite   bool      `json:"can_write"`
	CanDelete  bool      `json:"can_delete"`
	CreatedAt  time.Time `json:"created_at"`
}

// Node is a registered worker in the cluster (for health / observability).
type Node struct {
	ID        string    `json:"id"`
	Role      string    `json:"role"` // "worker"
	Endpoint  string    `json:"endpoint,omitempty"`
	LastSeen  time.Time `json:"last_seen"`
	CreatedAt time.Time `json:"created_at"`
}

// TransferRequest is one audited POST /api/transfers request, whether it was accepted
// (a transfer got created) or rejected (validation/authorization failure).
type TransferRequest struct {
	ID         int64     `json:"id"`
	CreatedAt  time.Time `json:"created_at"`
	FlowID     string    `json:"flow_id,omitempty"`
	Source     string    `json:"source"`
	Target     string    `json:"target,omitempty"`
	SenderApp  string    `json:"sender_app,omitempty"`
	TargetApp  string    `json:"target_app,omitempty"`
	ClientCN   string    `json:"client_cn,omitempty"`
	RemoteAddr string    `json:"remote_addr,omitempty"`
	Outcome    string    `json:"outcome"` // accepted | rejected
	StatusCode int       `json:"status_code"`
	Error      string    `json:"error,omitempty"`
	TransferID string    `json:"transfer_id,omitempty"`
}

// Request outcomes.
const (
	RequestAccepted = "accepted"
	RequestRejected = "rejected"
)

// CreateTransferReq is the REST/JMS payload to initiate a transfer.
//
// Under mTLS + flows, Source is a path relative to the calling (sender) app's sandbox,
// TargetApp names the receiver application, and Target is a path relative to that
// receiver's sandbox. Without mTLS, Source/Target are used as-is.
type CreateTransferReq struct {
	Source      string `json:"source"`
	FlowID      string `json:"flow_id,omitempty"`    // flow identifier (preferred selector)
	SenderApp   string `json:"sender_app,omitempty"` // sender application (fallback when no flow_id)
	TargetApp   string `json:"target_app,omitempty"` // receiver application name (fallback when no flow_id)
	Target      string `json:"target"`
	ChunkSize   int64  `json:"chunk_size,omitempty"` // 0 => master default
	Priority    int    `json:"priority,omitempty"`
	RequestedBy string `json:"requested_by,omitempty"`
}
