# FileTransfer

A distributed, highly-available file-transfer service written in Go. A single binary runs
in one of two roles — **master** or **worker** — that together move files between two
applications' sandboxed folders in configurable, checksum-verified chunks.

**Application A and Application B run in different environments** (separate hosts /
networks). Each is a fully independent deployment — its own master, workers, Postgres,
certificates and `FT_HOME` — reachable over the network at its own endpoint. They share no
filesystem and no database; they cooperate only over mTLS-secured HTTP. The two are
connected by a **flow** — a signed, permissioned agreement describing who may send what, to
whom, and where.

```
    ENVIRONMENT A  (host-a)                        ENVIRONMENT B  (host-b)
  ┌───────────────────────────┐                 ┌───────────────────────────┐
  │  MASTER  (UI + API)        │   flow:         │  MASTER  (UI + API)        │
  │  • authorizes requests     │ app-a_app-b_…   │  • authorizes requests     │
  │  • records to its Postgres │◄─ mTLS / HTTP ─►│  • records to its Postgres │
  │  • creates claimable TASKS │  (cross-trust)  │  • creates claimable TASKS │
  └──────────┬─────────────────┘   over the      └──────────┬─────────────────┘
             │ Postgres A          network                  │ Postgres B
      ┌──────┴───────┐                               ┌──────┴───────┐
   WORKER a-w1   WORKER a-w2   …                   WORKER b-w1   WORKER b-w2   …

   A's worker: claim task → read source from A's sandbox → chunk + SHA-256 →
               deliver to B's endpoint → B verifies checksum → ACK
```

> Each environment only ever connects to the other over the network (endpoints in
> `applications.yml`) and trusts it via exchanged certificates — never via a shared disk or DB.

## Roles

| Role | Responsibility |
|------|----------------|
| **master** | Serves the tracking UI + REST API; authorizes every transfer request against the flows; records transfers, requests, per-chunk state and nodes in Postgres; creates tasks; polls peer masters for cluster health. |
| **worker** | Registers with its master (stable id, e.g. `app-a-w1`); atomically claims a pending task; reads the source from the sender sandbox, splits it into configurable chunks with per-chunk + whole-file SHA-256, delivers into the receiver sandbox, and verifies the assembled file. |

## Core concepts

### Application registry — `config/applications.yml`

Flows reference applications only by **name**; this file maps each name to its endpoint
across environments, so a master/worker can reach the right node. Use routable hostnames
(or IPs) — not `localhost` — since A and B are on different hosts. Deploy the **same
registry on both environments**.

```yaml
app-a: https://host-a.example.com:9095
app-b: https://host-b.example.com:9096
```

### Flows — `flows/<flow_identifier>.yml`

One file per flow, named by its identifier
(`<Sender>_<Receiver>_<Functionality>_<Country>`). Each file defines **both** ends —
application, sandbox `file_path`, cert `cn`, and `permissions`:

```yaml
flow_identifier: app-a_app-b_biz_in
sender:
  application: app-a
  file_path: /srv/filetransfer/outbox   # Application A's local sandbox (on host-a)
  cn: app-a-cert                         # identity-cert CN that authenticates the sender
  permissions: write, delete, list, read
receiver:
  application: app-b
  file_path: /srv/filetransfer/inbox    # Application B's local sandbox (on host-b)
  cn: app-b-cert
  permissions: list, read
```

Deploy the **same flow file to both environments**. Because A and B are on different hosts,
use an **absolute `file_path`** on each side — the application's real local sandbox. (A
relative path is resolved on the host running that application, which is convenient only for
a single-host dev setup where both deployments sit side by side.) Transfer requests then use
paths **relative to the sandbox**; absolute paths and `..` traversal are rejected (HTTP 403).

### Permissions

Each endpoint grants a subset of `read, write, list, delete` **on its own folder**. An
operation is allowed only if the required permission is present — otherwise it is refused
(HTTP 403) and, for transfers, recorded as a rejected request.

| Operation | Requires |
|-----------|----------|
| Initiate a transfer | **sender** has `read` (read source) **and** `write` (deliver to destination) |
| Browse / list a folder | that endpoint has `list` |
| Delete a file (`DELETE /api/flows/file`) | that endpoint has `delete` (+ mTLS + matching CN) |

In the example above, `app-a` may send (it has read+write) and `app-b` may list/read its
inbox — the delivery write into B's inbox is authorized by A's `write`.

### Identity certificates & mTLS

Each environment has **one self-signed identity certificate**
(`certs/app-a-cert.{crt,key}`, `CN=app-a-cert`) used for **both** the master (server cert)
and its workers (client cert). It is its own trust anchor (`CA:TRUE`,
`serverAuth,clientAuth`), and its SAN must cover the **routable hostname** the peer uses to
reach it. Trust is established by **exchanging public certs between the two environments**:

```
host-a  certs/CAs/  →  app-a-cert.crt (self) + app-b-cert.crt (copied from host-b)
host-b  certs/CAs/  →  app-b-cert.crt (self) + app-a-cert.crt (copied from host-a)
```

Only the public `.crt` crosses environments — private keys never leave their host.

Mint an identity cert (run on each host for its own application):

```bash
openssl genrsa -out app-a-cert.key 2048
openssl req -new -key app-a-cert.key -subj "/O=FileTransfer/CN=app-a-cert" -out app-a-cert.csr
cat > app-a-cert.ext <<EOF
basicConstraints=critical,CA:TRUE
keyUsage=critical,digitalSignature,keyEncipherment,keyCertSign
extendedKeyUsage=serverAuth,clientAuth
subjectAltName=DNS:host-a.example.com,DNS:localhost,IP:127.0.0.1
EOF
openssl x509 -req -in app-a-cert.csr -signkey app-a-cert.key -days 825 -sha256 \
  -extfile app-a-cert.ext -out app-a-cert.crt
```

When `tls.mtls: true` the master requires a verified client cert on the **worker
coordination** endpoints (task claim/manifest/progress/complete/heartbeat, node register)
and on **delete**. The read-only UI/API and transfer creation are reachable without a
client cert (a transfer initiated by a real sender app presents its cert; the CN must then
match the flow's sender). Read endpoints being open lets the browser dashboard work.

## Quick start — two environments (A → B)

Provision each environment **independently** on its own host; nothing is shared between
them except the exchanged public certs and the (identical) flow + application registry.

**On host-a — Application A (sender):**

```bash
# 1. Its own Postgres
createdb filetransfer                        # A's database (on host-a)

# 2. Build and stage the FT_HOME package (bin/ config/ flows/ certs/ lib/ logs/ tmp/)
make build                                   # -> deploy/lib/filetransfer

# 3. Configure this environment:
#    config/applications.yml   — endpoints for app-a and app-b (routable hostnames)
#    flows/app-a_app-b_biz_in.yml — the flow (absolute file_path = A's local sandbox)
#    certs/app-a-cert.{crt,key} + certs/CAs/ (app-a-cert.crt self + app-b-cert.crt from host-b)
#    config.yml:
#      master.addr: ":9095"
#      database.dsn: "postgres://…@localhost/filetransfer?sslmode=disable"
#      tls: { cert_file: certs/app-a-cert.crt, key_file: certs/app-a-cert.key,
#             client_cert: certs/app-a-cert.crt, client_key: certs/app-a-cert.key,
#             ca_dir: certs/CAs, mtls: true }

# 4. Start the master (applies its schema on first start) + workers
bin/start.sh master
bin/start.sh worker a-w1                     # stable node id = <app-home>-<instance>
bin/start.sh worker a-w2
#    A's UI/API: https://host-a.example.com:9095
```

**On host-b — Application B (receiver):** repeat the same steps with B's own database,
`certs/app-b-cert.*`, `certs/CAs/` holding `app-b-cert.crt` (self) + `app-a-cert.crt`
(copied from host-a), `master.addr: ":9096"`, and the **same** `applications.yml` and flow
file (its absolute `file_path` is B's local inbox). B's UI/API:
`https://host-b.example.com:9096`.

> A single-host dev setup can place both deployments side by side (`deploy/app-a`,
> `deploy/app-b`) with `localhost` endpoints and relative sandbox paths — but production A
> and B live on different environments as above.

## Initiating a transfer

Every transfer is created via `POST /api/transfers` and belongs to a flow. Provide the
`flow_id`; `target` is optional (defaults to the source's filename in the receiver
sandbox).

**As Application A (presenting its identity cert):**

```bash
# run on host-a with Application A's identity cert (paths relative to A's FT_HOME)
curl --cacert certs/CAs/app-a-cert.crt \
     --cert   certs/app-a-cert.crt \
     --key    certs/app-a-cert.key \
     -X POST https://host-a.example.com:9095/api/transfers \
     -H 'Content-Type: application/json' \
     -d '{"flow_id":"app-a_app-b_biz_in","source":"invoice.csv"}'
```

The master authorizes the flow (sender needs `read`+`write`), resolves `invoice.csv` inside
A's outbox sandbox, resolves the target inside B's inbox sandbox, and registers the transfer.
A worker then claims it, chunks + checksums the file, delivers it to Application B, and
verifies the result.

**From the UI:** open the **Initiate Transfer** page, pick the flow, browse/select a source
file, optionally set a target path, and submit.

## Web UI (served by each master)

| Page | Shows |
|------|-------|
| **Overview** | Cluster health — the main application + its workers first, then each **partner** with its flows (send → / ← receive) and that partner's workers. Master health is gathered by polling peer endpoints from `applications.yml`. |
| **Requests** | Audit log of every `POST /api/transfers` (accepted or rejected) with outcome, flow, source→target, client CN, status, and reason. Filterable + paginated. |
| **File Transfers** | Full-width, filterable (flow / worker / status / search), sortable by date, paginated. Columns include flow id, worker node, created/updated. Click a row for the full **detail** page (all fields + per-chunk table). |
| **Flows** | Configured flows with sender/receiver applications, sandboxes, CNs, and permission pills; browse each endpoint's sandbox. |
| **Initiate Transfer** | Pick a flow, browse the sender sandbox, submit. |

## Layout

```
main.go                      entry point — dispatches to master/worker
internal/config/             config loading (yaml + env overrides)
internal/apps/               application registry (applications.yml)
internal/flows/              flow model, permissions, sandbox resolution/browse
internal/model/              shared domain types + status enums
internal/db/                 Postgres connection + schema.sql migration
internal/master/             HTTP server, REST API, Postgres-backed store
internal/worker/             task-claim loop + chunked transfer engine
internal/transfer/           chunking, checksums, metadata handshake types
internal/master/ui/          master UI (embedded, served by the master)

<deployment> = FT_HOME (every path in config.yml is relative to it):
  bin/     start.sh · stop.sh
  config/  config.yml · applications.yml
  flows/   <flow_identifier>.yml
  certs/   <app>-cert.crt · <app>-cert.key · CAs/ (self + partner certs)
  lib/     filetransfer binary
  logs/    filetransfer.log · transfers.log (rotating) · <node>.out/.pid
  tmp/     chunk staging (the sandbox may live here in dev, or at an absolute file_path)
```

## REST API (summary)

| Method + path | Auth | Purpose |
|---------------|------|---------|
| `GET /healthz` | open | liveness |
| `GET /api/cluster` | open | master + worker health across applications, and partners |
| `GET /api/applications` | open | application → endpoint registry |
| `GET /api/flows` | open | configured flows (with permissions) |
| `GET /api/flows/browse?flow=&role=&path=` | open (needs `list`) | list a sandbox folder |
| `GET /api/transfers` · `GET /api/transfers/{id}` | open | transfers + chunk detail |
| `GET /api/worker-transfers` | open | transfers joined with the processing worker |
| `GET /api/requests` | open | request audit log |
| `GET /api/nodes` | open | active worker nodes |
| `POST /api/transfers` | flow-authorized | register a transfer (sender needs read+write) |
| `DELETE /api/flows/file?flow=&role=&path=` | mTLS + `delete` | delete a sandbox file |
| `POST /api/tasks/*`, `POST /api/nodes/register` | mTLS | worker coordination |
| `POST /api/receive/manifest` | mTLS + flow sender CN | receiver accepts an incoming manifest |
| `PUT /api/receive/chunks/{id}/{seq}` | mTLS + flow sender CN | receiver verifies and stages one chunk |
| `POST /api/receive/{id}/complete` | mTLS + flow sender CN | receiver verifies whole-file checksum and atomically moves into place |

## Status

Working today: flow-authorized, permissioned, checksum-verified transfers; request
auditing; cross-environment **cluster/partner health** (each master polls the other's
endpoint over mTLS); remote sender-worker → receiver-master chunk push; receiver-side
chunk staging; per-chunk and whole-file SHA-256 verification; and the full UI.
Master↔master, worker↔master and sender-worker↔receiver-master traffic can all run as
mTLS over the network between environments.

For cross-application flows, the sender worker resolves the receiver from
`applications.yml`, posts the manifest to the receiver master, streams each chunk over
HTTPS/mTLS, and asks the receiver to finalize. The receiver stages chunks as a hidden
`.part` file in the receiver sandbox directory, verifies the assembled checksum, and then
renames it into place. Same-application flows still use the local filesystem delivery path.
S3 multipart is planned.
