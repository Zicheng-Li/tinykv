# TinyKV

TinyKV is a small Go-based key-value store project that is being built in phases.

Current focus:

- a durable single-node KV engine
- an HTTP API for reads and writes
- a static Phase 2 cluster layer with sharding, forwarding, and best-effort replication

## Current Status

### Implemented

- single-node `PUT / GET / DELETE`
- WAL-based persistence and recovery
- compaction for rewriting live keys
- concurrent access protection in the storage engine
- HTTP API for KV operations
- snapshot / restore support
- static cluster membership
- shard routing abstraction
- leader/follower role abstraction per shard
- node-to-node HTTP forwarding
- best-effort replication to follower replicas
- cluster inspection endpoints

### Not Implemented Yet

- TTL
- dynamic cluster membership
- quorum-based replication
- Raft leader election
- heartbeats
- log replication
- commit index
- crash recovery for distributed replication

## Project Layout

- `engine/`: local storage engine, WAL, index rebuild, cache, compaction, snapshot / restore
- `server/`: HTTP API, cluster-aware routing, forwarding, replication hooks, snapshot endpoints
- `cluster/`: membership, shard routing, replica roles, peer HTTP client
- `cmd/tinykv/`: executable entrypoint

## HTTP API

### Single-Node KV Endpoints

- `PUT /kv/{key}`
  - request body: raw value bytes
  - success: `204 No Content`
- `GET /kv/{key}`
  - success: `200 OK`
  - response body: raw value bytes
- `DELETE /kv/{key}`
  - success: `204 No Content`
- `GET /snapshot`
  - returns a binary snapshot of the current local node state
- `POST /restore`
  - request body: raw snapshot bytes previously created by `GET /snapshot`
  - success: `204 No Content`
- `GET /healthz`
  - success: `200 OK`
  - response body: `ok`

### Cluster Debug Endpoints

- `GET /cluster/membership`
  - returns the local node view of cluster membership, shard count, and replication factor
- `GET /cluster/route/{key}`
  - returns the shard route for a key, including the leader and follower replicas

## Run In Single-Node Mode

```bash
go run ./cmd/tinykv \
  -addr 127.0.0.1:8080 \
  -data ./tinykv.data
```

Example requests:

```bash
curl -i -X PUT --data 'world' http://127.0.0.1:8080/kv/hello
curl -i http://127.0.0.1:8080/kv/hello
curl -i -X DELETE http://127.0.0.1:8080/kv/hello
curl -o backup.snapshot http://127.0.0.1:8080/snapshot
curl -i -X POST --data-binary @backup.snapshot http://127.0.0.1:8080/restore
curl -i http://127.0.0.1:8080/healthz
```

## Run In Static Cluster Mode

All nodes must start with the same:

- `-peers`
- `-shards`
- `-replicas`

Each node must use its own:

- `-addr`
- `-data`
- `-node-id`

Example: two-node cluster with replication factor `2`

Terminal 1:

```bash
go run ./cmd/tinykv \
  -addr 127.0.0.1:8081 \
  -data ./node-a.data \
  -node-id node-a \
  -peers node-a=http://127.0.0.1:8081,node-b=http://127.0.0.1:8082 \
  -shards 8 \
  -replicas 2
```

Terminal 2:

```bash
go run ./cmd/tinykv \
  -addr 127.0.0.1:8082 \
  -data ./node-b.data \
  -node-id node-b \
  -peers node-a=http://127.0.0.1:8081,node-b=http://127.0.0.1:8082 \
  -shards 8 \
  -replicas 2
```

Useful checks:

```bash
curl -i http://127.0.0.1:8081/cluster/membership
curl -i http://127.0.0.1:8081/cluster/route/hello
curl -i -X PUT --data 'world' http://127.0.0.1:8082/kv/hello
curl -i http://127.0.0.1:8081/kv/hello
curl -i http://127.0.0.1:8082/kv/hello
curl -o node-a.snapshot http://127.0.0.1:8081/snapshot
```

## Phase 2 Behavior

The current Phase 2 layer is intentionally simple:

- shard ownership is deterministic and static
- the first replica in a shard route acts as leader
- follower nodes accept replicated writes from the leader
- client writes sent to non-leader nodes are proxied to the leader
- replication is best-effort and synchronous
- snapshot / restore is currently node-local administration, not a cluster-wide consistent snapshot

This is a useful abstraction layer for Phase 3, but it is not a consensus system yet.

## Limitations

- there is no Raft or quorum logic yet
- replication can become inconsistent if a follower is unavailable during a write
- membership is static and configured at startup
- there is no TTL eviction yet
- restoring a snapshot in cluster mode only replaces the local node state

## Testing

```bash
go test ./...
go test -race ./...
```

## Next Steps

Recommended next implementation order:

1. TTL
2. stronger cluster write guarantees
3. cluster-aware snapshot coordination
4. Phase 3 Raft-based replication
