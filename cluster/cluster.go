package cluster

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
)

var (
	ErrEmptyMembership = errors.New("cluster membership requires at least one node")
	ErrSelfNotFound    = errors.New("self node not found in cluster membership")
)

type Role string

const (
	RoleLeader   Role = "leader"
	RoleFollower Role = "follower"
	RoleNone     Role = "none"
)

type Node struct {
	ID      string `json:"id"`
	Address string `json:"address"`
}

type Replica struct {
	Node Node `json:"node"`
	Role Role `json:"role"`
}

type Route struct {
	Key      string    `json:"key"`
	Shard    int       `json:"shard"`
	Leader   Node      `json:"leader"`
	Replicas []Replica `json:"replicas"`
}

type Summary struct {
	Enabled           bool   `json:"enabled"`
	Self              Node   `json:"self,omitempty"`
	Nodes             []Node `json:"nodes,omitempty"`
	ShardCount        int    `json:"shard_count,omitempty"`
	ReplicationFactor int    `json:"replication_factor,omitempty"`
}

type Config struct {
	SelfID            string
	Nodes             []Node
	ShardCount        int
	ReplicationFactor int
}

type Membership struct {
	self              Node
	nodes             []Node
	shardCount        int
	replicationFactor int
}

func NewMembership(cfg Config) (*Membership, error) {
	if len(cfg.Nodes) == 0 {
		return nil, ErrEmptyMembership
	}
	if cfg.SelfID == "" {
		return nil, fmt.Errorf("self id is required")
	}

	nodes := make([]Node, 0, len(cfg.Nodes))
	seen := make(map[string]struct{}, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		node.ID = strings.TrimSpace(node.ID)
		node.Address = strings.TrimRight(strings.TrimSpace(node.Address), "/")
		if node.ID == "" {
			return nil, fmt.Errorf("node id is required")
		}
		if node.Address == "" {
			return nil, fmt.Errorf("node %q address is required", node.ID)
		}
		if _, ok := seen[node.ID]; ok {
			return nil, fmt.Errorf("duplicate node id %q", node.ID)
		}
		seen[node.ID] = struct{}{}
		nodes = append(nodes, node)
	}

	sort.Slice(nodes, func(i, j int) bool {
		return nodes[i].ID < nodes[j].ID
	})

	shardCount := cfg.ShardCount
	if shardCount <= 0 {
		shardCount = 1
	}
	replicationFactor := cfg.ReplicationFactor
	if replicationFactor <= 0 {
		replicationFactor = 1
	}
	if replicationFactor > len(nodes) {
		replicationFactor = len(nodes)
	}

	membership := &Membership{
		nodes:             nodes,
		shardCount:        shardCount,
		replicationFactor: replicationFactor,
	}
	for _, node := range nodes {
		if node.ID == cfg.SelfID {
			membership.self = node
			return membership, nil
		}
	}
	return nil, ErrSelfNotFound
}

func (m *Membership) Enabled() bool {
	return m != nil
}

func (m *Membership) Self() Node {
	if m == nil {
		return Node{}
	}
	return m.self
}

func (m *Membership) Nodes() []Node {
	if m == nil {
		return nil
	}
	nodes := make([]Node, len(m.nodes))
	copy(nodes, m.nodes)
	return nodes
}

func (m *Membership) Summary() Summary {
	if m == nil {
		return Summary{Enabled: false}
	}
	return Summary{
		Enabled:           true,
		Self:              m.self,
		Nodes:             m.Nodes(),
		ShardCount:        m.shardCount,
		ReplicationFactor: m.replicationFactor,
	}
}

func (m *Membership) RouteForKey(key string) Route {
	if m == nil {
		return Route{Key: key}
	}

	shard := shardForKey(key, m.shardCount)
	start := shard % len(m.nodes)
	replicas := make([]Replica, 0, m.replicationFactor)
	for i := 0; i < m.replicationFactor; i++ {
		role := RoleFollower
		if i == 0 {
			role = RoleLeader
		}
		replicas = append(replicas, Replica{
			Node: m.nodes[(start+i)%len(m.nodes)],
			Role: role,
		})
	}

	route := Route{
		Key:      key,
		Shard:    shard,
		Replicas: replicas,
	}
	if len(replicas) > 0 {
		route.Leader = replicas[0].Node
	}
	return route
}

func (m *Membership) SelfRole(key string) Role {
	if m == nil {
		return RoleNone
	}
	return m.RouteForKey(key).RoleFor(m.self.ID)
}

func (r Route) RoleFor(nodeID string) Role {
	for _, replica := range r.Replicas {
		if replica.Node.ID == nodeID {
			return replica.Role
		}
	}
	return RoleNone
}

func (r Route) HasNode(nodeID string) bool {
	return r.RoleFor(nodeID) != RoleNone
}

func (r Route) Followers() []Node {
	followers := make([]Node, 0, len(r.Replicas))
	for _, replica := range r.Replicas {
		if replica.Role == RoleFollower {
			followers = append(followers, replica.Node)
		}
	}
	return followers
}

func shardForKey(key string, shardCount int) int {
	hasher := fnv.New32a()
	_, _ = hasher.Write([]byte(key))
	return int(hasher.Sum32() % uint32(shardCount))
}
