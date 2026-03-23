package cluster

import "testing"

func TestRouteForKey(t *testing.T) {
	membership, err := NewMembership(Config{
		SelfID:            "node-a",
		Nodes:             []Node{{ID: "node-a", Address: "http://127.0.0.1:8081"}, {ID: "node-b", Address: "http://127.0.0.1:8082"}},
		ShardCount:        4,
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("new membership: %v", err)
	}

	route := membership.RouteForKey("hello")
	if route.Key != "hello" {
		t.Fatalf("unexpected route key: %q", route.Key)
	}
	if len(route.Replicas) != 2 {
		t.Fatalf("unexpected replica count: %d", len(route.Replicas))
	}
	if route.Replicas[0].Role != RoleLeader {
		t.Fatalf("unexpected leader role: %s", route.Replicas[0].Role)
	}
	if route.Replicas[1].Role != RoleFollower {
		t.Fatalf("unexpected follower role: %s", route.Replicas[1].Role)
	}
	if route.RoleFor(route.Leader.ID) != RoleLeader {
		t.Fatalf("leader node should resolve to leader role")
	}
}

func TestSelfRole(t *testing.T) {
	membership, err := NewMembership(Config{
		SelfID:            "node-b",
		Nodes:             []Node{{ID: "node-a", Address: "http://127.0.0.1:8081"}, {ID: "node-b", Address: "http://127.0.0.1:8082"}},
		ShardCount:        1,
		ReplicationFactor: 2,
	})
	if err != nil {
		t.Fatalf("new membership: %v", err)
	}

	if role := membership.SelfRole("hello"); role != RoleFollower {
		t.Fatalf("unexpected self role: %s", role)
	}
}
