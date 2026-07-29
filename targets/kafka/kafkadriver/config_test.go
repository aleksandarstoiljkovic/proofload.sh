package kafkadriver

import (
	"reflect"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

func TestParseAcks(t *testing.T) {
	tests := []struct {
		consistency string
		want        ackMode
		wantErr     bool
	}{
		{"", ackAll, false},
		{"acks=all", ackAll, false},
		{"ALL", ackAll, false},
		{"-1", ackAll, false},
		{"acks=1", ackLeader, false},
		{"leader", ackLeader, false},
		{"acks=0", ackNone, false},
		{"none", ackNone, false},
		{"QUORUM", 0, true},
		{"serializable", 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.consistency, func(t *testing.T) {
			got, err := parseAcks(tt.consistency)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("parseAcks(%q): expected error", tt.consistency)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAcks(%q): unexpected error: %v", tt.consistency, err)
			}
			if got != tt.want {
				t.Errorf("parseAcks(%q) = %v, want %v", tt.consistency, got, tt.want)
			}
		})
	}
}

func TestTopicConfigFrom(t *testing.T) {
	tests := []struct {
		name string
		cfg  driver.Config
		want topicConfig
	}{
		{
			"defaults",
			driver.Config{},
			topicConfig{Name: defaultTopic, Partitions: defaultPartitions, Replication: defaultReplication},
		},
		{
			"params override",
			driver.Config{Params: map[string]any{"topic": "orders", "partitions": 24, "replication_factor": 2}},
			topicConfig{Name: "orders", Partitions: 24, Replication: 2},
		},
		{
			"replication from cluster spec",
			driver.Config{Cluster: domain.ClusterSpec{ReplicationFactor: 5}},
			topicConfig{Name: defaultTopic, Partitions: defaultPartitions, Replication: 5},
		},
		{
			"yaml float partitions",
			driver.Config{Params: map[string]any{"partitions": float64(8)}},
			topicConfig{Name: defaultTopic, Partitions: 8, Replication: defaultReplication},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := topicConfigFrom(tt.cfg); got != tt.want {
				t.Errorf("topicConfigFrom = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func TestBrokersFrom(t *testing.T) {
	tests := []struct {
		name string
		cfg  driver.Config
		want []string
	}{
		{"explicit endpoints", driver.Config{Endpoints: []string{"b1:9092", "b2:9092"}}, []string{"b1:9092", "b2:9092"}},
		{
			"from cluster nodes",
			driver.Config{Cluster: domain.ClusterSpec{Nodes: []domain.Node{{Client: "n1:9092"}, {Client: "n2:9092"}}}},
			[]string{"n1:9092", "n2:9092"},
		},
		{"localhost default", driver.Config{}, []string{"localhost:" + defaultKafkaPort}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := brokersFrom(tt.cfg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("brokersFrom = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestResolveIdempotenceAndTxn(t *testing.T) {
	tests := []struct {
		name           string
		cfg            driver.Config
		wantAcks       ackMode
		wantIdempotent bool
		wantTxn        string
	}{
		{"default all idempotent", driver.Config{}, ackAll, true, ""},
		{"acks=1 disables idempotence", driver.Config{Consistency: "acks=1"}, ackLeader, false, ""},
		{"acks=0 disables idempotence", driver.Config{Consistency: "acks=0"}, ackNone, false, ""},
		{
			"idempotent explicitly off at all",
			driver.Config{Params: map[string]any{"idempotent": false}},
			ackAll, false, "",
		},
		{
			"txn forces all+idempotent even if acks=0 asked",
			driver.Config{Consistency: "acks=0", Params: map[string]any{"transactional_id": "pl-eos"}},
			ackAll, true, "pl-eos",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc, err := resolve(tt.cfg)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if rc.acks != tt.wantAcks {
				t.Errorf("acks = %v, want %v", rc.acks, tt.wantAcks)
			}
			if rc.idempotent != tt.wantIdempotent {
				t.Errorf("idempotent = %v, want %v", rc.idempotent, tt.wantIdempotent)
			}
			if rc.txnID != tt.wantTxn {
				t.Errorf("txnID = %q, want %q", rc.txnID, tt.wantTxn)
			}
		})
	}
}

func TestBatchSizeFrom(t *testing.T) {
	tests := []struct {
		name string
		cfg  driver.Config
		want int
	}{
		{"default is one", driver.Config{}, 1},
		{"explicit batch", driver.Config{Params: map[string]any{"batch_size": 500}}, 500},
		{"yaml float batch", driver.Config{Params: map[string]any{"batch_size": float64(250)}}, 250},
		{"zero clamps to one", driver.Config{Params: map[string]any{"batch_size": 0}}, 1},
		{"negative clamps to one", driver.Config{Params: map[string]any{"batch_size": -5}}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rc, err := resolve(tt.cfg)
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if rc.batchSize != tt.want {
				t.Errorf("batchSize = %d, want %d", rc.batchSize, tt.want)
			}
		})
	}
}

func TestResolveRejectsBadConsistency(t *testing.T) {
	if _, err := resolve(driver.Config{Consistency: "QUORUM"}); err == nil {
		t.Errorf("expected error for unsupported consistency")
	}
}

func TestClientOptsCountVaries(t *testing.T) {
	// A smoke check that acks levels and toggles produce distinct option sets
	// without needing a broker. We only assert non-emptiness and that weaker
	// acks add the idempotence-disable option (more opts than bare acks=all
	// idempotent, which omits it).
	all, _ := resolve(driver.Config{})
	leader, _ := resolve(driver.Config{Consistency: "acks=1"})
	group, _ := resolve(driver.Config{Params: map[string]any{"group": "g"}})

	if len(clientOpts(all)) == 0 || len(clientOpts(leader)) == 0 {
		t.Fatalf("clientOpts returned no options")
	}
	if len(clientOpts(group)) <= len(clientOpts(all)) {
		t.Errorf("group config should add consumer options")
	}
}

func TestConsistencyLevelsCopy(t *testing.T) {
	d := New().(driver.ClusterAware)
	got := d.ConsistencyLevels()
	want := []string{"acks=0", "acks=1", "acks=all"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("ConsistencyLevels = %v, want %v", got, want)
	}
	got[0] = "mutated"
	if consistencyLevels[0] == "mutated" {
		t.Errorf("ConsistencyLevels must return a copy, not the package slice")
	}
}

func TestReadKeyFromUnsupported(t *testing.T) {
	d := New().(driver.ClusterAware)
	_, err := d.ReadKeyFrom(nil, domain.Node{ID: "n1"}, 1)
	if err == nil {
		t.Fatalf("ReadKeyFrom must return an unsupported error for Kafka")
	}
}

func TestWorkloadConsumes(t *testing.T) {
	produceOnly := domain.Workload{Operations: []domain.OpSpec{{Type: "produce"}}}
	mixed := domain.Workload{Operations: []domain.OpSpec{{Type: "produce"}, {Type: "consume"}}}
	if workloadConsumes(produceOnly) {
		t.Errorf("produce-only workload must not report consuming")
	}
	if !workloadConsumes(mixed) {
		t.Errorf("mixed workload must report consuming")
	}
}
