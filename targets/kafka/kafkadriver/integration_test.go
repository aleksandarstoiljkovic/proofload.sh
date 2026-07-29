package kafkadriver

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
)

// brokersEnv enables the integration tests. When unset the tests skip, so the
// default `go test ./targets/kafka/...` needs no broker.
const brokersEnv = "PROOFLOAD_KAFKA_BROKERS"

// integrationConfig returns a driver.Config pointed at the brokers from the
// environment, or skips the test when they are absent.
func integrationConfig(t *testing.T) driver.Config {
	t.Helper()
	raw := envOrSkip(t)
	return driver.Config{
		Endpoints:   strings.Split(raw, ","),
		Consistency: "acks=all",
		Params:      map[string]any{"topic": "proofload-itest", "partitions": 3, "replication_factor": 1},
	}
}

func envOrSkip(t *testing.T) string {
	t.Helper()
	v := os.Getenv(brokersEnv)
	if v == "" {
		t.Skipf("%s not set; skipping Kafka integration test", brokersEnv)
	}
	return v
}

func TestIntegrationProduceConsume(t *testing.T) {
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := New()
	w := domain.Workload{
		Name:       "itest",
		Mode:       domain.ModePerformance,
		Operations: []domain.OpSpec{{Type: "produce", Weight: 1}, {Type: "consume", Weight: 1}},
	}
	if err := d.Schema(ctx, cfg, w); err != nil {
		t.Fatalf("Schema: %v", err)
	}

	conn, err := d.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Prepare(ctx, w); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	key := time.Now().UnixNano()
	val := []byte("integration-value")
	if r := conn.Execute(ctx, domain.Operation{Type: "produce", Key: key, Value: val, Seq: 1}); r.Err != nil {
		t.Fatalf("produce: %v", r.Err)
	}

	// Poll for the record we just wrote. It may take a couple of polls for the
	// direct consumer to observe it.
	deadline := time.Now().Add(20 * time.Second)
	var got []byte
	for time.Now().Before(deadline) {
		r := conn.Execute(ctx, domain.Operation{Type: "consume"})
		if r.Err != nil {
			t.Fatalf("consume: %v", r.Err)
		}
		if b, ok := r.Observed.([]byte); ok && b != nil {
			got = b
			break
		}
	}
	if got == nil {
		t.Fatalf("did not observe any consumed record before deadline")
	}
}

func TestIntegrationVerify(t *testing.T) {
	cfg := integrationConfig(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	d := New()
	w := domain.Workload{Name: "itest", Operations: []domain.OpSpec{{Type: "produce", Weight: 1}}}
	if err := d.Schema(ctx, cfg, w); err != nil {
		t.Fatalf("Schema: %v", err)
	}
	conn, err := d.Connect(ctx, cfg)
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer conn.Close()
	if err := conn.Prepare(ctx, w); err != nil {
		t.Fatalf("Prepare: %v", err)
	}

	// Produce a small contiguous per-key sequence.
	key := time.Now().UnixNano()
	for seq := int64(0); seq < 5; seq++ {
		r := conn.Execute(ctx, domain.Operation{Type: "produce", Key: key, Value: []byte("v"), Seq: seq})
		if r.Err != nil {
			t.Fatalf("produce seq %d: %v", seq, r.Err)
		}
	}

	v := d.(driver.Verifier)
	if v.Model() != domain.VerifyKafkaLog {
		t.Fatalf("Model = %v, want kafka-log", v.Model())
	}
	rep, err := v.Verify(ctx, driver.RunArtifacts{Cfg: cfg})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if rep.Checked == 0 {
		t.Fatalf("verify checked 0 records")
	}
}
