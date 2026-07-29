package kafkadriver

import (
	"testing"

	"github.com/proofload/proofload/core/domain"
)

func TestAnalyzeLogClean(t *testing.T) {
	// Two keys, each with a contiguous monotonic sequence in offset order.
	recs := []logRecord{
		{Partition: 0, Offset: 0, Key: 1, Seq: 0},
		{Partition: 0, Offset: 1, Key: 1, Seq: 1},
		{Partition: 0, Offset: 2, Key: 1, Seq: 2},
		{Partition: 1, Offset: 0, Key: 2, Seq: 0},
		{Partition: 1, Offset: 1, Key: 2, Seq: 1},
	}
	rep := analyzeLog(recs)
	if rep.Verdict != domain.VerdictPass {
		t.Fatalf("verdict = %v, want pass (%+v)", rep.Verdict, rep)
	}
	if rep.Checked != 5 || rep.Lost != 0 || rep.Duplicated != 0 || rep.OrderingViol != 0 {
		t.Errorf("unexpected tallies: %+v", rep)
	}
	if rep.Model != domain.VerifyKafkaLog {
		t.Errorf("model = %v, want kafka-log", rep.Model)
	}
}

func TestAnalyzeLogEmpty(t *testing.T) {
	rep := analyzeLog(nil)
	if rep.Verdict != domain.VerdictUnknown {
		t.Errorf("empty log verdict = %v, want unknown", rep.Verdict)
	}
}

func TestAnalyzeLogLoss(t *testing.T) {
	// key 1 is missing seq 1 (gap between 0 and 2).
	recs := []logRecord{
		{Partition: 0, Offset: 0, Key: 1, Seq: 0},
		{Partition: 0, Offset: 1, Key: 1, Seq: 2},
	}
	rep := analyzeLog(recs)
	if rep.Verdict != domain.VerdictFail {
		t.Fatalf("verdict = %v, want fail", rep.Verdict)
	}
	if rep.Lost != 1 {
		t.Errorf("lost = %d, want 1", rep.Lost)
	}
	if !hasAnomaly(rep, "message-loss") {
		t.Errorf("expected message-loss anomaly, got %+v", rep.Anomalies)
	}
}

func TestAnalyzeLogDuplication(t *testing.T) {
	recs := []logRecord{
		{Partition: 0, Offset: 0, Key: 1, Seq: 0},
		{Partition: 0, Offset: 1, Key: 1, Seq: 1},
		{Partition: 0, Offset: 2, Key: 1, Seq: 1}, // duplicate seq 1
	}
	rep := analyzeLog(recs)
	if rep.Duplicated != 1 {
		t.Errorf("duplicated = %d, want 1 (%+v)", rep.Duplicated, rep)
	}
	if rep.Verdict != domain.VerdictFail {
		t.Errorf("verdict = %v, want fail", rep.Verdict)
	}
	if !hasAnomaly(rep, "duplication") {
		t.Errorf("expected duplication anomaly")
	}
}

func TestAnalyzeLogOrdering(t *testing.T) {
	// Within one partition, key 1's sequence goes backwards at offset 2.
	recs := []logRecord{
		{Partition: 0, Offset: 0, Key: 1, Seq: 0},
		{Partition: 0, Offset: 1, Key: 1, Seq: 2},
		{Partition: 0, Offset: 2, Key: 1, Seq: 1}, // 1 < prev 2 -> ordering violation
	}
	rep := analyzeLog(recs)
	if rep.OrderingViol != 1 {
		t.Errorf("ordering violations = %d, want 1 (%+v)", rep.OrderingViol, rep)
	}
	// Seq 1 is present, 0..2 all present, so no loss/dup — only ordering fails.
	if rep.Lost != 0 || rep.Duplicated != 0 {
		t.Errorf("expected only ordering violation, got %+v", rep)
	}
	if rep.Verdict != domain.VerdictFail {
		t.Errorf("verdict = %v, want fail", rep.Verdict)
	}
	if !hasAnomaly(rep, "ordering") {
		t.Errorf("expected ordering anomaly")
	}
}

func hasAnomaly(rep domain.VerifyReport, kind string) bool {
	for _, a := range rep.Anomalies {
		if a.Kind == kind {
			return true
		}
	}
	return false
}
