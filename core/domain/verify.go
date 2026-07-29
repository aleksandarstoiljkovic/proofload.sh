package domain

import "time"

// VerifyModel selects the correctness checker applied after (or during) a run.
// Each model imposes requirements on the workload and on what the runner records.
type VerifyModel string

const (
	// VerifyNone disables verification.
	VerifyNone VerifyModel = ""
	// VerifyReconciliation replays recorded committed writes and asserts no loss,
	// no unexpected duplication, matching checksums/counts, plus cluster
	// convergence and staleness. Pure Go; works for any key/value target.
	VerifyReconciliation VerifyModel = "reconciliation"
	// VerifyRegister checks single-object linearizability via Porcupine.
	VerifyRegister VerifyModel = "register"
	// VerifyListAppend checks transactional isolation via Elle (list-append).
	VerifyListAppend VerifyModel = "list-append"
	// VerifyKafkaLog checks message loss/duplication/ordering and end-to-end
	// latency against configured delivery semantics.
	VerifyKafkaLog VerifyModel = "kafka-log"
)

// Verdict is the top-level outcome of a verification.
type Verdict string

const (
	VerdictPass    Verdict = "pass"
	VerdictFail    Verdict = "fail"
	VerdictUnknown Verdict = "unknown"
)

// Anomaly is a single correctness violation with enough context to reproduce it.
type Anomaly struct {
	Kind    string   `json:"kind"` // e.g. "G2-item", "lost-update", "message-loss"
	Detail  string   `json:"detail"`
	Witness []string `json:"witness,omitempty"` // op ids / offsets implicated
}

// VerifyReport is the structured result persisted as verify.json and ingested
// into the warehouse so correctness is trackable across runs.
type VerifyReport struct {
	Model        VerifyModel    `json:"model"`
	Verdict      Verdict        `json:"verdict"`
	Anomalies    []Anomaly      `json:"anomalies,omitempty"`
	Checked      int64          `json:"checked"`
	Lost         int64          `json:"lost"`
	Duplicated   int64          `json:"duplicated"`
	OrderingViol int64          `json:"ordering_violations"`
	ConvergedIn  time.Duration  `json:"converged_in,omitempty"`
	MaxStaleness time.Duration  `json:"max_staleness,omitempty"`
	Extra        map[string]any `json:"extra,omitempty"`
}

// FaultType enumerates the nemesis actions a FaultController can perform.
type FaultType string

const (
	FaultKillNode    FaultType = "kill-node"
	FaultPauseNode   FaultType = "pause-node"
	FaultPartition   FaultType = "network-partition"
	FaultClockSkew   FaultType = "clock-skew"
	FaultLeaderElect FaultType = "leader-election"
	FaultISRShrink   FaultType = "isr-shrink"
)

// Fault is a single nemesis action. Target is a node ID, or empty to let the
// scheduler choose (seeded, so runs are reproducible).
type Fault struct {
	Type   FaultType      `json:"type"`
	Target string         `json:"target,omitempty"`
	Params map[string]any `json:"params,omitempty"`
}

// FaultSpec schedules a fault relative to the start of the measure phase.
type FaultSpec struct {
	Fault    Fault         `json:"fault"`
	At       time.Duration `json:"at"`
	Duration time.Duration `json:"duration"`         // heal after this long
	Repeat   time.Duration `json:"repeat,omitempty"` // 0 = once
}
