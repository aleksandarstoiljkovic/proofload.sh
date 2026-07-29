package verify_test

import (
	"strconv"
	"testing"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/verify"
)

// TestCheckRegister exercises the per-key register linearizability checker on
// hand-built synthetic histories: a linearizable key, a stale-read key that
// violates real-time order, and a two-key mix where one key is broken.
func TestCheckRegister(t *testing.T) {
	tests := []struct {
		name        string
		events      []verify.Event
		wantVerdict domain.Verdict
		wantChecked int64
		wantBadKey  int64 // -1 = expect no non-linearizable anomaly
	}{
		{
			name: "linearizable write-then-read",
			events: []verify.Event{
				{Process: 0, F: "write", Key: 1, WVal: 1, Invoke: 10, Complete: 20, OK: true},
				{Process: 0, F: "read", Key: 1, RVal: 1, Invoke: 30, Complete: 40, OK: true},
			},
			wantVerdict: domain.VerdictPass,
			wantChecked: 1,
			wantBadKey:  -1,
		},
		{
			name: "read of never-written observes empty",
			events: []verify.Event{
				{Process: 0, F: "read", Key: 9, RVal: 0, Invoke: 10, Complete: 20, OK: true},
			},
			wantVerdict: domain.VerdictPass,
			wantChecked: 1,
			wantBadKey:  -1,
		},
		{
			name: "stale read violates real-time order",
			events: []verify.Event{
				{Process: 0, F: "write", Key: 1, WVal: 111, Invoke: 10, Complete: 20, OK: true},
				// Read starts strictly after the write completed, yet observes
				// the empty value: no linearization can explain this.
				{Process: 1, F: "read", Key: 1, RVal: 0, Invoke: 30, Complete: 40, OK: true},
			},
			wantVerdict: domain.VerdictFail,
			wantChecked: 1,
			wantBadKey:  1,
		},
		{
			name: "one good key, one broken key",
			events: []verify.Event{
				{Process: 0, F: "write", Key: 1, WVal: 5, Invoke: 10, Complete: 20, OK: true},
				{Process: 0, F: "read", Key: 1, RVal: 5, Invoke: 30, Complete: 40, OK: true},
				{Process: 0, F: "write", Key: 2, WVal: 7, Invoke: 10, Complete: 20, OK: true},
				{Process: 1, F: "read", Key: 2, RVal: 0, Invoke: 30, Complete: 40, OK: true},
			},
			wantVerdict: domain.VerdictFail,
			wantChecked: 2,
			wantBadKey:  2,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := writeHistory(t, tc.events)
			report := verify.CheckRegister(h)

			if report.Model != domain.VerifyRegister {
				t.Errorf("Model = %q, want %q", report.Model, domain.VerifyRegister)
			}
			if report.Verdict != tc.wantVerdict {
				t.Errorf("Verdict = %q, want %q (anomalies: %+v)", report.Verdict, tc.wantVerdict, report.Anomalies)
			}
			if report.Checked != tc.wantChecked {
				t.Errorf("Checked = %d, want %d", report.Checked, tc.wantChecked)
			}
			assertBadKey(t, report, tc.wantBadKey)
		})
	}
}

// assertBadKey checks for exactly the expected non-linearizable anomaly (if
// any), with a witness that names the implicated key.
func assertBadKey(t *testing.T, report domain.VerifyReport, wantKey int64) {
	t.Helper()
	var nonLin []domain.Anomaly
	for _, a := range report.Anomalies {
		if a.Kind == "non-linearizable" {
			nonLin = append(nonLin, a)
		}
	}
	if wantKey < 0 {
		if len(nonLin) != 0 {
			t.Errorf("expected no non-linearizable anomaly, got %+v", nonLin)
		}
		return
	}
	if len(nonLin) != 1 {
		t.Fatalf("expected 1 non-linearizable anomaly, got %d: %+v", len(nonLin), nonLin)
	}
	wantWitness := "key=" + strconv.FormatInt(wantKey, 10)
	if len(nonLin[0].Witness) == 0 || nonLin[0].Witness[0] != wantWitness {
		t.Errorf("witness = %v, want first entry %q", nonLin[0].Witness, wantWitness)
	}
}
