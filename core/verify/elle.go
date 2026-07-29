package verify

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/proofload/proofload/core/domain"
)

// elleBinary is the external checker proofload shells out to. Elle is a Clojure
// library; elle-cli (https://github.com/ligurio/elle-cli) exposes it as a
// standalone JVM binary that consumes an EDN history file.
const elleBinary = "elle-cli"

// WriteElleEDN emits an Elle list-append history to path, one EDN map per line
// (an :invoke immediately followed by its :ok/:fail completion). Each recorded
// Event becomes a single-mop transaction:
//
//	append : {:type :invoke :process p :value [[:append k e]]}
//	         {:type :ok     :process p :value [[:append k e]]}
//	read   : {:type :invoke :process p :value [[:r k nil]]}
//	         {:type :ok     :process p :value [[:r k [e ...]]]}
//
// An append that also read the list back adds a trailing [:r k [..]] mop. A
// non-OK operation completes as :fail. The value tokens are this package's
// Checksums; see the value-distinguishability note on Event for why these are
// only meaningful once the workload writes unique tokens per append.
func WriteElleEDN(h *History, path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	bw := bufio.NewWriter(f)
	for _, e := range h.All() {
		fmt.Fprintf(bw, "{:type :invoke, :process %d, :value %s}\n", e.Process, mops(e, false))
		completion := ":ok"
		if !e.OK {
			completion = ":fail"
		}
		fmt.Fprintf(bw, "{:type %s, :process %d, :value %s}\n", completion, e.Process, mops(e, true))
	}
	if err := bw.Flush(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// mops renders an Event's micro-operation vector. On invoke, reads carry an
// unknown value (nil); on completion they carry the observed list.
func mops(e Event, complete bool) string {
	switch e.F {
	case fAppend:
		v := fmt.Sprintf("[:append %d %d]", e.Key, e.WVal)
		if complete && len(e.RList) > 0 {
			return fmt.Sprintf("[%s [:r %d %s]]", v, e.Key, ednList(e.RList))
		}
		return "[" + v + "]"
	case fRead:
		if complete {
			return fmt.Sprintf("[[:r %d %s]]", e.Key, ednList(e.RList))
		}
		return fmt.Sprintf("[[:r %d nil]]", e.Key)
	default: // fWrite has no list-append representation; emit an empty txn.
		return "[]"
	}
}

// ednList renders tokens as an EDN vector, e.g. [1 2 3]. A nil/empty list is [].
func ednList(list []int64) string {
	if len(list) == 0 {
		return "[]"
	}
	parts := make([]string, len(list))
	for i, e := range list {
		parts[i] = fmt.Sprintf("%d", e)
	}
	return "[" + strings.Join(parts, " ") + "]"
}

// RunElle checks the list-append history at ednPath with the external elle-cli
// binary. If the binary is not on PATH, it degrades gracefully to a
// VerdictUnknown "tooling" report rather than failing — proofload does not
// require a JVM toolchain to run. When present, elle-cli's output is parsed for
// the :valid? verdict; anomalies surface as a single anomaly with the detail.
func RunElle(ctx context.Context, ednPath string) domain.VerifyReport {
	report := domain.VerifyReport{Model: domain.VerifyListAppend}
	bin, err := exec.LookPath(elleBinary)
	if err != nil {
		report.Verdict = domain.VerdictUnknown
		report.Anomalies = []domain.Anomaly{{
			Kind:   "tooling",
			Detail: "elle-cli not installed",
		}}
		return report
	}
	out, runErr := exec.CommandContext(ctx, bin, "--model", "list-append", ednPath).CombinedOutput()
	return parseElleOutput(string(out), runErr, report)
}

// parseElleOutput maps elle-cli's textual output onto a VerifyReport. elle-cli
// prints an EDN result whose :valid? field is the verdict. A missing/ambiguous
// verdict is reported Unknown so a parsing gap never masquerades as a pass.
func parseElleOutput(out string, runErr error, report domain.VerifyReport) domain.VerifyReport {
	switch {
	case strings.Contains(out, ":valid? true"):
		report.Verdict = domain.VerdictPass
	case strings.Contains(out, ":valid? false") || strings.Contains(out, ":anomal"):
		report.Verdict = domain.VerdictFail
		report.Anomalies = append(report.Anomalies, domain.Anomaly{
			Kind:   "isolation",
			Detail: strings.TrimSpace(out),
		})
	default:
		report.Verdict = domain.VerdictUnknown
		detail := strings.TrimSpace(out)
		if runErr != nil {
			detail = fmt.Sprintf("elle-cli: %v: %s", runErr, detail)
		}
		report.Anomalies = append(report.Anomalies, domain.Anomaly{
			Kind:   "tooling",
			Detail: fmt.Sprintf("could not parse elle-cli verdict: %s", detail),
		})
	}
	return report
}
