package kafkadriver

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/proofload/proofload/core/domain"
	"github.com/proofload/proofload/core/driver"
	"github.com/twmb/franz-go/pkg/kgo"
)

// verify.go implements the optional driver.Verifier capability for the
// kafka-log model. It consumes the whole topic and, using the per-key sequence
// numbers embedded in record headers by recordFrom, reports message loss
// (gaps), duplication, and per-partition ordering violations. The analysis
// (analyzeLog) is a pure function so it is unit tested without a broker; the
// broker-dependent part only drains the topic into logRecords.

// idleDrainTimeout bounds how long the verifier waits for more records before
// concluding the topic has been fully drained.
const idleDrainTimeout = 3 * time.Second

// Model implements driver.Verifier.
func (*kafkaDriver) Model() domain.VerifyModel { return domain.VerifyKafkaLog }

// Verify consumes the configured topic from the beginning and analyses the
// per-key sequence stream. It builds its own consumer from the recorded run
// config so it can run standalone after a load phase.
func (d *kafkaDriver) Verify(ctx context.Context, art driver.RunArtifacts) (domain.VerifyReport, error) {
	rc, err := resolve(art.Cfg)
	if err != nil {
		return domain.VerifyReport{Model: domain.VerifyKafkaLog}, err
	}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(rc.brokers...),
		kgo.ConsumeTopics(rc.topic.Name),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	if err != nil {
		return domain.VerifyReport{Model: domain.VerifyKafkaLog}, fmt.Errorf("kafkadriver: verify client: %w", err)
	}
	defer cl.Close()

	recs, err := drainTopic(ctx, cl)
	if err != nil {
		return domain.VerifyReport{Model: domain.VerifyKafkaLog}, err
	}
	return analyzeLog(recs), nil
}

// logRecord is the minimal projection of a Kafka record the analysis needs.
type logRecord struct {
	Partition int32
	Offset    int64
	Key       int64
	Seq       int64
}

// drainTopic polls until an idle window elapses with no new records, decoding
// each record's key and sequence header. Records that are not proofload records
// (unparseable key or missing/short seq header) are skipped.
func drainTopic(ctx context.Context, cl *kgo.Client) ([]logRecord, error) {
	var out []logRecord
	for {
		pollCtx, cancel := context.WithTimeout(ctx, idleDrainTimeout)
		fetches := cl.PollFetches(pollCtx)
		cancel()

		if err := ctx.Err(); err != nil {
			return out, err
		}
		if err := fetches.Err(); err != nil {
			// A poll that times out on the idle window signals end-of-topic.
			if ctx.Err() == nil {
				return out, nil
			}
			return out, err
		}

		recs := fetches.Records()
		if len(recs) == 0 {
			return out, nil
		}
		for _, r := range recs {
			key, kerr := keyFromBytes(r.Key)
			seq, serr := seqFromHeaders(r.Headers)
			if kerr != nil || serr != nil {
				continue
			}
			out = append(out, logRecord{Partition: r.Partition, Offset: r.Offset, Key: key, Seq: seq})
		}
	}
}

// analyzeLog is the pure core of kafka-log verification. For each key it treats
// the produced sequence numbers as a contiguous range [min,max]: any missing
// value is a lost message and any repeat is a duplicate. Ordering is checked per
// partition in offset order — because a key routes to a single partition, a
// sequence that decreases relative to the previous record for the same key is an
// ordering violation.
func analyzeLog(recs []logRecord) domain.VerifyReport {
	rep := domain.VerifyReport{Model: domain.VerifyKafkaLog, Checked: int64(len(recs))}
	if len(recs) == 0 {
		rep.Verdict = domain.VerdictUnknown
		return rep
	}

	sorted := append([]logRecord(nil), recs...)
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].Partition != sorted[j].Partition {
			return sorted[i].Partition < sorted[j].Partition
		}
		return sorted[i].Offset < sorted[j].Offset
	})

	seqsByKey := make(map[int64][]int64)
	lastSeq := make(map[int64]int64)
	var ordering int64
	var orderWitness []string
	for _, r := range sorted {
		if prev, ok := lastSeq[r.Key]; ok && r.Seq < prev {
			ordering++
			if len(orderWitness) < maxWitness {
				orderWitness = append(orderWitness, fmt.Sprintf("key=%d p=%d off=%d seq=%d<prev=%d", r.Key, r.Partition, r.Offset, r.Seq, prev))
			}
		}
		lastSeq[r.Key] = r.Seq
		seqsByKey[r.Key] = append(seqsByKey[r.Key], r.Seq)
	}

	var lost, dup int64
	var lossWitness, dupWitness []string
	for key, seqs := range seqsByKey {
		uniq := make(map[int64]struct{}, len(seqs))
		minS, maxS := seqs[0], seqs[0]
		for _, s := range seqs {
			uniq[s] = struct{}{}
			if s < minS {
				minS = s
			}
			if s > maxS {
				maxS = s
			}
		}
		keyLost := (maxS - minS + 1) - int64(len(uniq))
		keyDup := int64(len(seqs)) - int64(len(uniq))
		if keyLost > 0 {
			lost += keyLost
			if len(lossWitness) < maxWitness {
				lossWitness = append(lossWitness, fmt.Sprintf("key=%d gaps=%d range=[%d,%d]", key, keyLost, minS, maxS))
			}
		}
		if keyDup > 0 {
			dup += keyDup
			if len(dupWitness) < maxWitness {
				dupWitness = append(dupWitness, fmt.Sprintf("key=%d dups=%d", key, keyDup))
			}
		}
	}

	rep.Lost, rep.Duplicated, rep.OrderingViol = lost, dup, ordering
	rep.Anomalies = buildAnomalies(lost, dup, ordering, lossWitness, dupWitness, orderWitness)
	if lost == 0 && dup == 0 && ordering == 0 {
		rep.Verdict = domain.VerdictPass
	} else {
		rep.Verdict = domain.VerdictFail
	}
	return rep
}

// maxWitness caps how many example violations each anomaly kind records.
const maxWitness = 10

// buildAnomalies assembles the anomaly list from the violation tallies.
func buildAnomalies(lost, dup, ordering int64, lossW, dupW, orderW []string) []domain.Anomaly {
	var out []domain.Anomaly
	if lost > 0 {
		out = append(out, domain.Anomaly{Kind: "message-loss", Detail: fmt.Sprintf("%d lost", lost), Witness: lossW})
	}
	if dup > 0 {
		out = append(out, domain.Anomaly{Kind: "duplication", Detail: fmt.Sprintf("%d duplicated", dup), Witness: dupW})
	}
	if ordering > 0 {
		out = append(out, domain.Anomaly{Kind: "ordering", Detail: fmt.Sprintf("%d ordering violations", ordering), Witness: orderW})
	}
	return out
}
