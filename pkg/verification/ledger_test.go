package verification

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// jsonLine marshals an event the same way Ledger.Append does (JSON + "\n").
func jsonLine(event Event) ([]byte, error) {
	data, err := json.Marshal(event)
	if err != nil {
		return nil, err
	}
	return append(data, '\n'), nil
}

func sampleEvent(runID, id, status string) Event {
	return Event{
		Time:           time.Date(2026, 10, 14, 10, 0, 0, 0, time.UTC),
		RunID:          runID,
		ID:             id,
		ExpectedSize:   5,
		ExpectedDigest: validDigest,
		ActualSize:     5,
		ActualDigest:   validDigest,
		Status:         status,
	}
}

func TestLedgerAppendAndReplay(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()
	ledgerPath := filepath.Join(tmp, ".verification", "ledger.jsonl")

	ledger, err := OpenLedger(ledgerPath)
	require.NoError(t, err)

	// Run 1 records a verified and a quarantined event.
	require.NoError(t, ledger.Append(sampleEvent("run-1", "a", StatusVerified)))
	require.NoError(t, ledger.Append(sampleEvent("run-1", "b", StatusQuarantined)))
	require.NoError(t, ledger.Close())

	raw1, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)

	// Run 2 appends more events: "a" is verified again (unchanged entry),
	// "c" is missing, and "b" gets no new event (sticky quarantine).
	ledger, err = OpenLedger(ledgerPath)
	require.NoError(t, err)
	require.NoError(t, ledger.Append(sampleEvent("run-2", "a", StatusVerified)))
	missingEvent := sampleEvent("run-2", "c", StatusMissing)
	missingEvent.ActualSize = 0
	missingEvent.ActualDigest = ""
	require.NoError(t, ledger.Append(missingEvent))
	require.NoError(t, ledger.Close())

	// Historical lines from run 1 must not have been rewritten.
	raw2, err := os.ReadFile(ledgerPath)
	require.NoError(t, err)
	a.Contains(string(raw2), string(raw1))

	store, err := ReplayLedger(ledgerPath)
	require.NoError(t, err)

	eventA, ok := store.Status("a")
	a.True(ok)
	a.Equal(StatusVerified, eventA.Status)
	a.Equal("run-2", eventA.RunID, "replay must return the last event per id")

	eventB, ok := store.Status("b")
	a.True(ok)
	a.Equal(StatusQuarantined, eventB.Status)
	a.Equal("run-1", eventB.RunID)

	eventC, ok := store.Status("c")
	a.True(ok)
	a.Equal(StatusMissing, eventC.Status)

	// Download blocking follows the current quarantined status.
	blocked, err := store.DownloadBlocked(context.Background(), "b")
	a.NoError(err)
	a.True(blocked)
	for _, id := range []string{"a", "c", "unknown"} {
		blocked, err := store.DownloadBlocked(context.Background(), id)
		a.NoError(err)
		a.False(blocked, "id %q must not be blocked", id)
	}
}

func TestReplayEmptyAndMissing(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	// Non-existent ledger -> empty store, no error.
	store, err := ReplayLedger(filepath.Join(tmp, "nope", "ledger.jsonl"))
	require.NoError(t, err)
	blocked, err := store.DownloadBlocked(context.Background(), "anything")
	a.NoError(err)
	a.False(blocked)

	// Explicitly created but empty ledger.
	emptyPath := filepath.Join(tmp, "empty.jsonl")
	require.NoError(t, os.WriteFile(emptyPath, nil, 0664))
	store, err = ReplayLedger(emptyPath)
	require.NoError(t, err)
	_, ok := store.Status("x")
	a.False(ok)
}

func TestReplayCorrupt(t *testing.T) {
	cases := map[string]string{
		"malformed json":   `{not json` + "\n",
		"empty line":       "\n",
		"empty id":         `{"time":"2026-10-14T10:00:00Z","run_id":"r","id":"","expected_digest":"` + validDigest + `","status":"verified"}` + "\n",
		"invalid status":   `{"time":"2026-10-14T10:00:00Z","run_id":"r","id":"a","expected_digest":"` + validDigest + `","status":"exploded"}` + "\n",
		"empty run id":     `{"time":"2026-10-14T10:00:00Z","run_id":"","id":"a","expected_digest":"` + validDigest + `","status":"verified"}` + "\n",
		"empty timestamp":  `{"time":"0001-01-01T00:00:00Z","run_id":"r","id":"a","expected_digest":"` + validDigest + `","status":"verified"}` + "\n",
		"invalid expected": `{"time":"2026-10-14T10:00:00Z","run_id":"r","id":"a","expected_digest":"abc","status":"verified"}` + "\n",
		"invalid actual":   `{"time":"2026-10-14T10:00:00Z","run_id":"r","id":"a","expected_digest":"` + validDigest + `","actual_digest":"zz","status":"verified"}` + "\n",
	}

	for name, content := range cases {
		t.Run(name, func(t *testing.T) {
			a := assert.New(t)
			tmp := t.TempDir()
			path := filepath.Join(tmp, "ledger.jsonl")
			require.NoError(t, os.WriteFile(path, []byte(content), 0664))

			store, err := ReplayLedger(path)
			a.Error(err)
			a.Nil(store)
		})
	}
}

func TestReplayRejectsQuarantineRegression(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()
	path := filepath.Join(tmp, "ledger.jsonl")

	// First quarantined, then a forged "verified" line: the ledger is
	// tampered and replay must fail closed instead of lifting the block.
	quarantined := sampleEvent("run-1", "x", StatusQuarantined)
	verified := sampleEvent("run-2", "x", StatusVerified)
	line1, err := jsonLine(quarantined)
	require.NoError(t, err)
	line2, err := jsonLine(verified)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, append(append([]byte{}, line1...), line2...), 0664))

	store, err := ReplayLedger(path)
	a.Error(err)
	a.Nil(store)
	a.Contains(err.Error(), "regresses")

	// A second quarantined line after a quarantined one is legal (no move is
	// ever performed by the verifier itself, but replay must not reject it).
	require.NoError(t, os.WriteFile(path, append(append([]byte{}, line1...), line1...), 0664))
	store, err = ReplayLedger(path)
	require.NoError(t, err)
	a.True(store.IsQuarantined("x"))
}

func TestReplayRejectsNegativeSizes(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()
	path := filepath.Join(tmp, "ledger.jsonl")

	event := sampleEvent("run-1", "x", StatusVerified)
	event.ExpectedSize = -1
	line, err := jsonLine(event)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(path, line, 0664))

	store, err := ReplayLedger(path)
	a.Error(err)
	a.Nil(store)
}

func TestOpenLedgerFailures(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()

	// A regular file blocks directory creation for the ledger path.
	blocker := filepath.Join(tmp, "blocker")
	require.NoError(t, os.WriteFile(blocker, []byte("x"), 0664))
	ledger, err := OpenLedger(filepath.Join(blocker, "ledger.jsonl"))
	a.Error(err)
	a.Nil(ledger)
}

func TestLedgerAppendFailure(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()
	path := filepath.Join(tmp, "ledger.jsonl")

	ledger, err := OpenLedger(path)
	require.NoError(t, err)
	require.NoError(t, ledger.Close())

	// Appending to the closed file must fail instead of being silently lost.
	err = ledger.Append(sampleEvent("run-1", "a", StatusVerified))
	a.Error(err)
}

func TestLedgerEventDefaultTime(t *testing.T) {
	a := assert.New(t)
	tmp := t.TempDir()
	path := filepath.Join(tmp, "ledger.jsonl")

	ledger, err := OpenLedger(path)
	require.NoError(t, err)

	event := sampleEvent("run-1", "a", StatusVerified)
	event.Time = time.Time{}
	require.NoError(t, ledger.Append(event))
	require.NoError(t, ledger.Close())

	store, err := ReplayLedger(path)
	require.NoError(t, err)
	stored, ok := store.Status("a")
	a.True(ok)
	a.False(stored.Time.IsZero())
}
