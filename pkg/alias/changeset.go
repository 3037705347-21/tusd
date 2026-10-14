package alias

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
)

// ChangeSet is the on-disk format of an alias batch file.
//
// Example:
//
//	{
//	  "changes": [
//	    {"op": "upsert", "alias": "2023/report", "canonical_id": "f8e2c1..."},
//	    {"op": "unbind", "alias": "2022/draft"}
//	  ]
//	}
type ChangeSet struct {
	Changes []Change `json:"changes"`
}

// ParseChanges decodes a JSON change set. Unknown fields and trailing data are
// rejected so that typos in an edited batch file surface instead of being
// ignored silently.
func ParseChanges(r io.Reader) ([]Change, error) {
	data, err := io.ReadAll(r)
	if err != nil {
		return nil, err
	}

	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var changeSet ChangeSet
	if err := decoder.Decode(&changeSet); err != nil {
		return nil, fmt.Errorf("parsing alias change set: %w", err)
	}

	// Reject any trailing JSON tokens after the change set object.
	var extra json.RawMessage
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return nil, fmt.Errorf("parsing alias change set: unexpected trailing data")
		}
		return nil, fmt.Errorf("parsing alias change set: %w", err)
	}

	return changeSet.Changes, nil
}
