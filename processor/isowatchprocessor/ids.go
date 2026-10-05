package isowatchprocessor

import (
	"encoding/hex"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
)

func parseTraceID(s string) (pcommon.TraceID, error) {
	var id pcommon.TraceID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(id) {
		return id, fmt.Errorf("bad trace id %q", s)
	}
	copy(id[:], b)
	return id, nil
}

func parseSpanID(s string) (pcommon.SpanID, error) {
	var id pcommon.SpanID
	b, err := hex.DecodeString(s)
	if err != nil || len(b) != len(id) {
		return id, fmt.Errorf("bad span id %q", s)
	}
	copy(id[:], b)
	return id, nil
}
