package internal

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseExpectedSnapshotIDRequiresADecimalString(t *testing.T) {
	parse := func(body string) (int64, error) {
		return parseExpectedSnapshotID(httptest.NewRequest("POST", "/", strings.NewReader(body)))
	}

	id, err := parse(`{"expected_snapshot_id":"6548219920550331090"}`)
	require.NoError(t, err)
	require.Equal(t, int64(6548219920550331090), id)

	// An empty body (a page from before this change), malformed JSON, a JSON
	// number (which loses precision) and a non-positive id are all rejected.
	rejected := []string{
		``,
		`{`,
		`{}`,
		`{"expected_snapshot_id":6548219920550331090}`,
		`{"expected_snapshot_id":"0"}`,
	}
	for _, body := range rejected {
		_, err = parse(body)
		require.ErrorIs(t, err, errExpectedSnapshotIDRequired, body)
	}
}
