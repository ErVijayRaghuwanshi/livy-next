package spark

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

const sampleConnectHTML = `<!DOCTYPE html><html><body>
<h4>3 session(s) are online, running 0 Request(s)</h4>
<table class="table table-bordered" id="sessionstat">
  <thead>
    <tr><th>User</th><th>Session ID</th><th>Start Time</th><th>Finish Time</th><th>Duration</th><th>Total Execute</th></tr>
  </thead>
  <tbody>
    <tr>
      <td> ervijay </td>
      <td> <a href="/connect/session/?id=37508513-f663-43c7-8215-f778e139d41e"> 37508513-f663-43c7-8215-f778e139d41e </a> </td>
      <td> 2026/09/27 18:26:13 </td>
      <td>  </td>
      <td> 5 minutes 24 seconds </td>
      <td> 12 </td>
    </tr>
    <tr>
      <td> na </td>
      <td> <a href="/connect/session/?id=a6357ad0-f4c8-4709-b681-2d42a78c2034"> a6357ad0-f4c8-4709-b681-2d42a78c2034 </a> </td>
      <td> 2026/09/27 18:29:27 </td>
      <td>  </td>
      <td> 2 minutes 10 seconds </td>
      <td> 0 </td>
    </tr>
    <tr>
      <td> ervijay </td>
      <td> <a href="/connect/session/?id=7847d17a-01df-4066-a0c2-1a7ce6748f02"> 7847d17a-01df-4066-a0c2-1a7ce6748f02 </a> </td>
      <td> 2026/09/27 18:26:10 </td>
      <td> 2026/09/27 18:26:11 </td>
      <td> 557 ms </td>
      <td> 2 </td>
    </tr>
  </tbody>
</table>
<table id="sqlstat">
  <tr><td>some other table</td></tr>
</table>
</body></html>`

func TestParseSessionStatHTML(t *testing.T) {
	sessions := ParseSessionStatHTML(sampleConnectHTML)
	assert.Len(t, sessions, 3)

	// Active session with user
	assert.Equal(t, "37508513-f663-43c7-8215-f778e139d41e", sessions[0].SessionID)
	assert.Equal(t, "ervijay", sessions[0].User)
	assert.True(t, sessions[0].IsActive)
	assert.Equal(t, 12, sessions[0].TotalExecute)

	// Active session with "na" user (converted to "")
	assert.Equal(t, "a6357ad0-f4c8-4709-b681-2d42a78c2034", sessions[1].SessionID)
	assert.Equal(t, "", sessions[1].User)
	assert.True(t, sessions[1].IsActive)
	assert.Equal(t, 0, sessions[1].TotalExecute)

	// Closed session
	assert.Equal(t, "7847d17a-01df-4066-a0c2-1a7ce6748f02", sessions[2].SessionID)
	assert.Equal(t, "ervijay", sessions[2].User)
	assert.False(t, sessions[2].IsActive)
	assert.NotEmpty(t, sessions[2].FinishTime)
}

func TestDiscoverActiveSessions(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/connect/" {
			fmt.Fprint(w, sampleConnectHTML)
			return
		}
		http.NotFound(w, r)
	}))
	defer ts.Close()

	ctx := context.Background()
	active, err := DiscoverActiveSessions(ctx, ts.URL)
	assert.NoError(t, err)
	assert.Len(t, active, 2)
	assert.Equal(t, "37508513-f663-43c7-8215-f778e139d41e", active[0].SessionID)
	assert.Equal(t, "a6357ad0-f4c8-4709-b681-2d42a78c2034", active[1].SessionID)

	// Test empty URL
	_, err = DiscoverActiveSessions(ctx, "")
	assert.Error(t, err)
}
