package jithttp

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// asUser adds the test resolver's X-Test-User header to every request.
type asUser struct {
	user string
	next http.RoundTripper
}

func (a asUser) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if a.user != "" {
		r.Header.Set("X-Test-User", a.user)
	}
	return a.next.RoundTrip(r)
}

func mcpSession(t *testing.T, srv *httptest.Server, user string) (*mcp.ClientSession, error) {
	t.Helper()
	c := mcp.NewClient(&mcp.Implementation{Name: "test", Version: "1"}, nil)
	s, err := c.Connect(context.Background(), &mcp.StreamableClientTransport{
		Endpoint:   srv.URL + "/mcp",
		HTTPClient: &http.Client{Transport: asUser{user, http.DefaultTransport}},
	}, nil)
	if err == nil {
		t.Cleanup(func() { s.Close() })
	}
	return s, err
}

// callTool calls a tool and decodes its structured output into out.
func callTool(t *testing.T, s *mcp.ClientSession, name string, args map[string]any, out any) (text string, isErr bool) {
	t.Helper()
	res, err := s.CallTool(context.Background(), &mcp.CallToolParams{Name: name, Arguments: args})
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	for _, c := range res.Content {
		if tc, ok := c.(*mcp.TextContent); ok {
			text += tc.Text
		}
	}
	if !res.IsError && out != nil {
		b, _ := json.Marshal(res.StructuredContent)
		if err := json.Unmarshal(b, out); err != nil {
			t.Fatalf("%s: decoding %s: %v", name, b, err)
		}
	}
	return text, res.IsError
}

func TestMCP(t *testing.T) {
	srv, ts := newTestServer(t)
	var wf struct{ ID string }
	if code := call(t, srv, "root", "POST", "/v1/workflows", map[string]any{
		"name": "RDP", "attribute": "windows_rdp", "resources": []map[string]string{{"name": "svc:windows-rdp", "description": "Windows jump host"}},
		"requesters": []string{"autogroup:member"}, "approvers": []string{"bob@example.com"}, "max_duration": "1h",
	}, &wf); code != 201 {
		t.Fatalf("create workflow: %d", code)
	}

	alice, err := mcpSession(t, srv, "alice")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(alice.InitializeResult().Instructions, "http://jit.example.ts.net/requests/req_") {
		t.Error("the instructions do not show where approval links point")
	}
	tools, err := alice.ListTools(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, tl := range tools.Tools {
		names = append(names, tl.Name)
	}
	slices.Sort(names)
	if want := []string{"cancel_access_request", "get_access_request", "list_requestable", "request_access", "wait_for_decision"}; !slices.Equal(names, want) {
		t.Errorf("tools = %v, want %v (and never an approve tool)", names, want)
	}

	// The caller is whoever sends each request: one server, two people.
	var list mcpRequestable
	text, _ := callTool(t, alice, "list_requestable", nil, &list)
	if list.Identity.Login != "alice@example.com" || len(list.Requestable) != 1 || list.Requestable[0].WorkflowID != wf.ID ||
		list.Requestable[0].Resources[0].Description != `"Windows jump host"` || !strings.Contains(text, wf.ID) {
		t.Errorf("alice's list_requestable = %+v\n%s", list, text)
	}
	bob, err := mcpSession(t, srv, "bob")
	if err != nil {
		t.Fatal(err)
	}
	callTool(t, bob, "list_requestable", nil, &list)
	if list.Identity.Login != "bob@example.com" {
		t.Errorf("bob's session says he is %q", list.Identity.Login)
	}

	var req mcpRequest
	text, isErr := callTool(t, alice, "request_access", map[string]any{"workflow_id": wf.ID, "reason": "patch the jump host", "duration": "15m"}, &req)
	if isErr || req.Status != "pending" || req.Requester.Login != "alice@example.com" || req.TargetDeviceID != "nALICE" ||
		req.URL != "http://jit.example.ts.net/requests/"+req.ID || req.Reason != `"patch the jump host"` || !strings.Contains(text, req.URL) {
		t.Fatalf("request_access = %+v\n%s", req, text)
	}
	// One open request per flow and device: the error points at it.
	if text, isErr := callTool(t, alice, "request_access", map[string]any{"workflow_id": wf.ID, "reason": "again"}, nil); !isErr || !strings.Contains(text, req.URL) {
		t.Errorf("a second request: error %v, %s", isErr, text)
	}
	if text, isErr := callTool(t, alice, "request_access", map[string]any{"workflow_id": wf.ID, "reason": "x", "duration": "soon"}, nil); !isErr || !strings.Contains(text, "duration") {
		t.Errorf("a bad duration: error %v, %s", isErr, text)
	}

	// Bob, its approver, may see it but not withdraw it for alice: only the
	// requester cancels a pending request (approvers decline instead).
	var seen mcpRequest
	if _, isErr := callTool(t, bob, "get_access_request", map[string]any{"request_id": req.ID}, &seen); isErr || seen.ID != req.ID {
		t.Errorf("bob cannot see the request he approves: %+v", seen)
	}
	if text, isErr := callTool(t, bob, "cancel_access_request", map[string]any{"request_id": req.ID}, nil); !isErr {
		t.Errorf("bob withdrew alice's request: %s", text)
	}

	// Nobody decides: the wait times out with the request still pending.
	start := time.Now()
	text, isErr = callTool(t, alice, "wait_for_decision", map[string]any{"request_id": req.ID, "timeout_seconds": 1}, &req)
	if isErr || req.Status != "pending" || !strings.Contains(text, "Still pending") || !strings.Contains(text, req.URL) || time.Since(start) > 5*time.Second {
		t.Errorf("wait without a decision: %+v\n%s", req, text)
	}

	// Bob approves in the browser while alice's agent waits: the wait ends
	// on the change, without waiting for its timeout.
	go func() {
		time.Sleep(200 * time.Millisecond)
		call(t, srv, "bob", "POST", "/v1/requests/"+req.ID+"/approve", map[string]string{"note": "ok"}, nil)
	}()
	start = time.Now()
	text, isErr = callTool(t, alice, "wait_for_decision", map[string]any{"request_id": req.ID, "timeout_seconds": 60}, &req)
	if isErr || req.Status != "approved" || !strings.Contains(text, "tailscale service list") || time.Since(start) > 2*time.Second {
		t.Fatalf("wait through an approval (%s): %+v\n%s", time.Since(start), req, text)
	}
	callTool(t, alice, "get_access_request", map[string]any{"request_id": req.ID}, &req)
	if req.Status != "approved" || len(req.Approvals) != 1 || req.Approvals[0].Note != `"ok"` {
		t.Errorf("get_access_request = %+v", req)
	}

	text, isErr = callTool(t, alice, "cancel_access_request", map[string]any{"request_id": req.ID, "note": "done"}, &req)
	if isErr || req.Status != "revoked" || !slices.Contains(ts.calls, "DEL nALICE custom:jit_windows_rdp") {
		t.Errorf("cancel_access_request = %+v %v\n%s", req, ts.calls, text)
	}
	if text, isErr := callTool(t, alice, "get_access_request", map[string]any{"request_id": "req_nope"}, nil); !isErr || !strings.Contains(text, "not found") {
		t.Errorf("an unknown request: error %v, %s", isErr, text)
	}
}

func TestMCPRefusals(t *testing.T) {
	srv, _ := newTestServer(t)

	// Nobody WhoIs knows gets nothing.
	if _, err := mcpSession(t, srv, ""); err == nil {
		t.Error("an unidentified caller connected")
	}

	// A web page cannot use the viewer's identity: browsers mark the POST
	// as cross-site.
	body := `{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"request_access","arguments":{}}}`
	for _, hdr := range []map[string]string{{"Origin": "http://evil.example"}, {"Sec-Fetch-Site": "cross-site"}} {
		req, _ := http.NewRequest("POST", srv.URL+"/mcp", strings.NewReader(body))
		req.Header.Set("X-Test-User", "alice")
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json, text/event-stream")
		for k, v := range hdr {
			req.Header.Set(k, v)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		if resp.StatusCode != http.StatusForbidden {
			t.Errorf("%v: status %d, want 403", hdr, resp.StatusCode)
		}
	}
}
