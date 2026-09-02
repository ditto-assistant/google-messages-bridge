package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

const testToken = "0123456789abcdef0123456789abcdef"

type fakeProtocol struct {
	pair        pairing
	session     bridgeSession
	lastMethod  pairingMethod
	lastCookies map[string]string
}

func (p *fakeProtocol) StartPairing(_ context.Context, method pairingMethod, cookies map[string]string) (pairing, error) {
	p.lastMethod = method
	p.lastCookies = cloneCookies(cookies)
	return p.pair, nil
}

func (p *fakeProtocol) OpenSession(context.Context, string) (bridgeSession, error) {
	return p.session, nil
}

type fakePairing struct {
	challenge pairingChallenge
}

func (p *fakePairing) Challenge() pairingChallenge {
	if p.challenge.Kind == "" {
		return pairingChallenge{Kind: "qr", Prompt: "qr-payload"}
	}
	return p.challenge
}
func (*fakePairing) Wait(context.Context) (string, string, error) {
	return "paired-session", "Pixel", nil
}
func (*fakePairing) Close() {}

type fakeSession struct{}

func (*fakeSession) Sync(_ context.Context, _ syncCursor, _ *time.Time, _ int) ([]rawMessage, []conversationThread, syncCursor, error) {
	return []rawMessage{{Timestamp: time.Unix(1, 0).UTC(), ThreadID: "thread", ThreadName: "Ada", ThreadKind: "dm", SenderID: "ada", SenderName: "Ada", Text: "hello", MessageID: "message"}}, []conversationThread{{LastMessageAt: time.Unix(1, 0).UTC(), ThreadID: "thread", ThreadName: "Ada", ThreadKind: "dm"}}, syncCursor{Version: cursorVersion, Phase: cursorPhaseLive, AfterMicros: 1}, nil
}
func (*fakeSession) Send(_ context.Context, threadID, text string) (rawMessage, error) {
	return rawMessage{Timestamp: time.Unix(2, 0).UTC(), ThreadID: threadID, ThreadName: "Ada", ThreadKind: "dm", SenderName: "You", Text: text, MessageID: "sent", IsFromSelf: true}, nil
}
func (*fakeSession) Close() {}

func TestHealthAndAuthentication(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/health", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status = %d", health.Code)
	}

	unauthorized := httptest.NewRecorder()
	server.handler().ServeHTTP(unauthorized, httptest.NewRequest(http.MethodPost, "/v1/sync", bytes.NewBufferString(`{}`)))
	if unauthorized.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized status = %d", unauthorized.Code)
	}
}

func TestPairSyncAndSend(t *testing.T) {
	server := testServer(t)
	initial := callJSON(t, server, "/v1/connect", map[string]any{"credentials": map[string]string{"start": "1"}})
	if initial.Code != http.StatusOK {
		t.Fatalf("initial pairing status = %d, body = %s", initial.Code, initial.Body.String())
	}
	var challenge struct {
		Challenge struct {
			Kind   string            `json:"kind"`
			Prompt string            `json:"prompt"`
			State  map[string]string `json:"state"`
		} `json:"challenge"`
	}
	if err := json.Unmarshal(initial.Body.Bytes(), &challenge); err != nil {
		t.Fatal(err)
	}
	if challenge.Challenge.Kind != "qr" || challenge.Challenge.Prompt != "qr-payload" || challenge.Challenge.State["flowID"] == "" {
		t.Fatalf("challenge = %#v", challenge)
	}

	var paired *httptest.ResponseRecorder
	for range 20 {
		paired = callJSON(t, server, "/v1/connect", map[string]any{"credentials": map[string]string{"poll": "1", "flowID": challenge.Challenge.State["flowID"]}})
		var response map[string]any
		_ = json.Unmarshal(paired.Body.Bytes(), &response)
		if response["credentials"] != nil {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if paired == nil || paired.Code != http.StatusOK || !bytes.Contains(paired.Body.Bytes(), []byte("paired-session")) {
		t.Fatalf("paired response = %v", paired)
	}

	syncResponse := callJSON(t, server, "/v1/sync", map[string]any{"credentials": map[string]string{"session": "paired-session"}, "limit": 10})
	if syncResponse.Code != http.StatusOK || !bytes.Contains(syncResponse.Body.Bytes(), []byte(`"messageId":"message"`)) || !bytes.Contains(syncResponse.Body.Bytes(), []byte(`"threads":[`)) {
		t.Fatalf("sync status = %d, body = %s", syncResponse.Code, syncResponse.Body.String())
	}

	sendResponse := callJSON(t, server, "/v1/send", map[string]any{"credentials": map[string]string{"session": "paired-session"}, "threadId": "thread", "text": "on my way"})
	if sendResponse.Code != http.StatusOK || !bytes.Contains(sendResponse.Body.Bytes(), []byte(`"isFromSelf":true`)) {
		t.Fatalf("send status = %d, body = %s", sendResponse.Code, sendResponse.Body.String())
	}
}

func TestGoogleAccountCookiePairingRemainsAvailable(t *testing.T) {
	proto := &fakeProtocol{
		pair:    &fakePairing{challenge: pairingChallenge{Kind: "emoji", Prompt: "🐢"}},
		session: &fakeSession{},
	}
	server, err := newServer(testToken, proto, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	response := callJSON(t, server, "/v1/connect", map[string]any{
		"credentials": map[string]string{"cookies": `{"SAPISID":"secret"}`},
	})
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
	if proto.lastMethod != pairingMethodGoogle || proto.lastCookies["SAPISID"] != "secret" {
		t.Fatalf("method = %q, cookies = %#v", proto.lastMethod, proto.lastCookies)
	}
}

func TestConnectRejectsUnknownPairingMethod(t *testing.T) {
	server := testServer(t)
	response := callJSON(t, server, "/v1/connect", map[string]any{
		"credentials": map[string]string{"method": "oauth"},
	})
	if response.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
	}
}

func testServer(t *testing.T) *server {
	t.Helper()
	result, err := newServer(testToken, &fakeProtocol{pair: &fakePairing{}, session: &fakeSession{}}, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func callJSON(t *testing.T, server *server, path string, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(raw))
	request.Header.Set("Authorization", "Bearer "+testToken)
	response := httptest.NewRecorder()
	server.handler().ServeHTTP(response, request)
	return response
}
