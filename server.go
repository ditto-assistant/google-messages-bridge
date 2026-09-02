package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
)

const (
	maxRequestBody     = 1 << 20
	pairingTTL         = 5 * time.Minute
	sessionIdleTTL     = 30 * time.Minute
	maxSyncLimit       = 500
	defaultSyncLimit   = 500
	minimumBridgeToken = 32
)

type pairResult struct {
	session     string
	displayName string
	err         error
}

type pairFlow struct {
	pairing pairing
	result  chan pairResult
	expires time.Time
}

type managedSession struct {
	session bridgeSession
	lastUse time.Time
}

type server struct {
	token    string
	protocol protocol
	logger   zerolog.Logger
	now      func() time.Time

	flowsMu sync.Mutex
	flows   map[string]*pairFlow

	sessionsMu sync.Mutex
	sessions   map[[sha256.Size]byte]*managedSession
}

func newServer(token string, proto protocol, logger zerolog.Logger) (*server, error) {
	if len(token) < minimumBridgeToken {
		return nil, fmt.Errorf("BRIDGE_TOKEN must be at least %d characters", minimumBridgeToken)
	}
	if proto == nil {
		return nil, errors.New("protocol is required")
	}
	s := &server{
		token: token, protocol: proto, logger: logger, now: time.Now,
		flows: make(map[string]*pairFlow), sessions: make(map[[sha256.Size]byte]*managedSession),
	}
	go s.cleanupLoop()
	return s, nil
}

func (s *server) handler() http.Handler {
	mux := http.NewServeMux()
	health := func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	}
	mux.HandleFunc("GET /health", health)
	mux.HandleFunc("GET /healthz", health)
	mux.Handle("POST /v1/connect", s.authenticate(http.HandlerFunc(s.handleConnect)))
	mux.Handle("POST /v1/sync", s.authenticate(http.HandlerFunc(s.handleSync)))
	mux.Handle("POST /v1/send", s.authenticate(http.HandlerFunc(s.handleSend)))
	return mux
}

func (s *server) authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		const prefix = "Bearer "
		header := r.Header.Get("Authorization")
		if !strings.HasPrefix(header, prefix) || subtle.ConstantTimeCompare([]byte(strings.TrimPrefix(header, prefix)), []byte(s.token)) != 1 {
			writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "unauthorized"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *server) handleConnect(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Credentials credentials `json:"credentials"`
	}
	if err := decodeRequest(r, &request); err != nil {
		writeClientError(w, http.StatusBadRequest, err)
		return
	}
	if flowID := stringCredential(request.Credentials, "flowID"); flowID != "" {
		s.pollPairing(w, flowID)
		return
	}
	method := pairingMethodQR
	cookies := map[string]string(nil)
	cookieJSON := stringCredential(request.Credentials, "cookies")
	if cookieJSON != "" {
		method = pairingMethodGoogle
		var err error
		cookies, err = parseCookies(cookieJSON)
		if err != nil {
			writeClientError(w, http.StatusBadRequest, err)
			return
		}
	}
	if requested := stringCredential(request.Credentials, "method"); requested != "" {
		method = pairingMethod(requested)
	}
	if method != pairingMethodQR && method != pairingMethodGoogle {
		writeClientError(w, http.StatusBadRequest, errors.New("method must be qr or google"))
		return
	}
	if method == pairingMethodGoogle && len(cookies) == 0 {
		writeClientError(w, http.StatusBadRequest, errors.New("Google-account pairing requires cookies"))
		return
	}
	pairCtx, cancel := context.WithTimeout(r.Context(), 45*time.Second)
	defer cancel()
	pair, err := s.protocol.StartPairing(pairCtx, method, cookies)
	if err != nil {
		s.logger.Warn().Err(err).Msg("failed to start Google Messages pairing")
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "could not start pairing; check Google Messages on the Android phone and try again"})
		return
	}
	flowID := uuid.NewString()
	flow := &pairFlow{pairing: pair, result: make(chan pairResult, 1), expires: s.now().Add(pairingTTL)}
	s.flowsMu.Lock()
	s.flows[flowID] = flow
	s.flowsMu.Unlock()
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), pairingTTL)
		defer cancel()
		session, displayName, waitErr := pair.Wait(ctx)
		flow.result <- pairResult{session: session, displayName: displayName, err: waitErr}
		close(flow.result)
	}()
	s.writePairingChallenge(w, flowID, pair.Challenge())
}

func (s *server) pollPairing(w http.ResponseWriter, flowID string) {
	s.flowsMu.Lock()
	flow := s.flows[flowID]
	s.flowsMu.Unlock()
	if flow == nil || s.now().After(flow.expires) {
		writeJSON(w, http.StatusGone, map[string]string{"error": "pairing expired; start again"})
		return
	}
	select {
	case result := <-flow.result:
		s.flowsMu.Lock()
		delete(s.flows, flowID)
		s.flowsMu.Unlock()
		flow.pairing.Close()
		if result.err != nil || result.session == "" {
			s.logger.Warn().Err(result.err).Msg("Google Messages pairing did not complete")
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": "pairing was rejected or expired; start again"})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"credentials": map[string]string{"session": result.session},
			"displayName": result.displayName,
		})
	default:
		s.writePairingChallenge(w, flowID, flow.pairing.Challenge())
	}
}

func (s *server) writePairingChallenge(w http.ResponseWriter, flowID string, challenge pairingChallenge) {
	writeJSON(w, http.StatusOK, map[string]any{"challenge": map[string]any{
		"field": "poll", "kind": challenge.Kind, "prompt": challenge.Prompt,
		"state": map[string]string{"flowID": flowID},
	}})
}

func (s *server) handleSync(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Credentials credentials `json:"credentials"`
		Cursor      string      `json:"cursor"`
		Since       string      `json:"since"`
		Limit       int         `json:"limit"`
	}
	if err := decodeRequest(r, &request); err != nil {
		writeClientError(w, http.StatusBadRequest, err)
		return
	}
	sessionValue := stringCredential(request.Credentials, "session")
	if sessionValue == "" {
		writeClientError(w, http.StatusBadRequest, errors.New("session is required"))
		return
	}
	cursor, err := decodeCursor(request.Cursor)
	if err != nil {
		writeClientError(w, http.StatusBadRequest, err)
		return
	}
	var since *time.Time
	if request.Since != "" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, request.Since)
		if parseErr != nil {
			writeClientError(w, http.StatusBadRequest, errors.New("since must be RFC 3339"))
			return
		}
		since = &parsed
	}
	limit := request.Limit
	if limit == 0 {
		limit = defaultSyncLimit
	}
	if limit < 1 || limit > maxSyncLimit {
		writeClientError(w, http.StatusBadRequest, fmt.Errorf("limit must be between 1 and %d", maxSyncLimit))
		return
	}
	bridge, err := s.getSession(r.Context(), sessionValue)
	if err != nil {
		s.logger.Warn().Err(err).Msg("failed to open Google Messages session")
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "session could not connect; reconnect Google Messages"})
		return
	}
	messages, threads, next, err := bridge.Sync(r.Context(), cursor, since, limit)
	if err != nil {
		s.logger.Warn().Err(err).Msg("Google Messages sync failed")
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "sync failed; ensure the Android phone is online"})
		return
	}
	nextValue, err := encodeCursor(next)
	if err != nil {
		s.logger.Error().Err(err).Msg("failed to encode Google Messages cursor")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "could not save sync position"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": messages, "threads": threads, "nextCursor": nextValue})
}

func (s *server) handleSend(w http.ResponseWriter, r *http.Request) {
	var request struct {
		Credentials credentials `json:"credentials"`
		ThreadID    string      `json:"threadId"`
		Text        string      `json:"text"`
	}
	if err := decodeRequest(r, &request); err != nil {
		writeClientError(w, http.StatusBadRequest, err)
		return
	}
	sessionValue := stringCredential(request.Credentials, "session")
	request.ThreadID = strings.TrimSpace(request.ThreadID)
	request.Text = strings.TrimSpace(request.Text)
	if sessionValue == "" || request.ThreadID == "" || request.Text == "" {
		writeClientError(w, http.StatusBadRequest, errors.New("session, threadId, and text are required"))
		return
	}
	if len(request.Text) > 16<<10 {
		writeClientError(w, http.StatusBadRequest, errors.New("text is too large"))
		return
	}
	bridge, err := s.getSession(r.Context(), sessionValue)
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "session could not connect; reconnect Google Messages"})
		return
	}
	message, err := bridge.Send(r.Context(), request.ThreadID, request.Text)
	if err != nil {
		s.logger.Warn().Err(err).Str("thread_id", request.ThreadID).Msg("Google Messages send failed")
		writeJSON(w, http.StatusBadGateway, map[string]string{"error": "send failed; ensure the Android phone is online and Google Messages is the default SMS app"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"message": message})
}

func (s *server) getSession(ctx context.Context, opaque string) (bridgeSession, error) {
	key := sha256.Sum256([]byte(opaque))
	s.sessionsMu.Lock()
	if existing := s.sessions[key]; existing != nil {
		existing.lastUse = s.now()
		session := existing.session
		s.sessionsMu.Unlock()
		return session, nil
	}
	s.sessionsMu.Unlock()

	opened, err := s.protocol.OpenSession(ctx, opaque)
	if err != nil {
		return nil, err
	}
	s.sessionsMu.Lock()
	if existing := s.sessions[key]; existing != nil {
		s.sessionsMu.Unlock()
		opened.Close()
		return existing.session, nil
	}
	s.sessions[key] = &managedSession{session: opened, lastUse: s.now()}
	s.sessionsMu.Unlock()
	return opened, nil
}

func (s *server) cleanupLoop() {
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		now := s.now()
		s.flowsMu.Lock()
		for id, flow := range s.flows {
			if now.After(flow.expires) {
				flow.pairing.Close()
				delete(s.flows, id)
			}
		}
		s.flowsMu.Unlock()

		s.sessionsMu.Lock()
		for key, managed := range s.sessions {
			if now.Sub(managed.lastUse) > sessionIdleTTL {
				managed.session.Close()
				delete(s.sessions, key)
			}
		}
		s.sessionsMu.Unlock()
	}
}

func decodeRequest(r *http.Request, target any) error {
	reader := http.MaxBytesReader(nil, r.Body, maxRequestBody)
	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return errors.New("request body must be valid JSON")
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("request body must contain one JSON object")
	}
	return nil
}

func parseCookies(value string) (map[string]string, error) {
	if strings.TrimSpace(value) == "" || len(value) > maxRequestBody {
		return nil, errors.New("cookies must be a non-empty JSON object")
	}
	var cookies map[string]string
	if err := json.Unmarshal([]byte(value), &cookies); err != nil || len(cookies) == 0 {
		return nil, errors.New("cookies must be a JSON object with string values")
	}
	for name, cookie := range cookies {
		if strings.TrimSpace(name) == "" || cookie == "" || len(name) > 256 || len(cookie) > 16<<10 {
			return nil, errors.New("cookies contain an invalid entry")
		}
	}
	return cookies, nil
}

func stringCredential(values credentials, key string) string {
	value, _ := values[key].(string)
	return strings.TrimSpace(value)
}

func writeClientError(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"error": err.Error()})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
