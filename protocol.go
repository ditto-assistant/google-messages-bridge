package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
	"google.golang.org/protobuf/proto"
)

const (
	defaultHistoryWindow    = 30 * 24 * time.Hour
	liveOverlap             = 5 * time.Minute
	conversationPageSize    = 100
	messagePageSize         = 50
	maxProtocolCallsPerSync = 20
	maxOpaqueSessionBytes   = 1 << 20
)

type libGMProtocol struct {
	logger zerolog.Logger
}

func newLibGMProtocol(logger zerolog.Logger) *libGMProtocol {
	return &libGMProtocol{logger: logger}
}

type libGMPairing struct {
	client  *libgm.Client
	auth    *libgm.AuthData
	session *libgm.PairingSession
	emoji   string
	once    sync.Once
}

type libGMQRPairing struct {
	client      *libgm.Client
	auth        *libgm.AuthData
	pairSuccess chan *gmproto.PairedData
	qrMu        sync.RWMutex
	qr          string
	once        sync.Once
}

func (p *libGMProtocol) StartPairing(ctx context.Context, method pairingMethod, cookies map[string]string) (pairing, error) {
	switch method {
	case pairingMethodQR:
		return p.startQRPairing(ctx)
	case pairingMethodGoogle:
		return p.startGooglePairing(ctx, cookies)
	default:
		return nil, fmt.Errorf("unsupported pairing method %q", method)
	}
}

func (p *libGMProtocol) startGooglePairing(ctx context.Context, cookies map[string]string) (pairing, error) {
	auth := libgm.NewAuthData()
	auth.SetCookies(cloneCookies(cookies))
	client := libgm.NewClient(auth, nil, p.logger)
	if err := client.FetchConfig(ctx); err != nil {
		client.Disconnect()
		return nil, fmt.Errorf("fetch Messages configuration: %w", err)
	}
	emoji, session, err := client.StartGaiaPairing(ctx)
	if err != nil {
		client.Disconnect()
		return nil, fmt.Errorf("start Google-account pairing: %w", err)
	}
	return &libGMPairing{client: client, auth: auth, session: session, emoji: emoji}, nil
}

func (p *libGMProtocol) startQRPairing(ctx context.Context) (pairing, error) {
	auth := libgm.NewAuthData()
	client := libgm.NewClient(auth, nil, p.logger)
	pairSuccess := make(chan *gmproto.PairedData, 1)
	callback := func(data *gmproto.PairedData) {
		select {
		case pairSuccess <- data:
		default:
		}
	}
	client.PairCallback.Store(&callback)
	if err := client.FetchConfig(ctx); err != nil {
		client.Disconnect()
		return nil, fmt.Errorf("fetch Messages configuration: %w", err)
	}
	qr, err := client.StartLogin()
	if err != nil {
		client.Disconnect()
		return nil, fmt.Errorf("start QR pairing: %w", err)
	}
	return &libGMQRPairing{
		client: client, auth: auth, pairSuccess: pairSuccess, qr: qr,
	}, nil
}

func (p *libGMPairing) Challenge() pairingChallenge {
	return pairingChallenge{Kind: "emoji", Prompt: p.emoji}
}

func (p *libGMQRPairing) Challenge() pairingChallenge {
	p.qrMu.RLock()
	defer p.qrMu.RUnlock()
	return pairingChallenge{Kind: "qr", Prompt: p.qr}
}

func (p *libGMPairing) Wait(ctx context.Context) (string, string, error) {
	phoneID, err := p.client.FinishGaiaPairing(ctx, p.session)
	if err != nil {
		return "", "", err
	}
	displayName := strings.TrimSpace(phoneID)
	if sourceID := strings.TrimSpace(p.auth.Mobile.GetSourceID()); sourceID != "" {
		displayName = sourceID
	}
	return serializeAuthData(p.auth, displayName)
}

const (
	qrRefreshInterval   = 30 * time.Second
	qrMaxAttempts       = 6
	qrPairingSettleTime = 2 * time.Second
)

func (p *libGMQRPairing) Wait(ctx context.Context) (string, string, error) {
	refresh := time.NewTimer(qrRefreshInterval)
	defer refresh.Stop()
	attemptsRemaining := qrMaxAttempts
	for {
		select {
		case data := <-p.pairSuccess:
			// Google Messages needs a moment to persist the linked device before
			// the bridge disconnects and opens the new session.
			settle := time.NewTimer(qrPairingSettleTime)
			select {
			case <-settle.C:
			case <-ctx.Done():
				settle.Stop()
				return "", "", ctx.Err()
			}
			displayName := strings.TrimSpace(data.GetMobile().GetSourceID())
			return serializeAuthData(p.auth, displayName)
		case <-refresh.C:
			attemptsRemaining--
			if attemptsRemaining <= 0 {
				return "", "", errors.New("QR pairing expired")
			}
			qr, err := p.client.RefreshPhoneRelay()
			if err != nil {
				return "", "", fmt.Errorf("refresh QR pairing: %w", err)
			}
			p.qrMu.Lock()
			p.qr = qr
			p.qrMu.Unlock()
			refresh.Reset(qrRefreshInterval)
		case <-ctx.Done():
			return "", "", ctx.Err()
		}
	}
}

func serializeAuthData(auth *libgm.AuthData, displayName string) (string, string, error) {
	raw, err := json.Marshal(auth)
	if err != nil {
		return "", "", fmt.Errorf("serialize paired session: %w", err)
	}
	displayName = strings.TrimSpace(displayName)
	if displayName == "" && auth.Mobile != nil {
		displayName = strings.TrimSpace(auth.Mobile.GetSourceID())
	}
	if displayName == "" {
		displayName = "Android phone"
	}
	return base64.RawURLEncoding.EncodeToString(raw), displayName, nil
}

func (p *libGMPairing) Close() {
	p.once.Do(func() { p.client.Disconnect() })
}

func (p *libGMQRPairing) Close() {
	p.once.Do(func() { p.client.Disconnect() })
}

type libGMSession struct {
	client *libgm.Client
	logger zerolog.Logger
	mu     sync.Mutex
	once   sync.Once
}

type gmSessionClient interface {
	SetEventHandler(libgm.EventHandler)
	Connect() error
}

func connectAndWaitForSession(ctx context.Context, client gmSessionClient, timeout time.Duration) error {
	ready := make(chan struct{})
	fatal := make(chan error, 1)
	var readyOnce sync.Once
	client.SetEventHandler(func(event any) {
		switch value := event.(type) {
		case *events.ClientReady, *events.ListenRecovered:
			// libgm v0.2608.0 declares ClientReady but does not emit it. Its
			// first successful long poll emits ListenRecovered, including when
			// there was no preceding transport error.
			readyOnce.Do(func() { close(ready) })
		case *events.ListenFatalError:
			select {
			case fatal <- value.Error:
			default:
			}
		}
	})
	if err := client.Connect(); err != nil {
		return fmt.Errorf("connect session: %w", err)
	}
	wait := time.NewTimer(timeout)
	defer wait.Stop()
	select {
	case <-ready:
		return nil
	case err := <-fatal:
		return fmt.Errorf("session listener failed: %w", err)
	case <-ctx.Done():
		return ctx.Err()
	case <-wait.C:
		return errors.New("timed out waiting for the Android phone")
	}
}

func (p *libGMProtocol) OpenSession(ctx context.Context, opaque string) (bridgeSession, error) {
	if len(opaque) > maxOpaqueSessionBytes*2 {
		return nil, errors.New("session is too large")
	}
	raw, err := base64.RawURLEncoding.DecodeString(opaque)
	if err != nil || len(raw) > maxOpaqueSessionBytes {
		return nil, errors.New("session is invalid")
	}
	auth := libgm.NewAuthData()
	if err := json.Unmarshal(raw, auth); err != nil {
		return nil, errors.New("session is invalid")
	}
	if auth.Browser == nil || auth.Mobile == nil || len(auth.TachyonAuthToken) == 0 {
		return nil, errors.New("session is incomplete")
	}

	client := libgm.NewClient(auth, nil, p.logger)
	if err := connectAndWaitForSession(ctx, client, 30*time.Second); err != nil {
		client.Disconnect()
		return nil, err
	}
	return &libGMSession{client: client, logger: p.logger}, nil
}

func (s *libGMSession) Close() {
	s.once.Do(func() { s.client.Disconnect() })
}

func (s *libGMSession) Sync(ctx context.Context, cursor syncCursor, since *time.Time, limit int) ([]rawMessage, []conversationThread, syncCursor, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	conversations, byID, err := s.listConversations(ctx)
	if err != nil {
		return nil, nil, cursor, err
	}
	threads := make([]conversationThread, 0, len(conversations))
	for _, conversation := range conversations {
		if thread, ok := normalizeConversation(conversation); ok {
			threads = append(threads, thread)
		}
	}
	nowMicros := time.Now().UTC().UnixMicro()
	if cursor.Version == 0 || cursor.Phase == cursorPhaseLive {
		sinceMicros := nowMicros - defaultHistoryWindow.Microseconds()
		if cursor.Phase == cursorPhaseLive && cursor.AfterMicros > 0 {
			sinceMicros = cursor.AfterMicros - liveOverlap.Microseconds()
		} else if since != nil {
			sinceMicros = since.UTC().UnixMicro()
		}
		ids := make([]string, 0, len(conversations))
		for _, conversation := range conversations {
			if cursor.Phase == cursorPhaseLive && conversation.GetLastMessageTimestamp() < sinceMicros {
				continue
			}
			ids = append(ids, conversation.GetConversationID())
		}
		cursor = syncCursor{
			Version: cursorVersion, Phase: cursorPhaseBatch, SinceMicros: sinceMicros,
			HighWaterMicros: nowMicros, ConversationIDs: ids,
		}
	}
	s.logger.Info().
		Int("listed_conversations", len(conversations)).
		Int("catalog_threads", len(threads)).
		Int("cursor_conversations", len(cursor.ConversationIDs)).
		Str("cursor_phase", cursor.Phase).
		Msg("prepared Google Messages sync catalog")

	messages := make([]rawMessage, 0, limit)
	seen := make(map[string]struct{}, limit)
	protocolCalls := 0
	for cursor.ConversationIndex < len(cursor.ConversationIDs) && len(messages) < limit && protocolCalls < maxProtocolCallsPerSync {
		conversationID := cursor.ConversationIDs[cursor.ConversationIndex]
		conversation := byID[conversationID]
		if conversation == nil {
			conversation = &gmproto.Conversation{ConversationID: conversationID, Name: conversationID}
		}
		messageCursor, err := decodeMessageCursor(cursor.MessageCursor)
		if err != nil {
			return nil, threads, cursor, err
		}
		count := int64(min(messagePageSize, limit-len(messages)))
		response, err := s.client.FetchMessages(ctx, conversationID, count, messageCursor)
		if err != nil {
			return nil, threads, cursor, fmt.Errorf("fetch messages for %s: %w", conversationID, err)
		}
		protocolCalls++
		oldestReached := false
		for _, message := range response.GetMessages() {
			if message.GetTimestamp() < cursor.SinceMicros {
				oldestReached = true
				continue
			}
			normalized, ok := normalizeMessage(message, conversation)
			if !ok {
				continue
			}
			if _, duplicate := seen[normalized.MessageID]; duplicate {
				continue
			}
			seen[normalized.MessageID] = struct{}{}
			messages = append(messages, normalized)
			if len(messages) == limit {
				break
			}
		}

		nextMessageCursor, hasMore, err := encodeMessageCursor(response.GetCursor())
		if err != nil {
			return nil, threads, cursor, err
		}
		if hasMore && !oldestReached {
			cursor.MessageCursor = nextMessageCursor
			if len(messages) == limit || protocolCalls >= maxProtocolCallsPerSync {
				break
			}
			continue
		}
		cursor.ConversationIndex++
		cursor.MessageCursor = ""
	}

	sort.Slice(messages, func(i, j int) bool {
		if messages[i].Timestamp.Equal(messages[j].Timestamp) {
			return messages[i].MessageID < messages[j].MessageID
		}
		return messages[i].Timestamp.Before(messages[j].Timestamp)
	})
	if cursor.ConversationIndex >= len(cursor.ConversationIDs) {
		cursor = syncCursor{Version: cursorVersion, Phase: cursorPhaseLive, AfterMicros: cursor.HighWaterMicros}
	}
	return messages, threads, cursor, nil
}

func (s *libGMSession) listConversations(ctx context.Context) ([]*gmproto.Conversation, map[string]*gmproto.Conversation, error) {
	folders := []gmproto.ListConversationsRequest_Folder{
		gmproto.ListConversationsRequest_INBOX,
		gmproto.ListConversationsRequest_ARCHIVE,
	}
	byID := make(map[string]*gmproto.Conversation)
	for _, folder := range folders {
		response, err := s.client.ListConversations(ctx, conversationPageSize, folder)
		if err != nil {
			return nil, nil, fmt.Errorf("list %s conversations: %w", folder.String(), err)
		}
		for _, conversation := range response.GetConversations() {
			if id := strings.TrimSpace(conversation.GetConversationID()); id != "" {
				byID[id] = conversation
			}
		}
	}
	conversations := make([]*gmproto.Conversation, 0, len(byID))
	for _, conversation := range byID {
		conversations = append(conversations, conversation)
	}
	sort.Slice(conversations, func(i, j int) bool {
		return conversations[i].GetLastMessageTimestamp() > conversations[j].GetLastMessageTimestamp()
	})
	if len(conversations) > maxCursorConversationIDs {
		conversations = conversations[:maxCursorConversationIDs]
	}
	return conversations, byID, nil
}

func (s *libGMSession) Send(ctx context.Context, threadID, text string) (rawMessage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	conversation, err := s.client.GetConversation(ctx, threadID)
	if err != nil {
		return rawMessage{}, fmt.Errorf("get conversation: %w", err)
	}
	participantID, sim := selfParticipant(conversation)
	tmpID := "ditto_" + uuid.NewString()
	payload := &gmproto.SendMessageRequest{
		ConversationID: threadID,
		MessagePayload: &gmproto.MessagePayload{
			TmpID: tmpID, ConversationID: threadID, ParticipantID: participantID, TmpID2: tmpID,
			MessageInfo: []*gmproto.MessageInfo{{Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: text}}}},
		},
		SIMPayload: sim,
		TmpID:      tmpID,
	}
	response, err := s.client.SendMessage(ctx, payload)
	if err != nil {
		return rawMessage{}, err
	}
	if response.GetStatus() != gmproto.SendMessageResponse_SUCCESS {
		return rawMessage{}, fmt.Errorf("phone rejected send with status %s", response.GetStatus().String())
	}
	threadKind := "dm"
	if conversation.GetIsGroupChat() {
		threadKind = "group"
	}
	return rawMessage{
		Timestamp: time.Now().UTC(), ThreadID: threadID, ThreadName: threadName(conversation), ThreadKind: threadKind,
		SenderID: participantID, SenderName: "You", Text: text, MessageID: tmpID, IsFromSelf: true,
	}, nil
}

func cloneCookies(cookies map[string]string) map[string]string {
	cloned := make(map[string]string, len(cookies))
	for key, value := range cookies {
		cloned[key] = value
	}
	return cloned
}

func decodeMessageCursor(value string) (*gmproto.Cursor, error) {
	if value == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) > 16<<10 {
		return nil, errors.New("message cursor is invalid")
	}
	var cursor gmproto.Cursor
	if err := proto.Unmarshal(raw, &cursor); err != nil {
		return nil, errors.New("message cursor is invalid")
	}
	return &cursor, nil
}

func encodeMessageCursor(cursor *gmproto.Cursor) (string, bool, error) {
	if cursor == nil || proto.Size(cursor) == 0 {
		return "", false, nil
	}
	raw, err := proto.Marshal(cursor)
	if err != nil {
		return "", false, fmt.Errorf("encode message cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(raw), true, nil
}
