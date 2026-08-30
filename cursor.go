package main

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

const (
	cursorVersion            = 1
	maxCursorBytes           = 64 << 10
	maxCursorConversationIDs = 200
	cursorPhaseBatch         = "batch"
	cursorPhaseLive          = "live"
)

type syncCursor struct {
	Version           int      `json:"v"`
	Phase             string   `json:"phase"`
	SinceMicros       int64    `json:"sinceMicros,omitempty"`
	HighWaterMicros   int64    `json:"highWaterMicros,omitempty"`
	AfterMicros       int64    `json:"afterMicros,omitempty"`
	ConversationIDs   []string `json:"conversationIds,omitempty"`
	ConversationIndex int      `json:"conversationIndex,omitempty"`
	MessageCursor     string   `json:"messageCursor,omitempty"`
}

func decodeCursor(value string) (syncCursor, error) {
	if strings.TrimSpace(value) == "" {
		return syncCursor{}, nil
	}
	if len(value) > maxCursorBytes*2 {
		return syncCursor{}, errors.New("cursor is too large")
	}
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) > maxCursorBytes {
		return syncCursor{}, errors.New("cursor is invalid")
	}
	var cursor syncCursor
	if err := json.Unmarshal(raw, &cursor); err != nil {
		return syncCursor{}, errors.New("cursor is invalid")
	}
	if cursor.Version != cursorVersion {
		return syncCursor{}, fmt.Errorf("cursor version %d is unsupported", cursor.Version)
	}
	if cursor.Phase != cursorPhaseBatch && cursor.Phase != cursorPhaseLive {
		return syncCursor{}, errors.New("cursor phase is invalid")
	}
	if len(cursor.ConversationIDs) > maxCursorConversationIDs || cursor.ConversationIndex < 0 || cursor.ConversationIndex > len(cursor.ConversationIDs) {
		return syncCursor{}, errors.New("cursor position is invalid")
	}
	return cursor, nil
}

func encodeCursor(cursor syncCursor) (string, error) {
	cursor.Version = cursorVersion
	raw, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode cursor: %w", err)
	}
	if len(raw) > maxCursorBytes {
		return "", errors.New("encoded cursor is too large")
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}
