package main

import (
	"strings"
	"testing"
)

func TestCursorRoundTrip(t *testing.T) {
	want := syncCursor{
		Version: cursorVersion, Phase: cursorPhaseBatch, SinceMicros: 123,
		HighWaterMicros: 456, ConversationIDs: []string{"one", "two"},
		ConversationIndex: 1, MessageCursor: "opaque",
	}
	encoded, err := encodeCursor(want)
	if err != nil {
		t.Fatal(err)
	}
	got, err := decodeCursor(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.Phase != want.Phase || got.ConversationIndex != want.ConversationIndex || got.MessageCursor != want.MessageCursor || len(got.ConversationIDs) != 2 {
		t.Fatalf("decoded cursor = %#v, want %#v", got, want)
	}
}

func TestCursorRejectsOversizedConversationList(t *testing.T) {
	cursor := syncCursor{Version: cursorVersion, Phase: cursorPhaseBatch, ConversationIDs: make([]string, maxCursorConversationIDs+1)}
	for index := range cursor.ConversationIDs {
		cursor.ConversationIDs[index] = "conversation"
	}
	encoded, err := encodeCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeCursor(encoded); err == nil || !strings.Contains(err.Error(), "position") {
		t.Fatalf("decodeCursor() error = %v, want invalid position", err)
	}
}
