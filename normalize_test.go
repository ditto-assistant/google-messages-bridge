package main

import (
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func TestNormalizeMessage(t *testing.T) {
	timestamp := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
	message := &gmproto.Message{
		MessageID: "message-1", ConversationID: "thread-1", Timestamp: timestamp.UnixMicro(),
		MessageInfo:       []*gmproto.MessageInfo{{Data: &gmproto.MessageInfo_MessageContent{MessageContent: &gmproto.MessageContent{Content: "Hello"}}}},
		SenderParticipant: &gmproto.Participant{FullName: "Ada", ID: &gmproto.SmallInfo{Number: "+15551234567"}},
	}
	conversation := &gmproto.Conversation{ConversationID: "thread-1", Name: "Ada", IsGroupChat: false}
	got, ok := normalizeMessage(message, conversation)
	if !ok {
		t.Fatal("normalizeMessage() rejected a valid message")
	}
	if got.Text != "Hello" || got.SenderName != "Ada" || got.SenderID != "+15551234567" || got.ThreadKind != "dm" || !got.Timestamp.Equal(timestamp) {
		t.Fatalf("normalizeMessage() = %#v", got)
	}
}

func TestNormalizeAttachmentOnlyMessage(t *testing.T) {
	message := &gmproto.Message{
		MessageID: "message-2", ConversationID: "thread-2", Timestamp: time.Now().UnixMicro(),
		MessageInfo: []*gmproto.MessageInfo{{Data: &gmproto.MessageInfo_MediaContent{MediaContent: &gmproto.MediaContent{MimeType: "image/jpeg", MediaName: "photo.jpg"}}}},
	}
	got, ok := normalizeMessage(message, &gmproto.Conversation{ConversationID: "thread-2", IsGroupChat: true})
	if !ok || len(got.Attachments) != 1 || got.Attachments[0].Type != "image" || got.ThreadKind != "group" {
		t.Fatalf("normalizeMessage() = %#v, %v", got, ok)
	}
}
