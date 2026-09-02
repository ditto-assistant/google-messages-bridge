package main

import (
	"strings"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm/gmproto"
)

func normalizeConversation(conversation *gmproto.Conversation) (conversationThread, bool) {
	if conversation == nil || strings.TrimSpace(conversation.GetConversationID()) == "" || conversation.GetLastMessageTimestamp() <= 0 {
		return conversationThread{}, false
	}
	threadKind := "dm"
	if conversation.GetIsGroupChat() {
		threadKind = "group"
	}
	return conversationThread{
		LastMessageAt: time.UnixMicro(conversation.GetLastMessageTimestamp()).UTC(),
		ThreadID:      conversation.GetConversationID(),
		ThreadName:    threadName(conversation),
		ThreadKind:    threadKind,
	}, true
}

func normalizeMessage(message *gmproto.Message, conversation *gmproto.Conversation) (rawMessage, bool) {
	if message == nil || strings.TrimSpace(message.GetMessageID()) == "" || strings.TrimSpace(message.GetConversationID()) == "" || message.GetTimestamp() <= 0 {
		return rawMessage{}, false
	}
	text := extractMessageBody(message)
	attachments := extractAttachments(message)
	if text == "" && len(attachments) == 0 {
		return rawMessage{}, false
	}
	fromSelf := messageIsFromSelf(message)
	senderName, senderID := senderInfo(message, conversation)
	if fromSelf {
		senderName = "You"
	}
	threadKind := "dm"
	if conversation.GetIsGroupChat() {
		threadKind = "group"
	}
	return rawMessage{
		Timestamp:   time.UnixMicro(message.GetTimestamp()).UTC(),
		ThreadID:    message.GetConversationID(),
		ThreadName:  threadName(conversation),
		ThreadKind:  threadKind,
		SenderID:    senderID,
		SenderName:  senderName,
		Text:        text,
		MessageID:   message.GetMessageID(),
		Attachments: attachments,
		IsFromSelf:  fromSelf,
	}, true
}

func extractMessageBody(message *gmproto.Message) string {
	for _, info := range message.GetMessageInfo() {
		if content := info.GetMessageContent(); content != nil {
			if text := strings.TrimSpace(content.GetContent()); text != "" {
				return text
			}
		}
	}
	return strings.TrimSpace(message.GetSubject())
}

func extractAttachments(message *gmproto.Message) []attachment {
	var attachments []attachment
	for _, info := range message.GetMessageInfo() {
		media := info.GetMediaContent()
		if media == nil {
			continue
		}
		mime := strings.ToLower(strings.TrimSpace(media.GetMimeType()))
		kind := "file"
		switch {
		case strings.HasPrefix(mime, "image/"):
			kind = "image"
		case strings.HasPrefix(mime, "video/"):
			kind = "video"
		case strings.HasPrefix(mime, "audio/"):
			kind = "audio"
		}
		attachments = append(attachments, attachment{Type: kind, Name: media.GetMediaName()})
	}
	return attachments
}

func senderInfo(message *gmproto.Message, conversation *gmproto.Conversation) (name, id string) {
	if sender := message.GetSenderParticipant(); sender != nil {
		name = firstNonEmpty(sender.GetFullName(), sender.GetFirstName())
		if participantID := sender.GetID(); participantID != nil {
			id = participantID.GetNumber()
		}
		id = firstNonEmpty(id, sender.GetFormattedNumber(), message.GetParticipantID())
	}
	if id == "" {
		id = message.GetParticipantID()
	}
	if name == "" {
		for _, participant := range conversation.GetParticipants() {
			participantNumber := participant.GetFormattedNumber()
			if participant.GetID() != nil {
				participantNumber = firstNonEmpty(participant.GetID().GetNumber(), participantNumber)
			}
			if id != "" && participantNumber == id {
				name = firstNonEmpty(participant.GetFullName(), participant.GetFirstName(), participantNumber)
				break
			}
		}
	}
	if name == "" {
		name = id
	}
	return name, id
}

func messageIsFromSelf(message *gmproto.Message) bool {
	if sender := message.GetSenderParticipant(); sender != nil && sender.GetIsMe() {
		return true
	}
	if status := message.GetMessageStatus(); status != nil {
		return strings.HasPrefix(status.GetStatus().String(), "OUTGOING")
	}
	return false
}

func selfParticipant(conversation *gmproto.Conversation) (string, *gmproto.SIMPayload) {
	for _, participant := range conversation.GetParticipants() {
		if !participant.GetIsMe() {
			continue
		}
		id := ""
		if participant.GetID() != nil {
			id = participant.GetID().GetNumber()
		}
		if participant.GetSimPayload() != nil {
			return id, participant.GetSimPayload()
		}
		if card := conversation.GetSimCard(); card != nil {
			return id, card.GetSIMData().GetSIMPayload()
		}
		return id, nil
	}
	if card := conversation.GetSimCard(); card != nil {
		return "", card.GetSIMData().GetSIMPayload()
	}
	return "", nil
}

func threadName(conversation *gmproto.Conversation) string {
	if name := strings.TrimSpace(conversation.GetName()); name != "" {
		return name
	}
	var names []string
	for _, participant := range conversation.GetParticipants() {
		if participant.GetIsMe() {
			continue
		}
		name := firstNonEmpty(participant.GetFullName(), participant.GetFirstName(), participant.GetFormattedNumber())
		if name == "" && participant.GetID() != nil {
			name = participant.GetID().GetNumber()
		}
		if name != "" {
			names = append(names, name)
		}
	}
	if len(names) > 0 {
		return strings.Join(names, ", ")
	}
	return conversation.GetConversationID()
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if trimmed := strings.TrimSpace(value); trimmed != "" {
			return trimmed
		}
	}
	return ""
}
