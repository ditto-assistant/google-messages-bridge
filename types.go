package main

import (
	"context"
	"time"
)

type credentials map[string]any

type attachment struct {
	Type string `json:"type"`
	Name string `json:"name,omitempty"`
	URL  string `json:"url,omitempty"`
}

type rawMessage struct {
	Timestamp   time.Time    `json:"timestamp"`
	ThreadID    string       `json:"threadId"`
	ThreadName  string       `json:"threadName"`
	ThreadKind  string       `json:"threadKind"`
	SenderID    string       `json:"senderId"`
	SenderName  string       `json:"senderName"`
	Text        string       `json:"text"`
	MessageID   string       `json:"messageId"`
	Attachments []attachment `json:"attachments,omitempty"`
	IsFromSelf  bool         `json:"isFromSelf"`
}

type pairingMethod string

const (
	pairingMethodQR     pairingMethod = "qr"
	pairingMethodGoogle pairingMethod = "google"
)

type pairingChallenge struct {
	Kind   string
	Prompt string
}

type pairing interface {
	Challenge() pairingChallenge
	Wait(context.Context) (session, displayName string, err error)
	Close()
}

type bridgeSession interface {
	Sync(context.Context, syncCursor, *time.Time, int) ([]rawMessage, syncCursor, error)
	Send(context.Context, string, string) (rawMessage, error)
	Close()
}

type protocol interface {
	StartPairing(context.Context, pairingMethod, map[string]string) (pairing, error)
	OpenSession(context.Context, string) (bridgeSession, error)
}
