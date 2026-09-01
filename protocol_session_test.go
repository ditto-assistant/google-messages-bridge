package main

import (
	"context"
	"testing"
	"time"

	"go.mau.fi/mautrix-gmessages/pkg/libgm"
	"go.mau.fi/mautrix-gmessages/pkg/libgm/events"
)

type fakeGMSessionClient struct {
	handler libgm.EventHandler
	event   any
}

func (c *fakeGMSessionClient) SetEventHandler(handler libgm.EventHandler) {
	c.handler = handler
}

func (c *fakeGMSessionClient) Connect() error {
	c.handler(c.event)
	return nil
}

func TestConnectAndWaitForSessionAcceptsFirstLongPoll(t *testing.T) {
	client := &fakeGMSessionClient{event: &events.ListenRecovered{}}
	if err := connectAndWaitForSession(context.Background(), client, 20*time.Millisecond); err != nil {
		t.Fatalf("first successful long poll was not accepted: %v", err)
	}
}
