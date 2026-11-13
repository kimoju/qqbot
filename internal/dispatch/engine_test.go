package dispatch

import (
	"fmt"
	"testing"
	"time"

	"github.com/kimoju/qqbot/internal/stream"
)

func TestEngineParsesDirectCommand(t *testing.T) {
	t.Parallel()

	engine := New(Config{Prefix: "/", DedupWindow: time.Hour})
	route, drop := engine.Route(stream.Envelope{
		ID:   "msg-1",
		Kind: "direct_message",
		To: stream.Destination{
			Kind: "direct",
			ID:   "user-1",
		},
		Text: "/status modem-1",
	})
	if drop {
		t.Fatal("Route() drop = true, want false")
	}
	if route.Command == nil {
		t.Fatal("Route() command = nil")
	}
	if route.Command.Name != "status" {
		t.Fatalf("command name = %q, want %q", route.Command.Name, "status")
	}
	if len(route.Command.Params) != 1 || route.Command.Params[0] != "modem-1" {
		t.Fatalf("command params = %#v", route.Command.Params)
	}
}

func TestEngineParsesGroupMentionCommand(t *testing.T) {
	t.Parallel()

	engine := New(Config{Prefix: "/", DedupWindow: time.Hour})
	route, drop := engine.Route(stream.Envelope{
		ID:   "msg-2",
		Kind: "group_mention",
		To: stream.Destination{
			Kind: "group",
			ID:   "group-1",
		},
		Text: "<@bot> /send 123 hello",
	})
	if drop {
		t.Fatal("Route() drop = true, want false")
	}
	if route.Command == nil || route.Command.Name != "send" {
		t.Fatalf("command = %#v", route.Command)
	}
}

func TestEngineDeduplicates(t *testing.T) {
	t.Parallel()

	engine := New(Config{Prefix: "/", DedupWindow: 50 * time.Millisecond})
	_, drop := engine.Route(stream.Envelope{ID: "msg-3", Kind: "direct_message"})
	if drop {
		t.Fatal("first drop = true, want false")
	}
	_, drop = engine.Route(stream.Envelope{ID: "msg-3", Kind: "direct_message"})
	if !drop {
		t.Fatal("second drop = false, want true")
	}
	time.Sleep(70 * time.Millisecond)
	_, drop = engine.Route(stream.Envelope{ID: "msg-3", Kind: "direct_message"})
	if drop {
		t.Fatal("drop after window = true, want false")
	}
}

func TestEngineMultiMentionBareMentionAndSlashNormalization(t *testing.T) {
	t.Parallel()

	engine := New(Config{Prefix: "/", DedupWindow: time.Hour})

	commandCases := []struct {
		id         string
		kind       string
		text       string
		wantCmd    string
		wantParams []string
	}{
		{
			id:         "multi-tag-spaces",
			kind:       "group_mention",
			text:       "<@!111> <@!222> /list",
			wantCmd:    "list",
			wantParams: nil,
		},
		{
			id:         "multi-tag-role-tight",
			kind:       "group_mention",
			text:       "<@!111><@&role99><@333>/switch dev-1 2",
			wantCmd:    "switch",
			wantParams: []string{"dev-1", "2"},
		},
		{
			id:         "mixed-tag-and-bare-at-ideographic-space",
			kind:       "group_mention",
			text:       "<@!111> @VoHiveBot\u3000/sms modem-2",
			wantCmd:    "sms",
			wantParams: []string{"modem-2"},
		},
	}

	for _, tc := range commandCases {
		route, drop := engine.Route(stream.Envelope{
			ID:   tc.id,
			Kind: tc.kind,
			To:   stream.Destination{Kind: "group", ID: "g-1"},
			Text: tc.text,
		})
		if drop {
			t.Fatalf("%s: unexpected drop", tc.id)
		}
		if route.Command == nil {
			t.Fatalf("%s: expected command %q, got nil", tc.id, tc.wantCmd)
		}
		if route.Command.Name != tc.wantCmd {
			t.Fatalf("%s: command name = %q, want %q", tc.id, route.Command.Name, tc.wantCmd)
		}
		if len(route.Command.Params) != len(tc.wantParams) {
			t.Fatalf("%s: params = %#v, want %#v", tc.id, route.Command.Params, tc.wantParams)
		}
		for i := range tc.wantParams {
			if route.Command.Params[i] != tc.wantParams[i] {
				t.Fatalf("%s: params[%d] = %q, want %q", tc.id, i, route.Command.Params[i], tc.wantParams[i])
			}
		}
	}

	nonCommandCases := []struct {
		id   string
		kind string
		text string
	}{
		{id: "double-slash", kind: "direct_message", text: "//comment not a command"},
		{id: "double-slash-cmd", kind: "direct_message", text: "//status"},
		{id: "triple-slash", kind: "direct_message", text: "///foo"},
		{id: "slash-space", kind: "direct_message", text: "/ status"},
		{id: "slash-only", kind: "direct_message", text: "/"},
		{id: "group-double-slash", kind: "group_mention", text: "<@!111> //notcmd"},
		{id: "bare-at-bot", kind: "group_mention", text: "@BotName"},
		{id: "bare-multi-at", kind: "group_mention", text: "<@!111> @BotName"},
		{id: "unclosed-tag", kind: "group_mention", text: "<@12345"},
	}

	for _, tc := range nonCommandCases {
		route, drop := engine.Route(stream.Envelope{
			ID:   tc.id,
			Kind: tc.kind,
			To:   stream.Destination{Kind: "group", ID: "g-1"},
			Text: tc.text,
		})
		if drop {
			t.Fatalf("%s: unexpected drop", tc.id)
		}
		if route.Command != nil {
			t.Fatalf("%s (%q): expected nil Command, got %+v", tc.id, tc.text, route.Command)
		}
	}

	// Verify bare "@BotName" normalizes to empty string even if prefix is "@".
	atEngine := New(Config{Prefix: "@", DedupWindow: time.Hour})
	route, _ := atEngine.Route(stream.Envelope{
		ID:   "at-prefix-bare",
		Kind: "group_mention",
		Text: "@BotName",
	})
	if route.Command != nil {
		t.Fatalf("bare @BotName with @ prefix must normalize to empty, got command %+v", route.Command)
	}
}

func TestMemorySeenStoreBoundedCapacityAndAmortizedEviction(t *testing.T) {
	t.Parallel()

	const limit = 64
	store := NewMemorySeenStoreWithLimit(limit)
	base := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	ttl := 2 * time.Hour

	for i := 0; i < 300; i++ {
		id := fmt.Sprintf("msg-%d", i)
		if store.Seen("direct_message", id, base, ttl) {
			t.Fatalf("first Seen(%s) = true, want false", id)
		}
		if got := store.len(); got > limit {
			t.Fatalf("store.len() = %d, exceeds limit %d", got, limit)
		}
	}

	// Newest `limit` items [236..299] must still be marked seen.
	for i := 300 - limit; i < 300; i++ {
		id := fmt.Sprintf("msg-%d", i)
		if !store.Seen("direct_message", id, base.Add(time.Second), ttl) {
			t.Fatalf("recent item %s should still be present in bounded store", id)
		}
	}

	// Oldest items [0..10] must have been FIFO-evicted when capacity was exceeded.
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("msg-%d", i)
		if store.Seen("direct_message", id, base.Add(time.Second), ttl) {
			t.Fatalf("oldest item %s should have been evicted", id)
		}
	}

	// Verify burst flood followed by TTL expiration compacts backing storage cleanly.
	floodStore := NewMemorySeenStoreWithLimit(2048)
	for i := 0; i < 1500; i++ {
		floodStore.Seen("group_mention", fmt.Sprintf("flood-%d", i), base, 5*time.Minute)
	}
	if got := floodStore.len(); got != 1500 {
		t.Fatalf("floodStore.len() = %d, want 1500", got)
	}

	afterExpiry := base.Add(10 * time.Minute)
	if floodStore.Seen("group_mention", "post-expiry", afterExpiry, 5*time.Minute) {
		t.Fatal("post-expiry item unexpectedly marked duplicate")
	}
	if got := floodStore.len(); got != 1 {
		t.Fatalf("floodStore.len() after expiry sweep = %d, want 1", got)
	}
}
