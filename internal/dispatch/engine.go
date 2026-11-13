package dispatch

import (
	"encoding/json"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/kimoju/qqbot/internal/stream"
)

type Config struct {
	Prefix      string
	DedupWindow time.Duration
	Store       SeenStore
}

type Engine struct {
	prefix string
	window time.Duration
	store  SeenStore
}

type Route struct {
	Incoming Incoming
	Command  *Command
}

type Incoming struct {
	ID   string
	Kind string
	From string
	To   Recipient
	Text string
	At   time.Time
	Raw  json.RawMessage
}

type Recipient struct {
	Kind string
	ID   string
}

type Command struct {
	Name     string
	Params   []string
	Original string
}

func New(cfg Config) *Engine {
	prefix := strings.TrimSpace(cfg.Prefix)
	if prefix == "" {
		prefix = "/"
	}
	window := cfg.DedupWindow
	if window <= 0 {
		window = 2 * time.Hour
	}
	store := cfg.Store
	if store == nil {
		store = NewMemorySeenStore()
	}
	return &Engine{
		prefix: prefix,
		window: window,
		store:  store,
	}
}

func (e *Engine) Route(event stream.Envelope) (Route, bool) {
	if e.duplicate(event.Kind, event.ID) {
		return Route{}, true
	}

	route := Route{
		Incoming: Incoming{
			ID:   event.ID,
			Kind: event.Kind,
			From: event.From,
			To: Recipient{
				Kind: event.To.Kind,
				ID:   event.To.ID,
			},
			Text: event.Text,
			At:   event.At,
			Raw:  event.Raw,
		},
	}

	text := normalizeText(event)
	route.Command = parseCommand(text, e.prefix)
	return route, false
}

func (e *Engine) duplicate(kind string, id string) bool {
	return e.store.Seen(kind, id, time.Now(), e.window)
}

func parseCommand(text string, prefix string) *Command {
	if text == "" || !strings.HasPrefix(text, prefix) {
		return nil
	}
	rest := strings.TrimPrefix(text, prefix)
	if rest == "" {
		return nil
	}
	firstRune, _ := utf8.DecodeRuneInString(rest)
	if firstRune == utf8.RuneError || unicode.IsSpace(firstRune) || firstRune == '/' || strings.HasPrefix(rest, prefix) {
		return nil
	}

	fields := strings.Fields(rest)
	if len(fields) == 0 {
		return nil
	}
	name := strings.ToLower(strings.TrimSpace(fields[0]))
	if name == "" {
		return nil
	}
	return &Command{
		Name:     name,
		Params:   append([]string(nil), fields[1:]...),
		Original: text,
	}
}

func normalizeText(event stream.Envelope) string {
	text := strings.TrimSpace(event.Text)
	if event.Kind != "group_mention" {
		return text
	}
	for text != "" {
		if strings.HasPrefix(text, "<@") {
			index := strings.IndexByte(text, '>')
			if index < 0 {
				break
			}
			text = strings.TrimSpace(text[index+1:])
			continue
		}
		if strings.HasPrefix(text, "@") {
			index := strings.IndexFunc(text, unicode.IsSpace)
			if index < 0 {
				return ""
			}
			text = strings.TrimSpace(text[index:])
			continue
		}
		break
	}
	return text
}
