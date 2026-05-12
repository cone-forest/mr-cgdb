package keywords

import (
	"bufio"
	"os"
	"strings"
	"unicode"
)

// Phrases from file, any phrase match (case-insensitive substring) passes.
type Matcher struct {
	phrases []string
}

func New(phrases []string) *Matcher {
	var out []string
	for _, p := range phrases {
		p = strings.ToLower(strings.TrimSpace(p))
		if p != "" {
			out = append(out, p)
		}
	}
	return &Matcher{phrases: out}
}

// PhraseCount returns how many non-empty phrases this matcher uses (global gate diagnostics).
func (m *Matcher) PhraseCount() int {
	if m == nil {
		return 0
	}
	return len(m.phrases)
}

// Load reads one phrase per line, ignores empty and # comments.
func Load(path string) (*Matcher, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var phrases []string
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		p := strings.ToLower(line)
		if p != "" {
			phrases = append(phrases, p)
		}
	}
	if err := s.Err(); err != nil {
		return nil, err
	}
	return New(phrases), nil
}

// MatchText returns true if any phrase is a substring of hay.
func (m *Matcher) MatchText(hay string) bool {
	lower := toLower(hay)
	for _, p := range m.phrases {
		if p != "" && strings.Contains(lower, p) {
			return true
		}
	}
	return false
}

func toLower(s string) string {
	if s == "" {
		return s
	}
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		b.WriteRune(unicode.ToLower(r))
	}
	return b.String()
}
