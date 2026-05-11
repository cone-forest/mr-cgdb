package identity

import (
	"crypto/sha256"
	"fmt"
	"strings"
	"unicode"

	"golang.org/x/text/unicode/norm"
)

// NormalizeTitle applies NFKC, lowercases, collapses whitespace, trims outer punctuation
// and trailing noise like (arxiv:...) per design discussion.
func NormalizeTitle(s string) string {
	s = norm.NFKC.String(s)
	s = strings.ToLower(s)
	s = strings.TrimSpace(s)
	// simple outer punctuation trim
	s = strings.Trim(s, " \t\n\r\"'“”")
	var b strings.Builder
	prevSpace := true
	for _, r := range s {
		if unicode.IsSpace(r) {
			if !prevSpace {
				b.WriteByte(' ')
			}
			prevSpace = true
			continue
		}
		prevSpace = false
		b.WriteRune(r)
	}
	return strings.TrimSpace(b.String())
}

// FirstAuthorSurname is a minimal extraction for weak keys.
func FirstAuthorSurname(authors []string) string {
	if len(authors) == 0 {
		return ""
	}
	a := strings.TrimSpace(authors[0])
	parts := strings.Fields(a)
	if len(parts) == 0 {
		return ""
	}
	s := strings.ToLower(parts[len(parts)-1])
	s = strings.Trim(s, ".,;:")
	return s
}

// WeakKey returns sha256 hex of normalized_title|year|surname, or "" if any required part missing.
func WeakKey(title string, year *int, authors []string) string {
	nt := NormalizeTitle(title)
	if nt == "" || year == nil {
		return ""
	}
	sur := FirstAuthorSurname(authors)
	if sur == "" {
		return ""
	}
	p := fmt.Sprintf("%s|%d|%s", nt, *year, sur)
	sum := sha256.Sum256([]byte(p))
	return fmt.Sprintf("%x", sum[:])
}
