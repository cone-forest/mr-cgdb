package seed

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"unicode"

	"mr-cgdb/internal/ollama"
)

// Text holds one seed with its arXiv id and L2-normalized embedding.
type Text struct {
	ID        string
	TitleAbs  string
	Embedding []float64
}

type bibEntry struct {
	ID       string
	Title    string
	Abstract string
	Book     string
	Authors  string
	Year     string
	Keywords string
}

// LoadFromFile parses BibTeX entries from seeds.txt and embeds each one.
func LoadFromFile(ctx context.Context, path string, oc *ollama.Client) ([]Text, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	entries, err := parseBibTeX(string(raw))
	if err != nil {
		return nil, err
	}
	out := make([]Text, 0, len(entries))
	for _, e := range entries {
		text := buildSeedText(e)
		if strings.TrimSpace(text) == "" {
			continue
		}
		emb, err := oc.Embedder(ctx, text)
		if err != nil {
			return nil, fmt.Errorf("embed seed %s: %w", e.ID, err)
		}
		out = append(out, Text{ID: e.ID, TitleAbs: text, Embedding: emb})
	}
	return out, nil
}

func parseBibTeX(src string) ([]bibEntry, error) {
	var out []bibEntry
	for i := 0; i < len(src); {
		at := strings.IndexByte(src[i:], '@')
		if at < 0 {
			break
		}
		at += i
		open := strings.IndexByte(src[at:], '{')
		if open < 0 {
			break
		}
		open += at
		depth := 0
		end := -1
		for j := open; j < len(src); j++ {
			switch src[j] {
			case '{':
				depth++
			case '}':
				depth--
				if depth == 0 {
					end = j
					break
				}
			}
		}
		if end < 0 {
			return nil, fmt.Errorf("unclosed BibTeX entry near byte %d", at)
		}
		block := src[at : end+1]
		if e, ok := parseBibEntry(block); ok {
			out = append(out, e)
		}
		i = end + 1
	}
	return out, nil
}

func parseBibEntry(block string) (bibEntry, bool) {
	headOpen := strings.IndexByte(block, '{')
	if headOpen < 0 {
		return bibEntry{}, false
	}
	firstComma := strings.IndexByte(block[headOpen+1:], ',')
	if firstComma < 0 {
		return bibEntry{}, false
	}
	firstComma += headOpen + 1
	key := strings.TrimSpace(block[headOpen+1 : firstComma])
	if key == "" {
		return bibEntry{}, false
	}
	body := strings.TrimSpace(block[firstComma+1 : len(block)-1])
	fields := parseBibFields(body)
	title := cleanBibValue(fields["title"])
	if title == "" {
		return bibEntry{}, false
	}
	return bibEntry{
		ID:       key,
		Title:    title,
		Abstract: cleanBibValue(fields["abstract"]),
		Book:     cleanBibValue(firstNonEmpty(fields["booktitle"], fields["journal"])),
		Authors:  cleanBibValue(fields["author"]),
		Year:     cleanBibValue(fields["year"]),
		Keywords: cleanBibValue(fields["keywords"]),
	}, true
}

func parseBibFields(body string) map[string]string {
	out := map[string]string{}
	i := 0
	for i < len(body) {
		for i < len(body) && (unicode.IsSpace(rune(body[i])) || body[i] == ',') {
			i++
		}
		startKey := i
		for i < len(body) && (unicode.IsLetter(rune(body[i])) || body[i] == '_' || body[i] == '-') {
			i++
		}
		if i <= startKey {
			break
		}
		key := strings.ToLower(strings.TrimSpace(body[startKey:i]))
		for i < len(body) && (unicode.IsSpace(rune(body[i])) || body[i] == '=') {
			i++
		}
		if i >= len(body) {
			break
		}
		valStart := i
		if body[i] == '{' {
			depth := 0
			for i < len(body) {
				if body[i] == '{' {
					depth++
				} else if body[i] == '}' {
					depth--
					if depth == 0 {
						i++
						break
					}
				}
				i++
			}
		} else if body[i] == '"' {
			i++
			for i < len(body) {
				if body[i] == '"' && body[i-1] != '\\' {
					i++
					break
				}
				i++
			}
		} else {
			for i < len(body) && body[i] != ',' && body[i] != '\n' {
				i++
			}
		}
		val := strings.TrimSpace(body[valStart:i])
		out[key] = val
	}
	return out
}

func cleanBibValue(v string) string {
	v = strings.TrimSpace(v)
	for len(v) >= 2 {
		if (v[0] == '{' && v[len(v)-1] == '}') || (v[0] == '"' && v[len(v)-1] == '"') {
			v = strings.TrimSpace(v[1 : len(v)-1])
			continue
		}
		break
	}
	return strings.Join(strings.Fields(v), " ")
}

func buildSeedText(e bibEntry) string {
	var parts []string
	parts = append(parts, e.Title)
	if e.Abstract != "" {
		parts = append(parts, e.Abstract)
	} else {
		if e.Book != "" {
			parts = append(parts, "Venue: "+e.Book)
		}
		if e.Authors != "" {
			parts = append(parts, "Authors: "+e.Authors)
		}
		if e.Keywords != "" {
			parts = append(parts, "Keywords: "+e.Keywords)
		}
		if e.Year != "" {
			if _, err := strconv.Atoi(e.Year); err == nil {
				parts = append(parts, "Year: "+e.Year)
			}
		}
	}
	return strings.TrimSpace(strings.Join(parts, "\n"))
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
