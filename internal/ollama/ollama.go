package ollama

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"
)

type Client struct {
	BaseURL    string
	HTTP       *http.Client
	EmbedModel string
	ChatModel  string
}

// NewDefault creates a client. base should be e.g. http://ollama:11434
func NewDefault(base, embed, chat string) *Client {
	return &Client{
		BaseURL:    base,
		HTTP:       &http.Client{Timeout: 10 * time.Minute},
		EmbedModel: embed,
		ChatModel:  chat,
	}
}

// Embedder returns a length-normalized embedding vector. Uses ollama /api/embed.
func (c *Client) Embedder(ctx context.Context, text string) ([]float64, error) {
	body := map[string]any{
		"model": c.EmbedModel,
		"input": text,
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/embed", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		// try legacy /api/embeddings
		return c.embeddingsLegacy(ctx, text)
	}
	var out struct {
		Embeddings [][]float64 `json:"embeddings"`
		Embedding  []float64   `json:"embedding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	v := out.Embedding
	if len(v) == 0 && len(out.Embeddings) > 0 {
		v = out.Embeddings[0]
	}
	if len(v) == 0 {
		return nil, fmt.Errorf("empty embedding")
	}
	return normalizeL2(v), nil
}

func (c *Client) embeddingsLegacy(ctx context.Context, text string) ([]float64, error) {
	body := map[string]any{
		"model":  c.EmbedModel,
		"prompt": text,
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/embeddings", bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("ollama embed: %s", resp.Status)
	}
	var out struct {
		Embedding []float64 `json:"embedding"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, err
	}
	return normalizeL2(out.Embedding), nil
}

// ChatRelevant asks the model for strict JSON: {"relevant":true|false}
func (c *Client) ChatRelevant(ctx context.Context, system, user string) (relevant bool, raw string, err error) {
	body := map[string]any{
		"model": c.ChatModel,
		"messages": []map[string]string{
			{"role": "system", "content": system},
			{"role": "user", "content": user},
		},
		"stream": false,
		"format": "json",
		"options": map[string]any{
			"temperature": 0.0,
		},
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/chat", bytes.NewReader(b))
	if err != nil {
		return false, "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false, "", fmt.Errorf("ollama chat: %s", resp.Status)
	}
	var out struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, "", err
	}
	raw = out.Message.Content
	var m struct {
		Relevant bool `json:"relevant"`
	}
	if jerr := json.Unmarshal([]byte(raw), &m); jerr != nil {
		return false, raw, jerr
	}
	return m.Relevant, raw, nil
}

// ChatDeepVerify asks for strict JSON: {"useful":true|false,"reason":"..."}.
func (c *Client) ChatDeepVerify(ctx context.Context, system, user string) (useful bool, reason string, raw string, err error) {
	body := map[string]any{
		"model": c.ChatModel,
		"messages": []map[string]string{
			{"role": "system", "content": system},
			{"role": "user", "content": user},
		},
		"stream": false,
		"format": "json",
		"options": map[string]any{
			"temperature": 0.0,
		},
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/chat", bytes.NewReader(b))
	if err != nil {
		return false, "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return false, "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return false, "", "", fmt.Errorf("ollama chat: %s", resp.Status)
	}
	var out struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return false, "", "", err
	}
	raw = out.Message.Content
	var m struct {
		Useful bool   `json:"useful"`
		Reason string `json:"reason"`
	}
	if jerr := json.Unmarshal([]byte(raw), &m); jerr != nil {
		return false, "", raw, jerr
	}
	return m.Useful, m.Reason, raw, nil
}

// ChatJSON asks the model for JSON output and returns raw content.
func (c *Client) ChatJSON(ctx context.Context, system, user string) (raw string, err error) {
	body := map[string]any{
		"model": c.ChatModel,
		"messages": []map[string]string{
			{"role": "system", "content": system},
			{"role": "user", "content": user},
		},
		"stream": false,
		"format": "json",
		"options": map[string]any{
			"temperature": 0.2,
		},
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/chat", bytes.NewReader(b))
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("ollama chat: %s", resp.Status)
	}
	var out struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", err
	}
	return out.Message.Content, nil
}

// Pull ensures a model exists locally in the Ollama service.
func (c *Client) Pull(ctx context.Context, model string) error {
	body := map[string]any{
		"model":  model,
		"stream": false,
	}
	b, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.BaseURL+"/api/pull", bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := c.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		msg, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ollama pull %s: %s %s", model, resp.Status, string(msg))
	}
	// Drain JSON response; schema can vary by Ollama version.
	_ = json.NewDecoder(resp.Body).Decode(&map[string]any{})
	return nil
}

func normalizeL2(v []float64) []float64 {
	var s float64
	for _, x := range v {
		s += x * x
	}
	if s == 0 {
		return v
	}
	inv := 1 / math.Sqrt(s)
	out := make([]float64, len(v))
	for i, x := range v {
		out[i] = x * inv
	}
	return out
}
