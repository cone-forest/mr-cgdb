package pipeline

import (
	"context"
	"errors"
	"os"
	"strings"
	"time"

	"mr-cgdb/internal/mathvec"
	"mr-cgdb/internal/ollama"
	"mr-cgdb/internal/seed"
	"mr-cgdb/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Options struct {
	ShadowThreshold    float64
	ShadowNegativeBias float64
	PositiveSeedPath   string
	NegativeSeedPath   string
	SystemPrompt       string
	LLMMaxAttempts     int
	HTTPClientTimeout  time.Duration
	EnableLLM          bool
}

// Processor runs embedding+shadow+LLM for a paper_id.
type Processor struct {
	Pool          *pgxpool.Pool
	OC            *ollama.Client
	PositiveSeeds []seed.Text
	NegativeSeeds []seed.Text
	Opt           Options
}

func (p *Processor) LoadSeeds(ctx context.Context) error {
	var pos []seed.Text
	var neg []seed.Text
	if path := p.Opt.PositiveSeedPath; path != "" {
		if _, err := os.Stat(path); err == nil {
			s, err := seed.LoadFromFile(ctx, path, p.OC)
			if err != nil {
				return err
			}
			pos = s
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	if path := p.Opt.NegativeSeedPath; path != "" {
		if _, err := os.Stat(path); err == nil {
			s, err := seed.LoadFromFile(ctx, path, p.OC)
			if err != nil {
				return err
			}
			neg = s
		} else if !os.IsNotExist(err) {
			return err
		}
	}
	p.PositiveSeeds = pos
	p.NegativeSeeds = neg
	return nil
}

func (p *Processor) Process(ctx context.Context, id int64) error {
	r, err := store.GetPaperRow(ctx, p.Pool, id)
	if err != nil {
		return err
	}
	text := strings.TrimSpace(r.Title) + "\n" + strings.TrimSpace(r.Abstract)
	emb, err := p.OC.Embedder(ctx, text)
	if err != nil {
		// Auto-recover common startup issue: model missing in Ollama cache.
		if perr := p.OC.Pull(ctx, p.OC.EmbedModel); perr == nil {
			emb, err = p.OC.Embedder(ctx, text)
		}
	}
	embedErr := err
	if embedErr == nil {
		var max float64
		var argmax string
		for _, s := range p.PositiveSeeds {
			sim := mathvec.Cosine(emb, s.Embedding)
			if sim > max {
				max, argmax = sim, s.ID
			}
		}
		var negMax float64
		for _, s := range p.NegativeSeeds {
			sim := mathvec.Cosine(emb, s.Embedding)
			if sim > negMax {
				negMax = sim
			}
		}
		var maxPtr *float64
		var wpPtr *bool
		if len(p.PositiveSeeds) > 0 {
			maxPtr = &max
			wp := false
			if p.Opt.ShadowThreshold > 0 {
				wp = max >= p.Opt.ShadowThreshold
				if len(p.NegativeSeeds) > 0 {
					wp = wp && (max-negMax >= p.Opt.ShadowNegativeBias)
				}
			}
			wpPtr = &wp
		}
		var ap *string
		if argmax != "" {
			ap = &argmax
		}
		if e := store.UpdateShadowResult(ctx, p.Pool, id, emb, maxPtr, wpPtr, ap); e != nil {
			return e
		}
	}
	if !p.Opt.EnableLLM {
		if embedErr != nil {
			return store.SetLLMPending(ctx, p.Pool, id, "embed failed: "+embedErr.Error())
		}
		return nil
	}
	sys := strings.TrimSpace(p.Opt.SystemPrompt)
	if sys == "" {
		sys = `You are a strict classifier. Given title and abstract, respond with JSON only: {"relevant":true} if the work is clearly about cluster / hierarchical / LOD in computer graphics research; otherwise {"relevant":false}.`
	}
	usr := "Title:\n" + r.Title + "\n\nAbstract:\n" + r.Abstract
	var last string
	attempts := p.Opt.LLMMaxAttempts
	if attempts < 1 {
		attempts = 3
	}
	chatCtx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	chatPulled := false
	for i := 0; i < attempts; i++ {
		rel, raw, e := p.OC.ChatRelevant(chatCtx, sys, usr)
		last = raw
		if e == nil {
			return store.SetLLMOK(ctx, p.Pool, id, rel, raw)
		}
		if !chatPulled {
			chatPulled = true
			_ = p.OC.Pull(chatCtx, p.OC.ChatModel)
			continue
		}
		if errors.Is(e, context.DeadlineExceeded) {
			return store.SetLLMPending(ctx, p.Pool, id, "llm timeout: "+e.Error())
		}
		// likely invalid JSON or model error: retry
	}
	if embedErr != nil {
		return store.SetLLMPending(ctx, p.Pool, id, "embed failed and llm failed; embed="+embedErr.Error()+"; llm last="+last)
	}
	return store.SetLLMPending(ctx, p.Pool, id, "llm parse failed after attempts; last: "+last)
}
