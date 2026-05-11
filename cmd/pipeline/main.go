package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"mr-cgdb/internal/model"
	"mr-cgdb/internal/ollama"
	"mr-cgdb/internal/pipeline"
	"mr-cgdb/internal/store"
	"mr-cgdb/internal/wire"
)

func main() {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		log.Fatal("DATABASE_URL required")
	}
	listen := getenv("LISTEN", ":9003")
	oll := getenv("OLLAMA_BASE_URL", "http://ollama:11434")
	embed := getenv("EMBED_MODEL", "nomic-embed-text")
	chat := getenv("CHAT_MODEL", "llama3.2:1b")
	posSeedPath := getenv("SEEDS_POSITIVE_FILE", "/config/seeds_positive.txt")
	negSeedPath := getenv("SEEDS_NEGATIVE_FILE", "/config/seeds_negative.txt")
	thr, _ := strconv.ParseFloat(getenv("SHADOW_THRESHOLD", "0.0"), 64)
	negBias, _ := strconv.ParseFloat(getenv("SHADOW_NEGATIVE_BIAS", "0.05"), 64)
	maxA, _ := strconv.Atoi(getenv("LLM_MAX_ATTEMPTS", "3"))
	if maxA < 1 {
		maxA = 3
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	pool, err := store.New(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	oc := ollama.NewDefault(oll, embed, chat)
	modelCtx, modelCancel := context.WithTimeout(ctx, 20*time.Minute)
	defer modelCancel()
	if err := oc.Pull(modelCtx, embed); err != nil {
		log.Printf("warning: embed model pull failed (%s): %v", embed, err)
	}
	if err := oc.Pull(modelCtx, chat); err != nil {
		log.Printf("warning: chat model pull failed (%s): %v", chat, err)
	}
	proc := &pipeline.Processor{Pool: pool, OC: oc, Opt: pipeline.Options{
		ShadowThreshold:    thr,
		ShadowNegativeBias: negBias,
		PositiveSeedPath:   posSeedPath,
		NegativeSeedPath:   negSeedPath,
		LLMMaxAttempts:     maxA,
	}}
	if err := proc.LoadSeeds(ctx); err != nil {
		log.Printf("warning: seed load failed, continuing without seed embeddings: %v", err)
	}
	log.Printf("seed embeddings loaded: positive=%d negative=%d", len(proc.PositiveSeeds), len(proc.NegativeSeeds))

	ln, err := net.Listen("tcp", listen)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("pipeline listening on %s, ollama %s", listen, oll)

	for {
		c, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Printf("accept: %v", err)
			continue
		}
		go handleConn(ctx, c, proc)
	}
}

func handleConn(ctx context.Context, c net.Conn, proc *pipeline.Processor) {
	defer c.Close()
	for {
		var job model.PipelineWork
		if err := wire.ReadFrame(c, &job); err != nil {
			return
		}
		if err := proc.Process(ctx, job.PaperID); err != nil {
			log.Printf("process paper %d: %v", job.PaperID, err)
		}
	}
}

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
