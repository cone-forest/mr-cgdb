package model

// IngestItem is sent over TCP from watchers through dedup and keyword to pipeline.
// Identity fields are used for deduplication: prefer arXiv id, else DOI, else weak key inputs.
type IngestItem struct {
	Source   string   `json:"source"` // e.g. "arxiv", "rss:myfeed"
	URL      string   `json:"url"`
	ArxivID  *string  `json:"arxiv_id"`
	DOI      *string  `json:"doi"`
	Title    string   `json:"title"`
	Year     *int     `json:"year"`
	Authors  []string `json:"authors"`
	Abstract string   `json:"abstract"`
	// Rss only
	FeedID    string `json:"feed_id"`
	ItemKey   string `json:"item_key"`
	Published string `json:"published"` // RFC3339, optional
}

// PipelineWork is sent from keyword to pipeline.
type PipelineWork struct {
	PaperID int64 `json:"paper_id"`
}
