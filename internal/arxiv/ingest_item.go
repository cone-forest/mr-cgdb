package arxiv

import "mr-cgdb/internal/model"

// IngestItem builds a TCP ingestion payload from search API metadata.
func IngestItem(e *Entry) *model.IngestItem {
	if e == nil {
		return nil
	}
	aid := e.ArxivID
	it := &model.IngestItem{
		Source:   "arxiv",
		ArxivID:  &aid,
		Title:    e.Title,
		Abstract: e.Summary,
		Authors:  e.Authors,
		URL:      "https://arxiv.org/abs/" + e.ArxivID,
	}
	if e.Year != nil {
		it.Year = e.Year
	}
	return it
}
