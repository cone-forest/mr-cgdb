package pdfx

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"rsc.io/pdf"
)

type TooLargeError struct {
	Limit int64
}

func (e *TooLargeError) Error() string {
	return fmt.Sprintf("pdf too large (>%d bytes)", e.Limit)
}

// FetchAndExtractText downloads a PDF and extracts plain text up to maxChars.
func FetchAndExtractText(ctx context.Context, url string, maxBytes int64, maxChars int) (text string, truncated bool, err error) {
	defer func() {
		if r := recover(); r != nil {
			text = ""
			truncated = false
			err = fmt.Errorf("pdf parser panic: %v", r)
		}
	}()
	if maxBytes <= 0 {
		maxBytes = 25 << 20 // 25 MiB
	}
	if maxChars <= 0 {
		maxChars = 120000
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return "", false, err
	}
	req.Header.Set("User-Agent", "mr-cgdb/1.0 deep-verify")
	cl := &http.Client{Timeout: 2 * time.Minute}
	resp, err := cl.Do(req)
	if err != nil {
		return "", false, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return "", false, fmt.Errorf("pdf download: %s", resp.Status)
	}
	if resp.ContentLength > maxBytes && resp.ContentLength > 0 {
		return "", false, &TooLargeError{Limit: maxBytes}
	}
	lr := io.LimitReader(resp.Body, maxBytes+1)
	data, err := io.ReadAll(lr)
	if err != nil {
		return "", false, err
	}
	if int64(len(data)) > maxBytes {
		return "", false, &TooLargeError{Limit: maxBytes}
	}
	if !looksLikePDF(data) {
		return "", false, fmt.Errorf("downloaded content is not a PDF")
	}

	reader, err := pdf.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return "", false, err
	}
	var b strings.Builder
	for i := 1; i <= reader.NumPage(); i++ {
		p := reader.Page(i)
		if p.V.IsNull() {
			continue
		}
		var c pdf.Content
		func() {
			defer func() {
				if recover() != nil {
					c = pdf.Content{}
				}
			}()
			c = p.Content()
		}()
		for _, t := range c.Text {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(t.S)
			if b.Len() >= maxChars {
				return b.String()[:maxChars], true, nil
			}
		}
	}
	if strings.TrimSpace(b.String()) == "" {
		return "", false, fmt.Errorf("unable to extract text from PDF")
	}
	return b.String(), false, nil
}

func looksLikePDF(data []byte) bool {
	if len(data) < 5 {
		return false
	}
	return bytes.HasPrefix(data, []byte("%PDF-"))
}
