package arxiv

import (
	"context"
	"fmt"
	"strings"
)

// FetchByIDList loads one API page for a comma-separated id list.
func FetchByIDList(ctx context.Context, ids []string) ([]Entry, error) {
	var all []string
	for _, s := range ids {
		s = strings.TrimSpace(s)
		if s != "" && !strings.HasPrefix(s, "#") {
			all = append(all, s)
		}
	}
	if len(all) == 0 {
		return nil, nil
	}
	const maxN = 200
	if len(all) > maxN {
		all = all[:maxN]
	}
	q := fmt.Sprintf("id_list=%s&max_results=%d", strings.Join(all, ","), len(all))
	return SearchPage(ctx, q)
}
