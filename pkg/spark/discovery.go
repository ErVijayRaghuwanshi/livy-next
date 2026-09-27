package spark

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// DiscoveredSession represents an active or finished Spark Connect session
// discovered dynamically from the Spark Connect Web UI.
type DiscoveredSession struct {
	SessionID    string
	User         string
	StartTime    string
	FinishTime   string
	Duration     string
	TotalExecute int
	IsActive     bool
}

var (
	sessionTableRegex = regexp.MustCompile(`(?is)<table[^>]*id=["']sessionstat["'][^>]*>(.*?)</table>`)
	sessionTbodyRegex = regexp.MustCompile(`(?is)<tbody>(.*?)</tbody>`)
	sessionRowRegex   = regexp.MustCompile(`(?is)<tr>\s*<td>(.*?)</td>\s*<td>\s*<a\s+href=["']/connect/session/\?id=([^"'\s>]+)["'].*?</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*<td>(.*?)</td>\s*</tr>`)
	htmlTagRegex      = regexp.MustCompile(`<[^>]+>`)
)

// ParseSessionStatHTML extracts sessions from the Spark Connect Web UI HTML page (/connect/).
func ParseSessionStatHTML(html string) []DiscoveredSession {
	tableMatch := sessionTableRegex.FindStringSubmatch(html)
	if len(tableMatch) < 2 {
		return nil
	}

	tbodyMatch := sessionTbodyRegex.FindStringSubmatch(tableMatch[1])
	content := tableMatch[1]
	if len(tbodyMatch) >= 2 {
		content = tbodyMatch[1]
	}

	matches := sessionRowRegex.FindAllStringSubmatch(content, -1)
	if len(matches) == 0 {
		return nil
	}

	var results []DiscoveredSession
	for _, m := range matches {
		if len(m) < 7 {
			continue
		}
		rawUser := strings.TrimSpace(htmlTagRegex.ReplaceAllString(m[1], ""))
		if strings.EqualFold(rawUser, "na") {
			rawUser = ""
		}
		sessionID := strings.TrimSpace(m[2])
		startTime := strings.TrimSpace(htmlTagRegex.ReplaceAllString(m[3], ""))
		finishTime := strings.TrimSpace(htmlTagRegex.ReplaceAllString(m[4], ""))
		duration := strings.TrimSpace(htmlTagRegex.ReplaceAllString(m[5], ""))
		totalExecStr := strings.TrimSpace(htmlTagRegex.ReplaceAllString(m[6], ""))
		totalExec, _ := strconv.Atoi(totalExecStr)

		results = append(results, DiscoveredSession{
			SessionID:    sessionID,
			User:         rawUser,
			StartTime:    startTime,
			FinishTime:   finishTime,
			Duration:     duration,
			TotalExecute: totalExec,
			IsActive:     finishTime == "",
		})
	}

	return results
}

// DiscoverActiveSessions probes the Spark Connect Web UI (/connect/) and extracts all active sessions.
func DiscoverActiveSessions(ctx context.Context, sparkUIUrl string) ([]DiscoveredSession, error) {
	if sparkUIUrl == "" {
		return nil, fmt.Errorf("spark UI URL is empty")
	}

	endpoint := strings.TrimRight(sparkUIUrl, "/") + "/connect/"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create discovery request: %w", err)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch spark connect UI at %s: %w", endpoint, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("spark connect UI returned status %d from %s", resp.StatusCode, endpoint)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response from %s: %w", endpoint, err)
	}

	allSessions := ParseSessionStatHTML(string(body))
	var activeSessions []DiscoveredSession
	for _, s := range allSessions {
		if s.IsActive {
			activeSessions = append(activeSessions, s)
		}
	}

	return activeSessions, nil
}
