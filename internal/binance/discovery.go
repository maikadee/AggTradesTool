package binance

import (
	"context"
	"encoding/xml"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"time"
)

const (
	// S3 bucket endpoint for Binance Data Vision
	s3BucketURL = "https://s3-ap-northeast-1.amazonaws.com/data.binance.vision"

	// Maximum keys to request (200 is enough for monthly data spanning 10+ years)
	maxKeys = 200

	// HTTP timeout for S3 API requests
	discoveryTimeout = 30 * time.Second
)

// DateRange represents the available date range for a symbol
type DateRange struct {
	FirstMonth string   // e.g., "2017-08"
	LastMonth  string   // e.g., "2025-11"
	AllMonths  []string // sorted list of all available months
}

// S3 XML response structures
type listBucketResult struct {
	XMLName     xml.Name   `xml:"ListBucketResult"`
	IsTruncated bool       `xml:"IsTruncated"`
	Contents    []s3Object `xml:"Contents"`
}

type s3Object struct {
	Key  string `xml:"Key"`
	Size int64  `xml:"Size"`
}

// DiscoverDateRange queries Binance Data Vision S3 bucket to find
// all available aggTrades months for a given symbol.
func DiscoverDateRange(ctx context.Context, symbol string) (*DateRange, error) {
	prefix := fmt.Sprintf("data/spot/monthly/aggTrades/%s/", symbol)

	// Build S3 ListObjectsV2 URL
	url := fmt.Sprintf("%s?list-type=2&prefix=%s&max-keys=%d",
		s3BucketURL, prefix, maxKeys)

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	client := &http.Client{Timeout: discoveryTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("S3 returned status %d", resp.StatusCode)
	}

	var result listBucketResult
	if err := xml.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("parse XML: %w", err)
	}

	// Extract months from file keys
	months := extractMonths(result.Contents, symbol)
	if len(months) == 0 {
		return nil, fmt.Errorf("no aggTrades data found for symbol %s", symbol)
	}

	// Sort months chronologically
	sort.Strings(months)

	return &DateRange{
		FirstMonth: months[0],
		LastMonth:  months[len(months)-1],
		AllMonths:  months,
	}, nil
}

// extractMonths parses S3 keys to extract YYYY-MM dates
// Keys look like: data/spot/monthly/aggTrades/BTCUSDT/BTCUSDT-aggTrades-2017-08.zip
func extractMonths(objects []s3Object, symbol string) []string {
	// Pattern: {SYMBOL}-aggTrades-{YYYY-MM}.zip
	pattern := regexp.MustCompile(
		fmt.Sprintf(`%s-aggTrades-(\d{4}-\d{2})\.zip$`, regexp.QuoteMeta(symbol)))

	months := make([]string, 0, len(objects)/2) // /2 because of .CHECKSUM files
	seen := make(map[string]bool)

	for _, obj := range objects {
		matches := pattern.FindStringSubmatch(obj.Key)
		if len(matches) == 2 {
			month := matches[1]
			if !seen[month] {
				months = append(months, month)
				seen[month] = true
			}
		}
	}

	return months
}
