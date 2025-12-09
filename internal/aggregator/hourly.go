package aggregator

import (
	"sort"
	"sync"
	"time"

	"github.com/clement/aggtrades/internal/parser"
	"github.com/clement/aggtrades/internal/whale"
)

// HourlyResult represents a finalized hourly bar with timestamp
type HourlyResult struct {
	Time time.Time
	Bar  *HourlyBar
}

// Aggregator aggregates trades into hourly bars with whale detection
type Aggregator struct {
	detector *whale.Detector
	windowDays int

	mu            sync.Mutex
	pendingBars   map[time.Time]*HourlyBar // Hours waiting for whale classification
	finalizedBars []HourlyResult            // Completed hourly bars

	// Track dates for warmup detection
	firstDate string
	lastDate  string
}

// NewAggregator creates a new hourly aggregator
func NewAggregator(windowDays, samplesPerDay int) *Aggregator {
	return &Aggregator{
		detector:      whale.NewDetector(windowDays, samplesPerDay),
		windowDays:    windowDays,
		pendingBars:   make(map[time.Time]*HourlyBar),
		finalizedBars: make([]HourlyResult, 0),
	}
}

// ProcessTrades processes a batch of trades
// Note: Does NOT finalize bars during processing to avoid duplicates.
// Call Flush() or GetBarsForMonth() when ready to finalize.
func (a *Aggregator) ProcessTrades(trades []parser.Trade) {
	if len(trades) == 0 {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// First pass: add samples to whale detector to build percentile distribution
	for _, trade := range trades {
		date := trade.Date()
		a.detector.AddSample(date, trade.Qty)

		// Track date range
		if a.firstDate == "" || date < a.firstDate {
			a.firstDate = date
		}
		if date > a.lastDate {
			a.lastDate = date
		}
	}

	// Second pass: classify trades and aggregate into hourly bars
	// Now that all samples are added, thresholds are up-to-date
	for _, trade := range trades {
		// Classify trade using rolling percentile thresholds
		classification := a.detector.Classify(trade)

		// Get or create hourly bar
		hour := trade.Hour()
		bar, exists := a.pendingBars[hour]
		if !exists {
			bar = NewHourlyBar()
			a.pendingBars[hour] = bar
		}

		// Add trade with proper whale classification
		bar.AddTrade(
			trade.Price,
			trade.Qty,
			trade.IsBuy(),
			trade.Minute(),
			classification.IsWhaleP99,
			classification.IsWhaleP999,
		)
	}

	// NOTE: We deliberately don't call tryFinalizePending() here.
	// Finalizing during batch processing causes duplicates when more trades
	// for the same hour arrive in later batches.
	// Bars are finalized in GetBarsForMonth() at checkpoint time.
}


// ProcessTradesWithClassification processes trades with immediate whale classification
// Use this for the second pass when thresholds are already known
func (a *Aggregator) ProcessTradesWithClassification(trades []parser.Trade) {
	if len(trades) == 0 {
		return
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	// Classify all trades
	classifications := a.detector.ClassifyBatch(trades)

	for i, trade := range trades {
		hour := trade.Hour()
		bar, exists := a.pendingBars[hour]
		if !exists {
			bar = NewHourlyBar()
			a.pendingBars[hour] = bar
		}

		bar.AddTrade(
			trade.Price,
			trade.Qty,
			trade.IsBuy(),
			trade.Minute(),
			classifications[i].IsWhaleP99,
			classifications[i].IsWhaleP999,
		)
	}
}

// Flush forces finalization of all remaining pending bars
// Use at the end of processing
func (a *Aggregator) Flush() {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Sort pending hours
	hours := make([]time.Time, 0, len(a.pendingBars))
	for h := range a.pendingBars {
		hours = append(hours, h)
	}
	sort.Slice(hours, func(i, j int) bool {
		return hours[i].Before(hours[j])
	})

	// Finalize all remaining
	for _, hour := range hours {
		bar := a.pendingBars[hour]
		bar.Finalize()

		a.finalizedBars = append(a.finalizedBars, HourlyResult{
			Time: hour,
			Bar:  bar,
		})
	}

	a.pendingBars = make(map[time.Time]*HourlyBar)
}

// GetFinalizedBars returns all finalized bars and clears the internal buffer
func (a *Aggregator) GetFinalizedBars() []HourlyResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	result := a.finalizedBars
	a.finalizedBars = make([]HourlyResult, 0)
	return result
}

// GetAllBars returns all bars (pending + finalized) sorted by time
// Use this at the end to get the complete dataset
func (a *Aggregator) GetAllBars() []HourlyResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	// Collect all bars
	all := make([]HourlyResult, 0, len(a.finalizedBars)+len(a.pendingBars))
	all = append(all, a.finalizedBars...)

	for hour, bar := range a.pendingBars {
		bar.Finalize()
		all = append(all, HourlyResult{
			Time: hour,
			Bar:  bar,
		})
	}

	// Sort by time
	sort.Slice(all, func(i, j int) bool {
		return all[i].Time.Before(all[j].Time)
	})

	return all
}

// Stats returns statistics about the aggregator state
func (a *Aggregator) Stats() (pending, finalized int, dateRange string) {
	a.mu.Lock()
	defer a.mu.Unlock()

	pending = len(a.pendingBars)
	finalized = len(a.finalizedBars)
	if a.firstDate != "" && a.lastDate != "" {
		dateRange = a.firstDate + " → " + a.lastDate
	}
	return
}

// TotalTrades returns the total number of trades processed
func (a *Aggregator) TotalTrades() int64 {
	a.mu.Lock()
	defer a.mu.Unlock()

	var total int64
	for _, bar := range a.pendingBars {
		total += bar.NTrades
	}
	for _, result := range a.finalizedBars {
		total += result.Bar.NTrades
	}
	return total
}

// Detector returns the whale detector for state export/import.
func (a *Aggregator) Detector() *whale.Detector {
	return a.detector
}

// GetBarsForMonth returns all bars (pending + finalized) for a specific month.
// Pending bars are finalized before being returned. Returned bars are removed from internal state.
func (a *Aggregator) GetBarsForMonth(month string) []HourlyResult {
	a.mu.Lock()
	defer a.mu.Unlock()

	var result []HourlyResult

	// Collect from finalizedBars
	remaining := make([]HourlyResult, 0, len(a.finalizedBars))
	for _, hr := range a.finalizedBars {
		if hr.Time.Format("2006-01") == month {
			result = append(result, hr)
		} else {
			remaining = append(remaining, hr)
		}
	}
	a.finalizedBars = remaining

	// Collect from pendingBars
	for hour, bar := range a.pendingBars {
		if hour.Format("2006-01") == month {
			bar.Finalize()
			result = append(result, HourlyResult{
				Time: hour,
				Bar:  bar,
			})
			delete(a.pendingBars, hour)
		}
	}

	// Sort by time
	sort.Slice(result, func(i, j int) bool {
		return result[i].Time.Before(result[j].Time)
	})

	return result
}
