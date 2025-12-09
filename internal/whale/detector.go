package whale

import (
	"sync"

	"github.com/clement/aggtrades/internal/parser"
)

// Classification holds whale classification for a trade
type Classification struct {
	IsWhaleP99  bool // Trade qty >= P99 threshold
	IsWhaleP999 bool // Trade qty >= P99.9 threshold
}

// Detector classifies trades as whale/non-whale based on rolling percentiles
type Detector struct {
	rolling *RollingPercentile

	// Cache for thresholds by date (avoid recalculating)
	cacheMu        sync.RWMutex
	thresholdCache map[string]Thresholds
}

// NewDetector creates a new whale detector
func NewDetector(windowDays, samplesPerDay int) *Detector {
	return &Detector{
		rolling:        NewRollingPercentile(windowDays, samplesPerDay),
		thresholdCache: make(map[string]Thresholds),
	}
}

// AddSample adds a trade quantity to the rolling percentile calculation
func (d *Detector) AddSample(date string, qty float64) {
	d.rolling.AddSample(date, qty)
	// Invalidate cache for this date
	d.cacheMu.Lock()
	delete(d.thresholdCache, date)
	d.cacheMu.Unlock()
}

// Classify returns the whale classification for a trade
func (d *Detector) Classify(trade parser.Trade) Classification {
	date := trade.Date()
	thresholds := d.GetThresholds(date)

	return Classification{
		IsWhaleP99:  trade.Qty >= thresholds.P99,
		IsWhaleP999: trade.Qty >= thresholds.P999,
	}
}

// GetThresholds returns the thresholds for a given date
func (d *Detector) GetThresholds(date string) Thresholds {
	// Fast path: check cache with read lock
	d.cacheMu.RLock()
	if t, ok := d.thresholdCache[date]; ok {
		d.cacheMu.RUnlock()
		return t
	}
	d.cacheMu.RUnlock()

	// Slow path: calculate and cache with write lock
	t := d.rolling.GetThresholds(date)

	d.cacheMu.Lock()
	d.thresholdCache[date] = t
	d.cacheMu.Unlock()

	return t
}

// HasEnoughData returns true if we have enough historical data for the date
func (d *Detector) HasEnoughData(date string) bool {
	return d.rolling.HasEnoughData(date)
}

// PruneOldDates removes old data to save memory
func (d *Detector) PruneOldDates(currentDate string) {
	d.rolling.PruneOldDates(currentDate)

	// Also clean threshold cache
	d.cacheMu.Lock()
	for date := range d.thresholdCache {
		if date < currentDate {
			delete(d.thresholdCache, date)
		}
	}
	d.cacheMu.Unlock()
}

// GetStats returns statistics about the detector state
func (d *Detector) GetStats() (numDates int, totalSamples int) {
	return d.rolling.GetStats()
}

// ClassifyBatch classifies multiple trades efficiently
func (d *Detector) ClassifyBatch(trades []parser.Trade) []Classification {
	results := make([]Classification, len(trades))

	// Group by date to minimize threshold lookups
	var currentDate string
	var currentThresholds Thresholds

	for i, trade := range trades {
		date := trade.Date()
		if date != currentDate {
			currentDate = date
			currentThresholds = d.GetThresholds(date)
		}

		results[i] = Classification{
			IsWhaleP99:  trade.Qty >= currentThresholds.P99,
			IsWhaleP999: trade.Qty >= currentThresholds.P999,
		}
	}

	return results
}

// ExportState exports detector state for serialization.
func (d *Detector) ExportState() (windowDays, samplesPerDay int, reservoirs map[string]ReservoirState, dateOrder []string) {
	return d.rolling.ExportState()
}

// ExportBootstrap exports bootstrap state for serialization.
func (d *Detector) ExportBootstrap() BootstrapState {
	return d.rolling.ExportBootstrap()
}

// ImportState restores detector state from serialization.
func (d *Detector) ImportState(reservoirs map[string]ReservoirState, dateOrder []string) {
	d.rolling.ImportState(reservoirs, dateOrder)
	d.cacheMu.Lock()
	d.thresholdCache = make(map[string]Thresholds)
	d.cacheMu.Unlock()
}

// ImportBootstrap restores bootstrap state from serialization.
func (d *Detector) ImportBootstrap(state BootstrapState) {
	d.rolling.ImportBootstrap(state)
}
