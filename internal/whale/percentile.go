package whale

import (
	"sort"
	"sync"
)

const (
	// Number of samples to collect before computing bootstrap thresholds
	bootstrapSampleCount = 50000
)

// Thresholds holds the percentile thresholds for whale detection
type Thresholds struct {
	P99  float64 // Top 1% threshold
	P999 float64 // Top 0.1% threshold
}

// DefaultThresholds returns default thresholds when no data is available
// These are conservative fallbacks, bootstrap thresholds are preferred
func DefaultThresholds() Thresholds {
	return Thresholds{
		P99:  5.0,  // 5 BTC (fallback)
		P999: 20.0, // 20 BTC (fallback)
	}
}

// RollingPercentile maintains a sliding window of daily samples
// for calculating rolling percentile thresholds
type RollingPercentile struct {
	windowDays    int
	samplesPerDay int

	mu           sync.Mutex
	dailySamples map[string]*Reservoir // date -> reservoir
	dateOrder    []string              // Ordered list of dates

	// Bootstrap: compute initial thresholds from first N trades
	bootstrapBuffer     []float64
	bootstrapThresholds Thresholds
	bootstrapped        bool
}

// NewRollingPercentile creates a new rolling percentile calculator
func NewRollingPercentile(windowDays, samplesPerDay int) *RollingPercentile {
	return &RollingPercentile{
		windowDays:      windowDays,
		samplesPerDay:   samplesPerDay,
		dailySamples:    make(map[string]*Reservoir),
		dateOrder:       make([]string, 0),
		bootstrapBuffer: make([]float64, 0, bootstrapSampleCount),
		bootstrapped:    false,
	}
}

// AddSample adds a trade quantity sample for a given date
func (rp *RollingPercentile) AddSample(date string, qty float64) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Bootstrap phase: collect first N samples to compute initial thresholds
	if !rp.bootstrapped {
		rp.bootstrapBuffer = append(rp.bootstrapBuffer, qty)
		if len(rp.bootstrapBuffer) >= bootstrapSampleCount {
			rp.computeBootstrapThresholds()
		}
	}

	reservoir, exists := rp.dailySamples[date]
	if !exists {
		reservoir = NewReservoir(rp.samplesPerDay)
		rp.dailySamples[date] = reservoir
		rp.dateOrder = append(rp.dateOrder, date)
		// Keep dates sorted
		sort.Strings(rp.dateOrder)
	}

	reservoir.Add(qty)
}

// computeBootstrapThresholds calculates initial thresholds from bootstrap samples
func (rp *RollingPercentile) computeBootstrapThresholds() {
	sorted := make([]float64, len(rp.bootstrapBuffer))
	copy(sorted, rp.bootstrapBuffer)
	sort.Float64s(sorted)

	rp.bootstrapThresholds = Thresholds{
		P99:  percentile(sorted, 99),
		P999: percentile(sorted, 99.9),
	}
	rp.bootstrapped = true
	rp.bootstrapBuffer = nil // Free memory
}

// GetThresholds calculates P99 and P99.9 thresholds for a given date
// Uses samples from the previous windowDays days
func (rp *RollingPercentile) GetThresholds(date string) Thresholds {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Find dates in the window (before the given date)
	var windowDates []string
	for _, d := range rp.dateOrder {
		if d >= date {
			break
		}
		windowDates = append(windowDates, d)
	}

	// Take only the last windowDays dates
	if len(windowDates) > rp.windowDays {
		windowDates = windowDates[len(windowDates)-rp.windowDays:]
	}

	// If no historical data, use bootstrap thresholds (preferred) or defaults
	if len(windowDates) == 0 {
		if rp.bootstrapped {
			return rp.bootstrapThresholds
		}
		return DefaultThresholds()
	}

	// Collect all samples from the window
	var allSamples []float64
	for _, d := range windowDates {
		if reservoir, ok := rp.dailySamples[d]; ok {
			allSamples = append(allSamples, reservoir.Samples()...)
		}
	}

	if len(allSamples) == 0 {
		if rp.bootstrapped {
			return rp.bootstrapThresholds
		}
		return DefaultThresholds()
	}

	// Sort for percentile calculation
	sort.Float64s(allSamples)

	return Thresholds{
		P99:  percentile(allSamples, 99),
		P999: percentile(allSamples, 99.9),
	}
}

// HasEnoughData returns true if we have at least windowDays of data before the given date
func (rp *RollingPercentile) HasEnoughData(date string) bool {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	count := 0
	for _, d := range rp.dateOrder {
		if d >= date {
			break
		}
		count++
	}
	return count >= rp.windowDays
}

// PruneOldDates removes dates older than the window to save memory
func (rp *RollingPercentile) PruneOldDates(currentDate string) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	// Find cutoff index
	cutoff := 0
	for i, d := range rp.dateOrder {
		if d >= currentDate {
			break
		}
		// Keep windowDays + 1 buffer
		if len(rp.dateOrder)-i <= rp.windowDays+1 {
			break
		}
		cutoff = i + 1
	}

	// Remove old dates
	for i := 0; i < cutoff; i++ {
		delete(rp.dailySamples, rp.dateOrder[i])
	}
	rp.dateOrder = rp.dateOrder[cutoff:]
}

// GetStats returns statistics about the rolling percentile state
func (rp *RollingPercentile) GetStats() (numDates int, totalSamples int) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	numDates = len(rp.dateOrder)
	for _, reservoir := range rp.dailySamples {
		totalSamples += reservoir.Size()
	}
	return
}

// percentile calculates the p-th percentile of a sorted slice
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}

	// Linear interpolation method
	rank := p / 100 * float64(len(sorted)-1)
	lower := int(rank)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}

	frac := rank - float64(lower)
	return sorted[lower] + frac*(sorted[upper]-sorted[lower])
}

// ReservoirState holds serializable reservoir data.
type ReservoirState struct {
	Samples []float64
	Count   int64
}

// BootstrapState holds serializable bootstrap data.
type BootstrapState struct {
	Thresholds   Thresholds
	Bootstrapped bool
}

// ExportState exports the rolling percentile state for serialization.
func (rp *RollingPercentile) ExportState() (windowDays, samplesPerDay int, reservoirs map[string]ReservoirState, dateOrder []string) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	reservoirs = make(map[string]ReservoirState, len(rp.dailySamples))
	for date, reservoir := range rp.dailySamples {
		samples, count := reservoir.ExportState()
		reservoirs[date] = ReservoirState{Samples: samples, Count: count}
	}

	dateOrder = make([]string, len(rp.dateOrder))
	copy(dateOrder, rp.dateOrder)

	return rp.windowDays, rp.samplesPerDay, reservoirs, dateOrder
}

// ExportBootstrap exports bootstrap state for serialization.
func (rp *RollingPercentile) ExportBootstrap() BootstrapState {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	return BootstrapState{
		Thresholds:   rp.bootstrapThresholds,
		Bootstrapped: rp.bootstrapped,
	}
}

// ImportState restores the rolling percentile state from serialization.
func (rp *RollingPercentile) ImportState(reservoirs map[string]ReservoirState, dateOrder []string) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.dailySamples = make(map[string]*Reservoir, len(reservoirs))
	for date, rs := range reservoirs {
		rp.dailySamples[date] = NewReservoirFromSamples(rp.samplesPerDay, rs.Samples, rs.Count)
	}

	rp.dateOrder = make([]string, len(dateOrder))
	copy(rp.dateOrder, dateOrder)
}

// ImportBootstrap restores bootstrap state from serialization.
func (rp *RollingPercentile) ImportBootstrap(state BootstrapState) {
	rp.mu.Lock()
	defer rp.mu.Unlock()

	rp.bootstrapThresholds = state.Thresholds
	rp.bootstrapped = state.Bootstrapped
	if rp.bootstrapped {
		rp.bootstrapBuffer = nil // Don't need buffer if already bootstrapped
	}
}
