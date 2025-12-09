package output

import (
	"fmt"
	"sort"
	"time"

	"github.com/clement/aggtrades/internal/aggregator"
	"github.com/clement/aggtrades/internal/config"
)

// ValidationResult holds the results of data validation
type ValidationResult struct {
	Valid           bool
	TotalHours      int
	ExpectedHours   int
	MissingHours    int
	DuplicateHours  int
	LowActivityHours int
	Gaps            []Gap
	Issues          []string
}

// Gap represents a gap in the data
type Gap struct {
	Start    time.Time
	End      time.Time
	Duration time.Duration
}

// Validate checks the hourly data for issues
func Validate(bars []aggregator.HourlyResult) ValidationResult {
	result := ValidationResult{
		Valid:  true,
		Issues: make([]string, 0),
		Gaps:   make([]Gap, 0),
	}

	if len(bars) == 0 {
		result.Valid = false
		result.Issues = append(result.Issues, "No data")
		return result
	}

	// Sort by time
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Time.Before(bars[j].Time)
	})

	result.TotalHours = len(bars)

	// Check for duplicates and build time set
	timeSet := make(map[time.Time]int)
	for _, bar := range bars {
		timeSet[bar.Time]++
		if timeSet[bar.Time] > 1 {
			result.DuplicateHours++
		}
	}

	if result.DuplicateHours > 0 {
		result.Valid = false
		result.Issues = append(result.Issues, fmt.Sprintf("%d duplicate hours", result.DuplicateHours))
	}

	// Check expected hours
	firstTime := bars[0].Time
	lastTime := bars[len(bars)-1].Time
	result.ExpectedHours = int(lastTime.Sub(firstTime).Hours()) + 1
	result.MissingHours = result.ExpectedHours - len(timeSet)

	// Find gaps
	var prevTime time.Time
	for i, bar := range bars {
		if i == 0 {
			prevTime = bar.Time
			continue
		}

		expectedTime := prevTime.Add(time.Hour)
		if !bar.Time.Equal(expectedTime) {
			gap := Gap{
				Start:    expectedTime,
				End:      bar.Time.Add(-time.Hour),
				Duration: bar.Time.Sub(expectedTime),
			}
			result.Gaps = append(result.Gaps, gap)
		}

		prevTime = bar.Time
	}

	if len(result.Gaps) > 0 {
		result.Valid = false
		result.Issues = append(result.Issues, fmt.Sprintf("%d gaps (%d missing hours)", len(result.Gaps), result.MissingHours))
	}

	// Check for low activity
	for _, bar := range bars {
		if bar.Bar.NTrades < config.MinTradesPerHour {
			result.LowActivityHours++
		}
	}

	if result.LowActivityHours > 0 {
		result.Issues = append(result.Issues, fmt.Sprintf("%d hours with low activity (<%d trades)", result.LowActivityHours, config.MinTradesPerHour))
	}

	return result
}

// FillGaps fills small gaps in the data using linear interpolation
func FillGaps(bars []aggregator.HourlyResult, maxGapHours int) ([]aggregator.HourlyResult, int, int) {
	if len(bars) == 0 {
		return bars, 0, 0
	}

	// Sort by time
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Time.Before(bars[j].Time)
	})

	// Build map for quick lookup
	barMap := make(map[time.Time]*aggregator.HourlyBar)
	for _, bar := range bars {
		barMap[bar.Time] = bar.Bar
	}

	// Find gaps and fill
	result := make([]aggregator.HourlyResult, 0, len(bars))
	var smallGapsFilled, largeGapsNaN int

	firstTime := bars[0].Time
	lastTime := bars[len(bars)-1].Time

	for t := firstTime; !t.After(lastTime); t = t.Add(time.Hour) {
		if bar, exists := barMap[t]; exists {
			result = append(result, aggregator.HourlyResult{Time: t, Bar: bar})
		} else {
			// Missing hour - check gap size
			gapStart := t
			gapEnd := t
			for {
				nextHour := gapEnd.Add(time.Hour)
				if nextHour.After(lastTime) {
					break
				}
				if _, exists := barMap[nextHour]; exists {
					break
				}
				gapEnd = nextHour
			}

			gapSize := int(gapEnd.Sub(gapStart).Hours()) + 1

			if gapSize <= maxGapHours {
				// Small gap - interpolate
				interpolated := interpolateGap(barMap, gapStart, gapSize)
				for i, bar := range interpolated {
					result = append(result, aggregator.HourlyResult{
						Time: gapStart.Add(time.Duration(i) * time.Hour),
						Bar:  bar,
					})
					smallGapsFilled++
				}
			} else {
				// Large gap - fill with NaN (zeros)
				for i := 0; i < gapSize; i++ {
					result = append(result, aggregator.HourlyResult{
						Time: gapStart.Add(time.Duration(i) * time.Hour),
						Bar:  aggregator.NewHourlyBar(),
					})
					largeGapsNaN++
				}
			}
		}
	}

	return result, smallGapsFilled, largeGapsNaN
}

// interpolateGap creates interpolated bars for a gap
func interpolateGap(barMap map[time.Time]*aggregator.HourlyBar, gapStart time.Time, gapSize int) []*aggregator.HourlyBar {
	prevTime := gapStart.Add(-time.Hour)
	nextTime := gapStart.Add(time.Duration(gapSize) * time.Hour)

	prevBar := barMap[prevTime]
	nextBar := barMap[nextTime]

	if prevBar == nil || nextBar == nil {
		// Can't interpolate, return zeros
		result := make([]*aggregator.HourlyBar, gapSize)
		for i := range result {
			result[i] = aggregator.NewHourlyBar()
		}
		return result
	}

	result := make([]*aggregator.HourlyBar, gapSize)
	for i := 0; i < gapSize; i++ {
		weight := float64(i+1) / float64(gapSize+1)
		result[i] = interpolateBars(prevBar, nextBar, weight)
	}

	return result
}

// interpolateBars creates an interpolated bar between two bars
func interpolateBars(prev, next *aggregator.HourlyBar, weight float64) *aggregator.HourlyBar {
	lerp := func(a, b float64) float64 {
		return a + weight*(b-a)
	}
	lerpInt := func(a, b int64) int64 {
		return int64(lerp(float64(a), float64(b)))
	}

	bar := aggregator.NewHourlyBar()
	bar.BuyVol = lerp(prev.BuyVol, next.BuyVol)
	bar.SellVol = lerp(prev.SellVol, next.SellVol)
	bar.NTrades = lerpInt(prev.NTrades, next.NTrades)
	bar.BuyCount = lerpInt(prev.BuyCount, next.BuyCount)
	bar.SellCount = lerpInt(prev.SellCount, next.SellCount)
	bar.WhaleBuyVolP99 = lerp(prev.WhaleBuyVolP99, next.WhaleBuyVolP99)
	bar.WhaleSellVolP99 = lerp(prev.WhaleSellVolP99, next.WhaleSellVolP99)
	bar.WhaleBuyCountP99 = lerpInt(prev.WhaleBuyCountP99, next.WhaleBuyCountP99)
	bar.WhaleSellCountP99 = lerpInt(prev.WhaleSellCountP99, next.WhaleSellCountP99)
	bar.WhaleBuyVolP999 = lerp(prev.WhaleBuyVolP999, next.WhaleBuyVolP999)
	bar.WhaleSellVolP999 = lerp(prev.WhaleSellVolP999, next.WhaleSellVolP999)
	bar.WhaleBuyCountP999 = lerpInt(prev.WhaleBuyCountP999, next.WhaleBuyCountP999)
	bar.WhaleSellCountP999 = lerpInt(prev.WhaleSellCountP999, next.WhaleSellCountP999)
	bar.VolFirst30Min = lerp(prev.VolFirst30Min, next.VolFirst30Min)
	bar.VolLast30Min = lerp(prev.VolLast30Min, next.VolLast30Min)
	bar.BuyVolUSD = lerp(prev.BuyVolUSD, next.BuyVolUSD)
	bar.SellVolUSD = lerp(prev.SellVolUSD, next.SellVolUSD)
	bar.MaxTradeSize = lerp(prev.MaxTradeSize, next.MaxTradeSize)
	bar.VWAP = lerp(prev.VWAP, next.VWAP)
	bar.PriceStd = lerp(prev.PriceStd, next.PriceStd)

	return bar
}

// TrimWarmup removes the warmup period from the beginning of the data
func TrimWarmup(bars []aggregator.HourlyResult, warmupHours int) []aggregator.HourlyResult {
	if len(bars) <= warmupHours {
		return bars
	}

	// Sort first
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Time.Before(bars[j].Time)
	})

	return bars[warmupHours:]
}

// Summary prints a summary of the data
func Summary(bars []aggregator.HourlyResult) string {
	if len(bars) == 0 {
		return "No data"
	}

	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Time.Before(bars[j].Time)
	})

	var totalTrades int64
	var totalVolume float64

	for _, bar := range bars {
		totalTrades += bar.Bar.NTrades
		totalVolume += bar.Bar.TotalVolume()
	}

	return fmt.Sprintf(
		"Period: %s to %s\nHours: %d\nTrades: %d\nVolume: %.2f BTC",
		bars[0].Time.Format("2006-01-02 15:04"),
		bars[len(bars)-1].Time.Format("2006-01-02 15:04"),
		len(bars),
		totalTrades,
		totalVolume,
	)
}
