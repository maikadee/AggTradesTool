package aggregator

import (
	"math"
)

// HourlyBar represents aggregated data for one hour
type HourlyBar struct {
	// Basic volumes
	BuyVol  float64
	SellVol float64

	// Trade counts
	NTrades   int64
	BuyCount  int64
	SellCount int64

	// Whale volumes (P99 threshold)
	WhaleBuyVolP99   float64
	WhaleSellVolP99  float64
	WhaleBuyCountP99 int64
	WhaleSellCountP99 int64

	// Mega-whale volumes (P99.9 threshold)
	WhaleBuyVolP999   float64
	WhaleSellVolP999  float64
	WhaleBuyCountP999 int64
	WhaleSellCountP999 int64

	// Intra-hour distribution
	VolFirst30Min float64
	VolLast30Min  float64

	// USD volumes
	BuyVolUSD  float64
	SellVolUSD float64

	// Price statistics
	MaxTradeSize float64
	VWAP         float64
	PriceStd     float64

	// Internal accumulators for VWAP/std calculation
	sumPriceQty   float64
	sumQty        float64
	sumPriceSqQty float64
}

// NewHourlyBar creates a new hourly bar
func NewHourlyBar() *HourlyBar {
	return &HourlyBar{}
}

// AddTrade adds a trade to the hourly bar
// isWhaleP99 and isWhaleP999 indicate whale classification
func (h *HourlyBar) AddTrade(
	price, qty float64,
	isBuy bool,
	minute int,
	isWhaleP99, isWhaleP999 bool,
) {
	dollarVol := price * qty

	// Basic volumes
	if isBuy {
		h.BuyVol += qty
		h.BuyCount++
		h.BuyVolUSD += dollarVol
	} else {
		h.SellVol += qty
		h.SellCount++
		h.SellVolUSD += dollarVol
	}

	h.NTrades++

	// Whale classification
	if isWhaleP99 {
		if isBuy {
			h.WhaleBuyVolP99 += qty
			h.WhaleBuyCountP99++
		} else {
			h.WhaleSellVolP99 += qty
			h.WhaleSellCountP99++
		}
	}

	if isWhaleP999 {
		if isBuy {
			h.WhaleBuyVolP999 += qty
			h.WhaleBuyCountP999++
		} else {
			h.WhaleSellVolP999 += qty
			h.WhaleSellCountP999++
		}
	}

	// Intra-hour distribution
	if minute < 30 {
		h.VolFirst30Min += qty
	} else {
		h.VolLast30Min += qty
	}

	// Max trade size
	if qty > h.MaxTradeSize {
		h.MaxTradeSize = qty
	}

	// Accumulators for VWAP and price std
	h.sumPriceQty += price * qty
	h.sumQty += qty
	h.sumPriceSqQty += price * price * qty
}

// Finalize calculates derived statistics (VWAP, PriceStd)
// Must be called after all trades are added
func (h *HourlyBar) Finalize() {
	if h.sumQty > 0 {
		h.VWAP = h.sumPriceQty / h.sumQty

		// Variance = E[X²] - E[X]²
		variance := (h.sumPriceSqQty / h.sumQty) - (h.VWAP * h.VWAP)
		if variance < 0 {
			variance = 0 // Numerical stability
		}
		h.PriceStd = math.Sqrt(variance)
	}
}

// Merge combines another hourly bar into this one
func (h *HourlyBar) Merge(other *HourlyBar) {
	h.BuyVol += other.BuyVol
	h.SellVol += other.SellVol
	h.NTrades += other.NTrades
	h.BuyCount += other.BuyCount
	h.SellCount += other.SellCount

	h.WhaleBuyVolP99 += other.WhaleBuyVolP99
	h.WhaleSellVolP99 += other.WhaleSellVolP99
	h.WhaleBuyCountP99 += other.WhaleBuyCountP99
	h.WhaleSellCountP99 += other.WhaleSellCountP99

	h.WhaleBuyVolP999 += other.WhaleBuyVolP999
	h.WhaleSellVolP999 += other.WhaleSellVolP999
	h.WhaleBuyCountP999 += other.WhaleBuyCountP999
	h.WhaleSellCountP999 += other.WhaleSellCountP999

	h.VolFirst30Min += other.VolFirst30Min
	h.VolLast30Min += other.VolLast30Min

	h.BuyVolUSD += other.BuyVolUSD
	h.SellVolUSD += other.SellVolUSD

	if other.MaxTradeSize > h.MaxTradeSize {
		h.MaxTradeSize = other.MaxTradeSize
	}

	h.sumPriceQty += other.sumPriceQty
	h.sumQty += other.sumQty
	h.sumPriceSqQty += other.sumPriceSqQty
}

// TotalVolume returns buy + sell volume
func (h *HourlyBar) TotalVolume() float64 {
	return h.BuyVol + h.SellVol
}

// NetVolume returns buy - sell volume
func (h *HourlyBar) NetVolume() float64 {
	return h.BuyVol - h.SellVol
}

// Clone creates a deep copy of the hourly bar
func (h *HourlyBar) Clone() *HourlyBar {
	clone := *h
	return &clone
}
