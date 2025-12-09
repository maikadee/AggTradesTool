package parser

import "time"

// Trade represents a single aggregated trade from Binance
type Trade struct {
	Time         time.Time
	Price        float64
	Qty          float64
	IsBuyerMaker bool
}

// RawTrade is an intermediate structure for parsing
// Uses int64 for timestamp to avoid allocations
type RawTrade struct {
	AggID        int64
	Price        float64
	Qty          float64
	FirstID      int64
	LastID       int64
	Timestamp    int64 // Milliseconds or microseconds
	IsBuyerMaker bool
}

// ToTrade converts RawTrade to Trade with proper timestamp handling
func (r *RawTrade) ToTrade() Trade {
	var ts time.Time

	// Detect timestamp format: >1e15 means microseconds, otherwise milliseconds
	if r.Timestamp > 1e15 {
		ts = time.UnixMicro(r.Timestamp)
	} else {
		ts = time.UnixMilli(r.Timestamp)
	}

	return Trade{
		Time:         ts,
		Price:        r.Price,
		Qty:          r.Qty,
		IsBuyerMaker: r.IsBuyerMaker,
	}
}

// IsBuy returns true if this trade is a taker buy (not buyer maker)
func (t *Trade) IsBuy() bool {
	return !t.IsBuyerMaker
}

// IsSell returns true if this trade is a taker sell (buyer maker)
func (t *Trade) IsSell() bool {
	return t.IsBuyerMaker
}

// DollarVolume returns the trade value in USD (price * qty)
func (t *Trade) DollarVolume() float64 {
	return t.Price * t.Qty
}

// Hour returns the hour timestamp (floored)
func (t *Trade) Hour() time.Time {
	return t.Time.Truncate(time.Hour)
}

// Date returns the date string (YYYY-MM-DD)
func (t *Trade) Date() string {
	return t.Time.Format("2006-01-02")
}

// Minute returns the minute within the hour (0-59)
func (t *Trade) Minute() int {
	return t.Time.Minute()
}
