package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/clement/aggtrades/internal/aggregator"
)

// SerializedBar is a JSON-friendly representation of HourlyResult.
type SerializedBar struct {
	Time int64 `json:"t"`

	BuyVol  float64 `json:"bv"`
	SellVol float64 `json:"sv"`

	NTrades   int64 `json:"nt"`
	BuyCount  int64 `json:"bc"`
	SellCount int64 `json:"sc"`

	WhaleBuyVolP99    float64 `json:"wbv99"`
	WhaleSellVolP99   float64 `json:"wsv99"`
	WhaleBuyCountP99  int64   `json:"wbc99"`
	WhaleSellCountP99 int64   `json:"wsc99"`

	WhaleBuyVolP999    float64 `json:"wbv999"`
	WhaleSellVolP999   float64 `json:"wsv999"`
	WhaleBuyCountP999  int64   `json:"wbc999"`
	WhaleSellCountP999 int64   `json:"wsc999"`

	VolFirst30Min float64 `json:"vf30"`
	VolLast30Min  float64 `json:"vl30"`

	BuyVolUSD  float64 `json:"bvu"`
	SellVolUSD float64 `json:"svu"`

	MaxTradeSize float64 `json:"mts"`
	VWAP         float64 `json:"vwap"`
	PriceStd     float64 `json:"pstd"`
}

// SaveMonthBars saves hourly bars for a month to disk.
func SaveMonthBars(dir, month string, bars []aggregator.HourlyResult) error {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return err
	}

	serialized := make([]SerializedBar, len(bars))
	for i, b := range bars {
		serialized[i] = serializeBar(b)
	}

	data, err := json.Marshal(serialized)
	if err != nil {
		return err
	}

	path := filepath.Join(dir, month+".json")
	return atomicWrite(path, data)
}

// LoadMonthBars loads hourly bars for a month from disk.
func LoadMonthBars(dir, month string) ([]aggregator.HourlyResult, error) {
	path := filepath.Join(dir, month+".json")

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var serialized []SerializedBar
	if err := json.Unmarshal(data, &serialized); err != nil {
		return nil, nil // Corrupted
	}

	bars := make([]aggregator.HourlyResult, len(serialized))
	for i, s := range serialized {
		bars[i] = deserializeBar(s)
	}

	return bars, nil
}

// LoadAllBars loads bars for all specified months.
func LoadAllBars(dir string, months []string) ([]aggregator.HourlyResult, error) {
	var allBars []aggregator.HourlyResult

	for _, month := range months {
		bars, err := LoadMonthBars(dir, month)
		if err != nil {
			return nil, err
		}
		if bars != nil {
			allBars = append(allBars, bars...)
		}
	}

	sort.Slice(allBars, func(i, j int) bool {
		return allBars[i].Time.Before(allBars[j].Time)
	})

	return allBars, nil
}

func serializeBar(b aggregator.HourlyResult) SerializedBar {
	return SerializedBar{
		Time:               b.Time.Unix(),
		BuyVol:             b.Bar.BuyVol,
		SellVol:            b.Bar.SellVol,
		NTrades:            b.Bar.NTrades,
		BuyCount:           b.Bar.BuyCount,
		SellCount:          b.Bar.SellCount,
		WhaleBuyVolP99:     b.Bar.WhaleBuyVolP99,
		WhaleSellVolP99:    b.Bar.WhaleSellVolP99,
		WhaleBuyCountP99:   b.Bar.WhaleBuyCountP99,
		WhaleSellCountP99:  b.Bar.WhaleSellCountP99,
		WhaleBuyVolP999:    b.Bar.WhaleBuyVolP999,
		WhaleSellVolP999:   b.Bar.WhaleSellVolP999,
		WhaleBuyCountP999:  b.Bar.WhaleBuyCountP999,
		WhaleSellCountP999: b.Bar.WhaleSellCountP999,
		VolFirst30Min:      b.Bar.VolFirst30Min,
		VolLast30Min:       b.Bar.VolLast30Min,
		BuyVolUSD:          b.Bar.BuyVolUSD,
		SellVolUSD:         b.Bar.SellVolUSD,
		MaxTradeSize:       b.Bar.MaxTradeSize,
		VWAP:               b.Bar.VWAP,
		PriceStd:           b.Bar.PriceStd,
	}
}

func deserializeBar(s SerializedBar) aggregator.HourlyResult {
	return aggregator.HourlyResult{
		Time: time.Unix(s.Time, 0).UTC(),
		Bar: &aggregator.HourlyBar{
			BuyVol:             s.BuyVol,
			SellVol:            s.SellVol,
			NTrades:            s.NTrades,
			BuyCount:           s.BuyCount,
			SellCount:          s.SellCount,
			WhaleBuyVolP99:     s.WhaleBuyVolP99,
			WhaleSellVolP99:    s.WhaleSellVolP99,
			WhaleBuyCountP99:   s.WhaleBuyCountP99,
			WhaleSellCountP99:  s.WhaleSellCountP99,
			WhaleBuyVolP999:    s.WhaleBuyVolP999,
			WhaleSellVolP999:   s.WhaleSellVolP999,
			WhaleBuyCountP999:  s.WhaleBuyCountP999,
			WhaleSellCountP999: s.WhaleSellCountP999,
			VolFirst30Min:      s.VolFirst30Min,
			VolLast30Min:       s.VolLast30Min,
			BuyVolUSD:          s.BuyVolUSD,
			SellVolUSD:         s.SellVolUSD,
			MaxTradeSize:       s.MaxTradeSize,
			VWAP:               s.VWAP,
			PriceStd:           s.PriceStd,
		},
	}
}
