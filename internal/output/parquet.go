package output

import (
	"os"
	"sort"

	"github.com/clement/aggtrades/internal/aggregator"
	"github.com/parquet-go/parquet-go"
)

// HourlyRow represents a single row in the output parquet file
// Field names match the Python output for compatibility
type HourlyRow struct {
	Time              int64   `parquet:"time,timestamp(millisecond)"`
	BuyVol            float64 `parquet:"buy_vol"`
	SellVol           float64 `parquet:"sell_vol"`
	NTrades           int64   `parquet:"n_trades"`
	BuyCount          int64   `parquet:"buy_count"`
	SellCount         int64   `parquet:"sell_count"`
	WhaleBuyVolP99    float64 `parquet:"whale_buy_vol_p99"`
	WhaleSellVolP99   float64 `parquet:"whale_sell_vol_p99"`
	WhaleBuyCountP99  int64   `parquet:"whale_buy_count_p99"`
	WhaleSellCountP99 int64   `parquet:"whale_sell_count_p99"`
	WhaleBuyVolP999   float64 `parquet:"whale_buy_vol_p999"`
	WhaleSellVolP999  float64 `parquet:"whale_sell_vol_p999"`
	WhaleBuyCountP999 int64   `parquet:"whale_buy_count_p999"`
	WhaleSellCountP999 int64  `parquet:"whale_sell_count_p999"`
	VolFirst30Min     float64 `parquet:"vol_first_30min"`
	VolLast30Min      float64 `parquet:"vol_last_30min"`
	BuyVolUSD         float64 `parquet:"buy_vol_usd"`
	SellVolUSD        float64 `parquet:"sell_vol_usd"`
	MaxTradeSize      float64 `parquet:"max_trade_size"`
	VWAP              float64 `parquet:"vwap"`
	PriceStd          float64 `parquet:"price_std"`
}

// toRow converts an HourlyResult to a parquet row
func toRow(result aggregator.HourlyResult) HourlyRow {
	return HourlyRow{
		Time:              result.Time.UnixMilli(),
		BuyVol:            result.Bar.BuyVol,
		SellVol:           result.Bar.SellVol,
		NTrades:           result.Bar.NTrades,
		BuyCount:          result.Bar.BuyCount,
		SellCount:         result.Bar.SellCount,
		WhaleBuyVolP99:    result.Bar.WhaleBuyVolP99,
		WhaleSellVolP99:   result.Bar.WhaleSellVolP99,
		WhaleBuyCountP99:  result.Bar.WhaleBuyCountP99,
		WhaleSellCountP99: result.Bar.WhaleSellCountP99,
		WhaleBuyVolP999:   result.Bar.WhaleBuyVolP999,
		WhaleSellVolP999:  result.Bar.WhaleSellVolP999,
		WhaleBuyCountP999: result.Bar.WhaleBuyCountP999,
		WhaleSellCountP999: result.Bar.WhaleSellCountP999,
		VolFirst30Min:     result.Bar.VolFirst30Min,
		VolLast30Min:      result.Bar.VolLast30Min,
		BuyVolUSD:         result.Bar.BuyVolUSD,
		SellVolUSD:        result.Bar.SellVolUSD,
		MaxTradeSize:      result.Bar.MaxTradeSize,
		VWAP:              result.Bar.VWAP,
		PriceStd:          result.Bar.PriceStd,
	}
}

// WriteParquet writes hourly bars to a parquet file
func WriteParquet(bars []aggregator.HourlyResult, outputPath string) error {
	// Sort by time
	sort.Slice(bars, func(i, j int) bool {
		return bars[i].Time.Before(bars[j].Time)
	})

	// Create output file
	file, err := os.Create(outputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create parquet writer
	writer := parquet.NewGenericWriter[HourlyRow](file)

	// Write rows in batches
	const batchSize = 1000
	rows := make([]HourlyRow, 0, batchSize)

	for _, bar := range bars {
		rows = append(rows, toRow(bar))

		if len(rows) >= batchSize {
			if _, err := writer.Write(rows); err != nil {
				return err
			}
			rows = rows[:0]
		}
	}

	// Write remaining rows
	if len(rows) > 0 {
		if _, err := writer.Write(rows); err != nil {
			return err
		}
	}

	return writer.Close()
}

// GetParquetStats returns statistics about a parquet file
func GetParquetStats(path string) (rows int64, sizeBytes int64, err error) {
	info, err := os.Stat(path)
	if err != nil {
		return 0, 0, err
	}
	sizeBytes = info.Size()

	file, err := os.Open(path)
	if err != nil {
		return 0, sizeBytes, err
	}
	defer file.Close()

	reader := parquet.NewGenericReader[HourlyRow](file)
	defer reader.Close()

	rows = reader.NumRows()
	return rows, sizeBytes, nil
}
