package parser

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	// CSV columns: agg_id, price, qty, first_id, last_id, time, is_buyer_maker, _
	colAggID        = 0
	colPrice        = 1
	colQty          = 2
	colFirstID      = 3
	colLastID       = 4
	colTime         = 5
	colIsBuyerMaker = 6
	numCols         = 8

	// Batch size for yielding trades
	batchSize = 10000

	// Buffer size for reading
	readBufferSize = 4 * 1024 * 1024 // 4MB buffer

	// Expected header columns (Binance aggTrades format)
	expectedHeader = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker"
)

// TradeCallback is called for each batch of trades
type TradeCallback func(trades []Trade) error

// ParseCSV reads a CSV file and calls the callback for each batch of trades
// Uses streaming to minimize memory usage
func ParseCSV(csvPath string, callback TradeCallback) (int64, error) {
	file, err := os.Open(csvPath)
	if err != nil {
		return 0, fmt.Errorf("open file: %w", err)
	}
	defer file.Close()

	reader := bufio.NewReaderSize(file, readBufferSize)
	batch := make([]Trade, 0, batchSize)
	var totalTrades int64

	// Read first line - Binance CSV may or may not have a header
	firstLine, err := reader.ReadString('\n')
	if err != nil {
		return 0, fmt.Errorf("read first line: %w", err)
	}
	firstLine = strings.TrimSpace(firstLine)

	// Check if first line is header or data (Binance CSVs typically have no header)
	if !isHeaderLine(firstLine) {
		// First line is data, process it
		trade, parseErr := parseLine(firstLine)
		if parseErr == nil {
			batch = append(batch, trade)
			totalTrades++
		}
	}
	// If it's a header, we just skip it (already read)

	for {
		line, err := reader.ReadString('\n')
		line = strings.TrimSpace(line)

		// Process line if non-empty (even on EOF with partial line)
		if line != "" {
			trade, parseErr := parseLine(line)
			if parseErr == nil {
				batch = append(batch, trade)
				totalTrades++

				// Yield batch when full
				if len(batch) >= batchSize {
					if cbErr := callback(batch); cbErr != nil {
						return totalTrades, cbErr
					}
					batch = batch[:0]
				}
			}
		}

		// Check for EOF after processing the line
		if err != nil {
			break
		}
	}

	// Yield remaining trades
	if len(batch) > 0 {
		if err := callback(batch); err != nil {
			return totalTrades, err
		}
	}

	return totalTrades, nil
}

// parseLine parses a single CSV line into a Trade
func parseLine(line string) (Trade, error) {
	var raw RawTrade
	var err error

	parts := strings.Split(line, ",")
	if len(parts) < numCols-1 {
		return Trade{}, fmt.Errorf("insufficient columns: %d", len(parts))
	}

	raw.AggID, err = strconv.ParseInt(parts[colAggID], 10, 64)
	if err != nil {
		return Trade{}, fmt.Errorf("parse agg_id: %w", err)
	}

	raw.Price, err = strconv.ParseFloat(parts[colPrice], 64)
	if err != nil {
		return Trade{}, fmt.Errorf("parse price: %w", err)
	}

	raw.Qty, err = strconv.ParseFloat(parts[colQty], 64)
	if err != nil {
		return Trade{}, fmt.Errorf("parse qty: %w", err)
	}

	raw.Timestamp, err = strconv.ParseInt(parts[colTime], 10, 64)
	if err != nil {
		return Trade{}, fmt.Errorf("parse timestamp: %w", err)
	}

	isBuyerMaker := strings.ToLower(parts[colIsBuyerMaker])
	raw.IsBuyerMaker = isBuyerMaker == "true"

	return raw.ToTrade(), nil
}

// CountLines counts the number of lines in a file (for progress estimation)
func CountLines(path string) (int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	var count int64
	scanner := bufio.NewScanner(file)
	buf := make([]byte, readBufferSize)
	scanner.Buffer(buf, readBufferSize)

	for scanner.Scan() {
		count++
	}

	return count, scanner.Err()
}

// isHeaderLine checks if a line is a CSV header (not data)
// Binance CSVs typically have no header, but some may have one
func isHeaderLine(line string) bool {
	// If line starts with expected header prefix, it's a header
	if strings.HasPrefix(line, "agg_trade_id") {
		return true
	}
	// If first field is not a number, it's likely a header
	parts := strings.Split(line, ",")
	if len(parts) > 0 {
		_, err := strconv.ParseInt(parts[0], 10, 64)
		if err != nil {
			return true // First field is not a number = header
		}
	}
	return false
}
