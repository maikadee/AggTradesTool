package config

import (
	"fmt"
	"os"
	"runtime"
	"time"

	"github.com/shirou/gopsutil/v3/mem"
)

const (
	// Binance Data Vision base URL
	BaseURL = "https://data.binance.vision/data/spot/monthly/aggTrades"

	// Default values
	DefaultSymbol          = "BTCUSDT"
	DefaultMaxMemoryPct    = 80
	DefaultDownloadWorkers = 3

	// Processing constants
	PercentileWindowDays  = 7
	WarmupHours           = 168 // 7 days
	MinTradesPerHour      = 100
	MaxGapToInterpolate   = 6
	ReservoirSamplesPerDay = 10000

	// Decompression ratio (ZIP to CSV)
	DecompressionRatio = 15
)

// Config holds all configuration for the application
type Config struct {
	Symbol       string
	StartDate    string
	EndDate      string
	TempDir      string
	OutputFile   string
	MaxMemoryPct int
	MaxMemoryBytes int64
	DownloadWorkers int
	NoTUI        bool
}

// New creates a new Config with default values
// Note: StartDate and EndDate are empty by default
// They are resolved in main.go via binance.DiscoverDateRange()
// or set explicitly via --start/--end flags
func New() *Config {
	return &Config{
		Symbol:          DefaultSymbol,
		StartDate:       "", // resolved dynamically
		EndDate:         "", // resolved dynamically
		MaxMemoryPct:    DefaultMaxMemoryPct,
		DownloadWorkers: DefaultDownloadWorkers,
		NoTUI:           false,
	}
}

// Validate checks the configuration and sets derived values
func (c *Config) Validate() error {
	// Validate dates are set (they should be resolved before calling Validate)
	if c.StartDate == "" {
		return fmt.Errorf("start date not set (use --start or let auto-discovery resolve it)")
	}
	if c.EndDate == "" {
		return fmt.Errorf("end date not set (use --end or let auto-discovery resolve it)")
	}

	// Validate date formats
	start, err := time.Parse("2006-01", c.StartDate)
	if err != nil {
		return fmt.Errorf("invalid start date format (expected YYYY-MM): %s", c.StartDate)
	}
	end, err := time.Parse("2006-01", c.EndDate)
	if err != nil {
		return fmt.Errorf("invalid end date format (expected YYYY-MM): %s", c.EndDate)
	}

	// Validate start <= end
	if start.After(end) {
		return fmt.Errorf("start date %s is after end date %s", c.StartDate, c.EndDate)
	}

	// Set default temp directory
	if c.TempDir == "" {
		c.TempDir = fmt.Sprintf("./aggtrades_temp_%s", c.Symbol)
	}

	// Set default output file
	if c.OutputFile == "" {
		c.OutputFile = fmt.Sprintf("aggtrades_%s_%s_%s.parquet", c.Symbol, c.StartDate, c.EndDate)
	}

	// Calculate max memory in bytes
	totalMem, err := GetTotalMemory()
	if err != nil {
		return fmt.Errorf("failed to get system memory: %w", err)
	}
	c.MaxMemoryBytes = int64(float64(totalMem) * float64(c.MaxMemoryPct) / 100.0)

	// Ensure temp directory exists
	if err := os.MkdirAll(c.TempDir, 0755); err != nil {
		return fmt.Errorf("failed to create temp directory: %w", err)
	}

	return nil
}

// GetTotalMemory returns total system memory in bytes
func GetTotalMemory() (uint64, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return v.Total, nil
}

// GetAvailableMemory returns currently available memory in bytes
func GetAvailableMemory() (uint64, error) {
	v, err := mem.VirtualMemory()
	if err != nil {
		return 0, err
	}
	return v.Available, nil
}

// GetNumCPU returns the number of CPUs
func GetNumCPU() int {
	return runtime.NumCPU()
}

// MonthURL returns the download URL for a specific month
func (c *Config) MonthURL(yearMonth string) string {
	return fmt.Sprintf("%s/%s/%s-aggTrades-%s.zip", BaseURL, c.Symbol, c.Symbol, yearMonth)
}

// MonthZipPath returns the local ZIP path for a month
func (c *Config) MonthZipPath(yearMonth string) string {
	return fmt.Sprintf("%s/%s.zip", c.TempDir, yearMonth)
}

// MonthCSVPath returns the local CSV path for a month
func (c *Config) MonthCSVPath(yearMonth string) string {
	return fmt.Sprintf("%s/%s.csv", c.TempDir, yearMonth)
}

// MonthParquetPath returns the local parquet path for a month
func (c *Config) MonthParquetPath(yearMonth string) string {
	return fmt.Sprintf("%s/%s.parquet", c.TempDir, yearMonth)
}

// GenerateMonths returns all months between start and end dates (inclusive)
func (c *Config) GenerateMonths() ([]string, error) {
	start, err := time.Parse("2006-01", c.StartDate)
	if err != nil {
		return nil, err
	}
	end, err := time.Parse("2006-01", c.EndDate)
	if err != nil {
		return nil, err
	}

	var months []string
	for current := start; !current.After(end); current = current.AddDate(0, 1, 0) {
		months = append(months, current.Format("2006-01"))
	}
	return months, nil
}

// String returns a human-readable representation of the config
func (c *Config) String() string {
	return fmt.Sprintf(
		"Symbol: %s, Period: %s → %s, MaxMem: %d%% (%.1f GB), Workers: %d",
		c.Symbol, c.StartDate, c.EndDate,
		c.MaxMemoryPct, float64(c.MaxMemoryBytes)/1024/1024/1024,
		c.DownloadWorkers,
	)
}
