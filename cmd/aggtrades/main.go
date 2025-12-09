package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/clement/aggtrades/internal/binance"
	"github.com/clement/aggtrades/internal/config"
	"github.com/clement/aggtrades/internal/memory"
	"github.com/clement/aggtrades/internal/output"
	"github.com/clement/aggtrades/internal/pipeline"
	"github.com/clement/aggtrades/internal/ui"
	"github.com/spf13/cobra"
)

var (
	cfg = config.New()
)

func main() {
	rootCmd := &cobra.Command{
		Use:   "aggtrades",
		Short: "Download and aggregate Binance aggTrades data",
		Long: `AggTradeTool downloads BTCUSDT (or other symbol) aggTrades from Binance Data Vision,
aggregates to hourly with order flow features and rolling percentile whale detection.

If --start is not specified, downloads from the first available data for the symbol.
If --end is not specified, downloads until the last available data for the symbol.`,
		RunE: run,
	}

	// Flags
	rootCmd.Flags().StringVarP(&cfg.Symbol, "symbol", "s", cfg.Symbol, "Trading pair symbol")
	rootCmd.Flags().StringVar(&cfg.StartDate, "start", "", "Start date (YYYY-MM), defaults to first available")
	rootCmd.Flags().StringVar(&cfg.EndDate, "end", "", "End date (YYYY-MM), defaults to last available")
	rootCmd.Flags().IntVar(&cfg.MaxMemoryPct, "max-memory", cfg.MaxMemoryPct, "Maximum memory usage percentage")
	rootCmd.Flags().IntVar(&cfg.DownloadWorkers, "download-workers", cfg.DownloadWorkers, "Number of download workers")
	rootCmd.Flags().BoolVar(&cfg.NoTUI, "no-tui", cfg.NoTUI, "Disable TUI (use plain logs)")

	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func run(cmd *cobra.Command, args []string) error {
	startTime := time.Now()

	// Resolve dynamic date range if needed
	if err := resolveDateRange(cmd); err != nil {
		return fmt.Errorf("failed to resolve date range: %w", err)
	}

	// Validate configuration
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("configuration error: %w", err)
	}

	// Get months to process
	months, err := cfg.GenerateMonths()
	if err != nil {
		return fmt.Errorf("generate months: %w", err)
	}

	fmt.Printf("AggTradeTool - %s\n", cfg.String())
	fmt.Printf("Months to process: %d (%s to %s)\n\n", len(months), cfg.StartDate, cfg.EndDate)

	// Initialize memory manager
	memMgr := memory.NewManager(cfg.MaxMemoryBytes)
	defer memMgr.Stop()

	// Initialize UI
	tui := ui.New(
		!cfg.NoTUI,
		cfg.Symbol,
		cfg.StartDate,
		cfg.EndDate,
		len(months),
		float64(cfg.MaxMemoryBytes)/1024/1024/1024,
	)
	tui.Start()
	defer tui.Stop()

	// Initialize pipeline
	p := pipeline.NewPipeline(cfg, memMgr, tui)

	// Context for graceful shutdown of background goroutines
	ctx, cancelStats := context.WithCancel(context.Background())
	defer cancelStats()

	// Handle interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		fmt.Println("\nInterrupted - saving checkpoint...")
		p.Stop()
	}()

	// Start memory stats update loop
	go func() {
		ticker := time.NewTicker(500 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				p.UpdateMemoryStats()
			}
		}
	}()

	// Run pipeline
	bars, err := p.Run()
	if err != nil {
		return fmt.Errorf("pipeline error: %w", err)
	}

	if len(bars) == 0 {
		tui.LogError("No data processed")
		return nil
	}

	// Validate data
	tui.LogInfo("Validating data...")
	validation := output.Validate(bars)
	if !validation.Valid {
		for _, issue := range validation.Issues {
			tui.LogWarning("Validation: %s", issue)
		}
	}

	// Fill gaps
	tui.LogInfo("Filling gaps...")
	bars, smallFilled, largeNaN := output.FillGaps(bars, config.MaxGapToInterpolate)
	tui.LogInfo("Gaps: %d interpolated, %d left as NaN", smallFilled, largeNaN)

	// Trim warmup period
	originalLen := len(bars)
	bars = output.TrimWarmup(bars, config.WarmupHours)
	tui.LogInfo("Trimmed warmup: %d hours removed", originalLen-len(bars))

	// Calculate totals
	var totalTrades int64
	for _, bar := range bars {
		totalTrades += bar.Bar.NTrades
	}

	// Write parquet
	tui.LogInfo("Writing parquet...")
	if err := output.WriteParquet(bars, cfg.OutputFile); err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	// Stop TUI and print final stats
	tui.Stop()
	duration := time.Since(startTime)
	tui.PrintFinalStats(len(bars), totalTrades, duration, cfg.OutputFile)

	return nil
}

// resolveDateRange queries Binance for available dates if start/end not specified
func resolveDateRange(cmd *cobra.Command) error {
	startSet := cmd.Flags().Changed("start")
	endSet := cmd.Flags().Changed("end")

	// If both are set, nothing to do
	if startSet && endSet {
		return nil
	}

	// Need to discover available dates
	fmt.Printf("Discovering available data for %s...\n", cfg.Symbol)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	dateRange, err := binance.DiscoverDateRange(ctx, cfg.Symbol)
	if err != nil {
		return fmt.Errorf("could not discover available dates for %s: %w\nPlease specify --start and --end manually, or check network connection",
			cfg.Symbol, err)
	}

	if !startSet {
		cfg.StartDate = dateRange.FirstMonth
		fmt.Printf("  First available: %s\n", cfg.StartDate)
	}

	if !endSet {
		cfg.EndDate = dateRange.LastMonth
		fmt.Printf("  Last available: %s\n", cfg.EndDate)
	}

	return nil
}
