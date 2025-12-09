package pipeline

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/clement/aggtrades/internal/aggregator"
	"github.com/clement/aggtrades/internal/config"
	"github.com/clement/aggtrades/internal/downloader"
	"github.com/clement/aggtrades/internal/memory"
	"github.com/clement/aggtrades/internal/parser"
	"github.com/clement/aggtrades/internal/state"
	"github.com/clement/aggtrades/internal/ui"
)

// Job represents a month to be processed
type Job struct {
	Month   string
	ZipPath string
	CSVPath string
	CSVSize int64
}

// Result represents the result of processing a month
type Result struct {
	Month  string
	Hours  int
	Trades int64
	Error  error
}

// Pipeline orchestrates the download and processing of monthly data
type Pipeline struct {
	cfg        *config.Config
	memMgr     *memory.Manager
	ui         *ui.UI
	aggregator *aggregator.Aggregator

	// State management
	stateMgr *state.Manager
	state    *state.State

	// Channels
	downloadQueue chan string    // Months to download
	processQueue  chan Job       // Jobs ready to process
	resultsChan   chan Result    // Processing results
	errorsChan    chan error     // Errors
	failedMonths  chan string    // Months that failed to download

	// State
	ctx        context.Context
	cancel     context.CancelFunc
	wg         sync.WaitGroup
	downloadWg sync.WaitGroup  // Separate WaitGroup for download workers

	// Ordered list of months for chronological processing
	monthOrder []string
}

// NewPipeline creates a new processing pipeline
func NewPipeline(cfg *config.Config, memMgr *memory.Manager, tui *ui.UI) *Pipeline {
	ctx, cancel := context.WithCancel(context.Background())
	stateDir := filepath.Join(cfg.TempDir, "checkpoint")

	return &Pipeline{
		cfg:           cfg,
		memMgr:        memMgr,
		ui:            tui,
		aggregator:    aggregator.NewAggregator(config.PercentileWindowDays, config.ReservoirSamplesPerDay),
		stateMgr:      state.NewManager(stateDir),
		downloadQueue: make(chan string, 100),
		processQueue:  make(chan Job, 10),
		resultsChan:   make(chan Result, 100),
		errorsChan:    make(chan error, 10),
		failedMonths:  make(chan string, 100),
		ctx:           ctx,
		cancel:        cancel,
	}
}

// Run executes the pipeline
func (p *Pipeline) Run() ([]aggregator.HourlyResult, error) {
	// Load existing state for resume
	var err error
	p.state, err = p.stateMgr.Load()
	if err != nil {
		return nil, fmt.Errorf("load state: %w", err)
	}

	// Initialize empty state if none exists
	if p.state == nil {
		p.state = &state.State{
			CompletedMonths: make([]string, 0),
		}
	}

	// Restore detector state if available
	if len(p.state.CompletedMonths) > 0 {
		if err := p.restoreDetectorState(); err != nil {
			p.ui.LogError("Failed to restore detector state: %v (starting fresh)", err)
		}
	}

	// Get all months to process
	allMonths, err := p.cfg.GenerateMonths()
	if err != nil {
		return nil, fmt.Errorf("generate months: %w", err)
	}

	// Filter to pending months
	pendingMonths := p.state.GetPendingMonths(allMonths)

	if len(pendingMonths) == 0 {
		p.ui.LogInfo("All months already processed")
		return p.loadExistingResults(allMonths)
	}

	p.ui.LogInfo("Processing %d months (%d already completed)", len(pendingMonths), len(allMonths)-len(pendingMonths))

	// Store month order for chronological processing
	p.monthOrder = GetMonthsInOrder(pendingMonths)

	// Start workers
	p.startDownloadWorkers(p.cfg.DownloadWorkers)
	p.startDispatcher()
	p.startResultCollector()

	// Queue months for download (in order)
	go func() {
		for _, month := range pendingMonths {
			select {
			case <-p.ctx.Done():
				return
			case p.downloadQueue <- month:
			}
		}
		close(p.downloadQueue)
	}()

	// Wait for downloads to complete, then close processQueue
	go func() {
		p.downloadWg.Wait()
		close(p.processQueue)
	}()

	// Wait for all processing to complete
	p.wg.Wait()

	// Flush aggregator
	p.aggregator.Flush()

	// Get new bars from this run
	newBars := p.aggregator.GetAllBars()

	// Load completed months' bars and merge
	completedBars, err := state.LoadAllBars(p.stateMgr.BarsDir(), p.state.CompletedMonths)
	if err != nil {
		return nil, fmt.Errorf("load completed bars: %w", err)
	}

	// Merge and sort all bars
	allBars := append(completedBars, newBars...)
	sort.Slice(allBars, func(i, j int) bool {
		return allBars[i].Time.Before(allBars[j].Time)
	})

	return allBars, nil
}

// restoreDetectorState loads and restores the detector state from disk.
func (p *Pipeline) restoreDetectorState() error {
	detectorState, err := state.LoadDetector(p.stateMgr.DetectorPath())
	if err != nil {
		return err
	}
	if detectorState == nil {
		return nil
	}

	p.aggregator.Detector().ImportState(detectorState.ToWhaleReservoirs(), detectorState.DateOrder)
	p.aggregator.Detector().ImportBootstrap(detectorState.ToWhaleBootstrap())
	p.ui.LogInfo("Restored detector state (%d days, bootstrap=%v)", len(detectorState.DateOrder), detectorState.Bootstrap.Bootstrapped)
	return nil
}

// startDownloadWorkers starts download worker goroutines
func (p *Pipeline) startDownloadWorkers(n int) {
	for i := 0; i < n; i++ {
		p.downloadWg.Add(1)
		go p.downloadWorker()
	}
}

// downloadWorker downloads files and queues them for processing
func (p *Pipeline) downloadWorker() {
	defer p.downloadWg.Done()

	for month := range p.downloadQueue {
		select {
		case <-p.ctx.Done():
			return
		default:
		}

		// Notify UI
		p.ui.WorkerChan <- ui.WorkerUpdate{
			Type:   ui.WorkerDownload,
			Month:  month,
			Active: true,
		}

		job, err := p.downloadMonth(month)

		p.ui.WorkerChan <- ui.WorkerUpdate{
			Type:   ui.WorkerDownload,
			Month:  month,
			Active: false,
		}

		if err != nil {
			p.errorsChan <- fmt.Errorf("download %s: %w", month, err)
			p.failedMonths <- month
			continue
		}

		if job != nil {
			p.processQueue <- *job
		}
	}
}

// downloadMonth downloads and extracts a month's data
func (p *Pipeline) downloadMonth(month string) (*Job, error) {
	zipPath := p.cfg.MonthZipPath(month)
	csvPath := p.cfg.MonthCSVPath(month)

	// Clean up any leftover .tmp files from interrupted downloads
	os.Remove(zipPath + ".tmp")
	os.Remove(csvPath + ".tmp")

	// Check if CSV already exists (complete file)
	if exists, size := downloader.FileExists(csvPath); exists {
		return &Job{
			Month:   month,
			CSVPath: csvPath,
			CSVSize: size,
		}, nil
	}

	// Check if ZIP already exists (complete file)
	zipExists, _ := downloader.FileExists(zipPath)

	// Download if needed
	if !zipExists {
		url := p.cfg.MonthURL(month)
		_, err := downloader.DownloadWithRetry(p.ctx, url, zipPath, nil)
		if err != nil {
			return nil, err
		}
	}

	// Extract to temp file first, then rename
	csvSize, err := downloader.ExtractAndRemoveZip(zipPath, csvPath)
	if err != nil {
		// ZIP might be corrupted, remove it and retry on next run
		os.Remove(zipPath)
		return nil, err
	}

	return &Job{
		Month:   month,
		ZipPath: zipPath,
		CSVPath: csvPath,
		CSVSize: csvSize,
	}, nil
}

// startDispatcher starts the memory-aware job dispatcher
func (p *Pipeline) startDispatcher() {
	p.wg.Add(1)
	go p.dispatcher()
}

// dispatcher dispatches jobs to process workers in CHRONOLOGICAL ORDER
// This ensures the rolling percentile window has correct historical data
func (p *Pipeline) dispatcher() {
	defer p.wg.Done()
	defer close(p.resultsChan) // Signal resultCollector to exit

	// Map to hold downloaded jobs waiting to be processed
	readyJobs := make(map[string]Job)
	failedSet := make(map[string]bool)
	nextMonthIdx := 0
	processQueueClosed := false

	for {
		// Try to process the next month in chronological order
		for nextMonthIdx < len(p.monthOrder) {
			nextMonth := p.monthOrder[nextMonthIdx]

			// Skip failed months
			if failedSet[nextMonth] {
				nextMonthIdx++
				continue
			}

			job, ok := readyJobs[nextMonth]
			if !ok {
				// Next month not downloaded yet, wait
				break
			}

			// Next month is ready - wait for memory and process
			estimatedMem := memory.EstimateProcessMemory(job.CSVSize)
			p.memMgr.Reserve(estimatedMem) // Blocking wait for memory

			delete(readyJobs, nextMonth)
			nextMonthIdx++

			// Process synchronously to maintain order
			p.processMonthSequential(job, estimatedMem)
		}

		// Check if we're done
		if processQueueClosed && nextMonthIdx >= len(p.monthOrder) {
			return
		}

		// Get next downloaded job or wait
		select {
		case <-p.ctx.Done():
			return

		case job, ok := <-p.processQueue:
			if !ok {
				processQueueClosed = true
				continue
			}
			// Store job for later processing in order
			readyJobs[job.Month] = job

		case failed := <-p.failedMonths:
			failedSet[failed] = true

		case <-time.After(50 * time.Millisecond):
			// Timeout to check for new jobs
		}
	}
}

// processMonthSequential processes a single month synchronously
func (p *Pipeline) processMonthSequential(job Job, reservedMem int64) {
	defer p.memMgr.Release(reservedMem)

	// Notify UI - start
	p.ui.WorkerChan <- ui.WorkerUpdate{
		Type:   ui.WorkerProcess,
		Month:  job.Month,
		Active: true,
	}

	result := p.processMonth(job)

	// Notify UI - end
	p.ui.WorkerChan <- ui.WorkerUpdate{
		Type:   ui.WorkerProcess,
		Month:  job.Month,
		Active: false,
	}

	// Send result
	p.resultsChan <- result
}

// processWorker processes a single month
func (p *Pipeline) processWorker(job Job, reservedMem int64) {
	defer p.wg.Done()
	defer p.memMgr.Release(reservedMem)

	// Notify UI
	p.ui.WorkerChan <- ui.WorkerUpdate{
		Type:   ui.WorkerProcess,
		Month:  job.Month,
		Active: true,
	}

	result := p.processMonth(job)

	p.ui.WorkerChan <- ui.WorkerUpdate{
		Type:   ui.WorkerProcess,
		Month:  job.Month,
		Active: false,
	}

	p.resultsChan <- result
}

// processMonth processes a single month's CSV file
func (p *Pipeline) processMonth(job Job) Result {
	result := Result{Month: job.Month}

	// Parse CSV and aggregate
	totalTrades, err := parser.ParseCSV(job.CSVPath, func(trades []parser.Trade) error {
		p.aggregator.ProcessTrades(trades)
		return nil
	})

	if err != nil {
		result.Error = err
		return result
	}

	result.Trades = totalTrades

	// Clean up CSV file
	os.Remove(job.CSVPath)

	// Save checkpoint (bars + detector + state)
	hours, err := p.saveCheckpoint(job.Month)
	if err != nil {
		p.ui.LogError("Failed to save checkpoint: %v", err)
	}
	result.Hours = hours

	return result
}

// saveCheckpoint saves the current state after completing a month.
// Order matters: bars → detector → state (state.json marks completion)
// Returns the number of hours saved.
func (p *Pipeline) saveCheckpoint(month string) (int, error) {
	// 1. Get ALL bars for this month (pending + finalized) and save
	bars := p.aggregator.GetBarsForMonth(month)
	if err := state.SaveMonthBars(p.stateMgr.BarsDir(), month, bars); err != nil {
		return 0, fmt.Errorf("save bars: %w", err)
	}

	// 2. Save detector state
	if err := state.SaveDetector(p.stateMgr.DetectorPath(), p.aggregator.Detector()); err != nil {
		return len(bars), fmt.Errorf("save detector: %w", err)
	}

	// 3. Update and save global state (marks month as completed)
	p.state.CompletedMonths = append(p.state.CompletedMonths, month)
	if err := p.stateMgr.Save(p.state); err != nil {
		return len(bars), fmt.Errorf("save state: %w", err)
	}

	return len(bars), nil
}

// startResultCollector collects and reports results
func (p *Pipeline) startResultCollector() {
	p.wg.Add(1)
	go p.resultCollector()
}

// resultCollector collects processing results
func (p *Pipeline) resultCollector() {
	defer p.wg.Done()

	for {
		select {
		case <-p.ctx.Done():
			return

		case result, ok := <-p.resultsChan:
			if !ok {
				return
			}

			// Notify UI
			p.ui.CompleteChan <- ui.MonthComplete{
				Month:   result.Month,
				Hours:   result.Hours,
				Trades:  result.Trades,
				Success: result.Error == nil,
			}

		case err := <-p.errorsChan:
			p.ui.LogError("%v", err)
		}
	}
}

// loadExistingResults loads bars from all completed months
func (p *Pipeline) loadExistingResults(months []string) ([]aggregator.HourlyResult, error) {
	return state.LoadAllBars(p.stateMgr.BarsDir(), months)
}

// Stop gracefully stops the pipeline
func (p *Pipeline) Stop() {
	p.cancel()

	// Save current state if available
	if p.state != nil && p.stateMgr != nil {
		if err := p.stateMgr.Save(p.state); err != nil {
			p.ui.LogError("Failed to save state on stop: %v", err)
		}
	}
}

// UpdateMemoryStats sends memory stats to the UI
func (p *Pipeline) UpdateMemoryStats() {
	stats := p.memMgr.GetStats()
	p.ui.UpdateMemory(
		float64(stats.Reserved)/1024/1024/1024,
		float64(stats.ActualUsed)/1024/1024/1024,
		float64(stats.MaxBytes)/1024/1024/1024,
	)
}

// GetMonthsInOrder returns months sorted chronologically
func GetMonthsInOrder(months []string) []string {
	sorted := make([]string, len(months))
	copy(sorted, months)
	sort.Strings(sorted)
	return sorted
}
