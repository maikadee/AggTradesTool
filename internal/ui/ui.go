package ui

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/pterm/pterm"
)

// WorkerType represents the type of worker
type WorkerType int

const (
	WorkerDownload WorkerType = iota
	WorkerProcess
)

// WorkerUpdate represents a worker status update
type WorkerUpdate struct {
	Type      WorkerType
	Month     string
	Active    bool
	BytesDown int64 // For download progress
}

// MonthComplete represents a completed month
type MonthComplete struct {
	Month      string
	Hours      int
	Trades     int64
	Success    bool
	Issues     []string
}

// UI handles the terminal user interface
type UI struct {
	enabled       bool
	totalMonths   int
	symbol        string
	startDate     string
	endDate       string
	maxMemoryGB   float64

	mu             sync.Mutex
	completedCount int
	startTime      time.Time

	// Worker tracking
	downloadWorkers map[string]struct{}
	processWorkers  map[string]struct{}

	// Memory stats
	memReservedGB float64
	memActualGB   float64
	memMaxGB      float64

	// Recent completions
	recentCompletions []MonthComplete

	// Channels for updates
	WorkerChan   chan WorkerUpdate
	CompleteChan chan MonthComplete
	stopChan     chan struct{}
	wg           sync.WaitGroup

	// pterm components
	area *pterm.AreaPrinter
}

// New creates a new UI
func New(enabled bool, symbol, startDate, endDate string, totalMonths int, maxMemoryGB float64) *UI {
	u := &UI{
		enabled:          enabled,
		totalMonths:      totalMonths,
		symbol:           symbol,
		startDate:        startDate,
		endDate:          endDate,
		maxMemoryGB:      maxMemoryGB,
		startTime:        time.Now(),
		downloadWorkers:  make(map[string]struct{}),
		processWorkers:   make(map[string]struct{}),
		recentCompletions: make([]MonthComplete, 0, 5),
		WorkerChan:       make(chan WorkerUpdate, 100),
		CompleteChan:     make(chan MonthComplete, 100),
		stopChan:         make(chan struct{}),
	}
	return u
}

// Start begins the UI update loop
func (u *UI) Start() {
	if !u.enabled {
		return
	}

	var err error
	u.area, err = pterm.DefaultArea.WithCenter(false).Start()
	if err != nil {
		u.enabled = false
		return
	}

	u.wg.Add(1)
	go u.updateLoop()
}

// Stop shuts down the UI (safe to call multiple times)
func (u *UI) Stop() {
	u.mu.Lock()
	select {
	case <-u.stopChan:
		// Already closed
		u.mu.Unlock()
		return
	default:
		close(u.stopChan)
	}
	u.mu.Unlock()

	u.wg.Wait()

	if u.area != nil {
		u.area.Stop()
		u.area = nil
	}
}

// UpdateMemory updates memory statistics
func (u *UI) UpdateMemory(reservedGB, actualGB, maxGB float64) {
	u.mu.Lock()
	u.memReservedGB = reservedGB
	u.memActualGB = actualGB
	u.memMaxGB = maxGB
	u.mu.Unlock()
}

func (u *UI) updateLoop() {
	defer u.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-u.stopChan:
			return

		case update := <-u.WorkerChan:
			u.handleWorkerUpdate(update)

		case complete := <-u.CompleteChan:
			u.handleComplete(complete)

		case <-ticker.C:
			u.render()
		}
	}
}

func (u *UI) handleWorkerUpdate(update WorkerUpdate) {
	u.mu.Lock()
	defer u.mu.Unlock()

	switch update.Type {
	case WorkerDownload:
		if update.Active {
			u.downloadWorkers[update.Month] = struct{}{}
		} else {
			delete(u.downloadWorkers, update.Month)
		}
	case WorkerProcess:
		if update.Active {
			u.processWorkers[update.Month] = struct{}{}
		} else {
			delete(u.processWorkers, update.Month)
		}
	}
}

func (u *UI) handleComplete(complete MonthComplete) {
	u.mu.Lock()
	defer u.mu.Unlock()

	u.completedCount++

	// Keep last 20 completions
	u.recentCompletions = append(u.recentCompletions, complete)
	if len(u.recentCompletions) > 20 {
		u.recentCompletions = u.recentCompletions[1:]
	}
}

func (u *UI) render() {
	if !u.enabled || u.area == nil {
		return
	}

	u.mu.Lock()
	defer u.mu.Unlock()

	// Build the display
	output := u.buildDisplay()
	u.area.Update(output)
}

func (u *UI) buildDisplay() string {
	var s string

	// Header
	header := pterm.DefaultHeader.WithBackgroundStyle(pterm.NewStyle(pterm.BgCyan)).
		WithTextStyle(pterm.NewStyle(pterm.FgBlack)).
		Sprintf("AggTradeTool - %s (%s → %s)", u.symbol, u.startDate, u.endDate)
	s += header + "\n\n"

	// Progress
	pct := float64(u.completedCount) / float64(u.totalMonths) * 100
	progressBar := u.makeProgressBar(pct, 40)
	eta := u.calculateETA()
	s += fmt.Sprintf("  Progress: %s %d/%d months (%.1f%%)\n", progressBar, u.completedCount, u.totalMonths, pct)
	s += fmt.Sprintf("  ETA: %s\n\n", eta)

	// Workers
	s += pterm.DefaultSection.Sprint("Workers")
	downloadList := u.getWorkerList(u.downloadWorkers)
	processList := u.getWorkerList(u.processWorkers)
	s += fmt.Sprintf("  Download:  %d active   %s\n", len(u.downloadWorkers), downloadList)
	s += fmt.Sprintf("  Process:   %d active   %s\n\n", len(u.processWorkers), processList)

	// Memory
	s += pterm.DefaultSection.Sprint("Resources")
	memPct := 0.0
	if u.memMaxGB > 0 {
		memPct = u.memActualGB / u.memMaxGB * 100
	}
	memBar := u.makeProgressBar(memPct, 30)
	s += fmt.Sprintf("  Memory: %.1f GB / %.1f GB %s\n\n", u.memActualGB, u.memMaxGB, memBar)

	// Recent completions
	if len(u.recentCompletions) > 0 {
		s += pterm.DefaultSection.Sprint("Recent")
		for _, c := range u.recentCompletions {
			status := pterm.Green("✓")
			if !c.Success {
				status = pterm.Yellow("!")
			}
			s += fmt.Sprintf("  %s %s  %dh  %.1fM trades\n", status, c.Month, c.Hours, float64(c.Trades)/1_000_000)
		}
	}

	return s
}

func (u *UI) makeProgressBar(pct float64, width int) string {
	filled := int(pct / 100 * float64(width))
	if filled > width {
		filled = width
	}
	if filled < 0 {
		filled = 0
	}

	bar := "["
	for i := 0; i < width; i++ {
		if i < filled {
			bar += "█"
		} else {
			bar += "░"
		}
	}
	bar += "]"
	return bar
}

func (u *UI) calculateETA() string {
	if u.completedCount == 0 {
		return "calculating..."
	}

	elapsed := time.Since(u.startTime)
	avgPerMonth := elapsed / time.Duration(u.completedCount)
	remaining := u.totalMonths - u.completedCount
	eta := avgPerMonth * time.Duration(remaining)

	if eta < time.Minute {
		return fmt.Sprintf("%ds", int(eta.Seconds()))
	} else if eta < time.Hour {
		return fmt.Sprintf("%dm %ds", int(eta.Minutes()), int(eta.Seconds())%60)
	}
	return fmt.Sprintf("%dh %dm", int(eta.Hours()), int(eta.Minutes())%60)
}

func (u *UI) getWorkerList(workers map[string]struct{}) string {
	if len(workers) == 0 {
		return ""
	}

	// Collect and sort keys to avoid shuffle
	months := make([]string, 0, len(workers))
	for month := range workers {
		months = append(months, month)
	}
	sort.Strings(months)

	var list string
	for i, month := range months {
		if i >= 4 {
			list += fmt.Sprintf(" +%d more", len(months)-4)
			break
		}
		list += fmt.Sprintf("[%s] ", month)
	}
	return list
}

// LogInfo logs an info message (works even with TUI disabled)
func (u *UI) LogInfo(format string, args ...interface{}) {
	if u.enabled {
		// Will be shown in the area
		return
	}
	pterm.Info.Printfln(format, args...)
}

// LogError logs an error message
func (u *UI) LogError(format string, args ...interface{}) {
	pterm.Error.Printfln(format, args...)
}

// LogSuccess logs a success message
func (u *UI) LogSuccess(format string, args ...interface{}) {
	pterm.Success.Printfln(format, args...)
}

// PrintFinalStats prints final statistics after processing
func (u *UI) PrintFinalStats(totalHours int, totalTrades int64, duration time.Duration, outputFile string) {
	fmt.Println()
	pterm.DefaultHeader.WithBackgroundStyle(pterm.NewStyle(pterm.BgGreen)).
		WithTextStyle(pterm.NewStyle(pterm.FgBlack)).
		Println("Processing Complete")

	fmt.Println()
	pterm.Info.Printfln("Total hours: %d", totalHours)
	pterm.Info.Printfln("Total trades: %s", formatNumber(totalTrades))
	pterm.Info.Printfln("Duration: %s", duration.Round(time.Second))
	pterm.Info.Printfln("Output: %s", outputFile)
	fmt.Println()
}

func formatNumber(n int64) string {
	if n >= 1_000_000_000 {
		return fmt.Sprintf("%.2fB", float64(n)/1_000_000_000)
	}
	if n >= 1_000_000 {
		return fmt.Sprintf("%.2fM", float64(n)/1_000_000)
	}
	if n >= 1_000 {
		return fmt.Sprintf("%.2fK", float64(n)/1_000)
	}
	return fmt.Sprintf("%d", n)
}
