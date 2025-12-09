package memory

import (
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/v3/process"
)

// Stats holds current memory statistics
type Stats struct {
	Reserved   int64 // Memory reserved by active workers
	ActualUsed int64 // Actual RSS memory usage
	MaxBytes   int64 // Maximum allowed memory
	Available  int64 // MaxBytes - Reserved
}

// Manager handles memory allocation for process workers
// It uses a reservation-based system rather than actual memory monitoring
// to avoid GC lag issues
type Manager struct {
	maxBytes     int64
	reservedBytes atomic.Int64
	minFreeBytes int64 // Safety margin (1GB default)

	mu        sync.Mutex
	waiters   []chan struct{}

	// Stats channel for UI updates
	StatsChan chan Stats

	stopChan chan struct{}
	wg       sync.WaitGroup
}

// NewManager creates a new memory manager
func NewManager(maxBytes int64) *Manager {
	m := &Manager{
		maxBytes:     maxBytes,
		minFreeBytes: 1 * 1024 * 1024 * 1024, // 1GB safety margin
		StatsChan:    make(chan Stats, 10),
		stopChan:     make(chan struct{}),
	}

	m.wg.Add(1)
	go m.statsLoop()

	return m
}

// statsLoop periodically sends stats to the UI
func (m *Manager) statsLoop() {
	defer m.wg.Done()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-m.stopChan:
			return
		case <-ticker.C:
			stats := m.GetStats()
			select {
			case m.StatsChan <- stats:
			default:
				// Drop if channel is full
			}
		}
	}
}

// GetStats returns current memory statistics
func (m *Manager) GetStats() Stats {
	reserved := m.reservedBytes.Load()

	// Get actual RSS memory
	var actualUsed int64
	if p, err := process.NewProcess(int32(os.Getpid())); err == nil {
		if memInfo, err := p.MemoryInfo(); err == nil {
			actualUsed = int64(memInfo.RSS)
		}
	}

	return Stats{
		Reserved:   reserved,
		ActualUsed: actualUsed,
		MaxBytes:   m.maxBytes,
		Available:  m.maxBytes - reserved - m.minFreeBytes,
	}
}

// CanReserve checks if the requested memory can be reserved
func (m *Manager) CanReserve(bytes int64) bool {
	reserved := m.reservedBytes.Load()
	available := m.maxBytes - reserved - m.minFreeBytes
	return available >= bytes
}

// TryReserve attempts to reserve memory without blocking
// Returns true if successful
func (m *Manager) TryReserve(bytes int64) bool {
	for {
		current := m.reservedBytes.Load()
		available := m.maxBytes - current - m.minFreeBytes

		if available < bytes {
			return false
		}

		if m.reservedBytes.CompareAndSwap(current, current+bytes) {
			return true
		}
		// CAS failed, retry
	}
}

// Reserve blocks until the requested memory can be reserved
func (m *Manager) Reserve(bytes int64) {
	// Fast path: try without waiting
	if m.TryReserve(bytes) {
		return
	}

	// Slow path: wait for memory to become available
	waiter := make(chan struct{}, 1)

	m.mu.Lock()
	m.waiters = append(m.waiters, waiter)
	m.mu.Unlock()

	for {
		select {
		case <-waiter:
			if m.TryReserve(bytes) {
				return
			}
			// Re-add to waiters
			m.mu.Lock()
			m.waiters = append(m.waiters, waiter)
			m.mu.Unlock()
		case <-m.stopChan:
			return
		}
	}
}

// Release frees previously reserved memory
func (m *Manager) Release(bytes int64) {
	m.reservedBytes.Add(-bytes)

	// Wake up one waiter
	m.mu.Lock()
	if len(m.waiters) > 0 {
		waiter := m.waiters[0]
		m.waiters = m.waiters[1:]
		m.mu.Unlock()

		select {
		case waiter <- struct{}{}:
		default:
		}
	} else {
		m.mu.Unlock()
	}
}

// Stop shuts down the manager
func (m *Manager) Stop() {
	close(m.stopChan)
	m.wg.Wait()
	close(m.StatsChan)
}

// EstimateProcessMemory estimates the memory needed to process a CSV
// Since we use streaming parsing, we don't load the whole file in memory
// We only need memory for:
// - Read buffer (~4MB)
// - Trade batches (~10K trades × ~50 bytes = ~500KB)
// - Aggregator state (~100MB max)
// - Some overhead
// Use ~500MB as a safe estimate per worker
func EstimateProcessMemory(csvSize int64) int64 {
	const baseMemory = 500 * 1024 * 1024 // 500MB base
	// Add a small fraction of CSV size for safety (1%)
	return baseMemory + csvSize/100
}
