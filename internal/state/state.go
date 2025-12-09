package state

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const (
	currentVersion = 1
	stateFileName  = "state.json"
	detectorFileName = "detector.json"
	barsDirName    = "bars"
)

// State represents the complete checkpoint state.
type State struct {
	Version         int       `json:"version"`
	CompletedMonths []string  `json:"completed_months"`
	LastUpdate      time.Time `json:"last_update"`
}

// Manager handles state persistence.
type Manager struct {
	dir string
}

// NewManager creates a new state manager.
func NewManager(dir string) *Manager {
	return &Manager{dir: dir}
}

// Dir returns the state directory path.
func (m *Manager) Dir() string {
	return m.dir
}

// BarsDir returns the bars subdirectory path.
func (m *Manager) BarsDir() string {
	return filepath.Join(m.dir, barsDirName)
}

// DetectorPath returns the detector state file path.
func (m *Manager) DetectorPath() string {
	return filepath.Join(m.dir, detectorFileName)
}

// Load loads the state from disk. Returns nil if no state exists.
func (m *Manager) Load() (*State, error) {
	path := filepath.Join(m.dir, stateFileName)

	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return nil, nil // Corrupted, start fresh
	}

	if st.Version != currentVersion {
		return nil, nil // Incompatible version
	}

	return &st, nil
}

// Save persists the state to disk atomically.
func (m *Manager) Save(st *State) error {
	if err := os.MkdirAll(m.dir, 0755); err != nil {
		return err
	}

	st.Version = currentVersion
	st.LastUpdate = time.Now()

	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		return err
	}

	return atomicWrite(filepath.Join(m.dir, stateFileName), data)
}

// IsCompleted checks if a month has been completed.
func (st *State) IsCompleted(month string) bool {
	for _, m := range st.CompletedMonths {
		if m == month {
			return true
		}
	}
	return false
}

// GetPendingMonths returns months that need processing.
func (st *State) GetPendingMonths(allMonths []string) []string {
	if st == nil {
		return allMonths
	}

	pending := make([]string, 0)
	for _, m := range allMonths {
		if !st.IsCompleted(m) {
			pending = append(pending, m)
		}
	}
	return pending
}

// Clear removes all state files.
func (m *Manager) Clear() error {
	return os.RemoveAll(m.dir)
}

// atomicWrite writes data to path atomically using a temp file.
func atomicWrite(path string, data []byte) error {
	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}
