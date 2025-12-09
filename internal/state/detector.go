package state

import (
	"encoding/json"
	"os"

	"github.com/clement/aggtrades/internal/whale"
)

// ReservoirData holds serializable reservoir data with count.
type ReservoirData struct {
	Samples []float64 `json:"s"`
	Count   int64     `json:"c"`
}

// BootstrapData holds serializable bootstrap thresholds.
type BootstrapData struct {
	P99          float64 `json:"p99"`
	P999         float64 `json:"p999"`
	Bootstrapped bool    `json:"bootstrapped"`
}

// DetectorState holds serializable whale detector data.
type DetectorState struct {
	WindowDays    int                      `json:"window_days"`
	SamplesPerDay int                      `json:"samples_per_day"`
	Reservoirs    map[string]ReservoirData `json:"reservoirs"`
	DateOrder     []string                 `json:"date_order"`
	Bootstrap     BootstrapData            `json:"bootstrap,omitempty"`
}

// DetectorExporter interface for exporting detector state.
type DetectorExporter interface {
	ExportState() (windowDays, samplesPerDay int, reservoirs map[string]whale.ReservoirState, dateOrder []string)
	ExportBootstrap() whale.BootstrapState
}

// SaveDetector saves detector state to disk.
func SaveDetector(path string, d DetectorExporter) error {
	windowDays, samplesPerDay, reservoirs, dateOrder := d.ExportState()
	bootstrap := d.ExportBootstrap()

	// Convert to serializable format
	data := make(map[string]ReservoirData, len(reservoirs))
	for date, rs := range reservoirs {
		data[date] = ReservoirData{Samples: rs.Samples, Count: rs.Count}
	}

	state := DetectorState{
		WindowDays:    windowDays,
		SamplesPerDay: samplesPerDay,
		Reservoirs:    data,
		DateOrder:     dateOrder,
		Bootstrap: BootstrapData{
			P99:          bootstrap.Thresholds.P99,
			P999:         bootstrap.Thresholds.P999,
			Bootstrapped: bootstrap.Bootstrapped,
		},
	}

	jsonData, err := json.Marshal(state)
	if err != nil {
		return err
	}

	return atomicWrite(path, jsonData)
}

// LoadDetector loads detector state from disk.
func LoadDetector(path string) (*DetectorState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var state DetectorState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, nil // Corrupted, will rebuild
	}

	return &state, nil
}

// ToWhaleReservoirs converts to whale.ReservoirState map for import.
func (ds *DetectorState) ToWhaleReservoirs() map[string]whale.ReservoirState {
	result := make(map[string]whale.ReservoirState, len(ds.Reservoirs))
	for date, rd := range ds.Reservoirs {
		result[date] = whale.ReservoirState{Samples: rd.Samples, Count: rd.Count}
	}
	return result
}

// ToWhaleBootstrap converts to whale.BootstrapState for import.
func (ds *DetectorState) ToWhaleBootstrap() whale.BootstrapState {
	return whale.BootstrapState{
		Thresholds: whale.Thresholds{
			P99:  ds.Bootstrap.P99,
			P999: ds.Bootstrap.P999,
		},
		Bootstrapped: ds.Bootstrap.Bootstrapped,
	}
}
