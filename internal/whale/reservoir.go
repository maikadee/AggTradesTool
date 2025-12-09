package whale

import (
	"math/rand"
)

// Reservoir implements reservoir sampling for collecting representative samples
// with bounded memory usage
type Reservoir struct {
	samples  []float64
	capacity int
	count    int64 // Total items seen
	rng      *rand.Rand
}

// NewReservoir creates a new reservoir with the specified capacity
func NewReservoir(capacity int) *Reservoir {
	return &Reservoir{
		samples:  make([]float64, 0, capacity),
		capacity: capacity,
		rng:      rand.New(rand.NewSource(42)), // Fixed seed for reproducibility
	}
}

// Add adds a value to the reservoir using reservoir sampling algorithm
func (r *Reservoir) Add(value float64) {
	r.count++

	// If we haven't filled the reservoir yet, just append
	if len(r.samples) < r.capacity {
		r.samples = append(r.samples, value)
		return
	}

	// Otherwise, randomly replace an existing sample
	// Probability of replacement: capacity / count
	j := r.rng.Int63n(r.count)
	if j < int64(r.capacity) {
		r.samples[j] = value
	}
}

// Samples returns a copy of the current samples
func (r *Reservoir) Samples() []float64 {
	result := make([]float64, len(r.samples))
	copy(result, r.samples)
	return result
}

// Count returns the total number of items seen
func (r *Reservoir) Count() int64 {
	return r.count
}

// Size returns the current number of samples stored
func (r *Reservoir) Size() int {
	return len(r.samples)
}

// Reset clears the reservoir
func (r *Reservoir) Reset() {
	r.samples = r.samples[:0]
	r.count = 0
}

// Merge combines samples from another reservoir
// Uses weighted sampling based on counts
func (r *Reservoir) Merge(other *Reservoir) {
	if other.Size() == 0 {
		return
	}

	totalCount := r.count + other.count
	otherSamples := other.Samples()

	for _, val := range otherSamples {
		// Probability of including this sample: proportional to other's count
		if r.rng.Float64() < float64(other.count)/float64(totalCount) {
			if len(r.samples) < r.capacity {
				r.samples = append(r.samples, val)
			} else {
				j := r.rng.Intn(r.capacity)
				r.samples[j] = val
			}
		}
	}

	r.count = totalCount
}

// NewReservoirFromSamples creates a reservoir from existing samples with known count.
func NewReservoirFromSamples(capacity int, samples []float64, count int64) *Reservoir {
	r := &Reservoir{
		samples:  make([]float64, len(samples)),
		capacity: capacity,
		count:    count,
		rng:      rand.New(rand.NewSource(42)),
	}
	copy(r.samples, samples)
	return r
}

// ExportState returns samples and total count for serialization.
func (r *Reservoir) ExportState() ([]float64, int64) {
	samples := make([]float64, len(r.samples))
	copy(samples, r.samples)
	return samples, r.count
}
