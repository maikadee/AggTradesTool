package downloader

import (
	"archive/zip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Extract extracts a ZIP file to the specified CSV path
// Returns the size of the extracted CSV file
// Extracts to a .tmp file first, then renames atomically on success
func Extract(zipPath, csvPath string) (int64, error) {
	tmpPath := csvPath + ".tmp"

	// Open the ZIP file
	reader, err := zip.OpenReader(zipPath)
	if err != nil {
		return 0, fmt.Errorf("open zip: %w", err)
	}
	defer reader.Close()

	// Find the CSV file in the ZIP
	if len(reader.File) == 0 {
		return 0, fmt.Errorf("empty zip file")
	}

	// Take the first file (should be the only one)
	zipFile := reader.File[0]

	// Open the file inside the ZIP
	src, err := zipFile.Open()
	if err != nil {
		return 0, fmt.Errorf("open file in zip: %w", err)
	}
	defer src.Close()

	// Create temp file
	dst, err := os.Create(tmpPath)
	if err != nil {
		return 0, fmt.Errorf("create csv: %w", err)
	}

	// Copy with buffering
	buf := make([]byte, 1024*1024) // 1MB buffer
	var totalBytes int64
	var extractErr error

	for {
		n, err := src.Read(buf)
		if n > 0 {
			if _, writeErr := dst.Write(buf[:n]); writeErr != nil {
				dst.Close()
				os.Remove(tmpPath)
				return 0, fmt.Errorf("write csv: %w", writeErr)
			}
			totalBytes += int64(n)
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			extractErr = fmt.Errorf("read zip: %w", err)
			break
		}
	}

	// Close file before rename
	dst.Close()

	if extractErr != nil {
		os.Remove(tmpPath)
		return 0, extractErr
	}

	// Atomic rename: tmp -> final
	if err := os.Rename(tmpPath, csvPath); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("rename temp file: %w", err)
	}

	return totalBytes, nil
}

// ExtractAndRemoveZip extracts and then removes the ZIP file
func ExtractAndRemoveZip(zipPath, csvPath string) (int64, error) {
	size, err := Extract(zipPath, csvPath)
	if err != nil {
		return 0, err
	}

	// Remove the ZIP file
	if err := os.Remove(zipPath); err != nil {
		// Non-fatal, just log
		fmt.Fprintf(os.Stderr, "warning: failed to remove zip %s: %v\n", zipPath, err)
	}

	return size, nil
}

// CleanupTempFiles removes all temporary files for a month
func CleanupTempFiles(tempDir, month string) {
	patterns := []string{
		filepath.Join(tempDir, month+".zip"),
		filepath.Join(tempDir, month+".csv"),
	}

	for _, pattern := range patterns {
		os.Remove(pattern)
	}
}
