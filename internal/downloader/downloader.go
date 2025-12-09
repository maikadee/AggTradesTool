package downloader

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

const (
	// HTTP settings
	downloadTimeout = 5 * time.Minute
	bufferSize      = 32 * 1024 // 32KB buffer for streaming
	maxRetries      = 3
)

// Result represents the result of a download
type Result struct {
	Month    string
	ZipPath  string
	ZipSize  int64
	CSVPath  string
	CSVSize  int64
	Error    error
	Skipped  bool // True if file already existed
}

// ProgressCallback is called periodically during download with bytes downloaded
type ProgressCallback func(bytesDownloaded int64)

// Download downloads a ZIP file from URL to the specified path
// Uses streaming to minimize memory usage
// Downloads to a .tmp file first, then renames atomically on success
func Download(ctx context.Context, url, destPath string, progressCb ProgressCallback) (int64, error) {
	tmpPath := destPath + ".tmp"

	// Create HTTP request
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("create request: %w", err)
	}

	// Execute request
	client := &http.Client{Timeout: downloadTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("http request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http status %d", resp.StatusCode)
	}

	// Create temp file
	file, err := os.Create(tmpPath)
	if err != nil {
		return 0, fmt.Errorf("create file: %w", err)
	}

	// Stream download with progress tracking
	var totalBytes int64
	buf := make([]byte, bufferSize)
	lastProgress := time.Now()
	var downloadErr error

	for {
		select {
		case <-ctx.Done():
			file.Close()
			os.Remove(tmpPath)
			return 0, ctx.Err()
		default:
		}

		n, err := resp.Body.Read(buf)
		if n > 0 {
			if _, writeErr := file.Write(buf[:n]); writeErr != nil {
				file.Close()
				os.Remove(tmpPath)
				return 0, fmt.Errorf("write file: %w", writeErr)
			}
			totalBytes += int64(n)

			// Report progress every 100ms
			if progressCb != nil && time.Since(lastProgress) > 100*time.Millisecond {
				progressCb(totalBytes)
				lastProgress = time.Now()
			}
		}

		if err == io.EOF {
			break
		}
		if err != nil {
			downloadErr = fmt.Errorf("read response: %w", err)
			break
		}
	}

	// Close file before rename
	file.Close()

	if downloadErr != nil {
		os.Remove(tmpPath)
		return 0, downloadErr
	}

	// Atomic rename: tmp -> final
	if err := os.Rename(tmpPath, destPath); err != nil {
		os.Remove(tmpPath)
		return 0, fmt.Errorf("rename temp file: %w", err)
	}

	// Final progress callback
	if progressCb != nil {
		progressCb(totalBytes)
	}

	return totalBytes, nil
}

// DownloadWithRetry downloads with exponential backoff retry
func DownloadWithRetry(ctx context.Context, url, destPath string, progressCb ProgressCallback) (int64, error) {
	var lastErr error
	delay := time.Second

	for attempt := 0; attempt < maxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return 0, ctx.Err()
			case <-time.After(delay):
			}
			delay *= 2
		}

		size, err := Download(ctx, url, destPath, progressCb)
		if err == nil {
			return size, nil
		}

		lastErr = err
		// Clean up partial file
		os.Remove(destPath)
	}

	return 0, fmt.Errorf("after %d retries: %w", maxRetries, lastErr)
}

// GetFileSize performs a HEAD request to get the file size without downloading
func GetFileSize(ctx context.Context, url string) (int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodHead, url, nil)
	if err != nil {
		return 0, err
	}

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("http status %d", resp.StatusCode)
	}

	return resp.ContentLength, nil
}

// FileExists checks if a file exists and returns its size
func FileExists(path string) (bool, int64) {
	info, err := os.Stat(path)
	if err != nil {
		return false, 0
	}
	return true, info.Size()
}
