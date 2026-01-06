package dlog

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

type FileLogger struct {
	logFile   *os.File
	logIndex  int
	logFolder string
}

// NewLogger initializes a new logger, creating a new log file with the next index.
func NewLogger(logFolder string) (*FileLogger, error) {
	// Ensure the log folder exists
	err := os.MkdirAll(logFolder, 0755)
	if err != nil {
		return nil, fmt.Errorf("failed to create log folder: %v", err)
	}

	// Find the latest log index
	latestIndex := 0
	err = filepath.Walk(logFolder, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() && strings.HasSuffix(info.Name(), ".log") {
			base := strings.TrimSuffix(info.Name(), ".log")
			index, err := strconv.Atoi(base)
			if err == nil && index > latestIndex {
				latestIndex = index
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("failed to scan log folder: %v", err)
	}

	// Create the next log file
	nextIndex := latestIndex + 1
	logFilePath := filepath.Join(logFolder, fmt.Sprintf("%d.log", nextIndex))
	logFile, err := os.Create(logFilePath)
	if err != nil {
		return nil, fmt.Errorf("failed to create log file: %v", err)
	}

	return &FileLogger{
		logFile:   logFile,
		logIndex:  nextIndex,
		logFolder: logFolder,
	}, nil
}

// Append writes a log message to the current log file.
func (fl *FileLogger) Append(message string) error {
	if fl.logFile == nil {
		return fmt.Errorf("log file is not initialized")
	}
	_, err := fl.logFile.WriteString(message + "\n")
	return err
}

// Close closes the current log file.
func (fl *FileLogger) Close() error {
	if fl.logFile != nil {
		return fl.logFile.Close()
	}
	return nil
}
