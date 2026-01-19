package dlog

import (
	"Mix/config"
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// FileLog is a simple line-based file log
type FileLog struct {
	filename string
	file     *os.File
	writer   *bufio.Writer
	mu       sync.Mutex
}

// OpenFileLog opens (or creates) a file for appending
func OpenFileLog(filename string) (*FileLog, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return &FileLog{
		filename: filename,
		file:     file,
		writer:   bufio.NewWriter(file),
	}, nil
}

// AppendLine appends a single line to the file
func (f *FileLog) AppendLine(line string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, err := f.writer.WriteString(line + "\n"); err != nil {
		return err
	}
	return f.writer.Flush()
}

// StoreLine overwrites the first line of the file with the given line
func (f *FileLog) StoreLine(line string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	// Read all existing lines
	data, err := os.ReadFile(f.filename)
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	lines := strings.Split(string(data), "\n")
	if len(lines) == 0 || lines[0] == "" {
		// file empty or first line empty
		lines = []string{line}
	} else {
		lines[0] = line
	}

	// Join lines and write back
	newData := strings.Join(lines, "\n")
	return os.WriteFile(f.filename, []byte(newData), 0644)
}

// ReadLines reads all lines from the file
func (f *FileLog) ReadLines() ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if _, err := f.file.Seek(0, 0); err != nil { // rewind
		return nil, err
	}

	scanner := bufio.NewScanner(f.file)
	var lines []string
	for scanner.Scan() {
		lines = append(lines, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return lines, nil
}

// Close closes the file
func (f *FileLog) Close() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.writer != nil {
		if err := f.writer.Flush(); err != nil {
			return err
		}
	}
	if f.file != nil {
		return f.file.Close()
	}
	return nil
}

func ReadLog(id int32) int32 {
	logsize := int32(0)
	if config.LogFile {
		filename := fmt.Sprintf("%v.txt", id)
		fileLog, err := OpenFileLog(filename)
		if err != nil {
			Info("Error while opening log: %v", err)
		}

		// read lines from file
		lines, err := fileLog.ReadLines()
		if err != nil {
			Info("Error while reading log: %v", err)
		}

		if len(lines) > 0 {
			// first line stores logSize
			if n, err := strconv.Atoi(lines[0]); err == nil {
				logsize = int32(n)
			}
		}
		fileLog.Close()
	}
	return logsize
}

func StoreLog(id int32, line string) error {
	if config.LogFile {
		filename := fmt.Sprintf("%v.txt", id)
		data, err := os.ReadFile(filename)
		if err != nil && !os.IsNotExist(err) {
			return err
		}

		lines := strings.Split(string(data), "\n")
		if len(lines) == 0 || lines[0] == "" {
			// file empty or first line empty
			lines = []string{line}
		} else {
			lines[0] = line
		}

		// Join lines and write back
		newData := strings.Join(lines, "\n")
		return os.WriteFile(filename, []byte(newData), 0644)
	}
	return nil
}
