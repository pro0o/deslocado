package bitcask

import (
	"bufio"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/pro0o/deslocado/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func createHintFileForTest(hintPath string, entries map[string]int64) error {
	file, err := os.Create(hintPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for key, offset := range entries {
		if err := binary.Write(writer, binary.BigEndian, uint32(len(key))); err != nil {
			return err
		}
		if _, err := writer.Write([]byte(key)); err != nil {
			return err
		}
		if err := binary.Write(writer, binary.BigEndian, uint64(offset)); err != nil {
			return err
		}
	}

	return nil
}

func createLogFileForTest(logPath string, entries []testEntry) error {
	file, err := os.Create(logPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	defer writer.Flush()

	for _, entry := range entries {
		if entry.flag == byte(types.FlagTombstone) {
			err := WriterTombstone(writer, []byte(entry.key))
			if err != nil {
				return err
			}
		} else {
			err := Writer(writer, []byte(entry.key), entry.value)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func TestBuildKeyDir(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	tempDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldDir)

	testCases := []struct {
		name           string
		hintFiles      map[string]map[string]int64
		expectedKeyDir map[string]types.FileOffset
		expectError    bool
	}{
		{
			name: "single_hint_file",
			hintFiles: map[string]map[string]int64{
				"data_compacted_123.hint": {
					"key1": 0,
					"key2": 25,
					"key3": 50,
				},
			},
			expectedKeyDir: map[string]types.FileOffset{
				"key1": {FileID: "data_compacted_123.log", Offset: 0},
				"key2": {FileID: "data_compacted_123.log", Offset: 25},
				"key3": {FileID: "data_compacted_123.log", Offset: 50},
			},
			expectError: false,
		},
		{
			name: "multiple_hint_files",
			hintFiles: map[string]map[string]int64{
				"data_compacted_123.hint": {
					"key1": 0,
					"key2": 25,
				},
				"data_compacted_456.hint": {
					"key3": 0,
					"key4": 30,
				},
			},
			expectedKeyDir: map[string]types.FileOffset{
				"key1": {FileID: "data_compacted_123.log", Offset: 0},
				"key2": {FileID: "data_compacted_123.log", Offset: 25},
				"key3": {FileID: "data_compacted_456.log", Offset: 0},
				"key4": {FileID: "data_compacted_456.log", Offset: 30},
			},
			expectError: false,
		},
		{
			name:           "no_hint_files",
			hintFiles:      map[string]map[string]int64{},
			expectedKeyDir: map[string]types.FileOffset{},
			expectError:    false,
		},
		{
			name: "empty_hint_file",
			hintFiles: map[string]map[string]int64{
				"data_compacted_empty.hint": {},
			},
			expectedKeyDir: map[string]types.FileOffset{},
			expectError:    false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for hintFile, entries := range tc.hintFiles {
				if err := createHintFileForTest(hintFile, entries); err != nil {
					t.Fatalf("Failed to create hint file %s: %v", hintFile, err)
				}
			}

			keyDir, err := BuildKeyDir()

			if tc.expectError && err == nil {
				t.Error("Expected error but got none")
			}
			if !tc.expectError && err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(keyDir) != len(tc.expectedKeyDir) {
				t.Errorf("Expected %d keys in keyDir, got %d", len(tc.expectedKeyDir), len(keyDir))
			}

			for expectedKey, expectedOffset := range tc.expectedKeyDir {
				actualOffset, exists := keyDir[expectedKey]
				if !exists {
					t.Errorf("Expected key %q not found in keyDir", expectedKey)
					continue
				}

				if actualOffset.FileID != expectedOffset.FileID {
					t.Errorf("Key %q: expected FileID %q, got %q", expectedKey, expectedOffset.FileID, actualOffset.FileID)
				}

				if actualOffset.Offset != expectedOffset.Offset {
					t.Errorf("Key %q: expected Offset %d, got %d", expectedKey, expectedOffset.Offset, actualOffset.Offset)
				}
			}

			files, _ := filepath.Glob("*")
			for _, file := range files {
				os.Remove(file)
			}
		})
	}
}

func TestBuildKeyDirFileOperations(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	tempDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldDir)

	t.Run("corrupted_hint_file", func(t *testing.T) {
		file, err := os.Create("data_corrupted.hint")
		if err != nil {
			t.Fatalf("Failed to create corrupted hint file: %v", err)
		}

		writer := bufio.NewWriter(file)
		binary.Write(writer, binary.BigEndian, uint32(5))
		writer.Flush()
		file.Close()

		_, err = BuildKeyDir()
		if err == nil {
			t.Error("Expected error when reading corrupted hint file")
		}

		os.Remove("data_corrupted.hint")
	})

	t.Run("file_permissions", func(t *testing.T) {
		hintEntries := map[string]int64{
			"test_key": 100,
		}

		if err := createHintFileForTest("data_test.hint", hintEntries); err != nil {
			t.Fatalf("Failed to create hint file: %v", err)
		}

		if err := os.Chmod("data_test.hint", 0000); err == nil {
			_, err := BuildKeyDir()
			if err == nil {
				t.Error("Expected error when hint file is unreadable")
			}

			os.Chmod("data_test.hint", 0644)
		}

		os.Remove("data_test.hint")
	})
}

func TestBuildKeyDirWithRealData(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	tempDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldDir)

	logEntries := []testEntry{
		{flag: byte(types.FlagNormal), key: "user:1", value: []byte("john_doe")},
		{flag: byte(types.FlagNormal), key: "user:2", value: []byte("jane_smith")},
		{flag: byte(types.FlagNormal), key: "config:timeout", value: []byte("30s")},
	}

	if err := createLogFileForTest("data_compacted_real.log", logEntries); err != nil {
		t.Fatalf("Failed to create log file: %v", err)
	}

	hintEntries := map[string]int64{
		"user:1":         0,
		"user:2":         22,
		"config:timeout": 46,
	}

	if err := createHintFileForTest("data_compacted_real.hint", hintEntries); err != nil {
		t.Fatalf("Failed to create hint file: %v", err)
	}

	keyDir, err := BuildKeyDir()
	if err != nil {
		t.Fatalf("BuildKeyDir failed: %v", err)
	}

	expectedKeys := []string{"user:1", "user:2", "config:timeout"}
	for _, key := range expectedKeys {
		fileOffset, exists := keyDir[key]
		if !exists {
			t.Errorf("Key %q not found in keyDir", key)
			continue
		}

		if fileOffset.FileID != "data_compacted_real.log" {
			t.Errorf("Key %q: expected FileID 'data_compacted_real.log', got %q", key, fileOffset.FileID)
		}

		if fileOffset.Offset < 0 {
			t.Errorf("Key %q: invalid negative offset %d", key, fileOffset.Offset)
		}
	}

	os.Remove("data_compacted_real.log")
	os.Remove("data_compacted_real.hint")
}
