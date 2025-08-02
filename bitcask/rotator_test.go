package bitcask

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/pro0o/deslocado/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func createDataFile(path string, entries []testEntry) error {
	file, err := os.Create(path)
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

func createExistingLogs(tempDir string, count int) error {
	for i := range count {
		logPath := filepath.Join(tempDir, "data_"+string(rune('0'+i))+".log")
		entries := []testEntry{
			{flag: byte(types.FlagNormal), key: "old_key_" + string(rune('0'+i)), value: []byte("old_value_" + string(rune('0'+i)))},
		}
		if err := createDataFile(logPath, entries); err != nil {
			return err
		}
	}
	return nil
}

func readHintFile(path string) (map[string]int64, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	hints := make(map[string]int64)

	for {
		var keyLen uint32
		if err := binary.Read(file, binary.BigEndian, &keyLen); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		keyBuffer := make([]byte, keyLen)
		if _, err := io.ReadFull(file, keyBuffer); err != nil {
			return nil, err
		}

		var offset uint64
		if err := binary.Read(file, binary.BigEndian, &offset); err != nil {
			return nil, err
		}

		hints[string(keyBuffer)] = int64(offset)
	}

	return hints, nil
}

func countFiles(pattern string) int {
	matches, _ := filepath.Glob(pattern)
	return len(matches)
}

func createMockKeyDir(entries []testEntry) map[string]types.FileOffset {
	keyDir := make(map[string]types.FileOffset)
	offset := int64(0)

	for _, entry := range entries {
		if entry.flag != byte(types.FlagTombstone) {
			keyDir[entry.key] = types.FileOffset{
				FileID: "data.txt",
				Offset: offset,
			}
			offset += int64(1 + 4 + 4 + len(entry.key) + len(entry.value))
		}
	}

	return keyDir
}

func TestRotator(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	tempDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldDir)

	testCases := []struct {
		name             string
		initialData      []testEntry
		existingLogCount int
		expectMerge      bool
		expectedKeys     []string
	}{
		{
			name: "basic_rotation_no_merge",
			initialData: []testEntry{
				{flag: byte(types.FlagNormal), key: "current_key1", value: []byte("current_value1")},
				{flag: byte(types.FlagNormal), key: "current_key2", value: []byte("current_value2")},
			},
			existingLogCount: 1,
			expectMerge:      false,
			expectedKeys:     []string{"current_key1", "current_key2"},
		},
		{
			name: "rotation_with_merge",
			initialData: []testEntry{
				{flag: byte(types.FlagNormal), key: "active_key1", value: []byte("active_value1")},
				{flag: byte(types.FlagNormal), key: "active_key2", value: []byte("active_value2")},
			},
			existingLogCount: MAX_IMMUTABLES,
			expectMerge:      true,
			expectedKeys:     []string{"active_key1", "active_key2", "old_key_0", "old_key_1", "old_key_2"},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if err := createDataFile("data.txt", tc.initialData); err != nil {
				t.Fatalf("Failed to create data.txt: %v", err)
			}

			if err := createExistingLogs(tempDir, tc.existingLogCount); err != nil {
				t.Fatalf("Failed to create existing logs: %v", err)
			}

			file, err := os.OpenFile("data.txt", os.O_RDWR|os.O_APPEND, 0644)
			if err != nil {
				t.Fatalf("Failed to open data.txt: %v", err)
			}
			oldWriter := bufio.NewWriter(file)

			// Create mock keyDir for the test
			keyDir := createMockKeyDir(tc.initialData)

			newWriter, err := Rotator(oldWriter, keyDir)
			if err != nil {
				t.Fatalf("Rotator failed: %v", err)
			}

			if newWriter == nil {
				t.Fatal("Expected new writer, got nil")
			}

			if _, err := os.Stat("data.txt"); os.IsNotExist(err) {
				t.Error("Expected new data.txt to exist")
			}

			logCount := countFiles("data_*.log")
			if tc.expectMerge {
				if logCount != 1 {
					t.Errorf("Expected 1 log file after merge, got %d", logCount)
				}

				compactedCount := countFiles("data_compacted_*.log")
				if compactedCount != 1 {
					t.Errorf("Expected 1 compacted log file, got %d", compactedCount)
				}

				hintCount := countFiles("*.hint")
				if hintCount != 1 {
					t.Errorf("Expected 1 hint file, got %d", hintCount)
				}

				hintFiles, _ := filepath.Glob("*.hint")
				if len(hintFiles) > 0 {
					hints, err := readHintFile(hintFiles[0])
					if err != nil {
						t.Fatalf("Failed to read hint file: %v", err)
					}

					for _, expectedKey := range tc.expectedKeys {
						if _, exists := hints[expectedKey]; !exists {
							t.Errorf("Expected key %q not found in hint file", expectedKey)
						}
					}
				}
			} else {
				expectedLogCount := tc.existingLogCount + 1
				if logCount != expectedLogCount {
					t.Errorf("Expected %d log files, got %d", logCount, expectedLogCount)
				}
			}

			files, _ := filepath.Glob("*")
			for _, file := range files {
				os.Remove(file)
			}
		})
	}
}

func TestRotatorFileOperations(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	tempDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldDir)

	initialData := []testEntry{
		{flag: byte(types.FlagNormal), key: "test_key", value: []byte("test_value")},
	}

	if err := createDataFile("data.txt", initialData); err != nil {
		t.Fatalf("Failed to create data.txt: %v", err)
	}

	file, err := os.OpenFile("data.txt", os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open data.txt: %v", err)
	}
	oldWriter := bufio.NewWriter(file)

	keyDir := createMockKeyDir(initialData)

	_, err = Rotator(oldWriter, keyDir)
	if err != nil {
		t.Fatalf("Rotator failed: %v", err)
	}

	renamedFiles, _ := filepath.Glob("data_*.log")
	if len(renamedFiles) != 1 {
		t.Errorf("Expected 1 renamed file, got %d", len(renamedFiles))
	}

	if len(renamedFiles) > 0 {
		filename := renamedFiles[0]
		if !strings.HasPrefix(filename, "data_") || !strings.HasSuffix(filename, ".log") {
			t.Errorf("Unexpected filename format: %s", filename)
		}
	}

	if _, err := os.Stat("data.txt"); os.IsNotExist(err) {
		t.Error("Expected new data.txt to be created")
	}
}

func TestRotatorHintFileGeneration(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	tempDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldDir)

	initialData := []testEntry{
		{flag: byte(types.FlagNormal), key: "hint_key1", value: []byte("hint_value1")},
		{flag: byte(types.FlagNormal), key: "hint_key2", value: []byte("hint_value2")},
	}

	if err := createDataFile("data.txt", initialData); err != nil {
		t.Fatalf("Failed to create data.txt: %v", err)
	}

	if err := createExistingLogs(tempDir, MAX_IMMUTABLES); err != nil {
		t.Fatalf("Failed to create existing logs: %v", err)
	}

	file, err := os.OpenFile("data.txt", os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open data.txt: %v", err)
	}
	oldWriter := bufio.NewWriter(file)

	keyDir := createMockKeyDir(initialData)

	_, err = Rotator(oldWriter, keyDir)
	if err != nil {
		t.Fatalf("Rotator failed: %v", err)
	}

	hintFiles, _ := filepath.Glob("*.hint")
	if len(hintFiles) != 1 {
		t.Fatalf("Expected 1 hint file, got %d", len(hintFiles))
	}

	hints, err := readHintFile(hintFiles[0])
	if err != nil {
		t.Fatalf("Failed to read hint file: %v", err)
	}

	expectedKeys := []string{"hint_key1", "hint_key2", "old_key_0", "old_key_1", "old_key_2"}
	for _, key := range expectedKeys {
		if offset, exists := hints[key]; !exists {
			t.Errorf("Expected key %q not found in hints", key)
		} else if offset < 0 {
			t.Errorf("Invalid offset for key %q: %d", key, offset)
		}
	}
}
