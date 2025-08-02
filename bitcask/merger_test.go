package bitcask

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/pro0o/deslocado/types"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func createTestLogFile(path string, entries []testEntry) error {
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
				log.Error().Err(err).Msg("Error while writing tombstone...")
				return err
			}
		} else {
			err := Writer(writer, []byte(entry.key), entry.value)
			if err != nil {
				log.Error().Err(err).Msg("Error while writing...")
				return err
			}
		}
	}

	return nil
}

type testEntry struct {
	flag  byte
	key   string
	value []byte
}

func readCompactedFile(path string) (map[string][]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := bufio.NewReader(file)
	result := make(map[string][]byte)

	for {
		flag, err := reader.ReadByte()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		var keyLen, valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}

		keyBuffer := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuffer); err != nil {
			return nil, err
		}

		valBuffer := make([]byte, valLen)
		if _, err := io.ReadFull(reader, valBuffer); err != nil {
			return nil, err
		}

		if flag != byte(types.FlagTombstone) {
			result[string(keyBuffer)] = valBuffer
		}
	}

	return result, nil
}

func mockSortedLogs(pattern string) ([]string, error) {
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	return matches, nil
}

func TestMerger(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	tempDir := t.TempDir()
	oldDir, _ := os.Getwd()
	os.Chdir(tempDir)
	defer os.Chdir(oldDir)

	testCases := []struct {
		name     string
		logFiles [][]testEntry
		expected map[string][]byte
	}{
		{
			name: "basic_merge",
			logFiles: [][]testEntry{
				{
					{flag: byte(types.FlagNormal), key: "key1", value: []byte("value1")},
					{flag: byte(types.FlagNormal), key: "key2", value: []byte("value2")},
				},
				{
					{flag: byte(types.FlagNormal), key: "key3", value: []byte("value3")},
					{flag: byte(types.FlagNormal), key: "key4", value: []byte("value4")},
				},
			},
			expected: map[string][]byte{
				"key1": []byte("value1"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
				"key4": []byte("value4"),
			},
		},
		{
			name: "overwrites_same_key",
			logFiles: [][]testEntry{
				{
					{flag: byte(types.FlagNormal), key: "key1", value: []byte("old_value")},
					{flag: byte(types.FlagNormal), key: "key2", value: []byte("value2")},
				},
				{
					{flag: byte(types.FlagNormal), key: "key1", value: []byte("new_value")},
					{flag: byte(types.FlagNormal), key: "key3", value: []byte("value3")},
				},
			},
			expected: map[string][]byte{
				"key1": []byte("new_value"),
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
		},
		{
			name: "handles_tombstones",
			logFiles: [][]testEntry{
				{
					{flag: byte(types.FlagNormal), key: "key1", value: []byte("value1")},
					{flag: byte(types.FlagNormal), key: "key2", value: []byte("value2")},
				},
				{
					{flag: byte(types.FlagTombstone), key: "key1"},
					{flag: byte(types.FlagNormal), key: "key3", value: []byte("value3")},
				},
			},
			expected: map[string][]byte{
				"key2": []byte("value2"),
				"key3": []byte("value3"),
			},
		},
		{
			name: "empty_logs",
			logFiles: [][]testEntry{
				{},
				{},
			},
			expected: map[string][]byte{},
		},
		{
			name: "single_log_file",
			logFiles: [][]testEntry{
				{
					{flag: byte(types.FlagNormal), key: "solo_key", value: []byte("solo_value")},
				},
			},
			expected: map[string][]byte{
				"solo_key": []byte("solo_value"),
			},
		},
		{
			name: "complex_scenario",
			logFiles: [][]testEntry{
				{
					{flag: byte(types.FlagNormal), key: "a", value: []byte("1")},
					{flag: byte(types.FlagNormal), key: "b", value: []byte("2")},
					{flag: byte(types.FlagNormal), key: "c", value: []byte("3")},
				},
				{
					{flag: byte(types.FlagNormal), key: "a", value: []byte("updated_a")},
					{flag: byte(types.FlagTombstone), key: "b"},
					{flag: byte(types.FlagNormal), key: "d", value: []byte("4")},
				},
				{
					{flag: byte(types.FlagNormal), key: "e", value: []byte("5")},
					{flag: byte(types.FlagTombstone), key: "c"},
				},
			},
			expected: map[string][]byte{
				"a": []byte("updated_a"),
				"d": []byte("4"),
				"e": []byte("5"),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			for i, entries := range tc.logFiles {
				logPath := filepath.Join(tempDir, "data_"+string(rune('0'+i))+".log")
				if err := createTestLogFile(logPath, entries); err != nil {
					t.Fatalf("Failed to create test log file: %v", err)
				}
			}

			logPaths, err := mockSortedLogs("data_*.log")
			if err != nil {
				t.Fatalf("Failed to get sorted logs: %v", err)
			}

			if err := Merger(logPaths); err != nil {
				t.Fatalf("Merger failed: %v", err)
			}

			compactedPath := "compacted_data.txt"
			actual, err := readCompactedFile(compactedPath)
			if err != nil {
				t.Fatalf("Failed to read compacted file: %v", err)
			}

			if len(actual) != len(tc.expected) {
				t.Errorf("Expected %d entries, got %d", len(tc.expected), len(actual))
			}

			for key, expectedValue := range tc.expected {
				actualValue, exists := actual[key]
				if !exists {
					t.Errorf("Expected key %q not found in result", key)
					continue
				}
				if string(actualValue) != string(expectedValue) {
					t.Errorf("For key %q: expected %q, got %q", key, expectedValue, actualValue)
				}
			}

			for key := range actual {
				if _, exists := tc.expected[key]; !exists {
					t.Errorf("Unexpected key %q found in result", key)
				}
			}

			os.Remove(compactedPath)
			for i := range tc.logFiles {
				os.Remove("data_" + string(rune('0'+i)) + ".log")
			}
		})
	}
}
