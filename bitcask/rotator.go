package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"time"

	"github.com/gofrs/flock"
	"github.com/pro0o/deslocado/types"
	"github.com/rs/zerolog/log"
)

const MAX_IMMUTABLES = 3

func createHintFile(compactedLog string) error {
	compact, err := os.Open(compactedLog)
	if err != nil {
		return fmt.Errorf("open compacted log: %w", err)
	}
	defer compact.Close()

	offsets := make(map[string]int64)

	for {
		off, err := compact.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}

		var flag byte
		if err := binary.Read(compact, binary.BigEndian, &flag); err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		if flag != byte(types.FlagNormal) {
			return fmt.Errorf("unexpected tombstone in compacted file at offset %d", off)
		}

		var keyLen, valLen uint32
		if err := binary.Read(compact, binary.BigEndian, &keyLen); err != nil {
			return err
		}
		if err := binary.Read(compact, binary.BigEndian, &valLen); err != nil {
			return err
		}

		keyBuffer := make([]byte, keyLen)
		if _, err := io.ReadFull(compact, keyBuffer); err != nil {
			return err
		}

		key := string(keyBuffer)
		offsets[key] = off

		// skip over to next kv via val.
		if _, err := compact.Seek(int64(valLen), io.SeekCurrent); err != nil {
			return err
		}
	}

	hint := fmt.Sprintf("%s.hint", compactedLog)
	hintFile, err := os.Create(hint)
	if err != nil {
		return fmt.Errorf("create hint file: %w", err)
	}

	for key, offset := range offsets {
		if err := binary.Write(hintFile, binary.BigEndian, uint32(len(key))); err != nil {
			return fmt.Errorf("write key length to hint: %w", err)
		}
		if _, err := hintFile.Write([]byte(key)); err != nil {
			return fmt.Errorf("write key to hint: %w", err)
		}
		if err := binary.Write(hintFile, binary.BigEndian, uint64(offset)); err != nil {
			return fmt.Errorf("write offset to hint: %w", err)
		}
	}

	hintFile.Close()
	return nil
}

func cleanupOldFiles(logs []string, compactedLog string) error {
	for _, oldLog := range logs {
		if oldLog != compactedLog {
			if err := os.Remove(oldLog); err != nil {
				log.Warn().Err(err).Str("file", oldLog).Msg("Failed to delete old log")
			}
		}
	}

	hints, _ := filepath.Glob("data*.hint")
	currentHint := fmt.Sprintf("%s.hint", compactedLog)
	for _, oldHint := range hints {
		if oldHint != currentHint {
			os.Remove(oldHint)
		}
	}

	return nil
}

// rotate -> gen immutables
// if immutables threshold -> merging
// immutables.log -> compacted.txt
// compacted.txt -> compacted.log
// compacted.log -> compacted.hint
func Rotator(oldWriter *bufio.Writer, keyDir map[string]types.FileOffset) (*bufio.Writer, error) {
	if err := oldWriter.Flush(); err != nil {
		return oldWriter, fmt.Errorf("flush old writer: %w", err)
	}

	lock := flock.New("data.txt.lock")
	if err := lock.Lock(); err != nil {
		return oldWriter, fmt.Errorf("lock file: %w", err)
	}
	defer lock.Unlock()

	log.Info().Msg("Rotation started!!")

	newLog := fmt.Sprintf("data_%d.log", time.Now().Unix())
	if err := os.Rename("data.txt", newLog); err != nil {
		return oldWriter, fmt.Errorf("rename file: %w", err)
	}
	log.Info().Msg("Immutable created!!")

	freshFile, err := os.OpenFile("data.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return oldWriter, fmt.Errorf("open new data.txt: %w", err)
	}
	newWriter := bufio.NewWriter(freshFile)

	logs, err := sorted("data_*.log")
	if err != nil {
		return newWriter, fmt.Errorf("failed to sort: %w", err)
	}

	if len(logs) >= MAX_IMMUTABLES {

		if err := Merger(logs); err != nil {
			return newWriter, fmt.Errorf("merging logs: %w", err)
		}

		compactedLog := fmt.Sprintf("data_compacted_%d.log", time.Now().Unix())
		if err := os.Rename("compacted_data.txt", compactedLog); err != nil {
			return newWriter, fmt.Errorf("rename compacted data: %w", err)
		}

		if err := createHintFile(compactedLog); err != nil {
			return newWriter, fmt.Errorf("create hint file: %w", err)
		}
		log.Info().Msg("Hint Files Generated!!")

		log.Info().Msg("Cleaning up the stale hints & logs!!")
		if err := cleanupOldFiles(logs, compactedLog); err != nil {
			log.Warn().Err(err).Msg("Failed to cleanup old files")
		}

		freshKeyDir, err := BuildKeyDir()
		if err != nil {
			return newWriter, nil
		}

		clear(keyDir)
		maps.Copy(keyDir, freshKeyDir)

	} else {
		log.Info().Msgf("No merge needed. Current log count: %d, threshold: %d", len(logs), MAX_IMMUTABLES)
	}

	log.Info().Msg("Rotation Complete!!")
	return newWriter, nil
}
