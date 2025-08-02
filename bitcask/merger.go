package bitcask

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pro0o/deslocado/types"
	"github.com/rs/zerolog/log"
)

func processImmutable(logPath string, fresh map[string]types.KeyState) (map[string]types.KeyState, error) {
	file, err := os.Open(logPath)
	if err != nil {
		return fresh, fmt.Errorf("opening log file %s: %w", logPath, err)
	}
	defer file.Close()

	reader := bufio.NewReader(file)

	for {
		flag, err := reader.ReadByte()
		if err == io.EOF {
			break
		} else if err != nil {
			return fresh, fmt.Errorf("reading flag from %s: %w", logPath, err)
		}

		var keyLen, valLen uint32
		if err := binary.Read(reader, binary.BigEndian, &keyLen); err != nil {
			return fresh, fmt.Errorf("reading keyLen from %s: %w", logPath, err)
		}

		if err := binary.Read(reader, binary.BigEndian, &valLen); err != nil {
			return fresh, fmt.Errorf("reading valLen from %s: %w", logPath, err)
		}

		keyBuffer := make([]byte, keyLen)
		if _, err := io.ReadFull(reader, keyBuffer); err != nil {
			return fresh, fmt.Errorf("reading key bytes from %s: %w", logPath, err)
		}

		// key -> latest
		key := string(keyBuffer)
		if _, seen := fresh[key]; seen {
			if flag == byte(types.FlagNormal) && valLen > 0 {
				if _, err := reader.Discard(int(valLen)); err != nil {
					return fresh, fmt.Errorf("discarding stale value for key %q in %s: %w", key, logPath, err)
				}
			}
			continue
		}

		// key -> latest -> val
		if flag == byte(types.FlagTombstone) {
			fresh[key] = types.KeyState{Val: nil, FlagTombstone: true}
		} else {
			valBuffer := make([]byte, valLen)
			if _, err := io.ReadFull(reader, valBuffer); err != nil {
				return fresh, fmt.Errorf("reading value bytes for key %q from %s: %w", key, logPath, err)
			}
			fresh[key] = types.KeyState{Val: valBuffer, FlagTombstone: false}
		}
	}
	return fresh, nil
}

// take immutables
// process each immuatble and create a fresh immutable file.
// append this fresh -> compacted_data.txt
func Merger(sorted []string) error {
	log.Info().Msg("Merging started!!")
	fresh := make(map[string]types.KeyState)
	var err error

	log.Info().Msg("Processing the Immutables!!")
	for i := len(sorted) - 1; i >= 0; i-- {
		logPath := sorted[i]
		fresh, err = processImmutable(logPath, fresh)
		if err != nil {
			return fmt.Errorf("merging log file %s: %w", logPath, err)
		}
	}

	log.Info().Msg("Compacting the Immutables!!")
	compact, err := os.OpenFile("compacted_data.txt", os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return fmt.Errorf("opening compacted_data.txt: %w", err)
	}

	writer := bufio.NewWriter(compact)
	defer func() {
		if err := writer.Flush(); err != nil {
			fmt.Fprintf(os.Stderr, "flush error: %v\n", err)
		}
		if err := compact.Close(); err != nil {
			fmt.Fprintf(os.Stderr, "close error: %v\n", err)
		}
	}()

	log.Info().Msg("Appending fresh data in Compact!!")
	for key, keyState := range fresh {
		if keyState.FlagTombstone {
			continue
		}
		if err := Writer(writer, []byte(key), keyState.Val); err != nil {
			return fmt.Errorf("writing key %q: %w", key, err)
		}
	}
	log.Info().Msg("Merging Complete!!")
	return nil
}
