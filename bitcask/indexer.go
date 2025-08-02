package bitcask

import (
	"bufio"
	"encoding/binary"
	"io"
	"os"
	"strings"

	"github.com/pro0o/deslocado/types"
)

func BuildKeyDir() (map[string]types.FileOffset, error) {
	keyDir := make(map[string]types.FileOffset)
	hints, err := sorted("data_*.hint")
	if err != nil {
		return nil, err
	}
	for _, hint := range hints {

		// compact.hint -> compact.log
		log := strings.TrimSuffix(hint, ".hint") + ".log"
		file, err := os.Open(hint)
		if err != nil {
			return nil, err
		}
		reader := bufio.NewReader(file)
		for {
			var keyLen uint32
			if err := binary.Read(reader, binary.BigEndian, &keyLen); err == io.EOF {
				break
			} else if err != nil {
				return nil, err
			}
			keyBuffer := make([]byte, (keyLen))
			if _, err := io.ReadFull(reader, keyBuffer); err != nil {
				return nil, err
			}

			var offset uint64
			if err := binary.Read(reader, binary.BigEndian, &offset); err != nil {
				return nil, err
			}
			keyDir[string(keyBuffer)] = types.FileOffset{
				FileID: log,
				Offset: int64(offset),
			}
		}
		file.Close()
	}
	return keyDir, nil
}
