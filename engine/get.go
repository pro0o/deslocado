package engine

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/pro0o/deslocado/types"
)

// fetch keydir
// seek the offset to curr file offset
// read key & set offset -> val
// return val
func Get(keyDir map[string]types.FileOffset, key string) (string, error) {
	fileOffset, ok := keyDir[key]
	if !ok {
		return "", fmt.Errorf("key not found")
	}
	file, err := os.Open(fileOffset.FileID)
	if err != nil {
		return "", fmt.Errorf("file not found from hint")
	}
	file.Seek(fileOffset.Offset, io.SeekStart)
	flag := make([]byte, 1)
	file.Read(flag)
	if flag[0] == byte(types.FlagTombstone) {
		return "", fmt.Errorf("the kv entry was deleted")
	}

	var keyLen, valLen uint32
	binary.Read(file, binary.BigEndian, &keyLen)
	binary.Read(file, binary.BigEndian, &valLen)

	file.Seek(int64(keyLen), io.SeekCurrent)

	valBuffer := make([]byte, valLen)
	io.ReadFull(file, valBuffer)

	file.Close()
	return string(valBuffer), nil
}
