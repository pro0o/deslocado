package bitcask

import (
	"bufio"
	"encoding/binary"

	"github.com/pro0o/deslocado/types"
)

func Writer(writer *bufio.Writer, key, val []byte) error {
	if err := writer.WriteByte(byte(types.FlagNormal)); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(len(val))); err != nil {
		return err
	}
	if _, err := writer.Write(key); err != nil {
		return err
	}
	if _, err := writer.Write(val); err != nil {
		return err
	}

	return nil
}

func WriterTombstone(writer *bufio.Writer, key []byte) error {
	if err := writer.WriteByte(byte(types.FlagTombstone)); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(len(key))); err != nil {
		return err
	}
	if err := binary.Write(writer, binary.BigEndian, uint32(0)); err != nil {
		return err
	}
	if _, err := writer.Write(key); err != nil {
		return err
	}
	return nil
}
