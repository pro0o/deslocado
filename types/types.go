package types

type RecordFlag byte

const (
	FlagNormal    RecordFlag = 0
	FlagTombstone RecordFlag = 1
)

type FileOffset struct {
	FileID string
	Offset int64
}

type KeyState struct {
	Val           []byte
	FlagTombstone bool
}
