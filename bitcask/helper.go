package bitcask

import (
	"fmt"
	"path/filepath"
	"sort"
)

func sorted(pattern string) ([]string, error) {
	logs, err := filepath.Glob(pattern)
	if err != nil {
		return nil, fmt.Errorf("glob logs: %w", err)
	}

	getTS := func(name string) int64 {
		var ts int64
		fmt.Sscanf(name, "data_%d.log", &ts)
		return ts
	}

	sort.Slice(logs, func(i, j int) bool {
		return getTS(logs[i]) > getTS(logs[j])
	})

	return logs, nil
}
