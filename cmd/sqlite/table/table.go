package table

import (
	"fmt"
)

type Data [][]string

func (t Data) RowLength() int {
	return len(t)
}

func (t Data) Row(n int) ([]string, error) {
	if t.RowLength() < n {
		return nil, fmt.Errorf("row[%d] does not exist", n)
	}

	return t[n-1], nil
}
