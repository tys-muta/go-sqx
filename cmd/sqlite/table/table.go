package table

import (
	"fmt"
)

type Table [][]string

func (t Table) RowLength() int {
	return len(t)
}

func (t Table) Row(n int) ([]string, error) {
	if t.RowLength() < n {
		return nil, fmt.Errorf("row[%d] does not exist", n)
	}

	return t[n-1], nil
}
