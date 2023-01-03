package types

import (
	"fmt"
)

type Rows [][]string

func (r Rows) Length() int {
	return len(r)
}

func (r Rows) Row(n int) ([]string, error) {
	if r.Length() < n {
		return nil, fmt.Errorf("row[%d] does not exist", n)
	}

	return r[n-1], nil
}
