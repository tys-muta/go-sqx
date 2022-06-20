package table

import (
	"fmt"
	"time"

	xls "github.com/tealeg/xlsx/v3"
	"github.com/tys-muta/go-sqx/cfg"
)

type xlsx struct{}

var _ parser = (*xlsx)(nil)

func (p *xlsx) Parse(bytes []byte) (Table, error) {
	var file *xls.File
	if v, err := xls.OpenBinary(bytes); err != nil {
		return nil, fmt.Errorf("failed to open xlsx file: %w", err)
	} else {
		file = v
	}

	table := Table{}

	for k, v := range file.Sheet {
		if k != cfg.Value.Sqlite.Gen.XLSX.Sheet {
			continue
		}
		if err := v.ForEachRow(func(row *xls.Row) error {
			r := []string{}
			if err := row.ForEachCell(func(cell *xls.Cell) error {
				if cell.IsTime() && cell.Value != "" {
					if v, err := cell.GetTime(false); err != nil {
						return fmt.Errorf("failed to get time: %w", err)
					} else {
						v = v.In(time.Local)
						_, offset := v.Zone()
						v = v.Add(time.Duration(offset) * -time.Second)
						r = append(r, v.Format(time.RFC3339))
					}
				} else {
					r = append(r, cell.Value)
				}
				return nil
			}); err != nil {
				return fmt.Errorf("failed to iterate cells: %w", err)
			}
			table = append(table, r)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to iterate rows: %w", err)
		}
	}

	return table, nil
}
