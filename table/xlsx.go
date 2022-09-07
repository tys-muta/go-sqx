// xlsx はマイクロソフトの xlsx だと問題ないが、
// xlsx-file-editor など保存するソフトウェアによって情報が取得できない場合がある
package table

import (
	"fmt"
	"strconv"
	"time"

	xls "github.com/tealeg/xlsx/v3"
	"github.com/tys-muta/go-sqx/config"
)

type xlsxParser struct{}

var _ parser = (*xlsxParser)(nil)

func (p *xlsxParser) Parse(bytes []byte) (Table, error) {
	var file *xls.File
	if v, err := xls.OpenBinary(bytes); err != nil {
		return nil, fmt.Errorf("failed to open xlsx file: %w", err)
	} else {
		file = v
	}

	table := Table{}
	cfg := config.Get().SQLite.Gen
	invalidMap := map[int]bool{}

	for k, v := range file.Sheet {
		if k != cfg.XLSX.Sheet {
			continue
		}
		if err := v.ForEachRow(func(row *xls.Row) error {
			values := []string{}
			if err := row.ForEachCell(func(cell *xls.Cell) error {
				cellValue, err := p.parseCell(cell)
				if err != nil {
					return fmt.Errorf("failed to parse cell: %w", err)
				}
				rowIndex, cellIndex := cell.GetCoordinates()
				if cellValue == "" && rowIndex == cfg.Head.ColumnNameRow-1 {
					invalidMap[cellIndex] = true
				}
				if invalidMap[cellIndex] {
					return nil
				}
				values = append(values, cellValue)
				return nil
			}); err != nil {
				return fmt.Errorf("failed to iterate cells: %w", err)
			}
			table = append(table, values)
			return nil
		}); err != nil {
			return nil, fmt.Errorf("failed to iterate rows: %w", err)
		}
	}

	return table, nil
}

func (p *xlsxParser) parseCell(cell *xls.Cell) (string, error) {
	// セルフォーマットが時間でかつ、
	// 値が数値に場合は RFC3339 形式の文字列に変換する
	var f float64
	if v, err := strconv.ParseFloat(cell.Value, 64); err == nil {
		f = v
	}
	if cell.IsTime() && f > 0 {
		if v, err := cell.GetTime(false); err != nil {
			return "", fmt.Errorf("failed to get time: %w", err)
		} else {
			v = v.In(config.Location())
			_, offset := v.Zone()
			v = v.Add(time.Duration(offset) * -time.Second)
			return v.Format(time.RFC3339), nil
		}
	}

	return cell.Value, nil
}
