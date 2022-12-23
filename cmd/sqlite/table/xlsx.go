// xlsx はマイクロソフトの xlsx だと問題ないが、
// xlsx-file-editor など保存するソフトウェアによって情報が取得できない場合がある
package table

import (
	"fmt"
	"strconv"
	"time"

	xls "github.com/tealeg/xlsx/v3"
	"github.com/tys-muta/go-sqx/cmd/sqlite/config"
)

type xlsxParser struct{}

var _ parser = (*xlsxParser)(nil)

func (p *xlsxParser) Parse(bytes []byte) (Table, error) {
	file, err := xls.OpenBinary(bytes)
	if err != nil {
		return nil, fmt.Errorf("failed to open xlsx file: %w", err)
	}

	table := Table{}
	invalidMap := map[int]bool{}

	for k, v := range file.Sheet {
		if k != config.Get().XLSX.Sheet {
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
				if cellValue == "" && rowIndex == config.Get().Head.ColumnNameRow-1 {
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

// セルフォーマットが時間でかつ, 値が数値に場合は RFC3339 形式の文字列に変換する
func (p *xlsxParser) parseCell(cell *xls.Cell) (string, error) {
	float, err := strconv.ParseFloat(cell.Value, 64)
	if err != nil || !cell.IsTime() || float == 0 {
		// セルフォーマットが時間でない場合はそのまま返す
		return cell.Value, nil
	}

	t, err := cell.GetTime(false)
	if err != nil {
		return "", fmt.Errorf("failed to get time: %w", err)
	}

	loc := config.Get().Location
	_, offset := t.In(&loc).Zone()
	t = t.Add(time.Duration(offset) * -time.Second)
	return t.Format(time.RFC3339), nil
}
