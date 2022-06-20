package table

type csv struct{}

var _ parser = (*csv)(nil)

func (p *csv) Parse(bytes []byte) (Table, error) {
	// TODO: 必要に応じて実装
	return nil, nil
}
