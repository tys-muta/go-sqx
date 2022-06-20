package table

type tsv struct{}

var _ parser = (*tsv)(nil)

func (p *tsv) Parse(bytes []byte) (Table, error) {
	// TODO: 必要に応じて実装
	return nil, nil
}
