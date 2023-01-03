package types

type Definition struct {
	Name    string
	Columns []Column
	Options []func(any)
}
