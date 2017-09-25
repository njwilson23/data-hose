package main

// Transformer is a function that takes a Row pointer and a row number and returns a Row pointer
type Transformer func(int, *Row) *Row

// RowSkipper returns a transformation that skips *n* rows
func RowSkipper(n int) Transformer {
	return func(count int, row *Row) *Row {
		if count < n {
			return nil
		}
		return row
	}
}

func contains(strings []string, name string) bool {
	for _, s := range strings {
		if s == name {
			return true
		}
	}
	return false
}

func ColumnSelector(columns []string) Transformer {
	return func(count int, row *Row) *Row {
		var selected []string
		for i, name := range row.ColumnNames {
			if contains(columns, name) {
				selected = append(selected, row.Values[i])
			}
		}
		return &Row{columns, selected}
	}
}

func IdentityTransformer(count int, row *Row) *Row {
	return row
}
