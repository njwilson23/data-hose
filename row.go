package main

// Row represents a single line from a flat file, with all values stored as test
type Row struct {
	ColumnNames []string
	Values      []string
}

func (r *Row) String() string {
	var b []byte
	for _, rec := range r.Values {
		b = append(b, rec...)
		b = append(b, ',')
	}
	return string(b)
}
