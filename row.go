package main

type Row struct {
	ColumnNames []string
	Values      []string
}

func (r *Row) String() string {
	b := make([]byte, 0)
	for _, rec := range r.Values {
		b = append(b, rec...)
		b = append(b, ',')
	}
	return string(b)
}
