package main

import (
	"encoding/json"
	"io"
	"strconv"
)

func (r *Row) MarshalJSON() ([]byte, error) {
	var err error
	b := []byte{'{'}
	for i, val := range r.Values {

		// is the value numeric?
		_, convErr := strconv.ParseFloat(val, 64)

		if i != 0 {
			b = append(b, ',')
		}
		b = append(b, '"')
		b = append(b, append([]byte(r.ColumnNames[i]), []byte{'"', ':'}...)...)

		if convErr == nil {
			b = append(b, []byte(val)...)
		} else {
			b = append(b, append([]byte{'"'}, append([]byte(val), '"')...)...)
		}
	}
	b = append(b, '}')
	return b, err
}

func writeJSONRows(writer io.Writer, ch <-chan *Row) error {
	first := true
	var err error
	var jsonBytes []byte
	for row := range ch {
		if first {
			first = !first
			_, err = writer.Write([]byte{'['})
			if err != nil {
				return err
			}
		} else {
			_, err = writer.Write([]byte{','})
			if err != nil {
				return err
			}
		}
		jsonBytes, err = json.Marshal(row)
		if err != nil {
			return err
		}
		_, err = writer.Write(jsonBytes)
		if err != nil {
			return err
		}
	}
	_, err = writer.Write([]byte{']'})
	if err != nil {
		return err
	}
	return nil
}
