package main

import (
	"bytes"
	"testing"
)

func TestMarshalRow(t *testing.T) {

	row := &Row{
		ColumnNames: []string{"col1", "col2", "col3"},
		Values:      []string{"1", "2.0", "three"},
	}

	jsonBytes, err := row.MarshalJSON()
	if err != nil {
		t.Error()
	}

	ref := `{"col1":1,"col2":2.0,"col3":"three"}`
	if string(jsonBytes) != ref {
		t.Fail()
	}

}

func TestMarshalRowChannel(t *testing.T) {

	ch := make(chan *Row)
	writer := bytes.NewBuffer([]byte{})
	go func(c chan *Row) {
		for i := 0; i != 3; i++ {
			row := &Row{
				ColumnNames: []string{"col1", "col2", "col3"},
				Values:      []string{"1", "2.0", "three"},
			}
			c <- row
		}
		close(c)
	}(ch)

	err := writeJSONRows(writer, ch)
	if err != nil {
		t.Error()
	}

	ref := `[{"col1":1,"col2":2.0,"col3":"three"},{"col1":1,"col2":2.0,"col3":"three"},{"col1":1,"col2":2.0,"col3":"three"}]`
	if writer.String() != ref {
		t.Fail()
	}

}
