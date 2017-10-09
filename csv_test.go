package main

import (
	"fmt"
	"testing"
)

func TestRowParseFast(t *testing.T) {
	colNames := []string{"UID", "Name", "Date", "Quantity"}
	b := []byte("34,\"Andrew Benson\",2013-05-23,2")

	row, err := parseRow(b, colNames)
	if err != nil {
		t.Error()
	}

	expectedValues := []string{"34", "\"Andrew Benson\"", "2013-05-23", "2"}
	for i, ev := range expectedValues {
		if len(row.Values) < i+1 || row.Values[i] != ev {
			fmt.Println("error on value", ev)
			t.Fail()
		}
	}
}
