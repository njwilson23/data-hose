package main

import (
	"fmt"
	"strconv"
	"testing"
)

func TestIdentity(t *testing.T) {
	tochan := make(chan *Row)
	fromchan := make(chan *Row, 10)
	go IdentityTransformer(fromchan, tochan)
	for i := 0; i != 10; i++ {
		fromchan <- &Row{[]string{"id", "data"}, []string{strconv.Itoa(i), "a"}}
	}
	close(fromchan)

	count := 0
	for row := range tochan {
		if row.Values[0] != strconv.Itoa(count) {
			fmt.Println("row value incorrect")
			t.Fail()
		}
		count++
	}
	if count != 10 {
		fmt.Println("final count incorrect")
		t.Fail()
	}
}

func TestRowLimiter(t *testing.T) {
	tochan := make(chan *Row)
	fromchan := make(chan *Row, 10)
	go RowLimiter(5)(fromchan, tochan)
	for i := 0; i != 10; i++ {
		fromchan <- &Row{[]string{"id", "data"}, []string{strconv.Itoa(i), "a"}}
	}
	close(fromchan)

	count := 0
	for row := range tochan {
		if row.Values[0] != strconv.Itoa(count) {
			fmt.Println("row value incorrect")
			t.Fail()
		}
		count++
	}
	if count != 5 {
		fmt.Println("final count incorrect")
		t.Fail()
	}
}

func TestRowSkipper(t *testing.T) {
	tochan := make(chan *Row)
	fromchan := make(chan *Row, 10)
	go RowSkipper(5)(fromchan, tochan)
	for i := 0; i != 10; i++ {
		fromchan <- &Row{[]string{"id", "data"}, []string{strconv.Itoa(i), "a"}}
	}
	close(fromchan)

	count := 5
	for row := range tochan {
		if row.Values[0] != strconv.Itoa(count) {
			fmt.Println("row value incorrect")
			t.Fail()
		}
		count++
	}
	if count != 10 {
		fmt.Println("final count incorrect")
		t.Fail()
	}
}

func TestColumnIntSelector(t *testing.T) {
	tochan := make(chan *Row)
	fromchan := make(chan *Row, 10)
	go ColumnIntSelector([]int{1, 3})(fromchan, tochan)
	for i := 0; i != 10; i++ {
		fromchan <- &Row{
			ColumnNames: []string{"a", "b", "c", "d"},
			Values:      []string{"1", "2", "3", "4"},
		}
	}
	close(fromchan)

	count := 0
	for row := range tochan {
		if row.ColumnNames[0] != "b" {
			t.Fail()
		}
		if row.ColumnNames[1] != "d" {
			t.Fail()
		}
		if row.Values[0] != "2" {
			t.Fail()
		}
		if row.Values[1] != "4" {
			t.Fail()
		}
		count++
	}
	if count != 10 {
		fmt.Println("final count incorrect")
		t.Fail()
	}
}

func TestColumnStringSelector(t *testing.T) {
	tochan := make(chan *Row)
	fromchan := make(chan *Row, 10)
	go ColumnStringSelector([]string{"b", "d"})(fromchan, tochan)
	for i := 0; i != 10; i++ {
		fromchan <- &Row{
			ColumnNames: []string{"a", "b", "c", "d"},
			Values:      []string{"1", "2", "3", "4"},
		}
	}
	close(fromchan)

	count := 0
	for row := range tochan {
		if row.ColumnNames[0] != "b" {
			t.Fail()
		}
		if row.ColumnNames[1] != "d" {
			t.Fail()
		}
		if row.Values[0] != "2" {
			t.Fail()
		}
		if row.Values[1] != "4" {
			t.Fail()
		}
		count++
	}
	if count != 10 {
		fmt.Println("final count incorrect")
		t.Fail()
	}
}
