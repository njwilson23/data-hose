package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"testing"
)

func TestReadSingleInput(t *testing.T) {
	buffer := make(chan *Row)
	errs := make(chan error)

	f, err := os.Open("testdata/sequence.txt")
	if err != nil {
		fmt.Println(err)
		t.Error()
	}

	options := &ReadOptions{}
	inputs := []RowBasedReader{&CSVReader{bufio.NewReader(f)}}

	go readInputs(inputs, buffer, errs, options)
	count := 0
	for _ = range buffer {
		count++
	}
	_, ok := <-errs
	if ok {
		t.Error()
	}
	if count != 1000 {
		t.Fail()
	}
}

func TestReadMultipleInput(t *testing.T) {
	buffer := make(chan *Row)
	errs := make(chan error)
	paths := []string{"testdata/sequence.txt", "testdata/sequence.txt"}

	inputs := make([]RowBasedReader, len(paths))
	for i, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			fmt.Println(err)
			t.Error()
		}

		inputs[i] = &CSVReader{bufio.NewReader(f)}
	}

	options := &ReadOptions{}

	go readInputs(inputs, buffer, errs, options)
	count := 0
	for _ = range buffer {
		count++
	}
	_, ok := <-errs
	if ok {
		t.Error()
	}
	if count != 2000 {
		t.Fail()
	}
}

func TestHandleLines(t *testing.T) {
	ch := make(chan *Row, 100)
	names := ColumnNames([]string{"A", "B", "C"})

	for i := 0; i != 100; i++ {
		ch <- &Row{
			Schema: []int{0, 1, 2},
			Values: []string{"10", "20", "30"},
			Names:  &names,
		}
	}
	close(ch)

	options := &WriteOptions{}
	b := bytes.Buffer{}
	writer := &CSVWriter{bufio.NewWriter(&b)}
	err := handleLines(writer, ch, options)

	if err != nil {
		fmt.Println(err)
		t.Error()
	}

	for i := 0; i != 100; i++ {
		line, err := b.ReadString('\n')
		if err != nil {
			t.Error()
		}
		if line != "10,20,30\n" {
			t.Fail()
		}
	}
}

func TestGetReader(t *testing.T) {
	reader := &bufio.Reader{}
	r, err := getReader("csv", reader)
	if err != nil {
		t.Error()
	}
	if _, ok := r.(*CSVReader); !ok {
		t.Fail()
	}

	r, err = getReader("svm", reader)
	if err != nil {
		t.Error()
	}
	if _, ok := r.(*LibSVMReader); !ok {
		t.Fail()
	}

	r, err = getReader("txt", reader)
	if err != nil {
		t.Error()
	}
	if _, ok := r.(*TextReader); !ok {
		t.Fail()
	}

	r, err = getReader("unknown", reader)
	if err == nil {
		t.Fail()
	}
}
func TestGetWriter(t *testing.T) {
	reader := &bufio.Writer{}
	r, err := getWriter("csv", reader)
	if err != nil {
		t.Error()
	}
	if _, ok := r.(*CSVWriter); !ok {
		t.Fail()
	}

	r, err = getWriter("svm", reader)
	if err != nil {
		t.Error()
	}
	if _, ok := r.(*LibSVMWriter); !ok {
		t.Fail()
	}

	r, err = getWriter("txt", reader)
	if err != nil {
		t.Error()
	}
	if _, ok := r.(*TextWriter); !ok {
		t.Fail()
	}

	r, err = getWriter("unknown", reader)
	if err == nil {
		t.Fail()
	}
}
