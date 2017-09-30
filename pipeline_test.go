package main

import "testing"

func TestPipelineRun(t *testing.T) {
	// try setting up a simple pipeline and running to ensure it completes

	input := make(chan *Row)
	output := make(chan *Row)

	pipeline := &Pipeline{
		Transformers: []Transformer{func(input <-chan *Row, output chan<- *Row) {
			for row := range input {
				output <- row
			}
			close(output)
		}, func(input <-chan *Row, output chan<- *Row) {
			for row := range input {
				output <- row
			}
			close(output)
		}, func(input <-chan *Row, output chan<- *Row) {
			for row := range input {
				output <- row
			}
			close(output)
		}},
	}

	go pipeline.Run(input, output)

	for i := 0; i != 10; i++ {
		input <- &Row{[]string{"A", "B", "C"}, []string{"1", "2", "3"}}
		row := <-output
		if row.Values[0] != "1" {
			t.Fail()
		}
	}

}
