package main

// Pipeline is a sequence of transformations
type Pipeline struct {
	Transformers []Transformer
}

// Add adds a transformer to the pipeline
func (p *Pipeline) Add(tf Transformer) error {
	p.Transformers = append(p.Transformers, tf)
	return nil
}

// Run executes the pipeline, passing rows from each transformer to the next
func (p *Pipeline) Run(input chan *Row, output chan *Row) error {

	var cout chan *Row
	cin := input

	for i, tf := range p.Transformers {

		if i == len(p.Transformers)-1 {
			cout = output
		} else {
			cout = make(chan *Row)
		}

		go func(cin <-chan *Row, cout chan<- *Row) {
			var transformedRow *Row
			rowCount := 0
			for row := range cin {
				transformedRow = tf(rowCount, row)
				if transformedRow != nil {
					cout <- transformedRow
				}
				rowCount++
			}
			close(cout)
		}(cin, cout)
		cin = cout
	}

	return nil
}
