package main

import "bufio"

type TextReader struct {
	buffer *bufio.Reader
}

type TextWriter struct {
	buffer *bufio.Writer
}

func (reader *TextReader) ReadRow(options *rowReadOptions) (*Row, error) {
	s, err := reader.buffer.ReadString('\n')
	if err != nil {
		return &Row{}, err
	}
	return &Row{Schema: []int{0}, Values: []string{s}}, nil
}

func (writer *TextWriter) WriteRow(row *Row, options *rowWriteOptions) error {
	for _, s := range row.Values {
		_, err := writer.buffer.WriteString(s)
		if err != nil {
			return err
		}
	}
	//_, err := writer.buffer.WriteRune('\n')
	//if err != nil {
	//	return err
	//}
	return nil
}

func (writer *TextWriter) Flush() error {
	return writer.buffer.Flush()
}
