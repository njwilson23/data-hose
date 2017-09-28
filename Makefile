SOURCE = cli.go row.go pipeline.go transformers.go csv.go json.go libsvm.go 

all: $(SOURCE) dist
	go build -v -o dist/flt

windows: $(SOURCE) dist
	env GOOS=windows GOARCH=amd64 go build -o dist/flt.exe

dist:
	mkdir -p dist

clean:
	rm -rf dist
