SOURCE = hose.go csv2lsvm.go

all: $(SOURCE) dist
	go build -v -o dist/hose

windows: $(SOURCE) dist
	env GOOS=windows GOARCH=amd64 go build -o dist/hose.exe

dist:
	mkdir -p dist

clean:
	rm -rf dist
