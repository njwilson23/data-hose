
all: hose.go dist
	go build -v -o dist/hose

windows: hose.go dist
	env GOOS=windows GOARCH=amd64 go build -o dist/hose.exe

dist:
	mkdir -p dist

clean:
	rm -rf dist
