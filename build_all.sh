#! /bin/sh
# Compile for multiple architectures

env GOOS=windows GOARCH=386 go build -v -o hose.exe
env GOOS=windows GOARCH=amd64 go build -v -o hose.exe

env GOOS=linux GOARCH=amd64 go build -v -o hose
