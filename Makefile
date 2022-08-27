GC := go

build: bin-folder
	go build -o bin/gomq main.go

bin-folder:
	rm -rf bin
	mkdir bin

dev:
	go run main.go
