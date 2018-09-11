all: build test

build:
	go build -x -o dx-download-agent dx-download-agent.go

test:
	rm -f ../test_files/*.stats.db single_file.bin other_file.bin many_files*.bin
	./dx-download-agent download ../test_files/two_files.manifest.json.bz2
	./dx-download-agent download ../test_files/many_files.manifest.json.bz2
	diff s.bin single_file.bin

clean:
	go clean -x
	rm -vf dx-download-agent

check:
	go test -v .

install:
	go install -v .

.PHONY: all clean check install
