.PHONY: run test race cover check bench

run:
	go run .

test:
	go test ./...

# the suite is expected to be deterministic, so this is a supported command
race:
	go test -race ./...

cover:
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out

# everything CI would run, in one go
check: test race cover
