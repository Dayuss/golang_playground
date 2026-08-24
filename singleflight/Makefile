.PHONY: run loadtest-sf loadtest-plain loadtest-sf-ramp loadtest-plain-ramp

run:
	go run .

# burst scenario: fast concurrent hits on the same key
loadtest-sf:
	go run ./loadtest -target=sf -scenario=burst

loadtest-plain:
	go run ./loadtest -target=plain -scenario=burst

# ramp scenario: 0->50rps over 10s, hold 20s, ramp down 10s
loadtest-sf-ramp:
	go run ./loadtest -target=sf -scenario=ramp

loadtest-plain-ramp:
	go run ./loadtest -target=plain -scenario=ramp
