.PHONY: remote-up
remote-up:
	bash scripts/remote-up.sh

.PHONY: e2e
e2e:
	timeout --verbose --kill-after=10s 290s bash scripts/e2e.sh
