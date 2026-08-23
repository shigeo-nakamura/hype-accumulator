.PHONY: research test

research:
	python3 -m hype_research.cli run --manifest fixtures/experiment.json --output build/research-report.json

test:
	python3 -m unittest discover -s tests -v
