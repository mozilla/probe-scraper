.PHONY: help clean lint test build docker-rm shell run stop apidoc

help:
	@echo "  apidoc                 Render the API documentation locally to index.html"
	@echo "  clean                  Remove build artifacts"
	@echo "  check-repos            Verify all repositories in repositories.yaml are scrapable"
	@echo "  lint                   Check style with flake8"
	@echo "  format                 Format code with black and isort"
	@echo "  test                   Run tests quickly with the default Python"
	@echo "  build                  Builds the docker images for the docker-compose setup"
	@echo "  docker-rm              Stops and removes all docker containers"
	@echo "  shell                  Opens a Bash shell"
	@echo "  run                    Run a command. Can run scripts, e.g. make run COMMAND=\"./scripts/schema_generator.sh\""
	@echo "  stop                   Stop docker compose"

clean: clean-build clean-pyc docker-rm

clean-build:
	rm -fr build/
	rm -fr dist/
	rm -fr *.egg-info

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +

apidoc:
	# Keep in sync with doc task in .circleci/config.yml
	docker run --rm \
		-v ${PWD}:/local \
		cimg/node:lts \
		sh -c "npm install @redocly/cli; npx @redocly/cli build-docs /local/probeinfo_api.yaml -o /local/index.html --theme.openapi.expandResponses='200,201' --theme.openapi.jsonSampleExpandLevel=2"

format:
	python3 -m black probe_scraper tests ./*.py
	python3 -m isort --profile black probe_scraper tests ./*.py

lint: build
	docker-compose run app flake8 .
	docker-compose run app yamllint repositories.yaml .circleci
	docker-compose run app python -m black --check probe_scraper tests ./*.py
	docker-compose run app python -m isort --profile black --check-only probe_scraper tests ./*.py

check-repos:
	docker-compose run app python -m probe_scraper.check_repositories

test: build
	docker-compose run app pytest tests/ --run-web-tests

# For this test, we scrape glean-core and burnham.
# Even though burnham is deprecated, it should still be valid to be scraped
# See also mozilla/probe-scraper#283.
# We set a limit date due to more strict parsing.
# glean-core's metrics.yaml prior to 2023-10-20 cannot be parsed with a modern glean-parser.
burnham-dryrun:
	docker-compose run app python -m probe_scraper.runner --glean --glean-repo glean-core --glean-repo glean-android --glean-repo burnham --glean-limit-date 2023-10-20 --dry-run

build:
	docker-compose build

docker-rm: stop
	docker-compose rm -f

shell:
	docker-compose run --entrypoint "/bin/bash" app

run: build
	docker-compose run app $(COMMAND)

stop:
	docker-compose down
	docker-compose stop
