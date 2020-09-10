.PHONY: help clean lint test build docker-rm shell run stop

help:
	@echo "  clean                  Remove build artifacts"
	@echo "  lint                   Check style with flake8"
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

lint: build
	docker-compose run app flake8 --max-line-length 100 .
	docker-compose run app yamllint repositories.yaml .circleci

test: build
	docker-compose run app pytest tests/ --run-web-tests

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
