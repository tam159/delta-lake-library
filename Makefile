.PHONY: install update test lint clean-pyc clean-build clean-test clean uninstall-wheel build deploy

help:
	@echo "install - install the packages by poetry"
	@echo "update - update the packages by poetry"
	@echo "test - run tests with pytest"
	@echo "lint - check code quality and fix"
	@echo "clean - remove all build, test, cache and Python artifacts"
	@echo "clean-pyc - remove Python file artifacts"
	@echo "clean-build - remove build artifacts"
	@echo "clean-test - remove test and coverage artifacts"

install:
	poetry install

update:
	poetry update

test:
	poetry run pytest tests

lint:
	poetry run black .
	poetry run isort .
	-poetry run mypy .
	-poetry run flake8
	-poetry run pydocstyle

clean-pyc:
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-build:
	rm -rf dist/

clean-test:
	rm -rf .tox/
	rm -rf htmlcov/
	rm -f .coverage

clean: clean-build clean-pyc clean-test

uninstall-wheel:
	python delta_lake_deployment/wheel_uninstallation.py
	dbfs rm --recursive dbfs:/dist

build: clean-build
	poetry version $(shell date -u +%Y.%m.%d.%H.%M.%S)
	poetry build -f wheel

deploy: uninstall-wheel build
	dbfs cp -r --overwrite dist/ dbfs:/dist/
	python delta_lake_deployment/wheel_installation.py --wheel_name $(shell ls dist)
