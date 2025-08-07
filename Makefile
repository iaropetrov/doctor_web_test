# Makefile для управления проектом kvstore

.PHONY: run test lint build run-docker clean

run:
	poetry run python main.py

test:
	PYTHONPATH=. poetry run pytest

lint:
	poetry run autopep8 --in-place --aggressive main.py utils/*.py

build:
	docker build -t kvstore .

run-docker: build
	docker run -it --rm kvstore

clean:
	rm -rf __pycache__ .pytest_cache .mypy_cache .coverage htmlcov
	echo "Очистка завершена" 