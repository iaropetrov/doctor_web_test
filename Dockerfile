FROM python:3.10-slim
RUN pip install poetry
WORKDIR /app
COPY pyproject.toml poetry.lock* ./
RUN poetry config virtualenvs.create false && poetry install --no-interaction --no-ansi --no-root
COPY . .
CMD ["python", "main.py"] 