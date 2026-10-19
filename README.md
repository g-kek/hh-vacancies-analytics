# Анализ вакансий C и C++

Вариант 6. Подсчёт навыков, пар и троек через MapReduce и DuckDB из RustFS, сравнение времени и памяти, анализ зарплат и регионов.

Для запуска нужны Python 3.12, uv и Docker. Скопировать `.env.example` в `.env` и задать `S3_SECRET_KEY`.

```bash
cp .env.example .env
uv venv --python python3.12
uv pip sync --python .venv/bin/python requirements.lock.txt
uv pip install --python .venv/bin/python --no-deps -e .
.venv/bin/python -c "import duckdb; duckdb.connect().execute('INSTALL httpfs')"
docker compose up -d
.venv/bin/hh load
.venv/bin/hh run
```
