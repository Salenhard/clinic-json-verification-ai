# Clinical JSON Validator — API

REST API для верификации клинических JSON-данных с помощью AI-пайплайна (Google Gemini).

---

## Архитектура

```
clinical_validator/
├── config/                      # Настройки (Settings dataclass)
│   └── settings.py
├── repository/                  # Слой данных
│   ├── base.py                  # Абстрактный интерфейс (AbstractTaskRepository)
│   ├── models.py                # Доменная модель Task + TaskStatus
│   └── sqlite_repository.py    # Конкретная реализация (SQLite + WAL)
├── pipeline/                   # Сервис для верификации и фикса json
│   ├── analysis_stage.py
│   ├── base.py
│   ├── chunker.py
│   ├── correction_stage.py
│   ├── finalization_stage.py
│   ├── json_preproccessor.py
│   ├── json_validator.py
│   ├── rate_limiter.py
├── service/                     # Бизнес-логика
│   └── verification_service.py  # VerificationService — оркестрирует пайплайн
├── controller/                  # HTTP-слой (Flask Blueprints)
│   ├── health_controller.py
│   └── verification_controller.py
├── app.py                       # Application factory (create_app)
├── main.py                      # Точка входа для прямого запуска
└── tests/
    ├── conftest.py
    ├── test_repository.py
    ├── test_service.py
    └── test_controllers.py
```

### Принципы слоёв

| Слой | Знает о | Не знает о |
|---|---|---|
| **Repository** | SQLite, SQL, сериализация JSON | Flask, pipeline, genai |
| **Service** | Pipeline stages, TaskRepository, genai | Flask, SQLite, HTTP |
| **Controller** | HTTP request/response, Service | Pipeline, SQLite, genai |
| **Config** | Переменные окружения | Всех остальных |

---

## Быстрый старт

### 1. Переменные окружения

```bash
cp .env.example .env
# Откройте .env и заполните GEMINI_API_KEY
```

### 2. Локальный запуск (dev)

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

python main.py
# → http://localhost:5000
```

### 3. Docker

```bash
docker compose up --build
```

### 4. Production (gunicorn)

```bash
gunicorn "app:create_app()" \
  --workers 4 \
  --worker-class gthread \
  --threads 4 \
  --bind 0.0.0.0:5000
```

---

## Endpoints

| Method | Path | Описание |
|--------|------|----------|
| `POST` | `/api/verify` | Запустить верификацию |
| `GET` | `/api/status/<task_id>` | Полный статус задачи |
| `GET` | `/api/result/<task_id>` | Только результат |
| `GET` | `/api/health` | Liveness probe |

### POST `/api/verify`

Принимает `multipart/form-data`:

| Поле | Тип | Обязательно | Описание |
|------|-----|-------------|----------|
| `data` | string (JSON) | да* | JSON-документ для верификации |
| `json_file` | file (.json) | да* | Альтернатива полю `data` |
| `recommendations` | string | да** | Текст клинических рекомендаций |
| `recommendations_file` | file (.pdf) | да** | PDF с рекомендациями |
| `model` | string | нет | Gemini model name (переопределяет env) |

\* одно из двух обязательно  
\*\* одно из двух обязательно

**Ответ `202`:**
```json
{
  "task_id": "550e8400-e29b-41d4-a716-446655440000",
  "status": "pending",
  "message": "Верификация запущена"
}
```

### GET `/api/status/<task_id>`

```json
{
  "task_id": "...",
  "status": "processing",
  "progress": 45,
  "message": "Итерация 2: анализ и исправление",
  "result": null,
  "created_at": "2024-01-15T10:30:00",
  "updated_at": "2024-01-15T10:30:05"
}
```

`status` может быть: `pending` | `processing` | `completed` | `error`

### GET `/api/result/<task_id>`

| Статус задачи | HTTP-код | Тело |
|---|---|---|
| pending / processing | `202` | `{ "status": "...", "progress": N }` |
| completed | `200` | результат верификации |
| error | `500` | `{ "error": "..." }` |

---

## Тесты

```bash
pip install pytest
pytest
```

Тесты покрывают три уровня:
- `test_repository.py` — SQLite I/O, изоляция через temp-файл
- `test_service.py` — логика пайплайна с мок-стейджами
- `test_controllers.py` — HTTP routes с мок-сервисом

---

## Расширение

### Заменить SQLite на PostgreSQL

Создайте `repository/postgres_repository.py`, унаследуйте `AbstractTaskRepository` и зарегистрируйте его в `app.py` — контроллер и сервис менять не нужно.

### Добавить новый endpoint

1. Создайте Blueprint в `controller/`
2. Зарегистрируйте его в `app.py` через `app.register_blueprint(...)`

### Добавить стейдж в пайплайн

Добавьте новый класс в `pipeline/`, подключите его в `VerificationService._build_stages()`.

---

## Конфигурация

| Переменная | По умолчанию | Описание |
|---|---|---|
| `GEMINI_API_KEY` | — | **Обязательна** |
| `GEMINI_MODEL` | `gemini-2.0-flash-lite` | Модель Gemini |
| `RPM` | `15` | Запросов в минуту |
| `DB_PATH` | `tasks.db` | Путь к SQLite |
| `MAX_ITERATIONS` | `5` | Макс. итераций рефайнмента |
| `TARGET_SCORE` | `1.0` | Целевой completeness score |
| `MAX_CONTENT_MB` | `50` | Макс. размер запроса |
