# AI_NHAC_VIEC_CHAT_JIRA

A production-ready reminder bot that connects to Jira, reads employee/tasks, and sends reminders via FPT Chat API. Built with FastAPI, SQLAlchemy, Alembic, and APScheduler.

## Features
- FastAPI REST with Swagger UI
- SQLAlchemy models and Alembic migrations
- APScheduler periodic reminder jobs
- Config via `.env` or environment variables
- Dockerized app + database
- Adapters to legacy Jira and Chat utilities

## Project Structure
```
app/
  main.py
  api/v1/routes.py
  api/v1/endpoints/health.py
  api/v1/endpoints/reminders.py
  dependencies.py
core/
  config.py
  logging.py
  constants.py
  container.py
db/
  base.py
  session.py
  models/
    employee.py
    reminder.py
    log.py
  repositories/
    employee_repo.py
    reminder_repo.py
    log_repo.py
schemas/
  common.py
  employee.py
  reminder.py
  log.py
services/
  jira_service.py
  chat_service.py
  reminder_service.py
  scheduler.py
alembic/
  env.py
  versions/

Dockerfile
docker-compose.yml
requirements.txt
.env.example
```

## Local Setup
1. Python 3.10+
2. Create and activate venv
```bash
python -m venv .venv
. .venv/Scripts/activate  # Windows PowerShell: . .venv/Scripts/Activate.ps1
pip install -r requirements.txt
```
3. Copy env
```bash
copy .env.example .env
```
4. Run API
```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```
5. Open Swagger: `http://localhost:8000/docs`

## Docker
```bash
docker compose up --build
```
- API: `http://localhost:8000/docs`
- DB: PostgreSQL on port 5432

## Alembic Migrations
Initialize (already scaffolded). Create a new migration and upgrade:
```bash
alembic revision -m "init tables"
alembic upgrade head
```

## Example API Calls
- Health: `GET /api/v1/health`
- Manual sync: `POST /api/v1/reminders/sync`
- Trigger reminders: `POST /api/v1/reminders/run`

## Notes
- Keep legacy `jira_utils.py`, `chat_api.py`, `rules.py` usable. Services act as adapters so existing logic remains intact.

