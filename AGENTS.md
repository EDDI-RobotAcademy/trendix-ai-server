# Repository Guidelines

## Project Structure and Module Organization
This is a FastAPI service organized by feature packages. Core entrypoint is
`app/main.py`, which wires routers and background batches. Domains follow a
layered layout: `*/domain` for entities, `*/application` for use cases and
ports, `*/adapter` for web entrypoints, and `*/infrastructure` for clients,
ORM, and repositories. Shared configuration lives under `config/` (database,
redis, S3, OpenAI). Batch jobs are under `app/batch/`. SQL references live in
`docs/sql/`. Utility scripts are in `scripts/`.

## Build, Test, and Development Commands
- `pip install -r requirements.txt`: install runtime dependencies.
- `python -m app.main`: run the API server using values from `.env`.
- `uvicorn app.main:app --reload --host 0.0.0.0 --port 33333`: dev server with
  auto-reload.
- `docker compose up --build`: run FastAPI, MySQL, and Redis together.
- `python scripts/update_channel_info.py`: run the channel update script.

## Coding Style and Naming Conventions
Use Python 4-space indentation and PEP 8 naming: `snake_case` for modules and
functions, `PascalCase` for classes. Routers follow `*_router.py`, requests and
responses live in `adapter/input/web/request` and `adapter/input/web/response`.
Keep FastAPI routes thin and push logic into use cases.

## Testing Guidelines
No test framework or coverage target is configured in this repo yet. If you add
tests, prefer `pytest`, place files under `tests/`, and name them `test_*.py`.
Document any required fixtures (DB, Redis) in the PR description.

## Commit and Pull Request Guidelines
Recent commits use short type prefixes like `feat:` and optional author tags,
for example `feat:[name] short summary`. Keep messages in present tense and
focus on the behavior change. For PRs, include a clear summary, testing notes,
and link the related issue if one exists. Call out any config or schema changes.

## Configuration and Secrets
Copy `.env.example` to `.env` and fill required keys for database, Redis, OAuth,
and API clients. Do not commit real secrets. Update `README.md` if you add new
required variables.

## Notes
- ì½˜í…ì¸  ë¹„êµ ë¶„ì„ API: `POST /analysis/shorts/compare` (íŒŒì¼: `content/adapter/input/web/compare_router.py`)
- ì˜ìƒ ìš”ì•½ ì¡°íšŒ: `ContentRepositoryImpl.fetch_video_summary` (íŒŒì¼: `content/infrastructure/repository/content_repository_impl.py`)
- Shorts ºñ±³ ºĞ¼® API´Â pp/main.py¿¡ compare_router¸¦ /analysis prefix·Î µî·ÏÇØ¾ß ÇÁ·ĞÆ®¿¡¼­ È£Ãâ °¡´ÉÇÕ´Ï´Ù.
- Ã¤³Î ºĞ¼® API: POST /analysis/channel (ÆÄÀÏ: content/adapter/input/web/channel_analysis_router.py)
- ¿µ»ó »ó¼¼ API: GET /analysis/videos/{video_id} (ÆÄÀÏ: content/adapter/input/web/video_detail_router.py)
