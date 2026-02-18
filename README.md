# Telegram News Digest Bot (RU)

Production-ориентированный Telegram-бот на Python для автономной публикации сухих политико-экономических сводок (дневных и недельных) в канал.

## Ключевые свойства

- Автосбор новостей из источников трех типов: `rss`, `site`, `api`.
- Интеллектуальная фильтрация и отсев шумовых/локальных событий.
- Конфликтный guardrail: блок тактических деталей и эмоционального шума.
- Дедупликация похожих заголовков между источниками (near-duplicate).
- Бесплатный fallback-суммаризатор (без внешнего LLM API).
- Баланс тем (политика/экономика/международка/конфликты) в итоговом посте.
- В каждом пункте публикуется ссылка на исходную новость.
- Публикация всех важных новостей с автоделением на несколько сообщений при длинной сводке.
- Публикация по расписанию в часовом поясе `Asia/Vladivostok`.
- Команды управления и статистики, включая `/quality`.

## Архитектура

- `app/main.py` — точка входа.
- `app/bot.py` — инициализация aiogram.
- `app/db.py`, `app/models.py` — БД и ORM-модели.
- `app/repositories.py` — доступ к данным и агрегирование quality-метрик.
- `app/services/collector.py` — сбор новостей + устойчивость к ошибкам источников.
- `app/services/filtering.py` — скоринг релевантности, тема новости, правила отклонения.
- `app/services/summarizer.py` — LLM/без-LLM суммаризация, дедуп и topic balancing.
- `app/services/digest_service.py` — orchestration сборки и публикации.
- `app/services/scheduler_service.py` — APScheduler cron jobs.
- `app/handlers/admin.py` — команды управления и статистики.

## Схема БД

Таблицы:

- `sources`
- `raw_news`
- `published_news`
- `rejected_news`
- `stats_daily`
- `stats_weekly`

Создаются автоматически при старте приложения.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Важные переменные `.env`

- `BOT_TOKEN` — токен бота.
- `CHANNEL_ID` — id канала (`@channel_name` или числовой id).
- `ADMIN_USER_IDS` — Telegram user id админов через запятую.
- `DATABASE_URL`:
  - SQLite: `sqlite+aiosqlite:///./news_bot.db`
  - PostgreSQL: `postgresql+asyncpg://user:pass@host:5432/dbname`
- `DEDUP_SIMILARITY_THRESHOLD` — порог похожести заголовков (0..1).
- `PER_TOPIC_LIMIT_DAILY` / `PER_TOPIC_LIMIT_WEEKLY` — лимиты пунктов по теме.
- `PUBLISH_ALL_IMPORTANT` — публиковать все важные новости (если `true`) или ограничивать размер одной сводки (если `false`).

### Полностью бесплатный режим

Оставьте:

```env
LLM_ENABLED=false
LLM_API_KEY=
```

Тогда бот будет работать без расходов на внешние LLM API.

### Запуск

```bash
python -m app.main
```

### Запуск без Docker

Рекомендуемый способ запуска (чтобы корректно работали импорты пакета `app`):

```bash
python -m app.main
```

Запуск `python app/main.py` также поддержан в коде как fallback, но в проде лучше использовать модульный запуск.

## Деплой через Docker

В репозитории есть готовый `Dockerfile`.

Сборка образа:

```bash
docker build -t news-digest-bot .
```

Запуск контейнера:

```bash
docker run -d --name news-digest-bot --restart unless-stopped --env-file .env news-digest-bot
```

В контейнере бот запускается командой:

```bash
python -m app.main
```

Это устраняет ошибку `ModuleNotFoundError: No module named app`, которая возникает при запуске файла напрямую как `/app/app/main.py` на некоторых хостингах.

## Планировщик

- Сбор источников: каждые 30 минут.
- Ежедневная публикация: каждый день в `DAILY_PUBLISH_HOUR` (Asia/Vladivostok).
- Недельная публикация: воскресенье в `WEEKLY_PUBLISH_HOUR` (Asia/Vladivostok).

## Команды управления (русские)

- `/addsource <rss|site|api> <имя> <url> [json_meta]` — добавить источник.
- `/removesource <id>` — удалить источник.
- `/stat` — статистика за день.
- `/statweek` — статистика за неделю.
- `/quality` — метрики качества за день: raw/rejected/dedup/topic split.

> Если сводка не помещается в один пост Telegram, бот автоматически отправляет продолжение в следующих сообщениях.

## Примеры источников

```text
/addsource rss rbc https://rssexport.rbc.ru/rbcnews/news/30/full.rss
/addsource rss interfax https://www.interfax.ru/rss.asp
/addsource site vedomosti https://www.vedomosti.ru {"selector":"article","title_selector":"h2"}
/addsource api govapi https://example.gov/api/news
```

## Рекомендации

- Используйте федеральные и международные источники + официальные ведомства.
- Для `site` задавайте точные `selector`/`title_selector`.
- Регулярно смотрите `/quality` и выключайте шумные источники.
