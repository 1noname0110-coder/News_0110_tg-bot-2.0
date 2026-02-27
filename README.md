# Telegram News Digest Bot (RU)

Production-ориентированный Telegram-бот на Python для автономной публикации сухих политико-экономических сводок (дневных и недельных) в канал.

## Ключевые свойства

- Автосбор новостей из источников трех типов: `rss`, `site`, `api`.
- Интеллектуальная фильтрация и отсев шумовых/локальных событий.
- Конфликтный guardrail: блок тактических деталей и эмоционального шума.
- Дедупликация учитывает exact-key и similarity-key без дублирования вычислений в pipeline.
- Бесплатный fallback-суммаризатор (без внешнего LLM API).
- Баланс тем (политика/экономика/международка/конфликты) в итоговом посте.
- В каждом пункте используется единый HTML-формат: `[Тема] Заголовок` + `<a href="...">Источник</a>` (во всех режимах генерации).
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

Ключевые ограничения и индексы:

- `raw_news.source_id` → `sources.id` (FK).
- `rejected_news.raw_news_id` → `raw_news.id` (FK).
- `rejected_news.source_id` → `sources.id` (FK).
- `raw_news`: `UNIQUE(source_id, external_id)`, индекс `ix_raw_news_published_at(published_at)`.
- `published_news`: индекс `ix_published_news_period(period_type, period_start, period_end)`.
- `rejected_news`: `UNIQUE(raw_news_id)`, индексы `ix_rejected_news_rejected_at(rejected_at)` и `ix_rejected_news_raw_news_id(raw_news_id)`.

Создаются автоматически при старте приложения. Для существующих инсталляций добавлена SQL-миграция `migrations/20260224_add_fk_and_indexes.sql`.

## Установка

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env
```

## Важные переменные `.env`

> Обязательные переменные для старта: `BOT_TOKEN`, `CHANNEL_ID` (или `CHAT_ID` / `TELEGRAM_CHANNEL_ID`) и `ADMIN_USER_IDS`.
> Если `CHANNEL_ID` или `ADMIN_USER_IDS` отсутствуют или пустые, приложение завершит запуск с ошибкой валидации конфигурации.

- `BOT_TOKEN` — токен бота.
- `CHANNEL_ID` — id канала (`@channel_name` или числовой id).
  - также поддержаны `CHAT_ID` и `TELEGRAM_CHANNEL_ID` как альтернативные имена переменной.
- `ADMIN_USER_IDS` — **обязательный** список Telegram user id админов через запятую (только числа), например: `123456789,987654321`.
- `DATABASE_URL`:
  - SQLite: `sqlite+aiosqlite:///./news_bot.db`
  - PostgreSQL: `postgresql+asyncpg://user:pass@host:5432/dbname`
- `MIN_PUBLISH_SCORE` — минимальный скор для публикации в fallback-волнах отбора.
- `HIGH_CONFIDENCE_MIN_COUNT_DAILY` / `HIGH_CONFIDENCE_MIN_COUNT_WEEKLY` — целевой минимум high-confidence новостей до fallback.
- `DEDUP_THRESHOLD_SAME_SOURCE` / `DEDUP_THRESHOLD_CROSS_SOURCE` — пороги dedup для одного и разных источников.
- `PER_TOPIC_LIMIT` — общий лимит пунктов на тему.
- `MAX_PERIOD_NEWS` — ограничение числа новостей из БД за период.

### Полностью бесплатный режим

Оставьте:

```env
LLM_ENABLED=false
LLM_API_KEY=
```

Тогда бот будет работать без расходов на внешние LLM API.

Редкие тумблеры зафиксированы безопасными дефолтами: профиль фильтра = `balanced`, timeout HTTP-клиента = 20 секунд.

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

Если в панели хостинга случайно указали ID канала как имя переменной (например, ключ `-100...`), бот теперь автоматически распознает это и подставит как `CHANNEL_ID`.

## Границы периодов

- Во всех выборках и публикациях период трактуется как полуинтервал `[start, end)`: начало включительно, конец исключительно.
- Новость с `published_at == end` не входит в текущий период и попадёт в следующий, начинающийся с этого же `end`.
- Для production cron-публикаций используются **фиксированные календарные границы** в локальной таймзоне, затем они переводятся в UTC:
  - `daily`: `day_start = 00:00 local`, `day_end = day_start + 1 day`.
  - `weekly`: `week_start = понедельник 00:00 local`, `week_end = week_start + 7 days`.
- Благодаря фиксированным `start/end` антидубль (`is_period_already_published`) получает одинаковые границы для одного календарного периода и не допускает повторной production-публикации.
- Режим «по текущий момент» вынесен отдельно: preview-методы используют `now_local` как верхнюю границу и не предназначены для production cron-публикаций.

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
