# Наименование
## mr-cgdb — профильный агрегатор и фильтр научных статей

# Описание
Система для автоматического сбора, маршрутизации и фильтрации научных статей из arXiv/RSS с последующим профиле-ориентированным кураторством. Пользователи настраивают собственные исследовательские профили, вручную помечают релевантные работы, запускают верификацию через LLM и публикуют подборки.

# Предметная область
1. Сбор и нормализация научных публикаций\
Получение метаданных из arXiv/RSS\
Удаление дублей и унификация источников\
Хранение URL и PDF URL для каждой статьи

2. Персонализированная фильтрация\
Настройка профильных ключевых слов и источников\
Профильный анализ кандидатов (keyword + shadow embedding)\\
Ручная отметка релевантности и теги

3. Верификация и контент-пайплайн\
Очереди фоновых задач (`pdf_download`, `profile_analyze`, `profile_llm_verify`)\
Скачивание PDF, извлечение текста, chunk/summarize\
Профильная LLM-проверка по запросу пользователя

4. Публичная витрина\
Публичные профили и списки релевантных статей\
Выдача кэшированных PDF для публично отмеченных работ

# Данные
1. Структурированные данные (PostgreSQL)\
Пользователи и сессии\
Профили, конфиги, источники\
Корпус статей (`papers`) с embedding и `pdf_url`\
Связи релевантности (`profile_paper_likes`, `profile_paper_analysis`)\
Очередь задач и файлы PDF

2. Внешние данные\
arXiv API и RSS-ленты\
Ollama API (`/api/embed`, `/api/chat`)\
Файловая система для локального PDF-кэша

## Для каждого элемента данных - ограничения
**Таблица `users`**
```sql
users (
  id BIGSERIAL PRIMARY KEY,
  username TEXT NOT NULL UNIQUE,
  password_hash TEXT NOT NULL,
  is_admin BOOLEAN NOT NULL DEFAULT FALSE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
)
```

**Таблица `profiles`**
```sql
profiles (
  id BIGSERIAL PRIMARY KEY,
  user_id BIGINT NOT NULL,              -- FK users(id)
  slug TEXT NOT NULL,
  name TEXT NOT NULL,
  visibility TEXT NOT NULL,             -- public/private
  UNIQUE (user_id, slug)
)
```

**Таблица `papers`**
```sql
papers (
  id BIGSERIAL PRIMARY KEY,
  arxiv_id TEXT UNIQUE,
  doi TEXT UNIQUE,
  weak_key TEXT UNIQUE,
  url TEXT,
  pdf_url TEXT,
  title TEXT NOT NULL,
  abstract TEXT,
  embedding DOUBLE PRECISION[],
  source TEXT NOT NULL
)
```

**Таблица `profile_paper_likes`**
```sql
profile_paper_likes (
  profile_id BIGINT NOT NULL,           -- FK profiles(id)
  paper_id BIGINT NOT NULL,             -- FK papers(id)
  note TEXT NOT NULL DEFAULT '',
  tags TEXT[] NOT NULL DEFAULT '{}',
  liked_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (profile_id, paper_id)
)
```

**Таблица `profile_paper_analysis`**
```sql
profile_paper_analysis (
  profile_id BIGINT NOT NULL,           -- FK profiles(id)
  paper_id BIGINT NOT NULL,             -- FK papers(id)
  keyword_pass BOOLEAN NOT NULL DEFAULT FALSE,
  keyword_hits TEXT[] NOT NULL DEFAULT '{}',
  llm_relevant BOOLEAN,
  shadow_score DOUBLE PRECISION NOT NULL DEFAULT 0,
  PRIMARY KEY (profile_id, paper_id)
)
```

**Таблица `jobs`**
```sql
jobs (
  id BIGSERIAL PRIMARY KEY,
  kind TEXT NOT NULL,                   -- pdf_download/profile_analyze/profile_llm_verify
  status TEXT NOT NULL,                 -- pending/running/failed/done
  payload JSONB NOT NULL DEFAULT '{}',
  error_reason TEXT
)
```

## Общие ограничения целостности
`users.username` — уникальный логин\
`profiles(user_id, slug)` — уникальный профиль пользователя\
`papers.arxiv_id` / `papers.doi` / `papers.weak_key` — уникальные идентичности\
`profile_paper_likes(profile_id, paper_id)` — уникальная пара профиль-статья\
`profile_paper_analysis(profile_id, paper_id)` — уникальная пара профиль-статья\
Каскадная целостность по FK при удалении профилей/пользователей

# Пользовательские роли
- user
- admin

## Для каждой роли - наименование, ответственность, количество пользователей в этой роли?
1. Роль: user (исследователь)
- Регистрация/вход и работа с несколькими профилями
- Настройка ключевых слов и источников профиля
- Ручное добавление релевантных статей
- Запуск `LLM Verify` для конкретных статей

Количество пользователей: Неограничено

2. Роль: admin (администратор)
- Все права user
- Запуск административного arXiv рескана
- Просмотр и ретрай неуспешных задач
- Контроль доступности пайплайна и диагностика

Количество пользователей: Ограничено (~5-10% от общего числа пользователей)

# UI / API
### API (RESTful)
Фреймворк: Chi\
Аутентификация: cookie-session + CSRF\
Формат данных: JSON\
Основные группы эндпоинтов:
```text
/api/auth/...                  # аутентификация
/api/profiles/...              # профили и конфиги
/api/profiles/{id}/analysis... # кандидаты, backfill, verify
/api/profiles/{id}/likes...    # релевантные статьи
/api/public/...                # публичный доступ
/api/jobs/...                  # мониторинг/ретрай задач
```

### Web UI
Интерфейс: SPA (`web/index.html`)\
Основные разделы:\
Explore (публичные профили)\
Candidate papers (кандидаты + фильтры + LLM Verify)\
Relevant (ручной список релевантных)\
Profile settings (ключевые слова, источники, промпты)\
Admin rescan modal

# Технологии разработки
## Язык программирования
### Go (backend), JavaScript/HTML/CSS (frontend)

## СУБД
### PostgreSQL

# Тестирование
- Ручное интеграционное тестирование ingest-конвейера
- Проверка маршрутизации по profile sources + profile keywords
- Проверка жизненного цикла job-очередей и обработки ошибок
- Проверка публичного доступа к профилям и PDF
- Базовые unit-тесты в отдельных сервисах/модулях
