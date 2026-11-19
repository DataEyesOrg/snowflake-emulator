# План реализации

Локальный эмулятор Snowflake: самостоятельный сервер на Python + DuckDB +
sqlglot, совместимый по протоколу с драйверами Snowflake.

## Состояние

Фаза 1 закрыта, идёт фаза 2. Сделано и проверено автотестами на
`snowflake-connector-python` 5.2, 3.15 и 4.8 и на Snowflake CLI 3.34:

- [x] логин, сессия, логаут (продление токена и heartbeat проверены только сырым HTTP)
- [x] `SELECT`, `INSERT` / `UPDATE` / `DELETE`, `CREATE` / `DROP` для баз, схем, таблиц и view
- [x] `USE`, `ALTER SESSION`, параметры сессии из логина, транзакции, `CURRENT_*()`
- [x] результаты в формате JSON, базовые типы, коды ошибок Snowflake
- [x] строгий режим: неподдержанный оператор получает `000002 (0A000)`
- [x] результаты в Arrow: row-режим, `fetch_pandas_all`, `get_result_batches`
      (структура epoch/fraction и метаданные полей сверены с декодерами
      коннекторов и с fakesnow, Apache-2.0)
- [x] большие результаты чанками: JSON и Arrow, скачивание по URL
- [x] проверка на Snowflake CLI и коннекторах 3.x / 4.x в CI
      (`.github/workflows/ci.yml`)
- [x] Docker-образ: собирается и отвечает коннектору, проверяется в CI
- [x] бинды: числа, строки, даты, время, binary; `executemany` через
      describe-only и массивные бинды
- [x] строгий режим внутри запросов: неизвестные функции, табличные функции
      DuckDB и `INFORMATION_SCHEMA` отклоняются, имена таблиц чувствительны
      к регистру, у DuckDB нет доступа к файлам и сети

Фаза 2, сделано (dbt Core 1.11 + dbt-snowflake и dbt Fusion 2.0, оба в
автотестах; jaffle_shop — отдельным шагом в CI):

- [x] `SHOW` для баз, схем, объектов, таблиц и view; `DESCRIBE TABLE` / `VIEW`
- [x] `INFORMATION_SCHEMA`: `SCHEMATA`, `TABLES`, `VIEWS`, `COLUMNS`
- [x] `MERGE`, `INSERT OVERWRITE`
- [x] `ALTER TABLE` / `VIEW`: переименование, колонки, комментарии; `COMMENT ON`
- [x] transient- и временные таблицы; `GRANT` / `REVOKE` как no-op
- [x] DDL фиксирует открытую транзакцию, как в Snowflake
- [x] `dbt seed` / `run` / `test` / `snapshot` / `build` / `docs generate`
      зелёные на jaffle_shop и на тестовом проекте `tests/dbt_project`
- [x] multi-statement в протоколе (`MULTI_STATEMENT_COUNT`), результат и
      статус запроса по query ID
- [x] `SHOW COLUMNS` / `PARAMETERS` / `PRIMARY KEYS`; первичные ключи
      запоминаются и не навязываются
- [x] `ALTER TABLE ... SWAP WITH`, переименование в другую схему той же базы
- [x] `RESULT_SCAN` и `LAST_QUERY_ID` (в том числе `get_results_from_sfqid`
      на коннекторах 3.x / 4.x)
- [x] `SHOW UNIQUE KEYS` / `IMPORTED KEYS`; уникальные и внешние ключи
      запоминаются и не навязываются
- [ ] один реальный dbt-проект — вторая половина критерия готовности
- [ ] переименование в другую базу; `SHOW EXPORTED KEYS`,
      `DROP UNIQUE (...)` / `DROP FOREIGN KEY (...)`

Фаза 0 пропущена и остаётся открытой: спайк на fakesnow и conformance-тесты
против настоящего аккаунта ещё не сделаны.

## Почему не на коде LocalStack

- Snowflake-эмулятор LocalStack закрыт: community-версии нет, образ
  `localstack/snowflake` проприетарный. Его код не используем и не изучаем.
- Ядро LocalStack в архиве с 23 марта 2026, а актуальный дистрибутив требует
  auth-токен. Расширение внутри их контейнера означало бы зависимость от
  платного продукта.
- У LocalStack берём подход: один контейнер, настоящие драйверы без правок
  кода, init-скрипты, сброс состояния, parity-тесты на снапшотах
  (их библиотека snapshot-тестирования открыта под Apache 2.0).

## Что уже есть в опенсорсе

| Проект | Стек | Состояние |
|---|---|---|
| [fakesnow](https://github.com/tekumara/fakesnow) | Python, DuckDB, sqlglot | Apache-2.0, есть HTTP-сервер; нет access control и процедур, semi-structured, стейджи и COPY INTO частично |
| [nnnkkk7/snowflake-emulator](https://github.com/nnnkkk7/snowflake-emulator) | Go, DuckDB | MIT, gosnowflake и REST API v2 |
| [Embucket](https://github.com/embucket/embucket) | Rust, DataFusion | Движок под AWS Lambda и S3 Tables, не локальный эмулятор |

Свой проект оправдан тем, чего у них нет: процедуры и Snowflake Scripting,
RBAC, строгий режим. Трансформации SQL из fakesnow можно переносить с
атрибуцией, лицензии совместимы.

## Архитектура

1. **Протокол** (`sfemu/server`). Внутренний API драйверов: `login-request`,
   `query-request`, `token-request`, heartbeat, закрытие сессии; позже статус
   и результат запроса по query ID и публичный SQL API v2.
2. **Сессии** (`sfemu/session.py`). Токены, текущие database / schema /
   warehouse / role, параметры сессии. У каждой сессии свой курсор DuckDB.
3. **SQL-слой** (`sfemu/sql`). Парсинг диалекта Snowflake через sqlglot,
   затем транспиляция в DuckDB или обработка самим эмулятором.
4. **Каталог** (`sfemu/engine.py`). База Snowflake — подключённая база DuckDB,
   поэтому трёхчастные имена работают нативно. Объекты без данных
   (warehouses, roles, stages) — во внутреннем хранилище метаданных, его ещё нет.
5. **Типы** (`sfemu/types.py`). Маппинг типов DuckDB в метаданные и значения
   Snowflake.
6. **Стейджи.** Внутренние на локальном диске, внешние S3 через httpfs.
7. **Поставка.** CLI, pytest-хелпер (`sfemu.testing`), Docker-образ, позже
   модуль для Testcontainers.

## Фазы

Оценки грубые, на одного инженера.

| Фаза | Содержание | Критерий готовности | Срок |
|---|---|---|---|
| 0. Спайк | Целевой сценарий на fakesnow, список падений; каркас conformance-тестов | Бэклог из реальных падений | 2–3 дня |
| 1. Каркас | Логин, сессия, `SELECT`, DDL/DML, базовые типы, JSON, затем Arrow | Python-коннектор и Snowflake CLI подключаются; работает `fetch_pandas_all` | 1–2 недели |
| 2. dbt | `SHOW` / `DESCRIBE`, `INFORMATION_SCHEMA`, `MERGE`, `ALTER TABLE`, transient-таблицы, `GRANT` как no-op, multi-statement | `dbt seed/run/test/snapshot` зелёные на jaffle_shop и одном реальном проекте | 3–5 недель |
| 3. Semi-structured и функции | `VARIANT`, путь через `:`, `FLATTEN`, `OBJECT_CONSTRUCT`, `TRY_*`, даты и таймзоны | Согласованный процент conformance-корпуса | 3–4 недели, дальше хвост |
| 4. Загрузка | `PUT` / `GET`, file formats, `COPY INTO`, внешние стейджи | Загрузка CSV / Parquet / JSON со стейджа | 2–3 недели |
| 5. Другие драйверы | JDBC, Go, Node.js, ODBC; SQL API v2; key-pair / JWT и OAuth без проверки; async-запросы | Матрица драйверов в CI | 2–3 недели |
| 6. Продвинутое | Процедуры и Snowflake Scripting, UDF, streams / tasks, time travel, clone, RBAC, Snowpark | По одной фиче, по приоритету | Каждая от недели |

Сквозные задачи: CI, персистентность (память или файл), эндпоинт сброса
состояния, таблица совместимости.

## Риски

- **Длинный хвост диалекта.** Основная стоимость — семантика: регистр
  идентификаторов, точность `NUMBER`, таймзоны, порядок `NULL`. Нужен
  conformance-корпус со снапшотами с настоящего аккаунта.
- **Ложноположительные тесты.** Запрос, прошедший локально, не должен падать
  на Snowflake. Отсюда строгий режим.
- **Недокументированный протокол.** Внутренний API драйверов официально не
  описан и меняется между версиями. Коннектор Python 5.x ходит в сеть через
  нативное ядро `sf_core`, более старые версии — через собственный HTTP-код,
  поэтому в CI нужна матрица версий.
- **Название.** `snowflake-emulator` совпадает с проектом nnnkkk7 и содержит
  чужой товарный знак. Перед публичным релизом вопрос нужно показать юристу.

## Открытые решения

- Первый целевой сценарий: dbt, pytest с Python-коннектором или JDBC. От него
  зависит порядок фаз 2–5.
- Целые типы: решено, `INT` / `BIGINT` / `SMALLINT` / `TINYINT` / `BYTEINT` —
  это `DECIMAL(38, 0)`, как и `NUMBER`. Открытым остаётся тип целых литералов
  (у DuckDB они 32- и 64-битные, поэтому `2147483647 + 1` переполняется).
- Ошибка внутри транзакции: DuckDB срывает транзакцию целиком, Snowflake
  продолжает её. Без savepoint-ов в DuckDB это не эмулируется.
- Метаданные каталога: времени создания, владельцев и признака transient нет,
  DuckDB их не хранит. Нужно собственное хранилище метаданных (оно же
  понадобится для warehouses, ролей и стейджей).
- Целочисленное деление: в DuckDB результат `DOUBLE`, в Snowflake `NUMBER(p, 6)`.
- Регистр имён колонок: DuckDB сравнивает их без учёта регистра, проверка
  сделана только для таблиц и view.
- Функции: отклоняются те, которых sqlglot не знает. Функции из других
  диалектов, которые он знает, проходят; нужен список функций Snowflake.
- `TIMESTAMP_TZ`: в DuckDB нет типа с сохранённым смещением.
- Часовой пояс по умолчанию: сейчас `UTC`, у Snowflake `America/Los_Angeles`.
- Размер чанка результатов: сейчас строки (`CLIENT_RESULT_CHUNK_SIZE` = 160,
  как у Snowflake в строках), JDBC читает этот параметр как мегабайты — при
  работе с JDBC стоит свериться с его бюджетом памяти.
