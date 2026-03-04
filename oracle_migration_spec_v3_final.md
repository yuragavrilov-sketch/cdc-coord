# Oracle Online Migration System — Финальная спецификация

> Онлайн-миграция Oracle → Oracle без остановки продакшн-нагрузки

---

## Содержание

1. [Общее описание](#1-общее-описание)
2. [Архитектура системы](#2-архитектура-системы)
3. [Алгоритм миграции таблицы](#3-алгоритм-миграции-таблицы)
4. [Схема базы данных (PostgreSQL)](#4-схема-базы-данных-postgresql)
5. [Компоненты воркера](#5-компоненты-воркера)
6. [Координатор (Flask)](#6-координатор-flask)
7. [Отказоустойчивость](#7-отказоустойчивость)
8. [Конфигурация](#8-конфигурация)
9. [Мониторинг и алерты](#9-мониторинг-и-алерты)
10. [Процедура Cutover](#10-процедура-cutover)

---

## 1. Общее описание

Система обеспечивает онлайн-миграцию данных из Oracle (source) в Oracle (target) без остановки продакшн-нагрузки. Миграция может продолжаться несколько дней или недель — все изменения в source непрерывно отражаются на target.

### 1.1 Режимы миграции

| Режим | Когда использовать | Компоненты |
|-------|-------------------|-----------|
| **CDC** | Активные таблицы с постоянными изменениями | Debezium + Kafka + CDC consumer + bulk chunks |
| **STATIC** | Статичные таблицы без изменений (справочники, архивы) | Только bulk chunks (без Debezium/Kafka) |

**Выбор режима:** оператор выбирает режим **вручную** при создании migration job в Web UI. Система НЕ определяет режим автоматически.

### 1.2 Принцип работы

#### CDC режим (активные таблицы)

| Фаза | Что происходит | Длительность |
|------|---------------|--------------|
| **1. Фиксация SCN** | Записать `CURRENT_SCN` из source Oracle | Момент времени |
| **2. Запуск Debezium** | Создать коннектор, начать стриминг в Kafka (события **не применяются**) | 10-60 сек |
| **3. Bulk Load** | Перенос данных `AS OF SCN` — точный snapshot. Параллельно N воркеров | Часы/дни |
| **4. CDC Apply** | Применение накопленных событий из Kafka (с начала топика) | Минуты/часы |

#### STATIC режим (неизменяемые таблицы)

| Фаза | Что происходит |
|------|---------------|
| **1. Нарезка чанков** | Создать ROWID-чанки |
| **2. Bulk Load** | Перенос данных обычным `SELECT` (без AS OF SCN). Параллельно N воркеров |
| **3. Завершение** | Статус → `completed` |

**Ключевые отличия:**
- Debezium запускается только для CDC режима
- STATIC не фиксирует SCN, не использует Kafka
- CDC требует UNDO retention, STATIC — нет

---

## 2. Архитектура системы

### 2.1 Компоненты

| Компонент | Технология | Назначение |
|-----------|-----------|-----------|
| Координатор | Flask + PostgreSQL | Web UI, создание jobs, генерация SQL шаблонов, мониторинг |
| Воркер | Python + Docker | Выполнение bulk и CDC; гомогенны — любой берет любую роль |
| CDC стриминг | Debezium + Kafka | Захват изменений из Oracle redo logs (только CDC режим) |
| Координационная БД | PostgreSQL | Jobs, chunks, SQL templates, Kafka offsets |
| Source DB | Oracle | Исходная база, не изменяется в процессе миграции |
| Target DB | Oracle | Целевая база, только бизнес-данные (без служебных колонок) |

### 2.2 Роли воркеров

Все воркеры гомогенны. Роль определяется динамически при взятии задания из PostgreSQL.

| Фаза job | Кто работает | Что делает |
|----------|-------------|-----------|
| **Phase 1** (cdc_accumulating) | Никто | Debezium пишет в Kafka, воркеры не трогают |
| **Phase 2** (bulk_loading) | Все доступные воркеры | Берут bulk-чанки через `FOR UPDATE SKIP LOCKED` |
| **Phase 3** (cdc_catchup) | Один CDC owner | Применяет накопленный CDC из Kafka |
| **Phase 4** (cdc_streaming) | Один CDC owner | Непрерывный CDC до cutover |


**Переходы между фазами:**

```
pending → cdc_accumulating (координатор создаёт Debezium)
       ↓
bulk_loading (координатор нарезает чанки)
       ↓
cdc_catchup (CDC owner берёт ownership, применяет backlog)
       ↓
cdc_streaming (непрерывный CDC)
       ↓
completed (cutover)
```

**Фазовость:** во время bulk-фазы CDC consumer не запускается вообще. Debezium пишет в Kafka, но воркеры не читают. После завершения bulk один воркер берет CDC ownership и начинает применять накопленные события с начала топика.

**Ограничения:**
- Bulk чанки: N воркеров параллельно (любое количество)
- CDC: только 1 воркер на job (гарантия порядка событий)

---

## 3. Алгоритм миграции таблицы

### 3.1 CDC режим — последовательность

**Координатор:**

| Шаг | Действие |
|-----|----------|
| **1. Фиксация SCN** | `SELECT CURRENT_SCN FROM V$DATABASE` в Oracle source. Сохранить в `migration_boundaries`. Это точка разреза: до неё — bulk (AS OF SCN), после — CDC. |
| **2. Подготовка JOB** | Генерация имен: `debezium_connector_name`, `kafka_topic_name`, `consumer_group_name`, Создание job record с `migration_mode='cdc'`, Генерация SQL шаблонов из DDL source, Подготовка Debezium конфигурации (из шаблона + специфичные параметры), Сохранение конфигурации в `debezium_config` |
| **3. Запуск Debezium** | Создать коннектор через Kafka Connect REST API: `snapshot.mode=no_data`, `offset.scn=SCN_cutoff`. Debezium начинает стримить изменения в Kafka начиная с зафиксированного SCN. **События накапливаются в Kafka, но не применяются на target.** |
| **4. Ожидание старта** | Polling статуса коннектора до `RUNNING`. Таймаут 60 сек. Только после `RUNNING` продолжать — иначе возможна потеря событий. |
| **5. Нарезка чанков** | Создать ROWID-чанки через `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` для всей таблицы (без ограничения по max_rowid). Записать в `migration_chunks` со статусом `pending`. |
| **6. Обновление статуса** | Обновление статуса → `bulk_running`. Воркеры начинают обрабатывать bulk-чанки. |

**Воркеры:**

| Шаг | Действие | Фаза |
|-----|----------|------|
| **7. Bulk обработка** | Все свободные воркеры берут bulk-чанки. Читают source **AS OF SCN scn_cutoff**. MERGE в target. | Phase 2 |
| **8. Завершение bulk** | Координатор обнаруживает что все чанки `completed`. Обновляет `job.status = 'cdc_catchup'`. | Phase 2→3 |
| **9. CDC ownership** | Один воркер: `UPDATE migration_jobs SET cdc_owner=worker_id WHERE cdc_owner IS NULL AND status='cdc_catchup'`. | Phase 3 |
| **10. CDC catchup** | CDCConsumer читает Kafka от начала топика до `catchup_target=end_offsets()`. Применяет все накопленные события. Kafka offsets сохраняются в PostgreSQL. | Phase 3 |
| **11. CDC streaming** | После достижения `catchup_target` обновить `job.status = 'cdc_streaming'`. Продолжать читать Kafka в реальном времени. | Phase 4 |
| **12. Ожидание cutover** | CDC продолжает работать. Координатор или оператор принимает решение о cutover после верификации. | Phase 4 |


### 3.2 STATIC режим — последовательность

**Координатор:**

| Шаг | Действие |
|-----|----------|
| **1. Подготовка JOB** | Создание job record с `migration_mode='static'`, Генерация SQL шаблонов из DDL target (без CDC шаблонов) |
| **2. Нарезка чанков** | | Создать ROWID-чанки через `DBMS_PARALLEL_EXECUTE.CREATE_CHUNKS_BY_ROWID` для всей таблицы (без ограничения по max_rowid). Записать в `migration_chunks` со статусом `pending`. **Без фиксации SCN.**|
| **3. Обновление статуса** | Обновить `job.status = 'bulk_running'`. Воркеры начинают брать чанки. |
| **4. Мониторинг** | Периодически проверять завершение всех чанков. |
| **5. Завершение** | Все чанки `completed` → `job.status = 'completed'`. **Debezium не запускается.** |


**Воркеры:**

| Шаг | Действие |
|-----|----------|
| **1. Взятие чанка** | `UPDATE migration_chunks SET assigned_worker=worker_id WHERE status='pending' AND job_id IN (SELECT job_id FROM migration_jobs WHERE migration_mode='static')` |
| **2. Bulk обработка** | Читать из source **обычным SELECT** (без AS OF SCN): `SELECT * FROM table WHERE ROWID BETWEEN :start AND :end`, применяют INSERT APPEND  |
| **3. Завершение чанка** | Обновить статус `completed`. |

---

## 4. Схема базы данных (PostgreSQL)

Все таблицы в схеме `migration_system`.

### 4.1 migration_jobs

```sql
CREATE TABLE migration_system.migration_jobs (
    job_id                UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    table_name            VARCHAR(200) NOT NULL,
    target_table_name     VARCHAR(200),
    migration_mode        VARCHAR(20) NOT NULL,  -- 'cdc' или 'static'
    message_key_columns   JSONB,  -- ["order_id", "line_num"] для таблиц без PK

    
    -- CDC-специфичные поля
    scn_cutoff            BIGINT,
    catchup_target        BIGINT, 
    debezium_connector_name VARCHAR(200),
    kafka_topic_name      VARCHAR(200),
    consumer_group_name   VARCHAR(200),
    debezium_config       JSONB,
    
    cdc_owner_worker_id   VARCHAR(100),
    cdc_started_at        TIMESTAMP,
    cdc_heartbeat_at      TIMESTAMP,
    
    status                VARCHAR(20) NOT NULL DEFAULT 'pending',
    config                JSONB,  -- catchup_target_offset и др.
    created_at            TIMESTAMP DEFAULT NOW(),
    completed_at          TIMESTAMP
);

CREATE INDEX ON migration_system.migration_jobs (migration_mode, status);
CREATE INDEX ON migration_system.migration_jobs (debezium_connector_name) 
    WHERE debezium_connector_name IS NOT NULL;
```

**Ключевые поля для CDC:**
- `catchup_target` - end_offsets() при первом старте CDC, для контроля перехода с фазы `cdc_catchup` -> `cdc_streaming`
- `debezium_connector_name` — для опроса статуса через REST API
- `kafka_topic_name` — для подключения CDC consumer
- `consumer_group_name` — уникальный consumer group на job
- `debezium_config` — полная конфигурация для пересоздания при сбое
- `message_key_columns` - Если таблица имеет PK → message_key_columns = NULL (используем PK), Если таблица без PK → оператор обязан указать key columns в UI

### 4.2 migration_chunks

```sql
CREATE TABLE migration_system.migration_chunks (
    chunk_id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    job_id                UUID NOT NULL REFERENCES migration_jobs(job_id),
    table_name            VARCHAR(200) NOT NULL,
    start_rowid           VARCHAR(100),
    end_rowid             VARCHAR(100),
    assigned_worker_id    VARCHAR(100),
    status                VARCHAR(20) NOT NULL DEFAULT 'pending',
    rows_processed        BIGINT DEFAULT 0,
    error_message         TEXT,
    assigned_at           TIMESTAMP,
    completed_at          TIMESTAMP
);

CREATE INDEX ON migration_system.migration_chunks (job_id, status);
```

### 4.3 migration_sql_templates

```sql
CREATE TABLE migration_system.migration_sql_templates (
    job_id              UUID PRIMARY KEY REFERENCES migration_jobs(job_id),
    table_name          VARCHAR(200) NOT NULL,
    
    -- Метаданные
    pk_columns          JSONB NOT NULL,
    all_columns         JSONB NOT NULL,
    insertable_columns  JSONB NOT NULL,  -- без виртуальных
    
    -- SQL шаблоны
    bulk_merge_sql      TEXT NOT NULL,   -- для bulk (CDC режим)
    bulk_insert_sql     TEXT,            -- для bulk (STATIC режим)
    cdc_merge_sql       TEXT,            -- для CDC create/update
    cdc_delete_sql      TEXT,            -- для CDC delete
    
    has_lobs            BOOLEAN DEFAULT FALSE,
    has_timestamps      BOOLEAN DEFAULT FALSE,
    created_at          TIMESTAMP DEFAULT NOW()
);
```

**Назначение:** хранить предварительно сгенерированные SQL запросы. Воркеры используют готовые шаблоны вместо построения SQL в runtime.

**В генерации SQL шаблонов:**
```
pk_columns теперь может быть либо реальный PK, либо message_key_columns
Пример: таблица без PK, оператор указал key_columns = ["order_id", "line_num"]
Тогда MERGE будет: ON (t.order_id = s.order_id AND t.line_num = s.line_num)
```

### 4.4 _migration_offsets

```sql
CREATE TABLE migration_system._migration_offsets (
    consumer_group  VARCHAR(200) NOT NULL,
    topic           VARCHAR(200) NOT NULL,
    partition       INTEGER NOT NULL,
    offset          BIGINT NOT NULL,
    updated_at      TIMESTAMP DEFAULT NOW(),
    PRIMARY KEY (consumer_group, topic, partition)
);
```

**Назначение:** Kafka consumer offsets. Сохраняются атомарно с данными для exactly-once гарантий.

---

## 5. Компоненты воркера

### 5.1 MigrationWorker — главный оркестратор

Точка входа процесса воркера. Запускается как Docker-контейнер.

**Главный цикл:**
1. Попытка взять CDC ownership (для CDC job в статусе `bulk_done`)
2. Если взял — полчуить max_offest() записать в БД, запустить CDC consumer (блокирующий вызов)
3. Иначе — попытка взять bulk-чанк (для job в статусе `bulk_running`)
4. Если взял чанк — обработать (чтение source, применение target)
5. Иначе — idle polling (5 сек)

**Graceful shutdown:**
- SIGTERM → установить shutdown flag
- Дождаться завершения текущего чанка или CDC батча
- Обновить статусы в БД

### 5.2 BulkChunkProcessor — перенос исторических данных

Обрабатывает один ROWID-чанк.

**Алгоритм:**
1. Загрузить SQL шаблоны и метаданные из PostgreSQL (один раз)
2. Сформировать SELECT запрос:
   - CDC режим: `SELECT ... AS OF SCN {scn_cutoff} WHERE ROWID BETWEEN ...`
   - STATIC режим: `SELECT ... WHERE ROWID BETWEEN ...`
3. Читать данные батчами (настраевыемым количеством строк строк)
4. Применять батч на target используя готовый SQL шаблон:
   - CDC режим: `bulk_merge_sql` (MERGE WHEN NOT MATCHED)
   - STATIC режим: `bulk_insert_sql` (INSERT APPEND) с fallback на MERGE при конфликтах
5. Обновить статус чанка → `completed`

**Идемпотентность:** MERGE WHEN NOT MATCHED — повторная обработка чанка безопасна.

### 5.3 CDCConsumer — применение изменений из Kafka

Работает в blocking режиме (не daemon thread).

**Инициализация:**
1. Загрузить SQL шаблоны (`cdc_merge_sql`, `cdc_delete_sql`) и метаданные
2. Создать Kafka consumer с параметрами из job:
   - `topic = kafka_topic_name`
   - `group_id = consumer_group_name`
   - `enable_auto_commit = false`
3. Seek to beginning (offset=0)
4. Если есть сохраненные offsets в `_migration_offsets` — resume с них

**Цикл обработки:**
1. Poll события из Kafka (батч до настраиваемого количества сообщений)
2. Применить батч на Oracle target:
   - `op = c/r/u` → `cdc_merge_sql`
   - `op = d` → `cdc_delete_sql`
3. Сохранить offsets в PostgreSQL (атомарно с данными Oracle)
4. Commit Kafka offsets
5. Проверить catchup completion (если `current_offset >= catchup_target - threshold`)

**Переход catchup → streaming:**
- При старте CDC consumer запомнить `catchup_target = end_offsets()`
- После каждого батча проверять: `lag = catchup_target - current_offset`
- Если `lag <= 0` → обновить статус job → `cdc_done`

---

## 6. Координатор (Flask)

### 6.1 Ответственность

- Web UI для создания и мониторинга jobs
- Pre-migration checks (DDL, constraints, triggers)
- Генерация имен компонентов (коннектор, топик, consumer group)
- Генерация SQL шаблонов из DDL source
- Управление жизненным циклом Debezium (создание, мониторинг, удаление)
- Мониторинг heartbeat CDC воркеров
- Мониторинг зависших чанков
- Координация завершения bulk фазы

### 6.2 Создание CDC job

1. Оператор выбирает режим 'cdc' в UI
2. Координатор запрашивает PK из target DDL
3. Если PK отсутствует:
   - Координатор останавливает создание job
   - Требует от оператора указать key columns
   - Оператор указывает: ["order_id", "line_num"]
   - Координатор валидирует: эти колонки существуют и NOT NULL
4. Если PK присутствует:
   - message_key_columns = NULL
   - Используем реальный PK

**В Debezium конфигурации:**
   ```yaml
    # Если message_key_columns указаны:
    message.key.columns: "{schema}.{table}:order_id,line_num"

    # Если используется PK:
    # параметр не указывается, Debezium использует PK автоматически
   ```
5. Координатор фиксирует SCN в source
6. Генерирует имена:
   - `debezium_connector_name = "migration-{schema}.{table}.{job_id}"`
   - `kafka_topic_name = "{topic_prefix}.{schema}.{table}.{job_id}"`
   - `consumer_group_name = "migration-consumer-{job_id}"`
7. Создает job record в PostgreSQL
8. Анализирует DDL target:
   - Запрашивает `ALL_TAB_COLUMNS`, `ALL_CONSTRAINTS`
   - Определяет PK, виртуальные колонки
   - Строит SQL шаблоны (MERGE, INSERT, DELETE)
   - Сохраняет в `migration_sql_templates`
9. Подготавливает Debezium конфигурацию:
   - Берет шаблон из `config.yaml`
   - Подставляет `log.mining.start.scn = scn_cutoff`
   - Подставляет `table.include.list = schema.table`
   - Сохраняет полную конфигурацию в `debezium_config`
10. Создает Debezium коннектор через REST API
11. Polling статуса до `RUNNING`
12. Нарезает ROWID-чанки
13. Обновляет статус → `bulk_running`
### 6.2.1 Pre-flight checks — проверка PK или key columns
Проверка:
| Проверка | Условие | Ошибка |
|----------|---------|--------| 
| еслиPrimary key или key columns | Запрос к ALL_CONSTRAINTS | PK отсутствует И оператор не указал message_key_columns |
| Key columns валидация | Проверка существования колонок | Указанные key columns не существуют или NULL |
| Key columns уникальность | Рекомендация (warning) | Указанные key columns не уникальны → возможны дубли |

Блокировка создания job:
   - Если нет PK И не указаны key columns → ОШИБКА, job не создается
   - Если указаны key columns И они не существуют → ОШИБКА
   - Если key columns не уникальны → WARNING (оператор подтверждает явно)

### 6.3 Создание STATIC job

1. Оператор выбирает режим 'static' в UI
2. Создает job record (без SCN, без Debezium полей)
3. Анализирует DDL target, генерирует SQL шаблоны (без CDC шаблонов)
4. Нарезает ROWID-чанки
5. Обновляет статус → `bulk_running`

### 6.4 Мониторинг CDC

- Опрос статуса через REST API: `GET /connectors/{debezium_connector_name}/status`
- Если статус `FAILED` → алерт оператору
- Heartbeat воркера: если `cdc_heartbeat_at` не обновлялся > 2 мин → освободить ownership

### 6.5 Координация завершения bulk

Периодически (каждые 30 сек) проверяет незавершенные jobs:

- Если все чанки в `completed` для CDC job → статус `bulk_done`
- Если все чанки в `completed` для STATIC job → статус `completed`

---

## 7. Отказоустойчивость

### 7.1 Идемпотентность операций

| Операция | Гарантия |
|----------|---------|
| Bulk MERGE | `WHEN NOT MATCHED` — повторная обработка чанка безопасна |
| Bulk AS OF SCN | Повторное чтение дает идентичные данные (snapshot на момент SCN) |
| CDC MERGE | `ON (pk)` — повторное применение события безопасно |
| CDC DELETE | `WHERE pk=X` — если строки нет, ничего не происходит |
| Kafka offset | Сохраняется атомарно с данными (двойной коммит Oracle + PostgreSQL) |

### 7.2 Сценарии отказов

| Сценарий | Восстановление |
|----------|---------------|
| Bulk воркер упал | Координатор сбрасывает чанк в `pending` по таймауту (10 мин). Другой воркер подхватит |
| CDC воркер упал | Координатор освобождает ownership по heartbeat timeout (2 мин). Другой воркер берет ownership, resume с сохраненного offset |
| Oracle target недоступен | Воркеры ждут восстановления, retry с backoff |
| Kafka недоступна | CDC воркер ждет восстановления. Bulk продолжает независимо |
| Debezium упал | Координатор обнаруживает `FAILED` статус, может пересоздать коннектор используя сохраненную конфигурацию |

### 7.3 Exactly-once гарантия для CDC

**Двойной коммит:**
1. Oracle target transaction commit
2. PostgreSQL transaction commit (offset save)
3. Kafka consumer commit

Если сбой между шагами 1-2: батч перечитается, MERGE идемпотентен.

---

## 8. Конфигурация

### 8.1 Именование компонентов

**Debezium connector:**
```
migration-{schema}.{table}.{job_id}
```

**Kafka topic:**
```
{topic_prefix}.{schema}.{table}.{job_id}
Пример: migration.PROD_SCHEMA.ORDERS.UUID
```

**Consumer group:**
```
migration-consumer-{job_id}
```

Имена генерируются координатором и сохраняются в `migration_jobs`.

### 8.2 config.yaml — координатор

```yaml
debezium:
  connect_url: "http://kafka-connect:8083"
  
  # Шаблон конфигурации Debezium коннектора
  connector_template:
    connector.class: "io.debezium.connector.oracle.OracleConnector"
    tasks.max: "1"
    
    # Подключение к source Oracle
    database.hostname: "${ORACLE_SOURCE_HOST}"
    database.port: "${ORACLE_SOURCE_PORT}"
    database.user: "${ORACLE_SOURCE_USER}"
    database.password: "${ORACLE_SOURCE_PASSWORD}"
    database.dbname: "${ORACLE_SOURCE_SERVICE}"
    
    # Топик prefix
    topic.prefix: "migration"
    
    # Snapshot режим
    snapshot.mode: "no_data"
    
    # LogMiner
    log.mining.strategy: "online_catalog"
    
    # Heartbeat
    heartbeat.interval.ms: "30000"
    
    # При создании job подставляются:
    # - log.mining.start.scn = {scn_cutoff}
    # - table.include.list = {schema}.{table}
    # - Если message_key_columns указаны:
    #   message.key.columns: "{schema}.{table}:col1,col2"
    # - Если используется PK:
    #   параметр не указывается
kafka:
  bootstrap_servers: "kafka:9092"
  
  topic_config:
    retention_ms: 1209600000  # 2 недели
    cleanup_policy: "delete"
    compression_type: "lz4"
```

### Web UI — форма создания job
```
┌─────────────────────────────────────────────┐
│ Create Migration Job                        │
├─────────────────────────────────────────────┤
│ Source table:  [SCHEMA.ORDERS_STAGING    ]  │
│ Target table:  [SCHEMA.ORDERS_STAGING    ]  │
│                                             │
│ Migration mode: ● CDC (live data)           │
│                 ○ STATIC (archived data)    │
│                                             │
│ This table has NO primary key               │
│                                             │
│ Message key columns (required for CDC):     │
│ [order_id, line_num                      ]  │
│                                             │
│ Note: These columns will be used as message │
│ key in Kafka and as join condition in MERGE │
│                                             │
│ [Pre-flight Check]  [Create Job]            │
└─────────────────────────────────────────────┘
```

**Если PK существует:**
```
│ Primary key detected: (order_id)            │
│                                             │
│ Message key columns: (auto, using PK)       │
```

---

### 8.3 Consumer Group стратегия

**Принципы:**

1. **Один consumer group на job:**
   - Уникальный `consumer_group_name` для каждой таблицы
   - Изолирует offset tracking между миграциями

2. **Один активный CDC воркер:**
   - Только один воркер = `cdc_owner` для job
   - Гарантирует порядок применения событий
   - Критично для консистентности

3. **Один partition на топик:**
   - Debezium `tasks.max: 1`
   - Все события в одном partition
   - Гарантирует строгий порядок

**Failover:**
- Воркер A падает
- Координатор обнаруживает (heartbeat timeout)
- Освобождает ownership: `cdc_owner_worker_id = NULL`
- Воркер B берет ownership
- Resume с сохраненного offset из `_migration_offsets`

**Параллелизм:**
- Bulk: ДА (N воркеров параллельно)
- CDC: НЕТ (1 воркер на job)
- Разные jobs: ДА (изолированы)

### 8.4 Требования к Oracle source

**UNDO retention:**
```sql
-- Для CDC режима (AS OF SCN)
ALTER SYSTEM SET UNDO_RETENTION = 1209600;  -- 2 недели
ALTER TABLESPACE UNDOTBS1 RETENTION GUARANTEE;
```

**Supplemental logging:**
```sql
ALTER TABLE schema.table ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
```

**Archive log retention:**
```
RMAN> CONFIGURE ARCHIVELOG RETENTION POLICY TO RECOVERY WINDOW OF 14 DAYS;
```

---

## 9. Мониторинг и алерты

| Метрика | Источник | Порог | Критичность |
|---------|---------|-------|-------------|
| UNDO retention age | `V$UNDOSTAT` | Транзакция старше retention - 1 час | 🔴 Critical |
| UNDO tablespace | `DBA_TABLESPACE_USAGE_METRICS` | > 85% | 🔴 Critical |
| Debezium status | REST API по `debezium_connector_name` | FAILED/PAUSED | 🔴 Critical |
| Archived log retention | `V$RECOVERY_FILE_DEST` | < 2 дней | 🔴 Critical |
| CDC lag (seconds) | SCN delta | > 300 сек | 🟠 High |
| CDC heartbeat | `cdc_heartbeat_at` | > 2 мин | 🟠 High |
| Kafka consumer lag | Consumer group offsets | > 100K сообщений | 🟠 High |
| Bulk скорость | `rows_processed / elapsed_time` | < 1000 строк/сек | 🟡 Medium |
| Failed chunks | `migration_chunks.status` | > 0 | 🟡 Medium |

---

*Документ описывает архитектуру и логику системы на концептуальном уровне. Детали реализации (конкретный код на Python) разрабатываются отдельно на основе этой спецификации.*
