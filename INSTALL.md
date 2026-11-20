# Установка и запуск ДЗ №5

Поток Prefect запускает распределённое Spark-приложение на кластере из ДЗ №1–4,
читает ранее загруженные события из HDFS, выполняет трансформации и сохраняет
дневную сводку как внешнюю таблицу Hive `hw5.events_daily`.
В комплект входят инструкция, установщик, автоматизированный запуск и проверки.

## Быстрый запуск на существующем стенде

Перенести архив на управляющую машину `team-34-en` (учётная запись и SSH-ключ —
те же, что в прошлых заданиях). Например, с компьютера, где скачан архив:

```bash
scp hw5-prefect.zip team@111.88.130.214:~/
ssh team@111.88.130.214
```

Далее на управляющей машине:

```bash
# Если пакеты уже есть, этот шаг можно пропустить.
sudo apt-get update
sudo apt-get install -y python3-venv curl unzip
unzip hw5-prefect.zip -d ~/
cd ~/hw5-prefect
sudo bash scripts/install.sh
sudo bash scripts/run.sh
```

Установщик использует Spark 3.5.7 из ДЗ №4, а при его отсутствии скачивает
официальный архив Apache и сверяет SHA512. В `/opt/hw5-prefect` создаётся
отдельное Python-окружение с `prefect==3.6.4` и `fastapi==0.119.1`.
Версия FastAPI закреплена: с автоматически установленным FastAPI 0.165.0
Prefect 3.6.4 падал при запуске временного API с ошибкой
`Body() got an unexpected keyword argument min_items`. Системный Python не изменяется.
Для первой установки нужен доступ к Apache Archive и PyPI.

`run.sh` вызывает именно Prefect flow, а не Spark напрямую. При отсутствии
`PREFECT_API_URL` Prefect автоматически запускает временный локальный API.
Аккаунт Prefect Cloud, worker и отдельный сервер для разового запуска не нужны.
История локального Prefect сохраняется в `/var/lib/hw5-prefect`.

Успешное завершение: `HW5_OK application_id=application_...`, затем `HW5_RUN_OK`.
Перед `HW5_RUN_OK` формируется краткий отчёт `evidence/RESULTS.md` из журналов
этого запуска. Скрипт проверяет их полноту и согласованность идентификаторов.
При любой ошибке команда завершается ненулевым кодом; последующие шаги не идут.
Путь к последнему успешному запуску записывается в `evidence/latest.txt`.

## Окружение и исходные данные

Параметры взяты из опубликованных прошлых ДЗ, текущее состояние служб проверяется
при запуске. Требуются уже работающие HDFS, YARN, Hive Metastore и HiveServer2.

| Компонент | Настройка |
|---|---|
| ОС и Java | Ubuntu 24.04, OpenJDK 11 |
| Hadoop | 3.4.2, `/opt/hadoop` |
| Конфигурация HDFS/YARN | `/etc/hadoop-yarn` |
| NameNode | `hdfs://10.34.0.11:9000` из `core-site.xml` |
| ResourceManager | `team-34-en`, адрес из `yarn-site.xml` |
| Рабочие узлы | `team-34-nn`, `team-34-00`, `team-34-01` |
| Spark | `/opt/spark-3.5.7-bin-hadoop3` |
| Hive | 4.0.0-alpha-2, `/opt/hive`, конфигурация `/etc/hive` |
| HiveServer2 | `jdbc:hive2://127.0.0.1:10000/default` |
| Пользователь приложения | `hive`, запуск обёртки через `sudo` |
| Python оркестратора | Python 3.12 стенда, отдельный venv |
| Источник | `/user/hive/warehouse-external/hw3/events` |
| Результат | `/user/hive/warehouse-external/hw5/events_daily` |
| Таблица | `hw5.events_daily` |

Вход — CSV без заголовка с тремя столбцами `event_id,user_id,event_type`.
Дата находится в имени каталога `event_date=YYYY-MM-DD`, например:

```text
/user/hive/warehouse-external/hw3/events/event_date=2026-10-20/<файл>.csv
/user/hive/warehouse-external/hw3/events/event_date=2026-10-21/<файл>.csv
```

Используются существующие данные ДЗ №3. Поток их не загружает повторно.
При отсутствии источника он останавливается. Для восстановления источника
следует выполнить инструкции [ДЗ №3](https://github.com/zarmeux/hw3-hive).

Нужны права `hive` на чтение источника и запись в `hw5` и
`/user/hive/spark-events`, а также доступ рабочих узлов к имени и портам драйвера
на управляющей машине. Ресурсы повторяют ДЗ №4: два executor по 512 MiB heap,
384 MiB overhead и 1 core; ApplicationMaster — 512+128 MiB; драйвер — 512 MiB
heap вне контейнеров YARN. Одновременно другие тяжёлые задания лучше не запускать.

## Последовательность потока и соответствие заданию

В `flows/events_flow.py` определены `@flow` и четыре последовательные `@task`:

| Задача Prefect | Действие | Пункт задания |
|---|---|---|
| `01-check-hdfs-yarn` | Проверка HDFS, источника и доступности YARN, подготовка каталогов | 2 |
| `02-spark-yarn-read-transform-write` | `spark-submit --master yarn --deploy-mode client`; одна SparkSession читает HDFS, обрабатывает строки, пишет Parquet | 1, 2, 3, 4 |
| `03-register-hive-table` | Beeline создаёт внешнюю таблицу над Parquet и синхронизирует партиции | 5 |
| `04-verify-yarn-spark-hive` | Проверка YARN, исполнителей и чтение результата через Hive | Проверка всех пунктов |

Вторая задача выполняет сессию, чтение, трансформации и запись в одном Spark-процессе.
DataFrame и SparkSession не передаются между задачами Prefect. Режим `client`
означает, что драйвер находится на edge; executor работают в контейнерах YARN.
Код проверяет `sparkContext.master == 'yarn'` и HDFS-схему обоих путей.
Адреса кластера берутся из Hadoop XML, который Spark распространяет контейнерам.

Трансформации в `jobs/events_daily.py`:

1. Удаление пробелов и приведение `event_type` к нижнему регистру.
2. Преобразование ID в `BIGINT`, даты — в `DATE`.
3. Проверка ID, даты и формата типа события. При любой ошибочной строке запись отменяется.
4. Удаление полных дубликатов события, пользователя, типа и даты.
5. Группировка по дню: число событий, уникальных пользователей, просмотров,
   регистраций, покупок и доля покупок среди событий дня.
6. Запись Parquet с партициями `event_date=YYYY-MM-DD`.

Сохранение таблицы входит в поток. Spark 3.5.7 не использует клиент Hive 4
Metastore напрямую: как в ДЗ №4, Beeline регистрирует те же Parquet-файлы в
существующем Hive. Поэтому не создаётся отдельный локальный Derby-каталог,
а результат доступен стандартным клиентам HiveServer2.

## Ожидаемая таблица

Для пяти исходных событий из ДЗ №3:

| event_date | events_count | unique_users | page_views | signups | purchases | purchase_share |
|---|---:|---:|---:|---:|---:|---:|
| 2026-10-20 | 3 | 2 | 1 | 1 | 1 | 0.333333 |
| 2026-10-21 | 2 | 2 | 1 | 1 | 0 | 0.0 |

Это ожидаемый пример, а не заявление о выполненном запуске. Если источник
изменился, проверка сравнивает Hive с фактическим расчётом Spark для текущих данных.
`purchase_share` — доля событий покупки, а не пользовательская конверсия.

## Проверка результата и артефакты запуска

`scripts/run.sh` автоматически сохраняет в отдельный каталог `evidence/<run_id>/`:

| Файл | Что подтверждает |
|---|---|
| `prefect.log` | Задачи Prefect, их последовательность и итоговый статус |
| `spark.log`, `metrics.json` | SparkSession, application ID, HDFS URI, количество строк и результат |
| `nodes.log`, `yarn-application.log` | Узлы YARN, тип `SPARK`, `Final-State: SUCCEEDED` |
| `executors.json` | Исполнители и успешно выполненные задачи из Spark event log |
| `input-hdfs.log`, `output-hdfs.log` | Источник и записанные файлы Parquet |
| `hive-register.log` | Создание таблицы, список партиций и описание |
| `hive-rows.tsv`, `hive-client.log` | Чтение зарегистрированной таблицы клиентом Hive |
| `verification.json` | Совпадение каждого показателя каждой даты в Hive и Spark |
| `flow-result.json` | Итог успешного потока с application ID |

После реального запуска для сдачи приложить код и инструкцию, `prefect.log`,
`metrics.json`, `yarn-application.log`, `executors.json`, `hive-register.log`,
`hive-rows.tsv`, `verification.json` и `flow-result.json` из одного запуска.
Полезен скриншот успешного приложения в ResourceManager через туннель из ДЗ №2.

Для ручной проверки на edge:

```bash
sudo -u hive env JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64 \
  HADOOP_HOME=/opt/hadoop HADOOP_CONF_DIR=/etc/hive/hadoop \
  HIVE_HOME=/opt/hive HIVE_CONF_DIR=/etc/hive \
  /opt/hive/bin/beeline -u jdbc:hive2://127.0.0.1:10000/default -n hive \
  -e 'SHOW PARTITIONS hw5.events_daily; SELECT * FROM hw5.events_daily;'
```

## Повторный запуск и ошибки

Повторить `sudo bash scripts/run.sh`. Все данные пересчитываются; перезаписывается
только результат `hw5/events_daily`, затем Hive синхронизирует партиции.
Исходный `hw3` и результат ДЗ №4 сохраняются. Кэширование Prefect отключено для
всех задач: новый запуск обязательно заново обрабатывает HDFS. Автоматические
повторы отключены, чтобы после ошибки записи можно было сначала проверить причину.

`flock` запрещает два запуска этой работы одновременно на одной edge-машине.
Перезапись Parquet не транзакционная: при записи не следует параллельно читать
или менять таблицу. После сбоя устранить причину и повторить полный поток.
Это учебный сценарий полного пересчёта для небольшого количества партиций.

Если YARN держит приложение в `ACCEPTED`, проверить свободную память и узлы.
Если исполнители не соединяются с драйвером — DNS/`/etc/hosts` и сеть edge.
При `Permission denied` — права `hive` в HDFS. При ошибке Beeline —
`sudo systemctl status hive-metastore hive-hiveserver2` и журналы в `evidence`.
Если `venv` не создаётся — установить `python3-venv`.
Пути для другого стенда меняются в `scripts/common.sh` и SQL LOCATION
`sql/register-table.sql` согласованно. Архив рассчитан на стенд team-34.

## Проверки комплекта и статус

Локальные проверки без Hadoop и Prefect:

```bash
python3 -m compileall -q flows jobs scripts tests
python3 -m unittest discover -s tests -v
for script in scripts/*.sh; do bash -n "$script" || exit; done
```

Тестовые данные синтетические;
эти тесты не исполняют Spark и Prefect. Кластерный запуск подтверждается отдельно
отчётом [evidence/RESULTS.md](evidence/RESULTS.md) и журналами соответствующего запуска.

## Происхождение и документация

Основа обработки, SQL, параметры памяти и клиентские команды адаптированы из
[zarmeux/hw4-spark](https://github.com/zarmeux/hw4-spark), просмотренного 20.11.2026.
В ДЗ №5 добавлены поток Prefect, разделение этапов, автоматическая установка
оркестратора, отдельная таблица hw5 и тесты проверки результатов.
Файлы `jobs/events_daily.py`, `conf/spark-defaults.conf`, SQL, `common.sh`,
`inspect-event-log.py`, `verify.py` и часть установщика основаны на этом репозитории.

- [ДЗ №1 — HDFS](https://github.com/zarmeux/hw1-hdfs).
- [ДЗ №2 — YARN](https://github.com/zarmeux/hw2-yarn).
- [ДЗ №3 — Hive и входные данные](https://github.com/zarmeux/hw3-hive).
- [Prefect: flows и tasks](https://docs.prefect.io/v3/develop/write-flows).
- [Spark 3.5.7 в YARN](https://spark.apache.org/docs/3.5.7/running-on-yarn.html).
- [Spark: поддерживаемые версии Hive Metastore](https://spark.apache.org/docs/3.5.7/sql-data-sources-hive-tables.html).
