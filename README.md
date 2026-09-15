# Apache Kafka курс 2026: бесплатный курс по Kafka с нуля до профи на русском

<p align="center">
  <img src="https://img.shields.io/badge/Apache%20Kafka-4.3-231F20?logo=apachekafka&logoColor=white" alt="Apache Kafka 4.3">
  <img src="https://img.shields.io/badge/KRaft-без%20ZooKeeper-blue" alt="KRaft без ZooKeeper">
  <img src="https://img.shields.io/badge/язык-русский-red" alt="Курс на русском">
  <img src="https://img.shields.io/badge/цена-бесплатно-brightgreen" alt="Бесплатный курс">
  <img src="https://img.shields.io/badge/уровень-junior%20→%20senior-orange" alt="От junior до senior">
</p>

> **Полный бесплатный курс по Apache Kafka на русском языке.** Теория, практика, Docker, Java, Go и Python, producer и consumer internals, репликация, KRaft, exactly-once, Kafka Streams, Kafka Connect, Schema Registry, мониторинг, безопасность, тюнинг производительности и production-архитектура. Всё в одном README, актуально для **Kafka 4.x (2026)**.

**Kafka обучение без воды:** каждый модуль состоит из понятной теории, схем, команд, которые можно запустить у себя, типичных ошибок и вопросов для самопроверки. Курс подходит, чтобы выучить Kafka с нуля, подготовиться к собеседованию на backend, data engineer или DevOps-позицию и спроектировать надёжную систему на Kafka в продакшене.

⭐ Если курс полезен, поставь звезду репозиторию: так его найдут другие разработчики.

---

## Для кого этот курс по Kafka

| Кто ты | Что получишь |
|---|---|
| **Новичок** в брокерах сообщений | Понимание, что такое Kafka, зачем она нужна и как запустить её за 5 минут |
| **Backend-разработчик** (Java, Go, Python, Node.js, .NET) | Надёжные producer и consumer, обработка ошибок, идемпотентность, transactional outbox |
| **Data engineer** | CDC с Debezium, Kafka Connect, Kafka Streams, схемы данных, event streaming пайплайны |
| **DevOps / SRE** | KRaft-кластер, репликация, мониторинг, алерты, безопасность, тюнинг, Kubernetes |
| **Архитектор / Tech Lead** | Проектирование топиков, event-driven architecture, multi-DC, антипаттерны |
| **Готовишься к собеседованию** | 30 вопросов по Kafka с ответами уровня junior, middle и senior |

## Что ты будешь уметь после курса

- объяснить архитектуру Apache Kafka: broker, topic, partition, offset, replica, ISR, controller, KRaft;
- поднять Kafka-кластер из трёх узлов в Docker и ломать его, наблюдая leader election;
- писать producer и consumer на Java, Go и Python без потери и дублирования сообщений;
- выбирать `acks`, `min.insync.replicas`, `linger.ms`, `batch.size`, compression под задачу;
- понимать at-most-once, at-least-once и exactly-once и реализовывать каждую семантику;
- использовать транзакции Kafka и паттерн transactional outbox;
- строить retry-топики и DLQ (dead letter queue);
- проектировать схему событий и их эволюцию через Schema Registry (Avro, Protobuf);
- подключать базы данных через Kafka Connect и Debezium (CDC);
- писать stream processing на Kafka Streams;
- использовать Share Groups (очереди в Kafka, KIP-932);
- мониторить consumer lag, under-replicated partitions и настраивать алерты;
- включать TLS, SASL/SCRAM, ACL и квоты;
- рассчитывать количество partitions, дисков и брокеров для продакшена.

---

## Полезные ресурсы

Лучшие ресурсы, чтобы не отставать от трендов разработки.

👣 [Golang Go](https://t.me/+5eALlmi6pfo4ZjMy) - авторский канал, посвящённый Go-разработке, DevOps и созданию высоконагруженных сервисов.

⚡ [Machine Learning](https://t.me/+gRRiZ6J044BmNGNi) - ИИ, разбор моделей машинного обучения, RAG, современные LLM, объясняем на пальцах.

🎁 [Продвинутый DEVOPS](https://t.me/addlist/MUtJEeJSxeY2YTFi) - собрали для вас лучшие ресурсы по DevOps на любой вкус в одной папке.

---

## Содержание

- [Для кого этот курс по Kafka](#для-кого-этот-курс-по-kafka)
- [Что ты будешь уметь после курса](#что-ты-будешь-уметь-после-курса)
- [Как проходить курс](#как-проходить-курс)
- [Модуль 0. Что такое Apache Kafka и зачем она нужна](#модуль-0-что-такое-apache-kafka-и-зачем-она-нужна)
- [Модуль 1. Архитектура Kafka: broker, topic, partition, offset](#модуль-1-архитектура-kafka-broker-topic-partition-offset)
- [Модуль 2. Установка Kafka в Docker и первые команды](#модуль-2-установка-kafka-в-docker-и-первые-команды)
- [Модуль 3. Partitions, ключи и порядок сообщений](#модуль-3-partitions-ключи-и-порядок-сообщений)
- [Модуль 4. Репликация Kafka: leader, ISR, acks, min.insync.replicas](#модуль-4-репликация-kafka-leader-isr-acks-mininsyncreplicas)
- [Модуль 5. Kafka Producer: как устроен и как настроить](#модуль-5-kafka-producer-как-устроен-и-как-настроить)
- [Модуль 6. Kafka Consumer и Consumer Groups](#модуль-6-kafka-consumer-и-consumer-groups)
- [Модуль 7. Exactly-once, транзакции Kafka и transactional outbox](#модуль-7-exactly-once-транзакции-kafka-и-transactional-outbox)
- [Модуль 8. Хранение данных: сегменты, retention, log compaction](#модуль-8-хранение-данных-сегменты-retention-log-compaction)
- [Модуль 9. Share Groups: очереди в Kafka](#модуль-9-share-groups-очереди-в-kafka)
- [Модуль 10. Schema Registry, Avro, Protobuf и проектирование событий](#модуль-10-schema-registry-avro-protobuf-и-проектирование-событий)
- [Модуль 11. Kafka Connect и CDC с Debezium](#модуль-11-kafka-connect-и-cdc-с-debezium)
- [Модуль 12. Kafka Streams: потоковая обработка данных](#модуль-12-kafka-streams-потоковая-обработка-данных)
- [Модуль 13. Обработка ошибок: retry, DLQ и poison pill](#модуль-13-обработка-ошибок-retry-dlq-и-poison-pill)
- [Модуль 14. Производительность и тюнинг Kafka](#модуль-14-производительность-и-тюнинг-kafka)
- [Модуль 15. Мониторинг Kafka: метрики, consumer lag, алерты](#модуль-15-мониторинг-kafka-метрики-consumer-lag-алерты)
- [Модуль 16. Безопасность Kafka: TLS, SASL, ACL, квоты](#модуль-16-безопасность-kafka-tls-sasl-acl-квоты)
- [Модуль 17. Kafka в продакшене: архитектура и эксплуатация](#модуль-17-kafka-в-продакшене-архитектура-и-эксплуатация)
- [Модуль 18. Итоговый проект: event-driven интернет-магазин](#модуль-18-итоговый-проект-event-driven-интернет-магазин)
- [Шпаргалка Kafka CLI](#шпаргалка-kafka-cli)
- [Шпаргалка важных настроек](#шпаргалка-важных-настроек)
- [Вопросы на собеседовании по Kafka с ответами](#вопросы-на-собеседовании-по-kafka-с-ответами)
- [FAQ: частые вопросы про Apache Kafka](#faq-частые-вопросы-про-apache-kafka)
- [Глоссарий Kafka](#глоссарий-kafka)
- [Официальные источники и что читать дальше](#официальные-источники-и-что-читать-дальше)

---

## Как проходить курс

1. **Иди по порядку.** Модули 0-4 это фундамент. Без понимания partition, offset и ISR всё остальное будет магией.
2. **Запускай каждую команду.** Kafka учится руками. Прочитать про rebalance и увидеть его в логах это разные уровни понимания.
3. **Ломай кластер.** Останавливай брокеры, убивай consumer, переполняй диск. Именно так появляется production-опыт.
4. **Отвечай на вопросы в конце модуля** вслух, как на собеседовании.
5. **Сделай итоговый проект.** Он собирает все темы в одну систему.

**Что нужно установить:** Docker и Docker Compose, Java 17+ (для примеров на Java и Kafka Streams), Git, любую IDE. Для примеров на Go нужен Go 1.22+, для Python нужен Python 3.10+.

**Версия:** все примеры написаны для Apache Kafka 4.x, образ `apache/kafka:4.3.1`. Kafka 4.x работает только в режиме KRaft, ZooKeeper полностью удалён.

---

# Модуль 0. Что такое Apache Kafka и зачем она нужна

## 0.1 Определение Apache Kafka простыми словами

**Apache Kafka** это распределённая платформа потоковой передачи событий (event streaming platform). Она делает три вещи:

1. **Публикует и подписывается** на потоки событий (как брокер сообщений).
2. **Надёжно хранит** события на диске столько, сколько нужно: часы, дни, годы.
3. **Обрабатывает** потоки событий в реальном времени или перечитывает историю.

Главная идея Kafka: **распределённый append-only log** (журнал, в который можно только дописывать). Всё остальное, от репликации до exactly-once, построено вокруг этой простой структуры.

Kafka была создана в LinkedIn в 2011 году для обработки активности пользователей, передана в Apache Software Foundation и сегодня используется большинством компаний из Fortune 100: банками, маркетплейсами, телекомом, такси, стримингами, игровыми студиями.

## 0.2 Проблема, которую решает Kafka

Представим интернет-магазин. Пользователь оформил заказ, и об этом должны узнать сразу несколько систем:

```text
                    +--> Payment Service
                    |
Order Service ------+--> Warehouse
                    |
                    +--> Analytics
                    |
                    +--> Notification Service
                    |
                    +--> Fraud Detection
```

Без Kafka Order Service вызывает каждый сервис напрямую по HTTP:

```text
Order Service
   |
   +--> POST /payment
   +--> POST /warehouse
   +--> POST /analytics
   +--> POST /notification
   +--> POST /fraud
```

На маленьком проекте это работает. Потом начинаются проблемы:

| Вопрос | Проблема синхронной интеграции |
|---|---|
| Notification Service упал | Заказ падает целиком или теряется уведомление |
| Analytics тормозит 3 секунды | Пользователь ждёт 3 секунды на оформлении заказа |
| Появилось ещё 10 потребителей | Приходится менять и деплоить Order Service |
| Нужно перечитать заказы за вчера | Данные уже нигде не лежат |
| Пик нагрузки в Чёрную пятницу | Нижестоящие сервисы ложатся каскадом |

Это называется **сильная связанность (tight coupling)**: каждый сервис знает о каждом.

## 0.3 Как выглядит та же система с Kafka

```text
Order Service
     |
     | OrderCreated
     v
+-----------------+
|  Kafka topic    |
|  orders         |
+-----------------+
     |      |      |       |
     v      v      v       v
Payment  Warehouse Analytics Fraud
```

Order Service публикует **одно событие** `OrderCreated` и больше ни о ком не знает. Каждый потребитель читает события в своём темпе.

Что мы получили:

- **Слабая связанность.** Новый сервис подключается без изменения Order Service.
- **Буферизация.** Если Analytics упал, события ждут его в Kafka и будут обработаны после восстановления.
- **Сглаживание пиков.** Kafka принимает миллионы событий в секунду, потребители обрабатывают их с комфортной скоростью.
- **Replay.** Можно перечитать события за любой период, пока они хранятся (retention).
- **Масштабирование.** Нагрузка на чтение распределяется между экземплярами сервиса.

## 0.4 Что такое событие (event)

**Событие** это неизменяемый факт о том, что уже произошло.

```json
{
  "event_id": "5f1c2a7e-9b1d-4c1e-8a4e-0c7b2d9f1a11",
  "event_type": "OrderCreated",
  "event_version": 1,
  "occurred_at": "2026-09-15T10:21:43Z",
  "order_id": "order-123",
  "user_id": "user-42",
  "amount": 4990,
  "currency": "RUB"
}
```

Свойства события:

- написано в **прошедшем времени**: `OrderCreated`, `PaymentFailed`, `UserRegistered`;
- **неизменяемо**: событие нельзя отредактировать, можно только опубликовать новое (`OrderCancelled`);
- содержит **время**, когда факт произошёл;
- имеет **уникальный идентификатор**, чтобы потребитель мог отбросить дубликат.

### Событие и команда: не одно и то же

| | Event (событие) | Command (команда) |
|---|---|---|
| Смысл | Это уже случилось | Сделай это |
| Пример | `OrderCreated` | `CreateOrder` |
| Время | Прошедшее | Повелительное наклонение |
| Получателей | Любое число, отправитель их не знает | Обычно один конкретный |
| Можно отказать | Нет, факт уже произошёл | Да, команду можно отклонить |

Kafka отлично подходит для событий. Команды через Kafka тоже передают, но это отдельный стиль интеграции со своими компромиссами.

## 0.5 Kafka это не просто очередь сообщений

Частая ошибка новичков:

```text
Kafka = RabbitMQ для больших нагрузок
```

В классической очереди сообщение **удаляется** после того, как его забрал потребитель. В Kafka сообщение **остаётся в журнале**, а потребитель лишь запоминает позицию (offset), до которой дочитал.

```text
partition-0

offset:  0    1    2    3    4    5
        [A]  [B]  [C]  [D]  [E]  [F]  <-- append (запись в конец)
                        ^
                        |
          consumer group "analytics" дочитала до offset 3

          consumer group "billing" может читать с offset 0 независимо
```

Отсюда главные свойства Kafka:

- **много независимых читателей** одних и тех же данных;
- **повторное чтение** истории;
- **строгий порядок** внутри partition;
- **огромная пропускная способность** за счёт последовательной записи на диск.

## 0.6 Kafka vs RabbitMQ vs HTTP vs Redis Streams

| Критерий | Apache Kafka | RabbitMQ | HTTP/gRPC | Redis Streams |
|---|---|---|---|---|
| Модель | Распределённый лог | Брокер очередей (AMQP) | Запрос-ответ | Лог в памяти |
| Хранение после чтения | Да, по retention | Нет (классические очереди) | Нет | Да, ограничено памятью |
| Replay истории | Да | Ограниченно (Streams) | Нет | Да |
| Пропускная способность | Миллионы msg/s на кластер | Десятки-сотни тысяч msg/s | Зависит от сервиса | Высокая, но ограничена RAM |
| Порядок | Внутри partition | Внутри очереди | Нет | Внутри stream |
| Сложная маршрутизация | Нет, через топики и потребителей | Да (exchanges, routing keys) | Нет | Нет |
| Задержка | Миллисекунды | Субмиллисекунды-миллисекунды | Зависит от сети | Субмиллисекунды |
| Когда выбирать | Event streaming, аналитика, CDC, интеграция микросервисов | Task queues, RPC, сложный роутинг | Синхронный ответ пользователю | Лёгкие стримы внутри одного сервиса |

> Начиная с Kafka 4.2 появились **Share Groups** (очереди в Kafka, KIP-932): потребители могут обрабатывать сообщения одного partition параллельно с поштучным подтверждением. Это закрывает часть сценариев RabbitMQ. Подробно в [модуле 9](#модуль-9-share-groups-очереди-в-kafka).

### Kafka и HTTP вместе

Kafka не заменяет HTTP. Типичная схема:

```text
Client --HTTP--> Order API --(сохраняет заказ, отвечает 201)--> Client
                     |
                     +--event OrderCreated--> Kafka --> остальные сервисы
```

HTTP нужен, когда пользователь ждёт синхронный ответ. Kafka нужна для асинхронного распространения событий.

## 0.7 Где используют Kafka: реальные сценарии

| Сценарий | Как применяется Kafka |
|---|---|
| **Микросервисы** | Event-driven взаимодействие, хореография саг, распространение изменений |
| **Аналитика в реальном времени** | Клики, просмотры, события приложений летят в Kafka, оттуда в ClickHouse, Druid, Pinot, Snowflake |
| **Сбор логов и метрик** | Приложения пишут логи в Kafka, дальше Elasticsearch/OpenSearch, Loki, S3 |
| **CDC (Change Data Capture)** | Debezium читает WAL PostgreSQL или binlog MySQL и публикует изменения таблиц |
| **Финансы и финтех** | Транзакции, антифрод, расчёт балансов, аудит |
| **IoT и телеметрия** | Датчики, геопозиции курьеров и такси, телеметрия автомобилей |
| **Machine Learning** | Online feature store, стриминговые признаки, доставка событий в модели |
| **Интеграция данных** | Центральная шина между legacy-системами, хранилищем данных и новыми сервисами |

## 0.8 Когда Kafka не нужна

Kafka это сложная распределённая система. Не бери её, если:

- у тебя один монолит и пара фоновых задач: хватит очереди в базе или Redis;
- нужен синхронный ответ пользователю: используй HTTP или gRPC;
- нагрузка сотни сообщений в минуту и не нужен replay;
- нужна сложная маршрутизация по заголовкам, приоритеты и TTL на сообщение: посмотри на RabbitMQ;
- в команде нет человека, готового поддерживать кластер, и нет бюджета на managed Kafka.

**Kafka действительно полезна, когда** много производителей и потребителей, нужна история событий, высокая нагрузка, несколько команд работают с одними данными, есть CDC или потоковая аналитика.

## 0.9 Что Kafka НЕ делает за тебя

Kafka даёт инфраструктурные гарантии. Корректность системы всё равно проектируешь ты:

- идемпотентность обработки на стороне приложения;
- схему и версионирование событий;
- стратегию retry и обработку «ядовитых» сообщений (poison pill);
- мониторинг и алерты;
- безопасность и разграничение доступа;
- выбор ключей партиционирования под бизнес-инварианты.

### Вопросы для самопроверки

1. Чем событие отличается от команды?
2. Почему в Kafka сообщение не удаляется после чтения и что это даёт?
3. В каких случаях ты выберешь RabbitMQ вместо Kafka?
4. Почему синхронная цепочка HTTP-вызовов плохо переносит пиковую нагрузку?

---

# Модуль 1. Архитектура Kafka: broker, topic, partition, offset

## 1.1 Главная иерархия

```text
Cluster (кластер)
  └── Broker (сервер Kafka)
        └── Topic (логический поток событий)
              └── Partition (упорядоченный журнал)
                    └── Segment (файл на диске)
                          └── Record (запись)
```

Запомни эту картинку. На ней держится весь курс.

## 1.2 Основные компоненты Kafka

| Компонент | Что это | Аналогия |
|---|---|---|
| **Record** (message) | Одна запись: key, value, headers, timestamp | Строка в журнале |
| **Topic** | Именованный поток записей одного типа | Таблица в БД |
| **Partition** | Упорядоченный неизменяемый журнал внутри топика | Шард таблицы |
| **Offset** | Порядковый номер записи внутри partition | Номер строки |
| **Broker** | Процесс Kafka, хранящий partitions и обслуживающий клиентов | Сервер БД |
| **Cluster** | Группа брокеров | Кластер БД |
| **Controller** | Узел, управляющий метаданными кластера (KRaft) | Мастер метаданных |
| **Producer** | Клиент, который пишет записи | Писатель |
| **Consumer** | Клиент, который читает записи | Читатель |
| **Consumer Group** | Группа consumer, делящих между собой partitions | Пул воркеров |
| **Replica** | Копия partition на другом брокере | Реплика БД |

## 1.3 Структура записи (Record)

```text
+-------------------------------------------+
| Record                                    |
|-------------------------------------------|
| key        : "order-123"   (может быть null)
| value      : {...json/avro/protobuf...}   |
| headers    : trace-id=abc, source=web     |
| timestamp  : 1789460503000                |
| offset     : 42  (назначает broker)       |
| partition  : 3   (выбирает producer)      |
+-------------------------------------------+
```

- **key** определяет partition и, значит, порядок событий. Все записи с одним ключом попадают в один partition.
- **value** это полезная нагрузка. Kafka работает с байтами и не знает формат данных.
- **headers** это метаданные: trace id, тип события, версия схемы.
- **timestamp** бывает `CreateTime` (время создания у producer) или `LogAppendTime` (время записи на брокер), настраивается `message.timestamp.type`.

Записи передаются и хранятся **батчами (RecordBatch)**. Сжатие применяется к целому батчу, поэтому оно так эффективно.

## 1.4 Topic

**Topic** это логическое имя потока событий: `orders`, `payments`, `user-clicks`.

- Топик состоит из одного или нескольких partitions.
- У топика есть собственные настройки: `retention.ms`, `cleanup.policy`, `min.insync.replicas`, `max.message.bytes`.
- Топик не удаляет сообщение после чтения.

**Именование топиков** в продакшене лучше стандартизировать:

```text
<домен>.<сущность>.<тип>.<версия>

shop.orders.events.v1
payments.transactions.events.v1
crm.customers.cdc.v1
shop.orders.events.v1.dlq
```

## 1.5 Partition: единица масштабирования и порядка

**Partition** это отдельный упорядоченный журнал. Каждый partition хранится целиком на одном брокере (плюс реплики на других).

```text
topic: orders (3 partitions)

partition-0:  [0:order-101] [1:order-104] [2:order-108]
partition-1:  [0:order-102] [1:order-105]
partition-2:  [0:order-103] [1:order-106] [2:order-107] [3:order-109]
```

Partition определяет:

- **параллелизм записи**: разные partitions пишутся на разные брокеры;
- **параллелизм чтения**: один partition читается максимум одним consumer в группе;
- **область порядка**: Kafka гарантирует порядок **только внутри partition**;
- **распределение данных** между брокерами;
- **потолок пропускной способности** топика.

Поэтому partition важнее topic. Topic это просто имя, а физика системы живёт в partitions.

## 1.6 Offset

**Offset** это монотонно растущий номер записи **внутри одного partition**.

```text
partition-0: offset 0, 1, 2, 3, 4 ...
partition-1: offset 0, 1, 2, 3 ...
```

Важные следствия:

- offset **не уникален в топике**: offset 5 есть в каждом partition;
- offset **не является ID сообщения**: для дедупликации нужен собственный `event_id`;
- в compacted-топиках и при транзакциях offsets могут идти с пропусками;
- consumer хранит **committed offset** это номер **следующей** записи, которую нужно прочитать.

Ключевые позиции в partition:

```text
 offset:  0   1   2   3   4   5   6   7
         [ ] [ ] [ ] [ ] [ ] [ ] [ ] [ ]
          ^           ^           ^       ^
          |           |           |       |
   log start    committed    high         log end
   offset       offset       watermark    offset (LEO)
                (группы)     (HW)
```

- **Log Start Offset**: самая старая доступная запись (старые удалены retention).
- **Committed Offset**: докуда дочитала конкретная consumer group.
- **High Watermark (HW)**: последняя запись, подтверждённая всеми репликами из ISR. Consumer видит записи только до HW.
- **Log End Offset (LEO)**: позиция, куда будет записана следующая запись у лидера.
- **Consumer lag** = HW − committed offset.

## 1.7 Broker

**Broker** это процесс Kafka (JVM), который:

- принимает записи от producer и пишет их на диск;
- отдаёт записи consumer;
- реплицирует partitions с других брокеров;
- обслуживает группы потребителей (group coordinator);
- хранит offsets групп во внутреннем топике `__consumer_offsets`.

Клиенту достаточно знать адрес одного брокера (`bootstrap.servers`). Он получит метаданные всего кластера и дальше будет ходить напрямую к лидерам нужных partitions.

```text
1. client --> bootstrap broker: Metadata request
2. broker --> client: список брокеров, topics, лидеры partitions
3. client --> leader partition-0 (broker-2): Produce / Fetch
```

> Самая частая ошибка при запуске Kafka в Docker: неправильный `advertised.listeners`. Брокер отдаёт клиенту адрес, по которому клиент потом не может подключиться. Разберём в модуле 2.

## 1.8 KRaft: Kafka без ZooKeeper

До версии 3.x Kafka хранила метаданные (список топиков, лидеров, конфигурации, ACL) в **Apache ZooKeeper**. В **Kafka 4.0 ZooKeeper полностью удалён**, единственный режим работы теперь **KRaft** (Kafka Raft).

```text
            KRaft controller quorum
     +-------------+-------------+-------------+
     | controller-1| controller-2| controller-3|
     |   (leader)  |  (follower) |  (follower) |
     +-------------+-------------+-------------+
              | журнал метаданных __cluster_metadata
              v
     +---------+   +---------+   +---------+
     |broker-1 |   |broker-2 |   |broker-3 |
     +---------+   +---------+   +---------+
```

Как работает KRaft:

- метаданные хранятся как **журнал событий** во внутреннем топике `__cluster_metadata`;
- контроллеры выбирают лидера по протоколу **Raft**;
- активный контроллер принимает решения: создание топиков, выбор лидеров partitions, реакция на падение брокера;
- брокеры получают изменения метаданных инкрементально и держат их в памяти.

Что даёт KRaft:

- одна система вместо двух (не нужно отдельно эксплуатировать ZooKeeper);
- быстрый failover контроллера (секунды вместо минут);
- поддержка **миллионов partitions** в кластере;
- быстрый старт и shutdown брокеров.

**Роли узлов** (`process.roles`):

| Значение | Когда использовать |
|---|---|
| `broker,controller` (combined) | Разработка, тесты, маленькие кластеры |
| `controller` | Продакшен: 3 или 5 выделенных контроллеров |
| `broker` | Продакшен: брокеры с данными |

Кворум из 3 контроллеров переживает падение 1 узла, из 5 контроллеров переживает падение 2 узлов. Чётное количество контроллеров не добавляет отказоустойчивости.

> **Миграция с ZooKeeper:** обновиться с кластера на ZooKeeper сразу до 4.x нельзя. Сначала нужно перейти на 3.9 и мигрировать метаданные в KRaft, и только потом обновляться до 4.x.

## 1.9 Первая ментальная модель

```text
Producer пишет record с key
   -> key хешируется -> выбирается partition
   -> запись уходит лидеру partition
   -> лидер дописывает её в конец лога на диске
   -> followers копируют запись
   -> запись становится доступна consumer (до high watermark)
   -> consumer читает батчами и коммитит offset
   -> запись живёт на диске до истечения retention
```

### Вопросы для самопроверки

1. Почему offset нельзя использовать как глобальный ID сообщения?
2. Что такое high watermark и почему consumer не видит записи после него?
3. Зачем Kafka отказалась от ZooKeeper?
4. Почему для кворума контроллеров берут 3 или 5 узлов, а не 4?

---

# Модуль 2. Установка Kafka в Docker и первые команды

## 2.1 Самый быстрый запуск Kafka (один узел)

```bash
docker run -d --name kafka -p 9092:9092 apache/kafka:4.3.1
```

Образ `apache/kafka` по умолчанию запускает один узел в режиме KRaft с ролями broker и controller, слушающий `localhost:9092`.

Проверяем:

```bash
docker ps
docker logs kafka | grep -i "started"
```

## 2.2 Первый topic, первое сообщение

Создаём топик:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic hello-kafka --partitions 3 --replication-factor 1
```

Смотрим описание:

```bash
docker exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe --topic hello-kafka
```

Пишем сообщения (терминал 1):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka
```

```text
> hello
> my first kafka event
> order-123 created
```

Читаем (терминал 2):

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --from-beginning
```

Ты увидишь все три сообщения, **но порядок может отличаться** от порядка отправки. Почему? У топика 3 partitions, а записи без ключа распределяются по ним. Порядок гарантирован только внутри partition.

## 2.3 Сообщения с ключами

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --property parse.key=true \
  --property key.separator=:
```

```text
> user-1:login
> user-2:login
> user-1:add_to_cart
> user-1:checkout
```

Читаем с выводом ключа, partition и offset:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka --from-beginning \
  --property print.key=true \
  --property print.partition=true \
  --property print.offset=true
```

Все события `user-1` окажутся в одном partition и будут идти строго по порядку.

## 2.4 Кластер из трёх брокеров в Docker Compose

Для изучения репликации нужен настоящий кластер. Создай директорию и файл `docker-compose.yml`:

```bash
mkdir kafka-course && cd kafka-course
```

```yaml
x-kafka-common: &kafka-common
  image: apache/kafka:4.3.1
  restart: unless-stopped

x-kafka-env: &kafka-env
  CLUSTER_ID: "4L6g3nShT-eMCtK--X86sw"
  KAFKA_PROCESS_ROLES: "broker,controller"
  KAFKA_CONTROLLER_QUORUM_VOTERS: "1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093"
  KAFKA_CONTROLLER_LISTENER_NAMES: "CONTROLLER"
  KAFKA_INTER_BROKER_LISTENER_NAME: "INTERNAL"
  KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: "CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,EXTERNAL:PLAINTEXT"
  KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
  KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
  KAFKA_DEFAULT_REPLICATION_FACTOR: 3
  KAFKA_MIN_INSYNC_REPLICAS: 2
  KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
  KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0

services:
  kafka-1:
    <<: *kafka-common
    container_name: kafka-1
    ports: ["19092:19092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 1
      KAFKA_LISTENERS: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:19092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-1:9092,EXTERNAL://localhost:19092"

  kafka-2:
    <<: *kafka-common
    container_name: kafka-2
    ports: ["29092:29092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 2
      KAFKA_LISTENERS: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:29092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-2:9092,EXTERNAL://localhost:29092"

  kafka-3:
    <<: *kafka-common
    container_name: kafka-3
    ports: ["39092:39092"]
    environment:
      <<: *kafka-env
      KAFKA_NODE_ID: 3
      KAFKA_LISTENERS: "INTERNAL://:9092,CONTROLLER://:9093,EXTERNAL://:39092"
      KAFKA_ADVERTISED_LISTENERS: "INTERNAL://kafka-3:9092,EXTERNAL://localhost:39092"

  kafka-ui:
    image: ghcr.io/kafbat/kafka-ui:latest
    container_name: kafka-ui
    ports: ["8080:8080"]
    environment:
      KAFKA_CLUSTERS_0_NAME: "course"
      KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS: "kafka-1:9092,kafka-2:9092,kafka-3:9092"
    depends_on: [kafka-1, kafka-2, kafka-3]
```

Запуск:

```bash
docker compose up -d
docker compose ps
```

Web-интерфейс Kafka UI откроется на `http://localhost:8080`.

## 2.5 Listeners и advertised.listeners: почему клиент не подключается

Это самая частая причина вопросов «Kafka в Docker не работает».

```text
KAFKA_LISTENERS            на каких интерфейсах и портах брокер СЛУШАЕТ
KAFKA_ADVERTISED_LISTENERS какой адрес брокер СООБЩАЕТ клиентам
```

Клиент сначала подключается к `bootstrap.servers`, получает метаданные и затем ходит **на advertised-адреса**. Поэтому:

| Откуда клиент | Какой listener использует | Адрес |
|---|---|---|
| Другой контейнер в той же сети Docker | `INTERNAL` | `kafka-1:9092` |
| Приложение на твоём ноутбуке | `EXTERNAL` | `localhost:19092` |
| Контроллеры между собой | `CONTROLLER` | `kafka-1:9093` |

Если advertised-адрес будет `kafka-1:9092`, приложение на хосте получит его в метаданных и упадёт с ошибкой `UnknownHostException` или бесконечными попытками переподключения.

## 2.6 Проверяем KRaft-кворум

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --status
```

Ты увидишь `LeaderId`, `CurrentVoters`, `HighWatermark` и отставание каждого контроллера:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-metadata-quorum.sh \
  --bootstrap-server kafka-1:9092 describe --replication
```

## 2.7 Где Kafka хранит данные на диске

```bash
docker exec kafka-1 ls /tmp/kraft-combined-logs
```

```text
orders-0/
├── 00000000000000000000.log        # сами записи
├── 00000000000000000000.index      # offset -> позиция в файле
├── 00000000000000000000.timeindex  # timestamp -> offset
├── leader-epoch-checkpoint
└── partition.metadata
```

> В образе `apache/kafka` данные по умолчанию лежат в `/tmp/kraft-combined-logs` внутри контейнера. Для долговременного хранения примонтируй volume и задай `KAFKA_LOG_DIRS`.

## 2.8 Альтернативы для локальной разработки

- **Testcontainers** (`org.testcontainers:kafka`, модуль для Go и Python): Kafka в интеграционных тестах.
- **Redpanda**: Kafka-совместимый брокер на C++, удобен для локальных тестов, но это другой продукт со своими особенностями.
- **Managed Kafka**: Confluent Cloud, Amazon MSK, Aiven, Yandex Managed Service for Apache Kafka, VK Cloud и другие. Для продакшена без собственной команды эксплуатации часто это лучший выбор.

### Практика

1. Подними кластер из трёх брокеров.
2. Создай топик `hello-kafka` с 3 partitions и отправь 10 сообщений с ключами.
3. Найди в Kafka UI, в какой partition попал каждый ключ.
4. Проверь статус KRaft-кворума и определи, какой узел лидер.

---

# Модуль 3. Partitions, ключи и порядок сообщений

## 3.1 Как producer выбирает partition

```text
             key != null                         key == null
                 |                                    |
   partition = murmur2(key) % numPartitions   sticky partitioner:
                 |                            пишет батч в один partition,
                 v                            потом переключается на другой
    одинаковый key -> одинаковый partition     (равномерно и с хорошим батчингом)
```

Правила:

1. Если в записи **явно указан partition**, используется он.
2. Если есть **key**, partition = `murmur2(key) mod число_partitions`.
3. Если key нет, работает **built-in sticky partitioner**: он заполняет батч для одного partition и затем переключается. Это даёт крупные батчи и низкую задержку.
4. Можно написать **собственный Partitioner** (например, для «горячих» ключей).

## 3.2 Почему нет глобального порядка

```text
Отправили: A1, B1, A2, B2, A3

partition-0 (key A): A1 -> A2 -> A3   порядок сохранён
partition-1 (key B): B1 -> B2         порядок сохранён

Consumer может получить: A1, B1, B2, A2, A3
```

Глобальный порядок по всему топику возможен только с **одним partition**, а это убивает масштабирование. Правильный вопрос звучит так: **какой порядок нужен бизнесу?** Обычно порядок нужен **в рамках сущности**: одного заказа, одного счёта, одного пользователя.

## 3.3 Как выбрать ключ партиционирования

| Задача | Хороший ключ | Плохой ключ |
|---|---|---|
| Жизненный цикл заказа | `order_id` | `event_type` (все `OrderCreated` в одном partition) |
| Баланс банковского счёта | `account_id` | `transaction_id` (списания и пополнения перемешаются) |
| Действия пользователя | `user_id` | `country` (Россия займёт один partition) |
| Метрики датчиков | `device_id` | `timestamp` |
| Логи без требований к порядку | `null` | `hostname` при 3 хостах и 50 partitions |

**Задача про банковский счёт.** События по счёту `acc-1`:

```text
Deposit +1000
Withdraw -700
Withdraw -500
```

Если ключ `transaction_id`, события попадут в разные partitions, и consumer может обработать `-500` раньше `+1000`: отказ в операции при достаточном балансе. Ключ `account_id` гарантирует последовательную обработку.

## 3.4 Горячие ключи (hot partitions)

Если один ключ генерирует 40% трафика (крупный продавец, популярный стример), его partition перегружен, а consumer этого partition отстаёт.

Решения:

- **составной ключ** `seller_id + bucket`, где bucket = `hash(order_id) % 8`, если порядок нужен только в рамках заказа;
- вынос горячего клиента в **отдельный топик**;
- **кастомный partitioner** для списка известных горячих ключей;
- пересмотр требований к порядку.

## 3.5 Ловушка: увеличение числа partitions

Число partitions можно только увеличить, уменьшить нельзя.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --alter --topic orders --partitions 12
```

Но формула `hash(key) % N` меняется:

```text
было 6 partitions:  hash("order-123") % 6  = 1
стало 12 partitions: hash("order-123") % 12 = 7
```

Новые события заказа уйдут в partition 7, а старые остались в partition 1. Consumer может обработать новые события раньше старых. **Порядок по ключу ломается на переходный период.**

Как жить с этим:

- закладывай число partitions **с запасом** на 1-2 года роста;
- увеличивай partitions, когда старые данные уже обработаны и сущности «закрыты»;
- для строгих требований создавай **новый топик** и мигрируй потребителей.

## 3.6 Сколько partitions нужно топику

Формула для оценки:

```text
partitions >= max( T / Tp , T / Tc )

T  - целевая пропускная способность топика (MB/s)
Tp - сколько MB/s выдерживает один partition на запись
Tc - сколько MB/s обрабатывает один consumer
```

Пример: нужно 100 MB/s, один consumer обрабатывает 10 MB/s, один partition принимает 50 MB/s.

```text
max(100/50, 100/10) = max(2, 10) = 10 partitions
+ запас на рост x2 = 20 partitions
```

Практические ориентиры:

- маленький топик: 3-6 partitions;
- средний сервис: 12-24 partitions;
- высоконагруженный поток: 50-200+ partitions;
- удобно выбирать числа с большим количеством делителей: 6, 12, 24, 48, 60.

Слишком много partitions тоже плохо: больше файлов и памяти на брокерах, дольше rebalance, больше метаданных, выше end-to-end задержка при `acks=all`.

### Практика

1. Создай топик `accounts` с 6 partitions.
2. Отправь по 5 событий для ключей `acc-1`, `acc-2`, `acc-3` и проверь, что события одного счёта в одном partition.
3. Увеличь число partitions до 12, отправь ещё события и найди ключ, который «переехал».

---

# Модуль 4. Репликация Kafka: leader, ISR, acks, min.insync.replicas

## 4.1 Replication factor

Каждый partition имеет N копий (**replication factor**). Одна копия **leader**, остальные **followers**.

```text
topic orders, partition-0, replication.factor=3

broker-1: partition-0 (LEADER)   <-- producer пишет, consumer читает
broker-2: partition-0 (follower) <-- копирует у лидера
broker-3: partition-0 (follower) <-- копирует у лидера
```

- Producer всегда пишет **в лидера**.
- Consumer по умолчанию читает **из лидера** (с KIP-392 можно читать с ближайшей реплики через `client.rack` и `replica.selector.class`).
- Followers постоянно отправляют лидеру fetch-запросы, как обычные consumer.

## 4.2 ISR (In-Sync Replicas)

**ISR** это набор реплик, которые успевают за лидером. Реплика выпадает из ISR, если не догоняла лидера дольше `replica.lag.time.max.ms` (по умолчанию 30 секунд).

```text
Replicas: 1,2,3   все реплики, назначенные partition
ISR:      1,3     реплики, синхронные с лидером прямо сейчас
```

Реплика вне ISR не может стать лидером при обычном выборе. Иначе мы потеряем подтверждённые данные.

## 4.3 acks: подтверждение записи

| `acks` | Когда producer получает ОК | Риск потери | Задержка |
|---|---|---|---|
| `0` | Сразу после отправки в сокет | Высокий: брокер мог не получить запись | Минимальная |
| `1` | Лидер записал в свой лог | Средний: лидер умер до репликации | Низкая |
| `all` (`-1`) | Записали все реплики из ISR | Минимальный при правильном `min.insync.replicas` | Выше |

С Kafka 3.0 значения по умолчанию у producer: `acks=all` и `enable.idempotence=true`.

## 4.4 Почему acks=all недостаточно без min.insync.replicas

`acks=all` означает «все реплики **из текущего ISR**». Если ISR сжался до одного лидера, `acks=all` превращается в `acks=1`.

```text
replication.factor=3, min.insync.replicas=1 (по умолчанию)

ISR = {1}            <- два follower отстали
producer acks=all    <- запись подтверждена только лидером
broker-1 умирает     <- подтверждённые данные ПОТЕРЯНЫ
```

**`min.insync.replicas`** задаёт минимальный размер ISR, при котором брокер принимает записи с `acks=all`. Если ISR меньше, producer получает ошибку `NotEnoughReplicasException`, и данные не теряются молча.

**Золотой стандарт надёжности:**

```properties
# topic / broker
replication.factor=3
min.insync.replicas=2
unclean.leader.election.enable=false

# producer
acks=all
enable.idempotence=true
```

Эта конфигурация переживает падение одного брокера без потери данных и без остановки записи.

| RF | min.insync.replicas | Переживёт падение без потери записи | Запись продолжится при падении |
|---|---|---|---|
| 3 | 1 | Нет гарантии | 2 брокеров |
| 3 | 2 | 1 брокера | 1 брокера |
| 3 | 3 | 2 брокеров | 0 брокеров (запись встанет) |
| 5 | 3 | 2 брокеров | 2 брокеров |

## 4.5 High watermark и leader epoch

- Лидер продвигает **high watermark**, когда запись реплицирована на все реплики ISR.
- Consumer видят только записи до HW. Поэтому они не прочитают запись, которая может исчезнуть при смене лидера.
- **Leader epoch** это номер «эпохи» лидерства. При смене лидера followers по эпохе понимают, какой хвост лога нужно обрезать, чтобы не было расхождений между репликами.

## 4.6 Unclean leader election

Если все реплики ISR умерли и осталась только отстающая реплика, есть выбор:

| `unclean.leader.election.enable` | Поведение | Цена |
|---|---|---|
| `false` (по умолчанию) | Partition недоступен, пока не вернётся реплика из ISR | Потеря доступности |
| `true` | Лидером станет отстающая реплика | Потеря подтверждённых данных |

Для платежей и заказов выбирают консистентность (`false`). Для метрик и логов иногда допустимо `true`.

## 4.7 Практика: создаём production-like топик

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create --topic orders \
  --partitions 6 --replication-factor 3 \
  --config min.insync.replicas=2
```

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --describe --topic orders
```

```text
Topic: orders  PartitionCount: 6  ReplicationFactor: 3  Configs: min.insync.replicas=2
  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
  Partition: 1  Leader: 2  Replicas: 2,3,1  Isr: 2,3,1
  Partition: 2  Leader: 3  Replicas: 3,1,2  Isr: 3,1,2
  ...
```

Лидеры распределены по брокерам равномерно. Первая реплика в списке `Replicas` это **preferred leader**.

## 4.8 Практика: убиваем брокер

```bash
docker stop kafka-1

docker exec kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:9092 --describe --topic orders
```

```text
Было:  Partition: 0  Leader: 1  Replicas: 1,2,3  Isr: 1,2,3
Стало: Partition: 0  Leader: 2  Replicas: 1,2,3  Isr: 2,3
```

Контроллер выбрал нового лидера из ISR. Producer и consumer обновили метаданные и продолжили работу.

Покажем только проблемные partitions:

```bash
docker exec kafka-2 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:9092 --describe --under-replicated-partitions
```

Теперь **останови второй брокер** и попробуй писать:

```bash
docker stop kafka-2
docker exec -it kafka-3 /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-3:9092 --topic orders \
  --producer-property acks=all
```

Получишь `NotEnoughReplicas`: ISR = 1 < `min.insync.replicas` = 2. Kafka **отказывается** принимать запись, которую не может надёжно сохранить. Это правильное поведение.

Возвращаем брокеры:

```bash
docker start kafka-1 kafka-2
```

После догоняющей репликации брокеры вернутся в ISR. Kafka периодически возвращает лидерство preferred-репликам (`auto.leader.rebalance.enable=true`). Сделать это вручную:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-leader-election.sh \
  --bootstrap-server kafka-1:9092 --election-type preferred --all-topic-partitions
```

## 4.9 Rack awareness

Если брокеры стоят в разных стойках или зонах доступности, укажи `broker.rack`. Kafka разместит реплики одного partition в **разных зонах**, и падение целой зоны не уничтожит все копии.

```properties
broker.rack=eu-central-1a
```

## 4.10 Антипаттерн: replication.factor = 1

```text
replication.factor=1
```

Один брокер умер, диск сломался, и partition недоступен или потерян навсегда. Для любых важных данных в продакшене используй `replication.factor=3`.

### Вопросы для самопроверки

1. Чем replica отличается от ISR?
2. Почему `acks=all` без `min.insync.replicas=2` не гарантирует сохранность?
3. Что произойдёт с записью при RF=3, min.insync.replicas=2 и двух упавших брокерах?
4. Что такое unclean leader election и когда его включают?
5. Зачем нужен `broker.rack`?

---

# Модуль 5. Kafka Producer: как устроен и как настроить

## 5.1 Внутреннее устройство producer

```text
send(record)
   |
   v
[Serializer] key/value -> bytes
   |
   v
[Partitioner] выбирает partition
   |
   v
[RecordAccumulator]  буферы батчей по partition (buffer.memory)
   |   batch orders-0: [r1][r2][r3]
   |   batch orders-1: [r4]
   v
[Sender thread] забирает готовые батчи (batch.size или linger.ms)
   |   группирует по брокеру-лидеру, сжимает
   v
Network -> Broker leader -> ответ -> callback / Future
```

Ключевой момент: `send()` **асинхронный**. Он кладёт запись в буфер и сразу возвращает `Future`. Реальная отправка происходит в фоновом потоке Sender.

## 5.2 Батчинг: linger.ms и batch.size

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `batch.size` | 16384 (16 KB) | Максимальный размер батча на один partition |
| `linger.ms` | 5 (с Kafka 4.0) | Сколько ждать наполнения батча перед отправкой |
| `buffer.memory` | 33554432 (32 MB) | Общий буфер producer |
| `max.block.ms` | 60000 | Сколько `send()` блокируется, если буфер полон или нет метаданных |
| `compression.type` | `none` | `gzip`, `snappy`, `lz4`, `zstd` |

Батч уходит, когда **заполнен `batch.size`** или **истёк `linger.ms`**, смотря что наступит раньше.

```text
linger.ms=0   много маленьких запросов, низкая задержка, низкий throughput
linger.ms=20  крупные батчи, лучшее сжатие, throughput в разы выше
```

## 5.3 Сжатие

| Алгоритм | Степень сжатия | CPU | Когда использовать |
|---|---|---|---|
| `lz4` | Средняя | Низкий | Хороший выбор по умолчанию для высокой нагрузки |
| `zstd` | Высокая | Средний | Экономия трафика и диска, JSON-логи |
| `snappy` | Средняя | Низкий | Совместимость со старыми системами |
| `gzip` | Высокая | Высокий | Редко, когда важен каждый байт |

Сжатие работает на уровне батча: чем больше батч, тем лучше сжатие. Держи на топике `compression.type=producer`, чтобы брокер не пережимал данные.

## 5.4 Retries и таймауты

```text
|<------------------- delivery.timeout.ms (120 s) ------------------->|
| linger | request.timeout.ms | retry.backoff | request.timeout.ms | ...
```

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `retries` | `2147483647` | Практически бесконечно, ограничивает `delivery.timeout.ms` |
| `delivery.timeout.ms` | 120000 | Общее время на доставку записи, включая все повторы |
| `request.timeout.ms` | 30000 | Ожидание ответа на один запрос |
| `retry.backoff.ms` | 100 | Пауза между повторами (растёт экспоненциально до `retry.backoff.max.ms`) |

Ретраи безопасны только с идемпотентностью. Иначе при потере ответа брокера запись будет продублирована.

## 5.5 Идемпотентный producer

Проблема без идемпотентности:

```text
producer --> broker: batch #1
broker пишет batch #1 в лог
broker --> producer: ACK   (ответ потерялся в сети)
producer: таймаут, retry batch #1
broker пишет batch #1 ЕЩЁ РАЗ   <-- дубликат
```

С `enable.idempotence=true`:

- producer получает **Producer ID (PID)**;
- каждый батч получает **sequence number** в рамках partition;
- брокер отбрасывает батч с уже виденным sequence number.

Требования: `acks=all`, `max.in.flight.requests.per.connection <= 5`. Порядок внутри partition сохраняется даже при ретраях.

> Идемпотентность защищает от дублей **из-за ретраев одного экземпляра producer**. Если приложение упало и заново отправило то же бизнес-событие, это новый PID и дубликат. Для этого нужен `event_id` и идемпотентный consumer.

## 5.6 Producer на Java

```xml
<dependency>
  <groupId>org.apache.kafka</groupId>
  <artifactId>kafka-clients</artifactId>
  <version>4.3.1</version>
</dependency>
```

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class OrderProducer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // надёжность
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000);

        // производительность
        props.put(ProducerConfig.LINGER_MS_CONFIG, 20);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 64 * 1024);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");

        props.put(ProducerConfig.CLIENT_ID_CONFIG, "order-service");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {
            for (int i = 1; i <= 10; i++) {
                String orderId = "order-" + i;
                String value = "{\"order_id\":\"" + orderId + "\",\"amount\":" + (i * 100) + "}";
                ProducerRecord<String, String> record = new ProducerRecord<>("orders", orderId, value);
                record.headers().add("event_type", "OrderCreated".getBytes());

                producer.send(record, (metadata, exception) -> {
                    if (exception != null) {
                        // сюда попадаем после исчерпания delivery.timeout.ms
                        System.err.println("Не удалось отправить " + orderId + ": " + exception);
                    } else {
                        System.out.printf("%s -> partition=%d offset=%d%n",
                                orderId, metadata.partition(), metadata.offset());
                    }
                });
            }
            producer.flush();
        }
    }
}
```

**Правила:**

- создавай **один `KafkaProducer` на приложение**: он потокобезопасен и дорог в создании;
- **всегда обрабатывай ошибку в callback**, иначе потеря данных будет незаметной;
- вызывай `flush()` и `close()` при остановке приложения, иначе буфер пропадёт;
- не делай `send().get()` на каждую запись в горячем пути: это убивает батчинг.

## 5.7 Producer на Go (franz-go)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func main() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers("localhost:19092", "localhost:29092", "localhost:39092"),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.ProducerLinger(20*time.Millisecond),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	)
	if err != nil {
		panic(err)
	}
	defer cl.Close()

	ctx := context.Background()
	for i := 1; i <= 10; i++ {
		rec := &kgo.Record{
			Topic: "orders",
			Key:   []byte(fmt.Sprintf("order-%d", i)),
			Value: []byte(fmt.Sprintf(`{"order_id":"order-%d"}`, i)),
		}
		cl.Produce(ctx, rec, func(r *kgo.Record, err error) {
			if err != nil {
				fmt.Println("error:", err)
				return
			}
			fmt.Printf("partition=%d offset=%d\n", r.Partition, r.Offset)
		})
	}
	if err := cl.Flush(ctx); err != nil {
		panic(err)
	}
}
```

Популярные клиенты Kafka для Go: `franz-go` (чистый Go, полная поддержка протокола), `confluent-kafka-go` (обёртка над librdkafka), `segmentio/kafka-go`.

## 5.8 Producer на Python (confluent-kafka)

```bash
pip install confluent-kafka
```

```python
import json
from confluent_kafka import Producer

producer = Producer({
    "bootstrap.servers": "localhost:19092,localhost:29092,localhost:39092",
    "acks": "all",
    "enable.idempotence": True,
    "linger.ms": 20,
    "compression.type": "lz4",
})

def on_delivery(err, msg):
    if err:
        print(f"Ошибка доставки: {err}")
    else:
        print(f"{msg.key().decode()} -> partition={msg.partition()} offset={msg.offset()}")

for i in range(1, 11):
    order = {"order_id": f"order-{i}", "amount": i * 100}
    producer.produce("orders", key=order["order_id"], value=json.dumps(order), callback=on_delivery)
    producer.poll(0)  # обработать callbacks

producer.flush()
```

## 5.9 Профили настроек producer

| Цель | Настройки |
|---|---|
| **Максимальная надёжность** (платежи) | `acks=all`, `enable.idempotence=true`, топик RF=3 и `min.insync.replicas=2`, либо транзакции |
| **Максимальный throughput** (логи, клики) | `linger.ms=50-100`, `batch.size=256KB-1MB`, `compression.type=zstd` или `lz4`, `buffer.memory` больше |
| **Минимальная задержка** | `linger.ms=0`, небольшие батчи, `compression.type=none` или `lz4` |

### Практика

1. Запусти Java producer и посмотри, как ключи распределились по partitions.
2. Замерь пропускную способность с `linger.ms=0` и `linger.ms=50`:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic orders --num-records 1000000 --record-size 512 --throughput -1 \
  --producer-props bootstrap.servers=kafka-1:9092 acks=all linger.ms=0 compression.type=none

docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic orders --num-records 1000000 --record-size 512 --throughput -1 \
  --producer-props bootstrap.servers=kafka-1:9092 acks=all linger.ms=50 batch.size=262144 compression.type=lz4
```

3. Сравни `records/sec`, `avg latency` и `99th` перцентиль.

---

# Модуль 6. Kafka Consumer и Consumer Groups

## 6.1 Poll loop: как читает consumer

Consumer в Kafka работает по модели **pull**: он сам запрашивает данные у брокера.

```text
while (running) {
    records = consumer.poll(timeout)     // fetch батчами, heartbeat, rebalance
    for record in records:
        process(record)
    consumer.commit()                     // сохранить прогресс
}
```

Что происходит внутри `poll()`:

- отправка fetch-запросов лидерам назначенных partitions;
- участие в протоколе группы (heartbeat, rebalance);
- возврат накопленных записей (до `max.poll.records`);
- автоматический commit, если включён `enable.auto.commit`.

## 6.2 Consumer Group

**Consumer group** это набор consumer с одним `group.id`, которые **делят partitions между собой**.

```text
topic orders: 6 partitions

group "billing" (3 consumers)          group "analytics" (1 consumer)
  consumer-1: p0, p1                     consumer-1: p0..p5
  consumer-2: p2, p3
  consumer-3: p4, p5
```

Правила:

- **один partition читается ровно одним consumer внутри группы**;
- разные группы читают топик **независимо** и хранят свои offsets;
- масштабирование чтения = добавление consumer в группу.

## 6.3 Consumer больше, чем partitions

```text
6 partitions, 8 consumers

consumer-1..6: по одному partition
consumer-7:    простаивает
consumer-8:    простаивает
```

**20 consumer не ускорят топик с 10 partitions.** Лишние consumer работают как горячий резерв. Хочешь больше параллелизма, увеличивай partitions или обрабатывай записи параллельно внутри consumer (с сохранением порядка по ключу).

## 6.4 Offsets и commit

Прогресс группы хранится во внутреннем топике `__consumer_offsets` (compacted). Commit означает «группа обработала всё до offset N, следующей читать N».

| Режим | Как работает | Риск |
|---|---|---|
| `enable.auto.commit=true` (по умолчанию) | Коммит каждые `auto.commit.interval.ms` (5 s) внутри `poll()` | Дубли при падении; потеря, если обработка асинхронная |
| `commitSync()` | Блокирующий коммит после обработки батча | Ниже throughput, зато предсказуемо |
| `commitAsync()` | Неблокирующий коммит | При ошибке нет повтора; в конце нужен `commitSync()` |
| Коммит в своей БД | Offset сохраняется в одной транзакции с результатом | Сложнее, но даёт exactly-once эффект |

**`auto.offset.reset`** определяет, откуда читать, если у группы нет сохранённого offset:

- `latest` (по умолчанию): только новые сообщения;
- `earliest`: с самого начала;
- `none`: выбросить ошибку.

## 6.5 Семантики доставки

```text
AT-MOST-ONCE (не больше одного раза)
  poll -> commit -> process
  упали во время process -> сообщение ПОТЕРЯНО

AT-LEAST-ONCE (хотя бы один раз)
  poll -> process -> commit
  упали перед commit -> сообщение обработается ПОВТОРНО

EXACTLY-ONCE (ровно один раз)
  транзакции Kafka (read-process-write внутри Kafka)
  или at-least-once + идемпотентная обработка
```

**Правило продакшена:** используй at-least-once и делай обработку **идемпотентной**. Дубликаты в распределённых системах неизбежны: ретраи, rebalance, падения.

## 6.6 Consumer на Java с ручным коммитом

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.time.Duration;
import java.util.*;

public class BillingConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "billing");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 500);
        // новый протокол групп (KIP-848), см. раздел 6.9
        props.put(ConsumerConfig.GROUP_PROTOCOL_CONFIG, "consumer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // корректное завершение по Ctrl+C
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try { mainThread.join(); } catch (InterruptedException ignored) {}
        }));

        try {
            consumer.subscribe(List.of("orders"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                for (ConsumerRecord<String, String> r : records) {
                    process(r); // должна быть идемпотентной
                }
                if (!records.isEmpty()) {
                    consumer.commitSync();
                }
            }
        } catch (WakeupException e) {
            // штатная остановка
        } finally {
            consumer.close(); // покинуть группу быстро, без ожидания session timeout
        }
    }

    static void process(ConsumerRecord<String, String> r) {
        System.out.printf("key=%s partition=%d offset=%d value=%s%n",
                r.key(), r.partition(), r.offset(), r.value());
    }
}
```

**Важно:** `KafkaConsumer` **не потокобезопасен**. Один consumer = один поток. Единственный безопасный вызов из другого потока это `wakeup()`.

## 6.7 Consumer на Python и Go

**Python (confluent-kafka):**

```python
from confluent_kafka import Consumer, KafkaException

consumer = Consumer({
    "bootstrap.servers": "localhost:19092",
    "group.id": "billing",
    "enable.auto.commit": False,
    "auto.offset.reset": "earliest",
})
consumer.subscribe(["orders"])

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            raise KafkaException(msg.error())
        print(msg.key(), msg.partition(), msg.offset(), msg.value())
        consumer.commit(message=msg, asynchronous=False)
finally:
    consumer.close()
```

**Go (franz-go):**

```go
cl, _ := kgo.NewClient(
	kgo.SeedBrokers("localhost:19092"),
	kgo.ConsumerGroup("billing"),
	kgo.ConsumeTopics("orders"),
	kgo.DisableAutoCommit(),
	kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
)
defer cl.Close()

for {
	fetches := cl.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		log.Println(errs)
	}
	fetches.EachRecord(func(r *kgo.Record) {
		fmt.Println(string(r.Key), r.Partition, r.Offset)
	})
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		log.Println("commit:", err)
	}
}
```

## 6.8 Rebalance

**Rebalance** это перераспределение partitions между consumer группы. Причины:

- новый consumer присоединился к группе;
- consumer корректно вышел или упал;
- consumer перестал слать heartbeat дольше `session.timeout.ms`;
- consumer не вызывал `poll()` дольше `max.poll.interval.ms`;
- изменилось число partitions или подписка.

### Классический протокол: eager и cooperative

```text
EAGER (stop-the-world):
  все consumer отдают ВСЕ partitions -> пауза -> новое распределение

COOPERATIVE (incremental, CooperativeStickyAssignor):
  отдают только те partitions, которые переезжают -> остальные продолжают работать
```

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `session.timeout.ms` | 45000 | Нет heartbeat дольше, consumer считается мёртвым |
| `heartbeat.interval.ms` | 3000 | Как часто слать heartbeat |
| `max.poll.interval.ms` | 300000 | Максимум между вызовами `poll()` |
| `max.poll.records` | 500 | Максимум записей за один `poll()` |
| `partition.assignment.strategy` | `RangeAssignor, CooperativeStickyAssignor` | Стратегия назначения |

### Самая частая авария: долгая обработка

```text
max.poll.records = 500
обработка одной записи = 1 s (вызов медленного API)
500 s > max.poll.interval.ms (300 s)
-> consumer исключён из группы
-> rebalance
-> батч обрабатывается заново другим consumer
-> снова не успевает -> бесконечный цикл rebalance
```

Решения: уменьшить `max.poll.records`, ускорить обработку, увеличить `max.poll.interval.ms`, вынести тяжёлую работу в пул потоков с паузой partitions (`consumer.pause()`).

### Static membership

С `group.instance.id` consumer получает постоянную идентичность. При рестарте пода в Kubernetes rebalance не запускается, если consumer вернулся в пределах `session.timeout.ms`. Это спасает от шторма rebalance при rolling deploy.

## 6.9 Новый протокол consumer group (KIP-848)

В Kafka 4.0 стал общедоступным **новый протокол групп** потребителей.

```properties
group.protocol=consumer
```

Что изменилось:

- **назначение partitions рассчитывает брокер** (group coordinator), а не лидер группы на клиенте;
- rebalance **полностью инкрементальный**, без глобальной синхронизации всех участников;
- медленный consumer больше не тормозит rebalance всей группы;
- `session.timeout.ms` и `heartbeat.interval.ms` настраиваются на брокере (`group.consumer.session.timeout.ms`, `group.consumer.heartbeat.interval.ms`);
- стратегия выбирается через `group.remote.assignor` (`uniform` или `range`) вместо `partition.assignment.strategy`.

Для новых приложений на Kafka 4.x рекомендуется использовать `group.protocol=consumer`. Классический протокол (`group.protocol=classic`) остаётся для совместимости.

## 6.10 Consumer lag

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --describe --group billing
```

```text
GROUP    TOPIC   PARTITION  CURRENT-OFFSET  LOG-END-OFFSET  LAG   CONSUMER-ID
billing  orders  0          1200            1250            50    consumer-1-...
billing  orders  1          980             4980            4000  consumer-2-...
```

- **LAG** = сколько сообщений группа ещё не обработала.
- Высокий lag **не всегда проблема**: важно, растёт он или сокращается, и укладывается ли задержка в SLA.
- Lag только на одном partition обычно означает горячий ключ или «ядовитое» сообщение.

## 6.11 Сброс offsets

Перечитать топик с начала (группа должна быть остановлена):

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 --group billing --topic orders \
  --reset-offsets --to-earliest --execute
```

Другие варианты: `--to-latest`, `--to-offset 100`, `--shift-by -500`, `--to-datetime 2026-09-15T00:00:00.000`, `--by-duration PT2H`. Без `--execute` команда показывает план (dry run).

### Практика

1. Запусти 3 consumer в группе `billing` для топика с 6 partitions и посмотри распределение.
2. Запусти 7-й consumer и убедись, что он простаивает.
3. Добавь в обработку `Thread.sleep(1000)`, уменьши `max.poll.interval.ms` до 10000 и воспроизведи цикл rebalance.
4. Сбрось offsets группы на 1 час назад.

---

# Модуль 7. Exactly-once, транзакции Kafka и transactional outbox

## 7.1 Где появляются дубли и потери

| Сценарий | Результат | Защита |
|---|---|---|
| Producer не получил ACK и повторил отправку | Дубль в топике | `enable.idempotence=true` |
| Приложение упало после send, но до отметки «отправлено» | Дубль бизнес-события | `event_id` + идемпотентный consumer |
| Consumer обработал, но упал до commit | Повторная обработка | Идемпотентная обработка |
| Consumer закоммитил до обработки и упал | Потеря | Коммит после обработки |
| Сервис записал в БД, но упал до отправки в Kafka | Потеря события | Transactional outbox |
| Сервис отправил в Kafka, но транзакция БД откатилась | «Фантомное» событие | Transactional outbox |
| RF=1 или `acks=1` и упал брокер | Потеря | RF=3, `min.insync.replicas=2`, `acks=all` |

## 7.2 Транзакции Kafka

Транзакции позволяют **атомарно** записать сообщения в несколько partitions и закоммитить offsets consumer: либо всё, либо ничего. Это основа паттерна **consume-transform-produce** с exactly-once.

```text
      read                    process                   write + commit offsets
orders ----> [ приложение ] -----------> payments  (одна транзакция Kafka)
```

Как это устроено:

- producer получает `transactional.id` (стабильный идентификатор экземпляра);
- **Transaction Coordinator** на брокере хранит состояние в топике `__transaction_state`;
- в partitions пишутся данные, а в конце транзакции **control records** (commit или abort);
- consumer с `isolation.level=read_committed` видит только закоммиченные данные;
- **fencing**: если стартовал новый экземпляр с тем же `transactional.id`, старый «зомби» получает `ProducerFencedException`.

```java
Properties p = new Properties();
p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "payment-processor-1");
p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
KafkaProducer<String, String> producer = new KafkaProducer<>(p);
producer.initTransactions();

Properties c = new Properties();
c.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
c.put(ConsumerConfig.GROUP_ID_CONFIG, "payment-processor");
c.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
c.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
c.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
c.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c);
consumer.subscribe(List.of("orders"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
    if (records.isEmpty()) continue;

    producer.beginTransaction();
    try {
        Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();
        for (ConsumerRecord<String, String> r : records) {
            producer.send(new ProducerRecord<>("payments", r.key(), transform(r.value())));
            offsets.put(new TopicPartition(r.topic(), r.partition()),
                        new OffsetAndMetadata(r.offset() + 1));
        }
        producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());
        producer.commitTransaction();
    } catch (ProducerFencedException e) {
        producer.close();   // нас заменил другой экземпляр
        break;
    } catch (KafkaException e) {
        producer.abortTransaction();
        // откатить позицию consumer к последнему закоммиченному offset
        for (TopicPartition tp : records.partitions()) {
            OffsetAndMetadata committed = consumer.committed(Set.of(tp)).get(tp);
            consumer.seek(tp, committed == null ? 0 : committed.offset());
        }
    }
}
```

> **Граница exactly-once.** Транзакции Kafka дают exactly-once **только внутри Kafka**: чтение из топика, запись в топик, коммит offset. Если обработка отправляет email, списывает деньги во внешнем API или пишет в PostgreSQL, эти эффекты не входят в транзакцию Kafka. Для них нужна идемпотентность.

## 7.3 Идемпотентный consumer

Самый надёжный способ получить «эффективно ровно один раз» при записи во внешнюю БД:

```sql
CREATE TABLE processed_events (
    event_id   UUID PRIMARY KEY,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

```sql
BEGIN;
INSERT INTO processed_events (event_id) VALUES ($1)
  ON CONFLICT (event_id) DO NOTHING;
-- если вставлено 0 строк, событие уже обработано: COMMIT и выходим
UPDATE accounts SET balance = balance - $2 WHERE id = $3;
COMMIT;
-- затем commit offset в Kafka
```

Другие варианты: `UPSERT` по естественному ключу, условное обновление по версии (`WHERE version = $expected`), хранение offset partition в той же таблице, что и результат.

## 7.4 Transactional outbox: как не потерять событие

**Проблема двойной записи (dual write):**

```text
1. INSERT INTO orders ...     OK
2. producer.send(OrderCreated) -> сервис упал
   -> заказ есть, события нет, остальные системы не узнали о заказе
```

**Решение: outbox-таблица в той же БД.**

```text
+---------------- одна транзакция БД ----------------+
| INSERT INTO orders (...)                            |
| INSERT INTO outbox (id, aggregate_id, type, payload)|
+-----------------------------------------------------+
               |
               v
  Debezium (CDC) или outbox-relay читает outbox
               |
               v
         Kafka topic shop.orders.events.v1
```

```sql
CREATE TABLE outbox (
    id             UUID PRIMARY KEY,
    aggregatetype  TEXT NOT NULL,     -- "order"
    aggregateid    TEXT NOT NULL,     -- ключ Kafka
    type           TEXT NOT NULL,     -- "OrderCreated"
    payload        JSONB NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

Relay может публиковать событие повторно, поэтому потребители всё равно должны быть идемпотентны по `id`. Настройку Debezium Outbox Event Router смотри в [модуле 11](#модуль-11-kafka-connect-и-cdc-с-debezium).

## 7.5 Итог: какую гарантию выбрать

| Задача | Рекомендация |
|---|---|
| Логи, метрики, клики | At-least-once, дубли допустимы |
| Микросервисы, бизнес-события | At-least-once + идемпотентный consumer + outbox |
| Kafka -> обработка -> Kafka | Транзакции Kafka или Kafka Streams `exactly_once_v2` |
| Kafka -> БД | Идемпотентная запись или offset в той же транзакции БД |

### Вопросы для самопроверки

1. Почему идемпотентный producer не защищает от дублей при рестарте приложения?
2. Что такое fencing и зачем нужен `transactional.id`?
3. Почему exactly-once в Kafka не распространяется на внешний HTTP-вызов?
4. Какую проблему решает transactional outbox?

---

# Модуль 8. Хранение данных: сегменты, retention, log compaction

## 8.1 Сегменты и индексы

Partition на диске разбит на **сегменты**:

```text
orders-0/
  00000000000000000000.log        старый сегмент (закрыт)
  00000000000000000000.index
  00000000000000000000.timeindex
  00000000000001000000.log        старый сегмент (закрыт)
  00000000000001000000.index
  00000000000002000000.log        ACTIVE сегмент: сюда идёт запись
  00000000000002000000.index
```

- Имя файла это **base offset** первой записи сегмента.
- Новый сегмент создаётся по достижении `segment.bytes` (1 GB) или `segment.ms` (7 дней).
- **Удаление и компактизация работают только с закрытыми сегментами.**
- `.index` это разреженный индекс offset -> позиция в файле; поиск записи: бинарный поиск по индексу + короткое последовательное чтение.

Посмотреть содержимое сегмента:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-dump-log.sh \
  --files /tmp/kraft-combined-logs/orders-0/00000000000000000000.log \
  --print-data-log
```

## 8.2 Почему Kafka такая быстрая

| Приём | Как работает |
|---|---|
| **Последовательная запись** | Только append в конец файла, диски и SSD любят последовательный I/O |
| **Page cache ОС** | Kafka не кэширует данные в JVM heap, чтение идёт из кэша файловой системы |
| **Zero-copy** (`sendfile`) | Данные из page cache отправляются в сокет без копирования в user space |
| **Батчинг** | Записи группируются на producer, брокере и consumer |
| **Сжатие батчей** | Меньше сети и диска, брокер не распаковывает данные |
| **Партиционирование** | Нагрузка распределена по брокерам и дискам |
| **Бинарный протокол** | Компактный протокол поверх TCP с пайплайнингом |

> Zero-copy не работает при включённом TLS: данные нужно шифровать в user space. Поэтому TLS заметно увеличивает нагрузку на CPU брокеров.

## 8.3 Page cache и JVM heap

```text
Сервер 64 GB RAM
  JVM heap Kafka:  6 GB   (метаданные, буферы запросов)
  Page cache ОС:  ~55 GB  (горячие данные партиций)
```

Рекомендации:

- heap брокера 4-8 GB достаточно даже для больших нагрузок;
- оставляй остальную память операционной системе под page cache;
- `vm.swappiness=1`, swap на брокере убивает задержки;
- consumer, которые читают «хвост» топика, получают данные из памяти; consumer, читающие старую историю, идут на диск и могут вытеснять горячие данные.

## 8.4 Retention: сколько хранить

| Параметр | По умолчанию | Смысл |
|---|---|---|
| `retention.ms` (топик) / `log.retention.hours` (брокер) | 7 дней | Время хранения |
| `retention.bytes` | `-1` (без лимита) | Лимит размера **на partition** |
| `segment.bytes` | 1 GB | Размер сегмента |
| `segment.ms` | 7 дней | Максимальный возраст активного сегмента |

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-configs.sh \
  --bootstrap-server kafka-1:9092 \
  --alter --entity-type topics --entity-name orders \
  --add-config retention.ms=259200000
```

> Ловушка: при маленьком трафике активный сегмент может не закрываться неделями, и данные будут храниться дольше retention. Для таких топиков уменьшай `segment.ms`.

## 8.5 cleanup.policy: delete и compact

**`delete`** (по умолчанию) удаляет целые сегменты старше retention.

**`compact`** оставляет **последнее значение для каждого ключа**:

```text
До компактизации:
offset 0: user-1 -> {"name":"Ann"}
offset 1: user-2 -> {"name":"Bob"}
offset 2: user-1 -> {"name":"Anna"}
offset 3: user-2 -> null            <- tombstone (удаление ключа)
offset 4: user-3 -> {"name":"Kate"}

После компактизации:
offset 2: user-1 -> {"name":"Anna"}
offset 4: user-3 -> {"name":"Kate"}
(tombstone user-2 удалится после delete.retention.ms)
```

Где используют log compaction:

- `__consumer_offsets` и другие внутренние топики;
- снапшоты состояния: профили пользователей, цены, остатки, настройки;
- CDC-топики (последнее состояние строки таблицы);
- changelog-топики state store в Kafka Streams.

Настройки компактизации: `min.cleanable.dirty.ratio` (0.5), `min.compaction.lag.ms`, `max.compaction.lag.ms`, `delete.retention.ms` (1 день). Можно комбинировать: `cleanup.policy=compact,delete`.

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 --create --topic user-profile \
  --partitions 6 --replication-factor 3 \
  --config cleanup.policy=compact \
  --config min.cleanable.dirty.ratio=0.01 \
  --config segment.ms=60000
```

## 8.6 Tiered Storage

**Tiered Storage** (KIP-405, production-ready с Kafka 3.9) выносит закрытые сегменты в объектное хранилище (S3, GCS, Azure Blob, MinIO), а на локальных дисках брокера держит только «горячий» хвост.

```text
broker local disk:  последние 1-2 дня (быстро)
object storage:     месяцы и годы (дёшево)
```

Что даёт: дешёвое длительное хранение, меньшие диски, быстрый перезапуск и перебалансировка брокеров. Включается `remote.log.storage.system.enable=true` на брокере и `remote.storage.enable=true` на топике, плюс плагин RemoteStorageManager для конкретного хранилища.

### Вопросы для самопроверки

1. Почему Kafka эффективно работает с диском и не держит данные в JVM heap?
2. Почему retention может не срабатывать на топике с редкими записями?
3. Когда выбрать `compact` вместо `delete`?
4. Что такое tombstone?

---

# Модуль 9. Share Groups: очереди в Kafka

## 9.1 Какую проблему решают Share Groups

В обычной consumer group параллелизм ограничен числом partitions. Для очереди задач (отправка писем, генерация PDF, вызовы внешних API) это неудобно: хочется 100 воркеров на топик с 6 partitions.

**Share Groups** (KIP-932 «Queues for Kafka», общедоступны с Kafka 4.2) позволяют:

- нескольким consumer **читать один и тот же partition одновременно**;
- подтверждать **каждую запись отдельно**;
- автоматически **повторно доставлять** неподтверждённые записи;
- ограничивать число попыток доставки.

```text
Consumer group:  partition-0 -> ровно 1 consumer
Share group:     partition-0 -> consumer-1, consumer-2, ... consumer-N
```

## 9.2 Как это работает

- брокер выдаёт записи consumer с **временной блокировкой (acquisition lock)**, по умолчанию 30 секунд;
- consumer подтверждает запись: `ACCEPT` (обработано), `RELEASE` (вернуть в очередь), `REJECT` (отбросить как необрабатываемую);
- если блокировка истекла без подтверждения, запись доставляется другому consumer;
- после превышения лимита попыток (`group.share.delivery.count.limit`, по умолчанию 5) запись считается необрабатываемой;
- **порядок не гарантируется**, это осознанный компромисс ради параллелизма.

## 9.3 Пример на Java

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:19092");
props.put("group.id", "email-workers");
props.put("key.deserializer", StringDeserializer.class.getName());
props.put("value.deserializer", StringDeserializer.class.getName());
props.put("share.acknowledgement.mode", "explicit");

try (KafkaShareConsumer<String, String> consumer = new KafkaShareConsumer<>(props)) {
    consumer.subscribe(List.of("email-jobs"));
    while (true) {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
        for (ConsumerRecord<String, String> r : records) {
            try {
                sendEmail(r.value());
                consumer.acknowledge(r, AcknowledgeType.ACCEPT);
            } catch (TemporaryException e) {
                consumer.acknowledge(r, AcknowledgeType.RELEASE); // повторить позже
            } catch (Exception e) {
                consumer.acknowledge(r, AcknowledgeType.REJECT);  // не повторять
            }
        }
        consumer.commitSync();
    }
}
```

Консольный клиент для экспериментов:

```bash
docker exec -it kafka-1 /opt/kafka/bin/kafka-console-share-consumer.sh \
  --bootstrap-server kafka-1:9092 --topic email-jobs --group email-workers
```

Если в твоём кластере share groups выключены, их включают через feature flag:

```bash
docker exec kafka-1 /opt/kafka/bin/kafka-features.sh \
  --bootstrap-server kafka-1:9092 upgrade --feature share.version=1
```

## 9.4 Consumer group или share group

| Нужно | Выбор |
|---|---|
| Порядок событий по ключу | Consumer group |
| Stream processing, агрегаты, CDC | Consumer group |
| Очередь независимых задач, воркеры | Share group |
| Параллелизм больше числа partitions | Share group |
| Поштучный retry без блокировки partition | Share group |

---

# Модуль 10. Schema Registry, Avro, Protobuf и проектирование событий

## 10.1 Зачем схемы, если Kafka хранит байты

Kafka не проверяет содержимое сообщений. Без контракта рано или поздно случится:

```text
Order Service переименовал поле amount -> total_amount
   -> 5 потребителей падают в 3 часа ночи
```

**Schema Registry** хранит версии схем и проверяет совместимость при публикации новой версии. Реализации: Confluent Schema Registry, Apicurio Registry, Karapace.

```text
producer --(регистрирует схему, получает schema id)--> Schema Registry
producer --[magic byte][schema id][payload]--> Kafka
consumer --(по schema id получает схему)--> Schema Registry
```

## 10.2 Форматы сообщений

| Формат | Плюсы | Минусы | Когда выбирать |
|---|---|---|---|
| **JSON** | Читаемый, прост в отладке | Большой размер, нет строгой схемы | Прототипы, небольшие нагрузки |
| **JSON Schema** | JSON + валидация | Размер как у JSON | Когда нужен JSON и контракт |
| **Avro** | Компактный, отличная эволюция схем | Нужен реестр схем | Data-платформы, CDC, аналитика |
| **Protobuf** | Компактный, кодогенерация, gRPC-экосистема | Правила эволюции нужно соблюдать | Микросервисы, полиглотные команды |

## 10.3 Режимы совместимости схем

| Режим | Разрешено | Кого обновлять первым |
|---|---|---|
| `BACKWARD` (часто по умолчанию) | Новая схема читает старые данные: удалить поле, добавить поле с default | Consumer |
| `FORWARD` | Старая схема читает новые данные: добавить поле, удалить поле с default | Producer |
| `FULL` | И то и другое | В любом порядке |
| `*_TRANSITIVE` | Проверка против **всех** прошлых версий | |
| `NONE` | Без проверки | Не используй в продакшене |

Пример Avro-схемы:

```json
{
  "type": "record",
  "name": "OrderCreated",
  "namespace": "shop.orders.v1",
  "fields": [
    {"name": "event_id", "type": "string"},
    {"name": "order_id", "type": "string"},
    {"name": "user_id", "type": "string"},
    {"name": "amount", "type": "long"},
    {"name": "currency", "type": "string", "default": "RUB"},
    {"name": "promo_code", "type": ["null", "string"], "default": null}
  ]
}
```

**Правила безопасной эволюции:**

- добавляй новые поля только с `default`;
- не переименовывай поля, добавляй новое и помечай старое устаревшим;
- не меняй тип поля;
- ломающее изменение = новый топик или новый тип события (`v2`).

## 10.4 Проектирование событий

**Хорошее событие:**

```json
{
  "event_id": "b3c9...",
  "event_type": "OrderPaid",
  "event_version": 2,
  "occurred_at": "2026-09-15T10:21:43Z",
  "source": "payment-service",
  "trace_id": "4bf92f3577b34da6",
  "data": {
    "order_id": "order-123",
    "payment_id": "pay-987",
    "amount": 4990,
    "currency": "RUB"
  }
}
```

| Вопрос | Рекомендация |
|---|---|
| Один топик на тип события или на сущность? | Для порядка жизненного цикла: один топик на агрегат (`orders`) с разными типами событий |
| Толстое или тонкое событие? | Event-carried state transfer (все нужные данные) снижает обратные HTTP-вызовы; тонкое событие проще, но порождает связанность |
| Деньги | Целые числа в минимальных единицах (копейки) + валюта, не float |
| Время | UTC, ISO-8601 или epoch millis |
| Метаданные | В headers: `trace_id`, `event_type`, `content-type` |
| Персональные данные | Минимизируй, для compacted-топиков удаляй через tombstone |

Стандарт **CloudEvents** описывает общий формат метаданных событий и может служить основой соглашений в компании.

---

# Модуль 11. Kafka Connect и CDC с Debezium

## 11.1 Что такое Kafka Connect

**Kafka Connect** это фреймворк для интеграции Kafka с внешними системами **без написания кода**.

```text
PostgreSQL --[Source connector]--> Kafka --[Sink connector]--> Elasticsearch
MySQL                                                         S3 / ClickHouse
MongoDB                                                       Snowflake / JDBC
```

| Понятие | Смысл |
|---|---|
| **Source connector** | Читает из внешней системы и пишет в Kafka |
| **Sink connector** | Читает из Kafka и пишет во внешнюю систему |
| **Worker** | JVM-процесс Connect; в distributed-режиме воркеры образуют кластер |
| **Task** | Единица параллелизма коннектора |
| **Converter** | Сериализация: `JsonConverter`, `AvroConverter`, `ProtobufConverter` |
| **SMT** (Single Message Transform) | Лёгкая трансформация записи: переименовать поле, извлечь ключ, маршрутизировать |

Состояние distributed Connect хранится в Kafka: топики `connect-configs`, `connect-offsets`, `connect-status`.

## 11.2 CDC: Change Data Capture

**CDC** превращает изменения в базе данных в поток событий. **Debezium** читает журнал транзакций (WAL в PostgreSQL, binlog в MySQL, oplog в MongoDB) и публикует каждое `INSERT`, `UPDATE`, `DELETE` в Kafka.

```text
UPDATE customers SET email='new@mail.ru' WHERE id=42;
        |
        v  (WAL / logical replication)
Debezium PostgreSQL connector
        |
        v
topic: crm.public.customers
{
  "before": {"id": 42, "email": "old@mail.ru"},
  "after":  {"id": 42, "email": "new@mail.ru"},
  "op": "u",
  "source": {"lsn": 123456789, "table": "customers"},
  "ts_ms": 1789460503000
}
```

Преимущества перед опросом таблицы (`SELECT ... WHERE updated_at > ?`): видны удаления, нет нагрузки от частых запросов, все промежуточные изменения, минимальная задержка.

## 11.3 Пример: Debezium PostgreSQL connector

Подготовка PostgreSQL: `wal_level=logical`, пользователь с правом `REPLICATION`.

Регистрация коннектора через REST API Kafka Connect:

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d '{
    "name": "crm-postgres-cdc",
    "config": {
      "connector.class": "io.debezium.connector.postgresql.PostgresConnector",
      "database.hostname": "postgres",
      "database.port": "5432",
      "database.user": "debezium",
      "database.password": "${file:/secrets/db.properties:password}",
      "database.dbname": "crm",
      "topic.prefix": "crm",
      "plugin.name": "pgoutput",
      "table.include.list": "public.customers,public.orders",
      "snapshot.mode": "initial",
      "key.converter": "org.apache.kafka.connect.json.JsonConverter",
      "value.converter": "org.apache.kafka.connect.json.JsonConverter"
    }
  }'
```

Полезные команды REST API:

```bash
curl http://localhost:8083/connectors                              # список
curl http://localhost:8083/connectors/crm-postgres-cdc/status      # статус задач
curl -X POST http://localhost:8083/connectors/crm-postgres-cdc/restart?includeTasks=true
curl -X PUT  http://localhost:8083/connectors/crm-postgres-cdc/pause
```

## 11.4 Outbox через Debezium Event Router

```json
{
  "transforms": "outbox",
  "transforms.outbox.type": "io.debezium.transforms.outbox.EventRouter",
  "transforms.outbox.table.field.event.key": "aggregateid",
  "transforms.outbox.route.by.field": "aggregatetype",
  "transforms.outbox.route.topic.replacement": "shop.${routedByValue}.events.v1",
  "table.include.list": "public.outbox"
}
```

Запись в `outbox` с `aggregatetype=order` окажется в топике `shop.order.events.v1` с ключом `aggregateid`.

## 11.5 Ошибки в Kafka Connect

```properties
errors.tolerance=all
errors.deadletterqueue.topic.name=dlq.crm-sink
errors.deadletterqueue.context.headers.enable=true
errors.log.enable=true
```

Dead letter queue в Connect работает для **sink**-коннекторов: сломанная запись уходит в DLQ, а коннектор продолжает работу.

**Типичные проблемы CDC:** растущий replication slot в PostgreSQL при остановленном коннекторе (WAL заполняет диск), изменение схемы таблицы, долгий initial snapshot больших таблиц, `REPLICA IDENTITY` для получения `before` при `UPDATE`/`DELETE`.

---

# Модуль 12. Kafka Streams: потоковая обработка данных

## 12.1 Что такое Kafka Streams

**Kafka Streams** это Java-библиотека для stream processing. Она **не требует отдельного кластера**: приложение это обычный JAR, который масштабируется запуском дополнительных экземпляров.

```text
orders topic --> [Kafka Streams app x3 instances] --> orders-per-minute topic
                        |
                   state store (RocksDB) + changelog topic в Kafka
```

Альтернативы: **Apache Flink** (мощный отдельный кластер, SQL, сложные окна), **ksqlDB** (SQL поверх Kafka Streams), **Spark Structured Streaming**.

## 12.2 KStream, KTable, GlobalKTable

| Абстракция | Смысл | Пример |
|---|---|---|
| **KStream** | Поток независимых событий (insert) | Клики, платежи |
| **KTable** | Changelog: последнее значение по ключу (upsert) | Текущий профиль пользователя |
| **GlobalKTable** | KTable, полностью реплицированная на каждый экземпляр | Небольшие справочники |

**Двойственность потока и таблицы:** поток изменений можно свернуть в таблицу, а таблицу развернуть обратно в поток изменений.

## 12.3 Пример: выручка по пользователям и заказы в минуту

```java
Properties props = new Properties();
props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-analytics");
props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092");
props.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> orders = builder.stream("orders");

// 1. Количество заказов в минуту (tumbling window)
orders
    .groupBy((key, value) -> "all")
    .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofMinutes(1)))
    .count(Materialized.as("orders-per-minute-store"))
    .toStream()
    .map((windowedKey, count) -> KeyValue.pair(windowedKey.window().startTime().toString(), count.toString()))
    .to("orders-per-minute");

// 2. Сумма заказов по пользователю (KTable)
orders
    .selectKey((key, value) -> extractUserId(value))
    .mapValues(value -> extractAmount(value))
    .groupByKey(Grouped.with(Serdes.String(), Serdes.Long()))
    .reduce(Long::sum, Materialized.as("revenue-by-user-store"))
    .toStream()
    .to("revenue-by-user", Produced.with(Serdes.String(), Serdes.Long()));

KafkaStreams streams = new KafkaStreams(builder.build(), props);
Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
streams.start();
```

## 12.4 Ключевые концепции Kafka Streams

| Концепция | Что важно знать |
|---|---|
| **Stateless-операции** | `filter`, `map`, `flatMap`, `branch`: не требуют хранилища |
| **Stateful-операции** | `count`, `aggregate`, `reduce`, `join`: состояние в RocksDB + changelog-топик |
| **Repartition** | `selectKey`/`groupBy` по новому ключу создают внутренний repartition-топик |
| **Окна** | Tumbling, hopping, sliding, session |
| **Grace period** | Сколько ждать опоздавших событий после закрытия окна |
| **Event time** | Обработка по времени события, а не времени прихода |
| **Joins** | Stream-stream (в окне), stream-table (обогащение), table-table |
| **Co-partitioning** | Для join топики должны иметь одинаковое число partitions и одинаковые ключи |
| **Interactive queries** | Чтение state store напрямую из приложения через REST |
| **Standby replicas** | `num.standby.replicas` ускоряет восстановление состояния после падения |

---

# Модуль 13. Обработка ошибок: retry, DLQ и poison pill

## 13.1 Типы ошибок

| Тип | Пример | Что делать |
|---|---|---|
| **Временная** (transient) | Таймаут БД, 503 от внешнего API | Повторить с backoff |
| **Постоянная** (permanent) | Невалидный JSON, нарушение бизнес-правила | Отправить в DLQ, не повторять |
| **Poison pill** | Сообщение, которое всегда роняет десериализатор | DLQ, иначе partition заблокирован навсегда |

## 13.2 Почему нельзя бесконечно повторять в consumer

```text
partition-3: [msg 100 - сломан] [101] [102] [103] ...
consumer бесконечно ретраит 100
-> все события partition-3 стоят
-> lag растёт, заказы клиентов не обрабатываются
```

## 13.3 Паттерн retry-топиков и DLQ

```text
orders ---> main consumer --ошибка--> orders.retry.30s ---> retry consumer (ждёт 30 s)
                                           |
                                        ошибка
                                           v
                                    orders.retry.5m ---> retry consumer (ждёт 5 min)
                                           |
                                        ошибка
                                           v
                                      orders.dlq  ---> алерт, ручной разбор, повторная отправка
```

Правила:

- в headers храни `original-topic`, `original-partition`, `original-offset`, `attempt`, `error-class`, `error-message`, `failed-at`;
- retry-consumer не спит в `poll`-цикле, а ставит partition на паузу (`pause`/`resume`) до наступления времени повтора;
- **retry-топики нарушают порядок**; если порядок по ключу критичен, блокируй ключ до успешной обработки или ретраи делай на месте с ограничением;
- на DLQ обязательно заведи **алерт** и инструмент **повторной отправки** (redrive);
- в Spring for Apache Kafka есть готовая реализация: `@RetryableTopic` и `DefaultErrorHandler` с `DeadLetterPublishingRecoverer`.

## 13.4 Ошибки десериализации

В Java-клиенте исключение десериализации выбрасывается из `poll()` и блокирует partition. Решения:

- обёртка `ErrorHandlingDeserializer` (Spring Kafka);
- читать `byte[]` и десериализовать в коде с `try/catch`;
- в Kafka Streams: `deserialization.exception.handler=LogAndContinueExceptionHandler` или собственный обработчик с отправкой в DLQ.

---

# Модуль 14. Производительность и тюнинг Kafka

## 14.1 Throughput против latency

```text
                больше батчи, больше linger.ms, сжатие
throughput  <----------------------------------------->  latency
                маленькие батчи, linger.ms=0, acks=1
```

Сначала определи цель: миллионы событий в секунду для аналитики или p99 < 10 ms для торговой системы. Одновременно получить максимум обоих нельзя.

## 14.2 Тюнинг producer

| Параметр | Для throughput | Для latency |
|---|---|---|
| `linger.ms` | 20-100 | 0-5 |
| `batch.size` | 128 KB - 1 MB | 16-32 KB |
| `compression.type` | `lz4`, `zstd` | `none`, `lz4` |
| `buffer.memory` | 64-256 MB | По умолчанию |
| `acks` | `all` (надёжность обычно важнее) | `all` или `1` |

## 14.3 Тюнинг consumer

| Параметр | По умолчанию | Для throughput |
|---|---|---|
| `fetch.min.bytes` | 1 | 64 KB - 1 MB: брокер копит данные перед ответом |
| `fetch.max.wait.ms` | 500 | Верхняя граница ожидания при `fetch.min.bytes` |
| `max.partition.fetch.bytes` | 1 MB | Больше для крупных сообщений |
| `fetch.max.bytes` | 50 MB | Лимит ответа fetch |
| `max.poll.records` | 500 | Больше при быстрой обработке |

Чаще всего узкое место не Kafka, а **обработка в consumer**: синхронные вызовы БД по одной записи. Используй пакетную запись (batch insert), параллельную обработку по ключам, асинхронные клиенты.

## 14.4 Тюнинг брокера

```properties
num.network.threads=6          # потоки сетевых запросов (по умолчанию 3)
num.io.threads=16              # потоки обработки запросов/диска (по умолчанию 8)
num.replica.fetchers=4         # параллельная репликация (по умолчанию 1)
socket.send.buffer.bytes=1048576
socket.receive.buffer.bytes=1048576
message.max.bytes=1048588      # максимальный размер батча (~1 MB)
auto.create.topics.enable=false
```

Не трогай `log.flush.interval.*`: Kafka полагается на репликацию, а не на fsync каждой записи.

## 14.5 Операционная система и железо

| Область | Рекомендация |
|---|---|
| **Диски** | NVMe/SSD или несколько HDD в JBOD; отдельные диски под данные Kafka |
| **Файловая система** | XFS (или ext4), монтирование с `noatime` |
| **Память** | JVM heap 6 GB, остальное page cache |
| **Swap** | `vm.swappiness=1` |
| **Лимиты** | `nofile` ≥ 100000 (много сегментов и сокетов), `vm.max_map_count` ≥ 262144 |
| **Сеть** | 10-25 Gbit, репликация и consumer часто упираются в сеть раньше диска |
| **JVM** | Java 17 или 21, G1GC (по умолчанию), следи за паузами GC |
| **CPU** | Важен для TLS, сжатия и большого числа соединений |

## 14.6 Нагрузочное тестирование

```bash
# producer: 5 млн записей по 1 KB
docker exec kafka-1 /opt/kafka/bin/kafka-producer-perf-test.sh \
  --topic perf --num-records 5000000 --record-size 1024 --throughput -1 \
  --producer-props bootstrap.servers=kafka-1:9092 acks=all linger.ms=20 batch.size=262144 compression.type=lz4

# consumer
docker exec kafka-1 /opt/kafka/bin/kafka-consumer-perf-test.sh \
  --bootstrap-server kafka-1:9092 --topic perf --messages 5000000 --group perf-test
```

Тестируй на реальном размере и формате сообщений, с тем же `acks`, TLS и числом partitions, что в продакшене.

## 14.7 Большие сообщения

Kafka оптимизирована под сообщения до ~1 MB. Для файлов, изображений и больших документов используй **claim check pattern**: положи объект в S3/MinIO, а в Kafka отправь ссылку и метаданные.

---

# Модуль 15. Мониторинг Kafka: метрики, consumer lag, алерты

## 15.1 Стек мониторинга

```text
Kafka brokers (JMX) --> JMX Exporter --> Prometheus --> Grafana
                                            |
Consumer lag -------> kafka-exporter/Burrow +--> Alertmanager --> Telegram/Slack/PagerDuty
Клиентские метрики --> Micrometer ---------->
```

## 15.2 Главные метрики брокера

| Метрика (JMX) | Норма | Что означает отклонение |
|---|---|---|
| `kafka.server:type=ReplicaManager,name=UnderReplicatedPartitions` | 0 | Реплики отстают: брокер упал, перегружен диск или сеть |
| `kafka.server:type=ReplicaManager,name=UnderMinIsrPartitionCount` | 0 | Запись с `acks=all` отклоняется |
| `kafka.controller:type=KafkaController,name=OfflinePartitionsCount` | 0 | Partitions без лидера: данные недоступны |
| `kafka.controller:type=KafkaController,name=ActiveControllerCount` | 1 на кластер | 0 = нет контроллера, >1 = split brain |
| `kafka.server:type=ReplicaManager,name=IsrShrinksPerSec` / `IsrExpandsPerSec` | Около 0 | Реплики «мигают»: сеть, GC, перегрузка |
| `kafka.server:type=KafkaRequestHandlerPool,name=RequestHandlerAvgIdlePercent` | > 30% | Брокер не успевает обрабатывать запросы |
| `kafka.network:type=SocketServer,name=NetworkProcessorAvgIdlePercent` | > 30% | Перегружены сетевые потоки |
| `kafka.network:type=RequestMetrics,name=TotalTimeMs,request=Produce` | Стабильная | Рост p99 задержки записи |
| `kafka.server:type=BrokerTopicMetrics,name=BytesInPerSec` / `BytesOutPerSec` | По профилю | Трафик и планирование ёмкости |
| Диск, CPU, сеть, GC pause | Запас > 30% | Необходимо масштабирование |

## 15.3 Метрики клиентов

| Клиент | Метрика | Зачем |
|---|---|---|
| Producer | `record-error-rate`, `record-retry-rate` | Ошибки и ретраи отправки |
| Producer | `request-latency-avg`, `batch-size-avg`, `compression-rate-avg` | Эффективность батчинга |
| Producer | `buffer-available-bytes` | Буфер переполняется, `send()` блокируется |
| Consumer | `records-lag-max` | Отставание |
| Consumer | `commit-rate`, `rebalance-rate-per-hour` | Частые rebalance = проблема |
| Consumer | `last-poll-seconds-ago` | Застрявший poll-цикл |

## 15.4 Алерты, которые должны быть у каждого

```yaml
groups:
- name: kafka
  rules:
  - alert: KafkaOfflinePartitions
    expr: sum(kafka_controller_kafkacontroller_offlinepartitionscount) > 0
    for: 1m
    labels: {severity: critical}

  - alert: KafkaUnderMinIsr
    expr: sum(kafka_server_replicamanager_underminisrpartitioncount) > 0
    for: 2m
    labels: {severity: critical}

  - alert: KafkaUnderReplicatedPartitions
    expr: sum(kafka_server_replicamanager_underreplicatedpartitions) > 0
    for: 10m
    labels: {severity: warning}

  - alert: KafkaConsumerLagGrowing
    expr: sum by (consumergroup, topic) (kafka_consumergroup_lag) > 100000
          and deriv(sum by (consumergroup, topic) (kafka_consumergroup_lag)[15m:1m]) > 0
    for: 15m
    labels: {severity: warning}
```

> Имена метрик зависят от правил JMX Exporter и выбранного exporter лага. Сверь их со своей конфигурацией.

**Лучший алерт по lag** выражается во времени: «группа отстаёт больше чем на 5 минут», а не «lag больше 100000 сообщений». 100000 сообщений для кликов это секунды, а для платежей это катастрофа.

## 15.5 Runbook: under-replicated partitions

1. Все URP на одном брокере? Проверь, жив ли брокер, диск, GC-логи.
2. URP на всех брокерах? Проверь сеть и общий трафик (`BytesInPerSec`).
3. Растёт `IsrShrinksPerSec`? Ищи паузы GC и насыщение сети.
4. Диск заполнен? Сократи retention проблемных топиков, добавь диски, перенеси partitions.

---

# Модуль 16. Безопасность Kafka: TLS, SASL, ACL, квоты

## 16.1 Три уровня защиты

| Уровень | Механизм |
|---|---|
| **Шифрование** | TLS между клиентами и брокерами, между брокерами, до контроллеров |
| **Аутентификация** | mTLS, SASL/SCRAM-SHA-512, SASL/OAUTHBEARER (OIDC), SASL/GSSAPI (Kerberos) |
| **Авторизация** | ACL через `StandardAuthorizer` (KRaft) |
| **Квоты** | Лимиты байт/с и запросов на клиента или пользователя |

По умолчанию Kafka работает **без шифрования и аутентификации**. Никогда не открывай такой кластер в интернет.

## 16.2 Конфигурация брокера с SASL_SSL

```properties
listeners=SASL_SSL://:9094,CONTROLLER://:9093
advertised.listeners=SASL_SSL://kafka-1.prod.internal:9094
listener.security.protocol.map=SASL_SSL:SASL_SSL,CONTROLLER:SSL
inter.broker.listener.name=SASL_SSL
sasl.enabled.mechanisms=SCRAM-SHA-512
sasl.mechanism.inter.broker.protocol=SCRAM-SHA-512

ssl.keystore.location=/etc/kafka/ssl/kafka-1.keystore.p12
ssl.keystore.type=PKCS12
ssl.truststore.location=/etc/kafka/ssl/truststore.p12
ssl.truststore.type=PKCS12

authorizer.class.name=org.apache.kafka.metadata.authorizer.StandardAuthorizer
allow.everyone.if.no.acl.found=false
super.users=User:admin
```

## 16.3 Пользователи SCRAM

```bash
kafka-configs.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --alter --add-config 'SCRAM-SHA-512=[iterations=8192,password=CHANGE_ME]' \
  --entity-type users --entity-name order-service
```

Конфигурация клиента:

```properties
bootstrap.servers=kafka-1.prod.internal:9094
security.protocol=SASL_SSL
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="order-service" password="${env:KAFKA_PASSWORD}";
ssl.truststore.location=/etc/app/truststore.p12
ssl.truststore.type=PKCS12
```

## 16.4 ACL: принцип минимальных привилегий

```bash
# producer: писать в orders
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --add --allow-principal User:order-service \
  --operation Write --operation Describe --topic shop.orders.events.v1

# идемпотентный producer
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --add --allow-principal User:order-service --operation IdempotentWrite --cluster

# consumer: читать топик и использовать свою группу
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --add --allow-principal User:billing-service \
  --operation Read --operation Describe --topic shop.orders.events.v1 \
  --group billing --resource-pattern-type literal

# список ACL
kafka-acls.sh --bootstrap-server kafka-1:9094 --command-config admin.properties --list
```

Для транзакционного producer добавь `Write` и `Describe` на `--transactional-id`. Префиксные ACL (`--resource-pattern-type prefixed --topic shop.orders.`) упрощают управление по доменам.

## 16.5 Квоты

```bash
kafka-configs.sh --bootstrap-server kafka-1:9094 --command-config admin.properties \
  --alter --add-config 'producer_byte_rate=10485760,consumer_byte_rate=20971520,request_percentage=200' \
  --entity-type users --entity-name analytics-service
```

Квоты защищают кластер от «шумного соседа»: одна команда с неудачным батч-джобом не должна положить Kafka для всей компании.

---

# Модуль 17. Kafka в продакшене: архитектура и эксплуатация

## 17.1 Референсная архитектура кластера

```text
                 Availability Zone A    Zone B          Zone C
Controllers:     controller-1           controller-2    controller-3
Brokers:         broker-1, broker-4     broker-2, 5     broker-3, 6
                 (broker.rack=a)        (rack=b)        (rack=c)

Топики: RF=3, min.insync.replicas=2, реплики в разных зонах
Клиенты: acks=all, идемпотентность, client.rack для чтения из своей зоны
```

## 17.2 Расчёт ёмкости (capacity planning)

```text
Входящий трафик:          50 MB/s
Retention:                 7 дней
Replication factor:        3
Запас:                     40%

Хранение = 50 MB/s × 86400 s × 7 × 3 ≈ 90.7 TB
С запасом 40%             ≈ 127 TB
Сжатие (lz4, ~x3 для JSON) уменьшает объём, учитывай фактический коэффициент
```

Проверь также: исходящий сетевой трафик (репликация × 2 + все consumer groups), число partitions на брокер (ориентир до 4000 реплик на брокер для типового железа), время восстановления брокера после замены диска.

## 17.3 Чеклист проектирования топика

- [ ] Понятное имя по стандарту компании
- [ ] Ключ выбран под бизнес-инвариант порядка
- [ ] Число partitions рассчитано с запасом
- [ ] `replication.factor=3`, `min.insync.replicas=2`
- [ ] Retention согласован с потребителями и требованиями к данным
- [ ] `cleanup.policy` выбран осознанно
- [ ] Схема зарегистрирована, задан режим совместимости
- [ ] Определены владелец топика и список потребителей
- [ ] Есть DLQ и алерты для критичных потребителей
- [ ] Выданы ACL по принципу минимальных привилегий
- [ ] Персональные данные минимизированы

## 17.4 Операции с кластером

**Rolling restart и обновление версии:** по одному брокеру, дождаться `UnderReplicatedPartitions = 0` перед следующим, контроллеры обновлять отдельно. После обновления бинарников поднимается `metadata.version`:

```bash
kafka-features.sh --bootstrap-server kafka-1:9092 describe
kafka-features.sh --bootstrap-server kafka-1:9092 upgrade --release-version 4.3
```

**Перенос partitions** на новые брокеры:

```bash
kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 \
  --topics-to-move-json-file topics.json --broker-list "4,5,6" --generate

kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 \
  --reassignment-json-file plan.json --execute --throttle 50000000

kafka-reassign-partitions.sh --bootstrap-server kafka-1:9092 \
  --reassignment-json-file plan.json --verify
```

Всегда используй `--throttle`, иначе перенос данных съест сеть и продакшен-трафик пострадает. Для автоматической балансировки есть **Cruise Control**.

## 17.5 Kafka в Kubernetes

- **Strimzi**: самый популярный open-source оператор Kafka для Kubernetes. Ресурсы `Kafka`, `KafkaNodePool`, `KafkaTopic`, `KafkaUser`, `KafkaConnect`, поддержка KRaft.
- Используй `StatefulSet`-подобное хранилище с локальными или быстрыми сетевыми дисками, anti-affinity по зонам, PodDisruptionBudget.
- Внешний доступ: отдельный адрес для каждого брокера (LoadBalancer, NodePort или Ingress с TLS passthrough), иначе снова проблема `advertised.listeners`.

## 17.6 Multi-DC и disaster recovery

| Схема | Инструмент | Особенности |
|---|---|---|
| **Active-passive** | MirrorMaker 2 | Резервный кластер в другом ДЦ, переключение клиентов при аварии |
| **Active-active** | MirrorMaker 2 с префиксами топиков | Каждый ДЦ пишет локально, данные зеркалируются (`dc1.orders`) |
| **Stretched cluster** | Один кластер на 3 ДЦ | Нужна низкая задержка между ДЦ (< 10-20 ms), RPO = 0 |

**MirrorMaker 2** построен на Kafka Connect, реплицирует топики, конфигурации, ACL и транслирует offsets consumer groups (`emit.checkpoints.enabled`, `sync.group.offsets.enabled`). Помни: offsets в разных кластерах не совпадают, переключение потребителей требует трансляции offsets.

## 17.7 Антипаттерны Kafka в продакшене

| Антипаттерн | Чем плохо | Как правильно |
|---|---|---|
| `replication.factor=1` | Потеря данных при сбое диска | RF=3 |
| `auto.create.topics.enable=true` | Опечатка создаёт топик с дефолтами | Топики через IaC (Terraform, Strimzi, GitOps) |
| Kafka как база данных для запросов | Нет индексов и выборок по полям | Kafka как лог + материализация в БД |
| Тысячи топиков на каждого клиента | Взрыв метаданных и partitions | Один топик + ключ/заголовки |
| Сообщения по 10-50 MB | Давление на память, репликацию | Claim check через S3 |
| Новый producer на каждый запрос | Утечка соединений, нет батчинга | Один producer на приложение |
| Игнорирование ошибок `send()` | Молчаливая потеря данных | Обработка callback, метрика ошибок |
| Нет мониторинга lag | Узнаёшь о проблеме от пользователей | Алерты по lag во времени |
| Синхронная длинная обработка в poll-цикле | Шторм rebalance | Меньше `max.poll.records`, пауза partitions |
| Порядок через один partition на весь топик | Нет масштабирования | Порядок по ключу |

---

# Модуль 18. Итоговый проект: event-driven интернет-магазин

## 18.1 Архитектура

```text
               HTTP
Client -----> Order Service ---(outbox + Debezium)---> shop.orders.events.v1
                                                          |
         +------------------------------+-----------------+-----------------+
         v                              v                                   v
  Payment Service                Inventory Service                 Analytics (Kafka Streams)
  (идемпотентный,                (резерв товара)                   заказы в минуту,
   транзакции Kafka)                    |                           выручка по категориям
         |                              v                                   |
         v                     shop.inventory.events.v1                     v
 payments.transactions.events.v1                                   analytics.orders.stats.v1
         |                                                                  |
         v                                                                  v
 Notification Service (share group, email-воркеры)                ClickHouse (sink connector)
         |
         v
 notifications.email.dlq
```

## 18.2 Требования

1. **Order Service** сохраняет заказ в PostgreSQL и пишет событие в outbox в одной транзакции. Debezium публикует `OrderCreated`.
2. **Топики**: RF=3, `min.insync.replicas=2`, ключ `order_id`, Avro или Protobuf схемы в Schema Registry с режимом `BACKWARD`.
3. **Payment Service** читает `OrderCreated`, списывает оплату идемпотентно по `event_id`, публикует `PaymentSucceeded` или `PaymentFailed`.
4. **Inventory Service** резервирует товар; при `PaymentFailed` снимает резерв (сага через хореографию).
5. **Notification Service** работает через share group, временные ошибки SMTP ретраит, постоянные отправляет в DLQ.
6. **Analytics** на Kafka Streams считает заказы в минуту и выручку с `exactly_once_v2`.
7. **Мониторинг**: Prometheus + Grafana, алерты по URP, offline partitions и lag.
8. **Безопасность**: SASL/SCRAM, отдельный пользователь и ACL на каждый сервис.

## 18.3 Хаос-тестирование

- [ ] Останови лидера partition во время нагрузки: нет потерь, нет ошибок у клиентов дольше нескольких секунд.
- [ ] Убей Payment Service посреди батча: после рестарта нет двойных списаний.
- [ ] Отправь сломанное сообщение: оно в DLQ, остальные обрабатываются.
- [ ] Останови Debezium на 10 минут: после запуска все события доставлены.
- [ ] Выполни rolling restart всех брокеров под нагрузкой.
- [ ] Добавь несовместимое изменение схемы: Schema Registry его отклоняет.

---

# Шпаргалка Kafka CLI

Команды ниже выполняются внутри контейнера (`docker exec -it kafka-1 bash`, затем `cd /opt/kafka/bin`) или с локально установленным дистрибутивом Kafka.

```bash
BS=kafka-1:9092   # bootstrap server

# ---------- Топики ----------
kafka-topics.sh --bootstrap-server $BS --list
kafka-topics.sh --bootstrap-server $BS --create --topic t --partitions 6 --replication-factor 3
kafka-topics.sh --bootstrap-server $BS --describe --topic t
kafka-topics.sh --bootstrap-server $BS --alter --topic t --partitions 12
kafka-topics.sh --bootstrap-server $BS --delete --topic t
kafka-topics.sh --bootstrap-server $BS --describe --under-replicated-partitions
kafka-topics.sh --bootstrap-server $BS --describe --unavailable-partitions

# ---------- Конфигурации ----------
kafka-configs.sh --bootstrap-server $BS --describe --entity-type topics --entity-name t
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name t --add-config retention.ms=86400000
kafka-configs.sh --bootstrap-server $BS --alter --entity-type topics --entity-name t --delete-config retention.ms
kafka-configs.sh --bootstrap-server $BS --describe --entity-type brokers --entity-name 1 --all

# ---------- Producer / Consumer ----------
kafka-console-producer.sh --bootstrap-server $BS --topic t --property parse.key=true --property key.separator=:
kafka-console-consumer.sh --bootstrap-server $BS --topic t --from-beginning --property print.key=true --property print.offset=true
kafka-console-consumer.sh --bootstrap-server $BS --topic t --partition 0 --offset 100 --max-messages 10

# ---------- Consumer groups ----------
kafka-consumer-groups.sh --bootstrap-server $BS --list
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g
kafka-consumer-groups.sh --bootstrap-server $BS --describe --group g --members --verbose
kafka-consumer-groups.sh --bootstrap-server $BS --group g --topic t --reset-offsets --to-earliest --execute
kafka-consumer-groups.sh --bootstrap-server $BS --delete --group g

# ---------- Offsets ----------
kafka-get-offsets.sh --bootstrap-server $BS --topic t              # последние offsets
kafka-get-offsets.sh --bootstrap-server $BS --topic t --time -2    # самые ранние

# ---------- Кластер / KRaft ----------
kafka-metadata-quorum.sh --bootstrap-server $BS describe --status
kafka-metadata-quorum.sh --bootstrap-server $BS describe --replication
kafka-features.sh --bootstrap-server $BS describe
kafka-broker-api-versions.sh --bootstrap-server $BS
kafka-log-dirs.sh --bootstrap-server $BS --describe --topic-list t
kafka-leader-election.sh --bootstrap-server $BS --election-type preferred --all-topic-partitions

# ---------- Отладка ----------
kafka-dump-log.sh --files /path/00000000000000000000.log --print-data-log
kafka-producer-perf-test.sh --topic t --num-records 1000000 --record-size 1024 --throughput -1 --producer-props bootstrap.servers=$BS
kafka-consumer-perf-test.sh --bootstrap-server $BS --topic t --messages 1000000
```

---

# Шпаргалка важных настроек

## Producer

| Параметр | По умолчанию | Рекомендация |
|---|---|---|
| `acks` | `all` | `all` |
| `enable.idempotence` | `true` | `true` |
| `linger.ms` | `5` | 5-50 |
| `batch.size` | `16384` | 32-256 KB |
| `compression.type` | `none` | `lz4` или `zstd` |
| `delivery.timeout.ms` | `120000` | Под SLA |
| `max.in.flight.requests.per.connection` | `5` | ≤ 5 с идемпотентностью |

## Consumer

| Параметр | По умолчанию | Рекомендация |
|---|---|---|
| `group.protocol` | `classic` | `consumer` для новых приложений на 4.x |
| `enable.auto.commit` | `true` | `false` + ручной commit после обработки |
| `auto.offset.reset` | `latest` | Осознанный выбор под сценарий |
| `max.poll.records` | `500` | Под время обработки |
| `max.poll.interval.ms` | `300000` | Больше максимального времени обработки батча |
| `isolation.level` | `read_uncommitted` | `read_committed` при транзакциях |
| `group.instance.id` | нет | Задать для static membership в Kubernetes |

## Topic и broker

| Параметр | По умолчанию | Рекомендация |
|---|---|---|
| `replication.factor` / `default.replication.factor` | `1` | `3` |
| `min.insync.replicas` | `1` | `2` |
| `unclean.leader.election.enable` | `false` | `false` для важных данных |
| `retention.ms` | 7 дней | Под требования |
| `cleanup.policy` | `delete` | `compact` для состояния |
| `auto.create.topics.enable` | `true` | `false` |
| `compression.type` (топик) | `producer` | `producer` |

---

# Вопросы на собеседовании по Kafka с ответами

## Junior

<details>
<summary><b>1. Что такое Apache Kafka?</b></summary>

Распределённая платформа потоковой передачи событий. Хранит события в реплицируемом append-only логе, разделённом на partitions, и позволяет многим независимым потребителям читать их с любой позиции.
</details>

<details>
<summary><b>2. Чем Kafka отличается от RabbitMQ?</b></summary>

Kafka хранит сообщения после чтения и отдаёт их по offset, масштабируется через partitions, подходит для event streaming и replay. RabbitMQ это брокер очередей со сложной маршрутизацией, где сообщение обычно удаляется после подтверждения.
</details>

<details>
<summary><b>3. Что такое topic, partition и offset?</b></summary>

Topic это именованный поток событий. Partition это упорядоченный журнал внутри топика и единица параллелизма. Offset это порядковый номер записи внутри partition.
</details>

<details>
<summary><b>4. Гарантирует ли Kafka порядок сообщений?</b></summary>

Только внутри одного partition. Чтобы события одной сущности шли по порядку, их отправляют с одинаковым ключом.
</details>

<details>
<summary><b>5. Что такое consumer group?</b></summary>

Набор consumer с одним `group.id`, которые делят partitions топика: каждый partition читается одним consumer группы. Разные группы читают независимо.
</details>

<details>
<summary><b>6. Что будет, если consumer больше, чем partitions?</b></summary>

Лишние consumer простаивают и ждут, пока освободится partition. Ускорения не будет.
</details>

<details>
<summary><b>7. Нужен ли ZooKeeper для Kafka?</b></summary>

Нет. С Kafka 4.0 ZooKeeper удалён, метаданные управляются встроенным протоколом KRaft.
</details>

## Middle

<details>
<summary><b>8. Как producer выбирает partition?</b></summary>

Явно указанный partition; иначе `murmur2(key) % numPartitions`; при отсутствии ключа sticky partitioner, заполняющий батч для одного partition перед переключением.
</details>

<details>
<summary><b>9. Объясни acks=0, 1, all.</b></summary>

`0`: без подтверждения. `1`: подтверждает лидер. `all`: подтверждают все реплики из ISR. Надёжность `all` зависит от `min.insync.replicas`.
</details>

<details>
<summary><b>10. Что такое ISR?</b></summary>

In-Sync Replicas: реплики, которые успевают за лидером в пределах `replica.lag.time.max.ms`. Только они могут стать лидером при обычных выборах.
</details>

<details>
<summary><b>11. Почему acks=all без min.insync.replicas недостаточно?</b></summary>

ISR может сжаться до одного лидера, и `acks=all` фактически станет `acks=1`. При падении лидера подтверждённые данные пропадут. `min.insync.replicas=2` запрещает запись в такой ситуации.
</details>

<details>
<summary><b>12. Что такое идемпотентный producer?</b></summary>

Producer с PID и sequence number на партицию: брокер отбрасывает повторно отправленные батчи. Защищает от дублей при ретраях внутри одного экземпляра producer.
</details>

<details>
<summary><b>13. Чем at-least-once отличается от at-most-once?</b></summary>

At-most-once: коммит до обработки, возможна потеря. At-least-once: коммит после обработки, возможны дубли. В продакшене обычно at-least-once плюс идемпотентная обработка.
</details>

<details>
<summary><b>14. Что вызывает rebalance и как его уменьшить?</b></summary>

Подключение и уход consumer, пропуск heartbeat, превышение `max.poll.interval.ms`, изменение подписки. Уменьшить: static membership, cooperative rebalancing или новый протокол KIP-848, быстрая обработка, корректный `close()`.
</details>

<details>
<summary><b>15. Что такое consumer lag и когда он проблема?</b></summary>

Разница между high watermark и committed offset. Проблема, когда lag стабильно растёт или задержка обработки выходит за SLA.
</details>

<details>
<summary><b>16. Чем log compaction отличается от retention delete?</b></summary>

Delete удаляет старые сегменты по времени или размеру. Compaction хранит последнее значение каждого ключа; tombstone (value = null) удаляет ключ.
</details>

<details>
<summary><b>17. Почему увеличение partitions ломает порядок по ключу?</b></summary>

Меняется `hash(key) % N`, и новые события ключа попадают в другой partition, чем старые.
</details>

<details>
<summary><b>18. Как обработать сообщение, которое всегда падает?</b></summary>

Не ретраить бесконечно: ограниченное число попыток, retry-топики с задержкой, затем DLQ с метаданными ошибки и алертом.
</details>

## Senior

<details>
<summary><b>19. Как работают транзакции Kafka?</b></summary>

`transactional.id` + Transaction Coordinator + `__transaction_state`. Producer пишет данные в несколько partitions и offsets через `sendOffsetsToTransaction`, коммит записывает control markers. Consumer с `read_committed` читает до Last Stable Offset. Fencing по эпохе producer отсекает зомби.
</details>

<details>
<summary><b>20. Где заканчивается exactly-once в Kafka?</b></summary>

На границе Kafka. Внешние побочные эффекты (БД, HTTP, email) требуют идемпотентности, outbox или хранения offset в той же транзакции, что и результат.
</details>

<details>
<summary><b>21. Что такое high watermark и leader epoch?</b></summary>

HW это граница записей, реплицированных на ISR и видимых consumer. Leader epoch позволяет followers после смены лидера корректно обрезать расходящийся хвост лога.
</details>

<details>
<summary><b>22. Почему Kafka быстрая?</b></summary>

Последовательный I/O, page cache, zero-copy, батчинг и сжатие на уровне батча, партиционирование, эффективный бинарный протокол.
</details>

<details>
<summary><b>23. Как выбрать число partitions?</b></summary>

`max(T/Tp, T/Tc)` с запасом на рост, с учётом ограничений на порядок, времени rebalance, числа реплик на брокер и end-to-end задержки.
</details>

<details>
<summary><b>24. Как надёжно опубликовать событие после записи в БД?</b></summary>

Transactional outbox: событие пишется в таблицу outbox в той же транзакции, публикуется CDC (Debezium) или relay. Потребители идемпотентны.
</details>

<details>
<summary><b>25. Как построить DR для Kafka?</b></summary>

Active-passive или active-active на MirrorMaker 2 с трансляцией offsets, либо stretched cluster на 3 ДЦ с низкой задержкой. Нужно определить RPO/RTO, регулярно тренировать переключение.
</details>

<details>
<summary><b>26. Чем Share Groups отличаются от consumer groups?</b></summary>

Share group позволяет нескольким consumer читать один partition параллельно, с поштучным подтверждением, повторной доставкой и лимитом попыток, но без гарантии порядка.
</details>

<details>
<summary><b>27. Что важнее при выборе ключа: равномерность или порядок?</b></summary>

Зависит от бизнес-инварианта. Если нарушение порядка ломает корректность (баланс счёта), порядок важнее равномерности. Горячие ключи решаются отдельно.
</details>

<details>
<summary><b>28. Какие изменения дал новый протокол групп KIP-848?</b></summary>

Назначение partitions считается на брокере, rebalance инкрементальный без глобальной синхронизации, медленный участник не блокирует группу, таймауты и assignor настраиваются на стороне сервера.
</details>

<details>
<summary><b>29. Как бы ты расследовал рост p99 задержки записи?</b></summary>

Метрики `TotalTimeMs` по фазам (RequestQueue, Local, Remote), `RequestHandlerAvgIdlePercent`, ISR shrinks, GC-паузы, диск и сеть, изменения трафика и размера батчей у клиентов, «шумные» клиенты и квоты.
</details>

<details>
<summary><b>30. Когда Kafka не стоит использовать?</b></summary>

Маленькая нагрузка без replay, нужен синхронный ответ, сложная маршрутизация и приоритеты сообщений, нет ресурсов на эксплуатацию.
</details>

---

# FAQ: частые вопросы про Apache Kafka

**Как выучить Kafka с нуля?**

Пройди модули 0-6 этого курса по порядку, подними кластер из трёх брокеров в Docker, напиши producer и consumer на своём языке, затем сломай кластер и наблюдай за поведением. После этого переходи к транзакциям, Kafka Connect и Kafka Streams.

**Сколько времени нужно, чтобы освоить Kafka?**

Базовое понимание и первые рабочие сервисы: 1-2 недели. Уверенный production-уровень с репликацией, гарантиями доставки и мониторингом: 1-3 месяца практики.

**Какой язык программирования выбрать для Kafka?**

Эталонный клиент написан на Java, Kafka Streams и Kafka Connect работают на JVM. Для Go есть franz-go и confluent-kafka-go, для Python confluent-kafka, для .NET Confluent.Kafka, для Node.js KafkaJS и confluent-kafka-javascript.

**Kafka это брокер сообщений или база данных?**

Это распределённый лог событий. Её используют как брокер сообщений и как долговременное хранилище событий, но она не заменяет базу данных с индексами и произвольными запросами.

**Нужен ли ZooKeeper в 2026 году?**

Нет. Kafka 4.x работает только в режиме KRaft.

**Сколько сообщений в секунду выдерживает Kafka?**

Кластер на типовом железе обрабатывает сотни тысяч и миллионы сообщений в секунду. Реальный предел зависит от размера сообщений, `acks`, сжатия, дисков, сети и числа partitions.

**Может ли Kafka потерять сообщения?**

Может при неправильной настройке: RF=1, `acks=1`, `min.insync.replicas=1`, включённый unclean leader election, игнорирование ошибок `send()`, коммит offset до обработки. С настройками из модуля 4 и 5 потеря подтверждённых данных при отказе одного брокера исключена.

**Как в Kafka сделать отложенные сообщения или задержку?**

Встроенных отложенных сообщений нет. Используют retry-топики с паузой partitions, внешний планировщик или хранение задач в БД с публикацией по времени.

**Kafka или RabbitMQ: что выбрать?**

Kafka для event streaming, аналитики, CDC, высокой нагрузки и replay. RabbitMQ для очередей задач, RPC и сложной маршрутизации. С появлением Share Groups Kafka закрывает часть сценариев очередей.

**Что лучше: своя Kafka или managed-сервис?**

Managed-сервис экономит время на эксплуатации, обновлениях и мониторинге. Своя Kafka даёт контроль над стоимостью и конфигурацией, но требует экспертизы команды.

---

# Глоссарий Kafka

| Термин | Определение |
|---|---|
| **Acks** | Уровень подтверждения записи producer |
| **Broker** | Сервер Kafka, хранящий partitions |
| **Bootstrap servers** | Начальные адреса брокеров для получения метаданных |
| **CDC** | Change Data Capture, захват изменений из БД |
| **Changelog topic** | Топик, в котором Kafka Streams сохраняет state store |
| **Cleanup policy** | Политика очистки: `delete` или `compact` |
| **Consumer group** | Группа consumer, делящих partitions |
| **Consumer lag** | Отставание группы от конца лога |
| **Controller** | Узел, управляющий метаданными кластера |
| **DLQ** | Dead Letter Queue, топик для необрабатываемых сообщений |
| **Exactly-once (EOS)** | Семантика обработки ровно один раз |
| **Fencing** | Отсечение устаревшего экземпляра producer или consumer |
| **High watermark** | Последний offset, реплицированный на ISR |
| **Idempotent producer** | Producer, исключающий дубли при ретраях |
| **ISR** | In-Sync Replicas, синхронные реплики |
| **KRaft** | Протокол консенсуса метаданных Kafka на основе Raft |
| **Leader** | Реплика partition, принимающая запись |
| **Log compaction** | Хранение последнего значения по ключу |
| **LEO** | Log End Offset, следующий offset для записи |
| **Offset** | Номер записи в partition |
| **Outbox** | Паттерн надёжной публикации событий через таблицу БД |
| **Partition** | Упорядоченный журнал внутри топика |
| **Rebalance** | Перераспределение partitions в группе |
| **Replication factor** | Число копий partition |
| **Retention** | Срок или объём хранения данных |
| **Schema Registry** | Сервис хранения и проверки схем сообщений |
| **Segment** | Файл лога partition на диске |
| **Share group** | Группа с очередной семантикой и поштучным подтверждением |
| **Tombstone** | Запись с value = null, удаляющая ключ в compacted-топике |
| **Topic** | Именованный поток событий |
| **Transactional ID** | Идентификатор транзакционного producer |

---

# Официальные источники и что читать дальше

- [Документация Apache Kafka](https://kafka.apache.org/documentation/)
- [Релизы и анонсы Apache Kafka](https://kafka.apache.org/blog/)
- [Kafka Improvement Proposals (KIP)](https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Improvement+Proposals)
- [KIP-848: новый протокол consumer group](https://cwiki.apache.org/confluence/display/KAFKA/KIP-848%3A+The+Next+Generation+of+the+Consumer+Rebalance+Protocol)
- [KIP-932: Queues for Kafka](https://cwiki.apache.org/confluence/display/KAFKA/KIP-932%3A+Queues+for+Kafka)
- [Документация Debezium](https://debezium.io/documentation/)
- [Strimzi: Kafka в Kubernetes](https://strimzi.io/documentation/)
- Книги: «Kafka: The Definitive Guide» (2-е издание), «Designing Data-Intensive Applications» (Martin Kleppmann), «Kafka Streams in Action».

---

## Как помочь курсу

- ⭐ Поставь звезду репозиторию, чтобы курс видели больше разработчиков.
- Нашёл ошибку или неточность? Создай Issue или Pull Request.
- Поделись курсом с командой и коллегами, которые изучают Kafka.

**Ключевые темы курса:** Apache Kafka курс, Kafka с нуля, Kafka обучение бесплатно, Kafka на русском, Kafka tutorial, KRaft, Kafka Docker Compose, Kafka producer consumer, consumer group, rebalance, exactly-once, Kafka transactions, transactional outbox, Kafka Streams, Kafka Connect, Debezium CDC, Schema Registry, Avro, Protobuf, Kafka мониторинг, Kafka безопасность, Kafka в Kubernetes, Strimzi, Kafka vs RabbitMQ, вопросы на собеседовании по Kafka.
