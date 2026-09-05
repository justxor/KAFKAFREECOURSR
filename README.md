[Kafka_4x_Advanced_Course_Part1_README.md](https://github.com/user-attachments/files/31864156/Kafka_4x_Advanced_Course_Part1_README.md)
# Apache Kafka 4.x - продвинутый бесплатный практический курс 2026 года

> Часть 1. Архитектура, KRaft, topics, partitions, replication, producer, consumer и первый production-like кластер.

## Что ты построишь

К концу первой части у тебя будет локальный Kafka-кластер, похожий на реальную инфраструктуру:

```text
Producer
   |
   v
+-----------------------+
| Kafka Cluster         |
|                       |
| broker-1              |
| broker-2              |
| broker-3              |
|                       |
| KRaft metadata quorum |
+-----------------------+
   |
   v[Kafka_4x_Course_Introduction_README.md](https://github.com/user-attachments/files/31864750/Kafka_4x_Course_Introduction_README.md)

Consumer Group
```

В курсе разберём:

- как Kafka физически хранит сообщения;
- зачем нужны partitions;
- как работает replication;
- что происходит при падении broker;
- как producer выбирает partition;
- что такое ISR;
- как consumer group распределяет partitions;
- где появляются duplicates;
- почему `acks=all` недостаточно само по себе;
- чем offset отличается от message id;
- почему Kafka не является очередью в классическом понимании;
- как проектировать topic под нагрузку.

Полезные ресурсы 

Лучшие ресурсы, чтобы не отставать от трендов Go разработки.

👣 [Golang Go](https://t.me/+5eALlmi6pfo4ZjMy) - авторский канал, посвященный Go разработке, Devops и созданию высоконагруженных сервисов.

⚡ [Machine Learning](https://t.me/+gRRiZ6J044BmNGNi) - ИИ, разбор моделей машинного обучение, rag, современные LLM, объясняем на пальцах ка

🎁 [Продвинутый DEVOPS](https://t.me/addlist/MUtJEeJSxeY2YTFi) - собрали для вас лучшие ресурсы по DEVOPS на любой вкус в одной папке.

## 1. Kafka - не просто очередь с# Apache Kafka 4.x - продвинутый практический курс

> Введение. Что такое Kafka, зачем она нужна, где применяется, как устроена и как запустить её локально.

---

# 0. Что вообще такое Apache Kafka

Apache Kafka - это распределённая платформа для передачи, хранения и обработки потоков событий.

Проще всего начать с обычной ситуации.

Допустим, у нас интернет-магазин.

Пользователь оформляет заказ:

```text
OrderCreated
```

После этого сразу несколько систем должны что-то сделать:

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

Без Kafka Order Service может напрямую обращаться ко всем этим системам.

Например:

```text
Order Service
   |
   +--> POST /payment
   +--> POST /warehouse
   +--> POST /analytics
   +--> POST /notification
   +--> POST /fraud
```

На маленьком проекте это работает.

Но дальше начинаются проблемы.

Если Notification Service недоступен:

```text
что делать с заказом?
```

Если Analytics работает медленно:

```text
должно ли оформление заказа ждать?
```

Если завтра появляется ещё десять сервисов:

```text
нужно менять Order Service?
```

Если один сервис хочет перечитать события за вчера:

```text
где их взять?
```

Именно здесь появляется Kafka.

---

# 1. Kafka как промежуточный журнал событий

С Kafka архитектура становится такой:

```text
Order Service
     |
     | OrderCreated
     v
+--------------------+
|       Kafka        |
| topic: orders      |
+--------------------+
   |       |       |
   v       v       v
Payment Warehouse Analytics
```

Order Service больше не обязан знать обо всех consumers.

Он просто сообщает:

```text
"Заказ 123 создан"
```

и записывает событие в Kafka.

Остальные системы читают его независимо.

---

# 2. Kafka - это не просто очередь

Kafka часто называют message queue.

Это удобно для первого объяснения, но технически не совсем точно.

Классическая очередь часто выглядит так:

```text
Producer
   |
   v
Queue
   |
   v
Consumer
```

Consumer забрал сообщение:

```text
message removed
```

В Kafka сообщение после чтения обычно не исчезает.

Kafka хранит события определённое время.

Например:

```text
orders

offset 0 -> OrderCreated
offset 1 -> OrderPaid
offset 2 -> OrderPacked
offset 3 -> OrderShipped
```

Consumer просто запоминает:

```text
я дочитал до offset 3
```

Поэтому Kafka удобнее представлять как:

```text
распределённый append-only log
```

---

# 3. Почему это настолько полезно

Допустим Analytics Service был выключен 6 часов.

За это время в Kafka накопилось:

```text
100 000 событий
```

После запуска Analytics может продолжить чтение с последнего сохранённого offset.

```text
Kafka

1
2
3
4
5
...
100000
       ^
       |
Analytics продолжает отсюда
```

Producer при этом ничего не должен повторно отправлять вручную.

---

# 4. Реальный пример без Kafka

Представим backend приложения такси.

После завершения поездки нужно:

```text
1. списать деньги
2. начислить бонусы
3. отправить чек
4. обновить аналитику
5. обновить рейтинг водителя
6. сохранить данные для ML
```

Прямая архитектура:

```text
Trip Service
 |
 +--> Billing
 +--> Loyalty
 +--> Email
 +--> Analytics
 +--> Driver Rating
 +--> ML Pipeline
```

Теперь Billing отвечает 10 секунд.

Trip Service начинает ждать.

Analytics упал.

Нужно решить:

```text
считать поездку завершённой или нет?
```

Добавился новый ML сервис.

Нужно снова менять Trip Service.

Это сильная связанность:

```text
tight coupling
```

---

# 5. С Kafka

```text
Trip Service
     |
     | TripCompleted
     v
+------------------------+
| Kafka                  |
| topic: trips           |
+------------------------+
  |    |    |    |    |
  v    v    v    v    v
Bill Loyalty Mail BI   ML
```

Trip Service знает только Kafka.

Consumers могут:

```text
падать
обновляться
масштабироваться
читать повторно
обрабатывать с разной скоростью
```

независимо друг от друга.

---

# 6. Что такое event

Event - факт, который уже произошёл.

Например:

```json
{
  "event_type": "OrderPaid",
  "order_id": "12345",
  "user_id": "42",
  "amount": 1999,
  "currency": "RUB",
  "timestamp": "2026-09-05T12:00:00Z"
}
```

Правильный смысл:

```text
Заказ 12345 был оплачен.
```

Это уже произошедший факт.

---

# 7. Event и command - не одно и то же

Command:

```text
ChargeCustomer
```

означает:

```text
сделай что-то
```

Event:

```text
CustomerCharged
```

означает:

```text
что-то уже произошло
```

В event-driven архитектуре это важное различие.

---

# 8. Где Kafka реально используют

Kafka особенно полезна там, где есть большой поток событий.

## Микросервисы

```text
orders
payments
shipments
notifications
```

## Аналитика

```text
clicks
views
purchases
searches
```

## Логи

```text
application logs
security events
audit logs
```

## IoT

```text
temperature
GPS
sensor data
device events
```

## Финансы

```text
transactions
payments
fraud events
market data
```

## CDC

Изменения базы данных:

```text
PostgreSQL
   |
   v
Debezium
   |
   v
Kafka
```

## Machine Learning

```text
user activity
     |
     v
Kafka
     |
     +--> feature pipeline
     +--> fraud model
     +--> recommendations
```

---

# 9. Когда Kafka не нужна

Kafka не нужно добавлять в каждый проект.

Если приложение:

```text
один backend
одна база
100 пользователей
несколько запросов в секунду
```

Kafka может только усложнить архитектуру.

Появятся:

```text
brokers
topics
partitions
replication
monitoring
schema management
consumer lag
deployments
storage
security
```

Для простого проекта обычного PostgreSQL + background jobs иногда достаточно.

---

# 10. Когда Kafka действительно полезна

Kafka стоит использовать, когда появляются задачи:

```text
много producers
много consumers
большой поток событий
независимое масштабирование
replay данных
отказоустойчивость
event-driven architecture
stream processing
```

---

# 11. Основные компоненты Kafka

Теперь введём главные понятия.

```text
Producer
   |
   v
Topic
   |
   v
Partition
   |
   v
Broker
   |
   v
Consumer
```

---

# 12. Producer

Producer - программа, которая пишет события в Kafka.

Например:

```text
Order Service
```

отправляет:

```text
OrderCreated
```

или:

```text
OrderPaid
```

---

# 13. Consumer

Consumer - приложение, которое читает события.

Например:

```text
Notification Service
```

читает:

```text
OrderPaid
```

и отправляет письмо.

---

# 14. Topic

Topic - логическая категория событий.

Например:

```text
orders
payments
users
notifications
```

Можно представить topic как поток:

```text
orders

OrderCreated
OrderPaid
OrderPacked
OrderShipped
OrderCancelled
```

---

# 15. Broker

Broker - сервер Kafka.

Один Kafka cluster обычно состоит из нескольких brokers:

```text
Kafka Cluster

broker-1
broker-2
broker-3
```

Каждый broker хранит часть partitions.

---

# 16. Partition

Topic делится на partitions.

Например:

```text
topic: orders

partition 0
partition 1
partition 2
```

Это позволяет Kafka масштабироваться горизонтально.

---

# 17. Почему partition важнее topic

На практике Kafka работает не просто с topic.

Основная единица хранения:

```text
topic + partition
```

Например:

```text
orders-0
orders-1
orders-2
```

Каждая partition является отдельным упорядоченным log.

---

# 18. Offset

Внутри partition каждому record присваивается номер:

```text
offset
```

Например:

```text
partition 0

offset 0 -> A
offset 1 -> B
offset 2 -> C
offset 3 -> D
```

Offset - позиция записи внутри partition.

---

# 19. Consumer Group

Допустим есть:

```text
6 partitions
```

И три экземпляра приложения:

```text
consumer-1
consumer-2
consumer-3
```

Они могут образовать:

```text
consumer group
```

Kafka распределит partitions между ними.

---

# 20. Зачем Kafka хранит сообщения

Это одно из главных отличий Kafka.

Consumer может:

```text
прочитать сообщение сегодня
```

а другой consumer:

```text
прочитать его завтра
```

Можно даже сделать replay:

```text
переместить offset назад
```

и снова обработать старые события.

---

# 21. Где replay полезен

Представим, что ты написал новый recommendation algorithm.

В Kafka есть события пользователей за семь дней.

Вместо ожидания новых данных можно:

```text
перечитать старые события
```

новой версией consumer.

---

# 22. Kafka как буфер между системами

Producer создаёт:

```text
100 000 msg/sec
```

Consumer способен обработать:

```text
50 000 msg/sec
```

Kafka может временно накопить backlog.

```text
Producer
100k/s
   |
   v
Kafka
████████████████
   |
   v
Consumer
50k/s
```

Consumer позже догоняет поток.

---

# 23. Что Kafka НЕ делает автоматически

Kafka не решает автоматически:

```text
бизнес-логику
идемпотентность приложения
правильную схему событий
архитектуру retry
обработку poison messages
monitoring
security
```

Kafka даёт инфраструктурные механизмы.

Корректность системы всё равно нужно проектировать.

---

# 24. Что установим для курса

Нам понадобится:

```text
Docker
Docker Compose
Java 17+
Git
любая IDE
```

Самый простой вариант для курса - Docker.

Для примеров используется официальный образ:

```text
apache/kafka:4.3.1
```

---

# 25. Проверяем Docker

```bash
docker --version
```

Проверяем Compose:

```bash
docker compose version
```

---

# 26. Самый быстрый запуск Kafka

```bash
docker pull apache/kafka:4.3.1
```

Запуск:

```bash
docker run -d \
  --name kafka \
  -p 9092:9092 \
  apache/kafka:4.3.1
```

Проверяем:

```bash
docker ps
```

---

# 27. Создаём первый topic

```bash
docker exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --topic hello-kafka
```

---

# 28. Проверяем topic

```bash
docker exec kafka \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --describe \
  --topic hello-kafka
```

---

# 29. Отправляем первое сообщение

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka
```

После запуска введи:

```text
hello
my first kafka event
order-123 created
```

---

# 30. Читаем события

Во втором терминале:

```bash
docker exec -it kafka \
  /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 \
  --topic hello-kafka \
  --from-beginning
```

Ты увидишь:

```text
hello
my first kafka event
order-123 created
```

Это уже минимальная Kafka pipeline:

```text
Producer
   |
   v
Kafka
   |
   v
Consumer
```

---

# 31. Что произошло внутри

Когда ты ввёл:

```text
order-123 created
```

произошла цепочка:

```text
Console Producer
      |
      v
Kafka Protocol
      |
      v
Broker
      |
      v
Topic
      |
      v
Partition
      |
      v
Log on disk
```

Consumer затем сделал fetch и прочитал record.

---

# 32. Где Kafka хранит эти данные

Kafka пишет partition на диск.

Упрощённо:

```text
data/
└── hello-kafka-0/
    ├── 00000000000000000000.log
    ├── 00000000000000000000.index
    └── ...
```

Kafka не просто пересылает сообщение между двумя приложениями.

Она хранит его.

---

# 33. Почему курс начинается именно с архитектуры

Можно быстро выучить команды:

```bash
kafka-topics.sh
kafka-console-producer.sh
kafka-console-consumer.sh
```

Но этого недостаточно.

Большинство production-проблем Kafka связано не с синтаксисом команд.

Они связаны с вопросами:

```text
почему вырос lag?
почему появились duplicates?
почему consumer rebalance?
почему одна partition перегружена?
почему broker потерял ISR?
почему запись стала медленной?
почему после увеличения partitions изменился ordering?
```

Чтобы отвечать на них, нужно понимать внутреннюю модель Kafka.

---

# 34. Как проходить этот курс

Не просто читай главы.

После каждого блока запускай команды.

Хороший цикл обучения:

```text
прочитал
↓
запустил
↓
сломал
↓
посмотрел metrics/logs
↓
исправил
↓
объяснил себе почему
```

---

# 35. Практическая философия курса

Мы специально будем:

```text
убивать brokers
останавливать consumers
создавать lag
создавать duplicates
ломать ordering
провоцировать rebalance
заполнять producer buffer
создавать hot partitions
```

Потому что Kafka лучше всего понимается через failure scenarios.

---

# 36. Что ты должен уметь после всего курса

После прохождения курса ты должен уметь не просто сказать:

```text
"Я работал с Kafka"
```

а объяснить:

```text
как выбрать количество partitions

как выбрать message key

как работает replication

что такое ISR

как Kafka выбирает leader

как producer batching влияет на latency

как работает idempotent producer

как появляются duplicates

как правильно commit'ить offsets

чем at-least-once отличается от exactly-once

как работает consumer rebalance

как расследовать consumer lag

как найти hot partition

как Kafka использует page cache

как настроить retention и compaction

как проектировать retry и DLT

как мониторить Kafka

как пережить падение broker

как оценить capacity cluster
```

---

# 37. Карта всего курса

## Часть 1 - Kafka Fundamentals

```text
architecture
KRaft
brokers
topics
partitions
replication
ISR
offsets
consumer groups
retention
compaction
```

## Часть 2 - Producer & Consumer Internals

```text
batching
RecordAccumulator
compression
retries
idempotence
fetch
poll
commits
delivery semantics
lag
rebalance
Consumer Group Protocol
```

## Часть 3 - Broker Internals & Performance

```text
log segments
indexes
page cache
replication
high watermark
leader epoch
disk I/O
network threads
request handlers
capacity planning
```

## Часть 4 - Reliability

```text
transactions
exactly-once
retry
DLT
idempotency
failure scenarios
disaster recovery
```

## Часть 5 - Observability

```text
JMX
Prometheus
Grafana
consumer lag
under-replicated partitions
ISR
request latency
disk pressure
alerts
```

## Часть 6 - Production Architecture

```text
security
TLS
SASL
ACL
Schema Registry
CDC
Debezium
Kafka Connect
Kubernetes
multi-region
capacity planning
```

---

# 38. Первый mental model

На старте достаточно помнить одну схему:

```text
Producer
   |
   v
Topic
   |
   v
Partitions
   |
   v
Kafka Brokers
   |
   v
Consumer Group
```

Дальше весь курс будет постепенно раскрывать каждую стрелку этой схемы.

---

# 39. Почему не обычный HTTP

Почему между сервисами вообще ставить Kafka, если можно сделать обычный HTTP?

HTTP отлично подходит, когда нужен:

```text
request -> immediate response
```

Например:

```text
GET /users/42
```

Kafka полезнее, когда нужен:

```text
event happened
↓
несколько независимых систем могут обработать его сейчас или позже
```

Например:

```text
OrderPaid
```

Обе технологии часто используются одновременно.

---

# 40. HTTP и Kafka вместе

Типичная production-архитектура:

```text
Client
  |
  | HTTP
  v
Order API
  |
  | Kafka event
  v
Kafka
  |
  +--> Analytics
  +--> Notifications
  +--> Warehouse
  +--> Fraud
```

HTTP используется для synchronous request.

Kafka - для asynchronous event propagation.

---

# Что дальше

После этого введения переходим к первой части курса:

```text
Kafka Fundamentals
```

где подробно разберём:

```text
KRaft
brokers
topics
partitions
replication
ISR
leader election
offsets
consumer groups
retention
log compaction
```

А затем во второй части уйдём внутрь producer и consumer:

```text
batching
compression
retries
idempotence
poll
commits
delivery semantics
lag
rebalance
```
ообщений

Частая ошибка:

```text
Kafka = RabbitMQ для больших нагрузок
```

Лучше думать о Kafka как о распределённом append-only log.

```text
partition-0

offset
0 -> event A
1 -> event B
2 -> event C
3 -> event D
4 -> event E
```

Новые сообщения добавляются в конец:

```text
WRITE
  |
  v

[A][B][C][D][E][F]
                 ^
                 |
               append
```

Consumer не удаляет запись после чтения. Он хранит позицию:

```text
consumer offset = 3
```

Это означает: «я обработал всё до offset 3».

Сами данные продолжают храниться согласно retention policy.

---

## 2. Главная модель Kafka

Запомни иерархию:

```text
Cluster
  ↓
Broker
  ↓
Topic
  ↓
Partition
  ↓
Record
```

Каждый partition - отдельный последовательный log.

```text
partition 0
0 -> order-101
1 -> order-104
2 -> order-108

partition 1
0 -> order-102
1 -> order-105

partition 2
0 -> order-103
1 -> order-106
```

Порядок Kafka гарантирует только внутри конкретного partition.

---

## 3. Почему partitions нужны

Один partition ограничивает параллелизм:

```text
producer
   |
   v
+-----------+
| partition |
+-----------+
      |
      v
 consumer
```

Несколько partitions позволяют масштабировать запись и чтение:

```text
             +--> partition-0 --> consumer-1
producer ----+--> partition-1 --> consumer-2
             +--> partition-2 --> consumer-3
```

Partition определяет:

- параллелизм записи;
- параллелизм чтения;
- область гарантированного порядка;
- распределение данных между brokers;
- потенциальный throughput topic.

---

## 4. Практика - запускаем Kafka 4.x

Создай директорию:

```bash
mkdir kafka-course
cd kafka-course
```

Создай `docker-compose.yml`:

```yaml
services:
  kafka-1:
    image: apache/kafka:4.3.1
    container_name: kafka-1
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-1:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    ports:
      - "19092:9092"

  kafka-2:
    image: apache/kafka:4.3.1
    container_name: kafka-2
    environment:
      KAFKA_NODE_ID: 2
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-2:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    ports:
      - "29092:9092"

  kafka-3:
    image: apache/kafka:4.3.1
    container_name: kafka-3
    environment:
      KAFKA_NODE_ID: 3
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka-3:9092
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 3
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 2
    ports:
      - "39092:9092"
```

Запуск:

```bash
docker compose up -d
```

Проверка:

```bash
docker ps
```

---

## 5. KRaft вместо ZooKeeper

Современная Kafka использует KRaft для metadata management.

В учебном кластере каждый node выполняет две роли:

```yaml
KAFKA_PROCESS_ROLES: broker,controller
```

В production часто выгоднее разделять роли:

```text
controller-1
controller-2
controller-3

broker-1
broker-2
broker-3
broker-4
broker-5
```

Controller quorum управляет:

```text
topics
partitions
leaders
replicas
ISR
broker registrations
cluster state
```

Brokers обслуживают основной data path.

---

## 6. KRaft quorum

```yaml
KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka-1:9093,2@kafka-2:9093,3@kafka-3:9093
```

Один controller становится leader, остальные followers:

```text
        controller-1
           LEADER
          /      \
         /        \
        v          v
controller-2   controller-3
 FOLLOWER       FOLLOWER
```

Метаданные реплицируются через Raft.

---

## 7. Создаём первый production-like topic

```bash
docker exec -it kafka-1 bash
```

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic orders \
  --partitions 6 \
  --replication-factor 3
```

Проверяем:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic orders
```

Пример результата:

```text
Topic: orders
PartitionCount: 6
ReplicationFactor: 3

Partition: 0
Leader: 1
Replicas: 1,2,3
Isr: 1,2,3
```

---

## 8. Leader и replicas

Для каждого partition существует leader.

```text
partition-0

broker-1
LEADER

broker-2
FOLLOWER

broker-3
FOLLOWER
```

Producer пишет в leader:

```text
producer
   |
   v
broker-1
 leader
   |
   +----> broker-2
   |
   +----> broker-3
```

Followers копируют log leader.

---

## 9. ISR

ISR = In-Sync Replicas.

Это replicas, которые достаточно синхронизированы с leader.

```text
Leader: 1
Replicas: 1,2,3
ISR: 1,2,3
```

Если broker 3 отстаёт:

```text
Leader: 1
Replicas: 1,2,3
ISR: 1,2
```

Важно:

```text
replica != ISR
```

---

## 10. `min.insync.replicas`

Представим:

```text
replication.factor = 3
```

Но жив только один broker:

```text
broker-1 alive
broker-2 dead
broker-3 dead
```

Если разрешать запись, то после смерти broker-1 данные могут быть потеряны.

Поэтому используем:

```text
replication.factor = 3
min.insync.replicas = 2
```

Kafka требует минимум две ISR для безопасной записи при соответствующей настройке producer.

Создадим topic:

```bash
/opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --create \
  --topic payments \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2
```

---

## 11. Producer acknowledgements

Producer поддерживает:

```text
acks=0
acks=1
acks=all
```

### `acks=0`

Producer не ждёт подтверждения.

Максимальная скорость, минимальные гарантии.

### `acks=1`

Leader записал сообщение и подтвердил.

Followers могли ещё не получить запись.

### `acks=all`

Leader ждёт подтверждения от требуемого числа ISR.

Для важных данных обычно используют сочетание:

```text
acks=all
replication.factor=3
min.insync.replicas=2
```

`acks=all` без корректного `min.insync.replicas` не закрывает все сценарии потери данных.

---

## 12. Пишем и читаем сообщения

Producer:

```bash
/opt/kafka/bin/kafka-console-producer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders
```

Сообщения:

```text
order-1001
order-1002
order-1003
order-1004
```

Consumer:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders \
  --from-beginning
```

После чтения данные не удаляются. Их удаление контролируется retention policy.

---

## 13. Offset

Каждое сообщение внутри partition получает offset.

```text
partition 0

offset 0 -> A
offset 1 -> B
offset 2 -> C
offset 3 -> D
```

Offset уникален только внутри partition.

Полная позиция записи:

```text
topic = orders
partition = 3
offset = 42
```

---

## 14. Почему нет глобального порядка

```text
partition 0
A
C
E

partition 1
B
D
F
```

Kafka гарантирует:

```text
A < C < E
B < D < F
```

Но не гарантирует:

```text
A < B < C < D < E < F
```

между partitions.

---

## 15. Key и порядок событий

Producer может отправлять `key` и `value`.

```json
{
  "key": "user-42",
  "value": {
    "action": "payment"
  }
}
```

Упрощённо partition выбирается так:

```text
hash(key) % partition_count
```

Поэтому одинаковый key обычно попадает в один partition:

```text
user-42 -> partition 3
user-42 -> partition 3
user-42 -> partition 3
```

Это позволяет сохранять порядок событий конкретной сущности.

---

## 16. Практическая задача - банковский счёт

Есть события:

```text
account-42 deposit 100
account-42 withdraw 40
account-42 withdraw 20
```

Без key они могут оказаться в разных partitions и обработаться в неправильном порядке.

Правильный вариант:

```text
key = account_id
```

Тогда lifecycle одного счёта остаётся в одном partition.

---

## 17. Ловушка при увеличении числа partitions

Было:

```text
partitions = 6
hash(user-42) % 6 = 4
```

Стало:

```text
partitions = 12
hash(user-42) % 12 = 10
```

После увеличения количества partitions один и тот же key может начать маршрутизироваться иначе.

Поэтому количество partitions нужно проектировать заранее.

Увеличить их просто. Уменьшить обратно обычно требует нового topic и repartition.

---

## 18. Consumer Group

Topic `orders` имеет 6 partitions.

Три consumers в одной group могут получить по два partition:

```text
consumer-1
  partition 0
  partition 1

consumer-2
  partition 2
  partition 3

consumer-3
  partition 4
  partition 5
```

Правило:

```text
один partition внутри consumer group
одновременно обрабатывается максимум одним consumer
```

---

## 19. Consumers больше, чем partitions

```text
partitions = 3
consumers = 5
```

Результат:

```text
consumer-1 -> partition 0
consumer-2 -> partition 1
consumer-3 -> partition 2
consumer-4 -> idle
consumer-5 -> idle
```

Максимальный параллелизм consumer group ограничен числом partitions.

---

## 20. Запускаем consumer group вручную

В трёх терминалах запусти одну и ту же команду:

```bash
/opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka-1:9092 \
  --topic orders \
  --group order-service
```

Затем отправь 30 сообщений через producer и посмотри, как partitions распределяются между consumers.

---

## 21. Смотрим consumer lag

```bash
/opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server kafka-1:9092 \
  --group order-service \
  --describe
```

Пример:

```text
TOPIC   PARTITION CURRENT-OFFSET LOG-END-OFFSET LAG
orders  0         120            125            5
orders  1         90             90             0
orders  2         130            150            20
```

Формула:

```text
consumer lag = log end offset - current consumer offset
```

Но сам lag ещё не говорит, что система сломалась.

Нужно смотреть:

```text
lag
lag growth rate
consumer throughput
producer throughput
time lag
```

---

## 22. Rebalance

Если один consumer умер:

```text
consumer-2 DEAD
```

Kafka перераспределит его partitions.

До:

```text
consumer-1 -> p0,p1
consumer-2 -> p2,p3
consumer-3 -> p4,p5
```

После:

```text
consumer-1 -> p0,p1,p2
consumer-3 -> p3,p4,p5
```

Частые rebalance могут вызывать:

```text
latency spikes
lag spikes
duplicate processing
throughput collapse
```

Причины:

```text
consumer joined
consumer left
consumer crashed
session timeout
partitions changed
subscription changed
```

---

## 23. Практика - убиваем broker

Проверяем распределение:

```bash
docker exec kafka-1 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-1:9092 \
  --describe \
  --topic orders
```

Найди partition с:

```text
Leader: 1
```

Останови broker:

```bash
docker stop kafka-1
```

Проверь topic через другой broker:

```bash
docker exec kafka-2 \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server kafka-2:9092 \
  --describe \
  --topic orders
```

Было:

```text
Partition: 0
Leader: 1
Replicas: 1,2,3
ISR: 1,2,3
```

Стало, например:

```text
Partition: 0
Leader: 2
Replicas: 1,2,3
ISR: 2,3
```

Поднимаем broker обратно:

```bash
docker start kafka-1
```

После синхронизации он вернётся в ISR.

---

## 24. Production-антипаттерн: replication factor = 1

```text
replication.factor=1
```

Один broker умер - partition недоступен.

Для важных production topic обычно используют:

```text
replication.factor=3
```

---

## 25. Как Kafka хранит данные на диске

Partition разбивается на segment files:

```text
orders-0/

00000000000000000000.log
00000000000000000000.index

00000000000001000000.log
00000000000001000000.index

00000000000002000000.log
00000000000002000000.index
```

Новые записи идут в active segment.

Когда он достигает лимита `segment.bytes`, создаётся следующий.

---

## 26. Почему Kafka быстрая

Kafka сочетает:

```text
append-only writes
sequential disk I/O
OS page cache
batching
compression
zero-copy optimizations
partition parallelism
```

Kafka не обязана держать весь dataset в JVM heap.

---

## 27. Page Cache и JVM heap

```text
producer
   |
   v
Kafka
   |
   v
OS page cache
   |
   v
disk
```

Поэтому конфигурация вроде:

```text
server RAM = 64 GB
-Xmx60G
```

часто плохая идея.

Kafka и ОС конкурируют за память.

Нередко лучше:

```text
moderate JVM heap
+
large OS page cache
```

Конкретные значения зависят от workload.

---

## 28. Retention

Kafka удаляет старые segments по политике хранения.

Например:

```text
retention.ms=604800000
```

Это 7 дней.

Также можно ограничивать размер:

```text
retention.bytes
```

Важно: retention применяется к log segments, а не к отдельным сообщениям мгновенно.

---

## 29. Delete vs Compaction

### Delete

```text
cleanup.policy=delete
```

Старые данные удаляются по retention.

### Compact

```text
cleanup.policy=compact
```

Kafka старается сохранить последнее значение каждого key.

Было:

```text
user-1 -> name=Alex
user-2 -> name=Bob
user-1 -> name=John
user-2 -> name=Mike
```

После compaction логически остаётся актуальное состояние:

```text
user-1 -> name=John
user-2 -> name=Mike
```

Полезно для:

```text
CDC
state topics
configuration
entity snapshots
Kafka Streams
```

---

## 30. Мини-задача

Есть topic:

```text
user-settings
```

События:

```json
{
  "user_id": 42,
  "theme": "dark"
}
```

позже:

```json
{
  "user_id": 42,
  "theme": "light"
}
```

Нужно хранить последнее состояние пользователя.

Решение:

```text
cleanup.policy=compact
key = user_id
```

---

## 31. Чеклист topic design

Перед созданием topic ответь:

```text
1. Что является key?
2. Нужен ли ordering?
3. По какой entity нужен ordering?
4. Какой throughput ожидается?
5. Сколько consumer instances будет?
6. Какой replication factor?
7. Какой retention?
8. delete или compact?
9. Допустимы ли duplicates?
10. Что произойдёт при повторной обработке?
```

Если ты не можешь ответить хотя бы на половину этих вопросов, topic design ещё не готов.

---

## 32. Production exercise - интернет-магазин

Есть события:

```text
OrderCreated
OrderPaid
OrderCancelled
OrderShipped
```

Требование: для одного заказа порядок должен сохраняться.

Правильно:

```text
topic: orders
key = order_id
```

Плохо:

```text
key = event_type
```

Иначе lifecycle одного заказа разъедется по partitions.

---

# Домашняя практика

## Задание 1

Создай topic:

```text
transactions
```

Параметры:

```text
partitions=12
replication.factor=3
min.insync.replicas=2
```

## Задание 2

Запусти 4 consumers в одной consumer group и проверь распределение partitions.

## Задание 3

Добавь пятого consumer и посмотри rebalance.

## Задание 4

Добавь 13 consumers и ответь, сколько из них реально будут обрабатывать partitions.

## Задание 5

Убей broker, который является leader нескольких partitions. Проверь:

```text
leader election
ISR
availability
```

## Задание 6

Создай compacted topic:

```text
user-profile
cleanup.policy=compact
```

Отправь несколько значений с одинаковыми keys.

---

# Senior-вопросы после части 1

1. Почему увеличение количества partitions может сломать ordering по key?
2. Чем replica отличается от ISR?
3. Почему `acks=all` без `min.insync.replicas` недостаточно?
4. Почему 20 consumers не ускорят topic с 10 partitions?
5. Что произойдёт с partition leader при падении broker?
6. Почему Kafka может эффективно работать с диском и не держать весь dataset в JVM heap?
7. Почему высокий consumer lag не всегда означает проблему?
8. Когда использовать log compaction вместо retention delete?
9. Почему offset нельзя использовать как глобальный ID сообщения?
10. Что важнее при проектировании key - равномерность распределения или ordering?

Правильный senior-ответ на последний вопрос:

```text
зависит от бизнес-инварианта
```

Иногда ordering важнее идеального распределения.

---

# Что должен уметь после части 1

Ты должен без подсказок объяснить:

```text
Topic
Partition
Offset
Leader
Replica
ISR
KRaft
Controller quorum
Replication factor
min.insync.replicas
acks
Consumer group
Consumer lag
Rebalance
Retention
Log compaction
```

И уметь руками:

```text
поднять Kafka cluster
создать topic
настроить replication
писать сообщения
читать сообщения
создать consumer group
проверить lag
убить broker
наблюдать leader election
проверить ISR recovery
```

---

# Следующая часть

## Часть 2 - Producer и Consumer Internals

Разберём:

```text
ProducerRecord
RecordBatch
linger.ms
batch.size
compression
acks
retries
delivery.timeout.ms
request.timeout.ms
idempotence
max.in.flight.requests.per.connection
partitioner
sticky partitioning
metadata cache
RecordAccumulator
sender thread

fetch loop
offset commit
auto commit
manual commit
at-most-once
at-least-once
exactly-once
consumer heartbeat
group coordinator
rebalance protocols
static membership
cooperative rebalancing
```

В практике напишем production producer и consumer, намеренно создадим duplicates, потеряем сообщение, устроим consumer lag и затем исправим каждую проблему.
