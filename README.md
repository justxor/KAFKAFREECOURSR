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
   v
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
## 1. Kafka - не просто очередь сообщений

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
