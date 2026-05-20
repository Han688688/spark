#!/usr/bin/env python3
"""
生成Kafka Java API高质量完整文档（类似Spark文档格式）
"""

KAFKA_DOC = '''
# Kafka Java API 高质量完整文档

> **文档特点**:
> - 包含Kafka核心public Java API
> - 核心方法提供完整可运行示例
> - 按业务分类组织，便于测试覆盖
> - 基于Kafka 4.2.0版本

> **说明**: 
> - 核心API：用户直接调用的public方法（KafkaProducer、KafkaConsumer、AdminClient等）
> - 所有方法都是public，可直接调用测试

---

## 快速入门

### 1. Producer示例 - 发送消息到Kafka

```java
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class KafkaProducerExample {
    public static void main(String[] args) {
        // 1. 配置Producer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", StringSerializer.class.getName());
        
        // 2. 创建Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        
        // 3. 发送消息（异步）
        for (int i = 0; i < 10; i++) {
            ProducerRecord<String, String> record = 
                new ProducerRecord<>("test-topic", "key-" + i, "value-" + i);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.println("发送成功: " + metadata.topic() + 
                        "-" + metadata.partition() + "@" + metadata.offset());
                } else {
                    exception.printStackTrace();
                }
            });
        }
        
        // 4. 刷新并关闭
        producer.flush();
        producer.close();
    }
}
```

### 2. Consumer示例 - 从Kafka消费消息

```java
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import java.util.Properties;
import java.util.Arrays;

public class KafkaConsumerExample {
    public static void main(String[] args) {
        // 1. 配置Consumer
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        props.put("auto.offset.reset", "earliest");
        
        // 2. 创建Consumer并订阅Topic
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("test-topic"));
        
        // 3. 消费消息
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.println("收到消息: " + 
                    record.topic() + "-" + record.partition() + "@" + record.offset() +
                    " key=" + record.key() + " value=" + record.value());
            }
        }
    }
}
```

### 3. Admin示例 - 创建Topic

```java
import org.apache.kafka.clients.admin.*;
import java.util.Properties;
import java.util.Collections;

public class KafkaAdminExample {
    public static void main(String[] args) throws Exception {
        // 1. 配置AdminClient
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        
        // 2. 创建AdminClient
        AdminClient admin = AdminClient.create(props);
        
        // 3. 创建Topic
        NewTopic newTopic = new NewTopic("test-topic", 3, (short) 1);
        admin.createTopics(Collections.singletonList(newTopic)).all().get();
        
        // 4. 列出所有Topic
        ListTopicsResult topics = admin.listTopics();
        System.out.println("Topics: " + topics.names().get());
        
        // 5. 关闭AdminClient
        admin.close();
    }
}
```

### 4. Streams示例 - 流处理

```java
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.serialization.Serdes;
import java.util.Properties;

public class KafkaStreamsExample {
    public static void main(String[] args) {
        // 1. 配置Streams
        Properties props = new Properties();
        props.put("application.id", "stream-app");
        props.put("bootstrap.servers", "localhost:9092");
        props.put("default.key.serde", Serdes.String().getClass());
        props.put("default.value.serde", Serdes.String().getClass());
        
        // 2. 创建StreamsBuilder
        StreamsBuilder builder = new StreamsBuilder();
        
        // 3. 构建流处理逻辑
        KStream<String, String> stream = builder.stream("input-topic");
        stream.filter((key, value) -> value.length() > 5)
              .mapValues(value -> value.toUpperCase())
              .to("output-topic");
        
        // 4. 创建并启动KafkaStreams
        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        
        // 5. 添加关闭钩子
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
```

---

'''

# Producer API部分
PRODUCER_API = '''
## 一、Producer API

### KafkaProducer<K,V>
**包路径**: `org.apache.kafka.clients.producer`
**说明**: Kafka生产者，用于发送消息到Kafka集群。线程安全，可多线程共享。
**方法数量**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `KafkaProducer` | Properties props | 构造方法 | 创建生产者 | `Properties props = new Properties();<br>props.put("bootstrap.servers", "localhost:9092");<br>KafkaProducer<String, String> producer = new KafkaProducer<>(props);` |
| `KafkaProducer` | Properties props, Serializer<K> keySerializer, Serializer<V> valueSerializer | 构造方法 | 创建生产者（自定义序列化器） | `KafkaProducer<String, String> producer = new KafkaProducer<>(props, new StringSerializer(), new StringSerializer());` |
| `send` | ProducerRecord<K,V> record | `Future<RecordMetadata>` | 异步发送消息 | `Future<RecordMetadata> future = producer.send(new ProducerRecord<>("topic", "key", "value"));` |
| `send` | ProducerRecord<K,V> record, Callback callback | `Future<RecordMetadata>` | 异步发送消息（带回调） | `producer.send(record, (metadata, ex) -> {<br>    if (ex == null) {<br>        System.out.println("分区: " + metadata.partition() + " offset: " + metadata.offset());<br>    }<br>});` |
| `flush` | 无 | `void` | 刷新缓冲区，等待所有消息发送完成 | `producer.flush();  // 确保所有消息已发送` |
| `close` | 无 | `void` | 关闭生产者 | `producer.close();` |
| `close` | Duration timeout | `void` | 关闭生产者（带超时） | `producer.close(Duration.ofSeconds(30));` |
| `partitionsFor` | String topic | `List<PartitionInfo>` | 获取Topic分区信息 | `List<PartitionInfo> partitions = producer.partitionsFor("test-topic");` |
| `metrics` | 无 | `Map<MetricName,Metric>` | 获取生产者指标 | `Map<MetricName, Metric> metrics = producer.metrics();` |
| `initTransactions` | 无 | `void` | 初始化事务 | `producer.initTransactions();  // 启用事务前必须调用` |
| `beginTransaction` | 无 | `void` | 开始事务 | `producer.beginTransaction();` |
| `commitTransaction` | 无 | `void` | 提交事务 | `producer.commitTransaction();` |
| `abortTransaction` | 无 | `void` | 中止事务 | `producer.abortTransaction();  // 发生错误时回滚` |
| `sendOffsetsToTransaction` | Map<TopicPartition,OffsetAndMetadata> offsets, ConsumerGroupMetadata groupMetadata | `void` | 发送Consumer偏移量到事务 | `producer.sendOffsetsToTransaction(offsets, consumer.groupMetadata());  // 精确一次语义` |
| `registerMetricForSubscription` | KafkaMetric metric | `void` | 注册自定义指标 | `producer.registerMetricForSubscription(customMetric);` |

---

### ProducerRecord<K,V>
**包路径**: `org.apache.kafka.clients.producer`
**说明**: 生产者记录，包含要发送到Kafka的消息。
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ProducerRecord` | String topic, V value | 构造方法 | 创建记录（无key） | `ProducerRecord<String, String> record = new ProducerRecord<>("topic", "value");` |
| `ProducerRecord` | String topic, K key, V value | 构造方法 | 创建记录（有key） | `ProducerRecord<String, String> record = new ProducerRecord<>("topic", "key", "value");` |
| `ProducerRecord` | String topic, Integer partition, K key, V value | 构造方法 | 创建记录（指定分区） | `ProducerRecord<String, String> record = new ProducerRecord<>("topic", 0, "key", "value");` |
| `ProducerRecord` | String topic, Integer partition, Long timestamp, K key, V value | 构造方法 | 创建记录（指定时间戳） | `ProducerRecord<String, String> record = new ProducerRecord<>("topic", 0, System.currentTimeMillis(), "key", "value");` |
| `topic` | 无 | `String` | 获取Topic名称 | `String topic = record.topic();` |
| `partition` | 无 | `Integer` | 获取分区号 | `Integer partition = record.partition();` |
| `key` | 无 | `K` | 获取Key | `String key = record.key();` |
| `value` | 无 | `V` | 获取Value | `String value = record.value();` |
| `timestamp` | 无 | `Long` | 获取时间戳 | `Long timestamp = record.timestamp();` |

---

### RecordMetadata
**包路径**: `org.apache.kafka.clients.producer`
**说明**: 消息元数据，包含发送成功后的信息。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `topic` | 无 | `String` | 获取Topic | `String topic = metadata.topic();` |
| `partition` | 无 | `int` | 获取分区 | `int partition = metadata.partition();` |
| `offset` | 无 | `long` | 获取偏移量 | `long offset = metadata.offset();` |
| `timestamp` | 无 | `long` | 获取时间戳 | `long timestamp = metadata.timestamp();` |
| `serializedKeySize` | 无 | `int` | Key序列化大小 | `int keySize = metadata.serializedKeySize();` |
| `serializedValueSize` | 无 | `int` | Value序列化大小 | `int valueSize = metadata.serializedValueSize();` |

---

'''

# Consumer API部分
CONSUMER_API = '''
## 二、Consumer API

### KafkaConsumer<K,V>
**包路径**: `org.apache.kafka.clients.consumer`
**说明**: Kafka消费者，用于从Kafka集群消费消息。非线程安全，需单线程使用或同步。
**方法数量**: 40+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `KafkaConsumer` | Properties props | 构造方法 | 创建消费者 | `Properties props = new Properties();<br>props.put("bootstrap.servers", "localhost:9092");<br>props.put("group.id", "test-group");<br>KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);` |
| `subscribe` | Collection<String> topics | `void` | 订阅Topic列表 | `consumer.subscribe(Arrays.asList("topic1", "topic2"));` |
| `subscribe` | Collection<String> topics, ConsumerRebalanceListener listener | `void` | 订阅Topic（带Rebalance监听器） | `consumer.subscribe(topics, new MyRebalanceListener());` |
| `assign` | Collection<TopicPartition> partitions | `void` | 手动分配分区 | `consumer.assign(Arrays.asList(new TopicPartition("topic", 0)));` |
| `poll` | Duration timeout | `ConsumerRecords<K,V>` | 消费消息 | `ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));` |
| `commitSync` | 无 | `void` | 同步提交偏移量 | `consumer.commitSync();  // 提交当前偏移量` |
| `commitSync` | Map<TopicPartition,OffsetAndMetadata> offsets | `void` | 同步提交指定偏移量 | `consumer.commitSync(offsets);` |
| `commitAsync` | 无 | `void` | 异步提交偏移量 | `consumer.commitAsync();  // 不等待确认` |
| `commitAsync` | OffsetCommitCallback callback | `void` | 异步提交（带回调） | `consumer.commitAsync((offsets, exception) -> {<br>    if (exception != null) exception.printStackTrace();<br>});` |
| `seek` | TopicPartition partition, long offset | `void` | 跳转到指定偏移量 | `consumer.seek(new TopicPartition("topic", 0), 100);` |
| `seekToBeginning` | Collection<TopicPartition> partitions | `void` | 跳到起始位置 | `consumer.seekToBeginning(partitions);` |
| `seekToEnd` | Collection<TopicPartition> partitions | `void` | 跳到末尾位置 | `consumer.seekToEnd(partitions);` |
| `position` | TopicPartition partition | `long` | 获取当前偏移量 | `long offset = consumer.position(new TopicPartition("topic", 0));` |
| `committed` | TopicPartition partition | `OffsetAndMetadata` | 获取已提交的偏移量 | `OffsetAndMetadata committed = consumer.committed(partition);` |
| `pause` | Collection<TopicPartition> partitions | `void` | 暂停消费指定分区 | `consumer.pause(partitions);` |
| `resume` | Collection<TopicPartition> partitions | `void` | 恢复消费指定分区 | `consumer.resume(partitions);` |
| `paused` | 无 | `Set<TopicPartition>` | 获取暂停的分区 | `Set<TopicPartition> paused = consumer.paused();` |
| `partitionsFor` | String topic | `List<PartitionInfo>` | 获取Topic分区信息 | `List<PartitionInfo> partitions = consumer.partitionsFor("topic");` |
| `listTopics` | 无 | `Map<String,List<PartitionInfo>>` | 获取所有Topic | `Map<String, List<PartitionInfo>> topics = consumer.listTopics();` |
| `close` | 无 | `void` | 关闭消费者 | `consumer.close();` |
| `close` | Duration timeout | `void` | 关闭消费者（带超时） | `consumer.close(Duration.ofSeconds(30));` |
| `wakeup` | 无 | `void` | 唤醒消费者（中断poll） | `consumer.wakeup();  // 用于从其他线程中断poll` |
| `enforceRebalance` | 无 | `void` | 强制Rebalance | `consumer.enforceRebalance();` |
| `groupMetadata` | 无 | `ConsumerGroupMetadata` | 获取Consumer组元数据 | `ConsumerGroupMetadata metadata = consumer.groupMetadata();` |

---

### ConsumerRecord<K,V>
**包路径**: `org.apache.kafka.clients.consumer`
**说明**: 消费者记录，包含从Kafka消费的消息。
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `topic` | 无 | `String` | 获取Topic | `String topic = record.topic();` |
| `partition` | 无 | `int` | 获取分区 | `int partition = record.partition();` |
| `offset` | 无 | `long` | 获取偏移量 | `long offset = record.offset();` |
| `key` | 无 | `K` | 获取Key | `String key = record.key();` |
| `value` | 无 | `V` | 获取Value | `String value = record.value();` |
| `timestamp` | 无 | `long` | 获取时间戳 | `long timestamp = record.timestamp();` |
| `timestampType` | 无 | `TimestampType` | 获取时间戳类型 | `TimestampType type = record.timestampType();` |
| `headers` | 无 | `Headers` | 获取消息头 | `Headers headers = record.headers();` |
| `serializedKeySize` | 无 | `int` | Key序列化大小 | `int keySize = record.serializedKeySize();` |

---

### ConsumerRecords<K,V>
**包路径**: `org.apache.kafka.clients.consumer`
**说明**: 消费者记录集合，poll返回的结果。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `count` | 无 | `int` | 记录数量 | `int count = records.count();` |
| `isEmpty` | 无 | `boolean` | 是否为空 | `boolean empty = records.isEmpty();` |
| `records` | TopicPartition partition | `List<ConsumerRecord<K,V>>` | 获取指定分区的记录 | `List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);` |
| `records` | String topic | `Iterable<ConsumerRecord<K,V>>` | 获取指定Topic的记录 | `Iterable<ConsumerRecord<String, String>> topicRecords = records.records("topic");` |
| `partitions` | 无 | `Set<TopicPartition>` | 获取包含的分区 | `Set<TopicPartition> partitions = records.partitions();` |
| `iterator` | 无 | `Iterator<ConsumerRecord<K,V>>` | 获取迭代器 | `for (ConsumerRecord<String, String> record : records) {<br>    System.out.println(record.value());<br>}` |

---

'''

# Admin API部分
ADMIN_API = '''
## 三、Admin API

### AdminClient
**包路径**: `org.apache.kafka.clients.admin`
**说明**: Kafka管理客户端，用于执行管理操作（创建Topic、配置Broker等）。
**方法数量**: 40+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | Properties props | `AdminClient` | 创建AdminClient | `AdminClient admin = AdminClient.create(props);` |
| `createTopics` | Collection<NewTopic> topics | `CreateTopicsResult` | 创建Topic | `NewTopic topic = new NewTopic("test", 3, (short) 1);<br>admin.createTopics(Collections.singletonList(topic)).all().get();` |
| `deleteTopics` | Collection<String> topics | `DeleteTopicsResult` | 删除Topic | `admin.deleteTopics(Arrays.asList("topic1", "topic2")).all().get();` |
| `listTopics` | 无 | `ListTopicsResult` | 列出所有Topic | `Set<String> topics = admin.listTopics().names().get();` |
| `describeTopics` | Collection<String> topics | `DescribeTopicsResult` | 获取Topic详情 | `Map<String, TopicDescription> desc = admin.describeTopics(topicNames).all().get();` |
| `describeCluster` | 无 | `DescribeClusterResult` | 获取集群信息 | `DescribeClusterResult cluster = admin.describeCluster();` |
| `createPartitions` | Map<String,NewPartitions> partitions | `CreatePartitionsResult` | 增加分区数 | `admin.createPartitions(Map.of("topic", NewPartitions.increaseTo(10))).all().get();` |
| `describeConfigs` | ConfigResource... resources | `DescribeConfigsResult` | 获取配置 | `ConfigResource resource = new ConfigResource(ConfigResource.Type.BROKER, "0");<br>Map<ConfigResource, Config> configs = admin.describeConfigs(resource).all().get();` |
| `alterConfigs` | Map<ConfigResource,Config> configs | `AlterConfigsResult` | 修改配置 | `admin.alterConfigs(configMap).all().get();` |
| `createAcls` | Collection<AclBinding> acls | `CreateAclsResult` | 创建ACL | `admin.createAcls(aclBindings).all().get();` |
| `deleteAcls` | Collection<AclBindingFilter> filters | `DeleteAclsResult` | 删除ACL | `admin.deleteAcls(filters).all().get();` |
| `listAcls` | AclBindingFilter filter | `ListAclsResult` | 列出ACL | `Collection<AclBinding> acls = admin.listAcls(filter).all().get();` |
| `close` | 无 | `void` | 关闭AdminClient | `admin.close();` |
| `close` | Duration timeout | `void` | 关闭AdminClient（带超时） | `admin.close(Duration.ofSeconds(30));` |

---

### NewTopic
**包路径**: `org.apache.kafka.clients.admin`
**说明**: 新Topic定义，用于创建Topic时指定参数。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `NewTopic` | String name, int numPartitions, short replicationFactor | 构造方法 | 创建Topic定义 | `NewTopic topic = new NewTopic("test", 3, (short) 1);` |
| `NewTopic` | String name, Map<Integer,List<Integer>> replicasAssignments | 构造方法 | 创建Topic（指定副本分配） | `NewTopic topic = new NewTopic("test", replicaAssignments);` |
| `name` | 无 | `String` | 获取Topic名称 | `String name = topic.name();` |
| `numPartitions` | 无 | `int` | 获取分区数 | `int partitions = topic.numPartitions();` |
| `replicationFactor` | 无 | `int` | 获取副本因子 | `int replicas = topic.replicationFactor();` |
| `configs` | 无 | `Map<String,String>` | 获取配置 | `Map<String, String> configs = topic.configs();` |
| `configs` | Map<String,String> configs | `NewTopic` | 设置配置 | `topic.configs(Map.of("retention.ms", "86400000"));` |

---

'''

# Streams API部分
STREAMS_API = '''
## 四、Streams API

### KafkaStreams
**包路径**: `org.apache.kafka.streams`
**说明**: Kafka Streams应用入口，管理流处理拓扑。
**方法数量**: 30+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `KafkaStreams` | Topology topology, Properties props | 构造方法 | 创建Streams应用 | `KafkaStreams streams = new KafkaStreams(topology, props);` |
| `KafkaStreams` | Topology topology, StreamsConfig config | 构造方法 | 创建Streams应用 | `KafkaStreams streams = new KafkaStreams(topology, config);` |
| `start` | 无 | `void` | 启动Streams | `streams.start();` |
| `close` | 无 | `void` | 关闭Streams | `streams.close();` |
| `close` | Duration timeout | `void` | 关闭Streams（带超时） | `streams.close(Duration.ofSeconds(30));` |
| `state` | 无 | `State` | 获取当前状态 | `State state = streams.state();  // CREATED, RUNNING, REBALANCING等` |
| `isRunning` | 无 | `boolean` | 是否正在运行 | `boolean running = streams.isRunning();` |
| `localThreadsMetadata` | 无 | `Set<ThreadMetadata>` | 获取线程元数据 | `Set<ThreadMetadata> threads = streams.localThreadsMetadata();` |
| `metadataForLocalThreads` | 无 | `Map<Integer,ThreadMetadata>` | 获取线程元数据（Map形式） | `Map<Integer, ThreadMetadata> threads = streams.metadataForLocalThreads();` |
| `metrics` | 无 | `Map<MetricName,Metric>` | 获取指标 | `Map<MetricName, Metric> metrics = streams.metrics();` |
| `cleanUp` | 无 | `void` | 清理本地状态 | `streams.cleanUp();  // 清空本地状态存储` |
| `setUncaughtExceptionHandler` | Thread.UncaughtExceptionHandler handler | `void` | 设置异常处理器 | `streams.setUncaughtExceptionHandler((thread, exception) -> exception.printStackTrace());` |
| `setStateListener` | StateListener listener | `void` | 设置状态监听器 | `streams.setStateListener((newState, oldState) -> System.out.println("State: " + newState));` |

---

### StreamsBuilder
**包路径**: `org.apache.kafka.streams`
**说明**: Streams拓扑构建器，用于定义流处理逻辑。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StreamsBuilder` | 无 | 构造方法 | 创建构建器 | `StreamsBuilder builder = new StreamsBuilder();` |
| `stream` | String topic | `KStream<K,V>` | 创建KStream | `KStream<String, String> stream = builder.stream("input-topic");` |
| `stream` | Collection<String> topics | `KStream<K,V>` | 创建KStream（多Topic） | `KStream<String, String> stream = builder.stream(Arrays.asList("topic1", "topic2"));` |
| `table` | String topic | `KTable<K,V>` | 创建KTable | `KTable<String, String> table = builder.table("table-topic");` |
| `globalTable` | String topic | `GlobalKTable<K,V>` | 创建GlobalKTable | `GlobalKTable<String, String> globalTable = builder.globalTable("global-topic");` |
| `build` | 无 | `Topology` | 构建拓扑 | `Topology topology = builder.build();` |

---

### KStream<K,V>
**包路径**: `org.apache.kafka.streams.kstream`
**说明**: Kafka Streams记录流，代表无界的记录序列。
**方法数量**: 50+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `filter` | Predicate<K,V> predicate | `KStream<K,V>` | 过滤记录 | `KStream<String, String> filtered = stream.filter((key, value) -> value.length() > 5);` |
| `filterNot` | Predicate<K,V> predicate | `KStream<K,V>` | 反向过滤 | `KStream<String, String> filtered = stream.filterNot((key, value) -> value.isEmpty());` |
| `map` | KeyValueMapper<K,V,KeyValue<KR,VR>> mapper | `KStream<KR,VR>` | 映射记录 | `KStream<String, Integer> mapped = stream.map((key, value) -> KeyValue.pair(key, value.length()));` |
| `mapValues` | ValueMapper<V,VR> mapper | `KStream<K,VR>` | 映射Value | `KStream<String, String> mapped = stream.mapValues(value -> value.toUpperCase());` |
| `flatMap` | KeyValueMapper<K,V,Iterable<KeyValue<KR,VR>>> mapper | `KStream<KR,VR>` | 扁平映射 | `KStream<String, String> flatMapped = stream.flatMap((key, value) -> Arrays.asList(KeyValue.pair(key, value.split(","))));` |
| `flatMapValues` | ValueMapper<V,Iterable<VR>> mapper | `KStream<K,Iterable<VR>>` | 扁平映射Value | `KStream<String, String> flatMapped = stream.flatMapValues(value -> Arrays.asList(value.split(" ")));` |
| `groupBy` | KeyValueMapper<K,V,KR> selector | `KGroupedStream<KR,V>` | 分组 | `KGroupedStream<String, String> grouped = stream.groupBy((key, value) -> value.substring(0, 1));` |
| `groupByKey` | 无 | `KGroupedStream<K,V>` | 按Key分组 | `KGroupedStream<String, String> grouped = stream.groupByKey();` |
| `count` | 无 | `KTable<K,Long>` | 计数 | `KTable<String, Long> counts = stream.groupByKey().count();` |
| `reduce` | Reducer<V> reducer | `KTable<K,V>` | 聚合 | `KTable<String, String> reduced = stream.groupByKey().reduce((v1, v2) -> v1 + v2);` |
| `aggregate` | Initializer<VR> initializer, Aggregator<K,V,VR> aggregator | `KTable<K,VR>` | 自定义聚合 | `KTable<String, Integer> agg = stream.groupByKey().aggregate(() -> 0, (key, value, agg) -> agg + 1);` |
| `join` | KStream<K,V> other, ValueJoiner<V,V,VR> joiner, JoinWindows windows | `KStream<K,VR>` | Join两个Stream | `KStream<String, String> joined = stream1.join(stream2, (v1, v2) -> v1 + v2, JoinWindows.of(Duration.ofMinutes(5)));` |
| `leftJoin` | KStream<K,V> other, ValueJoiner<V,V,VR> joiner, JoinWindows windows | `KStream<K,VR>` | 左Join | `KStream<String, String> joined = stream1.leftJoin(stream2, (v1, v2) -> v1 + ":" + v2, windows);` |
| `merge` | KStream<K,V> stream | `KStream<K,V>` | 合并Stream | `KStream<String, String> merged = stream1.merge(stream2);` |
| `peek` | KeyValueMapper<K,V,Void> action | `KStream<K,V>` | 查看记录（不修改） | `stream.peek((key, value) -> System.out.println(key + "=" + value));` |
| `to` | String topic | `void` | 输出到Topic | `stream.to("output-topic");` |
| `to` | TopicNameExtractor<K,V> topicExtractor | `void` | 动态输出Topic | `stream.to((key, value, context) -> "output-" + key);` |
| `through` | String topic | `KStream<K,V>` | 输出到Topic并继续 | `KStream<String, String> continued = stream.through("intermediate-topic");` |
| `print` | Printed<K,V> printed | `void` | 打印记录 | `stream.print(Printed.toSysOut());` |
| `foreach` | ForeachAction<K,V> action | `void` | 遍历记录 | `stream.foreach((key, value) -> System.out.println(key));` |

---

### KTable<K,V>
**包路径**: `org.apache.kafka.streams.kstream`
**说明**: Kafka Streams表，代表有界的记录集合（按Key聚合）。
**方法数量**: 30+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `filter` | Predicate<K,V> predicate | `KTable<K,V>` | 过滤记录 | `KTable<String, String> filtered = table.filter((key, value) -> value.length() > 5);` |
| `mapValues` | ValueMapper<V,VR> mapper | `KTable<K,VR>` | 映射Value | `KTable<String, String> mapped = table.mapValues(value -> value.toUpperCase());` |
| `groupBy` | KeyValueMapper<K,V,KeyValue<KR,VR>> selector | `KGroupedTable<KR,VR>` | 分组 | `KGroupedTable<String, String> grouped = table.groupBy((key, value) -> KeyValue.pair(value, key));` |
| `join` | KTable<K,V> other, ValueJoiner<V,V,VR> joiner | `KTable<K,VR>` | Join两个Table | `KTable<String, String> joined = table1.join(table2, (v1, v2) -> v1 + v2);` |
| `leftJoin` | KTable<K,V> other, ValueJoiner<V,V,VR> joiner | `KTable<K,VR>` | 左Join | `KTable<String, String> joined = table1.leftJoin(table2, (v1, v2) -> v1 + ":" + v2);` |
| `toStream` | 无 | `KStream<K,V>` | 转为KStream | `KStream<String, String> stream = table.toStream();` |
| `toStream` | KeyValueMapper<K,V,KR> mapper | `KStream<KR,V>` | 转为KStream（修改Key） | `KStream<Integer, String> stream = table.toStream((key, value) -> key.hashCode());` |
| `to` | String topic | `void` | 输出到Topic | `table.to("output-topic");` |
| `suppress` | Suppressed<? extends K> suppressed | `KTable<K,V>` | 抑制中间结果 | `KTable<String, Long> suppressed = table.suppress(Suppressed.untilTimeLimit(Duration.ofMinutes(1)));` |

---

'''

# Serialization API部分
SERIALIZATION_API = '''
## 五、Serialization API

### Serializer<T>
**包路径**: `org.apache.kafka.common.serialization`
**说明**: 序列化器接口，将对象序列化为字节数组。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `configure` | Map<String,?> configs, boolean isKey | `void` | 配置序列化器 | `serializer.configure(configs, true);  // true表示序列化Key` |
| `serialize` | String topic, T data | `byte[]` | 序列化数据 | `byte[] bytes = serializer.serialize("topic", data);` |
| `close` | 无 | `void` | 关闭序列化器 | `serializer.close();` |

---

### StringSerializer
**包路径**: `org.apache.kafka.common.serialization`
**说明**: String序列化器，将String序列化为字节数组。
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `configure` | Map<String,?> configs, boolean isKey | `void` | 配置（可设置编码） | `StringSerializer serializer = new StringSerializer();` |
| `serialize` | String topic, String data | `byte[]` | 序列化String | `byte[] bytes = serializer.serialize("topic", "message");` |
| `close` | 无 | `void` | 关闭 | `serializer.close();` |

---

### IntegerSerializer
**包路径**: `org.apache.kafka.common.serialization`
**说明**: Integer序列化器。
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `serialize` | String topic, Integer data | `byte[]` | 序列化Integer | `IntegerSerializer serializer = new IntegerSerializer();<br>byte[] bytes = serializer.serialize("topic", 123);` |

---

### ByteArraySerializer
**包路径**: `org.apache.kafka.common.serialization`
**说明**: 字节数组序列化器（直接返回原数据）。
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `serialize` | String topic, byte[] data | `byte[]` | 序列化byte[] | `ByteArraySerializer serializer = new ByteArraySerializer();<br>byte[] bytes = serializer.serialize("topic", data);` |

---

### Deserializer<T>
**包路径**: `org.apache.kafka.common.serialization`
**说明**: 反序列化器接口，将字节数组反序列化为对象。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `configure` | Map<String,?> configs, boolean isKey | `void` | 配置反序列化器 | `deserializer.configure(configs, false);  // false表示反序列化Value` |
| `deserialize` | String topic, byte[] data | `T` | 反序列化数据 | `String value = deserializer.deserialize("topic", bytes);` |
| `close` | 无 | `void` | 关闭反序列化器 | `deserializer.close();` |

---

### StringDeserializer
**包路径**: `org.apache.kafka.common.serialization`
**说明**: String反序列化器。
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `deserialize` | String topic, byte[] data | `String` | 反序列化String | `StringDeserializer deserializer = new StringDeserializer();<br>String value = deserializer.deserialize("topic", bytes);` |

---

### IntegerDeserializer
**包路径**: `org.apache.kafka.common.serialization`
**说明**: Integer反序列化器。
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `deserialize` | String topic, byte[] data | `Integer` | 反序列化Integer | `IntegerDeserializer deserializer = new IntegerDeserializer();<br>Integer value = deserializer.deserialize("topic", bytes);` |

---

'''

# Common API部分
COMMON_API = '''
## 六、Common API

### TopicPartition
**包路径**: `org.apache.kafka.common`
**说明**: Topic和Partition的组合，标识特定分区。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `TopicPartition` | String topic, int partition | 构造方法 | 创建TopicPartition | `TopicPartition tp = new TopicPartition("test-topic", 0);` |
| `topic` | 无 | `String` | 获取Topic | `String topic = tp.topic();` |
| `partition` | 无 | `int` | 获取分区 | `int partition = tp.partition();` |
| `hashCode` | 无 | `int` | 哈希码 | `int hash = tp.hashCode();` |
| `equals` | Object obj | `boolean` | 判断相等 | `boolean equal = tp.equals(other);` |

---

### OffsetAndMetadata
**包路径**: `org.apache.kafka.common`
**说明**: 偏移量和元数据的组合，用于提交偏移量。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `OffsetAndMetadata` | long offset | 构造方法 | 创建偏移量 | `OffsetAndMetadata offset = new OffsetAndMetadata(100);` |
| `OffsetAndMetadata` | long offset, String metadata | 构造方法 | 创建偏移量（带元数据） | `OffsetAndMetadata offset = new OffsetAndMetadata(100, "metadata");` |
| `offset` | 无 | `long` | 获取偏移量 | `long offset = offsetMeta.offset();` |
| `metadata` | 无 | `String` | 获取元数据 | `String metadata = offsetMeta.metadata();` |

---

### PartitionInfo
**包路径**: `org.apache.kafka.common`
**说明**: 分区信息，包含分区详细信息。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `topic` | 无 | `String` | 获取Topic | `String topic = partitionInfo.topic();` |
| `partition` | 无 | `int` | 获取分区号 | `int partition = partitionInfo.partition();` |
| `leader` | 无 | `Node` | 获取Leader节点 | `Node leader = partitionInfo.leader();` |
| `replicas` | 无 | `Node[]` | 获取副本节点 | `Node[] replicas = partitionInfo.replicas();` |
| `inSyncReplicas` | 无 | `Node[]` | 获取ISR节点 | `Node[] isr = partitionInfo.inSyncReplicas();` |

---

### Headers
**包路径**: `org.apache.kafka.common.header`
**说明**: 消息头接口，包含自定义元数据。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | Header header | `Headers` | 添加消息头 | `headers.add(new RecordHeader("key", "value".getBytes()));` |
| `add` | String key, byte[] value | `Headers` | 添加消息头 | `headers.add("source", "app1".getBytes());` |
| `remove` | String key | `Headers` | 删除消息头 | `headers.remove("source");` |
| `headers` | String key | `Iterable<Header>` | 获取指定Key的消息头 | `Iterable<Header> hdrs = headers.headers("source");` |
| `lastHeader` | String key | `Header` | 获取最后一个指定Key的消息头 | `Header header = headers.lastHeader("source");` |
| `toArray` | 无 | `Header[]` | 转为数组 | `Header[] headerArray = headers.toArray();` |

---

'''

def create_kafka_doc(filepath):
    """创建Kafka Java API文档"""
    content = KAFKA_DOC + PRODUCER_API + CONSUMER_API + ADMIN_API + STREAMS_API + SERIALIZATION_API + COMMON_API
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    # 统计
    classes = content.count('### ')
    methods = content.count('| `')
    
    return classes, methods

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/kafka_java_api高质量完整文档.md'
    
    print("创建Kafka Java API高质量完整文档...")
    classes, methods = create_kafka_doc(filepath)
    
    print(f"  类数量: {classes}")
    print(f"  方法数量: {methods}")
    print(f"  输出: {filepath}")
    print("\n完成")
