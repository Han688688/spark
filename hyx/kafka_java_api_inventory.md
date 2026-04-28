# Kafka Java API 清单

> 基于Kafka代码仓 + 官方Javadoc (Kafka 4.2.0) 生成

## 官方文档参考

| 文档类型 | URL |
|---------|-----|
| **官方Javadoc** | https://kafka.apache.org/42/javadoc |
| **Producer API** | https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/producer/package-summary.html |
| **Consumer API** | https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/package-summary.html |
| **Admin API** | https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/admin/package-summary.html |
| **Connect API** | https://kafka.apache.org/42/javadoc/org/apache/kafka/connect/package-summary.html |
| **Streams API** | https://kafka.apache.org/42/javadoc/org/apache/kafka/streams/package-summary.html |
| **官方文档** | https://kafka.apache.org/documentation |

---

## 官方包描述 (Kafka 4.2.0)

根据官方Javadoc，Kafka 4.2.0 包含以下公共API包：

| 包名 | 官方描述 |
|------|---------|
| `org.apache.kafka.clients.admin` | Provides a Kafka client for performing administrative operations |
| `org.apache.kafka.clients.consumer` | Provides a Kafka client for consuming records from topics |
| `org.apache.kafka.clients.producer` | Provides a Kafka client for producing records to topics |
| `org.apache.kafka.common` | Provides shared functionality for Kafka clients and servers |
| `org.apache.kafka.common.acl` | Provides classes representing Access Control Lists |
| `org.apache.kafka.common.config` | Provides mechanisms for defining, parsing, validating configuration |
| `org.apache.kafka.common.errors` | Provides common exception classes |
| `org.apache.kafka.common.header` | Provides API for application-defined metadata attached to records |
| `org.apache.kafka.common.metrics` | Provides API for emitting metrics |
| `org.apache.kafka.common.quota` | Provides mechanisms for enforcing resource quotas |
| `org.apache.kafka.common.resource` | Provides client handles representing logical resources |
| `org.apache.kafka.common.security.auth` | Provides pluggable interfaces for authentication mechanisms |
| `org.apache.kafka.common.serialization` | Provides interface and implementations of serialization/deserialization |
| `org.apache.kafka.connect.connector` | Provides interfaces for Connector and Task implementations |
| `org.apache.kafka.connect.data` | Provides classes for representing data and schemas |
| `org.apache.kafka.connect.sink` | Provides API for implementing sink connectors |
| `org.apache.kafka.connect.source` | Provides API for implementing source connectors |
| `org.apache.kafka.connect.storage` | Provides interfaces for (de)serializing data |
| `org.apache.kafka.connect.transforms` | Provides interface for altering data by Connect |
| `org.apache.kafka.streams` | Provides Kafka Streams library for streaming data applications |
| `org.apache.kafka.streams.kstream` | Provides DSL for data flow computation over streams and tables |
| `org.apache.kafka.streams.processor` | Provides Processor API for data flow computation |
| `org.apache.kafka.streams.state` | Provides interfaces for managing intermediate state |
| `org.apache.kafka.streams.query` | Provides query API over state stores |

---

## 1. Producer API

**包路径:** `org.apache.kafka.clients.producer`

### 核心类

| 类名 | 类型 | 描述 |
|------|------|------|
| `Producer<K,V>` | 接口 | 生产者核心接口 |
| `KafkaProducer<K,V>` | 类 | 生产者实现类 |
| `MockProducer<K,V>` | 类 | 测试用模拟生产者 |
| `ProducerRecord<K,V>` | 类 | 生产者记录 |
| `RecordMetadata` | 类 | 记录元数据 |
| `ProducerConfig` | 类 | 生产者配置 |
| `Callback` | 接口 | 异步回调接口 |
| `Partitioner` | 接口 | 分区器接口 |
| `ProducerInterceptor<K,V>` | 接口 | 生产者拦截器 |
| `RoundRobinPartitioner` | 类 | 轮询分区器 |

### 使用示例

```java
Producer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("topic", "key", "value"), 
    (metadata, exception) -> {
        if (exception != null) {
            // 处理异常
        }
    });
producer.close();
```

---

## 2. Consumer API

**包路径:** `org.apache.kafka.clients.consumer`

### 核心类

| 类名 | 类型 | 描述 |
|------|------|------|
| `Consumer<K,V>` | 接口 | 消费者核心接口 |
| `KafkaConsumer<K,V>` | 类 | 消费者实现类 |
| `MockConsumer<K,V>` | 类 | 测试用模拟消费者 |
| `ConsumerRecord<K,V>` | 类 | 消费者记录 |
| `ConsumerRecords<K,V>` | 类 | 记录集合 |
| `ConsumerConfig` | 类 | 消费者配置 |
| `ConsumerRebalanceListener` | 接口 | 重平衡监听器 |
| `ConsumerInterceptor<K,V>` | 接口 | 消费者拦截器 |
| `ConsumerPartitionAssignor` | 接口 | 分区分配器 |
| `OffsetAndMetadata` | 类 | Offset和元数据 |
| `OffsetAndTimestamp` | 类 | Offset和时间戳 |
| `ConsumerGroupMetadata` | 类 | 消费者组元数据 |
| `OffsetCommitCallback` | 接口 | Offset提交回调 |
| `OffsetResetStrategy` | 枚举 | Offset重置策略 |

### 分区分配策略

| 类名 | 描述 |
|------|------|
| `RangeAssignor` | 范围分配策略 |
| `RoundRobinAssignor` | 轮询分配策略 |
| `StickyAssignor` | 粘性分配策略 |
| `CooperativeStickyAssignor` | 协作粘性分配策略 |

---

## 3. Share Consumer API (新特性)

**包路径:** `org.apache.kafka.clients.consumer`

| 类名 | 类型 | 描述 |
|------|------|------|
| `ShareConsumer<K,V>` | 接口 | Share Group消费者接口 |
| `KafkaShareConsumer<K,V>` | 类 | Share Group消费者实现 |
| `MockShareConsumer<K,V>` | 类 | 模拟Share消费者 |
| `AcknowledgeType` | 枚举 | 确认类型 |
| `AcknowledgementCommitCallback` | 接口 | 确认提交回调 |

---

## 4. Admin API

**包路径:** `org.apache.kafka.clients.admin`

### 核心类

| 类名 | 类型 | 描述 |
|------|------|------|
| `Admin` | 接口 | 管理接口核心 |
| `KafkaAdminClient` | 类 | Admin实现类 |
| `AdminClientConfig` | 类 | Admin配置 |

### Topic操作

| 类名 | 描述 |
|------|------|
| `CreateTopicsResult` | 创建主题结果 |
| `CreateTopicsOptions` | 创建主题选项 |
| `NewTopic` | 新主题定义 |
| `DeleteTopicsResult` | 删除主题结果 |
| `DeleteTopicsOptions` | 删除主题选项 |
| `ListTopicsResult` | 列出主题结果 |
| `ListTopicsOptions` | 列出主题选项 |
| `DescribeTopicsResult` | 描述主题结果 |
| `TopicDescription` | 主题描述 |
| `CreatePartitionsResult` | 创建分区结果 |
| `NewPartitions` | 新分区定义 |

### 配置操作

| 类名 | 描述 |
|------|------|
| `AlterConfigOp` | 配置修改操作 |
| `ConfigEntry` | 配置条目 |
| `DescribeConfigsResult` | 配置描述结果 |
| `ConfigResource` | 配置资源 |

### ACL操作

| 类名 | 描述 |
|------|------|
| `CreateAclsResult` | 创建ACL结果 |
| `CreateAclsOptions` | 创建ACL选项 |
| `DeleteAclsResult` | 删除ACL结果 |

### 消费者组操作

| 类名 | 描述 |
|------|------|
| `ListConsumerGroupsResult` | 列出消费者组结果 |
| `DescribeConsumerGroupsResult` | 描述消费者组结果 |
| `MemberDescription` | 成员描述 |
| `AlterConsumerGroupOffsetsResult` | 修改Offset结果 |

### 其他操作

| 类名 | 描述 |
|------|------|
| `DeleteRecordsResult` | 删除记录结果 |
| `RecordsToDelete` | 待删除记录 |
| `ListOffsetsResult` | 列出Offset结果 |
| `ElectLeadersResult` | 领导者选举结果 |
| `FeatureMetadata` | 特性元数据 |
| `UpdateFeaturesResult` | 更新特性结果 |

---

## 5. Common API

**包路径:** `org.apache.kafka.common`

### 核心类

| 类名 | 描述 |
|------|------|
| `TopicPartition` | 主题分区 |
| `TopicPartitionInfo` | 主题分区信息 |
| `PartitionInfo` | 分区信息 |
| `Node` | 节点信息 |
| `Cluster` | 集群信息 |
| `Metric` | 指标 |
| `MetricName` | 指标名称 |
| `KafkaFuture` | 异步结果 |
| `Configurable` | 可配置接口 |
| `Reconfigurable` | 可重配置接口 |
| `Uuid` | UUID |

### Serialization

| 类名 | 类型 | 描述 |
|------|------|------|
| `Serializer<T>` | 接口 | 序列化接口 |
| `Deserializer<T>` | 接口 | 反序列化接口 |
| `Serde<T>` | 接口 | 序列化/反序列化组合 |
| `Serdes` | 类 | 预定义Serde工厂 |

### 预定义序列化器

| 类名 | 描述 |
|------|------|
| `StringSerializer/Deserializer` | String序列化 |
| `IntegerSerializer/Deserializer` | Integer序列化 |
| `LongSerializer/Deserializer` | Long序列化 |
| `DoubleSerializer/Deserializer` | Double序列化 |
| `ByteArraySerializer/Deserializer` | ByteArray序列化 |

### ACL

| 类名 | 描述 |
|------|------|
| `AclBinding` | ACL绑定 |
| `AclBindingFilter` | ACL绑定过滤器 |
| `AclOperation` | ACL操作类型 |
| `AclPermissionType` | ACL权限类型 |
| `AccessControlEntry` | 访问控制条目 |

### Security

| 类名 | 描述 |
|------|------|
| `SecurityProtocol` | 安全协议 |
| `KafkaPrincipal` | Principal |
| `KafkaPrincipalBuilder` | Principal构建器 |
| `AuthenticateCallbackHandler` | 认证回调处理 |
| `SslEngineFactory` | SSL引擎工厂 |

### Metrics

| 类名 | 描述 |
|------|------|
| `Metrics` | 指标系统 |
| `MetricConfig` | 指标配置 |
| `Sensor` | 指标传感器 |
| `MetricsReporter` | 指标报告器 |
| `JmxReporter` | JMX报告器 |

---

## 6. Connect API

**包路径:** `org.apache.kafka.connect`

### Connector核心

| 类名 | 描述 |
|------|------|
| `Connector` | Connector基类 |
| `ConnectorContext` | Connector上下文 |
| `Task` | Task接口 |
| `ConnectRecord` | 记录基类 |

### Source Connector

**包路径:** `org.apache.kafka.connect.source`

| 类名 | 描述 |
|------|------|
| `SourceConnector` | Source Connector基类 |
| `SourceTask` | Source Task基类 |
| `SourceRecord` | Source记录 |
| `SourceTaskContext` | Source Task上下文 |
| `ExactlyOnceSupport` | 恰好一次支持 |

### Sink Connector

**包路径:** `org.apache.kafka.connect.sink`

| 类名 | 描述 |
|------|------|
| `SinkConnector` | Sink Connector基类 |
| `SinkTask` | Sink Task基类 |
| `SinkRecord` | Sink记录 |
| `SinkTaskContext` | Sink Task上下文 |
| `ErrantRecordReporter` | 错误记录报告器 |

### Data API

**包路径:** `org.apache.kafka.connect.data`

| 类名 | 描述 |
|------|------|
| `Schema` | Schema接口 |
| `SchemaBuilder` | Schema构建器 |
| `Struct` | 结构化数据 |
| `Field` | Schema字段 |
| `SchemaAndValue` | Schema和值 |
| `Date` | Date类型 |
| `Time` | Time类型 |
| `Timestamp` | Timestamp类型 |
| `Decimal` | Decimal类型 |

### Storage

**包路径:** `org.apache.kafka.connect.storage`

| 类名 | 描述 |
|------|------|
| `Converter` | 转换器接口 |
| `HeaderConverter` | 头转换器 |
| `StringConverter` | String转换器 |
| `OffsetStorageReader` | Offset存储读取器 |

### Transforms

**包路径:** `org.apache.kafka.connect.transforms`

| 接口 | 描述 |
|------|------|
| `Transformation<R>` | 转换接口 |
| `Predicate<R>` | 断言接口 |

---

## 7. Streams API

**包路径:** `org.apache.kafka.streams`

### 核心类

| 类名 | 描述 |
|------|------|
| `KafkaStreams` | Streams客户端核心 |
| `StreamsBuilder` | 流构建器(DSL入口) |
| `Topology` | 拓扑定义 |
| `TopologyDescription` | 拓扑描述 |
| `StreamsConfig` | Streams配置 |
| `StreamsMetrics` | Streams指标 |
| `KeyValue<K,V>` | Key-Value对 |
| `StoreQueryParameters<T>` | 存储查询参数 |
| `KeyQueryMetadata` | Key查询元数据 |
| `StreamsMetadata` | Streams元数据 |

### KStream DSL

**包路径:** `org.apache.kafka.streams.kstream`

| 类名 | 描述 |
|------|------|
| `KStream<K,V>` | 流接口 |
| `KTable<K,V>` | 表接口 |
| `GlobalKTable<K,V>` | 全局表接口 |
| `KGroupedStream<K,V>` | 分组流 |
| `KGroupedTable<K,V>` | 分组表 |
| `CogroupedKStream<K,V>` | 协同分组流 |
| `TimeWindowedKStream<K,V>` | 时间窗口流 |
| `SessionWindowedKStream<K,V>` | Session窗口流 |

### 窗口定义

| 类名 | 描述 |
|------|------|
| `Windows` | 窗口基类 |
| `TimeWindows` | 时间窗口 |
| `SessionWindows` | Session窗口 |
| `SlidingWindows` | 滑动窗口 |
| `UnlimitedWindows` | 无限窗口 |
| `JoinWindows` | 连接窗口 |

### 操作接口

| 接口 | 描述 |
|------|------|
| `Predicate<K,V>` | 断言 |
| `KeyValueMapper<K,V,R>` | Key-Value映射 |
| `ValueMapper<V,R>` | 值映射 |
| `ValueJoiner<V1,V2,R>` | 值连接 |
| `Aggregator<K,V,VA>` | 聚合器 |
| `Initializer<VA>` | 初始化器 |
| `Reducer<V>` | 归约器 |
| `ForeachAction<K,V>` | foreach操作 |
| `Transformer<K,V,R>` | 转换器 |
| `ValueTransformer<V,R>` | 值转换器 |

### Processor API

**包路径:** `org.apache.kafka.streams.processor`

| 类/接口 | 描述 |
|---------|------|
| `StateStore` | 状态存储接口 |
| `ProcessorContext` | 处理器上下文 |
| `TaskId` | Task标识 |
| `TimestampExtractor` | 时间戳提取器 |
| `Punctuator` | 定时回调 |
| `StreamPartitioner<K,V>` | 流分区器 |
| `TopicNameExtractor<K,V>` | 主题名提取器 |

### State Store

**包路径:** `org.apache.kafka.streams.state`

| 类/接口 | 描述 |
|---------|------|
| `KeyValueStore<K,V>` | Key-Value存储 |
| `WindowStore<K,V>` | 窗口存储 |
| `SessionStore<K,V>` | Session存储 |
| `VersionedKeyValueStore<K,V>` | 版本化KV存储 |
| `ReadOnlyKeyValueStore<K,V>` | 只读KV存储 |
| `Stores` | 存储工厂 |
| `StoreBuilder<T>` | 存储构建器 |
| `KeyValueIterator<K,V>` | KV迭代器 |
| `WindowStoreIterator<T>` | 窗口存储迭代器 |

---

## 8. MirrorMaker API

**包路径:** `org.apache.kafka.connect.mirror`

| 类名 | 描述 |
|------|------|
| `MirrorClient` | MirrorMaker客户端 |
| `MirrorClientConfig` | MirrorMaker配置 |
| `ReplicationPolicy` | 复制策略接口 |
| `DefaultReplicationPolicy` | 默认复制策略 |
| `IdentityReplicationPolicy` | 身份复制策略 |
| `RemoteClusterUtils` | 远程集群工具 |
| `MirrorSourceConnector` | Source Connector |
| `MirrorSourceTask` | Source Task |

---

## 模块依赖关系

```
clients (核心客户端)
    ├── producer
    ├── consumer
    ├── admin
    └── common
    
connect (连接器框架)
    ├── api (接口定义)
    ├── runtime (运行时)
    ├── transforms (转换)
    └── mirror (MirrorMaker)
    
streams (流处理)
    ├── kstream (DSL)
    ├── processor (Processor API)
    └── state (状态存储)
```

---

## 使用示例

### Producer

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", StringSerializer.class);
props.put("value.serializer", StringSerializer.class);

KafkaProducer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("topic", "key", "value"));
producer.close();
```

### Consumer

```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("group.id", "group1");
props.put("key.deserializer", StringDeserializer.class);
props.put("value.deserializer", StringDeserializer.class);

KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList("topic"));

while (true) {
    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
    for (ConsumerRecord<String, String> record : records) {
        // 处理记录
    }
}
```

### Streams

```java
StreamsBuilder builder = new StreamsBuilder();
KStream<String, String> stream = builder.stream("input");
stream.filter((key, value) -> value != null)
      .mapValues(value -> value.toUpperCase())
      .to("output");

KafkaStreams streams = new KafkaStreams(builder.build(), props);
streams.start();
```

---

## 参考链接

- Kafka官方文档: https://kafka.apache.org/documentation
- Java Client API: https://kafka.apache.org/documentation/#api