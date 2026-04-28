# Apache大数据组件公共Java API完整清单

> **用户可直接使用的稳定公共API汇总**
> 
> 结合代码仓稳定性标注 + 官方文档验证

---

## 使用指南

| 标注 | 含义 | 使用建议 |
|------|------|----------|
| ✅ Stable | 稳定API，保证向后兼容 | **推荐使用**，跨版本无需修改 |
| ⚠️ Evolving | 演进中，可能变化 | 可用，但关注版本迁移指南 |
| ❌ Unstable | 不稳定，无保证 | 仅用于测试，不建议生产 |
| 🆕 New | 新增API | 查看最新版本支持情况 |
| ⛔ Deprecated | 已废弃 | **应迁移**到替代API |

---

# 一、Spark Java公共API

## 官方文档参考

| 文档 | URL |
|------|-----|
| Spark 3.5.6 Javadoc | https://spark.apache.org/docs/3.5.6/api/java/ |
| Spark编程指南 | https://spark.apache.org/docs/latest/ |

---

## 1.1 Java函数接口（24个）✅全部Stable

**包路径**: `org.apache.spark.api.java.function`

| 接口名 | 标注 | 方法签名 | 用途 |
|--------|------|----------|------|
| **Function<T,R>** | ✅ Stable | `R call(T t)` | 单参数转换 |
| **MapFunction<T,U>** | ✅ Stable | `U call(T t)` | Dataset.map() |
| **FilterFunction<T>** | ✅ Stable | `boolean call(T t)` | Dataset.filter() |
| **FlatMapFunction<T,U>** | ✅ Stable | `Iterator<U> call(T t)` | Dataset.flatMap() |
| **MapPartitionsFunction<T,U>** | ✅ Stable | `Iterator<U> call(Iterator<T> it)` | 分区级map |
| **PairFunction<T,K,V>** | ✅ Stable | `Tuple2<K,V> call(T t)` | 转为键值对 |
| **PairFlatMapFunction<T,K,V>** | ✅ Stable | `Iterator<Tuple2<K,V>> call(T t)` | flatMap转键值对 |
| **ReduceFunction<T>** | ✅ Stable | `T call(T v1, T v2)` | reduce聚合 |
| **ForeachFunction<T>** | ✅ Stable | `void call(T t)` | foreach遍历 |
| **ForeachPartitionFunction<T>** | ✅ Stable | `void call(Iterator<T> it)` | 分区级foreach |
| **FlatMapGroupsFunction<K,V,U>** | ✅ Stable | `Iterator<U> call(K key, Iterator<V> values)` | 分组flatMap |
| **MapGroupsFunction<K,V,U>** | ✅ Stable | `U call(K key, Iterator<V> values)` | 分组map |
| **FlatMapGroupsWithStateFunction<K,V,S,U>** | ⚠️ Evolving | `Iterator<U> call(K key, Iterator<V> values, GroupState<S> state)` | 带状态分组flatMap |
| **MapGroupsWithStateFunction<K,V,S,U>** | ⚠️ Evolving | `U call(K key, Iterator<V> values, GroupState<S> state)` | 带状态分组map |
| **DoubleFunction<T>** | ✅ Stable | `double call(T t)` | 返回Double |
| **DoubleFlatMapFunction<T>** | ✅ Stable | `Iterator<Double> call(T t)` | Double flatMap |
| **Function0<R>** | ✅ Stable | `R call()` | 无参数函数 |
| **Function2<T1,T2,R>** | ✅ Stable | `R call(T1 t1, T2 t2)` | 双参数函数 |
| **Function3<T1,T2,T3,R>** | ✅ Stable | `R call(T1, T2, T3)` | 三参数函数 |
| **Function4<T1,T2,T3,T4,R>** | ✅ Stable | `R call(T1, T2, T3, T4)` | 四参数函数 |
| **VoidFunction<T>** | ✅ Stable | `void call(T t)` | 无返回值 |
| **VoidFunction2<T1,T2>** | ✅ Stable | `void call(T1, T2)` | 双参数无返回 |
| **CoGroupFunction<K,V1,V2,R>** | ✅ Stable | `Iterator<R> call(K, Iterator<V1>, Iterator<V2>)` | 双Dataset分组合并 |
| **FlatMapFunction2<T1,T2,U>** | ✅ Stable | `Iterator<U> call(T1, T2)` | 双输入flatMap |

---

## 1.2 UDF接口（23个）✅全部Stable

**包路径**: `org.apache.spark.sql.api.java`

| 接口名 | 标注 | 方法签名 | 用途 |
|--------|------|----------|------|
| **UDF0<R>** | ✅ Stable | `R call()` | 无参数UDF |
| **UDF1<T1,R>** | ✅ Stable | `R call(T1 t1)` | 1参数UDF |
| **UDF2<T1,T2,R>** | ✅ Stable | `R call(T1, T2)` | 2参数UDF |
| **UDF3-UDF22** | ✅ Stable | `R call(T1...Tn)` | 3-22参数UDF |

**使用示例**:
```java
// 注册UDF
spark.udf().register("myUdf", new UDF2<String, Integer, String>() {
    @Override
    public String call(String s, Integer i) {
        return s + "_" + i;
    }
}, DataTypes.StringType);

// SQL中使用
spark.sql("SELECT myUdf(name, age) FROM users");
```

---

## 1.3 Dataset核心方法（Java特有）

**包路径**: `org.apache.spark.sql.Dataset`

| 方法 | 标注 | 说明 |
|------|------|------|
| `map(MapFunction<T,U>, Encoder<U>)` | ✅ Stable | 映射转换 |
| `filter(FilterFunction<T>)` | ✅ Stable | 过滤 |
| `flatMap(FlatMapFunction<T,U>, Encoder<U>)` | ✅ Stable | flatMap |
| `mapPartitions(MapPartitionsFunction<T,U>, Encoder<U>)` | ✅ Stable | 分区级map |
| `groupByKey(MapFunction<T,K>, Encoder<K>)` | ✅ Stable | 分组为KeyValueGroupedDataset |
| `reduce(ReduceFunction<T>)` | ✅ Stable | reduce聚合 |
| `foreach(ForeachFunction<T>)` | ✅ Stable | 遍历 |
| `foreachPartition(ForeachPartitionFunction<T>)` | ✅ Stable | 分区级遍历 |
| `javaRDD()` | ✅ Stable | 转为JavaRDD |

---

## 1.4 Streaming API

**包路径**: `org.apache.spark.sql.streaming`

| 类/接口 | 标注 | 说明 |
|---------|------|------|
| **StreamingQuery** | ✅ Stable | 流式查询接口 |
| **StreamingQueryManager** | ✅ Stable | 流式查询管理器 |
| **Trigger** | ✅ Stable | 触发器策略类 |
| `Trigger.Once()` | ⛔ Deprecated(3.4.0) | 使用`Trigger.AvailableNow()`替代 |
| `Trigger.AvailableNow()` | ✅ Stable | 新一次性触发器 |
| `Trigger.ProcessingTime()` | ✅ Stable | 定时触发器 |
| `Trigger.Continuous()` | ⚠️ Evolving | 连续处理触发器 |
| **OutputMode** | ✅ Stable | 输出模式（Append/Complete/Update） |
| **GroupState<S>** | ⚠️ Evolving | 流式状态管理接口 |
| **GroupStateTimeout** | ✅ Stable | 状态超时定义 |
| **TimeMode** | 🆕 Evolving | transformWithState时间模式 |

---

## 1.5 Java特有工具类

| 类名 | 标注 | 说明 |
|------|------|------|
| **RowFactory** | ✅ Stable | 创建Row对象工厂 |
| **Optional<T>** | ✅ Stable | Java 8风格可选值 |
| **StorageLevels** | ✅ Stable | 存储级别常量 |
| **JavaFutureAction<T>** | ✅ Stable | Java风格Future |
| **JavaRDD<T>** | ✅ Stable | Java RDD包装类 |
| **JavaPairRDD<K,V>** | ✅ Stable | Java键值对RDD |

---

## 1.6 新增API（Spark 4.x特性）

| 类名 | 标注 | 说明 |
|------|------|------|
| **Geometry** | ❌ Unstable | 空间几何类型 |
| **Geography** | ❌ Unstable | 空间地理类型 |
| **Identifier** | ⚠️ Evolving | Catalog对象标识接口 |
| **IdentityColumnSpec** | ⚠️ Evolving | 身份列规范 |

---

# 二、Kafka Java公共API

## 官方文档参考

| 文档 | URL |
|------|-----|
| Kafka 4.2 Javadoc | https://kafka.apache.org/42/javadoc |
| Kafka文档 | https://kafka.apache.org/documentation |

---

## 2.1 Producer API ✅核心Stable

**包路径**: `org.apache.kafka.clients.producer`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Producer<K,V>** | ✅ Stable | 生产者接口 |
| **KafkaProducer<K,V>** | ✅ Stable | 生产者实现 |
| **ProducerRecord<K,V>** | ✅ Stable | 生产消息记录 |
| **RecordMetadata** | ✅ Stable | 消息元数据（topic/partition/offset/timestamp） |
| **Callback** | ✅ Stable | 异步回调接口 |
| **ProducerConfig** | ✅ Stable | 生产者配置类 |
| **Partitioner** | ✅ Stable | 分区器接口 |
| **RoundRobinPartitioner** | ✅ Stable | 轮询分区器 |
| **UniformStickyPartitioner** | ✅ Stable | 粘性分区器 |

**使用示例**:
```java
Properties props = new Properties();
props.put("bootstrap.servers", "localhost:9092");
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

Producer<String, String> producer = new KafkaProducer<>(props);
producer.send(new ProducerRecord<>("topic", "key", "value"), 
    (metadata, exception) -> {
        if (exception == null) {
            System.out.println("Sent to partition " + metadata.partition());
        }
    });
producer.close();
```

---

## 2.2 Consumer API ✅核心Stable

**包路径**: `org.apache.kafka.clients.consumer`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Consumer<K,V>** | ✅ Stable | 消费者接口 |
| **KafkaConsumer<K,V>** | ✅ Stable | 消费者实现 |
| **ConsumerRecord<K,V>** | ✅ Stable | 消费消息记录 |
| **ConsumerRecords<K,V>** | ✅ Stable | 消息记录集合 |
| **ConsumerConfig** | ✅ Stable | 消费者配置类 |
| **OffsetAndMetadata** | ✅ Stable | Offset+元数据 |
| **OffsetAndTimestamp** | ✅ Stable | Offset+时间戳 |
| **ConsumerGroupMetadata** | ✅ Stable | 消费者组元数据 |
| **ConsumerRebalanceListener** | ✅ Stable | 重平衡监听器 |
| **OffsetResetStrategy** | ✅ Stable | Offset重置策略（EARLIEST/LATEST/NONE） |
| **RangeAssignor** | ✅ Stable | 范围分区分配器 |
| **RoundRobinAssignor** | ✅ Stable | 轮询分配器 |
| **StickyAssignor** | ✅ Stable | 粘性分配器 |
| **CooperativeStickyAssignor** | ✅ Stable | 协作粘性分配器 |

---

## 2.3 Share Consumer API 🆕新特性

**包路径**: `org.apache.kafka.clients.consumer`

| 类名 | 标注 | 说明 |
|------|------|------|
| **ShareConsumer<K,V>** | 🆕 Evolving | Share消费者接口（KIP-932） |
| **KafkaShareConsumer<K,V>** | 🆕 Evolving | Share消费者实现 |
| **AcknowledgeType** | 🆕 Evolving | 确认类型（ACCEPT/REJECT/RELEASE） |

---

## 2.4 Admin API ✅核心Stable

**包路径**: `org.apache.kafka.clients.admin`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Admin** | ✅ Stable | 管理接口 |
| **AdminClientConfig** | ✅ Stable | Admin配置类 |
| **NewTopic** | ✅ Stable | 新主题定义 |
| **CreateTopicsResult** | ✅ Stable | 创建主题结果 |
| **DeleteTopicsResult** | ✅ Stable | 删除主题结果 |
| **ListTopicsResult** | ✅ Stable | 列出主题结果 |
| **DescribeTopicsResult** | ✅ Stable | 描述主题结果 |
| **TopicDescription** | ✅ Stable | 主题描述 |
| **NewPartitions** | ✅ Stable | 新分区定义 |
| **CreatePartitionsResult** | ✅ Stable | 创建分区结果 |
| **ConfigEntry** | ✅ Stable | 配置条目 |
| **AlterConfigOp** | ✅ Stable | 配置修改操作 |
| **CreateAclsResult** | ✅ Stable | 创建ACL结果 |
| **DeleteAclsResult** | ✅ Stable | 删除ACL结果 |
| **ListConsumerGroupsResult** | ✅ Stable | 列出消费者组结果 |
| **DescribeConsumerGroupsResult** | ✅ Stable | 描述消费者组结果 |
| **MemberDescription** | ✅ Stable | 消费者组成员描述 |
| **DeleteRecordsResult** | ✅ Stable | 删除记录结果 |

### 🆕 Admin新增API（Kafka 4.x）

| 类名 | 标注 | 说明 |
|------|------|------|
| **StreamsGroupDescription** | 🆕 Evolving | Streams Group描述（KIP-919） |
| **StreamsGroupMemberDescription** | 🆕 Evolving | Streams Group成员描述 |
| **ListStreamsGroupOffsetsResult** | 🆕 Evolving | Streams Group Offset操作 |
| **AddRaftVoterOptions/Result** | 🆕 Stable | Raft Voter添加（KIP-853） |
| **RemoveRaftVoterOptions/Result** | 🆕 Stable | Raft Voter移除 |

---

## 2.5 Common API

**包路径**: `org.apache.kafka.common`

| 类名 | 标注 | 说明 |
|------|------|------|
| **TopicPartition** | ✅ Stable | 主题分区标识 |
| **PartitionInfo** | ✅ Stable | 分区信息 |
| **Node** | ✅ Stable | Broker节点信息 |
| **Cluster** | ✅ Stable | 集群信息 |
| **Metric** | ✅ Stable | 指标接口 |
| **MetricName** | ✅ Stable | 指标名称 |
| **Uuid** | ✅ Stable | UUID类 |
| **Endpoint** | ✅ Stable | Broker端点 |

### 序列化接口

| 类名 | 标注 | 说明 |
|------|------|------|
| **Serializer<T>** | ✅ Stable | 序列化接口 |
| **Deserializer<T>** | ✅ Stable | 反序列化接口 |
| **Serde<T>** | ✅ Stable | 序列化/反序列化组合 |
| **Serdes** | ✅ Stable | 预定义Serde工厂 |

### 安全相关

| 类名 | 标注 | 说明 |
|------|------|------|
| **SecurityProtocol** | ✅ Stable | 安全协议（PLAINTEXT/SSL/SASL_PLAINTEXT/SASL_SSL） |
| **KafkaPrincipal** | ✅ Stable | Principal类 |

---

## 2.6 Connect API ✅全部Stable

**包路径**: `org.apache.kafka.connect`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Connector** | ✅ Stable | Connector基类 |
| **Task** | ✅ Stable | Task接口 |
| **SourceConnector** | ✅ Stable | Source Connector基类 |
| **SourceTask** | ✅ Stable | Source Task基类 |
| **SourceRecord** | ✅ Stable | Source记录 |
| **SinkConnector** | ✅ Stable | Sink Connector基类 |
| **SinkTask** | ✅ Stable | Sink Task基类 |
| **SinkRecord** | ✅ Stable | Sink记录 |
| **Schema** | ✅ Stable | Schema接口 |
| **SchemaBuilder** | ✅ Stable | Schema构建器 |
| **Struct** | ✅ Stable | 结构化数据 |
| **Converter** | ✅ Stable | 转换器接口 |
| **Transformation<R>** | ✅ Stable | 转换接口 |

---

## 2.7 Streams API

**包路径**: `org.apache.kafka.streams`

| 类名 | 标注 | 说明 |
|------|------|------|
| **KafkaStreams** | ✅ Stable | Streams客户端核心类 |
| **StreamsBuilder** | ✅ Stable | 流构建器（DSL入口） |
| **Topology** | ✅ Stable | 拓扑定义 |
| **StreamsConfig** | ✅ Stable | Streams配置类 |
| **KeyValue<K,V>** | ✅ Stable | Key-Value对 |
| **StoreQueryParameters<T>** | ✅ Stable | 状态存储查询参数 |

### KStream DSL

**包路径**: `org.apache.kafka.streams.kstream`

| 类名 | 标注 | 说明 |
|------|------|------|
| **KStream<K,V>** | ✅ Stable | 流接口 |
| **KTable<K,V>** | ✅ Stable | 表接口 |
| **GlobalKTable<K,V>** | ✅ Stable | 全局表接口 |
| **KGroupedStream<K,V>** | ✅ Stable | 分组流 |
| **TimeWindows** | ✅ Stable | 时间窗口 |
| **SessionWindows** | ✅ Stable | Session窗口 |
| **JoinWindows** | ✅ Stable | 连接窗口 |
| **Materialized<K,V,S>** | ✅ Stable | 状态存储配置 |
| **Produced<K,V>** | ✅ Stable | 生产配置 |
| **Consumed<K,V>** | ✅ Stable | 消费配置 |
| **Grouped<K,V>** | ✅ Stable | 分组配置 |
| **StreamJoined<K,V,V2>** | ✅ Stable | 连接配置 |

### 状态存储

**包路径**: `org.apache.kafka.streams.state`

| 类名 | 标注 | 说明 |
|------|------|------|
| **KeyValueStore<K,V>** | ✅ Stable | KV存储接口 |
| **WindowStore<K,V>** | ✅ Stable | 窗口存储接口 |
| **SessionStore<K,V>** | ✅ Stable | Session存储接口 |
| **ReadOnlyKeyValueStore<K,V>** | ✅ Stable | 只读KV存储 |
| **Stores** | ✅ Stable | 存储工厂类 |

---

## ⛔ 不应使用的内部包

| 包名 | 类数量 | 说明 |
|------|--------|------|
| `org.apache.kafka.clients.*.internals` | 381个 | **内部实现，不应公开使用** |
| `org.apache.kafka.streams.kstream.internals` | 160个 | **DSL内部实现** |

---

# 三、Iceberg Java公共API

## 官方文档参考

| 文档 | URL |
|------|-----|
| Iceberg Javadoc | https://iceberg.apache.org/javadoc/latest/ |
| Iceberg API文档 | https://iceberg.apache.org/docs/latest/api/ |

---

## 3.1 Table API ✅核心Stable

**包路径**: `org.apache.iceberg`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **Table** | ✅ Stable | 表核心接口，所有操作入口 |
| **Snapshot** | ✅ Stable | 快照定义 |
| **Transaction** | ✅ Stable | 事务接口 |
| **Schema** | ✅ Stable | 表Schema定义 |
| **PartitionSpec** | ✅ Stable | 分区规格定义 |
| **SortOrder** | ✅ Stable | 排序规格定义 |
| **TableScan** | ✅ Stable | 表扫描接口 |
| **BatchScan** | ✅ Stable | 批量扫描接口 |
| **FileScanTask** | ✅ Stable | 文件扫描任务 |
| **CombinedScanTask** | ✅ Stable | 组合扫描任务 |

### 数据操作接口

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **AppendFiles** | ✅ Stable | 追加数据文件 |
| **DeleteFiles** | ✅ Stable | 删除数据文件 |
| **OverwriteFiles** | ✅ Stable | 覆盖数据文件 |
| **RewriteFiles** | ✅ Stable | 重写数据文件 |
| **ReplacePartitions** | ✅ Stable | 替换分区 |
| **RowDelta** | ✅ Stable | 行级变更 |

### 元数据更新接口

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **UpdateSchema** | ✅ Stable | 更新Schema |
| **UpdatePartitionSpec** | ✅ Stable | 更新分区规格 |
| **UpdateProperties** | ✅ Stable | 更新表属性 |
| **UpdateStatistics** | ✅ Stable | 更新统计信息 |
| **ExpireSnapshots** | ✅ Stable | 过期快照清理 |

---

## 3.2 Catalog API ✅核心Stable

**包路径**: `org.apache.iceberg.catalog`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **Catalog** | ✅ Stable | Catalog核心接口 |
| **SessionCatalog** | ✅ Stable | Session Catalog |
| **SupportsNamespaces** | ✅ Stable | Namespace支持接口 |
| **TableIdentifier** | ✅ Stable | 表标识符 |
| **Namespace** | ✅ Stable | Namespace定义 |

---

## 3.3 Types API ✅全部Stable

**包路径**: `org.apache.iceberg.types`

| 类型 | 标注 | 说明 |
|------|------|------|
| **BooleanType** | ✅ Stable | Boolean类型 |
| **IntegerType** | ✅ Stable | Integer类型（32位） |
| **LongType** | ✅ Stable | Long类型（64位） |
| **FloatType** | ✅ Stable | Float类型 |
| **DoubleType** | ✅ Stable | Double类型 |
| **StringType** | ✅ Stable | String类型 |
| **DateType** | ✅ Stable | Date类型 |
| **TimeType** | ✅ Stable | Time类型 |
| **TimestampType** | ✅ Stable | Timestamp类型 |
| **TimestampType.withZone()** | ✅ Stable | 带时区Timestamp |
| **TimestampType.withoutZone()** | ✅ Stable | 不带时区Timestamp |
| **BinaryType** | ✅ Stable | Binary类型 |
| **DecimalType** | ✅ Stable | Decimal类型 |
| **FixedType** | ✅ Stable | Fixed类型 |
| **UUIDType** | ✅ Stable | UUID类型 |
| **ListType** | ✅ Stable | List类型 |
| **MapType** | ✅ Stable | Map类型 |
| **StructType** | ✅ Stable | Struct类型 |

---

## 3.4 Expressions API ✅核心Stable

**包路径**: `org.apache.iceberg.expressions`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Expressions** | ✅ Stable | 表达式工厂类 |
| **Expression** | ✅ Stable | 表达式接口 |
| **Literal<T>** | ✅ Stable | Literal接口 |

### 表达式操作方法

| 方法 | 标注 | 说明 |
|------|------|------|
| `Expressions.equal("col", value)` | ✅ Stable | 等于 |
| `Expressions.notEqual("col", value)` | ✅ Stable | 不等于 |
| `Expressions.lessThan("col", value)` | ✅ Stable | 小于 |
| `Expressions.greaterThan("col", value)` | ✅ Stable | 大于 |
| `Expressions.isNull("col")` | ✅ Stable | 为空 |
| `Expressions.notNull("col")` | ✅ Stable | 不为空 |
| `Expressions.in("col", values)` | ✅ Stable | 包含 |
| `Expressions.startsWith("col", prefix)` | ✅ Stable | 以...开始 |
| `Expressions.and(expr1, expr2)` | ✅ Stable | And组合 |
| `Expressions.or(expr1, expr2)` | ✅ Stable | Or组合 |
| `Expressions.not(expr)` | ✅ Stable | Not否定 |

---

## 3.5 IO API ✅核心Stable

**包路径**: `org.apache.iceberg.io`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **FileIO** | ✅ Stable | 文件IO接口 |
| **InputFile** | ✅ Stable | 输入文件接口 |
| **OutputFile** | ✅ Stable | 输出文件接口 |
| **LocationProvider** | ✅ Stable | 位置提供者 |
| **CloseableIterable<T>** | ✅ Stable | 可关闭迭代器 |

---

## 3.6 Actions API ✅核心Stable

**包路径**: `org.apache.iceberg.actions`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **ActionsProvider** | ✅ Stable | Actions提供者 |
| **RewriteDataFiles** | ✅ Stable | 重写数据文件 |
| **RewriteManifests** | ✅ Stable | 重写Manifests |
| **ExpireSnapshots** | ✅ Stable | 过期快照 |
| **DeleteOrphanFiles** | ✅ Stable | 删除孤儿文件 |
| **MigrateTable** | ✅ Stable | 迁移表 |
| **SnapshotTable** | ✅ Stable | 快照表 |

### 🆕 新增Actions

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **ComputePartitionStats** | 🆕 Evolving | 计算分区统计 |
| **ConvertEqualityDeleteFiles** | 🆕 Evolving | 等式删除转换 |
| **RemoveDanglingDeleteFiles** | 🆕 Evolving | 悬空删除清理 |

---

## 3.7 View API 🆕新特性

**包路径**: `org.apache.iceberg.view`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **View** | 🆕 Stable | View接口 |
| **ViewBuilder** | 🆕 Stable | View构建器 |
| **ViewVersion** | 🆕 Stable | View版本定义 |
| **UpdateViewProperties** | 🆕 Stable | 更新View属性 |

---

## 3.8 Variant API 🆕新特性

**包路径**: `org.apache.iceberg.variants`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **Variant** | 🆕 Stable | Variant接口 |
| **VariantArray** | 🆕 Stable | Variant数组 |
| **VariantObject** | 🆕 Stable | Variant对象 |
| **VariantPrimitive** | 🆕 Stable | Variant原始类型 |

---

## 3.9 Spark集成API

**包路径**: `org.apache.iceberg.spark`

| 类名 | 标注 | 说明 |
|------|------|------|
| **SparkCatalog** | ✅ Stable | Spark Catalog实现 |
| **SparkSessionCatalog** | ✅ Stable | Spark Session Catalog |
| **SparkTableUtil** | ✅ Stable | Spark表工具 |
| **SparkSchemaUtil** | ✅ Stable | Schema转换工具 |
| **SparkActions** | ✅ Stable | Spark Actions入口 |

---

## ⛔ 不应直接使用的实现类

| 包名 | 类名 | 说明 |
|------|------|------|
| `org.apache.iceberg` | BaseTable | Table实现类，用户应使用Table接口 |
| `org.apache.iceberg` | BaseTransaction | Transaction实现类 |
| `org.apache.iceberg` | CatalogUtil | Catalog工具类 |
| `org.apache.iceberg.core` | 所有实现类 | **core模块是内部实现** |

---

# 四、HBase Java公共API

## 官方文档参考

| 文档 | URL |
|------|-----|
| HBase 4.0 Javadoc | https://hbase.apache.org/apidocs/index.html |
| HBase Reference Guide | https://hbase.apache.org/book.html |

---

## 4.1 Connection API ✅核心Stable

**包路径**: `org.apache.hadoop.hbase.client`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **Connection** | ✅ Stable | 集群连接接口 |
| **AsyncConnection** | ✅ Stable | 异步连接接口 |
| **ConnectionFactory** | ✅ Stable | 连接工厂类 |

**使用示例**:
```java
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "localhost");
Connection connection = ConnectionFactory.createConnection(config);
```

---

## 4.2 Table Operations ✅核心Stable

**包路径**: `org.apache.hadoop.hbase.client`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Table** | ✅ Stable | 表操作接口 |
| **AsyncTable** | ✅ Stable | 异步表接口 |
| **Get** | ✅ Stable | 查询操作 |
| **Put** | ✅ Stable | 写入操作 |
| **Delete** | ✅ Stable | 删除操作 |
| **Scan** | ✅ Stable | 扫描操作 |
| **Result** | ✅ Stable | 结果对象 |
| **ResultScanner** | ✅ Stable | 结果扫描器接口 |
| **Increment** | ✅ Stable | 增量操作 |
| **Append** | ✅ Stable | 追加操作 |
| **RowMutations** | ✅ Stable | 行级多操作 |
| **CheckAndMutate** | ✅ Stable | 条件操作（替代checkAndMutate方法） |
| **BufferedMutator** | ✅ Stable | 批量写入接口 |

---

## 4.3 Admin Operations ✅核心Stable

**包路径**: `org.apache.hadoop.hbase.client`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **Admin** | ✅ Stable | 管理接口 |
| **AsyncAdmin** | ✅ Stable | 异步管理接口 |

### 表管理方法

| 方法 | 标注 | 说明 |
|------|------|------|
| `createTable(TableDescriptor)` | ✅ Stable | 创建表 |
| `deleteTable(TableName)` | ✅ Stable | 删除表 |
| `disableTable(TableName)` | ✅ Stable | 禁用表 |
| `enableTable(TableName)` | ✅ Stable | 启用表 |
| `modifyTable(TableDescriptor)` | ✅ Stable | 修改表 |
| `truncateTable(TableName, boolean)` | ✅ Stable | 清空表 |
| `listTables()` | ✅ Stable | 列出表 |

---

## 4.4 Table Metadata ✅核心Stable

| 类名 | 标注 | 说明 |
|------|------|------|
| **TableDescriptor** | ✅ Stable | 表描述符接口 |
| **TableDescriptorBuilder** | ✅ Stable | 表描述符构建器 |
| **ColumnFamilyDescriptor** | ✅ Stable | 列族描述符接口 |
| **ColumnFamilyDescriptorBuilder** | ✅ Stable | 列族描述符构建器 |
| **TableName** | ✅ Stable | 表名类 |
| **RegionInfo** | ✅ Stable | Region信息接口 |
| **NamespaceDescriptor** | ✅ Stable | Namespace描述符 |

---

## 4.5 Filter API ✅全部Stable

**包路径**: `org.apache.hadoop.hbase.filter`

### 基础过滤器

| 类名 | 标注 | 说明 |
|------|------|------|
| **Filter** | ✅ Stable | 过滤器基接口 |
| **FilterList** | ✅ Stable | 过滤器组合（AND/OR） |

### 行过滤器

| 类名 | 标注 | 说明 |
|------|------|------|
| **RowFilter** | ✅ Stable | 行过滤器 |
| **PrefixFilter** | ✅ Stable | 前缀过滤器 |
| **PageFilter** | ✅ Stable | 分页过滤器（限制行数） |
| **FirstKeyOnlyFilter** | ✅ Stable | 仅首键过滤器 |
| **KeyOnlyFilter** | ✅ Stable | 仅键过滤器 |
| **InclusiveStopFilter** | ✅ Stable | 包含停止过滤器 |
| **RandomRowFilter** | ✅ Stable | 随机行过滤器 |
| **MultiRowRangeFilter** | ✅ Stable | 多行范围过滤器 |
| **FuzzyRowFilter** | ✅ Stable | 模糊行过滤器 |

### 列过滤器

| 类名 | 标注 | 说明 |
|------|------|------|
| **QualifierFilter** | ✅ Stable | 列名过滤器 |
| **FamilyFilter** | ✅ Stable | 列族过滤器 |
| **ColumnPrefixFilter** | ✅ Stable | 列前缀过滤器 |
| **MultipleColumnPrefixFilter** | ✅ Stable | 多列前缀过滤器 |
| **ColumnRangeFilter** | ✅ Stable | 列范围过滤器 |
| **ColumnPaginationFilter** | ✅ Stable | 列分页过滤器 |
| **ColumnCountGetFilter** | ✅ Stable | 列计数过滤器 |

### 值过滤器

| 类名 | 标注 | 说明 |
|------|------|------|
| **ValueFilter** | ✅ Stable | 值过滤器 |
| **SingleColumnValueFilter** | ✅ Stable | 单列值过滤器 |
| **SingleColumnValueExcludeFilter** | ✅ Stable | 单列值排除过滤器 |
| **ColumnValueFilter** | ✅ Stable | 列值过滤器 |
| **DependentColumnFilter** | ✅ Stable | 依赖列过滤器 |

### 时间过滤器

| 类名 | 标注 | 说明 |
|------|------|------|
| **TimestampsFilter** | ✅ Stable | 时间戳过滤器 |
| **TimeRangeFilter** | ✅ Stable | 时间范围过滤器 |

### 特殊过滤器

| 类名 | 标注 | 说明 |
|------|------|------|
| **SkipFilter** | ✅ Stable | 跳过过滤器 |
| **WhileMatchFilter** | ✅ Stable | 匹配终止过滤器 |

### 比较器

| 类名 | 标注 | 说明 |
|------|------|------|
| **BinaryComparator** | ✅ Stable | 二进制比较器 |
| **BinaryPrefixComparator** | ✅ Stable | 二进制前缀比较器 |
| **RegexStringComparator** | ✅ Stable | 正则比较器 |
| **SubstringComparator** | ✅ Stable | 子串比较器 |
| **LongComparator** | ✅ Stable | Long比较器 |
| **DoubleComparator** | ✅ Stable | Double比较器 |
| **BigDecimalComparator** | ✅ Stable | BigDecimal比较器 |
| **NullComparator** | ✅ Stable | 空值比较器 |
| **BitComparator** | ✅ Stable | 位比较器 |

**使用示例**:
```java
Scan scan = new Scan();
SingleColumnValueFilter filter = new SingleColumnValueFilter(
    Bytes.toBytes("cf"),
    Bytes.toBytes("col"),
    CompareOperator.EQUAL,
    new BinaryComparator(Bytes.toBytes("value"))
);
scan.setFilter(filter);
```

---

## 4.6 Cell API ✅核心Stable

**包路径**: `org.apache.hadoop.hbase`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **Cell** | ✅ Stable | 单元格接口（核心数据单元） |
| **CellBuilder** | ✅ Stable | Cell构建器接口 |
| **CellComparator** | ✅ Stable | Cell比较器接口 |

| 类名 | 标注 | 说明 |
|------|------|------|
| **CellUtil** | ✅ Stable | Cell工具类 |
| **CellBuilderFactory** | ✅ Stable | Cell构建器工厂 |
| **KeyValue** | ✅ Stable | 键值对类 |

---

## 4.7 Common API ✅核心Stable

**包路径**: `org.apache.hadoop.hbase`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Bytes** | ✅ Stable | 字节数组工具类 |
| **HConstants** | ✅ Stable | 常量定义 |
| **HBaseConfiguration** | ✅ Stable | 配置类 |
| **ServerName** | ✅ Stable | 服务器名类 |
| **HBaseIOException** | ✅ Stable | IO异常基类 |
| **TimeRange** | ✅ Stable | 时间范围类 |
| **User** | ⚠️ LimitedPrivate | 用户类（Coprocessor可用） |

---

## 4.8 MapReduce API ✅核心Stable

**包路径**: `org.apache.hadoop.hbase.mapreduce`

| 类名 | 标注 | 说明 |
|------|------|------|
| **TableInputFormat** | ✅ Stable | 表输入格式 |
| **TableOutputFormat** | ✅ Stable | 表输出格式 |
| **TableSnapshotInputFormat** | ✅ Stable | 表快照输入格式 |
| **MultiTableInputFormat** | ✅ Stable | 多表输入格式 |
| **TableMapper<K,V>** | ✅ Stable | 表Mapper基类 |
| **TableReducer<K,V,KEY>** | ✅ Stable | 表Reducer基类 |
| **TableMapReduceUtil** | ✅ Stable | MapReduce工具类 |
| **Import** | ✅ Stable | 导入工具 |
| **Export** | ✅ Stable | 导出工具 |
| **CopyTable** | ✅ Stable | 复制表工具 |
| **BulkLoadHFiles** | ✅ Stable | 批量加载HFiles |

---

## 4.9 🆕 新增API

| 类名 | 标注 | 说明 |
|------|------|------|
| **ConnectionRegistry** | 🆕 Stable | 连接注册器接口（替代ZK） |
| **RpcConnectionRegistry** | 🆕 Stable | RPC连接注册器实现 |
| **TestingHBaseCluster** | 🆕 Stable | 新测试集群框架 |
| **QueryMetrics** | 🆕 Evolving | 查询指标类 |

---

## ⛔ Deprecated API（应迁移）

| 类名 | 废弃版本 | 替代方案 | 紧迫程度 |
|------|----------|----------|----------|
| **HBaseTestingUtility** | 3.0.0 | TestingHBaseCluster | HBase 4.0移除 |
| **MiniHBaseCluster** | 3.0.0 | TestingHBaseCluster | HBase 4.0移除 |
| **Table.checkAndMutate方法** | 4.0.0 | CheckAndMutate类 | 已移除 |
| **CoprocessorRpcChannel** | 4.0.0 | 不支持低级别RPC | 已移除 |
| **MasterRegistry** | 2.5.0 | RpcConnectionRegistry | 使用新Registry |
| **QuotaRetriever** | 3.0.0 | Admin.getQuota() | 已废弃 |

---

# 五、Hadoop Java公共API

## 官方文档参考

| 文档 | URL |
|------|-----|
| Hadoop文档 | https://hadoop.apache.org/docs/stable/ |
| MapReduce教程 | https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html |

---

## 5.1 Configuration API ✅核心Stable

**包路径**: `org.apache.hadoop.conf`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Configuration** | ✅ Stable | Hadoop配置核心类 |
| **Configured** | ✅ Stable | 可配置基类 |
| **Configurable** | ✅ Stable | 可配置接口 |

---

## 5.2 FileSystem API ✅核心Stable

**包路径**: `org.apache.hadoop.fs`

| 类名 | 标注 | 说明 |
|------|------|------|
| **FileSystem** | ✅ Stable | 文件系统抽象基类 |
| **Path** | ✅ Stable | 路径表示类 |
| **FSDataInputStream** | ✅ Stable | 文件输入流 |
| **FSDataOutputStream** | ✅ Stable | 文件输出流 |
| **FileStatus** | ✅ Stable | 文件状态信息 |
| **BlockLocation** | ✅ Stable | 块位置信息 |
| **ContentSummary** | ✅ Stable | 内容摘要 |
| **FileChecksum** | ✅ Stable | 文件校验和 |
| **LocalFileSystem** | ✅ Stable | 本地文件系统 |

### 文件操作方法

| 方法 | 标注 | 说明 |
|------|------|------|
| `open(Path)` | ✅ Stable | 打开文件读取 |
| `create(Path)` | ✅ Stable | 创建文件写入 |
| `append(Path)` | ✅ Stable | 追加文件 |
| `delete(Path, boolean)` | ✅ Stable | 删除文件/目录 |
| `rename(Path, Path)` | ✅ Stable | 重命名 |
| `listStatus(Path)` | ✅ Stable | 列出文件状态 |
| `mkdirs(Path)` | ✅ Stable | 创建目录 |
| `exists(Path)` | ✅ Stable | 检查存在 |
| `getFileStatus(Path)` | ✅ Stable | 获取文件状态 |

---

## 5.3 IO API ✅全部Stable

**包路径**: `org.apache.hadoop.io`

### Writable接口

| 类名 | 标注 | 说明 |
|------|------|------|
| **Writable** | ✅ Stable | 序列化接口 |
| **WritableComparable<T>** | ✅ Stable | 可比较Writable |

### 基本类型Writable

| 类名 | 标注 | 说明 |
|------|------|------|
| **Text** | ✅ Stable | UTF8文本类 |
| **IntWritable** | ✅ Stable | Integer Writable |
| **LongWritable** | ✅ Stable | Long Writable |
| **FloatWritable** | ✅ Stable | Float Writable |
| **DoubleWritable** | ✅ Stable | Double Writable |
| **BooleanWritable** | ✅ Stable | Boolean Writable |
| **BytesWritable** | ✅ Stable | Bytes Writable |
| **NullWritable** | ✅ Stable | Null Writable |
| **VIntWritable** | ✅ Stable | Variable Int |
| **VLongWritable** | ✅ Stable | Variable Long |

### 文件格式

| 类名 | 标注 | 说明 |
|------|------|------|
| **SequenceFile** | ✅ Stable | SequenceFile格式 |
| **SequenceFile.Reader** | ✅ Stable | SequenceFile读取器 |
| **SequenceFile.Writer** | ✅ Stable | SequenceFile写入器 |
| **MapFile** | ✅ Stable | MapFile格式 |
| **MapFile.Reader** | ✅ Stable | MapFile读取器 |
| **MapFile.Writer** | ✅ Stable | MapFile写入器 |

---

## 5.4 MapReduce API ✅核心Stable

**包路径**: `org.apache.hadoop.mapreduce`

| 类名 | 标注 | 说明 |
|------|------|------|
| **Job** | ⚠️ Evolving | Job类（核心但标记Evolving） |
| **Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>** | ✅ Stable | Mapper基类 |
| **Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>** | ✅ Stable | Reducer基类 |
| **InputFormat<K,V>** | ✅ Stable | 输入格式基类 |
| **OutputFormat<K,V>** | ✅ Stable | 输出格式基类 |
| **InputSplit** | ✅ Stable | 输入分片接口 |
| **RecordReader<K,V>** | ✅ Stable | 记录读取器 |
| **RecordWriter<K,V>** | ✅ Stable | 记录写入器 |
| **Partitioner<K,V>** | ✅ Stable | 分区器 |
| **OutputCommitter** | ✅ Stable | Output提交器 |
| **JobContext** | ✅ Stable | Job上下文 |
| **TaskAttemptContext** | ✅ Stable | Task上下文 |
| **Counters** | ✅ Stable | 计数器 |
| **Counter** | ✅ Stable | 计数器接口 |
| **JobID** | ✅ Stable | Job ID |
| **TaskID** | ✅ Stable | Task ID |
| **TaskAttemptID** | ✅ Stable | Task尝试ID |

### InputFormats

**包路径**: `org.apache.hadoop.mapreduce.lib.input`

| 类名 | 标注 | 说明 |
|------|------|------|
| **FileInputFormat<K,V>** | ✅ Stable | 文件输入格式基类 |
| **TextInputFormat** | ✅ Stable | 文本输入格式 |
| **SequenceFileInputFormat<K,V>** | ✅ Stable | SequenceFile输入格式 |
| **KeyValueTextInputFormat** | ✅ Stable | KeyValue文本输入格式 |
| **NLineInputFormat** | ✅ Stable | N行输入格式 |
| **CombineFileInputFormat<K,V>** | ✅ Stable | 组合文件输入格式 |

### OutputFormats

**包路径**: `org.apache.hadoop.mapreduce.lib.output`

| 类名 | 标注 | 说明 |
|------|------|------|
| **FileOutputFormat<K,V>** | ✅ Stable | 文件输出格式基类 |
| **TextOutputFormat<K,V>** | ✅ Stable | 文本输出格式 |
| **SequenceFileOutputFormat<K,V>** | ✅ Stable | SequenceFile输出格式 |
| **MapFileOutputFormat** | ✅ Stable | MapFile输出格式 |
| **NullOutputFormat<K,V>** | ✅ Stable | Null输出格式 |

### Partitioners

**包路径**: `org.apache.hadoop.mapreduce.lib.partition`

| 类名 | 标注 | 说明 |
|------|------|------|
| **HashPartitioner<K,V>** | ✅ Stable | Hash分区器 |
| **TotalOrderPartitioner<K,V>** | ✅ Stable | 全序分区器 |

---

## 5.5 YARN API

### 协议接口 ✅Stable

**包路径**: `org.apache.hadoop.yarn.api`

| 接口名 | 标注 | 说明 |
|--------|------|------|
| **ApplicationClientProtocol** | ✅ Stable | Application客户端协议 |
| **ApplicationMasterProtocol** | ✅ Stable | AM协议 |
| **ContainerManagementProtocol** | ✅ Stable | 容器管理协议 |

### Record类 ⚠️大部分Unstable

**包路径**: `org.apache.hadoop.yarn.api.records`

| 类名 | 标注 | 说明 |
|------|------|------|
| **ApplicationId** | ✅ Stable | Application ID |
| **ApplicationAttemptId** | ✅ Stable | Application尝试ID |
| **ContainerId** | ✅ Stable | Container ID |
| **NodeId** | ✅ Stable | Node ID |
| **Resource** | ⚠️ Unstable | Resource类 |
| **ResourceRequest** | ⚠️ Unstable | Resource请求 |
| **Container** | ⚠️ Unstable | Container接口 |
| **ContainerLaunchContext** | ⚠️ Unstable | Container启动上下文 |
| **ApplicationSubmissionContext** | ⚠️ Unstable | Application提交上下文 |
| **ApplicationReport** | ⚠️ Unstable | Application报告 |
| **Priority** | ⚠️ Unstable | Priority类 |
| **LocalResource** | ⚠️ Unstable | 本地资源 |

### YARN客户端 ✅Stable

**包路径**: `org.apache.hadoop.yarn.client.api`

| 类名 | 标注 | 说明 |
|------|------|------|
| **YarnClient** | ✅ Stable | YARN客户端 |
| **AMRMClient** | ✅ Stable | AM-RM客户端 |
| **NMClient** | ✅ Stable | NM客户端 |
| **AMRMClientAsync** | ✅ Stable | 异步AM-RM客户端 |
| **NMClientAsync** | ✅ Stable | 异步NM客户端 |

---

## ⚠️ 重要警告：DistributedFileSystem

| 项目 | 发现 |
|------|------|
| **官方文档** | 当作主要HDFS API广泛介绍 |
| **代码仓标注** | @LimitedPrivate(MapReduce, HBase) + @Unstable |
| **实际含义** | 仅对MapReduce和HBase项目有稳定性保证 |
| **建议** | 普通用户应使用FileSystem抽象类，避免依赖DistributedFileSystem特有方法 |

---

## ⛔ 不应使用的Private API

| 包名 | 说明 |
|------|------|
| `org.apache.hadoop.fs.CommonConfigurationKeys` | 使用CommonConfigurationKeysPublic替代 |
| `org.apache.hadoop.hdfs.server.*` | NameNode/DataNode服务器内部实现 |
| `org.apache.hadoop.yarn.server.*` | ResourceManager/NodeManager内部实现 |

---

# 六、使用建议汇总

## 优先使用✅Stable API

| 项目 | 核心Stable API |
|------|----------------|
| **Spark** | Dataset.map/filter/flatMap、UDF0-22、Java函数接口24个 |
| **Kafka** | Producer、Consumer、Admin、Connect、Streams核心类 |
| **Iceberg** | Table、Catalog、Schema、Expressions、IO接口 |
| **HBase** | Connection、Table、Get/Put/Delete/Scan、Filter 46个 |
| **Hadoop** | FileSystem、Configuration、Writable系列、Mapper/Reducer |

---

## 关注⚠️Evolving API变化

| 项目 | Evolving API | 说明 |
|------|--------------|------|
| **Spark** | FlatMapGroupsWithStateFunction、MapGroupsWithStateFunction | 流式状态处理 |
| **Spark** | TimeMode、Identifier | 新特性 |
| **Kafka** | StreamsGroup系列 | Streams Group管理（KIP-919） |
| **Kafka** | ShareConsumer | Share消费（KIP-932） |
| **Hadoop** | Job类 | MapReduce Job可能演进 |

---

## 避免⛔Deprecated API

| 项目 | 废弃API | 替代方案 |
|------|---------|----------|
| **Spark** | Trigger.Once() | Trigger.AvailableNow() |
| **HBase** | HBaseTestingUtility | TestingHBaseCluster |
| **HBase** | Table.checkAndMutate方法 | CheckAndMutate类 |
| **HBase** | CoprocessorRpcChannel | 不支持 |
| **Hadoop** | CommonConfigurationKeys | CommonConfigurationKeysPublic |

---

## 禁止使用⛔Internal/Private API

| 项目 | 内部包 | 类数量 |
|------|--------|--------|
| **Kafka** | clients.*.internals | 381个 |
| **Kafka** | streams.kstream.internals | 160个 |
| **Iceberg** | core模块实现类 | ~60个 |
| **HBase** | *Impl实现类 | ~30个 |
| **Hadoop** | server包 | 全部 |

---

# 七、快速参考表

## API数量统计

| 项目 | Stable | Evolving | Deprecated | 内部Private |
|------|--------|----------|------------|-------------|
| **Spark** | 46 | 5 | 2 | internal包 |
| **Kafka** | ~400 | ~15 | 少量 | 541个internals |
| **Iceberg** | 139接口 | View/Variant | 无 | core实现类 |
| **HBase** | 核心类 | ConnectionRegistry | 16个 | ~30个Impl |
| **Hadoop** | MR核心 | Job/HDFS | DistributedFileSystem争议 | server全部 |

---

## 文档版本参考

| 项目 | 推荐文档版本 |
|------|--------------|
| **Spark** | https://spark.apache.org/docs/3.5.6/api/java/ |
| **Kafka** | https://kafka.apache.org/42/javadoc |
| **Iceberg** | https://iceberg.apache.org/javadoc/latest/ |
| **HBase** | https://hbase.apache.org/apidocs/index.html |
| **Hadoop** | https://hadoop.apache.org/docs/stable/ |