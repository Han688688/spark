# Apache大数据组件Java API对比汇总

> 代码仓API vs 官方文档API 差异对比

---

## 对比概览

| 项目 | 代码仓公共API | 官方Javadoc公共API | 主要差异 |
|------|---------------|-------------------|----------|
| **Spark** | 69个 | ~65个 | 新增TimeMode、Geometry/Geography空间类型 |
| **Kafka** | 577个(不含internals) | 577个 | internals包381个内部类未公开 |
| **Iceberg** | 139个(api模块) | 200+ | api模块仅接口，core模块含实现类 |
| **HBase** | 343个 | 400+ | deprecated类16个将移除，新增ConnectionRegistry |
| **Hadoop** | 902个@Public | ~800+ | DistributedFileSystem实际是LimitedPrivate |

---

# 一、Spark Java API对比

## 代码仓Java公共API数量：69个

### Java函数接口（24个）

| 接口名 | 模块 | 状态 | 说明 |
|--------|------|------|------|
| Function | common/utils-java | Stable ✓ | 基础函数 |
| MapFunction | common/utils-java | Stable ✓ | map函数 |
| FilterFunction | common/utils-java | Stable ✓ | filter函数 |
| FlatMapFunction | common/utils-java | Stable ✓ | flatMap函数 |
| MapPartitionsFunction | common/utils-java | Stable ✓ | 分区map函数 |
| PairFunction | common/utils-java | Stable ✓ | 键值对函数 |
| PairFlatMapFunction | common/utils-java | Stable ✓ | 键值对flatMap函数 |
| ReduceFunction | common/utils-java | Stable ✓ | reduce函数 |
| ForeachFunction | common/utils-java | Stable ✓ | foreach函数 |
| ForeachPartitionFunction | common/utils-java | Stable ✓ | 分区foreach函数 |
| FlatMapGroupsFunction | common/utils-java | Stable ✓ | 分组flatMap函数 |
| MapGroupsFunction | common/utils-java | Stable ✓ | 分组map函数 |
| **FlatMapGroupsWithStateFunction** | **sql/api** | **Evolving** | **带状态分组flatMap（流式专用）** |
| **MapGroupsWithStateFunction** | **sql/api** | **Evolving** | **带状态分组map（流式专用）** |
| DoubleFunction | common/utils-java | Stable ✓ | Double返回函数 |
| DoubleFlatMapFunction | common/utils-java | Stable ✓ | Double返回flatMap |
| Function0 | common/utils-java | Stable ✓ | 无参函数 |
| Function2-4 | common/utils-java | Stable ✓ | 多参数函数 |
| VoidFunction | common/utils-java | Stable ✓ | 无返回值函数 |
| VoidFunction2 | common/utils-java | Stable ✓ | 双参数无返回值 |
| CoGroupFunction | common/utils-java | Stable ✓ | 双Dataset分组合并 |
| FlatMapFunction2 | common/utils-java | Stable ✓ | 双输入flatMap |

### UDF接口（23个）

| 接口名 | 状态 | 说明 |
|--------|------|------|
| UDF0 | Stable ✓ | 无参数UDF |
| UDF1-UDF22 | Stable ✓ | 1-22参数UDF |

### 新增API（代码仓有，官方文档未明确列出）

| 包名 | 类名 | 标注 | 说明 |
|------|------|------|------|
| org.apache.spark.sql.streaming | **TimeMode** | @Evolving | transformWithState时间模式定义 |
| org.apache.spark.sql.types | **Geometry** | @Unstable | 空间几何类型客户端类 |
| org.apache.spark.sql.types | **Geography** | @Unstable | 空间地理类型客户端类 |
| org.apache.spark.sql.connector.catalog | **Identifier** | @Evolving | Catalog对象标识接口 |
| org.apache.spark.sql.connector.catalog | **IdentityColumnSpec** | @Evolving | 身份列规范类 |

### Deprecated API（官方废弃，代码仓仍保留）

| 包名 | 类名 | 废弃版本 | 替代方案 |
|------|------|----------|----------|
| org.apache.spark.sql.expressions.javalang | typed类 | 3.0.0 | 使用非类型化内置聚合函数 |
| org.apache.spark.sql.streaming | Trigger.Once() | 3.4.0 | 使用Trigger.AvailableNow() |

### Java特有API（Scala中没有）

| 类名 | 说明 |
|------|------|
| RowFactory | 创建Row对象的工厂类 |
| Optional | Java 8风格可选值包装（Scala用Option） |
| StorageLevels | 存储级别常量类（Java友好） |
| JavaFutureAction | Java风格Future动作接口 |

---

# 二、Kafka API对比

## 代码仓公共类数量统计

### Clients模块（不含internals）

| 包名 | 公共类 | internals内部类 |
|------|--------|-----------------|
| org.apache.kafka.clients.admin | 177 | 36 |
| org.apache.kafka.clients.consumer | 33 | 167 |
| org.apache.kafka.clients.producer | 12 | 18 |
| **总计** | **222** | **221** |

### Connect模块

| 包名 | 公共类 |
|------|--------|
| org.apache.kafka.connect.connector | 4 |
| org.apache.kafka.connect.sink | 6 |
| org.apache.kafka.connect.source | 8 |
| org.apache.kafka.connect.data | 12 |
| org.apache.kafka.connect.storage | 8 |
| org.apache.kafka.connect.transforms | 2 |
| **总计** | **40** |

### Streams模块

| 包名 | 公共类 | internals内部类 |
|------|--------|-----------------|
| org.apache.kafka.streams | 20 | 0 |
| org.apache.kafka.streams.kstream | 56 | 160 |
| org.apache.kafka.streams.processor | 23 | 0 |
| org.apache.kafka.streams.state | 40 | 0 |
| org.apache.kafka.streams.errors | 32 | 0 |
| **总计** | **171** | **160** |

### internals包内部类（代码仓有但不应公开使用）

| 包名 | 类数量 | 说明 |
|------|--------|------|
| clients.admin.internals | 36 | Admin客户端内部实现 |
| clients.consumer.internals | 167 | Consumer内部实现 |
| clients.producer.internals | 18 | Producer内部实现 |
| streams.kstream.internals | 160 | Streams DSL内部实现 |
| **总计** | **381** | **不应在公共Javadoc中** |

### 新增Evolving API（Kafka 4.x新特性）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.kafka.clients.admin | **StreamsGroupDescription** | Streams Group描述（KIP-919） |
| org.apache.kafka.clients.admin | **StreamsGroupMemberAssignment** | Streams Group成员分配 |
| org.apache.kafka.clients.admin | **StreamsGroupMemberDescription** | Streams Group成员描述 |
| org.apache.kafka.clients.admin | **ListStreamsGroupOffsetsOptions/Result** | Streams Group偏移量操作 |
| org.apache.kafka.clients.admin | **DescribeStreamsGroupsOptions/Result** | Streams Group描述操作 |
| org.apache.kafka.clients.admin | **DeleteStreamsGroupsOptions/Result** | Streams Group删除操作 |
| org.apache.kafka.clients.admin | **AlterStreamsGroupOffsetsOptions/Result** | Streams Group偏移量修改 |

### Unstable API（实验性功能）

| 包名 | 类/方法 | 说明 |
|------|---------|------|
| org.apache.kafka.clients.admin.Admin | **unregisterBroker()** | 取消注册Broker（实验性） |
| org.apache.kafka.tools | **StreamsResetter** | Streams重置工具 |

### 新增Stable API（Raft Voter管理）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.kafka.clients.admin | **AddRaftVoterOptions/Result** | Raft投票者添加（KIP-853） |
| org.apache.kafka.clients.admin | **RemoveRaftVoterOptions/Result** | Raft投票者移除 |
| org.apache.kafka.clients.admin | **RaftVoterEndpoint** | Raft投票者端点 |

### Share Consumer API（KIP-932新功能）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.kafka.clients.consumer | **ShareConsumer<K,V>** | Share消费者接口 |
| org.apache.kafka.clients.consumer | **KafkaShareConsumer<K,V>** | Share消费者实现 |
| org.apache.kafka.clients.consumer | **AcknowledgeType** | 确认类型枚举 |

---

# 三、Iceberg API对比

## 代码仓公共接口数量：139个（api模块）

### api模块接口清单

| 包名 | 接口数量 | 核心接口 |
|------|----------|----------|
| org.apache.iceberg | 52 | Table, Snapshot, Transaction, Scan, AppendFiles, DeleteFiles, OverwriteFiles, RewriteFiles |
| org.apache.iceberg.catalog | 5 | Catalog, SessionCatalog, SupportsNamespaces, ViewCatalog |
| org.apache.iceberg.actions | 16 | Action, ActionsProvider, RewriteDataFiles, ExpireSnapshots, DeleteOrphanFiles |
| org.apache.iceberg.io | 15 | FileIO, InputFile, OutputFile, LocationProvider, SupportsBulkOperations |
| org.apache.iceberg.expressions | 8 | Expression, Term, Literal, Reference |
| org.apache.iceberg.metrics | 6 | MetricsReporter, MetricsContext, Counter, Timer |
| org.apache.iceberg.encryption | 6 | EncryptionManager, EncryptedInputFile, KmsClient |
| org.apache.iceberg.view | 10 | View, ViewVersion, ViewRepresentation, ViewBuilder |
| org.apache.iceberg.variants | 7 | Variant, VariantArray, VariantObject, VariantPrimitive |

### 新增API（代码仓新特性）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.iceberg.view | **View系列** | View功能（新特性） |
| org.apache.iceberg.variants | **Variant系列** | Variant类型支持（新特性） |
| org.apache.iceberg | **PartitionStatistics** | 分区统计功能 |
| org.apache.iceberg.actions | **ComputePartitionStats** | 分区统计计算 |
| org.apache.iceberg.actions | **ConvertEqualityDeleteFiles** | 等式删除转换 |
| org.apache.iceberg.actions | **RemoveDanglingDeleteFiles** | 悬空删除清理 |
| org.apache.iceberg.actions | **RewriteTablePath** | 表路径重写 |
| org.apache.iceberg.geospatial | **BoundingBox等** | 地理空间支持（新增） |

### core模块实现类（官方Javadoc包含，用户不应直接使用）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.iceberg | BaseTable | Table实现类 |
| org.apache.iceberg | BaseTransaction | Transaction实现类 |
| org.apache.iceberg | BaseMetastoreCatalog | Catalog实现类 |
| org.apache.iceberg | CatalogUtil | Catalog工具类 |
| org.apache.iceberg | AllDataFilesTable等 | 元数据表实现 |

---

# 四、HBase API对比

## 代码仓公共类数量：343个

### 核心类状态

| 类名 | 状态 | 说明 |
|------|------|------|
| Connection | Public ✓ | 核心连接接口 |
| Table | Public ✓ | 核心表接口（有deprecated方法） |
| Admin | Public ✓ | 管理接口（有deprecated方法） |
| AsyncConnection | Public ✓ | 异步连接接口 |
| AsyncTable | Public ✓ | 异步表接口 |
| Get/Put/Delete/Scan | Public ✓ | 操作类 |
| Result | Public ✓ | 结果对象 |
| Increment/Append | Public ✓ | 增量/追加操作 |

### 过滤器（46个，全部Public）

| 类型 | 过滤器 |
|------|--------|
| 行过滤器 | RowFilter, PrefixFilter, PageFilter, FirstKeyOnlyFilter, FuzzyRowFilter, MultiRowRangeFilter |
| 列过滤器 | QualifierFilter, FamilyFilter, ColumnPrefixFilter, ColumnRangeFilter, ColumnPaginationFilter |
| 值过滤器 | ValueFilter, SingleColumnValueFilter, ColumnValueFilter, DependentColumnFilter |
| 比较器 | BinaryComparator, RegexStringComparator, SubstringComparator, LongComparator |

### 新增API（代码仓新特性）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.hadoop.hbase.client | **ConnectionRegistry** | 连接注册器接口（替代ZK） |
| org.apache.hadoop.hbase.client | **RpcConnectionRegistry** | RPC连接注册器实现 |
| org.apache.hadoop.hbase.client | **TestingHBaseCluster** | 新测试集群框架 |
| org.apache.hadoop.hbase.client | **CheckAndMutate** | 条件操作新接口 |
| org.apache.hadoop.hbase.client | **QueryMetrics** | 查询指标类 |

### Deprecated API（将移除）

| 包名 | 类名 | 废弃版本 | 替代方案 |
|------|------|----------|----------|
| org.apache.hadoop.hbase.client | Table.checkAndMutate方法 | 4.0.0 | 使用CheckAndMutate类 |
| org.apache.hadoop.hbase.client | **MasterRegistry** | 2.5.0 | RpcConnectionRegistry |
| org.apache.hadoop.hbase.client | **HBaseTestingUtility** | 3.0.0 | TestingHBaseCluster |
| org.apache.hadoop.hbase.client | **MiniHBaseCluster** | 3.0.0 | TestingHBaseCluster |
| org.apache.hadoop.hbase.ipc | **CoprocessorRpcChannel** | 4.0.0 | 不再支持低级别RPC |
| org.apache.hadoop.hbase.quotas | **QuotaRetriever** | 3.0.0 | Admin API |

### 内部实现类（Private，不应使用）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.hadoop.hbase.client | AsyncConnectionImpl | 内部实现 |
| org.apache.hadoop.hbase.client | ConnectionOverAsyncConnection | 内部实现 |
| org.apache.hadoop.hbase.client | TableOverAsyncTable | 内部实现 |
| org.apache.hadoop.hbase.ipc | AbstractRpcClient | RPC客户端基类 |
| org.apache.hadoop.hbase.shaded.protobuf | ProtobufUtil | Protobuf工具 |

---

# 五、Hadoop API对比

## 代码仓Public API数量：902个

### 稳定性分布

| 模块 | @Public+@Stable | @Public+@Evolving | @Public+@Unstable |
|------|-----------------|-------------------|-------------------|
| hadoop-common | 108 | 172 | 48 |
| hadoop-hdfs-client | 3 | 34 | 3 |
| hadoop-mapreduce-client-core | 233 | 54 | 7 |
| hadoop-yarn-api | 0 | 8 | 36 |
| hadoop-yarn-client | 6 | 0 | 0 |

### @Stable核心API

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.hadoop.conf | **Configuration** | 核心配置类 |
| org.apache.hadoop.fs | **FileSystem** | 文件系统抽象基类 |
| org.apache.hadoop.fs | **Path** | 路径表示类 |
| org.apache.hadoop.fs | **FSDataInputStream/OutputStream** | 文件流 |
| org.apache.hadoop.io | **Writable, Text, IntWritable** | 序列化系列 |
| org.apache.hadoop.mapreduce | **Mapper, Reducer** | MR核心类 |
| org.apache.hadoop.mapreduce | **InputFormat, OutputFormat** | MR格式类 |
| org.apache.hadoop.yarn.client.api | **YarnClient, AMRMClient, NMClient** | YARN客户端 |

### 重要发现：DistributedFileSystem争议

| 项目 | 发现 |
|------|------|
| **官方文档** | 广泛介绍和使用DistributedFileSystem |
| **代码仓标注** | @LimitedPrivate({MapReduce, HBase}) + @Unstable |
| **实际影响** | 仅对MapReduce和HBase项目公开稳定性保证，普通用户不应依赖 |

### @Evolving API（演进中）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.hadoop.mapreduce | **Job** | 核心Job类（Evolving而非Stable） |
| org.apache.hadoop.hdfs.client | **HdfsAdmin** | HDFS管理API |
| org.apache.hadoop.hdfs.protocol | **CacheDirectiveInfo** | 缓存指令 |
| org.apache.hadoop.hdfs.protocol | **EncryptionZone** | 加密区域 |

### @LimitedPrivate API

| 包名 | 类名 | 限制范围 |
|------|------|----------|
| org.apache.hadoop.hdfs | **DistributedFileSystem** | MapReduce, HBase |
| org.apache.hadoop.yarn.ipc | **YarnRPC** | MapReduce, YARN |
| org.apache.hadoop.yarn.util | **RackResolver** | YARN, MapReduce |

### @Private API（不应使用）

| 包名 | 类名 | 说明 |
|------|------|------|
| org.apache.hadoop.fs | CommonConfigurationKeys | 使用CommonConfigurationKeysPublic替代 |
| org.apache.hadoop.hdfs.server | **所有server包** | NameNode/DataNode内部实现 |
| org.apache.hadoop.yarn.server | **所有server包** | ResourceManager/NodeManager内部实现 |

---

# 六、总结

## API稳定性对比汇总

| 项目 | Stable API | Evolving API | Unstable API | 内部Private API |
|------|------------|--------------|--------------|-----------------|
| Spark | 46个 | 5个 | 2个 | internal包 |
| Kafka | ~100个 | ~15个 | ~7个 | 381个internals |
| Iceberg | 大部分stable | View/Variants | geospatial | core模块实现类 |
| HBase | 核心类stable | ConnectionRegistry | deprecated类 | 实现类Private |
| Hadoop | MapReduce核心 | Job类/HDFS | DistributedFileSystem | server包全部Private |

## 关键发现

1. **Spark**：新增TimeMode、Geometry/Geography空间类型，Java特有API完整
2. **Kafka**：internals包381个内部类设计良好，新增Streams Group/Share Consumer/Raft Voter API
3. **Iceberg**：api模块仅接口，View和Variant是新特性，core实现类不应直接使用
4. **HBase**：16个deprecated类将移除，ConnectionRegistry替代ZK，CheckAndMutate替代旧接口
5. **Hadoop**：DistributedFileSystem实际是LimitedPrivate，Job类是Evolving，server包全部Private

## 使用建议

1. 优先使用@Stable标记的API
2. 关注@Evolving API的变化趋势
3. 避免依赖@Private/@LimitedPrivate API
4. deprecated API应尽快迁移到替代方案
5. internals包/internal包中的类不应直接使用