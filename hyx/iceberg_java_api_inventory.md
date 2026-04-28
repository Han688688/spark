# Iceberg Java API 清单

> 基于Iceberg代码仓 + 官方Javadoc (Iceberg 1.10.1) + 官方文档生成

## 官方文档参考

| 文档类型 | URL |
|---------|-----|
| **官方Javadoc** | https://iceberg.apache.org/javadoc/latest/ |
| **官方API文档** | https://iceberg.apache.org/docs/latest/api/ |
| **Java Quickstart** | https://iceberg.apache.org/docs/latest/java-api-quickstart/ |
| **Spark集成** | https://iceberg.apache.org/docs/latest/spark-getting-started/ |
| **Flink集成** | https://iceberg.apache.org/docs/latest/flink/ |

---

## 官方包描述 (Iceberg Latest)

根据官方Javadoc，Iceberg包含以下公共API包：

| 包名 | 官方描述 |
|------|---------|
| `org.apache.iceberg` | Core Iceberg API |
| `org.apache.iceberg.catalog` | Catalog interfaces |
| `org.apache.iceberg.expressions` | Expression API for filtering |
| `org.apache.iceberg.types` | Type definitions |
| `org.apache.iceberg.io` | File I/O interfaces |
| `org.apache.iceberg.encryption` | Encryption management |
| `org.apache.iceberg.metrics` | Metrics reporting |
| `org.apache.iceberg.actions` | Table maintenance actions |
| `org.apache.iceberg.view` | View API |
| `org.apache.iceberg.transforms` | Partition transforms |
| `org.apache.iceberg.data` | Generic data API |
| `org.apache.iceberg.spark` | Spark integration |
| `org.apache.iceberg.flink` | Flink integration |
| `org.apache.iceberg.hive` | Hive integration |
| `org.apache.iceberg.aws` | AWS integration (S3, Glue, DynamoDB) |
| `org.apache.iceberg.gcp` | GCP integration (GCS, BigQuery) |
| `org.apache.iceberg.azure` | Azure integration (ADLS) |
| `org.apache.iceberg.nessie` | Nessie integration |
| `org.apache.iceberg.parquet` | Parquet support |
| `org.apache.iceberg.orc` | ORC support |
| `org.apache.iceberg.kafka-connect` | Kafka Connect sink |
| `org.apache.iceberg.delta` | Delta Lake migration |

---

## 1. Core Table API

**包路径:** `org.apache.iceberg`

### 核心接口

| 接口名 | 描述 |
|--------|------|
| `Table` | 表的核心接口，提供表操作能力 |
| `Transaction` | 事务接口 |
| `Snapshot` | 快照定义 |
| `Scan` | 扫描接口 |
| `TableScan` | 表扫描接口 |
| `BatchScan` | 批量扫描接口 |
| `IncrementalAppendScan` | 增量追加扫描 |
| `IncrementalChangelogScan` | 增量变更日志扫描 |

### 数据操作接口

| 接口名 | 描述 |
|--------|------|
| `AppendFiles` | 追加文件 |
| `DeleteFiles` | 删除文件 |
| `OverwriteFiles` | 覆盖文件 |
| `RewriteFiles` | 重写文件 |
| `ReplacePartitions` | 替换分区 |
| `RowDelta` | 行变更 |

### 更新操作接口

| 接口名 | 描述 |
|--------|------|
| `UpdateSchema` | 更新Schema |
| `UpdatePartitionSpec` | 更新分区规格 |
| `UpdateProperties` | 更新属性 |
| `UpdateStatistics` | 更新统计信息 |
| `PendingUpdate` | 待更新接口 |
| `SnapshotUpdate` | 快照更新接口 |

### 文件元数据接口

| 接口名 | 描述 |
|--------|------|
| `DataFile` | 数据文件 |
| `DeleteFile` | 删除文件 |
| `ContentFile` | 内容文件 |
| `ManifestFile` | Manifest文件 |
| `ManifestListFile` | Manifest列表文件 |

---

## 2. Catalog API

**包路径:** `org.apache.iceberg.catalog`

### 核心接口

| 接口名 | 描述 |
|--------|------|
| `Catalog` | Catalog核心接口 |
| `SessionCatalog` | Session Catalog |
| `ViewCatalog` | View Catalog |
| `SupportsNamespaces` | Namespace支持接口 |

### 标识符类型

| 类名 | 描述 |
|------|------|
| `TableIdentifier` | 表标识符 |
| `Namespace` | Namespace |

---

## 3. Schema API

**包路径:** `org.apache.iceberg`

### 核心类

| 类名 | 描述 |
|------|------|
| `Schema` | 表Schema定义 |
| `PartitionSpec` | 分区规格 |
| `SortOrder` | 排序规格 |

---

## 4. Types API

**包路径:** `org.apache.iceberg.types`

### 核心类

| 类名 | 描述 |
|------|------|
| `Type` | 类型接口 |
| `Types` | 类型工厂 |
| `TypeUtil` | 类型工具 |
| `Conversions` | 类型转换 |

### 类型定义

| 类型 | 描述 |
|------|------|
| `Types.BooleanType` | Boolean类型 |
| `Types.IntegerType` | Integer类型 |
| `Types.LongType` | Long类型 |
| `Types.FloatType` | Float类型 |
| `Types.DoubleType` | Double类型 |
| `Types.StringType` | String类型 |
| `Types.DateType` | Date类型 |
| `Types.TimeType` | Time类型 |
| `Types.TimestampType` | Timestamp类型 |
| `Types.TimestampType.withZone()` | 带时区Timestamp |
| `Types.TimestampType.withoutZone()` | 不带时区Timestamp |
| `Types.BinaryType` | Binary类型 |
| `Types.DecimalType` | Decimal类型 |
| `Types.UUIDType` | UUID类型 |
| `Types.FixedType` | Fixed类型 |
| `Types.ListType` | List类型 |
| `Types.MapType` | Map类型 |
| `Types.StructType` | Struct类型 |
| `Types.NestedField` | 嵌套字段 |

---

## 5. Expressions API

**包路径:** `org.apache.iceberg.expressions`

### 核心类

| 类名 | 描述 |
|------|------|
| `Expression` | 表达式接口 |
| `Expressions` | 表达式工厂类 |
| `Predicate` | 谓词接口 |
| `Term` | Term接口 |
| `Literal` | Literal接口 |
| `BoundPredicate` | 绑定谓词 |
| `UnboundPredicate` | 未绑定谓词 |
| `Projections` | 表达式投影 |
| `Evaluator` | 表达式评估器 |
| `ManifestEvaluator` | Manifest评估器 |

### 表达式操作

| 操作 | 方法 |
|------|------|
| 等于 | `Expressions.equal("col", value)` |
| 不等于 | `Expressions.notEqual("col", value)` |
| 小于 | `Expressions.lessThan("col", value)` |
| 小于等于 | `Expressions.lessThanOrEqual("col", value)` |
| 大于 | `Expressions.greaterThan("col", value)` |
| 大于等于 | `Expressions.greaterThanOrEqual("col", value)` |
| 为空 | `Expressions.isNull("col")` |
| 不为空 | `Expressions.notNull("col")` |
| 包含 | `Expressions.in("col", values)` |
| 不包含 | `Expressions.notIn("col", values)` |
| 以...开始 | `Expressions.startsWith("col", prefix)` |
| 不以...开始 | `Expressions.notStartsWith("col", prefix)` |
| And | `Expressions.and(expr1, expr2)` |
| Or | `Expressions.or(expr1, expr2)` |
| Not | `Expressions.not(expr)` |

---

## 6. IO API

**包路径:** `org.apache.iceberg.io`

### 核心接口

| 接口名 | 描述 |
|--------|------|
| `FileIO` | 文件IO接口 |
| `InputFile` | 输入文件接口 |
| `OutputFile` | 输出文件接口 |
| `FileAppender` | 文件追加器 |
| `CloseableIterable` | 可关闭迭代器 |
| `LocationProvider` | 位置提供者 |
| `SupportsBulkOperations` | 批量操作支持 |
| `SupportsPrefixOperations` | 前缀操作支持 |

---

## 7. Encryption API

**包路径:** `org.apache.iceberg.encryption`

| 接口名 | 描述 |
|--------|------|
| `EncryptionManager` | 加密管理器 |
| `EncryptedInputFile` | 加密输入文件 |
| `EncryptedOutputFile` | 加密输出文件 |
| `KmsClient` | KMS客户端接口 |

---

## 8. Metrics API

**包路径:** `org.apache.iceberg.metrics`

| 接口名 | 描述 |
|--------|------|
| `MetricsContext` | 指标上下文 |
| `MetricsReporter` | 指标报告器 |
| `Counter` | 计数器 |
| `Timer` | 计时器 |
| `Histogram` | 直方图 |

---

## 9. Actions API

**包路径:** `org.apache.iceberg.actions`

| 接口名 | 描述 |
|--------|------|
| `Action` | Action基础接口 |
| `ActionsProvider` | Actions提供者 |
| `RewriteDataFiles` | 重写数据文件 |
| `RewriteManifests` | 重写Manifests |
| `RewritePositionDeleteFiles` | 重写位置删除文件 |
| `ExpireSnapshots` | 过期快照 |
| `DeleteOrphanFiles` | 删除孤儿文件 |
| `DeleteReachableFiles` | 删除可达文件 |
| `MigrateTable` | 迁移表 |
| `SnapshotTable` | 快照表 |
| `ComputePartitionStats` | 计算分区统计 |

---

## 10. View API

**包路径:** `org.apache.iceberg.view`

| 接口名 | 描述 |
|--------|------|
| `View` | View接口 |
| `ViewBuilder` | View构建器 |
| `ViewVersion` | View版本 |
| `ViewRepresentation` | View表示 |
| `UpdateViewProperties` | 更新View属性 |
| `ReplaceViewVersion` | 替换View版本 |

---

## 11. Transforms API

**包路径:** `org.apache.iceberg.transforms`

| 接口名 | 描述 |
|--------|------|
| `Transform` | 转换接口 |
| `PartitionSpecVisitor` | 分区规格访问者 |
| `SortOrderVisitor` | 排序规格访问者 |

### Transform类型

| Transform | 描述 |
|-----------|------|
| `identity` | 身份转换 |
| `year` | 年转换 |
| `month` | 月转换 |
| `day` | 日转换 |
| `hour` | 小时转换 |
| `bucket[N]` | 分桶转换 |
| `truncate[W]` | 截断转换 |
| `void` | 空转换 |

---

## 12. Variants API (新特性)

**包路径:** `org.apache.iceberg.variants`

| 接口名 | 描述 |
|--------|------|
| `Variant` | Variant接口 |
| `VariantValue` | Variant值 |
| `VariantObject` | Variant对象 |
| `VariantArray` | Variant数组 |
| `VariantPrimitive` | Variant原始类型 |

---

## 13. Spark Integration

**包路径:** `org.apache.iceberg.spark`

| 类名 | 描述 |
|------|------|
| `SparkCatalog` | Spark Catalog实现 |
| `SparkSessionCatalog` | Spark Session Catalog |
| `SparkTableUtil` | Spark表工具 |
| `SparkSchemaUtil` | Schema转换工具 |
| `SparkFilters` | 过滤器转换 |
| `SparkV2Filters` | V2过滤器转换 |
| `IcebergSpark` | Iceberg Spark入口 |
| `SparkReadConf` | 读取配置 |
| `SparkWriteConf` | 写入配置 |
| `SparkReadOptions` | 读取选项 |
| `SparkWriteOptions` | 写入选项 |

### Spark Actions

**包路径:** `org.apache.iceberg.spark.actions`

| 类名 | 描述 |
|------|------|
| `SparkActions` | Spark Actions入口 |
| `RewriteDataFilesSparkAction` | 重写数据文件Action |
| `RewriteManifestsSparkAction` | 重写Manifests Action |
| `ExpireSnapshotsSparkAction` | 过期快照Action |
| `DeleteOrphanFilesSparkAction` | 删除孤儿文件Action |

---

## 14. Flink Integration

**包路径:** `org.apache.iceberg.flink`

| 类名 | 描述 |
|------|------|
| `FlinkCatalog` | Flink Catalog实现 |
| `FlinkCatalogFactory` | Flink Catalog工厂 |
| `FlinkSchemaUtil` | Schema转换工具 |
| `FlinkFilters` | 过滤器转换 |
| `TableLoader` | 表加载器 |
| `CatalogLoader` | Catalog加载器 |
| `FlinkReadConf` | 读取配置 |
| `FlinkWriteConf` | 写入配置 |
| `FlinkReadOptions` | 读取选项 |
| `FlinkWriteOptions` | 写入选项 |

### Flink Source

**包路径:** `org.apache.iceberg.flink.source`

| 类名 | 描述 |
|------|------|
| `FlinkSource` | Source API |
| `IcebergSource` | Iceberg Source |
| `FlinkInputFormat` | 输入格式 |
| `StreamingMonitorFunction` | 流式监控 |

### Flink Sink

**包路径:** `org.apache.iceberg.flink.sink`

| 类名 | 描述 |
|------|------|
| `FlinkSink` | Sink API |
| `IcebergSink` | Iceberg Sink |
| `IcebergStreamWriter` | 流写入器 |
| `IcebergCommitter` | 提交器 |
| `FlinkFileWriterFactory` | 文件写入工厂 |

---

## 15. Hive Metastore Integration

**包路径:** `org.apache.iceberg.hive`

| 类名 | 描述 |
|------|------|
| `HiveCatalog` | Hive Catalog实现 |
| `HiveTableOperations` | Hive表操作 |
| `HiveViewOperations` | Hive视图操作 |
| `HiveClientPool` | Hive客户端池 |
| `HiveSchemaConverter` | Schema转换 |
| `HiveSchemaUtil` | Schema工具 |

---

## 16. AWS Integration

**包路径:** `org.apache.iceberg.aws`

| 类名 | 描述 |
|------|------|
| `AwsClientFactories` | AWS客户端工厂 |
| `AwsClientFactory` | AWS客户端工厂接口 |
| `AwsProperties` | AWS属性配置 |

### S3 FileIO

**包路径:** `org.apache.iceberg.aws.s3`

| 类名 | 描述 |
|------|------|
| `S3FileIO` | S3 FileIO实现 |
| `S3InputFile` | S3输入文件 |
| `S3OutputFile` | S3输出文件 |

### Glue Catalog

**包路径:** `org.apache.iceberg.aws.glue`

| 类名 | 描述 |
|------|------|
| `GlueCatalog` | Glue Catalog实现 |

---

## 17. GCP Integration

**包路径:** `org.apache.iceberg.gcp`

| 类名 | 描述 |
|------|------|
| `GCPProperties` | GCP属性配置 |
| `GCPAuthUtils` | GCP认证工具 |
| `GcpKeyManagementClient` | KMS客户端 |

### GCS FileIO

**包路径:** `org.apache.iceberg.gcp.gcs`

| 类名 | 描述 |
|------|------|
| `GCSFileIO` | GCS FileIO实现 |

---

## 18. Azure Integration

**包路径:** `org.apache.iceberg.azure`

| 类名 | 描述 |
|------|------|
| `AzureProperties` | Azure属性配置 |
| `AdlsTokenCredentialProvider` | ADLS凭证提供者 |

### ADLS FileIO

**包路径:** `org.apache.iceberg.azure.adlsv2`

| 类名 | 描述 |
|------|------|
| `AdlsFileIO` | ADLS v2 FileIO实现 |

---

## 19. Nessie Integration

**包路径:** `org.apache.iceberg.nessie`

| 类名 | 描述 |
|------|------|
| `NessieCatalog` | Nessie Catalog实现 |
| `NessieIcebergClient` | Nessie客户端 |
| `NessieTableOperations` | Nessie表操作 |
| `NessieViewOperations` | Nessie视图操作 |

---

## 20. Data API (Generic)

**包路径:** `org.apache.iceberg.data`

| 类名 | 描述 |
|------|------|
| `IcebergGenerics` | Generic API入口 |
| `TableScanIterable` | 表扫描迭代器 |
| `GenericAppenderFactory` | Generic追加器工厂 |
| `GenericFileWriterFactory` | Generic文件写入工厂 |
| `Record` | Record接口 |

---

## 21. Parquet Support

**包路径:** `org.apache.iceberg.parquet`

| 类名 | 描述 |
|------|------|
| `Parquet` | Parquet入口 |
| `ParquetIO` | Parquet IO |
| `ParquetReader` | Parquet读取器 |
| `ParquetWriter` | Parquet写入器 |
| `ParquetSchemaUtil` | Schema工具 |
| `ParquetFilters` | 过滤器 |
| `ParquetMetrics` | 指标 |

---

## 22. ORC Support

**包路径:** `org.apache.iceberg.orc`

| 类名 | 描述 |
|------|------|
| `ORC` | ORC入口 |
| `ORCFormatModel` | ORC格式模型 |
| `OrcReader` | ORC读取器 |
| `OrcWriter` | ORC写入器 |
| `OrcRowReader` | ORC行读取器 |
| `OrcRowWriter` | ORC行写入器 |
| `ORCSchemaUtil` | Schema工具 |

---

## 23. Arrow Support

**包路径:** `org.apache.iceberg.arrow`

| 类名 | 描述 |
|------|------|
| `ArrowSchemaUtil` | Schema工具 |
| `ArrowAllocation` | Arrow内存分配 |
| `DictEncodedArrowConverter` | 字典编码转换 |

---

## 24. MapReduce Support

**包路径:** `org.apache.iceberg.mr`

| 类名 | 描述 |
|------|------|
| `Catalogs` | Catalog工具 |
| `InputFormatConfig` | InputFormat配置 |
| `IcebergInputFormat` | Iceberg InputFormat |
| `IcebergOutputFormat` | Iceberg OutputFormat |

---

## 25. Kafka Connect Support

**包路径:** `org.apache.iceberg.connect`

| 类名 | 描述 |
|------|------|
| `IcebergSinkConnector` | Kafka Sink Connector |
| `IcebergSinkTask` | Kafka Sink Task |
| `IcebergSinkConfig` | Sink配置 |
| `TableSinkConfig` | 表Sink配置 |
| `Committer` | 提交器 |
| `CommitterFactory` | 提交器工厂 |

---

## 26. Delta Lake Integration

**包路径:** `org.apache.iceberg.delta`

| 类名 | 描述 |
|------|------|
| `DeltaLakeToIcebergMigrationActionsProvider` | Delta迁移Action |
| `SnapshotDeltaLakeTable` | Delta表快照 |

---

## 模块依赖关系

```
api (核心接口定义)
    ↓
core (核心实现)
    ↓
┌───────────────────────────────────────┐
│  集成模块                              │
│  ├── spark (Spark集成)                 │
│  ├── flink (Flink集成)                 │
│  ├── hive-metastore (Hive集成)         │
│  ├── nessie (Nessie集成)               │
│  ├── mr (MapReduce集成)                │
│  ├── kafka-connect (Kafka Connect)     │
│  └── delta-lake (Delta Lake迁移)       │
└───────────────────────────────────────┘
    ↓
┌───────────────────────────────────────┐
│  云存储集成                            │
│  ├── aws (S3, Glue, DynamoDB)          │
│  ├── gcp (GCS)                         │
│  ├── azure (ADLS)                      │
│  └── dell (Dell ECS)                   │
└───────────────────────────────────────┘
    ↓
┌───────────────────────────────────────┐
│  文件格式支持                          │
│  ├── parquet (Parquet)                 │
│  ├── orc (ORC)                         │
│  ├── arrow (Arrow)                     │
│  └── data (Generic Record)             │
└───────────────────────────────────────┘
```

---

## 使用示例

### 创建表

```java
Catalog catalog = CatalogUtil.loadCatalog(HiveCatalog.class.getName(), 
    "uri", "thrift://localhost:9083", new HashMap<>());

Table table = catalog.createTable(
    TableIdentifier.of("db", "table"),
    new Schema(
        Types.NestedField.required(1, "id", Types.IntegerType.get()),
        Types.NestedField.required(2, "name", Types.StringType.get())
    ),
    PartitionSpec.unpartitioned()
);
```

### 写入数据

```java
Table table = catalog.loadTable(TableIdentifier.of("db", "table"));
AppendFiles append = table.newAppend();
append.appendFile(dataFile);
append.commit();
```

### 查询数据

```java
Table table = catalog.loadTable(TableIdentifier.of("db", "table"));
TableScan scan = table.newScan()
    .filter(Expressions.greaterThan("id", 100))
    .select("id", "name");

for (DataFile file : scan.planFiles()) {
    // 处理文件
}
```

### Spark集成

```sql
-- Spark SQL
CREATE TABLE db.table USING iceberg
AS SELECT * FROM source;

-- 查询
SELECT * FROM db.table WHERE id > 100;
```

### 维护操作

```java
SparkActions actions = SparkActions.get(spark);

// 重写数据文件
actions.rewriteDataFiles(table)
    .option("target-file-size-bytes", "134217728")
    .execute();

// 过期快照
actions.expireSnapshots(table)
    .expireOlderThan(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(7))
    .execute();
```

---

## 参考链接

- Iceberg官方文档: https://iceberg.apache.org
- Java API文档: https://iceberg.apache.org/docs/latest/api