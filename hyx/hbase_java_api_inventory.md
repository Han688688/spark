# HBase Java API 清单

> 基于HBase代码仓 + 官方Javadoc (HBase 4.0.0) + 官方文档生成

## 官方文档参考

| 文档类型 | URL |
|---------|-----|
| **官方Javadoc** | https://hbase.apache.org/apidocs/index.html |
| **官方文档** | https://hbase.apache.org/book.html |
| **客户端API文档** | https://hbase.apache.org/book.html#hbase_apis |
| **MapReduce集成** | https://hbase.apache.org/book.html#mapreduce |
| **Coprocessor** | https://hbase.apache.org/book.html#cp |

---

## 官方包描述 (HBase 4.0.0)

根据官方Javadoc，HBase 4.0.0包含以下公共API包：

| 包名 | 官方描述 |
|------|---------|
| `org.apache.hadoop.hbase.client` | **Provides HBase Client** - 核心客户端API |
| `org.apache.hadoop.hbase.filter` | **Row-level filters** - 行级过滤器，应用于HRegion scan结果 |
| `org.apache.hadoop.hbase` | Core HBase classes |
| `org.apache.hadoop.hbase.backup` | Backup API |
| `org.apache.hadoop.hbase.coprocessor` | Coprocessor API |
| `org.apache.hadoop.hbase.mapreduce` | **MapReduce Input/OutputFormats** - MapReduce集成 |
| `org.apache.hadoop.hbase.mapred` | MapReduce (old API) |
| `org.apache.hadoop.hbase.replication` | **Multi Cluster Replication** - 多集群复制 |
| `org.apache.hadoop.hbase.rest` | **HBase REST** - REST API |
| `org.apache.hadoop.hbase.thrift` | **Thrift service** - Thrift服务 |
| `org.apache.hadoop.hbase.quotas` | Quota API |
| `org.apache.hadoop.hbase.security` | Security API |
| `org.apache.hadoop.hbase.types` | **Extensible data type API** - 可扩展数据类型 |
| `org.apache.hadoop.hbase.util` | Utility classes |
| `org.apache.hadoop.hbase.io` | I/O classes |
| `org.apache.hadoop.hbase.io.encoding` | Data block encoding |
| `org.apache.hadoop.hbase.io.hfile` | **HFile and BlockCache** - HFile实现 |
| `org.apache.hadoop.hbase.metrics` | **Metrics API** - 指标API |
| `org.apache.hadoop.hbase.mob` | MOB (Medium-sized Objects) |
| `org.apache.hadoop.hbase.snapshot` | Snapshot API |
| `org.apache.hadoop.hbase.testing` | Testing utilities |

---

## 1. Connection API

**包路径:** `org.apache.hadoop.hbase.client`

### 核心接口

| 接口名 | 描述 |
|--------|------|
| `Connection` | 集群连接接口 |
| `Table` | 表操作接口 |
| `Admin` | 管理接口 |
| `AsyncConnection` | 异步连接接口 |
| `AsyncTable<ScanResultConsumer>` | 异步表接口 |
| `AsyncAdmin` | 异步管理接口 |
| `BufferedMutator` | 批量写入接口 |
| `RegionLocator` | Region定位接口 |

### 连接工厂

| 类名 | 描述 |
|------|------|
| `ConnectionFactory` | 连接工厂类 |
| `ConnectionBuilder` | 连接构建器 |

---

## 2. Table Operations

**包路径:** `org.apache.hadoop.hbase.client`

### 核心操作类

| 类名 | 描述 |
|------|------|
| `Get` | 查询操作 |
| `Put` | 写入操作 |
| `Delete` | 删除操作 |
| `Scan` | 扫描操作 |
| `Result` | 结果对象 |
| `Increment` | 增量操作 |
| `Append` | 追加操作 |
| `Mutation` | 变更操作基类 |
| `RowMutations` | 行变更操作集合 |

### 条件操作

| 类名 | 描述 |
|------|------|
| `CheckAndMutate` | 条件变更操作 |
| `CheckAndMutateResult` | 条件变更结果 |

### 扫描结果

| 类名 | 描述 |
|------|------|
| `ResultScanner` | 结果扫描器接口 |
| `AsyncTableResultScanner` | 异步结果扫描器 |

---

## 3. Table Metadata

**包路径:** `org.apache.hadoop.hbase.client`

### 描述符

| 类名 | 描述 |
|------|------|
| `TableDescriptor` | 表描述符接口 |
| `TableDescriptorBuilder` | 表描述符构建器 |
| `ColumnFamilyDescriptor` | 列族描述符接口 |
| `ColumnFamilyDescriptorBuilder` | 列族描述符构建器 |
| `RegionInfo` | Region信息接口 |
| `RegionInfoBuilder` | Region信息构建器 |
| `SnapshotDescription` | 快照描述 |
| `NamespaceDescriptor` | Namespace描述符 |

### 配置枚举

| 枚举 | 描述 |
|------|------|
| `Durability` | 持久性级别 |
| `Consistency` | 一致性级别 |
| `CompactionState` | 压缩状态 |
| `IsolationLevel` | 隔离级别 |

---

## 4. Filter API

**包路径:** `org.apache.hadoop.hbase.filter`

### 基础过滤器

| 类名 | 描述 |
|------|------|
| `Filter` | 过滤器基接口 |
| `FilterBase` | 过滤器基类 |
| `FilterList` | 过滤器列表组合 |

### 行过滤器

| 类名 | 描述 |
|------|------|
| `RowFilter` | 行过滤器 |
| `PrefixFilter` | 前缀过滤器 |
| `PageFilter` | 分页过滤器 |
| `FirstKeyOnlyFilter` | 仅首键过滤器 |
| `KeyOnlyFilter` | 仅键过滤器 |
| `RandomRowFilter` | 随机行过滤器 |
| `MultiRowRangeFilter` | 多行范围过滤器 |
| `FuzzyRowFilter` | 模糊行过滤器 |
| `InclusiveStopFilter` | 包含停止过滤器 |

### 列过滤器

| 类名 | 描述 |
|------|------|
| `ColumnPrefixFilter` | 列前缀过滤器 |
| `ColumnPaginationFilter` | 列分页过滤器 |
| `ColumnRangeFilter` | 列范围过滤器 |
| `FirstKeyValueMatchingQualifiersFilter` | 首值匹配过滤器 |
| `MultipleColumnPrefixFilter` | 多列前缀过滤器 |
| `QualifierFilter` | 列名过滤器 |
| `FamilyFilter` | 列族过滤器 |

### 值过滤器

| 类名 | 描述 |
|------|------|
| `ValueFilter` | 值过滤器 |
| `SingleColumnValueFilter` | 单列值过滤器 |
| `SingleColumnValueExcludeFilter` | 单列值排除过滤器 |
| `ColumnValueFilter` | 列值过滤器 |
| `DependentColumnFilter` | 依赖列过滤器 |

### 比较过滤器

| 类名 | 描述 |
|------|------|
| `CompareFilter` | 比较过滤器基类 |
| `RowFilter` | 行比较过滤器 |
| `QualifierFilter` | 列比较过滤器 |
| `ValueFilter` | 值比较过滤器 |
| `FamilyFilter` | 列族比较过滤器 |

### 时间过滤器

| 类名 | 描述 |
|------|------|
| `TimestampsFilter` | 时间戳过滤器 |
| `TimeRangeFilter` | 时间范围过滤器 |

### 特殊过滤器

| 类名 | 描述 |
|------|------|
| `SkipFilter` | 跳过过滤器 |
| `WhileMatchFilter` | 匹配终止过滤器 |
| `FilterAllFilter` | 全过滤过滤器 |

### 比较器

| 类名 | 描述 |
|------|------|
| `ByteArrayComparable` | 比较器基类 |
| `BinaryComparator` | 二进制比较器 |
| `BinaryPrefixComparator` | 二进制前缀比较器 |
| `RegexStringComparator` | 正则比较器 |
| `SubstringComparator` | 子字符串比较器 |
| `LongComparator` | Long比较器 |
| `DoubleComparator` | Double比较器 |
| `BigDecimalComparator` | BigDecimal比较器 |
| `NullComparator` | 空值比较器 |
| `BitComparator` | 位比较器 |

---

## 5. Cell API

**包路径:** `org.apache.hadoop.hbase`

### 核心接口

| 接口名 | 描述 |
|--------|------|
| `Cell` | 单元格接口 |
| `CellBuilder` | 单元格构建器接口 |
| `CellComparator` | 单元格比较器接口 |

### 实现类

| 类名 | 描述 |
|------|------|
| `KeyValue` | 键值对类 |
| `CellUtil` | 单元格工具类 |
| `CellBuilderFactory` | 单元格构建器工厂 |
| `CellComparatorImpl` | 单元格比较器实现 |
| `ExtendedCell` | 扩展单元格接口 |
| `ExtendedCellBuilder` | 扩展单元格构建器 |
| `RawCell` | Raw单元格接口 |
| `RawCellBuilderFactory` | Raw单元格构建器工厂 |

### Cell类型

| 类型 | 描述 |
|------|------|
| `Cell.Type.Put` | Put类型 |
| `Cell.Type.Delete` | Delete类型 |
| `Cell.Type.DeleteFamily` | DeleteFamily类型 |
| `Cell.Type.DeleteColumn` | DeleteColumn类型 |
| `Cell.Type.DeleteFamilyVersion` | DeleteFamilyVersion类型 |

---

## 6. Common API

**包路径:** `org.apache.hadoop.hbase`

### 核心类

| 类名 | 描述 |
|------|------|
| `TableName` | 表名类 |
| `ServerName` | 服务器名类 |
| `HConstants` | 常量定义 |
| `HBaseConfiguration` | 配置类 |
| `HBaseIOException` | IO异常基类 |
| `NamespaceDescriptor` | Namespace描述符 |

### 工具类

| 类名 | 描述 |
|------|------|
| `Bytes` | 字节数组工具类 |
| `Pair<T1,T2>` | 二元组 |
| `PairOfPairs<T1,T2,T3,T4>` | 四元组 |
| `Triple<T1,T2,T3>` | 三元组 |
| `Order` | 有序编码 |
| `OrderedBytes` | 有序字节编码 |
| `ByteRange` | 字节范围接口 |
| `TimeRange` | 时间范围 |

### 安全相关

| 类名 | 描述 |
|------|------|
| `User` | 用户类 |
| `AuthUtil` | 认证工具 |
| `AccessControlException` | 访问控制异常 |

### IO相关

| 类名 | 描述 |
|------|------|
| `ImmutableBytesWritable` | 不可变字节可写对象 |
| `ByteBufferOutputStream` | 字节缓冲输出流 |
| `Tag` | Tag接口 |
| `ArrayBackedTag` | 数组 backed Tag |

### 加密相关

| 类名 | 描述 |
|------|------|
| `Cipher` | 加密类 |
| `CipherProvider` | 加密提供者 |
| `KeyProvider` | 密钥提供者 |
| `Encryption` | 加密工具 |

### 类型系统

| 接口名 | 描述 |
|--------|------|
| `DataType` | 数据类型接口 |
| `Struct` | 结构化数据接口 |
| `StructBuilder` | 结构构建器 |
| `OrderedString` | 有序String类型 |
| `OrderedNumeric` | 有序Numeric类型 |
| `OrderedBytes` | 有序字节类型 |

---

## 7. Admin Operations

**包路径:** `org.apache.hadoop.hbase.client`

### 表管理操作

| 方法 | 描述 |
|------|------|
| `createTable(TableDescriptor)` | 创建表 |
| `createTableAsync(TableDescriptor)` | 异步创建表 |
| `deleteTable(TableName)` | 删除表 |
| `disableTable(TableName)` | 禁用表 |
| `enableTable(TableName)` | 启用表 |
| `modifyTable(TableDescriptor)` | 修改表 |
| `truncateTable(TableName, boolean)` | 清空表 |

### Region操作

| 方法 | 描述 |
|------|------|
| `split(TableName)` | 分裂Region |
| `splitRegion(byte[])` | 分裂指定Region |
| `mergeRegions(byte[], byte[], boolean)` | 合并Region |
| `compact(TableName)` | 压缩表 |
| `majorCompact(TableName)` | 主压缩表 |
| `flush(TableName)` | 刷新表 |
| `assign(byte[])` | 分配Region |
| `unassign(byte[], boolean)` | 取消分配Region |
| `move(byte[], byte[])` | 移动Region |

### 快照操作

| 方法 | 描述 |
|------|------|
| `snapshot(String snapshotName, TableName)` | 创建快照 |
| `cloneSnapshot(String, TableName)` | 克隆快照 |
| `restoreSnapshot(String)` | 恢复快照 |
| `deleteSnapshot(String)` | 删除快照 |
| `listSnapshots()` | 列出快照 |

### Namespace操作

| 方法 | 描述 |
|------|------|
| `createNamespace(NamespaceDescriptor)` | 创建Namespace |
| `modifyNamespace(NamespaceDescriptor)` | 修改Namespace |
| `deleteNamespace(String)` | 删除Namespace |
| `getNamespaceDescriptor(String)` | 获取Namespace描述 |
| `listNamespaceDescriptors()` | 列出Namespace |

---

## 8. Quota API

**包路径:** `org.apache.hadoop.hbase.quotas`

| 类名 | 描述 |
|------|------|
| `QuotaSettings` | 配额设置基类 |
| `QuotaSettingsFactory` | 配额设置工厂 |
| `ThrottleSettings` | 限流设置 |
| `SpaceLimitSettings` | 空间限制设置 |
| `BypassThrottleSettings` | 绕过限流设置 |

---

## 9. Security API

**包路径:** `org.apache.hadoop.hbase.security`

| 类名 | 描述 |
|------|------|
| `AccessControlClient` | 访问控制客户端工具 |
| `VisibilityClient` | 可见性客户端工具 |
| `User` | 用户类 |
| `AuthUtil` | 认证工具 |

---

## 10. REST API

**包路径:** `org.apache.hadoop.hbase.rest`

| 类名 | 描述 |
|------|------|
| `Client` | REST客户端 |
| `Cluster` | 集群配置 |
| `Response` | REST响应 |
| `RestCsrfPreventionFilter` | CSRF防护过滤器 |
| `Constants` | REST常量 |

---

## 11. Thrift API

**包路径:** `org.apache.hadoop.hbase.thrift`

### Thrift客户端

| 类名 | 描述 |
|------|------|
| `ThriftConnection` | Thrift连接实现 |
| `ThriftTable` | Thrift表实现 |
| `ThriftAdmin` | Thrift管理实现 |
| `ThriftClientBuilder` | Thrift客户端构建器 |

### Thrift2

| 类名 | 描述 |
|------|------|
| `ThriftHBaseServiceHandler` | Thrift服务处理器 |
| `ThriftUtilities` | Thrift工具类 |

### Thrift数据类型

| 类名 | 描述 |
|------|------|
| `TGet` | Thrift Get操作 |
| `TPut` | Thrift Put操作 |
| `TDelete` | Thrift Delete操作 |
| `TScan` | Thrift Scan操作 |
| `TResult` | Thrift结果 |
| `TColumnValue` | Thrift列值 |
| `TColumn` | Thrift列 |
| `TRowMutations` | Thrift行变更 |
| `TIncrement` | Thrift增量操作 |
| `TAppend` | Thrift追加操作 |

---

## 12. MapReduce API

**包路径:** `org.apache.hadoop.hbase.mapreduce`

### InputFormat

| 类名 | 描述 |
|------|------|
| `TableInputFormat` | 表输入格式 |
| `TableInputFormatBase` | 表输入格式基类 |
| `TableSnapshotInputFormat` | 表快照输入格式 |
| `MultiTableInputFormat` | 多表输入格式 |
| `WALInputFormat` | WAL输入格式 |
| `HFileInputFormat` | HFile输入格式 |

### OutputFormat

| 类名 | 描述 |
|------|------|
| `TableOutputFormat` | 表输出格式 |
| `MultiTableOutputFormat` | 多表输出格式 |
| `HFileOutputFormat2` | HFile输出格式 |

### Mapper/Reducer

| 类名 | 描述 |
|------|------|
| `TableMapper<K,V>` | 表Mapper基类 |
| `TableReducer<K,V,KEY>` | 表Reducer基类 |
| `IdentityTableMapper` | 身份表Mapper |
| `IdentityTableReducer` | 身份表Reducer |
| `GroupingTableMapper` | 分组表Mapper |

### 工具类

| 类名 | 描述 |
|------|------|
| `TableMapReduceUtil` | MapReduce工具类 |
| `Import` | 导入工具 |
| `Export` | 导出工具 |
| `CopyTable` | 复制表工具 |
| `RowCounter` | 行计数器 |
| `CellCounter` | 单元格计数器 |
| `ImportTsv` | TSV导入工具 |
| `WALPlayer` | WAL播放器 |
| `ExportSnapshot` | 快照导出 |
| `SyncTable` | 同步表工具 |
| `BulkLoadHFiles` | 批量加载HFiles |

### 分区器

| 类名 | 描述 |
|------|------|
| `HRegionPartitioner` | Region分区器 |
| `SimpleTotalOrderPartitioner` | 简单全序分区器 |
| `KeyHashPartitioner` | Key哈希分区器 |

---

## 13. Coprocessor Endpoint API

**包路径:** `org.apache.hadoop.hbase.client.coprocessor`

| 类名 | 描述 |
|------|------|
| `AggregationClient` | 聚合客户端 |
| `AsyncAggregationClient` | 异步聚合客户端 |

---

## 14. Replication API

**包路径:** `org.apache.hadoop.hbase.replication`

| 类名 | 描述 |
|------|------|
| `ReplicationPeer` | 复制对端接口 |
| `ReplicationPeerConfig` | 复制对端配置 |
| `ReplicationQueueStorage` | 复制队列存储 |
| `ReplicationPeers` | 复制对端管理 |

---

## 15. Backup API

**包路径:** `org.apache.hadoop.hbase.backup`

| 类名 | 描述 |
|------|------|
| `BackupAdmin` | 备份管理接口 |
| `BackupRequest` | 备份请求 |
| `RestoreRequest` | 恢复请求 |
| `BackupInfo` | 备份信息 |
| `BackupClientFactory` | 备份客户端工厂 |

---

## 16. Server API (服务端)

**包路径:** `org.apache.hadoop.hbase`

| 类名 | 描述 |
|------|------|
| `LocalHBaseCluster` | 本地HBase集群 |
| `JMXListener` | JMX监听器 |

---

## 模块依赖关系

```
hbase-common (公共API)
    ├── Cell, KeyValue, TableName
    ├── Bytes, Pair等工具类
    └── HConstants常量
    ↓
hbase-client (客户端API)
    ├── Connection, Table, Admin
    ├── Get, Put, Delete, Scan, Result
    ├── Filter API (46+过滤器)
    └── Descriptor (Table, ColumnFamily, Region)
    ↓
┌───────────────────────────────────────┐
│  扩展接口                              │
│  ├── hbase-rest (REST客户端)           │
│  ├── hbase-thrift (Thrift客户端)       │
│  ├── hbase-mapreduce (MapReduce集成)   │
│  ├── hbase-endpoint (协处理器)         │
│  ├── hbase-replication (复制)          │
│  └── hbase-backup (备份)               │
└───────────────────────────────────────┘
    ↓
hbase-server (服务端实现)
    ├── LocalHBaseCluster
    └── 管理工具
```

---

## 使用示例

### 连接HBase

```java
Configuration config = HBaseConfiguration.create();
config.set("hbase.zookeeper.quorum", "localhost");
Connection connection = ConnectionFactory.createConnection(config);
```

### 创建表

```java
Admin admin = connection.getAdmin();
TableDescriptor tableDescriptor = TableDescriptorBuilder.newBuilder(TableName.valueOf("mytable"))
    .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder("cf")
        .setMaxVersions(3)
        .build())
    .build();
admin.createTable(tableDescriptor);
```

### Put操作

```java
Table table = connection.getTable(TableName.valueOf("mytable"));
Put put = new Put(Bytes.toBytes("row1"));
put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col1"), Bytes.toBytes("value1"));
table.put(put);
```

### Get操作

```java
Get get = new Get(Bytes.toBytes("row1"));
Result result = table.get(get);
byte[] value = result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("col1"));
```

### Scan操作

```java
Scan scan = new Scan();
scan.setStartRow(Bytes.toBytes("row1"));
scan.setStopRow(Bytes.toBytes("row9"));
scan.addFamily(Bytes.toBytes("cf"));
ResultScanner scanner = table.getScanner(scan);
for (Result result : scanner) {
    // 处理结果
}
scanner.close();
```

### 使用过滤器

```java
Scan scan = new Scan();
SingleColumnValueFilter filter = new SingleColumnValueFilter(
    Bytes.toBytes("cf"),
    Bytes.toBytes("col1"),
    CompareOperator.EQUAL,
    Bytes.toBytes("value1")
);
scan.setFilter(filter);
```

### 异步API

```java
AsyncConnection asyncConnection = ConnectionFactory.createAsyncConnection(config);
AsyncTable<ScanResultConsumer> asyncTable = asyncConnection.getTableBuilder(TableName.valueOf("mytable"))
    .setOperationTimeout(30, TimeUnit.SECONDS)
    .build();

// 异步Put
asyncTable.put(new Put(Bytes.toBytes("row1"))
    .addColumn(Bytes.toBytes("cf"), Bytes.toBytes("col1"), Bytes.toBytes("value1")))
    .thenAccept(v -> System.out.println("Put completed"));
```

### MapReduce集成

```java
Configuration config = HBaseConfiguration.create();
Job job = Job.getInstance(config, "HBaseMR");

Scan scan = new Scan();
TableMapReduceUtil.initTableMapperJob(
    TableName.valueOf("input_table"),
    scan,
    MyMapper.class,
    Text.class,
    IntWritable.class,
    job
);
TableMapReduceUtil.initTableReducerJob(
    "output_table",
    MyReducer.class,
    job
);
```

---

## 参考链接

- HBase官方文档: https://hbase.apache.org/book.html
- Java API文档: https://hbase.apache.org/apidocs/index.html