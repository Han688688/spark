# HBase Java API 完整清单

> 参考官方文档: https://hbase.apache.org/apidocs/
> 版本: HBase 2.2.3 / 4.0.0-alpha
> 生成日期: 2026-04-29

---

## 目录

1. [Connection API](#1-connection-api)
2. [Table Operations](#2-table-operations)
3. [Admin API](#3-admin-api)
4. [Scan Operations](#4-scan-operations)
5. [Put/Delete Operations](#5-putdelete-operations)
6. [CheckAndMutate API](#6-checkandmutate-api)
7. [新测试框架](#7-新测试框架)
8. [方法数量统计](#8-方法数量统计)

---

## 1. Connection API

### 1.1 Connection 接口
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Public`

Connection是一个集群连接，封装了与实际服务器的底层连接和ZooKeeper连接。

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `getConfiguration()` | Configuration | 返回此Connection实例使用的Configuration对象 | Stable |
| `getTable(TableName tableName)` | Table | 获取访问表的Table实例 (default方法) | Stable |
| `getTable(TableName tableName, ExecutorService pool)` | Table | 获取Table实例，指定线程池 (default方法) | Stable |
| `getBufferedMutator(TableName tableName)` | BufferedMutator | 获取用于客户端写缓冲的BufferedMutator | Stable |
| `getBufferedMutator(BufferedMutatorParams params)` | BufferedMutator | 获取BufferedMutator，自定义参数 | Stable |
| `getRegionLocator(TableName tableName)` | RegionLocator | 获取RegionLocator用于检查表的region信息 | Stable |
| `clearRegionLocationCache()` | void | 清除所有表的region位置缓存 | Evolving |
| `getAdmin()` | Admin | 获取Admin实例用于集群管理 | Stable |
| `close()` | void | 关闭连接，释放资源 | Stable |
| `isClosed()` | boolean | 返回连接是否已关闭 | Stable |
| `getTableBuilder(TableName tableName, ExecutorService pool)` | TableBuilder | 返回TableBuilder用于创建Table | Stable |

**继承的方法** (来自 `Abortable`):
- `abort(String why, Throwable e)` - 终止服务器或客户端
- `isAborted()` - 返回是否已终止

### 1.2 ConnectionFactory 类
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Public`

ConnectionFactory是管理Connection创建的非实例化类。

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `createConnection()` | Connection | 创建默认配置的Connection | Stable |
| `createConnection(Configuration conf)` | Connection | 创建指定配置的Connection | Stable |
| `createConnection(Configuration conf, ExecutorService pool)` | Connection | 创建Connection，指定线程池 | Stable |
| `createConnection(Configuration conf, User user)` | Connection | 创建Connection，指定用户 | Stable |
| `createConnection(Configuration conf, ExecutorService pool, User user)` | Connection | 创建Connection，完整参数 | Stable |

### 1.3 AsyncConnection 接口 (异步版本)
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Evolving`

异步版本的Connection接口。

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `getAdmin()` | AsyncAdmin | 获取异步Admin实例 | Evolving |
| `getAdminBuilder()` | AsyncAdminBuilder | 获取AsyncAdminBuilder | Evolving |
| `getTableBuilder(TableName tableName)` | AsyncTableBuilder | 获取AsyncTableBuilder | Evolving |
| `getRegionLocator(TableName tableName)` | AsyncTableRegionLocator | 获取异步RegionLocator | Evolving |
| `close()` | void | 关闭连接 | Evolving |
| `isClosed()` | boolean | 返回是否已关闭 | Evolving |

### 1.4 ConnectionRegistry 系列 (新增替代ZK)
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Private` (内部使用)

用于替代直接ZooKeeper连接的新的连接注册机制。

---

## 2. Table Operations

### 2.1 Table 接口
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Public`

Table接口用于与单个HBase表通信。

#### 数据读取操作

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `get(Get get)` | Result | 获取指定行的数据 (default) | Stable |
| `get(List<Get> gets)` | Result[] | 批量获取多行数据 (default) | Stable |
| `exists(Get get)` | boolean | 测试指定列是否存在 (default) | Stable |
| `exists(List<Get> gets)` | boolean[] | 批量测试列是否存在 (default) | Stable |
| `getScanner(Scan scan)` | ResultScanner | 返回Scanner扫描表 (default) | Stable |
| `getScanner(byte[] family)` | ResultScanner | 扫描指定列族 (default) | Stable |
| `getScanner(byte[] family, byte[] qualifier)` | ResultScanner | 扫描指定列 (default) | Stable |

#### 数据写入操作

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `put(Put put)` | void | 写入单个Put (default) | Stable |
| `put(List<Put> puts)` | void | 批量写入多个Put (default) | Stable |
| `delete(Delete delete)` | void | 删除数据 (default) | Stable |
| `delete(List<Delete> deletes)` | void | 批量删除 (default) | Stable |
| `append(Append append)` | Result | 追加操作 (default) | Stable |
| `increment(Increment increment)` | Result | 增量计数操作 (default) | Stable |
| `incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount)` | long | 增加列值 (default) | Stable |
| `incrementColumnValue(byte[] row, byte[] family, byte[] qualifier, long amount, Durability durability)` | long | 增加列值，指定Durability (default) | Stable |

#### 批量和原子操作

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `batch(List<? extends Row> actions, Object[] results)` | void | 批量执行操作 (default) | Stable |
| `batchCallback(List<? extends Row> actions, Object[] results, Batch.Callback<R> callback)` | void | 批量执行带回调 (default) | Stable |
| `mutateRow(RowMutations rm)` | void | 单行原子多个mutation操作 (default) | Stable |

#### CheckAndMutate操作 (新增)

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `checkAndMutate(byte[] row, byte[] family)` | CheckAndMutateBuilder | 条件更新构建器 (default) | Stable |

**Deprecated方法** (将在3.0.0移除):
| 方法 | 替代方案 |
|------|----------|
| `checkAndPut(...)` | 使用 `checkAndMutate()` |
| `checkAndDelete(...)` | 使用 `checkAndMutate()` |
| `checkAndMutate(...)` (旧版本) | 使用新的Builder模式 |
| `existsAll(List<Get> gets)` | 使用 `exists(List<Get>)` |

#### Coprocessor操作

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `coprocessorService(byte[] row)` | CoprocessorRpcChannel | 获取指定region的RPC通道 (default) | Stable |
| `coprocessorService(Class<T> service, byte[] startKey, byte[] endKey, Batch.Call<T,R> callable)` | Map<byte[],R> | 执行Coprocessor批量调用 (default) | Stable |
| `batchCoprocessorService(...)` | Map<byte[],R> | 批量Coprocessor服务调用 (default) | Stable |

#### 元信息和配置

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `getName()` | TableName | 获取表名 | Stable |
| `getConfiguration()` | Configuration | 获取Configuration对象 | Stable |
| `getDescriptor()` | TableDescriptor | 获取表描述符 | Stable |
| `getTableDescriptor()` | HTableDescriptor | **Deprecated** 使用getDescriptor() | Deprecated |
| `getOperationTimeout()` | int | **Deprecated** 使用新版本 | Deprecated |
| `getOperationTimeout(TimeUnit unit)` | long | 获取操作超时时间 | Stable |
| `getRpcTimeout()` | int | **Deprecated** | Deprecated |
| `getRpcTimeout(TimeUnit unit)` | long | 获取RPC超时 | Stable |
| `getReadRpcTimeout()` | int | **Deprecated** | Deprecated |
| `getReadRpcTimeout(TimeUnit unit)` | long | 获取读RPC超时 | Stable |
| `getWriteRpcTimeout()` | int | **Deprecated** | Deprecated |
| `getWriteRpcTimeout(TimeUnit unit)` | long | 获取写RPC超时 | Stable |

#### 资源管理

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `close()` | void | 关闭Table，释放资源 | Stable |

### 2.2 BufferedMutator 接口
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Public`

用于批量异步写入的接口。

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `getName()` | TableName | 获取表名 | Stable |
| `getConfiguration()` | Configuration | 获取Configuration | Stable |
| `mutate(Mutation mutation)` | void | 发送单个Mutation | Stable |
| `mutate(List<? extends Mutation> mutations)` | void | 发送多个Mutation | Stable |
| `flush()` | void | 刷新缓冲区 | Stable |
| `close()` | void | 关闭并刷新 | Stable |
| `getWriteBufferSize()` | long | 获取写缓冲区大小 | Stable |
| `setRpcTimeout(int timeout)` | void | 设置RPC超时 | Stable |
| `setOperationTimeout(int timeout)` | void | 设置操作超时 | Stable |
| `setWriteBufferPeriodicFlush(long timeoutMs)` | void | 设置周期刷新超时 (default) | Stable |
| `disableWriteBufferPeriodicFlush()` | void | 禁用周期刷新 (default) | Stable |

### 2.3 AsyncTable 接口 (异步版本)
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Evolving`

异步版本的Table接口，支持CompletableFuture返回。

| 主要方法 | 描述 |
|----------|------|
| `get(Get get)` | 返回 `CompletableFuture<Result>` |
| `put(Put put)` | 返回 `CompletableFuture<Void>` |
| `delete(Delete delete)` | 返回 `CompletableFuture<Void>` |
| `append(Append append)` | 返回 `CompletableFuture<Result>` |
| `increment(Increment increment)` | 返回 `CompletableFuture<Result>` |
| `scanAll(Scan scan)` | 返回 `CompletableFuture<List<Result>>` |

---

## 3. Admin API

### 3.1 Admin 接口
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Public`

HBase集群管理接口。

#### 表管理操作 (246个方法)

**创建和删除表**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `createTable(TableDescriptor desc)` | void | 创建表 |
| `createTable(TableDescriptor desc, byte[][] splitKeys)` | void | 创建表，指定split keys |
| `createTable(TableDescriptor desc, byte[] startKey, byte[] endKey, int numRegions)` | void | 创建表，指定region数量 |
| `createTableAsync(TableDescriptor desc)` | Future<Void> | 异步创建表 |
| `createTableAsync(TableDescriptor desc, byte[][] splitKeys)` | Future<Void> | 异步创建表 |
| `deleteTable(TableName tableName)` | void | 删除表 |
| `deleteTables(String regex)` | HTableDescriptor[] | 模式删除多个表 |
| `deleteTables(Pattern pattern)` | HTableDescriptor[] | Pattern删除多个表 |
| `truncateTable(TableName tableName, boolean preserveSplits)` | void | 清空表 |
| `truncateTableAsync(TableName tableName, boolean preserveSplits)` | Future<Void> | 异步清空表 |

**表状态管理**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `enableTable(TableName tableName)` | void | 启用表 |
| `enableTableAsync(TableName tableName)` | Future<Void> | 异步启用表 |
| `disableTable(TableName tableName)` | void | 禁用表 |
| `disableTableAsync(TableName tableName)` | Future<Void> | 异步禁用表 |
| `isTableEnabled(TableName tableName)` | boolean | 表是否启用 |
| `isTableDisabled(TableName tableName)` | boolean | 表是否禁用 |
| `isTableAvailable(TableName tableName)` | boolean | 表是否可用 |

**表修改操作**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `modifyTable(TableDescriptor descriptor)` | void | 修改表 |
| `modifyTableAsync(TableDescriptor descriptor)` | Future<Void> | 异步修改表 |
| `addColumnFamily(TableName tableName, ColumnFamilyDescriptor columnFamily)` | void | 添加列族 |
| `addColumnFamilyAsync(...)` | Future<Void> | 异步添加列族 |
| `deleteColumnFamily(TableName tableName, byte[] columnFamily)` | void | 删除列族 |
| `deleteColumnFamilyAsync(...)` | Future<Void> | 异步删除列族 |
| `modifyColumnFamily(...)` | void | 修改列族 |
| `modifyColumnFamilyAsync(...)` | Future<Void> | 异步修改列族 |

**表信息查询**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `listTables()` | TableDescriptor[] | 列出所有表 |
| `listTables(Pattern pattern)` | TableDescriptor[] | 按Pattern列出表 |
| `listTables(String regex)` | TableDescriptor[] | 按regex列出表 |
| `getTableDescriptor(TableName tableName)` | TableDescriptor | 获取表描述符 |
| `getTableRegions(TableName tableName)` | List<RegionInfo> | 获取表的region列表 |
| `getRegionLocation(TableName tableName, byte[] row)` | RegionLocator | 获取region位置 |

**Namespace管理**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `createNamespace(NamespaceDescriptor descriptor)` | void | 创建namespace |
| `createNamespaceAsync(...)` | Future<Void> | 异步创建namespace |
| `deleteNamespace(String name)` | void | 删除namespace |
| `deleteNamespaceAsync(String name)` | Future<Void> | 异步删除namespace |
| `modifyNamespace(NamespaceDescriptor descriptor)` | void | 修改namespace |
| `getNamespaceDescriptor(String name)` | NamespaceDescriptor | 获取namespace描述 |
| `listNamespaceDescriptors()` | NamespaceDescriptor[] | 列出所有namespace |

**Region管理**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `assign(byte[] regionName)` | void | 分配region |
| `unassign(byte[] regionName, boolean force)` | void | 取消分配region |
| `move(byte[] encodedRegionName, byte[] destServerName)` | void | 移动region |
| `balance()` | boolean | 执行负载均衡 |
| `balance(boolean force)` | boolean | 强制负载均衡 |
| `balancerSwitch(boolean onOrOff, boolean synchronous)` | boolean | 开关负载均衡器 |
| `split(TableName tableName)` | void | 分裂表 |
| `split(TableName tableName, byte[] splitPoint)` | void | 指定位置分裂 |
| `splitRegion(byte[] regionName)` | void | 分裂region |
| `splitRegionAsync(byte[] regionName)` | Future<Void> | 异步分裂 |
| `mergeRegions(byte[] nameA, byte[] nameB, boolean forcible)` | void | 合并regions |
| `compact(TableName tableName)` | void | 压缩表 |
| `compactRegion(byte[] regionName)` | void | 压缩region |
| `majorCompact(TableName tableName)` | void | Major压缩表 |
| `majorCompactRegion(byte[] regionName)` | void | Major压缩region |

**快照管理**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `snapshot(String snapshotName, TableName tableName)` | void | 创建快照 |
| `snapshot(String snapshotName, TableName tableName, SnapshotType type)` | void | 创建指定类型快照 |
| `snapshotAsync(...)` | Future<Void> | 异步创建快照 |
| `deleteSnapshot(String snapshotName)` | void | 删除快照 |
| `deleteSnapshots(Pattern pattern)` | void | Pattern删除快照 |
| `listSnapshots()` | List<SnapshotDescription> | 列出快照 |
| `listSnapshots(Pattern pattern)` | List<SnapshotDescription> | Pattern列出快照 |
| `cloneSnapshot(String snapshotName, TableName tableName)` | void | 从快照克隆表 |
| `cloneSnapshotAsync(...)` | Future<Void> | 异步克隆 |
| `restoreSnapshot(String snapshotName)` | void | 恢复快照 |

**集群信息**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `getClusterMetrics()` | ClusterMetrics | 获取集群状态 |
| `getMaster()` | ServerName | 获取Master信息 |
| `getRegionServers()` | List<ServerName> | 获取RegionServer列表 |
| `getOnlineRegions(ServerName sn)` | List<RegionInfo> | 获取RS上在线region |
| `getMasterCoprocessors()` | String[] | 获取Master Coprocessors |
| `getBalancerEnabled()` | boolean | 获取均衡器状态 |

**Coprocessor管理**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `coprocessorService()` | CoprocessorRpcChannel | Master RPC通道 |
| `coprocessorService(ServerName serverName)` | CoprocessorRpcChannel | RS RPC通道 |

**其他管理**:
| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `abort(String why, Throwable e)` | void | 终止 |
| `isAborted()` | boolean | 是否终止 |
| `close()` | void | 关闭Admin |
| `getConfiguration()` | Configuration | 获取配置 |

**Deprecated方法**:
| 方法 | 替代方案 |
|------|----------|
| `addColumn(...)` | `addColumnFamily()` |
| `deleteColumn(...)` | `deleteColumnFamily()` |
| `closeRegion(...)` | `unassign()` |
| `getClusterStatus()` | `getClusterMetrics()` |
| `balancer()` | `balance()` |
| `abortProcedure(...)` | 移除 |
| `abortProcedureAsync(...)` | 移除 |

---

## 4. Scan Operations

### 4.1 Scan 类
**包**: `org.apache.hadoop.hbase.client`
**继承**: `Query` → `OperationWithAttributes` → `Operation`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `setStartRow(byte[] startRow)` | Scan | 设置起始行 | Stable |
| `setStopRow(byte[] stopRow)` | Scan | 设置结束行 | Stable |
| `setRowPrefixFilter(byte[] rowPrefix)` | Scan | 设置行前缀过滤 | Stable |
| `setFilter(Filter filter)` | Scan | 设置过滤器 | Stable |
| `setMaxVersions(int maxVersions)` | Scan | 设置最大版本数 | Stable |
| `setMaxVersions()` | Scan | 设置返回所有版本 | Stable |
| `setBatch(int batch)` | Scan | 设置批量大小 | Stable |
| `setCaching(int caching)` | Scan | 设置缓存大小 | Stable |
| `setCacheBlocks(boolean cacheBlocks)` | Scan | 设置是否缓存blocks | Stable |
| `setReversed(boolean reversed)` | Scan | 设置反向扫描 | Stable |
| `setRaw(boolean raw)` | Scan | 设置raw扫描 | Stable |
| `readAllColumns()` | Scan | 读取所有列 | Stable |
| `setSmall(boolean small)` | Scan | 设置small扫描 | Stable |
| `setAllowPartialResults(boolean allowPartial)` | Scan | 允许部分结果 | Stable |
| `setAsyncPrefetch(boolean asyncPrefetch)` | Scan | 异步预取 | Stable |
| `setReadType(ReadType readType)` | Scan | 设置读类型 | Stable |
| `setNeedCursorResult(boolean needCursor)` | Scan | 需要cursor结果 | Stable |
| `setLimit(int limit)` | Scan | 设置结果限制 | Stable |
| `setOneRowLimit()` | Scan | 限制一行 | Stable |
| `addColumn(byte[] family, byte[] qualifier)` | Scan | 添加列 | Stable |
| `addFamily(byte[] family)` | Scan | 添加列族 | Stable |
| `setTimeRange(long min, long max)` | Scan | 设置时间范围 | Stable |
| `setTimeStamp(long timestamp)` | Scan | 设置时间戳 | Stable |
| `setColumnFamilyTimeRange(byte[] cf, long min, long max)` | Scan | 列族时间范围 | Stable |
| `setConsistency(Consistency consistency)` | Scan | 设置一致性 | Stable |
| `setIsolationLevel(IsolationLevel level)` | Scan | 设置隔离级别 | Stable |
| `setMaxResultSize(long maxResultSize)` | Scan | 设置最大结果大小 | Stable |
| `getStartRow()` | byte[] | 获取起始行 | Stable |
| `getStopRow()` | byte[] | 获取结束行 | Stable |
| `getTimeRange()` | TimeRange | 获取时间范围 | Stable |
| `getFilter()` | Filter | 获取过滤器 | Stable |
| `getMaxVersions()` | int | 获取最大版本数 | Stable |
| `getBatch()` | int | 获取批量大小 | Stable |
| `getCaching()` | int | 获取缓存大小 | Stable |
| `isReversed()` | boolean | 是否反向扫描 | Stable |
| `getMaxResultSize()` | long | 获取最大结果大小 | Stable |
| `getFamilies()` | Map<byte[],NavigableSet<byte[]>> | 获取列族映射 | Stable |
| `getReadType()` | ReadType | 获取读类型 | Stable |

### 4.2 Get 类
**包**: `org.apache.hadoop.hbase.client`
**继承**: `Query` → `OperationWithAttributes` → `Operation`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `addColumn(byte[] family, byte[] qualifier)` | Get | 添加列 | Stable |
| `addFamily(byte[] family)` | Get | 添加列族 | Stable |
| `setFilter(Filter filter)` | Get | 设置过滤器 | Stable |
| `setMaxVersions(int maxVersions)` | Get | 设置最大版本数 | Stable |
| `setMaxVersions()` | Get | 设置返回所有版本 | Stable |
| `readAllColumns()` | Get | 读取所有列 | Stable |
| `setCheckExists(boolean checkExists)` | Get | 设置检查存在 | Stable |
| `setTimeRange(long min, long max)` | Get | 设置时间范围 | Stable |
| `setTimeStamp(long timestamp)` | Get | 设置时间戳 | Stable |
| `setColumnFamilyTimeRange(byte[] cf, long min, long max)` | Get | 列族时间范围 | Stable |
| `setConsistency(Consistency consistency)` | Get | 设置一致性 | Stable |
| `setIsolationLevel(IsolationLevel level)` | Get | 设置隔离级别 | Stable |
| `getRow()` | byte[] | 获取行键 | Stable |
| `getFilter()` | Filter | 获取过滤器 | Stable |
| `getMaxVersions()` | int | 获取最大版本数 | Stable |
| `getTimeRange()` | TimeRange | 获取时间范围 | Stable |
| `getFamilies()` | Map | 获取列族映射 | Stable |

### 4.3 Result 类
**包**: `org.apache.hadoop.hbase.client`
**实现**: `CellScannable`, `CellScanner`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `rawCells()` | Cell[] | 返回底层Cell数组 | Stable |
| `listCells()` | List<Cell> | 创建排序Cell列表 | Stable |
| `getRow()` | byte[] | 获取行键 | Stable |
| `getValue(byte[] family, byte[] qualifier)` | byte[] | 获取最新版本值 | Stable |
| `getValueAsByteBuffer(...)` | ByteBuffer | 获取ByteBuffer值 | Stable |
| `loadValue(...)` | boolean | 加载值到ByteBuffer | Stable |
| `getColumnCells(byte[] family, byte[] qualifier)` | List<Cell> | 获取列的所有Cell | Stable |
| `getColumnLatestCell(...)` | Cell | 获取列最新Cell | Stable |
| `getFamilyMap(byte[] family)` | NavigableMap<byte[],byte[]> | 获取列族Map | Stable |
| `getMap()` | NavigableMap<byte[],NavigableMap<byte[],NavigableMap<Long,byte[]>>> | 获取完整Map | Stable |
| `getNoVersionMap()` | NavigableMap<byte[],NavigableMap<byte[],byte[]>> | 获取无版本Map | Stable |
| `containsColumn(byte[] family, byte[] qualifier)` | boolean | 是否包含列 | Stable |
| `containsEmptyColumn(...)` | boolean | 是否包含空列 | Stable |
| `containsNonEmptyColumn(...)` | boolean | 是否包含非空列 | Stable |
| `size()` | int | 返回Cell数量 | Stable |
| `isEmpty()` | boolean | 是否空结果 | Stable |
| `isStale()` | boolean | 是否来自过期数据 | Stable |
| `mayHaveMoreCellsInRow()` | boolean | 行可能还有更多Cell | Stable |
| `isCursor()` | boolean | 是否cursor结果 | Stable |
| `getCursor()` | Cursor | 获取cursor | Stable |
| `value()` | byte[] | 返回第一列的值 | Stable |
| `advance()` | boolean | CellScanner前进 | Stable |
| `current()` | Cell | CellScanner当前Cell | Stable |
| `cellScanner()` | CellScanner | 获取CellScanner | Stable |

**静态工厂方法**:
| 方法 | 描述 |
|------|------|
| `create(Cell[] cells)` | 创建Result |
| `create(List<Cell> cells)` | 创建Result |
| `create(Cell[] cells, Boolean exists, boolean stale)` | 创建Result |
| `createCompleteResult(Iterable<Result> partialResults)` | 合并部分结果 |
| `createCursorResult(Cursor cursor)` | 创建cursor结果 |
| `compareResults(Result res1, Result res2)` | 比较两个Result |
| `getTotalSizeOfCells(Result result)` | 获取Cell总大小 |

**Deprecated方法**:
| 方法 | 替代方案 |
|------|----------|
| `isPartial()` | `mayHaveMoreCellsInRow()` |

### 4.4 ResultScanner 接口
**包**: `org.apache.hadoop.hbase.client`
**实现**: `Iterable<Result>`, `Closeable`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `next()` | Result | 获取下一个Result | Stable |
| `next(int nbRows)` | Result[] | 获取多个Result | Stable |
| `close()` | void | 关闭Scanner | Stable |
| `iterator()` | Iterator<Result> | 获取迭代器 | Stable |
| `renewLease()` | boolean | 续租 | Stable |

---

## 5. Put/Delete Operations

### 5.1 Put 类
**包**: `org.apache.hadoop.hbase.client`
**继承**: `Mutation` → `OperationWithAttributes` → `Operation`
**实现**: `HeapSize`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `addColumn(byte[] family, byte[] qualifier, byte[] value)` | Put | 添加列 | Stable |
| `addColumn(byte[] family, byte[] qualifier, long ts, byte[] value)` | Put | 添加列带时间戳 | Stable |
| `addColumn(byte[] family, ByteBuffer qualifier, long ts, ByteBuffer value)` | Put | 添加列ByteBuffer | Stable |
| `add(Cell cell)` | Put | 添加Cell | Stable |
| `addImmutable(byte[] family, byte[] qualifier, byte[] value)` | Put | **Deprecated** 使用add(Cell) | Deprecated |
| `addImmutable(byte[] family, byte[] qualifier, long ts, byte[] value)` | Put | **Deprecated** | Deprecated |
| `setDurability(Durability d)` | Put | 设置Durability | Stable |
| `setTimestamp(long timestamp)` | Put | 设置时间戳 | Stable |
| `setTTL(long ttl)` | Put | 设置TTL | Stable |
| `setACL(Map<String,Permission> perms)` | Put | 设置ACL | Stable |
| `setACL(String user, Permission perms)` | Put | 设置用户ACL | Stable |
| `setCellVisibility(CellVisibility expression)` | Put | 设置可见性 | Stable |
| `setAttribute(String name, byte[] value)` | Put | 设置属性 | Stable |
| `setId(String id)` | Put | 设置ID | Stable |
| `setPriority(int priority)` | Put | 设置优先级 | Stable |
| `getRow()` | byte[] | 获取行键 (继承) | Stable |
| `getFamilyCellMap()` | NavigableMap<byte[],List<Cell>> | 获取列族Cell映射 | Stable |

### 5.2 Delete 类
**包**: `org.apache.hadoop.hbase.client`
**继承**: `Mutation`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `addFamily(byte[] family)` | Delete | 删除整个列族 | Stable |
| `addFamily(byte[] family, long timestamp)` | Delete | 删除列族到指定时间戳 | Stable |
| `addFamilyVersion(byte[] family, long timestamp)` | Delete | 删除指定时间戳的列族 | Stable |
| `addColumns(byte[] family, byte[] qualifier)` | Delete | 删除列所有版本 | Stable |
| `addColumns(byte[] family, byte[] qualifier, long timestamp)` | Delete | 删除列到指定时间戳 | Stable |
| `addColumn(byte[] family, byte[] qualifier)` | Delete | 删除列最新版本 | Stable |
| `addColumn(byte[] family, byte[] qualifier, long timestamp)` | Delete | 删除列指定版本 | Stable |
| `add(Cell cell)` | Delete | 添加删除Cell | Stable |
| `addDeleteMarker(Cell kv)` | Delete | **Deprecated** 使用add(Cell) | Deprecated |
| `setDurability(Durability d)` | Delete | 设置Durability | Stable |
| `setTimestamp(long timestamp)` | Delete | 设置时间戳 | Stable |
| `setTTL(long ttl)` | Delete | 设置TTL | Stable |
| `setACL(...)` | Delete | 设置ACL | Stable |
| `setCellVisibility(...)` | Delete | 设置可见性 | Stable |
| `setAttribute(...)` | Delete | 设置属性 | Stable |

### 5.3 Append 类
**包**: `org.apache.hadoop.hbase.client`
**继承**: `Mutation`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `addColumn(byte[] family, byte[] qualifier, byte[] value)` | Append | 添加列追增值 | Stable |
| `add(Cell cell)` | Append | 添加Cell | Stable |
| `add(byte[] family, byte[] qualifier, byte[] value)` | Append | **Deprecated** 使用addColumn | Deprecated |
| `setTimeRange(long minStamp, long maxStamp)` | Append | 设置时间范围 | Stable |
| `getTimeRange()` | TimeRange | 获取时间范围 | Stable |
| `setReturnResults(boolean returnResults)` | Append | 设置是否返回结果 | Stable |
| `isReturnResults()` | boolean | 是否返回结果 | Stable |
| `setDurability(Durability d)` | Append | 设置Durability | Stable |
| `setTimestamp(long timestamp)` | Append | 设置时间戳 | Stable |

### 5.4 Increment 类
**包**: `org.apache.hadoop.hbase.client`
**继承**: `Mutation`
**稳定性**: `@InterfaceAudience.Public`

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `addColumn(byte[] family, byte[] qualifier, long amount)` | Increment | 增加列值 | Stable |
| `add(Cell cell)` | Increment | 添加Cell | Stable |
| `setTimeRange(long minStamp, long maxStamp)` | Increment | 设置时间范围 | Stable |
| `getTimeRange()` | TimeRange | 获取时间范围 | Stable |
| `setReturnResults(boolean returnResults)` | Increment | 设置是否返回结果 | Stable |
| `isReturnResults()` | boolean | 是否返回结果 | Stable |
| `hasFamilies()` | boolean | 是否有列族 | Stable |
| `numFamilies()` | int | 列族数量 | Stable |
| `getFamilyMapOfLongs()` | Map<byte[],NavigableMap<byte[],Long>> | 获取增量映射 | Stable |
| `setDurability(Durability d)` | Increment | 设置Durability | Stable |

### 5.5 RowMutations 类
**包**: `org.apache.hadoop.hbase.client`
**稳定性**: `@InterfaceAudience.Public`

单行原子多个mutation操作。

| 方法 | 返回类型 | 描述 |
|------|----------|------|
| `add(Put put)` | RowMutations | 添加Put |
| `add(Delete delete)` | RowMutations | 添加Delete |
| `add(Append append)` | RowMutations | 添加Append |
| `add(Increment increment)` | RowMutations | 添加Increment |
| `getRow()` | byte[] | 获取行键 |
| `getMutations()` | List<Mutation> | 获取mutation列表 |

---

## 6. CheckAndMutate API

### 6.1 CheckAndMutateBuilder 接口
**包**: `org.apache.hadoop.hbase.client` (Table内部接口)
**稳定性**: `@InterfaceAudience.Public`

Builder模式用于条件更新操作。

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `qualifier(byte[] qualifier)` | CheckAndMutateBuilder | 设置检查的qualifier | Stable |
| `ifNotExists()` | CheckAndMutateBuilder | 检查不存在 | Stable |
| `ifExists()` | CheckAndMutateBuilder | 检查存在 | Stable |
| `ifEquals(byte[] value)` | CheckAndMutateBuilder | 检查值相等 | Stable |
| `ifNotEquals(byte[] value)` | CheckAndMutateBuilder | 检查值不等 | Stable |
| `ifMatches(CompareOperator op, byte[] value)` | CheckAndMutateBuilder | 条件比较 | Stable |
| `timeRange(TimeRange timeRange)` | CheckAndMutateBuilder | 设置时间范围 | Stable |
| `thenPut(Put put)` | boolean | 条件成立则Put | Stable |
| `thenDelete(Delete delete)` | boolean | 条件成立则Delete | Stable |
| `thenMutate(RowMutations mutations)` | boolean | 条件成立则执行mutations | Stable |

### 6.2 使用示例

```java
// 新的Builder模式 (推荐)
boolean success = table.checkAndMutate(row, family)
    .qualifier(qualifier)
    .ifEquals(expectedValue)
    .thenPut(put);

// 条件删除
boolean success = table.checkAndMutate(row, family)
    .qualifier(qualifier)
    .ifNotExists()
    .thenDelete(delete);

// 条件mutations
boolean success = table.checkAndMutate(row, family)
    .qualifier(qualifier)
    .ifMatches(CompareOperator.GREATER, threshold)
    .thenMutate(rowMutations);
```

---

## 7. 新测试框架

### 7.1 TestingHBaseCluster (替代HBaseTestingUtility)
**包**: `org.apache.hadoop.hbase.testing`
**稳定性**: `@InterfaceAudience.Public`

HBase 4.0.0引入的新测试框架，替代旧的HBaseTestingUtility。

| 方法 | 返回类型 | 描述 | 状态 |
|------|----------|------|------|
| `create()` | TestingHBaseCluster | 创建测试集群 | Stable |
| `start()` | void | 启动集群 | Stable |
| `stop()` | void | 停止集群 | Stable |
| `waitForClusterUp()` | void | 等待集群启动 | Stable |
| `waitForRegionServerReady()` | void | 等待RS就绪 | Stable |
| `getConnection()` | Connection | 获取测试Connection | Stable |
| `getAdmin()` | Admin | 获取测试Admin | Stable |
| `createTable(TableDescriptor desc)` | void | 创建测试表 | Stable |
| `getDataTestDir()` | Path | 获取测试数据目录 | Stable |
| `getMiniHBaseCluster()` | MiniHBaseCluster | 获取Mini集群 | Stable |

### 7.2 HBaseTestingUtility (Deprecated)
**包**: `org.apache.hadoop.hbase.HBaseTestingUtility`
**状态**: `@Deprecated` (将在4.0.0移除)

替代方案: 使用 `TestingHBaseCluster`

---

## 8. 方法数量统计

### 模块统计

| 模块 | 类/接口 | 公共方法数 | Deprecated方法数 |
|------|---------|------------|------------------|
| **Connection API** | Connection | 11+2(继承) | 0 |
| | ConnectionFactory | 5 | 0 |
| | AsyncConnection | 6 | 0 |
| **Table Operations** | Table | 50 | 22 |
| | BufferedMutator | 14 | 0 |
| | AsyncTable | ~15 | 0 |
| **Admin API** | Admin | 246 | 30+ |
| | AsyncAdmin | ~200 | - |
| **Scan Operations** | Scan | 35 | 0 |
| | Get | 18 | 0 |
| | Result | 48 | 1 |
| | ResultScanner | 6 | 0 |
| **Put/Delete** | Put | 18 | 3 |
| | Delete | 20 | 1 |
| | Append | 19 | 1 |
| | Increment | 24 | 2 |
| | RowMutations | 5 | 0 |
| **CheckAndMutate** | CheckAndMutateBuilder | 10 | 0 |
| **Testing** | TestingHBaseCluster | ~10 | 0 |

### 总计

| 类型 | 数量 |
|------|------|
| 接口总数 | 15+ |
| 类总数 | 12+ |
| 公共方法总数 | ~500+ |
| Deprecated方法 | ~60 |

### 稳定性分布

| 标记 | 含义 | 主要类 |
|------|------|--------|
| `@InterfaceAudience.Public` | 公共API，稳定 | Connection, Table, Admin, Put, Get, Scan |
| `@InterfaceAudience.Evolving` | 可能变化 | AsyncTable, AsyncAdmin |
| `@InterfaceAudience.Private` | 内部使用 | 实现类 |

---

## 附录: Enum类

### Durability
写入持久性级别。

| 值 | 描述 |
|----|------|
| `USE_DEFAULT` | 使用默认设置 |
| `SKIP_WAL` | 不写WAL |
| `ASYNC_WAL` | 异步写WAL |
| `SYNC_WAL` | 同步写WAL |
| `FSYNC_WAL` | WAL fsync |

### Consistency
一致性级别。

| 值 | 描述 |
|----|------|
| `STRONG` | 强一致性 |
| `TIMELINE` | 时间线一致性 |

### IsolationLevel
隔离级别。

| 值 | 描述 |
|----|------|
| `READ_UNCOMMITTED` | 读未提交 |
| `READ_COMMITTED` | 读已提交 |

### SnapshotType
快照类型。

| 值 | 描述 |
|----|------|
| `FLUSHED` | Flush后快照 |
| `SKIPFLUSH` | 不Flush快照 |
| `MANIFEST` | Manifest快照 |

---

## 附录: 重要变更说明

1. **Connection管理**: Connection是重量级对象，应创建一次并共享；Table/Admin是轻量级，应每次使用后关闭。

2. **CheckAndMutate**: 2.0.0引入Builder模式，旧版checkAndPut/checkAndDelete已Deprecated。

3. **TableDescriptor**: 替代旧的HTableDescriptor。

4. **异步API**: AsyncTable/AsyncAdmin提供CompletableFuture返回，适合高并发场景。

5. **测试框架**: TestingHBaseCluster替代HBaseTestingUtility，更轻量更易用。

---

*文档结束*