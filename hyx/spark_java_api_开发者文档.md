# Spark Java API 开发者文档

> **文档定位**: 数据源Connector开发者、插件开发者使用的public API
> **普通用户不需要这些API**

---

## 文档说明

本文档包含以下类型的API：

1. **Connector接口**: 实现自定义数据源需要调用
2. **列式存储API**: 向量化执行引擎内部使用
3. **约束定义API**: 元数据约束管理
4. **自定义度量API**: 监控指标定制
5. **内部服务API**: RPC、存储、调度内部实现

---

### TaskStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `TaskStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |



### NioManagedBuffer
**包路径**: `org.apache.spark.network.buffer`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertToNetty` | 无 | `Object` | 转换ToNetty相关功能 | 调用该方法执行转换ToNetty相关功能 |
| `convertToNettyForSsl` | 无 | `Object` | 转换ToNettyForSsl相关功能 | 调用该方法执行转换ToNettyForSsl相关功能 |
| `createInputStream` | 无 | `InputStream` | 创建InputStream相关功能 | 调用该方法执行创建InputStream相关功能 |
| `nioByteBuffer` | 无 | `ByteBuffer` | nioByteBuffer操作 | 调用该方法执行nioByteBuffer操作 |
| `release` | 无 | `ManagedBuffer` | 发布相关功能 | 调用该方法执行发布相关功能 |
| `retain` | 无 | `ManagedBuffer` | retain操作 | 调用该方法执行retain操作 |
| `size` | 无 | `long` | 计算大小 | 调用该方法执行size操作 |



### ColumnarBatch
**包路径**: `org.apache.spark.sql.vectorized`
**说明**: 列式批处理容器，将多个ColumnVector组织为行式表格，提供行视图访问数据。用于向量化执行，大幅提升数据处理效率。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭批处理，释放所有列向量占用的内存资源，数据将不可访问 | `ColumnarBatch batch = ...;<br>try {<br>    // 使用batch处理数据<br>} finally {<br>    batch.close();  // 确保释放内存<br>}` |
| `closeIfFreeable` | 无 | `void` | 如果列向量的资源可被释放，则关闭它们，用于批处理间清理临时内存 | `batch.closeIfFreeable();<br>// 在批处理之间清理可释放资源` |
| `column` | ordinal: int | `ColumnVector` | 获取指定列索引位置的列向量对象，ordinal从0开始 | `ColumnVector col0 = batch.column(0);<br>ColumnVector col1 = batch.column(1);<br>// 访问各列数据` |
| `getRow` | rowId: int | `InternalRow` | 获取指定行号的内行对象，返回的行对象在多次调用间会被复用 | `InternalRow row = batch.getRow(0);<br>int value = row.getInt(0);<br>// 注意：row对象会被复用，不要跨调用保存` |
| `hasNext` | 无 | `boolean` | 检查行迭代器是否还有更多行可遍历（需先调用rowIterator获取迭代器） | `Iterator&lt;InternalRow&gt; iter = batch.rowIterator();<br>while (iter.hasNext()) {<br>    InternalRow row = iter.next();<br>    // 处理每行数据<br>}` |
| `next` | 无 | `InternalRow` | 获取行迭代器的下一行数据（需先调用rowIterator获取迭代器） | `Iterator&lt;InternalRow&gt; iter = batch.rowIterator();<br>while (iter.hasNext()) {<br>    InternalRow row = iter.next();<br>}` |
| `numCols` | 无 | `int` | 返回批处理中的列数量 | `int cols = batch.numCols();<br>System.out.println("列数: " + cols);` |
| `numRows` | 无 | `int` | 返回批处理中的行数量（包括被过滤的行） | `int rows = batch.numRows();<br>System.out.println("行数: " + rows);` |
| `rowIterator` | 无 | `Iterator&lt;InternalRow&gt;` | 返回行迭代器，用于按行遍历批处理中的所有数据 | `Iterator&lt;InternalRow&gt; iter = batch.rowIterator();<br>while (iter.hasNext()) {<br>    InternalRow row = iter.next();<br>    // 按行处理数据<br>}` |
| `setNumRows` | numRows: int | `void` | 设置批处理的行数量，用于动态调整批处理大小 | `batch.setNumRows(100);<br>// 设置批处理包含100行` |


### NumericHistogram
**包路径**: `org.apache.spark.sql.util`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | v: double | `void` | 添加元素 | 传入参数执行添加相关功能 |
| `addBin` | x: double, y: double, b: int | `void` | 添加二进制数据 | 传入参数执行添加二进制数据 |
| `allocate` | num_bins: int | `void` | 分配相关功能 | 传入参数执行分配相关功能 |
| `compareTo` | other: Coord | `int` | 比较To相关功能 | 传入参数执行比较To相关功能 |
| `getBin` | b: int | `Coord` | 获取Bin相关功能 | 传入参数执行获取Bin相关功能 |
| `getNumBins` | 无 | `int` | 获取NumBins相关功能 | 调用该方法执行获取NumBins相关功能 |
| `getUsedBins` | 无 | `int` | 获取UsedBins相关功能 | 调用该方法执行获取UsedBins相关功能 |
| `isReady` | 无 | `boolean` | 判断是否Ready相关功能 | 调用该方法执行判断是否Ready相关功能 |
| `merge` | other: NumericHistogram | `void` | 合并相关功能 | 传入参数执行合并相关功能 |
| `reset` | 无 | `void` | 重置相关功能 | 调用该方法执行重置相关功能 |
| `setUsedBins` | nusedBins: int | `void` | 设置UsedBins相关功能 | 传入参数执行设置UsedBins相关功能 |


### CustomAvgMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `aggregateTaskMetrics` | taskMetrics: long&lt;&gt; | `String` | 聚合任务级别的度量指标 | 聚合任务度量指标为字符串 |



### ThreadFactoryWithGarbageCleanup
**包路径**: `org.apache.hive.service.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getThreadRawStoreMap` | 无 | `Map&lt;Long, RawStore&gt;` | 获取ThreadRawStoreMap相关功能 | 调用该方法执行获取ThreadRawStoreMap相关功能 |
| `newThread` | runnable: Runnable | `Thread` | 读取相关功能 | 传入参数执行读取相关功能 |



### instead
**包路径**: `org.apache.spark.api.java`
**方法数量**: 48

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkpoint` | 无 | `Unit` | checkpoint DStream | 调用该方法执行检查point相关功能 |
| `collect` | 无 | `JList` | 收集所有行 | // collect：将RDD收集到Driver端<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"));<br>List<String> list = rdd.collect();<br>// 注意：collect会将所有数据拉回Driver<br>// 数据量大时可能导致Driver内存溢出，慎用！ |
| `collectAsync` | 无 | `JavaFutureAction` | 收集Async相关功能 | 调用该方法执行收集Async相关功能 |
| `collectPartitions` | Array[Int]: partitionIds | `Array` | 收集Partitions相关功能 | 传入参数执行收集Partitions相关功能 |
| `count` | 无 | `Long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `countApprox` | timeout: Long, confidence: Double | `PartialResult` | 计数Approx相关功能 | 传入参数执行计数Approx相关功能 |
| `countApprox` | timeout: Long | `PartialResult` | 计数Approx相关功能 | 传入参数执行计数Approx相关功能 |
| `countApproxDistinct` | relativeSD: Double | `Long` | 计数ApproxDistinct相关功能 | 传入参数执行计数ApproxDistinct相关功能 |
| `countAsync` | 无 | `JavaFutureAction` | 计数Async相关功能 | 调用该方法执行计数Async相关功能 |
| `countByValue` | 无 | `JMap` | 统计每个批次每个值的出现次数 | 调用该方法执行计数ByValue相关功能 |
| `countByValueApprox` | timeout: Long, confidence: Double | `PartialResult` | 计数ByValueApprox相关功能 | 传入参数执行计数ByValueApprox相关功能 |
| `countByValueApprox` | timeout: Long | `PartialResult` | 计数ByValueApprox相关功能 | 传入参数执行计数ByValueApprox相关功能 |
| `first` | 无 | `T` | 第一行 | // first：获取第一个元素<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(10, 20, 30));<br>Integer first = rdd.first();<br>// 结果: 10 |
| `flatMapToDouble` | DoubleFlatMapFunction[T]: f | `JavaDoubleRDD` | 映射相关功能 | 传入参数执行映射相关功能 |
| `fold` | T: zeroValue | `Unit` | 使用零值和组合函数聚合RDD | 传入参数执行折叠/归约相关功能 |
| `foreach` | VoidFunction[T]: f | `Unit` | 对每个元素应用函数，用于副作用操作 | // foreach：对每个元素执行操作（副作用）<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"));<br>// 打印每个元素（在Executor上执行）<br>rdd.foreach(x -> System.out.println("Element: " + x));<br>// 写入外部系统<br>rdd.foreach(x -> {<br>    // 写入数据库、发送消息等<br>    database.insert(x);<br>}); |
| `foreachAsync` | VoidFunction[T]: f | `JavaFutureAction` | 检查是否存在相关功能 | 传入参数执行检查是否存在相关功能 |
| `foreachPartition` | VoidFunction[JIterator[T]]: f | `Unit` | 对每个分区应用函数 | 传入参数执行foreachPartition操作 |
| `foreachPartitionAsync` | VoidFunction[JIterator[T]]: f | `JavaFutureAction` | foreachPartitionAsync操作 | 传入参数执行foreachPartitionAsync操作 |
| `getCheckpointFile` | 无 | `Optional` | 获取CheckpointFile相关功能 | 调用该方法执行获取CheckpointFile相关功能 |
| `glom` | 无 | `JavaRDD` | glom操作 | 调用该方法执行glom操作 |
| `isEmpty` | 无 | `Boolean` | 判断是否为空 | 调用该方法执行判断是否Empty相关功能 |
| `iterator` | Partition: split, TaskContext: taskContext | `JIterator` | 获取迭代器 | 传入参数执行时期相关功能 |
| `mapPartitionsToDouble` | DoubleFlatMapFunction[JIterator[T]]: f | `JavaDoubleRDD` | 映射PartitionsToDouble相关功能 | 传入参数执行映射PartitionsToDouble相关功能 |
| `mapPartitionsToDouble` | DoubleFlatMapFunction[JIterator[T]]: f, preservesPartitioning: Boolean | `JavaDoubleRDD` | 映射PartitionsToDouble相关功能 | 传入参数执行映射PartitionsToDouble相关功能 |
| `max` | Comparator[T]: comp | `T` | 最大值 | // max：最大值<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));<br>double max = doubleRDD.max();<br>// 结果: 30.0 |
| `min` | Comparator[T]: comp | `T` | 最小值 | // min：最小值<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));<br>double min = doubleRDD.min();<br>// 结果: 5.0 |
| `pipe` | command: String | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | JList[String]: command | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | JList[String]: command, JMap[String: env | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | JList[String]: command, JMap[String: env, separateWorkingDir: Boolean, bufferSize: Int | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | JList[String]: command, JMap[String: env, separateWorkingDir: Boolean, bufferSize: Int, encoding: String | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `reduce` | JFunction2[T: f | `T` | 聚合DStream每个RDD | // reduce：聚合所有元素为单个结果<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));<br>// 求和<br>Integer sum = numbers.reduce((a, b) -> a + b);<br>// 结果: 15<br>// 求最大值<br>Integer max = numbers.reduce((a, b) -> Math.max(a, b));<br>// 结果: 5<br>// 字符串拼接<br>JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));<br>String concatenated = words.reduce((a, b) -> a + b);<br>// 结果: "abc" |
| `saveAsObjectFile` | path: String | `Unit` | 保存RDD为序列化对象文件 | 传入参数执行保存AsObjectFile相关功能 |
| `saveAsTextFile` | path: String | `Unit` | 保存RDD为文本文件 | // saveAsTextFile：保存为文本文件<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("line1", "line2", "line3"));<br>rdd.saveAsTextFile("hdfs://output/path/");<br>// 输出目录下会有多个文件：part-00000, part-00001... |
| `saveAsTextFile` | path: String, CompressionCodec]: codec | `Unit` | 保存RDD为文本文件 | // saveAsTextFile：保存为文本文件<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("line1", "line2", "line3"));<br>rdd.saveAsTextFile("hdfs://output/path/");<br>// 输出目录下会有多个文件：part-00000, part-00001... |
| `take` | num: Int | `JList` | 取前n行 | // take：获取前n个元素<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>List<Integer> top5 = rdd.take(5);<br>// 结果: [1, 2, 3, 4, 5] |
| `takeAsync` | num: Int | `JavaFutureAction` | 获取Async相关功能 | 传入参数执行获取Async相关功能 |
| `takeOrdered` | num: Int, Comparator[T]: comp | `JList` | 返回排序后的前n个元素 | 传入参数执行获取Ordered相关功能 |
| `takeOrdered` | num: Int | `JList` | 返回排序后的前n个元素 | 传入参数执行获取Ordered相关功能 |
| `toDebugString` | 无 | `String` | 调试相关功能 | 调用该方法执行调试相关功能 |
| `toLocalIterator` | 无 | `JIterator` | 本地相关功能 | 调用该方法执行本地相关功能 |
| `top` | num: Int, Comparator[T]: comp | `JList` | 返回最大的n个元素 | 传入参数执行顶部相关功能 |
| `top` | num: Int | `JList` | 返回最大的n个元素 | 传入参数执行顶部相关功能 |
| `treeReduce` | JFunction2[T: f, depth: Int | `T` | 减少相关功能 | 传入参数执行减少相关功能 |
| `treeReduce` | JFunction2[T: f | `T` | 减少相关功能 | 传入参数执行减少相关功能 |
| `zipWithIndex` | 无 | `JavaPairRDD` | zipWithIndex操作 | 调用该方法执行zipWithIndex操作 |
| `zipWithUniqueId` | 无 | `JavaPairRDD` | 唯一相关功能 | 调用该方法执行唯一相关功能 |


### ThriftCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 34

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `CancelDelegationToken` | req: TCancelDelegationTokenReq | `TCancelDelegationTokenResp` | 判断能否celDelegationToken相关功能 | 传入参数执行判断能否celDelegationToken相关功能 |
| `CancelOperation` | req: TCancelOperationReq | `TCancelOperationResp` | 判断能否celOperation相关功能 | 传入参数执行判断能否celOperation相关功能 |
| `CloseOperation` | req: TCloseOperationReq | `TCloseOperationResp` | 关闭Operation相关功能 | 传入参数执行关闭Operation相关功能 |
| `CloseSession` | req: TCloseSessionReq | `TCloseSessionResp` | 关闭Session相关功能 | 传入参数执行关闭Session相关功能 |
| `DownloadData` | req: TDownloadDataReq | `TDownloadDataResp` | 执行wnloadData相关功能 | 传入参数执行执行wnloadData相关功能 |
| `ExecuteStatement` | req: TExecuteStatementReq | `TExecuteStatementResp` | 执行Statement相关功能 | 传入参数执行执行Statement相关功能 |
| `FetchResults` | req: TFetchResultsReq | `TFetchResultsResp` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `GetCatalogs` | req: TGetCatalogsReq | `TGetCatalogsResp` | 获取Catalogs相关功能 | 传入参数执行获取Catalogs相关功能 |
| `GetColumns` | req: TGetColumnsReq | `TGetColumnsResp` | 获取Columns相关功能 | 传入参数执行获取Columns相关功能 |
| `GetCrossReference` | req: TGetCrossReferenceReq | `TGetCrossReferenceResp` | 获取CrossReference相关功能 | 传入参数执行获取CrossReference相关功能 |
| `GetDelegationToken` | req: TGetDelegationTokenReq | `TGetDelegationTokenResp` | 获取DelegationToken相关功能 | 传入参数执行获取DelegationToken相关功能 |
| `GetFunctions` | req: TGetFunctionsReq | `TGetFunctionsResp` | 获取Functions相关功能 | 传入参数执行获取Functions相关功能 |
| `GetInfo` | req: TGetInfoReq | `TGetInfoResp` | 获取Info相关功能 | 传入参数执行获取Info相关功能 |
| `GetOperationStatus` | req: TGetOperationStatusReq | `TGetOperationStatusResp` | 获取OperationStatus相关功能 | 传入参数执行获取OperationStatus相关功能 |
| `GetPrimaryKeys` | req: TGetPrimaryKeysReq | `TGetPrimaryKeysResp` | 获取PrimaryKeys相关功能 | 传入参数执行获取PrimaryKeys相关功能 |
| `GetQueryId` | req: TGetQueryIdReq | `TGetQueryIdResp` | 获取QueryId相关功能 | 传入参数执行获取QueryId相关功能 |
| `GetResultSetMetadata` | req: TGetResultSetMetadataReq | `TGetResultSetMetadataResp` | 获取ResultSetMetadata相关功能 | 传入参数执行获取ResultSetMetadata相关功能 |
| `GetSchemas` | req: TGetSchemasReq | `TGetSchemasResp` | 获取Schemas相关功能 | 传入参数执行获取Schemas相关功能 |
| `GetTableTypes` | req: TGetTableTypesReq | `TGetTableTypesResp` | 获取TableTypes相关功能 | 传入参数执行获取TableTypes相关功能 |
| `GetTables` | req: TGetTablesReq | `TGetTablesResp` | 获取Tables相关功能 | 传入参数执行获取Tables相关功能 |
| `GetTypeInfo` | req: TGetTypeInfoReq | `TGetTypeInfoResp` | 获取TypeInfo相关功能 | 传入参数执行获取TypeInfo相关功能 |
| `OpenSession` | req: TOpenSessionReq | `TOpenSessionResp` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |
| `RenewDelegationToken` | req: TRenewDelegationTokenReq | `TRenewDelegationTokenResp` | RenewDelegationToken操作 | 传入参数执行RenewDelegationToken操作 |
| `SetClientInfo` | req: TSetClientInfoReq | `TSetClientInfoResp` | 设置ClientInfo相关功能 | 传入参数执行设置ClientInfo相关功能 |
| `UploadData` | req: TUploadDataReq | `TUploadDataResp` | 向上loadData相关功能 | 传入参数执行向上loadData相关功能 |
| `createContext` | input: TProtocol, output: TProtocol | `ServerContext` | 创建Context相关功能 | 传入参数执行创建Context相关功能 |
| `deleteContext` | serverContext: ServerContext, input: TProtocol, output: TProtocol | `void` | 删除请求Context相关功能 | 传入参数执行删除请求Context相关功能 |
| `getPortNumber` | 无 | `int` | 获取PortNumber相关功能 | 调用该方法执行获取PortNumber相关功能 |
| `getServerIPAddress` | 无 | `InetAddress` | 获取ServerIPAddress相关功能 | 调用该方法执行获取ServerIPAddress相关功能 |
| `getSessionHandle` | 无 | `SessionHandle` | 获取SessionHandle相关功能 | 调用该方法执行获取SessionHandle相关功能 |
| `isWrapperFor` | aClass: Class<?> | `boolean` | 判断是否WrapperFor相关功能 | 传入参数执行判断是否WrapperFor相关功能 |
| `preServe` | 无 | `void` | 前Serve相关功能 | 调用该方法执行前Serve相关功能 |
| `processContext` | serverContext: ServerContext, input: TTransport, output: TTransport | `void` | 处理Context相关功能 | 传入参数执行处理Context相关功能 |
| `setSessionHandle` | sessionHandle: SessionHandle | `void` | 设置SessionHandle相关功能 | 传入参数执行设置SessionHandle相关功能 |



### LocalDiskShuffleMapOutputWriter
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `abort` | error: Throwable | `void` | 中止操作 | 传入参数执行中止操作 |
| `channel` | 无 | `WritableByteChannel` | channel操作 | 调用该方法执行channel操作 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `commitAllPartitions` | checksums: long&lt;&gt; | `MapOutputCommitMessage` | commitAllPartitions操作 | 传入参数执行commitAllPartitions操作 |
| `getCount` | 无 | `long` | 获取Count相关功能 | 调用该方法执行获取Count相关功能 |
| `getNumBytesWritten` | 无 | `long` | 获取NumBytesWritten相关功能 | 调用该方法执行获取NumBytesWritten相关功能 |
| `getPartitionWriter` | reducePartitionId: int | `ShufflePartitionWriter` | 获取PartitionWriter相关功能 | 传入参数执行获取PartitionWriter相关功能 |
| `openChannelWrapper` | 无 | `Optional&lt;WritableByteChannelWrapper&gt;` | 打开ChannelWrapper相关功能 | 调用该方法执行打开ChannelWrapper相关功能 |
| `openStream` | 无 | `OutputStream` | 打开Stream相关功能 | 调用该方法执行打开Stream相关功能 |
| `write` | b: int | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | buf: byte&lt;&gt;, pos: int, length: int | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### GeneralScalarExpression
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |


### InProcessLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `startApplication` | listeners: SparkAppHandle.Listener... | `SparkAppHandle` | 启动Application相关功能 | 传入参数执行启动Application相关功能 |


### TransportResponseHandler
**包路径**: `org.apache.spark.network.client`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addFetchRequest` | streamChunkId: StreamChunkId, callback: ChunkReceivedCallback | `void` | 添加数据获取请求 | 传入参数执行添加数据获取请求 |
| `addRpcRequest` | requestId: long, callback: BaseResponseCallback | `void` | 添加RPC请求 | 传入参数执行添加RPC请求 |
| `addStreamCallback` | streamId: String, callback: StreamCallback | `void` | 添加流回调 | 传入参数执行添加流回调 |
| `channelActive` | 无 | `void` | 活跃相关功能 | 调用该方法执行活跃相关功能 |
| `channelInactive` | 无 | `void` | 活跃相关功能 | 调用该方法执行活跃相关功能 |
| `deactivateStream` | 无 | `void` | deactivateStream操作 | 调用该方法执行deactivateStream操作 |
| `exceptionCaught` | cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `getTimeOfLastRequestNs` | 无 | `long` | 获取TimeOfLastRequestNs相关功能 | 调用该方法执行获取TimeOfLastRequestNs相关功能 |
| `handle` | message: ResponseMessage | `void` | 处理相关功能 | 传入参数执行处理相关功能 |
| `hasOutstandingRequests` | 无 | `Boolean` | 检查是否存在OutstandingRequests相关功能 | 调用该方法执行检查是否存在OutstandingRequests相关功能 |
| `numOutstandingRequests` | 无 | `int` | 请求相关功能 | 调用该方法执行请求相关功能 |
| `removeFetchRequest` | streamChunkId: StreamChunkId | `void` | 移除FetchRequest相关功能 | 传入参数执行移除FetchRequest相关功能 |
| `removeRpcRequest` | requestId: long | `void` | 移除RpcRequest相关功能 | 传入参数执行移除RpcRequest相关功能 |
| `updateTimeOfLastRequest` | 无 | `void` | 更新TimeOfLastRequest相关功能 | 调用该方法执行更新TimeOfLastRequest相关功能 |



### DiagnoseCorruption
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `DiagnoseCorruption` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### RemoveShuffleMerge
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RemoveShuffleMerge` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### RowSetFactory
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | schema: TableSchema, version: TProtocolVersion, isBlobBased: boolean | `RowSet` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | results: TRowSet, version: TProtocolVersion | `RowSet` | 创建相关功能 | 传入参数执行创建相关功能 |



### ForeignKey
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `ForeignKey` | 构建约束对象 | 构建Check约束对象 |
| `referencedTable` | 无 | `Identifier` | 引用encedTable相关功能 | 调用该方法执行引用encedTable相关功能 |



### GetArrayItem
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `childArray` | 无 | `Expression` | 子级Array相关功能 | 调用该方法执行子级Array相关功能 |
| `failOnError` | 无 | `boolean` | failOnError操作 | 调用该方法执行failOnError操作 |
| `ordinal` | 无 | `Expression` | ordinal操作 | 调用该方法执行ordinal操作 |


### GetFunctionsOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### ApplicationStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `ApplicationStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |



### OpenBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `OpenBlocks` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### GcmTransportCipher
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addToChannel` | ch: Channel | `void` | 添加到通道 | 传入参数执行添加到通道 |
| `channelRead` | ctx: ChannelHandlerContext, ciphertextMessage: Object | `void` | 读取相关功能 | 传入参数执行读取相关功能 |
| `count` | 无 | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `getKeyId` | 无 | `String` | 获取KeyId相关功能 | 调用该方法执行获取KeyId相关功能 |
| `position` | 无 | `long` | position操作 | 调用该方法执行position操作 |
| `release` | decrement: int | `boolean` | 发布相关功能 | 传入参数执行发布相关功能 |
| `retain` | increment: int | `GcmEncryptedMessage` | retain操作 | 传入参数执行retain操作 |
| `touch` | o: Object | `GcmEncryptedMessage` | touch操作 | 传入参数执行touch操作 |
| `transferTo` | target: WritableByteChannel, position: long | `long` | 转移To相关功能 | 传入参数执行转移To相关功能 |
| `transferred` | 无 | `long` | 转移red相关功能 | 调用该方法执行转移red相关功能 |
| `write` | ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### IdentityColumnSpec
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStart` | 无 | `long` | 获取Start相关功能 | 调用该方法执行获取Start相关功能 |
| `getStep` | 无 | `long` | 获取Step相关功能 | 调用该方法执行获取Step相关功能 |
| `isAllowExplicitInsert` | 无 | `boolean` | 判断是否AllowExplicitInsert相关功能 | 调用该方法执行判断是否AllowExplicitInsert相关功能 |



### BlockTransferMessage
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromByteBuffer` | msg: ByteBuffer | `BlockTransferMessage` | fromByteBuffer操作 | 传入参数执行fromByteBuffer操作 |
| `id` | 无 | `byte` | id操作 | 调用该方法执行id操作 |
| `toByteBuffer` | 无 | `ByteBuffer` | toByteBuffer操作 | 调用该方法执行toByteBuffer操作 |



### LocalDirsForExecutors
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `LocalDirsForExecutors` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `getLocalDirsByExec` | 无 | `Map&lt;String, String[]&gt;` | 获取LocalDirsByExec相关功能 | 调用该方法执行获取LocalDirsByExec相关功能 |



### AbstractFetchShuffleBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### SaslRpcHandler
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `doAuthChallenge` | client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback | `boolean` | 执行AuthChallenge相关功能 | 传入参数执行执行AuthChallenge相关功能 |



### TransportChannelHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acceptInboundMessage` | msg: Object | `boolean` | 接受入站消息 | 传入参数执行接受入站消息 |
| `channelActive` | ctx: ChannelHandlerContext | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `channelInactive` | ctx: ChannelHandlerContext | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `channelRead0` | ctx: ChannelHandlerContext, request: Message | `void` | 读取相关功能 | 传入参数执行读取相关功能 |
| `channelRegistered` | ctx: ChannelHandlerContext | `void` | 注册相关功能 | 传入参数执行注册相关功能 |
| `channelUnregistered` | ctx: ChannelHandlerContext | `void` | 注册相关功能 | 传入参数执行注册相关功能 |
| `exceptionCaught` | ctx: ChannelHandlerContext, cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `getClient` | 无 | `TransportClient` | 获取Client相关功能 | 调用该方法执行获取Client相关功能 |
| `getRequestHandler` | 无 | `TransportRequestHandler` | 获取RequestHandler相关功能 | 调用该方法执行获取RequestHandler相关功能 |
| `getResponseHandler` | 无 | `TransportResponseHandler` | 获取ResponseHandler相关功能 | 调用该方法执行获取ResponseHandler相关功能 |
| `userEventTriggered` | ctx: ChannelHandlerContext, evt: Object | `void` | 触发相关功能 | 传入参数执行触发相关功能 |



### MergedBlockMetaRequest
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `MergedBlockMetaRequest` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `type` | 无 | `Type` | type操作 | 调用该方法执行type操作 |



### CompositeService
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getServices` | 无 | `Collection&lt;Service&gt;` | 获取Services相关功能 | 调用该方法执行获取Services相关功能 |
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |



### ByteArrayReadableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `feedData` | buf: ByteBuf | `void` | feedData操作 | 传入参数执行feedData操作 |
| `isOpen` | 无 | `boolean` | 判断是否Open相关功能 | 调用该方法执行判断是否Open相关功能 |
| `read` | dst: ByteBuffer | `int` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |



### ParentClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `findClass` | name: String | `Class&lt;?&gt;` | 查找Class相关功能 | 传入参数执行查找Class相关功能 |
| `loadClass` | name: String, resolve: boolean | `Class&lt;?&gt;` | 加载Class相关功能 | 传入参数执行加载Class相关功能 |



### Distributions
**包路径**: `org.apache.spark.sql.connector.distributions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `clustered` | clustering: Expression&lt;&gt; | `ClusteredDistribution` | 创建聚类分布 | 创建聚类分布对象 |
| `ordered` | ordering: SortOrder&lt;&gt; | `OrderedDistribution` | 创建有序分布 | 传入参数执行创建有序分布 |
| `unspecified` | 无 | `UnspecifiedDistribution` | 创建未指定分布 | 调用该方法执行创建未指定分布 |


### Expressions
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | name: String, args: Expression... | `Transform` | 应用数据类型转换 | 获取数据类型对应的列向量 |
| `bucket` | numBuckets: int, columns: String... | `Transform` | 创建分桶分区转换 | 传入参数执行创建分桶分区转换 |
| `column` | name: String | `NamedReference` | 创建列引用表达式 | 传入参数执行创建列引用表达式 |
| `days` | column: String | `Transform` | 将日期转换为天数 | 传入参数执行将日期转换为天数 |
| `hours` | column: String | `Transform` | 将时间转换为小时数 | 传入参数执行将时间转换为小时数 |
| `identity` | column: String | `Transform` | 创建身份分区转换 | 传入参数执行创建身份分区转换 |
| `months` | column: String | `Transform` | 将日期转换为月份数 | 传入参数执行将日期转换为月份数 |
| `sort` | expr: Expression, direction: SortDirection, nullOrder: NullOrdering | `SortOrder` | 排序 | 传入参数执行创建排序表达式 |
| `sort` | expr: Expression, direction: SortDirection | `SortOrder` | 排序 | 传入参数执行创建排序表达式 |
| `years` | column: String | `Transform` | 年份转换相关功能 | 传入参数执行年份转换相关功能 |



### ShuffleSecretManager
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSaslUser` | appId: String | `String` | 获取SaslUser相关功能 | 传入参数执行获取SaslUser相关功能 |
| `getSecretKey` | appId: String | `String` | 获取SecretKey相关功能 | 传入参数执行获取SecretKey相关功能 |
| `registerApp` | appId: String, shuffleSecret: String | `void` | 注册App相关功能 | 传入参数执行注册App相关功能 |
| `registerApp` | appId: String, shuffleSecret: ByteBuffer | `void` | 注册App相关功能 | 传入参数执行注册App相关功能 |
| `unregisterApp` | appId: String | `void` | 取消注册App相关功能 | 传入参数执行取消注册App相关功能 |



### EncryptedMessageWithHeader
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `isEndOfInput` | 无 | `boolean` | 判断是否EndOfInput相关功能 | 调用该方法执行判断是否EndOfInput相关功能 |
| `length` | 无 | `long` | 计算长度 | 调用该方法执行length操作 |
| `progress` | 无 | `long` | progress操作 | 调用该方法执行progress操作 |
| `readChunk` | ctx: ChannelHandlerContext | `ByteBuf` | 读取Chunk相关功能 | 传入参数执行读取Chunk相关功能 |
| `readChunk` | allocator: ByteBufAllocator | `ByteBuf` | 读取Chunk相关功能 | 传入参数执行读取Chunk相关功能 |



### GetSchemasOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### SparkAppHandle
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isFinal` | 无 | `boolean` | 判断是否Final相关功能 | 调用该方法执行判断是否Final相关功能 |


### AbstractLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addAppArgs` | args: String... | `T` | 添加应用参数 | 传入参数执行添加应用参数 |
| `addFile` | file: String | `T` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业<br>sc.addFile("hdfs://path/to/config.txt");<br>sc.addFile("s3://bucket/data.json");<br>// 在Executor中访问文件<br>String filePath = SparkFiles.get("config.txt"); |
| `addJar` | jar: String | `T` | 添加JAR包到Spark作业 | // 添加依赖JAR包<br>sc.addJar("hdfs://path/to/dependency.jar");<br>sc.addJar("/local/path/to/lib.jar"); |
| `addPyFile` | file: String | `T` | 添加Python文件 | 传入参数执行添加Python文件 |
| `addSparkArg` | arg: String | `T` | 添加Spark参数 | 传入参数执行添加Spark参数 |
| `addSparkArg` | name: String, value: String | `T` | 添加Spark参数 | 传入参数执行添加Spark参数 |
| `setAppName` | appName: String | `T` | 设置AppName相关功能 | 传入参数执行设置AppName相关功能 |
| `setAppResource` | resource: String | `T` | 设置AppResource相关功能 | 传入参数执行设置AppResource相关功能 |
| `setConf` | key: String, value: String | `T` | 设置Conf相关功能 | 传入参数执行设置Conf相关功能 |
| `setDeployMode` | mode: String | `T` | 设置DeployMode相关功能 | 传入参数执行设置DeployMode相关功能 |
| `setMainClass` | mainClass: String | `T` | 设置MainClass相关功能 | 传入参数执行设置MainClass相关功能 |
| `setMaster` | master: String | `T` | 设置Master相关功能 | 传入参数执行设置Master相关功能 |
| `setPropertiesFile` | path: String | `T` | 设置PropertiesFile相关功能 | 传入参数执行设置PropertiesFile相关功能 |
| `setRemote` | remote: String | `T` | 设置Remote相关功能 | 传入参数执行设置Remote相关功能 |
| `setVerbose` | verbose: boolean | `T` | 设置Verbose相关功能 | 传入参数执行设置Verbose相关功能 |



### GetLocalDirsForExecutors
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `GetLocalDirsForExecutors` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### KeyGroupedPartitioning
**包路径**: `org.apache.spark.sql.connector.read.partitioning`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `numPartitions` | 无 | `int` | numPartitions操作 | 调用该方法执行numPartitions操作 |



### NettyManagedBuffer
**包路径**: `org.apache.spark.network.buffer`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertToNetty` | 无 | `Object` | 转换ToNetty相关功能 | 调用该方法执行转换ToNetty相关功能 |
| `convertToNettyForSsl` | 无 | `Object` | 转换ToNettyForSsl相关功能 | 调用该方法执行转换ToNettyForSsl相关功能 |
| `createInputStream` | 无 | `InputStream` | 创建InputStream相关功能 | 调用该方法执行创建InputStream相关功能 |
| `nioByteBuffer` | 无 | `ByteBuffer` | nioByteBuffer操作 | 调用该方法执行nioByteBuffer操作 |
| `release` | 无 | `ManagedBuffer` | 发布相关功能 | 调用该方法执行发布相关功能 |
| `retain` | 无 | `ManagedBuffer` | retain操作 | 调用该方法执行retain操作 |
| `size` | 无 | `long` | 计算大小 | 调用该方法执行size操作 |



### HiveTableTypeMapping
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeNames` | 无 | `Set&lt;String&gt;` | 获取TableTypeNames相关功能 | 调用该方法执行获取TableTypeNames相关功能 |
| `mapToClientType` | hiveTypeName: String | `String` | 映射ToClientType相关功能 | 传入参数执行映射ToClientType相关功能 |



### TransportServer
**包路径**: `org.apache.spark.network.server`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getAllMetrics` | 无 | `MetricSet` | 获取AllMetrics相关功能 | 调用该方法执行获取AllMetrics相关功能 |
| `getPort` | 无 | `int` | 获取Port相关功能 | 调用该方法执行获取Port相关功能 |
| `getRegisteredConnections` | 无 | `Counter` | 获取RegisteredConnections相关功能 | 调用该方法执行获取RegisteredConnections相关功能 |



### FetchShuffleBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FetchShuffleBlocks` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `getNumBlocks` | 无 | `int` | 获取NumBlocks相关功能 | 调用该方法执行获取NumBlocks相关功能 |



### Message
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `Type` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `id` | 无 | `byte` | id操作 | 调用该方法执行id操作 |



### CodePointIteratorType
**包路径**: `org.apache.spark.unsafe.types`
**方法数量**: 91

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `binaryCompare` | other: final UTF8String | `int` | 二进制数据比较 | 传入参数执行二进制数据比较 |
| `binaryEquals` | other: final UTF8String | `boolean` | 二进制数据相等判断 | 传入参数执行二进制数据相等判断 |
| `blankString` | length: int | `UTF8String` | 生成空白字符串 | 传入参数执行生成空白字符串 |
| `bytePosToChar` | bytePos: int | `int` | 后相关功能 | 传入参数执行后相关功能 |
| `charPosToByte` | charPos: int | `int` | 后相关功能 | 传入参数执行后相关功能 |
| `clone` | 无 | `UTF8String` | 克隆对象 | 调用该方法执行克隆相关功能 |
| `codePointFrom` | byteIndex: int | `int` | 指向相关功能 | 传入参数执行指向相关功能 |
| `codePointIterator` | 无 | `Iterator&lt;Integer&gt;` | 指向相关功能 | 调用该方法执行指向相关功能 |
| `codePointIterator` | iteratorMode: CodePointIteratorType | `Iterator&lt;Integer&gt;` | 指向相关功能 | 传入参数执行指向相关功能 |
| `compareTo` | other: @Nonnull final UTF8String | `int` | 比较To相关功能 | 传入参数执行比较To相关功能 |
| `concat` | inputs: UTF8String... | `UTF8String` | 拼接字符串 | 传入参数执行concat操作 |
| `concatWs` | separator: UTF8String, inputs: UTF8String... | `UTF8String` | concatWs操作 | 传入参数执行concatWs操作 |
| `contains` | substring: final UTF8String | `boolean` | 判断是否包含 | 传入参数执行包含相关功能 |
| `copy` | 无 | `UTF8String` | 复制相关功能 | 调用该方法执行复制相关功能 |
| `copyUTF8String` | start: int, end: int | `UTF8String` | 复制UTF8String相关功能 | 传入参数执行复制UTF8String相关功能 |
| `endsWith` | suffix: final UTF8String | `boolean` | 判断是否以指定字符串结尾 | 传入参数执行结束sWith相关功能 |
| `find` | str: UTF8String, start: int | `int` | 在哈希表中查找指定key的位置，返回索引 | 在哈希表中查找指定key，返回索引位置 |
| `findInSet` | match: UTF8String | `int` | 在集合字符串中查找匹配项位置 | 在逗号分隔集合中查找元素位置 |
| `fromAddress` | base: Object, offset: long, numBytes: int | `UTF8String` | 添加相关功能 | 传入参数执行添加相关功能 |
| `fromBytes` | bytes: byte&lt;&gt; | `UTF8String` | fromBytes操作 | 传入参数执行fromBytes操作 |
| `fromBytes` | bytes: byte&lt;&gt;, offset: int, numBytes: int | `UTF8String` | fromBytes操作 | 传入参数执行fromBytes操作 |
| `fromString` | str: String | `UTF8String` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |
| `getBaseObject` | 无 | `Object` | 获取BaseObject相关功能 | 调用该方法执行获取BaseObject相关功能 |
| `getBaseOffset` | 无 | `long` | 获取BaseOffset相关功能 | 调用该方法执行获取BaseOffset相关功能 |
| `getByte` | byteIndex: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getByteBuffer` | 无 | `ByteBuffer` | 获取ByteBuffer相关功能 | 调用该方法执行获取ByteBuffer相关功能 |
| `getChar` | charIndex: int | `int` | 获取Char相关功能 | 传入参数执行获取Char相关功能 |
| `getPrefix` | 无 | `long` | 获取Prefix相关功能 | 调用该方法执行获取Prefix相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `indexOf` | v: UTF8String, start: int | `int` | 查找子串在字符串中的起始位置 | 查找子串起始位置，支持指定起始索引 |
| `indexOfEmpty` | start: int | `int` | indexOfEmpty操作 | 传入参数执行indexOfEmpty操作 |
| `isFullAscii` | 无 | `boolean` | 判断是否FullAscii相关功能 | 调用该方法执行判断是否FullAscii相关功能 |
| `isValid` | 无 | `boolean` | 判断是否Valid相关功能 | 调用该方法执行判断是否Valid相关功能 |
| `isWhitespaceOrISOControl` | codePoint: int | `boolean` | 判断是否WhitespaceOrISOControl相关功能 | 传入参数执行判断是否WhitespaceOrISOControl相关功能 |
| `levenshteinDistance` | other: UTF8String | `int` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `levenshteinDistance` | other: UTF8String, threshold: int | `int` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `lpad` | len: int, pad: UTF8String | `UTF8String` | lpad操作 | 传入参数执行lpad操作 |
| `makeValid` | 无 | `UTF8String` | 创建Valid相关功能 | 调用该方法执行创建Valid相关功能 |
| `matchAt` | s: final UTF8String, pos: int | `boolean` | matchAt操作 | 传入参数执行matchAt操作 |
| `next` | 无 | `Integer` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `numBytes` | 无 | `int` | numBytes操作 | 调用该方法执行numBytes操作 |
| `numBytesForFirstByte` | b: final byte | `int` | 第一个相关功能 | 传入参数执行第一个相关功能 |
| `numChars` | 无 | `int` | numChars操作 | 调用该方法执行numChars操作 |
| `read` | kryo: Kryo, in: Input | `void` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |
| `readExternal` | in: ObjectInput | `void` | 读取External相关功能 | 传入参数执行读取External相关功能 |
| `repeat` | times: int | `UTF8String` | 重复相关功能 | 传入参数执行重复相关功能 |
| `replace` | search: UTF8String, replace: UTF8String | `UTF8String` | 替换字符串中匹配的内容 | 替换字符串中匹配内容 |
| `reverse` | 无 | `UTF8String` | reverse操作 | 调用该方法执行reverse操作 |
| `reverseCodePointIterator` | 无 | `Iterator&lt;Integer&gt;` | 指向相关功能 | 调用该方法执行指向相关功能 |
| `reverseCodePointIterator` | iteratorMode: CodePointIteratorType | `Iterator&lt;Integer&gt;` | 指向相关功能 | 传入参数执行指向相关功能 |
| `rfind` | str: UTF8String, start: int | `int` | 查找相关功能 | 传入参数执行查找相关功能 |
| `rpad` | len: int, pad: UTF8String | `UTF8String` | rpad操作 | 传入参数执行rpad操作 |
| `semanticCompare` | other: final UTF8String, collationId: int | `int` | 比较相关功能 | 传入参数执行比较相关功能 |
| `semanticEquals` | other: final UTF8String, collationId: int | `boolean` | 判断相等相关功能 | 传入参数执行判断相等相关功能 |
| `soundex` | 无 | `UTF8String` | soundex操作 | 调用该方法执行soundex操作 |
| `startsWith` | prefix: final UTF8String | `boolean` | 判断是否以指定字符串开头 | 传入参数执行启动sWith相关功能 |
| `subStringIndex` | delim: UTF8String, count: int | `UTF8String` | 查找分隔符分隔的子串索引 | 按分隔符查找第N个子串 |
| `substring` | start: final int, until: final int | `UTF8String` | 截取子字符串 | 传入参数执行子string相关功能 |
| `substringSQL` | pos: int, length: int | `UTF8String` | 子stringSQL相关功能 | 传入参数执行子stringSQL相关功能 |
| `toBinaryString` | val: long | `UTF8String` | 双相关功能 | 传入参数执行双相关功能 |
| `toByte` | intWrapper: IntWrapper | `boolean` | toByte操作 | 传入参数执行toByte操作 |
| `toByteExact` | 无 | `byte` | 艾相关功能 | 调用该方法执行艾相关功能 |
| `toInt` | intWrapper: IntWrapper | `boolean` | toInt操作 | 传入参数执行toInt操作 |
| `toIntExact` | 无 | `int` | 艾相关功能 | 调用该方法执行艾相关功能 |
| `toLong` | toLongResult: LongWrapper | `boolean` | toLong操作 | 传入参数执行toLong操作 |
| `toLongExact` | 无 | `long` | 艾相关功能 | 调用该方法执行艾相关功能 |
| `toLowerCase` | 无 | `UTF8String` | 转换为小写 | 转换为小写字符串 |
| `toLowerCaseAscii` | 无 | `UTF8String` | toLowerCaseAscii操作 | 调用该方法执行toLowerCaseAscii操作 |
| `toShort` | intWrapper: IntWrapper | `boolean` | toShort操作 | 传入参数执行toShort操作 |
| `toShortExact` | 无 | `short` | 艾相关功能 | 调用该方法执行艾相关功能 |
| `toTitleCase` | 无 | `UTF8String` | 转换为标题大小写 | 转换为标题大小写（首字母大写） |
| `toTitleCaseICU` | 无 | `UTF8String` | 使用ICU库转换为标题大小写 | ICU库标题大小写转换 |
| `toUpperCase` | 无 | `UTF8String` | 转换为大写 | 转换为大写字符串 |
| `toUpperCaseAscii` | 无 | `UTF8String` | 向上相关功能 | 调用该方法执行向上相关功能 |
| `toValidString` | 无 | `String` | 有效相关功能 | 调用该方法执行有效相关功能 |
| `translate` | dict: String> | `UTF8String` | 字符映射转换 | 按字符映射表转换字符串 |
| `trim` | 无 | `UTF8String` | 去除空白 | 去除字符串两端空白 |
| `trim` | trimString: UTF8String | `UTF8String` | 去除空白 | 去除字符串两端空白 |
| `trimAll` | 无 | `UTF8String` | 三mAll相关功能 | 调用该方法执行三mAll相关功能 |
| `trimLeft` | 无 | `UTF8String` | 去除字符串左侧空白 | 去除字符串左侧空白 |
| `trimLeft` | trimString: UTF8String | `UTF8String` | 去除字符串左侧空白 | 去除字符串左侧空白 |
| `trimRight` | 无 | `UTF8String` | 去除字符串右侧空白 | 去除字符串右侧空白 |
| `trimRight` | trimString: UTF8String | `UTF8String` | 去除字符串右侧空白 | 去除字符串右侧空白 |
| `trimTrailingSpaces` | numSpaces: int | `UTF8String` | 三mTrailingSpaces相关功能 | 传入参数执行三mTrailingSpaces相关功能 |
| `write` | kryo: Kryo, out: Output | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `writeExternal` | out: ObjectOutput | `void` | 写入External相关功能 | 传入参数执行写入External相关功能 |
| `writeTo` | buffer: ByteBuffer | `void` | 写入To相关功能 | 传入参数执行写入To相关功能 |
| `writeTo` | out: OutputStream | `void` | 写入To相关功能 | 传入参数执行写入To相关功能 |
| `writeToMemory` | target: Object, targetOffset: long | `void` | 写入ToMemory相关功能 | 传入参数执行写入ToMemory相关功能 |



### VariantShreddingWriter
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `castShredded` | v: Variant, schema: VariantSchema, builder: ShreddedResultBuilder | `ShreddedResult` | castShredded操作 | 传入参数执行castShredded操作 |



### HiveSQLException
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toCause` | details: List<String> | `Throwable` | toCause操作 | 传入参数执行toCause操作 |
| `toTStatus` | 无 | `TStatus` | toTStatus操作 | 调用该方法执行toTStatus操作 |
| `toTStatus` | e: Exception | `TStatus` | toTStatus操作 | 传入参数执行toTStatus操作 |



### GetTableTypesOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### HiveSessionProxy
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getProxy` | hiveSession: HiveSession, ugi: UserGroupInformation | `HiveSession` | 获取Proxy相关功能 | 传入参数执行获取Proxy相关功能 |
| `invoke` | arg0: Object, method: final Method, args: final Object&lt;&gt; | `Object` | 调用相关功能 | 传入参数执行调用相关功能 |



### UnsafeShuffleWriter
**包路径**: `org.apache.spark.shuffle.sort`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channel` | 无 | `WritableByteChannel` | channel操作 | 调用该方法执行channel操作 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getPeakMemoryUsedBytes` | 无 | `long` | 获取PeakMemoryUsedBytes相关功能 | 调用该方法执行获取PeakMemoryUsedBytes相关功能 |
| `stop` | success: boolean | `Option&lt;MapStatus&gt;` | 停止SparkContext，释放资源 | 传入参数执行停止相关功能 |
| `write` | records: V>> | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### SessionManager
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `clearIpAddress` | 无 | `void` | 清除IpAddress相关功能 | 调用该方法执行清除IpAddress相关功能 |
| `clearProxyUserName` | 无 | `void` | 清除ProxyUserName相关功能 | 调用该方法执行清除ProxyUserName相关功能 |
| `clearUserName` | 无 | `void` | 清除UserName相关功能 | 调用该方法执行清除UserName相关功能 |
| `closeSession` | sessionHandle: SessionHandle | `void` | 关闭Session相关功能 | 传入参数执行关闭Session相关功能 |
| `getIpAddress` | 无 | `String` | 获取IpAddress相关功能 | 调用该方法执行获取IpAddress相关功能 |
| `getOpenSessionCount` | 无 | `int` | 获取OpenSessionCount相关功能 | 调用该方法执行获取OpenSessionCount相关功能 |
| `getOperationManager` | 无 | `OperationManager` | 获取OperationManager相关功能 | 调用该方法执行获取OperationManager相关功能 |
| `getProxyUserName` | 无 | `String` | 获取ProxyUserName相关功能 | 调用该方法执行获取ProxyUserName相关功能 |
| `getSession` | sessionHandle: SessionHandle | `HiveSession` | 获取Session相关功能 | 传入参数执行获取Session相关功能 |
| `getUserName` | 无 | `String` | 获取UserName相关功能 | 调用该方法执行获取UserName相关功能 |
| `openSession` | protocol: TProtocolVersion, username: String, password: String, ipAddress: String, sessionConf: String> | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |
| `openSession` | protocol: TProtocolVersion, username: String, password: String, ipAddress: String, sessionConf: String>, withImpersonation: boolean, delegationToken: String | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |
| `setIpAddress` | ipAddress: String | `void` | 设置IpAddress相关功能 | 传入参数执行设置IpAddress相关功能 |
| `setProxyUserName` | userName: String | `void` | 设置ProxyUserName相关功能 | 传入参数执行设置ProxyUserName相关功能 |
| `setUserName` | userName: String | `void` | 设置UserName相关功能 | 传入参数执行设置UserName相关功能 |
| `submitBackgroundOperation` | r: Runnable | `Future&lt;?&gt;` | 子mitBackgroundOperation相关功能 | 传入参数执行子mitBackgroundOperation相关功能 |


### ErrorHandler
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `shouldLogError` | t: Throwable | `boolean` | 判断是否应该LogError相关功能 | 传入参数执行判断是否应该LogError相关功能 |
| `shouldRetryError` | t: Throwable | `boolean` | 判断是否应该RetryError相关功能 | 传入参数执行判断是否应该RetryError相关功能 |



### AbstractAuthRpcHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelActive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `channelInactive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `exceptionCaught` | cause: Throwable, client: TransportClient | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `getMergedBlockMetaReqHandler` | 无 | `MergedBlockMetaReqHandler` | 获取MergedBlockMetaReqHandler相关功能 | 调用该方法执行获取MergedBlockMetaReqHandler相关功能 |
| `getStreamManager` | 无 | `StreamManager` | 获取StreamManager相关功能 | 调用该方法执行获取StreamManager相关功能 |
| `isAuthenticated` | 无 | `boolean` | 判断是否Authenticated相关功能 | 调用该方法执行判断是否Authenticated相关功能 |
| `receive` | client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback | `void` | 接收相关功能 | 传入参数执行接收相关功能 |
| `receive` | client: TransportClient, message: ByteBuffer | `void` | 接收相关功能 | 传入参数执行接收相关功能 |
| `receiveStream` | client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback | `StreamCallbackWithID` | 接收Stream相关功能 | 传入参数执行接收Stream相关功能 |



### Check
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `Check` | 构建约束对象 | 构建Check约束对象 |
| `predicate` | 无 | `Predicate` | 获取或设置断言条件 | 获取或设置断言条件 |
| `predicate` | predicate: Predicate | `Builder` | 获取或设置断言条件 | 获取或设置断言条件 |
| `predicateSql` | 无 | `String` | 获取或设置断言SQL表达式 | 获取或设置断言SQL表达式 |
| `predicateSql` | predicateSql: String | `Builder` | 获取或设置断言SQL表达式 | 获取或设置断言SQL表达式 |


### ColumnValue
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toColumnValue` | value: TColumnValue | `Object` | 列相关功能 | 传入参数执行列相关功能 |
| `toTColumnValue` | typeDescriptor: TypeDescriptor, value: Object | `TColumnValue` | 列相关功能 | 传入参数执行列相关功能 |



### TypeDescriptor
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getColumnSize` | 无 | `Integer` | 获取ColumnSize相关功能 | 调用该方法执行获取ColumnSize相关功能 |
| `getDecimalDigits` | 无 | `Integer` | 获取DecimalDigits相关功能 | 调用该方法执行获取DecimalDigits相关功能 |
| `getPrecision` | 无 | `Integer` | 获取Precision相关功能 | 调用该方法执行获取Precision相关功能 |
| `getType` | 无 | `Type` | 获取Type相关功能 | 调用该方法执行获取Type相关功能 |
| `getTypeName` | 无 | `String` | 获取TypeName相关功能 | 调用该方法执行获取TypeName相关功能 |
| `getTypeQualifiers` | 无 | `TypeQualifiers` | 获取TypeQualifiers相关功能 | 调用该方法执行获取TypeQualifiers相关功能 |
| `setTypeQualifiers` | typeQualifiers: TypeQualifiers | `void` | 设置TypeQualifiers相关功能 | 传入参数执行设置TypeQualifiers相关功能 |
| `toTTypeDesc` | 无 | `TTypeDesc` | toTTypeDesc操作 | 调用该方法执行toTTypeDesc操作 |



### AvroCompressionCodec
**包路径**: `org.apache.spark.sql.avro`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | s: String | `AvroCompressionCodec` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |
| `getCodecName` | 无 | `String` | 获取压缩编解码器的名称 | 返回压缩编解码器名称（如"snappy"、"deflate"） |
| `getSupportCompressionLevel` | 无 | `boolean` | 检查是否支持压缩级别配置 | 检查编解码器是否支持自定义压缩级别 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |


### ByteArrayMethods
**包路径**: `org.apache.spark.unsafe.array`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `arrayEquals` | leftBase: Object, leftOffset: long, rightBase: Object, rightOffset: long, length: final long | `boolean` | 数组相等判断 | 传入参数执行数组相等判断 |
| `contains` | arr: byte&lt;&gt;, sub: byte&lt;&gt; | `boolean` | 判断是否包含 | 传入参数执行包含相关功能 |
| `endsWith` | array: byte&lt;&gt;, target: byte&lt;&gt; | `boolean` | 判断是否以指定字符串结尾 | 传入参数执行结束sWith相关功能 |
| `matchAt` | arr: byte&lt;&gt;, sub: byte&lt;&gt;, pos: int | `boolean` | matchAt操作 | 传入参数执行matchAt操作 |
| `nextPowerOf2` | num: long | `long` | 之后PowerOf2相关功能 | 传入参数执行之后PowerOf2相关功能 |
| `roundNumberOfBytesToNearestWord` | numBytes: int | `int` | roundNumberOfBytesToNearestWord操作 | 传入参数执行roundNumberOfBytesToNearestWord操作 |
| `roundNumberOfBytesToNearestWord` | numBytes: long | `long` | roundNumberOfBytesToNearestWord操作 | 传入参数执行roundNumberOfBytesToNearestWord操作 |
| `startsWith` | array: byte&lt;&gt;, target: byte&lt;&gt; | `boolean` | 判断是否以指定字符串开头 | 传入参数执行启动sWith相关功能 |



### Unique
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `Unique` | 构建约束对象 | 构建Check约束对象 |



### VariantUtil
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 18

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `arrayHeader` | largeSize: boolean, offsetSize: int | `byte` | 获取数组头部信息 | 传入参数执行获取数组头部信息 |
| `getBoolean` | value: byte&lt;&gt;, pos: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getDecimal` | value: byte&lt;&gt;, pos: int | `BigDecimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDecimalWithOriginalScale` | value: byte&lt;&gt;, pos: int | `BigDecimal` | 获取DecimalWithOriginalScale相关功能 | 传入参数执行获取DecimalWithOriginalScale相关功能 |
| `getDouble` | value: byte&lt;&gt;, pos: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | value: byte&lt;&gt;, pos: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getLong` | value: byte&lt;&gt;, pos: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMetadataKey` | metadata: byte&lt;&gt;, id: int | `String` | 获取MetadataKey相关功能 | 传入参数执行获取MetadataKey相关功能 |
| `getString` | value: byte&lt;&gt;, pos: int | `String` | 获取String相关功能 | 传入参数执行获取String相关功能 |
| `getType` | value: byte&lt;&gt;, pos: int | `Type` | 获取Type相关功能 | 传入参数执行获取Type相关功能 |
| `getTypeInfo` | value: byte&lt;&gt;, pos: int | `int` | 获取TypeInfo相关功能 | 传入参数执行获取TypeInfo相关功能 |
| `getUuid` | value: byte&lt;&gt;, pos: int | `UUID` | 获取Uuid相关功能 | 传入参数执行获取Uuid相关功能 |
| `objectHeader` | largeSize: boolean, idSize: int, offsetSize: int | `byte` | 头部请求相关功能 | 传入参数执行头部请求相关功能 |
| `primitiveHeader` | type: int | `byte` | 头部请求相关功能 | 传入参数执行头部请求相关功能 |
| `readUnsigned` | bytes: byte&lt;&gt;, pos: int, numBytes: int | `int` | 读取Unsigned相关功能 | 传入参数执行读取Unsigned相关功能 |
| `shortStrHeader` | size: int | `byte` | 头部请求相关功能 | 传入参数执行头部请求相关功能 |
| `valueSize` | value: byte&lt;&gt;, pos: int | `int` | valueSize操作 | 传入参数执行valueSize操作 |
| `writeLong` | bytes: byte&lt;&gt;, pos: int, value: long, numBytes: int | `void` | 写入Long相关功能 | 传入参数执行写入Long相关功能 |



### TransportClient
**包路径**: `org.apache.spark.network.client`
**方法数量**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `fetchChunk` | streamId: long, chunkIndex: int, callback: ChunkReceivedCallback | `void` | 获取Chunk相关功能 | 传入参数执行获取Chunk相关功能 |
| `getChannel` | 无 | `Channel` | 获取Channel相关功能 | 调用该方法执行获取Channel相关功能 |
| `getClientId` | 无 | `String` | 获取ClientId相关功能 | 调用该方法执行获取ClientId相关功能 |
| `getHandler` | 无 | `TransportResponseHandler` | 获取Handler相关功能 | 调用该方法执行获取Handler相关功能 |
| `getSocketAddress` | 无 | `SocketAddress` | 获取SocketAddress相关功能 | 调用该方法执行获取SocketAddress相关功能 |
| `isActive` | 无 | `boolean` | 判断是否Active相关功能 | 调用该方法执行判断是否Active相关功能 |
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `operationComplete` | future: Future<? super Void> | `void` | 完成相关功能 | 传入参数执行完成相关功能 |
| `removeRpcRequest` | requestId: long | `void` | 移除RpcRequest相关功能 | 传入参数执行移除RpcRequest相关功能 |
| `send` | message: ByteBuffer | `void` | 发送相关功能 | 传入参数执行发送相关功能 |
| `sendMergedBlockMetaReq` | appId: String, shuffleId: int, shuffleMergeId: int, reduceId: int, callback: MergedBlockMetaResponseCallback | `void` | 发送MergedBlockMetaReq相关功能 | 传入参数执行发送MergedBlockMetaReq相关功能 |
| `sendRpc` | message: ByteBuffer, callback: RpcResponseCallback | `long` | 发送Rpc相关功能 | 传入参数执行发送Rpc相关功能 |
| `sendRpcSync` | message: ByteBuffer, timeoutMs: long | `ByteBuffer` | 发送RpcSync相关功能 | 传入参数执行发送RpcSync相关功能 |
| `setClientId` | id: String | `void` | 设置ClientId相关功能 | 传入参数执行设置ClientId相关功能 |
| `stream` | streamId: String, callback: StreamCallback | `void` | stream操作 | 传入参数执行stream操作 |
| `timeOut` | 无 | `void` | 超时相关功能 | 调用该方法执行超时相关功能 |
| `uploadStream` | meta: ManagedBuffer, data: ManagedBuffer, callback: RpcResponseCallback | `long` | 向上loadStream相关功能 | 传入参数执行向上loadStream相关功能 |



### Handle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getHandleIdentifier` | 无 | `HandleIdentifier` | 获取HandleIdentifier相关功能 | 调用该方法执行获取HandleIdentifier相关功能 |



### SaslQOP
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `SaslQOP` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |



### UserDefinedScalarFunc
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `canonicalName` | 无 | `String` | 判断能否onicalName相关功能 | 调用该方法执行判断能否onicalName相关功能 |
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |



### LevelDBIterator
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `seek` | key: byte&lt;&gt; | `void` | 定位相关功能 | 传入参数执行定位相关功能 |



### MessageWithHeader
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `count` | 无 | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `position` | 无 | `long` | position操作 | 调用该方法执行position操作 |
| `release` | decrement: int | `boolean` | 发布相关功能 | 传入参数执行发布相关功能 |
| `retain` | increment: int | `MessageWithHeader` | retain操作 | 传入参数执行retain操作 |
| `touch` | o: Object | `MessageWithHeader` | touch操作 | 传入参数执行touch操作 |
| `transferTo` | target: final WritableByteChannel, position: final long | `long` | 转移To相关功能 | 传入参数执行转移To相关功能 |
| `transferred` | 无 | `long` | 转移red相关功能 | 调用该方法执行转移red相关功能 |



### PlainSaslServer
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createSaslServer` | mechanism: String, protocol: String, serverName: String, props: ?>, cbh: CallbackHandler | `SaslServer` | 创建SaslServer相关功能 | 传入参数执行创建SaslServer相关功能 |
| `dispose` | 无 | `void` | 释放相关功能 | 调用该方法执行释放相关功能 |
| `getAuthorizationID` | 无 | `String` | 获取AuthorizationID相关功能 | 调用该方法执行获取AuthorizationID相关功能 |
| `getMechanismName` | 无 | `String` | 获取MechanismName相关功能 | 调用该方法执行获取MechanismName相关功能 |
| `getNegotiatedProperty` | propName: String | `Object` | 获取NegotiatedProperty相关功能 | 传入参数执行获取NegotiatedProperty相关功能 |
| `isComplete` | 无 | `boolean` | 判断是否Complete相关功能 | 调用该方法执行判断是否Complete相关功能 |



### TransportContext
**包路径**: `org.apache.spark.network`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `createClientFactory` | bootstraps: List<TransportClientBootstrap> | `TransportClientFactory` | 创建ClientFactory相关功能 | 传入参数执行创建ClientFactory相关功能 |
| `createClientFactory` | 无 | `TransportClientFactory` | 创建ClientFactory相关功能 | 调用该方法执行创建ClientFactory相关功能 |
| `createServer` | port: int, bootstraps: List<TransportServerBootstrap> | `TransportServer` | 创建Server相关功能 | 传入参数执行创建Server相关功能 |
| `createServer` | host: String, port: int, bootstraps: List<TransportServerBootstrap> | `TransportServer` | 创建Server相关功能 | 传入参数执行创建Server相关功能 |
| `createServer` | bootstraps: List<TransportServerBootstrap> | `TransportServer` | 创建Server相关功能 | 传入参数执行创建Server相关功能 |
| `createServer` | 无 | `TransportServer` | 创建Server相关功能 | 调用该方法执行创建Server相关功能 |
| `getConf` | 无 | `TransportConf` | 获取Conf相关功能 | 调用该方法执行获取Conf相关功能 |
| `getRegisteredConnections` | 无 | `Counter` | 获取RegisteredConnections相关功能 | 调用该方法执行获取RegisteredConnections相关功能 |
| `initializePipeline` | channel: SocketChannel, isClient: boolean | `TransportChannelHandler` | 初始化ializePipeline相关功能 | 传入参数执行初始化ializePipeline相关功能 |
| `initializePipeline` | channel: SocketChannel, channelRpcHandler: RpcHandler, isClient: boolean | `TransportChannelHandler` | 初始化ializePipeline相关功能 | 传入参数执行初始化ializePipeline相关功能 |
| `sslEncryptionEnabled` | 无 | `boolean` | 启用相关功能 | 调用该方法执行启用相关功能 |



### PushBlockStream
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `PushBlockStream` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### StageStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `StageStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |



### TransportFrameDecoder
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | ctx: ChannelHandlerContext | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `channelRead` | ctx: ChannelHandlerContext, data: Object | `void` | 读取相关功能 | 传入参数执行读取相关功能 |
| `exceptionCaught` | ctx: ChannelHandlerContext, cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `handlerRemoved` | ctx: ChannelHandlerContext | `void` | 处理rRemoved相关功能 | 传入参数执行处理rRemoved相关功能 |
| `setInterceptor` | interceptor: Interceptor | `void` | 设置Interceptor相关功能 | 传入参数执行设置Interceptor相关功能 |



### SparkFirehoseListener
**包路径**: `org.apache.spark`
**方法数量**: 36

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onApplicationEnd` | applicationEnd: SparkListenerApplicationEnd | `void` | 结束相关功能 | 传入参数执行结束相关功能 |
| `onApplicationStart` | applicationStart: SparkListenerApplicationStart | `void` | 启动相关功能 | 传入参数执行启动相关功能 |
| `onBlockManagerAdded` | blockManagerAdded: SparkListenerBlockManagerAdded | `void` | 添加相关功能 | 传入参数执行添加相关功能 |
| `onBlockManagerRemoved` | blockManagerRemoved: SparkListenerBlockManagerRemoved | `void` | 移除相关功能 | 传入参数执行移除相关功能 |
| `onBlockUpdated` | blockUpdated: SparkListenerBlockUpdated | `void` | 更新相关功能 | 传入参数执行更新相关功能 |
| `onEnvironmentUpdate` | environmentUpdate: SparkListenerEnvironmentUpdate | `void` | 更新相关功能 | 传入参数执行更新相关功能 |
| `onEvent` | event: SparkListenerEvent | `void` | onEvent操作 | 传入参数执行onEvent操作 |
| `onExecutorAdded` | executorAdded: SparkListenerExecutorAdded | `void` | 添加相关功能 | 传入参数执行添加相关功能 |
| `onExecutorBlacklisted` | executorBlacklisted: SparkListenerExecutorBlacklisted | `void` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `onExecutorBlacklistedForStage` | executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage | `void` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `onExecutorExcluded` | executorExcluded: SparkListenerExecutorExcluded | `void` | onExecutorExcluded操作 | 传入参数执行onExecutorExcluded操作 |
| `onExecutorExcludedForStage` | executorExcludedForStage: SparkListenerExecutorExcludedForStage | `void` | 年龄相关功能 | 传入参数执行年龄相关功能 |
| `onExecutorMetricsUpdate` | executorMetricsUpdate: SparkListenerExecutorMetricsUpdate | `void` | 更新相关功能 | 传入参数执行更新相关功能 |
| `onExecutorRemoved` | executorRemoved: SparkListenerExecutorRemoved | `void` | 移除相关功能 | 传入参数执行移除相关功能 |
| `onExecutorUnblacklisted` | executorUnblacklisted: SparkListenerExecutorUnblacklisted | `void` | 运行相关功能 | 传入参数执行运行相关功能 |
| `onExecutorUnexcluded` | executorUnexcluded: SparkListenerExecutorUnexcluded | `void` | 运行相关功能 | 传入参数执行运行相关功能 |
| `onJobEnd` | jobEnd: SparkListenerJobEnd | `void` | 结束相关功能 | 传入参数执行结束相关功能 |
| `onJobStart` | jobStart: SparkListenerJobStart | `void` | 启动相关功能 | 传入参数执行启动相关功能 |
| `onNodeBlacklisted` | nodeBlacklisted: SparkListenerNodeBlacklisted | `void` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `onNodeBlacklistedForStage` | nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage | `void` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `onNodeExcluded` | nodeExcluded: SparkListenerNodeExcluded | `void` | onNodeExcluded操作 | 传入参数执行onNodeExcluded操作 |
| `onNodeExcludedForStage` | nodeExcludedForStage: SparkListenerNodeExcludedForStage | `void` | 年龄相关功能 | 传入参数执行年龄相关功能 |
| `onNodeUnblacklisted` | nodeUnblacklisted: SparkListenerNodeUnblacklisted | `void` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `onNodeUnexcluded` | nodeUnexcluded: SparkListenerNodeUnexcluded | `void` | onNodeUnexcluded操作 | 传入参数执行onNodeUnexcluded操作 |
| `onOtherEvent` | event: SparkListenerEvent | `void` | onOtherEvent操作 | 传入参数执行onOtherEvent操作 |
| `onResourceProfileAdded` | event: SparkListenerResourceProfileAdded | `void` | 添加相关功能 | 传入参数执行添加相关功能 |
| `onSpeculativeTaskSubmitted` | speculativeTask: SparkListenerSpeculativeTaskSubmitted | `void` | 子相关功能 | 传入参数执行子相关功能 |
| `onStageCompleted` | stageCompleted: SparkListenerStageCompleted | `void` | 完成相关功能 | 传入参数执行完成相关功能 |
| `onStageExecutorMetrics` | executorMetrics: SparkListenerStageExecutorMetrics | `void` | 年龄相关功能 | 传入参数执行年龄相关功能 |
| `onStageSubmitted` | stageSubmitted: SparkListenerStageSubmitted | `void` | 子相关功能 | 传入参数执行子相关功能 |
| `onTaskEnd` | taskEnd: SparkListenerTaskEnd | `void` | 结束相关功能 | 传入参数执行结束相关功能 |
| `onTaskGettingResult` | taskGettingResult: SparkListenerTaskGettingResult | `void` | 获取相关功能 | 传入参数执行获取相关功能 |
| `onTaskStart` | taskStart: SparkListenerTaskStart | `void` | 启动相关功能 | 传入参数执行启动相关功能 |
| `onUnpersistRDD` | unpersistRDD: SparkListenerUnpersistRDD | `void` | 持久化相关功能 | 传入参数执行持久化相关功能 |
| `onUnschedulableTaskSetAdded` | unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded | `void` | 设置相关功能 | 传入参数执行设置相关功能 |
| `onUnschedulableTaskSetRemoved` | unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved | `void` | 设置相关功能 | 传入参数执行设置相关功能 |



### MergedBlockMeta
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getChunksBitmapBuffer` | 无 | `ManagedBuffer` | 获取ChunksBitmapBuffer相关功能 | 调用该方法执行获取ChunksBitmapBuffer相关功能 |
| `getNumChunks` | 无 | `int` | 获取NumChunks相关功能 | 调用该方法执行获取NumChunks相关功能 |



### CountMinSketch
**包路径**: `org.apache.spark.util.sketch`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | depth: int, width: int, seed: int | `CountMinSketch` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | eps: double, confidence: double, seed: int | `CountMinSketch` | 创建相关功能 | 传入参数执行创建相关功能 |
| `readFrom` | in: InputStream | `CountMinSketch` | 读取From相关功能 | 传入参数执行读取From相关功能 |
| `readFrom` | bytes: byte&lt;&gt; | `CountMinSketch` | 读取From相关功能 | 传入参数执行读取From相关功能 |


### LocalDiskShuffleDriverComponents
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cleanupApplication` | 无 | `void` | 向上相关功能 | 调用该方法执行向上相关功能 |
| `initializeApplication` | 无 | `Map&lt;String, String&gt;` | 初始化ializeApplication相关功能 | 调用该方法执行初始化ializeApplication相关功能 |
| `removeShuffle` | shuffleId: int, blocking: boolean | `void` | 移除Shuffle相关功能 | 传入参数执行移除Shuffle相关功能 |



### AuthMethods
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getAuthMethod` | 无 | `String` | 获取AuthMethod相关功能 | 调用该方法执行获取AuthMethod相关功能 |
| `getAuthenticationProvider` | authMethod: AuthMethods | `PasswdAuthenticationProvider` | 获取AuthenticationProvider相关功能 | 传入参数执行获取AuthenticationProvider相关功能 |
| `getValidAuthMethod` | authMethodStr: String | `AuthMethods` | 获取ValidAuthMethod相关功能 | 传入参数执行获取ValidAuthMethod相关功能 |



### ColumnDescriptor
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getComment` | 无 | `String` | 获取Comment相关功能 | 调用该方法执行获取Comment相关功能 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getOrdinalPosition` | 无 | `int` | 获取OrdinalPosition相关功能 | 调用该方法执行获取OrdinalPosition相关功能 |
| `getType` | 无 | `Type` | 获取Type相关功能 | 调用该方法执行获取Type相关功能 |
| `getTypeDescriptor` | 无 | `TypeDescriptor` | 获取TypeDescriptor相关功能 | 调用该方法执行获取TypeDescriptor相关功能 |
| `getTypeName` | 无 | `String` | 获取TypeName相关功能 | 调用该方法执行获取TypeName相关功能 |
| `isPrimitive` | 无 | `boolean` | 判断是否Primitive相关功能 | 调用该方法执行判断是否Primitive相关功能 |
| `newPrimitiveColumnDescriptor` | name: String, comment: String, type: Type, position: int | `ColumnDescriptor` | 列相关功能 | 传入参数执行列相关功能 |
| `toTColumnDesc` | 无 | `TColumnDesc` | 列相关功能 | 调用该方法执行列相关功能 |



### CorruptionCause
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `CorruptionCause` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### HadoopConfigProvider
**包路径**: `org.apache.spark.network.yarn.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String | `String` | 获取元素 | 传入参数执行获取相关功能 |
| `get` | name: String, defaultValue: String | `String` | 获取元素 | 传入参数执行获取相关功能 |



### DBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initDB` | dbBackend: DBBackend, dbFile: File, version: StoreVersion, mapper: ObjectMapper | `DB` | 初始化DB相关功能 | 传入参数执行初始化DB相关功能 |
| `initDB` | dbBackend: DBBackend, file: File | `DB` | 初始化DB相关功能 | 传入参数执行初始化DB相关功能 |



### VariantBuilder
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addKey` | key: String | `int` | 添加键 | 传入参数执行添加键 |
| `appendBinary` | binary: byte&lt;&gt; | `void` | 追加二进制数据到数组 | 传入参数执行追加二进制数据到数组 |
| `appendBoolean` | b: boolean | `void` | 追加布尔值到数组 | 传入参数执行追加布尔值到数组 |
| `appendDate` | daysSinceEpoch: int | `void` | 追加日期到数组 | 传入参数执行追加日期到数组 |
| `appendDecimal` | d: BigDecimal | `void` | 追加Decimal到数组 | 传入参数执行追加Decimal到数组 |
| `appendDouble` | d: double | `void` | 追加双精度浮点数到数组 | 传入参数执行追加双精度浮点数到数组 |
| `appendFloat` | f: float | `void` | 追加单精度浮点数到数组 | 传入参数执行追加单精度浮点数到数组 |
| `appendLong` | l: long | `void` | 追加长整数到数组 | 传入参数执行追加长整数到数组 |
| `appendNull` | 无 | `void` | 追加null值到数组 | 调用该方法执行追加null值到数组 |
| `appendString` | str: String | `void` | 追加字符串到数组 | 传入参数执行追加字符串到数组 |
| `appendTimestamp` | microsSinceEpoch: long | `void` | 追加时间戳到数组 | 传入参数执行追加时间戳到数组 |
| `appendTimestampNtz` | microsSinceEpoch: long | `void` | 追加无时区时间戳到数组 | 传入参数执行追加无时区时间戳到数组 |
| `appendUuid` | uuid: UUID | `void` | 追加UUID到数组 | 传入参数执行追加UUID到数组 |
| `appendVariant` | v: Variant | `void` | 追加Variant类型到数组 | 传入参数执行追加Variant类型到数组 |
| `compareTo` | other: FieldEntry | `int` | 比较To相关功能 | 传入参数执行比较To相关功能 |
| `finishWritingArray` | start: int, offsets: ArrayList<Integer> | `void` | 完成WritingArray相关功能 | 传入参数执行完成WritingArray相关功能 |
| `finishWritingObject` | start: int, fields: ArrayList<FieldEntry> | `void` | 完成WritingObject相关功能 | 传入参数执行完成WritingObject相关功能 |
| `getWritePos` | 无 | `int` | 获取WritePos相关功能 | 调用该方法执行获取WritePos相关功能 |
| `parseJson` | json: String, allowDuplicateKeys: boolean | `Variant` | 解析Json相关功能 | 传入参数执行解析Json相关功能 |
| `parseJson` | parser: JsonParser, allowDuplicateKeys: boolean | `Variant` | 解析Json相关功能 | 传入参数执行解析Json相关功能 |
| `result` | 无 | `Variant` | result操作 | 调用该方法执行result操作 |
| `shallowAppendVariant` | v: Variant | `void` | 追加相关功能 | 传入参数执行追加相关功能 |



### MapConfigProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String | `String` | 获取元素 | 传入参数执行获取相关功能 |
| `get` | name: String, defaultValue: String | `String` | 获取元素 | 传入参数执行获取相关功能 |



### ViewChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | `String` | property操作 | 调用该方法执行property操作 |
| `value` | 无 | `String` | 获取度量指标值 | 返回度量指标数值 |



### LevelDBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkVersion` | db: DB, newversion: StoreVersion, mapper: ObjectMapper | `void` | 检查Version相关功能 | 传入参数执行检查Version相关功能 |
| `initLevelDB` | dbFile: File, version: StoreVersion, mapper: ObjectMapper | `DB` | 初始化LevelDB相关功能 | 传入参数执行初始化LevelDB相关功能 |
| `log` | message: String | `void` | 日志相关功能 | 传入参数执行日志相关功能 |
| `storeVersion` | db: DB, version: StoreVersion, mapper: ObjectMapper | `void` | 版本相关功能 | 传入参数执行版本相关功能 |



### CookieSigner
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `signCookie` | str: String | `String` | 签名Cookie相关功能 | 传入参数执行签名Cookie相关功能 |
| `verifyAndExtract` | signedStr: String | `String` | 验证AndExtract相关功能 | 传入参数执行验证AndExtract相关功能 |



### MemoryConsumer
**包路径**: `org.apache.spark.memory`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acquireMemory` | size: long | `long` | 获取内存 | 传入参数执行获取内存 |
| `allocateArray` | size: long | `LongArray` | 分配数组内存 | 传入参数执行分配数组内存 |
| `freeArray` | array: LongArray | `void` | freeArray操作 | 传入参数执行freeArray操作 |
| `freeMemory` | size: long | `void` | freeMemory操作 | 传入参数执行freeMemory操作 |
| `getMode` | 无 | `MemoryMode` | 获取Mode相关功能 | 调用该方法执行获取Mode相关功能 |
| `getUsed` | 无 | `long` | 获取Used相关功能 | 调用该方法执行获取Used相关功能 |
| `spill` | 无 | `void` | spill操作 | 调用该方法执行spill操作 |



### SessionHandle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getProtocolVersion` | 无 | `TProtocolVersion` | 获取ProtocolVersion相关功能 | 调用该方法执行获取ProtocolVersion相关功能 |
| `getSessionId` | 无 | `UUID` | 获取SessionId相关功能 | 调用该方法执行获取SessionId相关功能 |
| `toTSessionHandle` | 无 | `TSessionHandle` | 处理相关功能 | 调用该方法执行处理相关功能 |



### FetchType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getFetchType` | tFetchType: short | `FetchType` | 获取FetchType相关功能 | 传入参数执行获取FetchType相关功能 |
| `toTFetchType` | 无 | `short` | 获取相关功能 | 调用该方法执行获取相关功能 |



### GetInfoType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getGetInfoType` | tGetInfoType: TGetInfoType | `GetInfoType` | 获取GetInfoType相关功能 | 传入参数执行获取GetInfoType相关功能 |
| `toTGetInfoType` | 无 | `TGetInfoType` | 获取相关功能 | 调用该方法执行获取相关功能 |



### BlockPushReturnCode
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `BlockPushReturnCode` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### MergeStatuses
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `MergeStatuses` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### ConfigProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String, defaultValue: String | `String` | 获取元素 | 传入参数执行获取相关功能 |
| `getBoolean` | name: String, defaultValue: boolean | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getDouble` | name: String, defaultValue: double | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getInt` | name: String, defaultValue: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | name: String, defaultValue: long | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |



### AmIpFilter
**包路径**: `org.apache.spark.deploy.yarn`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `destroy` | 无 | `void` | 销毁相关功能 | 调用该方法执行销毁相关功能 |
| `doFilter` | req: ServletRequest, resp: ServletResponse, chain: FilterChain | `void` | 执行Filter相关功能 | 传入参数执行执行Filter相关功能 |
| `findRedirectUrl` | 无 | `String` | 查找RedirectUrl相关功能 | 调用该方法执行查找RedirectUrl相关功能 |
| `init` | conf: FilterConfig | `void` | 初始化相关功能 | 传入参数执行初始化相关功能 |
| `isValidUrl` | url: String | `boolean` | 判断是否ValidUrl相关功能 | 传入参数执行判断是否ValidUrl相关功能 |



### ChildFirstURLClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getResource` | name: String | `URL` | 获取Resource相关功能 | 传入参数执行获取Resource相关功能 |
| `getResources` | name: String | `Enumeration&lt;URL&gt;` | 获取Resources相关功能 | 传入参数执行获取Resources相关功能 |
| `loadClass` | name: String, resolve: boolean | `Class&lt;?&gt;` | 加载Class相关功能 | 传入参数执行加载Class相关功能 |



### GetInfoValue
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getIntValue` | 无 | `int` | 获取IntValue相关功能 | 调用该方法执行获取IntValue相关功能 |
| `getLongValue` | 无 | `long` | 获取LongValue相关功能 | 调用该方法执行获取LongValue相关功能 |
| `getShortValue` | 无 | `short` | 获取ShortValue相关功能 | 调用该方法执行获取ShortValue相关功能 |
| `getStringValue` | 无 | `String` | 获取StringValue相关功能 | 调用该方法执行获取StringValue相关功能 |
| `toTGetInfoValue` | 无 | `TGetInfoValue` | 获取相关功能 | 调用该方法执行获取相关功能 |



### AuthClientBootstrap
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | client: TransportClient, channel: Channel | `void` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |



### DBBackend
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `byName` | value: String | `DBBackend` | byName操作 | 传入参数执行byName操作 |
| `fileName` | prefix: String | `String` | fileName操作 | 传入参数执行fileName操作 |



### CtrTransportCipher
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addToChannel` | ch: Channel | `void` | 添加到通道 | 传入参数执行添加到通道 |
| `channelRead` | ctx: ChannelHandlerContext, data: Object | `void` | 读取相关功能 | 传入参数执行读取相关功能 |
| `close` | ctx: ChannelHandlerContext, promise: ChannelPromise | `void` | 关闭相关功能 | 传入参数执行关闭相关功能 |
| `count` | 无 | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `getKeyId` | 无 | `String` | 获取KeyId相关功能 | 调用该方法执行获取KeyId相关功能 |
| `handlerRemoved` | ctx: ChannelHandlerContext | `void` | 处理rRemoved相关功能 | 传入参数执行处理rRemoved相关功能 |
| `position` | 无 | `long` | position操作 | 调用该方法执行position操作 |
| `release` | decrement: int | `boolean` | 发布相关功能 | 传入参数执行发布相关功能 |
| `retain` | increment: int | `EncryptedMessage` | retain操作 | 传入参数执行retain操作 |
| `touch` | o: Object | `EncryptedMessage` | touch操作 | 传入参数执行touch操作 |
| `transferTo` | target: WritableByteChannel, position: long | `long` | 转移To相关功能 | 传入参数执行转移To相关功能 |
| `transferred` | 无 | `long` | 转移red相关功能 | 调用该方法执行转移red相关功能 |
| `write` | ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |


### HiveSessionImplwithUGI
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | authFactory: HiveAuthFactory, tokenStr: String | `void` | 判断能否celDelegationToken相关功能 | 传入参数执行判断能否celDelegationToken相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getDelegationToken` | 无 | `String` | 获取DelegationToken相关功能 | 调用该方法执行获取DelegationToken相关功能 |
| `getDelegationToken` | authFactory: HiveAuthFactory, owner: String, renewer: String | `String` | 获取DelegationToken相关功能 | 传入参数执行获取DelegationToken相关功能 |
| `getSessionUgi` | 无 | `UserGroupInformation` | 获取SessionUgi相关功能 | 调用该方法执行获取SessionUgi相关功能 |
| `renewDelegationToken` | authFactory: HiveAuthFactory, tokenStr: String | `void` | renewDelegationToken操作 | 传入参数执行renewDelegationToken操作 |
| `setProxySession` | proxySession: HiveSession | `void` | 设置ProxySession相关功能 | 传入参数执行设置ProxySession相关功能 |
| `setSessionUGI` | owner: String | `void` | 设置SessionUGI相关功能 | 传入参数执行设置SessionUGI相关功能 |



### BlockStoreClient
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `diagnoseCorruption` | host: String, port: int, execId: String, shuffleId: int, mapId: long, reduceId: int, checksum: long, algorithm: String | `Cause` | 向上相关功能 | 传入参数执行向上相关功能 |
| `finalizeShuffleMerge` | host: String, port: int, shuffleId: int, shuffleMergeId: int, listener: MergeFinalizerListener | `void` | 终结ShuffleMerge相关功能 | 传入参数执行终结ShuffleMerge相关功能 |
| `getAppAttemptId` | 无 | `String` | 获取AppAttemptId相关功能 | 调用该方法执行获取AppAttemptId相关功能 |
| `getHostLocalDirs` | host: String, port: int, execIds: String&lt;&gt;, hostLocalDirsCompletable: String&lt;&gt;>> | `void` | 获取HostLocalDirs相关功能 | 传入参数执行获取HostLocalDirs相关功能 |
| `getMergedBlockMeta` | host: String, port: int, shuffleId: int, shuffleMergeId: int, reduceId: int, listener: MergedBlocksMetaListener | `void` | 获取MergedBlockMeta相关功能 | 传入参数执行获取MergedBlockMeta相关功能 |
| `onFailure` | t: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `pushBlocks` | host: String, port: int, blockIds: String&lt;&gt;, buffers: ManagedBuffer&lt;&gt;, listener: BlockPushingListener | `void` | 压入Blocks相关功能 | 传入参数执行压入Blocks相关功能 |
| `removeShuffleMerge` | host: String, port: int, shuffleId: int, shuffleMergeId: int | `boolean` | 移除ShuffleMerge相关功能 | 传入参数执行移除ShuffleMerge相关功能 |
| `setAppAttemptId` | appAttemptId: String | `void` | 设置AppAttemptId相关功能 | 传入参数执行设置AppAttemptId相关功能 |
| `shuffleMetrics` | 无 | `MetricSet` | 随机打乱Metrics相关功能 | 调用该方法执行随机打乱Metrics相关功能 |



### RpcHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelActive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `channelInactive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `exceptionCaught` | cause: Throwable, client: TransportClient | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `getMergedBlockMetaReqHandler` | 无 | `MergedBlockMetaReqHandler` | 获取MergedBlockMetaReqHandler相关功能 | 调用该方法执行获取MergedBlockMetaReqHandler相关功能 |
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `receive` | client: TransportClient, message: ByteBuffer | `void` | 接收相关功能 | 传入参数执行接收相关功能 |
| `receiveMergeBlockMetaReq` | client: TransportClient, mergedBlockMetaRequest: MergedBlockMetaRequest, callback: MergedBlockMetaResponseCallback | `void` | 接收MergeBlockMetaReq相关功能 | 传入参数执行接收MergeBlockMetaReq相关功能 |
| `receiveStream` | client: TransportClient, messageHeader: ByteBuffer, callback: RpcResponseCallback | `StreamCallbackWithID` | 接收Stream相关功能 | 传入参数执行接收Stream相关功能 |



### NoOpMergedShuffleFileManager
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String, cleanupLocalDirs: boolean | `void` | 应用移除事件 | 传入参数执行应用移除事件 |
| `finalizeShuffleMerge` | msg: FinalizeShuffleMerge | `MergeStatuses` | 终结ShuffleMerge相关功能 | 传入参数执行终结ShuffleMerge相关功能 |
| `getMergedBlockData` | appId: String, shuffleId: int, shuffleMergeId: int, reduceId: int, chunkId: int | `ManagedBuffer` | 获取MergedBlockData相关功能 | 传入参数执行获取MergedBlockData相关功能 |
| `getMergedBlockMeta` | appId: String, shuffleId: int, shuffleMergeId: int, reduceId: int | `MergedBlockMeta` | 获取MergedBlockMeta相关功能 | 传入参数执行获取MergedBlockMeta相关功能 |
| `receiveBlockDataAsStream` | msg: PushBlockStream | `StreamCallbackWithID` | 接收BlockDataAsStream相关功能 | 传入参数执行接收BlockDataAsStream相关功能 |
| `registerExecutor` | appId: String, executorInfo: ExecutorShuffleInfo | `void` | 注册Executor相关功能 | 传入参数执行注册Executor相关功能 |
| `removeShuffleMerge` | removeShuffleMerge: RemoveShuffleMerge | `void` | 移除ShuffleMerge相关功能 | 传入参数执行移除ShuffleMerge相关功能 |



### SupportsPushDownJoin
**包路径**: `org.apache.spark.sql.connector.read`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `prettyString` | 无 | `String` | 前ttyString相关功能 | 调用该方法执行前ttyString相关功能 |



### CaseInsensitiveStringMap
**包路径**: `org.apache.spark.sql.util`
**说明**: 大小写不敏感的字符串键值映射，用于传递配置选项到数据源实现。所有key在内部转换为小写存储，确保key匹配时忽略大小写差异。
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `asCaseSensitiveMap` | 无 | `Map&lt;String, String&gt;` | 转换为保留原始大小写的不可变Map，用于需要区分大小写的场景 | `Map&lt;String, String&gt; original = map.asCaseSensitiveMap();<br>// 返回原始key大小写的Map` |
| `clear` | 无 | `void` | 清空集合（此实现不支持，抛出UnsupportedOperationException） | `// 注意：此方法会抛出异常<br>// CaseInsensitiveStringMap是不可变的` |
| `containsKey` | key: Object | `boolean` | 检查指定key是否存在（大小写不敏感匹配） | `boolean exists = map.containsKey("Path");<br>// 即使内部存储为"path"也会返回true` |
| `containsValue` | value: Object | `boolean` | 检查指定value是否存在于Map中 | `boolean hasValue = map.containsValue("hdfs://...");` |
| `empty` | 无 | `CaseInsensitiveStringMap` | 创建一个空的CaseInsensitiveStringMap实例 | `CaseInsensitiveStringMap empty = CaseInsensitiveStringMap.empty();<br>// 返回空的不可变Map` |
| `get` | key: Object | `String` | 获取指定key对应的value（大小写不敏感），不存在返回null | `String path = map.get("path");  // 或"PATH"都可` |
| `getBoolean` | key: String, defaultValue: boolean | `boolean` | 获取指定key的布尔值配置选项，不存在则返回默认值，仅接受"true"/"false"字符串 | `boolean compress = map.getBoolean("compression", false);<br>// key不存在或无效时返回false` |
| `getDouble` | key: String, defaultValue: double | `double` | 获取指定key的双精度浮点数配置选项，不存在则返回默认值 | `double ratio = map.getDouble("ratio", 1.0);<br>// 解析字符串为double` |
| `getInt` | key: String, defaultValue: int | `int` | 获取指定key的整数配置选项，不存在则返回默认值 | `int batchSize = map.getInt("batchSize", 1024);<br>// key不存在时返回1024` |
| `getLong` | key: String, defaultValue: long | `long` | 获取指定key的长整数配置选项，不存在则返回默认值 | `long timeout = map.getLong("timeout", 30000L);` |
| `isEmpty` | 无 | `boolean` | 判断Map是否为空（没有任何键值对） | `if (map.isEmpty()) {<br>    // Map为空，无配置项<br>}` |
| `keySet` | 无 | `Set&lt;String&gt;` | 返回所有key的集合（key已转换为小写） | `Set&lt;String&gt; keys = map.keySet();<br>for (String key : keys) {<br>    System.out.println(key);  // 输出小写key<br>}` |
| `put` | key: String, value: String | `String` | 添加键值对（此实现不支持，抛出UnsupportedOperationException） | `// 注意：CaseInsensitiveStringMap是不可变的<br>// 需要通过构造函数创建` |
| `putAll` | Map&lt;? extends String, ? extends String&gt; | `void` | 批量添加键值对（此实现不支持，抛出UnsupportedOperationException） | `// 注意：不支持修改操作` |
| `remove` | key: Object | `String` | 删除指定key（此实现不支持，抛出UnsupportedOperationException） | `// 注意：不支持删除操作` |
| `size` | 无 | `int` | 返回Map中键值对的数量 | `int count = map.size();<br>System.out.println("配置项数量: " + count);` |
| `values` | 无 | `Collection&lt;String&gt;` | 返回所有value的集合 | `Collection&lt;String&gt; values = map.values();<br>for (String value : values) {<br>    System.out.println(value);<br>}` |


### JobExecutionStatus
**包路径**: `org.apache.spark`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `JobExecutionStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |



### InMemoryStore
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 21

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `accept` | key: Comparable<Object>, value: T | `void` | 接受相关功能 | 传入参数执行接受相关功能 |
| `clear` | 无 | `void` | 清空集合 | 调用该方法执行清除相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `count` | type: Class<?> | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `count` | type: Class<?>, index: String, indexedValue: Object | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `count` | 无 | `int` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `delete` | type: Class<?>, naturalKey: Object | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `delete` | key: Object | `boolean` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `delete` | key: Object, value: T | `boolean` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `get` | key: Object | `T` | 获取元素 | 传入参数执行获取相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `iterator` | 无 | `Iterator&lt;T&gt;` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `next` | 无 | `T` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `next` | max: int | `List&lt;T&gt;` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `put` | value: T | `void` | 添加键值对 | 传入参数执行放入相关功能 |
| `setMetadata` | value: Object | `void` | 设置Metadata相关功能 | 传入参数执行设置Metadata相关功能 |
| `size` | 无 | `int` | 计算大小 | 调用该方法执行size操作 |
| `skip` | n: long | `boolean` | 跳过相关功能 | 传入参数执行跳过相关功能 |
| `view` | 无 | `InMemoryView&lt;T&gt;` | view操作 | 调用该方法执行view操作 |
| `write` | value: Object | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### StreamManager
**包路径**: `org.apache.spark.network.server`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkAuthorization` | client: TransportClient, streamId: long | `void` | 检查Authorization相关功能 | 传入参数执行检查Authorization相关功能 |
| `chunkBeingSent` | streamId: long | `void` | chunkBeingSent操作 | 传入参数执行chunkBeingSent操作 |
| `chunkSent` | streamId: long | `void` | chunkSent操作 | 传入参数执行chunkSent操作 |
| `chunksBeingTransferred` | 无 | `long` | 转移相关功能 | 调用该方法执行转移相关功能 |
| `connectionTerminated` | channel: Channel | `void` | 连接ionTerminated相关功能 | 传入参数执行连接ionTerminated相关功能 |
| `openStream` | streamId: String | `ManagedBuffer` | 打开Stream相关功能 | 传入参数执行打开Stream相关功能 |
| `streamBeingSent` | streamId: String | `void` | streamBeingSent操作 | 传入参数执行streamBeingSent操作 |
| `streamSent` | streamId: String | `void` | streamSent操作 | 传入参数执行streamSent操作 |



### FilterService
**包路径**: `org.apache.hive.service`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getHiveConf` | 无 | `HiveConf` | 获取HiveConf相关功能 | 调用该方法执行获取HiveConf相关功能 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getStartTime` | 无 | `long` | 获取StartTime相关功能 | 调用该方法执行获取StartTime相关功能 |
| `init` | config: HiveConf | `void` | 初始化相关功能 | 传入参数执行初始化相关功能 |
| `register` | listener: ServiceStateChangeListener | `void` | 注册相关功能 | 传入参数执行注册相关功能 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |
| `stop` | 无 | `void` | 停止SparkContext，释放资源 | 调用该方法执行停止相关功能 |
| `unregister` | listener: ServiceStateChangeListener | `void` | 取消注册相关功能 | 传入参数执行取消注册相关功能 |



### TaskMemoryManager
**包路径**: `org.apache.spark.memory`
**方法数量**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acquireExecutionMemory` | required: long, requestingConsumer: MemoryConsumer | `long` | 获取执行内存 | 传入参数执行获取执行内存 |
| `allocatePage` | size: long, consumer: MemoryConsumer | `MemoryBlock` | 分配页面内存 | 传入参数执行分配页面内存 |
| `cleanUpAllAllocatedMemory` | 无 | `long` | 分配相关功能 | 调用该方法执行分配相关功能 |
| `decodePageNumber` | pagePlusOffsetAddress: long | `int` | 解码PageNumber相关功能 | 传入参数执行解码PageNumber相关功能 |
| `encodePageNumberAndOffset` | page: MemoryBlock, offsetInPage: long | `long` | 编码PageNumberAndOffset相关功能 | 传入参数执行编码PageNumberAndOffset相关功能 |
| `encodePageNumberAndOffset` | pageNumber: int, offsetInPage: long | `long` | 编码PageNumberAndOffset相关功能 | 传入参数执行编码PageNumberAndOffset相关功能 |
| `freePage` | page: MemoryBlock, consumer: MemoryConsumer | `void` | 年龄相关功能 | 传入参数执行年龄相关功能 |
| `getMemoryConsumptionForThisTask` | 无 | `long` | 获取MemoryConsumptionForThisTask相关功能 | 调用该方法执行获取MemoryConsumptionForThisTask相关功能 |
| `getOffsetInPage` | pagePlusOffsetAddress: long | `long` | 获取OffsetInPage相关功能 | 传入参数执行获取OffsetInPage相关功能 |
| `getPage` | pagePlusOffsetAddress: long | `Object` | 获取Page相关功能 | 传入参数执行获取Page相关功能 |
| `getPeakOffHeapExecutionMemory` | 无 | `long` | 获取PeakOffHeapExecutionMemory相关功能 | 调用该方法执行获取PeakOffHeapExecutionMemory相关功能 |
| `getPeakOnHeapExecutionMemory` | 无 | `long` | 获取PeakOnHeapExecutionMemory相关功能 | 调用该方法执行获取PeakOnHeapExecutionMemory相关功能 |
| `getTungstenMemoryMode` | 无 | `MemoryMode` | 获取TungstenMemoryMode相关功能 | 调用该方法执行获取TungstenMemoryMode相关功能 |
| `pageSizeBytes` | 无 | `long` | 年龄相关功能 | 调用该方法执行年龄相关功能 |
| `releaseExecutionMemory` | size: long, consumer: MemoryConsumer | `void` | 发布ExecutionMemory相关功能 | 传入参数执行发布ExecutionMemory相关功能 |
| `showMemoryUsage` | 无 | `void` | 年龄相关功能 | 调用该方法执行年龄相关功能 |



### ThriftCLIServiceClient
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String | `void` | 判断能否celDelegationToken相关功能 | 传入参数执行判断能否celDelegationToken相关功能 |
| `cancelOperation` | opHandle: OperationHandle | `void` | 判断能否celOperation相关功能 | 传入参数执行判断能否celOperation相关功能 |
| `checkStatus` | status: TStatus | `void` | 检查Status相关功能 | 传入参数执行检查Status相关功能 |
| `closeOperation` | opHandle: OperationHandle | `void` | 关闭Operation相关功能 | 传入参数执行关闭Operation相关功能 |
| `closeSession` | sessionHandle: SessionHandle | `void` | 关闭Session相关功能 | 传入参数执行关闭Session相关功能 |
| `executeStatement` | sessionHandle: SessionHandle, statement: String, confOverlay: String> | `OperationHandle` | 执行Statement相关功能 | 传入参数执行执行Statement相关功能 |
| `executeStatement` | sessionHandle: SessionHandle, statement: String, confOverlay: String>, queryTimeout: long | `OperationHandle` | 执行Statement相关功能 | 传入参数执行执行Statement相关功能 |
| `executeStatementAsync` | sessionHandle: SessionHandle, statement: String, confOverlay: String> | `OperationHandle` | 执行StatementAsync相关功能 | 传入参数执行执行StatementAsync相关功能 |
| `executeStatementAsync` | sessionHandle: SessionHandle, statement: String, confOverlay: String>, queryTimeout: long | `OperationHandle` | 执行StatementAsync相关功能 | 传入参数执行执行StatementAsync相关功能 |
| `fetchResults` | opHandle: OperationHandle, orientation: FetchOrientation, maxRows: long, fetchType: FetchType | `TRowSet` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `fetchResults` | opHandle: OperationHandle | `TRowSet` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `getCatalogs` | sessionHandle: SessionHandle | `OperationHandle` | 获取Catalogs相关功能 | 传入参数执行获取Catalogs相关功能 |
| `getColumns` | sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, columnName: String | `OperationHandle` | 获取Columns相关功能 | 传入参数执行获取Columns相关功能 |
| `getCrossReference` | sessionHandle: SessionHandle, primaryCatalog: String, primarySchema: String, primaryTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String | `OperationHandle` | 获取CrossReference相关功能 | 传入参数执行获取CrossReference相关功能 |
| `getDelegationToken` | sessionHandle: SessionHandle, authFactory: HiveAuthFactory, owner: String, renewer: String | `String` | 获取DelegationToken相关功能 | 传入参数执行获取DelegationToken相关功能 |
| `getFunctions` | sessionHandle: SessionHandle, catalogName: String, schemaName: String, functionName: String | `OperationHandle` | 获取Functions相关功能 | 传入参数执行获取Functions相关功能 |
| `getInfo` | sessionHandle: SessionHandle, infoType: GetInfoType | `GetInfoValue` | 获取Info相关功能 | 传入参数执行获取Info相关功能 |
| `getOperationStatus` | opHandle: OperationHandle | `OperationStatus` | 获取OperationStatus相关功能 | 传入参数执行获取OperationStatus相关功能 |
| `getPrimaryKeys` | sessionHandle: SessionHandle, catalog: String, schema: String, table: String | `OperationHandle` | 获取PrimaryKeys相关功能 | 传入参数执行获取PrimaryKeys相关功能 |
| `getQueryId` | operationHandle: TOperationHandle | `String` | 获取QueryId相关功能 | 传入参数执行获取QueryId相关功能 |
| `getResultSetMetadata` | opHandle: OperationHandle | `TTableSchema` | 获取ResultSetMetadata相关功能 | 传入参数执行获取ResultSetMetadata相关功能 |
| `getSchemas` | sessionHandle: SessionHandle, catalogName: String, schemaName: String | `OperationHandle` | 获取Schemas相关功能 | 传入参数执行获取Schemas相关功能 |
| `getTableTypes` | sessionHandle: SessionHandle | `OperationHandle` | 获取TableTypes相关功能 | 传入参数执行获取TableTypes相关功能 |
| `getTables` | sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, tableTypes: List<String> | `OperationHandle` | 获取Tables相关功能 | 传入参数执行获取Tables相关功能 |
| `getTypeInfo` | sessionHandle: SessionHandle | `OperationHandle` | 获取TypeInfo相关功能 | 传入参数执行获取TypeInfo相关功能 |
| `openSession` | username: String, password: String, configuration: String> | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |
| `openSessionWithImpersonation` | username: String, password: String, configuration: String>, delegationToken: String | `SessionHandle` | 打开SessionWithImpersonation相关功能 | 传入参数执行打开SessionWithImpersonation相关功能 |
| `renewDelegationToken` | sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String | `void` | renewDelegationToken操作 | 传入参数执行renewDelegationToken操作 |



### OperationManager
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelOperation` | opHandle: OperationHandle | `void` | 判断能否celOperation相关功能 | 传入参数执行判断能否celOperation相关功能 |
| `closeOperation` | opHandle: OperationHandle | `void` | 关闭Operation相关功能 | 传入参数执行关闭Operation相关功能 |
| `getOperation` | operationHandle: OperationHandle | `Operation` | 获取Operation相关功能 | 传入参数执行获取Operation相关功能 |
| `getOperationLogByThread` | 无 | `OperationLog` | 获取OperationLogByThread相关功能 | 调用该方法执行获取OperationLogByThread相关功能 |
| `getOperationLogRowSet` | opHandle: OperationHandle, orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取OperationLogRowSet相关功能 | 传入参数执行获取OperationLogRowSet相关功能 |
| `getOperationNextRowSet` | opHandle: OperationHandle, orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取OperationNextRowSet相关功能 | 传入参数执行获取OperationNextRowSet相关功能 |
| `getOperationResultSetSchema` | opHandle: OperationHandle | `TTableSchema` | 获取OperationResultSetSchema相关功能 | 传入参数执行获取OperationResultSetSchema相关功能 |
| `getOperationStatus` | opHandle: OperationHandle | `OperationStatus` | 获取OperationStatus相关功能 | 传入参数执行获取OperationStatus相关功能 |
| `newExecuteStatementOperation` | parentSession: HiveSession, statement: String, confOverlay: String>, runAsync: boolean, queryTimeout: long | `ExecuteStatementOperation` | 执行相关功能 | 传入参数执行执行相关功能 |
| `newGetCatalogsOperation` | parentSession: HiveSession | `GetCatalogsOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetColumnsOperation` | parentSession: HiveSession, catalogName: String, schemaName: String, tableName: String, columnName: String | `GetColumnsOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetCrossReferenceOperation` | session: HiveSession, primaryCatalog: String, primarySchema: String, primaryTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String | `GetCrossReferenceOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetFunctionsOperation` | parentSession: HiveSession, catalogName: String, schemaName: String, functionName: String | `GetFunctionsOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetPrimaryKeysOperation` | parentSession: HiveSession, catalogName: String, schemaName: String, tableName: String | `GetPrimaryKeysOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetSchemasOperation` | parentSession: HiveSession, catalogName: String, schemaName: String | `GetSchemasOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetTableTypesOperation` | parentSession: HiveSession | `GetTableTypesOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetTablesOperation` | parentSession: HiveSession, catalogName: String, schemaName: String, tableName: String, tableTypes: List<String> | `MetadataOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `newGetTypeInfoOperation` | parentSession: HiveSession | `GetTypeInfoOperation` | 获取相关功能 | 传入参数执行获取相关功能 |
| `removeExpiredOperations` | handles: OperationHandle&lt;&gt; | `List&lt;Operation&gt;` | 移除ExpiredOperations相关功能 | 传入参数执行移除ExpiredOperations相关功能 |



### YarnShuffleService
**包路径**: `org.apache.spark.network.yarn`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMetaData` | 无 | `ByteBuffer` | 获取MetaData相关功能 | 调用该方法执行获取MetaData相关功能 |
| `initializeApplication` | context: ApplicationInitializationContext | `void` | 初始化ializeApplication相关功能 | 传入参数执行初始化ializeApplication相关功能 |
| `initializeContainer` | context: ContainerInitializationContext | `void` | 初始化ializeContainer相关功能 | 传入参数执行初始化ializeContainer相关功能 |
| `setRecoveryPath` | recoveryPath: Path | `void` | 设置RecoveryPath相关功能 | 传入参数执行设置RecoveryPath相关功能 |
| `stopApplication` | context: ApplicationTerminationContext | `void` | 停止Application相关功能 | 传入参数执行停止Application相关功能 |
| `stopContainer` | context: ContainerTerminationContext | `void` | 停止Container相关功能 | 传入参数执行停止Container相关功能 |



### LocalDiskShuffleExecutorComponents
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createMapOutputWriter` | shuffleId: int, mapTaskId: long, numPartitions: int | `ShuffleMapOutputWriter` | 创建MapOutputWriter相关功能 | 传入参数执行创建MapOutputWriter相关功能 |
| `createSingleFileMapOutputWriter` | shuffleId: int, mapId: long | `Optional&lt;SingleSpillShuffleMapOutputWriter&gt;` | 创建SingleFileMapOutputWriter相关功能 | 传入参数执行创建SingleFileMapOutputWriter相关功能 |
| `initializeExecutor` | appId: String, execId: String, extraConfigs: String> | `void` | 初始化ializeExecutor相关功能 | 传入参数执行初始化ializeExecutor相关功能 |



### LogDivertAppender
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 24

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `append` | event: LogEvent | `void` | 追加元素 | 向缓冲迭代器追加一行数据 |
| `create` | operationManager: OperationManager, loggingMode: OperationLog.LoggingLevel | `LogDivertAppender` | 创建相关功能 | 传入参数执行创建相关功能 |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, objects: Object... | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object, o7: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object, o7: Object, o8: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object, o7: Object, o8: Object, o9: Object | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, o: Object, throwable: Throwable | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, message: Message, throwable: Throwable | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `filter` | logEvent: LogEvent | `Result` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `getOnMatch` | 无 | `Result` | 获取OnMatch相关功能 | 调用该方法执行获取OnMatch相关功能 |
| `getOnMismatch` | 无 | `Result` | 获取OnMismatch相关功能 | 调用该方法执行获取OnMismatch相关功能 |
| `getState` | 无 | `State` | 获取State相关功能 | 调用该方法执行获取State相关功能 |
| `initialize` | 无 | `void` | 初始化插件 | 初始化目录插件 |
| `isStarted` | 无 | `boolean` | 判断是否Started相关功能 | 调用该方法执行判断是否Started相关功能 |
| `isStopped` | 无 | `boolean` | 判断是否Stopped相关功能 | 调用该方法执行判断是否Stopped相关功能 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |
| `stop` | 无 | `void` | 停止SparkContext，释放资源 | 调用该方法执行停止相关功能 |



### UploadBlockStream
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `UploadBlockStream` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### SaslClientBootstrap
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | client: TransportClient, channel: Channel | `void` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |



### RocksDB
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `count` | type: Class<?> | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `count` | type: Class<?>, index: String, indexedValue: Object | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `delete` | key: byte&lt;&gt; | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `delete` | type: Class<?>, naturalKey: Object | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `iterator` | 无 | `DBIterator` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `iterator` | 无 | `Iterator&lt;T&gt;` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `put` | key: byte&lt;&gt;, value: byte&lt;&gt; | `void` | 添加键值对 | 传入参数执行放入相关功能 |
| `setMetadata` | value: Object | `void` | 设置Metadata相关功能 | 传入参数执行设置Metadata相关功能 |
| `write` | value: Object | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `writeAll` | values: List<?> | `void` | 写入All相关功能 | 传入参数执行写入All相关功能 |



### FetchOrientation
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getFetchOrientation` | tFetchOrientation: TFetchOrientation | `FetchOrientation` | 获取FetchOrientation相关功能 | 传入参数执行获取FetchOrientation相关功能 |
| `toTFetchOrientation` | 无 | `TFetchOrientation` | 获取相关功能 | 调用该方法执行获取相关功能 |



### RowBasedSet
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addRow` | fields: Object&lt;&gt; | `RowBasedSet` | 添加行到批处理 | 传入参数执行添加行到批处理 |
| `extractSubset` | maxRows: int | `RowBasedSet` | 额外ctSubset相关功能 | 传入参数执行额外ctSubset相关功能 |
| `getSize` | 无 | `int` | 获取Size相关功能 | 调用该方法执行获取Size相关功能 |
| `getStartOffset` | 无 | `long` | 获取StartOffset相关功能 | 调用该方法执行获取StartOffset相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `iterator` | 无 | `Iterator&lt;Object[]&gt;` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `numColumns` | 无 | `int` | 列相关功能 | 调用该方法执行列相关功能 |
| `numRows` | 无 | `int` | numRows操作 | 调用该方法执行numRows操作 |
| `remove` | 无 | `void` | 删除元素 | 调用该方法执行移除相关功能 |
| `removeRange` | fromIndex: int, toIndex: int | `void` | 移除Range相关功能 | 传入参数执行移除Range相关功能 |
| `setStartOffset` | startOffset: long | `void` | 设置StartOffset相关功能 | 传入参数执行设置StartOffset相关功能 |
| `toTRowSet` | 无 | `TRowSet` | 设置相关功能 | 调用该方法执行设置相关功能 |



### ProcedureParameter
**包路径**: `org.apache.spark.sql.connector.catalog.procedures`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `ProcedureParameter` | 构建约束对象 | 构建Check约束对象 |
| `comment` | comment: String | `Builder` | comment操作 | 传入参数执行comment操作 |
| `defaultValue` | sql: String | `Builder` | 默认Value相关功能 | 传入参数执行默认Value相关功能 |
| `defaultValue` | expression: Expression | `Builder` | 默认Value相关功能 | 传入参数执行默认Value相关功能 |
| `defaultValue` | defaultValue: DefaultValue | `Builder` | 默认Value相关功能 | 传入参数执行默认Value相关功能 |


### VariantSchema
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isUnshredded` | 无 | `boolean` | 判断是否Unshredded相关功能 | 调用该方法执行判断是否Unshredded相关功能 |



### ExternalBlockHandler
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String, cleanupLocalDirs: boolean | `void` | 应用移除事件 | 传入参数执行应用移除事件 |
| `channelActive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `channelInactive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `exceptionCaught` | cause: Throwable, client: TransportClient | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `executorRemoved` | executorId: String, appId: String | `void` | 移除相关功能 | 传入参数执行移除相关功能 |
| `getAllMetrics` | 无 | `MetricSet` | 获取AllMetrics相关功能 | 调用该方法执行获取AllMetrics相关功能 |
| `getBlockResolver` | 无 | `ExternalShuffleBlockResolver` | 获取BlockResolver相关功能 | 调用该方法执行获取BlockResolver相关功能 |
| `getMergedBlockMetaReqHandler` | 无 | `MergedBlockMetaReqHandler` | 获取MergedBlockMetaReqHandler相关功能 | 调用该方法执行获取MergedBlockMetaReqHandler相关功能 |
| `getMetrics` | 无 | `Map&lt;String, Metric&gt;` | 获取Metrics相关功能 | 调用该方法执行获取Metrics相关功能 |
| `getStreamManager` | 无 | `StreamManager` | 获取StreamManager相关功能 | 调用该方法执行获取StreamManager相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `next` | 无 | `ManagedBuffer` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `receive` | client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback | `void` | 接收相关功能 | 传入参数执行接收相关功能 |
| `receiveMergeBlockMetaReq` | client: TransportClient, metaRequest: MergedBlockMetaRequest, callback: MergedBlockMetaResponseCallback | `void` | 接收MergeBlockMetaReq相关功能 | 传入参数执行接收MergeBlockMetaReq相关功能 |
| `receiveStream` | client: TransportClient, messageHeader: ByteBuffer, callback: RpcResponseCallback | `StreamCallbackWithID` | 接收Stream相关功能 | 传入参数执行接收Stream相关功能 |



### DelegateSymlinkTextInputFormat
**包路径**: `org.apache.hadoop.hive.ql.io`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `configure` | job: JobConf | `void` | configure操作 | 传入参数执行configure操作 |
| `getContentSummary` | p: Path, job: JobConf | `ContentSummary` | 获取ContentSummary相关功能 | 传入参数执行获取ContentSummary相关功能 |
| `getRecordReader` | split: InputSplit, job: JobConf, reporter: Reporter | `RecordReader&lt;LongWritable, Text&gt;` | 获取RecordReader相关功能 | 传入参数执行获取RecordReader相关功能 |
| `getTargetPath` | 无 | `Path` | 获取TargetPath相关功能 | 调用该方法执行获取TargetPath相关功能 |
| `readFields` | in: DataInput | `void` | 读取Fields相关功能 | 传入参数执行读取Fields相关功能 |
| `write` | out: DataOutput | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### CLIService
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String | `void` | 判断能否celDelegationToken相关功能 | 传入参数执行判断能否celDelegationToken相关功能 |
| `cancelOperation` | opHandle: OperationHandle | `void` | 判断能否celOperation相关功能 | 传入参数执行判断能否celOperation相关功能 |
| `closeOperation` | opHandle: OperationHandle | `void` | 关闭Operation相关功能 | 传入参数执行关闭Operation相关功能 |
| `closeSession` | sessionHandle: SessionHandle | `void` | 关闭Session相关功能 | 传入参数执行关闭Session相关功能 |
| `executeStatement` | sessionHandle: SessionHandle, statement: String, confOverlay: String> | `OperationHandle` | 执行Statement相关功能 | 传入参数执行执行Statement相关功能 |
| `executeStatement` | sessionHandle: SessionHandle, statement: String, confOverlay: String>, queryTimeout: long | `OperationHandle` | 执行Statement相关功能 | 传入参数执行执行Statement相关功能 |
| `executeStatementAsync` | sessionHandle: SessionHandle, statement: String, confOverlay: String> | `OperationHandle` | 执行StatementAsync相关功能 | 传入参数执行执行StatementAsync相关功能 |
| `executeStatementAsync` | sessionHandle: SessionHandle, statement: String, confOverlay: String>, queryTimeout: long | `OperationHandle` | 执行StatementAsync相关功能 | 传入参数执行执行StatementAsync相关功能 |
| `fetchResults` | opHandle: OperationHandle | `TRowSet` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `fetchResults` | opHandle: OperationHandle, orientation: FetchOrientation, maxRows: long, fetchType: FetchType | `TRowSet` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `getCatalogs` | sessionHandle: SessionHandle | `OperationHandle` | 获取Catalogs相关功能 | 传入参数执行获取Catalogs相关功能 |
| `getColumns` | sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, columnName: String | `OperationHandle` | 获取Columns相关功能 | 传入参数执行获取Columns相关功能 |
| `getCrossReference` | sessionHandle: SessionHandle, primaryCatalog: String, primarySchema: String, primaryTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String | `OperationHandle` | 获取CrossReference相关功能 | 传入参数执行获取CrossReference相关功能 |
| `getDelegationToken` | sessionHandle: SessionHandle, authFactory: HiveAuthFactory, owner: String, renewer: String | `String` | 获取DelegationToken相关功能 | 传入参数执行获取DelegationToken相关功能 |
| `getFunctions` | sessionHandle: SessionHandle, catalogName: String, schemaName: String, functionName: String | `OperationHandle` | 获取Functions相关功能 | 传入参数执行获取Functions相关功能 |
| `getHttpUGI` | 无 | `UserGroupInformation` | 获取HttpUGI相关功能 | 调用该方法执行获取HttpUGI相关功能 |
| `getInfo` | sessionHandle: SessionHandle, getInfoType: GetInfoType | `GetInfoValue` | 获取Info相关功能 | 传入参数执行获取Info相关功能 |
| `getOperationStatus` | opHandle: OperationHandle | `OperationStatus` | 获取OperationStatus相关功能 | 传入参数执行获取OperationStatus相关功能 |
| `getPrimaryKeys` | sessionHandle: SessionHandle, catalog: String, schema: String, table: String | `OperationHandle` | 获取PrimaryKeys相关功能 | 传入参数执行获取PrimaryKeys相关功能 |
| `getQueryId` | opHandle: TOperationHandle | `String` | 获取QueryId相关功能 | 传入参数执行获取QueryId相关功能 |
| `getResultSetMetadata` | opHandle: OperationHandle | `TTableSchema` | 获取ResultSetMetadata相关功能 | 传入参数执行获取ResultSetMetadata相关功能 |
| `getSchemas` | sessionHandle: SessionHandle, catalogName: String, schemaName: String | `OperationHandle` | 获取Schemas相关功能 | 传入参数执行获取Schemas相关功能 |
| `getServiceUGI` | 无 | `UserGroupInformation` | 获取ServiceUGI相关功能 | 调用该方法执行获取ServiceUGI相关功能 |
| `getSessionConf` | sessionHandle: SessionHandle | `HiveConf` | 获取SessionConf相关功能 | 传入参数执行获取SessionConf相关功能 |
| `getSessionManager` | 无 | `SessionManager` | 获取SessionManager相关功能 | 调用该方法执行获取SessionManager相关功能 |
| `getTableTypes` | sessionHandle: SessionHandle | `OperationHandle` | 获取TableTypes相关功能 | 传入参数执行获取TableTypes相关功能 |
| `getTables` | sessionHandle: SessionHandle, catalogName: String, schemaName: String, tableName: String, tableTypes: List<String> | `OperationHandle` | 获取Tables相关功能 | 传入参数执行获取Tables相关功能 |
| `getTypeInfo` | sessionHandle: SessionHandle | `OperationHandle` | 获取TypeInfo相关功能 | 传入参数执行获取TypeInfo相关功能 |
| `openSession` | protocol: TProtocolVersion, username: String, password: String, configuration: String> | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |
| `openSession` | protocol: TProtocolVersion, username: String, password: String, ipAddress: String, configuration: String> | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |
| `openSession` | username: String, password: String, configuration: String> | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |
| `openSessionWithImpersonation` | protocol: TProtocolVersion, username: String, password: String, configuration: String>, delegationToken: String | `SessionHandle` | 打开SessionWithImpersonation相关功能 | 传入参数执行打开SessionWithImpersonation相关功能 |
| `openSessionWithImpersonation` | protocol: TProtocolVersion, username: String, password: String, ipAddress: String, configuration: String>, delegationToken: String | `SessionHandle` | 打开SessionWithImpersonation相关功能 | 传入参数执行打开SessionWithImpersonation相关功能 |
| `openSessionWithImpersonation` | username: String, password: String, configuration: String>, delegationToken: String | `SessionHandle` | 打开SessionWithImpersonation相关功能 | 传入参数执行打开SessionWithImpersonation相关功能 |
| `renewDelegationToken` | sessionHandle: SessionHandle, authFactory: HiveAuthFactory, tokenStr: String | `void` | renewDelegationToken操作 | 传入参数执行renewDelegationToken操作 |



### RemoveBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RemoveBlocks` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### GangliaReporter
**包路径**: `com.codahale.metrics.ganglia`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | gmetric: GMetric | `GangliaReporter` | 构建约束对象 | 构建Check约束对象 |
| `build` | gmetrics: GMetric... | `GangliaReporter` | 构建约束对象 | 构建Check约束对象 |
| `convertDurationsTo` | durationUnit: TimeUnit | `Builder` | 转换DurationsTo相关功能 | 传入参数执行转换DurationsTo相关功能 |
| `convertRatesTo` | rateUnit: TimeUnit | `Builder` | 转换RatesTo相关功能 | 传入参数执行转换RatesTo相关功能 |
| `disabledMetricAttributes` | disabledMetricAttributes: Set<MetricAttribute> | `Builder` | 禁用dMetricAttributes相关功能 | 传入参数执行禁用dMetricAttributes相关功能 |
| `filter` | filter: MetricFilter | `Builder` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `forRegistry` | registry: MetricRegistry | `Builder` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `prefixedWith` | prefix: String | `Builder` | 前fixedWith相关功能 | 传入参数执行前fixedWith相关功能 |
| `report` | gauges: Gauge>, counters: Counter>, histograms: Histogram>, meters: Meter>, timers: Timer> | `void` | report操作 | 传入参数执行report操作 |
| `scheduleOn` | executor: ScheduledExecutorService | `Builder` | 调度On相关功能 | 传入参数执行调度On相关功能 |
| `shutdownExecutorOnStop` | shutdownExecutorOnStop: boolean | `Builder` | 关闭ExecutorOnStop相关功能 | 传入参数执行关闭ExecutorOnStop相关功能 |
| `withDMax` | dMax: int | `Builder` | withDMax操作 | 传入参数执行withDMax操作 |
| `withTMax` | tMax: int | `Builder` | withTMax操作 | 传入参数执行withTMax操作 |



### OneForOneStreamManager
**包路径**: `org.apache.spark.network.server`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkAuthorization` | client: TransportClient, streamId: long | `void` | 检查Authorization相关功能 | 传入参数执行检查Authorization相关功能 |
| `chunkBeingSent` | streamId: long | `void` | chunkBeingSent操作 | 传入参数执行chunkBeingSent操作 |
| `chunkSent` | streamId: long | `void` | chunkSent操作 | 传入参数执行chunkSent操作 |
| `chunksBeingTransferred` | 无 | `long` | 转移相关功能 | 调用该方法执行转移相关功能 |
| `connectionTerminated` | channel: Channel | `void` | 连接ionTerminated相关功能 | 传入参数执行连接ionTerminated相关功能 |
| `genStreamChunkId` | streamId: long, chunkId: int | `String` | genStreamChunkId操作 | 传入参数执行genStreamChunkId操作 |
| `getChunk` | streamId: long, chunkIndex: int | `ManagedBuffer` | 获取Chunk相关功能 | 传入参数执行获取Chunk相关功能 |
| `numStreamStates` | 无 | `int` | numStreamStates操作 | 调用该方法执行numStreamStates操作 |
| `openStream` | streamChunkId: String | `ManagedBuffer` | 打开Stream相关功能 | 传入参数执行打开Stream相关功能 |
| `parseStreamChunkId` | streamChunkId: String | `Pair&lt;Long, Integer&gt;` | 解析StreamChunkId相关功能 | 传入参数执行解析StreamChunkId相关功能 |
| `registerStream` | appId: String, buffers: Iterator<ManagedBuffer>, channel: Channel, isBufferMaterializedOnNext: boolean | `long` | 注册Stream相关功能 | 传入参数执行注册Stream相关功能 |
| `registerStream` | appId: String, buffers: Iterator<ManagedBuffer>, channel: Channel | `long` | 注册Stream相关功能 | 传入参数执行注册Stream相关功能 |
| `streamBeingSent` | streamId: String | `void` | streamBeingSent操作 | 传入参数执行streamBeingSent操作 |
| `streamSent` | streamId: String | `void` | streamSent操作 | 传入参数执行streamSent操作 |



### TableInfo
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `TableInfo` | 构建约束对象 | 构建Check约束对象 |
| `properties` | 无 | `Map&lt;String, String&gt;` | properties操作 | 调用该方法执行properties操作 |
| `schema` | 无 | `StructType` | 获取schema | 调用该方法执行schema操作 |
| `withColumns` | columns: Column&lt;&gt; | `Builder` | 列相关功能 | 传入参数执行列相关功能 |
| `withConstraints` | constraints: Constraint&lt;&gt; | `Builder` | 约束相关功能 | 传入参数执行约束相关功能 |
| `withPartitions` | partitions: Transform&lt;&gt; | `Builder` | withPartitions操作 | 传入参数执行withPartitions操作 |
| `withProperties` | properties: String> | `Builder` | withProperties操作 | 传入参数执行withProperties操作 |


### SimpleDownloadFile
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `closeAndRead` | 无 | `ManagedBuffer` | 关闭AndRead相关功能 | 调用该方法执行关闭AndRead相关功能 |
| `delete` | 无 | `boolean` | 删除请求相关功能 | 调用该方法执行删除请求相关功能 |
| `isOpen` | 无 | `boolean` | 判断是否Open相关功能 | 调用该方法执行判断是否Open相关功能 |
| `openForWriting` | 无 | `DownloadFileWritableChannel` | 打开ForWriting相关功能 | 调用该方法执行打开ForWriting相关功能 |
| `path` | 无 | `String` | path操作 | 调用该方法执行path操作 |
| `write` | src: ByteBuffer | `int` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### Operation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancel` | 无 | `void` | 判断能否cel相关功能 | 调用该方法执行判断能否cel相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getBackgroundHandle` | 无 | `Future&lt;?&gt;` | 获取BackgroundHandle相关功能 | 调用该方法执行获取BackgroundHandle相关功能 |
| `getConfiguration` | 无 | `HiveConf` | 获取Configuration相关功能 | 调用该方法执行获取Configuration相关功能 |
| `getHandle` | 无 | `OperationHandle` | 获取Handle相关功能 | 调用该方法执行获取Handle相关功能 |
| `getLastAccessTime` | 无 | `long` | 获取LastAccessTime相关功能 | 调用该方法执行获取LastAccessTime相关功能 |
| `getOperationLog` | 无 | `OperationLog` | 获取OperationLog相关功能 | 调用该方法执行获取OperationLog相关功能 |
| `getOperationTimeout` | 无 | `long` | 获取OperationTimeout相关功能 | 调用该方法执行获取OperationTimeout相关功能 |
| `getParentSession` | 无 | `HiveSession` | 获取ParentSession相关功能 | 调用该方法执行获取ParentSession相关功能 |
| `getProtocolVersion` | 无 | `TProtocolVersion` | 获取ProtocolVersion相关功能 | 调用该方法执行获取ProtocolVersion相关功能 |
| `getStatus` | 无 | `OperationStatus` | 获取Status相关功能 | 调用该方法执行获取Status相关功能 |
| `getType` | 无 | `OperationType` | 获取Type相关功能 | 调用该方法执行获取Type相关功能 |
| `hasResultSet` | 无 | `boolean` | 检查是否存在ResultSet相关功能 | 调用该方法执行检查是否存在ResultSet相关功能 |
| `isCanceled` | 无 | `boolean` | 判断是否Canceled相关功能 | 调用该方法执行判断是否Canceled相关功能 |
| `isFailed` | 无 | `boolean` | 判断是否Failed相关功能 | 调用该方法执行判断是否Failed相关功能 |
| `isFinished` | 无 | `boolean` | 判断是否Finished相关功能 | 调用该方法执行判断是否Finished相关功能 |
| `isRunning` | 无 | `boolean` | 判断是否Running相关功能 | 调用该方法执行判断是否Running相关功能 |
| `isTimedOut` | current: long | `boolean` | 判断是否TimedOut相关功能 | 传入参数执行判断是否TimedOut相关功能 |
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |
| `setConfiguration` | configuration: HiveConf | `void` | 设置Configuration相关功能 | 传入参数执行设置Configuration相关功能 |
| `setOperationTimeout` | operationTimeout: long | `void` | 设置OperationTimeout相关功能 | 传入参数执行设置OperationTimeout相关功能 |
| `shouldRunAsync` | 无 | `boolean` | 判断是否应该RunAsync相关功能 | 调用该方法执行判断是否应该RunAsync相关功能 |



### HeapMemoryAllocator
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `allocate` | size: long | `MemoryBlock` | 分配相关功能 | 传入参数执行分配相关功能 |
| `free` | memory: MemoryBlock | `void` | free操作 | 传入参数执行free操作 |



### ByteArrayWritableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `isOpen` | 无 | `boolean` | 判断是否Open相关功能 | 调用该方法执行判断是否Open相关功能 |
| `length` | 无 | `int` | 计算长度 | 调用该方法执行length操作 |
| `reset` | 无 | `void` | 重置相关功能 | 调用该方法执行重置相关功能 |
| `write` | src: ByteBuffer | `int` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### StorageLevelMapper
**包路径**: `org.apache.spark.storage`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | s: String | `StorageLevel` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |


### TServlet
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addCustomHeader` | key: final String, value: final String | `void` | 添加自定义请求头 | 传入参数执行添加自定义请求头 |
| `getKey` | 无 | `String` | 获取Key相关功能 | 调用该方法执行获取Key相关功能 |
| `getValue` | 无 | `String` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `setCustomHeaders` | headers: String>> | `void` | 设置CustomHeaders相关功能 | 传入参数执行设置CustomHeaders相关功能 |
| `setValue` | value: String | `String` | 设置Value相关功能 | 传入参数执行设置Value相关功能 |



### OneForOneBlockFetcher
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onComplete` | streamId: String | `void` | 完成相关功能 | 传入参数执行完成相关功能 |
| `onData` | streamId: String, buf: ByteBuffer | `void` | onData操作 | 传入参数执行onData操作 |
| `onFailure` | chunkIndex: int, e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onFailure` | streamId: String, cause: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | chunkIndex: int, buffer: ManagedBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |



### UTF8StringBuilder
**包路径**: `org.apache.spark.unsafe`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `append` | value: UTF8String | `void` | 追加元素 | 向缓冲迭代器追加一行数据 |
| `append` | value: String | `void` | 追加元素 | 向缓冲迭代器追加一行数据 |
| `appendBytes` | base: Object, offset: long, length: int | `void` | 追加字节到数组 | 传入参数执行追加字节到数组 |
| `appendCodePoint` | codePoint: int | `void` | 追加Unicode码点到字符串 | 传入参数执行追加Unicode码点到字符串 |
| `build` | 无 | `UTF8String` | 构建约束对象 | 构建Check约束对象 |



### StreamHandle
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `StreamHandle` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### BreakableService
**包路径**: `org.apache.hive.service`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCount` | state: STATE | `int` | 获取Count相关功能 | 传入参数执行获取Count相关功能 |
| `init` | conf: HiveConf | `void` | 初始化相关功能 | 传入参数执行初始化相关功能 |
| `setFailOnInit` | failOnInit: boolean | `void` | 设置FailOnInit相关功能 | 传入参数执行设置FailOnInit相关功能 |
| `setFailOnStart` | failOnStart: boolean | `void` | 设置FailOnStart相关功能 | 传入参数执行设置FailOnStart相关功能 |
| `setFailOnStop` | failOnStop: boolean | `void` | 设置FailOnStop相关功能 | 传入参数执行设置FailOnStop相关功能 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |
| `stop` | 无 | `void` | 停止SparkContext，释放资源 | 调用该方法执行停止相关功能 |



### UnsafeMemoryAllocator
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `allocate` | size: long | `MemoryBlock` | 分配相关功能 | 传入参数执行分配相关功能 |
| `free` | memory: MemoryBlock | `void` | free操作 | 传入参数执行free操作 |



### SSLFactory
**包路径**: `org.apache.spark.network.ssl`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `SSLFactory` | 构建约束对象 | 构建Check约束对象 |
| `certChain` | certChain: File | `Builder` | certChain操作 | 传入参数执行certChain操作 |
| `checkClientTrusted` | x509Certificates: X509Certificate&lt;&gt;, s: String | `void` | 检查ClientTrusted相关功能 | 传入参数执行检查ClientTrusted相关功能 |
| `checkServerTrusted` | x509Certificates: X509Certificate&lt;&gt;, s: String | `void` | 检查ServerTrusted相关功能 | 传入参数执行检查ServerTrusted相关功能 |
| `createSSLEngine` | isClient: boolean, allocator: ByteBufAllocator | `SSLEngine` | 创建SSLEngine相关功能 | 传入参数执行创建SSLEngine相关功能 |
| `destroy` | 无 | `void` | 销毁相关功能 | 调用该方法执行销毁相关功能 |
| `keyPassword` | keyPassword: String | `Builder` | keyPassword操作 | 传入参数执行keyPassword操作 |
| `keyStore` | keyStore: File, keyStorePassword: String | `Builder` | keyStore操作 | 传入参数执行keyStore操作 |
| `openSslEnabled` | enabled: boolean | `Builder` | 打开SslEnabled相关功能 | 传入参数执行打开SslEnabled相关功能 |
| `privateKey` | privateKey: File | `Builder` | 私有Key相关功能 | 传入参数执行私有Key相关功能 |
| `privateKeyPassword` | privateKeyPassword: String | `Builder` | 私有KeyPassword相关功能 | 传入参数执行私有KeyPassword相关功能 |
| `requestedCiphers` | requestedCiphers: String&lt;&gt; | `Builder` | 请求edCiphers相关功能 | 传入参数执行请求edCiphers相关功能 |
| `requestedProtocol` | requestedProtocol: String | `Builder` | 请求edProtocol相关功能 | 传入参数执行请求edProtocol相关功能 |
| `trustStore` | trustStore: File, trustStorePassword: String, trustStoreReloadingEnabled: boolean, trustStoreReloadIntervalMs: int | `Builder` | trustStore操作 | 传入参数执行trustStore操作 |



### RetryingBlockTransferor
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getRetryCount` | 无 | `int` | 获取RetryCount相关功能 | 调用该方法执行获取RetryCount相关功能 |
| `getTransferType` | 无 | `String` | 获取TransferType相关功能 | 调用该方法执行获取TransferType相关功能 |
| `onBlockFetchFailure` | blockId: String, exception: Throwable | `void` | 锁定相关功能 | 传入参数执行锁定相关功能 |
| `onBlockFetchSuccess` | blockId: String, data: ManagedBuffer | `void` | 锁定相关功能 | 传入参数执行锁定相关功能 |
| `onBlockPushFailure` | blockId: String, exception: Throwable | `void` | 锁定相关功能 | 传入参数执行锁定相关功能 |
| `onBlockPushSuccess` | blockId: String, data: ManagedBuffer | `void` | 锁定相关功能 | 传入参数执行锁定相关功能 |
| `onBlockTransferFailure` | blockId: String, exception: Throwable | `void` | 锁定相关功能 | 传入参数执行锁定相关功能 |
| `onBlockTransferSuccess` | blockId: String, data: ManagedBuffer | `void` | 锁定相关功能 | 传入参数执行锁定相关功能 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |



### CustomSumMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `aggregateTaskMetrics` | taskMetrics: long&lt;&gt; | `String` | 聚合任务级别的度量指标 | 聚合任务度量指标为字符串 |



### SparkSaslServer
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `encodeIdentifier` | identifier: String | `String` | 编码Identifier相关功能 | 传入参数执行编码Identifier相关功能 |
| `getNegotiatedProperty` | name: String | `Object` | 获取NegotiatedProperty相关功能 | 传入参数执行获取NegotiatedProperty相关功能 |
| `handle` | callbacks: Callback&lt;&gt; | `void` | 处理相关功能 | 传入参数执行处理相关功能 |



### ByteBufferWriteableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `isOpen` | 无 | `boolean` | 判断是否Open相关功能 | 调用该方法执行判断是否Open相关功能 |
| `write` | src: ByteBuffer | `int` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |



### V2ExpressionSQLBuilder
**包路径**: `org.apache.spark.sql.connector.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | expr: Expression | `String` | 构建约束对象 | 构建Check约束对象 |


### GetCatalogsOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### StorageLevels
**包路径**: `org.apache.spark.api.java`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | useDisk: boolean, useMemory: boolean, useOffHeap: boolean, deserialized: boolean, replication: int | `StorageLevel` | 创建相关功能 | 传入参数执行创建相关功能 |



### BlocksRemoved
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `BlocksRemoved` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### JavaModuleOptions
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultModuleOptions` | 无 | `String` | 默认ModuleOptions相关功能 | 调用该方法执行默认ModuleOptions相关功能 |


### IntegerAdd
**包路径**: `org.apache.spark.sql.connector.catalog.functions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `invoke` | left: int, right: int | `int` | 调用相关功能 | 传入参数执行调用相关功能 |
| `produceResult` | input: InternalRow | `Integer` | 生产Result相关功能 | 传入参数执行生产Result相关功能 |


### TSubjectAssumingTransport
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `open` | 无 | `void` | 打开相关功能 | 调用该方法执行打开相关功能 |



### ThriftHttpServlet
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | 无 | `String` | 运行相关功能 | 调用该方法执行运行相关功能 |



### GetTablesOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### ViewInfo
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `currentCatalog` | 无 | `String` | 当前Catalog相关功能 | 调用该方法执行当前Catalog相关功能 |
| `ident` | 无 | `Identifier` | ident操作 | 调用该方法执行ident操作 |
| `properties` | 无 | `Map&lt;String, String&gt;` | properties操作 | 调用该方法执行properties操作 |
| `schema` | 无 | `StructType` | 获取schema | 调用该方法执行schema操作 |
| `sql` | 无 | `String` | 执行SQL查询 | 调用该方法执行sql操作 |


### OperationState
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationState` | tOperationState: TOperationState | `OperationState` | 获取OperationState相关功能 | 传入参数执行获取OperationState相关功能 |
| `isTerminal` | 无 | `boolean` | 判断是否Terminal相关功能 | 调用该方法执行判断是否Terminal相关功能 |
| `toTOperationState` | 无 | `TOperationState` | 顶部相关功能 | 调用该方法执行顶部相关功能 |
| `validateTransition` | oldState: OperationState, newState: OperationState | `void` | 校验Transition相关功能 | 传入参数执行校验Transition相关功能 |
| `validateTransition` | newState: OperationState | `void` | 校验Transition相关功能 | 传入参数执行校验Transition相关功能 |



### PrefixComparators
**包路径**: `org.apache.spark.util.collection.unsafe.sort`
**方法数量**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `compare` | aPrefix: long, bPrefix: long | `int` | 比较相关功能 | 传入参数执行比较相关功能 |
| `compare` | bPrefix: long, aPrefix: long | `int` | 比较相关功能 | 传入参数执行比较相关功能 |
| `compare` | a: long, b: long | `int` | 比较相关功能 | 传入参数执行比较相关功能 |
| `compare` | b: long, a: long | `int` | 比较相关功能 | 传入参数执行比较相关功能 |
| `computePrefix` | value: UTF8String | `long` | 计算Prefix相关功能 | 传入参数执行计算Prefix相关功能 |
| `computePrefix` | bytes: byte&lt;&gt; | `long` | 计算Prefix相关功能 | 传入参数执行计算Prefix相关功能 |
| `computePrefix` | value: double | `long` | 计算Prefix相关功能 | 传入参数执行计算Prefix相关功能 |
| `nullsFirst` | 无 | `boolean` | 第一个相关功能 | 调用该方法执行第一个相关功能 |
| `sortDescending` | 无 | `boolean` | 排序Descending相关功能 | 调用该方法执行排序Descending相关功能 |
| `sortSigned` | 无 | `boolean` | 排序Signed相关功能 | 调用该方法执行排序Signed相关功能 |


### TableSchema
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addPrimitiveColumn` | columnName: String, columnType: Type, columnComment: String | `TableSchema` | 添加原始类型列 | 传入参数执行添加原始类型列 |
| `addStringColumn` | columnName: String, columnComment: String | `TableSchema` | 添加字符串类型列 | 传入参数执行添加字符串类型列 |
| `clear` | 无 | `void` | 清空集合 | 调用该方法执行清除相关功能 |
| `getColumnDescriptorAt` | pos: int | `ColumnDescriptor` | 获取ColumnDescriptorAt相关功能 | 传入参数执行获取ColumnDescriptorAt相关功能 |
| `getColumnDescriptors` | 无 | `List&lt;ColumnDescriptor&gt;` | 获取ColumnDescriptors相关功能 | 调用该方法执行获取ColumnDescriptors相关功能 |
| `getSize` | 无 | `int` | 获取Size相关功能 | 调用该方法执行获取Size相关功能 |
| `toTTableSchema` | 无 | `TTableSchema` | toTTableSchema操作 | 调用该方法执行toTTableSchema操作 |



### SparkGenericUDAFBridge
**包路径**: `org.apache.hadoop.hive.ql.udf.generic`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getEvaluator` | parameters: TypeInfo&lt;&gt; | `GenericUDAFEvaluator` | 获取Evaluator相关功能 | 传入参数执行获取Evaluator相关功能 |
| `iterate` | agg: AggregationBuffer, parameters: Object&lt;&gt; | `void` | 迭代相关功能 | 传入参数执行迭代相关功能 |
| `merge` | agg: AggregationBuffer, partial: Object | `void` | 合并相关功能 | 传入参数执行合并相关功能 |
| `terminate` | agg: AggregationBuffer | `Object` | terminate操作 | 传入参数执行terminate操作 |
| `terminatePartial` | agg: AggregationBuffer | `Object` | terminatePartial操作 | 传入参数执行terminatePartial操作 |



### MyLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |



### UnknownPartitioning
**包路径**: `org.apache.spark.sql.connector.read.partitioning`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `numPartitions` | 无 | `int` | numPartitions操作 | 调用该方法执行numPartitions操作 |


### RemoteBlockPushResolver
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String, cleanupLocalDirs: boolean | `void` | 应用移除事件 | 传入参数执行应用移除事件 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `finalizeShuffleMerge` | msg: FinalizeShuffleMerge | `MergeStatuses` | 终结ShuffleMerge相关功能 | 传入参数执行终结ShuffleMerge相关功能 |
| `getAppPathsInfo` | 无 | `AppPathsInfo` | 获取AppPathsInfo相关功能 | 调用该方法执行获取AppPathsInfo相关功能 |
| `getCompletionResponse` | 无 | `ByteBuffer` | 获取CompletionResponse相关功能 | 调用该方法执行获取CompletionResponse相关功能 |
| `getDataFilePos` | 无 | `long` | 获取DataFilePos相关功能 | 调用该方法执行获取DataFilePos相关功能 |
| `getDos` | 无 | `DataOutputStream` | 获取Dos相关功能 | 调用该方法执行获取Dos相关功能 |
| `getID` | 无 | `String` | 获取ID相关功能 | 调用该方法执行获取ID相关功能 |
| `getMapTracker` | 无 | `RoaringBitmap` | 获取MapTracker相关功能 | 调用该方法执行获取MapTracker相关功能 |
| `getMergedBlockData` | appId: String, shuffleId: int, shuffleMergeId: int, reduceId: int, chunkId: int | `ManagedBuffer` | 获取MergedBlockData相关功能 | 传入参数执行获取MergedBlockData相关功能 |
| `getMergedBlockMeta` | appId: String, shuffleId: int, shuffleMergeId: int, reduceId: int | `MergedBlockMeta` | 获取MergedBlockMeta相关功能 | 传入参数执行获取MergedBlockMeta相关功能 |
| `getMergedShuffleDataFile` | shuffleId: int, shuffleMergeId: int, reduceId: int | `File` | 获取MergedShuffleDataFile相关功能 | 传入参数执行获取MergedShuffleDataFile相关功能 |
| `getMergedShuffleIndexFilePath` | shuffleId: int, shuffleMergeId: int, reduceId: int | `String` | 获取MergedShuffleIndexFilePath相关功能 | 传入参数执行获取MergedShuffleIndexFilePath相关功能 |
| `getMergedShuffleMetaFile` | shuffleId: int, shuffleMergeId: int, reduceId: int | `File` | 获取MergedShuffleMetaFile相关功能 | 传入参数执行获取MergedShuffleMetaFile相关功能 |
| `getMetrics` | 无 | `MetricSet` | 获取Metrics相关功能 | 调用该方法执行获取Metrics相关功能 |
| `getMetrics` | 无 | `Map&lt;String, Metric&gt;` | 获取Metrics相关功能 | 调用该方法执行获取Metrics相关功能 |
| `getShuffleMergePartitions` | 无 | `Map&lt;Integer, AppShufflePartitionInfo&gt;` | 获取ShuffleMergePartitions相关功能 | 调用该方法执行获取ShuffleMergePartitions相关功能 |
| `getShuffles` | 无 | `ConcurrentMap&lt;Integer, AppShuffleMergePartitionsInfo&gt;` | 获取Shuffles相关功能 | 调用该方法执行获取Shuffles相关功能 |
| `isFinalized` | 无 | `boolean` | 判断是否Finalized相关功能 | 调用该方法执行判断是否Finalized相关功能 |
| `load` | filePath: String | `ShuffleIndexInformation` | 加载相关功能 | 传入参数执行加载相关功能 |
| `onComplete` | streamId: String | `void` | 完成相关功能 | 传入参数执行完成相关功能 |
| `onData` | streamId: String, buf: ByteBuffer | `void` | onData操作 | 传入参数执行onData操作 |
| `onFailure` | streamId: String, cause: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onFailure` | streamId: String, throwable: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `receiveBlockDataAsStream` | msg: PushBlockStream | `StreamCallbackWithID` | 接收BlockDataAsStream相关功能 | 传入参数执行接收BlockDataAsStream相关功能 |
| `registerExecutor` | appId: String, executorInfo: ExecutorShuffleInfo | `void` | 注册Executor相关功能 | 传入参数执行注册Executor相关功能 |
| `removeShuffleMerge` | msg: RemoveShuffleMerge | `void` | 移除ShuffleMerge相关功能 | 传入参数执行移除ShuffleMerge相关功能 |
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |
| `setDataFilePos` | dataFilePos: long | `void` | 设置DataFilePos相关功能 | 传入参数执行设置DataFilePos相关功能 |
| `setReduceIds` | reduceIds: int&lt;&gt; | `void` | 设置ReduceIds相关功能 | 传入参数执行设置ReduceIds相关功能 |
| `shouldLogError` | t: Throwable | `boolean` | 判断是否应该LogError相关功能 | 传入参数执行判断是否应该LogError相关功能 |



### ShuffleTransportContext
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acceptInboundMessage` | msg: Object | `boolean` | 接受入站消息 | 传入参数执行接受入站消息 |
| `initializePipeline` | channel: SocketChannel, isClient: boolean | `TransportChannelHandler` | 初始化ializePipeline相关功能 | 传入参数执行初始化ializePipeline相关功能 |
| `initializePipeline` | channel: SocketChannel, channelRpcHandler: RpcHandler, isClient: boolean | `TransportChannelHandler` | 初始化ializePipeline相关功能 | 传入参数执行初始化ializePipeline相关功能 |



### TableChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `column` | 无 | `String` | 创建列引用表达式 | 调用该方法执行创建列引用表达式 |
| `comment` | 无 | `String` | comment操作 | 调用该方法执行comment操作 |
| `constraint` | 无 | `Constraint` | 约束相关功能 | 调用该方法执行约束相关功能 |
| `dataType` | 无 | `DataType` | 获取数据类型 | 返回Cast目标的数据类型 |
| `defaultValue` | 无 | `ColumnDefaultValue` | 默认Value相关功能 | 调用该方法执行默认Value相关功能 |
| `ifExists` | 无 | `Boolean` | 判断是否相关功能 | 调用该方法执行判断是否相关功能 |
| `ifExists` | 无 | `boolean` | 判断是否相关功能 | 调用该方法执行判断是否相关功能 |
| `isNullable` | 无 | `boolean` | 判断是否Nullable相关功能 | 调用该方法执行判断是否Nullable相关功能 |
| `mode` | 无 | `Mode` | mode操作 | 调用该方法执行mode操作 |
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |
| `newComment` | 无 | `String` | newComment操作 | 调用该方法执行newComment操作 |
| `newCurrentDefault` | 无 | `DefaultValue` | 默认相关功能 | 调用该方法执行默认相关功能 |
| `newDataType` | 无 | `DataType` | newDataType操作 | 调用该方法执行newDataType操作 |
| `newDefaultValue` | 无 | `String` | 默认相关功能 | 调用该方法执行默认相关功能 |
| `newName` | 无 | `String` | newName操作 | 调用该方法执行newName操作 |
| `nullable` | 无 | `boolean` | nullable操作 | 调用该方法执行nullable操作 |
| `position` | 无 | `ColumnPosition` | position操作 | 调用该方法执行position操作 |
| `property` | 无 | `String` | property操作 | 调用该方法执行property操作 |
| `validatedTableVersion` | 无 | `String` | 校验dTableVersion相关功能 | 调用该方法执行校验dTableVersion相关功能 |
| `value` | 无 | `String` | 获取度量指标值 | 返回度量指标数值 |



### GetTypeInfoOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### ThriftHttpCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |



### BlockPushNonFatalFailure
**包路径**: `org.apache.spark.network.server`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getErrorMsg` | blockId: String, errorCode: ReturnCode | `String` | 获取ErrorMsg相关功能 | 传入参数执行获取ErrorMsg相关功能 |
| `getResponse` | 无 | `ByteBuffer` | 获取Response相关功能 | 调用该方法执行获取Response相关功能 |
| `getReturnCode` | 无 | `ReturnCode` | 获取ReturnCode相关功能 | 调用该方法执行获取ReturnCode相关功能 |
| `getReturnCode` | id: byte | `ReturnCode` | 获取ReturnCode相关功能 | 传入参数执行获取ReturnCode相关功能 |
| `id` | 无 | `byte` | id操作 | 调用该方法执行id操作 |
| `shouldNotRetryErrorCode` | returnCode: ReturnCode | `boolean` | 判断是否应该NotRetryErrorCode相关功能 | 传入参数执行判断是否应该NotRetryErrorCode相关功能 |



### TransportClientFactory
**包路径**: `org.apache.spark.network.client`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `createClient` | remoteHost: String, remotePort: int, fastFail: boolean | `TransportClient` | 创建Client相关功能 | 传入参数执行创建Client相关功能 |
| `createClient` | remoteHost: String, remotePort: int | `TransportClient` | 创建Client相关功能 | 传入参数执行创建Client相关功能 |
| `createUnmanagedClient` | remoteHost: String, remotePort: int | `TransportClient` | 创建UnmanagedClient相关功能 | 传入参数执行创建UnmanagedClient相关功能 |
| `getAllMetrics` | 无 | `MetricSet` | 获取AllMetrics相关功能 | 调用该方法执行获取AllMetrics相关功能 |
| `initChannel` | ch: SocketChannel | `void` | 初始化Channel相关功能 | 传入参数执行初始化Channel相关功能 |
| `operationComplete` | handshakeFuture: final Future<Channel> | `void` | 完成相关功能 | 传入参数执行完成相关功能 |



### ExternalBlockStoreClient
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `fetchBlocks` | host: String, port: int, execId: String, blockIds: String&lt;&gt;, listener: BlockFetchingListener, downloadFileManager: DownloadFileManager | `void` | 获取Blocks相关功能 | 传入参数执行获取Blocks相关功能 |
| `finalizeShuffleMerge` | host: String, port: int, shuffleId: int, shuffleMergeId: int, listener: MergeFinalizerListener | `void` | 终结ShuffleMerge相关功能 | 传入参数执行终结ShuffleMerge相关功能 |
| `getMergedBlockMeta` | host: String, port: int, shuffleId: int, shuffleMergeId: int, reduceId: int, listener: MergedBlocksMetaListener | `void` | 获取MergedBlockMeta相关功能 | 传入参数执行获取MergedBlockMeta相关功能 |
| `init` | appId: String | `void` | 初始化相关功能 | 传入参数执行初始化相关功能 |
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `onSuccess` | numChunks: int, buffer: ManagedBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `pushBlocks` | host: String, port: int, blockIds: String&lt;&gt;, buffers: ManagedBuffer&lt;&gt;, listener: BlockPushingListener | `void` | 压入Blocks相关功能 | 传入参数执行压入Blocks相关功能 |
| `registerWithShuffleServer` | host: String, port: int, execId: String, executorInfo: ExecutorShuffleInfo | `void` | 注册WithShuffleServer相关功能 | 传入参数执行注册WithShuffleServer相关功能 |
| `removeBlocks` | host: String, port: int, execId: String, blockIds: String&lt;&gt; | `Future&lt;Integer&gt;` | 移除Blocks相关功能 | 传入参数执行移除Blocks相关功能 |
| `removeShuffleMerge` | host: String, port: int, shuffleId: int, shuffleMergeId: int | `boolean` | 移除ShuffleMerge相关功能 | 传入参数执行移除ShuffleMerge相关功能 |
| `setAppAttemptId` | appAttemptId: String | `void` | 设置AppAttemptId相关功能 | 传入参数执行设置AppAttemptId相关功能 |
| `shuffleMetrics` | 无 | `MetricSet` | 随机打乱Metrics相关功能 | 调用该方法执行随机打乱Metrics相关功能 |



### KVStoreView
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `closeableIterator` | 无 | `KVStoreIterator&lt;T&gt;` | 关闭ableIterator相关功能 | 调用该方法执行关闭ableIterator相关功能 |
| `first` | value: Object | `KVStoreView&lt;T&gt;` | 第一行 | // first：获取第一个元素<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(10, 20, 30));<br>Integer first = rdd.first();<br>// 结果: 10 |
| `index` | name: String | `KVStoreView&lt;T&gt;` | index操作 | 传入参数执行index操作 |
| `last` | value: Object | `KVStoreView&lt;T&gt;` | 最后一个相关功能 | 传入参数执行最后一个相关功能 |
| `max` | max: long | `KVStoreView&lt;T&gt;` | 最大值 | // max：最大值<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));<br>double max = doubleRDD.max();<br>// 结果: 30.0 |
| `parent` | value: Object | `KVStoreView&lt;T&gt;` | 父级相关功能 | 传入参数执行父级相关功能 |
| `reverse` | 无 | `KVStoreView&lt;T&gt;` | reverse操作 | 调用该方法执行reverse操作 |
| `skip` | n: long | `KVStoreView&lt;T&gt;` | 跳过相关功能 | 传入参数执行跳过相关功能 |



### UserDefinedAggregateFunc
**包路径**: `org.apache.spark.sql.connector.expressions.aggregate`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `canonicalName` | 无 | `String` | 判断能否onicalName相关功能 | 调用该方法执行判断能否onicalName相关功能 |
| `isDistinct` | 无 | `boolean` | 判断是否Distinct相关功能 | 调用该方法执行判断是否Distinct相关功能 |
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |



### OneForOneBlockPusher
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |



### LocalDiskSingleSpillMapOutputWriter
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transferMapSpillFile` | mapSpillFile: File, partitionLengths: long&lt;&gt;, checksums: long&lt;&gt; | `void` | 转移MapSpillFile相关功能 | 传入参数执行转移MapSpillFile相关功能 |



### BloomFilter
**包路径**: `org.apache.spark.util.sketch`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cardinality` | 无 | `long` | cardinality操作 | 调用该方法执行cardinality操作 |
| `create` | expectedNumItems: long | `BloomFilter` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | expectedNumItems: long, fpp: double | `BloomFilter` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | expectedNumItems: long, numBits: long | `BloomFilter` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | expectedNumItems: long, numBits: long, seed: int | `BloomFilter` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | version: Version, expectedNumItems: long, numBits: long, seed: int | `BloomFilter` | 创建相关功能 | 传入参数执行创建相关功能 |
| `optimalNumOfBits` | n: long, p: double | `long` | 双相关功能 | 传入参数执行双相关功能 |
| `optimalNumOfBits` | expectedNumItems: long, maxNumItems: long, maxNumOfBits: long | `long` | 双相关功能 | 传入参数执行双相关功能 |
| `readFrom` | in: InputStream | `BloomFilter` | 读取From相关功能 | 传入参数执行读取From相关功能 |
| `readFrom` | bytes: byte&lt;&gt; | `BloomFilter` | 读取From相关功能 | 传入参数执行读取From相关功能 |



### TSetIpAddressProcessor
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getUserIpAddress` | 无 | `String` | 获取UserIpAddress相关功能 | 调用该方法执行获取UserIpAddress相关功能 |
| `getUserName` | 无 | `String` | 获取UserName相关功能 | 调用该方法执行获取UserName相关功能 |
| `process` | in: final TProtocol, out: final TProtocol | `void` | 处理相关功能 | 传入参数执行处理相关功能 |



### LevelDB
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `count` | type: Class<?> | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `count` | type: Class<?>, index: String, indexedValue: Object | `long` | 统计行数 | // count：统计元素总数<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));<br>long count = rdd.count();<br>// 结果: 5 |
| `delete` | key: byte&lt;&gt; | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `delete` | type: Class<?>, naturalKey: Object | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `iterator` | 无 | `DBIterator` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `iterator` | 无 | `Iterator&lt;T&gt;` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `put` | key: byte&lt;&gt;, value: byte&lt;&gt; | `void` | 添加键值对 | 传入参数执行放入相关功能 |
| `setMetadata` | value: Object | `void` | 设置Metadata相关功能 | 传入参数执行设置Metadata相关功能 |
| `write` | value: Object | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `writeAll` | values: List<?> | `void` | 写入All相关功能 | 传入参数执行写入All相关功能 |



### StreamInterceptor
**包路径**: `org.apache.spark.network.client`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | 无 | `void` | 活跃相关功能 | 调用该方法执行活跃相关功能 |
| `exceptionCaught` | cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `handle` | buf: ByteBuf | `boolean` | 处理相关功能 | 传入参数执行处理相关功能 |



### ByteUnit
**包路径**: `org.apache.spark.network.util`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertFrom` | d: long, u: ByteUnit | `long` | 转换From相关功能 | 传入参数执行转换From相关功能 |
| `convertTo` | d: long, u: ByteUnit | `long` | 转换To相关功能 | 传入参数执行转换To相关功能 |
| `toBytes` | d: long | `long` | toBytes操作 | 传入参数执行toBytes操作 |
| `toGiB` | d: long | `long` | toGiB操作 | 传入参数执行toGiB操作 |
| `toKiB` | d: long | `long` | toKiB操作 | 传入参数执行toKiB操作 |
| `toMiB` | d: long | `long` | toMiB操作 | 传入参数执行toMiB操作 |
| `toPiB` | d: long | `long` | 顶部iB相关功能 | 传入参数执行顶部iB相关功能 |
| `toTiB` | d: long | `long` | toTiB操作 | 传入参数执行toTiB操作 |



### ReadAheadInputStream
**包路径**: `org.apache.spark.io`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `available` | 无 | `int` | 检查数据是否可用 | 调用该方法执行检查数据是否可用 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `read` | 无 | `int` | 读取数据源创建DataFrame | 调用该方法执行读取相关功能 |
| `read` | b: byte&lt;&gt;, offset: int, len: int | `int` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |
| `skip` | n: long | `long` | 跳过相关功能 | 传入参数执行跳过相关功能 |



### ShuffleIndexInformation
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getIndex` | reduceId: int | `ShuffleIndexRecord` | 获取Index相关功能 | 传入参数执行获取Index相关功能 |
| `getIndex` | startReduceId: int, endReduceId: int | `ShuffleIndexRecord` | 获取Index相关功能 | 传入参数执行获取Index相关功能 |
| `getRetainedMemorySize` | 无 | `int` | 获取RetainedMemorySize相关功能 | 调用该方法执行获取RetainedMemorySize相关功能 |



### NettyLogger
**包路径**: `org.apache.spark.network.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getLoggingHandler` | 无 | `LoggingHandler` | 获取LoggingHandler相关功能 | 调用该方法执行获取LoggingHandler相关功能 |



### RadixSort
**包路径**: `org.apache.spark.util.collection.unsafe.sort`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `sort` | array: LongArray, numRecords: long, startByteIndex: int, endByteIndex: int, desc: boolean, signed: boolean | `int` | 排序 | 传入参数执行创建排序表达式 |
| `sortKeyPrefixArray` | array: LongArray, startIndex: long, numRecords: long, startByteIndex: int, endByteIndex: int, desc: boolean, signed: boolean | `int` | 排序KeyPrefixArray相关功能 | 传入参数执行排序KeyPrefixArray相关功能 |



### OperationStatus
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationException` | 无 | `HiveSQLException` | 获取OperationException相关功能 | 调用该方法执行获取OperationException相关功能 |
| `getState` | 无 | `OperationState` | 获取State相关功能 | 调用该方法执行获取State相关功能 |



### ExternalShuffleBlockResolver
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String, cleanupLocalDirs: boolean | `void` | 应用移除事件 | 传入参数执行应用移除事件 |
| `diagnoseShuffleBlockCorruption` | appId: String, execId: String, shuffleId: int, mapId: long, reduceId: int, checksumByReader: long, algorithm: String | `Cause` | 锁定相关功能 | 传入参数执行锁定相关功能 |
| `executorRemoved` | executorId: String, appId: String | `void` | 移除相关功能 | 传入参数执行移除相关功能 |
| `getBlockData` | appId: String, execId: String, shuffleId: int, mapId: long, reduceId: int | `ManagedBuffer` | 获取BlockData相关功能 | 传入参数执行获取BlockData相关功能 |
| `getContinuousBlocksData` | appId: String, execId: String, shuffleId: int, mapId: long, startReduceId: int, endReduceId: int | `ManagedBuffer` | 获取ContinuousBlocksData相关功能 | 传入参数执行获取ContinuousBlocksData相关功能 |
| `getDiskPersistedRddBlockData` | executor: ExecutorShuffleInfo, rddId: int, splitIndex: int | `ManagedBuffer` | 获取DiskPersistedRddBlockData相关功能 | 传入参数执行获取DiskPersistedRddBlockData相关功能 |
| `getLocalDirs` | appId: String, execIds: Set<String> | `Map&lt;String, String[]&gt;` | 获取LocalDirs相关功能 | 传入参数执行获取LocalDirs相关功能 |
| `getRddBlockData` | appId: String, execId: String, rddId: int, splitIndex: int | `ManagedBuffer` | 获取RddBlockData相关功能 | 传入参数执行获取RddBlockData相关功能 |
| `getRegisteredExecutorsSize` | 无 | `int` | 获取RegisteredExecutorsSize相关功能 | 调用该方法执行获取RegisteredExecutorsSize相关功能 |
| `load` | filePath: String | `ShuffleIndexInformation` | 加载相关功能 | 传入参数执行加载相关功能 |
| `registerExecutor` | appId: String, execId: String, executorInfo: ExecutorShuffleInfo | `void` | 注册Executor相关功能 | 传入参数执行注册Executor相关功能 |
| `removeBlocks` | appId: String, execId: String, blockIds: String&lt;&gt; | `int` | 移除Blocks相关功能 | 传入参数执行移除Blocks相关功能 |



### HiveServer2
**包路径**: `org.apache.hive.service.server`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `BoxedUnit` | 应用数据类型转换 | 获取数据类型对应的列向量 |
| `execute` | 无 | `void` | 执行相关功能 | 调用该方法执行执行相关功能 |
| `isHTTPTransportMode` | hiveConf: HiveConf | `boolean` | 判断是否HTTPTransportMode相关功能 | 传入参数执行判断是否HTTPTransportMode相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `parse` | argv: String&lt;&gt; | `ServerOptionsProcessorResponse` | 解析相关功能 | 传入参数执行解析相关功能 |


### TransportConf
**包路径**: `org.apache.spark.network.util`
**方法数量**: 58

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `authEngineVersion` | 无 | `int` | 认证引擎版本 | 调用该方法执行认证引擎版本 |
| `authRTTimeoutMs` | 无 | `int` | 认证超时时间（毫秒） | 调用该方法执行认证超时时间（毫秒） |
| `backLog` | 无 | `int` | 获取待处理日志 | 调用该方法执行获取待处理日志 |
| `chunkFetchHandlerThreads` | 无 | `int` | 读取相关功能 | 调用该方法执行读取相关功能 |
| `cipherTransformation` | 无 | `String` | 转换相关功能 | 调用该方法执行转换相关功能 |
| `clientThreads` | 无 | `int` | 读取相关功能 | 调用该方法执行读取相关功能 |
| `connectionCreationTimeoutMs` | 无 | `int` | 连接ionCreationTimeoutMs相关功能 | 调用该方法执行连接ionCreationTimeoutMs相关功能 |
| `connectionTimeoutMs` | 无 | `int` | 连接ionTimeoutMs相关功能 | 调用该方法执行连接ionTimeoutMs相关功能 |
| `cryptoConf` | 无 | `Properties` | cryptoConf操作 | 调用该方法执行cryptoConf操作 |
| `enableSaslRetries` | 无 | `boolean` | 启用SaslRetries相关功能 | 调用该方法执行启用SaslRetries相关功能 |
| `enableTcpKeepAlive` | 无 | `boolean` | 启用TcpKeepAlive相关功能 | 调用该方法执行启用TcpKeepAlive相关功能 |
| `encryptionEnabled` | 无 | `boolean` | 加密ionEnabled相关功能 | 调用该方法执行加密ionEnabled相关功能 |
| `finalizeShuffleMergeHandlerThreads` | 无 | `int` | 终结ShuffleMergeHandlerThreads相关功能 | 调用该方法执行终结ShuffleMergeHandlerThreads相关功能 |
| `get` | name: String, defaultValue: String | `String` | 获取元素 | 传入参数执行获取相关功能 |
| `getInt` | name: String, defaultValue: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getModuleName` | 无 | `String` | 获取ModuleName相关功能 | 调用该方法执行获取ModuleName相关功能 |
| `ioExceptionsThresholdDuringMerge` | 无 | `int` | 合并相关功能 | 调用该方法执行合并相关功能 |
| `ioMode` | 无 | `String` | ioMode操作 | 调用该方法执行ioMode操作 |
| `ioRetryWaitTimeMs` | 无 | `int` | 等待相关功能 | 调用该方法执行等待相关功能 |
| `lazyFileDescriptor` | 无 | `boolean` | lazyFileDescriptor操作 | 调用该方法执行lazyFileDescriptor操作 |
| `maxChunksBeingTransferred` | 无 | `long` | 转移相关功能 | 调用该方法执行转移相关功能 |
| `maxIORetries` | 无 | `int` | 三相关功能 | 调用该方法执行三相关功能 |
| `maxSaslEncryptedBlockSize` | 无 | `int` | 锁定相关功能 | 调用该方法执行锁定相关功能 |
| `memoryMapBytes` | 无 | `int` | 映射相关功能 | 调用该方法执行映射相关功能 |
| `mergedIndexCacheSize` | 无 | `long` | 合并dIndexCacheSize相关功能 | 调用该方法执行合并dIndexCacheSize相关功能 |
| `mergedShuffleCleanerShutdownTimeout` | 无 | `long` | 合并dShuffleCleanerShutdownTimeout相关功能 | 调用该方法执行合并dShuffleCleanerShutdownTimeout相关功能 |
| `mergedShuffleFileManagerImpl` | 无 | `String` | 合并dShuffleFileManagerImpl相关功能 | 调用该方法执行合并dShuffleFileManagerImpl相关功能 |
| `minChunkSizeInMergedShuffleFile` | 无 | `int` | 合并相关功能 | 调用该方法执行合并相关功能 |
| `numConnectionsPerPeer` | 无 | `int` | 连接相关功能 | 调用该方法执行连接相关功能 |
| `portMaxRetries` | 无 | `int` | 三相关功能 | 调用该方法执行三相关功能 |
| `preferDirectBufs` | 无 | `boolean` | 前ferDirectBufs相关功能 | 调用该方法执行前ferDirectBufs相关功能 |
| `preferDirectBufsForSharedByteBufAllocators` | 无 | `boolean` | 前ferDirectBufsForSharedByteBufAllocators相关功能 | 调用该方法执行前ferDirectBufsForSharedByteBufAllocators相关功能 |
| `receiveBuf` | 无 | `int` | 接收Buf相关功能 | 调用该方法执行接收Buf相关功能 |
| `saslEncryption` | 无 | `boolean` | 加密相关功能 | 调用该方法执行加密相关功能 |
| `saslFallback` | 无 | `boolean` | saslFallback操作 | 调用该方法执行saslFallback操作 |
| `saslServerAlwaysEncrypt` | 无 | `boolean` | 加密相关功能 | 调用该方法执行加密相关功能 |
| `sendBuf` | 无 | `int` | 发送Buf相关功能 | 调用该方法执行发送Buf相关功能 |
| `separateChunkFetchRequest` | 无 | `boolean` | 请求相关功能 | 调用该方法执行请求相关功能 |
| `separateFinalizeShuffleMerge` | 无 | `boolean` | 合并相关功能 | 调用该方法执行合并相关功能 |
| `serverThreads` | 无 | `int` | 读取相关功能 | 调用该方法执行读取相关功能 |
| `sharedByteBufAllocators` | 无 | `boolean` | sharedByteBufAllocators操作 | 调用该方法执行sharedByteBufAllocators操作 |
| `sslRpcCertChain` | 无 | `File` | sslRpcCertChain操作 | 调用该方法执行sslRpcCertChain操作 |
| `sslRpcEnabled` | 无 | `boolean` | 启用相关功能 | 调用该方法执行启用相关功能 |
| `sslRpcEnabledAndKeysAreValid` | 无 | `boolean` | 启用相关功能 | 调用该方法执行启用相关功能 |
| `sslRpcKeyPassword` | 无 | `String` | sslRpcKeyPassword操作 | 调用该方法执行sslRpcKeyPassword操作 |
| `sslRpcKeyStore` | 无 | `File` | sslRpcKeyStore操作 | 调用该方法执行sslRpcKeyStore操作 |
| `sslRpcKeyStorePassword` | 无 | `String` | sslRpcKeyStorePassword操作 | 调用该方法执行sslRpcKeyStorePassword操作 |
| `sslRpcOpenSslEnabled` | 无 | `boolean` | 打开相关功能 | 调用该方法执行打开相关功能 |
| `sslRpcPrivateKey` | 无 | `File` | 私有相关功能 | 调用该方法执行私有相关功能 |
| `sslRpcPrivateKeyPassword` | 无 | `String` | 私有相关功能 | 调用该方法执行私有相关功能 |
| `sslRpcProtocol` | 无 | `String` | 原型相关功能 | 调用该方法执行原型相关功能 |
| `sslRpcTrustStore` | 无 | `File` | sslRpcTrustStore操作 | 调用该方法执行sslRpcTrustStore操作 |
| `sslRpcTrustStorePassword` | 无 | `String` | sslRpcTrustStorePassword操作 | 调用该方法执行sslRpcTrustStorePassword操作 |
| `sslRpcTrustStoreReloadingEnabled` | 无 | `boolean` | 加载相关功能 | 调用该方法执行加载相关功能 |
| `sslRpctrustStoreReloadIntervalMs` | 无 | `int` | 加载相关功能 | 调用该方法执行加载相关功能 |
| `sslShuffleChunkSize` | 无 | `int` | 随机打乱相关功能 | 调用该方法执行随机打乱相关功能 |
| `useOldFetchProtocol` | 无 | `boolean` | 获取相关功能 | 调用该方法执行获取相关功能 |
| `verboseMetrics` | 无 | `boolean` | 三相关功能 | 调用该方法执行三相关功能 |



### HiveAuthFactory
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | delegationToken: String | `void` | 判断能否celDelegationToken相关功能 | 传入参数执行判断能否celDelegationToken相关功能 |
| `getAuthName` | 无 | `String` | 获取AuthName相关功能 | 调用该方法执行获取AuthName相关功能 |
| `getAuthProcFactory` | service: ThriftCLIService | `TProcessorFactory` | 获取AuthProcFactory相关功能 | 传入参数执行获取AuthProcFactory相关功能 |
| `getAuthTransFactory` | 无 | `TTransportFactory` | 获取AuthTransFactory相关功能 | 调用该方法执行获取AuthTransFactory相关功能 |
| `getDelegationToken` | owner: String, renewer: String, remoteAddr: String | `String` | 获取DelegationToken相关功能 | 传入参数执行获取DelegationToken相关功能 |
| `getIpAddress` | 无 | `String` | 获取IpAddress相关功能 | 调用该方法执行获取IpAddress相关功能 |
| `getRemoteUser` | 无 | `String` | 获取RemoteUser相关功能 | 调用该方法执行获取RemoteUser相关功能 |
| `getSaslProperties` | 无 | `Map&lt;String, String&gt;` | 获取SaslProperties相关功能 | 调用该方法执行获取SaslProperties相关功能 |
| `getUserFromToken` | delegationToken: String | `String` | 获取UserFromToken相关功能 | 传入参数执行获取UserFromToken相关功能 |
| `loginFromKeytab` | hiveConf: HiveConf | `void` | 日志inFromKeytab相关功能 | 传入参数执行日志inFromKeytab相关功能 |
| `loginFromSpnegoKeytabAndReturnUGI` | hiveConf: HiveConf | `UserGroupInformation` | 日志inFromSpnegoKeytabAndReturnUGI相关功能 | 传入参数执行日志inFromSpnegoKeytabAndReturnUGI相关功能 |
| `needUgiLogin` | ugi: UserGroupInformation, principal: String, keytab: String | `boolean` | 需要UgiLogin相关功能 | 传入参数执行需要UgiLogin相关功能 |
| `renewDelegationToken` | delegationToken: String | `void` | renewDelegationToken操作 | 传入参数执行renewDelegationToken操作 |
| `verifyDelegationToken` | delegationToken: String | `String` | 验证DelegationToken相关功能 | 传入参数执行验证DelegationToken相关功能 |
| `verifyProxyAccess` | realUser: String, proxyUser: String, ipAddress: String, hiveConf: HiveConf | `void` | 验证ProxyAccess相关功能 | 传入参数执行验证ProxyAccess相关功能 |


### VariantVal
**包路径**: `org.apache.spark.unsafe.types`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `debugString` | 无 | `String` | 调试String相关功能 | 调用该方法执行调试String相关功能 |
| `readFromUnsafeRow` | offsetAndSize: long, baseObject: Object, baseOffset: long | `VariantVal` | 读取FromUnsafeRow相关功能 | 传入参数执行读取FromUnsafeRow相关功能 |
| `toJson` | zoneId: ZoneId | `String` | toJson操作 | 传入参数执行toJson操作 |



### ExecutorShuffleInfo
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `ExecutorShuffleInfo` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### OperationType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationType` | tOperationType: TOperationType | `OperationType` | 获取OperationType相关功能 | 传入参数执行获取OperationType相关功能 |
| `toTOperationType` | 无 | `TOperationType` | 顶部相关功能 | 调用该方法执行顶部相关功能 |


### FetchShuffleBlockChunks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FetchShuffleBlockChunks` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `getNumBlocks` | 无 | `int` | 获取NumBlocks相关功能 | 调用该方法执行获取NumBlocks相关功能 |



### HandleIdentifier
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getPublicId` | 无 | `UUID` | 获取PublicId相关功能 | 调用该方法执行获取PublicId相关功能 |
| `getSecretId` | 无 | `UUID` | 获取SecretId相关功能 | 调用该方法执行获取SecretId相关功能 |
| `toTHandleIdentifier` | 无 | `THandleIdentifier` | 处理相关功能 | 调用该方法执行处理相关功能 |



### TimerWithCustomTimeUnit
**包路径**: `org.apache.spark.network.util`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `dump` | outputStream: OutputStream | `void` | dump操作 | 传入参数执行dump操作 |
| `getMax` | 无 | `long` | 获取Max相关功能 | 调用该方法执行获取Max相关功能 |
| `getMean` | 无 | `double` | 获取Mean相关功能 | 调用该方法执行获取Mean相关功能 |
| `getMin` | 无 | `long` | 获取Min相关功能 | 调用该方法执行获取Min相关功能 |
| `getSnapshot` | 无 | `Snapshot` | 获取Snapshot相关功能 | 调用该方法执行获取Snapshot相关功能 |
| `getStdDev` | 无 | `double` | 获取StdDev相关功能 | 调用该方法执行获取StdDev相关功能 |
| `getValue` | v: double | `double` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `size` | 无 | `int` | 计算大小 | 调用该方法执行size操作 |



### AbstractMessage
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `body` | 无 | `ManagedBuffer` | body操作 | 调用该方法执行body操作 |
| `isBodyInFrame` | 无 | `boolean` | 判断是否BodyInFrame相关功能 | 调用该方法执行判断是否BodyInFrame相关功能 |



### OperationHandle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationType` | 无 | `OperationType` | 获取OperationType相关功能 | 调用该方法执行获取OperationType相关功能 |
| `getProtocolVersion` | 无 | `TProtocolVersion` | 获取ProtocolVersion相关功能 | 调用该方法执行获取ProtocolVersion相关功能 |
| `hasResultSet` | 无 | `boolean` | 检查是否存在ResultSet相关功能 | 调用该方法执行检查是否存在ResultSet相关功能 |
| `setHasResultSet` | hasResultSet: boolean | `void` | 设置HasResultSet相关功能 | 传入参数执行设置HasResultSet相关功能 |
| `toTOperationHandle` | 无 | `TOperationHandle` | 处理相关功能 | 调用该方法执行处理相关功能 |



### RocksDBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkVersion` | db: RocksDB, newversion: StoreVersion, mapper: ObjectMapper | `void` | 检查Version相关功能 | 传入参数执行检查Version相关功能 |
| `initRockDB` | dbFile: File, version: StoreVersion, mapper: ObjectMapper | `RocksDB` | 初始化RockDB相关功能 | 传入参数执行初始化RockDB相关功能 |
| `storeVersion` | db: RocksDB, version: StoreVersion, mapper: ObjectMapper | `void` | 版本相关功能 | 传入参数执行版本相关功能 |



### RegisterExecutor
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RegisterExecutor` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### ExecuteStatementOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStatement` | 无 | `String` | 获取Statement相关功能 | 调用该方法执行获取Statement相关功能 |


### AmIpPrincipal
**包路径**: `org.apache.spark.deploy.yarn`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |


### MetadataOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |



### CLIServiceClient
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fetchResults` | opHandle: OperationHandle | `TRowSet` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `openSession` | username: String, password: String | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |


### RocksDBIterator
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `seek` | key: byte&lt;&gt; | `void` | 定位相关功能 | 传入参数执行定位相关功能 |



### SparkSaslClient
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNegotiatedProperty` | name: String | `Object` | 获取NegotiatedProperty相关功能 | 传入参数执行获取NegotiatedProperty相关功能 |
| `handle` | callbacks: Callback&lt;&gt; | `void` | 处理相关功能 | 传入参数执行处理相关功能 |



### SaslServerBootstrap
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | channel: Channel, rpcHandler: RpcHandler | `RpcHandler` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |


### CustomTaskMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |
| `value` | 无 | `long` | 获取度量指标值 | 返回度量指标数值 |



### UnsafeAlignedOffset
**包路径**: `org.apache.spark.unsafe`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSize` | object: Object, offset: long | `int` | 获取Size相关功能 | 传入参数执行获取Size相关功能 |
| `getUaoSize` | 无 | `int` | 获取UaoSize相关功能 | 调用该方法执行获取UaoSize相关功能 |
| `putSize` | object: Object, offset: long, value: int | `void` | 放入Size相关功能 | 传入参数执行放入Size相关功能 |
| `setUaoSize` | size: int | `void` | 设置UaoSize相关功能 | 传入参数执行设置UaoSize相关功能 |



### BestEffortLazyVal
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `T` | 应用数据类型转换 | 获取数据类型对应的列向量 |



### ChunkFetchRequestHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `exceptionCaught` | ctx: ChannelHandlerContext, cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `processFetchRequest` | channel: final Channel, msg: final ChunkFetchRequest | `void` | 处理FetchRequest相关功能 | 传入参数执行处理FetchRequest相关功能 |



### TableTypeMappingFactory
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeMapping` | mappingType: String | `TableTypeMapping` | 获取TableTypeMapping相关功能 | 传入参数执行获取TableTypeMapping相关功能 |



### GetPrimaryKeysOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### GetColumnsOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### GetCrossReferenceOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |



### TaskSorting
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `TaskSorting` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |



### SortDirection
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultNullOrdering` | 无 | `NullOrdering` | 默认NullOrdering相关功能 | 调用该方法执行默认NullOrdering相关功能 |


### TransportRequestHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelActive` | 无 | `void` | 活跃相关功能 | 调用该方法执行活跃相关功能 |
| `channelInactive` | 无 | `void` | 活跃相关功能 | 调用该方法执行活跃相关功能 |
| `exceptionCaught` | cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `getID` | 无 | `String` | 获取ID相关功能 | 调用该方法执行获取ID相关功能 |
| `handle` | request: RequestMessage | `void` | 处理相关功能 | 传入参数执行处理相关功能 |
| `onComplete` | streamId: String | `void` | 完成相关功能 | 传入参数执行完成相关功能 |
| `onData` | streamId: String, buf: ByteBuffer | `void` | onData操作 | 传入参数执行onData操作 |
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onFailure` | streamId: String, cause: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `onSuccess` | numChunks: int, buffer: ManagedBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |



### HashMapGrowthStrategy
**包路径**: `org.apache.spark.unsafe.map`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `nextCapacity` | currentCapacity: int | `int` | 之后Capacity相关功能 | 传入参数执行之后Capacity相关功能 |



### NettyMemoryMetrics
**包路径**: `org.apache.spark.network.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMetrics` | 无 | `Map&lt;String, Metric&gt;` | 获取Metrics相关功能 | 调用该方法执行获取Metrics相关功能 |


### ColumnVector
**包路径**: `org.apache.spark.sql.vectorized`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | t: DataType | `DataType` | 应用数据类型转换 | 获取数据类型对应的列向量 |
| `closeIfFreeable` | 无 | `void` | 检查并释放可释放的资源 | 检查并释放可释放的列向量资源 |
| `dataType` | 无 | `DataType` | 获取数据类型 | 返回Cast目标的数据类型 |
| `getGeography` | rowId: int | `GeographyVal` | 获取地理空间数据值 | 获取地理空间数据值 |
| `getGeometry` | rowId: int | `GeometryVal` | 获取几何空间数据值 | 获取几何空间数据值 |
| `getInterval` | rowId: int | `CalendarInterval` | 获取时间间隔值 | 获取时间间隔数据 |
| `getStruct` | rowId: int | `ColumnarRow` | 获取Struct类型数据 | 获取Struct结构数据 |
| `getVariant` | rowId: int | `VariantVal` | 获取Variant类型数据 | 获取Variant变体数据 |
| `isDefinedAt` | x: DataType | `boolean` | 检查数据类型是否定义 | 检查数据类型是否已定义 |


### SparkOrcNewRecordReader
**包路径**: `org.apache.hadoop.hive.ql.io.orc`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getCurrentKey` | 无 | `NullWritable` | 获取CurrentKey相关功能 | 调用该方法执行获取CurrentKey相关功能 |
| `getCurrentValue` | 无 | `OrcStruct` | 获取CurrentValue相关功能 | 调用该方法执行获取CurrentValue相关功能 |
| `getObjectInspector` | 无 | `ObjectInspector` | 获取ObjectInspector相关功能 | 调用该方法执行获取ObjectInspector相关功能 |
| `getProgress` | 无 | `float` | 获取Progress相关功能 | 调用该方法执行获取Progress相关功能 |
| `initialize` | split: InputSplit, context: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `nextKeyValue` | 无 | `boolean` | 之后KeyValue相关功能 | 调用该方法执行之后KeyValue相关功能 |



### PrimaryKey
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `PrimaryKey` | 构建约束对象 | 构建Check约束对象 |



### AuthServerBootstrap
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | channel: Channel, rpcHandler: RpcHandler | `RpcHandler` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |



### ColumnBasedSet
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addRow` | fields: Object&lt;&gt; | `ColumnBasedSet` | 添加行到批处理 | 传入参数执行添加行到批处理 |
| `extractSubset` | maxRows: int | `ColumnBasedSet` | 额外ctSubset相关功能 | 传入参数执行额外ctSubset相关功能 |
| `getColumns` | 无 | `List&lt;ColumnBuffer&gt;` | 获取Columns相关功能 | 调用该方法执行获取Columns相关功能 |
| `getStartOffset` | 无 | `long` | 获取StartOffset相关功能 | 调用该方法执行获取StartOffset相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `iterator` | 无 | `Iterator&lt;Object[]&gt;` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `numColumns` | 无 | `int` | 列相关功能 | 调用该方法执行列相关功能 |
| `numRows` | 无 | `int` | numRows操作 | 调用该方法执行numRows操作 |
| `setStartOffset` | startOffset: long | `void` | 设置StartOffset相关功能 | 传入参数执行设置StartOffset相关功能 |
| `toTRowSet` | 无 | `TRowSet` | 设置相关功能 | 调用该方法执行设置相关功能 |



### UploadBlock
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `UploadBlock` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### AbstractFileRegion
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `retain` | 无 | `AbstractFileRegion` | retain操作 | 调用该方法执行retain操作 |
| `retain` | increment: int | `AbstractFileRegion` | retain操作 | 传入参数执行retain操作 |
| `touch` | 无 | `AbstractFileRegion` | touch操作 | 调用该方法执行touch操作 |
| `touch` | o: Object | `AbstractFileRegion` | touch操作 | 传入参数执行touch操作 |
| `transfered` | 无 | `long` | 转移ed相关功能 | 调用该方法执行转移ed相关功能 |



### MergedBlockMetaSuccess
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createFailureResponse` | error: String | `ResponseMessage` | 创建FailureResponse相关功能 | 传入参数执行创建FailureResponse相关功能 |
| `decode` | buf: ByteBuf | `MergedBlockMetaSuccess` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `getNumChunks` | 无 | `int` | 获取NumChunks相关功能 | 调用该方法执行获取NumChunks相关功能 |
| `type` | 无 | `Type` | type操作 | 调用该方法执行type操作 |



### ThriftBinaryCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GetQueryId` | req: TGetQueryIdReq | `TGetQueryIdResp` | 获取QueryId相关功能 | 传入参数执行获取QueryId相关功能 |
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |



### TransientBestEffortLazyVal
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `T` | 应用数据类型转换 | 获取数据类型对应的列向量 |



### MemoryBlock
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fill` | value: byte | `void` | fill操作 | 传入参数执行fill操作 |
| `fromLongArray` | array: final long&lt;&gt; | `MemoryBlock` | fromLongArray操作 | 传入参数执行fromLongArray操作 |
| `size` | 无 | `long` | 计算大小 | 调用该方法执行size操作 |



### MutableURLClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addURL` | url: URL | `void` | 添加URL | 传入参数执行添加URL |



### WriteBuilder
**包路径**: `org.apache.spark.sql.connector.write`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toBatch` | 无 | `BatchWrite` | 将行迭代器转换为列式批处理 | 将行迭代器转为列式批处理 |
| `toStreaming` | 无 | `StreamingWrite` | toStreaming操作 | 调用该方法执行toStreaming操作 |


### LocalDiskShuffleDataIO
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `driver` | 无 | `ShuffleDriverComponents` | driver操作 | 调用该方法执行driver操作 |
| `executor` | 无 | `ShuffleExecutorComponents` | executor操作 | 调用该方法执行executor操作 |



### TypeQualifiers
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromTTypeQualifiers` | ttq: TTypeQualifiers | `TypeQualifiers` | 判断相等相关功能 | 传入参数执行判断相等相关功能 |
| `fromTypeInfo` | pti: PrimitiveTypeInfo | `TypeQualifiers` | fromTypeInfo操作 | 传入参数执行fromTypeInfo操作 |
| `getCharacterMaximumLength` | 无 | `Integer` | 获取CharacterMaximumLength相关功能 | 调用该方法执行获取CharacterMaximumLength相关功能 |
| `getPrecision` | 无 | `Integer` | 获取Precision相关功能 | 调用该方法执行获取Precision相关功能 |
| `getScale` | 无 | `Integer` | 获取Scale相关功能 | 调用该方法执行获取Scale相关功能 |
| `setCharacterMaximumLength` | characterMaximumLength: int | `void` | 设置CharacterMaximumLength相关功能 | 传入参数执行设置CharacterMaximumLength相关功能 |
| `setPrecision` | precision: Integer | `void` | 设置Precision相关功能 | 传入参数执行设置Precision相关功能 |
| `setScale` | scale: Integer | `void` | 设置Scale相关功能 | 传入参数执行设置Scale相关功能 |
| `toTTypeQualifiers` | 无 | `TTypeQualifiers` | 判断相等相关功能 | 调用该方法执行判断相等相关功能 |



### MemoryLocation
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getBaseObject` | 无 | `Object` | 获取BaseObject相关功能 | 调用该方法执行获取BaseObject相关功能 |
| `getBaseOffset` | 无 | `long` | 获取BaseOffset相关功能 | 调用该方法执行获取BaseOffset相关功能 |
| `setObjAndOffset` | newObj: Object, newOffset: long | `void` | 设置ObjAndOffset相关功能 | 传入参数执行设置ObjAndOffset相关功能 |



### ClassicTableTypeMapping
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeNames` | 无 | `Set&lt;String&gt;` | 获取TableTypeNames相关功能 | 调用该方法执行获取TableTypeNames相关功能 |
| `mapToClientType` | hiveTypeName: String | `String` | 映射ToClientType相关功能 | 传入参数执行映射ToClientType相关功能 |



### NamespaceChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | `String` | property操作 | 调用该方法执行property操作 |
| `value` | 无 | `String` | 获取度量指标值 | 返回度量指标数值 |


### Extract
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `field` | 无 | `String` | field操作 | 调用该方法执行field操作 |
| `source` | 无 | `Expression` | 源相关功能 | 调用该方法执行源相关功能 |



### FinalizeShuffleMerge
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FinalizeShuffleMerge` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |



### NoOpRpcHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStreamManager` | 无 | `StreamManager` | 获取StreamManager相关功能 | 调用该方法执行获取StreamManager相关功能 |
| `receive` | client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback | `void` | 接收相关功能 | 传入参数执行接收相关功能 |



### ThreadWithGarbageCleanup
**包路径**: `org.apache.hive.service.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cacheThreadLocalRawStore` | 无 | `void` | 缓存ThreadLocalRawStore相关功能 | 调用该方法执行缓存ThreadLocalRawStore相关功能 |
| `finalize` | 无 | `void` | 终结相关功能 | 调用该方法执行终结相关功能 |



### KVTypeInfo
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | instance: Object | `Object` | 获取元素 | 传入参数执行获取相关功能 |
| `getIndexValue` | indexName: String, instance: Object | `Object` | 获取IndexValue相关功能 | 传入参数执行获取IndexValue相关功能 |
| `getType` | 无 | `Class&lt;?&gt;` | 获取Type相关功能 | 调用该方法执行获取Type相关功能 |
| `indices` | 无 | `Stream&lt;KVIndex&gt;` | indices操作 | 调用该方法执行indices操作 |
| `type` | 无 | `Class&lt;?&gt;` | type操作 | 调用该方法执行type操作 |


### AbstractService
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getStartTime` | 无 | `long` | 获取StartTime相关功能 | 调用该方法执行获取StartTime相关功能 |



