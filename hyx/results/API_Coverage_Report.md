# API覆盖度分析报告

> 分析时间: 2026-05-06T14:49:06.146151

## 一、总体统计

| 指标 | 数值 |
|------|------|
| 总API数 | 380 |
| 已测试API数 | 233 |
| 缺失API数 | 226 |
| 覆盖率 | 40.53% |
| 测试文件数 | 171 |

## 二、按组件统计

| 组件 | 总API数 | 已测试 | 缺失 | 覆盖率 |
|------|--------|--------|------|--------|
| spark | 224 | 153 | 71 | 68.30% |
| kafka | 156 | 1 | 155 | 0.64% |

## 三、按稳定性统计

| 稳定性 | 总API数 | 已测试 | 缺失 | 覆盖率 |
|--------|--------|--------|------|--------|
| Stable | 218 | 102 | 116 | 46.79% |
| DeveloperApi | 1 | 0 | 1 | 0.00% |
| Deprecated | 76 | 52 | 24 | 68.42% |
| Unknown | 83 | 0 | 83 | 0.00% |
| Evolving | 2 | 0 | 2 | 0.00% |

## 四、已测试API对比

> 已测试的类和方法，可对比API清单验证

### 4.1 已测试类列表

共 233 个类在测试文件中被引用

 `AbstractGenericUDAFResolver` | `AggregateFunction` | `Aggregator` | `Algo` | `AnalysisException` 
 `Attribute` | `AttributeGroup` | `Batch` | `BinaryClassificationEvaluator` | `BinarySample` 
 `BloomFilter` | `BoundFunction` | `BoundedDouble` | `ByteArrayMethods` | `CannotReplaceMissingTableException` 
 `CaseInsensitiveStringMap` | `ChiSqTestResult` | `Column` | `ColumnVector` | `ColumnarBatch` 
 `Configuration` | `CountMinSketch` | `CryptoStreamUtils` | `DataFrameWriter` | `DataType` 
 `DataTypes` | `Dataset` | `DateTimeUtils` | `DayTimeIntervalType` | `DecisionTreeModel` 
 `DefaultCodec` | `DenseVector` | `DoubleType` | `Duration` | `Durations` 
 `Encoder` | `Encoders` | `EncodingUtils` | `ExecutorResourceRequests` | `Expression` 
 `ExpressionInfo` | `Expressions` | `FeatureData` | `FieldReference` | `FileSystem` 
 `Filter` | `FilterFunction` | `ForeachWriter` | `FreqItemset` | `FreqSequence` 
 `Function` | `FunctionIdentifier` | `GenericInternalRow` | `GenericRow` | `GenericUDAFEvaluator` 
 `GenericUDF` | `Gini` | `GreaterThan` | `HashPartitioner` | `IScheme` 
 `InMemoryTableCatalog` | `IndexShuffleBlockResolver` | `InputPartition` | `IntWritable` | `IntegerType` 
 `InternalRow` | `JavaCheckpointTestUtils` | `JavaDStream` | `JavaDoubleRDD` | `JavaFutureAction` 
 `JavaMapWithStateDStream` | `JavaPairDStream` | `JavaPairRDD` | `JavaRDD` | `JavaReceiverInputDStream` 
 `JavaSerializer` | `JavaSparkContext` | `JavaStreamingContext` | `JavaTestUtils` | `JavaTypeInferenceBeans` 
 `JavaUtils` | `JdbcRDD` | `Job` | `KMSClientProvider` | `KeyGroupedPartitioning` 
 `KeyProvider` | `KeyProviderCryptoExtension` | `KeyProviderFactory` | `KeyValueGroupedDataset` | `KolmogorovSmirnovTestResult` 
 `KryoSerializer` | `LZFCompressionCodec` | `LabeledPoint` | `LimitedInputStream` | `LinearDataGenerator` 
 `Literal` | `LiteralValue` | `LocalDiskShuffleExecutorComponents` | `LocalJavaStreamingContext` | `LogisticRegression` 
 `LogisticRegressionSuite` | `LongAccumulator` | `LongArray` | `LongType` | `LongWritable` 
 `ManualClock` | `MapFunction` | `MapStatus` | `Matrix` | `MemoryAllocator` 
 `MemoryBlock` | `MemoryConsumer` | `MemoryMode` | `Metadata` | `MutableAggregationBuffer` 
 `MyDoubleSum` | `NoSuchTableException` | `NumericAttribute` | `OnHeapColumnVector` | `Optional` 
 `OuterScopes` | `PairFunction` | `ParamMap` | `PartialResult` | `PartitionReader` 
 `PartitionReaderFactory` | `Partitioner` | `Partitioning` | `Path` | `Platform` 
 `PortableDataStream` | `Predicate` | `QRDecomposition` | `RDD` | `Receiver` 
 `RecordBinaryComparator` | `ReduceFunction` | `ResourceInformation` | `ResourceProfile` | `ResourceProfileBuilder` 
 `Row` | `RowBasedChecksum` | `RowFactory` | `RowMatrix` | `SQLConf` 
 `SQLContext` | `SaveMode` | `ScalarFunction` | `Scan` | `ScanBuilder` 
 `SchemeFactory` | `Seconds` | `SemanticException` | `SequenceFileInputFormat` | `SequenceFileOutputFormat` 
 `SerializableConfiguration` | `SerializerInstance` | `SerializerManager` | `SharedSparkSession` | `ShuffleChecksumHelper` 
 `ShuffleChecksumTestHelper` | `ShuffleWriteMetrics` | `SimpleCounter` | `SnappyCompressionCodec` | `SparkConf` 
 `SparkContext` | `SparkHadoopUtil` | `SparkIllegalArgumentException` | `SparkOutOfMemoryError` | `SparkSession` 
 `SparkUnsupportedOperationException` | `Stable` | `StandardScaler` | `StandardScheme` | `StatCounter` 
 `State` | `StateSpec` | `Statistics` | `StorageLevel` | `Strategy` 
 `StreamingContextState` | `StreamingContextSuite` | `StreamingQuery` | `StreamingTest` | `StringType` 
 `StructField` | `StructType` | `SupportsRead` | `SupportsReportStatistics` | `SupportsWrite` 
 `TTupleProtocol` | `Table` | `TableAlreadyExistsException` | `TableCapability` | `TableIdentifier` 
 `TableProvider` | `TaskCompletionListener` | `TaskContext` | `TaskFailureListener` | `TaskInterruptListener` 
 `TaskMemoryManager` | `TaskMetrics` | `TaskResourceRequests` | `TestMemoryConsumer` | `TestMemoryManager` 
 `TestSparkSession` | `Text` | `TextInputFormat` | `Time` | `TimeType` 
 `TimestampFormatter` | `Transform` | `TreeTests` | `TupleScheme` | `UDF` 
 `UDFArgumentException` | `UnboundFunction` | `UnknownPartitioning` | `UnsafeAlignedOffset` | `UnsafeArrayData` 
 `UnsafeRow` | `UserDefinedAggregateFunction` | `UserDefinedFunction` | `Utils` | `Vector` 
 `VectorUDT` | `Vectors` | `VoidFunction` | `Window` | `WriteAheadLog` 
 `WriteAheadLogRecordHandle` | `WriteAheadLogUtils` | `XmlOptions` 

### 4.2 已测试API详细对比（示例）

| 类名 | 测试覆盖的方法（推测） | API清单定义的方法 |
|------|----------------------|------------------|

## 五、缺失API详细列表（优先级P0）

> 包含完整方法签名，便于对比和测试生成

### 5.1 P0优先级缺失API（Stable）

共 116 个核心API缺失，应优先补充测试

| 组件 | 类名 | 方法名 | 方法签名 | 返回类型 | 参数 | 稳定性 |
|------|------|--------|----------|----------|------|--------|
| spark | **JavaRDDLike** | `map` | `JavaRDD<R> map(Function<T,R> f)` | `JavaRDD<R>` | `Function<T, R>` | Stable |
| spark | **JavaRDDLike** | `mapToDouble` | `JavaDoubleRDD mapToDouble(DoubleFunction<T> f)` | `JavaDoubleRDD` | `DoubleFunction<T>` | Stable |
| spark | **JavaRDDLike** | `mapToPair` | `JavaPairRDD<K,V> mapToPair(PairFunction<T,K,V> f)` | `JavaPairRDD<K,V>` | `PairFunction<T, K, V>` | Stable |
| spark | **JavaRDDLike** | `flatMap` | `JavaRDD<U> flatMap(FlatMapFunction<T,U> f)` | `JavaRDD<U>` | `FlatMapFunction<T, U>` | Stable |
| spark | **JavaRDDLike** | `flatMapToDouble` | `JavaDoubleRDD flatMapToDouble(DoubleFlatMapFunction<T> f)` | `JavaDoubleRDD` | `DoubleFlatMapFunction<T>` | Stable |
| spark | **JavaRDDLike** | `flatMapToPair` | `JavaPairRDD<K,V> flatMapToPair(PairFlatMapFunction<T,K,V> f)` | `JavaPairRDD<K,V>` | `PairFlatMapFunction<T, K, V>` | Stable |
| spark | **JavaRDDLike** | `mapPartitions` | `JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<T>,U> f)` | `JavaRDD<U>` | `FlatMapFunction<Iterator<T>, U>` | Stable |
| spark | **JavaRDDLike** | `mapPartitionsWithIndex` | `JavaRDD<R> mapPartitionsWithIndex(Function2<Integer,Iterator...` | `JavaRDD<R>` | `Function2<Integer, Iterator<T>, Iterator<R>>` | Stable |
| spark | **JavaRDDLike** | `mapPartitionsToDouble` | `JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapFunction<It...` | `JavaDoubleRDD` | `DoubleFlatMapFunction<Iterator<T>>` | Stable |
| spark | **JavaRDDLike** | `mapPartitionsToPair` | `JavaPairRDD<K,V> mapPartitionsToPair(PairFlatMapFunction<Ite...` | `JavaPairRDD<K,V>` | `PairFlatMapFunction<Iterator<T>, K, V>` | Stable |
| spark | **JavaRDDLike** | `filter` | `JavaRDD<T> filter(Function<T,Boolean> f)` | `JavaRDD<T>` | `Function<T, Boolean>` | Stable |
| spark | **JavaRDDLike** | `cartesian` | `JavaPairRDD<T,U> cartesian(JavaRDDLike<U,?> other)` | `JavaPairRDD<T,U>` | `JavaRDDLike<U, ?>` | Stable |
| spark | **JavaRDDLike** | `groupBy` | `JavaPairRDD<U,Iterable<T>> groupBy(Function<T,U> f)` | `JavaPairRDD<U,Iterable<T>>` | `Function<T, U>` | Stable |
| spark | **JavaRDDLike** | `pipe` | `JavaRDD<String> pipe(List<String> command)` | `JavaRDD<String>` | `List<String>` | Stable |
| spark | **JavaRDDLike** | `zip` | `JavaPairRDD<T,U> zip(JavaRDDLike<U,?> other)` | `JavaPairRDD<T,U>` | `JavaRDDLike<U, ?>` | Stable |
| spark | **JavaRDDLike** | `zipPartitions` | `JavaRDD<V> zipPartitions(JavaRDDLike<U,?>, Function2<Iterato...` | `JavaRDD<V>` | `JavaRDDLike<U, ?>, Function2<Iterator<T>, Iterator<U>, Iterator<V>>` | Stable |
| spark | **JavaRDDLike** | `keyBy` | `JavaPairRDD<U,T> keyBy(Function<T,U> f)` | `JavaPairRDD<U,T>` | `Function<T, U>` | Stable |
| spark | **JavaRDDLike** | `foreach` | `void foreach(VoidFunction<T> f)` | `void` | `VoidFunction<T>` | Stable |
| spark | **JavaRDDLike** | `foreachPartition` | `void foreachPartition(VoidFunction<Iterator<T>> f)` | `void` | `VoidFunction<Iterator<T>>` | Stable |
| spark | **JavaRDDLike** | `collectPartitions` | `List<T>[] collectPartitions(int[] partitionIds)` | `List<T>[]` | `int[]` | Stable |
| spark | **JavaRDDLike** | `reduce` | `T reduce(Function2<T,T,T> f)` | `T` | `Function2<T, T, T>` | Stable |
| spark | **JavaRDDLike** | `treeReduce` | `T treeReduce(Function2<T,T,T> f)` | `T` | `Function2<T, T, T>` | Stable |
| spark | **JavaRDDLike** | `treeReduce` | `T treeReduce(Function2<T,T,T> f, int depth)` | `T` | `Function2<T, T, T>, int` | Stable |
| spark | **JavaRDDLike** | `fold` | `T fold(T zeroValue, Function2<T,T,T> f)` | `T` | `T, Function2<T, T, T>` | Stable |
| spark | **JavaRDDLike** | `aggregate` | `U aggregate(U zeroValue, Function2<U,T,U> seqOp, Function2<U...` | `U` | `U, Function2<U, T, U>, Function2<U, U, U>` | Stable |
| spark | **JavaRDDLike** | `treeAggregate` | `U treeAggregate(U zeroValue, Function2<U,T,U> seqOp, Functio...` | `U` | `U, Function2<U, T, U>, Function2<U, U, U>` | Stable |
| spark | **JavaRDDLike** | `countApprox` | `PartialResult<BoundedDouble> countApprox(long timeout)` | `PartialResult<BoundedDouble>` | `long` | Stable |
| spark | **JavaRDDLike** | `countByValueApprox` | `PartialResult<Map<T,BoundedDouble>> countByValueApprox(long ...` | `PartialResult<Map<T,BoundedDouble>>` | `long` | Stable |
| spark | **JavaRDDLike** | `take` | `List<T> take(int num)` | `List<T>` | `int` | Stable |
| spark | **JavaRDDLike** | `takeSample` | `List<T> takeSample(boolean withReplacement, int num)` | `List<T>` | `boolean, int` | Stable |

> 仅显示前30个，共 116 个P0缺失API

### 5.2 P1优先级缺失API（Evolving）

共 2 个演进API缺失

| 组件 | 类名 | 方法名 | 方法签名 |
|------|------|--------|----------|
| spark | Catalog | `(类级别)` | `Catalog` |
| spark | Write | `(类级别)` | `Write` |

## 六、所有缺失API完整清单

共 226 个API缺失

### kafka组件缺失API (155个)

| 类名 | 方法名 | 方法签名 | 稳定性 | 优先级 |
|------|--------|----------|--------|--------|
| DeleteShareGroupOffsetsResult | `(类级别)` | `DeleteShareGroupOffsetsResult` | Unknown | P2 |
| CreateTopicsOptions | `(类级别)` | `CreateTopicsOptions` | Unknown | P2 |
| Description | `(类级别)` | `Description` | Stable | P0 |
| DescribeClientQuotasResult | `(类级别)` | `DescribeClientQuotasResult` | Unknown | P2 |
| KafkaProducer | `(类级别)` | `KafkaProducer` | Stable | P0 |
| ListTopicsOptions | `(类级别)` | `ListTopicsOptions` | Unknown | P2 |
| WindowStore | `(类级别)` | `WindowStore` | Unknown | P2 |
| OutOfOrderSequenceException | `(类级别)` | `OutOfOrderSequenceException` | Stable | P0 |
| ProducerRecord | `(类级别)` | `ProducerRecord` | Stable | P0 |
| DescribeTopicsOptions | `(类级别)` | `DescribeTopicsOptions` | Unknown | P2 |
| Node | `(类级别)` | `Node` | Stable | P0 |
| RemoveRaftVoterResult | `(类级别)` | `RemoveRaftVoterResult` | Unknown | P2 |
| ExpireDelegationTokenResult | `(类级别)` | `ExpireDelegationTokenResult` | Unknown | P2 |
| DescribeTransactionsResult | `(类级别)` | `DescribeTransactionsResult` | Unknown | P2 |
| DescribeFeaturesResult | `(类级别)` | `DescribeFeaturesResult` | Unknown | P2 |
| KafkaConsumer | `(类级别)` | `KafkaConsumer` | Stable | P0 |
| DescribeConsumerGroupsResult | `(类级别)` | `DescribeConsumerGroupsResult` | Unknown | P2 |
| CommitFailedException | `(类级别)` | `CommitFailedException` | Unknown | P2 |
| ListOffsetsResult | `(类级别)` | `ListOffsetsResult` | Unknown | P2 |
| FenceProducersResult | `(类级别)` | `FenceProducersResult` | Unknown | P2 |
| AuthenticationException | `(类级别)` | `AuthenticationException` | Stable | P0 |
| ElectLeadersResult | `(类级别)` | `ElectLeadersResult` | Unknown | P2 |
| SessionWindows | `(类级别)` | `SessionWindows` | Unknown | P2 |
| AuthorizationException | `(类级别)` | `AuthorizationException` | Stable | P0 |
| BranchedKStream | `(类级别)` | `BranchedKStream` | Unknown | P2 |
| KStream | `(类级别)` | `KStream` | Stable | P0 |
| DescribeClusterResult | `(类级别)` | `DescribeClusterResult` | Unknown | P2 |
| Produced | `(类级别)` | `Produced` | Unknown | P2 |
| KeyQueryMetadata | `(类级别)` | `KeyQueryMetadata` | Stable | P0 |
| DeleteTopicsResult | `(类级别)` | `DeleteTopicsResult` | Unknown | P2 |
| DeleteStreamsGroupOffsetsResult | `(类级别)` | `DeleteStreamsGroupOffsetsResult` | Unknown | P2 |
| DeleteShareGroupsResult | `(类级别)` | `DeleteShareGroupsResult` | Unknown | P2 |
| DescribeProducersResult | `(类级别)` | `DescribeProducersResult` | Unknown | P2 |
| AddRaftVoterResult | `(类级别)` | `AddRaftVoterResult` | Unknown | P2 |
| Task | `(类级别)` | `Task` | Unknown | P2 |
| SinkTask | `(类级别)` | `SinkTask` | Stable | P0 |
| ConsumerGroupMetadata | `(类级别)` | `ConsumerGroupMetadata` | Stable | P0 |
| DescribeClusterOptions | `(类级别)` | `DescribeClusterOptions` | Unknown | P2 |
| Callback | `(类级别)` | `Callback` | Stable | P0 |
| StreamJoined | `(类级别)` | `StreamJoined` | Unknown | P2 |
| Configure | `(类级别)` | `Configure` | Stable | P0 |
| ListClientMetricsResourcesResult | `(类级别)` | `ListClientMetricsResourcesResult` | Stable | P0 |
| Admin | `(类级别)` | `Admin` | Stable | P0 |
| ClusterResource | `(类级别)` | `ClusterResource` | Stable | P0 |
| Connector | `(类级别)` | `Connector` | Stable | P0 |
| SchemaBuilder | `(类级别)` | `SchemaBuilder` | Unknown | P2 |
| StreamsBuilder | `(类级别)` | `StreamsBuilder` | Stable | P0 |
| DeleteRecordsResult | `(类级别)` | `DeleteRecordsResult` | Unknown | P2 |
| RemoveMembersFromConsumerGroupResult | `(类级别)` | `RemoveMembersFromConsumerGroupResult` | Unknown | P2 |
| AlterShareGroupOffsetsResult | `(类级别)` | `AlterShareGroupOffsetsResult` | Unknown | P2 |

> 仅显示前50个，该组件共 155 个缺失API

### spark组件缺失API (71个)

| 类名 | 方法名 | 方法签名 | 稳定性 | 优先级 |
|------|--------|----------|--------|--------|
| JavaRDDLike | `map` | `JavaRDD<R> map(Function<T,R> f)` | Stable | P0 |
| JavaRDDLike | `mapToDouble` | `JavaDoubleRDD mapToDouble(DoubleFunction<T> f)` | Stable | P0 |
| JavaRDDLike | `mapToPair` | `JavaPairRDD<K,V> mapToPair(PairFunction<T,K,V> f)` | Stable | P0 |
| JavaRDDLike | `flatMap` | `JavaRDD<U> flatMap(FlatMapFunction<T,U> f)` | Stable | P0 |
| JavaRDDLike | `flatMapToDouble` | `JavaDoubleRDD flatMapToDouble(DoubleFlatMapFunctio` | Stable | P0 |
| JavaRDDLike | `flatMapToPair` | `JavaPairRDD<K,V> flatMapToPair(PairFlatMapFunction` | Stable | P0 |
| JavaRDDLike | `mapPartitions` | `JavaRDD<U> mapPartitions(FlatMapFunction<Iterator<` | Stable | P0 |
| JavaRDDLike | `mapPartitionsWithIndex` | `JavaRDD<R> mapPartitionsWithIndex(Function2<Intege` | Stable | P0 |
| JavaRDDLike | `mapPartitionsToDouble` | `JavaDoubleRDD mapPartitionsToDouble(DoubleFlatMapF` | Stable | P0 |
| JavaRDDLike | `mapPartitionsToPair` | `JavaPairRDD<K,V> mapPartitionsToPair(PairFlatMapFu` | Stable | P0 |
| JavaRDDLike | `filter` | `JavaRDD<T> filter(Function<T,Boolean> f)` | Stable | P0 |
| JavaRDDLike | `cartesian` | `JavaPairRDD<T,U> cartesian(JavaRDDLike<U,?> other)` | Stable | P0 |
| JavaRDDLike | `groupBy` | `JavaPairRDD<U,Iterable<T>> groupBy(Function<T,U> f` | Stable | P0 |
| JavaRDDLike | `pipe` | `JavaRDD<String> pipe(List<String> command)` | Stable | P0 |
| JavaRDDLike | `zip` | `JavaPairRDD<T,U> zip(JavaRDDLike<U,?> other)` | Stable | P0 |
| JavaRDDLike | `zipPartitions` | `JavaRDD<V> zipPartitions(JavaRDDLike<U,?>, Functio` | Stable | P0 |
| JavaRDDLike | `keyBy` | `JavaPairRDD<U,T> keyBy(Function<T,U> f)` | Stable | P0 |
| JavaRDDLike | `foreach` | `void foreach(VoidFunction<T> f)` | Stable | P0 |
| JavaRDDLike | `foreachPartition` | `void foreachPartition(VoidFunction<Iterator<T>> f)` | Stable | P0 |
| JavaRDDLike | `collectPartitions` | `List<T>[] collectPartitions(int[] partitionIds)` | Stable | P0 |
| JavaRDDLike | `reduce` | `T reduce(Function2<T,T,T> f)` | Stable | P0 |
| JavaRDDLike | `treeReduce` | `T treeReduce(Function2<T,T,T> f)` | Stable | P0 |
| JavaRDDLike | `treeReduce` | `T treeReduce(Function2<T,T,T> f, int depth)` | Stable | P0 |
| JavaRDDLike | `fold` | `T fold(T zeroValue, Function2<T,T,T> f)` | Stable | P0 |
| JavaRDDLike | `aggregate` | `U aggregate(U zeroValue, Function2<U,T,U> seqOp, F` | Stable | P0 |
| JavaRDDLike | `treeAggregate` | `U treeAggregate(U zeroValue, Function2<U,T,U> seqO` | Stable | P0 |
| JavaRDDLike | `countApprox` | `PartialResult<BoundedDouble> countApprox(long time` | Stable | P0 |
| JavaRDDLike | `countByValueApprox` | `PartialResult<Map<T,BoundedDouble>> countByValueAp` | Stable | P0 |
| JavaRDDLike | `take` | `List<T> take(int num)` | Stable | P0 |
| JavaRDDLike | `takeSample` | `List<T> takeSample(boolean withReplacement, int nu` | Stable | P0 |
| JavaRDDLike | `top` | `List<T> top(int num, Comparator<T> comp)` | Stable | P0 |
| JavaRDDLike | `takeOrdered` | `List<T> takeOrdered(int num, Comparator<T> comp)` | Stable | P0 |
| JavaRDDLike | `min` | `T min(Comparator<T> comp)` | Stable | P0 |
| JavaRDDLike | `max` | `T max(Comparator<T> comp)` | Stable | P0 |
| JavaRDDLike | `saveAsTextFile` | `void saveAsTextFile(String path)` | Stable | P0 |
| JavaRDDLike | `saveAsTextFile` | `void saveAsTextFile(String path, Class<? extends C` | Stable | P0 |
| JavaRDDLike | `saveAsObjectFile` | `void saveAsObjectFile(String path)` | Stable | P0 |
| JavaRDDLike | `takeAsync` | `JavaFutureAction<List<T>> takeAsync(int num)` | Stable | P0 |
| JavaRDDLike | `foreachAsync` | `JavaFutureAction<Void> foreachAsync(VoidFunction<T` | Stable | P0 |
| JavaRDDLike | `foreachPartitionAsync` | `JavaFutureAction<Void> foreachPartitionAsync(VoidF` | Stable | P0 |
| JavaRDDLike | `iterator` | `Iterator<T> iterator(Partition, TaskContext)` | Stable | P0 |
| JavaHadoopRDD | `mapPartitionsWithInputSplit` | `JavaRDD<R> mapPartitionsWithInputSplit(Function2<I` | DeveloperApi | P2 |
| JavaDStreamLike | `map` | `JavaDStream<U> map(Function<T,U> f)` | Deprecated | P3 |
| JavaDStreamLike | `mapToPair` | `JavaPairDStream<K,V> mapToPair(PairFunction<T,K,V>` | Deprecated | P3 |
| JavaDStreamLike | `flatMap` | `JavaDStream<U> flatMap(FlatMapFunction<T,U> f)` | Deprecated | P3 |
| JavaDStreamLike | `flatMapToPair` | `JavaPairDStream<K,V> flatMapToPair(PairFlatMapFunc` | Deprecated | P3 |
| JavaDStreamLike | `mapPartitions` | `JavaDStream<U> mapPartitions(FlatMapFunction<Itera` | Deprecated | P3 |
| JavaDStreamLike | `mapPartitionsToPair` | `JavaPairDStream<K,V> mapPartitionsToPair(PairFlatM` | Deprecated | P3 |
| JavaDStreamLike | `reduce` | `JavaDStream<T> reduce(Function2<T,T,T> f)` | Deprecated | P3 |
| JavaDStreamLike | `countByValue` | `JavaPairDStream<T,Long> countByValue(int numPartit` | Deprecated | P3 |

> 仅显示前50个，该组件共 71 个缺失API

