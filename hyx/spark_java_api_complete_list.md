# Spark 暴露给用户的Java API完整清单

> **基于代码仓 + 官方文档综合提取**
> 
> **Spark版本**: 4.2.0-SNAPSHOT (代码仓) / 4.1.1 (官方文档)
> 
> **提取日期**: 2026-04-29

---

## 文档来源

| 来源 | URL |
|------|-----|
| Spark JavaDoc | https://spark.apache.org/docs/latest/api/java/ |
| RDD编程指南 | https://spark.apache.org/docs/latest/rdd-programming-guide.html |
| Streaming编程指南 | https://spark.apache.org/docs/latest/streaming-programming-guide.html |
| MLlib指南 | https://spark.apache.org/docs/latest/ml-guide.html |
| GraphX指南 | https://spark.apache.org/docs/latest/graphx-programming-guide.html |
| 代码仓 | /home/h00517772/spark |

---

## 稳定性标注说明

| 标注 | 含义 | 使用建议 |
|------|------|----------|
| Stable | 稳定API，保证向后兼容 | **推荐使用** |
| Evolving | 演进API，可能变化 | 可用，关注版本迁移 |
| Experimental | 实验性API | 可能被移除或修改 |
| DeveloperApi | 开发者API | 仅供开发者扩展使用 |
| Deprecated | 已废弃 | **应迁移到替代API** |

---

# 一、Core RDD Java API

## 1.1 JavaSparkContext

**包路径**: `org.apache.spark.api.java.JavaSparkContext`

**官方文档描述**: A Java-friendly version of SparkContext that returns JavaRDDs and works with Java collections instead of Scala ones.

**稳定性**: Stable (核心入口类)

**源码路径**: `core/src/main/scala/org/apache/spark/api/java/JavaSparkContext.scala`

### 数据创建算子 (创建RDD)

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `parallelize` | `List<T> list` | `JavaRDD<T>` | 将Java集合并行化为RDD |
| `parallelize` | `List<T> list, int numSlices` | `JavaRDD<T>` | 指定分区数的并行化 |
| `parallelizeDoubles` | `List<Double> list` | `JavaDoubleRDD` | 并行化Double集合 |
| `parallelizePairs` | `List<Tuple2<K,V>> list` | `JavaPairRDD<K,V>` | 并行化键值对集合 |
| `emptyRDD` | - | `JavaRDD<T>` | 创建空RDD |

### 文件读取算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `textFile` | `String path` | `JavaRDD<String>` | 读取文本文件（每行一条记录） |
| `textFile` | `String path, int minPartitions` | `JavaRDD<String>` | 指定最小分区数的文本文件读取 |
| `wholeTextFiles` | `String path` | `JavaPairRDD<String,String>` | 读取目录下所有文本文件，返回(文件名,内容) |
| `wholeTextFiles` | `String path, int minPartitions` | `JavaPairRDD<String,String>` | 指定分区的wholeTextFiles |
| `binaryFiles` | `String path` | `JavaPairRDD<String,PortableDataStream>` | 读取二进制文件目录 |
| `binaryRecords` | `String path, int recordLength` | `JavaRDD<byte[]>` | 读取固定长度二进制记录 |
| `sequenceFile` | `String path, Class<K>, Class<V>` | `JavaPairRDD<K,V>` | 读取Hadoop SequenceFile |
| `objectFile` | `String path` | `JavaRDD<T>` | 读取序列化对象文件 |

### Hadoop输入算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `hadoopRDD` | `JobConf, InputFormatClass, KeyClass, ValueClass` | `JavaPairRDD<K,V>` | 创建Hadoop RDD（旧API） |
| `hadoopFile` | `String path, InputFormatClass, KeyClass, ValueClass` | `JavaPairRDD<K,V>` | 读取Hadoop文件（旧API） |
| `newAPIHadoopRDD` | `Configuration, InputFormatClass, KeyClass, ValueClass` | `JavaPairRDD<K,V>` | 创建Hadoop RDD（新API） |
| `newAPIHadoopFile` | `String path, InputFormatClass, KeyClass, ValueClass` | `JavaPairRDD<K,V>` | 读取Hadoop文件（新API） |

### RDD合并算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `union` | `JavaRDD<T>... rdds` | `JavaRDD<T>` | 合并多个RDD（@varargs） |
| `union` | `JavaPairRDD<K,V>... rdds` | `JavaPairRDD<K,V>` | 合并多个PairRDD |
| `union` | `JavaDoubleRDD... rdds` | `JavaDoubleRDD` | 合并多个DoubleRDD |

### 广播变量与累加器

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `broadcast` | `T value` | `Broadcast<T>` | 创建广播变量 |
| `longAccumulator` | - | `LongAccumulator` | 创建Long累加器 |
| `doubleAccumulator` | - | `DoubleAccumulator` | 创建Double累加器 |
| `collectionAccumulator` | - | `CollectionAccumulator<T>` | 创建集合累加器 |

### 配置与控制

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `stop` | - | `void` | 停止SparkContext |
| `stop` | `int exitCode` | `void` | 带退出码停止 |
| `addFile` | `String path` | `void` | 添加文件到所有节点 |
| `addJar` | `String path` | `void` | 添加JAR到classpath |
| `setCheckpointDir` | `String dir` | `void` | 设置checkpoint目录 |
| `setLogLevel` | `String level` | `void` | 设置日志级别 |
| `setJobGroup` | `String groupId, String description` | `void` | 设置作业组 |
| `cancelJobGroup` | `String groupId` | `void` | 取消作业组 |
| `cancelAllJobs` | - | `void` | 取消所有作业 |
| `hadoopConfiguration` | - | `Configuration` | 获取Hadoop配置 |
| `sparkContext` | - | `SparkContext` | 获取底层SparkContext |
| `getConf` | - | `SparkConf` | 获取SparkConf |
| `getOrCreate` | `SparkConf` | `JavaSparkContext` | 获取或创建SparkContext |

---

## 1.2 JavaRDDLike (接口)

**包路径**: `org.apache.spark.api.java.JavaRDDLike<T,This>`

**官方文档描述**: Defines operations common to several Java RDD implementations.

**稳定性**: Stable

**源码路径**: `core/src/main/scala/org/apache/spark/api/java/JavaRDDLike.scala`

### 转换算子 (Transformations)

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `map` | `Function<T,R> f` | `JavaRDD<R>` | 映射转换 |
| `mapToDouble` | `DoubleFunction<T> f` | `JavaDoubleRDD` | 映射为Double RDD |
| `mapToPair` | `PairFunction<T,K,V> f` | `JavaPairRDD<K,V>` | 映射为Pair RDD |
| `flatMap` | `FlatMapFunction<T,U> f` | `JavaRDD<U>` | 展平映射（一对多） |
| `flatMapToDouble` | `DoubleFlatMapFunction<T> f` | `JavaDoubleRDD` | 展平映射为Double |
| `flatMapToPair` | `PairFlatMapFunction<T,K,V> f` | `JavaPairRDD<K,V>` | 展平映射为Pair |
| `mapPartitions` | `FlatMapFunction<Iterator<T>,U> f` | `JavaRDD<U>` | 分区级映射 |
| `mapPartitionsWithIndex` | `Function2<Integer,Iterator<T>,Iterator<R>> f` | `JavaRDD<R>` | 带分区索引的分区映射 |
| `mapPartitionsToDouble` | `DoubleFlatMapFunction<Iterator<T>> f` | `JavaDoubleRDD` | 分区映射为Double |
| `mapPartitionsToPair` | `PairFlatMapFunction<Iterator<T>,K,V> f` | `JavaPairRDD<K,V>` | 分区映射为Pair |
| `filter` | `Function<T,Boolean> f` | `JavaRDD<T>` | 过滤 |
| `glom` | - | `JavaRDD<List<T>>` | 将每个分区合并为列表 |
| `cartesian` | `JavaRDDLike<U,?> other` | `JavaPairRDD<T,U>` | 笛卡尔积 |
| `groupBy` | `Function<T,U> f` | `JavaPairRDD<U,Iterable<T>>` | 按函数分组 |
| `pipe` | `List<String> command` | `JavaRDD<String>` | 管道到外部命令 |
| `zip` | `JavaRDDLike<U,?> other` | `JavaPairRDD<T,U>` | 与另一RDD压缩 |
| `zipPartitions` | `JavaRDDLike<U,?>, Function2<Iterator<T>,Iterator<U>,Iterator<V>>` | `JavaRDD<V>` | 分区级压缩 |
| `zipWithUniqueId` | - | `JavaPairRDD<T,Long>` | 带唯一ID压缩 |
| `zipWithIndex` | - | `JavaPairRDD<T,Long>` | 带索引压缩 |
| `keyBy` | `Function<T,U> f` | `JavaPairRDD<U,T>` | 创建键值对 |

### 行动算子 (Actions)

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `foreach` | `VoidFunction<T> f` | `void` | 遍历每个元素 |
| `foreachPartition` | `VoidFunction<Iterator<T>> f` | `void` | 分区级遍历 |
| `collect` | - | `List<T>` | 收集所有元素到Driver |
| `toLocalIterator` | - | `Iterator<T>` | 本地迭代器 |
| `collectPartitions` | `int[] partitionIds` | `List<T>[]` | 收集指定分区 |
| `reduce` | `Function2<T,T,T> f` | `T` | reduce聚合 |
| `treeReduce` | `Function2<T,T,T> f` | `T` | 树形reduce |
| `treeReduce` | `Function2<T,T,T> f, int depth` | `T` | 指定深度树形reduce |
| `fold` | `T zeroValue, Function2<T,T,T> f` | `T` | fold聚合 |
| `aggregate` | `U zeroValue, Function2<U,T,U> seqOp, Function2<U,U,U> combOp` | `U` | aggregate聚合 |
| `treeAggregate` | `U zeroValue, Function2<U,T,U> seqOp, Function2<U,U,U> combOp` | `U` | 树形聚合 |
| `count` | - | `long` | 计数 |
| `countApprox` | `long timeout` | `PartialResult<BoundedDouble>` | 近似计数 |
| `countByValue` | - | `Map<T,Long>` | 按值计数 |
| `countByValueApprox` | `long timeout` | `PartialResult<Map<T,BoundedDouble>>` | 近似按值计数 |
| `countApproxDistinct` | - | `long` | 近似去重计数 |
| `take` | `int num` | `List<T>` | 取前N个元素 |
| `takeSample` | `boolean withReplacement, int num` | `List<T>` | 取样本 |
| `first` | - | `T` | 取第一个元素 |
| `isEmpty` | - | `boolean` | 判断是否为空 |
| `top` | `int num, Comparator<T> comp` | `List<T>` | 取最大的N个 |
| `takeOrdered` | `int num, Comparator<T> comp` | `List<T>` | 取最小的N个 |
| `min` | `Comparator<T> comp` | `T` | 最小值 |
| `max` | `Comparator<T> comp` | `T` | 最大值 |
| `saveAsTextFile` | `String path` | `void` | 保存为文本文件 |
| `saveAsTextFile` | `String path, Class<? extends CompressionCodec> codec` | `void` | 带压缩保存 |
| `saveAsObjectFile` | `String path` | `void` | 保存为对象文件 |

### 异步行动算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `countAsync` | - | `JavaFutureAction<Long>` | 异步计数 |
| `collectAsync` | - | `JavaFutureAction<List<T>>` | 异步收集 |
| `takeAsync` | `int num` | `JavaFutureAction<List<T>>` | 异步取前N个 |
| `foreachAsync` | `VoidFunction<T> f` | `JavaFutureAction<Void>` | 异步遍历 |
| `foreachPartitionAsync` | `VoidFunction<Iterator<T>> f` | `JavaFutureAction<Void>` | 异步分区遍历 |

### 属性与方法

| 方法名 | 返回类型 | 描述 |
|--------|----------|------|
| `partitions` | `List<Partition>` | 获取分区列表 |
| `getNumPartitions` | `int` | 获取分区数 |
| `partitioner` | `Optional<Partitioner>` | 获取分区器 |
| `context` | `SparkContext` | 获取SparkContext |
| `id` | `int` | 获取RDD ID |
| `getStorageLevel` | `StorageLevel` | 获取存储级别 |
| `iterator` | `Partition, TaskContext` | `Iterator<T>` | 获取分区迭代器 |
| `rdd` | `RDD<T>` | 获取底层Scala RDD |
| `checkpoint` | `void` | checkpoint |
| `isCheckpointed` | `boolean` | 是否已checkpoint |
| `getCheckpointFile` | `Optional<String>` | checkpoint文件路径 |
| `toDebugString` | `String` | 获取调试字符串 |

---

## 1.3 JavaRDD

**包路径**: `org.apache.spark.api.java.JavaRDD<T>`

**官方文档描述**: A Java-friendly RDD type.

**稳定性**: Stable

**源码路径**: `core/src/main/scala/org/apache/spark/api/java/JavaRDD.scala`

### 持久化算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `cache` | - | `JavaRDD<T>` | 缓存RDD（MEMORY_ONLY） |
| `persist` | `StorageLevel newLevel` | `JavaRDD<T>` | 指定级别持久化 |
| `unpersist` | - | `JavaRDD<T>` | 取消持久化 |
| `unpersist` | `boolean blocking` | `JavaRDD<T>` | 阻塞取消持久化 |

### 分区算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `coalesce` | `int numPartitions` | `JavaRDD<T>` | 合并分区（不shuffle） |
| `coalesce` | `int numPartitions, boolean shuffle` | `JavaRDD<T>` | 可shuffle的合并分区 |
| `repartition` | `int numPartitions` | `JavaRDD<T>` | 重分区（会shuffle） |

### 去重与集合算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `distinct` | - | `JavaRDD<T>` | 去重 |
| `distinct` | `int numPartitions` | `JavaRDD<T>` | 指定分区去重 |
| `union` | `JavaRDD<T> other` | `JavaRDD<T>` | 合并 |
| `intersection` | `JavaRDD<T> other` | `JavaRDD<T>` | 交集 |
| `subtract` | `JavaRDD<T> other` | `JavaRDD<T>` | 差集 |

### 采样与排序算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `sample` | `boolean withReplacement, double fraction` | `JavaRDD<T>` | 采样 |
| `sample` | `boolean withReplacement, double fraction, long seed` | `JavaRDD<T>` | 带种子采样 |
| `randomSplit` | `double[] weights` | `JavaRDD<T>[]` | 随机分割 |
| `randomSplit` | `double[] weights, long seed` | `JavaRDD<T>[]` | 带种子随机分割 |
| `sortBy` | `Function<T,S> f, boolean ascending, int numPartitions` | `JavaRDD<T>` | 排序 |

### 其他

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `setName` | `String name` | `JavaRDD<T>` | 设置RDD名称 |
| `withResources` | `ResourceProfile` | `JavaRDD<T>` | 指定资源配置 |

---

## 1.4 JavaPairRDD

**包路径**: `org.apache.spark.api.java.JavaPairRDD<K,V>`

**官方文档描述**: A Java-friendly RDD of key-value pairs, providing extra methods like reduceByKey and join.

**稳定性**: Stable

**源码路径**: `core/src/main/scala/org/apache/spark/api/java/JavaPairRDD.scala`

### 聚合算子 (ByKey)

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `reduceByKey` | `Function2<V,V,V> func` | `JavaPairRDD<K,V>` | 按键reduce |
| `reduceByKey` | `Function2<V,V,V> func, int numPartitions` | `JavaPairRDD<K,V>` | 指定分区reduceByKey |
| `reduceByKey` | `Function2<V,V,V> func, Partitioner partitioner` | `JavaPairRDD<K,V>` | 指定分区器reduceByKey |
| `reduceByKeyLocally` | `Function2<V,V,V> func` | `Map<K,V>` | 本地reduceByKey |
| `groupByKey` | - | `JavaPairRDD<K,Iterable<V>>` | 按键分组 |
| `groupByKey` | `int numPartitions` | `JavaPairRDD<K,Iterable<V>>` | 指定分区groupByKey |
| `groupByKey` | `Partitioner partitioner` | `JavaPairRDD<K,Iterable<V>>` | 指定分区器groupByKey |
| `combineByKey` | `Function<V,C> createCombiner, Function2<C,V,C> mergeValue, Function2<C,C,C> mergeCombiners` | `JavaPairRDD<K,C>` | 组合按键聚合 |
| `aggregateByKey` | `U zeroValue, Function2<U,V,U> seqFunc, Function2<U,U,U> combFunc` | `JavaPairRDD<K,U>` | 按键聚合 |
| `aggregateByKey` | `U zeroValue, int numPartitions, ...` | `JavaPairRDD<K,U>` | 指定分区aggregateByKey |
| `aggregateByKey` | `U zeroValue, Partitioner partitioner, ...` | `JavaPairRDD<K,U>` | 指定分区器aggregateByKey |
| `foldByKey` | `V zeroValue, Function2<V,V,V> func` | `JavaPairRDD<K,V>` | 按键fold |
| `foldByKey` | `V zeroValue, int numPartitions, Function2<V,V,V> func` | `JavaPairRDD<K,V>` | 指定分区foldByKey |
| `foldByKey` | `V zeroValue, Partitioner partitioner, Function2<V,V,V> func` | `JavaPairRDD<K,V>` | 指定分区器foldByKey |

### 计数算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `countByKey` | - | `Map<K,Long>` | 按键计数 |
| `countByKeyApprox` | `long timeout` | `PartialResult<Map<K,BoundedDouble>>` | 近似按键计数 |
| `countApproxDistinctByKey` | `int precision` | `JavaPairRDD<K,Long>` | 按键近似去重计数 |

### 连接算子 (Join)

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `join` | `JavaPairRDD<K,W> other` | `JavaPairRDD<K,Tuple2<V,W>>` | 内连接 |
| `join` | `JavaPairRDD<K,W> other, int numPartitions` | `JavaPairRDD<K,Tuple2<V,W>>` | 指定分区join |
| `join` | `JavaPairRDD<K,W> other, Partitioner partitioner` | `JavaPairRDD<K,Tuple2<V,W>>` | 指定分区器join |
| `leftOuterJoin` | `JavaPairRDD<K,W> other` | `JavaPairRDD<K,Tuple2<V,Optional<W>>>` | 左外连接 |
| `leftOuterJoin` | `JavaPairRDD<K,W> other, int numPartitions` | `JavaPairRDD<K,Tuple2<V,Optional<W>>>` | 指定分区左外连接 |
| `leftOuterJoin` | `JavaPairRDD<K,W> other, Partitioner partitioner` | `JavaPairRDD<K,Tuple2<V,Optional<W>>>` | 指定分区器左外连接 |
| `rightOuterJoin` | `JavaPairRDD<K,W> other` | `JavaPairRDD<K,Tuple2<Optional<V>,W>>` | 右外连接 |
| `rightOuterJoin` | `JavaPairRDD<K,W> other, int numPartitions` | `JavaPairRDD<K,Tuple2<Optional<V>,W>>` | 指定分区右外连接 |
| `rightOuterJoin` | `JavaPairRDD<K,W> other, Partitioner partitioner` | `JavaPairRDD<K,Tuple2<Optional<V>,W>>` | 指定分区器右外连接 |
| `fullOuterJoin` | `JavaPairRDD<K,W> other` | `JavaPairRDD<K,Tuple2<Optional<V>,Optional<W>>>` | 全外连接 |
| `fullOuterJoin` | `JavaPairRDD<K,W> other, int numPartitions` | `JavaPairRDD<K,Tuple2<Optional<V>,Optional<W>>>` | 指定分区全外连接 |
| `fullOuterJoin` | `JavaPairRDD<K,W> other, Partitioner partitioner` | `JavaPairRDD<K,Tuple2<Optional<V>,Optional<W>>>` | 指定分区器全外连接 |

### 协同分组算子 (Cogroup)

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `cogroup` | `JavaPairRDD<K,W> other` | `JavaPairRDD<K,Tuple2<Iterable<V>,Iterable<W>>>` | 二路协同分组 |
| `cogroup` | `JavaPairRDD<K,W1>, JavaPairRDD<K,W2>` | `JavaPairRDD<K,Tuple3<...>>` | 三路协同分组 |
| `cogroup` | `JavaPairRDD<K,W1>, JavaPairRDD<K,W2>, JavaPairRDD<K,W3>` | `JavaPairRDD<K,Tuple4<...>>` | 四路协同分组 |
| `groupWith` | `JavaPairRDD<K,W> other` | `JavaPairRDD<K,Tuple2<Iterable<V>,Iterable<W>>>` | 分组合并（cogroup别名） |
| `groupWith` | `JavaPairRDD<K,W1>, JavaPairRDD<K,W2>` | `...` | 三路groupWith |
| `groupWith` | `JavaPairRDD<K,W1>, JavaPairRDD<K,W2>, JavaPairRDD<K,W3>` | `...` | 四路groupWith |

### 值映射算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `mapValues` | `Function<V,U> f` | `JavaPairRDD<K,U>` | 映射值（保留键） |
| `flatMapValues` | `FlatMapFunction<V,U> f` | `JavaPairRDD<K,U>` | 展平映射值 |
| `keys` | - | `JavaRDD<K>` | 获取所有键 |
| `values` | - | `JavaRDD<V>` | 获取所有值 |

### 排序算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `sortByKey` | - | `JavaPairRDD<K,V>` | 按键排序（升序） |
| `sortByKey` | `boolean ascending` | `JavaPairRDD<K,V>` | 指定升降序排序 |
| `sortByKey` | `Comparator<K> comp, boolean ascending, int numPartitions` | `JavaPairRDD<K,V>` | 自定义比较器排序 |
| `repartitionAndSortWithinPartitions` | `Partitioner partitioner` | `JavaPairRDD<K,V>` | 重分区并排序 |
| `filterByRange` | `K lower, K upper` | `JavaPairRDD<K,V>` | 按范围过滤（@Since 3.1.0） |

### 分区算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `partitionBy` | `Partitioner partitioner` | `JavaPairRDD<K,V>` | 按分区器分区 |

### 采样算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `sampleByKey` | `boolean withReplacement, Map<K,Double> fractions` | `JavaPairRDD<K,V>` | 按键分层采样 |
| `sampleByKeyExact` | `boolean withReplacement, Map<K,Double> fractions` | `JavaPairRDD<K,V>` | 精确按键分层采样 |

### 差集算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `subtractByKey` | `JavaPairRDD<K,W> other` | `JavaPairRDD<K,V>` | 按键差集 |
| `subtractByKey` | `JavaPairRDD<K,W> other, int numPartitions` | `JavaPairRDD<K,V>` | 指定分区subtractByKey |
| `subtractByKey` | `JavaPairRDD<K,W> other, Partitioner partitioner` | `JavaPairRDD<K,V>` | 指定分区器subtractByKey |
| `subtract` | `JavaPairRDD<K,V> other` | `JavaPairRDD<K,V>` | 差集 |
| `subtract` | `JavaPairRDD<K,V> other, int numPartitions` | `JavaPairRDD<K,V>` | 指定分区差集 |
| `subtract` | `JavaPairRDD<K,V> other, Partitioner partitioner` | `JavaPairRDD<K,V>` | 指定分区器差集 |

### 输出算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `saveAsHadoopFile` | `String path, Class<F> outputFormatClass` | `void` | 保存为Hadoop文件（旧API） |
| `saveAsNewAPIHadoopFile` | `String path, Class<F> outputFormatClass` | `void` | 保存为Hadoop文件（新API） |
| `saveAsHadoopDataset` | `JobConf conf` | `void` | 保存为Hadoop数据集 |
| `saveAsNewAPIHadoopDataset` | `Configuration conf` | `void` | 保存为新API数据集 |
| `collectAsMap` | - | `Map<K,V>` | 收集为Map |
| `lookup` | `K key` | `List<V>` | 查找键对应值 |

---

## 1.5 JavaDoubleRDD

**包路径**: `org.apache.spark.api.java.JavaDoubleRDD`

**官方文档描述**: A Java-friendly RDD of doubles, providing statistical functions.

**稳定性**: Stable

**源码路径**: `core/src/main/scala/org/apache/spark/api/java/JavaDoubleRDD.scala`

### 统计算子

| 方法名 | 返回类型 | 描述 |
|--------|----------|------|
| `sum` | `double` | 求和 |
| `min` | `double` | 最小值 |
| `max` | `double` | 最大值 |
| `stats` | `StatCounter` | 统计摘要（count, mean, variance, max, min） |
| `mean` | `double` | 均值 |
| `variance` | `double` | 方差 |
| `stdev` | `double` | 标准差 |
| `sampleStdev` | `double` | 样本标准差 |
| `sampleVariance` | `double` | 样本方差 |
| `popStdev` | `double` | 总体标准差 (@Since 2.1.0) |
| `popVariance` | `double` | 总体方差 (@Since 2.1.0) |
| `meanApprox` | `long timeout` | `PartialResult<BoundedDouble>` | 近似均值 |
| `sumApprox` | `long timeout` | `PartialResult<BoundedDouble>` | 近似求和 |

### 直方图算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `histogram` | `int bucketCount` | `Tuple2<double[],long[]>` | 直方图（指定桶数） |
| `histogram` | `double[] buckets` | `long[]` | 直方图（自定义桶边界） |

---

## 1.6 JavaHadoopRDD / JavaNewHadoopRDD

**包路径**: `org.apache.spark.api.java`

**稳定性**: DeveloperApi

**源码路径**: `core/src/main/scala/org/apache/spark/api/java/JavaHadoopRDD.scala`

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `mapPartitionsWithInputSplit` | `Function2<InputSplit,Iterator<Tuple2<K,V>>,Iterator<R>> f` | `JavaRDD<R>` | 带InputSplit的分区映射 |

---

## 1.7 其他Core API

### Optional

**包路径**: `org.apache.spark.api.java.Optional<T>`

**官方文档描述**: Like java.util.Optional in Java 8, scala.Option in Scala, represents a value that may or may not exist.

**稳定性**: Stable

| 方法名 | 返回类型 | 描述 |
|--------|----------|------|
| `get` | `T` | 获取值（不存在时抛异常） |
| `isPresent` | `boolean` | 判断是否存在 |
| `orElse` | `T other` | 不存在时返回默认值 |
| `of` | `T value` | `Optional<T>` | 创建Optional（静态方法） |
| `empty` | - | `Optional<T>` | 创建空Optional（静态方法） |

### StorageLevels

**包路径**: `org.apache.spark.api.java.StorageLevels`

**官方文档描述**: Expose some commonly useful storage level constants.

**稳定性**: Stable

| 常量名 | 描述 |
|--------|------|
| `MEMORY_ONLY` | 仅内存 |
| `MEMORY_ONLY_2` | 仅内存（2副本） |
| `MEMORY_ONLY_SER` | 仅内存（序列化） |
| `MEMORY_AND_DISK` | 内存+磁盘 |
| `MEMORY_AND_DISK_2` | 内存+磁盘（2副本） |
| `MEMORY_AND_DISK_SER` | 内存+磁盘（序列化） |
| `DISK_ONLY` | 仅磁盘 |
| `DISK_ONLY_2` | 仅磁盘（2副本） |
| `NONE` | 不存储 |
| `OFF_HEAP` | 堆外内存 |

### JavaFutureAction

**包路径**: `org.apache.spark.api.java.JavaFutureAction<T>`

**稳定性**: Stable

| 方法名 | 返回类型 | 描述 |
|--------|----------|------|
| `cancel` | `boolean` | 取消 |
| `isCancelled` | `boolean` | 是否已取消 |
| `isDone` | `boolean` | 是否已完成 |
| `get` | `T` | 获取结果 |
| `get` | `long timeout, TimeUnit unit` | `T` | 带超时获取结果 |

### JavaSparkStatusTracker

**包路径**: `org.apache.spark.api.java.JavaSparkStatusTracker`

**官方文档描述**: Low-level status reporting APIs for monitoring job and stage progress.

**稳定性**: Stable

| 方法名 | 返回类型 | 描述 |
|--------|----------|------|
| `getActiveJobsIds` | `int[]` | 获取活跃作业ID |
| `getActiveStageIds` | `int[]` | 获取活跃阶段ID |
| `getJobInfo` | `int jobId` | `SparkJobInfo` | 获取作业信息 |
| `getStageInfo` | `int stageId` | `SparkStageInfo` | 获取阶段信息 |

---

# 二、Streaming Java API

> **注意**: Streaming API自Spark 3.4.0起已废弃，建议使用Structured Streaming

## 2.1 JavaStreamingContext

**包路径**: `org.apache.spark.streaming.api.java.JavaStreamingContext`

**官方文档描述**: Java-friendly StreamingContext. Deprecated since Spark 3.4.0.

**稳定性**: Deprecated (Spark 3.4.0)

**源码路径**: `streaming/src/main/scala/org/apache/spark/streaming/api/java/JavaStreamingContext.scala`

### 构造方法

| 构造器 | 描述 |
|--------|------|
| `JavaStreamingContext(SparkConf conf, Duration batchDuration)` | 从SparkConf创建 |
| `JavaStreamingContext(JavaSparkContext jsc, Duration batchDuration)` | 从JavaSparkContext创建 |

### 输入流创建算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `socketTextStream` | `String hostname, int port` | `JavaReceiverInputDStream<String>` | Socket文本流 |
| `socketTextStream` | `String hostname, int port, StorageLevel storageLevel` | `JavaReceiverInputDStream<String>` | 带存储级别Socket流 |
| `socketStream` | `String hostname, int port, Function<byte[],T> converter, StorageLevel` | `JavaReceiverInputDStream<T>` | 自定义Socket流 |
| `rawSocketStream` | `String hostname, int port, StorageLevel` | `JavaReceiverInputDStream<T>` | 原始Socket流 |
| `textFileStream` | `String directory` | `JavaDStream<String>` | 文件文本流 |
| `binaryRecordsStream` | `String directory, int recordLength` | `JavaDStream<byte[]>` | 二进制记录流 |
| `fileStream` | `String directory, Class<K>, Class<V>, Class<F>` | `JavaPairInputDStream<K,V>` | Hadoop文件流 |
| `queueStream` | `Queue<JavaRDD<T>> queue` | `JavaInputDStream<T>` | 队列流（测试用） |
| `receiverStream` | `Receiver<T> receiver` | `JavaReceiverInputDStream<T>` | 自定义接收器流 |

### 流合并算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `union` | `JavaDStream<T>... streams` | `JavaDStream<T>` | 合并多个DStream |
| `union` | `JavaPairDStream<K,V>... streams` | `JavaPairDStream<K,V>` | 合并多个Pair DStream |

### 控制算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `start` | - | `void` | 启动流处理 |
| `stop` | - | `void` | 停止流处理 |
| `stop` | `boolean stopSparkContext` | `void` | 带参数停止 |
| `stop` | `boolean stopSparkContext, boolean stopGracefully` | `void` | 优雅停止 |
| `awaitTermination` | - | `void` | 等待终止 |
| `awaitTerminationOrTimeout` | `long timeout` | `boolean` | 带超时等待 |
| `checkpoint` | `String directory` | `void` | 设置checkpoint目录 |
| `remember` | `Duration duration` | `void` | 设置记忆时长 |
| `addStreamingListener` | `JavaStreamingListener listener` | `void` | 添加监听器 |
| `getState` | - | `StreamingContextState` | 获取状态 |
| `sparkContext` | - | `JavaSparkContext` | 获取JavaSparkContext |
| `getConf` | - | `SparkConf` | 获取配置 |

---

## 2.2 JavaDStreamLike

**包路径**: `org.apache.spark.streaming.api.java.JavaDStreamLike<T,This,R>`

**稳定性**: Deprecated (Spark 3.4.0)

**源码路径**: `streaming/src/main/scala/org/apache/spark/streaming/api/java/JavaDStreamLike.scala`

### 转换算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `map` | `Function<T,U> f` | `JavaDStream<U>` | 映射 |
| `mapToPair` | `PairFunction<T,K,V> f` | `JavaPairDStream<K,V>` | 映射为Pair |
| `flatMap` | `FlatMapFunction<T,U> f` | `JavaDStream<U>` | 展平映射 |
| `flatMapToPair` | `PairFlatMapFunction<T,K,V> f` | `JavaPairDStream<K,V>` | 展平映射为Pair |
| `mapPartitions` | `FlatMapFunction<Iterator<T>,U> f` | `JavaDStream<U>` | 分区映射 |
| `mapPartitionsToPair` | `PairFlatMapFunction<Iterator<T>,K,V> f` | `JavaPairDStream<K,V>` | 分区映射为Pair |
| `reduce` | `Function2<T,T,T> f` | `JavaDStream<T>` | 每批次reduce |
| `glom` | - | `JavaDStream<List<T>>` | 分区合并为列表 |
| `count` | - | `JavaDStream<Long>` | 每批次计数 |
| `countByValue` | - | `JavaPairDStream<T,Long>` | 每批次按值计数 |
| `countByValue` | `int numPartitions` | `JavaPairDStream<T,Long>` | 指定分区countByValue |

### 窗口算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `countByWindow` | `Duration windowDuration, Duration slideDuration` | `JavaDStream<Long>` | 窗口计数 |
| `countByValueAndWindow` | `Duration windowDuration, Duration slideDuration` | `JavaPairDStream<T,Long>` | 窗口按值计数 |
| `reduceByWindow` | `Function2<T,T,T> reduceFunc, Duration windowDuration, Duration slideDuration` | `JavaDStream<T>` | 窗口reduce |
| `reduceByWindow` | `Function2<T,T,T> reduceFunc, Function2<T,T,T> invReduceFunc, Duration windowDuration, Duration slideDuration` | `JavaDStream<T>` | 增量窗口reduce |

### RDD转换算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `transform` | `Function<R,JavaRDD<U>> transformFunc` | `JavaDStream<U>` | RDD级转换 |
| `transform` | `Function2<R,Time,JavaRDD<U>> transformFunc` | `JavaDStream<U>` | 带时间RDD转换 |
| `transformToPair` | `Function<R,JavaPairRDD<K,V>> transformFunc` | `JavaPairDStream<K,V>` | RDD转换为Pair |
| `transformToPair` | `Function2<R,Time,JavaPairRDD<K,V>> transformFunc` | `JavaPairDStream<K,V>` | 带时间RDD转换为Pair |
| `transformWith` | `JavaDStream<U> other, Function3<R,JavaRDD<U>,Time,JavaRDD<W>> transformFunc` | `JavaDStream<W>` | 与另一DStream转换 |
| `transformWithToPair` | `JavaDStream<U> other, Function3<R,JavaRDD<U>,Time,JavaPairRDD<K,V>> transformFunc` | `JavaPairDStream<K,V>` | 与另一DStream转换为Pair |

### 输出算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `print` | - | `void` | 打印前10个元素 |
| `print` | `int num` | `void` | 打印指定数量 |
| `foreachRDD` | `VoidFunction<R> foreachFunc` | `void` | 对每个RDD执行 |
| `foreachRDD` | `VoidFunction2<R,Time> foreachFunc` | `void` | 带时间foreachRDD |
| `slice` | `Time fromTime, Time toTime` | `List<R>` | 时间切片 |

### checkpoint

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `checkpoint` | `Duration interval` | `void` | checkpoint |

---

## 2.3 JavaDStream

**包路径**: `org.apache.spark.streaming.api.java.JavaDStream<T>`

**官方文档描述**: A Java-friendly interface to DStream, the basic abstraction in Spark Streaming.

**稳定性**: Deprecated (Spark 3.4.0)

**源码路径**: `streaming/src/main/scala/org/apache/spark/streaming/api/java/JavaDStream.scala`

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `filter` | `Function<T,Boolean> f` | `JavaDStream<T>` | 过滤 |
| `cache` | - | `JavaDStream<T>` | 缓存 |
| `persist` | - | `JavaDStream<T>` | 持久化 |
| `persist` | `StorageLevel level` | `JavaDStream<T>` | 指定级别持久化 |
| `window` | `Duration windowDuration` | `JavaDStream<T>` | 窗口 |
| `window` | `Duration windowDuration, Duration slideDuration` | `JavaDStream<T>` | 指定滑动窗口 |
| `union` | `JavaDStream<T> that` | `JavaDStream<T>` | 合并 |
| `repartition` | `int numPartitions` | `JavaDStream<T>` | 重分区 |
| `compute` | `Time validTime` | `JavaRDD<T>` | 计算指定时间RDD |

---

## 2.4 JavaPairDStream

**包路径**: `org.apache.spark.streaming.api.java.JavaPairDStream<K,V>`

**官方文档描述**: A Java-friendly interface to a DStream of key-value pairs, providing extra methods like reduceByKey and join.

**稳定性**: Deprecated (Spark 3.4.0)

**源码路径**: `streaming/src/main/scala/org/apache/spark/streaming/api/java/JavaPairDStream.scala`

### 聚合算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `reduceByKey` | `Function2<V,V,V> func` | `JavaPairDStream<K,V>` | 按键reduce |
| `reduceByKey` | `Function2<V,V,V> func, int numPartitions` | `JavaPairDStream<K,V>` | 指定分区reduceByKey |
| `reduceByKey` | `Function2<V,V,V> func, Partitioner partitioner` | `JavaPairDStream<K,V>` | 指定分区器reduceByKey |
| `groupByKey` | - | `JavaPairDStream<K,Iterable<V>>` | 按键分组 |
| `groupByKey` | `int numPartitions` | `JavaPairDStream<K,Iterable<V>>` | 指定分区groupByKey |
| `groupByKey` | `Partitioner partitioner` | `JavaPairDStream<K,Iterable<V>>` | 指定分区器groupByKey |
| `combineByKey` | `Function<V,C> createCombiner, Function2<C,V,C> mergeValue, Function2<C,C,C> mergeCombiners` | `JavaPairDStream<K,C>` | 组合按键聚合 |

### 窗口聚合算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `groupByKeyAndWindow` | `Duration windowDuration` | `JavaPairDStream<K,Iterable<V>>` | 窗口groupByKey |
| `groupByKeyAndWindow` | `Duration windowDuration, Duration slideDuration` | `JavaPairDStream<K,Iterable<V>>` | 指定滑动窗口groupByKey |
| `reduceByKeyAndWindow` | `Function2<V,V,V> func, Duration windowDuration` | `JavaPairDStream<K,V>` | 窗口reduceByKey |
| `reduceByKeyAndWindow` | `Function2<V,V,V> func, Duration windowDuration, Duration slideDuration` | `JavaPairDStream<K,V>` | 指定滑动窗口reduceByKey |
| `reduceByKeyAndWindow` | `Function2<V,V,V> reduceFunc, Function2<V,V,V> invReduceFunc, Duration windowDuration, Duration slideDuration` | `JavaPairDStream<K,V>` | 增量窗口reduceByKey |

### 状态算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `updateStateByKey` | `Function2<List<V>,Optional<S>,Optional<S>> updateFunc` | `JavaPairDStream<K,S>` | 状态更新 |
| `mapWithState` | `MapWithStateFunction<K,V,S,U> func, StateSpec<S> spec` | `JavaMapWithStateDStream<K,V,S,U>` | 带状态映射 |

### 值映射算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `mapValues` | `Function<V,U> f` | `JavaPairDStream<K,U>` | 映射值 |
| `flatMapValues` | `FlatMapFunction<V,U> f` | `JavaPairDStream<K,U>` | 展平映射值 |

### 连接算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `join` | `JavaPairDStream<K,W> other` | `JavaPairDStream<K,Tuple2<V,W>>` | 内连接 |
| `leftOuterJoin` | `JavaPairDStream<K,W> other` | `JavaPairDStream<K,Tuple2<V,Optional<W>>>` | 左外连接 |
| `rightOuterJoin` | `JavaPairDStream<K,W> other` | `JavaPairDStream<K,Tuple2<Optional<V>,W>>` | 右外连接 |
| `fullOuterJoin` | `JavaPairDStream<K,W> other` | `JavaPairDStream<K,Tuple2<Optional<V>,Optional<W>>>` | 全外连接 |
| `cogroup` | `JavaPairDStream<K,W> other` | `JavaPairDStream<K,Tuple2<Iterable<V>,Iterable<W>>>` | 协同分组 |

### 其他算子

| 方法名 | 参数 | 返回类型 | 描述 |
|--------|------|----------|------|
| `filter` | `Function<Tuple2<K,V>,Boolean> f` | `JavaPairDStream<K,V>` | 过滤 |
| `window` | `Duration windowDuration` | `JavaPairDStream<K,V>` | 窗口 |
| `window` | `Duration windowDuration, Duration slideDuration` | `JavaPairDStream<K,V>` | 指定滑动窗口 |
| `union` | `JavaPairDStream<K,V> that` | `JavaPairDStream<K,V>` | 合并 |
| `repartition` | `int numPartitions` | `JavaPairDStream<K,V>` | 重分区 |
| `saveAsHadoopFiles` | `String prefix, String suffix` | `void` | 保存为Hadoop文件 |
| `saveAsNewAPIHadoopFiles` | `String prefix, String suffix` | `void` | 保存为新API Hadoop文件 |

---

## 2.5 JavaInputDStream / JavaReceiverInputDStream

**包路径**: `org.apache.spark.streaming.api.java`

**官方文档描述**: Java-friendly interface to InputDStream/ReceiverInputDStream.

**稳定性**: Deprecated (Spark 3.4.0)

- **JavaInputDStream**: 基础输入流
- **JavaReceiverInputDStream**: 接收器输入流（需要Receiver）
- **JavaPairInputDStream**: 键值对输入流
- **JavaPairReceiverInputDStream**: 键值对接收器输入流

---

## 2.6 JavaMapWithStateDStream

**包路径**: `org.apache.spark.streaming.api.java.JavaMapWithStateDStream<K,V,S,U>`

**官方文档描述**: DStream representing the stream of data generated by mapWithState operation.

**稳定性**: Deprecated (Spark 3.4.0)

| 方法名 | 返回类型 | 描述 |
|--------|----------|------|
| `stateSnapshots` | `JavaPairDStream<K,S>` | 状态快照 |

---

## 2.7 JavaStreamingListener

**包路径**: `org.apache.spark.streaming.api.java.JavaStreamingListener`

**官方文档描述**: Base trait for events related to JavaStreamingListener.

**稳定性**: Deprecated (Spark 3.4.0)

| 回调方法 | 描述 |
|----------|------|
| `onStreamingStarted` | 流启动 |
| `onReceiverStarted` | 接收器启动 |
| `onReceiverError` | 接收器错误 |
| `onReceiverStopped` | 接收器停止 |
| `onBatchSubmitted` | 批次提交 |
| `onBatchStarted` | 批次启动 |
| `onBatchCompleted` | 批次完成 |
| `onOutputOperationStarted` | 输出操作启动 |
| `onOutputOperationCompleted` | 输出操作完成 |

---

# 三、Java函数接口

**包路径**: `org.apache.spark.api.java.function`

**官方文档描述**: Set of interfaces to represent functions in Spark's Java API.

**稳定性**: Stable (全部稳定)

**源码路径**: `common/utils-java/src/main/java/org/apache/spark/api/java/function/`

### 基础函数接口

| 接口名 | 签名 | 用于方法 | 描述 |
|--------|------|----------|------|
| `Function<T,R>` | `R call(T t)` | `map()` | 单参数转换 |
| `Function0<R>` | `R call()` | - | 无参数函数 |
| `Function2<T1,T2,R>` | `R call(T1 t1, T2 t2)` | `reduce()`, `aggregate()` | 双参数函数 |
| `Function3<T1,T2,T3,R>` | `R call(T1, T2, T3)` | - | 三参数函数 |
| `Function4<T1,T2,T3,T4,R>` | `R call(T1, T2, T3, T4)` | - | 四参数函数 |

### Dataset/RDD专用接口

| 接口名 | 签名 | 用于方法 | 描述 |
|--------|------|----------|------|
| `MapFunction<T,U>` | `U call(T t)` | `Dataset.map()` | 类型化映射 |
| `FilterFunction<T>` | `boolean call(T t)` | `Dataset.filter()` | 过滤 |
| `FlatMapFunction<T,U>` | `Iterator<U> call(T t)` | `Dataset.flatMap()` | 展平映射 |
| `MapPartitionsFunction<T,U>` | `Iterator<U> call(Iterator<T> it)` | `Dataset.mapPartitions()` | 分区级映射 |
| `ReduceFunction<T>` | `T call(T v1, T v2)` | `Dataset.reduce()` | 归约 |
| `ForeachFunction<T>` | `void call(T t)` | `Dataset.foreach()` | 遍历 |
| `ForeachPartitionFunction<T>` | `void call(Iterator<T> it)` | `Dataset.foreachPartition()` | 分区级遍历 |

### Pair RDD专用接口

| 接口名 | 签名 | 用于方法 | 描述 |
|--------|------|----------|------|
| `PairFunction<T,K,V>` | `Tuple2<K,V> call(T t)` | `mapToPair()` | 转为Pair RDD |
| `PairFlatMapFunction<T,K,V>` | `Iterator<Tuple2<K,V>> call(T t)` | `flatMapToPair()` | 展平转Pair |
| `FlatMapFunction2<K,V,R>` | `Iterator<R> call(K key, V value)` | `flatMapValues()` | 值展平 |

### 分组专用接口

| 接口名 | 签名 | 用于方法 | 描述 |
|--------|------|----------|------|
| `MapGroupsFunction<K,V,U>` | `U call(K key, Iterator<V> values)` | `mapGroups()` | 分组映射 |
| `FlatMapGroupsFunction<K,V,U>` | `Iterator<U> call(K key, Iterator<V> values)` | `flatMapGroups()` | 分组展平映射 |
| `CoGroupFunction<K,V1,V2,R>` | `Iterator<R> call(K, Iterator<V1>, Iterator<V2>)` | `cogroup()` | 协分组 |

### 有状态接口 (Streaming)

| 接口名 | 签名 | 用于方法 | 描述 |
|--------|------|----------|------|
| `MapGroupsWithStateFunction<K,V,S,U>` | `U call(K key, Iterator<V> values, GroupState<S> state)` | `mapGroupsWithState()` | 带状态分组映射 |
| `FlatMapGroupsWithStateFunction<K,V,S,U>` | `Iterator<U> call(K key, Iterator<V> values, GroupState<S> state)` | `flatMapGroupsWithState()` | 带状态分组展平映射 |

### Double专用接口

| 接口名 | 签名 | 用于方法 | 描述 |
|--------|------|----------|------|
| `DoubleFunction<T>` | `double call(T t)` | `mapToDouble()` | Double返回 |
| `DoubleFlatMapFunction<T>` | `Iterator<Double> call(T t)` | `flatMapToDouble()` | Double展平映射 |

### Void函数接口

| 接口名 | 签名 | 用于方法 | 描述 |
|--------|------|----------|------|
| `VoidFunction<T>` | `void call(T t)` | `foreach()` | 无返回值 |
| `VoidFunction2<T1,T2>` | `void call(T1 t1, T2 t2)` | - | 双参数无返回值 |

---

# 四、UDF接口

**包路径**: `org.apache.spark.sql.api.java`

**官方文档描述**: Allows the execution of relational queries, including those expressed in SQL using Spark.

**稳定性**: Stable (全部稳定)

**源码路径**: `sql/api/src/main/java/org/apache/spark/sql/api/java/`

| 接口名 | 参数数量 | 签名 | 描述 |
|--------|----------|------|------|
| `UDF0<R>` | 0 | `R call()` | 无参数UDF |
| `UDF1<T1,R>` | 1 | `R call(T1 t1)` | 1参数UDF |
| `UDF2<T1,T2,R>` | 2 | `R call(T1 t1, T2 t2)` | 2参数UDF |
| `UDF3<T1,T2,T3,R>` | 3 | `R call(T1, T2, T3)` | 3参数UDF |
| `UDF4<T1,T2,T3,T4,R>` | 4 | `R call(T1, T2, T3, T4)` | 4参数UDF |
| `UDF5` - `UDF10` | 5-10 | `R call(T1...Tn)` | 5-10参数UDF |
| `UDF11` - `UDF15` | 11-15 | `R call(T1...Tn)` | 11-15参数UDF |
| `UDF16` - `UDF20` | 16-20 | `R call(T1...Tn)` | 16-20参数UDF |
| `UDF21<T1,...,T21,R>` | 21 | `R call(T1...T21)` | 21参数UDF |
| `UDF22<T1,...,T22,R>` | 22 | `R call(T1...T22)` | 22参数UDF（最大） |

---

# 五、MLlib Java API

> **注意**: RDD-based MLlib处于维护模式，建议使用DataFrame-based ML (spark.ml)

## 5.1 分类算法

**包路径**: `org.apache.spark.mllib.classification`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `LogisticRegressionModel` | 逻辑回归模型 | @Since 0.8.0 |
| `LogisticRegressionWithSGD` | SGD逻辑回归训练 | @Since 0.8.0 |
| `LogisticRegressionWithLBFGS` | LBFGS逻辑回归训练 | @Since 1.1.0 |
| `SVMModel` | SVM模型 | @Since 0.8.0 |
| `SVMWithSGD` | SVM训练 | @Since 0.8.0 |
| `NaiveBayesModel` | 朴素贝叶斯模型 | @Since 0.9.0 |
| `NaiveBayes` | 朴素贝叶斯训练 | @Since 0.9.0 |

**公共方法**:
- `predict(Vector)` - 预测单个样本
- `predict(RDD<Vector>)` - 批量预测
- `save(SparkContext, path)` - 保存模型
- `load(SparkContext, path)` - 加载模型
- `clearThreshold()` - 清除阈值
- `setThreshold(double)` - 设置阈值

## 5.2 聚类算法

**包路径**: `org.apache.spark.mllib.clustering`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `KMeansModel` | K-means模型 | @Since 0.8.0 |
| `KMeans` | K-means训练 | @Since 0.8.0 |
| `BisectingKMeansModel` | 二分K-means模型 | @Since 1.6.0 |
| `BisectingKMeans` | 二分K-means训练 | @Since 1.6.0 |
| `GaussianMixtureModel` | 高斯混合模型 | @Since 1.3.0 |
| `GaussianMixture` | 高斯混合训练 | @Since 1.3.0 |
| `LDAModel` | LDA模型 | @Since 1.3.0 |
| `LDA` | LDA训练 | @Since 1.3.0 |
| `PowerIterationClustering` | 幂迭代聚类 | @Since 1.3.0 |
| `StreamingKMeans` | 流式K-means | @Since 1.2.0 |

**公共方法**:
- `run(RDD<Vector>)` - 训练
- `predict(Vector)` - 预测聚类
- `predict(RDD<Vector>)` - 批量预测
- `clusterCenters` - 获取聚类中心
- `k` - 获取聚类数
- `setK(int)` - 设置聚类数
- `setMaxIterations(int)` - 设置最大迭代次数
- `setInitializationMode(String)` - 设置初始化模式
- `setSeed(long)` - 设置随机种子

## 5.3 回归算法

**包路径**: `org.apache.spark.mllib.regression`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `LinearRegressionModel` | 线性回归模型 | @Since 0.8.0 |
| `LinearRegressionWithSGD` | 纯性回归训练(SGD) | @Since 0.8.0 |
| `LassoModel` | Lasso模型 | @Since 0.8.0 |
| `LassoWithSGD` | Lasso训练 | @Since 0.8.0 |
| `RidgeRegressionModel` | Ridge模型 | @Since 0.8.0 |
| `RidgeRegressionWithSGD` | Ridge训练 | @Since 0.8.0 |
| `IsotonicRegressionModel` | 保序回归模型 | @Since 1.3.0 |
| `IsotonicRegression` | 保序回归训练 | @Since 1.3.0 |

## 5.4 推荐算法

**包路径**: `org.apache.spark.mllib.recommendation`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `MatrixFactorizationModel` | 矩阵分解模型(ALS) | @Since 0.8.0 |
| `ALS` | ALS推荐算法 | @Since 0.8.0 |
| `Rating` | 评分数据结构 | @Since 0.8.0 |

**公共方法**:
- `run(RDD<Rating>)` - 训练
- `predict(int user, int product)` - 预测评分
- `recommendProducts(int user, int num)` - 为用户推荐产品
- `recommendUsers(int product, int num)` - 为产品推荐用户
- `recommendProductsForUsers(int num)` - 为所有用户推荐
- `recommendUsersForProducts(int num)` - 为所有产品推荐

## 5.5 特征提取/转换

**包路径**: `org.apache.spark.mllib.feature`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `StandardScalerModel` | 标准缩放器模型 | @Since 1.1.0 |
| `StandardScaler` | 标准缩放器 | @Since 1.1.0 |
| `Normalizer` | 正则化器 | @Since 1.1.0 |
| `IDFModel` | IDF模型 | @Since 1.1.0 |
| `IDF` | IDF计算器 | @Since 1.1.0 |
| `Word2VecModel` | Word2Vec模型 | @Since 1.1.0 |
| `Word2Vec` | Word2Vec训练 | @Since 1.1.0 |
| `PCA` | PCA降维 | @Since 1.4.0 |
| `ChiSqSelectorModel` | 卡方选择器模型 | @Since 1.3.0 |
| `ChiSqSelector` | 卡方选择器 | @Since 1.3.0 |
| `HashingTF` | Hash TF | @Since 1.1.0 |

## 5.6 频繁模式挖掘

**包路径**: `org.apache.spark.mllib.fpm`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `FPGrowthModel` | FP-Growth模型 | @Since 1.3.0 |
| `FPGrowth` | FP-Growth训练 | @Since 1.3.0 |
| `PrefixSpanModel` | PrefixSpan模型 | @Since 1.5.0 |
| `PrefixSpan` | PrefixSpan训练 | @Since 1.5.0 |
| `AssociationRules` | 关联规则 | @Since 1.5.0 |

## 5.7 评估指标

**包路径**: `org.apache.spark.mllib.evaluation`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `BinaryClassificationMetrics` | 二分类指标 | @Since 1.0.0 |
| `MulticlassMetrics` | 多分类指标 | @Since 1.1.0 |
| `RegressionMetrics` | 回归指标 | @Since 1.2.0 |
| `RankingMetrics` | 排序指标 | @Since 1.2.0 |

## 5.8 统计

**包路径**: `org.apache.spark.mllib.stat`

| 类名 | 描述 | 稳定性 |
|------|------|--------|
| `Statistics` | 统计工具类 | @Since 1.1.0 |
| `MultivariateStatisticalSummary` | 多变量统计摘要 | @Since 1.0.0 |
| `KernelDensity` | 核密度估计 | @Since 1.4.0 |
| `Correlation` | 相关性计算 | @Since 1.1.0 |

---

# 六、GraphX API

> **注意**: GraphX主要为Scala API，Java用户需通过Scala API间接使用

**包路径**: `org.apache.spark.graphx`

**官方文档描述**: ALPHA COMPONENT - GraphX is a graph processing framework built on top of Spark.

**稳定性**: Alpha

## 6.1 核心类

| 类名 | 描述 |
|------|------|
| `Graph<VD,ED>` | 图抽象类 |
| `GraphOps<VD,ED>` | 图操作类 |
| `VertexRDD<VD>` | 顶点RDD |
| `EdgeRDD<ED>` | 边RDD |
| `Edge<ED>` | 边类 |
| `EdgeTriplet<VD,ED>` | 边三元组 |
| `EdgeContext<VD,ED,A>` | 边上下文 |
| `EdgeDirection` | 边方向枚举 |
| `PartitionStrategy` | 分区策略 |
| `GraphLoader` | 图加载器 |
| `Pregel` | Pregel抽象 |

## 6.2 Java可用类

**包路径**: `org.apache.spark.graphx`

| 类名 | 描述 |
|------|------|
| `TripletFields` | 三元组字段枚举 |
| `EdgeActiveness` | 边活跃性枚举 |

---

# 七、SQL Connector API (DataSource V2)

> **用于实现自定义数据源**

## 7.1 Catalog接口

**包路径**: `org.apache.spark.sql.connector.catalog`

| 接口名 | 稳定性 | 描述 |
|--------|--------|------|
| `CatalogPlugin` | @Evolving | Catalog标记接口 |
| `TableCatalog` | @Evolving | 表Catalog |
| `Table` | @Evolving | 表接口 |
| `TableProvider` | @Evolving | 表提供者 |
| `TableChange` | @Evolving | 表变更操作 |
| `SupportsNamespaces` | @Evolving | 支持命名空间 |
| `SupportsRead` | @Evolving | 支持读取 |
| `SupportsWrite` | @Evolving | 支持写入 |
| `SupportsDelete` | @Evolving | 支持删除 |
| `StagingTableCatalog` | @Evolving | 支持暂存表 |
| `FunctionCatalog` | @Evolving | 函数Catalog |
| `ProcedureCatalog` | @Evolving | 存储过程Catalog |
| `Identifier` | @Evolving | 对象标识 |

## 7.2 Read接口

**包路径**: `org.apache.spark.sql.connector.read`

| 接口名 | 稳定性 | 描述 |
|--------|--------|------|
| `Scan` | @Evolving | 扫描接口 |
| `ScanBuilder` | @Evolving | Scan构建器 |
| `Batch` | @Evolving | 批处理扫描 |
| `InputPartition` | @Evolving | 输入分区 |
| `PartitionReader` | @Evolving | 分区读取器 |
| `PartitionReaderFactory` | @Evolving | 分区读取器工厂 |
| `Statistics` | @Evolving | 统计信息 |
| `LocalScan` | @Stable | 本地扫描 |
| `SupportsPushDownFilters` | @Evolving | 支持下推过滤 |
| `SupportsPushDownAggregates` | @Evolving | 支持下推聚合 |

## 7.3 Write接口

**包路径**: `org.apache.spark.sql.connector.write`

| 接口名 | 稳定性 | 描述 |
|--------|--------|------|
| `Write` | @Evolving | 写入接口 |
| `WriteBuilder` | @Evolving | 写入构建器 |
| `BatchWrite` | @Evolving | 批处理写入 |
| `DataWriter` | @Evolving | 数据写入器 |
| `DataWriterFactory` | @Evolving | 数据写入器工厂 |
| `WriterCommitMessage` | @Evolving | 写入提交消息 |
| `SupportsOverwrite` | @Evolving | 支持覆写 |
| `SupportsTruncate` | @Evolving | 支持截断 |

---

# 八、方法数量统计

| 分类 | 类/接口数 | 方法数 |
|------|----------|--------|
| **Core RDD** | 10 | ~300 |
| JavaSparkContext | 1 | ~50 |
| JavaRDDLike | 1 | ~60 |
| JavaRDD | 1 | ~25 |
| JavaPairRDD | 1 | ~80 |
| JavaDoubleRDD | 1 | ~30 |
| **Streaming** | 10 | ~170 |
| JavaStreamingContext | 1 | ~30 |
| JavaDStreamLike | 1 | ~35 |
| JavaDStream | 1 | ~12 |
| JavaPairDStream | 1 | ~55 |
| **函数接口** | 21 | 21 |
| **UDF接口** | 23 | 23 |
| **MLlib** | ~50 | ~400 |
| **GraphX** | ~15 | ~70 |
| **Connector V2** | ~50 | ~100 |
| **总计** | ~180 | ~1100 |

---

# 九、使用建议

## 1. 优先使用Stable API

- Core RDD API (JavaSparkContext, JavaRDD, JavaPairRDD)
- Java函数接口 (全部Stable)
- UDF接口 (UDF0-22)
- SQL核心类 (RowFactory, SaveMode)

## 2. 避免使用Deprecated API

- Streaming API → 使用Structured Streaming替代
- RDD-based MLlib → 使用DataFrame-based ML替代

## 3. 关注Evolving API变化

- Connector V2 API
- Streaming状态接口

## 4. 算子能力重点

### RDD算子优先级
1. **高频算子**: map, filter, reduce, groupByKey, reduceByKey, join
2. **聚合算子**: aggregate, aggregateByKey, combineByKey
3. **文件算子**: textFile, saveAsTextFile
4. **统计算子**: count, countByKey, sum, mean

### Streaming算子优先级（如需使用）
1. **基础算子**: map, filter, reduce, count
2. **窗口算子**: reduceByWindow, reduceByKeyAndWindow
3. **状态算子**: updateStateByKey, mapWithState

---

# 十、官方文档参考

| 文档 | URL |
|------|-----|
| Spark JavaDoc | https://spark.apache.org/docs/latest/api/java/ |
| RDD编程指南 | https://spark.apache.org/docs/latest/rdd-programming-guide.html |
| Streaming编程指南 | https://spark.apache.org/docs/latest/streaming-programming-guide.html |
| MLlib指南 | https://spark.apache.org/docs/latest/ml-guide.html |
| GraphX指南 | https://spark.apache.org/docs/latest/graphx-programming-guide.html |
| Structured Streaming | https://spark.apache.org/docs/latest/streaming/ |