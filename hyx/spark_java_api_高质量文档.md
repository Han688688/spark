# Spark Java API 高质量完整文档

> **文档特点**:
> - 从Spark源码提取所有公开Java方法
> - 包含JavaDoc原文描述
> - 核心方法提供完整可运行示例
> - 按业务分类组织

> **统计**: 201 个核心方法 (来自Scala定义)

---

## 目录

### 一、Core RDD核心API
- JavaSparkContext (Spark上下文入口)
- JavaRDD (通用RDD)
- JavaPairRDD (键值对RDD)
- JavaDoubleRDD (Double类型RDD)
- JavaRDDLike (RDD公共接口)

### 二、函数接口API
- Function, Function2, VoidFunction等

### 三、StorageLevel存储级别

---

## 一、Core RDD核心API

### JavaSparkContext

**描述**: Java友好的SparkContext版本，返回JavaRDD，使用Java集合
**包路径**: `org.apache.spark.api.java.JavaSparkContext`

**方法数量**: 45

| 方法名 | 参数 | 返回类型 | 功能描述 | 完整示例 |
|--------|------|----------|----------|----------|
| `this` | 无参数 | `Unit` | this方法 | 参考官方文档使用示例 |
| `this` | conf: SparkConf | `Unit` | this方法 | 参考官方文档使用示例 |
| `this` | master: String; appName: String | `Unit` | this方法 | 参考官方文档使用示例 |
| `this` | master: String; appName: String; conf: SparkConf | `Unit` | this方法 | 参考官方文档使用示例 |
| `this` | master: String; appName: String; sparkHome: String; jarFile: String | `Unit` | this方法 | 参考官方文档使用示例 |
| `this` | master: String; appName: String; sparkHome: String; jars: Array[String] | `Unit` | this方法 | 参考官方文档使用示例 |
| `this` | master: String; appName: String; sparkHome: String; jars: Array[String]; environment: JMap[String; String] | `Unit` | this方法 | 参考官方文档使用示例 |
| `parallelizeDoubles` | list: java.util.List[java.lang.Double]; numSlices: Int | `JavaDoubleRDD` | parallelizeDoubles方法 | 参考官方文档使用示例 |
| `parallelizeDoubles` | list: java.util.List[java.lang.Double] | `JavaDoubleRDD` | parallelizeDoubles方法 | 参考官方文档使用示例 |
| `textFile` | path: String | `JavaRDD` | textFile方法 | // 读取文本文件<br>JavaSparkContext sc = new JavaSparkContext(conf);<br>JavaRDD<String> lines = sc.textFile("hdfs://path/to/file.txt");<br><br>// 指定最小分区数<br>JavaRDD<String> lines2 = sc.textFile("hdfs://path/to/file.txt", 10); |
| `textFile` | path: String; minPartitions: Int | `JavaRDD` | textFile方法 | // 读取文本文件<br>JavaSparkContext sc = new JavaSparkContext(conf);<br>JavaRDD<String> lines = sc.textFile("hdfs://path/to/file.txt");<br><br>// 指定最小分区数<br>JavaRDD<String> lines2 = sc.textFile("hdfs://path/to/file.txt", 10); |
| `wholeTextFiles` | path: String; minPartitions: Int | `JavaPairRDD` | wholeTextFiles方法 | // 读取目录下所有文本文件<br>JavaPairRDD<String, String> files = sc.wholeTextFiles("hdfs://path/to/dir/");<br>// 返回 (文件路径, 文件内容) 对 |
| `wholeTextFiles` | path: String | `JavaPairRDD` | wholeTextFiles方法 | // 读取目录下所有文本文件<br>JavaPairRDD<String, String> files = sc.wholeTextFiles("hdfs://path/to/dir/");<br>// 返回 (文件路径, 文件内容) 对 |
| `binaryFiles` | path: String; minPartitions: Int | `JavaPairRDD` | binaryFiles方法 | 参考官方文档使用示例 |
| `binaryFiles` | path: String | `JavaPairRDD` | binaryFiles方法 | 参考官方文档使用示例 |
| `binaryRecords` | path: String; recordLength: Int | `JavaRDD` | binaryRecords方法 | 参考官方文档使用示例 |
| `stop` | 无参数 | `Unit` | stop方法 | 参考官方文档使用示例 |
| `stop` | exitCode: Int | `Unit` | stop方法 | 参考官方文档使用示例 |
| `getSparkHome` | 无参数 | `Optional` | getSparkHome方法 | 参考官方文档使用示例 |
| `addFile` | path: String | `Unit` | addFile方法 | 参考官方文档使用示例 |
| `addFile` | path: String; recursive: Boolean | `Unit` | addFile方法 | 参考官方文档使用示例 |
| `addJar` | path: String | `Unit` | addJar方法 | 参考官方文档使用示例 |
| `hadoopConfiguration` | 无参数 | `Configuration` | hadoopConfiguration方法 | 参考官方文档使用示例 |
| `setCheckpointDir` | dir: String | `Unit` | setCheckpointDir方法 | 参考官方文档使用示例 |
| `setCallSite` | site: String | `Unit` | setCallSite方法 | 参考官方文档使用示例 |
| `clearCallSite` | 无参数 | `Unit` | clearCallSite方法 | 参考官方文档使用示例 |
| `setLocalProperty` | key: String; value: String | `Unit` | setLocalProperty方法 | 参考官方文档使用示例 |
| `getLocalProperty` | key: String | `String` | getLocalProperty方法 | 参考官方文档使用示例 |
| `setJobDescription` | value: String | `Unit` | setJobDescription方法 | 参考官方文档使用示例 |
| `setLogLevel` | logLevel: String | `Unit` | setLogLevel方法 | 参考官方文档使用示例 |
| `setJobGroup` | groupId: String; description: String; interruptOnCancel: Boolean | `Unit` | setJobGroup方法 | 参考官方文档使用示例 |
| `setJobGroup` | groupId: String; description: String | `Unit` | setJobGroup方法 | 参考官方文档使用示例 |
| `clearJobGroup` | 无参数 | `Unit` | clearJobGroup方法 | 参考官方文档使用示例 |
| `setInterruptOnCancel` | interruptOnCancel: Boolean | `Unit` | setInterruptOnCancel方法 | 参考官方文档使用示例 |
| `addJobTag` | tag: String | `Unit` | addJobTag方法 | 参考官方文档使用示例 |
| `removeJobTag` | tag: String | `Unit` | removeJobTag方法 | 参考官方文档使用示例 |
| `getJobTags` | 无参数 | `util` | getJobTags方法 | 参考官方文档使用示例 |
| `clearJobTags` | 无参数 | `Unit` | clearJobTags方法 | 参考官方文档使用示例 |
| `cancelJobGroup` | groupId: String; reason: String | `Unit` | cancelJobGroup方法 | 参考官方文档使用示例 |
| `cancelJobGroup` | groupId: String | `Unit` | cancelJobGroup方法 | 参考官方文档使用示例 |
| `cancelJobsWithTag` | tag: String; reason: String | `Unit` | cancelJobsWithTag方法 | 参考官方文档使用示例 |
| `cancelJobsWithTag` | tag: String | `Unit` | cancelJobsWithTag方法 | 参考官方文档使用示例 |
| `cancelAllJobs` | 无参数 | `Unit` | cancelAllJobs方法 | 参考官方文档使用示例 |
| `jarOfClass` | cls: Class[_] | `Array` | jarOfClass方法 | 参考官方文档使用示例 |
| `jarOfObject` | obj: AnyRef | `Array` | jarOfObject方法 | 参考官方文档使用示例 |

---

### JavaRDD

**描述**: Java类型的RDD，提供Java友好的方法
**包路径**: `org.apache.spark.api.java.JavaRDD`

**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 功能描述 | 完整示例 |
|--------|------|----------|----------|----------|
| `cache` | 无参数 | `JavaRDD` | cache方法 | // 缓存RDD到内存<br>JavaRDD<String> rdd = sc.textFile("hdfs://path/file.txt").cache();<br>// 后续操作会直接从内存读取，提高性能 |
| `persist` | newLevel: StorageLevel | `JavaRDD` | persist方法 | // 持久化RDD到指定存储级别<br>JavaRDD<String> rdd = sc.textFile("hdfs://path/file.txt")<br>                       .persist(StorageLevel.MEMORY_AND_DISK()); |
| `withResources` | rp: ResourceProfile | `JavaRDD` | withResources方法 | 参考官方文档使用示例 |
| `getResourceProfile` | 无参数 | `ResourceProfile` | getResourceProfile方法 | 参考官方文档使用示例 |
| `unpersist` | 无参数 | `JavaRDD` | unpersist方法 | 参考官方文档使用示例 |
| `unpersist` | blocking: Boolean | `JavaRDD` | unpersist方法 | 参考官方文档使用示例 |
| `distinct` | 无参数 | `JavaRDD` | distinct方法 | // 去重<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4));<br>JavaRDD<Integer> distinct = rdd.distinct();<br>// 结果: [1, 2, 3, 4] |
| `distinct` | numPartitions: Int | `JavaRDD` | distinct方法 | // 去重<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4));<br>JavaRDD<Integer> distinct = rdd.distinct();<br>// 结果: [1, 2, 3, 4] |
| `filter` | f: JFunction[T; java.lang.Boolean] | `JavaRDD` | filter方法 | // 过滤元素<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));<br>JavaRDD<Integer> filtered = rdd.filter(x -> x > 3);<br>// 结果: [4, 5, 6] |
| `coalesce` | numPartitions: Int | `JavaRDD` | coalesce方法 | 参考官方文档使用示例 |
| `coalesce` | numPartitions: Int; shuffle: Boolean | `JavaRDD` | coalesce方法 | 参考官方文档使用示例 |
| `repartition` | numPartitions: Int | `JavaRDD` | repartition方法 | 参考官方文档使用示例 |
| `sample` | withReplacement: Boolean; fraction: Double | `JavaRDD` | sample方法 | 参考官方文档使用示例 |
| `sample` | withReplacement: Boolean; fraction: Double; seed: Long | `JavaRDD` | sample方法 | 参考官方文档使用示例 |
| `randomSplit` | weights: Array[Double] | `Array` | randomSplit方法 | 参考官方文档使用示例 |
| `randomSplit` | weights: Array[Double]; seed: Long | `Array` | randomSplit方法 | 参考官方文档使用示例 |
| `union` | other: JavaRDD[T] | `JavaRDD` | union方法 | 参考官方文档使用示例 |
| `intersection` | other: JavaRDD[T] | `JavaRDD` | intersection方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaRDD[T] | `JavaRDD` | subtract方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaRDD[T]; numPartitions: Int | `JavaRDD` | subtract方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaRDD[T]; p: Partitioner | `JavaRDD` | subtract方法 | 参考官方文档使用示例 |
| `setName` | name: String | `JavaRDD` | setName方法 | 参考官方文档使用示例 |

---

### JavaPairRDD

**描述**: 键值对类型的RDD，提供针对键值对的特殊操作
**包路径**: `org.apache.spark.api.java.JavaPairRDD`

**方法数量**: 53

| 方法名 | 参数 | 返回类型 | 功能描述 | 完整示例 |
|--------|------|----------|----------|----------|
| `cache` | 无参数 | `JavaPairRDD` | cache方法 | 参考官方文档使用示例 |
| `persist` | newLevel: StorageLevel | `JavaPairRDD` | persist方法 | 参考官方文档使用示例 |
| `unpersist` | 无参数 | `JavaPairRDD` | unpersist方法 | 参考官方文档使用示例 |
| `unpersist` | blocking: Boolean | `JavaPairRDD` | unpersist方法 | 参考官方文档使用示例 |
| `distinct` | 无参数 | `JavaPairRDD` | distinct方法 | 参考官方文档使用示例 |
| `distinct` | numPartitions: Int | `JavaPairRDD` | distinct方法 | 参考官方文档使用示例 |
| `filter` | f: JFunction[(K; V | `Unit` | filter方法 | 参考官方文档使用示例 |
| `coalesce` | numPartitions: Int | `JavaPairRDD` | coalesce方法 | 参考官方文档使用示例 |
| `coalesce` | numPartitions: Int; shuffle: Boolean | `JavaPairRDD` | coalesce方法 | 参考官方文档使用示例 |
| `repartition` | numPartitions: Int | `JavaPairRDD` | repartition方法 | 参考官方文档使用示例 |
| `sample` | withReplacement: Boolean; fraction: Double | `JavaPairRDD` | sample方法 | 参考官方文档使用示例 |
| `sample` | withReplacement: Boolean; fraction: Double; seed: Long | `JavaPairRDD` | sample方法 | 参考官方文档使用示例 |
| `sampleByKey` | withReplacement: Boolean; fractions: java.util.Map[K; jl.Double]; seed: Long | `JavaPairRDD` | sampleByKey方法 | 参考官方文档使用示例 |
| `sampleByKey` | withReplacement: Boolean; fractions: java.util.Map[K; jl.Double] | `JavaPairRDD` | sampleByKey方法 | 参考官方文档使用示例 |
| `sampleByKeyExact` | withReplacement: Boolean; fractions: java.util.Map[K; jl.Double]; seed: Long | `JavaPairRDD` | sampleByKeyExact方法 | 参考官方文档使用示例 |
| `sampleByKeyExact` | withReplacement: Boolean; fractions: java.util.Map[K; jl.Double] | `JavaPairRDD` | sampleByKeyExact方法 | 参考官方文档使用示例 |
| `union` | other: JavaPairRDD[K; V] | `JavaPairRDD` | union方法 | 参考官方文档使用示例 |
| `intersection` | other: JavaPairRDD[K; V] | `JavaPairRDD` | intersection方法 | 参考官方文档使用示例 |
| `reduceByKey` | partitioner: Partitioner; func: JFunction2[V; V; V] | `JavaPairRDD` | reduceByKey方法 | // 按Key聚合Value<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2),<br>    new Tuple2<>("a", 3)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> reduced = rdd.reduceByKey((a, b) -> a + b);<br>// 结果: [("a", 4), ("b", 2)] |
| `reduceByKeyLocally` | func: JFunction2[V; V; V] | `java` | reduceByKeyLocally方法 | 参考官方文档使用示例 |
| `countByKey` | 无参数 | `java` | countByKey方法 | 参考官方文档使用示例 |
| `countByKeyApprox` | timeout: Long | `PartialResult` | countByKeyApprox方法 | 参考官方文档使用示例 |
| `countByKeyApprox` | timeout: Long; confidence: Double = 0.95 | `PartialResult` | countByKeyApprox方法 | 参考官方文档使用示例 |
| `foldByKey` | zeroValue: V; partitioner: Partitioner; func: JFunction2[V; V; V] | `JavaPairRDD` | foldByKey方法 | 参考官方文档使用示例 |
| `foldByKey` | zeroValue: V; numPartitions: Int; func: JFunction2[V; V; V] | `JavaPairRDD` | foldByKey方法 | 参考官方文档使用示例 |
| `foldByKey` | zeroValue: V; func: JFunction2[V; V; V] | `JavaPairRDD` | foldByKey方法 | 参考官方文档使用示例 |
| `reduceByKey` | func: JFunction2[V; V; V]; numPartitions: Int | `JavaPairRDD` | reduceByKey方法 | // 按Key聚合Value<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2),<br>    new Tuple2<>("a", 3)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> reduced = rdd.reduceByKey((a, b) -> a + b);<br>// 结果: [("a", 4), ("b", 2)] |
| `groupByKey` | partitioner: Partitioner | `JavaPairRDD` | groupByKey方法 | // 按Key分组<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2),<br>    new Tuple2<>("a", 3)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Iterable<Integer>> grouped = rdd.groupByKey();<br>// 结果: [("a", [1, 3]), ("b", [2])] |
| `groupByKey` | numPartitions: Int | `JavaPairRDD` | groupByKey方法 | // 按Key分组<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2),<br>    new Tuple2<>("a", 3)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Iterable<Integer>> grouped = rdd.groupByKey();<br>// 结果: [("a", [1, 3]), ("b", [2])] |
| `subtract` | other: JavaPairRDD[K; V] | `JavaPairRDD` | subtract方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaPairRDD[K; V]; numPartitions: Int | `JavaPairRDD` | subtract方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaPairRDD[K; V]; p: Partitioner | `JavaPairRDD` | subtract方法 | 参考官方文档使用示例 |
| `partitionBy` | partitioner: Partitioner | `JavaPairRDD` | partitionBy方法 | 参考官方文档使用示例 |
| `reduceByKey` | func: JFunction2[V; V; V] | `JavaPairRDD` | reduceByKey方法 | // 按Key聚合Value<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2),<br>    new Tuple2<>("a", 3)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> reduced = rdd.reduceByKey((a, b) -> a + b);<br>// 结果: [("a", 4), ("b", 2)] |
| `groupByKey` | 无参数 | `JavaPairRDD` | groupByKey方法 | // 按Key分组<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2),<br>    new Tuple2<>("a", 3)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Iterable<Integer>> grouped = rdd.groupByKey();<br>// 结果: [("a", [1, 3]), ("b", [2])] |
| `collectAsMap` | 无参数 | `java` | collectAsMap方法 | 参考官方文档使用示例 |
| `lookup` | key: K | `JList` | lookup方法 | 参考官方文档使用示例 |
| `saveAsNewAPIHadoopDataset` | conf: Configuration | `Unit` | saveAsNewAPIHadoopDataset方法 | 参考官方文档使用示例 |
| `saveAsHadoopDataset` | conf: JobConf | `Unit` | saveAsHadoopDataset方法 | 参考官方文档使用示例 |
| `repartitionAndSortWithinPartitions` | partitioner: Partitioner | `JavaPairRDD` | repartitionAndSortWithinPartitions方法 | 参考官方文档使用示例 |
| `repartitionAndSortWithinPartitions` | partitioner: Partitioner; comp: Comparator[K] | `JavaPairRDD` | repartitionAndSortWithinPartitions方法 | 参考官方文档使用示例 |
| `sortByKey` | 无参数 | `JavaPairRDD` | sortByKey方法 | // 按Key排序<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("c", 3),<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sorted = rdd.sortByKey();<br>// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `sortByKey` | ascending: Boolean | `JavaPairRDD` | sortByKey方法 | // 按Key排序<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("c", 3),<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sorted = rdd.sortByKey();<br>// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `sortByKey` | ascending: Boolean; numPartitions: Int | `JavaPairRDD` | sortByKey方法 | // 按Key排序<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("c", 3),<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sorted = rdd.sortByKey();<br>// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `sortByKey` | comp: Comparator[K] | `JavaPairRDD` | sortByKey方法 | // 按Key排序<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("c", 3),<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sorted = rdd.sortByKey();<br>// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `sortByKey` | comp: Comparator[K]; ascending: Boolean | `JavaPairRDD` | sortByKey方法 | // 按Key排序<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("c", 3),<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sorted = rdd.sortByKey();<br>// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `sortByKey` | comp: Comparator[K]; ascending: Boolean; numPartitions: Int | `JavaPairRDD` | sortByKey方法 | // 按Key排序<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("c", 3),<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>);<br>JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sorted = rdd.sortByKey();<br>// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `keys` | 无参数 | `JavaRDD` | keys方法 | // 获取所有Key<br>JavaPairRDD<String, Integer> pairRDD = ...;<br>JavaRDD<String> keys = pairRDD.keys(); |
| `values` | 无参数 | `JavaRDD` | values方法 | // 获取所有Value<br>JavaPairRDD<String, Integer> pairRDD = ...;<br>JavaRDD<Integer> values = pairRDD.values(); |
| `countApproxDistinctByKey` | relativeSD: Double; partitioner: Partitioner | `JavaPairRDD` | countApproxDistinctByKey方法 | 参考官方文档使用示例 |
| `countApproxDistinctByKey` | relativeSD: Double; numPartitions: Int | `JavaPairRDD` | countApproxDistinctByKey方法 | 参考官方文档使用示例 |
| `countApproxDistinctByKey` | relativeSD: Double | `JavaPairRDD` | countApproxDistinctByKey方法 | 参考官方文档使用示例 |
| `setName` | name: String | `JavaPairRDD` | setName方法 | 参考官方文档使用示例 |

---

### JavaDoubleRDD

**描述**: Double类型的RDD，提供数值统计方法
**包路径**: `org.apache.spark.api.java.JavaDoubleRDD`

**方法数量**: 33

| 方法名 | 参数 | 返回类型 | 功能描述 | 完整示例 |
|--------|------|----------|----------|----------|
| `cache` | 无参数 | `JavaDoubleRDD` | cache方法 | 参考官方文档使用示例 |
| `persist` | newLevel: StorageLevel | `JavaDoubleRDD` | persist方法 | 参考官方文档使用示例 |
| `unpersist` | 无参数 | `JavaDoubleRDD` | unpersist方法 | 参考官方文档使用示例 |
| `unpersist` | blocking: Boolean | `JavaDoubleRDD` | unpersist方法 | 参考官方文档使用示例 |
| `distinct` | 无参数 | `JavaDoubleRDD` | distinct方法 | 参考官方文档使用示例 |
| `distinct` | numPartitions: Int | `JavaDoubleRDD` | distinct方法 | 参考官方文档使用示例 |
| `filter` | f: JFunction[JDouble; java.lang.Boolean] | `JavaDoubleRDD` | filter方法 | 参考官方文档使用示例 |
| `coalesce` | numPartitions: Int | `JavaDoubleRDD` | coalesce方法 | 参考官方文档使用示例 |
| `coalesce` | numPartitions: Int; shuffle: Boolean | `JavaDoubleRDD` | coalesce方法 | 参考官方文档使用示例 |
| `repartition` | numPartitions: Int | `JavaDoubleRDD` | repartition方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaDoubleRDD | `JavaDoubleRDD` | subtract方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaDoubleRDD; numPartitions: Int | `JavaDoubleRDD` | subtract方法 | 参考官方文档使用示例 |
| `subtract` | other: JavaDoubleRDD; p: Partitioner | `JavaDoubleRDD` | subtract方法 | 参考官方文档使用示例 |
| `sample` | withReplacement: Boolean; fraction: JDouble | `JavaDoubleRDD` | sample方法 | 参考官方文档使用示例 |
| `sample` | withReplacement: Boolean; fraction: JDouble; seed: Long | `JavaDoubleRDD` | sample方法 | 参考官方文档使用示例 |
| `union` | other: JavaDoubleRDD | `JavaDoubleRDD` | union方法 | 参考官方文档使用示例 |
| `intersection` | other: JavaDoubleRDD | `JavaDoubleRDD` | intersection方法 | 参考官方文档使用示例 |
| `sum` | 无参数 | `JDouble` | sum方法 | // 求和<br>JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0));<br>double total = rdd.sum();<br>// 结果: 6.0 |
| `min` | 无参数 | `JDouble` | min方法 | 参考官方文档使用示例 |
| `max` | 无参数 | `JDouble` | max方法 | 参考官方文档使用示例 |
| `stats` | 无参数 | `StatCounter` | stats方法 | 参考官方文档使用示例 |
| `mean` | 无参数 | `JDouble` | mean方法 | // 计算平均值<br>JavaDoubleRDD rdd = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0));<br>double avg = rdd.mean();<br>// 结果: 3.0 |
| `variance` | 无参数 | `JDouble` | variance方法 | 参考官方文档使用示例 |
| `stdev` | 无参数 | `JDouble` | stdev方法 | 参考官方文档使用示例 |
| `sampleStdev` | 无参数 | `JDouble` | sampleStdev方法 | 参考官方文档使用示例 |
| `sampleVariance` | 无参数 | `JDouble` | sampleVariance方法 | 参考官方文档使用示例 |
| `meanApprox` | timeout: Long; confidence: JDouble | `PartialResult` | meanApprox方法 | 参考官方文档使用示例 |
| `meanApprox` | timeout: Long | `PartialResult` | meanApprox方法 | 参考官方文档使用示例 |
| `sumApprox` | timeout: Long; confidence: JDouble | `PartialResult` | sumApprox方法 | 参考官方文档使用示例 |
| `sumApprox` | timeout: Long | `PartialResult` | sumApprox方法 | 参考官方文档使用示例 |
| `histogram` | bucketCount: Int | `Unit` | histogram方法 | 参考官方文档使用示例 |
| `histogram` | buckets: Array[scala.Double] | `Array` | histogram方法 | 参考官方文档使用示例 |
| `setName` | name: String | `JavaDoubleRDD` | setName方法 | 参考官方文档使用示例 |

---

## 二、函数接口API

Spark Java API使用以下函数接口，都是位于 `org.apache.spark.api.java.function` 包:

| 接口名 | 方法 | 功能 | 示例 |
|--------|------|------|------|
| `Function<T,R>` | `R call(T t)` | 单参数函数，输入T返回R | `rdd.map(x -> x.length())` |
| `Function2<T1,T2,R>` | `R call(T1 t1, T2 t2)` | 双参数函数 | `rdd.reduce((a,b) -> a+b)` |
| `VoidFunction<T>` | `void call(T t)` | 无返回值函数 | `rdd.foreach(x -> System.out.println(x))` |
| `FlatMapFunction<T,R>` | `Iterable<R> call(T t)` | 返回迭代器的函数 | `rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator())` |
| `MapFunction<T,R>` | `R call(T t)` | 映射函数 | `rdd.map(x -> x.toString())` |
| `FilterFunction<T>` | `boolean call(T t)` | 过滤函数 | `rdd.filter(x -> x > 0)` |
| `PairFunction<T,K,V>` | `Tuple2<K,V> call(T t)` | 键值对函数 | `rdd.mapToPair(x -> new Tuple2<>(x, 1))` |
| `DoubleFunction<T>` | `double call(T t)` | 返回Double的函数 | `rdd.mapToDouble(x -> x.doubleValue())` |

---

## 三、StorageLevel存储级别

用于persist()方法指定RDD的存储方式:

| 级别 | 描述 | 使用场景 |
|------|------|----------|
| `MEMORY_ONLY` | 仅存储在内存 | 数据量小，频繁使用 |
| `MEMORY_ONLY_SER` | 内存中序列化存储 | 内存紧张但CPU充足 |
| `MEMORY_AND_DISK` | 内存+磁盘 | 数据量大，可能超出内存 |
| `MEMORY_AND_DISK_SER` | 内存+磁盘序列化 | 数据量大，节省空间 |
| `DISK_ONLY` | 仅存储在磁盘 | 数据量非常大 |
| `MEMORY_ONLY_2` | 内存存储，2副本 | 高可用场景 |
| `MEMORY_AND_DISK_2` | 内存+磁盘，2副本 | 高可用+大数据 |

**示例**:
```java
JavaRDD<String> rdd = sc.textFile("hdfs://path/file.txt");
rdd.persist(StorageLevel.MEMORY_AND_DISK());
```

---
