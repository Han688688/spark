# Spark Java API 完整文档 - 所有Public方法

> **基于Spark源代码完整提取**
> **Spark版本**: 4.2.0-SNAPSHOT
> **生成日期**: 2026-05-09
> **总类数**: 412
> **总方法数**: 4218

---

## 目录

- [org.apache.spark](2 个类)
- [org.apache.spark.api.java](8 个类)
- [org.apache.spark.internal](2 个类)
- [org.apache.spark.io](1 个类)
- [org.apache.spark.launcher](5 个类)
- [org.apache.spark.memory](1 个类)
- [org.apache.spark.ml](7 个类)
- [org.apache.spark.ml.ann](3 个类)
- [org.apache.spark.ml.attribute](3 个类)
- [org.apache.spark.ml.classification](12 个类)
- [org.apache.spark.ml.clustering](5 个类)
- [org.apache.spark.ml.evaluation](8 个类)
- [org.apache.spark.ml.feature](40 个类)
- [org.apache.spark.ml.fpm](2 个类)
- [org.apache.spark.ml.image](2 个类)
- [org.apache.spark.ml.linalg](4 个类)
- [org.apache.spark.ml.optim](3 个类)
- [org.apache.spark.ml.optim.aggregator](7 个类)
- [org.apache.spark.ml.optim.loss](2 个类)
- [org.apache.spark.ml.param](1 个类)
- [org.apache.spark.ml.python](2 个类)
- [org.apache.spark.ml.r](23 个类)
- [org.apache.spark.ml.recommendation](1 个类)
- [org.apache.spark.ml.regression](8 个类)
- [org.apache.spark.ml.source.image](1 个类)
- [org.apache.spark.ml.source.libsvm](2 个类)
- [org.apache.spark.ml.stat](7 个类)
- [org.apache.spark.ml.tree](4 个类)
- [org.apache.spark.ml.tree.impl](6 个类)
- [org.apache.spark.ml.tuning](4 个类)
- [org.apache.spark.ml.util](8 个类)
- [org.apache.spark.mllib.api.python](5 个类)
- [org.apache.spark.mllib.classification](5 个类)
- [org.apache.spark.mllib.classification.impl](1 个类)
- [org.apache.spark.mllib.clustering](13 个类)
- [org.apache.spark.mllib.evaluation](6 个类)
- [org.apache.spark.mllib.evaluation.binary](1 个类)
- [org.apache.spark.mllib.feature](8 个类)
- [org.apache.spark.mllib.fpm](5 个类)
- [org.apache.spark.mllib.linalg](5 个类)
- [org.apache.spark.mllib.linalg.distributed](4 个类)
- [org.apache.spark.mllib.optimization](5 个类)
- [org.apache.spark.mllib.pmml.](1 个类)
- [org.apache.spark.mllib.random](2 个类)
- [org.apache.spark.mllib.rdd](4 个类)
- [org.apache.spark.mllib.recommendation](2 个类)
- [org.apache.spark.mllib.regression](9 个类)
- [org.apache.spark.mllib.regression.impl](1 个类)
- [org.apache.spark.mllib.stat](3 个类)
- [org.apache.spark.mllib.stat.correlation](3 个类)
- [org.apache.spark.mllib.stat.distribution](1 个类)
- [org.apache.spark.mllib.tree](3 个类)
- [org.apache.spark.mllib.tree.configuration](2 个类)
- [org.apache.spark.mllib.tree.impurity](4 个类)
- [org.apache.spark.mllib.tree.loss](5 个类)
- [org.apache.spark.mllib.tree.model](5 个类)
- [org.apache.spark.mllib.util](9 个类)
- [org.apache.spark.network.util](2 个类)
- [org.apache.spark.shuffle.checksum](1 个类)
- [org.apache.spark.shuffle.sort](1 个类)
- [org.apache.spark.shuffle.sort.io](5 个类)
- [org.apache.spark.sql](1 个类)
- [org.apache.spark.sql.avro](1 个类)
- [org.apache.spark.sql.catalyst.expressions](8 个类)
- [org.apache.spark.sql.catalyst.expressions.json](1 个类)
- [org.apache.spark.sql.catalyst.expressions.xml](1 个类)
- [org.apache.spark.sql.catalyst.util](2 个类)
- [org.apache.spark.sql.catalyst.util.geo](3 个类)
- [org.apache.spark.sql.connector.catalog](8 个类)
- [org.apache.spark.sql.connector.catalog.constraints](4 个类)
- [org.apache.spark.sql.connector.catalog.functions](1 个类)
- [org.apache.spark.sql.connector.catalog.procedures](1 个类)
- [org.apache.spark.sql.connector.distributions](1 个类)
- [org.apache.spark.sql.connector.expressions](7 个类)
- [org.apache.spark.sql.connector.expressions.aggregate](1 个类)
- [org.apache.spark.sql.connector.metric](1 个类)
- [org.apache.spark.sql.connector.read](1 个类)
- [org.apache.spark.sql.connector.read.partitioning](2 个类)
- [org.apache.spark.sql.connector.read.streaming](3 个类)
- [org.apache.spark.sql.connector.util](1 个类)
- [org.apache.spark.sql.connector.write](1 个类)
- [org.apache.spark.sql.execution](1 个类)
- [org.apache.spark.sql.execution.datasources](2 个类)
- [org.apache.spark.sql.execution.datasources.orc](8 个类)
- [org.apache.spark.sql.execution.datasources.parquet](10 个类)
- [org.apache.spark.sql.execution.vectorized](3 个类)
- [org.apache.spark.sql.expressions.javalang](1 个类)
- [org.apache.spark.sql.internal](1 个类)
- [org.apache.spark.sql.internal.types](3 个类)
- [org.apache.spark.sql.streaming](4 个类)
- [org.apache.spark.sql.types](1 个类)
- [org.apache.spark.sql.util](2 个类)
- [org.apache.spark.sql.vectorized](2 个类)
- [org.apache.spark.status.api.v1](4 个类)
- [org.apache.spark.status.api.v1.streaming](1 个类)
- [org.apache.spark.streaming.api.java](7 个类)
- [org.apache.spark.unsafe.map](1 个类)
- [org.apache.spark.util](5 个类)
- [org.apache.spark.util.collection.unsafe.sort](2 个类)

---

## 包: org.apache.spark

**类数量**: 2

### JobExecutionStatus

**完整类名**: `org.apache.spark.JobExecutionStatus`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | String str | JobExecutionStatus | 暂无描述 | `fromString(...)` |

---

### SparkFirehoseListener

**完整类名**: `org.apache.spark.SparkFirehoseListener`

**描述**: Class that allows users to receive all SparkListener events.

**方法数**: 36

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onEvent` | SparkListenerEvent event | void | 暂无描述 | `onEvent(...)` |
| `onStageCompleted` | SparkListenerStageCompleted stageCompleted | void | 暂无描述 | `onStageCompleted(...)` |
| `onStageSubmitted` | SparkListenerStageSubmitted stageSubmitted | void | 暂无描述 | `onStageSubmitted(...)` |
| `onTaskStart` | SparkListenerTaskStart taskStart | void | 暂无描述 | `onTaskStart(...)` |
| `onTaskGettingResult` | SparkListenerTaskGettingResult taskGettingResult | void | 暂无描述 | `onTaskGettingResult(...)` |
| `onTaskEnd` | SparkListenerTaskEnd taskEnd | void | 暂无描述 | `onTaskEnd(...)` |
| `onJobStart` | SparkListenerJobStart jobStart | void | 暂无描述 | `onJobStart(...)` |
| `onJobEnd` | SparkListenerJobEnd jobEnd | void | 暂无描述 | `onJobEnd(...)` |
| `onEnvironmentUpdate` | SparkListenerEnvironmentUpdate environmentUpdate | void | 暂无描述 | `onEnvironmentUpdate(...)` |
| `onBlockManagerAdded` | SparkListenerBlockManagerAdded blockManagerAdded | void | 暂无描述 | `onBlockManagerAdded(...)` |
| `onBlockManagerRemoved` | SparkListenerBlockManagerRemoved blockManagerRemoved | void | 暂无描述 | `onBlockManagerRemoved(...)` |
| `onUnpersistRDD` | SparkListenerUnpersistRDD unpersistRDD | void | 暂无描述 | `onUnpersistRDD(...)` |
| `onApplicationStart` | SparkListenerApplicationStart applicationStart | void | 暂无描述 | `onApplicationStart(...)` |
| `onApplicationEnd` | SparkListenerApplicationEnd applicationEnd | void | 暂无描述 | `onApplicationEnd(...)` |
| `onExecutorMetricsUpdate` | SparkListenerExecutorMetricsUpdate executorMetricsUpdate | void | 暂无描述 | `onExecutorMetricsUpdate(...)` |
| `onStageExecutorMetrics` | SparkListenerStageExecutorMetrics executorMetrics | void | 暂无描述 | `onStageExecutorMetrics(...)` |
| `onExecutorAdded` | SparkListenerExecutorAdded executorAdded | void | 暂无描述 | `onExecutorAdded(...)` |
| `onExecutorRemoved` | SparkListenerExecutorRemoved executorRemoved | void | 暂无描述 | `onExecutorRemoved(...)` |
| `onExecutorBlacklisted` | SparkListenerExecutorBlacklisted executorBlacklisted | void | 暂无描述 | `onExecutorBlacklisted(...)` |
| `onExecutorExcluded` | SparkListenerExecutorExcluded executorExcluded | void | 暂无描述 | `onExecutorExcluded(...)` |
| `onExecutorBlacklistedForStage` | SparkListenerExecutorBlacklistedForStage executorBlacklistedForStage | void | 暂无描述 | `onExecutorBlacklistedForStage(...)` |
| `onExecutorExcludedForStage` | SparkListenerExecutorExcludedForStage executorExcludedForStage | void | 暂无描述 | `onExecutorExcludedForStage(...)` |
| `onNodeBlacklistedForStage` | SparkListenerNodeBlacklistedForStage nodeBlacklistedForStage | void | 暂无描述 | `onNodeBlacklistedForStage(...)` |
| `onNodeExcludedForStage` | SparkListenerNodeExcludedForStage nodeExcludedForStage | void | 暂无描述 | `onNodeExcludedForStage(...)` |
| `onExecutorUnblacklisted` | SparkListenerExecutorUnblacklisted executorUnblacklisted | void | 暂无描述 | `onExecutorUnblacklisted(...)` |
| `onExecutorUnexcluded` | SparkListenerExecutorUnexcluded executorUnexcluded | void | 暂无描述 | `onExecutorUnexcluded(...)` |
| `onNodeBlacklisted` | SparkListenerNodeBlacklisted nodeBlacklisted | void | 暂无描述 | `onNodeBlacklisted(...)` |
| `onNodeExcluded` | SparkListenerNodeExcluded nodeExcluded | void | 暂无描述 | `onNodeExcluded(...)` |
| `onNodeUnblacklisted` | SparkListenerNodeUnblacklisted nodeUnblacklisted | void | 暂无描述 | `onNodeUnblacklisted(...)` |
| `onNodeUnexcluded` | SparkListenerNodeUnexcluded nodeUnexcluded | void | 暂无描述 | `onNodeUnexcluded(...)` |
| `onBlockUpdated` | SparkListenerBlockUpdated blockUpdated | void | 暂无描述 | `onBlockUpdated(...)` |
| `onSpeculativeTaskSubmitted` | SparkListenerSpeculativeTaskSubmitted speculativeTask | void | 暂无描述 | `onSpeculativeTaskSubmitted(...)` |
| `onUnschedulableTaskSetAdded` | SparkListenerUnschedulableTaskSetAdded unschedulableTaskSetAdded | void | 暂无描述 | `onUnschedulableTaskSetAdded(...)` |
| `onUnschedulableTaskSetRemoved` | SparkListenerUnschedulableTaskSetRemoved unschedulableTaskSetRemoved | void | 暂无描述 | `onUnschedulableTaskSetRemoved(...)` |
| `onResourceProfileAdded` | SparkListenerResourceProfileAdded event | void | 暂无描述 | `onResourceProfileAdded(...)` |
| `onOtherEvent` | SparkListenerEvent event | void | 暂无描述 | `onOtherEvent(...)` |

---

## 包: org.apache.spark.api.java

**类数量**: 8

### JavaDoubleRDD

**完整类名**: `org.apache.spark.api.java.JavaDoubleRDD`

**描述**: Scala定义的Java友好接口

**方法数**: 40

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wrapRDD` | rdd: RDD[JDouble] | JavaDoubleRDD | Scala方法 | `wrapRDD(...)` |
| `cache` | 无 | JavaDoubleRDD | Scala方法 | `cache(...)` |
| `persist` | newLevel: StorageLevel | JavaDoubleRDD | Scala方法 | `persist(...)` |
| `unpersist` | 无 | JavaDoubleRDD | Scala方法 | `unpersist(...)` |
| `unpersist` | blocking: Boolean | JavaDoubleRDD | Scala方法 | `unpersist(...)` |
| `first` | 无 | JDouble | Scala方法 | `first(...)` |
| `distinct` | 无 | JavaDoubleRDD | Scala方法 | `distinct(...)` |
| `distinct` | numPartitions: Int | JavaDoubleRDD | Scala方法 | `distinct(...)` |
| `filter` | f: JFunction[JDouble, java.lang.Boolean] | JavaDoubleRDD | Scala方法 | `filter(...)` |
| `coalesce` | numPartitions: Int | JavaDoubleRDD | Scala方法 | `coalesce(...)` |
| `coalesce` | numPartitions: Int, shuffle: Boolean | JavaDoubleRDD | Scala方法 | `coalesce(...)` |
| `repartition` | numPartitions: Int | JavaDoubleRDD | Scala方法 | `repartition(...)` |
| `subtract` | other: JavaDoubleRDD | JavaDoubleRDD | Scala方法 | `subtract(...)` |
| `subtract` | other: JavaDoubleRDD, numPartitions: Int | JavaDoubleRDD | Scala方法 | `subtract(...)` |
| `subtract` | other: JavaDoubleRDD, p: Partitioner | JavaDoubleRDD | Scala方法 | `subtract(...)` |
| `sample` | withReplacement: Boolean, fraction: JDouble | JavaDoubleRDD | Scala方法 | `sample(...)` |
| `sample` | withReplacement: Boolean, fraction: JDouble, seed: Long | JavaDoubleRDD | Scala方法 | `sample(...)` |
| `union` | other: JavaDoubleRDD | JavaDoubleRDD | Scala方法 | `union(...)` |
| `intersection` | other: JavaDoubleRDD | JavaDoubleRDD | Scala方法 | `intersection(...)` |
| `sum` | 无 | JDouble | Scala方法 | `sum(...)` |
| `min` | 无 | JDouble | Scala方法 | `min(...)` |
| `max` | 无 | JDouble | Scala方法 | `max(...)` |
| `stats` | 无 | StatCounter | Scala方法 | `stats(...)` |
| `mean` | 无 | JDouble | Scala方法 | `mean(...)` |
| `variance` | 无 | JDouble | Scala方法 | `variance(...)` |
| `stdev` | 无 | JDouble | Scala方法 | `stdev(...)` |
| `sampleStdev` | 无 | JDouble | Scala方法 | `sampleStdev(...)` |
| `sampleVariance` | 无 | JDouble | Scala方法 | `sampleVariance(...)` |
| `popStdev` | 无 | JDouble | Scala方法 | `popStdev(...)` |
| `popVariance` | 无 | JDouble | Scala方法 | `popVariance(...)` |
| `meanApprox` | timeout: Long, confidence: JDouble | PartialResult[BoundedDouble] | Scala方法 | `meanApprox(...)` |
| `meanApprox` | timeout: Long | PartialResult[BoundedDouble] | Scala方法 | `meanApprox(...)` |
| `sumApprox` | timeout: Long, confidence: JDouble | PartialResult[BoundedDouble] | Scala方法 | `sumApprox(...)` |
| `sumApprox` | timeout: Long | PartialResult[BoundedDouble] | Scala方法 | `sumApprox(...)` |
| `histogram` | bucketCount: Int |  | Scala方法 | `histogram(...)` |
| `histogram` | buckets: Array[scala.Double] | Array[Long] | Scala方法 | `histogram(...)` |
| `histogram` | buckets: Array[JDouble], evenBuckets: Boolean | Array[Long] | Scala方法 | `histogram(...)` |
| `setName` | name: String | JavaDoubleRDD | Scala方法 | `setName(...)` |
| `fromRDD` | rdd: RDD[scala.Double] | JavaDoubleRDD | Scala方法 | `fromRDD(...)` |
| `toRDD` | rdd: JavaDoubleRDD | RDD[scala | Scala方法 | `toRDD(...)` |

---

### JavaPairRDD

**完整类名**: `org.apache.spark.api.java.JavaPairRDD`

**描述**: Scala定义的Java友好接口

**方法数**: 57

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wrapRDD` | rdd: RDD[(K, V | Unit | Scala方法 | `wrapRDD(...)` |
| `cache` | 无 | JavaPairRDD[K, V] | Scala方法 | `cache(...)` |
| `persist` | newLevel: StorageLevel | JavaPairRDD[K, V] | Scala方法 | `persist(...)` |
| `unpersist` | 无 | JavaPairRDD[K, V] | Scala方法 | `unpersist(...)` |
| `unpersist` | blocking: Boolean | JavaPairRDD[K, V] | Scala方法 | `unpersist(...)` |
| `distinct` | 无 | JavaPairRDD[K, V] | Scala方法 | `distinct(...)` |
| `distinct` | numPartitions: Int | JavaPairRDD[K, V] | Scala方法 | `distinct(...)` |
| `filter` | f: JFunction[(K, V | Unit | Scala方法 | `filter(...)` |
| `coalesce` | numPartitions: Int | JavaPairRDD[K, V] | Scala方法 | `coalesce(...)` |
| `coalesce` | numPartitions: Int, shuffle: Boolean | JavaPairRDD[K, V] | Scala方法 | `coalesce(...)` |
| `repartition` | numPartitions: Int | JavaPairRDD[K, V] | Scala方法 | `repartition(...)` |
| `sample` | withReplacement: Boolean, fraction: Double | JavaPairRDD[K, V] | Scala方法 | `sample(...)` |
| `sample` | withReplacement: Boolean, fraction: Double, seed: Long | JavaPairRDD[K, V] | Scala方法 | `sample(...)` |
| `sampleByKey` | withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double],
      seed: Long | JavaPairRDD[K, V] | Scala方法 | `sampleByKey(...)` |
| `sampleByKey` | withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double] | JavaPairRDD[K, V] | Scala方法 | `sampleByKey(...)` |
| `sampleByKeyExact` | withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double],
      seed: Long | JavaPairRDD[K, V] | Scala方法 | `sampleByKeyExact(...)` |
| `sampleByKeyExact` | withReplacement: Boolean,
      fractions: java.util.Map[K, jl.Double] | JavaPairRDD[K, V] | Scala方法 | `sampleByKeyExact(...)` |
| `union` | other: JavaPairRDD[K, V] | JavaPairRDD[K, V] | Scala方法 | `union(...)` |
| `intersection` | other: JavaPairRDD[K, V] | JavaPairRDD[K, V] | Scala方法 | `intersection(...)` |
| `first` | 无 |  | Scala方法 | `first(...)` |
| `reduceByKey` | partitioner: Partitioner, func: JFunction2[V, V, V] | JavaPairRDD[K, V] | Scala方法 | `reduceByKey(...)` |
| `reduceByKeyLocally` | func: JFunction2[V, V, V] | java | Scala方法 | `reduceByKeyLocally(...)` |
| `countByKey` | 无 | java | Scala方法 | `countByKey(...)` |
| `countByKeyApprox` | timeout: Long | PartialResult[java | Scala方法 | `countByKeyApprox(...)` |
| `countByKeyApprox` | timeout: Long, confidence: Double = 0.95 | PartialResult[java | Scala方法 | `countByKeyApprox(...)` |
| `foldByKey` | zeroValue: V, partitioner: Partitioner, func: JFunction2[V, V, V] | JavaPairRDD[K, V] | Scala方法 | `foldByKey(...)` |
| `foldByKey` | zeroValue: V, numPartitions: Int, func: JFunction2[V, V, V] | JavaPairRDD[K, V] | Scala方法 | `foldByKey(...)` |
| `foldByKey` | zeroValue: V, func: JFunction2[V, V, V] | JavaPairRDD[K, V] | Scala方法 | `foldByKey(...)` |
| `reduceByKey` | func: JFunction2[V, V, V], numPartitions: Int | JavaPairRDD[K, V] | Scala方法 | `reduceByKey(...)` |
| `groupByKey` | partitioner: Partitioner | JavaPairRDD[K, JIterable[V]] | Scala方法 | `groupByKey(...)` |
| `groupByKey` | numPartitions: Int | JavaPairRDD[K, JIterable[V]] | Scala方法 | `groupByKey(...)` |
| `subtract` | other: JavaPairRDD[K, V] | JavaPairRDD[K, V] | Scala方法 | `subtract(...)` |
| `subtract` | other: JavaPairRDD[K, V], numPartitions: Int | JavaPairRDD[K, V] | Scala方法 | `subtract(...)` |
| `subtract` | other: JavaPairRDD[K, V], p: Partitioner | JavaPairRDD[K, V] | Scala方法 | `subtract(...)` |
| `partitionBy` | partitioner: Partitioner | JavaPairRDD[K, V] | Scala方法 | `partitionBy(...)` |
| `reduceByKey` | func: JFunction2[V, V, V] | JavaPairRDD[K, V] | Scala方法 | `reduceByKey(...)` |
| `groupByKey` | 无 | JavaPairRDD[K, JIterable[V]] | Scala方法 | `groupByKey(...)` |
| `collectAsMap` | 无 | java | Scala方法 | `collectAsMap(...)` |
| `lookup` | key: K | JList[V] | Scala方法 | `lookup(...)` |
| `saveAsNewAPIHadoopDataset` | conf: Configuration | Unit | Scala方法 | `saveAsNewAPIHadoopDataset(...)` |
| `saveAsHadoopDataset` | conf: JobConf | Unit | Scala方法 | `saveAsHadoopDataset(...)` |
| `repartitionAndSortWithinPartitions` | partitioner: Partitioner | JavaPairRDD[K, V] | Scala方法 | `repartitionAndSortWithinPartitions(...)` |
| `repartitionAndSortWithinPartitions` | partitioner: Partitioner, comp: Comparator[K] | JavaPairRDD[K, V] | Scala方法 | `repartitionAndSortWithinPartitions(...)` |
| `sortByKey` | 无 | JavaPairRDD[K, V] | Scala方法 | `sortByKey(...)` |
| `sortByKey` | ascending: Boolean | JavaPairRDD[K, V] | Scala方法 | `sortByKey(...)` |
| `sortByKey` | ascending: Boolean, numPartitions: Int | JavaPairRDD[K, V] | Scala方法 | `sortByKey(...)` |
| `sortByKey` | comp: Comparator[K] | JavaPairRDD[K, V] | Scala方法 | `sortByKey(...)` |
| `sortByKey` | comp: Comparator[K], ascending: Boolean | JavaPairRDD[K, V] | Scala方法 | `sortByKey(...)` |
| `sortByKey` | comp: Comparator[K], ascending: Boolean, numPartitions: Int | JavaPairRDD[K, V] | Scala方法 | `sortByKey(...)` |
| `filterByRange` | lower: K, upper: K | JavaPairRDD[K, V] | Scala方法 | `filterByRange(...)` |
| `filterByRange` | comp: Comparator[K], lower: K, upper: K | JavaPairRDD[K, V] | Scala方法 | `filterByRange(...)` |
| `keys` | 无 | JavaRDD[K] | Scala方法 | `keys(...)` |
| `values` | 无 | JavaRDD[V] | Scala方法 | `values(...)` |
| `countApproxDistinctByKey` | relativeSD: Double, partitioner: Partitioner | JavaPairRDD[K, jl | Scala方法 | `countApproxDistinctByKey(...)` |
| `countApproxDistinctByKey` | relativeSD: Double, numPartitions: Int | JavaPairRDD[K, jl | Scala方法 | `countApproxDistinctByKey(...)` |
| `countApproxDistinctByKey` | relativeSD: Double | JavaPairRDD[K, jl | Scala方法 | `countApproxDistinctByKey(...)` |
| `setName` | name: String | JavaPairRDD[K, V] | Scala方法 | `setName(...)` |

---

### JavaRDD

**完整类名**: `org.apache.spark.api.java.JavaRDD`

**描述**: Scala定义的Java友好接口

**方法数**: 23

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wrapRDD` | rdd: RDD[T] | JavaRDD[T] | Scala方法 | `wrapRDD(...)` |
| `cache` | 无 | JavaRDD[T] | Scala方法 | `cache(...)` |
| `persist` | newLevel: StorageLevel | JavaRDD[T] | Scala方法 | `persist(...)` |
| `withResources` | rp: ResourceProfile | JavaRDD[T] | Scala方法 | `withResources(...)` |
| `getResourceProfile` | 无 | ResourceProfile | Scala方法 | `getResourceProfile(...)` |
| `unpersist` | 无 | JavaRDD[T] | Scala方法 | `unpersist(...)` |
| `unpersist` | blocking: Boolean | JavaRDD[T] | Scala方法 | `unpersist(...)` |
| `distinct` | 无 | JavaRDD[T] | Scala方法 | `distinct(...)` |
| `distinct` | numPartitions: Int | JavaRDD[T] | Scala方法 | `distinct(...)` |
| `filter` | f: JFunction[T, java.lang.Boolean] | JavaRDD[T] | Scala方法 | `filter(...)` |
| `coalesce` | numPartitions: Int | JavaRDD[T] | Scala方法 | `coalesce(...)` |
| `coalesce` | numPartitions: Int, shuffle: Boolean | JavaRDD[T] | Scala方法 | `coalesce(...)` |
| `repartition` | numPartitions: Int | JavaRDD[T] | Scala方法 | `repartition(...)` |
| `sample` | withReplacement: Boolean, fraction: Double | JavaRDD[T] | Scala方法 | `sample(...)` |
| `sample` | withReplacement: Boolean, fraction: Double, seed: Long | JavaRDD[T] | Scala方法 | `sample(...)` |
| `randomSplit` | weights: Array[Double] | Array[JavaRDD[T]] | Scala方法 | `randomSplit(...)` |
| `randomSplit` | weights: Array[Double], seed: Long | Array[JavaRDD[T]] | Scala方法 | `randomSplit(...)` |
| `union` | other: JavaRDD[T] | JavaRDD[T] | Scala方法 | `union(...)` |
| `intersection` | other: JavaRDD[T] | JavaRDD[T] | Scala方法 | `intersection(...)` |
| `subtract` | other: JavaRDD[T] | JavaRDD[T] | Scala方法 | `subtract(...)` |
| `subtract` | other: JavaRDD[T], numPartitions: Int | JavaRDD[T] | Scala方法 | `subtract(...)` |
| `subtract` | other: JavaRDD[T], p: Partitioner | JavaRDD[T] | Scala方法 | `subtract(...)` |
| `setName` | name: String | JavaRDD[T] | Scala方法 | `setName(...)` |

---

### JavaSparkContext

**完整类名**: `org.apache.spark.api.java.JavaSparkContext`

**描述**: Scala定义的Java友好接口

**方法数**: 49

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `this` | conf: SparkConf | Unit | Scala方法 | `this(...)` |
| `this` | master: String, appName: String | Unit | Scala方法 | `this(...)` |
| `this` | master: String, appName: String, conf: SparkConf | Unit | Scala方法 | `this(...)` |
| `this` | master: String, appName: String, sparkHome: String, jarFile: String | Unit | Scala方法 | `this(...)` |
| `this` | master: String, appName: String, sparkHome: String, jars: Array[String] | Unit | Scala方法 | `this(...)` |
| `this` | master: String, appName: String, sparkHome: String, jars: Array[String],
      environment: JMap[String, String] | Unit | Scala方法 | `this(...)` |
| `parallelizeDoubles` | list: java.util.List[java.lang.Double], numSlices: Int | JavaDoubleRDD | Scala方法 | `parallelizeDoubles(...)` |
| `parallelizeDoubles` | list: java.util.List[java.lang.Double] | JavaDoubleRDD | Scala方法 | `parallelizeDoubles(...)` |
| `textFile` | path: String | JavaRDD[String] | Scala方法 | `textFile(...)` |
| `textFile` | path: String, minPartitions: Int | JavaRDD[String] | Scala方法 | `textFile(...)` |
| `wholeTextFiles` | path: String, minPartitions: Int | JavaPairRDD[String, String] | Scala方法 | `wholeTextFiles(...)` |
| `wholeTextFiles` | path: String | JavaPairRDD[String, String] | Scala方法 | `wholeTextFiles(...)` |
| `binaryFiles` | path: String, minPartitions: Int | JavaPairRDD[String, PortableDataStream] | Scala方法 | `binaryFiles(...)` |
| `binaryFiles` | path: String | JavaPairRDD[String, PortableDataStream] | Scala方法 | `binaryFiles(...)` |
| `binaryRecords` | path: String, recordLength: Int | JavaRDD[Array[Byte]] | Scala方法 | `binaryRecords(...)` |
| `union` | rdds: JavaDoubleRDD* | JavaDoubleRDD | Scala方法 | `union(...)` |
| `stop` | 无 | Unit | Scala方法 | `stop(...)` |
| `stop` | exitCode: Int | Unit | Scala方法 | `stop(...)` |
| `close` | 无 | Unit | Scala方法 | `close(...)` |
| `getSparkHome` | 无 | Optional[String] | Scala方法 | `getSparkHome(...)` |
| `addFile` | path: String | Unit | Scala方法 | `addFile(...)` |
| `addFile` | path: String, recursive: Boolean | Unit | Scala方法 | `addFile(...)` |
| `addJar` | path: String | Unit | Scala方法 | `addJar(...)` |
| `hadoopConfiguration` | 无 | Configuration | Scala方法 | `hadoopConfiguration(...)` |
| `setCheckpointDir` | dir: String | Unit | Scala方法 | `setCheckpointDir(...)` |
| `setCallSite` | site: String | Unit | Scala方法 | `setCallSite(...)` |
| `clearCallSite` | 无 | Unit | Scala方法 | `clearCallSite(...)` |
| `setLocalProperty` | key: String, value: String | Unit | Scala方法 | `setLocalProperty(...)` |
| `getLocalProperty` | key: String | String | Scala方法 | `getLocalProperty(...)` |
| `setJobDescription` | value: String | Unit | Scala方法 | `setJobDescription(...)` |
| `setLogLevel` | logLevel: String | Unit | Scala方法 | `setLogLevel(...)` |
| `setJobGroup` | groupId: String, description: String, interruptOnCancel: Boolean | Unit | Scala方法 | `setJobGroup(...)` |
| `setJobGroup` | groupId: String, description: String | Unit | Scala方法 | `setJobGroup(...)` |
| `clearJobGroup` | 无 | Unit | Scala方法 | `clearJobGroup(...)` |
| `setInterruptOnCancel` | interruptOnCancel: Boolean | Unit | Scala方法 | `setInterruptOnCancel(...)` |
| `addJobTag` | tag: String | Unit | Scala方法 | `addJobTag(...)` |
| `removeJobTag` | tag: String | Unit | Scala方法 | `removeJobTag(...)` |
| `getJobTags` | 无 | util | Scala方法 | `getJobTags(...)` |
| `clearJobTags` | 无 | Unit | Scala方法 | `clearJobTags(...)` |
| `cancelJobGroup` | groupId: String, reason: String | Unit | Scala方法 | `cancelJobGroup(...)` |
| `cancelJobGroup` | groupId: String | Unit | Scala方法 | `cancelJobGroup(...)` |
| `cancelJobsWithTag` | tag: String, reason: String | Unit | Scala方法 | `cancelJobsWithTag(...)` |
| `cancelJobsWithTag` | tag: String | Unit | Scala方法 | `cancelJobsWithTag(...)` |
| `cancelAllJobs` | 无 | Unit | Scala方法 | `cancelAllJobs(...)` |
| `fromSparkContext` | sc: SparkContext | JavaSparkContext | Scala方法 | `fromSparkContext(...)` |
| `toSparkContext` | jsc: JavaSparkContext | SparkContext | Scala方法 | `toSparkContext(...)` |
| `jarOfClass` | cls: Class[_] | Array[String] | Scala方法 | `jarOfClass(...)` |
| `jarOfObject` | obj: AnyRef | Array[String] | Scala方法 | `jarOfObject(...)` |

---

### JavaSparkStatusTracker

**完整类名**: `org.apache.spark.api.java.JavaSparkStatusTracker`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getJobIdsForGroup` | jobGroup: String | Array[Int] | Scala方法 | `getJobIdsForGroup(...)` |
| `getActiveStageIds` | 无 | Array[Int] | Scala方法 | `getActiveStageIds(...)` |
| `getActiveJobIds` | 无 | Array[Int] | Scala方法 | `getActiveJobIds(...)` |
| `getJobInfo` | jobId: Int | SparkJobInfo | Scala方法 | `getJobInfo(...)` |
| `getStageInfo` | stageId: Int | SparkStageInfo | Scala方法 | `getStageInfo(...)` |

---

### JavaUtils

**完整类名**: `org.apache.spark.api.java.JavaUtils`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `containsKey` | key: AnyRef | Boolean | Scala方法 | `containsKey(...)` |
| `get` | key: AnyRef | B | Scala方法 | `get(...)` |
| `next` | 无 | Entry[A, B] | Scala方法 | `next(...)` |
| `setValue` | v1 : B | B | Scala方法 | `setValue(...)` |
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `remove` | 无 | Unit | Scala方法 | `remove(...)` |

---

### StorageLevels

**完整类名**: `org.apache.spark.api.java.StorageLevels`

**描述**: Expose some commonly useful storage level constants.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | boolean useDisk,
    boolean useMemory,
    boolean useOffHeap,
    boolean deserialized,
    int replication | StorageLevel | 暂无描述 | `create(...)` |

---

### instead

**完整类名**: `org.apache.spark.api.java.instead`

**描述**: Scala定义的Java友好接口

**方法数**: 52

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wrapRDD` | rdd: RDD[T] | This

  implicit val classTag | Scala方法 | `wrapRDD(...)` |
| `iterator` | split: Partition, taskContext: TaskContext | JIterator[T] | Scala方法 | `iterator(...)` |
| `flatMapToDouble` | f: DoubleFlatMapFunction[T] | JavaDoubleRDD | Scala方法 | `flatMapToDouble(...)` |
| `mapPartitionsToDouble` | f: DoubleFlatMapFunction[JIterator[T]] | JavaDoubleRDD | Scala方法 | `mapPartitionsToDouble(...)` |
| `mapPartitionsToDouble` | f: DoubleFlatMapFunction[JIterator[T]],
      preservesPartitioning: Boolean | JavaDoubleRDD | Scala方法 | `mapPartitionsToDouble(...)` |
| `foreachPartition` | f: VoidFunction[JIterator[T]] | Unit | Scala方法 | `foreachPartition(...)` |
| `glom` | 无 | JavaRDD[JList[T]] | Scala方法 | `glom(...)` |
| `pipe` | command: String | JavaRDD[String] | Scala方法 | `pipe(...)` |
| `pipe` | command: JList[String] | JavaRDD[String] | Scala方法 | `pipe(...)` |
| `pipe` | command: JList[String], env: JMap[String, String] | JavaRDD[String] | Scala方法 | `pipe(...)` |
| `pipe` | command: JList[String],
           env: JMap[String, String],
           separateWorkingDir: Boolean,
           bufferSize: Int | JavaRDD[String] | Scala方法 | `pipe(...)` |
| `pipe` | command: JList[String],
           env: JMap[String, String],
           separateWorkingDir: Boolean,
           bufferSize: Int,
           encoding: String | JavaRDD[String] | Scala方法 | `pipe(...)` |
| `zipWithUniqueId` | 无 | JavaPairRDD[T, jl | Scala方法 | `zipWithUniqueId(...)` |
| `zipWithIndex` | 无 | JavaPairRDD[T, jl | Scala方法 | `zipWithIndex(...)` |
| `foreach` | f: VoidFunction[T] | Unit | Scala方法 | `foreach(...)` |
| `collect` | 无 | JList[T] | Scala方法 | `collect(...)` |
| `toLocalIterator` | 无 | JIterator[T] | Scala方法 | `toLocalIterator(...)` |
| `collectPartitions` | partitionIds: Array[Int] | Array[JList[T]] | Scala方法 | `collectPartitions(...)` |
| `reduce` | f: JFunction2[T, T, T] | T | Scala方法 | `reduce(...)` |
| `treeReduce` | f: JFunction2[T, T, T], depth: Int | T | Scala方法 | `treeReduce(...)` |
| `treeReduce` | f: JFunction2[T, T, T] | T | Scala方法 | `treeReduce(...)` |
| `fold` | zeroValue: T | Unit | Scala方法 | `fold(...)` |
| `count` | 无 | Long | Scala方法 | `count(...)` |
| `countApprox` | timeout: Long, confidence: Double | PartialResult[BoundedDouble] | Scala方法 | `countApprox(...)` |
| `countApprox` | timeout: Long | PartialResult[BoundedDouble] | Scala方法 | `countApprox(...)` |
| `countByValue` | 无 | JMap[T, jl | Scala方法 | `countByValue(...)` |
| `countByValueApprox` | timeout: Long,
    confidence: Double | PartialResult[JMap[T, BoundedDouble]] | Scala方法 | `countByValueApprox(...)` |
| `countByValueApprox` | timeout: Long | PartialResult[JMap[T, BoundedDouble]] | Scala方法 | `countByValueApprox(...)` |
| `take` | num: Int | JList[T] | Scala方法 | `take(...)` |
| `takeSample` | withReplacement: Boolean, num: Int | JList[T] | Scala方法 | `takeSample(...)` |
| `takeSample` | withReplacement: Boolean, num: Int, seed: Long | JList[T] | Scala方法 | `takeSample(...)` |
| `first` | 无 | T | Scala方法 | `first(...)` |
| `isEmpty` | 无 | Boolean | Scala方法 | `isEmpty(...)` |
| `saveAsTextFile` | path: String | Unit | Scala方法 | `saveAsTextFile(...)` |
| `saveAsTextFile` | path: String, codec: Class[_ <: CompressionCodec] | Unit | Scala方法 | `saveAsTextFile(...)` |
| `saveAsObjectFile` | path: String | Unit | Scala方法 | `saveAsObjectFile(...)` |
| `checkpoint` | 无 | Unit | Scala方法 | `checkpoint(...)` |
| `getCheckpointFile` | 无 | Optional[String] | Scala方法 | `getCheckpointFile(...)` |
| `toDebugString` | 无 | String | Scala方法 | `toDebugString(...)` |
| `top` | num: Int, comp: Comparator[T] | JList[T] | Scala方法 | `top(...)` |
| `top` | num: Int | JList[T] | Scala方法 | `top(...)` |
| `takeOrdered` | num: Int, comp: Comparator[T] | JList[T] | Scala方法 | `takeOrdered(...)` |
| `max` | comp: Comparator[T] | T | Scala方法 | `max(...)` |
| `min` | comp: Comparator[T] | T | Scala方法 | `min(...)` |
| `takeOrdered` | num: Int | JList[T] | Scala方法 | `takeOrdered(...)` |
| `countApproxDistinct` | relativeSD: Double | Long | Scala方法 | `countApproxDistinct(...)` |
| `name` | 无 | String | Scala方法 | `name(...)` |
| `countAsync` | 无 | JavaFutureAction[jl | Scala方法 | `countAsync(...)` |
| `collectAsync` | 无 | JavaFutureAction[JList[T]] | Scala方法 | `collectAsync(...)` |
| `takeAsync` | num: Int | JavaFutureAction[JList[T]] | Scala方法 | `takeAsync(...)` |
| `foreachAsync` | f: VoidFunction[T] | JavaFutureAction[Void] | Scala方法 | `foreachAsync(...)` |
| `foreachPartitionAsync` | f: VoidFunction[JIterator[T]] | JavaFutureAction[Void] | Scala方法 | `foreachPartitionAsync(...)` |

---

## 包: org.apache.spark.internal

**类数量**: 2

### CustomLogKeys

**完整类名**: `org.apache.spark.internal.CustomLogKeys`

**描述**: Guidelines for the Structured Logging Framework - Java Logging

**方法数**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isErrorEnabled` | 无 | boolean | 暂无描述 | `isErrorEnabled(...)` |
| `error` | String msg | void | 暂无描述 | `error(...)` |
| `error` | String msg, Throwable throwable | void | 暂无描述 | `error(...)` |
| `error` | String msg, MDC... mdcs | void | 暂无描述 | `error(...)` |
| `error` | String msg, Throwable throwable, MDC... mdcs | void | 暂无描述 | `error(...)` |
| `isWarnEnabled` | 无 | boolean | 暂无描述 | `isWarnEnabled(...)` |
| `warn` | String msg | void | 暂无描述 | `warn(...)` |
| `warn` | String msg, Throwable throwable | void | 暂无描述 | `warn(...)` |
| `warn` | String msg, MDC... mdcs | void | 暂无描述 | `warn(...)` |
| `warn` | String msg, Throwable throwable, MDC... mdcs | void | 暂无描述 | `warn(...)` |
| `isInfoEnabled` | 无 | boolean | 暂无描述 | `isInfoEnabled(...)` |
| `info` | String msg | void | 暂无描述 | `info(...)` |
| `info` | String msg, Throwable throwable | void | 暂无描述 | `info(...)` |
| `info` | String msg, MDC... mdcs | void | 暂无描述 | `info(...)` |
| `info` | String msg, Throwable throwable, MDC... mdcs | void | 暂无描述 | `info(...)` |
| `isDebugEnabled` | 无 | boolean | 暂无描述 | `isDebugEnabled(...)` |
| `debug` | String msg | void | 暂无描述 | `debug(...)` |
| `debug` | String format, Object arg | void | 暂无描述 | `debug(...)` |
| `debug` | String format, Object arg1, Object arg2 | void | 暂无描述 | `debug(...)` |
| `debug` | String format, Object... arguments | void | 暂无描述 | `debug(...)` |
| `debug` | String msg, Throwable throwable | void | 暂无描述 | `debug(...)` |
| `isTraceEnabled` | 无 | boolean | 暂无描述 | `isTraceEnabled(...)` |
| `trace` | String msg | void | 暂无描述 | `trace(...)` |
| `trace` | String format, Object arg | void | 暂无描述 | `trace(...)` |
| `trace` | String format, Object arg1, Object arg2 | void | 暂无描述 | `trace(...)` |
| `trace` | String format, Object... arguments | void | 暂无描述 | `trace(...)` |
| `trace` | String msg, Throwable throwable | void | 暂无描述 | `trace(...)` |
| `getSlf4jLogger` | 无 | Logger | 暂无描述 | `getSlf4jLogger(...)` |

---

### SparkLoggerFactory

**完整类名**: `org.apache.spark.internal.SparkLoggerFactory`

**描述**: 暂无描述

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `enableStructuredLogging` | 无 | void | 暂无描述 | `enableStructuredLogging(...)` |
| `disableStructuredLogging` | 无 | void | 暂无描述 | `disableStructuredLogging(...)` |
| `isStructuredLoggingEnabled` | 无 | boolean | 暂无描述 | `isStructuredLoggingEnabled(...)` |
| `getLogger` | String name | SparkLogger | 暂无描述 | `getLogger(...)` |
| `getLogger` | Class<?> clazz | SparkLogger | 暂无描述 | `getLogger(...)` |

---

## 包: org.apache.spark.io

**类数量**: 1

### ReadAheadInputStream

**完整类名**: `org.apache.spark.io.ReadAheadInputStream`

**描述**: {@link InputStream} implementation which asynchronously reads ahead from the underlying input

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `read` | 无 | int | 暂无描述 | `read(...)` |
| `read` | byte[] b, int offset, int len | int | 暂无描述 | `read(...)` |
| `available` | 无 | int | 暂无描述 | `available(...)` |
| `skip` | long n | long | 暂无描述 | `skip(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |

---

## 包: org.apache.spark.launcher

**类数量**: 5

### InProcessLauncher

**完整类名**: `org.apache.spark.launcher.InProcessLauncher`

**描述**: In-process launcher for Spark applications.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `startApplication` | SparkAppHandle.Listener... listeners | SparkAppHandle | 暂无描述 | `startApplication(...)` |

---

### JavaModuleOptions

**完整类名**: `org.apache.spark.launcher.JavaModuleOptions`

**描述**: This helper class is used to place some JVM runtime options(eg: `--add-opens`)

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultModuleOptions` | 无 | String | Returns the default JVM runtime options used by Spark. | `defaultModuleOptions(...)` |
| `defaultModuleOptionArray` | 无 | String[] | Returns the default JVM runtime option array used by Spark. | `defaultModuleOptionArray(...)` |

---

### MyLauncher

**完整类名**: `org.apache.spark.launcher.MyLauncher`

**描述**: Library for launching Spark applications programmatically.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | String[] args | void | 暂无描述 | `main(...)` |
| `main` | String[] args | void | 暂无描述 | `main(...)` |

---

### SparkAppHandle

**完整类名**: `org.apache.spark.launcher.SparkAppHandle`

**描述**: A handle to a running Spark application.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isFinal` | 无 | boolean | Whether this state is a final state, meaning the application is not running anymore | `isFinal(...)` |

---

### SparkLauncher

**完整类名**: `org.apache.spark.launcher.SparkLauncher`

**描述**: Launcher for Spark applications.

**方法数**: 26

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setConfig` | String name, String value | void | 暂无描述 | `setConfig(...)` |
| `setJavaHome` | String javaHome | SparkLauncher | Set a custom JAVA_HOME for launching the Spark application. | `setJavaHome(...)` |
| `setSparkHome` | String sparkHome | SparkLauncher | Set a custom Spark installation location for the application. | `setSparkHome(...)` |
| `directory` | File dir | SparkLauncher | Sets the working directory of spark-submit. | `directory(...)` |
| `redirectError` | 无 | SparkLauncher | Specifies that stderr in spark-submit should be redirected to stdout. | `redirectError(...)` |
| `redirectError` | ProcessBuilder.Redirect to | SparkLauncher | Redirects error output to the specified Redirect. | `redirectError(...)` |
| `redirectOutput` | ProcessBuilder.Redirect to | SparkLauncher | Redirects standard output to the specified Redirect. | `redirectOutput(...)` |
| `redirectError` | File errFile | SparkLauncher | Redirects error output to the specified File. | `redirectError(...)` |
| `redirectOutput` | File outFile | SparkLauncher | Redirects error output to the specified File. | `redirectOutput(...)` |
| `redirectToLog` | String loggerName | SparkLauncher | 暂无描述 | `redirectToLog(...)` |
| `setPropertiesFile` | String path | SparkLauncher | 暂无描述 | `setPropertiesFile(...)` |
| `setConf` | String key, String value | SparkLauncher | 暂无描述 | `setConf(...)` |
| `setAppName` | String appName | SparkLauncher | 暂无描述 | `setAppName(...)` |
| `setMaster` | String master | SparkLauncher | 暂无描述 | `setMaster(...)` |
| `setDeployMode` | String mode | SparkLauncher | 暂无描述 | `setDeployMode(...)` |
| `setAppResource` | String resource | SparkLauncher | 暂无描述 | `setAppResource(...)` |
| `setMainClass` | String mainClass | SparkLauncher | 暂无描述 | `setMainClass(...)` |
| `addSparkArg` | String arg | SparkLauncher | 暂无描述 | `addSparkArg(...)` |
| `addSparkArg` | String name, String value | SparkLauncher | 暂无描述 | `addSparkArg(...)` |
| `addAppArgs` | String... args | SparkLauncher | 暂无描述 | `addAppArgs(...)` |
| `addJar` | String jar | SparkLauncher | 暂无描述 | `addJar(...)` |
| `addFile` | String file | SparkLauncher | 暂无描述 | `addFile(...)` |
| `addPyFile` | String file | SparkLauncher | 暂无描述 | `addPyFile(...)` |
| `setVerbose` | boolean verbose | SparkLauncher | 暂无描述 | `setVerbose(...)` |
| `launch` | 无 | Process | 暂无描述 | `launch(...)` |
| `startApplication` | SparkAppHandle.Listener... listeners | SparkAppHandle | 暂无描述 | `startApplication(...)` |

---

## 包: org.apache.spark.memory

**类数量**: 1

### TaskMemoryManager

**完整类名**: `org.apache.spark.memory.TaskMemoryManager`

**描述**: Manages the memory allocated by an individual task.

**方法数**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acquireExecutionMemory` | long required, MemoryConsumer requestingConsumer | long | 暂无描述 | `acquireExecutionMemory(...)` |
| `releaseExecutionMemory` | long size, MemoryConsumer consumer | void | Release N bytes of execution memory for a MemoryConsumer. | `releaseExecutionMemory(...)` |
| `showMemoryUsage` | 无 | void | Dump the memory usage of all consumers. | `showMemoryUsage(...)` |
| `pageSizeBytes` | 无 | long | Return the page size in bytes. | `pageSizeBytes(...)` |
| `allocatePage` | long size, MemoryConsumer consumer | MemoryBlock | Return the page size in bytes. | `allocatePage(...)` |
| `freePage` | MemoryBlock page, MemoryConsumer consumer | void | Free a block of memory allocated via {@link TaskMemoryManager#allocatePage}. | `freePage(...)` |
| `encodePageNumberAndOffset` | MemoryBlock page, long offsetInPage | long | 暂无描述 | `encodePageNumberAndOffset(...)` |
| `encodePageNumberAndOffset` | int pageNumber, long offsetInPage | long | 暂无描述 | `encodePageNumberAndOffset(...)` |
| `decodePageNumber` | long pagePlusOffsetAddress | int | 暂无描述 | `decodePageNumber(...)` |
| `getPage` | long pagePlusOffsetAddress | Object | Get the page associated with an address encoded by | `getPage(...)` |
| `getOffsetInPage` | long pagePlusOffsetAddress | long | Get the offset associated with an address encoded by | `getOffsetInPage(...)` |
| `cleanUpAllAllocatedMemory` | 无 | long | Clean up all allocated memory and pages. Returns the number of bytes freed. A non-zero return | `cleanUpAllAllocatedMemory(...)` |
| `getMemoryConsumptionForThisTask` | 无 | long | Returns the memory consumption, in bytes, for the current task. | `getMemoryConsumptionForThisTask(...)` |
| `getTungstenMemoryMode` | 无 | MemoryMode | Returns Tungsten memory mode | `getTungstenMemoryMode(...)` |
| `getPeakOnHeapExecutionMemory` | 无 | long | Returns peak task-level off-heap memory usage in bytes. | `getPeakOnHeapExecutionMemory(...)` |
| `getPeakOffHeapExecutionMemory` | 无 | long | Returns peak task-level on-heap memory usage in bytes. | `getPeakOffHeapExecutionMemory(...)` |

---

## 包: org.apache.spark.ml

**类数量**: 7

### Model

**完整类名**: `org.apache.spark.ml.Model`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setParent` | parent: Estimator[M] | M | Scala方法 | `setParent(...)` |
| `copy` | extra: ParamMap | M | Scala方法 | `copy(...)` |

---

### PipelineStage

**完整类名**: `org.apache.spark.ml.PipelineStage`

**描述**: Scala定义的Java友好接口

**方法数**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | PipelineStage | Scala方法 | `copy(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setStages` | value: Array[_ <: PipelineStage] | this | Scala方法 | `setStages(...)` |
| `fit` | dataset: Dataset[_] | PipelineModel | Scala方法 | `fit(...)` |
| `copy` | extra: ParamMap | Pipeline | Scala方法 | `copy(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | Pipeline | Scala方法 | `load(...)` |
| `save` | path: String | Unit | Scala方法 | `save(...)` |
| `validateStages` | stages: Array[PipelineStage] | Unit | Scala方法 | `validateStages(...)` |
| `saveImpl` | instance: Params,
        stages: Array[PipelineStage],
        sc: SparkContext,
        path: String | Unit | Scala方法 | `saveImpl(...)` |
| `saveImpl` | instance: Params,
        stages: Array[PipelineStage],
        spark: SparkSession,
        path: String | Unit | Scala方法 | `saveImpl(...)` |
| `load` | expectedClassName: String,
        sc: SparkContext,
        path: String |  | Scala方法 | `load(...)` |
| `load` | expectedClassName: String,
        spark: SparkSession,
        path: String |  | Scala方法 | `load(...)` |
| `getStagePath` | stageUid: String, stageIdx: Int, numStages: Int, stagesDir: String | String | Scala方法 | `getStagePath(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | PipelineModel | Scala方法 | `copy(...)` |
| `load` | path: String | PipelineModel | Scala方法 | `load(...)` |
| `save` | path: String | Unit | Scala方法 | `save(...)` |

---

### TransformStart

**完整类名**: `org.apache.spark.ml.TransformStart`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `logEvent` | event: MLEvent | Unit | Scala方法 | `logEvent(...)` |
| `withTransformEvent` | transformer: Transformer, input: Dataset[_] | Unit | Scala方法 | `withTransformEvent(...)` |
| `withSaveInstanceEvent` | writer: MLWriter, path: String | Unit | Scala方法 | `withSaveInstanceEvent(...)` |

---

### for

**完整类名**: `org.apache.spark.ml.for`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_],
      firstParamPair: ParamPair[_],
      otherParamPairs: ParamPair[_]* | DataFrame | Scala方法 | `transform(...)` |
| `transform` | dataset: Dataset[_], paramMap: ParamMap | DataFrame | Scala方法 | `transform(...)` |
| `transform` | dataset: Dataset[_] | DataFrame

  override def copy | Scala方法 | `transform(...)` |
| `setInputCol` | value: String | T | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | T | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `copy` | extra: ParamMap | T | Scala方法 | `copy(...)` |

---

### for

**完整类名**: `org.apache.spark.ml.for`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fit` | dataset: Dataset[_], firstParamPair: ParamPair[_], otherParamPairs: ParamPair[_]* | M | Scala方法 | `fit(...)` |
| `fit` | dataset: Dataset[_], paramMap: ParamMap | M | Scala方法 | `fit(...)` |
| `fit` | dataset: Dataset[_] | M | Scala方法 | `fit(...)` |
| `fit` | dataset: Dataset[_], paramMaps: Seq[ParamMap] | Seq[M] | Scala方法 | `fit(...)` |
| `copy` | extra: ParamMap | Estimator[M] | Scala方法 | `copy(...)` |

---

### functions

**完整类名**: `org.apache.spark.ml.functions`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `vector_to_array` | v: Column, dtype: String = "float64" | Column | Scala方法 | `vector_to_array(...)` |
| `array_to_vector` | v: Column | Column | Scala方法 | `array_to_vector(...)` |

---

### this

**完整类名**: `org.apache.spark.ml.this`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setLabelCol` | value: String | Learner | Scala方法 | `setLabelCol(...)` |
| `setFeaturesCol` | value: String | Learner | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | Learner | Scala方法 | `setPredictionCol(...)` |
| `fit` | dataset: Dataset[_] | M | Scala方法 | `fit(...)` |
| `copy` | extra: ParamMap | Learner | Scala方法 | `copy(...)` |
| `setFeaturesCol` | value: String | M | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | M | Scala方法 | `setPredictionCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predict` | features: FeaturesType | Double | Scala方法 | `predict(...)` |

---

## 包: org.apache.spark.ml.ann

**类数量**: 3

### AffineLayer

**完整类名**: `org.apache.spark.ml.ann.AffineLayer`

**描述**: Scala定义的Java友好接口

**方法数**: 43

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOutputSize` | inputSize: Int | Int | Scala方法 | `getOutputSize(...)` |
| `createModel` | initialWeights: BDV[Double] | LayerModel | Scala方法 | `createModel(...)` |
| `initModel` | weights: BDV[Double], random: Random | LayerModel | Scala方法 | `initModel(...)` |
| `eval` | data: BDM[Double], output: BDM[Double] | Unit | Scala方法 | `eval(...)` |
| `computePrevDelta` | delta: BDM[Double], output: BDM[Double], prevDelta: BDM[Double] | Unit | Scala方法 | `computePrevDelta(...)` |
| `grad` | delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double] | Unit | Scala方法 | `grad(...)` |
| `getOutputSize` | inputSize: Int | Int | Scala方法 | `getOutputSize(...)` |
| `createModel` | weights: BDV[Double] | LayerModel | Scala方法 | `createModel(...)` |
| `initModel` | weights: BDV[Double], random: Random | LayerModel | Scala方法 | `initModel(...)` |
| `computePrevDelta` | delta: BDM[Double],
    output: BDM[Double],
    prevDelta: BDM[Double] | Unit | Scala方法 | `computePrevDelta(...)` |
| `grad` | delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double] | Unit | Scala方法 | `grad(...)` |
| `apply` | layer: AffineLayer, weights: BDV[Double], random: Random | AffineLayerModel | Scala方法 | `apply(...)` |
| `randomWeights` | numIn: Int,
    numOut: Int,
    weights: BDV[Double],
    random: Random | Unit | Scala方法 | `randomWeights(...)` |
| `apply` | x1: BDM[Double],
    x2: BDM[Double],
    y: BDM[Double],
    func: (Double, Double | Unit | Scala方法 | `apply(...)` |
| `getOutputSize` | inputSize: Int | Int | Scala方法 | `getOutputSize(...)` |
| `createModel` | weights: BDV[Double] | LayerModel | Scala方法 | `createModel(...)` |
| `initModel` | weights: BDV[Double], random: Random | LayerModel | Scala方法 | `initModel(...)` |
| `eval` | data: BDM[Double], output: BDM[Double] | Unit | Scala方法 | `eval(...)` |
| `computePrevDelta` | nextDelta: BDM[Double],
    input: BDM[Double],
    delta: BDM[Double] | Unit | Scala方法 | `computePrevDelta(...)` |
| `grad` | delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double] | Unit | Scala方法 | `grad(...)` |
| `forward` | data: BDM[Double], includeLastLayer: Boolean | Array[BDM[Double]] | Scala方法 | `forward(...)` |
| `predict` | features: Vector | Vector | Scala方法 | `predict(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `raw2ProbabilityInPlace` | rawPrediction: Vector | Vector | Scala方法 | `raw2ProbabilityInPlace(...)` |
| `computeGradient` | data: BDM[Double], target: BDM[Double], cumGradient: Vector,
                      blockSize: Int | Double | Scala方法 | `computeGradient(...)` |
| `model` | seed: Long | TopologyModel | Scala方法 | `model(...)` |
| `apply` | layers: Array[Layer] | FeedForwardTopology | Scala方法 | `apply(...)` |
| `multiLayerPerceptron` | layerSizes: Array[Int],
    softmaxOnTop: Boolean = true | FeedForwardTopology | Scala方法 | `multiLayerPerceptron(...)` |
| `computeGradient` | data: BDM[Double],
    target: BDM[Double],
    cumGradient: Vector,
    realBatchSize: Int | Double | Scala方法 | `computeGradient(...)` |
| `predict` | data: Vector | Vector | Scala方法 | `predict(...)` |
| `predictRaw` | data: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `raw2ProbabilityInPlace` | data: Vector | Vector | Scala方法 | `raw2ProbabilityInPlace(...)` |
| `apply` | topology: FeedForwardTopology, weights: Vector | FeedForwardModel | Scala方法 | `apply(...)` |
| `apply` | topology: FeedForwardTopology, seed: Long = 11L | FeedForwardModel | Scala方法 | `apply(...)` |
| `compute` | data: OldVector,
    label: Double,
    weights: OldVector,
    cumGradient: OldVector | Double | Scala方法 | `compute(...)` |
| `stack` | data: RDD[(Vector, Vector | Unit | Scala方法 | `stack(...)` |
| `unstack` | data: Vector |  | Scala方法 | `unstack(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setWeights` | value: Vector | this | Scala方法 | `setWeights(...)` |
| `setStackSize` | value: Int | this | Scala方法 | `setStackSize(...)` |
| `setUpdater` | value: Updater | this | Scala方法 | `setUpdater(...)` |
| `setGradient` | value: Gradient | this | Scala方法 | `setGradient(...)` |
| `train` | data: RDD[(Vector, Vector | Unit | Scala方法 | `train(...)` |

---

### BreezeUtil

**完整类名**: `org.apache.spark.ml.ann.BreezeUtil`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `dgemm` | alpha: Double, A: BDM[Double], B: BDM[Double], beta: Double, C: BDM[Double] | Unit | Scala方法 | `dgemm(...)` |
| `dgemv` | alpha: Double, A: BDM[Double], x: BDV[Double], beta: Double, y: BDV[Double] | Unit | Scala方法 | `dgemv(...)` |

---

### SigmoidLayerWithSquaredError

**完整类名**: `org.apache.spark.ml.ann.SigmoidLayerWithSquaredError`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `loss` | output: BDM[Double], target: BDM[Double], delta: BDM[Double] | Double | Scala方法 | `loss(...)` |
| `getOutputSize` | inputSize: Int | Int | Scala方法 | `getOutputSize(...)` |
| `createModel` | weights: BDV[Double] | LayerModel | Scala方法 | `createModel(...)` |
| `initModel` | weights: BDV[Double], random: Random | LayerModel | Scala方法 | `initModel(...)` |
| `loss` | output: BDM[Double], target: BDM[Double], delta: BDM[Double] | Double | Scala方法 | `loss(...)` |
| `getOutputSize` | inputSize: Int | Int | Scala方法 | `getOutputSize(...)` |
| `createModel` | weights: BDV[Double] | LayerModel | Scala方法 | `createModel(...)` |
| `initModel` | weights: BDV[Double], random: Random | LayerModel | Scala方法 | `initModel(...)` |
| `eval` | data: BDM[Double], output: BDM[Double] | Unit | Scala方法 | `eval(...)` |
| `computePrevDelta` | nextDelta: BDM[Double],
    input: BDM[Double],
    delta: BDM[Double] | Unit | Scala方法 | `computePrevDelta(...)` |
| `grad` | delta: BDM[Double], input: BDM[Double], cumGrad: BDV[Double] | Unit | Scala方法 | `grad(...)` |
| `loss` | output: BDM[Double], target: BDM[Double], delta: BDM[Double] | Double | Scala方法 | `loss(...)` |

---

## 包: org.apache.spark.ml.attribute

**类数量**: 3

### AttributeGroup

**完整类名**: `org.apache.spark.ml.attribute.AttributeGroup`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | name: String | Unit | Scala方法 | `this(...)` |
| `this` | name: String, numAttributes: Int | Unit | Scala方法 | `this(...)` |
| `this` | name: String, attrs: Array[Attribute] | Unit | Scala方法 | `this(...)` |
| `hasAttr` | attrName: String | Boolean | Scala方法 | `hasAttr(...)` |
| `indexOf` | attrName: String | Int | Scala方法 | `indexOf(...)` |
| `apply` | attrName: String | Attribute | Scala方法 | `apply(...)` |
| `getAttr` | attrName: String | Attribute | Scala方法 | `getAttr(...)` |
| `apply` | attrIndex: Int | Attribute | Scala方法 | `apply(...)` |
| `getAttr` | attrIndex: Int | Attribute | Scala方法 | `getAttr(...)` |
| `toMetadata` | existingMetadata: Metadata | Metadata | Scala方法 | `toMetadata(...)` |
| `toMetadata` | 无 | Metadata | Scala方法 | `toMetadata(...)` |
| `toStructField` | existingMetadata: Metadata | StructField | Scala方法 | `toStructField(...)` |
| `toStructField` | 无 | StructField | Scala方法 | `toStructField(...)` |
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `fromStructField` | field: StructField | AttributeGroup | Scala方法 | `fromStructField(...)` |

---

### AttributeType

**完整类名**: `org.apache.spark.ml.attribute.AttributeType`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromName` | name: String | AttributeType | Scala方法 | `fromName(...)` |

---

### for

**完整类名**: `org.apache.spark.ml.attribute.for`

**描述**: Scala定义的Java友好接口

**方法数**: 29

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `withName` | name: String | Attribute | Scala方法 | `withName(...)` |
| `withIndex` | index: Int | Attribute | Scala方法 | `withIndex(...)` |
| `toMetadata` | existingMetadata: Metadata | Metadata | Scala方法 | `toMetadata(...)` |
| `toMetadata` | 无 | Metadata | Scala方法 | `toMetadata(...)` |
| `toStructField` | existingMetadata: Metadata | StructField | Scala方法 | `toStructField(...)` |
| `toStructField` | 无 | StructField | Scala方法 | `toStructField(...)` |
| `fromStructField` | field: StructField | Attribute | Scala方法 | `fromStructField(...)` |
| `withName` | name: String | NumericAttribute | Scala方法 | `withName(...)` |
| `withIndex` | index: Int | NumericAttribute | Scala方法 | `withIndex(...)` |
| `withMin` | min: Double | NumericAttribute | Scala方法 | `withMin(...)` |
| `withMax` | max: Double | NumericAttribute | Scala方法 | `withMax(...)` |
| `withStd` | std: Double | NumericAttribute | Scala方法 | `withStd(...)` |
| `withSparsity` | sparsity: Double | NumericAttribute | Scala方法 | `withSparsity(...)` |
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `indexOf` | value: String | Int | Scala方法 | `indexOf(...)` |
| `hasValue` | value: String | Boolean | Scala方法 | `hasValue(...)` |
| `getValue` | index: Int | String | Scala方法 | `getValue(...)` |
| `withName` | name: String | NominalAttribute | Scala方法 | `withName(...)` |
| `withIndex` | index: Int | NominalAttribute | Scala方法 | `withIndex(...)` |
| `withValues` | values: Array[String] | NominalAttribute | Scala方法 | `withValues(...)` |
| `withValues` | first: String, others: String* | NominalAttribute | Scala方法 | `withValues(...)` |
| `withNumValues` | numValues: Int | NominalAttribute | Scala方法 | `withNumValues(...)` |
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `withName` | name: String | BinaryAttribute | Scala方法 | `withName(...)` |
| `withIndex` | index: Int | BinaryAttribute | Scala方法 | `withIndex(...)` |
| `withValues` | negative: String, positive: String | BinaryAttribute | Scala方法 | `withValues(...)` |
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `withIndex` | index: Int | Attribute | Scala方法 | `withIndex(...)` |
| `withName` | name: String | Attribute | Scala方法 | `withName(...)` |

---

## 包: org.apache.spark.ml.classification

**类数量**: 12

### LinearSVC

**完整类名**: `org.apache.spark.ml.classification.LinearSVC`

**描述**: Scala定义的Java友好接口

**方法数**: 18

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setRegParam` | value: Double | this | Scala方法 | `setRegParam(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setFitIntercept` | value: Boolean | this | Scala方法 | `setFitIntercept(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setStandardization` | value: Boolean | this | Scala方法 | `setStandardization(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setThreshold` | value: Double | this | Scala方法 | `setThreshold(...)` |
| `setAggregationDepth` | value: Int | this | Scala方法 | `setAggregationDepth(...)` |
| `setMaxBlockSizeInMB` | value: Double | this | Scala方法 | `setMaxBlockSizeInMB(...)` |
| `copy` | extra: ParamMap | LinearSVC | Scala方法 | `copy(...)` |
| `load` | path: String | LinearSVC | Scala方法 | `load(...)` |
| `setThreshold` | value: Double | this | Scala方法 | `setThreshold(...)` |
| `evaluate` | dataset: Dataset[_] | LinearSVCSummary | Scala方法 | `evaluate(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `copy` | extra: ParamMap | LinearSVCModel | Scala方法 | `copy(...)` |
| `load` | path: String | LinearSVCModel | Scala方法 | `load(...)` |

---

### MultilayerPerceptronClassifier

**完整类名**: `org.apache.spark.ml.classification.MultilayerPerceptronClassifier`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setLayers` | value: Array[Int] | this | Scala方法 | `setLayers(...)` |
| `setBlockSize` | value: Int | this | Scala方法 | `setBlockSize(...)` |
| `setSolver` | value: String | this | Scala方法 | `setSolver(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setInitialWeights` | value: Vector | this | Scala方法 | `setInitialWeights(...)` |
| `setStepSize` | value: Double | this | Scala方法 | `setStepSize(...)` |
| `copy` | extra: ParamMap | MultilayerPerceptronClassifier | Scala方法 | `copy(...)` |
| `evaluate` | dataset: Dataset[_] | MultilayerPerceptronClassificationSummary | Scala方法 | `evaluate(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `copy` | extra: ParamMap | MultilayerPerceptronClassificationModel | Scala方法 | `copy(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `load` | path: String | MultilayerPerceptronClassificationModel | Scala方法 | `load(...)` |

---

### classification

**完整类名**: `org.apache.spark.ml.classification.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setRawPredictionCol` | value: String | E | Scala方法 | `setRawPredictionCol(...)` |
| `setRawPredictionCol` | value: String | M | Scala方法 | `setRawPredictionCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformImpl` | dataset: Dataset[_] | DataFrame | Scala方法 | `transformImpl(...)` |
| `predict` | features: FeaturesType | Double | Scala方法 | `predict(...)` |
| `predictRaw` | features: FeaturesType | Vector | Scala方法 | `predictRaw(...)` |

---

### classification

**完整类名**: `org.apache.spark.ml.classification.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fMeasureByLabel` | beta: Double | Array[Double] | Scala方法 | `fMeasureByLabel(...)` |
| `weightedFMeasure` | beta: Double | Double | Scala方法 | `weightedFMeasure(...)` |

---

### classification

**完整类名**: `org.apache.spark.ml.classification.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkElement` | elem: Params, name: String | Unit | Scala方法 | `checkElement(...)` |
| `saveImpl` | path: String,
      instance: OneVsRestParams,
      spark: SparkSession,
      extraMetadata: Option[JObject] = None | Unit | Scala方法 | `saveImpl(...)` |
| `loadImpl` | path: String,
      spark: SparkSession,
      expectedClassName: String |  | Scala方法 | `loadImpl(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setRawPredictionCol` | value: String | this | Scala方法 | `setRawPredictionCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `copy` | extra: ParamMap | OneVsRestModel | Scala方法 | `copy(...)` |
| `load` | path: String | OneVsRestModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setClassifier` | value: Classifier[_, _, _] | this | Scala方法 | `setClassifier(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setRawPredictionCol` | value: String | this | Scala方法 | `setRawPredictionCol(...)` |
| `setParallelism` | value: Int | this | Scala方法 | `setParallelism(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `fit` | dataset: Dataset[_] | OneVsRestModel | Scala方法 | `fit(...)` |
| `copy` | extra: ParamMap | OneVsRest | Scala方法 | `copy(...)` |
| `load` | path: String | OneVsRest | Scala方法 | `load(...)` |

---

### classifier

**完整类名**: `org.apache.spark.ml.classification.classifier`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setProbabilityCol` | value: String | E | Scala方法 | `setProbabilityCol(...)` |
| `setThresholds` | value: Array[Double] | E | Scala方法 | `setThresholds(...)` |
| `setProbabilityCol` | value: String | M | Scala方法 | `setProbabilityCol(...)` |
| `setThresholds` | value: Array[Double] | M | Scala方法 | `setThresholds(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predictProbability` | features: FeaturesType | Vector | Scala方法 | `predictProbability(...)` |
| `normalizeToProbabilitiesInPlace` | v: DenseVector | Unit | Scala方法 | `normalizeToProbabilitiesInPlace(...)` |

---

### label

**完整类名**: `org.apache.spark.ml.classification.label`

**描述**: Scala定义的Java友好接口

**方法数**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setThreshold` | value: Double | this | Scala方法 | `setThreshold(...)` |
| `setThresholds` | value: Array[Double] | this | Scala方法 | `setThresholds(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setRegParam` | value: Double | this | Scala方法 | `setRegParam(...)` |
| `setElasticNetParam` | value: Double | this | Scala方法 | `setElasticNetParam(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setFitIntercept` | value: Boolean | this | Scala方法 | `setFitIntercept(...)` |
| `setFamily` | value: String | this | Scala方法 | `setFamily(...)` |
| `setStandardization` | value: Boolean | this | Scala方法 | `setStandardization(...)` |
| `setThreshold` | value: Double | this | Scala方法 | `setThreshold(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setThresholds` | value: Array[Double] | this | Scala方法 | `setThresholds(...)` |
| `setAggregationDepth` | value: Int | this | Scala方法 | `setAggregationDepth(...)` |
| `setLowerBoundsOnCoefficients` | value: Matrix | this | Scala方法 | `setLowerBoundsOnCoefficients(...)` |
| `setUpperBoundsOnCoefficients` | value: Matrix | this | Scala方法 | `setUpperBoundsOnCoefficients(...)` |
| `setLowerBoundsOnIntercepts` | value: Vector | this | Scala方法 | `setLowerBoundsOnIntercepts(...)` |
| `setUpperBoundsOnIntercepts` | value: Vector | this | Scala方法 | `setUpperBoundsOnIntercepts(...)` |
| `setMaxBlockSizeInMB` | value: Double | this | Scala方法 | `setMaxBlockSizeInMB(...)` |
| `copy` | extra: ParamMap | LogisticRegression | Scala方法 | `copy(...)` |
| `load` | path: String | LogisticRegression | Scala方法 | `load(...)` |
| `setThreshold` | value: Double | this | Scala方法 | `setThreshold(...)` |
| `setThresholds` | value: Array[Double] | this | Scala方法 | `setThresholds(...)` |
| `evaluate` | dataset: Dataset[_] | LogisticRegressionSummary | Scala方法 | `evaluate(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `copy` | extra: ParamMap | LogisticRegressionModel | Scala方法 | `copy(...)` |
| `load` | path: String | LogisticRegressionModel | Scala方法 | `load(...)` |

---

### labels

**完整类名**: `org.apache.spark.ml.classification.labels`

**描述**: Scala定义的Java友好接口

**方法数**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFactorSize` | value: Int | this | Scala方法 | `setFactorSize(...)` |
| `setFitIntercept` | value: Boolean | this | Scala方法 | `setFitIntercept(...)` |
| `setFitLinear` | value: Boolean | this | Scala方法 | `setFitLinear(...)` |
| `setRegParam` | value: Double | this | Scala方法 | `setRegParam(...)` |
| `setMiniBatchFraction` | value: Double | this | Scala方法 | `setMiniBatchFraction(...)` |
| `setInitStd` | value: Double | this | Scala方法 | `setInitStd(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setStepSize` | value: Double | this | Scala方法 | `setStepSize(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setSolver` | value: String | this | Scala方法 | `setSolver(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `copy` | extra: ParamMap | FMClassifier | Scala方法 | `copy(...)` |
| `estimateModelSize` | dataset: Dataset[_] | Long | Scala方法 | `estimateModelSize(...)` |
| `load` | path: String | FMClassifier | Scala方法 | `load(...)` |
| `evaluate` | dataset: Dataset[_] | FMClassificationSummary | Scala方法 | `evaluate(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `copy` | extra: ParamMap | FMClassificationModel | Scala方法 | `copy(...)` |
| `load` | path: String | FMClassificationModel | Scala方法 | `load(...)` |

---

### labels

**完整类名**: `org.apache.spark.ml.classification.labels`

**描述**: Scala定义的Java友好接口

**方法数**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMaxDepth` | value: Int | this | Scala方法 | `setMaxDepth(...)` |
| `setMaxBins` | value: Int | this | Scala方法 | `setMaxBins(...)` |
| `setMinInstancesPerNode` | value: Int | this | Scala方法 | `setMinInstancesPerNode(...)` |
| `setMinWeightFractionPerNode` | value: Double | this | Scala方法 | `setMinWeightFractionPerNode(...)` |
| `setMinInfoGain` | value: Double | this | Scala方法 | `setMinInfoGain(...)` |
| `setMaxMemoryInMB` | value: Int | this | Scala方法 | `setMaxMemoryInMB(...)` |
| `setCacheNodeIds` | value: Boolean | this | Scala方法 | `setCacheNodeIds(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setImpurity` | value: String | this | Scala方法 | `setImpurity(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `copy` | extra: ParamMap | DecisionTreeClassifier | Scala方法 | `copy(...)` |
| `load` | path: String | DecisionTreeClassifier | Scala方法 | `load(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `copy` | extra: ParamMap | DecisionTreeClassificationModel | Scala方法 | `copy(...)` |
| `load` | path: String | DecisionTreeClassificationModel | Scala方法 | `load(...)` |

---

### labels

**完整类名**: `org.apache.spark.ml.classification.labels`

**描述**: Scala定义的Java友好接口

**方法数**: 24

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMaxDepth` | value: Int | this | Scala方法 | `setMaxDepth(...)` |
| `setMaxBins` | value: Int | this | Scala方法 | `setMaxBins(...)` |
| `setMinInstancesPerNode` | value: Int | this | Scala方法 | `setMinInstancesPerNode(...)` |
| `setMinWeightFractionPerNode` | value: Double | this | Scala方法 | `setMinWeightFractionPerNode(...)` |
| `setMinInfoGain` | value: Double | this | Scala方法 | `setMinInfoGain(...)` |
| `setMaxMemoryInMB` | value: Int | this | Scala方法 | `setMaxMemoryInMB(...)` |
| `setCacheNodeIds` | value: Boolean | this | Scala方法 | `setCacheNodeIds(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setImpurity` | value: String | this | Scala方法 | `setImpurity(...)` |
| `setSubsamplingRate` | value: Double | this | Scala方法 | `setSubsamplingRate(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setNumTrees` | value: Int | this | Scala方法 | `setNumTrees(...)` |
| `setBootstrap` | value: Boolean | this | Scala方法 | `setBootstrap(...)` |
| `setFeatureSubsetStrategy` | value: String | this | Scala方法 | `setFeatureSubsetStrategy(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `copy` | extra: ParamMap | RandomForestClassifier | Scala方法 | `copy(...)` |
| `load` | path: String | RandomForestClassifier | Scala方法 | `load(...)` |
| `evaluate` | dataset: Dataset[_] | RandomForestClassificationSummary | Scala方法 | `evaluate(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `copy` | extra: ParamMap | RandomForestClassificationModel | Scala方法 | `copy(...)` |
| `load` | path: String | RandomForestClassificationModel | Scala方法 | `load(...)` |

---

### labels

**完整类名**: `org.apache.spark.ml.classification.labels`

**描述**: Scala定义的Java友好接口

**方法数**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMaxDepth` | value: Int | this | Scala方法 | `setMaxDepth(...)` |
| `setMaxBins` | value: Int | this | Scala方法 | `setMaxBins(...)` |
| `setMinInstancesPerNode` | value: Int | this | Scala方法 | `setMinInstancesPerNode(...)` |
| `setMinWeightFractionPerNode` | value: Double | this | Scala方法 | `setMinWeightFractionPerNode(...)` |
| `setMinInfoGain` | value: Double | this | Scala方法 | `setMinInfoGain(...)` |
| `setMaxMemoryInMB` | value: Int | this | Scala方法 | `setMaxMemoryInMB(...)` |
| `setCacheNodeIds` | value: Boolean | this | Scala方法 | `setCacheNodeIds(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setImpurity` | value: String | this | Scala方法 | `setImpurity(...)` |
| `setSubsamplingRate` | value: Double | this | Scala方法 | `setSubsamplingRate(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setStepSize` | value: Double | this | Scala方法 | `setStepSize(...)` |
| `setFeatureSubsetStrategy` | value: String | this | Scala方法 | `setFeatureSubsetStrategy(...)` |
| `setLossType` | value: String | this | Scala方法 | `setLossType(...)` |
| `setValidationIndicatorCol` | value: String | this | Scala方法 | `setValidationIndicatorCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `copy` | extra: ParamMap | GBTClassifier | Scala方法 | `copy(...)` |
| `load` | path: String | GBTClassifier | Scala方法 | `load(...)` |
| `this` | uid: String, _trees: Array[DecisionTreeRegressionModel], _treeWeights: Array[Double] | Unit | Scala方法 | `this(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `copy` | extra: ParamMap | GBTClassificationModel | Scala方法 | `copy(...)` |
| `evaluateEachIteration` | dataset: Dataset[_] | Array[Double] | Scala方法 | `evaluateEachIteration(...)` |
| `load` | path: String | GBTClassificationModel | Scala方法 | `load(...)` |

---

### to

**完整类名**: `org.apache.spark.ml.classification.to`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setSmoothing` | value: Double | this | Scala方法 | `setSmoothing(...)` |
| `setModelType` | value: String | this | Scala方法 | `setModelType(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `copy` | extra: ParamMap | NaiveBayes | Scala方法 | `copy(...)` |
| `load` | path: String | NaiveBayes | Scala方法 | `load(...)` |
| `predictRaw` | features: Vector | Vector | Scala方法 | `predictRaw(...)` |
| `copy` | extra: ParamMap | NaiveBayesModel | Scala方法 | `copy(...)` |
| `load` | path: String | NaiveBayesModel | Scala方法 | `load(...)` |

---

## 包: org.apache.spark.ml.clustering

**类数量**: 5

### BisectingKMeansModel

**完整类名**: `org.apache.spark.ml.clustering.BisectingKMeansModel`

**描述**: Scala定义的Java友好接口

**方法数**: 21

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `copy` | extra: ParamMap | BisectingKMeansModel | Scala方法 | `copy(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `predict` | features: Vector | Int | Scala方法 | `predict(...)` |
| `computeCost` | dataset: Dataset[_] | Double | Scala方法 | `computeCost(...)` |
| `load` | path: String | BisectingKMeansModel | Scala方法 | `load(...)` |
| `copy` | extra: ParamMap | BisectingKMeans | Scala方法 | `copy(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setK` | value: Int | this | Scala方法 | `setK(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setMinDivisibleClusterSize` | value: Double | this | Scala方法 | `setMinDivisibleClusterSize(...)` |
| `setDistanceMeasure` | value: String | this | Scala方法 | `setDistanceMeasure(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `fit` | dataset: Dataset[_] | BisectingKMeansModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | BisectingKMeans | Scala方法 | `load(...)` |

---

### GaussianMixtureModel

**完整类名**: `org.apache.spark.ml.clustering.GaussianMixtureModel`

**描述**: Scala定义的Java友好接口

**方法数**: 23

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setProbabilityCol` | value: String | this | Scala方法 | `setProbabilityCol(...)` |
| `copy` | extra: ParamMap | GaussianMixtureModel | Scala方法 | `copy(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `predict` | features: Vector | Int | Scala方法 | `predict(...)` |
| `predictProbability` | features: Vector | Vector | Scala方法 | `predictProbability(...)` |
| `load` | path: String | GaussianMixtureModel | Scala方法 | `load(...)` |
| `copy` | extra: ParamMap | GaussianMixture | Scala方法 | `copy(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setProbabilityCol` | value: String | this | Scala方法 | `setProbabilityCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setK` | value: Int | this | Scala方法 | `setK(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setAggregationDepth` | value: Int | this | Scala方法 | `setAggregationDepth(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | GaussianMixture | Scala方法 | `load(...)` |
| `add` | instance: (Vector, Double | Unit | Scala方法 | `add(...)` |

---

### KMeansModel

**完整类名**: `org.apache.spark.ml.clustering.KMeansModel`

**描述**: Scala定义的Java友好接口

**方法数**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `copy` | extra: ParamMap | KMeansModel | Scala方法 | `copy(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `predict` | features: Vector | Int | Scala方法 | `predict(...)` |
| `stageName` | 无 | String | Scala方法 | `stageName(...)` |
| `write` | path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage | Unit | Scala方法 | `write(...)` |
| `stageName` | 无 | String | Scala方法 | `stageName(...)` |
| `write` | path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage | Unit | Scala方法 | `write(...)` |
| `load` | path: String | KMeansModel | Scala方法 | `load(...)` |
| `copy` | extra: ParamMap | KMeans | Scala方法 | `copy(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setK` | value: Int | this | Scala方法 | `setK(...)` |
| `setInitMode` | value: String | this | Scala方法 | `setInitMode(...)` |
| `setDistanceMeasure` | value: String | this | Scala方法 | `setDistanceMeasure(...)` |
| `setInitSteps` | value: Int | this | Scala方法 | `setInitSteps(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setSolver` | value: String | this | Scala方法 | `setSolver(...)` |
| `setMaxBlockSizeInMB` | value: Double | this | Scala方法 | `setMaxBlockSizeInMB(...)` |
| `fit` | dataset: Dataset[_] | KMeansModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | KMeans | Scala方法 | `load(...)` |

---

### LDAParams

**完整类名**: `org.apache.spark.ml.clustering.LDAParams`

**描述**: Scala定义的Java友好接口

**方法数**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getAndSetParams` | model: LDAParams, metadata: Metadata | Unit | Scala方法 | `getAndSetParams(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setTopicDistributionCol` | value: String | this | Scala方法 | `setTopicDistributionCol(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `logLikelihood` | dataset: Dataset[_] | Double | Scala方法 | `logLikelihood(...)` |
| `logPerplexity` | dataset: Dataset[_] | Double | Scala方法 | `logPerplexity(...)` |
| `describeTopics` | maxTermsPerTopic: Int | DataFrame | Scala方法 | `describeTopics(...)` |
| `describeTopics` | 无 | DataFrame | Scala方法 | `describeTopics(...)` |
| `copy` | extra: ParamMap | LocalLDAModel | Scala方法 | `copy(...)` |
| `load` | path: String | LocalLDAModel | Scala方法 | `load(...)` |
| `copy` | extra: ParamMap | DistributedLDAModel | Scala方法 | `copy(...)` |
| `deleteCheckpointFiles` | 无 | Unit | Scala方法 | `deleteCheckpointFiles(...)` |
| `load` | path: String | DistributedLDAModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setK` | value: Int | this | Scala方法 | `setK(...)` |
| `setDocConcentration` | value: Array[Double] | this | Scala方法 | `setDocConcentration(...)` |
| `setDocConcentration` | value: Double | this | Scala方法 | `setDocConcentration(...)` |
| `setTopicConcentration` | value: Double | this | Scala方法 | `setTopicConcentration(...)` |
| `setOptimizer` | value: String | this | Scala方法 | `setOptimizer(...)` |
| `setTopicDistributionCol` | value: String | this | Scala方法 | `setTopicDistributionCol(...)` |
| `setLearningOffset` | value: Double | this | Scala方法 | `setLearningOffset(...)` |
| `setLearningDecay` | value: Double | this | Scala方法 | `setLearningDecay(...)` |
| `setSubsamplingRate` | value: Double | this | Scala方法 | `setSubsamplingRate(...)` |
| `setOptimizeDocConcentration` | value: Boolean | this | Scala方法 | `setOptimizeDocConcentration(...)` |
| `setKeepLastCheckpoint` | value: Boolean | this | Scala方法 | `setKeepLastCheckpoint(...)` |
| `copy` | extra: ParamMap | LDA | Scala方法 | `copy(...)` |
| `fit` | dataset: Dataset[_] | LDAModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | LDA | Scala方法 | `load(...)` |

---

### is

**完整类名**: `org.apache.spark.ml.clustering.is`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setK` | value: Int | this | Scala方法 | `setK(...)` |
| `setInitMode` | value: String | this | Scala方法 | `setInitMode(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setSrcCol` | value: String | this | Scala方法 | `setSrcCol(...)` |
| `setDstCol` | value: String | this | Scala方法 | `setDstCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `assignClusters` | dataset: Dataset[_] | DataFrame | Scala方法 | `assignClusters(...)` |
| `copy` | extra: ParamMap | PowerIterationClustering | Scala方法 | `copy(...)` |
| `load` | path: String | PowerIterationClustering | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | PowerIterationClusteringWrapper | Scala方法 | `copy(...)` |
| `load` | path: String | PowerIterationClusteringWrapper | Scala方法 | `load(...)` |

---

## 包: org.apache.spark.ml.evaluation

**类数量**: 8

### BinaryClassificationEvaluator

**完整类名**: `org.apache.spark.ml.evaluation.BinaryClassificationEvaluator`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMetricName` | value: String | this | Scala方法 | `setMetricName(...)` |
| `setNumBins` | value: Int | this | Scala方法 | `setNumBins(...)` |
| `setRawPredictionCol` | value: String | this | Scala方法 | `setRawPredictionCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `evaluate` | dataset: Dataset[_] | Double | Scala方法 | `evaluate(...)` |
| `getMetrics` | dataset: Dataset[_] | BinaryClassificationMetrics | Scala方法 | `getMetrics(...)` |
| `copy` | extra: ParamMap | BinaryClassificationEvaluator | Scala方法 | `copy(...)` |
| `load` | path: String | BinaryClassificationEvaluator | Scala方法 | `load(...)` |

---

### ClusteringEvaluator

**完整类名**: `org.apache.spark.ml.evaluation.ClusteringEvaluator`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `copy` | pMap: ParamMap | ClusteringEvaluator | Scala方法 | `copy(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setMetricName` | value: String | this | Scala方法 | `setMetricName(...)` |
| `setDistanceMeasure` | value: String | this | Scala方法 | `setDistanceMeasure(...)` |
| `evaluate` | dataset: Dataset[_] | Double | Scala方法 | `evaluate(...)` |
| `getMetrics` | dataset: Dataset[_] | ClusteringMetrics | Scala方法 | `getMetrics(...)` |
| `load` | path: String | ClusteringEvaluator | Scala方法 | `load(...)` |

---

### ClusteringMetrics

**完整类名**: `org.apache.spark.ml.evaluation.ClusteringMetrics`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setDistanceMeasure` | value: String | this | Scala方法 | `setDistanceMeasure(...)` |
| `silhouette` | 无 | Double | Scala方法 | `silhouette(...)` |
| `pointSilhouetteCoefficient` | clusterIds: Set[Double],
      pointClusterId: Double,
      weightSum: Double,
      weight: Double,
      averageDistanceToCluster: (Double | Unit | Scala方法 | `pointSilhouetteCoefficient(...)` |
| `overallScore` | df: DataFrame, scoreColumn: Column, weightColumn: Column | Double | Scala方法 | `overallScore(...)` |
| `registerKryoClasses` | sc: SparkContext | Unit | Scala方法 | `registerKryoClasses(...)` |
| `computeClusterStats` | df: DataFrame,
      predictionCol: String,
      featuresCol: String,
      weightCol: String | Map[Double, ClusterStats] | Scala方法 | `computeClusterStats(...)` |
| `computeSilhouetteCoefficient` | broadcastedClustersMap: Broadcast[Map[Double, ClusterStats]],
      point: Vector,
      clusterId: Double,
      weight: Double,
      squaredNorm: Double | Double | Scala方法 | `computeSilhouetteCoefficient(...)` |
| `compute` | targetClusterId: Double | Double | Scala方法 | `compute(...)` |
| `computeSilhouetteScore` | dataset: Dataset[_],
      predictionCol: String,
      featuresCol: String,
      weightCol: String | Double | Scala方法 | `computeSilhouetteScore(...)` |
| `computeClusterStats` | df: DataFrame,
      featuresCol: String,
      predictionCol: String,
      weightCol: String | Map[Double, | Scala方法 | `computeClusterStats(...)` |
| `computeSilhouetteCoefficient` | broadcastedClustersMap: Broadcast[Map[Double, (Vector, Double | Unit | Scala方法 | `computeSilhouetteCoefficient(...)` |
| `compute` | targetClusterId: Double | Double | Scala方法 | `compute(...)` |
| `computeSilhouetteScore` | dataset: Dataset[_],
      predictionCol: String,
      featuresCol: String,
      weightCol: String | Double | Scala方法 | `computeSilhouetteScore(...)` |

---

### MultilabelClassificationEvaluator

**完整类名**: `org.apache.spark.ml.evaluation.MultilabelClassificationEvaluator`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMetricName` | value: String | this | Scala方法 | `setMetricName(...)` |
| `setMetricLabel` | value: Double | this | Scala方法 | `setMetricLabel(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `evaluate` | dataset: Dataset[_] | Double | Scala方法 | `evaluate(...)` |
| `getMetrics` | dataset: Dataset[_] | MultilabelMetrics | Scala方法 | `getMetrics(...)` |
| `copy` | extra: ParamMap | MultilabelClassificationEvaluator | Scala方法 | `copy(...)` |
| `load` | path: String | MultilabelClassificationEvaluator | Scala方法 | `load(...)` |

---

### RankingEvaluator

**完整类名**: `org.apache.spark.ml.evaluation.RankingEvaluator`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMetricName` | value: String | this | Scala方法 | `setMetricName(...)` |
| `setK` | value: Int | this | Scala方法 | `setK(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `evaluate` | dataset: Dataset[_] | Double | Scala方法 | `evaluate(...)` |
| `getMetrics` | dataset: Dataset[_] | RankingMetrics[Double] | Scala方法 | `getMetrics(...)` |
| `copy` | extra: ParamMap | RankingEvaluator | Scala方法 | `copy(...)` |
| `load` | path: String | RankingEvaluator | Scala方法 | `load(...)` |

---

### RegressionEvaluator

**完整类名**: `org.apache.spark.ml.evaluation.RegressionEvaluator`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMetricName` | value: String | this | Scala方法 | `setMetricName(...)` |
| `setThroughOrigin` | value: Boolean | this | Scala方法 | `setThroughOrigin(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `evaluate` | dataset: Dataset[_] | Double | Scala方法 | `evaluate(...)` |
| `getMetrics` | dataset: Dataset[_] | RegressionMetrics | Scala方法 | `getMetrics(...)` |
| `copy` | extra: ParamMap | RegressionEvaluator | Scala方法 | `copy(...)` |
| `load` | path: String | RegressionEvaluator | Scala方法 | `load(...)` |

---

### classification

**完整类名**: `org.apache.spark.ml.evaluation.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMetricName` | value: String | this | Scala方法 | `setMetricName(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setProbabilityCol` | value: String | this | Scala方法 | `setProbabilityCol(...)` |
| `setMetricLabel` | value: Double | this | Scala方法 | `setMetricLabel(...)` |
| `setBeta` | value: Double | this | Scala方法 | `setBeta(...)` |
| `setEps` | value: Double | this | Scala方法 | `setEps(...)` |
| `evaluate` | dataset: Dataset[_] | Double | Scala方法 | `evaluate(...)` |
| `getMetrics` | dataset: Dataset[_] | MulticlassMetrics | Scala方法 | `getMetrics(...)` |
| `copy` | extra: ParamMap | MulticlassClassificationEvaluator | Scala方法 | `copy(...)` |
| `load` | path: String | MulticlassClassificationEvaluator | Scala方法 | `load(...)` |

---

### for

**完整类名**: `org.apache.spark.ml.evaluation.for`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `evaluate` | dataset: Dataset[_], paramMap: ParamMap | Double | Scala方法 | `evaluate(...)` |
| `evaluate` | dataset: Dataset[_] | Double | Scala方法 | `evaluate(...)` |
| `copy` | extra: ParamMap | Evaluator | Scala方法 | `copy(...)` |

---

## 包: org.apache.spark.ml.feature

**类数量**: 40

### Binarizer

**完整类名**: `org.apache.spark.ml.feature.Binarizer`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setThreshold` | value: Double | this | Scala方法 | `setThreshold(...)` |
| `setThresholds` | value: Array[Double] | this | Scala方法 | `setThresholds(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | Binarizer | Scala方法 | `copy(...)` |
| `load` | path: String | Binarizer | Scala方法 | `load(...)` |

---

### BucketedRandomProjectionLSHModel

**完整类名**: `org.apache.spark.ml.feature.BucketedRandomProjectionLSHModel`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `copy` | extra: ParamMap | BucketedRandomProjectionLSHModel | Scala方法 | `copy(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setNumHashTables` | value: Int | this | Scala方法 | `setNumHashTables(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setBucketLength` | value: Double | this | Scala方法 | `setBucketLength(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | this | Scala方法 | `copy(...)` |
| `load` | path: String | BucketedRandomProjectionLSH | Scala方法 | `load(...)` |
| `load` | path: String | BucketedRandomProjectionLSHModel | Scala方法 | `load(...)` |

---

### Bucketizer

**完整类名**: `org.apache.spark.ml.feature.Bucketizer`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setSplits` | value: Array[Double] | this | Scala方法 | `setSplits(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `setSplitsArray` | value: Array[Array[Double]] | this | Scala方法 | `setSplitsArray(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | Bucketizer | Scala方法 | `copy(...)` |
| `load` | path: String | Bucketizer | Scala方法 | `load(...)` |

---

### ChiSqSelector

**完整类名**: `org.apache.spark.ml.feature.ChiSqSelector`

**描述**: Scala定义的Java友好接口

**方法数**: 18

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setNumTopFeatures` | value: Int | this | Scala方法 | `setNumTopFeatures(...)` |
| `setPercentile` | value: Double | this | Scala方法 | `setPercentile(...)` |
| `setFpr` | value: Double | this | Scala方法 | `setFpr(...)` |
| `setFdr` | value: Double | this | Scala方法 | `setFdr(...)` |
| `setFwe` | value: Double | this | Scala方法 | `setFwe(...)` |
| `setSelectorType` | value: String | this | Scala方法 | `setSelectorType(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `fit` | dataset: Dataset[_] | ChiSqSelectorModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | ChiSqSelector | Scala方法 | `copy(...)` |
| `load` | path: String | ChiSqSelector | Scala方法 | `load(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | ChiSqSelectorModel | Scala方法 | `copy(...)` |
| `load` | path: String | ChiSqSelectorModel | Scala方法 | `load(...)` |

---

### CountVectorizer

**完整类名**: `org.apache.spark.ml.feature.CountVectorizer`

**描述**: Scala定义的Java友好接口

**方法数**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setVocabSize` | value: Int | this | Scala方法 | `setVocabSize(...)` |
| `setMinDF` | value: Double | this | Scala方法 | `setMinDF(...)` |
| `setMaxDF` | value: Double | this | Scala方法 | `setMaxDF(...)` |
| `setMinTF` | value: Double | this | Scala方法 | `setMinTF(...)` |
| `setBinary` | value: Boolean | this | Scala方法 | `setBinary(...)` |
| `fit` | dataset: Dataset[_] | CountVectorizerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | CountVectorizer | Scala方法 | `copy(...)` |
| `load` | path: String | CountVectorizer | Scala方法 | `load(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setMinTF` | value: Double | this | Scala方法 | `setMinTF(...)` |
| `setBinary` | value: Boolean | this | Scala方法 | `setBinary(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | CountVectorizerModel | Scala方法 | `copy(...)` |
| `load` | path: String | CountVectorizerModel | Scala方法 | `load(...)` |

---

### DCT

**完整类名**: `org.apache.spark.ml.feature.DCT`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInverse` | value: Boolean | this | Scala方法 | `setInverse(...)` |
| `load` | path: String | DCT | Scala方法 | `load(...)` |

---

### ElementwiseProduct

**完整类名**: `org.apache.spark.ml.feature.ElementwiseProduct`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setScalingVec` | value: Vector | this | Scala方法 | `setScalingVec(...)` |
| `load` | path: String | ElementwiseProduct | Scala方法 | `load(...)` |

---

### FeatureHasher

**完整类名**: `org.apache.spark.ml.feature.FeatureHasher`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setNumFeatures` | value: Int | this | Scala方法 | `setNumFeatures(...)` |
| `setInputCols` | values: String* | this | Scala方法 | `setInputCols(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setCategoricalCols` | value: Array[String] | this | Scala方法 | `setCategoricalCols(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `getDouble` | x: Any | Double | Scala方法 | `getDouble(...)` |
| `copy` | extra: ParamMap | FeatureHasher | Scala方法 | `copy(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | FeatureHasher | Scala方法 | `load(...)` |

---

### HashingTF

**完整类名**: `org.apache.spark.ml.feature.HashingTF`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `this` | uid: String | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setNumFeatures` | value: Int | this | Scala方法 | `setNumFeatures(...)` |
| `setBinary` | value: Boolean | this | Scala方法 | `setBinary(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `indexOf` | term: Any | Int | Scala方法 | `indexOf(...)` |
| `copy` | extra: ParamMap | HashingTF | Scala方法 | `copy(...)` |
| `save` | path: String | Unit | Scala方法 | `save(...)` |
| `load` | path: String | HashingTF | Scala方法 | `load(...)` |

---

### IDF

**完整类名**: `org.apache.spark.ml.feature.IDF`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setMinDocFreq` | value: Int | this | Scala方法 | `setMinDocFreq(...)` |
| `fit` | dataset: Dataset[_] | IDFModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | IDF | Scala方法 | `copy(...)` |
| `load` | path: String | IDF | Scala方法 | `load(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | IDFModel | Scala方法 | `copy(...)` |
| `load` | path: String | IDFModel | Scala方法 | `load(...)` |

---

### Imputer

**完整类名**: `org.apache.spark.ml.feature.Imputer`

**描述**: Scala定义的Java友好接口

**方法数**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `setStrategy` | value: String | this | Scala方法 | `setStrategy(...)` |
| `setMissingValue` | value: Double | this | Scala方法 | `setMissingValue(...)` |
| `setRelativeError` | value: Double | this | Scala方法 | `setRelativeError(...)` |
| `fit` | dataset: Dataset[_] | ImputerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | Imputer | Scala方法 | `copy(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | ImputerModel | Scala方法 | `copy(...)` |
| `load` | path: String | ImputerModel | Scala方法 | `load(...)` |

---

### Instance

**完整类名**: `org.apache.spark.ml.feature.Instance`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getLabel` | i: Int | Double | Scala方法 | `getLabel(...)` |
| `fromInstances` | instances: Seq[Instance] | InstanceBlock | Scala方法 | `fromInstances(...)` |
| `blokify` | instances: RDD[Instance], blockSize: Int | RDD[InstanceBlock] | Scala方法 | `blokify(...)` |
| `blokifyWithMaxMemUsage` | instanceIterator: Iterator[Instance],
      maxMemUsage: Long | Iterator[InstanceBlock] | Scala方法 | `blokifyWithMaxMemUsage(...)` |
| `next` | 无 | InstanceBlock | Scala方法 | `next(...)` |
| `blokifyWithMaxMemUsage` | instances: RDD[Instance],
      maxMemUsage: Long | RDD[InstanceBlock] | Scala方法 | `blokifyWithMaxMemUsage(...)` |

---

### Interaction

**完整类名**: `org.apache.spark.ml.feature.Interaction`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCols` | values: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `format` | index: Int,
        attrName: Option[String],
        categoryName: Option[String] | String | Scala方法 | `format(...)` |
| `copy` | extra: ParamMap | Interaction | Scala方法 | `copy(...)` |
| `load` | path: String | Interaction | Scala方法 | `load(...)` |
| `foreachNonzeroOutput` | value: Any, f: (Int, Double | Unit | Scala方法 | `foreachNonzeroOutput(...)` |

---

### LSHModel

**完整类名**: `org.apache.spark.ml.feature.LSHModel`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `sameBucket` | x: Array[Vector], y: Array[Vector] | Boolean | Scala方法 | `sameBucket(...)` |
| `approxNearestNeighbors` | dataset: Dataset[_],
      key: Vector,
      numNearestNeighbors: Int,
      distCol: String | Dataset[_] | Scala方法 | `approxNearestNeighbors(...)` |
| `approxNearestNeighbors` | dataset: Dataset[_],
      key: Vector,
      numNearestNeighbors: Int | Dataset[_] | Scala方法 | `approxNearestNeighbors(...)` |
| `approxSimilarityJoin` | datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double,
      distCol: String | Dataset[_] | Scala方法 | `approxSimilarityJoin(...)` |
| `approxSimilarityJoin` | datasetA: Dataset[_],
      datasetB: Dataset[_],
      threshold: Double | Dataset[_] | Scala方法 | `approxSimilarityJoin(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setNumHashTables` | value: Int | this | Scala方法 | `setNumHashTables(...)` |

---

### MaxAbsScaler

**完整类名**: `org.apache.spark.ml.feature.MaxAbsScaler`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `fit` | dataset: Dataset[_] | MaxAbsScalerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | MaxAbsScaler | Scala方法 | `copy(...)` |
| `load` | path: String | MaxAbsScaler | Scala方法 | `load(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | MaxAbsScalerModel | Scala方法 | `copy(...)` |
| `load` | path: String | MaxAbsScalerModel | Scala方法 | `load(...)` |

---

### MinHashLSHModel

**完整类名**: `org.apache.spark.ml.feature.MinHashLSHModel`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `copy` | extra: ParamMap | MinHashLSHModel | Scala方法 | `copy(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setNumHashTables` | value: Int | this | Scala方法 | `setNumHashTables(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | this | Scala方法 | `copy(...)` |
| `load` | path: String | MinHashLSHModel | Scala方法 | `load(...)` |

---

### MinMaxScaler

**完整类名**: `org.apache.spark.ml.feature.MinMaxScaler`

**描述**: Scala定义的Java友好接口

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setMin` | value: Double | this | Scala方法 | `setMin(...)` |
| `setMax` | value: Double | this | Scala方法 | `setMax(...)` |
| `fit` | dataset: Dataset[_] | MinMaxScalerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | MinMaxScaler | Scala方法 | `copy(...)` |
| `load` | path: String | MinMaxScaler | Scala方法 | `load(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setMin` | value: Double | this | Scala方法 | `setMin(...)` |
| `setMax` | value: Double | this | Scala方法 | `setMax(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | MinMaxScalerModel | Scala方法 | `copy(...)` |
| `load` | path: String | MinMaxScalerModel | Scala方法 | `load(...)` |

---

### NGram

**完整类名**: `org.apache.spark.ml.feature.NGram`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setN` | value: Int | this | Scala方法 | `setN(...)` |
| `load` | path: String | NGram | Scala方法 | `load(...)` |

---

### Normalizer

**完整类名**: `org.apache.spark.ml.feature.Normalizer`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setP` | value: Double | this | Scala方法 | `setP(...)` |
| `load` | path: String | Normalizer | Scala方法 | `load(...)` |

---

### OneHotEncoder

**完整类名**: `org.apache.spark.ml.feature.OneHotEncoder`

**描述**: Scala定义的Java友好接口

**方法数**: 24

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | values: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | values: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `setDropLast` | value: Boolean | this | Scala方法 | `setDropLast(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `fit` | dataset: Dataset[_] | OneHotEncoderModel | Scala方法 | `fit(...)` |
| `copy` | extra: ParamMap | OneHotEncoder | Scala方法 | `copy(...)` |
| `load` | path: String | OneHotEncoder | Scala方法 | `load(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | values: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | values: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `setDropLast` | value: Boolean | this | Scala方法 | `setDropLast(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `copy` | extra: ParamMap | OneHotEncoderModel | Scala方法 | `copy(...)` |
| `load` | path: String | OneHotEncoderModel | Scala方法 | `load(...)` |
| `transformOutputColumnSchema` | inputCol: StructField,
      outputColName: String,
      dropLast: Boolean,
      keepInvalid: Boolean = false | StructField | Scala方法 | `transformOutputColumnSchema(...)` |
| `getOutputAttrGroupFromData` | dataset: Dataset[_],
      inputColNames: Seq[String],
      outputColNames: Seq[String],
      dropLast: Boolean | Seq[AttributeGroup] | Scala方法 | `getOutputAttrGroupFromData(...)` |
| `createAttrGroupForAttrNames` | outputColName: String,
      numAttrs: Int,
      dropLast: Boolean,
      keepInvalid: Boolean | AttributeGroup | Scala方法 | `createAttrGroupForAttrNames(...)` |

---

### PCA

**完整类名**: `org.apache.spark.ml.feature.PCA`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setK` | value: Int | this | Scala方法 | `setK(...)` |
| `fit` | dataset: Dataset[_] | PCAModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | PCA | Scala方法 | `copy(...)` |
| `load` | path: String | PCA | Scala方法 | `load(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | PCAModel | Scala方法 | `copy(...)` |
| `load` | path: String | PCAModel | Scala方法 | `load(...)` |
| `load` | path: String | PCAModel | Scala方法 | `load(...)` |

---

### ParsedRFormula

**完整类名**: `org.apache.spark.ml.feature.ParsedRFormula`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `resolve` | schema: StructType | ResolvedRFormula | Scala方法 | `resolve(...)` |
| `add` | other: Term | Term | Scala方法 | `add(...)` |
| `subtract` | other: Term | Term | Scala方法 | `subtract(...)` |
| `interact` | other: Term | Term | Scala方法 | `interact(...)` |
| `interact` | other: Term | Term | Scala方法 | `interact(...)` |
| `interact` | other: Term | Term | Scala方法 | `interact(...)` |
| `interact` | other: Term | Term | Scala方法 | `interact(...)` |
| `interact` | other: Term | Term | Scala方法 | `interact(...)` |
| `parse` | value: String | ParsedRFormula | Scala方法 | `parse(...)` |

---

### PolynomialExpansion

**完整类名**: `org.apache.spark.ml.feature.PolynomialExpansion`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setDegree` | value: Int | this | Scala方法 | `setDegree(...)` |
| `load` | path: String | PolynomialExpansion | Scala方法 | `load(...)` |

---

### QuantileDiscretizer

**完整类名**: `org.apache.spark.ml.feature.QuantileDiscretizer`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setRelativeError` | value: Double | this | Scala方法 | `setRelativeError(...)` |
| `setNumBuckets` | value: Int | this | Scala方法 | `setNumBuckets(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `setNumBucketsArray` | value: Array[Int] | this | Scala方法 | `setNumBucketsArray(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `fit` | dataset: Dataset[_] | Bucketizer | Scala方法 | `fit(...)` |
| `copy` | extra: ParamMap | QuantileDiscretizer | Scala方法 | `copy(...)` |
| `load` | path: String | QuantileDiscretizer | Scala方法 | `load(...)` |

---

### RFormula

**完整类名**: `org.apache.spark.ml.feature.RFormula`

**描述**: Scala定义的Java友好接口

**方法数**: 25

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFormula` | value: String | this | Scala方法 | `setFormula(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `setForceIndexLabel` | value: Boolean | this | Scala方法 | `setForceIndexLabel(...)` |
| `setStringIndexerOrderType` | value: String | this | Scala方法 | `setStringIndexerOrderType(...)` |
| `fit` | dataset: Dataset[_] | RFormulaModel | Scala方法 | `fit(...)` |
| `tmpColumn` | category: String | String | Scala方法 | `tmpColumn(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | RFormula | Scala方法 | `copy(...)` |
| `load` | path: String | RFormula | Scala方法 | `load(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | RFormulaModel | Scala方法 | `copy(...)` |
| `load` | path: String | RFormulaModel | Scala方法 | `load(...)` |
| `this` | columnsToPrune: Set[String] | Unit | Scala方法 | `this(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | ColumnPruner | Scala方法 | `copy(...)` |
| `load` | path: String | ColumnPruner | Scala方法 | `load(...)` |
| `this` | vectorCol: String, prefixesToRewrite: Map[String, String] | Unit | Scala方法 | `this(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | VectorAttributeRewriter | Scala方法 | `copy(...)` |
| `load` | path: String | VectorAttributeRewriter | Scala方法 | `load(...)` |

---

### RobustScaler

**完整类名**: `org.apache.spark.ml.feature.RobustScaler`

**描述**: Scala定义的Java友好接口

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setLower` | value: Double | this | Scala方法 | `setLower(...)` |
| `setUpper` | value: Double | this | Scala方法 | `setUpper(...)` |
| `setWithCentering` | value: Boolean | this | Scala方法 | `setWithCentering(...)` |
| `setWithScaling` | value: Boolean | this | Scala方法 | `setWithScaling(...)` |
| `setRelativeError` | value: Double | this | Scala方法 | `setRelativeError(...)` |
| `fit` | dataset: Dataset[_] | RobustScalerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | RobustScaler | Scala方法 | `copy(...)` |
| `load` | path: String | RobustScaler | Scala方法 | `load(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | RobustScalerModel | Scala方法 | `copy(...)` |
| `load` | path: String | RobustScalerModel | Scala方法 | `load(...)` |

---

### SQLTransformer

**完整类名**: `org.apache.spark.ml.feature.SQLTransformer`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setStatement` | value: String | this | Scala方法 | `setStatement(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | SQLTransformer | Scala方法 | `copy(...)` |
| `load` | path: String | SQLTransformer | Scala方法 | `load(...)` |

---

### StandardScaler

**完整类名**: `org.apache.spark.ml.feature.StandardScaler`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setWithMean` | value: Boolean | this | Scala方法 | `setWithMean(...)` |
| `setWithStd` | value: Boolean | this | Scala方法 | `setWithStd(...)` |
| `fit` | dataset: Dataset[_] | StandardScalerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | StandardScaler | Scala方法 | `copy(...)` |
| `load` | path: String | StandardScaler | Scala方法 | `load(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | StandardScalerModel | Scala方法 | `copy(...)` |
| `load` | path: String | StandardScalerModel | Scala方法 | `load(...)` |

---

### StopWordsRemover

**完整类名**: `org.apache.spark.ml.feature.StopWordsRemover`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `setStopWords` | value: Array[String] | this | Scala方法 | `setStopWords(...)` |
| `setCaseSensitive` | value: Boolean | this | Scala方法 | `setCaseSensitive(...)` |
| `setLocale` | value: String | this | Scala方法 | `setLocale(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | StopWordsRemover | Scala方法 | `copy(...)` |
| `load` | path: String | StopWordsRemover | Scala方法 | `load(...)` |
| `loadDefaultStopWords` | language: String | Array[String] | Scala方法 | `loadDefaultStopWords(...)` |

---

### StringIndexer

**完整类名**: `org.apache.spark.ml.feature.StringIndexer`

**描述**: Scala定义的Java友好接口

**方法数**: 31

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `setStringOrderType` | value: String | this | Scala方法 | `setStringOrderType(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `fit` | dataset: Dataset[_] | StringIndexerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | StringIndexer | Scala方法 | `copy(...)` |
| `load` | path: String | StringIndexer | Scala方法 | `load(...)` |
| `this` | uid: String, labels: Array[String] | Unit | Scala方法 | `this(...)` |
| `this` | labels: Array[String] | Unit | Scala方法 | `this(...)` |
| `this` | labelsArray: Array[Array[String]] | Unit | Scala方法 | `this(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | value: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | StringIndexerModel | Scala方法 | `copy(...)` |
| `load` | path: String | StringIndexerModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setLabels` | value: Array[String] | this | Scala方法 | `setLabels(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `copy` | extra: ParamMap | IndexToString | Scala方法 | `copy(...)` |
| `load` | path: String | IndexToString | Scala方法 | `load(...)` |

---

### Tokenizer

**完整类名**: `org.apache.spark.ml.feature.Tokenizer`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `copy` | extra: ParamMap | Tokenizer | Scala方法 | `copy(...)` |
| `load` | path: String | Tokenizer | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMinTokenLength` | value: Int | this | Scala方法 | `setMinTokenLength(...)` |
| `setGaps` | value: Boolean | this | Scala方法 | `setGaps(...)` |
| `setPattern` | value: String | this | Scala方法 | `setPattern(...)` |
| `setToLowercase` | value: Boolean | this | Scala方法 | `setToLowercase(...)` |
| `copy` | extra: ParamMap | RegexTokenizer | Scala方法 | `copy(...)` |
| `load` | path: String | RegexTokenizer | Scala方法 | `load(...)` |

---

### UnivariateFeatureSelector

**完整类名**: `org.apache.spark.ml.feature.UnivariateFeatureSelector`

**描述**: Scala定义的Java友好接口

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setSelectionMode` | value: String | this | Scala方法 | `setSelectionMode(...)` |
| `setSelectionThreshold` | value: Double | this | Scala方法 | `setSelectionThreshold(...)` |
| `setFeatureType` | value: String | this | Scala方法 | `setFeatureType(...)` |
| `setLabelType` | value: String | this | Scala方法 | `setLabelType(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `fit` | dataset: Dataset[_] | UnivariateFeatureSelectorModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | UnivariateFeatureSelector | Scala方法 | `copy(...)` |
| `load` | path: String | UnivariateFeatureSelector | Scala方法 | `load(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | UnivariateFeatureSelectorModel | Scala方法 | `copy(...)` |
| `load` | path: String | UnivariateFeatureSelectorModel | Scala方法 | `load(...)` |

---

### VarianceThresholdSelector

**完整类名**: `org.apache.spark.ml.feature.VarianceThresholdSelector`

**描述**: Scala定义的Java友好接口

**方法数**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setVarianceThreshold` | value: Double | this | Scala方法 | `setVarianceThreshold(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `fit` | dataset: Dataset[_] | VarianceThresholdSelectorModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | VarianceThresholdSelector | Scala方法 | `copy(...)` |
| `load` | path: String | VarianceThresholdSelector | Scala方法 | `load(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | VarianceThresholdSelectorModel | Scala方法 | `copy(...)` |
| `load` | path: String | VarianceThresholdSelectorModel | Scala方法 | `load(...)` |

---

### VectorAssembler

**完整类名**: `org.apache.spark.ml.feature.VectorAssembler`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCols` | value: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | VectorAssembler | Scala方法 | `copy(...)` |
| `load` | path: String | VectorAssembler | Scala方法 | `load(...)` |

---

### VectorIndexer

**完整类名**: `org.apache.spark.ml.feature.VectorIndexer`

**描述**: Scala定义的Java友好接口

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMaxCategories` | value: Int | this | Scala方法 | `setMaxCategories(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `fit` | dataset: Dataset[_] | VectorIndexerModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | VectorIndexer | Scala方法 | `copy(...)` |
| `load` | path: String | VectorIndexer | Scala方法 | `load(...)` |
| `merge` | other: CategoryStats | CategoryStats | Scala方法 | `merge(...)` |
| `addVector` | v: Vector | Unit | Scala方法 | `addVector(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | VectorIndexerModel | Scala方法 | `copy(...)` |
| `load` | path: String | VectorIndexerModel | Scala方法 | `load(...)` |

---

### VectorSizeHint

**完整类名**: `org.apache.spark.ml.feature.VectorSizeHint`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setSize` | value: Int | this | Scala方法 | `setSize(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | this | Scala方法 | `copy(...)` |
| `load` | path: String | VectorSizeHint | Scala方法 | `load(...)` |

---

### Word2Vec

**完整类名**: `org.apache.spark.ml.feature.Word2Vec`

**描述**: Scala定义的Java友好接口

**方法数**: 26

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setVectorSize` | value: Int | this | Scala方法 | `setVectorSize(...)` |
| `setWindowSize` | value: Int | this | Scala方法 | `setWindowSize(...)` |
| `setStepSize` | value: Double | this | Scala方法 | `setStepSize(...)` |
| `setNumPartitions` | value: Int | this | Scala方法 | `setNumPartitions(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setMinCount` | value: Int | this | Scala方法 | `setMinCount(...)` |
| `setMaxSentenceLength` | value: Int | this | Scala方法 | `setMaxSentenceLength(...)` |
| `fit` | dataset: Dataset[_] | Word2VecModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | Word2Vec | Scala方法 | `copy(...)` |
| `load` | path: String | Word2Vec | Scala方法 | `load(...)` |
| `findSynonyms` | word: String, num: Int | DataFrame | Scala方法 | `findSynonyms(...)` |
| `findSynonyms` | vec: Vector, num: Int | DataFrame | Scala方法 | `findSynonyms(...)` |
| `findSynonymsArray` | vec: Vector, num: Int | Array[ | Scala方法 | `findSynonymsArray(...)` |
| `findSynonymsArray` | word: String, num: Int | Array[ | Scala方法 | `findSynonymsArray(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | Word2VecModel | Scala方法 | `copy(...)` |
| `calculateNumberOfPartitions` | bufferSizeInBytes: Long,
        numWords: Int,
        vectorSize: Int | Int | Scala方法 | `calculateNumberOfPartitions(...)` |
| `load` | path: String | Word2VecModel | Scala方法 | `load(...)` |

---

### estimates

**完整类名**: `org.apache.spark.ml.feature.estimates`

**描述**: Scala定义的Java友好接口

**方法数**: 21

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | values: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | values: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `setTargetType` | value: String | this | Scala方法 | `setTargetType(...)` |
| `setSmoothing` | value: Double | this | Scala方法 | `setSmoothing(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `fit` | dataset: Dataset[_] | TargetEncoderModel | Scala方法 | `fit(...)` |
| `copy` | extra: ParamMap | TargetEncoder | Scala方法 | `copy(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setInputCols` | values: Array[String] | this | Scala方法 | `setInputCols(...)` |
| `setOutputCols` | values: Array[String] | this | Scala方法 | `setOutputCols(...)` |
| `setHandleInvalid` | value: String | this | Scala方法 | `setHandleInvalid(...)` |
| `setSmoothing` | value: Double | this | Scala方法 | `setSmoothing(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `copy` | extra: ParamMap | TargetEncoderModel | Scala方法 | `copy(...)` |
| `load` | path: String | TargetEncoderModel | Scala方法 | `load(...)` |

---

### for

**完整类名**: `org.apache.spark.ml.feature.for`

**描述**: Scala定义的Java友好接口

**方法数**: 18

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `setNumTopFeatures` | value: Int | this | Scala方法 | `setNumTopFeatures(...)` |
| `setPercentile` | value: Double | this | Scala方法 | `setPercentile(...)` |
| `setFpr` | value: Double | this | Scala方法 | `setFpr(...)` |
| `setFdr` | value: Double | this | Scala方法 | `setFdr(...)` |
| `setFwe` | value: Double | this | Scala方法 | `setFwe(...)` |
| `setSelectorType` | value: String | this | Scala方法 | `setSelectorType(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `fit` | dataset: Dataset[_] | T | Scala方法 | `fit(...)` |
| `getTopIndices` | k: Int | Array[Int] | Scala方法 | `getTopIndices(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | Selector[T] | Scala方法 | `copy(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `prepOutputField` | schema: StructType,
      selectedFeatures: Array[Int],
      outputCol: String,
      featuresCol: String,
      isNumericAttribute: Boolean | StructField | Scala方法 | `prepOutputField(...)` |
| `compressSparse` | indices: Array[Int],
      values: Array[Double],
      selectedFeatures: Array[Int] |  | Scala方法 | `compressSparse(...)` |

---

### takes

**完整类名**: `org.apache.spark.ml.feature.takes`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setIndices` | value: Array[Int] | this | Scala方法 | `setIndices(...)` |
| `setNames` | value: Array[String] | this | Scala方法 | `setNames(...)` |
| `setInputCol` | value: String | this | Scala方法 | `setInputCol(...)` |
| `setOutputCol` | value: String | this | Scala方法 | `setOutputCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | VectorSlicer | Scala方法 | `copy(...)` |
| `load` | path: String | VectorSlicer | Scala方法 | `load(...)` |

---

## 包: org.apache.spark.ml.fpm

**类数量**: 2

### FPGrowth

**完整类名**: `org.apache.spark.ml.fpm.FPGrowth`

**描述**: Scala定义的Java友好接口

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMinSupport` | value: Double | this | Scala方法 | `setMinSupport(...)` |
| `setNumPartitions` | value: Int | this | Scala方法 | `setNumPartitions(...)` |
| `setMinConfidence` | value: Double | this | Scala方法 | `setMinConfidence(...)` |
| `setItemsCol` | value: String | this | Scala方法 | `setItemsCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `fit` | dataset: Dataset[_] | FPGrowthModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | FPGrowth | Scala方法 | `copy(...)` |
| `load` | path: String | FPGrowth | Scala方法 | `load(...)` |
| `setMinConfidence` | value: Double | this | Scala方法 | `setMinConfidence(...)` |
| `setItemsCol` | value: String | this | Scala方法 | `setItemsCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | FPGrowthModel | Scala方法 | `copy(...)` |
| `load` | path: String | FPGrowthModel | Scala方法 | `load(...)` |

---

### is

**完整类名**: `org.apache.spark.ml.fpm.is`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMinSupport` | value: Double | this | Scala方法 | `setMinSupport(...)` |
| `setMaxPatternLength` | value: Int | this | Scala方法 | `setMaxPatternLength(...)` |
| `setMaxLocalProjDBSize` | value: Long | this | Scala方法 | `setMaxLocalProjDBSize(...)` |
| `setSequenceCol` | value: String | this | Scala方法 | `setSequenceCol(...)` |
| `findFrequentSequentialPatterns` | dataset: Dataset[_] | DataFrame | Scala方法 | `findFrequentSequentialPatterns(...)` |
| `copy` | extra: ParamMap | PrefixSpan | Scala方法 | `copy(...)` |

---

## 包: org.apache.spark.ml.image

**类数量**: 2

### ImageSchema

**完整类名**: `org.apache.spark.ml.image.ImageSchema`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOrigin` | row: Row | String | Scala方法 | `getOrigin(...)` |
| `getHeight` | row: Row | Int | Scala方法 | `getHeight(...)` |
| `getWidth` | row: Row | Int | Scala方法 | `getWidth(...)` |
| `getNChannels` | row: Row | Int | Scala方法 | `getNChannels(...)` |
| `getMode` | row: Row | Int | Scala方法 | `getMode(...)` |
| `getData` | row: Row | Array[Byte] | Scala方法 | `getData(...)` |

---

### RecursiveFlag

**完整类名**: `org.apache.spark.ml.image.RecursiveFlag`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setConf` | conf: Configuration | Unit | Scala方法 | `setConf(...)` |
| `accept` | path: Path | Boolean | Scala方法 | `accept(...)` |

---

## 包: org.apache.spark.ml.linalg

**类数量**: 4

### JsonMatrixConverter

**完整类名**: `org.apache.spark.ml.linalg.JsonMatrixConverter`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromJson` | json: String | Matrix | Scala方法 | `fromJson(...)` |
| `toJson` | m: Matrix | String | Scala方法 | `toJson(...)` |

---

### JsonVectorConverter

**完整类名**: `org.apache.spark.ml.linalg.JsonVectorConverter`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromJson` | json: String | Vector | Scala方法 | `fromJson(...)` |
| `toJson` | v: Vector | String | Scala方法 | `toJson(...)` |

---

### MatrixUDT

**完整类名**: `org.apache.spark.ml.linalg.MatrixUDT`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `serialize` | obj: Matrix | InternalRow | Scala方法 | `serialize(...)` |
| `deserialize` | datum: Any | Matrix | Scala方法 | `deserialize(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |

---

### VectorUDT

**完整类名**: `org.apache.spark.ml.linalg.VectorUDT`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `serialize` | obj: Vector | InternalRow | Scala方法 | `serialize(...)` |
| `deserialize` | datum: Any | Vector | Scala方法 | `deserialize(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |

---

## 包: org.apache.spark.ml.optim

**类数量**: 3

### IterativelyReweightedLeastSquaresModel

**完整类名**: `org.apache.spark.ml.optim.IterativelyReweightedLeastSquaresModel`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fit` | instances: RDD[OffsetInstance],
      instr: OptionalInstrumentation = OptionalInstrumentation.create(
        classOf[IterativelyReweightedLeastSquares] | Unit | Scala方法 | `fit(...)` |

---

### WeightedLeastSquaresModel

**完整类名**: `org.apache.spark.ml.optim.WeightedLeastSquaresModel`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `fit` | instances: RDD[Instance],
      instr: OptionalInstrumentation = OptionalInstrumentation.create(
        classOf[WeightedLeastSquares] | Unit | Scala方法 | `fit(...)` |
| `add` | instance: Instance | this | Scala方法 | `add(...)` |
| `merge` | other: Aggregator | this | Scala方法 | `merge(...)` |
| `validate` | 无 | Unit | Scala方法 | `validate(...)` |

---

### to

**完整类名**: `org.apache.spark.ml.optim.to`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `solve` | bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector | NormalEquationSolution | Scala方法 | `solve(...)` |
| `solve` | bBar: Double,
      bbBar: Double,
      abBar: DenseVector,
      aaBar: DenseVector,
      aBar: DenseVector | NormalEquationSolution | Scala方法 | `solve(...)` |
| `calculate` | coefficients: BDV[Double] |  | Scala方法 | `calculate(...)` |
| `this` | message: String | Unit | Scala方法 | `this(...)` |

---

## 包: org.apache.spark.ml.optim.aggregator

**类数量**: 7

### AFTBlockAggregator

**完整类名**: `org.apache.spark.ml.optim.aggregator.AFTBlockAggregator`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | block: InstanceBlock | this | Scala方法 | `add(...)` |

---

### BinaryLogisticBlockAggregator

**完整类名**: `org.apache.spark.ml.optim.aggregator.BinaryLogisticBlockAggregator`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | block: InstanceBlock | this | Scala方法 | `add(...)` |

---

### HingeBlockAggregator

**完整类名**: `org.apache.spark.ml.optim.aggregator.HingeBlockAggregator`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | block: InstanceBlock | this | Scala方法 | `add(...)` |

---

### HuberBlockAggregator

**完整类名**: `org.apache.spark.ml.optim.aggregator.HuberBlockAggregator`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | block: InstanceBlock | this | Scala方法 | `add(...)` |

---

### LeastSquaresBlockAggregator

**完整类名**: `org.apache.spark.ml.optim.aggregator.LeastSquaresBlockAggregator`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | block: InstanceBlock | this | Scala方法 | `add(...)` |

---

### MultinomialLogisticBlockAggregator

**完整类名**: `org.apache.spark.ml.optim.aggregator.MultinomialLogisticBlockAggregator`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | block: InstanceBlock | this | Scala方法 | `add(...)` |

---

### this

**完整类名**: `org.apache.spark.ml.optim.aggregator.this`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | instance: Datum | Agg | Scala方法 | `add(...)` |
| `merge` | other: Agg | Agg | Scala方法 | `merge(...)` |

---

## 包: org.apache.spark.ml.optim.loss

**类数量**: 2

### L2Regularization

**完整类名**: `org.apache.spark.ml.optim.loss.L2Regularization`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `calculate` | coefficients: Vector |  | Scala方法 | `calculate(...)` |

---

### computes

**完整类名**: `org.apache.spark.ml.optim.loss.computes`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `calculate` | coefficients: BDV[Double] |  | Scala方法 | `calculate(...)` |

---

## 包: org.apache.spark.ml.param

**类数量**: 1

### Param

**完整类名**: `org.apache.spark.ml.param.Param`

**描述**: Scala定义的Java友好接口

**方法数**: 72

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | parent: String, name: String, doc: String, isValid: T => Boolean | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String, isValid: T => Boolean | Unit | Scala方法 | `this(...)` |
| `this` | parent: String, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `this` | parent: String, name: String, doc: String, dataClass: Class[T] | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: T | ParamPair[T] | Scala方法 | `w(...)` |
| `jsonEncode` | value: T | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | T | Scala方法 | `jsonDecode(...)` |
| `equals` | obj: Any | Boolean | Scala方法 | `equals(...)` |
| `checkSingleVsMultiColumnParams` | model: Params,
      singleColumnParams: Seq[Param[_]],
      multiColumnParams: Seq[Param[_]] | Unit | Scala方法 | `checkSingleVsMultiColumnParams(...)` |
| `checkExclusiveParams` | isSingleCol: Boolean,
        requiredParams: Seq[Param[_]],
        excludedParams: Seq[Param[_]] | Unit | Scala方法 | `checkExclusiveParams(...)` |
| `this` | parent: String, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String, isValid: Double => Boolean | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: Double | ParamPair[Double] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Double | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Double | Scala方法 | `jsonDecode(...)` |
| `jValueDecode` | jValue: JValue | Double | Scala方法 | `jValueDecode(...)` |
| `this` | parent: String, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String, isValid: Int => Boolean | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: Int | ParamPair[Int] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Int | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Int | Scala方法 | `jsonDecode(...)` |
| `this` | parent: String, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String, isValid: Float => Boolean | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: Float | ParamPair[Float] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Float | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Float | Scala方法 | `jsonDecode(...)` |
| `jValueDecode` | jValue: JValue | Float | Scala方法 | `jValueDecode(...)` |
| `this` | parent: String, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String, isValid: Long => Boolean | Unit | Scala方法 | `this(...)` |
| `this` | parent: Identifiable, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: Long | ParamPair[Long] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Long | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Long | Scala方法 | `jsonDecode(...)` |
| `this` | parent: Identifiable, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: Boolean | ParamPair[Boolean] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Boolean | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Boolean | Scala方法 | `jsonDecode(...)` |
| `this` | parent: Params, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: java.util.List[String] | ParamPair[Array[String]] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Array[String] | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Array[String] | Scala方法 | `jsonDecode(...)` |
| `this` | parent: Params, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: java.util.List[java.lang.Double] | ParamPair[Array[Double]] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Array[Double] | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Array[Double] | Scala方法 | `jsonDecode(...)` |
| `this` | parent: Params, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: java.util.List[java.util.List[java.lang.Double]] | ParamPair[Array[Array[Double]]] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Array[Array[Double]] | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Array[Array[Double]] | Scala方法 | `jsonDecode(...)` |
| `this` | parent: Params, name: String, doc: String | Unit | Scala方法 | `this(...)` |
| `w` | value: java.util.List[java.lang.Integer] | ParamPair[Array[Int]] | Scala方法 | `w(...)` |
| `jsonEncode` | value: Array[Int] | String | Scala方法 | `jsonEncode(...)` |
| `jsonDecode` | json: String | Array[Int] | Scala方法 | `jsonDecode(...)` |
| `explainParam` | param: Param[_] | String | Scala方法 | `explainParam(...)` |
| `explainParams` | 无 | String | Scala方法 | `explainParams(...)` |
| `isSet` | param: Param[_] | Boolean | Scala方法 | `isSet(...)` |
| `isDefined` | param: Param[_] | Boolean | Scala方法 | `isDefined(...)` |
| `hasParam` | paramName: String | Boolean | Scala方法 | `hasParam(...)` |
| `getParam` | paramName: String | Param[Any] | Scala方法 | `getParam(...)` |
| `clear` | param: Param[_] | this | Scala方法 | `clear(...)` |
| `copy` | extra: ParamMap | Params | Scala方法 | `copy(...)` |
| `extractParamMap` | extra: ParamMap | ParamMap | Scala方法 | `extractParamMap(...)` |
| `extractParamMap` | 无 | ParamMap | Scala方法 | `extractParamMap(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `put` | paramPairs: ParamPair[_]* | this | Scala方法 | `put(...)` |
| `contains` | param: Param[_] | Boolean | Scala方法 | `contains(...)` |
| `filter` | parent: Params | ParamMap | Scala方法 | `filter(...)` |
| `apply` | paramPairs: ParamPair[_]* | ParamMap | Scala方法 | `apply(...)` |

---

## 包: org.apache.spark.ml.python

**类数量**: 2

### MLSerDe

**完整类名**: `org.apache.spark.ml.python.MLSerDe`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `initialize` | 无 | Unit | Scala方法 | `initialize(...)` |

---

### MLUtil

**完整类名**: `org.apache.spark.ml.python.MLUtil`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `copyFileFromLocalToFs` | localPath: String, destPath: String | Unit | Scala方法 | `copyFileFromLocalToFs(...)` |

---

## 包: org.apache.spark.ml.r

**类数量**: 23

### AFTSurvivalRegressionWrapper

**完整类名**: `org.apache.spark.ml.r.AFTSurvivalRegressionWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | formula: String,
      data: DataFrame,
      aggregationDepth: Int,
      stringIndexerOrderType: String | AFTSurvivalRegressionWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | AFTSurvivalRegressionWrapper | Scala方法 | `load(...)` |
| `load` | path: String | AFTSurvivalRegressionWrapper | Scala方法 | `load(...)` |

---

### ALSWrapper

**完整类名**: `org.apache.spark.ml.r.ALSWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | ALSWrapper | Scala方法 | `load(...)` |
| `load` | path: String | ALSWrapper | Scala方法 | `load(...)` |

---

### BisectingKMeansWrapper

**完整类名**: `org.apache.spark.ml.r.BisectingKMeansWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fitted` | method: String | DataFrame | Scala方法 | `fitted(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | BisectingKMeansWrapper | Scala方法 | `load(...)` |
| `load` | path: String | BisectingKMeansWrapper | Scala方法 | `load(...)` |

---

### DecisionTreeClassifierWrapper

**完整类名**: `org.apache.spark.ml.r.DecisionTreeClassifierWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | // scalastyle:ignore
      data: DataFrame,
      formula: String,
      maxDepth: Int,
      maxBins: Int,
      impurity: String,
      minInstancesPerNode: Int,
      minInfoGain: Double,
      checkpointInterval: Int,
      seed: String,
      maxMemoryInMB: Int,
      cacheNodeIds: Boolean,
      handleInvalid: String | DecisionTreeClassifierWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | DecisionTreeClassifierWrapper | Scala方法 | `load(...)` |
| `load` | path: String | DecisionTreeClassifierWrapper | Scala方法 | `load(...)` |

---

### DecisionTreeRegressorWrapper

**完整类名**: `org.apache.spark.ml.r.DecisionTreeRegressorWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | DecisionTreeRegressorWrapper | Scala方法 | `load(...)` |
| `load` | path: String | DecisionTreeRegressorWrapper | Scala方法 | `load(...)` |

---

### FMClassifierWrapper

**完整类名**: `org.apache.spark.ml.r.FMClassifierWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | // scalastyle:ignore
      data: DataFrame,
      formula: String,
      factorSize: Int,
      fitLinear: Boolean,
      regParam: Double,
      miniBatchFraction: Double,
      initStd: Double,
      maxIter: Int,
      stepSize: Double,
      tol: Double,
      solver: String,
      seed: String,
      thresholds: Array[Double],
      handleInvalid: String | FMClassifierWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | FMClassifierWrapper | Scala方法 | `load(...)` |

---

### FMRegressorWrapper

**完整类名**: `org.apache.spark.ml.r.FMRegressorWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | FMRegressorWrapper | Scala方法 | `load(...)` |

---

### FPGrowthWrapper

**完整类名**: `org.apache.spark.ml.r.FPGrowthWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | FPGrowthWrapper | Scala方法 | `load(...)` |

---

### GBTClassifierWrapper

**完整类名**: `org.apache.spark.ml.r.GBTClassifierWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | // scalastyle:ignore
      data: DataFrame,
      formula: String,
      maxDepth: Int,
      maxBins: Int,
      maxIter: Int,
      stepSize: Double,
      minInstancesPerNode: Int,
      minInfoGain: Double,
      checkpointInterval: Int,
      lossType: String,
      seed: String,
      subsamplingRate: Double,
      maxMemoryInMB: Int,
      cacheNodeIds: Boolean,
      handleInvalid: String | GBTClassifierWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | GBTClassifierWrapper | Scala方法 | `load(...)` |
| `load` | path: String | GBTClassifierWrapper | Scala方法 | `load(...)` |

---

### GBTRegressorWrapper

**完整类名**: `org.apache.spark.ml.r.GBTRegressorWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | GBTRegressorWrapper | Scala方法 | `load(...)` |
| `load` | path: String | GBTRegressorWrapper | Scala方法 | `load(...)` |

---

### GaussianMixtureWrapper

**完整类名**: `org.apache.spark.ml.r.GaussianMixtureWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | GaussianMixtureWrapper | Scala方法 | `load(...)` |
| `load` | path: String | GaussianMixtureWrapper | Scala方法 | `load(...)` |

---

### GeneralizedLinearRegressionWrapper

**完整类名**: `org.apache.spark.ml.r.GeneralizedLinearRegressionWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `residuals` | residualsType: String | DataFrame | Scala方法 | `residuals(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | formula: String,
      data: DataFrame,
      family: String,
      link: String,
      tol: Double,
      maxIter: Int,
      weightCol: String,
      regParam: Double,
      variancePower: Double,
      linkPower: Double,
      stringIndexerOrderType: String,
      offsetCol: String | GeneralizedLinearRegressionWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | GeneralizedLinearRegressionWrapper | Scala方法 | `load(...)` |
| `load` | path: String | GeneralizedLinearRegressionWrapper | Scala方法 | `load(...)` |

---

### IsotonicRegressionWrapper

**完整类名**: `org.apache.spark.ml.r.IsotonicRegressionWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | IsotonicRegressionWrapper | Scala方法 | `load(...)` |
| `load` | path: String | IsotonicRegressionWrapper | Scala方法 | `load(...)` |

---

### KMeansWrapper

**完整类名**: `org.apache.spark.ml.r.KMeansWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fitted` | method: String | DataFrame | Scala方法 | `fitted(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | KMeansWrapper | Scala方法 | `load(...)` |
| `load` | path: String | KMeansWrapper | Scala方法 | `load(...)` |

---

### LDAWrapper

**完整类名**: `org.apache.spark.ml.r.LDAWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | data: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `computeLogPerplexity` | data: Dataset[_] | Double | Scala方法 | `computeLogPerplexity(...)` |
| `topics` | maxTermsPerTopic: Int | DataFrame | Scala方法 | `topics(...)` |
| `fit` | data: DataFrame,
      features: String,
      k: Int,
      maxIter: Int,
      optimizer: String,
      subsamplingRate: Double,
      topicConcentration: Double,
      docConcentration: Array[Double],
      customizedStopWords: Array[String],
      maxVocabSize: Int | LDAWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | LDAWrapper | Scala方法 | `load(...)` |
| `load` | path: String | LDAWrapper | Scala方法 | `load(...)` |

---

### LinearRegressionWrapper

**完整类名**: `org.apache.spark.ml.r.LinearRegressionWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | LinearRegressionWrapper | Scala方法 | `load(...)` |

---

### LinearSVCWrapper

**完整类名**: `org.apache.spark.ml.r.LinearSVCWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | data: DataFrame,
      formula: String,
      regParam: Double,
      maxIter: Int,
      tol: Double,
      standardization: Boolean,
      threshold: Double,
      weightCol: String,
      aggregationDepth: Int,
      handleInvalid: String | LinearSVCWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | LinearSVCWrapper | Scala方法 | `load(...)` |
| `load` | path: String | LinearSVCWrapper | Scala方法 | `load(...)` |

---

### LogisticRegressionWrapper

**完整类名**: `org.apache.spark.ml.r.LogisticRegressionWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | // scalastyle:ignore
      data: DataFrame,
      formula: String,
      regParam: Double,
      elasticNetParam: Double,
      maxIter: Int,
      tol: Double,
      family: String,
      standardization: Boolean,
      thresholds: Array[Double],
      weightCol: String,
      aggregationDepth: Int,
      numRowsOfBoundsOnCoefficients: Int,
      numColsOfBoundsOnCoefficients: Int,
      lowerBoundsOnCoefficients: Array[Double],
      upperBoundsOnCoefficients: Array[Double],
      lowerBoundsOnIntercepts: Array[Double],
      upperBoundsOnIntercepts: Array[Double],
      handleInvalid: String | LogisticRegressionWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | LogisticRegressionWrapper | Scala方法 | `load(...)` |
| `load` | path: String | LogisticRegressionWrapper | Scala方法 | `load(...)` |

---

### MultilayerPerceptronClassifierWrapper

**完整类名**: `org.apache.spark.ml.r.MultilayerPerceptronClassifierWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | // scalastyle:ignore
      data: DataFrame,
      formula: String,
      blockSize: Int,
      layers: Array[Int],
      solver: String,
      maxIter: Int,
      tol: Double,
      stepSize: Double,
      seed: String,
      initialWeights: Array[Double],
      handleInvalid: String | MultilayerPerceptronClassifierWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | MultilayerPerceptronClassifierWrapper | Scala方法 | `load(...)` |
| `load` | path: String | MultilayerPerceptronClassifierWrapper | Scala方法 | `load(...)` |

---

### NaiveBayesWrapper

**完整类名**: `org.apache.spark.ml.r.NaiveBayesWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | formula: String,
      data: DataFrame,
      smoothing: Double,
      handleInvalid: String | NaiveBayesWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | NaiveBayesWrapper | Scala方法 | `load(...)` |
| `load` | path: String | NaiveBayesWrapper | Scala方法 | `load(...)` |

---

### RWrapperUtils

**完整类名**: `org.apache.spark.ml.r.RWrapperUtils`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkDataColumns` | rFormula: RFormula, data: Dataset[_] | Unit | Scala方法 | `checkDataColumns(...)` |
| `getFeaturesAndLabels` | rFormulaModel: RFormulaModel,
      data: Dataset[_] |  | Scala方法 | `getFeaturesAndLabels(...)` |

---

### RandomForestClassifierWrapper

**完整类名**: `org.apache.spark.ml.r.RandomForestClassifierWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `fit` | // scalastyle:ignore
      data: DataFrame,
      formula: String,
      maxDepth: Int,
      maxBins: Int,
      numTrees: Int,
      impurity: String,
      minInstancesPerNode: Int,
      minInfoGain: Double,
      checkpointInterval: Int,
      featureSubsetStrategy: String,
      seed: String,
      subsamplingRate: Double,
      maxMemoryInMB: Int,
      cacheNodeIds: Boolean,
      handleInvalid: String,
      bootstrap: Boolean | RandomForestClassifierWrapper | Scala方法 | `fit(...)` |
| `load` | path: String | RandomForestClassifierWrapper | Scala方法 | `load(...)` |
| `load` | path: String | RandomForestClassifierWrapper | Scala方法 | `load(...)` |

---

### RandomForestRegressorWrapper

**完整类名**: `org.apache.spark.ml.r.RandomForestRegressorWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `load` | path: String | RandomForestRegressorWrapper | Scala方法 | `load(...)` |
| `load` | path: String | RandomForestRegressorWrapper | Scala方法 | `load(...)` |

---

## 包: org.apache.spark.ml.recommendation

**类数量**: 1

### ALSModel

**完整类名**: `org.apache.spark.ml.recommendation.ALSModel`

**描述**: Scala定义的Java友好接口

**方法数**: 62

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setItemCol` | value: String | this | Scala方法 | `setItemCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setColdStartStrategy` | value: String | this | Scala方法 | `setColdStartStrategy(...)` |
| `setBlockSize` | value: Int | this | Scala方法 | `setBlockSize(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | ALSModel | Scala方法 | `copy(...)` |
| `recommendForAllUsers` | numItems: Int | DataFrame | Scala方法 | `recommendForAllUsers(...)` |
| `recommendForUserSubset` | dataset: Dataset[_], numItems: Int | DataFrame | Scala方法 | `recommendForUserSubset(...)` |
| `recommendForAllItems` | numUsers: Int | DataFrame | Scala方法 | `recommendForAllItems(...)` |
| `recommendForItemSubset` | dataset: Dataset[_], numUsers: Int | DataFrame | Scala方法 | `recommendForItemSubset(...)` |
| `compare` | left: Int, right: Int | Int | Scala方法 | `compare(...)` |
| `load` | path: String | ALSModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setRank` | value: Int | this | Scala方法 | `setRank(...)` |
| `setNumUserBlocks` | value: Int | this | Scala方法 | `setNumUserBlocks(...)` |
| `setNumItemBlocks` | value: Int | this | Scala方法 | `setNumItemBlocks(...)` |
| `setImplicitPrefs` | value: Boolean | this | Scala方法 | `setImplicitPrefs(...)` |
| `setAlpha` | value: Double | this | Scala方法 | `setAlpha(...)` |
| `setUserCol` | value: String | this | Scala方法 | `setUserCol(...)` |
| `setItemCol` | value: String | this | Scala方法 | `setItemCol(...)` |
| `setRatingCol` | value: String | this | Scala方法 | `setRatingCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setRegParam` | value: Double | this | Scala方法 | `setRegParam(...)` |
| `setNonnegative` | value: Boolean | this | Scala方法 | `setNonnegative(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setIntermediateStorageLevel` | value: String | this | Scala方法 | `setIntermediateStorageLevel(...)` |
| `setFinalStorageLevel` | value: String | this | Scala方法 | `setFinalStorageLevel(...)` |
| `setColdStartStrategy` | value: String | this | Scala方法 | `setColdStartStrategy(...)` |
| `setBlockSize` | value: Int | this | Scala方法 | `setBlockSize(...)` |
| `setNumBlocks` | value: Int | this | Scala方法 | `setNumBlocks(...)` |
| `fit` | dataset: Dataset[_] | ALSModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | ALS | Scala方法 | `copy(...)` |
| `estimateModelSize` | dataset: Dataset[_] | Long | Scala方法 | `estimateModelSize(...)` |
| `load` | path: String | ALS | Scala方法 | `load(...)` |
| `solve` | ne: NormalEquation, lambda: Double | Array[Float] | Scala方法 | `solve(...)` |
| `solve` | ne: NormalEquation, lambda: Double | Array[Float] | Scala方法 | `solve(...)` |
| `solve` | ne: NormalEquation, lambda: Double | Array[Float] | Scala方法 | `solve(...)` |
| `add` | a: Array[Float], b: Double, c: Double = 1.0 | NormalEquation | Scala方法 | `add(...)` |
| `merge` | other: NormalEquation | NormalEquation | Scala方法 | `merge(...)` |
| `reset` | 无 | Unit | Scala方法 | `reset(...)` |
| `add` | r: Rating[ID] | this | Scala方法 | `add(...)` |
| `merge` | other: RatingBlock[ID] | this | Scala方法 | `merge(...)` |
| `build` | 无 | RatingBlock[ID] | Scala方法 | `build(...)` |
| `add` | dstBlockId: Int,
        srcIds: Array[ID],
        dstLocalIndices: Array[Int],
        ratings: Array[Float] | this | Scala方法 | `add(...)` |
| `build` | 无 | UncompressedInBlock[ID] | Scala方法 | `build(...)` |
| `compress` | 无 | InBlock[ID] | Scala方法 | `compress(...)` |
| `compare` | that: KeyWrapper[ID] | Int | Scala方法 | `compare(...)` |
| `setKey` | key: ID | this | Scala方法 | `setKey(...)` |
| `newKey` | 无 | KeyWrapper[ID] | Scala方法 | `newKey(...)` |
| `getKey` | data: UncompressedInBlock[ID],
        pos: Int,
        reuse: KeyWrapper[ID] | KeyWrapper[ID] | Scala方法 | `getKey(...)` |
| `getKey` | data: UncompressedInBlock[ID],
        pos: Int | KeyWrapper[ID] | Scala方法 | `getKey(...)` |
| `swap` | data: UncompressedInBlock[ID], pos0: Int, pos1: Int | Unit | Scala方法 | `swap(...)` |
| `copyRange` | src: UncompressedInBlock[ID],
        srcPos: Int,
        dst: UncompressedInBlock[ID],
        dstPos: Int,
        length: Int | Unit | Scala方法 | `copyRange(...)` |
| `allocate` | length: Int | UncompressedInBlock[ID] | Scala方法 | `allocate(...)` |
| `copyElement` | src: UncompressedInBlock[ID],
        srcPos: Int,
        dst: UncompressedInBlock[ID],
        dstPos: Int | Unit | Scala方法 | `copyElement(...)` |
| `encode` | blockId: Int, localIndex: Int | Int | Scala方法 | `encode(...)` |
| `blockId` | encoded: Int | Int | Scala方法 | `blockId(...)` |
| `localIndex` | encoded: Int | Int | Scala方法 | `localIndex(...)` |

---

## 包: org.apache.spark.ml.regression

**类数量**: 8

### AFTSurvivalRegression

**完整类名**: `org.apache.spark.ml.regression.AFTSurvivalRegression`

**描述**: Scala定义的Java友好接口

**方法数**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setCensorCol` | value: String | this | Scala方法 | `setCensorCol(...)` |
| `setQuantileProbabilities` | value: Array[Double] | this | Scala方法 | `setQuantileProbabilities(...)` |
| `setQuantilesCol` | value: String | this | Scala方法 | `setQuantilesCol(...)` |
| `setFitIntercept` | value: Boolean | this | Scala方法 | `setFitIntercept(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setAggregationDepth` | value: Int | this | Scala方法 | `setAggregationDepth(...)` |
| `setMaxBlockSizeInMB` | value: Double | this | Scala方法 | `setMaxBlockSizeInMB(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | AFTSurvivalRegression | Scala方法 | `copy(...)` |
| `load` | path: String | AFTSurvivalRegression | Scala方法 | `load(...)` |
| `setQuantileProbabilities` | value: Array[Double] | this | Scala方法 | `setQuantileProbabilities(...)` |
| `setQuantilesCol` | value: String | this | Scala方法 | `setQuantilesCol(...)` |
| `predictQuantiles` | features: Vector | Vector | Scala方法 | `predictQuantiles(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | AFTSurvivalRegressionModel | Scala方法 | `copy(...)` |
| `load` | path: String | AFTSurvivalRegressionModel | Scala方法 | `load(...)` |

---

### DecisionTreeRegressor

**完整类名**: `org.apache.spark.ml.regression.DecisionTreeRegressor`

**描述**: Scala定义的Java友好接口

**方法数**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMaxDepth` | value: Int | this | Scala方法 | `setMaxDepth(...)` |
| `setMaxBins` | value: Int | this | Scala方法 | `setMaxBins(...)` |
| `setMinInstancesPerNode` | value: Int | this | Scala方法 | `setMinInstancesPerNode(...)` |
| `setMinWeightFractionPerNode` | value: Double | this | Scala方法 | `setMinWeightFractionPerNode(...)` |
| `setMinInfoGain` | value: Double | this | Scala方法 | `setMinInfoGain(...)` |
| `setMaxMemoryInMB` | value: Int | this | Scala方法 | `setMaxMemoryInMB(...)` |
| `setCacheNodeIds` | value: Boolean | this | Scala方法 | `setCacheNodeIds(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setImpurity` | value: String | this | Scala方法 | `setImpurity(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setVarianceCol` | value: String | this | Scala方法 | `setVarianceCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `copy` | extra: ParamMap | DecisionTreeRegressor | Scala方法 | `copy(...)` |
| `load` | path: String | DecisionTreeRegressor | Scala方法 | `load(...)` |
| `setVarianceCol` | value: String | this | Scala方法 | `setVarianceCol(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `copy` | extra: ParamMap | DecisionTreeRegressionModel | Scala方法 | `copy(...)` |
| `load` | path: String | DecisionTreeRegressionModel | Scala方法 | `load(...)` |

---

### FactorizationMachines

**完整类名**: `org.apache.spark.ml.regression.FactorizationMachines`

**描述**: Scala定义的Java友好接口

**方法数**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `parseSolver` | solver: String, coefficientsSize: Int | Updater | Scala方法 | `parseSolver(...)` |
| `parseLoss` | lossFunc: String,
      factorSize: Int,
      fitIntercept: Boolean,
      fitLinear: Boolean,
      numFeatures: Int | BaseFactorizationMachinesGradient | Scala方法 | `parseLoss(...)` |
| `splitCoefficients` | coefficients: Vector,
      numFeatures: Int,
      factorSize: Int,
      fitIntercept: Boolean,
      fitLinear: Boolean |  | Scala方法 | `splitCoefficients(...)` |
| `combineCoefficients` | intercept: Double,
      linear: Vector,
      factors: Matrix,
      fitIntercept: Boolean,
      fitLinear: Boolean | Vector | Scala方法 | `combineCoefficients(...)` |
| `getRawPrediction` | features: Vector,
      intercept: Double,
      linear: Vector,
      factors: Matrix | Double | Scala方法 | `getRawPrediction(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFactorSize` | value: Int | this | Scala方法 | `setFactorSize(...)` |
| `setFitIntercept` | value: Boolean | this | Scala方法 | `setFitIntercept(...)` |
| `setFitLinear` | value: Boolean | this | Scala方法 | `setFitLinear(...)` |
| `setRegParam` | value: Double | this | Scala方法 | `setRegParam(...)` |
| `setMiniBatchFraction` | value: Double | this | Scala方法 | `setMiniBatchFraction(...)` |
| `setInitStd` | value: Double | this | Scala方法 | `setInitStd(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setStepSize` | value: Double | this | Scala方法 | `setStepSize(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setSolver` | value: String | this | Scala方法 | `setSolver(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `copy` | extra: ParamMap | FMRegressor | Scala方法 | `copy(...)` |
| `load` | path: String | FMRegressor | Scala方法 | `load(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `copy` | extra: ParamMap | FMRegressionModel | Scala方法 | `copy(...)` |
| `load` | path: String | FMRegressionModel | Scala方法 | `load(...)` |
| `compute` | data: OldVector,
      label: Double,
      weights: OldVector,
      cumGradient: OldVector | Double | Scala方法 | `compute(...)` |
| `getPrediction` | rawPrediction: Double | Double

  protected def getMultiplier | Scala方法 | `getPrediction(...)` |
| `getPrediction` | rawPrediction: Double | Double | Scala方法 | `getPrediction(...)` |
| `getPrediction` | rawPrediction: Double | Double | Scala方法 | `getPrediction(...)` |
| `compute` | weightsOld: OldVector,
    gradient: OldVector,
    stepSize: Double,
    iter: Int,
    regParam: Double |  | Scala方法 | `compute(...)` |

---

### GBTRegressor

**完整类名**: `org.apache.spark.ml.regression.GBTRegressor`

**描述**: Scala定义的Java友好接口

**方法数**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMaxDepth` | value: Int | this | Scala方法 | `setMaxDepth(...)` |
| `setMaxBins` | value: Int | this | Scala方法 | `setMaxBins(...)` |
| `setMinInstancesPerNode` | value: Int | this | Scala方法 | `setMinInstancesPerNode(...)` |
| `setMinWeightFractionPerNode` | value: Double | this | Scala方法 | `setMinWeightFractionPerNode(...)` |
| `setMinInfoGain` | value: Double | this | Scala方法 | `setMinInfoGain(...)` |
| `setMaxMemoryInMB` | value: Int | this | Scala方法 | `setMaxMemoryInMB(...)` |
| `setCacheNodeIds` | value: Boolean | this | Scala方法 | `setCacheNodeIds(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setImpurity` | value: String | this | Scala方法 | `setImpurity(...)` |
| `setSubsamplingRate` | value: Double | this | Scala方法 | `setSubsamplingRate(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setStepSize` | value: Double | this | Scala方法 | `setStepSize(...)` |
| `setLossType` | value: String | this | Scala方法 | `setLossType(...)` |
| `setFeatureSubsetStrategy` | value: String | this | Scala方法 | `setFeatureSubsetStrategy(...)` |
| `setValidationIndicatorCol` | value: String | this | Scala方法 | `setValidationIndicatorCol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `copy` | extra: ParamMap | GBTRegressor | Scala方法 | `copy(...)` |
| `load` | path: String | GBTRegressor | Scala方法 | `load(...)` |
| `this` | uid: String, _trees: Array[DecisionTreeRegressionModel], _treeWeights: Array[Double] | Unit | Scala方法 | `this(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `copy` | extra: ParamMap | GBTRegressionModel | Scala方法 | `copy(...)` |
| `evaluateEachIteration` | dataset: Dataset[_], loss: String | Array[Double] | Scala方法 | `evaluateEachIteration(...)` |
| `load` | path: String | GBTRegressionModel | Scala方法 | `load(...)` |

---

### GeneralizedLinearRegression

**完整类名**: `org.apache.spark.ml.regression.GeneralizedLinearRegression`

**描述**: Scala定义的Java友好接口

**方法数**: 84

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `validateAndTransformSchema` | schema: StructType,
      fitting: Boolean,
      featuresDataType: DataType | StructType | Scala方法 | `validateAndTransformSchema(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setFamily` | value: String | this | Scala方法 | `setFamily(...)` |
| `setVariancePower` | value: Double | this | Scala方法 | `setVariancePower(...)` |
| `setLinkPower` | value: Double | this | Scala方法 | `setLinkPower(...)` |
| `setLink` | value: String | this | Scala方法 | `setLink(...)` |
| `setFitIntercept` | value: Boolean | this | Scala方法 | `setFitIntercept(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setRegParam` | value: Double | this | Scala方法 | `setRegParam(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setOffsetCol` | value: String | this | Scala方法 | `setOffsetCol(...)` |
| `setSolver` | value: String | this | Scala方法 | `setSolver(...)` |
| `setLinkPredictionCol` | value: String | this | Scala方法 | `setLinkPredictionCol(...)` |
| `setAggregationDepth` | value: Int | this | Scala方法 | `setAggregationDepth(...)` |
| `copy` | extra: ParamMap | GeneralizedLinearRegression | Scala方法 | `copy(...)` |
| `estimateModelSize` | dataset: Dataset[_] | Long | Scala方法 | `estimateModelSize(...)` |
| `load` | path: String | GeneralizedLinearRegression | Scala方法 | `load(...)` |
| `predict` | mu: Double | Double | Scala方法 | `predict(...)` |
| `fitted` | eta: Double | Double | Scala方法 | `fitted(...)` |
| `initialize` | instances: RDD[OffsetInstance],
        fitIntercept: Boolean,
        regParam: Double,
        instr: OptionalInstrumentation = OptionalInstrumentation.create(
          classOf[GeneralizedLinearRegression] | Unit | Scala方法 | `initialize(...)` |
| `reweightFunc` | instance: OffsetInstance, model: WeightedLeastSquaresModel |  | Scala方法 | `reweightFunc(...)` |
| `apply` | params: GeneralizedLinearRegressionBase | FamilyAndLink | Scala方法 | `apply(...)` |
| `initialize` | y: Double, weight: Double | Double | Scala方法 | `initialize(...)` |
| `variance` | mu: Double | Double | Scala方法 | `variance(...)` |
| `deviance` | y: Double, mu: Double, weight: Double | Double | Scala方法 | `deviance(...)` |
| `aic` | predictions: RDD[(Double, Double, Double | Unit | Scala方法 | `aic(...)` |
| `project` | mu: Double | Double | Scala方法 | `project(...)` |
| `fromParams` | params: GeneralizedLinearRegressionBase | Family | Scala方法 | `fromParams(...)` |
| `initialize` | y: Double, weight: Double | Double | Scala方法 | `initialize(...)` |
| `variance` | mu: Double | Double | Scala方法 | `variance(...)` |
| `deviance` | y: Double, mu: Double, weight: Double | Double | Scala方法 | `deviance(...)` |
| `aic` | predictions: RDD[(Double, Double, Double | Unit | Scala方法 | `aic(...)` |
| `project` | mu: Double | Double | Scala方法 | `project(...)` |
| `initialize` | y: Double, weight: Double | Double | Scala方法 | `initialize(...)` |
| `variance` | mu: Double | Double | Scala方法 | `variance(...)` |
| `deviance` | y: Double, mu: Double, weight: Double | Double | Scala方法 | `deviance(...)` |
| `aic` | predictions: RDD[(Double, Double, Double | Unit | Scala方法 | `aic(...)` |
| `project` | mu: Double | Double | Scala方法 | `project(...)` |
| `initialize` | y: Double, weight: Double | Double | Scala方法 | `initialize(...)` |
| `variance` | mu: Double | Double | Scala方法 | `variance(...)` |
| `deviance` | y: Double, mu: Double, weight: Double | Double | Scala方法 | `deviance(...)` |
| `aic` | predictions: RDD[(Double, Double, Double | Unit | Scala方法 | `aic(...)` |
| `project` | mu: Double | Double | Scala方法 | `project(...)` |
| `initialize` | y: Double, weight: Double | Double | Scala方法 | `initialize(...)` |
| `variance` | mu: Double | Double | Scala方法 | `variance(...)` |
| `deviance` | y: Double, mu: Double, weight: Double | Double | Scala方法 | `deviance(...)` |
| `aic` | predictions: RDD[(Double, Double, Double | Unit | Scala方法 | `aic(...)` |
| `initialize` | y: Double, weight: Double | Double | Scala方法 | `initialize(...)` |
| `variance` | mu: Double | Double | Scala方法 | `variance(...)` |
| `deviance` | y: Double, mu: Double, weight: Double | Double | Scala方法 | `deviance(...)` |
| `aic` | predictions: RDD[(Double, Double, Double | Unit | Scala方法 | `aic(...)` |
| `link` | mu: Double | Double | Scala方法 | `link(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `fromParams` | params: GeneralizedLinearRegressionBase | Link | Scala方法 | `fromParams(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `link` | mu: Double | Double | Scala方法 | `link(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `link` | mu: Double | Double | Scala方法 | `link(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `link` | mu: Double | Double | Scala方法 | `link(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `link` | mu: Double | Double | Scala方法 | `link(...)` |
| `deriv` | mu: Double | Double | Scala方法 | `deriv(...)` |
| `unlink` | eta: Double | Double | Scala方法 | `unlink(...)` |
| `setLinkPredictionCol` | value: String | this | Scala方法 | `setLinkPredictionCol(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `evaluate` | dataset: Dataset[_] | GeneralizedLinearRegressionSummary | Scala方法 | `evaluate(...)` |
| `copy` | extra: ParamMap | GeneralizedLinearRegressionModel | Scala方法 | `copy(...)` |
| `load` | path: String | GeneralizedLinearRegressionModel | Scala方法 | `load(...)` |
| `residuals` | 无 | DataFrame | Scala方法 | `residuals(...)` |
| `residuals` | residualsType: String | DataFrame | Scala方法 | `residuals(...)` |
| `round` | x: Double | String | Scala方法 | `round(...)` |

---

### IsotonicRegression

**完整类名**: `org.apache.spark.ml.regression.IsotonicRegression`

**描述**: Scala定义的Java友好接口

**方法数**: 18

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setLabelCol` | value: String | this | Scala方法 | `setLabelCol(...)` |
| `setFeaturesCol` | value: String | this | Scala方法 | `setFeaturesCol(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setIsotonic` | value: Boolean | this | Scala方法 | `setIsotonic(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setFeatureIndex` | value: Int | this | Scala方法 | `setFeatureIndex(...)` |
| `copy` | extra: ParamMap | IsotonicRegression | Scala方法 | `copy(...)` |
| `fit` | dataset: Dataset[_] | IsotonicRegressionModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | IsotonicRegression | Scala方法 | `load(...)` |
| `setPredictionCol` | value: String | this | Scala方法 | `setPredictionCol(...)` |
| `setFeatureIndex` | value: Int | this | Scala方法 | `setFeatureIndex(...)` |
| `copy` | extra: ParamMap | IsotonicRegressionModel | Scala方法 | `copy(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predict` | value: Double | Double | Scala方法 | `predict(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `load` | path: String | IsotonicRegressionModel | Scala方法 | `load(...)` |

---

### LinearRegression

**完整类名**: `org.apache.spark.ml.regression.LinearRegression`

**描述**: Scala定义的Java友好接口

**方法数**: 25

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setRegParam` | value: Double | this | Scala方法 | `setRegParam(...)` |
| `setFitIntercept` | value: Boolean | this | Scala方法 | `setFitIntercept(...)` |
| `setStandardization` | value: Boolean | this | Scala方法 | `setStandardization(...)` |
| `setElasticNetParam` | value: Double | this | Scala方法 | `setElasticNetParam(...)` |
| `setMaxIter` | value: Int | this | Scala方法 | `setMaxIter(...)` |
| `setTol` | value: Double | this | Scala方法 | `setTol(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `setSolver` | value: String | this | Scala方法 | `setSolver(...)` |
| `setAggregationDepth` | value: Int | this | Scala方法 | `setAggregationDepth(...)` |
| `setLoss` | value: String | this | Scala方法 | `setLoss(...)` |
| `setEpsilon` | value: Double | this | Scala方法 | `setEpsilon(...)` |
| `setMaxBlockSizeInMB` | value: Double | this | Scala方法 | `setMaxBlockSizeInMB(...)` |
| `copy` | extra: ParamMap | LinearRegression | Scala方法 | `copy(...)` |
| `estimateModelSize` | dataset: Dataset[_] | Long | Scala方法 | `estimateModelSize(...)` |
| `load` | path: String | LinearRegression | Scala方法 | `load(...)` |
| `evaluate` | dataset: Dataset[_] | LinearRegressionSummary | Scala方法 | `evaluate(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `copy` | extra: ParamMap | LinearRegressionModel | Scala方法 | `copy(...)` |
| `format` | 无 | String | Scala方法 | `format(...)` |
| `stageName` | 无 | String | Scala方法 | `stageName(...)` |
| `write` | path: String, sparkSession: SparkSession,
    optionMap: mutable.Map[String, String], stage: PipelineStage | Unit | Scala方法 | `write(...)` |
| `format` | 无 | String | Scala方法 | `format(...)` |
| `stageName` | 无 | String | Scala方法 | `stageName(...)` |
| `load` | path: String | LinearRegressionModel | Scala方法 | `load(...)` |

---

### RandomForestRegressor

**完整类名**: `org.apache.spark.ml.regression.RandomForestRegressor`

**描述**: Scala定义的Java友好接口

**方法数**: 23

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMaxDepth` | value: Int | this | Scala方法 | `setMaxDepth(...)` |
| `setMaxBins` | value: Int | this | Scala方法 | `setMaxBins(...)` |
| `setMinInstancesPerNode` | value: Int | this | Scala方法 | `setMinInstancesPerNode(...)` |
| `setMinWeightFractionPerNode` | value: Double | this | Scala方法 | `setMinWeightFractionPerNode(...)` |
| `setMinInfoGain` | value: Double | this | Scala方法 | `setMinInfoGain(...)` |
| `setMaxMemoryInMB` | value: Int | this | Scala方法 | `setMaxMemoryInMB(...)` |
| `setCacheNodeIds` | value: Boolean | this | Scala方法 | `setCacheNodeIds(...)` |
| `setCheckpointInterval` | value: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setImpurity` | value: String | this | Scala方法 | `setImpurity(...)` |
| `setSubsamplingRate` | value: Double | this | Scala方法 | `setSubsamplingRate(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setNumTrees` | value: Int | this | Scala方法 | `setNumTrees(...)` |
| `setBootstrap` | value: Boolean | this | Scala方法 | `setBootstrap(...)` |
| `setFeatureSubsetStrategy` | value: String | this | Scala方法 | `setFeatureSubsetStrategy(...)` |
| `setWeightCol` | value: String | this | Scala方法 | `setWeightCol(...)` |
| `copy` | extra: ParamMap | RandomForestRegressor | Scala方法 | `copy(...)` |
| `load` | path: String | RandomForestRegressor | Scala方法 | `load(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `copy` | extra: ParamMap | RandomForestRegressionModel | Scala方法 | `copy(...)` |
| `load` | path: String | RandomForestRegressionModel | Scala方法 | `load(...)` |

---

## 包: org.apache.spark.ml.source.image

**类数量**: 1

### ImageFileFormat

**完整类名**: `org.apache.spark.ml.source.image.ImageFileFormat`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `inferSchema` | sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus] | Option[StructType] | Scala方法 | `inferSchema(...)` |
| `prepareWrite` | sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType | OutputWriterFactory | Scala方法 | `prepareWrite(...)` |
| `shortName` | 无 | String | Scala方法 | `shortName(...)` |

---

## 包: org.apache.spark.ml.source.libsvm

**类数量**: 2

### LibSVMOptions

**完整类名**: `org.apache.spark.ml.source.libsvm.LibSVMOptions`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | parameters: Map[String, String] | Unit | Scala方法 | `this(...)` |

---

### LibSVMOutputWriter

**完整类名**: `org.apache.spark.ml.source.libsvm.LibSVMOutputWriter`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | Unit | Scala方法 | `close(...)` |
| `shortName` | 无 | String | Scala方法 | `shortName(...)` |
| `inferSchema` | sparkSession: SparkSession,
      options: Map[String, String],
      files: Seq[FileStatus] | Option[StructType] | Scala方法 | `inferSchema(...)` |
| `prepareWrite` | sparkSession: SparkSession,
      job: Job,
      options: Map[String, String],
      dataSchema: StructType | OutputWriterFactory | Scala方法 | `prepareWrite(...)` |
| `newInstance` | path: String,
          dataSchema: StructType,
          context: TaskAttemptContext | OutputWriter | Scala方法 | `newInstance(...)` |
| `getFileExtension` | context: TaskAttemptContext | String | Scala方法 | `getFileExtension(...)` |
| `buildReader` | sparkSession: SparkSession,
      dataSchema: StructType,
      partitionSchema: StructType,
      requiredSchema: StructType,
      filters: Seq[Filter],
      options: Map[String, String],
      hadoopConf: Configuration |  | Scala方法 | `buildReader(...)` |

---

## 包: org.apache.spark.ml.stat

**类数量**: 7

### ANOVATest

**完整类名**: `org.apache.spark.ml.stat.ANOVATest`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `test` | dataset: DataFrame, featuresCol: String, labelCol: String | DataFrame | Scala方法 | `test(...)` |
| `test` | dataset: DataFrame,
      featuresCol: String,
      labelCol: String,
      flatten: Boolean | DataFrame | Scala方法 | `test(...)` |

---

### ChiSquareTest

**完整类名**: `org.apache.spark.ml.stat.ChiSquareTest`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `test` | dataset: DataFrame, featuresCol: String, labelCol: String | DataFrame | Scala方法 | `test(...)` |
| `test` | dataset: DataFrame,
      featuresCol: String,
      labelCol: String,
      flatten: Boolean | DataFrame | Scala方法 | `test(...)` |

---

### Correlation

**完整类名**: `org.apache.spark.ml.stat.Correlation`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `corr` | dataset: Dataset[_], column: String, method: String | DataFrame | Scala方法 | `corr(...)` |
| `corr` | dataset: Dataset[_], column: String | DataFrame | Scala方法 | `corr(...)` |

---

### FValueTest

**完整类名**: `org.apache.spark.ml.stat.FValueTest`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `test` | dataset: DataFrame, featuresCol: String, labelCol: String | DataFrame | Scala方法 | `test(...)` |
| `test` | dataset: DataFrame,
      featuresCol: String,
      labelCol: String,
      flatten: Boolean | DataFrame | Scala方法 | `test(...)` |

---

### KolmogorovSmirnovTest

**完整类名**: `org.apache.spark.ml.stat.KolmogorovSmirnovTest`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `test` | dataset: Dataset[_], sampleCol: String, cdf: Double => Double | DataFrame | Scala方法 | `test(...)` |
| `test` | dataset: Dataset[_],
      sampleCol: String,
      cdf: Function[java.lang.Double, java.lang.Double] | DataFrame | Scala方法 | `test(...)` |
| `test` | dataset: Dataset[_],
      sampleCol: String, distName: String,
      params: Double* | DataFrame | Scala方法 | `test(...)` |

---

### multi

**完整类名**: `org.apache.spark.ml.stat.multi`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | label: Double, weight: Double = 1.0 | MultiClassSummarizer | Scala方法 | `add(...)` |
| `merge` | other: MultiClassSummarizer | MultiClassSummarizer | Scala方法 | `merge(...)` |

---

### that

**完整类名**: `org.apache.spark.ml.stat.that`

**描述**: Scala定义的Java友好接口

**方法数**: 41

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `summary` | featuresCol: Column, weightCol: Column | Column | Scala方法 | `summary(...)` |
| `summary` | featuresCol: Column | Column | Scala方法 | `summary(...)` |
| `metrics` | metrics: String* | SummaryBuilder | Scala方法 | `metrics(...)` |
| `mean` | col: Column, weightCol: Column | Column | Scala方法 | `mean(...)` |
| `mean` | col: Column | Column | Scala方法 | `mean(...)` |
| `sum` | col: Column, weightCol: Column | Column | Scala方法 | `sum(...)` |
| `sum` | col: Column | Column | Scala方法 | `sum(...)` |
| `variance` | col: Column, weightCol: Column | Column | Scala方法 | `variance(...)` |
| `variance` | col: Column | Column | Scala方法 | `variance(...)` |
| `std` | col: Column, weightCol: Column | Column | Scala方法 | `std(...)` |
| `std` | col: Column | Column | Scala方法 | `std(...)` |
| `count` | col: Column, weightCol: Column | Column | Scala方法 | `count(...)` |
| `count` | col: Column | Column | Scala方法 | `count(...)` |
| `numNonZeros` | col: Column, weightCol: Column | Column | Scala方法 | `numNonZeros(...)` |
| `numNonZeros` | col: Column | Column | Scala方法 | `numNonZeros(...)` |
| `max` | col: Column, weightCol: Column | Column | Scala方法 | `max(...)` |
| `max` | col: Column | Column | Scala方法 | `max(...)` |
| `min` | col: Column, weightCol: Column | Column | Scala方法 | `min(...)` |
| `min` | col: Column | Column | Scala方法 | `min(...)` |
| `normL1` | col: Column, weightCol: Column | Column | Scala方法 | `normL1(...)` |
| `normL1` | col: Column | Column | Scala方法 | `normL1(...)` |
| `normL2` | col: Column, weightCol: Column | Column | Scala方法 | `normL2(...)` |
| `normL2` | col: Column | Column | Scala方法 | `normL2(...)` |
| `summary` | featuresCol: Column, weightCol: Column | Column | Scala方法 | `summary(...)` |
| `getRelevantMetrics` | requested: Seq[String] |  | Scala方法 | `getRelevantMetrics(...)` |
| `structureForMetrics` | metrics: Seq[Metric] | StructType | Scala方法 | `structureForMetrics(...)` |
| `this` | metrics: (Seq[Metric], Seq[ComputeMetric] | Unit | Scala方法 | `this(...)` |
| `this` | requestedMetrics: Expression,
        featuresExpr: Expression,
        weightExpr: Expression | Unit | Scala方法 | `this(...)` |
| `this` | requestedMetrics: Expression,
        featuresExpr: Expression | Unit | Scala方法 | `this(...)` |
| `eval` | state: SummarizerBuffer | Any | Scala方法 | `eval(...)` |
| `update` | state: SummarizerBuffer, row: InternalRow | SummarizerBuffer | Scala方法 | `update(...)` |
| `merge` | state: SummarizerBuffer,
      other: SummarizerBuffer | SummarizerBuffer | Scala方法 | `merge(...)` |
| `createAggregationBuffer` | 无 | SummarizerBuffer | Scala方法 | `createAggregationBuffer(...)` |
| `serialize` | state: SummarizerBuffer | Array[Byte] | Scala方法 | `serialize(...)` |
| `deserialize` | bytes: Array[Byte] | SummarizerBuffer | Scala方法 | `deserialize(...)` |
| `withNewMutableAggBufferOffset` | newMutableAggBufferOffset: Int | MetricsAggregate | Scala方法 | `withNewMutableAggBufferOffset(...)` |
| `withNewInputAggBufferOffset` | newInputAggBufferOffset: Int | MetricsAggregate | Scala方法 | `withNewInputAggBufferOffset(...)` |
| `add` | nonZeroIterator: Iterator[(Int, Double | Unit | Scala方法 | `add(...)` |
| `add` | instance: Vector, weight: Double | this | Scala方法 | `add(...)` |
| `add` | instance: Vector | this | Scala方法 | `add(...)` |
| `merge` | other: SummarizerBuffer | this | Scala方法 | `merge(...)` |

---

## 包: org.apache.spark.ml.tree

**类数量**: 4

### Node

**完整类名**: `org.apache.spark.ml.tree.Node`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromOld` | oldNode: OldNode, categoricalFeatures: Map[Int, Int] | Node | Scala方法 | `fromOld(...)` |
| `toNode` | prune: Boolean = true | Node | Scala方法 | `toNode(...)` |
| `predictImpl` | binnedFeatures: Array[Int], splits: Array[Array[Split]] | Int | Scala方法 | `predictImpl(...)` |
| `emptyNode` | nodeIndex: Int | LearningNode | Scala方法 | `emptyNode(...)` |
| `leftChildIndex` | nodeIndex: Int | Int | Scala方法 | `leftChildIndex(...)` |
| `rightChildIndex` | nodeIndex: Int | Int | Scala方法 | `rightChildIndex(...)` |
| `parentIndex` | nodeIndex: Int | Int | Scala方法 | `parentIndex(...)` |
| `indexToLevel` | nodeIndex: Int | Int | Scala方法 | `indexToLevel(...)` |
| `isLeftChild` | nodeIndex: Int | Boolean | Scala方法 | `isLeftChild(...)` |
| `maxNodesInLevel` | level: Int | Int | Scala方法 | `maxNodesInLevel(...)` |
| `startIndexInLevel` | level: Int | Int | Scala方法 | `startIndexInLevel(...)` |
| `getNode` | nodeIndex: Int, rootNode: LearningNode | LearningNode | Scala方法 | `getNode(...)` |

---

### Split

**完整类名**: `org.apache.spark.ml.tree.Split`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |

---

### TreeClassifierParams

**完整类名**: `org.apache.spark.ml.tree.TreeClassifierParams`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setLeafCol` | value: String | this | Scala方法 | `setLeafCol(...)` |

---

### TreeEnsembleModel

**完整类名**: `org.apache.spark.ml.tree.TreeEnsembleModel`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predictLeaf` | features: Vector | Double | Scala方法 | `predictLeaf(...)` |
| `getEstimatedSize` | 无 | Long | Scala方法 | `getEstimatedSize(...)` |
| `predictLeaf` | features: Vector | Vector | Scala方法 | `predictLeaf(...)` |
| `computeFeatureImportance` | node: Node,
      importances: OpenHashMap[Int, Double] | Unit | Scala方法 | `computeFeatureImportance(...)` |
| `normalizeMapValues` | map: OpenHashMap[Int, Double] | Unit | Scala方法 | `normalizeMapValues(...)` |
| `apply` | split: Split | SplitData | Scala方法 | `apply(...)` |
| `build` | node: Node, id: Int |  | Scala方法 | `build(...)` |
| `inferNumPartitions` | numNodes: Long | Int | Scala方法 | `inferNumPartitions(...)` |
| `loadTreeNodes` | path: String,
      metadata: DefaultParamsReader.Metadata,
      sparkSession: SparkSession | Node | Scala方法 | `loadTreeNodes(...)` |
| `buildTreeFromNodes` | data: Array[NodeData], impurityType: String | Node | Scala方法 | `buildTreeFromNodes(...)` |
| `loadImpl` | path: String,
      sparkSession: SparkSession,
      className: String,
      treeClassName: String |  | Scala方法 | `loadImpl(...)` |
| `build` | tree: DecisionTreeModel, treeID: Int | Seq[EnsembleNodeData] | Scala方法 | `build(...)` |

---

## 包: org.apache.spark.ml.tree.impl

**类数量**: 6

### DecisionTreeMetadata

**完整类名**: `org.apache.spark.ml.tree.impl.DecisionTreeMetadata`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isUnordered` | featureIndex: Int | Boolean | Scala方法 | `isUnordered(...)` |
| `isCategorical` | featureIndex: Int | Boolean | Scala方法 | `isCategorical(...)` |
| `isContinuous` | featureIndex: Int | Boolean | Scala方法 | `isContinuous(...)` |
| `numSplits` | featureIndex: Int | Int | Scala方法 | `numSplits(...)` |
| `setNumSplits` | featureIndex: Int, numSplits: Int | Unit | Scala方法 | `setNumSplits(...)` |
| `buildMetadata` | input: RDD[Instance],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String | DecisionTreeMetadata | Scala方法 | `buildMetadata(...)` |
| `buildMetadata` | input: RDD[Instance],
      strategy: Strategy | DecisionTreeMetadata | Scala方法 | `buildMetadata(...)` |
| `numUnorderedBins` | arity: Int | Int | Scala方法 | `numUnorderedBins(...)` |

---

### GradientBoostedTrees

**完整类名**: `org.apache.spark.ml.tree.impl.GradientBoostedTrees`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | input: RDD[Instance],
      boostingStrategy: OldBoostingStrategy,
      seed: Long,
      featureSubsetStrategy: String,
      instr: Option[Instrumentation] = None |  | Scala方法 | `run(...)` |
| `runWithValidation` | input: RDD[Instance],
      validationInput: RDD[Instance],
      boostingStrategy: OldBoostingStrategy,
      seed: Long,
      featureSubsetStrategy: String,
      instr: Option[Instrumentation] = None |  | Scala方法 | `runWithValidation(...)` |
| `computeInitialPredictionAndError` | data: RDD[TreePoint],
      initTreeWeight: Double,
      initTree: DecisionTreeRegressionModel,
      loss: OldLoss,
      bcSplits: Broadcast[Array[Array[Split]]] | RDD[ | Scala方法 | `computeInitialPredictionAndError(...)` |
| `updatePredictionError` | data: RDD[TreePoint],
      predictionAndError: RDD[(Double, Double | Unit | Scala方法 | `updatePredictionError(...)` |
| `updatePrediction` | treePoint: TreePoint,
      prediction: Double,
      tree: DecisionTreeRegressionModel,
      weight: Double,
      splits: Array[Array[Split]] | Double | Scala方法 | `updatePrediction(...)` |
| `updatePrediction` | features: Vector,
      prediction: Double,
      tree: DecisionTreeRegressionModel,
      weight: Double | Double | Scala方法 | `updatePrediction(...)` |
| `computeWeightedError` | data: RDD[Instance],
      trees: Array[DecisionTreeRegressionModel],
      treeWeights: Array[Double],
      loss: OldLoss | Double | Scala方法 | `computeWeightedError(...)` |
| `computeWeightedError` | data: RDD[TreePoint],
      predError: RDD[(Double, Double | Unit | Scala方法 | `computeWeightedError(...)` |
| `evaluateEachIteration` | data: RDD[Instance],
      trees: Array[DecisionTreeRegressionModel],
      treeWeights: Array[Double],
      loss: OldLoss,
      algo: OldAlgo.Value | Array[Double] | Scala方法 | `evaluateEachIteration(...)` |
| `boost` | input: RDD[Instance],
      validationInput: RDD[Instance],
      boostingStrategy: OldBoostingStrategy,
      validate: Boolean,
      seed: Long,
      featureSubsetStrategy: String,
      instr: Option[Instrumentation] = None |  | Scala方法 | `boost(...)` |

---

### RandomForest

**完整类名**: `org.apache.spark.ml.tree.impl.RandomForest`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | input: RDD[LabeledPoint],
      strategy: OldStrategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Long | Array[DecisionTreeModel] | Scala方法 | `run(...)` |
| `runBagged` | baggedInput: RDD[BaggedPoint[TreePoint]],
      metadata: DecisionTreeMetadata,
      bcSplits: Broadcast[Array[Array[Split]]],
      strategy: OldStrategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Long,
      instr: Option[Instrumentation],
      prune: Boolean = true, // exposed for testing only, real trees are always pruned
      parentUID: Option[String] = None,
      earlyStopModelSizeThresholdInBytes: Long = 0 | Array[DecisionTreeModel] | Scala方法 | `runBagged(...)` |
| `run` | input: RDD[Instance],
      strategy: OldStrategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Long,
      instr: Option[Instrumentation],
      prune: Boolean = true, // exposed for testing only, real trees are always pruned
      parentUID: Option[String] = None | Array[DecisionTreeModel] | Scala方法 | `run(...)` |
| `nodeBinSeqOp` | treeIndex: Int,
        nodeInfo: NodeIndexInfo,
        agg: Array[DTStatsAggregator],
        baggedPoint: BaggedPoint[TreePoint],
        splits: Array[Array[Split]] | Unit | Scala方法 | `nodeBinSeqOp(...)` |
| `binSeqOp` | agg: Array[DTStatsAggregator],
        baggedPoint: BaggedPoint[TreePoint],
        splits: Array[Array[Split]] | Array[DTStatsAggregator] | Scala方法 | `binSeqOp(...)` |
| `binSeqOpWithNodeIdCache` | agg: Array[DTStatsAggregator],
        dataPoint: (BaggedPoint[TreePoint], Array[Int] | Unit | Scala方法 | `binSeqOpWithNodeIdCache(...)` |
| `getNodeToFeatures` | treeToNodeToIndexInfo: Map[Int, Map[Int, NodeIndexInfo]] | Option[Map[Int, Array[Int]]] | Scala方法 | `getNodeToFeatures(...)` |

---

### TimeTracker

**完整类名**: `org.apache.spark.ml.tree.impl.TimeTracker`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `start` | timerLabel: String | Unit | Scala方法 | `start(...)` |
| `stop` | timerLabel: String | Double | Scala方法 | `stop(...)` |

---

### classification

**完整类名**: `org.apache.spark.ml.tree.impl.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertToTreeRDD` | input: RDD[Instance],
      splits: Array[Array[Split]],
      metadata: DecisionTreeMetadata | RDD[TreePoint] | Scala方法 | `convertToTreeRDD(...)` |

---

### is

**完整类名**: `org.apache.spark.ml.tree.impl.is`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getImpurityCalculator` | featureOffset: Int, binIndex: Int | ImpurityCalculator | Scala方法 | `getImpurityCalculator(...)` |
| `getParentImpurityCalculator` | 无 | ImpurityCalculator | Scala方法 | `getParentImpurityCalculator(...)` |
| `update` | featureIndex: Int,
      binIndex: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double | Unit | Scala方法 | `update(...)` |
| `updateParent` | label: Double, numSamples: Int, sampleWeight: Double | Unit | Scala方法 | `updateParent(...)` |
| `featureUpdate` | featureOffset: Int,
      binIndex: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double | Unit | Scala方法 | `featureUpdate(...)` |
| `getFeatureOffset` | featureIndex: Int | Int | Scala方法 | `getFeatureOffset(...)` |
| `mergeForFeature` | featureOffset: Int, binIndex: Int, otherBinIndex: Int | Unit | Scala方法 | `mergeForFeature(...)` |
| `merge` | other: DTStatsAggregator | DTStatsAggregator | Scala方法 | `merge(...)` |

---

## 包: org.apache.spark.ml.tuning

**类数量**: 4

### CrossValidator

**完整类名**: `org.apache.spark.ml.tuning.CrossValidator`

**描述**: Scala定义的Java友好接口

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setEstimator` | value: Estimator[_] | this | Scala方法 | `setEstimator(...)` |
| `setEstimatorParamMaps` | value: Array[ParamMap] | this | Scala方法 | `setEstimatorParamMaps(...)` |
| `setEvaluator` | value: Evaluator | this | Scala方法 | `setEvaluator(...)` |
| `setNumFolds` | value: Int | this | Scala方法 | `setNumFolds(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setFoldCol` | value: String | this | Scala方法 | `setFoldCol(...)` |
| `setParallelism` | value: Int | this | Scala方法 | `setParallelism(...)` |
| `setCollectSubModels` | value: Boolean | this | Scala方法 | `setCollectSubModels(...)` |
| `fit` | dataset: Dataset[_] | CrossValidatorModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | CrossValidator | Scala方法 | `copy(...)` |
| `load` | path: String | CrossValidator | Scala方法 | `load(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | CrossValidatorModel | Scala方法 | `copy(...)` |
| `load` | path: String | CrossValidatorModel | Scala方法 | `load(...)` |

---

### ParamGridBuilder

**完整类名**: `org.apache.spark.ml.tuning.ParamGridBuilder`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `baseOn` | paramMap: ParamMap | this | Scala方法 | `baseOn(...)` |
| `baseOn` | paramPairs: ParamPair[_]* | this | Scala方法 | `baseOn(...)` |
| `addGrid` | param: DoubleParam, values: Array[Double] | this | Scala方法 | `addGrid(...)` |
| `addGrid` | param: IntParam, values: Array[Int] | this | Scala方法 | `addGrid(...)` |
| `addGrid` | param: FloatParam, values: Array[Float] | this | Scala方法 | `addGrid(...)` |
| `addGrid` | param: LongParam, values: Array[Long] | this | Scala方法 | `addGrid(...)` |
| `addGrid` | param: BooleanParam | this | Scala方法 | `addGrid(...)` |
| `build` | 无 | Array[ParamMap] | Scala方法 | `build(...)` |

---

### TrainValidationSplit

**完整类名**: `org.apache.spark.ml.tuning.TrainValidationSplit`

**描述**: Scala定义的Java友好接口

**方法数**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setEstimator` | value: Estimator[_] | this | Scala方法 | `setEstimator(...)` |
| `setEstimatorParamMaps` | value: Array[ParamMap] | this | Scala方法 | `setEstimatorParamMaps(...)` |
| `setEvaluator` | value: Evaluator | this | Scala方法 | `setEvaluator(...)` |
| `setTrainRatio` | value: Double | this | Scala方法 | `setTrainRatio(...)` |
| `setSeed` | value: Long | this | Scala方法 | `setSeed(...)` |
| `setParallelism` | value: Int | this | Scala方法 | `setParallelism(...)` |
| `setCollectSubModels` | value: Boolean | this | Scala方法 | `setCollectSubModels(...)` |
| `fit` | dataset: Dataset[_] | TrainValidationSplitModel | Scala方法 | `fit(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | TrainValidationSplit | Scala方法 | `copy(...)` |
| `load` | path: String | TrainValidationSplit | Scala方法 | `load(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |
| `copy` | extra: ParamMap | TrainValidationSplitModel | Scala方法 | `copy(...)` |
| `load` | path: String | TrainValidationSplitModel | Scala方法 | `load(...)` |

---

### ValidatorParams

**完整类名**: `org.apache.spark.ml.tuning.ValidatorParams`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `validateParams` | instance: ValidatorParams | Unit | Scala方法 | `validateParams(...)` |
| `checkElement` | elem: Params, name: String | Unit | Scala方法 | `checkElement(...)` |
| `saveImpl` | path: String,
      instance: ValidatorParams,
      spark: SparkSession,
      extraMetadata: Option[JObject] = None | Unit | Scala方法 | `saveImpl(...)` |

---

## 包: org.apache.spark.ml.util

**类数量**: 8

### ConnectHelper

**完整类名**: `org.apache.spark.ml.util.ConnectHelper`

**描述**: Scala定义的Java友好接口

**方法数**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | session: SparkSession | Unit | Scala方法 | `this(...)` |
| `handleOverwrite` | path: String, shouldOverwrite: Boolean | Boolean | Scala方法 | `handleOverwrite(...)` |
| `stringIndexerModelFromLabels` | uid: String, labels: Array[String] | StringIndexerModel | Scala方法 | `stringIndexerModelFromLabels(...)` |
| `stringIndexerModelFromLabelsArray` | uid: String, labelsArray: Array[Array[String]] | StringIndexerModel | Scala方法 | `stringIndexerModelFromLabelsArray(...)` |
| `countVectorizerModelFromVocabulary` | uid: String, vocabulary: Array[String] | CountVectorizerModel | Scala方法 | `countVectorizerModelFromVocabulary(...)` |
| `stopWordsRemoverLoadDefaultStopWords` | language: String | Array[String] | Scala方法 | `stopWordsRemoverLoadDefaultStopWords(...)` |
| `chiSquareTest` | dataset: DataFrame,
      featuresCol: String,
      labelCol: String,
      flatten: Boolean | DataFrame | Scala方法 | `chiSquareTest(...)` |
| `correlation` | dataset: DataFrame,
      column: String,
      method: String | DataFrame | Scala方法 | `correlation(...)` |
| `kolmogorovSmirnovTest` | dataset: DataFrame,
      sampleCol: String,
      distName: String,
      params: Array[Double] | DataFrame | Scala方法 | `kolmogorovSmirnovTest(...)` |
| `powerIterationClusteringAssignClusters` | dataset: DataFrame,
      k: Int,
      maxIter: Int,
      initMode: String,
      srcCol: String,
      dstCol: String,
      weightCol: String | DataFrame | Scala方法 | `powerIterationClusteringAssignClusters(...)` |
| `prefixSpanFindFrequentSequentialPatterns` | dataset: DataFrame,
      minSupport: Double,
      maxPatternLength: Int,
      maxLocalProjDBSize: Long,
      sequenceCol: String | DataFrame | Scala方法 | `prefixSpanFindFrequentSequentialPatterns(...)` |
| `copy` | extra: ParamMap | ConnectHelper | Scala方法 | `copy(...)` |
| `transform` | dataset: Dataset[_] | DataFrame | Scala方法 | `transform(...)` |
| `transformSchema` | schema: StructType | StructType | Scala方法 | `transformSchema(...)` |

---

### DatasetUtils

**完整类名**: `org.apache.spark.ml.util.DatasetUtils`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `columnToVector` | dataset: Dataset[_], colName: String | Column | Scala方法 | `columnToVector(...)` |
| `columnToOldVector` | dataset: Dataset[_], colName: String | RDD[OldVector] | Scala方法 | `columnToOldVector(...)` |

---

### Instrumentation

**完整类名**: `org.apache.spark.ml.util.Instrumentation`

**描述**: Scala定义的Java友好接口

**方法数**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `logPipelineStage` | stage: PipelineStage | Unit | Scala方法 | `logPipelineStage(...)` |
| `logDataset` | dataset: Dataset[_] | Unit | Scala方法 | `logDataset(...)` |
| `logDataset` | dataset: RDD[_] | Unit | Scala方法 | `logDataset(...)` |
| `logDebug` | msg: => String | Unit | Scala方法 | `logDebug(...)` |
| `logWarning` | msg: => String | Unit | Scala方法 | `logWarning(...)` |
| `logWarning` | entry: LogEntry | Unit | Scala方法 | `logWarning(...)` |
| `logError` | msg: => String | Unit | Scala方法 | `logError(...)` |
| `logError` | entry: LogEntry | Unit | Scala方法 | `logError(...)` |
| `logInfo` | msg: => String | Unit | Scala方法 | `logInfo(...)` |
| `logInfo` | entry: LogEntry | Unit | Scala方法 | `logInfo(...)` |
| `logParams` | hasParams: Params, params: Param[_]* | Unit | Scala方法 | `logParams(...)` |
| `logNumFeatures` | num: Long | Unit | Scala方法 | `logNumFeatures(...)` |
| `logNumClasses` | num: Long | Unit | Scala方法 | `logNumClasses(...)` |
| `logNumExamples` | num: Long | Unit | Scala方法 | `logNumExamples(...)` |
| `logSumOfWeights` | num: Double | Unit | Scala方法 | `logSumOfWeights(...)` |
| `logNamedValue` | name: String, value: String | Unit | Scala方法 | `logNamedValue(...)` |
| `logNamedValue` | name: String, value: Long | Unit | Scala方法 | `logNamedValue(...)` |
| `logNamedValue` | name: String, value: Double | Unit | Scala方法 | `logNamedValue(...)` |
| `logNamedValue` | name: String, value: Array[String] | Unit | Scala方法 | `logNamedValue(...)` |
| `logNamedValue` | name: String, value: Array[Long] | Unit | Scala方法 | `logNamedValue(...)` |
| `logNamedValue` | name: String, value: Array[Double] | Unit | Scala方法 | `logNamedValue(...)` |
| `logSuccess` | 无 | Unit | Scala方法 | `logSuccess(...)` |
| `logFailure` | e: Throwable | Unit | Scala方法 | `logFailure(...)` |
| `logInfo` | logEntry: LogEntry | Unit | Scala方法 | `logInfo(...)` |
| `logWarning` | msg: => String | Unit | Scala方法 | `logWarning(...)` |
| `logError` | msg: => String | Unit | Scala方法 | `logError(...)` |
| `create` | instr: Instrumentation | OptionalInstrumentation | Scala方法 | `create(...)` |
| `create` | clazz: Class[_] | OptionalInstrumentation | Scala方法 | `create(...)` |

---

### MetadataUtils

**完整类名**: `org.apache.spark.ml.util.MetadataUtils`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNumClasses` | labelSchema: StructField | Option[Int] | Scala方法 | `getNumClasses(...)` |
| `getNumFeatures` | vectorSchema: StructField | Option[Int] | Scala方法 | `getNumFeatures(...)` |
| `getCategoricalFeatures` | featuresSchema: StructField | Map[Int, Int] | Scala方法 | `getCategoricalFeatures(...)` |
| `getFeatureIndicesFromNames` | col: StructField, names: Array[String] | Array[Int] | Scala方法 | `getFeatureIndicesFromNames(...)` |

---

### SchemaUtils

**完整类名**: `org.apache.spark.ml.util.SchemaUtils`

**描述**: Scala定义的Java友好接口

**方法数**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkColumnType` | schema: StructType,
      colName: String,
      dataType: DataType,
      msg: String = "" | Unit | Scala方法 | `checkColumnType(...)` |
| `checkColumnTypes` | schema: StructType,
      colName: String,
      dataTypes: Seq[DataType],
      msg: String = "" | Unit | Scala方法 | `checkColumnTypes(...)` |
| `checkNumericType` | schema: StructType,
      colName: String,
      msg: String = "" | Unit | Scala方法 | `checkNumericType(...)` |
| `appendColumn` | schema: StructType,
      colName: String,
      dataType: DataType,
      nullable: Boolean = false | StructType | Scala方法 | `appendColumn(...)` |
| `appendColumn` | schema: StructType, col: StructField | StructType | Scala方法 | `appendColumn(...)` |
| `updateAttributeGroupSize` | schema: StructType,
      colName: String,
      size: Int | StructType | Scala方法 | `updateAttributeGroupSize(...)` |
| `updateNumValues` | schema: StructType,
      colName: String,
      numValues: Int | StructType | Scala方法 | `updateNumValues(...)` |
| `updateNumeric` | schema: StructType,
      colName: String | StructType | Scala方法 | `updateNumeric(...)` |
| `updateField` | schema: StructType,
      field: StructField,
      overwriteMetadata: Boolean = true | StructType | Scala方法 | `updateField(...)` |
| `validateVectorCompatibleColumn` | schema: StructType, colName: String | Unit | Scala方法 | `validateVectorCompatibleColumn(...)` |
| `toSQLId` | parts: String | String | Scala方法 | `toSQLId(...)` |
| `getSchemaField` | schema: StructType, colName: String | StructField | Scala方法 | `getSchemaField(...)` |
| `getSchemaFieldType` | schema: StructType, colName: String | DataType | Scala方法 | `getSchemaFieldType(...)` |
| `checkSchemaFieldExist` | schema: StructType, colName: String | Boolean | Scala方法 | `checkSchemaFieldExist(...)` |

---

### for

**完整类名**: `org.apache.spark.ml.util.for`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `start` | 无 | Unit | Scala方法 | `start(...)` |
| `stop` | 无 | Long | Scala方法 | `stop(...)` |
| `elapsed` | 无 | Long

  override def toString | Scala方法 | `elapsed(...)` |
| `elapsed` | 无 | Long | Scala方法 | `elapsed(...)` |
| `addLocal` | name: String | this | Scala方法 | `addLocal(...)` |
| `addDistributed` | name: String | this | Scala方法 | `addDistributed(...)` |
| `apply` | name: String | Stopwatch | Scala方法 | `apply(...)` |

---

### to

**完整类名**: `org.apache.spark.ml.util.to`

**描述**: Scala定义的Java友好接口

**方法数**: 52

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `session` | sparkSession: SparkSession | this | Scala方法 | `session(...)` |
| `write` | path: String, session: SparkSession, optionMap: mutable.Map[String, String],
    stage: PipelineStage | Unit | Scala方法 | `write(...)` |
| `format` | 无 | String | Scala方法 | `format(...)` |
| `format` | 无 | String | Scala方法 | `format(...)` |
| `stageName` | 无 | String | Scala方法 | `stageName(...)` |
| `stageName` | 无 | String

  private[ml] def shortName | Scala方法 | `stageName(...)` |
| `save` | path: String | Unit | Scala方法 | `save(...)` |
| `overwrite` | 无 | this | Scala方法 | `overwrite(...)` |
| `option` | key: String, value: String | this | Scala方法 | `option(...)` |
| `session` | sparkSession: SparkSession | this | Scala方法 | `session(...)` |
| `format` | source: String | this | Scala方法 | `format(...)` |
| `session` | sparkSession: SparkSession | this | Scala方法 | `session(...)` |
| `save` | path: String | Unit | Scala方法 | `save(...)` |
| `load` | path: String | T | Scala方法 | `load(...)` |
| `session` | sparkSession: SparkSession | this | Scala方法 | `session(...)` |
| `load` | path: String | T | Scala方法 | `load(...)` |
| `saveMetadata` | instance: Params,
      path: String,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None,
      paramMap: Option[JValue] = None | Unit | Scala方法 | `saveMetadata(...)` |
| `saveMetadata` | instance: Params,
      path: String,
      spark: SparkSession,
      extraMetadata: Option[JObject],
      paramMap: Option[JValue] | Unit | Scala方法 | `saveMetadata(...)` |
| `saveMetadata` | instance: Params,
      path: String,
      spark: SparkSession,
      extraMetadata: Option[JObject] | Unit | Scala方法 | `saveMetadata(...)` |
| `saveMetadata` | instance: Params, path: String, spark: SparkSession | Unit | Scala方法 | `saveMetadata(...)` |
| `getMetadataToSave` | instance: Params,
      sc: SparkContext,
      extraMetadata: Option[JObject] = None,
      paramMap: Option[JValue] = None | String | Scala方法 | `getMetadataToSave(...)` |
| `getMetadataToSave` | instance: Params,
      spark: SparkSession,
      extraMetadata: Option[JObject],
      paramMap: Option[JValue] | String | Scala方法 | `getMetadataToSave(...)` |
| `getMetadataToSave` | instance: Params,
      spark: SparkSession,
      extraMetadata: Option[JObject] | String | Scala方法 | `getMetadataToSave(...)` |
| `getMetadataToSave` | instance: Params,
      spark: SparkSession | String | Scala方法 | `getMetadataToSave(...)` |
| `load` | className: String | Class[_] | Scala方法 | `load(...)` |
| `getParamValue` | paramName: String | JValue | Scala方法 | `getParamValue(...)` |
| `getAndSetParams` | instance: Params,
        skipParams: Option[List[String]] = None | Unit | Scala方法 | `getAndSetParams(...)` |
| `loadMetadata` | path: String, sc: SparkContext, expectedClassName: String = "" | Metadata | Scala方法 | `loadMetadata(...)` |
| `loadMetadata` | path: String, spark: SparkSession, expectedClassName: String | Metadata | Scala方法 | `loadMetadata(...)` |
| `loadMetadata` | path: String, spark: SparkSession | Metadata | Scala方法 | `loadMetadata(...)` |
| `parseMetadata` | metadataStr: String, expectedClassName: String = "" | Metadata | Scala方法 | `parseMetadata(...)` |
| `getUidMap` | instance: Params | Map[String, Params] | Scala方法 | `getUidMap(...)` |
| `serializeIntArray` | array: Array[Int], dos: DataOutputStream | Unit | Scala方法 | `serializeIntArray(...)` |
| `deserializeIntArray` | dis: DataInputStream | Array[Int] | Scala方法 | `deserializeIntArray(...)` |
| `serializeLongArray` | array: Array[Long], dos: DataOutputStream | Unit | Scala方法 | `serializeLongArray(...)` |
| `deserializeLongArray` | dis: DataInputStream | Array[Long] | Scala方法 | `deserializeLongArray(...)` |
| `serializeFloatArray` | array: Array[Float], dos: DataOutputStream | Unit | Scala方法 | `serializeFloatArray(...)` |
| `deserializeFloatArray` | dis: DataInputStream | Array[Float] | Scala方法 | `deserializeFloatArray(...)` |
| `serializeDoubleArray` | array: Array[Double], dos: DataOutputStream | Unit | Scala方法 | `serializeDoubleArray(...)` |
| `deserializeDoubleArray` | dis: DataInputStream | Array[Double] | Scala方法 | `deserializeDoubleArray(...)` |
| `serializeStringArray` | array: Array[String], dos: DataOutputStream | Unit | Scala方法 | `serializeStringArray(...)` |
| `deserializeStringArray` | dis: DataInputStream | Array[String] | Scala方法 | `deserializeStringArray(...)` |
| `serializeVector` | vector: Vector, dos: DataOutputStream | Unit | Scala方法 | `serializeVector(...)` |
| `deserializeVector` | dis: DataInputStream | Vector | Scala方法 | `deserializeVector(...)` |
| `serializeMatrix` | matrix: Matrix, dos: DataOutputStream | Unit | Scala方法 | `serializeMatrix(...)` |
| `serializeCommon` | 无 | Unit | Scala方法 | `serializeCommon(...)` |
| `deserializeMatrix` | dis: DataInputStream | Matrix | Scala方法 | `deserializeMatrix(...)` |
| `deserializeCommon` | 无 |  | Scala方法 | `deserializeCommon(...)` |
| `saveText` | path: String, data: String, spark: SparkSession | Unit | Scala方法 | `saveText(...)` |
| `loadText` | path: String, spark: SparkSession | String | Scala方法 | `loadText(...)` |
| `saveDataFrame` | path: String, df: DataFrame | Unit | Scala方法 | `saveDataFrame(...)` |
| `loadDataFrame` | path: String, spark: SparkSession | DataFrame | Scala方法 | `loadDataFrame(...)` |

---

### with

**完整类名**: `org.apache.spark.ml.util.with`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `randomUID` | prefix: String | String | Scala方法 | `randomUID(...)` |

---

## 包: org.apache.spark.mllib.api.python

**类数量**: 5

### GaussianMixtureModelWrapper

**完整类名**: `org.apache.spark.mllib.api.python.GaussianMixtureModelWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predictSoft` | point: Vector | Vector | Scala方法 | `predictSoft(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |

---

### LDAModelWrapper

**完整类名**: `org.apache.spark.mllib.api.python.LDAModelWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `vocabSize` | 无 | Int | Scala方法 | `vocabSize(...)` |
| `describeTopics` | 无 | Array[Byte] | Scala方法 | `describeTopics(...)` |
| `describeTopics` | maxTermsPerTopic: Int | Array[Byte] | Scala方法 | `describeTopics(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |

---

### MatrixFactorizationModelWrapper

**完整类名**: `org.apache.spark.mllib.api.python.MatrixFactorizationModelWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | userAndProducts: JavaRDD[Array[Any]] | RDD[Rating] | Scala方法 | `predict(...)` |
| `wrappedRecommendProductsForUsers` | num: Int | RDD[Array[Any]] | Scala方法 | `wrappedRecommendProductsForUsers(...)` |
| `wrappedRecommendUsersForProducts` | num: Int | RDD[Array[Any]] | Scala方法 | `wrappedRecommendUsersForProducts(...)` |

---

### PythonMLLibAPI

**完整类名**: `org.apache.spark.mllib.api.python.PythonMLLibAPI`

**描述**: Scala定义的Java友好接口

**方法数**: 84

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `loadLabeledPoints` | jsc: JavaSparkContext,
      path: String,
      minPartitions: Int | JavaRDD[LabeledPoint] | Scala方法 | `loadLabeledPoints(...)` |
| `loadVectors` | jsc: JavaSparkContext, path: String | RDD[Vector] | Scala方法 | `loadVectors(...)` |
| `getUpdaterFromString` | regType: String | Updater | Scala方法 | `getUpdaterFromString(...)` |
| `trainBisectingKMeans` | data: JavaRDD[Vector],
      k: Int,
      maxIterations: Int,
      minDivisibleClusterSize: Double,
      seed: java.lang.Long | BisectingKMeansModel | Scala方法 | `trainBisectingKMeans(...)` |
| `trainLinearRegressionModelWithSGD` | data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      regParam: Double,
      regType: String,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double | JList[Object] | Scala方法 | `trainLinearRegressionModelWithSGD(...)` |
| `trainLassoModelWithSGD` | data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double | JList[Object] | Scala方法 | `trainLassoModelWithSGD(...)` |
| `trainRidgeModelWithSGD` | data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double | JList[Object] | Scala方法 | `trainRidgeModelWithSGD(...)` |
| `trainSVMModelWithSGD` | data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      regType: String,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double | JList[Object] | Scala方法 | `trainSVMModelWithSGD(...)` |
| `trainLogisticRegressionModelWithSGD` | data: JavaRDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      miniBatchFraction: Double,
      initialWeights: Vector,
      regParam: Double,
      regType: String,
      intercept: Boolean,
      validateData: Boolean,
      convergenceTol: Double | JList[Object] | Scala方法 | `trainLogisticRegressionModelWithSGD(...)` |
| `trainLogisticRegressionModelWithLBFGS` | data: JavaRDD[LabeledPoint],
      numIterations: Int,
      initialWeights: Vector,
      regParam: Double,
      regType: String,
      intercept: Boolean,
      corrections: Int,
      tolerance: Double,
      validateData: Boolean,
      numClasses: Int | JList[Object] | Scala方法 | `trainLogisticRegressionModelWithLBFGS(...)` |
| `trainNaiveBayesModel` | data: JavaRDD[LabeledPoint],
      lambda: Double | JList[Object] | Scala方法 | `trainNaiveBayesModel(...)` |
| `trainIsotonicRegressionModel` | data: JavaRDD[Vector],
      isotonic: Boolean | JList[Object] | Scala方法 | `trainIsotonicRegressionModel(...)` |
| `trainKMeansModel` | data: JavaRDD[Vector],
      k: Int,
      maxIterations: Int,
      initializationMode: String,
      seed: java.lang.Long,
      initializationSteps: Int,
      epsilon: Double,
      initialModel: java.util.ArrayList[Vector],
      distanceMeasure: String | KMeansModel | Scala方法 | `trainKMeansModel(...)` |
| `computeCostKmeansModel` | data: JavaRDD[Vector],
      centers: java.util.ArrayList[Vector] | Double | Scala方法 | `computeCostKmeansModel(...)` |
| `trainGaussianMixtureModel` | data: JavaRDD[Vector],
      k: Int,
      convergenceTol: Double,
      maxIterations: Int,
      seed: java.lang.Long,
      initialModelWeights: java.util.ArrayList[Double],
      initialModelMu: java.util.ArrayList[Vector],
      initialModelSigma: java.util.ArrayList[Matrix] | GaussianMixtureModelWrapper | Scala方法 | `trainGaussianMixtureModel(...)` |
| `predictSoftGMM` | data: JavaRDD[Vector],
      wt: Vector,
      mu: Array[Object],
      si: Array[Object] | RDD[Vector] | Scala方法 | `predictSoftGMM(...)` |
| `trainPowerIterationClusteringModel` | data: JavaRDD[Vector],
      k: Int,
      maxIterations: Int,
      initMode: String | PowerIterationClusteringModel | Scala方法 | `trainPowerIterationClusteringModel(...)` |
| `trainALSModel` | ratingsJRDD: JavaRDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      nonnegative: Boolean,
      seed: java.lang.Long | MatrixFactorizationModel | Scala方法 | `trainALSModel(...)` |
| `trainImplicitALSModel` | ratingsJRDD: JavaRDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      alpha: Double,
      nonnegative: Boolean,
      seed: java.lang.Long | MatrixFactorizationModel | Scala方法 | `trainImplicitALSModel(...)` |
| `trainLDAModel` | data: JavaRDD[java.util.List[Any]],
      k: Int,
      maxIterations: Int,
      docConcentration: Double,
      topicConcentration: Double,
      seed: java.lang.Long,
      checkpointInterval: Int,
      optimizer: String | LDAModelWrapper | Scala方法 | `trainLDAModel(...)` |
| `loadLDAModel` | jsc: JavaSparkContext, path: String | LDAModelWrapper | Scala方法 | `loadLDAModel(...)` |
| `trainFPGrowthModel` | data: JavaRDD[java.lang.Iterable[Any]],
      minSupport: Double,
      numPartitions: Int | FPGrowthModel[Any] | Scala方法 | `trainFPGrowthModel(...)` |
| `trainPrefixSpanModel` | data: JavaRDD[java.util.ArrayList[java.util.ArrayList[Any]]],
      minSupport: Double,
      maxPatternLength: Int,
      localProjDBSize: Int | PrefixSpanModelWrapper | Scala方法 | `trainPrefixSpanModel(...)` |
| `normalizeVector` | p: Double, vector: Vector | Vector | Scala方法 | `normalizeVector(...)` |
| `normalizeVector` | p: Double, rdd: JavaRDD[Vector] | JavaRDD[Vector] | Scala方法 | `normalizeVector(...)` |
| `fitStandardScaler` | withMean: Boolean,
      withStd: Boolean,
      data: JavaRDD[Vector] | StandardScalerModel | Scala方法 | `fitStandardScaler(...)` |
| `fitChiSqSelector` | selectorType: String,
      numTopFeatures: Int,
      percentile: Double,
      fpr: Double,
      fdr: Double,
      fwe: Double,
      data: JavaRDD[LabeledPoint] | ChiSqSelectorModel | Scala方法 | `fitChiSqSelector(...)` |
| `fitPCA` | k: Int, data: JavaRDD[Vector] | PCAModel | Scala方法 | `fitPCA(...)` |
| `fitIDF` | minDocFreq: Int, dataset: JavaRDD[Vector] | IDFModel | Scala方法 | `fitIDF(...)` |
| `trainWord2VecModel` | dataJRDD: JavaRDD[java.util.ArrayList[String]],
      vectorSize: Int,
      learningRate: Double,
      numPartitions: Int,
      numIterations: Int,
      seed: java.lang.Long,
      minCount: Int,
      windowSize: Int | Word2VecModelWrapper | Scala方法 | `trainWord2VecModel(...)` |
| `trainDecisionTreeModel` | data: JavaRDD[LabeledPoint],
      algoStr: String,
      numClasses: Int,
      categoricalFeaturesInfo: JMap[Int, Int],
      impurityStr: String,
      maxDepth: Int,
      maxBins: Int,
      minInstancesPerNode: Int,
      minInfoGain: Double | DecisionTreeModel | Scala方法 | `trainDecisionTreeModel(...)` |
| `trainRandomForestModel` | data: JavaRDD[LabeledPoint],
      algoStr: String,
      numClasses: Int,
      categoricalFeaturesInfo: JMap[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurityStr: String,
      maxDepth: Int,
      maxBins: Int,
      seed: java.lang.Long | RandomForestModel | Scala方法 | `trainRandomForestModel(...)` |
| `trainGradientBoostedTreesModel` | data: JavaRDD[LabeledPoint],
      algoStr: String,
      categoricalFeaturesInfo: JMap[Int, Int],
      lossStr: String,
      numIterations: Int,
      learningRate: Double,
      maxDepth: Int,
      maxBins: Int | GradientBoostedTreesModel | Scala方法 | `trainGradientBoostedTreesModel(...)` |
| `elementwiseProductVector` | scalingVector: Vector, vector: Vector | Vector | Scala方法 | `elementwiseProductVector(...)` |
| `elementwiseProductVector` | scalingVector: Vector, vector: JavaRDD[Vector] | JavaRDD[Vector] | Scala方法 | `elementwiseProductVector(...)` |
| `colStats` | rdd: JavaRDD[Vector] | MultivariateStatisticalSummary | Scala方法 | `colStats(...)` |
| `corr` | x: JavaRDD[Vector], method: String | Matrix | Scala方法 | `corr(...)` |
| `corr` | x: JavaRDD[Double], y: JavaRDD[Double], method: String | Double | Scala方法 | `corr(...)` |
| `chiSqTest` | observed: Vector, expected: Vector | ChiSqTestResult | Scala方法 | `chiSqTest(...)` |
| `chiSqTest` | observed: Matrix | ChiSqTestResult | Scala方法 | `chiSqTest(...)` |
| `chiSqTest` | data: JavaRDD[LabeledPoint] | Array[ChiSqTestResult] | Scala方法 | `chiSqTest(...)` |
| `uniformRDD` | jsc: JavaSparkContext,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Double] | Scala方法 | `uniformRDD(...)` |
| `normalRDD` | jsc: JavaSparkContext,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Double] | Scala方法 | `normalRDD(...)` |
| `logNormalRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Double] | Scala方法 | `logNormalRDD(...)` |
| `poissonRDD` | jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Double] | Scala方法 | `poissonRDD(...)` |
| `exponentialRDD` | jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Double] | Scala方法 | `exponentialRDD(...)` |
| `gammaRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Double] | Scala方法 | `gammaRDD(...)` |
| `uniformVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Vector] | Scala方法 | `uniformVectorRDD(...)` |
| `normalVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Vector] | Scala方法 | `normalVectorRDD(...)` |
| `logNormalVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Vector] | Scala方法 | `logNormalVectorRDD(...)` |
| `poissonVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Vector] | Scala方法 | `poissonVectorRDD(...)` |
| `exponentialVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Vector] | Scala方法 | `exponentialVectorRDD(...)` |
| `gammaVectorRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: java.lang.Integer,
      seed: java.lang.Long | JavaRDD[Vector] | Scala方法 | `gammaVectorRDD(...)` |
| `newRankingMetrics` | predictionAndLabels: DataFrame | RankingMetrics[Any] | Scala方法 | `newRankingMetrics(...)` |
| `estimateKernelDensity` | sample: JavaRDD[Double],
      bandwidth: Double, points: java.util.ArrayList[Double] | Array[Double] | Scala方法 | `estimateKernelDensity(...)` |
| `updateStreamingKMeansModel` | clusterCenters: JList[Vector],
      clusterWeights: JList[Double],
      data: JavaRDD[Vector],
      decayFactor: Double,
      timeUnit: String | JList[Object] | Scala方法 | `updateStreamingKMeansModel(...)` |
| `generateLinearInputWrapper` | intercept: Double,
      weights: JList[Double],
      xMean: JList[Double],
      xVariance: JList[Double],
      nPoints: Int,
      seed: Int,
      eps: Double | Array[LabeledPoint] | Scala方法 | `generateLinearInputWrapper(...)` |
| `generateLinearRDDWrapper` | sc: JavaSparkContext,
      nexamples: Int,
      nfeatures: Int,
      eps: Double,
      nparts: Int,
      intercept: Double | JavaRDD[LabeledPoint] | Scala方法 | `generateLinearRDDWrapper(...)` |
| `kolmogorovSmirnovTest` | data: JavaRDD[Double],
      distName: String,
      params: JList[Double] | KolmogorovSmirnovTestResult | Scala方法 | `kolmogorovSmirnovTest(...)` |
| `createRowMatrix` | rows: JavaRDD[Vector], numRows: Long, numCols: Int | RowMatrix | Scala方法 | `createRowMatrix(...)` |
| `createRowMatrix` | df: DataFrame, numRows: Long, numCols: Int | RowMatrix | Scala方法 | `createRowMatrix(...)` |
| `createIndexedRowMatrix` | rows: DataFrame, numRows: Long, numCols: Int | IndexedRowMatrix | Scala方法 | `createIndexedRowMatrix(...)` |
| `createCoordinateMatrix` | rows: DataFrame, numRows: Long, numCols: Long | CoordinateMatrix | Scala方法 | `createCoordinateMatrix(...)` |
| `createBlockMatrix` | blocks: DataFrame, rowsPerBlock: Int, colsPerBlock: Int,
                        numRows: Long, numCols: Long | BlockMatrix | Scala方法 | `createBlockMatrix(...)` |
| `getIndexedRows` | indexedRowMatrix: IndexedRowMatrix | DataFrame | Scala方法 | `getIndexedRows(...)` |
| `getMatrixEntries` | coordinateMatrix: CoordinateMatrix | DataFrame | Scala方法 | `getMatrixEntries(...)` |
| `getMatrixBlocks` | blockMatrix: BlockMatrix | DataFrame | Scala方法 | `getMatrixBlocks(...)` |
| `convertVectorColumnsToML` | dataset: DataFrame, cols: JArrayList[String] | DataFrame | Scala方法 | `convertVectorColumnsToML(...)` |
| `convertVectorColumnsFromML` | dataset: DataFrame, cols: JArrayList[String] | DataFrame | Scala方法 | `convertVectorColumnsFromML(...)` |
| `convertMatrixColumnsToML` | dataset: DataFrame, cols: JArrayList[String] | DataFrame | Scala方法 | `convertMatrixColumnsToML(...)` |
| `convertMatrixColumnsFromML` | dataset: DataFrame, cols: JArrayList[String] | DataFrame | Scala方法 | `convertMatrixColumnsFromML(...)` |
| `pickle` | obj: Object, out: OutputStream, pickler: Pickler | Unit | Scala方法 | `pickle(...)` |
| `loads` | bytes: Array[Byte] | AnyRef | Scala方法 | `loads(...)` |
| `asTupleRDD` | rdd: RDD[Array[Any]] | RDD[ | Scala方法 | `asTupleRDD(...)` |
| `fromTuple2RDD` | rdd: RDD[(Any, Any | Unit | Scala方法 | `fromTuple2RDD(...)` |
| `javaToPython` | jRDD: JavaRDD[Any] | JavaRDD[Array[Byte]] | Scala方法 | `javaToPython(...)` |
| `pythonToJava` | pyRDD: JavaRDD[Array[Byte]], batched: Boolean | JavaRDD[Any] | Scala方法 | `pythonToJava(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `construct` | args: Array[Object] | Object | Scala方法 | `construct(...)` |
| `initialize` | 无 | Unit | Scala方法 | `initialize(...)` |

---

### Word2VecModelWrapper

**完整类名**: `org.apache.spark.mllib.api.python.Word2VecModelWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | rdd: JavaRDD[String] | JavaRDD[Vector] | Scala方法 | `transform(...)` |
| `findSynonyms` | word: String, num: Int | JList[Object] | Scala方法 | `findSynonyms(...)` |
| `findSynonyms` | vector: Vector, num: Int | JList[Object] | Scala方法 | `findSynonyms(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |

---

## 包: org.apache.spark.mllib.classification

**类数量**: 5

### ClassificationModel

**完整类名**: `org.apache.spark.mllib.classification.ClassificationModel`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | testData: RDD[Vector] | RDD[Double] | Scala方法 | `predict(...)` |
| `predict` | testData: Vector | Double | Scala方法 | `predict(...)` |
| `predict` | testData: JavaRDD[Vector] | JavaRDD[java | Scala方法 | `predict(...)` |
| `getNumFeaturesClasses` | metadata: JValue |  | Scala方法 | `getNumFeaturesClasses(...)` |

---

### LogisticRegressionModel

**完整类名**: `org.apache.spark.mllib.classification.LogisticRegressionModel`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | weights: Vector, intercept: Double | Unit | Scala方法 | `this(...)` |
| `setThreshold` | threshold: Double | this | Scala方法 | `setThreshold(...)` |
| `clearThreshold` | 无 | this | Scala方法 | `clearThreshold(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | LogisticRegressionModel | Scala方法 | `load(...)` |
| `setNumClasses` | numClasses: Int | this | Scala方法 | `setNumClasses(...)` |
| `run` | input: RDD[LabeledPoint] | LogisticRegressionModel | Scala方法 | `run(...)` |
| `run` | input: RDD[LabeledPoint], initialWeights: Vector | LogisticRegressionModel | Scala方法 | `run(...)` |
| `runWithMlLogisticRegression` | elasticNetParam: Double | Unit | Scala方法 | `runWithMlLogisticRegression(...)` |

---

### SVMModel

**完整类名**: `org.apache.spark.mllib.classification.SVMModel`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setThreshold` | threshold: Double | this | Scala方法 | `setThreshold(...)` |
| `clearThreshold` | 无 | this | Scala方法 | `clearThreshold(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | SVMModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `train` | input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double,
      initialWeights: Vector | SVMModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double,
      miniBatchFraction: Double | SVMModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint],
      numIterations: Int,
      stepSize: Double,
      regParam: Double | SVMModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint], numIterations: Int | SVMModel | Scala方法 | `train(...)` |

---

### StreamingLogisticRegressionWithSGD

**完整类名**: `org.apache.spark.mllib.classification.StreamingLogisticRegressionWithSGD`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setStepSize` | stepSize: Double | this | Scala方法 | `setStepSize(...)` |
| `setNumIterations` | numIterations: Int | this | Scala方法 | `setNumIterations(...)` |
| `setMiniBatchFraction` | miniBatchFraction: Double | this | Scala方法 | `setMiniBatchFraction(...)` |
| `setRegParam` | regParam: Double | this | Scala方法 | `setRegParam(...)` |
| `setInitialWeights` | initialWeights: Vector | this | Scala方法 | `setInitialWeights(...)` |

---

### priors

**完整类名**: `org.apache.spark.mllib.classification.priors`

**描述**: Scala定义的Java友好接口

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | testData: RDD[Vector] | RDD[Double] | Scala方法 | `predict(...)` |
| `predict` | testData: Vector | Double | Scala方法 | `predict(...)` |
| `predictProbabilities` | testData: RDD[Vector] | RDD[Vector] | Scala方法 | `predictProbabilities(...)` |
| `predictProbabilities` | testData: Vector | Vector | Scala方法 | `predictProbabilities(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `save` | sc: SparkContext, path: String, data: Data | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | NaiveBayesModel | Scala方法 | `load(...)` |
| `save` | sc: SparkContext, path: String, data: Data | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | NaiveBayesModel | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | NaiveBayesModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setLambda` | lambda: Double | NaiveBayes | Scala方法 | `setLambda(...)` |
| `setModelType` | modelType: String | NaiveBayes | Scala方法 | `setModelType(...)` |
| `run` | data: RDD[LabeledPoint] | NaiveBayesModel | Scala方法 | `run(...)` |
| `train` | input: RDD[LabeledPoint] | NaiveBayesModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint], lambda: Double | NaiveBayesModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint], lambda: Double, modelType: String | NaiveBayesModel | Scala方法 | `train(...)` |

---

## 包: org.apache.spark.mllib.classification.impl

**类数量**: 1

### for

**完整类名**: `org.apache.spark.mllib.classification.impl.for`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext,
        path: String,
        modelClass: String,
        numFeatures: Int,
        numClasses: Int,
        weights: Vector,
        intercept: Double,
        threshold: Option[Double] | Unit | Scala方法 | `save(...)` |
| `loadData` | sc: SparkContext, path: String, modelClass: String | Data | Scala方法 | `loadData(...)` |

---

## 包: org.apache.spark.mllib.clustering

**类数量**: 13

### BisectingKMeansModel

**完整类名**: `org.apache.spark.mllib.clustering.BisectingKMeansModel`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | root: ClusteringTreeNode | Unit | Scala方法 | `this(...)` |
| `predict` | point: Vector | Int | Scala方法 | `predict(...)` |
| `predict` | points: RDD[Vector] | RDD[Int] | Scala方法 | `predict(...)` |
| `predict` | points: JavaRDD[Vector] | JavaRDD[java | Scala方法 | `predict(...)` |
| `computeCost` | point: Vector | Double | Scala方法 | `computeCost(...)` |
| `computeCost` | data: RDD[Vector] | Double | Scala方法 | `computeCost(...)` |
| `computeCost` | data: JavaRDD[Vector] | Double | Scala方法 | `computeCost(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | BisectingKMeansModel | Scala方法 | `load(...)` |
| `save` | sc: SparkContext, model: BisectingKMeansModel, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | BisectingKMeansModel | Scala方法 | `load(...)` |
| `save` | sc: SparkContext, model: BisectingKMeansModel, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | BisectingKMeansModel | Scala方法 | `load(...)` |
| `save` | sc: SparkContext, model: BisectingKMeansModel, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | BisectingKMeansModel | Scala方法 | `load(...)` |

---

### DistanceMeasure

**完整类名**: `org.apache.spark.mllib.clustering.DistanceMeasure`

**描述**: Scala定义的Java友好接口

**方法数**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `computeStatistics` | distance: Double | Double | Scala方法 | `computeStatistics(...)` |
| `computeStatistics` | centers: Array[VectorWithNorm] | Array[Double] | Scala方法 | `computeStatistics(...)` |
| `computeStatisticsDistributedly` | sc: SparkContext,
      bcCenters: Broadcast[Array[VectorWithNorm]] | Array[Double] | Scala方法 | `computeStatisticsDistributedly(...)` |
| `findClosest` | centers: Array[VectorWithNorm],
      statistics: Option[Array[Double]],
      point: VectorWithNorm |  | Scala方法 | `findClosest(...)` |
| `findClosest` | centers: Array[VectorWithNorm],
      statistics: Array[Double],
      point: VectorWithNorm |  | Scala方法 | `findClosest(...)` |
| `findClosest` | centers: Array[VectorWithNorm],
      point: VectorWithNorm |  | Scala方法 | `findClosest(...)` |
| `pointCost` | centers: Array[VectorWithNorm],
      point: VectorWithNorm | Double | Scala方法 | `pointCost(...)` |
| `isCenterConverged` | oldCenter: VectorWithNorm,
      newCenter: VectorWithNorm,
      epsilon: Double | Boolean | Scala方法 | `isCenterConverged(...)` |
| `distance` | v1: VectorWithNorm,
      v2: VectorWithNorm | Double | Scala方法 | `distance(...)` |
| `clusterCost` | centroid: VectorWithNorm,
      pointsSum: VectorWithNorm,
      weightSum: Double,
      pointsSquaredNorm: Double | Double | Scala方法 | `clusterCost(...)` |
| `updateClusterSum` | point: VectorWithNorm, sum: Vector | Unit | Scala方法 | `updateClusterSum(...)` |
| `centroid` | sum: Vector, weightSum: Double | VectorWithNorm | Scala方法 | `centroid(...)` |
| `symmetricCentroids` | level: Double,
      noise: Vector,
      centroid: Vector |  | Scala方法 | `symmetricCentroids(...)` |
| `cost` | point: VectorWithNorm,
      centroid: VectorWithNorm | Double | Scala方法 | `cost(...)` |
| `computeStatistics` | distance: Double | Double | Scala方法 | `computeStatistics(...)` |
| `findClosest` | centers: Array[VectorWithNorm],
      statistics: Array[Double],
      point: VectorWithNorm |  | Scala方法 | `findClosest(...)` |
| `findClosest` | centers: Array[VectorWithNorm],
      point: VectorWithNorm |  | Scala方法 | `findClosest(...)` |
| `isCenterConverged` | oldCenter: VectorWithNorm,
      newCenter: VectorWithNorm,
      epsilon: Double | Boolean | Scala方法 | `isCenterConverged(...)` |
| `distance` | v1: VectorWithNorm, v2: VectorWithNorm | Double | Scala方法 | `distance(...)` |
| `clusterCost` | centroid: VectorWithNorm,
      pointsSum: VectorWithNorm,
      weightSum: Double,
      pointsSquaredNorm: Double | Double | Scala方法 | `clusterCost(...)` |
| `cost` | point: VectorWithNorm,
      centroid: VectorWithNorm | Double | Scala方法 | `cost(...)` |
| `computeStatistics` | distance: Double | Double | Scala方法 | `computeStatistics(...)` |
| `findClosest` | centers: Array[VectorWithNorm],
      statistics: Array[Double],
      point: VectorWithNorm |  | Scala方法 | `findClosest(...)` |
| `distance` | v1: VectorWithNorm, v2: VectorWithNorm | Double | Scala方法 | `distance(...)` |
| `updateClusterSum` | point: VectorWithNorm, sum: Vector | Unit | Scala方法 | `updateClusterSum(...)` |
| `centroid` | sum: Vector, weightSum: Double | VectorWithNorm | Scala方法 | `centroid(...)` |
| `clusterCost` | centroid: VectorWithNorm,
      pointsSum: VectorWithNorm,
      weightSum: Double,
      pointsSquaredNorm: Double | Double | Scala方法 | `clusterCost(...)` |
| `symmetricCentroids` | level: Double,
      noise: Vector,
      centroid: Vector |  | Scala方法 | `symmetricCentroids(...)` |

---

### GaussianMixtureModel

**完整类名**: `org.apache.spark.mllib.clustering.GaussianMixtureModel`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `predict` | points: RDD[Vector] | RDD[Int] | Scala方法 | `predict(...)` |
| `predict` | point: Vector | Int | Scala方法 | `predict(...)` |
| `predict` | points: JavaRDD[Vector] | JavaRDD[java | Scala方法 | `predict(...)` |
| `predictSoft` | points: RDD[Vector] | RDD[Array[Double]] | Scala方法 | `predictSoft(...)` |
| `predictSoft` | point: Vector | Array[Double] | Scala方法 | `predictSoft(...)` |
| `save` | sc: SparkContext,
        path: String,
        weights: Array[Double],
        gaussians: Array[MultivariateGaussian] | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | GaussianMixtureModel | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | GaussianMixtureModel | Scala方法 | `load(...)` |

---

### KMeans

**完整类名**: `org.apache.spark.mllib.clustering.KMeans`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setK` | k: Int | this | Scala方法 | `setK(...)` |
| `setMaxIterations` | maxIterations: Int | this | Scala方法 | `setMaxIterations(...)` |
| `setInitializationMode` | initializationMode: String | this | Scala方法 | `setInitializationMode(...)` |
| `setInitializationSteps` | initializationSteps: Int | this | Scala方法 | `setInitializationSteps(...)` |
| `setEpsilon` | epsilon: Double | this | Scala方法 | `setEpsilon(...)` |
| `setSeed` | seed: Long | this | Scala方法 | `setSeed(...)` |
| `setDistanceMeasure` | distanceMeasure: String | this | Scala方法 | `setDistanceMeasure(...)` |
| `setInitialModel` | model: KMeansModel | this | Scala方法 | `setInitialModel(...)` |
| `run` | data: RDD[Vector] | KMeansModel | Scala方法 | `run(...)` |
| `train` | data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      initializationMode: String,
      seed: Long | KMeansModel | Scala方法 | `train(...)` |
| `train` | data: RDD[Vector],
      k: Int,
      maxIterations: Int,
      initializationMode: String | KMeansModel | Scala方法 | `train(...)` |
| `train` | data: RDD[Vector],
      k: Int,
      maxIterations: Int | KMeansModel | Scala方法 | `train(...)` |
| `this` | vector: Vector | Unit | Scala方法 | `this(...)` |
| `this` | array: Array[Double] | Unit | Scala方法 | `this(...)` |

---

### KMeansModel

**完整类名**: `org.apache.spark.mllib.clustering.KMeansModel`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | clusterCenters: Array[Vector] | Unit | Scala方法 | `this(...)` |
| `this` | centers: java.lang.Iterable[Vector] | Unit | Scala方法 | `this(...)` |
| `predict` | point: Vector | Int | Scala方法 | `predict(...)` |
| `predict` | points: RDD[Vector] | RDD[Int] | Scala方法 | `predict(...)` |
| `predict` | points: JavaRDD[Vector] | JavaRDD[java | Scala方法 | `predict(...)` |
| `computeCost` | data: RDD[Vector] | Double | Scala方法 | `computeCost(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | KMeansModel | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | KMeansModel | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | KMeansModel | Scala方法 | `load(...)` |

---

### LDA

**完整类名**: `org.apache.spark.mllib.clustering.LDA`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setK` | k: Int | this | Scala方法 | `setK(...)` |
| `setDocConcentration` | docConcentration: Vector | this | Scala方法 | `setDocConcentration(...)` |
| `setDocConcentration` | docConcentration: Double | this | Scala方法 | `setDocConcentration(...)` |
| `setAlpha` | alpha: Vector | this | Scala方法 | `setAlpha(...)` |
| `setAlpha` | alpha: Double | this | Scala方法 | `setAlpha(...)` |
| `setTopicConcentration` | topicConcentration: Double | this | Scala方法 | `setTopicConcentration(...)` |
| `setBeta` | beta: Double | this | Scala方法 | `setBeta(...)` |
| `setMaxIterations` | maxIterations: Int | this | Scala方法 | `setMaxIterations(...)` |
| `setSeed` | seed: Long | this | Scala方法 | `setSeed(...)` |
| `setCheckpointInterval` | checkpointInterval: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `setOptimizer` | optimizer: LDAOptimizer | this | Scala方法 | `setOptimizer(...)` |
| `setOptimizer` | optimizerName: String | this | Scala方法 | `setOptimizer(...)` |
| `run` | documents: RDD[(Long, Vector | Unit | Scala方法 | `run(...)` |
| `run` | documents: JavaPairRDD[java.lang.Long, Vector] | LDAModel | Scala方法 | `run(...)` |

---

### LDAModel

**完整类名**: `org.apache.spark.mllib.clustering.LDAModel`

**描述**: Scala定义的Java友好接口

**方法数**: 29

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `describeTopics` | maxTermsPerTopic: Int | Array[ | Scala方法 | `describeTopics(...)` |
| `describeTopics` | 无 | Array[ | Scala方法 | `describeTopics(...)` |
| `describeTopicsAsStrings` | maxTermsPerTopic: Int | Array[ | Scala方法 | `describeTopicsAsStrings(...)` |
| `describeTopicsAsStrings` | 无 | Array[ | Scala方法 | `describeTopicsAsStrings(...)` |
| `logLikelihood` | documents: RDD[(Long, Vector | Unit | Scala方法 | `logLikelihood(...)` |
| `topicDistributions` | documents: RDD[(Long, Vector | Unit | Scala方法 | `topicDistributions(...)` |
| `describeTopics` | maxTermsPerTopic: Int | Array[ | Scala方法 | `describeTopics(...)` |
| `setSeed` | seed: Long | this | Scala方法 | `setSeed(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `logLikelihood` | documents: RDD[(Long, Vector | Unit | Scala方法 | `logLikelihood(...)` |
| `logLikelihood` | documents: JavaPairRDD[java.lang.Long, Vector] | Double | Scala方法 | `logLikelihood(...)` |
| `logPerplexity` | documents: RDD[(Long, Vector | Unit | Scala方法 | `logPerplexity(...)` |
| `logPerplexity` | documents: JavaPairRDD[java.lang.Long, Vector] | Double | Scala方法 | `logPerplexity(...)` |
| `topicDistributions` | documents: RDD[(Long, Vector | Unit | Scala方法 | `topicDistributions(...)` |
| `topicDistribution` | document: Vector | Vector | Scala方法 | `topicDistribution(...)` |
| `topicDistributions` | documents: JavaPairRDD[java.lang.Long, Vector] | JavaPairRDD[java | Scala方法 | `topicDistributions(...)` |
| `save` | sc: SparkContext,
        path: String,
        topicsMatrix: Matrix,
        docConcentration: Vector,
        topicConcentration: Double,
        gammaShape: Double | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext,
        path: String,
        docConcentration: Vector,
        topicConcentration: Double,
        gammaShape: Double | LocalLDAModel | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | LocalLDAModel | Scala方法 | `load(...)` |
| `describeTopics` | maxTermsPerTopic: Int | Array[ | Scala方法 | `describeTopics(...)` |
| `topDocumentsPerTopic` | maxDocumentsPerTopic: Int | Array[ | Scala方法 | `topDocumentsPerTopic(...)` |
| `logLikelihood` | documents: RDD[(Long, Vector | Unit | Scala方法 | `logLikelihood(...)` |
| `topTopicsPerDocument` | k: Int | RDD[ | Scala方法 | `topTopicsPerDocument(...)` |
| `javaTopTopicsPerDocument` | k: Int | JavaRDD[ | Scala方法 | `javaTopTopicsPerDocument(...)` |
| `topicDistributions` | documents: RDD[(Long, Vector | Unit | Scala方法 | `topicDistributions(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `save` | sc: SparkContext,
        path: String,
        graph: Graph[LDA.TopicCounts, LDA.TokenCount],
        globalTopicTotals: LDA.TopicCounts,
        k: Int,
        vocabSize: Int,
        docConcentration: Vector,
        topicConcentration: Double,
        iterationTimes: Array[Double],
        gammaShape: Double | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext,
        path: String,
        vocabSize: Int,
        docConcentration: Vector,
        topicConcentration: Double,
        iterationTimes: Array[Double],
        gammaShape: Double | DistributedLDAModel | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | DistributedLDAModel | Scala方法 | `load(...)` |

---

### PowerIterationClusteringModel

**完整类名**: `org.apache.spark.mllib.clustering.PowerIterationClusteringModel`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | PowerIterationClusteringModel | Scala方法 | `load(...)` |
| `save` | sc: SparkContext, model: PowerIterationClusteringModel, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | PowerIterationClusteringModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setK` | k: Int | this | Scala方法 | `setK(...)` |
| `setMaxIterations` | maxIterations: Int | this | Scala方法 | `setMaxIterations(...)` |
| `setInitializationMode` | mode: String | this | Scala方法 | `setInitializationMode(...)` |
| `run` | graph: Graph[Double, Double] | PowerIterationClusteringModel | Scala方法 | `run(...)` |
| `run` | similarities: RDD[(Long, Long, Double | Unit | Scala方法 | `run(...)` |
| `run` | similarities: JavaRDD[(java.lang.Long, java.lang.Long, java.lang.Double | Unit | Scala方法 | `run(...)` |

---

### StreamingKMeansModel

**完整类名**: `org.apache.spark.mllib.clustering.StreamingKMeansModel`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `update` | data: RDD[Vector], decayFactor: Double, timeUnit: String | StreamingKMeansModel | Scala方法 | `update(...)` |
| `mergeContribs` | p1: (Vector, Long | Unit | Scala方法 | `mergeContribs(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setK` | k: Int | this | Scala方法 | `setK(...)` |
| `setDecayFactor` | a: Double | this | Scala方法 | `setDecayFactor(...)` |
| `setHalfLife` | halfLife: Double, timeUnit: String | this | Scala方法 | `setHalfLife(...)` |
| `setInitialCenters` | centers: Array[Vector], weights: Array[Double] | this | Scala方法 | `setInitialCenters(...)` |
| `setRandomCenters` | dim: Int, weight: Double, seed: Long = Utils.random.nextLong | this | Scala方法 | `setRandomCenters(...)` |
| `latestModel` | 无 | StreamingKMeansModel | Scala方法 | `latestModel(...)` |
| `trainOn` | data: DStream[Vector] | Unit | Scala方法 | `trainOn(...)` |
| `trainOn` | data: JavaDStream[Vector] | Unit | Scala方法 | `trainOn(...)` |
| `predictOn` | data: DStream[Vector] | DStream[Int] | Scala方法 | `predictOn(...)` |
| `predictOn` | data: JavaDStream[Vector] | JavaDStream[java | Scala方法 | `predictOn(...)` |

---

### implements

**完整类名**: `org.apache.spark.mllib.clustering.implements`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setKeepLastCheckpoint` | keepLastCheckpoint: Boolean | this | Scala方法 | `setKeepLastCheckpoint(...)` |
| `setTau0` | tau0: Double | this | Scala方法 | `setTau0(...)` |
| `setKappa` | kappa: Double | this | Scala方法 | `setKappa(...)` |
| `setMiniBatchFraction` | miniBatchFraction: Double | this | Scala方法 | `setMiniBatchFraction(...)` |
| `setOptimizeDocConcentration` | optimizeDocConcentration: Boolean | this | Scala方法 | `setOptimizeDocConcentration(...)` |
| `elementWiseSum` | u: (BDM[Double], Option[BDV[Double]], Long | Unit | Scala方法 | `elementWiseSum(...)` |

---

### name

**完整类名**: `org.apache.spark.mllib.clustering.name`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setK` | k: Int | this | Scala方法 | `setK(...)` |
| `setMaxIterations` | maxIterations: Int | this | Scala方法 | `setMaxIterations(...)` |
| `setMinDivisibleClusterSize` | minDivisibleClusterSize: Double | this | Scala方法 | `setMinDivisibleClusterSize(...)` |
| `setSeed` | seed: Long | this | Scala方法 | `setSeed(...)` |
| `setDistanceMeasure` | distanceMeasure: String | this | Scala方法 | `setDistanceMeasure(...)` |
| `run` | input: RDD[Vector] | BisectingKMeansModel | Scala方法 | `run(...)` |
| `run` | data: JavaRDD[Vector] | BisectingKMeansModel | Scala方法 | `run(...)` |
| `merge` | other: ClusterSummaryAggregator | this | Scala方法 | `merge(...)` |
| `buildSubTree` | rawIndex: Long | ClusteringTreeNode | Scala方法 | `buildSubTree(...)` |
| `predict` | point: Vector, distanceMeasure: DistanceMeasure | Int | Scala方法 | `predict(...)` |
| `predictPath` | point: Vector, distanceMeasure: DistanceMeasure | Array[ClusteringTreeNode] | Scala方法 | `predictPath(...)` |
| `computeCost` | point: Vector, distanceMeasure: DistanceMeasure | Double | Scala方法 | `computeCost(...)` |

---

### performs

**完整类名**: `org.apache.spark.mllib.clustering.performs`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setInitialModel` | model: GaussianMixtureModel | this | Scala方法 | `setInitialModel(...)` |
| `setK` | k: Int | this | Scala方法 | `setK(...)` |
| `setMaxIterations` | maxIterations: Int | this | Scala方法 | `setMaxIterations(...)` |
| `setConvergenceTol` | convergenceTol: Double | this | Scala方法 | `setConvergenceTol(...)` |
| `setSeed` | seed: Long | this | Scala方法 | `setSeed(...)` |
| `run` | data: RDD[Vector] | GaussianMixtureModel | Scala方法 | `run(...)` |
| `run` | data: JavaRDD[Vector] | GaussianMixtureModel | Scala方法 | `run(...)` |
| `shouldDistributeGaussians` | k: Int, d: Int | Boolean | Scala方法 | `shouldDistributeGaussians(...)` |
| `add` | weights: Array[Double],
      dists: Array[MultivariateGaussian] | Unit | Scala方法 | `add(...)` |

---

### to

**完整类名**: `org.apache.spark.mllib.clustering.to`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `kMeansPlusPlus` | seed: Int,
      points: Array[VectorWithNorm],
      weights: Array[Double],
      k: Int,
      maxIterations: Int | Array[VectorWithNorm] | Scala方法 | `kMeansPlusPlus(...)` |

---

## 包: org.apache.spark.mllib.evaluation

**类数量**: 6

### AreaUnderCurve

**完整类名**: `org.apache.spark.mllib.evaluation.AreaUnderCurve`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `of` | curve: RDD[(Double, Double | Unit | Scala方法 | `of(...)` |
| `of` | curve: Iterable[(Double, Double | Unit | Scala方法 | `of(...)` |

---

### BinaryClassificationMetrics

**完整类名**: `org.apache.spark.mllib.evaluation.BinaryClassificationMetrics`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | scoreAndLabels: RDD[(Double, Double | Unit | Scala方法 | `this(...)` |
| `unpersist` | 无 | Unit | Scala方法 | `unpersist(...)` |
| `thresholds` | 无 | RDD[Double] | Scala方法 | `thresholds(...)` |
| `roc` | 无 | RDD[ | Scala方法 | `roc(...)` |
| `areaUnderROC` | 无 | Double | Scala方法 | `areaUnderROC(...)` |
| `pr` | 无 | RDD[ | Scala方法 | `pr(...)` |
| `areaUnderPR` | 无 | Double | Scala方法 | `areaUnderPR(...)` |
| `fMeasureByThreshold` | beta: Double | RDD[ | Scala方法 | `fMeasureByThreshold(...)` |
| `fMeasureByThreshold` | 无 | RDD[ | Scala方法 | `fMeasureByThreshold(...)` |
| `precisionByThreshold` | 无 | RDD[ | Scala方法 | `precisionByThreshold(...)` |
| `recallByThreshold` | 无 | RDD[ | Scala方法 | `recallByThreshold(...)` |

---

### MultilabelMetrics

**完整类名**: `org.apache.spark.mllib.evaluation.MultilabelMetrics`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `precision` | label: Double | Double | Scala方法 | `precision(...)` |
| `recall` | label: Double | Double | Scala方法 | `recall(...)` |
| `f1Measure` | label: Double | Double | Scala方法 | `f1Measure(...)` |
| `add` | predictions: Array[Double], labels: Array[Double] | this | Scala方法 | `add(...)` |
| `merge` | other: MultilabelSummarizer | this | Scala方法 | `merge(...)` |

---

### RankingMetrics

**完整类名**: `org.apache.spark.mllib.evaluation.RankingMetrics`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `precisionAt` | k: Int | Double | Scala方法 | `precisionAt(...)` |
| `meanAveragePrecisionAt` | k: Int | Double | Scala方法 | `meanAveragePrecisionAt(...)` |
| `ndcgAt` | k: Int | Double | Scala方法 | `ndcgAt(...)` |
| `recallAt` | k: Int | Double | Scala方法 | `recallAt(...)` |

---

### RegressionMetrics

**完整类名**: `org.apache.spark.mllib.evaluation.RegressionMetrics`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | predictionAndObservations: RDD[_ <: Product] | Unit | Scala方法 | `this(...)` |

---

### classification

**完整类名**: `org.apache.spark.mllib.evaluation.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `truePositiveRate` | label: Double | Double | Scala方法 | `truePositiveRate(...)` |
| `falsePositiveRate` | label: Double | Double | Scala方法 | `falsePositiveRate(...)` |
| `precision` | label: Double | Double | Scala方法 | `precision(...)` |
| `recall` | label: Double | Double | Scala方法 | `recall(...)` |
| `fMeasure` | label: Double, beta: Double | Double | Scala方法 | `fMeasure(...)` |
| `fMeasure` | label: Double | Double | Scala方法 | `fMeasure(...)` |
| `weightedFMeasure` | beta: Double | Double | Scala方法 | `weightedFMeasure(...)` |
| `logLoss` | eps: Double = 1e-15 | Double | Scala方法 | `logLoss(...)` |

---

## 包: org.apache.spark.mllib.evaluation.binary

**类数量**: 1

### Precision

**完整类名**: `org.apache.spark.mllib.evaluation.binary.Precision`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | c: BinaryConfusionMatrix | Double | Scala方法 | `apply(...)` |

---

## 包: org.apache.spark.mllib.feature

**类数量**: 8

### ChiSqSelectorModel

**完整类名**: `org.apache.spark.mllib.feature.ChiSqSelectorModel`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | vector: Vector | Vector | Scala方法 | `transform(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | ChiSqSelectorModel | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | ChiSqSelectorModel | Scala方法 | `load(...)` |
| `this` | numTopFeatures: Int | Unit | Scala方法 | `this(...)` |
| `setNumTopFeatures` | value: Int | this | Scala方法 | `setNumTopFeatures(...)` |
| `setPercentile` | value: Double | this | Scala方法 | `setPercentile(...)` |
| `setFpr` | value: Double | this | Scala方法 | `setFpr(...)` |
| `setFdr` | value: Double | this | Scala方法 | `setFdr(...)` |
| `setFwe` | value: Double | this | Scala方法 | `setFwe(...)` |
| `setSelectorType` | value: String | this | Scala方法 | `setSelectorType(...)` |
| `fit` | data: RDD[LabeledPoint] | ChiSqSelectorModel | Scala方法 | `fit(...)` |

---

### ElementwiseProduct

**完整类名**: `org.apache.spark.mllib.feature.ElementwiseProduct`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | vector: Vector | Vector | Scala方法 | `transform(...)` |

---

### HashingTF

**完整类名**: `org.apache.spark.mllib.feature.HashingTF`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setBinary` | value: Boolean | this | Scala方法 | `setBinary(...)` |
| `setHashAlgorithm` | value: String | this | Scala方法 | `setHashAlgorithm(...)` |
| `indexOf` | term: Any | Int | Scala方法 | `indexOf(...)` |
| `transform` | document: Iterable[_] | Vector | Scala方法 | `transform(...)` |
| `transform` | document: JavaIterable[_] | Vector | Scala方法 | `transform(...)` |

---

### IDF

**完整类名**: `org.apache.spark.mllib.feature.IDF`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `fit` | dataset: RDD[Vector] | IDFModel | Scala方法 | `fit(...)` |
| `fit` | dataset: JavaRDD[Vector] | IDFModel | Scala方法 | `fit(...)` |
| `merge` | other: DocumentFrequencyAggregator | this | Scala方法 | `merge(...)` |
| `idf` | 无 |  | Scala方法 | `idf(...)` |
| `transform` | dataset: RDD[Vector] | RDD[Vector] | Scala方法 | `transform(...)` |
| `transform` | v: Vector | Vector | Scala方法 | `transform(...)` |
| `transform` | dataset: JavaRDD[Vector] | JavaRDD[Vector] | Scala方法 | `transform(...)` |
| `transform` | idf: Vector, v: Vector | Vector | Scala方法 | `transform(...)` |

---

### Normalizer

**完整类名**: `org.apache.spark.mllib.feature.Normalizer`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `transform` | vector: Vector | Vector | Scala方法 | `transform(...)` |

---

### PCA

**完整类名**: `org.apache.spark.mllib.feature.PCA`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fit` | sources: RDD[Vector] | PCAModel | Scala方法 | `fit(...)` |
| `fit` | sources: JavaRDD[Vector] | PCAModel | Scala方法 | `fit(...)` |
| `transform` | vector: Vector | Vector | Scala方法 | `transform(...)` |
| `memoryCost` | k: Int, numFeatures: Int | Long | Scala方法 | `memoryCost(...)` |

---

### StandardScaler

**完整类名**: `org.apache.spark.mllib.feature.StandardScaler`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `fit` | data: RDD[Vector] | StandardScalerModel | Scala方法 | `fit(...)` |
| `this` | std: Vector, mean: Vector | Unit | Scala方法 | `this(...)` |
| `this` | std: Vector | Unit | Scala方法 | `this(...)` |
| `setWithMean` | withMean: Boolean | this | Scala方法 | `setWithMean(...)` |
| `setWithStd` | withStd: Boolean | this | Scala方法 | `setWithStd(...)` |
| `transform` | vector: Vector | Vector | Scala方法 | `transform(...)` |

---

### VocabWord

**完整类名**: `org.apache.spark.mllib.feature.VocabWord`

**描述**: Scala定义的Java友好接口

**方法数**: 18

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setMaxSentenceLength` | maxSentenceLength: Int | this | Scala方法 | `setMaxSentenceLength(...)` |
| `setVectorSize` | vectorSize: Int | this | Scala方法 | `setVectorSize(...)` |
| `setLearningRate` | learningRate: Double | this | Scala方法 | `setLearningRate(...)` |
| `setNumPartitions` | numPartitions: Int | this | Scala方法 | `setNumPartitions(...)` |
| `setNumIterations` | numIterations: Int | this | Scala方法 | `setNumIterations(...)` |
| `setSeed` | seed: Long | this | Scala方法 | `setSeed(...)` |
| `setWindowSize` | window: Int | this | Scala方法 | `setWindowSize(...)` |
| `setMinCount` | minCount: Int | this | Scala方法 | `setMinCount(...)` |
| `this` | model: Map[String, Array[Float]] | Unit | Scala方法 | `this(...)` |
| `this` | model: Map[String, Array[Float]] | Unit | Scala方法 | `this(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `transform` | word: String | Vector | Scala方法 | `transform(...)` |
| `findSynonyms` | word: String, num: Int | Array[ | Scala方法 | `findSynonyms(...)` |
| `findSynonyms` | vector: Vector, num: Int | Array[ | Scala方法 | `findSynonyms(...)` |
| `compare` | left: Int, right: Int | Int | Scala方法 | `compare(...)` |
| `load` | sc: SparkContext, path: String | Word2VecModel | Scala方法 | `load(...)` |
| `save` | sc: SparkContext, path: String, model: Map[String, Array[Float]] | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | Word2VecModel | Scala方法 | `load(...)` |

---

## 包: org.apache.spark.mllib.fpm

**类数量**: 5

### AssociationRules

**完整类名**: `org.apache.spark.mllib.fpm.AssociationRules`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMinConfidence` | minConfidence: Double | this | Scala方法 | `setMinConfidence(...)` |

---

### FPGrowthModel

**完整类名**: `org.apache.spark.mllib.fpm.FPGrowthModel`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | freqItemsets: RDD[FreqItemset[Item]] | Unit | Scala方法 | `this(...)` |
| `generateAssociationRules` | confidence: Double | RDD[AssociationRules | Scala方法 | `generateAssociationRules(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | FPGrowthModel[_] | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | FPGrowthModel[_] | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMinSupport` | minSupport: Double | this | Scala方法 | `setMinSupport(...)` |
| `setNumPartitions` | numPartitions: Int | this | Scala方法 | `setNumPartitions(...)` |

---

### FPTree

**完整类名**: `org.apache.spark.mllib.fpm.FPTree`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | t: Iterable[T], count: Long = 1L | FPTree[T] | Scala方法 | `add(...)` |
| `merge` | other: FPTree[T] | FPTree[T] | Scala方法 | `merge(...)` |
| `extract` | minCount: Long,
      validateSuffix: T => Boolean = _ => true | Iterator[ | Scala方法 | `extract(...)` |

---

### LocalPrefixSpan

**完整类名**: `org.apache.spark.mllib.fpm.LocalPrefixSpan`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | postfixes: Array[Postfix] | Iterator[ | Scala方法 | `run(...)` |

---

### PrefixSpan

**完整类名**: `org.apache.spark.mllib.fpm.PrefixSpan`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setMinSupport` | minSupport: Double | this | Scala方法 | `setMinSupport(...)` |
| `setMaxPatternLength` | maxPatternLength: Int | this | Scala方法 | `setMaxPatternLength(...)` |
| `setMaxLocalProjDBSize` | maxLocalProjDBSize: Long | this | Scala方法 | `setMaxLocalProjDBSize(...)` |
| `toPublicRepr` | pattern: Array[Int] | Array[Array[Item]] | Scala方法 | `toPublicRepr(...)` |
| `project` | prefix: Int | Postfix | Scala方法 | `project(...)` |
| `project` | prefix: Prefix | Postfix | Scala方法 | `project(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | PrefixSpanModel[_] | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | PrefixSpanModel[_] | Scala方法 | `load(...)` |

---

## 包: org.apache.spark.mllib.linalg

**类数量**: 5

### BLAS

**完整类名**: `org.apache.spark.mllib.linalg.BLAS`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `axpy` | a: Double, x: Vector, y: Vector | Unit | Scala方法 | `axpy(...)` |
| `dot` | x: Vector, y: Vector | Double | Scala方法 | `dot(...)` |
| `copy` | x: Vector, y: Vector | Unit | Scala方法 | `copy(...)` |
| `scal` | a: Double, x: Vector | Unit | Scala方法 | `scal(...)` |
| `spr` | alpha: Double, v: Vector, U: DenseVector | Unit | Scala方法 | `spr(...)` |
| `spr` | alpha: Double, v: Vector, U: Array[Double] | Unit | Scala方法 | `spr(...)` |
| `syr` | alpha: Double, x: Vector, A: DenseMatrix | Unit | Scala方法 | `syr(...)` |
| `gemm` | alpha: Double,
      A: Matrix,
      B: DenseMatrix,
      beta: Double,
      C: DenseMatrix | Unit | Scala方法 | `gemm(...)` |
| `gemv` | alpha: Double,
      A: Matrix,
      x: Vector,
      beta: Double,
      y: DenseVector | Unit | Scala方法 | `gemv(...)` |

---

### CholeskyDecomposition

**完整类名**: `org.apache.spark.mllib.linalg.CholeskyDecomposition`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `solve` | A: Array[Double], bx: Array[Double] | Array[Double] | Scala方法 | `solve(...)` |
| `inverse` | UAi: Array[Double], k: Int | Array[Double] | Scala方法 | `inverse(...)` |

---

### EigenValueDecomposition

**完整类名**: `org.apache.spark.mllib.linalg.EigenValueDecomposition`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `symmetricEigs` | mul: BDV[Double] => BDV[Double],
      n: Int,
      k: Int,
      tol: Double,
      maxIterations: Int |  | Scala方法 | `symmetricEigs(...)` |

---

### MatrixUDT

**完整类名**: `org.apache.spark.mllib.linalg.MatrixUDT`

**描述**: Scala定义的Java友好接口

**方法数**: 46

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `multiply` | y: DenseMatrix | DenseMatrix | Scala方法 | `multiply(...)` |
| `multiply` | y: DenseVector | DenseVector | Scala方法 | `multiply(...)` |
| `multiply` | y: Vector | DenseVector | Scala方法 | `multiply(...)` |
| `toString` | maxLines: Int, maxLineWidth: Int | String | Scala方法 | `toString(...)` |
| `serialize` | obj: Matrix | InternalRow | Scala方法 | `serialize(...)` |
| `deserialize` | datum: Any | Matrix | Scala方法 | `deserialize(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |
| `this` | numRows: Int, numCols: Int, values: Array[Double] | Unit | Scala方法 | `this(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `zeros` | numRows: Int, numCols: Int | DenseMatrix | Scala方法 | `zeros(...)` |
| `ones` | numRows: Int, numCols: Int | DenseMatrix | Scala方法 | `ones(...)` |
| `eye` | n: Int | DenseMatrix | Scala方法 | `eye(...)` |
| `rand` | numRows: Int, numCols: Int, rng: Random | DenseMatrix | Scala方法 | `rand(...)` |
| `randn` | numRows: Int, numCols: Int, rng: Random | DenseMatrix | Scala方法 | `randn(...)` |
| `diag` | vector: Vector | DenseMatrix | Scala方法 | `diag(...)` |
| `fromML` | m: newlinalg.DenseMatrix | DenseMatrix | Scala方法 | `fromML(...)` |
| `this` | numRows: Int,
      numCols: Int,
      colPtrs: Array[Int],
      rowIndices: Array[Int],
      values: Array[Double] | Unit | Scala方法 | `this(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |
| `apply` | i: Int, j: Int | Double | Scala方法 | `apply(...)` |
| `fromCOO` | numRows: Int, numCols: Int, entries: Iterable[(Int, Int, Double | Unit | Scala方法 | `fromCOO(...)` |
| `speye` | n: Int | SparseMatrix | Scala方法 | `speye(...)` |
| `sprand` | numRows: Int, numCols: Int, density: Double, rng: Random | SparseMatrix | Scala方法 | `sprand(...)` |
| `sprandn` | numRows: Int, numCols: Int, density: Double, rng: Random | SparseMatrix | Scala方法 | `sprandn(...)` |
| `spdiag` | vector: Vector | SparseMatrix | Scala方法 | `spdiag(...)` |
| `fromML` | m: newlinalg.SparseMatrix | SparseMatrix | Scala方法 | `fromML(...)` |
| `dense` | numRows: Int, numCols: Int, values: Array[Double] | Matrix | Scala方法 | `dense(...)` |
| `sparse` | numRows: Int,
     numCols: Int,
     colPtrs: Array[Int],
     rowIndices: Array[Int],
     values: Array[Double] | Matrix | Scala方法 | `sparse(...)` |
| `zeros` | numRows: Int, numCols: Int | Matrix | Scala方法 | `zeros(...)` |
| `ones` | numRows: Int, numCols: Int | Matrix | Scala方法 | `ones(...)` |
| `eye` | n: Int | Matrix | Scala方法 | `eye(...)` |
| `speye` | n: Int | Matrix | Scala方法 | `speye(...)` |
| `rand` | numRows: Int, numCols: Int, rng: Random | Matrix | Scala方法 | `rand(...)` |
| `sprand` | numRows: Int, numCols: Int, density: Double, rng: Random | Matrix | Scala方法 | `sprand(...)` |
| `randn` | numRows: Int, numCols: Int, rng: Random | Matrix | Scala方法 | `randn(...)` |
| `sprandn` | numRows: Int, numCols: Int, density: Double, rng: Random | Matrix | Scala方法 | `sprandn(...)` |
| `diag` | vector: Vector | Matrix | Scala方法 | `diag(...)` |
| `horzcat` | matrices: Array[Matrix] | Matrix | Scala方法 | `horzcat(...)` |
| `vertcat` | matrices: Array[Matrix] | Matrix | Scala方法 | `vertcat(...)` |
| `fromML` | m: newlinalg.Matrix | Matrix | Scala方法 | `fromML(...)` |
| `mllibDenseMatrixToMLDenseMatrix` | m: DenseMatrix | newlinalg | Scala方法 | `mllibDenseMatrixToMLDenseMatrix(...)` |
| `mllibSparseMatrixToMLSparseMatrix` | m: SparseMatrix | newlinalg | Scala方法 | `mllibSparseMatrixToMLSparseMatrix(...)` |
| `mlMatrixToMLlibMatrix` | m: newlinalg.Matrix | Matrix | Scala方法 | `mlMatrixToMLlibMatrix(...)` |
| `mlDenseMatrixToMLlibDenseMatrix` | m: newlinalg.DenseMatrix | DenseMatrix | Scala方法 | `mlDenseMatrixToMLlibDenseMatrix(...)` |
| `mlSparseMatrixToMLlibSparseMatrix` | m: newlinalg.SparseMatrix | SparseMatrix | Scala方法 | `mlSparseMatrixToMLlibSparseMatrix(...)` |

---

### VectorUDT

**完整类名**: `org.apache.spark.mllib.linalg.VectorUDT`

**描述**: Scala定义的Java友好接口

**方法数**: 36

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |
| `apply` | i: Int | Double | Scala方法 | `apply(...)` |
| `foreachActive` | f: (Int, Double | Unit | Scala方法 | `foreachActive(...)` |
| `dot` | v: Vector | Double | Scala方法 | `dot(...)` |
| `serialize` | obj: Vector | InternalRow | Scala方法 | `serialize(...)` |
| `deserialize` | datum: Any | Vector | Scala方法 | `deserialize(...)` |
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |
| `dense` | firstValue: Double, otherValues: Double* | Vector | Scala方法 | `dense(...)` |
| `dense` | values: Array[Double] | Vector | Scala方法 | `dense(...)` |
| `sparse` | size: Int, indices: Array[Int], values: Array[Double] | Vector | Scala方法 | `sparse(...)` |
| `sparse` | size: Int, elements: Seq[(Int, Double | Unit | Scala方法 | `sparse(...)` |
| `sparse` | size: Int, elements: JavaIterable[(JavaInteger, JavaDouble | Unit | Scala方法 | `sparse(...)` |
| `zeros` | size: Int | Vector | Scala方法 | `zeros(...)` |
| `parse` | s: String | Vector | Scala方法 | `parse(...)` |
| `fromJson` | json: String | Vector | Scala方法 | `fromJson(...)` |
| `norm` | vector: Vector, p: Double | Double | Scala方法 | `norm(...)` |
| `sqdist` | v1: Vector, v2: Vector | Double | Scala方法 | `sqdist(...)` |
| `fromML` | v: newlinalg.Vector | Vector | Scala方法 | `fromML(...)` |
| `apply` | i: Int | Double | Scala方法 | `apply(...)` |
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |
| `unapply` | dv: DenseVector | Option[Array[Double]] | Scala方法 | `unapply(...)` |
| `fromML` | v: newlinalg.DenseVector | DenseVector | Scala方法 | `fromML(...)` |
| `apply` | i: Int | Double | Scala方法 | `apply(...)` |
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |
| `hashCode` | 无 | Int | Scala方法 | `hashCode(...)` |
| `next` | 无 |  | Scala方法 | `next(...)` |
| `unapply` | sv: SparseVector | Option[ | Scala方法 | `unapply(...)` |
| `fromML` | v: newlinalg.SparseVector | SparseVector | Scala方法 | `fromML(...)` |
| `mllibDenseVectorToMLDenseVector` | v: DenseVector | newlinalg | Scala方法 | `mllibDenseVectorToMLDenseVector(...)` |
| `mllibSparseVectorToMLSparseVector` | v: SparseVector | newlinalg | Scala方法 | `mllibSparseVectorToMLSparseVector(...)` |
| `mlVectorToMLlibVector` | v: newlinalg.Vector | Vector | Scala方法 | `mlVectorToMLlibVector(...)` |
| `mlDenseVectorToMLlibDenseVector` | v: newlinalg.DenseVector | DenseVector | Scala方法 | `mlDenseVectorToMLlibDenseVector(...)` |
| `mlSparseVectorToMLlibSparseVector` | v: newlinalg.SparseVector | SparseVector | Scala方法 | `mlSparseVectorToMLlibSparseVector(...)` |

---

## 包: org.apache.spark.mllib.linalg.distributed

**类数量**: 4

### GridPartitioner

**完整类名**: `org.apache.spark.mllib.linalg.distributed.GridPartitioner`

**描述**: Scala定义的Java友好接口

**方法数**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getPartition` | key: Any | Int | Scala方法 | `getPartition(...)` |
| `equals` | obj: Any | Boolean | Scala方法 | `equals(...)` |
| `apply` | rows: Int, cols: Int, suggestedNumPartitions: Int | GridPartitioner | Scala方法 | `apply(...)` |
| `this` | blocks: RDD[((Int, Int | Unit | Scala方法 | `this(...)` |
| `numRows` | 无 | Long | Scala方法 | `numRows(...)` |
| `numCols` | 无 | Long | Scala方法 | `numCols(...)` |
| `validate` | 无 | Unit | Scala方法 | `validate(...)` |
| `cache` | 无 | this | Scala方法 | `cache(...)` |
| `persist` | storageLevel: StorageLevel | this | Scala方法 | `persist(...)` |
| `toCoordinateMatrix` | 无 | CoordinateMatrix | Scala方法 | `toCoordinateMatrix(...)` |
| `toIndexedRowMatrix` | 无 | IndexedRowMatrix | Scala方法 | `toIndexedRowMatrix(...)` |
| `toLocalMatrix` | 无 | Matrix | Scala方法 | `toLocalMatrix(...)` |
| `add` | other: BlockMatrix | BlockMatrix | Scala方法 | `add(...)` |
| `subtract` | other: BlockMatrix | BlockMatrix | Scala方法 | `subtract(...)` |
| `multiply` | other: BlockMatrix | BlockMatrix | Scala方法 | `multiply(...)` |
| `multiply` | other: BlockMatrix,
      numMidDimSplits: Int | BlockMatrix | Scala方法 | `multiply(...)` |

---

### IndexedRow

**完整类名**: `org.apache.spark.mllib.linalg.distributed.IndexedRow`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | rows: RDD[IndexedRow] | Unit | Scala方法 | `this(...)` |
| `numCols` | 无 | Long | Scala方法 | `numCols(...)` |
| `numRows` | 无 | Long | Scala方法 | `numRows(...)` |
| `columnSimilarities` | 无 | CoordinateMatrix | Scala方法 | `columnSimilarities(...)` |
| `toRowMatrix` | 无 | RowMatrix | Scala方法 | `toRowMatrix(...)` |
| `toBlockMatrix` | 无 | BlockMatrix | Scala方法 | `toBlockMatrix(...)` |
| `toBlockMatrix` | rowsPerBlock: Int, colsPerBlock: Int | BlockMatrix | Scala方法 | `toBlockMatrix(...)` |
| `toCoordinateMatrix` | 无 | CoordinateMatrix | Scala方法 | `toCoordinateMatrix(...)` |
| `computeSVD` | k: Int,
      computeU: Boolean = false,
      rCond: Double = 1e-9 | SingularValueDecomposition[IndexedRowMatrix, Matrix] | Scala方法 | `computeSVD(...)` |
| `multiply` | B: Matrix | IndexedRowMatrix | Scala方法 | `multiply(...)` |
| `computeGramianMatrix` | 无 | Matrix | Scala方法 | `computeGramianMatrix(...)` |

---

### MatrixEntry

**完整类名**: `org.apache.spark.mllib.linalg.distributed.MatrixEntry`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | entries: RDD[MatrixEntry] | Unit | Scala方法 | `this(...)` |
| `numCols` | 无 | Long | Scala方法 | `numCols(...)` |
| `numRows` | 无 | Long | Scala方法 | `numRows(...)` |
| `transpose` | 无 | CoordinateMatrix | Scala方法 | `transpose(...)` |
| `toIndexedRowMatrix` | 无 | IndexedRowMatrix | Scala方法 | `toIndexedRowMatrix(...)` |
| `toRowMatrix` | 无 | RowMatrix | Scala方法 | `toRowMatrix(...)` |
| `toBlockMatrix` | 无 | BlockMatrix | Scala方法 | `toBlockMatrix(...)` |
| `toBlockMatrix` | rowsPerBlock: Int, colsPerBlock: Int | BlockMatrix | Scala方法 | `toBlockMatrix(...)` |

---

### RowMatrix

**完整类名**: `org.apache.spark.mllib.linalg.distributed.RowMatrix`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | rows: RDD[Vector] | Unit | Scala方法 | `this(...)` |
| `numCols` | 无 | Long | Scala方法 | `numCols(...)` |
| `numRows` | 无 | Long | Scala方法 | `numRows(...)` |
| `computeGramianMatrix` | 无 | Matrix | Scala方法 | `computeGramianMatrix(...)` |
| `computeSVD` | k: Int,
      computeU: Boolean = false,
      rCond: Double = 1e-9 | SingularValueDecomposition[RowMatrix, Matrix] | Scala方法 | `computeSVD(...)` |
| `computeCovariance` | 无 | Matrix | Scala方法 | `computeCovariance(...)` |
| `computePrincipalComponentsAndExplainedVariance` | k: Int |  | Scala方法 | `computePrincipalComponentsAndExplainedVariance(...)` |
| `computePrincipalComponents` | k: Int | Matrix | Scala方法 | `computePrincipalComponents(...)` |
| `computeColumnSummaryStatistics` | 无 | MultivariateStatisticalSummary | Scala方法 | `computeColumnSummaryStatistics(...)` |
| `multiply` | B: Matrix | RowMatrix | Scala方法 | `multiply(...)` |
| `columnSimilarities` | 无 | CoordinateMatrix | Scala方法 | `columnSimilarities(...)` |
| `columnSimilarities` | threshold: Double | CoordinateMatrix | Scala方法 | `columnSimilarities(...)` |
| `tallSkinnyQR` | computeQ: Boolean = false | QRDecomposition[RowMatrix, Matrix] | Scala方法 | `tallSkinnyQR(...)` |

---

## 包: org.apache.spark.mllib.optimization

**类数量**: 5

### Gradient

**完整类名**: `org.apache.spark.mllib.optimization.Gradient`

**描述**: Scala定义的Java友好接口

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `compute` | data: Vector, label: Double, weights: Vector |  | Scala方法 | `compute(...)` |
| `compute` | data: Vector, label: Double, weights: Vector, cumGradient: Vector | Double | Scala方法 | `compute(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `compute` | data: Vector,
      label: Double,
      weights: Vector,
      cumGradient: Vector | Double | Scala方法 | `compute(...)` |
| `compute` | data: Vector, label: Double, weights: Vector |  | Scala方法 | `compute(...)` |
| `compute` | data: Vector,
      label: Double,
      weights: Vector,
      cumGradient: Vector | Double | Scala方法 | `compute(...)` |
| `compute` | data: Vector, label: Double, weights: Vector |  | Scala方法 | `compute(...)` |
| `compute` | data: Vector,
      label: Double,
      weights: Vector,
      cumGradient: Vector | Double | Scala方法 | `compute(...)` |

---

### GradientDescent

**完整类名**: `org.apache.spark.mllib.optimization.GradientDescent`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setStepSize` | step: Double | this | Scala方法 | `setStepSize(...)` |
| `setMiniBatchFraction` | fraction: Double | this | Scala方法 | `setMiniBatchFraction(...)` |
| `setNumIterations` | iters: Int | this | Scala方法 | `setNumIterations(...)` |
| `setRegParam` | regParam: Double | this | Scala方法 | `setRegParam(...)` |
| `setConvergenceTol` | tolerance: Double | this | Scala方法 | `setConvergenceTol(...)` |
| `setGradient` | gradient: Gradient | this | Scala方法 | `setGradient(...)` |
| `setUpdater` | updater: Updater | this | Scala方法 | `setUpdater(...)` |
| `optimize` | data: RDD[(Double, Vector | Unit | Scala方法 | `optimize(...)` |
| `optimizeWithLossReturned` | data: RDD[(Double, Vector | Unit | Scala方法 | `optimizeWithLossReturned(...)` |
| `runMiniBatchSGD` | data: RDD[(Double, Vector | Unit | Scala方法 | `runMiniBatchSGD(...)` |
| `runMiniBatchSGD` | data: RDD[(Double, Vector | Unit | Scala方法 | `runMiniBatchSGD(...)` |

---

### LBFGS

**完整类名**: `org.apache.spark.mllib.optimization.LBFGS`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setNumCorrections` | corrections: Int | this | Scala方法 | `setNumCorrections(...)` |
| `setConvergenceTol` | tolerance: Double | this | Scala方法 | `setConvergenceTol(...)` |
| `setNumIterations` | iters: Int | this | Scala方法 | `setNumIterations(...)` |
| `setRegParam` | regParam: Double | this | Scala方法 | `setRegParam(...)` |
| `setGradient` | gradient: Gradient | this | Scala方法 | `setGradient(...)` |
| `setUpdater` | updater: Updater | this | Scala方法 | `setUpdater(...)` |
| `optimizeWithLossReturned` | data: RDD[(Double, Vector | Unit | Scala方法 | `optimizeWithLossReturned(...)` |
| `runLBFGS` | data: RDD[(Double, Vector | Unit | Scala方法 | `runLBFGS(...)` |
| `calculate` | weights: BDV[Double] |  | Scala方法 | `calculate(...)` |

---

### NNLS

**完整类名**: `org.apache.spark.mllib.optimization.NNLS`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wipe` | 无 | Unit | Scala方法 | `wipe(...)` |
| `createWorkspace` | n: Int | Workspace | Scala方法 | `createWorkspace(...)` |
| `solve` | ata: Array[Double], atb: Array[Double], ws: Workspace | Array[Double] | Scala方法 | `solve(...)` |
| `steplen` | dir: Array[Double], res: Array[Double] | Double | Scala方法 | `steplen(...)` |
| `stop` | step: Double, ndir: Double, nx: Double | Boolean | Scala方法 | `stop(...)` |

---

### Updater

**完整类名**: `org.apache.spark.mllib.optimization.Updater`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `compute` | weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double |  | Scala方法 | `compute(...)` |
| `compute` | weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double |  | Scala方法 | `compute(...)` |
| `compute` | weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double |  | Scala方法 | `compute(...)` |
| `compute` | weightsOld: Vector,
      gradient: Vector,
      stepSize: Double,
      iter: Int,
      regParam: Double |  | Scala方法 | `compute(...)` |

---

## 包: org.apache.spark.mllib.pmml.

**类数量**: 1

### PMMLModelExportFactory

**完整类名**: `org.apache.spark.mllib.pmml..PMMLModelExportFactory`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createPMMLModelExport` | model: Any | PMMLModelExport | Scala方法 | `createPMMLModelExport(...)` |

---

## 包: org.apache.spark.mllib.random

**类数量**: 2

### RandomRDDs

**完整类名**: `org.apache.spark.mllib.random.RandomRDDs`

**描述**: Scala定义的Java友好接口

**方法数**: 52

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `uniformRDD` | sc: SparkContext,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `uniformRDD(...)` |
| `uniformJavaRDD` | jsc: JavaSparkContext,
      size: Long,
      numPartitions: Int,
      seed: Long | JavaDoubleRDD | Scala方法 | `uniformJavaRDD(...)` |
| `uniformJavaRDD` | jsc: JavaSparkContext, size: Long, numPartitions: Int | JavaDoubleRDD | Scala方法 | `uniformJavaRDD(...)` |
| `uniformJavaRDD` | jsc: JavaSparkContext, size: Long | JavaDoubleRDD | Scala方法 | `uniformJavaRDD(...)` |
| `normalRDD` | sc: SparkContext,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `normalRDD(...)` |
| `normalJavaRDD` | jsc: JavaSparkContext,
      size: Long,
      numPartitions: Int,
      seed: Long | JavaDoubleRDD | Scala方法 | `normalJavaRDD(...)` |
| `normalJavaRDD` | jsc: JavaSparkContext, size: Long, numPartitions: Int | JavaDoubleRDD | Scala方法 | `normalJavaRDD(...)` |
| `normalJavaRDD` | jsc: JavaSparkContext, size: Long | JavaDoubleRDD | Scala方法 | `normalJavaRDD(...)` |
| `poissonRDD` | sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `poissonRDD(...)` |
| `poissonJavaRDD` | jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int,
      seed: Long | JavaDoubleRDD | Scala方法 | `poissonJavaRDD(...)` |
| `poissonJavaRDD` | jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int | JavaDoubleRDD | Scala方法 | `poissonJavaRDD(...)` |
| `poissonJavaRDD` | jsc: JavaSparkContext, mean: Double, size: Long | JavaDoubleRDD | Scala方法 | `poissonJavaRDD(...)` |
| `exponentialRDD` | sc: SparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `exponentialRDD(...)` |
| `exponentialJavaRDD` | jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int,
      seed: Long | JavaDoubleRDD | Scala方法 | `exponentialJavaRDD(...)` |
| `exponentialJavaRDD` | jsc: JavaSparkContext,
      mean: Double,
      size: Long,
      numPartitions: Int | JavaDoubleRDD | Scala方法 | `exponentialJavaRDD(...)` |
| `exponentialJavaRDD` | jsc: JavaSparkContext, mean: Double, size: Long | JavaDoubleRDD | Scala方法 | `exponentialJavaRDD(...)` |
| `gammaRDD` | sc: SparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `gammaRDD(...)` |
| `gammaJavaRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: Int,
      seed: Long | JavaDoubleRDD | Scala方法 | `gammaJavaRDD(...)` |
| `gammaJavaRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long,
      numPartitions: Int | JavaDoubleRDD | Scala方法 | `gammaJavaRDD(...)` |
| `gammaJavaRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      size: Long | JavaDoubleRDD | Scala方法 | `gammaJavaRDD(...)` |
| `logNormalRDD` | sc: SparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `logNormalRDD(...)` |
| `logNormalJavaRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: Int,
      seed: Long | JavaDoubleRDD | Scala方法 | `logNormalJavaRDD(...)` |
| `logNormalJavaRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long,
      numPartitions: Int | JavaDoubleRDD | Scala方法 | `logNormalJavaRDD(...)` |
| `logNormalJavaRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      size: Long | JavaDoubleRDD | Scala方法 | `logNormalJavaRDD(...)` |
| `uniformVectorRDD` | sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `uniformVectorRDD(...)` |
| `uniformJavaVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long | JavaRDD[Vector] | Scala方法 | `uniformJavaVectorRDD(...)` |
| `uniformJavaVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int | JavaRDD[Vector] | Scala方法 | `uniformJavaVectorRDD(...)` |
| `uniformJavaVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int | JavaRDD[Vector] | Scala方法 | `uniformJavaVectorRDD(...)` |
| `normalVectorRDD` | sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `normalVectorRDD(...)` |
| `normalJavaVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long | JavaRDD[Vector] | Scala方法 | `normalJavaVectorRDD(...)` |
| `normalJavaVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int | JavaRDD[Vector] | Scala方法 | `normalJavaVectorRDD(...)` |
| `normalJavaVectorRDD` | jsc: JavaSparkContext,
      numRows: Long,
      numCols: Int | JavaRDD[Vector] | Scala方法 | `normalJavaVectorRDD(...)` |
| `logNormalVectorRDD` | sc: SparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `logNormalVectorRDD(...)` |
| `logNormalJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long | JavaRDD[Vector] | Scala方法 | `logNormalJavaVectorRDD(...)` |
| `logNormalJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int | JavaRDD[Vector] | Scala方法 | `logNormalJavaVectorRDD(...)` |
| `logNormalJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      std: Double,
      numRows: Long,
      numCols: Int | JavaRDD[Vector] | Scala方法 | `logNormalJavaVectorRDD(...)` |
| `poissonVectorRDD` | sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `poissonVectorRDD(...)` |
| `poissonJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long | JavaRDD[Vector] | Scala方法 | `poissonJavaVectorRDD(...)` |
| `poissonJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int | JavaRDD[Vector] | Scala方法 | `poissonJavaVectorRDD(...)` |
| `poissonJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int | JavaRDD[Vector] | Scala方法 | `poissonJavaVectorRDD(...)` |
| `exponentialVectorRDD` | sc: SparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `exponentialVectorRDD(...)` |
| `exponentialJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long | JavaRDD[Vector] | Scala方法 | `exponentialJavaVectorRDD(...)` |
| `exponentialJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int | JavaRDD[Vector] | Scala方法 | `exponentialJavaVectorRDD(...)` |
| `exponentialJavaVectorRDD` | jsc: JavaSparkContext,
      mean: Double,
      numRows: Long,
      numCols: Int | JavaRDD[Vector] | Scala方法 | `exponentialJavaVectorRDD(...)` |
| `gammaVectorRDD` | sc: SparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `gammaVectorRDD(...)` |
| `gammaJavaVectorRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long | JavaRDD[Vector] | Scala方法 | `gammaJavaVectorRDD(...)` |
| `gammaJavaVectorRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int,
      numPartitions: Int | JavaRDD[Vector] | Scala方法 | `gammaJavaVectorRDD(...)` |
| `gammaJavaVectorRDD` | jsc: JavaSparkContext,
      shape: Double,
      scale: Double,
      numRows: Long,
      numCols: Int | JavaRDD[Vector] | Scala方法 | `gammaJavaVectorRDD(...)` |
| `randomVectorRDD` | sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0,
      seed: Long = Utils.random.nextLong( | Unit | Scala方法 | `randomVectorRDD(...)` |
| `randomJavaVectorRDD` | jsc: JavaSparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long | JavaRDD[Vector] | Scala方法 | `randomJavaVectorRDD(...)` |
| `randomJavaVectorRDD` | jsc: JavaSparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int | JavaRDD[Vector] | Scala方法 | `randomJavaVectorRDD(...)` |
| `randomJavaVectorRDD` | jsc: JavaSparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int | JavaRDD[Vector] | Scala方法 | `randomJavaVectorRDD(...)` |

---

### used

**完整类名**: `org.apache.spark.mllib.random.used`

**描述**: Scala定义的Java友好接口

**方法数**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `nextValue` | 无 | T | Scala方法 | `nextValue(...)` |
| `copy` | 无 | RandomDataGenerator[T] | Scala方法 | `copy(...)` |
| `setSeed` | seed: Long | Unit | Scala方法 | `setSeed(...)` |
| `copy` | 无 | UniformGenerator | Scala方法 | `copy(...)` |
| `setSeed` | seed: Long | Unit | Scala方法 | `setSeed(...)` |
| `copy` | 无 | StandardNormalGenerator | Scala方法 | `copy(...)` |
| `setSeed` | seed: Long | Unit | Scala方法 | `setSeed(...)` |
| `copy` | 无 | PoissonGenerator | Scala方法 | `copy(...)` |
| `setSeed` | seed: Long | Unit | Scala方法 | `setSeed(...)` |
| `copy` | 无 | ExponentialGenerator | Scala方法 | `copy(...)` |
| `setSeed` | seed: Long | Unit | Scala方法 | `setSeed(...)` |
| `copy` | 无 | GammaGenerator | Scala方法 | `copy(...)` |
| `setSeed` | seed: Long | Unit | Scala方法 | `setSeed(...)` |
| `copy` | 无 | LogNormalGenerator | Scala方法 | `copy(...)` |
| `setSeed` | seed: Long | Unit | Scala方法 | `setSeed(...)` |
| `copy` | 无 | WeibullGenerator | Scala方法 | `copy(...)` |

---

## 包: org.apache.spark.mllib.rdd

**类数量**: 4

### MLPairRDDFunctions

**完整类名**: `org.apache.spark.mllib.rdd.MLPairRDDFunctions`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `topByKey` | num: Int | Unit | Scala方法 | `topByKey(...)` |

---

### RDDFunctions

**完整类名**: `org.apache.spark.mllib.rdd.RDDFunctions`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `sliding` | windowSize: Int, step: Int | RDD[Array[T]] | Scala方法 | `sliding(...)` |
| `sliding` | windowSize: Int | RDD[Array[T]] | Scala方法 | `sliding(...)` |

---

### RandomRDDPartition

**完整类名**: `org.apache.spark.mllib.rdd.RandomRDDPartition`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `compute` | splitIn: Partition, context: TaskContext | Iterator[T] | Scala方法 | `compute(...)` |
| `compute` | splitIn: Partition, context: TaskContext | Iterator[Vector] | Scala方法 | `compute(...)` |
| `getVectorIterator` | partition: RandomRDDPartition[Double],
      vectorSize: Int | Iterator[Vector] | Scala方法 | `getVectorIterator(...)` |

---

### SlidingRDDPartition

**完整类名**: `org.apache.spark.mllib.rdd.SlidingRDDPartition`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `compute` | split: Partition, context: TaskContext | Iterator[Array[T]] | Scala方法 | `compute(...)` |
| `getPreferredLocations` | split: Partition | Seq[String] | Scala方法 | `getPreferredLocations(...)` |

---

## 包: org.apache.spark.mllib.recommendation

**类数量**: 2

### MatrixFactorizationModel

**完整类名**: `org.apache.spark.mllib.recommendation.MatrixFactorizationModel`

**描述**: Scala定义的Java友好接口

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | user: Int, product: Int | Double | Scala方法 | `predict(...)` |
| `predict` | usersProducts: RDD[(Int, Int | Unit | Scala方法 | `predict(...)` |
| `predict` | usersProducts: JavaPairRDD[JavaInteger, JavaInteger] | JavaRDD[Rating] | Scala方法 | `predict(...)` |
| `recommendProducts` | user: Int, num: Int | Array[Rating] | Scala方法 | `recommendProducts(...)` |
| `recommendUsers` | product: Int, num: Int | Array[Rating] | Scala方法 | `recommendUsers(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `recommendProductsForUsers` | num: Int | RDD[ | Scala方法 | `recommendProductsForUsers(...)` |
| `recommendUsersForProducts` | num: Int | RDD[ | Scala方法 | `recommendUsersForProducts(...)` |
| `compare` | left: Int, right: Int | Int | Scala方法 | `compare(...)` |
| `load` | sc: SparkContext, path: String | MatrixFactorizationModel | Scala方法 | `load(...)` |
| `save` | model: MatrixFactorizationModel, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | MatrixFactorizationModel | Scala方法 | `load(...)` |

---

### to

**完整类名**: `org.apache.spark.mllib.recommendation.to`

**描述**: Scala定义的Java友好接口

**方法数**: 24

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setBlocks` | numBlocks: Int | this | Scala方法 | `setBlocks(...)` |
| `setUserBlocks` | numUserBlocks: Int | this | Scala方法 | `setUserBlocks(...)` |
| `setProductBlocks` | numProductBlocks: Int | this | Scala方法 | `setProductBlocks(...)` |
| `setRank` | rank: Int | this | Scala方法 | `setRank(...)` |
| `setIterations` | iterations: Int | this | Scala方法 | `setIterations(...)` |
| `setLambda` | lambda: Double | this | Scala方法 | `setLambda(...)` |
| `setImplicitPrefs` | implicitPrefs: Boolean | this | Scala方法 | `setImplicitPrefs(...)` |
| `setAlpha` | alpha: Double | this | Scala方法 | `setAlpha(...)` |
| `setSeed` | seed: Long | this | Scala方法 | `setSeed(...)` |
| `setNonnegative` | b: Boolean | this | Scala方法 | `setNonnegative(...)` |
| `setIntermediateRDDStorageLevel` | storageLevel: StorageLevel | this | Scala方法 | `setIntermediateRDDStorageLevel(...)` |
| `setFinalRDDStorageLevel` | storageLevel: StorageLevel | this | Scala方法 | `setFinalRDDStorageLevel(...)` |
| `setCheckpointInterval` | checkpointInterval: Int | this | Scala方法 | `setCheckpointInterval(...)` |
| `run` | ratings: RDD[Rating] | MatrixFactorizationModel | Scala方法 | `run(...)` |
| `run` | ratings: JavaRDD[Rating] | MatrixFactorizationModel | Scala方法 | `run(...)` |
| `train` | ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      seed: Long | MatrixFactorizationModel | Scala方法 | `train(...)` |
| `train` | ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int | MatrixFactorizationModel | Scala方法 | `train(...)` |
| `train` | ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double | MatrixFactorizationModel | Scala方法 | `train(...)` |
| `train` | ratings: RDD[Rating], rank: Int, iterations: Int | MatrixFactorizationModel | Scala方法 | `train(...)` |
| `trainImplicit` | ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      alpha: Double,
      seed: Long | MatrixFactorizationModel | Scala方法 | `trainImplicit(...)` |
| `trainImplicit` | ratings: RDD[Rating],
      rank: Int,
      iterations: Int,
      lambda: Double,
      blocks: Int,
      alpha: Double | MatrixFactorizationModel | Scala方法 | `trainImplicit(...)` |
| `trainImplicit` | ratings: RDD[Rating], rank: Int, iterations: Int, lambda: Double, alpha: Double | MatrixFactorizationModel | Scala方法 | `trainImplicit(...)` |
| `trainImplicit` | ratings: RDD[Rating], rank: Int, iterations: Int | MatrixFactorizationModel | Scala方法 | `trainImplicit(...)` |

---

## 包: org.apache.spark.mllib.regression

**类数量**: 9

### GeneralizedLinearModel

**完整类名**: `org.apache.spark.mllib.regression.GeneralizedLinearModel`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | testData: RDD[Vector] | RDD[Double] | Scala方法 | `predict(...)` |
| `predict` | testData: Vector | Double | Scala方法 | `predict(...)` |
| `setIntercept` | addIntercept: Boolean | this | Scala方法 | `setIntercept(...)` |
| `setValidateData` | validateData: Boolean | this | Scala方法 | `setValidateData(...)` |
| `run` | input: RDD[LabeledPoint] | M | Scala方法 | `run(...)` |
| `run` | input: RDD[LabeledPoint], initialWeights: Vector | M | Scala方法 | `run(...)` |

---

### IsotonicRegressionModel

**完整类名**: `org.apache.spark.mllib.regression.IsotonicRegressionModel`

**描述**: Scala定义的Java友好接口

**方法数**: 21

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | boundaries: java.lang.Iterable[Double],
      predictions: java.lang.Iterable[Double],
      isotonic: java.lang.Boolean | Unit | Scala方法 | `this(...)` |
| `predict` | testData: RDD[Double] | RDD[Double] | Scala方法 | `predict(...)` |
| `predict` | testData: JavaDoubleRDD | JavaDoubleRDD | Scala方法 | `predict(...)` |
| `predict` | testData: Double | Double | Scala方法 | `predict(...)` |
| `linearInterpolation` | x1: Double, y1: Double, x2: Double, y2: Double, x: Double | Double | Scala方法 | `linearInterpolation(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `save` | sc: SparkContext,
        path: String,
        boundaries: Array[Double],
        predictions: Array[Double],
        isotonic: Boolean | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String |  | Scala方法 | `load(...)` |
| `load` | sc: SparkContext, path: String | IsotonicRegressionModel | Scala方法 | `load(...)` |
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setIsotonic` | isotonic: Boolean | this | Scala方法 | `setIsotonic(...)` |
| `run` | input: RDD[(Double, Double, Double | Unit | Scala方法 | `run(...)` |
| `run` | input: JavaRDD[(JDouble, JDouble, JDouble | Unit | Scala方法 | `run(...)` |
| `blockEnd` | start: Int | Int | Scala方法 | `blockEnd(...)` |
| `blockStart` | end: Int | Int | Scala方法 | `blockStart(...)` |
| `nextBlock` | start: Int | Int | Scala方法 | `nextBlock(...)` |
| `prevBlock` | start: Int | Int | Scala方法 | `prevBlock(...)` |
| `merge` | block1: Int, block2: Int | Int | Scala方法 | `merge(...)` |
| `average` | start: Int | Double | Scala方法 | `average(...)` |
| `shouldAccumulate` | feature: Double | Boolean | Scala方法 | `shouldAccumulate(...)` |
| `appendToOutput` | 无 | Unit | Scala方法 | `appendToOutput(...)` |

---

### LabeledPoint

**完整类名**: `org.apache.spark.mllib.regression.LabeledPoint`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `parse` | s: String | LabeledPoint | Scala方法 | `parse(...)` |

---

### LassoModel

**完整类名**: `org.apache.spark.mllib.regression.LassoModel`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | LassoModel | Scala方法 | `load(...)` |

---

### LinearRegressionModel

**完整类名**: `org.apache.spark.mllib.regression.LinearRegressionModel`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | LinearRegressionModel | Scala方法 | `load(...)` |

---

### RegressionModel

**完整类名**: `org.apache.spark.mllib.regression.RegressionModel`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | testData: RDD[Vector] | RDD[Double] | Scala方法 | `predict(...)` |
| `predict` | testData: Vector | Double | Scala方法 | `predict(...)` |
| `predict` | testData: JavaRDD[Vector] | JavaRDD[java | Scala方法 | `predict(...)` |
| `getNumFeatures` | metadata: JValue | Int | Scala方法 | `getNumFeatures(...)` |

---

### RidgeRegressionModel

**完整类名**: `org.apache.spark.mllib.regression.RidgeRegressionModel`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | RidgeRegressionModel | Scala方法 | `load(...)` |

---

### StreamingLinearRegressionWithSGD

**完整类名**: `org.apache.spark.mllib.regression.StreamingLinearRegressionWithSGD`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | 无 | Unit | Scala方法 | `this(...)` |
| `setStepSize` | stepSize: Double | this | Scala方法 | `setStepSize(...)` |
| `setRegParam` | regParam: Double | this | Scala方法 | `setRegParam(...)` |
| `setNumIterations` | numIterations: Int | this | Scala方法 | `setNumIterations(...)` |
| `setMiniBatchFraction` | miniBatchFraction: Double | this | Scala方法 | `setMiniBatchFraction(...)` |
| `setInitialWeights` | initialWeights: Vector | this | Scala方法 | `setInitialWeights(...)` |
| `setConvergenceTol` | tolerance: Double | this | Scala方法 | `setConvergenceTol(...)` |

---

### takes

**完整类名**: `org.apache.spark.mllib.regression.takes`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `trainOn` | data: DStream[LabeledPoint] | Unit | Scala方法 | `trainOn(...)` |
| `trainOn` | data: JavaDStream[LabeledPoint] | Unit | Scala方法 | `trainOn(...)` |
| `predictOn` | data: DStream[Vector] | DStream[Double] | Scala方法 | `predictOn(...)` |
| `predictOn` | data: JavaDStream[Vector] | JavaDStream[java | Scala方法 | `predictOn(...)` |

---

## 包: org.apache.spark.mllib.regression.impl

**类数量**: 1

### GLMRegressionModel

**完整类名**: `org.apache.spark.mllib.regression.impl.GLMRegressionModel`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext,
        path: String,
        modelClass: String,
        weights: Vector,
        intercept: Double | Unit | Scala方法 | `save(...)` |
| `loadData` | sc: SparkContext, path: String, modelClass: String, numFeatures: Int | Data | Scala方法 | `loadData(...)` |

---

## 包: org.apache.spark.mllib.stat

**类数量**: 3

### KernelDensity

**完整类名**: `org.apache.spark.mllib.stat.KernelDensity`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setBandwidth` | bandwidth: Double | this | Scala方法 | `setBandwidth(...)` |
| `setSample` | sample: RDD[Double] | this | Scala方法 | `setSample(...)` |
| `setSample` | sample: JavaRDD[java.lang.Double] | this | Scala方法 | `setSample(...)` |
| `estimate` | points: Array[Double] | Array[Double] | Scala方法 | `estimate(...)` |

---

### MultivariateOnlineSummarizer

**完整类名**: `org.apache.spark.mllib.stat.MultivariateOnlineSummarizer`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | sample: Vector | this | Scala方法 | `add(...)` |
| `merge` | other: MultivariateOnlineSummarizer | this | Scala方法 | `merge(...)` |

---

### Statistics

**完整类名**: `org.apache.spark.mllib.stat.Statistics`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `colStats` | X: RDD[Vector] | MultivariateStatisticalSummary | Scala方法 | `colStats(...)` |
| `corr` | X: RDD[Vector] | Matrix | Scala方法 | `corr(...)` |
| `corr` | X: RDD[Vector], method: String | Matrix | Scala方法 | `corr(...)` |
| `corr` | x: RDD[Double], y: RDD[Double] | Double | Scala方法 | `corr(...)` |
| `corr` | x: JavaRDD[java.lang.Double], y: JavaRDD[java.lang.Double] | Double | Scala方法 | `corr(...)` |
| `corr` | x: RDD[Double], y: RDD[Double], method: String | Double | Scala方法 | `corr(...)` |
| `corr` | x: JavaRDD[java.lang.Double], y: JavaRDD[java.lang.Double], method: String | Double | Scala方法 | `corr(...)` |
| `chiSqTest` | observed: Vector, expected: Vector | ChiSqTestResult | Scala方法 | `chiSqTest(...)` |
| `chiSqTest` | observed: Vector | ChiSqTestResult | Scala方法 | `chiSqTest(...)` |
| `chiSqTest` | observed: Matrix | ChiSqTestResult | Scala方法 | `chiSqTest(...)` |
| `chiSqTest` | data: RDD[LabeledPoint] | Array[ChiSqTestResult] | Scala方法 | `chiSqTest(...)` |
| `chiSqTest` | data: JavaRDD[LabeledPoint] | Array[ChiSqTestResult] | Scala方法 | `chiSqTest(...)` |
| `kolmogorovSmirnovTest` | data: RDD[Double], cdf: Double => Double | KolmogorovSmirnovTestResult | Scala方法 | `kolmogorovSmirnovTest(...)` |
| `kolmogorovSmirnovTest` | data: RDD[Double], distName: String, params: Double* | KolmogorovSmirnovTestResult | Scala方法 | `kolmogorovSmirnovTest(...)` |
| `kolmogorovSmirnovTest` | data: JavaDoubleRDD,
      distName: String,
      params: Double* | KolmogorovSmirnovTestResult | Scala方法 | `kolmogorovSmirnovTest(...)` |

---

## 包: org.apache.spark.mllib.stat.correlation

**类数量**: 3

### PearsonCorrelation

**完整类名**: `org.apache.spark.mllib.stat.correlation.PearsonCorrelation`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `computeCorrelation` | x: RDD[Double], y: RDD[Double] | Double | Scala方法 | `computeCorrelation(...)` |
| `computeCorrelationMatrix` | X: RDD[Vector] | Matrix | Scala方法 | `computeCorrelationMatrix(...)` |
| `computeCorrelationMatrixFromCovariance` | covarianceMatrix: Matrix | Matrix | Scala方法 | `computeCorrelationMatrixFromCovariance(...)` |

---

### SpearmanCorrelation

**完整类名**: `org.apache.spark.mllib.stat.correlation.SpearmanCorrelation`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `computeCorrelation` | x: RDD[Double], y: RDD[Double] | Double | Scala方法 | `computeCorrelation(...)` |
| `computeCorrelationMatrix` | X: RDD[Vector] | Matrix | Scala方法 | `computeCorrelationMatrix(...)` |

---

### based

**完整类名**: `org.apache.spark.mllib.stat.correlation.based`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `computeCorrelationMatrix` | X: RDD[Vector] | Matrix | Scala方法 | `computeCorrelationMatrix(...)` |
| `computeCorrelationWithMatrixImpl` | x: RDD[Double], y: RDD[Double] | Double | Scala方法 | `computeCorrelationWithMatrixImpl(...)` |
| `corrMatrix` | X: RDD[Vector],
      method: String = CorrelationNames.defaultCorrName | Matrix | Scala方法 | `corrMatrix(...)` |
| `getCorrelationFromName` | method: String | Correlation | Scala方法 | `getCorrelationFromName(...)` |

---

## 包: org.apache.spark.mllib.stat.distribution

**类数量**: 1

### provides

**完整类名**: `org.apache.spark.mllib.stat.distribution.provides`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `pdf` | x: Vector | Double | Scala方法 | `pdf(...)` |
| `logpdf` | x: Vector | Double | Scala方法 | `logpdf(...)` |

---

## 包: org.apache.spark.mllib.tree

**类数量**: 3

### that

**完整类名**: `org.apache.spark.mllib.tree.that`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | boostingStrategy: BoostingStrategy | Unit | Scala方法 | `this(...)` |
| `run` | input: RDD[LabeledPoint] | GradientBoostedTreesModel | Scala方法 | `run(...)` |
| `run` | input: JavaRDD[LabeledPoint] | GradientBoostedTreesModel | Scala方法 | `run(...)` |
| `runWithValidation` | input: RDD[LabeledPoint],
      validationInput: RDD[LabeledPoint] | GradientBoostedTreesModel | Scala方法 | `runWithValidation(...)` |
| `runWithValidation` | input: JavaRDD[LabeledPoint],
      validationInput: JavaRDD[LabeledPoint] | GradientBoostedTreesModel | Scala方法 | `runWithValidation(...)` |
| `train` | input: RDD[LabeledPoint],
      boostingStrategy: BoostingStrategy | GradientBoostedTreesModel | Scala方法 | `train(...)` |
| `train` | input: JavaRDD[LabeledPoint],
      boostingStrategy: BoostingStrategy | GradientBoostedTreesModel | Scala方法 | `train(...)` |

---

### that

**完整类名**: `org.apache.spark.mllib.tree.that`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | input: RDD[LabeledPoint] | RandomForestModel | Scala方法 | `run(...)` |
| `trainClassifier` | input: RDD[LabeledPoint],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Int | RandomForestModel | Scala方法 | `trainClassifier(...)` |
| `trainClassifier` | input: RDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt( | Unit | Scala方法 | `trainClassifier(...)` |
| `trainClassifier` | input: JavaRDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int | RandomForestModel | Scala方法 | `trainClassifier(...)` |
| `trainRegressor` | input: RDD[LabeledPoint],
      strategy: Strategy,
      numTrees: Int,
      featureSubsetStrategy: String,
      seed: Int | RandomForestModel | Scala方法 | `trainRegressor(...)` |
| `trainRegressor` | input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int = Utils.random.nextInt( | Unit | Scala方法 | `trainRegressor(...)` |
| `trainRegressor` | input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      numTrees: Int,
      featureSubsetStrategy: String,
      impurity: String,
      maxDepth: Int,
      maxBins: Int,
      seed: Int | RandomForestModel | Scala方法 | `trainRegressor(...)` |

---

### which

**完整类名**: `org.apache.spark.mllib.tree.which`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | strategy: Strategy | Unit | Scala方法 | `this(...)` |
| `run` | input: RDD[LabeledPoint] | DecisionTreeModel | Scala方法 | `run(...)` |
| `train` | input: RDD[LabeledPoint], strategy: Strategy | DecisionTreeModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int | DecisionTreeModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int | DecisionTreeModel | Scala方法 | `train(...)` |
| `train` | input: RDD[LabeledPoint],
      algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int,
      maxBins: Int,
      quantileCalculationStrategy: QuantileStrategy,
      categoricalFeaturesInfo: Map[Int, Int] | DecisionTreeModel | Scala方法 | `train(...)` |
| `trainClassifier` | input: RDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int | DecisionTreeModel | Scala方法 | `trainClassifier(...)` |
| `trainClassifier` | input: JavaRDD[LabeledPoint],
      numClasses: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      impurity: String,
      maxDepth: Int,
      maxBins: Int | DecisionTreeModel | Scala方法 | `trainClassifier(...)` |
| `trainRegressor` | input: RDD[LabeledPoint],
      categoricalFeaturesInfo: Map[Int, Int],
      impurity: String,
      maxDepth: Int,
      maxBins: Int | DecisionTreeModel | Scala方法 | `trainRegressor(...)` |
| `trainRegressor` | input: JavaRDD[LabeledPoint],
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer],
      impurity: String,
      maxDepth: Int,
      maxBins: Int | DecisionTreeModel | Scala方法 | `trainRegressor(...)` |

---

## 包: org.apache.spark.mllib.tree.configuration

**类数量**: 2

### BoostingStrategy

**完整类名**: `org.apache.spark.mllib.tree.configuration.BoostingStrategy`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultParams` | algo: String | BoostingStrategy | Scala方法 | `defaultParams(...)` |
| `defaultParams` | algo: Algo | BoostingStrategy | Scala方法 | `defaultParams(...)` |

---

### Strategy

**完整类名**: `org.apache.spark.mllib.tree.configuration.Strategy`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int,
      maxBins: Int,
      quantileCalculationStrategy: QuantileStrategy,
      categoricalFeaturesInfo: Map[Int, Int],
      minInstancesPerNode: Int,
      minInfoGain: Double,
      maxMemoryInMB: Int,
      subsamplingRate: Double,
      useNodeIdCache: Boolean,
      checkpointInterval: Int | Unit | Scala方法 | `this(...)` |
| `this` | algo: Algo,
      impurity: Impurity,
      maxDepth: Int,
      numClasses: Int,
      maxBins: Int,
      categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer] | Unit | Scala方法 | `this(...)` |
| `setAlgo` | algo: String | Unit | Scala方法 | `setAlgo(...)` |
| `setCategoricalFeaturesInfo` | categoricalFeaturesInfo: java.util.Map[java.lang.Integer, java.lang.Integer] | Unit | Scala方法 | `setCategoricalFeaturesInfo(...)` |
| `defaultStrategy` | algo: String | Strategy | Scala方法 | `defaultStrategy(...)` |
| `defaultStrategy` | algo: Algo | Strategy | Scala方法 | `defaultStrategy(...)` |

---

## 包: org.apache.spark.mllib.tree.impurity

**类数量**: 4

### Variance

**完整类名**: `org.apache.spark.mllib.tree.impurity.Variance`

**描述**: Scala定义的Java友好接口

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `calculate` | counts: Array[Double], totalCount: Double | Double | Scala方法 | `calculate(...)` |
| `calculate` | count: Double, sum: Double, sumSquares: Double | Double | Scala方法 | `calculate(...)` |
| `update` | allStats: Array[Double],
      offset: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double | Unit | Scala方法 | `update(...)` |
| `getCalculator` | allStats: Array[Double], offset: Int | VarianceCalculator | Scala方法 | `getCalculator(...)` |
| `calculate` | 无 | Double | Scala方法 | `calculate(...)` |

---

### classification

**完整类名**: `org.apache.spark.mllib.tree.impurity.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `calculate` | counts: Array[Double], totalCount: Double | Double | Scala方法 | `calculate(...)` |
| `calculate` | count: Double, sum: Double, sumSquares: Double | Double | Scala方法 | `calculate(...)` |
| `update` | allStats: Array[Double],
      offset: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double | Unit | Scala方法 | `update(...)` |
| `getCalculator` | allStats: Array[Double], offset: Int | EntropyCalculator | Scala方法 | `getCalculator(...)` |
| `calculate` | 无 | Double | Scala方法 | `calculate(...)` |
| `prob` | label: Double | Double | Scala方法 | `prob(...)` |

---

### classification

**完整类名**: `org.apache.spark.mllib.tree.impurity.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `calculate` | counts: Array[Double], totalCount: Double | Double | Scala方法 | `calculate(...)` |
| `calculate` | count: Double, sum: Double, sumSquares: Double | Double | Scala方法 | `calculate(...)` |
| `update` | allStats: Array[Double],
      offset: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double | Unit | Scala方法 | `update(...)` |
| `getCalculator` | allStats: Array[Double], offset: Int | GiniCalculator | Scala方法 | `getCalculator(...)` |
| `calculate` | 无 | Double | Scala方法 | `calculate(...)` |
| `prob` | label: Double | Double | Scala方法 | `prob(...)` |

---

### classification

**完整类名**: `org.apache.spark.mllib.tree.impurity.classification`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `calculate` | counts: Array[Double], totalCount: Double | Double | Scala方法 | `calculate(...)` |
| `calculate` | count: Double, sum: Double, sumSquares: Double | Double | Scala方法 | `calculate(...)` |
| `merge` | allStats: Array[Double], offset: Int, otherOffset: Int | Unit | Scala方法 | `merge(...)` |
| `update` | allStats: Array[Double],
      offset: Int,
      label: Double,
      numSamples: Int,
      sampleWeight: Double | Unit | Scala方法 | `update(...)` |
| `getCalculator` | allStats: Array[Double], offset: Int | ImpurityCalculator | Scala方法 | `getCalculator(...)` |
| `calculate` | 无 | Double | Scala方法 | `calculate(...)` |
| `add` | other: ImpurityCalculator | ImpurityCalculator | Scala方法 | `add(...)` |
| `subtract` | other: ImpurityCalculator | ImpurityCalculator | Scala方法 | `subtract(...)` |
| `prob` | label: Double | Double | Scala方法 | `prob(...)` |
| `getCalculator` | impurity: String,
      stats: Array[Double],
      rawCount: Long | ImpurityCalculator | Scala方法 | `getCalculator(...)` |

---

## 包: org.apache.spark.mllib.tree.loss

**类数量**: 5

### AbsoluteError

**完整类名**: `org.apache.spark.mllib.tree.loss.AbsoluteError`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `gradient` | prediction: Double, label: Double | Double | Scala方法 | `gradient(...)` |

---

### LogLoss

**完整类名**: `org.apache.spark.mllib.tree.loss.LogLoss`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `gradient` | prediction: Double, label: Double | Double | Scala方法 | `gradient(...)` |

---

### Losses

**完整类名**: `org.apache.spark.mllib.tree.loss.Losses`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | name: String | Loss | Scala方法 | `fromString(...)` |

---

### SquaredError

**完整类名**: `org.apache.spark.mllib.tree.loss.SquaredError`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `gradient` | prediction: Double, label: Double | Double | Scala方法 | `gradient(...)` |

---

### probability

**完整类名**: `org.apache.spark.mllib.tree.loss.probability`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `gradient` | prediction: Double, label: Double | Double | Scala方法 | `gradient(...)` |
| `computeError` | model: TreeEnsembleModel, data: RDD[LabeledPoint] | Double | Scala方法 | `computeError(...)` |

---

## 包: org.apache.spark.mllib.tree.model

**类数量**: 5

### DecisionTreeModel

**完整类名**: `org.apache.spark.mllib.tree.model.DecisionTreeModel`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `predict` | features: RDD[Vector] | RDD[Double] | Scala方法 | `predict(...)` |
| `predict` | features: JavaRDD[Vector] | JavaRDD[java | Scala方法 | `predict(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `apply` | p: Predict | PredictData | Scala方法 | `apply(...)` |
| `apply` | r: Row | PredictData | Scala方法 | `apply(...)` |
| `apply` | s: Split | SplitData | Scala方法 | `apply(...)` |
| `apply` | r: Row | SplitData | Scala方法 | `apply(...)` |
| `apply` | treeId: Int, n: Node | NodeData | Scala方法 | `apply(...)` |
| `apply` | r: Row | NodeData | Scala方法 | `apply(...)` |
| `save` | sc: SparkContext, path: String, model: DecisionTreeModel | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String, algo: String, numNodes: Int | DecisionTreeModel | Scala方法 | `load(...)` |
| `constructTrees` | nodes: RDD[NodeData] | Array[Node] | Scala方法 | `constructTrees(...)` |
| `constructTree` | data: Array[NodeData] | Node | Scala方法 | `constructTree(...)` |
| `load` | sc: SparkContext, path: String | DecisionTreeModel | Scala方法 | `load(...)` |

---

### InformationGainStats

**完整类名**: `org.apache.spark.mllib.tree.model.InformationGainStats`

**描述**: Scala定义的Java友好接口

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `equals` | o: Any | Boolean | Scala方法 | `equals(...)` |
| `getInvalidImpurityStats` | impurityCalculator: ImpurityCalculator | ImpurityStats | Scala方法 | `getInvalidImpurityStats(...)` |
| `getEmptyImpurityStats` | impurityCalculator: ImpurityCalculator | ImpurityStats | Scala方法 | `getEmptyImpurityStats(...)` |

---

### Node

**完整类名**: `org.apache.spark.mllib.tree.model.Node`

**描述**: Scala定义的Java友好接口

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `emptyNode` | nodeIndex: Int | Node | Scala方法 | `emptyNode(...)` |
| `apply` | nodeIndex: Int,
      predict: Predict,
      impurity: Double,
      isLeaf: Boolean | Node | Scala方法 | `apply(...)` |
| `leftChildIndex` | nodeIndex: Int | Int | Scala方法 | `leftChildIndex(...)` |
| `rightChildIndex` | nodeIndex: Int | Int | Scala方法 | `rightChildIndex(...)` |
| `parentIndex` | nodeIndex: Int | Int | Scala方法 | `parentIndex(...)` |
| `indexToLevel` | nodeIndex: Int | Int | Scala方法 | `indexToLevel(...)` |
| `isLeftChild` | nodeIndex: Int | Boolean | Scala方法 | `isLeftChild(...)` |
| `maxNodesInLevel` | level: Int | Int | Scala方法 | `maxNodesInLevel(...)` |
| `startIndexInLevel` | level: Int | Int | Scala方法 | `startIndexInLevel(...)` |
| `getNode` | nodeIndex: Int, rootNode: Node | Node | Scala方法 | `getNode(...)` |

---

### Predict

**完整类名**: `org.apache.spark.mllib.tree.model.Predict`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `equals` | other: Any | Boolean | Scala方法 | `equals(...)` |

---

### RandomForestModel

**完整类名**: `org.apache.spark.mllib.tree.model.RandomForestModel`

**描述**: Scala定义的Java友好接口

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | RandomForestModel | Scala方法 | `load(...)` |
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `evaluateEachIteration` | data: RDD[LabeledPoint],
      loss: Loss | Array[Double] | Scala方法 | `evaluateEachIteration(...)` |
| `computeInitialPredictionAndError` | data: RDD[LabeledPoint],
      initTreeWeight: Double,
      initTree: DecisionTreeModel,
      loss: Loss | RDD[ | Scala方法 | `computeInitialPredictionAndError(...)` |
| `updatePredictionError` | data: RDD[LabeledPoint],
    predictionAndError: RDD[(Double, Double | Unit | Scala方法 | `updatePredictionError(...)` |
| `load` | sc: SparkContext, path: String | GradientBoostedTreesModel | Scala方法 | `load(...)` |
| `predict` | features: Vector | Double | Scala方法 | `predict(...)` |
| `predict` | features: RDD[Vector] | RDD[Double] | Scala方法 | `predict(...)` |
| `predict` | features: JavaRDD[Vector] | JavaRDD[java | Scala方法 | `predict(...)` |
| `save` | sc: SparkContext, path: String, model: TreeEnsembleModel, className: String | Unit | Scala方法 | `save(...)` |
| `readMetadata` | metadata: JValue | Metadata | Scala方法 | `readMetadata(...)` |
| `loadTrees` | sc: SparkContext,
        path: String,
        treeAlgo: String | Array[DecisionTreeModel] | Scala方法 | `loadTrees(...)` |

---

## 包: org.apache.spark.mllib.util

**类数量**: 9

### DataValidators

**完整类名**: `org.apache.spark.mllib.util.DataValidators`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `multiLabelValidator` | k: Int | RDD[LabeledPoint] | Scala方法 | `multiLabelValidator(...)` |

---

### MFDataGenerator

**完整类名**: `org.apache.spark.mllib.util.MFDataGenerator`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: Array[String] | Unit | Scala方法 | `main(...)` |

---

### MLUtils

**完整类名**: `org.apache.spark.mllib.util.MLUtils`

**描述**: Scala定义的Java友好接口

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `loadLibSVMFile` | sc: SparkContext,
      path: String,
      numFeatures: Int,
      minPartitions: Int | RDD[LabeledPoint] | Scala方法 | `loadLibSVMFile(...)` |
| `loadLibSVMFile` | sc: SparkContext,
      path: String,
      numFeatures: Int | RDD[LabeledPoint] | Scala方法 | `loadLibSVMFile(...)` |
| `loadLibSVMFile` | sc: SparkContext, path: String | RDD[LabeledPoint] | Scala方法 | `loadLibSVMFile(...)` |
| `saveAsLibSVMFile` | data: RDD[LabeledPoint], dir: String | Unit | Scala方法 | `saveAsLibSVMFile(...)` |
| `loadVectors` | sc: SparkContext, path: String, minPartitions: Int | RDD[Vector] | Scala方法 | `loadVectors(...)` |
| `loadVectors` | sc: SparkContext, path: String | RDD[Vector] | Scala方法 | `loadVectors(...)` |
| `loadLabeledPoints` | sc: SparkContext, path: String, minPartitions: Int | RDD[LabeledPoint] | Scala方法 | `loadLabeledPoints(...)` |
| `loadLabeledPoints` | sc: SparkContext, dir: String | RDD[LabeledPoint] | Scala方法 | `loadLabeledPoints(...)` |
| `kFold` | df: DataFrame, numFolds: Int, foldColName: String | Array[ | Scala方法 | `kFold(...)` |
| `appendBias` | vector: Vector | Vector | Scala方法 | `appendBias(...)` |
| `convertVectorColumnsToML` | dataset: Dataset[_], cols: String* | DataFrame | Scala方法 | `convertVectorColumnsToML(...)` |
| `convertVectorColumnsFromML` | dataset: Dataset[_], cols: String* | DataFrame | Scala方法 | `convertVectorColumnsFromML(...)` |
| `convertMatrixColumnsToML` | dataset: Dataset[_], cols: String* | DataFrame | Scala方法 | `convertMatrixColumnsToML(...)` |
| `convertMatrixColumnsFromML` | dataset: Dataset[_], cols: String* | DataFrame | Scala方法 | `convertMatrixColumnsFromML(...)` |
| `optimizerFailed` | instr: Instrumentation, optimizerClass: Class[_] | Unit | Scala方法 | `optimizerFailed(...)` |

---

### NumericParser

**完整类名**: `org.apache.spark.mllib.util.NumericParser`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `parse` | s: String | Any | Scala方法 | `parse(...)` |

---

### chooses

**完整类名**: `org.apache.spark.mllib.util.chooses`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `generateLogisticRDD` | sc: SparkContext,
    nexamples: Int,
    nfeatures: Int,
    eps: Double,
    nparts: Int = 2,
    probOne: Double = 0.5 | RDD[LabeledPoint] | Scala方法 | `generateLogisticRDD(...)` |
| `main` | args: Array[String] | Unit | Scala方法 | `main(...)` |

---

### first

**完整类名**: `org.apache.spark.mllib.util.first`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `generateKMeansRDD` | sc: SparkContext,
      numPoints: Int,
      k: Int,
      d: Int,
      r: Double,
      numPartitions: Int = 2 | RDD[Array[Double]] | Scala方法 | `generateKMeansRDD(...)` |
| `main` | args: Array[String] | Unit | Scala方法 | `main(...)` |

---

### generates

**完整类名**: `org.apache.spark.mllib.util.generates`

**描述**: Scala定义的Java友好接口

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `generateLinearInputAsList` | intercept: Double,
      weights: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double | java | Scala方法 | `generateLinearInputAsList(...)` |
| `generateLinearInput` | intercept: Double,
      weights: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double = 0.1 | Seq[LabeledPoint] | Scala方法 | `generateLinearInput(...)` |
| `generateLinearInput` | intercept: Double,
      weights: Array[Double],
      xMean: Array[Double],
      xVariance: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double | Seq[LabeledPoint] | Scala方法 | `generateLinearInput(...)` |
| `generateLinearInput` | intercept: Double,
      weights: Array[Double],
      xMean: Array[Double],
      xVariance: Array[Double],
      nPoints: Int,
      seed: Int,
      eps: Double,
      sparsity: Double | Seq[LabeledPoint] | Scala方法 | `generateLinearInput(...)` |
| `rndElement` | i: Int | Unit | Scala方法 | `rndElement(...)` |
| `generateLinearRDD` | sc: SparkContext,
      nexamples: Int,
      nfeatures: Int,
      eps: Double,
      nparts: Int = 2,
      intercept: Double = 0.0 | RDD[LabeledPoint] | Scala方法 | `generateLinearRDD(...)` |
| `main` | args: Array[String] | Unit | Scala方法 | `main(...)` |

---

### generates

**完整类名**: `org.apache.spark.mllib.util.generates`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: Array[String] | Unit | Scala方法 | `main(...)` |

---

### which

**完整类名**: `org.apache.spark.mllib.util.which`

**描述**: Scala定义的Java友好接口

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | sc: SparkContext, path: String | Unit | Scala方法 | `save(...)` |
| `load` | sc: SparkContext, path: String | M | Scala方法 | `load(...)` |
| `metadataPath` | path: String | String | Scala方法 | `metadataPath(...)` |
| `loadMetadata` | sc: SparkContext, path: String |  | Scala方法 | `loadMetadata(...)` |

---

## 包: org.apache.spark.network.util

**类数量**: 2

### ByteUnit

**完整类名**: `org.apache.spark.network.util.ByteUnit`

**描述**: 暂无描述

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertFrom` | long d, ByteUnit u | long | 暂无描述 | `convertFrom(...)` |
| `convertTo` | long d, ByteUnit u | long | 暂无描述 | `convertTo(...)` |
| `toBytes` | long d | long | 暂无描述 | `toBytes(...)` |
| `toKiB` | long d | long | 暂无描述 | `toKiB(...)` |
| `toMiB` | long d | long | 暂无描述 | `toMiB(...)` |
| `toGiB` | long d | long | 暂无描述 | `toGiB(...)` |
| `toTiB` | long d | long | 暂无描述 | `toTiB(...)` |
| `toPiB` | long d | long | 暂无描述 | `toPiB(...)` |

---

### JavaUtils

**完整类名**: `org.apache.spark.network.util.JavaUtils`

**描述**: General utilities available in the network package. Many of these are sourced from Spark's

**方法数**: 50

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `closeQuietly` | Closeable closeable | void | 暂无描述 | `closeQuietly(...)` |
| `deleteQuietly` | File file | void | 暂无描述 | `deleteQuietly(...)` |
| `forceDeleteOnExit` | File file | void | 暂无描述 | `forceDeleteOnExit(...)` |
| `preVisitDirectory` | Path p, BasicFileAttributes a | FileVisitResult | 暂无描述 | `preVisitDirectory(...)` |
| `visitFile` | Path p, BasicFileAttributes a | FileVisitResult | 暂无描述 | `visitFile(...)` |
| `moveFile` | File src, File dst | void | 暂无描述 | `moveFile(...)` |
| `moveDirectory` | File src, File dst | void | 暂无描述 | `moveDirectory(...)` |
| `copyDirectory` | File src, File dst | void | 暂无描述 | `copyDirectory(...)` |
| `preVisitDirectory` | Path dir, BasicFileAttributes attrs | FileVisitResult | 暂无描述 | `preVisitDirectory(...)` |
| `visitFile` | Path file, BasicFileAttributes attrs | FileVisitResult | 暂无描述 | `visitFile(...)` |
| `nonNegativeHash` | Object obj | int | 暂无描述 | `nonNegativeHash(...)` |
| `stringToBytes` | String s | ByteBuffer | Convert the given string to a byte buffer. The resulting buffer can be | `stringToBytes(...)` |
| `bytesToString` | ByteBuffer b | String | Convert the given byte buffer to a string. The resulting string can be | `bytesToString(...)` |
| `sizeOf` | File file | long | 暂无描述 | `sizeOf(...)` |
| `sizeOf` | Path dirPath | long | 暂无描述 | `sizeOf(...)` |
| `visitFile` | Path file, BasicFileAttributes attrs | FileVisitResult | 暂无描述 | `visitFile(...)` |
| `cleanDirectory` | File dir | void | 暂无描述 | `cleanDirectory(...)` |
| `visitFile` | Path file, BasicFileAttributes attrs | FileVisitResult | 暂无描述 | `visitFile(...)` |
| `postVisitDirectory` | Path dir, IOException e | FileVisitResult | 暂无描述 | `postVisitDirectory(...)` |
| `deleteRecursively` | File file | void | 暂无描述 | `deleteRecursively(...)` |
| `deleteRecursively` | File file, FilenameFilter filter | void | 暂无描述 | `deleteRecursively(...)` |
| `listPaths` | File dir | Set<Path> | 暂无描述 | `listPaths(...)` |
| `listFiles` | File dir | Set<File> | 暂无描述 | `listFiles(...)` |
| `timeStringAs` | String str, TimeUnit unit | long | Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count in the given unit. | `timeStringAs(...)` |
| `timeStringAsMs` | String str | long | Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds for internal use. If | `timeStringAsMs(...)` |
| `timeStringAsSec` | String str | long | Convert a time parameter such as (50s, 100ms, or 250us) to seconds for internal use. If | `timeStringAsSec(...)` |
| `byteStringAs` | String str, ByteUnit unit | long | Convert a passed byte string (e.g. 50b, 100kb, or 250mb) to the given. If no suffix is | `byteStringAs(...)` |
| `byteStringAsBytes` | String str | long | Convert a passed byte string (e.g. 50b, 100k, or 250m) to bytes for | `byteStringAsBytes(...)` |
| `byteStringAsKb` | String str | long | Convert a passed byte string (e.g. 50b, 100k, or 250m) to kibibytes for | `byteStringAsKb(...)` |
| `byteStringAsMb` | String str | long | Convert a passed byte string (e.g. 50b, 100k, or 250m) to mebibytes for | `byteStringAsMb(...)` |
| `byteStringAsGb` | String str | long | Convert a passed byte string (e.g. 50b, 100k, or 250m) to gibibytes for | `byteStringAsGb(...)` |
| `bufferToArray` | ByteBuffer buffer | byte[] | Returns a byte array with the buffer's contents, trying to avoid copying the data if | `bufferToArray(...)` |
| `createDirectory` | String root | File | Create a directory inside the given parent directory with default namePrefix "spark". | `createDirectory(...)` |
| `createDirectory` | String root, String namePrefix | File | Create a directory inside the given parent directory. The directory is guaranteed to be | `createDirectory(...)` |
| `readFully` | ReadableByteChannel channel, ByteBuffer dst | void | Fills a buffer with data read from the channel. | `readFully(...)` |
| `readFully` | InputStream in, byte[] arr, int off, int len | void | Read len bytes exactly, otherwise throw exceptions. | `readFully(...)` |
| `copyURLToFile` | URL url, File file | void | Copy the content of a URL into a file. | `copyURLToFile(...)` |
| `join` | List<Object> arr, String sep | String | 暂无描述 | `join(...)` |
| `stackTraceToString` | Throwable t | String | 暂无描述 | `stackTraceToString(...)` |
| `checkedCast` | long value | int | 暂无描述 | `checkedCast(...)` |
| `contentEquals` | File file1, File file2 | boolean | 暂无描述 | `contentEquals(...)` |
| `isTesting` | 无 | boolean | Indicates whether Spark is currently running unit tests. | `isTesting(...)` |
| `checkArgument` | boolean check, String msg, Object... args | void | Throws IllegalArgumentException with the given message if the check is false. | `checkArgument(...)` |
| `checkState` | boolean check, String msg, Object... args | void | Throws IllegalStateException with the given message if the check is false. | `checkState(...)` |
| `digestToHexString` | String algorithm, byte[] input | String | Computes the digest of the input bytes using the given algorithm | `digestToHexString(...)` |
| `digestToHexString` | String algorithm, String input | String | Computes the digest of the input string using the given algorithm | `digestToHexString(...)` |
| `md5Hex` | byte[] input | String | Computes the MD5 digest of the input bytes | `md5Hex(...)` |
| `md5Hex` | String input | String | Computes the MD5 digest of the input string | `md5Hex(...)` |
| `sha256Hex` | byte[] input | String | Computes the SHA-256 digest of the input bytes | `sha256Hex(...)` |
| `sha256Hex` | String input | String | Computes the SHA-256 digest of the input string | `sha256Hex(...)` |

---

## 包: org.apache.spark.shuffle.checksum

**类数量**: 1

### for

**完整类名**: `org.apache.spark.shuffle.checksum.for`

**描述**: Scala定义的Java友好接口

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `update` | key: Any, value: Any | Unit | Scala方法 | `update(...)` |
| `getAggregatedChecksumValue` | rowBasedChecksums: Array[RowBasedChecksum] | Long | Scala方法 | `getAggregatedChecksumValue(...)` |

---

## 包: org.apache.spark.shuffle.sort

**类数量**: 1

### UnsafeShuffleWriter

**完整类名**: `org.apache.spark.shuffle.sort.UnsafeShuffleWriter`

**描述**: RowBasedChecksum calculator for each partition. RowBasedChecksum is independent

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getPeakMemoryUsedBytes` | 无 | long | Return the peak memory used so far, in bytes. | `getPeakMemoryUsedBytes(...)` |
| `write` | Iterator<Product2<K, V>> records | void | This convenience method should only be called in test code. | `write(...)` |
| `write` | scala.collection.Iterator<Product2<K, V>> records | void | 暂无描述 | `write(...)` |
| `stop` | boolean success | Option<MapStatus> | 暂无描述 | `stop(...)` |
| `channel` | 无 | WritableByteChannel | 暂无描述 | `channel(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |
| `getPartitionLengths` | 无 | long[] | 暂无描述 | `getPartitionLengths(...)` |

---

## 包: org.apache.spark.shuffle.sort.io

**类数量**: 5

### LocalDiskShuffleDataIO

**完整类名**: `org.apache.spark.shuffle.sort.io.LocalDiskShuffleDataIO`

**描述**: Implementation of the {@link ShuffleDataIO} plugin system that replicates the local shuffle

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `executor` | 无 | ShuffleExecutorComponents | 暂无描述 | `executor(...)` |
| `driver` | 无 | ShuffleDriverComponents | 暂无描述 | `driver(...)` |

---

### LocalDiskShuffleDriverComponents

**完整类名**: `org.apache.spark.shuffle.sort.io.LocalDiskShuffleDriverComponents`

**描述**: 暂无描述

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initializeApplication` | 无 | Map<String, String> | 暂无描述 | `initializeApplication(...)` |
| `cleanupApplication` | 无 | void | 暂无描述 | `cleanupApplication(...)` |
| `removeShuffle` | int shuffleId, boolean blocking | void | 暂无描述 | `removeShuffle(...)` |

---

### LocalDiskShuffleExecutorComponents

**完整类名**: `org.apache.spark.shuffle.sort.io.LocalDiskShuffleExecutorComponents`

**描述**: 暂无描述

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initializeExecutor` | String appId, String execId, Map<String, String> extraConfigs | void | 暂无描述 | `initializeExecutor(...)` |
| `createMapOutputWriter` | int shuffleId,
      long mapTaskId,
      int numPartitions | ShuffleMapOutputWriter | 暂无描述 | `createMapOutputWriter(...)` |
| `createSingleFileMapOutputWriter` | int shuffleId,
      long mapId | Optional<SingleSpillShuffleMapOutputWriter> | 暂无描述 | `createSingleFileMapOutputWriter(...)` |

---

### LocalDiskShuffleMapOutputWriter

**完整类名**: `org.apache.spark.shuffle.sort.io.LocalDiskShuffleMapOutputWriter`

**描述**: Implementation of {@link ShuffleMapOutputWriter} that replicates the functionality of shuffle

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getPartitionWriter` | int reducePartitionId | ShufflePartitionWriter | 暂无描述 | `getPartitionWriter(...)` |
| `commitAllPartitions` | long[] checksums | MapOutputCommitMessage | 暂无描述 | `commitAllPartitions(...)` |
| `abort` | Throwable error | void | 暂无描述 | `abort(...)` |
| `openStream` | 无 | OutputStream | 暂无描述 | `openStream(...)` |
| `openChannelWrapper` | 无 | Optional<WritableByteChannelWrapper> | 暂无描述 | `openChannelWrapper(...)` |
| `getNumBytesWritten` | 无 | long | 暂无描述 | `getNumBytesWritten(...)` |
| `getCount` | 无 | long | 暂无描述 | `getCount(...)` |
| `write` | int b | void | 暂无描述 | `write(...)` |
| `write` | byte[] buf, int pos, int length | void | 暂无描述 | `write(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |
| `getCount` | 无 | long | 暂无描述 | `getCount(...)` |
| `channel` | 无 | WritableByteChannel | 暂无描述 | `channel(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |

---

### LocalDiskSingleSpillMapOutputWriter

**完整类名**: `org.apache.spark.shuffle.sort.io.LocalDiskSingleSpillMapOutputWriter`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transferMapSpillFile` | File mapSpillFile,
      long[] partitionLengths,
      long[] checksums | void | 暂无描述 | `transferMapSpillFile(...)` |

---

## 包: org.apache.spark.sql

**类数量**: 1

### RowFactory

**完整类名**: `org.apache.spark.sql.RowFactory`

**描述**: A factory class used to construct {@link Row} objects.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | Object ... values | Row | Create a {@link Row} from the given arguments. Position i in the argument list becomes | `create(...)` |

---

## 包: org.apache.spark.sql.avro

**类数量**: 1

### AvroCompressionCodec

**完整类名**: `org.apache.spark.sql.avro.AvroCompressionCodec`

**描述**: A mapper class from Spark supported avro compression codecs to avro compression codecs.

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCodecName` | 无 | String | 暂无描述 | `getCodecName(...)` |
| `getSupportCompressionLevel` | 无 | boolean | 暂无描述 | `getSupportCompressionLevel(...)` |
| `fromString` | String s | AvroCompressionCodec | 暂无描述 | `fromString(...)` |
| `lowerCaseName` | 无 | String | 暂无描述 | `lowerCaseName(...)` |

---

## 包: org.apache.spark.sql.catalyst.expressions

**类数量**: 8

### ArrayExpressionUtils

**完整类名**: `org.apache.spark.sql.catalyst.expressions.ArrayExpressionUtils`

**描述**: 暂无描述

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `binarySearch` | boolean[] data, boolean value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Boolean[] data, Boolean value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | byte[] data, byte value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Byte[] data, Byte value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | short[] data, short value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Short[] data, Short value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | int[] data, int value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Integer[] data, Integer value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | long[] data, long value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Long[] data, Long value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | float[] data, float value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Float[] data, Float value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | double[] data, double value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Double[] data, Double value | int | 暂无描述 | `binarySearch(...)` |
| `binarySearch` | Object[] data, Object value, Comparator<Object> comp | int | 暂无描述 | `binarySearch(...)` |

---

### ArrayOfDecimalsSerDe

**完整类名**: `org.apache.spark.sql.catalyst.expressions.ArrayOfDecimalsSerDe`

**描述**: Serialize and deserialize Decimal as byte array.

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `serializeToByteArray` | Decimal item | byte[] | 暂无描述 | `serializeToByteArray(...)` |
| `serializeToByteArray` | Decimal[] items | byte[] | 暂无描述 | `serializeToByteArray(...)` |
| `deserializeFromMemory` | Memory mem, long offsetBytes, int numItems | Decimal[] | 暂无描述 | `deserializeFromMemory(...)` |
| `sizeOf` | Decimal item | int | 暂无描述 | `sizeOf(...)` |
| `sizeOf` | Memory mem, long offsetBytes, int numItems | int | 暂无描述 | `sizeOf(...)` |
| `getClassOfT` | 无 | Class<Decimal> | 暂无描述 | `getClassOfT(...)` |
| `serializeToByteArray` | Decimal item | byte[] | 暂无描述 | `serializeToByteArray(...)` |
| `serializeToByteArray` | Decimal[] items | byte[] | 暂无描述 | `serializeToByteArray(...)` |
| `deserializeFromMemory` | Memory mem, long offsetBytes, int numItems | Decimal[] | 暂无描述 | `deserializeFromMemory(...)` |
| `sizeOf` | Decimal item | int | 暂无描述 | `sizeOf(...)` |
| `sizeOf` | Memory mem, long offsetBytes, int numItems | int | 暂无描述 | `sizeOf(...)` |
| `getClassOfT` | 无 | Class<Decimal> | 暂无描述 | `getClassOfT(...)` |

---

### BitmapExpressionUtils

**完整类名**: `org.apache.spark.sql.catalyst.expressions.BitmapExpressionUtils`

**描述**: A utility class for constructing bitmap expressions.

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `bitmapBucketNumber` | long value | long | 暂无描述 | `bitmapBucketNumber(...)` |
| `bitmapBitPosition` | long value | long | 暂无描述 | `bitmapBitPosition(...)` |
| `bitmapCount` | byte[] bitmap | long | 暂无描述 | `bitmapCount(...)` |
| `bitmapMerge` | byte[] bitmap1, byte[] bitmap2 | void | 暂无描述 | `bitmapMerge(...)` |
| `bitmapAndMerge` | byte[] bitmap1, byte[] bitmap2 | void | 暂无描述 | `bitmapAndMerge(...)` |

---

### ExpressionImplUtils

**完整类名**: `org.apache.spark.sql.catalyst.expressions.ExpressionImplUtils`

**描述**: A utility class for constructing expressions.

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isLuhnNumber` | UTF8String numberString | boolean | 暂无描述 | `isLuhnNumber(...)` |
| `validateUTF8String` | UTF8String utf8String | UTF8String | 暂无描述 | `validateUTF8String(...)` |
| `tryValidateUTF8String` | UTF8String utf8String | UTF8String | 暂无描述 | `tryValidateUTF8String(...)` |
| `aesEncrypt` | byte[] input,
                                  byte[] key,
                                  UTF8String mode,
                                  UTF8String padding,
                                  byte[] iv,
                                  byte[] aad | byte[] | 暂无描述 | `aesEncrypt(...)` |
| `aesDecrypt` | byte[] input,
                                  byte[] key,
                                  UTF8String mode,
                                  UTF8String padding,
                                  byte[] aad | byte[] | 暂无描述 | `aesDecrypt(...)` |
| `getSparkVersion` | 无 | UTF8String | Function to return the Spark version. | `getSparkVersion(...)` |
| `getSentences` | UTF8String str,
      UTF8String language,
      UTF8String country | ArrayData | 暂无描述 | `getSentences(...)` |
| `randStr` | XORShiftRandom rng, int length | UTF8String | 暂无描述 | `randStr(...)` |
| `quote` | UTF8String str | UTF8String | 暂无描述 | `quote(...)` |

---

### ExpressionInfo

**完整类名**: `org.apache.spark.sql.catalyst.expressions.ExpressionInfo`

**描述**: Expression information, will be used to describe an expression.

**方法数**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getClassName` | 无 | String | 暂无描述 | `getClassName(...)` |
| `getUsage` | 无 | String | 暂无描述 | `getUsage(...)` |
| `getName` | 无 | String | 暂无描述 | `getName(...)` |
| `getExtended` | 无 | String | 暂无描述 | `getExtended(...)` |
| `getSince` | 无 | String | 暂无描述 | `getSince(...)` |
| `getArguments` | 无 | String | 暂无描述 | `getArguments(...)` |
| `getOriginalExamples` | 无 | String | 暂无描述 | `getOriginalExamples(...)` |
| `getExamples` | 无 | String | 暂无描述 | `getExamples(...)` |
| `getNote` | 无 | String | 暂无描述 | `getNote(...)` |
| `getDeprecated` | 无 | String | 暂无描述 | `getDeprecated(...)` |
| `getGroup` | 无 | String | 暂无描述 | `getGroup(...)` |
| `getDb` | 无 | String | 暂无描述 | `getDb(...)` |
| `getSource` | 无 | String | 暂无描述 | `getSource(...)` |

---

### ToJavaArrayUtils

**完整类名**: `org.apache.spark.sql.catalyst.expressions.ToJavaArrayUtils`

**描述**: 暂无描述

**方法数**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toBooleanArray` | ArrayData arrayData | boolean[] | 暂无描述 | `toBooleanArray(...)` |
| `toBoxedBooleanArray` | ArrayData arrayData | Boolean[] | 暂无描述 | `toBoxedBooleanArray(...)` |
| `toByteArray` | ArrayData arrayData | byte[] | 暂无描述 | `toByteArray(...)` |
| `toBoxedByteArray` | ArrayData arrayData | Byte[] | 暂无描述 | `toBoxedByteArray(...)` |
| `toShortArray` | ArrayData arrayData | short[] | 暂无描述 | `toShortArray(...)` |
| `toBoxedShortArray` | ArrayData arrayData | Short[] | 暂无描述 | `toBoxedShortArray(...)` |
| `toIntegerArray` | ArrayData arrayData | int[] | 暂无描述 | `toIntegerArray(...)` |
| `toBoxedIntegerArray` | ArrayData arrayData | Integer[] | 暂无描述 | `toBoxedIntegerArray(...)` |
| `toLongArray` | ArrayData arrayData | long[] | 暂无描述 | `toLongArray(...)` |
| `toBoxedLongArray` | ArrayData arrayData | Long[] | 暂无描述 | `toBoxedLongArray(...)` |
| `toFloatArray` | ArrayData arrayData | float[] | 暂无描述 | `toFloatArray(...)` |
| `toBoxedFloatArray` | ArrayData arrayData | Float[] | 暂无描述 | `toBoxedFloatArray(...)` |
| `toDoubleArray` | ArrayData arrayData | double[] | 暂无描述 | `toDoubleArray(...)` |
| `toBoxedDoubleArray` | ArrayData arrayData | Double[] | 暂无描述 | `toBoxedDoubleArray(...)` |

---

### UnsafeRowChecksum

**完整类名**: `org.apache.spark.sql.catalyst.expressions.UnsafeRowChecksum`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createUnsafeRowChecksums` | numPartitions: Int | Array[RowBasedChecksum] | Scala方法 | `createUnsafeRowChecksums(...)` |

---

### VectorFunctionImplUtils

**完整类名**: `org.apache.spark.sql.catalyst.expressions.VectorFunctionImplUtils`

**描述**: A utility class for vector similarity/distance function implementations.

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `vectorCosineSimilarity` | ArrayData left, ArrayData right, UTF8String funcName | Float | 暂无描述 | `vectorCosineSimilarity(...)` |
| `vectorInnerProduct` | ArrayData left, ArrayData right, UTF8String funcName | Float | 暂无描述 | `vectorInnerProduct(...)` |
| `vectorL2Distance` | ArrayData left, ArrayData right, UTF8String funcName | Float | 暂无描述 | `vectorL2Distance(...)` |
| `vectorL1Norm` | ArrayData vec | Float | 暂无描述 | `vectorL1Norm(...)` |
| `vectorL2Norm` | ArrayData vec | Float | 暂无描述 | `vectorL2Norm(...)` |
| `vectorInfNorm` | ArrayData vec | Float | Computes the infinity norm (maximum absolute value) of a float vector. | `vectorInfNorm(...)` |
| `vectorNormalizeWithNorm` | ArrayData vec, float norm | ArrayData | 暂无描述 | `vectorNormalizeWithNorm(...)` |
| `vectorNorm` | ArrayData vec, float degree, UTF8String funcName | Float | 暂无描述 | `vectorNorm(...)` |
| `vectorNormalize` | ArrayData vec, float degree, UTF8String funcName | ArrayData | 暂无描述 | `vectorNormalize(...)` |

---

## 包: org.apache.spark.sql.catalyst.expressions.json

**类数量**: 1

### JsonExpressionUtils

**完整类名**: `org.apache.spark.sql.catalyst.expressions.json.JsonExpressionUtils`

**描述**: 暂无描述

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `lengthOfJsonArray` | UTF8String json | Integer | 暂无描述 | `lengthOfJsonArray(...)` |
| `jsonObjectKeys` | UTF8String json | GenericArrayData | 暂无描述 | `jsonObjectKeys(...)` |

---

## 包: org.apache.spark.sql.catalyst.expressions.xml

**类数量**: 1

### UDFXPathUtil

**完整类名**: `org.apache.spark.sql.catalyst.expressions.xml.UDFXPathUtil`

**描述**: Utility class for all XPath UDFs. Each UDF instance should keep an instance of this class.

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `eval` | String xml, String path, QName qname | Object | 暂无描述 | `eval(...)` |
| `evalBoolean` | String xml, String path | Boolean | 暂无描述 | `evalBoolean(...)` |
| `evalString` | String xml, String path | String | 暂无描述 | `evalString(...)` |
| `evalNumber` | String xml, String path | Double | 暂无描述 | `evalNumber(...)` |
| `evalNode` | String xml, String path | Node | 暂无描述 | `evalNode(...)` |
| `evalNodeList` | String xml, String path | NodeList | 暂无描述 | `evalNodeList(...)` |
| `set` | String s | void | 暂无描述 | `set(...)` |
| `read` | 无 | int | 暂无描述 | `read(...)` |
| `read` | char[] cbuf, int off, int len | int | 暂无描述 | `read(...)` |
| `skip` | long ns | long | 暂无描述 | `skip(...)` |
| `ready` | 无 | boolean | 暂无描述 | `ready(...)` |
| `markSupported` | 无 | boolean | 暂无描述 | `markSupported(...)` |
| `mark` | int readAheadLimit | void | 暂无描述 | `mark(...)` |
| `reset` | 无 | void | 暂无描述 | `reset(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |

---

## 包: org.apache.spark.sql.catalyst.util

**类数量**: 2

### CharVarcharCodegenUtils

**完整类名**: `org.apache.spark.sql.catalyst.util.CharVarcharCodegenUtils`

**描述**: 暂无描述

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `charTypeWriteSideCheck` | UTF8String inputStr, int limit | UTF8String | 暂无描述 | `charTypeWriteSideCheck(...)` |
| `varcharTypeWriteSideCheck` | UTF8String inputStr, int limit | UTF8String | 暂无描述 | `varcharTypeWriteSideCheck(...)` |
| `readSidePadding` | UTF8String inputStr, int limit | UTF8String | 暂无描述 | `readSidePadding(...)` |

---

### HadoopCompressionCodec

**完整类名**: `org.apache.spark.sql.catalyst.util.HadoopCompressionCodec`

**描述**: A mapper class from Spark supported hadoop compression codecs to hadoop compression codecs.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCompressionCodec` | 无 | CompressionCodec | 暂无描述 | `getCompressionCodec(...)` |
| `lowerCaseName` | 无 | String | 暂无描述 | `lowerCaseName(...)` |

---

## 包: org.apache.spark.sql.catalyst.util.geo

**类数量**: 3

### WkbParseException

**完整类名**: `org.apache.spark.sql.catalyst.util.geo.WkbParseException`

**描述**: Exception thrown when parsing WKB data fails.

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getParseError` | 无 | String | 暂无描述 | `getParseError(...)` |
| `getPosition` | 无 | long | 暂无描述 | `getPosition(...)` |
| `getWkb` | 无 | byte[] | 暂无描述 | `getWkb(...)` |

---

### WkbReader

**完整类名**: `org.apache.spark.sql.catalyst.util.geo.WkbReader`

**描述**: Reader for parsing Well-Known Binary (WKB) format geometries and geographies.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `read` | byte[] wkb | GeometryModel | Reads a geometry from WKB bytes. | `read(...)` |
| `read` | byte[] wkb, int srid | GeometryModel | Reads a geometry from WKB bytes with a specified SRID. | `read(...)` |

---

### WkbWriter

**完整类名**: `org.apache.spark.sql.catalyst.util.geo.WkbWriter`

**描述**: Utility class for converting geometries to Well-Known Binary (WKB) format.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `write` | GeometryModel geometry | byte[] | Writes a geometry to WKB format. | `write(...)` |
| `write` | GeometryModel geometry, ByteOrder byteOrder | byte[] | 暂无描述 | `write(...)` |

---

## 包: org.apache.spark.sql.connector.catalog

**类数量**: 8

### ChangelogInfo

**完整类名**: `org.apache.spark.sql.connector.catalog.ChangelogInfo`

**描述**: Encapsulates the parameters of a Change Data Capture (CDC) query, passed from the

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `range` | 无 | ChangelogRange | 暂无描述 | `range(...)` |
| `deduplicationMode` | 无 | DeduplicationMode | 暂无描述 | `deduplicationMode(...)` |
| `computeUpdates` | 无 | boolean | 暂无描述 | `computeUpdates(...)` |

---

### DefaultValue

**完整类名**: `org.apache.spark.sql.connector.catalog.DefaultValue`

**描述**: A class that represents default values.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSql` | 无 | String | Returns the SQL representation of the default value (Spark SQL dialect), if provided. | `getSql(...)` |
| `getExpression` | 无 | Expression | Returns the expression representing the default value, if provided. | `getExpression(...)` |

---

### IdentityColumnSpec

**完整类名**: `org.apache.spark.sql.connector.catalog.IdentityColumnSpec`

**描述**: Identity column specification.

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStart` | 无 | long | @return the start value to generate the identity values | `getStart(...)` |
| `getStep` | 无 | long | @return the step value to generate the identity values | `getStep(...)` |
| `isAllowExplicitInsert` | 无 | boolean | @return whether the identity column allows explicit insertion of values | `isAllowExplicitInsert(...)` |

---

### NamespaceChange

**完整类名**: `org.apache.spark.sql.connector.catalog.NamespaceChange`

**描述**: NamespaceChange subclasses represent requested changes to a namespace. These are passed to

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | String | 暂无描述 | `property(...)` |
| `value` | 无 | String | 暂无描述 | `value(...)` |
| `property` | 无 | String | 暂无描述 | `property(...)` |

---

### TableChange

**完整类名**: `org.apache.spark.sql.connector.catalog.TableChange`

**描述**: TableChange subclasses represent requested changes to a table. These are passed to

**方法数**: 31

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | String | 暂无描述 | `property(...)` |
| `value` | 无 | String | 暂无描述 | `value(...)` |
| `property` | 无 | String | 暂无描述 | `property(...)` |
| `column` | 无 | String | 暂无描述 | `column(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `dataType` | 无 | DataType | 暂无描述 | `dataType(...)` |
| `isNullable` | 无 | boolean | 暂无描述 | `isNullable(...)` |
| `comment` | 无 | String | 暂无描述 | `comment(...)` |
| `position` | 无 | ColumnPosition | 暂无描述 | `position(...)` |
| `defaultValue` | 无 | ColumnDefaultValue | 暂无描述 | `defaultValue(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `newName` | 无 | String | 暂无描述 | `newName(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `newDataType` | 无 | DataType | 暂无描述 | `newDataType(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `nullable` | 无 | boolean | 暂无描述 | `nullable(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `newComment` | 无 | String | 暂无描述 | `newComment(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `position` | 无 | ColumnPosition | 暂无描述 | `position(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `newDefaultValue` | 无 | String | 暂无描述 | `newDefaultValue(...)` |
| `newCurrentDefault` | 无 | DefaultValue | 暂无描述 | `newCurrentDefault(...)` |
| `fieldNames` | 无 | String[] | 暂无描述 | `fieldNames(...)` |
| `ifExists` | 无 | Boolean | 暂无描述 | `ifExists(...)` |
| `clusteringColumns` | 无 | NamedReference[] | 暂无描述 | `clusteringColumns(...)` |
| `constraint` | 无 | Constraint | 暂无描述 | `constraint(...)` |
| `validatedTableVersion` | 无 | String | 暂无描述 | `validatedTableVersion(...)` |
| `name` | 无 | String | 暂无描述 | `name(...)` |
| `ifExists` | 无 | boolean | 暂无描述 | `ifExists(...)` |
| `mode` | 无 | Mode | 暂无描述 | `mode(...)` |

---

### TableInfo

**完整类名**: `org.apache.spark.sql.connector.catalog.TableInfo`

**描述**: Constructor for TableInfo used by the builder.

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `columns` | 无 | Column[] | 暂无描述 | `columns(...)` |
| `schema` | 无 | StructType | 暂无描述 | `schema(...)` |
| `properties` | 无 | Map<String, String> | 暂无描述 | `properties(...)` |
| `partitions` | 无 | Transform[] | 暂无描述 | `partitions(...)` |
| `constraints` | 无 | Constraint[] | 暂无描述 | `constraints(...)` |
| `withColumns` | Column[] columns | Builder | 暂无描述 | `withColumns(...)` |
| `withProperties` | Map<String, String> properties | Builder | 暂无描述 | `withProperties(...)` |
| `withPartitions` | Transform[] partitions | Builder | 暂无描述 | `withPartitions(...)` |
| `withConstraints` | Constraint[] constraints | Builder | 暂无描述 | `withConstraints(...)` |
| `build` | 无 | TableInfo | 暂无描述 | `build(...)` |

---

### ViewChange

**完整类名**: `org.apache.spark.sql.connector.catalog.ViewChange`

**描述**: ViewChange subclasses represent requested changes to a view.

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | String | 暂无描述 | `property(...)` |
| `value` | 无 | String | 暂无描述 | `value(...)` |
| `property` | 无 | String | 暂无描述 | `property(...)` |

---

### ViewInfo

**完整类名**: `org.apache.spark.sql.connector.catalog.ViewInfo`

**描述**: A class that holds view information.

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ident` | 无 | Identifier | @return The view identifier | `ident(...)` |
| `sql` | 无 | String | @return The view identifier | `sql(...)` |
| `currentCatalog` | 无 | String | @return The SQL text that defines the view | `currentCatalog(...)` |
| `currentNamespace` | 无 | String[] | @return The current catalog | `currentNamespace(...)` |
| `schema` | 无 | StructType | @return The current namespace | `schema(...)` |
| `queryColumnNames` | 无 | String[] | @return The view query output schema | `queryColumnNames(...)` |
| `columnAliases` | 无 | String[] | @return The query column names | `columnAliases(...)` |
| `columnComments` | 无 | String[] | @return The column aliases | `columnComments(...)` |
| `properties` | 无 | Map<String, String> | @return The column comments | `properties(...)` |

---

## 包: org.apache.spark.sql.connector.catalog.constraints

**类数量**: 4

### Check

**完整类名**: `org.apache.spark.sql.connector.catalog.constraints.Check`

**描述**: A CHECK constraint.

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `predicateSql` | 无 | String | Returns the SQL representation of the search condition (Spark SQL dialect). | `predicateSql(...)` |
| `predicate` | 无 | Predicate | Returns the search condition. | `predicate(...)` |
| `predicateSql` | String predicateSql | Builder | 暂无描述 | `predicateSql(...)` |
| `predicate` | Predicate predicate | Builder | 暂无描述 | `predicate(...)` |
| `build` | 无 | Check | 暂无描述 | `build(...)` |

---

### ForeignKey

**完整类名**: `org.apache.spark.sql.connector.catalog.constraints.ForeignKey`

**描述**: A FOREIGN KEY constraint.

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `columns` | 无 | NamedReference[] | Returns the referencing columns. | `columns(...)` |
| `referencedTable` | 无 | Identifier | Returns the referencing columns. | `referencedTable(...)` |
| `referencedColumns` | 无 | NamedReference[] | Returns the referenced table. | `referencedColumns(...)` |
| `build` | 无 | ForeignKey | 暂无描述 | `build(...)` |

---

### PrimaryKey

**完整类名**: `org.apache.spark.sql.connector.catalog.constraints.PrimaryKey`

**描述**: A PRIMARY KEY constraint.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `columns` | 无 | NamedReference[] | Returns the columns that comprise the primary key. | `columns(...)` |
| `build` | 无 | PrimaryKey | 暂无描述 | `build(...)` |

---

### Unique

**完整类名**: `org.apache.spark.sql.connector.catalog.constraints.Unique`

**描述**: A UNIQUE constraint.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `columns` | 无 | NamedReference[] | Returns the columns that comprise the unique key. | `columns(...)` |
| `build` | 无 | Unique | 暂无描述 | `build(...)` |

---

## 包: org.apache.spark.sql.connector.catalog.functions

**类数量**: 1

### IntegerAdd

**完整类名**: `org.apache.spark.sql.connector.catalog.functions.IntegerAdd`

**描述**: Interface for a function that produces a result value for each input row.

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `inputTypes` | 无 | DataType[] | 暂无描述 | `inputTypes(...)` |
| `invoke` | int left, int right | int | 暂无描述 | `invoke(...)` |
| `inputTypes` | 无 | DataType[] | 暂无描述 | `inputTypes(...)` |
| `invoke` | int left, int right | int | 暂无描述 | `invoke(...)` |
| `produceResult` | InternalRow input | Integer | 暂无描述 | `produceResult(...)` |

---

## 包: org.apache.spark.sql.connector.catalog.procedures

**类数量**: 1

### ProcedureParameter

**完整类名**: `org.apache.spark.sql.connector.catalog.procedures.ProcedureParameter`

**描述**: A {@link Procedure procedure} parameter.

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultValue` | String sql | Builder | Sets the default value of the parameter using SQL. | `defaultValue(...)` |
| `defaultValue` | Expression expression | Builder | Sets the default value of the parameter using an expression. | `defaultValue(...)` |
| `defaultValue` | DefaultValue defaultValue | Builder | Sets the default value of the parameter. | `defaultValue(...)` |
| `comment` | String comment | Builder | Sets the comment of the parameter. | `comment(...)` |
| `build` | 无 | ProcedureParameter | Builds the stored procedure parameter. | `build(...)` |

---

## 包: org.apache.spark.sql.connector.distributions

**类数量**: 1

### Distributions

**完整类名**: `org.apache.spark.sql.connector.distributions.Distributions`

**描述**: Helper methods to create distributions to pass into Spark.

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `unspecified` | 无 | UnspecifiedDistribution | Creates a distribution where no promises are made about co-location of data. | `unspecified(...)` |
| `clustered` | Expression[] clustering | ClusteredDistribution | Creates a distribution where tuples that share the same values for clustering expressions are | `clustered(...)` |
| `ordered` | SortOrder[] ordering | OrderedDistribution | Creates a distribution where tuples have been ordered across partitions according | `ordered(...)` |

---

## 包: org.apache.spark.sql.connector.expressions

**类数量**: 7

### Cast

**完整类名**: `org.apache.spark.sql.connector.expressions.Cast`

**描述**: Represents a cast expression in the public logical expression API.

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `expression` | 无 | Expression | 暂无描述 | `expression(...)` |
| `expressionDataType` | 无 | DataType | 暂无描述 | `expressionDataType(...)` |
| `dataType` | 无 | DataType | 暂无描述 | `dataType(...)` |
| `children` | 无 | Expression[] | 暂无描述 | `children(...)` |

---

### Expressions

**完整类名**: `org.apache.spark.sql.connector.expressions.Expressions`

**描述**: Helper methods to create logical transforms to pass into Spark.

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | String name, Expression... args | Transform | 暂无描述 | `apply(...)` |
| `column` | String name | NamedReference | 暂无描述 | `column(...)` |
| `literal` | T value | <T> Literal<T> | 暂无描述 | `literal(...)` |
| `bucket` | int numBuckets, String... columns | Transform | 暂无描述 | `bucket(...)` |
| `identity` | String column | Transform | 暂无描述 | `identity(...)` |
| `years` | String column | Transform | 暂无描述 | `years(...)` |
| `months` | String column | Transform | 暂无描述 | `months(...)` |
| `days` | String column | Transform | 暂无描述 | `days(...)` |
| `hours` | String column | Transform | 暂无描述 | `hours(...)` |
| `sort` | Expression expr, SortDirection direction, NullOrdering nullOrder | SortOrder | 暂无描述 | `sort(...)` |
| `sort` | Expression expr, SortDirection direction | SortOrder | Create a sort expression. | `sort(...)` |

---

### Extract

**完整类名**: `org.apache.spark.sql.connector.expressions.Extract`

**描述**: Represent an extract function, which extracts and returns the value of a

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `field` | 无 | String | 暂无描述 | `field(...)` |
| `source` | 无 | Expression | 暂无描述 | `source(...)` |
| `children` | 无 | Expression[] | 暂无描述 | `children(...)` |

---

### GeneralScalarExpression

**完整类名**: `org.apache.spark.sql.connector.expressions.GeneralScalarExpression`

**描述**: The general representation of SQL scalar expressions, which contains the upper-cased

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | String | 暂无描述 | `name(...)` |
| `children` | 无 | Expression[] | 暂无描述 | `children(...)` |

---

### GetArrayItem

**完整类名**: `org.apache.spark.sql.connector.expressions.GetArrayItem`

**描述**: Get array item expression.

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `childArray` | 无 | Expression | 暂无描述 | `childArray(...)` |
| `ordinal` | 无 | Expression | 暂无描述 | `ordinal(...)` |
| `failOnError` | 无 | boolean | 暂无描述 | `failOnError(...)` |
| `children` | 无 | Expression[] | 暂无描述 | `children(...)` |

---

### SortDirection

**完整类名**: `org.apache.spark.sql.connector.expressions.SortDirection`

**描述**: A sort direction used in sorting expressions.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultNullOrdering` | 无 | NullOrdering | Returns the default null ordering to use if no null ordering is specified explicitly. | `defaultNullOrdering(...)` |

---

### UserDefinedScalarFunc

**完整类名**: `org.apache.spark.sql.connector.expressions.UserDefinedScalarFunc`

**描述**: The general representation of user defined scalar function, which contains the upper-cased

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | String | 暂无描述 | `name(...)` |
| `canonicalName` | 无 | String | 暂无描述 | `canonicalName(...)` |
| `children` | 无 | Expression[] | 暂无描述 | `children(...)` |

---

## 包: org.apache.spark.sql.connector.expressions.aggregate

**类数量**: 1

### UserDefinedAggregateFunc

**完整类名**: `org.apache.spark.sql.connector.expressions.aggregate.UserDefinedAggregateFunc`

**描述**: The general representation of user defined aggregate function, which implements

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | String | 暂无描述 | `name(...)` |
| `canonicalName` | 无 | String | 暂无描述 | `canonicalName(...)` |
| `isDistinct` | 无 | boolean | 暂无描述 | `isDistinct(...)` |
| `children` | 无 | Expression[] | 暂无描述 | `children(...)` |

---

## 包: org.apache.spark.sql.connector.metric

**类数量**: 1

### CustomTaskMetric

**完整类名**: `org.apache.spark.sql.connector.metric.CustomTaskMetric`

**描述**: A custom task metric. This is a logical representation of a metric reported by data sources

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | String | 暂无描述 | `name(...)` |
| `value` | 无 | long | 暂无描述 | `value(...)` |

---

## 包: org.apache.spark.sql.connector.read

**类数量**: 1

### SupportsPushDownJoin

**完整类名**: `org.apache.spark.sql.connector.read.SupportsPushDownJoin`

**描述**: A mix-in interface for {@link ScanBuilder}. Data sources can implement this interface to

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `prettyString` | 无 | String | 暂无描述 | `prettyString(...)` |

---

## 包: org.apache.spark.sql.connector.read.partitioning

**类数量**: 2

### KeyGroupedPartitioning

**完整类名**: `org.apache.spark.sql.connector.read.partitioning.KeyGroupedPartitioning`

**描述**: Represents a partitioning where rows are split across partitions based on the

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `keys` | 无 | Expression[] | Returns the partition transform expressions for this partitioning. | `keys(...)` |
| `numPartitions` | 无 | int | Returns the partition transform expressions for this partitioning. | `numPartitions(...)` |

---

### UnknownPartitioning

**完整类名**: `org.apache.spark.sql.connector.read.partitioning.UnknownPartitioning`

**描述**: Represents a partitioning where rows are split across partitions in an unknown pattern.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `numPartitions` | 无 | int | 暂无描述 | `numPartitions(...)` |

---

## 包: org.apache.spark.sql.connector.read.streaming

**类数量**: 3

### ReadMaxBytes

**完整类名**: `org.apache.spark.sql.connector.read.streaming.ReadMaxBytes`

**描述**: Represents a {@link ReadLimit} where the {@link MicroBatchStream} should scan files which total

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `maxBytes` | 无 | long | 暂无描述 | `maxBytes(...)` |

---

### ReadMaxFiles

**完整类名**: `org.apache.spark.sql.connector.read.streaming.ReadMaxFiles`

**描述**: Represents a {@link ReadLimit} where the {@link MicroBatchStream} should scan approximately the

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `maxFiles` | 无 | int | 暂无描述 | `maxFiles(...)` |

---

### SupportsRealTimeRead

**完整类名**: `org.apache.spark.sql.connector.read.streaming.SupportsRealTimeRead`

**描述**: A variation on {@link PartitionReader} for use with low latency streaming processing.

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `newStatusWithoutArrivalTime` | boolean hasRecord | RecordStatus | 暂无描述 | `newStatusWithoutArrivalTime(...)` |
| `newStatusWithArrivalTimeMs` | Long recArrivalTime | RecordStatus | 暂无描述 | `newStatusWithArrivalTimeMs(...)` |
| `hasRecord` | 无 | boolean | 暂无描述 | `hasRecord(...)` |
| `recArrivalTime` | 无 | Optional<Long> | 暂无描述 | `recArrivalTime(...)` |

---

## 包: org.apache.spark.sql.connector.util

**类数量**: 1

### V2ExpressionSQLBuilder

**完整类名**: `org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder`

**描述**: The builder to generate SQL from V2 expressions.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | Expression expr | String | 暂无描述 | `build(...)` |

---

## 包: org.apache.spark.sql.connector.write

**类数量**: 1

### WriteBuilder

**完整类名**: `org.apache.spark.sql.connector.write.WriteBuilder`

**描述**: An interface for building the {@link Write}. Implementations can mix in some interfaces to

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toBatch` | 无 | BatchWrite | Returns a logical {@link Write} shared between batch and streaming. | `toBatch(...)` |
| `toStreaming` | 无 | StreamingWrite | 暂无描述 | `toStreaming(...)` |

---

## 包: org.apache.spark.sql.execution

**类数量**: 1

### KVSorterIterator

**完整类名**: `org.apache.spark.sql.execution.KVSorterIterator`

**描述**: A class for performing external sorting on key-value records. Both key and value are UnsafeRows.

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `insertKV` | UnsafeRow key, UnsafeRow value | void | 暂无描述 | `insertKV(...)` |
| `merge` | UnsafeKVExternalSorter other | void | Merges another UnsafeKVExternalSorter into `this`, the other one will be emptied. | `merge(...)` |
| `sortedIterator` | 无 | KVSorterIterator | Returns a sorted iterator. It is the caller's responsibility to call `cleanupResources()` | `sortedIterator(...)` |
| `getSpillSize` | 无 | long | Return the total number of bytes that has been spilled into disk so far. | `getSpillSize(...)` |
| `getPeakMemoryUsedBytes` | 无 | long | Return the peak memory used so far, in bytes. | `getPeakMemoryUsedBytes(...)` |
| `cleanupResources` | 无 | void | Frees this sorter's in-memory data structures and cleans up its spill files. | `cleanupResources(...)` |
| `compare` | Object baseObj1,
        long baseOff1,
        int baseLen1,
        Object baseObj2,
        long baseOff2,
        int baseLen2 | int | 暂无描述 | `compare(...)` |
| `next` | 无 | boolean | 暂无描述 | `next(...)` |
| `getKey` | 无 | UnsafeRow | 暂无描述 | `getKey(...)` |
| `getValue` | 无 | UnsafeRow | 暂无描述 | `getValue(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |

---

## 包: org.apache.spark.sql.execution.datasources

**类数量**: 2

### HadoopLineRecordReader

**完整类名**: `org.apache.spark.sql.execution.datasources.HadoopLineRecordReader`

**描述**: Inlined from Hadoop's LineRecordReader to add support for passing compression option

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initialize` | InputSplit genericSplit,
                           TaskAttemptContext context | void | 暂无描述 | `initialize(...)` |
| `nextKeyValue` | 无 | boolean | 暂无描述 | `nextKeyValue(...)` |
| `getCurrentKey` | 无 | LongWritable | 暂无描述 | `getCurrentKey(...)` |
| `getCurrentValue` | 无 | Text | 暂无描述 | `getCurrentValue(...)` |
| `getProgress` | 无 | float | Get the progress within the split | `getProgress(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |

---

### SchemaColumnConvertNotSupportedException

**完整类名**: `org.apache.spark.sql.execution.datasources.SchemaColumnConvertNotSupportedException`

**描述**: Exception thrown when the parquet reader find column type mismatches.

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getColumn` | 无 | String | Logical column type in the parquet schema the parquet reader use to parse all files. | `getColumn(...)` |
| `getPhysicalType` | 无 | String | Logical column type in the parquet schema the parquet reader use to parse all files. | `getPhysicalType(...)` |
| `getLogicalType` | 无 | String | 暂无描述 | `getLogicalType(...)` |

---

## 包: org.apache.spark.sql.execution.datasources.orc

**类数量**: 8

### OrcArrayColumnVector

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcArrayColumnVector`

**描述**: A column vector implementation for Spark's {@link ArrayType}.

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getArray` | int rowId | ColumnarArray | 暂无描述 | `getArray(...)` |
| `getBoolean` | int rowId | boolean | 暂无描述 | `getBoolean(...)` |
| `getByte` | int rowId | byte | 暂无描述 | `getByte(...)` |
| `getShort` | int rowId | short | 暂无描述 | `getShort(...)` |
| `getInt` | int rowId | int | 暂无描述 | `getInt(...)` |
| `getLong` | int rowId | long | 暂无描述 | `getLong(...)` |
| `getFloat` | int rowId | float | 暂无描述 | `getFloat(...)` |
| `getDouble` | int rowId | double | 暂无描述 | `getDouble(...)` |
| `getDecimal` | int rowId, int precision, int scale | Decimal | 暂无描述 | `getDecimal(...)` |
| `getUTF8String` | int rowId | UTF8String | 暂无描述 | `getUTF8String(...)` |
| `getBinary` | int rowId | byte[] | 暂无描述 | `getBinary(...)` |
| `getMap` | int rowId | ColumnarMap | 暂无描述 | `getMap(...)` |

---

### OrcAtomicColumnVector

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcAtomicColumnVector`

**描述**: A column vector implementation for Spark's AtomicType.

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getBoolean` | int rowId | boolean | 暂无描述 | `getBoolean(...)` |
| `getByte` | int rowId | byte | 暂无描述 | `getByte(...)` |
| `getShort` | int rowId | short | 暂无描述 | `getShort(...)` |
| `getInt` | int rowId | int | 暂无描述 | `getInt(...)` |
| `getLong` | int rowId | long | 暂无描述 | `getLong(...)` |
| `getFloat` | int rowId | float | 暂无描述 | `getFloat(...)` |
| `getDouble` | int rowId | double | 暂无描述 | `getDouble(...)` |
| `getDecimal` | int rowId, int precision, int scale | Decimal | 暂无描述 | `getDecimal(...)` |
| `getUTF8String` | int rowId | UTF8String | 暂无描述 | `getUTF8String(...)` |
| `getBinary` | int rowId | byte[] | 暂无描述 | `getBinary(...)` |
| `getArray` | int rowId | ColumnarArray | 暂无描述 | `getArray(...)` |
| `getMap` | int rowId | ColumnarMap | 暂无描述 | `getMap(...)` |

---

### OrcColumnStatistics

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcColumnStatistics`

**描述**: Columns statistics interface wrapping ORC {@link ColumnStatistics}s.

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStatistics` | 无 | ColumnStatistics | 暂无描述 | `getStatistics(...)` |
| `get` | int ordinal | OrcColumnStatistics | 暂无描述 | `get(...)` |
| `add` | OrcColumnStatistics newChild | void | 暂无描述 | `add(...)` |

---

### OrcColumnarBatchReader

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcColumnarBatchReader`

**描述**: To support vectorization in WholeStageCodeGen, this reader returns ColumnarBatch.

**方法数**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCurrentKey` | 无 | Void | 暂无描述 | `getCurrentKey(...)` |
| `getCurrentValue` | 无 | ColumnarBatch | 暂无描述 | `getCurrentValue(...)` |
| `getProgress` | 无 | float | 暂无描述 | `getProgress(...)` |
| `nextKeyValue` | 无 | boolean | 暂无描述 | `nextKeyValue(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |
| `initialize` | InputSplit inputSplit, TaskAttemptContext taskAttemptContext | void | Initialize ORC file reader and batch record reader. | `initialize(...)` |
| `initialize` | InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext,
      OrcTail orcTail | void | 暂无描述 | `initialize(...)` |
| `initBatch` | TypeDescription orcSchema,
      StructField[] requiredFields,
      int[] requestedDataColIds,
      int[] requestedPartitionColIds,
      InternalRow partitionValues | void | 暂无描述 | `initBatch(...)` |

---

### OrcCompressionCodec

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcCompressionCodec`

**描述**: A mapper class from Spark supported orc compression codecs to orc compression codecs.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCompressionKind` | 无 | CompressionKind | 暂无描述 | `getCompressionKind(...)` |
| `lowerCaseName` | 无 | String | 暂无描述 | `lowerCaseName(...)` |

---

### OrcFooterReader

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcFooterReader`

**描述**: {@link OrcFooterReader} is a util class which encapsulates the helper

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `readStatistics` | Reader orcReader | OrcColumnStatistics | Read the columns statistics from ORC file footer. | `readStatistics(...)` |

---

### OrcMapColumnVector

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcMapColumnVector`

**描述**: A column vector implementation for Spark's {@link MapType}.

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMap` | int ordinal | ColumnarMap | 暂无描述 | `getMap(...)` |
| `getBoolean` | int rowId | boolean | 暂无描述 | `getBoolean(...)` |
| `getByte` | int rowId | byte | 暂无描述 | `getByte(...)` |
| `getShort` | int rowId | short | 暂无描述 | `getShort(...)` |
| `getInt` | int rowId | int | 暂无描述 | `getInt(...)` |
| `getLong` | int rowId | long | 暂无描述 | `getLong(...)` |
| `getFloat` | int rowId | float | 暂无描述 | `getFloat(...)` |
| `getDouble` | int rowId | double | 暂无描述 | `getDouble(...)` |
| `getDecimal` | int rowId, int precision, int scale | Decimal | 暂无描述 | `getDecimal(...)` |
| `getUTF8String` | int rowId | UTF8String | 暂无描述 | `getUTF8String(...)` |
| `getBinary` | int rowId | byte[] | 暂无描述 | `getBinary(...)` |
| `getArray` | int rowId | ColumnarArray | 暂无描述 | `getArray(...)` |

---

### OrcStructColumnVector

**完整类名**: `org.apache.spark.sql.execution.datasources.orc.OrcStructColumnVector`

**描述**: A column vector implementation for Spark's {@link StructType}.

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getBoolean` | int rowId | boolean | 暂无描述 | `getBoolean(...)` |
| `getByte` | int rowId | byte | 暂无描述 | `getByte(...)` |
| `getShort` | int rowId | short | 暂无描述 | `getShort(...)` |
| `getInt` | int rowId | int | 暂无描述 | `getInt(...)` |
| `getLong` | int rowId | long | 暂无描述 | `getLong(...)` |
| `getFloat` | int rowId | float | 暂无描述 | `getFloat(...)` |
| `getDouble` | int rowId | double | 暂无描述 | `getDouble(...)` |
| `getDecimal` | int rowId, int precision, int scale | Decimal | 暂无描述 | `getDecimal(...)` |
| `getUTF8String` | int rowId | UTF8String | 暂无描述 | `getUTF8String(...)` |
| `getBinary` | int rowId | byte[] | 暂无描述 | `getBinary(...)` |
| `getArray` | int rowId | ColumnarArray | 暂无描述 | `getArray(...)` |
| `getMap` | int rowId | ColumnarMap | 暂无描述 | `getMap(...)` |

---

## 包: org.apache.spark.sql.execution.datasources.parquet

**类数量**: 10

### ParquetCompressionCodec

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodec`

**描述**: A mapper class from Spark supported parquet compression codecs to parquet compression codecs.

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCompressionCodec` | 无 | CompressionCodecName | 暂无描述 | `getCompressionCodec(...)` |
| `fromString` | String s | ParquetCompressionCodec | 暂无描述 | `fromString(...)` |
| `lowerCaseName` | 无 | String | 暂无描述 | `lowerCaseName(...)` |

---

### ParquetFooterReader

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.ParquetFooterReader`

**描述**: `ParquetFooterReader` is a util class which encapsulates the helper

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `readFooter` | HadoopInputFile inputFile,
      ParquetMetadataConverter.MetadataFilter filter | ParquetMetadata | 暂无描述 | `readFooter(...)` |
| `openFileAndReadFooter` | Configuration hadoopConf,
      PartitionedFile file,
      boolean keepInputStreamOpen | OpenedParquetFooter | 暂无描述 | `openFileAndReadFooter(...)` |

---

### ParquetVectorUpdaterFactory

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.ParquetVectorUpdaterFactory`

**描述**: Updater should not be called if all values are nulls, so all methods throw exception here.

**方法数**: 146

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getUpdater` | ColumnDescriptor descriptor, DataType sparkType | ParquetVectorUpdater | 暂无描述 | `getUpdater(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | Updater should not be called if all values are nulls, so all methods throw exception here. | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `readValues` | int total, int offset, WritableColumnVector values,
       VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `readValues` | int total, int offset, WritableColumnVector values,
       VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `readValues` | int total,
        int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValues(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |
| `skipValues` | int total, VectorizedValuesReader valuesReader | void | 暂无描述 | `skipValues(...)` |
| `readValue` | int offset,
        WritableColumnVector values,
        VectorizedValuesReader valuesReader | void | 暂无描述 | `readValue(...)` |
| `decodeSingleDictionaryId` | int offset,
        WritableColumnVector values,
        WritableColumnVector dictionaryIds,
        Dictionary dictionary | void | 暂无描述 | `decodeSingleDictionaryId(...)` |

---

### VectorizedColumnReader

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.VectorizedColumnReader`

**描述**: Decoder to return values from a single column.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `visit` | DataPageV1 dataPageV1 | Integer | 暂无描述 | `visit(...)` |
| `visit` | DataPageV2 dataPageV2 | Integer | 暂无描述 | `visit(...)` |

---

### VectorizedDeltaBinaryPackedReader

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.VectorizedDeltaBinaryPackedReader`

**描述**: An implementation of the Parquet DELTA_BINARY_PACKED decoder that supports the vectorized

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initFromPage` | int valueCount, ByteBufferInputStream in | void | 暂无描述 | `initFromPage(...)` |
| `readByte` | 无 | byte | 暂无描述 | `readByte(...)` |
| `readShort` | 无 | short | 暂无描述 | `readShort(...)` |
| `readInteger` | 无 | int | 暂无描述 | `readInteger(...)` |
| `readLong` | 无 | long | 暂无描述 | `readLong(...)` |
| `readBytes` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBytes(...)` |
| `readShorts` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readShorts(...)` |
| `readIntegers` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readIntegers(...)` |
| `readIntegersWithRebase` | int total, WritableColumnVector c, int rowId, boolean failIfRebase | void | 暂无描述 | `readIntegersWithRebase(...)` |
| `readUnsignedIntegers` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readUnsignedIntegers(...)` |
| `readUnsignedLongs` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readUnsignedLongs(...)` |
| `readLongs` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readLongs(...)` |
| `readLongsWithRebase` | int total, WritableColumnVector c, int rowId, boolean failIfRebase, String timeZone | void | 暂无描述 | `readLongsWithRebase(...)` |
| `skipBytes` | int total | void | 暂无描述 | `skipBytes(...)` |
| `skipShorts` | int total | void | 暂无描述 | `skipShorts(...)` |
| `skipIntegers` | int total | void | 暂无描述 | `skipIntegers(...)` |
| `skipLongs` | int total | void | 暂无描述 | `skipLongs(...)` |

---

### VectorizedDeltaByteArrayReader

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.VectorizedDeltaByteArrayReader`

**描述**: An implementation of the Parquet DELTA_BYTE_ARRAY decoder that supports the vectorized

**方法数**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initFromPage` | int valueCount, ByteBufferInputStream in | void | 暂无描述 | `initFromPage(...)` |
| `readBinary` | int len | Binary | 暂无描述 | `readBinary(...)` |
| `readBinary` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBinary(...)` |
| `readGeometry` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readGeometry(...)` |
| `readGeography` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readGeography(...)` |
| `setPreviousReader` | ValuesReader reader | void | 暂无描述 | `setPreviousReader(...)` |
| `skipBinary` | int total | void | 暂无描述 | `skipBinary(...)` |

---

### VectorizedDeltaLengthByteArrayReader

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.VectorizedDeltaLengthByteArrayReader`

**描述**: An implementation of the Parquet DELTA_LENGTH_BYTE_ARRAY decoder that supports the vectorized

**方法数**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initFromPage` | int valueCount, ByteBufferInputStream in | void | 暂无描述 | `initFromPage(...)` |
| `readBinary` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBinary(...)` |
| `readGeometry` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readGeometry(...)` |
| `readGeography` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readGeography(...)` |
| `getBytes` | int rowId | ByteBuffer | 暂无描述 | `getBytes(...)` |
| `skipBinary` | int total | void | 暂无描述 | `skipBinary(...)` |

---

### VectorizedParquetRecordReader

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.VectorizedParquetRecordReader`

**描述**: A specialized RecordReader that reads into InternalRows or ColumnarBatches directly using the

**方法数**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initialize` | InputSplit inputSplit, TaskAttemptContext taskAttemptContext | void | Implementation of RecordReader API. | `initialize(...)` |
| `initialize` | InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext,
      Option<HadoopInputFile> inputFile,
      Option<SeekableInputStream> inputStream,
      Option<ParquetMetadata> fileFooter | void | 暂无描述 | `initialize(...)` |
| `initialize` | String path, List<String> columns | void | 暂无描述 | `initialize(...)` |
| `initialize` | MessageType fileSchema,
      MessageType requestedSchema,
      ParquetRowGroupReader rowGroupReader,
      int totalRowCount | void | 暂无描述 | `initialize(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |
| `nextKeyValue` | 无 | boolean | 暂无描述 | `nextKeyValue(...)` |
| `getCurrentValue` | 无 | Object | 暂无描述 | `getCurrentValue(...)` |
| `getProgress` | 无 | float | 暂无描述 | `getProgress(...)` |
| `initBatch` | StructType partitionColumns, InternalRow partitionValues | void | 暂无描述 | `initBatch(...)` |
| `resultBatch` | 无 | ColumnarBatch | 暂无描述 | `resultBatch(...)` |
| `enableReturningBatches` | 无 | void | Can be called before any rows are returned to enable returning columnar batches directly. | `enableReturningBatches(...)` |
| `nextBatch` | 无 | boolean | Advances to the next batch of rows. Returns false if there are no more. | `nextBatch(...)` |

---

### VectorizedPlainValuesReader

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.VectorizedPlainValuesReader`

**描述**: An implementation of the Parquet PLAIN decoder that supports the vectorized interface.

**方法数**: 33

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initFromPage` | int valueCount, ByteBufferInputStream in | void | 暂无描述 | `initFromPage(...)` |
| `skip` | 无 | void | 暂无描述 | `skip(...)` |
| `readBooleans` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBooleans(...)` |
| `skipBooleans` | int total | void | 暂无描述 | `skipBooleans(...)` |
| `readIntegers` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readIntegers(...)` |
| `skipIntegers` | int total | void | 暂无描述 | `skipIntegers(...)` |
| `readUnsignedIntegers` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readUnsignedIntegers(...)` |
| `readIntegersWithRebase` | int total, WritableColumnVector c, int rowId, boolean failIfRebase | void | 暂无描述 | `readIntegersWithRebase(...)` |
| `readLongs` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readLongs(...)` |
| `skipLongs` | int total | void | 暂无描述 | `skipLongs(...)` |
| `readUnsignedLongs` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readUnsignedLongs(...)` |
| `readLongsWithRebase` | int total,
      WritableColumnVector c,
      int rowId,
      boolean failIfRebase,
      String timeZone | void | 暂无描述 | `readLongsWithRebase(...)` |
| `readFloats` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readFloats(...)` |
| `skipFloats` | int total | void | 暂无描述 | `skipFloats(...)` |
| `readDoubles` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readDoubles(...)` |
| `skipDoubles` | int total | void | 暂无描述 | `skipDoubles(...)` |
| `readBytes` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBytes(...)` |
| `skipBytes` | int total | void | 暂无描述 | `skipBytes(...)` |
| `readShorts` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readShorts(...)` |
| `skipShorts` | int total | void | 暂无描述 | `skipShorts(...)` |
| `readBoolean` | 无 | boolean | 暂无描述 | `readBoolean(...)` |
| `readInteger` | 无 | int | 暂无描述 | `readInteger(...)` |
| `readLong` | 无 | long | 暂无描述 | `readLong(...)` |
| `readByte` | 无 | byte | 暂无描述 | `readByte(...)` |
| `readShort` | 无 | short | 暂无描述 | `readShort(...)` |
| `readFloat` | 无 | float | 暂无描述 | `readFloat(...)` |
| `readDouble` | 无 | double | 暂无描述 | `readDouble(...)` |
| `readBinary` | int total, WritableColumnVector v, int rowId | void | 暂无描述 | `readBinary(...)` |
| `skipBinary` | int total | void | 暂无描述 | `skipBinary(...)` |
| `readBinary` | int len | Binary | 暂无描述 | `readBinary(...)` |
| `skipFixedLenByteArray` | int total, int len | void | 暂无描述 | `skipFixedLenByteArray(...)` |
| `readGeometry` | int total, WritableColumnVector v, int rowId | void | 暂无描述 | `readGeometry(...)` |
| `readGeography` | int total, WritableColumnVector v, int rowId | void | 暂无描述 | `readGeography(...)` |

---

### VectorizedReaderBase

**完整类名**: `org.apache.spark.sql.execution.datasources.parquet.VectorizedReaderBase`

**描述**: Base class for implementations of VectorizedValuesReader. Mainly to avoid duplication

**方法数**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `skip` | 无 | void | 暂无描述 | `skip(...)` |
| `readByte` | 无 | byte | 暂无描述 | `readByte(...)` |
| `readShort` | 无 | short | 暂无描述 | `readShort(...)` |
| `readBinary` | int len | Binary | 暂无描述 | `readBinary(...)` |
| `readBooleans` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBooleans(...)` |
| `readBytes` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBytes(...)` |
| `readShorts` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readShorts(...)` |
| `readIntegers` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readIntegers(...)` |
| `readIntegersWithRebase` | int total, WritableColumnVector c, int rowId,
      boolean failIfRebase | void | 暂无描述 | `readIntegersWithRebase(...)` |
| `readUnsignedIntegers` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readUnsignedIntegers(...)` |
| `readUnsignedLongs` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readUnsignedLongs(...)` |
| `readLongs` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readLongs(...)` |
| `readLongsWithRebase` | int total, WritableColumnVector c, int rowId,
      boolean failIfRebase, String timeZone | void | 暂无描述 | `readLongsWithRebase(...)` |
| `readFloats` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readFloats(...)` |
| `readDoubles` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readDoubles(...)` |
| `readBinary` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readBinary(...)` |
| `readGeometry` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readGeometry(...)` |
| `readGeography` | int total, WritableColumnVector c, int rowId | void | 暂无描述 | `readGeography(...)` |
| `skipBooleans` | int total | void | 暂无描述 | `skipBooleans(...)` |
| `skipBytes` | int total | void | 暂无描述 | `skipBytes(...)` |
| `skipShorts` | int total | void | 暂无描述 | `skipShorts(...)` |
| `skipIntegers` | int total | void | 暂无描述 | `skipIntegers(...)` |
| `skipLongs` | int total | void | 暂无描述 | `skipLongs(...)` |
| `skipFloats` | int total | void | 暂无描述 | `skipFloats(...)` |
| `skipDoubles` | int total | void | 暂无描述 | `skipDoubles(...)` |
| `skipBinary` | int total | void | 暂无描述 | `skipBinary(...)` |
| `skipFixedLenByteArray` | int total, int len | void | 暂无描述 | `skipFixedLenByteArray(...)` |

---

## 包: org.apache.spark.sql.execution.vectorized

**类数量**: 3

### AggregateHashMap

**完整类名**: `org.apache.spark.sql.execution.vectorized.AggregateHashMap`

**描述**: This is an illustrative implementation of an append-only single-key/single value aggregate hash

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `findOrInsert` | long key | MutableColumnarRow | 暂无描述 | `findOrInsert(...)` |
| `find` | long key | int | 暂无描述 | `find(...)` |

---

### ColumnVectorUtils

**完整类名**: `org.apache.spark.sql.execution.vectorized.ColumnVectorUtils`

**描述**: Utilities to help manipulate data associate with ColumnVectors. These should be used mostly

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `populate` | ConstantColumnVector col, InternalRow row, int fieldIdx | void | Populates the value of `row[fieldIdx]` into `ConstantColumnVector`. | `populate(...)` |
| `toJavaIntArray` | ColumnarArray array | int[] | Returns the array data as the java primitive array. | `toJavaIntArray(...)` |
| `toJavaIntMap` | ColumnarMap map | Map<Integer, Integer> | 暂无描述 | `toJavaIntMap(...)` |
| `toBatch` | StructType schema, MemoryMode memMode, Iterator<Row> row | ColumnarBatch | Converts an iterator of rows into a single ColumnBatch. | `toBatch(...)` |

---

### ConstantColumnVector

**完整类名**: `org.apache.spark.sql.execution.vectorized.ConstantColumnVector`

**描述**: This class adds the constant support to ColumnVector.

**方法数**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `closeIfFreeable` | 无 | void | 暂无描述 | `closeIfFreeable(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |
| `hasNull` | 无 | boolean | 暂无描述 | `hasNull(...)` |
| `numNulls` | 无 | int | 暂无描述 | `numNulls(...)` |
| `isNullAt` | int rowId | boolean | 暂无描述 | `isNullAt(...)` |
| `setNull` | 无 | void | Sets all rows as `null` | `setNull(...)` |
| `setNotNull` | 无 | void | Sets all rows as `null` | `setNotNull(...)` |
| `getBoolean` | int rowId | boolean | Sets all rows as not `null` | `getBoolean(...)` |
| `setBoolean` | boolean value | void | Sets the boolean `value` for all rows | `setBoolean(...)` |
| `getByte` | int rowId | byte | Sets the boolean `value` for all rows | `getByte(...)` |
| `setByte` | byte value | void | Sets the byte `value` for all rows | `setByte(...)` |
| `getShort` | int rowId | short | Sets the byte `value` for all rows | `getShort(...)` |
| `setShort` | short value | void | Sets the short `value` for all rows | `setShort(...)` |
| `getInt` | int rowId | int | Sets the short `value` for all rows | `getInt(...)` |
| `setInt` | int value | void | Sets the int `value` for all rows | `setInt(...)` |
| `getLong` | int rowId | long | Sets the int `value` for all rows | `getLong(...)` |
| `setLong` | long value | void | Sets the long `value` for all rows | `setLong(...)` |
| `getFloat` | int rowId | float | Sets the long `value` for all rows | `getFloat(...)` |
| `setFloat` | float value | void | Sets the float `value` for all rows | `setFloat(...)` |
| `getDouble` | int rowId | double | Sets the float `value` for all rows | `getDouble(...)` |
| `setDouble` | double value | void | Sets the double `value` for all rows | `setDouble(...)` |
| `getArray` | int rowId | ColumnarArray | Sets the double `value` for all rows | `getArray(...)` |
| `setArray` | ColumnarArray value | void | Sets the `ColumnarArray` `value` for all rows | `setArray(...)` |
| `getMap` | int ordinal | ColumnarMap | Sets the `ColumnarArray` `value` for all rows | `getMap(...)` |
| `setMap` | ColumnarMap value | void | Sets the `ColumnarMap` `value` for all rows | `setMap(...)` |
| `getDecimal` | int rowId, int precision, int scale | Decimal | Sets the `ColumnarMap` `value` for all rows | `getDecimal(...)` |
| `setDecimal` | Decimal value, int precision | void | Sets the `Decimal` `value` with the precision for all rows | `setDecimal(...)` |
| `getUTF8String` | int rowId | UTF8String | 暂无描述 | `getUTF8String(...)` |
| `setUtf8String` | UTF8String value | void | Sets the `UTF8String` `value` for all rows | `setUtf8String(...)` |
| `getBinary` | int rowId | byte[] | Sets the byte array `value` for all rows | `getBinary(...)` |
| `setBinary` | byte[] value | void | Sets the binary `value` for all rows | `setBinary(...)` |
| `getChild` | int ordinal | ColumnVector | Sets the binary `value` for all rows | `getChild(...)` |
| `setChild` | int ordinal, ConstantColumnVector value | void | Sets the child `ConstantColumnVector` `value` at the given ordinal for all rows | `setChild(...)` |
| `setCalendarInterval` | CalendarInterval value | void | Sets the CalendarInterval `value` for all rows | `setCalendarInterval(...)` |
| `setVariant` | VariantVal value | void | Sets the Variant `value` for all rows | `setVariant(...)` |

---

## 包: org.apache.spark.sql.expressions.javalang

**类数量**: 1

### typed

**完整类名**: `org.apache.spark.sql.expressions.javalang.typed`

**描述**: Type-safe functions available for {@link org.apache.spark.sql.Dataset} operations in Java.

**方法数**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `avg` | MapFunction<T, Double> f | <T> TypedColumn<T, Double> | Average aggregate function. | `avg(...)` |
| `count` | MapFunction<T, Object> f | <T> TypedColumn<T, Long> | Count aggregate function. | `count(...)` |
| `sum` | MapFunction<T, Double> f | <T> TypedColumn<T, Double> | Sum aggregate function for floating point (double) type. | `sum(...)` |
| `sumLong` | MapFunction<T, Long> f | <T> TypedColumn<T, Long> | Sum aggregate function for integral (long, i.e. 64 bit integer) type. | `sumLong(...)` |

---

## 包: org.apache.spark.sql.internal

**类数量**: 1

### NonClosableMutableURLClassLoader

**完整类名**: `org.apache.spark.sql.internal.NonClosableMutableURLClassLoader`

**描述**: This class loader cannot be closed (its `close` method is a no-op).

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | void | 暂无描述 | `close(...)` |

---

## 包: org.apache.spark.sql.internal.types

**类数量**: 3

### CartesianSpatialReferenceSystemMapper

**完整类名**: `org.apache.spark.sql.internal.types.CartesianSpatialReferenceSystemMapper`

**描述**: Class for providing SRS mappings for cartesian spatial reference systems.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStringId` | int srid | String | 暂无描述 | `getStringId(...)` |
| `getSrid` | String stringId | Integer | 暂无描述 | `getSrid(...)` |

---

### GeographicSpatialReferenceSystemMapper

**完整类名**: `org.apache.spark.sql.internal.types.GeographicSpatialReferenceSystemMapper`

**描述**: Class for providing SRS mappings for geographic spatial reference systems.

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStringId` | int srid | String | 暂无描述 | `getStringId(...)` |
| `getSrid` | String stringId | Integer | 暂无描述 | `getSrid(...)` |

---

### SpatialReferenceSystemCache

**完整类名**: `org.apache.spark.sql.internal.types.SpatialReferenceSystemCache`

**描述**: Class for maintaining the mappings between supported SRID/CRS values and the corresponding SRS.

**方法数**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getInstance` | 无 | SpatialReferenceSystemCache | 暂无描述 | `getInstance(...)` |
| `getSrsInfo` | int srid | SpatialReferenceSystemInformation | 暂无描述 | `getSrsInfo(...)` |
| `getSrsInfo` | String stringId | SpatialReferenceSystemInformation | 暂无描述 | `getSrsInfo(...)` |
| `getSridToSrs` | 无 | Map<Integer, SpatialReferenceSystemInformation> | Returns an unmodifiable view of the SRID-to-SRS map. | `getSridToSrs(...)` |
| `getStringIdToSrs` | 无 | Map<String, SpatialReferenceSystemInformation> | Returns an unmodifiable view of the string CRS ID-to-SRS map. | `getStringIdToSrs(...)` |

---

## 包: org.apache.spark.sql.streaming

**类数量**: 4

### GroupStateTimeout

**完整类名**: `org.apache.spark.sql.streaming.GroupStateTimeout`

**描述**: Represents the type of timeouts possible for the Dataset operations

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ProcessingTimeTimeout` | 无 | GroupStateTimeout | 暂无描述 | `ProcessingTimeTimeout(...)` |
| `EventTimeTimeout` | 无 | GroupStateTimeout | 暂无描述 | `EventTimeTimeout(...)` |
| `NoTimeout` | 无 | GroupStateTimeout | 暂无描述 | `NoTimeout(...)` |

---

### OutputMode

**完整类名**: `org.apache.spark.sql.streaming.OutputMode`

**描述**: OutputMode describes what data will be written to a streaming sink when there is

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Append` | 无 | OutputMode | 暂无描述 | `Append(...)` |
| `Complete` | 无 | OutputMode | 暂无描述 | `Complete(...)` |
| `Update` | 无 | OutputMode | 暂无描述 | `Update(...)` |

---

### TimeMode

**完整类名**: `org.apache.spark.sql.streaming.TimeMode`

**描述**: Represents the time modes (used for specifying timers and ttl) possible for

**方法数**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `None` | 无 | TimeMode | Neither timers nor ttl is supported in this mode. | `None(...)` |
| `ProcessingTime` | 无 | TimeMode | Stateful processor that uses query processing time to register timers and | `ProcessingTime(...)` |
| `EventTime` | 无 | TimeMode | Stateful processor that uses event time to register timers. Note that ttl is not | `EventTime(...)` |

---

### Trigger

**完整类名**: `org.apache.spark.sql.streaming.Trigger`

**描述**: Policy used to indicate how often results should be produced by a [[StreamingQuery]].

**方法数**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ProcessingTime` | long intervalMs | Trigger | A trigger policy that runs a query periodically based on an interval in processing time. | `ProcessingTime(...)` |
| `ProcessingTime` | long interval, TimeUnit timeUnit | Trigger | 暂无描述 | `ProcessingTime(...)` |
| `ProcessingTime` | Duration interval | Trigger | 暂无描述 | `ProcessingTime(...)` |
| `ProcessingTime` | String interval | Trigger | 暂无描述 | `ProcessingTime(...)` |
| `Once` | 无 | Trigger | 暂无描述 | `Once(...)` |
| `AvailableNow` | 无 | Trigger | 暂无描述 | `AvailableNow(...)` |
| `Continuous` | long intervalMs | Trigger | A trigger that continuously processes streaming data, asynchronously checkpointing at | `Continuous(...)` |
| `Continuous` | long interval, TimeUnit timeUnit | Trigger | 暂无描述 | `Continuous(...)` |
| `Continuous` | Duration interval | Trigger | 暂无描述 | `Continuous(...)` |
| `Continuous` | String interval | Trigger | 暂无描述 | `Continuous(...)` |
| `RealTime` | long batchDurationMs | Trigger | A trigger for real time mode, with batch at the specified duration. | `RealTime(...)` |
| `RealTime` | long batchDuration, TimeUnit timeUnit | Trigger | A trigger for real time mode, with batch at the specified duration. | `RealTime(...)` |
| `RealTime` | Duration batchDuration | Trigger | 暂无描述 | `RealTime(...)` |
| `RealTime` | String batchDuration | Trigger | A trigger for real time mode, with batch at the specified duration. | `RealTime(...)` |
| `RealTime` | 无 | Trigger | A trigger for real time mode, with batch at the specified duration. The default duration is 5 | `RealTime(...)` |

---

## 包: org.apache.spark.sql.types

**类数量**: 1

### DataTypes

**完整类名**: `org.apache.spark.sql.types.DataTypes`

**描述**: To get/create specific data type, users should use singleton objects and factory methods

**方法数**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createGeographyType` | int srid | GeographyType | Creates a GeographyType by specifying the SRID value. | `createGeographyType(...)` |
| `createGeographyType` | String crs | GeographyType | Creates a GeographyType by specifying the CRS value. | `createGeographyType(...)` |
| `createGeometryType` | int srid | GeometryType | Creates a GeometryType by specifying the SRID value. | `createGeometryType(...)` |
| `createGeometryType` | String crs | GeometryType | Creates a GeometryType by specifying the CRS value. | `createGeometryType(...)` |
| `createArrayType` | DataType elementType | ArrayType | Creates an ArrayType by specifying the data type of elements ({@code elementType}). | `createArrayType(...)` |
| `createArrayType` | DataType elementType, boolean containsNull | ArrayType | Creates an ArrayType by specifying the data type of elements ({@code elementType}) and | `createArrayType(...)` |
| `createDecimalType` | int precision, int scale | DecimalType | Creates a DecimalType by specifying the precision and scale. | `createDecimalType(...)` |
| `createDecimalType` | 无 | DecimalType | Creates a DecimalType with default precision and scale, which are 10 and 0. | `createDecimalType(...)` |
| `createDayTimeIntervalType` | byte startField, byte endField | DayTimeIntervalType | Creates a DayTimeIntervalType by specifying the start and end fields. | `createDayTimeIntervalType(...)` |
| `createDayTimeIntervalType` | 无 | DayTimeIntervalType | Creates a DayTimeIntervalType with default start and end fields: interval day to second. | `createDayTimeIntervalType(...)` |
| `createYearMonthIntervalType` | byte startField, byte endField | YearMonthIntervalType | Creates a YearMonthIntervalType by specifying the start and end fields. | `createYearMonthIntervalType(...)` |
| `createYearMonthIntervalType` | 无 | YearMonthIntervalType | Creates a YearMonthIntervalType with default start and end fields: interval year to month. | `createYearMonthIntervalType(...)` |
| `createMapType` | DataType keyType, DataType valueType | MapType | Creates a MapType by specifying the data type of keys ({@code keyType}) and values | `createMapType(...)` |
| `createMapType` | DataType keyType,
      DataType valueType,
      boolean valueContainsNull | MapType | 暂无描述 | `createMapType(...)` |
| `createStructField` | String name,
      DataType dataType,
      boolean nullable,
      Metadata metadata | StructField | Creates a StructField by specifying the name ({@code name}), data type ({@code dataType}) and | `createStructField(...)` |
| `createStructField` | String name, DataType dataType, boolean nullable | StructField | Creates a StructField with empty metadata. | `createStructField(...)` |
| `createStructType` | List<StructField> fields | StructType | Creates a StructType with the given list of StructFields ({@code fields}). | `createStructType(...)` |
| `createStructType` | StructField[] fields | StructType | Creates a StructType with the given StructField array ({@code fields}). | `createStructType(...)` |
| `createCharType` | int length | CharType | Creates a CharType with the given length. | `createCharType(...)` |
| `createVarcharType` | int length | VarcharType | Creates a VarcharType with the given length. | `createVarcharType(...)` |

---

## 包: org.apache.spark.sql.util

**类数量**: 2

### CaseInsensitiveStringMap

**完整类名**: `org.apache.spark.sql.util.CaseInsensitiveStringMap`

**描述**: Case-insensitive map of string keys to string values.

**方法数**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `empty` | 无 | CaseInsensitiveStringMap | 暂无描述 | `empty(...)` |
| `size` | 无 | int | 暂无描述 | `size(...)` |
| `isEmpty` | 无 | boolean | 暂无描述 | `isEmpty(...)` |
| `containsKey` | Object key | boolean | 暂无描述 | `containsKey(...)` |
| `containsValue` | Object value | boolean | 暂无描述 | `containsValue(...)` |
| `get` | Object key | String | 暂无描述 | `get(...)` |
| `put` | String key, String value | String | 暂无描述 | `put(...)` |
| `remove` | Object key | String | 暂无描述 | `remove(...)` |
| `putAll` | Map<? extends String, ? extends String> m | void | 暂无描述 | `putAll(...)` |
| `clear` | 无 | void | 暂无描述 | `clear(...)` |
| `keySet` | 无 | Set<String> | 暂无描述 | `keySet(...)` |
| `values` | 无 | Collection<String> | 暂无描述 | `values(...)` |
| `getBoolean` | String key, boolean defaultValue | boolean | Returns the boolean value to which the specified key is mapped, | `getBoolean(...)` |
| `getInt` | String key, int defaultValue | int | Returns the integer value to which the specified key is mapped, | `getInt(...)` |
| `getLong` | String key, long defaultValue | long | Returns the long value to which the specified key is mapped, | `getLong(...)` |
| `getDouble` | String key, double defaultValue | double | Returns the double value to which the specified key is mapped, | `getDouble(...)` |
| `asCaseSensitiveMap` | 无 | Map<String, String> | Returns the original case-sensitive map. | `asCaseSensitiveMap(...)` |

---

### NumericHistogram

**完整类名**: `org.apache.spark.sql.util.NumericHistogram`

**描述**: A generic, re-usable histogram class that supports partial aggregations.

**方法数**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `compareTo` | Coord other | int | 暂无描述 | `compareTo(...)` |
| `reset` | 无 | void | Resets a histogram object to its initial state. allocate() or merge() must be | `reset(...)` |
| `getNumBins` | 无 | int | Returns the number of bins. | `getNumBins(...)` |
| `getUsedBins` | 无 | int | Returns the number of bins. | `getUsedBins(...)` |
| `setUsedBins` | int nusedBins | void | Set the number of bins currently being used by the histogram. | `setUsedBins(...)` |
| `isReady` | 无 | boolean | Returns true if this histogram object has been initialized by calling merge() | `isReady(...)` |
| `getBin` | int b | Coord | Returns a particular histogram bin. | `getBin(...)` |
| `addBin` | double x, double y, int b | void | Returns a particular histogram bin. | `addBin(...)` |
| `allocate` | int num_bins | void | Sets the number of histogram bins to use for approximating data. | `allocate(...)` |
| `merge` | NumericHistogram other | void | Takes a histogram and merges it with the current histogram object. | `merge(...)` |
| `add` | double v | void | 暂无描述 | `add(...)` |

---

## 包: org.apache.spark.sql.vectorized

**类数量**: 2

### ArrowColumnVector

**完整类名**: `org.apache.spark.sql.vectorized.ArrowColumnVector`

**描述**: A column vector backed by Apache Arrow.

**方法数**: 21

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getValueVector` | 无 | ValueVector | A column vector backed by Apache Arrow. | `getValueVector(...)` |
| `hasNull` | 无 | boolean | 暂无描述 | `hasNull(...)` |
| `numNulls` | 无 | int | 暂无描述 | `numNulls(...)` |
| `close` | 无 | void | 暂无描述 | `close(...)` |
| `isNullAt` | int rowId | boolean | 暂无描述 | `isNullAt(...)` |
| `getBoolean` | int rowId | boolean | 暂无描述 | `getBoolean(...)` |
| `getByte` | int rowId | byte | 暂无描述 | `getByte(...)` |
| `getShort` | int rowId | short | 暂无描述 | `getShort(...)` |
| `getInt` | int rowId | int | 暂无描述 | `getInt(...)` |
| `getLong` | int rowId | long | 暂无描述 | `getLong(...)` |
| `getFloat` | int rowId | float | 暂无描述 | `getFloat(...)` |
| `getDouble` | int rowId | double | 暂无描述 | `getDouble(...)` |
| `getDecimal` | int rowId, int precision, int scale | Decimal | 暂无描述 | `getDecimal(...)` |
| `getUTF8String` | int rowId | UTF8String | 暂无描述 | `getUTF8String(...)` |
| `getInterval` | int rowId | CalendarInterval | 暂无描述 | `getInterval(...)` |
| `getBinary` | int rowId | byte[] | 暂无描述 | `getBinary(...)` |
| `getArray` | int rowId | ColumnarArray | 暂无描述 | `getArray(...)` |
| `getMap` | int rowId | ColumnarMap | 暂无描述 | `getMap(...)` |
| `getChild` | int ordinal | ArrowColumnVector | 暂无描述 | `getChild(...)` |
| `getGeography` | int rowId | GeographyVal | 暂无描述 | `getGeography(...)` |
| `getGeometry` | int rowId | GeometryVal | 暂无描述 | `getGeometry(...)` |

---

### ColumnarBatch

**完整类名**: `org.apache.spark.sql.vectorized.ColumnarBatch`

**描述**: This class wraps multiple ColumnVectors as a row-wise table. It provides a row view of this

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | void | Called to close all the columns in this batch. It is not valid to access the data after | `close(...)` |
| `closeIfFreeable` | 无 | void | Called to close all the columns if their resources are freeable between batches. | `closeIfFreeable(...)` |
| `rowIterator` | 无 | Iterator<InternalRow> | Returns an iterator over the rows in this batch. | `rowIterator(...)` |
| `hasNext` | 无 | boolean | 暂无描述 | `hasNext(...)` |
| `next` | 无 | InternalRow | 暂无描述 | `next(...)` |
| `setNumRows` | int numRows | void | Sets the number of rows in this batch. | `setNumRows(...)` |
| `numCols` | 无 | int | Returns the number of columns that make up this batch. | `numCols(...)` |
| `numRows` | 无 | int | Returns the number of rows for read, including filtered rows. | `numRows(...)` |
| `column` | int ordinal | ColumnVector | Returns the number of rows for read, including filtered rows. | `column(...)` |
| `getRow` | int rowId | InternalRow | Returns the row in this batch at `rowId`. Returned row is reused across calls. | `getRow(...)` |

---

## 包: org.apache.spark.status.api.v1

**类数量**: 4

### ApplicationStatus

**完整类名**: `org.apache.spark.status.api.v1.ApplicationStatus`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | String str | ApplicationStatus | 暂无描述 | `fromString(...)` |

---

### StageStatus

**完整类名**: `org.apache.spark.status.api.v1.StageStatus`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | String str | StageStatus | 暂无描述 | `fromString(...)` |

---

### TaskSorting

**完整类名**: `org.apache.spark.status.api.v1.TaskSorting`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | String str | TaskSorting | 暂无描述 | `fromString(...)` |

---

### TaskStatus

**完整类名**: `org.apache.spark.status.api.v1.TaskStatus`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | String str | TaskStatus | 暂无描述 | `fromString(...)` |

---

## 包: org.apache.spark.status.api.v1.streaming

**类数量**: 1

### BatchStatus

**完整类名**: `org.apache.spark.status.api.v1.streaming.BatchStatus`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | String str | BatchStatus | 暂无描述 | `fromString(...)` |

---

## 包: org.apache.spark.streaming.api.java

**类数量**: 7

### JavaDStream

**完整类名**: `org.apache.spark.streaming.api.java.JavaDStream`

**描述**: Scala定义的Java友好接口

**方法数**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wrapRDD` | rdd: RDD[T] | JavaRDD[T] | Scala方法 | `wrapRDD(...)` |
| `filter` | f: JFunction[T, java.lang.Boolean] | JavaDStream[T] | Scala方法 | `filter(...)` |
| `cache` | 无 | JavaDStream[T] | Scala方法 | `cache(...)` |
| `persist` | 无 | JavaDStream[T] | Scala方法 | `persist(...)` |
| `persist` | storageLevel: StorageLevel | JavaDStream[T] | Scala方法 | `persist(...)` |
| `compute` | validTime: Time | JavaRDD[T] | Scala方法 | `compute(...)` |
| `window` | windowDuration: Duration | JavaDStream[T] | Scala方法 | `window(...)` |
| `window` | windowDuration: Duration, slideDuration: Duration | JavaDStream[T] | Scala方法 | `window(...)` |
| `union` | that: JavaDStream[T] | JavaDStream[T] | Scala方法 | `union(...)` |
| `repartition` | numPartitions: Int | JavaDStream[T] | Scala方法 | `repartition(...)` |

---

### JavaMapWithStateDStream

**完整类名**: `org.apache.spark.streaming.api.java.JavaMapWithStateDStream`

**描述**: Scala定义的Java友好接口

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `stateSnapshots` | 无 | JavaPairDStream[KeyType, StateType] | Scala方法 | `stateSnapshots(...)` |

---

### JavaPairDStream

**完整类名**: `org.apache.spark.streaming.api.java.JavaPairDStream`

**描述**: Scala定义的Java友好接口

**方法数**: 30

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wrapRDD` | rdd: RDD[(K, V | Unit | Scala方法 | `wrapRDD(...)` |
| `filter` | f: JFunction[(K, V | Unit | Scala方法 | `filter(...)` |
| `cache` | 无 | JavaPairDStream[K, V] | Scala方法 | `cache(...)` |
| `persist` | 无 | JavaPairDStream[K, V] | Scala方法 | `persist(...)` |
| `persist` | storageLevel: StorageLevel | JavaPairDStream[K, V] | Scala方法 | `persist(...)` |
| `repartition` | numPartitions: Int | JavaPairDStream[K, V] | Scala方法 | `repartition(...)` |
| `compute` | validTime: Time | JavaPairRDD[K, V] | Scala方法 | `compute(...)` |
| `window` | windowDuration: Duration | JavaPairDStream[K, V] | Scala方法 | `window(...)` |
| `window` | windowDuration: Duration, slideDuration: Duration | JavaPairDStream[K, V] | Scala方法 | `window(...)` |
| `union` | that: JavaPairDStream[K, V] | JavaPairDStream[K, V] | Scala方法 | `union(...)` |
| `groupByKey` | 无 | JavaPairDStream[K, JIterable[V]] | Scala方法 | `groupByKey(...)` |
| `groupByKey` | numPartitions: Int | JavaPairDStream[K, JIterable[V]] | Scala方法 | `groupByKey(...)` |
| `groupByKey` | partitioner: Partitioner | JavaPairDStream[K, JIterable[V]] | Scala方法 | `groupByKey(...)` |
| `reduceByKey` | func: JFunction2[V, V, V] | JavaPairDStream[K, V] | Scala方法 | `reduceByKey(...)` |
| `reduceByKey` | func: JFunction2[V, V, V], numPartitions: Int | JavaPairDStream[K, V] | Scala方法 | `reduceByKey(...)` |
| `reduceByKey` | func: JFunction2[V, V, V], partitioner: Partitioner | JavaPairDStream[K, V] | Scala方法 | `reduceByKey(...)` |
| `groupByKeyAndWindow` | windowDuration: Duration | JavaPairDStream[K, JIterable[V]] | Scala方法 | `groupByKeyAndWindow(...)` |
| `groupByKeyAndWindow` | windowDuration: Duration, slideDuration: Duration | JavaPairDStream[K, JIterable[V]] | Scala方法 | `groupByKeyAndWindow(...)` |
| `groupByKeyAndWindow` | windowDuration: Duration, slideDuration: Duration, numPartitions: Int | JavaPairDStream[K, JIterable[V]] | Scala方法 | `groupByKeyAndWindow(...)` |
| `groupByKeyAndWindow` | windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner | JavaPairDStream[K, JIterable[V]] | Scala方法 | `groupByKeyAndWindow(...)` |
| `reduceByKeyAndWindow` | reduceFunc: JFunction2[V, V, V], windowDuration: Duration | JavaPairDStream[K, V] | Scala方法 | `reduceByKeyAndWindow(...)` |
| `reduceByKeyAndWindow` | reduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration | JavaPairDStream[K, V] | Scala方法 | `reduceByKeyAndWindow(...)` |
| `reduceByKeyAndWindow` | reduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int | JavaPairDStream[K, V] | Scala方法 | `reduceByKeyAndWindow(...)` |
| `reduceByKeyAndWindow` | reduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner | JavaPairDStream[K, V] | Scala方法 | `reduceByKeyAndWindow(...)` |
| `reduceByKeyAndWindow` | reduceFunc: JFunction2[V, V, V],
      invReduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration | JavaPairDStream[K, V] | Scala方法 | `reduceByKeyAndWindow(...)` |
| `reduceByKeyAndWindow` | reduceFunc: JFunction2[V, V, V],
      invReduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      numPartitions: Int,
      filterFunc: JFunction[(K, V | Unit | Scala方法 | `reduceByKeyAndWindow(...)` |
| `reduceByKeyAndWindow` | reduceFunc: JFunction2[V, V, V],
      invReduceFunc: JFunction2[V, V, V],
      windowDuration: Duration,
      slideDuration: Duration,
      partitioner: Partitioner,
      filterFunc: JFunction[(K, V | Unit | Scala方法 | `reduceByKeyAndWindow(...)` |
| `saveAsHadoopFiles` | prefix: String, suffix: String | Unit | Scala方法 | `saveAsHadoopFiles(...)` |
| `saveAsNewAPIHadoopFiles` | prefix: String, suffix: String | Unit | Scala方法 | `saveAsNewAPIHadoopFiles(...)` |
| `toJavaDStream` | 无 | JavaDStream[ | Scala方法 | `toJavaDStream(...)` |

---

### JavaStreamingContext

**完整类名**: `org.apache.spark.streaming.api.java.JavaStreamingContext`

**描述**: Scala定义的Java友好接口

**方法数**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `this` | master: String, appName: String, batchDuration: Duration | Unit | Scala方法 | `this(...)` |
| `this` | master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String,
      jarFile: String | Unit | Scala方法 | `this(...)` |
| `this` | master: String,
      appName: String,
      batchDuration: Duration,
      sparkHome: String,
      jars: Array[String] | Unit | Scala方法 | `this(...)` |
| `this` | master: String,
    appName: String,
    batchDuration: Duration,
    sparkHome: String,
    jars: Array[String],
    environment: JMap[String, String] | Unit | Scala方法 | `this(...)` |
| `this` | sparkContext: JavaSparkContext, batchDuration: Duration | Unit | Scala方法 | `this(...)` |
| `this` | conf: SparkConf, batchDuration: Duration | Unit | Scala方法 | `this(...)` |
| `this` | path: String | Unit | Scala方法 | `this(...)` |
| `this` | path: String, hadoopConf: Configuration | Unit | Scala方法 | `this(...)` |
| `socketTextStream` | hostname: String, port: Int,
      storageLevel: StorageLevel | JavaReceiverInputDStream[String] | Scala方法 | `socketTextStream(...)` |
| `socketTextStream` | hostname: String, port: Int | JavaReceiverInputDStream[String] | Scala方法 | `socketTextStream(...)` |
| `textFileStream` | directory: String | JavaDStream[String] | Scala方法 | `textFileStream(...)` |
| `binaryRecordsStream` | directory: String, recordLength: Int | JavaDStream[Array[Byte]] | Scala方法 | `binaryRecordsStream(...)` |
| `checkpoint` | directory: String | Unit | Scala方法 | `checkpoint(...)` |
| `remember` | duration: Duration | Unit | Scala方法 | `remember(...)` |
| `addStreamingListener` | streamingListener: StreamingListener | Unit | Scala方法 | `addStreamingListener(...)` |
| `getState` | 无 | StreamingContextState | Scala方法 | `getState(...)` |
| `start` | 无 | Unit | Scala方法 | `start(...)` |
| `awaitTermination` | 无 | Unit | Scala方法 | `awaitTermination(...)` |
| `awaitTerminationOrTimeout` | timeout: Long | Boolean | Scala方法 | `awaitTerminationOrTimeout(...)` |
| `stop` | 无 | Unit | Scala方法 | `stop(...)` |
| `stop` | stopSparkContext: Boolean | Unit | Scala方法 | `stop(...)` |
| `stop` | stopSparkContext: Boolean, stopGracefully: Boolean | Unit | Scala方法 | `stop(...)` |
| `close` | 无 | Unit | Scala方法 | `close(...)` |
| `getOrCreate` | checkpointPath: String,
      creatingFunc: JFunction0[JavaStreamingContext] | JavaStreamingContext | Scala方法 | `getOrCreate(...)` |
| `getOrCreate` | checkpointPath: String,
      creatingFunc: JFunction0[JavaStreamingContext],
      hadoopConf: Configuration | JavaStreamingContext | Scala方法 | `getOrCreate(...)` |
| `getOrCreate` | checkpointPath: String,
      creatingFunc: JFunction0[JavaStreamingContext],
      hadoopConf: Configuration,
      createOnError: Boolean | JavaStreamingContext | Scala方法 | `getOrCreate(...)` |
| `jarOfClass` | cls: Class[_] | Array[String] | Scala方法 | `jarOfClass(...)` |

---

### JavaStreamingListenerWrapper

**完整类名**: `org.apache.spark.streaming.api.java.JavaStreamingListenerWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onStreamingStarted` | streamingStarted: StreamingListenerStreamingStarted | Unit | Scala方法 | `onStreamingStarted(...)` |
| `onReceiverStarted` | receiverStarted: StreamingListenerReceiverStarted | Unit | Scala方法 | `onReceiverStarted(...)` |
| `onReceiverError` | receiverError: StreamingListenerReceiverError | Unit | Scala方法 | `onReceiverError(...)` |
| `onReceiverStopped` | receiverStopped: StreamingListenerReceiverStopped | Unit | Scala方法 | `onReceiverStopped(...)` |
| `onBatchSubmitted` | batchSubmitted: StreamingListenerBatchSubmitted | Unit | Scala方法 | `onBatchSubmitted(...)` |
| `onBatchStarted` | batchStarted: StreamingListenerBatchStarted | Unit | Scala方法 | `onBatchStarted(...)` |
| `onBatchCompleted` | batchCompleted: StreamingListenerBatchCompleted | Unit | Scala方法 | `onBatchCompleted(...)` |
| `onOutputOperationStarted` | outputOperationStarted: StreamingListenerOutputOperationStarted | Unit | Scala方法 | `onOutputOperationStarted(...)` |
| `onOutputOperationCompleted` | outputOperationCompleted: StreamingListenerOutputOperationCompleted | Unit | Scala方法 | `onOutputOperationCompleted(...)` |

---

### PythonStreamingListenerWrapper

**完整类名**: `org.apache.spark.streaming.api.java.PythonStreamingListenerWrapper`

**描述**: Scala定义的Java友好接口

**方法数**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onStreamingStarted` | streamingStarted: JavaStreamingListenerStreamingStarted | Unit | Scala方法 | `onStreamingStarted(...)` |
| `onReceiverStarted` | receiverStarted: JavaStreamingListenerReceiverStarted | Unit | Scala方法 | `onReceiverStarted(...)` |
| `onReceiverError` | receiverError: JavaStreamingListenerReceiverError | Unit | Scala方法 | `onReceiverError(...)` |
| `onReceiverStopped` | receiverStopped: JavaStreamingListenerReceiverStopped | Unit | Scala方法 | `onReceiverStopped(...)` |
| `onBatchSubmitted` | batchSubmitted: JavaStreamingListenerBatchSubmitted | Unit | Scala方法 | `onBatchSubmitted(...)` |
| `onBatchStarted` | batchStarted: JavaStreamingListenerBatchStarted | Unit | Scala方法 | `onBatchStarted(...)` |
| `onBatchCompleted` | batchCompleted: JavaStreamingListenerBatchCompleted | Unit | Scala方法 | `onBatchCompleted(...)` |
| `onOutputOperationStarted` | outputOperationStarted: JavaStreamingListenerOutputOperationStarted | Unit | Scala方法 | `onOutputOperationStarted(...)` |
| `onOutputOperationCompleted` | outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted | Unit | Scala方法 | `onOutputOperationCompleted(...)` |
| `onStreamingStarted` | streamingStarted: JavaStreamingListenerStreamingStarted | Unit | Scala方法 | `onStreamingStarted(...)` |
| `onReceiverStarted` | receiverStarted: JavaStreamingListenerReceiverStarted | Unit | Scala方法 | `onReceiverStarted(...)` |
| `onReceiverError` | receiverError: JavaStreamingListenerReceiverError | Unit | Scala方法 | `onReceiverError(...)` |
| `onReceiverStopped` | receiverStopped: JavaStreamingListenerReceiverStopped | Unit | Scala方法 | `onReceiverStopped(...)` |
| `onBatchSubmitted` | batchSubmitted: JavaStreamingListenerBatchSubmitted | Unit | Scala方法 | `onBatchSubmitted(...)` |
| `onBatchStarted` | batchStarted: JavaStreamingListenerBatchStarted | Unit | Scala方法 | `onBatchStarted(...)` |
| `onBatchCompleted` | batchCompleted: JavaStreamingListenerBatchCompleted | Unit | Scala方法 | `onBatchCompleted(...)` |
| `onOutputOperationStarted` | outputOperationStarted: JavaStreamingListenerOutputOperationStarted | Unit | Scala方法 | `onOutputOperationStarted(...)` |
| `onOutputOperationCompleted` | outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted | Unit | Scala方法 | `onOutputOperationCompleted(...)` |
| `onStreamingStarted` | streamingStarted: JavaStreamingListenerStreamingStarted | Unit | Scala方法 | `onStreamingStarted(...)` |
| `onReceiverStarted` | receiverStarted: JavaStreamingListenerReceiverStarted | Unit | Scala方法 | `onReceiverStarted(...)` |
| `onReceiverError` | receiverError: JavaStreamingListenerReceiverError | Unit | Scala方法 | `onReceiverError(...)` |
| `onReceiverStopped` | receiverStopped: JavaStreamingListenerReceiverStopped | Unit | Scala方法 | `onReceiverStopped(...)` |
| `onBatchSubmitted` | batchSubmitted: JavaStreamingListenerBatchSubmitted | Unit | Scala方法 | `onBatchSubmitted(...)` |
| `onBatchStarted` | batchStarted: JavaStreamingListenerBatchStarted | Unit | Scala方法 | `onBatchStarted(...)` |
| `onBatchCompleted` | batchCompleted: JavaStreamingListenerBatchCompleted | Unit | Scala方法 | `onBatchCompleted(...)` |
| `onOutputOperationStarted` | outputOperationStarted: JavaStreamingListenerOutputOperationStarted | Unit | Scala方法 | `onOutputOperationStarted(...)` |
| `onOutputOperationCompleted` | outputOperationCompleted: JavaStreamingListenerOutputOperationCompleted | Unit | Scala方法 | `onOutputOperationCompleted(...)` |

---

### instead

**完整类名**: `org.apache.spark.streaming.api.java.instead`

**描述**: Scala定义的Java友好接口

**方法数**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `wrapRDD` | in: RDD[T] | R | Scala方法 | `wrapRDD(...)` |
| `scalaIntToJavaLong` | in: DStream[Long] | JavaDStream[jl | Scala方法 | `scalaIntToJavaLong(...)` |
| `print` | 无 | Unit | Scala方法 | `print(...)` |
| `print` | num: Int | Unit | Scala方法 | `print(...)` |
| `count` | 无 | JavaDStream[jl | Scala方法 | `count(...)` |
| `countByValue` | 无 | JavaPairDStream[T, jl | Scala方法 | `countByValue(...)` |
| `countByValue` | numPartitions: Int | JavaPairDStream[T, jl | Scala方法 | `countByValue(...)` |
| `countByWindow` | windowDuration: Duration, slideDuration: Duration | JavaDStream[jl | Scala方法 | `countByWindow(...)` |
| `countByValueAndWindow` | windowDuration: Duration, slideDuration: Duration | JavaPairDStream[T, jl | Scala方法 | `countByValueAndWindow(...)` |
| `countByValueAndWindow` | windowDuration: Duration, slideDuration: Duration, numPartitions: Int | JavaPairDStream[T, jl | Scala方法 | `countByValueAndWindow(...)` |
| `glom` | 无 | JavaDStream[JList[T]] | Scala方法 | `glom(...)` |
| `context` | 无 | StreamingContext | Scala方法 | `context(...)` |
| `reduce` | f: JFunction2[T, T, T] | JavaDStream[T] | Scala方法 | `reduce(...)` |
| `reduceByWindow` | reduceFunc: JFunction2[T, T, T],
      windowDuration: Duration,
      slideDuration: Duration | JavaDStream[T] | Scala方法 | `reduceByWindow(...)` |
| `reduceByWindow` | reduceFunc: JFunction2[T, T, T],
      invReduceFunc: JFunction2[T, T, T],
      windowDuration: Duration,
      slideDuration: Duration | JavaDStream[T] | Scala方法 | `reduceByWindow(...)` |
| `slice` | fromTime: Time, toTime: Time | JList[R] | Scala方法 | `slice(...)` |
| `foreachRDD` | foreachFunc: JVoidFunction[R] | Unit | Scala方法 | `foreachRDD(...)` |
| `foreachRDD` | foreachFunc: JVoidFunction2[R, Time] | Unit | Scala方法 | `foreachRDD(...)` |
| `scalaTransform` | in: RDD[T] | RDD[U] | Scala方法 | `scalaTransform(...)` |
| `scalaTransform` | in: RDD[T], time: Time | RDD[U] | Scala方法 | `scalaTransform(...)` |
| `scalaTransform` | in: RDD[T] | RDD[ | Scala方法 | `scalaTransform(...)` |
| `scalaTransform` | in: RDD[T], time: Time | RDD[ | Scala方法 | `scalaTransform(...)` |
| `scalaTransform` | inThis: RDD[T], inThat: RDD[U], time: Time | RDD[W] | Scala方法 | `scalaTransform(...)` |
| `scalaTransform` | inThis: RDD[T], inThat: RDD[U], time: Time | RDD[ | Scala方法 | `scalaTransform(...)` |
| `scalaTransform` | inThis: RDD[T], inThat: RDD[(K2, V2 | Unit | Scala方法 | `scalaTransform(...)` |
| `scalaTransform` | inThis: RDD[T], inThat: RDD[(K2, V2 | Unit | Scala方法 | `scalaTransform(...)` |
| `checkpoint` | interval: Duration | DStream[T] | Scala方法 | `checkpoint(...)` |

---

## 包: org.apache.spark.unsafe.map

**类数量**: 1

### HashMapGrowthStrategy

**完整类名**: `org.apache.spark.unsafe.map.HashMapGrowthStrategy`

**描述**: Interface that defines how we can grow the size of a hash map when it is over a threshold.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `nextCapacity` | int currentCapacity | int | 暂无描述 | `nextCapacity(...)` |

---

## 包: org.apache.spark.util

**类数量**: 5

### BestEffortLazyVal

**完整类名**: `org.apache.spark.util.BestEffortLazyVal`

**描述**: A lock-free implementation of a lazily-initialized variable.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | T | 暂无描述 | `apply(...)` |

---

### ChildFirstURLClassLoader

**完整类名**: `org.apache.spark.util.ChildFirstURLClassLoader`

**描述**: A mutable class loader that gives preference to its own URLs over the parent class loader

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getResources` | String name | Enumeration<URL> | 暂无描述 | `getResources(...)` |
| `getResource` | String name | URL | 暂无描述 | `getResource(...)` |

---

### EnumUtil

**完整类名**: `org.apache.spark.util.EnumUtil`

**描述**: 暂无描述

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `parseIgnoreCase` | Class<E> clz, String str | <E extends Enum<E>> E | 暂无描述 | `parseIgnoreCase(...)` |

---

### MutableURLClassLoader

**完整类名**: `org.apache.spark.util.MutableURLClassLoader`

**描述**: URL class loader that exposes the `addURL` method in URLClassLoader.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addURL` | URL url | void | 暂无描述 | `addURL(...)` |

---

### TransientBestEffortLazyVal

**完整类名**: `org.apache.spark.util.TransientBestEffortLazyVal`

**描述**: A lock-free implementation of a lazily-initialized variable.

**方法数**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | T | 暂无描述 | `apply(...)` |

---

## 包: org.apache.spark.util.collection.unsafe.sort

**类数量**: 2

### PrefixComparators

**完整类名**: `org.apache.spark.util.collection.unsafe.sort.PrefixComparators`

**描述**: Converts the double into a value that compares correctly as an unsigned long. For more

**方法数**: 38

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `computePrefix` | UTF8String value | long | 暂无描述 | `computePrefix(...)` |
| `computePrefix` | byte[] bytes | long | 暂无描述 | `computePrefix(...)` |
| `computePrefix` | double value | long | Converts the double into a value that compares correctly as an unsigned long. For more | `computePrefix(...)` |
| `sortDescending` | 无 | abstract boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | abstract boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | abstract boolean | 暂无描述 | `nullsFirst(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long aPrefix, long bPrefix | int | 暂无描述 | `compare(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long aPrefix, long bPrefix | int | 暂无描述 | `compare(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long bPrefix, long aPrefix | int | 暂无描述 | `compare(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long bPrefix, long aPrefix | int | 暂无描述 | `compare(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long a, long b | int | 暂无描述 | `compare(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long a, long b | int | 暂无描述 | `compare(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long b, long a | int | 暂无描述 | `compare(...)` |
| `sortDescending` | 无 | boolean | 暂无描述 | `sortDescending(...)` |
| `sortSigned` | 无 | boolean | 暂无描述 | `sortSigned(...)` |
| `nullsFirst` | 无 | boolean | 暂无描述 | `nullsFirst(...)` |
| `compare` | long b, long a | int | 暂无描述 | `compare(...)` |

---

### RadixSort

**完整类名**: `org.apache.spark.util.collection.unsafe.sort.RadixSort`

**描述**: Sorts a given array of longs using least-significant-digit radix sort. This routine assumes

**方法数**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `sort` | LongArray array, long numRecords, int startByteIndex, int endByteIndex,
      boolean desc, boolean signed | int | 暂无描述 | `sort(...)` |
| `sortKeyPrefixArray` | LongArray array,
      long startIndex,
      long numRecords,
      int startByteIndex,
      int endByteIndex,
      boolean desc,
      boolean signed | int | 暂无描述 | `sortKeyPrefixArray(...)` |

---
