# Spark Java API 高质量完整文档

> **文档特点**:
> - 包含Spark所有public Java方法
> - 核心方法提供完整示例
> - 其他方法提供清晰参数说明
> - 按业务分类组织

> **统计**: 2840 个方法

---

## 目录

- Core RDD核心: 4 类, 153 方法
- MLlib机器学习: 33 类, 43 方法
- SQL DataFrame: 97 类, 858 方法
- Streaming流处理: 13 类, 69 方法
- 其他辅助类: 292 类, 1716 方法
- 存储级别: 1 类, 1 方法

---

## Core RDD核心

### JavaDoubleRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 33

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaDoubleRDD` | cache方法 |  |
| `coalesce` | Int: numPartitions: | `JavaDoubleRDD` | coalesce方法 |  |
| `coalesce` | Int: numPartitions:; Boolean: shuffle: | `JavaDoubleRDD` | coalesce方法 |  |
| `distinct` | 无 | `JavaDoubleRDD` | distinct方法 |  |
| `distinct` | Int: numPartitions: | `JavaDoubleRDD` | distinct方法 |  |
| `filter` | JFunction[JDouble: f: | `JavaDoubleRDD` | filter方法 |  |
| `histogram` | Int: bucketCount: | `Unit` | histogram方法 |  |
| `histogram` | Array[scala.Double]: buckets: | `Array` | histogram方法 |  |
| `intersection` | JavaDoubleRDD: other: | `JavaDoubleRDD` | intersection方法 |  |
| `max` | 无 | `JDouble` | max方法 |  |
| `mean` | 无 | `JDouble` | mean方法 |  |
| `meanApprox` | Long: timeout:; JDouble: confidence: | `PartialResult` | meanApprox方法 |  |
| `meanApprox` | Long: timeout: | `PartialResult` | meanApprox方法 |  |
| `min` | 无 | `JDouble` | min方法 |  |
| `persist` | StorageLevel: newLevel: | `JavaDoubleRDD` | persist方法 |  |
| `repartition` | Int: numPartitions: | `JavaDoubleRDD` | repartition方法 |  |
| `sample` | Boolean: withReplacement:; JDouble: fraction: | `JavaDoubleRDD` | sample方法 |  |
| `sample` | Boolean: withReplacement:; JDouble: fraction:; Long: seed: | `JavaDoubleRDD` | sample方法 |  |
| `sampleStdev` | 无 | `JDouble` | sampleStdev方法 |  |
| `sampleVariance` | 无 | `JDouble` | sampleVariance方法 |  |
| `setName` | String: name: | `JavaDoubleRDD` | setName方法 |  |
| `stats` | 无 | `StatCounter` | stats方法 |  |
| `stdev` | 无 | `JDouble` | stdev方法 |  |
| `subtract` | JavaDoubleRDD: other: | `JavaDoubleRDD` | subtract方法 |  |
| `subtract` | JavaDoubleRDD: other:; Int: numPartitions: | `JavaDoubleRDD` | subtract方法 |  |
| `subtract` | JavaDoubleRDD: other:; Partitioner: p: | `JavaDoubleRDD` | subtract方法 |  |
| `sum` | 无 | `JDouble` | sum方法 |  |
| `sumApprox` | Long: timeout:; JDouble: confidence: | `PartialResult` | sumApprox方法 |  |
| `sumApprox` | Long: timeout: | `PartialResult` | sumApprox方法 |  |
| `union` | JavaDoubleRDD: other: | `JavaDoubleRDD` | union方法 |  |
| `unpersist` | 无 | `JavaDoubleRDD` | unpersist方法 |  |
| `unpersist` | Boolean: blocking: | `JavaDoubleRDD` | unpersist方法 |  |
| `variance` | 无 | `JDouble` | variance方法 |  |

### JavaPairRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 53

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaPairRDD` | cache方法 |  |
| `coalesce` | Int: numPartitions: | `JavaPairRDD` | coalesce方法 |  |
| `coalesce` | Int: numPartitions:; Boolean: shuffle: | `JavaPairRDD` | coalesce方法 |  |
| `collectAsMap` | 无 | `java` | collectAsMap方法 |  |
| `countApproxDistinctByKey` | Double: relativeSD:; Partitioner: partitioner: | `JavaPairRDD` | countApproxDistinctByKey方法 |  |
| `countApproxDistinctByKey` | Double: relativeSD:; Int: numPartitions: | `JavaPairRDD` | countApproxDistinctByKey方法 |  |
| `countApproxDistinctByKey` | Double: relativeSD: | `JavaPairRDD` | countApproxDistinctByKey方法 |  |
| `countByKey` | 无 | `java` | countByKey方法 |  |
| `countByKeyApprox` | Long: timeout: | `PartialResult` | countByKeyApprox方法 |  |
| `countByKeyApprox` | Long: timeout:; 0.95: confidence: Double = | `PartialResult` | countByKeyApprox方法 |  |
| `distinct` | 无 | `JavaPairRDD` | distinct方法 |  |
| `distinct` | Int: numPartitions: | `JavaPairRDD` | distinct方法 |  |
| `filter` | JFunction[(K: f: | `Unit` | filter方法 |  |
| `foldByKey` | V: zeroValue:; Partitioner: partitioner:; JFunction2[V: func: | `JavaPairRDD` | foldByKey方法 |  |
| `foldByKey` | V: zeroValue:; Int: numPartitions:; JFunction2[V: func: | `JavaPairRDD` | foldByKey方法 |  |
| `foldByKey` | V: zeroValue:; JFunction2[V: func: | `JavaPairRDD` | foldByKey方法 |  |
| `groupByKey` | Partitioner: partitioner: | `JavaPairRDD` | groupByKey方法 |  |
| `groupByKey` | Int: numPartitions: | `JavaPairRDD` | groupByKey方法 |  |
| `groupByKey` | 无 | `JavaPairRDD` | groupByKey方法 |  |
| `intersection` | JavaPairRDD[K: other: | `JavaPairRDD` | intersection方法 |  |
| `keys` | 无 | `JavaRDD` | keys方法 |  |
| `lookup` | K: key: | `JList` | lookup方法 |  |
| `partitionBy` | Partitioner: partitioner: | `JavaPairRDD` | partitionBy方法 |  |
| `persist` | StorageLevel: newLevel: | `JavaPairRDD` | persist方法 |  |
| `reduceByKey` | Partitioner: partitioner:; JFunction2[V: func: | `JavaPairRDD` | reduceByKey方法 | JavaPairRDD<String, Integer> reduced = pairRDD.reduceByKey((a, b) -> a + b); |
| `reduceByKey` | JFunction2[V: func:; Int: numPartitions: | `JavaPairRDD` | reduceByKey方法 | JavaPairRDD<String, Integer> reduced = pairRDD.reduceByKey((a, b) -> a + b); |
| `reduceByKey` | JFunction2[V: func: | `JavaPairRDD` | reduceByKey方法 | JavaPairRDD<String, Integer> reduced = pairRDD.reduceByKey((a, b) -> a + b); |
| `reduceByKeyLocally` | JFunction2[V: func: | `java` | reduceByKeyLocally方法 |  |
| `repartition` | Int: numPartitions: | `JavaPairRDD` | repartition方法 |  |
| `repartitionAndSortWithinPartitions` | Partitioner: partitioner: | `JavaPairRDD` | repartitionAndSortWithinPartitions方法 |  |
| `repartitionAndSortWithinPartitions` | Partitioner: partitioner:; Comparator[K]: comp: | `JavaPairRDD` | repartitionAndSortWithinPartitions方法 |  |
| `sample` | Boolean: withReplacement:; Double: fraction: | `JavaPairRDD` | sample方法 |  |
| `sample` | Boolean: withReplacement:; Double: fraction:; Long: seed: | `JavaPairRDD` | sample方法 |  |
| `sampleByKey` | Boolean: withReplacement:; java.util.Map[K: fractions:; Long: seed: | `JavaPairRDD` | sampleByKey方法 |  |
| `sampleByKey` | Boolean: withReplacement:; java.util.Map[K: fractions: | `JavaPairRDD` | sampleByKey方法 |  |
| `sampleByKeyExact` | Boolean: withReplacement:; java.util.Map[K: fractions:; Long: seed: | `JavaPairRDD` | sampleByKeyExact方法 |  |
| `sampleByKeyExact` | Boolean: withReplacement:; java.util.Map[K: fractions: | `JavaPairRDD` | sampleByKeyExact方法 |  |
| `saveAsHadoopDataset` | JobConf: conf: | `Unit` | saveAsHadoopDataset方法 |  |
| `saveAsNewAPIHadoopDataset` | Configuration: conf: | `Unit` | saveAsNewAPIHadoopDataset方法 |  |
| `setName` | String: name: | `JavaPairRDD` | setName方法 |  |
| `sortByKey` | 无 | `JavaPairRDD` | sortByKey方法 |  |
| `sortByKey` | Boolean: ascending: | `JavaPairRDD` | sortByKey方法 |  |
| `sortByKey` | Boolean: ascending:; Int: numPartitions: | `JavaPairRDD` | sortByKey方法 |  |
| `sortByKey` | Comparator[K]: comp: | `JavaPairRDD` | sortByKey方法 |  |
| `sortByKey` | Comparator[K]: comp:; Boolean: ascending: | `JavaPairRDD` | sortByKey方法 |  |
| `sortByKey` | Comparator[K]: comp:; Boolean: ascending:; Int: numPartitions: | `JavaPairRDD` | sortByKey方法 |  |
| `subtract` | JavaPairRDD[K: other: | `JavaPairRDD` | subtract方法 |  |
| `subtract` | JavaPairRDD[K: other:; Int: numPartitions: | `JavaPairRDD` | subtract方法 |  |
| `subtract` | JavaPairRDD[K: other:; Partitioner: p: | `JavaPairRDD` | subtract方法 |  |
| `union` | JavaPairRDD[K: other: | `JavaPairRDD` | union方法 |  |
| `unpersist` | 无 | `JavaPairRDD` | unpersist方法 |  |
| `unpersist` | Boolean: blocking: | `JavaPairRDD` | unpersist方法 |  |
| `values` | 无 | `JavaRDD` | values方法 |  |

### JavaRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaRDD` | cache方法 |  |
| `coalesce` | Int: numPartitions: | `JavaRDD` | coalesce方法 |  |
| `coalesce` | Int: numPartitions:; Boolean: shuffle: | `JavaRDD` | coalesce方法 |  |
| `distinct` | 无 | `JavaRDD` | distinct方法 |  |
| `distinct` | Int: numPartitions: | `JavaRDD` | distinct方法 |  |
| `filter` | JFunction[T: f: | `JavaRDD` | filter方法 | JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>JavaRDD<Integer> filtered = rdd.filter(x -> x > 2); |
| `getResourceProfile` | 无 | `ResourceProfile` | getResourceProfile方法 |  |
| `intersection` | JavaRDD[T]: other: | `JavaRDD` | intersection方法 |  |
| `persist` | StorageLevel: newLevel: | `JavaRDD` | persist方法 |  |
| `randomSplit` | Array[Double]: weights: | `Array` | randomSplit方法 |  |
| `randomSplit` | Array[Double]: weights:; Long: seed: | `Array` | randomSplit方法 |  |
| `repartition` | Int: numPartitions: | `JavaRDD` | repartition方法 |  |
| `sample` | Boolean: withReplacement:; Double: fraction: | `JavaRDD` | sample方法 |  |
| `sample` | Boolean: withReplacement:; Double: fraction:; Long: seed: | `JavaRDD` | sample方法 |  |
| `setName` | String: name: | `JavaRDD` | setName方法 |  |
| `subtract` | JavaRDD[T]: other: | `JavaRDD` | subtract方法 |  |
| `subtract` | JavaRDD[T]: other:; Int: numPartitions: | `JavaRDD` | subtract方法 |  |
| `subtract` | JavaRDD[T]: other:; Partitioner: p: | `JavaRDD` | subtract方法 |  |
| `union` | JavaRDD[T]: other: | `JavaRDD` | union方法 |  |
| `unpersist` | 无 | `JavaRDD` | unpersist方法 |  |
| `unpersist` | Boolean: blocking: | `JavaRDD` | unpersist方法 |  |
| `withResources` | ResourceProfile: rp: | `JavaRDD` | withResources方法 |  |

### JavaSparkContext
**包路径**: `org.apache.spark.api.java`
**方法数量**: 45

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addFile` | String: path: | `Unit` | addFile方法 |  |
| `addFile` | String: path:; Boolean: recursive: | `Unit` | addFile方法 |  |
| `addJar` | String: path: | `Unit` | addJar方法 |  |
| `addJobTag` | String: tag: | `Unit` | addJobTag方法 |  |
| `binaryFiles` | String: path:; Int: minPartitions: | `JavaPairRDD` | binaryFiles方法 |  |
| `binaryFiles` | String: path: | `JavaPairRDD` | binaryFiles方法 |  |
| `binaryRecords` | String: path:; Int: recordLength: | `JavaRDD` | binaryRecords方法 |  |
| `cancelAllJobs` | 无 | `Unit` | cancelAllJobs方法 |  |
| `cancelJobGroup` | String: groupId:; String: reason: | `Unit` | cancelJobGroup方法 |  |
| `cancelJobGroup` | String: groupId: | `Unit` | cancelJobGroup方法 |  |
| `cancelJobsWithTag` | String: tag:; String: reason: | `Unit` | cancelJobsWithTag方法 |  |
| `cancelJobsWithTag` | String: tag: | `Unit` | cancelJobsWithTag方法 |  |
| `clearCallSite` | 无 | `Unit` | clearCallSite方法 |  |
| `clearJobGroup` | 无 | `Unit` | clearJobGroup方法 |  |
| `clearJobTags` | 无 | `Unit` | clearJobTags方法 |  |
| `getJobTags` | 无 | `util` | getJobTags方法 |  |
| `getLocalProperty` | String: key: | `String` | getLocalProperty方法 |  |
| `getSparkHome` | 无 | `Optional` | getSparkHome方法 |  |
| `hadoopConfiguration` | 无 | `Configuration` | hadoopConfiguration方法 |  |
| `jarOfClass` | Class[_]: cls: | `Array` | jarOfClass方法 |  |
| `jarOfObject` | AnyRef: obj: | `Array` | jarOfObject方法 |  |
| `parallelizeDoubles` | java.util.List[java.lang.Double]: list:; Int: numSlices: | `JavaDoubleRDD` | parallelizeDoubles方法 |  |
| `parallelizeDoubles` | java.util.List[java.lang.Double]: list: | `JavaDoubleRDD` | parallelizeDoubles方法 |  |
| `removeJobTag` | String: tag: | `Unit` | removeJobTag方法 |  |
| `setCallSite` | String: site: | `Unit` | setCallSite方法 |  |
| `setCheckpointDir` | String: dir: | `Unit` | setCheckpointDir方法 |  |
| `setInterruptOnCancel` | Boolean: interruptOnCancel: | `Unit` | setInterruptOnCancel方法 |  |
| `setJobDescription` | String: value: | `Unit` | setJobDescription方法 |  |
| `setJobGroup` | String: groupId:; String: description:; Boolean: interruptOnCancel: | `Unit` | setJobGroup方法 |  |
| `setJobGroup` | String: groupId:; String: description: | `Unit` | setJobGroup方法 |  |
| `setLocalProperty` | String: key:; String: value: | `Unit` | setLocalProperty方法 |  |
| `setLogLevel` | String: logLevel: | `Unit` | setLogLevel方法 |  |
| `stop` | 无 | `Unit` | stop方法 |  |
| `stop` | Int: exitCode: | `Unit` | stop方法 |  |
| `textFile` | String: path: | `JavaRDD` | textFile方法 | JavaRDD<String> lines = sc.textFile("hdfs://path/file.txt"); |
| `textFile` | String: path:; Int: minPartitions: | `JavaRDD` | textFile方法 | JavaRDD<String> lines = sc.textFile("hdfs://path/file.txt"); |
| `this` | 无 | `Unit` | this方法 |  |
| `this` | SparkConf: conf: | `Unit` | this方法 |  |
| `this` | String: master:; String: appName: | `Unit` | this方法 |  |
| `this` | String: master:; String: appName:; SparkConf: conf: | `Unit` | this方法 |  |
| `this` | String: master:; String: appName:; String: sparkHome:; String: jarFile: | `Unit` | this方法 |  |
| `this` | String: master:; String: appName:; String: sparkHome:; Array[String]: jars: | `Unit` | this方法 |  |
| `this` | String: master:; String: appName:; String: sparkHome:; Array[String]: jars:; JMap[String: environment: | `Unit` | this方法 |  |
| `wholeTextFiles` | String: path:; Int: minPartitions: | `JavaPairRDD` | wholeTextFiles方法 |  |
| `wholeTextFiles` | String: path: | `JavaPairRDD` | wholeTextFiles方法 |  |

---

## MLlib机器学习

### JavaAssociationRulesExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaBinaryClassificationMetricsExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaBisectingKMeansExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaChiSqSelectorExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaCorrelationsExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaElementwiseProductExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaGaussianMixtureExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaGradientBoostingClassificationExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaGradientBoostingRegressionExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaHypothesisTestingExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaHypothesisTestingKolmogorovSmirnovTestExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaIsotonicRegressionExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaKMeansExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaKernelDensityEstimationExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLBFGSExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLatentDirichletAllocationExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLogisticRegressionWithLBFGSExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaMultiLabelClassificationMetricsExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaMulticlassClassificationMetricsExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaNaiveBayesExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaPCAExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaPowerIterationClusteringExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaPrefixSpanExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### JavaRandomForestClassificationExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaRandomForestRegressionExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaRankingMetricsExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaRecommendationExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaSVDExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaSVMWithSGDExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaSimpleFPGrowth
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaStratifiedSamplingExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaStreamingTestExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaSummaryStatisticsExample
**包路径**: `org.apache.spark.examples.mllib`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

---

## SQL DataFrame

### AggregateHashMap
**包路径**: `org.apache.spark.sql.execution.vectorized`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `find` | key: long | `int` | find方法 |  |
| `findOrInsert` | key: long | `MutableColumnarRow` | findOrInsert方法 |  |

### ArrayExpressionUtils
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `binarySearch` | data: boolean[]; value: boolean | `int` | binarySearch方法 |  |
| `binarySearch` | data: Boolean[]; value: Boolean | `int` | binarySearch方法 |  |
| `binarySearch` | data: byte[]; value: byte | `int` | binarySearch方法 |  |
| `binarySearch` | data: Byte[]; value: Byte | `int` | binarySearch方法 |  |
| `binarySearch` | data: short[]; value: short | `int` | binarySearch方法 |  |
| `binarySearch` | data: Short[]; value: Short | `int` | binarySearch方法 |  |
| `binarySearch` | data: int[]; value: int | `int` | binarySearch方法 |  |
| `binarySearch` | data: Integer[]; value: Integer | `int` | binarySearch方法 |  |
| `binarySearch` | data: long[]; value: long | `int` | binarySearch方法 |  |
| `binarySearch` | data: Long[]; value: Long | `int` | binarySearch方法 |  |
| `binarySearch` | data: float[]; value: float | `int` | binarySearch方法 |  |
| `binarySearch` | data: Float[]; value: Float | `int` | binarySearch方法 |  |
| `binarySearch` | data: double[]; value: double | `int` | binarySearch方法 |  |
| `binarySearch` | data: Double[]; value: Double | `int` | binarySearch方法 |  |
| `binarySearch` | data: Object[]; value: Object; comp: Comparator<Object> | `int` | binarySearch方法 |  |

### ArrayOfDecimalsSerDe
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getClassOfT` | 无 | `Class&lt;Decimal&gt;` | getClassOfT方法 |  |
| `getClassOfT` | 无 | `Class&lt;Decimal&gt;` | getClassOfT方法 |  |
| `sizeOf` | item: Decimal | `int` | sizeOf方法 |  |
| `sizeOf` | mem: Memory; offsetBytes: long; numItems: int | `int` | sizeOf方法 |  |
| `sizeOf` | item: Decimal | `int` | sizeOf方法 |  |
| `sizeOf` | mem: Memory; offsetBytes: long; numItems: int | `int` | sizeOf方法 |  |

### ArrowColumnVector
**包路径**: `org.apache.spark.sql.vectorized`
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `getArray` | rowId: int | `ColumnarArray` | getArray方法 |  |
| `getBoolean` | rowId: int | `boolean` | getBoolean方法 |  |
| `getByte` | rowId: int | `byte` | getByte方法 |  |
| `getChild` | ordinal: int | `ArrowColumnVector` | getChild方法 |  |
| `getDecimal` | rowId: int; precision: int; scale: int | `Decimal` | getDecimal方法 |  |
| `getDouble` | rowId: int | `double` | getDouble方法 |  |
| `getFloat` | rowId: int | `float` | getFloat方法 |  |
| `getGeography` | rowId: int | `GeographyVal` | getGeography方法 |  |
| `getGeometry` | rowId: int | `GeometryVal` | getGeometry方法 |  |
| `getInt` | rowId: int | `int` | getInt方法 |  |
| `getInterval` | rowId: int | `CalendarInterval` | getInterval方法 |  |
| `getLong` | rowId: int | `long` | getLong方法 |  |
| `getMap` | rowId: int | `ColumnarMap` | getMap方法 |  |
| `getShort` | rowId: int | `short` | getShort方法 |  |
| `getUTF8String` | rowId: int | `UTF8String` | getUTF8String方法 |  |
| `getValueVector` | 无 | `ValueVector` | getValueVector方法 |  |
| `hasNull` | 无 | `boolean` | hasNull方法 |  |
| `isNullAt` | rowId: int | `boolean` | isNullAt方法 |  |
| `numNulls` | 无 | `int` | numNulls方法 |  |

### AvroCompressionCodec
**包路径**: `org.apache.spark.sql.avro`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | s: String | `AvroCompressionCodec` | fromString方法 |  |
| `getCodecName` | 无 | `String` | getCodecName方法 |  |
| `getSupportCompressionLevel` | 无 | `boolean` | getSupportCompressionLevel方法 |  |
| `lowerCaseName` | 无 | `String` | lowerCaseName方法 |  |

### BitmapExpressionUtils
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `bitmapAndMerge` | bitmap1: byte[]; bitmap2: byte[] | `void` | bitmapAndMerge方法 |  |
| `bitmapBitPosition` | value: long | `long` | bitmapBitPosition方法 |  |
| `bitmapBucketNumber` | value: long | `long` | bitmapBucketNumber方法 |  |
| `bitmapCount` | bitmap: byte[] | `long` | bitmapCount方法 |  |
| `bitmapMerge` | bitmap1: byte[]; bitmap2: byte[] | `void` | bitmapMerge方法 |  |

### BufferedRowIterator
**包路径**: `org.apache.spark.sql.execution`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `append` | row: InternalRow | `void` | append方法 |  |
| `durationMs` | 无 | `long` | durationMs方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `incPeakExecutionMemory` | size: long | `void` | incPeakExecutionMemory方法 |  |
| `next` | 无 | `InternalRow` | next方法 |  |
| `shouldStop` | 无 | `boolean` | shouldStop方法 |  |

### CartesianSpatialReferenceSystemMapper
**包路径**: `org.apache.spark.sql.internal.types`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSrid` | stringId: String | `Integer` | getSrid方法 |  |
| `getStringId` | srid: int | `String` | getStringId方法 |  |

### CaseInsensitiveStringMap
**包路径**: `org.apache.spark.sql.util`
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `asCaseSensitiveMap` | 无 | `Map&lt;String, String&gt;` | asCaseSensitiveMap方法 |  |
| `clear` | 无 | `void` | clear方法 |  |
| `containsKey` | key: Object | `boolean` | containsKey方法 |  |
| `containsValue` | value: Object | `boolean` | containsValue方法 |  |
| `empty` | 无 | `CaseInsensitiveStringMap` | empty方法 |  |
| `get` | key: Object | `String` | get方法 |  |
| `getBoolean` | key: String; defaultValue: boolean | `boolean` | getBoolean方法 |  |
| `getDouble` | key: String; defaultValue: double | `double` | getDouble方法 |  |
| `getInt` | key: String; defaultValue: int | `int` | getInt方法 |  |
| `getLong` | key: String; defaultValue: long | `long` | getLong方法 |  |
| `isEmpty` | 无 | `boolean` | isEmpty方法 |  |
| `keySet` | 无 | `Set&lt;String&gt;` | keySet方法 |  |
| `put` | key: String; value: String | `String` | put方法 |  |
| `putAll` | String: Map<? extends; m: ? extends String> | `void` | putAll方法 |  |
| `remove` | key: Object | `String` | remove方法 |  |
| `size` | 无 | `int` | size方法 |  |
| `values` | 无 | `Collection&lt;String&gt;` | values方法 |  |

### Cast
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `dataType` | 无 | `DataType` | dataType方法 |  |
| `expression` | 无 | `Expression` | expression方法 |  |
| `expressionDataType` | 无 | `DataType` | expressionDataType方法 |  |

### ChangelogInfo
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `computeUpdates` | 无 | `boolean` | computeUpdates方法 |  |
| `deduplicationMode` | 无 | `DeduplicationMode` | deduplicationMode方法 |  |
| `range` | 无 | `ChangelogRange` | range方法 |  |

### CharVarcharCodegenUtils
**包路径**: `org.apache.spark.sql.catalyst.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `charTypeWriteSideCheck` | inputStr: UTF8String; limit: int | `UTF8String` | charTypeWriteSideCheck方法 |  |
| `readSidePadding` | inputStr: UTF8String; limit: int | `UTF8String` | readSidePadding方法 |  |
| `varcharTypeWriteSideCheck` | inputStr: UTF8String; limit: int | `UTF8String` | varcharTypeWriteSideCheck方法 |  |

### Check
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `Check` | build方法 |  |
| `predicate` | 无 | `Predicate` | predicate方法 |  |
| `predicate` | predicate: Predicate | `Builder` | predicate方法 |  |
| `predicateSql` | 无 | `String` | predicateSql方法 |  |
| `predicateSql` | predicateSql: String | `Builder` | predicateSql方法 |  |

### CollationAwareUTF8String
**包路径**: `org.apache.spark.sql.catalyst.util`
**方法数量**: 29

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `binaryTrim` | srcString: final UTF8String; trimString: final UTF8String; collationId: final int | `UTF8String` | binaryTrim方法 |  |
| `binaryTrimRight` | srcString: final UTF8String; trimString: final UTF8String; collationId: final int | `UTF8String` | binaryTrimRight方法 |  |
| `compareLowerCase` | left: final UTF8String; right: final UTF8String | `int` | compareLowerCase方法 |  |
| `findInSet` | match: final UTF8String; set: final UTF8String; collationId: int | `int` | findInSet方法 |  |
| `indexOf` | target: final UTF8String; pattern: final UTF8String; start: final int; collationId: final int | `int` | indexOf方法 |  |
| `lowerCaseCodePoints` | target: final UTF8String | `UTF8String` | lowerCaseCodePoints方法 |  |
| `lowercaseContains` | target: final UTF8String; pattern: final UTF8String | `boolean` | lowercaseContains方法 |  |
| `lowercaseEndsWith` | target: final UTF8String; pattern: final UTF8String | `boolean` | lowercaseEndsWith方法 |  |
| `lowercaseIndexOf` | target: final UTF8String; pattern: final UTF8String; start: final int | `int` | lowercaseIndexOf方法 |  |
| `lowercaseReplace` | target: final UTF8String; search: final UTF8String; replace: final UTF8String | `UTF8String` | lowercaseReplace方法 |  |
| `lowercaseStartsWith` | target: final UTF8String; pattern: final UTF8String | `boolean` | lowercaseStartsWith方法 |  |
| `lowercaseSubStringIndex` | string: final UTF8String; delimiter: final UTF8String; count: int | `UTF8String` | lowercaseSubStringIndex方法 |  |
| `lowercaseTranslate` | input: final UTF8String; Map<String: final; dict: String> | `UTF8String` | lowercaseTranslate方法 |  |
| `lowercaseTrim` | srcString: final UTF8String; trimString: final UTF8String; collationId: final int | `UTF8String` | lowercaseTrim方法 |  |
| `lowercaseTrimLeft` | srcString: final UTF8String; trimString: final UTF8String | `UTF8String` | lowercaseTrimLeft方法 |  |
| `lowercaseTrimRight` | srcString: final UTF8String; trimString: final UTF8String; collationId: final int | `UTF8String` | lowercaseTrimRight方法 |  |
| `replace` | target: final UTF8String; search: final UTF8String; replace: final UTF8String; collationId: final int | `UTF8String` | replace方法 |  |
| `subStringIndex` | string: final UTF8String; delimiter: final UTF8String; count: int; collationId: final int | `UTF8String` | subStringIndex方法 |  |
| `toLowerCase` | target: final UTF8String | `UTF8String` | toLowerCase方法 |  |
| `toLowerCase` | target: final UTF8String; collationId: final int | `UTF8String` | toLowerCase方法 |  |
| `toTitleCase` | target: final UTF8String | `UTF8String` | toTitleCase方法 |  |
| `toTitleCase` | target: final UTF8String; collationId: final int | `UTF8String` | toTitleCase方法 |  |
| `toTitleCaseICU` | source: UTF8String | `UTF8String` | toTitleCaseICU方法 |  |
| `toUpperCase` | target: final UTF8String | `UTF8String` | toUpperCase方法 |  |
| `toUpperCase` | target: final UTF8String; collationId: final int | `UTF8String` | toUpperCase方法 |  |
| `translate` | input: final UTF8String; Map<String: final; dict: String>; collationId: final int | `UTF8String` | translate方法 |  |
| `trim` | srcString: final UTF8String; trimString: final UTF8String; collationId: final int | `UTF8String` | trim方法 |  |
| `trimLeft` | srcString: final UTF8String; trimString: final UTF8String; collationId: final int | `UTF8String` | trimLeft方法 |  |
| `trimRight` | srcString: final UTF8String; trimString: final UTF8String; collationId: final int | `UTF8String` | trimRight方法 |  |

### ColumnDefaultValue
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getValue` | 无 | `Literal&lt;?&gt;` | getValue方法 |  |

### ColumnVector
**包路径**: `org.apache.spark.sql.vectorized`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | t: DataType | `DataType` | apply方法 |  |
| `closeIfFreeable` | 无 | `void` | closeIfFreeable方法 |  |
| `dataType` | 无 | `DataType` | dataType方法 |  |
| `getGeography` | rowId: int | `GeographyVal` | getGeography方法 |  |
| `getGeometry` | rowId: int | `GeometryVal` | getGeometry方法 |  |
| `getInterval` | rowId: int | `CalendarInterval` | getInterval方法 |  |
| `getStruct` | rowId: int | `ColumnarRow` | getStruct方法 |  |
| `getVariant` | rowId: int | `VariantVal` | getVariant方法 |  |
| `isDefinedAt` | x: DataType | `boolean` | isDefinedAt方法 |  |

### ColumnVectorUtils
**包路径**: `org.apache.spark.sql.execution.vectorized`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `populate` | col: ConstantColumnVector; row: InternalRow; fieldIdx: int | `void` | populate方法 |  |
| `toBatch` | schema: StructType; memMode: MemoryMode; row: Iterator<Row> | `ColumnarBatch` | toBatch方法 |  |
| `toJavaIntMap` | map: ColumnarMap | `Map&lt;Integer, Integer&gt;` | toJavaIntMap方法 |  |

### ColumnarBatch
**包路径**: `org.apache.spark.sql.vectorized`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `closeIfFreeable` | 无 | `void` | closeIfFreeable方法 |  |
| `column` | ordinal: int | `ColumnVector` | column方法 |  |
| `getRow` | rowId: int | `InternalRow` | getRow方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `next` | 无 | `InternalRow` | next方法 |  |
| `numCols` | 无 | `int` | numCols方法 |  |
| `numRows` | 无 | `int` | numRows方法 |  |
| `rowIterator` | 无 | `Iterator&lt;InternalRow&gt;` | rowIterator方法 |  |
| `setNumRows` | numRows: int | `void` | setNumRows方法 |  |

### ConstantColumnVector
**包路径**: `org.apache.spark.sql.execution.vectorized`
**方法数量**: 34

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `closeIfFreeable` | 无 | `void` | closeIfFreeable方法 |  |
| `getArray` | rowId: int | `ColumnarArray` | getArray方法 |  |
| `getBoolean` | rowId: int | `boolean` | getBoolean方法 |  |
| `getByte` | rowId: int | `byte` | getByte方法 |  |
| `getChild` | ordinal: int | `ColumnVector` | getChild方法 |  |
| `getDecimal` | rowId: int; precision: int; scale: int | `Decimal` | getDecimal方法 |  |
| `getDouble` | rowId: int | `double` | getDouble方法 |  |
| `getFloat` | rowId: int | `float` | getFloat方法 |  |
| `getInt` | rowId: int | `int` | getInt方法 |  |
| `getLong` | rowId: int | `long` | getLong方法 |  |
| `getMap` | ordinal: int | `ColumnarMap` | getMap方法 |  |
| `getShort` | rowId: int | `short` | getShort方法 |  |
| `getUTF8String` | rowId: int | `UTF8String` | getUTF8String方法 |  |
| `hasNull` | 无 | `boolean` | hasNull方法 |  |
| `isNullAt` | rowId: int | `boolean` | isNullAt方法 |  |
| `numNulls` | 无 | `int` | numNulls方法 |  |
| `setArray` | value: ColumnarArray | `void` | setArray方法 |  |
| `setBinary` | value: byte[] | `void` | setBinary方法 |  |
| `setBoolean` | value: boolean | `void` | setBoolean方法 |  |
| `setByte` | value: byte | `void` | setByte方法 |  |
| `setCalendarInterval` | value: CalendarInterval | `void` | setCalendarInterval方法 |  |
| `setChild` | ordinal: int; value: ConstantColumnVector | `void` | setChild方法 |  |
| `setDecimal` | value: Decimal; precision: int | `void` | setDecimal方法 |  |
| `setDouble` | value: double | `void` | setDouble方法 |  |
| `setFloat` | value: float | `void` | setFloat方法 |  |
| `setInt` | value: int | `void` | setInt方法 |  |
| `setLong` | value: long | `void` | setLong方法 |  |
| `setMap` | value: ColumnarMap | `void` | setMap方法 |  |
| `setNotNull` | 无 | `void` | setNotNull方法 |  |
| `setNull` | 无 | `void` | setNull方法 |  |
| `setShort` | value: short | `void` | setShort方法 |  |
| `setUtf8String` | value: UTF8String | `void` | setUtf8String方法 |  |
| `setVariant` | value: VariantVal | `void` | setVariant方法 |  |

### CustomAvgMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `aggregateTaskMetrics` | taskMetrics: long[] | `String` | aggregateTaskMetrics方法 |  |

### CustomSumMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `aggregateTaskMetrics` | taskMetrics: long[] | `String` | aggregateTaskMetrics方法 |  |

### CustomTaskMetric
**包路径**: `org.apache.spark.sql.connector.metric`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | name方法 |  |
| `value` | 无 | `long` | value方法 |  |

### DataTypes
**包路径**: `org.apache.spark.sql.types`
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createArrayType` | elementType: DataType | `ArrayType` | createArrayType方法 |  |
| `createArrayType` | elementType: DataType; containsNull: boolean | `ArrayType` | createArrayType方法 |  |
| `createCharType` | length: int | `CharType` | createCharType方法 |  |
| `createDayTimeIntervalType` | startField: byte; endField: byte | `DayTimeIntervalType` | createDayTimeIntervalType方法 |  |
| `createDayTimeIntervalType` | 无 | `DayTimeIntervalType` | createDayTimeIntervalType方法 |  |
| `createDecimalType` | precision: int; scale: int | `DecimalType` | createDecimalType方法 |  |
| `createDecimalType` | 无 | `DecimalType` | createDecimalType方法 |  |
| `createGeographyType` | srid: int | `GeographyType` | createGeographyType方法 |  |
| `createGeographyType` | crs: String | `GeographyType` | createGeographyType方法 |  |
| `createGeometryType` | srid: int | `GeometryType` | createGeometryType方法 |  |
| `createGeometryType` | crs: String | `GeometryType` | createGeometryType方法 |  |
| `createMapType` | keyType: DataType; valueType: DataType | `MapType` | createMapType方法 |  |
| `createMapType` | keyType: DataType; valueType: DataType; valueContainsNull: boolean | `MapType` | createMapType方法 |  |
| `createStructField` | name: String; dataType: DataType; nullable: boolean; metadata: Metadata | `StructField` | createStructField方法 |  |
| `createStructField` | name: String; dataType: DataType; nullable: boolean | `StructField` | createStructField方法 |  |
| `createStructType` | fields: List<StructField> | `StructType` | createStructType方法 |  |
| `createStructType` | fields: StructField[] | `StructType` | createStructType方法 |  |
| `createVarcharType` | length: int | `VarcharType` | createVarcharType方法 |  |
| `createYearMonthIntervalType` | startField: byte; endField: byte | `YearMonthIntervalType` | createYearMonthIntervalType方法 |  |
| `createYearMonthIntervalType` | 无 | `YearMonthIntervalType` | createYearMonthIntervalType方法 |  |

### DefaultValue
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getExpression` | 无 | `Expression` | getExpression方法 |  |
| `getSql` | 无 | `String` | getSql方法 |  |

### DelegatingCatalogExtension
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `alterNamespace` | namespace: String[]; changes: NamespaceChange... | `void` | alterNamespace方法 |  |
| `alterTable` | ident: Identifier; changes: TableChange... | `Table` | alterTable方法 |  |
| `capabilities` | 无 | `Set&lt;TableCatalogCapability&gt;` | capabilities方法 |  |
| `createNamespace` | namespace: String[]; metadata: String> | `void` | createNamespace方法 |  |
| `createTable` | ident: Identifier; schema: StructType; partitions: Transform[]; properties: String> | `Table` | createTable方法 |  |
| `createTable` | ident: Identifier; columns: Column[]; partitions: Transform[]; properties: String> | `Table` | createTable方法 |  |
| `dropNamespace` | namespace: String[]; cascade: boolean | `boolean` | dropNamespace方法 |  |
| `dropTable` | ident: Identifier | `boolean` | dropTable方法 |  |
| `functionExists` | ident: Identifier | `boolean` | functionExists方法 |  |
| `initialize` | name: String; options: CaseInsensitiveStringMap | `void` | initialize方法 |  |
| `invalidateTable` | ident: Identifier | `void` | invalidateTable方法 |  |
| `loadFunction` | ident: Identifier | `UnboundFunction` | loadFunction方法 |  |
| `loadNamespaceMetadata` | namespace: String[] | `Map&lt;String, String&gt;` | loadNamespaceMetadata方法 |  |
| `loadTable` | ident: Identifier | `Table` | loadTable方法 |  |
| `loadTable` | ident: Identifier; timestamp: long | `Table` | loadTable方法 |  |
| `loadTable` | ident: Identifier; version: String | `Table` | loadTable方法 |  |
| `name` | 无 | `String` | name方法 |  |
| `namespaceExists` | namespace: String[] | `boolean` | namespaceExists方法 |  |
| `purgeTable` | ident: Identifier | `boolean` | purgeTable方法 |  |
| `renameTable` | oldIdent: Identifier; newIdent: Identifier | `void` | renameTable方法 |  |
| `setDelegateCatalog` | delegate: CatalogPlugin | `void` | setDelegateCatalog方法 |  |
| `tableExists` | ident: Identifier | `boolean` | tableExists方法 |  |

### Distributions
**包路径**: `org.apache.spark.sql.connector.distributions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `clustered` | clustering: Expression[] | `ClusteredDistribution` | clustered方法 |  |
| `ordered` | ordering: SortOrder[] | `OrderedDistribution` | ordered方法 |  |
| `unspecified` | 无 | `UnspecifiedDistribution` | unspecified方法 |  |

### ExpressionImplUtils
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSentences` | str: UTF8String; language: UTF8String; country: UTF8String | `ArrayData` | getSentences方法 |  |
| `getSparkVersion` | 无 | `UTF8String` | getSparkVersion方法 |  |
| `isLuhnNumber` | numberString: UTF8String | `boolean` | isLuhnNumber方法 |  |
| `quote` | str: UTF8String | `UTF8String` | quote方法 |  |
| `randStr` | rng: XORShiftRandom; length: int | `UTF8String` | randStr方法 |  |
| `tryValidateUTF8String` | utf8String: UTF8String | `UTF8String` | tryValidateUTF8String方法 |  |
| `validateUTF8String` | utf8String: UTF8String | `UTF8String` | validateUTF8String方法 |  |

### ExpressionInfo
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getArguments` | 无 | `String` | getArguments方法 |  |
| `getClassName` | 无 | `String` | getClassName方法 |  |
| `getDb` | 无 | `String` | getDb方法 |  |
| `getDeprecated` | 无 | `String` | getDeprecated方法 |  |
| `getExamples` | 无 | `String` | getExamples方法 |  |
| `getExtended` | 无 | `String` | getExtended方法 |  |
| `getGroup` | 无 | `String` | getGroup方法 |  |
| `getName` | 无 | `String` | getName方法 |  |
| `getNote` | 无 | `String` | getNote方法 |  |
| `getOriginalExamples` | 无 | `String` | getOriginalExamples方法 |  |
| `getSince` | 无 | `String` | getSince方法 |  |
| `getSource` | 无 | `String` | getSource方法 |  |
| `getUsage` | 无 | `String` | getUsage方法 |  |

### Expressions
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | name: String; args: Expression... | `Transform` | apply方法 |  |
| `bucket` | numBuckets: int; columns: String... | `Transform` | bucket方法 |  |
| `column` | name: String | `NamedReference` | column方法 |  |
| `days` | column: String | `Transform` | days方法 |  |
| `hours` | column: String | `Transform` | hours方法 |  |
| `identity` | column: String | `Transform` | identity方法 |  |
| `months` | column: String | `Transform` | months方法 |  |
| `sort` | expr: Expression; direction: SortDirection; nullOrder: NullOrdering | `SortOrder` | sort方法 |  |
| `sort` | expr: Expression; direction: SortDirection | `SortOrder` | sort方法 |  |
| `years` | column: String | `Transform` | years方法 |  |

### Extract
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `field` | 无 | `String` | field方法 |  |
| `source` | 无 | `Expression` | source方法 |  |

### ForeignKey
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `ForeignKey` | build方法 |  |
| `referencedTable` | 无 | `Identifier` | referencedTable方法 |  |

### GeneralScalarExpression
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | name方法 |  |

### GeographicSpatialReferenceSystemMapper
**包路径**: `org.apache.spark.sql.internal.types`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSrid` | stringId: String | `Integer` | getSrid方法 |  |
| `getStringId` | srid: int | `String` | getStringId方法 |  |

### GeometryModel
**包路径**: `org.apache.spark.sql.catalyst.util.geo`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toWkt` | 无 | `String` | toWkt方法 |  |

### GetArrayItem
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `childArray` | 无 | `Expression` | childArray方法 |  |
| `failOnError` | 无 | `boolean` | failOnError方法 |  |
| `ordinal` | 无 | `Expression` | ordinal方法 |  |

### HadoopCompressionCodec
**包路径**: `org.apache.spark.sql.catalyst.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCompressionCodec` | 无 | `CompressionCodec` | getCompressionCodec方法 |  |
| `lowerCaseName` | 无 | `String` | lowerCaseName方法 |  |

### HadoopLineRecordReader
**包路径**: `org.apache.spark.sql.execution.datasources`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCurrentKey` | 无 | `LongWritable` | getCurrentKey方法 |  |
| `getCurrentValue` | 无 | `Text` | getCurrentValue方法 |  |
| `getProgress` | 无 | `float` | getProgress方法 |  |
| `initialize` | genericSplit: InputSplit; context: TaskAttemptContext | `void` | initialize方法 |  |
| `nextKeyValue` | 无 | `boolean` | nextKeyValue方法 |  |

### HiveHasher
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `hashInt` | input: int | `int` | hashInt方法 |  |
| `hashLong` | input: long | `int` | hashLong方法 |  |
| `hashUnsafeBytes` | base: Object; offset: long; lengthInBytes: int | `int` | hashUnsafeBytes方法 |  |

### IdentityColumnSpec
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStart` | 无 | `long` | getStart方法 |  |
| `getStep` | 无 | `long` | getStep方法 |  |
| `isAllowExplicitInsert` | 无 | `boolean` | isAllowExplicitInsert方法 |  |

### IntegerAdd
**包路径**: `org.apache.spark.sql.connector.catalog.functions`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `invoke` | left: int; right: int | `int` | invoke方法 |  |
| `invoke` | left: int; right: int | `int` | invoke方法 |  |
| `produceResult` | input: InternalRow | `Integer` | produceResult方法 |  |

### JavaSQLDataSourceExample
**包路径**: `org.apache.spark.examples.sql`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCube` | 无 | `int` | getCube方法 |  |
| `getSquare` | 无 | `int` | getSquare方法 |  |
| `getValue` | 无 | `int` | getValue方法 |  |
| `getValue` | 无 | `int` | getValue方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `setCube` | cube: int | `void` | setCube方法 |  |
| `setSquare` | square: int | `void` | setSquare方法 |  |
| `setValue` | value: int | `void` | setValue方法 |  |
| `setValue` | value: int | `void` | setValue方法 |  |

### JavaSparkHiveExample
**包路径**: `org.apache.spark.examples.sql.hive`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getKey` | 无 | `int` | getKey方法 |  |
| `getValue` | 无 | `String` | getValue方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `setKey` | key: int | `void` | setKey方法 |  |
| `setValue` | value: String | `void` | setValue方法 |  |

### JavaSparkSQLCli
**包路径**: `org.apache.spark.examples.sql`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaSparkSQLExample
**包路径**: `org.apache.spark.examples.sql`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getAge` | 无 | `long` | getAge方法 |  |
| `getName` | 无 | `String` | getName方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `setAge` | age: long | `void` | setAge方法 |  |
| `setName` | name: String | `void` | setName方法 |  |

### JavaUserDefinedScalar
**包路径**: `org.apache.spark.examples.sql`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaUserDefinedTypedAggregation
**包路径**: `org.apache.spark.examples.sql`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `bufferEncoder` | 无 | `Encoder&lt;Average&gt;` | bufferEncoder方法 |  |
| `finish` | reduction: Average | `Double` | finish方法 |  |
| `getCount` | 无 | `long` | getCount方法 |  |
| `getName` | 无 | `String` | getName方法 |  |
| `getSalary` | 无 | `long` | getSalary方法 |  |
| `getSum` | 无 | `long` | getSum方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `merge` | b1: Average; b2: Average | `Average` | merge方法 |  |
| `outputEncoder` | 无 | `Encoder&lt;Double&gt;` | outputEncoder方法 |  |
| `reduce` | buffer: Average; employee: Employee | `Average` | reduce方法 |  |
| `setCount` | count: long | `void` | setCount方法 |  |
| `setName` | name: String | `void` | setName方法 |  |
| `setSalary` | salary: long | `void` | setSalary方法 |  |
| `setSum` | sum: long | `void` | setSum方法 |  |
| `zero` | 无 | `Average` | zero方法 |  |

### JavaUserDefinedUntypedAggregation
**包路径**: `org.apache.spark.examples.sql`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `bufferEncoder` | 无 | `Encoder&lt;Average&gt;` | bufferEncoder方法 |  |
| `finish` | reduction: Average | `Double` | finish方法 |  |
| `getCount` | 无 | `long` | getCount方法 |  |
| `getSum` | 无 | `long` | getSum方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `merge` | b1: Average; b2: Average | `Average` | merge方法 |  |
| `outputEncoder` | 无 | `Encoder&lt;Double&gt;` | outputEncoder方法 |  |
| `reduce` | buffer: Average; data: Long | `Average` | reduce方法 |  |
| `setCount` | count: long | `void` | setCount方法 |  |
| `setSum` | sum: long | `void` | setSum方法 |  |
| `zero` | 无 | `Average` | zero方法 |  |

### JsonExpressionUtils
**包路径**: `org.apache.spark.sql.catalyst.expressions.json`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `jsonObjectKeys` | json: UTF8String | `GenericArrayData` | jsonObjectKeys方法 |  |
| `lengthOfJsonArray` | json: UTF8String | `Integer` | lengthOfJsonArray方法 |  |

### KVSorterIterator
**包路径**: `org.apache.spark.sql.execution`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cleanupResources` | 无 | `void` | cleanupResources方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `compare` | baseObj1: Object; baseOff1: long; baseLen1: int; baseObj2: Object; baseOff2: long; baseLen2: int | `int` | compare方法 |  |
| `getKey` | 无 | `UnsafeRow` | getKey方法 |  |
| `getPeakMemoryUsedBytes` | 无 | `long` | getPeakMemoryUsedBytes方法 |  |
| `getSpillSize` | 无 | `long` | getSpillSize方法 |  |
| `getValue` | 无 | `UnsafeRow` | getValue方法 |  |
| `insertKV` | key: UnsafeRow; value: UnsafeRow | `void` | insertKV方法 |  |
| `merge` | other: UnsafeKVExternalSorter | `void` | merge方法 |  |
| `next` | 无 | `boolean` | next方法 |  |
| `sortedIterator` | 无 | `KVSorterIterator` | sortedIterator方法 |  |

### KeyGroupedPartitioning
**包路径**: `org.apache.spark.sql.connector.read.partitioning`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `numPartitions` | 无 | `int` | numPartitions方法 |  |

### NamespaceChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | `String` | property方法 |  |
| `property` | 无 | `String` | property方法 |  |
| `value` | 无 | `String` | value方法 |  |

### NonClosableMutableURLClassLoader
**包路径**: `org.apache.spark.sql.internal`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |

### NumericHistogram
**包路径**: `org.apache.spark.sql.util`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | v: double | `void` | add方法 |  |
| `addBin` | x: double; y: double; b: int | `void` | addBin方法 |  |
| `allocate` | num_bins: int | `void` | allocate方法 |  |
| `compareTo` | other: Coord | `int` | compareTo方法 |  |
| `getBin` | b: int | `Coord` | getBin方法 |  |
| `getNumBins` | 无 | `int` | getNumBins方法 |  |
| `getUsedBins` | 无 | `int` | getUsedBins方法 |  |
| `isReady` | 无 | `boolean` | isReady方法 |  |
| `merge` | other: NumericHistogram | `void` | merge方法 |  |
| `reset` | 无 | `void` | reset方法 |  |
| `setUsedBins` | nusedBins: int | `void` | setUsedBins方法 |  |

### OrcArrayColumnVector
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | getArray方法 |  |
| `getBoolean` | rowId: int | `boolean` | getBoolean方法 |  |
| `getByte` | rowId: int | `byte` | getByte方法 |  |
| `getDecimal` | rowId: int; precision: int; scale: int | `Decimal` | getDecimal方法 |  |
| `getDouble` | rowId: int | `double` | getDouble方法 |  |
| `getFloat` | rowId: int | `float` | getFloat方法 |  |
| `getInt` | rowId: int | `int` | getInt方法 |  |
| `getLong` | rowId: int | `long` | getLong方法 |  |
| `getMap` | rowId: int | `ColumnarMap` | getMap方法 |  |
| `getShort` | rowId: int | `short` | getShort方法 |  |
| `getUTF8String` | rowId: int | `UTF8String` | getUTF8String方法 |  |

### OrcAtomicColumnVector
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | getArray方法 |  |
| `getBoolean` | rowId: int | `boolean` | getBoolean方法 |  |
| `getByte` | rowId: int | `byte` | getByte方法 |  |
| `getDecimal` | rowId: int; precision: int; scale: int | `Decimal` | getDecimal方法 |  |
| `getDouble` | rowId: int | `double` | getDouble方法 |  |
| `getFloat` | rowId: int | `float` | getFloat方法 |  |
| `getInt` | rowId: int | `int` | getInt方法 |  |
| `getLong` | rowId: int | `long` | getLong方法 |  |
| `getMap` | rowId: int | `ColumnarMap` | getMap方法 |  |
| `getShort` | rowId: int | `short` | getShort方法 |  |
| `getUTF8String` | rowId: int | `UTF8String` | getUTF8String方法 |  |

### OrcColumnStatistics
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | newChild: OrcColumnStatistics | `void` | add方法 |  |
| `get` | ordinal: int | `OrcColumnStatistics` | get方法 |  |
| `getStatistics` | 无 | `ColumnStatistics` | getStatistics方法 |  |

### OrcColumnVector
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `hasNull` | 无 | `boolean` | hasNull方法 |  |
| `isNullAt` | rowId: int | `boolean` | isNullAt方法 |  |
| `numNulls` | 无 | `int` | numNulls方法 |  |
| `setBatchSize` | batchSize: int | `void` | setBatchSize方法 |  |

### OrcColumnarBatchReader
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `getCurrentKey` | 无 | `Void` | getCurrentKey方法 |  |
| `getCurrentValue` | 无 | `ColumnarBatch` | getCurrentValue方法 |  |
| `getProgress` | 无 | `float` | getProgress方法 |  |
| `initBatch` | orcSchema: TypeDescription; requiredFields: StructField[]; requestedDataColIds: int[]; requestedPartitionColIds: int[]; partitionValues: InternalRow | `void` | initBatch方法 |  |
| `initialize` | inputSplit: InputSplit; taskAttemptContext: TaskAttemptContext | `void` | initialize方法 |  |
| `initialize` | inputSplit: InputSplit; taskAttemptContext: TaskAttemptContext; orcTail: OrcTail | `void` | initialize方法 |  |
| `nextKeyValue` | 无 | `boolean` | nextKeyValue方法 |  |

### OrcCompressionCodec
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCompressionKind` | 无 | `CompressionKind` | getCompressionKind方法 |  |
| `lowerCaseName` | 无 | `String` | lowerCaseName方法 |  |

### OrcFooterReader
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `readStatistics` | orcReader: Reader | `OrcColumnStatistics` | readStatistics方法 |  |

### OrcMapColumnVector
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | getArray方法 |  |
| `getBoolean` | rowId: int | `boolean` | getBoolean方法 |  |
| `getByte` | rowId: int | `byte` | getByte方法 |  |
| `getDecimal` | rowId: int; precision: int; scale: int | `Decimal` | getDecimal方法 |  |
| `getDouble` | rowId: int | `double` | getDouble方法 |  |
| `getFloat` | rowId: int | `float` | getFloat方法 |  |
| `getInt` | rowId: int | `int` | getInt方法 |  |
| `getLong` | rowId: int | `long` | getLong方法 |  |
| `getMap` | ordinal: int | `ColumnarMap` | getMap方法 |  |
| `getShort` | rowId: int | `short` | getShort方法 |  |
| `getUTF8String` | rowId: int | `UTF8String` | getUTF8String方法 |  |

### OrcStructColumnVector
**包路径**: `org.apache.spark.sql.execution.datasources.orc`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | getArray方法 |  |
| `getBoolean` | rowId: int | `boolean` | getBoolean方法 |  |
| `getByte` | rowId: int | `byte` | getByte方法 |  |
| `getDecimal` | rowId: int; precision: int; scale: int | `Decimal` | getDecimal方法 |  |
| `getDouble` | rowId: int | `double` | getDouble方法 |  |
| `getFloat` | rowId: int | `float` | getFloat方法 |  |
| `getInt` | rowId: int | `int` | getInt方法 |  |
| `getLong` | rowId: int | `long` | getLong方法 |  |
| `getMap` | rowId: int | `ColumnarMap` | getMap方法 |  |
| `getShort` | rowId: int | `short` | getShort方法 |  |
| `getUTF8String` | rowId: int | `UTF8String` | getUTF8String方法 |  |

### ParquetCompressionCodec
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | s: String | `ParquetCompressionCodec` | fromString方法 |  |
| `getCompressionCodec` | 无 | `CompressionCodecName` | getCompressionCodec方法 |  |
| `lowerCaseName` | 无 | `String` | lowerCaseName方法 |  |

### ParquetFooterReader
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `openFileAndReadFooter` | hadoopConf: Configuration; file: PartitionedFile; keepInputStreamOpen: boolean | `OpenedParquetFooter` | openFileAndReadFooter方法 |  |
| `readFooter` | inputFile: HadoopInputFile; filter: ParquetMetadataConverter.MetadataFilter | `ParquetMetadata` | readFooter方法 |  |

### ParquetVectorUpdaterFactory
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 146

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `decodeSingleDictionaryId` | offset: int; values: WritableColumnVector; dictionaryIds: WritableColumnVector; dictionary: Dictionary | `void` | decodeSingleDictionaryId方法 |  |
| `getUpdater` | descriptor: ColumnDescriptor; sparkType: DataType | `ParquetVectorUpdater` | getUpdater方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValue` | offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValue方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `readValues` | total: int; offset: int; values: WritableColumnVector; valuesReader: VectorizedValuesReader | `void` | readValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |
| `skipValues` | total: int; valuesReader: VectorizedValuesReader | `void` | skipValues方法 |  |

### PrimaryKey
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `PrimaryKey` | build方法 |  |

### ProcedureParameter
**包路径**: `org.apache.spark.sql.connector.catalog.procedures`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `ProcedureParameter` | build方法 |  |
| `comment` | comment: String | `Builder` | comment方法 |  |
| `defaultValue` | sql: String | `Builder` | defaultValue方法 |  |
| `defaultValue` | expression: Expression | `Builder` | defaultValue方法 |  |
| `defaultValue` | defaultValue: DefaultValue | `Builder` | defaultValue方法 |  |

### RowBasedKeyValueBatch
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `allocate` | keySchema: StructType; valueSchema: StructType; manager: TaskMemoryManager | `RowBasedKeyValueBatch` | allocate方法 |  |
| `allocate` | keySchema: StructType; valueSchema: StructType; manager: TaskMemoryManager; maxRows: int | `RowBasedKeyValueBatch` | allocate方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `getValueRow` | rowId: int | `UnsafeRow` | getValueRow方法 |  |
| `numRows` | 无 | `int` | numRows方法 |  |
| `spill` | size: long; trigger: MemoryConsumer | `long` | spill方法 |  |

### RowFactory
**包路径**: `org.apache.spark.sql`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | values: Object ... | `Row` | create方法 |  |

### SchemaColumnConvertNotSupportedException
**包路径**: `org.apache.spark.sql.execution.datasources`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getColumn` | 无 | `String` | getColumn方法 |  |
| `getLogicalType` | 无 | `String` | getLogicalType方法 |  |
| `getPhysicalType` | 无 | `String` | getPhysicalType方法 |  |

### SortDirection
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultNullOrdering` | 无 | `NullOrdering` | defaultNullOrdering方法 |  |

### SpatialReferenceSystemCache
**包路径**: `org.apache.spark.sql.internal.types`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getInstance` | 无 | `SpatialReferenceSystemCache` | getInstance方法 |  |
| `getSridToSrs` | 无 | `Map&lt;Integer, SpatialReferenceSystemInformation&gt;` | getSridToSrs方法 |  |
| `getSrsInfo` | srid: int | `SpatialReferenceSystemInformation` | getSrsInfo方法 |  |
| `getSrsInfo` | stringId: String | `SpatialReferenceSystemInformation` | getSrsInfo方法 |  |
| `getStringIdToSrs` | 无 | `Map&lt;String, SpatialReferenceSystemInformation&gt;` | getStringIdToSrs方法 |  |

### SpecificParquetRecordReaderBase
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `getCurrentKey` | 无 | `Void` | getCurrentKey方法 |  |
| `initialize` | inputSplit: InputSplit; taskAttemptContext: TaskAttemptContext | `void` | initialize方法 |  |
| `initialize` | inputSplit: InputSplit; taskAttemptContext: TaskAttemptContext; inputFile: Option<HadoopInputFile>; inputStream: Option<SeekableInputStream>; fileFooter: Option<ParquetMetadata> | `void` | initialize方法 |  |
| `readNextRowGroup` | 无 | `PageReadStore` | readNextRowGroup方法 |  |

### SupportsPushDownJoin
**包路径**: `org.apache.spark.sql.connector.read`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `prettyString` | 无 | `String` | prettyString方法 |  |

### TableChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `column` | 无 | `String` | column方法 |  |
| `comment` | 无 | `String` | comment方法 |  |
| `constraint` | 无 | `Constraint` | constraint方法 |  |
| `dataType` | 无 | `DataType` | dataType方法 |  |
| `defaultValue` | 无 | `ColumnDefaultValue` | defaultValue方法 |  |
| `ifExists` | 无 | `Boolean` | ifExists方法 |  |
| `ifExists` | 无 | `boolean` | ifExists方法 |  |
| `isNullable` | 无 | `boolean` | isNullable方法 |  |
| `mode` | 无 | `Mode` | mode方法 |  |
| `name` | 无 | `String` | name方法 |  |
| `newComment` | 无 | `String` | newComment方法 |  |
| `newCurrentDefault` | 无 | `DefaultValue` | newCurrentDefault方法 |  |
| `newDataType` | 无 | `DataType` | newDataType方法 |  |
| `newDefaultValue` | 无 | `String` | newDefaultValue方法 |  |
| `newName` | 无 | `String` | newName方法 |  |
| `nullable` | 无 | `boolean` | nullable方法 |  |
| `position` | 无 | `ColumnPosition` | position方法 |  |
| `position` | 无 | `ColumnPosition` | position方法 |  |
| `property` | 无 | `String` | property方法 |  |
| `property` | 无 | `String` | property方法 |  |
| `validatedTableVersion` | 无 | `String` | validatedTableVersion方法 |  |
| `value` | 无 | `String` | value方法 |  |

### TableInfo
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `TableInfo` | build方法 |  |
| `properties` | 无 | `Map&lt;String, String&gt;` | properties方法 |  |
| `schema` | 无 | `StructType` | schema方法 |  |
| `withColumns` | columns: Column[] | `Builder` | withColumns方法 |  |
| `withConstraints` | constraints: Constraint[] | `Builder` | withConstraints方法 |  |
| `withPartitions` | partitions: Transform[] | `Builder` | withPartitions方法 |  |
| `withProperties` | properties: String> | `Builder` | withProperties方法 |  |

### UDFXPathUtil
**包路径**: `org.apache.spark.sql.catalyst.expressions.xml`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `eval` | xml: String; path: String; qname: QName | `Object` | eval方法 |  |
| `evalBoolean` | xml: String; path: String | `Boolean` | evalBoolean方法 |  |
| `evalNode` | xml: String; path: String | `Node` | evalNode方法 |  |
| `evalNodeList` | xml: String; path: String | `NodeList` | evalNodeList方法 |  |
| `evalNumber` | xml: String; path: String | `Double` | evalNumber方法 |  |
| `evalString` | xml: String; path: String | `String` | evalString方法 |  |
| `mark` | readAheadLimit: int | `void` | mark方法 |  |
| `markSupported` | 无 | `boolean` | markSupported方法 |  |
| `read` | 无 | `int` | read方法 |  |
| `read` | cbuf: char[]; off: int; len: int | `int` | read方法 |  |
| `ready` | 无 | `boolean` | ready方法 |  |
| `reset` | 无 | `void` | reset方法 |  |
| `set` | s: String | `void` | set方法 |  |
| `skip` | ns: long | `long` | skip方法 |  |

### Unique
**包路径**: `org.apache.spark.sql.connector.catalog.constraints`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `Unique` | build方法 |  |

### UnknownPartitioning
**包路径**: `org.apache.spark.sql.connector.read.partitioning`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `numPartitions` | 无 | `int` | numPartitions方法 |  |

### UnsafeWriter
**包路径**: `org.apache.spark.sql.catalyst.expressions.codegen`
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cursor` | 无 | `int` | cursor方法 |  |
| `getBufferHolder` | 无 | `BufferHolder` | getBufferHolder方法 |  |
| `grow` | neededSize: int | `void` | grow方法 |  |
| `increaseCursor` | val: int | `void` | increaseCursor方法 |  |
| `reset` | 无 | `void` | reset方法 |  |
| `setOffsetAndSizeFromPreviousCursor` | ordinal: int; previousCursor: int | `void` | setOffsetAndSizeFromPreviousCursor方法 |  |
| `totalSize` | 无 | `int` | totalSize方法 |  |
| `write` | ordinal: int; input: UTF8String | `void` | write方法 |  |
| `write` | ordinal: int; input: GeographyVal | `void` | write方法 |  |
| `write` | ordinal: int; input: GeometryVal | `void` | write方法 |  |
| `write` | ordinal: int; input: byte[] | `void` | write方法 |  |
| `write` | ordinal: int; input: byte[]; offset: int; numBytes: int | `void` | write方法 |  |
| `write` | ordinal: int; input: CalendarInterval | `void` | write方法 |  |
| `write` | ordinal: int; input: VariantVal | `void` | write方法 |  |
| `write` | ordinal: int; row: UnsafeRow | `void` | write方法 |  |
| `write` | ordinal: int; map: UnsafeMapData | `void` | write方法 |  |
| `write` | array: UnsafeArrayData | `void` | write方法 |  |

### UserDefinedAggregateFunc
**包路径**: `org.apache.spark.sql.connector.expressions.aggregate`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `canonicalName` | 无 | `String` | canonicalName方法 |  |
| `isDistinct` | 无 | `boolean` | isDistinct方法 |  |
| `name` | 无 | `String` | name方法 |  |

### UserDefinedScalarFunc
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `canonicalName` | 无 | `String` | canonicalName方法 |  |
| `name` | 无 | `String` | name方法 |  |

### V2ExpressionSQLBuilder
**包路径**: `org.apache.spark.sql.connector.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | expr: Expression | `String` | build方法 |  |

### VectorFunctionImplUtils
**包路径**: `org.apache.spark.sql.catalyst.expressions`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `vectorCosineSimilarity` | left: ArrayData; right: ArrayData; funcName: UTF8String | `Float` | vectorCosineSimilarity方法 |  |
| `vectorInfNorm` | vec: ArrayData | `Float` | vectorInfNorm方法 |  |
| `vectorInnerProduct` | left: ArrayData; right: ArrayData; funcName: UTF8String | `Float` | vectorInnerProduct方法 |  |
| `vectorL1Norm` | vec: ArrayData | `Float` | vectorL1Norm方法 |  |
| `vectorL2Distance` | left: ArrayData; right: ArrayData; funcName: UTF8String | `Float` | vectorL2Distance方法 |  |
| `vectorL2Norm` | vec: ArrayData | `Float` | vectorL2Norm方法 |  |
| `vectorNorm` | vec: ArrayData; degree: float; funcName: UTF8String | `Float` | vectorNorm方法 |  |
| `vectorNormalize` | vec: ArrayData; degree: float; funcName: UTF8String | `ArrayData` | vectorNormalize方法 |  |
| `vectorNormalizeWithNorm` | vec: ArrayData; norm: float | `ArrayData` | vectorNormalizeWithNorm方法 |  |

### VectorizedColumnReader
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `visit` | dataPageV1: DataPageV1 | `Integer` | visit方法 |  |
| `visit` | dataPageV2: DataPageV2 | `Integer` | visit方法 |  |

### VectorizedDeltaBinaryPackedReader
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initFromPage` | valueCount: int; in: ByteBufferInputStream | `void` | initFromPage方法 |  |
| `readByte` | 无 | `byte` | readByte方法 |  |
| `readBytes` | total: int; c: WritableColumnVector; rowId: int | `void` | readBytes方法 |  |
| `readInteger` | 无 | `int` | readInteger方法 |  |
| `readIntegers` | total: int; c: WritableColumnVector; rowId: int | `void` | readIntegers方法 |  |
| `readIntegersWithRebase` | total: int; c: WritableColumnVector; rowId: int; failIfRebase: boolean | `void` | readIntegersWithRebase方法 |  |
| `readLong` | 无 | `long` | readLong方法 |  |
| `readLongs` | total: int; c: WritableColumnVector; rowId: int | `void` | readLongs方法 |  |
| `readLongsWithRebase` | total: int; c: WritableColumnVector; rowId: int; failIfRebase: boolean; timeZone: String | `void` | readLongsWithRebase方法 |  |
| `readShort` | 无 | `short` | readShort方法 |  |
| `readShorts` | total: int; c: WritableColumnVector; rowId: int | `void` | readShorts方法 |  |
| `readUnsignedIntegers` | total: int; c: WritableColumnVector; rowId: int | `void` | readUnsignedIntegers方法 |  |
| `readUnsignedLongs` | total: int; c: WritableColumnVector; rowId: int | `void` | readUnsignedLongs方法 |  |
| `skipBytes` | total: int | `void` | skipBytes方法 |  |
| `skipIntegers` | total: int | `void` | skipIntegers方法 |  |
| `skipLongs` | total: int | `void` | skipLongs方法 |  |
| `skipShorts` | total: int | `void` | skipShorts方法 |  |

### VectorizedDeltaByteArrayReader
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initFromPage` | valueCount: int; in: ByteBufferInputStream | `void` | initFromPage方法 |  |
| `readBinary` | len: int | `Binary` | readBinary方法 |  |
| `readBinary` | total: int; c: WritableColumnVector; rowId: int | `void` | readBinary方法 |  |
| `readGeography` | total: int; c: WritableColumnVector; rowId: int | `void` | readGeography方法 |  |
| `readGeometry` | total: int; c: WritableColumnVector; rowId: int | `void` | readGeometry方法 |  |
| `setPreviousReader` | reader: ValuesReader | `void` | setPreviousReader方法 |  |
| `skipBinary` | total: int | `void` | skipBinary方法 |  |

### VectorizedDeltaLengthByteArrayReader
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getBytes` | rowId: int | `ByteBuffer` | getBytes方法 |  |
| `initFromPage` | valueCount: int; in: ByteBufferInputStream | `void` | initFromPage方法 |  |
| `readBinary` | total: int; c: WritableColumnVector; rowId: int | `void` | readBinary方法 |  |
| `readGeography` | total: int; c: WritableColumnVector; rowId: int | `void` | readGeography方法 |  |
| `readGeometry` | total: int; c: WritableColumnVector; rowId: int | `void` | readGeometry方法 |  |
| `skipBinary` | total: int | `void` | skipBinary方法 |  |

### VectorizedParquetRecordReader
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `enableReturningBatches` | 无 | `void` | enableReturningBatches方法 |  |
| `getCurrentValue` | 无 | `Object` | getCurrentValue方法 |  |
| `getProgress` | 无 | `float` | getProgress方法 |  |
| `initBatch` | partitionColumns: StructType; partitionValues: InternalRow | `void` | initBatch方法 |  |
| `initialize` | inputSplit: InputSplit; taskAttemptContext: TaskAttemptContext | `void` | initialize方法 |  |
| `initialize` | inputSplit: InputSplit; taskAttemptContext: TaskAttemptContext; inputFile: Option<HadoopInputFile>; inputStream: Option<SeekableInputStream>; fileFooter: Option<ParquetMetadata> | `void` | initialize方法 |  |
| `initialize` | path: String; columns: List<String> | `void` | initialize方法 |  |
| `initialize` | fileSchema: MessageType; requestedSchema: MessageType; rowGroupReader: ParquetRowGroupReader; totalRowCount: int | `void` | initialize方法 |  |
| `nextBatch` | 无 | `boolean` | nextBatch方法 |  |
| `nextKeyValue` | 无 | `boolean` | nextKeyValue方法 |  |
| `resultBatch` | 无 | `ColumnarBatch` | resultBatch方法 |  |

### VectorizedPlainValuesReader
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 33

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initFromPage` | valueCount: int; in: ByteBufferInputStream | `void` | initFromPage方法 |  |
| `readBinary` | total: int; v: WritableColumnVector; rowId: int | `void` | readBinary方法 |  |
| `readBinary` | len: int | `Binary` | readBinary方法 |  |
| `readBoolean` | 无 | `boolean` | readBoolean方法 |  |
| `readBooleans` | total: int; c: WritableColumnVector; rowId: int | `void` | readBooleans方法 |  |
| `readByte` | 无 | `byte` | readByte方法 |  |
| `readBytes` | total: int; c: WritableColumnVector; rowId: int | `void` | readBytes方法 |  |
| `readDouble` | 无 | `double` | readDouble方法 |  |
| `readDoubles` | total: int; c: WritableColumnVector; rowId: int | `void` | readDoubles方法 |  |
| `readFloat` | 无 | `float` | readFloat方法 |  |
| `readFloats` | total: int; c: WritableColumnVector; rowId: int | `void` | readFloats方法 |  |
| `readGeography` | total: int; v: WritableColumnVector; rowId: int | `void` | readGeography方法 |  |
| `readGeometry` | total: int; v: WritableColumnVector; rowId: int | `void` | readGeometry方法 |  |
| `readInteger` | 无 | `int` | readInteger方法 |  |
| `readIntegers` | total: int; c: WritableColumnVector; rowId: int | `void` | readIntegers方法 |  |
| `readIntegersWithRebase` | total: int; c: WritableColumnVector; rowId: int; failIfRebase: boolean | `void` | readIntegersWithRebase方法 |  |
| `readLong` | 无 | `long` | readLong方法 |  |
| `readLongs` | total: int; c: WritableColumnVector; rowId: int | `void` | readLongs方法 |  |
| `readLongsWithRebase` | total: int; c: WritableColumnVector; rowId: int; failIfRebase: boolean; timeZone: String | `void` | readLongsWithRebase方法 |  |
| `readShort` | 无 | `short` | readShort方法 |  |
| `readShorts` | total: int; c: WritableColumnVector; rowId: int | `void` | readShorts方法 |  |
| `readUnsignedIntegers` | total: int; c: WritableColumnVector; rowId: int | `void` | readUnsignedIntegers方法 |  |
| `readUnsignedLongs` | total: int; c: WritableColumnVector; rowId: int | `void` | readUnsignedLongs方法 |  |
| `skip` | 无 | `void` | skip方法 |  |
| `skipBinary` | total: int | `void` | skipBinary方法 |  |
| `skipBooleans` | total: int | `void` | skipBooleans方法 |  |
| `skipBytes` | total: int | `void` | skipBytes方法 |  |
| `skipDoubles` | total: int | `void` | skipDoubles方法 |  |
| `skipFixedLenByteArray` | total: int; len: int | `void` | skipFixedLenByteArray方法 |  |
| `skipFloats` | total: int | `void` | skipFloats方法 |  |
| `skipIntegers` | total: int | `void` | skipIntegers方法 |  |
| `skipLongs` | total: int | `void` | skipLongs方法 |  |
| `skipShorts` | total: int | `void` | skipShorts方法 |  |

### VectorizedReaderBase
**包路径**: `org.apache.spark.sql.execution.datasources.parquet`
**方法数量**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `readBinary` | len: int | `Binary` | readBinary方法 |  |
| `readBinary` | total: int; c: WritableColumnVector; rowId: int | `void` | readBinary方法 |  |
| `readBooleans` | total: int; c: WritableColumnVector; rowId: int | `void` | readBooleans方法 |  |
| `readByte` | 无 | `byte` | readByte方法 |  |
| `readBytes` | total: int; c: WritableColumnVector; rowId: int | `void` | readBytes方法 |  |
| `readDoubles` | total: int; c: WritableColumnVector; rowId: int | `void` | readDoubles方法 |  |
| `readFloats` | total: int; c: WritableColumnVector; rowId: int | `void` | readFloats方法 |  |
| `readGeography` | total: int; c: WritableColumnVector; rowId: int | `void` | readGeography方法 |  |
| `readGeometry` | total: int; c: WritableColumnVector; rowId: int | `void` | readGeometry方法 |  |
| `readIntegers` | total: int; c: WritableColumnVector; rowId: int | `void` | readIntegers方法 |  |
| `readIntegersWithRebase` | total: int; c: WritableColumnVector; rowId: int; failIfRebase: boolean | `void` | readIntegersWithRebase方法 |  |
| `readLongs` | total: int; c: WritableColumnVector; rowId: int | `void` | readLongs方法 |  |
| `readLongsWithRebase` | total: int; c: WritableColumnVector; rowId: int; failIfRebase: boolean; timeZone: String | `void` | readLongsWithRebase方法 |  |
| `readShort` | 无 | `short` | readShort方法 |  |
| `readShorts` | total: int; c: WritableColumnVector; rowId: int | `void` | readShorts方法 |  |
| `readUnsignedIntegers` | total: int; c: WritableColumnVector; rowId: int | `void` | readUnsignedIntegers方法 |  |
| `readUnsignedLongs` | total: int; c: WritableColumnVector; rowId: int | `void` | readUnsignedLongs方法 |  |
| `skip` | 无 | `void` | skip方法 |  |
| `skipBinary` | total: int | `void` | skipBinary方法 |  |
| `skipBooleans` | total: int | `void` | skipBooleans方法 |  |
| `skipBytes` | total: int | `void` | skipBytes方法 |  |
| `skipDoubles` | total: int | `void` | skipDoubles方法 |  |
| `skipFixedLenByteArray` | total: int; len: int | `void` | skipFixedLenByteArray方法 |  |
| `skipFloats` | total: int | `void` | skipFloats方法 |  |
| `skipIntegers` | total: int | `void` | skipIntegers方法 |  |
| `skipLongs` | total: int | `void` | skipLongs方法 |  |
| `skipShorts` | total: int | `void` | skipShorts方法 |  |

### ViewChange
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `property` | 无 | `String` | property方法 |  |
| `property` | 无 | `String` | property方法 |  |
| `value` | 无 | `String` | value方法 |  |

### ViewInfo
**包路径**: `org.apache.spark.sql.connector.catalog`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `currentCatalog` | 无 | `String` | currentCatalog方法 |  |
| `ident` | 无 | `Identifier` | ident方法 |  |
| `properties` | 无 | `Map&lt;String, String&gt;` | properties方法 |  |
| `schema` | 无 | `StructType` | schema方法 |  |
| `sql` | 无 | `String` | sql方法 |  |

### WkbParseException
**包路径**: `org.apache.spark.sql.catalyst.util.geo`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getParseError` | 无 | `String` | getParseError方法 |  |
| `getPosition` | 无 | `long` | getPosition方法 |  |

### WkbReader
**包路径**: `org.apache.spark.sql.catalyst.util.geo`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `read` | wkb: byte[] | `GeometryModel` | read方法 |  |
| `read` | wkb: byte[]; srid: int | `GeometryModel` | read方法 |  |

### WritableColumnVector
**包路径**: `org.apache.spark.sql.execution.vectorized`
**方法数量**: 58

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addElementsAppended` | num: int | `void` | addElementsAppended方法 |  |
| `appendArray` | length: int | `int` | appendArray方法 |  |
| `appendBoolean` | v: boolean | `int` | appendBoolean方法 |  |
| `appendBooleans` | count: int; v: boolean | `int` | appendBooleans方法 |  |
| `appendBooleans` | count: int; src: byte; offset: int | `int` | appendBooleans方法 |  |
| `appendByte` | v: byte | `int` | appendByte方法 |  |
| `appendByteArray` | value: byte[]; offset: int; length: int | `int` | appendByteArray方法 |  |
| `appendBytes` | count: int; v: byte | `int` | appendBytes方法 |  |
| `appendBytes` | length: int; src: byte[]; offset: int | `int` | appendBytes方法 |  |
| `appendDouble` | v: double | `int` | appendDouble方法 |  |
| `appendDoubles` | count: int; v: double | `int` | appendDoubles方法 |  |
| `appendDoubles` | length: int; src: double[]; offset: int | `int` | appendDoubles方法 |  |
| `appendFloat` | v: float | `int` | appendFloat方法 |  |
| `appendFloats` | count: int; v: float | `int` | appendFloats方法 |  |
| `appendFloats` | length: int; src: float[]; offset: int | `int` | appendFloats方法 |  |
| `appendInt` | v: int | `int` | appendInt方法 |  |
| `appendInts` | count: int; v: int | `int` | appendInts方法 |  |
| `appendInts` | length: int; src: int[]; offset: int | `int` | appendInts方法 |  |
| `appendLong` | v: long | `int` | appendLong方法 |  |
| `appendLongs` | count: int; v: long | `int` | appendLongs方法 |  |
| `appendLongs` | length: int; src: long[]; offset: int | `int` | appendLongs方法 |  |
| `appendNotNull` | 无 | `int` | appendNotNull方法 |  |
| `appendNotNulls` | count: int | `int` | appendNotNulls方法 |  |
| `appendNull` | 无 | `int` | appendNull方法 |  |
| `appendNulls` | count: int | `int` | appendNulls方法 |  |
| `appendObjects` | length: int; value: Object | `Optional&lt;Integer&gt;` | appendObjects方法 |  |
| `appendShort` | v: short | `int` | appendShort方法 |  |
| `appendShorts` | count: int; v: short | `int` | appendShorts方法 |  |
| `appendShorts` | length: int; src: short[]; offset: int | `int` | appendShorts方法 |  |
| `appendStruct` | isNull: boolean | `int` | appendStruct方法 |  |
| `arrayData` | 无 | `WritableColumnVector` | arrayData方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `closeIfFreeable` | 无 | `void` | closeIfFreeable方法 |  |
| `getArray` | rowId: int | `ColumnarArray` | getArray方法 |  |
| `getChild` | ordinal: int | `WritableColumnVector` | getChild方法 |  |
| `getDecimal` | rowId: int; precision: int; scale: int | `Decimal` | getDecimal方法 |  |
| `getDictionaryIds` | 无 | `WritableColumnVector` | getDictionaryIds方法 |  |
| `getElementsAppended` | 无 | `int` | getElementsAppended方法 |  |
| `getMap` | rowId: int | `ColumnarMap` | getMap方法 |  |
| `getNumChildren` | 无 | `int` | getNumChildren方法 |  |
| `getUTF8String` | rowId: int | `UTF8String` | getUTF8String方法 |  |
| `hasDictionary` | 无 | `boolean` | hasDictionary方法 |  |
| `hasNull` | 无 | `boolean` | hasNull方法 |  |
| `isAllNull` | 无 | `boolean` | isAllNull方法 |  |
| `isMissing` | 无 | `boolean` | isMissing方法 |  |
| `numNulls` | 无 | `int` | numNulls方法 |  |
| `putBooleans` | rowId: int; count: int; src: byte; srcIndex: int | `void` | putBooleans方法 |  |
| `putByteArray` | rowId: int; value: byte[] | `int` | putByteArray方法 |  |
| `putByteArray` | rowId: int; src: ByteBuffer; srcPosition: int; length: int | `int` | putByteArray方法 |  |
| `putDecimal` | rowId: int; value: Decimal; precision: int | `void` | putDecimal方法 |  |
| `putInterval` | rowId: int; value: CalendarInterval | `void` | putInterval方法 |  |
| `reserve` | requiredCapacity: int | `void` | reserve方法 |  |
| `reserveAdditional` | additionalCapacity: int | `void` | reserveAdditional方法 |  |
| `reserveDictionaryIds` | capacity: int | `WritableColumnVector` | reserveDictionaryIds方法 |  |
| `reset` | 无 | `void` | reset方法 |  |
| `setDictionary` | dictionary: Dictionary | `void` | setDictionary方法 |  |
| `setIsConstant` | 无 | `void` | setIsConstant方法 |  |
| `setMissing` | 无 | `void` | setMissing方法 |  |

### WriteBuilder
**包路径**: `org.apache.spark.sql.connector.write`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toBatch` | 无 | `BatchWrite` | toBatch方法 |  |
| `toStreaming` | 无 | `StreamingWrite` | toStreaming方法 |  |

---

## Streaming流处理

### BatchStatus
**包路径**: `org.apache.spark.status.api.v1.streaming`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `BatchStatus` | fromString方法 |  |

### EventTypes
**包路径**: `org.apache.spark.examples.sql.streaming`
**方法数量**: 27

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | userId: String; events: Iterator<Row>; state: GroupState<Sessions> | `Iterator&lt;Session&gt;` | call方法 |  |
| `endTime` | 无 | `Timestamp` | endTime方法 |  |
| `getDuration` | 无 | `long` | getDuration方法 |  |
| `getEndTimestamp` | 无 | `Timestamp` | getEndTimestamp方法 |  |
| `getEventType` | 无 | `EventTypes` | getEventType方法 |  |
| `getEvents` | 无 | `List&lt;SessionEvent&gt;` | getEvents方法 |  |
| `getId` | 无 | `String` | getId方法 |  |
| `getNumEvents` | 无 | `int` | getNumEvents方法 |  |
| `getSessions` | 无 | `List&lt;SessionAcc&gt;` | getSessions方法 |  |
| `getStartTimestamp` | 无 | `Timestamp` | getStartTimestamp方法 |  |
| `getUserId` | 无 | `String` | getUserId方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `newInstance` | sessions: List<SessionAcc> | `Sessions` | newInstance方法 |  |
| `newInstance` | userId: String; eventTypeStr: String; startTimestamp: Timestamp; gapDuration: long | `SessionEvent` | newInstance方法 |  |
| `newInstance` | event: SessionEvent | `SessionAcc` | newInstance方法 |  |
| `newInstance` | events: List<SessionEvent> | `SessionAcc` | newInstance方法 |  |
| `newInstance` | id: String; duration: long; numEvents: int | `Session` | newInstance方法 |  |
| `setDuration` | duration: long | `void` | setDuration方法 |  |
| `setEndTimestamp` | endTimestamp: Timestamp | `void` | setEndTimestamp方法 |  |
| `setEventType` | eventType: EventTypes | `void` | setEventType方法 |  |
| `setEvents` | events: List<SessionEvent> | `void` | setEvents方法 |  |
| `setId` | id: String | `void` | setId方法 |  |
| `setNumEvents` | numEvents: int | `void` | setNumEvents方法 |  |
| `setSessions` | sessions: List<SessionAcc> | `void` | setSessions方法 |  |
| `setStartTimestamp` | startTimestamp: Timestamp | `void` | setStartTimestamp方法 |  |
| `setUserId` | userId: String | `void` | setUserId方法 |  |
| `startTime` | 无 | `Timestamp` | startTime方法 |  |

### GroupStateTimeout
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `EventTimeTimeout` | 无 | `GroupStateTimeout` | EventTimeTimeout方法 |  |
| `NoTimeout` | 无 | `GroupStateTimeout` | NoTimeout方法 |  |
| `ProcessingTimeTimeout` | 无 | `GroupStateTimeout` | ProcessingTimeTimeout方法 |  |

### JavaCustomReceiver
**包路径**: `org.apache.spark.examples.streaming`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `onStart` | 无 | `void` | onStart方法 |  |
| `onStop` | 无 | `void` | onStop方法 |  |

### JavaRecord
**包路径**: `org.apache.spark.examples.streaming`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getWord` | 无 | `String` | getWord方法 |  |
| `setWord` | word: String | `void` | setWord方法 |  |

### JavaStatefulNetworkWordCount
**包路径**: `org.apache.spark.examples.streaming`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### KinesisInitialPositions
**包路径**: `org.apache.spark.streaming.kinesis`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromKinesisInitialPosition` | initialPositionInStream: InitialPositionInStream | `KinesisInitialPosition` | fromKinesisInitialPosition方法 |  |
| `getPosition` | 无 | `InitialPositionInStream` | getPosition方法 |  |
| `getPosition` | 无 | `InitialPositionInStream` | getPosition方法 |  |
| `getPosition` | 无 | `InitialPositionInStream` | getPosition方法 |  |
| `getTimestamp` | 无 | `Date` | getTimestamp方法 |  |

### OutputMode
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Append` | 无 | `OutputMode` | Append方法 |  |
| `Complete` | 无 | `OutputMode` | Complete方法 |  |
| `Update` | 无 | `OutputMode` | Update方法 |  |

### ReadMaxBytes
**包路径**: `org.apache.spark.sql.connector.read.streaming`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `maxBytes` | 无 | `long` | maxBytes方法 |  |

### ReadMaxFiles
**包路径**: `org.apache.spark.sql.connector.read.streaming`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `maxFiles` | 无 | `int` | maxFiles方法 |  |

### SupportsRealTimeRead
**包路径**: `org.apache.spark.sql.connector.read.streaming`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `hasRecord` | 无 | `boolean` | hasRecord方法 |  |
| `newStatusWithArrivalTimeMs` | recArrivalTime: Long | `RecordStatus` | newStatusWithArrivalTimeMs方法 |  |
| `newStatusWithoutArrivalTime` | hasRecord: boolean | `RecordStatus` | newStatusWithoutArrivalTime方法 |  |
| `recArrivalTime` | 无 | `Optional&lt;Long&gt;` | recArrivalTime方法 |  |

### TimeMode
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `EventTime` | 无 | `TimeMode` | EventTime方法 |  |
| `None` | 无 | `TimeMode` | None方法 |  |
| `ProcessingTime` | 无 | `TimeMode` | ProcessingTime方法 |  |

### Trigger
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AvailableNow` | 无 | `Trigger` | AvailableNow方法 |  |
| `Continuous` | intervalMs: long | `Trigger` | Continuous方法 |  |
| `Continuous` | interval: long; timeUnit: TimeUnit | `Trigger` | Continuous方法 |  |
| `Continuous` | interval: Duration | `Trigger` | Continuous方法 |  |
| `Continuous` | interval: String | `Trigger` | Continuous方法 |  |
| `Once` | 无 | `Trigger` | Once方法 |  |
| `ProcessingTime` | intervalMs: long | `Trigger` | ProcessingTime方法 |  |
| `ProcessingTime` | interval: long; timeUnit: TimeUnit | `Trigger` | ProcessingTime方法 |  |
| `ProcessingTime` | interval: Duration | `Trigger` | ProcessingTime方法 |  |
| `ProcessingTime` | interval: String | `Trigger` | ProcessingTime方法 |  |
| `RealTime` | batchDurationMs: long | `Trigger` | RealTime方法 |  |
| `RealTime` | batchDuration: long; timeUnit: TimeUnit | `Trigger` | RealTime方法 |  |
| `RealTime` | batchDuration: Duration | `Trigger` | RealTime方法 |  |
| `RealTime` | batchDuration: String | `Trigger` | RealTime方法 |  |
| `RealTime` | 无 | `Trigger` | RealTime方法 |  |

---

## 其他辅助类

### AbstractAuthRpcHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelActive` | client: TransportClient | `void` | channelActive方法 |  |
| `channelInactive` | client: TransportClient | `void` | channelInactive方法 |  |
| `exceptionCaught` | cause: Throwable; client: TransportClient | `void` | exceptionCaught方法 |  |
| `getMergedBlockMetaReqHandler` | 无 | `MergedBlockMetaReqHandler` | getMergedBlockMetaReqHandler方法 |  |
| `getStreamManager` | 无 | `StreamManager` | getStreamManager方法 |  |
| `isAuthenticated` | 无 | `boolean` | isAuthenticated方法 |  |
| `receive` | client: TransportClient; message: ByteBuffer; callback: RpcResponseCallback | `void` | receive方法 |  |
| `receive` | client: TransportClient; message: ByteBuffer | `void` | receive方法 |  |
| `receiveStream` | client: TransportClient; message: ByteBuffer; callback: RpcResponseCallback | `StreamCallbackWithID` | receiveStream方法 |  |

### AbstractFetchShuffleBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### AbstractFileRegion
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `retain` | 无 | `AbstractFileRegion` | retain方法 |  |
| `retain` | increment: int | `AbstractFileRegion` | retain方法 |  |
| `touch` | 无 | `AbstractFileRegion` | touch方法 |  |
| `touch` | o: Object | `AbstractFileRegion` | touch方法 |  |
| `transfered` | 无 | `long` | transfered方法 |  |

### AbstractLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addAppArgs` | args: String... | `T` | addAppArgs方法 |  |
| `addFile` | file: String | `T` | addFile方法 |  |
| `addJar` | jar: String | `T` | addJar方法 |  |
| `addPyFile` | file: String | `T` | addPyFile方法 |  |
| `addSparkArg` | arg: String | `T` | addSparkArg方法 |  |
| `addSparkArg` | name: String; value: String | `T` | addSparkArg方法 |  |
| `setAppName` | appName: String | `T` | setAppName方法 |  |
| `setAppResource` | resource: String | `T` | setAppResource方法 |  |
| `setConf` | key: String; value: String | `T` | setConf方法 |  |
| `setDeployMode` | mode: String | `T` | setDeployMode方法 |  |
| `setMainClass` | mainClass: String | `T` | setMainClass方法 |  |
| `setMaster` | master: String | `T` | setMaster方法 |  |
| `setPropertiesFile` | path: String | `T` | setPropertiesFile方法 |  |
| `setRemote` | remote: String | `T` | setRemote方法 |  |
| `setVerbose` | verbose: boolean | `T` | setVerbose方法 |  |

### AbstractMessage
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `body` | 无 | `ManagedBuffer` | body方法 |  |
| `isBodyInFrame` | 无 | `boolean` | isBodyInFrame方法 |  |

### AbstractService
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getName` | 无 | `String` | getName方法 |  |
| `getStartTime` | 无 | `long` | getStartTime方法 |  |

### AmIpFilter
**包路径**: `org.apache.spark.deploy.yarn`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `destroy` | 无 | `void` | destroy方法 |  |
| `doFilter` | req: ServletRequest; resp: ServletResponse; chain: FilterChain | `void` | doFilter方法 |  |
| `findRedirectUrl` | 无 | `String` | findRedirectUrl方法 |  |
| `init` | conf: FilterConfig | `void` | init方法 |  |
| `isValidUrl` | url: String | `boolean` | isValidUrl方法 |  |

### AmIpPrincipal
**包路径**: `org.apache.spark.deploy.yarn`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getName` | 无 | `String` | getName方法 |  |

### AmIpServletRequestWrapper
**包路径**: `org.apache.spark.deploy.yarn`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getRemoteUser` | 无 | `String` | getRemoteUser方法 |  |
| `getUserPrincipal` | 无 | `Principal` | getUserPrincipal方法 |  |
| `isUserInRole` | role: String | `boolean` | isUserInRole方法 |  |

### AnonymousAuthenticationProviderImpl
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Authenticate` | user: String; password: String | `void` | Authenticate方法 |  |

### ApplicationStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `ApplicationStatus` | fromString方法 |  |

### AuthClientBootstrap
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | client: TransportClient; channel: Channel | `void` | doBootstrap方法 |  |

### AuthMethods
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getAuthMethod` | 无 | `String` | getAuthMethod方法 |  |
| `getAuthenticationProvider` | authMethod: AuthMethods | `PasswdAuthenticationProvider` | getAuthenticationProvider方法 |  |
| `getValidAuthMethod` | authMethodStr: String | `AuthMethods` | getValidAuthMethod方法 |  |

### AuthServerBootstrap
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | channel: Channel; rpcHandler: RpcHandler | `RpcHandler` | doBootstrap方法 |  |

### BestEffortLazyVal
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `T` | apply方法 |  |

### BlockPushNonFatalFailure
**包路径**: `org.apache.spark.network.server`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getErrorMsg` | blockId: String; errorCode: ReturnCode | `String` | getErrorMsg方法 |  |
| `getResponse` | 无 | `ByteBuffer` | getResponse方法 |  |
| `getReturnCode` | 无 | `ReturnCode` | getReturnCode方法 |  |
| `getReturnCode` | id: byte | `ReturnCode` | getReturnCode方法 |  |
| `id` | 无 | `byte` | id方法 |  |
| `shouldNotRetryErrorCode` | returnCode: ReturnCode | `boolean` | shouldNotRetryErrorCode方法 |  |

### BlockPushReturnCode
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `BlockPushReturnCode` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### BlockStoreClient
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `diagnoseCorruption` | host: String; port: int; execId: String; shuffleId: int; mapId: long; reduceId: int; checksum: long; algorithm: String | `Cause` | diagnoseCorruption方法 |  |
| `finalizeShuffleMerge` | host: String; port: int; shuffleId: int; shuffleMergeId: int; listener: MergeFinalizerListener | `void` | finalizeShuffleMerge方法 |  |
| `getAppAttemptId` | 无 | `String` | getAppAttemptId方法 |  |
| `getHostLocalDirs` | host: String; port: int; execIds: String[]; hostLocalDirsCompletable: String[]>> | `void` | getHostLocalDirs方法 |  |
| `getMergedBlockMeta` | host: String; port: int; shuffleId: int; shuffleMergeId: int; reduceId: int; listener: MergedBlocksMetaListener | `void` | getMergedBlockMeta方法 |  |
| `onFailure` | t: Throwable | `void` | onFailure方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `pushBlocks` | host: String; port: int; blockIds: String[]; buffers: ManagedBuffer[]; listener: BlockPushingListener | `void` | pushBlocks方法 |  |
| `removeShuffleMerge` | host: String; port: int; shuffleId: int; shuffleMergeId: int | `boolean` | removeShuffleMerge方法 |  |
| `setAppAttemptId` | appAttemptId: String | `void` | setAppAttemptId方法 |  |
| `shuffleMetrics` | 无 | `MetricSet` | shuffleMetrics方法 |  |

### BlockTransferMessage
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromByteBuffer` | msg: ByteBuffer | `BlockTransferMessage` | fromByteBuffer方法 |  |
| `id` | 无 | `byte` | id方法 |  |
| `toByteBuffer` | 无 | `ByteBuffer` | toByteBuffer方法 |  |

### BlocksRemoved
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `BlocksRemoved` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### BloomFilter
**包路径**: `org.apache.spark.util.sketch`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cardinality` | 无 | `long` | cardinality方法 |  |
| `create` | expectedNumItems: long | `BloomFilter` | create方法 |  |
| `create` | expectedNumItems: long; fpp: double | `BloomFilter` | create方法 |  |
| `create` | expectedNumItems: long; numBits: long | `BloomFilter` | create方法 |  |
| `create` | expectedNumItems: long; numBits: long; seed: int | `BloomFilter` | create方法 |  |
| `create` | version: Version; expectedNumItems: long; numBits: long; seed: int | `BloomFilter` | create方法 |  |
| `optimalNumOfBits` | n: long; p: double | `long` | optimalNumOfBits方法 |  |
| `optimalNumOfBits` | expectedNumItems: long; maxNumItems: long; maxNumOfBits: long | `long` | optimalNumOfBits方法 |  |
| `readFrom` | in: InputStream | `BloomFilter` | readFrom方法 |  |
| `readFrom` | bytes: byte[] | `BloomFilter` | readFrom方法 |  |

### BreakableService
**包路径**: `org.apache.hive.service`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getCount` | state: STATE | `int` | getCount方法 |  |
| `init` | conf: HiveConf | `void` | init方法 |  |
| `setFailOnInit` | failOnInit: boolean | `void` | setFailOnInit方法 |  |
| `setFailOnStart` | failOnStart: boolean | `void` | setFailOnStart方法 |  |
| `setFailOnStop` | failOnStop: boolean | `void` | setFailOnStop方法 |  |
| `start` | 无 | `void` | start方法 |  |
| `stop` | 无 | `void` | stop方法 |  |

### ByteArrayMethods
**包路径**: `org.apache.spark.unsafe.array`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `arrayEquals` | leftBase: Object; leftOffset: long; rightBase: Object; rightOffset: long; length: final long | `boolean` | arrayEquals方法 |  |
| `contains` | arr: byte[]; sub: byte[] | `boolean` | contains方法 |  |
| `endsWith` | array: byte[]; target: byte[] | `boolean` | endsWith方法 |  |
| `matchAt` | arr: byte[]; sub: byte[]; pos: int | `boolean` | matchAt方法 |  |
| `nextPowerOf2` | num: long | `long` | nextPowerOf2方法 |  |
| `roundNumberOfBytesToNearestWord` | numBytes: int | `int` | roundNumberOfBytesToNearestWord方法 |  |
| `roundNumberOfBytesToNearestWord` | numBytes: long | `long` | roundNumberOfBytesToNearestWord方法 |  |
| `startsWith` | array: byte[]; target: byte[] | `boolean` | startsWith方法 |  |

### ByteArrayReadableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `feedData` | buf: ByteBuf | `void` | feedData方法 |  |
| `isOpen` | 无 | `boolean` | isOpen方法 |  |
| `read` | dst: ByteBuffer | `int` | read方法 |  |

### ByteArrayWritableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `isOpen` | 无 | `boolean` | isOpen方法 |  |
| `length` | 无 | `int` | length方法 |  |
| `reset` | 无 | `void` | reset方法 |  |
| `write` | src: ByteBuffer | `int` | write方法 |  |

### ByteBufferWriteableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `isOpen` | 无 | `boolean` | isOpen方法 |  |
| `write` | src: ByteBuffer | `int` | write方法 |  |

### ByteUnit
**包路径**: `org.apache.spark.network.util`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertFrom` | d: long; u: ByteUnit | `long` | convertFrom方法 |  |
| `convertTo` | d: long; u: ByteUnit | `long` | convertTo方法 |  |
| `toBytes` | d: long | `long` | toBytes方法 |  |
| `toGiB` | d: long | `long` | toGiB方法 |  |
| `toKiB` | d: long | `long` | toKiB方法 |  |
| `toMiB` | d: long | `long` | toMiB方法 |  |
| `toPiB` | d: long | `long` | toPiB方法 |  |
| `toTiB` | d: long | `long` | toTiB方法 |  |

### CLIService
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | sessionHandle: SessionHandle; authFactory: HiveAuthFactory; tokenStr: String | `void` | cancelDelegationToken方法 |  |
| `cancelOperation` | opHandle: OperationHandle | `void` | cancelOperation方法 |  |
| `closeOperation` | opHandle: OperationHandle | `void` | closeOperation方法 |  |
| `closeSession` | sessionHandle: SessionHandle | `void` | closeSession方法 |  |
| `executeStatement` | sessionHandle: SessionHandle; statement: String; confOverlay: String> | `OperationHandle` | executeStatement方法 |  |
| `executeStatement` | sessionHandle: SessionHandle; statement: String; confOverlay: String>; queryTimeout: long | `OperationHandle` | executeStatement方法 |  |
| `executeStatementAsync` | sessionHandle: SessionHandle; statement: String; confOverlay: String> | `OperationHandle` | executeStatementAsync方法 |  |
| `executeStatementAsync` | sessionHandle: SessionHandle; statement: String; confOverlay: String>; queryTimeout: long | `OperationHandle` | executeStatementAsync方法 |  |
| `fetchResults` | opHandle: OperationHandle | `TRowSet` | fetchResults方法 |  |
| `fetchResults` | opHandle: OperationHandle; orientation: FetchOrientation; maxRows: long; fetchType: FetchType | `TRowSet` | fetchResults方法 |  |
| `getCatalogs` | sessionHandle: SessionHandle | `OperationHandle` | getCatalogs方法 |  |
| `getColumns` | sessionHandle: SessionHandle; catalogName: String; schemaName: String; tableName: String; columnName: String | `OperationHandle` | getColumns方法 |  |
| `getCrossReference` | sessionHandle: SessionHandle; primaryCatalog: String; primarySchema: String; primaryTable: String; foreignCatalog: String; foreignSchema: String; foreignTable: String | `OperationHandle` | getCrossReference方法 |  |
| `getDelegationToken` | sessionHandle: SessionHandle; authFactory: HiveAuthFactory; owner: String; renewer: String | `String` | getDelegationToken方法 |  |
| `getFunctions` | sessionHandle: SessionHandle; catalogName: String; schemaName: String; functionName: String | `OperationHandle` | getFunctions方法 |  |
| `getHttpUGI` | 无 | `UserGroupInformation` | getHttpUGI方法 |  |
| `getInfo` | sessionHandle: SessionHandle; getInfoType: GetInfoType | `GetInfoValue` | getInfo方法 |  |
| `getOperationStatus` | opHandle: OperationHandle | `OperationStatus` | getOperationStatus方法 |  |
| `getPrimaryKeys` | sessionHandle: SessionHandle; catalog: String; schema: String; table: String | `OperationHandle` | getPrimaryKeys方法 |  |
| `getQueryId` | opHandle: TOperationHandle | `String` | getQueryId方法 |  |
| `getResultSetMetadata` | opHandle: OperationHandle | `TTableSchema` | getResultSetMetadata方法 |  |
| `getSchemas` | sessionHandle: SessionHandle; catalogName: String; schemaName: String | `OperationHandle` | getSchemas方法 |  |
| `getServiceUGI` | 无 | `UserGroupInformation` | getServiceUGI方法 |  |
| `getSessionConf` | sessionHandle: SessionHandle | `HiveConf` | getSessionConf方法 |  |
| `getSessionManager` | 无 | `SessionManager` | getSessionManager方法 |  |
| `getTableTypes` | sessionHandle: SessionHandle | `OperationHandle` | getTableTypes方法 |  |
| `getTables` | sessionHandle: SessionHandle; catalogName: String; schemaName: String; tableName: String; tableTypes: List<String> | `OperationHandle` | getTables方法 |  |
| `getTypeInfo` | sessionHandle: SessionHandle | `OperationHandle` | getTypeInfo方法 |  |
| `openSession` | protocol: TProtocolVersion; username: String; password: String; configuration: String> | `SessionHandle` | openSession方法 |  |
| `openSession` | protocol: TProtocolVersion; username: String; password: String; ipAddress: String; configuration: String> | `SessionHandle` | openSession方法 |  |
| `openSession` | username: String; password: String; configuration: String> | `SessionHandle` | openSession方法 |  |
| `openSessionWithImpersonation` | protocol: TProtocolVersion; username: String; password: String; configuration: String>; delegationToken: String | `SessionHandle` | openSessionWithImpersonation方法 |  |
| `openSessionWithImpersonation` | protocol: TProtocolVersion; username: String; password: String; ipAddress: String; configuration: String>; delegationToken: String | `SessionHandle` | openSessionWithImpersonation方法 |  |
| `openSessionWithImpersonation` | username: String; password: String; configuration: String>; delegationToken: String | `SessionHandle` | openSessionWithImpersonation方法 |  |
| `renewDelegationToken` | sessionHandle: SessionHandle; authFactory: HiveAuthFactory; tokenStr: String | `void` | renewDelegationToken方法 |  |

### CLIServiceClient
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fetchResults` | opHandle: OperationHandle | `TRowSet` | fetchResults方法 |  |
| `openSession` | username: String; password: String | `SessionHandle` | openSession方法 |  |

### CLIServiceUtils
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `patternToRegex` | pattern: String | `String` | patternToRegex方法 |  |

### ChildFirstURLClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getResource` | name: String | `URL` | getResource方法 |  |
| `getResources` | name: String | `Enumeration&lt;URL&gt;` | getResources方法 |  |
| `loadClass` | name: String; resolve: boolean | `Class&lt;?&gt;` | loadClass方法 |  |

### ChunkFetchRequestHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `exceptionCaught` | ctx: ChannelHandlerContext; cause: Throwable | `void` | exceptionCaught方法 |  |
| `processFetchRequest` | channel: final Channel; msg: final ChunkFetchRequest | `void` | processFetchRequest方法 |  |

### ClassicTableTypeMapping
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeNames` | 无 | `Set&lt;String&gt;` | getTableTypeNames方法 |  |
| `mapToClientType` | hiveTypeName: String | `String` | mapToClientType方法 |  |

### CodePointIteratorType
**包路径**: `org.apache.spark.unsafe.types`
**方法数量**: 91

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `binaryCompare` | other: final UTF8String | `int` | binaryCompare方法 |  |
| `binaryEquals` | other: final UTF8String | `boolean` | binaryEquals方法 |  |
| `blankString` | length: int | `UTF8String` | blankString方法 |  |
| `bytePosToChar` | bytePos: int | `int` | bytePosToChar方法 |  |
| `charPosToByte` | charPos: int | `int` | charPosToByte方法 |  |
| `clone` | 无 | `UTF8String` | clone方法 |  |
| `codePointFrom` | byteIndex: int | `int` | codePointFrom方法 |  |
| `codePointIterator` | 无 | `Iterator&lt;Integer&gt;` | codePointIterator方法 |  |
| `codePointIterator` | iteratorMode: CodePointIteratorType | `Iterator&lt;Integer&gt;` | codePointIterator方法 |  |
| `compareTo` | other: @Nonnull final UTF8String | `int` | compareTo方法 |  |
| `concat` | inputs: UTF8String... | `UTF8String` | concat方法 |  |
| `concatWs` | separator: UTF8String; inputs: UTF8String... | `UTF8String` | concatWs方法 |  |
| `contains` | substring: final UTF8String | `boolean` | contains方法 |  |
| `copy` | 无 | `UTF8String` | copy方法 |  |
| `copyUTF8String` | start: int; end: int | `UTF8String` | copyUTF8String方法 |  |
| `endsWith` | suffix: final UTF8String | `boolean` | endsWith方法 |  |
| `find` | str: UTF8String; start: int | `int` | find方法 |  |
| `findInSet` | match: UTF8String | `int` | findInSet方法 |  |
| `fromAddress` | base: Object; offset: long; numBytes: int | `UTF8String` | fromAddress方法 |  |
| `fromBytes` | bytes: byte[] | `UTF8String` | fromBytes方法 |  |
| `fromBytes` | bytes: byte[]; offset: int; numBytes: int | `UTF8String` | fromBytes方法 |  |
| `fromString` | str: String | `UTF8String` | fromString方法 |  |
| `getBaseObject` | 无 | `Object` | getBaseObject方法 |  |
| `getBaseOffset` | 无 | `long` | getBaseOffset方法 |  |
| `getByte` | byteIndex: int | `byte` | getByte方法 |  |
| `getByteBuffer` | 无 | `ByteBuffer` | getByteBuffer方法 |  |
| `getChar` | charIndex: int | `int` | getChar方法 |  |
| `getPrefix` | 无 | `long` | getPrefix方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `indexOf` | v: UTF8String; start: int | `int` | indexOf方法 |  |
| `indexOfEmpty` | start: int | `int` | indexOfEmpty方法 |  |
| `isFullAscii` | 无 | `boolean` | isFullAscii方法 |  |
| `isValid` | 无 | `boolean` | isValid方法 |  |
| `isWhitespaceOrISOControl` | codePoint: int | `boolean` | isWhitespaceOrISOControl方法 |  |
| `levenshteinDistance` | other: UTF8String | `int` | levenshteinDistance方法 |  |
| `levenshteinDistance` | other: UTF8String; threshold: int | `int` | levenshteinDistance方法 |  |
| `lpad` | len: int; pad: UTF8String | `UTF8String` | lpad方法 |  |
| `makeValid` | 无 | `UTF8String` | makeValid方法 |  |
| `matchAt` | s: final UTF8String; pos: int | `boolean` | matchAt方法 |  |
| `next` | 无 | `Integer` | next方法 |  |
| `next` | 无 | `Integer` | next方法 |  |
| `numBytes` | 无 | `int` | numBytes方法 |  |
| `numBytesForFirstByte` | b: final byte | `int` | numBytesForFirstByte方法 |  |
| `numChars` | 无 | `int` | numChars方法 |  |
| `read` | kryo: Kryo; in: Input | `void` | read方法 |  |
| `readExternal` | in: ObjectInput | `void` | readExternal方法 |  |
| `repeat` | times: int | `UTF8String` | repeat方法 |  |
| `replace` | search: UTF8String; replace: UTF8String | `UTF8String` | replace方法 |  |
| `reverse` | 无 | `UTF8String` | reverse方法 |  |
| `reverseCodePointIterator` | 无 | `Iterator&lt;Integer&gt;` | reverseCodePointIterator方法 |  |
| `reverseCodePointIterator` | iteratorMode: CodePointIteratorType | `Iterator&lt;Integer&gt;` | reverseCodePointIterator方法 |  |
| `rfind` | str: UTF8String; start: int | `int` | rfind方法 |  |
| `rpad` | len: int; pad: UTF8String | `UTF8String` | rpad方法 |  |
| `semanticCompare` | other: final UTF8String; collationId: int | `int` | semanticCompare方法 |  |
| `semanticEquals` | other: final UTF8String; collationId: int | `boolean` | semanticEquals方法 |  |
| `soundex` | 无 | `UTF8String` | soundex方法 |  |
| `startsWith` | prefix: final UTF8String | `boolean` | startsWith方法 |  |
| `subStringIndex` | delim: UTF8String; count: int | `UTF8String` | subStringIndex方法 |  |
| `substring` | start: final int; until: final int | `UTF8String` | substring方法 |  |
| `substringSQL` | pos: int; length: int | `UTF8String` | substringSQL方法 |  |
| `toBinaryString` | val: long | `UTF8String` | toBinaryString方法 |  |
| `toByte` | intWrapper: IntWrapper | `boolean` | toByte方法 |  |
| `toByteExact` | 无 | `byte` | toByteExact方法 |  |
| `toInt` | intWrapper: IntWrapper | `boolean` | toInt方法 |  |
| `toIntExact` | 无 | `int` | toIntExact方法 |  |
| `toLong` | toLongResult: LongWrapper | `boolean` | toLong方法 |  |
| `toLongExact` | 无 | `long` | toLongExact方法 |  |
| `toLowerCase` | 无 | `UTF8String` | toLowerCase方法 |  |
| `toLowerCaseAscii` | 无 | `UTF8String` | toLowerCaseAscii方法 |  |
| `toShort` | intWrapper: IntWrapper | `boolean` | toShort方法 |  |
| `toShortExact` | 无 | `short` | toShortExact方法 |  |
| `toTitleCase` | 无 | `UTF8String` | toTitleCase方法 |  |
| `toTitleCaseICU` | 无 | `UTF8String` | toTitleCaseICU方法 |  |
| `toUpperCase` | 无 | `UTF8String` | toUpperCase方法 |  |
| `toUpperCaseAscii` | 无 | `UTF8String` | toUpperCaseAscii方法 |  |
| `toValidString` | 无 | `String` | toValidString方法 |  |
| `translate` | dict: String> | `UTF8String` | translate方法 |  |
| `trim` | 无 | `UTF8String` | trim方法 |  |
| `trim` | trimString: UTF8String | `UTF8String` | trim方法 |  |
| `trimAll` | 无 | `UTF8String` | trimAll方法 |  |
| `trimLeft` | 无 | `UTF8String` | trimLeft方法 |  |
| `trimLeft` | trimString: UTF8String | `UTF8String` | trimLeft方法 |  |
| `trimRight` | 无 | `UTF8String` | trimRight方法 |  |
| `trimRight` | trimString: UTF8String | `UTF8String` | trimRight方法 |  |
| `trimTrailingSpaces` | numSpaces: int | `UTF8String` | trimTrailingSpaces方法 |  |
| `write` | kryo: Kryo; out: Output | `void` | write方法 |  |
| `writeExternal` | out: ObjectOutput | `void` | writeExternal方法 |  |
| `writeTo` | buffer: ByteBuffer | `void` | writeTo方法 |  |
| `writeTo` | out: OutputStream | `void` | writeTo方法 |  |
| `writeToMemory` | target: Object; targetOffset: long | `void` | writeToMemory方法 |  |

### ColumnBasedSet
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addRow` | fields: Object[] | `ColumnBasedSet` | addRow方法 |  |
| `extractSubset` | maxRows: int | `ColumnBasedSet` | extractSubset方法 |  |
| `getColumns` | 无 | `List&lt;ColumnBuffer&gt;` | getColumns方法 |  |
| `getStartOffset` | 无 | `long` | getStartOffset方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `iterator` | 无 | `Iterator&lt;Object[]&gt;` | iterator方法 |  |
| `numColumns` | 无 | `int` | numColumns方法 |  |
| `numRows` | 无 | `int` | numRows方法 |  |
| `setStartOffset` | startOffset: long | `void` | setStartOffset方法 |  |
| `toTRowSet` | 无 | `TRowSet` | toTRowSet方法 |  |

### ColumnDescriptor
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getComment` | 无 | `String` | getComment方法 |  |
| `getName` | 无 | `String` | getName方法 |  |
| `getOrdinalPosition` | 无 | `int` | getOrdinalPosition方法 |  |
| `getType` | 无 | `Type` | getType方法 |  |
| `getTypeDescriptor` | 无 | `TypeDescriptor` | getTypeDescriptor方法 |  |
| `getTypeName` | 无 | `String` | getTypeName方法 |  |
| `isPrimitive` | 无 | `boolean` | isPrimitive方法 |  |
| `newPrimitiveColumnDescriptor` | name: String; comment: String; type: Type; position: int | `ColumnDescriptor` | newPrimitiveColumnDescriptor方法 |  |
| `toTColumnDesc` | 无 | `TColumnDesc` | toTColumnDesc方法 |  |

### ColumnValue
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toColumnValue` | value: TColumnValue | `Object` | toColumnValue方法 |  |
| `toTColumnValue` | typeDescriptor: TypeDescriptor; value: Object | `TColumnValue` | toTColumnValue方法 |  |

### CompositeService
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getServices` | 无 | `Collection&lt;Service&gt;` | getServices方法 |  |
| `run` | 无 | `void` | run方法 |  |

### ConfigProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String; defaultValue: String | `String` | get方法 |  |
| `getBoolean` | name: String; defaultValue: boolean | `boolean` | getBoolean方法 |  |
| `getDouble` | name: String; defaultValue: double | `double` | getDouble方法 |  |
| `getInt` | name: String; defaultValue: int | `int` | getInt方法 |  |
| `getLong` | name: String; defaultValue: long | `long` | getLong方法 |  |

### CookieSigner
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `signCookie` | str: String | `String` | signCookie方法 |  |
| `verifyAndExtract` | signedStr: String | `String` | verifyAndExtract方法 |  |

### CorruptionCause
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `CorruptionCause` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### CountMinSketch
**包路径**: `org.apache.spark.util.sketch`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | depth: int; width: int; seed: int | `CountMinSketch` | create方法 |  |
| `create` | eps: double; confidence: double; seed: int | `CountMinSketch` | create方法 |  |
| `readFrom` | in: InputStream | `CountMinSketch` | readFrom方法 |  |
| `readFrom` | bytes: byte[] | `CountMinSketch` | readFrom方法 |  |

### CryptoUtils
**包路径**: `org.apache.spark.network.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toCryptoConf` | prefix: String; conf: String>> | `Properties` | toCryptoConf方法 |  |

### CtrTransportCipher
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addToChannel` | ch: Channel | `void` | addToChannel方法 |  |
| `channelRead` | ctx: ChannelHandlerContext; data: Object | `void` | channelRead方法 |  |
| `close` | ctx: ChannelHandlerContext; promise: ChannelPromise | `void` | close方法 |  |
| `count` | 无 | `long` | count方法 |  |
| `getKeyId` | 无 | `String` | getKeyId方法 |  |
| `handlerRemoved` | ctx: ChannelHandlerContext | `void` | handlerRemoved方法 |  |
| `position` | 无 | `long` | position方法 |  |
| `release` | decrement: int | `boolean` | release方法 |  |
| `retain` | increment: int | `EncryptedMessage` | retain方法 |  |
| `touch` | o: Object | `EncryptedMessage` | touch方法 |  |
| `transferTo` | target: WritableByteChannel; position: long | `long` | transferTo方法 |  |
| `transferred` | 无 | `long` | transferred方法 |  |
| `write` | ctx: ChannelHandlerContext; msg: Object; promise: ChannelPromise | `void` | write方法 |  |

### CustomAuthenticationProviderImpl
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Authenticate` | user: String; password: String | `void` | Authenticate方法 |  |

### CustomLogKeys
**包路径**: `org.apache.spark.internal`
**方法数量**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `debug` | msg: String | `void` | debug方法 |  |
| `debug` | format: String; arg: Object | `void` | debug方法 |  |
| `debug` | format: String; arg1: Object; arg2: Object | `void` | debug方法 |  |
| `debug` | format: String; arguments: Object... | `void` | debug方法 |  |
| `debug` | msg: String; throwable: Throwable | `void` | debug方法 |  |
| `error` | msg: String | `void` | error方法 |  |
| `error` | msg: String; throwable: Throwable | `void` | error方法 |  |
| `error` | msg: String; mdcs: MDC... | `void` | error方法 |  |
| `error` | msg: String; throwable: Throwable; mdcs: MDC... | `void` | error方法 |  |
| `getSlf4jLogger` | 无 | `Logger` | getSlf4jLogger方法 |  |
| `info` | msg: String | `void` | info方法 |  |
| `info` | msg: String; throwable: Throwable | `void` | info方法 |  |
| `info` | msg: String; mdcs: MDC... | `void` | info方法 |  |
| `info` | msg: String; throwable: Throwable; mdcs: MDC... | `void` | info方法 |  |
| `isDebugEnabled` | 无 | `boolean` | isDebugEnabled方法 |  |
| `isErrorEnabled` | 无 | `boolean` | isErrorEnabled方法 |  |
| `isInfoEnabled` | 无 | `boolean` | isInfoEnabled方法 |  |
| `isTraceEnabled` | 无 | `boolean` | isTraceEnabled方法 |  |
| `isWarnEnabled` | 无 | `boolean` | isWarnEnabled方法 |  |
| `trace` | msg: String | `void` | trace方法 |  |
| `trace` | format: String; arg: Object | `void` | trace方法 |  |
| `trace` | format: String; arg1: Object; arg2: Object | `void` | trace方法 |  |
| `trace` | format: String; arguments: Object... | `void` | trace方法 |  |
| `trace` | msg: String; throwable: Throwable | `void` | trace方法 |  |
| `warn` | msg: String | `void` | warn方法 |  |
| `warn` | msg: String; throwable: Throwable | `void` | warn方法 |  |
| `warn` | msg: String; mdcs: MDC... | `void` | warn方法 |  |
| `warn` | msg: String; throwable: Throwable; mdcs: MDC... | `void` | warn方法 |  |

### DBBackend
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `byName` | value: String | `DBBackend` | byName方法 |  |
| `fileName` | prefix: String | `String` | fileName方法 |  |

### DBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initDB` | dbBackend: DBBackend; dbFile: File; version: StoreVersion; mapper: ObjectMapper | `DB` | initDB方法 |  |
| `initDB` | dbBackend: DBBackend; file: File | `DB` | initDB方法 |  |

### DelegateSymlinkTextInputFormat
**包路径**: `org.apache.hadoop.hive.ql.io`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `configure` | job: JobConf | `void` | configure方法 |  |
| `getContentSummary` | p: Path; job: JobConf | `ContentSummary` | getContentSummary方法 |  |
| `getRecordReader` | split: InputSplit; job: JobConf; reporter: Reporter | `RecordReader&lt;LongWritable, Text&gt;` | getRecordReader方法 |  |
| `getTargetPath` | 无 | `Path` | getTargetPath方法 |  |
| `readFields` | in: DataInput | `void` | readFields方法 |  |
| `write` | out: DataOutput | `void` | write方法 |  |

### DiagnoseCorruption
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `DiagnoseCorruption` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### Encoders
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `String` | decode方法 |  |
| `decode` | buf: ByteBuf | `RoaringBitmap` | decode方法 |  |
| `encode` | buf: ByteBuf; s: String | `void` | encode方法 |  |
| `encode` | buf: ByteBuf; b: RoaringBitmap | `void` | encode方法 |  |
| `encode` | buf: ByteBuf; arr: byte[] | `void` | encode方法 |  |
| `encode` | buf: ByteBuf; strings: String[] | `void` | encode方法 |  |
| `encode` | buf: ByteBuf; ints: int[] | `void` | encode方法 |  |
| `encode` | buf: ByteBuf; longs: long[] | `void` | encode方法 |  |
| `encode` | buf: ByteBuf; bitmaps: RoaringBitmap[] | `void` | encode方法 |  |
| `encodedLength` | s: String | `int` | encodedLength方法 |  |
| `encodedLength` | b: RoaringBitmap | `int` | encodedLength方法 |  |
| `encodedLength` | arr: byte[] | `int` | encodedLength方法 |  |
| `encodedLength` | strings: String[] | `int` | encodedLength方法 |  |
| `encodedLength` | ints: int[] | `int` | encodedLength方法 |  |
| `encodedLength` | longs: long[] | `int` | encodedLength方法 |  |
| `encodedLength` | bitmaps: RoaringBitmap[] | `int` | encodedLength方法 |  |

### EncryptedMessageWithHeader
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `isEndOfInput` | 无 | `boolean` | isEndOfInput方法 |  |
| `length` | 无 | `long` | length方法 |  |
| `progress` | 无 | `long` | progress方法 |  |
| `readChunk` | ctx: ChannelHandlerContext | `ByteBuf` | readChunk方法 |  |
| `readChunk` | allocator: ByteBufAllocator | `ByteBuf` | readChunk方法 |  |

### ErrorHandler
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `shouldLogError` | t: Throwable | `boolean` | shouldLogError方法 |  |
| `shouldLogError` | t: Throwable | `boolean` | shouldLogError方法 |  |
| `shouldRetryError` | t: Throwable | `boolean` | shouldRetryError方法 |  |
| `shouldRetryError` | t: Throwable | `boolean` | shouldRetryError方法 |  |

### ExecuteStatementOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStatement` | 无 | `String` | getStatement方法 |  |

### ExecutorDiskUtils
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getFilePath` | localDirs: String[]; subDirsPerLocalDir: int; filename: String | `String` | getFilePath方法 |  |

### ExecutorShuffleInfo
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `ExecutorShuffleInfo` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### ExternalBlockHandler
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String; cleanupLocalDirs: boolean | `void` | applicationRemoved方法 |  |
| `channelActive` | client: TransportClient | `void` | channelActive方法 |  |
| `channelInactive` | client: TransportClient | `void` | channelInactive方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `exceptionCaught` | cause: Throwable; client: TransportClient | `void` | exceptionCaught方法 |  |
| `executorRemoved` | executorId: String; appId: String | `void` | executorRemoved方法 |  |
| `getAllMetrics` | 无 | `MetricSet` | getAllMetrics方法 |  |
| `getBlockResolver` | 无 | `ExternalShuffleBlockResolver` | getBlockResolver方法 |  |
| `getMergedBlockMetaReqHandler` | 无 | `MergedBlockMetaReqHandler` | getMergedBlockMetaReqHandler方法 |  |
| `getMetrics` | 无 | `Map&lt;String, Metric&gt;` | getMetrics方法 |  |
| `getStreamManager` | 无 | `StreamManager` | getStreamManager方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `next` | 无 | `ManagedBuffer` | next方法 |  |
| `next` | 无 | `ManagedBuffer` | next方法 |  |
| `next` | 无 | `ManagedBuffer` | next方法 |  |
| `receive` | client: TransportClient; message: ByteBuffer; callback: RpcResponseCallback | `void` | receive方法 |  |
| `receiveMergeBlockMetaReq` | client: TransportClient; metaRequest: MergedBlockMetaRequest; callback: MergedBlockMetaResponseCallback | `void` | receiveMergeBlockMetaReq方法 |  |
| `receiveStream` | client: TransportClient; messageHeader: ByteBuffer; callback: RpcResponseCallback | `StreamCallbackWithID` | receiveStream方法 |  |

### ExternalBlockStoreClient
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `fetchBlocks` | host: String; port: int; execId: String; blockIds: String[]; listener: BlockFetchingListener; downloadFileManager: DownloadFileManager | `void` | fetchBlocks方法 |  |
| `finalizeShuffleMerge` | host: String; port: int; shuffleId: int; shuffleMergeId: int; listener: MergeFinalizerListener | `void` | finalizeShuffleMerge方法 |  |
| `getMergedBlockMeta` | host: String; port: int; shuffleId: int; shuffleMergeId: int; reduceId: int; listener: MergedBlocksMetaListener | `void` | getMergedBlockMeta方法 |  |
| `init` | appId: String | `void` | init方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `onSuccess` | numChunks: int; buffer: ManagedBuffer | `void` | onSuccess方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `pushBlocks` | host: String; port: int; blockIds: String[]; buffers: ManagedBuffer[]; listener: BlockPushingListener | `void` | pushBlocks方法 |  |
| `registerWithShuffleServer` | host: String; port: int; execId: String; executorInfo: ExecutorShuffleInfo | `void` | registerWithShuffleServer方法 |  |
| `removeBlocks` | host: String; port: int; execId: String; blockIds: String[] | `Future&lt;Integer&gt;` | removeBlocks方法 |  |
| `removeShuffleMerge` | host: String; port: int; shuffleId: int; shuffleMergeId: int | `boolean` | removeShuffleMerge方法 |  |
| `setAppAttemptId` | appAttemptId: String | `void` | setAppAttemptId方法 |  |
| `shuffleMetrics` | 无 | `MetricSet` | shuffleMetrics方法 |  |

### ExternalShuffleBlockResolver
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String; cleanupLocalDirs: boolean | `void` | applicationRemoved方法 |  |
| `diagnoseShuffleBlockCorruption` | appId: String; execId: String; shuffleId: int; mapId: long; reduceId: int; checksumByReader: long; algorithm: String | `Cause` | diagnoseShuffleBlockCorruption方法 |  |
| `executorRemoved` | executorId: String; appId: String | `void` | executorRemoved方法 |  |
| `getBlockData` | appId: String; execId: String; shuffleId: int; mapId: long; reduceId: int | `ManagedBuffer` | getBlockData方法 |  |
| `getContinuousBlocksData` | appId: String; execId: String; shuffleId: int; mapId: long; startReduceId: int; endReduceId: int | `ManagedBuffer` | getContinuousBlocksData方法 |  |
| `getDiskPersistedRddBlockData` | executor: ExecutorShuffleInfo; rddId: int; splitIndex: int | `ManagedBuffer` | getDiskPersistedRddBlockData方法 |  |
| `getLocalDirs` | appId: String; execIds: Set<String> | `Map&lt;String, String[]&gt;` | getLocalDirs方法 |  |
| `getRddBlockData` | appId: String; execId: String; rddId: int; splitIndex: int | `ManagedBuffer` | getRddBlockData方法 |  |
| `getRegisteredExecutorsSize` | 无 | `int` | getRegisteredExecutorsSize方法 |  |
| `load` | filePath: String | `ShuffleIndexInformation` | load方法 |  |
| `registerExecutor` | appId: String; execId: String; executorInfo: ExecutorShuffleInfo | `void` | registerExecutor方法 |  |
| `removeBlocks` | appId: String; execId: String; blockIds: String[] | `int` | removeBlocks方法 |  |

### FetchOrientation
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getFetchOrientation` | tFetchOrientation: TFetchOrientation | `FetchOrientation` | getFetchOrientation方法 |  |
| `toTFetchOrientation` | 无 | `TFetchOrientation` | toTFetchOrientation方法 |  |

### FetchShuffleBlockChunks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FetchShuffleBlockChunks` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |
| `getNumBlocks` | 无 | `int` | getNumBlocks方法 |  |

### FetchShuffleBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FetchShuffleBlocks` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |
| `getNumBlocks` | 无 | `int` | getNumBlocks方法 |  |

### FetchType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getFetchType` | tFetchType: short | `FetchType` | getFetchType方法 |  |
| `toTFetchType` | 无 | `short` | toTFetchType方法 |  |

### FilterService
**包路径**: `org.apache.hive.service`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getHiveConf` | 无 | `HiveConf` | getHiveConf方法 |  |
| `getName` | 无 | `String` | getName方法 |  |
| `getStartTime` | 无 | `long` | getStartTime方法 |  |
| `init` | config: HiveConf | `void` | init方法 |  |
| `register` | listener: ServiceStateChangeListener | `void` | register方法 |  |
| `start` | 无 | `void` | start方法 |  |
| `stop` | 无 | `void` | stop方法 |  |
| `unregister` | listener: ServiceStateChangeListener | `void` | unregister方法 |  |

### FinalizeShuffleMerge
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FinalizeShuffleMerge` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### GangliaReporter
**包路径**: `com.codahale.metrics.ganglia`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | gmetric: GMetric | `GangliaReporter` | build方法 |  |
| `build` | gmetrics: GMetric... | `GangliaReporter` | build方法 |  |
| `convertDurationsTo` | durationUnit: TimeUnit | `Builder` | convertDurationsTo方法 |  |
| `convertRatesTo` | rateUnit: TimeUnit | `Builder` | convertRatesTo方法 |  |
| `disabledMetricAttributes` | disabledMetricAttributes: Set<MetricAttribute> | `Builder` | disabledMetricAttributes方法 |  |
| `filter` | filter: MetricFilter | `Builder` | filter方法 |  |
| `forRegistry` | registry: MetricRegistry | `Builder` | forRegistry方法 |  |
| `prefixedWith` | prefix: String | `Builder` | prefixedWith方法 |  |
| `report` | gauges: Gauge>; counters: Counter>; histograms: Histogram>; meters: Meter>; timers: Timer> | `void` | report方法 |  |
| `scheduleOn` | executor: ScheduledExecutorService | `Builder` | scheduleOn方法 |  |
| `shutdownExecutorOnStop` | shutdownExecutorOnStop: boolean | `Builder` | shutdownExecutorOnStop方法 |  |
| `withDMax` | dMax: int | `Builder` | withDMax方法 |  |
| `withTMax` | tMax: int | `Builder` | withTMax方法 |  |

### GcmTransportCipher
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addToChannel` | ch: Channel | `void` | addToChannel方法 |  |
| `channelRead` | ctx: ChannelHandlerContext; ciphertextMessage: Object | `void` | channelRead方法 |  |
| `count` | 无 | `long` | count方法 |  |
| `getKeyId` | 无 | `String` | getKeyId方法 |  |
| `position` | 无 | `long` | position方法 |  |
| `release` | decrement: int | `boolean` | release方法 |  |
| `retain` | increment: int | `GcmEncryptedMessage` | retain方法 |  |
| `touch` | o: Object | `GcmEncryptedMessage` | touch方法 |  |
| `transferTo` | target: WritableByteChannel; position: long | `long` | transferTo方法 |  |
| `transferred` | 无 | `long` | transferred方法 |  |
| `write` | ctx: ChannelHandlerContext; msg: Object; promise: ChannelPromise | `void` | write方法 |  |

### GetCatalogsOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetColumnsOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetCrossReferenceOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetFunctionsOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetInfoType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getGetInfoType` | tGetInfoType: TGetInfoType | `GetInfoType` | getGetInfoType方法 |  |
| `toTGetInfoType` | 无 | `TGetInfoType` | toTGetInfoType方法 |  |

### GetInfoValue
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getIntValue` | 无 | `int` | getIntValue方法 |  |
| `getLongValue` | 无 | `long` | getLongValue方法 |  |
| `getShortValue` | 无 | `short` | getShortValue方法 |  |
| `getStringValue` | 无 | `String` | getStringValue方法 |  |
| `toTGetInfoValue` | 无 | `TGetInfoValue` | toTGetInfoValue方法 |  |

### GetLocalDirsForExecutors
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `GetLocalDirsForExecutors` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### GetPrimaryKeysOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetSchemasOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetTableTypesOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetTablesOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### GetTypeInfoOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation; maxRows: long | `TRowSet` | getNextRowSet方法 |  |
| `getResultSetSchema` | 无 | `TTableSchema` | getResultSetSchema方法 |  |
| `runInternal` | 无 | `void` | runInternal方法 |  |

### HadoopConfigProvider
**包路径**: `org.apache.spark.network.yarn.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String | `String` | get方法 |  |
| `get` | name: String; defaultValue: String | `String` | get方法 |  |

### Handle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getHandleIdentifier` | 无 | `HandleIdentifier` | getHandleIdentifier方法 |  |

### HandleIdentifier
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getPublicId` | 无 | `UUID` | getPublicId方法 |  |
| `getSecretId` | 无 | `UUID` | getSecretId方法 |  |
| `toTHandleIdentifier` | 无 | `THandleIdentifier` | toTHandleIdentifier方法 |  |

### HashMapGrowthStrategy
**包路径**: `org.apache.spark.unsafe.map`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `nextCapacity` | currentCapacity: int | `int` | nextCapacity方法 |  |

### HeapMemoryAllocator
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `allocate` | size: long | `MemoryBlock` | allocate方法 |  |
| `free` | memory: MemoryBlock | `void` | free方法 |  |

### HiveAuthFactory
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | delegationToken: String | `void` | cancelDelegationToken方法 |  |
| `getAuthName` | 无 | `String` | getAuthName方法 |  |
| `getAuthProcFactory` | service: ThriftCLIService | `TProcessorFactory` | getAuthProcFactory方法 |  |
| `getAuthTransFactory` | 无 | `TTransportFactory` | getAuthTransFactory方法 |  |
| `getDelegationToken` | owner: String; renewer: String; remoteAddr: String | `String` | getDelegationToken方法 |  |
| `getIpAddress` | 无 | `String` | getIpAddress方法 |  |
| `getRemoteUser` | 无 | `String` | getRemoteUser方法 |  |
| `getSaslProperties` | 无 | `Map&lt;String, String&gt;` | getSaslProperties方法 |  |
| `getUserFromToken` | delegationToken: String | `String` | getUserFromToken方法 |  |
| `loginFromKeytab` | hiveConf: HiveConf | `void` | loginFromKeytab方法 |  |
| `loginFromSpnegoKeytabAndReturnUGI` | hiveConf: HiveConf | `UserGroupInformation` | loginFromSpnegoKeytabAndReturnUGI方法 |  |
| `needUgiLogin` | ugi: UserGroupInformation; principal: String; keytab: String | `boolean` | needUgiLogin方法 |  |
| `renewDelegationToken` | delegationToken: String | `void` | renewDelegationToken方法 |  |
| `verifyDelegationToken` | delegationToken: String | `String` | verifyDelegationToken方法 |  |
| `verifyProxyAccess` | realUser: String; proxyUser: String; ipAddress: String; hiveConf: HiveConf | `void` | verifyProxyAccess方法 |  |

### HiveFunctionRegistryUtils
**包路径**: `org.apache.hadoop.hive.ql.exec`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMethodInternal` | udfClass: Class<?>; mlist: List<Method>; exact: boolean; argumentsPassed: List<TypeInfo> | `Method` | getMethodInternal方法 |  |
| `invoke` | m: Method; thisObject: Object; arguments: Object... | `Object` | invoke方法 |  |
| `matchCost` | argumentPassed: TypeInfo; argumentAccepted: TypeInfo; exact: boolean | `int` | matchCost方法 |  |

### HiveSQLException
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toCause` | details: List<String> | `Throwable` | toCause方法 |  |
| `toTStatus` | 无 | `TStatus` | toTStatus方法 |  |
| `toTStatus` | e: Exception | `TStatus` | toTStatus方法 |  |

### HiveServer2
**包路径**: `org.apache.hive.service.server`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `BoxedUnit` | apply方法 |  |
| `execute` | 无 | `void` | execute方法 |  |
| `execute` | 无 | `void` | execute方法 |  |
| `isHTTPTransportMode` | hiveConf: HiveConf | `boolean` | isHTTPTransportMode方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `parse` | argv: String[] | `ServerOptionsProcessorResponse` | parse方法 |  |

### HiveSessionHookContextImpl
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSessionConf` | 无 | `HiveConf` | getSessionConf方法 |  |
| `getSessionHandle` | 无 | `String` | getSessionHandle方法 |  |
| `getSessionUser` | 无 | `String` | getSessionUser方法 |  |

### HiveSessionImpl
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 44

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | authFactory: HiveAuthFactory; tokenStr: String | `void` | cancelDelegationToken方法 |  |
| `cancelOperation` | opHandle: OperationHandle | `void` | cancelOperation方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `closeExpiredOperations` | 无 | `void` | closeExpiredOperations方法 |  |
| `closeOperation` | opHandle: OperationHandle | `void` | closeOperation方法 |  |
| `executeStatement` | statement: String; confOverlay: String> | `OperationHandle` | executeStatement方法 |  |
| `executeStatement` | statement: String; confOverlay: String>; queryTimeout: long | `OperationHandle` | executeStatement方法 |  |
| `executeStatementAsync` | statement: String; confOverlay: String> | `OperationHandle` | executeStatementAsync方法 |  |
| `executeStatementAsync` | statement: String; confOverlay: String>; queryTimeout: long | `OperationHandle` | executeStatementAsync方法 |  |
| `fetchResults` | opHandle: OperationHandle; orientation: FetchOrientation; maxRows: long; fetchType: FetchType | `TRowSet` | fetchResults方法 |  |
| `getCatalogs` | 无 | `OperationHandle` | getCatalogs方法 |  |
| `getColumns` | catalogName: String; schemaName: String; tableName: String; columnName: String | `OperationHandle` | getColumns方法 |  |
| `getCrossReference` | primaryCatalog: String; primarySchema: String; primaryTable: String; foreignCatalog: String; foreignSchema: String; foreignTable: String | `OperationHandle` | getCrossReference方法 |  |
| `getDelegationToken` | authFactory: HiveAuthFactory; owner: String; renewer: String | `String` | getDelegationToken方法 |  |
| `getFunctions` | catalogName: String; schemaName: String; functionName: String | `OperationHandle` | getFunctions方法 |  |
| `getHiveConf` | 无 | `HiveConf` | getHiveConf方法 |  |
| `getInfo` | getInfoType: GetInfoType | `GetInfoValue` | getInfo方法 |  |
| `getIpAddress` | 无 | `String` | getIpAddress方法 |  |
| `getLastAccessTime` | 无 | `long` | getLastAccessTime方法 |  |
| `getMetaStoreClient` | 无 | `IMetaStoreClient` | getMetaStoreClient方法 |  |
| `getNoOperationTime` | 无 | `long` | getNoOperationTime方法 |  |
| `getOperationLogSessionDir` | 无 | `File` | getOperationLogSessionDir方法 |  |
| `getPassword` | 无 | `String` | getPassword方法 |  |
| `getPrimaryKeys` | catalog: String; schema: String; table: String | `OperationHandle` | getPrimaryKeys方法 |  |
| `getProtocolVersion` | 无 | `TProtocolVersion` | getProtocolVersion方法 |  |
| `getResultSetMetadata` | opHandle: OperationHandle | `TTableSchema` | getResultSetMetadata方法 |  |
| `getSchemas` | catalogName: String; schemaName: String | `OperationHandle` | getSchemas方法 |  |
| `getSessionHandle` | 无 | `SessionHandle` | getSessionHandle方法 |  |
| `getSessionManager` | 无 | `SessionManager` | getSessionManager方法 |  |
| `getSessionState` | 无 | `SessionState` | getSessionState方法 |  |
| `getTableTypes` | 无 | `OperationHandle` | getTableTypes方法 |  |
| `getTables` | catalogName: String; schemaName: String; tableName: String; tableTypes: List<String> | `OperationHandle` | getTables方法 |  |
| `getTypeInfo` | 无 | `OperationHandle` | getTypeInfo方法 |  |
| `getUserName` | 无 | `String` | getUserName方法 |  |
| `getUsername` | 无 | `String` | getUsername方法 |  |
| `isOperationLogEnabled` | 无 | `boolean` | isOperationLogEnabled方法 |  |
| `open` | sessionConfMap: String> | `void` | open方法 |  |
| `renewDelegationToken` | authFactory: HiveAuthFactory; tokenStr: String | `void` | renewDelegationToken方法 |  |
| `setIpAddress` | ipAddress: String | `void` | setIpAddress方法 |  |
| `setOperationLogSessionDir` | operationLogRootDir: File | `void` | setOperationLogSessionDir方法 |  |
| `setOperationManager` | operationManager: OperationManager | `void` | setOperationManager方法 |  |
| `setSessionManager` | sessionManager: SessionManager | `void` | setSessionManager方法 |  |
| `setUserName` | userName: String | `void` | setUserName方法 |  |
| `setVariable` | varname: String; varvalue: String | `int` | setVariable方法 |  |

### HiveSessionImplwithUGI
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | authFactory: HiveAuthFactory; tokenStr: String | `void` | cancelDelegationToken方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `getDelegationToken` | 无 | `String` | getDelegationToken方法 |  |
| `getDelegationToken` | authFactory: HiveAuthFactory; owner: String; renewer: String | `String` | getDelegationToken方法 |  |
| `getSessionUgi` | 无 | `UserGroupInformation` | getSessionUgi方法 |  |
| `renewDelegationToken` | authFactory: HiveAuthFactory; tokenStr: String | `void` | renewDelegationToken方法 |  |
| `setProxySession` | proxySession: HiveSession | `void` | setProxySession方法 |  |
| `setSessionUGI` | owner: String | `void` | setSessionUGI方法 |  |

### HiveSessionProxy
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getProxy` | hiveSession: HiveSession; ugi: UserGroupInformation | `HiveSession` | getProxy方法 |  |
| `invoke` | arg0: Object; method: final Method; args: final Object[] | `Object` | invoke方法 |  |

### HiveTableTypeMapping
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeNames` | 无 | `Set&lt;String&gt;` | getTableTypeNames方法 |  |
| `mapToClientType` | hiveTypeName: String | `String` | mapToClientType方法 |  |

### InMemoryStore
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 21

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `accept` | key: Comparable<Object>; value: T | `void` | accept方法 |  |
| `clear` | 无 | `void` | clear方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `count` | type: Class<?> | `long` | count方法 |  |
| `count` | type: Class<?>; index: String; indexedValue: Object | `long` | count方法 |  |
| `count` | 无 | `int` | count方法 |  |
| `delete` | type: Class<?>; naturalKey: Object | `void` | delete方法 |  |
| `delete` | key: Object | `boolean` | delete方法 |  |
| `delete` | key: Object; value: T | `boolean` | delete方法 |  |
| `get` | key: Object | `T` | get方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `iterator` | 无 | `Iterator&lt;T&gt;` | iterator方法 |  |
| `next` | 无 | `T` | next方法 |  |
| `next` | max: int | `List&lt;T&gt;` | next方法 |  |
| `put` | value: T | `void` | put方法 |  |
| `setMetadata` | value: Object | `void` | setMetadata方法 |  |
| `size` | 无 | `int` | size方法 |  |
| `skip` | n: long | `boolean` | skip方法 |  |
| `view` | 无 | `InMemoryView&lt;T&gt;` | view方法 |  |
| `write` | value: Object | `void` | write方法 |  |

### InProcessLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `startApplication` | listeners: SparkAppHandle.Listener... | `SparkAppHandle` | startApplication方法 |  |

### JavaAFTSurvivalRegressionExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaALSExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMovieId` | 无 | `int` | getMovieId方法 |  |
| `getRating` | 无 | `float` | getRating方法 |  |
| `getTimestamp` | 无 | `long` | getTimestamp方法 |  |
| `getUserId` | 无 | `int` | getUserId方法 |  |
| `main` | args: String[] | `void` | main方法 |  |
| `parseRating` | str: String | `Rating` | parseRating方法 |  |

### JavaBinarizerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaBucketedRandomProjectionLSHExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaBucketizerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaChiSquareTestExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaCorrelationExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaCountVectorizerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaDCTExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaDecisionTreeClassificationExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaDecisionTreeRegressionExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaDocument
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getId` | 无 | `long` | getId方法 |  |
| `getText` | 无 | `String` | getText方法 |  |

### JavaEstimatorTransformerParamExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaFMClassifierExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaFMRegressorExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaFPGrowthExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaFeatureHasherExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaGeneralizedLinearRegressionExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaGradientBoostedTreeClassifierExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaGradientBoostedTreeRegressorExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaImputerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaIndexToStringExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaInteractionExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLDAExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLabeledDocument
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getLabel` | 无 | `double` | getLabel方法 |  |

### JavaLinearRegressionWithElasticNetExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLinearSVCExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLogisticRegressionSummaryExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaLogisticRegressionWithElasticNetExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaMaxAbsScalerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaMinHashLSHExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaMinMaxScalerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaModelSelectionViaCrossValidationExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaModelSelectionViaTrainValidationSplitExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaModuleOptions
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultModuleOptions` | 无 | `String` | defaultModuleOptions方法 |  |

### JavaMulticlassLogisticRegressionWithElasticNetExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaMultilayerPerceptronClassifierExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaNGramExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaNormalizerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaOneHotEncoderExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaOneVsRestExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaPipelineExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaPolynomialExpansionExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaQuantileDiscretizerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaRFormulaExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaRandomForestClassifierExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaRandomForestRegressorExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaRobustScalerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaSQLTransformerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaStandardScalerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaStopWordsRemoverExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaStringIndexerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaSummarizerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaTargetEncoderExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaTfIdfExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaTokenizerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaUnivariateFeatureSelectorExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaUtils
**包路径**: `org.apache.spark.network.util`
**方法数量**: 49

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `byteStringAs` | str: String; unit: ByteUnit | `long` | byteStringAs方法 |  |
| `byteStringAsBytes` | str: String | `long` | byteStringAsBytes方法 |  |
| `byteStringAsGb` | str: String | `long` | byteStringAsGb方法 |  |
| `byteStringAsKb` | str: String | `long` | byteStringAsKb方法 |  |
| `byteStringAsMb` | str: String | `long` | byteStringAsMb方法 |  |
| `bytesToString` | b: ByteBuffer | `String` | bytesToString方法 |  |
| `checkArgument` | check: boolean; msg: String; args: Object... | `void` | checkArgument方法 |  |
| `checkState` | check: boolean; msg: String; args: Object... | `void` | checkState方法 |  |
| `checkedCast` | value: long | `int` | checkedCast方法 |  |
| `cleanDirectory` | dir: File | `void` | cleanDirectory方法 |  |
| `closeQuietly` | closeable: Closeable | `void` | closeQuietly方法 |  |
| `contentEquals` | file1: File; file2: File | `boolean` | contentEquals方法 |  |
| `copyDirectory` | src: File; dst: File | `void` | copyDirectory方法 |  |
| `copyURLToFile` | url: URL; file: File | `void` | copyURLToFile方法 |  |
| `createDirectory` | root: String | `File` | createDirectory方法 |  |
| `createDirectory` | root: String; namePrefix: String | `File` | createDirectory方法 |  |
| `deleteQuietly` | file: File | `void` | deleteQuietly方法 |  |
| `deleteRecursively` | file: File | `void` | deleteRecursively方法 |  |
| `deleteRecursively` | file: File; filter: FilenameFilter | `void` | deleteRecursively方法 |  |
| `digestToHexString` | algorithm: String; input: byte[] | `String` | digestToHexString方法 |  |
| `digestToHexString` | algorithm: String; input: String | `String` | digestToHexString方法 |  |
| `forceDeleteOnExit` | file: File | `void` | forceDeleteOnExit方法 |  |
| `isTesting` | 无 | `boolean` | isTesting方法 |  |
| `join` | arr: List<Object>; sep: String | `String` | join方法 |  |
| `listFiles` | dir: File | `Set&lt;File&gt;` | listFiles方法 |  |
| `listPaths` | dir: File | `Set&lt;Path&gt;` | listPaths方法 |  |
| `md5Hex` | input: byte[] | `String` | md5Hex方法 |  |
| `md5Hex` | input: String | `String` | md5Hex方法 |  |
| `moveDirectory` | src: File; dst: File | `void` | moveDirectory方法 |  |
| `moveFile` | src: File; dst: File | `void` | moveFile方法 |  |
| `nonNegativeHash` | obj: Object | `int` | nonNegativeHash方法 |  |
| `postVisitDirectory` | dir: Path; e: IOException | `FileVisitResult` | postVisitDirectory方法 |  |
| `preVisitDirectory` | p: Path; a: BasicFileAttributes | `FileVisitResult` | preVisitDirectory方法 |  |
| `preVisitDirectory` | dir: Path; attrs: BasicFileAttributes | `FileVisitResult` | preVisitDirectory方法 |  |
| `readFully` | channel: ReadableByteChannel; dst: ByteBuffer | `void` | readFully方法 |  |
| `readFully` | in: InputStream; arr: byte[]; off: int; len: int | `void` | readFully方法 |  |
| `sha256Hex` | input: byte[] | `String` | sha256Hex方法 |  |
| `sha256Hex` | input: String | `String` | sha256Hex方法 |  |
| `sizeOf` | file: File | `long` | sizeOf方法 |  |
| `sizeOf` | dirPath: Path | `long` | sizeOf方法 |  |
| `stackTraceToString` | t: Throwable | `String` | stackTraceToString方法 |  |
| `stringToBytes` | s: String | `ByteBuffer` | stringToBytes方法 |  |
| `timeStringAs` | str: String; unit: TimeUnit | `long` | timeStringAs方法 |  |
| `timeStringAsMs` | str: String | `long` | timeStringAsMs方法 |  |
| `timeStringAsSec` | str: String | `long` | timeStringAsSec方法 |  |
| `visitFile` | p: Path; a: BasicFileAttributes | `FileVisitResult` | visitFile方法 |  |
| `visitFile` | file: Path; attrs: BasicFileAttributes | `FileVisitResult` | visitFile方法 |  |
| `visitFile` | file: Path; attrs: BasicFileAttributes | `FileVisitResult` | visitFile方法 |  |
| `visitFile` | file: Path; attrs: BasicFileAttributes | `FileVisitResult` | visitFile方法 |  |

### JavaVarianceThresholdSelectorExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaVectorAssemblerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaVectorIndexerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaVectorSizeHintExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaVectorSlicerExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JavaWord2VecExample
**包路径**: `org.apache.spark.examples.ml`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |

### JobExecutionStatus
**包路径**: `org.apache.spark`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `JobExecutionStatus` | fromString方法 |  |

### KVStoreView
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `closeableIterator` | 无 | `KVStoreIterator&lt;T&gt;` | closeableIterator方法 |  |
| `first` | value: Object | `KVStoreView&lt;T&gt;` | first方法 |  |
| `index` | name: String | `KVStoreView&lt;T&gt;` | index方法 |  |
| `last` | value: Object | `KVStoreView&lt;T&gt;` | last方法 |  |
| `max` | max: long | `KVStoreView&lt;T&gt;` | max方法 |  |
| `parent` | value: Object | `KVStoreView&lt;T&gt;` | parent方法 |  |
| `reverse` | 无 | `KVStoreView&lt;T&gt;` | reverse方法 |  |
| `skip` | n: long | `KVStoreView&lt;T&gt;` | skip方法 |  |

### KVTypeInfo
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | instance: Object | `Object` | get方法 |  |
| `get` | instance: Object | `Object` | get方法 |  |
| `getIndexValue` | indexName: String; instance: Object | `Object` | getIndexValue方法 |  |
| `getType` | 无 | `Class&lt;?&gt;` | getType方法 |  |
| `getType` | 无 | `Class&lt;?&gt;` | getType方法 |  |
| `indices` | 无 | `Stream&lt;KVIndex&gt;` | indices方法 |  |
| `type` | 无 | `Class&lt;?&gt;` | type方法 |  |

### LdapAuthenticationProviderImpl
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Authenticate` | user: String; password: String | `void` | Authenticate方法 |  |

### LevelDB
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `count` | type: Class<?> | `long` | count方法 |  |
| `count` | type: Class<?>; index: String; indexedValue: Object | `long` | count方法 |  |
| `delete` | key: byte[] | `void` | delete方法 |  |
| `delete` | type: Class<?>; naturalKey: Object | `void` | delete方法 |  |
| `iterator` | 无 | `DBIterator` | iterator方法 |  |
| `iterator` | 无 | `Iterator&lt;T&gt;` | iterator方法 |  |
| `put` | key: byte[]; value: byte[] | `void` | put方法 |  |
| `setMetadata` | value: Object | `void` | setMetadata方法 |  |
| `write` | value: Object | `void` | write方法 |  |
| `writeAll` | values: List<?> | `void` | writeAll方法 |  |

### LevelDBIterator
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `seek` | key: byte[] | `void` | seek方法 |  |

### LevelDBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkVersion` | db: DB; newversion: StoreVersion; mapper: ObjectMapper | `void` | checkVersion方法 |  |
| `initLevelDB` | dbFile: File; version: StoreVersion; mapper: ObjectMapper | `DB` | initLevelDB方法 |  |
| `log` | message: String | `void` | log方法 |  |
| `storeVersion` | db: DB; version: StoreVersion; mapper: ObjectMapper | `void` | storeVersion方法 |  |

### LocalDirsForExecutors
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `LocalDirsForExecutors` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |
| `getLocalDirsByExec` | 无 | `Map&lt;String, String[]&gt;` | getLocalDirsByExec方法 |  |

### LocalDiskShuffleDataIO
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `driver` | 无 | `ShuffleDriverComponents` | driver方法 |  |
| `executor` | 无 | `ShuffleExecutorComponents` | executor方法 |  |

### LocalDiskShuffleDriverComponents
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cleanupApplication` | 无 | `void` | cleanupApplication方法 |  |
| `initializeApplication` | 无 | `Map&lt;String, String&gt;` | initializeApplication方法 |  |
| `removeShuffle` | shuffleId: int; blocking: boolean | `void` | removeShuffle方法 |  |

### LocalDiskShuffleExecutorComponents
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createMapOutputWriter` | shuffleId: int; mapTaskId: long; numPartitions: int | `ShuffleMapOutputWriter` | createMapOutputWriter方法 |  |
| `createSingleFileMapOutputWriter` | shuffleId: int; mapId: long | `Optional&lt;SingleSpillShuffleMapOutputWriter&gt;` | createSingleFileMapOutputWriter方法 |  |
| `initializeExecutor` | appId: String; execId: String; extraConfigs: String> | `void` | initializeExecutor方法 |  |

### LocalDiskShuffleMapOutputWriter
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `abort` | error: Throwable | `void` | abort方法 |  |
| `channel` | 无 | `WritableByteChannel` | channel方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `commitAllPartitions` | checksums: long[] | `MapOutputCommitMessage` | commitAllPartitions方法 |  |
| `getCount` | 无 | `long` | getCount方法 |  |
| `getCount` | 无 | `long` | getCount方法 |  |
| `getNumBytesWritten` | 无 | `long` | getNumBytesWritten方法 |  |
| `getPartitionWriter` | reducePartitionId: int | `ShufflePartitionWriter` | getPartitionWriter方法 |  |
| `openChannelWrapper` | 无 | `Optional&lt;WritableByteChannelWrapper&gt;` | openChannelWrapper方法 |  |
| `openStream` | 无 | `OutputStream` | openStream方法 |  |
| `write` | b: int | `void` | write方法 |  |
| `write` | buf: byte[]; pos: int; length: int | `void` | write方法 |  |

### LocalDiskSingleSpillMapOutputWriter
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transferMapSpillFile` | mapSpillFile: File; partitionLengths: long[]; checksums: long[] | `void` | transferMapSpillFile方法 |  |

### LogDivertAppender
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 24

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `append` | event: LogEvent | `void` | append方法 |  |
| `create` | operationManager: OperationManager; loggingMode: OperationLog.LoggingLevel | `LogDivertAppender` | create方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; objects: Object... | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object; o3: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object; o3: Object; o4: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object; o3: Object; o4: Object; o5: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object; o3: Object; o4: Object; o5: Object; o6: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object; o3: Object; o4: Object; o5: Object; o6: Object; o7: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object; o3: Object; o4: Object; o5: Object; o6: Object; o7: Object; o8: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; s: String; o: Object; o1: Object; o2: Object; o3: Object; o4: Object; o5: Object; o6: Object; o7: Object; o8: Object; o9: Object | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; o: Object; throwable: Throwable | `Result` | filter方法 |  |
| `filter` | logger: org.apache.logging.log4j.core.Logger; level: Level; marker: Marker; message: Message; throwable: Throwable | `Result` | filter方法 |  |
| `filter` | logEvent: LogEvent | `Result` | filter方法 |  |
| `getOnMatch` | 无 | `Result` | getOnMatch方法 |  |
| `getOnMismatch` | 无 | `Result` | getOnMismatch方法 |  |
| `getState` | 无 | `State` | getState方法 |  |
| `initialize` | 无 | `void` | initialize方法 |  |
| `isStarted` | 无 | `boolean` | isStarted方法 |  |
| `isStopped` | 无 | `boolean` | isStopped方法 |  |
| `start` | 无 | `void` | start方法 |  |
| `stop` | 无 | `void` | stop方法 |  |

### MapConfigProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String | `String` | get方法 |  |
| `get` | name: String; defaultValue: String | `String` | get方法 |  |

### MemoryBlock
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fill` | value: byte | `void` | fill方法 |  |
| `fromLongArray` | array: final long[] | `MemoryBlock` | fromLongArray方法 |  |
| `size` | 无 | `long` | size方法 |  |

### MemoryConsumer
**包路径**: `org.apache.spark.memory`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acquireMemory` | size: long | `long` | acquireMemory方法 |  |
| `allocateArray` | size: long | `LongArray` | allocateArray方法 |  |
| `freeArray` | array: LongArray | `void` | freeArray方法 |  |
| `freeMemory` | size: long | `void` | freeMemory方法 |  |
| `getMode` | 无 | `MemoryMode` | getMode方法 |  |
| `getUsed` | 无 | `long` | getUsed方法 |  |
| `spill` | 无 | `void` | spill方法 |  |

### MemoryLocation
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getBaseObject` | 无 | `Object` | getBaseObject方法 |  |
| `getBaseOffset` | 无 | `long` | getBaseOffset方法 |  |
| `setObjAndOffset` | newObj: Object; newOffset: long | `void` | setObjAndOffset方法 |  |

### MergeStatuses
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `MergeStatuses` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### MergedBlockMeta
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getChunksBitmapBuffer` | 无 | `ManagedBuffer` | getChunksBitmapBuffer方法 |  |
| `getNumChunks` | 无 | `int` | getNumChunks方法 |  |

### MergedBlockMetaRequest
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `MergedBlockMetaRequest` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |
| `type` | 无 | `Type` | type方法 |  |

### MergedBlockMetaSuccess
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createFailureResponse` | error: String | `ResponseMessage` | createFailureResponse方法 |  |
| `decode` | buf: ByteBuf | `MergedBlockMetaSuccess` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |
| `getNumChunks` | 无 | `int` | getNumChunks方法 |  |
| `type` | 无 | `Type` | type方法 |  |

### Message
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `Type` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |
| `id` | 无 | `byte` | id方法 |  |

### MessageWithHeader
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `count` | 无 | `long` | count方法 |  |
| `position` | 无 | `long` | position方法 |  |
| `release` | decrement: int | `boolean` | release方法 |  |
| `retain` | increment: int | `MessageWithHeader` | retain方法 |  |
| `touch` | o: Object | `MessageWithHeader` | touch方法 |  |
| `transferTo` | target: final WritableByteChannel; position: final long | `long` | transferTo方法 |  |
| `transferred` | 无 | `long` | transferred方法 |  |

### MetadataOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |

### MutableURLClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addURL` | url: URL | `void` | addURL方法 |  |

### MyLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String[] | `void` | main方法 |  |
| `main` | args: String[] | `void` | main方法 |  |

### NettyLogger
**包路径**: `org.apache.spark.network.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getLoggingHandler` | 无 | `LoggingHandler` | getLoggingHandler方法 |  |

### NettyManagedBuffer
**包路径**: `org.apache.spark.network.buffer`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertToNetty` | 无 | `Object` | convertToNetty方法 |  |
| `convertToNettyForSsl` | 无 | `Object` | convertToNettyForSsl方法 |  |
| `createInputStream` | 无 | `InputStream` | createInputStream方法 |  |
| `nioByteBuffer` | 无 | `ByteBuffer` | nioByteBuffer方法 |  |
| `release` | 无 | `ManagedBuffer` | release方法 |  |
| `retain` | 无 | `ManagedBuffer` | retain方法 |  |
| `size` | 无 | `long` | size方法 |  |

### NettyMemoryMetrics
**包路径**: `org.apache.spark.network.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMetrics` | 无 | `Map&lt;String, Metric&gt;` | getMetrics方法 |  |

### NettyUtils
**包路径**: `org.apache.spark.network.util`
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createEventLoop` | mode: IOMode; numThreads: int; threadPrefix: String | `EventLoopGroup` | createEventLoop方法 |  |
| `createFrameDecoder` | 无 | `TransportFrameDecoder` | createFrameDecoder方法 |  |
| `createPooledByteBufAllocator` | allowDirectBufs: boolean; allowCache: boolean; numCores: int | `PooledByteBufAllocator` | createPooledByteBufAllocator方法 |  |
| `createThreadFactory` | threadPoolPrefix: String | `ThreadFactory` | createThreadFactory方法 |  |
| `defaultNumThreads` | numUsableCores: int | `int` | defaultNumThreads方法 |  |
| `freeDirectMemory` | 无 | `long` | freeDirectMemory方法 |  |
| `getClientChannelClass` | mode: IOMode | `Class&lt;? extends Channel&gt;` | getClientChannelClass方法 |  |
| `getRemoteAddress` | channel: Channel | `String` | getRemoteAddress方法 |  |
| `getServerChannelClass` | mode: IOMode | `Class&lt;? extends ServerChannel&gt;` | getServerChannelClass方法 |  |
| `preferDirectBufs` | conf: TransportConf | `boolean` | preferDirectBufs方法 |  |

### NioManagedBuffer
**包路径**: `org.apache.spark.network.buffer`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertToNetty` | 无 | `Object` | convertToNetty方法 |  |
| `convertToNettyForSsl` | 无 | `Object` | convertToNettyForSsl方法 |  |
| `createInputStream` | 无 | `InputStream` | createInputStream方法 |  |
| `nioByteBuffer` | 无 | `ByteBuffer` | nioByteBuffer方法 |  |
| `release` | 无 | `ManagedBuffer` | release方法 |  |
| `retain` | 无 | `ManagedBuffer` | retain方法 |  |
| `size` | 无 | `long` | size方法 |  |

### NoOpMergedShuffleFileManager
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String; cleanupLocalDirs: boolean | `void` | applicationRemoved方法 |  |
| `finalizeShuffleMerge` | msg: FinalizeShuffleMerge | `MergeStatuses` | finalizeShuffleMerge方法 |  |
| `getMergedBlockData` | appId: String; shuffleId: int; shuffleMergeId: int; reduceId: int; chunkId: int | `ManagedBuffer` | getMergedBlockData方法 |  |
| `getMergedBlockMeta` | appId: String; shuffleId: int; shuffleMergeId: int; reduceId: int | `MergedBlockMeta` | getMergedBlockMeta方法 |  |
| `receiveBlockDataAsStream` | msg: PushBlockStream | `StreamCallbackWithID` | receiveBlockDataAsStream方法 |  |
| `registerExecutor` | appId: String; executorInfo: ExecutorShuffleInfo | `void` | registerExecutor方法 |  |
| `removeShuffleMerge` | removeShuffleMerge: RemoveShuffleMerge | `void` | removeShuffleMerge方法 |  |

### NoOpRpcHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStreamManager` | 无 | `StreamManager` | getStreamManager方法 |  |
| `receive` | client: TransportClient; message: ByteBuffer; callback: RpcResponseCallback | `void` | receive方法 |  |

### OneForOneBlockFetcher
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onComplete` | streamId: String | `void` | onComplete方法 |  |
| `onData` | streamId: String; buf: ByteBuffer | `void` | onData方法 |  |
| `onFailure` | chunkIndex: int; e: Throwable | `void` | onFailure方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onFailure` | streamId: String; cause: Throwable | `void` | onFailure方法 |  |
| `onSuccess` | chunkIndex: int; buffer: ManagedBuffer | `void` | onSuccess方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `start` | 无 | `void` | start方法 |  |

### OneForOneBlockPusher
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `start` | 无 | `void` | start方法 |  |

### OneForOneStreamManager
**包路径**: `org.apache.spark.network.server`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkAuthorization` | client: TransportClient; streamId: long | `void` | checkAuthorization方法 |  |
| `chunkBeingSent` | streamId: long | `void` | chunkBeingSent方法 |  |
| `chunkSent` | streamId: long | `void` | chunkSent方法 |  |
| `chunksBeingTransferred` | 无 | `long` | chunksBeingTransferred方法 |  |
| `connectionTerminated` | channel: Channel | `void` | connectionTerminated方法 |  |
| `genStreamChunkId` | streamId: long; chunkId: int | `String` | genStreamChunkId方法 |  |
| `getChunk` | streamId: long; chunkIndex: int | `ManagedBuffer` | getChunk方法 |  |
| `numStreamStates` | 无 | `int` | numStreamStates方法 |  |
| `openStream` | streamChunkId: String | `ManagedBuffer` | openStream方法 |  |
| `parseStreamChunkId` | streamChunkId: String | `Pair&lt;Long, Integer&gt;` | parseStreamChunkId方法 |  |
| `registerStream` | appId: String; buffers: Iterator<ManagedBuffer>; channel: Channel; isBufferMaterializedOnNext: boolean | `long` | registerStream方法 |  |
| `registerStream` | appId: String; buffers: Iterator<ManagedBuffer>; channel: Channel | `long` | registerStream方法 |  |
| `streamBeingSent` | streamId: String | `void` | streamBeingSent方法 |  |
| `streamSent` | streamId: String | `void` | streamSent方法 |  |

### OpenBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `OpenBlocks` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### Operation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancel` | 无 | `void` | cancel方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `getBackgroundHandle` | 无 | `Future&lt;?&gt;` | getBackgroundHandle方法 |  |
| `getConfiguration` | 无 | `HiveConf` | getConfiguration方法 |  |
| `getHandle` | 无 | `OperationHandle` | getHandle方法 |  |
| `getLastAccessTime` | 无 | `long` | getLastAccessTime方法 |  |
| `getOperationLog` | 无 | `OperationLog` | getOperationLog方法 |  |
| `getOperationTimeout` | 无 | `long` | getOperationTimeout方法 |  |
| `getParentSession` | 无 | `HiveSession` | getParentSession方法 |  |
| `getProtocolVersion` | 无 | `TProtocolVersion` | getProtocolVersion方法 |  |
| `getStatus` | 无 | `OperationStatus` | getStatus方法 |  |
| `getType` | 无 | `OperationType` | getType方法 |  |
| `hasResultSet` | 无 | `boolean` | hasResultSet方法 |  |
| `isCanceled` | 无 | `boolean` | isCanceled方法 |  |
| `isFailed` | 无 | `boolean` | isFailed方法 |  |
| `isFinished` | 无 | `boolean` | isFinished方法 |  |
| `isRunning` | 无 | `boolean` | isRunning方法 |  |
| `isTimedOut` | current: long | `boolean` | isTimedOut方法 |  |
| `run` | 无 | `void` | run方法 |  |
| `setConfiguration` | configuration: HiveConf | `void` | setConfiguration方法 |  |
| `setOperationTimeout` | operationTimeout: long | `void` | setOperationTimeout方法 |  |
| `shouldRunAsync` | 无 | `boolean` | shouldRunAsync方法 |  |

### OperationHandle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationType` | 无 | `OperationType` | getOperationType方法 |  |
| `getProtocolVersion` | 无 | `TProtocolVersion` | getProtocolVersion方法 |  |
| `hasResultSet` | 无 | `boolean` | hasResultSet方法 |  |
| `setHasResultSet` | hasResultSet: boolean | `void` | setHasResultSet方法 |  |
| `toTOperationHandle` | 无 | `TOperationHandle` | toTOperationHandle方法 |  |

### OperationManager
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelOperation` | opHandle: OperationHandle | `void` | cancelOperation方法 |  |
| `closeOperation` | opHandle: OperationHandle | `void` | closeOperation方法 |  |
| `getOperation` | operationHandle: OperationHandle | `Operation` | getOperation方法 |  |
| `getOperationLogByThread` | 无 | `OperationLog` | getOperationLogByThread方法 |  |
| `getOperationLogRowSet` | opHandle: OperationHandle; orientation: FetchOrientation; maxRows: long | `TRowSet` | getOperationLogRowSet方法 |  |
| `getOperationNextRowSet` | opHandle: OperationHandle; orientation: FetchOrientation; maxRows: long | `TRowSet` | getOperationNextRowSet方法 |  |
| `getOperationResultSetSchema` | opHandle: OperationHandle | `TTableSchema` | getOperationResultSetSchema方法 |  |
| `getOperationStatus` | opHandle: OperationHandle | `OperationStatus` | getOperationStatus方法 |  |
| `newExecuteStatementOperation` | parentSession: HiveSession; statement: String; confOverlay: String>; runAsync: boolean; queryTimeout: long | `ExecuteStatementOperation` | newExecuteStatementOperation方法 |  |
| `newGetCatalogsOperation` | parentSession: HiveSession | `GetCatalogsOperation` | newGetCatalogsOperation方法 |  |
| `newGetColumnsOperation` | parentSession: HiveSession; catalogName: String; schemaName: String; tableName: String; columnName: String | `GetColumnsOperation` | newGetColumnsOperation方法 |  |
| `newGetCrossReferenceOperation` | session: HiveSession; primaryCatalog: String; primarySchema: String; primaryTable: String; foreignCatalog: String; foreignSchema: String; foreignTable: String | `GetCrossReferenceOperation` | newGetCrossReferenceOperation方法 |  |
| `newGetFunctionsOperation` | parentSession: HiveSession; catalogName: String; schemaName: String; functionName: String | `GetFunctionsOperation` | newGetFunctionsOperation方法 |  |
| `newGetPrimaryKeysOperation` | parentSession: HiveSession; catalogName: String; schemaName: String; tableName: String | `GetPrimaryKeysOperation` | newGetPrimaryKeysOperation方法 |  |
| `newGetSchemasOperation` | parentSession: HiveSession; catalogName: String; schemaName: String | `GetSchemasOperation` | newGetSchemasOperation方法 |  |
| `newGetTableTypesOperation` | parentSession: HiveSession | `GetTableTypesOperation` | newGetTableTypesOperation方法 |  |
| `newGetTablesOperation` | parentSession: HiveSession; catalogName: String; schemaName: String; tableName: String; tableTypes: List<String> | `MetadataOperation` | newGetTablesOperation方法 |  |
| `newGetTypeInfoOperation` | parentSession: HiveSession | `GetTypeInfoOperation` | newGetTypeInfoOperation方法 |  |
| `removeExpiredOperations` | handles: OperationHandle[] | `List&lt;Operation&gt;` | removeExpiredOperations方法 |  |

### OperationState
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationState` | tOperationState: TOperationState | `OperationState` | getOperationState方法 |  |
| `isTerminal` | 无 | `boolean` | isTerminal方法 |  |
| `toTOperationState` | 无 | `TOperationState` | toTOperationState方法 |  |
| `validateTransition` | oldState: OperationState; newState: OperationState | `void` | validateTransition方法 |  |
| `validateTransition` | newState: OperationState | `void` | validateTransition方法 |  |

### OperationStatus
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationException` | 无 | `HiveSQLException` | getOperationException方法 |  |
| `getState` | 无 | `OperationState` | getState方法 |  |

### OperationType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationType` | tOperationType: TOperationType | `OperationType` | getOperationType方法 |  |
| `toTOperationType` | 无 | `TOperationType` | toTOperationType方法 |  |

### PamAuthenticationProviderImpl
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Authenticate` | user: String; password: String | `void` | Authenticate方法 |  |

### ParentClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `findClass` | name: String | `Class&lt;?&gt;` | findClass方法 |  |
| `loadClass` | name: String; resolve: boolean | `Class&lt;?&gt;` | loadClass方法 |  |

### PlainSaslServer
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createSaslServer` | mechanism: String; protocol: String; serverName: String; props: ?>; cbh: CallbackHandler | `SaslServer` | createSaslServer方法 |  |
| `dispose` | 无 | `void` | dispose方法 |  |
| `getAuthorizationID` | 无 | `String` | getAuthorizationID方法 |  |
| `getMechanismName` | 无 | `String` | getMechanismName方法 |  |
| `getNegotiatedProperty` | propName: String | `Object` | getNegotiatedProperty方法 |  |
| `isComplete` | 无 | `boolean` | isComplete方法 |  |

### PrefixComparators
**包路径**: `org.apache.spark.util.collection.unsafe.sort`
**方法数量**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `compare` | aPrefix: long; bPrefix: long | `int` | compare方法 |  |
| `compare` | aPrefix: long; bPrefix: long | `int` | compare方法 |  |
| `compare` | bPrefix: long; aPrefix: long | `int` | compare方法 |  |
| `compare` | bPrefix: long; aPrefix: long | `int` | compare方法 |  |
| `compare` | a: long; b: long | `int` | compare方法 |  |
| `compare` | a: long; b: long | `int` | compare方法 |  |
| `compare` | b: long; a: long | `int` | compare方法 |  |
| `compare` | b: long; a: long | `int` | compare方法 |  |
| `computePrefix` | value: UTF8String | `long` | computePrefix方法 |  |
| `computePrefix` | bytes: byte[] | `long` | computePrefix方法 |  |
| `computePrefix` | value: double | `long` | computePrefix方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `nullsFirst` | 无 | `boolean` | nullsFirst方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortDescending` | 无 | `boolean` | sortDescending方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |
| `sortSigned` | 无 | `boolean` | sortSigned方法 |  |

### ProxyUtils
**包路径**: `org.apache.spark.deploy.yarn`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `html` | 无 | `HTML&lt;ProxyUtils.__&gt;` | html方法 |  |
| `notFound` | resp: HttpServletResponse; message: String | `void` | notFound方法 |  |
| `rejectNonHttpRequests` | req: ServletRequest | `void` | rejectNonHttpRequests方法 |  |
| `sendRedirect` | request: HttpServletRequest; response: HttpServletResponse; target: String | `void` | sendRedirect方法 |  |

### PushBlockStream
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `PushBlockStream` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### RadixSort
**包路径**: `org.apache.spark.util.collection.unsafe.sort`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `sort` | array: LongArray; numRecords: long; startByteIndex: int; endByteIndex: int; desc: boolean; signed: boolean | `int` | sort方法 |  |
| `sortKeyPrefixArray` | array: LongArray; startIndex: long; numRecords: long; startByteIndex: int; endByteIndex: int; desc: boolean; signed: boolean | `int` | sortKeyPrefixArray方法 |  |

### ReadAheadInputStream
**包路径**: `org.apache.spark.io`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `available` | 无 | `int` | available方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `read` | 无 | `int` | read方法 |  |
| `read` | b: byte[]; offset: int; len: int | `int` | read方法 |  |
| `skip` | n: long | `long` | skip方法 |  |

### RegisterExecutor
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RegisterExecutor` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### RemoteBlockPushResolver
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 35

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `applicationRemoved` | appId: String; cleanupLocalDirs: boolean | `void` | applicationRemoved方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `finalizeShuffleMerge` | msg: FinalizeShuffleMerge | `MergeStatuses` | finalizeShuffleMerge方法 |  |
| `getAppPathsInfo` | 无 | `AppPathsInfo` | getAppPathsInfo方法 |  |
| `getCompletionResponse` | 无 | `ByteBuffer` | getCompletionResponse方法 |  |
| `getCompletionResponse` | 无 | `ByteBuffer` | getCompletionResponse方法 |  |
| `getDataFilePos` | 无 | `long` | getDataFilePos方法 |  |
| `getDos` | 无 | `DataOutputStream` | getDos方法 |  |
| `getID` | 无 | `String` | getID方法 |  |
| `getID` | 无 | `String` | getID方法 |  |
| `getMapTracker` | 无 | `RoaringBitmap` | getMapTracker方法 |  |
| `getMergedBlockData` | appId: String; shuffleId: int; shuffleMergeId: int; reduceId: int; chunkId: int | `ManagedBuffer` | getMergedBlockData方法 |  |
| `getMergedBlockMeta` | appId: String; shuffleId: int; shuffleMergeId: int; reduceId: int | `MergedBlockMeta` | getMergedBlockMeta方法 |  |
| `getMergedShuffleDataFile` | shuffleId: int; shuffleMergeId: int; reduceId: int | `File` | getMergedShuffleDataFile方法 |  |
| `getMergedShuffleIndexFilePath` | shuffleId: int; shuffleMergeId: int; reduceId: int | `String` | getMergedShuffleIndexFilePath方法 |  |
| `getMergedShuffleMetaFile` | shuffleId: int; shuffleMergeId: int; reduceId: int | `File` | getMergedShuffleMetaFile方法 |  |
| `getMetrics` | 无 | `MetricSet` | getMetrics方法 |  |
| `getMetrics` | 无 | `Map&lt;String, Metric&gt;` | getMetrics方法 |  |
| `getShuffleMergePartitions` | 无 | `Map&lt;Integer, AppShufflePartitionInfo&gt;` | getShuffleMergePartitions方法 |  |
| `getShuffles` | 无 | `ConcurrentMap&lt;Integer, AppShuffleMergePartitionsInfo&gt;` | getShuffles方法 |  |
| `isFinalized` | 无 | `boolean` | isFinalized方法 |  |
| `load` | filePath: String | `ShuffleIndexInformation` | load方法 |  |
| `onComplete` | streamId: String | `void` | onComplete方法 |  |
| `onComplete` | streamId: String | `void` | onComplete方法 |  |
| `onData` | streamId: String; buf: ByteBuffer | `void` | onData方法 |  |
| `onData` | streamId: String; buf: ByteBuffer | `void` | onData方法 |  |
| `onFailure` | streamId: String; cause: Throwable | `void` | onFailure方法 |  |
| `onFailure` | streamId: String; throwable: Throwable | `void` | onFailure方法 |  |
| `receiveBlockDataAsStream` | msg: PushBlockStream | `StreamCallbackWithID` | receiveBlockDataAsStream方法 |  |
| `registerExecutor` | appId: String; executorInfo: ExecutorShuffleInfo | `void` | registerExecutor方法 |  |
| `removeShuffleMerge` | msg: RemoveShuffleMerge | `void` | removeShuffleMerge方法 |  |
| `run` | 无 | `void` | run方法 |  |
| `setDataFilePos` | dataFilePos: long | `void` | setDataFilePos方法 |  |
| `setReduceIds` | reduceIds: int[] | `void` | setReduceIds方法 |  |
| `shouldLogError` | t: Throwable | `boolean` | shouldLogError方法 |  |

### RemoveBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RemoveBlocks` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### RemoveShuffleMerge
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RemoveShuffleMerge` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### RetryingBlockTransferor
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getRetryCount` | 无 | `int` | getRetryCount方法 |  |
| `getTransferType` | 无 | `String` | getTransferType方法 |  |
| `onBlockFetchFailure` | blockId: String; exception: Throwable | `void` | onBlockFetchFailure方法 |  |
| `onBlockFetchSuccess` | blockId: String; data: ManagedBuffer | `void` | onBlockFetchSuccess方法 |  |
| `onBlockPushFailure` | blockId: String; exception: Throwable | `void` | onBlockPushFailure方法 |  |
| `onBlockPushSuccess` | blockId: String; data: ManagedBuffer | `void` | onBlockPushSuccess方法 |  |
| `onBlockTransferFailure` | blockId: String; exception: Throwable | `void` | onBlockTransferFailure方法 |  |
| `onBlockTransferSuccess` | blockId: String; data: ManagedBuffer | `void` | onBlockTransferSuccess方法 |  |
| `start` | 无 | `void` | start方法 |  |

### RocksDB
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `count` | type: Class<?> | `long` | count方法 |  |
| `count` | type: Class<?>; index: String; indexedValue: Object | `long` | count方法 |  |
| `delete` | key: byte[] | `void` | delete方法 |  |
| `delete` | type: Class<?>; naturalKey: Object | `void` | delete方法 |  |
| `iterator` | 无 | `DBIterator` | iterator方法 |  |
| `iterator` | 无 | `Iterator&lt;T&gt;` | iterator方法 |  |
| `put` | key: byte[]; value: byte[] | `void` | put方法 |  |
| `setMetadata` | value: Object | `void` | setMetadata方法 |  |
| `write` | value: Object | `void` | write方法 |  |
| `writeAll` | values: List<?> | `void` | writeAll方法 |  |

### RocksDBIterator
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `seek` | key: byte[] | `void` | seek方法 |  |

### RocksDBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkVersion` | db: RocksDB; newversion: StoreVersion; mapper: ObjectMapper | `void` | checkVersion方法 |  |
| `initRockDB` | dbFile: File; version: StoreVersion; mapper: ObjectMapper | `RocksDB` | initRockDB方法 |  |
| `storeVersion` | db: RocksDB; version: StoreVersion; mapper: ObjectMapper | `void` | storeVersion方法 |  |

### RowBasedSet
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addRow` | fields: Object[] | `RowBasedSet` | addRow方法 |  |
| `extractSubset` | maxRows: int | `RowBasedSet` | extractSubset方法 |  |
| `getSize` | 无 | `int` | getSize方法 |  |
| `getStartOffset` | 无 | `long` | getStartOffset方法 |  |
| `hasNext` | 无 | `boolean` | hasNext方法 |  |
| `iterator` | 无 | `Iterator&lt;Object[]&gt;` | iterator方法 |  |
| `numColumns` | 无 | `int` | numColumns方法 |  |
| `numRows` | 无 | `int` | numRows方法 |  |
| `remove` | 无 | `void` | remove方法 |  |
| `removeRange` | fromIndex: int; toIndex: int | `void` | removeRange方法 |  |
| `setStartOffset` | startOffset: long | `void` | setStartOffset方法 |  |
| `toTRowSet` | 无 | `TRowSet` | toTRowSet方法 |  |

### RowSetFactory
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | schema: TableSchema; version: TProtocolVersion; isBlobBased: boolean | `RowSet` | create方法 |  |
| `create` | results: TRowSet; version: TProtocolVersion | `RowSet` | create方法 |  |

### RpcHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelActive` | client: TransportClient | `void` | channelActive方法 |  |
| `channelInactive` | client: TransportClient | `void` | channelInactive方法 |  |
| `exceptionCaught` | cause: Throwable; client: TransportClient | `void` | exceptionCaught方法 |  |
| `getMergedBlockMetaReqHandler` | 无 | `MergedBlockMetaReqHandler` | getMergedBlockMetaReqHandler方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `receive` | client: TransportClient; message: ByteBuffer | `void` | receive方法 |  |
| `receiveMergeBlockMetaReq` | client: TransportClient; mergedBlockMetaRequest: MergedBlockMetaRequest; callback: MergedBlockMetaResponseCallback | `void` | receiveMergeBlockMetaReq方法 |  |
| `receiveStream` | client: TransportClient; messageHeader: ByteBuffer; callback: RpcResponseCallback | `StreamCallbackWithID` | receiveStream方法 |  |

### SSLFactory
**包路径**: `org.apache.spark.network.ssl`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | 无 | `SSLFactory` | build方法 |  |
| `certChain` | certChain: File | `Builder` | certChain方法 |  |
| `checkClientTrusted` | x509Certificates: X509Certificate[]; s: String | `void` | checkClientTrusted方法 |  |
| `checkServerTrusted` | x509Certificates: X509Certificate[]; s: String | `void` | checkServerTrusted方法 |  |
| `createSSLEngine` | isClient: boolean; allocator: ByteBufAllocator | `SSLEngine` | createSSLEngine方法 |  |
| `destroy` | 无 | `void` | destroy方法 |  |
| `keyPassword` | keyPassword: String | `Builder` | keyPassword方法 |  |
| `keyStore` | keyStore: File; keyStorePassword: String | `Builder` | keyStore方法 |  |
| `openSslEnabled` | enabled: boolean | `Builder` | openSslEnabled方法 |  |
| `privateKey` | privateKey: File | `Builder` | privateKey方法 |  |
| `privateKeyPassword` | privateKeyPassword: String | `Builder` | privateKeyPassword方法 |  |
| `requestedCiphers` | requestedCiphers: String[] | `Builder` | requestedCiphers方法 |  |
| `requestedProtocol` | requestedProtocol: String | `Builder` | requestedProtocol方法 |  |
| `trustStore` | trustStore: File; trustStorePassword: String; trustStoreReloadingEnabled: boolean; trustStoreReloadIntervalMs: int | `Builder` | trustStore方法 |  |

### SaslClientBootstrap
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | client: TransportClient; channel: Channel | `void` | doBootstrap方法 |  |

### SaslQOP
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `SaslQOP` | fromString方法 |  |

### SaslRpcHandler
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | client: TransportClient | `void` | channelInactive方法 |  |
| `doAuthChallenge` | client: TransportClient; message: ByteBuffer; callback: RpcResponseCallback | `boolean` | doAuthChallenge方法 |  |

### SaslServerBootstrap
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | channel: Channel; rpcHandler: RpcHandler | `RpcHandler` | doBootstrap方法 |  |

### ServiceUtils
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cleanup` | log: SparkLogger; closeables: java.io.Closeable... | `void` | cleanup方法 |  |
| `indexOfDomainMatch` | userName: String | `int` | indexOfDomainMatch方法 |  |

### SessionHandle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getProtocolVersion` | 无 | `TProtocolVersion` | getProtocolVersion方法 |  |
| `getSessionId` | 无 | `UUID` | getSessionId方法 |  |
| `toTSessionHandle` | 无 | `TSessionHandle` | toTSessionHandle方法 |  |

### SessionManager
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 17

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `clearIpAddress` | 无 | `void` | clearIpAddress方法 |  |
| `clearProxyUserName` | 无 | `void` | clearProxyUserName方法 |  |
| `clearUserName` | 无 | `void` | clearUserName方法 |  |
| `closeSession` | sessionHandle: SessionHandle | `void` | closeSession方法 |  |
| `getIpAddress` | 无 | `String` | getIpAddress方法 |  |
| `getOpenSessionCount` | 无 | `int` | getOpenSessionCount方法 |  |
| `getOperationManager` | 无 | `OperationManager` | getOperationManager方法 |  |
| `getProxyUserName` | 无 | `String` | getProxyUserName方法 |  |
| `getSession` | sessionHandle: SessionHandle | `HiveSession` | getSession方法 |  |
| `getUserName` | 无 | `String` | getUserName方法 |  |
| `openSession` | protocol: TProtocolVersion; username: String; password: String; ipAddress: String; sessionConf: String> | `SessionHandle` | openSession方法 |  |
| `openSession` | protocol: TProtocolVersion; username: String; password: String; ipAddress: String; sessionConf: String>; withImpersonation: boolean; delegationToken: String | `SessionHandle` | openSession方法 |  |
| `run` | 无 | `void` | run方法 |  |
| `setIpAddress` | ipAddress: String | `void` | setIpAddress方法 |  |
| `setProxyUserName` | userName: String | `void` | setProxyUserName方法 |  |
| `setUserName` | userName: String | `void` | setUserName方法 |  |
| `submitBackgroundOperation` | r: Runnable | `Future&lt;?&gt;` | submitBackgroundOperation方法 |  |

### ShreddingUtils
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `rebuild` | row: ShreddedRow; schema: VariantSchema | `Variant` | rebuild方法 |  |
| `rebuild` | row: ShreddedRow; metadata: byte[]; schema: VariantSchema; builder: VariantBuilder | `void` | rebuild方法 |  |

### ShuffleChecksumHelper
**包路径**: `org.apache.spark.network.shuffle.checksum`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `diagnoseCorruption` | algorithm: String; checksumFile: File; reduceId: int; partitionData: ManagedBuffer; checksumByReader: long | `Cause` | diagnoseCorruption方法 |  |
| `getChecksumByAlgorithm` | algorithm: String | `Checksum` | getChecksumByAlgorithm方法 |  |
| `getChecksumFileName` | blockName: String; algorithm: String | `String` | getChecksumFileName方法 |  |

### ShuffleIndexInformation
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getIndex` | reduceId: int | `ShuffleIndexRecord` | getIndex方法 |  |
| `getIndex` | startReduceId: int; endReduceId: int | `ShuffleIndexRecord` | getIndex方法 |  |
| `getRetainedMemorySize` | 无 | `int` | getRetainedMemorySize方法 |  |

### ShuffleSecretManager
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSaslUser` | appId: String | `String` | getSaslUser方法 |  |
| `getSecretKey` | appId: String | `String` | getSecretKey方法 |  |
| `registerApp` | appId: String; shuffleSecret: String | `void` | registerApp方法 |  |
| `registerApp` | appId: String; shuffleSecret: ByteBuffer | `void` | registerApp方法 |  |
| `unregisterApp` | appId: String | `void` | unregisterApp方法 |  |

### ShuffleTransportContext
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acceptInboundMessage` | msg: Object | `boolean` | acceptInboundMessage方法 |  |
| `initializePipeline` | channel: SocketChannel; isClient: boolean | `TransportChannelHandler` | initializePipeline方法 |  |
| `initializePipeline` | channel: SocketChannel; channelRpcHandler: RpcHandler; isClient: boolean | `TransportChannelHandler` | initializePipeline方法 |  |

### SimpleDownloadFile
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `closeAndRead` | 无 | `ManagedBuffer` | closeAndRead方法 |  |
| `delete` | 无 | `boolean` | delete方法 |  |
| `isOpen` | 无 | `boolean` | isOpen方法 |  |
| `openForWriting` | 无 | `DownloadFileWritableChannel` | openForWriting方法 |  |
| `path` | 无 | `String` | path方法 |  |
| `write` | src: ByteBuffer | `int` | write方法 |  |

### SparkAppHandle
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isFinal` | 无 | `boolean` | isFinal方法 |  |

### SparkDefaultUDAFEvaluatorResolver
**包路径**: `org.apache.hadoop.hive.ql.exec`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getEvaluatorClass` | argClasses: List<TypeInfo> | `Class&lt;? extends UDAFEvaluator&gt;` | getEvaluatorClass方法 |  |

### SparkDefaultUDFMethodResolver
**包路径**: `org.apache.hadoop.hive.ql.exec`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getEvalMethod` | argClasses: List<TypeInfo> | `Method` | getEvalMethod方法 |  |

### SparkFirehoseListener
**包路径**: `org.apache.spark`
**方法数量**: 36

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onApplicationEnd` | applicationEnd: SparkListenerApplicationEnd | `void` | onApplicationEnd方法 |  |
| `onApplicationStart` | applicationStart: SparkListenerApplicationStart | `void` | onApplicationStart方法 |  |
| `onBlockManagerAdded` | blockManagerAdded: SparkListenerBlockManagerAdded | `void` | onBlockManagerAdded方法 |  |
| `onBlockManagerRemoved` | blockManagerRemoved: SparkListenerBlockManagerRemoved | `void` | onBlockManagerRemoved方法 |  |
| `onBlockUpdated` | blockUpdated: SparkListenerBlockUpdated | `void` | onBlockUpdated方法 |  |
| `onEnvironmentUpdate` | environmentUpdate: SparkListenerEnvironmentUpdate | `void` | onEnvironmentUpdate方法 |  |
| `onEvent` | event: SparkListenerEvent | `void` | onEvent方法 |  |
| `onExecutorAdded` | executorAdded: SparkListenerExecutorAdded | `void` | onExecutorAdded方法 |  |
| `onExecutorBlacklisted` | executorBlacklisted: SparkListenerExecutorBlacklisted | `void` | onExecutorBlacklisted方法 |  |
| `onExecutorBlacklistedForStage` | executorBlacklistedForStage: SparkListenerExecutorBlacklistedForStage | `void` | onExecutorBlacklistedForStage方法 |  |
| `onExecutorExcluded` | executorExcluded: SparkListenerExecutorExcluded | `void` | onExecutorExcluded方法 |  |
| `onExecutorExcludedForStage` | executorExcludedForStage: SparkListenerExecutorExcludedForStage | `void` | onExecutorExcludedForStage方法 |  |
| `onExecutorMetricsUpdate` | executorMetricsUpdate: SparkListenerExecutorMetricsUpdate | `void` | onExecutorMetricsUpdate方法 |  |
| `onExecutorRemoved` | executorRemoved: SparkListenerExecutorRemoved | `void` | onExecutorRemoved方法 |  |
| `onExecutorUnblacklisted` | executorUnblacklisted: SparkListenerExecutorUnblacklisted | `void` | onExecutorUnblacklisted方法 |  |
| `onExecutorUnexcluded` | executorUnexcluded: SparkListenerExecutorUnexcluded | `void` | onExecutorUnexcluded方法 |  |
| `onJobEnd` | jobEnd: SparkListenerJobEnd | `void` | onJobEnd方法 |  |
| `onJobStart` | jobStart: SparkListenerJobStart | `void` | onJobStart方法 |  |
| `onNodeBlacklisted` | nodeBlacklisted: SparkListenerNodeBlacklisted | `void` | onNodeBlacklisted方法 |  |
| `onNodeBlacklistedForStage` | nodeBlacklistedForStage: SparkListenerNodeBlacklistedForStage | `void` | onNodeBlacklistedForStage方法 |  |
| `onNodeExcluded` | nodeExcluded: SparkListenerNodeExcluded | `void` | onNodeExcluded方法 |  |
| `onNodeExcludedForStage` | nodeExcludedForStage: SparkListenerNodeExcludedForStage | `void` | onNodeExcludedForStage方法 |  |
| `onNodeUnblacklisted` | nodeUnblacklisted: SparkListenerNodeUnblacklisted | `void` | onNodeUnblacklisted方法 |  |
| `onNodeUnexcluded` | nodeUnexcluded: SparkListenerNodeUnexcluded | `void` | onNodeUnexcluded方法 |  |
| `onOtherEvent` | event: SparkListenerEvent | `void` | onOtherEvent方法 |  |
| `onResourceProfileAdded` | event: SparkListenerResourceProfileAdded | `void` | onResourceProfileAdded方法 |  |
| `onSpeculativeTaskSubmitted` | speculativeTask: SparkListenerSpeculativeTaskSubmitted | `void` | onSpeculativeTaskSubmitted方法 |  |
| `onStageCompleted` | stageCompleted: SparkListenerStageCompleted | `void` | onStageCompleted方法 |  |
| `onStageExecutorMetrics` | executorMetrics: SparkListenerStageExecutorMetrics | `void` | onStageExecutorMetrics方法 |  |
| `onStageSubmitted` | stageSubmitted: SparkListenerStageSubmitted | `void` | onStageSubmitted方法 |  |
| `onTaskEnd` | taskEnd: SparkListenerTaskEnd | `void` | onTaskEnd方法 |  |
| `onTaskGettingResult` | taskGettingResult: SparkListenerTaskGettingResult | `void` | onTaskGettingResult方法 |  |
| `onTaskStart` | taskStart: SparkListenerTaskStart | `void` | onTaskStart方法 |  |
| `onUnpersistRDD` | unpersistRDD: SparkListenerUnpersistRDD | `void` | onUnpersistRDD方法 |  |
| `onUnschedulableTaskSetAdded` | unschedulableTaskSetAdded: SparkListenerUnschedulableTaskSetAdded | `void` | onUnschedulableTaskSetAdded方法 |  |
| `onUnschedulableTaskSetRemoved` | unschedulableTaskSetRemoved: SparkListenerUnschedulableTaskSetRemoved | `void` | onUnschedulableTaskSetRemoved方法 |  |

### SparkGenericUDAFBridge
**包路径**: `org.apache.hadoop.hive.ql.udf.generic`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getEvaluator` | parameters: TypeInfo[] | `GenericUDAFEvaluator` | getEvaluator方法 |  |
| `iterate` | agg: AggregationBuffer; parameters: Object[] | `void` | iterate方法 |  |
| `merge` | agg: AggregationBuffer; partial: Object | `void` | merge方法 |  |
| `terminate` | agg: AggregationBuffer | `Object` | terminate方法 |  |
| `terminatePartial` | agg: AggregationBuffer | `Object` | terminatePartial方法 |  |

### SparkLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 26

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addAppArgs` | args: String... | `SparkLauncher` | addAppArgs方法 |  |
| `addFile` | file: String | `SparkLauncher` | addFile方法 |  |
| `addJar` | jar: String | `SparkLauncher` | addJar方法 |  |
| `addPyFile` | file: String | `SparkLauncher` | addPyFile方法 |  |
| `addSparkArg` | arg: String | `SparkLauncher` | addSparkArg方法 |  |
| `addSparkArg` | name: String; value: String | `SparkLauncher` | addSparkArg方法 |  |
| `directory` | dir: File | `SparkLauncher` | directory方法 |  |
| `launch` | 无 | `Process` | launch方法 |  |
| `redirectError` | 无 | `SparkLauncher` | redirectError方法 |  |
| `redirectError` | to: ProcessBuilder.Redirect | `SparkLauncher` | redirectError方法 |  |
| `redirectError` | errFile: File | `SparkLauncher` | redirectError方法 |  |
| `redirectOutput` | to: ProcessBuilder.Redirect | `SparkLauncher` | redirectOutput方法 |  |
| `redirectOutput` | outFile: File | `SparkLauncher` | redirectOutput方法 |  |
| `redirectToLog` | loggerName: String | `SparkLauncher` | redirectToLog方法 |  |
| `setAppName` | appName: String | `SparkLauncher` | setAppName方法 |  |
| `setAppResource` | resource: String | `SparkLauncher` | setAppResource方法 |  |
| `setConf` | key: String; value: String | `SparkLauncher` | setConf方法 |  |
| `setConfig` | name: String; value: String | `void` | setConfig方法 |  |
| `setDeployMode` | mode: String | `SparkLauncher` | setDeployMode方法 |  |
| `setJavaHome` | javaHome: String | `SparkLauncher` | setJavaHome方法 |  |
| `setMainClass` | mainClass: String | `SparkLauncher` | setMainClass方法 |  |
| `setMaster` | master: String | `SparkLauncher` | setMaster方法 |  |
| `setPropertiesFile` | path: String | `SparkLauncher` | setPropertiesFile方法 |  |
| `setSparkHome` | sparkHome: String | `SparkLauncher` | setSparkHome方法 |  |
| `setVerbose` | verbose: boolean | `SparkLauncher` | setVerbose方法 |  |
| `startApplication` | listeners: SparkAppHandle.Listener... | `SparkAppHandle` | startApplication方法 |  |

### SparkLoggerFactory
**包路径**: `org.apache.spark.internal`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `disableStructuredLogging` | 无 | `void` | disableStructuredLogging方法 |  |
| `enableStructuredLogging` | 无 | `void` | enableStructuredLogging方法 |  |
| `getLogger` | name: String | `SparkLogger` | getLogger方法 |  |
| `getLogger` | clazz: Class<?> | `SparkLogger` | getLogger方法 |  |
| `isStructuredLoggingEnabled` | 无 | `boolean` | isStructuredLoggingEnabled方法 |  |

### SparkOrcNewRecordReader
**包路径**: `org.apache.hadoop.hive.ql.io.orc`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `getCurrentKey` | 无 | `NullWritable` | getCurrentKey方法 |  |
| `getCurrentValue` | 无 | `OrcStruct` | getCurrentValue方法 |  |
| `getObjectInspector` | 无 | `ObjectInspector` | getObjectInspector方法 |  |
| `getProgress` | 无 | `float` | getProgress方法 |  |
| `initialize` | split: InputSplit; context: TaskAttemptContext | `void` | initialize方法 |  |
| `nextKeyValue` | 无 | `boolean` | nextKeyValue方法 |  |

### SparkSaslClient
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNegotiatedProperty` | name: String | `Object` | getNegotiatedProperty方法 |  |
| `handle` | callbacks: Callback[] | `void` | handle方法 |  |

### SparkSaslServer
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `encodeIdentifier` | identifier: String | `String` | encodeIdentifier方法 |  |
| `getNegotiatedProperty` | name: String | `Object` | getNegotiatedProperty方法 |  |
| `handle` | callbacks: Callback[] | `void` | handle方法 |  |

### StageStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `StageStatus` | fromString方法 |  |

### StorageLevels
**包路径**: `org.apache.spark.api.java`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | useDisk: boolean; useMemory: boolean; useOffHeap: boolean; deserialized: boolean; replication: int | `StorageLevel` | create方法 |  |

### StreamHandle
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `StreamHandle` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### StreamInterceptor
**包路径**: `org.apache.spark.network.client`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | 无 | `void` | channelInactive方法 |  |
| `exceptionCaught` | cause: Throwable | `void` | exceptionCaught方法 |  |
| `handle` | buf: ByteBuf | `boolean` | handle方法 |  |

### StreamManager
**包路径**: `org.apache.spark.network.server`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkAuthorization` | client: TransportClient; streamId: long | `void` | checkAuthorization方法 |  |
| `chunkBeingSent` | streamId: long | `void` | chunkBeingSent方法 |  |
| `chunkSent` | streamId: long | `void` | chunkSent方法 |  |
| `chunksBeingTransferred` | 无 | `long` | chunksBeingTransferred方法 |  |
| `connectionTerminated` | channel: Channel | `void` | connectionTerminated方法 |  |
| `openStream` | streamId: String | `ManagedBuffer` | openStream方法 |  |
| `streamBeingSent` | streamId: String | `void` | streamBeingSent方法 |  |
| `streamSent` | streamId: String | `void` | streamSent方法 |  |

### TServlet
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addCustomHeader` | key: final String; value: final String | `void` | addCustomHeader方法 |  |
| `getKey` | 无 | `String` | getKey方法 |  |
| `getValue` | 无 | `String` | getValue方法 |  |
| `setCustomHeaders` | headers: String>> | `void` | setCustomHeaders方法 |  |
| `setValue` | value: String | `String` | setValue方法 |  |

### TSetIpAddressProcessor
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getUserIpAddress` | 无 | `String` | getUserIpAddress方法 |  |
| `getUserName` | 无 | `String` | getUserName方法 |  |
| `process` | in: final TProtocol; out: final TProtocol | `void` | process方法 |  |

### TSubjectAssumingTransport
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `open` | 无 | `void` | open方法 |  |

### TableSchema
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addPrimitiveColumn` | columnName: String; columnType: Type; columnComment: String | `TableSchema` | addPrimitiveColumn方法 |  |
| `addStringColumn` | columnName: String; columnComment: String | `TableSchema` | addStringColumn方法 |  |
| `clear` | 无 | `void` | clear方法 |  |
| `getColumnDescriptorAt` | pos: int | `ColumnDescriptor` | getColumnDescriptorAt方法 |  |
| `getColumnDescriptors` | 无 | `List&lt;ColumnDescriptor&gt;` | getColumnDescriptors方法 |  |
| `getSize` | 无 | `int` | getSize方法 |  |
| `toTTableSchema` | 无 | `TTableSchema` | toTTableSchema方法 |  |

### TableTypeMappingFactory
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeMapping` | mappingType: String | `TableTypeMapping` | getTableTypeMapping方法 |  |

### TaskMemoryManager
**包路径**: `org.apache.spark.memory`
**方法数量**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acquireExecutionMemory` | required: long; requestingConsumer: MemoryConsumer | `long` | acquireExecutionMemory方法 |  |
| `allocatePage` | size: long; consumer: MemoryConsumer | `MemoryBlock` | allocatePage方法 |  |
| `cleanUpAllAllocatedMemory` | 无 | `long` | cleanUpAllAllocatedMemory方法 |  |
| `decodePageNumber` | pagePlusOffsetAddress: long | `int` | decodePageNumber方法 |  |
| `encodePageNumberAndOffset` | page: MemoryBlock; offsetInPage: long | `long` | encodePageNumberAndOffset方法 |  |
| `encodePageNumberAndOffset` | pageNumber: int; offsetInPage: long | `long` | encodePageNumberAndOffset方法 |  |
| `freePage` | page: MemoryBlock; consumer: MemoryConsumer | `void` | freePage方法 |  |
| `getMemoryConsumptionForThisTask` | 无 | `long` | getMemoryConsumptionForThisTask方法 |  |
| `getOffsetInPage` | pagePlusOffsetAddress: long | `long` | getOffsetInPage方法 |  |
| `getPage` | pagePlusOffsetAddress: long | `Object` | getPage方法 |  |
| `getPeakOffHeapExecutionMemory` | 无 | `long` | getPeakOffHeapExecutionMemory方法 |  |
| `getPeakOnHeapExecutionMemory` | 无 | `long` | getPeakOnHeapExecutionMemory方法 |  |
| `getTungstenMemoryMode` | 无 | `MemoryMode` | getTungstenMemoryMode方法 |  |
| `pageSizeBytes` | 无 | `long` | pageSizeBytes方法 |  |
| `releaseExecutionMemory` | size: long; consumer: MemoryConsumer | `void` | releaseExecutionMemory方法 |  |
| `showMemoryUsage` | 无 | `void` | showMemoryUsage方法 |  |

### TaskSorting
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `TaskSorting` | fromString方法 |  |

### TaskStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `TaskStatus` | fromString方法 |  |

### ThreadFactoryWithGarbageCleanup
**包路径**: `org.apache.hive.service.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getThreadRawStoreMap` | 无 | `Map&lt;Long, RawStore&gt;` | getThreadRawStoreMap方法 |  |
| `newThread` | runnable: Runnable | `Thread` | newThread方法 |  |

### ThreadWithGarbageCleanup
**包路径**: `org.apache.hive.service.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cacheThreadLocalRawStore` | 无 | `void` | cacheThreadLocalRawStore方法 |  |
| `finalize` | 无 | `void` | finalize方法 |  |

### ThriftBinaryCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GetQueryId` | req: TGetQueryIdReq | `TGetQueryIdResp` | GetQueryId方法 |  |
| `run` | 无 | `void` | run方法 |  |

### ThriftCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 34

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `CancelDelegationToken` | req: TCancelDelegationTokenReq | `TCancelDelegationTokenResp` | CancelDelegationToken方法 |  |
| `CancelOperation` | req: TCancelOperationReq | `TCancelOperationResp` | CancelOperation方法 |  |
| `CloseOperation` | req: TCloseOperationReq | `TCloseOperationResp` | CloseOperation方法 |  |
| `CloseSession` | req: TCloseSessionReq | `TCloseSessionResp` | CloseSession方法 |  |
| `DownloadData` | req: TDownloadDataReq | `TDownloadDataResp` | DownloadData方法 |  |
| `ExecuteStatement` | req: TExecuteStatementReq | `TExecuteStatementResp` | ExecuteStatement方法 |  |
| `FetchResults` | req: TFetchResultsReq | `TFetchResultsResp` | FetchResults方法 |  |
| `GetCatalogs` | req: TGetCatalogsReq | `TGetCatalogsResp` | GetCatalogs方法 |  |
| `GetColumns` | req: TGetColumnsReq | `TGetColumnsResp` | GetColumns方法 |  |
| `GetCrossReference` | req: TGetCrossReferenceReq | `TGetCrossReferenceResp` | GetCrossReference方法 |  |
| `GetDelegationToken` | req: TGetDelegationTokenReq | `TGetDelegationTokenResp` | GetDelegationToken方法 |  |
| `GetFunctions` | req: TGetFunctionsReq | `TGetFunctionsResp` | GetFunctions方法 |  |
| `GetInfo` | req: TGetInfoReq | `TGetInfoResp` | GetInfo方法 |  |
| `GetOperationStatus` | req: TGetOperationStatusReq | `TGetOperationStatusResp` | GetOperationStatus方法 |  |
| `GetPrimaryKeys` | req: TGetPrimaryKeysReq | `TGetPrimaryKeysResp` | GetPrimaryKeys方法 |  |
| `GetQueryId` | req: TGetQueryIdReq | `TGetQueryIdResp` | GetQueryId方法 |  |
| `GetResultSetMetadata` | req: TGetResultSetMetadataReq | `TGetResultSetMetadataResp` | GetResultSetMetadata方法 |  |
| `GetSchemas` | req: TGetSchemasReq | `TGetSchemasResp` | GetSchemas方法 |  |
| `GetTableTypes` | req: TGetTableTypesReq | `TGetTableTypesResp` | GetTableTypes方法 |  |
| `GetTables` | req: TGetTablesReq | `TGetTablesResp` | GetTables方法 |  |
| `GetTypeInfo` | req: TGetTypeInfoReq | `TGetTypeInfoResp` | GetTypeInfo方法 |  |
| `OpenSession` | req: TOpenSessionReq | `TOpenSessionResp` | OpenSession方法 |  |
| `RenewDelegationToken` | req: TRenewDelegationTokenReq | `TRenewDelegationTokenResp` | RenewDelegationToken方法 |  |
| `SetClientInfo` | req: TSetClientInfoReq | `TSetClientInfoResp` | SetClientInfo方法 |  |
| `UploadData` | req: TUploadDataReq | `TUploadDataResp` | UploadData方法 |  |
| `createContext` | input: TProtocol; output: TProtocol | `ServerContext` | createContext方法 |  |
| `deleteContext` | serverContext: ServerContext; input: TProtocol; output: TProtocol | `void` | deleteContext方法 |  |
| `getPortNumber` | 无 | `int` | getPortNumber方法 |  |
| `getServerIPAddress` | 无 | `InetAddress` | getServerIPAddress方法 |  |
| `getSessionHandle` | 无 | `SessionHandle` | getSessionHandle方法 |  |
| `isWrapperFor` | aClass: Class<?> | `boolean` | isWrapperFor方法 |  |
| `preServe` | 无 | `void` | preServe方法 |  |
| `processContext` | serverContext: ServerContext; input: TTransport; output: TTransport | `void` | processContext方法 |  |
| `setSessionHandle` | sessionHandle: SessionHandle | `void` | setSessionHandle方法 |  |

### ThriftCLIServiceClient
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 28

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cancelDelegationToken` | sessionHandle: SessionHandle; authFactory: HiveAuthFactory; tokenStr: String | `void` | cancelDelegationToken方法 |  |
| `cancelOperation` | opHandle: OperationHandle | `void` | cancelOperation方法 |  |
| `checkStatus` | status: TStatus | `void` | checkStatus方法 |  |
| `closeOperation` | opHandle: OperationHandle | `void` | closeOperation方法 |  |
| `closeSession` | sessionHandle: SessionHandle | `void` | closeSession方法 |  |
| `executeStatement` | sessionHandle: SessionHandle; statement: String; confOverlay: String> | `OperationHandle` | executeStatement方法 |  |
| `executeStatement` | sessionHandle: SessionHandle; statement: String; confOverlay: String>; queryTimeout: long | `OperationHandle` | executeStatement方法 |  |
| `executeStatementAsync` | sessionHandle: SessionHandle; statement: String; confOverlay: String> | `OperationHandle` | executeStatementAsync方法 |  |
| `executeStatementAsync` | sessionHandle: SessionHandle; statement: String; confOverlay: String>; queryTimeout: long | `OperationHandle` | executeStatementAsync方法 |  |
| `fetchResults` | opHandle: OperationHandle; orientation: FetchOrientation; maxRows: long; fetchType: FetchType | `TRowSet` | fetchResults方法 |  |
| `fetchResults` | opHandle: OperationHandle | `TRowSet` | fetchResults方法 |  |
| `getCatalogs` | sessionHandle: SessionHandle | `OperationHandle` | getCatalogs方法 |  |
| `getColumns` | sessionHandle: SessionHandle; catalogName: String; schemaName: String; tableName: String; columnName: String | `OperationHandle` | getColumns方法 |  |
| `getCrossReference` | sessionHandle: SessionHandle; primaryCatalog: String; primarySchema: String; primaryTable: String; foreignCatalog: String; foreignSchema: String; foreignTable: String | `OperationHandle` | getCrossReference方法 |  |
| `getDelegationToken` | sessionHandle: SessionHandle; authFactory: HiveAuthFactory; owner: String; renewer: String | `String` | getDelegationToken方法 |  |
| `getFunctions` | sessionHandle: SessionHandle; catalogName: String; schemaName: String; functionName: String | `OperationHandle` | getFunctions方法 |  |
| `getInfo` | sessionHandle: SessionHandle; infoType: GetInfoType | `GetInfoValue` | getInfo方法 |  |
| `getOperationStatus` | opHandle: OperationHandle | `OperationStatus` | getOperationStatus方法 |  |
| `getPrimaryKeys` | sessionHandle: SessionHandle; catalog: String; schema: String; table: String | `OperationHandle` | getPrimaryKeys方法 |  |
| `getQueryId` | operationHandle: TOperationHandle | `String` | getQueryId方法 |  |
| `getResultSetMetadata` | opHandle: OperationHandle | `TTableSchema` | getResultSetMetadata方法 |  |
| `getSchemas` | sessionHandle: SessionHandle; catalogName: String; schemaName: String | `OperationHandle` | getSchemas方法 |  |
| `getTableTypes` | sessionHandle: SessionHandle | `OperationHandle` | getTableTypes方法 |  |
| `getTables` | sessionHandle: SessionHandle; catalogName: String; schemaName: String; tableName: String; tableTypes: List<String> | `OperationHandle` | getTables方法 |  |
| `getTypeInfo` | sessionHandle: SessionHandle | `OperationHandle` | getTypeInfo方法 |  |
| `openSession` | username: String; password: String; configuration: String> | `SessionHandle` | openSession方法 |  |
| `openSessionWithImpersonation` | username: String; password: String; configuration: String>; delegationToken: String | `SessionHandle` | openSessionWithImpersonation方法 |  |
| `renewDelegationToken` | sessionHandle: SessionHandle; authFactory: HiveAuthFactory; tokenStr: String | `void` | renewDelegationToken方法 |  |

### ThriftHttpCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | 无 | `void` | run方法 |  |

### ThriftHttpServlet
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | 无 | `String` | run方法 |  |

### TimerWithCustomTimeUnit
**包路径**: `org.apache.spark.network.util`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `dump` | outputStream: OutputStream | `void` | dump方法 |  |
| `getMax` | 无 | `long` | getMax方法 |  |
| `getMean` | 无 | `double` | getMean方法 |  |
| `getMin` | 无 | `long` | getMin方法 |  |
| `getSnapshot` | 无 | `Snapshot` | getSnapshot方法 |  |
| `getStdDev` | 无 | `double` | getStdDev方法 |  |
| `getValue` | v: double | `double` | getValue方法 |  |
| `size` | 无 | `int` | size方法 |  |

### TransientBestEffortLazyVal
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `T` | apply方法 |  |

### TransportChannelHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acceptInboundMessage` | msg: Object | `boolean` | acceptInboundMessage方法 |  |
| `channelActive` | ctx: ChannelHandlerContext | `void` | channelActive方法 |  |
| `channelInactive` | ctx: ChannelHandlerContext | `void` | channelInactive方法 |  |
| `channelRead0` | ctx: ChannelHandlerContext; request: Message | `void` | channelRead0方法 |  |
| `channelRegistered` | ctx: ChannelHandlerContext | `void` | channelRegistered方法 |  |
| `channelUnregistered` | ctx: ChannelHandlerContext | `void` | channelUnregistered方法 |  |
| `exceptionCaught` | ctx: ChannelHandlerContext; cause: Throwable | `void` | exceptionCaught方法 |  |
| `getClient` | 无 | `TransportClient` | getClient方法 |  |
| `getRequestHandler` | 无 | `TransportRequestHandler` | getRequestHandler方法 |  |
| `getResponseHandler` | 无 | `TransportResponseHandler` | getResponseHandler方法 |  |
| `userEventTriggered` | ctx: ChannelHandlerContext; evt: Object | `void` | userEventTriggered方法 |  |

### TransportClient
**包路径**: `org.apache.spark.network.client`
**方法数量**: 19

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `fetchChunk` | streamId: long; chunkIndex: int; callback: ChunkReceivedCallback | `void` | fetchChunk方法 |  |
| `getChannel` | 无 | `Channel` | getChannel方法 |  |
| `getClientId` | 无 | `String` | getClientId方法 |  |
| `getHandler` | 无 | `TransportResponseHandler` | getHandler方法 |  |
| `getSocketAddress` | 无 | `SocketAddress` | getSocketAddress方法 |  |
| `isActive` | 无 | `boolean` | isActive方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `operationComplete` | future: Future<? super Void> | `void` | operationComplete方法 |  |
| `removeRpcRequest` | requestId: long | `void` | removeRpcRequest方法 |  |
| `send` | message: ByteBuffer | `void` | send方法 |  |
| `sendMergedBlockMetaReq` | appId: String; shuffleId: int; shuffleMergeId: int; reduceId: int; callback: MergedBlockMetaResponseCallback | `void` | sendMergedBlockMetaReq方法 |  |
| `sendRpc` | message: ByteBuffer; callback: RpcResponseCallback | `long` | sendRpc方法 |  |
| `sendRpcSync` | message: ByteBuffer; timeoutMs: long | `ByteBuffer` | sendRpcSync方法 |  |
| `setClientId` | id: String | `void` | setClientId方法 |  |
| `stream` | streamId: String; callback: StreamCallback | `void` | stream方法 |  |
| `timeOut` | 无 | `void` | timeOut方法 |  |
| `uploadStream` | meta: ManagedBuffer; data: ManagedBuffer; callback: RpcResponseCallback | `long` | uploadStream方法 |  |

### TransportClientFactory
**包路径**: `org.apache.spark.network.client`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `createClient` | remoteHost: String; remotePort: int; fastFail: boolean | `TransportClient` | createClient方法 |  |
| `createClient` | remoteHost: String; remotePort: int | `TransportClient` | createClient方法 |  |
| `createUnmanagedClient` | remoteHost: String; remotePort: int | `TransportClient` | createUnmanagedClient方法 |  |
| `getAllMetrics` | 无 | `MetricSet` | getAllMetrics方法 |  |
| `initChannel` | ch: SocketChannel | `void` | initChannel方法 |  |
| `operationComplete` | handshakeFuture: final Future<Channel> | `void` | operationComplete方法 |  |

### TransportConf
**包路径**: `org.apache.spark.network.util`
**方法数量**: 58

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `authEngineVersion` | 无 | `int` | authEngineVersion方法 |  |
| `authRTTimeoutMs` | 无 | `int` | authRTTimeoutMs方法 |  |
| `backLog` | 无 | `int` | backLog方法 |  |
| `chunkFetchHandlerThreads` | 无 | `int` | chunkFetchHandlerThreads方法 |  |
| `cipherTransformation` | 无 | `String` | cipherTransformation方法 |  |
| `clientThreads` | 无 | `int` | clientThreads方法 |  |
| `connectionCreationTimeoutMs` | 无 | `int` | connectionCreationTimeoutMs方法 |  |
| `connectionTimeoutMs` | 无 | `int` | connectionTimeoutMs方法 |  |
| `cryptoConf` | 无 | `Properties` | cryptoConf方法 |  |
| `enableSaslRetries` | 无 | `boolean` | enableSaslRetries方法 |  |
| `enableTcpKeepAlive` | 无 | `boolean` | enableTcpKeepAlive方法 |  |
| `encryptionEnabled` | 无 | `boolean` | encryptionEnabled方法 |  |
| `finalizeShuffleMergeHandlerThreads` | 无 | `int` | finalizeShuffleMergeHandlerThreads方法 |  |
| `get` | name: String; defaultValue: String | `String` | get方法 |  |
| `getInt` | name: String; defaultValue: int | `int` | getInt方法 |  |
| `getModuleName` | 无 | `String` | getModuleName方法 |  |
| `ioExceptionsThresholdDuringMerge` | 无 | `int` | ioExceptionsThresholdDuringMerge方法 |  |
| `ioMode` | 无 | `String` | ioMode方法 |  |
| `ioRetryWaitTimeMs` | 无 | `int` | ioRetryWaitTimeMs方法 |  |
| `lazyFileDescriptor` | 无 | `boolean` | lazyFileDescriptor方法 |  |
| `maxChunksBeingTransferred` | 无 | `long` | maxChunksBeingTransferred方法 |  |
| `maxIORetries` | 无 | `int` | maxIORetries方法 |  |
| `maxSaslEncryptedBlockSize` | 无 | `int` | maxSaslEncryptedBlockSize方法 |  |
| `memoryMapBytes` | 无 | `int` | memoryMapBytes方法 |  |
| `mergedIndexCacheSize` | 无 | `long` | mergedIndexCacheSize方法 |  |
| `mergedShuffleCleanerShutdownTimeout` | 无 | `long` | mergedShuffleCleanerShutdownTimeout方法 |  |
| `mergedShuffleFileManagerImpl` | 无 | `String` | mergedShuffleFileManagerImpl方法 |  |
| `minChunkSizeInMergedShuffleFile` | 无 | `int` | minChunkSizeInMergedShuffleFile方法 |  |
| `numConnectionsPerPeer` | 无 | `int` | numConnectionsPerPeer方法 |  |
| `portMaxRetries` | 无 | `int` | portMaxRetries方法 |  |
| `preferDirectBufs` | 无 | `boolean` | preferDirectBufs方法 |  |
| `preferDirectBufsForSharedByteBufAllocators` | 无 | `boolean` | preferDirectBufsForSharedByteBufAllocators方法 |  |
| `receiveBuf` | 无 | `int` | receiveBuf方法 |  |
| `saslEncryption` | 无 | `boolean` | saslEncryption方法 |  |
| `saslFallback` | 无 | `boolean` | saslFallback方法 |  |
| `saslServerAlwaysEncrypt` | 无 | `boolean` | saslServerAlwaysEncrypt方法 |  |
| `sendBuf` | 无 | `int` | sendBuf方法 |  |
| `separateChunkFetchRequest` | 无 | `boolean` | separateChunkFetchRequest方法 |  |
| `separateFinalizeShuffleMerge` | 无 | `boolean` | separateFinalizeShuffleMerge方法 |  |
| `serverThreads` | 无 | `int` | serverThreads方法 |  |
| `sharedByteBufAllocators` | 无 | `boolean` | sharedByteBufAllocators方法 |  |
| `sslRpcCertChain` | 无 | `File` | sslRpcCertChain方法 |  |
| `sslRpcEnabled` | 无 | `boolean` | sslRpcEnabled方法 |  |
| `sslRpcEnabledAndKeysAreValid` | 无 | `boolean` | sslRpcEnabledAndKeysAreValid方法 |  |
| `sslRpcKeyPassword` | 无 | `String` | sslRpcKeyPassword方法 |  |
| `sslRpcKeyStore` | 无 | `File` | sslRpcKeyStore方法 |  |
| `sslRpcKeyStorePassword` | 无 | `String` | sslRpcKeyStorePassword方法 |  |
| `sslRpcOpenSslEnabled` | 无 | `boolean` | sslRpcOpenSslEnabled方法 |  |
| `sslRpcPrivateKey` | 无 | `File` | sslRpcPrivateKey方法 |  |
| `sslRpcPrivateKeyPassword` | 无 | `String` | sslRpcPrivateKeyPassword方法 |  |
| `sslRpcProtocol` | 无 | `String` | sslRpcProtocol方法 |  |
| `sslRpcTrustStore` | 无 | `File` | sslRpcTrustStore方法 |  |
| `sslRpcTrustStorePassword` | 无 | `String` | sslRpcTrustStorePassword方法 |  |
| `sslRpcTrustStoreReloadingEnabled` | 无 | `boolean` | sslRpcTrustStoreReloadingEnabled方法 |  |
| `sslRpctrustStoreReloadIntervalMs` | 无 | `int` | sslRpctrustStoreReloadIntervalMs方法 |  |
| `sslShuffleChunkSize` | 无 | `int` | sslShuffleChunkSize方法 |  |
| `useOldFetchProtocol` | 无 | `boolean` | useOldFetchProtocol方法 |  |
| `verboseMetrics` | 无 | `boolean` | verboseMetrics方法 |  |

### TransportContext
**包路径**: `org.apache.spark.network`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `createClientFactory` | bootstraps: List<TransportClientBootstrap> | `TransportClientFactory` | createClientFactory方法 |  |
| `createClientFactory` | 无 | `TransportClientFactory` | createClientFactory方法 |  |
| `createServer` | port: int; bootstraps: List<TransportServerBootstrap> | `TransportServer` | createServer方法 |  |
| `createServer` | host: String; port: int; bootstraps: List<TransportServerBootstrap> | `TransportServer` | createServer方法 |  |
| `createServer` | bootstraps: List<TransportServerBootstrap> | `TransportServer` | createServer方法 |  |
| `createServer` | 无 | `TransportServer` | createServer方法 |  |
| `getConf` | 无 | `TransportConf` | getConf方法 |  |
| `getRegisteredConnections` | 无 | `Counter` | getRegisteredConnections方法 |  |
| `initializePipeline` | channel: SocketChannel; isClient: boolean | `TransportChannelHandler` | initializePipeline方法 |  |
| `initializePipeline` | channel: SocketChannel; channelRpcHandler: RpcHandler; isClient: boolean | `TransportChannelHandler` | initializePipeline方法 |  |
| `sslEncryptionEnabled` | 无 | `boolean` | sslEncryptionEnabled方法 |  |

### TransportFrameDecoder
**包路径**: `org.apache.spark.network.util`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | ctx: ChannelHandlerContext | `void` | channelInactive方法 |  |
| `channelRead` | ctx: ChannelHandlerContext; data: Object | `void` | channelRead方法 |  |
| `exceptionCaught` | ctx: ChannelHandlerContext; cause: Throwable | `void` | exceptionCaught方法 |  |
| `handlerRemoved` | ctx: ChannelHandlerContext | `void` | handlerRemoved方法 |  |
| `setInterceptor` | interceptor: Interceptor | `void` | setInterceptor方法 |  |

### TransportRequestHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelActive` | 无 | `void` | channelActive方法 |  |
| `channelInactive` | 无 | `void` | channelInactive方法 |  |
| `exceptionCaught` | cause: Throwable | `void` | exceptionCaught方法 |  |
| `getID` | 无 | `String` | getID方法 |  |
| `handle` | request: RequestMessage | `void` | handle方法 |  |
| `onComplete` | streamId: String | `void` | onComplete方法 |  |
| `onData` | streamId: String; buf: ByteBuffer | `void` | onData方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onFailure` | streamId: String; cause: Throwable | `void` | onFailure方法 |  |
| `onFailure` | e: Throwable | `void` | onFailure方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess方法 |  |
| `onSuccess` | numChunks: int; buffer: ManagedBuffer | `void` | onSuccess方法 |  |

### TransportResponseHandler
**包路径**: `org.apache.spark.network.client`
**方法数量**: 14

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addFetchRequest` | streamChunkId: StreamChunkId; callback: ChunkReceivedCallback | `void` | addFetchRequest方法 |  |
| `addRpcRequest` | requestId: long; callback: BaseResponseCallback | `void` | addRpcRequest方法 |  |
| `addStreamCallback` | streamId: String; callback: StreamCallback | `void` | addStreamCallback方法 |  |
| `channelActive` | 无 | `void` | channelActive方法 |  |
| `channelInactive` | 无 | `void` | channelInactive方法 |  |
| `deactivateStream` | 无 | `void` | deactivateStream方法 |  |
| `exceptionCaught` | cause: Throwable | `void` | exceptionCaught方法 |  |
| `getTimeOfLastRequestNs` | 无 | `long` | getTimeOfLastRequestNs方法 |  |
| `handle` | message: ResponseMessage | `void` | handle方法 |  |
| `hasOutstandingRequests` | 无 | `Boolean` | hasOutstandingRequests方法 |  |
| `numOutstandingRequests` | 无 | `int` | numOutstandingRequests方法 |  |
| `removeFetchRequest` | streamChunkId: StreamChunkId | `void` | removeFetchRequest方法 |  |
| `removeRpcRequest` | requestId: long | `void` | removeRpcRequest方法 |  |
| `updateTimeOfLastRequest` | 无 | `void` | updateTimeOfLastRequest方法 |  |

### TransportServer
**包路径**: `org.apache.spark.network.server`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | close方法 |  |
| `getAllMetrics` | 无 | `MetricSet` | getAllMetrics方法 |  |
| `getPort` | 无 | `int` | getPort方法 |  |
| `getRegisteredConnections` | 无 | `Counter` | getRegisteredConnections方法 |  |

### TypeDescriptor
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getColumnSize` | 无 | `Integer` | getColumnSize方法 |  |
| `getDecimalDigits` | 无 | `Integer` | getDecimalDigits方法 |  |
| `getPrecision` | 无 | `Integer` | getPrecision方法 |  |
| `getType` | 无 | `Type` | getType方法 |  |
| `getTypeName` | 无 | `String` | getTypeName方法 |  |
| `getTypeQualifiers` | 无 | `TypeQualifiers` | getTypeQualifiers方法 |  |
| `setTypeQualifiers` | typeQualifiers: TypeQualifiers | `void` | setTypeQualifiers方法 |  |
| `toTTypeDesc` | 无 | `TTypeDesc` | toTTypeDesc方法 |  |

### TypeQualifiers
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 9

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromTTypeQualifiers` | ttq: TTypeQualifiers | `TypeQualifiers` | fromTTypeQualifiers方法 |  |
| `fromTypeInfo` | pti: PrimitiveTypeInfo | `TypeQualifiers` | fromTypeInfo方法 |  |
| `getCharacterMaximumLength` | 无 | `Integer` | getCharacterMaximumLength方法 |  |
| `getPrecision` | 无 | `Integer` | getPrecision方法 |  |
| `getScale` | 无 | `Integer` | getScale方法 |  |
| `setCharacterMaximumLength` | characterMaximumLength: int | `void` | setCharacterMaximumLength方法 |  |
| `setPrecision` | precision: Integer | `void` | setPrecision方法 |  |
| `setScale` | scale: Integer | `void` | setScale方法 |  |
| `toTTypeQualifiers` | 无 | `TTypeQualifiers` | toTTypeQualifiers方法 |  |

### UTF8StringBuilder
**包路径**: `org.apache.spark.unsafe`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `append` | value: UTF8String | `void` | append方法 |  |
| `append` | value: String | `void` | append方法 |  |
| `appendBytes` | base: Object; offset: long; length: int | `void` | appendBytes方法 |  |
| `appendCodePoint` | codePoint: int | `void` | appendCodePoint方法 |  |
| `build` | 无 | `UTF8String` | build方法 |  |

### UnsafeAlignedOffset
**包路径**: `org.apache.spark.unsafe`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSize` | object: Object; offset: long | `int` | getSize方法 |  |
| `getUaoSize` | 无 | `int` | getUaoSize方法 |  |
| `putSize` | object: Object; offset: long; value: int | `void` | putSize方法 |  |
| `setUaoSize` | size: int | `void` | setUaoSize方法 |  |

### UnsafeMemoryAllocator
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `allocate` | size: long | `MemoryBlock` | allocate方法 |  |
| `free` | memory: MemoryBlock | `void` | free方法 |  |

### UnsafeShuffleWriter
**包路径**: `org.apache.spark.shuffle.sort`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channel` | 无 | `WritableByteChannel` | channel方法 |  |
| `close` | 无 | `void` | close方法 |  |
| `getPeakMemoryUsedBytes` | 无 | `long` | getPeakMemoryUsedBytes方法 |  |
| `stop` | success: boolean | `Option&lt;MapStatus&gt;` | stop方法 |  |
| `write` | records: V>> | `void` | write方法 |  |
| `write` | records: V>> | `void` | write方法 |  |

### UploadBlock
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `UploadBlock` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### UploadBlockStream
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `UploadBlockStream` | decode方法 |  |
| `encode` | buf: ByteBuf | `void` | encode方法 |  |
| `encodedLength` | 无 | `int` | encodedLength方法 |  |

### VariantBuilder
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addKey` | key: String | `int` | addKey方法 |  |
| `appendBinary` | binary: byte[] | `void` | appendBinary方法 |  |
| `appendBoolean` | b: boolean | `void` | appendBoolean方法 |  |
| `appendDate` | daysSinceEpoch: int | `void` | appendDate方法 |  |
| `appendDecimal` | d: BigDecimal | `void` | appendDecimal方法 |  |
| `appendDouble` | d: double | `void` | appendDouble方法 |  |
| `appendFloat` | f: float | `void` | appendFloat方法 |  |
| `appendLong` | l: long | `void` | appendLong方法 |  |
| `appendNull` | 无 | `void` | appendNull方法 |  |
| `appendString` | str: String | `void` | appendString方法 |  |
| `appendTimestamp` | microsSinceEpoch: long | `void` | appendTimestamp方法 |  |
| `appendTimestampNtz` | microsSinceEpoch: long | `void` | appendTimestampNtz方法 |  |
| `appendUuid` | uuid: UUID | `void` | appendUuid方法 |  |
| `appendVariant` | v: Variant | `void` | appendVariant方法 |  |
| `compareTo` | other: FieldEntry | `int` | compareTo方法 |  |
| `finishWritingArray` | start: int; offsets: ArrayList<Integer> | `void` | finishWritingArray方法 |  |
| `finishWritingObject` | start: int; fields: ArrayList<FieldEntry> | `void` | finishWritingObject方法 |  |
| `getWritePos` | 无 | `int` | getWritePos方法 |  |
| `parseJson` | json: String; allowDuplicateKeys: boolean | `Variant` | parseJson方法 |  |
| `parseJson` | parser: JsonParser; allowDuplicateKeys: boolean | `Variant` | parseJson方法 |  |
| `result` | 无 | `Variant` | result方法 |  |
| `shallowAppendVariant` | v: Variant | `void` | shallowAppendVariant方法 |  |

### VariantSchema
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isUnshredded` | 无 | `boolean` | isUnshredded方法 |  |

### VariantShreddingWriter
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `castShredded` | v: Variant; schema: VariantSchema; builder: ShreddedResultBuilder | `ShreddedResult` | castShredded方法 |  |

### VariantUtil
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 18

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `arrayHeader` | largeSize: boolean; offsetSize: int | `byte` | arrayHeader方法 |  |
| `getBoolean` | value: byte[]; pos: int | `boolean` | getBoolean方法 |  |
| `getDecimal` | value: byte[]; pos: int | `BigDecimal` | getDecimal方法 |  |
| `getDecimalWithOriginalScale` | value: byte[]; pos: int | `BigDecimal` | getDecimalWithOriginalScale方法 |  |
| `getDouble` | value: byte[]; pos: int | `double` | getDouble方法 |  |
| `getFloat` | value: byte[]; pos: int | `float` | getFloat方法 |  |
| `getLong` | value: byte[]; pos: int | `long` | getLong方法 |  |
| `getMetadataKey` | metadata: byte[]; id: int | `String` | getMetadataKey方法 |  |
| `getString` | value: byte[]; pos: int | `String` | getString方法 |  |
| `getType` | value: byte[]; pos: int | `Type` | getType方法 |  |
| `getTypeInfo` | value: byte[]; pos: int | `int` | getTypeInfo方法 |  |
| `getUuid` | value: byte[]; pos: int | `UUID` | getUuid方法 |  |
| `objectHeader` | largeSize: boolean; idSize: int; offsetSize: int | `byte` | objectHeader方法 |  |
| `primitiveHeader` | type: int | `byte` | primitiveHeader方法 |  |
| `readUnsigned` | bytes: byte[]; pos: int; numBytes: int | `int` | readUnsigned方法 |  |
| `shortStrHeader` | size: int | `byte` | shortStrHeader方法 |  |
| `valueSize` | value: byte[]; pos: int | `int` | valueSize方法 |  |
| `writeLong` | bytes: byte[]; pos: int; value: long; numBytes: int | `void` | writeLong方法 |  |

### VariantVal
**包路径**: `org.apache.spark.unsafe.types`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `debugString` | 无 | `String` | debugString方法 |  |
| `readFromUnsafeRow` | offsetAndSize: long; baseObject: Object; baseOffset: long | `VariantVal` | readFromUnsafeRow方法 |  |
| `toJson` | zoneId: ZoneId | `String` | toJson方法 |  |

### YarnShuffleService
**包路径**: `org.apache.spark.network.yarn`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMetaData` | 无 | `ByteBuffer` | getMetaData方法 |  |
| `initializeApplication` | context: ApplicationInitializationContext | `void` | initializeApplication方法 |  |
| `initializeContainer` | context: ContainerInitializationContext | `void` | initializeContainer方法 |  |
| `setRecoveryPath` | recoveryPath: Path | `void` | setRecoveryPath方法 |  |
| `stopApplication` | context: ApplicationTerminationContext | `void` | stopApplication方法 |  |
| `stopContainer` | context: ContainerTerminationContext | `void` | stopContainer方法 |  |

### instead
**包路径**: `org.apache.spark.api.java`
**方法数量**: 48

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkpoint` | 无 | `Unit` | checkpoint方法 |  |
| `collect` | 无 | `JList` | collect方法 |  |
| `collectAsync` | 无 | `JavaFutureAction` | collectAsync方法 |  |
| `collectPartitions` | Array[Int]: partitionIds: | `Array` | collectPartitions方法 |  |
| `count` | 无 | `Long` | count方法 |  |
| `countApprox` | Long: timeout:; Double: confidence: | `PartialResult` | countApprox方法 |  |
| `countApprox` | Long: timeout: | `PartialResult` | countApprox方法 |  |
| `countApproxDistinct` | Double: relativeSD: | `Long` | countApproxDistinct方法 |  |
| `countAsync` | 无 | `JavaFutureAction` | countAsync方法 |  |
| `countByValue` | 无 | `JMap` | countByValue方法 |  |
| `countByValueApprox` | Long: timeout:; Double: confidence: | `PartialResult` | countByValueApprox方法 |  |
| `countByValueApprox` | Long: timeout: | `PartialResult` | countByValueApprox方法 |  |
| `first` | 无 | `T` | first方法 |  |
| `flatMapToDouble` | DoubleFlatMapFunction[T]: f: | `JavaDoubleRDD` | flatMapToDouble方法 |  |
| `fold` | T: zeroValue: | `Unit` | fold方法 |  |
| `foreach` | VoidFunction[T]: f: | `Unit` | foreach方法 |  |
| `foreachAsync` | VoidFunction[T]: f: | `JavaFutureAction` | foreachAsync方法 |  |
| `foreachPartition` | VoidFunction[JIterator[T]]: f: | `Unit` | foreachPartition方法 |  |
| `foreachPartitionAsync` | VoidFunction[JIterator[T]]: f: | `JavaFutureAction` | foreachPartitionAsync方法 |  |
| `getCheckpointFile` | 无 | `Optional` | getCheckpointFile方法 |  |
| `glom` | 无 | `JavaRDD` | glom方法 |  |
| `isEmpty` | 无 | `Boolean` | isEmpty方法 |  |
| `iterator` | Partition: split:; TaskContext: taskContext: | `JIterator` | iterator方法 |  |
| `mapPartitionsToDouble` | DoubleFlatMapFunction[JIterator[T]]: f: | `JavaDoubleRDD` | mapPartitionsToDouble方法 |  |
| `mapPartitionsToDouble` | DoubleFlatMapFunction[JIterator[T]]: f:; Boolean: preservesPartitioning: | `JavaDoubleRDD` | mapPartitionsToDouble方法 |  |
| `max` | Comparator[T]: comp: | `T` | max方法 |  |
| `min` | Comparator[T]: comp: | `T` | min方法 |  |
| `pipe` | String: command: | `JavaRDD` | pipe方法 |  |
| `pipe` | JList[String]: command: | `JavaRDD` | pipe方法 |  |
| `pipe` | JList[String]: command:; JMap[String: env: | `JavaRDD` | pipe方法 |  |
| `pipe` | JList[String]: command:; JMap[String: env:; Boolean: separateWorkingDir:; Int: bufferSize: | `JavaRDD` | pipe方法 |  |
| `pipe` | JList[String]: command:; JMap[String: env:; Boolean: separateWorkingDir:; Int: bufferSize:; String: encoding: | `JavaRDD` | pipe方法 |  |
| `reduce` | JFunction2[T: f: | `T` | reduce方法 |  |
| `saveAsObjectFile` | String: path: | `Unit` | saveAsObjectFile方法 |  |
| `saveAsTextFile` | String: path: | `Unit` | saveAsTextFile方法 |  |
| `saveAsTextFile` | String: path:; CompressionCodec]: codec: Class[_ <: | `Unit` | saveAsTextFile方法 |  |
| `take` | Int: num: | `JList` | take方法 |  |
| `takeAsync` | Int: num: | `JavaFutureAction` | takeAsync方法 |  |
| `takeOrdered` | Int: num:; Comparator[T]: comp: | `JList` | takeOrdered方法 |  |
| `takeOrdered` | Int: num: | `JList` | takeOrdered方法 |  |
| `toDebugString` | 无 | `String` | toDebugString方法 |  |
| `toLocalIterator` | 无 | `JIterator` | toLocalIterator方法 |  |
| `top` | Int: num:; Comparator[T]: comp: | `JList` | top方法 |  |
| `top` | Int: num: | `JList` | top方法 |  |
| `treeReduce` | JFunction2[T: f:; Int: depth: | `T` | treeReduce方法 |  |
| `treeReduce` | JFunction2[T: f: | `T` | treeReduce方法 |  |
| `zipWithIndex` | 无 | `JavaPairRDD` | zipWithIndex方法 |  |
| `zipWithUniqueId` | 无 | `JavaPairRDD` | zipWithUniqueId方法 |  |

---

## 存储级别

### StorageLevelMapper
**包路径**: `org.apache.spark.storage`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | s: String | `StorageLevel` | fromString方法 |  |

---
