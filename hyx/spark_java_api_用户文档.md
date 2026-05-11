# Spark Java API 用户文档

> **文档定位**: 仅包含用户直接调用的public API（约2200+方法）
> **开发者API**: 已移除Connector/插件开发接口，普通用户不需要

---

## 快速入门

### 1. RDD完整示例

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class RDDExample {
    public static void main(String[] args) {
        // 创建SparkContext
        SparkConf conf = new SparkConf()
            .setAppName("RDD Example")
            .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 读取数据
        JavaRDD<String> lines = sc.textFile("data.txt");
        
        // 转换：过滤长度>10的行
        JavaRDD<String> filtered = lines.filter(
            s -> s.length() > 10
        );
        
        // 行动：计数
        long count = filtered.count();
        System.out.println("Count: " + count);
        
        sc.stop();
    }
}
```

### 2. DataFrame完整示例

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class DataFrameExample {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("DataFrame Example")
            .master("local[*]")
            .getOrCreate();
        
        // 读取CSV
        Dataset<Row> df = spark.read()
            .option("header", "true")
            .csv("data.csv");
        
        // SQL操作
        Dataset<Row> result = df
            .filter(col("age").gt(18))
            .groupBy("city")
            .agg(count("id").as("count"));
        
        result.show();
        spark.stop();
    }
}
```

### 3. Streaming完整示例

```java
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaSparkContext;

public class StreamingExample {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = ...;
        
        // 创建StreamingContext，每5秒一个批次
        JavaStreamingContext jssc = new JavaStreamingContext(
            sc, Durations.seconds(5)
        );
        
        // 监控目录中的新文件
        JavaDStream<String> lines = jssc.textFileStream("hdfs://logs/");
        
        // 处理：统计词频
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
             .mapToPair(word -> new Tuple2<>(word, 1))
             .reduceByKey((a, b) -> a + b)
             .print();
        
        jssc.start();
        jssc.awaitTermination();
    }
}
```

### 4. 机器学习完整示例

```java
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MLlibExample {
    public static void main(String[] args) {
        JavaSparkContext sc = ...;
        
        // 准备数据
        JavaRDD<Vector> data = sc.parallelize(Arrays.asList(
            Vectors.dense(1.0, 2.0),
            Vectors.dense(3.0, 4.0),
            Vectors.dense(5.0, 6.0)
        ));
        
        // 训练KMeans（3个簇，20次迭代）
        KMeansModel model = KMeans.train(data.rdd(), 3, 20);
        
        // 预测
        int cluster = model.predict(Vectors.dense(2.0, 3.0));
        System.out.println("Cluster: " + cluster);
        
        sc.stop();
    }
}
```

---

## 核心导入速查

```java
// RDD Core
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;

// SQL Core
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

// Streaming
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Durations;

// MLlib
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.recommendation.ALS;

// 共享变量
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
```

---

### JavaDoubleRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 33

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaDoubleRDD` | 缓存RDD到内存，默认MEMORY_ONLY | // cache：缓存RDD到内存<br>JavaRDD<String> rdd = sc.textFile("hdfs://large/file.txt");<br>// 缓存后，后续操作会直接从内存读取<br>rdd.cache();<br>// 多次使用RDD时缓存可提升性能<br>long count1 = rdd.count();  // 第一次计算，会缓存<br>long count2 = rdd.count();  // 第二次直接从内存读取 |
| `coalesce` | numPartitions: Int | `JavaDoubleRDD` | 减少分区数，默认不触发shuffle，适用于合并小分区提高效率 | // coalesce(numPartitions)：仅减少分区，不shuffle<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);  // 10个分区<br>// 减少到2个分区（高效，数据保持原位置）<br>JavaRDD<String> coalesced = rdd.coalesce(2); |
| `coalesce` | numPartitions: Int, shuffle: Boolean | `JavaDoubleRDD` | 减少分区数，可控制是否shuffle。shuffle=true时可真正重新分布数据 | // coalesce(numPartitions, shuffle)：可强制shuffle重新分布<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);<br>// shuffle=true：数据重新均匀分布到2个分区<br>JavaRDD<String> coalescedShuffle = rdd.coalesce(2, true);<br>// shuffle=false（默认）：仅合并分区，数据不移动<br>JavaRDD<String> coalescedNoShuffle = rdd.coalesce(2, false); |
| `distinct` | 无 | `JavaDoubleRDD` | 去除重复元素，使用默认分区数 | // distinct()：去重，使用默认分区<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));<br>JavaRDD<Integer> distinct = rdd.distinct();<br>// 结果: [1, 2, 3, 4, 5] |
| `distinct` | numPartitions: Int | `JavaDoubleRDD` | 去除重复元素，指定结果分区数，可控制并行度 | // distinct(numPartitions)：去重并指定分区数<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));<br>// 指定3个分区，适合大数据去重时控制并行度<br>JavaRDD<Integer> distinct = rdd.distinct(3);<br>// 结果: [1, 2, 3, 4, 5]，分散在3个分区中 |
| `filter` | JFunction[JDouble: f | `JavaDoubleRDD` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `histogram` | bucketCount: Int | `Unit` | 计算直方图，按指定桶数量均匀划分数据范围 | // histogram(bucketCount)：按桶数计算直方图<br>List<Double> data = Arrays.asList(1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>// 指定3个桶，Spark自动计算桶边界<br>Tuple2<double[], long[]> hist = doubleRDD.histogram(3);<br>// hist._1 = [1.0, 2.0, 3.0, 4.0] 桶边界<br>// hist._2 = [3, 2, 2] 每桶元素数 |
| `histogram` | Array[scala.Double]: buckets | `Array` | 计算直方图，使用自定义桶边界，精确控制分桶范围 | // histogram(buckets)：使用自定义桶边界<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>// 自定义桶边界：[0, 2, 4, 6]<br>double[] buckets = new double[]{0.0, 2.0, 4.0, 6.0};<br>Tuple2<double[], long[]> hist = doubleRDD.histogram(buckets);<br>// hist._1 = [0.0, 2.0, 4.0, 6.0]<br>// hist._2 = [2, 2, 2] 每桶元素数 |
| `intersection` | JavaDoubleRDD: other | `JavaDoubleRDD` | 返回两个RDD的交集 | // intersection：取交集<br>JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));<br>JavaRDD<Integer> intersection = rdd1.intersection(rdd2);<br>// 结果: [3, 4] |
| `max` | 无 | `JDouble` | 最大值 | // max：最大值<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));<br>double max = doubleRDD.max();<br>// 结果: 30.0 |
| `mean` | 无 | `JDouble` | 计算平均值 | // mean：计算平均值<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>double avg = doubleRDD.mean();<br>// 结果: 3.0 |
| `meanApprox` | timeout: Long, confidence: JDouble | `PartialResult` | 近似计算平均值，在超时时间内返回带置信区间的近似结果 | // meanApprox(timeout, confidence)：近似平均值<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 100.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>// 1000ms超时，95%置信度<br>PartialResult<BoundedDouble> result = doubleRDD.meanApprox(1000, 0.95);<br>// result.getFinalValue() 返回近似均值及置信区间 |
| `meanApprox` | timeout: Long | `PartialResult` | 近似计算平均值，仅指定超时时间，使用默认置信度0.95 | // meanApprox(timeout)：仅指定超时<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0));<br>// 500ms超时，默认95%置信度<br>PartialResult<BoundedDouble> result = doubleRDD.meanApprox(500); |
| `min` | 无 | `JDouble` | 最小值 | // min：最小值<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));<br>double min = doubleRDD.min();<br>// 结果: 5.0 |
| `persist` | StorageLevel: newLevel | `JavaDoubleRDD` | 持久化RDD到指定存储级别 | // persist：持久化到指定存储级别<br>JavaRDD<String> rdd = sc.textFile("hdfs://data/file.txt");<br>// 内存+磁盘持久化<br>rdd.persist(StorageLevel.MEMORY_AND_DISK());<br>// 序列化存储（节省空间）<br>rdd.persist(StorageLevel.MEMORY_ONLY_SER());<br>// 堆外内存存储<br>rdd.persist(StorageLevel.OFF_HEAP()); |
| `repartition` | numPartitions: Int | `JavaDoubleRDD` | 重新分区，增加或减少分区数，触发shuffle | // repartition：重新分区（会shuffle）<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 2);  // 2个分区<br>// 增加到10个分区（触发shuffle）<br>JavaRDD<String> repartitioned = rdd.repartition(10);<br>// 注意：repartition会shuffle，coalesce只减少分区不shuffle |
| `sample` | withReplacement: Boolean, fraction: JDouble | `JavaDoubleRDD` | 随机采样，fraction为期望采样比例，非精确比例 | // sample(withReplacement, fraction)：随机采样<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 不重复采样（false），期望50%比例<br>JavaRDD<Integer> sampled = rdd.sample(false, 0.5);<br>// 重复采样（true），每个元素可被选中多次，期望200%<br>JavaRDD<Integer> sampledWithRep = rdd.sample(true, 2.0); |
| `sample` | withReplacement: Boolean, fraction: JDouble, seed: Long | `JavaDoubleRDD` | 随机采样，指定随机种子确保结果可重现 | // sample(withReplacement, fraction, seed)：可重现采样<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 指定种子42，每次运行结果相同<br>JavaRDD<Integer> sampled1 = rdd.sample(false, 0.5, 42);<br>JavaRDD<Integer> sampled2 = rdd.sample(false, 0.5, 42);<br>// sampled1与sampled2结果完全相同 |
| `sampleStdev` | 无 | `JDouble` | 计算样本标准差（n-1校正），适用于抽样数据 | // sampleStdev：样本标准差<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>double sampleStdev = doubleRDD.sampleStdev();<br>// 样本标准差 = sqrt(sum((x-mean)^2)/(n-1))<br>// 用于抽样数据，消除偏差 |
| `sampleVariance` | 无 | `JDouble` | 计算样本方差（n-1校正），衡量抽样数据离散程度 | // sampleVariance：样本方差<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>double sampleVar = doubleRDD.sampleVariance();<br>// 样本方差 = sum((x-mean)^2)/(n-1) |
| `setName` | name: String | `JavaDoubleRDD` | 设置RDD名称，用于调试和Spark UI显示 | // setName：设置RDD名称便于调试<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0));<br>doubleRDD.setName("my-double-rdd");<br>// 在Spark UI中显示此名称，便于追踪作业 |
| `stats` | 无 | `StatCounter` | 返回统计摘要(计数、均值、方差、最小、最大) | // stats：获取完整统计信息<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>StatCounter stats = doubleRDD.stats();<br>System.out.println("Count: " + stats.count());      // 5<br>System.out.println("Mean: " + stats.mean());        // 3.0<br>System.out.println("Sum: " + stats.sum());          // 15.0<br>System.out.println("Min: " + stats.min());          // 1.0<br>System.out.println("Max: " + stats.max());          // 5.0<br>System.out.println("Stdev: " + stats.stdev());      // 1.41... |
| `stdev` | 无 | `JDouble` | 计算标准差 | // stdev：计算标准差<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>double stdev = doubleRDD.stdev();<br>// 标准差 = 方差的平方根 |
| `subtract` | JavaDoubleRDD: other | `JavaDoubleRDD` | 返回当前RDD减去另一个RDD的差集，使用默认分区数 | // subtract(other)：取差集<br>JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));<br>JavaRDD<Integer> subtracted = rdd1.subtract(rdd2);<br>// 结果: [1, 2] (rdd1中不在rdd2的元素) |
| `subtract` | JavaDoubleRDD: other, numPartitions: Int | `JavaDoubleRDD` | 返回差集，指定结果分区数控制并行度 | // subtract(other, numPartitions)：指定分区数的差集<br>JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));<br>// 指定结果使用2个分区<br>JavaRDD<Integer> subtracted = rdd1.subtract(rdd2, 2); |
| `subtract` | JavaDoubleRDD: other, Partitioner: p | `JavaDoubleRDD` | 返回差集，使用自定义分区器控制数据分布 | // subtract(other, partitioner)：自定义分区器的差集<br>JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));<br>// 使用HashPartitioner<br>JavaRDD<Integer> subtracted = rdd1.subtract(rdd2, new HashPartitioner(4)); |
| `sum` | 无 | `JDouble` | 求和 | // sum：求和<br>List<Double> data = Arrays.asList(10.0, 20.0, 30.0, 40.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>double total = doubleRDD.sum();<br>// 结果: 100.0 |
| `sumApprox` | timeout: Long, confidence: JDouble | `PartialResult` | 近似计算总和，在超时内返回带置信区间的近似结果 | // sumApprox(timeout, confidence)：近似求和<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>// 1000ms超时，90%置信度<br>PartialResult<BoundedDouble> result = doubleRDD.sumApprox(1000, 0.90); |
| `sumApprox` | timeout: Long | `PartialResult` | 近似计算总和，仅指定超时时间，默认置信度0.95 | // sumApprox(timeout)：仅指定超时的近似求和<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0));<br>PartialResult<BoundedDouble> result = doubleRDD.sumApprox(500); |
| `union` | JavaDoubleRDD: other | `JavaDoubleRDD` | 合并两个RDD，保留所有元素（包括重复），不触发shuffle | // union：合并RDD（保留重复）<br>JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "b"));<br>JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("c", "d"));<br>JavaRDD<String> unionRDD = rdd1.union(rdd2);<br>// 结果: ["a", "b", "b", "c", "d"] 注意重复元素保留 |
| `unpersist` | 无 | `JavaDoubleRDD` | 取消RDD持久化，非阻塞方式立即释放内存 | // unpersist()：非阻塞释放缓存<br>JavaRDD<String> rdd = sc.textFile("hdfs://file.txt");<br>rdd.cache();<br>rdd.count(); // 触发缓存<br>// 非阻塞释放，立即返回<br>rdd.unpersist(); |
| `unpersist` | blocking: Boolean | `JavaDoubleRDD` | 取消RDD持久化，可控制是否阻塞等待释放完成 | // unpersist(blocking)：可阻塞释放缓存<br>JavaRDD<String> rdd = sc.textFile("hdfs://file.txt");<br>rdd.cache();<br>rdd.count();<br>// blocking=true：等待释放完成后再返回<br>rdd.unpersist(true);<br>// blocking=false：非阻塞立即返回（默认）<br>rdd.unpersist(false); |
| `variance` | 无 | `JDouble` | 计算方差 | // variance：计算方差<br>List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);<br>double variance = doubleRDD.variance();<br>// 方差衡量数据的离散程度 |

| `fromRDD` | RDD[Double] rdd | `JavaDoubleRDD` | 从Scala RDD创建JavaDoubleRDD | `JavaDoubleRDD doubleRdd = JavaDoubleRDD.fromRDD(scalaRdd);` |
| `popStdev` | 无 | `double` | 总体标准差 | `double stdev = doubleRdd.popStdev();` |
| `popVariance` | 无 | `double` | 总体方差 | `double variance = doubleRdd.popVariance();` |

### JavaPairRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 53

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaPairRDD` | 缓存RDD到内存，默认MEMORY_ONLY | // cache：缓存RDD到内存<br>JavaRDD<String> rdd = sc.textFile("hdfs://large/file.txt");<br>// 缓存后，后续操作会直接从内存读取<br>rdd.cache();<br>// 多次使用RDD时缓存可提升性能<br>long count1 = rdd.count();  // 第一次计算，会缓存<br>long count2 = rdd.count();  // 第二次直接从内存读取 |
| `coalesce` | numPartitions: Int | `JavaPairRDD` | 减少分区数，默认不触发shuffle，保持Key-Value映射 | // coalesce(numPartitions)：减少PairRDD分区<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("b", 2));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data, 10);<br>// 减少到2个分区<br>JavaPairRDD<String, Integer> coalesced = pairRDD.coalesce(2); |
| `coalesce` | numPartitions: Int, shuffle: Boolean | `JavaPairRDD` | 减少分区数，可控制shuffle。shuffle=true会重新分布KV数据 | // coalesce(numPartitions, shuffle)：PairRDD可shuffle合并<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data, 10);<br>// shuffle=true：KV数据重新均匀分布<br>JavaPairRDD<String, Integer> coalesced = pairRDD.coalesce(2, true); |
| `collectAsMap` | 无 | `java` | 收集RDD为Java Map | // collectAsMap：收集为Map<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("key1", 10),<br>    new Tuple2<>("key2", 20)<br>);<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>Map<String, Integer> map = pairRDD.collectAsMap();<br>// 结果: {"key1": 10, "key2": 20}<br>// 注意：如果Key重复，只保留最后一个Value |
| `countApproxDistinctByKey` | relativeSD: Double, Partitioner: partitioner | `JavaPairRDD` | 近似统计每个Key的唯一Value数量，使用自定义分区器和相对标准偏差 | // countApproxDistinctByKey(relativeSD, partitioner)<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 1));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>// relativeSD=0.05表示5%误差率<br>JavaPairRDD<String, Long> approx = pairRDD.countApproxDistinctByKey(0.05, new HashPartitioner(2)); |
| `countApproxDistinctByKey` | relativeSD: Double, numPartitions: Int | `JavaPairRDD` | 近似统计每个Key的唯一Value数量，指定分区数 | // countApproxDistinctByKey(relativeSD, numPartitions)<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>JavaPairRDD<String, Long> approx = pairRDD.countApproxDistinctByKey(0.05, 4); |
| `countApproxDistinctByKey` | relativeSD: Double | `JavaPairRDD` | 近似统计每个Key的唯一Value数量，使用默认分区数 | // countApproxDistinctByKey(relativeSD)<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>JavaPairRDD<String, Long> approx = pairRDD.countApproxDistinctByKey(0.05); |
| `countByKey` | 无 | `java` | 统计每个Key的数量 | // countByKey：统计每个Key的数量<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("apple", 1),<br>    new Tuple2<>("banana", 2),<br>    new Tuple2<>("apple", 3),<br>    new Tuple2<>("apple", 4)<br>);<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>Map<String, Long> counts = pairRDD.countByKey();<br>// 结果: {"apple": 3, "banana": 1} |
| `countByKeyApprox` | timeout: Long | `PartialResult` | 近似统计每个Key的数量，仅指定超时时间 | // countByKeyApprox(timeout)：近似Key计数<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 1));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>PartialResult<Map<String, BoundedDouble>> result = pairRDD.countByKeyApprox(1000); |
| `countByKeyApprox` | timeout: Long, 0.95: confidence | `PartialResult` | 近似统计每个Key的数量，指定超时和置信度 | // countByKeyApprox(timeout, confidence)<br>PartialResult<Map<String, BoundedDouble>> result = pairRDD.countByKeyApprox(1000, 0.90); |
| `distinct` | 无 | `JavaPairRDD` | 去除重复(K,V)键值对，使用默认分区数 | // distinct()：PairRDD去重<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("a", 1), new Tuple2<>("b", 2));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> distinct = pairRDD.distinct();<br>// 结果: [("a", 1), ("b", 2)] |
| `distinct` | numPartitions: Int | `JavaPairRDD` | 去除重复键值对，指定结果分区数 | // distinct(numPartitions)：指定分区去重<br>JavaPairRDD<String, Integer> distinct = pairRDD.distinct(3); |
| `filter` | JFunction[(K: f | `Unit` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `foldByKey` | V: zeroValue, Partitioner: partitioner, JFunction2[V: func | `JavaPairRDD` | 按Key聚合，使用零值和自定义分区器，适用于需要初始值的聚合 | // foldByKey(zeroValue, partitioner, func)<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 3));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>// 使用零值0和HashPartitioner<br>JavaPairRDD<String, Integer> folded = pairRDD.foldByKey(0, new HashPartitioner(2), (a, b) -> a + b);<br>// 结果: [("a", 3), ("b", 3)] |
| `foldByKey` | V: zeroValue, numPartitions: Int, JFunction2[V: func | `JavaPairRDD` | 按Key聚合，使用零值并指定分区数 | // foldByKey(zeroValue, numPartitions, func)<br>JavaPairRDD<String, Integer> folded = pairRDD.foldByKey(0, 3, (a, b) -> a + b); |
| `foldByKey` | V: zeroValue, JFunction2[V: func | `JavaPairRDD` | 按Key聚合，使用零值和默认分区数，最常用形式 | // foldByKey(zeroValue, func)<br>JavaPairRDD<String, Integer> folded = pairRDD.foldByKey(0, (a, b) -> a + b);<br>// 注意：零值在每个分区的聚合开始时都会使用 |
| `groupByKey` | Partitioner: partitioner | `JavaPairRDD` | 按Key分组Value，使用自定义分区器控制数据分布 | // groupByKey(partitioner)：自定义分区器分组<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("fruit", 1), new Tuple2<>("fruit", 2), new Tuple2<>("vegetable", 3));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>// 使用HashPartitioner控制分布<br>JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey(new HashPartitioner(2));<br>// 注意：groupByKey可能导致数据倾斜，建议用reduceByKey替代 |
| `groupByKey` | numPartitions: Int | `JavaPairRDD` | 按Key分组Value，指定结果分区数控制并行度 | // groupByKey(numPartitions)：指定分区数分组<br>JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey(4); |
| `groupByKey` | 无 | `JavaPairRDD` | 按Key分组Value，使用默认分区数，最简形式 | // groupByKey()：默认分组<br>JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey();<br>// 结果: [("fruit", [1, 2]), ("vegetable", [3])] |
| `intersection` | JavaPairRDD[K: other | `JavaPairRDD` | 返回两个PairRDD的交集（相同Key和Value），使用默认分区 | // intersection：PairRDD取交集<br>List<Tuple2<String, Integer>> data1 = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("b", 2));<br>List<Tuple2<String, Integer>> data2 = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("c", 3));<br>JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);<br>JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);<br>JavaPairRDD<String, Integer> intersect = rdd1.intersection(rdd2);<br>// 结果: [("a", 1)] 需Key和Value都相同 |
| `keys` | 无 | `JavaRDD` | 返回所有Key的RDD | // keys：获取所有Key<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>));<br>JavaRDD<String> keysRDD = pairRDD.keys();<br>// 结果: ["a", "b"] |
| `lookup` | K: key | `JList` | 查找指定Key的所有Value | // lookup：查找指定Key的所有Value<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("apple", 1),<br>    new Tuple2<>("apple", 2),<br>    new Tuple2<>("banana", 3)<br>);<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>List<Integer> appleValues = pairRDD.lookup("apple");<br>// 结果: [1, 2] |
| `partitionBy` | Partitioner: partitioner | `JavaPairRDD` | 使用指定分区器重新分区，确保相同Key的数据在同一分区 | // partitionBy：使用分区器重新分布<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("c", 3));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>// 使用HashPartitioner，确保相同Key在同一分区<br>JavaPairRDD<String, Integer> partitioned = pairRDD.partitionBy(new HashPartitioner(4));<br>// 常用于join前的预分区，优化join性能 |
| `persist` | StorageLevel: newLevel | `JavaPairRDD` | 持久化RDD到指定存储级别 | // persist：持久化到指定存储级别<br>JavaRDD<String> rdd = sc.textFile("hdfs://data/file.txt");<br>// 内存+磁盘持久化<br>rdd.persist(StorageLevel.MEMORY_AND_DISK());<br>// 序列化存储（节省空间）<br>rdd.persist(StorageLevel.MEMORY_ONLY_SER());<br>// 堆外内存存储<br>rdd.persist(StorageLevel.OFF_HEAP()); |
| `reduceByKey` | Partitioner: partitioner, JFunction2[V: func | `JavaPairRDD` | 按Key聚合Value，使用自定义分区器控制分区数和数据分布 | // reduceByKey(partitioner, func)：自定义分区器聚合<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("apple", 1), new Tuple2<>("apple", 3), new Tuple2<>("banana", 2));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> summed = pairRDD.reduceByKey(new HashPartitioner(2), (a, b) -> a + b);<br>// 结果: [("apple", 4), ("banana", 2)] |
| `reduceByKey` | JFunction2[V: func, numPartitions: Int | `JavaPairRDD` | 按Key聚合Value，指定分区数控制并行度 | // reduceByKey(func, numPartitions)：指定分区数<br>JavaPairRDD<String, Integer> summed = pairRDD.reduceByKey((a, b) -> a + b, 4);<br>// 使用4个分区进行聚合 |
| `reduceByKey` | JFunction2[V: func | `JavaPairRDD` | 按Key聚合Value，使用默认分区数，最常用形式，比groupByKey高效 | // reduceByKey(func)：默认聚合<br>JavaPairRDD<String, Integer> summed = pairRDD.reduceByKey((a, b) -> a + b);<br>// 注意：map端预聚合，比groupByKey更高效 |
| `reduceByKeyLocally` | JFunction2[V: func | `java` | 按Key聚合Value并返回本地Map，不触发shuffle，适合小数据集 | // reduceByKeyLocally：本地聚合返回Map<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>Map<String, Integer> localMap = pairRDD.reduceByKeyLocally((a, b) -> a + b);<br>// 直接返回Java Map，不需要collect |
| `repartition` | numPartitions: Int | `JavaPairRDD` | 重新分区，增加或减少分区数，触发shuffle | // repartition：重新分区（会shuffle）<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 2);  // 2个分区<br>// 增加到10个分区（触发shuffle）<br>JavaRDD<String> repartitioned = rdd.repartition(10);<br>// 注意：repartition会shuffle，coalesce只减少分区不shuffle |
| `repartitionAndSortWithinPartitions` | Partitioner: partitioner | `JavaPairRDD` | 重新分区并在每个分区内按Key排序，适用于范围查询优化 | // repartitionAndSortWithinPartitions(partitioner)：分区排序<br>List<Tuple2<Integer, String>> data = Arrays.asList(<br>    new Tuple2<>(1, "a"), new Tuple2<>(2, "b"), new Tuple2<>(1, "c"));<br>JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(data);<br>JavaPairRDD<Integer, String> sorted = pairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2));<br>// 每个分区内部已排序，优化后续范围查询 |
| `repartitionAndSortWithinPartitions` | Partitioner: partitioner, Comparator[K]: comp | `JavaPairRDD` | 重新分区并在分区内使用自定义比较器排序 | // repartitionAndSortWithinPartitions(partitioner, comp)<br>JavaPairRDD<Integer, String> sorted = pairRDD.repartitionAndSortWithinPartitions(<br>    new HashPartitioner(2), Comparator.reverseOrder());<br>// 使用自定义比较器在分区内降序排序 |
| `sample` | withReplacement: Boolean, fraction: Double | `JavaPairRDD` | 随机采样PairRDD，fraction为期望比例 | // sample(withReplacement, fraction)：PairRDD采样<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("b", 2));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sampled = pairRDD.sample(false, 0.5); |
| `sample` | withReplacement: Boolean, fraction: Double, seed: Long | `JavaPairRDD` | 随机采样PairRDD，指定种子确保结果可重现 | // sample(withReplacement, fraction, seed)：可重现采样<br>JavaPairRDD<String, Integer> sampled1 = pairRDD.sample(false, 0.5, 42);<br>JavaPairRDD<String, Integer> sampled2 = pairRDD.sample(false, 0.5, 42);<br>// 相同种子，相同结果 |
| `sampleByKey` | withReplacement: Boolean, java.util.Map[K: fractions, seed: Long | `JavaPairRDD` | 按Key分层采样，每个Key使用不同采样比例，可重现 | // sampleByKey：分层采样<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("c", 3));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>Map<String, Double> fractions = new HashMap<>();<br>fractions.put("a", 0.5);  // Key "a" 采样50%<br>fractions.put("b", 1.0);  // Key "b" 全采样<br>JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKey(false, fractions, 42L); |
| `sampleByKey` | withReplacement: Boolean, java.util.Map[K: fractions | `JavaPairRDD` | 按Key分层采样，每个Key使用不同比例 | // sampleByKey：分层采样（无种子）<br>JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKey(false, fractions); |
| `sampleByKeyExact` | withReplacement: Boolean, java.util.Map[K: fractions, seed: Long | `JavaPairRDD` | 按Key精确分层采样，确保每个Key采样精确数量 | // sampleByKeyExact：精确分层采样<br>JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKeyExact(false, fractions, 42L);<br>// 确保每个Key采样数量精确符合比例 |
| `sampleByKeyExact` | withReplacement: Boolean, java.util.Map[K: fractions | `JavaPairRDD` | 按Key精确分层采样，无种子 | // sampleByKeyExact：精确采样（无种子）<br>JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKeyExact(false, fractions); |
| `saveAsHadoopDataset` | JobConf: conf | `Unit` | 使用旧版Hadoop API保存RDD到Hadoop输出格式 | // saveAsHadoopDataset：旧版Hadoop API保存<br>JobConf jobConf = new JobConf();<br>jobConf.setOutputFormatClass(TextOutputFormat.class);<br>jobConf.setOutputKeyClass(String.class);<br>jobConf.setOutputValueClass(Integer.class);<br>pairRDD.saveAsHadoopDataset(jobConf); |
| `saveAsNewAPIHadoopDataset` | Configuration: conf | `Unit` | 使用新版Hadoop API保存RDD到Hadoop输出格式 | // saveAsNewAPIHadoopDataset：新版Hadoop API保存<br>Configuration conf = new Configuration();<br>conf.set("mapreduce.output.fileoutputformat.outputdir", "/output");<br>pairRDD.saveAsNewAPIHadoopDataset(conf); |
| `setName` | name: String | `JavaPairRDD` | 设置PairRDD名称，用于调试和Spark UI显示 | // setName：设置名称便于调试<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>pairRDD.setName("user-clicks-pair-rdd");<br>// Spark UI中显示此名称 |
| `sortByKey` | 无 | `JavaPairRDD` | 按Key升序排序，使用默认分区数 | // sortByKey()：默认升序排序<br>List<Tuple2<String, Integer>> data = Arrays.asList(<br>    new Tuple2<>("c", 3), new Tuple2<>("a", 1), new Tuple2<>("b", 2));<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);<br>JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey();<br>// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `sortByKey` | ascending: Boolean | `JavaPairRDD` | 按Key排序，控制升序或降序 | // sortByKey(ascending)：控制排序方向<br>JavaPairRDD<String, Integer> sortedAsc = pairRDD.sortByKey(true);  // 升序<br>JavaPairRDD<String, Integer> sortedDesc = pairRDD.sortByKey(false);  // 降序 |
| `sortByKey` | ascending: Boolean, numPartitions: Int | `JavaPairRDD` | 按Key排序，控制排序方向和分区数 | // sortByKey(ascending, numPartitions)：指定分区数<br>JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(true, 4);<br>// 升序，使用4个分区 |
| `sortByKey` | Comparator[K]: comp | `JavaPairRDD` | 按Key排序，使用自定义比较器 | // sortByKey(comp)：自定义比较器<br>JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(Comparator.reverseOrder());<br>// 使用降序比较器 |
| `sortByKey` | Comparator[K]: comp, ascending: Boolean | `JavaPairRDD` | 按Key排序，使用自定义比较器和排序方向 | // sortByKey(comp, ascending)：比较器+方向<br>Comparator<String> customComp = (a, b) -> b.compareTo(a);<br>JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(customComp, true); |
| `sortByKey` | Comparator[K]: comp, ascending: Boolean, numPartitions: Int | `JavaPairRDD` | 按Key排序，完全自定义：比较器、方向和分区数 | // sortByKey(comp, ascending, numPartitions)：完全自定义<br>JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(customComp, true, 4); |
| `subtract` | JavaPairRDD[K: other | `JavaPairRDD` | 返回PairRDD差集（Key+Value都需匹配），使用默认分区 | // subtract(other)：PairRDD差集<br>List<Tuple2<String, Integer>> data1 = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("b", 2));<br>List<Tuple2<String, Integer>> data2 = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("c", 3));<br>JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);<br>JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);<br>JavaPairRDD<String, Integer> diff = rdd1.subtract(rdd2);<br>// 结果: [("b", 2)] |
| `subtract` | JavaPairRDD[K: other, numPartitions: Int | `JavaPairRDD` | 返回PairRDD差集，指定分区数 | // subtract(other, numPartitions)<br>JavaPairRDD<String, Integer> diff = rdd1.subtract(rdd2, 4); |
| `subtract` | JavaPairRDD[K: other, Partitioner: p | `JavaPairRDD` | 返回PairRDD差集，使用自定义分区器 | // subtract(other, partitioner)<br>JavaPairRDD<String, Integer> diff = rdd1.subtract(rdd2, new HashPartitioner(2)); |
| `union` | JavaPairRDD[K: other | `JavaPairRDD` | 合并两个PairRDD，保留所有键值对（含重复） | // union：合并PairRDD<br>List<Tuple2<String, Integer>> data1 = Arrays.asList(<br>    new Tuple2<>("a", 1), new Tuple2<>("a", 1));<br>List<Tuple2<String, Integer>> data2 = Arrays.asList(<br>    new Tuple2<>("b", 2));<br>JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);<br>JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);<br>JavaPairRDD<String, Integer> unionRDD = rdd1.union(rdd2);<br>// 结果: [("a", 1), ("a", 1), ("b", 2)] 保留重复 |
| `unpersist` | 无 | `JavaPairRDD` | 取消PairRDD持久化，非阻塞方式 | // unpersist()：非阻塞释放<br>pairRDD.cache();<br>pairRDD.unpersist(); |
| `unpersist` | blocking: Boolean | `JavaPairRDD` | 取消PairRDD持久化，可阻塞等待 | // unpersist(blocking)<br>pairRDD.unpersist(true);  // 阻塞等待完成 |
| `values` | 无 | `JavaRDD` | 返回所有Value的RDD | // values：获取所有Value<br>JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(<br>    new Tuple2<>("a", 1),<br>    new Tuple2<>("b", 2)<br>));<br>JavaRDD<Integer> valuesRDD = pairRDD.values();<br>// 结果: [1, 2] |

| `aggregateByKey` | U zeroValue, JFunction2[U, V, U] seqFunc, JFunction2[U, U, U] combFunc | `JavaPairRDD[K, U]` | 按Key聚合，支持不同类型 | `JavaPairRDD<String, Integer> result = pairRdd.aggregateByKey(0, (a, b) -> a + b, (a, b) -> a + b);` |
| `combineByKey` | JFunction[V, C] createCombiner, JFunction2[C, V, C] mergeValue, JFunction2[C, C, C] mergeCombiners | `JavaPairRDD[K, C]` | 通用组合函数 | `JavaPairRDD<String, Integer> combined = pairRdd.combineByKey(v -> v, (a, b) -> a + b, (a, b) -> a + b);` |
| `combineByKeyWithClassTag` | JFunction[V, C] createCombiner, JFunction2[C, V, C] mergeValue, JFunction2[C, C, C] mergeCombiners, ClassTag[C] ct | `JavaPairRDD[K, C]` | 带ClassTag的组合函数 | `JavaPairRDD<String, Integer> combined = pairRdd.combineByKeyWithClassTag(v -> v, (a, b) -> a + b, (a, b) -> a + b, ClassTag.apply(Integer.class));` |
| `subtractByKey` | JavaPairRDD[K, W] other | `JavaPairRDD[K, V]` | 减去other中存在的key | `JavaPairRDD<String, Integer> result = pairRdd.subtractByKey(otherRdd);` |
| `sampleStdevByKey` | K key | `double` | 按key采样标准差 | `double stdev = pairRdd.sampleStdevByKey("key1");` |
| `sampleVarianceByKey` | K key | `double` | 按key采样方差 | `double variance = pairRdd.sampleVarianceByKey("key1");` |
| `stdevByKey` | K key | `double` | 按key标准差 | `double stdev = pairRdd.stdevByKey("key1");` |
| `varianceByKey` | K key | `double` | 按key方差 | `double variance = pairRdd.varianceByKey("key1");` |
| `mapPartitionsByKey` | JFunction[Iterator[T], Iterator[U]] f | `JavaPairRDD[K, U]` | 按分区处理 | `pairRdd.mapPartitionsByKey(iter -> process(iter));` |
| `flatMapValuesWithKey` | FlatMapFunction[K, V, U] f | `JavaPairRDD[K, U]` | 带key的flatMapValues | `pairRdd.flatMapValuesWithKey((k, v) -> {...});` |

### JavaRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 22

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaRDD` | 缓存RDD到内存，默认MEMORY_ONLY | // cache：缓存RDD到内存<br>JavaRDD<String> rdd = sc.textFile("hdfs://large/file.txt");<br>// 缓存后，后续操作会直接从内存读取<br>rdd.cache();<br>// 多次使用RDD时缓存可提升性能<br>long count1 = rdd.count();  // 第一次计算，会缓存<br>long count2 = rdd.count();  // 第二次直接从内存读取 |
| `coalesce` | numPartitions: Int | `JavaRDD` | 减少分区数，不触发shuffle | // coalesce：减少分区数（不shuffle）<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);  // 10个分区<br>// 减少到2个分区（不触发shuffle，高效）<br>JavaRDD<String> coalesced = rdd.coalesce(2); |
| `coalesce` | numPartitions: Int, shuffle: Boolean | `JavaRDD` | 减少分区数，不触发shuffle | // coalesce：减少分区数（不shuffle）<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);  // 10个分区<br>// 减少到2个分区（不触发shuffle，高效）<br>JavaRDD<String> coalesced = rdd.coalesce(2); |
| `distinct` | 无 | `JavaRDD` | 去重 | // distinct：去重<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));<br>JavaRDD<Integer> distinct = rdd.distinct();<br>// 结果: [1, 2, 3, 4, 5] |
| `distinct` | numPartitions: Int | `JavaRDD` | 去重 | // distinct：去重<br>JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));<br>JavaRDD<Integer> distinct = rdd.distinct();<br>// 结果: [1, 2, 3, 4, 5] |
| `filter` | JFunction[T: f | `JavaRDD` | 过滤行 | // 过滤满足条件的元素<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 过滤大于5的数<br>JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);<br>// 结果: [6, 7, 8, 9, 10]<br>// 过滤偶数<br>JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);<br>// 结果: [2, 4, 6, 8, 10] |
| `getResourceProfile` | 无 | `ResourceProfile` | 获取RDD的资源配置，用于资源隔离管理 | // getResourceProfile：获取资源配置<br>JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b"));<br>ResourceProfile profile = rdd.getResourceProfile();<br>// 查看RDD使用的CPU/内存资源配置 |
| `intersection` | JavaRDD[T]: other | `JavaRDD` | 返回两个RDD的交集 | // intersection：取交集<br>JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));<br>JavaRDD<Integer> intersection = rdd1.intersection(rdd2);<br>// 结果: [3, 4] |
| `persist` | StorageLevel: newLevel | `JavaRDD` | 持久化RDD到指定存储级别 | // persist：持久化到指定存储级别<br>JavaRDD<String> rdd = sc.textFile("hdfs://data/file.txt");<br>// 内存+磁盘持久化<br>rdd.persist(StorageLevel.MEMORY_AND_DISK());<br>// 序列化存储（节省空间）<br>rdd.persist(StorageLevel.MEMORY_ONLY_SER());<br>// 堆外内存存储<br>rdd.persist(StorageLevel.OFF_HEAP()); |
| `randomSplit` | Array[Double]: weights | `Array` | 按权重随机分割RDD为多个RDD，用于数据集划分（如训练/测试） | // randomSplit(weights)：按权重随机分割<br>JavaRDD<Integer> data = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>// 60%训练集，40%测试集<br>JavaRDD<Integer>[] splits = data.randomSplit(new double[]{0.6, 0.4});<br>JavaRDD<Integer> trainData = splits[0];<br>JavaRDD<Integer> testData = splits[1]; |
| `randomSplit` | Array[Double]: weights, seed: Long | `Array` | 按权重随机分割，指定种子确保每次分割结果一致 | // randomSplit(weights, seed)：可重现分割<br>JavaRDD<Integer>[] splits1 = data.randomSplit(new double[]{0.6, 0.4}, 42L);<br>JavaRDD<Integer>[] splits2 = data.randomSplit(new double[]{0.6, 0.4}, 42L);<br>// splits1和splits2分割结果完全相同 |
| `setName` | name: String | `JavaRDD` | 设置RDD名称，用于Spark UI调试和监控 | // setName：设置RDD名称<br>JavaRDD<String> rdd = sc.textFile("hdfs://data.txt");<br>rdd.setName("raw-input-data");<br>// 在Spark UI中显示此名称便于调试 |
| `withResources` | ResourceProfile: rp | `JavaRDD` | 设置RDD的资源配置，用于资源隔离和精细控制 | // withResources：设置资源配置<br>ResourceProfile profile = new ResourceProfileBuilder().requireCores(2).build();<br>JavaRDD<String> rddWithResources = rdd.withResources(profile);<br>// 此RDD执行时使用指定的资源 |

### JavaRDDLike (核心接口)
**包路径**: `org.apache.spark.api.java`
**说明**: JavaRDD、JavaPairRDD、JavaDoubleRDD共同继承的接口，包含最常用的RDD操作方法。
**方法数量**: 50

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `map` | JFunction[T, R] f | `JavaRDD[R]` | 对每个元素应用函数，一对一转换 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>JavaRDD<Integer> doubled = nums.map(x -> x * 2);<br>// 结果: [2, 4, 6]` |
| `mapToPair` | PairFunction[T, K, V] f | `JavaPairRDD[K, V]` | 将元素转换为键值对 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("apple", "banana"));<br>JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, w.length()));<br>// 结果: [("apple", 5), ("banana", 6)]` |
| `mapToDouble` | DoubleFunction[T] f | `JavaDoubleRDD` | 将元素转换为Double值 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>JavaDoubleRDD sqrt = nums.mapToDouble(x -> Math.sqrt(x));` |
| `flatMap` | FlatMapFunction[T, U] f | `JavaRDD[U]` | 将每个元素映射为多个输出元素 | `JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "spark java"));<br>JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());<br>// 结果: ["hello", "world", "spark", "java"]` |
| `flatMapToPair` | PairFlatMapFunction[T, K, V] f | `JavaPairRDD[K, V]` | 将每个元素映射为多个键值对 | `JavaRDD<String> lines = sc.parallelize(Arrays.asList("a b", "c d"));<br>JavaPairRDD<String, Integer> pairs = lines.flatMapToPair(line -> {<br>    List<Tuple2<String, Integer>> result = new ArrayList<>();<br>    for (String w : line.split(" ")) {<br>        result.add(new Tuple2<>(w, 1));<br>    }<br>    return result.iterator();<br>});` |
| `mapPartitions` | FlatMapFunction[JIterator[T], U] f | `JavaRDD[U]` | 对每个分区应用函数，适合批量处理 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);<br>JavaRDD<Integer> partitionSum = nums.mapPartitions(iter -> {<br>    int sum = 0;<br>    while (iter.hasNext()) sum += iter.next();<br>    return Arrays.asList(sum).iterator();<br>});<br>// 结果: [3, 7] (分区0:1+2=3, 分区1:3+4=7)` |
| `mapPartitionsWithIndex` | JFunction2[Integer, JIterator[T], JIterator[R]] f | `JavaRDD[R]` | 对每个分区应用函数，带分区索引 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);<br>JavaRDD<String> indexed = nums.mapPartitionsWithIndex((idx, iter) -> {<br>    List<String> result = new ArrayList<>();<br>    while (iter.hasNext()) result.add("Partition " + idx + ": " + iter.next());<br>    return result.iterator();<br>});` |
| `glom` | 无 | `JavaRDD[JList[T]]` | 将每个分区的元素合并为一个List | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);<br>JavaRDD<List<Integer>> partitions = nums.glom();<br>// 结果: [[1, 2], [3, 4]]` |
| `collect` | 无 | `JList[T]` | 将RDD所有元素收集到Driver端，返回Java List | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>List<Integer> list = nums.collect();<br>// 注意：数据量大时可能导致Driver内存溢出` |
| `collectPartitions` | Array[Int] partitionIds | `Array[JList[T]]` | 收集指定分区的元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3);<br>List<Integer>[] parts = nums.collectPartitions(new int[]{0, 2});<br>// 只收集分区0和分区2` |
| `toLocalIterator` | 无 | `JIterator[T]` | 返回本地迭代器，逐分区拉取数据 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));<br>Iterator<Integer> iter = nums.toLocalIterator();<br>while (iter.hasNext()) {<br>    System.out.println(iter.next());<br>}` |
| `foreach` | VoidFunction[T] f | `Unit` | 对每个元素执行操作（不返回结果） | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>nums.foreach(x -> System.out.println("Value: " + x));` |
| `foreachPartition` | VoidFunction[JIterator[T]] f | `Unit` | 对每个分区的迭代器执行操作 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);<br>nums.foreachPartition(iter -> {<br>    int sum = 0;<br>    while (iter.hasNext()) sum += iter.next();<br>    System.out.println("Partition sum: " + sum);<br>});` |
| `reduce` | JFunction2[T, T, T] f | `T` | 使用函数聚合所有元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>int sum = nums.reduce((a, b) -> a + b);<br>// 结果: 10` |
| `treeReduce` | JFunction2[T, T, T] f, depth: Int | `T` | 树形聚合，减少Driver负载 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));<br>int sum = nums.treeReduce((a, b) -> a + b, 2);<br>// 深度为2的树形聚合` |
| `fold` | zeroValue: T, JFunction2[T, T, T] f | `T` | 使用零值进行聚合 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>int sum = nums.fold(0, (a, b) -> a + b);<br>// 结果: 6` |
| `aggregate` | zeroValue: U, seqOp: JFunction2[U, T, U], combOp: JFunction2[U, U, U] | `U` | 使用不同类型的零值进行聚合 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>Tuple2<Integer, Integer> result = nums.aggregate(<br>    new Tuple2<>(0, 0),  // (sum, count)<br>    (acc, x) -> new Tuple2<>(acc._1 + x, acc._2 + 1),  // 分区内聚合<br>    (acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2)  // 分区间合并<br>);<br>// 结果: (10, 4)` |
| `treeAggregate` | zeroValue: U, seqOp, combOp, depth: Int | `U` | 树形聚合，减少Driver内存压力 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));<br>int sum = nums.treeAggregate(0, (a, b) -> a + b, (a, b) -> a + b, 2);` |
| `count` | 无 | `Long` | 计算元素总数 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4));<br>long total = nums.count();<br>// 结果: 4` |
| `countApprox` | timeout: Long, confidence: Double | `PartialResult[BoundedDouble]` | 近似计数，在超时内返回带置信区间结果 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));<br>PartialResult<BoundedDouble> approx = nums.countApprox(1000, 0.95);` |
| `countByValue` | 无 | `JMap[T, jl.Long]` | 统计每个值的出现次数 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 1));<br>Map<Integer, Long> counts = nums.countByValue();<br>// 结果: {1: 3, 2: 2, 3: 1}` |
| `countApproxDistinct` | relativeSD: Double | `Long` | 近似统计唯一值数量（HyperLogLog算法） | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 5, 6, 7));<br>long distinct = nums.countApproxDistinct(0.05);<br>// relativeSD=0.05表示5%误差率` |
| `take` | num: Int | `JList[T]` | 获取前n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));<br>List<Integer> first3 = nums.take(3);<br>// 结果: [1, 2, 3]` |
| `takeSample` | withReplacement: Boolean, num: Int, seed: Long | `JList[T]` | 随机采样n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));<br>List<Integer> sample = nums.takeSample(false, 3, 42L);<br>// 不重复采样3个元素` |
| `first` | 无 | `T` | 获取第一个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>int first = nums.first();<br>// 结果: 1` |
| `top` | num: Int, comp: Comparator[T] | `JList[T]` | 获取最大的n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 5, 3, 8, 2));<br>List<Integer> top3 = nums.top(3, Comparator.naturalOrder());<br>// 结果: [8, 5, 3]` |
| `takeOrdered` | num: Int, comp: Comparator[T] | `JList[T]` | 获取最小的n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(5, 1, 3, 8, 2));<br>List<Integer> smallest3 = nums.takeOrdered(3, Comparator.naturalOrder());<br>// 结果: [1, 2, 3]` |
| `max` | comp: Comparator[T] | `T` | 获取最大元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 5, 3, 8, 2));<br>int max = nums.max(Comparator.naturalOrder());<br>// 结果: 8` |
| `min` | comp: Comparator[T] | `T` | 获取最小元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(5, 1, 3, 8, 2));<br>int min = nums.min(Comparator.naturalOrder());<br>// 结果: 1` |
| `isEmpty` | 无 | `Boolean` | 判断RDD是否为空 | `JavaRDD<Integer> empty = sc.emptyRDD();<br>boolean emptyFlag = empty.isEmpty();<br>// 结果: true` |
| `groupBy` | JFunction[T, U] f | `JavaPairRDD[U, JIterable[T]]` | 按函数结果分组 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));<br>JavaPairRDD<Boolean, Iterable<Integer>> grouped = nums.groupBy(x -> x % 2 == 0);<br>// 结果: {false: [1, 3, 5], true: [2, 4, 6]}` |
| `keyBy` | JFunction[T, U] f | `JavaPairRDD[U, T]` | 将元素转换为键值对，原值为Value | `JavaRDD<String> words = sc.parallelize(Arrays.asList("apple", "banana"));<br>JavaPairRDD<Integer, String> keyed = words.keyBy(w -> w.length());<br>// 结果: [(5, "apple"), (6, "banana")]` |
| `cartesian` | JavaRDDLike[U, _] other | `JavaPairRDD[T, U]` | 计算两个RDD的笛卡尔积 | `JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2));<br>JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b"));<br>JavaPairRDD<Integer, String> cartesian = rdd1.cartesian(rdd2);<br>// 结果: [(1, "a"), (1, "b"), (2, "a"), (2, "b")]` |
| `zip` | JavaRDDLike[U, _] other | `JavaPairRDD[T, U]` | 将两个RDD按位置配对 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>JavaRDD<String> chars = sc.parallelize(Arrays.asList("a", "b", "c"));<br>JavaPairRDD<Integer, String> zipped = nums.zip(chars);<br>// 结果: [(1, "a"), (2, "b"), (3, "c")]` |
| `zipPartitions` | JavaRDDLike[U, _] other, FlatMapFunction2[JIterator[T], JIterator[U], V] f | `JavaRDD[V]` | 对两个RDD的分区进行配对处理 | `JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2), 1);<br>JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(10, 20), 1);<br>JavaRDD<Integer> sums = rdd1.zipPartitions(rdd2, (iter1, iter2) -> {<br>    List<Integer> result = new ArrayList<>();<br>    while (iter1.hasNext() && iter2.hasNext()) {<br>        result.add(iter1.next() + iter2.next());<br>    }<br>    return result.iterator();<br>});` |
| `zipWithIndex` | 无 | `JavaPairRDD[T, jl.Long]` | 为每个元素添加索引 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));<br>JavaPairRDD<String, Long> indexed = words.zipWithIndex();<br>// 结果: [("a", 0), ("b", 1), ("c", 2)]` |
| `zipWithUniqueId` | 无 | `JavaPairRDD[T, jl.Long]` | 为每个元素生成唯一ID（不保证连续） | `JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"), 2);<br>JavaPairRDD<String, Long> uid = words.zipWithUniqueId();<br>// 结果如: [("a", 0), ("b", 1), ("c", 4)]` |
| `pipe` | command: String | `JavaRDD[String]` | 通过外部程序处理RDD元素 | `JavaRDD<String> data = sc.parallelize(Arrays.asList("1", "2", "3"));<br>JavaRDD<String> piped = data.pipe("cat");<br>// 将每个元素通过cat命令处理` |
| `pipe` | JList[String] command, JMap[String, String] env | `JavaRDD[String]` | 通过外部程序处理，带环境变量 | `List<String> cmd = Arrays.asList("awk", "{print $1*2}");<br>Map<String, String> env = new HashMap<>();<br>env.put("LC_ALL", "C");<br>JavaRDD<String> result = data.pipe(cmd, env);` |
| `saveAsTextFile` | path: String | `Unit` | 保存RDD为文本文件 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("hello", "world"));<br>words.saveAsTextFile("hdfs://output/path");` |
| `saveAsTextFile` | path: String, codec: Class[_ <: CompressionCodec] | `Unit` | 保存为压缩文本文件 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("hello", "world"));<br>words.saveAsTextFile("hdfs://output/path", GzipCodec.class);` |
| `saveAsObjectFile` | path: String | `Unit` | 保存为序列化对象文件 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>nums.saveAsObjectFile("hdfs://output/nums");` |
| `checkpoint` | 无 | `Unit` | 标记RDD进行checkpoint | `sc.setCheckpointDir("hdfs://checkpoint/dir");<br>JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));<br>nums.checkpoint();  // 后续计算会保存到checkpoint目录` |
| `isCheckpointed` | 无 | `Boolean` | 判断是否已checkpoint | `boolean checked = nums.isCheckpointed();` |
| `getCheckpointFile` | 无 | `Optional[String]` | 获取checkpoint文件路径 | `Optional<String> file = nums.getCheckpointFile();` |
| `getNumPartitions` | 无 | `Int` | 获取分区数 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 4);<br>int partitions = nums.getNumPartitions();<br>// 结果: 4` |
| `partitions` | 无 | `JList[Partition]` | 获取所有分区对象 | `List<Partition> parts = nums.partitions();` |
| `partitioner` | 无 | `Optional[Partitioner]` | 获取分区器（如果有） | `JavaPairRDD<String, Integer> pairs = ...;<br>Optional<Partitioner> partitioner = pairs.partitioner();` |
| `id` | 无 | `Int` | 获取RDD唯一ID | `int rddId = nums.id();` |
| `name` | 无 | `String` | 获取RDD名称 | `String rddName = nums.name();` |
| `getStorageLevel` | 无 | `StorageLevel` | 获取当前存储级别 | `StorageLevel level = nums.getStorageLevel();` |
| `toDebugString` | 无 | `String` | 获取RDD的血缘关系字符串 | `String lineage = nums.toDebugString();<br>// 显示RDD如何从父RDD计算而来` |
| `context` | 无 | `SparkContext` | 获取SparkContext | `SparkContext sc = nums.context();` |
| `rdd` | 无 | `RDD[T]` | 获取底层Scala RDD | `RDD<Integer> scalaRdd = nums.rdd();` |
| `countAsync` | 无 | `JavaFutureAction[jl.Long]` | 异步计数 | `JavaFutureAction<Long> future = nums.countAsync();<br>Long count = future.get();  // 阻塞等待结果` |
| `collectAsync` | 无 | `JavaFutureAction[JList[T]]` | 异步collect | `JavaFutureAction<List<Integer>> future = nums.collectAsync();<br>List<Integer> result = future.get();` |
| `takeAsync` | num: Int | `JavaFutureAction[JList[T]]` | 异步take | `JavaFutureAction<List<Integer>> future = nums.takeAsync(5);<br>List<Integer> first5 = future.get();` |
| `foreachAsync` | VoidFunction[T] f | `JavaFutureAction[Void]` | 异步foreach | `JavaFutureAction<Void> future = nums.foreachAsync(x -> System.out.println(x));<br>future.get();  // 等待完成` |

| `mapPartitionsToPair` | FlatMapFunction[T, K, V] f | `JavaPairRDD[K, V]` | 对每个分区映射为键值对 | `JavaPairRDD<String, Integer> pairs = rdd.mapPartitionsToPair(iter -> {...});` |
| `wrapRDD` | RDD[T] rdd | `JavaRDD[T]` | 将Scala RDD包装为Java RDD | `JavaRDD<String> javaRdd = JavaRDD.fromRDD(scalaRdd);` |

### JavaSparkContext
**包路径**: `org.apache.spark.api.java`
**方法数量**: 45

**补充说明**: 以下属性getter方法很重要但常被忽略：

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultMinPartitions` | 无 | `Integer` | 默认最小分区数 | `int min = sc.defaultMinPartitions();` |
| `defaultParallelism` | 无 | `Integer` | 默认并行度 | `int para = sc.defaultParallelism();` |
| `emptyRDD` | 无 | `JavaRDD[T]` | 创建空RDD | `JavaRDD<String> empty = sc.emptyRDD();` |
| `getCheckpointDir` | 无 | `Optional<String>` | checkpoint目录 | `Optional<String> dir = sc.getCheckpointDir();` |
| `getPersistentRDDs` | 无 | `Map<Integer, JavaRDD<?>>` | 持久化RDD列表 | `Map<Integer, JavaRDD<?>> rdds = sc.getPersistentRDDs();` |
| `getReadOnlyConf` | 无 | `ReadOnlySparkConf` | 只读配置 | `ReadOnlySparkConf conf = sc.getReadOnlyConf();` |
| `isLocal` | 无 | `Boolean` | 是否本地模式 | `boolean local = sc.isLocal();` |
| `jars` | 无 | `List<String>` | JAR包列表 | `List<String> jars = sc.jars();` |
| `resources` | 无 | `Map<String, ResourceInformation>` | 资源配置 | `Map<String, ResourceInformation> res = sc.resources();` |
| `sparkUser` | 无 | `String` | Spark用户名 | `String user = sc.sparkUser();` |
| `statusTracker` | 无 | `JavaSparkStatusTracker` | 状态追踪器 | `JavaSparkStatusTracker tracker = sc.statusTracker();` |

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addFile` | path: String | `Unit` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业<br>sc.addFile("hdfs://path/to/config.txt");<br>sc.addFile("s3://bucket/data.json");<br>// 在Executor中访问文件<br>String filePath = SparkFiles.get("config.txt"); |
| `addFile` | path: String, recursive: Boolean | `Unit` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业<br>sc.addFile("hdfs://path/to/config.txt");<br>sc.addFile("s3://bucket/data.json");<br>// 在Executor中访问文件<br>String filePath = SparkFiles.get("config.txt"); |
| `addJar` | path: String | `Unit` | 添加JAR包到Spark作业 | // 添加依赖JAR包<br>sc.addJar("hdfs://path/to/dependency.jar");<br>sc.addJar("/local/path/to/lib.jar"); |
| `addJobTag` | tag: String | `Unit` | 为当前作业添加标签，用于作业追踪和取消 | // addJobTag：添加作业标签<br>sc.addJobTag("ml-training");<br>// 可以通过标签取消相关作业<br>sc.cancelJobsWithTag("ml-training"); |
| `binaryFiles` | path: String, minPartitions: Int | `JavaPairRDD` | 读取二进制文件目录，返回(文件路径,PortableDataStream)，指定最小分区数 | // binaryFiles(path, minPartitions)：指定分区数<br>JavaPairRDD<String, PortableDataStream> binaryRDD = sc.binaryFiles("hdfs://images/", 10);<br>JavaRDD<byte[]> dataRDD = binaryRDD.map(tuple -> tuple._2().toArray()); |
| `binaryFiles` | path: String | `JavaPairRDD` | 读取二进制文件目录，返回(文件路径,PortableDataStream)，使用默认分区 | // binaryFiles(path)：读取二进制文件<br>JavaPairRDD<String, PortableDataStream> binaryRDD = sc.binaryFiles("hdfs://binary/dir/");<br>// 适合处理图片、视频等二进制数据 |
| `binaryRecords` | path: String, recordLength: Int | `JavaRDD` | 读取固定长度的二进制记录文件，每条记录为byte[] | // binaryRecords：读取固定长度二进制记录<br>// 每条记录固定100字节<br>JavaRDD<byte[]> records = sc.binaryRecords("hdfs://data.bin", 100);<br>// 适合处理固定格式的二进制数据 |
| `cancelAllJobs` | 无 | `Unit` | 取消所有正在运行的作业，紧急停止使用 | // cancelAllJobs：取消所有作业<br>sc.cancelAllJobs();<br>// 立即取消所有正在执行的任务 |
| `cancelJobGroup` | groupId: String, reason: String | `Unit` | 取消指定作业组，并指定取消原因 | // cancelJobGroup(groupId, reason)<br>sc.setJobGroup("etl-job", "ETL processing");<br>sc.cancelJobGroup("etl-job", "User requested stop"); |
| `cancelJobGroup` | groupId: String | `Unit` | 取消指定作业组，不指定原因 | // cancelJobGroup(groupId)<br>sc.cancelJobGroup("etl-job"); |
| `cancelJobsWithTag` | tag: String, reason: String | `Unit` | 取消带指定标签的作业，并说明原因 | // cancelJobsWithTag(tag, reason)<br>sc.addJobTag("batch-processing");<br>sc.cancelJobsWithTag("batch-processing", "Resource limit reached"); |
| `cancelJobsWithTag` | tag: String | `Unit` | 取消带指定标签的作业 | // cancelJobsWithTag(tag)<br>sc.cancelJobsWithTag("batch-processing"); |
| `clearCallSite` | 无 | `Unit` | 清除调用点信息，用于调试追踪 | // clearCallSite：清除调用点<br>sc.clearCallSite();<br>// 清除Spark UI中显示的调用位置信息 |
| `clearJobGroup` | 无 | `Unit` | 清除当前作业组设置 | // clearJobGroup：清除作业组<br>sc.clearJobGroup();<br>// 后续作业不再属于之前设置的作业组 |
| `clearJobTags` | 无 | `Unit` | 清除所有作业标签 | // clearJobTags：清除所有标签<br>sc.clearJobTags(); |
| `getJobTags` | 无 | `util` | 获取当前作业的所有标签 | // getJobTags：获取作业标签<br>Set<String> tags = sc.getJobTags(); |
| `getLocalProperty` | key: String | `String` | 获取本地线程属性，用于任务上下文传递 | // getLocalProperty：获取本地属性<br>String value = sc.getLocalProperty("spark.task.queue"); |
| `getSparkHome` | 无 | `Optional` | 获取Spark安装目录路径 | // getSparkHome：获取Spark安装目录<br>Optional<String> sparkHome = sc.getSparkHome();<br>if (sparkHome.isPresent()) {<br>    System.out.println("Spark home: " + sparkHome.get());<br>} |
| `hadoopConfiguration` | 无 | `Configuration` | 获取Hadoop配置对象，用于访问HDFS设置 | // hadoopConfiguration：获取Hadoop配置<br>Configuration hadoopConf = sc.hadoopConfiguration();<br>hadoopConf.set("dfs.replication", "3");<br>// 直接修改Hadoop配置 |
| `jarOfClass` | Class[_]: cls | `Array` | 获取包含指定类的JAR路径，用于依赖分发 | // jarOfClass：获取类所在JAR<br>String[] jars = sc.jarOfClass(MyApp.class);<br>// 用于自动分发包含主类的JAR |
| `jarOfObject` | AnyRef: obj | `Array` | 获取包含指定对象的JAR路径 | // jarOfObject：获取对象所在JAR<br>String[] jars = sc.jarOfObject(new MyClass()); |
| `parallelizeDoubles` | java.util.List[java.lang.Double]: list, numSlices: Int | `JavaDoubleRDD` | 从Java List创建DoubleRDD，指定分区数 | // parallelizeDoubles(list, numSlices)：指定分区数<br>List<Double> doubles = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(doubles, 3);<br>// 使用3个分区 |
| `parallelizeDoubles` | java.util.List[java.lang.Double]: list | `JavaDoubleRDD` | 从Java List创建DoubleRDD，使用默认分区 | // parallelizeDoubles(list)：创建DoubleRDD<br>JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(doubles);<br>double mean = doubleRDD.mean();<br>double sum = doubleRDD.sum(); |
| `removeJobTag` | tag: String | `Unit` | 移除指定作业标签 | // removeJobTag：移除标签<br>sc.addJobTag("batch");<br>sc.removeJobTag("batch"); |
| `setCallSite` | site: String | `Unit` | 设置调用点信息，用于调试追踪 | // setCallSite：设置调用点<br>sc.setCallSite("MyApp.main:数据处理阶段");<br>// Spark UI会显示此信息 |
| `setInterruptOnCancel` | interruptOnCancel: Boolean | `Unit` | 设置取消作业时是否中断线程 | // setInterruptOnCancel：设置取消中断行为<br>sc.setInterruptOnCancel(true);<br>// 取消作业时中断任务线程 |
| `setJobDescription` | value: String | `Unit` | 设置作业描述，用于Spark UI显示 | // setJobDescription：设置作业描述<br>sc.setJobDescription("Daily ETL batch job");<br>// Spark UI中显示作业描述 |
| `setJobGroup` | groupId: String, description: String, interruptOnCancel: Boolean | `Unit` | 设置作业组，指定ID、描述和取消中断行为 | // setJobGroup(id, desc, interrupt)<br>sc.setJobGroup("etl-group", "ETL processing", true);<br>// 可以通过groupId取消整组作业 |
| `setJobGroup` | groupId: String, description: String | `Unit` | 设置作业组，不指定中断行为 | // setJobGroup(id, desc)<br>sc.setJobGroup("ml-training", "ML model training"); |
| `setLocalProperty` | key: String, value: String | `Unit` | 设置本地线程属性，用于任务上下文传递 | // setLocalProperty：设置本地属性<br>sc.setLocalProperty("spark.task.queue", "high-priority");<br>// 任务可以读取此属性 |
| `setLogLevel` | logLevel: String | `Unit` | 设置SparkContext日志级别（ALL, DEBUG, INFO, WARN, ERROR, OFF） | // setLogLevel：设置日志级别<br>sc.setLogLevel("WARN");  // 只显示警告和错误<br>sc.setLogLevel("INFO");  // 显示信息级别 |
| `stop` | 无 | `Unit` | 停止SparkContext，释放所有资源，正常退出 | // stop()：正常停止<br>sc.stop();<br>// 释放所有资源，关闭连接 |
| `stop` | exitCode: Int | `Unit` | 停止SparkContext并指定退出码，用于异常退出 | // stop(exitCode)：指定退出码<br>sc.stop(1);  // 异常退出<br>sc.stop(0);  // 正常退出 |
| `textFile` | path: String | `JavaRDD` | 从文件系统读取文本文件，每行一条记录，支持通配符 | // textFile(path)：读取文本文件<br>JavaRDD<String> lines = sc.textFile("hdfs://path/file.txt");<br>JavaRDD<String> multiFiles = sc.textFile("hdfs://path/*.txt");  // 通配符 |
| `textFile` | path: String, minPartitions: Int | `JavaRDD` | 从文件系统读取文本文件，指定最小分区数 | // textFile(path, minPartitions)：指定分区数<br>JavaRDD<String> lines = sc.textFile("hdfs://path/file.txt", 10);<br>// 至少10个分区 |
| `wholeTextFiles` | path: String, minPartitions: Int | `JavaPairRDD` | 读取目录下所有文本文件，返回(路径,完整内容)，指定分区数 | // wholeTextFiles(path, minPartitions)：指定分区<br>JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles("hdfs://docs/", 10);<br>// 每个文件完整内容作为一个元素 |
| `wholeTextFiles` | path: String | `JavaPairRDD` | 读取目录下所有文本文件，返回(路径,完整内容)，适合小文件处理 | // wholeTextFiles(path)：读取所有文件<br>JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles("hdfs://docs/");<br>filesRDD.foreach(tuple -> System.out.println(tuple._1())); |

---

## MLlib机器学习

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 关联规则挖掘示例，从交易数据中发现频繁项集和关联规则 | // 运行: spark-submit --class JavaAssociationRulesExample target/spark-examples.jar<br>// 输入: 交易数据集，输出: 满足置信度的关联规则 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 二分类评估指标示例，计算AUC、精确率、召回率、F1等指标 | // 运行: spark-submit --class JavaBinaryClassificationMetricsExample target/spark-examples.jar<br>// 输入: 预测结果和真实标签，输出: ROC曲线、PR曲线等评估指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 二分K-Means聚类示例，层次聚类算法，自顶向下分裂 | // 运行: spark-submit --class JavaBisectingKMeansExample target/spark-examples.jar<br>// 输入: 向量数据集，输出: 聚类中心和分配结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 卡方检验特征选择示例，选择与标签最相关的特征 | // 运行: spark-submit --class JavaChiSqSelectorExample target/spark-examples.jar<br>// 输入: 特征向量和标签，输出: 选定的特征索引 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 相关性计算示例，计算Pearson和Spearman相关系数 | // 运行: spark-submit --class JavaCorrelationsExample target/spark-examples.jar<br>// 输入: 数值数据集，输出: 相关系数矩阵 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 逐元素乘积示例，向量与权重向量的逐元素加权 | // 运行: spark-submit --class JavaElementwiseProductExample target/spark-examples.jar<br>// 输入: 向量数据和权重向量，输出: 加权后的向量 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 高斯混合模型示例，概率聚类，假设数据由多个高斯分布生成 | // 运行: spark-submit --class JavaGaussianMixtureExample target/spark-examples.jar<br>// 输入: 向量数据集，输出: 混合模型参数和聚类分配 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 梯度提升分类示例，GBDT集成学习分类算法 | // 运行: spark-submit --class JavaGradientBoostingClassificationExample target/spark-examples.jar<br>// 输入: 训练数据集，输出: 分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 梯度提升回归示例，GBDT集成学习回归算法 | // 运行: spark-submit --class JavaGradientBoostingRegressionExample target/spark-examples.jar<br>// 输入: 训练数据集，输出: 回归模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 假设检验示例，统计显著性检验（卡方检验、t检验等） | // 运行: spark-submit --class JavaHypothesisTestingExample target/spark-examples.jar<br>// 输入: 样本数据，输出: 检验统计量和p值 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | Kolmogorov-Smirnov检验示例，检验样本是否服从指定分布 | // 运行: spark-submit --class JavaHypothesisTestingKolmogorovSmirnovTestExample target/spark-examples.jar<br>// 输入: 样本数据，输出: KS检验统计量和p值 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 保序回归示例，单调约束下的回归分析 | // 运行: spark-submit --class JavaIsotonicRegressionExample target/spark-examples.jar<br>// 输入: 有序数据，输出: 保序拟合结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | K-Means聚类示例，经典聚类算法，将数据划分为K个簇 | // 运行: spark-submit --class JavaKMeansExample target/spark-examples.jar<br>// 输入: 向量数据集，输出: 聚类中心和数据点分配 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 核密度估计示例，估计数据的概率密度函数 | // 运行: spark-submit --class JavaKernelDensityEstimationExample target/spark-examples.jar<br>// 输入: 样本数据，输出: 密度估计值 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | LBFGS优化示例，拟牛顿法求解大规模优化问题 | // 运行: spark-submit --class JavaLBFGSExample target/spark-examples.jar<br>// 输入: 优化问题和参数，输出: 最优解 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | LDA主题模型示例，文档主题发现和词分布估计 | // 运行: spark-submit --class JavaLatentDirichletAllocationExample target/spark-examples.jar<br>// 输入: 文档词频矩阵，输出: 主题分布和词主题分布 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | LBFGS逻辑回归示例，使用拟牛顿法优化逻辑回归 | // 运行: spark-submit --class JavaLogisticRegressionWithLBFGSExample target/spark-examples.jar<br>// 输入: 训练数据集，输出: 逻辑回归模型 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 多标签分类评估示例，计算多标签分类指标 | // 运行: spark-submit --class JavaMultiLabelClassificationMetricsExample target/spark-examples.jar<br>// 输入: 多标签预测结果，输出: 准确率、召回率等指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 多分类评估指标示例，计算混淆矩阵、准确率等 | // 运行: spark-submit --class JavaMulticlassClassificationMetricsExample target/spark-examples.jar<br>// 输入: 多分类预测结果，输出: 混淆矩阵和各项指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 朴素贝叶斯分类示例，基于概率的分类算法 | // 运行: spark-submit --class JavaNaiveBayesExample target/spark-examples.jar<br>// 输入: 训练数据集，输出: 分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | PCA降维示例，主成分分析，将高维数据降至低维 | // 运行: spark-submit --class JavaPCAExample target/spark-examples.jar<br>// 输入: 高维向量数据，输出: 降维后的向量和主成分 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 幂迭代聚类示例，基于相似度矩阵的图聚类算法 | // 运行: spark-submit --class JavaPowerIterationClusteringExample target/spark-examples.jar<br>// 输入: 相似度数据，输出: 聚类分配 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | PrefixSpan序列模式挖掘示例，发现序列数据中的频繁模式 | // 运行: spark-submit --class JavaPrefixSpanExample target/spark-examples.jar<br>// 输入: 序列数据集，输出: 频繁序列模式 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 随机森林分类示例，集成多棵决策树的分类算法 | // 运行: spark-submit --class JavaRandomForestClassificationExample target/spark-examples.jar<br>// 输入: 训练数据集，输出: 分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 随机森林回归示例，集成多棵决策树的回归算法 | // 运行: spark-submit --class JavaRandomForestRegressionExample target/spark-examples.jar<br>// 输入: 训练数据集，输出: 回归模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 排序评估指标示例，计算NDCG、MAP等推荐排序指标 | // 运行: spark-submit --class JavaRankingMetricsExample target/spark-examples.jar<br>// 输入: 排序预测结果，输出: NDCG、MAP等指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 协同过滤推荐示例，ALS算法实现用户-物品推荐 | // 运行: spark-submit --class JavaRecommendationExample target/spark-examples.jar<br>// 输入: 用户-物品评分矩阵，输出: 用户推荐列表 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | SVD奇异值分解示例，矩阵分解降维技术 | // 运行: spark-submit --class JavaSVDExample target/spark-examples.jar<br>// 输入: 矩阵数据，输出: U、S、V分解结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | SVM支持向量机示例，SGD优化训练线性SVM分类器 | // 运行: spark-submit --class JavaSVMWithSGDExample target/spark-examples.jar<br>// 输入: 训练数据集，输出: SVM分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | FP-Growth频繁项集挖掘示例，高效发现交易数据中的频繁模式 | // 运行: spark-submit --class JavaSimpleFPGrowth target/spark-examples.jar<br>// 输入: 交易数据集，输出: 频繁项集及其支持度 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 分层采样示例，按标签比例进行数据采样 | // 运行: spark-submit --class JavaStratifiedSamplingExample target/spark-examples.jar<br>// 输入: 带标签数据集，输出: 分层采样结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 流式假设检验示例，实时数据流的统计检验 | // 运行: spark-submit --class JavaStreamingTestExample target/spark-examples.jar<br>// 输入: 流式数据，输出: 实时检验结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 统计摘要示例，计算均值、方差、最大值、最小值等统计量 | // 运行: spark-submit --class JavaSummaryStatisticsExample target/spark-examples.jar<br>// 输入: 数值数据，输出: 完整统计摘要 |

---

## SQL DataFrame

--------|------|----------|------|------|
| `find` | key: long | `int` | 在哈希表中查找指定key的位置，返回索引 | 在哈希表中查找指定key，返回索引位置 |
| `findOrInsert` | key: long | `MutableColumnarRow` | 查找key位置，不存在则插入新条目 | 查找key或插入新条目，返回MutableColumnarRow |

--------|------|----------|------|------|
| `binarySearch` | data: boolean&lt;&gt;, value: boolean | `int` | 在boolean数组中二分查找，false排在true前面 | `boolean[] arr = {false, false, true, true};<br>int idx = ArrayExpressionUtils.binarySearch(arr, true);<br>// 返回2（找到索引）<br>int notFound = ArrayExpressionUtils.binarySearch(arr, true);<br>// 未找到返回-(插入点+1)` |
| `binarySearch` | data: Boolean&lt;&gt;, value: Boolean | `int` | 在Boolean数组中二分查找，支持null值（null排在最前） | `Boolean[] arr = {null, false, true};<br>int idx = ArrayExpressionUtils.binarySearch(arr, false);<br>// null < false < true 排序顺序` |
| `binarySearch` | data: byte&lt;&gt;, value: byte | `int` | 在byte数组中二分查找，数组必须已升序排序 | `byte[] arr = {1, 3, 5, 7, 9};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 5);<br>// 返回2` |
| `binarySearch` | data: Byte&lt;&gt;, value: Byte | `int` | 在Byte数组中二分查找，支持null值排序 | `Byte[] arr = {null, 1, 5, 10};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 5);` |
| `binarySearch` | data: short&lt;&gt;, value: short | `int` | 在short数组中二分查找，数组必须已升序排序 | `short[] arr = {100, 200, 300};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 200);` |
| `binarySearch` | data: Short&lt;&gt;, value: Short | `int` | 在Short数组中二分查找，支持null值排序 | `Short[] arr = {null, 10, 20, 30};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 20);` |
| `binarySearch` | data: int&lt;&gt;, value: int | `int` | 在int数组中二分查找，最常用的整数查找方法 | `int[] arr = {1, 5, 10, 15, 20};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 10);<br>// 返回2（找到返回索引）<br>int notFound = ArrayExpressionUtils.binarySearch(arr, 8);<br>// 返回-3（插入点为2，返回-(2+1)）` |
| `binarySearch` | data: Integer&lt;&gt;, value: Integer | `int` | 在Integer数组中二分查找，支持null值排序（null排在最前） | `Integer[] arr = {null, 1, 5, 10};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 5);` |
| `binarySearch` | data: long&lt;&gt;, value: long | `int` | 在long数组中二分查找，用于大整数查找 | `long[] arr = {100L, 200L, 300L};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 200L);` |
| `binarySearch` | data: Long&lt;&gt;, value: Long | `int` | 在Long数组中二分查找，支持null值排序 | `Long[] arr = {null, 100L, 500L};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 100L);` |
| `binarySearch` | data: float&lt;&gt;, value: float | `int` | 在float数组中二分查找，遵循SQL浮点数排序规则 | `float[] arr = {1.0f, 2.5f, 3.0f};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 2.5f);` |
| `binarySearch` | data: Float&lt;&gt;, value: Float | `int` | 在Float数组中二分查找，支持null值，使用SQLOrderingUtil.compareFloats | `Float[] arr = {null, 1.0f, 2.0f};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 1.0f);` |
| `binarySearch` | data: double&lt;&gt;, value: double | `int` | 在double数组中二分查找，使用标准二分查找算法 | `double[] arr = {1.1, 2.2, 3.3};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 2.2);` |
| `binarySearch` | data: Double&lt;&gt;, value: Double | `int` | 在Double数组中二分查找，使用SQLOrderingUtil.compareDoubles处理特殊值 | `Double[] arr = {null, 1.0, 5.0};<br>int idx = ArrayExpressionUtils.binarySearch(arr, 1.0);` |
| `binarySearch` | data: Object&lt;&gt;, value: Object, comp: Comparator<Object> | `int` | 在Object数组中二分查找，使用自定义Comparator定义排序规则 | `String[] arr = {"apple", "banana", "cherry"};<br>Comparator<String> comp = String::compareTo;<br>int idx = ArrayExpressionUtils.binarySearch(arr, "banana", comp);<br>// 返回1` |

--------|------|----------|------|------|
| `getClassOfT` | 无 | `Class&lt;Decimal&gt;` | 获取泛型类型的Class对象 | 返回Decimal类型的Class对象 |
| `sizeOf` | item: Decimal | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `sizeOf` | mem: Memory, offsetBytes: long, numItems: int | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `sizeOf` | item: Decimal | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `sizeOf` | mem: Memory, offsetBytes: long, numItems: int | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭Arrow列向量，释放底层Arrow ValueVector和子列向量占用的内存资源，防止内存泄漏 | `ArrowColumnVector vector = ...;<br>vector.close();  // 释放内存` |
| `getArray` | rowId: int | `ColumnarArray` | 获取指定行的数组类型数据，返回ColumnarArray对象，可通过它遍历数组元素 | `ColumnarArray arr = vector.getArray(0);<br>int len = arr.length();<br>for (int i = 0; i < len; i++) {<br>    Object elem = arr.get(i, elementType);<br>}` |
| `getBoolean` | rowId: int | `boolean` | 获取指定行位置的布尔值数据 | `boolean value = vector.getBoolean(0);<br>// 返回true或false` |
| `getByte` | rowId: int | `byte` | 获取指定行位置的字节值数据（-128到127） | `byte value = vector.getByte(0);` |
| `getChild` | ordinal: int | `ArrowColumnVector` | 获取嵌套类型（Struct/Array/Map）的子列向量，ordinal为子列索引 | `ArrowColumnVector child = vector.getChild(0);<br>// 获取Struct的第一个字段列` |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取指定行的Decimal高精度数值，precision为总位数，scale为小数位数 | `Decimal dec = vector.getDecimal(0, 10, 2);<br>// precision=10表示最多10位数字，scale=2表示2位小数` |
| `getDouble` | rowId: int | `double` | 获取指定行位置的双精度浮点数数据 | `double value = vector.getDouble(0);` |
| `getFloat` | rowId: int | `float` | 获取指定行位置的单精度浮点数数据 | `float value = vector.getFloat(0);` |
| `getGeography` | rowId: int | `GeographyVal` | 获取指定行的地理空间数据（Geography类型），用于GIS应用 | `GeographyVal geo = vector.getGeography(0);` |
| `getGeometry` | rowId: int | `GeometryVal` | 获取指定行的几何空间数据（Geometry类型），用于GIS应用 | `GeometryVal geom = vector.getGeometry(0);` |
| `getInt` | rowId: int | `int` | 获取指定行位置的整数值数据 | `int value = vector.getInt(0);` |
| `getInterval` | rowId: int | `CalendarInterval` | 获取指定行的时间间隔数据，包含months、days、microseconds三个字段 | `CalendarInterval interval = vector.getInterval(0);<br>int months = interval.months;<br>int days = interval.days;<br>long microseconds = interval.microseconds;` |
| `getLong` | rowId: int | `long` | 获取指定行位置的长整数值数据 | `long value = vector.getLong(0);` |
| `getMap` | rowId: int | `ColumnarMap` | 获取指定行的Map类型数据，返回ColumnarMap对象 | `ColumnarMap map = vector.getMap(0);<br>int numElements = map.numElements();<br>// 可通过keyArray()和valueArray()访问键值` |
| `getShort` | rowId: int | `short` | 获取指定行位置的短整数值数据（-32768到32767） | `short value = vector.getShort(0);` |
| `getUTF8String` | rowId: int | `UTF8String` | 获取指定行的UTF8编码字符串数据 | `UTF8String str = vector.getUTF8String(0);<br>String javaStr = str.toString();` |
| `getValueVector` | 无 | `ValueVector` | 获取底层Arrow ValueVector对象，用于直接访问Arrow原生API | `ValueVector arrowVec = vector.getValueVector();<br>// 可使用Arrow原生API进行高级操作` |
| `hasNull` | 无 | `boolean` | 检查列向量中是否存在null值，比遍历检查更高效 | `boolean hasNullValues = vector.hasNull();<br>if (hasNullValues) {<br>    // 需要处理null值逻辑<br>}` |
| `isNullAt` | rowId: int | `boolean` | 检查指定行位置是否为null值，读取数据前应先检查 | `if (!vector.isNullAt(0)) {<br>    int value = vector.getInt(0);<br>} else {<br>    // 处理null情况<br>}` |
| `numNulls` | 无 | `int` | 返回列向量中null值的总数量 | `int nullCount = vector.numNulls();<br>System.out.println("Null values: " + nullCount);` |

### JavaSparkStatusTracker
**包路径**: `org.apache.spark.api.java`
**说明**: 作业状态追踪器，用于监控Spark作业的执行状态。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getActiveJobsIds` | 无 | `int[]` | 获取活动作业ID列表 | `int[] activeJobs = tracker.getActiveJobsIds();` |
| `getActiveStageIds` | 无 | `int[]` | 获取活动Stage ID列表 | `int[] activeStages = tracker.getActiveStageIds();` |
| `getPendingJobsIds` | 无 | `int[]` | 获取等待中作业ID列表 | `int[] pendingJobs = tracker.getPendingJobsIds();` |
| `getPendingStageIds` | 无 | `int[]` | 获取等待中Stage ID列表 | `int[] pendingStages = tracker.getPendingStageIds();` |
| `getActiveJobIds` | 无 | `int[]` | 获取活动作业ID列表（别名） | `int[] active = tracker.getActiveJobIds();` |
| `getActiveStageIds` | 无 | `int[]` | 获取活动Stage ID列表（别名） | `int[] active = tracker.getActiveStageIds();` |
| `getPendingJobIds` | 无 | `int[]` | 获取等待作业ID列表（别名） | `int[] pending = tracker.getPendingJobIds();` |
| `getPendingStageIds` | 无 | `int[]` | 获取等待Stage ID列表（别名） | `int[] pending = tracker.getPendingStageIds();` |

---


--------|------|----------|------|------|
| `bitmapAndMerge` | bitmap1: byte&lt;&gt;, bitmap2: byte&lt;&gt; | `void` | 对两个位图执行AND合并操作 | 对两个位图执行AND操作，返回交集位图 |
| `bitmapBitPosition` | value: long | `long` | 计算位图中指定值的位位置 | 计算值在桶内的位位置（0-63） |
| `bitmapBucketNumber` | value: long | `long` | 计算位图中指定值的桶编号 | 计算值所在的桶编号 |
| `bitmapCount` | bitmap: byte&lt;&gt; | `long` | 统计位图中设置的位数 | 返回位图中设置的位数统计 |
| `bitmapMerge` | bitmap1: byte&lt;&gt;, bitmap2: byte&lt;&gt; | `void` | 合并两个位图 | 合并两个位图，返回OR结果 |

--------|------|----------|------|------|
| `append` | row: InternalRow | `void` | 追加元素 | 向缓冲迭代器追加一行数据 |
| `durationMs` | 无 | `long` | 获取执行耗时（毫秒） | 返回执行耗时（毫秒） |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `incPeakExecutionMemory` | size: long | `void` | 增加峰值执行内存计数 | 增加峰值执行内存统计 |
| `next` | 无 | `InternalRow` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `shouldStop` | 无 | `boolean` | 检查是否应该停止迭代 | 检查是否应停止迭代处理 |

--------|------|----------|------|------|
| `getSrid` | stringId: String | `Integer` | 获取空间参考系统ID（SRID） | 将字符串空间参考ID转换为整数SRID |
| `getStringId` | srid: int | `String` | 将SRID转换为字符串标识 | 将整数SRID转换为字符串标识 |


--------|------|----------|------|------|
| `dataType` | 无 | `DataType` | 获取数据类型 | 返回Cast目标的数据类型 |
| `expression` | 无 | `Expression` | 获取表达式对象 | 返回被转换的表达式对象 |
| `expressionDataType` | 无 | `DataType` | 获取表达式的数据类型 | 返回源表达式的数据类型 |

--------|------|----------|------|------|
| `computeUpdates` | 无 | `boolean` | 是否计算更新操作 | 检查是否计算更新记录 |
| `deduplicationMode` | 无 | `DeduplicationMode` | 获取去重模式 | 返回去重模式配置 |
| `range` | 无 | `ChangelogRange` | 获取变更日志范围 | 返回变更日志的时间范围 |

--------|------|----------|------|------|
| `charTypeWriteSideCheck` | inputStr: UTF8String, limit: int | `UTF8String` | CHAR类型写入端校验，截断超长字符串 | 校验CHAR类型写入，超长则截断 |
| `readSidePadding` | inputStr: UTF8String, limit: int | `UTF8String` | 读取端填充，补齐CHAR类型定长 | 读取端补齐CHAR定长字符串 |
| `varcharTypeWriteSideCheck` | inputStr: UTF8String, limit: int | `UTF8String` | VARCHAR类型写入端校验，截断超长字符串 | 校验VARCHAR类型写入，超长则截断 |


--------|------|----------|------|------|
| `binaryTrim` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 二进制模式去除两端指定字符 | 去除字符串两端指定字符（二进制模式） |
| `binaryTrimRight` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 二进制模式去除右侧指定字符 | 去除字符串右侧指定字符（二进制模式） |
| `compareLowerCase` | left: final UTF8String, right: final UTF8String | `int` | 比较两个字符串的小写形式 | 比较两字符串小写形式，返回差值 |
| `findInSet` | match: final UTF8String, set: final UTF8String, collationId: int | `int` | 在集合字符串中查找匹配项位置 | 在逗号分隔集合中查找元素位置 |
| `indexOf` | target: final UTF8String, pattern: final UTF8String, start: final int, collationId: final int | `int` | 查找子串在字符串中的起始位置 | 查找子串起始位置，支持指定起始索引 |
| `lowerCaseCodePoints` | target: final UTF8String | `UTF8String` | 获取小写形式的Unicode码点 | 获取小写Unicode码点字符串 |
| `lowercaseContains` | target: final UTF8String, pattern: final UTF8String | `boolean` | 忽略大小写检查是否包含子串 | 忽略大小写检查是否包含子串 |
| `lowercaseEndsWith` | target: final UTF8String, pattern: final UTF8String | `boolean` | 忽略大小写检查是否以指定字符串结尾 | 忽略大小写检查是否以指定结尾 |
| `lowercaseIndexOf` | target: final UTF8String, pattern: final UTF8String, start: final int | `int` | 忽略大小写查找子串位置 | 忽略大小写查找子串位置 |
| `lowercaseReplace` | target: final UTF8String, search: final UTF8String, replace: final UTF8String | `UTF8String` | 忽略大小写替换匹配的字符串 | 忽略大小写替换匹配内容 |
| `lowercaseStartsWith` | target: final UTF8String, pattern: final UTF8String | `boolean` | 忽略大小写检查是否以指定字符串开头 | 忽略大小写检查是否以指定开头 |
| `lowercaseSubStringIndex` | string: final UTF8String, delimiter: final UTF8String, count: int | `UTF8String` | 忽略大小写的子串索引查找 | 忽略大小写的子串索引 |
| `lowercaseTranslate` | input: final UTF8String, Map<String: final, dict: String> | `UTF8String` | 忽略大小写的字符转换 | 忽略大小写的字符映射转换 |
| `lowercaseTrim` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 忽略大小写去除两端空白 | 忽略大小写去除两端空白 |
| `lowercaseTrimLeft` | srcString: final UTF8String, trimString: final UTF8String | `UTF8String` | 忽略大小写去除左侧空白 | 忽略大小写去除左侧空白 |
| `lowercaseTrimRight` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 忽略大小写去除右侧空白 | 忽略大小写去除右侧空白 |
| `replace` | target: final UTF8String, search: final UTF8String, replace: final UTF8String, collationId: final int | `UTF8String` | 替换字符串中匹配的内容 | 替换字符串中匹配内容 |
| `subStringIndex` | string: final UTF8String, delimiter: final UTF8String, count: int, collationId: final int | `UTF8String` | 查找分隔符分隔的子串索引 | 按分隔符查找第N个子串 |
| `toLowerCase` | target: final UTF8String | `UTF8String` | 转换为小写 | 转换为小写字符串 |
| `toLowerCase` | target: final UTF8String, collationId: final int | `UTF8String` | 转换为小写 | 转换为小写字符串 |
| `toTitleCase` | target: final UTF8String | `UTF8String` | 转换为标题大小写 | 转换为标题大小写（首字母大写） |
| `toTitleCase` | target: final UTF8String, collationId: final int | `UTF8String` | 转换为标题大小写 | 转换为标题大小写（首字母大写） |
| `toTitleCaseICU` | source: UTF8String | `UTF8String` | 使用ICU库转换为标题大小写 | ICU库标题大小写转换 |
| `toUpperCase` | target: final UTF8String | `UTF8String` | 转换为大写 | 转换为大写字符串 |
| `toUpperCase` | target: final UTF8String, collationId: final int | `UTF8String` | 转换为大写 | 转换为大写字符串 |
| `translate` | input: final UTF8String, Map<String: final, dict: String>, collationId: final int | `UTF8String` | 字符映射转换 | 按字符映射表转换字符串 |
| `trim` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 去除空白 | 去除字符串两端空白 |
| `trimLeft` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 去除字符串左侧空白 | 去除字符串左侧空白 |
| `trimRight` | srcString: final UTF8String, trimString: final UTF8String, collationId: final int | `UTF8String` | 去除字符串右侧空白 | 去除字符串右侧空白 |

--------|------|----------|------|------|
| `getValue` | 无 | `Literal&lt;?&gt;` | 获取列的默认值 | 返回列默认值的Literal对象 |


--------|------|----------|------|------|
| `populate` | col: ConstantColumnVector, row: InternalRow, fieldIdx: int | `void` | 填充常量列向量数据 | 填充常量列向量数据 |
| `toBatch` | schema: StructType, memMode: MemoryMode, row: Iterator<Row> | `ColumnarBatch` | 将行迭代器转换为列式批处理 | 将行迭代器转为列式批处理 |
| `toJavaIntMap` | map: ColumnarMap | `Map&lt;Integer, Integer&gt;` | 将ColumnarMap转换为Java Map | 将ColumnarMap转为Java整数Map |

| `isNaN` | 无 | `Column` | 判断是否NaN | `Column isNan = col("value").isNaN();` |
| `regexp` | String pattern | `Column` | 正则匹配（rlike别名） | `Column matched = col("name").regexp("^[A-Z]");` |


--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭列向量，释放字符串、数组、Map和子列向量占用的内存 | `vector.close();  // 释放所有资源` |
| `closeIfFreeable` | 无 | `void` | 无操作（常量向量跨批处理复用，仅在close时释放） | `// 常量向量数据跨批复用<br>// 此方法为空实现` |
| `getArray` | rowId: int | `ColumnarArray` | 获取数组数据（所有行返回相同的ColumnarArray） | `ColumnarArray arr = vector.getArray(0);<br>// 所有rowId返回相同的数组` |
| `getBoolean` | rowId: int | `boolean` | 获取布尔值（所有行返回相同的值） | `boolean val = vector.getBoolean(0);<br>// 无需关心rowId，值相同` |
| `getByte` | rowId: int | `byte` | 获取字节值（所有行返回相同的值） | `byte val = vector.getByte(0);` |
| `getChild` | ordinal: int | `ColumnVector` | 获取嵌套类型的子列向量 | `ColumnVector child = vector.getChild(0);<br>// 用于Struct/Array等嵌套类型` |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal高精度数值（所有行返回相同的值） | `Decimal dec = vector.getDecimal(0, 10, 2);<br>// precision和scale指定精度` |
| `getDouble` | rowId: int | `double` | 获取双精度浮点数（所有行返回相同的值） | `double val = vector.getDouble(0);` |
| `getFloat` | rowId: int | `float` | 获取单精度浮点数（所有行返回相同的值） | `float val = vector.getFloat(0);` |
| `getInt` | rowId: int | `int` | 获取整数（所有行返回相同的值） | `int val = vector.getInt(0);` |
| `getLong` | rowId: int | `long` | 获取长整数（所有行返回相同的值） | `long val = vector.getLong(0);` |
| `getMap` | ordinal: int | `ColumnarMap` | 获取Map数据（所有行返回相同的ColumnarMap） | `ColumnarMap map = vector.getMap(0);` |
| `getShort` | rowId: int | `short` | 获取短整数（所有行返回相同的值） | `short val = vector.getShort(0);` |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8字符串（所有行返回相同的值） | `UTF8String str = vector.getUTF8String(0);<br>String javaStr = str.toString();` |
| `hasNull` | 无 | `boolean` | 检查是否所有行都是null值 | `boolean isAllNull = vector.hasNull();` |
| `isNullAt` | rowId: int | `boolean` | 检查指定行是否为null（所有行返回相同的null标记） | `if (vector.isNullAt(0)) {<br>    // 处理null情况<br>}` |
| `numNulls` | 无 | `int` | 返回null值数量（要么0要么全部行数） | `int nulls = vector.numNulls();<br>// 如果hasNull为true，返回numRows` |
| `setArray` | value: ColumnarArray | `void` | 设置所有行的数组常量值 | `vector.setArray(arrayValue);<br>// 所有行共享此数组` |
| `setBinary` | value: byte[] | `void` | 设置所有行的二进制常量值 | `byte[] data = new byte[]{1, 2, 3};<br>vector.setBinary(data);` |
| `setBoolean` | value: boolean | `void` | 设置所有行的布尔常量值 | `vector.setBoolean(true);<br>// 所有行值为true` |
| `setByte` | value: byte | `void` | 设置所有行的字节常量值 | `vector.setByte((byte) 100);` |
| `setCalendarInterval` | value: CalendarInterval | `void` | 设置所有行的时间间隔常量值 | `CalendarInterval interval = new CalendarInterval(1, 2, 1000L);<br>vector.setCalendarInterval(interval);` |
| `setChild` | ordinal: int, value: ConstantColumnVector | `void` | 设置嵌套类型的子列向量 | `vector.setChild(0, childVector);<br>// 用于Struct字段的子列` |
| `setDecimal` | value: Decimal, precision: int | `void` | 设置所有行的Decimal常量值 | `Decimal dec = Decimal.apply(123.45);<br>vector.setDecimal(dec, 10);` |
| `setDouble` | value: double | `void` | 设置所有行的双精度常量值 | `vector.setDouble(3.14);` |
| `setFloat` | value: float | `void` | 设置所有行的单精度常量值 | `vector.setFloat(2.5f);` |
| `setInt` | value: int | `void` | 设置所有行的整数常量值 | `vector.setInt(42);<br>// 所有行值为42` |
| `setLong` | value: long | `void` | 设置所有行的长整数常量值 | `vector.setLong(123456789L);` |
| `setMap` | value: ColumnarMap | `void` | 设置所有行的Map常量值 | `vector.setMap(mapValue);` |
| `setNotNull` | 无 | `void` | 设置所有行为非null值 | `vector.setNotNull();<br>// 清除null标记` |
| `setNull` | 无 | `void` | 设置所有行为null值 | `vector.setNull();<br>// 所有行都是null` |
| `setShort` | value: short | `void` | 设置所有行的短整数常量值 | `vector.setShort((short) 1000);` |
| `setUtf8String` | value: UTF8String | `void` | 设置所有行的UTF8字符串常量值 | `UTF8String str = UTF8String.fromString("hello");<br>vector.setUtf8String(str);` |
| `setVariant` | value: VariantVal | `void` | 设置所有行的Variant类型常量值 | `vector.setVariant(variantValue);` |

### DataTypes
**包路径**: `org.apache.spark.sql.types`
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createArrayType` | elementType: DataType | `ArrayType` | 创建数组类型 | 创建数组数据类型 |
| `createArrayType` | elementType: DataType, containsNull: boolean | `ArrayType` | 创建数组类型 | 创建数组数据类型 |
| `createCharType` | length: int | `CharType` | 创建CHAR定长字符类型 | 创建CHAR定长类型 |
| `createDayTimeIntervalType` | startField: byte, endField: byte | `DayTimeIntervalType` | 创建日-时间间隔类型 | 创建日-时间间隔类型 |
| `createDayTimeIntervalType` | 无 | `DayTimeIntervalType` | 创建日-时间间隔类型 | 创建日-时间间隔类型 |
| `createDecimalType` | precision: int, scale: int | `DecimalType` | 创建Decimal高精度数值类型 | 创建Decimal高精度类型 |
| `createDecimalType` | 无 | `DecimalType` | 创建Decimal高精度数值类型 | 创建Decimal高精度类型 |
| `createGeographyType` | srid: int | `GeographyType` | 创建地理空间类型 | 创建地理空间类型 |
| `createGeographyType` | crs: String | `GeographyType` | 创建地理空间类型 | 创建地理空间类型 |
| `createGeometryType` | srid: int | `GeometryType` | 创建几何空间类型 | 创建几何空间类型 |
| `createGeometryType` | crs: String | `GeometryType` | 创建几何空间类型 | 创建几何空间类型 |
| `createMapType` | keyType: DataType, valueType: DataType | `MapType` | 创建Map类型 | 创建Map映射类型 |
| `createMapType` | keyType: DataType, valueType: DataType, valueContainsNull: boolean | `MapType` | 创建Map类型 | 创建Map映射类型 |
| `createStructField` | name: String, dataType: DataType, nullable: boolean, metadata: Metadata | `StructField` | 创建结构字段 | 创建结构字段定义 |
| `createStructField` | name: String, dataType: DataType, nullable: boolean | `StructField` | 创建结构字段 | 创建结构字段定义 |
| `createStructType` | fields: List<StructField> | `StructType` | 创建结构类型 | 创建结构类型定义 |
| `createStructType` | fields: StructField&lt;&gt; | `StructType` | 创建结构类型 | 创建结构类型定义 |
| `createVarcharType` | length: int | `VarcharType` | 创建VARCHAR变长字符类型 | 创建VARCHAR变长类型 |
| `createYearMonthIntervalType` | startField: byte, endField: byte | `YearMonthIntervalType` | 创建年-月间隔类型 | 创建年-月间隔类型 |
| `createYearMonthIntervalType` | 无 | `YearMonthIntervalType` | 创建年-月间隔类型 | 创建年-月间隔类型 |

--------|------|----------|------|------|
| `getExpression` | 无 | `Expression` | 获取默认值表达式 | 获取默认值表达式对象 |
| `getSql` | 无 | `String` | 获取默认值的SQL表示 | 获取默认值SQL字符串 |

--------|------|----------|------|------|
| `alterNamespace` | namespace: String&lt;&gt;, changes: NamespaceChange... | `void` | 修改命名空间属性 | 修改命名空间属性 |
| `alterTable` | ident: Identifier, changes: TableChange... | `Table` | 修改表结构或属性 | 修改表结构 |
| `capabilities` | 无 | `Set&lt;TableCatalogCapability&gt;` | 获取表目录支持的能力 | 返回目录支持的能力集合 |
| `createNamespace` | namespace: String&lt;&gt;, metadata: String> | `void` | 创建命名空间 | 创建命名空间 |
| `createTable` | ident: Identifier, schema: StructType, partitions: Transform&lt;&gt;, properties: String> | `Table` | 创建表 | 创建新表 |
| `createTable` | ident: Identifier, columns: Column&lt;&gt;, partitions: Transform&lt;&gt;, properties: String> | `Table` | 创建表 | 创建新表 |
| `dropNamespace` | namespace: String&lt;&gt;, cascade: boolean | `boolean` | 删除命名空间 | 删除命名空间 |
| `dropTable` | ident: Identifier | `boolean` | 删除表 | 删除表 |
| `functionExists` | ident: Identifier | `boolean` | 检查函数是否存在 | 检查函数是否存在 |
| `initialize` | name: String, options: CaseInsensitiveStringMap | `void` | 初始化插件 | 初始化目录插件 |
| `invalidateTable` | ident: Identifier | `void` | 失效表缓存 | 失效表缓存 |
| `loadFunction` | ident: Identifier | `UnboundFunction` | 加载函数 | 加载指定函数 |
| `loadNamespaceMetadata` | namespace: String&lt;&gt; | `Map&lt;String, String&gt;` | 加载命名空间元数据 | 加载命名空间元数据 |
| `loadTable` | ident: Identifier | `Table` | 加载表 | 加载表对象 |
| `loadTable` | ident: Identifier, timestamp: long | `Table` | 加载表 | 加载表对象 |
| `loadTable` | ident: Identifier, version: String | `Table` | 加载表 | 加载表对象 |
| `name` | 无 | `String` | 获取度量指标名称 | 返回度量指标名称 |
| `namespaceExists` | namespace: String&lt;&gt; | `boolean` | 检查命名空间是否存在 | 检查命名空间是否存在 |
| `purgeTable` | ident: Identifier | `boolean` | 彻底删除表（不可恢复） | 彻底删除表 |
| `renameTable` | oldIdent: Identifier, newIdent: Identifier | `void` | 重命名表 | 重命名表 |
| `setDelegateCatalog` | delegate: CatalogPlugin | `void` | 设置代理目录 | 设置代理目录 |
| `tableExists` | ident: Identifier | `boolean` | 检查表是否存在 | 检查表是否存在 |

### StructType
**包路径**: `org.apache.spark.sql.types`
**说明**: DataFrame结构定义，包含多个StructField。
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StructType` | 无 | 构造方法 | 创建空结构 | `StructType schema = new StructType();` |
| `add` | StructField field | `StructType` | 添加字段 | `schema.add(new StructField("name", DataTypes.StringType, false, Metadata.empty()));` |
| `add` | String name, DataType dataType | `StructType` | 添加字段（简化） | `schema.add("age", DataTypes.IntegerType);` |
| `add` | String name, DataType dataType, boolean nullable | `StructType` | 添加字段（指定nullable） | `schema.add("id", DataTypes.LongType, false);` |
| `fields` | 无 | `StructField[]` | 获取所有字段 | `StructField[] fields = schema.fields();` |
| `fieldNames` | 无 | `String[]` | 获取所有字段名 | `String[] names = schema.fieldNames();` |
| `apply` | String name | `StructField` | 获取指定字段 | `StructField field = schema.apply("name");` |
| `apply` | int index | `StructField` | 获取指定位置字段 | `StructField field = schema.apply(0);` |
| `length` | 无 | `int` | 获取字段数量 | `int len = schema.length();` |
| `toDDL` | 无 | `String` | DDL格式字符串 | `String ddl = schema.toDDL();` |
| `json` | 无 | `String` | 转为JSON | `String json = schema.json();` |
| `prettyJson` | 无 | `String` | 转为格式化JSON | `String pretty = schema.prettyJson();` |

---

### StructField
**包路径**: `org.apache.spark.sql.types`
**说明**: 单个列结构定义，包含名称、类型、nullable等。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StructField` | String name, DataType dataType, boolean nullable, Metadata metadata | 构造方法 | 创建字段 | `StructField field = new StructField("name", DataTypes.StringType, true, Metadata.empty());` |
| `name` | 无 | `String` | 获取字段名 | `String name = field.name();` |
| `dataType` | 无 | `DataType` | 获取数据类型 | `DataType type = field.dataType();` |
| `nullable` | 无 | `boolean` | 是否可空 | `boolean nullable = field.nullable();` |
| `metadata` | 无 | `Metadata` | 获取元数据 | `Metadata meta = field.metadata();` |
| `getComment` | 无 | `String` | 获取注释 | `String comment = field.getComment();` |
| `withComment` | String comment | `StructField` | 添加注释 | `StructField newField = field.withComment("用户名");` |
| `toDDL` | 无 | `String` | DDL格式 | `String ddl = field.toDDL();` |

---


--------|------|----------|------|------|
| `getSentences` | str: UTF8String, language: UTF8String, country: UTF8String | `ArrayData` | 将文本分割为句子数组 | 传入参数执行将文本分割为句子数组 |
| `getSparkVersion` | 无 | `UTF8String` | 获取Spark版本字符串 | 调用该方法执行获取Spark版本字符串 |
| `isLuhnNumber` | numberString: UTF8String | `boolean` | 校验Luhn算法数字（信用卡号校验） | 传入参数执行校验Luhn算法数字（信用卡号校验） |
| `quote` | str: UTF8String | `UTF8String` | 对字符串进行引用处理 | 传入参数执行对字符串进行引用处理 |
| `randStr` | rng: XORShiftRandom, length: int | `UTF8String` | 生成随机字符串 | 传入参数执行生成随机字符串 |
| `tryValidateUTF8String` | utf8String: UTF8String | `UTF8String` | 尝试校验UTF8字符串 | 传入参数执行尝试校验UTF8字符串 |
| `validateUTF8String` | utf8String: UTF8String | `UTF8String` | 校验UTF8字符串有效性 | 传入参数执行校验UTF8字符串有效性 |

--------|------|----------|------|------|
| `getArguments` | 无 | `String` | 获取函数参数说明 | 调用该方法执行获取函数参数说明 |
| `getClassName` | 无 | `String` | 获取类名 | 调用该方法执行获取类名 |
| `getDb` | 无 | `String` | 获取数据库名 | 调用该方法执行获取数据库名 |
| `getDeprecated` | 无 | `String` | 获取弃用说明 | 调用该方法执行获取弃用说明 |
| `getExamples` | 无 | `String` | 获取使用示例 | 调用该方法执行获取使用示例 |
| `getExtended` | 无 | `String` | 获取扩展说明 | 调用该方法执行获取扩展说明 |
| `getGroup` | 无 | `String` | 获取函数分组 | 调用该方法执行获取函数分组 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getNote` | 无 | `String` | 获取备注说明 | 调用该方法执行获取备注说明 |
| `getOriginalExamples` | 无 | `String` | 获取原始示例 | 调用该方法执行获取原始示例 |
| `getSince` | 无 | `String` | 获取版本信息 | 调用该方法执行获取版本信息 |
| `getSource` | 无 | `String` | 获取来源 | 调用该方法执行获取来源 |
| `getUsage` | 无 | `String` | 获取使用说明 | 调用该方法执行获取使用说明 |


--------|------|----------|------|------|
| `getSrid` | stringId: String | `Integer` | 获取空间参考系统ID（SRID） | 将字符串空间参考ID转换为整数SRID |
| `getStringId` | srid: int | `String` | 将SRID转换为字符串标识 | 将整数SRID转换为字符串标识 |

--------|------|----------|------|------|
| `toWkt` | 无 | `String` | toWkt操作 | 调用该方法执行toWkt操作 |


--------|------|----------|------|------|
| `getCompressionCodec` | 无 | `CompressionCodec` | 获取CompressionCodec相关功能 | 调用该方法执行获取CompressionCodec相关功能 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |

--------|------|----------|------|------|
| `getCurrentKey` | 无 | `LongWritable` | 获取CurrentKey相关功能 | 调用该方法执行获取CurrentKey相关功能 |
| `getCurrentValue` | 无 | `Text` | 获取CurrentValue相关功能 | 调用该方法执行获取CurrentValue相关功能 |
| `getProgress` | 无 | `float` | 获取Progress相关功能 | 调用该方法执行获取Progress相关功能 |
| `initialize` | genericSplit: InputSplit, context: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `nextKeyValue` | 无 | `boolean` | 之后KeyValue相关功能 | 调用该方法执行之后KeyValue相关功能 |

--------|------|----------|------|------|
| `hashInt` | input: int | `int` | 检查是否存在hInt相关功能 | 传入参数执行检查是否存在hInt相关功能 |
| `hashLong` | input: long | `int` | 检查是否存在hLong相关功能 | 传入参数执行检查是否存在hLong相关功能 |
| `hashUnsafeBytes` | base: Object, offset: long, lengthInBytes: int | `int` | 检查是否存在hUnsafeBytes相关功能 | 传入参数执行检查是否存在hUnsafeBytes相关功能 |


--------|------|----------|------|------|
| `getCube` | 无 | `int` | 获取Cube相关功能 | 调用该方法执行获取Cube相关功能 |
| `getSquare` | 无 | `int` | 获取Square相关功能 | 调用该方法执行获取Square相关功能 |
| `getValue` | 无 | `int` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `setCube` | cube: int | `void` | 设置Cube相关功能 | 传入参数执行设置Cube相关功能 |
| `setSquare` | square: int | `void` | 设置Square相关功能 | 传入参数执行设置Square相关功能 |
| `setValue` | value: int | `void` | 设置Value相关功能 | 传入参数执行设置Value相关功能 |

--------|------|----------|------|------|
| `getKey` | 无 | `int` | 获取Key相关功能 | 调用该方法执行获取Key相关功能 |
| `getValue` | 无 | `String` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `setKey` | key: int | `void` | 设置Key相关功能 | 传入参数执行设置Key相关功能 |
| `setValue` | value: String | `void` | 设置Value相关功能 | 传入参数执行设置Value相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `getAge` | 无 | `long` | 获取Age相关功能 | 调用该方法执行获取Age相关功能 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `setAge` | age: long | `void` | 设置Age相关功能 | 传入参数执行设置Age相关功能 |
| `setName` | name: String | `void` | 设置RDD名称 | 传入参数执行设置Name相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `bufferEncoder` | 无 | `Encoder&lt;Average&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `finish` | reduction: Average | `Double` | 完成相关功能 | 传入参数执行完成相关功能 |
| `getCount` | 无 | `long` | 获取Count相关功能 | 调用该方法执行获取Count相关功能 |
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getSalary` | 无 | `long` | 获取Salary相关功能 | 调用该方法执行获取Salary相关功能 |
| `getSum` | 无 | `long` | 获取Sum相关功能 | 调用该方法执行获取Sum相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `merge` | b1: Average, b2: Average | `Average` | 合并相关功能 | 传入参数执行合并相关功能 |
| `outputEncoder` | 无 | `Encoder&lt;Double&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `reduce` | buffer: Average, employee: Employee | `Average` | 聚合DStream每个RDD | // reduce：聚合所有元素为单个结果<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));<br>// 求和<br>Integer sum = numbers.reduce((a, b) -> a + b);<br>// 结果: 15<br>// 求最大值<br>Integer max = numbers.reduce((a, b) -> Math.max(a, b));<br>// 结果: 5<br>// 字符串拼接<br>JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));<br>String concatenated = words.reduce((a, b) -> a + b);<br>// 结果: "abc" |
| `setCount` | count: long | `void` | 设置Count相关功能 | 传入参数执行设置Count相关功能 |
| `setName` | name: String | `void` | 设置RDD名称 | 传入参数执行设置Name相关功能 |
| `setSalary` | salary: long | `void` | 设置Salary相关功能 | 传入参数执行设置Salary相关功能 |
| `setSum` | sum: long | `void` | 设置Sum相关功能 | 传入参数执行设置Sum相关功能 |
| `zero` | 无 | `Average` | zero操作 | 调用该方法执行zero操作 |

--------|------|----------|------|------|
| `bufferEncoder` | 无 | `Encoder&lt;Average&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `finish` | reduction: Average | `Double` | 完成相关功能 | 传入参数执行完成相关功能 |
| `getCount` | 无 | `long` | 获取Count相关功能 | 调用该方法执行获取Count相关功能 |
| `getSum` | 无 | `long` | 获取Sum相关功能 | 调用该方法执行获取Sum相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `merge` | b1: Average, b2: Average | `Average` | 合并相关功能 | 传入参数执行合并相关功能 |
| `outputEncoder` | 无 | `Encoder&lt;Double&gt;` | 编码相关功能 | 调用该方法执行编码相关功能 |
| `reduce` | buffer: Average, data: Long | `Average` | 聚合DStream每个RDD | // reduce：聚合所有元素为单个结果<br>JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));<br>// 求和<br>Integer sum = numbers.reduce((a, b) -> a + b);<br>// 结果: 15<br>// 求最大值<br>Integer max = numbers.reduce((a, b) -> Math.max(a, b));<br>// 结果: 5<br>// 字符串拼接<br>JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));<br>String concatenated = words.reduce((a, b) -> a + b);<br>// 结果: "abc" |
| `setCount` | count: long | `void` | 设置Count相关功能 | 传入参数执行设置Count相关功能 |
| `setSum` | sum: long | `void` | 设置Sum相关功能 | 传入参数执行设置Sum相关功能 |
| `zero` | 无 | `Average` | zero操作 | 调用该方法执行zero操作 |

--------|------|----------|------|------|
| `jsonObjectKeys` | json: UTF8String | `GenericArrayData` | jsonObjectKeys操作 | 传入参数执行jsonObjectKeys操作 |
| `lengthOfJsonArray` | json: UTF8String | `Integer` | lengthOfJsonArray操作 | 传入参数执行lengthOfJsonArray操作 |

--------|------|----------|------|------|
| `cleanupResources` | 无 | `void` | 向上相关功能 | 调用该方法执行向上相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `compare` | baseObj1: Object, baseOff1: long, baseLen1: int, baseObj2: Object, baseOff2: long, baseLen2: int | `int` | 比较相关功能 | 传入参数执行比较相关功能 |
| `getKey` | 无 | `UnsafeRow` | 获取Key相关功能 | 调用该方法执行获取Key相关功能 |
| `getPeakMemoryUsedBytes` | 无 | `long` | 获取PeakMemoryUsedBytes相关功能 | 调用该方法执行获取PeakMemoryUsedBytes相关功能 |
| `getSpillSize` | 无 | `long` | 获取SpillSize相关功能 | 调用该方法执行获取SpillSize相关功能 |
| `getValue` | 无 | `UnsafeRow` | 获取列的默认值 | 返回列默认值的Literal对象 |
| `insertKV` | key: UnsafeRow, value: UnsafeRow | `void` | 插入KV相关功能 | 传入参数执行插入KV相关功能 |
| `merge` | other: UnsafeKVExternalSorter | `void` | 合并相关功能 | 传入参数执行合并相关功能 |
| `next` | 无 | `boolean` | 获取迭代器的下一个元素 | 获取迭代器下一行数据 |
| `sortedIterator` | 无 | `KVSorterIterator` | 排序edIterator相关功能 | 调用该方法执行排序edIterator相关功能 |


--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |


--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | rowId: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | rowId: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `add` | newChild: OrcColumnStatistics | `void` | 添加元素 | 传入参数执行添加相关功能 |
| `get` | ordinal: int | `OrcColumnStatistics` | 获取元素 | 传入参数执行获取相关功能 |
| `getStatistics` | 无 | `ColumnStatistics` | 获取Statistics相关功能 | 调用该方法执行获取Statistics相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `hasNull` | 无 | `boolean` | 检查是否存在Null相关功能 | 调用该方法执行检查是否存在Null相关功能 |
| `isNullAt` | rowId: int | `boolean` | 判断是否NullAt相关功能 | 传入参数执行判断是否NullAt相关功能 |
| `numNulls` | 无 | `int` | numNulls操作 | 调用该方法执行numNulls操作 |
| `setBatchSize` | batchSize: int | `void` | 设置BatchSize相关功能 | 传入参数执行设置BatchSize相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getCurrentKey` | 无 | `Void` | 获取CurrentKey相关功能 | 调用该方法执行获取CurrentKey相关功能 |
| `getCurrentValue` | 无 | `ColumnarBatch` | 获取CurrentValue相关功能 | 调用该方法执行获取CurrentValue相关功能 |
| `getProgress` | 无 | `float` | 获取Progress相关功能 | 调用该方法执行获取Progress相关功能 |
| `initBatch` | orcSchema: TypeDescription, requiredFields: StructField&lt;&gt;, requestedDataColIds: int&lt;&gt;, requestedPartitionColIds: int&lt;&gt;, partitionValues: InternalRow | `void` | 初始化Batch相关功能 | 传入参数执行初始化Batch相关功能 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext, orcTail: OrcTail | `void` | 初始化插件 | 初始化目录插件 |
| `nextKeyValue` | 无 | `boolean` | 之后KeyValue相关功能 | 调用该方法执行之后KeyValue相关功能 |

--------|------|----------|------|------|
| `getCompressionKind` | 无 | `CompressionKind` | 获取CompressionKind相关功能 | 调用该方法执行获取CompressionKind相关功能 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |

--------|------|----------|------|------|
| `readStatistics` | orcReader: Reader | `OrcColumnStatistics` | 读取Statistics相关功能 | 传入参数执行读取Statistics相关功能 |

--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | ordinal: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `getArray` | rowId: int | `ColumnarArray` | 获取Array相关功能 | 传入参数执行获取Array相关功能 |
| `getBoolean` | rowId: int | `boolean` | 获取Boolean相关功能 | 传入参数执行获取Boolean相关功能 |
| `getByte` | rowId: int | `byte` | 获取Byte相关功能 | 传入参数执行获取Byte相关功能 |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal相关功能 | 传入参数执行获取Decimal相关功能 |
| `getDouble` | rowId: int | `double` | 获取Double相关功能 | 传入参数执行获取Double相关功能 |
| `getFloat` | rowId: int | `float` | 获取Float相关功能 | 传入参数执行获取Float相关功能 |
| `getInt` | rowId: int | `int` | 获取Int相关功能 | 传入参数执行获取Int相关功能 |
| `getLong` | rowId: int | `long` | 获取Long相关功能 | 传入参数执行获取Long相关功能 |
| `getMap` | rowId: int | `ColumnarMap` | 获取Map相关功能 | 传入参数执行获取Map相关功能 |
| `getShort` | rowId: int | `short` | 获取Short相关功能 | 传入参数执行获取Short相关功能 |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8String相关功能 | 传入参数执行获取UTF8String相关功能 |

--------|------|----------|------|------|
| `fromString` | s: String | `ParquetCompressionCodec` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |
| `getCompressionCodec` | 无 | `CompressionCodecName` | 获取CompressionCodec相关功能 | 调用该方法执行获取CompressionCodec相关功能 |
| `lowerCaseName` | 无 | `String` | 转换为小写的名称 | 返回编解码器名称的小写形式 |

--------|------|----------|------|------|
| `openFileAndReadFooter` | hadoopConf: Configuration, file: PartitionedFile, keepInputStreamOpen: boolean | `OpenedParquetFooter` | 打开FileAndReadFooter相关功能 | 传入参数执行打开FileAndReadFooter相关功能 |
| `readFooter` | inputFile: HadoopInputFile, filter: ParquetMetadataConverter.MetadataFilter | `ParquetMetadata` | 读取Footer相关功能 | 传入参数执行读取Footer相关功能 |

--------|------|----------|------|------|
| `decodeSingleDictionaryId` | offset: int, values: WritableColumnVector, dictionaryIds: WritableColumnVector, dictionary: Dictionary | `void` | 解码SingleDictionaryId相关功能 | 传入参数执行解码SingleDictionaryId相关功能 |
| `getUpdater` | descriptor: ColumnDescriptor, sparkType: DataType | `ParquetVectorUpdater` | 获取Updater相关功能 | 传入参数执行获取Updater相关功能 |
| `readValue` | offset: int, values: WritableColumnVector, valuesReader: VectorizedValuesReader | `void` | 读取Value相关功能 | 传入参数执行读取Value相关功能 |
| `readValues` | total: int, offset: int, values: WritableColumnVector, valuesReader: VectorizedValuesReader | `void` | 读取Values相关功能 | 传入参数执行读取Values相关功能 |
| `skipValues` | total: int, valuesReader: VectorizedValuesReader | `void` | 跳过Values相关功能 | 传入参数执行跳过Values相关功能 |


--------|------|----------|------|------|
| `allocate` | keySchema: StructType, valueSchema: StructType, manager: TaskMemoryManager | `RowBasedKeyValueBatch` | 分配相关功能 | 传入参数执行分配相关功能 |
| `allocate` | keySchema: StructType, valueSchema: StructType, manager: TaskMemoryManager, maxRows: int | `RowBasedKeyValueBatch` | 分配相关功能 | 传入参数执行分配相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getValueRow` | rowId: int | `UnsafeRow` | 获取ValueRow相关功能 | 传入参数执行获取ValueRow相关功能 |
| `numRows` | 无 | `int` | numRows操作 | 调用该方法执行numRows操作 |
| `spill` | size: long, trigger: MemoryConsumer | `long` | spill操作 | 传入参数执行spill操作 |

### RowFactory
**包路径**: `org.apache.spark.sql`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | values: Object ... | `Row` | 创建相关功能 | 传入参数执行创建相关功能 |

--------|------|----------|------|------|
| `getColumn` | 无 | `String` | 获取Column相关功能 | 调用该方法执行获取Column相关功能 |
| `getLogicalType` | 无 | `String` | 获取LogicalType相关功能 | 调用该方法执行获取LogicalType相关功能 |
| `getPhysicalType` | 无 | `String` | 获取PhysicalType相关功能 | 调用该方法执行获取PhysicalType相关功能 |


--------|------|----------|------|------|
| `getInstance` | 无 | `SpatialReferenceSystemCache` | 获取Instance相关功能 | 调用该方法执行获取Instance相关功能 |
| `getSridToSrs` | 无 | `Map&lt;Integer, SpatialReferenceSystemInformation&gt;` | 获取SridToSrs相关功能 | 调用该方法执行获取SridToSrs相关功能 |
| `getSrsInfo` | srid: int | `SpatialReferenceSystemInformation` | 获取SrsInfo相关功能 | 传入参数执行获取SrsInfo相关功能 |
| `getSrsInfo` | stringId: String | `SpatialReferenceSystemInformation` | 获取SrsInfo相关功能 | 传入参数执行获取SrsInfo相关功能 |
| `getStringIdToSrs` | 无 | `Map&lt;String, SpatialReferenceSystemInformation&gt;` | 获取StringIdToSrs相关功能 | 调用该方法执行获取StringIdToSrs相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getCurrentKey` | 无 | `Void` | 获取CurrentKey相关功能 | 调用该方法执行获取CurrentKey相关功能 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext, inputFile: Option<HadoopInputFile>, inputStream: Option<SeekableInputStream>, fileFooter: Option<ParquetMetadata> | `void` | 初始化插件 | 初始化目录插件 |
| `readNextRowGroup` | 无 | `PageReadStore` | 读取NextRowGroup相关功能 | 调用该方法执行读取NextRowGroup相关功能 |


--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `eval` | xml: String, path: String, qname: QName | `Object` | eval操作 | 传入参数执行eval操作 |
| `evalBoolean` | xml: String, path: String | `Boolean` | evalBoolean操作 | 传入参数执行evalBoolean操作 |
| `evalNode` | xml: String, path: String | `Node` | evalNode操作 | 传入参数执行evalNode操作 |
| `evalNodeList` | xml: String, path: String | `NodeList` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `evalNumber` | xml: String, path: String | `Double` | evalNumber操作 | 传入参数执行evalNumber操作 |
| `evalString` | xml: String, path: String | `String` | 三相关功能 | 传入参数执行三相关功能 |
| `mark` | readAheadLimit: int | `void` | mark操作 | 传入参数执行mark操作 |
| `markSupported` | 无 | `boolean` | 支持相关功能 | 调用该方法执行支持相关功能 |
| `read` | 无 | `int` | 读取数据源创建DataFrame | 调用该方法执行读取相关功能 |
| `read` | cbuf: char&lt;&gt;, off: int, len: int | `int` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |
| `ready` | 无 | `boolean` | 读取y相关功能 | 调用该方法执行读取y相关功能 |
| `reset` | 无 | `void` | 重置相关功能 | 调用该方法执行重置相关功能 |
| `set` | s: String | `void` | 设置元素 | 传入参数执行设置相关功能 |
| `skip` | ns: long | `long` | 跳过相关功能 | 传入参数执行跳过相关功能 |


--------|------|----------|------|------|
| `cursor` | 无 | `int` | cursor操作 | 调用该方法执行cursor操作 |
| `getBufferHolder` | 无 | `BufferHolder` | 获取BufferHolder相关功能 | 调用该方法执行获取BufferHolder相关功能 |
| `grow` | neededSize: int | `void` | grow操作 | 传入参数执行grow操作 |
| `increaseCursor` | val: int | `void` | increaseCursor操作 | 传入参数执行increaseCursor操作 |
| `reset` | 无 | `void` | 重置相关功能 | 调用该方法执行重置相关功能 |
| `setOffsetAndSizeFromPreviousCursor` | ordinal: int, previousCursor: int | `void` | 设置OffsetAndSizeFromPreviousCursor相关功能 | 传入参数执行设置OffsetAndSizeFromPreviousCursor相关功能 |
| `totalSize` | 无 | `int` | totalSize操作 | 调用该方法执行totalSize操作 |
| `write` | ordinal: int, input: UTF8String | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: GeographyVal | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: GeometryVal | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: byte&lt;&gt; | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: byte&lt;&gt;, offset: int, numBytes: int | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: CalendarInterval | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, input: VariantVal | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, row: UnsafeRow | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | ordinal: int, map: UnsafeMapData | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `write` | array: UnsafeArrayData | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |


--------|------|----------|------|------|
| `vectorCosineSimilarity` | left: ArrayData, right: ArrayData, funcName: UTF8String | `Float` | vectorCosineSimilarity操作 | 传入参数执行vectorCosineSimilarity操作 |
| `vectorInfNorm` | vec: ArrayData | `Float` | vectorInfNorm操作 | 传入参数执行vectorInfNorm操作 |
| `vectorInnerProduct` | left: ArrayData, right: ArrayData, funcName: UTF8String | `Float` | vectorInnerProduct操作 | 传入参数执行vectorInnerProduct操作 |
| `vectorL1Norm` | vec: ArrayData | `Float` | vectorL1Norm操作 | 传入参数执行vectorL1Norm操作 |
| `vectorL2Distance` | left: ArrayData, right: ArrayData, funcName: UTF8String | `Float` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `vectorL2Norm` | vec: ArrayData | `Float` | vectorL2Norm操作 | 传入参数执行vectorL2Norm操作 |
| `vectorNorm` | vec: ArrayData, degree: float, funcName: UTF8String | `Float` | vectorNorm操作 | 传入参数执行vectorNorm操作 |
| `vectorNormalize` | vec: ArrayData, degree: float, funcName: UTF8String | `ArrayData` | 正常相关功能 | 传入参数执行正常相关功能 |
| `vectorNormalizeWithNorm` | vec: ArrayData, norm: float | `ArrayData` | 正常相关功能 | 传入参数执行正常相关功能 |

--------|------|----------|------|------|
| `visit` | dataPageV1: DataPageV1 | `Integer` | 访问相关功能 | 传入参数执行访问相关功能 |
| `visit` | dataPageV2: DataPageV2 | `Integer` | 访问相关功能 | 传入参数执行访问相关功能 |

--------|------|----------|------|------|
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readByte` | 无 | `byte` | 读取Byte相关功能 | 调用该方法执行读取Byte相关功能 |
| `readBytes` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Bytes相关功能 | 传入参数执行读取Bytes相关功能 |
| `readInteger` | 无 | `int` | 读取Integer相关功能 | 调用该方法执行读取Integer相关功能 |
| `readIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Integers相关功能 | 传入参数执行读取Integers相关功能 |
| `readIntegersWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean | `void` | 读取IntegersWithRebase相关功能 | 传入参数执行读取IntegersWithRebase相关功能 |
| `readLong` | 无 | `long` | 读取Long相关功能 | 调用该方法执行读取Long相关功能 |
| `readLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Longs相关功能 | 传入参数执行读取Longs相关功能 |
| `readLongsWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean, timeZone: String | `void` | 读取LongsWithRebase相关功能 | 传入参数执行读取LongsWithRebase相关功能 |
| `readShort` | 无 | `short` | 读取Short相关功能 | 调用该方法执行读取Short相关功能 |
| `readShorts` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Shorts相关功能 | 传入参数执行读取Shorts相关功能 |
| `readUnsignedIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedIntegers相关功能 | 传入参数执行读取UnsignedIntegers相关功能 |
| `readUnsignedLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedLongs相关功能 | 传入参数执行读取UnsignedLongs相关功能 |
| `skipBytes` | total: int | `void` | 跳过Bytes相关功能 | 传入参数执行跳过Bytes相关功能 |
| `skipIntegers` | total: int | `void` | 跳过Integers相关功能 | 传入参数执行跳过Integers相关功能 |
| `skipLongs` | total: int | `void` | 跳过Longs相关功能 | 传入参数执行跳过Longs相关功能 |
| `skipShorts` | total: int | `void` | 跳过Shorts相关功能 | 传入参数执行跳过Shorts相关功能 |

--------|------|----------|------|------|
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readBinary` | len: int | `Binary` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBinary` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readGeography` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `setPreviousReader` | reader: ValuesReader | `void` | 设置PreviousReader相关功能 | 传入参数执行设置PreviousReader相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |

--------|------|----------|------|------|
| `getBytes` | rowId: int | `ByteBuffer` | 获取Bytes相关功能 | 传入参数执行获取Bytes相关功能 |
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readBinary` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readGeography` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `enableReturningBatches` | 无 | `void` | 启用ReturningBatches相关功能 | 调用该方法执行启用ReturningBatches相关功能 |
| `getCurrentValue` | 无 | `Object` | 获取CurrentValue相关功能 | 调用该方法执行获取CurrentValue相关功能 |
| `getProgress` | 无 | `float` | 获取Progress相关功能 | 调用该方法执行获取Progress相关功能 |
| `initBatch` | partitionColumns: StructType, partitionValues: InternalRow | `void` | 初始化Batch相关功能 | 传入参数执行初始化Batch相关功能 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | inputSplit: InputSplit, taskAttemptContext: TaskAttemptContext, inputFile: Option<HadoopInputFile>, inputStream: Option<SeekableInputStream>, fileFooter: Option<ParquetMetadata> | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | path: String, columns: List<String> | `void` | 初始化插件 | 初始化目录插件 |
| `initialize` | fileSchema: MessageType, requestedSchema: MessageType, rowGroupReader: ParquetRowGroupReader, totalRowCount: int | `void` | 初始化插件 | 初始化目录插件 |
| `nextBatch` | 无 | `boolean` | 之后Batch相关功能 | 调用该方法执行之后Batch相关功能 |
| `nextKeyValue` | 无 | `boolean` | 之后KeyValue相关功能 | 调用该方法执行之后KeyValue相关功能 |
| `resultBatch` | 无 | `ColumnarBatch` | resultBatch操作 | 调用该方法执行resultBatch操作 |

--------|------|----------|------|------|
| `initFromPage` | valueCount: int, in: ByteBufferInputStream | `void` | 初始化FromPage相关功能 | 传入参数执行初始化FromPage相关功能 |
| `readBinary` | total: int, v: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBinary` | len: int | `Binary` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBoolean` | 无 | `boolean` | 读取Boolean相关功能 | 调用该方法执行读取Boolean相关功能 |
| `readBooleans` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Booleans相关功能 | 传入参数执行读取Booleans相关功能 |
| `readByte` | 无 | `byte` | 读取Byte相关功能 | 调用该方法执行读取Byte相关功能 |
| `readBytes` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Bytes相关功能 | 传入参数执行读取Bytes相关功能 |
| `readDouble` | 无 | `double` | 读取Double相关功能 | 调用该方法执行读取Double相关功能 |
| `readDoubles` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Doubles相关功能 | 传入参数执行读取Doubles相关功能 |
| `readFloat` | 无 | `float` | 读取Float相关功能 | 调用该方法执行读取Float相关功能 |
| `readFloats` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Floats相关功能 | 传入参数执行读取Floats相关功能 |
| `readGeography` | total: int, v: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, v: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `readInteger` | 无 | `int` | 读取Integer相关功能 | 调用该方法执行读取Integer相关功能 |
| `readIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Integers相关功能 | 传入参数执行读取Integers相关功能 |
| `readIntegersWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean | `void` | 读取IntegersWithRebase相关功能 | 传入参数执行读取IntegersWithRebase相关功能 |
| `readLong` | 无 | `long` | 读取Long相关功能 | 调用该方法执行读取Long相关功能 |
| `readLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Longs相关功能 | 传入参数执行读取Longs相关功能 |
| `readLongsWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean, timeZone: String | `void` | 读取LongsWithRebase相关功能 | 传入参数执行读取LongsWithRebase相关功能 |
| `readShort` | 无 | `short` | 读取Short相关功能 | 调用该方法执行读取Short相关功能 |
| `readShorts` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Shorts相关功能 | 传入参数执行读取Shorts相关功能 |
| `readUnsignedIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedIntegers相关功能 | 传入参数执行读取UnsignedIntegers相关功能 |
| `readUnsignedLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedLongs相关功能 | 传入参数执行读取UnsignedLongs相关功能 |
| `skip` | 无 | `void` | 跳过相关功能 | 调用该方法执行跳过相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |
| `skipBooleans` | total: int | `void` | 跳过Booleans相关功能 | 传入参数执行跳过Booleans相关功能 |
| `skipBytes` | total: int | `void` | 跳过Bytes相关功能 | 传入参数执行跳过Bytes相关功能 |
| `skipDoubles` | total: int | `void` | 跳过Doubles相关功能 | 传入参数执行跳过Doubles相关功能 |
| `skipFixedLenByteArray` | total: int, len: int | `void` | 跳过FixedLenByteArray相关功能 | 传入参数执行跳过FixedLenByteArray相关功能 |
| `skipFloats` | total: int | `void` | 跳过Floats相关功能 | 传入参数执行跳过Floats相关功能 |
| `skipIntegers` | total: int | `void` | 跳过Integers相关功能 | 传入参数执行跳过Integers相关功能 |
| `skipLongs` | total: int | `void` | 跳过Longs相关功能 | 传入参数执行跳过Longs相关功能 |
| `skipShorts` | total: int | `void` | 跳过Shorts相关功能 | 传入参数执行跳过Shorts相关功能 |

--------|------|----------|------|------|
| `readBinary` | len: int | `Binary` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBinary` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Binary相关功能 | 传入参数执行读取Binary相关功能 |
| `readBooleans` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Booleans相关功能 | 传入参数执行读取Booleans相关功能 |
| `readByte` | 无 | `byte` | 读取Byte相关功能 | 调用该方法执行读取Byte相关功能 |
| `readBytes` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Bytes相关功能 | 传入参数执行读取Bytes相关功能 |
| `readDoubles` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Doubles相关功能 | 传入参数执行读取Doubles相关功能 |
| `readFloats` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Floats相关功能 | 传入参数执行读取Floats相关功能 |
| `readGeography` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geography相关功能 | 传入参数执行读取Geography相关功能 |
| `readGeometry` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Geometry相关功能 | 传入参数执行读取Geometry相关功能 |
| `readIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Integers相关功能 | 传入参数执行读取Integers相关功能 |
| `readIntegersWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean | `void` | 读取IntegersWithRebase相关功能 | 传入参数执行读取IntegersWithRebase相关功能 |
| `readLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Longs相关功能 | 传入参数执行读取Longs相关功能 |
| `readLongsWithRebase` | total: int, c: WritableColumnVector, rowId: int, failIfRebase: boolean, timeZone: String | `void` | 读取LongsWithRebase相关功能 | 传入参数执行读取LongsWithRebase相关功能 |
| `readShort` | 无 | `short` | 读取Short相关功能 | 调用该方法执行读取Short相关功能 |
| `readShorts` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取Shorts相关功能 | 传入参数执行读取Shorts相关功能 |
| `readUnsignedIntegers` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedIntegers相关功能 | 传入参数执行读取UnsignedIntegers相关功能 |
| `readUnsignedLongs` | total: int, c: WritableColumnVector, rowId: int | `void` | 读取UnsignedLongs相关功能 | 传入参数执行读取UnsignedLongs相关功能 |
| `skip` | 无 | `void` | 跳过相关功能 | 调用该方法执行跳过相关功能 |
| `skipBinary` | total: int | `void` | 跳过Binary相关功能 | 传入参数执行跳过Binary相关功能 |
| `skipBooleans` | total: int | `void` | 跳过Booleans相关功能 | 传入参数执行跳过Booleans相关功能 |
| `skipBytes` | total: int | `void` | 跳过Bytes相关功能 | 传入参数执行跳过Bytes相关功能 |
| `skipDoubles` | total: int | `void` | 跳过Doubles相关功能 | 传入参数执行跳过Doubles相关功能 |
| `skipFixedLenByteArray` | total: int, len: int | `void` | 跳过FixedLenByteArray相关功能 | 传入参数执行跳过FixedLenByteArray相关功能 |
| `skipFloats` | total: int | `void` | 跳过Floats相关功能 | 传入参数执行跳过Floats相关功能 |
| `skipIntegers` | total: int | `void` | 跳过Integers相关功能 | 传入参数执行跳过Integers相关功能 |
| `skipLongs` | total: int | `void` | 跳过Longs相关功能 | 传入参数执行跳过Longs相关功能 |
| `skipShorts` | total: int | `void` | 跳过Shorts相关功能 | 传入参数执行跳过Shorts相关功能 |


--------|------|----------|------|------|
| `getParseError` | 无 | `String` | 获取ParseError相关功能 | 调用该方法执行获取ParseError相关功能 |
| `getPosition` | 无 | `long` | 获取Position相关功能 | 调用该方法执行获取Position相关功能 |

--------|------|----------|------|------|
| `read` | wkb: byte&lt;&gt; | `GeometryModel` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |
| `read` | wkb: byte&lt;&gt;, srid: int | `GeometryModel` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |

--------|------|----------|------|------|
| `addElementsAppended` | num: int | `void` | 增加已追加元素计数，用于手动调整追加位置 | `vector.addElementsAppended(5);<br>// 增加5个元素的计数` |
| `appendArray` | length: int | `int` | 追加数组数据，返回追加的起始位置 | `int startPos = vector.appendArray(3);<br>// 追加长度为3的数组` |
| `appendBoolean` | v: boolean | `int` | 追加单个布尔值，返回追加位置 | `int pos = vector.appendBoolean(true);` |
| `appendBooleans` | count: int, v: boolean | `int` | 批量追加相同布尔值，返回起始位置 | `int startPos = vector.appendBooleans(10, true);<br>// 追加10个true值` |
| `appendBooleans` | count: int, src: byte, offset: int | `int` | 从字节位图追加布尔值，src每bit对应一个布尔 | `byte bitmap = 0x0F;  // 低4位为true<br>int pos = vector.appendBooleans(4, bitmap, 0);` |
| `appendByte` | v: byte | `int` | 追加单个字节值 | `int pos = vector.appendByte((byte) 100);` |
| `appendByteArray` | value: byte[], offset: int, length: int | `int` | 追加字节数组的部分内容 | `byte[] data = {1, 2, 3, 4, 5};<br>int pos = vector.appendByteArray(data, 1, 3);  // 追加{2,3,4}` |
| `appendBytes` | count: int, v: byte | `int` | 批量追加相同字节值 | `int pos = vector.appendBytes(100, (byte) 0);<br>// 追加100个0字节` |
| `appendBytes` | length: int, src: byte[], offset: int | `int` | 批量追加字节数组内容 | `byte[] src = {1, 2, 3};<br>int pos = vector.appendBytes(3, src, 0);` |
| `appendDouble` | v: double | `int` | 追加单个双精度值 | `int pos = vector.appendDouble(3.14);` |
| `appendDoubles` | count: int, v: double | `int` | 批量追加相同双精度值 | `int pos = vector.appendDoubles(10, 1.5);` |
| `appendDoubles` | length: int, src: double[], offset: int | `int` | 批量追加双精度数组内容 | `double[] values = {1.1, 2.2, 3.3};<br>int pos = vector.appendDoubles(3, values, 0);` |
| `appendFloat` | v: float | `int` | 追加单个单精度值 | `int pos = vector.appendFloat(2.5f);` |
| `appendFloats` | count: int, v: float | `int` | 批量追加相同单精度值 | `int pos = vector.appendFloats(5, 1.0f);` |
| `appendFloats` | length: int, src: float[], offset: int | `int` | 批量追加单精度数组内容 | `float[] values = {1.0f, 2.0f};<br>int pos = vector.appendFloats(2, values, 0);` |
| `appendInt` | v: int | `int` | 追加单个整数值 | `int pos = vector.appendInt(42);` |
| `appendInts` | count: int, v: int | `int` | 批量追加相同整数值 | `int pos = vector.appendInts(100, 0);` |
| `appendInts` | length: int, src: int[], offset: int | `int` | 批量追加整数数组内容 | `int[] values = {1, 2, 3, 4, 5};<br>int pos = vector.appendInts(3, values, 2);  // 追加{3,4,5}` |
| `appendLong` | v: long | `int` | 追加单个长整数值 | `int pos = vector.appendLong(100000L);` |
| `appendLongs` | count: int, v: long | `int` | 批量追加相同长整数值 | `int pos = vector.appendLongs(10, 0L);` |
| `appendLongs` | length: int, src: long[], offset: int | `int` | 批量追加长整数数组内容 | `long[] values = {1L, 2L, 3L};<br>int pos = vector.appendLongs(3, values, 0);` |
| `appendNotNull` | 无 | `int` | 追加非null标记，返回追加位置 | `int pos = vector.appendNotNull();` |
| `appendNotNulls` | count: int | `int` | 批量追加非null标记 | `int pos = vector.appendNotNulls(100);` |
| `appendNull` | 无 | `int` | 追加null标记，返回追加位置 | `int pos = vector.appendNull();` |
| `appendNulls` | count: int | `int` | 批量追加null标记 | `int pos = vector.appendNulls(10);` |
| `appendObjects` | length: int, value: Object | `Optional<Integer>` | 追加对象数组（不常用，部分类型不支持） | `Optional&lt;Integer&gt; pos = vector.appendObjects(1, obj);` |
| `appendShort` | v: short | `int` | 追加单个短整数值 | `int pos = vector.appendShort((short) 100);` |
| `appendShorts` | count: int, v: short | `int` | 批量追加相同短整数值 | `int pos = vector.appendShorts(5, (short) 10);` |
| `appendShorts` | length: int, src: short[], offset: int | `int` | 批量追加短整数数组内容 | `short[] values = {1, 2, 3};<br>int pos = vector.appendShorts(3, values, 0);` |
| `appendStruct` | isNull: boolean | `int` | 追加Struct结构，isNull指定是否为null | `int pos = vector.appendStruct(false);<br>// 需后续填充子字段` |
| `arrayData` | 无 | `WritableColumnVector` | 获取存储数组数据的底层列向量 | `WritableColumnVector arrData = vector.arrayData();<br>// 用于写入Array类型的元素` |
| `close` | 无 | `void` | 关闭列向量，释放内存和子列向量 | `vector.close();` |
| `closeIfFreeable` | 无 | `void` | 无操作（实现类可能重写） | `// 默认为空实现` |
| `getArray` | rowId: int | `ColumnarArray` | 获取指定行的数组数据 | `ColumnarArray arr = vector.getArray(0);` |
| `getChild` | ordinal: int | `WritableColumnVector` | 获取嵌套类型的子列向量 | `WritableColumnVector child = vector.getChild(0);<br>// 用于写入Struct字段` |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取指定行的Decimal值 | `Decimal dec = vector.getDecimal(0, 10, 2);` |
| `getDictionaryIds` | 无 | `WritableColumnVector` | 获取字典编码的ID列向量 | `WritableColumnVector dictIds = vector.getDictionaryIds();` |
| `getElementsAppended` | 无 | `int` | 获取已追加元素的数量 | `int count = vector.getElementsAppended();` |
| `getMap` | rowId: int | `ColumnarMap` | 获取指定行的Map数据 | `ColumnarMap map = vector.getMap(0);` |
| `getNumChildren` | 无 | `int` | 获取子列向量数量 | `int numChildren = vector.getNumChildren();` |
| `getUTF8String` | rowId: int | `UTF8String` | 获取指定行的UTF8字符串 | `UTF8String str = vector.getUTF8String(0);` |
| `hasDictionary` | 无 | `boolean` | 检查是否使用字典编码 | `if (vector.hasDictionary()) {<br>    // 使用字典解码读取<br>}` |
| `hasNull` | 无 | `boolean` | 检查是否存在null值 | `boolean hasNulls = vector.hasNull();` |
| `isAllNull` | 无 | `boolean` | 检查是否所有值都是null | `boolean allNull = vector.isAllNull();` |
| `isMissing` | 无 | `boolean` | 检查是否为缺失状态 | `boolean missing = vector.isMissing();` |
| `numNulls` | 无 | `int` | 返回null值数量 | `int nullCount = vector.numNulls();` |
| `putBooleans` | rowId: int, count: int, src: byte, srcIndex: int | `void` | 从位图写入布尔值到指定位置 | `byte bitmap = 0x55;<br>vector.putBooleans(0, 4, bitmap, 0);` |
| `putByteArray` | rowId: int, value: byte[] | `int` | 写入字节数组到指定行 | `int offset = vector.putByteArray(0, new byte[]{1,2,3});` |
| `putByteArray` | rowId: int, src: ByteBuffer, srcPosition: int, length: int | `int` | 从ByteBuffer写入字节数组 | `ByteBuffer buf = ByteBuffer.wrap(data);<br>int offset = vector.putByteArray(0, buf, 0, 10);` |
| `putDecimal` | rowId: int, value: Decimal, precision: int | `void` | 写入Decimal值到指定行 | `Decimal dec = Decimal.apply(123.45);<br>vector.putDecimal(0, dec, 10);` |
| `putInterval` | rowId: int, value: CalendarInterval | `void` | 写入时间间隔到指定行 | `CalendarInterval interval = new CalendarInterval(1, 2, 1000L);<br>vector.putInterval(0, interval);` |
| `reserve` | requiredCapacity: int | `void` | 预留指定容量的内存空间，写入前必须调用 | `vector.reserve(1000);<br>// 预留1000个元素的容量` |
| `reserveAdditional` | additionalCapacity: int | `void` | 预留额外的内存空间（追加当前容量） | `vector.reserveAdditional(100);<br>// 增加100容量` |
| `reserveDictionaryIds` | capacity: int | `WritableColumnVector` | 为字典编码预留ID存储空间 | `WritableColumnVector dictIds = vector.reserveDictionaryIds(1000);` |
| `reset` | 无 | `void` | 重置列向量，清空数据准备重新写入 | `vector.reset();<br>// 清空数据，重置计数器` |
| `setDictionary` | dictionary: Dictionary | `void` | 设置字典编码对象 | `vector.setDictionary(dictionary);<br>// 启用字典解码` |
| `setIsConstant` | 无 | `void` | 设置为常量列向量 | `vector.setIsConstant();<br>// 标记为常量值列` |
| `setMissing` | 无 | `void` | 设置为缺失状态 | `vector.setMissing();` |


---

## Streaming流处理

### BatchStatus
**包路径**: `org.apache.spark.status.api.v1.streaming`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `BatchStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |

--------|------|----------|------|------|
| `call` | userId: String, events: Iterator<Row>, state: GroupState<Sessions> | `Iterator&lt;Session&gt;` | 调用相关功能 | 传入参数执行调用相关功能 |
| `endTime` | 无 | `Timestamp` | 结束Time相关功能 | 调用该方法执行结束Time相关功能 |
| `getDuration` | 无 | `long` | 获取Duration相关功能 | 调用该方法执行获取Duration相关功能 |
| `getEndTimestamp` | 无 | `Timestamp` | 获取EndTimestamp相关功能 | 调用该方法执行获取EndTimestamp相关功能 |
| `getEventType` | 无 | `EventTypes` | 获取EventType相关功能 | 调用该方法执行获取EventType相关功能 |
| `getEvents` | 无 | `List&lt;SessionEvent&gt;` | 获取Events相关功能 | 调用该方法执行获取Events相关功能 |
| `getId` | 无 | `String` | 获取Id相关功能 | 调用该方法执行获取Id相关功能 |
| `getNumEvents` | 无 | `int` | 获取NumEvents相关功能 | 调用该方法执行获取NumEvents相关功能 |
| `getSessions` | 无 | `List&lt;SessionAcc&gt;` | 获取Sessions相关功能 | 调用该方法执行获取Sessions相关功能 |
| `getStartTimestamp` | 无 | `Timestamp` | 获取StartTimestamp相关功能 | 调用该方法执行获取StartTimestamp相关功能 |
| `getUserId` | 无 | `String` | 获取UserId相关功能 | 调用该方法执行获取UserId相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `newInstance` | sessions: List<SessionAcc> | `Sessions` | newInstance操作 | 传入参数执行newInstance操作 |
| `newInstance` | userId: String, eventTypeStr: String, startTimestamp: Timestamp, gapDuration: long | `SessionEvent` | newInstance操作 | 传入参数执行newInstance操作 |
| `newInstance` | event: SessionEvent | `SessionAcc` | newInstance操作 | 传入参数执行newInstance操作 |
| `newInstance` | events: List<SessionEvent> | `SessionAcc` | newInstance操作 | 传入参数执行newInstance操作 |
| `newInstance` | id: String, duration: long, numEvents: int | `Session` | newInstance操作 | 传入参数执行newInstance操作 |
| `setDuration` | duration: long | `void` | 设置Duration相关功能 | 传入参数执行设置Duration相关功能 |
| `setEndTimestamp` | endTimestamp: Timestamp | `void` | 设置EndTimestamp相关功能 | 传入参数执行设置EndTimestamp相关功能 |
| `setEventType` | eventType: EventTypes | `void` | 设置EventType相关功能 | 传入参数执行设置EventType相关功能 |
| `setEvents` | events: List<SessionEvent> | `void` | 设置Events相关功能 | 传入参数执行设置Events相关功能 |
| `setId` | id: String | `void` | 设置Id相关功能 | 传入参数执行设置Id相关功能 |
| `setNumEvents` | numEvents: int | `void` | 设置NumEvents相关功能 | 传入参数执行设置NumEvents相关功能 |
| `setSessions` | sessions: List<SessionAcc> | `void` | 设置Sessions相关功能 | 传入参数执行设置Sessions相关功能 |
| `setStartTimestamp` | startTimestamp: Timestamp | `void` | 设置StartTimestamp相关功能 | 传入参数执行设置StartTimestamp相关功能 |
| `setUserId` | userId: String | `void` | 设置UserId相关功能 | 传入参数执行设置UserId相关功能 |
| `startTime` | 无 | `Timestamp` | 启动Time相关功能 | 调用该方法执行启动Time相关功能 |

### GroupStateTimeout
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `EventTimeTimeout` | 无 | `GroupStateTimeout` | 基于事件时间的超时策略，状态在水印超时时被清理 | // 事件时间超时，适合处理延迟数据<br>.timeout(EventTimeTimeout())<br>// 状态在eventTime超过watermark时触发超时清理 |
| `NoTimeout` | 无 | `GroupStateTimeout` | 无超时策略，状态永不自动清理，需手动管理 | // 不设置超时，状态永久保留<br>.timeout(NoTimeout())<br>// 需要手动调用remove()清理状态 |
| `ProcessingTimeTimeout` | 无 | `GroupStateTimeout` | 基于处理时间的超时策略，状态在指定时间后清理 | // 处理时间超时，定时清理状态<br>.timeout(ProcessingTimeTimeout())<br>// 状态在processingTime超过阈值时触发超时清理 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `onStart` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |
| `onStop` | 无 | `void` | 停止相关功能 | 调用该方法执行停止相关功能 |

--------|------|----------|------|------|
| `getWord` | 无 | `String` | 获取Word相关功能 | 调用该方法执行获取Word相关功能 |
| `setWord` | word: String | `void` | 设置Word相关功能 | 传入参数执行设置Word相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

### KinesisInitialPositions
**包路径**: `org.apache.spark.streaming.kinesis`
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromKinesisInitialPosition` | initialPositionInStream: InitialPositionInStream | `KinesisInitialPosition` | 初始化相关功能 | 传入参数执行初始化相关功能 |
| `getPosition` | 无 | `InitialPositionInStream` | 获取Position相关功能 | 调用该方法执行获取Position相关功能 |
| `getTimestamp` | 无 | `Date` | 获取Timestamp相关功能 | 调用该方法执行获取Timestamp相关功能 |

### OutputMode
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Append` | 无 | `OutputMode` | Append输出模式，只输出新增结果，适合无状态或聚合查询 | // Append模式：只输出新数据<br>// 适合无聚合操作或事件时间水印查询<br>.outputMode("append")<br>// 只追加新行，不修改已有数据 |
| `Complete` | 无 | `OutputMode` | Complete输出模式，输出完整结果，适合聚合查询 | // Complete模式：输出全部结果<br>// 适合聚合查询（如groupBy后count）<br>.outputMode("complete")<br>// 每次输出完整聚合结果表 |
| `Update` | 无 | `OutputMode` | Update输出模式，只输出更新的行，适合聚合查询 | // Update模式：只输出变更的行<br>// 适合聚合查询，只输出有更新的分组<br>.outputMode("update")<br>// 仅输出被更新或新增的行 |

### ReadMaxBytes
**包路径**: `org.apache.spark.sql.connector.read.streaming`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `maxBytes` | 无 | `long` | maxBytes操作 | 调用该方法执行maxBytes操作 |

### ReadMaxFiles
**包路径**: `org.apache.spark.sql.connector.read.streaming`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `maxFiles` | 无 | `int` | maxFiles操作 | 调用该方法执行maxFiles操作 |

### SupportsRealTimeRead
**包路径**: `org.apache.spark.sql.connector.read.streaming`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `hasRecord` | 无 | `boolean` | 检查是否存在Record相关功能 | 调用该方法执行检查是否存在Record相关功能 |
| `newStatusWithArrivalTimeMs` | recArrivalTime: Long | `RecordStatus` | 时间相关功能 | 传入参数执行时间相关功能 |
| `newStatusWithoutArrivalTime` | hasRecord: boolean | `RecordStatus` | 时间相关功能 | 传入参数执行时间相关功能 |
| `recArrivalTime` | 无 | `Optional&lt;Long&gt;` | 时间相关功能 | 调用该方法执行时间相关功能 |

### TimeMode
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `EventTime` | 无 | `TimeMode` | EventTime时间模式，使用事件时间戳处理数据，支持水印和延迟数据 | // 事件时间模式：基于数据中的时间字段<br>// 支持水印处理延迟数据<br>.withWatermark("timestamp", "10 minutes")<br>// 按数据携带的时间戳处理 |
| `None` | 无 | `TimeMode` | None时间模式，不使用时间概念，适用于无时间语义的处理 | // 无时间模式：不考虑时间<br>// 适用于简单映射、过滤等无时间语义操作<br>// 不支持水印和超时功能 |
| `ProcessingTime` | 无 | `TimeMode` | ProcessingTime时间模式，使用处理时间（系统时钟），不支持延迟数据处理 | // 处理时间模式：基于Spark处理时间<br>// 使用系统时钟，不处理延迟数据<br>// 结果依赖于数据处理时刻 |

### Trigger
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AvailableNow` | 无 | `Trigger` | AvailableNow触发器，一次性处理所有可用数据，适合批量加载 | // AvailableNow：处理所有可用数据后停止<br>// 适合一次性批量处理或历史数据加载<br>.trigger(Trigger.AvailableNow())<br>// 处理完所有数据后自动停止查询 |
| `Continuous` | intervalMs: long | `Trigger` | Continuous连续触发器，毫秒级低延迟连续处理，支持持续查询 | // Continuous：连续处理模式（毫秒）<br>// 低延迟连续处理，适合实时流处理<br>.trigger(Trigger.Continuous(100))  // 100ms间隔 |
| `Continuous` | interval: long, timeUnit: TimeUnit | `Trigger` | Continuous连续触发器，指定时间单位，支持灵活间隔设置 | // Continuous：连续处理（指定时间单位）<br>.trigger(Trigger.Continuous(1, TimeUnit.SECONDS))  // 1秒间隔 |
| `Continuous` | interval: Duration | `Trigger` | Continuous连续触发器，使用Java Duration对象设置间隔 | // Continuous：使用Duration设置间隔<br>.trigger(Trigger.Continuous(Duration.ofSeconds(5))) |
| `Continuous` | interval: String | `Trigger` | Continuous连续触发器，字符串格式设置间隔（如"5 seconds"） | // Continuous：字符串格式设置间隔<br>.trigger(Trigger.Continuous("5 seconds")) |
| `Once` | 无 | `Trigger` | Once触发器，执行一次批处理后停止，适合一次性查询 | // Once：执行一次批处理后停止<br>// 适合测试或一次性数据处理<br>.trigger(Trigger.Once())<br>// 处理完当前数据后停止查询 |
| `ProcessingTime` | intervalMs: long | `Trigger` | ProcessingTime定时触发器，按固定毫秒间隔触发批处理 | // ProcessingTime：固定间隔触发（毫秒）<br>// 微批处理模式，按固定间隔执行<br>.trigger(Trigger.ProcessingTime(5000))  // 5秒间隔 |
| `ProcessingTime` | interval: long, timeUnit: TimeUnit | `Trigger` | ProcessingTime定时触发器，指定时间单位设置间隔 | // ProcessingTime：固定间隔（指定时间单位）<br>.trigger(Trigger.ProcessingTime(1, TimeUnit.MINUTES))  // 1分钟间隔 |
| `ProcessingTime` | interval: Duration | `Trigger` | ProcessingTime定时触发器，使用Java Duration对象设置间隔 | // ProcessingTime：使用Duration设置间隔<br>.trigger(Trigger.ProcessingTime(Duration.ofMinutes(10))) |
| `ProcessingTime` | interval: String | `Trigger` | ProcessingTime定时触发器，字符串格式设置间隔（如"1 minute"） | // ProcessingTime：字符串格式设置间隔<br>.trigger(Trigger.ProcessingTime("1 minute")) |
| `RealTime` | batchDurationMs: long | `Trigger` | RealTime实时触发器，毫秒级实时处理新数据 | // RealTime：实时处理新数据（毫秒）<br>// 尽快处理新到达的数据<br>.trigger(Trigger.RealTime(1000))  // 1秒检查新数据 |
| `RealTime` | batchDuration: long, timeUnit: TimeUnit | `Trigger` | RealTime实时触发器，指定时间单位设置批处理时长 | // RealTime：指定时间单位设置批处理时长<br>.trigger(Trigger.RealTime(5, TimeUnit.SECONDS)) |
| `RealTime` | batchDuration: Duration | `Trigger` | RealTime实时触发器，使用Java Duration对象设置批处理时长 | // RealTime：使用Duration设置批处理时长<br>.trigger(Trigger.RealTime(Duration.ofSeconds(10))) |
| `RealTime` | batchDuration: String | `Trigger` | RealTime实时触发器，字符串格式设置批处理时长 | // RealTime：字符串格式设置批处理时长<br>.trigger(Trigger.RealTime("10 seconds")) |
| `RealTime` | 无 | `Trigger` | RealTime实时触发器，使用默认批处理时长 | // RealTime：默认批处理时长<br>// 使用系统默认设置进行实时处理<br>.trigger(Trigger.RealTime()) |

---

## 其他辅助类


--------|------|----------|------|------|
| `getRemoteUser` | 无 | `String` | 获取RemoteUser相关功能 | 调用该方法执行获取RemoteUser相关功能 |
| `getUserPrincipal` | 无 | `Principal` | 获取UserPrincipal相关功能 | 调用该方法执行获取UserPrincipal相关功能 |
| `isUserInRole` | role: String | `boolean` | 判断是否UserInRole相关功能 | 传入参数执行判断是否UserInRole相关功能 |

--------|------|----------|------|------|
| `Authenticate` | user: String, password: String | `void` | 认证操作 | 传入参数执行认证操作 |


--------|------|----------|------|------|
| `patternToRegex` | pattern: String | `String` | patternToRegex操作 | 传入参数执行patternToRegex操作 |


--------|------|----------|------|------|
| `toCryptoConf` | prefix: String, conf: String>> | `Properties` | toCryptoConf操作 | 传入参数执行toCryptoConf操作 |


--------|------|----------|------|------|
| `Authenticate` | user: String, password: String | `void` | 认证操作 | 传入参数执行认证操作 |

--------|------|----------|------|------|
| `debug` | msg: String | `void` | 调试相关功能 | 传入参数执行调试相关功能 |
| `debug` | format: String, arg: Object | `void` | 调试相关功能 | 传入参数执行调试相关功能 |
| `debug` | format: String, arg1: Object, arg2: Object | `void` | 调试相关功能 | 传入参数执行调试相关功能 |
| `debug` | format: String, arguments: Object... | `void` | 调试相关功能 | 传入参数执行调试相关功能 |
| `debug` | msg: String, throwable: Throwable | `void` | 调试相关功能 | 传入参数执行调试相关功能 |
| `error` | msg: String | `void` | error操作 | 传入参数执行error操作 |
| `error` | msg: String, throwable: Throwable | `void` | error操作 | 传入参数执行error操作 |
| `error` | msg: String, mdcs: MDC... | `void` | error操作 | 传入参数执行error操作 |
| `error` | msg: String, throwable: Throwable, mdcs: MDC... | `void` | error操作 | 传入参数执行error操作 |
| `getSlf4jLogger` | 无 | `Logger` | 获取Slf4jLogger相关功能 | 调用该方法执行获取Slf4jLogger相关功能 |
| `info` | msg: String | `void` | info操作 | 传入参数执行info操作 |
| `info` | msg: String, throwable: Throwable | `void` | info操作 | 传入参数执行info操作 |
| `info` | msg: String, mdcs: MDC... | `void` | info操作 | 传入参数执行info操作 |
| `info` | msg: String, throwable: Throwable, mdcs: MDC... | `void` | info操作 | 传入参数执行info操作 |
| `isDebugEnabled` | 无 | `boolean` | 判断是否DebugEnabled相关功能 | 调用该方法执行判断是否DebugEnabled相关功能 |
| `isErrorEnabled` | 无 | `boolean` | 判断是否ErrorEnabled相关功能 | 调用该方法执行判断是否ErrorEnabled相关功能 |
| `isInfoEnabled` | 无 | `boolean` | 判断是否InfoEnabled相关功能 | 调用该方法执行判断是否InfoEnabled相关功能 |
| `isTraceEnabled` | 无 | `boolean` | 判断是否TraceEnabled相关功能 | 调用该方法执行判断是否TraceEnabled相关功能 |
| `isWarnEnabled` | 无 | `boolean` | 判断是否WarnEnabled相关功能 | 调用该方法执行判断是否WarnEnabled相关功能 |
| `trace` | msg: String | `void` | 追踪相关功能 | 传入参数执行追踪相关功能 |
| `trace` | format: String, arg: Object | `void` | 追踪相关功能 | 传入参数执行追踪相关功能 |
| `trace` | format: String, arg1: Object, arg2: Object | `void` | 追踪相关功能 | 传入参数执行追踪相关功能 |
| `trace` | format: String, arguments: Object... | `void` | 追踪相关功能 | 传入参数执行追踪相关功能 |
| `trace` | msg: String, throwable: Throwable | `void` | 追踪相关功能 | 传入参数执行追踪相关功能 |
| `warn` | msg: String | `void` | warn操作 | 传入参数执行warn操作 |
| `warn` | msg: String, throwable: Throwable | `void` | warn操作 | 传入参数执行warn操作 |
| `warn` | msg: String, mdcs: MDC... | `void` | warn操作 | 传入参数执行warn操作 |
| `warn` | msg: String, throwable: Throwable, mdcs: MDC... | `void` | warn操作 | 传入参数执行warn操作 |

### Encoders
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 16

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `String` | 解码相关功能 | 传入参数执行解码相关功能 |
| `decode` | buf: ByteBuf | `RoaringBitmap` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf, s: String | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encode` | buf: ByteBuf, b: RoaringBitmap | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encode` | buf: ByteBuf, arr: byte&lt;&gt; | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encode` | buf: ByteBuf, strings: String&lt;&gt; | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encode` | buf: ByteBuf, ints: int&lt;&gt; | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encode` | buf: ByteBuf, longs: long&lt;&gt; | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encode` | buf: ByteBuf, bitmaps: RoaringBitmap&lt;&gt; | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | s: String | `int` | 编码dLength相关功能 | 传入参数执行编码dLength相关功能 |
| `encodedLength` | b: RoaringBitmap | `int` | 编码dLength相关功能 | 传入参数执行编码dLength相关功能 |
| `encodedLength` | arr: byte&lt;&gt; | `int` | 编码dLength相关功能 | 传入参数执行编码dLength相关功能 |
| `encodedLength` | strings: String&lt;&gt; | `int` | 编码dLength相关功能 | 传入参数执行编码dLength相关功能 |
| `encodedLength` | ints: int&lt;&gt; | `int` | 编码dLength相关功能 | 传入参数执行编码dLength相关功能 |
| `encodedLength` | longs: long&lt;&gt; | `int` | 编码dLength相关功能 | 传入参数执行编码dLength相关功能 |
| `encodedLength` | bitmaps: RoaringBitmap&lt;&gt; | `int` | 编码dLength相关功能 | 传入参数执行编码dLength相关功能 |


--------|------|----------|------|------|
| `getFilePath` | localDirs: String&lt;&gt;, subDirsPerLocalDir: int, filename: String | `String` | 获取FilePath相关功能 | 传入参数执行获取FilePath相关功能 |


--------|------|----------|------|------|
| `getMethodInternal` | udfClass: Class<?>, mlist: List<Method>, exact: boolean, argumentsPassed: List<TypeInfo> | `Method` | 获取MethodInternal相关功能 | 传入参数执行获取MethodInternal相关功能 |
| `invoke` | m: Method, thisObject: Object, arguments: Object... | `Object` | 调用相关功能 | 传入参数执行调用相关功能 |
| `matchCost` | argumentPassed: TypeInfo, argumentAccepted: TypeInfo, exact: boolean | `int` | matchCost操作 | 传入参数执行matchCost操作 |


--------|------|----------|------|------|
| `getSessionConf` | 无 | `HiveConf` | 获取SessionConf相关功能 | 调用该方法执行获取SessionConf相关功能 |
| `getSessionHandle` | 无 | `String` | 获取SessionHandle相关功能 | 调用该方法执行获取SessionHandle相关功能 |
| `getSessionUser` | 无 | `String` | 获取SessionUser相关功能 | 调用该方法执行获取SessionUser相关功能 |

--------|------|----------|------|------|
| `cancelDelegationToken` | authFactory: HiveAuthFactory, tokenStr: String | `void` | 判断能否celDelegationToken相关功能 | 传入参数执行判断能否celDelegationToken相关功能 |
| `cancelOperation` | opHandle: OperationHandle | `void` | 判断能否celOperation相关功能 | 传入参数执行判断能否celOperation相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `closeExpiredOperations` | 无 | `void` | 关闭ExpiredOperations相关功能 | 调用该方法执行关闭ExpiredOperations相关功能 |
| `closeOperation` | opHandle: OperationHandle | `void` | 关闭Operation相关功能 | 传入参数执行关闭Operation相关功能 |
| `executeStatement` | statement: String, confOverlay: String> | `OperationHandle` | 执行Statement相关功能 | 传入参数执行执行Statement相关功能 |
| `executeStatement` | statement: String, confOverlay: String>, queryTimeout: long | `OperationHandle` | 执行Statement相关功能 | 传入参数执行执行Statement相关功能 |
| `executeStatementAsync` | statement: String, confOverlay: String> | `OperationHandle` | 执行StatementAsync相关功能 | 传入参数执行执行StatementAsync相关功能 |
| `executeStatementAsync` | statement: String, confOverlay: String>, queryTimeout: long | `OperationHandle` | 执行StatementAsync相关功能 | 传入参数执行执行StatementAsync相关功能 |
| `fetchResults` | opHandle: OperationHandle, orientation: FetchOrientation, maxRows: long, fetchType: FetchType | `TRowSet` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `getCatalogs` | 无 | `OperationHandle` | 获取Catalogs相关功能 | 调用该方法执行获取Catalogs相关功能 |
| `getColumns` | catalogName: String, schemaName: String, tableName: String, columnName: String | `OperationHandle` | 获取Columns相关功能 | 传入参数执行获取Columns相关功能 |
| `getCrossReference` | primaryCatalog: String, primarySchema: String, primaryTable: String, foreignCatalog: String, foreignSchema: String, foreignTable: String | `OperationHandle` | 获取CrossReference相关功能 | 传入参数执行获取CrossReference相关功能 |
| `getDelegationToken` | authFactory: HiveAuthFactory, owner: String, renewer: String | `String` | 获取DelegationToken相关功能 | 传入参数执行获取DelegationToken相关功能 |
| `getFunctions` | catalogName: String, schemaName: String, functionName: String | `OperationHandle` | 获取Functions相关功能 | 传入参数执行获取Functions相关功能 |
| `getHiveConf` | 无 | `HiveConf` | 获取HiveConf相关功能 | 调用该方法执行获取HiveConf相关功能 |
| `getInfo` | getInfoType: GetInfoType | `GetInfoValue` | 获取Info相关功能 | 传入参数执行获取Info相关功能 |
| `getIpAddress` | 无 | `String` | 获取IpAddress相关功能 | 调用该方法执行获取IpAddress相关功能 |
| `getLastAccessTime` | 无 | `long` | 获取LastAccessTime相关功能 | 调用该方法执行获取LastAccessTime相关功能 |
| `getMetaStoreClient` | 无 | `IMetaStoreClient` | 获取MetaStoreClient相关功能 | 调用该方法执行获取MetaStoreClient相关功能 |
| `getNoOperationTime` | 无 | `long` | 获取NoOperationTime相关功能 | 调用该方法执行获取NoOperationTime相关功能 |
| `getOperationLogSessionDir` | 无 | `File` | 获取OperationLogSessionDir相关功能 | 调用该方法执行获取OperationLogSessionDir相关功能 |
| `getPassword` | 无 | `String` | 获取Password相关功能 | 调用该方法执行获取Password相关功能 |
| `getPrimaryKeys` | catalog: String, schema: String, table: String | `OperationHandle` | 获取PrimaryKeys相关功能 | 传入参数执行获取PrimaryKeys相关功能 |
| `getProtocolVersion` | 无 | `TProtocolVersion` | 获取ProtocolVersion相关功能 | 调用该方法执行获取ProtocolVersion相关功能 |
| `getResultSetMetadata` | opHandle: OperationHandle | `TTableSchema` | 获取ResultSetMetadata相关功能 | 传入参数执行获取ResultSetMetadata相关功能 |
| `getSchemas` | catalogName: String, schemaName: String | `OperationHandle` | 获取Schemas相关功能 | 传入参数执行获取Schemas相关功能 |
| `getSessionHandle` | 无 | `SessionHandle` | 获取SessionHandle相关功能 | 调用该方法执行获取SessionHandle相关功能 |
| `getSessionManager` | 无 | `SessionManager` | 获取SessionManager相关功能 | 调用该方法执行获取SessionManager相关功能 |
| `getSessionState` | 无 | `SessionState` | 获取SessionState相关功能 | 调用该方法执行获取SessionState相关功能 |
| `getTableTypes` | 无 | `OperationHandle` | 获取TableTypes相关功能 | 调用该方法执行获取TableTypes相关功能 |
| `getTables` | catalogName: String, schemaName: String, tableName: String, tableTypes: List<String> | `OperationHandle` | 获取Tables相关功能 | 传入参数执行获取Tables相关功能 |
| `getTypeInfo` | 无 | `OperationHandle` | 获取TypeInfo相关功能 | 调用该方法执行获取TypeInfo相关功能 |
| `getUserName` | 无 | `String` | 获取UserName相关功能 | 调用该方法执行获取UserName相关功能 |
| `getUsername` | 无 | `String` | 获取Username相关功能 | 调用该方法执行获取Username相关功能 |
| `isOperationLogEnabled` | 无 | `boolean` | 判断是否OperationLogEnabled相关功能 | 调用该方法执行判断是否OperationLogEnabled相关功能 |
| `open` | sessionConfMap: String> | `void` | 打开相关功能 | 传入参数执行打开相关功能 |
| `renewDelegationToken` | authFactory: HiveAuthFactory, tokenStr: String | `void` | renewDelegationToken操作 | 传入参数执行renewDelegationToken操作 |
| `setIpAddress` | ipAddress: String | `void` | 设置IpAddress相关功能 | 传入参数执行设置IpAddress相关功能 |
| `setOperationLogSessionDir` | operationLogRootDir: File | `void` | 设置OperationLogSessionDir相关功能 | 传入参数执行设置OperationLogSessionDir相关功能 |
| `setOperationManager` | operationManager: OperationManager | `void` | 设置OperationManager相关功能 | 传入参数执行设置OperationManager相关功能 |
| `setSessionManager` | sessionManager: SessionManager | `void` | 设置SessionManager相关功能 | 传入参数执行设置SessionManager相关功能 |
| `setUserName` | userName: String | `void` | 设置UserName相关功能 | 传入参数执行设置UserName相关功能 |
| `setVariable` | varname: String, varvalue: String | `int` | 设置Variable相关功能 | 传入参数执行设置Variable相关功能 |


--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `getMovieId` | 无 | `int` | 获取MovieId相关功能 | 调用该方法执行获取MovieId相关功能 |
| `getRating` | 无 | `float` | 获取Rating相关功能 | 调用该方法执行获取Rating相关功能 |
| `getTimestamp` | 无 | `long` | 获取Timestamp相关功能 | 调用该方法执行获取Timestamp相关功能 |
| `getUserId` | 无 | `int` | 获取UserId相关功能 | 调用该方法执行获取UserId相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `parseRating` | str: String | `Rating` | 解析Rating相关功能 | 传入参数执行解析Rating相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `getId` | 无 | `long` | 获取Id相关功能 | 调用该方法执行获取Id相关功能 |
| `getText` | 无 | `String` | 获取Text相关功能 | 调用该方法执行获取Text相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `getLabel` | 无 | `double` | 获取Label相关功能 | 调用该方法执行获取Label相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |


--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `byteStringAs` | str: String, unit: ByteUnit | `long` | 三相关功能 | 传入参数执行三相关功能 |
| `byteStringAsBytes` | str: String | `long` | 三相关功能 | 传入参数执行三相关功能 |
| `byteStringAsGb` | str: String | `long` | 三相关功能 | 传入参数执行三相关功能 |
| `byteStringAsKb` | str: String | `long` | 三相关功能 | 传入参数执行三相关功能 |
| `byteStringAsMb` | str: String | `long` | 三相关功能 | 传入参数执行三相关功能 |
| `bytesToString` | b: ByteBuffer | `String` | 三相关功能 | 传入参数执行三相关功能 |
| `checkArgument` | check: boolean, msg: String, args: Object... | `void` | 检查Argument相关功能 | 传入参数执行检查Argument相关功能 |
| `checkState` | check: boolean, msg: String, args: Object... | `void` | 检查State相关功能 | 传入参数执行检查State相关功能 |
| `checkedCast` | value: long | `int` | 检查edCast相关功能 | 传入参数执行检查edCast相关功能 |
| `cleanDirectory` | dir: File | `void` | cleanDirectory操作 | 传入参数执行cleanDirectory操作 |
| `closeQuietly` | closeable: Closeable | `void` | 关闭Quietly相关功能 | 传入参数执行关闭Quietly相关功能 |
| `contentEquals` | file1: File, file2: File | `boolean` | 判断相等相关功能 | 传入参数执行判断相等相关功能 |
| `copyDirectory` | src: File, dst: File | `void` | 复制Directory相关功能 | 传入参数执行复制Directory相关功能 |
| `copyURLToFile` | url: URL, file: File | `void` | 复制URLToFile相关功能 | 传入参数执行复制URLToFile相关功能 |
| `createDirectory` | root: String | `File` | 创建Directory相关功能 | 传入参数执行创建Directory相关功能 |
| `createDirectory` | root: String, namePrefix: String | `File` | 创建Directory相关功能 | 传入参数执行创建Directory相关功能 |
| `deleteQuietly` | file: File | `void` | 删除请求Quietly相关功能 | 传入参数执行删除请求Quietly相关功能 |
| `deleteRecursively` | file: File | `void` | 删除请求Recursively相关功能 | 传入参数执行删除请求Recursively相关功能 |
| `deleteRecursively` | file: File, filter: FilenameFilter | `void` | 删除请求Recursively相关功能 | 传入参数执行删除请求Recursively相关功能 |
| `digestToHexString` | algorithm: String, input: byte&lt;&gt; | `String` | 摘要ToHexString相关功能 | 传入参数执行摘要ToHexString相关功能 |
| `digestToHexString` | algorithm: String, input: String | `String` | 摘要ToHexString相关功能 | 传入参数执行摘要ToHexString相关功能 |
| `forceDeleteOnExit` | file: File | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `isTesting` | 无 | `boolean` | 判断是否Testing相关功能 | 调用该方法执行判断是否Testing相关功能 |
| `join` | arr: List<Object>, sep: String | `String` | 连接DataFrame | // join：内连接<br>List<Tuple2<String, Integer>> orders = Arrays.asList(<br>    new Tuple2<>("user1", 100),<br>    new Tuple2<>("user2", 200)<br>);<br>List<Tuple2<String, String>> users = Arrays.asList(<br>    new Tuple2<>("user1", "Alice"),<br>    new Tuple2<>("user2", "Bob")<br>);<br>JavaPairRDD<String, Integer> orderRDD = sc.parallelizePairs(orders);<br>JavaPairRDD<String, String> userRDD = sc.parallelizePairs(users);<br>// 内连接<br>JavaPairRDD<String, Tuple2<Integer, String>> joined = orderRDD.join(userRDD);<br>// 结果: [("user1", (100, "Alice")), ("user2", (200, "Bob"))] |
| `listFiles` | dir: File | `Set&lt;File&gt;` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `listPaths` | dir: File | `Set&lt;Path&gt;` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `md5Hex` | input: byte&lt;&gt; | `String` | md5Hex操作 | 传入参数执行md5Hex操作 |
| `md5Hex` | input: String | `String` | md5Hex操作 | 传入参数执行md5Hex操作 |
| `moveDirectory` | src: File, dst: File | `void` | 移动Directory相关功能 | 传入参数执行移动Directory相关功能 |
| `moveFile` | src: File, dst: File | `void` | 移动File相关功能 | 传入参数执行移动File相关功能 |
| `nonNegativeHash` | obj: Object | `int` | 检查是否存在相关功能 | 传入参数执行检查是否存在相关功能 |
| `postVisitDirectory` | dir: Path, e: IOException | `FileVisitResult` | 后VisitDirectory相关功能 | 传入参数执行后VisitDirectory相关功能 |
| `preVisitDirectory` | p: Path, a: BasicFileAttributes | `FileVisitResult` | 上一个isitDirectory相关功能 | 传入参数执行上一个isitDirectory相关功能 |
| `preVisitDirectory` | dir: Path, attrs: BasicFileAttributes | `FileVisitResult` | 上一个isitDirectory相关功能 | 传入参数执行上一个isitDirectory相关功能 |
| `readFully` | channel: ReadableByteChannel, dst: ByteBuffer | `void` | 读取Fully相关功能 | 传入参数执行读取Fully相关功能 |
| `readFully` | in: InputStream, arr: byte&lt;&gt;, off: int, len: int | `void` | 读取Fully相关功能 | 传入参数执行读取Fully相关功能 |
| `sha256Hex` | input: byte&lt;&gt; | `String` | sha256Hex操作 | 传入参数执行sha256Hex操作 |
| `sha256Hex` | input: String | `String` | sha256Hex操作 | 传入参数执行sha256Hex操作 |
| `sizeOf` | file: File | `long` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `sizeOf` | dirPath: Path | `long` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `stackTraceToString` | t: Throwable | `String` | 追踪相关功能 | 传入参数执行追踪相关功能 |
| `stringToBytes` | s: String | `ByteBuffer` | 三相关功能 | 传入参数执行三相关功能 |
| `timeStringAs` | str: String, unit: TimeUnit | `long` | 时间StringAs相关功能 | 传入参数执行时间StringAs相关功能 |
| `timeStringAsMs` | str: String | `long` | 时间StringAsMs相关功能 | 传入参数执行时间StringAsMs相关功能 |
| `timeStringAsSec` | str: String | `long` | 时间StringAsSec相关功能 | 传入参数执行时间StringAsSec相关功能 |
| `visitFile` | p: Path, a: BasicFileAttributes | `FileVisitResult` | 访问File相关功能 | 传入参数执行访问File相关功能 |
| `visitFile` | file: Path, attrs: BasicFileAttributes | `FileVisitResult` | 访问File相关功能 | 传入参数执行访问File相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |


--------|------|----------|------|------|
| `Authenticate` | user: String, password: String | `void` | 认证操作 | 传入参数执行认证操作 |


--------|------|----------|------|------|
| `createEventLoop` | mode: IOMode, numThreads: int, threadPrefix: String | `EventLoopGroup` | 创建EventLoop相关功能 | 传入参数执行创建EventLoop相关功能 |
| `createFrameDecoder` | 无 | `TransportFrameDecoder` | 创建FrameDecoder相关功能 | 调用该方法执行创建FrameDecoder相关功能 |
| `createPooledByteBufAllocator` | allowDirectBufs: boolean, allowCache: boolean, numCores: int | `PooledByteBufAllocator` | 创建PooledByteBufAllocator相关功能 | 传入参数执行创建PooledByteBufAllocator相关功能 |
| `createThreadFactory` | threadPoolPrefix: String | `ThreadFactory` | 创建ThreadFactory相关功能 | 传入参数执行创建ThreadFactory相关功能 |
| `defaultNumThreads` | numUsableCores: int | `int` | 默认NumThreads相关功能 | 传入参数执行默认NumThreads相关功能 |
| `freeDirectMemory` | 无 | `long` | freeDirectMemory操作 | 调用该方法执行freeDirectMemory操作 |
| `getClientChannelClass` | mode: IOMode | `Class&lt;? extends Channel&gt;` | 获取ClientChannelClass相关功能 | 传入参数执行获取ClientChannelClass相关功能 |
| `getRemoteAddress` | channel: Channel | `String` | 获取RemoteAddress相关功能 | 传入参数执行获取RemoteAddress相关功能 |
| `getServerChannelClass` | mode: IOMode | `Class&lt;? extends ServerChannel&gt;` | 获取ServerChannelClass相关功能 | 传入参数执行获取ServerChannelClass相关功能 |
| `preferDirectBufs` | conf: TransportConf | `boolean` | 前ferDirectBufs相关功能 | 传入参数执行前ferDirectBufs相关功能 |


--------|------|----------|------|------|
| `Authenticate` | user: String, password: String | `void` | 认证操作 | 传入参数执行认证操作 |


--------|------|----------|------|------|
| `html` | 无 | `HTML&lt;ProxyUtils.__&gt;` | html操作 | 调用该方法执行html操作 |
| `notFound` | resp: HttpServletResponse, message: String | `void` | notFound操作 | 传入参数执行notFound操作 |
| `rejectNonHttpRequests` | req: ServletRequest | `void` | 拒绝NonHttpRequests相关功能 | 传入参数执行拒绝NonHttpRequests相关功能 |
| `sendRedirect` | request: HttpServletRequest, response: HttpServletResponse, target: String | `void` | 发送Redirect相关功能 | 传入参数执行发送Redirect相关功能 |


--------|------|----------|------|------|
| `cleanup` | log: SparkLogger, closeables: java.io.Closeable... | `void` | 向上相关功能 | 传入参数执行向上相关功能 |
| `indexOfDomainMatch` | userName: String | `int` | 执行相关功能 | 传入参数执行执行相关功能 |


--------|------|----------|------|------|
| `rebuild` | row: ShreddedRow, schema: VariantSchema | `Variant` | 构建相关功能 | 传入参数执行构建相关功能 |
| `rebuild` | row: ShreddedRow, metadata: byte&lt;&gt;, schema: VariantSchema, builder: VariantBuilder | `void` | 构建相关功能 | 传入参数执行构建相关功能 |

--------|------|----------|------|------|
| `diagnoseCorruption` | algorithm: String, checksumFile: File, reduceId: int, partitionData: ManagedBuffer, checksumByReader: long | `Cause` | 向上相关功能 | 传入参数执行向上相关功能 |
| `getChecksumByAlgorithm` | algorithm: String | `Checksum` | 获取ChecksumByAlgorithm相关功能 | 传入参数执行获取ChecksumByAlgorithm相关功能 |
| `getChecksumFileName` | blockName: String, algorithm: String | `String` | 获取ChecksumFileName相关功能 | 传入参数执行获取ChecksumFileName相关功能 |


--------|------|----------|------|------|
| `getEvaluatorClass` | argClasses: List<TypeInfo> | `Class&lt;? extends UDAFEvaluator&gt;` | 获取EvaluatorClass相关功能 | 传入参数执行获取EvaluatorClass相关功能 |

--------|------|----------|------|------|
| `getEvalMethod` | argClasses: List<TypeInfo> | `Method` | 获取EvalMethod相关功能 | 传入参数执行获取EvalMethod相关功能 |

### SparkLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 26

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addAppArgs` | args: String... | `SparkLauncher` | 添加应用参数 | 传入参数执行添加应用参数 |
| `addFile` | file: String | `SparkLauncher` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业<br>sc.addFile("hdfs://path/to/config.txt");<br>sc.addFile("s3://bucket/data.json");<br>// 在Executor中访问文件<br>String filePath = SparkFiles.get("config.txt"); |
| `addJar` | jar: String | `SparkLauncher` | 添加JAR包到Spark作业 | // 添加依赖JAR包<br>sc.addJar("hdfs://path/to/dependency.jar");<br>sc.addJar("/local/path/to/lib.jar"); |
| `addPyFile` | file: String | `SparkLauncher` | 添加Python文件 | 传入参数执行添加Python文件 |
| `addSparkArg` | arg: String | `SparkLauncher` | 添加Spark参数 | 传入参数执行添加Spark参数 |
| `addSparkArg` | name: String, value: String | `SparkLauncher` | 添加Spark参数 | 传入参数执行添加Spark参数 |
| `directory` | dir: File | `SparkLauncher` | directory操作 | 传入参数执行directory操作 |
| `launch` | 无 | `Process` | launch操作 | 调用该方法执行launch操作 |
| `redirectError` | 无 | `SparkLauncher` | 重定向Error相关功能 | 调用该方法执行重定向Error相关功能 |
| `redirectError` | to: ProcessBuilder.Redirect | `SparkLauncher` | 重定向Error相关功能 | 传入参数执行重定向Error相关功能 |
| `redirectError` | errFile: File | `SparkLauncher` | 重定向Error相关功能 | 传入参数执行重定向Error相关功能 |
| `redirectOutput` | to: ProcessBuilder.Redirect | `SparkLauncher` | 重定向Output相关功能 | 传入参数执行重定向Output相关功能 |
| `redirectOutput` | outFile: File | `SparkLauncher` | 重定向Output相关功能 | 传入参数执行重定向Output相关功能 |
| `redirectToLog` | loggerName: String | `SparkLauncher` | 重定向ToLog相关功能 | 传入参数执行重定向ToLog相关功能 |
| `setAppName` | appName: String | `SparkLauncher` | 设置AppName相关功能 | 传入参数执行设置AppName相关功能 |
| `setAppResource` | resource: String | `SparkLauncher` | 设置AppResource相关功能 | 传入参数执行设置AppResource相关功能 |
| `setConf` | key: String, value: String | `SparkLauncher` | 设置Conf相关功能 | 传入参数执行设置Conf相关功能 |
| `setConfig` | name: String, value: String | `void` | 设置Config相关功能 | 传入参数执行设置Config相关功能 |
| `setDeployMode` | mode: String | `SparkLauncher` | 设置DeployMode相关功能 | 传入参数执行设置DeployMode相关功能 |
| `setJavaHome` | javaHome: String | `SparkLauncher` | 设置JavaHome相关功能 | 传入参数执行设置JavaHome相关功能 |
| `setMainClass` | mainClass: String | `SparkLauncher` | 设置MainClass相关功能 | 传入参数执行设置MainClass相关功能 |
| `setMaster` | master: String | `SparkLauncher` | 设置Master相关功能 | 传入参数执行设置Master相关功能 |
| `setPropertiesFile` | path: String | `SparkLauncher` | 设置PropertiesFile相关功能 | 传入参数执行设置PropertiesFile相关功能 |
| `setSparkHome` | sparkHome: String | `SparkLauncher` | 设置SparkHome相关功能 | 传入参数执行设置SparkHome相关功能 |
| `setVerbose` | verbose: boolean | `SparkLauncher` | 设置Verbose相关功能 | 传入参数执行设置Verbose相关功能 |
| `startApplication` | listeners: SparkAppHandle.Listener... | `SparkAppHandle` | 启动Application相关功能 | 传入参数执行启动Application相关功能 |

--------|------|----------|------|------|
| `disableStructuredLogging` | 无 | `void` | 禁用StructuredLogging相关功能 | 调用该方法执行禁用StructuredLogging相关功能 |
| `enableStructuredLogging` | 无 | `void` | 启用StructuredLogging相关功能 | 调用该方法执行启用StructuredLogging相关功能 |
| `getLogger` | name: String | `SparkLogger` | 获取Logger相关功能 | 传入参数执行获取Logger相关功能 |
| `getLogger` | clazz: Class<?> | `SparkLogger` | 获取Logger相关功能 | 传入参数执行获取Logger相关功能 |
| `isStructuredLoggingEnabled` | 无 | `boolean` | 判断是否StructuredLoggingEnabled相关功能 | 调用该方法执行判断是否StructuredLoggingEnabled相关功能 |


---

## 存储级别


---

---

## SparkSession（现代Spark入口）

### SparkSession
**包路径**: `org.apache.spark.sql`
**说明**: Spark 2.0+的主入口点，替代了旧版的SQLContext和HiveContext。提供DataFrame/Dataset创建、SQL执行等功能。
**方法数量**: 30

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `builder` | 无 | `SparkSession.Builder` | 获取SparkSession构建器 | `SparkSession spark = SparkSession.builder()<br>    .appName("MyApp")<br>    .master("local[*]")<br>    .getOrCreate();` |
| `appName` | String name | `Builder` | 设置应用名称 | `SparkSession.builder().appName("DataProcessing").getOrCreate();` |
| `master` | String master | `Builder` | 设置运行模式（local/yarn等） | `SparkSession.builder().master("yarn").getOrCreate();` |
| `config` | String key, String value | `Builder` | 设置配置项 | `SparkSession.builder()<br>    .config("spark.sql.shuffle.partitions", "200")<br>    .getOrCreate();` |
| `enableHiveSupport` | 无 | `Builder` | 启用Hive支持 | `SparkSession.builder().enableHiveSupport().getOrCreate();` |
| `getOrCreate` | 无 | `SparkSession` | 获取或创建SparkSession | `SparkSession spark = SparkSession.builder().getOrCreate();` |
| `version` | 无 | `String` | 获取Spark版本 | `String version = spark.version();<br>// 返回如 "3.5.6"` |
| `sparkContext` | 无 | `SparkContext` | 获取底层SparkContext | `SparkContext sc = spark.sparkContext();` |
| `sql` | String sqlText | `DataFrame` | 执行SQL查询 | `DataFrame result = spark.sql("SELECT * FROM table WHERE id > 100");` |
| `sql` | String sqlText, Map[String, Any] args | `DataFrame` | 执行带参数的SQL查询 | `Map<String, Any> args = new HashMap<>();<br>args.put("id", 100);<br>DataFrame result = spark.sql("SELECT * FROM table WHERE id > :id", args);` |
| `table` | String tableName | `DataFrame` | 从表名创建DataFrame | `DataFrame df = spark.table("my_table");` |
| `read` | 无 | `DataFrameReader` | 获取数据读取器 | `DataFrameReader reader = spark.read();<br>DataFrame df = reader.parquet("data.parquet");` |
| `readStream` | 无 | `DataStreamReader` | 获取流数据读取器 | `DataStreamReader reader = spark.readStream();` |
| `createDataFrame` | List[Row] rows, StructType schema | `DataFrame` | 从Java List创建DataFrame | `StructType schema = new StructType()<br>    .add("id", DataTypes.IntegerType)<br>    .add("name", DataTypes.StringType);<br>List<Row> rows = Arrays.asList(<br>    RowFactory.create(1, "Alice"),<br>    RowFactory.create(2, "Bob"));<br>DataFrame df = spark.createDataFrame(rows, schema);` |
| `createDataFrame` | JavaRDD[Row] rdd, StructType schema | `DataFrame` | 从JavaRDD创建DataFrame | `JavaRDD<Row> rowRDD = sc.parallelize(Arrays.asList(<br>    RowFactory.create(1, "Alice")));<br>DataFrame df = spark.createDataFrame(rowRDD, schema);` |
| `createDataset` | List[T] data, Encoder[T] encoder | `Dataset[T]` | 从Java List创建Dataset | `Encoder<Integer> encoder = Encoders.INT();<br>List<Integer> data = Arrays.asList(1, 2, 3);<br>Dataset<Integer> ds = spark.createDataset(data, encoder);` |
| `emptyDataFrame` | 无 | `DataFrame` | 创建空DataFrame | `DataFrame empty = spark.emptyDataFrame();` |
| `range` | long end | `Dataset[Long]` | 创建范围数据（0到end-1） | `Dataset<Long> range = spark.range(100);<br>// 生成0到99的序列` |
| `range` | long start, long end, long step, int numPartitions | `Dataset[Long]` | 创建范围数据，指定参数 | `Dataset<Long> range = spark.range(0, 100, 2, 10);<br>// 0, 2, 4, ... 98，10个分区` |
| `udf` | 无 | `UDFRegistration` | 获取UDF注册器 | `spark.udf().register("myFunc", (String s) -> s.toUpperCase(), DataTypes.StringType);` |
| `catalog` | 无 | `Catalog` | 获取Catalog接口 | `Catalog catalog = spark.catalog();<br>catalog.listTables().show();` |
| `conf` | 无 | `RuntimeConfig` | 获取运行时配置 | `RuntimeConfig conf = spark.conf();<br>conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB");` |
| `newSession` | 无 | `SparkSession` | 创建新Session（隔离配置） | `SparkSession newSpark = spark.newSession();` |
| `stop` | 无 | `Unit` | 停止SparkSession | `spark.stop();` |
| `close` | 无 | `Unit` | 关闭SparkSession（Java友好） | `spark.close();` |
| `time` | T => T f | `T` | 测量函数执行时间 | `long result = spark.time(() -> {<br>    return df.count();<br>});<br>// 打印执行时间并返回结果` |
| `addTag` | String tag | `Unit` | 为操作添加标签 | `spark.addTag("batch-job");` |
| `removeTag` | String tag | `Unit` | 移除标签 | `spark.removeTag("batch-job");` |
| `getTags` | 无 | `Set[String]` | 获取所有标签 | `Set<String> tags = spark.getTags();` |
| `clearTags` | 无 | `Unit` | 清除所有标签 | `spark.clearTags();` |
| `interruptTag` | String tag | `Seq[String]` | 中断指定标签的操作 | `spark.interruptTag("batch-job");` |
| `interruptAll` | 无 | `Seq[String]` | 中断所有操作 | `spark.interruptAll();` |

### RuntimeConfig
**包路径**: `org.apache.spark.sql`
**说明**: Spark运行时配置，从SparkSession.conf()获取。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | String key | `String` | 获取配置值 | `String value = spark.conf().get("spark.sql.shuffle.partitions");` |
| `get` | String key, String default | `String` | 获取配置值（带默认值） | `String value = spark.conf().get("spark.sql.autoBroadcastJoinThreshold", "10MB");` |
| `getAll` | 无 | `Map[String, String]` | 获取所有配置 | `Map<String, String> all = spark.conf().getAll();` |
| `set` | String key, String value | `RuntimeConfig` | 设置配置值 | `spark.conf().set("spark.sql.shuffle.partitions", "200");` |
| `unset` | String key | `RuntimeConfig` | 取消设置 | `spark.conf().unset("spark.sql.shuffle.partitions");` |
| `isModifiable` | String key | `boolean` | 是否可修改 | `boolean modifiable = spark.conf().isModifiable("spark.sql.shuffle.partitions");` |

---

### UDF0[R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 无参数用户自定义函数接口。
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | 无 | `R` | 调用函数 | `public String call() { return "constant"; }` |

---

### UDF1[T, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 单参数用户自定义函数接口。
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T t | `R` | 调用函数 | `public Integer call(String s) { return s.length(); }` |

---

### UDF2[T1, T2, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 双参数用户自定义函数接口。
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2 | `R` | 调用函数 | `public String call(String a, String b) { return a + b; }` |

---

### UDF3[T1, T2, T3, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 三参数用户自定义函数接口。
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2, T3 t3 | `R` | 调用函数 | `public Double call(Double a, Double b, Double c) { return a + b + c; }` |

---

### UDF4[T1, T2, T3, T4, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 四参数用户自定义函数接口。
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2, T3 t3, T4 t4 | `R` | 调用函数 | `public R call(T1 t1, T2 t2, T3 t3, T4 t4) { return func(t1, t2, t3, t4); }` |

---

### UDF5[T1, T2, T3, T4, T5, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 五参数用户自定义函数接口（最大支持）。
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 | `R` | 调用函数 | `public R call(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) { return func(t1, t2, t3, t4, t5); }` |

---

### UDAF1[I, O]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 单输入用户自定义聚合函数接口（需要继承Aggregator）。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `zero` | 无 | `O` | 初始化聚合缓冲区 | `public Buffer zero() { return new Buffer(0, 0); }` |
| `reduce` | Buffer b, I i | `Buffer` | 减少输入到缓冲区 | `public Buffer reduce(Buffer b, Integer i) { b.sum += i; b.count++; return b; }` |
| `merge` | Buffer b1, Buffer b2 | `Buffer` | 合并两个缓冲区 | `public Buffer merge(Buffer b1, Buffer b2) { b1.sum += b2.sum; b1.count += b2.count; return b1; }` |
| `finish` | Buffer b | `Double` | 输出最终结果 | `public Double finish(Buffer b) { return b.sum / b.count; }` |

---

### Dataset[T]（类型安全数据集）
**包路径**: `org.apache.spark.sql`
**说明**: Spark 2.0+的核心数据处理API，提供类型安全的数据操作。DataFrame是Dataset[Row]的特例。
**方法数量**: 80

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `show` | 无 | `Unit` | 显示前20行数据 | `df.show();` |
| `show` | int numRows | `Unit` | 显示指定行数 | `df.show(50);` |
| `show` | int numRows, boolean truncate | `Unit` | 显示指定行数，控制截断 | `df.show(50, false);  // 不截断长字符串` |
| `printSchema` | 无 | `Unit` | 打印schema结构 | `df.printSchema();` |
| `schema` | 无 | `StructType` | 获取schema | `StructType schema = df.schema();` |
| `columns` | 无 | `String[]` | 获取列名数组 | `String[] cols = df.columns();` |
| `dtypes` | 无 | `Tuple2[]` | 获取列名和类型数组 | `Tuple2<String, String>[] types = df.dtypes();` |
| `select` | String col, String... cols | `DataFrame` | 选择指定列 | `DataFrame result = df.select("id", "name");` |
| `select` | Column... cols | `DataFrame` | 选择列（使用Column表达式） | `DataFrame result = df.select(col("id"), col("name").alias("user_name"));` |
| `selectExpr` | String... exprs | `DataFrame` | 使用SQL表达式选择 | `DataFrame result = df.selectExpr("id", "name as user_name", "age * 2 as double_age");` |
| `filter` | Column condition | `Dataset[T]` | 过滤数据 | `DataFrame result = df.filter(col("age").gt(18));` |
| `filter` | String conditionExpr | `Dataset[T]` | 使用SQL表达式过滤 | `DataFrame result = df.filter("age > 18");` |
| `filter` | FilterFunction[T] func | `Dataset[T]` | 使用函数过滤（Java） | `Dataset<Integer> filtered = ds.filter((FilterFunction<Integer>) x -> x > 10);` |
| `where` | Column condition | `Dataset[T]` | 过滤（同filter） | `DataFrame result = df.where(col("status").equalTo("active"));` |
| `groupBy` | String col1, String... cols | `RelationalGroupedDataset` | 按列分组 | `RelationalGroupedDataset grouped = df.groupBy("category");<br>DataFrame result = grouped.count();` |
| `groupBy` | Column... cols | `RelationalGroupedDataset` | 按Column分组 | `RelationalGroupedDataset grouped = df.groupBy(col("category"), col("region"));` |
| `agg` | Column expr, Column... exprs | `DataFrame` | 聚合计算 | `DataFrame result = df.agg(count("id").alias("total"), avg("price").alias("avg_price"));` |
| `agg` | Map[String, String] exprs | `DataFrame` | 聚合（使用字符串表达式） | `Map<String, String> exprs = new HashMap<>();<br>exprs.put("id", "count");<br>exprs.put("price", "avg");<br>DataFrame result = df.agg(exprs);` |
| `count` | 无 | `long` | 计数 | `long total = df.count();` |
| `collect` | 无 | `T[]` | 收集所有数据到Driver | `Row[] rows = df.collect();` |
| `collectAsList` | 无 | `List[T]` | 收集为Java List | `List<Row> rows = df.collectAsList();` |
| `take` | int n | `T[]` | 获取前n行 | `Row[] first10 = df.take(10);` |
| `takeAsList` | int n | `List[T]` | 获取前n行为List | `List<Row> first10 = df.takeAsList(10);` |
| `first` | 无 | `T` | 获取第一行 | `Row firstRow = df.first();` |
| `head` | 无 | `T` | 获取第一行（同first） | `Row headRow = df.head();` |
| `head` | int n | `T[]` | 获取前n行（同take） | `Row[] top5 = df.head(5);` |
| `limit` | int n | `Dataset[T]` | 限制结果行数 | `DataFrame limited = df.limit(100);` |
| `offset` | int n | `Dataset[T]` | 跳过前n行 | `DataFrame skipped = df.offset(10);` |
| `distinct` | 无 | `Dataset[T]` | 去重 | `DataFrame unique = df.distinct();` |
| `dropDuplicates` | 无 | `Dataset[T]` | 去重（同distinct） | `DataFrame unique = df.dropDuplicates();` |
| `dropDuplicates` | String... colNames | `Dataset[T]` | 按指定列去重 | `DataFrame unique = df.dropDuplicates("id", "name");` |
| `orderBy` | String sortCol, String... sortCols | `Dataset[T]` | 排序 | `DataFrame sorted = df.orderBy("id");` |
| `orderBy` | Column... sortExprs | `Dataset[T]` | 排序（使用Column） | `DataFrame sorted = df.orderBy(col("id").desc(), col("name").asc());` |
| `sort` | String sortCol, String... sortCols | `Dataset[T]` | 排序（同orderBy） | `DataFrame sorted = df.sort("age");` |
| `sort` | Column... sortExprs | `Dataset[T]` | 排序（同orderBy） | `DataFrame sorted = df.sort(col("age").desc());` |
| `sortWithinPartitions` | String sortCol, String... sortCols | `Dataset[T]` | 分区内排序 | `DataFrame sorted = df.sortWithinPartitions("id");` |
| `union` | Dataset[T] other | `Dataset[T]` | 合合（保留重复） | `DataFrame merged = df1.union(df2);` |
| `unionByName` | Dataset[T] other | `Dataset[T]` | 按列名合并 | `DataFrame merged = df1.unionByName(df2);` |
| `unionByName` | Dataset[T] other, boolean allowMissingColumns | `Dataset[T]` | 按列名合并，允许缺失列 | `DataFrame merged = df1.unionByName(df2, true);` |
| `intersect` | Dataset[T] other | `Dataset[T]` | 取交集 | `DataFrame common = df1.intersect(df2);` |
| `intersectAll` | Dataset[T] other | `Dataset[T]` | 取交集（保留重复） | `DataFrame common = df1.intersectAll(df2);` |
| `except` | Dataset[T] other | `Dataset[T]` | 取差集 | `DataFrame diff = df1.except(df2);` |
| `exceptAll` | Dataset[T] other | `Dataset[T]` | 取差集（保留重复） | `DataFrame diff = df1.exceptAll(df2);` |
| `join` | Dataset[_] right | `DataFrame` | 笛卡尔连接 | `DataFrame result = df1.join(df2);` |
| `join` | Dataset[_] right, String usingColumn | `DataFrame` | 使用列名连接 | `DataFrame result = df1.join(df2, "id");` |
| `join` | Dataset[_] right, String[] usingColumns | `DataFrame` | 使用多列连接 | `DataFrame result = df1.join(df2, new String[]{"id", "name"});` |
| `join` | Dataset[_] right, String usingColumn, String joinType | `DataFrame` | 使用列名连接，指定类型 | `DataFrame result = df1.join(df2, "id", "left");<br>// joinType: inner, left, right, full, semi, anti` |
| `join` | Dataset[_] right, Column joinExprs | `DataFrame` | 使用条件连接 | `DataFrame result = df1.join(df2, col("df1.id").equalTo(col("df2.user_id")));` |
| `join` | Dataset[_] right, Column joinExprs, String joinType | `DataFrame` | 使用条件连接，指定类型 | `DataFrame result = df1.join(df2, col("id").equalTo(col("user_id")), "left");` |
| `crossJoin` | Dataset[_] right | `DataFrame` | 显式笛卡尔连接 | `DataFrame result = df1.crossJoin(df2);` |
| `joinWith` | Dataset[U] other, Column condition, String joinType | `Dataset[Tuple2[T, U]]` | 类型安全连接 | `Dataset<Tuple2<Row, Row>> result = ds1.joinWith(ds2, col("id").equalTo(col("user_id")), "inner");` |
| `leftOuterJoin` | JavaPairRDD[K, W] other | `JavaPairRDD[K, (V, Optional[W])]` | 左外连接（PairRDD） | `JavaPairRDD<String, Tuple2<Integer, Optional<String>>> result = pairRDD.leftOuterJoin(otherRDD);` |
| `rightOuterJoin` | JavaPairRDD[K, W] other | `JavaPairRDD[K, (Optional[V], W)]` | 右外连接（PairRDD） | `JavaPairRDD<String, Tuple2<Optional<Integer>, String>> result = pairRDD.rightOuterJoin(otherRDD);` |
| `fullOuterJoin` | JavaPairRDD[K, W] other | `JavaPairRDD[K, (Optional[V], Optional[W])]` | 全外连接（PairRDD） | `JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> result = pairRDD.fullOuterJoin(otherRDD);` |
| `map` | MapFunction[T, U] func, Encoder[U] encoder | `Dataset[U]` | 映射转换（Java） | `Dataset<String> names = ds.map((MapFunction<Integer, String>) x -> "id:" + x, Encoders.STRING());` |
| `flatMap` | FlatMapFunction[T, U] func, Encoder[U] encoder | `Dataset[U]` | 扁平映射（Java） | `Dataset<String> words = ds.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());` |
| `mapPartitions` | MapPartitionsFunction[T, U] f, Encoder[U] encoder | `Dataset[U]` | 分区映射（Java） | `Dataset<Integer> partitionSums = ds.mapPartitions((MapPartitionsFunction<Integer, Integer>) iter -> {<br>    int sum = 0;<br>    while (iter.hasNext()) sum += iter.next();<br>    return Arrays.asList(sum).iterator();<br>}, Encoders.INT());` |
| `foreach` | ForeachFunction[T] func | `Unit` | 对每行执行操作（Java） | `df.foreach((ForeachFunction<Row>) row -> System.out.println(row));` |
| `foreachPartition` | ForeachPartitionFunction[T] func | `Unit` | 对每个分区执行操作（Java） | `df.foreachPartition((ForeachPartitionFunction<Row>) iter -> {<br>    while (iter.hasNext()) {<br>        Row row = iter.next();<br>        // 处理每行<br>    }<br>});` |
| `reduce` | ReduceFunction[T] func | `T` | 聚合（Java） | `Integer sum = ds.reduce((ReduceFunction<Integer>) (a, b) -> a + b);` |
| `groupByKey` | MapFunction[T, K] func, Encoder[K] encoder | `KeyValueGroupedDataset[K, T]` | 按键分组 | `KeyValueGroupedDataset<String, Integer> grouped = ds.groupByKey((MapFunction<Integer, String>) x -> "group_" + x % 3, Encoders.STRING());` |
| `withColumn` | String colName, Column col | `DataFrame` | 添加新列 | `DataFrame result = df.withColumn("double_age", col("age").multiply(2));` |
| `withColumnRenamed` | String existingName, String newName | `DataFrame` | 重命名列 | `DataFrame result = df.withColumnRenamed("old_name", "new_name");` |
| `withColumns` | Map[String, Column] colsMap | `DataFrame` | 批量添加列 | `Map<String, Column> cols = new HashMap<>();<br>cols.put("col1", col("a").plus(col("b")));<br>DataFrame result = df.withColumns(cols);` |
| `drop` | String colName | `DataFrame` | 删除列 | `DataFrame result = df.drop("unwanted_column");` |
| `drop` | String... colNames | `DataFrame` | 删除多列 | `DataFrame result = df.drop("col1", "col2");` |
| `drop` | Column col | `DataFrame` | 删除列（使用Column） | `DataFrame result = df.drop(col("unwanted"));` |
| `alias` | String alias | `Dataset[T]` | 设置别名 | `DataFrame aliased = df.alias("t1");<br>df.alias("t1").join(df.alias("t2"), col("t1.id").equalTo(col("t2.id")));` |
| `as` | String alias | `Dataset[T]` | 设置别名（同alias） | `DataFrame aliased = df.as("my_table");` |
| `toDF` | 无 | `DataFrame` | 转换为DataFrame | `DataFrame df = ds.toDF();` |
| `toDF` | String... colNames | `DataFrame` | 转换为DataFrame并重命名列 | `DataFrame df = ds.toDF("id", "name", "value");` |
| `as` | Encoder[U] encoder | `Dataset[U]` | 类型转换 | `Dataset<MyClass> ds = df.as(Encoders.bean(MyClass.class));` |
| `na` | 无 | `DataFrameNaFunctions` | 获取null值处理工具 | `DataFrameNaFunctions naFuncs = df.na();<br>DataFrame cleaned = df.na().drop();  // 删除含null的行` |
| `stat` | 无 | `DataFrameStatFunctions` | 获取统计工具 | `DataFrameStatFunctions statFuncs = df.stat();<br>double corr = df.stat().corr("col1", "col2");` |

| `freqItems` | String[] cols, double support | `Dataset[Row]` | 频繁项集挖掘 | `Dataset<Row> freq = df.stat().freqItems(new String[]{"category"}, 0.3);` |
| `freqItems` | String[] cols | `Dataset[Row]` | 频繁项集挖掘（默认support） | `Dataset<Row> freq = df.stat().freqItems(new String[]{"category"});` |
| `sampleBy` | String col, Map<K, Double> fractions, long seed | `Dataset[Row]` | 按列分层采样 | `Dataset<Row> sampled = df.stat().sampleBy("category", fractions, seed);` |
| `crosstab` | String col1, String col2 | `Dataset[Row]` | 交叉表 | `Dataset<Row> cross = df.stat().crosstab("category", "region");` |
| `cov` | String col1, String col2 | `double` | 协方差 | `double cov = df.stat().cov("x", "y");` |
| `approxQuantile` | String col, double[] probabilities, double relativeError | `double[]` | 近似分位数 | `double[] quantiles = df.stat().approxQuantile("value", new double[]{0.25, 0.5, 0.75}, 0.01);` |

| `describe` | String... cols | `DataFrame` | 计算统计描述 | `DataFrame stats = df.describe("age", "salary");<br>stats.show();  // 显示count, mean, stddev, min, max` |
| `summary` | String... statistics | `DataFrame` | 计算指定统计量 | `DataFrame stats = df.summary("count", "mean", "max");` |
| `sample` | double fraction | `Dataset[T]` | 随机采样 | `DataFrame sample = df.sample(0.1);  // 10%采样` |
| `sample` | boolean withReplacement, double fraction, long seed | `Dataset[T]` | 随机采样，指定参数 | `DataFrame sample = df.sample(false, 0.1, 42L);` |
| `randomSplit` | double[] weights | `Dataset[T][]` | 按权重随机分割 | `Dataset<Row>[] splits = df.randomSplit(new double[]{0.7, 0.3});<br>DataFrame train = splits[0];<br>DataFrame test = splits[1];` |
| `randomSplit` | double[] weights, long seed | `Dataset[T][]` | 按权重随机分割，指定种子 | `Dataset<Row>[] splits = df.randomSplit(new double[]{0.7, 0.3}, 42L);` |
| `randomSplitAsList` | double[] weights, long seed | `List[Dataset[T]]` | 按权重分割为List | `List<Dataset<Row>> splits = df.randomSplitAsList(new double[]{0.7, 0.3}, 42L);` |
| `repartition` | int numPartitions | `Dataset[T]` | 重新分区 | `DataFrame repartitioned = df.repartition(10);` |
| `repartition` | int numPartitions, Column... partitionExprs | `Dataset[T]` | 按表达式分区 | `DataFrame partitioned = df.repartition(10, col("category"));` |
| `repartition` | Column... partitionExprs | `Dataset[T]` | 按表达式分区（默认分区数） | `DataFrame partitioned = df.repartition(col("category"));` |
| `repartitionByRange` | int numPartitions, Column... partitionExprs | `Dataset[T]` | 范围分区 | `DataFrame rangePartitioned = df.repartitionByRange(5, col("id"));` |
| `coalesce` | int numPartitions | `Dataset[T]` | 合并分区（不shuffle） | `DataFrame merged = df.coalesce(2);` |
| `cache` | 无 | `Dataset[T]` | 缓存 | `DataFrame cached = df.cache();` |
| `persist` | 无 | `Dataset[T]` | 持久化（默认MEMORY_AND_DISK） | `DataFrame persisted = df.persist();` |
| `persist` | StorageLevel newLevel | `Dataset[T]` | 持久化到指定级别 | `DataFrame persisted = df.persist(StorageLevel.MEMORY_ONLY());` |
| `unpersist` | 无 | `Dataset[T]` | 取消持久化 | `df.unpersist();` |
| `unpersist` | boolean blocking | `Dataset[T]` | 取消持久化，指定阻塞 | `df.unpersist(true);  // 阻塞等待释放` |
| `checkpoint` | 无 | `Dataset[T]` | checkpoint | `DataFrame checked = df.checkpoint();` |
| `localCheckpoint` | 无 | `Dataset[T]` | 本地checkpoint | `DataFrame localCheck = df.localCheckpoint();` |
| `createTempView` | String viewName | `Unit` | 创建临时视图 | `df.createTempView("my_view");<br>spark.sql("SELECT * FROM my_view");` |
| `createOrReplaceTempView` | String viewName | `Unit` | 创建或替换临时视图 | `df.createOrReplaceTempView("my_view");` |
| `createGlobalTempView` | String viewName | `Unit` | 创建全局临时视图 | `df.createGlobalTempView("global_view");<br>spark.sql("SELECT * FROM global_temp.global_view");` |
| `write` | 无 | `DataFrameWriter[T]` | 获取写入器 | `df.write().mode("overwrite").parquet("output.parquet");` |
| `writeTo` | String table | `DataFrameWriterV2[T]` | 写入表（V2 API） | `df.writeTo("catalog.db.table").append();` |
| `writeStream` | 无 | `DataStreamWriter[T]` | 获取流写入器 | `df.writeStream().format("console").start();` |
| `inputFiles` | 无 | `String[]` | 获取输入文件列表 | `String[] files = df.inputFiles();` |
| `isEmpty` | 无 | `boolean` | 判断是否为空 | `boolean empty = df.isEmpty();` |
| `explain` | 无 | `Unit` | 打印执行计划 | `df.explain();` |
| `explain` | boolean extended | `Unit` | 打印详细执行计划 | `df.explain(true);  // 显示物理计划和逻辑计划` |
| `explain` | String mode | `Unit` | 打印执行计划（指定模式） | `df.explain("extended");<br>// mode: simple, extended, codegen, cost, formatted` |

| `groupByCube` | Column... cols | `RelationalGroupedDataset` | 立方体分组（所有维度组合） | `RelationalGroupedDataset cube = ds.groupByCube("year", "month", "day");` |
| `groupByRollup` | Column... cols | `RelationalGroupedDataset` | 上卷分组（层级聚合） | `RelationalGroupedDataset rollup = ds.groupByRollup("year", "month");` |
| `unionAll` | Dataset[T] other | `Dataset[T]` | 联合所有（保留重复） | `Dataset<Row> union = ds1.unionAll(ds2);` |
| `dropDuplicatesWithinWatermark` | String... cols | `Dataset[T]` | 在watermark内去重 | `Dataset<Row> dedup = ds.dropDuplicatesWithinWatermark("id");` |
| `withColumnsRenamed` | Map<String, String> cols | `Dataset[T]` | 批量重命名列 | `Dataset<Row> renamed = ds.withColumnsRenamed(Map.of("old1", "new1", "old2", "new2"));` |
| `withWatermark` | String eventTime, String delayThreshold | `Dataset[T]` | 设置watermark用于流处理 | `Dataset<Row> withWm = ds.withWatermark("timestamp", "10 minutes");` |
| `hint` | String name, Object... params | `Dataset[T]` | 添加查询提示 | `Dataset<Row> hinted = ds.hint("broadcast");` |
| `writeToMetadata` | String tableName | `DataFrameWriter[T]` | 写入元数据表 | `df.write().writeToMetadata("metadata_table");` |
| `saveAsParquetFile` | String path | `Unit` | 保存为Parquet（旧API） | `ds.saveAsParquetFile("hdfs://path/");` |
| `observe` | String name, Column expr, Column... exprs | `Dataset[T]` | 观察聚合指标 | `Dataset<Row> observed = ds.observe("metric", count("*").as("cnt"));` |
| `queryExecution` | 无 | `QueryExecution` | 获取查询执行计划 | `QueryExecution qe = ds.queryExecution();` |
| `isStreaming` | 无 | `boolean` | 是否流Dataset | `boolean streaming = ds.isStreaming();` |
| `toJavaRDD` | 无 | `JavaRDD[T]` | 转为Java RDD | `JavaRDD<Row> javaRdd = ds.toJavaRDD();` |
| `storageLevel` | 无 | `StorageLevel` | 获取存储级别 | `StorageLevel level = ds.storageLevel();` |
| `createOrReplaceGlobalTempView` | String viewName | `Unit` | 创建或替换全局临时视图 | `ds.createOrReplaceGlobalTempView("global_view");` |
| `toLocalIteratorAsList` | 无 | `List[T]` | 转为本地迭代器List | `List<Row> list = ds.toLocalIteratorAsList();` |
| `reduceAgg` | Column e | `Row` | 聚合reduce | `Row result = ds.reduceAgg(col("value"));` |
| `aggByAddr` | Column... exprs | `Dataset[Row]` | 按地址聚合 | `Dataset<Row> result = ds.aggByAddr(sum("value"), count("*"));` |

---

## SparkConf（配置）

### RelationalGroupedDataset
**包路径**: `org.apache.spark.sql`
**说明**: 分组后的Dataset，用于聚合操作。由Dataset.groupBy()返回。
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `agg` | Column... exprs | `Dataset[Row]` | 聚合操作 | `Dataset<Row> result = grouped.agg(count("id").as("cnt"), sum("value").as("total"));` |
| `agg` | Map<String, Column> exprs | `Dataset[Row]` | 聚合操作（Map形式） | `Dataset<Row> result = grouped.agg(Map.of("cnt", count("id"), "avg", avg("value")));` |
| `count` | 无 | `Dataset[Row]` | 计数 | `Dataset<Row> counts = grouped.count();` |
| `mean` | String... cols | `Dataset[Row]` | 平均值 | `Dataset<Row> means = grouped.mean("value", "score");` |
| `avg` | String... cols | `Dataset[Row]` | 平均值（别名） | `Dataset<Row> avg = grouped.avg("value");` |
| `max` | String... cols | `Dataset[Row]` | 最大值 | `Dataset<Row> maxes = grouped.max("value");` |
| `min` | String... cols | `Dataset[Row]` | 最小值 | `Dataset<Row> mins = grouped.min("value");` |
| `sum` | String... cols | `Dataset[Row]` | 求和 | `Dataset<Row> sums = grouped.sum("value");` |
| `pivot` | String pivotColumn | `RelationalGroupedDataset` | 透视转换（自动发现值） | `RelationalGroupedDataset pivoted = grouped.pivot("month");` |
| `pivot` | String pivotColumn, Object... values | `RelationalGroupedDataset` | 透视转换（指定值） | `RelationalGroupedDataset pivoted = grouped.pivot("month", "Jan", "Feb", "Mar");` |
| `pivot` | String pivotColumn, List<Object> values | `RelationalGroupedDataset` | 透视转换（List形式） | `RelationalGroupedDataset pivoted = grouped.pivot("month", Arrays.asList("Jan", "Feb"));` |
| `as` | String alias | `RelationalGroupedDataset` | 别名 | `RelationalGroupedDataset aliased = grouped.as("my_group");` |
| `alias` | String alias | `RelationalGroupedDataset` | 别名 | `RelationalGroupedDataset aliased = grouped.alias("my_group");` |
| `cogroup` | Dataset[U] other, MapFunction[T, K] thisFunc, MapFunction[U, K] otherFunc, Encoder[K] encoder | `KeyValueGroupedDataset[K, Tuple[T, U]]` | 协同分组 | `KeyValueGroupedDataset<String, Tuple2<Row, Row>> cogrouped = grouped.cogroup(otherDs, func1, func2, encoder);` |
| `flatMapGroups` | FlatMapGroupsFunction[K, V, R] f | `Dataset[R]` | 扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroups((key, iter) -> {...});` |
| `mapGroups` | MapGroupsFunction[K, V, R] f | `Dataset[R]` | 映射分组 | `Dataset<Row> result = grouped.mapGroups((key, iter) -> {...});` |
| `mapGroupsWithState` | MapGroupsWithStateFunction[K, V, S, R] func, OutputMode outputMode, Encoder[S] stateEncoder, Encoder[R] outputEncoder | `Dataset[R]` | 带状态的分组映射 | `Dataset<Row> result = grouped.mapGroupsWithState(stateFunc, OutputMode.Update(), stateEnc, outputEnc);` |
| `flatMapGroupsWithState` | FlatMapGroupsWithStateFunction[K, V, S, R] func, OutputMode outputMode, Encoder[S] stateEncoder, Encoder[R] outputEncoder | `Dataset[R]` | 带状态的扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroupsWithState(stateFunc, OutputMode.Append(), stateEnc, outputEnc);` |
| `flatMapGroupsInPandas` | FlatMapGroupsInPandasFunction[K, V, R] f | `Dataset[R]` | Pandas扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroupsInPandas(pandasFunc);` |
| `applyInPandas` | ApplyInPandasFunction[K, V, R] f | `Dataset[R]` | Pandas apply函数 | `Dataset<Row> result = grouped.applyInPandas(pandasFunc);` |

---

### KeyValueGroupedDataset[K, V]
**包路径**: `org.apache.spark.sql`
**说明**: 按键分组后的Dataset，由Dataset.groupByKey()返回。支持更灵活的分组操作。
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `agg` | Aggregator[V, S, R] aggregator | `Dataset[R]` | 使用Aggregator聚合 | `Dataset<Row> result = grouped.agg(new MyAggregator());` |
| `reduceGroups` | ReduceFunction[V] f | `Dataset[Tuple2[K, V]]` | 按组reduce | `Dataset<Tuple2<String, Integer>> reduced = grouped.reduceGroups((a, b) -> a + b);` |
| `mapGroups` | MapGroupsFunction[K, V, U] f, Encoder[U] encoder | `Dataset[U]` | 映射每组数据 | `Dataset<String> mapped = grouped.mapGroups((key, iter) -> key + ":" + iter.size(), Encoders.STRING());` |
| `flatMapGroups` | FlatMapGroupsFunction[K, V, U] f, Encoder[U] encoder | `Dataset[U]` | 扁平映射每组数据 | `Dataset<String> flatMapped = grouped.flatMapGroups((key, iter) -> {...}, Encoders.STRING());` |
| `mapGroupsWithState` | MapGroupsWithStateFunction[K, V, S, U] func, Encoder[S] stateEncoder, Encoder[U] outputEncoder | `Dataset[U]` | 带状态的分组映射 | `Dataset<Row> result = grouped.mapGroupsWithState(stateFunc, stateEnc, outputEnc);` |
| `flatMapGroupsWithState` | FlatMapGroupsWithStateFunction[K, V, S, U] func, OutputMode outputMode, Encoder[S] stateEncoder, Encoder[U] outputEncoder | `Dataset[U]` | 带状态的扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroupsWithState(stateFunc, OutputMode.Update(), stateEnc, outputEnc);` |
| `keys` | 无 | `Dataset[K]` | 获取所有键 | `Dataset<String> keys = grouped.keys();` |
| `keyAs` | Encoder[K] encoder | `KeyValueGroupedDataset[K, V]` | 指定键编码器 | `KeyValueGroupedDataset<String, Row> newGrouped = grouped.keyAs(Encoders.STRING());` |
| `mapValues` | MapFunction[V, U] f, Encoder[U] encoder | `KeyValueGroupedDataset[K, U]` | 映射值 | `KeyValueGroupedDataset<String, String> mapped = grouped.mapValues(v -> v.toString(), Encoders.STRING());` |
| `flatMapValues` | FlatMapFunction[V, U] f, Encoder[U] encoder | `KeyValueGroupedDataset[K, U]` | 扁平映射值 | `KeyValueGroupedDataset<String, String> flatMapped = grouped.flatMapValues(v -> {...}, Encoders.STRING());` |
| `cogroup` | KeyValueGroupedDataset[K, W] other | `KeyValueGroupedDataset[K, Tuple2[V, W]]` | 协同分组 | `KeyValueGroupedDataset<String, Tuple2<Row, Row>> cogrouped = grouped.cogroup(otherGrouped);` |
| `cogroup` | KeyValueGroupedDataset[K, W] other, CoGroupFunction[K, V, W, U] f, Encoder[U] encoder | `Dataset[U]` | 协同分组并处理 | `Dataset<Row> result = grouped.cogroup(otherGrouped, coGroupFunc, Encoders.bean(Row.class));` |

---

### SparkConf
**包路径**: `org.apache.spark`
**说明**: Spark配置类，用于设置各种Spark参数。
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `set` | String key, String value | `SparkConf` | 设置配置项 | `SparkConf conf = new SparkConf().set("spark.executor.memory", "4g");` |
| `setMaster` | String master | `SparkConf` | 设置运行模式 | `conf.setMaster("local[4]");` |
| `setAppName` | String name | `SparkConf` | 设置应用名称 | `conf.setAppName("My Spark App");` |
| `setSparkHome` | String home | `SparkConf` | 设置Spark安装目录 | `conf.setSparkHome("/opt/spark");` |
| `setExecutorEnv` | String key, String value | `SparkConf` | 设置Executor环境变量 | `conf.setExecutorEnv("JAVA_HOME", "/usr/lib/jvm/java-11");` |
| `setJars` | String... jars | `SparkConf` | 设置依赖JAR包 | `conf.setJars("hdfs://libs/my-lib.jar");` |
| `setAll` | Map[String, String] settings | `SparkConf` | 批量设置配置 | `Map<String, String> settings = new HashMap<>();<br>settings.put("spark.executor.cores", "2");<br>conf.setAll(settings);` |
| `get` | String key | `String` | 获取配置值 | `String value = conf.get("spark.executor.memory");` |
| `get` | String key, String defaultValue | `String` | 获取配置值，带默认值 | `String value = conf.get("spark.executor.memory", "2g");` |
| `getAll` | 无 | `Array[Tuple2[String, String]]` | 获取所有配置 | `Tuple2<String, String>[] all = conf.getAll();` |
| `contains` | String key | `Boolean` | 检查配置是否存在 | `boolean exists = conf.contains("spark.executor.memory");` |
| `remove` | String key | `SparkConf` | 移除配置项 | `conf.remove("spark.executor.memory");` |
| `clone` | 无 | `SparkConf` | 克隆配置 | `SparkConf cloned = conf.clone();` |

---

## Broadcast & Accumulator（共享变量）

### Broadcast[T]
**包路径**: `org.apache.spark.broadcast`
**说明**: 广播变量，将数据高效分发到所有Executor。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `value` | 无 | `T` | 获取广播变量的值 | `Broadcast<Map<String, String>> config = sc.broadcast(configMap);<br>Map<String, String> map = config.value();` |
| `unpersist` | 无 | `Unit` | 从Executor释放广播变量 | `config.unpersist();` |
| `unpersist` | Boolean blocking | `Unit` | 从Executor释放，指定阻塞 | `config.unpersist(true);  // 阻塞等待释放` |
| `destroy` | 无 | `Unit` | 完全销毁广播变量 | `config.destroy();  // Driver和Executor都释放` |

### Accumulator[T]
**包路径**: `org.apache.spark`
**说明**: 累加器，用于聚合Worker端数据到Driver。仅支持累加操作。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | T term | `Unit` | 累加值（只能在Worker端调用） | `Accumulator<Integer> acc = sc.accumulator(0);<br>rdd.foreach(x -> acc.add(x));` |
| `value` | 无 | `T` | 获取累加结果（只能在Driver端调用） | `int total = acc.value();` |
| `setValue` | T newValue | `Unit` | 设置值（只能在Driver端调用） | `acc.setValue(100);` |
| `isZero` | 无 | `Boolean` | 检查是否为零值 | `boolean zero = acc.isZero();` |
| `reset` | 无 | `Unit` | 重置为零值 | `acc.reset();` |
| `name` | 无 | `String` | 获取累加器名称 | `String name = acc.name();` |

### LongAccumulator / DoubleAccumulator / CollectionAccumulator
**包路径**: `org.apache.spark.util`
**说明**: 特化累加器，支持特定类型的累加。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | Long/Double/T term | `Unit` | 累加值 | `LongAccumulator longAcc = sc.sc().longAccumulator("counter");<br>longAcc.add(10);` |
| `value` | 无 | `Long/Double/List[T]` | 获取累加结果 | `long sum = longAcc.value();` |
| `count` | 无 | `Long` | 获取计数（LongAccumulator） | `long count = longAcc.count();` |
| `avg` | 无 | `Double` | 获取平均值（LongAccumulator/DoubleAccumulator） | `double avg = longAcc.avg();` |
| `sum` | 无 | `Long/Double` | 获取总和 | `long sum = longAcc.sum();` |

---

## SQL辅助类

### AccumulatorV2[T]
**包路径**: `org.apache.spark.util`
**说明**: 累加器V2版本，用于分布式计数和聚合。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | T v | `Unit` | 添加值 | `accumulator.add(1L);` |
| `value` | 无 | `T` | 获取值 | `long value = accumulator.value();` |
| `copy` | 无 | `AccumulatorV2[T]` | 复制累加器 | `AccumulatorV2<Long> copy = accumulator.copy();` |
| `isZero` | 无 | `boolean` | 是否为零 | `boolean isZero = accumulator.isZero();` |
| `reset` | 无 | `Unit` | 重置为零 | `accumulator.reset();` |
| `merge` | AccumulatorV2[T] other | `Unit` | 合并另一个累加器 | `accumulator.merge(otherAccumulator);` |

---

### DoubleAccumulator
**包路径**: `org.apache.spark.util`
**说明**: 双精度浮点数累加器。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | double v | `Unit` | 添加值 | `doubleAccumulator.add(3.14);` |
| `value` | 无 | `double` | 获取值 | `double value = doubleAccumulator.value();` |
| `reset` | 无 | `Unit` | 重置为零 | `doubleAccumulator.reset();` |
| `isZero` | 无 | `boolean` | 是否为零 | `boolean isZero = doubleAccumulator.isZero();` |

---

### CollectionAccumulator[T]
**包路径**: `org.apache.spark.util`
**说明**: 集合累加器，收集所有添加的元素。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | T v | `Unit` | 添加元素 | `collectionAccumulator.add("element");` |
| `value` | 无 | `java.util.List[T]` | 获取所有元素 | `List<String> elements = collectionAccumulator.value();` |
| `reset` | 无 | `Unit` | 重置为空 | `collectionAccumulator.reset();` |
| `isZero` | 无 | `boolean` | 是否为空 | `boolean isZero = collectionAccumulator.isZero();` |

---

### Column
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame列引用，用于构建SQL表达式。
**方法数量**: 40

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `col` | String colName | `Column` | 创建列引用（静态方法） | `Column c = col("age");` |
| `equalTo` | Object other | `Column` | 等于条件 | `df.filter(col("id").equalTo(100));` |
| `notEqual` | Object other | `Column` | 不等于条件 | `df.filter(col("status").notEqual("deleted"));` |
| `gt` | Object other | `Column` | 大于条件 | `df.filter(col("age").gt(18));` |
| `lt` | Object other | `Column` | 小于条件 | `df.filter(col("price").lt(1000));` |
| `geq` | Object other | `Column` | 大于等于条件 | `df.filter(col("score").geq(60));` |
| `leq` | Object other | `Column` | 小于等于条件 | `df.filter(col("qty").leq(10));` |
| `isNull` | 无 | `Column` | 判断是否为null | `df.filter(col("email").isNull());` |
| `isNotNull` | 无 | `Column` | 判断是否非null | `df.filter(col("email").isNotNull());` |
| `and` | Column other | `Column` | 逻辑与 | `df.filter(col("age").gt(18).and(col("status").equalTo("active")));` |
| `or` | Column other | `Column` | 逻辑或 | `df.filter(col("type").equalTo("A").or(col("type").equalTo("B")));` |
| `plus` | Object other | `Column` | 加法 | `df.withColumn("total", col("price").plus(col("tax")));` |
| `minus` | Object other | `Column` | 减法 | `df.withColumn("diff", col("end").minus(col("start")));` |
| `multiply` | Object other | `Column` | 乘法 | `df.withColumn("double", col("value").multiply(2));` |
| `divide` | Object other | `Column` | 除法 | `df.withColumn("avg", col("total").divide(col("count")));` |
| `mod` | Object other | `Column` | 取模 | `df.filter(col("id").mod(2).equalTo(0));  // 奇数` |
| `like` | String literal | `Column` | LIKE匹配 | `df.filter(col("name").like("%John%"));` |
| `rlike` | String regex | `Column` | 正则匹配 | `df.filter(col("email").rlike("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$"));` |
| `contains` | String other | `Column` | 包含字符串 | `df.filter(col("content").contains("error"));` |
| `startsWith` | String prefix | `Column` | 以...开始 | `df.filter(col("name").startsWith("John"));` |
| `endsWith` | String suffix | `Column` | 以...结束 | `df.filter(col("filename").endsWith(".csv"));` |
| `alias` | String alias | `Column` | 设置别名 | `df.select(col("id").alias("user_id"));` |
| `as` | String alias | `Column` | 设置别名（同alias） | `df.select(col("id").as("user_id"));` |
| `cast` | DataType to | `Column` | 类型转换 | `df.withColumn("id_str", col("id").cast(DataTypes.StringType));` |
| `asc` | 无 | `Column` | 升序排序 | `df.orderBy(col("id").asc());` |
| `desc` | 无 | `Column` | 降序排序 | `df.orderBy(col("id").desc());` |
| `asc_nulls_first` | 无 | `Column` | 升序，null排前 | `df.orderBy(col("value").asc_nulls_first());` |
| `asc_nulls_last` | 无 | `Column` | 升序，null排后 | `df.orderBy(col("value").asc_nulls_last());` |
| `desc_nulls_first` | 无 | `Column` | 降序，null排前 | `df.orderBy(col("value").desc_nulls_first());` |
| `desc_nulls_last` | 无 | `Column` | 降序，null排后 | `df.orderBy(col("value").desc_nulls_last());` |
| `between` | Object lowerBound, Object upperBound | `Column` | 范围条件 | `df.filter(col("age").between(18, 65));` |
| `when` | Column condition, Object value | `Column` | CASE WHEN条件 | `df.withColumn("category", when(col("age").lt(18), "child")<br>    .when(col("age").lt(60), "adult")<br>    .otherwise("senior"));` |
| `otherwise` | Object value | `Column` | CASE WHEN默认值 | `when(col("score").geq(90), "A").otherwise("B");` |
| `over` | Window window | `Column` | 窗口函数 | `col("value").sum().over(Window.partitionBy("group"));` |
| `isNull` | 无 | `Column` | 判断null | `df.filter(col("name").isNull());` |
| `isNotNull` | 无 | `Column` | 判断非null | `df.filter(col("name").isNotNull());` |
| `isin` | Object... values | `Column` | IN条件 | `df.filter(col("status").isin("active", "pending", "running"));` |
| `in` | Column list | `Column` | IN子查询 | `df.filter(col("id").in(otherDf.select(col("user_id"))));` |
| `substr` | int startPos, int len | `Column` | 截取子串 | `df.withColumn("first3", col("name").substr(0, 3));` |
| `upper` | 无 | `Column` | 转大写 | `df.withColumn("upper_name", col("name").upper());` |
| `lower` | 无 | `Column` | 转小写 | `df.withColumn("lower_name", col("name").lower());` |

### functions（内置函数）
**包路径**: `org.apache.spark.sql.functions`
**说明**: Spark SQL内置函数集合，提供聚合、字符串、数学、日期等函数。
**方法数量**: 100

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `count` | Column e | `Column` | 计数 | `df.agg(count(col("id")));` |
| `countDistinct` | Column e, Column... es | `Column` | 唯一值计数 | `df.agg(countDistinct(col("user_id")));` |
| `sum` | Column e | `Column` | 求和 | `df.agg(sum(col("amount")));` |
| `sumDistinct` | Column e | `Column` | 唯一值求和 | `df.agg(sumDistinct(col("price")));` |
| `avg` | Column e | `Column` | 平均值 | `df.agg(avg(col("score")));` |
| `mean` | Column e | `Column` | 平均值（同avg） | `df.agg(mean(col("score")));` |
| `max` | Column e | `Column` | 最大值 | `df.agg(max(col("price")));` |
| `min` | Column e | `Column` | 最小值 | `df.agg(min(col("price")));` |
| `first` | Column e | `Column` | 第一个值 | `df.agg(first(col("name")));` |
| `first` | Column e, boolean ignoreNulls | `Column` | 第一个非null值 | `df.agg(first(col("name"), true));` |
| `last` | Column e | `Column` | 最后一个值 | `df.agg(last(col("name")));` |
| `last` | Column e, boolean ignoreNulls | `Column` | 最后一个非null值 | `df.agg(last(col("name"), true));` |
| `collect_list` | Column e | `Column` | 收集为数组（保留重复） | `df.groupBy("group").agg(collect_list(col("value")));` |
| `collect_set` | Column e | `Column` | 收集为数组（去重） | `df.groupBy("group").agg(collect_set(col("value")));` |
| `approx_count_distinct` | Column e | `Column` | 近似唯一值计数 | `df.agg(approx_count_distinct(col("user_id")));` |
| `approx_count_distinct` | Column e, double rsd | `Column` | 近似计数，指定误差率 | `df.agg(approx_count_distinct(col("user_id"), 0.05));` |
| `variance` | Column e | `Column` | 方差 | `df.agg(variance(col("value")));` |
| `var_samp` | Column e | `Column` | 样本方差 | `df.agg(var_samp(col("value")));` |
| `var_pop` | Column e | `Column` | 总体方差 | `df.agg(var_pop(col("value")));` |
| `stddev` | Column e | `Column` | 标准差 | `df.agg(stddev(col("value")));` |
| `stddev_samp` | Column e | `Column` | 样本标准差 | `df.agg(stddev_samp(col("value")));` |
| `stddev_pop` | Column e | `Column` | 总体标准差 | `df.agg(stddev_pop(col("value")));` |
| `skewness` | Column e | `Column` | 偏度 | `df.agg(skewness(col("value")));` |
| `kurtosis` | Column e | `Column` | 峰度 | `df.agg(kurtosis(col("value")));` |
| `corr` | Column col1, Column col2 | `Column` | Pearson相关系数 | `df.agg(corr(col("price"), col("rating")));` |
| `covar_pop` | Column col1, Column col2 | `Column` | 总体协方差 | `df.agg(covar_pop(col("x"), col("y")));` |
| `covar_samp` | Column col1, Column col2 | `Column` | 样本协方差 | `df.agg(covar_samp(col("x"), col("y")));` |
| `lit` | Object literal | `Column` | 创建常量列 | `df.withColumn("constant", lit(100));` |
| `col` | String colName | `Column` | 创建列引用 | `df.select(col("name"));` |
| `column` | String colName | `Column` | 创建列引用（同col） | `df.select(column("name"));` |
| `when` | Column condition, Object value | `Column` | CASE WHEN | `when(col("age").lt(18), "child").otherwise("adult");` |
| `concat` | Column... exprs | `Column` | 连接字符串 | `df.withColumn("full_name", concat(col("first"), lit(" "), col("last")));` |
| `concat_ws` | String sep, Column... exprs | `Column` | 用分隔符连接字符串 | `df.withColumn("tags", concat_ws(",", col("tag1"), col("tag2")));` |
| `split` | Column str, String pattern | `Column` | 分割字符串为数组 | `df.withColumn("words", split(col("sentence"), " "));` |
| `substring` | Column str, int pos, int len | `Column` | 截取子串 | `df.withColumn("abbr", substring(col("name"), 0, 3));` |
| `length` | Column e | `Column` | 字符串长度 | `df.withColumn("name_len", length(col("name")));` |
| `trim` | Column e | `Column` | 去除两端空白 | `df.withColumn("clean_name", trim(col("name")));` |
| `ltrim` | Column e | `Column` | 去除左侧空白 | `df.withColumn("clean", ltrim(col("name")));` |
| `rtrim` | Column e | `Column` | 去除右侧空白 | `df.withColumn("clean", rtrim(col("name")));` |
| `upper` | Column e | `Column` | 转大写 | `df.withColumn("upper_name", upper(col("name")));` |
| `lower` | Column e | `Column` | 转小写 | `df.withColumn("lower_name", lower(col("name")));` |
| `initcap` | Column e | `Column` | 首字母大写 | `df.withColumn("capitalized", initcap(col("name")));` |
| `regexp_replace` | Column e, String pattern, String replacement | `Column` | 正则替换 | `df.withColumn("clean", regexp_replace(col("text"), "[0-9]+", ""));` |
| `regexp_extract` | Column e, String pattern, int idx | `Column` | 正则提取 | `df.withColumn("domain", regexp_extract(col("url"), "https?://([^/]+)", 1));` |
| `instr` | Column str, String substring | `Column` | 查找子串位置 | `df.filter(instr(col("name"), "John") > 0);` |
| `locate` | String substr, Column str | `Column` | 查找子串位置 | `df.filter(locate("John", col("name")) > 0);` |
| `replace` | Column src, Column search, Column replace | `Column` | 字符替换 | `df.withColumn("clean", replace(col("text"), lit("old"), lit("new")));` |
| `abs` | Column e | `Column` | 绝对值 | `df.withColumn("abs_value", abs(col("value")));` |
| `ceil` | Column e | `Column` | 向上取整 | `df.withColumn("rounded", ceil(col("value")));` |
| `floor` | Column e | `Column` | 向下取整 | `df.withColumn("rounded", floor(col("value")));` |
| `round` | Column e | `Column` | 四舍五入 | `df.withColumn("rounded", round(col("value")));` |
| `round` | Column e, int scale | `Column` | 四舍五入到指定小数位 | `df.withColumn("rounded", round(col("value"), 2));` |
| `bround` | Column e | `Column` | 银行家舍入 | `df.withColumn("rounded", bround(col("value")));` |
| `exp` | Column e | `Column` | e指数 | `df.withColumn("exp_val", exp(col("log_value")));` |
| `log` | Column e | `Column` | 自然对数 | `df.withColumn("log_val", log(col("value")));` |
| `log10` | Column e | `Column` | 10为底对数 | `df.withColumn("log10_val", log10(col("value")));` |
| `log2` | Column e | `Column` | 2为底对数 | `df.withColumn("log2_val", log2(col("value")));` |
| `pow` | Column l, Column r | `Column` | 幂运算 | `df.withColumn("squared", pow(col("value"), lit(2)));` |
| `sqrt` | Column e | `Column` | 平方根 | `df.withColumn("sqrt_val", sqrt(col("value")));` |
| `sin` | Column e | `Column` | 正弦 | `df.withColumn("sin_val", sin(col("angle")));` |
| `cos` | Column e | `Column` | 余弦 | `df.withColumn("cos_val", cos(col("angle")));` |
| `tan` | Column e | `Column` | 正切 | `df.withColumn("tan_val", tan(col("angle")));` |
| `asin` | Column e | `Column` | 反正弦 | `df.withColumn("asin_val", asin(col("value")));` |
| `acos` | Column e | `Column` | 反余弦 | `df.withColumn("acos_val", acos(col("value")));` |
| `atan` | Column e | `Column` | 反正切 | `df.withColumn("atan_val", atan(col("value")));` |
| `rand` | 无 | `Column` | 随机数（0-1） | `df.withColumn("random", rand());` |
| `randn` | 无 | `Column` | 正态分布随机数 | `df.withColumn("normal", randn());` |
| `current_date` | 无 | `Column` | 当前日期 | `df.withColumn("today", current_date());` |
| `current_timestamp` | 无 | `Column` | 当前时间戳 | `df.withColumn("now", current_timestamp());` |
| `date_add` | Column start, int days | `Column` | 日期加天数 | `df.withColumn("future", date_add(col("date"), 30));` |
| `date_sub` | Column start, int days | `Column` | 日期减天数 | `df.withColumn("past", date_sub(col("date"), 30));` |
| `datediff` | Column end, Column start | `Column` | 日期差（天数） | `df.withColumn("days_diff", datediff(col("end_date"), col("start_date")));` |
| `add_months` | Column startDate, int numMonths | `Column` | 加月份 | `df.withColumn("future", add_months(col("date"), 12));` |
| `months_between` | Column end, Column start | `Column` | 月份差 | `df.withColumn("months", months_between(col("end_date"), col("start_date")));` |
| `year` | Column e | `Column` | 提取年份 | `df.withColumn("year", year(col("date")));` |
| `month` | Column e | `Column` | 提取月份 | `df.withColumn("month", month(col("date")));` |
| `dayofmonth` | Column e | `Column` | 提取日 | `df.withColumn("day", dayofmonth(col("date")));` |
| `dayofweek` | Column e | `Column` | 提取星期几（1=周日） | `df.withColumn("weekday", dayofweek(col("date")));` |
| `dayofyear` | Column e | `Column` | 提取年中第几天 | `df.withColumn("daynum", dayofyear(col("date")));` |
| `weekofyear` | Column e | `Column` | 提取年中第几周 | `df.withColumn("week", weekofyear(col("date")));` |
| `hour` | Column e | `Column` | 提取小时 | `df.withColumn("hour", hour(col("timestamp")));` |
| `minute` | Column e | `Column` | 提取分钟 | `df.withColumn("minute", minute(col("timestamp")));` |
| `second` | Column e | `Column` | 提取秒 | `df.withColumn("second", second(col("timestamp")));` |
| `to_date` | Column e | `Column` | 转为日期 | `df.withColumn("date", to_date(col("date_str")));` |
| `to_date` | Column e, String fmt | `Column` | 指定格式转日期 | `df.withColumn("date", to_date(col("date_str"), "yyyy-MM-dd"));` |
| `to_timestamp` | Column e | `Column` | 转为时间戳 | `df.withColumn("ts", to_timestamp(col("ts_str")));` |
| `to_timestamp` | Column e, String fmt | `Column` | 指定格式转时间戳 | `df.withColumn("ts", to_timestamp(col("ts_str"), "yyyy-MM-dd HH:mm:ss"));` |
| `date_format` | Column dateExpr, String format | `Column` | 格式化日期 | `df.withColumn("formatted", date_format(col("date"), "yyyy年MM月dd日"));` |
| `from_unixtime` | Column ut | `Column` | Unix时间戳转字符串 | `df.withColumn("time_str", from_unixtime(col("unix_ts")));` |
| `unix_timestamp` | 无 | `Column` | 当前Unix时间戳 | `df.withColumn("ts", unix_timestamp());` |
| `unix_timestamp` | Column time | `Column` | 转为Unix时间戳 | `df.withColumn("unix", unix_timestamp(col("timestamp")));` |
| `unix_timestamp` | Column time, String fmt | `Column` | 指定格式转Unix时间戳 | `df.withColumn("unix", unix_timestamp(col("time_str"), "yyyy-MM-dd"));` |
| `array` | Column... cols | `Column` | 创建数组 | `df.withColumn("arr", array(col("a"), col("b")));` |
| `map` | Column... cols | `Column` | 创建Map | `df.withColumn("kv", map(col("key"), col("value")));` |
| `struct` | Column... cols | `Column` | 创建Struct | `df.withColumn("info", struct(col("name"), col("age")));` |
| `explode` | Column e | `Column` | 展开数组/Map为多行 | `df.select(col("id"), explode(col("tags")));` |
| `explode_outer` | Column e | `Column` | 展开数组/Map（保留null） | `df.select(col("id"), explode_outer(col("tags")));` |
| `posexplode` | Column e | `Column` | 展开数组并带位置 | `df.select(col("id"), posexplode(col("items")));` |
| `posexplode_outer` | Column e | `Column` | 展开数组带位置（保留null） | `df.select(col("id"), posexplode_outer(col("items")));` |
| `size` | Column e | `Column` | 数组/Map大小 | `df.withColumn("num_tags", size(col("tags")));` |
| `array_contains` | Column col, Object value | `Column` | 数组是否包含元素 | `df.filter(array_contains(col("tags"), "spark"));` |
| `sort_array` | Column e | `Column` | 数组排序（升序） | `df.withColumn("sorted", sort_array(col("arr")));` |
| `sort_array` | Column e, boolean asc | `Column` | 数组排序 | `df.withColumn("sorted", sort_array(col("arr"), false));` |
| `array_distinct` | Column e | `Column` | 数组去重 | `df.withColumn("unique", array_distinct(col("arr")));` |
| `array_intersect` | Column a1, Column a2 | `Column` | 数组交集 | `df.withColumn("common", array_intersect(col("arr1"), col("arr2")));` |
| `array_union` | Column a1, Column a2 | `Column` | 数组并集 | `df.withColumn("combined", array_union(col("arr1"), col("arr2")));` |
| `array_except` | Column a1, Column a2 | `Column` | 数组差集 | `df.withColumn("diff", array_except(col("arr1"), col("arr2")));` |
| `array_remove` | Column col, Object element | `Column` | 移除数组元素 | `df.withColumn("cleaned", array_remove(col("tags"), "old"));` |
| `array_position` | Column col, Object value | `Column` | 元素位置 | `df.withColumn("pos", array_position(col("arr"), "target"));` |
| `element_at` | Column col, Object extraction | `Column` | 获取数组/Map元素 | `df.withColumn("first", element_at(col("arr"), 1));` |
| `get_json_object` | Column e, String path | `Column` | 提取JSON字段 | `df.withColumn("name", get_json_object(col("json"), "$.name"));` |
| `json_tuple` | Column json, String... fields | `Column` | 提取多个JSON字段 | `df.select(json_tuple(col("json"), "name", "age"));` |
| `from_json` | Column col, Column schema | `Column` | JSON字符串转Struct | `df.withColumn("parsed", from_json(col("json_str"), schema));` |
| `to_json` | Column col | `Column` | Struct转JSON字符串 | `df.withColumn("json", to_json(col("struct_col")));` |
| `sha1` | Column e | `Column` | SHA1哈希 | `df.withColumn("hash", sha1(col("password")));` |
| `sha2` | Column e, int numBits | `Column` | SHA2哈希 | `df.withColumn("hash", sha2(col("password"), 256));` |
| `md5` | Column e | `Column` | MD5哈希 | `df.withColumn("hash", md5(col("content")));` |
| `crc32` | Column e | `Column` | CRC32哈希 | `df.withColumn("checksum", crc32(col("data")));` |
| `hash` | Column... cols | `Column` | 混合哈希 | `df.withColumn("hash", hash(col("id"), col("name")));` |
| `xxhash64` | Column... cols | `Column` | xxhash64哈希 | `df.withColumn("hash", xxhash64(col("id"), col("name")));` |
| `base64` | Column col | `Column` | Base64编码 | `df.withColumn("encoded", base64(col("data")));` |
| `unbase64` | Column col | `Column` | Base64解码 | `df.withColumn("decoded", unbase64(col("encoded")));` |
| `encode` | Column col, String charset | `Column` | 字符编码 | `df.withColumn("bytes", encode(col("text"), "UTF-8"));` |
| `decode` | Column col, String charset | `Column` | 字符解码 | `df.withColumn("text", decode(col("bytes"), "UTF-8"));` |
| `coalesce` | Column... e | `Column` | 返回第一个非null值 | `df.withColumn("name", coalesce(col("nickname"), col("fullname"), lit("N/A")));` |
| `ifnull` | Column col1, Column col2 | `Column` | 如果null返回第二个 | `df.withColumn("name", ifnull(col("name"), lit("Unknown")));` |
| `nullif` | Column col1, Column col2 | `Column` | 如果相等返回null | `df.withColumn("diff", nullif(col("a"), col("b")));` |
| `nvl` | Column col1, Column col2 | `Column` | NVL函数 | `df.withColumn("value", nvl(col("value"), lit(0)));` |
| `isnan` | Column e | `Column` | 判断是否NaN | `df.filter(isnan(col("score")));` |
| `nanvl` | Column col1, Column col2 | `Column` | 如果NaN返回第二个 | `df.withColumn("score", nanvl(col("score"), lit(0)));` |
| `monotonically_increasing_id` | 无 | `Column` | 生成单调递增ID | `df.withColumn("row_id", monotonically_increasing_id());` |
| `row_number` | 无 | `Column` | 行号（窗口函数） | `df.withColumn("row_num", row_number().over(Window.orderBy(col("id"))));` |
| `rank` | 无 | `Column` | 排名（有间隙） | `df.withColumn("rank", rank().over(Window.orderBy(col("score").desc())));` |
| `dense_rank` | 无 | `Column` | 排名（无间隙） | `df.withColumn("dense_rank", dense_rank().over(Window.orderBy(col("score").desc())));` |
| `percent_rank` | 无 | `Column` | 百分比排名 | `df.withColumn("pct", percent_rank().over(Window.orderBy(col("score"))));` |
| `lead` | Column e, int offset | `Column` | 向前N行 | `df.withColumn("next", lead(col("value"), 1).over(Window.orderBy(col("id"))));` |
| `lag` | Column e, int offset | `Column` | 向后N行 | `df.withColumn("prev", lag(col("value"), 1).over(Window.orderBy(col("id"))));` |
| `ntile` | int n | `Column` | 分桶 | `df.withColumn("bucket", ntile(4).over(Window.orderBy(col("score"))));` |
| `first_value` | Column e | `Column` | 窗口第一个值 | `df.withColumn("first", first_value(col("value")).over(Window.partitionBy("group")));` |
| `last_value` | Column e | `Column` | 窗口最后一个值 | `df.withColumn("last", last_value(col("value")).over(Window.partitionBy("group")));` |

| `count_distinct` | Column... cols | `Column` | 唯一值计数（别名） | `long count = df.select(count_distinct(col("id"))).first().getLong(0);` |
| `array_size` | Column array | `Column` | 数组大小（别名） | `Column size = array_size(col("items"));` |
| `array_sort` | Column array | `Column` | 数组排序 | `Column sorted = array_sort(col("items"));` |
| `map_contains_key` | Column map, Column key | `Column` | 判断Map是否包含key | `Column contains = map_contains_key(col("data"), lit("key1"));` |
| `map_keys` | Column map | `Column` | 获取Map的所有key | `Column keys = map_keys(col("data"));` |
| `map_values` | Column map | `Column` | 获取Map的所有value | `Column values = map_values(col("data"));` |
| `typedLit` | T value, Encoder[T] encoder | `Column` | 类型化字面值 | `Column typed = typedLit(Arrays.asList(1, 2, 3), Encoders.INT());` |
| `spark_partition_id` | 无 | `Column` | 获取分区ID | `df.select(spark_partition_id().as("partition")).show();` |
| `input_file_name` | 无 | `Column` | 获取输入文件名 | `df.select(input_file_name().as("file")).show();` |
| `input_file_block_start` | 无 | `Column` | 获取文件块起始位置 | `df.select(input_file_block_start().as("start")).show();` |
| `input_file_block_length` | 无 | `Column` | 获取文件块长度 | `df.select(input_file_block_length().as("length")).show();` |
| `trunc` | Column date, String format | `Column` | 截断日期 | `Column truncated = trunc(col("date"), "month");` |
| `date_trunc` | String format, Column timestamp | `Column` | 截断时间戳 | `Column truncated = date_trunc("hour", col("timestamp"));` |
| `expr` | String str | `Column` | 解析SQL表达式 | `Column result = expr("col1 + col2 * 10");` |
| `format_number` | Column x, int d | `Column` | 格式化数字 | `Column formatted = format_number(col("value"), 2);` |
| `format_string` | String format, Column... cols | `Column` | 格式化字符串 | `Column formatted = format_string("%s: %d", col("name"), col("value"));` |
| `regexp_count` | Column str, Column regexp | `Column` | 正则匹配计数 | `Column count = regexp_count(col("text"), lit("[0-9]+"));` |
| `regexp_instr` | Column str, Column regexp | `Column` | 正则匹配位置 | `Column pos = regexp_instr(col("text"), lit("[0-9]+"));` |
| `regexp_like` | Column str, Column regexp | `Column` | 正则判断是否匹配 | `Column matched = regexp_like(col("text"), lit("^[A-Z]"));` |
| `isnull` | Column col | `Column` | 判断是否null | `Column isNull = isnull(col("value"));` |
| `isnotnull` | Column col | `Column` | 判断是否非null | `Column notNull = isnotnull(col("value"));` |
| `nvl2` | Column col1, Column col2, Column col3 | `Column` | NVL2函数 | `Column result = nvl2(col("a"), col("b"), col("c"));` |
| `greatest` | Column... cols | `Column` | 取最大值 | `Column max = greatest(col("a"), col("b"), col("c"));` |
| `least` | Column... cols | `Column` | 取最小值 | `Column min = least(col("a"), col("b"), col("c"));` |
| `case_when` | Column... branches | `Column` | CASE WHEN表达式 | `Column result = case_when(col("a").equalTo(1), lit("one"), col("a").equalTo(2), lit("two"), lit("other"));` |

| `signum` | Column col | `Column` | 符号函数 | `Column sign = signum(col("value"));` |
| `atan2` | Column y, Column x | `Column` | 双参数反正切 | `Column angle = atan2(col("y"), col("x"));` |
| `bin` | Column col | `Column` | 转为二进制字符串 | `Column binary = bin(col("num"));` |
| `hex` | Column col | `Column` | 转为十六进制字符串 | `Column hexStr = hex(col("num"));` |
| `unhex` | Column col | `Column` | 解析十六进制字符串 | `Column bytes = unhex(col("hex_str"));` |
| `degrees` | Column col | `Column` | 弧度转角度 | `Column deg = degrees(col("radians"));` |
| `radians` | Column col | `Column` | 角度转弧度 | `Column rad = radians(col("degrees"));` |
| `pmod` | Column a, Column b | `Column` | 正模运算（总是正数） | `Column mod = pmod(col("a"), col("b"));` |
| `shiftleft` | Column col, int numBits | `Column` | 左移位 | `Column shifted = shiftleft(col("num"), 2);` |
| `shiftright` | Column col, int numBits | `Column` | 右移位 | `Column shifted = shiftright(col("num"), 2);` |
| `shiftRightUnsigned` | Column col, int numBits | `Column` | 无符号右移位 | `Column shifted = shiftRightUnsigned(col("num"), 2);` |

| `sequence` | Column start, Column end | `Column` | 生成序列数组 | `Column seq = sequence(lit(1), lit(10));` |
| `sequence` | Column start, Column end, Column step | `Column` | 生成序列数组（指定步长） | `Column seq = sequence(lit(1), lit(10), lit(2));` |
| `array_prepend` | Column array, Column element | `Column` | 数组前面添加元素 | `Column arr = array_prepend(col("items"), lit("first"));` |
| `array_append` | Column array, Column element | `Column` | 数组后面添加元素 | `Column arr = array_append(col("items"), lit("last"));` |
| `arrays_overlap` | Column a1, Column a2 | `Column` | 数组是否有重叠元素 | `Column overlap = arrays_overlap(col("a"), col("b"));` |
| `shuffle` | Column array | `Column` | 随机打乱数组 | `Column shuffled = shuffle(col("items"));` |
| `character_length` | Column str | `Column` | 字符串字符数（别名） | `Column len = character_length(col("text"));` |
| `char_length` | Column str | `Column` | 字符串字符数（别名） | `Column len = char_length(col("text"));` |
| `octet_length` | Column str | `Column` | 字符串字节长度 | `Column len = octet_length(col("text"));` |
| `bit_length` | Column str | `Column` | 字符串位长度 | `Column len = bit_length(col("text"));` |
| `bit_get` | Column col, int pos | `Column` | 获取指定位置的位值 | `Column bit = bit_get(col("value"), 0);` |
| `bit_count` | Column col | `Column` | 计算位的数量 | `Column count = bit_count(col("value"));` |
| `levenshtein` | Column left, Column right | `Column` | 计算编辑距离 | `Column dist = levenshtein(col("str1"), col("str2"));` |
| `substring_index` | Column str, String delim, int count | `Column` | 子字符串索引 | `Column sub = substring_index(col("url"), ".", 2);` |
| `left` | Column str, int len | `Column` | 取左边N个字符 | `Column leftStr = left(col("text"), 5);` |
| `right` | Column str, int len | `Column` | 取右边N个字符 | `Column rightStr = right(col("text"), 5);` |
| `btrim` | Column str | `Column` | 去除两端空白（别名） | `Column trimmed = btrim(col("text"));` |
| `conv` | Column num, int fromBase, int toBase | `Column` | 进制转换 | `Column hex = conv(col("num"), 10, 16);` |
| `typeof` | Column col | `Column` | 返回类型字符串 | `Column type = typeof(col("value"));` |
| `stack` | int n, Column... cols | `Column` | 将多列堆叠为多行 | `Column stacked = stack(3, col("a"), col("b"), col("c"));` |
| `assert_true` | Column condition | `Column` | 断言条件为真 | `assert_true(col("value").gt(0));` |
| `raise_error` | String message | `Column` | 抛出错误 | `raise_error("Custom error message");` |

| `repeat` | Column str, int n | `Column` | 重复字符串N次 | `Column repeated = repeat(col("text"), 3);` |
| `reverse` | Column str | `Column` | 反转字符串 | `Column reversed = reverse(col("text"));` |
| `element_at` | Column array, Column index | `Column` | 获取数组元素 | `Column elem = element_at(col("items"), lit(0));` |
| `array_except` | Column a1, Column a2 | `Column` | 数组差集 | `Column except = array_except(col("a"), col("b"));` |
| `array_intersect` | Column a1, Column a2 | `Column` | 数组交集 | `Column intersect = array_intersect(col("a"), col("b"));` |
| `array_union` | Column a1, Column a2 | `Column` | 数组并集 | `Column union = array_union(col("a"), col("b"));` |
| `array_remove` | Column array, Column element | `Column` | 移除数组元素 | `Column removed = array_remove(col("items"), lit("value"));` |

| `broadcast` | Column col | `Column` | 广播提示，优化join | `Dataset<Row> result = smallDF.join(bigDF, broadcast(smallDF.col("id")));` |
| `cbrt` | Column col | `Column` | 立方根 | `Column result = cbrt(col("value"));` |
| `ceiling` | Column col | `Column` | 向上取整（ceil别名） | `Column result = ceiling(col("value"));` |
| `chr` | int n | `Column` | ASCII码转字符 | `Column result = chr(65);  // 返回'A'` |
| `cosh` | Column col | `Column` | 双曲余弦 | `Column result = cosh(col("angle"));` |
| `cot` | Column col | `Column` | 余切 | `Column result = cot(col("angle"));` |
| `count_if` | Column condition | `Column` | 条件计数 | `Column count = count_if(col("value").gt(100));` |
| `convert_timezone` | String fromTz, String toTz, Column timestamp | `Column` | 时区转换 | `Column converted = convert_timezone("UTC", "Asia/Shanghai", col("timestamp"));` |
| `bitmap_construct_agg` | Column col | `Column` | 位图聚合 | `Column bitmap = bitmap_construct_agg(col("id"));` |
| `bitmap_count` | Column bitmap | `Column` | 位图计数 | `Column count = bitmap_count(col("bitmap"));` |
| `cardinality` | Column col | `Column` | 数组/Map大小 | `Column size = cardinality(col("items"));` |
| `covar_pop` | Column col1, Column col2 | `Column` | 总体协方差 | `Column cov = covar_pop(col("x"), col("y"));` |
| `covar_samp` | Column col1, Column col2 | `Column` | 样本协方差 | `Column cov = covar_samp(col("x"), col("y"));` |

| `call_udf` | String udfName, Column... cols | `Column` | 调用注册的UDF | `Column result = call_udf("my_udf", col("value"));` |
| `call_function` | String functionName, Column... cols | `Column` | 调用注册的函数 | `Column result = call_function("my_func", col("arg1"), col("arg2"));` |
| `forall` | Column array, Column predicate | `Column` | 判断数组所有元素是否满足条件 | `Column allPositive = forall(col("values"), x -> x.gt(0));` |
| `exists` | Column array, Column predicate | `Column` | 判断数组是否存在满足条件的元素 | `Column hasNegative = exists(col("values"), x -> x.lt(0));` |
| `zip_with` | Column left, Column right, BiFunction[Column, Column, Column] f | `Column` | 合并两个数组 | `Column zipped = zip_with(col("a"), col("b"), (x, y) -> x.plus(y));` |
| `inline` | Column array | `Column` | 展开数组中的struct为多列 | `df.select(inline(col("structs")));` |
| `inline_outer` | Column array | `Column` | 展开数组中的struct（含null） | `df.select(inline_outer(col("structs")));` |
| `array_min` | Column array | `Column` | 数组最小值 | `Column minVal = array_min(col("values"));` |
| `array_max` | Column array | `Column` | 数组最大值 | `Column maxVal = array_max(col("values"));` |

### Window
**包路径**: `org.apache.spark.sql.expressions`
**说明**: 窗口函数定义工具类，用于创建WindowSpec。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `partitionBy` | Column... columns | `WindowSpec` | 按列分区 | `WindowSpec window = Window.partitionBy(col("category"));` |
| `partitionBy` | String... colNames | `WindowSpec` | 按列名分区 | `WindowSpec window = Window.partitionBy("category", "region");` |
| `orderBy` | Column... columns | `WindowSpec` | 按列排序 | `WindowSpec window = Window.orderBy(col("date"));` |
| `orderBy` | String... colNames | `WindowSpec` | 按列名排序 | `WindowSpec window = Window.orderBy("date", "time");` |
| `rangeBetween` | long start, long end | `WindowSpec` | 范围窗口（基于值） | `WindowSpec window = Window.orderBy("value").rangeBetween(-10, 10);` |
| `rowsBetween` | long start, long end | `WindowSpec` | 行窗口（基于行数） | `WindowSpec window = Window.orderBy("value").rowsBetween(-3, 3);` |
| `unboundedPreceding` | 无 | `long` | 无界起始 | `Window.rowsBetween(Window.unboundedPreceding(), Window.currentRow());` |
| `unboundedFollowing` | 无 | `long` | 无界结束 | `Window.rowsBetween(Window.currentRow(), Window.unboundedFollowing());` |

---

### WindowSpec
**包路径**: `org.apache.spark.sql.expressions`
**说明**: 窗口规范，定义窗口函数的计算范围。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `partitionBy` | Column... columns | `WindowSpec` | 按列分区 | `WindowSpec window = Window.partitionBy("category").orderBy("date");` |
| `partitionBy` | String... colNames | `WindowSpec` | 按列名分区 | `WindowSpec window = spec.partitionBy("region");` |
| `orderBy` | Column... columns | `WindowSpec` | 按列排序 | `WindowSpec window = spec.orderBy(col("value"));` |
| `orderBy` | String... colNames | `WindowSpec` | 按列名排序 | `WindowSpec window = spec.orderBy("value");` |
| `rangeBetween` | long start, long end | `WindowSpec` | 范围窗口 | `WindowSpec window = spec.rangeBetween(-100, 100);` |
| `rowsBetween` | long start, long end | `WindowSpec` | 行窗口 | `WindowSpec window = spec.rowsBetween(-5, 5);` |

---

### DataFrameReader
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame读取器，用于从各种数据源读取数据。
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataFrameReader` | 指定数据源格式 | `spark.read().format("json").load("data.json");` |
| `option` | String key, String value | `DataFrameReader` | 设置选项（字符串） | `spark.read().option("header", "true").csv("data.csv");` |
| `option` | String key, boolean value | `DataFrameReader` | 设置选项（布尔） | `spark.read().option("multiline", true).json("data.json");` |
| `option` | String key, long value | `DataFrameReader` | 设置选项（长整数） | `spark.read().option("maxRowsPerFile", 10000L).format("csv");` |
| `options` | Map[String, String] options | `DataFrameReader` | 批量设置选项 | `Map<String, String> opts = new HashMap<>();<br>opts.put("header", "true");<br>spark.read().options(opts).csv("data.csv");` |
| `schema` | StructType schema | `DataFrameReader` | 指定schema | `StructType schema = DataTypes.createStructType(Arrays.asList(<br>    DataTypes.createStructField("id", DataTypes.IntegerType, true),<br>    DataTypes.createStructField("name", DataTypes.StringType, true)));<br>spark.read().schema(schema).csv("data.csv");` |
| `load` | 无 | `DataFrame` | 加载数据（用format指定格式） | `DataFrame df = spark.read().format("parquet").load("data.parquet");` |
| `load` | String path | `DataFrame` | 加载指定路径数据 | `DataFrame df = spark.read().format("json").load("data/*.json");` |
| `load` | String... paths | `DataFrame` | 加载多个路径数据 | `DataFrame df = spark.read().parquet("data1.parquet", "data2.parquet");` |
| `json` | String path | `DataFrame` | 读取JSON文件 | `DataFrame df = spark.read().json("data.json");` |
| `json` | Dataset[String] jsonDataset | `DataFrame` | 从Dataset读取JSON | `Dataset<String> jsonStrings = spark.createDataset(Arrays.asList("{"id":1}"), Encoders.STRING());<br>DataFrame df = spark.read().json(jsonStrings);` |
| `csv` | String path | `DataFrame` | 读取CSV文件 | `DataFrame df = spark.read().option("header", "true").csv("data.csv");` |
| `parquet` | String path | `DataFrame` | 读取Parquet文件 | `DataFrame df = spark.read().parquet("data.parquet");` |
| `orc` | String path | `DataFrame` | 读取ORC文件 | `DataFrame df = spark.read().orc("data.orc");` |
| `avro` | String path | `DataFrame` | 读取Avro文件 | `DataFrame df = spark.read().format("avro").load("data.avro");` |
| `text` | String path | `DataFrame` | 读取文本文件（每行一条记录） | `DataFrame df = spark.read().text("data.txt");` |
| `table` | String tableName | `DataFrame` | 从表读取数据 | `DataFrame df = spark.read().table("my_table");` |
| `jdbc` | String url, String table, Properties properties | `DataFrame` | 从JDBC读取数据 | `Properties props = new Properties();<br>props.put("user", "root");<br>props.put("password", "pwd");<br>DataFrame df = spark.read().jdbc("jdbc:mysql://localhost/db", "users", props);` |

### DataFrameWriter[T]
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame写入器，用于将数据写入各种数据源。
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataFrameWriter[T]` | 指定输出格式 | `df.write().format("parquet").save("output");` |
| `option` | String key, String value | `DataFrameWriter[T]` | 设置选项（字符串） | `df.write().option("header", "true").csv("output");` |
| `option` | String key, boolean value | `DataFrameWriter[T]` | 设置选项（布尔） | `df.write().option("compression", "snappy").parquet("output");` |
| `options` | Map[String, String] options | `DataFrameWriter[T]` | 批量设置选项 | `Map<String, String> opts = new HashMap<>();<br>opts.put("header", "true");<br>df.write().options(opts).csv("output");` |
| `mode` | SaveMode mode | `DataFrameWriter[T]` | 设置写入模式 | `df.write().mode(SaveMode.Append).parquet("output");` |
| `mode` | String mode | `DataFrameWriter[T]` | 设置写入模式字符串 | `df.write().mode("overwrite").parquet("output");  // overwrite/append/ignore/errorIfExists` |
| `partitionBy` | String... colNames | `DataFrameWriter[T]` | 按列分区存储 | `df.write().partitionBy("year", "month").parquet("output");` |
| `bucketBy` | int numBuckets, String colName, String... colNames | `DataFrameWriter[T]` | 分桶存储 | `df.write().bucketBy(100, "id").sortBy("timestamp").saveAsTable("bucketed_table");` |
| `sortBy` | String... colNames | `DataFrameWriter[T]` | 分桶内排序 | `df.write().bucketBy(100, "id").sortBy("name").saveAsTable("sorted_table");` |
| `save` | 无 | `Unit` | 保存数据（用format指定格式） | `df.write().format("parquet").save();` |
| `save` | String path | `Unit` | 保存到指定路径 | `df.write().parquet("output/data.parquet");` |
| `saveAsTable` | String tableName | `Unit` | 保存为表 | `df.write().saveAsTable("my_table");` |
| `insertInto` | String tableName | `Unit` | 插入到表（不创建新表） | `df.write().insertInto("existing_table");` |
| `json` | String path | `Unit` | 写入JSON文件 | `df.write().json("output/data.json");` |
| `csv` | String path | `Unit` | 写入CSV文件 | `df.write().option("header", "true").csv("output/data.csv");` |
| `parquet` | String path | `Unit` | 写入Parquet文件 | `df.write().parquet("output/data.parquet");` |
| `orc` | String path | `Unit` | 写入ORC文件 | `df.write().orc("output/data.orc");` |
| `avro` | String path | `Unit` | 写入Avro文件 | `df.write().format("avro").save("output/data.avro");` |
| `text` | String path | `Unit` | 写入文本文件 | `df.select(col("text_col")).write().text("output/data.txt");` |
| `jdbc` | String url, String table, Properties connectionProperties | `Unit` | 写入JDBC表 | `Properties props = new Properties();<br>props.put("user", "root");<br>props.put("password", "pwd");<br>df.write().jdbc("jdbc:mysql://localhost/db", "users", props);` |

| `clusterBy` | String... colNames | `DataFrameWriter[T]` | 按列聚类（Delta Lake） | `DataFrameWriter<Row> writer = df.write().clusterBy("id", "date");` |

### DataStreamReader
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming数据流读取器，从SparkSession.readStream()获取。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataStreamReader` | 设置数据源格式 | `reader.format("kafka");` |
| `option` | String key, String value | `DataStreamReader` | 设置选项 | `reader.option("kafka.bootstrap.servers", "localhost:9092");` |
| `option` | String key, boolean value | `DataStreamReader` | 设置布尔选项 | `reader.option("startingOffsets", "earliest");` |
| `options` | Map<String, String> options | `DataStreamReader` | 设置多个选项 | `reader.options(kafkaParams);` |
| `schema` | StructType schema | `DataStreamReader` | 设置schema（自定义格式） | `reader.schema(schema);` |
| `load` | 无 | `Dataset[Row]` | 加载流数据 | `Dataset<Row> kafkaStream = reader.load();` |
| `load` | String path | `Dataset[Row]` | 加载流数据（指定路径） | `Dataset<Row> jsonStream = reader.load("hdfs://stream/");` |
| `table` | String tableName | `Dataset[Row]` | 从表读取流数据 | `Dataset<Row> tableStream = reader.table("stream_table");` |
| `json` | String path | `Dataset[Row]` | JSON格式流数据 | `Dataset<Row> jsonStream = spark.readStream().json("hdfs://stream/");` |
| `csv` | String path | `Dataset[Row]` | CSV格式流数据 | `Dataset<Row> csvStream = spark.readStream().csv("hdfs://stream/");` |

---

### DataStreamWriter[T]
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming数据流写入器，从Dataset.writeStream()获取。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataStreamWriter[T]` | 设置输出格式 | `writer.format("console");` |
| `outputMode` | String outputMode | `DataStreamWriter[T]` | 设置输出模式 | `writer.outputMode("append");` |
| `option` | String key, String value | `DataStreamWriter[T]` | 设置选项 | `writer.option("checkpointLocation", "hdfs://checkpoint/");` |
| `option` | String key, boolean value | `DataStreamWriter[T]` | 设置布尔选项 | `writer.option("truncate", false);` |
| `options` | Map<String, String> options | `DataStreamWriter[T]` | 设置多个选项 | `writer.options(outputParams);` |
| `partitionBy` | String... colNames | `DataStreamWriter[T]` | 按列分区 | `writer.partitionBy("date");` |
| `foreach` | ForeachWriter[T] writer | `DataStreamWriter[T]` | 自定义foreach输出 | `writer.foreach(new MyForeachWriter());` |
| `foreachBatch` | VoidFunction2[Dataset[T], Long] function | `DataStreamWriter[T]` | 批次处理函数 | `writer.foreachBatch((batch, batchId) -> { batch.write().parquet("hdfs://output/" + batchId); });` |
| `trigger` | Trigger trigger | `DataStreamWriter[T]` | 设置触发器 | `writer.trigger(Trigger.ProcessingTime("5 seconds"));` |
| `start` | 无 | `StreamingQuery` | 启动流查询 | `StreamingQuery query = writer.start();` |

---

### StreamingQuery
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming查询对象，用于监控和管理流查询。
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 获取查询名称 | `String name = query.name();` |
| `id` | 无 | `long` | 获取查询ID | `long id = query.id();` |
| `runId` | 无 | `long` | 获取运行ID | `long runId = query.runId();` |
| `isActive` | 无 | `boolean` | 是否活跃 | `boolean active = query.isActive();` |
| `status` | 无 | `StreamingQueryStatus` | 获取状态 | `StreamingQueryStatus status = query.status();` |
| `lastProgress` | 无 | `StreamingQueryProgress` | 获取最新进度 | `StreamingQueryProgress progress = query.lastProgress();` |
| `recentProgress` | 无 | `StreamingQueryProgress[]` | 获取最近进度列表 | `StreamingQueryProgress[] progress = query.recentProgress();` |
| `awaitTermination` | 无 | `Unit` | 等待终止 | `query.awaitTermination();` |
| `awaitTermination` | long timeoutMs | `boolean` | 等待终止或超时 | `boolean terminated = query.awaitTermination(60000);` |
| `stop` | 无 | `Unit` | 停止查询 | `query.stop();` |
| `exception` | 无 | `Option[StreamingQueryException]` | 获取异常 | `Optional<StreamingQueryException> ex = query.exception();` |
| `explain` | boolean extended | `String` | 解释执行计划 | `String plan = query.explain(true);` |
| `sinkStatus` | 无 | `SinkStatus` | 获取sink状态 | `SinkStatus sink = query.sinkStatus();` |
| `sourceStatus` | int index | `SourceStatus` | 获取source状态 | `SourceStatus source = query.sourceStatus(0);` |

---

### StreamingQueryListener
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming查询监听器，监控查询生命周期事件。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onQueryStarted` | QueryStartedEvent event | `void` | 查询启动事件 | `public void onQueryStarted(QueryStartedEvent event) { System.out.println("Query started: " + event.id()); }` |
| `onQueryProgress` | QueryProgressEvent event | `void` | 查询进度事件 | `public void onQueryProgress(QueryProgressEvent event) { System.out.println("Progress: " + event.progress().numInputRows()); }` |
| `onQueryIdle` | QueryIdleEvent event | `void` | 查询空闲事件 | `public void onQueryIdle(QueryIdleEvent event) { System.out.println("Query idle"); }` |
| `onQueryTerminated` | QueryTerminatedEvent event | `void` | 查询终止事件 | `public void onQueryTerminated(QueryTerminatedEvent event) { System.out.println("Query terminated: " + event.id()); }` |
| `onQueryFailure` | QueryFailureEvent event | `void` | 查询失败事件 | `public void onQueryFailure(QueryFailureEvent event) { System.out.println("Query failed: " + event.exception()); }` |

---

### StreamingQueryStatus
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming查询状态信息。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 查询名称 | `String name = status.name();` |
| `isDataAvailable` | 无 | `boolean` | 是否有数据可用 | `boolean hasData = status.isDataAvailable();` |
| `isTriggerActive` | 无 | `boolean` | 触发器是否活跃 | `boolean active = status.isTriggerActive();` |
| `timestamp` | 无 | `long` | 时间戳 | `long ts = status.timestamp();` |
| `json` | 无 | `String` | JSON表示 | `String json = status.json();` |

---

### ForeachWriter[T]
**包路径**: `org.apache.spark.sql`
**说明**: Structured Streaming自定义输出写入器。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `open` | long partitionId, long epochId | `boolean` | 打开写入器，返回false则跳过 | `public boolean open(long partitionId, long epochId) { connection = createConnection(); return true; }` |
| `process` | T value | `void` | 处理单条数据 | `public void process(String value) { connection.write(value); }` |
| `close` | Throwable errorOrNull | `void` | 关闭写入器 | `public void close(Throwable error) { connection.close(); }` |

---

### Catalog
**包路径**: `org.apache.spark.sql.catalog`
**说明**: Spark Catalog接口，用于管理数据库、表、函数等元数据。
**方法数量**: 20

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `currentDatabase` | 无 | `String` | 获取当前数据库 | `String db = spark.catalog().currentDatabase();` |
| `setCurrentDatabase` | String db | `Unit` | 设置当前数据库 | `spark.catalog().setCurrentDatabase("my_db");` |
| `listDatabases` | 无 | `Dataset[Database]` | 列出所有数据库 | `spark.catalog().listDatabases().show();` |
| `listTables` | 无 | `Dataset[Table]` | 列出当前数据库的所有表 | `spark.catalog().listTables().show();` |
| `listTables` | String dbName | `Dataset[Table]` | 列出指定数据库的所有表 | `spark.catalog().listTables("my_db").show();` |
| `listFunctions` | 无 | `Dataset[Function]` | 列出所有函数 | `spark.catalog().listFunctions().show();` |
| `listFunctions` | String dbName | `Dataset[Function]` | 列出指定数据库的函数 | `spark.catalog().listFunctions("my_db").show();` |
| `listColumns` | String tableName | `Dataset[Column]` | 列出表的所有列 | `spark.catalog().listColumns("my_table").show();` |
| `listColumns` | String dbName, String tableName | `Dataset[Column]` | 列出指定数据库表的列 | `spark.catalog().listColumns("my_db", "my_table").show();` |
| `getTable` | String dbName, String tableName | `Table` | 获取表详情 | `Table table = spark.catalog().getTable("my_db", "my_table");` |
| `getTable` | String tableName | `Table` | 获取当前数据库的表 | `Table table = spark.catalog().getTable("my_table");` |
| `databaseExists` | String dbName | `Boolean` | 检查数据库是否存在 | `boolean exists = spark.catalog().databaseExists("my_db");` |
| `tableExists` | String tableName | `Boolean` | 检查表是否存在（当前库） | `boolean exists = spark.catalog().tableExists("my_table");` |
| `tableExists` | String dbName, String tableName | `Boolean` | 检查指定库表是否存在 | `boolean exists = spark.catalog().tableExists("my_db", "my_table");` |
| `functionExists` | String functionName | `Boolean` | 检查函数是否存在 | `boolean exists = spark.catalog().functionExists("my_func");` |
| `functionExists` | String dbName, String functionName | `Boolean` | 检查指定库函数是否存在 | `boolean exists = spark.catalog().functionExists("my_db", "my_func");` |
| `createDatabase` | String dbName, boolean ignoreIfExists | `Unit` | 创建数据库 | `spark.catalog().createDatabase("new_db", true);` |
| `createDatabase` | String dbName, boolean ignoreIfExists, String comment | `Unit` | 创建数据库（带注释） | `spark.catalog().createDatabase("new_db", false, "My test database");` |
| `dropDatabase` | String dbName, boolean ignoreIfNotExists, boolean cascade | `Unit` | 删除数据库 | `spark.catalog().dropDatabase("old_db", true, false);` |
| `createTable` | String tableName, String path | `Unit` | 创建表（指定路径） | `spark.catalog().createTable("new_table", "hdfs://data/path");` |
| `createTable` | String tableName, String path, String source | `Unit` | 创建表（指定格式） | `spark.catalog().createTable("new_table", "hdfs://data", "parquet");` |
| `createExternalTable` | String tableName, String path | `DataFrame` | 创建外部表 | `DataFrame df = spark.catalog().createExternalTable("ext_table", "hdfs://data");` |
| `createExternalTable` | String tableName, String path, String source | `DataFrame` | 创建外部表（指定格式） | `DataFrame df = spark.catalog().createExternalTable("ext_table", "hdfs://data", "parquet");` |
| `dropTable` | String dbName, String tableName, boolean ignoreIfNotExists, boolean purge | `Unit` | 删除表 | `spark.catalog().dropTable("my_db", "old_table", true, false);` |
| `dropTable` | String tableName, boolean ignoreIfNotExists, boolean purge | `Unit` | 删除当前库表 | `spark.catalog().dropTable("old_table", true, false);` |
| `dropTempView` | String viewName | `Unit` | 删除临时视图 | `spark.catalog().dropTempView("temp_view");` |
| `dropGlobalTempView` | String viewName | `Unit` | 删除全局临时视图 | `spark.catalog().dropGlobalTempView("global_view");` |
| `recoverPartitions` | String tableName | `Unit` | 恢复分区信息 | `spark.catalog().recoverPartitions("partitioned_table");` |
| `refreshTable` | String tableName | `Unit` | 刷新表缓存 | `spark.catalog().refreshTable("my_table");` |
| `refreshByPath` | String path | `Unit` | 刷新指定路径缓存 | `spark.catalog().refreshByPath("hdfs://data/table");` |
| `clearCache` | 无 | `Unit` | 清除所有缓存 | `spark.catalog().clearCache();` |
| `isCached` | String tableName | `Boolean` | 检查表是否被缓存 | `boolean cached = spark.catalog().isCached("my_table");` |
| `cacheTable` | String tableName | `Unit` | 缓存表 | `spark.catalog().cacheTable("my_table");` |
| `uncacheTable` | String tableName | `Unit` | 取消缓存表 | `spark.catalog().uncacheTable("my_table");` |

### UDFRegistration
**包路径**: `org.apache.spark.sql`
**说明**: UDF注册接口，用于注册用户自定义函数。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `register` | String name, UDF1[T1, R] f, DataType returnType | `void` | 注册UDF（1个参数） | `spark.udf().register("myUpper", (String s) -> s.toUpperCase(), DataTypes.StringType);` |
| `register` | String name, UDF2[T1, T2, R] f, DataType returnType | `void` | 注册UDF（2个参数） | `spark.udf().register("concat2", (String a, String b) -> a + b, DataTypes.StringType);` |
| `register` | String name, UDF3[T1, T2, T3, R] f, DataType returnType | `void` | 注册UDF（3个参数） | `spark.udf().register("combine3", (String a, String b, String c) -> a+b+c, DataTypes.StringType);` |
| `register` | String name, UDF4[T1, T2, T3, T4, R] f, DataType returnType | `void` | 注册UDF（4个参数） | `spark.udf().register("myUDF4", new MyUDF4(), DataTypes.IntegerType);` |
| `register` | String name, UDF5... | `void` | 注册UDF（5+参数） | `spark.udf().register("myUDF5", new MyUDF5(), DataTypes.StringType);` |
| `register` | String name, UDAF udaf | `void` | 注册聚合UDF | `spark.udf().register("mySum", new MySumUDAF());` |
| `register` | String name, UserDefinedAggregateFunction udaf | `void` | 注册聚合UDF（旧API） | `spark.udf().register("myUDAF", new MyUDAF());` |
| `registerJava` | String name, String className, DataType returnType | `void` | 注册Java UDF类 | `spark.udf().registerJava("myFunc", "com.example.MyUDF", DataTypes.StringType);` |
| `registerPython` | String name, String command, DataType returnType | `void` | 注册Python UDF | `spark.udf().registerPython("pyFunc", "python_code", DataTypes.StringType);` |
| `callUDF` | String udfName, Column... cols | `Column` | 调用已注册的UDF | `df.select(callUDF("myUpper", col("name")));` |

---

## Streaming流处理API

### JavaStreamingContext
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Spark Streaming的Java入口，用于创建DStream和处理实时数据流。
**方法数量**: 25

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `JavaStreamingContext` | SparkConf conf, Duration batchDuration | 构造方法 | 创建StreamingContext | `SparkConf conf = new SparkConf().setAppName("Streaming");<br>JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));` |
| `JavaStreamingContext` | JavaSparkContext sparkContext, Duration batchDuration | 构造方法 | 从JavaSparkContext创建 | `JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));` |
| `textFileStream` | String directory | `JavaDStream[String]` | 监控目录中的新文本文件 | `JavaDStream<String> lines = jssc.textFileStream("hdfs://logs/");` |
| `fileStream` | String directory, Class[K] keyClass, Class[V] valueClass, Class[F] inputFormatClass | `JavaPairDStream[K, V]` | 监控目录中的新文件（指定格式） | `JavaPairDStream<Text, IntWritable> files = jssc.fileStream("hdfs://input/", Text.class, IntWritable.class, TextInputFormat.class);` |
| `socketTextStream` | String hostname, int port | `JavaDStream[String]` | 从TCP socket读取文本流 | `JavaDStream<String> socketStream = jssc.socketTextStream("localhost", 9999);` |
| `socketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 从socket读取，指定存储级别 | `JavaReceiverInputDStream<String> stream = jssc.socketStream("localhost", 9999, StorageLevel.MEMORY_ONLY());` |
| `rawSocketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 原始socket流 | `JavaReceiverInputDStream<String> stream = jssc.rawSocketStream("localhost", 9999, StorageLevel.MEMORY_ONLY());` |
| `kafkaStream` | Map[String, String] kafkaParams, Map[String, Integer] topics | `JavaPairDStream[String, String]` | 从Kafka读取流 | `Map<String, String> kafkaParams = new HashMap<>();<br>kafkaParams.put("bootstrap.servers", "localhost:9092");<br>Map<String, Integer> topics = new HashMap<>();<br>topics.put("my_topic", 1);<br>JavaPairDStream<String, String> kafkaStream = jssc.kafkaStream(kafkaParams, topics);` |
| `flumeStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[SparkFlumeEvent]` | 从Flume读取流 | `JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = jssc.flumeStream("localhost", 41414, StorageLevel.MEMORY_ONLY());` |
| `queueStream` | Queue[JavaRDD[T]] rdds | `JavaInputDStream[T]` | 从RDD队列创建测试流 | `Queue<JavaRDD<String>> queue = new LinkedList<>();<br>queue.add(sc.parallelize(Arrays.asList("a", "b")));<br>JavaInputDStream<String> testStream = jssc.queueStream(queue);` |
| `queueStream` | Queue[JavaRDD[T]] rdds, boolean oneAtATime | `JavaInputDStream[T]` | 逐个RDD处理 | `JavaInputDStream<String> stream = jssc.queueStream(queue, true);` |
| `union` | JavaDStream[T]... streams | `JavaDStream[T]` | 合合多个DStream | `JavaDStream<String> combined = jssc.union(stream1, stream2);` |
| `transform` | JavaDStream[T] dstream, JFunction[JavaRDD[T], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对DStream每个RDD应用变换 | `JavaDStream<String> transformed = dstream.transform(rdd -> rdd.filter(s -> s.length() > 3));` |
| `transformWith` | JavaDStream[T] dstream1, JavaDStream[W] dstream2, JFunction2[JavaRDD[T], JavaRDD[W], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对两个DStream每个RDD应用变换 | `JavaDStream<String> result = dstream1.transformWith(dstream2, (rdd1, rdd2) -> rdd1.union(rdd2));` |
| `checkpoint` | String directory | `Unit` | 设置checkpoint目录 | `jssc.checkpoint("hdfs://checkpoint/streaming/");` |
| `start` | 无 | `Unit` | 启动Streaming | `jssc.start();` |
| `awaitTermination` | 无 | `Unit` | 阻塞等待终止 | `jssc.awaitTermination();` |
| `awaitTerminationOrTimeout` | long timeout | `Unit` | 阻塞等待终止或超时 | `jssc.awaitTerminationOrTimeout(60000L);  // 最多等待60秒` |
| `stop` | 无 | `Unit` | 停止Streaming | `jssc.stop();` |
| `stop` | boolean stopSparkContext | `Unit` | 停止Streaming，控制是否停SparkContext | `jssc.stop(false);  // 停止Streaming但保留SparkContext` |
| `stop` | boolean stopSparkContext, boolean stopGracefully | `Unit` | 停止Streaming，控制优雅停止 | `jssc.stop(true, true);  // 优雅停止处理中的数据` |
| `close` | 无 | `Unit` | 关闭（Java友好） | `jssc.close();` |
| `sparkContext` | 无 | `JavaSparkContext` | 获取底层JavaSparkContext | `JavaSparkContext sc = jssc.sparkContext();` |
| `ssc` | 无 | `StreamingContext` | 获取底层Scala StreamingContext | `StreamingContext ssc = jssc.ssc();` |
| `remember` | Duration duration | `Unit` | 设置DStream数据保留时间 | `jssc.remember(Durations.minutes(30));  // 保留30分钟数据用于状态更新` |
| `addStreamingListener` | StreamingListener listener | `Unit` | 添加流处理监听器，监控批次事件 | `jssc.addStreamingListener(new MyStreamingListener());  // 监听批次开始、完成、错误` |
| `binaryRecordsStream` | String directory, int recordLength | `JavaDStream[byte[]]` | 监控目录中的固定长度二进制文件流 | `JavaDStream<byte[]> stream = jssc.binaryRecordsStream("hdfs://data/", 100);  // 每条记录100字节` |
| `receiverStream` | JavaReceiverInputDStream[T] receiver | `JavaDStream[T]` | 使用自定义Receiver创建DStream | `JavaDStream<String> customStream = jssc.receiverStream(new MyCustomReceiver());  // 自定义数据源` |

| `removeStreamingListener` | StreamingListener listener | `Unit` | 移除流处理监听器 | `jssc.removeStreamingListener(listener);` |
| `getActiveContexts` | 无 | `List[StreamingContext]` | 获取所有活动的StreamingContext | `List<StreamingContext> contexts = StreamingContext.getActiveContexts();` |

### JavaDStream[T]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Java版本的DStream（离散化流），代表连续的RDD序列。
**方法数量**: 30

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `map` | JFunction[T, U] f | `JavaDStream[U]` | 对每个元素映射 | `JavaDStream<Integer> lengths = lines.map(s -> s.length());` |
| `flatMap` | FlatMapFunction[T, U] f | `JavaDStream[U]` | 对每个元素映射为多个输出 | `JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());` |
| `filter` | JFunction[T, Boolean] f | `JavaDStream[T]` | 过滤元素 | `JavaDStream<String> filtered = lines.filter(s -> s.length() > 3);` |
| `mapToPair` | PairFunction[T, K, V] f | `JavaPairDStream[K, V]` | 映射为键值对 | `JavaPairDStream<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));` |
| `reduce` | JFunction2[T, T, T] f | `JavaDStream[T]` | 对每个RDD内元素聚合 | `JavaDStream<Integer> sums = numbers.reduce((a, b) -> a + b);` |
| `count` | 无 | `JavaDStream[Long]` | 对每个RDD计数 | `JavaDStream<Long> counts = dstream.count();` |
| `countByValue` | 无 | `JavaPairDStream[T, Long]` | 对每个RDD统计每个值的出现次数 | `JavaPairDStream<String, Long> wordCounts = words.countByValue();` |
| `reduceByKey` | JFunction2[V, V, V] func | `JavaPairDStream[K, V]` | 按Key聚合 | `JavaPairDStream<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);` |
| `groupByKey` | 无 | `JavaPairDStream[K, JIterable[V]]` | 按Key分组 | `JavaPairDStream<String, Iterable<Integer>> grouped = pairs.groupByKey();` |
| `mapValues` | JFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value映射 | `JavaPairDStream<String, String> transformed = pairs.mapValues(v -> "value:" + v);` |
| `flatMapValues` | FlatMapFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value扁平映射 | `JavaPairDStream<String, Integer> result = pairDStream.flatMapValues(v -> Arrays.asList(v, v*2).iterator());` |
| `foreachRDD` | VoidFunction[JavaRDD[T]] foreachFunc | `Unit` | 对每个RDD执行操作 | `wordCounts.foreachRDD(rdd -> {<br>    rdd.foreach(pair -> System.out.println(pair._1() + ": " + pair._2()));<br>});` |
| `transform` | JFunction[JavaRDD[T], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对每个RDD变换 | `JavaDStream<String> transformed = dstream.transform(rdd -> rdd.distinct());` |
| `transformToPair` | JFunction[JavaRDD[T], JavaPairRDD[K, V]] transformFunc | `JavaPairDStream[K, V]` | 对每个RDD变换为PairRDD | `JavaPairDStream<String, Integer> result = dstream.transformToPair(rdd -> rdd.mapToPair(x -> new Tuple2<>(x, 1)));` |
| `union` | JavaDStream[T] other | `JavaDStream[T]` | 合合DStream | `JavaDStream<String> merged = stream1.union(stream2);` |
| `glom` | 无 | `JavaDStream[JList[T]]` | 将每个RDD分区合并为List | `JavaDStream<List<String>> partitioned = dstream.glom();` |
| `slice` | Duration fromTime, Duration toTime | `List[JavaRDD[T]]` | 获取时间范围内的RDD列表 | `List<JavaRDD<String>> rdds = dstream.slice(Durations.seconds(10), Durations.seconds(20));` |
| `window` | Duration windowDuration | `JavaDStream[T]` | 窗口操作 | `JavaDStream<String> windowed = dstream.window(Durations.seconds(30));  // 30秒窗口` |
| `window` | Duration windowDuration, Duration slideDuration | `JavaDStream[T]` | 窗口操作，指定滑动间隔 | `JavaDStream<String> windowed = dstream.window(Durations.seconds(30), Durations.seconds(10));  // 30秒窗口，每10秒滑动` |
| `reduceByWindow` | JFunction2[T, T, T] reduceFunc, Duration windowDuration, Duration slideDuration | `JavaDStream[T]` | 窗口聚合 | `JavaDStream<Integer> windowSum = numbers.reduceByWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(10));` |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合 | `JavaPairDStream<String, Integer> windowCounts = pairs.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30));` |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合，指定滑动 | `JavaPairDStream<String, Integer> result = pairDStream.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(10), Durations.seconds(2));` |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, JFunction2[V, V, V] invReduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合（带逆函数，高效） | `JavaPairDStream<String, Integer> counts = pairs.reduceByKeyAndWindow(<br>    (a, b) -> a + b,  // 加新数据<br>    (a, b) -> a - b,  // 减旧数据（高效计算）<br>    Durations.seconds(30), Durations.seconds(10));` |
| `countByWindow` | Duration windowDuration, Duration slideDuration | `JavaDStream[Long]` | 窗口内计数 | `JavaDStream<Long> counts = dstream.countByWindow(Durations.seconds(30), Durations.seconds(10));` |
| `countByValueAndWindow` | Duration windowDuration, Duration slideDuration | `JavaPairDStream[T, Long]` | 窗口内按值计数 | `JavaPairDStream<String, Long> result = dstream.countByValueAndWindow(Durations.seconds(10), Durations.seconds(2));` |
| `checkpoint` | 无 | `JavaDStream[T]` | 启用checkpoint | `dstream.checkpoint();` |
| `persist` | StorageLevel level | `JavaDStream[T]` | 持久化DStream | `dstream.persist(StorageLevel.MEMORY_ONLY());` |
| `cache` | 无 | `JavaDStream[T]` | 缓存DStream | `dstream.cache();` |
| `print` | 无 | `Unit` | 打印每个RDD的前10元素 | `dstream.print();` |
| `saveAsTextFiles` | String prefix, String suffix | `Unit` | 保存为文本文件序列 | `dstream.saveAsTextFiles("output/stream", "txt");  // 生成output/stream-TIME.txt` |
| `saveAsObjectFiles` | String prefix, String suffix | `Unit` | 保存为对象文件序列 | `dstream.saveAsObjectFiles("output/stream", "obj");` |

### JavaInputDStream[T]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Java版本的InputDStream，是JavaReceiverInputDStream的父类。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `start` | 无 | `Unit` | 启动接收器 | `inputDStream.start();` |
| `stop` | 无 | `Unit` | 停止接收器 | `inputDStream.stop();` |
| `compute` | Time validTime | `Option[RDD[T]]` | 计算指定时间的RDD | `Option<JavaRDD<String>> rdd = inputDStream.compute(time);` |
| `isInitialized` | 无 | `boolean` | 是否已初始化 | `boolean init = inputDStream.isInitialized();` |
| `slideDuration` | 无 | `Duration` | 获取滑动间隔 | `Duration duration = inputDStream.slideDuration();` |

---

### JavaReceiverInputDStream[T]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Java版本的ReceiverInputDStream，用于自定义数据接收器。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `start` | 无 | `Unit` | 启动接收器 | `receiverInputDStream.start();` |
| `stop` | 无 | `Unit` | 停止接收器 | `receiverInputDStream.stop();` |
| `receiver` | 无 | `Receiver[T]` | 获取底层Receiver | `Receiver<String> receiver = receiverInputDStream.receiver();` |
| `compute` | Time validTime | `Option[RDD[T]]` | 计算指定时间的RDD | `Option<JavaRDD<String>> rdd = receiverInputDStream.compute(time);` |
| `isInitialized` | 无 | `boolean` | 是否已初始化 | `boolean init = receiverInputDStream.isInitialized();` |
| `slideDuration` | 无 | `Duration` | 获取滑动间隔 | `Duration duration = receiverInputDStream.slideDuration();` |
| `storageLevel` | 无 | `StorageLevel` | 获取存储级别 | `StorageLevel level = receiverInputDStream.storageLevel();` |
| `repartition` | int numPartitions | `JavaDStream[T]` | 重新分区 | `JavaDStream<String> repartitioned = receiverInputDStream.repartition(4);` |

---

### JavaPairDStream[K, V]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: 键值对版本的DStream，继承JavaDStream并添加键值对操作。
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `keys` | 无 | `JavaDStream[K]` | 获取所有Key的DStream | `JavaDStream<String> keys = pairs.keys();` |
| `values` | 无 | `JavaDStream[V]` | 获取所有Value的DStream | `JavaDStream<Integer> values = pairs.values();` |
| `join` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[V, W]]` | 内连接 | `JavaPairDStream<String, Tuple2<Integer, String>> joined = pairs.join(otherPairs);` |
| `join` | JavaPairDStream[K, W] other, Duration windowDuration | `JavaPairDStream[K, Tuple2[V, W]]` | 窗口内连接 | `JavaPairDStream<String, Tuple2<Integer, String>> joined = pairs.join(otherPairs, Durations.seconds(30));` |
| `leftOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[V, Optional[W]]]` | 左外连接 | `JavaPairDStream<String, Tuple2<Integer, Optional<String>>> joined = pairs.leftOuterJoin(otherPairs);` |
| `rightOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[Optional[V], W]]` | 右外连接 | `JavaPairDStream<String, Tuple2<Optional<Integer>, String>> joined = pairs.rightOuterJoin(otherPairs);` |
| `fullOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[Optional[V], Optional[W]]]` | 全外连接 | `JavaPairDStream<String, Tuple2<Optional<Integer>, Optional<Integer>>> result = pairDStream1.fullOuterJoin(pairDStream2);` |
| `cogroup` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[JIterable[V], JIterable[W]]]` | 共同分组 | `JavaPairDStream<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> result = pairDStream1.cogroup(pairDStream2);` |
| `updateStateByKey` | JFunction2[JList[V], Optional[S], Optional[S]] updateFunc | `JavaPairDStream[K, S]` | 更新状态（带状态计算） | `JavaPairDStream<String, Integer> stateCounts = wordCounts.updateStateByKey((values, state) -> {<br>    int sum = state.orElse(0);<br>    for (int v : values) sum += v;<br>    return Optional.of(sum);<br>});` |
| `mapWithState` | StateSpec[K, V, S, M] spec | `JavaMapWithStateDStream[K, V, S, M]` | 高效状态更新 | `JavaMapWithStateDStream<String, Integer, Integer, String> stateStream = pairDStream.mapWithState(StateSpec.function(stateFunc));` |

| `compute` | Time time | `Option[RDD[(K, V)]]` | 计算指定时间的RDD | `Option<RDD<Tuple2<String, Integer>>> rdd = pairDStream.compute(time);` |
| `fromJavaDStream` | JavaDStream[(K, V)] dstream | `JavaPairDStream[K, V]` | 从JavaDStream创建 | `JavaPairDStream<String, Integer> pair = JavaPairDStream.fromJavaDStream(dstream);` |
| `groupByKeyAndWindow` | Duration windowDuration | `JavaPairDStream[K, JIterable[V]]` | 按窗口分组 | `JavaPairDStream<String, Iterable<Integer>> grouped = pairDStream.groupByKeyAndWindow(Durations.seconds(10));` |
| `groupByKeyAndWindow` | Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, JIterable[V]]` | 按窗口分组（指定滑动间隔） | `JavaPairDStream<String, Iterable<Integer>> grouped = pairDStream.groupByKeyAndWindow(Durations.seconds(10), Durations.seconds(2));` |
| `saveAsHadoopFiles` | String prefix, String suffix | `Unit` | 保存为Hadoop文件 | `pairDStream.saveAsHadoopFiles("hdfs://output/", "txt");` |
| `saveAsNewAPIHadoopFiles` | String prefix, String suffix | `Unit` | 保存为新API Hadoop文件 | `pairDStream.saveAsNewAPIHadoopFiles("hdfs://output/", "txt");` |
| `toJavaDStream` | 无 | `JavaDStream[(K, V)]` | 转为JavaDStream | `JavaDStream<Tuple2<String, Integer>> dstream = pairDStream.toJavaDStream();` |

---

## MLlib机器学习算法API

### KMeans / KMeansModel
**包路径**: `org.apache.spark.mllib.clustering`
**说明**: K-Means聚类算法和模型。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations | `KMeansModel` | 训练K-Means模型 | `JavaRDD<Vector> data = vectorsRDD;<br>KMeansModel model = KMeans.train(data.rdd(), 3, 20);  // 3个簇，20次迭代` |
| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs | `KMeansModel` | 训练模型，多次运行 | `KMeansModel model = KMeans.train(data, 10, 20, 1);` |
| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs, String initializationMode | `KMeansModel` | 指定初始化模式 | `KMeansModel model = KMeans.train(data.rdd(), 3, 20, 1, "k-means||");` |
| `predict` | Vector point | `Int` | 预测单个点的簇归属 | `int cluster = model.predict(vector);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Integer]` | 预测多个点的簇归属 | `JavaRDD<Integer> predictions = model.predict(data);` |
| `clusterCenters` | 无 | `Vector[]` | 获取所有簇中心 | `Vector[] centers = model.clusterCenters();` |
| `k` | 无 | `Int` | 获取簇数量 | `int k = model.k();` |
| `computeCost` | JavaRDD[Vector] data | `Double` | 计算聚类成本（误差平方和） | `double cost = model.computeCost(data.rdd());` |

### BisectingKMeans / BisectingKMeansModel
**包路径**: `org.apache.spark.mllib.clustering`
**说明**: 二分K-Means聚类，层次聚类算法。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setK` | int k | `BisectingKMeans` | 设置目标簇数量 | `BisectingKMeans bkm = new BisectingKMeans().setK(3);` |
| `setMaxIterations` | int maxIterations | `BisectingKMeans` | 设置最大迭代次数 | `bkm.setMaxIterations(20);` |
| `setMinDivisibleClusterSize` | double minDivisibleClusterSize | `BisectingKMeans` | 设置最小可分簇大小 | `bkm.setMinDivisibleClusterSize(1.0);` |
| `run` | JavaRDD[Vector] data | `BisectingKMeansModel` | 运行聚类 | `BisectingKMeansModel model = bkm.run(data.rdd());` |
| `predict` | Vector point | `Int` | 预测簇归属 | `int cluster = model.predict(vector);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Integer]` | 批量预测 | `JavaRDD<Integer> predictions = model.predict(data);` |
| `clusterCenters` | 无 | `Vector[]` | 获取簇中心 | `Vector[] centers = model.clusterCenters();` |
| `computeCost` | JavaRDD[Vector] data | `Double` | 计算成本 | `double cost = model.computeCost(data.rdd());` |

### GaussianMixture
**包路径**: `org.apache.spark.ml.clustering`
**说明**: 高斯混合模型聚类，假设数据由多个高斯分布组成。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GaussianMixture` | 无 | 构造方法 | 创建高斯混合模型 | `GaussianMixture gm = new GaussianMixture();` |
| `setK` | int value | `GaussianMixture` | 设置聚类数（默认2） | `gm.setK(3);` |
| `setMaxIter` | int value | `GaussianMixture` | 设置最大迭代次数（默认100） | `gm.setMaxIter(50);` |
| `setTol` | double value | `GaussianMixture` | 设置收敛容忍度（默认0.01） | `gm.setTol(0.001);` |
| `setFeaturesCol` | String value | `GaussianMixture` | 设置特征列名 | `gm.setFeaturesCol("features");` |
| `setSeed` | long value | `GaussianMixture` | 设置随机种子 | `gm.setSeed(12345L);` |
| `fit` | Dataset<?> dataset | `GaussianMixtureModel` | 训练模型 | `GaussianMixtureModel model = gm.fit(data);` |
| `setAggregationDepth` | int value | `GaussianMixture` | 设置聚合深度 | `gm.setAggregationDepth(10);` |

---

### LogisticRegressionModel / LogisticRegressionWithSGD
**包路径**: `org.apache.spark.mllib.classification`
**说明**: 逻辑回归分类模型。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `LogisticRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations | `LogisticRegressionModel` | SGD训练逻辑回归 | `JavaRDD<LabeledPoint> training = labeledRDD;<br>LogisticRegressionModel model = LogisticRegressionWithSGD.train(training.rdd(), 100);` |
| `LogisticRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations, double stepSize | `LogisticRegressionModel` | 指定步长 | `LogisticRegressionModel model = LogisticRegressionWithSGD.train(training.rdd(), 100, 1.0);` |
| `LogisticRegressionWithSGD.train` | ... int regParam, int miniBatchFraction | `LogisticRegressionModel` | 指定正则化和批次比例 | `LogisticRegressionModel model = LogisticRegressionWithSGD.train(data, 100, 0.01, 1.0);` |
| `predict` | Vector point | `Double` | 预测类别（0或1） | `double label = model.predict(features);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Double]` | 批量预测 | `JavaRDD<Double> predictions = model.predict(testData);` |
| `predictProbabilities` | JavaRDD[Vector] points | `JavaRDD[Vector]` | 预测概率 | `JavaRDD<Vector> probs = model.predictProbabilities(testData);` |
| `weights` | 无 | `Vector` | 获取模型权重 | `Vector weights = model.weights();` |
| `intercept` | 无 | `Double` | 获取截距 | `double intercept = model.intercept();` |
| `clearThreshold` | 无 | `LogisticRegressionModel` | 清除阈值，返回概率 | `model.clearThreshold();` |
| `setThreshold` | double threshold | `LogisticRegressionModel` | 设置分类阈值 | `model.setThreshold(0.5);` |

### RandomForestClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 随机森林分类器，集成多个决策树进行分类。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `RandomForestClassifier` | 无 | 构造方法 | 创建随机森林分类器 | `RandomForestClassifier rf = new RandomForestClassifier();` |
| `setLabelCol` | String value | `RandomForestClassifier` | 设置标签列名 | `rf.setLabelCol("label");` |
| `setFeaturesCol` | String value | `RandomForestClassifier` | 设置特征列名 | `rf.setFeaturesCol("features");` |
| `setNumTrees` | int value | `RandomForestClassifier` | 设置树数量（默认20） | `rf.setNumTrees(50);` |
| `setMaxDepth` | int value | `RandomForestClassifier` | 设置最大深度（默认5） | `rf.setMaxDepth(10);` |
| `setMaxBins` | int value | `RandomForestClassifier` | 设置最大分箱数（默认32） | `rf.setMaxBins(64);` |
| `setImpurity` | String value | `RandomForestClassifier` | 设置不纯度度量 | `rf.setImpurity("gini");` |
| `setFeatureSubsetStrategy` | String value | `RandomForestClassifier` | 设置特征子集策略 | `rf.setFeatureSubsetStrategy("auto");` |
| `fit` | Dataset<?> dataset | `RandomForestClassificationModel` | 训练模型 | `RandomForestClassificationModel model = rf.fit(trainingData);` |
| `setSeed` | long value | `RandomForestClassifier` | 设置随机种子 | `rf.setSeed(12345L);` |

---

### GBTClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 梯度提升树分类器（Gradient-Boosted Trees），通过迭代训练决策树。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GBTClassifier` | 无 | 构造方法 | 创建GBT分类器 | `GBTClassifier gbt = new GBTClassifier();` |
| `setLabelCol` | String value | `GBTClassifier` | 设置标签列名 | `gbt.setLabelCol("label");` |
| `setFeaturesCol` | String value | `GBTClassifier` | 设置特征列名 | `gbt.setFeaturesCol("features");` |
| `setMaxIter` | int value | `GBTClassifier` | 设置迭代次数（默认20） | `gbt.setMaxIter(50);` |
| `setMaxDepth` | int value | `GBTClassifier` | 设置最大深度（默认5） | `gbt.setMaxDepth(10);` |
| `setMaxBins` | int value | `GBTClassifier` | 设置最大分箱数（默认32） | `gbt.setMaxBins(64);` |
| `setLearningRate` | double value | `GBTClassifier` | 设置学习率（默认0.1） | `gbt.setLearningRate(0.05);` |
| `setStepSize` | double value | `GBTClassifier` | 设置步长 | `gbt.setStepSize(0.1);` |
| `fit` | Dataset<?> dataset | `GBTClassificationModel` | 训练模型 | `GBTClassificationModel model = gbt.fit(trainingData);` |
| `setValidationTol` | double value | `GBTClassifier` | 设置验证容忍度 | `gbt.setValidationTol(0.01);` |

---

### RandomForestRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 随机森林回归器。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `RandomForestRegressor` | 无 | 构造方法 | 创建随机森林回归器 | `RandomForestRegressor rf = new RandomForestRegressor();` |
| `setLabelCol` | String value | `RandomForestRegressor` | 设置标签列名 | `rf.setLabelCol("label");` |
| `setFeaturesCol` | String value | `RandomForestRegressor` | 设置特征列名 | `rf.setFeaturesCol("features");` |
| `setNumTrees` | int value | `RandomForestRegressor` | 设置树数量（默认20） | `rf.setNumTrees(50);` |
| `setMaxDepth` | int value | `RandomForestRegressor` | 设置最大深度（默认5） | `rf.setMaxDepth(10);` |
| `setMaxBins` | int value | `RandomForestRegressor` | 设置最大分箱数（默认32） | `rf.setMaxBins(64);` |
| `setImpurity` | String value | `RandomForestRegressor` | 设置不纯度度量 | `rf.setImpurity("variance");` |
| `setFeatureSubsetStrategy` | String value | `RandomForestRegressor` | 设置特征子集策略 | `rf.setFeatureSubsetStrategy("auto");` |
| `fit` | Dataset<?> dataset | `RandomForestRegressionModel` | 训练模型 | `RandomForestRegressionModel model = rf.fit(trainingData);` |
| `setSeed` | long value | `RandomForestRegressor` | 设置随机种子 | `rf.setSeed(12345L);` |

---

### GBTRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 梯度提升树回归器。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GBTRegressor` | 无 | 构造方法 | 创建GBT回归器 | `GBTRegressor gbt = new GBTRegressor();` |
| `setLabelCol` | String value | `GBTRegressor` | 设置标签列名 | `gbt.setLabelCol("label");` |
| `setFeaturesCol` | String value | `GBTRegressor` | 设置特征列名 | `gbt.setFeaturesCol("features");` |
| `setMaxIter` | int value | `GBTRegressor` | 设置迭代次数（默认20） | `gbt.setMaxIter(50);` |
| `setMaxDepth` | int value | `GBTRegressor` | 设置最大深度（默认5） | `gbt.setMaxDepth(10);` |
| `setMaxBins` | int value | `GBTRegressor` | 设置最大分箱数（默认32） | `gbt.setMaxBins(64);` |
| `setLearningRate` | double value | `GBTRegressor` | 设置学习率（默认0.1） | `gbt.setLearningRate(0.05);` |
| `setStepSize` | double value | `GBTRegressor` | 设置步长 | `gbt.setStepSize(0.1);` |
| `fit` | Dataset<?> dataset | `GBTRegressionModel` | 训练模型 | `GBTRegressionModel model = gbt.fit(trainingData);` |
| `setValidationTol` | double value | `GBTRegressor` | 设置验证容忍度 | `gbt.setValidationTol(0.01);` |

---

### SVMModel / SVMWithSGD
**包路径**: `org.apache.spark.mllib.classification`
**说明**: SVM支持向量机分类模型。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `SVMWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations | `SVMModel` | SGD训练SVM | `SVMModel model = SVMWithSGD.train(training.rdd(), 100);` |
| `SVMWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations, double stepSize, double regParam | `SVMModel` | 指定步长和正则化 | `SVMModel model = SVMWithSGD.train(training.rdd(), 100, 1.0, 0.01);` |
| `predict` | Vector point | `Double` | 预测类别 | `double label = model.predict(features);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Double]` | 批量预测 | `JavaRDD<Double> predictions = model.predict(testData);` |
| `weights` | 无 | `Vector` | 获取权重 | `Vector weights = model.weights();` |
| `intercept` | 无 | `Double` | 获取截距 | `double intercept = model.intercept();` |
| `clearThreshold` | 无 | `SVMModel` | 清除阈值 | `model.clearThreshold();` |
| `setThreshold` | double threshold | `SVMModel` | 设置阈值 | `model.setThreshold(0.0);` |

### NaiveBayes / NaiveBayesModel
**包路径**: `org.apache.spark.mllib.classification`
**说明**: 朴素贝叶斯分类模型。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `NaiveBayes.train` | JavaRDD[LabeledPoint] data, double lambda | `NaiveBayesModel` | 训练朴素贝叶斯模型 | `NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);` |
| `predict` | Vector point | `Double` | 预测类别 | `double label = model.predict(features);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Double]` | 批量预测 | `JavaRDD<Double> predictions = model.predict(testData);` |
| `predictProbabilities` | JavaRDD[Vector] points | `JavaRDD[Vector]` | 预测概率分布 | `JavaRDD<Vector> probs = model.predictProbabilities(testData);` |
| `labels` | 无 | `Double[]` | 获取所有类别标签 | `double[] labels = model.labels();` |
| `pi` | 无 | `Vector` | 获取类别先验概率 | `Vector pi = model.pi();` |

### MultilayerPerceptronClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 多层感知机分类器（神经网络），用于复杂分类任务。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `MultilayerPerceptronClassifier` | 无 | 构造方法 | 创建多层感知机分类器 | `MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier();` |
| `setLayers` | int[] layers | `MultilayerPerceptronClassifier` | 设置网络结构 | `mlp.setLayers(new int[]{4, 5, 4, 2});` |
| `setMaxIter` | int value | `MultilayerPerceptronClassifier` | 设置最大迭代次数（默认100） | `mlp.setMaxIter(100);` |
| `setBlockSize` | int value | `MultilayerPerceptronClassifier` | 设置块大小（默认128） | `mlp.setBlockSize(128);` |
| `setSeed` | long value | `MultilayerPerceptronClassifier` | 设置随机种子 | `mlp.setSeed(12345L);` |
| `setFeaturesCol` | String value | `MultilayerPerceptronClassifier` | 设置特征列名 | `mlp.setFeaturesCol("features");` |
| `setLabelCol` | String value | `MultilayerPerceptronClassifier` | 设置标签列名 | `mlp.setLabelCol("label");` |
| `setSolver` | String value | `MultilayerPerceptronClassifier` | 设置求解器 | `mlp.setSolver("l-bfgs");` |
| `setStepSize` | double value | `MultilayerPerceptronClassifier` | 设置步长 | `mlp.setStepSize(0.03);` |
| `fit` | Dataset<?> dataset | `MultilayerPerceptronClassificationModel` | 训练模型 | `MultilayerPerceptronClassificationModel model = mlp.fit(trainingData);` |

---

### LinearSVC
**包路径**: `org.apache.spark.ml.classification`
**说明**: 线性支持向量分类器，用于二分类任务。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `LinearSVC` | 无 | 构造方法 | 创建线性SVC | `LinearSVC svc = new LinearSVC();` |
| `setMaxIter` | int value | `LinearSVC` | 设置最大迭代次数（默认100） | `svc.setMaxIter(100);` |
| `setRegParam` | double value | `LinearSVC` | 设置正则化参数（默认0） | `svc.setRegParam(0.01);` |
| `setStandardization` | boolean value | `LinearSVC` | 是否标准化特征（默认true） | `svc.setStandardization(true);` |
| `setThreshold` | double value | `LinearSVC` | 设置阈值（默认0） | `svc.setThreshold(0.0);` |
| `setAggregationDepth` | int value | `LinearSVC` | 设置聚合深度（默认2） | `svc.setAggregationDepth(2);` |
| `setFeaturesCol` | String value | `LinearSVC` | 设置特征列名 | `svc.setFeaturesCol("features");` |
| `setLabelCol` | String value | `LinearSVC` | 设置标签列名 | `svc.setLabelCol("label");` |
| `fit` | Dataset<?> dataset | `LinearSVCModel` | 训练模型 | `LinearSVCModel model = svc.fit(trainingData);` |
| `setWeightCol` | String value | `LinearSVC` | 设置权重列名 | `svc.setWeightCol("weight");` |

---

### OneVsRest
**包路径**: `org.apache.spark.ml.classification`
**说明**: 一对多分类器，将二分类器转换为多分类器。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `OneVsRest` | 无 | 构造方法 | 创建一对多分类器 | `OneVsRest ovr = new OneVsRest();` |
| `setClassifier` | Classifier classifier | `OneVsRest` | 设置二分类器 | `ovr.setClassifier(new LogisticRegression());` |
| `setLabelCol` | String value | `OneVsRest` | 设置标签列名 | `ovr.setLabelCol("label");` |
| `setFeaturesCol` | String value | `OneVsRest` | 设置特征列名 | `ovr.setFeaturesCol("features");` |
| `setPredictionCol` | String value | `OneVsRest` | 设置预测列名 | `ovr.setPredictionCol("prediction");` |
| `fit` | Dataset<?> dataset | `OneVsRestModel` | 训练模型 | `OneVsRestModel model = ovr.fit(trainingData);` |
| `setParallelism` | int value | `OneVsRest` | 设置并行度 | `ovr.setParallelism(2);` |
| `copy` | ParamMap extra | `OneVsRest` | 复制分类器 | `OneVsRest copied = ovr.copy(new ParamMap());` |

---

### LinearRegressionModel / LinearRegressionWithSGD
**包路径**: `org.apache.spark.mllib.regression`
**说明**: 线性回归模型。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `LinearRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations | `LinearRegressionModel` | SGD训练线性回归 | `LinearRegressionModel model = LinearRegressionWithSGD.train(training.rdd(), 100);` |
| `LinearRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations, double stepSize | `LinearRegressionModel` | 指定步长 | `LinearRegressionModel model = LinearRegressionWithSGD.train(training.rdd(), 100, 0.1);` |
| `predict` | Vector point | `Double` | 预测值 | `double value = model.predict(features);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Double]` | 批量预测 | `JavaRDD<Double> predictions = model.predict(testData);` |
| `weights` | 无 | `Vector` | 获取权重 | `Vector weights = model.weights();` |
| `intercept` | 无 | `Double` | 获取截距 | `double intercept = model.intercept();` |

### DecisionTreeRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 决策树回归器。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `DecisionTreeRegressor` | 无 | 构造方法 | 创建决策树回归器 | `DecisionTreeRegressor dt = new DecisionTreeRegressor();` |
| `setMaxDepth` | int value | `DecisionTreeRegressor` | 设置最大深度（默认5） | `dt.setMaxDepth(10);` |
| `setMaxBins` | int value | `DecisionTreeRegressor` | 设置最大分箱数（默认32） | `dt.setMaxBins(64);` |
| `setMinInstancesPerNode` | int value | `DecisionTreeRegressor` | 设置每个节点最小实例数 | `dt.setMinInstancesPerNode(1);` |
| `setMinInfoGain` | double value | `DecisionTreeRegressor` | 设置最小信息增益 | `dt.setMinInfoGain(0.0);` |
| `setFeaturesCol` | String value | `DecisionTreeRegressor` | 设置特征列名 | `dt.setFeaturesCol("features");` |
| `setLabelCol` | String value | `DecisionTreeRegressor` | 设置标签列名 | `dt.setLabelCol("label");` |
| `fit` | Dataset<?> dataset | `DecisionTreeRegressionModel` | 训练模型 | `DecisionTreeRegressionModel model = dt.fit(trainingData);` |

---

### GeneralizedLinearRegression
**包路径**: `org.apache.spark.ml.regression`
**说明**: 广义线性回归，支持多种分布族和链接函数。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GeneralizedLinearRegression` | 无 | 构造方法 | 创建广义线性回归 | `GeneralizedLinearRegression glr = new GeneralizedLinearRegression();` |
| `setFamily` | String value | `GeneralizedLinearRegression` | 设置分布族 | `glr.setFamily("gaussian");` |
| `setLink` | String value | `GeneralizedLinearRegression` | 设置链接函数 | `glr.setLink("identity");` |
| `setMaxIter` | int value | `GeneralizedLinearRegression` | 设置最大迭代次数（默认25） | `glr.setMaxIter(100);` |
| `setRegParam` | double value | `GeneralizedLinearRegression` | 设置正则化参数 | `glr.setRegParam(0.0);` |
| `setTol` | double value | `GeneralizedLinearRegression` | 设置收敛容忍度 | `glr.setTol(1e-6);` |
| `setFeaturesCol` | String value | `GeneralizedLinearRegression` | 设置特征列名 | `glr.setFeaturesCol("features");` |
| `setLabelCol` | String value | `GeneralizedLinearRegression` | 设置标签列名 | `glr.setLabelCol("label");` |
| `fit` | Dataset<?> dataset | `GeneralizedLinearRegressionModel` | 训练模型 | `GeneralizedLinearRegressionModel model = glr.fit(trainingData);` |
| `setWeightCol` | String value | `GeneralizedLinearRegression` | 设置权重列名 | `glr.setWeightCol("weight");` |

---

### AFTSurvivalRegression
**包路径**: `org.apache.spark.ml.regression`
**说明**: 加速失效时间生存分析回归，用于生存时间预测。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AFTSurvivalRegression` | 无 | 构造方法 | 创建AFT生存回归 | `AFTSurvivalRegression aft = new AFTSurvivalRegression();` |
| `setFeaturesCol` | String value | `AFTSurvivalRegression` | 设置特征列名 | `aft.setFeaturesCol("features");` |
| `setLabelCol` | String value | `AFTSurvivalRegression` | 设置标签列名（生存时间） | `aft.setLabelCol("time");` |
| `setCensorCol` | String value | `AFTSurvivalRegression` | 设置截尾列名 | `aft.setCensorCol("censor");` |
| `setMaxIter` | int value | `AFTSurvivalRegression` | 设置最大迭代次数（默认100） | `aft.setMaxIter(100);` |
| `setTol` | double value | `AFTSurvivalRegression` | 设置收敛容忍度（默认1E-6） | `aft.setTol(1e-6);` |
| `setAggregationDepth` | int value | `AFTSurvivalRegression` | 设置聚合深度 | `aft.setAggregationDepth(2);` |
| `setQuantileProbabilities` | double[] value | `AFTSurvivalRegression` | 设置分位数概率 | `aft.setQuantileProbabilities(new double[]{0.1, 0.5, 0.9});` |
| `setQuantilesCol` | String value | `AFTSurvivalRegression` | 设置分位数输出列名 | `aft.setQuantilesCol("quantiles");` |
| `fit` | Dataset<?> dataset | `AFTSurvivalRegressionModel` | 训练模型 | `AFTSurvivalRegressionModel model = aft.fit(data);` |

---

### FMRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 因子分解机回归器，用于推荐系统特征交叉建模。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `FMRegressor` | 无 | 构造方法 | 创建FM回归器 | `FMRegressor fm = new FMRegressor();` |
| `setFactorSize` | int value | `FMRegressor` | 设置因子维度（默认8） | `fm.setFactorSize(8);` |
| `setFitLinear` | boolean value | `FMRegressor` | 是否拟合线性项（默认true） | `fm.setFitLinear(true);` |
| `setRegParam` | double value | `FMRegressor` | 设置正则化参数（默认0） | `fm.setRegParam(0.01);` |
| `setMiniBatchFraction` | double value | `FMRegressor` | 设置小批量比例（默认1.0） | `fm.setMiniBatchFraction(0.5);` |
| `setInitStd` | double value | `FMRegressor` | 设置初始化标准差（默认0.01） | `fm.setInitStd(0.01);` |
| `setMaxIter` | int value | `FMRegressor` | 设置最大迭代次数 | `fm.setMaxIter(100);` |
| `fit` | Dataset<?> dataset | `FMRegressionModel` | 训练模型 | `FMRegressionModel model = fm.fit(data);` |

---

### FMClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 因子分解机分类器，用于推荐系统特征交叉建模分类。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `FMClassifier` | 无 | 构造方法 | 创建FM分类器 | `FMClassifier fm = new FMClassifier();` |
| `setFactorSize` | int value | `FMClassifier` | 设置因子维度（默认8） | `fm.setFactorSize(8);` |
| `setFitLinear` | boolean value | `FMClassifier` | 是否拟合线性项（默认true） | `fm.setFitLinear(true);` |
| `setRegParam` | double value | `FMClassifier` | 设置正则化参数（默认0） | `fm.setRegParam(0.01);` |
| `setMiniBatchFraction` | double value | `FMClassifier` | 设置小批量比例（默认1.0） | `fm.setMiniBatchFraction(0.5);` |
| `setInitStd` | double value | `FMClassifier` | 设置初始化标准差（默认0.01） | `fm.setInitStd(0.01);` |
| `setMaxIter` | int value | `FMClassifier` | 设置最大迭代次数 | `fm.setMaxIter(100);` |
| `fit` | Dataset<?> dataset | `FMClassificationModel` | 训练模型 | `FMClassificationModel model = fm.fit(data);` |

---

### ALS / MatrixFactorizationModel
**包路径**: `org.apache.spark.mllib.recommendation`
**说明**: ALS协同过滤推荐算法。
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setRank` | int rank | `ALS` | 设置隐藏因子数量 | `ALS als = new ALS().setRank(10);` |
| `setIterations` | int iterations | `ALS` | 设置迭代次数 | `als.setIterations(20);` |
| `setLambda` | double lambda | `ALS` | 设置正则化参数 | `als.setLambda(0.01);` |
| `setBlocks` | int blocks | `ALS` | 设置分块数 | `als.setBlocks(-1);  // 自动设置` |
| `setAlpha` | double alpha | `ALS` | 设置置信度参数（隐式反馈） | `als.setAlpha(1.0);` |
| `setImplicitPrefs` | boolean implicitPrefs | `ALS` | 设置是否隐式反馈 | `als.setImplicitPrefs(true);  // 隐式反馈模式` |
| `run` | JavaRDD[Rating] ratings | `MatrixFactorizationModel` | 运行ALS | `MatrixFactorizationModel model = als.run(ratingsRDD.rdd());` |
| `ALS.train` | JavaRDD[Rating] ratings, int rank, int iterations | `MatrixFactorizationModel` | 快速训练 | `MatrixFactorizationModel model = ALS.train(ratingsRDD.rdd(), 10, 20);` |
| `ALS.trainImplicit` | JavaRDD[Rating] ratings, int rank, int iterations | `MatrixFactorizationModel` | 隐式反馈训练 | `MatrixFactorizationModel model = ALS.trainImplicit(ratingsRDD.rdd(), 10, 20, 0.01, -1);` |
| `predict` | JavaRDD[Tuple2[Int, Int]] usersProducts | `JavaRDD[Rating]` | 预测评分 | `JavaRDD<Rating> predictions = model.predict(userItemRDD);` |
| `predictAll` | JavaRDD[Tuple2[Int, Int]] usersProducts | `JavaRDD[Rating]` | 预测所有（同predict） | `JavaRDD<Rating> predictions = alsModel.predictAll(userProductPairs);` |
| `recommendProducts` | int user, int num | `Rating[]` | 为用户推荐产品 | `Rating[] top5 = model.recommendProducts(userId, 5);` |
| `recommendUsers` | int product, int num | `Rating[]` | 为产品推荐用户 | `Rating[] top5Users = model.recommendUsers(productId, 5);` |
| `productFeatures` | 无 | `JavaPairRDD[Int, Vector]` | 获取产品特征矩阵 | `JavaPairRDD<Integer, Vector> features = model.productFeatures();` |
| `userFeatures` | 无 | `JavaPairRDD[Int, Vector]` | 获取用户特征矩阵 | `JavaPairRDD<Integer, Vector> features = model.userFeatures();` |
| `rank` | 无 | `Int` | 获取隐藏因子数量 | `int rank = model.rank();` |

### ALSModel
**包路径**: `org.apache.spark.ml.recommendation`
**说明**: ALS训练后的模型，用于推荐预测。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行预测 | `Dataset<Row> predictions = model.transform(testData);` |
| `recommendForAllUsers` | int numItems | `Dataset[Row]` | 为所有用户推荐物品 | `Dataset<Row> userRecs = model.recommendForAllUsers(10);` |
| `recommendForAllItems` | int numUsers | `Dataset[Row]` | 为所有物品推荐用户 | `Dataset<Row> itemRecs = model.recommendForAllItems(10);` |
| `recommendForUserSubset` | Dataset<?> users, int numItems | `Dataset[Row]` | 为指定用户推荐 | `Dataset<Row> userRecs = model.recommendForUserSubset(userSubset, 10);` |
| `recommendForItemSubset` | Dataset<?> items, int numUsers | `Dataset[Row]` | 为指定物品推荐 | `Dataset<Row> itemRecs = model.recommendForItemSubset(itemSubset, 10);` |
| `write` | 无 | `MLWriter` | 保存模型 | `model.write().overwrite().save("hdfs://model/als");` |

---

### PCA
**包路径**: `org.apache.spark.mllib.feature`
**说明**: PCA主成分分析降维。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `PCA` | int k | 构造方法 | 创建PCA变换器 | `PCA pca = new PCA(3);  // 降到3维` |
| `fit` | JavaRDD[Vector] data | `PCAModel` | 训练PCA模型 | `PCAModel model = pca.fit(data.rdd());` |
| `transform` | Vector vector | `Vector` | 转换向量 | `Vector reduced = model.transform(originalVector);` |
| `transform` | JavaRDD[Vector] data | `JavaRDD[Vector]` | 批量转换 | `JavaRDD<Vector> reduced = model.transform(data);` |
| `pc` | 无 | `Matrix` | 获取主成分矩阵 | `Matrix principalComponents = model.pc();` |
| `explainedVariance` | 无 | `Vector` | 获取解释方差比例 | `Vector variance = model.explainedVariance();` |

### HashingTF
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将文本词转换为固定大小的向量，使用哈希技巧。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `HashingTF` | 无 | 构造方法 | 创建HashingTF | `HashingTF hashingTF = new HashingTF();` |
| `setInputCol` | String value | `HashingTF` | 设置输入列名 | `hashingTF.setInputCol("words");` |
| `setOutputCol` | String value | `HashingTF` | 设置输出列名 | `hashingTF.setOutputCol("features");` |
| `setNumFeatures` | int value | `HashingTF` | 设置特征数量（默认2^18） | `hashingTF.setNumFeatures(10000);` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> result = hashingTF.transform(sentences);` |

---

### Tokenizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将文本分割为单词列表。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Tokenizer` | 无 | 构造方法 | 创建Tokenizer | `Tokenizer tokenizer = new Tokenizer();` |
| `setInputCol` | String value | `Tokenizer` | 设置输入列名 | `tokenizer.setInputCol("text");` |
| `setOutputCol` | String value | `Tokenizer` | 设置输出列名 | `tokenizer.setOutputCol("words");` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> words = tokenizer.transform(texts);` |

---

### StopWordsRemover
**包路径**: `org.apache.spark.ml.feature`
**说明**: 移除停用词（如"a", "the"等）。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StopWordsRemover` | 无 | 构造方法 | 创建StopWordsRemover | `StopWordsRemover remover = new StopWordsRemover();` |
| `setInputCol` | String value | `StopWordsRemover` | 设置输入列名 | `remover.setInputCol("words");` |
| `setOutputCol` | String value | `StopWordsRemover` | 设置输出列名 | `remover.setOutputCol("filtered");` |
| `setStopWords` | String[] stopWords | `StopWordsRemover` | 设置停用词列表 | `remover.setStopWords(new String[]{"a", "the", "is"});` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> filtered = remover.transform(words);` |

---

### IDF
**包路径**: `org.apache.spark.ml.feature`
**说明**: 计算词频-逆文档频率，衡量词的重要性。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `IDF` | 无 | 构造方法 | 创建IDF | `IDF idf = new IDF();` |
| `setInputCol` | String value | `IDF` | 设置输入列名 | `idf.setInputCol("features");` |
| `setOutputCol` | String value | `IDF` | 设置输出列名 | `idf.setOutputCol("idf_features");` |
| `fit` | Dataset<?> dataset | `IDFModel` | 训练IDF模型 | `IDFModel model = idf.fit(tfFeatures);` |
| `setMinDocFreq` | int value | `IDF` | 设置最小文档频率 | `idf.setMinDocFreq(3);` |

---

### Word2Vec
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将单词映射到向量空间，捕捉语义相似性。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Word2Vec` | 无 | 构造方法 | 创建Word2Vec | `Word2Vec word2Vec = new Word2Vec();` |
| `setInputCol` | String value | `Word2Vec` | 设置输入列名 | `word2Vec.setInputCol("words");` |
| `setOutputCol` | String value | `Word2Vec` | 设置输出列名 | `word2Vec.setOutputCol("vector");` |
| `setVectorSize` | int value | `Word2Vec` | 设置向量维度（默认100） | `word2Vec.setVectorSize(50);` |
| `setMinCount` | int value | `Word2Vec` | 设置最小出现次数（默认5） | `word2Vec.setMinCount(2);` |
| `setWindowSize` | int value | `Word2Vec` | 设置窗口大小（默认5） | `word2Vec.setWindowSize(10);` |
| `fit` | Dataset<?> dataset | `Word2VecModel` | 训练模型 | `Word2VecModel model = word2Vec.fit(sentences);` |
| `setMaxSentenceLength` | int value | `Word2Vec` | 设置最大句子长度 | `word2Vec.setMaxSentenceLength(1000);` |

---

### CountVectorizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将文本转换为词频向量。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `CountVectorizer` | 无 | 构造方法 | 创建CountVectorizer | `CountVectorizer cv = new CountVectorizer();` |
| `setInputCol` | String value | `CountVectorizer` | 设置输入列名 | `cv.setInputCol("words");` |
| `setOutputCol` | String value | `CountVectorizer` | 设置输出列名 | `cv.setOutputCol("features");` |
| `setVocabSize` | int value | `CountVectorizer` | 设置词汇表大小（默认2^18） | `cv.setVocabSize(1000);` |
| `setMinDF` | double value | `CountVectorizer` | 设置最小文档频率 | `cv.setMinDF(2.0);` |
| `setMinTF` | double value | `CountVectorizer` | 设置最小词频 | `cv.setMinTF(1.0);` |
| `fit` | Dataset<?> dataset | `CountVectorizerModel` | 训练模型 | `CountVectorizerModel model = cv.fit(sentences);` |
| `setBinary` | boolean value | `CountVectorizer` | 设置是否二进制输出 | `cv.setBinary(true);` |

---

### VectorAssembler
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将多列合并为单个向量列。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `VectorAssembler` | 无 | 构造方法 | 创建VectorAssembler | `VectorAssembler assembler = new VectorAssembler();` |
| `setInputCols` | String[] values | `VectorAssembler` | 设置输入列名 | `assembler.setInputCols(new String[]{"age", "income", "score"});` |
| `setOutputCol` | String value | `VectorAssembler` | 设置输出列名 | `assembler.setOutputCol("features");` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> assembled = assembler.transform(data);` |

---

### MinMaxScaler
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将向量列缩放到[0,1]范围。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `MinMaxScaler` | 无 | 构造方法 | 创建MinMaxScaler | `MinMaxScaler scaler = new MinMaxScaler();` |
| `setInputCol` | String value | `MinMaxScaler` | 设置输入列名 | `scaler.setInputCol("features");` |
| `setOutputCol` | String value | `MinMaxScaler` | 设置输出列名 | `scaler.setOutputCol("scaled");` |
| `setMin` | double value | `MinMaxScaler` | 设置最小值（默认0） | `scaler.setMin(0.0);` |
| `setMax` | double value | `MinMaxScaler` | 设置最大值（默认1） | `scaler.setMax(1.0);` |
| `fit` | Dataset<?> dataset | `MinMaxScalerModel` | 训练模型 | `MinMaxScalerModel model = scaler.fit(data);` |

---

### OneHotEncoder
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将分类特征转换为二进制向量。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `OneHotEncoder` | 无 | 构造方法 | 创建OneHotEncoder | `OneHotEncoder encoder = new OneHotEncoder();` |
| `setInputCol` | String value | `OneHotEncoder` | 设置输入列名 | `encoder.setInputCol("category");` |
| `setOutputCol` | String value | `OneHotEncoder` | 设置输出列名 | `encoder.setOutputCol("category_vec");` |
| `setDropLast` | boolean value | `OneHotEncoder` | 是否丢弃最后一个类别（默认true） | `encoder.setDropLast(false);` |
| `fit` | Dataset<?> dataset | `OneHotEncoderModel` | 训练模型 | `OneHotEncoderModel model = encoder.fit(data);` |

---

### Bucketizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将连续特征分桶为离散特征。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Bucketizer` | 无 | 构造方法 | 创建Bucketizer | `Bucketizer bucketizer = new Bucketizer();` |
| `setInputCol` | String value | `Bucketizer` | 设置输入列名 | `bucketizer.setInputCol("value");` |
| `setOutputCol` | String value | `Bucketizer` | 设置输出列名 | `bucketizer.setOutputCol("bucket");` |
| `setSplitsArray` | double[][] splitsArray | `Bucketizer` | 设置分桶边界 | `bucketizer.setSplitsArray(new double[][]{{0, 10, 20, 100}});` |

---

### Imputer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 缺失值填充器，使用均值或中位数填充缺失值。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Imputer` | 无 | 构造方法 | 创建Imputer | `Imputer imputer = new Imputer();` |
| `setInputCols` | String[] value | `Imputer` | 设置输入列名 | `imputer.setInputCols(new String[]{"age", "income"});` |
| `setOutputCols` | String[] value | `Imputer` | 设置输出列名 | `imputer.setOutputCols(new String[]{"age_imputed", "income_imputed"});` |
| `setStrategy` | String value | `Imputer` | 设置填充策略 | `imputer.setStrategy("mean");` |
| `setMissingValue` | double value | `Imputer` | 设置缺失值标识 | `imputer.setMissingValue(Double.NaN);` |
| `fit` | Dataset<?> dataset | `ImputerModel` | 训练填充模型 | `ImputerModel model = imputer.fit(data);` |

---

### Binarizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 二值化器，将连续特征转换为二值（0/1）。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Binarizer` | 无 | 构造方法 | 创建Binarizer | `Binarizer binarizer = new Binarizer();` |
| `setInputCol` | String value | `Binarizer` | 设置输入列名 | `binarizer.setInputCol("feature");` |
| `setOutputCol` | String value | `Binarizer` | 设置输出列名 | `binarizer.setOutputCol("binary_feature");` |
| `setThreshold` | double value | `Binarizer` | 设置阈值（默认0.5） | `binarizer.setThreshold(0.5);` |

---

### QuantileDiscretizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 分位数离散化器，将连续特征按分位数分为多个桶。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `QuantileDiscretizer` | 无 | 构造方法 | 创建QuantileDiscretizer | `QuantileDiscretizer discretizer = new QuantileDiscretizer();` |
| `setInputCol` | String value | `QuantileDiscretizer` | 设置输入列名 | `discretizer.setInputCol("value");` |
| `setOutputCol` | String value | `QuantileDiscretizer` | 设置输出列名 | `discretizer.setOutputCol("bucket");` |
| `setNumBuckets` | int value | `QuantileDiscretizer` | 设置桶数量（默认10） | `discretizer.setNumBuckets(10);` |
| `fit` | Dataset<?> dataset | `BucketizerModel` | 训练模型 | `BucketizerModel model = discretizer.fit(data);` |

---

### StringIndexer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将字符串标签转换为数值索引（按频率排序）。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StringIndexer` | 无 | 构造方法 | 创建StringIndexer | `StringIndexer indexer = new StringIndexer();` |
| `setInputCol` | String value | `StringIndexer` | 设置输入列名 | `indexer.setInputCol("category");` |
| `setOutputCol` | String value | `StringIndexer` | 设置输出列名 | `indexer.setOutputCol("category_index");` |
| `setHandleInvalid` | String value | `StringIndexer` | 处理无效值方式 | `indexer.setHandleInvalid("keep");` |
| `fit` | Dataset<?> dataset | `StringIndexerModel` | 训练模型 | `StringIndexerModel model = indexer.fit(data);` |

---

### Pipeline
**包路径**: `org.apache.spark.ml`
**说明**: Pipeline是一个工作流，将多个Transformer和Estimator串联执行。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Pipeline` | 无 | 构造方法 | 创建Pipeline | `Pipeline pipeline = new Pipeline();` |
| `setStages` | PipelineStage... stages | `Pipeline` | 设置Pipeline阶段 | `pipeline.setStages(new PipelineStage[]{tokenizer, hashingTF, lr});` |
| `getStages` | 无 | `PipelineStage[]` | 获取Pipeline阶段 | `PipelineStage[] stages = pipeline.getStages();` |
| `fit` | Dataset<?> dataset | `PipelineModel` | 训练Pipeline | `PipelineModel model = pipeline.fit(trainingData);` |
| `copy` | ParamMap extra | `Pipeline` | 复制Pipeline | `Pipeline copied = pipeline.copy(new ParamMap());` |

---

### PipelineModel
**包路径**: `org.apache.spark.ml`
**说明**: Pipeline训练后的模型，包含多个Transformer。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行Pipeline转换 | `Dataset<Row> predictions = model.transform(testData);` |
| `stages` | 无 | `Transformer[]` | 获取所有阶段 | `Transformer[] stages = model.stages();` |
| `copy` | ParamMap extra | `PipelineModel` | 复制模型 | `PipelineModel copied = model.copy(new ParamMap());` |
| `write` | 无 | `MLWriter` | 保存模型 | `model.write().overwrite().save("hdfs://model/path");` |

---

### IndexToString
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将数值索引还原为原始字符串标签（StringIndexer的逆操作）。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `IndexToString` | 无 | 构造方法 | 创建IndexToString | `IndexToString converter = new IndexToString();` |
| `setInputCol` | String value | `IndexToString` | 设置输入列名 | `converter.setInputCol("category_index");` |
| `setOutputCol` | String value | `IndexToString` | 设置输出列名 | `converter.setOutputCol("original_category");` |
| `setLabels` | String[] labels | `IndexToString` | 设置标签数组 | `converter.setLabels(new String[]{"cat", "dog", "bird"});` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> converted = converter.transform(indexed);` |

---

### StandardScaler / StandardScalerModel
**包路径**: `org.apache.spark.mllib.feature`
**说明**: 标准化变换器，将特征标准化到均值0、方差1。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StandardScaler` | boolean withMean, boolean withStd | 构造方法 | 创建标准化变换器 | `StandardScaler scaler = new StandardScaler(true, true);  // 均值和方差标准化` |
| `fit` | JavaRDD[Vector] data | `StandardScalerModel` | 训练标准化模型 | `StandardScalerModel model = scaler.fit(data.rdd());` |
| `transform` | Vector vector | `Vector` | 转换向量 | `Vector scaled = model.transform(originalVector);` |
| `transform` | JavaRDD[Vector] data | `JavaRDD[Vector]` | 批量转换 | `JavaRDD<Vector> scaled = model.transform(data);` |
| `mean` | 无 | `Vector` | 获取均值 | `Vector mean = model.mean();` |
| `std` | 无 | `Vector` | 获取标准差 | `Vector std = model.std();` |

### BinaryClassificationEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 二分类评估器，计算AUC、PR等指标。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `BinaryClassificationEvaluator` | 无 | 构造方法 | 创建评估器 | `BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator();` |
| `setLabelCol` | String value | `BinaryClassificationEvaluator` | 设置标签列名 | `evaluator.setLabelCol("label");` |
| `setRawPredictionCol` | String value | `BinaryClassificationEvaluator` | 设置原始预测列名 | `evaluator.setRawPredictionCol("rawPrediction");` |
| `setMetricName` | String value | `BinaryClassificationEvaluator` | 设置评估指标 | `evaluator.setMetricName("areaUnderROC");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double auc = evaluator.evaluate(predictions);` |
| `getMetricName` | 无 | `String` | 获取当前指标名 | `String metric = evaluator.getMetricName();` |

---

### MulticlassClassificationEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 多分类评估器，计算准确率、F1等指标。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `MulticlassClassificationEvaluator` | 无 | 构造方法 | 创建评估器 | `MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();` |
| `setLabelCol` | String value | `MulticlassClassificationEvaluator` | 设置标签列名 | `evaluator.setLabelCol("label");` |
| `setPredictionCol` | String value | `MulticlassClassificationEvaluator` | 设置预测列名 | `evaluator.setPredictionCol("prediction");` |
| `setMetricName` | String value | `MulticlassClassificationEvaluator` | 设置评估指标 | `evaluator.setMetricName("accuracy");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double accuracy = evaluator.evaluate(predictions);` |
| `getMetricName` | 无 | `String` | 获取当前指标名 | `String metric = evaluator.getMetricName();` |

---

### RegressionEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 回归评估器，计算RMSE、MAE等指标。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `RegressionEvaluator` | 无 | 构造方法 | 创建评估器 | `RegressionEvaluator evaluator = new RegressionEvaluator();` |
| `setLabelCol` | String value | `RegressionEvaluator` | 设置标签列名 | `evaluator.setLabelCol("label");` |
| `setPredictionCol` | String value | `RegressionEvaluator` | 设置预测列名 | `evaluator.setPredictionCol("prediction");` |
| `setMetricName` | String value | `RegressionEvaluator` | 设置评估指标 | `evaluator.setMetricName("rmse");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double rmse = evaluator.evaluate(predictions);` |
| `getMetricName` | 无 | `String` | 获取当前指标名 | `String metric = evaluator.getMetricName();` |

---

### ParamGridBuilder
**包路径**: `org.apache.spark.ml.tuning`
**说明**: 参数网格构建器，用于构建超参数搜索空间。
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ParamGridBuilder` | 无 | 构造方法 | 创建参数网格构建器 | `ParamGridBuilder gridBuilder = new ParamGridBuilder();` |
| `addGrid` | Param[T] param, T[] values | `ParamGridBuilder` | 添加参数网格 | `gridBuilder.addGrid(lr.regParam(), new Double[]{0.01, 0.1, 1.0});` |
| `build` | 无 | `ParamMap[]` | 构建参数网格 | `ParamMap[] paramMaps = gridBuilder.build();` |

---

### ParamMap
**包路径**: `org.apache.spark.ml.param`
**说明**: 参数映射，用于设置算法参数。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ParamMap` | 无 | 构造方法 | 创建空参数映射 | `ParamMap params = new ParamMap();` |
| `put` | Param[T] param, T value | `ParamMap` | 设置参数 | `params.put(lr.regParam(), 0.1);` |
| `put` | Pair[Param[T], T]... pairs | `ParamMap` | 设置多个参数 | `params.put(lr.regParam().w(0.1), lr.maxIter().w(100));` |
| `get` | Param[T] param | `T` | 获取参数值 | `double regParam = params.get(lr.regParam());` |
| `getOrDefault` | Param[T] param | `T` | 获取参数值或默认值 | `double value = params.getOrDefault(lr.regParam());` |
| `copy` | 无 | `ParamMap` | 复制参数映射 | `ParamMap copied = params.copy();` |
| `clear` | Param[T] param | `ParamMap` | 清除参数 | `params.clear(lr.regParam());` |
| `contains` | Param[T] param | `boolean` | 判断是否包含参数 | `boolean has = params.contains(lr.regParam());` |

---

### MLWriter
**包路径**: `org.apache.spark.ml.util`
**说明**: ML模型写入器，用于保存MLlib模型。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | String path | `Unit` | 保存模型 | `model.write().save("hdfs://model/path");` |
| `overwrite` | 无 | `MLWriter` | 覆盖写入 | `model.write().overwrite().save("hdfs://model/path");` |
| `option` | String key, String value | `MLWriter` | 设置选项 | `model.write().option("compression", "gzip").save("path");` |
| `option` | String key, boolean value | `MLWriter` | 设置布尔选项 | `model.write().option("skipValidations", true).save("path");` |
| `session` | SparkSession session | `MLWriter` | 设置SparkSession | `model.write().session(spark).save("path");` |
| `context` | SparkContext context | `MLWriter` | 设置SparkContext | `model.write().context(spark.sparkContext()).save("path");` |

---

### MLReader[T]
**包路径**: `org.apache.spark.ml.util`
**说明**: ML模型读取器，用于加载MLlib模型。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `load` | String path | `T` | 加载模型 | `LogisticRegressionModel model = LogisticRegressionModel.load("hdfs://model/path");` |
| `option` | String key, String value | `MLReader[T]` | 设置选项 | `model.read().option("skipValidations", "true").load("path");` |
| `session` | SparkSession session | `MLReader[T]` | 设置SparkSession | `model.read().session(spark).load("path");` |
| `context` | SparkContext context | `MLReader[T]` | 设置SparkContext | `model.read().context(spark.sparkContext()).load("path");` |
| `isFileSystemLoaded` | 无 | `boolean` | 是否从文件系统加载 | `boolean loaded = reader.isFileSystemLoaded();` |

---

### CrossValidator
**包路径**: `org.apache.spark.ml.tuning`
**说明**: K折交叉验证，用于模型选择和超参数调优。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `CrossValidator` | 无 | 构造方法 | 创建交叉验证器 | `CrossValidator cv = new CrossValidator();` |
| `setEstimator` | Estimator<?> estimator | `CrossValidator` | 设置估计器 | `cv.setEstimator(lr);` |
| `setEstimatorParamMaps` | ParamMap[] paramMaps | `CrossValidator` | 设置参数网格 | `cv.setEstimatorParamMaps(paramGrid);` |
| `setEvaluator` | Evaluator evaluator | `CrossValidator` | 设置评估器 | `cv.setEvaluator(new BinaryClassificationEvaluator());` |
| `setNumFolds` | int value | `CrossValidator` | 设置折叠数（默认3） | `cv.setNumFolds(5);` |
| `setParallelism` | int value | `CrossValidator` | 设置并行度 | `cv.setParallelism(2);` |
| `fit` | Dataset<?> dataset | `CrossValidatorModel` | 执行交叉验证 | `CrossValidatorModel model = cv.fit(trainingData);` |
| `getBestModel` | 无 | `Model<?>` | 获取最佳模型 | `Model<?> best = cvModel.bestModel();` |

---

### CrossValidatorModel
**包路径**: `org.apache.spark.ml.tuning`
**说明**: 交叉验证后的模型，包含最佳模型和所有模型。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `bestModel` | 无 | `Model<?>` | 获取最佳模型 | `Model<?> best = cvModel.bestModel();` |
| `avgMetrics` | 无 | `double[]` | 获取平均指标 | `double[] metrics = cvModel.avgMetrics();` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 使用最佳模型转换 | `Dataset<Row> predictions = cvModel.transform(testData);` |
| `write` | 无 | `MLWriter` | 保存模型 | `cvModel.write().overwrite().save("hdfs://model/cv");` |

---

### TrainValidationSplit
**包路径**: `org.apache.spark.ml.tuning`
**说明**: 单次训练验证分割，比交叉验证更快但更不稳定。
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `TrainValidationSplit` | 无 | 构造方法 | 创建训练验证分割器 | `TrainValidationSplit tvs = new TrainValidationSplit();` |
| `setEstimator` | Estimator<?> estimator | `TrainValidationSplit` | 设置估计器 | `tvs.setEstimator(lr);` |
| `setEstimatorParamMaps` | ParamMap[] paramMaps | `TrainValidationSplit` | 设置参数网格 | `tvs.setEstimatorParamMaps(paramGrid);` |
| `setEvaluator` | Evaluator evaluator | `TrainValidationSplit` | 设置评估器 | `tvs.setEvaluator(new RegressionEvaluator());` |
| `setTrainRatio` | double value | `TrainValidationSplit` | 设置训练比例（默认0.75） | `tvs.setTrainRatio(0.8);` |
| `setParallelism` | int value | `TrainValidationSplit` | 设置并行度 | `tvs.setParallelism(2);` |
| `fit` | Dataset<?> dataset | `TrainValidationSplitModel` | 执行训练验证分割 | `TrainValidationSplitModel model = tvs.fit(trainingData);` |

---

### ClusteringEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 聚类评估器，计算轮廓系数等指标。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ClusteringEvaluator` | 无 | 构造方法 | 创建评估器 | `ClusteringEvaluator evaluator = new ClusteringEvaluator();` |
| `setPredictionCol` | String value | `ClusteringEvaluator` | 设置预测列名 | `evaluator.setPredictionCol("prediction");` |
| `setFeaturesCol` | String value | `ClusteringEvaluator` | 设置特征列名 | `evaluator.setFeaturesCol("features");` |
| `setMetricName` | String value | `ClusteringEvaluator` | 设置评估指标 | `evaluator.setMetricName("silhouette");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double silhouette = evaluator.evaluate(predictions);` |

---

### Normalizer
**包路径**: `org.apache.spark.mllib.feature`
**说明**: 归一化变换器，将向量归一化到单位长度。
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Normalizer` | double p | 构造方法 | 创建归一化器 | `Normalizer normalizer = new Normalizer(2.0);  // L2归一化` |
| `transform` | Vector vector | `Vector` | 归一化向量 | `Vector normalized = normalizer.transform(originalVector);` |
| `transform` | JavaRDD[Vector] data | `JavaRDD[Vector]` | 批量归一化 | `JavaRDD<Vector> normalized = normalizer.transform(data);` |

### Word2Vec / Word2VecModel
**包路径**: `org.apache.spark.mllib.feature`
**说明**: Word2Vec词向量训练。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setVectorSize` | int vectorSize | `Word2Vec` | 设置向量维度 | `Word2Vec w2v = new Word2Vec().setVectorSize(100);` |
| `setWindowSize` | int windowSize | `Word2Vec` | 设置窗口大小 | `w2v.setWindowSize(5);` |
| `setMinCount` | int minCount | `Word2Vec` | 设置最小词频 | `w2v.setMinCount(10);  // 出现少于10次的词被忽略` |
| `setNumIterations` | int numIterations | `Word2Vec` | 设置迭代次数 | `w2v.setNumIterations(10);` |
| `setLearningRate` | double learningRate | `Word2Vec` | 设置学习率 | `w2v.setLearningRate(0.025);` |
| `setNumPartitions` | int numPartitions | `Word2Vec` | 设置分区数 | `w2v.setNumPartitions(4);` |
| `fit` | JavaRDD[String] data | `Word2VecModel` | 训练词向量 | `JavaRDD<String> documents = sc.parallelize(Arrays.asList("hello world", "spark java"));<br>Word2VecModel model = w2v.fit(documents);` |
| `transform` | String word | `Vector` | 获取词向量 | `Vector vec = model.transform("spark");` |
| `findSynonyms` | String word, int num | `Tuple2[String, Double][]` | 查找相似词 | `Tuple2<String, Double>[] synonyms = model.findSynonyms("spark", 5);` |
| `findSynonyms` | Vector vector, int num | `Tuple2[String, Double][]` | 查找与向量相似的词 | `Tuple2<String, Double>[] similar = model.findSynonyms(vector, 10);` |
| `getVectors` | 无 | `Map[String, Vector]` | 获取所有词向量 | `Map<String, Vector> vectors = model.getVectors();` |

### FPGrowth / FPGrowthModel
**包路径**: `org.apache.spark.mllib.fpm`
**说明**: FP-Growth频繁项集挖掘算法。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setMinSupport` | double minSupport | `FPGrowth` | 设置最小支持度 | `FPGrowth fpg = new FPGrowth().setMinSupport(0.3);  // 30%支持度` |
| `setNumPartitions` | int numPartitions | `FPGrowth` | 设置分区数 | `fpg.setNumPartitions(10);` |
| `run` | JavaRDD[String[]] data | `FPGrowthModel` | 运行FP-Growth | `JavaRDD<String[]> transactions = sc.parallelize(Arrays.asList(<br>    new String[]{"a", "b", "c"},<br>    new String[]{"a", "b"}));<br>FPGrowthModel model = fpg.run(transitions.rdd());` |
| `freqItemsets` | 无 | `JavaRDD[FreqItemset]` | 获取频繁项集 | `JavaRDD<FreqItemset> itemsets = model.freqItemsets();<br>itemsets.foreach(item -> System.out.println(item.items() + ": " + item.freq()));` |

### AssociationRules
**包路径**: `org.apache.spark.mllib.fpm`
**说明**: 关联规则生成。
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AssociationRules` | 无 | 构造方法 | 创建关联规则生成器 | `AssociationRules ar = new AssociationRules();` |
| `setMinConfidence` | double minConfidence | `AssociationRules` | 设置最小置信度 | `ar.setMinConfidence(0.5);  // 50%置信度` |
| `run` | JavaRDD[FreqItemset] freqItemsets | `JavaRDD[Rule]` | 生成关联规则 | `JavaRDD<Rule> rules = ar.run(fpgModel.freqItemsets().toJavaRDD());<br>rules.foreach(rule -> System.out.println(<br>    rule.antecedent() + " => " + rule.consequent() +<br>    ": confidence=" + rule.confidence()));` |

### BinaryClassificationMetrics
**包路径**: `org.apache.spark.mllib.evaluation`
**说明**: 二分类评估指标。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `BinaryClassificationMetrics` | JavaPairRDD[Double, Double] predictionAndLabels | 构造方法 | 创建评估器 | `JavaPairRDD<Double, Double> predictions = predictedLabelsRDD;<br>BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictions.rdd());` |
| `areaUnderPR` | 无 | `Double` | PR曲线下面积 | `double aupr = metrics.areaUnderPR();` |
| `areaUnderROC` | 无 | `Double` | ROC曲线下面积（AUC） | `double auc = metrics.areaUnderROC();` |
| `pr` | 无 | `JavaRDD[Tuple2[Double, Double]]` | PR曲线数据点 | `JavaRDD<Tuple2<Double, Double>> prCurve = metrics.pr().toJavaRDD();` |
| `roc` | 无 | `JavaRDD[Tuple2[Double, Double]]` | ROC曲线数据点 | `JavaRDD<Tuple2<Double, Double>> rocCurve = metrics.roc().toJavaRDD();` |
| `precisionByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的精确率 | `JavaRDD<Tuple2<Double, Double>> precision = metrics.precisionByThreshold();` |
| `recallByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的召回率 | `JavaRDD<Tuple2<Double, Double>> recall = metrics.recallByThreshold();` |
| `fMeasureByThreshold` | double beta | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的F值 | `JavaRDD<Tuple2<Double, Double>> f1 = metrics.fMeasureByThreshold(1.0);` |

### MulticlassMetrics
**包路径**: `org.apache.spark.mllib.evaluation`
**说明**: 多分类评估指标。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `MulticlassMetrics` | JavaPairRDD[Double, Double] predictionAndLabels | 构造方法 | 创建评估器 | `MulticlassMetrics metrics = new MulticlassMetrics(predictions.rdd());` |
| `accuracy` | 无 | `Double` | 准确率 | `double acc = metrics.accuracy();` |
| `confusionMatrix` | 无 | `Matrix` | 混淆矩阵 | `Matrix cm = metrics.confusionMatrix();` |
| `precision` | 无 | `Double` | 平均精确率 | `double prec = metrics.precision();` |
| `recall` | 无 | `Double` | 平均召回率 | `double rec = metrics.recall();` |
| `fMeasure` | 无 | `Double` | 平均F1值 | `double f1 = metrics.fMeasure();` |

### RegressionMetrics
**包路径**: `org.apache.spark.mllib.evaluation`
**说明**: 回归评估指标。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `RegressionMetrics` | JavaPairRDD[Double, Double] predictionAndLabels | 构造方法 | 创建评估器 | `RegressionMetrics metrics = new RegressionMetrics(predictions.rdd());` |
| `meanAbsoluteError` | 无 | `Double` | 平均绝对误差（MAE） | `double mae = metrics.meanAbsoluteError();` |
| `meanSquaredError` | 无 | `Double` | 平均平方误差（MSE） | `double mse = metrics.meanSquaredError();` |
| `rootMeanSquaredError` | 无 | `Double` | 根均方误差（RMSE） | `double rmse = metrics.rootMeanSquaredError();` |
| `r2` | 无 | `Double` | R平方（决定系数） | `double r2 = metrics.r2();` |
| `explainedVariance` | 无 | `Double` | 解释方差 | `double ev = metrics.explainedVariance();` |

### LDA / LDAModel
**包路径**: `org.apache.spark.mllib.clustering`
**说明**: LDA主题模型（Latent Dirichlet Allocation）。
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setK` | int k | `LDA` | 设置主题数量 | `LDA lda = new LDA().setK(10);` |
| `setMaxIterations` | int maxIterations | `LDA` | 设置最大迭代次数 | `lda.setMaxIterations(50);` |
| `setDocConcentration` | double docConcentration | `LDA` | 设置文档主题分布参数 | `lda.setDocConcentration(-1);  // 自动设置` |
| `setTopicConcentration` | double topicConcentration | `LDA` | 设置主题词分布参数 | `lda.setTopicConcentration(-1);` |
| `run` | JavaRDD[Vector] data | `LDAModel` | 运行LDA | `LDAModel model = lda.run(documents.rdd());` |
| `topicsMatrix` | 无 | `Matrix` | 获取主题-词矩阵 | `Matrix topics = model.topicsMatrix();` |
| `describeTopics` | int maxTermsPerTopic | `Tuple2[Int, Tuple2[Int, Double][]][]` | 描述主题（Top词） | `model.describeTopics(10);  // 每个主题的Top10词` |
| `topicDistributions` | 无 | `JavaPairRDD[Long, Vector]` | 获取文档主题分布 | `JavaPairRDD<Long, Vector> docTopics = model.topicDistributions().toJavaRDD();` |

### Vectors / Matrices
**包路径**: `org.apache.spark.mllib.linalg`
**说明**: 向量和矩阵工具类。
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Vectors.dense` | double... values | `Vector` | 创建密集向量 | `Vector denseVec = Vectors.dense(1.0, 2.0, 3.0);` |
| `Vectors.dense` | double[] values | `Vector` | 创建密集向量（数组） | `double[] arr = {1.0, 2.0, 3.0};<br>Vector vec = Vectors.dense(arr);` |
| `Vectors.sparse` | int size, int[] indices, double[] values | `Vector` | 创建稀疏向量 | `Vector sparseVec = Vectors.sparse(10, new int[]{0, 5}, new double[]{1.0, 2.0});  // 10维，位置0和5有值` |
| `Vectors.sparse` | int size, Iterable[Tuple2[Int, Double]] entries | `Vector` | 创建稀疏向量（迭代器） | `Vector sparse = Vectors.sparse(10, Arrays.asList(new Tuple2<>(0, 1.0), new Tuple2<>(5, 2.0)));` |
| `Vectors.zeros` | int size | `Vector` | 创建零向量 | `Vector zero = Vectors.zeros(10);` |
| `Vectors.norm` | Vector v, double p | `Double` | 计算向量范数 | `double norm = Vectors.norm(vec, 2.0);  // L2范数` |
| `Vectors.sqdist` | Vector v1, Vector v2 | `Double` | 计算向量平方距离 | `double sqDist = Vectors.sqdist(vec1, vec2);` |
| `Matrices.dense` | int numRows, int numCols, double[] values | `Matrix` | 创建密集矩阵 | `Matrix denseMat = Matrices.dense(2, 3, new double[]{1,2,3,4,5,6});` |
| `Matrices.sparse` | int numRows, int numCols, int[] colPtrs, int[] rowIndices, double[] values | `Matrix` | 创建稀疏矩阵（CSC格式） | `Matrix sparse = Matrices.sparse(3, 2, new int[]{0, 1, 3}, new int[]{0, 1, 2}, new double[]{1.0, 2.0, 3.0});` |
| `Matrices.zeros` | int numRows, int numCols | `Matrix` | 创建零矩阵 | `Matrix zeroMat = Matrices.zeros(3, 3);` |
| `Matrices.eye` | int n | `Matrix` | 创建单位矩阵 | `Matrix identity = Matrices.eye(3);` |
| `Matrices.rand` | int numRows, int numCols | `Matrix` | 创建随机矩阵 | `Matrix randMat = Matrices.rand(3, 4);` |
| `size` | 无 | `Int` | 向量维度 | `int dim = vec.size();` |
| `toArray` | 无 | `double[]` | 转为数组 | `double[] arr = vec.toArray();` |
| `dot` | Vector other | `Double` | 向量点积 | `double dot = vec1.dot(vec2);` |

