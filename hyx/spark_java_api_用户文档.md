# Spark Java API 用户文档

> **说明**: 本文档仅包含用户直接调用的public API，开发者API已分离到独立文档。

---

## 快速入门

### 1. RDD方式（Spark Core）

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkRDDExample {
    public static void main(String[] args) {
        // 1. 创建SparkContext
        SparkConf conf = new SparkConf()
            .setAppName("RDD Example")
            .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 2. 读取数据
        JavaRDD<String> lines = sc.textFile("data.txt");
        
        // 3. 转换操作
        JavaRDD<String> filtered = lines.filter(
            new Function<String, Boolean>() {
                public Boolean call(String s) {
                    return s.length() > 10;
                }
            }
        );
        
        // 4. 行动操作
        long count = filtered.count();
        System.out.println("Count: " + count);
        
        // 5. 关闭
        sc.stop();
    }
}
```

### 2. DataFrame方式（Spark SQL）

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class SparkSQLExample {
    public static void main(String[] args) {
        // 1. 创建SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("SQL Example")
            .master("local[*]")
            .getOrCreate();
        
        // 2. 读取数据
        Dataset<Row> df = spark.read()
            .option("header", "true")
            .csv("data.csv");
        
        // 3. SQL操作
        Dataset<Row> result = df
            .filter(col("age").gt(18))
            .groupBy("city")
            .agg(count("id").as("count"));
        
        // 4. 显示结果
        result.show();
        
        // 5. 关闭
        spark.stop();
    }
}
```

### 3. 机器学习（MLlib）

```java
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkMLlibExample {
    public static void main(String[] args) {
        // 1. 初始化
        SparkConf conf = new SparkConf().setAppName("MLlib Example");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 2. 准备数据
        JavaRDD<Vector> data = sc.parallelize(Arrays.asList(
            Vectors.dense(1.0, 2.0),
            Vectors.dense(3.0, 4.0),
            Vectors.dense(5.0, 6.0)
        ));
        
        // 3. 训练模型
        KMeansModel model = KMeans.train(data.rdd(), 2, 10);
        
        // 4. 预测
        int cluster = model.predict(Vectors.dense(2.0, 3.0));
        System.out.println("Cluster: " + cluster);
        
        // 5. 关闭
        sc.stop();
    }
}
```

---

## 文档结构

### JavaDoubleRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 33

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaDoubleRDD` | 缓存RDD到内存，默认MEMORY_ONLY | // cache：缓存RDD到内存
JavaRDD<String> rdd = sc.textFile("hdfs://large/file.txt");

// 缓存后，后续操作会直接从内存读取
rdd.cache();

// 多次使用RDD时缓存可提升性能
long count1 = rdd.count();  // 第一次计算，会缓存
long count2 = rdd.count();  // 第二次直接从内存读取 |
| `coalesce` | numPartitions: Int | `JavaDoubleRDD` | 减少分区数，默认不触发shuffle，适用于合并小分区提高效率 | // coalesce(numPartitions)：仅减少分区，不shuffle
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);  // 10个分区
// 减少到2个分区（高效，数据保持原位置）
JavaRDD<String> coalesced = rdd.coalesce(2); |
| `coalesce` | numPartitions: Int, shuffle: Boolean | `JavaDoubleRDD` | 减少分区数，可控制是否shuffle。shuffle=true时可真正重新分布数据 | // coalesce(numPartitions, shuffle)：可强制shuffle重新分布
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);
// shuffle=true：数据重新均匀分布到2个分区
JavaRDD<String> coalescedShuffle = rdd.coalesce(2, true);
// shuffle=false（默认）：仅合并分区，数据不移动
JavaRDD<String> coalescedNoShuffle = rdd.coalesce(2, false); |
| `distinct` | 无 | `JavaDoubleRDD` | 去除重复元素，使用默认分区数 | // distinct()：去重，使用默认分区
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));
JavaRDD<Integer> distinct = rdd.distinct();
// 结果: [1, 2, 3, 4, 5] |
| `distinct` | numPartitions: Int | `JavaDoubleRDD` | 去除重复元素，指定结果分区数，可控制并行度 | // distinct(numPartitions)：去重并指定分区数
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));
// 指定3个分区，适合大数据去重时控制并行度
JavaRDD<Integer> distinct = rdd.distinct(3);
// 结果: [1, 2, 3, 4, 5]，分散在3个分区中 |
| `filter` | JFunction[Double: f | `JavaDoubleRDD` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `histogram` | bucketCount: Int | `void` | 计算直方图，按指定桶数量均匀划分数据范围 | // histogram(bucketCount)：按桶数计算直方图
List<Double> data = Arrays.asList(1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);
// 指定3个桶，Spark自动计算桶边界
Tuple2<double[], long[]> hist = doubleRDD.histogram(3);
// hist._1 = [1.0, 2.0, 3.0, 4.0] 桶边界
// hist._2 = [3, 2, 2] 每桶元素数 |
| `histogram` | Array[scala.Double]: buckets | `Array` | 计算直方图，使用自定义桶边界，精确控制分桶范围 | // histogram(buckets)：使用自定义桶边界
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 6.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);
// 自定义桶边界：[0, 2, 4, 6]
double[] buckets = new double[]{0.0, 2.0, 4.0, 6.0};
Tuple2<double[], long[]> hist = doubleRDD.histogram(buckets);
// hist._1 = [0.0, 2.0, 4.0, 6.0]
// hist._2 = [2, 2, 2] 每桶元素数 |
| `intersection` | JavaDoubleRDD: other | `JavaDoubleRDD` | 返回两个RDD的交集 | // intersection：取交集
JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));

JavaRDD<Integer> intersection = rdd1.intersection(rdd2);
// 结果: [3, 4] |
| `max` | 无 | `Double` | 最大值 | // max：最大值
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));
double max = doubleRDD.max();
// 结果: 30.0 |
| `mean` | 无 | `Double` | 计算平均值 | // mean：计算平均值
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);

double avg = doubleRDD.mean();
// 结果: 3.0 |
| `meanApprox` | timeout: Long, confidence: Double | `PartialResult` | 近似计算平均值，在超时时间内返回带置信区间的近似结果 | // meanApprox(timeout, confidence)：近似平均值
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0, 100.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);
// 1000ms超时，95%置信度
PartialResult<BoundedDouble> result = doubleRDD.meanApprox(1000, 0.95);
// result.getFinalValue() 返回近似均值及置信区间 |
| `meanApprox` | timeout: Long | `PartialResult` | 近似计算平均值，仅指定超时时间，使用默认置信度0.95 | // meanApprox(timeout)：仅指定超时
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0));
// 500ms超时，默认95%置信度
PartialResult<BoundedDouble> result = doubleRDD.meanApprox(500); |
| `min` | 无 | `Double` | 最小值 | // min：最小值
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));
double min = doubleRDD.min();
// 结果: 5.0 |
| `persist` | StorageLevel: newLevel | `JavaDoubleRDD` | 持久化RDD到指定存储级别 | // persist：持久化到指定存储级别
JavaRDD<String> rdd = sc.textFile("hdfs://data/file.txt");

// 内存+磁盘持久化
rdd.persist(StorageLevel.MEMORY_AND_DISK());

// 序列化存储（节省空间）
rdd.persist(StorageLevel.MEMORY_ONLY_SER());

// 堆外内存存储
rdd.persist(StorageLevel.OFF_HEAP()); |
| `repartition` | numPartitions: Int | `JavaDoubleRDD` | 重新分区，增加或减少分区数，触发shuffle | // repartition：重新分区（会shuffle）
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 2);  // 2个分区

// 增加到10个分区（触发shuffle）
JavaRDD<String> repartitioned = rdd.repartition(10);

// 注意：repartition会shuffle，coalesce只减少分区不shuffle |
| `sample` | withReplacement: Boolean, fraction: Double | `JavaDoubleRDD` | 随机采样，fraction为期望采样比例，非精确比例 | // sample(withReplacement, fraction)：随机采样
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
// 不重复采样（false），期望50%比例
JavaRDD<Integer> sampled = rdd.sample(false, 0.5);
// 重复采样（true），每个元素可被选中多次，期望200%
JavaRDD<Integer> sampledWithRep = rdd.sample(true, 2.0); |
| `sample` | withReplacement: Boolean, fraction: Double, seed: Long | `JavaDoubleRDD` | 随机采样，指定随机种子确保结果可重现 | // sample(withReplacement, fraction, seed)：可重现采样
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
// 指定种子42，每次运行结果相同
JavaRDD<Integer> sampled1 = rdd.sample(false, 0.5, 42);
JavaRDD<Integer> sampled2 = rdd.sample(false, 0.5, 42);
// sampled1与sampled2结果完全相同 |
| `sampleStdev` | 无 | `Double` | 计算样本标准差（n-1校正），适用于抽样数据 | // sampleStdev：样本标准差
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);
double sampleStdev = doubleRDD.sampleStdev();
// 样本标准差 = sqrt(sum((x-mean)^2)/(n-1))
// 用于抽样数据，消除偏差 |
| `sampleVariance` | 无 | `Double` | 计算样本方差（n-1校正），衡量抽样数据离散程度 | // sampleVariance：样本方差
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);
double sampleVar = doubleRDD.sampleVariance();
// 样本方差 = sum((x-mean)^2)/(n-1) |
| `setName` | name: String | `JavaDoubleRDD` | 设置RDD名称，用于调试和Spark UI显示 | // setName：设置RDD名称便于调试
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0));
doubleRDD.setName("my-double-rdd");
// 在Spark UI中显示此名称，便于追踪作业 |
| `stats` | 无 | `StatCounter` | 返回统计摘要(计数、均值、方差、最小、最大) | // stats：获取完整统计信息
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);

StatCounter stats = doubleRDD.stats();
System.out.println("Count: " + stats.count());      // 5
System.out.println("Mean: " + stats.mean());        // 3.0
System.out.println("Sum: " + stats.sum());          // 15.0
System.out.println("Min: " + stats.min());          // 1.0
System.out.println("Max: " + stats.max());          // 5.0
System.out.println("Stdev: " + stats.stdev());      // 1.41... |
| `stdev` | 无 | `Double` | 计算标准差 | // stdev：计算标准差
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);

double stdev = doubleRDD.stdev();
// 标准差 = 方差的平方根 |
| `subtract` | JavaDoubleRDD: other | `JavaDoubleRDD` | 返回当前RDD减去另一个RDD的差集，使用默认分区数 | // subtract(other)：取差集
JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));
JavaRDD<Integer> subtracted = rdd1.subtract(rdd2);
// 结果: [1, 2] (rdd1中不在rdd2的元素) |
| `subtract` | JavaDoubleRDD: other, numPartitions: Int | `JavaDoubleRDD` | 返回差集，指定结果分区数控制并行度 | // subtract(other, numPartitions)：指定分区数的差集
JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));
// 指定结果使用2个分区
JavaRDD<Integer> subtracted = rdd1.subtract(rdd2, 2); |
| `subtract` | JavaDoubleRDD: other, Partitioner: p | `JavaDoubleRDD` | 返回差集，使用自定义分区器控制数据分布 | // subtract(other, partitioner)：自定义分区器的差集
JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));
// 使用HashPartitioner
JavaRDD<Integer> subtracted = rdd1.subtract(rdd2, new HashPartitioner(4)); |
| `sum` | 无 | `Double` | 求和 | // sum：求和
List<Double> data = Arrays.asList(10.0, 20.0, 30.0, 40.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);

double total = doubleRDD.sum();
// 结果: 100.0 |
| `sumApprox` | timeout: Long, confidence: Double | `PartialResult` | 近似计算总和，在超时内返回带置信区间的近似结果 | // sumApprox(timeout, confidence)：近似求和
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);
// 1000ms超时，90%置信度
PartialResult<BoundedDouble> result = doubleRDD.sumApprox(1000, 0.90); |
| `sumApprox` | timeout: Long | `PartialResult` | 近似计算总和，仅指定超时时间，默认置信度0.95 | // sumApprox(timeout)：仅指定超时的近似求和
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(1.0, 2.0, 3.0));
PartialResult<BoundedDouble> result = doubleRDD.sumApprox(500); |
| `union` | JavaDoubleRDD: other | `JavaDoubleRDD` | 合并两个RDD，保留所有元素（包括重复），不触发shuffle | // union：合并RDD（保留重复）
JavaRDD<String> rdd1 = sc.parallelize(Arrays.asList("a", "b", "b"));
JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("c", "d"));
JavaRDD<String> unionRDD = rdd1.union(rdd2);
// 结果: ["a", "b", "b", "c", "d"] 注意重复元素保留 |
| `unpersist` | 无 | `JavaDoubleRDD` | 取消RDD持久化，非阻塞方式立即释放内存 | // unpersist()：非阻塞释放缓存
JavaRDD<String> rdd = sc.textFile("hdfs://file.txt");
rdd.cache();
rdd.count(); // 触发缓存
// 非阻塞释放，立即返回
rdd.unpersist(); |
| `unpersist` | blocking: Boolean | `JavaDoubleRDD` | 取消RDD持久化，可控制是否阻塞等待释放完成 | // unpersist(blocking)：可阻塞释放缓存
JavaRDD<String> rdd = sc.textFile("hdfs://file.txt");
rdd.cache();
rdd.count();
// blocking=true：等待释放完成后再返回
rdd.unpersist(true);
// blocking=false：非阻塞立即返回（默认）
rdd.unpersist(false); |
| `variance` | 无 | `Double` | 计算方差 | // variance：计算方差
List<Double> data = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(data);

double variance = doubleRDD.variance();
// 方差衡量数据的离散程度 |


### JavaPairRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 53

**导入示例**:
```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

// 从JavaRDD转换
JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(
    s -> new Tuple2<>(s, s.length())
);
```


| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaPairRDD` | 缓存RDD到内存，默认MEMORY_ONLY | // cache：缓存RDD到内存
JavaRDD<String> rdd = sc.textFile("hdfs://large/file.txt");

// 缓存后，后续操作会直接从内存读取
rdd.cache();

// 多次使用RDD时缓存可提升性能
long count1 = rdd.count();  // 第一次计算，会缓存
long count2 = rdd.count();  // 第二次直接从内存读取 |
| `coalesce` | numPartitions: Int | `JavaPairRDD` | 减少分区数，默认不触发shuffle，保持Key-Value映射 | // coalesce(numPartitions)：减少PairRDD分区
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("b", 2));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data, 10);
// 减少到2个分区
JavaPairRDD<String, Integer> coalesced = pairRDD.coalesce(2); |
| `coalesce` | numPartitions: Int, shuffle: Boolean | `JavaPairRDD` | 减少分区数，可控制shuffle。shuffle=true会重新分布KV数据 | // coalesce(numPartitions, shuffle)：PairRDD可shuffle合并
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data, 10);
// shuffle=true：KV数据重新均匀分布
JavaPairRDD<String, Integer> coalesced = pairRDD.coalesce(2, true); |
| `collectAsMap` | 无 | `java` | 收集RDD为Java Map | // collectAsMap：收集为Map
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("key1", 10),
    new Tuple2<>("key2", 20)
);
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);

Map<String, Integer> map = pairRDD.collectAsMap();
// 结果: {"key1": 10, "key2": 20}

// 注意：如果Key重复，只保留最后一个Value |
| `countApproxDistinctByKey` | relativeSD: Double, Partitioner: partitioner | `JavaPairRDD` | 近似统计每个Key的唯一Value数量，使用自定义分区器和相对标准偏差 | // countApproxDistinctByKey(relativeSD, partitioner)
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 1));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
// relativeSD=0.05表示5%误差率
JavaPairRDD<String, Long> approx = pairRDD.countApproxDistinctByKey(0.05, new HashPartitioner(2)); |
| `countApproxDistinctByKey` | relativeSD: Double, numPartitions: Int | `JavaPairRDD` | 近似统计每个Key的唯一Value数量，指定分区数 | // countApproxDistinctByKey(relativeSD, numPartitions)
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
JavaPairRDD<String, Long> approx = pairRDD.countApproxDistinctByKey(0.05, 4); |
| `countApproxDistinctByKey` | relativeSD: Double | `JavaPairRDD` | 近似统计每个Key的唯一Value数量，使用默认分区数 | // countApproxDistinctByKey(relativeSD)
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
JavaPairRDD<String, Long> approx = pairRDD.countApproxDistinctByKey(0.05); |
| `countByKey` | 无 | `java` | 统计每个Key的数量 | // countByKey：统计每个Key的数量
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("apple", 1),
    new Tuple2<>("banana", 2),
    new Tuple2<>("apple", 3),
    new Tuple2<>("apple", 4)
);
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);

Map<String, Long> counts = pairRDD.countByKey();
// 结果: {"apple": 3, "banana": 1} |
| `countByKeyApprox` | timeout: Long | `PartialResult` | 近似统计每个Key的数量，仅指定超时时间 | // countByKeyApprox(timeout)：近似Key计数
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 1));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
PartialResult<Map<String, BoundedDouble>> result = pairRDD.countByKeyApprox(1000); |
| `countByKeyApprox` | timeout: Long, 0.95: confidence | `PartialResult` | 近似统计每个Key的数量，指定超时和置信度 | // countByKeyApprox(timeout, confidence)
PartialResult<Map<String, BoundedDouble>> result = pairRDD.countByKeyApprox(1000, 0.90); |
| `distinct` | 无 | `JavaPairRDD` | 去除重复(K,V)键值对，使用默认分区数 | // distinct()：PairRDD去重
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("a", 1), new Tuple2<>("b", 2));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
JavaPairRDD<String, Integer> distinct = pairRDD.distinct();
// 结果: [("a", 1), ("b", 2)] |
| `distinct` | numPartitions: Int | `JavaPairRDD` | 去除重复键值对，指定结果分区数 | // distinct(numPartitions)：指定分区去重
JavaPairRDD<String, Integer> distinct = pairRDD.distinct(3); |
| `filter` | JFunction[(K: f | `void` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `foldByKey` | V: zeroValue, Partitioner: partitioner, JFunction2[V: func | `JavaPairRDD` | 按Key聚合，使用零值和自定义分区器，适用于需要初始值的聚合 | // foldByKey(zeroValue, partitioner, func)
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 3));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
// 使用零值0和HashPartitioner
JavaPairRDD<String, Integer> folded = pairRDD.foldByKey(0, new HashPartitioner(2), (a, b) -> a + b);
// 结果: [("a", 3), ("b", 3)] |
| `foldByKey` | V: zeroValue, numPartitions: Int, JFunction2[V: func | `JavaPairRDD` | 按Key聚合，使用零值并指定分区数 | // foldByKey(zeroValue, numPartitions, func)
JavaPairRDD<String, Integer> folded = pairRDD.foldByKey(0, 3, (a, b) -> a + b); |
| `foldByKey` | V: zeroValue, JFunction2[V: func | `JavaPairRDD` | 按Key聚合，使用零值和默认分区数，最常用形式 | // foldByKey(zeroValue, func)
JavaPairRDD<String, Integer> folded = pairRDD.foldByKey(0, (a, b) -> a + b);
// 注意：零值在每个分区的聚合开始时都会使用 |
| `groupByKey` | Partitioner: partitioner | `JavaPairRDD` | 按Key分组Value，使用自定义分区器控制数据分布 | // groupByKey(partitioner)：自定义分区器分组
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("fruit", 1), new Tuple2<>("fruit", 2), new Tuple2<>("vegetable", 3));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
// 使用HashPartitioner控制分布
JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey(new HashPartitioner(2));
// 注意：groupByKey可能导致数据倾斜，建议用reduceByKey替代 |
| `groupByKey` | numPartitions: Int | `JavaPairRDD` | 按Key分组Value，指定结果分区数控制并行度 | // groupByKey(numPartitions)：指定分区数分组
JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey(4); |
| `groupByKey` | 无 | `JavaPairRDD` | 按Key分组Value，使用默认分区数，最简形式 | // groupByKey()：默认分组
JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey();
// 结果: [("fruit", [1, 2]), ("vegetable", [3])] |
| `intersection` | JavaPairRDD[K: other | `JavaPairRDD` | 返回两个PairRDD的交集（相同Key和Value），使用默认分区 | // intersection：PairRDD取交集
List<Tuple2<String, Integer>> data1 = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("b", 2));
List<Tuple2<String, Integer>> data2 = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("c", 3));
JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
JavaPairRDD<String, Integer> intersect = rdd1.intersection(rdd2);
// 结果: [("a", 1)] 需Key和Value都相同 |
| `keys` | 无 | `JavaRDD` | 返回所有Key的RDD | // keys：获取所有Key
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("a", 1),
    new Tuple2<>("b", 2)
));

JavaRDD<String> keysRDD = pairRDD.keys();
// 结果: ["a", "b"] |
| `lookup` | K: key | `JList` | 查找指定Key的所有Value | // lookup：查找指定Key的所有Value
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("apple", 1),
    new Tuple2<>("apple", 2),
    new Tuple2<>("banana", 3)
);
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);

List<Integer> appleValues = pairRDD.lookup("apple");
// 结果: [1, 2] |
| `partitionBy` | Partitioner: partitioner | `JavaPairRDD` | 使用指定分区器重新分区，确保相同Key的数据在同一分区 | // partitionBy：使用分区器重新分布
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("c", 3));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
// 使用HashPartitioner，确保相同Key在同一分区
JavaPairRDD<String, Integer> partitioned = pairRDD.partitionBy(new HashPartitioner(4));
// 常用于join前的预分区，优化join性能 |
| `persist` | StorageLevel: newLevel | `JavaPairRDD` | 持久化RDD到指定存储级别 | // persist：持久化到指定存储级别
JavaRDD<String> rdd = sc.textFile("hdfs://data/file.txt");

// 内存+磁盘持久化
rdd.persist(StorageLevel.MEMORY_AND_DISK());

// 序列化存储（节省空间）
rdd.persist(StorageLevel.MEMORY_ONLY_SER());

// 堆外内存存储
rdd.persist(StorageLevel.OFF_HEAP()); |
| `reduceByKey` | Partitioner: partitioner, JFunction2[V: func | `JavaPairRDD` | 按Key聚合Value，使用自定义分区器控制分区数和数据分布 | // reduceByKey(partitioner, func)：自定义分区器聚合
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("apple", 1), new Tuple2<>("apple", 3), new Tuple2<>("banana", 2));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
JavaPairRDD<String, Integer> summed = pairRDD.reduceByKey(new HashPartitioner(2), (a, b) -> a + b);
// 结果: [("apple", 4), ("banana", 2)] |
| `reduceByKey` | JFunction2[V: func, numPartitions: Int | `JavaPairRDD` | 按Key聚合Value，指定分区数控制并行度 | // reduceByKey(func, numPartitions)：指定分区数
JavaPairRDD<String, Integer> summed = pairRDD.reduceByKey((a, b) -> a + b, 4);
// 使用4个分区进行聚合 |
| `reduceByKey` | JFunction2[V: func | `JavaPairRDD` | 按Key聚合Value，使用默认分区数，最常用形式，比groupByKey高效 | // reduceByKey(func)：默认聚合
JavaPairRDD<String, Integer> summed = pairRDD.reduceByKey((a, b) -> a + b);
// 注意：map端预聚合，比groupByKey更高效 |
| `reduceByKeyLocally` | JFunction2[V: func | `java` | 按Key聚合Value并返回本地Map，不触发shuffle，适合小数据集 | // reduceByKeyLocally：本地聚合返回Map
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
Map<String, Integer> localMap = pairRDD.reduceByKeyLocally((a, b) -> a + b);
// 直接返回Java Map，不需要collect |
| `repartition` | numPartitions: Int | `JavaPairRDD` | 重新分区，增加或减少分区数，触发shuffle | // repartition：重新分区（会shuffle）
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 2);  // 2个分区

// 增加到10个分区（触发shuffle）
JavaRDD<String> repartitioned = rdd.repartition(10);

// 注意：repartition会shuffle，coalesce只减少分区不shuffle |
| `repartitionAndSortWithinPartitions` | Partitioner: partitioner | `JavaPairRDD` | 重新分区并在每个分区内按Key排序，适用于范围查询优化 | // repartitionAndSortWithinPartitions(partitioner)：分区排序
List<Tuple2<Integer, String>> data = Arrays.asList(
    new Tuple2<>(1, "a"), new Tuple2<>(2, "b"), new Tuple2<>(1, "c"));
JavaPairRDD<Integer, String> pairRDD = sc.parallelizePairs(data);
JavaPairRDD<Integer, String> sorted = pairRDD.repartitionAndSortWithinPartitions(new HashPartitioner(2));
// 每个分区内部已排序，优化后续范围查询 |
| `repartitionAndSortWithinPartitions` | Partitioner: partitioner, Comparator[K]: comp | `JavaPairRDD` | 重新分区并在分区内使用自定义比较器排序 | // repartitionAndSortWithinPartitions(partitioner, comp)
JavaPairRDD<Integer, String> sorted = pairRDD.repartitionAndSortWithinPartitions(
    new HashPartitioner(2), Comparator.reverseOrder());
// 使用自定义比较器在分区内降序排序 |
| `sample` | withReplacement: Boolean, fraction: Double | `JavaPairRDD` | 随机采样PairRDD，fraction为期望比例 | // sample(withReplacement, fraction)：PairRDD采样
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("b", 2));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
JavaPairRDD<String, Integer> sampled = pairRDD.sample(false, 0.5); |
| `sample` | withReplacement: Boolean, fraction: Double, seed: Long | `JavaPairRDD` | 随机采样PairRDD，指定种子确保结果可重现 | // sample(withReplacement, fraction, seed)：可重现采样
JavaPairRDD<String, Integer> sampled1 = pairRDD.sample(false, 0.5, 42);
JavaPairRDD<String, Integer> sampled2 = pairRDD.sample(false, 0.5, 42);
// 相同种子，相同结果 |
| `sampleByKey` | withReplacement: Boolean, java.util.Map[K: fractions, seed: Long | `JavaPairRDD` | 按Key分层采样，每个Key使用不同采样比例，可重现 | // sampleByKey：分层采样
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("b", 2), new Tuple2<>("c", 3));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
Map<String, Double> fractions = new HashMap<>();
fractions.put("a", 0.5);  // Key "a" 采样50%
fractions.put("b", 1.0);  // Key "b" 全采样
JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKey(false, fractions, 42L); |
| `sampleByKey` | withReplacement: Boolean, java.util.Map[K: fractions | `JavaPairRDD` | 按Key分层采样，每个Key使用不同比例 | // sampleByKey：分层采样（无种子）
JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKey(false, fractions); |
| `sampleByKeyExact` | withReplacement: Boolean, java.util.Map[K: fractions, seed: Long | `JavaPairRDD` | 按Key精确分层采样，确保每个Key采样精确数量 | // sampleByKeyExact：精确分层采样
JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKeyExact(false, fractions, 42L);
// 确保每个Key采样数量精确符合比例 |
| `sampleByKeyExact` | withReplacement: Boolean, java.util.Map[K: fractions | `JavaPairRDD` | 按Key精确分层采样，无种子 | // sampleByKeyExact：精确采样（无种子）
JavaPairRDD<String, Integer> sampled = pairRDD.sampleByKeyExact(false, fractions); |
| `saveAsHadoopDataset` | JobConf: conf | `void` | 使用旧版Hadoop API保存RDD到Hadoop输出格式 | // saveAsHadoopDataset：旧版Hadoop API保存
JobConf jobConf = new JobConf();
jobConf.setOutputFormatClass(TextOutputFormat.class);
jobConf.setOutputKeyClass(String.class);
jobConf.setOutputValueClass(Integer.class);
pairRDD.saveAsHadoopDataset(jobConf); |
| `saveAsNewAPIHadoopDataset` | Configuration: conf | `void` | 使用新版Hadoop API保存RDD到Hadoop输出格式 | // saveAsNewAPIHadoopDataset：新版Hadoop API保存
Configuration conf = new Configuration();
conf.set("mapreduce.output.fileoutputformat.outputdir", "/output");
pairRDD.saveAsNewAPIHadoopDataset(conf); |
| `setName` | name: String | `JavaPairRDD` | 设置PairRDD名称，用于调试和Spark UI显示 | // setName：设置名称便于调试
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
pairRDD.setName("user-clicks-pair-rdd");
// Spark UI中显示此名称 |
| `sortByKey` | 无 | `JavaPairRDD` | 按Key升序排序，使用默认分区数 | // sortByKey()：默认升序排序
List<Tuple2<String, Integer>> data = Arrays.asList(
    new Tuple2<>("c", 3), new Tuple2<>("a", 1), new Tuple2<>("b", 2));
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(data);
JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey();
// 结果: [("a", 1), ("b", 2), ("c", 3)] |
| `sortByKey` | ascending: Boolean | `JavaPairRDD` | 按Key排序，控制升序或降序 | // sortByKey(ascending)：控制排序方向
JavaPairRDD<String, Integer> sortedAsc = pairRDD.sortByKey(true);  // 升序
JavaPairRDD<String, Integer> sortedDesc = pairRDD.sortByKey(false);  // 降序 |
| `sortByKey` | ascending: Boolean, numPartitions: Int | `JavaPairRDD` | 按Key排序，控制排序方向和分区数 | // sortByKey(ascending, numPartitions)：指定分区数
JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(true, 4);
// 升序，使用4个分区 |
| `sortByKey` | Comparator[K]: comp | `JavaPairRDD` | 按Key排序，使用自定义比较器 | // sortByKey(comp)：自定义比较器
JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(Comparator.reverseOrder());
// 使用降序比较器 |
| `sortByKey` | Comparator[K]: comp, ascending: Boolean | `JavaPairRDD` | 按Key排序，使用自定义比较器和排序方向 | // sortByKey(comp, ascending)：比较器+方向
Comparator<String> customComp = (a, b) -> b.compareTo(a);
JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(customComp, true); |
| `sortByKey` | Comparator[K]: comp, ascending: Boolean, numPartitions: Int | `JavaPairRDD` | 按Key排序，完全自定义：比较器、方向和分区数 | // sortByKey(comp, ascending, numPartitions)：完全自定义
JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey(customComp, true, 4); |
| `subtract` | JavaPairRDD[K: other | `JavaPairRDD` | 返回PairRDD差集（Key+Value都需匹配），使用默认分区 | // subtract(other)：PairRDD差集
List<Tuple2<String, Integer>> data1 = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("b", 2));
List<Tuple2<String, Integer>> data2 = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("c", 3));
JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
JavaPairRDD<String, Integer> diff = rdd1.subtract(rdd2);
// 结果: [("b", 2)] |
| `subtract` | JavaPairRDD[K: other, numPartitions: Int | `JavaPairRDD` | 返回PairRDD差集，指定分区数 | // subtract(other, numPartitions)
JavaPairRDD<String, Integer> diff = rdd1.subtract(rdd2, 4); |
| `subtract` | JavaPairRDD[K: other, Partitioner: p | `JavaPairRDD` | 返回PairRDD差集，使用自定义分区器 | // subtract(other, partitioner)
JavaPairRDD<String, Integer> diff = rdd1.subtract(rdd2, new HashPartitioner(2)); |
| `union` | JavaPairRDD[K: other | `JavaPairRDD` | 合并两个PairRDD，保留所有键值对（含重复） | // union：合并PairRDD
List<Tuple2<String, Integer>> data1 = Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("a", 1));
List<Tuple2<String, Integer>> data2 = Arrays.asList(
    new Tuple2<>("b", 2));
JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(data1);
JavaPairRDD<String, Integer> rdd2 = sc.parallelizePairs(data2);
JavaPairRDD<String, Integer> unionRDD = rdd1.union(rdd2);
// 结果: [("a", 1), ("a", 1), ("b", 2)] 保留重复 |
| `unpersist` | 无 | `JavaPairRDD` | 取消PairRDD持久化，非阻塞方式 | // unpersist()：非阻塞释放
pairRDD.cache();
pairRDD.unpersist(); |
| `unpersist` | blocking: Boolean | `JavaPairRDD` | 取消PairRDD持久化，可阻塞等待 | // unpersist(blocking)
pairRDD.unpersist(true);  // 阻塞等待完成 |
| `values` | 无 | `JavaRDD` | 返回所有Value的RDD | // values：获取所有Value
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("a", 1),
    new Tuple2<>("b", 2)
));

JavaRDD<Integer> valuesRDD = pairRDD.values();
// 结果: [1, 2] |


### JavaRDD
**包路径**: `org.apache.spark.api.java`
**方法数量**: 22

**导入示例**:
```java
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// 从SparkContext获取
JavaRDD<String> rdd = sc.textFile("hdfs://data.txt");
```


| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cache` | 无 | `JavaRDD` | 缓存RDD到内存，默认MEMORY_ONLY | // cache：缓存RDD到内存
JavaRDD<String> rdd = sc.textFile("hdfs://large/file.txt");

// 缓存后，后续操作会直接从内存读取
rdd.cache();

// 多次使用RDD时缓存可提升性能
long count1 = rdd.count();  // 第一次计算，会缓存
long count2 = rdd.count();  // 第二次直接从内存读取 |
| `coalesce` | numPartitions: Int | `JavaRDD` | 减少分区数，不触发shuffle | // coalesce：减少分区数（不shuffle）
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);  // 10个分区

// 减少到2个分区（不触发shuffle，高效）
JavaRDD<String> coalesced = rdd.coalesce(2); |
| `coalesce` | numPartitions: Int, shuffle: Boolean | `JavaRDD` | 减少分区数，不触发shuffle | // coalesce：减少分区数（不shuffle）
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"), 10);  // 10个分区

// 减少到2个分区（不触发shuffle，高效）
JavaRDD<String> coalesced = rdd.coalesce(2); |
| `distinct` | 无 | `JavaRDD` | 去重 | // distinct：去重
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));
JavaRDD<Integer> distinct = rdd.distinct();
// 结果: [1, 2, 3, 4, 5] |
| `distinct` | numPartitions: Int | `JavaRDD` | 去重 | // distinct：去重
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 3, 5));
JavaRDD<Integer> distinct = rdd.distinct();
// 结果: [1, 2, 3, 4, 5] |
| `filter` | JFunction[T: f | `JavaRDD` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `getResourceProfile` | 无 | `ResourceProfile` | 获取RDD的资源配置，用于资源隔离管理 | // getResourceProfile：获取资源配置
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b"));
ResourceProfile profile = rdd.getResourceProfile();
// 查看RDD使用的CPU/内存资源配置 |
| `intersection` | JavaRDD[T]: other | `JavaRDD` | 返回两个RDD的交集 | // intersection：取交集
JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2, 3, 4));
JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(3, 4, 5, 6));

JavaRDD<Integer> intersection = rdd1.intersection(rdd2);
// 结果: [3, 4] |
| `persist` | StorageLevel: newLevel | `JavaRDD` | 持久化RDD到指定存储级别 | // persist：持久化到指定存储级别
JavaRDD<String> rdd = sc.textFile("hdfs://data/file.txt");

// 内存+磁盘持久化
rdd.persist(StorageLevel.MEMORY_AND_DISK());

// 序列化存储（节省空间）
rdd.persist(StorageLevel.MEMORY_ONLY_SER());

// 堆外内存存储
rdd.persist(StorageLevel.OFF_HEAP()); |
| `randomSplit` | Array[Double]: weights | `Array` | 按权重随机分割RDD为多个RDD，用于数据集划分（如训练/测试） | // randomSplit(weights)：按权重随机分割
JavaRDD<Integer> data = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
// 60%训练集，40%测试集
JavaRDD<Integer>[] splits = data.randomSplit(new double[]{0.6, 0.4});
JavaRDD<Integer> trainData = splits[0];
JavaRDD<Integer> testData = splits[1]; |
| `randomSplit` | Array[Double]: weights, seed: Long | `Array` | 按权重随机分割，指定种子确保每次分割结果一致 | // randomSplit(weights, seed)：可重现分割
JavaRDD<Integer>[] splits1 = data.randomSplit(new double[]{0.6, 0.4}, 42L);
JavaRDD<Integer>[] splits2 = data.randomSplit(new double[]{0.6, 0.4}, 42L);
// splits1和splits2分割结果完全相同 |
| `setName` | name: String | `JavaRDD` | 设置RDD名称，用于Spark UI调试和监控 | // setName：设置RDD名称
JavaRDD<String> rdd = sc.textFile("hdfs://data.txt");
rdd.setName("raw-input-data");
// 在Spark UI中显示此名称便于调试 |
| `withResources` | ResourceProfile: rp | `JavaRDD` | 设置RDD的资源配置，用于资源隔离和精细控制 | // withResources：设置资源配置
ResourceProfile profile = new ResourceProfileBuilder().requireCores(2).build();
JavaRDD<String> rddWithResources = rdd.withResources(profile);
// 此RDD执行时使用指定的资源 |


### JavaRDDLike (核心接口)
**包路径**: `org.apache.spark.api.java`
**说明**: JavaRDD、JavaPairRDD、JavaDoubleRDD共同继承的接口，包含最常用的RDD操作方法。
**方法数量**: 50+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `map` | Function<T, R> f | `JavaRDD[R]` | 对每个元素应用函数，一对一转换 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
JavaRDD<Integer> doubled = nums.map(x -> x * 2);
// 结果: [2, 4, 6]` |
| `mapToPair` | PairFunction[T, K, V] f | `JavaPairRDD[K, V]` | 将元素转换为键值对 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("apple", "banana"));
JavaPairRDD<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, w.length()));
// 结果: [("apple", 5), ("banana", 6)]` |
| `mapToDouble` | DoubleFunction[T] f | `JavaDoubleRDD` | 将元素转换为Double值 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
JavaDoubleRDD sqrt = nums.mapToDouble(x -> Math.sqrt(x));` |
| `flatMap` | FlatMapFunction[T, U] f | `JavaRDD[U]` | 将每个元素映射为多个输出元素 | `JavaRDD<String> lines = sc.parallelize(Arrays.asList("hello world", "spark java"));
JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
// 结果: ["hello", "world", "spark", "java"]` |
| `flatMapToPair` | PairFlatMapFunction[T, K, V] f | `JavaPairRDD[K, V]` | 将每个元素映射为多个键值对 | `JavaRDD<String> lines = sc.parallelize(Arrays.asList("a b", "c d"));
JavaPairRDD<String, Integer> pairs = lines.flatMapToPair(line -> {
    List<Tuple2<String, Integer>> result = new ArrayList<>();
    for (String w : line.split(" ")) {
        result.add(new Tuple2<>(w, 1));
    }
    return result.iterator();
});` |
| `mapPartitions` | FlatMapFunction[Iterator<T>, U] f | `JavaRDD[U]` | 对每个分区应用函数，适合批量处理 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
JavaRDD<Integer> partitionSum = nums.mapPartitions(iter -> {
    int sum = 0;
    while (iter.hasNext()) sum += iter.next();
    return Arrays.asList(sum).iterator();
});
// 结果: [3, 7] (分区0:1+2=3, 分区1:3+4=7)` |
| `mapPartitionsWithIndex` | Function2<Integer, Iterator<T>, Iterator<R>> f | `JavaRDD[R]` | 对每个分区应用函数，带分区索引 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
JavaRDD<String> indexed = nums.mapPartitionsWithIndex((idx, iter) -> {
    List<String> result = new ArrayList<>();
    while (iter.hasNext()) result.add("Partition " + idx + ": " + iter.next());
    return result.iterator();
});` |
| `glom` | 无 | `JavaRDD[List<T>]` | 将每个分区的元素合并为一个List | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
JavaRDD<List<Integer>> partitions = nums.glom();
// 结果: [[1, 2], [3, 4]]` |
| `collect` | 无 | `List<T>` | 将RDD所有元素收集到Driver端，返回Java List | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
List<Integer> list = nums.collect();
// 注意：数据量大时可能导致Driver内存溢出` |
| `collectPartitions` | Array[Int] partitionIds | `Array[List<T>]` | 收集指定分区的元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5), 3);
List<Integer>[] parts = nums.collectPartitions(new int[]{0, 2});
// 只收集分区0和分区2` |
| `toLocalIterator` | 无 | `Iterator<T>` | 返回本地迭代器，逐分区拉取数据 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
Iterator<Integer> iter = nums.toLocalIterator();
while (iter.hasNext()) {
    System.out.println(iter.next());
}` |
| `foreach` | VoidFunction[T] f | `void` | 对每个元素执行操作（不返回结果） | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
nums.foreach(x -> System.out.println("Value: " + x));` |
| `foreachPartition` | VoidFunction[Iterator<T>] f | `void` | 对每个分区的迭代器执行操作 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 2);
nums.foreachPartition(iter -> {
    int sum = 0;
    while (iter.hasNext()) sum += iter.next();
    System.out.println("Partition sum: " + sum);
});` |
| `reduce` | Function2<T, T, T> f | `T` | 使用函数聚合所有元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4));
int sum = nums.reduce((a, b) -> a + b);
// 结果: 10` |
| `treeReduce` | Function2<T, T, T> f, depth: Int | `T` | 树形聚合，减少Driver负载 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
int sum = nums.treeReduce((a, b) -> a + b, 2);
// 深度为2的树形聚合` |
| `fold` | zeroValue: T, Function2<T, T, T> f | `T` | 使用零值进行聚合 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
int sum = nums.fold(0, (a, b) -> a + b);
// 结果: 6` |
| `aggregate` | zeroValue: U, seqOp: Function2<U, T, U>, combOp: Function2<U, U, U> | `U` | 使用不同类型的零值进行聚合 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4));
Tuple2<Integer, Integer> result = nums.aggregate(
    new Tuple2<>(0, 0),  // (sum, count)
    (acc, x) -> new Tuple2<>(acc._1 + x, acc._2 + 1),  // 分区内聚合
    (acc1, acc2) -> new Tuple2<>(acc1._1 + acc2._1, acc1._2 + acc2._2)  // 分区间合并
);
// 结果: (10, 4)` |
| `treeAggregate` | zeroValue: U, seqOp, combOp, depth: Int | `U` | 树形聚合，减少Driver内存压力 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));
int sum = nums.treeAggregate(0, (a, b) -> a + b, (a, b) -> a + b, 2);` |
| `count` | 无 | `Long` | 计算元素总数 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4));
long total = nums.count();
// 结果: 4` |
| `countApprox` | timeout: Long, confidence: Double | `PartialResult[BoundedDouble]` | 近似计数，在超时内返回带置信区间结果 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
PartialResult<BoundedDouble> approx = nums.countApprox(1000, 0.95);` |
| `countByValue` | 无 | `Map<T, Long>` | 统计每个值的出现次数 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 1));
Map<Integer, Long> counts = nums.countByValue();
// 结果: {1: 3, 2: 2, 3: 1}` |
| `countApproxDistinct` | relativeSD: Double | `Long` | 近似统计唯一值数量（HyperLogLog算法） | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 1, 3, 2, 4, 5, 6, 7));
long distinct = nums.countApproxDistinct(0.05);
// relativeSD=0.05表示5%误差率` |
| `take` | num: Int | `List<T>` | 获取前n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
List<Integer> first3 = nums.take(3);
// 结果: [1, 2, 3]` |
| `takeSample` | withReplacement: Boolean, num: Int, seed: Long | `List<T>` | 随机采样n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
List<Integer> sample = nums.takeSample(false, 3, 42L);
// 不重复采样3个元素` |
| `first` | 无 | `T` | 获取第一个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
int first = nums.first();
// 结果: 1` |
| `top` | num: Int, comp: Comparator[T] | `List<T>` | 获取最大的n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 5, 3, 8, 2));
List<Integer> top3 = nums.top(3, Comparator.naturalOrder());
// 结果: [8, 5, 3]` |
| `takeOrdered` | num: Int, comp: Comparator[T] | `List<T>` | 获取最小的n个元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(5, 1, 3, 8, 2));
List<Integer> smallest3 = nums.takeOrdered(3, Comparator.naturalOrder());
// 结果: [1, 2, 3]` |
| `max` | comp: Comparator[T] | `T` | 获取最大元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 5, 3, 8, 2));
int max = nums.max(Comparator.naturalOrder());
// 结果: 8` |
| `min` | comp: Comparator[T] | `T` | 获取最小元素 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(5, 1, 3, 8, 2));
int min = nums.min(Comparator.naturalOrder());
// 结果: 1` |
| `isEmpty` | 无 | `Boolean` | 判断RDD是否为空 | `JavaRDD<Integer> empty = sc.emptyRDD();
boolean emptyFlag = empty.isEmpty();
// 结果: true` |
| `groupBy` | JFunction[T, U] f | `JavaPairRDD[U, Iterable<T>]` | 按函数结果分组 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6));
JavaPairRDD<Boolean, Iterable<Integer>> grouped = nums.groupBy(x -> x % 2 == 0);
// 结果: {false: [1, 3, 5], true: [2, 4, 6]}` |
| `keyBy` | JFunction[T, U] f | `JavaPairRDD[U, T]` | 将元素转换为键值对，原值为Value | `JavaRDD<String> words = sc.parallelize(Arrays.asList("apple", "banana"));
JavaPairRDD<Integer, String> keyed = words.keyBy(w -> w.length());
// 结果: [(5, "apple"), (6, "banana")]` |
| `cartesian` | JavaRDDLike[U, _] other | `JavaPairRDD[T, U]` | 计算两个RDD的笛卡尔积 | `JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2));
JavaRDD<String> rdd2 = sc.parallelize(Arrays.asList("a", "b"));
JavaPairRDD<Integer, String> cartesian = rdd1.cartesian(rdd2);
// 结果: [(1, "a"), (1, "b"), (2, "a"), (2, "b")]` |
| `zip` | JavaRDDLike[U, _] other | `JavaPairRDD[T, U]` | 将两个RDD按位置配对 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
JavaRDD<String> chars = sc.parallelize(Arrays.asList("a", "b", "c"));
JavaPairRDD<Integer, String> zipped = nums.zip(chars);
// 结果: [(1, "a"), (2, "b"), (3, "c")]` |
| `zipPartitions` | JavaRDDLike[U, _] other, FlatMapFunction2[Iterator<T>, JIterator[U], V] f | `JavaRDD[V]` | 对两个RDD的分区进行配对处理 | `JavaRDD<Integer> rdd1 = sc.parallelize(Arrays.asList(1, 2), 1);
JavaRDD<Integer> rdd2 = sc.parallelize(Arrays.asList(10, 20), 1);
JavaRDD<Integer> sums = rdd1.zipPartitions(rdd2, (iter1, iter2) -> {
    List<Integer> result = new ArrayList<>();
    while (iter1.hasNext() && iter2.hasNext()) {
        result.add(iter1.next() + iter2.next());
    }
    return result.iterator();
});` |
| `zipWithIndex` | 无 | `JavaPairRDD[T, Long]` | 为每个元素添加索引 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));
JavaPairRDD<String, Long> indexed = words.zipWithIndex();
// 结果: [("a", 0), ("b", 1), ("c", 2)]` |
| `zipWithUniqueId` | 无 | `JavaPairRDD[T, Long]` | 为每个元素生成唯一ID（不保证连续） | `JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"), 2);
JavaPairRDD<String, Long> uid = words.zipWithUniqueId();
// 结果如: [("a", 0), ("b", 1), ("c", 4)]` |
| `pipe` | command: String | `JavaRDD[String]` | 通过外部程序处理RDD元素 | `JavaRDD<String> data = sc.parallelize(Arrays.asList("1", "2", "3"));
JavaRDD<String> piped = data.pipe("cat");
// 将每个元素通过cat命令处理` |
| `pipe` | List<String> command, Map<String, String> env | `JavaRDD[String]` | 通过外部程序处理，带环境变量 | `List<String> cmd = Arrays.asList("awk", "{print $1*2}");
Map<String, String> env = new HashMap<>();
env.put("LC_ALL", "C");
JavaRDD<String> result = data.pipe(cmd, env);` |
| `saveAsTextFile` | path: String | `void` | 保存RDD为文本文件 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("hello", "world"));
words.saveAsTextFile("hdfs://output/path");` |
| `saveAsTextFile` | path: String, codec: Class[_ <: CompressionCodec] | `void` | 保存为压缩文本文件 | `JavaRDD<String> words = sc.parallelize(Arrays.asList("hello", "world"));
words.saveAsTextFile("hdfs://output/path", GzipCodec.class);` |
| `saveAsObjectFile` | path: String | `void` | 保存为序列化对象文件 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
nums.saveAsObjectFile("hdfs://output/nums");` |
| `checkpoint` | 无 | `void` | 标记RDD进行checkpoint | `sc.setCheckpointDir("hdfs://checkpoint/dir");
JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3));
nums.checkpoint();  // 后续计算会保存到checkpoint目录` |
| `isCheckpointed` | 无 | `Boolean` | 判断是否已checkpoint | `boolean checked = nums.isCheckpointed();` |
| `getCheckpointFile` | 无 | `Optional[String]` | 获取checkpoint文件路径 | `Optional<String> file = nums.getCheckpointFile();` |
| `getNumPartitions` | 无 | `Int` | 获取分区数 | `JavaRDD<Integer> nums = sc.parallelize(Arrays.asList(1, 2, 3, 4), 4);
int partitions = nums.getNumPartitions();
// 结果: 4` |
| `partitions` | 无 | `JList[Partition]` | 获取所有分区对象 | `List<Partition> parts = nums.partitions();` |
| `partitioner` | 无 | `Optional[Partitioner]` | 获取分区器（如果有） | `JavaPairRDD<String, Integer> pairs = ...;
Optional<Partitioner> partitioner = pairs.partitioner();` |
| `id` | 无 | `Int` | 获取RDD唯一ID | `int rddId = nums.id();` |
| `name` | 无 | `String` | 获取RDD名称 | `String rddName = nums.name();` |
| `getStorageLevel` | 无 | `StorageLevel` | 获取当前存储级别 | `StorageLevel level = nums.getStorageLevel();` |
| `toDebugString` | 无 | `String` | 获取RDD的血缘关系字符串 | `String lineage = nums.toDebugString();
// 显示RDD如何从父RDD计算而来` |
| `context` | 无 | `SparkContext` | 获取SparkContext | `SparkContext sc = nums.context();` |
| `rdd` | 无 | `RDD[T]` | 获取底层Scala RDD | `RDD<Integer> scalaRdd = nums.rdd();` |
| `countAsync` | 无 | `JavaFutureAction[Long]` | 异步计数 | `JavaFutureAction<Long> future = nums.countAsync();
Long count = future.get();  // 阻塞等待结果` |
| `collectAsync` | 无 | `JavaFutureAction[List<T>]` | 异步collect | `JavaFutureAction<List<Integer>> future = nums.collectAsync();
List<Integer> result = future.get();` |
| `takeAsync` | num: Int | `JavaFutureAction[List<T>]` | 异步take | `JavaFutureAction<List<Integer>> future = nums.takeAsync(5);
List<Integer> first5 = future.get();` |
| `foreachAsync` | VoidFunction[T] f | `JavaFutureAction[Void]` | 异步foreach | `JavaFutureAction<Void> future = nums.foreachAsync(x -> System.out.println(x));
future.get();  // 等待完成` |


### JavaSparkContext
**包路径**: `org.apache.spark.api.java`
**方法数量**: 45

**导入示例**:
```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// 创建SparkContext
SparkConf conf = new SparkConf()
    .setAppName("MyApp")
    .setMaster("local[*]");
JavaSparkContext sc = new JavaSparkContext(conf);
```


| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addFile` | path: String | `void` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业
sc.addFile("hdfs://path/to/config.txt");
sc.addFile("s3://bucket/data.json");

// 在Executor中访问文件
String filePath = SparkFiles.get("config.txt"); |
| `addFile` | path: String, recursive: Boolean | `void` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业
sc.addFile("hdfs://path/to/config.txt");
sc.addFile("s3://bucket/data.json");

// 在Executor中访问文件
String filePath = SparkFiles.get("config.txt"); |
| `addJar` | path: String | `void` | 添加JAR包到Spark作业 | // 添加依赖JAR包
sc.addJar("hdfs://path/to/dependency.jar");
sc.addJar("/local/path/to/lib.jar"); |
| `addJobTag` | tag: String | `void` | 为当前作业添加标签，用于作业追踪和取消 | // addJobTag：添加作业标签
sc.addJobTag("ml-training");
// 可以通过标签取消相关作业
sc.cancelJobsWithTag("ml-training"); |
| `binaryFiles` | path: String, minPartitions: Int | `JavaPairRDD` | 读取二进制文件目录，返回(文件路径,PortableDataStream)，指定最小分区数 | // binaryFiles(path, minPartitions)：指定分区数
JavaPairRDD<String, PortableDataStream> binaryRDD = sc.binaryFiles("hdfs://images/", 10);
JavaRDD<byte[]> dataRDD = binaryRDD.map(tuple -> tuple._2().toArray()); |
| `binaryFiles` | path: String | `JavaPairRDD` | 读取二进制文件目录，返回(文件路径,PortableDataStream)，使用默认分区 | // binaryFiles(path)：读取二进制文件
JavaPairRDD<String, PortableDataStream> binaryRDD = sc.binaryFiles("hdfs://binary/dir/");
// 适合处理图片、视频等二进制数据 |
| `binaryRecords` | path: String, recordLength: Int | `JavaRDD` | 读取固定长度的二进制记录文件，每条记录为byte[] | // binaryRecords：读取固定长度二进制记录
// 每条记录固定100字节
JavaRDD<byte[]> records = sc.binaryRecords("hdfs://data.bin", 100);
// 适合处理固定格式的二进制数据 |
| `cancelAllJobs` | 无 | `void` | 取消所有正在运行的作业，紧急停止使用 | // cancelAllJobs：取消所有作业
sc.cancelAllJobs();
// 立即取消所有正在执行的任务 |
| `cancelJobGroup` | groupId: String, reason: String | `void` | 取消指定作业组，并指定取消原因 | // cancelJobGroup(groupId, reason)
sc.setJobGroup("etl-job", "ETL processing");
sc.cancelJobGroup("etl-job", "User requested stop"); |
| `cancelJobGroup` | groupId: String | `void` | 取消指定作业组，不指定原因 | // cancelJobGroup(groupId)
sc.cancelJobGroup("etl-job"); |
| `cancelJobsWithTag` | tag: String, reason: String | `void` | 取消带指定标签的作业，并说明原因 | // cancelJobsWithTag(tag, reason)
sc.addJobTag("batch-processing");
sc.cancelJobsWithTag("batch-processing", "Resource limit reached"); |
| `cancelJobsWithTag` | tag: String | `void` | 取消带指定标签的作业 | // cancelJobsWithTag(tag)
sc.cancelJobsWithTag("batch-processing"); |
| `clearCallSite` | 无 | `void` | 清除调用点信息，用于调试追踪 | // clearCallSite：清除调用点
sc.clearCallSite();
// 清除Spark UI中显示的调用位置信息 |
| `clearJobGroup` | 无 | `void` | 清除当前作业组设置 | // clearJobGroup：清除作业组
sc.clearJobGroup();
// 后续作业不再属于之前设置的作业组 |
| `clearJobTags` | 无 | `void` | 清除所有作业标签 | // clearJobTags：清除所有标签
sc.clearJobTags(); |
| `getJobTags` | 无 | `util` | 获取当前作业的所有标签 | // getJobTags：获取作业标签
Set<String> tags = sc.getJobTags(); |
| `getLocalProperty` | key: String | `String` | 获取本地线程属性，用于任务上下文传递 | // getLocalProperty：获取本地属性
String value = sc.getLocalProperty("spark.task.queue"); |
| `getSparkHome` | 无 | `Optional` | 获取Spark安装目录路径 | // getSparkHome：获取Spark安装目录
Optional<String> sparkHome = sc.getSparkHome();
if (sparkHome.isPresent()) {
    System.out.println("Spark home: " + sparkHome.get());
} |
| `hadoopConfiguration` | 无 | `Configuration` | 获取Hadoop配置对象，用于访问HDFS设置 | // hadoopConfiguration：获取Hadoop配置
Configuration hadoopConf = sc.hadoopConfiguration();
hadoopConf.set("dfs.replication", "3");
// 直接修改Hadoop配置 |
| `jarOfClass` | Class[_]: cls | `Array` | 获取包含指定类的JAR路径，用于依赖分发 | // jarOfClass：获取类所在JAR
String[] jars = sc.jarOfClass(MyApp.class);
// 用于自动分发包含主类的JAR |
| `jarOfObject` | AnyRef: obj | `Array` | 获取包含指定对象的JAR路径 | // jarOfObject：获取对象所在JAR
String[] jars = sc.jarOfObject(new MyClass()); |
| `parallelizeDoubles` | java.util.List[Double]: list, numSlices: Int | `JavaDoubleRDD` | 从Java List创建DoubleRDD，指定分区数 | // parallelizeDoubles(list, numSlices)：指定分区数
List<Double> doubles = Arrays.asList(1.0, 2.0, 3.0, 4.0, 5.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(doubles, 3);
// 使用3个分区 |
| `parallelizeDoubles` | java.util.List[Double]: list | `JavaDoubleRDD` | 从Java List创建DoubleRDD，使用默认分区 | // parallelizeDoubles(list)：创建DoubleRDD
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(doubles);
double mean = doubleRDD.mean();
double sum = doubleRDD.sum(); |
| `removeJobTag` | tag: String | `void` | 移除指定作业标签 | // removeJobTag：移除标签
sc.addJobTag("batch");
sc.removeJobTag("batch"); |
| `setCallSite` | site: String | `void` | 设置调用点信息，用于调试追踪 | // setCallSite：设置调用点
sc.setCallSite("MyApp.main:数据处理阶段");
// Spark UI会显示此信息 |
| `setInterruptOnCancel` | interruptOnCancel: Boolean | `void` | 设置取消作业时是否中断线程 | // setInterruptOnCancel：设置取消中断行为
sc.setInterruptOnCancel(true);
// 取消作业时中断任务线程 |
| `setJobDescription` | value: String | `void` | 设置作业描述，用于Spark UI显示 | // setJobDescription：设置作业描述
sc.setJobDescription("Daily ETL batch job");
// Spark UI中显示作业描述 |
| `setJobGroup` | groupId: String, description: String, interruptOnCancel: Boolean | `void` | 设置作业组，指定ID、描述和取消中断行为 | // setJobGroup(id, desc, interrupt)
sc.setJobGroup("etl-group", "ETL processing", true);
// 可以通过groupId取消整组作业 |
| `setJobGroup` | groupId: String, description: String | `void` | 设置作业组，不指定中断行为 | // setJobGroup(id, desc)
sc.setJobGroup("ml-training", "ML model training"); |
| `setLocalProperty` | key: String, value: String | `void` | 设置本地线程属性，用于任务上下文传递 | // setLocalProperty：设置本地属性
sc.setLocalProperty("spark.task.queue", "high-priority");
// 任务可以读取此属性 |
| `setLogLevel` | logLevel: String | `void` | 设置SparkContext日志级别（ALL, DEBUG, INFO, WARN, ERROR, OFF） | // setLogLevel：设置日志级别
sc.setLogLevel("WARN");  // 只显示警告和错误
sc.setLogLevel("INFO");  // 显示信息级别 |
| `stop` | 无 | `void` | 停止SparkContext，释放所有资源，正常退出 | // stop()：正常停止
sc.stop();
// 释放所有资源，关闭连接 |
| `stop` | exitCode: Int | `void` | 停止SparkContext并指定退出码，用于异常退出 | // stop(exitCode)：指定退出码
sc.stop(1);  // 异常退出
sc.stop(0);  // 正常退出 |
| `textFile` | path: String | `JavaRDD` | 从文件系统读取文本文件，每行一条记录，支持通配符 | // textFile(path)：读取文本文件
JavaRDD<String> lines = sc.textFile("hdfs://path/file.txt");
JavaRDD<String> multiFiles = sc.textFile("hdfs://path/*.txt");  // 通配符 |
| `textFile` | path: String, minPartitions: Int | `JavaRDD` | 从文件系统读取文本文件，指定最小分区数 | // textFile(path, minPartitions)：指定分区数
JavaRDD<String> lines = sc.textFile("hdfs://path/file.txt", 10);
// 至少10个分区 |
| `wholeTextFiles` | path: String, minPartitions: Int | `JavaPairRDD` | 读取目录下所有文本文件，返回(路径,完整内容)，指定分区数 | // wholeTextFiles(path, minPartitions)：指定分区
JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles("hdfs://docs/", 10);
// 每个文件完整内容作为一个元素 |
| `wholeTextFiles` | path: String | `JavaPairRDD` | 读取目录下所有文本文件，返回(路径,完整内容)，适合小文件处理 | // wholeTextFiles(path)：读取所有文件
JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles("hdfs://docs/");
filesRDD.foreach(tuple -> System.out.println(tuple._1())); |

---

## MLlib机器学习

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 关联规则挖掘示例，从交易数据中发现频繁项集和关联规则 | // 运行: spark-submit --class JavaAssociationRulesExample target/spark-examples.jar
// 输入: 交易数据集，输出: 满足置信度的关联规则 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 二分类评估指标示例，计算AUC、精确率、召回率、F1等指标 | // 运行: spark-submit --class JavaBinaryClassificationMetricsExample target/spark-examples.jar
// 输入: 预测结果和真实标签，输出: ROC曲线、PR曲线等评估指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 二分K-Means聚类示例，层次聚类算法，自顶向下分裂 | // 运行: spark-submit --class JavaBisectingKMeansExample target/spark-examples.jar
// 输入: 向量数据集，输出: 聚类中心和分配结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 卡方检验特征选择示例，选择与标签最相关的特征 | // 运行: spark-submit --class JavaChiSqSelectorExample target/spark-examples.jar
// 输入: 特征向量和标签，输出: 选定的特征索引 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 相关性计算示例，计算Pearson和Spearman相关系数 | // 运行: spark-submit --class JavaCorrelationsExample target/spark-examples.jar
// 输入: 数值数据集，输出: 相关系数矩阵 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 逐元素乘积示例，向量与权重向量的逐元素加权 | // 运行: spark-submit --class JavaElementwiseProductExample target/spark-examples.jar
// 输入: 向量数据和权重向量，输出: 加权后的向量 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 高斯混合模型示例，概率聚类，假设数据由多个高斯分布生成 | // 运行: spark-submit --class JavaGaussianMixtureExample target/spark-examples.jar
// 输入: 向量数据集，输出: 混合模型参数和聚类分配 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 梯度提升分类示例，GBDT集成学习分类算法 | // 运行: spark-submit --class JavaGradientBoostingClassificationExample target/spark-examples.jar
// 输入: 训练数据集，输出: 分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 梯度提升回归示例，GBDT集成学习回归算法 | // 运行: spark-submit --class JavaGradientBoostingRegressionExample target/spark-examples.jar
// 输入: 训练数据集，输出: 回归模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 假设检验示例，统计显著性检验（卡方检验、t检验等） | // 运行: spark-submit --class JavaHypothesisTestingExample target/spark-examples.jar
// 输入: 样本数据，输出: 检验统计量和p值 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | Kolmogorov-Smirnov检验示例，检验样本是否服从指定分布 | // 运行: spark-submit --class JavaHypothesisTestingKolmogorovSmirnovTestExample target/spark-examples.jar
// 输入: 样本数据，输出: KS检验统计量和p值 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 保序回归示例，单调约束下的回归分析 | // 运行: spark-submit --class JavaIsotonicRegressionExample target/spark-examples.jar
// 输入: 有序数据，输出: 保序拟合结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | K-Means聚类示例，经典聚类算法，将数据划分为K个簇 | // 运行: spark-submit --class JavaKMeansExample target/spark-examples.jar
// 输入: 向量数据集，输出: 聚类中心和数据点分配 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 核密度估计示例，估计数据的概率密度函数 | // 运行: spark-submit --class JavaKernelDensityEstimationExample target/spark-examples.jar
// 输入: 样本数据，输出: 密度估计值 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | LBFGS优化示例，拟牛顿法求解大规模优化问题 | // 运行: spark-submit --class JavaLBFGSExample target/spark-examples.jar
// 输入: 优化问题和参数，输出: 最优解 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | LDA主题模型示例，文档主题发现和词分布估计 | // 运行: spark-submit --class JavaLatentDirichletAllocationExample target/spark-examples.jar
// 输入: 文档词频矩阵，输出: 主题分布和词主题分布 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | LBFGS逻辑回归示例，使用拟牛顿法优化逻辑回归 | // 运行: spark-submit --class JavaLogisticRegressionWithLBFGSExample target/spark-examples.jar
// 输入: 训练数据集，输出: 逻辑回归模型 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 多标签分类评估示例，计算多标签分类指标 | // 运行: spark-submit --class JavaMultiLabelClassificationMetricsExample target/spark-examples.jar
// 输入: 多标签预测结果，输出: 准确率、召回率等指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 多分类评估指标示例，计算混淆矩阵、准确率等 | // 运行: spark-submit --class JavaMulticlassClassificationMetricsExample target/spark-examples.jar
// 输入: 多分类预测结果，输出: 混淆矩阵和各项指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 朴素贝叶斯分类示例，基于概率的分类算法 | // 运行: spark-submit --class JavaNaiveBayesExample target/spark-examples.jar
// 输入: 训练数据集，输出: 分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | PCA降维示例，主成分分析，将高维数据降至低维 | // 运行: spark-submit --class JavaPCAExample target/spark-examples.jar
// 输入: 高维向量数据，输出: 降维后的向量和主成分 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 幂迭代聚类示例，基于相似度矩阵的图聚类算法 | // 运行: spark-submit --class JavaPowerIterationClusteringExample target/spark-examples.jar
// 输入: 相似度数据，输出: 聚类分配 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | PrefixSpan序列模式挖掘示例，发现序列数据中的频繁模式 | // 运行: spark-submit --class JavaPrefixSpanExample target/spark-examples.jar
// 输入: 序列数据集，输出: 频繁序列模式 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 随机森林分类示例，集成多棵决策树的分类算法 | // 运行: spark-submit --class JavaRandomForestClassificationExample target/spark-examples.jar
// 输入: 训练数据集，输出: 分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 随机森林回归示例，集成多棵决策树的回归算法 | // 运行: spark-submit --class JavaRandomForestRegressionExample target/spark-examples.jar
// 输入: 训练数据集，输出: 回归模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 排序评估指标示例，计算NDCG、MAP等推荐排序指标 | // 运行: spark-submit --class JavaRankingMetricsExample target/spark-examples.jar
// 输入: 排序预测结果，输出: NDCG、MAP等指标 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 协同过滤推荐示例，ALS算法实现用户-物品推荐 | // 运行: spark-submit --class JavaRecommendationExample target/spark-examples.jar
// 输入: 用户-物品评分矩阵，输出: 用户推荐列表 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | SVD奇异值分解示例，矩阵分解降维技术 | // 运行: spark-submit --class JavaSVDExample target/spark-examples.jar
// 输入: 矩阵数据，输出: U、S、V分解结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | SVM支持向量机示例，SGD优化训练线性SVM分类器 | // 运行: spark-submit --class JavaSVMWithSGDExample target/spark-examples.jar
// 输入: 训练数据集，输出: SVM分类模型和预测结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | FP-Growth频繁项集挖掘示例，高效发现交易数据中的频繁模式 | // 运行: spark-submit --class JavaSimpleFPGrowth target/spark-examples.jar
// 输入: 交易数据集，输出: 频繁项集及其支持度 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 分层采样示例，按标签比例进行数据采样 | // 运行: spark-submit --class JavaStratifiedSamplingExample target/spark-examples.jar
// 输入: 带标签数据集，输出: 分层采样结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 流式假设检验示例，实时数据流的统计检验 | // 运行: spark-submit --class JavaStreamingTestExample target/spark-examples.jar
// 输入: 流式数据，输出: 实时检验结果 |

--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 统计摘要示例，计算均值、方差、最大值、最小值等统计量 | // 运行: spark-submit --class JavaSummaryStatisticsExample target/spark-examples.jar
// 输入: 数值数据，输出: 完整统计摘要 |

---

## SQL DataFrame

--------|------|----------|------|------|
| `find` | key: long | `int` | 在哈希表中查找指定key的位置，返回索引 | 在哈希表中查找指定key，返回索引位置 |
| `findOrInsert` | key: long | `MutableColumnarRow` | 查找key位置，不存在则插入新条目 | 查找key或插入新条目，返回MutableColumnarRow |

--------|------|----------|------|------|
| `binarySearch` | data: boolean&lt;&gt;, value: boolean | `int` | 在boolean数组中二分查找，false排在true前面 | `boolean[] arr = {false, false, true, true};
int idx = ArrayExpressionUtils.binarySearch(arr, true);
// 返回2（找到索引）
int notFound = ArrayExpressionUtils.binarySearch(arr, true);
// 未找到返回-(插入点+1)` |
| `binarySearch` | data: Boolean&lt;&gt;, value: Boolean | `int` | 在Boolean数组中二分查找，支持null值（null排在最前） | `Boolean[] arr = {null, false, true};
int idx = ArrayExpressionUtils.binarySearch(arr, false);
// null < false < true 排序顺序` |
| `binarySearch` | data: byte&lt;&gt;, value: byte | `int` | 在byte数组中二分查找，数组必须已升序排序 | `byte[] arr = {1, 3, 5, 7, 9};
int idx = ArrayExpressionUtils.binarySearch(arr, 5);
// 返回2` |
| `binarySearch` | data: Byte&lt;&gt;, value: Byte | `int` | 在Byte数组中二分查找，支持null值排序 | `Byte[] arr = {null, 1, 5, 10};
int idx = ArrayExpressionUtils.binarySearch(arr, 5);` |
| `binarySearch` | data: short&lt;&gt;, value: short | `int` | 在short数组中二分查找，数组必须已升序排序 | `short[] arr = {100, 200, 300};
int idx = ArrayExpressionUtils.binarySearch(arr, 200);` |
| `binarySearch` | data: Short&lt;&gt;, value: Short | `int` | 在Short数组中二分查找，支持null值排序 | `Short[] arr = {null, 10, 20, 30};
int idx = ArrayExpressionUtils.binarySearch(arr, 20);` |
| `binarySearch` | data: int&lt;&gt;, value: int | `int` | 在int数组中二分查找，最常用的整数查找方法 | `int[] arr = {1, 5, 10, 15, 20};
int idx = ArrayExpressionUtils.binarySearch(arr, 10);
// 返回2（找到返回索引）
int notFound = ArrayExpressionUtils.binarySearch(arr, 8);
// 返回-3（插入点为2，返回-(2+1)）` |
| `binarySearch` | data: Integer&lt;&gt;, value: Integer | `int` | 在Integer数组中二分查找，支持null值排序（null排在最前） | `Integer[] arr = {null, 1, 5, 10};
int idx = ArrayExpressionUtils.binarySearch(arr, 5);` |
| `binarySearch` | data: long&lt;&gt;, value: long | `int` | 在long数组中二分查找，用于大整数查找 | `long[] arr = {100L, 200L, 300L};
int idx = ArrayExpressionUtils.binarySearch(arr, 200L);` |
| `binarySearch` | data: Long&lt;&gt;, value: Long | `int` | 在Long数组中二分查找，支持null值排序 | `Long[] arr = {null, 100L, 500L};
int idx = ArrayExpressionUtils.binarySearch(arr, 100L);` |
| `binarySearch` | data: float&lt;&gt;, value: float | `int` | 在float数组中二分查找，遵循SQL浮点数排序规则 | `float[] arr = {1.0f, 2.5f, 3.0f};
int idx = ArrayExpressionUtils.binarySearch(arr, 2.5f);` |
| `binarySearch` | data: Float&lt;&gt;, value: Float | `int` | 在Float数组中二分查找，支持null值，使用SQLOrderingUtil.compareFloats | `Float[] arr = {null, 1.0f, 2.0f};
int idx = ArrayExpressionUtils.binarySearch(arr, 1.0f);` |
| `binarySearch` | data: double&lt;&gt;, value: double | `int` | 在double数组中二分查找，使用标准二分查找算法 | `double[] arr = {1.1, 2.2, 3.3};
int idx = ArrayExpressionUtils.binarySearch(arr, 2.2);` |
| `binarySearch` | data: Double&lt;&gt;, value: Double | `int` | 在Double数组中二分查找，使用SQLOrderingUtil.compareDoubles处理特殊值 | `Double[] arr = {null, 1.0, 5.0};
int idx = ArrayExpressionUtils.binarySearch(arr, 1.0);` |
| `binarySearch` | data: Object&lt;&gt;, value: Object, comp: Comparator<Object> | `int` | 在Object数组中二分查找，使用自定义Comparator定义排序规则 | `String[] arr = {"apple", "banana", "cherry"};
Comparator<String> comp = String::compareTo;
int idx = ArrayExpressionUtils.binarySearch(arr, "banana", comp);
// 返回1` |

--------|------|----------|------|------|
| `getClassOfT` | 无 | `Class&lt;Decimal&gt;` | 获取泛型类型的Class对象 | 返回Decimal类型的Class对象 |
| `sizeOf` | item: Decimal | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `sizeOf` | mem: Memory, offsetBytes: long, numItems: int | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `sizeOf` | item: Decimal | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |
| `sizeOf` | mem: Memory, offsetBytes: long, numItems: int | `int` | 计算对象或数组占用的内存大小 | 计算Decimal对象或数组的内存大小 |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭Arrow列向量，释放底层Arrow ValueVector和子列向量占用的内存资源，防止内存泄漏 | `ArrowColumnVector vector = ...;
vector.close();  // 释放内存` |
| `getArray` | rowId: int | `ColumnarArray` | 获取指定行的数组类型数据，返回ColumnarArray对象，可通过它遍历数组元素 | `ColumnarArray arr = vector.getArray(0);
int len = arr.length();
for (int i = 0; i < len; i++) {
    Object elem = arr.get(i, elementType);
}` |
| `getBoolean` | rowId: int | `boolean` | 获取指定行位置的布尔值数据 | `boolean value = vector.getBoolean(0);
// 返回true或false` |
| `getByte` | rowId: int | `byte` | 获取指定行位置的字节值数据（-128到127） | `byte value = vector.getByte(0);` |
| `getChild` | ordinal: int | `ArrowColumnVector` | 获取嵌套类型（Struct/Array/Map）的子列向量，ordinal为子列索引 | `ArrowColumnVector child = vector.getChild(0);
// 获取Struct的第一个字段列` |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取指定行的Decimal高精度数值，precision为总位数，scale为小数位数 | `Decimal dec = vector.getDecimal(0, 10, 2);
// precision=10表示最多10位数字，scale=2表示2位小数` |
| `getDouble` | rowId: int | `double` | 获取指定行位置的双精度浮点数数据 | `double value = vector.getDouble(0);` |
| `getFloat` | rowId: int | `float` | 获取指定行位置的单精度浮点数数据 | `float value = vector.getFloat(0);` |
| `getGeography` | rowId: int | `GeographyVal` | 获取指定行的地理空间数据（Geography类型），用于GIS应用 | `GeographyVal geo = vector.getGeography(0);` |
| `getGeometry` | rowId: int | `GeometryVal` | 获取指定行的几何空间数据（Geometry类型），用于GIS应用 | `GeometryVal geom = vector.getGeometry(0);` |
| `getInt` | rowId: int | `int` | 获取指定行位置的整数值数据 | `int value = vector.getInt(0);` |
| `getInterval` | rowId: int | `CalendarInterval` | 获取指定行的时间间隔数据，包含months、days、microseconds三个字段 | `CalendarInterval interval = vector.getInterval(0);
int months = interval.months;
int days = interval.days;
long microseconds = interval.microseconds;` |
| `getLong` | rowId: int | `long` | 获取指定行位置的长整数值数据 | `long value = vector.getLong(0);` |
| `getMap` | rowId: int | `ColumnarMap` | 获取指定行的Map类型数据，返回ColumnarMap对象 | `ColumnarMap map = vector.getMap(0);
int numElements = map.numElements();
// 可通过keyArray()和valueArray()访问键值` |
| `getShort` | rowId: int | `short` | 获取指定行位置的短整数值数据（-32768到32767） | `short value = vector.getShort(0);` |
| `getUTF8String` | rowId: int | `UTF8String` | 获取指定行的UTF8编码字符串数据 | `UTF8String str = vector.getUTF8String(0);
String javaStr = str.toString();` |
| `getValueVector` | 无 | `ValueVector` | 获取底层Arrow ValueVector对象，用于直接访问Arrow原生API | `ValueVector arrowVec = vector.getValueVector();
// 可使用Arrow原生API进行高级操作` |
| `hasNull` | 无 | `boolean` | 检查列向量中是否存在null值，比遍历检查更高效 | `boolean hasNullValues = vector.hasNull();
if (hasNullValues) {
    // 需要处理null值逻辑
}` |
| `isNullAt` | rowId: int | `boolean` | 检查指定行位置是否为null值，读取数据前应先检查 | `if (!vector.isNullAt(0)) {
    int value = vector.getInt(0);
} else {
    // 处理null情况
}` |
| `numNulls` | 无 | `int` | 返回列向量中null值的总数量 | `int nullCount = vector.numNulls();
System.out.println("Null values: " + nullCount);` |


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

--------|------|----------|------|------|
| `populate` | col: ConstantColumnVector, row: InternalRow, fieldIdx: int | `void` | 填充常量列向量数据 | 填充常量列向量数据 |
| `toBatch` | schema: StructType, memMode: MemoryMode, row: Iterator<Row> | `ColumnarBatch` | 将行迭代器转换为列式批处理 | 将行迭代器转为列式批处理 |
| `toJavaIntMap` | map: ColumnarMap | `Map&lt;Integer, Integer&gt;` | 将ColumnarMap转换为Java Map | 将ColumnarMap转为Java整数Map |


### ColumnarBatch
**包路径**: `org.apache.spark.sql.vectorized`
**说明**: 列式批处理容器，将多个ColumnVector组织为行式表格，提供行视图访问数据。用于向量化执行，大幅提升数据处理效率。
**方法数量**: 10

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭批处理，释放所有列向量占用的内存资源，数据将不可访问 | `ColumnarBatch batch = ...;
try {
    // 使用batch处理数据
} finally {
    batch.close();  // 确保释放内存
}` |
| `closeIfFreeable` | 无 | `void` | 如果列向量的资源可被释放，则关闭它们，用于批处理间清理临时内存 | `batch.closeIfFreeable();
// 在批处理之间清理可释放资源` |
| `column` | ordinal: int | `ColumnVector` | 获取指定列索引位置的列向量对象，ordinal从0开始 | `ColumnVector col0 = batch.column(0);
ColumnVector col1 = batch.column(1);
// 访问各列数据` |
| `getRow` | rowId: int | `InternalRow` | 获取指定行号的内行对象，返回的行对象在多次调用间会被复用 | `InternalRow row = batch.getRow(0);
int value = row.getInt(0);
// 注意：row对象会被复用，不要跨调用保存` |
| `hasNext` | 无 | `boolean` | 检查行迭代器是否还有更多行可遍历（需先调用rowIterator获取迭代器） | `Iterator&lt;InternalRow&gt; iter = batch.rowIterator();
while (iter.hasNext()) {
    InternalRow row = iter.next();
    // 处理每行数据
}` |
| `next` | 无 | `InternalRow` | 获取行迭代器的下一行数据（需先调用rowIterator获取迭代器） | `Iterator&lt;InternalRow&gt; iter = batch.rowIterator();
while (iter.hasNext()) {
    InternalRow row = iter.next();
}` |
| `numCols` | 无 | `int` | 返回批处理中的列数量 | `int cols = batch.numCols();
System.out.println("列数: " + cols);` |
| `numRows` | 无 | `int` | 返回批处理中的行数量（包括被过滤的行） | `int rows = batch.numRows();
System.out.println("行数: " + rows);` |
| `rowIterator` | 无 | `Iterator&lt;InternalRow&gt;` | 返回行迭代器，用于按行遍历批处理中的所有数据 | `Iterator&lt;InternalRow&gt; iter = batch.rowIterator();
while (iter.hasNext()) {
    InternalRow row = iter.next();
    // 按行处理数据
}` |
| `setNumRows` | numRows: int | `void` | 设置批处理的行数量，用于动态调整批处理大小 | `batch.setNumRows(100);
// 设置批处理包含100行` |

--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭列向量，释放字符串、数组、Map和子列向量占用的内存 | `vector.close();  // 释放所有资源` |
| `closeIfFreeable` | 无 | `void` | 无操作（常量向量跨批处理复用，仅在close时释放） | `// 常量向量数据跨批复用
// 此方法为空实现` |
| `getArray` | rowId: int | `ColumnarArray` | 获取数组数据（所有行返回相同的ColumnarArray） | `ColumnarArray arr = vector.getArray(0);
// 所有rowId返回相同的数组` |
| `getBoolean` | rowId: int | `boolean` | 获取布尔值（所有行返回相同的值） | `boolean val = vector.getBoolean(0);
// 无需关心rowId，值相同` |
| `getByte` | rowId: int | `byte` | 获取字节值（所有行返回相同的值） | `byte val = vector.getByte(0);` |
| `getChild` | ordinal: int | `ColumnVector` | 获取嵌套类型的子列向量 | `ColumnVector child = vector.getChild(0);
// 用于Struct/Array等嵌套类型` |
| `getDecimal` | rowId: int, precision: int, scale: int | `Decimal` | 获取Decimal高精度数值（所有行返回相同的值） | `Decimal dec = vector.getDecimal(0, 10, 2);
// precision和scale指定精度` |
| `getDouble` | rowId: int | `double` | 获取双精度浮点数（所有行返回相同的值） | `double val = vector.getDouble(0);` |
| `getFloat` | rowId: int | `float` | 获取单精度浮点数（所有行返回相同的值） | `float val = vector.getFloat(0);` |
| `getInt` | rowId: int | `int` | 获取整数（所有行返回相同的值） | `int val = vector.getInt(0);` |
| `getLong` | rowId: int | `long` | 获取长整数（所有行返回相同的值） | `long val = vector.getLong(0);` |
| `getMap` | ordinal: int | `ColumnarMap` | 获取Map数据（所有行返回相同的ColumnarMap） | `ColumnarMap map = vector.getMap(0);` |
| `getShort` | rowId: int | `short` | 获取短整数（所有行返回相同的值） | `short val = vector.getShort(0);` |
| `getUTF8String` | rowId: int | `UTF8String` | 获取UTF8字符串（所有行返回相同的值） | `UTF8String str = vector.getUTF8String(0);
String javaStr = str.toString();` |
| `hasNull` | 无 | `boolean` | 检查是否所有行都是null值 | `boolean isAllNull = vector.hasNull();` |
| `isNullAt` | rowId: int | `boolean` | 检查指定行是否为null（所有行返回相同的null标记） | `if (vector.isNullAt(0)) {
    // 处理null情况
}` |
| `numNulls` | 无 | `int` | 返回null值数量（要么0要么全部行数） | `int nulls = vector.numNulls();
// 如果hasNull为true，返回numRows` |
| `setArray` | value: ColumnarArray | `void` | 设置所有行的数组常量值 | `vector.setArray(arrayValue);
// 所有行共享此数组` |
| `setBinary` | value: byte[] | `void` | 设置所有行的二进制常量值 | `byte[] data = new byte[]{1, 2, 3};
vector.setBinary(data);` |
| `setBoolean` | value: boolean | `void` | 设置所有行的布尔常量值 | `vector.setBoolean(true);
// 所有行值为true` |
| `setByte` | value: byte | `void` | 设置所有行的字节常量值 | `vector.setByte((byte) 100);` |
| `setCalendarInterval` | value: CalendarInterval | `void` | 设置所有行的时间间隔常量值 | `CalendarInterval interval = new CalendarInterval(1, 2, 1000L);
vector.setCalendarInterval(interval);` |
| `setChild` | ordinal: int, value: ConstantColumnVector | `void` | 设置嵌套类型的子列向量 | `vector.setChild(0, childVector);
// 用于Struct字段的子列` |
| `setDecimal` | value: Decimal, precision: int | `void` | 设置所有行的Decimal常量值 | `Decimal dec = Decimal.apply(123.45);
vector.setDecimal(dec, 10);` |
| `setDouble` | value: double | `void` | 设置所有行的双精度常量值 | `vector.setDouble(3.14);` |
| `setFloat` | value: float | `void` | 设置所有行的单精度常量值 | `vector.setFloat(2.5f);` |
| `setInt` | value: int | `void` | 设置所有行的整数常量值 | `vector.setInt(42);
// 所有行值为42` |
| `setLong` | value: long | `void` | 设置所有行的长整数常量值 | `vector.setLong(123456789L);` |
| `setMap` | value: ColumnarMap | `void` | 设置所有行的Map常量值 | `vector.setMap(mapValue);` |
| `setNotNull` | 无 | `void` | 设置所有行为非null值 | `vector.setNotNull();
// 清除null标记` |
| `setNull` | 无 | `void` | 设置所有行为null值 | `vector.setNull();
// 所有行都是null` |
| `setShort` | value: short | `void` | 设置所有行的短整数常量值 | `vector.setShort((short) 1000);` |
| `setUtf8String` | value: UTF8String | `void` | 设置所有行的UTF8字符串常量值 | `UTF8String str = UTF8String.fromString("hello");
vector.setUtf8String(str);` |
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


### Extract
**包路径**: `org.apache.spark.sql.connector.expressions`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `field` | 无 | `String` | field操作 | 调用该方法执行field操作 |
| `source` | 无 | `Expression` | 源相关功能 | 调用该方法执行源相关功能 |


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
| `EventTimeTimeout` | 无 | `GroupStateTimeout` | 基于事件时间的超时策略，状态在水印超时时被清理 | // 事件时间超时，适合处理延迟数据
.timeout(EventTimeTimeout())
// 状态在eventTime超过watermark时触发超时清理 |
| `NoTimeout` | 无 | `GroupStateTimeout` | 无超时策略，状态永不自动清理，需手动管理 | // 不设置超时，状态永久保留
.timeout(NoTimeout())
// 需要手动调用remove()清理状态 |
| `ProcessingTimeTimeout` | 无 | `GroupStateTimeout` | 基于处理时间的超时策略，状态在指定时间后清理 | // 处理时间超时，定时清理状态
.timeout(ProcessingTimeTimeout())
// 状态在processingTime超过阈值时触发超时清理 |

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
| `Append` | 无 | `OutputMode` | Append输出模式，只输出新增结果，适合无状态或聚合查询 | // Append模式：只输出新数据
// 适合无聚合操作或事件时间水印查询
.outputMode("append")
// 只追加新行，不修改已有数据 |
| `Complete` | 无 | `OutputMode` | Complete输出模式，输出完整结果，适合聚合查询 | // Complete模式：输出全部结果
// 适合聚合查询（如groupBy后count）
.outputMode("complete")
// 每次输出完整聚合结果表 |
| `Update` | 无 | `OutputMode` | Update输出模式，只输出更新的行，适合聚合查询 | // Update模式：只输出变更的行
// 适合聚合查询，只输出有更新的分组
.outputMode("update")
// 仅输出被更新或新增的行 |


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
| `EventTime` | 无 | `TimeMode` | EventTime时间模式，使用事件时间戳处理数据，支持水印和延迟数据 | // 事件时间模式：基于数据中的时间字段
// 支持水印处理延迟数据
.withWatermark("timestamp", "10 minutes")
// 按数据携带的时间戳处理 |
| `None` | 无 | `TimeMode` | None时间模式，不使用时间概念，适用于无时间语义的处理 | // 无时间模式：不考虑时间
// 适用于简单映射、过滤等无时间语义操作
// 不支持水印和超时功能 |
| `ProcessingTime` | 无 | `TimeMode` | ProcessingTime时间模式，使用处理时间（系统时钟），不支持延迟数据处理 | // 处理时间模式：基于Spark处理时间
// 使用系统时钟，不处理延迟数据
// 结果依赖于数据处理时刻 |


### Trigger
**包路径**: `org.apache.spark.sql.streaming`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AvailableNow` | 无 | `Trigger` | AvailableNow触发器，一次性处理所有可用数据，适合批量加载 | // AvailableNow：处理所有可用数据后停止
// 适合一次性批量处理或历史数据加载
.trigger(Trigger.AvailableNow())
// 处理完所有数据后自动停止查询 |
| `Continuous` | intervalMs: long | `Trigger` | Continuous连续触发器，毫秒级低延迟连续处理，支持持续查询 | // Continuous：连续处理模式（毫秒）
// 低延迟连续处理，适合实时流处理
.trigger(Trigger.Continuous(100))  // 100ms间隔 |
| `Continuous` | interval: long, timevoid: Timevoid | `Trigger` | Continuous连续触发器，指定时间单位，支持灵活间隔设置 | // Continuous：连续处理（指定时间单位）
.trigger(Trigger.Continuous(1, Timevoid.SECONDS))  // 1秒间隔 |
| `Continuous` | interval: Duration | `Trigger` | Continuous连续触发器，使用Java Duration对象设置间隔 | // Continuous：使用Duration设置间隔
.trigger(Trigger.Continuous(Duration.ofSeconds(5))) |
| `Continuous` | interval: String | `Trigger` | Continuous连续触发器，字符串格式设置间隔（如"5 seconds"） | // Continuous：字符串格式设置间隔
.trigger(Trigger.Continuous("5 seconds")) |
| `Once` | 无 | `Trigger` | Once触发器，执行一次批处理后停止，适合一次性查询 | // Once：执行一次批处理后停止
// 适合测试或一次性数据处理
.trigger(Trigger.Once())
// 处理完当前数据后停止查询 |
| `ProcessingTime` | intervalMs: long | `Trigger` | ProcessingTime定时触发器，按固定毫秒间隔触发批处理 | // ProcessingTime：固定间隔触发（毫秒）
// 微批处理模式，按固定间隔执行
.trigger(Trigger.ProcessingTime(5000))  // 5秒间隔 |
| `ProcessingTime` | interval: long, timevoid: Timevoid | `Trigger` | ProcessingTime定时触发器，指定时间单位设置间隔 | // ProcessingTime：固定间隔（指定时间单位）
.trigger(Trigger.ProcessingTime(1, Timevoid.MINUTES))  // 1分钟间隔 |
| `ProcessingTime` | interval: Duration | `Trigger` | ProcessingTime定时触发器，使用Java Duration对象设置间隔 | // ProcessingTime：使用Duration设置间隔
.trigger(Trigger.ProcessingTime(Duration.ofMinutes(10))) |
| `ProcessingTime` | interval: String | `Trigger` | ProcessingTime定时触发器，字符串格式设置间隔（如"1 minute"） | // ProcessingTime：字符串格式设置间隔
.trigger(Trigger.ProcessingTime("1 minute")) |
| `RealTime` | batchDurationMs: long | `Trigger` | RealTime实时触发器，毫秒级实时处理新数据 | // RealTime：实时处理新数据（毫秒）
// 尽快处理新到达的数据
.trigger(Trigger.RealTime(1000))  // 1秒检查新数据 |
| `RealTime` | batchDuration: long, timevoid: Timevoid | `Trigger` | RealTime实时触发器，指定时间单位设置批处理时长 | // RealTime：指定时间单位设置批处理时长
.trigger(Trigger.RealTime(5, Timevoid.SECONDS)) |
| `RealTime` | batchDuration: Duration | `Trigger` | RealTime实时触发器，使用Java Duration对象设置批处理时长 | // RealTime：使用Duration设置批处理时长
.trigger(Trigger.RealTime(Duration.ofSeconds(10))) |
| `RealTime` | batchDuration: String | `Trigger` | RealTime实时触发器，字符串格式设置批处理时长 | // RealTime：字符串格式设置批处理时长
.trigger(Trigger.RealTime("10 seconds")) |
| `RealTime` | 无 | `Trigger` | RealTime实时触发器，使用默认批处理时长 | // RealTime：默认批处理时长
// 使用系统默认设置进行实时处理
.trigger(Trigger.RealTime()) |

---

## 其他辅助类


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


### AbstractFetchShuffleBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
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


### AbstractLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 15

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addAppArgs` | args: String... | `T` | 添加应用参数 | 传入参数执行添加应用参数 |
| `addFile` | file: String | `T` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业
sc.addFile("hdfs://path/to/config.txt");
sc.addFile("s3://bucket/data.json");

// 在Executor中访问文件
String filePath = SparkFiles.get("config.txt"); |
| `addJar` | jar: String | `T` | 添加JAR包到Spark作业 | // 添加依赖JAR包
sc.addJar("hdfs://path/to/dependency.jar");
sc.addJar("/local/path/to/lib.jar"); |
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


### AbstractMessage
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `body` | 无 | `ManagedBuffer` | body操作 | 调用该方法执行body操作 |
| `isBodyInFrame` | 无 | `boolean` | 判断是否BodyInFrame相关功能 | 调用该方法执行判断是否BodyInFrame相关功能 |


### AbstractService
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |
| `getStartTime` | 无 | `long` | 获取StartTime相关功能 | 调用该方法执行获取StartTime相关功能 |


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


### AmIpPrincipal
**包路径**: `org.apache.spark.deploy.yarn`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getName` | 无 | `String` | 获取名称 | 调用该方法执行获取名称 |

--------|------|----------|------|------|
| `getRemoteUser` | 无 | `String` | 获取RemoteUser相关功能 | 调用该方法执行获取RemoteUser相关功能 |
| `getUserPrincipal` | 无 | `Principal` | 获取UserPrincipal相关功能 | 调用该方法执行获取UserPrincipal相关功能 |
| `isUserInRole` | role: String | `boolean` | 判断是否UserInRole相关功能 | 传入参数执行判断是否UserInRole相关功能 |

--------|------|----------|------|------|
| `Authenticate` | user: String, password: String | `void` | 认证操作 | 传入参数执行认证操作 |


### ApplicationStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `ApplicationStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |


### AuthClientBootstrap
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | client: TransportClient, channel: Channel | `void` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |


### AuthMethods
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getAuthMethod` | 无 | `String` | 获取AuthMethod相关功能 | 调用该方法执行获取AuthMethod相关功能 |
| `getAuthenticationProvider` | authMethod: AuthMethods | `PasswdAuthenticationProvider` | 获取AuthenticationProvider相关功能 | 传入参数执行获取AuthenticationProvider相关功能 |
| `getValidAuthMethod` | authMethodStr: String | `AuthMethods` | 获取ValidAuthMethod相关功能 | 传入参数执行获取ValidAuthMethod相关功能 |


### AuthServerBootstrap
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | channel: Channel, rpcHandler: RpcHandler | `RpcHandler` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |


### BestEffortLazyVal
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `T` | 应用数据类型转换 | 获取数据类型对应的列向量 |


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


### BlockPushReturnCode
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `BlockPushReturnCode` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


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


### BlockTransferMessage
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromByteBuffer` | msg: ByteBuffer | `BlockTransferMessage` | fromByteBuffer操作 | 传入参数执行fromByteBuffer操作 |
| `id` | 无 | `byte` | id操作 | 调用该方法执行id操作 |
| `toByteBuffer` | 无 | `ByteBuffer` | toByteBuffer操作 | 调用该方法执行toByteBuffer操作 |


### BlocksRemoved
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `BlocksRemoved` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


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


### ByteArrayReadableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `feedData` | buf: ByteBuf | `void` | feedData操作 | 传入参数执行feedData操作 |
| `isOpen` | 无 | `boolean` | 判断是否Open相关功能 | 调用该方法执行判断是否Open相关功能 |
| `read` | dst: ByteBuffer | `int` | 读取数据源创建DataFrame | 传入参数执行读取相关功能 |


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


### ByteBufferWriteableChannel
**包路径**: `org.apache.spark.network.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `isOpen` | 无 | `boolean` | 判断是否Open相关功能 | 调用该方法执行判断是否Open相关功能 |
| `write` | src: ByteBuffer | `int` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |


### Bytevoid
**包路径**: `org.apache.spark.network.util`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `convertFrom` | d: long, u: Bytevoid | `long` | 转换From相关功能 | 传入参数执行转换From相关功能 |
| `convertTo` | d: long, u: Bytevoid | `long` | 转换To相关功能 | 传入参数执行转换To相关功能 |
| `toBytes` | d: long | `long` | toBytes操作 | 传入参数执行toBytes操作 |
| `toGiB` | d: long | `long` | toGiB操作 | 传入参数执行toGiB操作 |
| `toKiB` | d: long | `long` | toKiB操作 | 传入参数执行toKiB操作 |
| `toMiB` | d: long | `long` | toMiB操作 | 传入参数执行toMiB操作 |
| `toPiB` | d: long | `long` | 顶部iB相关功能 | 传入参数执行顶部iB相关功能 |
| `toTiB` | d: long | `long` | toTiB操作 | 传入参数执行toTiB操作 |


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


### CLIServiceClient
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fetchResults` | opHandle: OperationHandle | `TRowSet` | 获取Results相关功能 | 传入参数执行获取Results相关功能 |
| `openSession` | username: String, password: String | `SessionHandle` | 打开Session相关功能 | 传入参数执行打开Session相关功能 |

--------|------|----------|------|------|
| `patternToRegex` | pattern: String | `String` | patternToRegex操作 | 传入参数执行patternToRegex操作 |


### ChildFirstURLClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getResource` | name: String | `URL` | 获取Resource相关功能 | 传入参数执行获取Resource相关功能 |
| `getResources` | name: String | `Enumeration&lt;URL&gt;` | 获取Resources相关功能 | 传入参数执行获取Resources相关功能 |
| `loadClass` | name: String, resolve: boolean | `Class&lt;?&gt;` | 加载Class相关功能 | 传入参数执行加载Class相关功能 |


### ChunkFetchRequestHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `exceptionCaught` | ctx: ChannelHandlerContext, cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `processFetchRequest` | channel: final Channel, msg: final ChunkFetchRequest | `void` | 处理FetchRequest相关功能 | 传入参数执行处理FetchRequest相关功能 |


### ClassicTableTypeMapping
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeNames` | 无 | `Set&lt;String&gt;` | 获取TableTypeNames相关功能 | 调用该方法执行获取TableTypeNames相关功能 |
| `mapToClientType` | hiveTypeName: String | `String` | 映射ToClientType相关功能 | 传入参数执行映射ToClientType相关功能 |


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


### ColumnValue
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toColumnValue` | value: TColumnValue | `Object` | 列相关功能 | 传入参数执行列相关功能 |
| `toTColumnValue` | typeDescriptor: TypeDescriptor, value: Object | `TColumnValue` | 列相关功能 | 传入参数执行列相关功能 |


### CompositeService
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getServices` | 无 | `Collection&lt;Service&gt;` | 获取Services相关功能 | 调用该方法执行获取Services相关功能 |
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |


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


### CookieSigner
**包路径**: `org.apache.hive.service`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `signCookie` | str: String | `String` | 签名Cookie相关功能 | 传入参数执行签名Cookie相关功能 |
| `verifyAndExtract` | signedStr: String | `String` | 验证AndExtract相关功能 | 传入参数执行验证AndExtract相关功能 |


### CorruptionCause
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `CorruptionCause` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


### CountMinSketch
**包路径**: `org.apache.spark.util.sketch`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | depth: int, width: int, seed: int | `CountMinSketch` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | eps: double, confidence: double, seed: int | `CountMinSketch` | 创建相关功能 | 传入参数执行创建相关功能 |
| `readFrom` | in: InputStream | `CountMinSketch` | 读取From相关功能 | 传入参数执行读取From相关功能 |
| `readFrom` | bytes: byte&lt;&gt; | `CountMinSketch` | 读取From相关功能 | 传入参数执行读取From相关功能 |

--------|------|----------|------|------|
| `toCryptoConf` | prefix: String, conf: String>> | `Properties` | toCryptoConf操作 | 传入参数执行toCryptoConf操作 |


### CtrTransportCipher
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addToChannel` | ch: Channel | `void` | 添加到通道 | 传入参数执行添加到通道 |
| `channelRead` | ctx: ChannelHandlerContext, data: Object | `void` | 读取相关功能 | 传入参数执行读取相关功能 |
| `close` | ctx: ChannelHandlerContext, promise: ChannelPromise | `void` | 关闭相关功能 | 传入参数执行关闭相关功能 |
| `count` | 无 | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `getKeyId` | 无 | `String` | 获取KeyId相关功能 | 调用该方法执行获取KeyId相关功能 |
| `handlerRemoved` | ctx: ChannelHandlerContext | `void` | 处理rRemoved相关功能 | 传入参数执行处理rRemoved相关功能 |
| `position` | 无 | `long` | position操作 | 调用该方法执行position操作 |
| `release` | decrement: int | `boolean` | 发布相关功能 | 传入参数执行发布相关功能 |
| `retain` | increment: int | `EncryptedMessage` | retain操作 | 传入参数执行retain操作 |
| `touch` | o: Object | `EncryptedMessage` | touch操作 | 传入参数执行touch操作 |
| `transferTo` | target: WritableByteChannel, position: long | `long` | 转移To相关功能 | 传入参数执行转移To相关功能 |
| `transferred` | 无 | `long` | 转移red相关功能 | 调用该方法执行转移red相关功能 |
| `write` | ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |

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


### DBBackend
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `byName` | value: String | `DBBackend` | byName操作 | 传入参数执行byName操作 |
| `fileName` | prefix: String | `String` | fileName操作 | 传入参数执行fileName操作 |


### DBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `initDB` | dbBackend: DBBackend, dbFile: File, version: StoreVersion, mapper: ObjectMapper | `DB` | 初始化DB相关功能 | 传入参数执行初始化DB相关功能 |
| `initDB` | dbBackend: DBBackend, file: File | `DB` | 初始化DB相关功能 | 传入参数执行初始化DB相关功能 |


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


### DiagnoseCorruption
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `DiagnoseCorruption` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


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


### ErrorHandler
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `shouldLogError` | t: Throwable | `boolean` | 判断是否应该LogError相关功能 | 传入参数执行判断是否应该LogError相关功能 |
| `shouldRetryError` | t: Throwable | `boolean` | 判断是否应该RetryError相关功能 | 传入参数执行判断是否应该RetryError相关功能 |


### ExecuteStatementOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStatement` | 无 | `String` | 获取Statement相关功能 | 调用该方法执行获取Statement相关功能 |

--------|------|----------|------|------|
| `getFilePath` | localDirs: String&lt;&gt;, subDirsPerLocalDir: int, filename: String | `String` | 获取FilePath相关功能 | 传入参数执行获取FilePath相关功能 |


### ExecutorShuffleInfo
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `ExecutorShuffleInfo` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


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


### FetchOrientation
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getFetchOrientation` | tFetchOrientation: TFetchOrientation | `FetchOrientation` | 获取FetchOrientation相关功能 | 传入参数执行获取FetchOrientation相关功能 |
| `toTFetchOrientation` | 无 | `TFetchOrientation` | 获取相关功能 | 调用该方法执行获取相关功能 |


### FetchShuffleBlockChunks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FetchShuffleBlockChunks` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `getNumBlocks` | 无 | `int` | 获取NumBlocks相关功能 | 调用该方法执行获取NumBlocks相关功能 |


### FetchShuffleBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FetchShuffleBlocks` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `getNumBlocks` | 无 | `int` | 获取NumBlocks相关功能 | 调用该方法执行获取NumBlocks相关功能 |


### FetchType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getFetchType` | tFetchType: short | `FetchType` | 获取FetchType相关功能 | 传入参数执行获取FetchType相关功能 |
| `toTFetchType` | 无 | `short` | 获取相关功能 | 调用该方法执行获取相关功能 |


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


### FinalizeShuffleMerge
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `FinalizeShuffleMerge` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


### GangliaReporter
**包路径**: `com.codahale.metrics.ganglia`
**方法数量**: 13

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `build` | gmetric: GMetric | `GangliaReporter` | 构建约束对象 | 构建Check约束对象 |
| `build` | gmetrics: GMetric... | `GangliaReporter` | 构建约束对象 | 构建Check约束对象 |
| `convertDurationsTo` | durationvoid: Timevoid | `Builder` | 转换DurationsTo相关功能 | 传入参数执行转换DurationsTo相关功能 |
| `convertRatesTo` | ratevoid: Timevoid | `Builder` | 转换RatesTo相关功能 | 传入参数执行转换RatesTo相关功能 |
| `disabledMetricAttributes` | disabledMetricAttributes: Set<MetricAttribute> | `Builder` | 禁用dMetricAttributes相关功能 | 传入参数执行禁用dMetricAttributes相关功能 |
| `filter` | filter: MetricFilter | `Builder` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `forRegistry` | registry: MetricRegistry | `Builder` | 判断是否相关功能 | 传入参数执行判断是否相关功能 |
| `prefixedWith` | prefix: String | `Builder` | 前fixedWith相关功能 | 传入参数执行前fixedWith相关功能 |
| `report` | gauges: Gauge>, counters: Counter>, histograms: Histogram>, meters: Meter>, timers: Timer> | `void` | report操作 | 传入参数执行report操作 |
| `scheduleOn` | executor: ScheduledExecutorService | `Builder` | 调度On相关功能 | 传入参数执行调度On相关功能 |
| `shutdownExecutorOnStop` | shutdownExecutorOnStop: boolean | `Builder` | 关闭ExecutorOnStop相关功能 | 传入参数执行关闭ExecutorOnStop相关功能 |
| `withDMax` | dMax: int | `Builder` | withDMax操作 | 传入参数执行withDMax操作 |
| `withTMax` | tMax: int | `Builder` | withTMax操作 | 传入参数执行withTMax操作 |


### GcmTransportCipher
**包路径**: `org.apache.spark.network.crypto`
**方法数量**: 11

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addToChannel` | ch: Channel | `void` | 添加到通道 | 传入参数执行添加到通道 |
| `channelRead` | ctx: ChannelHandlerContext, ciphertextMessage: Object | `void` | 读取相关功能 | 传入参数执行读取相关功能 |
| `count` | 无 | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `getKeyId` | 无 | `String` | 获取KeyId相关功能 | 调用该方法执行获取KeyId相关功能 |
| `position` | 无 | `long` | position操作 | 调用该方法执行position操作 |
| `release` | decrement: int | `boolean` | 发布相关功能 | 传入参数执行发布相关功能 |
| `retain` | increment: int | `GcmEncryptedMessage` | retain操作 | 传入参数执行retain操作 |
| `touch` | o: Object | `GcmEncryptedMessage` | touch操作 | 传入参数执行touch操作 |
| `transferTo` | target: WritableByteChannel, position: long | `long` | 转移To相关功能 | 传入参数执行转移To相关功能 |
| `transferred` | 无 | `long` | 转移red相关功能 | 调用该方法执行转移red相关功能 |
| `write` | ctx: ChannelHandlerContext, msg: Object, promise: ChannelPromise | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |


### GetCatalogsOperation
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


### GetFunctionsOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |


### GetInfoType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getGetInfoType` | tGetInfoType: TGetInfoType | `GetInfoType` | 获取GetInfoType相关功能 | 传入参数执行获取GetInfoType相关功能 |
| `toTGetInfoType` | 无 | `TGetInfoType` | 获取相关功能 | 调用该方法执行获取相关功能 |


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


### GetLocalDirsForExecutors
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `GetLocalDirsForExecutors` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


### GetSchemasOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |


### GetTableTypesOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |


### GetTablesOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |


### GetTypeInfoOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNextRowSet` | orientation: FetchOrientation, maxRows: long | `TRowSet` | 获取NextRowSet相关功能 | 传入参数执行获取NextRowSet相关功能 |
| `getResultSetSchema` | 无 | `TTableSchema` | 获取ResultSetSchema相关功能 | 调用该方法执行获取ResultSetSchema相关功能 |
| `runInternal` | 无 | `void` | 运行Internal相关功能 | 调用该方法执行运行Internal相关功能 |


### HadoopConfigProvider
**包路径**: `org.apache.spark.network.yarn.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String | `String` | 获取元素 | 传入参数执行获取相关功能 |
| `get` | name: String, defaultValue: String | `String` | 获取元素 | 传入参数执行获取相关功能 |


### Handle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getHandleIdentifier` | 无 | `HandleIdentifier` | 获取HandleIdentifier相关功能 | 调用该方法执行获取HandleIdentifier相关功能 |


### HandleIdentifier
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getPublicId` | 无 | `UUID` | 获取PublicId相关功能 | 调用该方法执行获取PublicId相关功能 |
| `getSecretId` | 无 | `UUID` | 获取SecretId相关功能 | 调用该方法执行获取SecretId相关功能 |
| `toTHandleIdentifier` | 无 | `THandleIdentifier` | 处理相关功能 | 调用该方法执行处理相关功能 |


### HashMapGrowthStrategy
**包路径**: `org.apache.spark.unsafe.map`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `nextCapacity` | currentCapacity: int | `int` | 之后Capacity相关功能 | 传入参数执行之后Capacity相关功能 |


### HeapMemoryAllocator
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `allocate` | size: long | `MemoryBlock` | 分配相关功能 | 传入参数执行分配相关功能 |
| `free` | memory: MemoryBlock | `void` | free操作 | 传入参数执行free操作 |


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

--------|------|----------|------|------|
| `getMethodInternal` | udfClass: Class<?>, mlist: List<Method>, exact: boolean, argumentsPassed: List<TypeInfo> | `Method` | 获取MethodInternal相关功能 | 传入参数执行获取MethodInternal相关功能 |
| `invoke` | m: Method, thisObject: Object, arguments: Object... | `Object` | 调用相关功能 | 传入参数执行调用相关功能 |
| `matchCost` | argumentPassed: TypeInfo, argumentAccepted: TypeInfo, exact: boolean | `int` | matchCost操作 | 传入参数执行matchCost操作 |


### HiveSQLException
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `toCause` | details: List<String> | `Throwable` | toCause操作 | 传入参数执行toCause操作 |
| `toTStatus` | 无 | `TStatus` | toTStatus操作 | 调用该方法执行toTStatus操作 |
| `toTStatus` | e: Exception | `TStatus` | toTStatus操作 | 传入参数执行toTStatus操作 |


### HiveServer2
**包路径**: `org.apache.hive.service.server`
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `Boxedvoid` | 应用数据类型转换 | 获取数据类型对应的列向量 |
| `execute` | 无 | `void` | 执行相关功能 | 调用该方法执行执行相关功能 |
| `isHTTPTransportMode` | hiveConf: HiveConf | `boolean` | 判断是否HTTPTransportMode相关功能 | 传入参数执行判断是否HTTPTransportMode相关功能 |
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |
| `parse` | argv: String&lt;&gt; | `ServerOptionsProcessorResponse` | 解析相关功能 | 传入参数执行解析相关功能 |

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


### HiveSessionProxy
**包路径**: `org.apache.hive.service.cli.session`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getProxy` | hiveSession: HiveSession, ugi: UserGroupInformation | `HiveSession` | 获取Proxy相关功能 | 传入参数执行获取Proxy相关功能 |
| `invoke` | arg0: Object, method: final Method, args: final Object&lt;&gt; | `Object` | 调用相关功能 | 传入参数执行调用相关功能 |


### HiveTableTypeMapping
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeNames` | 无 | `Set&lt;String&gt;` | 获取TableTypeNames相关功能 | 调用该方法执行获取TableTypeNames相关功能 |
| `mapToClientType` | hiveTypeName: String | `String` | 映射ToClientType相关功能 | 传入参数执行映射ToClientType相关功能 |


### InMemoryStore
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 21

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `accept` | key: Comparable<Object>, value: T | `void` | 接受相关功能 | 传入参数执行接受相关功能 |
| `clear` | 无 | `void` | 清空集合 | 调用该方法执行清除相关功能 |
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `count` | type: Class<?> | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `count` | type: Class<?>, index: String, indexedValue: Object | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `count` | 无 | `int` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
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


### InProcessLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `startApplication` | listeners: SparkAppHandle.Listener... | `SparkAppHandle` | 启动Application相关功能 | 传入参数执行启动Application相关功能 |

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


### JavaModuleOptions
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultModuleOptions` | 无 | `String` | 默认ModuleOptions相关功能 | 调用该方法执行默认ModuleOptions相关功能 |

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
| `byteStringAs` | str: String, unit: Bytevoid | `long` | 三相关功能 | 传入参数执行三相关功能 |
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
| `join` | arr: List<Object>, sep: String | `String` | 连接DataFrame | // join：内连接
List<Tuple2<String, Integer>> orders = Arrays.asList(
    new Tuple2<>("user1", 100),
    new Tuple2<>("user2", 200)
);
List<Tuple2<String, String>> users = Arrays.asList(
    new Tuple2<>("user1", "Alice"),
    new Tuple2<>("user2", "Bob")
);

JavaPairRDD<String, Integer> orderRDD = sc.parallelizePairs(orders);
JavaPairRDD<String, String> userRDD = sc.parallelizePairs(users);

// 内连接
JavaPairRDD<String, Tuple2<Integer, String>> joined = orderRDD.join(userRDD);
// 结果: [("user1", (100, "Alice")), ("user2", (200, "Bob"))] |
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
| `timeStringAs` | str: String, unit: Timevoid | `long` | 时间StringAs相关功能 | 传入参数执行时间StringAs相关功能 |
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


### JobExecutionStatus
**包路径**: `org.apache.spark`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `JobExecutionStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |


### KVStoreView
**包路径**: `org.apache.spark.util.kvstore`
**方法数量**: 8

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `closeableIterator` | 无 | `KVStoreIterator&lt;T&gt;` | 关闭ableIterator相关功能 | 调用该方法执行关闭ableIterator相关功能 |
| `first` | value: Object | `KVStoreView&lt;T&gt;` | 第一行 | // first：获取第一个元素
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(10, 20, 30));
Integer first = rdd.first();
// 结果: 10 |
| `index` | name: String | `KVStoreView&lt;T&gt;` | index操作 | 传入参数执行index操作 |
| `last` | value: Object | `KVStoreView&lt;T&gt;` | 最后一个相关功能 | 传入参数执行最后一个相关功能 |
| `max` | max: long | `KVStoreView&lt;T&gt;` | 最大值 | // max：最大值
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));
double max = doubleRDD.max();
// 结果: 30.0 |
| `parent` | value: Object | `KVStoreView&lt;T&gt;` | 父级相关功能 | 传入参数执行父级相关功能 |
| `reverse` | 无 | `KVStoreView&lt;T&gt;` | reverse操作 | 调用该方法执行reverse操作 |
| `skip` | n: long | `KVStoreView&lt;T&gt;` | 跳过相关功能 | 传入参数执行跳过相关功能 |


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

--------|------|----------|------|------|
| `Authenticate` | user: String, password: String | `void` | 认证操作 | 传入参数执行认证操作 |


### LevelDB
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `count` | type: Class<?> | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `count` | type: Class<?>, index: String, indexedValue: Object | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `delete` | key: byte&lt;&gt; | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `delete` | type: Class<?>, naturalKey: Object | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `iterator` | 无 | `DBIterator` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `iterator` | 无 | `Iterator&lt;T&gt;` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `put` | key: byte&lt;&gt;, value: byte&lt;&gt; | `void` | 添加键值对 | 传入参数执行放入相关功能 |
| `setMetadata` | value: Object | `void` | 设置Metadata相关功能 | 传入参数执行设置Metadata相关功能 |
| `write` | value: Object | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `writeAll` | values: List<?> | `void` | 写入All相关功能 | 传入参数执行写入All相关功能 |


### LevelDBIterator
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `seek` | key: byte&lt;&gt; | `void` | 定位相关功能 | 传入参数执行定位相关功能 |


### LevelDBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkVersion` | db: DB, newversion: StoreVersion, mapper: ObjectMapper | `void` | 检查Version相关功能 | 传入参数执行检查Version相关功能 |
| `initLevelDB` | dbFile: File, version: StoreVersion, mapper: ObjectMapper | `DB` | 初始化LevelDB相关功能 | 传入参数执行初始化LevelDB相关功能 |
| `log` | message: String | `void` | 日志相关功能 | 传入参数执行日志相关功能 |
| `storeVersion` | db: DB, version: StoreVersion, mapper: ObjectMapper | `void` | 版本相关功能 | 传入参数执行版本相关功能 |


### LocalDirsForExecutors
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `LocalDirsForExecutors` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `getLocalDirsByExec` | 无 | `Map&lt;String, String[]&gt;` | 获取LocalDirsByExec相关功能 | 调用该方法执行获取LocalDirsByExec相关功能 |


### LocalDiskShuffleDataIO
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `driver` | 无 | `ShuffleDriverComponents` | driver操作 | 调用该方法执行driver操作 |
| `executor` | 无 | `ShuffleExecutorComponents` | executor操作 | 调用该方法执行executor操作 |


### LocalDiskShuffleDriverComponents
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cleanupApplication` | 无 | `void` | 向上相关功能 | 调用该方法执行向上相关功能 |
| `initializeApplication` | 无 | `Map&lt;String, String&gt;` | 初始化ializeApplication相关功能 | 调用该方法执行初始化ializeApplication相关功能 |
| `removeShuffle` | shuffleId: int, blocking: boolean | `void` | 移除Shuffle相关功能 | 传入参数执行移除Shuffle相关功能 |


### LocalDiskShuffleExecutorComponents
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `createMapOutputWriter` | shuffleId: int, mapTaskId: long, numPartitions: int | `ShuffleMapOutputWriter` | 创建MapOutputWriter相关功能 | 传入参数执行创建MapOutputWriter相关功能 |
| `createSingleFileMapOutputWriter` | shuffleId: int, mapId: long | `Optional&lt;SingleSpillShuffleMapOutputWriter&gt;` | 创建SingleFileMapOutputWriter相关功能 | 传入参数执行创建SingleFileMapOutputWriter相关功能 |
| `initializeExecutor` | appId: String, execId: String, extraConfigs: String> | `void` | 初始化ializeExecutor相关功能 | 传入参数执行初始化ializeExecutor相关功能 |


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


### LocalDiskSingleSpillMapOutputWriter
**包路径**: `org.apache.spark.shuffle.sort.io`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transferMapSpillFile` | mapSpillFile: File, partitionLengths: long&lt;&gt;, checksums: long&lt;&gt; | `void` | 转移MapSpillFile相关功能 | 传入参数执行转移MapSpillFile相关功能 |


### LogDivertAppender
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 24

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `append` | event: LogEvent | `void` | 追加元素 | 向缓冲迭代器追加一行数据 |
| `create` | operationManager: OperationManager, loggingMode: OperationLog.LoggingLevel | `LogDivertAppender` | 创建相关功能 | 传入参数执行创建相关功能 |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, objects: Object... | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object, o7: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object, o7: Object, o8: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, s: String, o: Object, o1: Object, o2: Object, o3: Object, o4: Object, o5: Object, o6: Object, o7: Object, o8: Object, o9: Object | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, o: Object, throwable: Throwable | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logger: org.apache.logging.log4j.core.Logger, level: Level, marker: Marker, message: Message, throwable: Throwable | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `filter` | logEvent: LogEvent | `Result` | 过滤行 | // 过滤满足条件的元素
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));

// 过滤大于5的数
JavaRDD<Integer> greaterThan5 = numbers.filter(x -> x > 5);
// 结果: [6, 7, 8, 9, 10]

// 过滤偶数
JavaRDD<Integer> evens = numbers.filter(x -> x % 2 == 0);
// 结果: [2, 4, 6, 8, 10] |
| `getOnMatch` | 无 | `Result` | 获取OnMatch相关功能 | 调用该方法执行获取OnMatch相关功能 |
| `getOnMismatch` | 无 | `Result` | 获取OnMismatch相关功能 | 调用该方法执行获取OnMismatch相关功能 |
| `getState` | 无 | `State` | 获取State相关功能 | 调用该方法执行获取State相关功能 |
| `initialize` | 无 | `void` | 初始化插件 | 初始化目录插件 |
| `isStarted` | 无 | `boolean` | 判断是否Started相关功能 | 调用该方法执行判断是否Started相关功能 |
| `isStopped` | 无 | `boolean` | 判断是否Stopped相关功能 | 调用该方法执行判断是否Stopped相关功能 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |
| `stop` | 无 | `void` | 停止SparkContext，释放资源 | 调用该方法执行停止相关功能 |


### MapConfigProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `get` | name: String | `String` | 获取元素 | 传入参数执行获取相关功能 |
| `get` | name: String, defaultValue: String | `String` | 获取元素 | 传入参数执行获取相关功能 |


### MemoryBlock
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fill` | value: byte | `void` | fill操作 | 传入参数执行fill操作 |
| `fromLongArray` | array: final long&lt;&gt; | `MemoryBlock` | fromLongArray操作 | 传入参数执行fromLongArray操作 |
| `size` | 无 | `long` | 计算大小 | 调用该方法执行size操作 |


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


### MemoryLocation
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getBaseObject` | 无 | `Object` | 获取BaseObject相关功能 | 调用该方法执行获取BaseObject相关功能 |
| `getBaseOffset` | 无 | `long` | 获取BaseOffset相关功能 | 调用该方法执行获取BaseOffset相关功能 |
| `setObjAndOffset` | newObj: Object, newOffset: long | `void` | 设置ObjAndOffset相关功能 | 传入参数执行设置ObjAndOffset相关功能 |


### MergeStatuses
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `MergeStatuses` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


### MergedBlockMeta
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getChunksBitmapBuffer` | 无 | `ManagedBuffer` | 获取ChunksBitmapBuffer相关功能 | 调用该方法执行获取ChunksBitmapBuffer相关功能 |
| `getNumChunks` | 无 | `int` | 获取NumChunks相关功能 | 调用该方法执行获取NumChunks相关功能 |


### MergedBlockMetaRequest
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `MergedBlockMetaRequest` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `type` | 无 | `Type` | type操作 | 调用该方法执行type操作 |


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


### Message
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `Type` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |
| `id` | 无 | `byte` | id操作 | 调用该方法执行id操作 |


### MessageWithHeader
**包路径**: `org.apache.spark.network.protocol`
**方法数量**: 7

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `count` | 无 | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `position` | 无 | `long` | position操作 | 调用该方法执行position操作 |
| `release` | decrement: int | `boolean` | 发布相关功能 | 传入参数执行发布相关功能 |
| `retain` | increment: int | `MessageWithHeader` | retain操作 | 传入参数执行retain操作 |
| `touch` | o: Object | `MessageWithHeader` | touch操作 | 传入参数执行touch操作 |
| `transferTo` | target: final WritableByteChannel, position: final long | `long` | 转移To相关功能 | 传入参数执行转移To相关功能 |
| `transferred` | 无 | `long` | 转移red相关功能 | 调用该方法执行转移red相关功能 |


### MetadataOperation
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |


### MutableURLClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addURL` | url: URL | `void` | 添加URL | 传入参数执行添加URL |


### MyLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `main` | args: String&lt;&gt; | `void` | 主要相关功能 | 传入参数执行主要相关功能 |


### NettyLogger
**包路径**: `org.apache.spark.network.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getLoggingHandler` | 无 | `LoggingHandler` | 获取LoggingHandler相关功能 | 调用该方法执行获取LoggingHandler相关功能 |


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


### NettyMemoryMetrics
**包路径**: `org.apache.spark.network.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getMetrics` | 无 | `Map&lt;String, Metric&gt;` | 获取Metrics相关功能 | 调用该方法执行获取Metrics相关功能 |

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


### NoOpRpcHandler
**包路径**: `org.apache.spark.network.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getStreamManager` | 无 | `StreamManager` | 获取StreamManager相关功能 | 调用该方法执行获取StreamManager相关功能 |
| `receive` | client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback | `void` | 接收相关功能 | 传入参数执行接收相关功能 |


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


### OneForOneBlockPusher
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `onFailure` | e: Throwable | `void` | onFailure操作 | 传入参数执行onFailure操作 |
| `onSuccess` | response: ByteBuffer | `void` | onSuccess操作 | 传入参数执行onSuccess操作 |
| `start` | 无 | `void` | 启动相关功能 | 调用该方法执行启动相关功能 |


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


### OpenBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `OpenBlocks` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


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


### OperationStatus
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationException` | 无 | `HiveSQLException` | 获取OperationException相关功能 | 调用该方法执行获取OperationException相关功能 |
| `getState` | 无 | `OperationState` | 获取State相关功能 | 调用该方法执行获取State相关功能 |


### OperationType
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getOperationType` | tOperationType: TOperationType | `OperationType` | 获取OperationType相关功能 | 传入参数执行获取OperationType相关功能 |
| `toTOperationType` | 无 | `TOperationType` | 顶部相关功能 | 调用该方法执行顶部相关功能 |

--------|------|----------|------|------|
| `Authenticate` | user: String, password: String | `void` | 认证操作 | 传入参数执行认证操作 |


### ParentClassLoader
**包路径**: `org.apache.spark.util`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `findClass` | name: String | `Class&lt;?&gt;` | 查找Class相关功能 | 传入参数执行查找Class相关功能 |
| `loadClass` | name: String, resolve: boolean | `Class&lt;?&gt;` | 加载Class相关功能 | 传入参数执行加载Class相关功能 |


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

--------|------|----------|------|------|
| `html` | 无 | `HTML&lt;ProxyUtils.__&gt;` | html操作 | 调用该方法执行html操作 |
| `notFound` | resp: HttpServletResponse, message: String | `void` | notFound操作 | 传入参数执行notFound操作 |
| `rejectNonHttpRequests` | req: ServletRequest | `void` | 拒绝NonHttpRequests相关功能 | 传入参数执行拒绝NonHttpRequests相关功能 |
| `sendRedirect` | request: HttpServletRequest, response: HttpServletResponse, target: String | `void` | 发送Redirect相关功能 | 传入参数执行发送Redirect相关功能 |


### PushBlockStream
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `PushBlockStream` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


### RadixSort
**包路径**: `org.apache.spark.util.collection.unsafe.sort`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `sort` | array: LongArray, numRecords: long, startByteIndex: int, endByteIndex: int, desc: boolean, signed: boolean | `int` | 排序 | 传入参数执行创建排序表达式 |
| `sortKeyPrefixArray` | array: LongArray, startIndex: long, numRecords: long, startByteIndex: int, endByteIndex: int, desc: boolean, signed: boolean | `int` | 排序KeyPrefixArray相关功能 | 传入参数执行排序KeyPrefixArray相关功能 |


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


### RegisterExecutor
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RegisterExecutor` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


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


### RemoveBlocks
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `RemoveBlocks` | 解码相关功能 | 传入参数执行解码相关功能 |
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


### RocksDB
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 12

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `count` | type: Class<?> | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `count` | type: Class<?>, index: String, indexedValue: Object | `long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `delete` | key: byte&lt;&gt; | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `delete` | type: Class<?>, naturalKey: Object | `void` | 删除请求相关功能 | 传入参数执行删除请求相关功能 |
| `iterator` | 无 | `DBIterator` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `iterator` | 无 | `Iterator&lt;T&gt;` | 获取迭代器 | 调用该方法执行时期相关功能 |
| `put` | key: byte&lt;&gt;, value: byte&lt;&gt; | `void` | 添加键值对 | 传入参数执行放入相关功能 |
| `setMetadata` | value: Object | `void` | 设置Metadata相关功能 | 传入参数执行设置Metadata相关功能 |
| `write` | value: Object | `void` | 写入DataFrame到数据源 | 传入参数执行写入相关功能 |
| `writeAll` | values: List<?> | `void` | 写入All相关功能 | 传入参数执行写入All相关功能 |


### RocksDBIterator
**包路径**: `org.apache.spark.network.shuffledb`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `hasNext` | 无 | `boolean` | 检查迭代器是否还有下一元素 | 检查迭代器是否还有下一行 |
| `seek` | key: byte&lt;&gt; | `void` | 定位相关功能 | 传入参数执行定位相关功能 |


### RocksDBProvider
**包路径**: `org.apache.spark.network.util`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkVersion` | db: RocksDB, newversion: StoreVersion, mapper: ObjectMapper | `void` | 检查Version相关功能 | 传入参数执行检查Version相关功能 |
| `initRockDB` | dbFile: File, version: StoreVersion, mapper: ObjectMapper | `RocksDB` | 初始化RockDB相关功能 | 传入参数执行初始化RockDB相关功能 |
| `storeVersion` | db: RocksDB, version: StoreVersion, mapper: ObjectMapper | `void` | 版本相关功能 | 传入参数执行版本相关功能 |


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


### RowSetFactory
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | schema: TableSchema, version: TProtocolVersion, isBlobBased: boolean | `RowSet` | 创建相关功能 | 传入参数执行创建相关功能 |
| `create` | results: TRowSet, version: TProtocolVersion | `RowSet` | 创建相关功能 | 传入参数执行创建相关功能 |


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


### SaslClientBootstrap
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | client: TransportClient, channel: Channel | `void` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |


### SaslQOP
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `SaslQOP` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |


### SaslRpcHandler
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | client: TransportClient | `void` | 活跃相关功能 | 传入参数执行活跃相关功能 |
| `doAuthChallenge` | client: TransportClient, message: ByteBuffer, callback: RpcResponseCallback | `boolean` | 执行AuthChallenge相关功能 | 传入参数执行执行AuthChallenge相关功能 |


### SaslServerBootstrap
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `doBootstrap` | channel: Channel, rpcHandler: RpcHandler | `RpcHandler` | 执行Bootstrap相关功能 | 传入参数执行执行Bootstrap相关功能 |

--------|------|----------|------|------|
| `cleanup` | log: SparkLogger, closeables: java.io.Closeable... | `void` | 向上相关功能 | 传入参数执行向上相关功能 |
| `indexOfDomainMatch` | userName: String | `int` | 执行相关功能 | 传入参数执行执行相关功能 |


### SessionHandle
**包路径**: `org.apache.hive.service.cli`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getProtocolVersion` | 无 | `TProtocolVersion` | 获取ProtocolVersion相关功能 | 调用该方法执行获取ProtocolVersion相关功能 |
| `getSessionId` | 无 | `UUID` | 获取SessionId相关功能 | 调用该方法执行获取SessionId相关功能 |
| `toTSessionHandle` | 无 | `TSessionHandle` | 处理相关功能 | 调用该方法执行处理相关功能 |


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

--------|------|----------|------|------|
| `rebuild` | row: ShreddedRow, schema: VariantSchema | `Variant` | 构建相关功能 | 传入参数执行构建相关功能 |
| `rebuild` | row: ShreddedRow, metadata: byte&lt;&gt;, schema: VariantSchema, builder: VariantBuilder | `void` | 构建相关功能 | 传入参数执行构建相关功能 |

--------|------|----------|------|------|
| `diagnoseCorruption` | algorithm: String, checksumFile: File, reduceId: int, partitionData: ManagedBuffer, checksumByReader: long | `Cause` | 向上相关功能 | 传入参数执行向上相关功能 |
| `getChecksumByAlgorithm` | algorithm: String | `Checksum` | 获取ChecksumByAlgorithm相关功能 | 传入参数执行获取ChecksumByAlgorithm相关功能 |
| `getChecksumFileName` | blockName: String, algorithm: String | `String` | 获取ChecksumFileName相关功能 | 传入参数执行获取ChecksumFileName相关功能 |


### ShuffleIndexInformation
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getIndex` | reduceId: int | `ShuffleIndexRecord` | 获取Index相关功能 | 传入参数执行获取Index相关功能 |
| `getIndex` | startReduceId: int, endReduceId: int | `ShuffleIndexRecord` | 获取Index相关功能 | 传入参数执行获取Index相关功能 |
| `getRetainedMemorySize` | 无 | `int` | 获取RetainedMemorySize相关功能 | 调用该方法执行获取RetainedMemorySize相关功能 |


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


### ShuffleTransportContext
**包路径**: `org.apache.spark.network.shuffle`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `acceptInboundMessage` | msg: Object | `boolean` | 接受入站消息 | 传入参数执行接受入站消息 |
| `initializePipeline` | channel: SocketChannel, isClient: boolean | `TransportChannelHandler` | 初始化ializePipeline相关功能 | 传入参数执行初始化ializePipeline相关功能 |
| `initializePipeline` | channel: SocketChannel, channelRpcHandler: RpcHandler, isClient: boolean | `TransportChannelHandler` | 初始化ializePipeline相关功能 | 传入参数执行初始化ializePipeline相关功能 |


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


### SparkAppHandle
**包路径**: `org.apache.spark.launcher`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isFinal` | 无 | `boolean` | 判断是否Final相关功能 | 调用该方法执行判断是否Final相关功能 |

--------|------|----------|------|------|
| `getEvaluatorClass` | argClasses: List<TypeInfo> | `Class&lt;? extends UDAFEvaluator&gt;` | 获取EvaluatorClass相关功能 | 传入参数执行获取EvaluatorClass相关功能 |

--------|------|----------|------|------|
| `getEvalMethod` | argClasses: List<TypeInfo> | `Method` | 获取EvalMethod相关功能 | 传入参数执行获取EvalMethod相关功能 |


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


### SparkLauncher
**包路径**: `org.apache.spark.launcher`
**方法数量**: 26

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `addAppArgs` | args: String... | `SparkLauncher` | 添加应用参数 | 传入参数执行添加应用参数 |
| `addFile` | file: String | `SparkLauncher` | 添加文件到Spark作业，所有Executor可访问 | // 添加文件到Spark作业
sc.addFile("hdfs://path/to/config.txt");
sc.addFile("s3://bucket/data.json");

// 在Executor中访问文件
String filePath = SparkFiles.get("config.txt"); |
| `addJar` | jar: String | `SparkLauncher` | 添加JAR包到Spark作业 | // 添加依赖JAR包
sc.addJar("hdfs://path/to/dependency.jar");
sc.addJar("/local/path/to/lib.jar"); |
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


### SparkSaslClient
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getNegotiatedProperty` | name: String | `Object` | 获取NegotiatedProperty相关功能 | 传入参数执行获取NegotiatedProperty相关功能 |
| `handle` | callbacks: Callback&lt;&gt; | `void` | 处理相关功能 | 传入参数执行处理相关功能 |


### SparkSaslServer
**包路径**: `org.apache.spark.network.sasl`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `encodeIdentifier` | identifier: String | `String` | 编码Identifier相关功能 | 传入参数执行编码Identifier相关功能 |
| `getNegotiatedProperty` | name: String | `Object` | 获取NegotiatedProperty相关功能 | 传入参数执行获取NegotiatedProperty相关功能 |
| `handle` | callbacks: Callback&lt;&gt; | `void` | 处理相关功能 | 传入参数执行处理相关功能 |


### StageStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `StageStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |


### StorageLevels
**包路径**: `org.apache.spark.api.java`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `create` | useDisk: boolean, useMemory: boolean, useOffHeap: boolean, deserialized: boolean, replication: int | `StorageLevel` | 创建相关功能 | 传入参数执行创建相关功能 |


### StreamHandle
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `StreamHandle` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


### StreamInterceptor
**包路径**: `org.apache.spark.network.client`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `channelInactive` | 无 | `void` | 活跃相关功能 | 调用该方法执行活跃相关功能 |
| `exceptionCaught` | cause: Throwable | `void` | exceptionCaught操作 | 传入参数执行exceptionCaught操作 |
| `handle` | buf: ByteBuf | `boolean` | 处理相关功能 | 传入参数执行处理相关功能 |


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


### TSetIpAddressProcessor
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getUserIpAddress` | 无 | `String` | 获取UserIpAddress相关功能 | 调用该方法执行获取UserIpAddress相关功能 |
| `getUserName` | 无 | `String` | 获取UserName相关功能 | 调用该方法执行获取UserName相关功能 |
| `process` | in: final TProtocol, out: final TProtocol | `void` | 处理相关功能 | 传入参数执行处理相关功能 |


### TSubjectAssumingTransport
**包路径**: `org.apache.hive.service.auth`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `open` | 无 | `void` | 打开相关功能 | 调用该方法执行打开相关功能 |


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


### TableTypeMappingFactory
**包路径**: `org.apache.hive.service.cli.operation`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getTableTypeMapping` | mappingType: String | `TableTypeMapping` | 获取TableTypeMapping相关功能 | 传入参数执行获取TableTypeMapping相关功能 |


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


### TaskSorting
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `TaskSorting` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |


### TaskStatus
**包路径**: `org.apache.spark.status.api.v1`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | str: String | `TaskStatus` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |


### ThreadFactoryWithGarbageCleanup
**包路径**: `org.apache.hive.service.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getThreadRawStoreMap` | 无 | `Map&lt;Long, RawStore&gt;` | 获取ThreadRawStoreMap相关功能 | 调用该方法执行获取ThreadRawStoreMap相关功能 |
| `newThread` | runnable: Runnable | `Thread` | 读取相关功能 | 传入参数执行读取相关功能 |


### ThreadWithGarbageCleanup
**包路径**: `org.apache.hive.service.server`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `cacheThreadLocalRawStore` | 无 | `void` | 缓存ThreadLocalRawStore相关功能 | 调用该方法执行缓存ThreadLocalRawStore相关功能 |
| `finalize` | 无 | `void` | 终结相关功能 | 调用该方法执行终结相关功能 |


### ThriftBinaryCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GetQueryId` | req: TGetQueryIdReq | `TGetQueryIdResp` | 获取QueryId相关功能 | 传入参数执行获取QueryId相关功能 |
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |


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


### ThriftHttpCLIService
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | 无 | `void` | 运行相关功能 | 调用该方法执行运行相关功能 |


### ThriftHttpServlet
**包路径**: `org.apache.hive.service.cli.thrift`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `run` | 无 | `String` | 运行相关功能 | 调用该方法执行运行相关功能 |


### TransientBestEffortLazyVal
**包路径**: `org.apache.spark.util`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `apply` | 无 | `T` | 应用数据类型转换 | 获取数据类型对应的列向量 |


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


### TransportServer
**包路径**: `org.apache.spark.network.server`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `close` | 无 | `void` | 关闭相关功能 | 调用该方法执行关闭相关功能 |
| `getAllMetrics` | 无 | `MetricSet` | 获取AllMetrics相关功能 | 调用该方法执行获取AllMetrics相关功能 |
| `getPort` | 无 | `int` | 获取Port相关功能 | 调用该方法执行获取Port相关功能 |
| `getRegisteredConnections` | 无 | `Counter` | 获取RegisteredConnections相关功能 | 调用该方法执行获取RegisteredConnections相关功能 |


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


### UnsafeAlignedOffset
**包路径**: `org.apache.spark.unsafe`
**方法数量**: 4

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getSize` | object: Object, offset: long | `int` | 获取Size相关功能 | 传入参数执行获取Size相关功能 |
| `getUaoSize` | 无 | `int` | 获取UaoSize相关功能 | 调用该方法执行获取UaoSize相关功能 |
| `putSize` | object: Object, offset: long, value: int | `void` | 放入Size相关功能 | 传入参数执行放入Size相关功能 |
| `setUaoSize` | size: int | `void` | 设置UaoSize相关功能 | 传入参数执行设置UaoSize相关功能 |


### UnsafeMemoryAllocator
**包路径**: `org.apache.spark.unsafe.memory`
**方法数量**: 2

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `allocate` | size: long | `MemoryBlock` | 分配相关功能 | 传入参数执行分配相关功能 |
| `free` | memory: MemoryBlock | `void` | free操作 | 传入参数执行free操作 |


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


### UploadBlock
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `UploadBlock` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


### UploadBlockStream
**包路径**: `org.apache.spark.network.shuffle.protocol`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `decode` | buf: ByteBuf | `UploadBlockStream` | 解码相关功能 | 传入参数执行解码相关功能 |
| `encode` | buf: ByteBuf | `void` | 编码相关功能 | 传入参数执行编码相关功能 |
| `encodedLength` | 无 | `int` | 编码dLength相关功能 | 调用该方法执行编码dLength相关功能 |


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


### VariantSchema
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `isUnshredded` | 无 | `boolean` | 判断是否Unshredded相关功能 | 调用该方法执行判断是否Unshredded相关功能 |


### VariantShreddingWriter
**包路径**: `org.apache.spark.types.variant`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `castShredded` | v: Variant, schema: VariantSchema, builder: ShreddedResultBuilder | `ShreddedResult` | castShredded操作 | 传入参数执行castShredded操作 |


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


### VariantVal
**包路径**: `org.apache.spark.unsafe.types`
**方法数量**: 3

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `debugString` | 无 | `String` | 调试String相关功能 | 调用该方法执行调试String相关功能 |
| `readFromUnsafeRow` | offsetAndSize: long, baseObject: Object, baseOffset: long | `VariantVal` | 读取FromUnsafeRow相关功能 | 传入参数执行读取FromUnsafeRow相关功能 |
| `toJson` | zoneId: ZoneId | `String` | toJson操作 | 传入参数执行toJson操作 |


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


### instead
**包路径**: `org.apache.spark.api.java`
**方法数量**: 48

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `checkpoint` | 无 | `void` | checkpoint DStream | 调用该方法执行检查point相关功能 |
| `collect` | 无 | `JList` | 收集所有行 | // collect：将RDD收集到Driver端
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"));
List<String> list = rdd.collect();

// 注意：collect会将所有数据拉回Driver
// 数据量大时可能导致Driver内存溢出，慎用！ |
| `collectAsync` | 无 | `JavaFutureAction` | 收集Async相关功能 | 调用该方法执行收集Async相关功能 |
| `collectPartitions` | Array[Int]: partitionIds | `Array` | 收集Partitions相关功能 | 传入参数执行收集Partitions相关功能 |
| `count` | 无 | `Long` | 统计行数 | // count：统计元素总数
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c", "d", "e"));
long count = rdd.count();
// 结果: 5 |
| `countApprox` | timeout: Long, confidence: Double | `PartialResult` | 计数Approx相关功能 | 传入参数执行计数Approx相关功能 |
| `countApprox` | timeout: Long | `PartialResult` | 计数Approx相关功能 | 传入参数执行计数Approx相关功能 |
| `countApproxDistinct` | relativeSD: Double | `Long` | 计数ApproxDistinct相关功能 | 传入参数执行计数ApproxDistinct相关功能 |
| `countAsync` | 无 | `JavaFutureAction` | 计数Async相关功能 | 调用该方法执行计数Async相关功能 |
| `countByValue` | 无 | `JMap` | 统计每个批次每个值的出现次数 | 调用该方法执行计数ByValue相关功能 |
| `countByValueApprox` | timeout: Long, confidence: Double | `PartialResult` | 计数ByValueApprox相关功能 | 传入参数执行计数ByValueApprox相关功能 |
| `countByValueApprox` | timeout: Long | `PartialResult` | 计数ByValueApprox相关功能 | 传入参数执行计数ByValueApprox相关功能 |
| `first` | 无 | `T` | 第一行 | // first：获取第一个元素
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(10, 20, 30));
Integer first = rdd.first();
// 结果: 10 |
| `flatMapToDouble` | DoubleFlatMapFunction[T]: f | `JavaDoubleRDD` | 映射相关功能 | 传入参数执行映射相关功能 |
| `fold` | T: zeroValue | `void` | 使用零值和组合函数聚合RDD | 传入参数执行折叠/归约相关功能 |
| `foreach` | VoidFunction[T]: f | `void` | 对每个元素应用函数，用于副作用操作 | // foreach：对每个元素执行操作（副作用）
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("a", "b", "c"));

// 打印每个元素（在Executor上执行）
rdd.foreach(x -> System.out.println("Element: " + x));

// 写入外部系统
rdd.foreach(x -> {
    // 写入数据库、发送消息等
    database.insert(x);
}); |
| `foreachAsync` | VoidFunction[T]: f | `JavaFutureAction` | 检查是否存在相关功能 | 传入参数执行检查是否存在相关功能 |
| `foreachPartition` | VoidFunction[Iterator<T>]: f | `void` | 对每个分区应用函数 | 传入参数执行foreachPartition操作 |
| `foreachPartitionAsync` | VoidFunction[Iterator<T>]: f | `JavaFutureAction` | foreachPartitionAsync操作 | 传入参数执行foreachPartitionAsync操作 |
| `getCheckpointFile` | 无 | `Optional` | 获取CheckpointFile相关功能 | 调用该方法执行获取CheckpointFile相关功能 |
| `glom` | 无 | `JavaRDD` | glom操作 | 调用该方法执行glom操作 |
| `isEmpty` | 无 | `Boolean` | 判断是否为空 | 调用该方法执行判断是否Empty相关功能 |
| `iterator` | Partition: split, TaskContext: taskContext | `JIterator` | 获取迭代器 | 传入参数执行时期相关功能 |
| `mapPartitionsToDouble` | DoubleFlatMapFunction[Iterator<T>]: f | `JavaDoubleRDD` | 映射PartitionsToDouble相关功能 | 传入参数执行映射PartitionsToDouble相关功能 |
| `mapPartitionsToDouble` | DoubleFlatMapFunction[Iterator<T>]: f, preservesPartitioning: Boolean | `JavaDoubleRDD` | 映射PartitionsToDouble相关功能 | 传入参数执行映射PartitionsToDouble相关功能 |
| `max` | Comparator[T]: comp | `T` | 最大值 | // max：最大值
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));
double max = doubleRDD.max();
// 结果: 30.0 |
| `min` | Comparator[T]: comp | `T` | 最小值 | // min：最小值
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(Arrays.asList(10.0, 20.0, 5.0, 30.0));
double min = doubleRDD.min();
// 结果: 5.0 |
| `pipe` | command: String | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | List<String>: command | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | List<String>: command, JMap[String: env | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | List<String>: command, JMap[String: env, separateWorkingDir: Boolean, bufferSize: Int | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `pipe` | List<String>: command, JMap[String: env, separateWorkingDir: Boolean, bufferSize: Int, encoding: String | `JavaRDD` | pipe操作 | 传入参数执行pipe操作 |
| `reduce` | JFunction2[T: f | `T` | 聚合DStream每个RDD | // reduce：聚合所有元素为单个结果
JavaRDD<Integer> numbers = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

// 求和
Integer sum = numbers.reduce((a, b) -> a + b);
// 结果: 15

// 求最大值
Integer max = numbers.reduce((a, b) -> Math.max(a, b));
// 结果: 5

// 字符串拼接
JavaRDD<String> words = sc.parallelize(Arrays.asList("a", "b", "c"));
String concatenated = words.reduce((a, b) -> a + b);
// 结果: "abc" |
| `saveAsObjectFile` | path: String | `void` | 保存RDD为序列化对象文件 | 传入参数执行保存AsObjectFile相关功能 |
| `saveAsTextFile` | path: String | `void` | 保存RDD为文本文件 | // saveAsTextFile：保存为文本文件
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("line1", "line2", "line3"));
rdd.saveAsTextFile("hdfs://output/path/");

// 输出目录下会有多个文件：part-00000, part-00001... |
| `saveAsTextFile` | path: String, CompressionCodec]: codec | `void` | 保存RDD为文本文件 | // saveAsTextFile：保存为文本文件
JavaRDD<String> rdd = sc.parallelize(Arrays.asList("line1", "line2", "line3"));
rdd.saveAsTextFile("hdfs://output/path/");

// 输出目录下会有多个文件：part-00000, part-00001... |
| `take` | num: Int | `JList` | 取前n行 | // take：获取前n个元素
JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10));
List<Integer> top5 = rdd.take(5);
// 结果: [1, 2, 3, 4, 5] |
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

---

## 存储级别


### StorageLevelMapper
**包路径**: `org.apache.spark.storage`
**方法数量**: 1

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `fromString` | s: String | `StorageLevel` | 从字符串解析枚举值或配置 | 从字符串解析Avro压缩编解码器类型 |

---

---

## SparkSession（现代Spark入口）


### SparkSession
**包路径**: `org.apache.spark.sql`
**说明**: Spark 2.0+的主入口点，替代了旧版的SQLContext和HiveContext。提供DataFrame/Dataset创建、SQL执行等功能。
**方法数量**: 30+

**导入示例**:
```java
import org.apache.spark.sql.SparkSession;

// 创建SparkSession
SparkSession spark = SparkSession.builder()
    .appName("MyApp")
    .master("local[*]")
    .getOrCreate();
```


| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `builder` | 无 | `SparkSession.Builder` | 获取SparkSession构建器 | `SparkSession spark = SparkSession.builder()
    .appName("MyApp")
    .master("local[*]")
    .getOrCreate();` |
| `appName` | String name | `Builder` | 设置应用名称 | `SparkSession.builder().appName("DataProcessing").getOrCreate();` |
| `master` | String master | `Builder` | 设置运行模式（local/yarn等） | `SparkSession.builder().master("yarn").getOrCreate();` |
| `config` | String key, String value | `Builder` | 设置配置项 | `SparkSession.builder()
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate();` |
| `enableHiveSupport` | 无 | `Builder` | 启用Hive支持 | `SparkSession.builder().enableHiveSupport().getOrCreate();` |
| `getOrCreate` | 无 | `SparkSession` | 获取或创建SparkSession | `SparkSession spark = SparkSession.builder().getOrCreate();` |
| `version` | 无 | `String` | 获取Spark版本 | `String version = spark.version();
// 返回如 "3.5.6"` |
| `sparkContext` | 无 | `SparkContext` | 获取底层SparkContext | `SparkContext sc = spark.sparkContext();` |
| `sql` | String sqlText | `DataFrame` | 执行SQL查询 | `DataFrame result = spark.sql("SELECT * FROM table WHERE id > 100");` |
| `sql` | String sqlText, Map[String, Any] args | `DataFrame` | 执行带参数的SQL查询 | `Map<String, Any> args = new HashMap<>();
args.put("id", 100);
DataFrame result = spark.sql("SELECT * FROM table WHERE id > :id", args);` |
| `table` | String tableName | `DataFrame` | 从表名创建DataFrame | `DataFrame df = spark.table("my_table");` |
| `read` | 无 | `DataFrameReader` | 获取数据读取器 | `DataFrameReader reader = spark.read();
DataFrame df = reader.parquet("data.parquet");` |
| `readStream` | 无 | `DataStreamReader` | 获取流数据读取器 | `DataStreamReader reader = spark.readStream();` |
| `createDataFrame` | List[Row] rows, StructType schema | `DataFrame` | 从Java List创建DataFrame | `StructType schema = new StructType()
    .add("id", DataTypes.IntegerType)
    .add("name", DataTypes.StringType);
List<Row> rows = Arrays.asList(
    RowFactory.create(1, "Alice"),
    RowFactory.create(2, "Bob"));
DataFrame df = spark.createDataFrame(rows, schema);` |
| `createDataFrame` | JavaRDD[Row] rdd, StructType schema | `DataFrame` | 从JavaRDD创建DataFrame | `JavaRDD<Row> rowRDD = sc.parallelize(Arrays.asList(
    RowFactory.create(1, "Alice")));
DataFrame df = spark.createDataFrame(rowRDD, schema);` |
| `createDataset` | List[T] data, Encoder[T] encoder | `Dataset[T]` | 从Java List创建Dataset | `Encoder<Integer> encoder = Encoders.INT();
List<Integer> data = Arrays.asList(1, 2, 3);
Dataset<Integer> ds = spark.createDataset(data, encoder);` |
| `emptyDataFrame` | 无 | `DataFrame` | 创建空DataFrame | `DataFrame empty = spark.emptyDataFrame();` |
| `range` | long end | `Dataset[Long]` | 创建范围数据（0到end-1） | `Dataset<Long> range = spark.range(100);
// 生成0到99的序列` |
| `range` | long start, long end, long step, int numPartitions | `Dataset[Long]` | 创建范围数据，指定参数 | `Dataset<Long> range = spark.range(0, 100, 2, 10);
// 0, 2, 4, ... 98，10个分区` |
| `udf` | 无 | `UDFRegistration` | 获取UDF注册器 | `spark.udf().register("myFunc", (String s) -> s.toUpperCase(), DataTypes.StringType);` |
| `catalog` | 无 | `Catalog` | 获取Catalog接口 | `Catalog catalog = spark.catalog();
catalog.listTables().show();` |
| `conf` | 无 | `RuntimeConfig` | 获取运行时配置 | `RuntimeConfig conf = spark.conf();
conf.set("spark.sql.autoBroadcastJoinThreshold", "10MB");` |
| `newSession` | 无 | `SparkSession` | 创建新Session（隔离配置） | `SparkSession newSpark = spark.newSession();` |
| `stop` | 无 | `void` | 停止SparkSession | `spark.stop();` |
| `close` | 无 | `void` | 关闭SparkSession（Java友好） | `spark.close();` |
| `time` | T => T f | `T` | 测量函数执行时间 | `long result = spark.time(() -> {
    return df.count();
});
// 打印执行时间并返回结果` |
| `addTag` | String tag | `void` | 为操作添加标签 | `spark.addTag("batch-job");` |
| `removeTag` | String tag | `void` | 移除标签 | `spark.removeTag("batch-job");` |
| `getTags` | 无 | `Set[String]` | 获取所有标签 | `Set<String> tags = spark.getTags();` |
| `clearTags` | 无 | `void` | 清除所有标签 | `spark.clearTags();` |
| `interruptTag` | String tag | `Seq[String]` | 中断指定标签的操作 | `spark.interruptTag("batch-job");` |
| `interruptAll` | 无 | `Seq[String]` | 中断所有操作 | `spark.interruptAll();` |


### Dataset[T]（类型安全数据集）
**包路径**: `org.apache.spark.sql`
**说明**: Spark 2.0+的核心数据处理API，提供类型安全的数据操作。DataFrame是Dataset[Row]的特例。
**方法数量**: 80+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `show` | 无 | `void` | 显示前20行数据 | `df.show();` |
| `show` | int numRows | `void` | 显示指定行数 | `df.show(50);` |
| `show` | int numRows, boolean truncate | `void` | 显示指定行数，控制截断 | `df.show(50, false);  // 不截断长字符串` |
| `printSchema` | 无 | `void` | 打印schema结构 | `df.printSchema();` |
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
| `groupBy` | String col1, String... cols | `RelationalGroupedDataset` | 按列分组 | `RelationalGroupedDataset grouped = df.groupBy("category");
DataFrame result = grouped.count();` |
| `groupBy` | Column... cols | `RelationalGroupedDataset` | 按Column分组 | `RelationalGroupedDataset grouped = df.groupBy(col("category"), col("region"));` |
| `agg` | Column expr, Column... exprs | `DataFrame` | 聚合计算 | `DataFrame result = df.agg(count("id").alias("total"), avg("price").alias("avg_price"));` |
| `agg` | Map[String, String] exprs | `DataFrame` | 聚合（使用字符串表达式） | `Map<String, String> exprs = new HashMap<>();
exprs.put("id", "count");
exprs.put("price", "avg");
DataFrame result = df.agg(exprs);` |
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
| `join` | Dataset[_] right, String usingColumn, String joinType | `DataFrame` | 使用列名连接，指定类型 | `DataFrame result = df1.join(df2, "id", "left");
// joinType: inner, left, right, full, semi, anti` |
| `join` | Dataset[_] right, Column joinExprs | `DataFrame` | 使用条件连接 | `DataFrame result = df1.join(df2, col("df1.id").equalTo(col("df2.user_id")));` |
| `join` | Dataset[_] right, Column joinExprs, String joinType | `DataFrame` | 使用条件连接，指定类型 | `DataFrame result = df1.join(df2, col("id").equalTo(col("user_id")), "left");` |
| `crossJoin` | Dataset[_] right | `DataFrame` | 显式笛卡尔连接 | `DataFrame result = df1.crossJoin(df2);` |
| `joinWith` | Dataset[U] other, Column condition, String joinType | `Dataset[Tuple2[T, U]]` | 类型安全连接 | `Dataset<Tuple2<Row, Row>> result = ds1.joinWith(ds2, col("id").equalTo(col("user_id")), "inner");` |
| `leftOuterJoin` | JavaPairRDD[K, W] other | `JavaPairRDD[K, (V, Optional[W])]` | 左外连接（PairRDD） | `JavaPairRDD<String, Tuple2<Integer, Optional<String>>> result = pairRDD.leftOuterJoin(otherRDD);` |
| `rightOuterJoin` | JavaPairRDD[K, W] other | `JavaPairRDD[K, (Optional[V], W)]` | 右外连接（PairRDD） | `JavaPairRDD<String, Tuple2<Optional<Integer>, String>> result = pairRDD.rightOuterJoin(otherRDD);` |
| `fullOuterJoin` | JavaPairRDD[K, W] other | `JavaPairRDD[K, (Optional[V], Optional[W])]` | 全外连接（PairRDD） | `JavaPairRDD<String, Tuple2<Optional<Integer>, Optional<String>>> result = pairRDD.fullOuterJoin(otherRDD);` |
| `map` | MapFunction[T, U] func, Encoder[U] encoder | `Dataset[U]` | 映射转换（Java） | `Dataset<String> names = ds.map((MapFunction<Integer, String>) x -> "id:" + x, Encoders.STRING());` |
| `flatMap` | FlatMapFunction[T, U] func, Encoder[U] encoder | `Dataset[U]` | 扁平映射（Java） | `Dataset<String> words = ds.flatMap((FlatMapFunction<String, String>) s -> Arrays.asList(s.split(" ")).iterator(), Encoders.STRING());` |
| `mapPartitions` | MapPartitionsFunction[T, U] f, Encoder[U] encoder | `Dataset[U]` | 分区映射（Java） | `Dataset<Integer> partitionSums = ds.mapPartitions((MapPartitionsFunction<Integer, Integer>) iter -> {
    int sum = 0;
    while (iter.hasNext()) sum += iter.next();
    return Arrays.asList(sum).iterator();
}, Encoders.INT());` |
| `foreach` | ForeachFunction[T] func | `void` | 对每行执行操作（Java） | `df.foreach((ForeachFunction<Row>) row -> System.out.println(row));` |
| `foreachPartition` | ForeachPartitionFunction[T] func | `void` | 对每个分区执行操作（Java） | `df.foreachPartition((ForeachPartitionFunction<Row>) iter -> {
    while (iter.hasNext()) {
        Row row = iter.next();
        // 处理每行
    }
});` |
| `reduce` | ReduceFunction[T] func | `T` | 聚合（Java） | `Integer sum = ds.reduce((ReduceFunction<Integer>) (a, b) -> a + b);` |
| `groupByKey` | MapFunction[T, K] func, Encoder[K] encoder | `KeyValueGroupedDataset[K, T]` | 按键分组 | `KeyValueGroupedDataset<String, Integer> grouped = ds.groupByKey((MapFunction<Integer, String>) x -> "group_" + x % 3, Encoders.STRING());` |
| `withColumn` | String colName, Column col | `DataFrame` | 添加新列 | `DataFrame result = df.withColumn("double_age", col("age").multiply(2));` |
| `withColumnRenamed` | String existingName, String newName | `DataFrame` | 重命名列 | `DataFrame result = df.withColumnRenamed("old_name", "new_name");` |
| `withColumns` | Map[String, Column] colsMap | `DataFrame` | 批量添加列 | `Map<String, Column> cols = new HashMap<>();
cols.put("col1", col("a").plus(col("b")));
DataFrame result = df.withColumns(cols);` |
| `drop` | String colName | `DataFrame` | 删除列 | `DataFrame result = df.drop("unwanted_column");` |
| `drop` | String... colNames | `DataFrame` | 删除多列 | `DataFrame result = df.drop("col1", "col2");` |
| `drop` | Column col | `DataFrame` | 删除列（使用Column） | `DataFrame result = df.drop(col("unwanted"));` |
| `alias` | String alias | `Dataset[T]` | 设置别名 | `DataFrame aliased = df.alias("t1");
df.alias("t1").join(df.alias("t2"), col("t1.id").equalTo(col("t2.id")));` |
| `as` | String alias | `Dataset[T]` | 设置别名（同alias） | `DataFrame aliased = df.as("my_table");` |
| `toDF` | 无 | `DataFrame` | 转换为DataFrame | `DataFrame df = ds.toDF();` |
| `toDF` | String... colNames | `DataFrame` | 转换为DataFrame并重命名列 | `DataFrame df = ds.toDF("id", "name", "value");` |
| `as` | Encoder[U] encoder | `Dataset[U]` | 类型转换 | `Dataset<MyClass> ds = df.as(Encoders.bean(MyClass.class));` |
| `na` | 无 | `DataFrameNaFunctions` | 获取null值处理工具 | `DataFrameNaFunctions naFuncs = df.na();
DataFrame cleaned = df.na().drop();  // 删除含null的行` |
| `stat` | 无 | `DataFrameStatFunctions` | 获取统计工具 | `DataFrameStatFunctions statFuncs = df.stat();
double corr = df.stat().corr("col1", "col2");` |
| `describe` | String... cols | `DataFrame` | 计算统计描述 | `DataFrame stats = df.describe("age", "salary");
stats.show();  // 显示count, mean, stddev, min, max` |
| `summary` | String... statistics | `DataFrame` | 计算指定统计量 | `DataFrame stats = df.summary("count", "mean", "max");` |
| `sample` | double fraction | `Dataset[T]` | 随机采样 | `DataFrame sample = df.sample(0.1);  // 10%采样` |
| `sample` | boolean withReplacement, double fraction, long seed | `Dataset[T]` | 随机采样，指定参数 | `DataFrame sample = df.sample(false, 0.1, 42L);` |
| `randomSplit` | double[] weights | `Dataset[T][]` | 按权重随机分割 | `Dataset<Row>[] splits = df.randomSplit(new double[]{0.7, 0.3});
DataFrame train = splits[0];
DataFrame test = splits[1];` |
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
| `createTempView` | String viewName | `void` | 创建临时视图 | `df.createTempView("my_view");
spark.sql("SELECT * FROM my_view");` |
| `createOrReplaceTempView` | String viewName | `void` | 创建或替换临时视图 | `df.createOrReplaceTempView("my_view");` |
| `createGlobalTempView` | String viewName | `void` | 创建全局临时视图 | `df.createGlobalTempView("global_view");
spark.sql("SELECT * FROM global_temp.global_view");` |
| `write` | 无 | `DataFrameWriter[T]` | 获取写入器 | `df.write().mode("overwrite").parquet("output.parquet");` |
| `writeTo` | String table | `DataFrameWriterV2[T]` | 写入表（V2 API） | `df.writeTo("catalog.db.table").append();` |
| `writeStream` | 无 | `DataStreamWriter[T]` | 获取流写入器 | `df.writeStream().format("console").start();` |
| `inputFiles` | 无 | `String[]` | 获取输入文件列表 | `String[] files = df.inputFiles();` |
| `isEmpty` | 无 | `boolean` | 判断是否为空 | `boolean empty = df.isEmpty();` |
| `explain` | 无 | `void` | 打印执行计划 | `df.explain();` |
| `explain` | boolean extended | `void` | 打印详细执行计划 | `df.explain(true);  // 显示物理计划和逻辑计划` |
| `explain` | String mode | `void` | 打印执行计划（指定模式） | `df.explain("extended");
// mode: simple, extended, codegen, cost, formatted` |

---

## SparkConf（配置）


### SparkConf
**包路径**: `org.apache.spark`
**说明**: Spark配置类，用于设置各种Spark参数。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `set` | String key, String value | `SparkConf` | 设置配置项 | `SparkConf conf = new SparkConf().set("spark.executor.memory", "4g");` |
| `setMaster` | String master | `SparkConf` | 设置运行模式 | `conf.setMaster("local[4]");` |
| `setAppName` | String name | `SparkConf` | 设置应用名称 | `conf.setAppName("My Spark App");` |
| `setSparkHome` | String home | `SparkConf` | 设置Spark安装目录 | `conf.setSparkHome("/opt/spark");` |
| `setExecutorEnv` | String key, String value | `SparkConf` | 设置Executor环境变量 | `conf.setExecutorEnv("JAVA_HOME", "/usr/lib/jvm/java-11");` |
| `setJars` | String... jars | `SparkConf` | 设置依赖JAR包 | `conf.setJars("hdfs://libs/my-lib.jar");` |
| `setAll` | Map[String, String] settings | `SparkConf` | 批量设置配置 | `Map<String, String> settings = new HashMap<>();
settings.put("spark.executor.cores", "2");
conf.setAll(settings);` |
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
| `value` | 无 | `T` | 获取广播变量的值 | `Broadcast<Map<String, String>> config = sc.broadcast(configMap);
Map<String, String> map = config.value();` |
| `unpersist` | 无 | `void` | 从Executor释放广播变量 | `config.unpersist();` |
| `unpersist` | Boolean blocking | `void` | 从Executor释放，指定阻塞 | `config.unpersist(true);  // 阻塞等待释放` |
| `destroy` | 无 | `void` | 完全销毁广播变量 | `config.destroy();  // Driver和Executor都释放` |


### Accumulator[T]
**包路径**: `org.apache.spark`
**说明**: 累加器，用于聚合Worker端数据到Driver。仅支持累加操作。
**方法数量**: 6

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | T term | `void` | 累加值（只能在Worker端调用） | `Accumulator<Integer> acc = sc.accumulator(0);
rdd.foreach(x -> acc.add(x));` |
| `value` | 无 | `T` | 获取累加结果（只能在Driver端调用） | `int total = acc.value();` |
| `setValue` | T newValue | `void` | 设置值（只能在Driver端调用） | `acc.setValue(100);` |
| `isZero` | 无 | `Boolean` | 检查是否为零值 | `boolean zero = acc.isZero();` |
| `reset` | 无 | `void` | 重置为零值 | `acc.reset();` |
| `name` | 无 | `String` | 获取累加器名称 | `String name = acc.name();` |


### LongAccumulator / DoubleAccumulator / CollectionAccumulator
**包路径**: `org.apache.spark.util`
**说明**: 特化累加器，支持特定类型的累加。
**方法数量**: 5

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | Long/Double/T term | `void` | 累加值 | `LongAccumulator longAcc = sc.sc().longAccumulator("counter");
longAcc.add(10);` |
| `value` | 无 | `Long/Double/List[T]` | 获取累加结果 | `long sum = longAcc.value();` |
| `count` | 无 | `Long` | 获取计数（LongAccumulator） | `long count = longAcc.count();` |
| `avg` | 无 | `Double` | 获取平均值（LongAccumulator/DoubleAccumulator） | `double avg = longAcc.avg();` |
| `sum` | 无 | `Long/Double` | 获取总和 | `long sum = longAcc.sum();` |

---

## SQL辅助类


### Column
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame列引用，用于构建SQL表达式。
**方法数量**: 40+

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
| `when` | Column condition, Object value | `Column` | CASE WHEN条件 | `df.withColumn("category", when(col("age").lt(18), "child")
    .when(col("age").lt(60), "adult")
    .otherwise("senior"));` |
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
**方法数量**: 100+

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


### DataFrameReader
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame读取器，用于从各种数据源读取数据。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataFrameReader` | 指定数据源格式 | `spark.read().format("json").load("data.json");` |
| `option` | String key, String value | `DataFrameReader` | 设置选项（字符串） | `spark.read().option("header", "true").csv("data.csv");` |
| `option` | String key, boolean value | `DataFrameReader` | 设置选项（布尔） | `spark.read().option("multiline", true).json("data.json");` |
| `option` | String key, long value | `DataFrameReader` | 设置选项（长整数） | `spark.read().option("maxRowsPerFile", 10000L).format("csv");` |
| `options` | Map[String, String] options | `DataFrameReader` | 批量设置选项 | `Map<String, String> opts = new HashMap<>();
opts.put("header", "true");
spark.read().options(opts).csv("data.csv");` |
| `schema` | StructType schema | `DataFrameReader` | 指定schema | `StructType schema = DataTypes.createStructType(Arrays.asList(
    DataTypes.createStructField("id", DataTypes.IntegerType, true),
    DataTypes.createStructField("name", DataTypes.StringType, true)));
spark.read().schema(schema).csv("data.csv");` |
| `load` | 无 | `DataFrame` | 加载数据（用format指定格式） | `DataFrame df = spark.read().format("parquet").load("data.parquet");` |
| `load` | String path | `DataFrame` | 加载指定路径数据 | `DataFrame df = spark.read().format("json").load("data/*.json");` |
| `load` | String... paths | `DataFrame` | 加载多个路径数据 | `DataFrame df = spark.read().parquet("data1.parquet", "data2.parquet");` |
| `json` | String path | `DataFrame` | 读取JSON文件 | `DataFrame df = spark.read().json("data.json");` |
| `json` | Dataset[String] jsonDataset | `DataFrame` | 从Dataset读取JSON | `Dataset<String> jsonStrings = spark.createDataset(Arrays.asList("{"id":1}"), Encoders.STRING());
DataFrame df = spark.read().json(jsonStrings);` |
| `csv` | String path | `DataFrame` | 读取CSV文件 | `DataFrame df = spark.read().option("header", "true").csv("data.csv");` |
| `parquet` | String path | `DataFrame` | 读取Parquet文件 | `DataFrame df = spark.read().parquet("data.parquet");` |
| `orc` | String path | `DataFrame` | 读取ORC文件 | `DataFrame df = spark.read().orc("data.orc");` |
| `avro` | String path | `DataFrame` | 读取Avro文件 | `DataFrame df = spark.read().format("avro").load("data.avro");` |
| `text` | String path | `DataFrame` | 读取文本文件（每行一条记录） | `DataFrame df = spark.read().text("data.txt");` |
| `table` | String tableName | `DataFrame` | 从表读取数据 | `DataFrame df = spark.read().table("my_table");` |
| `jdbc` | String url, String table, Properties properties | `DataFrame` | 从JDBC读取数据 | `Properties props = new Properties();
props.put("user", "root");
props.put("password", "pwd");
DataFrame df = spark.read().jdbc("jdbc:mysql://localhost/db", "users", props);` |


### DataFrameWriter[T]
**包路径**: `org.apache.spark.sql`
**说明**: DataFrame写入器，用于将数据写入各种数据源。
**方法数量**: 20+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataFrameWriter[T]` | 指定输出格式 | `df.write().format("parquet").save("output");` |
| `option` | String key, String value | `DataFrameWriter[T]` | 设置选项（字符串） | `df.write().option("header", "true").csv("output");` |
| `option` | String key, boolean value | `DataFrameWriter[T]` | 设置选项（布尔） | `df.write().option("compression", "snappy").parquet("output");` |
| `options` | Map[String, String] options | `DataFrameWriter[T]` | 批量设置选项 | `Map<String, String> opts = new HashMap<>();
opts.put("header", "true");
df.write().options(opts).csv("output");` |
| `mode` | SaveMode mode | `DataFrameWriter[T]` | 设置写入模式 | `df.write().mode(SaveMode.Append).parquet("output");` |
| `mode` | String mode | `DataFrameWriter[T]` | 设置写入模式字符串 | `df.write().mode("overwrite").parquet("output");  // overwrite/append/ignore/errorIfExists` |
| `partitionBy` | String... colNames | `DataFrameWriter[T]` | 按列分区存储 | `df.write().partitionBy("year", "month").parquet("output");` |
| `bucketBy` | int numBuckets, String colName, String... colNames | `DataFrameWriter[T]` | 分桶存储 | `df.write().bucketBy(100, "id").sortBy("timestamp").saveAsTable("bucketed_table");` |
| `sortBy` | String... colNames | `DataFrameWriter[T]` | 分桶内排序 | `df.write().bucketBy(100, "id").sortBy("name").saveAsTable("sorted_table");` |
| `save` | 无 | `void` | 保存数据（用format指定格式） | `df.write().format("parquet").save();` |
| `save` | String path | `void` | 保存到指定路径 | `df.write().parquet("output/data.parquet");` |
| `saveAsTable` | String tableName | `void` | 保存为表 | `df.write().saveAsTable("my_table");` |
| `insertInto` | String tableName | `void` | 插入到表（不创建新表） | `df.write().insertInto("existing_table");` |
| `json` | String path | `void` | 写入JSON文件 | `df.write().json("output/data.json");` |
| `csv` | String path | `void` | 写入CSV文件 | `df.write().option("header", "true").csv("output/data.csv");` |
| `parquet` | String path | `void` | 写入Parquet文件 | `df.write().parquet("output/data.parquet");` |
| `orc` | String path | `void` | 写入ORC文件 | `df.write().orc("output/data.orc");` |
| `avro` | String path | `void` | 写入Avro文件 | `df.write().format("avro").save("output/data.avro");` |
| `text` | String path | `void` | 写入文本文件 | `df.select(col("text_col")).write().text("output/data.txt");` |
| `jdbc` | String url, String table, Properties connectionProperties | `void` | 写入JDBC表 | `Properties props = new Properties();
props.put("user", "root");
props.put("password", "pwd");
df.write().jdbc("jdbc:mysql://localhost/db", "users", props);` |


### Catalog
**包路径**: `org.apache.spark.sql.catalog`
**说明**: Spark Catalog接口，用于管理数据库、表、函数等元数据。
**方法数量**: 20+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `currentDatabase` | 无 | `String` | 获取当前数据库 | `String db = spark.catalog().currentDatabase();` |
| `setCurrentDatabase` | String db | `void` | 设置当前数据库 | `spark.catalog().setCurrentDatabase("my_db");` |
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
| `createDatabase` | String dbName, boolean ignoreIfExists | `void` | 创建数据库 | `spark.catalog().createDatabase("new_db", true);` |
| `createDatabase` | String dbName, boolean ignoreIfExists, String comment | `void` | 创建数据库（带注释） | `spark.catalog().createDatabase("new_db", false, "My test database");` |
| `dropDatabase` | String dbName, boolean ignoreIfNotExists, boolean cascade | `void` | 删除数据库 | `spark.catalog().dropDatabase("old_db", true, false);` |
| `createTable` | String tableName, String path | `void` | 创建表（指定路径） | `spark.catalog().createTable("new_table", "hdfs://data/path");` |
| `createTable` | String tableName, String path, String source | `void` | 创建表（指定格式） | `spark.catalog().createTable("new_table", "hdfs://data", "parquet");` |
| `createExternalTable` | String tableName, String path | `DataFrame` | 创建外部表 | `DataFrame df = spark.catalog().createExternalTable("ext_table", "hdfs://data");` |
| `createExternalTable` | String tableName, String path, String source | `DataFrame` | 创建外部表（指定格式） | `DataFrame df = spark.catalog().createExternalTable("ext_table", "hdfs://data", "parquet");` |
| `dropTable` | String dbName, String tableName, boolean ignoreIfNotExists, boolean purge | `void` | 删除表 | `spark.catalog().dropTable("my_db", "old_table", true, false);` |
| `dropTable` | String tableName, boolean ignoreIfNotExists, boolean purge | `void` | 删除当前库表 | `spark.catalog().dropTable("old_table", true, false);` |
| `dropTempView` | String viewName | `void` | 删除临时视图 | `spark.catalog().dropTempView("temp_view");` |
| `dropGlobalTempView` | String viewName | `void` | 删除全局临时视图 | `spark.catalog().dropGlobalTempView("global_view");` |
| `recoverPartitions` | String tableName | `void` | 恢复分区信息 | `spark.catalog().recoverPartitions("partitioned_table");` |
| `refreshTable` | String tableName | `void` | 刷新表缓存 | `spark.catalog().refreshTable("my_table");` |
| `refreshByPath` | String path | `void` | 刷新指定路径缓存 | `spark.catalog().refreshByPath("hdfs://data/table");` |
| `clearCache` | 无 | `void` | 清除所有缓存 | `spark.catalog().clearCache();` |
| `isCached` | String tableName | `Boolean` | 检查表是否被缓存 | `boolean cached = spark.catalog().isCached("my_table");` |
| `cacheTable` | String tableName | `void` | 缓存表 | `spark.catalog().cacheTable("my_table");` |
| `uncacheTable` | String tableName | `void` | 取消缓存表 | `spark.catalog().uncacheTable("my_table");` |


### UDFRegistration
**包路径**: `org.apache.spark.sql`
**说明**: UDF注册接口，用于注册用户自定义函数。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `register` | String name, UDF1[T1, R] f, DataType returnType | `void` | 注册UDF（1个参数） | `spark.udf().register("myUpper", (String s) -> s.toUpperCase(), DataTypes.StringType);` |
| `register` | String name, UDF2[T1, T2, R] f, DataType returnType | `void` | 注册UDF（2个参数） | `spark.udf().register("concat2", (String a, String b) -> a + b, DataTypes.StringType);` |
| `register` | String name, UDF3[T1, T2, T3, R] f, DataType returnType | `void` | 注册UDF（3个参数） | `spark.udf().register("combine3", (String a, String b, String c) -> a+b+c, DataTypes.StringType);` |
| `register` | String name, UDF4[T1, T2, T3, T4, R] f, DataType returnType | `void` | 注册UDF（4个参数） | - |
| `register` | String name, UDF5... | `void` | 注册UDF（5+参数） | - |
| `register` | String name, UDAF udaf | `void` | 注册聚合UDF | `spark.udf().register("mySum", new MySumUDAF());` |
| `register` | String name, UserDefinedAggregateFunction udaf | `void` | 注册聚合UDF（旧API） | - |
| `registerJava` | String name, String className, DataType returnType | `void` | 注册Java UDF类 | `spark.udf().registerJava("myFunc", "com.example.MyUDF", DataTypes.StringType);` |
| `registerPython` | String name, String command, DataType returnType | `void` | 注册Python UDF | - |
| `callUDF` | String udfName, Column... cols | `Column` | 调用已注册的UDF | `df.select(callUDF("myUpper", col("name")));` |

---

## Streaming流处理API


### JavaStreamingContext
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Spark Streaming的Java入口，用于创建DStream和处理实时数据流。
**方法数量**: 25+

**导入示例**:
```java
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaSparkContext;

// 创建StreamingContext
JavaStreamingContext jssc = new JavaStreamingContext(
    sc, Durations.seconds(5)
);
```


| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `JavaStreamingContext` | SparkConf conf, Duration batchDuration | 构造方法 | 创建StreamingContext | `SparkConf conf = new SparkConf().setAppName("Streaming");
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));` |
| `JavaStreamingContext` | JavaSparkContext sparkContext, Duration batchDuration | 构造方法 | 从JavaSparkContext创建 | `JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));` |
| `textFileStream` | String directory | `JavaDStream[String]` | 监控目录中的新文本文件 | `JavaDStream<String> lines = jssc.textFileStream("hdfs://logs/");` |
| `fileStream` | String directory, Class[K] keyClass, Class[V] valueClass, Class[F] inputFormatClass | `JavaPairDStream[K, V]` | 监控目录中的新文件（指定格式） | `JavaPairDStream<Text, IntWritable> files = jssc.fileStream("hdfs://input/", Text.class, IntWritable.class, TextInputFormat.class);` |
| `socketTextStream` | String hostname, int port | `JavaDStream[String]` | 从TCP socket读取文本流 | `JavaDStream<String> socketStream = jssc.socketTextStream("localhost", 9999);` |
| `socketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 从socket读取，指定存储级别 | `JavaReceiverInputDStream<String> stream = jssc.socketStream("localhost", 9999, StorageLevel.MEMORY_ONLY());` |
| `rawSocketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 原始socket流 | - |
| `kafkaStream` | Map[String, String] kafkaParams, Map[String, Integer] topics | `JavaPairDStream[String, String]` | 从Kafka读取流 | `Map<String, String> kafkaParams = new HashMap<>();
kafkaParams.put("bootstrap.servers", "localhost:9092");
Map<String, Integer> topics = new HashMap<>();
topics.put("my_topic", 1);
JavaPairDStream<String, String> kafkaStream = jssc.kafkaStream(kafkaParams, topics);` |
| `flumeStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[SparkFlumeEvent]` | 从Flume读取流 | `JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = jssc.flumeStream("localhost", 41414, StorageLevel.MEMORY_ONLY());` |
| `queueStream` | Queue[JavaRDD[T]] rdds | `JavaInputDStream[T]` | 从RDD队列创建测试流 | `Queue<JavaRDD<String>> queue = new LinkedList<>();
queue.add(sc.parallelize(Arrays.asList("a", "b")));
JavaInputDStream<String> testStream = jssc.queueStream(queue);` |
| `queueStream` | Queue[JavaRDD[T]] rdds, boolean oneAtATime | `JavaInputDStream[T]` | 逐个RDD处理 | `JavaInputDStream<String> stream = jssc.queueStream(queue, true);` |
| `union` | JavaDStream[T]... streams | `JavaDStream[T]` | 合合多个DStream | `JavaDStream<String> combined = jssc.union(stream1, stream2);` |
| `transform` | JavaDStream[T] dstream, JFunction[JavaRDD[T], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对DStream每个RDD应用变换 | `JavaDStream<String> transformed = dstream.transform(rdd -> rdd.filter(s -> s.length() > 3));` |
| `transformWith` | JavaDStream[T] dstream1, JavaDStream[W] dstream2, JFunction2[JavaRDD[T], JavaRDD[W], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对两个DStream每个RDD应用变换 | - |
| `checkpoint` | String directory | `void` | 设置checkpoint目录 | `jssc.checkpoint("hdfs://checkpoint/streaming/");` |
| `start` | 无 | `void` | 启动Streaming | `jssc.start();` |
| `awaitTermination` | 无 | `void` | 阻塞等待终止 | `jssc.awaitTermination();` |
| `awaitTerminationOrTimeout` | long timeout | `void` | 阻塞等待终止或超时 | `jssc.awaitTerminationOrTimeout(60000L);  // 最多等待60秒` |
| `stop` | 无 | `void` | 停止Streaming | `jssc.stop();` |
| `stop` | boolean stopSparkContext | `void` | 停止Streaming，控制是否停SparkContext | `jssc.stop(false);  // 停止Streaming但保留SparkContext` |
| `stop` | boolean stopSparkContext, boolean stopGracefully | `void` | 停止Streaming，控制优雅停止 | `jssc.stop(true, true);  // 优雅停止处理中的数据` |
| `close` | 无 | `void` | 关闭（Java友好） | `jssc.close();` |
| `sparkContext` | 无 | `JavaSparkContext` | 获取底层JavaSparkContext | `JavaSparkContext sc = jssc.sparkContext();` |
| `ssc` | 无 | `StreamingContext` | 获取底层Scala StreamingContext | `StreamingContext ssc = jssc.ssc();` |


### JavaDStream[T]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Java版本的DStream（离散化流），代表连续的RDD序列。
**方法数量**: 30+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `map` | JFunction[T, U] f | `JavaDStream[U]` | 对每个元素映射 | `JavaDStream<Integer> lengths = lines.map(s -> s.length());` |
| `flatMap` | FlatMapFunction[T, U] f | `JavaDStream[U]` | 对每个元素映射为多个输出 | `JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());` |
| `filter` | JFunction[T, Boolean] f | `JavaDStream[T]` | 过滤元素 | `JavaDStream<String> filtered = lines.filter(s -> s.length() > 3);` |
| `mapToPair` | PairFunction[T, K, V] f | `JavaPairDStream[K, V]` | 映射为键值对 | `JavaPairDStream<String, Integer> pairs = words.mapToPair(w -> new Tuple2<>(w, 1));` |
| `reduce` | Function2<T, T, T> f | `JavaDStream[T]` | 对每个RDD内元素聚合 | `JavaDStream<Integer> sums = numbers.reduce((a, b) -> a + b);` |
| `count` | 无 | `JavaDStream[Long]` | 对每个RDD计数 | `JavaDStream<Long> counts = dstream.count();` |
| `countByValue` | 无 | `JavaPairDStream[T, Long]` | 对每个RDD统计每个值的出现次数 | `JavaPairDStream<String, Long> wordCounts = words.countByValue();` |
| `reduceByKey` | JFunction2[V, V, V] func | `JavaPairDStream[K, V]` | 按Key聚合 | `JavaPairDStream<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);` |
| `groupByKey` | 无 | `JavaPairDStream[K, Iterable<V>]` | 按Key分组 | `JavaPairDStream<String, Iterable<Integer>> grouped = pairs.groupByKey();` |
| `mapValues` | JFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value映射 | `JavaPairDStream<String, String> transformed = pairs.mapValues(v -> "value:" + v);` |
| `flatMapValues` | FlatMapFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value扁平映射 | - |
| `foreachRDD` | VoidFunction[JavaRDD[T]] foreachFunc | `void` | 对每个RDD执行操作 | `wordCounts.foreachRDD(rdd -> {
    rdd.foreach(pair -> System.out.println(pair._1() + ": " + pair._2()));
});` |
| `transform` | JFunction[JavaRDD[T], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对每个RDD变换 | `JavaDStream<String> transformed = dstream.transform(rdd -> rdd.distinct());` |
| `transformToPair` | JFunction[JavaRDD[T], JavaPairRDD[K, V]] transformFunc | `JavaPairDStream[K, V]` | 对每个RDD变换为PairRDD | - |
| `union` | JavaDStream[T] other | `JavaDStream[T]` | 合合DStream | `JavaDStream<String> merged = stream1.union(stream2);` |
| `glom` | 无 | `JavaDStream[List<T>]` | 将每个RDD分区合并为List | - |
| `slice` | Duration fromTime, Duration toTime | `List[JavaRDD[T]]` | 获取时间范围内的RDD列表 | `List<JavaRDD<String>> rdds = dstream.slice(Durations.seconds(10), Durations.seconds(20));` |
| `window` | Duration windowDuration | `JavaDStream[T]` | 窗口操作 | `JavaDStream<String> windowed = dstream.window(Durations.seconds(30));  // 30秒窗口` |
| `window` | Duration windowDuration, Duration slideDuration | `JavaDStream[T]` | 窗口操作，指定滑动间隔 | `JavaDStream<String> windowed = dstream.window(Durations.seconds(30), Durations.seconds(10));  // 30秒窗口，每10秒滑动` |
| `reduceByWindow` | Function2<T, T, T> reduceFunc, Duration windowDuration, Duration slideDuration | `JavaDStream[T]` | 窗口聚合 | `JavaDStream<Integer> windowSum = numbers.reduceByWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(10));` |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合 | `JavaPairDStream<String, Integer> windowCounts = pairs.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30));` |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合，指定滑动 | - |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, JFunction2[V, V, V] invReduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合（带逆函数，高效） | `JavaPairDStream<String, Integer> counts = pairs.reduceByKeyAndWindow(
    (a, b) -> a + b,  // 加新数据
    (a, b) -> a - b,  // 减旧数据（高效计算）
    Durations.seconds(30), Durations.seconds(10));` |
| `countByWindow` | Duration windowDuration, Duration slideDuration | `JavaDStream[Long]` | 窗口内计数 | `JavaDStream<Long> counts = dstream.countByWindow(Durations.seconds(30), Durations.seconds(10));` |
| `countByValueAndWindow` | Duration windowDuration, Duration slideDuration | `JavaPairDStream[T, Long]` | 窗口内按值计数 | - |
| `checkpoint` | 无 | `JavaDStream[T]` | 启用checkpoint | `dstream.checkpoint();` |
| `persist` | StorageLevel level | `JavaDStream[T]` | 持久化DStream | `dstream.persist(StorageLevel.MEMORY_ONLY());` |
| `cache` | 无 | `JavaDStream[T]` | 缓存DStream | `dstream.cache();` |
| `print` | 无 | `void` | 打印每个RDD的前10元素 | `dstream.print();` |
| `saveAsTextFiles` | String prefix, String suffix | `void` | 保存为文本文件序列 | `dstream.saveAsTextFiles("output/stream", "txt");  // 生成output/stream-TIME.txt` |
| `saveAsObjectFiles` | String prefix, String suffix | `void` | 保存为对象文件序列 | `dstream.saveAsObjectFiles("output/stream", "obj");` |


### JavaPairDStream[K, V]
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: 键值对版本的DStream，继承JavaDStream并添加键值对操作。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `keys` | 无 | `JavaDStream[K]` | 获取所有Key的DStream | `JavaDStream<String> keys = pairs.keys();` |
| `values` | 无 | `JavaDStream[V]` | 获取所有Value的DStream | `JavaDStream<Integer> values = pairs.values();` |
| `join` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[V, W]]` | 内连接 | `JavaPairDStream<String, Tuple2<Integer, String>> joined = pairs.join(otherPairs);` |
| `join` | JavaPairDStream[K, W] other, Duration windowDuration | `JavaPairDStream[K, Tuple2[V, W]]` | 窗口内连接 | `JavaPairDStream<String, Tuple2<Integer, String>> joined = pairs.join(otherPairs, Durations.seconds(30));` |
| `leftOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[V, Optional[W]]]` | 左外连接 | `JavaPairDStream<String, Tuple2<Integer, Optional<String>>> joined = pairs.leftOuterJoin(otherPairs);` |
| `rightOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[Optional[V], W]]` | 右外连接 | `JavaPairDStream<String, Tuple2<Optional<Integer>, String>> joined = pairs.rightOuterJoin(otherPairs);` |
| `fullOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[Optional[V], Optional[W]]]` | 全外连接 | - |
| `cogroup` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[Iterable<V>, Iterable[W]]]` | 共同分组 | - |
| `updateStateByKey` | JFunction2[JList[V], Optional[S], Optional[S]] updateFunc | `JavaPairDStream[K, S]` | 更新状态（带状态计算） | `JavaPairDStream<String, Integer> stateCounts = wordCounts.updateStateByKey((values, state) -> {
    int sum = state.orElse(0);
    for (int v : values) sum += v;
    return Optional.of(sum);
});` |
| `mapWithState` | StateSpec[K, V, S, M] spec | `JavaMapWithStateDStream[K, V, S, M]` | 高效状态更新 | - |

---

## MLlib机器学习算法API


### KMeans / KMeansModel
**包路径**: `org.apache.spark.mllib.clustering`
**说明**: K-Means聚类算法和模型。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations | `KMeansModel` | 训练K-Means模型 | `JavaRDD<Vector> data = vectorsRDD;
KMeansModel model = KMeans.train(data.rdd(), 3, 20);  // 3个簇，20次迭代` |
| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs | `KMeansModel` | 训练模型，多次运行 | - |
| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs, String initializationMode | `KMeansModel` | 指定初始化模式 | `KMeansModel model = KMeans.train(data.rdd(), 3, 20, 1, "k-means||");` |
| `predict` | Vector point | `Int` | 预测单个点的簇归属 | `int cluster = model.predict(vector);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Integer]` | 预测多个点的簇归属 | `JavaRDD<Integer> predictions = model.predict(data);` |
| `clusterCenters` | 无 | `Vector[]` | 获取所有簇中心 | `Vector[] centers = model.clusterCenters();` |
| `k` | 无 | `Int` | 获取簇数量 | `int k = model.k();` |
| `computeCost` | JavaRDD[Vector] data | `Double` | 计算聚类成本（误差平方和） | `double cost = model.computeCost(data.rdd());` |


### BisectingKMeans / BisectingKMeansModel
**包路径**: `org.apache.spark.mllib.clustering`
**说明**: 二分K-Means聚类，层次聚类算法。
**方法数量**: 8+

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


### LogisticRegressionModel / LogisticRegressionWithSGD
**包路径**: `org.apache.spark.mllib.classification`
**说明**: 逻辑回归分类模型。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `LogisticRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations | `LogisticRegressionModel` | SGD训练逻辑回归 | `JavaRDD<LabeledPoint> training = labeledRDD;
LogisticRegressionModel model = LogisticRegressionWithSGD.train(training.rdd(), 100);` |
| `LogisticRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations, double stepSize | `LogisticRegressionModel` | 指定步长 | `LogisticRegressionModel model = LogisticRegressionWithSGD.train(training.rdd(), 100, 1.0);` |
| `LogisticRegressionWithSGD.train` | ... int regParam, int miniBatchFraction | `LogisticRegressionModel` | 指定正则化和批次比例 | - |
| `predict` | Vector point | `Double` | 预测类别（0或1） | `double label = model.predict(features);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Double]` | 批量预测 | `JavaRDD<Double> predictions = model.predict(testData);` |
| `predictProbabilities` | JavaRDD[Vector] points | `JavaRDD[Vector]` | 预测概率 | `JavaRDD<Vector> probs = model.predictProbabilities(testData);` |
| `weights` | 无 | `Vector` | 获取模型权重 | `Vector weights = model.weights();` |
| `intercept` | 无 | `Double` | 获取截距 | `double intercept = model.intercept();` |
| `clearThreshold` | 无 | `LogisticRegressionModel` | 清除阈值，返回概率 | `model.clearThreshold();` |
| `setThreshold` | double threshold | `LogisticRegressionModel` | 设置分类阈值 | `model.setThreshold(0.5);` |


### SVMModel / SVMWithSGD
**包路径**: `org.apache.spark.mllib.classification`
**说明**: SVM支持向量机分类模型。
**方法数量**: 8+

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
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `NaiveBayes.train` | JavaRDD[LabeledPoint] data, double lambda | `NaiveBayesModel` | 训练朴素贝叶斯模型 | `NaiveBayesModel model = NaiveBayes.train(training.rdd(), 1.0);` |
| `predict` | Vector point | `Double` | 预测类别 | `double label = model.predict(features);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Double]` | 批量预测 | `JavaRDD<Double> predictions = model.predict(testData);` |
| `predictProbabilities` | JavaRDD[Vector] points | `JavaRDD[Vector]` | 预测概率分布 | `JavaRDD<Vector> probs = model.predictProbabilities(testData);` |
| `labels` | 无 | `Double[]` | 获取所有类别标签 | `double[] labels = model.labels();` |
| `pi` | 无 | `Vector` | 获取类别先验概率 | `Vector pi = model.pi();` |


### LinearRegressionModel / LinearRegressionWithSGD
**包路径**: `org.apache.spark.mllib.regression`
**说明**: 线性回归模型。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `LinearRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations | `LinearRegressionModel` | SGD训练线性回归 | `LinearRegressionModel model = LinearRegressionWithSGD.train(training.rdd(), 100);` |
| `LinearRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations, double stepSize | `LinearRegressionModel` | 指定步长 | `LinearRegressionModel model = LinearRegressionWithSGD.train(training.rdd(), 100, 0.1);` |
| `predict` | Vector point | `Double` | 预测值 | `double value = model.predict(features);` |
| `predict` | JavaRDD[Vector] points | `JavaRDD[Double]` | 批量预测 | `JavaRDD<Double> predictions = model.predict(testData);` |
| `weights` | 无 | `Vector` | 获取权重 | `Vector weights = model.weights();` |
| `intercept` | 无 | `Double` | 获取截距 | `double intercept = model.intercept();` |


### ALS / MatrixFactorizationModel
**包路径**: `org.apache.spark.mllib.recommendation`
**说明**: ALS协同过滤推荐算法。
**方法数量**: 12+

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
| `predictAll` | JavaRDD[Tuple2[Int, Int]] usersProducts | `JavaRDD[Rating]` | 预测所有（同predict） | - |
| `recommendProducts` | int user, int num | `Rating[]` | 为用户推荐产品 | `Rating[] top5 = model.recommendProducts(userId, 5);` |
| `recommendUsers` | int product, int num | `Rating[]` | 为产品推荐用户 | `Rating[] top5Users = model.recommendUsers(productId, 5);` |
| `productFeatures` | 无 | `JavaPairRDD[Int, Vector]` | 获取产品特征矩阵 | `JavaPairRDD<Integer, Vector> features = model.productFeatures();` |
| `userFeatures` | 无 | `JavaPairRDD[Int, Vector]` | 获取用户特征矩阵 | `JavaPairRDD<Integer, Vector> features = model.userFeatures();` |
| `rank` | 无 | `Int` | 获取隐藏因子数量 | `int rank = model.rank();` |


### PCA
**包路径**: `org.apache.spark.mllib.feature`
**说明**: PCA主成分分析降维。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `PCA` | int k | 构造方法 | 创建PCA变换器 | `PCA pca = new PCA(3);  // 降到3维` |
| `fit` | JavaRDD[Vector] data | `PCAModel` | 训练PCA模型 | `PCAModel model = pca.fit(data.rdd());` |
| `transform` | Vector vector | `Vector` | 转换向量 | `Vector reduced = model.transform(originalVector);` |
| `transform` | JavaRDD[Vector] data | `JavaRDD[Vector]` | 批量转换 | `JavaRDD<Vector> reduced = model.transform(data);` |
| `pc` | 无 | `Matrix` | 获取主成分矩阵 | `Matrix principalComponents = model.pc();` |
| `explainedVariance` | 无 | `Vector` | 获取解释方差比例 | `Vector variance = model.explainedVariance();` |


### StandardScaler / StandardScalerModel
**包路径**: `org.apache.spark.mllib.feature`
**说明**: 标准化变换器，将特征标准化到均值0、方差1。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StandardScaler` | boolean withMean, boolean withStd | 构造方法 | 创建标准化变换器 | `StandardScaler scaler = new StandardScaler(true, true);  // 均值和方差标准化` |
| `fit` | JavaRDD[Vector] data | `StandardScalerModel` | 训练标准化模型 | `StandardScalerModel model = scaler.fit(data.rdd());` |
| `transform` | Vector vector | `Vector` | 转换向量 | `Vector scaled = model.transform(originalVector);` |
| `transform` | JavaRDD[Vector] data | `JavaRDD[Vector]` | 批量转换 | `JavaRDD<Vector> scaled = model.transform(data);` |
| `mean` | 无 | `Vector` | 获取均值 | `Vector mean = model.mean();` |
| `std` | 无 | `Vector` | 获取标准差 | `Vector std = model.std();` |


### Normalizer
**包路径**: `org.apache.spark.mllib.feature`
**说明**: 归一化变换器，将向量归一化到单位长度。
**方法数量**: 3+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Normalizer` | double p | 构造方法 | 创建归一化器 | `Normalizer normalizer = new Normalizer(2.0);  // L2归一化` |
| `transform` | Vector vector | `Vector` | 归一化向量 | `Vector normalized = normalizer.transform(originalVector);` |
| `transform` | JavaRDD[Vector] data | `JavaRDD[Vector]` | 批量归一化 | `JavaRDD<Vector> normalized = normalizer.transform(data);` |


### Word2Vec / Word2VecModel
**包路径**: `org.apache.spark.mllib.feature`
**说明**: Word2Vec词向量训练。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setVectorSize` | int vectorSize | `Word2Vec` | 设置向量维度 | `Word2Vec w2v = new Word2Vec().setVectorSize(100);` |
| `setWindowSize` | int windowSize | `Word2Vec` | 设置窗口大小 | `w2v.setWindowSize(5);` |
| `setMinCount` | int minCount | `Word2Vec` | 设置最小词频 | `w2v.setMinCount(10);  // 出现少于10次的词被忽略` |
| `setNumIterations` | int numIterations | `Word2Vec` | 设置迭代次数 | `w2v.setNumIterations(10);` |
| `setLearningRate` | double learningRate | `Word2Vec` | 设置学习率 | `w2v.setLearningRate(0.025);` |
| `setNumPartitions` | int numPartitions | `Word2Vec` | 设置分区数 | `w2v.setNumPartitions(4);` |
| `fit` | JavaRDD[String] data | `Word2VecModel` | 训练词向量 | `JavaRDD<String> documents = sc.parallelize(Arrays.asList("hello world", "spark java"));
Word2VecModel model = w2v.fit(documents);` |
| `transform` | String word | `Vector` | 获取词向量 | `Vector vec = model.transform("spark");` |
| `findSynonyms` | String word, int num | `Tuple2[String, Double][]` | 查找相似词 | `Tuple2<String, Double>[] synonyms = model.findSynonyms("spark", 5);` |
| `findSynonyms` | Vector vector, int num | `Tuple2[String, Double][]` | 查找与向量相似的词 | `Tuple2<String, Double>[] similar = model.findSynonyms(vector, 10);` |
| `getVectors` | 无 | `Map[String, Vector]` | 获取所有词向量 | `Map<String, Vector> vectors = model.getVectors();` |


### FPGrowth / FPGrowthModel
**包路径**: `org.apache.spark.mllib.fpm`
**说明**: FP-Growth频繁项集挖掘算法。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `setMinSupport` | double minSupport | `FPGrowth` | 设置最小支持度 | `FPGrowth fpg = new FPGrowth().setMinSupport(0.3);  // 30%支持度` |
| `setNumPartitions` | int numPartitions | `FPGrowth` | 设置分区数 | `fpg.setNumPartitions(10);` |
| `run` | JavaRDD[String[]] data | `FPGrowthModel` | 运行FP-Growth | `JavaRDD<String[]> transactions = sc.parallelize(Arrays.asList(
    new String[]{"a", "b", "c"},
    new String[]{"a", "b"}));
FPGrowthModel model = fpg.run(transitions.rdd());` |
| `freqItemsets` | 无 | `JavaRDD[FreqItemset]` | 获取频繁项集 | `JavaRDD<FreqItemset> itemsets = model.freqItemsets();
itemsets.foreach(item -> System.out.println(item.items() + ": " + item.freq()));` |


### AssociationRules
**包路径**: `org.apache.spark.mllib.fpm`
**说明**: 关联规则生成。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AssociationRules` | 无 | 构造方法 | 创建关联规则生成器 | `AssociationRules ar = new AssociationRules();` |
| `setMinConfidence` | double minConfidence | `AssociationRules` | 设置最小置信度 | `ar.setMinConfidence(0.5);  // 50%置信度` |
| `run` | JavaRDD[FreqItemset] freqItemsets | `JavaRDD[Rule]` | 生成关联规则 | `JavaRDD<Rule> rules = ar.run(fpgModel.freqItemsets().toJavaRDD());
rules.foreach(rule -> System.out.println(
    rule.antecedent() + " => " + rule.consequent() +
    ": confidence=" + rule.confidence()));` |


### BinaryClassificationMetrics
**包路径**: `org.apache.spark.mllib.evaluation`
**说明**: 二分类评估指标。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `BinaryClassificationMetrics` | JavaPairRDD[Double, Double] predictionAndLabels | 构造方法 | 创建评估器 | `JavaPairRDD<Double, Double> predictions = predictedLabelsRDD;
BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictions.rdd());` |
| `areaUnderPR` | 无 | `Double` | PR曲线下面积 | `double aupr = metrics.areaUnderPR();` |
| `areaUnderROC` | 无 | `Double` | ROC曲线下面积（AUC） | `double auc = metrics.areaUnderROC();` |
| `pr` | 无 | `JavaRDD[Tuple2[Double, Double]]` | PR曲线数据点 | `JavaRDD<Tuple2<Double, Double>> prCurve = metrics.pr().toJavaRDD();` |
| `roc` | 无 | `JavaRDD[Tuple2[Double, Double]]` | ROC曲线数据点 | `JavaRDD<Tuple2<Double, Double>> rocCurve = metrics.roc().toJavaRDD();` |
| `precisionByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的精确率 | - |
| `recallByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的召回率 | - |
| `fMeasureByThreshold` | double beta | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的F值 | - |


### MulticlassMetrics
**包路径**: `org.apache.spark.mllib.evaluation`
**说明**: 多分类评估指标。
**方法数量**: 6+

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
**方法数量**: 6+

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
**方法数量**: 8+

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
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Vectors.dense` | double... values | `Vector` | 创建密集向量 | `Vector denseVec = Vectors.dense(1.0, 2.0, 3.0);` |
| `Vectors.dense` | double[] values | `Vector` | 创建密集向量（数组） | `double[] arr = {1.0, 2.0, 3.0};
Vector vec = Vectors.dense(arr);` |
| `Vectors.sparse` | int size, int[] indices, double[] values | `Vector` | 创建稀疏向量 | `Vector sparseVec = Vectors.sparse(10, new int[]{0, 5}, new double[]{1.0, 2.0});  // 10维，位置0和5有值` |
| `Vectors.sparse` | int size, Iterable[Tuple2[Int, Double]] entries | `Vector` | 创建稀疏向量（迭代器） | - |
| `Vectors.zeros` | int size | `Vector` | 创建零向量 | `Vector zero = Vectors.zeros(10);` |
| `Vectors.norm` | Vector v, double p | `Double` | 计算向量范数 | `double norm = Vectors.norm(vec, 2.0);  // L2范数` |
| `Vectors.sqdist` | Vector v1, Vector v2 | `Double` | 计算向量平方距离 | `double sqDist = Vectors.sqdist(vec1, vec2);` |
| `Matrices.dense` | int numRows, int numCols, double[] values | `Matrix` | 创建密集矩阵 | `Matrix denseMat = Matrices.dense(2, 3, new double[]{1,2,3,4,5,6});` |
| `Matrices.sparse` | int numRows, int numCols, int[] colPtrs, int[] rowIndices, double[] values | `Matrix` | 创建稀疏矩阵（CSC格式） | - |
| `Matrices.zeros` | int numRows, int numCols | `Matrix` | 创建零矩阵 | `Matrix zeroMat = Matrices.zeros(3, 3);` |
| `Matrices.eye` | int n | `Matrix` | 创建单位矩阵 | `Matrix identity = Matrices.eye(3);` |
| `Matrices.rand` | int numRows, int numCols | `Matrix` | 创建随机矩阵 | `Matrix randMat = Matrices.rand(3, 4);` |
| `size` | 无 | `Int` | 向量维度 | `int dim = vec.size();` |
| `toArray` | 无 | `double[]` | 转为数组 | `double[] arr = vec.toArray();` |
| `dot` | Vector other | `Double` | 向量点积 | `double dot = vec1.dot(vec2);` |

