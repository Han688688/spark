# Spark Java API示例代码补充

## 一、Core RDD Java API示例

### 1.1 JavaSparkContext示例

#### 创建RDD

```java
// 示例1：从List创建RDD
JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("Test").setMaster("local"));
List<String> data = Arrays.asList("hello", "world", "spark");
JavaRDD<String> rdd = sc.parallelize(data);

// 示例2：指定分区数
JavaRDD<String> rdd2 = sc.parallelize(data, 3);

// 示例3：创建Double RDD
List<Double> doubles = Arrays.asList(1.0, 2.0, 3.0);
JavaDoubleRDD doubleRDD = sc.parallelizeDoubles(doubles);

// 示例4：创建Pair RDD
List<Tuple2<String, Integer>> pairs = Arrays.asList(
    new Tuple2<>("key1", 1),
    new Tuple2<>("key2", 2)
);
JavaPairRDD<String, Integer> pairRDD = sc.parallelizePairs(pairs);
```

#### 文件读取

```java
// 示例1：读取文本文件
JavaRDD<String> textRDD = sc.textFile("/path/to/file.txt");

// 示例2：指定分区数
JavaRDD<String> textRDD2 = sc.textFile("/path/to/file.txt", 10);

// 示例3：读取所有文本文件（返回文件名+内容）
JavaPairRDD<String, String> filesRDD = sc.wholeTextFiles("/path/to/dir/");

// 示例4：读取二进制文件
JavaPairRDD<String, PortableDataStream> binaryRDD = sc.binaryFiles("/path/to/dir/");
```

#### 广播变量

```java
// 示例：创建广播变量
Map<String, String> lookupTable = new HashMap<>();
lookupTable.put("key1", "value1");
lookupTable.put("key2", "value2");

Broadcast<Map<String, String>> broadcastVar = sc.broadcast(lookupTable);

// 使用广播变量
JavaRDD<String> result = rdd.map(s -> broadcastVar.value().get(s));
```

#### 累加器

```java
// 示例：创建和使用累加器
LongAccumulator acc = sc.longAccumulator("myAccumulator");

rdd.foreach(s -> {
    acc.add(1);
});

System.out.println("Count: " + acc.value());
```

### 1.2 JavaRDD示例

#### Map操作

```java
// 示例1：map映射
JavaRDD<Integer> lengths = rdd.map(s -> s.length());

// 示例2：mapToPair转为PairRDD
JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(s -> new Tuple2<>(s, s.length()));

// 示例3：mapToDouble转为DoubleRDD
JavaDoubleRDD doubleRDD = rdd.mapToDouble(s -> s.length() * 1.5);
```

#### Filter操作

```java
// 示例：过滤数据
JavaRDD<String> filtered = rdd.filter(s -> s.length() > 3);
```

#### FlatMap操作

```java
// 示例1：flatMap展平
JavaRDD<String> words = rdd.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

// 示例2：flatMapToPair
JavaPairRDD<String, Integer> wordCounts = rdd.flatMapToPair(s -> {
    List<Tuple2<String, Integer>> list = new ArrayList<>();
    for (String word : s.split(" ")) {
        list.add(new Tuple2<>(word, 1));
    }
    return list.iterator();
});
```

#### Reduce操作

```java
// 示例1：reduce聚合
Integer totalLength = lengths.reduce((a, b) -> a + b);

// 示例2：treeReduce（树形聚合，性能更好）
Integer totalLength2 = lengths.treeReduce((a, b) -> a + b);

// 示例3：fold（带初始值）
Integer totalLength3 = lengths.fold(0, (a, b) -> a + b);

// 示例4：aggregate（不同类型聚合）
Integer totalLength4 = rdd.aggregate(
    0,
    (acc, s) -> acc + s.length(),  // seqOp
    (acc1, acc2) -> acc1 + acc2    // combOp
);
```

#### Count操作

```java
// 示例1：计数
long count = rdd.count();

// 示例2：近似计数（快速）
PartialResult<BoundedDouble> approxCount = rdd.countApprox(1000);

// 示例3：按值计数
Map<String, Long> countByValue = rdd.countByValue();
```

#### Collect操作

```java
// 示例1：收集所有数据
List<String> collected = rdd.collect();

// 示例2：取前N个
List<String> top5 = rdd.take(5);

// 示例3：取样本
List<String> sample = rdd.takeSample(false, 10);

// 示例4：取最大的N个
List<String> top3 = rdd.top(3, Comparator.naturalOrder());
```

#### Persist操作

```java
// 示例1：缓存
JavaRDD<String> cachedRDD = rdd.cache();

// 示例2：指定存储级别
JavaRDD<String> persistedRDD = rdd.persist(StorageLevel.MEMORY_AND_DISK());

// 示例3：取消缓存
persistedRDD.unpersist();
```

### 1.3 JavaPairRDD示例

#### reduceByKey

```java
// 示例：按Key聚合
JavaPairRDD<String, Integer> wordCounts = pairRDD.reduceByKey((a, b) -> a + b);

// 示例2：指定分区数
JavaPairRDD<String, Integer> wordCounts2 = pairRDD.reduceByKey((a, b) -> a + b, 10);
```

#### groupByKey

```java
// 示例：按Key分组
JavaPairRDD<String, Iterable<Integer>> grouped = pairRDD.groupByKey();
```

#### join

```java
// 示例：内连接
JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("key1", 1),
    new Tuple2<>("key2", 2)
));

JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("key1", "value1"),
    new Tuple2<>("key2", "value2")
));

JavaPairRDD<String, Tuple2<Integer, String>> joined = rdd1.join(rdd2);

// 结果：("key1", (1, "value1")), ("key2", (2, "value2"))
```

#### leftOuterJoin / rightOuterJoin

```java
// 示例：左外连接
JavaPairRDD<String, Tuple2<Integer, Optional<String>>> leftJoined = rdd1.leftOuterJoin(rdd2);

// 示例：右外连接
JavaPairRDD<String, Tuple2<Optional<Integer>, String>> rightJoined = rdd1.rightOuterJoin(rdd2);
```

#### mapValues

```java
// 示例：只对Value映射
JavaPairRDD<String, Integer> mappedValues = pairRDD.mapValues(v -> v * 2);
```

#### sortByKey

```java
// 示例：按Key排序
JavaPairRDD<String, Integer> sorted = pairRDD.sortByKey();

// 示例：降序排序
JavaPairRDD<String, Integer> sortedDesc = pairRDD.sortByKey(false);
```

## 二、SQL DataFrame Java API示例

### 2.1 SparkSession示例

```java
// 示例1：创建SparkSession
SparkSession spark = SparkSession.builder()
    .appName("Java Spark SQL Example")
    .master("local")
    .getOrCreate();

// 示例2：启用Hive支持
SparkSession sparkWithHive = SparkSession.builder()
    .appName("Hive Example")
    .config("spark.sql.warehouse.dir", "/user/hive/warehouse")
    .enableHiveSupport()
    .getOrCreate();
```

### 2.2 DataFrame创建示例

```java
// 示例1：从JSON文件创建
Dataset<Row> df = spark.read().json("/path/to/json");

// 示例2：从CSV文件创建
Dataset<Row> df = spark.read()
    .option("header", "true")
    .option("inferSchema", "true")
    .csv("/path/to/csv");

// 示例3：从Parquet文件创建
Dataset<Row> df = spark.read().parquet("/path/to/parquet");

// 示例4：从Java对象创建
List<Person> people = Arrays.asList(
    new Person("Alice", 25),
    new Person("Bob", 30)
);
Dataset<Row> df = spark.createDataFrame(people, Person.class);

// 示例5：从RDD创建
JavaRDD<Person> personRDD = sc.parallelize(people);
Dataset<Row> df = spark.createDataFrame(personRDD, Person.class);
```

### 2.3 SQL查询示例

```java
// 示例1：SQL查询
df.createOrReplaceTempView("people");
Dataset<Row> result = spark.sql("SELECT name, age FROM people WHERE age > 25");

// 示例2：DataFrame API查询
Dataset<Row> result2 = df.filter(df.col("age").gt(25))
    .select(df.col("name"), df.col("age"));

// 示例3：聚合查询
Dataset<Row> aggregated = df.groupBy("name")
    .agg(functions.avg("age"), functions.count("name"));
```

### 2.4 DataFrame写入示例

```java
// 示例1：写入JSON
df.write().json("/path/to/output/json");

// 示例2：写入CSV
df.write()
    .option("header", "true")
    .csv("/path/to/output/csv");

// 示例3：写入Parquet
df.write().parquet("/path/to/output/parquet");

// 示例4：追加模式写入
df.write().mode(SaveMode.Append).parquet("/path/to/output/parquet");

// 示例5：覆盖模式写入
df.write().mode(SaveMode.Overwrite).parquet("/path/to/output/parquet");
```

### 2.5 UDF示例

```java
// 示例1：注册UDF
spark.udf().register("myUDF", new UDF1<String, Integer>() {
    @Override
    public Integer call(String s) throws Exception {
        return s.length();
    }
}, DataTypes.IntegerType);

// 示例2：使用UDF
Dataset<Row> result = spark.sql("SELECT myUDF(name) FROM people");

// 示例3：注册Java函数为UDF
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;

spark.udf().register("strlen", (UDF1<String, Integer>) s -> s.length(), DataTypes.IntegerType);
```

## 三、Streaming Java API示例

### 3.1 JavaStreamingContext示例

```java
// 示例：创建StreamingContext
SparkConf conf = new SparkConf().setAppName("Streaming Example").setMaster("local[2]");
JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(1));
```

### 3.2 Socket文本流示例

```java
// 示例1：从Socket读取文本流
JavaDStream<String> lines = jssc.socketTextStream("localhost", 9999);

// 示例2：处理流数据
JavaDStream<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());

// 示例3：统计单词
JavaPairDStream<String, Integer> wordCounts = words.mapToPair(word -> new Tuple2<>(word, 1))
    .reduceByKey((a, b) -> a + b);

// 示例4：打印结果
wordCounts.print();

// 启动流处理
jssc.start();
jssc.awaitTermination();
```

### 3.3 Kafka流示例

```java
// 示例：从Kafka读取流
Map<String, Object> kafkaParams = new HashMap<>();
kafkaParams.put("bootstrap.servers", "localhost:9092");
kafkaParams.put("group.id", "spark-streaming-group");
kafkaParams.put("auto.offset.reset", "latest");

Collection<String> topics = Arrays.asList("topic1", "topic2");

JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(
    jssc,
    LocationStrategies.PreferConsistent(),
    ConsumerStrategies.Subscribe(topics, kafkaParams)
);
```

### 3.4 Window操作示例

```java
// 示例1：滑动窗口
JavaPairDStream<String, Integer> windowCounts = wordCounts.reduceByKeyAndWindow(
    (a, b) -> a + b,
    Durations.seconds(30),  // 窗口长度
    Durations.seconds(10)   // 滑动间隔
);

// 示例2：逆窗口（减去旧数据）
JavaPairDStream<String, Integer> windowCounts2 = wordCounts.reduceByKeyAndWindow(
    (a, b) -> a + b,
    (a, b) -> a - b,
    Durations.seconds(30),
    Durations.seconds(10)
);
```

## 四、MLlib Java API示例

### 4.1 线性回归示例

```java
// 示例：线性回归
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;

Dataset<Row> training = spark.read().format("libsvm").load("/path/to/training");

LinearRegression lr = new LinearRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8);

LinearRegressionModel model = lr.fit(training);

Dataset<Row> predictions = model.transform(training);
```

### 4.2 分类示例

```java
// 示例：逻辑回归分类
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.classification.LogisticRegressionModel;

LogisticRegression lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.3)
    .setElasticNetParam(0.8);

LogisticRegressionModel model = lr.fit(training);

Dataset<Row> predictions = model.transform(testData);
```

### 4.3 聚类示例

```java
// 示例：K-Means聚类
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;

KMeans kmeans = new KMeans().setK(2).setSeed(1L);

KMeansModel model = kmeans.fit(dataset);

Dataset<Row> predictions = model.transform(dataset);
```

## 五、GraphX Java API示例

### 5.1 TripletFields示例

```java
// 示例：设置Triplet字段
TripletFields fields = TripletFields.All;  // 使用所有字段
TripletFields fields2 = TripletFields.EdgeOnly;  // 只使用Edge字段
```

## 六、常用组合示例

### 6.1 WordCount完整示例

```java
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;

public class WordCount {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 读取文件
        JavaRDD<String> lines = sc.textFile("/path/to/file.txt");
        
        // 分割单词
        JavaRDD<String> words = lines.flatMap(line -> 
            Arrays.asList(line.split(" ")).iterator());
        
        // 映射为(word, 1)
        JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> 
            new Tuple2<>(word, 1));
        
        // 按Key聚合
        JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);
        
        // 输出结果
        counts.foreach(pair -> System.out.println(pair._1 + ": " + pair._2));
        
        sc.stop();
    }
}
```

### 6.2 DataFrame查询完整示例

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class DataFrameExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("DataFrame Example")
            .master("local")
            .getOrCreate();
        
        // 读取JSON文件
        Dataset<Row> df = spark.read().json("/path/to/people.json");
        
        // 注册临时视图
        df.createOrReplaceTempView("people");
        
        // SQL查询
        Dataset<Row> adults = spark.sql("SELECT * FROM people WHERE age >= 18");
        
        // 显示结果
        adults.show();
        
        // 使用DataFrame API
        Dataset<Row> adults2 = df.filter("age >= 18").select("name", "age");
        adults2.show();
        
        spark.stop();
    }
}
```

---

## 使用建议

1. **运行环境准备**
   - 设置SparkConf配置
   - 创建SparkContext或SparkSession
   - 设置Master（本地测试用"local"，生产环境用集群地址）

2. **依赖导入**
   ```xml
   <dependency>
       <groupId>org.apache.spark</groupId>
       <artifactId>spark-core_2.12</artifactId>
       <version>4.1.1</version>
   </dependency>
   <dependency>
       <groupId>org.apache.spark</groupId>
       <artifactId>spark-sql_2.12</artifactId>
       <version>4.1.1</version>
   </dependency>
   ```

3. **测试有效性验证**
   - 使用上述示例代码测试API调用
   - 验证参数类型和返回类型
   - 验证异常处理

---

**完整示例代码补充完成！可直接用于测试API有效性！**