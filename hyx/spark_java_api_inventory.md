# Spark Java API 清单

基于代码仓分析和官方文档，版本：Spark 4.2.0-SNAPSHOT

---

## 一、Java函数接口（Functional Interface）

**位置**: `common/utils-java/src/main/java/org/apache/spark/api/java/function/`

所有接口都是`@FunctionalInterface`，支持Lambda表达式。

### 1.1 Dataset核心操作接口

| 接口 | 签名 | 用于方法 | 描述 |
|------|------|----------|------|
| **MapFunction<T,U>** | `U call(T value)` | `map()` | 类型化映射 |
| **FilterFunction<T>** | `boolean call(T value)` | `filter()` | 条件过滤 |
| **FlatMapFunction<T,R>** | `Iterator<R> call(T t)` | `flatMap()` | 展平映射 |
| **MapPartitionsFunction<T,U>** | `Iterator<U> call(Iterator<T> it)` | `mapPartitions()` | 分区映射 |
| **ReduceFunction<T>** | `T call(T v1, T v2)` | `reduce()` | 类型化reduce |
| **ForeachFunction<T>** | `void call(T t)` | `foreach()` | 遍历处理 |
| **ForeachPartitionFunction<T>** | `void call(Iterator<T> it)` | `foreachPartition()` | 分区遍历 |

### 1.2 分组操作接口

| 接口 | 签名 | 用于方法 | 描述 |
|------|------|----------|------|
| **MapGroupsFunction<K,V,R>** | `R call(K key, Iterator<V> values)` | `mapGroups()` | 分组处理 |
| **FlatMapGroupsFunction<K,V,R>** | `Iterator<R> call(K key, Iterator<V> values)` | `flatMapGroups()` | 展平分组处理 |
| **CoGroupFunction<K,V1,V2,R>** | `Iterator<R> call(K key, Iterator<V1> left, Iterator<V2> right)` | `cogroup()` | 协分组处理 |

### 1.3 有状态处理接口（流处理）

| 接口 | 签名 | 用于方法 | 描述 |
|------|------|----------|------|
| **MapGroupsWithStateFunction<K,V,S,R>** | `R call(K key, Iterator<V> values, GroupState<S> state)` | `mapGroupsWithState()` | 有状态分组处理 |
| **FlatMapGroupsWithStateFunction<K,V,S,R>** | `Iterator<R> call(K key, Iterator<V> values, GroupState<S> state)` | `flatMapGroupsWithState()` | 展平有状态处理 |

### 1.4 PairRDD专用接口（KeyValue操作）

| 接口 | 签名 | 用于方法 | 描述 |
|------|------|----------|------|
| **PairFunction<T,K,V>** | `Tuple2<K,V> call(T t)` | `mapToPair()` | 转为PairRDD |
| **PairFlatMapFunction<T,K,V>** | `Iterator<Tuple2<K,V>> call(T t)` | `flatMapToPair()` | 展平转为Pair |
| **FlatMapFunction2<K,V,R>** | `Iterator<R> call(K key, V value)` | `flatMapValues()` | 值展平 |

### 1.5 Double类型专用接口

| 接口 | 签名 | 用于方法 | 描述 |
|------|------|----------|------|
| **DoubleFunction<T>** | `double call(T t)` | `mapToDouble()` | 转为Double |
| **DoubleFlatMapFunction<T>** | `Iterator<Double> call(T t)` | `flatMapToDouble()` | 展平Double |

### 1.6 Void函数接口

| 接口 | 签名 | 用于方法 | 描述 |
|------|------|----------|------|
| **VoidFunction<T>** | `void call(T t)` | `foreach()` | 无返回值处理 |
| **VoidFunction2<T1,T2>** | `void call(T1 t1, T2 t2)` | `foreach()` | 双参数无返回值 |

### 1.7 通用Function接口

| 接口 | 签名 | 描述 |
|------|------|------|
| **Function0<R>** | `R call()` | 无参数函数 |
| **Function<T,R>** | `R call(T t)` | 单参数函数 |
| **Function2<T1,T2,R>** | `R call(T1 t1, T2 t2)` | 双参数函数 |
| **Function3<T1,T2,T3,R>** | `R call(T1 t1, T2 t2, T3 t3)` | 三参数函数 |
| **Function4<T1,T2,T3,T4,R>** | `R call(T1 t1, T2 t2, T3 t3, T4 t4)` | 四参数函数 |

---

## 二、UDF接口（用户自定义函数）

**位置**: `sql/api/src/main/java/org/apache/spark/sql/api/java/`

Spark提供0到22个参数的UDF接口：

### 2.1 UDF接口列表

| 接口 | 签名 | 参数数量 |
|------|------|----------|
| **UDF0<R>** | `R call()` | 0参数 |
| **UDF1<T1,R>** | `R call(T1 t1)` | 1参数 |
| **UDF2<T1,T2,R>** | `R call(T1 t1, T2 t2)` | 2参数 |
| **UDF3<T1,T2,T3,R>** | `R call(T1 t1, T2 t2, T3 t3)` | 3参数 |
| **UDF4<T1,T2,T3,T4,R>** | `R call(T1 t1, T2 t2, T3 t3, T4 t4)` | 4参数 |
| **UDF5<T1,...,T5,R>** | `R call(T1 t1, ..., T5 t5)` | 5参数 |
| **UDF6** - **UDF10** | ... | 6-10参数 |
| **UDF11** - **UDF15** | ... | 11-15参数 |
| **UDF16** - **UDF20** | ... | 16-20参数 |
| **UDF21<T1,...,T21,R>** | `R call(T1 t1, ..., T21 t21)` | 21参数 |
| **UDF22<T1,...,T22,R>** | `R call(T1 t1, ..., T22 t22)` | 22参数（最大） |

### 2.2 UDAF接口（用户自定义聚合函数）

```java
// Aggregator接口（需要继承）
public abstract class Aggregator<I, B, O> {
    public B zero();                                    // 初始值
    public B reduce(B b, I i);                          // 聚合逻辑
    public B merge(B b1, B b2);                         // 合并逻辑
    public O finish(B b);                               // 最终输出
    public Encoder<B> bufferEncoder();                  // Buffer编码器
    public Encoder<O> outputEncoder();                  // 输出编码器
}
```

---

## 三、Dataset Java API

**Java Dataset API与Scala基本相同，但使用Java风格函数接口**

### 3.1 Dataset创建

```java
// 创建SparkSession
SparkSession spark = SparkSession.builder()
    .appName("JavaApp")
    .config("spark.master", "local")
    .getOrCreate();

// 创建DataFrame
Dataset<Row> df = spark.read().parquet("path");
Dataset<Row> df = spark.read().json("path");
Dataset<Row> df = spark.read().csv("path");
Dataset<Row> df = spark.read().format("format").load("path");
Dataset<Row> df = spark.sql("SELECT * FROM table");
Dataset<Row> df = spark.table("tableName");

// 创建Dataset（类型化）
Dataset<String> ds = spark.createDataset(Arrays.asList("a", "b"), Encoders.STRING());
Dataset<Person> ds = spark.createDataset(javaList, Encoders.bean(Person.class));
Dataset<Row> df = spark.createDataFrame(javaRDD, schema);
Dataset<Row> df = spark.createDataFrame(javaList, schema);
```

### 3.2 Dataset转换操作（Transformations）

| 方法 | Java签名 | 函数接口 | 返回类型 |
|------|----------|----------|----------|
| **map** | `Dataset<U> map(MapFunction<T,U> f, Encoder<U> enc)` | MapFunction | Dataset<U> |
| **flatMap** | `Dataset<U> flatMap(FlatMapFunction<T,U> f, Encoder<U> enc)` | FlatMapFunction | Dataset<U> |
| **mapPartitions** | `Dataset<U> mapPartitions(MapPartitionsFunction<T,U> f, Encoder<U> enc)` | MapPartitionsFunction | Dataset<U> |
| **filter** | `Dataset<T> filter(FilterFunction<T> f)` | FilterFunction | Dataset<T> |
| **filter** | `Dataset<T> filter(Column condition)` | Column | Dataset<T> |
| **reduce** | `T reduce(ReduceFunction<T> f)` | ReduceFunction | T |
| **groupByKey** | `KeyValueGroupedDataset<K,T> groupByKey(MapFunction<T,K> f, Encoder<K> enc)` | MapFunction | KeyValueGroupedDataset |

### 3.3 Dataset动作操作（Actions）

| 方法 | Java签名 | 返回类型 |
|------|----------|----------|
| **collect** | `T[] collect()` | T[] |
| **collectAsList** | `List<T> collectAsList()` | List<T> |
| **take** | `T[] take(int n)` | T[] |
| **takeAsList** | `List<T> takeAsList(int n)` | List<T> |
| **head** | `T head()` | T |
| **head** | `T[] head(int n)` | T[] |
| **first** | `T first()` | T |
| **count** | `long count()` | long |
| **foreach** | `void foreach(ForeachFunction<T> f)` | void |
| **foreachPartition** | `void foreachPartition(ForeachPartitionFunction<T> f)` | void |
| **show** | `void show()` | void |
| **show** | `void show(int numRows)` | void |
| **show** | `void show(int numRows, boolean truncate)` | void |
| **show** | `void show(int numRows, int truncate)` | void |

### 3.4 DataFrame操作（Untyped）

| 方法 | Java签名 | 返回类型 |
|------|----------|----------|
| **select** | `DataFrame select(Column... cols)` | DataFrame |
| **select** | `DataFrame select(String... colNames)` | DataFrame |
| **selectExpr** | `DataFrame selectExpr(String... exprs)` | DataFrame |
| **where/filter** | `DataFrame filter(Column condition)` | DataFrame |
| **groupBy** | `RelationalGroupedDataset groupBy(Column... cols)` | RelationalGroupedDataset |
| **agg** | `DataFrame agg(Column... aggExprs)` | DataFrame |
| **join** | `DataFrame join(Dataset<?> right)` | DataFrame |
| **join** | `DataFrame join(Dataset<?> right, String joinCol)` | DataFrame |
| **join** | `DataFrame join(Dataset<?> right, Column joinExpr)` | DataFrame |
| **join** | `DataFrame join(Dataset<?> right, Column joinExpr, String joinType)` | DataFrame |
| **sort/orderBy** | `DataFrame sort(Column... sortExprs)` | DataFrame |
| **limit** | `DataFrame limit(int n)` | DataFrame |
| **union** | `Dataset<T> union(Dataset<T> other)` | Dataset<T> |
| **intersect** | `Dataset<T> intersect(Dataset<T> other)` | Dataset<T> |
| **except** | `Dataset<T> except(Dataset<T> other)` | Dataset<T> |
| **drop** | `DataFrame drop(Column... cols)` | DataFrame |
| **drop** | `DataFrame drop(String... colNames)` | DataFrame |
| **dropDuplicates** | `Dataset<T> dropDuplicates(String... colNames)` | Dataset<T> |
| **withColumn** | `DataFrame withColumn(String colName, Column col)` | DataFrame |
| **withColumnRenamed** | `DataFrame withColumnRenamed(String existing, String new)` | DataFrame |
| **na** | `DataFrameNaFunctions na()` | DataFrameNaFunctions |
| **stat** | `DataFrameStatFunctions stat()` | DataFrameStatFunctions |

### 3.5 KeyValueGroupedDataset Java API

| 方法 | Java签名 | 函数接口 | 返回类型 |
|------|----------|----------|----------|
| **mapValues** | `Dataset<U> mapValues(MapFunction<V,U> f, Encoder<U> enc)` | MapFunction | Dataset<U> |
| **flatMapValues** | `Dataset<U> flatMapValues(FlatMapFunction<V,U> f, Encoder<U> enc)` | FlatMapFunction | Dataset<U> |
| **mapGroups** | `Dataset<U> mapGroups(MapGroupsFunction<K,V,U> f, Encoder<U> enc)` | MapGroupsFunction | Dataset<U> |
| **flatMapGroups** | `Dataset<U> flatMapGroups(FlatMapGroupsFunction<K,V,U> f, Encoder<U> enc)` | FlatMapGroupsFunction | Dataset<U> |
| **reduce** | `Dataset<V> reduce(ReduceFunction<V> f)` | ReduceFunction | Dataset<V> |
| **agg** | `DataFrame agg(Column... aggExprs)` | Column | DataFrame |
| **count** | `DataFrame count()` | - | DataFrame |
| **cogroup** | `Dataset<U> cogroup(KeyValueGroupedDataset<K,W> other, CoGroupFunction<K,V,W,U> f, Encoder<U> enc)` | CoGroupFunction | Dataset<U> |

### 3.6 RelationalGroupedDataset Java API

| 方法 | Java签名 | 返回类型 |
|------|----------|----------|
| **agg** | `DataFrame agg(Column... exprs)` | DataFrame |
| **agg** | `DataFrame agg(Map<String,String> exprs)` | DataFrame |
| **count** | `DataFrame count()` | DataFrame |
| **avg** | `DataFrame avg(String... colNames)` | DataFrame |
| **sum** | `DataFrame sum(String... colNames)` | DataFrame |
| **max** | `DataFrame max(String... colNames)` | DataFrame |
| **min** | `DataFrame min(String... colNames)` | DataFrame |
| **mean** | `DataFrame mean(String... colNames)` | DataFrame |
| **pivot** | `RelationalGroupedDataset pivot(String pivotColumn)` | RelationalGroupedDataset |

---

## 四、Column Java API

**位置**: `sql/api/src/main/scala/org/apache/spark/sql/Column.scala`（共享，Java可调用）

### 4.1 Column创建

```java
// 方式1：通过Dataset
Column col = dataset.col("columnName");
Column col = dataset.apply("columnName");  // Scala风格

// 方式2：通过functions
Column col = functions.col("columnName");
Column col = functions.column("columnName");
Column col = functions.lit(value);  // 常量列
Column col = functions.expr("expression");  // SQL表达式
```

### 4.2 Column比较操作（Java风格）

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **equalTo** | `Column equalTo(Object other)` | 等于 (=) |
| **eqNullSafe** | `Column eqNullSafe(Object other)` | 空值安全等于 |
| **notEqual** | `Column notEqual(Object other)` | 不等于 (!=) |
| **lessThan** | `Column lessThan(Object other)` | 小于 (<) |
| **lessThanOrEqual** | `Column lessThanOrEqual(Object other)` | 小于等于 (<=) |
| **greaterThan** | `Column greaterThan(Object other)` | 大于 (>0) |
| **greaterThanOrEqual** | `Column greaterThanOrEqual(Object other)` | 大于等于 (>=) |
| **isNull** | `Column isNull()` | 是否为空 |
| **isNotNull** | `Column isNotNull()` | 是否非空 |
| **isNaN** | `Column isNaN()` | 是否NaN |
| **isin** | `Column isin(Object... objects)` | 是否在集合中 |

### 4.3 Column排序操作

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **asc** | `Column asc()` | 升序 |
| **desc** | `Column desc()` | 降序 |
| **asc_nulls_first** | `Column asc_nulls_first()` | 升序空值优先 |
| **asc_nulls_last** | `Column asc_nulls_last()` | 升序空值后置 |
| **desc_nulls_first** | `Column desc_nulls_first()` | 降序空值优先 |
| **desc_nulls_last** | `Column desc_nulls_last()` | 降序空值后置 |

### 4.4 Column字符串操作

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **like** | `Column like(String literal)` | LIKE匹配 |
| **ilike** | `Column ilike(String literal)` | 不区分大小写LIKE |
| **rlike** | `Column rlike(String literal)` | 正则匹配 |
| **contains** | `Column contains(String other)` | 包含 |
| **startsWith** | `Column startsWith(String other)` | 前缀匹配 |
| **endsWith** | `Column endsWith(String other)` | 后缀匹配 |
| **substr** | `Column substr(int startPos, int len)` | 子字符串 |
| **substring** | `Column substring(int startPos, int len)` | 子字符串 |

### 4.5 Column类型操作

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **cast** | `Column cast(String to)` | 类型转换 |
| **cast** | `Column cast(DataType to)` | 类型转换（DataType） |
| **try_cast** | `Column try_cast(String to)` | 安全类型转换 |
| **alias/as** | `Column alias(String alias)` | 设置别名 |
| **alias/as** | `Column alias(String... aliases)` | 多级别名 |
| **name** | `Column name(String alias)` | 设置名称 |

### 4.6 Column算术操作

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **plus** | `Column plus(Object other)` | 加 (+) |
| **minus** | `Column minus(Object other)` | 减 (-) |
| **multiply** | `Column multiply(Object other)` | 乘 (*) |
| **divide** | `Column divide(Object other)` | 除 (/) |
| **mod** | `Column mod(Object other)` | 模 (%) |
| **negate** | `Column negate()` | 取负 (-) |

### 4.7 Column条件操作

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **when** | `Column when(Column condition, Object value)` | CASE WHEN |
| **otherwise** | `Column otherwise(Object value)` | CASE ELSE |
| **between** | `Column between(Object lower, Object upper)` | 范围条件 |

### 4.8 Column结构/数组操作

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **getField** | `Column getField(String fieldName)` | 获取结构字段 |
| **getItem** | `Column getItem(int index)` | 获取数组元素 |
| **getItem** | `Column getItem(String key)` | 获取Map元素 |
| **dropFields** | `Column dropFields(String... fields)` | 删除结构字段 |
| **withField** | `Column withField(String name, Column col)` | 添加结构字段 |

### 4.9 Column位运算

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **bitwiseAND** | `Column bitwiseAND(Object other)` | 位与 (&) |
| **bitwiseOR** | `Column bitwiseOR(Object other)` | 位或 (|) |
| **bitwiseXOR** | `Column bitwiseXOR(Object other)` | 位异或 (^) |

### 4.10 Column窗口操作

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **over** | `Column over(WindowSpec window)` | 窗口函数 |
| **over** | `Column over()` | 默认窗口 |

---

## 五、DataFrameReader Java API

```java
DataFrameReader reader = spark.read();

// 基本配置
reader.format(String format);
reader.schema(StructType schema);
reader.option(String key, Object value);
reader.options(Map<String,String> options);

// 加载方法
Dataset<Row> df = reader.load();
Dataset<Row> df = reader.load(String... paths);

// 格式化加载
Dataset<Row> df = spark.read().json(String... paths);
Dataset<Row> df = spark.read().csv(String... paths);
Dataset<Row> df = spark.read().parquet(String... paths);
Dataset<Row> df = spark.read().orc(String... paths);
Dataset<Row> df = spark.read().text(String... paths);
Dataset<Row> df = spark.read().avro(String... paths);

// JDBC
Dataset<Row> df = spark.read().jdbc(String url, String table, Properties props);
Dataset<Row> df = spark.read().format("jdbc")
    .option("url", url)
    .option("dbtable", table)
    .option("user", user)
    .option("password", password)
    .load();

// 表读取
Dataset<Row> df = spark.read().table(String tableName);
```

---

## 六、DataFrameWriter Java API

```java
DataFrameWriter<T> writer = dataset.write();

// 基本配置
writer.format(String format);
writer.mode(SaveMode mode);  // Append, Overwrite, ErrorIfExists, Ignore
writer.option(String key, Object value);
writer.options(Map<String,String> options);
writer.partitionBy(String... colNames);
writer.bucketBy(int numBuckets, String... colNames);
writer.sortBy(String... colNames);

// 保存方法
writer.save();
writer.save(String path);

// 格式化保存
writer.json(String path);
writer.csv(String path);
writer.parquet(String path);
writer.orc(String path);
writer.text(String path);
writer.avro(String path);

// JDBC
writer.jdbc(String url, String table, Properties props);

// 表写入
writer.saveAsTable(String tableName);
writer.insertInto(String tableName);
```

### 6.1 SaveMode枚举

**位置**: `sql/api/src/main/java/org/apache/spark/sql/SaveMode.java`

| 值 | 描述 |
|------|------|
| **Append** | 追加到现有数据 |
| **Overwrite** | 覆盖现有数据 |
| **ErrorIfExists** | 数据存在时抛异常（默认） |
| **Ignore** | 数据存在时不保存 |

---

## 七、DataFrameWriterV2 Java API

```java
DataFrameWriterV2<T> writer = dataset.writeTo(String table);

// 配置
writer.using(String provider);
writer.option(String key, Object value);
writer.options(Map<String,String> options);
writer.tableProperty(String property, String value);
writer.partitionedBy(Column... columns);

// 操作
writer.create();               // 创建表
writer.replace();              // 替换表
writer.createOrReplace();      // 创建或替换
writer.append();               // 追加数据
writer.overwrite(Column condition);  // 条件覆盖
writer.overwritePartitions();  // 覆盖分区
```

---

## 八、MergeIntoWriter Java API（4.0新增）

```java
MergeIntoWriter<T> writer = dataset.mergeInto(String table, Column condition);

// 条件分支
WhenMatched matched = writer.whenMatched();
WhenNotMatched notMatched = writer.whenNotMatched();
WhenNotMatchedBySource notMatchedBySource = writer.whenNotMatchedBySource();

// WhenMatched操作
matched.delete();
matched.delete(Column condition);
matched.updateAll();
matched.updateAll(Column condition);
matched.update(Column condition, Map<String,Column> assignments);
matched.update(Map<String,Column> assignments);

// WhenNotMatched操作
notMatched.insertAll();
notMatched.insertAll(Column condition);
notMatched.insert(Column condition, Map<String,Column> values);
notMatched.insert(Map<String,Column> values);

// Schema演进
writer.withSchemaEvolution();

// 执行
writer.merge();
```

---

## 九、流处理 Java API

### 9.1 DataStreamReader

```java
DataStreamReader reader = spark.readStream();

// 配置
reader.format(String format);
reader.schema(StructType schema);
reader.option(String key, Object value);
reader.options(Map<String,String> options);

// 加载
Dataset<Row> df = reader.load();
Dataset<Row> df = reader.load(String path);

// 格式化加载
Dataset<Row> df = spark.readStream().format("kafka")
    .option("kafka.bootstrap.servers", "host:9092")
    .option("subscribe", "topic")
    .load();
```

### 9.2 DataStreamWriter

```java
DataStreamWriter<T> writer = dataset.writeStream();

// 配置
writer.format(String format);
writer.outputMode(String mode);  // "append", "complete", "update"
writer.option(String key, Object value);
writer.options(Map<String,String> options);
writer.partitionBy(String... colNames);
writer.trigger(Trigger trigger);
writer.foreach(ForeachWriter<T> writer);

// 启动
StreamingQuery query = writer.start();
StreamingQuery query = writer.start(String path);
```

### 9.3 Trigger

**位置**: `sql/api/src/main/java/org/apache/spark/sql/streaming/Trigger.java`

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **ProcessingTime** | `Trigger ProcessingTime(long intervalMs)` | 定时触发 |
| **ProcessingTime** | `Trigger ProcessingTime(String interval)` | 定时触发（字符串） |
| **Continuous** | `Trigger Continuous(long intervalMs)` | 连续处理 |
| **Continuous** | `Trigger Continuous(String interval)` | 连续处理（字符串） |
| **Once** | `Trigger Once()` | 单次触发 |
| **AvailableNow** | `Trigger AvailableNow()` | 可用数据触发 |

### 9.4 OutputMode

**位置**: `sql/api/src/main/java/org/apache/spark/sql/streaming/OutputMode.java`

| 值 | 描述 |
|------|------|
| **Append** | 只追加新数据 |
| **Complete** | 输出完整结果（聚合用） |
| **Update** | 输出更新的结果 |

### 9.5 StreamingQuery

```java
// 查询信息
String id = query.id();
String runId = query.runId();
String name = query.name();
StreamingQueryStatus status = query.status();
StreamingQueryProgress lastProgress = query.lastProgress();
StreamingQueryProgress[] progress = query.progress();
StreamingQueryException exception = query.exception();

// 控制
query.stop();
query.awaitTermination();
query.awaitTermination(long timeoutMs);
boolean isActive = query.isActive();
```

### 9.6 StreamingQueryManager

```java
StreamingQueryManager manager = spark.streams();

// 查询管理
StreamingQuery[] active = manager.active();
StreamingQuery query = manager.get(String id);

// 控制
manager.awaitAnyTermination();
manager.awaitAnyTermination(long timeoutMs);
manager.stopAll();

// 监听
manager.addListener(StreamingQueryListener listener);
manager.removeListener(StreamingQueryListener listener);
```

### 9.7 GroupState Java接口

**位置**: `sql/api/src/main/java/org/apache/spark/sql/streaming/GroupState.java`

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **exists** | `boolean exists()` | 状态是否存在 |
| **get** | `S get()` | 获取状态 |
| **update** | `void update(S newState)` | 更新状态 |
| **remove** | `void remove()` | 移除状态 |
| **hasTimedOut** | `boolean hasTimedOut()` | 是否超时 |
| **setTimeoutTimestamp** | `void setTimeoutTimestamp(long timestamp)` | 设置超时时间戳 |
| **setTimeoutTimestamp** | `void setTimeoutTimestamp(long timestamp, String additionalTimeout)` | 设置超时（额外延迟） |
| **setTimeoutDuration** | `void setTimeoutDuration(long durationMs)` | 设置超时时长 |
| **setTimeoutDuration** | `void setTimeoutDuration(String duration)` | 设置超时（字符串） |

### 9.8 GroupStateTimeout

**位置**: `sql/api/src/main/java/org/apache/spark/sql/streaming/GroupStateTimeout.java`

| 值 | 描述 |
|------|------|
| **NoTimeout** | 无超时 |
| **ProcessingTimeTimeout** | 处理时间超时 |
| **EventTimeTimeout** | 事件时间超时 |

### 9.9 ForeachWriter接口

```java
public abstract class ForeachWriter<T> {
    public abstract boolean open(long partitionId, long epochId);  // 打开
    public abstract void process(T value);                         // 处理
    public abstract void close(Throwable errorOrNull);             // 关闭
}
```

---

## 十、Catalog Java API

```java
Catalog catalog = spark.catalog();

// 数据库操作
String currentDb = catalog.currentDatabase();
catalog.setCurrentDatabase(String dbName);
Dataset<Database> dbs = catalog.listDatabases();
Dataset<Database> dbs = catalog.listDatabases(String pattern);
Database db = catalog.getDatabase(String dbName);
boolean exists = catalog.databaseExists(String dbName);

// 表操作
Dataset<Table> tables = catalog.listTables();
Dataset<Table> tables = catalog.listTables(String dbName);
Dataset<Table> tables = catalog.listTables(String dbName, String pattern);
Table table = catalog.getTable(String tableName);
boolean exists = catalog.tableExists(String tableName);
catalog.createTable(String tableName, String path, String source);
catalog.createTable(String tableName, String path, String source, Map<String,String> options);

// 函数操作
Dataset<Function> functions = catalog.listFunctions();
Dataset<Function> functions = catalog.listFunctions(String dbName);
Dataset<Function> functions = catalog.listFunctions(String dbName, String pattern);
Function func = catalog.getFunction(String functionName);
boolean exists = catalog.functionExists(String functionName);

// 列操作
Dataset<Column> columns = catalog.listColumns(String tableName);

// 视图操作
catalog.dropTempView(String viewName);
catalog.dropGlobalTempView(String viewName);

// 缓存操作
catalog.cacheTable(String tableName);
catalog.uncacheTable(String tableName);
catalog.clearCache();
boolean isCached = catalog.isCached(String tableName);

// 刷新操作
catalog.refreshTable(String tableName);
catalog.recoverPartitions(String tableName);
```

---

## 十一、UDFRegistration Java API

```java
UDFRegistration udf = spark.udf();

// 注册Scala UDF（通过函数接口）
udf.register(String name, UDF0<R> f, DataType returnType);
udf.register(String name, UDF1<T1,R> f, DataType returnType);
udf.register(String name, UDF2<T1,T2,R> f, DataType returnType);
// ... UDF3 到 UDF22

// 注册Java UDF（通过类名）
udf.registerJava(String name, String className, DataType returnType);

// 注册聚合函数
udf.register(String name, Aggregator<I,B,O> agg, Encoder<O> enc);
```

### 11.1 UDTFRegistration（新增）

```java
UDTFRegistration udtf = spark.udtf();

// 注册表值函数
udtf.register(String name, Class<?> udtfClass);
```

---

## 十二、Encoders Java API

**位置**: `sql/api/src/main/scala/org/apache/spark/sql/Encoders.scala`（共享）

```java
// 基本类型
Encoder<Integer> enc = Encoders.INT();
Encoder<Long> enc = Encoders.LONG();
Encoder<Double> enc = Encoders.DOUBLE();
Encoder<Float> enc = Encoders.FLOAT();
Encoder<String> enc = Encoders.STRING();
Encoder<Boolean> enc = Encoders.BOOLEAN();
Encoder<Byte> enc = Encoders.BYTE();
Encoder<Short> enc = Encoders.SHORT();
Encoder<Date> enc = Encoders.DATE();
Encoder<Timestamp> enc = Encoders.TIMESTAMP();
Encoder<byte[]> enc = Encoders.BINARY();
Encoder<BigDecimal> enc = Encoders.DECIMAL();

// Java Bean
Encoder<Person> enc = Encoders.bean(Person.class);

// Tuple
Encoder<Tuple2<T1,T2>> enc = Encoders.tuple(Encoder<T1> e1, Encoder<T2> e2);
Encoder<Tuple3<T1,T2,T3>> enc = Encoders.tuple(Encoder<T1> e1, Encoder<T2> e2, Encoder<T3> e3);
// ... Tuple4

// 集合
Encoder<List<T>> enc = Encoders.javaList(Encoder<T> elementEncoder);
Encoder<Map<K,V>> enc = Encoders.javaMap(Encoder<K> keyEncoder, Encoder<V> valueEncoder);
```

---

## 十三、Row Java API

**位置**: `sql/api/src/main/scala/org/apache/spark/sql/Row.scala`（共享）

### 13.1 Row创建

```java
// Java方式：使用RowFactory
Row row = RowFactory.create(Object... values);
Row row = RowFactory.create(1, "name", 100.0);
```

### 13.2 Row访问

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **get** | `Object get(int i)` | 按索引获取 |
| **getInt** | `int getInt(int i)` | 获取Int |
| **getLong** | `long getLong(int i)` | 获取Long |
| **getDouble** | `double getDouble(int i)` | 获取Double |
| **getFloat** | `float getFloat(int i)` | 获取Float |
| **getBoolean** | `boolean getBoolean(int i)` | 获取Boolean |
| **getString** | `String getString(int i)` | 获取String |
| **getShort** | `short getShort(int i)` | 获取Short |
| **getByte** | `byte getByte(int i)` | 获取Byte |
| **getDate** | `Date getDate(int i)` | 获取Date |
| **getTimestamp** | `Timestamp getTimestamp(int i)` | 获取Timestamp |
| **getDecimal** | `BigDecimal getDecimal(int i)` | 获取BigDecimal |
| **getSeq** | `Seq<T> getSeq(int i)` | 获取Seq（Scala） |
| **getList** | `List<T> getList(int i)` | 获取List（Java） |
| **getMap** | `Map<K,V> getMap(int i)` | 获取Map（Scala） |
| **getJavaMap** | `java.util.Map<K,V> getJavaMap(int i)` | 获取Map（Java） |
| **getStruct** | `Row getStruct(int i)` | 获取结构Row |
| **isNullAt** | `boolean isNullAt(int i)` | 是否为空 |

### 13.3 Row信息

| 方法 | Java签名 | 描述 |
|------|----------|------|
| **length** / **size** | `int length()` | Row长度 |
| **schema** | `StructType schema()` | Schema |
| **fieldIndex** | `int fieldIndex(String name)` | 列名索引 |

---

## 十四、DataTypes Java API

**位置**: `sql/api/src/main/java/org/apache/spark/sql/types/DataTypes.java`

```java
// 基本类型
DataType type = DataTypes.NullType;
DataType type = DataTypes.BooleanType;
DataType type = DataTypes.ByteType;
DataType type = DataTypes.ShortType;
DataType type = DataTypes.IntegerType;
DataType type = DataTypes.LongType;
DataType type = DataTypes.FloatType;
DataType type = DataTypes.DoubleType;
DataType type = DataTypes.StringType;
DataType type = DataTypes.BinaryType;
DataType type = DataTypes.DateType;
DataType type = DataTypes.TimestampType;
DataType type = DataTypes.TimestampNTZType;
DataType type = DataTypes.TimeType;
DataType type = DataTypes.CalendarIntervalType;

// Decimal
DataType type = DataTypes.createDecimalType();
DataType type = DataTypes.createDecimalType(int precision, int scale);

// Array
DataType type = DataTypes.createArrayType(DataType elementType);
DataType type = DataTypes.createArrayType(DataType elementType, boolean containsNull);

// Map
DataType type = DataTypes.createMapType(DataType keyType, DataType valueType);
DataType type = DataTypes.createMapType(DataType keyType, DataType valueType, boolean valueContainsNull);

// Struct
StructField field = DataTypes.createStructField(String name, DataType dataType, boolean nullable);
StructType struct = DataTypes.createStructType(StructField[] fields);
StructType struct = DataTypes.createStructType(List<StructField> fields);

// 新增类型
DataType type = DataTypes.VariantType;
DataType type = DataTypes.GeographyType;
DataType type = DataTypes.GeometryType;
```

---

## 十五、StructField/StructType Java API

```java
// 创建StructField
StructField field = new StructField(
    String name,
    DataType dataType,
    boolean nullable,
    Metadata metadata
);

// 创建StructType
StructType schema = new StructType()
    .add("col1", DataTypes.IntegerType)
    .add("col2", DataTypes.StringType)
    .add("col3", DataTypes.DoubleType, false);

// 或使用数组
StructType schema = new StructType(new StructField[]{
    DataTypes.createStructField("col1", DataTypes.IntegerType, true),
    DataTypes.createStructField("col2", DataTypes.StringType, true)
});

// 访问
StructField[] fields = schema.fields();
String[] names = schema.fieldNames();
StructField field = schema.apply(String fieldName);
int index = schema.fieldIndex(String fieldName);
```

---

## 十六、内置函数 Java API

**位置**: `sql/api/src/main/scala/org/apache/spark/sql/functions.scala`（共享）

使用方式：`import static org.apache.spark.sql.functions.*;`

### 16.1 列创建

```java
Column col = col("columnName");
Column col = column("columnName");
Column col = lit(value);
Column col = expr("expression");
```

### 16.2 数学函数

```java
Column result = abs(Column col);
Column result = ceil(Column col);
Column result = floor(Column col);
Column result = round(Column col);
Column result = round(Column col, int scale);
Column result = bround(Column col);
Column result = bround(Column col, int scale);
Column result = sqrt(Column col);
Column result = exp(Column col);
Column result = log(Column col);
Column result = log10(Column col);
Column result = log2(Column col);
Column result = pow(Column col1, Column col2);
Column result = power(Column col1, Column col2);
Column result = sin(Column col);
Column result = cos(Column col);
Column result = tan(Column col);
Column result = asin(Column col);
Column result = acos(Column col);
Column result = atan(Column col);
Column result = atan2(Column y, Column x);
Column result = degrees(Column col);
Column result = radians(Column col);
Column result = signum(Column col);
Column result = rand();
Column result = rand(long seed);
Column result = randn();
Column result = randn(long seed);
Column result = greatest(Column... cols);
Column result = least(Column... cols);
```

### 16.3 字符串函数

```java
Column result = upper(Column col);
Column result = lower(Column col);
Column result = initcap(Column col);
Column result = length(Column col);
Column result = concat(Column... cols);
Column result = concat_ws(String sep, Column... cols);
Column result = substring(Column col, int pos, int len);
Column result = substring_index(Column str, String delim, int count);
Column result = trim(Column col);
Column result = ltrim(Column col);
Column result = rtrim(Column col);
Column result = ltrim(Column col, String trimString);
Column result = rtrim(Column col, String trimString);
Column result = trim(Column col, String trimString);
Column result = lpad(Column col, int len, String pad);
Column result = rpad(Column col, int len, String pad);
Column result = repeat(Column col, int n);
Column result = reverse(Column col);
Column result = replace(Column col, String search, String replace);
Column result = regexp_replace(Column col, String pattern, String replacement);
Column result = regexp_extract(Column col, String pattern, int idx);
Column result = regexp_extract_all(Column col, String pattern, int idx);
Column result = split(Column col, String pattern);
Column result = split(Column col, String pattern, int limit);
Column result = like(Column col, String pattern);
Column result = rlike(Column col, String regex);
Column result = contains(Column col, String substring);
Column result = startsWith(Column col, String substring);
Column result = endsWith(Column col, String substring);
Column result = locate(String substr, Column col);
Column result = locate(String substr, Column col, int pos);
Column result = instr(Column str, String substr);
Column result = ascii(Column col);
Column result = base64(Column col);
Column result = unbase64(Column col);
Column result = encode(Column col, String charset);
Column result = decode(Column col, String charset);
Column result = format_number(Column col, int d);
Column result = format_string(String format, Column... cols);
Column result = translate(Column col, String matchingString, String replaceString);
Column result = initcap(Column col);
Column result = soundex(Column col);
Column result = levenshtein(Column col1, Column col2);
```

### 16.4 日期时间函数

```java
Column result = current_date();
Column result = current_timestamp();
Column result = current_timezone();
Column result = now();
Column result = date_add(Column start, int days);
Column result = date_add(Column start, Column days);
Column result = date_sub(Column start, int days);
Column result = date_sub(Column start, Column days);
Column result = datediff(Column end, Column start);
Column result = add_months(Column start, int months);
Column result = add_months(Column start, Column months);
Column result = months_between(Column end, Column start);
Column result = months_between(Column end, Column start, boolean roundOff);
Column result = last_day(Column col);
Column result = next_day(Column date, String dayOfWeek);
Column result = year(Column col);
Column result = quarter(Column col);
Column result = month(Column col);
Column result = day(Column col);
Column result = dayofmonth(Column col);
Column result = dayofweek(Column col);
Column result = dayofyear(Column col);
Column result = hour(Column col);
Column result = minute(Column col);
Column result = second(Column col);
Column result = weekofyear(Column col);
Column result = weekday(Column col);
Column result = make_date(Column year, Column month, Column day);
Column result = make_timestamp(Column year, Column month, Column day, Column hour, Column min, Column sec);
Column result = to_date(Column col);
Column result = to_date(Column col, String format);
Column result = to_timestamp(Column col);
Column result = to_timestamp(Column col, String format);
Column result = date_format(Column date, String format);
Column result = trunc(Column date, String format);
Column result = date_trunc(String format, Column date);
Column result = from_unixtime(Column ut);
Column result = from_unixtime(Column ut, String format);
Column result = unix_timestamp();
Column result = unix_timestamp(Column time);
Column result = unix_timestamp(Column time, String format);
Column result = from_utc_timestamp(Column timestamp, String timezone);
Column result = to_utc_timestamp(Column timestamp, String timezone);
```

### 16.5 聚合函数

```java
Column result = count(Column col);
Column result = countDistinct(Column... cols);
Column result = approx_count_distinct(Column col);
Column result = approx_count_distinct(Column col, double rsd);
Column result = sum(Column col);
Column result = sumDistinct(Column col);
Column result = avg(Column col);
Column result = mean(Column col);
Column result = max(Column col);
Column result = min(Column col);
Column result = first(Column col);
Column result = first(Column col, boolean ignoreNulls);
Column result = last(Column col);
Column result = last(Column col, boolean ignoreNulls);
Column result = count(String colName);
Column result = sum(String colName);
Column result = avg(String colName);
Column result = max(String colName);
Column result = min(String colName);
Column result = variance(Column col);
Column result = var_samp(Column col);
Column result = var_pop(Column col);
Column result = stddev(Column col);
Column result = stddev_samp(Column col);
Column result = stddev_pop(Column col);
Column result = skewness(Column col);
Column result = kurtosis(Column col);
Column result = corr(Column col1, Column col2);
Column result = covar_pop(Column col1, Column col2);
Column result = covar_samp(Column col1, Column col2);
Column result = collect_list(Column col);
Column result = collect_set(Column col);
Column result = grouping(Column col);
Column result = grouping_id(Column... cols);
```

### 16.6 窗口函数

```java
Column result = row_number();
Column result = rank();
Column result = dense_rank();
Column result = percent_rank();
Column result = ntile(int n);
Column result = cume_dist();
Column result = lead(Column col, int offset);
Column result = lead(Column col, int offset, Object defaultValue);
Column result = lag(Column col, int offset);
Column result = lag(Column col, int offset, Object defaultValue);
Column result = first_value(Column col);
Column result = last_value(Column col);
Column result = nth_value(Column col, int offset);
```

### 16.7 数组/集合函数

```java
Column result = array(Column... cols);
Column result = array_contains(Column array, Object value);
Column result = array_append(Column array, Object value);
Column result = array_insert(Column array, int pos, Object value);
Column result = array_remove(Column array, Object value);
Column result = array_position(Column array, Object value);
Column result = array_size(Column array);
Column result = size(Column col);
Column result = array_sort(Column array);
Column result = array_distinct(Column array);
Column result = array_union(Column array1, Column array2);
Column result = array_intersect(Column array1, Column array2);
Column result = array_except(Column array1, Column array2);
Column result = explode(Column array);
Column result = explode_outer(Column array);
Column result = posexplode(Column array);
Column result = posexplode_outer(Column array);
Column result = flatten(Column array);
Column result = sort_array(Column array);
Column result = sort_array(Column array, boolean asc);
Column result = sequence(Column start, Column end);
Column result = sequence(Column start, Column end, Column step);
Column result = slice(Column array, int start, int length);
Column result = array_join(Column array, String delimiter);
Column result = array_join(Column array, String delimiter, String nullReplacement);
Column result = arrays_zip(Column... arrays);
Column result = element_at(Column array, int index);
Column result = element_at(Column map, String key);
Column result = filter(Column array, Function filterFunc);
Column result = transform(Column array, Function transformFunc);
Column result = aggregate(Column array, Column initialValue, Function mergeFunc, Function finishFunc);
Column result = forall(Column array, Function predFunc);
Column result = exists(Column array, Function predFunc);
```

### 16.8 Map函数

```java
Column result = map(Column key, Column value);
Column result = map(Column... cols);
Column result = map_from_arrays(Column keys, Column values);
Column result = map_keys(Column map);
Column result = map_values(Column map);
Column result = map_entries(Column map);
Column result = map_contains_key(Column map, Column key);
Column result = map_get(Column map, Column key);
Column result = map_concat(Column... maps);
Column result = transform_keys(Column map, Function func);
Column result = transform_values(Column map, Function func);
Column result = element_at(Column map, String key);
```

### 16.9 JSON函数

```java
Column result = get_json_object(Column col, String path);
Column result = json_tuple(Column json, String... fields);
Column result = from_json(Column json, Column schema);
Column result = from_json(Column json, StructType schema);
Column result = from_json(Column json, String schema);
Column result = to_json(Column col);
Column result = to_json(Column col, Map<String,String> options);
Column result = schema_of_json(String json);
Column result = schema_of_json(Column json);
Column result = json_array_length(Column json);
```

### 16.10 条件函数

```java
Column result = when(Column condition, Object value);
Column result = coalesce(Column... cols);
Column result = ifnull(Column col1, Column col2);
Column result = nullif(Column col1, Column col2);
Column result = nvl(Column col1, Column col2);
Column result = nvl2(Column col1, Column col2, Column col3);
Column result = isnan(Column col);
Column result = isnull(Column col);
Column result = isnotnull(Column col);
```

### 16.11 排序函数

```java
Column result = asc(String columnName);
Column result = asc_nulls_first(String columnName);
Column result = asc_nulls_last(String columnName);
Column result = desc(String columnName);
Column result = desc_nulls_first(String columnName);
Column result = desc_nulls_last(String columnName);
```

### 16.12 窗口定义

```java
WindowSpec window = Window.partitionBy(Column... cols);
WindowSpec window = Window.partitionBy(String... colNames);
WindowSpec window = Window.orderBy(Column... cols);
WindowSpec window = Window.orderBy(String... colNames);
WindowSpec window = Window.rowsBetween(int start, int end);
WindowSpec window = Window.rangeBetween(int start, int end);
WindowSpec window = Window.unboundedPreceding();
WindowSpec window = Window.unboundedFollowing();
WindowSpec window = Window.currentRow();

// 组合使用
WindowSpec window = Window.partitionBy("dept").orderBy("salary").rowsBetween(Window.unboundedPreceding(), Window.currentRow());
```

---

## 十七、RuntimeConfig Java API

```java
RuntimeConfig conf = spark.conf();

// 配置操作
String value = conf.get(String key);
String value = conf.get(String key, String defaultValue);
Option<String> value = conf.getOption(String key);
Map<String,String> all = conf.getAll();
conf.set(String key, String value);
conf.unset(String key);
boolean modifiable = conf.isModifiable(String key);
```

---

## 十八、Java使用示例

### 18.1 批处理示例

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.FilterFunction;

public class JavaBatchExample {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("JavaBatchExample")
            .master("local[*]")
            .getOrCreate();
        
        // 读取数据
        Dataset<Row> df = spark.read().parquet("/path/to/data.parquet");
        
        // DataFrame操作
        Dataset<Row> result = df
            .filter(functions.col("age").gt(18))
            .groupBy("department")
            .agg(
                functions.avg("salary"),
                functions.count("*")
            );
        
        // 显示结果
        result.show();
        
        // 类型化Dataset操作
        Dataset<Person> people = df.as(Encoders.bean(Person.class));
        
        // map操作
        Dataset<String> names = people.map(
            (MapFunction<Person, String>) p -> p.getName(),
            Encoders.STRING()
        );
        
        // filter操作
        Dataset<Person> filtered = people.filter(
            (FilterFunction<Person>) p -> p.getAge() > 30
        );
        
        // 写入结果
        result.write()
            .mode(SaveMode.Overwrite)
            .parquet("/path/to/output");
        
        spark.stop();
    }
}

// Java Bean定义
public class Person implements Serializable {
    private String name;
    private int age;
    private double salary;
    
    // Getter和Setter方法
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public int getAge() { return age; }
    public void setAge(int age) { this.age = age; }
    public double getSalary() { return salary; }
    public void setSalary(double salary) { this.salary = salary; }
}
```

### 18.2 流处理示例

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.Trigger;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.functions;

public class JavaStreamingExample {
    public static void main(String[] args) throws Exception {
        SparkSession spark = SparkSession.builder()
            .appName("JavaStreamingExample")
            .master("local[*]")
            .getOrCreate();
        
        // Kafka流读取
        Dataset<Row> kafkaStream = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "topic")
            .option("startingOffsets", "latest")
            .load();
        
        // 解析JSON
        Dataset<Row> parsed = kafkaStream
            .select(
                functions.from_json(
                    functions.col("value").cast("string"),
                    schema
                ).as("data")
            )
            .select("data.*");
        
        // 设置水位
        Dataset<Row> withWatermark = parsed
            .withWatermark("timestamp", "10 minutes");
        
        // 窗口聚合
        Dataset<Row> aggregated = withWatermark
            .groupBy(
                functions.window(functions.col("timestamp"), "5 minutes"),
                functions.col("category")
            )
            .agg(
                functions.count("*").as("count"),
                functions.sum("amount").as("total")
            );
        
        // 启动流查询
        StreamingQuery query = aggregated.writeStream()
            .outputMode("update")
            .format("console")
            .trigger(Trigger.ProcessingTime("5 seconds"))
            .start();
        
        query.awaitTermination();
    }
}
```

### 18.3 有状态处理示例

```java
import org.apache.spark.sql.streaming.GroupState;
import org.apache.spark.sql.streaming.GroupStateTimeout;
import org.apache.spark.api.java.function.MapGroupsWithStateFunction;

public class JavaStatefulExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("StatefulExample")
            .getOrCreate();
        
        // 事件数据
        Dataset<Event> events = spark.readStream()
            .format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "events")
            .load()
            .as(Encoders.bean(Event.class));
        
        // 按用户分组
        KeyValueGroupedDataset<String, Event> grouped = events
            .groupByKey(
                (MapFunction<Event, String>) e -> e.getUserId(),
                Encoders.STRING()
            );
        
        // 有状态处理
        Dataset<UserState> result = grouped.mapGroupsWithState(
            new MapGroupsWithStateFunction<String, Event, UserState, UserState>() {
                @Override
                public UserState call(
                    String userId,
                    Iterator<Event> events,
                    GroupState<UserState> state
                ) throws Exception {
                    
                    // 获取或初始化状态
                    UserState prevState;
                    if (state.exists()) {
                        prevState = state.get();
                    } else {
                        prevState = new UserState(userId);
                    }
                    
                    // 处理事件
                    while (events.hasNext()) {
                        Event event = events.next();
                        prevState.update(event);
                    }
                    
                    // 更新状态
                    state.update(prevState);
                    
                    // 设置超时
                    state.setTimeoutDuration("1 hour");
                    
                    return prevState;
                }
            },
            GroupStateTimeout.ProcessingTimeTimeout(),
            Encoders.bean(UserState.class),
            Encoders.bean(UserState.class)
        );
        
        result.writeStream()
            .format("console")
            .outputMode("update")
            .start()
            .awaitTermination();
    }
}
```

### 18.4 UDF注册示例

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.api.java.UDF2;
import org.apache.spark.sql.types.DataTypes;

public class JavaUDFExample {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("UDFExample")
            .master("local[*]")
            .getOrCreate();
        
        // 注册UDF1
        spark.udf().register("stringLength",
            (UDF1<String, Integer>) s -> s.length(),
            DataTypes.IntegerType);
        
        // 注册UDF2
        spark.udf().register("concatWithSpace",
            (UDF2<String, String, String>) (s1, s2) -> s1 + " " + s2,
            DataTypes.StringType);
        
        // 使用UDF
        Dataset<Row> df = spark.sql("SELECT stringLength('hello')");
        df.show();
        
        Dataset<Row> df2 = spark.sql("SELECT concatWithSpace('hello', 'world')");
        df2.show();
        
        // Lambda方式注册
        spark.udf().register("addOne",
            (Integer x) -> x + 1,
            DataTypes.IntegerType);
        
        // 在DataFrame中使用
        Dataset<Row> df3 = spark.range(10)
            .withColumn("plusOne", functions.call_function("addOne", functions.col("id")));
        
        df3.show();
    }
}
```

### 18.5 UDAF（聚合函数）示例

```java
import org.apache.spark.sql.expressions.Aggregator;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Encoder;

public class JavaUDAFExample {
    
    // 定义Buffer类
    public static class AverageBuffer implements Serializable {
        public long sum;
        public long count;
        
        public AverageBuffer() {
            this.sum = 0;
            this.count = 0;
        }
        
        public AverageBuffer(long sum, long count) {
            this.sum = sum;
            this.count = count;
        }
    }
    
    // 定义Aggregator
    public static class AverageAggregator extends Aggregator<Long, AverageBuffer, Double> {
        
        @Override
        public AverageBuffer zero() {
            return new AverageBuffer();
        }
        
        @Override
        public AverageBuffer reduce(AverageBuffer buffer, Long input) {
            buffer.sum += input;
            buffer.count += 1;
            return buffer;
        }
        
        @Override
        public AverageBuffer merge(AverageBuffer b1, AverageBuffer b2) {
            return new AverageBuffer(b1.sum + b2.sum, b1.count + b2.count);
        }
        
        @Override
        public Double finish(AverageBuffer buffer) {
            return (double) buffer.sum / buffer.count;
        }
        
        @Override
        public Encoder<AverageBuffer> bufferEncoder() {
            return Encoders.bean(AverageBuffer.class);
        }
        
        @Override
        public Encoder<Double> outputEncoder() {
            return Encoders.DOUBLE();
        }
    }
    
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("UDAFExample")
            .master("local[*]")
            .getOrCreate();
        
        // 注册UDAF
        spark.udf().register("myAverage",
            new AverageAggregator(),
            Encoders.DOUBLE());
        
        // 使用UDAF
        Dataset<Row> df = spark.range(100)
            .groupBy()
            .agg(functions.call_function("myAverage", functions.col("id")));
        
        df.show();
    }
}
```

---

## 十九、Java与Scala API对比

| 特性 | Java API | Scala API |
|------|----------|-----------|
| **函数定义** | Function接口（如`MapFunction<T,U>`） | Scala lambda（如`T => U`） |
| **UDF参数** | `UDF1<T1,R>` - `UDF22`接口 | 函数定义 |
| **Row创建** | `RowFactory.create(Object...)` | `Row(value1, value2)` |
| **列引用** | `functions.col("name")` | `$"name"` 或 `col("name")` |
| **列比较** | `col.equalTo(other)` | `$"col" === other` |
| **算术运算** | `col.plus(other)` | `$"col" + other` |
| **集合类型** | `java.util.List` | `Seq`, `List` |
| **Map类型** | `java.util.Map` | `Map` |
| **返回数组** | `T[]` | `Array[T]` |
| **返回列表** | `List<T>` | `Seq[T]` |
| **Option类型** | `Optional<T>` | `Option[T]` |
| **Encoder** | `Encoders.bean(Class)` | `implicitly[Encoder[T]]` |
| **类型转换** | `dataset.as(Encoders.bean(Person.class))` | `dataset.as[Person]` |
| **静态导入** | `import static org.apache.spark.sql.functions.*;` | `import org.apache.spark.sql.functions._` |

---

## 二十、Java API包结构

```
org.apache.spark.api.java/
  ├── JavaSparkContext          # RDD入口
  ├── JavaRDD                   # Java RDD
  ├── JavaPairRDD               # Java PairRDD
  ├── JavaDoubleRDD             # Java DoubleRDD
  ├── Optional                  # Java Optional
  ├── StorageLevels             # 存储级别
  └── function/                 # 函数接口
      ├── MapFunction<T,U>
      ├── FilterFunction<T>
      ├── FlatMapFunction<T,R>
      ├── ReduceFunction<T>
      ├── ForeachFunction<T>
      ├── MapGroupsFunction<K,V,R>
      ├── MapPartitionsFunction<T,U>
      ├── PairFunction<T,K,V>
      ├── Function<T,R>
      ├── Function2<T1,T2,R>
      └── ... (更多函数接口)

org.apache.spark.sql/
  ├── SparkSession              # 入口
  ├── Dataset<T>                # 类型化Dataset
  ├── DataFrame (= Dataset<Row>)
  ├── Column                    # 列表达式
  ├── Row                       # 数据行
  ├── RowFactory                # Row创建工厂
  ├── SaveMode                  # 保存模式枚举
  ├── DataFrameReader           # 数据读取器
  ├── DataFrameWriter<T>        # 数据写入器
  ├── DataFrameNaFunctions      # 空值处理
  ├── DataFrameStatFunctions    # 统计函数
  ├── RelationalGroupedDataset  # 分组数据集
  ├── KeyValueGroupedDataset<K,V>  # 键值分组
  ├── Catalog                   # 元数据接口
  ├── UDFRegistration           # UDF注册
  ├── Encoders                  # 编码器
  ├── functions                 # 内置函数（静态方法）
  └── types/
      ├── DataTypes             # 数据类型工厂
      ├── StructType            # 结构类型
      ├── StructField           # 结构字段
      ├── ArrayType             # 数组类型
      ├── MapType               # Map类型
      ├── DataType              # 数据类型基类
      └── ...

org.apache.spark.sql.api.java/
  ├── UDF0<R>
  ├── UDF1<T1,R>
  ├── UDF2<T1,T2,R>
  ├── ...
  ├── UDF22<T1,...,T22,R>

org.apache.spark.sql.streaming/
  ├── DataStreamReader          # 流读取器
  ├── DataStreamWriter<T>       # 流写入器
  ├── StreamingQuery            # 流查询
  ├── StreamingQueryManager     # 流查询管理器
  ├── Trigger                   # 触发器
  ├── OutputMode                # 输出模式枚举
  ├── GroupState                # 状态接口
  ├── GroupStateTimeout         # 超时类型枚举
  └── ForeachWriter<T>          # foreach输出器
```

---

## 二十一、常用Java静态导入

```java
// 常用的静态导入
import static org.apache.spark.sql.functions.*;

// Column操作
col("columnName");
lit(value);
expr("expression");

// 聚合函数
count("*");
sum("column");
avg("column");
max("column");
min("column");

// 数学函数
abs(col("x"));
sqrt(col("x"));
ceil(col("x"));

// 字符串函数
upper(col("name"));
lower(col("name"));
trim(col("name"));
concat(col("a"), col("b"));

// 日期函数
current_date();
current_timestamp();
date_add(col("date"), 1);
year(col("date"));

// 窗口函数
row_number();
rank();
dense_rank();
lead(col("x"), 1);
lag(col("x"), 1);

// 排序
asc("column");
desc("column");
```

---

## 二十二、Java API文档资源

- **官方JavaDoc**: https://spark.apache.org/docs/latest/api/java/
- **核心包**: `org.apache.spark.sql`
- **函数接口**: `org.apache.spark.api.java.function`
- **UDF接口**: `org.apache.spark.sql.api.java`
- **数据类型**: `org.apache.spark.sql.types`
- **流处理**: `org.apache.spark.sql.streaming`

---

**文档版本**: v1.0  
**生成日期**: 2026-04-28  
**基于版本**: Apache Spark 4.2.0-SNAPSHOT