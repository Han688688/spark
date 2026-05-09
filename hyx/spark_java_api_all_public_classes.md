# Spark Java API Complete Public Classes Reference

This document provides a comprehensive reference of all public Java classes, interfaces, and enums in Apache Spark.

**Total Java Files: 937**
**Generated from: Apache Spark Source Code**

---

## Table of Contents

1. [Core RDD Java API](#一core-rdd-java-api)
2. [Streaming Java API](#二streaming-java-api)
3. [Java Function Interfaces](#三java-function-interfaces)
4. [SQL DataFrame Java API](#四sql-dataframe-java-api)
5. [UDF Interfaces](#五udf接口)
6. [SQL Connector Catalog API](#六sql-connector-catalog-api)
7. [SQL Connector Read API](#七sql-connector-read-api)
8. [SQL Connector Write API](#八sql-connector-write-api)
9. [SQL Connector Expressions API](#九sql-connector-expressions-api)
10. [SQL Vectorized API](#十sql-vectorized-api)
11. [Streaming State API](#十一streaming-state-api)
12. [GraphX Java API](#十二graphx-java-api)
13. [Shuffle API](#十三shuffle-api)
14. [Memory API](#十四memory-api)
15. [Launcher API](#十五launcher-api)
16. [Unsafe Types API](#十六unsafe-types-api)
17. [Variant Types API](#十七variant-types-api)
18. [Annotation API](#十八annotation-api)
19. [Other Utility APIs](#十九其他utility-api)
20. [Status API](#二十status-api)
21. [Parquet/ORC API](#二十一parquetorc-api)
21. [Hive ThriftServer API](#二十二hive-thriftserver-api)

---

## 一、Core RDD Java API

### SparkJobInfo
**包路径**: `org.apache.spark.SparkJobInfo`
**类型**: Interface

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| jobId | 无 | int | 获取作业ID | `int id = jobInfo.jobId();` |
| stageIds | 无 | int[] | 获取所有阶段ID | `int[] stages = jobInfo.stageIds();` |
| status | 无 | JobExecutionStatus | 获取作业状态 | `JobExecutionStatus status = jobInfo.status();` |

### JobExecutionStatus
**包路径**: `org.apache.spark.JobExecutionStatus`
**类型**: Enum

| 枚举值 | 描述 | 示例 |
|--------|------|------|
| RUNNING | 作业正在运行 | `status == JobExecutionStatus.RUNNING` |
| SUCCEEDED | 作业成功完成 | `status == JobExecutionStatus.SUCCEEDED` |
| FAILED | 作业失败 | `status == JobExecutionStatus.FAILED` |
| UNKNOWN | 作业状态未知 | `status == JobExecutionStatus.UNKNOWN` |

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| fromString | String str | JobExecutionStatus | 从字符串解析状态 | `JobExecutionStatus.fromString("RUNNING");` |

---

## 二、Streaming Java API

### StreamingContextState
**包路径**: `org.apache.spark.streaming.StreamingContextState`
**类型**: Enum (@DeveloperApi)

| 枚举值 | 描述 | 示例 |
|--------|------|------|
| INITIALIZED | 上下文已创建但未启动 | `state == StreamingContextState.INITIALIZED` |
| ACTIVE | 上下文已启动且未停止 | `state == StreamingContextState.ACTIVE` |
| STOPPED | 上下文已停止，无法再使用 | `state == StreamingContextState.STOPPED` |

### WriteAheadLog
**包路径**: `org.apache.spark.streaming.util.WriteAheadLog`
**类型**: Abstract Class (@DeveloperApi)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| write | ByteBuffer record, long time | WriteAheadLogRecordHandle | 写入记录并返回句柄 | `WriteAheadLogRecordHandle handle = wal.write(data, timestamp);` |
| read | WriteAheadLogRecordHandle handle | ByteBuffer | 根据句柄读取记录 | `ByteBuffer data = wal.read(handle);` |
| readAll | 无 | Iterator<ByteBuffer> | 读取所有未清理的记录 | `Iterator<ByteBuffer> iter = wal.readAll();` |
| clean | long threshTime, boolean waitForCompletion | void | 清理旧记录 | `wal.clean(threshold, true);` |
| close | 无 | void | 关闭日志 | `wal.close();` |

### WriteAheadLogRecordHandle
**包路径**: `org.apache.spark.streaming.util.WriteAheadLogRecordHandle`
**类型**: Class

用于表示预写日志中记录的句柄。

### BatchStatus
**包路径**: `org.apache.spark.status.api.v1.streaming.BatchStatus`
**类型**: Enum

---

## 三、Java Function Interfaces

所有函数接口位于 `org.apache.spark.api.java.function` 包下。

### Function<T1, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 v1 | R | 应用函数 | `rdd.map(x -> x + 1);` |

**示例**:
```java
JavaRDD<Integer> result = rdd.map(new Function<Integer, Integer>() {
    @Override
    public Integer call(Integer v1) {
        return v1 * 2;
    }
});
```

### Function0<R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | 无 | R | 无参数函数 | `() -> 42` |

### Function2<T1, T2, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 v1, T2 v2 | R | 双参数函数 | `(a, b) -> a + b` |

### Function3<T1, T2, T3, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 v1, T2 v2, T3 v3 | R | 三参数函数 | `(a, b, c) -> a + b + c` |

### Function4<T1, T2, T3, T4, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 v1, T2 v2, T3 v3, T4 v4 | R | 四参数函数 | `(a, b, c, d) -> a + b + c + d` |

### FilterFunction<T>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T value | boolean | 过滤判断 | `ds.filter(x -> x > 10);` |

**示例**:
```java
Dataset<Row> filtered = df.filter(new FilterFunction<Row>() {
    @Override
    public boolean call(Row value) {
        return value.getInt(0) > 100;
    }
});
```

### MapFunction<T, U>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T value | U | 映射转换 | `ds.map(x -> x.toString());` |

### FlatMapFunction<T, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T t | Iterator<R> | 展平映射 | `rdd.flatMap(x -> Arrays.asList(x.split(" ")).iterator());` |

### ReduceFunction<T>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T v1, T v2 | T | 归约操作 | `(a, b) -> a + b` |

**示例**:
```java
Integer sum = ds.reduce(new ReduceFunction<Integer>() {
    @Override
    public Integer call(Integer v1, Integer v2) {
        return v1 + v2;
    }
});
```

### ForeachFunction<T>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T t | void | 遍历处理 | `ds.foreach(x -> System.out.println(x));` |

### VoidFunction<T>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T t | void | 无返回值操作 | `rdd.foreach(x -> process(x));` |

### VoidFunction2<T1, T2>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 v1, T2 v2 | void | 双参数无返回值操作 | `(a, b) -> process(a, b);` |

### PairFunction<T, K, V>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T t | Tuple2<K, V> | 创建键值对 | `rdd.mapToPair(x -> new Tuple2<>(x, 1));` |

**示例**:
```java
JavaPairRDD<String, Integer> pairs = rdd.mapToPair(
    new PairFunction<String, String, Integer>() {
        @Override
        public Tuple2<String, Integer> call(String s) {
            return new Tuple2<>(s, s.length());
        }
    });
```

### PairFlatMapFunction<T, K, V>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T t | Iterator<Tuple2<K, V>> | 展平映射为键值对 | `rdd.flatMapToPair(...)` |

### DoubleFunction<T>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T t | double | 映射为Double | `rdd.mapToDouble(x -> x.doubleValue());` |

### DoubleFlatMapFunction<T>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T t | Iterator<Double> | 展平映射为Double | `rdd.flatMapToDouble(...)` |

### MapPartitionsFunction<T, U>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | Iterator<T> input | Iterator<U> | 分区映射 | `ds.mapPartitions(iter -> processPartition(iter));` |

### ForeachPartitionFunction<T>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | Iterator<T> t | void | 分区遍历处理 | `ds.foreachPartition(iter -> saveToDb(iter));` |

### MapGroupsFunction<K, V, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | K key, Iterator<V> values | R | 分组映射 | `groupedDs.mapGroups((k, iter) -> ...);` |

### FlatMapGroupsFunction<K, V, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | K key, Iterator<V> values | Iterator<R> | 分组展平映射 | `groupedDs.flatMapGroups(...);` |

### CoGroupFunction<K, V1, V2, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | K key, Iterator<V1> left, Iterator<V2> right | Iterator<R> | 协同分组 | `ds1.coGroup(ds2, ...);` |

### FlatMapFunction2<T1, T2, R>
**类型**: Interface (@FunctionalInterface)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 t1, T2 t2 | Iterator<R> | 双输入展平映射 | `(a, b) -> ...;` |

### MapGroupsWithStateFunction<K, V, S, R>
**包路径**: `org.apache.spark.api.java.function.MapGroupsWithStateFunction`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | K key, Iterator<V> values, GroupState<S> state | R | 带状态的分组映射 | `grouped.mapGroupsWithState(...);` |

### FlatMapGroupsWithStateFunction<K, V, S, R>
**包路径**: `org.apache.spark.api.java.function.FlatMapGroupsWithStateFunction`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | K key, Iterator<V> values, GroupState<S> state | Iterator<R> | 带状态的分组展平映射 | `grouped.flatMapGroupsWithState(...);` |

---

## 四、SQL DataFrame Java API

### DataTypes
**包路径**: `org.apache.spark.sql.types.DataTypes`
**类型**: Class (@Stable)

| 静态字段 | 类型 | 描述 | 示例 |
|----------|------|------|------|
| StringType | DataType | 字符串类型 | `DataTypes.StringType` |
| BinaryType | DataType | 二进制类型 | `DataTypes.BinaryType` |
| BooleanType | DataType | 布尔类型 | `DataTypes.BooleanType` |
| DateType | DataType | 日期类型 | `DataTypes.DateType` |
| TimestampType | DataType | 时间戳类型 | `DataTypes.TimestampType` |
| TimestampNTZType | DataType | 无时区时间戳类型 | `DataTypes.TimestampNTZType` |
| CalendarIntervalType | DataType | 日历间隔类型 | `DataTypes.CalendarIntervalType` |
| DoubleType | DataType | 双精度浮点类型 | `DataTypes.DoubleType` |
| FloatType | DataType | 单精度浮点类型 | `DataTypes.FloatType` |
| ByteType | DataType | 字节类型 | `DataTypes.ByteType` |
| IntegerType | DataType | 整数类型 | `DataTypes.IntegerType` |
| LongType | DataType | 长整数类型 | `DataTypes.LongType` |
| ShortType | DataType | 短整数类型 | `DataTypes.ShortType` |
| NullType | DataType | 空类型 | `DataTypes.NullType` |
| VariantType | DataType | Variant类型 | `DataTypes.VariantType` |

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| createArrayType | DataType elementType | ArrayType | 创建数组类型 | `DataTypes.createArrayType(DataTypes.IntegerType);` |
| createArrayType | DataType elementType, boolean containsNull | ArrayType | 创建数组类型(指定是否可空) | `DataTypes.createArrayType(DataTypes.StringType, true);` |
| createDecimalType | int precision, int scale | DecimalType | 创建Decimal类型 | `DataTypes.createDecimalType(10, 2);` |
| createDecimalType | 无 | DecimalType | 创建默认Decimal类型 | `DataTypes.createDecimalType();` |
| createDayTimeIntervalType | byte startField, byte endField | DayTimeIntervalType | 创建日期时间间隔类型 | `DataTypes.createDayTimeIntervalType(0, 3);` |
| createDayTimeIntervalType | 无 | DayTimeIntervalType | 创建默认日期时间间隔类型 | `DataTypes.createDayTimeIntervalType();` |
| createYearMonthIntervalType | byte startField, byte endField | YearMonthIntervalType | 创建年月间隔类型 | `DataTypes.createYearMonthIntervalType(0, 1);` |
| createYearMonthIntervalType | 无 | YearMonthIntervalType | 创建默认年月间隔类型 | `DataTypes.createYearMonthIntervalType();` |
| createMapType | DataType keyType, DataType valueType | MapType | 创建Map类型 | `DataTypes.createMapType(DataTypes.StringType, DataTypes.IntegerType);` |
| createMapType | DataType keyType, DataType valueType, boolean valueContainsNull | MapType | 创建Map类型(指定是否可空) | `DataTypes.createMapType(k, v, false);` |
| createStructField | String name, DataType dataType, boolean nullable, Metadata metadata | StructField | 创建结构字段 | `DataTypes.createStructField("id", DataTypes.IntegerType, true);` |
| createStructField | String name, DataType dataType, boolean nullable | StructField | 创建结构字段(无元数据) | `DataTypes.createStructField("name", DataTypes.StringType, false);` |
| createStructType | List<StructField> fields | StructType | 创建结构类型 | `DataTypes.createStructType(Arrays.asList(field1, field2));` |
| createStructType | StructField[] fields | StructType | 创建结构类型(数组) | `DataTypes.createStructType(new StructField[]{f1, f2});` |
| createCharType | int length | CharType | 创建Char类型 | `DataTypes.createCharType(10);` |
| createVarcharType | int length | VarcharType | 创建Varchar类型 | `DataTypes.createVarcharType(255);` |
| createGeographyType | int srid | GeographyType | 创建地理类型 | `DataTypes.createGeographyType(4326);` |
| createGeographyType | String crs | GeographyType | 创建地理类型(CRS) | `DataTypes.createGeographyType("WGS84");` |
| createGeometryType | int srid | GeometryType | 创建几何类型 | `DataTypes.createGeometryType(4326);` |
| createGeometryType | String crs | GeometryType | 创建几何类型(CRS) | `DataTypes.createGeometryType("WGS84");` |

### RowFactory
**包路径**: `org.apache.spark.sql.RowFactory`
**类型**: Class (@Stable)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| create | Object... values | Row | 创建Row对象 | `RowFactory.create(1, "Alice", 100);` |

### SaveMode
**包路径**: `org.apache.spark.sql.SaveMode`
**类型**: Enum (@Stable)

| 枚举值 | 描述 | 示例 |
|--------|------|------|
| Append | 追加模式，如果数据已存在则追加 | `df.write().mode(SaveMode.Append).save(path);` |
| Overwrite | 覆盖模式，如果数据已存在则覆盖 | `df.write().mode(SaveMode.Overwrite).save(path);` |
| ErrorIfExists | 错误模式，如果数据已存在则抛异常 | `df.write().mode(SaveMode.ErrorIfExists).save(path);` |
| Ignore | 忽略模式，如果数据已存在则忽略 | `df.write().mode(SaveMode.Ignore).save(path);` |

### Trigger
**包路径**: `org.apache.spark.sql.streaming.Trigger`
**类型**: Class (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| ProcessingTime | long intervalMs | Trigger | 处理时间触发器(毫秒) | `Trigger.ProcessingTime(1000);` |
| ProcessingTime | long interval, TimeUnit timeUnit | Trigger | 处理时间触发器(时间单位) | `Trigger.ProcessingTime(10, TimeUnit.SECONDS);` |
| ProcessingTime | Duration interval | Trigger | 处理时间触发器(Duration) | `Trigger.ProcessingTime(Duration.apply(10));` |
| ProcessingTime | String interval | Trigger | 处理时间触发器(字符串) | `Trigger.ProcessingTime("10 seconds");` |
| Once | 无 | Trigger | 单次触发(已废弃) | `Trigger.Once();` |
| AvailableNow | 无 | Trigger | 立即处理所有可用数据 | `Trigger.AvailableNow();` |
| Continuous | long intervalMs | Trigger | 连续处理触发器(毫秒) | `Trigger.Continuous(1000);` |
| Continuous | long interval, TimeUnit timeUnit | Trigger | 连续处理触发器 | `Trigger.Continuous(10, TimeUnit.SECONDS);` |
| Continuous | Duration interval | Trigger | 连续处理触发器(Duration) | `Trigger.Continuous(Duration.apply(10));` |
| Continuous | String interval | Trigger | 连续处理触发器(字符串) | `Trigger.Continuous("10 seconds");` |
| RealTime | long batchDurationMs | Trigger | 实时模式触发器 | `Trigger.RealTime(1000);` |
| RealTime | long batchDuration, TimeUnit timeUnit | Trigger | 实时模式触发器 | `Trigger.RealTime(10, TimeUnit.SECONDS);` |
| RealTime | Duration batchDuration | Trigger | 实时模式触发器(Duration) | `Trigger.RealTime(Duration.apply(10));` |
| RealTime | String batchDuration | Trigger | 实时模式触发器(字符串) | `Trigger.RealTime("10 seconds");` |
| RealTime | 无 | Trigger | 实时模式触发器(默认5分钟) | `Trigger.RealTime();` |

### OutputMode
**包路径**: `org.apache.spark.sql.streaming.OutputMode`
**类型**: Class (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| Append | 无 | OutputMode | 仅输出新行 | `OutputMode.Append();` |
| Complete | 无 | OutputMode | 输出所有行 | `OutputMode.Complete();` |
| Update | 无 | OutputMode | 仅输出更新的行 | `OutputMode.Update();` |

### GroupStateTimeout
**包路径**: `org.apache.spark.sql.streaming.GroupStateTimeout`
**类型**: Class (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| ProcessingTimeTimeout | 无 | GroupStateTimeout | 处理时间超时 | `GroupStateTimeout.ProcessingTimeTimeout();` |
| EventTimeTimeout | 无 | GroupStateTimeout | 事件时间超时 | `GroupStateTimeout.EventTimeTimeout();` |
| NoTimeout | 无 | GroupStateTimeout | 无超时 | `GroupStateTimeout.NoTimeout();` |

---

## 五、UDF接口

所有UDF接口位于 `org.apache.spark.sql.api.java` 包下。

### UDF0<R>
**类型**: Interface (@Stable)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | 无 | R | 无参数UDF | `udf.call()` |

### UDF1<T1, R>
**类型**: Interface (@Stable)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 t1 | R | 1参数UDF | `udf.call(value)` |

### UDF2<T1, T2, R>
**类型**: Interface (@Stable)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 t1, T2 t2 | R | 2参数UDF | `udf.call(a, b)` |

### UDF3<T1, T2, T3, R>
**类型**: Interface (@Stable)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 t1, T2 t2, T3 t3 | R | 3参数UDF | `udf.call(a, b, c)` |

### UDF4<T1, T2, T3, T4, R>
**类型**: Interface (@Stable)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | T1 t1, T2 t2, T3 t3, T4 t4 | R | 4参数UDF | `udf.call(a, b, c, d)` |

### UDF5 ~ UDF22
类似模式，参数数量从5到22。

### UDF22<T1, T2, ..., T22, R>
**类型**: Interface (@Stable)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| call | 22个参数 | R | 22参数UDF | `udf.call(t1, t2, ..., t22)` |

**注册UDF示例**:
```java
// 注册UDF
spark.udf().register("myFunc", new UDF1<String, Integer>() {
    @Override
    public Integer call(String s) throws Exception {
        return s.length();
    }
}, DataTypes.IntegerType);

// 在SQL中使用
df.selectExpr("myFunc(name)").show();
```

---

## 六、SQL Connector Catalog API

### CatalogPlugin
**包路径**: `org.apache.spark.sql.connector.catalog.CatalogPlugin`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| initialize | String name, CaseInsensitiveStringMap options | void | 初始化Catalog | `catalog.initialize("my_catalog", options);` |
| name | 无 | String | 获取Catalog名称 | `String name = catalog.name();` |
| defaultNamespace | 无 | String[] | 获取默认命名空间 | `String[] ns = catalog.defaultNamespace();` |

### TableCatalog
**包路径**: `org.apache.spark.sql.connector.catalog.TableCatalog`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| listTables | String[] namespace | Identifier[] | 列出命名空间中的表 | `Identifier[] tables = catalog.listTables(new String[]{"db"});` |
| loadTable | Identifier ident | Table | 加载表元数据 | `Table table = catalog.loadTable(Identifier.of(new String[]{"db"}, "table"));` |
| createTable | Identifier ident, TableInfo tableInfo | Table | 创建表 | `catalog.createTable(ident, tableInfo);` |
| alterTable | Identifier ident, TableChange... changes | Table | 修改表 | `catalog.alterTable(ident, changes);` |
| dropTable | Identifier ident | boolean | 删除表 | `boolean dropped = catalog.dropTable(ident);` |
| renameTable | Identifier oldIdent, Identifier newIdent | void | 重命名表 | `catalog.renameTable(oldIdent, newIdent);` |
| tableExists | Identifier ident | boolean | 判断表是否存在 | `boolean exists = catalog.tableExists(ident);` |
| purgeTable | Identifier ident | boolean | 清除表 | `catalog.purgeTable(ident);` |

### Table
**包路径**: `org.apache.spark.sql.connector.catalog.Table`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| name | 无 | String | 获取表名称 | `String name = table.name();` |
| id | 无 | String | 获取表ID | `String id = table.id();` |
| columns | 无 | Column[] | 获取列定义 | `Column[] cols = table.columns();` |
| partitioning | 无 | Transform[] | 获取分区信息 | `Transform[] parts = table.partitioning();` |
| properties | 无 | Map<String, String> | 获取表属性 | `Map<String, String> props = table.properties();` |
| capabilities | 无 | Set<TableCapability> | 获取表能力 | `Set<TableCapability> caps = table.capabilities();` |

### Identifier
**包路径**: `org.apache.spark.sql.connector.catalog.Identifier`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| of | String[] namespace, String name | Identifier | 创建标识符 | `Identifier.of(new String[]{"db"}, "table");` |
| namespace | 无 | String[] | 获取命名空间 | `String[] ns = ident.namespace();` |
| name | 无 | String | 获取名称 | `String n = ident.name();` |

### SupportsRead
**包路径**: `org.apache.spark.sql.connector.catalog.SupportsRead`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| newScanBuilder | CaseInsensitiveStringMap options | ScanBuilder | 创建扫描构建器 | `ScanBuilder builder = table.newScanBuilder(options);` |

### SupportsWrite
**包路径**: `org.apache.spark.sql.connector.catalog.SupportsWrite`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| newWriteBuilder | LogicalWriteInfo info | WriteBuilder | 创建写入构建器 | `WriteBuilder builder = table.newWriteBuilder(info);` |

### SupportsNamespaces
**包路径**: `org.apache.spark.sql.connector.catalog.SupportsNamespaces`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| listNamespaces | 无 | String[][] | 列出顶层命名空间 | `String[][] ns = catalog.listNamespaces();` |
| listNamespaces | String[] namespace | String[][] | 列出子命名空间 | `String[][] ns = catalog.listNamespaces(new String[]{"db"});` |
| namespaceExists | String[] namespace | boolean | 判断命名空间是否存在 | `boolean exists = catalog.namespaceExists(ns);` |
| loadNamespaceMetadata | String[] namespace | Map<String, String> | 加载命名空间元数据 | `Map<String, String> meta = catalog.loadNamespaceMetadata(ns);` |
| createNamespace | String[] namespace, Map<String, String> metadata | void | 创建命名空间 | `catalog.createNamespace(ns, metadata);` |
| alterNamespace | String[] namespace, NamespaceChange... changes | void | 修改命名空间 | `catalog.alterNamespace(ns, changes);` |
| dropNamespace | String[] namespace, boolean cascade | boolean | 删除命名空间 | `catalog.dropNamespace(ns, true);` |

---

## 七、SQL Connector Read API

### Scan
**包路径**: `org.apache.spark.sql.connector.read.Scan`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| readSchema | 无 | StructType | 获取读取schema | `StructType schema = scan.readSchema();` |
| description | 无 | String | 获取描述 | `String desc = scan.description();` |
| toBatch | 无 | Batch | 转换为批处理扫描 | `Batch batch = scan.toBatch();` |
| toMicroBatchStream | String checkpointLocation | MicroBatchStream | 转换为微批流 | `MicroBatchStream stream = scan.toMicroBatchStream(cp);` |
| toContinuousStream | String checkpointLocation | ContinuousStream | 转换为连续流 | `ContinuousStream stream = scan.toContinuousStream(cp);` |

### Batch
**包路径**: `org.apache.spark.sql.connector.read.Batch`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| planInputPartitions | 无 | InputPartition[] | 规划输入分区 | `InputPartition[] parts = batch.planInputPartitions();` |
| createReaderFactory | 无 | PartitionReaderFactory | 创建读取器工厂 | `PartitionReaderFactory factory = batch.createReaderFactory();` |

### InputPartition
**包路径**: `org.apache.spark.sql.connector.read.InputPartition`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| preferredLocations | 无 | String[] | 获取偏好位置 | `String[] locs = partition.preferredLocations();` |

### PartitionReader<T>
**包路径**: `org.apache.spark.sql.connector.read.PartitionReader`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| next | 无 | boolean | 是否有下一个记录 | `while (reader.next()) { ... }` |
| get | 无 | T | 获取当前记录 | `T record = reader.get();` |
| currentMetricsValues | 无 | CustomTaskMetric[] | 获取当前指标值 | `CustomTaskMetric[] metrics = reader.currentMetricsValues();` |

---

## 八、SQL Connector Write API

### WriteBuilder
**包路径**: `org.apache.spark.sql.connector.write.WriteBuilder`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| build | 无 | Write | 构建Write对象 | `Write write = builder.build();` |

### BatchWrite
**包路径**: `org.apache.spark.sql.connector.write.BatchWrite`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| createBatchWriterFactory | PhysicalWriteInfo info | DataWriterFactory | 创建写入器工厂 | `DataWriterFactory factory = batchWrite.createBatchWriterFactory(info);` |
| useCommitCoordinator | 无 | boolean | 是否使用提交协调器 | `boolean use = batchWrite.useCommitCoordinator();` |
| onDataWriterCommit | WriterCommitMessage message | void | 处理写入器提交 | `batchWrite.onDataWriterCommit(msg);` |
| commit | WriterCommitMessage[] messages | void | 提交写入作业 | `batchWrite.commit(messages);` |
| abort | WriterCommitMessage[] messages | void | 中止写入作业 | `batchWrite.abort(messages);` |

### DataWriter<T>
**包路径**: `org.apache.spark.sql.connector.write.DataWriter`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| write | T record | void | 写入记录 | `writer.write(record);` |
| commit | 无 | WriterCommitMessage | 提交写入 | `WriterCommitMessage msg = writer.commit();` |
| abort | 无 | void | 中止写入 | `writer.abort();` |
| currentMetricsValues | 无 | CustomTaskMetric[] | 获取当前指标 | `CustomTaskMetric[] metrics = writer.currentMetricsValues();` |

---

## 九、SQL Connector Expressions API

### Expression
**包路径**: `org.apache.spark.sql.connector.expressions.Expression`
**类型**: Interface (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| describe | 无 | String | 描述表达式 | `String desc = expr.describe();` |
| children | 无 | Expression[] | 获取子表达式 | `Expression[] children = expr.children();` |
| references | 无 | NamedReference[] | 获取引用字段 | `NamedReference[] refs = expr.references();` |

---

## 十、SQL Vectorized API

### ColumnVector
**包路径**: `org.apache.spark.sql.vectorized.ColumnVector`
**类型**: Abstract Class (@Evolving)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| dataType | 无 | DataType | 获取数据类型 | `DataType type = col.dataType();` |
| close | 无 | void | 关闭释放资源 | `col.close();` |
| hasNull | 无 | boolean | 是否有null值 | `boolean has = col.hasNull();` |
| numNulls | 无 | int | null值数量 | `int n = col.numNulls();` |
| isNullAt | int rowId | boolean | 指定位置是否为null | `boolean isNull = col.isNullAt(0);` |
| getBoolean | int rowId | boolean | 获取布尔值 | `boolean b = col.getBoolean(0);` |
| getByte | int rowId | byte | 获取字节值 | `byte b = col.getByte(0);` |
| getShort | int rowId | short | 获取短整型值 | `short s = col.getShort(0);` |
| getInt | int rowId | int | 获取整型值 | `int i = col.getInt(0);` |
| getLong | int rowId | long | 获取长整型值 | `long l = col.getLong(0);` |
| getFloat | int rowId | float | 获取浮点值 | `float f = col.getFloat(0);` |
| getDouble | int rowId | double | 获取双精度值 | `double d = col.getDouble(0);` |
| getStruct | int rowId | ColumnarRow | 获取结构体 | `ColumnarRow row = col.getStruct(0);` |
| getArray | int rowId | ColumnarArray | 获取数组 | `ColumnarArray arr = col.getArray(0);` |
| getMap | int ordinal | ColumnarMap | 获取Map | `ColumnarMap map = col.getMap(0);` |
| getDecimal | int rowId, int precision, int scale | Decimal | 获取Decimal | `Decimal dec = col.getDecimal(0, 10, 2);` |
| getUTF8String | int rowId | UTF8String | 获取字符串 | `UTF8String s = col.getUTF8String(0);` |
| getBinary | int rowId | byte[] | 获取二进制数据 | `byte[] bytes = col.getBinary(0);` |
| getChild | int ordinal | ColumnVector | 获取子列 | `ColumnVector child = col.getChild(0);` |

### ColumnarBatch
**包路径**: `org.apache.spark.sql.vectorized.ColumnarBatch`
**类型**: Class (@DeveloperApi)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| close | 无 | void | 关闭批次 | `batch.close();` |
| rowIterator | 无 | Iterator<InternalRow> | 行迭代器 | `Iterator<InternalRow> iter = batch.rowIterator();` |
| setNumRows | int numRows | void | 设置行数 | `batch.setNumRows(1000);` |
| numCols | 无 | int | 列数量 | `int cols = batch.numCols();` |
| numRows | 无 | int | 行数量 | `int rows = batch.numRows();` |
| column | int ordinal | ColumnVector | 获取指定列 | `ColumnVector col = batch.column(0);` |
| getRow | int rowId | InternalRow | 获取指定行 | `InternalRow row = batch.getRow(0);` |

---

## 十一、Streaming State API

### TimeMode
**包路径**: `org.apache.spark.sql.streaming.TimeMode`
**类型**: Class

定义时间模式的常量类。

---

## 十二、GraphX Java API

### TripletFields
**包路径**: `org.apache.spark.graphx.TripletFields`
**类型**: Class

| 字段 | 类型 | 描述 | 示例 |
|------|------|------|------|
| useSrc | boolean | 是否使用源顶点属性 | `fields.useSrc` |
| useDst | boolean | 是否使用目标顶点属性 | `fields.useDst` |
| useEdge | boolean | 是否使用边属性 | `fields.useEdge` |

| 静态字段 | 描述 | 示例 |
|----------|------|------|
| None | 不暴露任何字段 | `TripletFields.None` |
| EdgeOnly | 仅暴露边字段 | `TripletFields.EdgeOnly` |
| Src | 暴露源和边字段 | `TripletFields.Src` |
| Dst | 暴露目标和边字段 | `TripletFields.Dst` |
| All | 暴露所有字段 | `TripletFields.All` |

| 构造方法 | 参数 | 描述 | 示例 |
|----------|------|------|------|
| TripletFields | 无 | 默认构造(包含所有字段) | `new TripletFields();` |
| TripletFields | boolean useSrc, boolean useDst, boolean useEdge | 自定义构造 | `new TripletFields(true, false, true);` |

---

## 十三、Shuffle API

### ShuffleDataIO
**包路径**: `org.apache.spark.shuffle.api.ShuffleDataIO`
**类型**: Interface (@Private)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| executor | 无 | ShuffleExecutorComponents | 获取Executor组件 | `ShuffleExecutorComponents exec = io.executor();` |
| driver | 无 | ShuffleDriverComponents | 获取Driver组件 | `ShuffleDriverComponents driver = io.driver();` |

### ShuffleExecutorComponents
**包路径**: `org.apache.spark.shuffle.api.ShuffleExecutorComponents`
**类型**: Interface (@Private)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| initializeExecutor | String appId, String execId, Map<String, String> extraConfigs | void | 初始化Executor | `components.initializeExecutor(appId, execId, configs);` |
| createMapOutputWriter | int shuffleId, long mapTaskId, int numPartitions | ShuffleMapOutputWriter | 创建输出写入器 | `ShuffleMapOutputWriter writer = components.createMapOutputWriter(shuffleId, taskId, numParts);` |

### ShuffleDriverComponents
**包路径**: `org.apache.spark.shuffle.api.ShuffleDriverComponents`
**类型**: Interface (@Private)

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| initializeApplication | 无 | Map<String, String> | 初始化应用 | `Map<String, String> configs = driver.initializeApplication();` |
| cleanupApplication | 无 | void | 清理应用 | `driver.cleanupApplication();` |
| registerShuffle | int shuffleId | void | 注册Shuffle | `driver.registerShuffle(shuffleId);` |
| removeShuffle | int shuffleId, boolean blocking | void | 移除Shuffle | `driver.removeShuffle(shuffleId, true);` |
| supportsReliableStorage | 无 | boolean | 是否支持可靠存储 | `boolean supports = driver.supportsReliableStorage();` |

---

## 十四、Memory API

### MemoryMode
**包路径**: `org.apache.spark.memory.MemoryMode`
**类型**: Enum (@Private)

| 枚举值 | 描述 | 示例 |
|--------|------|------|
| ON_HEAP | 堆内存 | `MemoryMode.ON_HEAP` |
| OFF_HEAP | 堆外内存 | `MemoryMode.OFF_HEAP` |

---

## 十五、Launcher API

### SparkLauncher
**包路径**: `org.apache.spark.launcher.SparkLauncher`
**类型**: Class

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| setJavaHome | String javaHome | SparkLauncher | 设置JAVA_HOME | `launcher.setJavaHome("/path/to/java");` |
| setSparkHome | String sparkHome | SparkLauncher | 设置SPARK_HOME | `launcher.setSparkHome("/path/to/spark");` |
| directory | File dir | SparkLauncher | 设置工作目录 | `launcher.directory(new File("/work"));` |
| setPropertiesFile | String path | SparkLauncher | 设置属性文件 | `launcher.setPropertiesFile("spark.properties");` |
| setConf | String key, String value | SparkLauncher | 设置配置 | `launcher.setConf("spark.executor.memory", "4g");` |
| setAppName | String appName | SparkLauncher | 设置应用名 | `launcher.setAppName("MyApp");` |
| setMaster | String master | SparkLauncher | 设置Master | `launcher.setMaster("local[*]");` |
| setDeployMode | String mode | SparkLauncher | 设置部署模式 | `launcher.setDeployMode("cluster");` |
| setAppResource | String resource | SparkLauncher | 设置应用资源 | `launcher.setAppResource("myapp.jar");` |
| setMainClass | String mainClass | SparkLauncher | 设置主类 | `launcher.setMainClass("com.example.Main");` |
| addSparkArg | String arg | SparkLauncher | 添加Spark参数 | `launcher.addSparkArg("--verbose");` |
| addAppArgs | String... args | SparkLauncher | 添加应用参数 | `launcher.addAppArgs("arg1", "arg2");` |
| addJar | String jar | SparkLauncher | 添加Jar | `launcher.addJar("extra.jar");` |
| addFile | String file | SparkLauncher | 添加文件 | `launcher.addFile("data.txt");` |
| launch | 无 | Process | 启动子进程 | `Process p = launcher.launch();` |
| startApplication | SparkAppHandle.Listener... listeners | SparkAppHandle | 启动应用 | `SparkAppHandle handle = launcher.startApplication();` |

**完整示例**:
```java
SparkLauncher launcher = new SparkLauncher()
    .setAppResource("my-app.jar")
    .setMainClass("com.example.MySparkApp")
    .setMaster("spark://host:7077")
    .setDeployMode("cluster")
    .setAppName("My Application")
    .setConf("spark.executor.memory", "4g")
    .setConf("spark.executor.cores", "2");

SparkAppHandle handle = launcher.startApplication();
// 等待应用完成
while (!handle.getState().isFinal()) {
    Thread.sleep(1000);
}
```

### SparkAppHandle
**包路径**: `org.apache.spark.launcher.SparkAppHandle`
**类型**: Interface

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| addListener | Listener l | void | 添加监听器 | `handle.addListener(listener);` |
| getState | 无 | State | 获取应用状态 | `State state = handle.getState();` |
| getAppId | 无 | String | 获取应用ID | `String appId = handle.getAppId();` |
| stop | 无 | void | 停止应用 | `handle.stop();` |
| kill | 无 | void | 杀死应用 | `handle.kill();` |
| disconnect | 无 | void | 断开连接 | `handle.disconnect();` |
| getError | 无 | Optional<Throwable> | 获取错误 | `Optional<Throwable> err = handle.getError();` |

### SparkAppHandle.State
**类型**: Enum

| 枚举值 | 描述 | 是否最终状态 |
|--------|------|--------------|
| UNKNOWN | 应用未报告 | 否 |
| CONNECTED | 应用已连接 | 否 |
| SUBMITTED | 应用已提交 | 否 |
| RUNNING | 应用正在运行 | 否 |
| FINISHED | 应用成功完成 | 是 |
| FAILED | 应用失败 | 是 |
| KILLED | 应用被杀死 | 是 |
| LOST | JVM退出状态未知 | 是 |

### InProcessLauncher
**包路径**: `org.apache.spark.launcher.InProcessLauncher`
**类型**: Class

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| startApplication | SparkAppHandle.Listener... listeners | SparkAppHandle | 在同一进程启动应用 | `SparkAppHandle handle = launcher.startApplication();` |

---

## 十六、Unsafe Types API

### UTF8String
**包路径**: `org.apache.spark.unsafe.types.UTF8String`
**类型**: Final Class

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| fromBytes | byte[] bytes | UTF8String | 从字节创建 | `UTF8String.fromString("hello");` |
| fromBytes | byte[] bytes, int offset, int numBytes | UTF8String | 从字节子数组创建 | `UTF8String.fromBytes(bytes, 0, 10);` |
| fromString | String str | UTF8String | 从String创建 | `UTF8String.fromString("hello");` |
| fromAddress | Object base, long offset, int numBytes | UTF8String | 从内存地址创建 | `UTF8String.fromAddress(base, offset, len);` |
| numBytes | 无 | int | 字节数量 | `int n = s.numBytes();` |
| numChars | 无 | int | 字符数量 | `int n = s.numChars();` |
| toString | 无 | String | 转为String | `String str = s.toString();` |
| getBytes | 无 | byte[] | 获取字节 | `byte[] bytes = s.getBytes();` |
| substring | int start, int until | UTF8String | 截取子串 | `UTF8String sub = s.substring(0, 5);` |
| contains | UTF8String substring | boolean | 是否包含 | `boolean has = s.contains(sub);` |
| startsWith | UTF8String prefix | boolean | 是否以指定开头 | `boolean starts = s.startsWith(prefix);` |
| endsWith | UTF8String suffix | boolean | 是否以指定结尾 | `boolean ends = s.endsWith(suffix);` |
| toUpperCase | 无 | UTF8String | 转大写 | `UTF8String upper = s.toUpperCase();` |
| toLowerCase | 无 | UTF8String | 转小写 | `UTF8String lower = s.toLowerCase();` |
| trim | 无 | UTF8String | 去空格 | `UTF8String trimmed = s.trim();` |
| reverse | 无 | UTF8String | 反转 | `UTF8String rev = s.reverse();` |
| repeat | int times | UTF8String | 重复 | `UTF8String rep = s.repeat(3);` |
| split | UTF8String pattern, int limit | UTF8String[] | 分割 | `UTF8String[] parts = s.split(pattern, -1);` |
| concat | UTF8String... inputs | UTF8String | 连接 | `UTF8String result = UTF8String.concat(a, b, c);` |
| isValid | 无 | boolean | 是否有效UTF8 | `boolean valid = s.isValid();` |
| makeValid | 无 | UTF8String | 使UTF8有效 | `UTF8String valid = s.makeValid();` |

### CalendarInterval
**包路径**: `org.apache.spark.unsafe.types.CalendarInterval`
**类型**: Class

表示日历间隔的数据类型。

### ByteArray
**包路径**: `org.apache.spark.unsafe.types.ByteArray`
**类型**: Class

字节数组操作工具类。

### VariantVal
**包路径**: `org.apache.spark.unsafe.types.VariantVal`
**类型**: Class

Variant值类型。

### GeographyVal
**包路径**: `org.apache.spark.unsafe.types.GeographyVal`
**类型**: Class

地理值类型。

### GeometryVal
**包路径**: `org.apache.spark.unsafe.types.GeometryVal`
**类型**: Class

几何值类型。

---

## 十七、Variant Types API

### Variant
**包路径**: `org.apache.spark.types.variant.Variant`
**类型**: Class

### VariantSchema
**包路径**: `org.apache.spark.types.variant.VariantSchema`
**类型**: Class

### VariantBuilder
**包路径**: `org.apache.spark.types.variant.VariantBuilder`
**类型**: Class

### VariantUtil
**包路径**: `org.apache.spark.types.variant.VariantUtil`
**类型**: Class

### VariantSizeLimitException
**包路径**: `org.apache.spark.types.variant.VariantSizeLimitException`
**类型**: Class

### ShreddingUtils
**包路径**: `org.apache.spark.types.variant.ShreddingUtils`
**类型**: Class

---

## 十八、Annotation API

所有注解位于 `org.apache.spark.annotation` 包下。

### @Stable
**类型**: Annotation

表示稳定的API，在主要版本内保持源码和二进制兼容。

### @Unstable
**类型**: Annotation

表示不稳定的API，可能会更改。

### @Evolving
**类型**: Annotation

表示正在演进的API，可能会在小版本内更改。

### @Experimental
**类型**: Annotation

表示实验性API，不保证向后兼容。

### @DeveloperApi
**类型**: Annotation

表示开发者API，用于扩展Spark。

### @Private
**类型**: Annotation

表示私有API，仅供Spark内部使用。

### @AlphaComponent
**类型**: Annotation

表示Alpha组件，尚未稳定。

### @ClassicOnly
**类型**: Annotation

表示仅在经典模式下可用的API。

---

## 十九、其他Utility API

### Pair<A, B>
**包路径**: `org.apache.spark.util.Pair`
**类型**: Class

通用键值对类。

### UUIDv7Generator
**包路径**: `org.apache.spark.util.UUIDv7Generator`
**类型**: Class

UUID v7生成器。

### ByteUnit
**包路径**: `org.apache.spark.network.util.ByteUnit`
**类型**: Enum

字节单位枚举。

### QueryContext
**包路径**: `org.apache.spark.QueryContext`
**类型**: Interface

查询上下文。

### QueryContextType
**包路径**: `org.apache.spark.QueryContextType`
**类型**: Enum

查询上下文类型。

---

## 二十、Status API

### TaskStatus
**包路径**: `org.apache.spark.status.api.v1.TaskStatus`
**类型**: Enum

---

## 二十一、Parquet/ORC API

### ParquetCompressionCodec
**包路径**: `org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodec`
**类型**: Enum

### OrcCompressionCodec
**包路径**: `org.apache.spark.sql.execution.datasources.orc.OrcCompressionCodec`
**类型**: Enum

### AvroCompressionCodec
**包路径**: `org.apache.spark.sql.avro.AvroCompressionCodec`
**类型**: Enum

---

## 二十二、Hive ThriftServer API

### Service
**包路径**: `org.apache.hive.service.Service`
**类型**: Interface

### HiveServer2
**包路径**: `org.apache.hive.service.server.HiveServer2`
**类型**: Class

### CLIService
**包路径**: `org.apache.hive.service.cli.CLIService`
**类型**: Class

### OperationHandle
**包路径**: `org.apache.hive.service.cli.OperationHandle`
**类型**: Class

### SessionHandle
**包路径**: `org.apache.hive.service.cli.SessionHandle`
**类型**: Class

---

## 附录：完整类列表

### 函数接口 (org.apache.spark.api.java.function) - 22个
- Function
- Function0
- Function2
- Function3
- Function4
- FilterFunction
- MapFunction
- FlatMapFunction
- ReduceFunction
- ForeachFunction
- VoidFunction
- VoidFunction2
- PairFunction
- PairFlatMapFunction
- DoubleFunction
- DoubleFlatMapFunction
- MapPartitionsFunction
- ForeachPartitionFunction
- MapGroupsFunction
- FlatMapGroupsFunction
- CoGroupFunction
- FlatMapFunction2

### SQL函数接口 (org.apache.spark.api.java.function) - 2个
- MapGroupsWithStateFunction
- FlatMapGroupsWithStateFunction

### UDF接口 (org.apache.spark.sql.api.java) - 23个
- UDF0
- UDF1 ~ UDF22

### SQL Streaming (org.apache.spark.sql.streaming) - 4个
- Trigger
- OutputMode
- GroupStateTimeout
- TimeMode

### SQL Types (org.apache.spark.sql.types) - 1个
- DataTypes

### SQL Connector Catalog - 约60个接口/类
- CatalogPlugin
- TableCatalog
- Table
- Identifier
- SupportsRead
- SupportsWrite
- SupportsNamespaces
- SupportsDelete
- SupportsDeleteV2
- SupportsPartitionManagement
- SupportsAtomicPartitionManagement
- SupportsMetadataColumns
- FunctionCatalog
- ViewCatalog
- StagedTable
- StagingTableCatalog
- TableChange
- NamespaceChange
- Column
- MetadataColumn
- TableCapability
- TableCatalogCapability
- TableProvider
- Changelog
- ChangelogInfo
- ChangelogRange
- 以及更多...

### SQL Connector Read - 约40个接口/类
- Scan
- Batch
- ScanBuilder
- InputPartition
- PartitionReader
- PartitionReaderFactory
- Statistics
- SupportsPushDownAggregates
- SupportsPushDownFilters
- SupportsPushDownLimit
- SupportsPushDownOffset
- SupportsPushDownTopN
- SupportsReportOrdering
- SupportsReportPartitioning
- SupportsReportStatistics
- 以及更多...

### SQL Connector Write - 约30个接口/类
- Write
- WriteBuilder
- BatchWrite
- DataWriter
- DataWriterFactory
- WriterCommitMessage
- SupportsOverwrite
- SupportsTruncate
- SupportsDynamicOverwrite
- DeltaWrite
- DeltaWriter
- RowLevelOperation
- 以及更多...

### SQL Connector Expressions - 约30个接口/类
- Expression
- Literal
- NamedReference
- Transform
- SortOrder
- Cast
- Expressions
- AggregateFunc
- And/Or/Not
- 以及更多...

### Vectorized (org.apache.spark.sql.vectorized) - 8个
- ColumnVector
- ColumnarBatch
- ColumnarArray
- ColumnarMap
- ColumnarRow
- ColumnarBatchRow
- ArrowColumnVector
- Dictionary

### Unsafe Types - 7个
- UTF8String
- CalendarInterval
- ByteArray
- VariantVal
- GeographyVal
- GeometryVal

### Launcher - 10个
- SparkLauncher
- SparkAppHandle
- InProcessLauncher
- AbstractLauncher
- AbstractAppHandle
- ChildProcAppHandle
- InProcessAppHandle
- LauncherServer
- LauncherProtocol
- CommandBuilderUtils

### Shuffle API - 约15个接口/类
- ShuffleDataIO
- ShuffleExecutorComponents
- ShuffleDriverComponents
- ShuffleMapOutputWriter
- ShufflePartitionWriter
- WritableByteChannelWrapper
- WriterCommitMessage
- SingleSpillShuffleMapOutputWriter
- MapOutputMetadata
- 以及更多...

### Annotations - 8个
- Stable
- Unstable
- Evolving
- Experimental
- DeveloperApi
- Private
- AlphaComponent
- ClassicOnly

### Core - 约50个类
- SparkJobInfo
- JobExecutionStatus
- UnsafeExternalSorter
- UnsafeInMemorySorter
- PrefixComparators
- RadixSort
- TimSort
- 以及更多...

### Streaming - 约10个
- StreamingContextState
- WriteAheadLog
- WriteAheadLogRecordHandle
- BatchStatus

### GraphX - 2个
- TripletFields
- EdgeActiveness

---

## 统计信息

| 类别 | 数量 |
|------|------|
| 函数接口 | 22 |
| SQL函数接口 | 2 |
| UDF接口 | 23 |
| SQL Connector Catalog | ~60 |
| SQL Connector Read | ~40 |
| SQL Connector Write | ~30 |
| SQL Connector Expressions | ~30 |
| Vectorized | 8 |
| Unsafe Types | 7 |
| Launcher | 10 |
| Shuffle API | ~15 |
| Annotations | 8 |
| Streaming | ~10 |
| GraphX | 2 |
| **总计** | **约260+ public类/接口/枚举** |

---

*文档生成日期: 2026-05-09*
*基于Apache Spark源码生成*
