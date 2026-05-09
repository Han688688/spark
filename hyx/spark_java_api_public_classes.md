# Apache Spark Java API Complete Documentation with Examples

本文档列出了Spark Java API的所有public类、接口和枚举，包含方法签名、描述和使用示例。

---

## 一、Core Java API

### 1. StorageLevels (公共类)
**路径**: `org.apache.spark.api.java.StorageLevels`

**描述**: 提供常用的存储级别常量。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| NONE | 无 | StorageLevel | 不存储数据 | `StorageLevels.NONE` |
| DISK_ONLY | 无 | StorageLevel | 仅存储到磁盘 | `rdd.persist(StorageLevels.DISK_ONLY)` |
| DISK_ONLY_2 | 无 | StorageLevel | 仅磁盘，副本数为2 | `rdd.persist(StorageLevels.DISK_ONLY_2)` |
| DISK_ONLY_3 | 无 | StorageLevel | 仅磁盘，副本数为3 | `rdd.persist(StorageLevels.DISK_ONLY_3)` |
| MEMORY_ONLY | 无 | StorageLevel | 仅存储到内存 | `rdd.persist(StorageLevels.MEMORY_ONLY)` |
| MEMORY_ONLY_2 | 无 | StorageLevel | 仅内存，副本数为2 | `rdd.persist(StorageLevels.MEMORY_ONLY_2)` |
| MEMORY_ONLY_SER | 无 | StorageLevel | 仅内存，序列化格式 | `rdd.persist(StorageLevels.MEMORY_ONLY_SER)` |
| MEMORY_ONLY_SER_2 | 无 | StorageLevel | 仅内存序列化，副本数2 | `rdd.persist(StorageLevels.MEMORY_ONLY_SER_2)` |
| MEMORY_AND_DISK | 无 | StorageLevel | 内存和磁盘混合存储 | `rdd.persist(StorageLevels.MEMORY_AND_DISK)` |
| MEMORY_AND_DISK_2 | 无 | StorageLevel | 内存磁盘混合，副本数2 | `rdd.persist(StorageLevels.MEMORY_AND_DISK_2)` |
| MEMORY_AND_DISK_SER | 无 | StorageLevel | 内存磁盘混合，序列化 | `rdd.persist(StorageLevels.MEMORY_AND_DISK_SER)` |
| MEMORY_AND_DISK_SER_2 | 无 | StorageLevel | 内存磁盘序列化，副本数2 | `rdd.persist(StorageLevels.MEMORY_AND_DISK_SER_2)` |
| OFF_HEAP | 无 | StorageLevel | 堆外内存存储 | `rdd.persist(StorageLevels.OFF_HEAP)` |
| create | boolean useDisk, boolean useMemory, boolean useOffHeap, boolean deserialized, int replication | StorageLevel | 创建自定义存储级别 | `StorageLevels.create(true, true, false, false, 1)` |

**完整示例**:
```java
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.StorageLevels;
import org.apache.spark.SparkConf;

SparkConf conf = new SparkConf().setAppName("StorageLevelsExample");
JavaSparkContext sc = new JavaSparkContext(conf);

JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

// 使用预定义的存储级别
rdd.persist(StorageLevels.MEMORY_ONLY);

// 创建自定义存储级别
StorageLevel customLevel = StorageLevels.create(true, true, false, true, 2);
rdd.persist(customLevel);
```

---

### 2. Optional<T> (公共类)
**路径**: `org.apache.spark.api.java.Optional`

**描述**: 表示可能存在或不存在的值，类似于Java 8的Optional和Guava的Optional。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| empty | 无 | Optional<T> | 创建空的Optional | `Optional.empty()` |
| of | T value | Optional<T> | 创建包含值的Optional（值不能为null） | `Optional.of("hello")` |
| ofNullable | T value | Optional<T> | 创建Optional，值可以为null | `Optional.ofNullable(maybeNull)` |
| get | 无 | T | 获取值，如果为空则抛异常 | `opt.get()` |
| orElse | T other | T | 如果为空则返回other | `opt.orElse("default")` |
| isPresent | 无 | boolean | 检查是否有值 | `if (opt.isPresent()) { ... }` |
| absent | 无 | Optional<T> | 创建空的Optional（Guava风格） | `Optional.absent()` |
| fromNullable | T value | Optional<T> | 从可能为null的值创建（Guava风格） | `Optional.fromNullable(value)` |
| or | T other | T | 如果为空则返回other（Guava风格） | `opt.or("default")` |
| orNull | 无 | T | 如果为空则返回null | `opt.orNull()` |
| equals | Object obj | boolean | 比较两个Optional是否相等 | `opt1.equals(opt2)` |
| hashCode | 无 | int | 返回hashCode | `int hash = opt.hashCode()` |
| toString | 无 | String | 返回字符串表示 | `String str = opt.toString()` |

**完整示例**:
```java
import org.apache.spark.api.java.Optional;

// 创建Optional
Optional<String> present = Optional.of("hello");
Optional<String> empty = Optional.empty();
Optional<String> nullable = Optional.ofNullable(null);

// 检查和使用值
if (present.isPresent()) {
    String value = present.get();
    System.out.println(value); // 输出: hello
}

// 使用orElse提供默认值
String result = empty.orElse("default");
System.out.println(result); // 输出: default

// 转换为null
String nullableResult = nullable.orNull(); // 返回null
```

---

### 3. JavaFutureAction<T> (公共接口)
**路径**: `org.apache.spark.api.java.JavaFutureAction`

**描述**: 表示异步操作的Future，继承自java.util.concurrent.Future。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| jobIds | 无 | List<Integer> | 返回底层异步操作运行的作业ID列表 | `List<Integer> ids = future.jobIds()` |

**继承自Future的方法**:
| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| cancel | boolean mayInterruptIfRunning | boolean | 取消任务 | `future.cancel(true)` |
| isCancelled | 无 | boolean | 检查是否已取消 | `boolean cancelled = future.isCancelled()` |
| isDone | 无 | boolean | 检查是否已完成 | `boolean done = future.isDone()` |
| get | 无 | T | 等待并获取结果 | `T result = future.get()` |
| get | long timeout, TimeUnit unit | T | 等待指定时间获取结果 | `T result = future.get(10, TimeUnit.SECONDS)` |

**完整示例**:
```java
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import java.util.concurrent.TimeUnit;

JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

// 异步收集数据
JavaFutureAction<List<Integer>> future = rdd.collectAsync();

// 检查作业ID
List<Integer> jobIds = future.jobIds();
System.out.println("Job IDs: " + jobIds);

// 等待结果
if (!future.isDone()) {
    List<Integer> result = future.get(30, TimeUnit.SECONDS);
    System.out.println("Result: " + result);
}
```

---

## 二、Common Java API - Function Interfaces

### 1. Function<T1, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.Function`

**描述**: 基础函数接口，接受一个参数并返回一个结果。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T1 v1 | R | 执行函数逻辑 | `function.call(input)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.Function;

JavaRDD<String> rdd = sc.parallelize(Arrays.asList("hello", "world"));

// 使用Lambda
JavaRDD<Integer> lengths = rdd.map(s -> s.length());

// 使用匿名类
JavaRDD<String> uppercased = rdd.map(new Function<String, String>() {
    @Override
    public String call(String s) {
        return s.toUpperCase();
    }
});
```

---

### 2. FilterFunction<T> (函数接口)
**路径**: `org.apache.spark.api.java.function.FilterFunction`

**描述**: 用于过滤的函数接口，返回布尔值。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T value | boolean | 判断是否保留该元素 | `filter.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

Dataset<Row> ds = spark.read().json("data.json");

// 使用Lambda
Dataset<Row> filtered = ds.filter((FilterFunction<Row>) row -> row.getInt("age") > 18);

// 使用匿名类
Dataset<Row> validData = ds.filter(new FilterFunction<Row>() {
    @Override
    public boolean call(Row row) {
        return !row.isNullAt("id");
    }
});
```

---

### 3. MapFunction<T, U> (函数接口)
**路径**: `org.apache.spark.api.java.function.MapFunction`

**描述**: Dataset的map操作使用的函数接口。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T value | U | 将输入转换为输出 | `mapFunc.call(input)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

Dataset<String> names = spark.createDataset(Arrays.asList("Alice", "Bob"), Encoders.STRING());

// 使用Lambda
Dataset<Integer> nameLengths = names.map((MapFunction<String, Integer>) s -> s.length(), Encoders.INT());

// 使用匿名类
Dataset<String> greetings = names.map(new MapFunction<String, String>() {
    @Override
    public String call(String name) {
        return "Hello, " + name;
    }
}, Encoders.STRING());
```

---

### 4. FlatMapFunction<T, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.FlatMapFunction`

**描述**: 一对多映射函数，返回Iterator。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T t | Iterator<R> | 将一个元素映射为多个元素 | `flatMap.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.FlatMapFunction;
import java.util.Iterator;
import java.util.Arrays;

JavaRDD<String> sentences = sc.parallelize(Arrays.asList("Hello World", "Apache Spark"));

// 使用Lambda - 将句子拆分为单词
JavaRDD<String> words = sentences.flatMap(s -> Arrays.asList(s.split(" ")).iterator());

// 使用匿名类
JavaRDD<String> letters = sentences.flatMap(new FlatMapFunction<String, String>() {
    @Override
    public Iterator<String> call(String sentence) {
        return Arrays.stream(sentence.split(""))
                     .filter(c -> !c.equals(" "))
                     .iterator();
    }
});
```

---

### 5. PairFunction<T, K, V> (函数接口)
**路径**: `org.apache.spark.api.java.function.PairFunction`

**描述**: 创建键值对(Tuple2)的函数，用于构建PairRDD。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T t | Tuple2<K, V> | 将元素转换为键值对 | `pairFunc.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

JavaRDD<String> names = sc.parallelize(Arrays.asList("Alice", "Bob", "Alice"));

// 使用Lambda - 创建键值对
JavaPairRDD<String, Integer> nameCount = names.mapToPair(
    s -> new Tuple2<>(s, 1)
);

// 使用匿名类
JavaPairRDD<Character, String> firstLetterToName = names.mapToPair(
    new PairFunction<String, Character, String>() {
        @Override
        public Tuple2<Character, String> call(String name) {
            return new Tuple2<>(name.charAt(0), name);
        }
    }
);
```

---

### 6. ReduceFunction<T> (函数接口)
**路径**: `org.apache.spark.api.java.function.ReduceFunction`

**描述**: 用于Dataset的reduce操作。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T v1, T v2 | T | 合并两个值 | `reduceFunc.call(a, b)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;

Dataset<Integer> numbers = spark.createDataset(Arrays.asList(1, 2, 3, 4, 5), Encoders.INT());

// 使用Lambda - 求和
Integer sum = numbers.reduce((a, b) -> a + b);

// 使用匿名类 - 求最大值
Integer max = numbers.reduce(new ReduceFunction<Integer>() {
    @Override
    public Integer call(Integer a, Integer b) {
        return Math.max(a, b);
    }
});
```

---

### 7. Function2<T1, T2, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.Function2`

**描述**: 双参数函数接口。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T1 v1, T2 v2 | R | 执行双参数函数 | `func.call(arg1, arg2)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.Function2;

JavaPairRDD<String, Integer> pairs = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("a", 1),
    new Tuple2<>("a", 2),
    new Tuple2<>("b", 3)
));

// 使用Lambda - 聚合
JavaPairRDD<String, Integer> sums = pairs.reduceByKey((a, b) -> a + b);

// 使用匿名类
JavaPairRDD<String, Integer> maxValues = pairs.reduceByKey(new Function2<Integer, Integer, Integer>() {
    @Override
    public Integer call(Integer a, Integer b) {
        return Math.max(a, b);
    }
});
```

---

### 8. VoidFunction<T> (函数接口)
**路径**: `org.apache.spark.api.java.function.VoidFunction`

**描述**: 无返回值的函数接口。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T t | void | 执行操作，无返回值 | `voidFunc.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.VoidFunction;

JavaRDD<String> rdd = sc.parallelize(Arrays.asList("Alice", "Bob"));

// 使用Lambda
rdd.foreach(name -> System.out.println("Name: " + name));

// 使用匿名类
rdd.foreach(new VoidFunction<String>() {
    @Override
    public void call(String name) {
        System.out.println("Processing: " + name);
    }
});
```

---

### 9. MapGroupsFunction<K, V, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.MapGroupsFunction`

**描述**: 用于GroupedDataset的mapGroups操作。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | K key, Iterator<V> values | R | 处理分组后的数据 | `func.call(key, values)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.MapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import java.util.Iterator;

Dataset<Row> ds = spark.read().json("data.json");

// 按部门分组并计算平均薪资
Dataset<Double> avgSalaries = ds.groupBy("dept")
    .mapGroups(new MapGroupsFunction<String, Row, Double>() {
        @Override
        public Double call(String dept, Iterator<Row> employees) {
            double sum = 0;
            int count = 0;
            while (employees.hasNext()) {
                sum += employees.next().getDouble("salary");
                count++;
            }
            return sum / count;
        }
    }, Encoders.DOUBLE());
```

---

### 10. ForeachFunction<T> (函数接口)
**路径**: `org.apache.spark.api.java.function.ForeachFunction`

**描述**: Dataset的foreach操作使用的函数。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T t | void | 对每个元素执行操作 | `foreachFunc.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

Dataset<Row> ds = spark.read().json("users.json");

// 使用Lambda
ds.foreach(row -> {
    String name = row.getString("name");
    System.out.println("Processing user: " + name);
});

// 使用匿名类
ds.foreach(new ForeachFunction<Row>() {
    @Override
    public void call(Row row) {
        // 写入外部系统
        writeToDatabase(row);
    }
});
```

---

### 11. DoubleFunction<T> (函数接口)
**路径**: `org.apache.spark.api.java.function.DoubleFunction`

**描述**: 返回Double值的函数，用于构建DoubleRDD。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T t | double | 计算并返回Double值 | `doubleFunc.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.DoubleFunction;

JavaRDD<String> strings = sc.parallelize(Arrays.asList("hello", "world"));

// 使用Lambda - 获取字符串长度
JavaDoubleRDD lengths = strings.mapToDouble(s -> s.length() * 1.5);

// 使用匿名类
JavaDoubleRDD scores = strings.mapToDouble(new DoubleFunction<String>() {
    @Override
    public double call(String s) {
        return calculateScore(s);
    }
});

// DoubleRDD特有的统计方法
double mean = lengths.mean();
double sum = lengths.sum();
```

---

### 12. MapPartitionsFunction<T, U> (函数接口)
**路径**: `org.apache.spark.api.java.function.MapPartitionsFunction`

**描述**: 用于Dataset的mapPartitions操作。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | Iterator<T> input | Iterator<U> | 处理整个分区的数据 | `func.call(partitionIterator)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.MapPartitionsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

Dataset<Integer> numbers = spark.createDataset(Arrays.asList(1, 2, 3, 4, 5), Encoders.INT());

// 每个分区计算总和
Dataset<Integer> partitionSums = numbers.mapPartitions(
    new MapPartitionsFunction<Integer, Integer>() {
        @Override
        public Iterator<Integer> call(Iterator<Integer> partition) {
            List<Integer> result = new ArrayList<>();
            int sum = 0;
            while (partition.hasNext()) {
                sum += partition.next();
            }
            result.add(sum);
            return result.iterator();
        }
    }, Encoders.INT());
```

---

### 13. FlatMapGroupsFunction<K, V, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.FlatMapGroupsFunction`

**描述**: 分组后flatMap操作的函数。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | K key, Iterator<V> values | Iterator<R> | 分组后产生多个结果 | `func.call(key, values)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.FlatMapGroupsFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

Dataset<Row> sales = spark.read().json("sales.json");

// 每个客户的所有订单
Dataset<String> customerOrders = sales.groupBy("customerId")
    .flatMapGroups(new FlatMapGroupsFunction<String, Row, String>() {
        @Override
        public Iterator<String> call(String customerId, Iterator<Row> orders) {
            List<String> result = new ArrayList<>();
            while (orders.hasNext()) {
                Row order = orders.next();
                result.add("Order: " + order.getString("orderId"));
            }
            return result.iterator();
        }
    }, Encoders.STRING());
```

---

### 14. ForeachPartitionFunction<T> (函数接口)
**路径**: `org.apache.spark.api.java.function.ForeachPartitionFunction`

**描述**: 用于Dataset的foreachPartition操作。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | Iterator<T> t | void | 对整个分区执行操作 | `func.call(partitionIterator)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.util.Iterator;

Dataset<Row> ds = spark.read().json("large_data.json");

// 每个分区建立一次数据库连接
ds.foreachPartition(new ForeachPartitionFunction<Row>() {
    @Override
    public void call(Iterator<Row> partition) {
        // 在分区开始时建立连接
        Connection conn = createDatabaseConnection();
        try {
            while (partition.hasNext()) {
                Row row = partition.next();
                // 批量写入数据库
                writeToDatabase(conn, row);
            }
        } finally {
            conn.close();
        }
    }
});
```

---

### 15. PairFlatMapFunction<T, K, V> (函数接口)
**路径**: `org.apache.spark.api.java.function.PairFlatMapFunction`

**描述**: 一对多键值对映射函数。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T t | Iterator<Tuple2<K, V>> | 一个元素映射为多个键值对 | `func.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;
import java.util.Iterator;
import java.util.Arrays;

JavaRDD<String> sentences = sc.parallelize(Arrays.asList("Hello World", "Apache Spark"));

// 每个句子拆分为单词并创建键值对(单词, 1)
JavaPairRDD<String, Integer> wordCounts = sentences.flatMapToPair(
    new PairFlatMapFunction<String, String, Integer>() {
        @Override
        public Iterator<Tuple2<String, Integer>> call(String sentence) {
            String[] words = sentence.split(" ");
            List<Tuple2<String, Integer>> result = new ArrayList<>();
            for (String word : words) {
                result.add(new Tuple2<>(word.toLowerCase(), 1));
            }
            return result.iterator();
        }
    }
);
```

---

### 16. DoubleFlatMapFunction<T> (函数接口)
**路径**: `org.apache.spark.api.java.function.DoubleFlatMapFunction`

**描述**: 一对多Double值映射函数。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T t | Iterator<Double> | 一个元素映射为多个Double值 | `func.call(item)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.DoubleFlatMapFunction;
import java.util.Iterator;
import java.util.Arrays;

JavaRDD<String> numbersStr = sc.parallelize(Arrays.asList("1,2,3", "4,5", "6"));

// 将逗号分隔的数字字符串拆分为多个Double
JavaDoubleRDD allNumbers = numbersStr.flatMapToDouble(
    new DoubleFlatMapFunction<String>() {
        @Override
        public Iterator<Double> call(String s) {
            String[] parts = s.split(",");
            List<Double> nums = new ArrayList<>();
            for (String part : parts) {
                nums.add(Double.parseDouble(part));
            }
            return nums.iterator();
        }
    }
);
```

---

### 17. CoGroupFunction<K, V1, V2, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.CoGroupFunction`

**描述**: 用于两个数据集的co-group操作。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | K key, Iterator<V1> left, Iterator<V2> right | Iterator<R> | 处理两个分组的值 | `func.call(key, leftIter, rightIter)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.CoGroupFunction;
import scala.Tuple2;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.List;

JavaPairRDD<String, Integer> rdd1 = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("a", 1), new Tuple2<>("a", 2), new Tuple2<>("b", 3)
));

JavaPairRDD<String, String> rdd2 = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("a", "x"), new Tuple2<>("a", "y"), new Tuple2<>("b", "z")
));

// Co-group并合并结果
JavaPairRDD<String, String> result = rdd1.cogroup(rdd2).flatMapValues(
    new CoGroupFunction<String, Integer, String, String>() {
        @Override
        public Iterator<String> call(String key, Iterator<Integer> v1, Iterator<String> v2) {
            List<String> results = new ArrayList<>();
            while (v1.hasNext() && v2.hasNext()) {
                results.add(v1.next() + "-" + v2.next());
            }
            return results.iterator();
        }
    }
);
```

---

### 18. FlatMapFunction2<T1, T2, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.FlatMapFunction2`

**描述**: 双参数一对多映射函数。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T1 t1, T2 t2 | Iterator<R> | 双参数映射为多个结果 | `func.call(arg1, arg2)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.FlatMapFunction2;
import java.util.Iterator;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;

// 在某些join操作中使用
JavaPairRDD<String, Integer> left = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("a", 1)
));

JavaPairRDD<String, String> right = sc.parallelizePairs(Arrays.asList(
    new Tuple2<>("a", "x")
));

// 自定义flatMapValues操作
JavaRDD<String> joined = left.flatMapValues(
    new FlatMapFunction2<Integer, String, String>() {
        @Override
        public Iterator<String> call(Integer num, String str) {
            List<String> results = new ArrayList<>();
            results.add(num + str);
            results.add(str + num);
            return results.iterator();
        }
    }
);
```

---

### 19. Function0<R> (函数接口)
**路径**: `org.apache.spark.api.java.function.Function0`

**描述**: 无参数函数接口。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | 无 | R | 执行无参数函数 | `func.call()` |

**使用示例**:
```java
import org.apache.spark.api.java.function.Function0;

// 创建零参数函数
Function0<String> getTimestamp = new Function0<String>() {
    @Override
    public String call() {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
    }
};

// 在某些Spark操作中使用
String timestamp = getTimestamp.call();
```

---

### 20. Function3<T1, T2, T3, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.Function3`

**描述**: 三参数函数接口。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T1 v1, T2 v2, T3 v3 | R | 执行三参数函数 | `func.call(arg1, arg2, arg3)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.Function3;

// 三参数函数
Function3<Integer, Integer, Integer, Integer> sum3 = 
    new Function3<Integer, Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer a, Integer b, Integer c) {
            return a + b + c;
        }
    };

int result = sum3.call(1, 2, 3); // 返回6
```

---

### 21. Function4<T1, T2, T3, T4, R> (函数接口)
**路径**: `org.apache.spark.api.java.function.Function4`

**描述**: 四参数函数接口。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T1 v1, T2 v2, T3 v3, T4 v4 | R | 执行四参数函数 | `func.call(arg1, arg2, arg3, arg4)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.Function4;

// 四参数函数
Function4<Integer, Integer, Integer, Integer, Integer> sum4 = 
    new Function4<Integer, Integer, Integer, Integer, Integer>() {
        @Override
        public Integer call(Integer a, Integer b, Integer c, Integer d) {
            return a + b + c + d;
        }
    };

int result = sum4.call(1, 2, 3, 4); // 返回10
```

---

### 22. VoidFunction2<T1, T2> (函数接口)
**路径**: `org.apache.spark.api.java.function.VoidFunction2`

**描述**: 双参数无返回值函数。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| call | T1 v1, T2 v2 | void | 执行双参数操作，无返回值 | `func.call(arg1, arg2)` |

**使用示例**:
```java
import org.apache.spark.api.java.function.VoidFunction2;

// 双参数无返回值函数
VoidFunction2<String, Integer> printWithCount = 
    new VoidFunction2<String, Integer>() {
        @Override
        public void call(String name, Integer count) {
            System.out.println(name + " appears " + count + " times");
        }
    };

printWithCount.call("Alice", 5);
```

---

## 三、SQL Java API

### 1. ColumnVectorUtils (公共类)
**路径**: `org.apache.spark.sql.execution.vectorized.ColumnVectorUtils`

**描述**: 操作ColumnVector的工具类，主要用于调试或非性能关键路径。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| populate | ConstantColumnVector col, InternalRow row, int fieldIdx | void | 将行数据填充到ConstantColumnVector | `ColumnVectorUtils.populate(col, row, 0)` |
| toJavaIntArray | ColumnarArray array | int[] | 将ColumnarArray转换为int[] | `int[] arr = ColumnVectorUtils.toJavaIntArray(colArray)` |
| toJavaIntMap | ColumnarMap map | Map<Integer, Integer> | 将ColumnarMap转换为Java Map | `Map<Integer, Integer> map = ColumnVectorUtils.toJavaIntMap(colMap)` |
| toBatch | StructType schema, MemoryMode memMode, Iterator<Row> row | ColumnarBatch | 将行迭代器转换为ColumnarBatch | `ColumnarBatch batch = ColumnVectorUtils.toBatch(schema, MemoryMode.ON_HEAP, rowIter)` |

**使用示例**:
```java
import org.apache.spark.sql.execution.vectorized.ColumnVectorUtils;
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.Row;
import java.util.Iterator;
import java.util.Arrays;
import java.util.List;

// 转换行为ColumnarBatch
StructType schema = new StructType()
    .add("id", "int")
    .add("name", "string");

List<Row> rows = Arrays.asList(
    RowFactory.create(1, "Alice"),
    RowFactory.create(2, "Bob")
);

ColumnarBatch batch = ColumnVectorUtils.toBatch(
    schema, 
    MemoryMode.ON_HEAP, 
    rows.iterator()
);
```

---

### 2. ConstantColumnVector (公共类)
**路径**: `org.apache.spark.sql.execution.vectorized.ConstantColumnVector`

**描述**: 存储常量值的ColumnVector，所有行共享同一个值。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| 构造方法 | int numRows, DataType type | ConstantColumnVector | 创建ConstantColumnVector | `new ConstantColumnVector(100, DataTypes.IntegerType)` |
| close | 无 | void | 关闭并释放资源 | `vector.close()` |
| closeIfFreeable | 无 | void | 如果可释放则关闭（无操作） | `vector.closeIfFreeable()` |
| hasNull | 无 | boolean | 检查是否有null值 | `boolean hasNull = vector.hasNull()` |
| numNulls | 无 | int | 返回null值的数量 | `int count = vector.numNulls()` |
| isNullAt | int rowId | boolean | 检查指定行是否为null | `boolean isNull = vector.isNullAt(0)` |
| setNull | 无 | void | 设置所有行为null | `vector.setNull()` |
| setNotNull | 无 | void | 设置所有行为非null | `vector.setNotNull()` |
| getBoolean | int rowId | boolean | 获取布尔值 | `boolean val = vector.getBoolean(0)` |
| setBoolean | boolean value | void | 设置所有行的布尔值 | `vector.setBoolean(true)` |
| getByte | int rowId | byte | 获取字节值 | `byte val = vector.getByte(0)` |
| setByte | byte value | void | 设置所有行的字节值 | `vector.setByte((byte)10)` |
| getShort | int rowId | short | 获取short值 | `short val = vector.getShort(0)` |
| setShort | short value | void | 设置所有行的short值 | `vector.setShort((short)100)` |
| getInt | int rowId | int | 获取int值 | `int val = vector.getInt(0)` |
| setInt | int value | void | 设置所有行的int值 | `vector.setInt(42)` |
| getLong | int rowId | long | 获取long值 | `long val = vector.getLong(0)` |
| setLong | long value | void | 设置所有行的long值 | `vector.setLong(123L)` |
| getFloat | int rowId | float | 获取float值 | `float val = vector.getFloat(0)` |
| setFloat | float value | void | 设置所有行的float值 | `vector.setFloat(3.14f)` |
| getDouble | int rowId | double | 获取double值 | `double val = vector.getDouble(0)` |
| setDouble | double value | void | 设置所有行的double值 | `vector.setDouble(2.718)` |
| getArray | int rowId | ColumnarArray | 获取数组值 | `ColumnarArray arr = vector.getArray(0)` |
| setArray | ColumnarArray value | void | 设置所有行的数组值 | `vector.setArray(array)` |
| getMap | int ordinal | ColumnarMap | 获取Map值 | `ColumnarMap map = vector.getMap(0)` |
| setMap | ColumnarMap value | void | 设置所有行的Map值 | `vector.setMap(mapValue)` |
| getDecimal | int rowId, int precision, int scale | Decimal | 获取Decimal值 | `Decimal d = vector.getDecimal(0, 10, 2)` |
| setDecimal | Decimal value, int precision | void | 设置所有行的Decimal值 | `vector.setDecimal(Decimal.apply(100.50), 10)` |
| getUTF8String | int rowId | UTF8String | 获取UTF8String值 | `UTF8String str = vector.getUTF8String(0)` |
| setUtf8String | UTF8String value | void | 设置所有行的UTF8String值 | `vector.setUtf8String(UTF8String.fromString("hello"))` |
| getBinary | int rowId | byte[] | 获取二进制值 | `byte[] data = vector.getBinary(0)` |
| setBinary | byte[] value | void | 设置所有行的二进制值 | `vector.setBinary(new byte[]{1, 2, 3})` |
| getChild | int ordinal | ColumnVector | 获取子列向量 | `ColumnVector child = vector.getChild(0)` |
| setChild | int ordinal, ConstantColumnVector value | void | 设置子列向量 | `vector.setChild(0, childVector)` |
| setCalendarInterval | CalendarInterval value | void | 设置CalendarInterval值 | `vector.setCalendarInterval(interval)` |
| setVariant | VariantVal value | void | 设置Variant值 | `vector.setVariant(variant)` |

**使用示例**:
```java
import org.apache.spark.sql.execution.vectorized.ConstantColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.unsafe.types.UTF8String;

// 创建包含100行的常量int列
ConstantColumnVector intVector = new ConstantColumnVector(100, DataTypes.IntegerType);
intVector.setInt(42);
intVector.setNotNull();

// 创建常量字符串列
ConstantColumnVector strVector = new ConstantColumnVector(100, DataTypes.StringType);
strVector.setUtf8String(UTF8String.fromString("hello"));

// 创建常量null列
ConstantColumnVector nullVector = new ConstantColumnVector(100, DataTypes.IntegerType);
nullVector.setNull();

// 使用完成后关闭
intVector.close();
strVector.close();
nullVector.close();
```

---

### 3. Dictionary (公共接口)
**路径**: `org.apache.spark.sql.execution.vectorized.Dictionary`

**描述**: ColumnVector中用于解码字典编码值的接口。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| decodeToInt | int id | int | 解码为int值 | `int value = dictionary.decodeToInt(0)` |
| decodeToLong | int id | long | 解码为long值 | `long value = dictionary.decodeToLong(0)` |
| decodeToFloat | int id | float | 解码为float值 | `float value = dictionary.decodeToFloat(0)` |
| decodeToDouble | int id | double | 解码为double值 | `double value = dictionary.decodeToDouble(0)` |
| decodeToBinary | int id | byte[] | 解码为二进制值 | `byte[] data = dictionary.decodeToBinary(0)` |

**使用示例**:
```java
import org.apache.spark.sql.execution.vectorized.Dictionary;

// 实现自定义Dictionary
public class MyDictionary implements Dictionary {
    private int[] intValues;
    
    public MyDictionary(int[] values) {
        this.intValues = values;
    }
    
    @Override
    public int decodeToInt(int id) {
        return intValues[id];
    }
    
    @Override
    public long decodeToLong(int id) {
        return (long) intValues[id];
    }
    
    // ... 其他方法实现
}

// 使用Dictionary解码值
Dictionary dict = new MyDictionary(new int[]{1, 2, 3, 4, 5});
int value = dict.decodeToInt(2); // 返回3
```

---

### 4. ParquetCompressionCodec (公共枚举)
**路径**: `org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodec`

**描述**: Spark支持的Parquet压缩编解码器映射。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| NONE | 无 | ParquetCompressionCodec | 无压缩 | `ParquetCompressionCodec.NONE` |
| UNCOMPRESSED | 无 | ParquetCompressionCodec | 不压缩 | `ParquetCompressionCodec.UNCOMPRESSED` |
| SNAPPY | 无 | ParquetCompressionCodec | Snappy压缩 | `ParquetCompressionCodec.SNAPPY` |
| GZIP | 无 | ParquetCompressionCodec | GZIP压缩 | `ParquetCompressionCodec.GZIP` |
| LZO | 无 | ParquetCompressionCodec | LZO压缩 | `ParquetCompressionCodec.LZO` |
| BROTLI | 无 | ParquetCompressionCodec | Brotli压缩 | `ParquetCompressionCodec.BROTLI` |
| LZ4 | 无 | ParquetCompressionCodec | LZ4压缩 | `ParquetCompressionCodec.LZ4` |
| LZ4_RAW | 无 | ParquetCompressionCodec | LZ4原始压缩 | `ParquetCompressionCodec.LZ4_RAW` |
| ZSTD | 无 | ParquetCompressionCodec | ZSTD压缩 | `ParquetCompressionCodec.ZSTD` |
| getCompressionCodec | 无 | CompressionCodecName | 获取Parquet编解码器 | `CompressionCodecName codec = ParquetCompressionCodec.SNAPPY.getCompressionCodec()` |
| fromString | String s | ParquetCompressionCodec | 从字符串解析 | `ParquetCompressionCodec codec = ParquetCompressionCodec.fromString("snappy")` |
| lowerCaseName | 无 | String | 获取小写名称 | `String name = ParquetCompressionCodec.SNAPPY.lowerCaseName()` |
| availableCodecs | 无 | List<ParquetCompressionCodec> | 可用的编解码器列表 | `List<ParquetCompressionCodec> codecs = ParquetCompressionCodec.availableCodecs` |

**使用示例**:
```java
import org.apache.spark.sql.execution.datasources.parquet.ParquetCompressionCodec;

// 查看所有可用的压缩编解码器
List<ParquetCompressionCodec> available = ParquetCompressionCodec.availableCodecs;
for (ParquetCompressionCodec codec : available) {
    System.out.println(codec.lowerCaseName());
}

// 从字符串获取压缩编解码器
ParquetCompressionCodec codec = ParquetCompressionCodec.fromString("snappy");

// 使用配置写入Parquet
Dataset<Row> df = spark.read().parquet("data.parquet");
df.write()
  .option("compression", codec.lowerCaseName())
  .parquet("output.parquet");
```

---

### 5. OrcCompressionCodec (公共枚举)
**路径**: `org.apache.spark.sql.execution.datasources.orc.OrcCompressionCodec`

**描述**: Spark支持的ORC压缩编解码器映射。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| NONE | 无 | OrcCompressionCodec | 无压缩 | `OrcCompressionCodec.NONE` |
| UNCOMPRESSED | 无 | OrcCompressionCodec | 不压缩 | `OrcCompressionCodec.UNCOMPRESSED` |
| ZLIB | 无 | OrcCompressionCodec | ZLIB压缩 | `OrcCompressionCodec.ZLIB` |
| SNAPPY | 无 | OrcCompressionCodec | Snappy压缩 | `OrcCompressionCodec.SNAPPY` |
| LZO | 无 | OrcCompressionCodec | LZO压缩 | `OrcCompressionCodec.LZO` |
| LZ4 | 无 | OrcCompressionCodec | LZ4压缩 | `OrcCompressionCodec.LZ4` |
| ZSTD | 无 | OrcCompressionCodec | ZSTD压缩 | `OrcCompressionCodec.ZSTD` |
| BROTLI | 无 | OrcCompressionCodec | Brotli压缩 | `OrcCompressionCodec.BROTLI` |
| getCompressionKind | 无 | CompressionKind | 获取ORC压缩类型 | `CompressionKind kind = OrcCompressionCodec.ZLIB.getCompressionKind()` |
| lowerCaseName | 无 | String | 获取小写名称 | `String name = OrcCompressionCodec.ZLIB.lowerCaseName()` |

**使用示例**:
```java
import org.apache.spark.sql.execution.datasources.orc.OrcCompressionCodec;

// 选择压缩编解码器
OrcCompressionCodec codec = OrcCompressionCodec.ZSTD;

// 使用配置写入ORC
Dataset<Row> df = spark.read().orc("data.orc");
df.write()
  .option("compression", codec.lowerCaseName())
  .orc("output.orc");
```

---

### 6. AvroCompressionCodec (公共枚举)
**路径**: `org.apache.spark.sql.avro.AvroCompressionCodec`

**描述**: Spark支持的Avro压缩编解码器映射。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| UNCOMPRESSED | 无 | AvroCompressionCodec | 不压缩 | `AvroCompressionCodec.UNCOMPRESSED` |
| DEFLATE | 无 | AvroCompressionCodec | Deflate压缩 | `AvroCompressionCodec.DEFLATE` |
| SNAPPY | 无 | AvroCompressionCodec | Snappy压缩 | `AvroCompressionCodec.SNAPPY` |
| BZIP2 | 无 | AvroCompressionCodec | BZIP2压缩 | `AvroCompressionCodec.BZIP2` |
| XZ | 无 | AvroCompressionCodec | XZ压缩 | `AvroCompressionCodec.XZ` |
| ZSTANDARD | 无 | AvroCompressionCodec | ZStandard压缩 | `AvroCompressionCodec.ZSTANDARD` |
| getCodecName | 无 | String | 获取编解码器名称 | `String name = AvroCompressionCodec.DEFLATE.getCodecName()` |
| getSupportCompressionLevel | 无 | boolean | 是否支持压缩级别 | `boolean supports = AvroCompressionCodec.DEFLATE.getSupportCompressionLevel()` |
| fromString | String s | AvroCompressionCodec | 从字符串解析 | `AvroCompressionCodec codec = AvroCompressionCodec.fromString("deflate")` |
| lowerCaseName | 无 | String | 获取小写名称 | `String name = AvroCompressionCodec.DEFLATE.lowerCaseName()` |

**使用示例**:
```java
import org.apache.spark.sql.avro.AvroCompressionCodec;

// 检查是否支持压缩级别
AvroCompressionCodec deflate = AvroCompressionCodec.DEFLATE;
if (deflate.getSupportCompressionLevel()) {
    // 设置压缩级别
    df.write()
      .option("compression", deflate.lowerCaseName())
      .option("deflateLevel", "9")
      .format("avro")
      .save("output.avro");
}

// 从字符串获取编解码器
AvroCompressionCodec codec = AvroCompressionCodec.fromString("snappy");
```

---

### 7. V1Scan (公共接口)
**路径**: `org.apache.spark.sql.connector.read.V1Scan`

**描述**: DataSource V1的扫描接口，用于迁移到V2 API。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| toV1TableScan | SQLContext context | T extends BaseRelation & TableScan | 创建V1的TableScan | `BaseRelation relation = scan.toV1TableScan(sqlContext)` |

**使用示例**:
```java
import org.apache.spark.sql.connector.read.V1Scan;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.sources.BaseRelation;
import org.apache.spark.sql.sources.TableScan;

public class MyV1Scan implements V1Scan {
    @Override
    public StructType readSchema() {
        return new StructType()
            .add("id", "int")
            .add("value", "string");
    }
    
    @Override
    public <T extends BaseRelation & TableScan> T toV1TableScan(SQLContext context) {
        return new MyTableRelation(context);
    }
}
```

---

### 8. V1Write (公共接口)
**路径**: `org.apache.spark.sql.connector.write.V1Write`

**描述**: V1写入接口，用于V1 InsertableRelation。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| toInsertableRelation | 无 | InsertableRelation | 创建V1的InsertableRelation | `InsertableRelation rel = write.toInsertableRelation()` |

**使用示例**:
```java
import org.apache.spark.sql.connector.write.V1Write;
import org.apache.spark.sql.sources.InsertableRelation;
import org.apache.spark.sql.DataFrameWriter;

public class MyV1Write implements V1Write {
    private String path;
    
    public MyV1Write(String path) {
        this.path = path;
    }
    
    @Override
    public InsertableRelation toInsertableRelation() {
        return new MyInsertableRelation(path);
    }
}

// 使用V1Write
InsertableRelation relation = v1Write.toInsertableRelation();
relation.insert(df, false);
```

---

### 9. Offset (公共抽象类 - 已弃用)
**路径**: `org.apache.spark.sql.execution.streaming.Offset`

**描述**: 流式数据源的偏移量类（内部使用，已弃用）。

**注**: 此类已弃用，新实现应使用DataSource V2 API中的`org.apache.spark.sql.connector.read.streaming.Offset`。

---

## 四、Streaming Java API

### 1. StreamingContextState (公共枚举)
**路径**: `org.apache.spark.streaming.StreamingContextState`

**描述**: 表示StreamingContext的状态（DeveloperApi）。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| INITIALIZED | 无 | StreamingContextState | 已创建但未启动 | `StreamingContextState.INITIALIZED` |
| ACTIVE | 无 | StreamingContextState | 已启动且未停止 | `StreamingContextState.ACTIVE` |
| STOPPED | 无 | StreamingContextState | 已停止，不能再使用 | `StreamingContextState.STOPPED` |

**使用示例**:
```java
import org.apache.spark.streaming.StreamingContextState;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(1));

// 检查状态
if (jssc.getState() == StreamingContextState.INITIALIZED) {
    jssc.start();
}

if (jssc.getState() == StreamingContextState.ACTIVE) {
    jssc.awaitTermination();
}

jssc.stop();
// 状态现在是 STOPPED
```

---

### 2. WriteAheadLog (公共抽象类)
**路径**: `org.apache.spark.streaming.util.WriteAheadLog`

**描述**: 预写日志抽象类，用于保存接收的数据以恢复Driver故障（DeveloperApi）。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| write | ByteBuffer record, long time | WriteAheadLogRecordHandle | 写入记录并返回句柄 | `WriteAheadLogRecordHandle handle = wal.write(data, timestamp)` |
| read | WriteAheadLogRecordHandle handle | ByteBuffer | 根据句柄读取记录 | `ByteBuffer data = wal.read(handle)` |
| readAll | 无 | Iterator<ByteBuffer> | 读取所有未清理的记录 | `Iterator<ByteBuffer> records = wal.readAll()` |
| clean | long threshTime, boolean waitForCompletion | void | 清理旧记录 | `wal.clean(oldTimestamp, true)` |
| close | 无 | void | 关闭日志（幂等） | `wal.close()` |

**使用示例**:
```java
import org.apache.spark.streaming.util.WriteAheadLog;
import org.apache.spark.streaming.util.WriteAheadLogRecordHandle;
import java.nio.ByteBuffer;
import java.util.Iterator;

public class MyWriteAheadLog extends WriteAheadLog {
    @Override
    public WriteAheadLogRecordHandle write(ByteBuffer record, long time) {
        // 实现写入逻辑
        byte[] data = new byte[record.remaining()];
        record.get(data);
        // 保存到持久化存储...
        return new MyRecordHandle(time, data.length);
    }
    
    @Override
    public ByteBuffer read(WriteAheadLogRecordHandle handle) {
        // 实现读取逻辑
        return ByteBuffer.wrap(readFromStorage(handle));
    }
    
    @Override
    public Iterator<ByteBuffer> readAll() {
        // 返回所有记录
        return getAllRecords().iterator();
    }
    
    @Override
    public void clean(long threshTime, boolean waitForCompletion) {
        // 清理旧数据
        deleteOldRecords(threshTime);
    }
    
    @Override
    public void close() {
        // 释放资源
        closeStorage();
    }
}
```

---

### 3. WriteAheadLogRecordHandle (公共抽象类)
**路径**: `org.apache.spark.streaming.util.WriteAheadLogRecordHandle`

**描述**: 预写日志记录句柄，包含读取记录所需的所有信息（DeveloperApi）。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| 无公共方法 | - | - | 实现类需提供读取记录所需的信息 | 自定义实现 |

**使用示例**:
```java
import org.apache.spark.streaming.util.WriteAheadLogRecordHandle;
import java.io.Serializable;

public class MyRecordHandle extends WriteAheadLogRecordHandle {
    private final long timestamp;
    private final int offset;
    
    public MyRecordHandle(long timestamp, int offset) {
        this.timestamp = timestamp;
        this.offset = offset;
    }
    
    public long getTimestamp() {
        return timestamp;
    }
    
    public int getOffset() {
        return offset;
    }
}
```

---

## 五、GraphX Java API

### 1. TripletFields (公共类)
**路径**: `org.apache.spark.graphx.TripletFields`

**描述**: 表示EdgeTriplet或EdgeContext的字段子集，用于优化。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| 构造方法 | 无 | TripletFields | 默认构造，包含所有字段 | `new TripletFields()` |
| 构造方法 | boolean useSrc, boolean useDst, boolean useEdge | TripletFields | 自定义字段选择 | `new TripletFields(true, false, true)` |
| useSrc | 无 | boolean | 是否包含源顶点属性 | `if (fields.useSrc) { ... }` |
| useDst | 无 | boolean | 是否包含目标顶点属性 | `if (fields.useDst) { ... }` |
| useEdge | 无 | boolean | 是否包含边属性 | `if (fields.useEdge) { ... }` |
| None | 无 | TripletFields | 不包含任何字段 | `TripletFields.None` |
| EdgeOnly | 无 | TripletFields | 仅包含边字段 | `TripletFields.EdgeOnly` |
| Src | 无 | TripletFields | 包含源顶点和边字段 | `TripletFields.Src` |
| Dst | 无 | TripletFields | 包含目标顶点和边字段 | `TripletFields.Dst` |
| All | 无 | TripletFields | 包含所有字段 | `TripletFields.All` |

**使用示例**:
```java
import org.apache.spark.graphx.TripletFields;
import org.apache.spark.graphx.Graph;

// 创建Graph
Graph<String, Integer> graph = Graph.apply(vertices, edges, "", StorageLevels.MEMORY_ONLY);

// 使用预定义的TripletFields
// 仅需要边属性时使用EdgeOnly
TripletFields edgeOnly = TripletFields.EdgeOnly;

// 需要源顶点和边时使用Src
TripletFields srcFields = TripletFields.Src;

// 自定义TripletFields - 仅需要目标顶点属性
TripletFields dstOnly = new TripletFields(false, true, false);

// 在aggregateMessages中使用
graph.aggregateMessages(
    sendMsg,
    mergeMsg,
    TripletFields.All  // 使用所有字段
);
```

---

### 2. EdgeActiveness (公共枚举 - impl包内)
**路径**: `org.apache.spark.graphx.impl.EdgeActiveness`

**描述**: 边活跃性枚举（内部实现，通常不直接使用）。

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|-------|------|---------|------|------|
| Neither | 无 | EdgeActiveness | 源和目标顶点都不活跃 | `EdgeActiveness.Neither` |
| SourceOnly | 无 | EdgeActiveness | 仅源顶点活跃 | `EdgeActiveness.SourceOnly` |
| DestinationOnly | 无 | EdgeActiveness | 仅目标顶点活跃 | `EdgeActiveness.DestinationOnly` |
| Both | 无 | EdgeActiveness | 源和目标顶点都活跃 | `EdgeActiveness.Both` |
| Either | 无 | EdgeActiveness | 源或目标顶点活跃 | `EdgeActiveness.Either` |

**注**: 此类在impl包内，属于内部实现，不推荐直接外部调用。

---

## 六、MLlib Java API

MLlib的主要Java API是通过Scala实现的Java包装类，在`mllib/src/main/scala/`目录下。主要的package-info.java文件提供了包级别的信息。

### 1. package-info.java (MLlib)
**路径**: `org.apache.spark.mllib`

**描述**: MLlib包信息，提供机器学习算法的Java API。

**主要组件**:
- 分类算法（LogisticRegression, SVM, NaiveBayes）
- 回归算法（LinearRegression, RidgeRegression, LassoRegression）
- 聚类算法（KMeans, GaussianMixture, LDA）
- 推荐算法（ALS）
- 特征提取（Word2Vec, TF-IDF）
- 统计分析（Statistics）

**使用示例参考**:
```java
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;

JavaRDD<LabeledPoint> trainingData = sc.parallelize(
    Arrays.asList(
        new LabeledPoint(1.0, Vectors.dense(0.0, 1.1, 0.1)),
        new LabeledPoint(0.0, Vectors.dense(2.0, 1.0, -1.0))
    )
);

// 训练逻辑回归模型
LogisticRegressionModel model = new LogisticRegressionWithLBFGS()
    .setNumClasses(2)
    .run(trainingData.rdd());

// 预测
double prediction = model.predict(Vectors.dense(1.0, 0.0, 1.0));
```

---

### 2. package-info.java (ML)
**路径**: `org.apache.spark.ml`

**描述**: ML包信息，提供基于DataFrame的机器学习Pipeline API。

**主要组件**:
- Pipeline（Estimator, Transformer, Model）
- 特征转换器（Tokenizer, HashingTF, IDF, StandardScaler）
- 分类算法（LogisticRegression, DecisionTreeClassifier, RandomForestClassifier）
- 回归算法（LinearRegression, DecisionTreeRegressor）
- 聚类算法（KMeans）
- 参数调优（CrossValidator, TrainValidationSplit）

**使用示例参考**:
```java
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.feature.Tokenizer;
import org.apache.spark.ml.feature.HashingTF;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

// 创建Pipeline
Tokenizer tokenizer = new Tokenizer()
    .setInputCol("text")
    .setOutputCol("words");

HashingTF hashingTF = new HashingTF()
    .setNumFeatures(1000)
    .setInputCol("words")
    .setOutputCol("features");

LogisticRegression lr = new LogisticRegression()
    .setMaxIter(10)
    .setRegParam(0.01);

Pipeline pipeline = new Pipeline()
    .setStages(new PipelineStage[]{tokenizer, hashingTF, lr});

// 训练模型
PipelineModel model = pipeline.fit(trainingData);

// 预测
Dataset<Row> predictions = model.transform(testData);
```

---

## 七、补充说明

### 关于Scala实现的Java包装类

以下主要的Java API类是通过Scala实现的，位于`core/src/main/scala/`等目录：

1. **JavaSparkContext** - Spark上下文的Java包装
2. **JavaRDD** - RDD的Java包装
3. **JavaPairRDD** - 键值对RDD的Java包装
4. **JavaDataset** / **JavaDataFrame** - Dataset/DataFrame的Java包装

这些类的详细API文档需要参考Spark官方文档：
- https://spark.apache.org/docs/latest/api/java/

### 使用Lambda表达式

大多数函数接口都支持Lambda表达式：

```java
// 传统方式
rdd.map(new Function<String, Integer>() {
    @Override
    public Integer call(String s) {
        return s.length();
    }
});

// Lambda方式
rdd.map(s -> s.length());
```

### Serializable要求

所有函数接口都继承自Serializable，确保可以在分布式环境中传输：

```java
// 函数类必须是可序列化的
public class MyFunction implements Function<String, Integer>, Serializable {
    private int multiplier; // 成员变量也必须可序列化
    
    @Override
    public Integer call(String s) {
        return s.length() * multiplier;
    }
}
```

---

## 八、完整使用示例

### Word Count示例

```java
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkConf;
import scala.Tuple2;
import java.util.Arrays;
import java.util.Iterator;

SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local[*]");
JavaSparkContext sc = new JavaSparkContext(conf);

JavaRDD<String> textFile = sc.textFile("hdfs://...");

// 拆分单词
JavaRDD<String> words = textFile.flatMap(
    s -> Arrays.asList(s.split(" ")).iterator()
);

// 映射为键值对
JavaPairRDD<String, Integer> pairs = words.mapToPair(
    s -> new Tuple2<>(s, 1)
);

// 按键聚合
JavaPairRDD<String, Integer> counts = pairs.reduceByKey(
    (a, b) -> a + b
);

counts.saveAsTextFile("hdfs://...");
sc.close();
```

### DataFrame操作示例

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Encoders;
import org.apache.spark.api.java.function.MapFunction;

SparkSession spark = SparkSession.builder()
    .appName("DataFrameExample")
    .master("local[*]")
    .getOrCreate();

Dataset<Row> df = spark.read().json("people.json");

// 过滤
Dataset<Row> adults = df.filter("age > 18");

// 转换
Dataset<Integer> ages = df.map(
    (MapFunction<Row, Integer>) row -> row.getInt("age"),
    Encoders.INT()
);

// 聚合
Dataset<Row> avgAgeByDept = df.groupBy("department")
    .avg("age");

spark.stop();
```

---

**文档版本**: Spark 4.0.0
**生成日期**: 2026-05-09
**注**: 此文档仅包含Java源码中定义的public类、接口和枚举。Scala实现的Java包装类请参考官方JavaDoc。

