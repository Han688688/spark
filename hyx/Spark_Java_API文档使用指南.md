# Spark Java API文档使用指南

## 现有文档

hyx目录包含两个完整的Spark Java API文档：

### 1. spark_java_api_complete_list.md (业务视角)

**特点**：
- ✅ 按业务类别分组（Core RDD、Streaming、SQL、MLlib、GraphX等）
- ✅ 包含所有API（包括Scala实现的Java包装类）
- ✅ 完整方法签名、参数、返回类型、描述
- ✅ 稳定性标注（@Stable/@Evolving/@Deprecated）
- ❌ 缺少示例列

**适用场景**：
- 了解Spark Java API全貌
- 按业务场景查找API（如RDD操作、Streaming等）
- API对比和覆盖率分析

**文档结构**：
```
一、Core RDD Java API
  1.1 JavaSparkContext（数据创建、文件读取、广播变量等）
  1.2 JavaRDDLike（转换算子、行动算子）
  1.3 JavaRDD（持久化、分区、去重）
  1.4 JavaPairRDD（聚合、连接、排序）
  1.5 JavaDoubleRDD

二、Streaming Java API
  2.1 JavaStreamingContext
  2.2 JavaDStreamLike
  2.3 JavaDStream
  ...

三、Java函数接口
  所有函数接口（Function、FilterFunction等）

四、UDF接口
  UDF0-22等用户自定义函数接口

五、MLlib Java API
  分类、聚类、回归、推荐等算法API

六、GraphX API
  TripletFields等图计算API

七、SQL Connector API
  DataSource V2接口

八、方法数量统计
  约1100个方法

九、使用建议
  API使用最佳实践
```

### 2. spark_java_api_public_classes.md (技术视角)

**特点**：
- ✅ 只包含Public类、Interface、Enum（可直接外部调用）
- ✅ 每个方法都有示例列（简单调用示例）
- ✅ 从Spark源码直接提取
- ✅ 过滤了internal、impl等内部实现类
- ❌ 按技术包分组（不如业务视角直观）

**适用场景**：
- 直接查看API调用示例
- 测试API有效性
- 只关注可直接使用的Public API

**文档结构**：
```
一、Core Java API
  StorageLevels、Optional、JavaFutureAction

二、Common Java API - Function Interfaces
  22个函数接口（每个都有示例）

三、SQL Java API
  ColumnVector、CompressionCodec等

四、Streaming Java API
  StreamingContextState、WriteAheadLog

五、GraphX Java API
  TripletFields、EdgeActiveness

六、MLlib Java API
  package说明

七、补充说明
  Lambda表达式使用、Serializable要求

八、完整使用示例
  Word Count示例、DataFrame操作示例
```

## 配合使用方案

### 方案1：先业务后技术

```
步骤1：查看spark_java_api_complete_list.md
  - 按业务类别找到需要的API（如RDD操作）

步骤2：查看spark_java_api_public_classes.md
  - 找到对应类的方法示例
  - 直接复制示例代码测试

示例：
  需要：JavaRDD的map操作
  1. spark_java_api_complete_list.md → 找到1.2 JavaRDDLike的map方法
  2. spark_java_api_public_classes.md → 查看Function接口的示例
  3. 组合使用：
     JavaRDD<Integer> lengths = rdd.map(s -> s.length());
```

### 方案2：按API名称查找

```
场景：已知API名称，需要查看示例

方法1：grep查找
  grep "map" spark_java_api_public_classes.md

方法2：查看表格示例列
  每个方法的表格都有示例列

示例：
  | 方法名 | 参数 | 返回类型 | 描述 | 示例 |
  | call | T1 v1 | R | 执行函数逻辑 | `function.call(input)` |
```

### 方案3：完整代码参考

```
场景：需要完整可运行代码示例

位置：spark_java_api_public_classes.md的第八部分

包含：
  - Word Count完整示例
  - DataFrame操作示例
  - 每个类的完整示例（在类定义后）
```

## API对比使用

### 对比测试覆盖

**步骤1**：从API清单提取所有方法
```bash
cd /home/h00517772/spark/hyx
grep "^| \`" spark_java_api_complete_list.md | wc -l
# 结果：约1100个方法
```

**步骤2**：查看API示例
```bash
grep "| 示例" spark_java_api_public_classes.md
# 查看每个方法的示例列
```

**步骤3**：对比您的测试代码
- 查看API定义
- 查看示例代码
- 测试API有效性

### 按优先级测试

**P0优先级**（Stable API）：
```bash
grep "@Stable" spark_java_api_complete_list.md -A 10
# 找到稳定API，优先测试
```

**示例验证**：
```bash
# 查看StorageLevels示例
grep -A 20 "StorageLevels" spark_java_api_public_classes.md

# 查看Optional示例
grep -A 30 "Optional" spark_java_api_public_classes.md
```

## 快速查找示例

### 查找特定类

```bash
# JavaSparkContext示例
grep "JavaSparkContext" spark_java_api_public_classes.md

# JavaRDD示例
grep "JavaRDD" spark_java_api_complete_list.md | head -20
```

### 查找特定方法

```bash
# map方法示例
grep "map" spark_java_api_public_classes.md | grep "| 示例"

# reduce方法示例
grep "reduce" spark_java_api_complete_list.md
```

### 查找特定类别

```bash
# Core RDD API
grep "^# 一、Core RDD" spark_java_api_complete_list.md -A 200

# Streaming API
grep "^# 二、Streaming" spark_java_api_complete_list.md -A 100
```

## 示例代码使用

### 直接复制示例

从spark_java_api_public_classes.md复制示例：

```java
// 示例1：创建Optional
Optional<String> present = Optional.of("hello");

// 示例2：使用StorageLevels
rdd.persist(StorageLevels.MEMORY_ONLY);

// 示例3：使用Function接口
JavaRDD<Integer> lengths = rdd.map(s -> s.length());
```

### 完整示例运行

从文档末尾复制完整示例：

```java
// Word Count完整示例（spark_java_api_public_classes.md第八部分）
SparkConf conf = new SparkConf().setAppName("WordCount").setMaster("local");
JavaSparkContext sc = new JavaSparkContext(conf);

JavaRDD<String> lines = sc.textFile("/path/to/file.txt");
JavaRDD<String> words = lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator());
JavaPairRDD<String, Integer> pairs = words.mapToPair(word -> new Tuple2<>(word, 1));
JavaPairRDD<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);

counts.foreach(pair -> System.out.println(pair._1 + ": " + pair._2));
sc.stop();
```

## 文档维护

### 更新API清单

当Spark版本更新时：
1. 从源码重新提取Public类
2. 更新spark_java_api_public_classes.md
3. 补充新增API的示例

### 补充缺失示例

为spark_java_api_complete_list.md补充示例：
1. 优先补充P0优先级API（Stable）
2. 优先补充高频使用API
3. 参考spark_java_api_public_classes.md的示例

## 总结

**两个文档配合使用**：
- spark_java_api_complete_list.md：了解API全貌、按业务查找
- spark_java_api_public_classes.md：查看示例、测试有效性

**查找顺序**：
1. 业务场景 → spark_java_api_complete_list.md
2. 具体示例 → spark_java_api_public_classes.md
3. 测试验证 → 复制示例代码运行

**对比测试覆盖**：
1. 从API清单提取所有方法
2. 查看示例了解如何调用
3. 对比测试代码覆盖情况

---

**两个文档互补，满足不同需求！**