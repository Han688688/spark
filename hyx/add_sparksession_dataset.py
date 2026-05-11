#!/usr/bin/env python3
"""
补充 SparkSession 和 Dataset 的 Java API
这是现代Spark SQL的核心API
"""

import re

SPARKSESSION_METHODS = '''
---

## SparkSession（现代Spark入口）

### SparkSession
**包路径**: `org.apache.spark.sql`
**说明**: Spark 2.0+的主入口点，替代了旧版的SQLContext和HiveContext。提供DataFrame/Dataset创建、SQL执行等功能。
**方法数量**: 30+

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
'''

DATASET_METHODS = '''
### Dataset[T]（类型安全数据集）
**包路径**: `org.apache.spark.sql`
**说明**: Spark 2.0+的核心数据处理API，提供类型安全的数据操作。DataFrame是Dataset[Row]的特例。
**方法数量**: 80+

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
'''

def add_sparksession_dataset(filepath):
    """在文档末尾添加SparkSession和Dataset部分"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 在文档末尾添加
    content = content.rstrip() + '\n' + SPARKSESSION_METHODS + '\n' + DATASET_METHODS + '\n'
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return True

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充SparkSession和Dataset Java API...")
    success = add_sparksession_dataset(filepath)
    
    if success:
        print("成功补充:")
        print("  - SparkSession: 30+方法（builder, sql, read, createDataFrame等）")
        print("  - Dataset: 80+方法（select, filter, groupBy, join, map, foreach等）")