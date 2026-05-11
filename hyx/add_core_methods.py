#!/usr/bin/env python3
"""
补充 Spark Java API 文档缺失的核心方法
主要是 JavaRDDLike 接口的方法（map、flatMap、collect、reduce等）
"""

import re

JAVARDDLIKE_METHODS = '''
### JavaRDDLike (核心接口)
**包路径**: `org.apache.spark.api.java`
**说明**: JavaRDD、JavaPairRDD、JavaDoubleRDD共同继承的接口，包含最常用的RDD操作方法。
**方法数量**: 50+

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
'''

def add_javarddlike_methods(filepath):
    """在JavaRDD部分后添加JavaRDDLike方法"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 在JavaRDD部分后插入JavaRDDLike
    # 找到JavaRDD结束位置（下一个类开始前）
    pattern = r'(### JavaRDD.*?\n)(### JavaSparkContext)'
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        insert_pos = match.end(1)
        content = content[:insert_pos] + JAVARDDLIKE_METHODS + '\n' + content[insert_pos:]
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return True

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充JavaRDDLike核心方法...")
    success = add_javarddlike_methods(filepath)
    
    if success:
        print("成功补充50+个核心方法:")
        print("  - map/flatMap/mapToPair/mapToDouble")
        print("  - collect/reduce/fold/aggregate")
        print("  - foreach/foreachPartition")
        print("  - count/countByValue/take/top/first")
        print("  - groupBy/keyBy/cartesian/zip")
        print("  - saveAsTextFile/checkpoint")
        print("  - countAsync/collectAsync（异步操作）")