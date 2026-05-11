#!/usr/bin/env python3
"""
补充 Streaming API 和 MLlib 算法 API
"""

STREAMING_MLLIB_API = '''
---

## Streaming流处理API

### JavaStreamingContext
**包路径**: `org.apache.spark.streaming.api.java`
**说明**: Spark Streaming的Java入口，用于创建DStream和处理实时数据流。
**方法数量**: 25+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `JavaStreamingContext` | SparkConf conf, Duration batchDuration | 构造方法 | 创建StreamingContext | `SparkConf conf = new SparkConf().setAppName("Streaming");<br>JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(5));` |
| `JavaStreamingContext` | JavaSparkContext sparkContext, Duration batchDuration | 构造方法 | 从JavaSparkContext创建 | `JavaStreamingContext jssc = new JavaStreamingContext(sc, Durations.seconds(1));` |
| `textFileStream` | String directory | `JavaDStream[String]` | 监控目录中的新文本文件 | `JavaDStream<String> lines = jssc.textFileStream("hdfs://logs/");` |
| `fileStream` | String directory, Class[K] keyClass, Class[V] valueClass, Class[F] inputFormatClass | `JavaPairDStream[K, V]` | 监控目录中的新文件（指定格式） | `JavaPairDStream<Text, IntWritable> files = jssc.fileStream("hdfs://input/", Text.class, IntWritable.class, TextInputFormat.class);` |
| `socketTextStream` | String hostname, int port | `JavaDStream[String]` | 从TCP socket读取文本流 | `JavaDStream<String> socketStream = jssc.socketTextStream("localhost", 9999);` |
| `socketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 从socket读取，指定存储级别 | `JavaReceiverInputDStream<String> stream = jssc.socketStream("localhost", 9999, StorageLevel.MEMORY_ONLY());` |
| `rawSocketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 原始socket流 | - |
| `kafkaStream` | Map[String, String] kafkaParams, Map[String, Integer] topics | `JavaPairDStream[String, String]` | 从Kafka读取流 | `Map<String, String> kafkaParams = new HashMap<>();<br>kafkaParams.put("bootstrap.servers", "localhost:9092");<br>Map<String, Integer> topics = new HashMap<>();<br>topics.put("my_topic", 1);<br>JavaPairDStream<String, String> kafkaStream = jssc.kafkaStream(kafkaParams, topics);` |
| `flumeStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[SparkFlumeEvent]` | 从Flume读取流 | `JavaReceiverInputDStream<SparkFlumeEvent> flumeStream = jssc.flumeStream("localhost", 41414, StorageLevel.MEMORY_ONLY());` |
| `queueStream` | Queue[JavaRDD[T]] rdds | `JavaInputDStream[T]` | 从RDD队列创建测试流 | `Queue<JavaRDD<String>> queue = new LinkedList<>();<br>queue.add(sc.parallelize(Arrays.asList("a", "b")));<br>JavaInputDStream<String> testStream = jssc.queueStream(queue);` |
| `queueStream` | Queue[JavaRDD[T]] rdds, boolean oneAtATime | `JavaInputDStream[T]` | 逐个RDD处理 | `JavaInputDStream<String> stream = jssc.queueStream(queue, true);` |
| `union` | JavaDStream[T]... streams | `JavaDStream[T]` | 合合多个DStream | `JavaDStream<String> combined = jssc.union(stream1, stream2);` |
| `transform` | JavaDStream[T] dstream, JFunction[JavaRDD[T], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对DStream每个RDD应用变换 | `JavaDStream<String> transformed = dstream.transform(rdd -> rdd.filter(s -> s.length() > 3));` |
| `transformWith` | JavaDStream[T] dstream1, JavaDStream[W] dstream2, JFunction2[JavaRDD[T], JavaRDD[W], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对两个DStream每个RDD应用变换 | - |
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
| `reduce` | JFunction2[T, T, T] f | `JavaDStream[T]` | 对每个RDD内元素聚合 | `JavaDStream<Integer> sums = numbers.reduce((a, b) -> a + b);` |
| `count` | 无 | `JavaDStream[Long]` | 对每个RDD计数 | `JavaDStream<Long> counts = dstream.count();` |
| `countByValue` | 无 | `JavaPairDStream[T, Long]` | 对每个RDD统计每个值的出现次数 | `JavaPairDStream<String, Long> wordCounts = words.countByValue();` |
| `reduceByKey` | JFunction2[V, V, V] func | `JavaPairDStream[K, V]` | 按Key聚合 | `JavaPairDStream<String, Integer> counts = pairs.reduceByKey((a, b) -> a + b);` |
| `groupByKey` | 无 | `JavaPairDStream[K, JIterable[V]]` | 按Key分组 | `JavaPairDStream<String, Iterable<Integer>> grouped = pairs.groupByKey();` |
| `mapValues` | JFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value映射 | `JavaPairDStream<String, String> transformed = pairs.mapValues(v -> "value:" + v);` |
| `flatMapValues` | FlatMapFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value扁平映射 | - |
| `foreachRDD` | VoidFunction[JavaRDD[T]] foreachFunc | `Unit` | 对每个RDD执行操作 | `wordCounts.foreachRDD(rdd -> {<br>    rdd.foreach(pair -> System.out.println(pair._1() + ": " + pair._2()));<br>});` |
| `transform` | JFunction[JavaRDD[T], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对每个RDD变换 | `JavaDStream<String> transformed = dstream.transform(rdd -> rdd.distinct());` |
| `transformToPair` | JFunction[JavaRDD[T], JavaPairRDD[K, V]] transformFunc | `JavaPairDStream[K, V]` | 对每个RDD变换为PairRDD | - |
| `union` | JavaDStream[T] other | `JavaDStream[T]` | 合合DStream | `JavaDStream<String> merged = stream1.union(stream2);` |
| `glom` | 无 | `JavaDStream[JList[T]]` | 将每个RDD分区合并为List | - |
| `slice` | Duration fromTime, Duration toTime | `List[JavaRDD[T]]` | 获取时间范围内的RDD列表 | `List<JavaRDD<String>> rdds = dstream.slice(Durations.seconds(10), Durations.seconds(20));` |
| `window` | Duration windowDuration | `JavaDStream[T]` | 窗口操作 | `JavaDStream<String> windowed = dstream.window(Durations.seconds(30));  // 30秒窗口` |
| `window` | Duration windowDuration, Duration slideDuration | `JavaDStream[T]` | 窗口操作，指定滑动间隔 | `JavaDStream<String> windowed = dstream.window(Durations.seconds(30), Durations.seconds(10));  // 30秒窗口，每10秒滑动` |
| `reduceByWindow` | JFunction2[T, T, T] reduceFunc, Duration windowDuration, Duration slideDuration | `JavaDStream[T]` | 窗口聚合 | `JavaDStream<Integer> windowSum = numbers.reduceByWindow((a, b) -> a + b, Durations.seconds(30), Durations.seconds(10));` |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合 | `JavaPairDStream<String, Integer> windowCounts = pairs.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(30));` |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合，指定滑动 | - |
| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, JFunction2[V, V, V] invReduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合（带逆函数，高效） | `JavaPairDStream<String, Integer> counts = pairs.reduceByKeyAndWindow(<br>    (a, b) -> a + b,  // 加新数据<br>    (a, b) -> a - b,  // 减旧数据（高效计算）<br>    Durations.seconds(30), Durations.seconds(10));` |
| `countByWindow` | Duration windowDuration, Duration slideDuration | `JavaDStream[Long]` | 窗口内计数 | `JavaDStream<Long> counts = dstream.countByWindow(Durations.seconds(30), Durations.seconds(10));` |
| `countByValueAndWindow` | Duration windowDuration, Duration slideDuration | `JavaPairDStream[T, Long]` | 窗口内按值计数 | - |
| `checkpoint` | 无 | `JavaDStream[T]` | 启用checkpoint | `dstream.checkpoint();` |
| `persist` | StorageLevel level | `JavaDStream[T]` | 持久化DStream | `dstream.persist(StorageLevel.MEMORY_ONLY());` |
| `cache` | 无 | `JavaDStream[T]` | 缓存DStream | `dstream.cache();` |
| `print` | 无 | `Unit` | 打印每个RDD的前10元素 | `dstream.print();` |
| `saveAsTextFiles` | String prefix, String suffix | `Unit` | 保存为文本文件序列 | `dstream.saveAsTextFiles("output/stream", "txt");  // 生成output/stream-TIME.txt` |
| `saveAsObjectFiles` | String prefix, String suffix | `Unit` | 保存为对象文件序列 | `dstream.saveAsObjectFiles("output/stream", "obj");` |

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
| `cogroup` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[JIterable[V], JIterable[W]]]` | 共同分组 | - |
| `updateStateByKey` | JFunction2[JList[V], Optional[S], Optional[S]] updateFunc | `JavaPairDStream[K, S]` | 更新状态（带状态计算） | `JavaPairDStream<String, Integer> stateCounts = wordCounts.updateStateByKey((values, state) -> {<br>    int sum = state.orElse(0);<br>    for (int v : values) sum += v;<br>    return Optional.of(sum);<br>});` |
| `mapWithState` | StateSpec[K, V, S, M] spec | `JavaMapWithStateDStream[K, V, S, M]` | 高效状态更新 | - |

---

## MLlib机器学习算法API

### KMeans / KMeansModel
**包路径**: `org.apache.spark.mllib.clustering`
**说明**: K-Means聚类算法和模型。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations | `KMeansModel` | 训练K-Means模型 | `JavaRDD<Vector> data = vectorsRDD;<br>KMeansModel model = KMeans.train(data.rdd(), 3, 20);  // 3个簇，20次迭代` |
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
| `LogisticRegressionWithSGD.train` | JavaRDD[LabeledPoint] data, int numIterations | `LogisticRegressionModel` | SGD训练逻辑回归 | `JavaRDD<LabeledPoint> training = labeledRDD;<br>LogisticRegressionModel model = LogisticRegressionWithSGD.train(training.rdd(), 100);` |
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
| `fit` | JavaRDD[String] data | `Word2VecModel` | 训练词向量 | `JavaRDD<String> documents = sc.parallelize(Arrays.asList("hello world", "spark java"));<br>Word2VecModel model = w2v.fit(documents);` |
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
| `run` | JavaRDD[String[]] data | `FPGrowthModel` | 运行FP-Growth | `JavaRDD<String[]> transactions = sc.parallelize(Arrays.asList(<br>    new String[]{"a", "b", "c"},<br>    new String[]{"a", "b"}));<br>FPGrowthModel model = fpg.run(transitions.rdd());` |
| `freqItemsets` | 无 | `JavaRDD[FreqItemset]` | 获取频繁项集 | `JavaRDD<FreqItemset> itemsets = model.freqItemsets();<br>itemsets.foreach(item -> System.out.println(item.items() + ": " + item.freq()));` |

### AssociationRules
**包路径**: `org.apache.spark.mllib.fpm`
**说明**: 关联规则生成。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AssociationRules` | 无 | 构造方法 | 创建关联规则生成器 | `AssociationRules ar = new AssociationRules();` |
| `setMinConfidence` | double minConfidence | `AssociationRules` | 设置最小置信度 | `ar.setMinConfidence(0.5);  // 50%置信度` |
| `run` | JavaRDD[FreqItemset] freqItemsets | `JavaRDD[Rule]` | 生成关联规则 | `JavaRDD<Rule> rules = ar.run(fpgModel.freqItemsets().toJavaRDD());<br>rules.foreach(rule -> System.out.println(<br>    rule.antecedent() + " => " + rule.consequent() +<br>    ": confidence=" + rule.confidence()));` |

### BinaryClassificationMetrics
**包路径**: `org.apache.spark.mllib.evaluation`
**说明**: 二分类评估指标。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `BinaryClassificationMetrics` | JavaPairRDD[Double, Double] predictionAndLabels | 构造方法 | 创建评估器 | `JavaPairRDD<Double, Double> predictions = predictedLabelsRDD;<br>BinaryClassificationMetrics metrics = new BinaryClassificationMetrics(predictions.rdd());` |
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
| `Vectors.dense` | double[] values | `Vector` | 创建密集向量（数组） | `double[] arr = {1.0, 2.0, 3.0};<br>Vector vec = Vectors.dense(arr);` |
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
'''

def add_streaming_mllib(filepath):
    """补充Streaming和MLlib API"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    content = content.rstrip() + '\n' + STREAMING_MLLIB_API + '\n'
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return True

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充Streaming和MLlib算法API...")
    success = add_streaming_mllib(filepath)
    
    if success:
        print("成功补充:")
        print("  - Streaming: JavaStreamingContext, JavaDStream, JavaPairDStream")
        print("  - MLlib聚类: KMeans, BisectingKMeans, LDA")
        print("  - MLlib分类: LogisticRegression, SVM, NaiveBayes")
        print("  - MLlib回归: LinearRegression")
        print("  - MLlib推荐: ALS")
        print("  - MLlib特征: PCA, StandardScaler, Normalizer, Word2Vec")
        print("  - MLlib挖掘: FPGrowth, AssociationRules")
        print("  - MLlib评估: BinaryClassificationMetrics, MulticlassMetrics, RegressionMetrics")
        print("  - MLlib向量/矩阵: Vectors, Matrices")