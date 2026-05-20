#!/usr/bin/env python3
"""
补充空白示例
"""

BLANK_EXAMPLES = {
    "| `call` | T1 t1, T2 t2, T3 t3, T4 t4 | `R` | 调用函数 | - |": "| `call` | T1 t1, T2 t2, T3 t3, T4 t4 | `R` | 调用函数 | `public R call(T1 t1, T2 t2, T3 t3, T4 t4) { return func(t1, t2, t3, t4); }` |",
    "| `call` | T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 | `R` | 调用函数 | - |": "| `call` | T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 | `R` | 调用函数 | `public R call(T1 t1, T2 t2, T3 t3, T4 t4, T5 t5) { return func(t1, t2, t3, t4, t5); }` |",
    "| `reduceAgg` | Column e | `Row` | 聚合reduce | - |": "| `reduceAgg` | Column e | `Row` | 聚合reduce | `Row result = ds.reduceAgg(col("value"));` |",
    "| `aggByAddr` | Column... exprs | `Dataset[Row]` | 按地址聚合 | - |": "| `aggByAddr` | Column... exprs | `Dataset[Row]` | 按地址聚合 | `Dataset<Row> result = ds.aggByAddr(sum("value"), count("*"));` |",
    "| `register` | String name, UDF4[T1, T2, T3, T4, R] f, DataType returnType | `void` | 注册UDF（4个参数） | - |": "| `register` | String name, UDF4[T1, T2, T3, T4, R] f, DataType returnType | `void` | 注册UDF（4个参数） | `spark.udf().register("myUDF", new MyUDF4(), DataTypes.IntegerType);` |",
    "| `register` | String name, UDF5... | `void` | 注册UDF（5+参数） | - |": "| `register` | String name, UDF5... | `void` | 注册UDF（5+参数） | `spark.udf().register("myUDF5", new MyUDF5(), DataTypes.StringType);` |",
    "| `register` | String name, UserDefinedAggregateFunction udaf | `void` | 注册聚合UDF（旧API） | - |": "| `register` | String name, UserDefinedAggregateFunction udaf | `void` | 注册聚合UDF（旧API） | `spark.udf().register("myUDAF", new MyUDAF());` |",
    "| `registerPython` | String name, String command, DataType returnType | `void` | 注册Python UDF | - |": "| `registerPython` | String name, String command, DataType returnType | `void` | 注册Python UDF | `spark.udf().registerPython("pyFunc", "python_code", DataTypes.StringType);` |",
    "| `rawSocketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 原始socket流 | - |": "| `rawSocketStream` | String hostname, int port, StorageLevel storageLevel | `JavaReceiverInputDStream[String]` | 原始socket流 | `JavaReceiverInputDStream<String> stream = jssc.rawSocketStream("localhost", 9999, StorageLevel.MEMORY_ONLY());` |",
    "| `transformWith` | JavaDStream[T] dstream1, JavaDStream[W] dstream2, JFunction2[JavaRDD[T], JavaRDD[W], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对两个DStream每个RDD应用变换 | - |": "| `transformWith` | JavaDStream[T] dstream1, JavaDStream[W] dstream2, JFunction2[JavaRDD[T], JavaRDD[W], JavaRDD[U]] transformFunc | `JavaDStream[U]` | 对两个DStream每个RDD应用变换 | `JavaDStream<String> result = dstream1.transformWith(dstream2, (rdd1, rdd2) -> rdd1.union(rdd2));` |",
    "| `flatMapValues` | FlatMapFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value扁平映射 | - |": "| `flatMapValues` | FlatMapFunction[V, U] f | `JavaPairDStream[K, U]` | 对Value扁平映射 | `JavaPairDStream<String, Integer> result = pairDStream.flatMapValues(v -> Arrays.asList(v, v*2).iterator());` |",
    "| `transformToPair` | JFunction[JavaRDD[T], JavaPairRDD[K, V]] transformFunc | `JavaPairDStream[K, V]` | 对每个RDD变换为PairRDD | - |": "| `transformToPair` | JFunction[JavaRDD[T], JavaPairRDD[K, V]] transformFunc | `JavaPairDStream[K, V]` | 对每个RDD变换为PairRDD | `JavaPairDStream<String, Integer> result = dstream.transformToPair(rdd -> rdd.mapToPair(x -> new Tuple2<>(x, 1)));` |",
    "| `glom` | 无 | `JavaDStream[JList[T]]` | 将每个RDD分区合并为List | - |": "| `glom` | 无 | `JavaDStream[JList[T]]` | 将每个RDD分区合并为List | `JavaDStream<List<String>> partitioned = dstream.glom();` |",
    "| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合，指定滑动 | - |": "| `reduceByKeyAndWindow` | JFunction2[V, V, V] reduceFunc, Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, V]` | 窗口内按Key聚合，指定滑动 | `JavaPairDStream<String, Integer> result = pairDStream.reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(10), Durations.seconds(2));` |",
    "| `countByValueAndWindow` | Duration windowDuration, Duration slideDuration | `JavaPairDStream[T, Long]` | 窗口内按值计数 | - |": "| `countByValueAndWindow` | Duration windowDuration, Duration slideDuration | `JavaPairDStream[T, Long]` | 窗口内按值计数 | `JavaPairDStream<String, Long> result = dstream.countByValueAndWindow(Durations.seconds(10), Durations.seconds(2));` |",
    "| `fullOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[Optional[V], Optional[W]]]` | 全外连接 | - |": "| `fullOuterJoin` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[Optional[V], Optional[W]]]` | 全外连接 | `JavaPairDStream<String, Tuple2<Optional<Integer>, Optional<Integer>>> result = pairDStream1.fullOuterJoin(pairDStream2);` |",
    "| `cogroup` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[JIterable[V], JIterable[W]]]` | 共同分组 | - |": "| `cogroup` | JavaPairDStream[K, W] other | `JavaPairDStream[K, Tuple2[JIterable[V], JIterable[W]]]` | 共同分组 | `JavaPairDStream<String, Tuple2<Iterable<Integer>, Iterable<Integer>>> result = pairDStream1.cogroup(pairDStream2);` |",
    "| `mapWithState` | StateSpec[K, V, S, M] spec | `JavaMapWithStateDStream[K, V, S, M]` | 高效状态更新 | - |": "| `mapWithState` | StateSpec[K, V, S, M] spec | `JavaMapWithStateDStream[K, V, S, M]` | 高效状态更新 | `JavaMapWithStateDStream<String, Integer, Integer, String> stateStream = pairDStream.mapWithState(StateSpec.function(stateFunc));` |",
    "| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs | `KMeansModel` | 训练模型，多次运行 | - |": "| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs | `KMeansModel` | 训练模型，多次运行 | `KMeansModel model = KMeans.train(data, 10, 20, 1);` |",
    "| `LogisticRegressionWithSGD.train` | ... int regParam, int miniBatchFraction | `LogisticRegressionModel` | 指定正则化和批次比例 | - |": "| `LogisticRegressionWithSGD.train` | ... int regParam, int miniBatchFraction | `LogisticRegressionModel` | 指定正则化和批次比例 | `LogisticRegressionModel model = LogisticRegressionWithSGD.train(data, 100, 0.01, 1.0);` |",
    "| `predictAll` | JavaRDD[Tuple2[Int, Int]] usersProducts | `JavaRDD[Rating]` | 预测所有（同predict） | - |": "| `predictAll` | JavaRDD[Tuple2[Int, Int]] usersProducts | `JavaRDD[Rating]` | 预测所有（同predict） | `JavaRDD<Rating> predictions = alsModel.predictAll(userProductPairs);` |",
    "| `precisionByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的精确率 | - |": "| `precisionByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的精确率 | `JavaRDD<Tuple2<Double, Double>> precision = metrics.precisionByThreshold();` |",
    "| `recallByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的召回率 | - |": "| `recallByThreshold` | 无 | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的召回率 | `JavaRDD<Tuple2<Double, Double>> recall = metrics.recallByThreshold();` |",
    "| `fMeasureByThreshold` | double beta | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的F值 | - |": "| `fMeasureByThreshold` | double beta | `JavaRDD[Tuple2[Double, Double]]` | 各阈值的F值 | `JavaRDD<Tuple2<Double, Double>> f1 = metrics.fMeasureByThreshold(1.0);` |",
    "| `Vectors.sparse` | int size, Iterable[Tuple2[Int, Double]] entries | `Vector` | 创建稀疏向量（迭代器） | - |": "| `Vectors.sparse` | int size, Iterable[Tuple2[Int, Double]] entries | `Vector` | 创建稀疏向量（迭代器） | `Vector sparse = Vectors.sparse(10, Arrays.asList(new Tuple2<>(0, 1.0), new Tuple2<>(5, 2.0)));` |",
    "| `Matrices.sparse` | int numRows, int numCols, int[] colPtrs, int[] rowIndices, double[] values | `Matrix` | 创建稀疏矩阵（CSC格式） | - |": "| `Matrices.sparse` | int numRows, int numCols, int[] colPtrs, int[] rowIndices, double[] values | `Matrix` | 创建稀疏矩阵（CSC格式） | `Matrix sparse = Matrices.sparse(3, 2, new int[]{0, 1, 3}, new int[]{0, 1, 2}, new double[]{1.0, 2.0, 3.0});` |",
}

def fix_blank_examples(filepath):
    """补充空白示例"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    count = 0
    for old, new in BLANK_EXAMPLES.items():
        if old in content:
            content = content.replace(old, new)
            count += 1
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充空白示例...")
    count = fix_blank_examples(filepath)
    print(f"  补充 {count} 个示例")
    print("\n完成")
