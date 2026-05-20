#!/usr/bin/env python3
"""
补充遗漏的方法和类 - 第五轮
"""
import re

# 补充JavaDoubleRDD方法
JAVA_DOUBLE_RDD = '''
| `fromRDD` | RDD[Double] rdd | `JavaDoubleRDD` | 从Scala RDD创建JavaDoubleRDD | `JavaDoubleRDD doubleRdd = JavaDoubleRDD.fromRDD(scalaRdd);` |
| `popStdev` | 无 | `double` | 总体标准差 | `double stdev = doubleRdd.popStdev();` |
| `popVariance` | 无 | `double` | 总体方差 | `double variance = doubleRdd.popVariance();` |
'''

# 补充functions内置函数
FUNCTIONS = '''
| `count_distinct` | Column... cols | `Column` | 唯一值计数（别名） | `long count = df.select(count_distinct(col("id"))).first().getLong(0);` |
| `array_size` | Column array | `Column` | 数组大小（别名） | `Column size = array_size(col("items"));` |
| `array_sort` | Column array | `Column` | 数组排序 | `Column sorted = array_sort(col("items"));` |
| `map_contains_key` | Column map, Column key | `Column` | 判断Map是否包含key | `Column contains = map_contains_key(col("data"), lit("key1"));` |
| `map_keys` | Column map | `Column` | 获取Map的所有key | `Column keys = map_keys(col("data"));` |
| `map_values` | Column map | `Column` | 获取Map的所有value | `Column values = map_values(col("data"));` |
| `typedLit` | T value, Encoder[T] encoder | `Column` | 类型化字面值 | `Column typed = typedLit(Arrays.asList(1, 2, 3), Encoders.INT());` |
| `spark_partition_id` | 无 | `Column` | 获取分区ID | `df.select(spark_partition_id().as("partition")).show();` |
| `input_file_name` | 无 | `Column` | 获取输入文件名 | `df.select(input_file_name().as("file")).show();` |
| `input_file_block_start` | 无 | `Column` | 获取文件块起始位置 | `df.select(input_file_block_start().as("start")).show();` |
| `input_file_block_length` | 无 | `Column` | 获取文件块长度 | `df.select(input_file_block_length().as("length")).show();` |
| `trunc` | Column date, String format | `Column` | 截断日期 | `Column truncated = trunc(col("date"), "month");` |
| `date_trunc` | String format, Column timestamp | `Column` | 截断时间戳 | `Column truncated = date_trunc("hour", col("timestamp"));` |
| `expr` | String str | `Column` | 解析SQL表达式 | `Column result = expr("col1 + col2 * 10");` |
| `format_number` | Column x, int d | `Column` | 格式化数字 | `Column formatted = format_number(col("value"), 2);` |
| `format_string` | String format, Column... cols | `Column` | 格式化字符串 | `Column formatted = format_string("%s: %d", col("name"), col("value"));` |
| `regexp_count` | Column str, Column regexp | `Column` | 正则匹配计数 | `Column count = regexp_count(col("text"), lit("[0-9]+"));` |
| `regexp_instr` | Column str, Column regexp | `Column` | 正则匹配位置 | `Column pos = regexp_instr(col("text"), lit("[0-9]+"));` |
| `regexp_like` | Column str, Column regexp | `Column` | 正则判断是否匹配 | `Column matched = regexp_like(col("text"), lit("^[A-Z]"));` |
| `isnull` | Column col | `Column` | 判断是否null | `Column isNull = isnull(col("value"));` |
| `isnotnull` | Column col | `Column` | 判断是否非null | `Column notNull = isnotnull(col("value"));` |
| `nvl2` | Column col1, Column col2, Column col3 | `Column` | NVL2函数 | `Column result = nvl2(col("a"), col("b"), col("c"));` |
| `greatest` | Column... cols | `Column` | 取最大值 | `Column max = greatest(col("a"), col("b"), col("c"));` |
| `least` | Column... cols | `Column` | 取最小值 | `Column min = least(col("a"), col("b"), col("c"));` |
| `case_when` | Column... branches | `Column` | CASE WHEN表达式 | `Column result = case_when(col("a").equalTo(1), lit("one"), col("a").equalTo(2), lit("two"), lit("other"));` |
'''

# 补充JavaPairDStream方法
JAVA_PAIR_DSTREAM = '''
| `compute` | Time time | `Option[RDD[(K, V)]]` | 计算指定时间的RDD | `Option<RDD<Tuple2<String, Integer>>> rdd = pairDStream.compute(time);` |
| `fromJavaDStream` | JavaDStream[(K, V)] dstream | `JavaPairDStream[K, V]` | 从JavaDStream创建 | `JavaPairDStream<String, Integer> pair = JavaPairDStream.fromJavaDStream(dstream);` |
| `groupByKeyAndWindow` | Duration windowDuration | `JavaPairDStream[K, JIterable[V]]` | 按窗口分组 | `JavaPairDStream<String, Iterable<Integer>> grouped = pairDStream.groupByKeyAndWindow(Durations.seconds(10));` |
| `groupByKeyAndWindow` | Duration windowDuration, Duration slideDuration | `JavaPairDStream[K, JIterable[V]]` | 按窗口分组（指定滑动间隔） | `JavaPairDStream<String, Iterable<Integer>> grouped = pairDStream.groupByKeyAndWindow(Durations.seconds(10), Durations.seconds(2));` |
| `saveAsHadoopFiles` | String prefix, String suffix | `Unit` | 保存为Hadoop文件 | `pairDStream.saveAsHadoopFiles("hdfs://output/", "txt");` |
| `saveAsNewAPIHadoopFiles` | String prefix, String suffix | `Unit` | 保存为新API Hadoop文件 | `pairDStream.saveAsNewAPIHadoopFiles("hdfs://output/", "txt");` |
| `toJavaDStream` | 无 | `JavaDStream[(K, V)]` | 转为JavaDStream | `JavaDStream<Tuple2<String, Integer>> dstream = pairDStream.toJavaDStream();` |
'''

# 新增MLlib特征工程类
MLLIB_FEATURES = '''
### HashingTF
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将文本词转换为固定大小的向量，使用哈希技巧。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `HashingTF` | 无 | 构造方法 | 创建HashingTF | `HashingTF hashingTF = new HashingTF();` |
| `setInputCol` | String value | `HashingTF` | 设置输入列名 | `hashingTF.setInputCol("words");` |
| `setOutputCol` | String value | `HashingTF` | 设置输出列名 | `hashingTF.setOutputCol("features");` |
| `setNumFeatures` | int value | `HashingTF` | 设置特征数量（默认2^18） | `hashingTF.setNumFeatures(10000);` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> result = hashingTF.transform(sentences);` |

---

### Tokenizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将文本分割为单词列表。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Tokenizer` | 无 | 构造方法 | 创建Tokenizer | `Tokenizer tokenizer = new Tokenizer();` |
| `setInputCol` | String value | `Tokenizer` | 设置输入列名 | `tokenizer.setInputCol("text");` |
| `setOutputCol` | String value | `Tokenizer` | 设置输出列名 | `tokenizer.setOutputCol("words");` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> words = tokenizer.transform(texts);` |

---

### StopWordsRemover
**包路径**: `org.apache.spark.ml.feature`
**说明**: 移除停用词（如"a", "the"等）。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StopWordsRemover` | 无 | 构造方法 | 创建StopWordsRemover | `StopWordsRemover remover = new StopWordsRemover();` |
| `setInputCol` | String value | `StopWordsRemover` | 设置输入列名 | `remover.setInputCol("words");` |
| `setOutputCol` | String value | `StopWordsRemover` | 设置输出列名 | `remover.setOutputCol("filtered");` |
| `setStopWords` | String[] stopWords | `StopWordsRemover` | 设置停用词列表 | `remover.setStopWords(new String[]{"a", "the", "is"});` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> filtered = remover.transform(words);` |

---

### IDF
**包路径**: `org.apache.spark.ml.feature`
**说明**: 计算词频-逆文档频率，衡量词的重要性。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `IDF` | 无 | 构造方法 | 创建IDF | `IDF idf = new IDF();` |
| `setInputCol` | String value | `IDF` | 设置输入列名 | `idf.setInputCol("features");` |
| `setOutputCol` | String value | `IDF` | 设置输出列名 | `idf.setOutputCol("idf_features");` |
| `fit` | Dataset<?> dataset | `IDFModel` | 训练IDF模型 | `IDFModel model = idf.fit(tfFeatures);` |
| `setMinDocFreq` | int value | `IDF` | 设置最小文档频率 | `idf.setMinDocFreq(3);` |

---

### Word2Vec
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将单词映射到向量空间，捕捉语义相似性。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Word2Vec` | 无 | 构造方法 | 创建Word2Vec | `Word2Vec word2Vec = new Word2Vec();` |
| `setInputCol` | String value | `Word2Vec` | 设置输入列名 | `word2Vec.setInputCol("words");` |
| `setOutputCol` | String value | `Word2Vec` | 设置输出列名 | `word2Vec.setOutputCol("vector");` |
| `setVectorSize` | int value | `Word2Vec` | 设置向量维度（默认100） | `word2Vec.setVectorSize(50);` |
| `setMinCount` | int value | `Word2Vec` | 设置最小出现次数（默认5） | `word2Vec.setMinCount(2);` |
| `setWindowSize` | int value | `Word2Vec` | 设置窗口大小（默认5） | `word2Vec.setWindowSize(10);` |
| `fit` | Dataset<?> dataset | `Word2VecModel` | 训练模型 | `Word2VecModel model = word2Vec.fit(sentences);` |
| `setMaxSentenceLength` | int value | `Word2Vec` | 设置最大句子长度 | `word2Vec.setMaxSentenceLength(1000);` |

---

### CountVectorizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将文本转换为词频向量。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `CountVectorizer` | 无 | 构造方法 | 创建CountVectorizer | `CountVectorizer cv = new CountVectorizer();` |
| `setInputCol` | String value | `CountVectorizer` | 设置输入列名 | `cv.setInputCol("words");` |
| `setOutputCol` | String value | `CountVectorizer` | 设置输出列名 | `cv.setOutputCol("features");` |
| `setVocabSize` | int value | `CountVectorizer` | 设置词汇表大小（默认2^18） | `cv.setVocabSize(1000);` |
| `setMinDF` | double value | `CountVectorizer` | 设置最小文档频率 | `cv.setMinDF(2.0);` |
| `setMinTF` | double value | `CountVectorizer` | 设置最小词频 | `cv.setMinTF(1.0);` |
| `fit` | Dataset<?> dataset | `CountVectorizerModel` | 训练模型 | `CountVectorizerModel model = cv.fit(sentences);` |
| `setBinary` | boolean value | `CountVectorizer` | 设置是否二进制输出 | `cv.setBinary(true);` |

---

### VectorAssembler
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将多列合并为单个向量列。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `VectorAssembler` | 无 | 构造方法 | 创建VectorAssembler | `VectorAssembler assembler = new VectorAssembler();` |
| `setInputCols` | String[] values | `VectorAssembler` | 设置输入列名 | `assembler.setInputCols(new String[]{"age", "income", "score"});` |
| `setOutputCol` | String value | `VectorAssembler` | 设置输出列名 | `assembler.setOutputCol("features");` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> assembled = assembler.transform(data);` |

---

### MinMaxScaler
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将向量列缩放到[0,1]范围。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `MinMaxScaler` | 无 | 构造方法 | 创建MinMaxScaler | `MinMaxScaler scaler = new MinMaxScaler();` |
| `setInputCol` | String value | `MinMaxScaler` | 设置输入列名 | `scaler.setInputCol("features");` |
| `setOutputCol` | String value | `MinMaxScaler` | 设置输出列名 | `scaler.setOutputCol("scaled");` |
| `setMin` | double value | `MinMaxScaler` | 设置最小值（默认0） | `scaler.setMin(0.0);` |
| `setMax` | double value | `MinMaxScaler` | 设置最大值（默认1） | `scaler.setMax(1.0);` |
| `fit` | Dataset<?> dataset | `MinMaxScalerModel` | 训练模型 | `MinMaxScalerModel model = scaler.fit(data);` |

---

### OneHotEncoder
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将分类特征转换为二进制向量。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `OneHotEncoder` | 无 | 构造方法 | 创建OneHotEncoder | `OneHotEncoder encoder = new OneHotEncoder();` |
| `setInputCol` | String value | `OneHotEncoder` | 设置输入列名 | `encoder.setInputCol("category");` |
| `setOutputCol` | String value | `OneHotEncoder` | 设置输出列名 | `encoder.setOutputCol("category_vec");` |
| `setDropLast` | boolean value | `OneHotEncoder` | 是否丢弃最后一个类别（默认true） | `encoder.setDropLast(false);` |
| `fit` | Dataset<?> dataset | `OneHotEncoderModel` | 训练模型 | `OneHotEncoderModel model = encoder.fit(data);` |

---

### Bucketizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将连续特征分桶为离散特征。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Bucketizer` | 无 | 构造方法 | 创建Bucketizer | `Bucketizer bucketizer = new Bucketizer();` |
| `setInputCol` | String value | `Bucketizer` | 设置输入列名 | `bucketizer.setInputCol("value");` |
| `setOutputCol` | String value | `Bucketizer` | 设置输出列名 | `bucketizer.setOutputCol("bucket");` |
| `setSplitsArray` | double[][] splitsArray | `Bucketizer` | 设置分桶边界 | `bucketizer.setSplitsArray(new double[][]{{0, 10, 20, 100}});` |

---

### StringIndexer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将字符串标签转换为数值索引（按频率排序）。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StringIndexer` | 无 | 构造方法 | 创建StringIndexer | `StringIndexer indexer = new StringIndexer();` |
| `setInputCol` | String value | `StringIndexer` | 设置输入列名 | `indexer.setInputCol("category");` |
| `setOutputCol` | String value | `StringIndexer` | 设置输出列名 | `indexer.setOutputCol("category_index");` |
| `setHandleInvalid` | String value | `StringIndexer` | 处理无效值方式 | `indexer.setHandleInvalid("keep");` |
| `fit` | Dataset<?> dataset | `StringIndexerModel` | 训练模型 | `StringIndexerModel model = indexer.fit(data);` |

---

'''

# 新增Evaluator类
EVALUATORS = '''
### BinaryClassificationEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 二分类评估器，计算AUC、PR等指标。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `BinaryClassificationEvaluator` | 无 | 构造方法 | 创建评估器 | `BinaryClassificationEvaluator evaluator = new BinaryClassificationEvaluator();` |
| `setLabelCol` | String value | `BinaryClassificationEvaluator` | 设置标签列名 | `evaluator.setLabelCol("label");` |
| `setRawPredictionCol` | String value | `BinaryClassificationEvaluator` | 设置原始预测列名 | `evaluator.setRawPredictionCol("rawPrediction");` |
| `setMetricName` | String value | `BinaryClassificationEvaluator` | 设置评估指标 | `evaluator.setMetricName("areaUnderROC");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double auc = evaluator.evaluate(predictions);` |
| `getMetricName` | 无 | `String` | 获取当前指标名 | `String metric = evaluator.getMetricName();` |

---

### MulticlassClassificationEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 多分类评估器，计算准确率、F1等指标。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `MulticlassClassificationEvaluator` | 无 | 构造方法 | 创建评估器 | `MulticlassClassificationEvaluator evaluator = new MulticlassClassificationEvaluator();` |
| `setLabelCol` | String value | `MulticlassClassificationEvaluator` | 设置标签列名 | `evaluator.setLabelCol("label");` |
| `setPredictionCol` | String value | `MulticlassClassificationEvaluator` | 设置预测列名 | `evaluator.setPredictionCol("prediction");` |
| `setMetricName` | String value | `MulticlassClassificationEvaluator` | 设置评估指标 | `evaluator.setMetricName("accuracy");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double accuracy = evaluator.evaluate(predictions);` |
| `getMetricName` | 无 | `String` | 获取当前指标名 | `String metric = evaluator.getMetricName();` |

---

### RegressionEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 回归评估器，计算RMSE、MAE等指标。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `RegressionEvaluator` | 无 | 构造方法 | 创建评估器 | `RegressionEvaluator evaluator = new RegressionEvaluator();` |
| `setLabelCol` | String value | `RegressionEvaluator` | 设置标签列名 | `evaluator.setLabelCol("label");` |
| `setPredictionCol` | String value | `RegressionEvaluator` | 设置预测列名 | `evaluator.setPredictionCol("prediction");` |
| `setMetricName` | String value | `RegressionEvaluator` | 设置评估指标 | `evaluator.setMetricName("rmse");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double rmse = evaluator.evaluate(predictions);` |
| `getMetricName` | 无 | `String` | 获取当前指标名 | `String metric = evaluator.getMetricName();` |

---

### ClusteringEvaluator
**包路径**: `org.apache.spark.ml.evaluation`
**说明**: 聚类评估器，计算轮廓系数等指标。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ClusteringEvaluator` | 无 | 构造方法 | 创建评估器 | `ClusteringEvaluator evaluator = new ClusteringEvaluator();` |
| `setPredictionCol` | String value | `ClusteringEvaluator` | 设置预测列名 | `evaluator.setPredictionCol("prediction");` |
| `setFeaturesCol` | String value | `ClusteringEvaluator` | 设置特征列名 | `evaluator.setFeaturesCol("features");` |
| `setMetricName` | String value | `ClusteringEvaluator` | 设置评估指标 | `evaluator.setMetricName("silhouette");` |
| `evaluate` | Dataset<?> dataset | `double` | 计算评估值 | `double silhouette = evaluator.evaluate(predictions);` |

---

'''

def add_supplements(filepath):
    """补充遗漏的方法和类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充JavaDoubleRDD
    if "### JavaDoubleRDD" in content:
        class_start = content.find("### JavaDoubleRDD")
        next_class = content.find("\n### ", class_start + 1)
        if next_class != -1:
            class_section = content[class_start:next_class]
            last_match = None
            for match in re.finditer(r'\| `[^`]+` \|.*?\n', class_section):
                last_match = match
            if last_match:
                insert_pos = class_start + last_match.end()
                content = content[:insert_pos] + JAVA_DOUBLE_RDD + "\n" + content[insert_pos:]
                added_count += JAVA_DOUBLE_RDD.count('| `')
                print(f"✅ JavaDoubleRDD: 补充 3 个方法")
    
    # 补充functions - 找到functions类
    if "### functions" in content or "### Functions" in content:
        func_start = content.find("### functions")
        if func_start == -1:
            func_start = content.find("### Functions")
        if func_start != -1:
            next_class = content.find("\n### ", func_start + 1)
            if next_class != -1:
                class_section = content[func_start:next_class]
                last_match = None
                for match in re.finditer(r'\| `[^`]+` \|.*?\n', class_section):
                    last_match = match
                if last_match:
                    insert_pos = func_start + last_match.end()
                    content = content[:insert_pos] + FUNCTIONS + "\n" + content[insert_pos:]
                    added_count += FUNCTIONS.count('| `')
                    print(f"✅ functions: 补充 25 个函数")
    
    # 补充JavaPairDStream
    if "### JavaPairDStream" in content:
        class_start = content.find("### JavaPairDStream")
        next_class = content.find("\n### ", class_start + 1)
        if next_class != -1:
            class_section = content[class_start:next_class]
            last_match = None
            for match in re.finditer(r'\| `[^`]+` \|.*?\n', class_section):
                last_match = match
            if last_match:
                insert_pos = class_start + last_match.end()
                content = content[:insert_pos] + JAVA_PAIR_DSTREAM + "\n" + content[insert_pos:]
                added_count += JAVA_PAIR_DSTREAM.count('| `')
                print(f"✅ JavaPairDStream: 补充 7 个方法")
    
    # 添加MLlib特征工程类 - 在PCA之后插入
    if "### PCA" in content:
        pca_pos = content.find("### PCA")
        next_class = content.find("\n### ", pca_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_FEATURES + content[insert_pos:]
            added_count += MLLIB_FEATURES.count('| `')
            print(f"✅ 添加MLlib特征工程类: 10个类")
    
    # 添加Evaluator类 - 在MLlib部分末尾插入
    if "### StandardScaler" in content:
        scaler_pos = content.find("### StandardScaler")
        next_class = content.find("\n### ", scaler_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + EVALUATORS + content[insert_pos:]
            added_count += EVALUATORS.count('| `')
            print(f"✅ 添加Evaluator评估器类: 4个类")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的方法和类（第五轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
