#!/usr/bin/env python3
"""
完整优化 Spark Java API 文档：
1. 分离用户API和开发者API
2. 优化参数类型显示
3. 去除HTML标签
4. 添加导入示例和快速入门
"""

import re

# 用户直接调用的核心API类
USER_API_CLASSES = {
    # Core RDD
    'JavaSparkContext',
    'JavaRDD',
    'JavaRDDLike',
    'JavaPairRDD',
    'JavaDoubleRDD',
    'JavaPairFlatMapRDD',
    
    # 配置
    'SparkConf',
    
    # 共享变量
    'Broadcast',
    'Accumulator',
    'LongAccumulator',
    'DoubleAccumulator',
    'CollectionAccumulator',
    
    # SQL核心
    'SparkSession',
    'Dataset',
    'DataFrame',
    'Column',
    'Row',
    'RowFactory',
    'StructType',
    'StructField',
    'DataTypes',
    
    # SQL读写
    'DataFrameReader',
    'DataFrameWriter',
    
    # SQL函数
    'functions',
    
    # SQL元数据
    'Catalog',
    'UDFRegistration',
    
    # Streaming
    'JavaStreamingContext',
    'JavaDStream',
    'JavaPairDStream',
    'JavaReceiverInputDStream',
    
    # MLlib算法
    'KMeans',
    'KMeansModel',
    'BisectingKMeans',
    'BisectingKMeansModel',
    'LDA',
    'LDAModel',
    'LogisticRegressionModel',
    'LogisticRegressionWithSGD',
    'SVMModel',
    'SVMWithSGD',
    'NaiveBayes',
    'NaiveBayesModel',
    'LinearRegressionModel',
    'LinearRegressionWithSGD',
    'ALS',
    'MatrixFactorizationModel',
    'FPGrowth',
    'FPGrowthModel',
    'AssociationRules',
    
    # MLlib特征
    'PCA',
    'PCAModel',
    'StandardScaler',
    'StandardScalerModel',
    'Normalizer',
    'Word2Vec',
    'Word2VecModel',
    
    # MLlib评估
    'BinaryClassificationMetrics',
    'MulticlassMetrics',
    'RegressionMetrics',
    
    # MLlib数据类型
    'Vectors',
    'Matrices',
    'LabeledPoint',
    'Rating',
    
    # 存储
    'StorageLevel',
    
    # Spark文件
    'SparkFiles',
    
    # 状态追踪
    'JavaSparkStatusTracker',
}

# 开发者API类（数据源/插件开发者）
DEVELOPER_API_CLASSES = {
    'ColumnVector',
    'ColumnarBatch',
    'CustomAvgMetric',
    'CustomSumMetric',
    'CustomTaskMetric',
    'Distributions',
    'Expressions',
    'ForeignKey',
    'GeneralScalarExpression',
    'GetArrayItem',
    'IdentityColumnSpec',
    'IntegerAdd',
    'KeyGroupedPartitioning',
    'NamespaceChange',
    'NumericHistogram',
    'PrimaryKey',
    'ProcedureParameter',
    'SortDirection',
    'SupportsPushDownJoin',
    'TableChange',
    'TableInfo',
    'Unique',
    'UnknownPartitioning',
    'UserDefinedAggregateFunc',
    'UserDefinedScalarFunc',
    'V2ExpressionSQLBuilder',
    'ViewChange',
    'ViewInfo',
    'WriteBuilder',
    'Check',
    'CaseInsensitiveStringMap',
    'GetPrimaryKeysOperation',
    'TimerWithCustomTimeUnit',
    'AvroCompressionCodec',
}

# 参数类型映射（优化显示）
TYPE_MAPPING = {
    'JFunction[T, R]': 'Function<T, R>',
    'JFunction[T, java.lang.Boolean]': 'Function<T, Boolean>',
    'JFunction2[T, T, T]': 'Function2<T, T, T>',
    'JFunction2[U, T, U]': 'Function2<U, T, U>',
    'JFunction2[U, U, U]': 'Function2<U, U, U>',
    'JFunction2[Integer, JIterator[T], JIterator[R]]': 'Function2<Integer, Iterator<T>, Iterator<R>>',
    'JFunction2[jl.Integer, JIterator[T], JIterator[R]]': 'Function2<Integer, Iterator<T>, Iterator<R>>',
    'JList[T]': 'List<T>',
    'JList[String]': 'List<String>',
    'JList[Integer]': 'List<Integer>',
    'JIterator[T]': 'Iterator<T>',
    'JIterable[T]': 'Iterable<T>',
    'JIterable[V]': 'Iterable<V>',
    'JMap[K, V]': 'Map<K, V>',
    'JMap[String, String]': 'Map<String, String>',
    'JMap[T, jl.Long]': 'Map<T, Long>',
    'JMap[String, jl.Long]': 'Map<String, Long>',
    'JMap[String, BoundedDouble]': 'Map<String, BoundedDouble>',
    'JMap[K, ResourceInformation]': 'Map<K, ResourceInformation>',
    'JMap[java.lang.Integer, JavaRDD[_]]': 'Map<Integer, JavaRDD<?>>',
    'JDouble': 'Double',
    'JIterable': 'Iterable',
    'jl.Long': 'Long',
    'jl.Integer': 'Integer',
    'jl.Double': 'Double',
    'java.lang.Boolean': 'Boolean',
    'java.lang.Long': 'Long',
    'java.lang.Integer': 'Integer',
    'java.lang.Double': 'Double',
    'util.List[String]': 'List<String>',
    'util.Set[String]': 'Set<String>',
    'util.Map[String, String]': 'Map<String, String>',
    'Unit': 'void',
}

# 导入示例模板
IMPORT_TEMPLATES = {
    'JavaSparkContext': '''```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

// 创建SparkContext
SparkConf conf = new SparkConf()
    .setAppName("MyApp")
    .setMaster("local[*]");
JavaSparkContext sc = new JavaSparkContext(conf);
```''',
    
    'JavaRDD': '''```java
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

// 从SparkContext获取
JavaRDD<String> rdd = sc.textFile("hdfs://data.txt");
```''',
    
    'JavaPairRDD': '''```java
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

// 从JavaRDD转换
JavaPairRDD<String, Integer> pairRDD = rdd.mapToPair(
    s -> new Tuple2<>(s, s.length())
);
```''',
    
    'SparkSession': '''```java
import org.apache.spark.sql.SparkSession;

// 创建SparkSession
SparkSession spark = SparkSession.builder()
    .appName("MyApp")
    .master("local[*]")
    .getOrCreate();
```''',
    
    'Dataset': '''```java
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

// 从SparkSession获取
Dataset<Row> df = spark.read().parquet("data.parquet");
```''',
    
    'JavaStreamingContext': '''```java
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaSparkContext;

// 创建StreamingContext
JavaStreamingContext jssc = new JavaStreamingContext(
    sc, Durations.seconds(5)
);
```''',
    
    'KMeans': '''```java
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.api.java.JavaRDD;

// 训练模型
JavaRDD<Vector> data = ...;
KMeansModel model = KMeans.train(data.rdd(), 3, 20);
```''',
    
    'ALS': '''```java
import org.apache.spark.mllib.recommendation.ALS;
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel;
import org.apache.spark.mllib.recommendation.Rating;
import org.apache.spark.api.java.JavaRDD;

// 训练推荐模型
JavaRDD<Rating> ratings = ...;
MatrixFactorizationModel model = ALS.train(ratings.rdd(), 10, 20);
```''',
}

# 快速入门模板
QUICK_START = '''# Spark Java API 用户文档

> **说明**: 本文档仅包含用户直接调用的public API，开发者API已分离到独立文档。

---

## 快速入门

### 1. RDD方式（Spark Core）

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class SparkRDDExample {
    public static void main(String[] args) {
        // 1. 创建SparkContext
        SparkConf conf = new SparkConf()
            .setAppName("RDD Example")
            .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 2. 读取数据
        JavaRDD<String> lines = sc.textFile("data.txt");
        
        // 3. 转换操作
        JavaRDD<String> filtered = lines.filter(
            new Function<String, Boolean>() {
                public Boolean call(String s) {
                    return s.length() > 10;
                }
            }
        );
        
        // 4. 行动操作
        long count = filtered.count();
        System.out.println("Count: " + count);
        
        // 5. 关闭
        sc.stop();
    }
}
```

### 2. DataFrame方式（Spark SQL）

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class SparkSQLExample {
    public static void main(String[] args) {
        // 1. 创建SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("SQL Example")
            .master("local[*]")
            .getOrCreate();
        
        // 2. 读取数据
        Dataset<Row> df = spark.read()
            .option("header", "true")
            .csv("data.csv");
        
        // 3. SQL操作
        Dataset<Row> result = df
            .filter(col("age").gt(18))
            .groupBy("city")
            .agg(count("id").as("count"));
        
        // 4. 显示结果
        result.show();
        
        // 5. 关闭
        spark.stop();
    }
}
```

### 3. 机器学习（MLlib）

```java
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkMLlibExample {
    public static void main(String[] args) {
        // 1. 初始化
        SparkConf conf = new SparkConf().setAppName("MLlib Example");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 2. 准备数据
        JavaRDD<Vector> data = sc.parallelize(Arrays.asList(
            Vectors.dense(1.0, 2.0),
            Vectors.dense(3.0, 4.0),
            Vectors.dense(5.0, 6.0)
        ));
        
        // 3. 训练模型
        KMeansModel model = KMeans.train(data.rdd(), 2, 10);
        
        // 4. 预测
        int cluster = model.predict(Vectors.dense(2.0, 3.0));
        System.out.println("Cluster: " + cluster);
        
        // 5. 关闭
        sc.stop();
    }
}
```

---

## 文档结构

'''

def clean_html_tags(text):
    """去除HTML标签，转换为代码块格式"""
    # 替换<br>为换行
    text = text.replace('<br>', '\n')
    # 替换<br/>为换行
    text = text.replace('<br/>', '\n')
    # 替换<br >为换行
    text = text.replace('<br >', '\n')
    
    # 清理多余空行
    while '\n\n\n' in text:
        text = text.replace('\n\n\n', '\n\n')
    
    return text

def optimize_param_type(text):
    """优化参数类型显示"""
    for old_type, new_type in TYPE_MAPPING.items():
        text = text.replace(old_type, new_type)
    return text

def add_import_example(class_name, content):
    """为类添加导入示例"""
    if class_name in IMPORT_TEMPLATES:
        import_block = IMPORT_TEMPLATES[class_name]
        # 在类的开头插入导入示例
        pattern = r'(### ' + class_name + r'\n\*\*包路径\*\*:.*?\n(?:\*\*说明\*\*:.*?\n)?(?:\*\*方法数量\*\*:.*?\n)?)'
        match = re.search(pattern, content)
        if match:
            insert_pos = match.end()
            content = content[:insert_pos] + '\n**导入示例**:\n' + import_block + '\n\n' + content[insert_pos:]
    return content

def optimize_example_table(text):
    """优化示例表格中的代码格式"""
    # 将表格中的示例转换为更清晰的格式
    lines = text.split('\n')
    result = []
    
    for line in lines:
        if '| `' in line and '|' in line:
            # 表格行
            parts = line.split('|')
            if len(parts) >= 6:
                # 最后一个部分是示例
                example = parts[-1].strip() if parts[-1].strip() else parts[-2].strip()
                # 清理HTML
                example = clean_html_tags(example)
                parts[-1] = ' ' + example + ' '
                line = '|'.join(parts)
        result.append(line)
    
    return '\n'.join(result)

def separate_documents(filepath):
    """分离用户API和开发者API文档"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 提取所有类块
    class_pattern = r'(### (\w+).*?)(?=### \w+|$)'
    classes = re.findall(class_pattern, content, re.DOTALL)
    
    user_api_content = QUICK_START
    developer_api_content = '''# Spark Java API 开发者文档

> **说明**: 本文档包含数据源开发者、插件开发者使用的public API。
> 普通用户通常不直接调用这些API。

---

## 文档结构

'''

    user_class_count = 0
    developer_class_count = 0
    
    for class_block, class_name in classes:
        # 判断是用户API还是开发者API
        is_user_api = False
        is_developer_api = False
        
        for user_class in USER_API_CLASSES:
            if class_name.startswith(user_class) or class_name == user_class:
                is_user_api = True
                break
        
        for dev_class in DEVELOPER_API_CLASSES:
            if class_name == dev_class or class_name.startswith(dev_class):
                is_developer_api = True
                break
        
        # 优化内容
        optimized_block = optimize_param_type(class_block)
        optimized_block = clean_html_tags(optimized_block)
        optimized_block = add_import_example(class_name, optimized_block)
        
        # 分类
        if is_user_api:
            user_api_content += optimized_block + '\n'
            user_class_count += 1
        elif is_developer_api:
            developer_api_content += optimized_block + '\n'
            developer_class_count += 1
        else:
            # 未知类，默认放入用户API
            user_api_content += optimized_block + '\n'
            user_class_count += 1
    
    return user_api_content, developer_api_content, user_class_count, developer_class_count

def main():
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("开始优化文档...")
    user_api, developer_api, user_count, dev_count = separate_documents(filepath)
    
    # 写入用户API文档
    user_filepath = '/home/h00517772/spark/hyx/spark_java_api_用户文档.md'
    with open(user_filepath, 'w', encoding='utf-8') as f:
        f.write(user_api)
    
    # 写入开发者API文档
    dev_filepath = '/home/h00517772/spark/hyx/spark_java_api_开发者文档.md'
    with open(dev_filepath, 'w', encoding='utf-8') as f:
        f.write(developer_api)
    
    print(f"\n优化完成:")
    print(f"  - 用户API文档: {user_filepath}")
    print(f"    类数量: {user_count}")
    print(f"  - 开发者API文档: {dev_filepath}")
    print(f"    类数量: {dev_count}")
    print(f"\n优化内容:")
    print(f"  ✓ 分离用户API和开发者API")
    print(f"  ✓ 优化参数类型显示 (JFunction → Function<T,R>)")
    print(f"  ✓ 去除HTML标签 (<br> → 换行)")
    print(f"  ✓ 添加导入示例和快速入门")

if __name__ == '__main__':
    main()