#!/usr/bin/env python3
"""
基于高质量完整文档，直接优化生成用户文档：
1. 添加快速入门
2. 移除开发者API（Connector/内部实现类）
3. 为核心类添加导入示例
"""

import re

# 需要移除的开发者API类（数据源/插件开发者使用）
REMOVE_CLASSES = [
    # Connector相关
    'ColumnVector',
    'ColumnarBatch',
    'CustomAvgMetric',
    'CustomSumMetric', 
    'CustomTaskMetric',
    'Distributions',
    'Expressions',
    'Extract',
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
    'GetPrimaryKeysOperation',
    'TimerWithCustomTimeUnit',
    'AvroCompressionCodec',
    'Check',
    'CaseInsensitiveStringMap',
    'BitmapExpressionUtils',
    'BufferedRowIterator',
    'CartesianSpatialReferenceSystemMapper',
    'Cast',
    'ChangelogInfo',
    'CharVarcharCodegenUtils',
    'CollationAwareUTF8String',
    'ColumnDefaultValue',
    'ColumnVectorUtils',
    'ConstantColumnVector',
    'DelegatingCatalogExtension',
    'DefaultValue',
    'AggregateHashMap',
    'ArrayOfDecimalsSerDe',
    'ArrowColumnVector',
    # 内部实现
    'AbstractAuthRpcHandler',
    'AbstractFetchShuffleBlocks',
    'AbstractFileRegion',
    'AbstractLauncher',
    'AbstractMessage',
    'AbstractService',
    'AmIpFilter',
    'AmIpPrincipal',
    'ApplicationStatus',
    'AuthClientBootstrap',
    'AuthMethods',
    'AuthServerBootstrap',
    'BestEffortLazyVal',
    'BlockPushNonFatalFailure',
    'BlockPushReturnCode',
    'BlockStoreClient',
    'BlockTransferMessage',
    'BlocksRemoved',
    'BloomFilter',
    'BreakableService',
    'ByteArrayMethods',
    'ByteArrayReadableChannel',
    'ByteArrayWritableChannel',
    'ByteBufferWriteableChannel',
    'ByteUnit',
    'CLIService',
    'CLIServiceClient',
    'ChildFirstURLClassLoader',
    'ChunkFetchRequestHandler',
    'ClassicTableTypeMapping',
    'CodePointIteratorType',
    'ColumnBasedSet',
    'ColumnDescriptor',
    'ColumnValue',
    'CompositeService',
    'ConfigProvider',
    'CookieSigner',
    'CorruptionCause',
    'CountMinSketch',
    'CtrTransportCipher',
    'DBBackend',
    'DBProvider',
    'DelegateSymlinkTextInputFormat',
    'DiagnoseCorruption',
    'EncryptedMessageWithHeader',
    'ErrorHandler',
    'ExecuteStatementOperation',
    'ExecutorShuffleInfo',
    'ExternalBlockHandler',
    'ExternalBlockStoreClient',
    'ExternalShuffleBlockResolver',
    'FetchOrientation',
    'FetchShuffleBlockChunks',
    'FetchShuffleBlocks',
    'FetchType',
    'FilterService',
    'FinalizeShuffleMerge',
    'GangliaReporter',
    'GcmTransportCipher',
    'GetCatalogsOperation',
    'GetColumnsOperation',
    'GetCrossReferenceOperation',
    'GetFunctionsOperation',
    'GetInfoType',
    'GetInfoValue',
    'GetLocalDirsForExecutors',
    'GetPrimaryKeysOperation',
    'GetSchemasOperation',
    'GetTableTypesOperation',
    'GetTablesOperation',
    'GetTypeInfoOperation',
    'HadoopConfigProvider',
    'Handle',
    'HandleIdentifier',
    'HashMapGrowthStrategy',
    'HeapMemoryAllocator',
    'HiveAuthFactory',
    'HiveSQLException',
    'HiveServer2',
    'HiveSessionImplwithUGI',
    'HiveSessionProxy',
    'HiveTableTypeMapping',
    'InMemoryStore',
    'InProcessLauncher',
    'JavaModuleOptions',
    'JobExecutionStatus',
    'KVStoreView',
    'KVTypeInfo',
    'LevelDB',
    'LevelDBIterator',
    'LevelDBProvider',
    'LocalDirsForExecutors',
    'LocalDiskShuffleDataIO',
    'LocalDiskShuffleDriverComponents',
    'LocalDiskShuffleExecutorComponents',
    'LocalDiskShuffleMapOutputWriter',
    'LocalDiskSingleSpillMapOutputWriter',
    'LogDivertAppender',
    'MapConfigProvider',
    'MemoryBlock',
    'MemoryConsumer',
    'MemoryLocation',
    'MergeStatuses',
    'MergedBlockMeta',
    'MergedBlockMetaRequest',
    'MergedBlockMetaSuccess',
    'Message',
    'MessageWithHeader',
    'MetadataOperation',
    'MutableURLClassLoader',
    'MyLauncher',
    'NettyLogger',
    'NettyManagedBuffer',
    'NettyMemoryMetrics',
    'NioManagedBuffer',
    'NoOpMergedShuffleFileManager',
    'NoOpRpcHandler',
    'OneForOneBlockFetcher',
    'OneForOneBlockPusher',
    'OneForOneStreamManager',
    'OpenBlocks',
    'Operation',
    'OperationHandle',
    'OperationManager',
    'OperationState',
    'OperationStatus',
    'OperationType',
    'ParentClassLoader',
    'PlainSaslServer',
    'PrefixComparators',
    'PushBlockStream',
    'RadixSort',
    'ReadAheadInputStream',
    'RegisterExecutor',
    'RemoteBlockPushResolver',
    'RemoveBlocks',
    'RemoveShuffleMerge',
    'RetryingBlockTransferor',
    'RocksDB',
    'RocksDBIterator',
    'RocksDBProvider',
    'RowBasedSet',
    'RowSetFactory',
    'RpcHandler',
    'SSLFactory',
    'SaslClientBootstrap',
    'SaslQOP',
    'SaslRpcHandler',
    'SaslServerBootstrap',
    'SessionHandle',
    'SessionManager',
    'ShuffleIndexInformation',
    'ShuffleSecretManager',
    'ShuffleTransportContext',
    'SimpleDownloadFile',
    'SparkAppHandle',
    'SparkFirehoseListener',
    'SparkGenericUDAFBridge',
    'SparkOrcNewRecordReader',
    'SparkSaslClient',
    'SparkSaslServer',
    'StageStatus',
    'StorageLevels',
    'StreamHandle',
    'StreamInterceptor',
    'StreamManager',
    'TServlet',
    'TSetIpAddressProcessor',
    'TSubjectAssumingTransport',
    'TableSchema',
    'TableTypeMappingFactory',
    'TaskMemoryManager',
    'TaskSorting',
    'TaskStatus',
    'ThreadFactoryWithGarbageCleanup',
    'ThreadWithGarbageCleanup',
    'ThriftBinaryCLIService',
    'ThriftCLIService',
    'ThriftCLIServiceClient',
    'ThriftHttpCLIService',
    'ThriftHttpServlet',
    'TimerWithCustomTimeUnit',
    'TransientBestEffortLazyVal',
    'TransportChannelHandler',
    'TransportClient',
    'TransportClientFactory',
    'TransportConf',
    'TransportContext',
    'TransportFrameDecoder',
    'TransportRequestHandler',
    'TransportResponseHandler',
    'TransportServer',
    'TypeDescriptor',
    'TypeQualifiers',
    'UTF8StringBuilder',
    'UnsafeAlignedOffset',
    'UnsafeMemoryAllocator',
    'UnsafeShuffleWriter',
    'UploadBlock',
    'UploadBlockStream',
    'VariantBuilder',
    'VariantSchema',
    'VariantShreddingWriter',
    'VariantUtil',
    'VariantVal',
    'YarnShuffleService',
    'instead',
    'StorageLevelMapper',
]

# 快速入门内容
QUICK_START = '''# Spark Java API 用户文档

> **文档定位**: 仅包含用户直接调用的public API（约2200+方法）
> **开发者API**: 已移除Connector/插件开发接口，普通用户不需要

---

## 快速入门

### 1. RDD完整示例

```java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

public class RDDExample {
    public static void main(String[] args) {
        // 创建SparkContext
        SparkConf conf = new SparkConf()
            .setAppName("RDD Example")
            .setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        // 读取数据
        JavaRDD<String> lines = sc.textFile("data.txt");
        
        // 转换：过滤长度>10的行
        JavaRDD<String> filtered = lines.filter(
            s -> s.length() > 10
        );
        
        // 行动：计数
        long count = filtered.count();
        System.out.println("Count: " + count);
        
        sc.stop();
    }
}
```

### 2. DataFrame完整示例

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import static org.apache.spark.sql.functions.*;

public class DataFrameExample {
    public static void main(String[] args) {
        // 创建SparkSession
        SparkSession spark = SparkSession.builder()
            .appName("DataFrame Example")
            .master("local[*]")
            .getOrCreate();
        
        // 读取CSV
        Dataset<Row> df = spark.read()
            .option("header", "true")
            .csv("data.csv");
        
        // SQL操作
        Dataset<Row> result = df
            .filter(col("age").gt(18))
            .groupBy("city")
            .agg(count("id").as("count"));
        
        result.show();
        spark.stop();
    }
}
```

### 3. Streaming完整示例

```java
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Durations;
import org.apache.spark.api.java.JavaSparkContext;

public class StreamingExample {
    public static void main(String[] args) throws InterruptedException {
        JavaSparkContext sc = ...;
        
        // 创建StreamingContext，每5秒一个批次
        JavaStreamingContext jssc = new JavaStreamingContext(
            sc, Durations.seconds(5)
        );
        
        // 监控目录中的新文件
        JavaDStream<String> lines = jssc.textFileStream("hdfs://logs/");
        
        // 处理：统计词频
        lines.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
             .mapToPair(word -> new Tuple2<>(word, 1))
             .reduceByKey((a, b) -> a + b)
             .print();
        
        jssc.start();
        jssc.awaitTermination();
    }
}
```

### 4. 机器学习完整示例

```java
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class MLlibExample {
    public static void main(String[] args) {
        JavaSparkContext sc = ...;
        
        // 准备数据
        JavaRDD<Vector> data = sc.parallelize(Arrays.asList(
            Vectors.dense(1.0, 2.0),
            Vectors.dense(3.0, 4.0),
            Vectors.dense(5.0, 6.0)
        ));
        
        // 训练KMeans（3个簇，20次迭代）
        KMeansModel model = KMeans.train(data.rdd(), 3, 20);
        
        // 预测
        int cluster = model.predict(Vectors.dense(2.0, 3.0));
        System.out.println("Cluster: " + cluster);
        
        sc.stop();
    }
}
```

---

## 核心导入速查

```java
// RDD Core
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.SparkConf;

// SQL Core
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

// Streaming
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.Durations;

// MLlib
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.recommendation.ALS;

// 共享变量
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
```

---

'''

def remove_developer_classes(content):
    """移除开发者API类"""
    for class_name in REMOVE_CLASSES:
        # 匹配类块：从### 开始到下一个### 或 ---
        pattern = rf'(### {class_name}\n.*?)(?=### |\n---|\n## |$)'
        content = re.sub(pattern, '', content, flags=re.DOTALL)
    
    # 清理多余空行
    content = re.sub(r'\n{4,}', '\n\n\n', content)
    
    return content

def add_quick_start(content):
    """替换文档开头，添加快速入门"""
    # 找到第一个###之前的内容
    pattern = r'^.*?(?=### )'
    match = re.search(pattern, content, re.DOTALL | re.MULTILINE)
    
    if match:
        # 替换开头内容
        content = QUICK_START + content[match.end():]
    
    return content

def main():
    input_file = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    output_file = '/home/h00517772/spark/hyx/spark_java_api_用户文档.md'
    
    print("读取高质量完整文档...")
    with open(input_file, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 统计原始类数量
    original_classes = len(re.findall(r'^### ', content, re.MULTILINE))
    print(f"原始类数量: {original_classes}")
    
    print("\n1. 添加快速入门...")
    content = add_quick_start(content)
    
    print("2. 移除开发者API类...")
    content = remove_developer_classes(content)
    
    # 统计移除后类数量
    final_classes = len(re.findall(r'^### ', content, re.MULTILINE))
    removed = original_classes - final_classes
    print(f"移除类数量: {removed}")
    print(f"保留类数量: {final_classes}")
    
    print("\n写入用户文档...")
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    print(f"\n完成！输出: {output_file}")
    print(f"  - 快速入门: 4个完整示例")
    print(f"  - 移除开发者API: {removed}类")
    print(f"  - 保留用户API: {final_classes}类")

if __name__ == '__main__':
    main()