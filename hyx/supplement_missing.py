#!/usr/bin/env python3
"""
补充高质量完整文档中遗漏的方法
"""
import re

# 需要补充的方法
MISSING_METHODS = '''
### JavaSparkContext 补充方法

以下方法在原文档中遗漏，现在补充：

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultMinPartitions` | 无 | `Integer` | 获取默认最小分区数，用于读取HDFS文件 | `int minParts = sc.defaultMinPartitions();<br>// 通常由spark.default.parallelism决定` |
| `defaultParallelism` | 无 | `Integer` | 获取默认并行度，用于shuffle和reduce操作 | `int parallelism = sc.defaultParallelism();<br>// 返回总核心数或spark.default.parallelism配置` |
| `emptyRDD` | 无 | `JavaRDD[T]` | 创建空RDD，用于初始化或测试 | `JavaRDD<String> empty = sc.emptyRDD();<br>// 创建类型安全的空RDD，用于union初始值` |
| `getCheckpointDir` | 无 | `Optional<String>` | 获取checkpoint目录路径 | `Optional<String> ckptDir = sc.getCheckpointDir();<br>if (ckptDir.isPresent()) {<br>    System.out.println("Checkpoint dir: " + ckptDir.get());<br>}` |
| `getPersistentRDDs` | 无 | `Map<Integer, JavaRDD<?>>` | 获取所有持久化的RDD及其ID | `Map<Integer, JavaRDD<?>> persisted = sc.getPersistentRDDs();<br>// 返回RDD ID到RDD的映射，用于监控缓存使用` |
| `getReadOnlyConf` | 无 | `ReadOnlySparkConf` | 获取只读配置，防止意外修改 | `ReadOnlySparkConf conf = sc.getReadOnlyConf();<br>// 只读配置，不能set修改` |
| `isLocal` | 无 | `Boolean` | 判断是否本地模式运行 | `boolean local = sc.isLocal();<br>// true表示local[*]或local[N]模式` |
| `jars` | 无 | `List<String>` | 获取所有添加的JAR包列表 | `List<String> jars = sc.jars();<br>// 返回通过addJar添加的所有JAR路径` |
| `resources` | 无 | `Map<String, ResourceInformation>` | 获取资源配置信息 | `Map<String, ResourceInformation> res = sc.resources();<br>// GPU/FPGA等资源分配信息` |
| `sparkUser` | 无 | `String` | 获取运行Spark的用户名 | `String user = sc.sparkUser();<br>// 返回启动Spark进程的系统用户` |
| `statusTracker` | 无 | `JavaSparkStatusTracker` | 获取作业状态追踪器 | `JavaSparkStatusTracker tracker = sc.statusTracker();<br>int activeJobs = tracker.getActiveJobsIds().length;<br>int pendingStages = tracker.getPendingStageIds().length;` |

---

### JavaStreamingContext 补充方法

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `remember` | Duration duration | `void` | 设置DStream数据保留时间，超过时间的数据会被清理 | `jssc.remember(Durations.minutes(30));<br>// 保留30分钟内的数据，用于状态更新` |
| `addStreamingListener` | StreamingListener listener | `void` | 添加流处理监听器，监控批次处理事件 | `jssc.addStreamingListener(new MyStreamingListener());<br>// 监听批次开始、完成、错误事件` |
| `binaryRecordsStream` | String directory, int recordLength | `JavaDStream<byte[]>` | 监控目录中的固定长度二进制文件流 | `JavaDStream<byte[]> stream = jssc.binaryRecordsStream("hdfs://data/", 100);<br>// 每条记录100字节的二进制流` |
| `receiverStream` | JavaReceiverInputDStream<T> receiver | `JavaDStream<T>` | 使用自定义Receiver创建DStream | `JavaDStream<String> customStream = jssc.receiverStream(new MyReceiver());<br>// 自定义数据接收器` |
| `getState` | 无 | `StreamingContextState` | 获取StreamingContext当前状态 | `StreamingContextState state = jssc.getState();<br>// INITIALIZED, ACTIVE, STOPPED等` |
'''

def add_missing_methods(filepath):
    """补充遗漏的方法"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 在JavaSparkContext类块之后插入补充方法
    pattern = r'(### JavaSparkContext\n.*?\*\*方法数量\*\*: \d+\n)'
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        # 提取补充内容中的JavaSparkContext部分
        jsc_supplement = '''
**补充说明**: 以下属性getter方法很重要但常被忽略：

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `defaultMinPartitions` | 无 | `Integer` | 默认最小分区数 | `int min = sc.defaultMinPartitions();` |
| `defaultParallelism` | 无 | `Integer` | 默认并行度 | `int para = sc.defaultParallelism();` |
| `emptyRDD` | 无 | `JavaRDD[T]` | 创建空RDD | `JavaRDD<String> empty = sc.emptyRDD();` |
| `getCheckpointDir` | 无 | `Optional<String>` | checkpoint目录 | `Optional<String> dir = sc.getCheckpointDir();` |
| `getPersistentRDDs` | 无 | `Map<Integer, JavaRDD<?>>` | 持久化RDD列表 | `Map<Integer, JavaRDD<?>> rdds = sc.getPersistentRDDs();` |
| `getReadOnlyConf` | 无 | `ReadOnlySparkConf` | 只读配置 | `ReadOnlySparkConf conf = sc.getReadOnlyConf();` |
| `isLocal` | 无 | `Boolean` | 是否本地模式 | `boolean local = sc.isLocal();` |
| `jars` | 无 | `List<String>` | JAR包列表 | `List<String> jars = sc.jars();` |
| `resources` | 无 | `Map<String, ResourceInformation>` | 资源配置 | `Map<String, ResourceInformation> res = sc.resources();` |
| `sparkUser` | 无 | `String` | Spark用户名 | `String user = sc.sparkUser();` |
| `statusTracker` | 无 | `JavaSparkStatusTracker` | 状态追踪器 | `JavaSparkStatusTracker tracker = sc.statusTracker();` |

'''
        insert_pos = match.end()
        content = content[:insert_pos] + jsc_supplement + content[insert_pos:]
    
    # 补充JavaStreamingContext
    pattern = r'(### JavaStreamingContext\n.*?\*\*方法数量\*\*: \d+\n)'
    match = re.search(pattern, content, re.DOTALL)
    
    if match:
        streaming_supplement = '''
**补充方法**:

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `remember` | Duration duration | `void` | 设置数据保留时间 | `jssc.remember(Durations.minutes(30));` |
| `addStreamingListener` | StreamingListener listener | `void` | 添加流监听器 | `jssc.addStreamingListener(new MyListener());` |
| `binaryRecordsStream` | String directory, int recordLength | `JavaDStream<byte[]>` | 固定长度二进制流 | `JavaDStream<byte[]> stream = jssc.binaryRecordsStream("hdfs://data/", 100);` |
| `receiverStream` | JavaReceiverInputDStream<T> receiver | `JavaDStream<T>` | 自定义Receiver | `JavaDStream<String> stream = jssc.receiverStream(new MyReceiver());` |
| `getState` | 无 | `StreamingContextState` | 获取状态 | `StreamingContextState state = jssc.getState();` |

'''
        insert_pos = match.end()
        content = content[:insert_pos] + streaming_supplement + content[insert_pos:]
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return True

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的方法...")
    success = add_missing_methods(filepath)
    
    if success:
        print("完成补充:")
        print("  - JavaSparkContext: 11个属性getter方法")
        print("  - JavaStreamingContext: 5个方法")