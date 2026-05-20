#!/usr/bin/env python3
"""
补充高质量完整文档中遗漏的方法 - 第二轮
"""
import re

# 补充方法定义
SUPPLEMENTS = {
    "JavaRDDLike": '''
| `mapPartitionsToPair` | FlatMapFunction[T, K, V] f | `JavaPairRDD[K, V]` | 对每个分区映射为键值对 | `JavaPairRDD<String, Integer> pairs = rdd.mapPartitionsToPair(iter -> {...});` |
| `wrapRDD` | RDD[T] rdd | `JavaRDD[T]` | 将Scala RDD包装为Java RDD | `JavaRDD<String> javaRdd = JavaRDD.fromRDD(scalaRdd);` |
''',
    "JavaPairRDD": '''
| `aggregateByKey` | U zeroValue, JFunction2[U, V, U] seqFunc, JFunction2[U, U, U] combFunc | `JavaPairRDD[K, U]` | 按Key聚合，支持不同类型 | `JavaPairRDD<String, Integer> result = pairRdd.aggregateByKey(0, (a, b) -> a + b, (a, b) -> a + b);` |
| `combineByKey` | JFunction[V, C] createCombiner, JFunction2[C, V, C] mergeValue, JFunction2[C, C, C] mergeCombiners | `JavaPairRDD[K, C]` | 通用组合函数 | `JavaPairRDD<String, Integer> combined = pairRdd.combineByKey(v -> v, (a, b) -> a + b, (a, b) -> a + b);` |
| `combineByKeyWithClassTag` | JFunction[V, C] createCombiner, JFunction2[C, V, C] mergeValue, JFunction2[C, C, C] mergeCombiners, ClassTag[C] ct | `JavaPairRDD[K, C]` | 带ClassTag的组合函数 | `JavaPairRDD<String, Integer> combined = pairRdd.combineByKeyWithClassTag(v -> v, (a, b) -> a + b, (a, b) -> a + b, ClassTag.apply(Integer.class));` |
| `subtractByKey` | JavaPairRDD[K, W] other | `JavaPairRDD[K, V]` | 减去other中存在的key | `JavaPairRDD<String, Integer> result = pairRdd.subtractByKey(otherRdd);` |
| `sampleStdevByKey` | K key | `double` | 按key采样标准差 | `double stdev = pairRdd.sampleStdevByKey("key1");` |
| `sampleVarianceByKey` | K key | `double` | 按key采样方差 | `double variance = pairRdd.sampleVarianceByKey("key1");` |
| `stdevByKey` | K key | `double` | 按key标准差 | `double stdev = pairRdd.stdevByKey("key1");` |
| `varianceByKey` | K key | `double` | 按key方差 | `double variance = pairRdd.varianceByKey("key1");` |
| `mapPartitionsByKey` | JFunction[Iterator[T], Iterator[U]] f | `JavaPairRDD[K, U]` | 按分区处理 | - |
| `flatMapValuesWithKey` | FlatMapFunction[K, V, U] f | `JavaPairRDD[K, U]` | 带key的flatMapValues | - |
''',
    "Dataset": '''
| `groupByCube` | Column... cols | `RelationalGroupedDataset` | 立方体分组（所有维度组合） | `RelationalGroupedDataset cube = ds.groupByCube("year", "month", "day");` |
| `groupByRollup` | Column... cols | `RelationalGroupedDataset` | 上卷分组（层级聚合） | `RelationalGroupedDataset rollup = ds.groupByRollup("year", "month");` |
| `unionAll` | Dataset[T] other | `Dataset[T]` | 联合所有（保留重复） | `Dataset<Row> union = ds1.unionAll(ds2);` |
| `dropDuplicatesWithinWatermark` | String... cols | `Dataset[T]` | 在watermark内去重 | `Dataset<Row> dedup = ds.dropDuplicatesWithinWatermark("id");` |
| `withColumnsRenamed` | Map<String, String> cols | `Dataset[T]` | 批量重命名列 | `Dataset<Row> renamed = ds.withColumnsRenamed(Map.of("old1", "new1", "old2", "new2"));` |
| `withWatermark` | String eventTime, String delayThreshold | `Dataset[T]` | 设置watermark用于流处理 | `Dataset<Row> withWm = ds.withWatermark("timestamp", "10 minutes");` |
| `hint` | String name, Object... params | `Dataset[T]` | 添加查询提示 | `Dataset<Row> hinted = ds.hint("broadcast");` |
| `writeToMetadata` | String tableName | `DataFrameWriter[T]` | 写入元数据表 | - |
| `saveAsParquetFile` | String path | `Unit` | 保存为Parquet（旧API） | `ds.saveAsParquetFile("hdfs://path/");` |
| `observe` | String name, Column expr, Column... exprs | `Dataset[T]` | 观察聚合指标 | `Dataset<Row> observed = ds.observe("metric", count("*").as("cnt"));` |
| `queryExecution` | 无 | `QueryExecution` | 获取查询执行计划 | `QueryExecution qe = ds.queryExecution();` |
| `isStreaming` | 无 | `boolean` | 是否流Dataset | `boolean streaming = ds.isStreaming();` |
| `toJavaRDD` | 无 | `JavaRDD[T]` | 转为Java RDD | `JavaRDD<Row> javaRdd = ds.toJavaRDD();` |
| `storageLevel` | 无 | `StorageLevel` | 获取存储级别 | `StorageLevel level = ds.storageLevel();` |
| `createOrReplaceGlobalTempView` | String viewName | `Unit` | 创建或替换全局临时视图 | `ds.createOrReplaceGlobalTempView("global_view");` |
| `toLocalIteratorAsList` | 无 | `List[T]` | 转为本地迭代器List | `List<Row> list = ds.toLocalIteratorAsList();` |
| `reduceAgg` | Column e | `Row` | 聚合reduce | - |
| `aggByAddr` | Column... exprs | `Dataset[Row]` | 按地址聚合 | - |
''',
}

def add_supplements(filepath):
    """补充遗漏的方法"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    for class_name, methods in SUPPLEMENTS.items():
        # 找到类的结束位置（下一个###之前）
        pattern = rf'(### {class_name}\n.*?\*\*方法数量\*\*: \d+\n.*?\| `[^\`]+` \|.*?\n)'
        
        # 简化：在方法表格末尾插入
        if f"### {class_name}" in content:
            # 找到该类的最后一个方法行
            class_start = content.find(f"### {class_name}")
            next_class_start = content.find("\n### ", class_start + 1)
            
            if next_class_start == -1:
                next_class_start = len(content)
            
            class_section = content[class_start:next_class_start]
            
            # 找到表格最后一行
            last_method_match = None
            for match in re.finditer(r'\| `[^`]+` \|.*?\n', class_section):
                last_method_match = match
            
            if last_method_match:
                insert_pos = class_start + last_method_match.end()
                content = content[:insert_pos] + methods + "\n" + content[insert_pos:]
                added_count += methods.count('| `')
                print(f"✅ {class_name}: 补充 {methods.count('| `')} 个方法")
            else:
                print(f"⚠️ {class_name}: 未找到方法表格")
        else:
            print(f"❌ {class_name}: 类不存在")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的方法（第二轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
