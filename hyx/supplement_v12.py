#!/usr/bin/env python3
"""
补充遗漏的Structured Streaming类 - 第十二轮
"""
import re

STREAMING_CLASSES = '''
### DataStreamReader
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming数据流读取器，从SparkSession.readStream()获取。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataStreamReader` | 设置数据源格式 | `reader.format("kafka");` |
| `option` | String key, String value | `DataStreamReader` | 设置选项 | `reader.option("kafka.bootstrap.servers", "localhost:9092");` |
| `option` | String key, boolean value | `DataStreamReader` | 设置布尔选项 | `reader.option("startingOffsets", "earliest");` |
| `options` | Map<String, String> options | `DataStreamReader` | 设置多个选项 | `reader.options(kafkaParams);` |
| `schema` | StructType schema | `DataStreamReader` | 设置schema（自定义格式） | `reader.schema(schema);` |
| `load` | 无 | `Dataset[Row]` | 加载流数据 | `Dataset<Row> kafkaStream = reader.load();` |
| `load` | String path | `Dataset[Row]` | 加载流数据（指定路径） | `Dataset<Row> jsonStream = reader.load("hdfs://stream/");` |
| `table` | String tableName | `Dataset[Row]` | 从表读取流数据 | `Dataset<Row> tableStream = reader.table("stream_table");` |
| `json` | String path | `Dataset[Row]` | JSON格式流数据 | `Dataset<Row> jsonStream = spark.readStream().json("hdfs://stream/");` |
| `csv` | String path | `Dataset[Row]` | CSV格式流数据 | `Dataset<Row> csvStream = spark.readStream().csv("hdfs://stream/");` |

---

### DataStreamWriter[T]
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming数据流写入器，从Dataset.writeStream()获取。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `format` | String source | `DataStreamWriter[T]` | 设置输出格式 | `writer.format("console");` |
| `outputMode` | String outputMode | `DataStreamWriter[T]` | 设置输出模式 | `writer.outputMode("append");` |
| `option` | String key, String value | `DataStreamWriter[T]` | 设置选项 | `writer.option("checkpointLocation", "hdfs://checkpoint/");` |
| `option` | String key, boolean value | `DataStreamWriter[T]` | 设置布尔选项 | `writer.option("truncate", false);` |
| `options` | Map<String, String> options | `DataStreamWriter[T]` | 设置多个选项 | `writer.options(outputParams);` |
| `partitionBy` | String... colNames | `DataStreamWriter[T]` | 按列分区 | `writer.partitionBy("date");` |
| `foreach` | ForeachWriter[T] writer | `DataStreamWriter[T]` | 自定义foreach输出 | `writer.foreach(new MyForeachWriter());` |
| `foreachBatch` | VoidFunction2[Dataset[T], Long] function | `DataStreamWriter[T]` | 批次处理函数 | `writer.foreachBatch((batch, batchId) -> { batch.write().parquet("hdfs://output/" + batchId); });` |
| `trigger` | Trigger trigger | `DataStreamWriter[T]` | 设置触发器 | `writer.trigger(Trigger.ProcessingTime("5 seconds"));` |
| `start` | 无 | `StreamingQuery` | 启动流查询 | `StreamingQuery query = writer.start();` |

---

### StreamingQuery
**包路径**: `org.apache.spark.sql.streaming`
**说明**: Structured Streaming查询对象，用于监控和管理流查询。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `name` | 无 | `String` | 获取查询名称 | `String name = query.name();` |
| `id` | 无 | `long` | 获取查询ID | `long id = query.id();` |
| `runId` | 无 | `long` | 获取运行ID | `long runId = query.runId();` |
| `isActive` | 无 | `boolean` | 是否活跃 | `boolean active = query.isActive();` |
| `status` | 无 | `StreamingQueryStatus` | 获取状态 | `StreamingQueryStatus status = query.status();` |
| `lastProgress` | 无 | `StreamingQueryProgress` | 获取最新进度 | `StreamingQueryProgress progress = query.lastProgress();` |
| `recentProgress` | 无 | `StreamingQueryProgress[]` | 获取最近进度列表 | `StreamingQueryProgress[] progress = query.recentProgress();` |
| `awaitTermination` | 无 | `Unit` | 等待终止 | `query.awaitTermination();` |
| `awaitTermination` | long timeoutMs | `boolean` | 等待终止或超时 | `boolean terminated = query.awaitTermination(60000);` |
| `stop` | 无 | `Unit` | 停止查询 | `query.stop();` |
| `exception` | 无 | `Option[StreamingQueryException]` | 获取异常 | `Optional<StreamingQueryException> ex = query.exception();` |
| `explain` | boolean extended | `String` | 解释执行计划 | `String plan = query.explain(true);` |
| `sinkStatus` | 无 | `SinkStatus` | 获取sink状态 | `SinkStatus sink = query.sinkStatus();` |
| `sourceStatus` | int index | `SourceStatus` | 获取source状态 | `SourceStatus source = query.sourceStatus(0);` |

---

'''

MLLIB_MODEL = '''
### ALSModel
**包路径**: `org.apache.spark.ml.recommendation`
**说明**: ALS训练后的模型，用于推荐预测。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行预测 | `Dataset<Row> predictions = model.transform(testData);` |
| `recommendForAllUsers` | int numItems | `Dataset[Row]` | 为所有用户推荐物品 | `Dataset<Row> userRecs = model.recommendForAllUsers(10);` |
| `recommendForAllItems` | int numUsers | `Dataset[Row]` | 为所有物品推荐用户 | `Dataset<Row> itemRecs = model.recommendForAllItems(10);` |
| `recommendForUserSubset` | Dataset<?> users, int numItems | `Dataset[Row]` | 为指定用户推荐 | `Dataset<Row> userRecs = model.recommendForUserSubset(userSubset, 10);` |
| `recommendForItemSubset` | Dataset<?> items, int numUsers | `Dataset[Row]` | 为指定物品推荐 | `Dataset<Row> itemRecs = model.recommendForItemSubset(itemSubset, 10);` |
| `write` | 无 | `MLWriter` | 保存模型 | `model.write().overwrite().save("hdfs://model/als");` |

---

'''

def add_supplements(filepath):
    """补充遗漏的类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充Structured Streaming类 - 在DataFrameWriter之后插入
    if "### DataFrameWriter[T]" in content:
        writer_pos = content.find("### DataFrameWriter[T]")
        next_class = content.find("\n### ", writer_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + STREAMING_CLASSES + content[insert_pos:]
            added_count += 35  # 手动计算
            print(f"✅ 添加Structured Streaming类: DataStreamReader, DataStreamWriter, StreamingQuery")
    
    # 补充ALSModel - 在ALS之后插入
    if "### ALS / MatrixFactorizationModel" in content:
        als_pos = content.find("### ALS / MatrixFactorizationModel")
        next_class = content.find("\n### ", als_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_MODEL + content[insert_pos:]
            added_count += 6  # 手动计算
            print(f"✅ 添加ALSModel")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的类（第十二轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
