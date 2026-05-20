#!/usr/bin/env python3
"""
补充RelationalGroupedDataset和KeyValueGroupedDataset类
"""
import re

NEW_CLASSES = '''
### RelationalGroupedDataset
**包路径**: `org.apache.spark.sql`
**说明**: 分组后的Dataset，用于聚合操作。由Dataset.groupBy()返回。
**方法数量**: 20+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `agg` | Column... exprs | `Dataset[Row]` | 聚合操作 | `Dataset<Row> result = grouped.agg(count("id").as("cnt"), sum("value").as("total"));` |
| `agg` | Map<String, Column> exprs | `Dataset[Row]` | 聚合操作（Map形式） | `Dataset<Row> result = grouped.agg(Map.of("cnt", count("id"), "avg", avg("value")));` |
| `count` | 无 | `Dataset[Row]` | 计数 | `Dataset<Row> counts = grouped.count();` |
| `mean` | String... cols | `Dataset[Row]` | 平均值 | `Dataset<Row> means = grouped.mean("value", "score");` |
| `avg` | String... cols | `Dataset[Row]` | 平均值（别名） | `Dataset<Row> avg = grouped.avg("value");` |
| `max` | String... cols | `Dataset[Row]` | 最大值 | `Dataset<Row> maxes = grouped.max("value");` |
| `min` | String... cols | `Dataset[Row]` | 最小值 | `Dataset<Row> mins = grouped.min("value");` |
| `sum` | String... cols | `Dataset[Row]` | 求和 | `Dataset<Row> sums = grouped.sum("value");` |
| `pivot` | String pivotColumn | `RelationalGroupedDataset` | 透视转换（自动发现值） | `RelationalGroupedDataset pivoted = grouped.pivot("month");` |
| `pivot` | String pivotColumn, Object... values | `RelationalGroupedDataset` | 透视转换（指定值） | `RelationalGroupedDataset pivoted = grouped.pivot("month", "Jan", "Feb", "Mar");` |
| `pivot` | String pivotColumn, List<Object> values | `RelationalGroupedDataset` | 透视转换（List形式） | `RelationalGroupedDataset pivoted = grouped.pivot("month", Arrays.asList("Jan", "Feb"));` |
| `as` | String alias | `RelationalGroupedDataset` | 别名 | `RelationalGroupedDataset aliased = grouped.as("my_group");` |
| `alias` | String alias | `RelationalGroupedDataset` | 别名 | `RelationalGroupedDataset aliased = grouped.alias("my_group");` |
| `cogroup` | Dataset[U] other, MapFunction[T, K] thisFunc, MapFunction[U, K] otherFunc, Encoder[K] encoder | `KeyValueGroupedDataset[K, Tuple[T, U]]` | 协同分组 | `KeyValueGroupedDataset<String, Tuple2<Row, Row>> cogrouped = grouped.cogroup(otherDs, func1, func2, encoder);` |
| `flatMapGroups` | FlatMapGroupsFunction[K, V, R] f | `Dataset[R]` | 扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroups((key, iter) -> {...});` |
| `mapGroups` | MapGroupsFunction[K, V, R] f | `Dataset[R]` | 映射分组 | `Dataset<Row> result = grouped.mapGroups((key, iter) -> {...});` |
| `mapGroupsWithState` | MapGroupsWithStateFunction[K, V, S, R] func, OutputMode outputMode, Encoder[S] stateEncoder, Encoder[R] outputEncoder | `Dataset[R]` | 带状态的分组映射 | `Dataset<Row> result = grouped.mapGroupsWithState(stateFunc, OutputMode.Update(), stateEnc, outputEnc);` |
| `flatMapGroupsWithState` | FlatMapGroupsWithStateFunction[K, V, S, R] func, OutputMode outputMode, Encoder[S] stateEncoder, Encoder[R] outputEncoder | `Dataset[R]` | 带状态的扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroupsWithState(stateFunc, OutputMode.Append(), stateEnc, outputEnc);` |
| `flatMapGroupsInPandas` | FlatMapGroupsInPandasFunction[K, V, R] f | `Dataset[R]` | Pandas扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroupsInPandas(pandasFunc);` |
| `applyInPandas` | ApplyInPandasFunction[K, V, R] f | `Dataset[R]` | Pandas apply函数 | `Dataset<Row> result = grouped.applyInPandas(pandasFunc);` |

---

### KeyValueGroupedDataset[K, V]
**包路径**: `org.apache.spark.sql`
**说明**: 按键分组后的Dataset，由Dataset.groupByKey()返回。支持更灵活的分组操作。
**方法数量**: 15+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `agg` | Aggregator[V, S, R] aggregator | `Dataset[R]` | 使用Aggregator聚合 | `Dataset<Row> result = grouped.agg(new MyAggregator());` |
| `reduceGroups` | ReduceFunction[V] f | `Dataset[Tuple2[K, V]]` | 按组reduce | `Dataset<Tuple2<String, Integer>> reduced = grouped.reduceGroups((a, b) -> a + b);` |
| `mapGroups` | MapGroupsFunction[K, V, U] f, Encoder[U] encoder | `Dataset[U]` | 映射每组数据 | `Dataset<String> mapped = grouped.mapGroups((key, iter) -> key + ":" + iter.size(), Encoders.STRING());` |
| `flatMapGroups` | FlatMapGroupsFunction[K, V, U] f, Encoder[U] encoder | `Dataset[U]` | 扁平映射每组数据 | `Dataset<String> flatMapped = grouped.flatMapGroups((key, iter) -> {...}, Encoders.STRING());` |
| `mapGroupsWithState` | MapGroupsWithStateFunction[K, V, S, U] func, Encoder[S] stateEncoder, Encoder[U] outputEncoder | `Dataset[U]` | 带状态的分组映射 | `Dataset<Row> result = grouped.mapGroupsWithState(stateFunc, stateEnc, outputEnc);` |
| `flatMapGroupsWithState` | FlatMapGroupsWithStateFunction[K, V, S, U] func, OutputMode outputMode, Encoder[S] stateEncoder, Encoder[U] outputEncoder | `Dataset[U]` | 带状态的扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroupsWithState(stateFunc, OutputMode.Update(), stateEnc, outputEnc);` |
| `keys` | 无 | `Dataset[K]` | 获取所有键 | `Dataset<String> keys = grouped.keys();` |
| `keyAs` | Encoder[K] encoder | `KeyValueGroupedDataset[K, V]` | 指定键编码器 | `KeyValueGroupedDataset<String, Row> newGrouped = grouped.keyAs(Encoders.STRING());` |
| `mapValues` | MapFunction[V, U] f, Encoder[U] encoder | `KeyValueGroupedDataset[K, U]` | 映射值 | `KeyValueGroupedDataset<String, String> mapped = grouped.mapValues(v -> v.toString(), Encoders.STRING());` |
| `flatMapValues` | FlatMapFunction[V, U] f, Encoder[U] encoder | `KeyValueGroupedDataset[K, U]` | 扁平映射值 | `KeyValueGroupedDataset<String, String> flatMapped = grouped.flatMapValues(v -> {...}, Encoders.STRING());` |
| `cogroup` | KeyValueGroupedDataset[K, W] other | `KeyValueGroupedDataset[K, Tuple2[V, W]]` | 协同分组 | `KeyValueGroupedDataset<String, Tuple2<Row, Row>> cogrouped = grouped.cogroup(otherGrouped);` |
| `cogroup` | KeyValueGroupedDataset[K, W] other, CoGroupFunction[K, V, W, U] f, Encoder[U] encoder | `Dataset[U]` | 协同分组并处理 | `Dataset<Row> result = grouped.cogroup(otherGrouped, coGroupFunc, Encoders.bean(Row.class));` |

---

'''

def add_new_classes(filepath):
    """添加新类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 在Dataset类之后插入
    dataset_pos = content.find("### Dataset[T]")
    if dataset_pos != -1:
        # 找到Dataset类结束（下一个###）
        next_class = content.find("\n### ", dataset_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + NEW_CLASSES + content[insert_pos:]
            print("✅ 添加RelationalGroupedDataset和KeyValueGroupedDataset")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return True

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("添加分组相关的类...")
    add_new_classes(filepath)
    print("完成")
