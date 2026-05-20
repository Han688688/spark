#!/usr/bin/env python3
"""
补充遗漏的方法 - 第三轮
"""
import re

# 补充方法
SUPPLEMENTS = {
    "RelationalGroupedDataset": '''
| `pivot` | String pivotColumn, Object... values | `RelationalGroupedDataset` | 透视转换 | `RelationalGroupedDataset pivoted = ds.groupBy("year").pivot("month", "Jan", "Feb", "Mar");` |
| `pivot` | String pivotColumn | `RelationalGroupedDataset` | 透视转换（自动发现值） | `RelationalGroupedDataset pivoted = ds.groupBy("year").pivot("month");` |
| `flatMapGroups` | FlatMapGroupsFunction[K, V, R] f | `Dataset[R]` | 扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroups((key, iter) -> {...});` |
| `flatMapGroupsInPandas` | FlatMapGroupsInPandasFunction[K, V, R] f | `Dataset[R]` | Pandas扁平映射分组 | `Dataset<Row> result = grouped.flatMapGroupsInPandas(pandasFunc);` |
| `applyInPandas` | ApplyInPandasFunction[K, V, R] f | `Dataset[R]` | Pandas apply函数 | `Dataset<Row> result = grouped.applyInPandas(pandasFunc);` |
| `mapGroups` | MapGroupsFunction[K, V, R] f | `Dataset[R]` | 映射分组 | `Dataset<Row> result = grouped.mapGroups((key, iter) -> {...});` |
| `mapGroupsWithState` | MapGroupsWithStateFunction[K, V, S, R] func, Encoder[S] stateEncoder, Encoder[R] outputEncoder | `Dataset[R]` | 带状态的分组映射（流处理） | `Dataset<Row> result = grouped.mapGroupsWithState(stateFunc, stateEnc, outputEnc);` |
| `flatMapGroupsWithState` | FlatMapGroupsWithStateFunction[K, V, S, R] func, OutputMode outputMode, Encoder[S] stateEncoder, Encoder[R] outputEncoder | `Dataset[R]` | 带状态的扁平映射分组（流处理） | `Dataset<Row> result = grouped.flatMapGroupsWithState(stateFunc, OutputMode.Update(), stateEnc, outputEnc);` |
''',
    "Column": '''
| `isNaN` | 无 | `Column` | 判断是否NaN | `Column isNan = col("value").isNaN();` |
| `regexp` | String pattern | `Column` | 正则匹配（rlike别名） | `Column matched = col("name").regexp("^[A-Z]");` |
''',
    "DataFrameWriter": '''
| `clusterBy` | String... colNames | `DataFrameWriter[T]` | 按列聚类（Delta Lake） | `DataFrameWriter<Row> writer = df.write().clusterBy("id", "date");` |
''',
}

# 新增类
NEW_CLASSES = '''
### JavaSparkStatusTracker
**包路径**: `org.apache.spark.api.java`
**说明**: 作业状态追踪器，用于监控Spark作业的执行状态。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `getActiveJobsIds` | 无 | `int[]` | 获取活动作业ID列表 | `int[] activeJobs = tracker.getActiveJobsIds();` |
| `getActiveStageIds` | 无 | `int[]` | 获取活动Stage ID列表 | `int[] activeStages = tracker.getActiveStageIds();` |
| `getPendingJobsIds` | 无 | `int[]` | 获取等待中作业ID列表 | `int[] pendingJobs = tracker.getPendingJobsIds();` |
| `getPendingStageIds` | 无 | `int[]` | 获取等待中Stage ID列表 | `int[] pendingStages = tracker.getPendingStageIds();` |
| `getActiveJobIds` | 无 | `int[]` | 获取活动作业ID列表（别名） | `int[] active = tracker.getActiveJobIds();` |
| `getActiveStageIds` | 无 | `int[]` | 获取活动Stage ID列表（别名） | `int[] active = tracker.getActiveStageIds();` |
| `getPendingJobIds` | 无 | `int[]` | 获取等待作业ID列表（别名） | `int[] pending = tracker.getPendingJobIds();` |
| `getPendingStageIds` | 无 | `int[]` | 获取等待Stage ID列表（别名） | `int[] pending = tracker.getPendingStageIds();` |

---

'''

def add_supplements(filepath):
    """补充遗漏的方法"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充现有类的方法
    for class_name, methods in SUPPLEMENTS.items():
        if f"### {class_name}" in content:
            class_start = content.find(f"### {class_name}")
            next_class_start = content.find("\n### ", class_start + 1)
            
            if next_class_start == -1:
                next_class_start = len(content)
            
            class_section = content[class_start:next_class_start]
            
            last_method_match = None
            for match in re.finditer(r'\| `[^`]+` \|.*?\n', class_section):
                last_method_match = match
            
            if last_method_match:
                insert_pos = class_start + last_method_match.end()
                content = content[:insert_pos] + methods + "\n" + content[insert_pos:]
                added_count += methods.count('| `')
                print(f"✅ {class_name}: 补充 {methods.count('| `')} 个方法")
    
    # 添加新类 - 在JavaSparkContext之后插入
    if "### JavaSparkStatusTracker" not in content:
        jsc_pos = content.find("### JavaSparkContext")
        if jsc_pos != -1:
            # 找到JavaSparkContext类结束
            next_class = content.find("\n### ", jsc_pos + 1)
            if next_class != -1:
                insert_pos = next_class
                content = content[:insert_pos] + "\n" + NEW_CLASSES + content[insert_pos:]
                print(f"✅ 添加新类: JavaSparkStatusTracker")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的方法（第三轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
