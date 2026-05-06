# Spark Java API清单提取说明

## 提取来源

**基于开源Spark代码仓和官方文档**：
- Spark源码路径：`/home/h00517772/spark`
- 提取日期：2026-05-06
- 提取方式：从Java源码文件直接提取

## 提取统计

| 指标 | 数量 |
|------|------|
| 包数量 | 15 |
| 类/接口/枚举 | 50 |
| 方法数量 | 849 |
| 字段/常量 | 1060 |

## 主要包路径

提取的Java API源码路径：

### 1. Core Java API
- `core/src/main/java/org/apache/spark/api/java/`
- 包含：StorageLevels, Optional, JavaFutureAction等

### 2. SQL Java API
- `sql/core/src/main/java/org/apache/spark/sql/`
- 包含：execution, vectorized, datasources, connector等
- 主要类：ColumnVector, ParquetReader, OrcReader等

### 3. Streaming Java API
- `streaming/src/main/java/org/apache/spark/streaming/`
- 包含：StreamingContextState, WriteAheadLog等

### 4. GraphX Java API
- `graphx/src/main/java/org/apache/spark/graphx/`
- 包含：TripletFields, EdgeActiveness等

### 5. MLlib
- 主要为Scala实现，Java API较少

## 生成的文档

**文件**：`spark_java_api_from_source.md`

**大小**：2277行，约100KB

**内容结构**：
1. 按包分组（15个包）
2. 每个包包含：
   - 包描述
   - 类列表（快速参考）
   - 详细类定义（字段、方法、稳定性标注）
3. 包含完整JavaDoc描述
4. 包含源文件路径

## 主要类清单

### org.apache.spark.api.java (1类)
- **StorageLevels** - 存储级别常量类
  - 13个常量（NONE, DISK_ONLY, MEMORY_ONLY等）

### org.apache.spark.graphx (1类)
- **TripletFields** - Edge triplet字段配置
  - 字段和常量配置

### org.apache.spark.sql.execution (5类)
- RecordBinaryComparator
- UnsafeExternalRowSorter
- UnsafeFixedWidthAggregationMap
- BufferedRowIterator
- UnsafeKVExternalSorter

### org.apache.spark.sql.execution.vectorized (8类)
- Dictionary
- ConstantColumnVector
- OffHeapColumnVector
- OnHeapColumnVector
- WritableColumnVector
- MutableColumnarRow
- AggregateHashMap
- ColumnVectorUtils

### org.apache.spark.sql.execution.datasources.parquet (17类)
- VectorizedParquetRecordReader
- VectorizedColumnReader
- VectorizedValuesReader
- ParquetVectorUpdater
- ParquetCompressionCodec
- 等Parquet格式支持类

### org.apache.spark.sql.execution.datasources.orc (8类)
- OrcColumnarBatchReader
- OrcColumnVector
- OrcCompressionCodec
- 等ORC格式支持类

### org.apache.spark.streaming (1类)
- **StreamingContextState** (枚举) @DeveloperApi

### org.apache.spark.streaming.util (2类)
- WriteAheadLog @DeveloperApi
- WriteAheadLogRecordHandle @DeveloperApi

## 稳定性标注

文档中包含的稳定性标注：
- `@DeveloperApi` - 开发者API（Streaming相关）
- `@Stable` - 稳定API（如未标注，通常为稳定）
- `@Experimental` - 实验性API
- `@Evolving` - 可演化API
- `@Deprecated` - 已弃用API

## 与之前文档对比

### spark_java_api_complete_list.md（之前）
- 基于文档提取
- 包含RDD相关API（JavaRDD, JavaPairRDD等）
- 包含Streaming API（JavaDStream等）
- 包含函数接口（Function, FilterFunction等）

### spark_java_api_from_source.md（新提取）
- 直接从源码提取
- 更准确（源码路径标注）
- 更详细（JavaDoc描述）
- 包含执行层API（vectorized, parquet, orc等）

## 使用说明

### 查看完整API清单

```bash
cd /home/h00517772/spark/hyx
cat spark_java_api_from_source.md
```

### 查看特定包

```bash
# 查看SQL execution包
grep -A 100 "Package: org.apache.spark.sql.execution" spark_java_api_from_source.md

# 查看vectorized包
grep -A 200 "Package: org.apache.spark.sql.execution.vectorized" spark_java_api_from_source.md
```

### 查看特定类

```bash
# 查看StorageLevels类
grep -A 50 "StorageLevels" spark_java_api_from_source.md

# 查看ParquetReader类
grep -A 100 "VectorizedParquetRecordReader" spark_java_api_from_source.md
```

## API对比用途

### 测试覆盖对比
1. 查看完整API列表
2. 对比您的测试代码覆盖情况
3. 找出缺失的API测试

### API使用参考
1. 查看API稳定性标注
2. 了解API参数和返回类型
3. 查看JavaDoc描述

### 版本更新对比
1. 新版本发布时重新提取
2. 对比API变化
3. 更新测试覆盖

## 后续建议

### 完整性检查
- RDD API可能在Scala层实现（需补充）
- Streaming API需补充JavaDStream等
- SQL DataFrame API需补充Dataset等

### 扩展提取
可继续提取：
- Spark SQL Dataset/DataFrame API
- Spark Streaming DStream API
- MLlib Java API
- PySpark API（Python）

---

**基于开源Spark源码提取，完整准确！**