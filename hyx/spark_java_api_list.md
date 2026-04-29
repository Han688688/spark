# Spark 暴露给用户的Java API完整清单

> 基于 Spark 4.2.0-SNAPSHOT 代码仓提取
> 
> 提取日期: 2026-04-29

---

## 稳定性标注说明

| 标注 | 含义 | 使用建议 |
|------|------|----------|
| @Stable | 稳定API，保证向后兼容 | **推荐使用** |
| @Evolving | 演进API，可能变化 | 可用，关注版本迁移 |
| @Unstable | 不稳定API | 谨慎使用 |
| @Experimental | 实验性API | 可能被移除 |
| - | 未明确标注 | 参考对应模块稳定性 |

---

## 一、Java函数接口 (21个)

**位置**: `common/utils-java/src/main/java/org/apache/spark/api/java/function/`

所有接口都是`@FunctionalInterface`，支持Lambda表达式。

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.api.java.function | Function | Stable | 基础函数接口，返回单个值 |
| org.apache.spark.api.java.function | Function0 | Stable | 无参数函数接口 |
| org.apache.spark.api.java.function | Function2 | Stable | 双参数函数接口 |
| org.apache.spark.api.java.function | Function3 | Stable | 三参数函数接口 |
| org.apache.spark.api.java.function | Function4 | Stable | 四参数函数接口 |
| org.apache.spark.api.java.function | MapFunction | Stable | 映射函数，用于Dataset.map() |
| org.apache.spark.api.java.function | FlatMapFunction | Stable | 展平映射，返回零或多个记录 |
| org.apache.spark.api.java.function | FlatMapFunction2 | Stable | 双参数版本FlatMap |
| org.apache.spark.api.java.function | FlatMapGroupsFunction | Stable | 分组映射函数 |
| org.apache.spark.api.java.function | PairFlatMapFunction | Stable | 返回键值对的映射函数 |
| org.apache.spark.api.java.function | PairFunction | Stable | 键值对函数接口 |
| org.apache.spark.api.java.function | FilterFunction | Stable | 过滤函数接口 |
| org.apache.spark.api.java.function | ReduceFunction | Stable | 归约函数接口 |
| org.apache.spark.api.java.function | ForeachFunction | Stable | 遍历函数接口 |
| org.apache.spark.api.java.function | ForeachPartitionFunction | Stable | 分区遍历函数接口 |
| org.apache.spark.api.java.function | MapGroupsFunction | Stable | 分组映射函数 |
| org.apache.spark.api.java.function | MapPartitionsFunction | Stable | 分区映射函数 |
| org.apache.spark.api.java.function | DoubleFunction | Stable | Double返回类型函数 |
| org.apache.spark.api.java.function | DoubleFlatMapFunction | Stable | Double返回类型的映射函数 |
| org.apache.spark.api.java.function | VoidFunction | Stable | 无返回值函数 |
| org.apache.spark.api.java.function | VoidFunction2 | Stable | 双参数无返回值函数 |
| org.apache.spark.api.java.function | CoGroupFunction | Stable | 协分组函数 |

---

## 二、UDF接口 (23个)

**位置**: `sql/api/src/main/java/org/apache/spark/sql/api/java/`

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.api.java | UDF0<R> | @Stable | Spark SQL UDF，0个参数 |
| org.apache.spark.sql.api.java | UDF1<T1,R> | @Stable | Spark SQL UDF，1个参数 |
| org.apache.spark.sql.api.java | UDF2<T1,T2,R> | @Stable | Spark SQL UDF，2个参数 |
| org.apache.spark.sql.api.java | UDF3<T1,T2,T3,R> | @Stable | Spark SQL UDF，3个参数 |
| org.apache.spark.sql.api.java | UDF4<T1,T2,T3,T4,R> | @Stable | Spark SQL UDF，4个参数 |
| org.apache.spark.sql.api.java | UDF5<T1,...,T5,R> | @Stable | Spark SQL UDF，5个参数 |
| org.apache.spark.sql.api.java | UDF6 | @Stable | Spark SQL UDF，6个参数 |
| org.apache.spark.sql.api.java | UDF7 | @Stable | Spark SQL UDF，7个参数 |
| org.apache.spark.sql.api.java | UDF8 | @Stable | Spark SQL UDF，8个参数 |
| org.apache.spark.sql.api.java | UDF9 | @Stable | Spark SQL UDF，9个参数 |
| org.apache.spark.sql.api.java | UDF10 | @Stable | Spark SQL UDF，10个参数 |
| org.apache.spark.sql.api.java | UDF11 | @Stable | Spark SQL UDF，11个参数 |
| org.apache.spark.sql.api.java | UDF12 | @Stable | Spark SQL UDF，12个参数 |
| org.apache.spark.sql.api.java | UDF13 | @Stable | Spark SQL UDF，13个参数 |
| org.apache.spark.sql.api.java | UDF14 | @Stable | Spark SQL UDF，14个参数 |
| org.apache.spark.sql.api.java | UDF15 | @Stable | Spark SQL UDF，15个参数 |
| org.apache.spark.sql.api.java | UDF16 | @Stable | Spark SQL UDF，16个参数 |
| org.apache.spark.sql.api.java | UDF17 | @Stable | Spark SQL UDF，17个参数 |
| org.apache.spark.sql.api.java | UDF18 | @Stable | Spark SQL UDF，18个参数 |
| org.apache.spark.sql.api.java | UDF19 | @Stable | Spark SQL UDF，19个参数 |
| org.apache.spark.sql.api.java | UDF20 | @Stable | Spark SQL UDF，20个参数 |
| org.apache.spark.sql.api.java | UDF21 | @Stable | Spark SQL UDF，21个参数 |
| org.apache.spark.sql.api.java | UDF22 | @Stable | Spark SQL UDF，22个参数（最大） |

---

## 三、SQL核心API

**位置**: `sql/api/src/main/java/org/apache/spark/sql/`

### 3.1 核心类

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql | SaveMode | @Stable | 保存DataFrame到数据源的预期行为模式枚举 |
| org.apache.spark.sql | RowFactory | @Stable | 创建Row对象的工厂类 |

### 3.2 数据类型 (sql.types)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.types | DataTypes | @Evolving | SQL数据类型定义工具类 |
| org.apache.spark.sql.types | Geography | @Evolving | 地理类型（Spark 4.x新增） |
| org.apache.spark.sql.types | Geometry | @Evolving | 几何类型（Spark 4.x新增） |
| org.apache.spark.sql.types | SQLUserDefinedType | @Evolving | 用户自定义类型注解 |

### 3.3 表达式 (sql.expressions)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.expressions.javalang | typed | @Evolving | 类型化聚合函数 |

---

## 四、Streaming API

**位置**: `sql/api/src/main/java/org/apache/spark/sql/streaming/`

### 4.1 核心Streaming类

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.streaming | Trigger | @Evolving | 流式查询触发策略 |
| org.apache.spark.sql.streaming | OutputMode | @Evolving | 流式查询输出模式（Append/Complete/Update） |
| org.apache.spark.sql.streaming | GroupStateTimeout | @Evolving | 分组状态超时配置 |
| org.apache.spark.sql.streaming | TimeMode | @Evolving | 时间模式配置（transformWithState） |

### 4.2 有状态处理函数接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.api.java.function | MapGroupsWithStateFunction<K,V,S,R> | @Evolving | 带状态的分组映射函数 |
| org.apache.spark.api.java.function | FlatMapGroupsWithStateFunction<K,V,S,R> | @Evolving | 带状态的分组展平映射函数 |

---

## 五、Connector Catalog API

**位置**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/catalog/`

### 5.1 Catalog核心接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.catalog | CatalogPlugin | @Evolving | Catalog实现的标记接口 |
| org.apache.spark.sql.connector.catalog | CatalogExtension | @Evolving | 扩展Spark内置session catalog的API |
| org.apache.spark.sql.connector.catalog | TableCatalog | @Evolving | 表Catalog接口 |
| org.apache.spark.sql.connector.catalog | Table | @Evolving | 表接口 |
| org.apache.spark.sql.connector.catalog | TableProvider | @Evolving | 表提供者接口（DataSource V2） |
| org.apache.spark.sql.connector.catalog | TableChange | @Evolving | 表变更操作接口 |
| org.apache.spark.sql.connector.catalog | TableCapability | @Evolving | 表能力枚举 |
| org.apache.spark.sql.connector.catalog | SupportsNamespaces | @Evolving | 支持命名空间的Catalog |
| org.apache.spark.sql.connector.catalog | SupportsRead | @Evolving | 支持读取的表 |
| org.apache.spark.sql.connector.catalog | SupportsWrite | @Evolving | 支持写入的表 |
| org.apache.spark.sql.connector.catalog | SupportsDelete | @Evolving | 支持删除的表 |
| org.apache.spark.sql.connector.catalog | SupportsDeleteV2 | @Evolving | 支持V2删除的表 |
| org.apache.spark.sql.connector.catalog | SupportsMetadataColumns | @Evolving | 支持元数据列的表 |
| org.apache.spark.sql.connector.catalog | StagingTableCatalog | @Evolving | 支持暂存表的Catalog |
| org.apache.spark.sql.connector.catalog | StagedTable | @Evolving | 暂存表接口 |
| org.apache.spark.sql.connector.catalog | FunctionCatalog | @Evolving | 函数Catalog接口 |
| org.apache.spark.sql.connector.catalog | ProcedureCatalog | @Evolving | 存储过程Catalog接口 |
| org.apache.spark.sql.connector.catalog | Identifier | @Evolving | 标识Catalog中的对象 |
| org.apache.spark.sql.connector.catalog | IdentityColumnSpec | @Evolving | 标识列规范 |
| org.apache.spark.sql.connector.catalog | Column | @Evolving | 列定义接口 |
| org.apache.spark.sql.connector.catalog | ColumnDefaultValue | @Evolving | 列默认值 |
| org.apache.spark.sql.connector.catalog | MetadataColumn | @Evolving | 元数据列接口 |
| org.apache.spark.sql.connector.catalog | NamespaceChange | @Evolving | 命名空间变更操作 |
| org.apache.spark.sql.connector.catalog | SessionConfigSupport | @Evolving | 支持会话配置 |
| org.apache.spark.sql.connector.catalog | TruncatableTable | @Evolving | 可截断表接口 |
| org.apache.spark.sql.connector.catalog | TableSummary | @Evolving | 表摘要接口 |

### 5.2 CDC接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.catalog | Changelog | @Evolving | CDC变更数据捕获核心接口 |
| org.apache.spark.sql.connector.catalog | ChangelogInfo | @Evolving | CDC查询参数封装 |
| org.apache.spark.sql.connector.catalog | ChangelogRange | @Evolving | CDC版本/时间戳范围 |

### 5.3 约束接口 (catalog.constraints)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.catalog.constraints | Constraint | @Evolving | 约束基类 |
| org.apache.spark.sql.connector.catalog.constraints | Check | @Evolving | CHECK约束 |
| org.apache.spark.sql.connector.catalog.constraints | PrimaryKey | @Evolving | 主键约束 |
| org.apache.spark.sql.connector.catalog.constraints | ForeignKey | @Evolving | 外键约束 |
| org.apache.spark.sql.connector.catalog.constraints | Unique | @Evolving | UNIQUE约束 |

### 5.4 函数接口 (catalog.functions)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.catalog.functions | Function | @Evolving | 函数接口 |
| org.apache.spark.sql.connector.catalog.functions | BoundFunction | @Evolving | 已绑定函数 |
| org.apache.spark.sql.connector.catalog.functions | UnboundFunction | @Evolving | 未绑定函数 |
| org.apache.spark.sql.connector.catalog.functions | ScalarFunction | @Evolving | 标量函数 |
| org.apache.spark.sql.connector.catalog.functions | AggregateFunction | @Evolving | 聚合函数 |
| org.apache.spark.sql.connector.catalog.functions | SimpleFunction | @Evolving | 简单函数 |
| org.apache.spark.sql.connector.catalog.functions | Reducer | @Evolving | 归约器接口 |
| org.apache.spark.sql.connector.catalog.functions | ReducibleFunction | @Evolving | 可归约函数 |

### 5.5 存储过程接口 (catalog.procedures)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.catalog.procedures | Procedure | @Evolving | 存储过程接口 |
| org.apache.spark.sql.connector.catalog.procedures | BoundProcedure | @Evolving | 已绑定存储过程 |
| org.apache.spark.sql.connector.catalog.procedures | UnboundProcedure | @Evolving | 未绑定存储过程 |
| org.apache.spark.sql.connector.catalog.procedures | ProcedureParameter | @Evolving | 存储过程参数 |
| org.apache.spark.sql.connector.catalog.procedures | SimpleProcedure | @Evolving | 简单存储过程 |

---

## 六、Connector Read API

**位置**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/read/`

### 6.1 核心读取接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.read | Scan | @Evolving | 数据源扫描的逻辑表示 |
| org.apache.spark.sql.connector.read | ScanBuilder | @Evolving | Scan构建器 |
| org.apache.spark.sql.connector.read | Batch | @Evolving | 批处理扫描接口 |
| org.apache.spark.sql.connector.read | InputPartition | @Evolving | 输入分区接口 |
| org.apache.spark.sql.connector.read | PartitionReader | @Evolving | 分区读取器 |
| org.apache.spark.sql.connector.read | PartitionReaderFactory | @Evolving | 分区读取器工厂 |
| org.apache.spark.sql.connector.read | Statistics | @Evolving | 统计信息接口 |
| org.apache.spark.sql.connector.read | LocalScan | @Stable | 本地扫描接口 |

### 6.2 下推优化接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.read | SupportsPushDownFilters | @Evolving | 支持下推过滤 |
| org.apache.spark.sql.connector.read | SupportsPushDownV2Filters | @Evolving | 支持V2下推过滤 |
| org.apache.spark.sql.connector.read | SupportsPushDownAggregates | @Evolving | 支持下推聚合 |
| org.apache.spark.sql.connector.read | SupportsPushDownLimit | @Evolving | 支持下推Limit |
| org.apache.spark.sql.connector.read | SupportsPushDownOffset | @Evolving | 支持下推Offset |
| org.apache.spark.sql.connector.read | SupportsPushDownTopN | @Evolving | 支持下推TopN |
| org.apache.spark.sql.connector.read | SupportsPushDownTableSample | @Evolving | 支持下推采样 |
| org.apache.spark.sql.connector.read | SupportsPushDownRequiredColumns | @Evolving | 支持下推列选择 |
| org.apache.spark.sql.connector.read | SupportsPushDownJoin | @Evolving | 支持下推Join |

### 6.3 统计信息接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.read | SupportsReportStatistics | @Evolving | 支持报告统计信息 |
| org.apache.spark.sql.connector.read | SupportsReportPartitioning | @Evolving | 支持报告分区信息 |
| org.apache.spark.sql.connector.read | SupportsReportOrdering | @Evolving | 支持报告排序信息 |

### 6.4 分区接口 (read.partitioning)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.read.partitioning | Partitioning | @Evolving | 分区接口 |
| org.apache.spark.sql.connector.read.partitioning | KeyGroupedPartitioning | @Evolving | 键分组分区 |
| org.apache.spark.sql.connector.read.partitioning | UnknownPartitioning | @Evolving | 未知分区 |

### 6.5 列统计接口 (read.colstats)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.read.colstats | ColumnStatistics | @Evolving | 列统计信息 |
| org.apache.spark.sql.connector.read.colstats | Histogram | @Evolving | 直方图统计 |
| org.apache.spark.sql.connector.read.colstats | HistogramBin | @Evolving | 直方图区间 |

---

## 七、Connector Write API

**位置**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/write/`

### 7.1 核心写入接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.write | Write | @Evolving | 写入接口 |
| org.apache.spark.sql.connector.write | WriteBuilder | @Evolving | 写入构建器 |
| org.apache.spark.sql.connector.write | BatchWrite | @Evolving | 批处理写入接口 |
| org.apache.spark.sql.connector.write | DataWriter | @Evolving | 数据写入器 |
| org.apache.spark.sql.connector.write | DataWriterFactory | @Evolving | 数据写入器工厂 |
| org.apache.spark.sql.connector.write | WriterCommitMessage | @Evolving | 写入提交消息 |
| org.apache.spark.sql.connector.write | LogicalWriteInfo | @Evolving | 逻辑写入信息 |
| org.apache.spark.sql.connector.write | PhysicalWriteInfo | @Evolving | 物理写入信息 |
| org.apache.spark.sql.connector.write | V1Write | @Evolving | V1数据源写入接口 |

### 7.2 写入支持接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.write | SupportsOverwrite | @Evolving | 支持覆写 |
| org.apache.spark.sql.connector.write | SupportsOverwriteV2 | @Evolving | 支持V2覆写 |
| org.apache.spark.sql.connector.write | SupportsDynamicOverwrite | @Evolving | 支持动态覆写 |
| org.apache.spark.sql.connector.write | SupportsTruncate | @Evolving | 支持截断 |
| org.apache.spark.sql.connector.write | RequiresDistributionAndOrdering | @Evolving | 要求分布和排序 |

---

## 八、Connector Expressions API

**位置**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/expressions/`

### 8.1 核心表达式接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.expressions | Expression | @Evolving | 表达式接口 |
| org.apache.spark.sql.connector.expressions | Expressions | @Evolving | 表达式工具类 |
| org.apache.spark.sql.connector.expressions | NamedReference | @Evolving | 命名引用 |
| org.apache.spark.sql.connector.expressions | Literal | @Evolving | 常量表达式 |
| org.apache.spark.sql.connector.expressions | Transform | @Evolving | 转换表达式 |
| org.apache.spark.sql.connector.expressions | SortOrder | @Evolving | 排序顺序 |
| org.apache.spark.sql.connector.expressions | SortDirection | @Evolving | 排序方向 |
| org.apache.spark.sql.connector.expressions | NullOrdering | @Evolving | NULL排序规则 |
| org.apache.spark.sql.connector.expressions | Cast | @Evolving | 类型转换表达式 |
| org.apache.spark.sql.connector.expressions | Extract | @Evolving | 提取表达式 |
| org.apache.spark.sql.connector.expressions | UserDefinedScalarFunc | @Evolving | 用户定义标量函数 |
| org.apache.spark.sql.connector.expressions | GeneralScalarExpression | @Evolving | 通用标量表达式 |
| org.apache.spark.sql.connector.expressions | PartitionFieldReference | @Evolving | 分区字段引用 |
| org.apache.spark.sql.connector.expressions | GetArrayItem | @Evolving | 数组元素获取 |

### 8.2 过滤表达式 (expressions.filter)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.expressions.filter | Predicate | @Evolving | 谓词表达式基类 |
| org.apache.spark.sql.connector.expressions.filter | And | @Evolving | AND逻辑表达式 |
| org.apache.spark.sql.connector.expressions.filter | Or | @Evolving | OR逻辑表达式 |
| org.apache.spark.sql.connector.expressions.filter | Not | @Evolving | NOT逻辑表达式 |
| org.apache.spark.sql.connector.expressions.filter | AlwaysTrue | @Evolving | 恒真表达式 |
| org.apache.spark.sql.connector.expressions.filter | AlwaysFalse | @Evolving | 恒假表达式 |
| org.apache.spark.sql.connector.expressions.filter | PartitionPredicate | @Evolving | 分区谓词 |

### 8.3 聚合表达式 (expressions.aggregate)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.expressions.aggregate | AggregateFunc | @Evolving | 聚合函数基类 |
| org.apache.spark.sql.connector.expressions.aggregate | Count | @Evolving | COUNT聚合 |
| org.apache.spark.sql.connector.expressions.aggregate | CountStar | @Evolving | COUNT(*)聚合 |
| org.apache.spark.sql.connector.expressions.aggregate | Sum | @Evolving | SUM聚合 |
| org.apache.spark.sql.connector.expressions.aggregate | Min | @Evolving | MIN聚合 |
| org.apache.spark.sql.connector.expressions.aggregate | Max | @Evolving | MAX聚合 |
| org.apache.spark.sql.connector.expressions.aggregate | Avg | @Evolving | AVG聚合 |
| org.apache.spark.sql.connector.expressions.aggregate | Aggregation | @Evolving | 聚合表达式集合 |
| org.apache.spark.sql.connector.expressions.aggregate | GeneralAggregateFunc | @Evolving | 通用聚合函数 |
| org.apache.spark.sql.connector.expressions.aggregate | UserDefinedAggregateFunc | @Evolving | 用户定义聚合函数 |

---

## 九、Connector Distribution API

**位置**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/distributions/`

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.distributions | Distribution | @Evolving | 分布接口 |
| org.apache.spark.sql.connector.distributions | Distributions | @Evolving | 分布工具类 |
| org.apache.spark.sql.connector.distributions | ClusteredDistribution | @Evolving | 聚簇分布 |
| org.apache.spark.sql.connector.distributions | OrderedDistribution | @Evolving | 有序分布 |
| org.apache.spark.sql.connector.distributions | UnspecifiedDistribution | @Evolving | 未指定分布 |

---

## 十、Connector Metric API

**位置**: `sql/catalyst/src/main/java/org/apache/spark/sql/connector/metric/`

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.connector.metric | CustomMetric | @Evolving | 自定义度量接口 |
| org.apache.spark.sql.connector.metric | CustomTaskMetric | @Evolving | 自定义任务度量 |
| org.apache.spark.sql.connector.metric | CustomSumMetric | @Evolving | 自定义求和度量 |
| org.apache.spark.sql.connector.metric | CustomAvgMetric | @Evolving | 自定义平均度量 |

---

## 十一、Vectorized API

**位置**: `sql/core/src/main/java/org/apache/spark/sql/vectorized/`

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.vectorized | ColumnVector | @Evolving | 列向量接口 |
| org.apache.spark.sql.vectorized | ColumnarRow | @Evolving | 列式行 |
| org.apache.spark.sql.vectorized | ColumnarArray | @Evolving | 列式数组 |

---

## 十二、Core API

**位置**: `core/src/main/java/org/apache/spark/api/java/`

### 12.1 Java工具类

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.api.java | Optional<T> | @Stable | Java Optional包装类 |
| org.apache.spark.api.java | JavaFutureAction<T> | @Stable | Java Future动作接口 |
| org.apache.spark.api.java | StorageLevels | @Stable | 存储级别工具类 |

### 12.2 Plugin API

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.api.plugin | SparkPlugin | @Evolving | Spark插件标记接口 |
| org.apache.spark.api.plugin | DriverPlugin | @Evolving | Driver端插件接口 |
| org.apache.spark.api.plugin | ExecutorPlugin | @Evolving | Executor端插件接口 |
| org.apache.spark.api.plugin | PluginContext | @Evolving | 插件上下文接口 |

### 12.3 Shuffle API

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.shuffle.api | ShuffleDataIO | @Evolving | Shuffle数据IO接口 |
| org.apache.spark.shuffle.api | ShuffleDriverComponents | @Evolving | Shuffle Driver组件 |
| org.apache.spark.shuffle.api | ShuffleExecutorComponents | @Evolving | Shuffle Executor组件 |
| org.apache.spark.shuffle.api | ShuffleMapOutputWriter | @Evolving | Shuffle Map输出写入器 |
| org.apache.spark.shuffle.api | ShufflePartitionWriter | @Evolving | Shuffle分区写入器 |
| org.apache.spark.shuffle.api | SingleSpillShuffleMapOutputWriter | @Evolving | 单次溢出写入器 |
| org.apache.spark.shuffle.api | WritableByteChannelWrapper | @Evolving | 可写字节通道包装器 |

### 12.4 Spark核心信息接口

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark | SparkJobInfo | @Stable | Spark作业信息接口 |
| org.apache.spark | SparkStageInfo | @Stable | Spark阶段信息接口 |
| org.apache.spark | SparkExecutorInfo | @Stable | Spark执行器信息接口 |
| org.apache.spark | SparkFirehoseListener | @Stable | Spark事件监听器基类 |
| org.apache.spark | JobExecutionStatus | @Stable | 作业执行状态枚举 |

---

## 十三、Resource Manager API

### 13.1 YARN相关

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.deploy.yarn | AmIpFilter | - | YARN Application Master IP过滤器 |

---

## 十四、Query Context API

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark | QueryContext | - | 查询上下文接口 |
| org.apache.spark | QueryContextType | - | 查询上下文类型枚举 |
| org.apache.spark | SparkThrowable | - | Spark异常接口 |

---

## 十五、Exception相关

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.sql.execution.datasources | SchemaColumnConvertNotSupportedException | - | Schema列转换不支持异常 |

---

## 十六、Spatial Types (空间类型)

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.unsafe.types | CalendarInterval | - | 日历间隔类型 |
| org.apache.spark.unsafe.types | GeometryVal | - | 几何值类型 |
| org.apache.spark.unsafe.types | GeographyVal | - | 地理值类型 |

---

## 十七、Network Shuffle API

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.network.shuffle | ErrorHandler | - | 错误处理器 |
| org.apache.spark.network.shuffle | MergedShuffleFileManager | - | 合并Shuffle文件管理器 |

---

## 十八、Legacy Streaming API

**位置**: `streaming/src/main/java/org/apache/spark/streaming/`

| 包路径 | 类/接口名 | 稳定性 | 描述 |
|--------|----------|--------|------|
| org.apache.spark.streaming | StreamingContextState | - | StreamingContext状态枚举 |

---

## API统计汇总

| 分类 | 数量 | 主要稳定性 |
|------|------|-----------|
| Java函数接口 | 21 | @Stable |
| UDF接口 | 23 | @Stable |
| SQL核心API | 7 | @Stable/@Evolving |
| Streaming API | 6 | @Evolving |
| Connector Catalog API | 37 | @Evolving |
| Connector Read API | 21 | @Evolving |
| Connector Write API | 13 | @Evolving |
| Connector Expressions API | 27 | @Evolving |
| Connector Distribution API | 5 | @Evolving |
| Connector Metric API | 4 | @Evolving |
| Vectorized API | 3 | @Evolving |
| Core API | 15 | @Stable/@Evolving |
| Plugin API | 4 | @Evolving |
| Shuffle API | 7 | @Evolving |
| 其他API | 12 | - |
| **总计** | **~200** | - |

---

## 使用建议

### 1. 优先使用 @Stable API
- Java函数接口（21个）
- UDF接口（23个）
- SQL核心类（SaveMode、RowFactory、Optional）
- Spark信息接口（SparkJobInfo等）

### 2. @Evolving API 可用但需关注变化
- Connector API（DataSource V2）
- Streaming API
- 空间类型（Geometry/Geography）

### 3. 避免使用内部API
- 未标注稳定性的API
- 包含internal的包路径
- 标注@Private或@Unstable的API

---

## 文件来源参考

| 模块 | 路径 |
|------|------|
| Java函数接口 | common/utils-java/src/main/java/org/apache/spark/api/java/function/ |
| UDF接口 | sql/api/src/main/java/org/apache/spark/sql/api/java/ |
| SQL核心 | sql/api/src/main/java/org/apache/spark/sql/ |
| Streaming | sql/api/src/main/java/org/apache/spark/sql/streaming/ |
| Connector | sql/catalyst/src/main/java/org/apache/spark/sql/connector/ |
| Core API | core/src/main/java/org/apache/spark/api/ |