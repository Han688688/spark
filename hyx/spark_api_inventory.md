# Spark API 清单

基于代码仓分析和官方文档梳理，版本：Spark 4.2.0-SNAPSHOT

## 一、核心入口API

### 1.1 SparkSession（入口类）
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/` + `sql/core/src/main/scala/org/apache/spark/sql/classic/SparkSession.scala`

| 方法 | 签名 | 功能分类 | 描述 |
|------|------|----------|------|
| builder | `SparkSession.Builder` | 入口 | 创建SparkSession构建器 |
| read | `DataFrameReader` | 数据读取 | 获取数据读取器 |
| readStream | `DataStreamReader` | 流读取 | 获取流数据读取器 |
| sql | `DataFrame` | SQL执行 | 执行SQL查询 |
| table | `DataFrame` | 表访问 | 根据表名获取DataFrame |
| range | `DataFrame` | 数据生成 | 生成范围数据 |
| createDataFrame | `DataFrame` | 数据创建 | 从RDD/集合创建DataFrame |
| catalog | `Catalog` | 元数据 | 访问Catalog接口 |
| conf | `RuntimeConfig` | 配置 | 运行时配置访问 |
| udf | `UDFRegistration` | UDF | UDF注册接口 |
| udtf | `UDTFRegistration` | UDTF | UDTF注册接口 |
| streams | `StreamingQueryManager` | 流管理 | 流查询管理器 |
| version | `String` | 信息 | Spark版本号 |
| sparkContext | `SparkContext` | 核心 | SparkContext访问 |
| newSession | `SparkSession` | 会话 | 创建新会话 |
| stop | `Unit` | 生命周期 | 停止会话 |
| addArtifact | `Unit` | 资源 | 添加资源文件（Connect专用） |

**Builder方法**:
- `appName(name: String)` - 设置应用名称
- `master(master: String)` - 设置Master地址
- `config(key, value)` - 设置配置
- `enableHiveSupport()` - 启用Hive支持
- `getOrCreate()` - 获取或创建会话
- `remote(url: String)` - Connect远程连接

### 1.2 SQLContext（遗留入口，1.x兼容）
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/SQLContext.scala`

| 方法 | 签名 | 功能分类 |
|------|------|----------|
| sql | `DataFrame` | SQL执行 |
| tables | `DataFrame` | 表列表 |
| tableNames | `Array[String]` | 表名列表 |
| read | `DataFrameReader` | 数据读取 |

---

## 二、数据操作API（Dataset/DataFrame）

### 2.1 Dataset核心API
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/Dataset.scala`

#### 转换操作（Transformations）

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| select | `DataFrame` | 投影 | 选择列 |
| selectExpr | `DataFrame` | 投影 | SQL表达式选择 |
| filter | `Dataset[T]` | 过滤 | 条件过滤 |
| where | `Dataset[T]` | 过滤 | filter别名 |
| groupBy | `RelationalGroupedDataset` | 聚合 | 分组聚合 |
| groupByKey | `KeyValueGroupedDataset[K,T]` | 聚合 | 类型化分组 |
| cube | `RelationalGroupedDataset` | 聚合 | 多维cube |
| rollup | `RelationalGroupedDataset` | 聚合 | 多维rollup |
| groupingSets | `RelationalGroupedDataset` | 聚合 | 自定义分组集（4.0新增） |
| agg | `DataFrame` | 聚合 | 聚合计算 |
| map | `Dataset[U]` | 转换 | 类型化映射 |
| mapPartitions | `Dataset[U]` | 转换 | 分区映射 |
| flatMap | `Dataset[U]` | 转换 | 展平映射 |
| mapInPandas | `DataFrame` | 转换 | Pandas映射 |
| mapInArrow | `DataFrame` | 转换 | Arrow映射 |
| join | `DataFrame` | 连接 | 表连接 |
| joinWith | `Dataset[(T,U)]` | 连接 | 类型化连接 |
| crossJoin | `DataFrame` | 连接 | 交叉连接 |
| lateralJoin | `DataFrame` | 连接 | Lateral连接（新增） |
| union | `Dataset[T]` | 合并 | 合并数据集 |
| unionAll | `Dataset[T]` | 合并 | union别名 |
| unionByName | `Dataset[T]` | 合并 | 按列名合并 |
| except | `Dataset[T]` | 集合 | 差集 |
| exceptAll | `Dataset[T]` | 集合 | 保留重复差集 |
| intersect | `Dataset[T]` | 集合 | 交集 |
| intersectAll | `Dataset[T]` | 集合 | 保留重复交集 |
| withColumn | `DataFrame` | 列操作 | 添加/替换列 |
| withColumns | `DataFrame` | 列操作 | 批量添加列 |
| withColumnRenamed | `DataFrame` | 列操作 | 重命名列 |
| withColumnsRenamed | `DataFrame` | 列操作 | 批量重命名 |
| withMetadata | `DataFrame` | 元数据 | 设置列元数据 |
| drop | `DataFrame` | 列操作 | 删除列 |
| dropDuplicates | `Dataset[T]` | 去重 | 去除重复行 |
| dropDuplicatesWithinWatermark | `Dataset[T]` | 流去重 | 流数据去重（3.5新增） |
| distinct | `Dataset[T]` | 去重 | 唯一值 |
| repartition | `Dataset[T]` | 重分区 | 重新分区 |
| repartitionByRange | `Dataset[T]` | 重分区 | 范围分区 |
| coalesce | `Dataset[T]` | 重分区 | 减少分区 |
| sample | `Dataset[T]` | 采样 | 随机采样 |
| sampleBy | `Dataset[T]` | 采样 | 分层采样 |
| randomSplit | `Array[Dataset[T]]` | 分割 | 随机分割 |
| limit | `Dataset[T]` | 限制 | 取前N行 |
| offset | `Dataset[T]` | 限制 | 跳过前N行（新增） |
| sort | `Dataset[T]` | 排序 | 排序 |
| orderBy | `Dataset[T]` | 排序 | sort别名 |
| sortWithinPartitions | `Dataset[T]` | 排序 | 分区内排序 |
| as | `Dataset[U]` | 类型转换 | 类型转换 |
| toDF | `DataFrame` | 类型转换 | 转为DataFrame |
| to | `DataFrame` | Schema适配 | 适配指定Schema（3.4新增） |
| alias | `Dataset[T]` | 别名 | 设置别名 |
| hint | `Dataset[T]` | 提示 | 设置查询提示 |
| transform | `Dataset[U]` | 转换 | 应用转换函数 |
| withWatermark | `Dataset[T]` | 流水印 | 设置流水印 |
| melt | `DataFrame` | reshape | 列转行（unpivot） |
| unpivot | `DataFrame` | reshape | melt别名 |
| transpose | `DataFrame` | reshape | 转置（新增） |

#### 动作操作（Actions）

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| show | `Unit` | 显示 | 打印数据 |
| collect | `Array[T]` | 收集 | 收集所有数据 |
| collectAsList | `List[T]` | 收集 | Java列表形式收集 |
| take | `Array[T]` | 收集 | 取前N行 |
| head | `Array[T]` | 收集 | 头部数据 |
| first | `T` | 收集 | 第一行 |
| tail | `Array[T]` | 收集 | 末尾N行 |
| count | `Long` | 统计 | 行数统计 |
| describe | `DataFrame` | 统计 | 统计描述 |
| summary | `DataFrame` | 统计 | 扩展统计描述 |
| foreach | `Unit` | 遍历 | 遍历处理 |
| foreachPartition | `Unit` | 遍历 | 分区遍历处理 |
| reduce | `T` | 聚合 | 类型化reduce |
| toLocalIterator | `Iterator[T]` | 收集 | 本地迭代器 |
| toPandas | `pandas.DataFrame` | 导出 | 转Pandas DataFrame |
| toArrow | `Arrow Table` | 导出 | Arrow格式 |
| toJSON | `Dataset[String]` | 导出 | 转JSON字符串 |
| write | `DataFrameWriter[T]` | 输出 | 获取写入器 |
| writeStream | `DataStreamWriter[T]` | 流输出 | 获取流写入器 |
| writeTo | `DataFrameWriterV2[T]` | V2输出 | V2 API写入 |
| mergeInto | `MergeIntoWriter[T]` | 合写 | Merge写入（4.0新增） |

#### 信息查询

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| schema | `StructType` | 元数据 | Schema信息 |
| printSchema | `Unit` | 元数据 | 打印Schema |
| dtypes | `Array[(String,String)]` | 元数据 | 列类型信息 |
| columns | `Array[String]` | 元数据 | 列名列表 |
| col | `Column` | 列访问 | 获取列引用 |
| colRegex | `Column` | 列访问 | 正则匹配列 |
| explain | `Unit` | 计划 | 打印执行计划 |
| inputFiles | `Array[String]` | 信息 | 输入文件列表 |
| storageLevel | `StorageLevel` | 信息 | 存储级别 |
| isLocal | `Boolean` | 信息 | 是否本地 |
| isStreaming | `Boolean` | 信息 | 是否流数据 |
| isEmpty | `Boolean` | 信息 | 是否空 |
| semanticHash | `Int` | 信息 |语义哈希 |
| sameSemantics | `Boolean` | 比较 |语义等价比较 |
| executionInfo | `ExecutionInfo` | 信息 | 执行信息（新增） |

#### 缓存/持久化

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| cache | `Dataset[T]` | 缓存 | 默认缓存 |
| persist | `Dataset[T]` | 缓存 | 指定级别缓存 |
| unpersist | `Dataset[T]` | 缓存 | 清除缓存 |
| checkpoint | `Dataset[T]` | 检查点 | 检查点 |
| localCheckpoint | `Dataset[T]` | 检查点 | 本地检查点 |

#### 视图注册

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| createTempView | `Unit` | 视图 | 创建临时视图 |
| createOrReplaceTempView | `Unit` | 视图 | 创建/替换临时视图 |
| createGlobalTempView | `Unit` | 视图 | 创建全局临时视图 |
| createOrReplaceGlobalTempView | `Unit` | 视图 | 创建/替换全局临时视图 |
| registerTempTable | `Unit` | 视图 | 遗留API |

#### 观察/监控

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| observe | `Dataset[T]` | 监控 | 观察指标（3.3新增） |

### 2.2 Column API
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/Column.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| alias / as | `Column` | 别名 | 设置别名 |
| cast | `Column` | 类型 | 类型转换 |
| try_cast | `Column` | 类型 | 安全类型转换 |
| asc | `Column` | 排序 | 升序 |
| desc | `Column` | 排序 | 降序 |
| asc_nulls_first | `Column` | 排序 | 升序空值优先 |
| asc_nulls_last | `Column` | 排序 | 升序空值后置 |
| desc_nulls_first | `Column` | 排序 | 降序空值优先 |
| desc_nulls_last | `Column` | 排序 | 降序空值后置 |
| equalTo / === | `Column` | 比较 | 等于 |
| notEqual / !== | `Column` | 比较 | 不等于 |
| lt / < | `Column` | 比较 | 小于 |
| le / <= | `Column` | 比较 | 小于等于 |
| gt / > | `Column` | 比较 | 大于 |
| ge / >= | `Column` | 比较 | 大于等于 |
| eqNullSafe | `Column` | 比较 | 空值安全等于 |
| isNull | `Column` | 空值 | 是否为空 |
| isNotNull | `Column` | 空值 | 是否非空 |
| isNaN | `Column` | NaN | 是否NaN |
| isin | `Column` | 集合 | 是否在集合中 |
| between | `Column` | 范围 | 范围条件 |
| when | `Column` | 条件 | CASE WHEN |
| otherwise | `Column` | 条件 | CASE WHEN ELSE |
| like | `Column` | 字符串 | LIKE匹配 |
| ilike | `Column` | 字符串 | 不区分大小写LIKE |
| rlike | `Column` | 字符串 | 正则匹配 |
| contains | `Column` | 字符串 | 包含 |
| startsWith | `Column` | 字符串 | 前缀匹配 |
| endsWith | `Column` | 字符串 | 后缀匹配 |
| substr | `Column` | 字符串 | 子字符串 |
| getField | `Column` | 结构 | 获取结构字段 |
| getItem | `Column` | 数组 | 获取数组元素 |
| dropFields | `Column` | 结构 | 删除结构字段 |
| withField | `Column` | 结构 | 添加结构字段 |
| over | `Column` | 窗口 | 窗口函数 |
| bitwiseAND | `Column` | 位运算 | 位与 |
| bitwiseOR | `Column` | 位运算 | 位或 |
| bitwiseXOR | `Column` | 位运算 | 位异或 |
| transform | `Column` | 数组 | 数组转换 |
| expr | `Column` | 表达式 | 从表达式创建 |

### 2.3 GroupedData/RelationalGroupedDataset
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/RelationalGroupedDataset.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| agg | `DataFrame` | 聚合 | 聚合计算 |
| avg | `DataFrame` | 聚合 | 平均值 |
| max | `DataFrame` | 聚合 | 最大值 |
| min | `DataFrame` | 聚合 | 最小值 |
| sum | `DataFrame` | 聚合 | 求和 |
| count | `DataFrame` | 聚合 | 计数 |
| mean | `DataFrame` | 聚合 | 平均值 |
| pivot | `RelationalGroupedDataset` | 聚合 | Pivot透视 |

### 2.4 KeyValueGroupedDataset（类型化分组）
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/KeyValueGroupedDataset.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| mapValues | `Dataset[U]` | 转换 | 值映射 |
| flatMapValues | `Dataset[U]` | 转换 | 展平值映射 |
| reduce | `Dataset[(K,V)]` | 聚合 | 类型化reduce |
| agg | `DataFrame` | 聚合 | 聚合计算 |
| mapGroups | `Dataset[U]` | 分组处理 | 分组处理 |
| flatMapGroups | `Dataset[U]` | 分组处理 | 展平分组处理 |
| mapGroupsWithState | `Dataset[U]` | 状态处理 | 有状态处理 |
| flatMapGroupsWithState | `Dataset[U]` | 状态处理 | 展平有状态处理 |
| cogroup | `Dataset[U]` | 协分组 | 协分组处理 |

---

## 三、数据读写API

### 3.1 DataFrameReader
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/DataFrameReader.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| format | `DataFrameReader` | 格式 | 指定格式 |
| schema | `DataFrameReader` | Schema | 指定Schema |
| option | `DataFrameReader` | 配置 | 设置选项 |
| options | `DataFrameReader` | 配置 | 批量设置选项 |
| load | `DataFrame` | 加载 | 加载数据 |
| json | `DataFrame` | 格式 | JSON文件 |
| csv | `DataFrame` | 格式 | CSV文件 |
| parquet | `DataFrame` | 格式 | Parquet文件 |
| orc | `DataFrame` | 格式 | ORC文件 |
| text | `DataFrame` | 格式 | 文本文件 |
| avro | `DataFrame` | 格式 | Avro文件 |
| jdbc | `DataFrame` | 数据库 | JDBC连接 |
| table | `DataFrame` | 表 | 表读取 |

### 3.2 DataFrameWriter
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/DataFrameWriter.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| format | `DataFrameWriter` | 格式 | 指定格式 |
| mode | `DataFrameWriter` | 模式 | SaveMode设置 |
| option | `DataFrameWriter` | 配置 | 设置选项 |
| options | `DataFrameWriter` | 配置 | 批量设置选项 |
| partitionBy | `DataFrameWriter` | 分区 | 分区列 |
| bucketBy | `DataFrameWriter` | 分桶 | 分桶 |
| sortBy | `DataFrameWriter` | 分桶排序 | 分桶排序 |
| save | `Unit` | 写入 | 写入数据 |
| saveAsTable | `Unit` | 表 | 写入表 |
| insertInto | `Unit` | 表 | 插入表 |
| json | `Unit` | 格式 | JSON输出 |
| csv | `Unit` | 格式 | CSV输出 |
| parquet | `Unit` | 格式 | Parquet输出 |
| orc | `Unit` | 格式 | ORC输出 |
| text | `Unit` | 格式 | 文本输出 |
| avro | `Unit` | 格式 | Avro输出 |
| jdbc | `Unit` | 数据库 | JDBC输出 |

### 3.3 DataFrameWriterV2
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/DataFrameWriterV2.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| using | `DataFrameWriterV2` | 格式 | 指定格式 |
| option | `DataFrameWriterV2` | 配置 | 设置选项 |
| tableProperty | `DataFrameWriterV2` | 属性 | 表属性 |
| partitionedBy | `DataFrameWriterV2` | 分区 | 分区列 |
| create | `Unit` | 操作 | 创建表 |
| replace | `Unit` | 操作 | 替换表 |
| createOrReplace | `Unit` | 操作 | 创建或替换 |
| append | `Unit` | 操作 | 追加数据 |
| overwrite | `Unit` | 操作 | 覆盖数据 |
| overwritePartitions | `Unit` | 操作 | 覆盖分区 |

### 3.4 MergeIntoWriter（4.0新增）
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/MergeIntoWriter.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| whenMatched | `WhenMatched` | 条件 | 匹配时处理 |
| whenNotMatched | `WhenNotMatched` | 条件 | 不匹配时处理 |
| whenNotMatchedBySource | `WhenNotMatchedBySource` | 条件 | 源不匹配时处理 |
| withSchemaEvolution | `MergeIntoWriter` | Schema | 启用Schema演进 |
| merge | `Unit` | 执行 | 执行Merge |

---

## 四、流处理API（Structured Streaming）

### 4.1 DataStreamReader
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/streaming/DataStreamReader.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| format | `DataStreamReader` | 格式 | 指定格式 |
| schema | `DataStreamReader` | Schema | 指定Schema |
| option | `DataStreamReader` | 配置 | 设置选项 |
| options | `DataStreamReader` | 配置 | 批量设置选项 |
| load | `DataFrame` | 加载 | 创建流DataFrame |

### 4.2 DataStreamWriter
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/streaming/DataStreamWriter.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| format | `DataStreamWriter` | 格式 | 输出格式 |
| option | `DataStreamWriter` | 配置 | 设置选项 |
| options | `DataStreamWriter` | 配置 | 批量设置选项 |
| outputMode | `DataStreamWriter` | 模式 | 输出模式 |
| partitionBy | `DataStreamWriter` | 分区 | 分区列 |
| trigger | `DataStreamWriter` | 触发 | Trigger设置 |
| foreach | `DataStreamWriter` | 输出 | foreach输出 |
| foreachBatch | `DataStreamWriter` | 输出 | foreachBatch输出 |
| start | `StreamingQuery` | 启动 | 启动流查询 |
| toTable | `StreamingQuery` | 输出 | 写入表（新增） |

### 4.3 StreamingQuery
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/streaming/StreamingQuery.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| id | `String` | 信息 | 查询ID |
| runId | `String` | 信息 | 运行ID |
| name | `String` | 信息 | 查询名称 |
| status | `StreamingQueryStatus` | 状态 | 当前状态 |
| lastProgress | `StreamingQueryProgress` | 进度 | 最新进度 |
| progress | `Array[StreamingQueryProgress]` | 进度 | 进度历史 |
| exception | `Option[StreamingQueryException]` | 错误 | 异常信息 |
| stop | `Unit` | 控制 | 停止查询 |
| awaitTermination | `Unit` | 控制 | 等待终止 |

### 4.4 StreamingQueryManager
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/streaming/StreamingQueryManager.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| active | `Array[StreamingQuery]` | 管理 | 活动查询列表 |
| get | `StreamingQuery` | 管理 | 获取查询 |
| awaitAnyTermination | `Unit` | 控制 | 等待任一终止 |
| addListener | `Unit` | 监控 | 添加监听器 |
| removeListener | `Unit` | 监控 | 移除监听器 |

### 4.5 Trigger
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/execution/streaming/Triggers.scala`

| 类型 | 描述 |
|------|------|
| ProcessingTime | 定时触发 |
| Continuous | 连续处理 |
| Once | 单次触发 |
| AvailableNow | 可用数据触发（新增） |

### 4.6 GroupState（有状态处理）
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/streaming/GroupState.scala`

| 方法 | 筋名 | 分类 | 描述 |
|------|------|------|------|
| exists | `Boolean` | 状态 | 状态是否存在 |
| get | `S` | 状态 | 获取状态 |
| update | `Unit` | 状态 | 更新状态 |
| remove | `Unit` | 状态 | 移除状态 |
| hasTimedOut | `Boolean` | 超时 | 是否超时 |
| setTimeoutTimestamp | `Unit` | 超时 | 设置超时时间戳 |
| setTimeoutDuration | `Unit` | 超时 | 设置超时时长 |

---

## 五、Catalog元数据API

### 5.1 Catalog
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/catalog/Catalog.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| currentDatabase | `String` | 数据库 | 当前数据库 |
| setCurrentDatabase | `Unit` | 数据库 | 设置数据库 |
| listDatabases | `Dataset[Database]` | 数据库 | 列出数据库 |
| listDatabases(pattern) | `Dataset[Database]` | 数据库 | 模式匹配列出（3.5新增） |
| getDatabase | `Database` | 数据库 | 获取数据库 |
| databaseExists | `Boolean` | 数据库 | 数据库是否存在 |
| listTables | `Dataset[Table]` | 表 | 列出表 |
| listTables(pattern) | `Dataset[Table]` | 表 | 模式匹配列出（3.5新增） |
| getTable | `Table` | 表 | 获取表 |
| tableExists | `Boolean` | 表 | 表是否存在 |
| listFunctions | `Dataset[Function]` | 函数 | 列出函数 |
| listFunctions(pattern) | `Dataset[Function]` | 函数 | 模式匹配列出（3.5新增） |
| getFunction | `Function` | 函数 | 获取函数 |
| functionExists | `Boolean` | 函数 | 函数是否存在 |
| listColumns | `Dataset[Column]` | 列 | 列出列 |
| createTable | `Unit` | 创建 | 创建表 |
| dropTempView | `Boolean` | 删除 | 删除临时视图 |
| dropGlobalTempView | `Boolean` | 删除 | 删除全局临时视图 |
| clearCache | `Unit` | 缓存 | 清除缓存 |
| refreshTable | `Unit` | 刷新 | 刷新表 |
| recoverPartitions | `Unit` | 分区 | 恢复分区 |
| cacheTable | `Unit` | 缓存 | 缓存表 |
| uncacheTable | `Unit` | 缓存 | 解除缓存 |

### 5.2 Catalog元数据类型
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/catalog/interface.scala`

| 类 | 描述 |
|------|------|
| Database | 数据库信息（name, description, locationUri） |
| Table | 表信息（name, database, description, tableType, isTemporary） |
| Function | 函数信息（name, database, description, className, isTemporary） |
| Column | 列信息（name, description, dataType, nullable, isPartition） |

---

## 六、内置函数API

### 6.1 functions对象
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/functions.scala`

#### 基础函数
| 方法 | 分类 | 描述 |
|------|------|------|
| col / column | 列引用 | 创建列引用 |
| lit | 常量 | 创建常量列 |
| typedLit / typedlit | 常量 | 类型化常量 |
| expr | 表达式 | 从SQL表达式创建 |

#### 数学函数
| 方法 | 分类 | 描述 |
|------|------|------|
| abs | 数学 | 绝对值 |
| ceil / ceiling | 数学 | 向上取整 |
| floor | 数学 | 向下取整 |
| round | 数学 | 四舍五入 |
| bround | 数学 | 银行家舍入 |
| exp | 数学 | e指数 |
| expm1 | 数学 | e指数-1 |
| log / ln | 数学 | 自然对数 |
| log10 | 数学 | 10对数 |
| log2 | 数学 | 2对数 |
| log1p | 数学 | ln(1+x) |
| pow / power | 数学 | 幂运算 |
| sqrt | 数学 | 平方根 |
| cbrt | 数学 | 立方根 |
| sin / sinh | 数学 | 正弦 |
| cos / cosh | 数学 | 余弦 |
| tan / tanh | 数学 | 正切 |
| atan / atan2 / atanh | 数学 | 反正切 |
| asin / asinh | 数学 | 反正弦 |
| acos / acosh | 数学 | 反余弦 |
| cot | 数学 | 余切 |
| degrees | 数学 | 弧度转角度 |
| radians | 数学 | 角度转弧度 |
| factorial | 数学 | 阶乘 |
| sign / signum | 数学 | 符号 |
| rand / randn | 数学 | 随机数 |
| random | 数学 | 随机数 |
| uniform | 数学 | 均匀分布随机数（新增） |
| hex / unhex | 数学 | 十六进制 |
| bin | 数学 | 二进制 |
| conv | 数学 | 进制转换 |
| pmod | 数学 | 正模 |
| greatest | 数学 | 最大值 |
| least | 数学 | 最小值 |
| hypot | 数学 | 勾股定理 |
| width_bucket | 数学 | 分桶（新增） |
| try_add/try_divide/try_multiply/try_subtract/try_mod | 数学 | 安全运算 |

#### 字符串函数
| 方法 | 分类 | 描述 |
|------|------|------|
| upper / ucase | 字符串 | 大写 |
| lower / lcase | 字符串 | 小写 |
| initcap | 字符串 | 首字母大写 |
| length | 字符串 | 长度 |
| char_length / character_length | 字符串 | 长度 |
| concat | 字符串 | 连接 |
| concat_ws | 字符串 | 分隔连接 |
| split | 字符串 | 分割 |
| split_part | 字符串 | 分割取部分（新增） |
| substring / substr | 字符串 | 子字符串 |
| substring_index | 字符串 | 子字符串索引 |
| left / right | 字符串 | 左/右取 |
| lpad / rpad | 字符串 | 左/右填充 |
| trim / ltrim / rtrim / btrim | 字符串 | 剪裁 |
| repeat | 字符串 | 重复 |
| reverse | 字符串 | 反转 |
| replace | 字符串 | 替换 |
| regexp_replace | 字符串 | 正则替换 |
| regexp_extract | 字符串 | 正则提取 |
| regexp_extract_all | 字符串 | 正则提取全部 |
| regexp_count | 字符串 | 正则计数（新增） |
| regexp_instr | 字符串 | 正则位置（新增） |
| regexp_substr | 字符串 | 正则子串（新增） |
| regexp / regexp_like / rlike | 字符串 | 正则匹配 |
| like / ilike | 字符串 | LIKE匹配 |
| contains | 字符串 | 包含 |
| startswith / endswith | 字符串 | 前缀/后缀 |
| locate / position | 字符串 | 定位 |
| instr | 字符串 | 位置 |
| ascii | 字符串 | ASCII |
| chr / char | 字符串 | 字符 |
| base64 / unbase64 | 字符串 | Base64 |
| encode / decode | 字符串 | 编码解码 |
| format_number | 字符串 | 格式化数字 |
| format_string / printf | 字符串 | 格式化字符串 |
| translate | 字符串 | 字符映射 |
| overlay | 字符串 | 覆盖 |
| elt | 字符串 | 选择元素 |
| levenshtein | 字符串 | 编辑距离 |
| soundex | 字符串 | Soundex编码 |
| find_in_set | 字符串 | 集合查找 |
| sentences | 字符串 | 分句 |
| mask | 字符串 | 遮蔽（新增） |
| quote | 字符串 | 引号包裹（新增） |
| randstr | 字符串 | 随机字符串（新增） |
| to_char/to_varchar | 字符串 | 转字符串（新增） |
| to_binary/try_to_binary | 字符串 | 转二进制（新增） |
| to_number/try_to_number | 字符串 | 转数字（新增） |
| bit_length/octet_length | 字符串 | 位/字节长度 |
| is_valid_utf8/try_validate_utf8/make_valid_utf8 | 字符串 | UTF8校验（新增） |
| collate/collation | 字符串 | 排序规则（新增） |

#### 日期时间函数
| 方法 | 分类 | 描述 |
|------|------|------|
| current_date / curdate | 日期 | 当前日期 |
| current_timestamp / now | 日期 | 当前时间戳 |
| current_time | 日期 | 当前时间 |
| current_timezone | 日期 | 当前时区 |
| localtimestamp | 日期 | 本地时间戳 |
| date_add / dateadd | 日期 | 日期加 |
| date_sub | 日期 | 日期减 |
| date_diff / datediff | 日期 | 日期差 |
| add_months | 日期 | 加月份 |
| months_between | 日期 | 月份差 |
| last_day | 日期 | 月末 |
| next_day | 日期 | 下个周几 |
| day / dayofmonth | 日期 | 日 |
| dayofweek | 日期 | 周几 |
| dayofyear | 日期 | 年日 |
| dayname | 日期 | 星期名（新增） |
| weekofyear / week | 日期 | 周数 |
| weekday | 日期 | 周几索引 |
| month / monthname | 日期 | 月 |
| quarter | 日期 | 季度 |
| year | 日期 | 年 |
| hour / minute / second | 日期 | 时分秒 |
| make_date | 日期 | 构造日期 |
| make_timestamp / make_timestamp_ltz / make_timestamp_ntz | 日期 | 构造时间戳 |
| make_time | 日期 | 构造时间 |
| make_dt_interval / make_ym_interval / make_interval | 日期 | 构造间隔 |
| try_make_timestamp | 日期 | 安全构造 |
| to_date / try_to_date | 日期 | 转日期 |
| to_timestamp / to_timestamp_ltz / to_timestamp_ntz / try_to_timestamp | 日期 | 转时间戳 |
| to_time / try_to_time | 日期 | 转时间（新增） |
| to_unix_timestamp | 日期 | 转Unix时间戳 |
| unix_date | 日期 | Unix日期 |
| unix_timestamp / unix_seconds / unix_millis / unix_micros | 日期 | Unix时间 |
| timestamp_seconds / timestamp_millis / timestamp_micros | 日期 | 时间戳 |
| from_unixtime | 日期 | Unix转时间 |
| from_utc_timestamp | 日期 | UTC转时间戳 |
| to_utc_timestamp | 日期 | 转UTC时间戳 |
| date_format | 日期 | 日期格式化 |
| date_trunc / trunc | 日期 | 日期截断 |
| time_trunc | 日期 | 时间截断（新增） |
| timestamp_add | 日期 | 时间戳加 |
| timestamp_diff | 日期 | 时间戳差 |
| time_diff | 日期 | 时间差（新增） |
| date_part / extract | 日期 | 日期部分提取 |
| datepart | 日期 | 日期部分（新增） |
| convert_timezone | 日期 | 时区转换 |
| date_from_unix_date | 日期 | Unix日期转 |

#### 条件函数
| 方法 | 分类 | 描述 |
|------|------|------|
| when | 条件 | CASE WHEN |
| coalesce | 条件 | 非空值 |
| ifnull | 条件 | IFNULL |
| nullif | 条件 | NULLIF |
| nullifzero / zeroifnull | 条件 | NULL条件 |
| nvl / nvl2 | 条件 | NVL |
| equal_null | 条件 | 空值安全等于 |

#### 集合/数组函数
| 方法 | 分类 | 描述 |
|------|------|------|
| array | 数组 | 创建数组 |
| array_contains | 数组 | 包含检查 |
| array_append | 数组 | 添加元素 |
| array_insert | 数组 | 插入元素 |
| array_remove | 数组 | 删除元素 |
| array_position | 数组 | 元素位置 |
| array_size / size | 数组 | 数组大小 |
| array_sort | 数组 | 排序 |
| array_distinct | 数组 | 唯一值 |
| array_union | 数组 | 合集 |
| array_intersect | 数组 | 交集 |
| array_except | 数组 | 差集 |
| array_compact | 数组 | 去空值 |
| array_repeat | 数组 | 重复数组 |
| array_prepend | 数组 | 前置元素 |
| array_reverse | 数组 | 反转 |
| array_join | 数组 | 连接 |
| flatten | 数组 | 展平 |
| explode | 数组 | 爆炸展开 |
| explode_outer | 数组 | 外部爆炸 |
| posexplode / posexplode_outer | 数组 | 带位置爆炸 |
| collect_list | 数组 | 收集列表 |
| collect_set | 数组 | 收集集合 |
| sequence | 数组 | 序列生成 |
| shuffle | 数组 | 随机排列 |
| slice | 数组 | 切片 |
| arrays_overlap | 数组 | 重叠检查 |
| arrays_zip | 数组 | Zip合并 |
| arrays_zip_with | 数组 | Zip合并转换 |
| element_at | 数组 | 元素访问 |
| array_max / array_min | 数组 | 最大最小 |
| array_agg | 数组 | 聚合数组 |
| filter | 数组 | 过滤 |
| forall | 数组 | 全满足检查 |
| exists | 数组 | 存在检查 |
| aggregate / reduce | 数组 | 聚合 |
| transform | 数组 | 转换 |
| zip_with | 数组 | Zip转换 |
| sort_array | 数组 | 排序 |
| get | 数组 | 获取元素（新增） |

#### Map函数
| 方法 | 分类 | 描述 |
|------|------|------|
| map | Map | 创建Map |
| map_from_arrays | Map | 数组转Map |
| map_from_entries | Map | 条目转Map |
| map_keys | Map | 获取Keys |
| map_values | Map | 获取Values |
| map_entries | Map | 获取条目 |
| map_contains_key | Map | 包含检查 |
| map_get | Map | 获取值 |
| element_at | Map | 元素访问 |
| map_concat | Map | 合并 |
| map_filter | Map | 过滤 |
| map_zip_with | Map | Zip合并 |
| map_agg | Map | 聚合Map |
| transform_keys / transform_values | Map | 转换Keys/Values |
| get | Map | 获取元素（新增） |

#### 结构函数
| 方法 | 分类 | 描述 |
|------|------|------|
| struct | 结构 | 创建结构 |
| named_struct | 结构 | 命名结构 |

#### JSON函数
| 方法 | 分类 | 描述 |
|------|------|------|
| get_json_object | JSON | JSON对象提取 |
| json_tuple | JSON | JSON元组 |
| from_json | JSON | JSON解析 |
| to_json | JSON | JSON生成 |
| schema_of_json | JSON | JSONSchema |
| json_array_length | JSON | JSON数组长度 |

#### CSV函数
| 方法 | 分类 | 描述 |
|------|------|------|
| from_csv | CSV | CSV解析 |
| to_csv | CSV | CSV生成 |
| schema_of_csv | CSV | CSV Schema |

#### XML函数
| 方法 | 分类 | 描述 |
|------|------|------|
| from_xml | XML | XML解析 |
| schema_of_xml | XML | XML Schema |

#### Variant函数（新增）
| 方法 | 分类 | 描述 |
|------|------|------|
| variant | Variant | Variant类型 |
| parse_variant | Variant | 解析Variant |
| try_parse_variant | Variant | 安全解析 |
| is_variant_null | Variant | Variant空检查 |
| variant_get | Variant | Variant提取 |
| try_variant_get | Variant | 安全提取 |
| to_variant_object | Variant | 转Variant对象 |
| cast_variant | Variant | Variant转换 |

#### 聚合函数
| 方法 | 分类 | 描述 |
|------|------|------|
| count | 聚合 | 计数 |
| count_distinct | 聚合 | 唯一计数 |
| sum | 聚合 | 求和 |
| avg / mean | 聚合 | 平均值 |
| max | 聚合 | 最大值 |
| min | 聚合 | 最小值 |
| first | 聚合 | 第一个值 |
| first_value | 聚合 | 第一个值（新增） |
| last | 聚合 | 最后一个值 |
| last_value | 聚合 | 最后一个值（新增） |
| any_value | 聚合 | 任一值（新增） |
| variance / var_samp / var_pop | 聚合 | 方差 |
| stddev / stddev_samp / stddev_pop | 聚合 | 标准差 |
| skewness | 聚合 | 偏度 |
| kurtosis | 聚合 | 峰度 |
| corr | 聚合 | 相关系数 |
| covar_pop / covar_samp | 聚合 | 协方差 |
| regr_count/regr_avgx/regr_avgy/regr_slope/regr_intercept/regr_r2/regr_sxx/regr_syy/regr_sxy | 聚合 | 线性回归 |
| approx_count_distinct | 聚合 | 近似唯一计数 |
| approx_distinct | 聚合 | 近似唯一值（新增） |
| approx_percentile | 聚合 | 近似百分位 |
| percentile | 聚合 | 百分位（新增） |
| percentile_approx | 聚合 | 近似百分位 |
| mode | 聚合 | 众数（新增） |
| grouping | 葛合 | 分组标识 |
| grouping_id | 葛合 | 分组ID |
| bit_and/bit_or/bit_xor | 葛合 | 位聚合 |
| every / any / some | 葛合 | 布尔聚合 |
| product | 葛合 | 乘积（新增） |
| sum_distinct | 葛合 | 唯一值求和（新增） |

#### 窗口函数
| 方法 | 分类 | 描述 |
|------|------|------|
| row_number | 窗口 | 行号 |
| rank | 窗口 | 排名 |
| dense_rank | 窗口 | 紧密排名 |
| percent_rank | 窗口 | 百分排名 |
| ntile | 窗口 | N分桶 |
| cume_dist | 窗口 | 累积分布 |
| lead | 窗口 | 向前 |
| lag | 窗口 | 向后 |
| first_value | 窗口 | 窗口首个 |
| last_value | 窗口 | 窗口末个 |
| nth_value | 窗口 | 第N个 |
| window | 窗口 | 时间窗口 |
| session_window | 窗口 | 会话窗口 |

#### 位运算函数
| 方法 | 分类 | 描述 |
|------|------|------|
| bit_count | 位运算 | 位计数 |
| bit_get / getbit | 位运算 | 获取位 |
| bitwise_not | 位运算 | 位非 |
| shiftleft | 位运算 | 左移 |
| shiftright | 位运算 | 右移 |
| shiftrightunsigned | 位运算 | 无符号右移 |

#### 哈希函数
| 方法 | 分类 | 描述 |
|------|------|------|
| hash | 哈希 | 哈希值 |
| md5 | 哈希 | MD5 |
| sha1 | 哈希 | SHA1 |
| sha2 | 哈希 | SHA2 |
| xxhash64 | 哈希 | xxHash64 |
| murmur3_hash | 哈希 | Murmur3 |

#### 生成器函数
| 方法 | 分类 | 描述 |
|------|------|------|
| explode | 生成器 | 爆炸展开 |
| explode_outer | 生成器 | 外部爆炸 |
| posexplode | 生成器 | 带位置爆炸 |
| inline / inline_outer | 生成器 | 结构数组展开 |
| stack | 生成器 | 堆叠展开 |
| json_tuple | 生成器 | JSON元组展开 |

#### 空值处理函数
| 方法 | 分类 | 描述 |
|------|------|------|
| isnull / isnotnull | 空值 | 空/非空检查 |
| isnan | 空值 | NaN检查 |
| nanvl | 空值 | NaN替换 |
| coalesce | 空值 | 非空选择 |
| nullif | 空值 | 条件置空 |
| ifnull | 空值 | 空值替换 |

#### 排序函数
| 方法 | 分类 | 描述 |
|------|------|------|
| asc | 排序 | 升序 |
| desc | 排序 | 降序 |
| asc_nulls_first / asc_nulls_last | 排序 | 空值位置 |
| desc_nulls_first / desc_nulls_last | 排序 | 空值位置 |

#### 广播/提示函数
| 方法 | 分类 | 描述 |
|------|------|------|
| broadcast | 提示 | 广播提示 |

#### 分区转换函数
| 方法 | 分类 | 描述 |
|------|------|------|
| years | 分区 | 年分区 |
| months | 分区 | 月分区 |
| days | 分区 | 日分区 |
| hours | 分区 | 时分区 |
| bucket | 分区 | 分桶 |
| iceberg_transforms | 分区 | Iceberg转换 |

#### 地理空间函数（新增）
| 方法 | 分类 | 描述 |
|------|------|------|
| st_point | 地理 | 创建点 |
| st_geomfromwkt | 地理 | WKT转几何 |
| st_geomfromwkb | 地理 | WKB转几何 |
| st_aswkt / st_astext | 地理 | 转WKT |
| st_aswkb / st_asbinary | 地理 | 转WKB |
| st_distance | 地理 | 距离 |
| st_contains | 地理 | 包含检查 |
| st_intersects | 地理 | 交集检查 |
| st_within | 地理 | 在内部检查 |
| st_area | 地理 | 面积 |
| st_length | 地理 | 长度 |
| st_buffer | 地理 | 缓冲区 |
| st_convexhull | 地理 | 凸包 |
| st_centroid | 地理 | 中心点 |
| st_x / st_y | 地理 | X/Y坐标 |
| st_makepoint | 地理 | 创建点 |
| st_linestring | 地理 | 创建线 |
| st_polygon | 地理 | 创建多边形 |
| st_multipoint | 地理 | 创建多点 |
| st_multilinestring | 地理 | 创建多线 |
| st_multipolygon | 地理 | 创建多边形 |
| st_geometrycollection | 地理 | 几何集合 |

#### Pandas UDF支持
| 方法 | 分类 | 描述 |
|------|------|------|
| pandas_udf | UDF | Pandas UDF创建 |

---

## 七、UDF注册API

### 7.1 UDFRegistration
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/UDFRegistration.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| register | `Unit` | UDF | 注册Scala UDF |
| registerJava | `Unit` | UDF | 注册Java UDF |
| registerPython | `Unit` | UDF | 注册Python UDF |
| registerSQL | `Unit` | UDF | 注册SQL UDF |
| registerUDF | `Unit` | UDF | 遗留注册 |

### 7.2 UDTFRegistration（新增）
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/UDTFRegistration.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| register | `Unit` | UDTF | 注册表值函数 |

---

## 八、数据类型API

### 8.1 DataType类型系统
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/types/`

| 类型 | 描述 |
|------|------|
| NullType | 空类型 |
| BooleanType | 布尔 |
| ByteType | 字节 |
| ShortType | 短整型 |
| IntegerType | 整型 |
| LongType | 长整型 |
| FloatType | 浮点 |
| DoubleType | 双精度 |
| DecimalType | 十进制 |
| StringType / CharType / VarcharType | 字符串 |
| BinaryType | 二进制 |
| DateType | 日期 |
| TimestampType / TimestampNTZType | 时间戳 |
| TimeType | 时间（新增） |
| CalendarIntervalType | 日历间隔 |
| YearMonthIntervalType | 年月间隔 |
| DayTimeIntervalType | 日时间隔 |
| ArrayType | 数组 |
| MapType | Map |
| StructType / StructField | 结构 |
| VariantType | Variant（新增） |
| UserDefinedType | 用户定义类型 |
| GeographyType / GeometryType | 地理类型（新增） |

### 8.2 Row API
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/Row.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| get | `Any` | 获取 | 按索引获取值 |
| getInt / getLong / getDouble... | 基本类型 | 按类型获取 |
| getString | `String` | 获取 | 获取字符串 |
| getSeq / getList | `Seq/List` | 获取 | 获取序列 |
| getMap | `Map` | 获取 | 获取Map |
| getStruct | `Row` | 获取 | 获取结构 |
| isNullAt | `Boolean` | 空值 | 是否为空 |
| schema | `StructType` | 元数据 | Schema |
| length / size | `Int` | 信息 | 长度 |
| toSeq | `Seq[Any]` | 转换 | 转序列 |
| asDict | `Map[String,Any]` | 转换 | 转字典 |
| apply | `Any` | 获取 | 按索引/列名获取 |
| getValuesMap | `Map[String,Any]` | 获取 | 按列名获取Map |

---

## 九、配置API

### 9.1 RuntimeConfig
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/RuntimeConfig.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| get | `String` | 配置 | 获取配置值 |
| getOption | `Option[String]` | 配置 | 获取可选配置 |
| getAll | `Map[String,String]` | 配置 | 获取所有配置 |
| set | `Unit` | 配置 | 设置配置 |
| unset | `Unit` | 配置 | 清除配置 |
| isModifiable | `Boolean` | 配置 | 是否可修改 |

---

## 十、观察/监控API

### 10.1 Observation
**位置**: `sql/api/src/main/scala/org/apache/spark/sql/Observation.scala`

| 方法 | 签名 | 分类 | 描述 |
|------|------|------|------|
| get | `Map[String,Any]` | 结果 | 获取观察结果 |
| waitFor | `Map[String,Any]` | 等待 | 等待结果 |

---

## 十一、Spark Connect API

### 11.1 Connect特有API
Connect模块提供了远程执行能力，主要差异：

| 特性 | 描述 |
|------|------|
| SparkSession.builder.remote(url) | 连接远程Spark服务器 |
| addArtifact | 上传资源文件 |
| addArtifacts | 批量上传资源 |
| copyFromLocalToFs | 本地文件复制到远程 |
| client | 访问Connect客户端 |
| registerProgressHandler | 注册进度处理回调 |

---

## 十二、API调用关系图

```
SparkSession (入口)
    │
    ├── read → DataFrameReader
    │       ├── format/option/schema
    │       └── load/json/csv/parquet/jdbc... → DataFrame
    │
    ├── readStream → DataStreamReader → DataFrame (流)
    │
    ├── sql(query) → DataFrame
    │
    ├── catalog → Catalog
    │       ├── listDatabases/listTables/listFunctions
    │       └── createTable/dropTempView/cacheTable...
    │
    ├── udf → UDFRegistration
    │       └── register/registerJava/registerPython
    │
    └── streams → StreamingQueryManager
            └── active/get/awaitAnyTermination

DataFrame/Dataset (核心数据结构)
    │
    ├── 转换操作（惰性）
    │       ├── select/filter/join/groupBy/agg
    │       ├── map/flatMap/mapPartitions
    │       ├── union/intersect/except
    │       ├── repartition/coalesce
    │       ├── withColumn/drop/dropDuplicates
    │       └── withWatermark (流)
    │
    ├── 动作操作（触发执行）
    │       ├── show/collect/take/head/count
    │       ├── foreach/foreachPartition
    │       └── write/writeStream
    │
    └── write → DataFrameWriter
            ├── format/mode/option/partitionBy
            └── save/saveAsTable/json/csv/parquet/jdbc...

GroupedData/RelationalGroupedDataset (分组后)
    │
    ├── agg/count/sum/avg/max/min
    └── pivot

KeyValueGroupedDataset (类型化分组)
    │
    ├── mapValues/flatMapValues
    ├── mapGroups/flatMapGroups
    ├── mapGroupsWithState (有状态)
    └── reduce/agg

StreamingQuery (流查询)
    │
    ├── status/lastProgress/exception
    └── stop/awaitTermination

functions (内置函数)
    │
    ├── 数学函数: abs/ceil/floor/round/sqrt...
    ├── 字符串函数: upper/lower/concat/split...
    ├── 日期函数: current_date/date_add/months_between...
    ├── 聚合函数: count/sum/avg/max/min/corr...
    ├── 窗口函数: row_number/rank/lag/lead
    ├── 数组函数: array/explode/collect_list...
    ├── Map函数: map/map_keys/map_values...
    ├── JSON函数: get_json_object/from_json...
    └── 条件函数: coalesce/when/nullif...
```

---

## 十三、版本演进

### Spark 3.x → 4.x 主要新增API

| API | 版本 | 描述 |
|------|------|------|
| MergeIntoWriter | 4.0 | MERGE INTO SQL支持 |
| groupingSets | 4.0 | 自定义分组集 |
| lateralJoin | 4.0 | LATERAL连接 |
| offset | 4.0 | 数据偏移 |
| transpose | 4.0 | DataFrame转置 |
| toTable (Streaming) | 4.0 | 流写入表 |
| TimeType | 4.0 | 时间类型 |
| VariantType | 4.0 | Variant类型 |
| GeographyType/GeometryType | 4.0 | 地理类型 |
| dropDuplicatesWithinWatermark | 3.5 | 流数据去重 |
| any_value/approx_distinct/mode/product | 3.5+ | 新聚合函数 |
| listDatabases(pattern)/listTables(pattern)/listFunctions(pattern) | 3.5 | 模式匹配列表 |
| uniform/randstr/mask/width_bucket | 3.5+ | 新数学/字符串函数 |
| time_diff/to_time/try_to_time | 3.5+ | 新时间函数 |
| regexp_count/regexp_instr/regexp_substr | 3.5+ | 新正则函数 |
| is_valid_utf8/make_valid_utf8 | 3.5+ | UTF8校验函数 |
| collate/collation | 3.5+ | 排序规则函数 |
| dayname/monthname | 3.5+ | 日期名称函数 |
| datepart | 3.5+ | 日期部分提取 |

---

## 十四、组件交互分析

### 14.1 Spark与Hive交互

| 交互点 | API | 描述 |
|------|------|------|
| SparkSession.enableHiveSupport | Builder | 启用Hive支持 |
| spark.sql(hiveQuery) | SparkSession | 执行Hive SQL |
| spark.read.table("hiveTable") | DataFrameReader | 读取Hive表 |
| df.write.saveAsTable("hiveTable") | DataFrameWriter | 写入Hive表 |
| spark.catalog.listTables | Catalog | 列出Hive表 |
| spark.catalog.createTable | Catalog | 创建Hive表 |
| Hive Metastore | 配置 | 元数据管理 |

### 14.2 Spark与HDFS交互

| 交互点 | API | 描述 |
|------|------|------|
| spark.read.parquet/hdfsPath | DataFrameReader | 读取HDFS Parquet |
| spark.read.csv/hdfsPath | DataFrameReader | 读取HDFS CSV |
| spark.read.text/hdfsPath | DataFrameReader | 读取HDFS文本 |
| spark.read.json/hdfsPath | DataFrameReader | 读取HDFS JSON |
| df.write.parquet/hdfsPath | DataFrameWriter | 写入HDFS Parquet |
| df.inputFiles | Dataset | 查看输入文件 |
| Hadoop Configuration | SparkSession | HDFS配置 |

### 14.3 Spark与Kafka交互（流处理）

| 交互点 | API | 描述 |
|------|------|------|
| spark.readStream.format("kafka") | DataStreamReader | Kafka源 |
| df.writeStream.format("kafka") | DataStreamWriter | Kafka sink |
| option("kafka.bootstrap.servers") | 配置 | Kafka配置 |
| option("subscribe"/"subscribePattern") | 配置 | 订阅Topic |
| option("startingOffsets"/"endingOffsets") | 配置 | 偏移量配置 |

### 14.4 Spark与HBase交互

| 交互点 | API | 描述 |
|------|------|------|
| spark.read.format("hbase") | DataFrameReader | HBase读取 |
| df.write.format("hbase") | DataFrameWriter | HBase写入 |
| option("hbase.table") | 配置 | HBase表名 |

### 14.5 Spark与JDBC交互

| 交互点 | API | 描述 |
|------|------|------|
| spark.read.jdbc(url, table, props) | DataFrameReader | JDBC读取 |
| df.write.jdbc(url, table, mode) | DataFrameWriter | JDBC写入 |
| JdbcDialects | 配置 | 数据库方言 |

---

## 十五、API使用模式示例

### 15.1 批处理典型模式

```scala
val spark = SparkSession.builder()
  .appName("batch-job")
  .enableHiveSupport()
  .getOrCreate()

val df = spark.read.parquet("hdfs://path/data.parquet")
  .filter($"age" > 18)
  .groupBy($"department")
  .agg(avg($"salary"), count("*"))

df.write.mode("overwrite").saveAsTable("result_table")
```

### 15.2 流处理典型模式

```scala
val spark = SparkSession.builder()
  .appName("stream-job")
  .getOrCreate()

val streamDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host:9092")
  .option("subscribe", "topic")
  .load()

val result = streamDF
  .withWatermark("timestamp", "10 minutes")
  .groupBy(window($"timestamp", "5 minutes"))
  .agg(count("*"))

result.writeStream
  .outputMode("update")
  .format("console")
  .trigger(ProcessingTime("5 seconds"))
  .start()
  .awaitTermination()
```

### 15.3 有状态处理典型模式

```scala
val result = events
  .groupByKey(_.userId)
  .mapGroupsWithState(StateTimeout.ProcessingTimeTimeout) {
    case (userId, events, state) =>
      val prevState = state.getOrElse(InitialState)
      val newState = updateState(prevState, events)
      state.update(newState)
      newState.output
  }
```

---

**文档版本**: v1.0
**生成日期**: 2026-04-28
**基于版本**: Apache Spark 4.2.0-SNAPSHOT

**数据来源**:
- 代码仓: `/home/h00517772/spark/sql/api/src/main/scala/org/apache/spark/sql/`
- 官方文档: https://spark.apache.org/docs/latest/api/