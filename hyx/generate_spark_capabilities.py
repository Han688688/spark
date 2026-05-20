#!/usr/bin/env python3
"""
生成Spark核心能力文档
"""

def generate_spark_capabilities():
    """生成Spark核心能力完整文档"""
    
    doc = []
    
    # 标题和概览
    doc.append("=" * 80)
    doc.append("Apache Spark 核心能力全景分析")
    doc.append("=" * 80)
    doc.append("")
    doc.append("版本: Apache Spark 3.5.x")
    doc.append("分析时间: 2026-05-18")
    doc.append("文档来源: Spark源码 + 官方文档 + API文档")
    doc.append("")
    
    # 一、Spark架构总览
    doc.append("=" * 80)
    doc.append("一、Spark架构总览")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 1.1 整体架构")
    doc.append("")
    doc.append("Apache Spark是一个统一的大数据分析引擎，提供：")
    doc.append("- 统一的编程模型（RDD、DataFrame、Dataset）")
    doc.append("- 统一的执行引擎（DAG调度器）")
    doc.append("- 统一的资源管理（支持YARN、K8s、Standalone）")
    doc.append("- 多语言支持（Scala、Java、Python、R、SQL）")
    doc.append("")
    
    doc.append("### 1.2 核心组件")
    doc.append("")
    doc.append("| 组件 | 位置 | 核心能力 |")
    doc.append("|------|------|----------|")
    doc.append("| **Spark Core** | core/ | RDD计算引擎，底层执行引擎 |")
    doc.append("| **Spark SQL** | sql/ | 结构化数据查询，DataFrame/Dataset |")
    doc.append("| **Spark Streaming** | sql/streaming/ | 流式计算（Structured Streaming） |")
    doc.append("| **MLlib** | mllib/ | 机器学习库（分类、聚类、回归） |")
    doc.append("| **GraphX** | graphx/ | 图计算引擎 |")
    doc.append("| **Spark Connect** | sql/connect/ | 远程客户端连接协议 |")
    doc.append("| **Resource Managers** | resource-managers/ | YARN、Kubernetes资源管理 |")
    doc.append("")
    
    # 二、Spark Core核心能力
    doc.append("=" * 80)
    doc.append("二、Spark Core核心能力")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 2.1 RDD核心API")
    doc.append("")
    doc.append("**核心类**: JavaRDD、JavaPairRDD、JavaDoubleRDD")
    doc.append("")
    doc.append("**方法数量**: 约160+核心方法")
    doc.append("")
    
    # RDD核心功能分类
    rdd_features = [
        ("转换操作(Transformations)", [
            "map - 元素映射",
            "filter - 条件过滤",
            "flatMap - 扁平化映射",
            "distinct - 去重",
            "union - 合并RDD",
            "intersection - 交集",
            "subtract - 差集",
            "sample - 随机采样",
            "repartition - 重新分区",
            "coalesce - 减少分区",
            "sortBy/sortByKey - 排序",
            "groupByKey - 按Key分组",
            "reduceByKey - 按Key聚合",
            "aggregateByKey - 自定义聚合",
            "foldByKey - 带零值聚合",
            "combineByKey - 组合聚合",
            "join - 连接操作",
            "leftJoin/rightJoin - 左/右连接",
            "cogroup - 协同分组",
            "mapValues - Value映射",
            "flatMapValues - Value扁平化",
            "zip - 拉链操作",
            "zipWithIndex - 带索引拉链"
        ]),
        ("行动操作(Actions)", [
            "collect - 收集所有元素",
            "count - 计数",
            "countByValue - 按值计数",
            "countByKey - 按Key计数",
            "first - 第一个元素",
            "take - 取前N个",
            "takeSample - 随机取样",
            "takeOrdered - 排序后取前N",
            "top - 取最大的N个",
            "reduce - 聚合",
            "aggregate - 自定义聚合",
            "fold - 带零值聚合",
            "foreach - 遍历操作",
            "saveAsTextFile - 保存为文本文件",
            "saveAsObjectFile - 保存为对象文件",
            "saveAsSequenceFile - 保存为SequenceFile",
            "countApproxDistinct - 近似去重计数",
            "sum/max/min/mean - 统计计算",
            "variance/stdev - 方差/标准差",
            "histogram - 直方图统计",
            "lookup - 查找Key对应Value"
        ]),
        ("持久化操作", [
            "cache - 缓存到内存",
            "persist - 持久化到指定存储级别",
            "unpersist - 释放缓存"
        ]),
        ("分区控制", [
            "repartition - 增加分区",
            "coalesce - 减少分区",
            "partitionBy - 自定义分区器",
            "mapPartitions - 分区级映射",
            "mapPartitionsWithIndex - 带索引分区映射",
            "foreachPartition - 分区级遍历",
            "glom - 分区聚合为数组"
        ]),
        ("PairRDD专用", [
            "groupByKey - 分组",
            "reduceByKey - 聚合",
            "aggregateByKey - 自定义聚合",
            "foldByKey - 带零值聚合",
            "combineByKey - 组合聚合",
            "join/leftOuterJoin/rightOuterJoin - 连接",
            "cogroup - 协同分组",
            "lookup - 查找",
            "keys - 获取所有Key",
            "values - 获取所有Value",
            "countByKey - 按Key计数",
            "collectAsMap - 收集为Map",
            "mapValues - Value映射",
            "flatMapValues - Value扁平化",
            "subtractByKey - Key差集"
        ])
    ]
    
    for category, methods in rdd_features:
        doc.append(f"#### {category}")
        doc.append("")
        for method in methods:
            doc.append(f"- {method}")
        doc.append("")
    
    doc.append("### 2.2 存储级别(StorageLevel)")
    doc.append("")
    storage_levels = [
        ("MEMORY_ONLY", "仅存储在内存"),
        ("MEMORY_ONLY_SER", "内存中序列化存储"),
        ("MEMORY_AND_DISK", "内存+磁盘"),
        ("MEMORY_AND_DISK_SER", "内存+磁盘序列化"),
        ("DISK_ONLY", "仅存储在磁盘"),
        ("MEMORY_ONLY_2", "内存存储2副本"),
        ("MEMORY_AND_DISK_2", "内存+磁盘2副本"),
        ("OFF_HEAP", "堆外内存存储")
    ]
    
    for level, desc in storage_levels:
        doc.append(f"- {level}: {desc}")
    doc.append("")
    
    doc.append("### 2.3 分区器(Partitioner)")
    doc.append("")
    doc.append("- HashPartitioner: 哈希分区")
    doc.append("- RangePartitioner: 范围分区")
    doc.append("- 自定义分区器: 用户自定义分区逻辑")
    doc.append("")
    
    doc.append("### 2.4 共享变量")
    doc.append("")
    doc.append("- **广播变量(Broadcast Variables)**: 大数据集广播到所有节点")
    doc.append("- **累加器(Accumulators)**: 分布式计数器和聚合器")
    doc.append("  - LongAccumulator: 长整型累加")
    doc.append("  - DoubleAccumulator: 双精度累加")
    doc.append("  - CollectionAccumulator: 集合累加")
    doc.append("")
    
    # 三、Spark SQL核心能力
    doc.append("=" * 80)
    doc.append("三、Spark SQL核心能力")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 3.1 DataFrame/Dataset API")
    doc.append("")
    doc.append("**核心类**: SparkSession、DataFrame、Dataset、Column")
    doc.append("")
    doc.append("**方法数量**: 约200+方法")
    doc.append("")
    
    sql_features = [
        ("DataFrame操作", [
            "select - 选择列",
            "filter/where - 过滤行",
            "groupBy - 分组",
            "orderBy/sort - 排序",
            "limit - 限制行数",
            "join - 连接表",
            "union/unionAll - 合并",
            "intersect - 交集",
            "except - 差集",
            "distinct - 去重",
            "dropDuplicates - 去重",
            "drop - 删除列",
            "withColumn - 新增列",
            "withColumnRenamed - 重命名列",
            "alias - 别名",
            "agg - 聚合",
            "count - 计数",
            "describe - 统计描述",
            "summary - 统计摘要",
            "show - 显示数据",
            "printSchema - 打印Schema",
            "schema - 获取Schema",
            "columns - 获取列名",
            "dtypes - 获取数据类型",
            "na - 缺失值处理",
            "stat - 统计函数",
            "randomSplit - 随机分割",
            "sample - 随机采样",
            "cache/persist - 缓存",
            "createOrReplaceTempView - 创建临时视图",
            "createGlobalTempView - 创建全局临时视图",
            "write - 写入数据源",
            "read - 读取数据源",
            "toDF - 转换为DataFrame"
        ]),
        ("聚合函数", [
            "count - 计数",
            "sum - 求和",
            "avg/mean - 平均值",
            "max - 最大值",
            "min - 最小值",
            "variance/var_pop/var_samp - 方差",
            "stddev/stddev_pop/stddev_samp - 标准差",
            "skewness - 偏度",
            "kurtosis - 峰度",
            "approx_count_distinct - 近似计数",
            "collect_list - 收集列表",
            "collect_set - 收集集合",
            "first/last - 首尾元素",
            "countDistinct - 唯一计数",
            "sumDistinct - 唯一求和"
        ]),
        ("窗口函数", [
            "row_number - 行号",
            "rank - 排名（有间隙）",
            "dense_rank - 排名（无间隙）",
            "percent_rank - 百分比排名",
            "ntile - N分位数",
            "lead - 向后偏移",
            "lag - 向前偏移",
            "first_value - 窗口首值",
            "last_value - 窗口尾值"
        ]),
        ("SQL查询", [
            "spark.sql() - 执行SQL查询",
            "SELECT - 选择",
            "FROM - 数据源",
            "WHERE - 条件过滤",
            "GROUP BY - 分组",
            "HAVING - 分组过滤",
            "ORDER BY - 排序",
            "LIMIT - 限制",
            "JOIN - 连接",
            "UNION - 合并",
            "SUBQUERY - 子查询",
            "CASE WHEN - 条件表达式",
            "WITH - CTE表达式"
        ])
    ]
    
    for category, methods in sql_features:
        doc.append(f"#### {category}")
        doc.append("")
        for method in methods:
            doc.append(f"- {method}")
        doc.append("")
    
    doc.append("### 3.2 数据源Connector")
    doc.append("")
    datasources = [
        ("内置数据源", [
            "Parquet - 列式存储格式",
            "ORC - 列式存储格式",
            "JSON - JSON文本格式",
            "CSV - CSV文本格式",
            "Text - 纯文本格式",
            "JDBC - 关系数据库连接",
            "Hive - Hive表集成",
            "Avro - Avro格式",
            "Delta Lake - Delta格式",
            "Binary - 二进制文件"
        ]),
        ("外部数据源", [
            "Kafka - 流式数据源",
            "Cassandra - 分布式数据库",
            "HBase - Hadoop数据库",
            "Elasticsearch - 搜索引擎",
            "MongoDB - 文档数据库",
            "Redis - 内存数据库",
            "Iceberg - 数据湖格式",
            "Hudi - 数据湖格式",
            "Delta Lake - 数据湖格式"
        ])
    ]
    
    for category, sources in datasources:
        doc.append(f"#### {category}")
        doc.append("")
        for source in sources:
            doc.append(f"- {source}")
        doc.append("")
    
    doc.append("### 3.3 Catalyst优化器")
    doc.append("")
    doc.append("核心优化能力:")
    doc.append("- 解析SQL/DataFrame为逻辑计划")
    doc.append("- 逻辑优化（谓词下推、列裁剪、常量折叠）")
    doc.append("- 物理计划生成（选择最佳执行策略）")
    doc.append("- 代码生成（Whole-Stage Code Generation）")
    doc.append("- Cost-Based Optimization (CBO)")
    doc.append("")
    
    # 四、Spark Streaming核心能力
    doc.append("=" * 80)
    doc.append("四、Spark Streaming核心能力")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 4.1 Structured Streaming")
    doc.append("")
    doc.append("核心概念:")
    doc.append("- 流式DataFrame/Dataset")
    doc.append("- 增量查询")
    doc.append("- 连续查询")
    doc.append("")
    
    streaming_features = [
        ("输入源", [
            "File source - 文件流（目录监控）",
            "Kafka source - Kafka流",
            "Socket source - Socket流",
            "Rate source - 速率源（测试）",
            "自定义源 - Custom source"
        ]),
        ("输出模式", [
            "Append mode - 仅追加新行",
            "Complete mode - 全量输出",
            "Update mode - 仅输出更新行"
        ]),
        ("触发器", [
            "ProcessingTime trigger - 定时触发",
            "Continuous trigger - 连续触发",
            "Once trigger - 单次执行",
            "Available-now trigger - 可用数据立即执行"
        ]),
        ("流式操作", [
            "readStream - 流式读取",
            "writeStream - 流式写入",
            "withWatermark - 水位线定义",
            "groupBy - 流式分组",
            "join - 流流连接",
            "flatMapGroupsWithState - 状态分组",
            "mapGroupsWithState - 状态映射",
            "dropDuplicates - 流式去重",
            "asOfJoin - 时间点连接"
        ]),
        ("状态管理", [
            "Memory sink - 内存输出",
            "Console sink - 控制台输出",
            "File sink - 文件输出",
            "Kafka sink - Kafka输出",
            "Foreach sink - 自定义输出",
            "ForeachBatch sink - 批次自定义输出",
            "Checkpointing - 状态检查点"
        ])
    ]
    
    for category, features in streaming_features:
        doc.append(f"#### {category}")
        doc.append("")
        for feature in features:
            doc.append(f"- {feature}")
        doc.append("")
    
    # 五、MLlib机器学习库
    doc.append("=" * 80)
    doc.append("五、MLlib机器学习库")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 5.1 机器学习算法")
    doc.append("")
    
    ml_features = [
        ("分类算法(Classification)", [
            "LogisticRegression - 逻辑回归",
            "DecisionTreeClassifier - 决策树",
            "RandomForestClassifier - 随机森林",
            "GradientBoostedTrees - GBT分类",
            "NaiveBayes - 朴素贝叶斯",
            "MultilayerPerceptronClassifier - 多层感知机",
            "LinearSVC - 线性支持向量机",
            "OneVsRest - 多分类",
            "FMClassifier - 因子分解机"
        ]),
        ("回归算法(Regression)", [
            "LinearRegression - 线性回归",
            "GeneralizedLinearRegression - 广义线性回归",
            "DecisionTreeRegressor - 决策树回归",
            "RandomForestRegressor - 随机森林回归",
            "GradientBoostedTreesRegressor - GBT回归",
            "IsotonicRegression - 保序回归",
            "FMRegressor - 因子分解机回归"
        ]),
        ("聚类算法(Clustering)", [
            "KMeans - K均值聚类",
            "BisectingKMeans - 二分K均值",
            "GaussianMixture - 高斯混合模型",
            "PowerIterationClustering - PIC聚类",
            "LDA - Latent Dirichlet Allocation",
            "StreamingKMeans - 流式K均值"
        ]),
        ("协同过滤(Collaborative Filtering)", [
            "ALS - Alternating Least Squares",
            "Implicit ALS - 隐式反馈ALS"
        ]),
        ("特征工程(Feature)", [
            "TF-IDF - 词频-逆文档频率",
            "Word2Vec - 词向量",
            "CountVectorizer - 计数向量化",
            "Tokenizer - 分词器",
            "StopWordsRemover - 停用词移除",
            "NGram - N元语法",
            "OneHotEncoder - 独热编码",
            "StringIndexer - 字符串索引",
            "VectorIndexer - 向量索引",
            "StandardScaler - 标准化",
            "MinMaxScaler - 最小最大标准化",
            "Normalizer - 归一化",
            "PCA - 主成分分析",
            "FeatureHasher - 特征哈希",
            "Interaction - 特征交互",
            "PolynomialExpansion - 多项式扩展",
            "Bucketizer - 分桶",
            "VectorAssembler - 向量组装",
            "VectorSlicer - 向量切片",
            "ElementwiseProduct - 元素乘积",
            "SQLTransformer - SQL特征转换",
            "RFormula - R公式特征"
        ]),
        ("模型评估(Evaluation)", [
            "BinaryClassificationEvaluator - 二分类评估",
            "MulticlassClassificationEvaluator - 多分类评估",
            "RegressionEvaluator - 回归评估",
            "ClusteringEvaluator - 聚类评估",
            "RankingEvaluator - 排序评估",
            "AUC/ROC - AUC/ROC曲线",
            "Precision/Recall/F1 - 精确率/召回率/F1",
            "RMSE/MSE/MAE/R2 - 回归指标"
        ]),
        ("模型调优(Tuning)", [
            "CrossValidator - 交叉验证",
            "TrainValidationSplit - 训练验证分割",
            "ParamGridBuilder - 参数网格",
            "Pipeline - 管道",
            "PipelineModel - 管道模型"
        ])
    ]
    
    for category, algorithms in ml_features:
        doc.append(f"#### {category}")
        doc.append("")
        for algorithm in algorithms:
            doc.append(f"- {algorithm}")
        doc.append("")
    
    doc.append("### 5.2 ML Pipelines")
    doc.append("")
    doc.append("- Pipeline: 特征工程+模型训练流水线")
    doc.append("- PipelineModel: 可保存/加载的完整模型")
    doc.append("- Transformer: 数据转换器")
    doc.append("- Estimator: 模型估计器")
    doc.append("- Parameter: 统一参数系统")
    doc.append("")
    
    # 六、GraphX图计算
    doc.append("=" * 80)
    doc.append("六、GraphX图计算引擎")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 6.1 图抽象")
    doc.append("")
    doc.append("- **Graph[VD, ED]**: 属性图抽象")
    doc.append("- **VertexRDD[VD]**: 顶点RDD")
    doc.append("- **EdgeRDD[ED]**: 边RDD")
    doc.append("- **EdgeTriplet[VD, ED]**: 顶点+边三元组")
    doc.append("")
    
    doc.append("### 6.2 图操作")
    doc.append("")
    graph_operations = [
        ("基本操作", [
            "numVertices - 顶点数量",
            "numEdges - 边数量",
            "inDegrees - 入度",
            "outDegrees - 出度",
            "degrees - 总度数",
            "vertices - 顶点集合",
            "edges - 边集合",
            "triplets - 三元组",
            "reverse - 反向图",
            "subgraph - 子图",
            "mask - 掩码图",
            "groupEdges - 边分组"
        ]),
        ("属性操作", [
            "mapVertices - 顶点映射",
            "mapEdges - 边映射",
            "mapTriplets - 三元组映射",
            "outerJoinVertices - 外连接顶点"
        ]),
        ("结构操作", [
            "subgraph - 子图过滤",
            "mask - 保留匹配顶点/边",
            "groupEdges - 合并多重边",
            "reverse - 边反向",
            "union - 图合并"
        ]),
        ("图算法", [
            "PageRank - PageRank算法",
            "ConnectedComponents - 连通分量",
            "StronglyConnectedComponents - 强连通分量",
            "TriangleCount - 三角计数",
            "ShortestPaths - 最短路径",
            "LabelPropagation - 标签传播",
            "SVDPlusPlus - SVD++推荐算法",
            "PersonalizedPageRank - 个性化PageRank",
            "Pregel - Pregel迭代计算框架"
        ])
    ]
    
    for category, ops in graph_operations:
        doc.append(f"#### {category}")
        doc.append("")
        for op in ops:
            doc.append(f"- {op}")
        doc.append("")
    
    # 七、Spark Connect远程连接
    doc.append("=" * 80)
    doc.append("七、Spark Connect远程连接协议")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 7.1 核心能力")
    doc.append("")
    doc.append("- 客户端-服务器分离架构")
    doc.append("- Protobuf协议定义")
    doc.append("- DataFrame/Dataset远程操作")
    doc.append("- 支持Python/Java客户端")
    doc.append("- 无需安装完整Spark")
    doc.append("")
    
    doc.append("### 7.2 主要API")
    doc.append("")
    doc.append("- SparkSession.connect(): 远程连接")
    doc.append("- RemoteDataFrame: 远程DataFrame操作")
    doc.append("- Plan serialization: 执行计划序列化")
    doc.append("")
    
    # 八、资源管理
    doc.append("=" * 80)
    doc.append("八、资源管理与部署")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 8.1 支持的资源管理器")
    doc.append("")
    doc.append("- **Standalone**: Spark内置资源管理器")
    doc.append("- **YARN**: Hadoop YARN集成")
    doc.append("- **Kubernetes**: K8s容器化部署")
    doc.append("- **Mesos**: Apache Mesos（已废弃）")
    doc.append("- **Local**: 本地单机模式")
    doc.append("")
    
    doc.append("### 8.2 部署模式")
    doc.append("")
    doc.append("- Client Mode: Driver在客户端")
    doc.append("- Cluster Mode: Driver在集群")
    doc.append("- Dynamic Allocation: 动态资源分配")
    doc.append("")
    
    # 九、性能优化能力
    doc.append("=" * 80)
    doc.append("九、性能优化能力")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 9.1 内存管理")
    doc.append("")
    doc.append("- Tungsten内存管理")
    doc.append("- 堆外内存利用")
    doc.append("- 内存池管理")
    doc.append("- Execution内存 vs Storage内存")
    doc.append("")
    
    doc.append("### 9.2 执行优化")
    doc.append("")
    doc.append("- Whole-Stage Code Generation")
    doc.append("- Vectorized Execution")
    doc.append("- DAG调度优化")
    doc.append("- Predicate Pushdown")
    doc.append("- Column Pruning")
    doc.append("- Broadcast Join优化")
    doc.append("- Shuffle优化")
    doc.append("")
    
    doc.append("### 9.3 Shuffle优化")
    doc.append("")
    doc.append("- Sort Shuffle Manager")
    doc.append("- Tungsten Sort Shuffle")
    doc.append("- Unsafe Shuffle")
    doc.append("- Shuffle Hash Join")
    doc.append("- Broadcast Join（避免Shuffle）")
    doc.append("")
    
    # 十、与其他组件集成
    doc.append("=" * 80)
    doc.append("十、与大数据生态组件集成")
    doc.append("=" * 80)
    doc.append("")
    
    doc.append("### 10.1 存储层集成")
    doc.append("")
    storage_integration = [
        ("HDFS", "Hadoop分布式文件系统", "全支持"),
        ("S3", "Amazon S3", "全支持"),
        ("Azure Blob", "Azure存储", "全支持"),
        ("GCS", "Google Cloud Storage", "全支持"),
        ("HBase", "Hadoop数据库", "Connector支持"),
        ("Cassandra", "分布式数据库", "Connector支持"),
        ("Iceberg", "数据湖表格式", "原生支持"),
        ("Delta Lake", "数据湖格式", "原生支持"),
        ("Hudi", "数据湖格式", "Connector支持")
    ]
    
    doc.append("| 组件 | 类型 | 支持程度 |")
    doc.append("|------|------|----------|")
    for component, type, support in storage_integration:
        doc.append(f"| {component} | {type} | {support} |")
    doc.append("")
    
    doc.append("### 10.2 计算引擎集成")
    doc.append("")
    compute_integration = [
        ("Kafka", "流式数据", "Structured Streaming集成"),
        ("Flink", "流计算", "互操作（有限）"),
        ("Hive", "数据仓库", "原生集成"),
        ("Presto", "交互查询", "数据共享"),
        ("Delta Lake", "数据湖", "深度集成"),
        ("Iceberg", "数据湖", "原生支持")
    ]
    
    doc.append("| 组件 | 类型 | 集成方式 |")
    doc.append("|------|------|----------|")
    for component, type, integration in compute_integration:
        doc.append(f"| {component} | {type} | {integration} |")
    doc.append("")
    
    # 十一、API统计
    doc.append("=" * 80)
    doc.append("十一、API方法数量统计")
    doc.append("=" * 80)
    doc.append("")
    
    api_stats = [
        ("Spark Core RDD", "160+", "JavaRDD, JavaPairRDD, JavaDoubleRDD"),
        ("Spark SQL DataFrame", "200+", "DataFrame, Dataset, Column"),
        ("MLlib机器学习", "100+", "分类、回归、聚类、特征"),
        ("GraphX图计算", "50+", "Graph, Pregel, 算法"),
        ("Streaming流计算", "50+", "Structured Streaming API"),
        ("总计", "500+", "完整API覆盖")
    ]
    
    doc.append("| 组件 | 方法数 | 核心类 |")
    doc.append("|------|--------|--------|")
    for component, count, classes in api_stats:
        doc.append(f"| {component} | {count} | {classes} |")
    doc.append("")
    
    # 十二、核心优势总结
    doc.append("=" * 80)
    doc.append("十二、Spark核心优势总结")
    doc.append("=" * 80)
    doc.append("")
    
    advantages = [
        ("统一引擎", "一个引擎解决批处理、流处理、ML、图计算"),
        ("高性能", "内存计算、DAG执行、代码生成"),
        ("易用性", "多语言API、SQL支持、丰富的算子"),
        ("扩展性", "数据源Connector、UDF/UDAF、自定义算子"),
        ("容错性", "RDD血缘、Checkpoint、自动重试"),
        ("生态完善", "与Hadoop生态无缝集成"),
        ("社区活跃", "Apache顶级项目，持续迭代"),
        ("企业应用", "生产环境大规模应用验证")
    ]
    
    for advantage, desc in advantages:
        doc.append(f"- **{advantage}**: {desc}")
    
    doc.append("")
    
    # 十三、适用场景
    doc.append("=" * 80)
    doc.append("十三、典型应用场景")
    doc.append("=" * 80)
    doc.append("")
    
    scenarios = [
        ("批处理ETL", "数据清洗、转换、加载"),
        ("流式数据处理", "实时日志分析、监控"),
        ("交互式查询", "SQL查询、即席分析"),
        ("机器学习", "特征工程、模型训练、预测"),
        ("图计算", "社交网络分析、推荐系统"),
        ("数据湖分析", "Delta/Iceberg/Hudi数据湖"),
        ("数据仓库", "Hive集成、数仓查询"),
        ("实时报表", "流式聚合、实时仪表盘")
    ]
    
    doc.append("| 场景 | 应用 |")
    doc.append("|------|------|")
    for scenario, application in scenarios:
        doc.append(f"| {scenario} | {application} |")
    
    doc.append("")
    
    # 结束
    doc.append("=" * 80)
    doc.append("文档总结")
    doc.append("=" * 80)
    doc.append("")
    doc.append("Apache Spark提供：")
    doc.append("- ✅ 500+核心API方法")
    doc.append("- ✅ 7大核心组件（Core、SQL、Streaming、MLlib、GraphX、Connect、Resource）")
    doc.append("- ✅ 20+内置数据源")
    doc.append("- ✅ 30+机器学习算法")
    doc.append("- ✅ 10+图计算算法")
    doc.append("- ✅ 多语言支持（Scala、Java、Python、R、SQL）")
    doc.append("- ✅ 多资源管理器（YARN、K8s、Standalone）")
    doc.append("- ✅ 完整的大数据生态集成")
    doc.append("")
    doc.append("Spark是大数据领域最强大的统一分析引擎！")
    doc.append("")
    doc.append("=" * 80)
    
    return '\n'.join(doc)

if __name__ == "__main__":
    output = generate_spark_capabilities()
    
    # 保存文档
    output_file = '/home/h00517772/spark/hyx/Spark核心能力全景分析.md'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(output)
    
    print("✅ Spark核心能力文档已生成")
    print(f"✅ 文件路径: {output_file}")
    print()
    # 打印前200行
    print('\n'.join(output.split('\n')[:200]))