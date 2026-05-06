# Spark Java API完整清单项目

## 项目说明

本目录包含Spark及相关组件的完整Java API清单，用于测试对比参考。

## 目录结构

```
hyx/
├── API清单文档/
│   ├── spark_java_api_complete_list.md       # Spark完整API（224个）
│   ├── kafka_java_api_complete_list.md       # Kafka完整API（156个）
│   ├── hbase_java_api_complete_list.md       # HBase完整API
│   ├── hadoop_java_api_complete_list.md      # Hadoop完整API
│   ├── iceberg_java_api_complete_list.md     # Iceberg完整API
│   └── api_comparison_summary.md             # 各组件API对比汇总
│
├── 项目文档/
│   ├── 总览.md                 # 可扩展测试框架总览
│   ├── 方案设计.md             # 数据驱动测试方案设计
│   ├── 问题背景.md             # 项目背景问题
│   ├── 使用指南.md             # 框架使用指南
│   └── 极简方案.md             # 原方案（已废弃）
│
└── 框架代码/
    ├── framework.py            # 可扩展测试自动化框架
    ├── framework.yaml          # 框架配置
    ├── start.py                # 快速启动脚本
    ├── agent.py                # 原Agent（已废弃）
    └── plugins/                # 自定义插件目录
```

## API清单说明

### Spark Java API清单

**文件**：`spark_java_api_complete_list.md`

**内容**：
- 17个核心类
- 224个Java API
- 包含：类名、方法名、方法签名、返回类型、参数类型、稳定性标注

**示例**：
```
| 类名 | 方法名 | 方法签名 | 返回类型 | 参数 |
|------|--------|----------|----------|------|
| JavaRDDLike | map | JavaRDD<R> map(Function<T,R> f) | JavaRDD<R> | Function<T,R> |
| JavaRDDLike | filter | JavaRDD<T> filter(Function<T,Boolean> f) | JavaRDD<T> | Function<T,Boolean> |
| JavaSparkContext | parallelize | JavaRDD<T> parallelize(List<T> list) | JavaRDD<T> | List<T> |
```

### Kafka Java API清单

**文件**：`kafka_java_api_complete_list.md`

**内容**：
- 156个Java API
- 包含核心类和方法定义

### 其他组件API清单

- HBase Java API
- Hadoop Java API
- Iceberg Java API

## 使用方法

### 查看API清单

```bash
# Spark API
cat spark_java_api_complete_list.md

# Kafka API
cat kafka_java_api_complete_list.md

# 所有组件对比
cat api_comparison_summary.md
```

### API清单用途

1. **测试对比参考** - 查看完整的API定义，对比测试覆盖情况
2. **测试用例设计** - 根据API签名设计测试用例
3. **API文档参考** - 了解各组件提供的Java接口

## API清单特点

### 完整性

- 提取自源码和官方文档
- 包含所有公开Java API
- 方法级别的详细定义

### 详细信息

每个API包含：
- 类名（完整Java类名）
- 方法名
- 方法签名（完整签名）
- 返回类型
- 参数类型列表
- 稳定性标注（Stable/Evolving/Deprecated等）
- 包路径
- 源码路径

### 示例API定义

**JavaRDDLike.map方法**：
```
类名：JavaRDDLike
方法名：map
方法签名：JavaRDD<R> map(Function<T,R> f)
返回类型：JavaRDD<R>
参数类型：Function<T,R>
稳定性：Stable
包路径：org.apache.spark.api.java.JavaRDDLike
```

## 组件对比

| 组件 | API数量 | 核心类数 | 稳定性标注 |
|------|--------|---------|-----------|
| Spark | 224 | 17 | Stable（144），Deprecated（76） |
| Kafka | 156 | 多个 | Stable为主 |
| HBase | - | 多个 | 包含稳定性标注 |
| Hadoop | - | 多个 | 包含稳定性标注 |
| Iceberg | - | 多个 | 包含稳定性标注 |

## 后续使用

### 对比测试覆盖

您可以：
1. 查看完整API列表
2. 与您的测试代码对比
3. 找出缺失的API
4. 补充测试用例

### API清单更新

当组件版本更新时，重新提取API清单：
1. 从源码提取新增API
2. 更新API清单文档
3. 对比API变化

## 文档来源

所有API清单提取自：
- Spark源码：`/home/h00517772/spark`
- 官方JavaDoc文档
- 组件官方文档

---

**本目录提供完整的Java API清单文档，用于测试对比参考**