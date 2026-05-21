# 组件交互描述规范

**文档版本**: v3.0  
**最后更新**: 2026-05-20  
**文档定位**: 定义如何描述组件之间的交互关系

---

## 一、交互描述定义

### 1.1 什么是交互描述

**定义**: 交互描述是对多组件协作系统中，组件之间数据流、事件流、状态同步等交互关系的结构化描述。

**作用**:
- 为测试用例生成提供场景上下文
- 定义测试需要覆盖的交互维度
- 提供数据Schema和约束条件

---

### 1.2 交互描述格式

**格式**: YAML  
**文件名**: `interaction.yaml`  
**模板**: `templates/interaction_template.yaml`

---

## 二、交互描述结构规范

### 2.1 顶层结构

**必须顶层字段**（3个）:

```yaml
interaction:        # ⭐⭐⭐ 必填 - 交互关系定义
  name: ...
  components: ...
  flow: ...

data_schema:        # ⭐⭐⭐ 必填 - 数据Schema定义
  input_data: ...
  output_data: ...

constraints:        # ⭐⭐⭐ 必填 - 约束条件定义
  data_constraints: ...
  performance_constraints: ...
```

---

### 2.2 interaction字段规范

**结构**:

```yaml
interaction:
  name: string              # ⭐⭐⭐ 必填 - 交互场景名称
  components: list          # ⭐⭐⭐ 必填 - 参与组件列表
  flow: list                # ⭐⭐⭐ 必填 - 交互流程步骤
```

---

#### 2.2.1 name字段

**定义**: 交互场景的唯一标识名称

**格式**: string  
**规范**: 
- 小写字母+下划线
- 描述性命名（如`spark_kafka_hdfs_data_flow`）
- 长度：5-50字符

**示例**:
```yaml
# ✅ 正确示例
interaction:
  name: spark_kafka_hdfs_data_flow
  name: flink_kafka_hive_streaming

# ❌ 错误示例
interaction:
  name: Test                # 太短
  name: 这是一个测试场景     # 非英文
  name: spark-kafka-hdfs    # 包含中划线（建议用下划线）
```

---

#### 2.2.2 components字段

**定义**: 参与交互的组件列表

**格式**: list of strings  
**规范**:
- 至少包含2个组件
- 每个组件名称唯一
- 使用标准组件名（Spark、Kafka、HDFS、Flink、Hive等）

**示例**:
```yaml
# ✅ 正确示例
interaction:
  components:
    - Spark
    - Kafka
    - HDFS

# ❌ 错误示例
interaction:
  components:
    - Spark                 # 只有1个组件，不符合交互定义
```

---

#### 2.2.3 flow字段

**定义**: 交互流程的详细步骤序列

**格式**: list of objects  
**规范**:
- 至少包含1个步骤
- 每个步骤必须包含`step`、`component`、`action`
- 步骤按顺序编号（step从1开始）

**步骤结构**:

```yaml
flow:
  - step: number          # ⭐⭐⭐ 必填 - 步骤编号
    component: string     # ⭐⭐⭐ 必填 - 执行组件
    action: string        # ⭐⭐⭐ 必填 - 动作名称
    input: string         # ⭐⭐ 可选 - 输入来源
    output: string        # ⭐⭐ 可选 - 输出目标
    description: string   # ⭐⭐ 可选 - 步骤描述
    timeout: number       # ⭐ 可选 - 超时时间（毫秒）
    retry_count: number   # ⭐ 可选 - 重试次数
```

**示例**:
```yaml
# ✅ 正确示例
interaction:
  flow:
    - step: 1
      component: Spark
      action: read_from_hdfs
      input: hdfs://data/input
      output: DataFrame
      description: 从HDFS读取原始数据
    
    - step: 2
      component: Spark
      action: process_data
      input: DataFrame
      output: ProcessedDataFrame
      description: Spark处理数据
    
    - step: 3
      component: Kafka
      action: produce_message
      input: ProcessedDataFrame
      output: kafka://topic/output
      description: 发送处理结果到Kafka

# ❌ 错误示例
interaction:
  flow:
    - step: 0              # 步骤编号应从1开始
      component: Spark
      action: test
```

---

### 2.3 data_schema字段规范

**结构**:

```yaml
data_schema:
  input_data: object         # ⭐⭐⭐ 必填 - 输入数据Schema
  intermediate_data: object  # ⭐⭐ 可选 - 中间数据Schema
  output_data: object        # ⭐⭐⭐ 必填 - 输出数据Schema
```

---

#### 2.3.1 input_data字段

**定义**: 输入数据的Schema定义

**结构**:

```yaml
input_data:
  type: string              # ⭐⭐⭐ 必填 - 数据类型（JSON/CSV/Avro等）
  schema: object            # ⭐⭐⭐ 必填 - Schema定义
```

**示例**:
```yaml
data_schema:
  input_data:
    type: JSON
    schema:
      fields:
        - name: id
          type: integer
        - name: name
          type: string
        - name: value
          type: float
```

---

#### 2.3.2 output_data字段

**定义**: 输出数据的Schema定义

**结构**: 同input_data

**示例**:
```yaml
data_schema:
  output_data:
    type: KafkaMessage
    schema:
      key_field: id
      value_fields:
        - processed_value
```

---

### 2.4 constraints字段规范

**结构**:

```yaml
constraints:
  data_constraints: list      # ⭐⭐⭐ 必填 - 数据约束
  performance_constraints: list  # ⭐⭐⭐ 必填 - 性能约束
  reliability_constraints: list  # ⭐⭐ 可选 - 可靠性约束
```

---

#### 2.4.1 data_constraints字段

**定义**: 数据相关的约束条件

**结构**:

```yaml
data_constraints:
  - name: string            # ⭐⭐⭐ 必填 - 约束名称
    type: string            # ⭐⭐⭐ 必填 - 约束类型（size/format/range等）
    min: number             # ⭐⭐ 可选 - 最小值
    max: number             # ⭐⭐ 可选 - 最大值
    value: any              # ⭐⭐ 可选 - 约束值
```

**示例**:
```yaml
constraints:
  data_constraints:
    - name: input_size
      type: size
      min: 1
      max: 10000
    
    - name: input_format
      type: format
      value: JSON
```

---

#### 2.4.2 performance_constraints字段

**定义**: 性能相关的约束条件

**结构**:

```yaml
performance_constraints:
  - name: string            # ⭐⭐⭐ 必填 - 约束名称
    type: string            # ⭐⭐⭐ 必填 - 约束类型（latency/throughput等）
    max_ms: number          # ⭐⭐ 可选 - 最大延迟（毫秒）
    min_qps: number         # ⭐⭐ 可选 - 最小吞吐量（QPS）
```

**示例**:
```yaml
constraints:
  performance_constraints:
    - name: processing_time
      type: latency
      max_ms: 5000
    
    - name: throughput
      type: throughput
      min_qps: 1000
```

---

## 三、交互类型分类

### 3.1 交互类型定义

**5种交互类型**:

| 类型 | 描述 | 特征关键词 | 示例场景 |
|------|------|-----------|---------|
| **data_flow** | 数据流传输 | send/receive/consume/produce/write/read | Spark-Kafka数据传输 |
| **state_sync** | 状态同步 | sync/update/refresh/consistency | 配置中心-应用配置同步 |
| **event_trigger** | 事件触发 | trigger/event/notify/callback | Kafka消息触发Flink作业 |
| **query_access** | 查询访问 | query/select/fetch/get/lookup | Hive查询HDFS数据 |
| **config_linkage** | 配置联动 | config/setting/parameter/option | Kafka配置同步到HDFS |

---

### 3.2 如何确定交互类型

**判断规则**:

根据`flow`中的`action`关键词判断：

```yaml
# data_flow类型示例
flow:
  - action: read_from_hdfs    # 包含read关键词
  - action: write_to_kafka    # 包含write关键词

# state_sync类型示例
flow:
  - action: sync_state        # 包含sync关键词
  - action: update_config     # 包含update关键词

# event_trigger类型示例
flow:
  - action: trigger_job       # 包含trigger关键词
  - action: notify_event      # 包含notify关键词

# query_access类型示例
flow:
  - action: query_data        # 包含query关键词
  - action: fetch_result      # 包含fetch关键词

# config_linkage类型示例
flow:
  - action: config_update     # 包含config关键词
  - action: setting_apply     # 包含setting关键词
```

---

## 四、交互描述示例

### 4.1 完整示例：Spark-Kafka-HDFS

```yaml
# interaction.yaml - Spark-Kafka-HDFS数据流场景

interaction:
  name: spark_kafka_hdfs_data_flow
  components:
    - Spark
    - Kafka
    - HDFS
  flow:
    - step: 1
      component: HDFS
      action: provide_data
      input: /data/input
      output: RawDataFile
      description: HDFS提供原始数据文件
    
    - step: 2
      component: Spark
      action: read_from_hdfs
      input: RawDataFile
      output: DataFrame
      description: Spark读取HDFS数据为DataFrame
    
    - step: 3
      component: Spark
      action: process_data
      input: DataFrame
      output: ProcessedDataFrame
      description: Spark处理数据（清洗/转换/聚合）
    
    - step: 4
      component: Kafka
      action: produce_message
      input: ProcessedDataFrame
      output: kafka://topic/output
      description: Spark将处理结果发送到Kafka
      timeout: 5000
      retry_count: 3

data_schema:
  input_data:
    type: JSON
    schema:
      fields:
        - name: id
          type: integer
          nullable: false
        - name: timestamp
          type: timestamp
        - name: value
          type: float
  
  intermediate_data:
    type: DataFrame
    schema:
      columns:
        - name: id
          type: integer
        - name: processed_value
          type: float
        - name: category
          type: string
  
  output_data:
    type: KafkaMessage
    schema:
      key_field: id
      value_fields:
        - processed_value
        - category
      partition_strategy: hash

constraints:
  data_constraints:
    - name: input_size
      type: size
      min: 1
      max: 10000
    
    - name: input_format
      type: format
      value: JSON
    
    - name: timestamp_range
      type: range
      min: 2026-01-01
      max: 2026-12-31
  
  performance_constraints:
    - name: processing_time
      type: latency
      max_ms: 10000
    
    - name: throughput
      type: throughput
      min_qps: 500
  
  reliability_constraints:
    - name: retry_count
      type: retry
      max: 3
    
    - name: message_delivery
      type: guarantee
      value: at_least_once
```

---

### 4.2 完整示例：Flink-Kafka-Hive

```yaml
# interaction.yaml - Flink-Kafka-Hive流式处理场景

interaction:
  name: flink_kafka_hive_streaming
  components:
    - Kafka
    - Flink
    - Hive
  flow:
    - step: 1
      component: Kafka
      action: produce_stream
      input: external_system
      output: kafka://topic/input_stream
      description: 外部系统实时发送数据到Kafka
    
    - step: 2
      component: Flink
      action: consume_stream
      input: kafka://topic/input_stream
      output: FlinkStream
      description: Flink消费Kafka流数据
    
    - step: 3
      component: Flink
      action: window_aggregation
      input: FlinkStream
      output: AggregatedResult
      description: Flink窗口聚合计算
    
    - step: 4
      component: Hive
      action: store_result
      input: AggregatedResult
      output: hive://table/result_table
      description: 结果存储到Hive表

data_schema:
  input_data:
    type: KafkaStream
    schema:
      topic: input_stream
      partitions: 10
      key_field: device_id
      value_fields:
        - timestamp
        - metric_value
  
  intermediate_data:
    type: FlinkDataStream
    schema:
      watermark_field: timestamp
      window_size: 5min
  
  output_data:
    type: HiveTable
    schema:
      table_name: result_table
      columns:
        - device_id
        - window_start
        - window_end
        - avg_value

constraints:
  data_constraints:
    - name: stream_rate
      type: rate
      min: 100
      max: 10000  # events/sec
  
  performance_constraints:
    - name: processing_latency
      type: latency
      max_ms: 1000
  
  reliability_constraints:
    - name: checkpoint_interval
      type: interval
      value: 5min
```

---

## 五、交互描述验证规范

### 5.1 必填字段验证

**验证规则**:

```yaml
# 必须包含的顶层字段
必填顶层字段:
  - interaction
  - data_schema
  - constraints

# interaction必须包含的字段
interaction必填字段:
  - name
  - components
  - flow

# data_schema必须包含的字段
data_schema必填字段:
  - input_data
  - output_data

# constraints必须包含的字段
constraints必填字段:
  - data_constraints
  - performance_constraints
```

**验证方法**: 检查YAML文件是否包含上述必填字段

---

### 5.2 格式验证

**验证规则**:

| 字段 | 类型 | 验证规则 |
|------|------|---------|
| name | string | 长度5-50字符，小写字母+下划线 |
| components | list | 长度≥2，每个元素为string |
| flow | list | 长度≥1，每个元素包含step/component/action |
| step | number | 从1开始，递增 |
| data_constraints | list | 长度≥1，每个元素包含name/type |

---

### 5.3 逻辑验证

**验证规则**:

**规则1: 组件一致性**
- flow中引用的component必须存在于components列表中

**规则2: 步骤顺序性**
- flow中的step必须从1开始递增

**规则3: 数据流连贯性**
- 每个步骤的input应与前一步骤的output关联（推荐）

---

## 六、交互描述最佳实践

### 6.1 命名最佳实践

**推荐命名**:
```yaml
# ✅ 好的命名（描述性强）
interaction:
  name: spark_kafka_realtime_streaming
  name: flink_hive_batch_analysis

# ❌ 差的命名（描述性弱）
interaction:
  name: test_interaction
  name: scenario_1
```

---

### 6.2 流程描述最佳实践

**推荐方式**:
```yaml
# ✅ 好的流程描述（步骤清晰，包含描述）
flow:
  - step: 1
    component: Spark
    action: read_from_hdfs
    description: Spark读取HDFS原始数据
    input: hdfs://data/input
    output: DataFrame
    
  - step: 2
    component: Spark
    action: transform_data
    description: Spark执行数据转换逻辑
    input: DataFrame
    output: TransformedDataFrame
```

**避免方式**:
```yaml
# ❌ 差的流程描述（缺少描述，缺少input/output）
flow:
  - step: 1
    component: Spark
    action: test
  
  - step: 2
    component: Kafka
    action: test2
```

---

### 6.3 约束定义最佳实践

**推荐方式**:
```yaml
# ✅ 好的约束定义（具体数值）
constraints:
  data_constraints:
    - name: input_size
      type: size
      min: 100
      max: 100000
  
  performance_constraints:
    - name: processing_time
      type: latency
      max_ms: 5000
```

**避免方式**:
```yaml
# ❌ 差的约束定义（模糊描述）
constraints:
  data_constraints:
    - name: 大数据量
      type: 大
  
  performance_constraints:
    - name: 快速处理
      type: 快
```

---

## 七、交互描述常见问题

### Q1: 如何描述复杂的多分支交互？

**答案**: 使用多个flow分支，每个分支描述一个交互路径

```yaml
interaction:
  flow:
    # 分支1: 正常流程
    - step: 1
      component: Spark
      action: read_from_hdfs
      branch: normal
    
    # 分支2: 异常流程
    - step: 1
      component: Spark
      action: handle_failure
      branch: error
```

---

### Q2: 如何描述异步交互？

**答案**: 使用timeout和retry_count字段

```yaml
flow:
  - step: 3
    component: Kafka
    action: async_produce
    timeout: 10000    # 10秒超时
    retry_count: 3    # 重试3次
```

---

### Q3: 如何描述循环交互？

**答案**: 使用循环标识字段

```yaml
flow:
  - step: 3
    component: Flink
    action: continuous_process
    loop: true
    loop_interval: 5min
```

---

## 八、交互描述模板索引

**模板路径**: `templates/interaction_template.yaml`

**使用方式**:
1. 复制模板文件
2. 根据实际场景填写内容
3. 验证格式正确性

---

**文档结束** - 组件交互描述规范 v3.0