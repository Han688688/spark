# 测试用例规范文档

**文档版本**: v3.0  
**最后更新**: 2026-05-21  
**文档定位**: 定义测试用例的标准格式和必填字段  
**权威性**: 本文档是case_type、assertion_type、priority枚举值的唯一定义源

---

## 零、权威枚举值定义

以下枚举值为全Skill体系的唯一定义源。模板、种子、配置中的枚举值必须与此一致。

### case_type枚举（5种）

| 值 | 说明 | 优先级 | 必选 |
|-----|------|--------|------|
| normal_flow | 正常流程 | P0 | 必须 |
| error_handling | 异常处理 | P0 | 必须 |
| boundary_values | 边界值 | P1 | 必须 |
| performance | 性能测试 | P1 | 可选 |
| stability | 稳定性测试 | P2 | 可选 |

### assertion_type枚举（6种）

| 值 | 说明 | 适用场景 |
|-----|------|---------|
| value | 值验证 | 验证字段值等于预期 |
| count | 数量验证 | 验证数量等于预期 |
| exception | 异常验证 | 验证异常被抛出 |
| function | 功能验证 | 验证功能正常执行 |
| state | 状态验证 | 验证状态变化正确 |
| file | 文件验证 | 验证文件存在/内容正确 |

### priority枚举（3种）

| 值 | 定义 | 占比目标 |
|-----|------|---------|
| P0 | 最高优先级 | >= 50% |
| P1 | 高优先级 | <= 30% |
| P2 | 中优先级 | <= 20% |

### error_type枚举（5种）

error_type仅用于error_handling类型用例的scenario.error_type字段。

| 值 | 说明 | 适用交互类型 |
|-----|------|-------------|
| component_failure | 组件故障/不可用 | 全类型 |
| network_failure | 网络/连接异常 | data_flow, event_trigger, query_access |
| data_corruption | 数据格式错误/损坏/缺失 | data_flow, state_sync |
| timeout_failure | 超时/延迟异常 | 全类型 |
| configuration_error | 配置错误/不兼容 | config_linkage, state_sync |

扩展规则: 如果交互描述中包含特定组件名或特定故障模式，error_type可从上述5种大类派生：
- `{component}_failure` — 组件故障（如kafka_connection_failure、hdfs_write_failure）
- `{component}_{failure_detail}` — 组件特定故障（如zookeeper_session_expired、consumer_processing_failure）
- 派生error_type必须以5种大类为根（如kafka_connection_failure根属于component_failure）

---

## 一、测试用例定义

### 1.1 什么是测试用例

**定义**: 测试用例是对特定测试场景的完整描述，包括场景、前置条件、测试步骤、验证点、清理步骤等。

**作用**:
- 明确测试目的和范围
- 指导测试执行过程
- 定义验证标准和预期结果

---

### 1.2 测试用例格式

**格式**: YAML  
**文件名**: `test_cases.yaml`  
**模板**: `templates/test_case_template.yaml`

---

## 二、测试用例结构规范

### 2.1 测试用例字段定义

**必须字段**（6个）:

| 字段 | 类型 | 优先级 | 说明 |
|------|------|--------|------|
| `case_id` | string | ⭐⭐⭐⭐⭐ | 用例唯一标识（自动生成） |
| `case_name` | string | ⭐⭐⭐⭐⭐ | 用例名称（描述性命名） |
| `case_type` | string | ⭐⭐⭐⭐⭐ | 用例类型（见枚举定义） |
| `priority` | string | ⭐⭐⭐⭐⭐ | 优先级（见枚举定义） |
| `test_steps` | list | ⭐⭐⭐⭐⭐ | 测试步骤列表（≥1个步骤） |
| `assertions` | list | ⭐⭐⭐⭐⭐ | 验证点列表（≥1个验证点） |

**推荐字段**（5个）:

| 字段 | 类型 | 优先级 | 说明 |
|------|------|--------|------|
| `scenario` | object | ⭐⭐⭐⭐ | 场景信息（组件、交互类型） |
| `preconditions` | list | ⭐⭐⭐⭐ | 前置条件列表 |
| `test_data` | object | ⭐⭐⭐⭐ | 测试数据（输入/输出） |
| `cleanup` | list | ⭐⭐⭐⭐ | 清理步骤列表 |
| `generated_time` | string | ⭐⭐⭐ | 生成时间（自动填充） |

---

### 2.2 测试用例完整结构

```yaml
case_id: string              # ⭐⭐⭐⭐⭐ 必填 - 用例唯一ID
case_name: string            # ⭐⭐⭐⭐⭐ 必填 - 用例名称
case_type: string            # ⭐⭐⭐⭐⭐ 必填 - 用例类型（normal_flow/error_handling/boundary_values）
priority: string             # ⭐⭐⭐⭐⭐ 必填 - 优先级（P0/P1/P2）
generated_time: string       # ⭐⭐⭐ 推荐填 - 生成时间（ISO格式）
seed_scenario: string        # ⭐⭐⭐ 推荐填 - 种子场景名称

scenario:                    # ⭐⭐⭐⭐ 推荐填 - 场景信息
  name: string               # 场景名称
  components: list           # 涉及组件
  interaction_type: string   # 交互类型
  description: string        # 场景描述

preconditions:               # ⭐⭐⭐⭐ 推荐填 - 前置条件列表
  - string                   # 前置条件描述
  - string

test_steps:                  # ⭐⭐⭐⭐⭐ 必填 - 测试步骤列表
  - step_number: number      # 步骤编号
    action: string           # 执行动作
    component: string        # 执行组件
    input: string            # 输入数据（可选）
    expected_result: string  # 预期结果
    timeout: number          # 超时时间（可选）

test_data:                   # ⭐⭐⭐⭐ 推荐填 - 测试数据
  input:                     # 输入数据
    data_size: number        # 数据量
    data_format: string      # 数据格式
    specific_data: object    # 特定数据（可选）
  expected_output:           # 预期输出
    status: string           # 状态
    count: number            # 数量（可选）

assertions:                  # ⭐⭐⭐⭐⭐ 必填 - 验证点列表
  - assertion_type: string   # 验证类型（见零节枚举定义）
    description: string      # 验证描述
    expected_value: string | number | boolean  # 预期值（见七节类型规范）
    actual_value: string | number | boolean    # 实际值（可选，执行时填充）

cleanup:                     # ⭐⭐⭐⭐ 推荐填 - 清理步骤
  - string                   # 清理动作描述
```

---

## 三、测试用例类型规范

### 3.1 正常流程用例（normal_flow）

**定义**: 测试组件交互在正常条件下的完整流程

**特点**:
- 数据量适中（100-1000条）
- 无异常情况
- 全流程成功执行
- 优先级通常为P0

**示例**:
```yaml
case_id: auto-202605201234-001
case_name: spark_kafka_normal_flow_basic
case_type: normal_flow
priority: P0

scenario:
  name: spark_kafka_hdfs_data_flow
  components: [Spark, Kafka, HDFS]
  interaction_type: data_flow
  description: Spark从HDFS读取数据，处理后发送到Kafka

preconditions:
  - HDFS正常运行，包含测试数据
  - Kafka正常运行，topic已创建
  - Spark集群正常运行

test_steps:
  - step_number: 1
    action: read_from_hdfs
    component: Spark
    input: hdfs://test/input/data.json
    expected_result: 成功读取DataFrame
  
  - step_number: 2
    action: process_data
    component: Spark
    input: DataFrame
    expected_result: 数据处理成功
  
  - step_number: 3
    action: produce_to_kafka
    component: Kafka
    input: ProcessedDataFrame
    expected_result: 消息成功发送到topic

test_data:
  input:
    data_size: 100
    data_format: JSON
  expected_output:
    status: success
    message_count: 100

assertions:
  - assertion_type: count
    description: 验证Kafka消息数量
    expected_value: 100
  
  - assertion_type: value
    description: 验证数据完整性
    expected_value: no_data_loss

cleanup:
  - 清理HDFS测试数据
  - 清理Kafka测试topic
```

---

### 3.2 异常处理用例（error_handling）

**定义**: 测试组件交互在异常条件下的处理能力

**特点**:
- 包含异常场景（网络故障、组件故障、数据异常）
- 测试异常捕获和恢复机制
- 优先级通常为P0

**示例**:
```yaml
case_id: auto-202605201234-002
case_name: spark_kafka_kafka_failure_handling
case_type: error_handling
priority: P0

scenario:
  name: spark_kafka_hdfs_data_flow
  error_type: kafka_connection_failure
  description: Kafka连接失败时的异常处理

preconditions:
  - HDFS正常运行
  - Kafka模拟故障状态
  - Spark配置重试机制

test_steps:
  - step_number: 1
    action: simulate_kafka_failure
    component: Kafka
    expected_result: Kafka停止运行
  
  - step_number: 2
    action: trigger_spark_processing
    component: Spark
    input: DataFrame
    expected_result: Spark尝试发送消息
  
  - step_number: 3
    action: verify_retry_mechanism
    component: Spark
    expected_result: 触发重试机制，记录异常

test_data:
  input:
    data_size: 100
  expected_output:
    status: error
    error_message: Kafka连接失败

assertions:
  - assertion_type: exception
    description: 验证异常被捕获
    expected_value: KafkaConnectionException
  
  - assertion_type: function
    description: 验证重试机制触发
    expected_value: 3
  
  - assertion_type: value
    description: 验证错误日志记录
    expected_value: kafka_failure_info_in_log

cleanup:
  - 恢复Kafka正常运行
  - 清理测试数据
```

---

### 3.3 边界值用例（boundary_values）

**定义**: 测试组件交互在边界条件下的处理能力

**特点**:
- 数据量最小（1条）或最大（10000+条）
- 测试边界处理逻辑
- 优先级通常为P1

**最小值示例**:
```yaml
case_id: auto-202605201234-003
case_name: spark_kafka_min_data_size
case_type: boundary_values
priority: P1

scenario:
  name: spark_kafka_hdfs_data_flow
  description: 最小数据量（1条）测试

test_data:
  input:
    data_size: 1  # 最小边界值
    data_format: JSON
  expected_output:
    status: success
    message_count: 1

test_steps:
  - step_number: 1
    action: read_single_record
    component: Spark
    input: hdfs://test/input/single.json
    expected_result: 成功读取单条数据
  
  - step_number: 2
    action: process_single_record
    component: Spark
    expected_result: 成功处理单条数据
  
  - step_number: 3
    action: send_to_kafka
    component: Kafka
    expected_result: 成功发送1条消息

assertions:
  - assertion_type: count
    description: 验证最小数据量处理
    expected_value: 1
```

---

## 四、测试步骤规范

### 4.1 测试步骤字段

**必须字段**（4个）:

| 字段 | 类型 | 说明 |
|------|------|------|
| `step_number` | number | 步骤编号（从1开始递增） |
| `action` | string | 执行动作名称 |
| `component` | string | 执行组件名称 |
| `expected_result` | string | 预期结果描述 |

**可选字段**（3个）:

| 字段 | 类型 | 说明 |
|------|------|------|
| `input` | string | 输入数据描述 |
| `timeout` | number | 超时时间（毫秒） |
| `retry_count` | number | 重试次数 |

---

### 4.2 测试步骤示例

```yaml
test_steps:
  - step_number: 1
    action: initialize_spark_session
    component: Spark
    expected_result: Spark Session成功创建
  
  - step_number: 2
    action: read_from_hdfs
    component: Spark
    input: hdfs://test/data/input.json
    expected_result: DataFrame成功创建，包含100行数据
    timeout: 5000
  
  - step_number: 3
    action: transform_data
    component: Spark
    input: DataFrame
    expected_result: 数据转换成功
  
  - step_number: 4
    action: send_to_kafka
    component: Kafka
    input: ProcessedDataFrame
    expected_result: Kafka消息成功发送
    timeout: 10000
    retry_count: 3
```

---

## 五、验证点规范

### 5.1 验证点类型

**6种验证类型**（权威定义见零节）:

| 类型 | 说明 | 适用场景 |
|------|------|---------|
| `value` | 值验证 | 验证特定字段值是否等于预期值 |
| `count` | 数量验证 | 验证数量是否等于预期值 |
| `exception` | 异常验证 | 验证异常是否抛出 |
| `function` | 功能验证 | 验证功能是否正常执行 |
| `state` | 状态验证 | 验证状态变化是否正确 |
| `file` | 文件验证 | 验证文件存在或内容正确 |

---

### 5.2 验证点示例

**值验证示例**:
```yaml
assertions:
  - assertion_type: value
    description: 验证数据处理结果字段值
    expected_value: processed_value=100.5
    actual_value: 待执行时填充
```

**数量验证示例**:
```yaml
assertions:
  - assertion_type: count
    description: 验证Kafka消息数量
    expected_value: 100
```

**异常验证示例**:
```yaml
assertions:
  - assertion_type: exception
    description: 验证Kafka连接异常被捕获
    expected_value: KafkaConnectionException
```

**功能验证示例**:
```yaml
assertions:
  - assertion_type: function
    description: 验证重试机制是否触发
    expected_value: retry_success_after_3_attempts
```

---

## 六、优先级规范

### 6.1 优先级定义

**3个优先级**:

| 优先级 | 定义 | 用例类型 | 占比目标 |
|--------|------|---------|---------|
| `P0` | 最高优先级，必须测试 | 正常流程、异常处理 | ≥50% |
| `P1` | 高优先级，推荐测试 | 边界值、性能测试 | ≤30% |
| `P2` | 中优先级，可选测试 | 稳定性测试 | ≤20% |

---

### 6.2 优先级分配规则

**规则1: 正常流程用例优先级**
- 第1个正常用例：P0
- 变体正常用例：P1

**规则2: 异常处理用例优先级**
- 核心异常用例：P0
- 边缘异常用例：P1

**规则3: 边界值用例优先级**
- 最小边界用例：P1
- 最大边界用例：P1

---

## 七、expected_value类型规范

expected_value字段允许3种类型，每种类型有明确使用场景：

| 类型 | 格式 | 适用场景 | 示例 |
|------|------|---------|------|
| string | 描述性文本或标识符 | value/file/function/state断言 | "success"、"hdfs_path_exists"、"retry_count >= 3" |
| number | 精确数值 | count断言、value断言（数值比较） | 100、1、10000 |
| boolean | true/false | state/function断言（二元判断） | true、false |

**类型选择规则**:
- count断言 → 必须用number
- exception断言 → 必须用string（异常类名或描述）
- value断言 → number（数值验证）或string（文本验证）
- function/state/file断言 → string（描述性预期）或boolean（二元判断）

**禁止**: 不得使用混合类型（如"输入1条，输出1条"这种中文描述），应拆分为具体值。

---

## 八、种子用例与输出用例格式关系

**种子用例**是高质量参考案例，可包含扩展字段（如metadata中的seed_id/quality_level、scenario中的flow列表、generalization_patterns等），用于帮助AI理解泛化方向。

**输出用例**必须严格遵循本规范二节的字段定义和test_case_template.yaml的结构，不得包含种子特有的扩展字段（code/check_code/generalization_patterns/flow等）。

**转换规则**: AI从种子提取验证模式、步骤模式、数据模式后，以精简模板格式输出最终用例。

---

## 九、测试用例命名规范

### 9.1 命名格式

**格式**: `{scenario}_{case_type}_{variant}`

**示例**:
```yaml
# ✅ 正确命名
case_name: spark_kafka_hdfs_normal_flow_basic
case_name: spark_kafka_hdfs_error_handling_kafka_failure
case_name: spark_kafka_hdfs_boundary_values_min_data

# ❌ 错误命名
case_name: Test1                    # 无场景信息
case_name: 测试用例                  # 非英文
case_name: spark_kafka_test_001     # 无类型信息
```

---

## 十、测试用例完整示例

### 10.1 正常流程用例完整示例

```yaml
case_id: auto-202605201234-001
case_name: spark_kafka_hdfs_normal_flow_basic
case_type: normal_flow
priority: P0
generated_time: 2026-05-20T12:34:56Z
seed_scenario: spark_kafka_hdfs_data_flow

scenario:
  name: spark_kafka_hdfs_data_flow
  components:
    - Spark
    - Kafka
    - HDFS
  interaction_type: data_flow
  description: Spark从HDFS读取JSON数据，处理后发送到Kafka，验证完整数据流

preconditions:
  - HDFS集群正常运行
  - HDFS包含测试数据文件（100条JSON记录）
  - Kafka集群正常运行
  - Kafka topic已创建（spark_output_topic）
  - Spark集群正常运行
  - Spark配置正确（HDFS和Kafka连接参数）

test_steps:
  - step_number: 1
    action: create_spark_session
    component: Spark
    expected_result: SparkSession成功创建
  
  - step_number: 2
    action: read_from_hdfs
    component: Spark
    input: hdfs://test/data/input/input_100.json
    expected_result: DataFrame成功创建，包含100行数据，schema包含id/name/value字段
  
  - step_number: 3
    action: filter_data
    component: Spark
    input: DataFrame
    expected_result: 数据过滤成功，保留value>50的记录
  
  - step_number: 4
    action: aggregate_data
    component: Spark
    input: FilteredDataFrame
    expected_result: 数据聚合成功，按category分组统计
  
  - step_number: 5
    action: send_to_kafka
    component: Kafka
    input: AggregatedDataFrame
    expected_result: Kafka消息成功发送到spark_output_topic，消息数量等于聚合结果数量
    timeout: 10000

test_data:
  input:
    data_size: 100
    data_format: JSON
    schema:
      fields:
        - id: integer
        - name: string
        - value: float
        - category: string
  
  expected_output:
    status: success
    message_count: 50
    message_format: JSON

assertions:
  - assertion_type: count
    description: 验证DataFrame初始行数
    expected_value: 100
  
  - assertion_type: count
    description: 验证过滤后DataFrame行数
    expected_value: 50
  
  - assertion_type: value
    description: 验证聚合结果字段
    expected_value: contains_category_and_avg_value
  
  - assertion_type: count
    description: 验证Kafka发送消息数量
    expected_value: 50
  
  - assertion_type: value
    description: 验证Kafka消息格式
    expected_value: json_with_key_and_value

cleanup:
  - 删除HDFS测试数据文件（hdfs://test/data/input/input_100.json）
  - 清理Kafka测试topic消息
  - 关闭SparkSession
  - 清理临时文件和日志
```

---

## 十一、测试用例验证规范

### 11.1 必填字段验证

**验证规则**:
- `case_id` 必填且唯一
- `case_name` 必填且长度≥10字符
- `case_type` 必填且为零节定义的5种之一（normal_flow/error_handling/boundary_values/performance/stability）
- `priority` 必填且为P0/P1/P2之一
- `test_steps` 必填且长度≥1
- `assertions` 必填且长度≥1

---

### 11.2 格式验证

**验证规则**:
- `case_id` 格式为`auto-{timestamp}-{sequence}`
- `priority` 格式为P0/P1/P2
- `step_number` 从1开始递增
- `assertion_type` 为零节定义的6种之一（value/count/exception/function/state/file）
- `case_name` 必须符合九节命名格式 `{scenario}_{case_type}_{variant}`
- `expected_value` 必须符合七节类型规范（string/number/boolean），禁止中文混合描述
- `error_type`（仅error_handling类型）必须为零节定义5种之一或其合法派生

---

## 十二、测试用例模板索引

**模板路径**: `templates/test_case_template.yaml`

**使用方式**:
1. 复制模板文件
2. 根据实际场景填写内容
3. 参照本规范验证格式

---

**文档结束** - 测试用例规范文档 v3.0