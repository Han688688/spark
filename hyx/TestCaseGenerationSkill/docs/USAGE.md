# TestCaseGenerationSkill使用指南

**版本**: v3.0（文档驱动版）

---

## 一、定位与适用范围

**适用**:
- 多组件协作系统（Spark-Kafka-HDFS等）
- 数据流/事件触发/状态同步/查询访问/配置联动场景

**不适用**:
- 单组件系统、纯UI测试、纯手工测试

---

## 二、准备输入

### 2.1 交互描述（必须）

参照 `templates/interaction_template.yaml` 和 `docs/interaction_spec.md`：

```yaml
interaction:
  name: spark_kafka_hdfs_data_flow       # 场景名称
  components: [Spark, Kafka, HDFS]        # 参与组件
  flow:
    - step: 1
      component: Spark
      action: read_from_hdfs
      input: hdfs://data/input
      output: DataFrame
    - step: 2
      component: Spark
      action: process_data
      output: ProcessedDataFrame
    - step: 3
      component: Kafka
      action: produce_message
      input: ProcessedDataFrame
      output: kafka://topic/output

data_schema:
  input_data: {type: JSON, schema: {fields: [id, name, value]}}
  output_data: {type: KafkaMessage}

constraints:
  data_constraints: [{name: input_size, min: 1, max: 10000}]
  performance_constraints: [{name: processing_time, max_ms: 5000}]
```

### 2.2 种子用例（必须）

参照 `templates/test_case_template.yaml` 和 `docs/testcase_spec.md`：

**最低要求**: 1个正常用例(P0) + 1个异常用例(P0推荐)

```yaml
test_cases:
  - case_name: spark_kafka_normal_flow_basic
    case_type: normal_flow
    priority: P0
    test_steps:
      - {step_number: 1, action: read_from_hdfs, component: Spark}
      - {step_number: 2, action: process_data, component: Spark}
      - {step_number: 3, action: produce_message, component: Kafka}
    test_data:
      input: {data_size: 100, data_format: JSON}
    assertions:
      - {assertion_type: count, description: 验证消息数量, expected_value: 100}
    cleanup: [清理HDFS数据, 清理Kafka消息]

  - case_name: spark_kafka_error_handling
    case_type: error_handling
    priority: P0
    scenario: {error_type: kafka_connection_failure}
    test_steps:
      - {step_number: 1, action: simulate_kafka_failure, component: Kafka}
    assertions:
      - {assertion_type: exception, description: 验证异常捕获}
```

---

## 三、AI执行生成

AI根据文档规范执行：

```
AI阅读文档 → 理解输入 → 根据generation_rules.md生成 → 
根据testcase_spec.md填写格式 → 根据quality_standards.md检查 → 输出结果
```

**必读文档**:
1. `docs/skill_spec.md` - Skill总体规范
2. `docs/interaction_spec.md` - 交互描述规范
3. `docs/testcase_spec.md` - 用例格式规范
4. `docs/generation_rules.md` - 生成规则

---

## 四、验证输出

**验证项**:
- 用例数量: >=3个（推荐5-10，复杂场景可超出）
- 优先级分布: P0≥50%
- 类型覆盖: 正常+异常+边界
- 质量分数: ≥0.8

---

## 五、调整生成参数（可选）

修改 `config/generation_limits.yaml`:

```yaml
quantity_limits:
  max_cases_per_scenario: 10    # 最多10个
  min_cases_per_scenario: 3     # 最少3个

priority_distribution:
  P0_ratio: 0.5                 # P0占50%
  P1_ratio: 0.3                 # P1占30%
  P2_ratio: 0.2                 # P2占20%
```

修改 `config/skill_config.yaml` 调整全局参数。

---

## 六、持续优化

1. **积累种子** - 每次测试后补充新种子用例
2. **调整配置** - 根据实际需求修改generation_limits.yaml
3. **扩展规范** - 新组件类型只需在interaction_spec.md中定义

---

## 七、常见问题

| 问题 | 原因 | 解决 |
|------|------|------|
| 用例太相似 | 种子不足 | 添加更多类型种子 |
| 覆盖率低 | 缺少异常种子 | 补充error_handling种子 |
| 用例数量不符 | 配置限制冲突 | 调整generation_limits.yaml |
| 格式不正确 | 未参照规范 | 严格参照testcase_spec.md |

---

**完整示例**: 参见 `seed_cases/spark_kafka_hdfs/example_seed.yaml`