# 使用示例

## Step 1: 准备交互描述

参照 `templates/interaction_template.yaml` 和 `docs/interaction_spec.md`：

```yaml
interaction:
  name: "spark_kafka_hdfs_data_flow"
  components: ["Spark", "Kafka", "HDFS"]
  flow:
    - step: 1
      component: "Spark"
      action: "read_from_hdfs"
      input: "hdfs://data/input"
      output: "DataFrame"
    - step: 2
      component: "Spark"
      action: "process_data"
      output: "ProcessedDataFrame"
    - step: 3
      component: "Kafka"
      action: "produce_message"
      input: "ProcessedDataFrame"
      output: "kafka://topic/output"

data_schema:
  input_data:
    type: "JSON"
    schema:
      fields:
        - {name: "id", type: "integer", nullable: false}
        - {name: "value", type: "float"}
  output_data:
    type: "KafkaMessage"

constraints:
  data_constraints:
    - {name: "input_size", type: "size", min: 1, max: 10000}
  performance_constraints:
    - {name: "processing_time", type: "latency", max_ms: 5000}
```

## Step 2: 准备种子用例

参照 `templates/test_case_template.yaml` 和 `docs/testcase_spec.md`：

```yaml
seed_cases:
  - case_name: "spark_kafka_normal_flow_basic"
    case_type: "normal_flow"       # 枚举: normal_flow/error_handling/boundary_values
    priority: "P0"                 # 枚举: P0/P1/P2
    scenario:
      name: "spark_kafka_hdfs_data_flow"
      components: ["Spark", "Kafka", "HDFS"]
      interaction_type: "data_flow"
    test_steps:
      - {step_number: 1, action: "read_from_hdfs", component: "Spark"}
      - {step_number: 2, action: "process_data", component: "Spark"}
      - {step_number: 3, action: "produce_message", component: "Kafka"}
    test_data:
      input: {data_size: 100, data_format: "JSON"}
    assertions:
      - {assertion_type: "count", description: "验证消息数量", expected_value: 100}
      - {assertion_type: "value", description: "验证数据完整性", expected_value: "无丢失"}
    cleanup: ["清理HDFS数据", "清理Kafka消息"]

  - case_name: "spark_kafka_error_handling"
    case_type: "error_handling"
    priority: "P0"
    scenario: {error_type: "kafka_connection_failure"}
    test_steps:
      - {step_number: 1, action: "simulate_kafka_failure", component: "Kafka"}
    assertions:
      - {assertion_type: "exception", description: "验证异常捕获", expected_value: "KafkaConnectionException"}
```

## Step 3: AI根据文档执行生成

AI按以下文档顺序阅读并执行：

```
docs/skill_spec.md → docs/interaction_spec.md → docs/testcase_spec.md → 
docs/generation_rules.md → docs/coverage_dimensions.md → docs/quality_standards.md
```

## Step 4: 验证输出

```
验证项：
- 用例数量: >=3个（推荐5-10，复杂场景可超出）
- P0占比 >= 50%
- 类型覆盖: normal_flow + error_handling + boundary_values
- 质量分数 >= 0.8
- 总体覆盖率 >= 80%
```

## 更多示例

- Spark-Kafka-HDFS: `seed_cases/spark_kafka_hdfs/example_seed.yaml`
- Flink-Kafka-Hive: `seed_cases/flink_kafka_hive/example_seed.yaml`