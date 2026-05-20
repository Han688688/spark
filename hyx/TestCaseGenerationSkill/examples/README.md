# 使用示例：生成Kafka-Spark-HDFS数据管道测试用例

# ========== Step 1: 准备交互描述 ==========

# 文件：examples/kafka_spark_hdfs_interaction.yaml

interaction:
  name: "实时数据处理管道"
  components: ["Kafka", "Spark", "HDFS"]
  
  flow:
    - step: "数据采集"
      component: "Kafka"
      action: "Producer发送JSON消息到Topic"
      input: "业务数据"
      output: "Kafka消息队列"
      
    - step: "数据处理"
      component: "Spark"
      action: "Structured Streaming消费处理"
      input: "Kafka Topic"
      output: "处理后DataFrame"
      
    - step: "数据存储"
      component: "HDFS"
      action: "写入Parquet文件"
      input: "DataFrame"
      output: "HDFS文件"

data_schema:
  input_data:
    type: "JSON"
    fields: ["id:int", "name:string", "value:double", "timestamp:long"]
    example: '{"id":1,"name":"test","value":100.0,"timestamp":1234567890}'
  
  output_data:
    type: "Parquet"
    format: "列式存储"

constraints:
  data_constraints:
    - "消息不能重复消费"
    - "文件写入保证完整性"
  performance_constraints:
    - "吞吐量>=10000条/秒"
  reliability_constraints:
    - "异常自动重试"
    - "Checkpoint机制保证"

# ========== Step 2: 准备种子用例 ==========

# 文件：examples/seed_cases.yaml

seed_cases:
  - case_name: "正常数据流"
    case_type: "normal_flow"
    priority: "P0"
    scenario:
      name: "实时数据处理管道"
      components: ["Kafka", "Spark", "HDFS"]
    test_steps:
      - step_number: 1
        action: "Kafka发送100条消息"
      - step_number: 2
        action: "Spark处理"
      - step_number: 3
        action: "写入HDFS"
    test_data:
      input: {"message_count": 100}
    assertions:
      - assertion_type: "data"
        description: "验证数量匹配"
    cleanup:
      - "清理数据"
  
  - case_name: "Kafka异常"
    case_type: "error_handling"
    priority: "P0"
    scenario:
      error_type: "component_failure"
    test_steps:
      - step_number: 1
        action: "模拟Kafka故障"
    assertions:
      - assertion_type: "exception"
        description: "验证异常处理"

# ========== Step 3: 执行生成 ==========

# 命令行执行：
# cd TestCaseGenerationSkill
# python src/skill.py \
#   --interaction examples/kafka_spark_hdfs_interaction.yaml \
#   --seed examples/seed_cases.yaml \
#   --output output

# ========== Step 4: 查看输出 ==========

# 输出目录结构：
# output/
# ├── test_cases.json           # 生成的测试用例
# ├── test_script_0.py          # 自动化脚本
# ├── coverage_analysis.yaml    # 覆盖分析
# └── generation_report.md      # 生成报告

# ========== Python代码示例 ==========

import yaml
from src.skill import TestCaseGenerationSkill

# 加载交互描述
with open('examples/kafka_spark_hdfs_interaction.yaml') as f:
    interaction = yaml.safe_load(f)

# 加载种子用例
with open('examples/seed_cases.yaml') as f:
    seed_cases = yaml.safe_load(f)['seed_cases']

# 创建Skill实例
skill = TestCaseGenerationSkill()

# 执行生成
result = skill.execute(interaction, seed_cases)

# 打印结果
print(f"生成用例数量: {len(result['test_cases'])}")
print(f"覆盖率: {result['coverage_analysis']['overall_coverage']*100:.1f}%")

# 查看第一个用例
first_case = result['test_cases'][0]
print(f"用例名称: {first_case['case_name']}")
print(f"优先级: {first_case['priority']}")