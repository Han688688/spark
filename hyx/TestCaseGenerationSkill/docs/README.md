# TestCaseGenerationSkill - 文档驱动版README

**Skill版本**: v3.0（文档驱动版）  
**核心理念**: **规范优先，代码辅助**

---

## 一、Skill简介

### 1.1 什么是TestCaseGenerationSkill

TestCaseGenerationSkill是一个基于**文档驱动**的测试用例自动生成规范体系，通过Markdown文档定义规范、YAML配置定义参数、模板定义格式，实现跨组件复用、灵活调整、AI友好执行的测试用例生成能力。

---

### 1.2 核心特点

**文档驱动优势**（7胜）:
1. ✅ **学习成本低** - 读文档vs读代码
2. ✅ **理解难度低** - 自然语言vs编程语言
3. ✅ **修改成本低** - 编辑文档vs修改代码
4. ✅ **维护成本低** - 文档版本vs代码版本
5. ✅ **复用性强** - 跨组件引用vsimport模块
6. ✅ **灵活性强** - 改文档即生效vs改代码需部署
7. ✅ **AI友好** - 直接阅读vs需要理解

---

## 二、目录结构

```
TestCaseGenerationSkill/
│
├── docs/                     # ⭐⭐⭐核心：规范文档
│   ├── skill_spec.md         # Skill总体规范
│   ├── interaction_spec.md   # 交互描述规范
│   ├── testcase_spec.md      # 测试用例规范
│   ├── generation_rules.md   # 生成规则
│   ├── quality_standards.md  # 质量标准
│   ├── coverage_dimensions.md# 覆盖维度（待创建）
│   ├── 文档驱动架构说明.md   # 文档驱动理念说明
│   └── README.md             # 本文档
│
├── config/                   # ⭐⭐配置：参数定义
│   ├── skill_config.yaml     # Skill配置
│   ├── generation_limits.yaml# 生成限制
│   └── quality_thresholds.yaml# 质量阈值（待创建）
│
├── templates/                # ⭐⭐模板：格式标准
│   ├── interaction_template.yaml
│   ├── testcase_template.yaml
│   └── script_template.md    # pytest模板（待创建）
│
├── seed_cases/               # ⭐种子：学习参考
│   ├── spark_kafka_hdfs/
│   └ flink_kafka_hive/
│   └── example_library.yaml
│
└── 架构优化说明.md           # 优化历程说明
```

---

## 三、快速开始

### Step 1: 阅读规范文档（必读）

```
必须阅读（按顺序）：
├── docs/README.md（本文档）
├── docs/skill_spec.md（Skill总体规范）
├── docs/interaction_spec.md（交互描述规范）
├── docs/testcase_spec.md（测试用例规范）
└── docs/generation_rules.md（生成规则）

推荐阅读：
├── docs/quality_standards.md（质量标准）
├── docs/文档驱动架构说明.md（架构理念）
└── 架构优化说明.md（优化历程）
```

---

### Step 2: 准备输入文件（必须）

```
准备交互描述：
├── 参照 templates/interaction_template.yaml
├── 参照 docs/interaction_spec.md规范
└── 创建 your_interaction.yaml

准备种子用例：
├── 参照 templates/testcase_template.yaml
├── 参照 docs/testcase_spec.md规范
├── 参照 seed_cases/spark_kafka_hdfs/example_seed.yaml示例
└── 创建 your_seed_cases.yaml

种子用例要求：
├── 至少1个正常流程用例（P0）
├── 至少1个异常处理用例（推荐）
├── 至少1个边界值用例（推荐）
```

---

### Step 3: AI执行生成

**AI执行流程**:
```
AI根据文档规范执行：
├── 阅读docs/规范文档理解输入
├── 根据generation_rules.md生成用例
├── 根据testcase_spec.md填写用例格式
├── 根据quality_standards.md检查质量
├── 根据script_template.md生成脚本
└── 输出test_cases.yaml和test_script.py
```

---

### Step 4: 验证输出

```
必须验证：
├── 输出文件格式正确（符合规范）
├── 用例数量符合限制（3-10个）
├── 质量分数达标（≥0.8）
├── 覆盖维度完整（正常+异常+边界）
```

---

## 四、使用示例

### 4.1 输入示例（交互描述）

```yaml
# interaction.yaml
interaction:
  name: spark_kafka_hdfs_data_flow
  components: [Spark, Kafka, HDFS]
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
  output_data: {type: KafkaMessage, schema: {key: id}}

constraints:
  data_constraints: [{name: input_size, min: 1, max: 10000}]
  performance_constraints: [{name: processing_time, max_ms: 5000}]
```

---

### 4.2 输入示例（种子用例）

```yaml
# seed_cases.yaml
seed_cases:
  - case_name: spark_kafka_normal_flow_basic
    case_type: normal_flow
    priority: P0
    test_steps:
      - {step_number: 1, action: read_from_hdfs, component: Spark}
      - {step_number: 2, action: process_data, component: Spark}
      - {step_number: 3, action: produce_message, component: Kafka}
    test_data:
      input: {data_size: 100, data_format: JSON}
      expected_output: {status: success}
    assertions:
      - {assertion_type: count, description: 验证消息数量, expected_value: 100}
    cleanup: [清理HDFS数据, 清理Kafka消息]
  
  - case_name: spark_kafka_error_handling_kafka_failure
    case_type: error_handling
    priority: P0
    test_steps:
      - {step_number: 1, action: simulate_kafka_failure, component: Kafka}
      - {step_number: 2, action: trigger_process, expected_result: exception}
    assertions:
      - {assertion_type: exception, description: 验证异常捕获}
```

---

### 4.3 输出示例（生成的测试用例）

```yaml
# test_cases.yaml（AI根据规范生成）
test_cases:
  - case_id: auto-202605201234-001
    case_name: spark_kafka_normal_flow_basic
    case_type: normal_flow
    priority: P0
    scenario:
      name: spark_kafka_hdfs_data_flow
      components: [Spark, Kafka, HDFS]
    preconditions:
      - HDFS正常运行
      - Kafka正常运行
    test_steps:
      - step_number: 1
        action: read_from_hdfs
        component: Spark
        expected_result: 成功读取DataFrame
      - step_number: 2
        action: process_data
        component: Spark
        expected_result: 处理成功
      - step_number: 3
        action: produce_message
        component: Kafka
        expected_result: 消息发送成功
    test_data:
      input: {data_size: 100, data_format: JSON}
    assertions:
      - assertion_type: count
        description: 验证消息数量
        expected_value: 100
    cleanup: [清理HDFS数据, 清理Kafka消息]
  
  # ... 其他用例（异常处理、边界值）
```

---

## 五、文档驱动vs代码驱动

### 对比总结

| 维度 | 文档驱动（v3.0） | 代码驱动（v2.0） |
|------|----------------|----------------|
| **学习成本** | ✅ 低（读文档） | ❌ 高（读1500+行代码） |
| **理解难度** | ✅ 低（自然语言） | ❌ 高（编程语言） |
| **修改成本** | ✅ 低（编辑Markdown） | ❌ 高（修改Python代码） |
| **复用性** | ✅ 强（跨组件引用文档） | ❌ 中（import模块） |
| **灵活性** | ✅ 强（改文档即生效） | ❌ 中（改代码需部署） |
| **AI友好** | ✅ 强（直接阅读） | ❌ 中（需要理解代码） |
| **文件数量** | ✅ 15个文档/配置 | ❌ 18个文件+代码 |

---

## 六、核心文档索引

### 6.1 必读文档（5个）

| 文档 | 路径 | 说明 | 优先级 |
|------|------|------|--------|
| README | `docs/README.md` | Skill总览（本文档） | ⭐⭐⭐⭐⭐ |
| Skill规范 | `docs/skill_spec.md` | Skill总体规范 | ⭐⭐⭐⭐⭐ |
| 交互规范 | `docs/interaction_spec.md` | 如何描述组件交互 | ⭐⭐⭐⭐⭐ |
| 用例规范 | `docs/testcase_spec.md` | 测试用例标准格式 | ⭐⭐⭐⭐⭐ |
| 生成规则 | `docs/generation_rules.md` | 用例生成规则 | ⭐⭐⭐⭐⭐ |

---

### 6.2 推荐文档（3个）

| 文档 | 路径 | 说明 | 优先级 |
|------|------|------|--------|
| 质量标准 | `docs/quality_standards.md` | 质量检查标准 | ⭐⭐⭐⭐ |
| 架构说明 | `docs/文档驱动架构说明.md` | 文档驱动理念 | ⭐⭐⭐⭐ |
| 优化说明 | `架构优化说明.md` | 优化历程 | ⭐⭐⭐ |

---

## 七、常见问题

### Q1: 为什么采用文档驱动而非代码实现？

**答案**: 文档驱动在学习、理解、修改、维护、复用、灵活性、AI友好7个维度胜出，更适合Skill规范定义场景。详细对比见`docs/文档驱动架构说明.md`。

---

### Q2: 如何调整生成规则？

**答案**: 直接编辑`docs/generation_rules.md`文档，修改规则描述即可生效。无需修改代码。

---

### Q3: 如何用于新组件（如Redis）？

**答案**: 
1. 阅读`docs/interaction_spec.md`学习交互描述规范
2. 创建Redis的交互描述YAML文件
3. 创建Redis的种子用例YAML文件
4. AI根据文档规范生成Redis测试用例

---

## 八、联系方式

**GitHub**: https://github.com/Han688688/spark  
**Issues**: https://github.com/Han688688/spark/issues  
**文档反馈**: 直接修改文档并提交PR

---

**文档结束** - TestCaseGenerationSkill v3.0 文档驱动版README