# TestCaseGenerationSkill

基于组件交互描述和种子用例自动生成测试用例的通用Skill。

## 核心特点

1. **通用性强** - 支持任意组件组合（Spark/Kafka/Flink/HDFS等）
2. **模板驱动** - 标准化输入输出，质量可控
3. **经验驱动** - 从种子用例学习，生成质量高
4. **自动生成** - 用例+脚本+覆盖分析一体化

## 目录结构

```
TestCaseGenerationSkill/
├── templates/               # 模板文件目录
│   ├── interaction_template.yaml   # 交互描述模板 ⭐必读
│   ├── test_case_template.yaml     # 用例模板
│   └── script_template.py          # 脚本模板
│
├── config/                  # 配置文件目录
│   ├── skill_config.yaml           # Skill主配置 ⭐必读
│   └── generation_limits.yaml      # 生成限制配置 ⭐必读
│
├── seed_cases/              # 种子用例目录 ⭐必填
│   ├── spark_kafka_hdfs/           # Spark-Kafka-HDFS种子用例
│   └── flink_kafka_hive/           # Flink-Kafka-Hive种子用例
│
├── src/                     # 源码目录
│   ├── skill.py                    # 主Skill类 ⭐核心
│   ├── interaction_parser.py       # 交互解析器
│   ├── seed_analyzer.py            # 种子分析器
│   ├── generator.py                # 用例生成器
│   ├── script_generator.py         # 脚本生成器
│   └── quality_checker.py          # 质量检查器
│
├── tests/                   # Skill测试目录
│   └ test_skill.py                 # 单元测试
│
├── examples/                # 使用示例目录 ⭐必读
│   └ README.md                     # 使用示例
│
├── docs/                    # 文档目录
│   ├── README.md                    # 本文档 ⭐必读
│   └── USAGE.md                     # 使用指南
│
└── output/                  # 输出目录（运行后生成）
    ├── test_cases.json             # 生成的测试用例
    ├── test_script_*.py            # 自动化脚本
    ├── coverage_analysis.yaml      # 覆盖分析
    └── generation_report.md        # 生成报告
```

## 快速开始

### 1. 准备交互描述

参考 `templates/interaction_template.yaml` 编写组件交互描述。

**必填字段**:
- `interaction.name` - 交互名称
- `interaction.components` - 组件列表
- `interaction.flow` - 交互流程步骤
- `data_schema` - 数据Schema
- `constraints` - 约束条件

### 2. 准备种子用例

参考 `seed_cases/spark_kafka_hdfs/example_seed.yaml` 编写种子用例。

**必填内容**:
- 至少1个正常流程用例
- 至少1个异常处理用例（可选但推荐）
- 至少1个边界值用例（可选但推荐）

### 3. 执行生成

```bash
cd TestCaseGenerationSkill

python src/skill.py \
  --interaction your_interaction.yaml \
  --seed your_seed_cases.yaml \
  --output output
```

### 4. 查看输出

```bash
ls output/

# test_cases.json         - 生成的测试用例列表
# test_script_0.py        - pytest自动化脚本
# coverage_analysis.yaml  - 覆盖维度分析
# generation_report.md    - 执行报告
```

## 输入文件说明

### 交互描述（必填）

**位置**: `templates/interaction_template.yaml`

**作用**: 描述组件之间的交互过程

**示例**:
```yaml
interaction:
  name: "数据管道"
  components: ["Kafka", "Spark", "HDFS"]
  flow:
    - step: "发送消息"
      component: "Kafka"
      action: "Producer.send"
      input: "JSON数据"
      output: "Topic"
```

### 种子用例（必填）

**位置**: `seed_cases/your_scenario/example_seed.yaml`

**作用**: 提供高质量种子用例供泛化学习

**要求**:
- 正常流程用例（P0优先级）
- 异常处理用例（P0优先级）
- 边界值用例（P1优先级）

### 生成配置（可选）

**位置**: `config/generation_limits.yaml`

**作用**: 控制生成数量、质量、覆盖维度

**关键配置**:
```yaml
quantity_limits:
  max_cases_per_scenario: 10
  min_cases_per_scenario: 3

priority_distribution:
  P0_ratio: 0.2
  P1_ratio: 0.5
  P2_ratio: 0.3

coverage_dimensions:
  required:
    - normal_flow      # 必选
    - error_handling   # 必选
    - boundary_values  # 必选
```

## 输出文件说明

### test_cases.json

生成的测试用例列表，包含：
- case_id: 用例ID
- case_name: 用例名称
- priority: 优先级（P0/P1/P2）
- scenario: 场景信息
- test_steps: 测试步骤
- assertions: 验证点
- cleanup: 清理步骤

### test_script_*.py

pytest自动化测试脚本，包含：
- 测试类定义
- Setup Fixture
- 正常测试方法
- 异常测试方法
- 边界测试方法
- 辅助函数

### coverage_analysis.yaml

覆盖维度分析，包含：
- 维度覆盖（normal/error/boundary）
- 组件覆盖
- 交互类型覆盖
- 总体覆盖率

### generation_report.md

执行报告，包含：
- 生成时间统计
- 用例数量统计
- 优先级分布
- 覆盖分析
- 质量指标

## 适用场景

✅ **适用场景**:
- 组件交互测试（Kafka→Spark→HDFS）
- 数据流测试
- 边界值测试
- 异常处理测试
- 新场景扩展

❌ **不适用场景**:
- API全覆盖测试
- 性能基准测试
- 安全渗透测试

## 质量保证

### 生成限制

通过 `generation_limits.yaml` 控制：
- 数量限制（min/max用例数）
- 优先级分布（P0/P1/P2比例）
- 覆盖维度（必选维度保证）
- 数据限制（输入大小、类型）

### 质量检查

自动检查：
- 用例完整性（必填字段）
- 步骤可执行性
- 验证点有效性
- 数据合法性
- 清理完整性

### 覆盖保证

必选维度：
1. normal_flow - 正常流程
2. error_handling - 异常处理
3. boundary_values - 边界值

## 扩展性

### 添加新场景

1. 在 `seed_cases/` 创建新目录
2. 添加种子用例
3. 准备交互描述
4. 执行生成

### 自定义模板

1. 复制 `templates/` 目录模板
2. 修改模板结构
3. 在 `config/skill_config.yaml` 指定自定义模板路径

### 自定义配置

修改 `config/generation_limits.yaml`:
- 调整数量限制
- 调整优先级分布
- 调整覆盖维度

## 示例场景

### Spark-Kafka-HDFS

已提供完整示例：
- `seed_cases/spark_kafka_hdfs/example_seed.yaml`
- `examples/kafka_spark_hdfs_interaction.yaml`

### Flink-Kafka-Hive

已提供完整示例：
- `seed_cases/flink_kafka_hive/example_seed.yaml`

## 最佳实践

1. **高质量种子用例** - 至少3-5个包含正常/异常/边界
2. **清晰的交互描述** - 使用标准模板，填写完整
3. **合理的生成限制** - 控制数量，确保质量
4. **持续迭代优化** - 积累种子，优化模板

## 常见问题

**Q: 生成用例数量太多怎么办？**
A: 调整 `generation_limits.yaml` 中的 `max_cases_per_scenario`

**Q: 覆盖维度不够怎么办？**
A: 确保种子用例包含 normal/error/boundary 三种类型

**Q: 生成的脚本不能执行怎么办？**
A: 检查模板中导入部分，根据实际组件调整

**Q: 如何添加新组件？**
A: 只需在交互描述中添加组件，无需修改Skill代码

## 技术原理

### 泛化策略

1. **参数泛化** - 改变输入参数值
2. **路径泛化** - 改变执行路径
3. **组合泛化** - 组合多个场景

### 学习机制

从种子用例提取：
- 交互模式
- 数据模式
- 验证模式
- 异常处理模式
- 清理模式

## 开发者信息

- **Skill名称**: TestCaseGenerationSkill
- **版本**: 1.0.0
- **类型**: test_generation
- **作者**: AI4SE

---

**参考文档**:
- templates/interaction_template.yaml - 交互描述模板
- config/generation_limits.yaml - 生成限制配置
- examples/README.md - 使用示例

**开始使用**: 阅读 `examples/README.md`