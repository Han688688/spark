# TestCaseGenerationSkill

**v3.0 | 文档驱动 | 规范优先，代码辅助**

---

## 核心理念

优秀的Skill是**Markdown规范文档驱动**，而非Python代码实现。

| 维度 | 文档驱动 | 代码驱动 |
|------|---------|---------|
| 学习 | 读文档 ✅ | 读1500+行代码 ❌ |
| 修改 | 编辑Markdown ✅ | 修改Python ❌ |
| 复用 | 跨组件引用 ✅ | import模块 ❌ |
| AI执行 | 直接阅读 ✅ | 需理解代码 ❌ |

---

## 目录结构

```
TestCaseGenerationSkill/
├── docs/                     # ⭐ 规范文档（核心）
│   ├── skill_spec.md         # Skill总体规范+枚举权威源
│   ├── interaction_spec.md   # 交互描述规范
│   ├── testcase_spec.md      # 测试用例规范（枚举唯一定义源）
│   ├── generation_rules.md   # 生成规则
│   ├── quality_standards.md  # 质量标准
│   ├── coverage_dimensions.md# 覆盖维度+覆盖率计算
│   ├── USAGE.md              # 使用指南
│   └── README.md             # 本文档
│
├── config/                   # ⭐ YAML配置
│   ├── skill_config.yaml     # Skill全局配置
│   ├── generation_limits.yaml# 生成数量/优先级限制
│
├── templates/                # ⭐ 格式模板
│   ├── interaction_template.yaml
│   ├── test_case_template.yaml
│   ├── script_template.md    # pytest脚本模板
│   ├── coverage_analysis_template.yaml
│   └── generation_report_template.yaml
│
├── seed_cases/               # ⭐ 种子用例示例（6个场景，5种交互类型全覆盖）
│   ├── spark_kafka_hdfs/     # data_flow（大数据管道）
│   ├── flink_kafka_hive/     # data_flow（流式处理）
│   ├── nginx_redis_mysql/    # query_access（Web查询缓存）
│   ├── zookeeper_kafka/      # config_linkage（配置联动）
│   ├── redis_elasticsearch/  # state_sync（状态同步）
│   └── rabbitmq_springboot/  # event_trigger（事件触发）
│
└── examples/                 # ⭐ 使用示例
    └── README.md
```

---

## 快速开始

**Step 1 - 阅读规范**（按顺序）:
```
docs/skill_spec.md → docs/interaction_spec.md → docs/testcase_spec.md → docs/generation_rules.md
```

**Step 2 - 准备输入**:
```yaml
# 交互描述（参照 templates/interaction_template.yaml）
interaction:
  name: spark_kafka_hdfs_data_flow
  components: [Spark, Kafka, HDFS]
  flow:
    - {step: 1, component: Spark, action: read_from_hdfs}
    - {step: 2, component: Spark, action: process_data}
    - {step: 3, component: Kafka, action: produce_message}
data_schema: {input_data: {type: JSON}, output_data: {type: KafkaMessage}}
constraints: {data_constraints: [{name: input_size, min: 1, max: 10000}]}

# 种子用例（参照 templates/test_case_template.yaml）
seed_cases:
  - {case_name: normal_basic, case_type: normal_flow, priority: P0, ...}
  - {case_name: error_kafka, case_type: error_handling, priority: P0, ...}
```

**Step 3 - AI执行生成**:
AI根据文档规范执行 → 输出test_cases.yaml + test_script.py

**Step 4 - 验证输出**:
- 用例数量: >=3个（推荐5-10） ✅
- P0占比 ≥50% ✅
- 覆盖: 正常+异常+边界 ✅
- 质量分数 ≥0.8 ✅

---

## 文档索引

**必读**:
| 文档 | 说明 |
|------|------|
| `skill_spec.md` | Skill总体规范 + 枚举权威源 |
| `interaction_spec.md` | 交互描述规范（格式/字段/类型/示例） |
| `testcase_spec.md` | 用例规范 + 枚举唯一定义源 |
| `generation_rules.md` | 生成规则（数量/分布/策略） |
| `coverage_dimensions.md` | 覆盖维度 + 覆盖率计算 |

**推荐**:
| 文档 | 说明 |
|------|------|
| `quality_standards.md` | 质量检查标准（维度/阈值） |
| `USAGE.md` | 使用指南（完整步骤） |

---

## 架构演进

| 版本 | 架构 | 文件数 | 核心载体 |
|------|------|--------|---------|
| v1.0 | API全覆盖 | ~30 | Excel列表 |
| v2.0 | 代码驱动 | 18 | Python代码 |
| **v3.0** | **文档驱动** | **15** | **Markdown规范** |

---

**GitHub**: https://github.com/Han688688/spark