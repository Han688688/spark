# TestCaseGenerationSkill完整目录结构

## 总览

```
TestCaseGenerationSkill/
│
├── templates/                      # 模板文件目录（3个文件）
│   ├── interaction_template.yaml          # ⭐交互描述模板 - 定义组件交互格式
│   ├── test_case_template.yaml            # ⭐用例模板 - 定义测试用例格式
│   └ script_template.py                   # ⭐脚本模板 - 定义pytest脚本格式
│
├── config/                         # 配置文件目录（2个文件）
│   ├── skill_config.yaml                  # ⭐Skill主配置 - 定义全局配置
│   ├── generation_limits.yaml             # ⭐生成限制配置 - 控制数量、质量、覆盖
│
├── seed_cases/                     # 种子用例目录（按场景组织）
│   ├── spark_kafka_hdfs/                  # Spark-Kafka-HDFS场景
│   │   └ example_seed.yaml               # ⭐种子用例示例 - 必须提供
│   │
│   └ flink_kafka_hive/                    # Flink-Kafka-Hive场景
│   │   └ example_seed.yaml               # ⭐种子用例示例
│   │
│   └ [your_scenario]/                     # 你的场景（需要创建）
│       └ example_seed.yaml               # ⭐你的种子用例
│
├── src/                            # 源码目录（6个文件）
│   ├── skill.py                           # ⭐⭐⭐主Skill类 - 核心入口
│   ├── interaction_parser.py              # 交互描述解析器
│   ├── seed_analyzer.py                   # 种子用例分析器
│   ├── generator.py                       # 测试用例生成器
│   ├── script_generator.py                # 自动化脚本生成器
│   ├── quality_checker.py                 # 质量检查器
│
├── tests/                           # Skill测试目录（1个文件）
│   └ test_skill.py                        # 单元测试 - 验证Skill功能
│
├── examples/                        # 使用示例目录（1个文件）
│   └ README.md                            # ⭐⭐完整使用示例 - 如何使用Skill
│
├── docs/                            # 文档目录（2个文件）
│   ├── README.md                          # ⭐Skill总览文档
│   ├── USAGE.md                           # ⭐⭐详细使用指南
│
└── output/                          # 输出目录（运行后生成，4个文件）
    ├── test_cases.json                    # 生成的测试用例列表
    ├── test_script_*.py                   # pytest自动化脚本
    ├── coverage_analysis.yaml             # 覆盖维度分析
    └── generation_report.md               # 生成执行报告
```

---

## 各目录详细说明

### 1. templates/ 目录

**作用**: 存放标准模板，定义输入输出格式

**文件说明**:

| 文件 | 用途 | 重要性 |
|------|------|---------|
| `interaction_template.yaml` | 定义组件交互描述的标准格式，包含必填字段、示例 | ⭐⭐⭐ 必读 |
| `test_case_template.yaml` | 定义测试用例的标准格式，包含必填字段、验证点 | ⭐⭐⭐ 必读 |
| `script_template.py` | 定义pytest自动化脚本的标准结构 | ⭐⭐ 参考 |

**使用方式**:
1. 参考 `interaction_template.yaml` 编写你的交互描述
2. 参考 `test_case_template.yaml` 了解生成的用例结构
3. 参考 `script_template.py` 了解生成的脚本结构

---

### 2. config/ 目录

**作用**: 存放配置文件，控制Skill行为

**文件说明**:

| 文件 | 用途 | 重要性 |
|------|------|---------|
| `skill_config.yaml` | Skill全局配置（输入输出、执行、模型等） | ⭐⭐⭐ 必读 |
| `generation_limits.yaml` | 生成限制配置（数量、优先级、覆盖维度） | ⭐⭐⭐ 必读 |

**关键配置项**:

`skill_config.yaml`:
- 输入配置（必填输入、可选输入）
- 输出配置（输出目录、输出文件）
- 执行配置（超时、日志）
- 模型配置（大模型参数）

`generation_limits.yaml`:
- 数量限制（max/min用例数）
- 优先级分布（P0/P1/P2比例）
- 覆盖维度（必选维度、可选维度）
- 质量控制（验证点数量、必填字段）

---

### 3. seed_cases/ 目录

**作用**: 存放种子用例，提供学习模板

**目录结构**:
```
seed_cases/
├── spark_kafka_hdfs/        # 已提供示例
│   └ example_seed.yaml     # 4个种子用例（正常+异常+边界）
│
├── flink_kafka_hive/        # 已提供示例
│   └ example_seed.yaml     # 2个种子用例
│
└── [your_scenario]/         # 你需要创建自己的场景目录
    └ example_seed.yaml     # 你的种子用例（至少3个）
```

**种子用例要求**:

必填：
1. 正常流程用例（1个，P0）
2. 边界值用例（1个，P1）

推荐：
3. 异常处理用例（1个，P0）

**重要性**: ⭐⭐⭐⭐⭐ 最重要！种子质量决定生成质量！

---

### 4. src/ 目录

**作用**: Skill核心源码实现

**文件说明**:

| 文件 | 功能 | 核心程度 |
|------|------|---------|
| `skill.py` | 主Skill类，协调各模块执行 | ⭐⭐⭐⭐⭐ 核心 |
| `interaction_parser.py` | 解析组件交互描述 | ⭐⭐⭐ 重要 |
| `seed_analyzer.py` | 分析种子用例模式 | ⭐⭐⭐ 重要 |
| `generator.py` | 生成测试用例 | ⭐⭐⭐⭐ 重要 |
| `script_generator.py` | 生成pytest脚本 | ⭐⭐⭐ 重要 |
| `quality_checker.py` | 检查用例质量 | ⭐⭐ 辅助 |

**执行流程**:
```
skill.py
  ├── interaction_parser.py  # 解析交互描述
  ├── seed_analyzer.py       # 分析种子模式
  ├── generator.py           # 生成用例
  ├── script_generator.py    # 生成脚本
  └── quality_checker.py     # 质量检查
```

---

### 5. tests/ 目录

**作用**: Skill自身测试

**文件说明**:

| 文件 | 功能 |
|------|------|
| `test_skill.py` | 单元测试，验证各模块功能 |

**运行测试**:
```bash
pytest tests/test_skill.py -v
```

---

### 6. examples/ 目录

**作用**: 提供完整使用示例

**文件说明**:

| 文件 | 用途 | 重要性 |
|------|------|---------|
| `README.md` | 完整的使用示例，包含交互描述、种子用例、执行命令 | ⭐⭐⭐⭐⭐ 必读 |

**内容包含**:
1. 交互描述示例
2. 种子用例示例
3. 执行命令示例
4. Python代码示例
5. 输出结果示例

---

### 7. docs/ 目录

**作用**: 提供详细文档

**文件说明**:

| 文件 | 用途 | 重要性 |
|------|------|---------|
| `README.md` | Skill总览，目录结构，快速开始 | ⭐⭐⭐⭐ 必读 |
| `USAGE.md` | 详细使用指南，每个步骤详解 | ⭐⭐⭐⭐⭐ 必读 |

**建议阅读顺序**:
1. `docs/README.md` - 了解Skill概览
2. `docs/USAGE.md` - 学习详细步骤
3. `examples/README.md` - 查看完整示例

---

### 8. output/ 目录

**作用**: 存放生成结果（运行后自动创建）

**文件说明**:

| 文件 | 内容 | 格式 |
|------|------|------|
| `test_cases.json` | 生成的测试用例列表（case_id/name/priority/steps/assertions） | JSON |
| `test_script_*.py` | pytest自动化测试脚本（类/方法/断言） | Python |
| `coverage_analysis.yaml` | 覆盖维度分析（dimensions/components/overall） | YAML |
| `generation_report.md` | 执行报告（时间/数量/覆盖/质量） | Markdown |

---

## 文件重要性分级

### ⭐⭐⭐⭐⭐ 必须准备/必读

**必须准备**:
- `seed_cases/[your_scenario]/example_seed.yaml` - 你的种子用例

**必须阅读**:
- `docs/USAGE.md` - 详细使用指南
- `examples/README.md` - 完整使用示例
- `templates/interaction_template.yaml` - 交互描述模板

### ⭐⭐⭐⭐ 重要

**重要阅读**:
- `docs/README.md` - Skill总览
- `config/generation_limits.yaml` - 生成限制配置

**重要文件**:
- `src/skill.py` - 主Skill类
- `src/generator.py` - 用例生成器

### ⭐⭐⭐ 参考

**参考文件**:
- `templates/test_case_template.yaml` - 用例模板
- `templates/script_template.py` - 脚本模板
- `config/skill_config.yaml` - Skill配置

### ⭐⭐ 辅助

**辅助文件**:
- `tests/test_skill.py` - 单元测试
- `src/quality_checker.py` - 质量检查

---

## 快速开始指南

### 第一步：阅读文档

```bash
# 1. 了解Skill概览
cat docs/README.md

# 2. 学习详细步骤
cat docs/USAGE.md

# 3. 查看完整示例
cat examples/README.md
```

### 第二步：准备输入

```bash
# 1. 参考模板编写交互描述
cat templates/interaction_template.yaml

# 2. 参考示例编写种子用例
cat seed_cases/spark_kafka_hdfs/example_seed.yaml

# 3. 创建你的种子用例目录
mkdir seed_cases/my_scenario
touch seed_cases/my_scenario/example_seed.yaml
```

### 第三步：执行生成

```bash
# 命令行执行
python src/skill.py \
  --interaction your_interaction.yaml \
  --seed your_seed.yaml \
  --output output

# 或Python代码执行
python -c "
from src.skill import TestCaseGenerationSkill
import yaml

skill = TestCaseGenerationSkill()
interaction = yaml.safe_load(open('your_interaction.yaml'))
seed = yaml.safe_load(open('your_seed.yaml'))
result = skill.execute(interaction, seed['seed_cases'])
"
```

### 第四步：查看输出

```bash
# 查看用例
cat output/test_cases.json

# 查看脚本
cat output/test_script_0.py

# 查看覆盖率
cat output/coverage_analysis.yaml

# 查看报告
cat output/generation_report.md
```

---

## 总文件数量统计

| 目录 | 文件数量 | 说明 |
|------|---------|------|
| templates/ | 3 | 模板文件 |
| config/ | 2 | 配置文件 |
| seed_cases/ | 2+ | 种子用例（示例+你的） |
| src/ | 6 | 源码文件 |
| tests/ | 1 | 测试文件 |
| examples/ | 1 | 示例文件 |
| docs/ | 2 | 文档文件 |
| output/ | 4 | 输出文件（运行后） |
| **总计** | **19+** | 完整Skill |

---

**总结**:
- 必读文档: docs/USAGE.md + examples/README.md
- 必填输入: 交互描述 + 种子用例
- 核心源码: src/skill.py
- 输出结果: test_cases.json + test_script.py