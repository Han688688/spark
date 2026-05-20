# TestCaseGenerationSkill使用指南

## 一、准备工作

### 1.1 理解Skill定位

本Skill适用于：
- 组件交互测试（多组件协同场景）
- 数据流测试（数据从一个组件流向另一个）
- 关键路径测试（聚焦重要场景，而非API全覆盖）

不适用于：
- API全覆盖测试（需要其他方案）
- 性能基准测试（需要专门工具）
- 安全测试（需要安全测试工具）

### 1.2 准备目录结构

确保Skill目录完整：
```
TestCaseGenerationSkill/
├── templates/      # 模板
├── config/         # 配置
├── seed_cases/     # 种子用例（你需要准备）
├── src/            # 源码
└── output/         # 输出（运行后生成）
```

## 二、编写交互描述

### 2.1 参考模板

打开 `templates/interaction_template.yaml`，了解必填字段。

### 2.2 编写步骤

**Step 1**: 定义交互名称
```yaml
interaction:
  name: "你的场景名称"
```

**Step 2**: 定义组件列表
```yaml
interaction:
  components: ["组件1", "组件2", "组件3"]
```

**Step 3**: 定义交互流程
```yaml
interaction:
  flow:
    - step: "步骤1"
      component: "组件1"
      action: "具体动作"
      input: "输入数据"
      output: "输出数据"
    
    - step: "步骤2"
      component: "组件2"
      action: "具体动作"
      input: "步骤1的输出"
      output: "处理后数据"
```

**Step 4**: 定义数据Schema
```yaml
data_schema:
  input_data:
    type: "JSON/Avro/Parquet"
    fields: ["字段1:类型", "字段2:类型"]
```

**Step 5**: 定义约束条件
```yaml
constraints:
  data_constraints:
    - "数据约束1"
  reliability_constraints:
    - "可靠性约束1"
```

### 2.3 示例

参考 `examples/kafka_spark_hdfs_interaction.yaml`

## 三、编写种子用例

### 3.1 理解种子用例重要性

种子用例是Skill学习的模板，质量直接影响生成结果。

### 3.2 编写要求

**必选**:
1. 正常流程用例（1个，P0优先级）
2. 边界值用例（1个，P1优先级）

**推荐**:
3. 异常处理用例（1个，P0优先级）

### 3.3 编写步骤

**Step 1**: 创建种子用例文件
```bash
mkdir seed_cases/your_scenario
touch seed_cases/your_scenario/example_seed.yaml
```

**Step 2**: 编写正常用例
```yaml
case_001:
  case_name: "正常流程_基础"
  case_type: "normal_flow"
  priority: "P0"
  
  scenario:
    description: "正常数据流处理"
  
  test_steps:
    - step_number: 1
      action: "步骤1动作"
      component: "组件1"
      expected_result: "预期结果"
  
  test_data:
    input: {"message_count": 100}
  
  assertions:
    - assertion_type: "data"
      description: "验证点描述"
      expected_value: "期望值"
  
  cleanup:
    - "清理动作1"
```

**Step 3**: 编写异常用例
```yaml
case_002:
  case_name: "组件故障处理"
  case_type: "error_handling"
  priority: "P0"
  
  scenario:
    error_type: "component_failure"
  
  test_steps:
    - step_number: 1
      action: "模拟组件故障"
```

**Step 4**: 编写边界用例
```yaml
case_003:
  case_name: "最小数据量"
  case_type: "boundary_values"
  priority: "P1"
  
  test_data:
    input: {"message_count": 1}  # 最小值
```

### 3.4 示例

参考 `seed_cases/spark_kafka_hdfs/example_seed.yaml`

## 四、配置生成限制（可选）

### 4.1 打开配置文件

`config/generation_limits.yaml`

### 4.2 调整数量限制

```yaml
quantity_limits:
  max_cases_per_scenario: 10  # 每场景最多10个用例
  min_cases_per_scenario: 3   # 每场景最少3个用例
```

### 4.3 调整优先级分布

```yaml
priority_distribution:
  P0_ratio: 0.2  # 20% P0用例
  P1_ratio: 0.5  # 50% P1用例
  P2_ratio: 0.3  # 30% P2用例
```

### 4.4 调整覆盖维度

```yaml
coverage_dimensions:
  required:
    - normal_flow      # 必选
    - error_handling   # 必选
    - boundary_values  # 必选
```

## 五、执行生成

### 5.1 命令行执行

```bash
cd TestCaseGenerationSkill

python src/skill.py \
  --interaction seed_cases/your_scenario/your_interaction.yaml \
  --seed seed_cases/your_scenario/example_seed.yaml \
  --output output
```

### 5.2 Python代码执行

```python
import yaml
from src.skill import TestCaseGenerationSkill

# 加载交互描述
with open('your_interaction.yaml') as f:
    interaction = yaml.safe_load(f)

# 加载种子用例
with open('example_seed.yaml') as f:
    seed_cases = yaml.safe_load(f)['seed_cases']

# 创建Skill
skill = TestCaseGenerationSkill()

# 执行生成
result = skill.execute(interaction, seed_cases)

# 保存输出
import json
with open('output/test_cases.json', 'w') as f:
    json.dump(result['test_cases'], f, indent=2)
```

## 六、查看输出

### 6.1 输出目录

```
output/
├── test_cases.json         # 用例列表
├── test_script_0.py        # 自动化脚本
├── coverage_analysis.yaml  # 覆盖分析
└── generation_report.md    # 执行报告
```

### 6.2 查看用例

```bash
cat output/test_cases.json

# 查看第一个用例
python -c "import json; cases=json.load(open('output/test_cases.json')); print(cases[0])"
```

### 6.3 查看脚本

```bash
cat output/test_script_0.py
```

### 6.4 查看覆盖率

```bash
cat output/coverage_analysis.yaml

# 查看总体覆盖率
grep overall_coverage output/coverage_analysis.yaml
```

### 6.5 查看报告

```bash
cat output/generation_report.md
```

## 七、执行测试脚本

### 7.1 安装依赖

```bash
pip install pytest
```

### 7.2 运行脚本

```bash
pytest output/test_script_0.py -v
```

### 7.3 查看结果

```bash
pytest output/test_script_0.py -v --tb=short
```

## 八、持续优化

### 8.1 积累种子用例

每次测试后：
1. 发现新场景，添加新种子用例
2. 发现边界值，补充边界种子
3. 发现异常，补充异常种子

### 8.2 调整生成配置

根据实际需求调整 `generation_limits.yaml`:
- 如果用例太多，降低 `max_cases_per_scenario`
- 如果覆盖不足，增加 `min_cases_per_scenario`

### 8.3 自定义模板

如果标准模板不满足需求：
1. 复制 `templates/` 模板
2. 修改模板结构
3. 在配置中指定新模板路径

## 九、常见问题解决

### 问题1: 生成的用例太相似

**原因**: 种子用例数量不足或模式单一

**解决**: 添加更多不同类型的种子用例

### 问题2: 覆盖率低

**原因**: 缺少某些维度的种子用例

**解决**: 补充 error_handling 或 boundary_values 种子用例

### 问题3: 脚本无法执行

**原因**: 组件导入路径不正确

**解决**: 编辑脚本，调整组件导入路径

### 问题4: 用例数量不符合预期

**原因**: 配置限制与实际生成冲突

**解决**: 调整 `generation_limits.yaml` 参数

## 十、最佳实践总结

1. **种子用例质量最重要** - 多花时间编写高质量种子
2. **交互描述要完整** - 按模板填写所有字段
3. **配置要合理** - 根据实际需求调整限制
4. **持续迭代** - 每次测试后补充种子

---

**开始使用**: 参考 `examples/README.md` 完整示例