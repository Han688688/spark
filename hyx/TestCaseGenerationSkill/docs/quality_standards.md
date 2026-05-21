# 质量标准文档

**版本**: v3.0  
**定位**: 定义测试用例的质量检查标准和阈值

---

## 一、质量维度定义

### 1.1 三大质量维度

| 维度 | 定义 | 权重 |
|------|------|------|
| **完整性** | 用例是否包含所有必填字段 | 40% |
| **可执行性** | 用例步骤是否可实际执行 | 30% |
| **验证性** | 用例验证点是否有效 | 30% |

---

### 1.2 质量分数计算

**总体分数公式**:
```
总体分数 = 完整性分数 * 0.4 + 可执行性分数 * 0.3 + 验证性分数 * 0.3
```

**目标**: 总体分数 ≥ 0.8（80分）

---

## 二、完整性标准

### 2.1 必填字段检查

**必填字段列表**（6个）:
1. `case_id`
2. `case_name`
3. `case_type`
4. `priority`
5. `test_steps`
6. `assertions`

**检查规则**:
- 每个字段必须存在且非空
- 字段类型必须正确（string/list等）

**完整性分数计算**:
```
完整性分数 = 包含必填字段的用例数 / 总用例数
```

---

### 2.2 推荐字段检查

**推荐字段列表**（5个）:
1. `scenario`
2. `preconditions`
3. `test_data`
4. `cleanup`
5. `generated_time`

**加分项**: 包含推荐字段可提高完整性分数（每包含1个推荐字段加0.1分，上限加0.5分）

---

## 三、可执行性标准

### 3.1 步骤可执行性

**检查规则**:
- 每个步骤包含action、component、expected_result
- 步骤编号从1开始递增
- component存在于交互描述的components列表中

**可执行性检查项**:
1. 正常用例步骤action是否在交互描述flow的action列表中存在或可合理泛化
2. 异常用例步骤action是否遵循衍生action命名规则（simulate_xxx/verify_xxx）
3. 步骤component是否在交互描述components列表中存在
4. 步骤是否包含action和expected_result两个必填子字段

**可执行性分数计算**:
```
可执行性分数 = 通过以上4项检查的步骤数 / 总步骤数
```

---

### 3.2 数据可执行性

**检查规则**:
- test_data.input包含data_size和数据格式
- data_size在约束范围内（min-max之间）

---

## 四、验证性标准

### 4.1 验证点数量

**最低要求**:
- 每个用例至少包含1个验证点
- 推荐包含2-3个验证点

**验证点分数计算**:
```
验证性分数 = 验证点达标用例数 / 总用例数
```

---

### 4.2 验证点类型

**有效验证类型**（6种，权威定义见testcase_spec.md零节）:
1. `value` - 值验证
2. `count` - 数量验证
3. `exception` - 异常验证
4. `function` - 功能验证
5. `state` - 状态验证
6. `file` - 文件验证

**检查规则**:
- assertion_type必须是上述6种之一
- 每个验证点包含description和expected_value

---

## 五、质量阈值配置

### 5.1 阈值定义

**最低阈值**（YAML配置）:
```yaml
quality_thresholds:
  completeness:
    min_score: 0.8
    required_fields: 6
  
  executability:
    min_score: 0.8
    min_steps: 1
  
  assertion:
    min_score: 0.8
    min_assertions: 1
  
  overall:
    min_score: 0.8
```

---

### 5.2 质量达标判定

**判定规则**:
```
达标条件:
  - 完整性分数 ≥ 0.8
  - 可执行性分数 ≥ 0.8
  - 验证性分数 ≥ 0.8
  - 总体分数 ≥ 0.8
```

**不达标处理**: 在生成报告中标红不达标项，并附具体修复建议

---

## 六、质量问题列表

### 6.1 常见质量问题

| 问题类型 | 描述 | 解决方式 |
|----------|------|---------|
| **缺少必填字段** | 用例缺少case_name等字段 | 补充必填字段 |
| **验证点不足** | 用例少于1个验证点 | 增加验证点 |
| **步骤数量不足** | 用例少于1个测试步骤 | 增加测试步骤 |
| **缺少清理步骤** | 用例缺少cleanup字段 | 增加清理步骤 |
| **优先级错误** | priority字段不在P0/P1/P2范围内 | 修正优先级 |

---

## 七、质量检查流程

### 7.1 检查步骤

```
Step 1: 验证必填字段完整性
Step 2: 验证步骤数量和格式
Step 3: 验证验证点数量和类型
Step 4: 计算完整性分数
Step 5: 计算可执行性分数
Step 6: 计算验证性分数
Step 7: 计算总体分数
Step 8: 判定是否达标
Step 9: 输出质量报告
```

---

## 八、质量报告模板

### 8.1 质量报告格式

```yaml
quality_metrics:
  total_cases: number
  valid_cases: number
  invalid_cases: number
  
  completeness_score: float
  executability_score: float
  assertion_score: float
  overall_score: float
  
  issues:
    - issue_type: string
      case_id: string
      description: string
```

---

**文档结束** - 质量标准文档 v3.0