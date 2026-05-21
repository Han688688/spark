# 覆盖维度文档

**版本**: v3.0  
**定位**: 定义覆盖维度要求和覆盖率计算方法  
**权威来源**: 本文档是覆盖维度的唯一定义源

---

## 一、必选维度（3个）

| 维度 | case_type | 最少用例数 | 说明 |
|------|-----------|-----------|------|
| 正常流程 | normal_flow | 1 | 正常数据流，无异常 |
| 异常处理 | error_handling | 1 | 组件/网络/数据异常 |
| 边界值 | boundary_values | 1 | 最小/最大/特殊值 |

---

## 二、可选维度（2个）

> **权威来源**: testcase_spec.md零节case_type枚举。可选维度必须与case_type枚举一致，不得自行扩展。

| 维度 | case_type | 说明 | 默认启用 |
|------|-----------|------|---------|
| 性能 | performance | 大数据量/高并发 | false |
| 稳定性 | stability | 长时间运行/资源耗尽 | false |

---

## 三、覆盖率计算

### 3.1 维度覆盖率

```
维度覆盖率 = 已覆盖的必选维度数 / 总必选维度数
```

示例：覆盖了normal_flow和error_handling，但缺少boundary_values
```
维度覆盖率 = 2 / 3 = 66.7%
```

### 3.2 组件覆盖率

```
组件覆盖率 = 已覆盖的组件数 / 交互描述中的总组件数
```

### 3.3 总体覆盖率

```
总体覆盖率 = (维度覆盖率 + 组件覆盖率) / 2
```

### 3.4 达标标准

- 维度覆盖率 >= 100%（3个必选维度全部覆盖）
- 组件覆盖率 >= 100%（每个组件至少1个用例涉及）
- error_type覆盖率 >= 50%（适用error_type至少覆盖一半）
- 总体覆盖率 >= 80%

### 3.5 覆盖率门禁机制

覆盖率不达标时，**必须触发补充生成循环**，而非仅标红报告：

1. 首次生成后计算覆盖率
2. 若维度覆盖率<100% → 补充缺失维度用例
3. 若组件覆盖率<100% → 为未覆盖组件补充涉及该组件的用例
4. 若error_type覆盖率<50% → 补充缺失error_type的异常用例
5. 重新计算覆盖率，最多循环3次
6. 3次后仍不达标 → 在generation_report中标记coverage_warning

---

## 四、覆盖分析输出格式

```yaml
coverage_analysis:
  dimensions:
    normal_flow:
      covered: true
      case_count: 2
      min_required: 1
    error_handling:
      covered: true
      case_count: 1
      min_required: 1
    boundary_values:
      covered: true
      case_count: 2
      min_required: 1
  
  components:
    Spark:
      covered: true
      case_count: 5
    Kafka:
      covered: true
      case_count: 3
    HDFS:
      covered: true
      case_count: 2
  
  error_type_coverage:
    applicable_types: [component_failure, network_failure, data_corruption, timeout_failure]
    covered_types: [component_failure, network_failure]
    coverage_ratio: 0.5     # 2/4
    qualified: true         # >= 50%
  
  dimension_coverage: 1.0     # 3/3必选维度覆盖
  component_coverage: 1.0     # 3/3组件覆盖
  overall_coverage: 1.0       # (1.0+1.0)/2
  
  coverage_qualified: true    # all thresholds met
  supplement_loops: 0         # 补生成循环次数
```

---

## 五、覆盖不足时的补充策略

| 缺少维度 | 补充方式 |
|----------|---------|
| normal_flow | 从种子正常用例泛化生成 |
| error_handling | 为当前交互类型适用的error_type生成异常场景 |
| boundary_values | 从constraints提取边界类型，生成size/format/null/range边界用例 |
| 组件未覆盖 | 检查该组件是否在flow中出现，增加涉及该组件的用例 |
| error_type未覆盖 | 补充缺失error_type的异常用例，按零节适用交互类型规则 |

补充生成后必须重新计算覆盖率（见3.5门禁机制）。

---

**文档结束** - 覆盖维度文档 v3.0