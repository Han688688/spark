# TestCaseGenerationSkill规范文档

**版本**: v3.0  
**定位**: Skill总体规范定义  
**权威性**: 本文档是Skill定位和流程的唯一定义源，详细规范见对应子文档

---

## 一、Skill定位

**名称**: TestCaseGenerationSkill  
**目标**: 基于组件交互描述和种子用例，自动生成高质量测试用例  
**类型**: 测试工程类Skill  

**适用**: 多组件协作、数据流、事件触发、状态同步、查询访问、配置联动  
**不适用**: 单组件系统、纯UI测试、纯手工测试

---

## 二、输入输出

### 2.1 输入

**必须**:
| 输入 | 格式 | 规范文档 | 模板 |
|------|------|---------|------|
| 交互描述 | YAML | interaction_spec.md | templates/interaction_template.yaml |
| 种子用例 | YAML | testcase_spec.md | templates/test_case_template.yaml |

**可选**:
| 输入 | 格式 | 默认路径 |
|------|------|---------|
| Skill配置 | YAML | config/skill_config.yaml |
| 生成限制 | YAML | config/generation_limits.yaml |

### 2.2 输出

**必须**:
| 输出 | 格式 | 规范文档 |
|------|------|---------|
| 测试用例 | YAML | testcase_spec.md |
| 自动化脚本 | Python | templates/script_template.md |
| 覆盖分析 | YAML | templates/coverage_analysis_template.yaml |
| 生成报告 | YAML | templates/generation_report_template.yaml |

---

## 三、执行流程

```
Step 1: 输入验证    → 验证格式和必填字段
Step 2: 交互解析    → 提取组件、流程、数据Schema（详见interaction_spec.md）
Step 3: 种子分析    → 提取交互模式、数据模式、验证模式（详见testcase_spec.md）
Step 4: 用例生成    → 根据规则生成（详见generation_rules.md）
Step 5: 质量检查    → 检查完整性、可执行性、验证性（详见quality_standards.md）
Step 6: 覆盖分析    → 分析维度和组件覆盖（详见coverage_dimensions.md）
Step 7: 结果输出    → 输出YAML用例、脚本、报告
```

---

## 四、核心约束

- 用例数量: N >= 3（推荐5-10，复杂场景可超出，详见generation_rules.md）
- P0占比 >= 50%（详见generation_rules.md）
- 类型覆盖: normal_flow + error_handling + boundary_values（详见coverage_dimensions.md）
- 质量分数 >= 0.8（详见quality_standards.md）
- 组件不限定：any_component: true（已验证6个场景，覆盖5种交互类型）

---

## 五、枚举值索引

以下为本Skill涉及的枚举值汇总索引（便于快速查阅），具体定义和扩展规则请查看对应的权威子文档：

| 枚举 | 权威定义源 | 值（概要） |
|------|-----------|-----------|
| case_type | testcase_spec.md零节 | 5种: normal_flow / error_handling / boundary_values / performance / stability |
| assertion_type | testcase_spec.md零节 | 6种: value / count / exception / function / state / file |
| priority | testcase_spec.md零节 | 3种: P0 / P1 / P2 |
| error_type | testcase_spec.md零节 | 5种: component_failure / network_failure / data_corruption / timeout_failure / configuration_error |
| interaction_type | interaction_spec.md | 5种: data_flow / state_sync / event_trigger / query_access / config_linkage |
| constraint.type | interaction_spec.md | 11种: size / format / range / rate / null / latency / throughput / resource / retry / guarantee / interval |

**冲突解决**: 如各文件枚举值有冲突，以testcase_spec.md零节（用例相关）和interaction_spec.md（交互相关）为权威定义源，本表仅为索引。

---

## 六、文档索引

**必读**:
| 文档 | 路径 | 说明 |
|------|------|------|
| Skill规范 | docs/skill_spec.md | 本文档 |
| 交互规范 | docs/interaction_spec.md | 交互描述格式和字段 |
| 用例规范 | docs/testcase_spec.md | 用例格式和枚举值 |
| 生成规则 | docs/generation_rules.md | 数量、分布、策略 |
| 覆盖维度 | docs/coverage_dimensions.md | 覆盖要求和计算 |

**推荐**:
| 文档 | 路径 | 说明 |
|------|------|------|
| 质量标准 | docs/quality_standards.md | 质量维度和阈值 |
| 使用指南 | docs/USAGE.md | 端到端使用流程 |

---

**文档结束** - skill_spec.md v3.0