# TestCaseGenerationSkill v3.0 完整状态文档

**记录时间**: 2026-05-21  
**版本**: v3.0（文档驱动版）  
**GitHub**: https://github.com/Han688688/spark

---

## 一、项目概述

### 1.1 Skill定位

- **名称**: TestCaseGenerationSkill
- **目标**: 基于组件交互描述和种子用例，自动生成高质量测试用例
- **类型**: 测试工程类Skill
- **适用**: 多组件协作场景（数据流、状态同步、事件触发、查询访问、配置联动）
- **不适用**: 单组件系统、纯UI测试、纯手工测试

### 1.2 文档驱动架构

本Skill采用**文档驱动架构**：Markdown规范文档是唯一的行为定义载体，AI直接读取规范文档执行生成逻辑，无需Python代码。

**架构分层**:

| 层 | 载体 | 作用 | 文件数 |
|----|------|------|--------|
| 规范层 | docs/*.md | 定义流程、格式、枚举、规则、覆盖、质量 | 8 |
| 配置层 | config/*.yaml | 参数阈值（数量/优先级/门禁） | 2 |
| 模板层 | templates/*.yaml/*.md | 输出格式标准 | 5 |
| 学习层 | seed_cases/*/example_seed.yaml | 种子用例参考 | 6 |
| 示例层 | examples/README.md | 端到端使用说明 | 1 |
| 输出层 | output/*.yaml | 运行产物（用例/脚本/报告） | 6 |

**关键特性**: 零Python代码、规范文档即程序、枚举值唯一定义源、门禁闭环自修正。

---

## 二、文件清单

### 2.1 完整目录树

```
TestCaseGenerationSkill/
├── docs/                                    # 规范文档层
│   ├── skill_spec.md                        # 定位+流程+枚举索引 (110行)
│   ├── interaction_spec.md                  # 交互描述规范+constraint.type枚举 (849行)
│   ├── testcase_spec.md                     # 用例格式+6枚举唯一定义源 (701行)
│   ├── generation_rules.md                  # 9节结构+7条泛化算法9.1-9.7 (384行)
│   ├── coverage_dimensions.md               # 3必选+2可选+4覆盖率因子+5达标条件 (158行)
│   ├── quality_standards.md                 # 3维加权(40/30/30)+5项门禁 (230行)
│   ├── USAGE.md                             # 端到端使用流程 (150行)
│   └── README.md                            # Skill总览 (124行)
│
├── config/                                  # 参数配置层
│   ├── skill_config.yaml                    # 全局配置(v3.0, any_component:true) (94行)
│   └── generation_limits.yaml               # 数量[3,50]/优先级(P0>=50%,P1<=30%且<=3个) (172行)
│
├── templates/                               # 格式标准层
│   ├── interaction_template.yaml            # 交互描述模板(结构化constraints) (119行)
│   ├── test_case_template.yaml              # 用例模板(枚举对齐testcase_spec零节) (91行)
│   ├── script_template.md                   # pytest脚本模板(5区结构) (217行)
│   ├── coverage_analysis_template.yaml      # 覆盖分析输出模板 (45行)
│   └── generation_report_template.yaml      # 生成报告输出模板 (46行)
│
├── seed_cases/                              # 学习参考层（6场景全覆盖5交互类型）
│   ├── spark_kafka_hdfs/example_seed.yaml   # data_flow种子 (232行)
│   ├── flink_kafka_hive/example_seed.yaml   # data_flow种子 (175行)
│   ├── nginx_redis_mysql/example_seed.yaml  # query_access种子 (194行)
│   ├── rabbitmq_springboot/example_seed.yaml # event_trigger种子 (182行)
│   ├── redis_elasticsearch/example_seed.yaml # state_sync种子 (171行)
│   └── zookeeper_kafka/example_seed.yaml    # config_linkage种子 (164行)
│
├── examples/                                # 使用示例
│   └── README.md                            # 端到端使用说明 (100行)
│
├── output/                                  # 运行产物（示例输出）
│   ├── test_cases.yaml                      # 生成的测试用例 (354行)
│   ├── quality_metrics.yaml                 # 质量评分结果 (66行)
│   ├── coverage_analysis.yaml               # 覆盖分析结果 (54行)
│   ├── generation_report.yaml               # 生成报告 (62行)
│   ├── interaction.yaml                     # 输入的交互描述 (103行)
│   └── seed.yaml                            # 输入的种子用例 (186行)
│
├── .gitignore                               # Git忽略规则 (46行)
└── TestCaseGenerationSkill_v3.0_完整状态文档.md  # 本文档
```

### 2.2 统计

| 指标 | 值 |
|------|---|
| 总文件数 | 30 |
| 总行数 | 5579 |
| 规范文档行数 | 2706 |
| 种子用例行数 | 1118 |
| Python代码 | 0 |
| 空目录 | 0 |
| 过时引用 | 0 |

---

## 三、枚举权威源清单

6枚举表是全Skill体系的唯一定义源，模板、种子、配置中的枚举值必须与此一致。冲突解决规则：用例相关以testcase_spec.md零节为准，交互相关以interaction_spec.md为准。

### 3.1 case_type（5种） — 权威源: testcase_spec.md零节

| 值 | 说明 | 优先级 | 必选 |
|----|------|--------|------|
| normal_flow | 正常流程 | P0 | 必须 |
| error_handling | 异常处理 | P0 | 必须 |
| boundary_values | 边界值 | P1 | 必须 |
| performance | 性能测试 | P1 | 可选 |
| stability | 稳定性测试 | P2 | 可选 |

### 3.2 assertion_type（6种） — 权威源: testcase_spec.md零节

| 值 | 说明 | 适用场景 |
|----|------|---------|
| value | 值验证 | 验证字段值等于预期 |
| count | 数量验证 | 验证数量等于预期 |
| exception | 异常验证 | 验证异常被抛出 |
| function | 功能验证 | 验证功能正常执行 |
| state | 状态验证 | 验证状态变化正确 |
| file | 文件验证 | 验证文件存在/内容正确 |

### 3.3 priority（3种） — 权威源: testcase_spec.md零节

| 值 | 定义 | 占比目标 |
|----|------|---------|
| P0 | 最高优先级 | >= 50% |
| P1 | 高优先级 | <=30%且<=3个 |
| P2 | 中优先级 | <= 20% |

**P1双约束**: 数量占比不超过30%，且绝对个数不超过3个。

### 3.4 error_type（5种+派生规则） — 权威源: testcase_spec.md零节

| 值 | 说明 | 适用交互类型 |
|----|------|-------------|
| component_failure | 组件故障/不可用 | 全类型 |
| network_failure | 网络/连接异常 | data_flow, event_trigger, query_access |
| data_corruption | 数据格式错误/损坏/缺失 | data_flow, state_sync |
| timeout_failure | 超时/延迟异常 | 全类型 |
| configuration_error | 配置错误/不兼容 | config_linkage, state_sync |

**派生规则**: 可从5种大类派生组件特定故障（如kafka_connection_failure根属component_failure），派生error_type必须以5种大类为根。

### 3.5 interaction_type（5种） — 权威源: interaction_spec.md

| 值 | 说明 | 种子验证 |
|----|------|---------|
| data_flow | 数据流传输 | spark_kafka_hdfs, flink_kafka_hive |
| state_sync | 状态同步 | redis_elasticsearch |
| event_trigger | 事件触发 | rabbitmq_springboot |
| query_access | 查询访问 | nginx_redis_mysql |
| config_linkage | 配置联动 | zookeeper_kafka |

### 3.6 constraint.type（11种） — 权威源: interaction_spec.md

| 类别 | 值 |
|------|---|
| 数据约束 | size / format / range / rate / null |
| 性能约束 | latency / throughput / resource |
| 可靠性约束 | retry / guarantee / interval |

---

## 四、执行流程

### 4.1 8步执行流程

| 步骤 | 功能 | 规范来源 | 输出 | 说明 |
|------|------|---------|------|------|
| Step 1 | 输入验证 | interaction_spec.md | 验证通过/失败 | 验证格式和必填字段 |
| Step 2 | 交互解析 | interaction_spec.md | parsed_interaction | 提取组件、流程、数据Schema |
| Step 3 | 种子分析 | testcase_spec.md | seed_patterns | 提取交互模式、数据模式、验证模式 |
| Step 4 | 用例生成 | generation_rules.md(7条泛化算法) | 3-50个test_cases | 根据规则+种子泛化生成 |
| Step 5 | 质量检查 | quality_standards.md(3维加权) | quality_metrics | 完整性/可执行性/验证性分数 |
| Step 6 | 覆盖分析 | coverage_dimensions.md(4因子+5达标) | coverage_analysis | 维度/组件/路径/error_type覆盖率 |
| Step 7 | 联合门禁 | skill_spec.md + quality_standards.md | pass/fix/supplement | quality>=0.8 AND coverage全部达标 |
| Step 8 | 结果输出 | templates/*.yaml | YAML+脚本+报告 | 输出用例、脚本、覆盖分析、报告 |

### 4.2 联合门禁逻辑（Step 7）

```
Step 7: 联合门禁判定
  ├─ 质量不达标(overall_score<0.8) → 修复重生成（返回Step 4）
  ├─ 覆盖不达标(5项任一未达标) → 补充生成循环（返回Step 4，最多3次）
  │   ├─ 维度覆盖率<100% → 补缺失维度用例
  │   ├─ 组件覆盖率<100% → 为未覆盖组件补充用例
  │   ├─ 路径覆盖率不足 → 补缺失flow.action
  │   ├─ error_type覆盖率<50% → 补缺失error_type异常用例
  │   └─ 总体覆盖率<80% → 综合补充
  ├─ 3次补充后仍不达标 → 标记coverage_warning，输出报告
  └─ 全部达标 → 进入Step 8输出
```

---

## 五、覆盖保障

### 5.1 维度定义（3必选+2可选）

| 维度 | case_type | 必选 | 最少用例数 |
|------|-----------|------|-----------|
| 正常流程 | normal_flow | 是 | 1 |
| 异常处理 | error_handling | 是 | 1 |
| 边界值 | boundary_values | 是 | 1 |
| 性能 | performance | 否(默认禁用) | 0-1 |
| 稳定性 | stability | 否(默认禁用) | 0-1 |

### 5.2 四覆盖率因子

| 因子 | 计算公式 | 说明 |
|------|---------|------|
| 维度覆盖率 | 已覆盖必选维度数 / 总必选维度数(3) | 必达100% |
| 组件覆盖率 | 已覆盖组件数 / 交互描述总组件数 | 每组件至少1个用例涉及 |
| 路径覆盖率 | 无分支: 已覆盖action数/总action数; 含分支: 已覆盖(branch×action)组合数/总组合数 | 无分支>=100%, 含分支>=80% |
| error_type覆盖率 | 已覆盖适用error_type数 / 适用error_type总数 | >= ceil(适用数×50%) |

**总体覆盖率公式**:
```
总体覆盖率 = (维度覆盖率 + 组件覆盖率 + 路径覆盖率) / 3
```

### 5.3 五达标条件（全部必须满足）

| 序号 | 条件 | 阈值 |
|------|------|------|
| 1 | 维度覆盖率 >= 100% | 3个必选维度全部覆盖 |
| 2 | 组件覆盖率 >= 100% | 每组件至少1个用例涉及 |
| 3 | 路径覆盖率 >= 100%(无分支) 或 >= 80%(含分支) | 按场景分支情况 |
| 4 | error_type覆盖率 >= ceil(适用数×50%) | 适用3种需覆盖2种，适用2种需覆盖1种 |
| 5 | 总体覆盖率 >= 80% | (维度+组件+路径)/3 |

### 5.4 门禁循环机制

覆盖率不达标时**必须触发补充生成循环**，而非仅标红报告：

1. 首次生成后计算覆盖率
2. 识别未达标因子，按针对性策略补充（见coverage_dimensions.md五节）
3. 重新计算覆盖率
4. 最多循环3次
5. 3次后仍不达标 → generation_report中标记coverage_warning

---

## 六、质量评分

### 6.1 三维加权体系

| 维度 | 权重 | 检查内容 | 分数计算 |
|------|------|---------|---------|
| 完整性 | 40% | 6必填字段(case_id/name/type/priority/steps/assertions) + 5推荐字段加分 | 包含字段用例数/总用例数 |
| 可执行性 | 30% | action合法性+component在列表中+步骤格式正确+衍生action命名规范 | 通过4项检查步骤数/总步骤数 |
| 验证性 | 30% | assertion_type为6种之一+>=1个assertion+description+expected_value | 达标用例数/总用例数 |

**总体分数** = 完整性×0.4 + 可执行性×0.3 + 验证性×0.3

### 6.2 五项质量门禁（全部必须通过）

| 序号 | 门禁 | 阈值 | 不达标处理 |
|------|------|------|-----------|
| 1 | 完整性分数 >= 0.8 | 0.8 | 标红+修复建议 |
| 2 | 可执行性分数 >= 0.8 | 0.8 | 标红+修复建议 |
| 3 | 验证性分数 >= 0.8 | 0.8 | 标红+修复建议 |
| 4 | 总体分数 >= 0.8 | 0.8 | 修复重生成 |
| 5 | 覆盖率门禁通过 | 5项达标(见五节) | 补充生成循环(最多3次) |

### 6.3 质量与覆盖联合门禁

**达标条件**: quality_score>=0.8 AND coverage全部达标  
**不达标路径**: 质量不达标→修复重生成; 覆盖不达标→补充生成循环  
**最终兜底**: 3次循环后仍不达标→标记warning输出报告

---

## 七、种子场景

### 7.1 6种子场景一览

| 种子目录 | 交互类型 | 涉及组件 | 文件行数 | 覆盖类型 |
|---------|---------|---------|---------|---------|
| spark_kafka_hdfs | data_flow | Spark, Kafka, HDFS | 232 | normal + error + boundary |
| flink_kafka_hive | data_flow | Flink, Kafka, Hive | 175 | normal + error + boundary |
| nginx_redis_mysql | query_access | Nginx, Redis, MySQL | 194 | normal + error + boundary |
| rabbitmq_springboot | event_trigger | RabbitMQ, Spring Boot | 182 | normal + error + boundary |
| redis_elasticsearch | state_sync | Redis, Elasticsearch | 171 | normal + error + boundary |
| zookeeper_kafka | config_linkage | ZooKeeper, Kafka | 164 | normal + error + boundary |

### 7.2 5交互类型覆盖验证

| 交互类型 | 种子数量 | 已验证 | 状态 |
|---------|---------|--------|------|
| data_flow | 2 | spark_kafka_hdfs, flink_kafka_hive | 已验证 |
| query_access | 1 | nginx_redis_mysql | 已验证 |
| event_trigger | 1 | rabbitmq_springboot | 已验证 |
| state_sync | 1 | redis_elasticsearch | 已验证 |
| config_linkage | 1 | zookeeper_kafka | 已验证 |

**覆盖率**: 5种交互类型全部有种子验证，any_component:true约束成立。

---

## 八、关键设计决策

### 8.1 为何文档驱动而非代码驱动

| 因素 | 代码驱动(v2.0) | 文档驱动(v3.0) |
|------|---------------|---------------|
| 载体 | 1500+行Python | 7份Markdown规范 |
| 维护成本 | 改逻辑=改代码+改测试 | 改规则=改Markdown |
| AI可读性 | 需解析代码语义 | 直接读取规范语义 |
| 扩展性 | 新类型=新代码 | 新类型=加枚举值 |
| 通用性 | 绑定特定组件 | any_component:true |

**转折点**: v2.0代码驱动版本验证时，发现AI更容易遵循明确文档规范而非隐式代码逻辑。

### 8.2 为何门禁闭环而非单次检查

- **问题**: 单次生成难以保证覆盖率（维度/组件/路径/error_type4个因子同时达标概率低）
- **解决**: Step 7联合门禁不达标→自动补充生成循环，最多3次
- **效果**: 从"报告问题"变为"修正问题"，覆盖率从可能不达标变为大概率达标

### 8.3 为何枚举唯一定义源

- **问题**: 多文件枚举值不一致导致生成逻辑混乱（v2.0教训）
- **解决**: testcase_spec.md零节定义用例枚举(case_type/assertion_type/priority/error_type)，interaction_spec.md定义交互枚举(interaction_type/constraint.type)
- **效果**: 冲突解决有明确规则，AI生成时枚举引用唯一无歧义

### 8.4 为何P1双约束（≤30%且≤3个）

- **问题**: 仅约束占比时，大量用例场景下P1绝对数量仍可能过多（如20个用例×30%=6个P1）
- **解决**: 双重约束——占比不超过30%，且绝对个数不超过3个
- **效果**: 无论用例总量多少，P1用例始终控制在3个以内

### 8.5 为何7条泛化算法而非5条

v2.0有5条泛化算法（步骤模板/数据量/异常翻转/组件替换/验证映射），v3.0新增2条：

| 新增算法 | 编号 | 解决的问题 |
|---------|------|-----------|
| Action映射规则 | 9.6 | 正常用例action需匹配flow.actions，异常用例允许衍生action |
| 分支循环路径覆盖 | 9.7 | 含branch/loop的flow需生成分支用例和循环验证用例 |

---

## 九、评分历史

| 版本 | 评分 | 时间 | 关键改进 |
|------|------|------|---------|
| v1.0 | 5.0 | 2026-05-18 | API全覆盖但Excel载体，无泛化算法 |
| v1.5 | 7.2 | 2026-05-19 | Skill架构+知识图谱概念设计 |
| v2.0 | 7.3 | 2026-05-20 | 代码驱动，1500+行Python，5条泛化算法 |
| v2.5 | 7.75 | 2026-05-20 | 模块合并优化，4模块Python |
| v3.0-rc | 8.13 | 2026-05-20 | 文档驱动重构，7文档+5枚举唯一定义源 |
| v3.0 | 8.5 | 2026-05-21 | 两轮审查修复，P1双约束，error_type覆盖率纳入 |
| v3.0+ | 9.0+ | 2026-05-21 | 6种子全覆盖5交互类型，7条泛化算法，4覆盖率因子+5达标条件+5质量门禁 |

**评分跃升关键节点**:
- 5→7.2: 从Excel列表到Skill架构概念
- 7.3→8.13: 从代码驱动到文档驱动（关键转折）
- 8.5→9.0+: 种子补齐5交互类型+覆盖保障闭环+质量门禁完备

---

**文档结束** - TestCaseGenerationSkill v3.0完整状态文档