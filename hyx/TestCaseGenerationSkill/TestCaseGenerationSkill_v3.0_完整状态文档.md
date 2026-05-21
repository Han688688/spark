# TestCaseGenerationSkill v3.0 完整状态文档

**记录时间**: 2026-05-21  
**版本**: v3.0（文档驱动版）  
**GitHub**: https://github.com/Han688688/spark

---

## 一、架构总览

### 1.1 目录结构

```
TestCaseGenerationSkill/
├── docs/                         # 规范文档层（核心驱动）
│   ├── skill_spec.md             # 定位+流程+枚举权威源
│   ├── interaction_spec.md       # 交互描述规范
│   ├── testcase_spec.md          # 用例格式+枚举唯一定义源
│   ├── generation_rules.md       # 数量+策略+5条泛化算法
│   ├── coverage_dimensions.md    # 3必选+4可选维度+覆盖率公式
│   ├── quality_standards.md      # 3维加权(40/30/30)+阈值0.8
│   ├── USAGE.md                  # 端到端使用流程
│   └── README.md                 # Skill总览
│
├── config/                       # 参数配置层
│   ├── skill_config.yaml         # 全局配置(v3.0, any_component:true)
│   └── generation_limits.yaml    # 数量(3-10)/优先级(P0>=50%)/必填字段(6个)
│
├── templates/                    # 格式标准层
│   ├── interaction_template.yaml # 交互描述模板(结构化constraints)
│   ├── testcase_template.yaml    # 用例模板(枚举对齐到testcase_spec)
│   └── script_template.md        # pytest脚本模板(5区结构)
│
├── seed_cases/                   # 学习参考层
│   ├── spark_kafka_hdfs/         # 4用例(normal+error+boundary_min+boundary_max)
│   ├── flink_kafka_hive/         # 3用例(normal+error+boundary)
│   └── (待补充4种交互类型种子)
│
├── examples/                     # 使用示例
│   └── README.md
│
└── .gitignore
```

### 1.2 统计

- 文件数: 16
- 目录数: 8
- 总行数: 3625
- 零Python代码
- 零空目录
- 零过时引用

---

## 二、功能描述

### 2.1 核心功能

输入交互描述(YAML) + 种子用例(YAML) → 7步流程 → 输出3-10个测试用例(YAML) + pytest脚本 + 覆盖分析 + 质量报告

### 2.2 7步执行流程

| 步骤 | 功能 | 规范来源 | 输出 |
|------|------|---------|------|
| Step 1 | 输入验证 | interaction_spec.md 5节 | 验证通过/失败 |
| Step 2 | 交互解析 | interaction_spec.md | parsed_interaction |
| Step 3 | 种子分析 | testcase_spec.md | seed_patterns |
| Step 4 | 用例生成 | generation_rules.md 7节(5条泛化算法) | 3-10个test_cases |
| Step 5 | 质量检查 | quality_standards.md(3维加权) | quality_metrics |
| Step 6 | 覆盖分析 | coverage_dimensions.md | coverage_analysis |
| Step 7 | 结果输出 | templates/*.yaml | YAML+脚本+报告 |

### 2.3 生成用例类型

| case_type | 必选 | 数量 | 优先级 | 说明 |
|-----------|------|------|--------|------|
| normal_flow | 是 | 1-3个 | P0 | 正常流程完整执行 |
| error_handling | 是 | 1-2个 | P0 | 组件/网络/数据异常处理 |
| boundary_values | 是 | 1个 | P1 | 最小/最大数据量边界 |
| performance | 可选 | 0-1个 | P1 | 大数据量/高并发 |
| stability | 可选 | 0-1个 | P2 | 长时间运行/资源耗尽 |

### 2.4 5条泛化算法

1. **步骤模板提取** - 从种子test_steps提取action列表
2. **数据量替换** - data_size替换为边界值[1, 10, 100, 1000, 10000]
3. **异常路径翻转** - 正常action→故障模拟+异常验证
4. **组件替换泛化** - 同类组件名替换
5. **验证点映射** - assertion_type模式映射

---

## 三、枚举值权威定义

**唯一定义源**: testcase_spec.md零节 + skill_spec.md五节

### 3.1 case_type（5种）

| 值 | 说明 | 优先级 | 必选 |
|----|------|--------|------|
| normal_flow | 正常流程 | P0 | 必须 |
| error_handling | 异常处理 | P0 | 必须 |
| boundary_values | 边界值 | P1 | 必须 |
| performance | 性能测试 | P1 | 可选 |
| stability | 稳定性测试 | P2 | 可选 |

### 3.2 assertion_type（6种）

| 值 | 说明 | 适用场景 |
|----|------|---------|
| value | 值验证 | 验证字段值等于预期 |
| count | 数量验证 | 验证数量等于预期 |
| exception | 异常验证 | 验证异常被抛出 |
| function | 功能验证 | 验证功能正常执行 |
| state | 状态验证 | 验证状态变化正确 |
| file | 文件验证 | 验证文件存在/内容正确 |

### 3.3 priority（3种）

| 值 | 定义 | 占比目标 |
|----|------|---------|
| P0 | 最高优先级 | >= 50% |
| P1 | 高优先级 | <= 30% |
| P2 | 中优先级 | <= 20% |

### 3.4 interaction_type（5种）

| 值 | 说明 | 种子验证 |
|----|------|---------|
| data_flow | 数据流传输 | Spark-Kafka-HDFS, Flink-Kafka-Hive |
| state_sync | 状态同步 | (待补充) |
| event_trigger | 事件触发 | (待补充) |
| query_access | 查询访问 | (待补充) |
| config_linkage | 配置联动 | (待补充) |

### 3.5 constraint.type（11种）

| 类别 | 值 |
|------|---|
| 数据约束 | size / format / range / rate / null |
| 性能约束 | latency / throughput / resource |
| 可靠性约束 | retry / guarantee / interval |

---

## 四、质量保证体系

### 4.1 三维加权

| 维度 | 权重 | 检查内容 | 分数计算 |
|------|------|---------|---------|
| 完整性 | 40% | 6必填字段存在且非空 | 包含字段用例数/总用例数 |
| 可执行性 | 30% | action在flow中存在+component在列表中+有action和expected_result | 通过检查步骤数/总步骤数 |
| 验证性 | 30% | assertion_type为6种之一+>=1个assertion | 达标用例数/总用例数 |

**总体分数** = 完整性*0.4 + 可执行性*0.3 + 验证性*0.3  
**达标阈值** >= 0.8

### 4.2 覆盖保证

| 维度 | 必选 | 最少用例数 | 达标标准 |
|------|------|-----------|---------|
| normal_flow | 是 | 1 | 维度覆盖率100% |
| error_handling | 是 | 1 | 维度覆盖率100% |
| boundary_values | 是 | 1 | 维度覆盖率100% |

**总体覆盖率** = (维度覆盖率 + 组件覆盖率) / 2 >= 80%

---

## 五、通用性现状与计划

### 5.1 当前通用性

**规范层**: 通用。5种交互类型、6种assertion_type、5种case_type均不绑定特定组件。

**验证层**: 有限。仅2个大数据种子(data_flow类型)，缺少其他4种交互类型和非大数据场景验证。

### 5.2 通用性补充计划

| 待补充种子 | 交互类型 | 场景 | 组件 |
|-----------|---------|------|------|
| nginx_redis_mysql | query_access | Web服务查询缓存数据库 | Nginx, Redis, MySQL |
| zookeeper_kafka | config_linkage | 配置中心联动消息队列 | ZooKeeper, Kafka |
| redis_elasticsearch | state_sync | 缓存同步搜索引擎 | Redis, Elasticsearch |
| rabbitmq_spring | event_trigger | 消息队列触发微服务 | RabbitMQ, Spring Boot |

补充后覆盖率: 5种交互类型全部有种子验证 → 真正通用

---

## 六、已知问题（中低优先级）

| 问题 | 说明 | 优先级 |
|------|------|--------|
| USAGE.md与examples重叠 | 内容重复可合并 | 低 |
| testcase_spec双重职责 | 格式+枚举可抽取为独立文件 | 低 |
| interaction_spec FAQ字段 | branch/loop未正式纳入flow定义 | 低 |
| data_format枚举 | 仅列大数据格式缺XML/Protobuf/REST | 中 |

---

## 七、演进历史

| 版本 | 时间 | 架构 | 文件数 | 核心载体 |
|------|------|------|--------|---------|
| v1.0 | 2026-05-18 | API全覆盖 | ~30 | Excel列表 |
| v1.5 | 2026-05-19 | Skill架构+知识图谱 | - | 概念设计 |
| v2.0 | 2026-05-20 | 代码驱动 | 18 | 1500+行Python |
| v2.5 | 2026-05-20 | 模块合并优化 | 15 | 4模块Python |
| v3.0-rc | 2026-05-20 | 文档驱动重构 | 16 | Markdown规范 |
| v3.0 | 2026-05-21 | 两轮审查修复 | 16 | 7文档+2配置+3模板+2种子 |

**关键转折**: 用户指出"优秀的Skills很多都是md文档规范"，从代码驱动转为文档驱动。

---

**文档结束** - TestCaseGenerationSkill v3.0完整状态文档