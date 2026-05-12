# 多组件交互场景生成系统 - Skill架构设计

## 1. 系统架构总览

### 核心设计理念
- **知识驱动**：以知识图谱为核心，所有skill基于知识图谱运行
- **链式处理**：skill之间通过输入输出串联，形成处理流水线
- **分层架构**：核心skill（内嵌）+ 扩展skill（外部服务）

---

## 2. Skill定义（5个核心Skill）

### Skill 1: KnowledgeGraphSkill（知识图谱管理）

**职责**：存储、管理、查询组件交互知识

| 属性 | 值 |
|------|---|
| **Skill名称** | KnowledgeGraphSkill |
| **类型** | 核心skill（内嵌） |
| **知识容量** | 10KB（约1000条） |
| **响应时间** | <1ms |

**输入**：
```
InputSchema:
  - operation: "query" | "update" | "validate"
  - component: string (可选，如"kafka")
  - relation: string (可选，如"interact")
  - params: dict (可选，查询参数)
```

**输出**：
```
OutputSchema:
  - status: "success" | "error"
  - data: dict (查询结果)
    - nodes: list[ComponentNode]
    - edges: list[InteractionEdge]
    - properties: dict (节点属性)
  - metadata: dict
    - query_time: float
    - confidence: float (0-1)
```

**核心知识**：
```
{
  "component_capabilities": {
    "kafka": {"role": "消息队列", "output": ["消息流"], "input": ["任意数据"]},
    "spark": {"role": "批处理", "output": ["DataFrame"], "input": ["消息流", "文件"]},
    ...
  },
  "interaction_rules": {
    "format_match": {"消息流": ["消息流", "DataFrame"]},
    "protocol_map": {"kafka-spark": "Kafka Consumer API"}
  },
  "api_signatures": {
    "kafka.Producer.send": {"output_format": "消息"},
    "spark.read.kafka": {"input_format": "消息流"}
  }
}
```

---

### Skill 2: InteractionIdentifierSkill（交互关系识别）

**职责**：自动识别组件间的交互关系

| 属性 | 值 |
|------|---|
| **Skill名称** | InteractionIdentifierSkill |
| **类型** | 核心skill（内嵌） |
| **处理能力** | 15组件→105组合→过滤有效组合 |
| **响应时间** | 10-50ms |

**输入**：
```
InputSchema:
  - components: list[string] (如["kafka", "spark", "flink", ...])
  - knowledge_graph: KnowledgeGraphSkill.output (来自上游skill)
  - options: dict
    - max_combinations: int (最大组合数，默认=10)
    - priority_filter: list["P0", "P1", "P2"] (优先级过滤)
```

**输出**：
```
OutputSchema:
  - status: "success"
  - valid_interactions: list[dict]
    [
      {
        "combination": ("kafka", "spark"),
        "can_interact": true,
        "reason": "消息流格式匹配",
        "priority": "P0",
        "confidence": 1.0
      },
      ...
    ]
  - statistics: dict
    - total_combinations: int (105)
    - valid_interactions: int (实际有效数)
    - coverage_rate: float
  - three_component_chains: list[dict] (3组件协同链路)
```

**处理逻辑**：
```
1. 生成所有组合（C(n,2)）
2. 查询知识图谱（能力匹配）
3. 过滤有效交互（can_interact=true）
4. 计算优先级（基于使用频率）
5. 识别链路（A→B→C）
```

---

### Skill 3: APIMatcherSkill（交互API匹配）

**职责**：精确匹配交互时使用的API

| 属性 | 值 |
|------|---|
| **Skill名称** | APIMatcherSkill |
| **类型** | 核心skill（内嵌） |
| **匹配规则** | 数据格式匹配 + 参数兼容性 |
| **响应时间** | 5-20ms |

**输入**：
```
InputSchema:
  - interactions: InteractionIdentifierSkill.output.valid_interactions
  - knowledge_graph: KnowledgeGraphSkill.output
  - match_rules: dict (可选，自定义匹配规则)
```

**输出**：
```
OutputSchema:
  - status: "success"
  - api_mappings: list[dict]
    [
      {
        "combination": ("kafka", "spark"),
        "output_api": {
          "component": "kafka",
          "apis": ["Producer.send()", "Producer.flush()"]
        },
        "input_api": {
          "component": "spark",
          "apis": ["read().kafka()", "readStream()"]
        },
        "match_type": "双向交互",
        "confidence": 1.0,
        "data_flow": "Kafka.send → Spark.read.kafka"
      },
      ...
    ]
  - validation_results: list[dict]
    - compatible: bool
    - format_match: bool
    - param_match: bool
```

**匹配算法**：
```
1. 提取输出API（从知识图谱）
2. 提取输入API（从知识图谱）
3. 数据格式匹配（消息→消息流）
4. 参数兼容性检查（bootstrap.servers一致）
5. 返回匹配结果
```

---

### Skill 4: ScenarioGeneratorSkill（场景生成）

**职责**：生成完整的测试场景文档

| 属性 | 值 |
|------|---|
| **Skill名称** | ScenarioGeneratorSkill |
| **类型** | 核心skill（内嵌）+ 扩展skill（外部案例） |
| **生成能力** | 代码示例 + API映射表 + 配置依赖 |
| **响应时间** | 100-500ms（含外部调用） |

**输入**：
```
InputSchema:
  - api_mappings: APIMatcherSkill.output.api_mappings
  - priorities: list["P0", "P1"] (生成哪些优先级的场景)
  - knowledge_graph: KnowledgeGraphSkill.output
  - external_examples: ExternalServiceSkill.output (可选)
  - user_requirements: dict (可选，用户自定义需求)
```

**输出**：
```
OutputSchema:
  - status: "success"
  - scenarios: list[dict]
    [
      {
        "id": "S1",
        "components": ["kafka", "spark"],
        "priority": "P0",
        "data_flow": "Kafka → Spark",
        "architecture_diagram": "ASCII图",
        "code_example": "完整代码（50行）",
        "api_mapping_table": "Markdown表格",
        "config_dependencies": "配置参数列表",
        "exception_handling": "异常处理矩阵",
        "best_practices": "最佳实践建议"
      },
      ...
    ]
  - coverage_report: dict
    - component_coverage: float
    - combination_coverage: float
    - api_coverage: float
```

**生成逻辑**：
```
1. 按优先级筛选场景
2. 生成数据流图（ASCII）
3. 查询外部案例（GitHub）
4. 生成完整代码示例
5. 创建API映射表
6. 补充配置依赖
7. 计算覆盖率
```

---

### Skill 5: DocumentationSkill（文档输出）

**职责**：格式化输出最终文档

| 属性 | 值 |
|------|---|
| **Skill名称** | DocumentationSkill |
| **类型** | 核心skill（内嵌） |
| **输出格式** | Markdown / JSON / HTML |
| **响应时间** | 10-50ms |

**输入**：
```
InputSchema:
  - scenarios: ScenarioGeneratorSkill.output.scenarios
  - format: "markdown" | "json" | "html"
  - template: string (可选，自定义模板)
  - metadata: dict
    - title: string
    - version: string
    - coverage: dict
```

**输出**：
```
OutputSchema:
  - status: "success"
  - document: string (完整文档内容)
  - file_path: string (保存路径，如"hyx/output.md")
  - format: string (输出格式)
  - statistics: dict
    - line_count: int
    - scenario_count: int
    - code_line_count: int
```

**文档结构**：
```
# 多组件交互场景文档

## 1. 场景总览（表格）
## 2. 场景S1: Kafka → Spark
  - 架构图
  - 完整示例
  - API映射表
  - 配置依赖
## 3. 场景S2: ...
## N. 覆盖率报告
## 附录：单组件API列表
```

---

## 3. 扩展Skill（外部服务）

### Skill 6: ExternalKnowledgeServiceSkill（外部知识服务）

**职责**：提供扩展知识（案例库、配置库）

| 属性 | 值 |
|------|---|
| **Skill名称** | ExternalKnowledgeServiceSkill |
| **类型** | 扩展skill（外部API） |
| **知识容量** | 1GB+ |
| **响应时间** | 100-500ms |

**输入**：
```
InputSchema:
  - query_type: "examples" | "configs" | "versions"
  - keywords: list[string]
  - filters: dict
```

**输出**：
```
OutputSchema:
  - status: "success"
  - results: list[dict]
    - source: "github" | "document" | "expert"
    - content: dict
    - relevance: float
```

---

## 4. Skill关联关系（流水线架构）

### 4.1 主流水线（链式调用）

```
用户请求 → [Skill流水线] → 最终文档

流水线顺序：
  1. KnowledgeGraphSkill (初始化知识图谱)
  2. InteractionIdentifierSkill (识别交互关系)
  3. APIMatcherSkill (匹配交互API)
  4. ScenarioGeneratorSkill (生成场景)
  5. DocumentationSkill (输出文档)
```

### 4.2 数据流向图

```
┌─────────────────┐
│  User Request   │
│ "生成Kafka→Spark │
│  交互场景"       │
└─────────────────┘
        │
        ▼
┌─────────────────────────────────────────────────────────┐
│               Skill Pipeline                             │
├─────────────────────────────────────────────────────────┤
│                                                           │
│  ┌──────────────┐                                        │
│  │ Skill 1      │ Input: components=["kafka","spark"]   │
│  │ KnowledgeGra │ Output: KG={nodes, edges, rules}      │
│  │ phSkill      │                                        │
│  └─────┬────────┘                                        │
│        │ output: KG                                      │
│        ▼                                                  │
│  ┌──────────────┐                                        │
│  │ Skill 2      │ Input: components + KG                │
│  │ InteractionI │ Output: valid_interactions=[...]      │
│  │ dentifier    │                                        │
│  └─────┬────────┘                                        │
│        │ output: interactions                            │
│        ▼                                                  │
│  ┌──────────────┐                                        │
│  │ Skill 3      │ Input: interactions + KG              │
│  │ APIMatcher   │ Output: api_mappings=[...]            │
│  └─────┬────────┘                                        │
│        │ output: api_mappings                            │
│        ▼                                                  │
│  ┌──────────────┐    ┌────────────────┐                 │
│  │ Skill 4      │←──→│ ExternalSkill  │                 │
│  │ ScenarioGene │    │ (案例库)       │                 │
│  │ rator        │    └────────────────┘                 │
│  └─────┬────────┘                                        │
│        │ output: scenarios                               │
│        ▼                                                  │
│  ┌──────────────┐                                        │
│  │ Skill 5      │ Input: scenarios                      │
│  │ Documentatio │ Output: markdown_doc                  │
│  │ nSkill       │                                        │
│  └─────┬────────┘                                        │
│        │                                                  │
└────────┼─────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────┐
│  Final Document │
│  hyx/output.md  │
└─────────────────┘
```

---

### 4.3 Skill依赖关系矩阵

| Skill | 依赖上游Skill | 被下游Skill依赖 | 外部依赖 |
|-------|--------------|----------------|---------|
| **KnowledgeGraphSkill** | 无（初始化） | Skill2, Skill3, Skill4 | 无 |
| **InteractionIdentifierSkill** | Skill1 | Skill3 | 无 |
| **APIMatcherSkill** | Skill1, Skill2 | Skill4 | 无 |
| **ScenarioGeneratorSkill** | Skill1, Skill3 | Skill5 | ExternalSkill |
| **DocumentationSkill** | Skill4 | 无（终端） | 无 |
| **ExternalKnowledgeServiceSkill** | 无 | Skill4 | GitHub API |

---

### 4.4 并行处理（性能优化）

```
可并行化的Skill：

┌──────────────┐
│ Skill 1      │ (必须串行，初始化KG)
│ KnowledgeGra │
│ phSkill      │
└─────┬────────┘
      │
      ├──────────────────┬──────────────────┐
      │                  │                  │
      ▼                  ▼                  ▼
┌──────────┐      ┌──────────┐      ┌──────────┐
│ Skill 2  │      │ Skill 3  │      │External  │
│Identify  │      │APIMatch  │      │Knowledge │
│Interact  │      │(基础匹配)│      │Service   │
└─────┬────┘      └─────┬────┘      └─────┬────┘
      │                  │                  │
      └──────────────────┴──────────────────┘
                         │
                         ▼
                  ┌──────────┐
                  │ Skill 4  │
                  │Scenario  │
                  │Generator │
                  └─────┬────┘
                        │
                        ▼
                  ┌──────────┐
                  │ Skill 5  │
                  │Document  │
                  │ation     │
                  └──────────┘

并行优化：
  - Skill2 + Skill3 + ExternalSkill 可并行执行
  - 总时间从600ms降低到300ms
```

---

## 5. Skill通信协议

### 5.1 输入输出格式（标准化）

```json
{
  "skill_protocol": {
    "version": "1.0",
    "input": {
      "schema": "InputSchema",
      "validation": "strict",
      "encoding": "json"
    },
    "output": {
      "schema": "OutputSchema",
      "status": "success | error",
      "metadata": {
        "skill_name": "string",
        "execution_time": "float",
        "confidence": "float"
      }
    },
    "error_handling": {
      "on_error": "return_error_object",
      "error_schema": {
        "status": "error",
        "error_code": "string",
        "error_message": "string",
        "fallback": "optional_output"
      }
    }
  }
}
```

### 5.2 Skill调用示例

```python
# 调用Skill流水线
def execute_skill_pipeline(user_request):
    # Step 1: 初始化知识图谱
    kg_result = KnowledgeGraphSkill.execute({
        "operation": "query",
        "components": ["kafka", "spark", "flink", ...]
    })
    
    # Step 2: 识别交互关系（并行）
    interaction_result = InteractionIdentifierSkill.execute({
        "components": user_request["components"],
        "knowledge_graph": kg_result["data"]
    })
    
    # Step 3: 匹配API（并行）
    api_result = APIMatcherSkill.execute({
        "interactions": interaction_result["valid_interactions"],
        "knowledge_graph": kg_result["data"]
    })
    
    # Step 4: 生成场景
    scenario_result = ScenarioGeneratorSkill.execute({
        "api_mappings": api_result["api_mappings"],
        "knowledge_graph": kg_result["data"],
        "external_examples": ExternalSkill.execute({"query": "kafka-spark"})
    })
    
    # Step 5: 输出文档
    doc_result = DocumentationSkill.execute({
        "scenarios": scenario_result["scenarios"],
        "format": "markdown"
    })
    
    return doc_result
```

---

## 6. 知识图谱在Skill中的作用（核心架构）

### 6.1 知识图谱作为Skill共享状态

```
知识图谱生命周期：

┌─────────────────────────────────────────────┐
│          Knowledge Graph (共享状态)          │
├─────────────────────────────────────────────┤
│                                              │
│  初始化 → Skill1加载核心知识                  │
│                                              │
│  查询 → Skill2查询: can_interact(A,B)?       │
│        Skill3查询: output_api(A)?            │
│        Skill4查询: format_match规则          │
│                                              │
│  更新 → Skill4更新: 添加新案例（可选）         │
│                                              │
│  验证 → Skill5验证: 场景完整性检查            │
│                                              │
└─────────────────────────────────────────────┘

关键作用：
  1. 状态共享：所有Skill共享同一知识图谱
  2. 推理引擎：支持Skill的自动推理
  3. 准确性保证：基于知识图谱的事实推理
  4. 一致性：所有Skill基于同一知识源
```

### 6.2 知识图谱查询模式

```python
# Skill查询知识图谱的典型模式

class KnowledgeGraphQueryPatterns:
    
    # Pattern 1: 能力查询（Skill2使用）
    def query_capability(component):
        """
        查询组件能力
        KG.query("kafka.output_format")
        → 返回: ["消息流"]
        """
        return KG.get_node_property(component, "output_format")
    
    # Pattern 2: 关系查询（Skill2使用）
    def query_relation(comp_a, comp_b):
        """
        查询组件关系
        KG.query_edge("kafka", "spark", "interact")
        → 返回: {"method": "Kafka Consumer", "valid": true}
        """
        return KG.get_edge(comp_a, comp_b, "interact")
    
    # Pattern 3: API查询（Skill3使用）
    def query_api(component, api_type):
        """
        查询组件API
        KG.query("kafka.output_apis")
        → 返回: ["Producer.send()", "Producer.flush()"]
        """
        return KG.get_node_property(component, f"{api_type}_apis")
    
    # Pattern 4: 规则查询（Skill3使用）
    def query_match_rule(format_a, format_b):
        """
        查询匹配规则
        KG.query_rule("format_match", "消息", "消息流")
        → 返回: true
        """
        return KG.get_rule("format_match", format_a, format_b)
    
    # Pattern 5: 推理查询（Skill4使用）
    def infer_chain(comp_a, comp_b, comp_c):
        """
        推理链路
        KG.infer("kafka→spark→hive")
        → 推理: 消息流→DataFrame→Hive表
        → 返回: {"valid": true, "data_formats": [...]}
        """
        return KG.chain_inference([comp_a, comp_b, comp_c])
```

---

## 7. 错误处理与回退机制

### 7.1 Skill错误处理

```python
# Skill错误处理策略

class SkillErrorHandler:
    
    def handle_skill_error(skill_name, error):
        """
        错误处理策略矩阵
        """
        strategies = {
            'KnowledgeGraphSkill': {
                'fallback': '返回默认知识图谱',
                'retry': False,
                'impact': 'critical'
            },
            'InteractionIdentifierSkill': {
                'fallback': '返回空交互列表',
                'retry': True,
                'impact': 'high'
            },
            'APIMatcherSkill': {
                'fallback': '返回基础API匹配',
                'retry': True,
                'impact': 'medium'
            },
            'ScenarioGeneratorSkill': {
                'fallback': '返回模板场景',
                'retry': True,
                'impact': 'medium'
            },
            'DocumentationSkill': {
                'fallback': '返回简化文档',
                'retry': False,
                'impact': 'low'
            },
            'ExternalKnowledgeServiceSkill': {
                'fallback': '跳过外部知识',
                'retry': True,
                'impact': 'low'
            }
        }
        
        strategy = strategies[skill_name]
        
        return {
            'action': strategy['fallback'],
            'retry': strategy['retry'],
            'impact_level': strategy['impact']
        }
```

### 7.2 回退示例

```python
# ExternalSkill失败时的回退

def execute_with_fallback():
    try:
        # 尝试调用外部知识服务
        examples = ExternalSkill.execute({"query": "kafka-spark"})
    except Exception as e:
        # 回退：使用内嵌模板
        examples = {
            "source": "fallback_template",
            "content": DEFAULT_EXAMPLE_TEMPLATE,
            "confidence": 0.7  # 降低置信度
        }
    
    return examples
```

---

## 8. 性能指标与监控

### 8.1 Skill性能指标

| Skill | 平均执行时间 | 内存占用 | CPU使用率 |
|-------|------------|---------|----------|
| KnowledgeGraphSkill | 1ms | 10KB | 1% |
| InteractionIdentifierSkill | 10-50ms | 50KB | 5% |
| APIMatcherSkill | 5-20ms | 30KB | 3% |
| ScenarioGeneratorSkill | 100-500ms | 200KB | 15% |
| DocumentationSkill | 10-50ms | 50KB | 5% |
| ExternalKnowledgeServiceSkill | 100-500ms | 0 | 0% (外部) |

### 8.2 流水线总性能

```
串行模式：
  总时间 = 1 + 50 + 20 + 500 + 50 = 621ms

并行模式：
  总时间 = 1 + max(50, 20, 500) + 50 = 551ms

优化后：
  总时间 ≈ 300-500ms（可接受）
```

---

## 9. 扩展性设计

### 9.1 添加新组件

```python
# 添加新组件的流程

def add_new_component(component_name, component_info):
    """
    添加新组件只需更新知识图谱
    
    步骤：
      1. 更新KnowledgeGraphSkill的知识库
      2. 其他Skill自动识别新交互
      3. 无需修改Skill代码
    """
    # Step 1: 添加到知识图谱
    KG.update({
        "operation": "add_node",
        "node": {
            "name": component_name,
            "type": "component",
            "properties": {
                "role": component_info["role"],
                "output_format": component_info["output"],
                "input_format": component_info["input"]
            }
        }
    })
    
    # Step 2: Skill自动识别（无需修改）
    # InteractionIdentifierSkill会自动识别新交互
    # APIMatcherSkill会自动匹配新API
    
    return {"status": "success", "auto_identified": True}
```

### 9.2 添加新Skill

```python
# 添加新Skill的流程

def add_new_skill(skill_name, skill_definition):
    """
    添加新Skill的扩展机制
    
    要求：
      1. 遵循Skill协议（输入输出格式）
      2. 声明依赖关系
      3. 注册到流水线
    """
    # Step 1: 定义Skill
    new_skill = {
        "name": skill_name,
        "input_schema": skill_definition["input"],
        "output_schema": skill_definition["output"],
        "dependencies": skill_definition["dependencies"],
        "implementation": skill_definition["code"]
    }
    
    # Step 2: 注册到流水线
    Pipeline.register_skill(new_skill)
    
    # Step 3: 更新流水线图
    Pipeline.update_flow_graph()
    
    return {"status": "registered", "skill": new_skill}
```

---

## 10. 最终架构总结

### 核心设计原则

1. **知识图谱为核心** - 所有Skill基于知识图谱推理
2. **链式流水线** - Skill通过输入输出串联
3. **标准化协议** - 统一的输入输出格式
4. **错误回退** - 每个Skill有fallback机制
5. **可扩展性** - 添加组件/Skill无需重构

### 关键创新点

1. **知识驱动推理** - 传统依赖大模型记忆 → 知识图谱推理
2. **Skill协作模式** - 单体Skill → 流水线协作
3. **混合知识架构** - 内嵌核心知识 + 外部扩展知识
4. **自动识别** - 人工判断 → 自动推理交互关系

---

## 附录：Skill快速参考

| Skill | 输入 | 输出 | 核心功能 |
|-------|------|------|---------|
| KnowledgeGraphSkill | components | KG | 知识管理 |
| InteractionIdentifierSkill | KG + components | interactions | 交互识别 |
| APIMatcherSkill | interactions + KG | api_mappings | API匹配 |
| ScenarioGeneratorSkill | api_mappings | scenarios | 场景生成 |
| DocumentationSkill | scenarios | document | 文档输出 |
| ExternalKnowledgeServiceSkill | query | examples | 外部知识 |

---

**文档版本**: 1.0  
**生成时间**: 2025-05-12  
**作者**: MultiComponentSystem架构设计
