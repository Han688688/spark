# Skill流程图（Mermaid格式）

## 图1: Skill流水线流程图

```mermaid
graph TD
    A[用户请求: 生成Kafka→Spark交互场景] --> B[Skill1: KnowledgeGraphSkill]
    
    B --> C[Skill2: InteractionIdentifierSkill]
    B --> D[Skill3: APIMatcherSkill]
    B --> E[ExternalSkill: GitHub案例库]
    
    C --> F[数据: valid_interactions]
    D --> G[数据: api_mappings]
    E --> H[数据: external_examples]
    
    F --> I[Skill4: ScenarioGeneratorSkill]
    G --> I
    H --> I
    B --> I
    
    I --> J[数据: scenarios]
    
    J --> K[Skill5: DocumentationSkill]
    
    K --> L[最终文档: hyx/output.md]
    
    style B fill:#f9f,stroke:#333,stroke-width:4px
    style C fill:#bbf,stroke:#333
    style D fill:#bbf,stroke:#333
    style E fill:#bfb,stroke:#333
    style I fill:#fbb,stroke:#333,stroke-width:4px
    style K fill:#bfb,stroke:#333
```

---

## 图2: Skill输入输出关系图

```mermaid
graph LR
    subgraph Skill1[Skill 1: KnowledgeGraphSkill]
        I1[Input: operation, component]
        O1[Output: KG={nodes,edges,rules}]
    end
    
    subgraph Skill2[Skill 2: InteractionIdentifierSkill]
        I2[Input: components + KG]
        O2[Output: valid_interactions]
    end
    
    subgraph Skill3[Skill 3: APIMatcherSkill]
        I3[Input: interactions + KG]
        O3[Output: api_mappings]
    end
    
    subgraph Skill4[Skill 4: ScenarioGeneratorSkill]
        I4[Input: api_mappings + KG + external]
        O4[Output: scenarios]
    end
    
    subgraph Skill5[Skill 5: DocumentationSkill]
        I5[Input: scenarios]
        O5[Output: document]
    end
    
    O1 --> I2
    O1 --> I3
    O2 --> I3
    O1 --> I4
    O3 --> I4
    O4 --> I5
    
    style Skill1 fill:#f9f
    style Skill2 fill:#bbf
    style Skill3 fill:#bbf
    style Skill4 fill:#fbb
    style Skill5 fill:#bfb
```

---

## 图3: 知识图谱结构图

```mermaid
graph TD
    KG[Knowledge Graph] --> Nodes[Nodes: 组件节点]
    KG --> Edges[Edges: 交互关系]
    KG --> Rules[Rules: 匹配规则]
    
    Nodes --> N1[kafka节点]
    Nodes --> N2[spark节点]
    Nodes --> N3[flink节点]
    
    N1 --> P1[properties: role, output, input, apis]
    N2 --> P2[properties: role, output, input, apis]
    N3 --> P3[properties: role, output, input, apis]
    
    Edges --> E1[kafka→spark边]
    Edges --> E2[kafka→flink边]
    
    E1 --> EP1[properties: valid, method, priority]
    E2 --> EP2[properties: valid, method, priority]
    
    Rules --> R1[format_match规则]
    Rules --> R2[param_compatible规则]
    
    R1 --> RC1[条件: 消息 matches 消息流]
    R2 --> RC2[条件: bootstrap.servers一致]
    
    style KG fill:#f9f,stroke:#333,stroke-width:4px
    style Nodes fill:#bbf
    style Edges fill:#bbf
    style Rules fill:#bfb
```

---

## 图4: Skill依赖关系图

```mermaid
graph TD
    S1[Skill1: KnowledgeGraphSkill] --> S2[Skill2: InteractionIdentifierSkill]
    S1 --> S3[Skill3: APIMatcherSkill]
    
    S2 --> S3
    
    S1 --> S4[Skill4: ScenarioGeneratorSkill]
    S3 --> S4
    
    Ext[ExternalSkill] --> S4
    
    S4 --> S5[Skill5: DocumentationSkill]
    
    S5 --> Doc[最终文档]
    
    style S1 fill:#f9f,stroke:#333,stroke-width:4px
    style S2 fill:#bbf
    style S3 fill:#bbf
    style S4 fill:#fbb,stroke:#333,stroke-width:4px
    style S5 fill:#bfb
    style Ext fill:#bfb,stroke:#999
```

---

## 图5: 数据流全链路图

```mermaid
graph TD
    Input[用户输入: components] --> Skill1
    
    subgraph Skill1[Skill 1]
        KG[KnowledgeGraphSkill]
        KG --> D1[数据1: KG]
    end
    
    subgraph Skill2[Skill 2]
        ID[InteractionIdentifierSkill]
        D1 --> ID
        ID --> D2[数据2: valid_interactions]
    end
    
    subgraph Skill3[Skill 3]
        AM[APIMatcherSkill]
        D1 --> AM
        D2 --> AM
        AM --> D3[数据3: api_mappings]
    end
    
    subgraph Skill4[Skill 4]
        SG[ScenarioGeneratorSkill]
        D1 --> SG
        D3 --> SG
        Ext[External: GitHub案例]
        Ext --> SG
        SG --> D4[数据4: scenarios]
    end
    
    subgraph Skill5[Skill 5]
        Doc[DocumentationSkill]
        D4 --> Doc
        Doc --> Output[数据5: document]
    end
    
    Output --> File[文件: hyx/output.md]
    
    style Skill1 fill:#f9f
    style Skill2 fill:#bbf
    style Skill3 fill:#bbf
    style Skill4 fill:#fbb
    style Skill5 fill:#bfb
    style D1 fill:#ff9
    style D2 fill:#ff9
    style D3 fill:#ff9
    style D4 fill:#ff9
    style Output fill:#ff9
```

---

## 图6: 并行执行流程图

```mermaid
graph TD
    A[用户请求] --> B[Skill1: 初始化KG]
    
    B --> C{并行执行}
    
    C --> D[Skill2: 识别交互]
    C --> E[Skill3: 基础API匹配]
    C --> F[External: 查询案例]
    
    D --> G[等待并行完成]
    E --> G
    F --> G
    
    G --> H[Skill4: 生成场景]
    
    H --> I[Skill5: 输出文档]
    
    I --> J[最终文档]
    
    style B fill:#f9f,stroke:#333,stroke-width:4px
    style D fill:#bbf
    style E fill:#bbf
    style F fill:#bfb
    style G fill:#ff9,stroke:#333
    style H fill:#fbb,stroke:#333,stroke-width:4px
    
    linkStyle 3,4,5 stroke:#f00,stroke-width:2px
```

---

## 图7: 错误处理流程图

```mermaid
graph TD
    A[Skill执行] --> B{检查status}
    
    B -->|success| C[继续流水线]
    
    B -->|error| D{查询错误级别}
    
    D -->|Critical| E[返回错误<br/>流水线终止]
    
    D -->|High/Medium| F[重试执行<br/>最多3次]
    
    F -->|重试成功| C
    F -->|重试失败| G[回退处理]
    
    D -->|Low| G
    
    G --> H[返回fallback数据]
    H --> I[降低confidence]
    I --> C
    
    C --> J[继续下一个Skill]
    
    style A fill:#bbf
    style B fill:#ff9,stroke:#333
    style D fill:#ff9,stroke:#333
    style E fill:#f99,stroke:#333,stroke-width:4px
    style F fill:#9ff
    style G fill:#9f9
    style H fill:#9f9
```

---

## 图8: Skill查询知识图谱流程

```mermaid
sequenceDiagram
    participant S2 as Skill2
    participant S3 as Skill3
    participant S4 as Skill4
    participant KG as KnowledgeGraph
    participant Ext as ExternalSkill
    
    S2->>KG: query_edge(kafka, spark, interact)
    KG-->>S2: {valid: true, method: Kafka Consumer}
    
    S3->>KG: query_node(kafka, output_apis)
    KG-->>S3: [Producer.send(), Producer.flush()]
    
    S3->>KG: apply_rule(format_match, 消息, 消息流)
    KG-->>S3: true
    
    S4->>KG: chain_inference(kafka→spark→hive)
    KG-->>S4: {valid: true, formats: [消息, DataFrame, Hive表]}
    
    S4->>Ext: query(kafka-spark-hive examples)
    Ext-->>S4: [{source: github, content: 代码示例}]
    
    Note over KG: 知识图谱支持5种查询模式<br/>1. 能力查询<br/>2. 关系查询<br/>3. API查询<br/>4. 规则查询<br/>5. 推理查询
```

---

## 图9: Skill生命周期

```mermaid
stateDiagram-v2
    [*] --> 初始化
    初始化 --> 知识加载: Skill1执行
    知识加载 --> 交互识别: Skill2执行
    交互识别 --> API匹配: Skill3执行
    API匹配 --> 场景生成: Skill4执行
    场景生成 --> 文档输出: Skill5执行
    文档输出 --> [*]
    
    交互识别 --> 错误处理: Skill2失败
    API匹配 --> 错误处理: Skill3失败
    场景生成 --> 错误处理: Skill4失败
    
    错误处理 --> 重试: 非Critical
    重试 --> 交互识别: 成功
    重试 --> 回退: 失败
    
    回退 --> API匹配: 继续流水线
    
    错误处理 --> [*]: Critical错误
```

---

## 图10: 组件交互识别流程

```mermaid
graph TD
    A[15个组件] --> B[生成所有组合<br/>C15,2=105组合]
    
    B --> C{知识图谱查询}
    
    C --> D[查询组件A.output_format]
    C --> E[查询组件B.input_format]
    
    D --> F{格式匹配判断}
    E --> F
    
    F -->|匹配| G[标记: can_interact=true]
    F -->|不匹配| H[标记: can_interact=false]
    
    G --> I[添加到valid_interactions]
    H --> J[过滤掉]
    
    I --> K[计算优先级<br/>基于使用频率]
    
    K --> L[返回有效交互列表]
    
    style A fill:#bbf
    style B fill:#ff9
    style C fill:#f9f,stroke:#333
    style F fill:#ff9,stroke:#333
    style G fill:#9f9
    style I fill:#9f9
```

---

## 使用说明

### 在Markdown中渲染Mermaid图

1. **GitHub**: 直接支持Mermaid语法，复制粘贴即可渲染
2. **VS Code**: 安装Markdown Preview Mermaid Support插件
3. **其他平台**: 使用Mermaid Live Editor: https://mermaid.live/

### 在线渲染

将上述Mermaid代码复制到：
- https://mermaid.live/
- https://mermaid.ink/

---

**文档版本**: 1.0  
**生成时间**: 2025-05-12  
**格式**: Mermaid (支持在线渲染)
