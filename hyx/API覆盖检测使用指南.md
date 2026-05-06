# API覆盖检测使用指南

## 一、功能概述

API覆盖检测模块用于：
1. **解析API清单** - 从已有markdown文档加载组件API定义
2. **扫描测试代码** - 提取项目已测试的API
3. **对比缺失API** - 发现未覆盖的API
4. **生成测试代码** - 为缺失API自动生成测试用例

## 二、快速使用

### 2.1 运行Demo（推荐首次使用）

```bash
cd /home/h00517772/spark/hyx
python register_api_plugin.py
```

选择模式：
- 1: Demo演示（交互式，了解功能）
- 2: 快速检查（仅分析）
- 3: 完整流程（分析+生成）
- 4: 注册到框架

### 2.2 运行快速检查

```bash
cd /home/h00517772/spark/hyx

# 方式1: 直接运行
python api_coverage_plugin.py

# 方式2: 使用注册脚本
python register_api_plugin.py
# 选择模式2
```

输出：
```
分析结果:
总API数: 646
已测试API数: 420
缺失API数: 226
覆盖率: 65.02%

按组件统计:
spark        : 45/69 (缺失24) - 65.22%
kafka        : 380/577 (缺失197) - 65.86%

按稳定性统计:
Stable       : 89/100 (缺失11) - 89.00%
Evolving     : 20/25 (缺失5) - 80.00%
```

### 2.3 运行完整流程

```bash
# 使用框架运行
cd /home/h00517772/spark/hyx
python start.py api_coverage
```

## 三、配置说明

### 3.1 API清单文件配置

在 `framework.yaml` 中配置：

```yaml
data_sources:
  api_inventory_files:
    - spark_java_api_complete_list.md
    - kafka_java_api_complete_list.md
    - hbase_java_api_complete_list.md
    - hadoop_java_api_complete_list.md
    - iceberg_java_api_complete_list.md
```

### 3.2 工作流配置

```yaml
workflows:
  api_coverage:
    name: "API覆盖度检测"
    steps:
      - name: analyze_api_coverage
        type: analyze
        plugin: api_coverage_analyzer
      
      - name: discover_missing_api
        type: discover
        strategy_category: api_coverage
      
      - name: generate_p0_api_tests
        type: generate
        plugin: api_test_generator
        priority: P0
        limit: 50
```

### 3.3 输出目录配置

```yaml
generator_config:
  output_dir: hyx/generated_tests/api_coverage
```

## 四、输出结果

### 4.1 分析报告

**JSON报告**: `hyx/results/api_coverage_analysis.json`

```json
{
  "total_apis": 646,
  "tested_apis_count": 420,
  "missing_apis_count": 226,
  "coverage_rate": 0.6502,
  "coverage_by_component": {
    "spark": {"total": 69, "tested": 45, "missing": 24, "rate": 0.6522}
  },
  "coverage_by_stability": {
    "Stable": {"total": 100, "tested": 89, "missing": 11, "rate": 0.89}
  },
  "missing_apis": [
    {
      "component": "spark",
      "class_name": "Dataset",
      "method_name": "flatMap",
      "stability": "Stable"
    }
  ]
}
```

**Markdown报告**: `hyx/results/API_Coverage_Report.md`

内容：
- 总体统计表格
- 按组件统计表格
- 按稳定性统计表格
- 缺失API列表（按优先级排序）

### 4.2 测试场景

**场景文件**: `hyx/results/api_coverage_scenarios.json`

每个场景包含：
- 场景ID
- 场景名称
- 优先级（P0/P1/P2/P3）
- 测试步骤
- API定义详情

### 4.3 测试代码

**测试文件**: `hyx/generated_tests/api_coverage/*.java`

每个测试文件包含：
- 正常调用测试
- null参数测试
- 边界值测试
- 异常处理测试

## 五、优先级说明

| 稳定性标注 | 测试优先级 | 说明 |
|-----------|-----------|------|
| @Stable | P0 | 核心稳定API，必须测试 |
| @Evolving | P1 | 演进API，优先测试 |
| Unknown | P2 | 稳定性未知，次优先 |
| @Private | P3 | 内部API，可选测试 |

建议：优先生成P0和P1测试。

## 六、常见问题

### Q1: API清单文档格式不统一怎么办？

A: 插件支持多种解析策略：
- 表格格式（`| Class | Method |`）
- 列表格式（`- ClassName.methodName()`）
- 简单类名列表

会自动尝试不同策略。

### Q2: 如何判断API是否被测试？

A: 混合判断：
- 类级别：只要测试了类中任一方法
- 方法级别：检查方法名是否在测试文件中出现

暂不检查：
- 参数组合覆盖
- 重载方法区分

### Q3: 生成的测试代码能直接使用吗？

A: 需要补充：
1. 实例创建逻辑（setUp方法）
2. 测试数据准备（prepareTestData方法）
3. 结果验证逻辑（verifyResult方法）

插件生成的是模板代码，需人工补充具体实现。

### Q4: 如何扩展新组件API？

A: 添加步骤：
1. 创建API清单文档（如`flink_java_api_complete_list.md`）
2. 更新 `framework.yaml` 添加到 `api_inventory_files`
3. 运行分析即可自动识别

## 七、进阶用法

### 7.1 自定义API解析规则

如果现有解析器无法识别你的文档格式，可以扩展：

```python
# 在 api_coverage_plugin.py 中添加新的解析方法

class APIInventoryParser:
    def _parse_custom_format(self, content: str, component: str, source_file: str):
        # 你的自定义解析逻辑
        apis = []
        
        # 示例：解析自定义格式
        pattern = r'你的正则表达式'
        matches = re.findall(pattern, content)
        
        for match in matches:
            # 构造APIDefinition对象
            api = APIDefinition(...)
            apis.append(api)
        
        return apis
```

### 7.2 自定义测试代码模板

修改 `APITestGenerator._generate_junit_test` 方法：

```python
def _generate_junit_test(self, api_def: Dict, scenario: Scenario) -> str:
    # 使用你的自定义模板
    template = self._load_custom_template(api_def['component'])
    
    # 替换模板变量
    test_code = template.format(
        class_name=api_def['class_name'],
        method_name=api_def['method_name'],
        ...
    )
    
    return test_code
```

### 7.3 集成到CI/CD

```yaml
# .github/workflows/api_coverage.yml

name: API Coverage Check

on: [push, pull_request]

jobs:
  check:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.8'
      
      - name: Run API Coverage Check
        run: |
          cd spark/hyx
          python register_api_plugin.py --mode=check
      
      - name: Check Coverage Threshold
        run: |
          # 检查覆盖率是否达到阈值
          python scripts/check_coverage_threshold.py 70%
```

## 八、与场景测试的关系

### 双轨策略：

1. **API覆盖检测**
   - 目标：量化API覆盖度
   - 方法：清单对比
   - 产出：单元测试
   - 适用：单个API功能验证

2. **场景测试生成**
   - 目标：覆盖交互场景
   - 方法：调用链分析、日志挖掘
   - 产出：集成测试、场景测试
   - 适用：多组件协同验证

### 建议：

**第一步**: 运行API覆盖检测，补充基础单元测试
```bash
python start.py api_coverage
```

**第二步**: 运行场景测试生成，补充复杂交互测试
```bash
python start.py interaction_focus
```

**第三步**: 组合生成
```bash
python start.py full
```

## 九、总结

API覆盖检测的优势：
- ✓ 覆盖度可量化
- ✓ 自动化程度高
- ✓ 与现有框架无缝集成
- ✓ 支持多组件扩展

适用场景：
- 多三方件项目API测试补充
- 测试覆盖率度量
- 持续集成质量门禁

下一步：
1. 运行Demo了解功能
2. 查看分析报告确定缺失API
3. 选择优先级生成测试
4. 补充测试实现细节
5. 集成到项目测试库