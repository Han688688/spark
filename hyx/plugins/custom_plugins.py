#!/usr/bin/env python3
"""
自定义插件示例

这个文件展示了如何编写自定义插件来扩展框架能力。

使用方法：
1. 将自定义插件文件放到 hyx/plugins/ 目录
2. 框架会自动加载并注册插件
3. 在配置文件中引用插件
"""

from pathlib import Path
from typing import Dict, List, Any, Optional
import re
import json
import yaml
import logging

from framework import (
    PluginBase, AnalyzerPlugin, DiscovererStrategy, GeneratorPlugin,
    Interaction, Scenario
)

logger = logging.getLogger(__name__)


# ==================== 示例1：配置分析插件 ====================

class ConfigDependencyAnalyzer(AnalyzerPlugin):
    """
    配置依赖分析插件
    
    功能：分析配置文件，提取组件间的配置依赖关系
    
    适用场景：
    - 组件通过配置文件连接（如数据库配置、API地址等）
    - 配置变更影响多个组件
    """
    
    @property
    def name(self) -> str:
        return "config_dependency_analyzer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def analyze(self, project_root: Path, config: Dict) -> Dict:
        """分析配置依赖"""
        logger.info("执行配置依赖分析...")
        
        config_deps = []
        
        # 查找配置文件
        config_files = self._find_config_files(project_root, config)
        
        for conf_file in config_files:
            deps = self._parse_config_file(conf_file, project_root)
            config_deps.extend(deps)
        
        return {
            "config_dependencies": config_deps,
            "config_files": [str(f.relative_to(project_root)) for f in config_files]
        }
    
    def _find_config_files(self, project_root: Path, config: Dict) -> List[Path]:
        """查找配置文件"""
        config_patterns = config.get("config_patterns", [
            "*.yaml", "*.yml", "*.properties", "*.json", "*.conf"
        ])
        
        config_files = []
        
        for pattern in config_patterns:
            for conf_file in project_root.rglob(pattern):
                # 排除测试配置
                if 'test' in str(conf_file).lower():
                    continue
                config_files.append(conf_file)
        
        return config_files[:20]  # 最多20个
    
    def _parse_config_file(self, conf_file: Path, project_root: Path) -> List[Dict]:
        """解析配置文件"""
        deps = []
        
        ext = conf_file.suffix
        
        try:
            if ext in ['.yaml', '.yml']:
                content = yaml.safe_load(conf_file.read_text())
                deps = self._extract_yaml_deps(content, conf_file)
            
            elif ext == '.properties':
                content = conf_file.read_text()
                deps = self._extract_properties_deps(content, conf_file)
            
            elif ext == '.json':
                content = json.loads(conf_file.read_text())
                deps = self._extract_json_deps(content, conf_file)
        
        except Exception as e:
            logger.warning(f"解析配置文件失败 {conf_file}: {e}")
        
        return deps
    
    def _extract_yaml_deps(self, content: Dict, conf_file: Path) -> List[Dict]:
        """提取YAML配置中的依赖"""
        deps = []
        
        # 递归查找包含connection、url、host等关键字
        def find_connections(obj, path=""):
            if isinstance(obj, dict):
                for key, value in obj.items():
                    current_path = f"{path}.{key}" if path else key
                    
                    # 检查是否是连接配置
                    connection_keywords = [
                        'connection', 'url', 'host', 'endpoint', 
                        'datasource', 'database', 'api', 'server'
                    ]
                    
                    if any(kw in key.lower() for kw in connection_keywords):
                        deps.append({
                            "config_file": str(conf_file),
                            "config_key": current_path,
                            "config_value": str(value),
                            "dependency_type": "connection"
                        })
                    
                    find_connections(value, current_path)
            
            elif isinstance(obj, list):
                for idx, item in enumerate(obj):
                    find_connections(item, f"{path}[{idx}]")
        
        find_connections(content)
        
        return deps
    
    def _extract_properties_deps(self, content: str, conf_file: Path) -> List[Dict]:
        """提取Properties配置中的依赖"""
        deps = []
        
        connection_keywords = [
            'connection', 'url', 'host', 'endpoint',
            'datasource', 'database', 'api', 'server'
        ]
        
        for line in content.split('\n'):
            if '=' in line and not line.startswith('#'):
                key, value = line.split('=')
                key = key.strip()
                value = value.strip()
                
                if any(kw in key.lower() for kw in connection_keywords):
                    deps.append({
                        "config_file": str(conf_file),
                        "config_key": key,
                        "config_value": value,
                        "dependency_type": "connection"
                    })
        
        return deps
    
    def _extract_json_deps(self, content: Dict, conf_file: Path) -> List[Dict]:
        """提取JSON配置中的依赖"""
        # 类似YAML处理
        return self._extract_yaml_deps(content, conf_file)
    
    def execute(self, context: Dict) -> Dict:
        """执行插件"""
        project_root = Path(context["project_root"])
        config = context.get("analyzer_config", {})
        return self.analyze(project_root, config)


# ==================== 示例2：API文档分析插件 ====================

class ApiDocAnalyzer(AnalyzerPlugin):
    """
    API文档分析插件
    
    功能：从API文档（Swagger/OpenAPI、自定义文档）提取接口信息
    
    适用场景：
    - 有Swagger/OpenAPI文档
    - 有API设计文档
    - 需要基于API生成测试
    """
    
    @property
    def name(self) -> str:
        return "api_doc_analyzer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def analyze(self, project_root: Path, config: Dict) -> Dict:
        """分析API文档"""
        logger.info("执行API文档分析...")
        
        apis = []
        
        # 查找Swagger/OpenAPI文档
        swagger_files = list(project_root.rglob("swagger*.yaml")) + \
                        list(project_root.rglob("swagger*.json")) + \
                        list(project_root.rglob("openapi*.yaml")) + \
                        list(project_root.rglob("openapi*.json"))
        
        for swagger_file in swagger_files:
            apis.extend(self._parse_swagger(swagger_file))
        
        # 查找自定义API文档
        api_doc_patterns = config.get("api_doc_patterns", ["*api*.md", "*接口*.md"])
        
        for pattern in api_doc_patterns:
            for doc_file in project_root.rglob(pattern):
                apis.extend(self._parse_api_doc(doc_file))
        
        return {
            "api_definitions": apis,
            "swagger_files": [str(f.relative_to(project_root)) for f in swagger_files]
        }
    
    def _parse_swagger(self, swagger_file: Path) -> List[Dict]:
        """解析Swagger/OpenAPI文档"""
        apis = []
        
        try:
            if swagger_file.suffix in ['.yaml', '.yml']:
                content = yaml.safe_load(swagger_file.read_text())
            else:
                content = json.loads(swagger_file.read_text())
            
            # 提取paths
            paths = content.get('paths', {})
            
            for path, methods in paths.items():
                for method, details in methods.items():
                    apis.append({
                        "path": path,
                        "method": method.upper(),
                        "summary": details.get('summary', ''),
                        "parameters": details.get('parameters', []),
                        "responses": details.get('responses', {}),
                        "source": str(swagger_file)
                    })
        
        except Exception as e:
            logger.warning(f"解析Swagger失败 {swagger_file}: {e}")
        
        return apis
    
    def _parse_api_doc(self, doc_file: Path) -> List[Dict]:
        """解析自定义API文档"""
        apis = []
        
        try:
            content = doc_file.read_text()
            
            # 提取API定义（示例格式）
            # ## API: /api/v1/users
            # Method: GET
            # Description: 获取用户列表
            
            pattern = r'##\s*API:\s*(.*?)\nMethod:\s*(.*?)\n.*?Description:\s*(.*?)\n'
            
            for match in re.finditer(pattern, content, re.IGNORECASE):
                apis.append({
                    "path": match.group(1).strip(),
                    "method": match.group(2).strip().upper(),
                    "summary": match.group(3).strip(),
                    "source": str(doc_file)
                })
        
        except Exception as e:
            logger.warning(f"解析API文档失败 {doc_file}: {e}")
        
        return apis
    
    def execute(self, context: Dict) -> Dict:
        """执行插件"""
        project_root = Path(context["project_root"])
        config = context.get("analyzer_config", {})
        return self.analyze(project_root, config)


# ==================== 示例3：历史缺陷分析插件 ====================

class BugHistoryDiscoverer(DiscovererStrategy):
    """
    历史缺陷场景发现策略
    
    功能：从历史缺陷数据库/文件中提取场景
    
    适用场景：
    - 有缺陷管理系统（Jira、Bugzilla等）
    - 有历史缺陷记录
    - 需要基于历史问题生成回归测试
    """
    
    @property
    def name(self) -> str:
        return "bug_history_discoverer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def discover(self, context: Dict) -> List[Scenario]:
        """从历史缺陷发现场景"""
        logger.info("执行历史缺陷场景发现...")
        
        scenarios = []
        
        # 加载历史缺陷
        bugs = self._load_bugs(context)
        
        for idx, bug in enumerate(bugs):
            # 为每个缺陷生成回归测试场景
            scenario = self._create_regression_scenario(bug, idx)
            scenarios.append(scenario)
            
            # 为缺陷生成相关的扩展场景
            extended_scenarios = self._create_extended_scenarios(bug, idx)
            scenarios.extend(extended_scenarios)
        
        return scenarios
    
    def get_interactions(self, context: Dict) -> List[Interaction]:
        """从缺陷提取交互"""
        interactions = []
        
        bugs = self._load_bugs(context)
        
        for bug in bugs:
            # 如果缺陷涉及多个组件
            components = bug.get("affected_components", [])
            
            if len(components) >= 2:
                # 假设是交互问题
                interactions.append(Interaction(
                    source=components[0],
                    target=components[1],
                    interaction_type="call",
                    interface=bug.get("interface", "unknown"),
                    frequency=1,
                    critical_path=bug.get("severity") == "critical"
                ))
        
        return interactions
    
    def _load_bugs(self, context: Dict) -> List[Dict]:
        """加载历史缺陷"""
        bugs = []
        
        # 从配置的缺陷文件加载
        bug_files = context.get("bug_history_files", [])
        
        if not bug_files:
            # 自动查找
            project_root = Path(context["project_root"])
            bug_files = list(project_root.rglob("bugs*.json")) + \
                        list(project_root.rglob("defects*.json"))
        
        for bug_file in bug_files[:10]:  # 最多10个文件
            try:
                content = json.loads(bug_file.read_text())
                
                if isinstance(content, list):
                    bugs.extend(content)
                elif isinstance(content, dict) and 'bugs' in content:
                    bugs.extend(content['bugs'])
            except:
                pass
        
        # 去重
        seen = set()
        unique_bugs = []
        for bug in bugs:
            bug_id = bug.get("id", bug.get("key", ""))
            if bug_id not in seen:
                seen.add(bug_id)
                unique_bugs.append(bug)
        
        return unique_bugs[:50]  # 最多50个
    
    def _create_regression_scenario(self, bug: Dict, idx: int) -> Scenario:
        """创建回归测试场景"""
        bug_id = bug.get("id", bug.get("key", f"BUG{idx}"))
        bug_title = bug.get("title", bug.get("summary", ""))
        bug_severity = bug.get("severity", "major")
        
        priority_map = {
            "critical": "P0",
            "major": "P1",
            "minor": "P2",
            "trivial": "P3"
        }
        
        return Scenario(
            id=f"BUG{idx+1:03d}_REGRESSION",
            name=f"回归测试: {bug_title[:30]}",
            type="exception",
            priority=priority_map.get(bug_severity, "P2"),
            components=bug.get("affected_components", ["unknown"]),
            description=f"验证历史缺陷已修复: {bug_title}",
            test_steps=[
                "重现原始缺陷场景",
                "验证缺陷不再发生",
                "验证修复未引入新问题"
            ],
            expected_result="缺陷已修复，系统正常",
            discovery_strategy=self.name,
            metadata={"bug_id": bug_id, "bug_details": bug}
        )
    
    def _create_extended_scenarios(self, bug: Dict, idx: int) -> List[Scenario]:
        """创建扩展场景"""
        scenarios = []
        
        bug_id = bug.get("id", f"BUG{idx}")
        affected_components = bug.get("affected_components", [])
        
        # 如果涉及多个组件，创建组件交互场景
        if len(affected_components) >= 2:
            scenarios.append(Scenario(
                id=f"BUG{idx+1:03d}_INTERACTION",
                name=f"组件交互测试: {affected_components[0]}-{affected_components[1]}",
                type="integration",
                priority="P1",
                components=affected_components,
                description=f"基于历史缺陷验证组件交互",
                test_steps=[
                    f"测试{affected_components[0]}与{affected_components[1]}的交互",
                    "验证各种边界条件",
                    "验证异常处理"
                ],
                expected_result="组件交互正常，无异常",
                discovery_strategy=self.name
            ))
        
        return scenarios
    
    def execute(self, context: Dict) -> List[Scenario]:
        """执行"""
        return self.discover(context)


# ==================== 示例4：性能场景发现策略 ====================

class PerformanceDiscoverer(DiscovererStrategy):
    """
    性能场景发现策略
    
    功能：识别需要性能测试的场景
    
    适用场景：
    - 有性能要求
    - 需要测试高并发、大数据量
    """
    
    @property
    def name(self) -> str:
        return "performance_discoverer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def discover(self, context: Dict) -> List[Scenario]:
        """发现性能场景"""
        logger.info("执行性能场景发现...")
        
        scenarios = []
        
        # 从API定义识别
        apis = context.get("api_definitions", [])
        
        for idx, api in enumerate(apis[:20]):  # 最多20个API
            # 为每个API生成性能测试场景
            
            # 高并发场景
            scenarios.append(Scenario(
                id=f"PERF{idx+1:03d}_CONCURRENT",
                name=f"{api.get('path', 'API')} 高并发测试",
                type="performance",
                priority="P2",
                components=["API"],
                description=f"测试{api.get('path')}的高并发性能",
                test_steps=[
                    "启动并发测试工具",
                    f"并发调用{api.get('path', 'API')} 100次",
                    "测量响应时间",
                    "验证成功率",
                    "检查资源消耗"
                ],
                expected_result="响应时间<200ms，成功率>99%",
                discovery_strategy=self.name,
                metadata={"api": api}
            ))
            
            # 大数据量场景
            scenarios.append(Scenario(
                id=f"PERF{idx+1:03d}_DATA_VOLUME",
                name=f"{api.get('path', 'API')} 大数据量测试",
                type="performance",
                priority="P2",
                components=["API"],
                description=f"测试{api.get('path')}处理大数据量的性能",
                test_steps=[
                    "准备大数据量输入",
                    f"调用{api.get('path', 'API')}",
                    "测量处理时间",
                    "验证内存使用",
                    "验证结果正确性"
                ],
                expected_result="处理10000条数据<5s，内存稳定",
                discovery_strategy=self.name
            ))
        
        # 从组件调用链识别高频调用
        call_chains = context.get("call_chains", [])
        
        # 统计调用频率
        call_freq = {}
        for chain in call_chains:
            callee = chain.get("callee", "")
            call_freq[callee] = call_freq.get(callee, 0) + 1
        
        # 为高频调用生成性能测试
        top_calls = sorted(call_freq.items(), key=lambda x: x[1], reverse=True)[:10]
        
        for idx, (call, freq) in enumerate(top_calls):
            scenarios.append(Scenario(
                id=f"PERF_CALL{idx+1:03d}",
                name=f"{call} 高频调用性能",
                type="performance",
                priority="P1" if freq > 100 else "P2",
                components=["unknown"],
                description=f"高频调用点性能测试（频率: {freq})",
                test_steps=[
                    f"模拟调用{call}",
                    f"执行{freq}次",
                    "测量性能",
                    "验证稳定性"
                ],
                expected_result="性能稳定，无内存泄漏",
                discovery_strategy=self.name
            ))
        
        return scenarios
    
    def get_interactions(self, context: Dict) -> List[Interaction]:
        """性能场景不需要识别交互"""
        return []
    
    def execute(self, context: Dict) -> List[Scenario]:
        """执行"""
        return self.discover(context)


# ==================== 示例5：数据驱动测试生成插件 ====================

class DataDrivenGenerator(GeneratorPlugin):
    """
    数据驱动测试生成插件
    
    功能：生成数据驱动测试（参数化测试）
    
    适用场景：
    - 需要多组数据测试
    - 边界值测试
    """
    
    @property
    def name(self) -> str:
        return "data_driven_generator"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def generate(self, scenario: Scenario, context: Dict) -> str:
        """生成数据驱动测试"""
        
        # 获取数据模板
        data_template = self._get_data_template(scenario, context)
        
        # 生成测试代码
        code = self._generate_test_code(scenario, data_template, context)
        
        return code
    
    def _get_data_template(self, scenario: Scenario, context: Dict) -> Dict:
        """获取数据模板"""
        
        # 从配置获取
        templates = context.get("data_templates", {})
        
        # 根据场景类型选择模板
        if scenario.type == "boundary":
            return templates.get("boundary", self._default_boundary_template())
        elif scenario.type == "normal":
            return templates.get("normal", self._default_normal_template())
        else:
            return self._default_normal_template()
    
    def _default_boundary_template(self) -> Dict:
        """默认边界值模板"""
        return {
            "fields": {
                "id": {"type": "number", "values": [0, 1, -1, 999999999]},
                "name": {"type": "string", "values": ["", "normal", "very_long_string_123456789"]},
                "amount": {"type": "number", "values": [0, 0.01, 9999999.99]}
            },
            "combinations": 10
        }
    
    def _default_normal_template(self) -> Dict:
        """默认正常值模板"""
        return {
            "fields": {
                "id": {"type": "number", "values": [100, 200, 300]},
                "name": {"type": "string", "values": ["test1", "test2", "test3"]},
                "amount": {"type": "number", "values": [10.5, 20.5, 30.5]}
            },
            "combinations": 5
        }
    
    def _generate_test_code(self, scenario: Scenario, template: Dict, context: Dict) -> str:
        """生成测试代码"""
        
        project_lang = context.get("project_language", "java")
        
        if project_lang == "java":
            return self._generate_java_test(scenario, template)
        elif project_lang == "python":
            return self._generate_python_test(scenario, template)
        else:
            return self._generate_java_test(scenario, template)
    
    def _generate_java_test(self, scenario: Scenario, template: Dict) -> str:
        """生成Java数据驱动测试"""
        
        code = f"""/**
 * 数据驱动测试: {scenario.name}
 * 场景ID: {scenario.id}
 */
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import java.util.Arrays;
import java.util.Collection;

@RunWith(Parameterized.class)
public class {scenario.id}Test {
    
    @Parameters(name = "{0}")
    public static Collection<Object[]> data() {{
        return Arrays.asList(new Object[][] {{
"""
        
        # 添加测试数据
        fields = template.get("fields", {})
        
        # 生成数据组合（简化：只组合前两个字段）
        field_names = list(fields.keys())[:2]
        if len(field_names) >= 2:
            values1 = fields[field_names[0]]["values"]
            values2 = fields[field_names[1]]["values"]
            
            for v1 in values1[:3]:
                for v2 in values2[:3]:
                    code += f"            {{\"{field_names[0]}={v1}, {field_names[1]}={v2}\", {v1}, \"{v2}\"}},\n"
        
        code += """        });
    }
    
    private String testName;
    private Object param1;
    private Object param2;
    
    public """ + scenario.id + """Test(String testName, Object param1, Object param2) {
        this.testName = testName;
        this.param1 = param1;
        this.param2 = param2;
    }
    
    @Test
    public void test() {
        // Given
        System.out.println("测试: " + testName);
        
        // When - TODO: 实现测试逻辑
        // 使用 param1 和 param2
        
        // Then - TODO: 验证结果
        // assertTrue(...)
    }
}
"""
        
        return code
    
    def _generate_python_test(self, scenario: Scenario, template: Dict) -> str:
        """生成Python数据驱动测试"""
        
        code = f"""\"\"\"
数据驱动测试: {scenario.name}
场景ID: {scenario.id}
\"\"\"
import pytest

class Test{scenario.id}:
    
    @pytest.mark.parametrize(\"param1,param2\", [
"""
        
        # 添加测试数据
        fields = template.get("fields", {})
        
        field_names = list(fields.keys())[:2]
        if len(field_names) >= 2:
            values1 = fields[field_names[0]]["values"]
            values2 = fields[field_names[1]]["values"]
            
            for v1 in values1[:3]:
                for v2 in values2[:3]:
                    if isinstance(v2, str):
                        code += f"        ({v1}, \"{v2}\"),\n"
                    else:
                        code += f"        ({v1}, {v2}),\n"
        
        code += """    ])
    def test(self, param1, param2):
        """Test case"""
        # Given
        print(f"测试参数: param1={param1}, param2={param2}")
        
        # When - TODO: 实现测试逻辑
        
        # Then - TODO: 验证结果
        # assert ...
"""
        
        return code
    
    def execute(self, context: Dict) -> str:
        """执行"""
        scenario = context.get("scenario")
        return self.generate(scenario, context)


# ==================== 使用说明 ====================

"""
使用示例：

1. 将此文件保存到 hyx/plugins/custom_plugins.py

2. 在框架中使用：

from framework import TestAutomationFramework
from custom_plugins import (
    ConfigDependencyAnalyzer,
    ApiDocAnalyzer,
    BugHistoryDiscoverer,
    PerformanceDiscoverer,
    DataDrivenGenerator
)

# 创建框架
framework = TestAutomationFramework()

# 注册插件
framework.add_plugin(ConfigDependencyAnalyzer)
framework.add_plugin(ApiDocAnalyzer)
framework.add_generator(DataDrivenGenerator)

# 注册策略
framework.add_strategy(BugHistoryDiscoverer, "exception")
framework.add_strategy(PerformanceDiscoverer, "performance")

# 创建自定义工作流
framework.add_workflow("custom", {
    "name": "custom_workflow",
    "steps": [
        {"name": "analyze_code", "type": "analyze", "plugin": "static_code_analyzer"},
        {"name": "analyze_config", "type": "analyze", "plugin": "config_dependency_analyzer"},
        {"name": "analyze_api", "type": "analyze", "plugin": "api_doc_analyzer"},
        {"name": "discover_interaction", "type": "discover", "strategy_category": "interaction"},
        {"name": "discover_bug", "type": "discover", "strategy_category": "exception"},
        {"name": "discover_performance", "type": "discover", "strategy_category": "performance"},
        {"name": "generate_tests", "type": "generate", "plugin": "llm_generator", "priority": "P0"},
        {"name": "generate_data_driven", "type": "generate", "plugin": "data_driven_generator", "type": "boundary"}
    ]
})

# 运行
framework.run("custom")

3. 或者通过配置文件配置：

# hyx/framework.yaml
plugins:
  builtin: [static_code_analyzer, llm_generator]
  custom: [config_dependency_analyzer, api_doc_analyzer, data_driven_generator]

strategies:
  interaction: [interaction_discoverer]
  exception: [bug_history_discoverer]
  performance: [performance_discoverer]

workflows:
  full:
    name: full_workflow
    steps:
      - name: analyze_all
        type: analyze
        plugin: static_code_analyzer
      - name: analyze_config
        type: analyze
        plugin: config_dependency_analyzer
      - name: discover_interaction
        type: discover
        strategy_category: interaction
      - name: discover_bugs
        type: discover
        strategy_category: exception
      - name: generate_tests
        type: generate
        plugin: llm_generator
        priority: P0
        limit: 30
"""

if __name__ == "__main__":
    print(__doc__)