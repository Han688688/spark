#!/usr/bin/env python3
"""
可扩展的测试自动化框架
核心设计：
1. 插件化架构 - 每个能力都是插件
2. 多策略识别 - 支持多种场景识别策略
3. 工作流编排 - 可自定义流程
4. 配置驱动 - 通过配置扩展能力

架构：
┌──────────────────────────────────────────────────┐
│                 Framework Core                    │
├──────────────────────────────────────────────────┤
│  Plugin Manager │ Strategy Registry │ Workflow  │
└──────────────────────────────────────────────────┘
         │                │              │
         ▼                ▼              ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Analyzer     │ │ Discoverer   │ │ Generator    │
│ Plugins      │ │ Strategies   │ │ Plugins      │
└──────────────┘ └──────────────┘ └──────────────┘
"""

import os
import json
import yaml
import importlib
import inspect
from pathlib import Path
from typing import Dict, List, Any, Type, Callable, Optional
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict, field
from datetime import datetime
import re
import subprocess
import ast
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================== 核心数据结构 ====================

@dataclass
class Component:
    """组件信息"""
    name: str
    type: str
    version: str = ""
    importance: int = 0
    dependencies: List[str] = field(default_factory=list)
    core_functions: List[str] = field(default_factory=list)
    api_interfaces: List[Dict] = field(default_factory=list)  # API接口定义
    data_models: List[Dict] = field(default_factory=list)  # 数据模型


@dataclass  
class Interaction:
    """组件交互"""
    source: str  # 源组件
    target: str  # 目标组件
    interaction_type: str  # call, data_flow, event, config
    interface: str  # 交互接口/API
    data_format: str = ""  # 数据格式
    frequency: int = 0  # 调用频率
    critical_path: bool = False  # 是否关键路径
    conditions: List[str] = field(default_factory=list)  # 触发条件


@dataclass
class Scenario:
    """测试场景"""
    id: str
    name: str
    type: str
    priority: str
    components: List[str]
    description: str
    test_steps: List[str]
    expected_result: str
    interactions: List[str] = field(default_factory=list)  # 涉及的交互
    source_code_ref: str = ""
    discovery_strategy: str = ""  # 来源策略
    metadata: Dict = field(default_factory=dict)  # 扩展字段


# ==================== 插件基类 ====================

class PluginBase(ABC):
    """插件基类"""
    
    @property
    @abstractmethod
    def name(self) -> str:
        """插件名称"""
        pass
    
    @property
    @abstractmethod
    def version(self) -> str:
        """插件版本"""
        pass
    
    @abstractmethod
    def execute(self, context: Dict) -> Any:
        """执行插件"""
        pass
    
    def validate_config(self, config: Dict) -> bool:
        """验证配置"""
        return True


class AnalyzerPlugin(PluginBase):
    """项目分析插件基类"""
    
    @abstractmethod
    def analyze(self, project_root: Path, config: Dict) -> Dict:
        """分析项目"""
        pass


class DiscovererStrategy(PluginBase):
    """场景发现策略基类"""
    
    @property
    def strategy_type(self) -> str:
        """策略类型"""
        return "discoverer"
    
    @abstractmethod
    def discover(self, context: Dict) -> List[Scenario]:
        """发现场景"""
        pass
    
    @abstractmethod
    def get_interactions(self, context: Dict) -> List[Interaction]:
        """识别交互"""
        pass


class GeneratorPlugin(PluginBase):
    """测试生成插件基类"""
    
    @abstractmethod
    def generate(self, scenario: Scenario, context: Dict) -> str:
        """生成测试代码"""
        pass


# ==================== 具体插件实现 ====================

class StaticCodeAnalyzer(AnalyzerPlugin):
    """静态代码分析插件"""
    
    @property
    def name(self) -> str:
        return "static_code_analyzer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def analyze(self, project_root: Path, config: Dict) -> Dict:
        """静态分析代码"""
        logger.info("执行静态代码分析...")
        
        result = {
            "call_chains": self._extract_call_chains(project_root, config),
            "data_flows": self._extract_data_flows(project_root, config),
            "api_definitions": self._extract_api_definitions(project_root, config),
            "exception_paths": self._extract_exception_paths(project_root, config),
            "config_dependencies": self._extract_config_dependencies(project_root, config)
        }
        
        return result
    
    def _extract_call_chains(self, project_root: Path, config: Dict) -> List[Dict]:
        """提取调用链"""
        call_chains = []
        
        # 分析Java代码
        for java_file in project_root.rglob("*.java"):
            if self._should_ignore(java_file, config):
                continue
            
            try:
                content = java_file.read_text()
                
                # 提取方法调用
                calls = self._extract_java_method_calls(content)
                
                for call in calls:
                    call_chains.append({
                        "file": str(java_file.relative_to(project_root)),
                        "caller": call["caller"],
                        "callee": call["callee"],
                        "line": call["line"]
                    })
            except Exception as e:
                logger.warning(f"分析文件失败 {java_file}: {e}")
        
        # 分析Python代码
        for py_file in project_root.rglob("*.py"):
            if self._should_ignore(py_file, config):
                continue
            
            try:
                tree = ast.parse(py_file.read_text())
                calls = self._extract_python_calls(tree)
                
                for call in calls:
                    call_chains.append({
                        "file": str(py_file.relative_to(project_root)),
                        "caller": call["caller"],
                        "callee": call["callee"],
                        "line": call["line"]
                    })
            except Exception as e:
                logger.warning(f"分析文件失败 {py_file}: {e}")
        
        return call_chains
    
    def _extract_java_method_calls(self, content: str) -> List[Dict]:
        """提取Java方法调用"""
        calls = []
        
        # 简化实现：正则提取
        # 实际应该使用JavaParser等工具
        
        patterns = [
            r'(\w+)\.(\w+)\s*\(',  # obj.method()
            r'(\w+)\s+(\w+)\s*\(',  # Class method()
        ]
        
        for pattern in patterns:
            for match in re.finditer(pattern, content):
                calls.append({
                    "caller": "unknown",  # 需要更复杂的分析
                    "callee": f"{match.group(1)}.{match.group(2)}",
                    "line": content[:match.start()].count('\n') + 1
                })
        
        return calls
    
    def _extract_python_calls(self, tree: ast.AST) -> List[Dict]:
        """提取Python调用"""
        calls = []
        
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                if isinstance(node.func, ast.Name):
                    callee = node.func.id
                elif isinstance(node.func, ast.Attribute):
                    callee = f"{node.func.attr}"
                else:
                    continue
                
                # 找调用者（所在的函数）
                caller = self._find_caller_function(tree, node)
                
                calls.append({
                    "caller": caller,
                    "callee": callee,
                    "line": node.lineno
                })
        
        return calls
    
    def _find_caller_function(self, tree: ast.AST, node: ast.Call) -> str:
        """找到调用所在的函数"""
        for parent in ast.walk(tree):
            if isinstance(parent, ast.FunctionDef):
                for child in ast.walk(parent):
                    if child is node:
                        return parent.name
        return "unknown"
    
    def _extract_data_flows(self, project_root: Path, config: Dict) -> List[Dict]:
        """提取数据流"""
        # TODO: 实现数据流分析
        return []
    
    def _extract_api_definitions(self, project_root: Path, config: Dict) -> List[Dict]:
        """提取API定义"""
        apis = []
        
        # Java: 查找Controller或Service类
        for java_file in project_root.rglob("*Controller*.java"):
            content = java_file.read_text()
            
            # 提取@RequestMapping等注解
            # 简化：提取public方法
            methods = re.findall(r'public\s+\w+\s+(\w+)\s*\((.*?)\)', content)
            
            for method_name, params in methods:
                apis.append({
                    "file": str(java_file.relative_to(project_root)),
                    "method": method_name,
                    "params": params,
                    "type": "rest_api"
                })
        
        # Python: 查找Flask/FastAPI路由
        for py_file in project_root.rglob("*.py"):
            content = py_file.read_text()
            
            # Flask路由
            routes = re.findall(r'@app\.route\(["\']([^"\']+)["\']', content)
            for route in routes:
                apis.append({
                    "file": str(py_file.relative_to(project_root)),
                    "route": route,
                    "type": "rest_api"
                })
        
        return apis
    
    def _extract_exception_paths(self, project_root: Path, config: Dict) -> List[Dict]:
        """提取异常路径"""
        # TODO: 实现异常路径分析
        return []
    
    def _extract_config_dependencies(self, project_root: Path, config: Dict) -> List[Dict]:
        """提取配置依赖"""
        deps = []
        
        # 查找配置文件
        for conf_file in project_root.rglob("*.yaml"):
            try:
                content = yaml.safe_load(conf_file.read_text())
                # 提取组件配置
                # TODO: 实现具体逻辑
            except:
                pass
        
        return deps
    
    def _should_ignore(self, file_path: Path, config: Dict) -> bool:
        """是否忽略"""
        ignore_dirs = config.get("ignore_dirs", ['.git', 'node_modules', '__pycache__', 'target', 'build', 'test', 'tests'])
        ignore_files = config.get("ignore_files", ['Test*.java', '*Test.java', 'test_*.py'])
        
        for ignore in ignore_dirs:
            if ignore in str(file_path):
                return True
        
        for pattern in ignore_files:
            if re.match(pattern, file_path.name):
                return True
        
        return False
    
    def execute(self, context: Dict) -> Dict:
        """执行插件"""
        project_root = Path(context["project_root"])
        config = context.get("analyzer_config", {})
        return self.analyze(project_root, config)


class InteractionDiscoverer(DiscovererStrategy):
    """交互场景发现策略"""
    
    @property
    def name(self) -> str:
        return "interaction_discoverer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def discover(self, context: Dict) -> List[Scenario]:
        """发现交互场景"""
        logger.info("执行交互场景发现...")
        
        interactions = self.get_interactions(context)
        
        scenarios = []
        
        for idx, interaction in enumerate(interactions):
            # 为每个交互生成多个测试场景
            
            # 正常交互场景
            scenarios.append(self._create_normal_scenario(interaction, idx))
            
            # 异常交互场景
            scenarios.extend(self._create_exception_scenarios(interaction, idx))
            
            # 边界交互场景
            scenarios.extend(self._create_boundary_scenarios(interaction, idx))
            
            # 并发交互场景
            scenarios.extend(self._create_concurrent_scenarios(interaction, idx))
        
        return scenarios
    
    def get_interactions(self, context: Dict) -> List[Interaction]:
        """识别交互"""
        interactions = []
        
        # 从调用链提取
        call_chains = context.get("call_chains", [])
        for chain in call_chains:
            # 分析调用链，识别组件交互
            interaction = self._analyze_call_chain(chain, context)
            if interaction:
                interactions.append(interaction)
        
        # 从API定义提取
        api_defs = context.get("api_definitions", [])
        for api in api_defs:
            interaction = self._analyze_api_interaction(api, context)
            if interaction:
                interactions.append(interaction)
        
        # 从配置依赖提取
        config_deps = context.get("config_dependencies", [])
        for dep in config_deps:
            interaction = self._analyze_config_dependency(dep, context)
            if interaction:
                interactions.append(interaction)
        
        # 去重
        interactions = self._deduplicate_interactions(interactions)
        
        # 排序
        interactions.sort(key=lambda x: x.frequency, reverse=True)
        
        return interactions
    
    def _analyze_call_chain(self, chain: Dict, context: Dict) -> Optional[Interaction]:
        """分析调用链识别交互"""
        callee = chain.get("callee", "")
        
        # 判断是否跨组件调用
        components = context.get("components", [])
        
        for comp in components:
            comp_name = comp.get("name", "")
            
            # 如果调用了其他组件的方法
            if comp_name.lower() in callee.lower():
                # 找到调用方
                caller_comp = self._find_component_by_file(chain["file"], components)
                
                if caller_comp and caller_comp != comp_name:
                    return Interaction(
                        source=caller_comp,
                        target=comp_name,
                        interaction_type="call",
                        interface=callee,
                        frequency=1,
                        critical_path=comp.get("importance", 0) >= 8
                    )
        
        return None
    
    def _analyze_api_interaction(self, api: Dict, context: Dict) -> Optional[Interaction]:
        """分析API交互"""
        # TODO: 实现API交互分析
        return None
    
    def _analyze_config_dependency(self, dep: Dict, context: Dict) -> Optional[Interaction]:
        """分析配置依赖"""
        # TODO: 实现配置依赖分析
        return None
    
    def _find_component_by_file(self, file_path: str, components: List[Dict]) -> Optional[str]:
        """根据文件找到组件"""
        # 简化实现
        for comp in components:
            if comp.get("name", "").lower() in file_path.lower():
                return comp["name"]
        return "unknown"
    
    def _deduplicate_interactions(self, interactions: List[Interaction]) -> List[Interaction]:
        """去重"""
        seen = set()
        unique = []
        
        for interaction in interactions:
            key = f"{interaction.source}->{interaction.target}:{interaction.interface}"
            if key not in seen:
                seen.add(key)
                unique.append(interaction)
            else:
                # 增加频率
                for i in unique:
                    if f"{i.source}->{i.target}:{i.interface}" == key:
                        i.frequency += 1
                        break
        
        return unique
    
    def _create_normal_scenario(self, interaction: Interaction, idx: int) -> Scenario:
        """创建正常交互场景"""
        return Scenario(
            id=f"INT{idx+1:03d}_NORMAL",
            name=f"{interaction.source}与{interaction.target}正常交互",
            type="integration",
            priority="P0" if interaction.critical_path else "P1",
            components=[interaction.source, interaction.target],
            interactions=[f"{interaction.source}->{interaction.target}"],
            description=f"测试{interaction.source}调用{interaction.target}的{interaction.interface}接口",
            test_steps=[
                f"初始化{interaction.source}组件",
                f"准备调用参数",
                f"{interaction.source}调用{interaction.target}.{interaction.interface}",
                f"验证返回结果",
                f"验证数据状态"
            ],
            expected_result=f"{interaction.interface}调用成功，返回正确结果",
            discovery_strategy=self.name
        )
    
    def _create_exception_scenarios(self, interaction: Interaction, idx: int) -> List[Scenario]:
        """创建异常交互场景"""
        scenarios = []
        
        # 目标组件异常
        scenarios.append(Scenario(
            id=f"INT{idx+1:03d}_EXCEPTION_TARGET",
            name=f"{interaction.target}组件异常",
            type="exception",
            priority="P1",
            components=[interaction.source, interaction.target],
            interactions=[f"{interaction.source}->{interaction.target}"],
            description=f"{interaction.target}返回异常时{interaction.source}的处理",
            test_steps=[
                f"初始化{interaction.source}",
                f"Mock {interaction.target}返回异常",
                f"{interaction.source}调用{interaction.target}.{interaction.interface}",
                f"验证异常处理",
                f"验证系统状态"
            ],
            expected_result="异常被正确处理，系统状态正常",
            discovery_strategy=self.name
        ))
        
        # 网络异常
        scenarios.append(Scenario(
            id=f"INT{idx+1:03d}_EXCEPTION_NETWORK",
            name=f"{interaction.source}与{interaction.target}网络异常",
            type="exception",
            priority="P1",
            components=[interaction.source, interaction.target],
            interactions=[f"{interaction.source}->{interaction.target}"],
            description="网络超时或中断时的处理",
            test_steps=[
                f"初始化{interaction.source}",
                f"模拟网络异常",
                f"{interaction.source}调用{interaction.target}.{interaction.interface}",
                f"验证超时处理",
                f"验证重试机制"
            ],
            expected_result="超时被正确处理，有重试机制",
            discovery_strategy=self.name
        ))
        
        return scenarios
    
    def _create_boundary_scenarios(self, interaction: Interaction, idx: int) -> List[Scenario]:
        """创建边界交互场景"""
        scenarios = []
        
        # 数据边界
        scenarios.append(Scenario(
            id=f"INT{idx+1:03d}_BOUNDARY_DATA",
            name=f"{interaction.interface}数据边界",
            type="boundary",
            priority="P2",
            components=[interaction.source, interaction.target],
            interactions=[f"{interaction.source}->{interaction.target}"],
            description="测试数据边界值",
            test_steps=[
                "准备边界值数据（空值、最大值、最小值）",
                f"{interaction.source}调用{interaction.target}.{interaction.interface}",
                "验证边界处理",
                "验证数据完整性"
            ],
            expected_result="边界值被正确处理",
            discovery_strategy=self.name
        ))
        
        # 频率边界
        scenarios.append(Scenario(
            id=f"INT{idx+1:03d}_BOUNDARY_FREQUENCY",
            name=f"{interaction.interface}高频调用",
            type="boundary",
            priority="P2",
            components=[interaction.source, interaction.target],
            interactions=[f"{interaction.source}->{interaction.target}"],
            description="高频调用下的稳定性",
            test_steps=[
                "准备高频调用场景",
                f"连续调用{interaction.interface} 100次",
                "验证系统稳定性",
                "验证资源消耗"
            ],
            expected_result="高频调用下系统稳定",
            discovery_strategy=self.name
        ))
        
        return scenarios
    
    def _create_concurrent_scenarios(self, interaction: Interaction, idx: int) -> List[Scenario]:
        """创建并发交互场景"""
        scenarios = []
        
        scenarios.append(Scenario(
            id=f"INT{idx+1:03d}_CONCURRENT",
            name=f"{interaction.interface}并发调用",
            type="integration",
            priority="P1" if interaction.critical_path else "P2",
            components=[interaction.source, interaction.target],
            interactions=[f"{interaction.source}->{interaction.target}"],
            description="多个实例并发调用同一接口",
            test_steps=[
                "启动多个并发实例",
                f"并发调用{interaction.interface}",
                "验证并发安全性",
                "验证数据一致性"
            ],
            expected_result="并发调用安全，数据一致",
            discovery_strategy=self.name
        ))
        
        return scenarios
    
    def execute(self, context: Dict) -> List[Scenario]:
        """执行插件"""
        return self.discover(context)


class LogBasedDiscoverer(DiscovererStrategy):
    """基于日志的场景发现策略"""
    
    @property
    def name(self) -> str:
        return "log_based_discoverer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def discover(self, context: Dict) -> List[Scenario]:
        """从日志发现场景"""
        logger.info("执行日志场景发现...")
        
        scenarios = []
        
        # 加载日志文件
        log_files = context.get("log_files", [])
        
        if not log_files:
            # 自动查找日志
            project_root = Path(context["project_root"])
            log_files = list(project_root.rglob("*.log"))[:10]  # 最多10个
        
        for log_file in log_files:
            try:
                scenarios.extend(self._analyze_log_file(log_file, context))
            except Exception as e:
                logger.warning(f"分析日志失败 {log_file}: {e}")
        
        return scenarios
    
    def get_interactions(self, context: Dict) -> List[Interaction]:
        """从日志提取交互"""
        interactions = []
        
        # 从日志提取组件调用记录
        log_files = context.get("log_files", [])
        
        for log_file in log_files:
            try:
                content = log_file.read_text()
                
                # 提取调用记录
                # 示例格式：[ComponentA] calling ComponentB.method()
                pattern = r'\[(\w+)\]\s+calling\s+(\w+)\.(\w+)'
                
                for match in re.finditer(pattern, content):
                    interactions.append(Interaction(
                        source=match.group(1),
                        target=match.group(2),
                        interaction_type="call",
                        interface=match.group(3),
                        frequency=1
                    ))
            except:
                pass
        
        return interactions
    
    def _analyze_log_file(self, log_file: Path, context: Dict) -> List[Scenario]:
        """分析单个日志文件"""
        scenarios = []
        
        content = log_file.read_text()
        
        # 提取错误日志
        errors = self._extract_errors(content)
        
        for idx, error in enumerate(errors):
            scenarios.append(Scenario(
                id=f"LOG{idx+1:03d}_ERROR",
                name=f"日志发现的问题: {error['message'][:30]}",
                type="exception",
                priority="P1",
                components=self._extract_components_from_error(error),
                description=f"从日志发现的错误: {error['message']}",
                test_steps=[
                    "重现日志中的场景",
                    "验证错误是否发生",
                    "验证修复是否有效"
                ],
                expected_result="错误不再发生",
                discovery_strategy=self.name,
                metadata={"log_file": str(log_file), "error": error}
            ))
        
        # 提取业务流程
        flows = self._extract_business_flows(content)
        
        for idx, flow in enumerate(flows):
            scenarios.append(Scenario(
                id=f"LOG{idx+1:03d}_FLOW",
                name=f"日志发现的流程: {flow['name']}",
                type="normal",
                priority="P2",
                components=flow['components'],
                description=f"从日志提取的业务流程",
                test_steps=flow['steps'],
                expected_result="流程正常执行",
                discovery_strategy=self.name
            ))
        
        return scenarios
    
    def _extract_errors(self, content: str) -> List[Dict]:
        """提取错误"""
        errors = []
        
        # 提取ERROR级别的日志
        for line in content.split('\n'):
            if 'ERROR' in line or 'Exception' in line or 'Failed' in line:
                errors.append({
                    "message": line,
                    "line_number": content[:content.index(line)].count('\n') + 1
                })
        
        return errors[:20]  # 最多20个
    
    def _extract_business_flows(self, content: str) -> List[Dict]:
        """提取业务流程"""
        # TODO: 实现流程提取
        return []
    
    def _extract_components_from_error(self, error: Dict) -> List[str]:
        """从错误提取组件"""
        message = error.get("message", "")
        
        # 简化：提取大写单词
        components = re.findall(r'\b[A-Z][a-z]+\b', message)
        
        return components[:5]
    
    def execute(self, context: Dict) -> List[Scenario]:
        """执行"""
        return self.discover(context)


class LLMBasedGenerator(GeneratorPlugin):
    """基于LLM的测试生成插件"""
    
    @property
    def name(self) -> str:
        return "llm_generator"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def generate(self, scenario: Scenario, context: Dict) -> str:
        """生成测试代码"""
        
        # 使用LLM生成
        llm_client = context.get("llm_client")
        
        if not llm_client:
            logger.error("没有LLM客户端")
            return ""
        
        # 构建Prompt
        prompt = self._build_prompt(scenario, context)
        
        # 调用LLM
        code = llm_client.generate(prompt)
        
        # 提取代码块
        code = self._extract_code(code)
        
        return code
    
    def _build_prompt(self, scenario: Scenario, context: Dict) -> str:
        """构建生成Prompt"""
        
        # 获取相关源码
        source_code = self._get_relevant_source(scenario, context)
        
        prompt = f"""请生成测试代码。

## 测试场景
- ID: {scenario.id}
- 名称: {scenario.name}
- 类型: {scenario.type}
- 涉及组件: {', '.join(scenario.components)}
- 涉及交互: {', '.join(scenario.interactions) if scenario.interactions else '无'}
- 测试步骤: {chr(10).join(f'{i+1}. {s}' for i, s in enumerate(scenario.test_steps))}
- 预期结果: {scenario.expected_result}

## 项目信息
- 语言: {context.get('project_language', 'java')}
- 框架: {', '.join(context.get('frameworks', []))}

## 相关源码
{source_code}

## 要求
1. 生成完整可运行的测试代码
2. 如果涉及多组件交互，使用Mock模拟依赖组件
3. 测试数据硬编码在测试中
4. 使用适当的断言验证结果
5. 添加必要注释

请直接输出测试代码。
"""
        
        return prompt
    
    def _get_relevant_source(self, scenario: Scenario, context: Dict) -> str:
        """获取相关源码"""
        
        project_root = Path(context["project_root"])
        snippets = []
        
        # 根据组件名查找源码
        for comp_name in scenario.components[:2]:
            for ext in ['*.java', '*.scala', '*.py']:
                for file in project_root.rglob(ext):
                    if comp_name.lower() in file.name.lower():
                        try:
                            content = file.read_text()
                            # 提取前100行
                            snippets.append(f"// {file.name}\n{content[:2000]}")
                        except:
                            pass
        
        return '\n\n'.join(snippets[:2])
    
    def _extract_code(self, text: str) -> str:
        """提取代码块"""
        # 提取 ```java 或 ```python 中的代码
        match = re.search(r'```(?:java|python)\s*(.*?)\s*```', text, re.DOTALL)
        return match.group(1) if match else text
    
    def execute(self, context: Dict) -> str:
        """执行"""
        scenario = context.get("scenario")
        return self.generate(scenario, context)


# ==================== 插件管理器 ====================

class PluginManager:
    """插件管理器"""
    
    def __init__(self):
        self.plugins: Dict[str, PluginBase] = {}
        self.plugin_dirs: List[Path] = []
    
    def register_plugin(self, plugin: PluginBase):
        """注册插件"""
        self.plugins[plugin.name] = plugin
        logger.info(f"注册插件: {plugin.name} v{plugin.version}")
    
    def load_plugins_from_dir(self, plugin_dir: Path):
        """从目录加载插件"""
        self.plugin_dirs.append(plugin_dir)
        
        # 动态加载
        for py_file in plugin_dir.rglob("*.py"):
            if py_file.name.startswith("_"):
                continue
            
            try:
                # 导入模块
                module_name = py_file.stem
                spec = importlib.util.spec_from_file_location(module_name, py_file)
                module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(module)
                
                # 查找插件类
                for name, obj in inspect.getmembers(module):
                    if inspect.isclass(obj) and issubclass(obj, PluginBase) and obj != PluginBase:
                        plugin = obj()
                        self.register_plugin(plugin)
            except Exception as e:
                logger.warning(f"加载插件失败 {py_file}: {e}")
    
    def get_plugin(self, name: str) -> Optional[PluginBase]:
        """获取插件"""
        return self.plugins.get(name)
    
    def get_plugins_by_type(self, plugin_type: Type) -> List[PluginBase]:
        """按类型获取插件"""
        return [p for p in self.plugins.values() if isinstance(p, plugin_type)]
    
    def list_plugins(self) -> List[str]:
        """列出所有插件"""
        return list(self.plugins.keys())


# ==================== 策略注册表 ====================

class StrategyRegistry:
    """策略注册表"""
    
    def __init__(self):
        self.strategies: Dict[str, List[DiscovererStrategy]] = {
            "interaction": [],  # 交互场景识别
            "business": [],     # 业务场景识别
            "exception": [],    # 异常场景识别
            "performance": [],  # 性能场景识别
            "security": []      # 安全场景识别
        }
    
    def register_strategy(self, strategy: DiscovererStrategy, category: str = "interaction"):
        """注册策略"""
        if category not in self.strategies:
            self.strategies[category] = []
        
        self.strategies[category].append(strategy)
        logger.info(f"注册策略: {strategy.name} -> {category}")
    
    def get_strategies(self, category: str) -> List[DiscovererStrategy]:
        """获取策略"""
        return self.strategies.get(category, [])
    
    def get_all_strategies(self) -> List[DiscovererStrategy]:
        """获取所有策略"""
        all_strategies = []
        for strategies in self.strategies.values():
            all_strategies.extend(strategies)
        return all_strategies


# ==================== 工作流引擎 ====================

class WorkflowEngine:
    """工作流引擎"""
    
    def __init__(self, plugin_manager: PluginManager, strategy_registry: StrategyRegistry):
        self.plugin_manager = plugin_manager
        self.strategy_registry = strategy_registry
        self.context: Dict = {}
    
    def execute_workflow(self, workflow_config: Dict) -> Dict:
        """执行工作流"""
        
        workflow_name = workflow_config.get("name", "default")
        logger.info(f"执行工作流: {workflow_name}")
        
        # 初始化上下文
        self.context = workflow_config.get("context", {})
        
        # 执行步骤
        steps = workflow_config.get("steps", [])
        
        for idx, step in enumerate(steps):
            step_name = step.get("name", f"step_{idx}")
            step_type = step.get("type")
            
            logger.info(f"执行步骤 {idx+1}/{len(steps)}: {step_name}")
            
            try:
                if step_type == "analyze":
                    self._execute_analyze_step(step)
                elif step_type == "discover":
                    self._execute_discover_step(step)
                elif step_type == "generate":
                    self._execute_generate_step(step)
                elif step_type == "run":
                    self._execute_run_step(step)
                elif step_type == "plugin":
                    self._execute_plugin_step(step)
                else:
                    logger.warning(f"未知步骤类型: {step_type}")
            except Exception as e:
                logger.error(f"步骤执行失败: {step_name} - {e}")
                
                if step.get("continue_on_error"):
                    continue
                else:
                    raise
        
        return self.context
    
    def _execute_analyze_step(self, step: Dict):
        """执行分析步骤"""
        plugin_name = step.get("plugin", "static_code_analyzer")
        plugin = self.plugin_manager.get_plugin(plugin_name)
        
        if plugin:
            result = plugin.execute(self.context)
            self.context.update(result)
    
    def _execute_discover_step(self, step: Dict):
        """执行发现步骤"""
        strategy_category = step.get("strategy_category", "interaction")
        strategies = self.strategy_registry.get_strategies(strategy_category)
        
        scenarios = []
        interactions = []
        
        for strategy in strategies:
            scenarios.extend(strategy.discover(self.context))
            interactions.extend(strategy.get_interactions(self.context))
        
        # 合并到上下文
        existing_scenarios = self.context.get("scenarios", [])
        existing_scenarios.extend(scenarios)
        self.context["scenarios"] = existing_scenarios
        
        existing_interactions = self.context.get("interactions", [])
        existing_interactions.extend(interactions)
        self.context["interactions"] = existing_interactions
    
    def _execute_generate_step(self, step: Dict):
        """执行生成步骤"""
        plugin_name = step.get("plugin", "llm_generator")
        plugin = self.plugin_manager.get_plugin(plugin_name)
        
        if not plugin:
            logger.error(f"找不到生成插件: {plugin_name}")
            return
        
        scenarios = self.context.get("scenarios", [])
        
        # 筛选
        if step.get("priority"):
            scenarios = [s for s in scenarios if s.priority == step["priority"]]
        if step.get("type"):
            scenarios = [s for s in scenarios if s.type == step["type"]]
        if step.get("limit"):
            scenarios = scenarios[:step["limit"]]
        
        generated_tests = []
        
        for scenario in scenarios:
            self.context["scenario"] = scenario
            code = plugin.execute(self.context)
            
            generated_tests.append({
                "scenario": asdict(scenario),
                "code": code
            })
        
        self.context["generated_tests"] = generated_tests
    
    def _execute_run_step(self, step: Dict):
        """执行运行步骤"""
        # TODO: 实现测试执行
        pass
    
    def _execute_plugin_step(self, step: Dict):
        """执行插件步骤"""
        plugin_name = step.get("plugin")
        plugin = self.plugin_manager.get_plugin(plugin_name)
        
        if plugin:
            result = plugin.execute(self.context)
            self.context[plugin_name] = result


# ==================== 框架核心 ====================

class TestAutomationFramework:
    """测试自动化框架"""
    
    def __init__(self, project_root: str = "."):
        self.root = Path(project_root).resolve()
        self.config_dir = self.root / "hyx"
        self.config_file = self.config_dir / "framework.yaml"
        
        # 核心组件
        self.plugin_manager = PluginManager()
        self.strategy_registry = StrategyRegistry()
        self.workflow_engine = WorkflowEngine(self.plugin_manager, self.strategy_registry)
        
        # 配置
        self.config = self._load_config()
        
        # 初始化
        self._init_framework()
    
    def _load_config(self) -> Dict:
        """加载配置"""
        if self.config_file.exists():
            with open(self.config_file) as f:
                return yaml.safe_load(f)
        else:
            return self._default_config()
    
    def _default_config(self) -> Dict:
        """默认配置"""
        return {
            "version": "1.0.0",
            "project_root": str(self.root),
            "plugins": {
                "builtin": ["static_code_analyzer", "llm_generator"],
                "custom": []
            },
            "strategies": {
                "interaction": ["interaction_discoverer", "log_based_discoverer"],
                "business": [],
                "exception": [],
                "performance": [],
                "security": []
            },
            "workflows": {
                "default": {
                    "name": "default_workflow",
                    "steps": [
                        {"name": "analyze_code", "type": "analyze", "plugin": "static_code_analyzer"},
                        {"name": "discover_interaction", "type": "discover", "strategy_category": "interaction"},
                        {"name": "discover_from_logs", "type": "discover", "strategy_category": "log"},
                        {"name": "generate_tests", "type": "generate", "plugin": "llm_generator", "priority": "P0", "limit": 20}
                    ]
                }
            }
        }
    
    def _save_config(self):
        """保存配置"""
        self.config_dir.mkdir(exist_ok=True)
        with open(self.config_file, 'w') as f:
            yaml.dump(self.config, f)
    
    def _init_framework(self):
        """初始化框架"""
        logger.info("初始化测试自动化框架...")
        
        # 注册内置插件
        self.plugin_manager.register_plugin(StaticCodeAnalyzer())
        self.plugin_manager.register_plugin(LLMBasedGenerator())
        
        # 注册内置策略
        self.strategy_registry.register_strategy(InteractionDiscoverer(), "interaction")
        self.strategy_registry.register_strategy(LogBasedDiscoverer(), "log")
        
        # 加载自定义插件
        plugin_dir = self.config_dir / "plugins"
        if plugin_dir.exists():
            self.plugin_manager.load_plugins_from_dir(plugin_dir)
        
        logger.info(f"框架初始化完成，已加载 {len(self.plugin_manager.list_plugins())} 个插件")
    
    def run(self, workflow_name: str = "default", custom_config: Dict = None):
        """运行工作流"""
        
        workflow_config = self.config["workflows"].get(workflow_name)
        
        if not workflow_config:
            logger.error(f"找不到工作流: {workflow_name}")
            return
        
        # 合合自定义配置
        if custom_config:
            workflow_config["context"] = custom_config
        
        # 初始化上下文
        workflow_config.setdefault("context", {})
        workflow_config["context"]["project_root"] = str(self.root)
        
        # 执行工作流
        result = self.workflow_engine.execute_workflow(workflow_config)
        
        # 保存结果
        self._save_results(result)
        
        return result
    
    def _save_results(self, result: Dict):
        """保存结果"""
        result_dir = self.config_dir / "results"
        result_dir.mkdir(exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # 保存场景
        if "scenarios" in result:
            scenarios_file = result_dir / f"scenarios_{timestamp}.json"
            with open(scenarios_file, 'w') as f:
                json.dump(result["scenarios"], f, indent=2)
            logger.info(f"场景已保存: {scenarios_file}")
        
        # 保存交互
        if "interactions" in result:
            interactions_file = result_dir / f"interactions_{timestamp}.json"
            with open(interactions_file, 'w') as f:
                json.dump([asdict(i) for i in result["interactions"]], f, indent=2)
            logger.info(f"交互已保存: {interactions_file}")
        
        # 保存生成的测试
        if "generated_tests" in result:
            tests_dir = self.config_dir / "generated_tests"
            tests_dir.mkdir(exist_ok=True)
            
            for test_info in result["generated_tests"]:
                scenario = test_info["scenario"]
                code = test_info["code"]
                
                if scenario["id"].startswith("INT"):
                    file_name = f"{scenario['id']}_InteractionTest.java"
                else:
                    file_name = f"{scenario['id']}_Test.java"
                
                test_file = tests_dir / file_name
                test_file.write_text(code)
                logger.info(f"测试已保存: {test_file}")
    
    def add_plugin(self, plugin_class: Type[PluginBase]):
        """添加插件"""
        plugin = plugin_class()
        self.plugin_manager.register_plugin(plugin)
    
    def add_strategy(self, strategy_class: Type[DiscovererStrategy], category: str):
        """添加策略"""
        strategy = strategy_class()
        self.strategy_registry.register_strategy(strategy, category)
    
    def add_workflow(self, workflow_name: str, workflow_config: Dict):
        """添加工作流"""
        self.config["workflows"][workflow_name] = workflow_config
        self._save_config()
    
    def list_capabilities(self):
        """列出所有能力"""
        print("\n=== 框架能力 ===")
        
        print("\n已注册插件:")
        for name in self.plugin_manager.list_plugins():
            plugin = self.plugin_manager.get_plugin(name)
            print(f"  - {name} v{plugin.version}")
        
        print("\n已注册策略:")
        for category, strategies in self.strategy_registry.strategies.items():
            print(f"  [{category}]")
            for strategy in strategies:
                print(f"    - {strategy.name} v{strategy.version}")
        
        print("\n可用工作流:")
        for name, config in self.config["workflows"].items():
            print(f"  - {name}: {len(config['steps'])} 步骤")


# ==================== CLI入口 ====================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(description="可扩展测试自动化框架")
    
    subparsers = parser.add_subparsers(dest="command")
    
    # run
    parser_run = subparsers.add_parser("run", help="运行工作流")
    parser_run.add_argument("--workflow", default="default", help="工作流名称")
    parser_run.add_argument("--priority", help="生成指定优先级的测试")
    parser_run.add_argument("--limit", type=int, help="限制生成数量")
    
    # list
    parser_list = subparsers.add_parser("list", help="列出能力")
    
    # init
    parser_init = subparsers.add_parser("init", help="初始化框架")
    
    args = parser.parse_args()
    
    framework = TestAutomationFramework()
    
    if args.command == "run":
        custom_config = {}
        if args.priority:
            custom_config["priority"] = args.priority
        if args.limit:
            custom_config["limit"] = args.limit
        
        framework.run(args.workflow, custom_config)
    
    elif args.command == "list":
        framework.list_capabilities()
    
    elif args.command == "init":
        framework._save_config()
        print("框架已初始化")
    
    else:
        parser.print_help()


if __name__ == "__main__":
    main()