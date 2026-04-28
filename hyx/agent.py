#!/usr/bin/env python3
"""
AI驱动的测试自动化Agent
核心能力：分析项目 → 识别场景 → 生成测试 → 执行验证 → 自动修复

使用方式：
    python hyx/agent.py init              # 初始化项目配置
    python hyx/agent.py analyze           # 分析项目结构
    python hyx/agent.py discover          # 发现测试场景
    python hyx/agent.py generate          # 生成测试脚本
    python hyx/agent.py run               # 执行测试
    python hyx/agent.py fix               # AI修复失败测试
    python hyx/agent.py all               # 一键全流程

配置文件：hyx/project.yaml
"""

import os
import json
import yaml
import argparse
from pathlib import Path
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime
import subprocess
import re


@dataclass
class Component:
    """组件信息"""
    name: str
    type: str  # java, python, spark, database, etc.
    version: str = ""
    importance: int = 0  # 1-10
    dependencies: List[str] = None
    core_functions: List[str] = None
    
    def __post_init__(self):
        if self.dependencies is None:
            self.dependencies = []
        if self.core_functions is None:
            self.core_functions = []


@dataclass
class Scenario:
    """测试场景"""
    id: str
    name: str
    type: str  # normal, exception, boundary, integration, performance
    priority: str  # P0, P1, P2
    components: List[str]
    description: str
    test_steps: List[str]
    expected_result: str
    source_code_ref: str = ""  # 关联的源码路径


@dataclass
class TestResult:
    """测试结果"""
    scenario_id: str
    test_file: str
    status: str  # passed, failed, error
    duration: float
    error_message: str = ""
    timestamp: str = ""


class ProjectAnalyzer:
    """项目分析器"""
    
    def __init__(self, project_root: Path):
        self.root = project_root
        self.ignore_dirs = {'.git', 'node_modules', '__pycache__', 'target', 'build', 'dist'}
    
    def analyze(self) -> Dict[str, Any]:
        """分析项目结构"""
        return {
            "basic_info": self._analyze_basic_info(),
            "languages": self._detect_languages(),
            "frameworks": self._detect_frameworks(),
            "dependencies": self._analyze_dependencies(),
            "components": self._identify_components(),
            "source_structure": self._analyze_source_structure()
        }
    
    def _analyze_basic_info(self) -> Dict:
        """分析基本信息"""
        info = {
            "name": self.root.name,
            "type": "unknown",
            "build_tools": []
        }
        
        # 检测构建工具
        if (self.root / "pom.xml").exists():
            info["build_tools"].append("maven")
            info["type"] = "java"
        if (self.root / "build.gradle").exists():
            info["build_tools"].append("gradle")
            info["type"] = "java"
        if (self.root / "build.sbt").exists():
            info["build_tools"].append("sbt")
            info["type"] = "scala"
        if (self.root / "requirements.txt").exists():
            info["type"] = "python"
        if (self.root / "setup.py").exists():
            info["type"] = "python"
        
        return info
    
    def _detect_languages(self) -> List[Dict]:
        """检测编程语言"""
        languages = {}
        
        for file_path in self.root.rglob("*"):
            if any(ignore in str(file_path) for ignore in self.ignore_dirs):
                continue
            
            ext = file_path.suffix
            if ext in ['.java', '.scala', '.py', '.sh', '.sql', '.go', '.js', '.ts']:
                lang = {
                    '.java': 'java',
                    '.scala': 'scala', 
                    '.py': 'python',
                    '.sh': 'shell',
                    '.sql': 'sql',
                    '.go': 'go',
                    '.js': 'javascript',
                    '.ts': 'typescript'
                }.get(ext, 'unknown')
                
                languages[lang] = languages.get(lang, 0) + 1
        
        return [{"language": k, "file_count": v} for k, v in sorted(languages.items(), key=lambda x: x[1], reverse=True)]
    
    def _detect_frameworks(self) -> List[str]:
        """检测使用的框架"""
        frameworks = []
        
        # 检查pom.xml
        pom = self.root / "pom.xml"
        if pom.exists():
            content = pom.read_text()
            if "spark" in content.lower():
                frameworks.append("spark")
            if "hadoop" in content.lower():
                frameworks.append("hadoop")
            if "hive" in content.lower():
                frameworks.append("hive")
            if "kafka" in content.lower():
                frameworks.append("kafka")
            if "flink" in content.lower():
                frameworks.append("flink")
            if "spring" in content.lower():
                frameworks.append("spring")
        
        # 检查requirements.txt
        reqs = self.root / "requirements.txt"
        if reqs.exists():
            content = reqs.read_text()
            if "pyspark" in content.lower():
                frameworks.append("pyspark")
            if "pytest" in content.lower():
                frameworks.append("pytest")
            if "django" in content.lower():
                frameworks.append("django")
            if "flask" in content.lower():
                frameworks.append("flask")
        
        return frameworks
    
    def _analyze_dependencies(self) -> Dict:
        """分析依赖"""
        deps = {
            "java": [],
            "python": []
        }
        
        # Maven依赖
        pom = self.root / "pom.xml"
        if pom.exists():
            content = pom.read_text()
            # 简化提取groupId和artifactId
            matches = re.findall(r'<groupId>(.*?)</groupId>\s*<artifactId>(.*?)</artifactId>', content)
            for group, artifact in matches:
                if group not in ['org.apache.spark', 'org.apache.hadoop']:  # 排除一些系统依赖
                    deps["java"].append(f"{group}:{artifact}")
        
        # Python依赖
        reqs = self.root / "requirements.txt"
        if reqs.exists():
            for line in reqs.read_text().strip().split('\n'):
                if line and not line.startswith('#'):
                    deps["python"].append(line.strip())
        
        return deps
    
    def _identify_components(self) -> List[Component]:
        """识别组件"""
        components = []
        
        # 基于框架推断组件
        frameworks = self._detect_frameworks()
        
        framework_components = {
            "spark": Component(
                name="Spark",
                type="compute",
                importance=9,
                core_functions=["分布式计算", "SQL查询", "流处理", "机器学习"]
            ),
            "hadoop": Component(
                name="Hadoop",
                type="storage",
                importance=8,
                core_functions=["分布式存储", "资源管理"]
            ),
            "hive": Component(
                name="Hive",
                type="warehouse",
                importance=8,
                core_functions=["数据仓库", "SQL查询", "元数据管理"]
            ),
            "kafka": Component(
                name="Kafka",
                type="messaging",
                importance=7,
                core_functions=["消息队列", "流式处理"]
            ),
            "hbase": Component(
                name="HBase",
                type="database",
                importance=7,
                core_functions=["NoSQL存储", "实时读写"]
            ),
            "flink": Component(
                name="Flink",
                type="streaming",
                importance=7,
                core_functions=["流处理", "事件驱动"]
            )
        }
        
        for fw in frameworks:
            if fw in framework_components:
                components.append(framework_components[fw])
        
        # 从源码中识别自定义组件
        components.extend(self._identify_custom_components())
        
        # 按重要性排序
        components.sort(key=lambda x: x.importance, reverse=True)
        
        return components
    
    def _identify_custom_components(self) -> List[Component]:
        """从源码识别自定义组件"""
        components = []
        
        # 查找主要的包/模块
        for lang, count in self._detect_languages():
            if lang == "java" or lang == "scala":
                # 查找src/main/java或src/main/scala下的主要包
                src_dirs = list(self.root.rglob("src/main/java")) + list(self.root.rglob("src/main/scala"))
                for src_dir in src_dirs[:1]:  # 只看第一个
                    for pkg_dir in src_dir.iterdir():
                        if pkg_dir.is_dir() and not pkg_dir.name.startswith('.'):
                            # 统计包下的文件数
                            file_count = len(list(pkg_dir.rglob("*.java"))) + len(list(pkg_dir.rglob("*.scala")))
                            if file_count > 5:  # 只关注较大的包
                                components.append(Component(
                                    name=pkg_dir.name,
                                    type="module",
                                    importance=5,
                                    core_functions=["业务模块"]
                                ))
            
            elif lang == "python":
                # 查找Python模块
                for py_dir in self.root.rglob("*/"):
                    if py_dir.name in ['test', 'tests', '__pycache__']:
                        continue
                    py_files = list(py_dir.glob("*.py"))
                    if len(py_files) > 3:
                        components.append(Component(
                            name=py_dir.name,
                            type="module",
                            importance=4,
                            core_functions=["业务模块"]
                        ))
        
        return components[:10]  # 最多10个自定义组件
    
    def _analyze_source_structure(self) -> Dict:
        """分析源码结构"""
        structure = {
            "test_dirs": [],
            "config_dirs": [],
            "doc_dirs": []
        }
        
        # 查找测试目录
        for test_dir in self.root.rglob("*test*"):
            if test_dir.is_dir():
                structure["test_dirs"].append(str(test_dir.relative_to(self.root)))
        
        # 查找配置目录
        for conf_dir in self.root.rglob("*conf*"):
            if conf_dir.is_dir():
                structure["config_dirs"].append(str(conf_dir.relative_to(self.root)))
        
        # 查找文档目录
        for doc_dir in self.root.rglob("*doc*"):
            if doc_dir.is_dir():
                structure["doc_dirs"].append(str(doc_dir.relative_to(self.root)))
        
        return structure


class LLMClient:
    """大模型客户端（统一接口）"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.provider = config.get("provider", "anthropic")  # anthropic, openai, local
        
        # 根据provider初始化客户端
        if self.provider == "anthropic":
            from anthropic import Anthropic
            self.client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
            self.model = config.get("model", "claude-3-5-sonnet-20241022")
        elif self.provider == "openai":
            from openai import OpenAI
            self.client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
            self.model = config.get("model", "gpt-4")
        else:
            raise ValueError(f"Unsupported provider: {self.provider}")
    
    def generate(self, prompt: str, system_prompt: str = "") -> str:
        """生成文本"""
        if self.provider == "anthropic":
            response = self.client.messages.create(
                model=self.model,
                max_tokens=4096,
                system=system_prompt,
                messages=[{"role": "user", "content": prompt}]
            )
            return response.content[0].text
        elif self.provider == "openai":
            messages = []
            if system_prompt:
                messages.append({"role": "system", "content": system_prompt})
            messages.append({"role": "user", "content": prompt})
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=messages
            )
            return response.choices[0].message.content
    
    def generate_json(self, prompt: str, system_prompt: str = "") -> Dict:
        """生成JSON"""
        response = self.generate(prompt, system_prompt)
        
        # 提取JSON
        json_match = re.search(r'```json\s*(.*?)\s*```', response, re.DOTALL)
        if json_match:
            json_str = json_match.group(1)
        else:
            json_str = response
        
        try:
            return json.loads(json_str)
        except:
            # 尝试修复常见问题
            json_str = re.sub(r',\s*}', '}', json_str)
            json_str = re.sub(r',\s*]', ']', json_str)
            return json.loads(json_str)


class ScenarioDiscoverer:
    """场景发现器"""
    
    def __init__(self, llm: LLMClient, project_info: Dict):
        self.llm = llm
        self.project_info = project_info
    
    def discover(self, focus_areas: List[str] = None) -> List[Scenario]:
        """发现测试场景"""
        
        # 构建Prompt
        prompt = self._build_discovery_prompt(focus_areas)
        
        # 调用LLM
        system_prompt = """你是一个资深的测试架构师，擅长识别测试场景。
你的任务是分析项目信息，识别所有重要的测试场景。
输出必须是严格的JSON格式。"""
        
        result = self.llm.generate_json(prompt, system_prompt)
        
        # 解析结果
        scenarios = []
        for idx, item in enumerate(result.get("scenarios", [])):
            scenarios.append(Scenario(
                id=f"SC{idx+1:03d}",
                name=item.get("name", ""),
                type=item.get("type", "normal"),
                priority=item.get("priority", "P2"),
                components=item.get("components", []),
                description=item.get("description", ""),
                test_steps=item.get("test_steps", []),
                expected_result=item.get("expected_result", "")
            ))
        
        return scenarios
    
    def _build_discovery_prompt(self, focus_areas: List[str] = None) -> str:
        """构建场景发现Prompt"""
        
        prompt = f"""请分析以下项目信息，识别测试场景。

## 项目基本信息
- 名称：{self.project_info['basic_info']['name']}
- 类型：{self.project_info['basic_info']['type']}
- 构建工具：{self.project_info['basic_info']['build_tools']}
- 编程语言：{self.project_info['languages']}
- 使用框架：{self.project_info['frameworks']}

## 核心组件
"""
        for comp in self.project_info['components'][:10]:  # 只展示TOP 10
            prompt += f"- {comp.name} (重要性: {comp.importance}/10)\n"
            prompt += f"  核心功能: {', '.join(comp.core_functions)}\n"
        
        prompt += f"""
## 依赖关系
{json.dumps(self.project_info['dependencies'], indent=2, ensure_ascii=False)}
"""
        
        if focus_areas:
            prompt += f"\n## 重点关注的领域\n{chr(10).join(focus_areas)}\n"
        
        prompt += """
## 要求
1. 识别至少50个测试场景
2. 场景类型包括：
   - normal: 正常流程
   - exception: 异常流程
   - boundary: 边界条件
   - integration: 组件交互
   - performance: 性能相关
3. 优先级：
   - P0: 核心业务路径，必须测试
   - P1: 重要功能，应该测试
   - P2: 一般场景，可以测试
4. 特别关注：
   - 组件间的交互场景
   - 数据流转场景
   - 异常处理场景
   - 历史问题场景

## 输出格式
```json
{
  "scenarios": [
    {
      "name": "场景名称",
      "type": "场景类型",
      "priority": "优先级",
      "components": ["涉及组件"],
      "description": "详细描述",
      "test_steps": ["步骤1", "步骤2"],
      "expected_result": "预期结果"
    }
  ]
}
```

请开始分析并输出JSON。
"""
        return prompt


class TestGenerator:
    """测试脚本生成器"""
    
    def __init__(self, llm: LLMClient, project_info: Dict, config: Dict):
        self.llm = llm
        self.project_info = project_info
        self.config = config
        self.test_language = config.get("test_language", "auto")  # auto, java, python
    
    def generate(self, scenario: Scenario) -> str:
        """生成测试脚本"""
        
        # 确定测试语言
        language = self._detect_test_language(scenario)
        
        # 构建Prompt
        prompt = self._build_generation_prompt(scenario, language)
        
        # 调用LLM
        system_prompt = f"""你是一个资深的{language}测试开发工程师。
你的任务是根据测试场景生成完整可运行的测试代码。
输出必须是完整的代码，可以直接保存为文件执行。"""
        
        code = self.llm.generate(prompt, system_prompt)
        
        # 提取代码块
        code = self._extract_code(code, language)
        
        return code
    
    def _detect_test_language(self, scenario: Scenario) -> str:
        """检测测试语言"""
        if self.test_language != "auto":
            return self.test_language
        
        # 基于项目类型推断
        project_type = self.project_info['basic_info']['type']
        
        lang_map = {
            "java": "java",
            "scala": "java",  # Scala项目通常也用JUnit
            "python": "python"
        }
        
        return lang_map.get(project_type, "java")
    
    def _build_generation_prompt(self, scenario: Scenario, language: str) -> str:
        """构建生成Prompt"""
        
        prompt = f"""请生成测试脚本。

## 测试场景
- ID: {scenario.id}
- 名称: {scenario.name}
- 类型: {scenario.type}
- 优先级: {scenario.priority}
- 涉及组件: {', '.join(scenario.components)}
- 描述: {scenario.description}
- 测试步骤: {chr(10).join(f'{i+1}. {s}' for i, s in enumerate(scenario.test_steps))}
- 预期结果: {scenario.expected_result}

## 项目上下文
- 项目类型: {self.project_info['basic_info']['type']}
- 构建工具: {self.project_info['basic_info']['build_tools']}
- 使用框架: {self.project_info['frameworks']}
- 主要依赖: {json.dumps(self.project_info['dependencies'], ensure_ascii=False, indent=2)}
"""
        
        # 查找相关源码
        source_code = self._find_relevant_source(scenario)
        if source_code:
            prompt += f"""
## 相关源码参考
```
{source_code[:2000]}  # 限制长度
```
"""
        
        if language == "java":
            prompt += """
## 生成要求（Java）
1. 使用JUnit 4或5框架
2. 包含必要的import语句
3. 使用@Before/@BeforeEach初始化测试环境
4. 使用@After/@AfterEach清理测试环境
5. 测试方法命名：testXxx或shouldXxx
6. 使用断言验证结果
7. 如果依赖外部服务，使用Mock
8. 测试数据直接硬编码在测试中
9. 添加注释说明测试目的

## 输出格式
```java
package com.example.tests;

import org.junit.Test;
// ... 完整代码
```
"""
        elif language == "python":
            prompt += """
## 生成要求（Python）
1. 使用pytest框架
2. 使用@pytest.fixture管理测试资源
3. 测试方法命名：test_xxx
4. 使用assert断言
5. 添加docstring说明
6. 如果依赖外部服务，使用pytest-mock或unittest.mock
7. 测试数据直接硬编码
8. 使用@pytest.mark.parametrize处理多组数据

## 输出格式
```python
import pytest
# ... 完整代码
```
"""
        
        return prompt
    
    def _find_relevant_source(self, scenario: Scenario) -> str:
        """查找相关源码"""
        # 简化：基于场景关键词搜索
        # 实际可以实现更复杂的语义搜索
        
        keywords = scenario.components + scenario.name.split()
        source_snippets = []
        
        project_root = Path(self.config.get("project_root", "."))
        
        for keyword in keywords[:3]:  # 只搜索前3个关键词
            for ext in ['*.java', '*.scala', '*.py']:
                for file_path in project_root.rglob(ext):
                    if any(ignore in str(file_path) for ignore in ['test', 'Test', 'target', 'build']):
                        continue
                    
                    try:
                        content = file_path.read_text()
                        if keyword.lower() in content.lower():
                            # 找到相关代码，提取片段
                            lines = content.split('\n')
                            for i, line in enumerate(lines):
                                if keyword.lower() in line.lower():
                                    start = max(0, i - 5)
                                    end = min(len(lines), i + 20)
                                    snippet = '\n'.join(lines[start:end])
                                    source_snippets.append(f"// File: {file_path.name}\n{snippet}")
                                    break
                    except:
                        continue
                    
                    if len(source_snippets) >= 2:
                        break
                if len(source_snippets) >= 2:
                    break
            if len(source_snippets) >= 2:
                break
        
        return '\n\n'.join(source_snippets[:2]) if source_snippets else ""
    
    def _extract_code(self, text: str, language: str) -> str:
        """提取代码块"""
        # 尝试提取代码块
        lang_tag = "java" if language == "java" else "python"
        pattern = rf'```{lang_tag}\s*(.*?)\s*```'
        match = re.search(pattern, text, re.DOTALL)
        
        if match:
            return match.group(1)
        
        # 如果没有代码块标记，尝试提取代码内容
        lines = text.split('\n')
        code_lines = []
        in_code = False
        
        start_patterns = {
            "java": ['package ', 'import ', 'public class', 'public interface'],
            "python": ['import ', 'from ', 'def test_', 'class Test']
        }
        
        for line in lines:
            if any(line.strip().startswith(p) for p in start_patterns.get(language, [])):
                in_code = True
            
            if in_code:
                code_lines.append(line)
        
        return '\n'.join(code_lines) if code_lines else text


class TestExecutor:
    """测试执行器"""
    
    def __init__(self, config: Dict):
        self.config = config
    
    def run(self, test_file: Path) -> TestResult:
        """执行测试"""
        
        start_time = datetime.now()
        
        # 确定测试类型
        if test_file.suffix == '.java':
            result = self._run_java_test(test_file)
        elif test_file.suffix == '.py':
            result = self._run_python_test(test_file)
        else:
            result = TestResult(
                scenario_id="",
                test_file=str(test_file),
                status="error",
                duration=0,
                error_message=f"Unsupported test file type: {test_file.suffix}"
            )
        
        result.duration = (datetime.now() - start_time).total_seconds()
        result.timestamp = start_time.isoformat()
        
        return result
    
    def _run_java_test(self, test_file: Path) -> TestResult:
        """运行Java测试"""
        
        scenario_id = self._extract_scenario_id(test_file)
        
        # 使用Maven或Gradle运行测试
        try:
            # 假设项目根目录有pom.xml
            result = subprocess.run(
                ['mvn', 'test', f'-Dtest={test_file.stem}'],
                cwd=self.config.get('project_root', '.'),
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                return TestResult(
                    scenario_id=scenario_id,
                    test_file=str(test_file),
                    status="passed"
                )
            else:
                return TestResult(
                    scenario_id=scenario_id,
                    test_file=str(test_file),
                    status="failed",
                    error_message=result.stdout + result.stderr
                )
        except subprocess.TimeoutExpired:
            return TestResult(
                scenario_id=scenario_id,
                test_file=str(test_file),
                status="error",
                error_message="Test timeout"
            )
        except Exception as e:
            return TestResult(
                scenario_id=scenario_id,
                test_file=str(test_file),
                status="error",
                error_message=str(e)
            )
    
    def _run_python_test(self, test_file: Path) -> TestResult:
        """运行Python测试"""
        
        scenario_id = self._extract_scenario_id(test_file)
        
        try:
            result = subprocess.run(
                ['pytest', '-v', str(test_file)],
                capture_output=True,
                text=True,
                timeout=300
            )
            
            if result.returncode == 0:
                return TestResult(
                    scenario_id=scenario_id,
                    test_file=str(test_file),
                    status="passed"
                )
            else:
                return TestResult(
                    scenario_id=scenario_id,
                    test_file=str(test_file),
                    status="failed",
                    error_message=result.stdout + result.stderr
                )
        except subprocess.TimeoutExpired:
            return TestResult(
                scenario_id=scenario_id,
                test_file=str(test_file),
                status="error",
                error_message="Test timeout"
            )
        except Exception as e:
            return TestResult(
                scenario_id=scenario_id,
                test_file=str(test_file),
                status="error",
                error_message=str(e)
            )
    
    def _extract_scenario_id(self, test_file: Path) -> str:
        """从文件名提取场景ID"""
        # 假设文件名格式: SC001_xxx.py 或 SC001XxxTest.java
        match = re.search(r'(SC\d+)', test_file.name)
        return match.group(1) if match else ""


class TestFixer:
    """测试修复器"""
    
    def __init__(self, llm: LLMClient):
        self.llm = llm
    
    def fix(self, test_file: Path, error_message: str, scenario: Scenario) -> str:
        """修复失败的测试"""
        
        # 读取原测试代码
        original_code = test_file.read_text()
        
        # 构建Prompt
        prompt = f"""测试失败了，请修复。

## 测试场景
- ID: {scenario.id}
- 名称: {scenario.name}
- 描述: {scenario.description}
- 预期结果: {scenario.expected_result}

## 原测试代码
```
{original_code}
```

## 错误信息
```
{error_message}
```

## 要求
1. 分析失败原因
2. 修复测试代码
3. 确保测试能够通过
4. 保持测试的目的不变

## 输出
请直接输出修复后的完整测试代码，不需要解释。
"""
        
        system_prompt = """你是一个资深的测试工程师，擅长调试和修复测试代码。
请仔细分析错误信息，找出根本原因，然后修复测试代码。"""
        
        fixed_code = self.llm.generate(prompt, system_prompt)
        
        # 提取代码
        if test_file.suffix == '.java':
            fixed_code = self._extract_code(fixed_code, 'java')
        elif test_file.suffix == '.py':
            fixed_code = self._extract_code(fixed_code, 'python')
        
        return fixed_code
    
    def _extract_code(self, text: str, language: str) -> str:
        """提取代码"""
        lang_tag = "java" if language == "java" else "python"
        pattern = rf'```{lang_tag}\s*(.*?)\s*```'
        match = re.search(pattern, text, re.DOTALL)
        return match.group(1) if match else text


class TestAgent:
    """测试自动化Agent（统一入口）"""
    
    def __init__(self, project_root: str = "."):
        self.root = Path(project_root).resolve()
        self.config_dir = self.root / "hyx"
        self.config_file = self.config_dir / "project.yaml"
        self.config = self._load_config()
        self.llm = None  # 延迟初始化
        self.project_info = None  # 延迟加载
    
    def _load_config(self) -> Dict:
        """加载配置"""
        if self.config_file.exists():
            with open(self.config_file) as f:
                return yaml.safe_load(f)
        else:
            return {
                "project_root": str(self.root),
                "llm": {
                    "provider": "anthropic",
                    "model": "claude-3-5-sonnet-20241022"
                },
                "test_language": "auto",
                "output_dir": "hyx/generated_tests",
                "scenario_file": "hyx/scenarios.json"
            }
    
    def _save_config(self):
        """保存配置"""
        self.config_dir.mkdir(exist_ok=True)
        with open(self.config_file, 'w') as f:
            yaml.dump(self.config, f)
    
    def _init_llm(self):
        """初始化LLM"""
        if self.llm is None:
            llm_config = self.config.get("llm", {})
            self.llm = LLMClient(llm_config)
    
    def _load_project_info(self) -> Dict:
        """加载项目信息"""
        info_file = self.config_dir / "project_info.json"
        if info_file.exists():
            with open(info_file) as f:
                data = json.load(f)
                # 重建Component对象
                data['components'] = [Component(**c) for c in data.get('components', [])]
                return data
        return None
    
    def _save_project_info(self, info: Dict):
        """保存项目信息"""
        self.config_dir.mkdir(exist_ok=True)
        info_file = self.config_dir / "project_info.json"
        
        # 转换为可序列化格式
        data = {
            "basic_info": info["basic_info"],
            "languages": info["languages"],
            "frameworks": info["frameworks"],
            "dependencies": info["dependencies"],
            "source_structure": info["source_structure"],
            "components": [asdict(c) for c in info["components"]]
        }
        
        with open(info_file, 'w') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
    
    def _load_scenarios(self) -> List[Scenario]:
        """加载场景"""
        scenario_file = self.config_dir / "scenarios.json"
        if scenario_file.exists():
            with open(scenario_file) as f:
                data = json.load(f)
                return [Scenario(**s) for s in data]
        return []
    
    def _save_scenarios(self, scenarios: List[Scenario]):
        """保存场景"""
        self.config_dir.mkdir(exist_ok=True)
        scenario_file = self.config_dir / "scenarios.json"
        
        with open(scenario_file, 'w') as f:
            json.dump([asdict(s) for s in scenarios], f, indent=2, ensure_ascii=False)
    
    # ==================== 命令实现 ====================
    
    def cmd_init(self, args):
        """初始化项目配置"""
        print("=== 初始化测试自动化Agent ===")
        
        # 创建目录
        self.config_dir.mkdir(exist_ok=True)
        (self.config_dir / "generated_tests" / "java").mkdir(parents=True, exist_ok=True)
        (self.config_dir / "generated_tests" / "python").mkdir(parents=True, exist_ok=True)
        
        # 交互式配置
        print("\n请配置LLM（用于生成测试）:")
        print("1. anthropic (Claude)")
        print("2. openai (GPT-4)")
        
        choice = input("请选择 [1]: ").strip() or "1"
        
        if choice == "1":
            self.config["llm"] = {
                "provider": "anthropic",
                "model": "claude-3-5-sonnet-20241022"
            }
            print("\n请确保设置了环境变量 ANTHROPIC_API_KEY")
        elif choice == "2":
            self.config["llm"] = {
                "provider": "openai",
                "model": "gpt-4"
            }
            print("\n请确保设置了环境变量 OPENAI_API_KEY")
        
        print("\n测试语言:")
        print("1. auto (自动检测)")
        print("2. java")
        print("3. python")
        
        lang_choice = input("请选择 [1]: ").strip() or "1"
        lang_map = {"1": "auto", "2": "java", "3": "python"}
        self.config["test_language"] = lang_map.get(lang_choice, "auto")
        
        # 保存配置
        self._save_config()
        
        print("\n✓ 初始化完成！")
        print(f"  配置文件: {self.config_file}")
        print("\n下一步:")
        print("  1. 运行: python hyx/agent.py analyze")
        print("  2. 运行: python hyx/agent.py discover")
        print("  3. 运行: python hyx/agent.py generate")
    
    def cmd_analyze(self, args):
        """分析项目"""
        print("=== 分析项目结构 ===")
        
        analyzer = ProjectAnalyzer(self.root)
        project_info = analyzer.analyze()
        
        # 保存
        self._save_project_info(project_info)
        
        # 显示结果
        print(f"\n项目名称: {project_info['basic_info']['name']}")
        print(f"项目类型: {project_info['basic_info']['type']}")
        print(f"构建工具: {project_info['basic_info']['build_tools']}")
        
        print(f"\n编程语言:")
        for lang in project_info['languages']:
            print(f"  - {lang['language']}: {lang['file_count']} 个文件")
        
        print(f"\n使用框架: {', '.join(project_info['frameworks'])}")
        
        print(f"\n识别的组件 (TOP 10):")
        for comp in project_info['components'][:10]:
            print(f"  {comp.importance}. {comp.name} - {', '.join(comp.core_functions[:3])}")
        
        print(f"\n✓ 分析结果已保存到: {self.config_dir / 'project_info.json'}")
    
    def cmd_discover(self, args):
        """发现测试场景"""
        print("=== 发现测试场景 ===")
        
        # 加载项目信息
        project_info = self._load_project_info()
        if not project_info:
            print("错误: 请先运行 'python hyx/agent.py analyze'")
            return
        
        # 初始化LLM
        self._init_llm()
        
        # 发现场景
        discoverer = ScenarioDiscoverer(self.llm, project_info)
        
        print("\n正在识别测试场景...")
        scenarios = discoverer.discover(args.focus)
        
        # 保存
        self._save_scenarios(scenarios)
        
        # 显示结果
        print(f"\n共发现 {len(scenarios)} 个测试场景:")
        
        # 按优先级统计
        p0_count = len([s for s in scenarios if s.priority == 'P0'])
        p1_count = len([s for s in scenarios if s.priority == 'P1'])
        p2_count = len([s for s in scenarios if s.priority == 'P2'])
        
        print(f"  - P0 (必须): {p0_count} 个")
        print(f"  - P1 (重要): {p1_count} 个")
        print(f"  - P2 (一般): {p2_count} 个")
        
        # 按类型统计
        types = {}
        for s in scenarios:
            types[s.type] = types.get(s.type, 0) + 1
        
        print(f"\n按类型分布:")
        for t, count in sorted(types.items(), key=lambda x: x[1], reverse=True):
            print(f"  - {t}: {count} 个")
        
        print(f"\n示例场景 (前5个):")
        for s in scenarios[:5]:
            print(f"\n  [{s.id}] {s.name} ({s.priority})")
            print(f"      组件: {', '.join(s.components)}")
            print(f"      类型: {s.type}")
            print(f"      描述: {s.description[:50]}...")
        
        print(f"\n✓ 场景清单已保存到: {self.config_dir / 'scenarios.json'}")
        print("\n下一步:")
        print("  - 运行: python hyx/agent.py generate --priority P0")
    
    def cmd_generate(self, args):
        """生成测试脚本"""
        print("=== 生成测试脚本 ===")
        
        # 加载
        project_info = self._load_project_info()
        if not project_info:
            print("错误: 请先运行 analyze")
            return
        
        scenarios = self._load_scenarios()
        if not scenarios:
            print("错误: 请先运行 discover")
            return
        
        # 筛选
        if args.priority:
            scenarios = [s for s in scenarios if s.priority == args.priority]
        if args.type:
            scenarios = [s for s in scenarios if s.type == args.type]
        if args.limit:
            scenarios = scenarios[:args.limit]
        
        # 初始化LLM
        self._init_llm()
        
        # 生成器
        generator = TestGenerator(self.llm, project_info, self.config)
        
        print(f"\n将生成 {len(scenarios)} 个测试脚本...")
        
        output_dir = self.root / self.config["output_dir"]
        output_dir.mkdir(parents=True, exist_ok=True)
        
        generated = 0
        for idx, scenario in enumerate(scenarios, 1):
            print(f"\n[{idx}/{len(scenarios)}] 生成: {scenario.name}...")
            
            try:
                # 生成代码
                code = generator.generate(scenario)
                
                # 确定文件路径
                language = generator._detect_test_language(scenario)
                if language == "java":
                    file_path = output_dir / "java" / f"{scenario.id}_{scenario.name.replace(' ', '_')}Test.java"
                else:
                    file_path = output_dir / "python" / f"test_{scenario.id}_{scenario.name.replace(' ', '_')}.py"
                
                # 保存
                file_path.parent.mkdir(parents=True, exist_ok=True)
                file_path.write_text(code)
                
                print(f"  ✓ 已保存: {file_path.relative_to(self.root)}")
                generated += 1
                
            except Exception as e:
                print(f"  ✗ 失败: {e}")
        
        print(f"\n✓ 成功生成 {generated}/{len(scenarios)} 个测试脚本")
        print(f"  保存位置: {output_dir}")
        print("\n下一步:")
        print("  - 运行: python hyx/agent.py run")
    
    def cmd_run(self, args):
        """执行测试"""
        print("=== 执行测试 ===")
        
        output_dir = self.root / self.config["output_dir"]
        
        # 收集测试文件
        test_files = []
        
        if args.file:
            test_files = [Path(args.file)]
        else:
            test_files.extend(output_dir.rglob("*.java"))
            test_files.extend(output_dir.rglob("*.py"))
        
        if not test_files:
            print("没有找到测试文件")
            return
        
        print(f"\n找到 {len(test_files)} 个测试文件")
        
        # 执行器
        executor = TestExecutor(self.config)
        
        # 执行
        results = []
        passed = 0
        failed = 0
        
        for idx, test_file in enumerate(test_files, 1):
            print(f"\n[{idx}/{len(test_files)}] 执行: {test_file.name}...")
            
            result = executor.run(test_file)
            results.append(result)
            
            if result.status == "passed":
                print(f"  ✓ 通过 ({result.duration:.2f}s)")
                passed += 1
            else:
                print(f"  ✗ 失败: {result.error_message[:100]}")
                failed += 1
        
        # 保存结果
        result_file = self.config_dir / "test_results.json"
        with open(result_file, 'w') as f:
            json.dump([asdict(r) for r in results], f, indent=2)
        
        # 统计
        print(f"\n=== 测试结果 ===")
        print(f"总计: {len(results)} 个")
        print(f"通过: {passed} 个")
        print(f"失败: {failed} 个")
        print(f"成功率: {passed/len(results)*100:.1f}%")
        print(f"\n详细结果: {result_file}")
        
        if failed > 0:
            print("\n下一步:")
            print("  - 运行: python hyx/agent.py fix")
    
    def cmd_fix(self, args):
        """修复失败的测试"""
        print("=== 修复失败测试 ===")
        
        # 加载结果
        result_file = self.config_dir / "test_results.json"
        if not result_file.exists():
            print("错误: 请先运行测试")
            return
        
        with open(result_file) as f:
            results = [TestResult(**r) for r in json.load(f)]
        
        # 筛选失败的
        failed_results = [r for r in results if r.status != "passed"]
        
        if not failed_results:
            print("没有失败的测试")
            return
        
        # 加载场景
        scenarios = self._load_scenarios()
        scenario_map = {s.id: s for s in scenarios}
        
        # 初始化LLM
        self._init_llm()
        
        # 修复器
        fixer = TestFixer(self.llm)
        
        print(f"\n将修复 {len(failed_results)} 个失败测试...")
        
        fixed = 0
        for idx, result in enumerate(failed_results, 1):
            print(f"\n[{idx}/{len(failed_results)}] 修复: {result.test_file}...")
            
            scenario = scenario_map.get(result.scenario_id)
            if not scenario:
                print("  ✗ 找不到对应场景")
                continue
            
            try:
                test_file = Path(result.test_file)
                fixed_code = fixer.fix(test_file, result.error_message, scenario)
                
                # 保存修复后的代码
                test_file.write_text(fixed_code)
                print(f"  ✓ 已修复并保存")
                fixed += 1
                
            except Exception as e:
                print(f"  ✗ 修复失败: {e}")
        
        print(f"\n✓ 成功修复 {fixed}/{len(failed_results)} 个测试")
        print("\n下一步:")
        print("  - 运行: python hyx/agent.py run  # 重新测试")
    
    def cmd_all(self, args):
        """一键全流程"""
        print("=== 一键执行全流程 ===\n")
        
        self.cmd_analyze(args)
        print("\n" + "="*60 + "\n")
        
        self.cmd_discover(args)
        print("\n" + "="*60 + "\n")
        
        args.priority = "P0"  # 先生成P0
        args.type = None
        args.limit = 20
        self.cmd_generate(args)
        print("\n" + "="*60 + "\n")
        
        args.file = None
        self.cmd_run(args)


def main():
    parser = argparse.ArgumentParser(
        description="AI驱动的测试自动化Agent",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python hyx/agent.py init                  # 初始化配置
  python hyx/agent.py analyze               # 分析项目
  python hyx/agent.py discover              # 发现场景
  python hyx/agent.py generate --priority P0  # 生成P0测试
  python hyx/agent.py run                   # 执行测试
  python hyx/agent.py fix                   # 修复失败测试
  python hyx/agent.py all                   # 一键全流程
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="可用命令")
    
    # init
    parser_init = subparsers.add_parser("init", help="初始化配置")
    
    # analyze
    parser_analyze = subparsers.add_parser("analyze", help="分析项目结构")
    
    # discover
    parser_discover = subparsers.add_parser("discover", help="发现测试场景")
    parser_discover.add_argument("--focus", nargs="+", help="重点关注的领域")
    
    # generate
    parser_generate = subparsers.add_parser("generate", help="生成测试脚本")
    parser_generate.add_argument("--priority", choices=["P0", "P1", "P2"], help="生成指定优先级的测试")
    parser_generate.add_argument("--type", choices=["normal", "exception", "boundary", "integration", "performance"], help="生成指定类型的测试")
    parser_generate.add_argument("--limit", type=int, help="限制生成的数量")
    
    # run
    parser_run = subparsers.add_parser("run", help="执行测试")
    parser_run.add_argument("--file", help="执行指定的测试文件")
    
    # fix
    parser_fix = subparsers.add_parser("fix", help="修复失败测试")
    
    # all
    parser_all = subparsers.add_parser("all", help="一键全流程")
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # 创建Agent
    agent = TestAgent()
    
    # 执行命令
    cmd_method = getattr(agent, f"cmd_{args.command}", None)
    if cmd_method:
        cmd_method(args)
    else:
        print(f"未知命令: {args.command}")


if __name__ == "__main__":
    main()