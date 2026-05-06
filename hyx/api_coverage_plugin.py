#!/usr/bin/env python3
"""
API覆盖检测插件
功能：
1. 从已有API清单文档加载API定义
2. 扫描测试代码提取已测试API
3. 对比找出缺失API
4. 为缺失API生成测试场景和测试代码
"""

import os
import re
import json
import ast
from pathlib import Path
from typing import Dict, List, Set, Any, Tuple
from dataclasses import dataclass, asdict
import logging

# 导入框架基类
import sys
sys.path.append(str(Path(__file__).parent))
from framework import AnalyzerPlugin, DiscovererStrategy, GeneratorPlugin, Scenario, Interaction

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# ==================== 数据结构 ====================

@dataclass
class APIDefinition:
    """API定义"""
    component: str          # 组件名: spark, kafka, hbase等
    package: str            # 包名
    class_name: str         # 类名
    method_name: str        # 方法名
    method_signature: str   # 完整方法签名
    stability: str          # 稳定性标注: Stable, Evolving, Unstable, Private
    description: str        # API描述
    source_file: str        # 来源文档文件名
    is_static: bool = False # 是否静态方法
    is_constructor: bool = False # 是否构造方法
    return_type: str = ""   # 返回类型
    parameters: List[str] = None # 参数列表
    
    def __post_init__(self):
        if self.parameters is None:
            self.parameters = []
    
    def get_unique_id(self) -> str:
        """获取唯一标识"""
        return f"{self.component}.{self.class_name}.{self.method_name}"
    
    def to_dict(self) -> dict:
        return asdict(self)


# ==================== API清单解析器 ====================

class APIInventoryParser:
    """解析API清单Markdown文档"""
    
    def __init__(self):
        self.parsers = {
            'table': self._parse_table_format,
            'list': self._parse_list_format,
            'javadoc': self._parse_javadoc_format,
            'spark_format': self._parse_spark_format  # 新增Spark专用解析
        }
    
    def parse_file(self, file_path: Path) -> List[APIDefinition]:
        """解析单个文档"""
        apis = []
        
        if not file_path.exists():
            logger.warning(f"文件不存在: {file_path}")
            return apis
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 提取组件名
        component = self._extract_component_name(file_path.stem)
        
        # 根据组件类型选择解析策略
        if component == 'spark':
            apis.extend(self._parse_spark_format(content, component, file_path.name))
        else:
            # 其他组件使用通用解析
            apis.extend(self._parse_table_format(content, component, file_path.name))
        
        logger.info(f"从 {file_path.name} 解析出 {len(apis)} 个API")
        return apis
    
    def _extract_component_name(self, file_stem: str) -> str:
        """从文件名提取组件名"""
        component_map = {
            'spark': 'spark',
            'kafka': 'kafka',
            'hbase': 'hbase',
            'hadoop': 'hadoop',
            'iceberg': 'iceberg',
            'starrocks': 'starrocks'
        }
        
        for key in component_map:
            if key in file_stem.lower():
                return component_map[key]
        
        return 'unknown'
    
    def _parse_table_format(self, content: str, component: str, source_file: str) -> List[APIDefinition]:
        """解析表格格式的API定义"""
        apis = []
        
        # Java通用类型黑名单（不应作为API）
        java_builtin_types = {
            'String', 'Object', 'Integer', 'Long', 'Double', 'Float', 'Boolean',
            'Byte', 'Short', 'Character', 'Void', 'Class', 'Number',
            'Throwable', 'Exception', 'RuntimeException', 'Error',
            'List', 'Map', 'Set', 'Collection', 'Iterator', 'Iterable',
            'ArrayList', 'HashMap', 'HashSet', 'LinkedList',
            'Optional', 'Stream', 'Path', 'File', 'URL', 'URI',
            'Comparable', 'Serializable', 'Runnable', 'Callable',
            'T', 'K', 'V', 'R', 'E', 'N', 'S', 'U',  # 泛型参数
            'Methods', 'Value', 'Key', 'Annotation',  # 元数据标记
            'Deprecated', 'Override', 'SuppressWarnings',  # Java注解
            'Stable', 'Evolving', 'Unstable', 'Private', 'Public', 'LimitedPrivate',  # API稳定性标注
            'DeveloperApi', 'Experimental', 'Unstable', 'Evolving',  # Spark API标注
            'Method', 'Field', 'Constructor', 'Signature', 'Schema',  # 反射/元数据术语
            'Stability', 'Type', 'Name', 'Id', 'Version',  # 通用属性名
            'TimeoutException', 'InterruptedException', 'IllegalArgumentException',  # 常见异常（太通用）
            'IllegalStateException', 'UnsupportedOperationException',
        }
        
        # JavaDoc注解标记黑名单
        javadoc_tags = {
            'Deprecated', 'Override', 'SuppressWarnings', 'FunctionalInterface',
            'Stable', 'Evolving', 'Unstable', 'Private', 'Public',
            'DeveloperApi', 'Experimental', 'InterfaceStability', 'InterfaceAudience',
        }
        
        # 匹配表格行：| 类名 | 方法签名 | 描述 | ...
        # 支持多种表格格式
        
        # 格式1: | 接口/类 | 签名 | 用于方法 | 描述 |
        pattern1 = r'\|\s*([A-Z][a-zA-Z0-9]*)\s*\|\s*([a-zA-Z<>?,\[\]\s]+\s+\w+\([^)]*\))\s*\|'
        
        # 格式2: | 方法名 | 返回类型 | 参数 | 描述 |
        pattern2 = r'\|\s*(\w+)\s*\|\s*([a-zA-Z<>?,\[\]\s]+)\s*\|\s*([^|]*)\s*\|'
        
        # 格式3: 简单类名列表
        pattern3 = r'\|\s*([A-Z][a-zA-Z0-9]*)\s*\|'
        
        # 先尝试复杂格式
        matches = re.findall(pattern1, content)
        
        for match in matches:
            class_name, method_sig = match
            
            # 过滤通用类型和JavaDoc标记
            if class_name in java_builtin_types or class_name in javadoc_tags:
                continue
            
            # 过滤过短的类名（通常是泛型参数或错误匹配）
            if len(class_name) < 3:
                continue
            
            # 解析方法签名
            method_info = self._parse_method_signature(method_sig)
            
            # 提取稳定性标注
            stability = self._extract_stability_from_context(content, class_name)
            
            api = APIDefinition(
                component=component,
                package="",  # 后续填充
                class_name=class_name,
                method_name=method_info['method_name'],
                method_signature=method_sig,
                stability=stability,
                description=f"来源: {source_file}",
                source_file=source_file,
                is_static=method_info['is_static'],
                is_constructor=method_info['is_constructor'],
                return_type=method_info['return_type'],
                parameters=method_info['parameters']
            )
            
            apis.append(api)
        
        # 如果复杂格式没匹配到，尝试简单格式
        if not apis:
            class_matches = re.findall(pattern3, content)
            unique_classes = set(class_matches)
            
            for class_name in unique_classes:
                # 过滤通用类型和JavaDoc标记
                if class_name in java_builtin_types or class_name in javadoc_tags:
                    continue
                
                # 过滤过短的类名
                if len(class_name) < 3:
                    continue
                
                # 过滤常见错误匹配（如注解标记、元数据等）
                if class_name in ['Stable', 'Evolving', 'Unstable', 'Private', 'Public', 
                                 'Deprecated', 'DeveloperApi', 'Experimental']:
                    continue
                
                stability = self._extract_stability_from_context(content, class_name)
                
                api = APIDefinition(
                    component=component,
                    package="",
                    class_name=class_name,
                    method_name="",  # 类级别API
                    method_signature=class_name,
                    stability=stability,
                    description=f"来源: {source_file}",
                    source_file=source_file
                )
                
                apis.append(api)
        
        return apis
    
    def _parse_method_signature(self, signature: str) -> Dict:
        """解析方法签名"""
        info = {
            'method_name': '',
            'return_type': '',
            'parameters': [],
            'is_static': False,
            'is_constructor': False
        }
        
        # 清理签名
        signature = signature.strip()
        
        # 检测构造方法
        if 'new ' in signature or signature.startswith('public ') and '(' in signature:
            # 格式: public ClassName(Type1 param1, Type2 param2)
            constructor_match = re.match(r'public\s+(\w+)\s*\(([^)]*)\)', signature)
            if constructor_match:
                info['is_constructor'] = True
                info['method_name'] = constructor_match.group(1)
                params_str = constructor_match.group(2)
                info['parameters'] = self._parse_parameters(params_str)
                return info
        
        # 检测普通方法
        # 格式: ReturnType methodName(Type1 param1, Type2 param2)
        method_match = re.match(r'([a-zA-Z<>?,\[\]\s]+)\s+(\w+)\s*\(([^)]*)\)', signature)
        if method_match:
            info['return_type'] = method_match.group(1).strip()
            info['method_name'] = method_match.group(2)
            params_str = method_match.group(3)
            info['parameters'] = self._parse_parameters(params_str)
        
        return info
    
    def _parse_parameters(self, params_str: str) -> List[str]:
        """解析参数列表"""
        if not params_str.strip():
            return []
        
        params = []
        # 简化处理：按逗号分割
        for param in params_str.split(','):
            param = param.strip()
            if param:
                # 提取类型（去掉参数名）
                type_match = re.match(r'([a-zA-Z<>?,\[\]\s]+)', param)
                if type_match:
                    params.append(type_match.group(1).strip())
        
        return params
    
    def _extract_stability_from_context(self, content: str, class_name: str) -> str:
        """从文档上下文提取稳定性标注"""
        # 查找类名附近的稳定性标注
        
        # 匹配 @Stable, @Evolving, @Unstable, @Private
        stability_patterns = [
            (r'@Stable', 'Stable'),
            (r'@Evolving', 'Evolving'),
            (r'@Unstable', 'Unstable'),
            (r'@Private', 'Private'),
            (r'@LimitedPrivate', 'LimitedPrivate')
        ]
        
        # 在类名前后500字符范围内查找
        class_pos = content.find(class_name)
        if class_pos == -1:
            return 'Unknown'
        
        context_window = content[max(0, class_pos-500):min(len(content), class_pos+500)]
        
        for pattern, stability_name in stability_patterns:
            if re.search(pattern, context_window):
                return stability_name
        
        return 'Unknown'
    
    def _parse_list_format(self, content: str, component: str, source_file: str) -> List[APIDefinition]:
        """解析列表格式"""
        apis = []
        
        # 匹配列表项：- ClassName.methodName() 或 * ClassName
        patterns = [
            r'-\s+([A-Z][a-zA-Z0-9]*)\.(\w+)\([^)]*\)',
            r'-\s+([A-Z][a-zA-Z0-9]*)'
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            for match in matches:
                if len(match) == 2:
                    class_name, method_name = match
                    api = APIDefinition(
                        component=component,
                        package="",
                        class_name=class_name,
                        method_name=method_name,
                        method_signature=f"{class_name}.{method_name}()",
                        stability='Unknown',
                        description=f"来源: {source_file}",
                        source_file=source_file
                    )
                    apis.append(api)
                else:
                    class_name = match
                    api = APIDefinition(
                        component=component,
                        package="",
                        class_name=class_name,
                        method_name="",
                        method_signature=class_name,
                        stability='Unknown',
                        description=f"来源: {source_file}",
                        source_file=source_file
                    )
                    apis.append(api)
        
        return apis
    
    def _parse_javadoc_format(self, content: str, component: str, source_file: str) -> List[APIDefinition]:
        """解析JavaDoc格式"""
        # 暂不实现，留给后续扩展
        return []
    
    def _parse_spark_format(self, content: str, component: str, source_file: str) -> List[APIDefinition]:
        """
        解析Spark文档专用格式
        
        文档结构：
        ## 1.1 ClassName
        **稳定性**: Stable
        
        ### 算子分类
        | 方法名 | 参数 | 返回类型 | 描述 |
        | `methodName` | `params` | `returnType` | desc |
        """
        apis = []
        
        # 分割文档为类级别的块
        # 匹配类标题：## 1.x ClassName 或 ## 2.x ClassName
        class_blocks = re.split(r'\n##\s+\d+\.\d+\s+', content)
        
        # 提取类名和稳定性
        class_pattern = r'([A-Z][a-zA-Z0-9]+)(?:\s+\(接口\))?'
        
        for block in class_blocks[1:]:  # 第一个块是文档头部，跳过
            # 提取类名
            class_match = re.match(class_pattern, block)
            if not class_match:
                continue
            
            class_name = class_match.group(1)
            
            # 过滤通用类型
            java_builtin_types = {
                'String', 'Object', 'Integer', 'Long', 'Double', 'Float', 'Boolean',
                'Byte', 'Short', 'Character', 'Void', 'Class', 'Number',
                'Throwable', 'Exception', 'RuntimeException', 'Error',
                'List', 'Map', 'Set', 'Collection', 'Iterator', 'Iterable',
                'ArrayList', 'HashMap', 'HashSet', 'LinkedList',
                'Optional', 'Stream', 'Path', 'File', 'URL', 'URI',
                'Comparable', 'Serializable', 'Runnable', 'Callable',
                'T', 'K', 'V', 'R', 'E', 'N', 'S', 'U',
                'Method', 'Field', 'Constructor', 'Signature', 'Schema',
                'Stability', 'Type', 'Name', 'Id', 'Version',
            }
            
            if class_name in java_builtin_types:
                continue
            
            if len(class_name) < 3:
                continue
            
            # 提取稳定性标注
            stability = 'Unknown'
            
            # 查找稳定性标注
            stability_patterns = [
                (r'\*\*稳定性\*\*:\s*Stable', 'Stable'),
                (r'\*\*稳定性\*\*:\s*Evolving', 'Evolving'),
                (r'\*\*稳定性\*\*:\s*Experimental', 'Experimental'),
                (r'\*\*稳定性\*\*:\s*DeveloperApi', 'DeveloperApi'),
                (r'\*\*稳定性\*\*:\s*Deprecated', 'Deprecated'),
                (r'@Stable', 'Stable'),
                (r'@Evolving', 'Evolving'),
                (r'@Experimental', 'Experimental'),
                (r'@DeveloperApi', 'DeveloperApi'),
            ]
            
            for pattern, stab in stability_patterns:
                if re.search(pattern, block):
                    stability = stab
                    break
            
            # 提取包路径（如果有）
            package = ''
            package_match = re.search(r'\*\*包路径\*\*:\s*`([^`]+)`', block)
            if package_match:
                package = package_match.group(1)
            
            # 提取该类下的所有方法
            # Spark文档格式：| `methodName` | `params` | `returnType` | desc |
            method_pattern = r'\|\s*`(\w+)`\s*\|\s*`([^`]*)`\s*\|\s*`([^`]*)`\s*\|'
            method_matches = re.findall(method_pattern, block)
            
            # 如果找到方法，创建方法级别API
            for method_match in method_matches:
                method_name, params_str, return_type = method_match
                
                # 过滤非方法名（如"方法名"、"参数"等表格标题）
                if method_name in ['方法名', '参数', '返回类型', '描述', '常量名', '属性']:
                    continue
                
                # 构造方法签名
                method_signature = f"{return_type} {method_name}({params_str})"
                
                # 解析参数列表
                parameters = []
                if params_str.strip():
                    # 简化参数解析：按逗号分割，提取类型
                    for param in params_str.split(','):
                        param = param.strip()
                        if param:
                            # 提取类型部分（去掉参数名）
                            parts = param.split()
                            if len(parts) >= 1:
                                param_type = parts[0]
                                parameters.append(param_type)
                
                api = APIDefinition(
                    component=component,
                    package=package,
                    class_name=class_name,
                    method_name=method_name,
                    method_signature=method_signature,
                    stability=stability,
                    description=f"来源: {source_file}",
                    source_file=source_file,
                    is_static=False,  # Spark文档通常不区分static
                    is_constructor=False,
                    return_type=return_type,
                    parameters=parameters
                )
                
                apis.append(api)
            
            # 如果没找到方法，创建类级别API
            if not method_matches:
                api = APIDefinition(
                    component=component,
                    package=package,
                    class_name=class_name,
                    method_name="",  # 类级别
                    method_signature=class_name,
                    stability=stability,
                    description=f"来源: {source_file}",
                    source_file=source_file
                )
                
                apis.append(api)
        
        logger.info(f"Spark格式解析：{len(apis)} 个API（{len(set([api.class_name for api in apis]))} 个类）")
        return apis


# ==================== 测试代码扫描器 ====================

class TestCodeScanner:
    """扫描测试代码提取已测试API"""
    
    def __init__(self):
        self.tested_apis = set()
        self.tested_classes = set()
        self.test_files_count = 0
    
    def scan_project(self, project_root: Path) -> Dict:
        """扫描项目测试代码"""
        test_dirs = [
            'src/test/java',
            'sql/core/src/test/java',
            'sql/hive/src/test/java',
            'streaming/src/test/java',
            'mllib/src/test/java',
            'core/src/test/java',
            'connector/kafka-0-10-sql/src/test/java',
            'connector/protobuf/src/test/java'
        ]
        
        results = {
            'tested_apis': set(),
            'tested_classes': set(),
            'test_files_count': 0,
            'api_calls': {},  # API调用详情
            'test_methods': {}  # 测试方法详情
        }
        
        for test_dir in test_dirs:
            test_path = project_root / test_dir
            if test_path.exists():
                dir_results = self._scan_test_directory(test_path)
                results['tested_apis'].update(dir_results['tested_apis'])
                results['tested_classes'].update(dir_results['tested_classes'])
                results['test_files_count'] += dir_results['test_files_count']
                results['api_calls'].update(dir_results['api_calls'])
                results['test_methods'].update(dir_results['test_methods'])
        
        logger.info(f"扫描完成: {results['test_files_count']} 个测试文件, "
                   f"{len(results['tested_classes'])} 个已测试类")
        
        return results
    
    def _scan_test_directory(self, test_dir: Path) -> Dict:
        """扫描单个测试目录"""
        results = {
            'tested_apis': set(),
            'tested_classes': set(),
            'test_files_count': 0,
            'api_calls': {},
            'test_methods': {}
        }
        
        for java_file in test_dir.glob('**/*.java'):
            results['test_files_count'] += 1
            
            file_results = self._scan_java_file(java_file)
            results['tested_apis'].update(file_results['tested_apis'])
            results['tested_classes'].update(file_results['tested_classes'])
            results['api_calls'][java_file.name] = file_results['api_calls']
            results['test_methods'][java_file.name] = file_results['test_methods']
        
        return results
    
    def _scan_java_file(self, java_file: Path) -> Dict:
        """扫描单个Java文件"""
        results = {
            'tested_apis': set(),
            'tested_classes': set(),
            'api_calls': [],
            'test_methods': []
        }
        
        try:
            with open(java_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # 提取import语句
            imports = self._extract_imports(content)
            apache_imports = [imp for imp in imports if 'org.apache' in imp]
            
            # 提取类名
            for imp in apache_imports:
                class_name = imp.split('.')[-1]
                results['tested_classes'].add(class_name)
            
            # 提取方法调用
            calls = self._extract_method_calls(content)
            for call in calls:
                if call['class'] in results['tested_classes']:
                    api_id = f"{call['class']}.{call['method']}"
                    results['tested_apis'].add(api_id)
                    results['api_calls'].append(call)
            
            # 提取@Test标注的测试方法
            test_methods = self._extract_test_methods(content)
            results['test_methods'] = test_methods
            
        except Exception as e:
            logger.warning(f"解析文件失败 {java_file}: {e}")
        
        return results
    
    def _extract_imports(self, content: str) -> List[str]:
        """提取import语句"""
        imports = []
        pattern = r'import\s+([a-zA-Z.]+);'
        matches = re.findall(pattern, content)
        return matches
    
    def _extract_method_calls(self, content: str) -> List[Dict]:
        """提取方法调用"""
        calls = []
        
        # 匹配：object.method(...) 或 Class.staticMethod(...)
        patterns = [
            r'(\w+)\.(\w+)\(',  # instance.method()
            r'([A-Z][a-zA-Z0-9]*)\.(\w+)\(',  # Class.staticMethod()
        ]
        
        for pattern in patterns:
            matches = re.findall(pattern, content)
            for class_or_obj, method in matches:
                # 过滤常见关键字
                if method not in ['equals', 'hashCode', 'toString', 'getClass']:
                    calls.append({
                        'class': class_or_obj,
                        'method': method,
                        'type': 'call'
                    })
        
        return calls
    
    def _extract_test_methods(self, content: str) -> List[Dict]:
        """提取@Test标注的测试方法"""
        test_methods = []
        
        # 匹配: @Test public void methodName(...)
        pattern = r'@Test\s+public\s+void\s+(\w+)\s*\([^)]*\)'
        matches = re.findall(pattern, content)
        
        for method_name in matches:
            test_methods.append({
                'name': method_name,
                'annotations': ['@Test']
            })
        
        return test_methods


# ==================== API对比分析器 ====================

class APICoverageAnalyzer(AnalyzerPlugin):
    """API覆盖度分析器"""
    
    @property
    def name(self) -> str:
        return "api_coverage_analyzer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def analyze(self, project_root: Path, config: Dict) -> Dict:
        """执行API覆盖度分析"""
        logger.info("开始API覆盖度分析...")
        
        # 1. 加载API清单
        api_inventory = self._load_api_inventory(config)
        logger.info(f"加载API清单: {len(api_inventory)} 个API")
        
        # 2. 扫描测试代码
        tested_results = self._scan_tested_apis(project_root)
        logger.info(f"扫描测试代码: {tested_results['test_files_count']} 个文件")
        
        # 3. 对比缺失API
        missing_apis = self._compare_missing(api_inventory, tested_results)
        
        # 4. 计算覆盖率
        coverage_stats = self._calculate_coverage(api_inventory, tested_results, missing_apis)
        
        # 5. 生成报告
        report = {
            'total_apis': len(api_inventory),
            'tested_apis_count': len(tested_results['tested_classes']),
            'missing_apis_count': len(missing_apis),
            'coverage_rate': coverage_stats['overall_rate'],
            'coverage_by_component': coverage_stats['by_component'],
            'coverage_by_stability': coverage_stats['by_stability'],
            'missing_apis': [api.to_dict() for api in missing_apis],
            'api_inventory': [api.to_dict() for api in api_inventory],
            'tested_classes': list(tested_results['tested_classes']),
            'test_files_count': tested_results['test_files_count'],
            'analysis_time': datetime.now().isoformat()
        }
        
        # 保存结果
        self._save_report(report, config)
        
        logger.info(f"分析完成: 覆盖率 {coverage_stats['overall_rate']:.2%}")
        
        return report
    
    def _load_api_inventory(self, config: Dict) -> List[APIDefinition]:
        """加载API清单"""
        parser = APIInventoryParser()
        
        hyx_dir = Path(config.get('hyx_dir', '/home/h00517772/spark/hyx'))
        
        # 默认API清单文件
        inventory_files = config.get('api_inventory_files', [
            'spark_java_api_complete_list.md',
            'kafka_java_api_complete_list.md',
            'hbase_java_api_complete_list.md',
            'hadoop_java_api_complete_list.md',
            'iceberg_java_api_complete_list.md'
        ])
        
        all_apis = []
        
        for file_name in inventory_files:
            file_path = hyx_dir / file_name
            apis = parser.parse_file(file_path)
            all_apis.extend(apis)
        
        return all_apis
    
    def _scan_tested_apis(self, project_root: Path) -> Dict:
        """扫描已测试API"""
        scanner = TestCodeScanner()
        return scanner.scan_project(project_root)
    
    def _compare_missing(self, inventory: List[APIDefinition], tested: Dict) -> List[APIDefinition]:
        """对比缺失API"""
        missing = []
        
        tested_classes = tested['tested_classes']
        tested_apis = tested['tested_apis']
        
        for api in inventory:
            # 类级别匹配
            if api.class_name in tested_classes:
                continue
            
            # 方法级别匹配（如果有方法名）
            if api.method_name:
                api_id = f"{api.class_name}.{api.method_name}"
                if api_id in tested_apis:
                    continue
            
            missing.append(api)
        
        return missing
    
    def _calculate_coverage(self, inventory: List[APIDefinition], tested: Dict, missing: List[APIDefinition]) -> Dict:
        """计算覆盖率统计"""
        stats = {
            'overall_rate': 0.0,
            'by_component': {},
            'by_stability': {}
        }
        
        # 总体覆盖率
        if len(inventory) > 0:
            stats['overall_rate'] = (len(inventory) - len(missing)) / len(inventory)
        
        # 按组件统计
        component_counts = {}
        component_missing = {}
        
        for api in inventory:
            comp = api.component
            component_counts[comp] = component_counts.get(comp, 0) + 1
        
        for api in missing:
            comp = api.component
            component_missing[comp] = component_missing.get(comp, 0) + 1
        
        for comp in component_counts:
            tested_count = component_counts[comp] - component_missing.get(comp, 0)
            rate = tested_count / component_counts[comp] if component_counts[comp] > 0 else 0
            stats['by_component'][comp] = {
                'total': component_counts[comp],
                'tested': tested_count,
                'missing': component_missing.get(comp, 0),
                'rate': rate
            }
        
        # 按稳定性统计
        stability_counts = {}
        stability_missing = {}
        
        for api in inventory:
            stab = api.stability
            stability_counts[stab] = stability_counts.get(stab, 0) + 1
        
        for api in missing:
            stab = api.stability
            stability_missing[stab] = stability_missing.get(stab, 0) + 1
        
        for stab in stability_counts:
            tested_count = stability_counts[stab] - stability_missing.get(stab, 0)
            rate = tested_count / stability_counts[stab] if stability_counts[stab] > 0 else 0
            stats['by_stability'][stab] = {
                'total': stability_counts[stab],
                'tested': tested_count,
                'missing': stability_missing.get(stab, 0),
                'rate': rate
            }
        
        return stats
    
    def _save_report(self, report: Dict, config: Dict) -> None:
        """保存分析报告"""
        output_dir = Path(config.get('output_dir', '/home/h00517772/spark/hyx/results'))
        output_dir.mkdir(parents=True, exist_ok=True)
        
        # JSON报告
        json_file = output_dir / 'api_coverage_analysis.json'
        with open(json_file, 'w', encoding='utf-8') as f:
            json.dump(report, f, indent=2, ensure_ascii=False)
        logger.info(f"保存JSON报告: {json_file}")
        
        # Markdown报告
        md_file = output_dir / 'API_Coverage_Report.md'
        self._write_markdown_report(report, md_file)
        logger.info(f"保存Markdown报告: {md_file}")
    
    def _write_markdown_report(self, report: Dict, md_file: Path) -> None:
        """生成Markdown报告"""
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write("# API覆盖度分析报告\n\n")
            f.write(f"> 分析时间: {report['analysis_time']}\n\n")
            
            f.write("## 一、总体统计\n\n")
            f.write(f"| 指标 | 数值 |\n")
            f.write(f"|------|------|\n")
            f.write(f"| 总API数 | {report['total_apis']} |\n")
            f.write(f"| 已测试API数 | {report['tested_apis_count']} |\n")
            f.write(f"| 缺失API数 | {report['missing_apis_count']} |\n")
            f.write(f"| 覆盖率 | {report['coverage_rate']:.2%} |\n")
            f.write(f"| 测试文件数 | {report['test_files_count']} |\n\n")
            
            f.write("## 二、按组件统计\n\n")
            f.write("| 组件 | 总API数 | 已测试 | 缺失 | 覆盖率 |\n")
            f.write("|------|--------|--------|------|--------|\n")
            for comp, stats in report['coverage_by_component'].items():
                f.write(f"| {comp} | {stats['total']} | {stats['tested']} | "
                       f"{stats['missing']} | {stats['rate']:.2%} |\n")
            f.write("\n")
            
            f.write("## 三、按稳定性统计\n\n")
            f.write("| 稳定性 | 总API数 | 已测试 | 缺失 | 覆盖率 |\n")
            f.write("|--------|--------|--------|------|--------|\n")
            for stab, stats in report['coverage_by_stability'].items():
                f.write(f"| {stab} | {stats['total']} | {stats['tested']} | "
                       f"{stats['missing']} | {stats['rate']:.2%} |\n")
            f.write("\n")
            
            # 新增：已测试API列表
            f.write("## 四、已测试API对比\n\n")
            f.write("> 已测试的类和方法，可对比API清单验证\n\n")
            
            tested_classes = sorted(report['tested_classes'])
            
            # 分组显示已测试API
            f.write("### 4.1 已测试类列表\n\n")
            f.write(f"共 {len(tested_classes)} 个类在测试文件中被引用\n\n")
            
            # 每行显示5个类名
            for i in range(0, len(tested_classes), 5):
                batch = tested_classes[i:i+5]
                f.write("|".join([f" `{cls}` " for cls in batch]) + "\n")
            
            f.write("\n### 4.2 已测试API详细对比（示例）\n\n")
            f.write("| 类名 | 测试覆盖的方法（推测） | API清单定义的方法 |\n")
            f.write("|------|----------------------|------------------|\n")
            
            # 对比前10个已测试类
            api_inventory = report['api_inventory']
            
            for tested_class in tested_classes[:10]:
                # 查找API清单中该类的所有方法
                class_apis = [api for api in api_inventory if api['class_name'] == tested_class]
                
                if class_apis:
                    # API清单中的方法列表
                    api_methods = [api['method_name'] for api in class_apis if api['method_name']]
                    
                    if api_methods:
                        # 显示对比
                        f.write(f"| **{tested_class}** | ")
                        f.write(f"（类在测试中被引用） | ")
                        f.write(f"{', '.join(api_methods[:5])}... ({len(api_methods)}个方法) |\n")
                    else:
                        f.write(f"| **{tested_class}** | （类在测试中被引用） | （类级别API） |\n")
            
            f.write("\n")
            
            # 新增：缺失API完整列表（包含签名）
            f.write("## 五、缺失API详细列表（优先级P0）\n\n")
            f.write("> 包含完整方法签名，便于对比和测试生成\n\n")
            
            priority_order = {'Stable': 'P0', 'Evolving': 'P1', 'Unknown': 'P2', 'Private': 'P3', 'Deprecated': 'P3'}
            
            sorted_missing = sorted(report['missing_apis'], 
                                   key=lambda x: priority_order.get(x['stability'], 'P2'))
            
            # 只显示P0优先级（Stable）
            p0_missing = [api for api in sorted_missing if priority_order.get(api['stability'], 'P2') == 'P0']
            
            f.write(f"### 5.1 P0优先级缺失API（Stable）\n\n")
            f.write(f"共 {len(p0_missing)} 个核心API缺失，应优先补充测试\n\n")
            
            f.write("| 组件 | 类名 | 方法名 | 方法签名 | 返回类型 | 参数 | 稳定性 |\n")
            f.write("|------|------|--------|----------|----------|------|--------|\n")
            
            for api in p0_missing[:30]:  # 限制显示30个避免过长
                method_name = api['method_name'] if api['method_name'] else '(类级别)'
                method_sig = api.get('method_signature', api['class_name'])
                return_type = api.get('return_type', '')
                params = ', '.join(api.get('parameters', [])) if api.get('parameters') else ''
                
                # 清理签名显示（去掉过长内容）
                if len(method_sig) > 60:
                    method_sig_display = method_sig[:60] + "..."
                else:
                    method_sig_display = method_sig
                
                f.write(f"| {api['component']} | **{api['class_name']}** | "
                       f"`{method_name}` | `{method_sig_display}` | "
                       f"`{return_type}` | `{params}` | {api['stability']} |\n")
            
            f.write("\n")
            if len(p0_missing) > 30:
                f.write(f"> 仅显示前30个，共 {len(p0_missing)} 个P0缺失API\n\n")
            
            # 新增：P1优先级缺失API
            p1_missing = [api for api in sorted_missing if priority_order.get(api['stability'], 'P2') == 'P1']
            
            if p1_missing:
                f.write(f"### 5.2 P1优先级缺失API（Evolving）\n\n")
                f.write(f"共 {len(p1_missing)} 个演进API缺失\n\n")
                
                f.write("| 组件 | 类名 | 方法名 | 方法签名 |\n")
                f.write("|------|------|--------|----------|\n")
                
                for api in p1_missing[:20]:
                    method_name = api['method_name'] if api['method_name'] else '(类级别)'
                    method_sig = api.get('method_signature', api['class_name'])
                    
                    f.write(f"| {api['component']} | {api['class_name']} | "
                           f"`{method_name}` | `{method_sig}` |\n")
                
                f.write("\n")
            
            # 新增：所有缺失API完整清单（附录）
            f.write("## 六、所有缺失API完整清单\n\n")
            f.write(f"共 {len(report['missing_apis'])} 个API缺失\n\n")
            
            # 按组件分组
            missing_by_component = {}
            for api in report['missing_apis']:
                comp = api['component']
                if comp not in missing_by_component:
                    missing_by_component[comp] = []
                missing_by_component[comp].append(api)
            
            for comp in sorted(missing_by_component.keys()):
                comp_missing = missing_by_component[comp]
                f.write(f"### {comp}组件缺失API ({len(comp_missing)}个)\n\n")
                
                f.write("| 类名 | 方法名 | 方法签名 | 稳定性 | 优先级 |\n")
                f.write("|------|--------|----------|--------|--------|\n")
                
                for api in comp_missing[:50]:  # 每个组件最多50个
                    method_name = api['method_name'] if api['method_name'] else '(类级别)'
                    method_sig = api.get('method_signature', api['class_name'])
                    priority = priority_order.get(api['stability'], 'P2')
                    
                    f.write(f"| {api['class_name']} | `{method_name}` | "
                           f"`{method_sig[:50]}` | {api['stability']} | {priority} |\n")
                
                if len(comp_missing) > 50:
                    f.write(f"\n> 仅显示前50个，该组件共 {len(comp_missing)} 个缺失API\n")
                
                f.write("\n")
    
    def execute(self, context: Dict) -> Any:
        """执行插件（框架接口）"""
        project_root = Path(context.get('project_root', '/home/h00517772/spark'))
        config = context.get('config', {})
        return self.analyze(project_root, config)


# ==================== API场景发现策略 ====================

class APICoverageDiscoverer(DiscovererStrategy):
    """API覆盖场景发现策略"""
    
    @property
    def name(self) -> str:
        return "api_coverage_discoverer"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def discover(self, context: Dict) -> List[Scenario]:
        """发现API缺失场景"""
        scenarios = []
        
        # 从context获取缺失API列表
        analysis_result = context.get('api_coverage_analysis', {})
        missing_apis = analysis_result.get('missing_apis', [])
        
        logger.info(f"发现 {len(missing_apis)} 个缺失API场景")
        
        for api_def in missing_apis:
            priority = self._determine_priority(api_def['stability'])
            
            scenario = Scenario(
                id=f"API_{api_def['component']}_{api_def['class_name']}_{api_def['method_name']}",
                name=f"测试{api_def['class_name']}.{api_def['method_name']}方法",
                type="api_coverage",
                priority=priority,
                components=[api_def['component']],
                interactions=[],
                description=f"""
缺少API测试：
- 组件: {api_def['component']}
- 类: {api_def['class_name']}
- 方法: {api_def['method_name']}
- 签名: {api_def['method_signature']}
- 稳定性: {api_def['stability']}
- 优先级: {priority}
                """.strip(),
                test_steps=[
                    f"准备{api_def['class_name']}实例或Mock对象",
                    f"调用{api_def['method_name']}方法",
                    "验证返回结果是否符合预期",
                    "测试异常处理（null参数、边界值）",
                    "测试方法副作用（如有）"
                ],
                expected_result="方法正常执行，返回结果正确，异常处理完善",
                source_code_ref=api_def['source_file'],
                discovery_strategy="api_coverage",
                metadata={
                    'api_definition': api_def,
                    'test_type': 'unit_test',
                    'stability': api_def['stability']
                }
            )
            
            scenarios.append(scenario)
        
        # 按优先级排序
        scenarios.sort(key=lambda s: s.priority)
        
        return scenarios
    
    def _determine_priority(self, stability: str) -> str:
        """根据稳定性确定测试优先级"""
        priority_map = {
            'Stable': 'P0',      # 核心稳定API，必须测试
            'Evolving': 'P1',    # 演进API，优先测试
            'Unknown': 'P2',     # 未知稳定性，次优先
            'Private': 'P3',     # 内部API，可选测试
            'LimitedPrivate': 'P3'
        }
        
        return priority_map.get(stability, 'P2')
    
    def get_interactions(self, context: Dict) -> List[Interaction]:
        """识别API相关交互"""
        interactions = []
        
        analysis_result = context.get('api_coverage_analysis', {})
        api_inventory = analysis_result.get('api_inventory', [])
        
        # 识别跨组件API调用
        # 例如：Spark调用HBase API
        
        for api_def in api_inventory:
            if api_def['component'] != 'spark':
                # 可能是其他组件API被Spark调用
                interaction = Interaction(
                    source='spark',
                    target=api_def['component'],
                    interaction_type='api_call',
                    interface=f"{api_def['class_name']}.{api_def['method_name']}",
                    data_format=api_def['return_type'] if api_def.get('return_type') else '',
                    frequency=0,
                    critical_path=False,
                    conditions=[]
                )
                interactions.append(interaction)
        
        return interactions
    
    def execute(self, context: Dict) -> Any:
        """执行插件（框架接口）"""
        return self.discover(context)


# ==================== API测试生成器 ====================

class APITestGenerator(GeneratorPlugin):
    """API测试代码生成器"""
    
    @property
    def name(self) -> str:
        return "api_test_generator"
    
    @property
    def version(self) -> str:
        return "1.0.0"
    
    def generate(self, scenario: Scenario, context: Dict) -> str:
        """生成API测试代码"""
        if scenario.type != "api_coverage":
            return ""
        
        # 提取API定义
        api_def = scenario.metadata.get('api_definition', {})
        
        # 生成测试代码
        test_code = self._generate_junit_test(api_def, scenario)
        
        return test_code
    
    def _generate_junit_test(self, api_def: Dict, scenario: Scenario) -> str:
        """生成JUnit测试代码"""
        class_name = api_def.get('class_name', 'UnknownClass')
        method_name = api_def.get('method_name', 'unknownMethod')
        component = api_def.get('component', 'unknown')
        stability = api_def.get('stability', 'Unknown')
        return_type = api_def.get('return_type', 'void')
        parameters = api_def.get('parameters', [])
        
        # 构造测试类名
        test_class_name = f"{class_name}APITest"
        
        # 生成测试代码模板
        test_code = f"""package org.apache.{component}.api.test;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

/**
 * API测试: {class_name}.{method_name}
 * 
 * 场景ID: {scenario.id}
 * 优先级: {scenario.priority}
 * 稳定性: {stability}
 * 描述: {scenario.description}
 * 
 * 自动生成时间: {datetime.now().isoformat()}
 */
@DisplayName("{scenario.name}")
public class {test_class_name} {{
    
    private {class_name} instance;
    
    @BeforeEach
    public void setUp() {{
        // TODO: 初始化{class_name}实例
        // 根据实际情况选择：
        // 1. 直接new实例（如果有public构造方法）
        // 2. 使用工厂方法获取实例
        // 3. Mock对象（如果依赖复杂）
        instance = createInstance();
    }}
    
    /**
     * 测试正常调用
     */
    @Test
    @DisplayName("正常调用测试")
    public void test{method_name}_NormalCase() {{
        // 准备测试数据
        Object testData = prepareTestData();
        
        // 执行方法调用
        {return_type} result = instance.{method_name}(testData);
        
        // 验证结果
        verifyResult(result);
    }}
    
    /**
     * 测试null参数处理
     */
    @Test
    @DisplayName("null参数测试")
    public void test{method_name}_NullInput() {{
        // 测试null输入的处理
        assertThrows(Exception.class, () -> {{
            instance.{method_name}(null);
        }}, "应抛出异常处理null参数");
    }}
    
    /**
     * 测试边界值
     */
    @Test
    @DisplayName("边界值测试")
    public void test{method_name}_BoundaryValues() {{
        // 根据参数类型生成边界值
        // - 数值类型: MIN, MAX, 0
        // - 字符串: 空串, 长字符串, 特殊字符
        // - 集合类型: 空, 单元素, 大集合
        
        Object boundaryData = prepareBoundaryData();
        
        {return_type} result = instance.{method_name}(boundaryData);
        
        verifyResult(result);
    }}
    
    /**
     * 测试异常场景
     */
    @Test
    @DisplayName("异常场景测试")
    public void test{method_name}_ExceptionHandling() {{
        // 测试异常情况的处理
        Object invalidData = prepareInvalidData();
        
        assertThrows(Exception.class, () -> {{
            instance.{method_name}(invalidData);
        }}, "应正确处理异常情况");
    }}
    
    // ==================== 辅助方法 ====================
    
    private {class_name} createInstance() {{
        // TODO: 实现实例创建逻辑
        // return new {class_name}();
        return null; // 暂时返回null，需补充实现
    }}
    
    private Object prepareTestData() {{
        // TODO: 根据API文档准备合适的测试数据
        return null;
    }}
    
    private Object prepareBoundaryData() {{
        // TODO: 准备边界值数据
        return null;
    }}
    
    private Object prepareInvalidData() {{
        // TODO: 准备非法数据
        return null;
    }}
    
    private void verifyResult({return_type} result) {{
        // TODO: 实现结果验证逻辑
        if ("void" != "{return_type}") {{
            assertNotNull(result, "结果不应为null");
        }}
    }}
}}
"""
        
        return test_code
    
    def execute(self, context: Dict) -> Any:
        """执行插件（框架接口）"""
        scenario = context.get('scenario')
        if scenario:
            return self.generate(scenario, context)
        return ""


# ==================== 导入datetime ====================
from datetime import datetime


# ==================== 测试入口 ====================

if __name__ == "__main__":
    """独立测试API覆盖分析"""
    
    # 配置
    config = {
        'hyx_dir': '/home/h00517772/spark/hyx',
        'output_dir': '/home/h00517772/spark/hyx/results',
        'project_root': '/home/h00517772/spark',
        'api_inventory_files': [
            'spark_java_api_complete_list.md',
            'kafka_java_api_complete_list.md'
        ]
    }
    
    # 执行分析
    analyzer = APICoverageAnalyzer()
    result = analyzer.analyze(Path(config['project_root']), config)
    
    print(f"\n分析结果:")
    print(f"总API数: {result['total_apis']}")
    print(f"已测试: {result['tested_apis_count']}")
    print(f"缺失: {result['missing_apis_count']}")
    print(f"覆盖率: {result['coverage_rate']:.2%}")
    
    print(f"\n缺失API示例（前10个）:")
    for api in result['missing_apis'][:10]:
        print(f"  - {api['component']}.{api['class_name']}.{api['method_name']} ({api['stability']})")
    
    print(f"\n报告已保存到: {config['output_dir']}")