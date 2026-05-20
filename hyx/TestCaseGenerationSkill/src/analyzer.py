#!/usr/bin/env python3
"""
分析器模块 - 合并交互解析和种子分析
"""

import yaml
import logging
from typing import Dict, Any, List
from pathlib import Path


# ========== 交互描述解析器 ==========

class InteractionParser:
    """
    解析组件交互描述
    
    功能：
    - 验证交互描述格式
    - 提取组件列表
    - 提取交互流程
    - 提取数据Schema
    - 提取约束条件
    """
    
    def __init__(self, template: str = None):
        """初始化解析器"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.template = template
        
        # 加载模板用于验证
        if template:
            self.template_structure = yaml.safe_load(template)
    
    def parse(self, interaction_description: Dict) -> Dict:
        """
        解析交互描述
        
        Args:
            interaction_description: 原始交互描述
        
        Returns:
            {
                'name': str,
                'components': List[str],
                'flow': List[Dict],
                'data_schema': Dict,
                'constraints': Dict,
                'interaction_type': str
            }
        """
        self.logger.info("开始解析交互描述")
        
        # 验证必填字段
        self._validate_required_fields(interaction_description)
        
        # 提取基本信息
        parsed = {
            'name': self._extract_name(interaction_description),
            'components': self._extract_components(interaction_description),
            'flow': self._extract_flow(interaction_description),
            'data_schema': self._extract_data_schema(interaction_description),
            'constraints': self._extract_constraints(interaction_description),
            'interaction_type': self._determine_interaction_type(interaction_description)
        }
        
        self.logger.info(f"解析完成，组件: {parsed['components']}, 类型: {parsed['interaction_type']}")
        return parsed
    
    def _validate_required_fields(self, description: Dict):
        """验证必填字段"""
        required_fields = ['interaction', 'data_schema', 'constraints']
        
        for field in required_fields:
            if field not in description:
                raise ValueError(f"缺少必填字段: {field}")
        
        # 验证interaction子字段
        interaction = description['interaction']
        interaction_required = ['name', 'components', 'flow']
        
        for field in interaction_required:
            if field not in interaction:
                raise ValueError(f"interaction缺少必填字段: {field}")
    
    def _extract_name(self, description: Dict) -> str:
        """提取交互名称"""
        return description['interaction']['name']
    
    def _extract_components(self, description: Dict) -> List[str]:
        """提取组件列表"""
        components = description['interaction']['components']
        
        # 验证组件数量
        if len(components) < 2:
            self.logger.warning("组件数量少于2，可能不是交互场景")
        
        return components
    
    def _extract_flow(self, description: Dict) -> List[Dict]:
        """提取交互流程"""
        flow = description['interaction']['flow']
        
        parsed_flow = []
        for step in flow:
            parsed_step = {
                'step': step.get('step', ''),
                'component': step.get('component', ''),
                'action': step.get('action', ''),
                'input': step.get('input', ''),
                'output': step.get('output', ''),
                'description': step.get('description', ''),
                'timeout': step.get('timeout', 0),
                'retry_count': step.get('retry_count', 0)
            }
            parsed_flow.append(parsed_step)
        
        return parsed_flow
    
    def _extract_data_schema(self, description: Dict) -> Dict:
        """提取数据Schema"""
        data_schema = description['data_schema']
        
        return {
            'input_data': data_schema.get('input_data', {}),
            'intermediate_data': data_schema.get('intermediate_data', {}),
            'output_data': data_schema.get('output_data', {})
        }
    
    def _extract_constraints(self, description: Dict) -> Dict:
        """提取约束条件"""
        constraints = description['constraints']
        
        return {
            'data_constraints': constraints.get('data_constraints', []),
            'performance_constraints': constraints.get('performance_constraints', []),
            'reliability_constraints': constraints.get('reliability_constraints', [])
        }
    
    def _determine_interaction_type(self, description: Dict) -> str:
        """确定交互类型"""
        flow = description['interaction']['flow']
        
        # 根据flow特征判断交互类型
        if self._is_data_flow(flow):
            return 'data_flow'
        elif self._is_state_sync(flow):
            return 'state_sync'
        elif self._is_event_trigger(flow):
            return 'event_trigger'
        elif self._is_query_access(flow):
            return 'query_access'
        elif self._is_config_linkage(flow):
            return 'config_linkage'
        else:
            return 'data_flow'  # 默认
    
    def _is_data_flow(self, flow: List[Dict]) -> bool:
        """判断是否为数据流"""
        keywords = ['send', 'receive', 'consume', 'produce', 'write', 'read']
        for step in flow:
            action = step.get('action', '').lower()
            if any(kw in action for kw in keywords):
                return True
        return False
    
    def _is_state_sync(self, flow: List[Dict]) -> bool:
        """判断是否为状态同步"""
        keywords = ['sync', 'update', 'refresh', 'consistency']
        for step in flow:
            action = step.get('action', '').lower()
            if any(kw in action for kw in keywords):
                return True
        return False
    
    def _is_event_trigger(self, flow: List[Dict]) -> bool:
        """判断是否为事件触发"""
        keywords = ['trigger', 'event', 'notify', 'callback']
        for step in flow:
            action = step.get('action', '').lower()
            if any(kw in action for kw in keywords):
                return True
        return False
    
    def _is_query_access(self, flow: List[Dict]) -> bool:
        """判断是否为查询访问"""
        keywords = ['query', 'select', 'fetch', 'get', 'lookup']
        for step in flow:
            action = step.get('action', '').lower()
            if any(kw in action for kw in keywords):
                return True
        return False
    
    def _is_config_linkage(self, flow: List[Dict]) -> bool:
        """判断是否为配置联动"""
        keywords = ['config', 'setting', 'parameter', 'option']
        for step in flow:
            action = step.get('action', '').lower()
            if any(kw in action for kw in keywords):
                return True
        return False


# ========== 种子用例分析器 ==========

class SeedAnalyzer:
    """
    种子用例分析器
    
    功能：
    - 分析种子用例的交互模式
    - 提取数据特征
    - 提取验证模式
    - 提取异常处理模式
    - 提取清理模式
    """
    
    def __init__(self):
        """初始化分析器"""
        self.logger = logging.getLogger(self.__class__.__name__)
    
    def analyze(self, seed_cases: List[Dict]) -> Dict:
        """
        分析种子用例
        
        Args:
            seed_cases: 种子用例列表
        
        Returns:
            {
                'interaction_patterns': Dict,
                'data_patterns': Dict,
                'assertion_patterns': Dict,
                'error_patterns': Dict,
                'cleanup_patterns': Dict,
                'generalization_strategies': Dict
            }
        """
        self.logger.info(f"开始分析种子用例，共{len(seed_cases)}个")
        
        patterns = {
            'interaction_patterns': self._extract_interaction_patterns(seed_cases),
            'data_patterns': self._extract_data_patterns(seed_cases),
            'assertion_patterns': self._extract_assertion_patterns(seed_cases),
            'error_patterns': self._extract_error_patterns(seed_cases),
            'cleanup_patterns': self._extract_cleanup_patterns(seed_cases),
            'generalization_strategies': self._extract_generalization_strategies(seed_cases)
        }
        
        self.logger.info("种子用例分析完成")
        return patterns
    
    def _extract_interaction_patterns(self, seed_cases: List[Dict]) -> Dict:
        """提取交互模式"""
        patterns = {
            'flow_sequence': [],
            'component_sequence': [],
            'action_sequence': []
        }
        
        for case in seed_cases:
            test_steps = case.get('test_steps', [])
            
            # 提取流程序列
            flow_seq = [step.get('action', '') for step in test_steps]
            patterns['flow_sequence'].append(flow_seq)
            
            # 提取组件序列
            comp_seq = [step.get('component', '') for step in test_steps]
            patterns['component_sequence'].append(comp_seq)
            
            # 提取动作序列
            action_seq = [(step.get('component', ''), step.get('action', '')) 
                          for step in test_steps]
            patterns['action_sequence'].append(action_seq)
        
        return patterns
    
    def _extract_data_patterns(self, seed_cases: List[Dict]) -> Dict:
        """提取数据模式"""
        patterns = {
            'input_patterns': [],
            'output_patterns': [],
            'data_sizes': [],
            'data_formats': []
        }
        
        for case in seed_cases:
            test_data = case.get('test_data', {})
            
            # 输入数据模式
            input_data = test_data.get('input', {})
            patterns['input_patterns'].append(input_data)
            
            # 输出数据模式
            output_data = test_data.get('expected_output', {})
            patterns['output_patterns'].append(output_data)
            
            # 数据大小
            if 'message_count' in input_data:
                patterns['data_sizes'].append(input_data['message_count'])
            
            # 数据格式
            if 'message_format' in test_data.get('input', {}):
                patterns['data_formats'].append('JSON')
        
        return patterns
    
    def _extract_assertion_patterns(self, seed_cases: List[Dict]) -> Dict:
        """提取验证模式"""
        patterns = {
            'assertion_types': [],
            'assertion_sequence': [],
            'assertion_count': []
        }
        
        for case in seed_cases:
            assertions = case.get('assertions', [])
            
            # 验证类型
            assertion_types = [a.get('assertion_type', '') for a in assertions]
            patterns['assertion_types'].append(assertion_types)
            
            # 验证序列
            assertion_descs = [a.get('description', '') for a in assertions]
            patterns['assertion_sequence'].append(assertion_descs)
            
            # 验证数量
            patterns['assertion_count'].append(len(assertions))
        
        return patterns
    
    def _extract_error_patterns(self, seed_cases: List[Dict]) -> Dict:
        """提取异常处理模式"""
        patterns = {
            'error_types': [],
            'error_handling_actions': [],
            'retry_patterns': []
        }
        
        # 筛选异常用例
        error_cases = [case for case in seed_cases 
                       if case.get('case_type') == 'error_handling']
        
        for case in error_cases:
            scenario = case.get('scenario', {})
            
            # 异常类型
            error_type = scenario.get('error_type', '')
            patterns['error_types'].append(error_type)
            
            # 异常处理动作
            test_steps = case.get('test_steps', [])
            error_actions = [step.get('action', '') for step in test_steps 
                            if 'exception' in step.get('expected_result', '').lower()]
            patterns['error_handling_actions'].append(error_actions)
        
        return patterns
    
    def _extract_cleanup_patterns(self, seed_cases: List[Dict]) -> Dict:
        """提取清理模式"""
        patterns = {
            'cleanup_actions': [],
            'cleanup_count': []
        }
        
        for case in seed_cases:
            cleanup = case.get('cleanup', [])
            
            # 清理动作
            patterns['cleanup_actions'].append(cleanup)
            
            # 清理数量
            patterns['cleanup_count'].append(len(cleanup))
        
        return patterns
    
    def _extract_generalization_strategies(self, seed_cases: List[Dict]) -> Dict:
        """提取泛化策略"""
        strategies = {
            'data_generalization': [],
            'error_generalization': [],
            'assertion_generalization': []
        }
        
        for case in seed_cases:
            if 'generalization_patterns' in case:
                gen_patterns = case['generalization_patterns']
                
                # 数据泛化
                if 'data_patterns' in gen_patterns:
                    strategies['data_generalization'].extend(gen_patterns['data_patterns'])
                
                # 异常泛化
                if 'error_patterns' in gen_patterns:
                    strategies['error_generalization'].extend(gen_patterns['error_patterns'])
                
                # 验证泛化
                if 'assertion_patterns' in gen_patterns:
                    strategies['assertion_generalization'].extend(gen_patterns['assertion_patterns'])
        
        return strategies