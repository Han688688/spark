#!/usr/bin/env python3
"""
种子用例分析器
分析已有种子用例的模式和特征
"""

import yaml
import logging
from typing import Dict, Any, List
from pathlib import Path


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
        # 从种子用例末尾的generalization_patterns提取
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