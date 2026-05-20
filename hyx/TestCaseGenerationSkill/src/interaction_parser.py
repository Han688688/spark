#!/usr/bin/env python3
"""
交互描述解析器
解析标准化的组件交互描述
"""

import yaml
import logging
from typing import Dict, Any, List
from pathlib import Path


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
        # 特征：数据从一个组件流向另一个
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