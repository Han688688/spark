#!/usr/bin/env python3
"""
测试用例生成器
基于交互描述和种子模式生成测试用例
"""

import yaml
import json
import logging
from typing import Dict, Any, List
from datetime import datetime
import copy


class TestCaseGenerator:
    """
    测试用例生成器
    
    功能：
    - 应用模板生成用例
    - 执行泛化策略
    - 控制生成数量
    - 确保覆盖维度
    """
    
    def __init__(self, template: str, limits: Dict):
        """初始化生成器"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.template = yaml.safe_load(template) if template else {}
        self.limits = limits
        
        self.logger.info("测试用例生成器初始化完成")
    
    def generate(self,
                parsed_interaction: Dict,
                seed_patterns: Dict,
                generation_config: Dict = None) -> List[Dict]:
        """
        生成测试用例
        
        Args:
            parsed_interaction: 解析后的交互描述
            seed_patterns: 种子用例模式
            generation_config: 生成配置
        
        Returns:
            List[Dict]: 生成的测试用例列表
        """
        self.logger.info("开始生成测试用例")
        
        config = generation_config or self.limits
        generated_cases = []
        
        # Step 1: 生成正常流程用例（必选）
        self.logger.info("生成正常流程用例")
        normal_cases = self._generate_normal_flow_cases(
            parsed_interaction,
            seed_patterns,
            config
        )
        generated_cases.extend(normal_cases)
        
        # Step 2: 生成异常处理用例（必选）
        self.logger.info("生成异常处理用例")
        error_cases = self._generate_error_handling_cases(
            parsed_interaction,
            seed_patterns,
            config
        )
        generated_cases.extend(error_cases)
        
        # Step 3: 生成边界值用例（必选）
        self.logger.info("生成边界值用例")
        boundary_cases = self._generate_boundary_cases(
            parsed_interaction,
            seed_patterns,
            config
        )
        generated_cases.extend(boundary_cases)
        
        # Step 4: 应用数量限制
        self.logger.info("应用数量限制")
        generated_cases = self._apply_quantity_limits(generated_cases, config)
        
        # Step 5: 填充用例ID和元数据
        self.logger.info("填充元数据")
        generated_cases = self._fill_metadata(generated_cases, parsed_interaction)
        
        self.logger.info(f"生成完成，共{len(generated_cases)}个用例")
        return generated_cases
    
    def _generate_normal_flow_cases(self,
                                    interaction: Dict,
                                    patterns: Dict,
                                    config: Dict) -> List[Dict]:
        """生成正常流程用例"""
        cases = []
        
        # 基础正常用例
        base_case = self._create_base_case(interaction, 'normal_flow', 'P0')
        
        # 应用数据泛化策略
        data_strategies = patterns['generalization_strategies']['data_generalization']
        
        # 至少生成1个正常用例
        case_1 = copy.deepcopy(base_case)
        case_1['case_name'] = f"{interaction['name']}_正常流程_基础"
        case_1['test_data']['input'] = self._generate_normal_input(interaction)
        cases.append(case_1)
        
        # 根据策略生成变体
        for strategy in data_strategies[:2]:  # 最多生成2个变体
            variant_case = copy.deepcopy(base_case)
            variant_case['case_name'] = f"{interaction['name']}_正常流程_变体"
            variant_case['priority'] = 'P1'
            variant_case['test_data'] = self._apply_data_strategy(
                strategy,
                interaction
            )
            cases.append(variant_case)
        
        return cases
    
    def _generate_error_handling_cases(self,
                                       interaction: Dict,
                                       patterns: Dict,
                                       config: Dict) -> List[Dict]:
        """生成异常处理用例"""
        cases = []
        
        # 获取组件列表
        components = interaction['components']
        
        # 为每个组件生成故障场景（至少1个）
        error_components = components[:min(2, len(components))]  # 最多2个组件
        
        for component in error_components:
            error_case = self._create_base_case(interaction, 'error_handling', 'P0')
            error_case['case_name'] = f"{interaction['name']}_{component}故障处理"
            error_case['scenario']['description'] = f"{component}组件异常时的处理流程"
            error_case['test_steps'] = self._generate_error_steps(component, interaction)
            error_case['assertions'] = self._generate_error_assertions(component)
            cases.append(error_case)
        
        return cases
    
    def _generate_boundary_cases(self,
                                interaction: Dict,
                                patterns: Dict,
                                config: Dict) -> List[Dict]:
        """生成边界值用例"""
        cases = []
        
        # 最小值边界
        min_case = self._create_base_case(interaction, 'boundary_values', 'P1')
        min_case['case_name'] = f"{interaction['name']}_最小数据量"
        min_case['scenario']['description'] = "测试最小数据量场景"
        min_case['test_data']['input'] = self._generate_min_boundary_data(interaction)
        cases.append(min_case)
        
        # 最大值边界
        max_case = self._create_base_case(interaction, 'boundary_values', 'P1')
        max_case['case_name'] = f"{interaction['name']}_最大数据量"
        max_case['scenario']['description'] = "测试最大数据量场景"
        max_case['test_data']['input'] = self._generate_max_boundary_data(interaction, config)
        cases.append(max_case)
        
        return cases
    
    def _create_base_case(self, interaction: Dict, case_type: str, priority: str) -> Dict:
        """创建基础用例模板"""
        case = {
            'case_name': '',
            'case_type': case_type,
            'priority': priority,
            'scenario': {
                'name': interaction['name'],
                'components': interaction['components'],
                'interaction_type': interaction['interaction_type'],
                'description': ''
            },
            'preconditions': self._generate_preconditions(interaction),
            'test_steps': self._generate_test_steps(interaction),
            'test_data': {
                'input': {},
                'expected_output': {}
            },
            'assertions': [],
            'cleanup': self._generate_cleanup(interaction)
        }
        return case
    
    def _generate_preconditions(self, interaction: Dict) -> List[str]:
        """生成前置条件"""
        preconditions = []
        for component in interaction['components']:
            preconditions.append(f"{component}正常运行")
        return preconditions
    
    def _generate_test_steps(self, interaction: Dict) -> List[Dict]:
        """生成测试步骤"""
        steps = []
        flow = interaction['flow']
        
        for i, flow_step in enumerate(flow, 1):
            step = {
                'step_number': i,
                'action': flow_step['action'],
                'component': flow_step['component'],
                'input': flow_step.get('input', ''),
                'expected_result': f"{flow_step['action']}成功完成"
            }
            steps.append(step)
        
        return steps
    
    def _generate_cleanup(self, interaction: Dict) -> List[str]:
        """生成清理步骤"""
        cleanup = []
        for component in interaction['components']:
            cleanup.append(f"清理{component}测试数据")
        return cleanup
    
    def _generate_normal_input(self, interaction: Dict) -> Dict:
        """生成正常输入数据"""
        data_schema = interaction['data_schema']
        input_data = data_schema.get('input_data', {})
        
        # 返回基础数据
        return {
            'data_size': 100,  # 默认100条
            'data_format': input_data.get('type', 'JSON')
        }
    
    def _generate_min_boundary_data(self, interaction: Dict) -> Dict:
        """生成最小边界数据"""
        return {
            'data_size': 1,  # 最小1条
            'data_format': 'JSON'
        }
    
    def _generate_max_boundary_data(self, interaction: Dict, config: Dict) -> Dict:
        """生成最大边界数据"""
        max_size = config['data_limits']['input_size']['max']
        return {
            'data_size': max_size,
            'data_format': 'JSON'
        }
    
    def _generate_error_steps(self, component: str, interaction: Dict) -> List[Dict]:
        """生成异常测试步骤"""
        steps = []
        
        # Step 1: 模拟故障
        steps.append({
            'step_number': 1,
            'action': f"模拟{component}故障",
            'component': component,
            'expected_result': f"{component}停止运行"
        })
        
        # Step 2: 触发操作
        steps.append({
            'step_number': 2,
            'action': "尝试执行正常流程",
            'component': "Test",
            'expected_result': "触发异常处理"
        })
        
        return steps
    
    def _generate_error_assertions(self, component: str) -> List[Dict]:
        """生成异常验证点"""
        return [
            {
                'assertion_type': 'exception',
                'description': f"验证{component}异常被捕获",
                'expected_value': '异常抛出'
            },
            {
                'assertion_type': 'function',
                'description': "验证异常处理机制",
                'expected_value': '重试或回退'
            }
        ]
    
    def _apply_data_strategy(self, strategy: str, interaction: Dict) -> Dict:
        """应用数据泛化策略"""
        # 简化实现：根据策略字符串生成不同数据
        test_data = {
            'input': {},
            'expected_output': {}
        }
        
        if '数量' in strategy:
            test_data['input']['data_size'] = 1000
        elif '格式' in strategy:
            test_data['input']['data_format'] = 'Avro'
        
        return test_data
    
    def _apply_quantity_limits(self, cases: List[Dict], config: Dict) -> List[Dict]:
        """应用数量限制"""
        max_cases = config['quantity_limits']['max_cases_per_scenario']
        
        if len(cases) > max_cases:
            self.logger.warning(f"用例数量超出限制，裁剪到{max_cases}个")
            # 优先保留P0，然后P1，最后P2
            p0_cases = [c for c in cases if c['priority'] == 'P0']
            p1_cases = [c for c in cases if c['priority'] == 'P1']
            p2_cases = [c for c in cases if c['priority'] == 'P2']
            
            cases = p0_cases + p1_cases + p2_cases[:max_cases - len(p0_cases) - len(p1_cases)]
        
        return cases
    
    def _fill_metadata(self, cases: List[Dict], interaction: Dict) -> List[Dict]:
        """填充元数据"""
        timestamp = datetime.now().strftime('%Y%m%d%H%M%S')
        
        for i, case in enumerate(cases, 1):
            case['case_id'] = f"auto-{timestamp}-{i}"
            case['generated_time'] = datetime.now().isoformat()
            case['seed_scenario'] = interaction['name']
        
        return cases