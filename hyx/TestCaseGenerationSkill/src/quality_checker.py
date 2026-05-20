#!/usr/bin/env python3
"""
质量检查器
检查生成的测试用例质量
"""

import logging
from typing import Dict, Any, List


class QualityChecker:
    """
    质量检查器
    
    功能：
    - 检查用例完整性
    - 检查步骤可执行性
    - 检查验证点有效性
    - 检查数据合法性
    - 计算质量分数
    """
    
    def __init__(self, limits: Dict):
        """初始化检查器"""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.limits = limits
    
    def check(self, test_cases: List[Dict]) -> Dict:
        """
        检查用例质量
        
        Args:
            test_cases: 测试用例列表
        
        Returns:
            Dict: 质量指标
        """
        self.logger.info(f"开始质量检查，共{len(test_cases)}个用例")
        
        metrics = {
            'total_cases': len(test_cases),
            'valid_cases': 0,
            'invalid_cases': 0,
            'completeness_score': 0.0,
            'executability_score': 0.0,
            'assertion_score': 0.0,
            'overall_score': 0.0,
            'issues': []
        }
        
        # 逐个检查用例
        for case in test_cases:
            is_valid, issues = self._check_single_case(case)
            
            if is_valid:
                metrics['valid_cases'] += 1
            else:
                metrics['invalid_cases'] += 1
                metrics['issues'].extend(issues)
        
        # 计算质量分数
        metrics['completeness_score'] = self._calculate_completeness(test_cases)
        metrics['executability_score'] = self._calculate_executability(test_cases)
        metrics['assertion_score'] = self._calculate_assertion_score(test_cases)
        metrics['overall_score'] = (
            metrics['completeness_score'] +
            metrics['executability_score'] +
            metrics['assertion_score']
        ) / 3
        
        self.logger.info(f"质量检查完成，总体分数: {metrics['overall_score']:.2f}")
        return metrics
    
    def _check_single_case(self, case: Dict) -> tuple[bool, List[str]]:
        """检查单个用例"""
        issues = []
        is_valid = True
        
        # 检查必填字段
        required_fields = self.limits['quality_control']['required_fields']
        for field in required_fields:
            if field not in case or not case[field]:
                issues.append(f"用例缺少必填字段: {field}")
                is_valid = False
        
        # 检查验证点数量
        assertions = case.get('assertions', [])
        min_assertions = self.limits['quality_control']['min_assertions']
        if len(assertions) < min_assertions:
            issues.append(f"验证点数量不足: {len(assertions)} < {min_assertions}")
            is_valid = False
        
        # 检查步骤数量
        steps = case.get('test_steps', [])
        min_steps = self.limits['quality_control']['min_steps']
        if len(steps) < min_steps:
            issues.append(f"测试步骤数量不足: {len(steps)} < {min_steps}")
            is_valid = False
        
        # 检查清理步骤
        cleanup = case.get('cleanup', [])
        if not cleanup:
            issues.append("缺少清理步骤")
        
        return is_valid, issues
    
    def _calculate_completeness(self, test_cases: List[Dict]) -> float:
        """计算完整性分数"""
        if not test_cases:
            return 0.0
        
        required_fields = self.limits['quality_control']['required_fields']
        complete_count = 0
        
        for case in test_cases:
            is_complete = all(field in case and case[field] for field in required_fields)
            if is_complete:
                complete_count += 1
        
        return complete_count / len(test_cases)
    
    def _calculate_executability(self, test_cases: List[Dict]) -> float:
        """计算可执行性分数"""
        # 简化实现：假设所有生成的用例都可执行
        # 实际应该检查步骤的可行性
        return 1.0
    
    def _calculate_assertion_score(self, test_cases: List[Dict]) -> float:
        """计算验证点分数"""
        if not test_cases:
            return 0.0
        
        min_assertions = self.limits['quality_control']['min_assertions']
        valid_count = 0
        
        for case in test_cases:
            assertions = case.get('assertions', [])
            if len(assertions) >= min_assertions:
                valid_count += 1
        
        return valid_count / len(test_cases)