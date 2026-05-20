#!/usr/bin/env python3
"""
TestCaseGenerationSkill测试用例
验证Skill的各个模块功能
"""

import pytest
import yaml
import json
from pathlib import Path
import sys

# 导入Skill模块
sys.path.append(str(Path(__file__).parent.parent / "src"))
from skill import TestCaseGenerationSkill
from interaction_parser import InteractionParser
from seed_analyzer import SeedAnalyzer


class TestInteractionParser:
    """测试交互描述解析器"""
    
    def test_parse_basic_interaction(self):
        """测试基础交互解析"""
        parser = InteractionParser()
        
        interaction = {
            'interaction': {
                'name': 'test-scenario',
                'components': ['Kafka', 'Spark'],
                'flow': [
                    {'step': 'send', 'component': 'Kafka', 'action': 'send'}
                ]
            },
            'data_schema': {'input_data': {}},
            'constraints': {}
        }
        
        result = parser.parse(interaction)
        
        assert result['name'] == 'test-scenario'
        assert result['components'] == ['Kafka', 'Spark']
        assert len(result['flow']) == 1
    
    def test_parse_missing_required_field(self):
        """测试缺少必填字段"""
        parser = InteractionParser()
        
        interaction = {
            'interaction': {
                'name': 'test',
                # 缺少components
            }
        }
        
        with pytest.raises(ValueError):
            parser.parse(interaction)
    
    def test_determine_interaction_type(self):
        """测试交互类型判断"""
        parser = InteractionParser()
        
        data_flow_interaction = {
            'interaction': {
                'name': 'test',
                'components': ['A', 'B'],
                'flow': [
                    {'action': 'send message'},
                    {'action': 'receive data'}
                ]
            },
            'data_schema': {},
            'constraints': {}
        }
        
        result = parser.parse(data_flow_interaction)
        assert result['interaction_type'] == 'data_flow'


class TestSeedAnalyzer:
    """测试种子用例分析器"""
    
    def test_analyze_basic_seed(self):
        """测试基础种子分析"""
        analyzer = SeedAnalyzer()
        
        seed_cases = [
            {
                'case_name': 'test1',
                'case_type': 'normal_flow',
                'test_steps': [
                    {'action': 'send', 'component': 'Kafka'}
                ],
                'assertions': [
                    {'assertion_type': 'data'}
                ]
            }
        ]
        
        patterns = analyzer.analyze(seed_cases)
        
        assert 'interaction_patterns' in patterns
        assert 'data_patterns' in patterns
        assert len(patterns['interaction_patterns']['flow_sequence']) == 1
    
    def test_extract_error_patterns(self):
        """测试异常模式提取"""
        analyzer = SeedAnalyzer()
        
        seed_cases = [
            {
                'case_type': 'error_handling',
                'scenario': {'error_type': 'connection_failed'},
                'test_steps': []
            }
        ]
        
        patterns = analyzer.analyze(seed_cases)
        
        assert 'connection_failed' in patterns['error_patterns']['error_types']


class TestTestCaseGenerationSkill:
    """测试主Skill"""
    
    @pytest.fixture
    def skill(self):
        """初始化Skill"""
        config_path = Path(__file__).parent.parent / "config" / "skill_config.yaml"
        return TestCaseGenerationSkill(str(config_path))
    
    def test_skill_initialization(self, skill):
        """测试Skill初始化"""
        assert skill.config['skill_info']['name'] == 'TestCaseGenerationSkill'
    
    def test_execute_generation(self, skill):
        """测试执行生成"""
        interaction = {
            'interaction': {
                'name': 'test-flow',
                'components': ['Kafka', 'Spark', 'HDFS'],
                'flow': [
                    {'step': '1', 'component': 'Kafka', 'action': 'send', 'input': 'data', 'output': 'queue'},
                    {'step': '2', 'component': 'Spark', 'action': 'process', 'input': 'queue', 'output': 'result'},
                    {'step': '3', 'component': 'HDFS', 'action': 'write', 'input': 'result', 'output': 'file'}
                ]
            },
            'data_schema': {
                'input_data': {'type': 'JSON', 'fields': []}
            },
            'constraints': {
                'data_constraints': ['no duplicates']
            }
        }
        
        seed_cases = [
            {
                'case_name': 'normal-flow',
                'case_type': 'normal_flow',
                'priority': 'P0',
                'scenario': {'name': 'test-flow'},
                'test_steps': [],
                'test_data': {'input': {'data_size': 100}},
                'assertions': [],
                'cleanup': []
            }
        ]
        
        result = skill.execute(interaction, seed_cases)
        
        assert 'test_cases' in result
        assert len(result['test_cases']) > 0
        assert 'automation_scripts' in result
        assert 'coverage_analysis' in result
    
    def test_coverage_analysis(self, skill):
        """测试覆盖分析"""
        interaction = {
            'interaction': {
                'name': 'test',
                'components': ['A', 'B'],
                'flow': []
            },
            'data_schema': {},
            'constraints': {}
        }
        
        seed_cases = []
        
        result = skill.execute(interaction, seed_cases)
        
        coverage = result['coverage_analysis']
        assert 'dimensions' in coverage
        assert 'overall_coverage' in coverage


class TestQualityChecker:
    """测试质量检查器"""
    
    def test_check_valid_case(self):
        """测试有效用例检查"""
        from quality_checker import QualityChecker
        
        limits = {
            'quality_control': {
                'min_assertions': 2,
                'min_steps': 2,
                'required_fields': ['case_name', 'priority', 'assertions']
            }
        }
        
        checker = QualityChecker(limits)
        
        cases = [
            {
                'case_name': 'test',
                'priority': 'P0',
                'assertions': [{'type': 'data'}, {'type': 'function'}],
                'test_steps': [{'step': 1}, {'step': 2}],
                'cleanup': ['clean']
            }
        ]
        
        metrics = checker.check(cases)
        
        assert metrics['valid_cases'] == 1
        assert metrics['overall_score'] > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])