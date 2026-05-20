#!/usr/bin/env python3
"""
TestCaseGenerationSkill - 主Skill类
基于组件交互描述和种子用例自动生成测试用例
"""

import yaml
import json
import logging
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime

# 导入子模块
from analyzer import InteractionParser, SeedAnalyzer
from generator import TestCaseGenerator, QualityChecker
from script_generator import ScriptGenerator


class TestCaseGenerationSkill:
    """
    测试用例自动生成Skill
    
    功能：
    - 解析组件交互描述
    - 分析种子用例模式
    - 生成新测试用例
    - 生成自动化脚本
    - 质量检查和覆盖分析
    """
    
    def __init__(self, config_path: str = None):
        """
        初始化Skill
        
        Args:
            config_path: 配置文件路径，默认使用skill_config.yaml
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        
        # 加载配置
        self.config = self._load_config(config_path or "config/skill_config.yaml")
        self.generation_limits = self._load_config("config/generation_limits.yaml")
        
        # 加载模板
        self.templates = self._load_templates()
        
        # 初始化子模块
        self.interaction_parser = InteractionParser(self.templates['interaction'])
        self.seed_analyzer = SeedAnalyzer()
        self.generator = TestCaseGenerator(
            self.templates['test_case'],
            self.generation_limits
        )
        self.script_generator = ScriptGenerator(self.templates['script'])
        self.quality_checker = QualityChecker(self.generation_limits)
        
        self.logger.info(f"Skill初始化完成: {self.config['skill_info']['name']}")
    
    def execute(self, 
                interaction_description: Dict,
                seed_cases: List[Dict],
                generation_config: Dict = None) -> Dict:
        """
        执行用例生成
        
        Args:
            interaction_description: 组件交互描述
            seed_cases: 种子用例列表
            generation_config: 生成配置（可选）
        
        Returns:
            {
                'test_cases': List[Dict],
                'automation_scripts': List[str],
                'coverage_analysis': Dict,
                'generation_report': str,
                'quality_metrics': Dict
            }
        """
        self.logger.info("开始执行用例生成")
        start_time = datetime.now()
        
        try:
            # Step 1: 解析交互描述
            self.logger.info("Step 1: 解析交互描述")
            parsed_interaction = self.interaction_parser.parse(interaction_description)
            
            # Step 2: 分析种子用例
            self.logger.info("Step 2: 分析种子用例")
            seed_patterns = self.seed_analyzer.analyze(seed_cases)
            
            # Step 3: 生成测试用例
            self.logger.info("Step 3: 生成测试用例")
            generated_cases = self.generator.generate(
                parsed_interaction,
                seed_patterns,
                generation_config or self.generation_limits
            )
            
            # Step 4: 生成自动化脚本
            self.logger.info("Step 4: 生成自动化脚本")
            automation_scripts = self.script_generator.generate_scripts(
                generated_cases,
                parsed_interaction
            )
            
            # Step 5: 覆盖分析
            self.logger.info("Step 5: 覆盖分析")
            coverage_analysis = self._analyze_coverage(
                generated_cases,
                parsed_interaction,
                seed_cases
            )
            
            # Step 6: 质量检查
            self.logger.info("Step 6: 质量检查")
            quality_metrics = self.quality_checker.check(generated_cases)
            
            # Step 7: 生成报告
            self.logger.info("Step 7: 生成报告")
            generation_report = self._generate_report(
                generated_cases,
                coverage_analysis,
                quality_metrics,
                start_time
            )
            
            result = {
                'test_cases': generated_cases,
                'automation_scripts': automation_scripts,
                'coverage_analysis': coverage_analysis,
                'generation_report': generation_report,
                'quality_metrics': quality_metrics,
                'metadata': {
                    'generation_time': (datetime.now() - start_time).total_seconds(),
                    'total_cases': len(generated_cases),
                    'skill_version': self.config['skill_info']['skill_version']
                }
            }
            
            self.logger.info(f"用例生成完成，共生成{len(generated_cases)}个用例")
            return result
            
        except Exception as e:
            self.logger.error(f"用例生成失败: {str(e)}")
            raise
    
    def _load_config(self, config_path: str) -> Dict:
        """加载配置文件"""
        path = Path(__file__).parent.parent / config_path
        if path.exists():
            with open(path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        else:
            self.logger.warning(f"配置文件不存在: {config_path}")
            return {}
    
    def _load_templates(self) -> Dict:
        """加载所有模板"""
        templates_dir = Path(__file__).parent.parent / "templates"
        templates = {}
        
        template_files = {
            'interaction': 'interaction_template.yaml',
            'test_case': 'test_case_template.yaml',
            'script': 'script_template.py'
        }
        
        for name, filename in template_files.items():
            template_path = templates_dir / filename
            if template_path.exists():
                with open(template_path, 'r', encoding='utf-8') as f:
                    templates[name] = f.read()
        
        return templates
    
    def _analyze_coverage(self, 
                         generated_cases: List[Dict],
                         parsed_interaction: Dict,
                         seed_cases: List[Dict]) -> Dict:
        """分析覆盖维度"""
        coverage = {
            'dimensions': {},
            'components': {},
            'interaction_types': {},
            'overall_coverage': 0.0
        }
        
        # 分析维度覆盖
        required_dimensions = self.generation_limits['coverage_dimensions']['required']
        for dimension in required_dimensions:
            dim_name = dimension['dimension']
            covered_cases = [
                case for case in generated_cases 
                if case.get('case_type') == dim_name
            ]
            coverage['dimensions'][dim_name] = {
                'covered': len(covered_cases) > 0,
                'case_count': len(covered_cases),
                'min_required': dimension['min_cases']
            }
        
        # 分析组件覆盖
        components = parsed_interaction.get('components', [])
        for component in components:
            component_cases = [
                case for case in generated_cases
                if component in case.get('scenario', {}).get('components', [])
            ]
            coverage['components'][component] = {
                'covered': len(component_cases) > 0,
                'case_count': len(component_cases)
            }
        
        # 分析交互类型覆盖
        interaction_type = parsed_interaction.get('interaction_type')
        if interaction_type:
            type_cases = [
                case for case in generated_cases
                if case.get('scenario', {}).get('interaction_type') == interaction_type
            ]
            coverage['interaction_types'][interaction_type] = {
                'covered': len(type_cases) > 0,
                'case_count': len(type_cases)
            }
        
        # 计算总体覆盖率
        required_covered = sum(
            1 for dim in coverage['dimensions'].values() 
            if dim['covered']
        )
        total_required = len(required_dimensions)
        coverage['overall_coverage'] = required_covered / total_required if total_required > 0 else 0
        
        return coverage
    
    def _generate_report(self,
                        generated_cases: List[Dict],
                        coverage_analysis: Dict,
                        quality_metrics: Dict,
                        start_time: datetime) -> str:
        """生成执行报告"""
        report = []
        report.append("# 测试用例生成报告")
        report.append("")
        report.append(f"生成时间: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        report.append(f"执行耗时: {(datetime.now() - start_time).total_seconds():.2f}秒")
        report.append("")
        
        report.append("## 生成的测试用例")
        report.append(f"总数: {len(generated_cases)}")
        report.append("")
        
        # 优先级分布
        priorities = {'P0': 0, 'P1': 0, 'P2': 0}
        for case in generated_cases:
            priority = case.get('priority', 'P2')
            priorities[priority] = priorities.get(priority, 0) + 1
        
        report.append("### 优先级分布")
        report.append(f"- P0: {priorities['P0']} ({priorities['P0']/len(generated_cases)*100:.1f}%)")
        report.append(f"- P1: {priorities['P1']} ({priorities['P1']/len(generated_cases)*100:.1f}%)")
        report.append(f"- P2: {priorities['P2']} ({priorities['P2']/len(generated_cases)*100:.1f}%)")
        report.append("")
        
        report.append("## 覆盖分析")
        report.append(f"总体覆盖率: {coverage_analysis['overall_coverage']*100:.1f}%")
        report.append("")
        
        report.append("### 维度覆盖")
        for dim_name, dim_info in coverage_analysis['dimensions'].items():
            status = "✅" if dim_info['covered'] else "❌"
            report.append(f"- {status} {dim_name}: {dim_info['case_count']}个用例")
        report.append("")
        
        report.append("## 质量指标")
        for metric_name, metric_value in quality_metrics.items():
            report.append(f"- {metric_name}: {metric_value}")
        report.append("")
        
        return '\n'.join(report)


# ========== Skill执行入口 ==========

def main():
    """Skill主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='TestCaseGenerationSkill')
    parser.add_argument('--interaction', required=True, help='交互描述文件路径')
    parser.add_argument('--seed', required=True, help='种子用例文件路径')
    parser.add_argument('--output', default='output', help='输出目录')
    parser.add_argument('--config', help='配置文件路径')
    
    args = parser.parse_args()
    
    # 加载输入
    with open(args.interaction, 'r', encoding='utf-8') as f:
        interaction = yaml.safe_load(f)
    
    with open(args.seed, 'r', encoding='utf-8') as f:
        seed_cases = yaml.safe_load(f)
    
    # 执行Skill
    skill = TestCaseGenerationSkill(args.config)
    result = skill.execute(interaction, seed_cases)
    
    # 保存输出
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True)
    
    with open(output_dir / 'test_cases.json', 'w', encoding='utf-8') as f:
        json.dump(result['test_cases'], f, indent=2, ensure_ascii=False)
    
    for i, script in enumerate(result['automation_scripts']):
        with open(output_dir / f'test_script_{i}.py', 'w', encoding='utf-8') as f:
            f.write(script)
    
    with open(output_dir / 'generation_report.md', 'w', encoding='utf-8') as f:
        f.write(result['generation_report'])
    
    print(f"✅ 生成完成，输出目录: {output_dir}")
    print(f"✅ 用例数量: {len(result['test_cases'])}")
    print(f"✅ 覆盖率: {result['coverage_analysis']['overall_coverage']*100:.1f}%")


if __name__ == "__main__":
    main()