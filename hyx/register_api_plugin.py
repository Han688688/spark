#!/usr/bin/env python3
"""
注册API覆盖插件到hyx框架
"""

import sys
from pathlib import Path

# 添加hyx目录到路径
sys.path.append(str(Path(__file__).parent))

from framework import TestAutomationFramework
from api_coverage_plugin import (
    APICoverageAnalyzer,
    APICoverageDiscoverer,
    APITestGenerator
)

def register_api_plugins(framework: TestAutomationFramework):
    """
    注册API覆盖插件到框架
    
    Args:
        framework: TestAutomationFramework实例
        
    Returns:
        注册后的框架实例
    """
    
    print("=" * 60)
    print("注册API覆盖插件")
    print("=" * 60)
    
    # 1. 注册分析插件
    framework.register_plugin('analyzer', APICoverageAnalyzer())
    print("✓ 注册分析插件: api_coverage_analyzer v1.0.0")
    
    # 2. 注册发现策略
    framework.register_plugin('discoverer', APICoverageDiscoverer())
    print("✓ 注册发现策略: api_coverage_discoverer v1.0.0")
    
    # 3. 注册生成插件
    framework.register_plugin('generator', APITestGenerator())
    print("✓ 注册生成插件: api_test_generator v1.0.0")
    
    # 4. 创建专用工作流
    api_workflow = {
        'name': 'api_coverage_workflow',
        'description': 'API覆盖度检测与测试生成流程',
        'steps': [
            {
                'name': 'analyze_api',
                'type': 'analyze',
                'plugin': 'api_coverage_analyzer',
                'description': '分析API覆盖度'
            },
            {
                'name': 'discover_missing',
                'type': 'discover',
                'plugin': 'api_coverage_discoverer',
                'description': '发现缺失API场景',
                'depends_on': ['analyze_api']
            },
            {
                'name': 'generate_tests',
                'type': 'generate',
                'plugin': 'api_test_generator',
                'description': '生成API测试代码',
                'depends_on': ['discover_missing'],
                'config': {
                    'output_dir': 'generated_tests/api_coverage',
                    'package_template': 'org.apache.{component}.api.test'
                }
            }
        ]
    }
    
    framework.add_workflow('api_coverage', api_workflow)
    print("✓ 创建工作流: api_coverage")
    
    # 5. 创建快速检查工作流（只分析不生成）
    quick_workflow = {
        'name': 'api_coverage_check',
        'description': '快速API覆盖度检查（仅分析）',
        'steps': [
            {
                'name': 'analyze_api',
                'type': 'analyze',
                'plugin': 'api_coverage_analyzer',
                'description': '快速检查API覆盖度'
            }
        ]
    }
    
    framework.add_workflow('api_check', quick_workflow)
    print("✓ 创建工作流: api_check")
    
    print("=" * 60)
    print("插件注册完成")
    print("=" * 60)
    
    return framework


def run_api_coverage_demo():
    """
    运行API覆盖度检测Demo
    """
    print("\n" + "=" * 60)
    print("API覆盖度检测 Demo")
    print("=" * 60 + "\n")
    
    # 1. 初始化框架
    framework = TestAutomationFramework()
    
    # 2. 注册插件
    framework = register_api_plugins(framework)
    
    # 3. 配置
    config = {
        'hyx_dir': '/home/h00517772/spark/hyx',
        'output_dir': '/home/h00517772/spark/hyx/results',
        'project_root': '/home/h00517772/spark',
        'api_inventory_files': [
            'spark_java_api_complete_list.md',
            'kafka_java_api_complete_list.md',
            'hbase_java_api_complete_list.md'
        ]
    }
    
    # 4. 运行快速检查
    print("\n步骤1: 运行API覆盖度快速检查...")
    print("-" * 60)
    
    context = {
        'project_root': config['project_root'],
        'config': config
    }
    
    # 直接调用分析插件
    analyzer = APICoverageAnalyzer()
    result = analyzer.analyze(Path(config['project_root']), config)
    
    # 显示结果
    print("\n分析结果:")
    print("-" * 60)
    print(f"总API数: {result['total_apis']}")
    print(f"已测试API数: {result['tested_apis_count']}")
    print(f"缺失API数: {result['missing_apis_count']}")
    print(f"覆盖率: {result['coverage_rate']:.2%}")
    print(f"测试文件数: {result['test_files_count']}")
    
    print("\n按组件统计:")
    print("-" * 60)
    for comp, stats in result['coverage_by_component'].items():
        print(f"{comp:15s}: {stats['tested']}/{stats['total']} "
              f"(缺失{stats['missing']}) - {stats['rate']:.2%}")
    
    print("\n按稳定性统计:")
    print("-" * 60)
    for stab, stats in result['coverage_by_stability'].items():
        print(f"{stab:15s}: {stats['tested']}/{stats['total']} "
              f"(缺失{stats['missing']}) - {stats['rate']:.2%}")
    
    # 5. 显示缺失API示例
    print("\n缺失API示例（优先级排序前20个）:")
    print("-" * 60)
    
    priority_map = {'Stable': 'P0', 'Evolving': 'P1', 'Unknown': 'P2', 'Private': 'P3'}
    
    sorted_missing = sorted(result['missing_apis'],
                           key=lambda x: priority_map.get(x['stability'], 'P2'))
    
    print(f"{'组件':<10s} {'类名':<30s} {'方法':<20s} {'稳定性':<10s} {'优先级':<5s}")
    print("-" * 80)
    
    for api in sorted_missing[:20]:
        priority = priority_map.get(api['stability'], 'P2')
        print(f"{api['component']:<10s} {api['class_name']:<30s} "
              f"{api['method_name']:<20s} {api['stability']:<10s} {priority:<5s}")
    
    # 6. 生成场景（可选）
    print("\n是否生成测试场景？(y/n): ")
    choice = input().strip().lower()
    
    if choice == 'y':
        print("\n步骤2: 生成缺失API测试场景...")
        print("-" * 60)
        
        discoverer = APICoverageDiscoverer()
        context['api_coverage_analysis'] = result
        scenarios = discoverer.discover(context)
        
        print(f"生成 {len(scenarios)} 个测试场景")
        
        # 显示前10个场景
        print("\n场景示例（前10个）:")
        print("-" * 60)
        for scenario in scenarios[:10]:
            print(f"\n场景ID: {scenario['id']}")
            print(f"名称: {scenario['name']}")
            print(f"优先级: {scenario['priority']}")
            print(f"描述: {scenario['description'][:100]}...")
        
        # 保存场景
        import json
        scenarios_file = Path(config['output_dir']) / 'api_coverage_scenarios.json'
        with open(scenarios_file, 'w', encoding='utf-8') as f:
            json.dump(scenarios, f, indent=2, ensure_ascii=False)
        
        print(f"\n场景已保存到: {scenarios_file}")
        
        # 7. 生成测试代码（可选）
        print("\n是否生成测试代码？(y/n): ")
        choice = input().strip().lower()
        
        if choice == 'y':
            print("\n步骤3: 生成测试代码...")
            print("-" * 60)
            
            generator = APITestGenerator()
            test_dir = Path(config['output_dir']) / 'generated_tests' / 'api_coverage'
            test_dir.mkdir(parents=True, exist_ok=True)
            
            # 只生成P0优先级的测试代码
            p0_scenarios = [s for s in scenarios if s['priority'] == 'P0']
            
            print(f"生成 {len(p0_scenarios)} 个P0优先级测试（避免生成过多）")
            
            for scenario in p0_scenarios[:10]:  # 限制数量
                test_code = generator.generate(scenario, context)
                
                if test_code:
                    # 提取类名
                    test_class_name = scenario['id'].split('_')[-1] + 'Test.java'
                    test_file = test_dir / test_class_name
                    
                    with open(test_file, 'w', encoding='utf-8') as f:
                        f.write(test_code)
                    
                    print(f"  ✓ {test_class_name}")
            
            print(f"\n测试代码已保存到: {test_dir}")
    
    print("\n" + "=" * 60)
    print("Demo完成")
    print("=" * 60)
    
    print("\n查看报告:")
    print(f"  - JSON: {config['output_dir']}/api_coverage_analysis.json")
    print(f"  - Markdown: {config['output_dir']}/API_Coverage_Report.md")
    
    print("\n下一步:")
    print("  1. 查看报告了解缺失API详情")
    print("  2. 运行完整流程生成所有测试")
    print("  3. 将生成的测试代码集成到项目")


def main():
    """
    主函数 - 可选择不同执行模式
    """
    print("\nAPI覆盖度检测工具")
    print("=" * 60)
    print("选择执行模式:")
    print("  1. Demo演示（推荐首次使用）")
    print("  2. 快速检查（仅分析）")
    print("  3. 完整流程（分析+生成）")
    print("  4. 注册到框架")
    print("=" * 60)
    
    choice = input("请选择 (1-4): ").strip()
    
    if choice == '1':
        run_api_coverage_demo()
    
    elif choice == '2':
        # 快速检查
        framework = TestAutomationFramework()
        framework = register_api_plugins(framework)
        
        config = {
            'hyx_dir': '/home/h00517772/spark/hyx',
            'output_dir': '/home/h00517772/spark/hyx/results',
            'project_root': '/home/h00517772/spark',
            'api_inventory_files': [
                'spark_java_api_complete_list.md',
                'kafka_java_api_complete_list.md'
            ]
        }
        
        analyzer = APICoverageAnalyzer()
        result = analyzer.analyze(Path(config['project_root']), config)
        
        print(f"\n覆盖率: {result['coverage_rate']:.2%}")
        print(f"缺失API: {result['missing_apis_count']} 个")
    
    elif choice == '3':
        # 完整流程
        framework = TestAutomationFramework()
        framework = register_api_plugins(framework)
        
        # 配置
        config = {
            'hyx_dir': '/home/h00517772/spark/hyx',
            'output_dir': '/home/h00517772/spark/hyx/results',
            'project_root': '/home/h00517772/spark'
        }
        
        # 运行工作流
        framework.run('api_coverage', config)
    
    elif choice == '4':
        # 仅注册
        framework = TestAutomationFramework()
        framework = register_api_plugins(framework)
        print("\n插件已注册，可通过框架调用")
    
    else:
        print("无效选择")


if __name__ == "__main__":
    main()