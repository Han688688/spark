#!/usr/bin/env python3
"""
测试自动化框架 - 快速启动脚本

简化使用，常用命令一键执行
"""

import sys
import subprocess
from pathlib import Path

FRAMEWORK_SCRIPT = Path(__file__).parent / "framework.py"


def print_help():
    """显示帮助"""
    print("""
测试自动化框架 - 快速启动

用法: python hyx/start.py <command>

命令:
  init        初始化框架
  list        列出所有能力
  run         运行默认工作流
  full        运行完整工作流（多维度分析）
  quick       快速生成（10个核心场景）
  interaction 重点识别交互场景
  p0          只生成P0优先级场景
  p1          只生成P1优先级场景
  help        显示帮助

示例:
  python hyx/start.py init
  python hyx/start.py run
  python hyx/start.py p0

交互场景识别:
  python hyx/start.py interaction
  
  会自动识别:
  - 组件调用链交互
  - 配置依赖交互  
  - 日志中的实际交互
  - API接口交互
  
  生成场景:
  - 正常交互场景
  - 异常交互场景
  - 边界交互场景
  - 并发交互场景
""")


def run_command(cmd):
    """执行命令"""
    result = subprocess.run(
        ["python", str(FRAMEWORK_SCRIPT), cmd],
        capture_output=False
    )
    return result.returncode


def run_workflow(workflow_name, extra_args=None):
    """运行工作流"""
    cmd = ["python", str(FRAMEWORK_SCRIPT), "run", "--workflow", workflow_name]
    
    if extra_args:
        cmd.extend(extra_args)
    
    subprocess.run(cmd)


def main():
    if len(sys.argv) < 2:
        print_help()
        return
    
    command = sys.argv[1]
    
    # 命令映射
    if command == "help":
        print_help()
    
    elif command == "init":
        print("=== 初始化框架 ===")
        run_command("init")
        
        # 创建必要的目录
        Path("hyx/plugins").mkdir(exist_ok=True)
        Path("hyx/generated_tests").mkdir(exist_ok=True)
        Path("hyx/results").mkdir(exist_ok=True)
        
        print("\n✓ 初始化完成")
        print("\n下一步:")
        print("  python hyx/start.py run      # 运行默认流程")
        print("  python hyx/start.py full     # 完整分析")
    
    elif command == "list":
        print("=== 框架能力列表 ===")
        run_command("list")
    
    elif command == "run":
        print("=== 运行默认工作流 ===")
        print("\n流程:")
        print("  1. 静态代码分析")
        print("  2. 交互场景识别")
        print("  3. 生成P0优先级测试")
        print()
        run_workflow("default")
    
    elif command == "full":
        print("=== 运行完整工作流 ===")
        print("\n流程:")
        print("  1. 静态代码分析")
        print("  2. 配置依赖分析")
        print("  3. API文档分析")
        print("  4. 交互场景识别")
        print("  5. 日志场景识别")
        print("  6. 历史缺陷场景")
        print("  7. 性能场景识别")
        print("  8. 生成所有优先级测试")
        print()
        run_workflow("full")
    
    elif command == "quick":
        print("=== 快速生成 ===")
        print("\n快速生成10个核心场景")
        print()
        run_workflow("quick")
    
    elif command == "interaction":
        print("=== 重点识别交互场景 ===")
        print("\n识别维度:")
        print("  - 调用链交互（代码静态分析）")
        print("  - 配置依赖（配置文件分析）")
        print("  - 运行时交互（日志分析）")
        print()
        print("生成场景:")
        print("  - 正常交互")
        print("  - 异常交互（网络故障、组件异常）")
        print("  - 边界交互（大数据、高频率）")
        print("  - 并发交互")
        print()
        run_workflow("interaction_focus")
    
    elif command == "p0":
        print("=== 生成P0优先级场景 ===")
        print()
        run_workflow("default", ["--priority", "P0"])
    
    elif command == "p1":
        print("=== 生成P1优先级场景 ===")
        print()
        run_workflow("default", ["--priority", "P1"])
    
    else:
        print(f"未知命令: {command}")
        print_help()


if __name__ == "__main__":
    main()