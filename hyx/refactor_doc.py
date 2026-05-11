#!/usr/bin/env python3
"""
重构 Spark Java API 文档
1. 移除内部类和示例程序
2. 补充缺失的核心方法
3. 重新组织结构
"""

import re

def is_internal_or_example(class_name, package_path):
    """判断是否是内部类或示例程序"""
    # 示例程序
    if 'examples' in package_path:
        return True
    
    # 内部实现包
    internal_packages = [
        'catalyst',
        'execution',
        'internal',
    ]
    for pkg in internal_packages:
        if pkg in package_path:
            return True
    
    # 内部工具类特征
    internal_patterns = [
        r'Utils$',
        r'SerDe$',
        r'Helper$',
        r'Impl$',
        r'Wrapper$',
        r'Adapter$',
        r'ExpressionUtils',
        r'Buffer.*Iterator',
        r'Columnar.*Vector',
        r'AggregateHashMap',
        r'ArrayOf.*',
        r'Arrow.*Vector',
        r'Bitmap.*',
        r'Cast',
        r'Changelog',
        r'CharVarchar.*',
        r'Collation.*',
        r'Constant.*Vector',
        r'Default.*',
        r'Delegating.*',
    ]
    
    for pattern in internal_patterns:
        if re.search(pattern, class_name):
            return True
    
    return False

def extract_class_block(content, class_name):
    """提取一个类的完整内容块"""
    pattern = rf'(### {class_name}\n.*?)(?=### \w+|---|$)'
    match = re.search(pattern, content, re.DOTALL)
    if match:
        return match.group(1)
    return None

def refactor_document(filepath):
    """重构文档"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 提取所有类名和包路径
    classes = re.findall(r'### (\w+)\n\*\*包路径\*\*: `([^`]+)`', content)
    
    removed_count = 0
    removed_sections = []
    
    for class_name, package_path in classes:
        if is_internal_or_example(class_name, package_path):
            block = extract_class_block(content, class_name)
            if block:
                content = content.replace(block, '')
                removed_count += 1
                removed_sections.append(f"{class_name} ({package_path})")
    
    # 清理多余的空行和分隔符
    content = re.sub(r'\n{3,}', '\n\n', content)
    
    # 更新统计信息
    stats_match = re.search(r'> \*\*统计\*\*: (\d+) 个方法', content)
    if stats_match:
        old_count = int(stats_match.group(1))
        # 估算移除的方法数（每个类约5-10个方法）
        estimated_removed_methods = removed_count * 8
        new_count = old_count - estimated_removed_methods
        content = content.replace(f'> **统计**: {old_count} 个方法', f'> **统计**: ~{new_count} 个方法')
    
    return content, removed_count, removed_sections

def add_missing_methods(filepath):
    """补充缺失的核心方法"""
    # 这个函数会在后续版本中实现
    # 目前先完成移除内部类的任务
    pass

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("开始重构文档...")
    new_content, removed_count, removed_sections = refactor_document(filepath)
    
    # 写回文件
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print(f"\n重构完成:")
    print(f"  - 移除 {removed_count} 个内部类/示例程序")
    print(f"\n移除的类:")
    for section in removed_sections[:20]:
        print(f"  - {section}")
    if len(removed_sections) > 20:
        print(f"  ... 还有 {len(removed_sections) - 20} 个")