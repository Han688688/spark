#!/usr/bin/env python3
"""
修复所有剩余的格式问题
"""
import re

def fix_all_format(filepath):
    """修复所有剩余格式问题"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    changes = []
    
    # 1. 移除错误的分隔符行（在方法行末尾）
    old_count = len(re.findall(r'\| --------\|------\|----------\|------\|------\|', content))
    content = re.sub(r'\| --------\|------\|----------\|------\|------\|', '', content)
    changes.append(f"移除错误分隔符行: {old_count}处")
    
    # 2. 修复13列的行（方法行后面接了另一个方法行）
    # 查找并拆分这些行
    old_count = 0
    lines = content.split('\n')
    new_lines = []
    for line in lines:
        if line.startswith('| `'):
            # 检查是否有多个方法行合并在一起
            if '| | `' in line:
                # 拆分成两个行
                parts = line.split('| | `')
                first_part = parts[0] + '|'
                second_part = '| `' + parts[1]
                new_lines.append(first_part)
                new_lines.append(second_part)
                old_count += 1
            else:
                new_lines.append(line)
        else:
            new_lines.append(line)
    content = '\n'.join(new_lines)
    changes.append(f"拆分合并的方法行: {old_count}处")
    
    # 3. 替换示例代码中的中文标点（更精确的匹配）
    # 替换示例中的中文逗号（在代码注释中）
    lines = content.split('\n')
    new_lines = []
    for line in lines:
        if line.startswith('| `'):
            # 提取示例列（最后一列）
            parts = line.split('|')
            if len(parts) >= 6:
                # 示例列是最后一个非空部分
                example = parts[-1] if parts[-1].strip() else parts[-2]
                # 替换示例中的中文标点
                example = example.replace('，', ',')
                example = example.replace('：', ':')
                parts[-1] = example
                line = '|'.join(parts)
        new_lines.append(line)
    content = '\n'.join(new_lines)
    changes.append("替换示例中的中文标点")
    
    # 4. 确保所有行尾有|
    lines = content.split('\n')
    new_lines = []
    for line in lines:
        if line.startswith('| `') and not line.endswith('|'):
            line = line + '|'
        new_lines.append(line)
    content = '\n'.join(new_lines)
    changes.append("确保行尾有|")
    
    # 5. 移除连续的空行
    content = re.sub(r'\n\n\n+', '\n\n', content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return changes

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("修复所有剩余格式问题...")
    changes = fix_all_format(filepath)
    
    for change in changes:
        print(f"  - {change}")
    
    print("\n完成")
