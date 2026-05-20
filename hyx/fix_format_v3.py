#!/usr/bin/env python3
"""
修复重复示例列的问题
"""
import re

def fix_duplicate_columns(filepath):
    """修复重复示例列"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    changes = []
    
    lines = content.split('\n')
    new_lines = []
    
    for line in lines:
        if line.startswith('| `') and not line.startswith('| `方法名'):
            parts = line.split('|')
            
            # 正确格式应该是7列：| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
            # 8列表示示例重复了
            
            if len(parts) == 8:  # 示例列重复
                # 取前6个部分 + 最后一个|
                # parts[0]是空的，parts[1]到parts[6]是数据，parts[7]是重复的示例
                # 正确格式：| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
                new_parts = parts[:6]
                # 确保最后一个部分是示例
                if len(new_parts) >= 6:
                    # 清理示例列
                    example = new_parts[-1].strip()
                    if example:
                        new_line = '|' + '|'.join(new_parts[1:]) + '|'
                    else:
                        new_line = '|' + '|'.join(new_parts[1:-1]) + ' | - |'
                else:
                    new_line = line
                new_lines.append(new_line)
                continue
            
            if len(parts) == 13 or len(parts) == 14:  # 多个重复
                # 提取前6个部分
                new_parts = parts[:6]
                new_line = '|' + '|'.join(new_parts[1:]) + '|'
                new_lines.append(new_line)
                continue
            
            if len(parts) == 7:  # 正确格式
                new_lines.append(line)
                continue
            
            # 其他情况保持不变
            new_lines.append(line)
        else:
            new_lines.append(line)
    
    content = '\n'.join(new_lines)
    
    # 统计修复的行数
    fixed_count = len(lines) - len(new_lines) + len([l for l in new_lines if l.startswith('| `')])
    changes.append(f"修复重复示例列: {fixed_count}行")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return changes

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("修复重复示例列...")
    changes = fix_duplicate_columns(filepath)
    
    for change in changes:
        print(f"  - {change}")
    
    print("\n完成")
