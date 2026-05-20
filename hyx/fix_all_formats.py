#!/usr/bin/env python3
"""
修复所有格式问题
"""
import re

def fix_all_formats(filepath):
    """修复所有格式问题"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    changes = []
    
    # 1. 修复参数列格式错误 JFunction[Type: paramName -> paramName: JFunction[Type]
    patterns = [
        # JFunction[JDouble: f -> f: JFunction[JDouble]
        (r'\| `[^`]+` \| JFunction\[JDouble: f \|', r'| `filter` | f: JFunction[JDouble, JDouble] |'),
        # JFunction[(K: f -> f: JFunction[K, Boolean]
        (r'\| `[^`]+` \| JFunction\[\(K: f \|', r'| `filter` | f: JFunction[K, Boolean] |'),
        # JFunction[T: f -> f: JFunction[T, Boolean]
        (r'\| `[^`]+` \| JFunction\[T: f \|', r'| `filter` | f: JFunction[T, Boolean] |'),
        # JFunction2[V: func -> func: JFunction2[V, V, V]
        (r'\| `[^`]+` \| JFunction2\[V: func \|', r'| `reduceByKey` | func: JFunction2[V, V, V] |'),
    ]
    
    for pattern, replacement in patterns:
        old_count = len(re.findall(pattern, content))
        content = re.sub(pattern, replacement, content)
        if old_count > 0:
            changes.append(f"修复参数列格式: {old_count}处")
    
    # 2. 修复 T => T f 格式 -> f: T => T（箭头函数）
    content = re.sub(r'\| `[^`]+` \| T => T f \|', r'| `identity` | f: Function[T, T] |', content)
    
    # 3. 修复简单的参数格式 K key -> key: K
    content = re.sub(r'\| `[^`]+` \| K key \|', r'| `sampleByKey` | key: K |', content)
    
    # 4. 统一参数列格式为 paramName: Type（可选，因为Type paramName也是合理的）
    # 为了保持一致性，不做大规模修改
    
    # 5. 移除示例中的中文标点（可选，不影响使用）
    # 中文标点在描述中是合理的
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return changes

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("修复所有格式问题...")
    changes = fix_all_formats(filepath)
    
    for change in changes:
        print(f"  - {change}")
    
    print("\n完成")
