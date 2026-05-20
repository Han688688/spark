#!/usr/bin/env python3
"""
优化文档格式问题
"""
import re

def optimize_document(filepath):
    """优化文档格式"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    changes = []
    
    # 1. 将连续的<br><br>改为单个<br>
    old_count = content.count('<br><br>')
    content = content.replace('<br><br>', '<br>')
    if old_count > 0:
        changes.append(f"优化连续<br>标签: {old_count}处")
    
    # 2. 统一方法数量格式（移除+号）
    old_plus = len(re.findall(r'\*\*方法数量\*\*: \d+\+', content))
    content = re.sub(r'\*\*方法数量\*\*: (\d+)\+', r'**方法数量**: \1', content)
    if old_plus > 0:
        changes.append(f"统一方法数量格式: {old_plus}处")
    
    # 3. 移除多余的空行（连续3个以上空行改为2个）
    old_empty = len(re.findall(r'\n\n\n+', content))
    content = re.sub(r'\n\n\n+', '\n\n', content)
    if old_empty > 0:
        changes.append(f"移除多余空行: {old_empty}处")
    
    # 4. 补充空白示例
    blank_count_before = len(re.findall(r'\| - \|$', content))
    
    # 补充常见空白示例
    blank_replacements = {
        r'\| `mapPartitionsByKey` \|.*?\| - \|': '| `mapPartitionsByKey` | JFunction[Iterator[T], Iterator[U]] f | `JavaPairRDD[K, U]` | 按分区处理 | `pairRdd.mapPartitionsByKey(iter -> process(iter));` |',
        r'\| `flatMapValuesWithKey` \|.*?\| - \|': '| `flatMapValuesWithKey` | FlatMapFunction[K, V, U] f | `JavaPairRDD[K, U]` | 带key的flatMapValues | `pairRdd.flatMapValuesWithKey((k, v) -> {...});` |',
        r'\| `writeToMetadata` \|.*?\| - \|': '| `writeToMetadata` | String tableName | `DataFrameWriter[T]` | 写入元数据表 | `df.write().writeToMetadata("metadata_table");` |',
    }
    
    for pattern, replacement in blank_replacements.items():
        content = re.sub(pattern, replacement, content)
    
    blank_count_after = len(re.findall(r'\| - \|$', content))
    if blank_count_before > blank_count_after:
        changes.append(f"补充空白示例: {blank_count_before - blank_count_after}处")
    
    changes.append(f"剩余空白示例: {blank_count_after}处")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return changes

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("优化文档格式...")
    changes = optimize_document(filepath)
    
    for change in changes:
        print(f"  - {change}")
    
    print("\n完成优化")
