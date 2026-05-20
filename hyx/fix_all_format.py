#!/usr/bin/env python3
"""
修复所有格式问题
"""
import re

def fix_format(filepath):
    """修复所有格式问题"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    changes = []
    
    # 1. 替换中文标点为英文标点（保留描述列中的中文）
    # 注意：只替换代码示例中的中文标点，不替换描述文字
    
    # 中文括号 → 英文括号（在示例代码中）
    old_count = content.count('（')
    # 只替换示例列中的中文括号（示例列是最后一个|之前的内容）
    # 使用正则替换示例中的中文括号
    content = re.sub(r'（([^）]*)）', r'(\1)', content)
    changes.append(f"替换中文括号: {old_count}处")
    
    # 中文逗号 → 英文逗号（在代码中）
    old_count = content.count('，')
    # 只在示例代码中替换（//注释后面）
    content = re.sub(r'//([^|]*)，', r'//\1,', content)
    changes.append(f"替换中文逗号（注释中）: 约{old_count}处")
    
    # 中文冒号 → 英文冒号（在注释中）
    old_count = content.count('：')
    content = re.sub(r'//([^|]*)：', r'//\1:', content)
    changes.append(f"替换中文冒号（注释中）: 约{old_count}处")
    
    # 2. 修复9列异常行（第4082行）
    # 查找并修复这行
    if '| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs, String initializationMode | `KMeansModel` | 指定初始化模式 |' in content:
        content = content.replace(
            '| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs, String initializationMode | `KMeansModel` | 指定初始化模式 |',
            '| `KMeans.train` | JavaRDD[Vector] data, int k, int maxIterations, int runs, String initializationMode | `KMeansModel` | 指定初始化模式 | `KMeansModel model = KMeans.train(data.rdd(), 3, 20, 1, "k-means||");` |'
        )
        changes.append("修复9列异常行: 1处")
    
    # 3. 移除参数列中的中文括号类型标注（如: Int → : Int）
    content = re.sub(r': Int（', r': Int(', content)
    content = re.sub(r'）', r')', content)
    
    # 4. 确保所有方法行的格式正确（7列）
    # 检查并修复格式
    lines = content.split('\n')
    fixed_lines = []
    for line in lines:
        if line.startswith('| `') and not line.startswith('| `方法名'):
            # 确保行尾有|
            if not line.endswith('|'):
                line = line + '|'
        fixed_lines.append(line)
    content = '\n'.join(fixed_lines)
    
    # 5. 统一空格格式
    # 移除多余的空格
    content = re.sub(r'\|\s\s+', '| ', content)
    content = re.sub(r'\s\s+\|', ' |', content)
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return changes

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("修复所有格式问题...")
    changes = fix_format(filepath)
    
    for change in changes:
        print(f"  - {change}")
    
    print("\n完成")
