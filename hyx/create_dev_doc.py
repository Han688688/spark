#!/usr/bin/env python3
"""
生成开发者文档：包含移除的Connector/内部实现API
"""

import re

# 开发者API类（从用户文档移除的）
DEVELOPER_CLASSES = [
    'ColumnVector',
    'ColumnarBatch',
    'CustomAvgMetric',
    'CustomSumMetric', 
    'CustomTaskMetric',
    'Distributions',
    'Expressions',
    'ForeignKey',
    'PrimaryKey',
    'TableChange',
    'ViewChange',
    'WriteBuilder',
    # ... 其他开发者类
]

def create_developer_doc():
    """创建开发者文档"""
    # 读取原始高质量文档
    with open('/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md', 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 读取用户文档
    with open('/home/h00517772/spark/hyx/spark_java_api_用户文档.md', 'r', encoding='utf-8') as f:
        user_content = f.read()
    
    # 开发者文档开头
    dev_header = '''# Spark Java API 开发者文档

> **文档定位**: 数据源Connector开发者、插件开发者使用的public API
> **普通用户不需要这些API**

---

## 文档说明

本文档包含以下类型的API：

1. **Connector接口**: 实现自定义数据源需要调用
2. **列式存储API**: 向量化执行引擎内部使用
3. **约束定义API**: 元数据约束管理
4. **自定义度量API**: 监控指标定制
5. **内部服务API**: RPC、存储、调度内部实现

---

'''
    
    # 找出被移除的类块
    user_classes = set(re.findall(r'### (\w+)', user_content))
    all_classes = set(re.findall(r'### (\w+)', content))
    removed_classes = all_classes - user_classes
    
    # 从原文档中提取这些类
    dev_content = dev_header
    
    for class_name in removed_classes:
        pattern = rf'(### {class_name}\n.*?)(?=### |\n---|\n## |$)'
        match = re.search(pattern, content, re.DOTALL)
        if match:
            dev_content += match.group(1) + '\n\n'
    
    # 写入开发者文档
    with open('/home/h00517772/spark/hyx/spark_java_api_开发者文档.md', 'w', encoding='utf-8') as f:
        f.write(dev_content)
    
    print(f"开发者文档生成完成:")
    print(f"  - 类数量: {len(removed_classes)}")
    print(f"  - 输出: hyx/spark_java_api_开发者文档.md")

if __name__ == '__main__':
    create_developer_doc()