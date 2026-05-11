#!/usr/bin/env python3
"""
重构 Spark Java API 文档：
1. 移除内部工具类（非用户直接调用的API）
2. 补充核心缺失方法
3. 添加SparkSession/Dataset Java API
4. 修正MLlib为真正的API而非示例程序
"""

import re

# 应该保留的核心类（用户直接使用的public API）
KEEP_CLASSES = {
    'JavaSparkContext',
    'JavaRDD',
    'JavaPairRDD', 
    'JavaDoubleRDD',
    'SparkSession',
    'Dataset',
    'DataFrame',
    'Column',
    'Row',
    'StructType',
    'StructField',
    'DataType',
    'StorageLevel',
    'Broadcast',
    'Accumulator',
    'JavaSparkStatusTracker',
    'SQLContext',
    'DataFrameReader',
    'DataFrameWriter',
    'StreamingQuery',
    'StreamingQueryManager',
}

# 应该移除的内部类（内部实现，用户不直接调用）
REMOVE_PACKAGE_PATTERNS = [
    r'org\.apache\.spark\.sql\.catalyst',  # 内部catalyst实现
    r'org\.apache\.spark\.sql\.execution',  # 内部execution实现
    r'org\.apache\.spark\.sql\.internal',   # 内部实现
    r'org\.apache\.spark\.examples',        # 示例代码，非API
]

# 内部工具类名称特征
REMOVE_CLASS_PATTERNS = [
    r'.*Utils$',           # xxxUtils工具类
    r'.*SerDe$',           # 序列化相关
    r'.*Ser$',             # 序列化相关
    r'.*Helper$',          # 辅助类
    r'.*Factory$',         # 工厂类（部分保留）
    r'.*Impl$',            # 实现类
    r'.*Builder$',         # Builder类（部分保留如SparkSession.Builder）
    r'.*Wrapper$',         # 包装类
    r'.*Adapter$',         # 适配器
    r'.*ExpressionUtils',  # 表达式工具
    r'.*Bitmap.*',         # 位图内部类
    r'.*Buffer.*Iterator', # 内部迭代器
    r'.*Columnar.*',       # 内部列式存储
    r'.*Vector.*',         # 内部向量（部分保留）
    r'AggregateHashMap',   # 内部哈希
    r'ArrayOf.*',          # 数组内部类
    r'Arrow.*',            # Arrow内部实现
    r'Avro.*',             # Avro内部实现（部分保留）
    r'Cast',               # 内部转换
    r'Changelog.*',        # 内部变更日志
    r'CharVarchar.*',      # 内部字符处理
    r'Check',              # 内部约束检查
    r'Collation.*',        # 内部collation
    r'Column.*Vector',     # 内部列向量
    r'Constant.*',         # 内部常量
    r'Custom.*',           # 自定义度量内部
    r'Delegating.*',       # 内部代理
    r'Default.*',          # 默认值内部
    r'Distribution.*',     # 分布相关
    r'Distributions',      # 分布工厂
    r'ExpressionUtils',    # 表达式工具
]

def should_remove_class(class_name, package_path):
    """判断是否应该移除该类"""
    # 检查包路径
    for pattern in REMOVE_PACKAGE_PATTERNS:
        if re.search(pattern, package_path):
            return True
    
    # 检查类名
    for pattern in REMOVE_CLASS_PATTERNS:
        if re.search(pattern, class_name):
            return True
    
    return False

def analyze_document(filepath):
    """分析文档结构"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # 提取所有类
    classes = re.findall(r'### (\w+)\n\*\*包路径\*\*: `([^`]+)`', content)
    
    keep_classes = []
    remove_classes = []
    
    for class_name, package_path in classes:
        if should_remove_class(class_name, package_path):
            remove_classes.append((class_name, package_path))
        else:
            keep_classes.append((class_name, package_path))
    
    return keep_classes, remove_classes, content

def generate_report(keep_classes, remove_classes):
    """生成分析报告"""
    report = []
    report.append("=" * 60)
    report.append("Spark Java API 文档分析报告")
    report.append("=" * 60)
    report.append("")
    
    report.append("## 应保留的核心API类（{}个）:".format(len(keep_classes)))
    for class_name, package_path in keep_classes[:20]:  # 只显示前20个
        report.append(f"  - {class_name} ({package_path})")
    if len(keep_classes) > 20:
        report.append(f"  ... 还有{len(keep_classes) - 20}个")
    report.append("")
    
    report.append("## 应移除的内部/工具类（{}个）:".format(len(remove_classes)))
    report.append("（这些类是内部实现，用户不直接调用）")
    for class_name, package_path in remove_classes[:30]:  # 只显示前30个
        report.append(f"  - {class_name} ({package_path})")
    if len(remove_classes) > 30:
        report.append(f"  ... 还有{len(remove_classes) - 30}个")
    report.append("")
    
    return '\n'.join(report)

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    keep_classes, remove_classes, content = analyze_document(filepath)
    
    report = generate_report(keep_classes, remove_classes)
    print(report)