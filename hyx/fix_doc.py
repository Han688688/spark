#!/usr/bin/env python3
"""
修正 spark_java_api_高质量完整文档.md 中的问题：
1. 删除重复的方法行
2. 为空白示例补充内容
3. 为不清晰描述（"xxx方法"）补充清晰描述
"""

import re

# 方法描述映射表 - 根据方法名推断功能
METHOD_DESCRIPTIONS = {
    'find': '在哈希表中查找指定key的位置，返回索引',
    'findOrInsert': '查找key位置，不存在则插入新条目',
    'getClassOfT': '获取泛型类型的Class对象',
    'sizeOf': '计算对象或数组占用的内存大小',
    'fromString': '从字符串解析枚举值或配置',
    'getCodecName': '获取压缩编解码器的名称',
    'getSupportCompressionLevel': '检查是否支持压缩级别配置',
    'lowerCaseName': '转换为小写的名称',
    'bitmapAndMerge': '对两个位图执行AND合并操作',
    'bitmapBitPosition': '计算位图中指定值的位位置',
    'bitmapBucketNumber': '计算位图中指定值的桶编号',
    'bitmapCount': '统计位图中设置的位数',
    'bitmapMerge': '合并两个位图',
    'append': '向缓冲区追加一行数据',
    'durationMs': '获取执行耗时（毫秒）',
    'hasNext': '检查迭代器是否还有下一元素',
    'incPeakExecutionMemory': '增加峰值执行内存计数',
    'next': '获取迭代器的下一个元素',
    'shouldStop': '检查是否应该停止迭代',
    'getSrid': '获取空间参考系统ID（SRID）',
    'getStringId': '将SRID转换为字符串标识',
    'dataType': '获取数据类型',
    'expression': '获取表达式对象',
    'expressionDataType': '获取表达式的数据类型',
    'computeUpdates': '是否计算更新操作',
    'deduplicationMode': '获取去重模式',
    'range': '获取变更日志范围',
    'charTypeWriteSideCheck': 'CHAR类型写入端校验，截断超长字符串',
    'readSidePadding': '读取端填充，补齐CHAR类型定长',
    'varcharTypeWriteSideCheck': 'VARCHAR类型写入端校验，截断超长字符串',
    'build': '构建约束对象',
    'predicate': '获取或设置断言条件',
    'predicateSql': '获取或设置断言SQL表达式',
    'binaryTrim': '二进制模式去除两端指定字符',
    'binaryTrimRight': '二进制模式去除右侧指定字符',
    'compareLowerCase': '比较两个字符串的小写形式',
    'findInSet': '在集合字符串中查找匹配项位置',
    'indexOf': '查找子串在字符串中的起始位置',
    'lowerCaseCodePoints': '获取小写形式的Unicode码点',
    'lowercaseContains': '忽略大小写检查是否包含子串',
    'lowercaseEndsWith': '忽略大小写检查是否以指定字符串结尾',
    'lowercaseIndexOf': '忽略大小写查找子串位置',
    'lowercaseReplace': '忽略大小写替换匹配的字符串',
    'lowercaseStartsWith': '忽略大小写检查是否以指定字符串开头',
    'lowercaseSubStringIndex': '忽略大小写的子串索引查找',
    'lowercaseTranslate': '忽略大小写的字符转换',
    'lowercaseTrim': '忽略大小写去除两端空白',
    'lowercaseTrimLeft': '忽略大小写去除左侧空白',
    'lowercaseTrimRight': '忽略大小写去除右侧空白',
    'replace': '替换字符串中匹配的内容',
    'subStringIndex': '查找分隔符分隔的子串索引',
    'toLowerCase': '转换为小写',
    'toTitleCase': '转换为标题大小写',
    'toTitleCaseICU': '使用ICU库转换为标题大小写',
    'toUpperCase': '转换为大写',
    'translate': '字符映射转换',
    'trim': '去除字符串两端空白',
    'trimLeft': '去除字符串左侧空白',
    'trimRight': '去除字符串右侧空白',
    'getValue': '获取列的默认值',
    'apply': '应用数据类型转换',
    'closeIfFreeable': '检查并释放可释放的资源',
    'getGeography': '获取地理空间数据值',
    'getGeometry': '获取几何空间数据值',
    'getInterval': '获取时间间隔值',
    'getStruct': '获取Struct类型数据',
    'getVariant': '获取Variant类型数据',
    'isDefinedAt': '检查数据类型是否定义',
    'populate': '填充常量列向量数据',
    'toBatch': '将行迭代器转换为列式批处理',
    'toJavaIntMap': '将ColumnarMap转换为Java Map',
    'aggregateTaskMetrics': '聚合任务级别的度量指标',
    'name': '获取度量指标名称',
    'value': '获取度量指标值',
    'createArrayType': '创建数组类型',
    'createCharType': '创建CHAR定长字符类型',
    'createDayTimeIntervalType': '创建日-时间间隔类型',
    'createDecimalType': '创建Decimal高精度数值类型',
    'createGeographyType': '创建地理空间类型',
    'createGeometryType': '创建几何空间类型',
    'createMapType': '创建Map类型',
    'createStructField': '创建结构字段',
    'createStructType': '创建结构类型',
    'createVarcharType': '创建VARCHAR变长字符类型',
    'createYearMonthIntervalType': '创建年-月间隔类型',
    'getExpression': '获取默认值表达式',
    'getSql': '获取默认值的SQL表示',
    'alterNamespace': '修改命名空间属性',
    'alterTable': '修改表结构或属性',
    'capabilities': '获取表目录支持的能力',
    'createNamespace': '创建命名空间',
    'createTable': '创建表',
    'dropNamespace': '删除命名空间',
    'dropTable': '删除表',
    'functionExists': '检查函数是否存在',
    'initialize': '初始化插件',
    'invalidateTable': '失效表缓存',
    'loadFunction': '加载函数',
    'loadNamespaceMetadata': '加载命名空间元数据',
    'loadTable': '加载表',
    'namespaceExists': '检查命名空间是否存在',
    'purgeTable': '彻底删除表（不可恢复）',
    'renameTable': '重命名表',
    'setDelegateCatalog': '设置代理目录',
    'tableExists': '检查表是否存在',
    'clustered': '创建聚类分布',
}

# 空白示例补充
EMPTY_EXAMPLE_COMMENTS = {
    'find': '在哈希表中查找指定key，返回索引位置',
    'findOrInsert': '查找key或插入新条目，返回MutableColumnarRow',
    'getClassOfT': '返回Decimal类型的Class对象',
    'sizeOf': '计算Decimal对象或数组的内存大小',
    'fromString': '从字符串解析Avro压缩编解码器类型',
    'getCodecName': '返回压缩编解码器名称（如"snappy"、"deflate"）',
    'getSupportCompressionLevel': '检查编解码器是否支持自定义压缩级别',
    'lowerCaseName': '返回编解码器名称的小写形式',
    'bitmapAndMerge': '对两个位图执行AND操作，返回交集位图',
    'bitmapBitPosition': '计算值在桶内的位位置（0-63）',
    'bitmapBucketNumber': '计算值所在的桶编号',
    'bitmapCount': '返回位图中设置的位数统计',
    'bitmapMerge': '合并两个位图，返回OR结果',
    'append': '向缓冲迭代器追加一行数据',
    'durationMs': '返回执行耗时（毫秒）',
    'hasNext': '检查迭代器是否还有下一行',
    'incPeakExecutionMemory': '增加峰值执行内存统计',
    'next': '获取迭代器下一行数据',
    'shouldStop': '检查是否应停止迭代处理',
    'getSrid': '将字符串空间参考ID转换为整数SRID',
    'getStringId': '将整数SRID转换为字符串标识',
    'dataType': '返回Cast目标的数据类型',
    'expression': '返回被转换的表达式对象',
    'expressionDataType': '返回源表达式的数据类型',
    'computeUpdates': '检查是否计算更新记录',
    'deduplicationMode': '返回去重模式配置',
    'range': '返回变更日志的时间范围',
    'charTypeWriteSideCheck': '校验CHAR类型写入，超长则截断',
    'readSidePadding': '读取端补齐CHAR定长字符串',
    'varcharTypeWriteSideCheck': '校验VARCHAR类型写入，超长则截断',
    'build': '构建Check约束对象',
    'predicate': '获取或设置断言条件',
    'predicateSql': '获取或设置断言SQL表达式',
    'binaryTrim': '去除字符串两端指定字符（二进制模式）',
    'binaryTrimRight': '去除字符串右侧指定字符（二进制模式）',
    'compareLowerCase': '比较两字符串小写形式，返回差值',
    'findInSet': '在逗号分隔集合中查找元素位置',
    'indexOf': '查找子串起始位置，支持指定起始索引',
    'lowerCaseCodePoints': '获取小写Unicode码点字符串',
    'lowercaseContains': '忽略大小写检查是否包含子串',
    'lowercaseEndsWith': '忽略大小写检查是否以指定结尾',
    'lowercaseIndexOf': '忽略大小写查找子串位置',
    'lowercaseReplace': '忽略大小写替换匹配内容',
    'lowercaseStartsWith': '忽略大小写检查是否以指定开头',
    'lowercaseSubStringIndex': '忽略大小写的子串索引',
    'lowercaseTranslate': '忽略大小写的字符映射转换',
    'lowercaseTrim': '忽略大小写去除两端空白',
    'lowercaseTrimLeft': '忽略大小写去除左侧空白',
    'lowercaseTrimRight': '忽略大小写去除右侧空白',
    'replace': '替换字符串中匹配内容',
    'subStringIndex': '按分隔符查找第N个子串',
    'toLowerCase': '转换为小写字符串',
    'toTitleCase': '转换为标题大小写（首字母大写）',
    'toTitleCaseICU': 'ICU库标题大小写转换',
    'toUpperCase': '转换为大写字符串',
    'translate': '按字符映射表转换字符串',
    'trim': '去除字符串两端空白',
    'trimLeft': '去除字符串左侧空白',
    'trimRight': '去除字符串右侧空白',
    'getValue': '返回列默认值的Literal对象',
    'apply': '获取数据类型对应的列向量',
    'closeIfFreeable': '检查并释放可释放的列向量资源',
    'getGeography': '获取地理空间数据值',
    'getGeometry': '获取几何空间数据值',
    'getInterval': '获取时间间隔数据',
    'getStruct': '获取Struct结构数据',
    'getVariant': '获取Variant变体数据',
    'isDefinedAt': '检查数据类型是否已定义',
    'populate': '填充常量列向量数据',
    'toBatch': '将行迭代器转为列式批处理',
    'toJavaIntMap': '将ColumnarMap转为Java整数Map',
    'aggregateTaskMetrics': '聚合任务度量指标为字符串',
    'name': '返回度量指标名称',
    'value': '返回度量指标数值',
    'createArrayType': '创建数组数据类型',
    'createCharType': '创建CHAR定长类型',
    'createDayTimeIntervalType': '创建日-时间间隔类型',
    'createDecimalType': '创建Decimal高精度类型',
    'createGeographyType': '创建地理空间类型',
    'createGeometryType': '创建几何空间类型',
    'createMapType': '创建Map映射类型',
    'createStructField': '创建结构字段定义',
    'createStructType': '创建结构类型定义',
    'createVarcharType': '创建VARCHAR变长类型',
    'createYearMonthIntervalType': '创建年-月间隔类型',
    'getExpression': '获取默认值表达式对象',
    'getSql': '获取默认值SQL字符串',
    'alterNamespace': '修改命名空间属性',
    'alterTable': '修改表结构',
    'capabilities': '返回目录支持的能力集合',
    'createNamespace': '创建命名空间',
    'createTable': '创建新表',
    'dropNamespace': '删除命名空间',
    'dropTable': '删除表',
    'functionExists': '检查函数是否存在',
    'initialize': '初始化目录插件',
    'invalidateTable': '失效表缓存',
    'loadFunction': '加载指定函数',
    'loadNamespaceMetadata': '加载命名空间元数据',
    'loadTable': '加载表对象',
    'namespaceExists': '检查命名空间是否存在',
    'purgeTable': '彻底删除表',
    'renameTable': '重命名表',
    'setDelegateCatalog': '设置代理目录',
    'tableExists': '检查表是否存在',
    'clustered': '创建聚类分布对象',
}

def fix_document(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    fixed_lines = []
    prev_content = None
    changes = {'duplicates_removed': 0, 'descriptions_fixed': 0, 'examples_added': 0}
    
    for i, line in enumerate(lines):
        content = line.rstrip('\n')
        
        # 检查是否是重复行（与前一行的内容完全相同）
        if content == prev_content and content.startswith('| `'):
            changes['duplicates_removed'] += 1
            continue
        
        # 提取方法名
        match = re.search(r'\| `(\w+)` \|', line)
        if match:
            method_name = match.group(1)
            
            # 修正描述（"xxx方法"形式）
            old_desc = f'{method_name}方法'
            if old_desc in line and method_name in METHOD_DESCRIPTIONS:
                new_desc = METHOD_DESCRIPTIONS[method_name]
                line = line.replace(old_desc, new_desc)
                changes['descriptions_fixed'] += 1
            
            # 补充空白示例（最后一个 |  | 结尾）
            if line.rstrip().endswith('|  |') and method_name in EMPTY_EXAMPLE_COMMENTS:
                example = EMPTY_EXAMPLE_COMMENTS[method_name]
                line = line.rstrip()
                # 替换最后一个 |  | 为有内容的示例
                line = re.sub(r'\|  \|$', f'| {example} |', line) + '\n'
                changes['examples_added'] += 1
        
        fixed_lines.append(line)
        prev_content = content
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.writelines(fixed_lines)
    
    return changes

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    changes = fix_document(filepath)
    print(f"修正完成:")
    print(f"  - 删除重复行: {changes['duplicates_removed']}")
    print(f"  - 修正描述: {changes['descriptions_fixed']}")
    print(f"  - 补充示例: {changes['examples_added']}")