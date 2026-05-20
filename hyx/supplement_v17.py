#!/usr/bin/env python3
"""
补充遗漏的SQL Schema类和SQL函数 - 第十七轮
"""
import re

SQL_SCHEMA_CLASSES = '''
### StructType
**包路径**: `org.apache.spark.sql.types`
**说明**: DataFrame结构定义，包含多个StructField。
**方法数量**: 12+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StructType` | 无 | 构造方法 | 创建空结构 | `StructType schema = new StructType();` |
| `add` | StructField field | `StructType` | 添加字段 | `schema.add(new StructField("name", DataTypes.StringType, false, Metadata.empty()));` |
| `add` | String name, DataType dataType | `StructType` | 添加字段（简化） | `schema.add("age", DataTypes.IntegerType);` |
| `add` | String name, DataType dataType, boolean nullable | `StructType` | 添加字段（指定nullable） | `schema.add("id", DataTypes.LongType, false);` |
| `fields` | 无 | `StructField[]` | 获取所有字段 | `StructField[] fields = schema.fields();` |
| `fieldNames` | 无 | `String[]` | 获取所有字段名 | `String[] names = schema.fieldNames();` |
| `apply` | String name | `StructField` | 获取指定字段 | `StructField field = schema.apply("name");` |
| `apply` | int index | `StructField` | 获取指定位置字段 | `StructField field = schema.apply(0);` |
| `length` | 无 | `int` | 获取字段数量 | `int len = schema.length();` |
| `toDDL` | 无 | `String` | DDL格式字符串 | `String ddl = schema.toDDL();` |
| `json` | 无 | `String` | 转为JSON | `String json = schema.json();` |
| `prettyJson` | 无 | `String` | 转为格式化JSON | `String pretty = schema.prettyJson();` |

---

### StructField
**包路径**: `org.apache.spark.sql.types`
**说明**: 单个列结构定义，包含名称、类型、nullable等。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `StructField` | String name, DataType dataType, boolean nullable, Metadata metadata | 构造方法 | 创建字段 | `StructField field = new StructField("name", DataTypes.StringType, true, Metadata.empty());` |
| `name` | 无 | `String` | 获取字段名 | `String name = field.name();` |
| `dataType` | 无 | `DataType` | 获取数据类型 | `DataType type = field.dataType();` |
| `nullable` | 无 | `boolean` | 是否可空 | `boolean nullable = field.nullable();` |
| `metadata` | 无 | `Metadata` | 获取元数据 | `Metadata meta = field.metadata();` |
| `getComment` | 无 | `String` | 获取注释 | `String comment = field.getComment();` |
| `withComment` | String comment | `StructField` | 添加注释 | `StructField newField = field.withComment("用户名");` |
| `toDDL` | 无 | `String` | DDL格式 | `String ddl = field.toDDL();` |

---

'''

SQL_FUNCTIONS = '''
| `broadcast` | Column col | `Column` | 广播提示，优化join | `Dataset<Row> result = smallDF.join(bigDF, broadcast(smallDF.col("id")));` |
| `cbrt` | Column col | `Column` | 立方根 | `Column result = cbrt(col("value"));` |
| `ceiling` | Column col | `Column` | 向上取整（ceil别名） | `Column result = ceiling(col("value"));` |
| `chr` | int n | `Column` | ASCII码转字符 | `Column result = chr(65);  // 返回'A'` |
| `cosh` | Column col | `Column` | 双曲余弦 | `Column result = cosh(col("angle"));` |
| `cot` | Column col | `Column` | 余切 | `Column result = cot(col("angle"));` |
| `count_if` | Column condition | `Column` | 条件计数 | `Column count = count_if(col("value").gt(100));` |
| `convert_timezone` | String fromTz, String toTz, Column timestamp | `Column` | 时区转换 | `Column converted = convert_timezone("UTC", "Asia/Shanghai", col("timestamp"));` |
| `bitmap_construct_agg` | Column col | `Column` | 位图聚合 | `Column bitmap = bitmap_construct_agg(col("id"));` |
| `bitmap_count` | Column bitmap | `Column` | 位图计数 | `Column count = bitmap_count(col("bitmap"));` |
| `cardinality` | Column col | `Column` | 数组/Map大小 | `Column size = cardinality(col("items"));` |
| `covar_pop` | Column col1, Column col2 | `Column` | 总体协方差 | `Column cov = covar_pop(col("x"), col("y"));` |
| `covar_samp` | Column col1, Column col2 | `Column` | 样本协方差 | `Column cov = covar_samp(col("x"), col("y"));` |
'''

def add_supplements(filepath):
    """补充遗漏的类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充SQL Schema类 - 在DataType之后插入
    if "### DataType" in content:
        dt_pos = content.find("### DataType")
        next_class = content.find("\n### ", dt_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + SQL_SCHEMA_CLASSES + content[insert_pos:]
            added_count += 20  # 手动计算
            print(f"✅ 添加SQL Schema类: StructType, StructField")
    
    # 补充SQL函数
    if "### functions" in content or "### Functions" in content:
        func_start = content.find("### functions")
        if func_start == -1:
            func_start = content.find("### Functions")
        if func_start != -1:
            next_class = content.find("\n### ", func_start + 1)
            if next_class != -1:
                class_section = content[func_start:next_class]
                last_match = None
                for match in re.finditer(r'\| `[^`]+` \|.*?\n', class_section):
                    last_match = match
                if last_match:
                    insert_pos = func_start + last_match.end()
                    content = content[:insert_pos] + SQL_FUNCTIONS + "\n" + content[insert_pos:]
                    added_count += 13  # 手动计算
                    print(f"✅ functions: 补充 13 个函数")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的SQL Schema类和SQL函数（第十七轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
