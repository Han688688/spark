#!/usr/bin/env python3
"""
补充遗漏的SQL高阶函数 - 第二十轮
"""
import re

SQL_HIGH_ORDER_FUNCTIONS = '''
| `call_udf` | String udfName, Column... cols | `Column` | 调用注册的UDF | `Column result = call_udf("my_udf", col("value"));` |
| `call_function` | String functionName, Column... cols | `Column` | 调用注册的函数 | `Column result = call_function("my_func", col("arg1"), col("arg2"));` |
| `forall` | Column array, Column predicate | `Column` | 判断数组所有元素是否满足条件 | `Column allPositive = forall(col("values"), x -> x.gt(0));` |
| `exists` | Column array, Column predicate | `Column` | 判断数组是否存在满足条件的元素 | `Column hasNegative = exists(col("values"), x -> x.lt(0));` |
| `zip_with` | Column left, Column right, BiFunction[Column, Column, Column] f | `Column` | 合并两个数组 | `Column zipped = zip_with(col("a"), col("b"), (x, y) -> x.plus(y));` |
| `inline` | Column array | `Column` | 展开数组中的struct为多列 | `df.select(inline(col("structs")));` |
| `inline_outer` | Column array | `Column` | 展开数组中的struct（含null） | `df.select(inline_outer(col("structs")));` |
| `array_min` | Column array | `Column` | 数组最小值 | `Column minVal = array_min(col("values"));` |
| `array_max` | Column array | `Column` | 数组最大值 | `Column maxVal = array_max(col("values"));` |
'''

def add_supplements(filepath):
    """补充遗漏的SQL高阶函数"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
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
                    content = content[:insert_pos] + SQL_HIGH_ORDER_FUNCTIONS + "\n" + content[insert_pos:]
                    added_count += 9  # 手动计算
                    print(f"✅ functions: 补充 9 个高阶函数")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的SQL高阶函数（第二十轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
