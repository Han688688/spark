#!/usr/bin/env python3
"""
补充遗漏的Accumulator和更多SQL函数 - 第十六轮
"""
import re

ACCUMULATOR_CLASSES = '''
### AccumulatorV2[T]
**包路径**: `org.apache.spark.util`
**说明**: 累加器V2版本，用于分布式计数和聚合。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | T v | `Unit` | 添加值 | `accumulator.add(1L);` |
| `value` | 无 | `T` | 获取值 | `long value = accumulator.value();` |
| `copy` | 无 | `AccumulatorV2[T]` | 复制累加器 | `AccumulatorV2<Long> copy = accumulator.copy();` |
| `isZero` | 无 | `boolean` | 是否为零 | `boolean isZero = accumulator.isZero();` |
| `reset` | 无 | `Unit` | 重置为零 | `accumulator.reset();` |
| `merge` | AccumulatorV2[T] other | `Unit` | 合并另一个累加器 | `accumulator.merge(otherAccumulator);` |

---

### DoubleAccumulator
**包路径**: `org.apache.spark.util`
**说明**: 双精度浮点数累加器。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | double v | `Unit` | 添加值 | `doubleAccumulator.add(3.14);` |
| `value` | 无 | `double` | 获取值 | `double value = doubleAccumulator.value();` |
| `reset` | 无 | `Unit` | 重置为零 | `doubleAccumulator.reset();` |
| `isZero` | 无 | `boolean` | 是否为零 | `boolean isZero = doubleAccumulator.isZero();` |

---

### CollectionAccumulator[T]
**包路径**: `org.apache.spark.util`
**说明**: 集合累加器，收集所有添加的元素。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `add` | T v | `Unit` | 添加元素 | `collectionAccumulator.add("element");` |
| `value` | 无 | `java.util.List[T]` | 获取所有元素 | `List<String> elements = collectionAccumulator.value();` |
| `reset` | 无 | `Unit` | 重置为空 | `collectionAccumulator.reset();` |
| `isZero` | 无 | `boolean` | 是否为空 | `boolean isZero = collectionAccumulator.isZero();` |

---

'''

MORE_SQL_FUNCTIONS = '''
| `repeat` | Column str, int n | `Column` | 重复字符串N次 | `Column repeated = repeat(col("text"), 3);` |
| `reverse` | Column str | `Column` | 反转字符串 | `Column reversed = reverse(col("text"));` |
| `element_at` | Column array, Column index | `Column` | 获取数组元素 | `Column elem = element_at(col("items"), lit(0));` |
| `array_except` | Column a1, Column a2 | `Column` | 数组差集 | `Column except = array_except(col("a"), col("b"));` |
| `array_intersect` | Column a1, Column a2 | `Column` | 数组交集 | `Column intersect = array_intersect(col("a"), col("b"));` |
| `array_union` | Column a1, Column a2 | `Column` | 数组并集 | `Column union = array_union(col("a"), col("b"));` |
| `array_remove` | Column array, Column element | `Column` | 移除数组元素 | `Column removed = array_remove(col("items"), lit("value"));` |
'''

def add_supplements(filepath):
    """补充遗漏的类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充Accumulator类 - 在LongAccumulator之后插入
    if "### LongAccumulator" in content:
        la_pos = content.find("### LongAccumulator")
        next_class = content.find("\n### ", la_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + ACCUMULATOR_CLASSES + content[insert_pos:]
            added_count += 14  # 手动计算
            print(f"✅ 添加Accumulator类: AccumulatorV2, DoubleAccumulator, CollectionAccumulator")
    
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
                    content = content[:insert_pos] + MORE_SQL_FUNCTIONS + "\n" + content[insert_pos:]
                    added_count += 7  # 手动计算
                    print(f"✅ functions: 补充 7 个数组函数")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的类（第十六轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
