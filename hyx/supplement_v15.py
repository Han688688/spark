#!/usr/bin/env python3
"""
补充遗漏的Window相关类 - 第十五轮
"""
import re

WINDOW_CLASSES = '''
### Window
**包路径**: `org.apache.spark.sql.expressions`
**说明**: 窗口函数定义工具类，用于创建WindowSpec。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `partitionBy` | Column... columns | `WindowSpec` | 按列分区 | `WindowSpec window = Window.partitionBy(col("category"));` |
| `partitionBy` | String... colNames | `WindowSpec` | 按列名分区 | `WindowSpec window = Window.partitionBy("category", "region");` |
| `orderBy` | Column... columns | `WindowSpec` | 按列排序 | `WindowSpec window = Window.orderBy(col("date"));` |
| `orderBy` | String... colNames | `WindowSpec` | 按列名排序 | `WindowSpec window = Window.orderBy("date", "time");` |
| `rangeBetween` | long start, long end | `WindowSpec` | 范围窗口（基于值） | `WindowSpec window = Window.orderBy("value").rangeBetween(-10, 10);` |
| `rowsBetween` | long start, long end | `WindowSpec` | 行窗口（基于行数） | `WindowSpec window = Window.orderBy("value").rowsBetween(-3, 3);` |
| `unboundedPreceding` | 无 | `long` | 无界起始 | `Window.rowsBetween(Window.unboundedPreceding(), Window.currentRow());` |
| `unboundedFollowing` | 无 | `long` | 无界结束 | `Window.rowsBetween(Window.currentRow(), Window.unboundedFollowing());` |

---

### WindowSpec
**包路径**: `org.apache.spark.sql.expressions`
**说明**: 窗口规范，定义窗口函数的计算范围。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `partitionBy` | Column... columns | `WindowSpec` | 按列分区 | `WindowSpec window = Window.partitionBy("category").orderBy("date");` |
| `partitionBy` | String... colNames | `WindowSpec` | 按列名分区 | `WindowSpec window = spec.partitionBy("region");` |
| `orderBy` | Column... columns | `WindowSpec` | 按列排序 | `WindowSpec window = spec.orderBy(col("value"));` |
| `orderBy` | String... colNames | `WindowSpec` | 按列名排序 | `WindowSpec window = spec.orderBy("value");` |
| `rangeBetween` | long start, long end | `WindowSpec` | 范围窗口 | `WindowSpec window = spec.rangeBetween(-100, 100);` |
| `rowsBetween` | long start, long end | `WindowSpec` | 行窗口 | `WindowSpec window = spec.rowsBetween(-5, 5);` |

---

'''

def add_supplements(filepath):
    """补充遗漏的Window相关类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 在functions之后插入
    if "### functions" in content or "### Functions" in content:
        func_start = content.find("### functions")
        if func_start == -1:
            func_start = content.find("### Functions")
        if func_start != -1:
            next_class = content.find("\n### ", func_start + 1)
            if next_class != -1:
                insert_pos = next_class
                content = content[:insert_pos] + "\n" + WINDOW_CLASSES + content[insert_pos:]
                added_count += 14  # 手动计算
                print(f"✅ 添加Window相关类: Window, WindowSpec")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的Window相关类（第十五轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
