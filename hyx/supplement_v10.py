#!/usr/bin/env python3
"""
补充遗漏的UDF相关类 - 第十轮
"""
import re

UDF_CLASSES = '''
### UDF0[R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 无参数用户自定义函数接口。
**方法数量**: 1+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | 无 | `R` | 调用函数 | `public String call() { return "constant"; }` |

---

### UDF1[T, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 单参数用户自定义函数接口。
**方法数量**: 1+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T t | `R` | 调用函数 | `public Integer call(String s) { return s.length(); }` |

---

### UDF2[T1, T2, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 双参数用户自定义函数接口。
**方法数量**: 1+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2 | `R` | 调用函数 | `public String call(String a, String b) { return a + b; }` |

---

### UDF3[T1, T2, T3, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 三参数用户自定义函数接口。
**方法数量**: 1+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2, T3 t3 | `R` | 调用函数 | `public Double call(Double a, Double b, Double c) { return a + b + c; }` |

---

### UDF4[T1, T2, T3, T4, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 四参数用户自定义函数接口。
**方法数量**: 1+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2, T3 t3, T4 t4 | `R` | 调用函数 | - |

---

### UDF5[T1, T2, T3, T4, T5, R]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 五参数用户自定义函数接口（最大支持）。
**方法数量**: 1+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `call` | T1 t1, T2 t2, T3 t3, T4 t4, T5 t5 | `R` | 调用函数 | - |

---

### UDAF1[I, O]
**包路径**: `org.apache.spark.sql.api.java`
**说明**: 单输入用户自定义聚合函数接口（需要继承Aggregator）。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `zero` | 无 | `O` | 初始化聚合缓冲区 | `public Buffer zero() { return new Buffer(0, 0); }` |
| `reduce` | Buffer b, I i | `Buffer` | 减少输入到缓冲区 | `public Buffer reduce(Buffer b, Integer i) { b.sum += i; b.count++; return b; }` |
| `merge` | Buffer b1, Buffer b2 | `Buffer` | 合并两个缓冲区 | `public Buffer merge(Buffer b1, Buffer b2) { b1.sum += b2.sum; b1.count += b2.count; return b1; }` |
| `finish` | Buffer b | `Double` | 输出最终结果 | `public Double finish(Buffer b) { return b.sum / b.count; }` |

---

'''

def add_supplements(filepath):
    """补充遗漏的UDF类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 在SparkSession之后插入
    if "### SparkSession" in content:
        ss_pos = content.find("### SparkSession")
        next_class = content.find("\n### ", ss_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + UDF_CLASSES + content[insert_pos:]
            added_count += UDF_CLASSES.count('| `')
            print(f"✅ 添加UDF接口类: UDF0-UDF5, UDAF1")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的UDF类（第十轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
