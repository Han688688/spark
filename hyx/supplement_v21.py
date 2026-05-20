#!/usr/bin/env python3
"""
补充遗漏的MLlib参数和读写类 - 第二十一轮
"""
import re

MLLIB_IO_CLASSES = '''
### ParamMap
**包路径**: `org.apache.spark.ml.param`
**说明**: 参数映射，用于设置算法参数。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ParamMap` | 无 | 构造方法 | 创建空参数映射 | `ParamMap params = new ParamMap();` |
| `put` | Param[T] param, T value | `ParamMap` | 设置参数 | `params.put(lr.regParam(), 0.1);` |
| `put` | Pair[Param[T], T]... pairs | `ParamMap` | 设置多个参数 | `params.put(lr.regParam().w(0.1), lr.maxIter().w(100));` |
| `get` | Param[T] param | `T` | 获取参数值 | `double regParam = params.get(lr.regParam());` |
| `getOrDefault` | Param[T] param | `T` | 获取参数值或默认值 | `double value = params.getOrDefault(lr.regParam());` |
| `copy` | 无 | `ParamMap` | 复制参数映射 | `ParamMap copied = params.copy();` |
| `clear` | Param[T] param | `ParamMap` | 清除参数 | `params.clear(lr.regParam());` |
| `contains` | Param[T] param | `boolean` | 判断是否包含参数 | `boolean has = params.contains(lr.regParam());` |

---

### MLWriter
**包路径**: `org.apache.spark.ml.util`
**说明**: ML模型写入器，用于保存MLlib模型。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `save` | String path | `Unit` | 保存模型 | `model.write().save("hdfs://model/path");` |
| `overwrite` | 无 | `MLWriter` | 覆盖写入 | `model.write().overwrite().save("hdfs://model/path");` |
| `option` | String key, String value | `MLWriter` | 设置选项 | `model.write().option("compression", "gzip").save("path");` |
| `option` | String key, boolean value | `MLWriter` | 设置布尔选项 | `model.write().option("skipValidations", true).save("path");` |
| `session` | SparkSession session | `MLWriter` | 设置SparkSession | `model.write().session(spark).save("path");` |
| `context` | SparkContext context | `MLWriter` | 设置SparkContext | `model.write().context(spark.sparkContext()).save("path");` |

---

### MLReader[T]
**包路径**: `org.apache.spark.ml.util`
**说明**: ML模型读取器，用于加载MLlib模型。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `load` | String path | `T` | 加载模型 | `LogisticRegressionModel model = LogisticRegressionModel.load("hdfs://model/path");` |
| `option` | String key, String value | `MLReader[T]` | 设置选项 | `model.read().option("skipValidations", "true").load("path");` |
| `session` | SparkSession session | `MLReader[T]` | 设置SparkSession | `model.read().session(spark).load("path");` |
| `context` | SparkContext context | `MLReader[T]` | 设置SparkContext | `model.read().context(spark.sparkContext()).load("path");` |
| `isFileSystemLoaded` | 无 | `boolean` | 是否从文件系统加载 | `boolean loaded = reader.isFileSystemLoaded();` |

---

'''

def add_supplements(filepath):
    """补充遗漏的MLlib参数和读写类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 在ParamGridBuilder之后插入
    if "### ParamGridBuilder" in content:
        pg_pos = content.find("### ParamGridBuilder")
        next_class = content.find("\n### ", pg_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_IO_CLASSES + content[insert_pos:]
            added_count += 19  # 手动计算
            print(f"✅ 添加MLlib参数和读写类: ParamMap, MLWriter, MLReader")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的MLlib参数和读写类（第二十一轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
