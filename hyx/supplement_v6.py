#!/usr/bin/env python3
"""
补充遗漏的方法和类 - 第六轮
"""
import re

# 补充functions内置函数 - 数学/字符串/哈希函数
FUNCTIONS_MATH = '''
| `signum` | Column col | `Column` | 符号函数 | `Column sign = signum(col("value"));` |
| `atan2` | Column y, Column x | `Column` | 双参数反正切 | `Column angle = atan2(col("y"), col("x"));` |
| `bin` | Column col | `Column` | 转为二进制字符串 | `Column binary = bin(col("num"));` |
| `hex` | Column col | `Column` | 转为十六进制字符串 | `Column hexStr = hex(col("num"));` |
| `unhex` | Column col | `Column` | 解析十六进制字符串 | `Column bytes = unhex(col("hex_str"));` |
| `degrees` | Column col | `Column` | 弧度转角度 | `Column deg = degrees(col("radians"));` |
| `radians` | Column col | `Column` | 角度转弧度 | `Column rad = radians(col("degrees"));` |
| `pmod` | Column a, Column b | `Column` | 正模运算（总是正数） | `Column mod = pmod(col("a"), col("b"));` |
| `shiftleft` | Column col, int numBits | `Column` | 左移位 | `Column shifted = shiftleft(col("num"), 2);` |
| `shiftright` | Column col, int numBits | `Column` | 右移位 | `Column shifted = shiftright(col("num"), 2);` |
| `shiftRightUnsigned` | Column col, int numBits | `Column` | 无符号右移位 | `Column shifted = shiftRightUnsigned(col("num"), 2);` |
'''

# 新增Pipeline和IndexToString类
MLLIB_PIPELINE = '''
### Pipeline
**包路径**: `org.apache.spark.ml`
**说明**: Pipeline是一个工作流，将多个Transformer和Estimator串联执行。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Pipeline` | 无 | 构造方法 | 创建Pipeline | `Pipeline pipeline = new Pipeline();` |
| `setStages` | PipelineStage... stages | `Pipeline` | 设置Pipeline阶段 | `pipeline.setStages(new PipelineStage[]{tokenizer, hashingTF, lr});` |
| `getStages` | 无 | `PipelineStage[]` | 获取Pipeline阶段 | `PipelineStage[] stages = pipeline.getStages();` |
| `fit` | Dataset<?> dataset | `PipelineModel` | 训练Pipeline | `PipelineModel model = pipeline.fit(trainingData);` |
| `copy` | ParamMap extra | `Pipeline` | 复制Pipeline | `Pipeline copied = pipeline.copy(new ParamMap());` |

---

### PipelineModel
**包路径**: `org.apache.spark.ml`
**说明**: Pipeline训练后的模型，包含多个Transformer。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行Pipeline转换 | `Dataset<Row> predictions = model.transform(testData);` |
| `stages` | 无 | `Transformer[]` | 获取所有阶段 | `Transformer[] stages = model.stages();` |
| `copy` | ParamMap extra | `PipelineModel` | 复制模型 | `PipelineModel copied = model.copy(new ParamMap());` |
| `write` | 无 | `MLWriter` | 保存模型 | `model.write().overwrite().save("hdfs://model/path");` |

---

### IndexToString
**包路径**: `org.apache.spark.ml.feature`
**说明**: 将数值索引还原为原始字符串标签（StringIndexer的逆操作）。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `IndexToString` | 无 | 构造方法 | 创建IndexToString | `IndexToString converter = new IndexToString();` |
| `setInputCol` | String value | `IndexToString` | 设置输入列名 | `converter.setInputCol("category_index");` |
| `setOutputCol` | String value | `IndexToString` | 设置输出列名 | `converter.setOutputCol("original_category");` |
| `setLabels` | String[] labels | `IndexToString` | 设置标签数组 | `converter.setLabels(new String[]{"cat", "dog", "bird"});` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 执行转换 | `Dataset<Row> converted = converter.transform(indexed);` |

---

'''

def add_supplements(filepath):
    """补充遗漏的方法和类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 补充functions
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
                    content = content[:insert_pos] + FUNCTIONS_MATH + "\n" + content[insert_pos:]
                    added_count += FUNCTIONS_MATH.count('| `')
                    print(f"✅ functions: 补充 11 个数学/字符串函数")
    
    # 添加Pipeline和IndexToString - 在StringIndexer之后插入
    if "### StringIndexer" in content:
        indexer_pos = content.find("### StringIndexer")
        next_class = content.find("\n### ", indexer_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_PIPELINE + content[insert_pos:]
            added_count += MLLIB_PIPELINE.count('| `')
            print(f"✅ 添加Pipeline和IndexToString类")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的方法和类（第六轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
