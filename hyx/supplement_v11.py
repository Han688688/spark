#!/usr/bin/env python3
"""
补充遗漏的SQL函数和MLlib类 - 第十一轮
"""
import re

# 补充SQL函数
SQL_FUNCTIONS = '''
| `sequence` | Column start, Column end | `Column` | 生成序列数组 | `Column seq = sequence(lit(1), lit(10));` |
| `sequence` | Column start, Column end, Column step | `Column` | 生成序列数组（指定步长） | `Column seq = sequence(lit(1), lit(10), lit(2));` |
| `array_prepend` | Column array, Column element | `Column` | 数组前面添加元素 | `Column arr = array_prepend(col("items"), lit("first"));` |
| `array_append` | Column array, Column element | `Column` | 数组后面添加元素 | `Column arr = array_append(col("items"), lit("last"));` |
| `arrays_overlap` | Column a1, Column a2 | `Column` | 数组是否有重叠元素 | `Column overlap = arrays_overlap(col("a"), col("b"));` |
| `shuffle` | Column array | `Column` | 随机打乱数组 | `Column shuffled = shuffle(col("items"));` |
| `character_length` | Column str | `Column` | 字符串字符数（别名） | `Column len = character_length(col("text"));` |
| `char_length` | Column str | `Column` | 字符串字符数（别名） | `Column len = char_length(col("text"));` |
| `octet_length` | Column str | `Column` | 字符串字节长度 | `Column len = octet_length(col("text"));` |
| `bit_length` | Column str | `Column` | 字符串位长度 | `Column len = bit_length(col("text"));` |
| `bit_get` | Column col, int pos | `Column` | 获取指定位置的位值 | `Column bit = bit_get(col("value"), 0);` |
| `bit_count` | Column col | `Column` | 计算位的数量 | `Column count = bit_count(col("value"));` |
| `levenshtein` | Column left, Column right | `Column` | 计算编辑距离 | `Column dist = levenshtein(col("str1"), col("str2"));` |
| `substring_index` | Column str, String delim, int count | `Column` | 子字符串索引 | `Column sub = substring_index(col("url"), ".", 2);` |
| `left` | Column str, int len | `Column` | 取左边N个字符 | `Column leftStr = left(col("text"), 5);` |
| `right` | Column str, int len | `Column` | 取右边N个字符 | `Column rightStr = right(col("text"), 5);` |
| `btrim` | Column str | `Column` | 去除两端空白（别名） | `Column trimmed = btrim(col("text"));` |
| `conv` | Column num, int fromBase, int toBase | `Column` | 进制转换 | `Column hex = conv(col("num"), 10, 16);` |
| `typeof` | Column col | `Column` | 返回类型字符串 | `Column type = typeof(col("value"));` |
| `stack` | int n, Column... cols | `Column` | 将多列堆叠为多行 | `Column stacked = stack(3, col("a"), col("b"), col("c"));` |
| `assert_true` | Column condition | `Column` | 断言条件为真 | `assert_true(col("value").gt(0));` |
| `raise_error` | String message | `Column` | 抛出错误 | `raise_error("Custom error message");` |
'''

# 补充DataFrameStatFunctions方法
STAT_FUNCTIONS = '''
| `freqItems` | String[] cols, double support | `Dataset[Row]` | 频繁项集挖掘 | `Dataset<Row> freq = df.stat().freqItems(new String[]{"category"}, 0.3);` |
| `freqItems` | String[] cols | `Dataset[Row]` | 频繁项集挖掘（默认support） | `Dataset<Row> freq = df.stat().freqItems(new String[]{"category"});` |
| `sampleBy` | String col, Map<K, Double> fractions, long seed | `Dataset[Row]` | 按列分层采样 | `Dataset<Row> sampled = df.stat().sampleBy("category", fractions, seed);` |
| `crosstab` | String col1, String col2 | `Dataset[Row]` | 交叉表 | `Dataset<Row> cross = df.stat().crosstab("category", "region");` |
| `cov` | String col1, String col2 | `double` | 协方差 | `double cov = df.stat().cov("x", "y");` |
| `approxQuantile` | String col, double[] probabilities, double relativeError | `double[]` | 近似分位数 | `double[] quantiles = df.stat().approxQuantile("value", new double[]{0.25, 0.5, 0.75}, 0.01);` |
'''

# 补充MLlib聚类算法
MLLIB_CLUSTERING = '''
### GaussianMixture
**包路径**: `org.apache.spark.ml.clustering`
**说明**: 高斯混合模型聚类，假设数据由多个高斯分布组成。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GaussianMixture` | 无 | 构造方法 | 创建高斯混合模型 | `GaussianMixture gm = new GaussianMixture();` |
| `setK` | int value | `GaussianMixture` | 设置聚类数（默认2） | `gm.setK(3);` |
| `setMaxIter` | int value | `GaussianMixture` | 设置最大迭代次数（默认100） | `gm.setMaxIter(50);` |
| `setTol` | double value | `GaussianMixture` | 设置收敛容忍度（默认0.01） | `gm.setTol(0.001);` |
| `setFeaturesCol` | String value | `GaussianMixture` | 设置特征列名 | `gm.setFeaturesCol("features");` |
| `setSeed` | long value | `GaussianMixture` | 设置随机种子 | `gm.setSeed(12345L);` |
| `fit` | Dataset<?> dataset | `GaussianMixtureModel` | 训练模型 | `GaussianMixtureModel model = gm.fit(data);` |
| `setAggregationDepth` | int value | `GaussianMixture` | 设置聚合深度 | `gm.setAggregationDepth(10);` |

---

'''

def add_supplements(filepath):
    """补充遗漏的内容"""
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
                    content = content[:insert_pos] + SQL_FUNCTIONS + "\n" + content[insert_pos:]
                    added_count += SQL_FUNCTIONS.count('| `')
                    print(f"✅ functions: 补充 22 个函数")
    
    # 补充DataFrameStatFunctions - 查找Dataset类中的stat方法部分
    if "### Dataset[T]" in content:
        ds_start = content.find("### Dataset[T]")
        next_class = content.find("\n### ", ds_start + 1)
        if next_class != -1:
            class_section = content[ds_start:next_class]
            # 找到stat方法之后插入
            stat_match = re.search(r'\| `stat` \|.*?\n', class_section)
            if stat_match:
                insert_pos = ds_start + stat_match.end()
                content = content[:insert_pos] + STAT_FUNCTIONS + "\n" + content[insert_pos:]
                added_count += STAT_FUNCTIONS.count('| `')
                print(f"✅ DataFrameStatFunctions: 补充 6 个方法")
    
    # 补充MLlib聚类算法 - 在BisectingKMeans之后插入
    if "### BisectingKMeans" in content:
        bkm_pos = content.find("### BisectingKMeans")
        next_class = content.find("\n### ", bkm_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_CLUSTERING + content[insert_pos:]
            added_count += MLLIB_CLUSTERING.count('| `')
            print(f"✅ 添加MLlib聚类算法: GaussianMixture")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的内容（第十一轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
