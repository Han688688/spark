#!/usr/bin/env python3
"""
补充遗漏的MLlib算法类 - 第九轮（修复版）
"""
import re

MLLIB_ALGORITHMS = '''
### RandomForestClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 随机森林分类器，集成多个决策树进行分类。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `RandomForestClassifier` | 无 | 构造方法 | 创建随机森林分类器 | `RandomForestClassifier rf = new RandomForestClassifier();` |
| `setLabelCol` | String value | `RandomForestClassifier` | 设置标签列名 | `rf.setLabelCol("label");` |
| `setFeaturesCol` | String value | `RandomForestClassifier` | 设置特征列名 | `rf.setFeaturesCol("features");` |
| `setNumTrees` | int value | `RandomForestClassifier` | 设置树数量（默认20） | `rf.setNumTrees(50);` |
| `setMaxDepth` | int value | `RandomForestClassifier` | 设置最大深度（默认5） | `rf.setMaxDepth(10);` |
| `setMaxBins` | int value | `RandomForestClassifier` | 设置最大分箱数（默认32） | `rf.setMaxBins(64);` |
| `setImpurity` | String value | `RandomForestClassifier` | 设置不纯度度量 | `rf.setImpurity("gini");` |
| `setFeatureSubsetStrategy` | String value | `RandomForestClassifier` | 设置特征子集策略 | `rf.setFeatureSubsetStrategy("auto");` |
| `fit` | Dataset<?> dataset | `RandomForestClassificationModel` | 训练模型 | `RandomForestClassificationModel model = rf.fit(trainingData);` |
| `setSeed` | long value | `RandomForestClassifier` | 设置随机种子 | `rf.setSeed(12345L);` |

---

### GBTClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 梯度提升树分类器（Gradient-Boosted Trees），通过迭代训练决策树。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GBTClassifier` | 无 | 构造方法 | 创建GBT分类器 | `GBTClassifier gbt = new GBTClassifier();` |
| `setLabelCol` | String value | `GBTClassifier` | 设置标签列名 | `gbt.setLabelCol("label");` |
| `setFeaturesCol` | String value | `GBTClassifier` | 设置特征列名 | `gbt.setFeaturesCol("features");` |
| `setMaxIter` | int value | `GBTClassifier` | 设置迭代次数（默认20） | `gbt.setMaxIter(50);` |
| `setMaxDepth` | int value | `GBTClassifier` | 设置最大深度（默认5） | `gbt.setMaxDepth(10);` |
| `setMaxBins` | int value | `GBTClassifier` | 设置最大分箱数（默认32） | `gbt.setMaxBins(64);` |
| `setLearningRate` | double value | `GBTClassifier` | 设置学习率（默认0.1） | `gbt.setLearningRate(0.05);` |
| `setStepSize` | double value | `GBTClassifier` | 设置步长 | `gbt.setStepSize(0.1);` |
| `fit` | Dataset<?> dataset | `GBTClassificationModel` | 训练模型 | `GBTClassificationModel model = gbt.fit(trainingData);` |
| `setValidationTol` | double value | `GBTClassifier` | 设置验证容忍度 | `gbt.setValidationTol(0.01);` |

---

### RandomForestRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 随机森林回归器。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `RandomForestRegressor` | 无 | 构造方法 | 创建随机森林回归器 | `RandomForestRegressor rf = new RandomForestRegressor();` |
| `setLabelCol` | String value | `RandomForestRegressor` | 设置标签列名 | `rf.setLabelCol("label");` |
| `setFeaturesCol` | String value | `RandomForestRegressor` | 设置特征列名 | `rf.setFeaturesCol("features");` |
| `setNumTrees` | int value | `RandomForestRegressor` | 设置树数量（默认20） | `rf.setNumTrees(50);` |
| `setMaxDepth` | int value | `RandomForestRegressor` | 设置最大深度（默认5） | `rf.setMaxDepth(10);` |
| `setMaxBins` | int value | `RandomForestRegressor` | 设置最大分箱数（默认32） | `rf.setMaxBins(64);` |
| `setImpurity` | String value | `RandomForestRegressor` | 设置不纯度度量 | `rf.setImpurity("variance");` |
| `setFeatureSubsetStrategy` | String value | `RandomForestRegressor` | 设置特征子集策略 | `rf.setFeatureSubsetStrategy("auto");` |
| `fit` | Dataset<?> dataset | `RandomForestRegressionModel` | 训练模型 | `RandomForestRegressionModel model = rf.fit(trainingData);` |
| `setSeed` | long value | `RandomForestRegressor` | 设置随机种子 | `rf.setSeed(12345L);` |

---

### GBTRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 梯度提升树回归器。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GBTRegressor` | 无 | 构造方法 | 创建GBT回归器 | `GBTRegressor gbt = new GBTRegressor();` |
| `setLabelCol` | String value | `GBTRegressor` | 设置标签列名 | `gbt.setLabelCol("label");` |
| `setFeaturesCol` | String value | `GBTRegressor` | 设置特征列名 | `gbt.setFeaturesCol("features");` |
| `setMaxIter` | int value | `GBTRegressor` | 设置迭代次数（默认20） | `gbt.setMaxIter(50);` |
| `setMaxDepth` | int value | `GBTRegressor` | 设置最大深度（默认5） | `gbt.setMaxDepth(10);` |
| `setMaxBins` | int value | `GBTRegressor` | 设置最大分箱数（默认32） | `gbt.setMaxBins(64);` |
| `setLearningRate` | double value | `GBTRegressor` | 设置学习率（默认0.1） | `gbt.setLearningRate(0.05);` |
| `setStepSize` | double value | `GBTRegressor` | 设置步长 | `gbt.setStepSize(0.1);` |
| `fit` | Dataset<?> dataset | `GBTRegressionModel` | 训练模型 | `GBTRegressionModel model = gbt.fit(trainingData);` |
| `setValidationTol` | double value | `GBTRegressor` | 设置验证容忍度 | `gbt.setValidationTol(0.01);` |

---

'''

def add_supplements(filepath):
    """补充遗漏的MLlib算法类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 在LogisticRegressionModel之后插入
    if "### LogisticRegressionModel" in content:
        lr_pos = content.find("### LogisticRegressionModel")
        next_class = content.find("\n### ", lr_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_ALGORITHMS + content[insert_pos:]
            added_count += MLLIB_ALGORITHMS.count('| `')
            print(f"✅ 添加MLlib算法类: RandomForestClassifier, GBTClassifier, RandomForestRegressor, GBTRegressor")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的MLlib算法类（第九轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
