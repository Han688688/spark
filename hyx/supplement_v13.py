#!/usr/bin/env python3
"""
补充遗漏的MLlib分类算法 - 第十三轮
"""
import re

MLLIB_CLASSIFICATION = '''
### MultilayerPerceptronClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 多层感知机分类器（神经网络），用于复杂分类任务。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `MultilayerPerceptronClassifier` | 无 | 构造方法 | 创建多层感知机分类器 | `MultilayerPerceptronClassifier mlp = new MultilayerPerceptronClassifier();` |
| `setLayers` | int[] layers | `MultilayerPerceptronClassifier` | 设置网络结构 | `mlp.setLayers(new int[]{4, 5, 4, 2});` |
| `setMaxIter` | int value | `MultilayerPerceptronClassifier` | 设置最大迭代次数（默认100） | `mlp.setMaxIter(100);` |
| `setBlockSize` | int value | `MultilayerPerceptronClassifier` | 设置块大小（默认128） | `mlp.setBlockSize(128);` |
| `setSeed` | long value | `MultilayerPerceptronClassifier` | 设置随机种子 | `mlp.setSeed(12345L);` |
| `setFeaturesCol` | String value | `MultilayerPerceptronClassifier` | 设置特征列名 | `mlp.setFeaturesCol("features");` |
| `setLabelCol` | String value | `MultilayerPerceptronClassifier` | 设置标签列名 | `mlp.setLabelCol("label");` |
| `setSolver` | String value | `MultilayerPerceptronClassifier` | 设置求解器 | `mlp.setSolver("l-bfgs");` |
| `setStepSize` | double value | `MultilayerPerceptronClassifier` | 设置步长 | `mlp.setStepSize(0.03);` |
| `fit` | Dataset<?> dataset | `MultilayerPerceptronClassificationModel` | 训练模型 | `MultilayerPerceptronClassificationModel model = mlp.fit(trainingData);` |

---

### LinearSVC
**包路径**: `org.apache.spark.ml.classification`
**说明**: 线性支持向量分类器，用于二分类任务。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `LinearSVC` | 无 | 构造方法 | 创建线性SVC | `LinearSVC svc = new LinearSVC();` |
| `setMaxIter` | int value | `LinearSVC` | 设置最大迭代次数（默认100） | `svc.setMaxIter(100);` |
| `setRegParam` | double value | `LinearSVC` | 设置正则化参数（默认0） | `svc.setRegParam(0.01);` |
| `setStandardization` | boolean value | `LinearSVC` | 是否标准化特征（默认true） | `svc.setStandardization(true);` |
| `setThreshold` | double value | `LinearSVC` | 设置阈值（默认0） | `svc.setThreshold(0.0);` |
| `setAggregationDepth` | int value | `LinearSVC` | 设置聚合深度（默认2） | `svc.setAggregationDepth(2);` |
| `setFeaturesCol` | String value | `LinearSVC` | 设置特征列名 | `svc.setFeaturesCol("features");` |
| `setLabelCol` | String value | `LinearSVC` | 设置标签列名 | `svc.setLabelCol("label");` |
| `fit` | Dataset<?> dataset | `LinearSVCModel` | 训练模型 | `LinearSVCModel model = svc.fit(trainingData);` |
| `setWeightCol` | String value | `LinearSVC` | 设置权重列名 | `svc.setWeightCol("weight");` |

---

### OneVsRest
**包路径**: `org.apache.spark.ml.classification`
**说明**: 一对多分类器，将二分类器转换为多分类器。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `OneVsRest` | 无 | 构造方法 | 创建一对多分类器 | `OneVsRest ovr = new OneVsRest();` |
| `setClassifier` | Classifier classifier | `OneVsRest` | 设置二分类器 | `ovr.setClassifier(new LogisticRegression());` |
| `setLabelCol` | String value | `OneVsRest` | 设置标签列名 | `ovr.setLabelCol("label");` |
| `setFeaturesCol` | String value | `OneVsRest` | 设置特征列名 | `ovr.setFeaturesCol("features");` |
| `setPredictionCol` | String value | `OneVsRest` | 设置预测列名 | `ovr.setPredictionCol("prediction");` |
| `fit` | Dataset<?> dataset | `OneVsRestModel` | 训练模型 | `OneVsRestModel model = ovr.fit(trainingData);` |
| `setParallelism` | int value | `OneVsRest` | 设置并行度 | `ovr.setParallelism(2);` |
| `copy` | ParamMap extra | `OneVsRest` | 复制分类器 | `OneVsRest copied = ovr.copy(new ParamMap());` |

---

'''

def add_supplements(filepath):
    """补充遗漏的MLlib分类算法"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 在NaiveBayes之后插入
    if "### NaiveBayes" in content:
        nb_pos = content.find("### NaiveBayes")
        next_class = content.find("\n### ", nb_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_CLASSIFICATION + content[insert_pos:]
            added_count += 28  # 手动计算
            print(f"✅ 添加MLlib分类算法: MultilayerPerceptronClassifier, LinearSVC, OneVsRest")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的MLlib分类算法（第十三轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
