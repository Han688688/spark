#!/usr/bin/env python3
"""
补充遗漏的MLlib算法类 - 第十九轮（修复版）
"""
import re

MLLIB_CLASSES = '''
### AFTSurvivalRegression
**包路径**: `org.apache.spark.ml.regression`
**说明**: 加速失效时间生存分析回归，用于生存时间预测。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `AFTSurvivalRegression` | 无 | 构造方法 | 创建AFT生存回归 | `AFTSurvivalRegression aft = new AFTSurvivalRegression();` |
| `setFeaturesCol` | String value | `AFTSurvivalRegression` | 设置特征列名 | `aft.setFeaturesCol("features");` |
| `setLabelCol` | String value | `AFTSurvivalRegression` | 设置标签列名（生存时间） | `aft.setLabelCol("time");` |
| `setCensorCol` | String value | `AFTSurvivalRegression` | 设置截尾列名 | `aft.setCensorCol("censor");` |
| `setMaxIter` | int value | `AFTSurvivalRegression` | 设置最大迭代次数（默认100） | `aft.setMaxIter(100);` |
| `setTol` | double value | `AFTSurvivalRegression` | 设置收敛容忍度（默认1E-6） | `aft.setTol(1e-6);` |
| `setAggregationDepth` | int value | `AFTSurvivalRegression` | 设置聚合深度 | `aft.setAggregationDepth(2);` |
| `setQuantileProbabilities` | double[] value | `AFTSurvivalRegression` | 设置分位数概率 | `aft.setQuantileProbabilities(new double[]{0.1, 0.5, 0.9});` |
| `setQuantilesCol` | String value | `AFTSurvivalRegression` | 设置分位数输出列名 | `aft.setQuantilesCol("quantiles");` |
| `fit` | Dataset<?> dataset | `AFTSurvivalRegressionModel` | 训练模型 | `AFTSurvivalRegressionModel model = aft.fit(data);` |

---

### FMRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 因子分解机回归器，用于推荐系统特征交叉建模。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `FMRegressor` | 无 | 构造方法 | 创建FM回归器 | `FMRegressor fm = new FMRegressor();` |
| `setFactorSize` | int value | `FMRegressor` | 设置因子维度（默认8） | `fm.setFactorSize(8);` |
| `setFitLinear` | boolean value | `FMRegressor` | 是否拟合线性项（默认true） | `fm.setFitLinear(true);` |
| `setRegParam` | double value | `FMRegressor` | 设置正则化参数（默认0） | `fm.setRegParam(0.01);` |
| `setMiniBatchFraction` | double value | `FMRegressor` | 设置小批量比例（默认1.0） | `fm.setMiniBatchFraction(0.5);` |
| `setInitStd` | double value | `FMRegressor` | 设置初始化标准差（默认0.01） | `fm.setInitStd(0.01);` |
| `setMaxIter` | int value | `FMRegressor` | 设置最大迭代次数 | `fm.setMaxIter(100);` |
| `fit` | Dataset<?> dataset | `FMRegressionModel` | 训练模型 | `FMRegressionModel model = fm.fit(data);` |

---

### FMClassifier
**包路径**: `org.apache.spark.ml.classification`
**说明**: 因子分解机分类器，用于推荐系统特征交叉建模分类。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `FMClassifier` | 无 | 构造方法 | 创建FM分类器 | `FMClassifier fm = new FMClassifier();` |
| `setFactorSize` | int value | `FMClassifier` | 设置因子维度（默认8） | `fm.setFactorSize(8);` |
| `setFitLinear` | boolean value | `FMClassifier` | 是否拟合线性项（默认true） | `fm.setFitLinear(true);` |
| `setRegParam` | double value | `FMClassifier` | 设置正则化参数（默认0） | `fm.setRegParam(0.01);` |
| `setMiniBatchFraction` | double value | `FMClassifier` | 设置小批量比例（默认1.0） | `fm.setMiniBatchFraction(0.5);` |
| `setInitStd` | double value | `FMClassifier` | 设置初始化标准差（默认0.01） | `fm.setInitStd(0.01);` |
| `setMaxIter` | int value | `FMClassifier` | 设置最大迭代次数 | `fm.setMaxIter(100);` |
| `fit` | Dataset<?> dataset | `FMClassificationModel` | 训练模型 | `FMClassificationModel model = fm.fit(data);` |

---

'''

def add_supplements(filepath):
    """补充遗漏的MLlib类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 在GeneralizedLinearRegression之后插入
    if "### GeneralizedLinearRegression" in content:
        glr_pos = content.find("### GeneralizedLinearRegression")
        next_class = content.find("\n### ", glr_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_CLASSES + content[insert_pos:]
            added_count += 26  # 手动计算
            print(f"✅ 添加MLlib类: AFTSurvivalRegression, FMRegressor, FMClassifier")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的MLlib类（第十九轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
