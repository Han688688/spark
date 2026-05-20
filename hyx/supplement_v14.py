#!/usr/bin/env python3
"""
补充遗漏的MLlib回归算法 - 第十四轮
"""
import re

MLLIB_REGRESSION = '''
### DecisionTreeRegressor
**包路径**: `org.apache.spark.ml.regression`
**说明**: 决策树回归器。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `DecisionTreeRegressor` | 无 | 构造方法 | 创建决策树回归器 | `DecisionTreeRegressor dt = new DecisionTreeRegressor();` |
| `setMaxDepth` | int value | `DecisionTreeRegressor` | 设置最大深度（默认5） | `dt.setMaxDepth(10);` |
| `setMaxBins` | int value | `DecisionTreeRegressor` | 设置最大分箱数（默认32） | `dt.setMaxBins(64);` |
| `setMinInstancesPerNode` | int value | `DecisionTreeRegressor` | 设置每个节点最小实例数 | `dt.setMinInstancesPerNode(1);` |
| `setMinInfoGain` | double value | `DecisionTreeRegressor` | 设置最小信息增益 | `dt.setMinInfoGain(0.0);` |
| `setFeaturesCol` | String value | `DecisionTreeRegressor` | 设置特征列名 | `dt.setFeaturesCol("features");` |
| `setLabelCol` | String value | `DecisionTreeRegressor` | 设置标签列名 | `dt.setLabelCol("label");` |
| `fit` | Dataset<?> dataset | `DecisionTreeRegressionModel` | 训练模型 | `DecisionTreeRegressionModel model = dt.fit(trainingData);` |

---

### GeneralizedLinearRegression
**包路径**: `org.apache.spark.ml.regression`
**说明**: 广义线性回归，支持多种分布族和链接函数。
**方法数量**: 10+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `GeneralizedLinearRegression` | 无 | 构造方法 | 创建广义线性回归 | `GeneralizedLinearRegression glr = new GeneralizedLinearRegression();` |
| `setFamily` | String value | `GeneralizedLinearRegression` | 设置分布族 | `glr.setFamily("gaussian");` |
| `setLink` | String value | `GeneralizedLinearRegression` | 设置链接函数 | `glr.setLink("identity");` |
| `setMaxIter` | int value | `GeneralizedLinearRegression` | 设置最大迭代次数（默认25） | `glr.setMaxIter(100);` |
| `setRegParam` | double value | `GeneralizedLinearRegression` | 设置正则化参数 | `glr.setRegParam(0.0);` |
| `setTol` | double value | `GeneralizedLinearRegression` | 设置收敛容忍度 | `glr.setTol(1e-6);` |
| `setFeaturesCol` | String value | `GeneralizedLinearRegression` | 设置特征列名 | `glr.setFeaturesCol("features");` |
| `setLabelCol` | String value | `GeneralizedLinearRegression` | 设置标签列名 | `glr.setLabelCol("label");` |
| `fit` | Dataset<?> dataset | `GeneralizedLinearRegressionModel` | 训练模型 | `GeneralizedLinearRegressionModel model = glr.fit(trainingData);` |
| `setWeightCol` | String value | `GeneralizedLinearRegression` | 设置权重列名 | `glr.setWeightCol("weight");` |

---

'''

def add_supplements(filepath):
    """补充遗漏的MLlib回归算法"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 在LinearRegression之后插入
    if "### LinearRegression" in content:
        lr_pos = content.find("### LinearRegression")
        next_class = content.find("\n### ", lr_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_REGRESSION + content[insert_pos:]
            added_count += 18  # 手动计算
            print(f"✅ 添加MLlib回归算法: DecisionTreeRegressor, GeneralizedLinearRegression")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的MLlib回归算法（第十四轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
