#!/usr/bin/env python3
"""
补充遗漏的MLlib类 - 第七轮
"""
import re

MLLIB_TUNING = '''
### ParamGridBuilder
**包路径**: `org.apache.spark.ml.tuning`
**说明**: 参数网格构建器，用于构建超参数搜索空间。
**方法数量**: 3+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `ParamGridBuilder` | 无 | 构造方法 | 创建参数网格构建器 | `ParamGridBuilder gridBuilder = new ParamGridBuilder();` |
| `addGrid` | Param[T] param, T[] values | `ParamGridBuilder` | 添加参数网格 | `gridBuilder.addGrid(lr.regParam(), new Double[]{0.01, 0.1, 1.0});` |
| `build` | 无 | `ParamMap[]` | 构建参数网格 | `ParamMap[] paramMaps = gridBuilder.build();` |

---

### CrossValidator
**包路径**: `org.apache.spark.ml.tuning`
**说明**: K折交叉验证，用于模型选择和超参数调优。
**方法数量**: 8+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `CrossValidator` | 无 | 构造方法 | 创建交叉验证器 | `CrossValidator cv = new CrossValidator();` |
| `setEstimator` | Estimator<?> estimator | `CrossValidator` | 设置估计器 | `cv.setEstimator(lr);` |
| `setEstimatorParamMaps` | ParamMap[] paramMaps | `CrossValidator` | 设置参数网格 | `cv.setEstimatorParamMaps(paramGrid);` |
| `setEvaluator` | Evaluator evaluator | `CrossValidator` | 设置评估器 | `cv.setEvaluator(new BinaryClassificationEvaluator());` |
| `setNumFolds` | int value | `CrossValidator` | 设置折叠数（默认3） | `cv.setNumFolds(5);` |
| `setParallelism` | int value | `CrossValidator` | 设置并行度 | `cv.setParallelism(2);` |
| `fit` | Dataset<?> dataset | `CrossValidatorModel` | 执行交叉验证 | `CrossValidatorModel model = cv.fit(trainingData);` |
| `getBestModel` | 无 | `Model<?>` | 获取最佳模型 | `Model<?> best = cvModel.bestModel();` |

---

### CrossValidatorModel
**包路径**: `org.apache.spark.ml.tuning`
**说明**: 交叉验证后的模型，包含最佳模型和所有模型。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `bestModel` | 无 | `Model<?>` | 获取最佳模型 | `Model<?> best = cvModel.bestModel();` |
| `avgMetrics` | 无 | `double[]` | 获取平均指标 | `double[] metrics = cvModel.avgMetrics();` |
| `transform` | Dataset<?> dataset | `Dataset[Row]` | 使用最佳模型转换 | `Dataset<Row> predictions = cvModel.transform(testData);` |
| `write` | 无 | `MLWriter` | 保存模型 | `cvModel.write().overwrite().save("hdfs://model/cv");` |

---

### TrainValidationSplit
**包路径**: `org.apache.spark.ml.tuning`
**说明**: 单次训练验证分割，比交叉验证更快但更不稳定。
**方法数量**: 7+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `TrainValidationSplit` | 无 | 构造方法 | 创建训练验证分割器 | `TrainValidationSplit tvs = new TrainValidationSplit();` |
| `setEstimator` | Estimator<?> estimator | `TrainValidationSplit` | 设置估计器 | `tvs.setEstimator(lr);` |
| `setEstimatorParamMaps` | ParamMap[] paramMaps | `TrainValidationSplit` | 设置参数网格 | `tvs.setEstimatorParamMaps(paramGrid);` |
| `setEvaluator` | Evaluator evaluator | `TrainValidationSplit` | 设置评估器 | `tvs.setEvaluator(new RegressionEvaluator());` |
| `setTrainRatio` | double value | `TrainValidationSplit` | 设置训练比例（默认0.75） | `tvs.setTrainRatio(0.8);` |
| `setParallelism` | int value | `TrainValidationSplit` | 设置并行度 | `tvs.setParallelism(2);` |
| `fit` | Dataset<?> dataset | `TrainValidationSplitModel` | 执行训练验证分割 | `TrainValidationSplitModel model = tvs.fit(trainingData);` |

---

'''

MLLIB_FEATURES_EXTRA = '''
### Imputer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 缺失值填充器，使用均值或中位数填充缺失值。
**方法数量**: 6+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Imputer` | 无 | 构造方法 | 创建Imputer | `Imputer imputer = new Imputer();` |
| `setInputCols` | String[] value | `Imputer` | 设置输入列名 | `imputer.setInputCols(new String[]{"age", "income"});` |
| `setOutputCols` | String[] value | `Imputer` | 设置输出列名 | `imputer.setOutputCols(new String[]{"age_imputed", "income_imputed"});` |
| `setStrategy` | String value | `Imputer` | 设置填充策略 | `imputer.setStrategy("mean");` |
| `setMissingValue` | double value | `Imputer` | 设置缺失值标识 | `imputer.setMissingValue(Double.NaN);` |
| `fit` | Dataset<?> dataset | `ImputerModel` | 训练填充模型 | `ImputerModel model = imputer.fit(data);` |

---

### Binarizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 二值化器，将连续特征转换为二值（0/1）。
**方法数量**: 4+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `Binarizer` | 无 | 构造方法 | 创建Binarizer | `Binarizer binarizer = new Binarizer();` |
| `setInputCol` | String value | `Binarizer` | 设置输入列名 | `binarizer.setInputCol("feature");` |
| `setOutputCol` | String value | `Binarizer` | 设置输出列名 | `binarizer.setOutputCol("binary_feature");` |
| `setThreshold` | double value | `Binarizer` | 设置阈值（默认0.5） | `binarizer.setThreshold(0.5);` |

---

### QuantileDiscretizer
**包路径**: `org.apache.spark.ml.feature`
**说明**: 分位数离散化器，将连续特征按分位数分为多个桶。
**方法数量**: 5+

| 方法名 | 参数 | 返回类型 | 描述 | 示例 |
|--------|------|----------|------|------|
| `QuantileDiscretizer` | 无 | 构造方法 | 创建QuantileDiscretizer | `QuantileDiscretizer discretizer = new QuantileDiscretizer();` |
| `setInputCol` | String value | `QuantileDiscretizer` | 设置输入列名 | `discretizer.setInputCol("value");` |
| `setOutputCol` | String value | `QuantileDiscretizer` | 设置输出列名 | `discretizer.setOutputCol("bucket");` |
| `setNumBuckets` | int value | `QuantileDiscretizer` | 设置桶数量（默认10） | `discretizer.setNumBuckets(10);` |
| `fit` | Dataset<?> dataset | `BucketizerModel` | 训练模型 | `BucketizerModel model = discretizer.fit(data);` |

---

'''

def add_supplements(filepath):
    """补充遗漏的MLlib类"""
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    added_count = 0
    
    # 添加调优类 - 在RegressionEvaluator之后插入
    if "### RegressionEvaluator" in content:
        evaluator_pos = content.find("### RegressionEvaluator")
        next_class = content.find("\n### ", evaluator_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_TUNING + content[insert_pos:]
            added_count += MLLIB_TUNING.count('| `')
            print(f"✅ 添加调优类: CrossValidator, TrainValidationSplit, ParamGridBuilder")
    
    # 添加特征工程类 - 在Bucketizer之后插入
    if "### Bucketizer" in content:
        bucket_pos = content.find("### Bucketizer")
        next_class = content.find("\n### ", bucket_pos + 1)
        if next_class != -1:
            insert_pos = next_class
            content = content[:insert_pos] + "\n" + MLLIB_FEATURES_EXTRA + content[insert_pos:]
            added_count += MLLIB_FEATURES_EXTRA.count('| `')
            print(f"✅ 添加特征工程类: Imputer, Binarizer, QuantileDiscretizer")
    
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return added_count

if __name__ == '__main__':
    filepath = '/home/h00517772/spark/hyx/spark_java_api_高质量完整文档.md'
    
    print("补充遗漏的MLlib类（第七轮）...")
    count = add_supplements(filepath)
    print(f"\n总计补充 {count} 个方法")
