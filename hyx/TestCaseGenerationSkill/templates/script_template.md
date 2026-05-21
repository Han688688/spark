# pytest自动化测试脚本模板

**版本**: v3.0  
**定位**: 定义pytest脚本的标准结构和格式规范

---

## 一、脚本结构规范

每个生成的pytest脚本必须包含以下5个部分：

```
1. 导入区    - 标准库导入 + 组件导入
2. 配置区    - 测试参数和fixture定义
3. 正常测试  - 正常流程测试方法
4. 异常测试  - 异常处理测试方法
5. 边界测试  - 边界值测试方法
```

---

## 二、脚本模板

### 2.1 导入区

```python
import pytest
import logging
from typing import Dict, Any

# 组件导入（根据实际场景替换）
# from your_components import KafkaClient, SparkApp, HDFSClient
```

### 2.2 配置区（fixture）

```python
@pytest.fixture(scope="class")
def setup_environment(self):
    """环境准备 - 根据preconditions填充"""
    logging.info("准备测试环境")
    
    env_config = {
        "component_A": {},
        "component_B": {},
    }
    
    yield env_config
    
    # 清理环境 - 根据cleanup字段填充
    logging.info("清理测试环境")
```

### 2.3 正常流程测试

```python
def test_{scenario}_normal_flow(self, setup_environment):
    """
    正常流程测试
    用例: {case_name}
    优先级: P0
    """
    # Step 1: {action_1}
    result_1 = {component}.{action}({input})
    assert {assertion_1}
    
    # Step 2: {action_2}
    result_2 = {component}.{action}(result_1)
    assert {assertion_2}
    
    # Step N: {action_N}
    # ...
```

### 2.4 异常处理测试

```python
def test_{scenario}_error_handling(self, setup_environment):
    """
    异常处理测试
    用例: {case_name}
    异常类型: {error_type}
    优先级: P0
    """
    # 模拟异常条件
    {component}.simulate_failure()
    
    # 验证异常捕获
    with pytest.raises({expected_exception}):
        {component}.{action}()
    
    # 验证异常处理机制
    assert {error_handling_assertion}
```

### 2.5 边界值测试

```python
def test_{scenario}_boundary_values(self, setup_environment):
    """
    边界值测试
    用例: {case_name}
    边界类型: {boundary_type}
    优先级: P1
    """
    # 使用边界数据
    boundary_data = {boundary_test_data}
    
    # 执行边界测试
    result = {component}.{action}(boundary_data)
    
    # 验证边界处理
    assert {boundary_assertion}
```

### 2.6 辅助函数

```python
def prepare_test_data(config: Dict) -> Any:
    """准备测试数据"""
    pass

def verify_result(actual, expected) -> bool:
    """验证结果"""
    return actual == expected

def cleanup_resources(resources: list):
    """清理资源"""
    pass
```

---

## 三、脚本命名规范

| 规则 | 说明 | 示例 |
|------|------|------|
| 文件名 | `test_{scenario}.py` | `test_spark_kafka_hdfs.py` |
| 类名 | `Test{Scenario}` | `TestSparkKafkaHDFS` |
| 方法名 | `test_{case_type}_{variant}` | `test_normal_flow_basic` |

---

## 四、完整脚本示例

```python
"""
Spark-Kafka-HDFS数据流自动化测试脚本
自动生成 - TestCaseGenerationSkill v3.0
"""

import pytest
import logging

class TestSparkKafkaHDFS:
    """
    场景: spark_kafka_hdfs_data_flow
    组件: Spark, Kafka, HDFS
    """
    
    @pytest.fixture(scope="class")
    def setup_environment(self):
        logging.info("准备环境: HDFS/Kafka/Spark正常运行")
        env = {"spark": {}, "kafka": {}, "hdfs": {}}
        yield env
        logging.info("清理环境")
    
    def test_normal_flow_basic(self, setup_environment):
        """正常流程 - P0"""
        # Step 1: Spark读取HDFS
        df = spark.read_from_hdfs("hdfs://test/input")
        assert df is not None
        
        # Step 2: Spark处理数据
        processed = spark.process_data(df)
        assert processed.count() > 0
        
        # Step 3: Kafka发送消息
        kafka.produce_message(processed, "output_topic")
        assert kafka.get_message_count("output_topic") == 100
    
    def test_error_handling_kafka_failure(self, setup_environment):
        """异常处理 - Kafka故障 - P0"""
        kafka.simulate_failure()
        
        with pytest.raises(KafkaConnectionException):
            kafka.produce_message(data, "output_topic")
        
        assert spark.get_retry_count() == 3
    
    def test_boundary_min_data(self, setup_environment):
        """边界值 - 最小数据量 - P1"""
        single_record = prepare_test_data({"size": 1})
        result = spark.process_data(single_record)
        assert result.count() == 1

if __name__ == "__main__":
    pytest.main([__file__, "-v"])
```

---

## 五、断言规范

断言类型映射（权威来源: testcase_spec.md零节6种assertion_type）:

| assertion_type | pytest语法 | 适用场景 |
|----------------|-----------|---------|
| value | `assert value == expected` | 验证字段值 |
| count | `assert count == expected` | 验证数量 |
| exception | `with pytest.raises(Exception)` | 验证异常 |
| function | `assert callable_executed()` 或 `assert result is not None` | 验证功能执行 |
| state | `assert obj.state == expected` | 验证状态变化 |
| file | `assert os.path.exists(path)` 或 `assert file_content == expected` | 验证文件存在/内容 |

---

**模板结束** - pytest脚本模板 v3.0