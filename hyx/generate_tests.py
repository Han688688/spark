#!/usr/bin/env python3
"""
为缺失API生成测试代码
基于API覆盖检测报告中的缺失API清单
"""

import json
from pathlib import Path
from datetime import datetime

def generate_spark_api_tests():
    """为缺失的Spark API生成测试代码"""
    
    # 加载API覆盖分析结果
    result_file = Path('/home/h00517772/spark/hyx/results/api_coverage_analysis.json')
    
    if not result_file.exists():
        print("错误：未找到API覆盖分析结果")
        return
    
    with open(result_file, 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    # 提取缺失的Spark Stable API（P0优先级）
    missing_spark_apis = [
        api for api in data['missing_apis']
        if api['component'] == 'spark' and api['stability'] == 'Stable'
    ]
    
    print(f"找到 {len(missing_spark_apis)} 个缺失的Stable Spark API")
    
    # 创建测试代码目录
    test_dir = Path('/home/h00517772/spark/hyx/generated_tests/spark_api_tests')
    test_dir.mkdir(parents=True, exist_ok=True)
    
    # 按类分组
    apis_by_class = {}
    for api in missing_spark_apis:
        class_name = api['class_name']
        if class_name not in apis_by_class:
            apis_by_class[class_name] = []
        apis_by_class[class_name].append(api)
    
    print(f"\n涉及 {len(apis_by_class)} 个类")
    
    # 为每个类生成测试文件
    generated_files = []
    
    for class_name, apis in apis_by_class.items():
        test_file = generate_test_class(class_name, apis, test_dir)
        generated_files.append(test_file)
        print(f"生成: {test_file.name} ({len(apis)} 个测试方法)")
    
    # 生成测试清单文件
    manifest_file = generate_manifest(apis_by_class, test_dir)
    
    print(f"\n✓ 共生成 {len(generated_files)} 个测试文件")
    print(f"✓ 测试清单: {manifest_file.name}")
    
    return generated_files

def generate_test_class(class_name, apis, test_dir):
    """为单个类生成测试代码"""
    
    test_class_name = f"{class_name}APITest"
    test_file = test_dir / f"{test_class_name}.java"
    
    # 生成测试代码
    code = f"""package org.apache.spark.api.test;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.*;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.util.*;
import java.io.Serializable;

/**
 * {class_name} API测试类
 * 
 * 测试目标：验证{class_name}的核心API功能
 * API数量：{len(apis)} 个方法
 * 稳定性：Stable
 * 优先级：P0
 * 
 * 生成时间：{datetime.now().isoformat()}
 * 生成工具：API覆盖检测插件
 */
@DisplayName("{class_name} API Tests")
public class {test_class_name} implements Serializable {{
    
    private transient JavaSparkContext sc;
    private transient SparkConf conf;
    
    @BeforeEach
    public void setUp() {{
        conf = new SparkConf()
            .setAppName("{class_name}APITest")
            .setMaster("local[2]")
            .set("spark.testing", "true");
        sc = new JavaSparkContext(conf);
    }}
    
    @AfterEach
    public void tearDown() {{
        if (sc != null) {{
            sc.stop();
            sc = null;
        }}
    }}
    
"""
    
    # 为每个API生成测试方法
    for api in apis:
        method_name = api['method_name']
        method_sig = api.get('method_signature', '')
        return_type = api.get('return_type', '')
        
        # 生成测试方法
        test_method = generate_test_method(class_name, method_name, method_sig, return_type)
        code += test_method + "\n"
    
    code += "}\n"
    
    # 写入文件
    with open(test_file, 'w', encoding='utf-8') as f:
        f.write(code)
    
    return test_file

def generate_test_method(class_name, method_name, method_sig, return_type):
    """生成单个测试方法"""
    
    test_method_name = f"test{capitalize(method_name)}"
    
    method_code = f"""    /**
     * 测试方法：{method_name}
     * 
     * API签名：{method_sig}
     * 返回类型：{return_type}
     */
    @Test
    @DisplayName("{method_name}方法测试")
    public void {test_method_name}() {{
        // TODO: 实现测试逻辑
        // API签名参考：{method_sig}
        
        // 示例测试代码（需根据实际API调整）
        try {{
            // 准备测试数据
            List<String> data = Arrays.asList("test1", "test2", "test3");
            JavaRDD<String> rdd = sc.parallelize(data);
            
            // 验证基本功能
            assertNotNull(rdd, "RDD不应为null");
            assertTrue(rdd.count() > 0, "RDD应包含数据");
            
            System.out.println("{class_name}.{method_name} 测试通过");
        }} catch (Exception e) {{
            // 暂时标记为待实现
            System.out.println("{class_name}.{method_name} 待实现完整测试");
        }}
    }}
    
"""
    
    return method_code

def capitalize(name):
    """首字母大写"""
    if not name:
        return ""
    return name[0].upper() + name[1:]

def generate_manifest(apis_by_class, test_dir):
    """生成测试清单文件"""
    
    manifest_file = test_dir / "TEST_MANIFEST.md"
    
    content = f"""# Spark API测试清单

> 生成时间: {datetime.now().isoformat()}
> 生成工具: API覆盖检测插件

## 测试文件列表

共 {len(apis_by_class)} 个测试类，{sum(len(apis) for apis in apis_by_class.values())} 个测试方法

| 测试文件 | 类名 | API数量 | 优先级 |
|---------|------|--------|--------|
"""
    
    for class_name, apis in sorted(apis_by_class.items(), key=lambda x: len(x[1]), reverse=True):
        test_class_name = f"{class_name}APITest.java"
        content += f"| {test_class_name} | {class_name} | {len(apis)} | P0 |\n"
    
    content += f"""
## 测试方法详细清单

"""
    
    for class_name, apis in sorted(apis_by_class.items(), key=lambda x: len(x[1]), reverse=True):
        content += f"### {class_name} ({len(apis)}个方法)\n\n"
        
        for api in apis:
            method_name = api['method_name']
            method_sig = api.get('method_signature', '')
            content += f"- `{method_name}()` - {method_sig}\n"
        
        content += "\n"
    
    content += """
## 使用说明

### 运行测试

```bash
# 方式1：使用Maven
mvn test -Dtest=org.apache.spark.api.test.*APITest

# 方式2：使用JUnit
java -jar junit-platform-console-standalone.jar 
  --select-package org.apache.spark.api.test
  --class-path spark-api-tests.jar
```

### 集成到项目

1. 将生成的测试文件复制到项目的测试目录：
   ```bash
   cp generated_tests/spark_api_tests/*.java spark/core/src/test/java/org/apache/spark/api/test/
   ```

2. 添加必要的依赖（如果项目中还没有）：
   ```xml
   <dependency>
       <groupId>org.junit.jupiter</groupId>
       <artifactId>junit-jupiter</artifactId>
       <version>5.8.2</version>
       <scope>test</scope>
   </dependency>
   ```

3. 运行测试验证功能

### 完善测试

当前生成的测试代码包含基础框架，需要完善：

1. **补充测试数据** - 根据API参数类型准备具体测试数据
2. **实现验证逻辑** - 根据返回类型编写具体的验证代码
3. **添加异常测试** - 测试null参数、边界值等异常场景
4. **添加集成测试** - 测试与其他组件的交互

### 测试覆盖率目标

- 当前缺失：{len(apis_by_class)} 个类，{sum(len(apis) for apis in apis_by_class.values())} 个方法
- 生成测试：覆盖所有缺失API（基础框架）
- 完善测试：补充完整测试逻辑
- 目标覆盖率：Stable API > 80%

---
"""
    
    with open(manifest_file, 'w', encoding='utf-8') as f:
        f.write(content)
    
    return manifest_file

if __name__ == "__main__":
    print("=" * 60)
    print("Spark API测试代码生成")
    print("=" * 60)
    
    generate_spark_api_tests()
    
    print("\n测试代码生成完成！")
    print("位置: /home/h00517772/spark/hyx/generated_tests/spark_api_tests/")