# StarRocks Java API 清单

> 基于StarRocks代码仓分析生成

## 官方文档参考

| 文档类型 | URL |
|---------|-----|
| **官方文档** | https://docs.starrocks.io |
| **Java UDF开发** | https://docs.starrocks.io/docs/development/Java_UDF (需搜索) |

---

## StarRocks Java扩展模块

StarRocks主要通过Java Extensions提供对外API，包括：

| 模块 | 路径 | 描述 |
|------|------|------|
| `jni-connector` | java-extensions/jni-connector | JNI扫描器框架 |
| `jdbc-bridge` | java-extensions/jdbc-bridge | JDBC数据源桥接 |
| `udf-extensions` | java-extensions/udf-extensions | Java UDF运行时 |
| `udf-examples` | java-extensions/udf-examples | UDF示例代码 |
| `hive-reader` | java-extensions/hive-reader | Hive数据源读取 |
| `hudi-reader` | java-extensions/hudi-reader | Hudi数据源读取 |
| `iceberg-metadata-reader` | java-extensions/iceberg-metadata-reader | Iceberg元数据读取 |
| `paimon-reader` | java-extensions/paimon-reader | Paimon数据源读取 |

---

## 1. JNI Connector API

**包路径:** `com.starrocks.jni.connector`

### 核心接口

| 类名 | 类型 | 描述 |
|------|------|------|
| `ConnectorScanner` | 抽象类 | 扫描器基类，需实现 open()/close()/getNext() |
| `ColumnValue` | 接口 | 列值读取接口 |
| `ColumnType` | 类 | 列类型定义 |
| `ScannerFactory` | 接口 | 扫描器工厂接口 |

---

## 2. JDBC Bridge API

**包路径:** `com.starrocks.jdbcbridge`

| 类名 | 描述 |
|------|------|
| `JDBCBridge` | JDBC桥接器，连接外部数据库 |
| `JDBCScanner` | JDBC扫描器 |
| `JDBCScanContext` | 扫描上下文 |

---

## 3. UDF Extensions API

**包路径:** `com.starrocks.udf`

| 类名 | 描述 |
|------|------|
| `UDFHelper` | UDF类型转换辅助类 |
| `UDFClassAnalyzer` | UDF类分析器 |
| `FunctionStates` | 函数状态管理 |

### UDF开发规范

用户自定义UDF只需实现 `evaluate()` 方法：

```java
public class UDFAdd {
    public Integer evaluate(Integer a, Integer b) {
        return a + b;
    }
}
```

---

## 4. 数据源读取器

### Hive Reader
**包路径:** `com.starrocks.jni.hive`

| 类名 | 描述 |
|------|------|
| `HiveScanner` | Hive数据源JNI读取器 |
| `HiveColumnValue` | Hive列值 |

### Hudi Reader
**包路径:** `com.starrocks.jni.hudi`

| 类名 | 描述 |
|------|------|
| `HudiScanner` | Hudi数据源读取器 |

### Iceberg Reader
**包路径:** `com.starrocks.jni.iceberg`

| 类名 | 描述 |
|------|------|
| `IcebergMetadataScanner` | Iceberg元数据读取器 |

### Paimon Reader
**包路径:** `com.starrocks.jni.paimon`

| 类名 | 描述 |
|------|------|
| `PaimonScanner` | Paimon数据源读取器 |

### ODPS Reader
**包路径:** `com.starrocks.jni.odps`

| 类名 | 描述 |
|------|------|
| `ODPSScanner` | ODPS/MaxCompute数据源读取器 |

### Kudu Reader
**包路径:** `com.starrocks.jni.kudu`

| 类名 | 描述 |
|------|------|
| `KuduScanner` | Kudu数据源读取器 |

---

## 5. FE SPI (服务提供者接口)

**包路径:** `com.starrocks.authentication`

| 接口名 | 描述 |
|--------|------|
| `AuthenticationProvider` | 认证提供者接口 |
| `AccessControlContext` | 访问控制上下文 |

**包路径:** `com.starrocks.catalog`

| 类名 | 描述 |
|------|------|
| `UserIdentity` | 用户身份类 |

**包路径:** `com.starrocks.common.io`

| 接口名 | 描述 |
|--------|------|
| `Writable` | 可序列化接口 |

**包路径:** `com.starrocks.common`

| 类名 | 描述 |
|------|------|
| `StarRocksException` | StarRocks异常基类 |
| `ErrorCode` | 错误码定义 |
| `InternalErrorCode` | 内部错误码 |

---

## 6. Connector Metadata API

**包路径:** `com.starrocks.connector`

| 接口名 | 描述 |
|--------|------|
| `Connector` | 数据源连接器接口 |
| `ConnectorMetadata` | 元数据操作接口 |

**包路径:** `com.starrocks.connector`

| 类名 | 描述 |
|------|------|
| `PartitionInfo` | 分区信息 |

---

## 7. Plugin System

**包路径:** `com.starrocks.plugin`

| 类/接口 | 描述 |
|---------|------|
| `Plugin` | 插件基类 |
| `AuditPlugin` | 审计插件接口 |

---

## 8. Authorization API

**包路径:** `com.starrocks.authorization`

| 接口名 | 描述 |
|--------|------|
| `AccessController` | 权限控制器接口 |

---

## 9. Cluster API

**包路径:** `com.starrocks.cluster`

| 类名 | 描述 |
|------|------|
| `ClusterNamespace` | 集群命名空间 |

---

## 10. Java Utils

**包路径:** `com.starrocks.common.util`

| 类名 | 描述 |
|------|------|
| `ChildFirstClassLoader` | 子类优先加载器 |
| `NativeMethodHelper` | Native方法辅助类 |
| `Platform` | 平台信息 |

---

## 11. Hadoop Extensions

**包路径:** `com.starrocks.hadoop`

| 类名 | 描述 |
|------|------|
| `HadoopCredentialConfig` | 云存储凭证配置 |
| `HadoopExtUtils` | Hadoop扩展工具 |

---

## 模块依赖关系

```
java-utils (基础工具)
    ↓
jni-connector (JNI连接框架)
    ↓
jdbc-bridge, hive-reader, hudi-reader, iceberg-reader, paimon-reader, odps-reader, kudu-reader
    ↓
udf-extensions (UDF运行时)
    ↓
fe-spi (FE扩展接口)
    ↓
fe-core (FE核心实现)
```

---

## 使用示例

### JDBC外部表查询

```java
// 通过StarRocks SQL访问JDBC外部表
// StarRocks内部使用JDBCBridge连接外部数据库
```

### UDF开发

```java
public class MyUDF {
    public String evaluate(String input) {
        return input.toUpperCase();
    }
}
```

### 自定义认证

```java
public class MyAuthProvider implements AuthenticationProvider {
    @Override
    public UserIdentity authenticate(String user, String password) {
        // 自定义认证逻辑
    }
}
```

---

## 参考链接

- StarRocks官方文档: https://docs.starrocks.io
- Java UDF开发指南: https://docs.starrocks.io/developing-java-udf