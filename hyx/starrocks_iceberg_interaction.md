# StarRocks与Iceberg交互集成

> 基于代码仓分析：StarRocks侧实现Iceberg Catalog集成

---

## 一、集成架构

```
┌─────────────────────────────────────────────────────────────┐
│                    Iceberg 表格式标准                         │
│  (开放表格式，提供Table/Catalog/Schema等接口定义)              │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              StarRocks作为查询引擎                            │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐       │
│  │   FE层       │  │   BE层       │  │ Java扩展     │       │
│  │ IcebergCatalog│  │ IcebergConnector│ │ JNI元数据扫描│       │
│  └──────────────┘  └──────────────┘  └──────────────┘       │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              Iceberg Catalog后端                             │
│  Hive Metastore | AWS Glue | REST Catalog | JDBC | Hadoop   │
└─────────────────────────────────────────────────────────────┘
                              ↓
┌─────────────────────────────────────────────────────────────┐
│              存储层                                           │
│  HDFS | S3 | Azure Blob | GCS | MinIO                        │
└─────────────────────────────────────────────────────────────┘
```

**关键点**: Iceberg不直接实现StarRocks适配，所有集成工作由StarRocks侧完成。

---

## 二、StarRocks侧集成模块

### 2.1 FE层集成

| 模块路径 | 类名 | 功能 |
|----------|------|------|
| `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/` | **IcebergCatalog** | Iceberg Catalog接口，支持表操作、分区查询 |
| 同上 | **IcebergConnector** | FE层Connector入口，创建管理Catalog |
| 同上 | **CachingIcebergCatalog** | 带缓存的Catalog，缓存表/分区/数据文件 |
| 同上 | **IcebergMetadata** | 元数据管理，表操作、扫描规划 |
| `fe/fe-core/src/main/java/org/apache/iceberg/` | **StarRocksIcebergTableScan** | 自定义表扫描，本地/分布式规划 |
| 同上 | **DeleteFileIndex** | Delete文件索引处理 |

### Catalog实现类

| 类名 | Catalog类型 | 说明 |
|------|-------------|------|
| **IcebergHiveCatalog** | Hive Metastore | 最常用，通过HMS管理Iceberg表 |
| **IcebergGlueCatalog** | AWS Glue | AWS环境使用 |
| **IcebergRESTCatalog** | REST Catalog | Tabular/Polaris等，支持OAuth2/JWT |
| **IcebergJdbcCatalog** | JDBC | MySQL/PostgreSQL后端 |
| **IcebergHadoopCatalog** | Hadoop | 文件系统目录管理 |

### 2.2 BE层集成

| 模块路径 | 文件 | 功能 |
|----------|------|------|
| `be/src/connector/` | **iceberg_connector.cpp** | BE层Connector实现 |
| 同上 | **iceberg_chunk_sink.h** | 数据写入(IcebergChunkSink) |
| 同上 | **iceberg_delete_sink.h** | 删除写入(IcebergDeleteSink) |

### 2.3 Java扩展模块（JNI）

| 模块路径 | 功能 |
|----------|------|
| `java-extensions/iceberg-metadata-reader/` | BE通过JNI调用Java读取Iceberg元数据表 |

**核心类**:

| 类名 | 功能 |
|------|------|
| **IcebergMetadataScannerFactory** | JNI Scanner工厂 |
| **IcebergSnapshotsTableScanner** | 扫描快照信息 |
| **IcebergFilesTableScanner** | 扫描数据文件和删除文件 |
| **IcebergManifestsTableScanner** | 扫描Manifest文件 |
| **IcebergPartitionsTableScanner** | 扫描分区信息 |
| **IcebergRefsTableScanner** | 扫描分支和标签 |
| **IcebergHistoryTableScanner** | 扫描表历史变更 |
| **IcebergPropertiesTableScanner** | 扫描表属性 |

---

## 三、StarRocks支持的Iceberg功能

### 3.1 核心功能支持

| 功能 | 支持版本 | 说明 |
|------|----------|------|
| **Iceberg Catalog集成** | v2.4+ | Hive/Glue/REST/JDBC/Hadoop |
| **Iceberg表读取** | v2.4+ | Parquet/ORC格式 |
| **Iceberg表写入** | v3.1+ | INSERT INTO/OVERWRITE |
| **元数据表查询** | v3.4.1+ | $snapshots/$files/$partitions等 |
| **时间旅行查询** | v3.4+ | VERSION AS OF / TIMESTAMP AS OF |
| **分支和标签管理** | v3.4+ | CREATE BRANCH/TAG |
| **异步物化视图** | v2.5+ | 基于Iceberg表的MV |

### 3.2 Iceberg表版本支持

| 表版本 | 特性 | StarRocks支持 |
|--------|------|--------------|
| **V1表** | 基础表格式 | ✅ 完全支持 (Parquet/ORC) |
| **V2表** | Position Deletes | ✅ Parquet(v3.1+) / ORC(v3.0+) |
| **V2表** | Equality Deletes | ✅ Parquet(v3.1.10+) / ORC(v3.1.8+) |
| **V3表** | Row Lineage | ✅ v4.1+ |

### 3.3 文件格式支持

| 格式 | 压缩格式 | 支持版本 |
|------|----------|----------|
| **Parquet** | SNAPPY/LZ4/ZSTD/GZIP/NO_COMPRESSION | v2.4+ |
| **ORC** | ZLIB/SNAPPY/LZO/LZ4/ZSTD/NO_COMPRESSION | v2.4+ |

### 3.4 DDL支持

| 操作 | 支持版本 | 说明 |
|------|----------|------|
| **CREATE DATABASE** | v3.1+ | REST/Hive/JDBC Catalog |
| **CREATE TABLE** | v3.1+ | 同上 |
| **CREATE TABLE AS SELECT** | v3.1+ | CTAS |
| **DROP DATABASE/TABLE** | v3.1+ | 同上 |
| **ALTER TABLE** | v3.4+ | 分支/标签管理 |

### 3.5 存储过程

| 过程名 | 功能 | 代码位置 |
|--------|------|----------|
| **expire_snapshots** | 过期旧快照 | `ExpireSnapshotsProcedure.java` |
| **rollback_to_snapshot** | 回滚到快照 | `RollbackToSnapshotProcedure.java` |
| **fast_forward** | 快进到分支快照 | `FastForwardProcedure.java` |
| **cherry_pick_snapshot** | 选择性应用快照 | `CherryPickSnapshotProcedure.java` |
| **remove_orphan_files** | 清理孤立文件 | `RemoveOrphanFilesProcedure.java` |
| **rewrite_data_files** | 数据文件Compaction | `RewriteDataFilesProcedure.java` |
| **rewrite_manifests** | Manifest重写 | `RewriteManifestsProcedure.java` |
| **add_files** | 添加外部文件 | `AddFilesProcedure.java` |
| **register_table** | 注册已有表 | `RegisterTableProcedure.java` |

---

## 四、使用示例

### 4.1 创建Iceberg Catalog

```sql
-- Hive Metastore Catalog
CREATE EXTERNAL CATALOG iceberg_hms
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift:// metastore:9083"
);

-- AWS Glue Catalog
CREATE EXTERNAL CATALOG iceberg_glue
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "glue",
    "aws.region" = "us-east-1",
    "aws.s3.use_instance_profile" = "true"
);

-- REST Catalog (Tabular/Polaris)
CREATE EXTERNAL CATALOG polaris
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "rest",
    "iceberg.catalog.uri" = "http://polaris:8181/api/catalog",
    "iceberg.catalog.security" = "oauth2",
    "iceberg.catalog.oauth2.credential" = "client:secret",
    "iceberg.catalog.warehouse" = "warehouse_name"
);

-- JDBC Catalog (MySQL后端)
CREATE EXTERNAL CATALOG iceberg_jdbc
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "jdbc",
    "iceberg.catalog.uri" = "jdbc:mysql://localhost:3306/iceberg_db",
    "iceberg.catalog.jdbc.user" = "root",
    "iceberg.catalog.jdbc.password" = "password"
);
```

### 4.2 查询Iceberg表

```sql
-- 设置Catalog
USE iceberg_hms.db_name;

-- 基础查询
SELECT * FROM iceberg_table LIMIT 10;

-- 时间旅行 - 指定快照ID
SELECT * FROM iceberg_table VERSION AS OF 123456789;

-- 时间旅行 - 指定分支
SELECT * FROM iceberg_table VERSION AS OF 'dev-branch';

-- 时间旅行 - 指定标签
SELECT * FROM iceberg_table VERSION AS OF 'release-1.0';

-- 时间旅行 - 指定时间戳
SELECT * FROM iceberg_table TIMESTAMP AS OF '2024-01-01 00:00:00';
```

### 4.3 元数据表查询

```sql
-- 查看快照信息
SELECT * FROM iceberg_table$snapshots;

-- 查看文件信息
SELECT * FROM iceberg_table$files;

-- 查看分区信息
SELECT * FROM iceberg_table$partitions;

-- 查看Manifest信息
SELECT * FROM iceberg_table$manifests;

-- 查看分支和标签
SELECT * FROM iceberg_table$refs;

-- 查看历史变更
SELECT * FROM iceberg_table$history;
```

### 4.4 写入Iceberg表

```sql
-- INSERT INTO
INSERT INTO iceberg_hms.db_name.iceberg_table
SELECT * FROM source_table;

-- INSERT OVERWRITE
INSERT OVERWRITE iceberg_hms.db_name.iceberg_table
SELECT * FROM source_table;

-- 创建Iceberg表
CREATE TABLE iceberg_hms.db_name.new_table (
    id INT,
    name STRING,
    created_at TIMESTAMP
) PARTITION BY (created_at)
PROPERTIES (
    "write.format.default" = "parquet",
    "write.compression.default" = "zstd"
);
```

### 4.5 分支和标签管理

```sql
-- 创建分支
ALTER TABLE iceberg_table 
CREATE BRANCH 'dev-branch' 
AS OF VERSION 123456789 
RETAIN 7 DAYS;

-- 创建标签
ALTER TABLE iceberg_table 
CREATE TAG 'release-1.0' 
AS OF VERSION 123456789 
RETAIN 30 DAYS;

-- 删除分支
ALTER TABLE iceberg_table DROP BRANCH 'dev-branch';

-- 删除标签
ALTER TABLE iceberg_table DROP TAG 'release-1.0';
```

### 4.6 存储过程调用

```sql
-- 过期快照
CALL iceberg_hms.db_name.expire_snapshots('2024-01-01 00:00:00');

-- 数据文件Compaction
CALL iceberg_hms.db_name.rewrite_data_files(rewrite_all=true);

-- 清理孤立文件
CALL iceberg_hms.db_name.remove_orphan_files('2024-01-01 00:00:00');

-- 回滚快照
CALL iceberg_hms.db_name.rollback_to_snapshot(123456789);
```

---

## 五、Iceberg侧文档

Iceberg官方文档将StarRocks列为第三方集成：

| 文档位置 | 内容 |
|----------|------|
| `site/docs/vendors.md` | CelerData提供StarRocks商业版，作为Iceberg Lakehouse查询引擎 |
| `site/nav.yml` | 导航栏包含StarRocks外链 |
| 外链 | https://docs.starrocks.io/en-us/latest/data_source/catalog/iceberg_catalog |

---

## 六、限制和不支持的功能

### 6.1 Catalog限制

| Catalog类型 | DDL支持 | 说明 |
|-------------|---------|------|
| REST Catalog | ✅ 支持 | 创建/删除数据库和表 |
| Hive Catalog | ✅ 支持 | 同上 |
| JDBC Catalog | ✅ 支持 | 同上 |
| **Hadoop Catalog** | ❌ 不支持 | 不能创建/删除数据库和表 |
| Glue Catalog | ❌ 不支持DDL | 仅读取 |

### 6.2 写入限制

| 项目 | 限制 |
|------|------|
| **文件格式** | 只支持写入Parquet格式 |
| **压缩格式** | Parquet: SNAPPY/LZ4/ZSTD/GZIP/NO_COMPRESSION |
| **非Parquet表** | 不支持写入ORC格式的Iceberg表 |

### 6.3 分区转换限制

| 项目 | 说明 |
|------|------|
| **add_files存储过程** | 不支持非identity分区转换的分区表 |

### 6.4 元数据缓存注意

| 项目 | 说明 |
|------|------|
| **默认启用缓存** | 元数据缓存默认开启，可能有分钟级数据延迟 |
| **即时可见** | 设置`iceberg_meta_cache_ttl_sec=0`可取消缓存 |
| **Vended Credentials** | 不建议开启表缓存(credentials可能过期) |

### 6.5 Delete支持版本矩阵

| Delete类型 | Parquet | ORC |
|------------|---------|-----|
| **Position Delete** | v3.1+ | v3.0+ |
| **Equality Delete** | v3.1.10+/v3.2.5+/v3.3+ | v3.1.8+/v3.2.3+/v3.3+ |

---

## 七、性能优化建议

### 7.1 Data Cache

```sql
-- 启用Data Cache（默认开启）
-- 配置BE的storage_root_path指定缓存目录

-- 查看缓存统计
SHOW CACHE STATISTICS;
```

### 7.2 物化视图

```sql
-- 创建基于Iceberg表的物化视图
CREATE MATERIALIZED VIEW mv_iceberg_agg
REFRESH ASYNC START('2024-01-01') EVERY(INTERVAL 1 DAY)
AS SELECT 
    date_col, 
    SUM(amount) as total_amount 
FROM iceberg_hms.db_name.iceberg_table 
GROUP BY date_col;
```

### 7.3 统计信息收集

```sql
-- 收集Iceberg表统计信息
ANALYZE TABLE iceberg_hms.db_name.iceberg_table;

-- 收集直方图统计（v3.3+）
ANALYZE TABLE iceberg_hms.db_name.iceberg_table 
WITH HISTOGRAM ON (col1, col2);
```

---

## 八、认证支持

### 8.1 Kerberos

```sql
CREATE EXTERNAL CATALOG iceberg_hms_kerberos
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "hive.metastore.uris" = "thrift://metastore:9083",
    "hive.metastore.authentication.type" = "kerberos",
    "hive.metastore.kerberos.principal" = "hive/_HOST@REALM"
);
```

### 8.2 AWS IAM

```sql
-- Instance Profile
CREATE EXTERNAL CATALOG iceberg_s3
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "aws.s3.use_instance_profile" = "true"
);

-- Assumed Role
CREATE EXTERNAL CATALOG iceberg_s3_role
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "aws.s3.use_instance_profile" = "true",
    "aws.s3.iam_role_arn" = "arn:aws:iam::123456789:role/role_name"
);
```

### 8.3 Azure认证

```sql
CREATE EXTERNAL CATALOG iceberg_azure
PROPERTIES (
    "type" = "iceberg",
    "iceberg.catalog.type" = "hive",
    "azure.adls1.use_managed_identity" = "true"
);
```

---

## 九、版本演进历史

| StarRocks版本 | Iceberg支持里程碑 |
|---------------|-------------------|
| **v2.4** | 初始支持Iceberg Catalog读取 |
| **v2.5** | 支持Data Cache、物化视图 |
| **v3.0** | 支持ORC position deletes |
| **v3.1** | 支持DDL、数据写入、Parquet deletes |
| **v3.2** | 支持统计信息收集、更完善写入 |
| **v3.3** | 支持REST Catalog视图、Hive视图、histogram |
| **v3.4** | 支持时间旅行、分支标签管理、元数据表 |
| **v3.4.1** | 元数据表查询完善 |
| **v4.0** | 支持Vended Credentials |
| **v4.1** | 支持Row Lineage (_row_id/_last_updated_sequence_number) |

---

## 十、代码仓关键路径

### StarRocks侧

| 路径 | 说明 |
|------|------|
| `fe/fe-core/src/main/java/com/starrocks/connector/iceberg/` | FE层Iceberg集成核心 |
| `be/src/connector/iceberg_connector.cpp` | BE层Connector |
| `java-extensions/iceberg-metadata-reader/` | JNI元数据扫描 |
| `java-extensions/hadoop-ext/` | AWS客户端工厂 |

### Iceberg侧

| 路径 | 说明 |
|------|------|
| `api/src/main/java/org/apache/iceberg/` | Iceberg核心接口 |
| `core/src/main/java/org/apache/iceberg/` | Iceberg核心实现 |
| `site/docs/vendors.md` | StarRocks集成文档 |

---

## 十一、最佳实践

1. **生产环境推荐**: Hive Metastore Catalog + S3存储 + Data Cache
2. **云原生推荐**: REST Catalog (Polaris/Tabular) + S3/GCS/Azure
3. **写入场景**: 确保Iceberg表为Parquet格式
4. **性能优化**: 启用Data Cache + 物化视图 + 统计信息收集
5. **数据新鲜度**: 关闭元数据缓存或设置较短TTL
6. **分支管理**: 使用分支进行开发测试，避免影响主分支生产数据