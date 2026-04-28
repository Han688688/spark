# Apache大数据组件Java API对比汇总

> **代码仓实际API vs 官方文档描述** - 直接对比差异点

---

## 一、稳定性争议差异（官方当公共API，代码仓说受限）

| 项目 | API类名 | 官方文档描述 | 代码仓实际标注 | 差异影响 |
|------|---------|--------------|---------------|----------|
| **Hadoop** | DistributedFileSystem | 当作主要HDFS API广泛介绍 | @LimitedPrivate(MapReduce,HBase) + @Unstable | ❌ 普通用户不应依赖，仅对MapReduce/HBase项目有稳定性保证 |
| **Hadoop** | Job类 | 当作MapReduce核心API介绍 | @Public + @Evolving（不是Stable） | ⚠️ API可能演进变化，不保证向后兼容 |
| **Hadoop** | HDFS客户端API(34个类) | 当作稳定客户端API | 81%是@Evolving，仅7%是@Stable | ⚠️ 大部分HDFS API不保证稳定 |
| **Hadoop** | YARN protocolrecords包 | 当作公共API介绍 | @Public + @Unstable | ⚠️ 协议结构可能变化 |

---

## 二、内部实现类暴露差异（代码仓存在但不应公开使用）

| 项目 | 包名 | 类数量 | 代码仓状态 | 官方Javadoc | 差异说明 |
|------|------|--------|-----------|------------|----------|
| **Kafka** | clients.*.internals | 221个 | 存在，无@Public标注 | ❌ 未列出 | ✅ 正确：内部实现不应公开 |
| **Kafka** | streams.kstream.internals | 160个 | 存在，无@Public标注 | ❌ 未列出 | ✅ 正确：DSL内部实现 |
| **Iceberg** | core模块实现类 | ~60个 | 存在，如BaseTable/BaseTransaction | ✓ 已列出 | ⚠️ 用户应使用api模块接口，不应直接用core实现类 |
| **HBase** | client.*Impl类 | ~30个 | 存在，@Private标注 | ❌ 未列出或标注Internal | ✅ 正确：内部实现 |
| **HBase** | shaded.protobuf | ProtobufUtil等 | 存在，@Private但Hive等外部使用 | ❌ 未列出 | ⚠️ 实际被外部依赖，但标注Private |
| **Hadoop** | hdfs.server包 | 全部 | @Private标注 | ❌ 未列出 | ✅ 正确：NameNode/DataNode内部 |

---

## 三、新增API差异（代码仓有新特性，官方文档可能滞后）

| 项目 | API类名 | 代码仓状态 | 官方文档状态 | 差异说明 |
|------|---------|-----------|--------------|----------|
| **Spark** | TimeMode | @Evolving，sql/api模块存在 | ❌ Spark 3.5.6文档未明确列出 | 新增：transformWithState时间模式 |
| **Spark** | Geometry/Geography | @Unstable，sql/api模块存在 | ❌ Spark 3.5.6文档未列出 | 新增：空间数据类型(Spark 4.1特性) |
| **Spark** | Identifier/IdentityColumnSpec | @Evolving，connector.catalog包 | ❌ Spark 3.5.6文档未列出 | 新增：Catalog增强API |
| **Kafka** | StreamsGroupDescription等10个类 | @Evolving，admin包存在 | ✓ 已列出 | 新增：Streams Group管理(KIP-919)，标注Evolving表示可能变化 |
| **Kafka** | ShareConsumer/ShareConsumer | @Public存在 | ✓ 已列出 | 新增：Share Groups(KIP-932) |
| **Kafka** | AddRaftVoterOptions等5个类 | @Stable，admin包存在 | ✓ 已列出 | 新增：Raft Voter管理(KIP-853)，已稳定 |
| **Iceberg** | View系列(10个接口) | @Public，view包存在 | ✓ 已列出 | 新增：View功能 |
| **Iceberg** | Variant系列(7个接口) | @Public，variants包存在 | ✓ 已列出 | 新增：Variant类型支持 |
| **Iceberg** | PartitionStatistics系列 | @Public，api包存在 | ✓ 已列出 | 新增：分区统计功能 |
| **Iceberg** | geospatial包 | @Public，新增包 | ⚠️ 可能未及时更新 | 新增：地理空间支持 |
| **HBase** | ConnectionRegistry系列(4个类) | @Public存在 | ⚠️ HBase 4.0.0-alpha文档可能未完整 | 新增：替代ZK连接注册 |
| **HBase** | TestingHBaseCluster | @Public存在 | ⚠️ 新测试框架 | 新增：替代HBaseTestingUtility |
| **HBase** | CheckAndMutate类 | @Public存在 | ✓ 已列出 | 新增：替代Table.checkAndMutate方法 |
| **HBase** | QueryMetrics | @Public存在 | ❌ 未列出 | 新增：查询指标 |

---

## 四、Deprecated API差异（官方说废弃，代码仓仍保留）

| 项目 | API类/方法 | 官方文档说明 | 代码仓状态 | 替代方案 | 差异说明 |
|------|------------|--------------|-----------|----------|----------|
| **Spark** | Trigger.Once() | Spark 3.4.0废弃 | ⚠️ 仍存在 | Trigger.AvailableNow() | 应迁移，但代码仓未移除 |
| **Spark** | typed聚合函数类 | Spark 3.0.0废弃 | ⚠️ 仍存在 | 非类型化内置聚合函数 | 整个类废弃但未移除 |
| **HBase** | HBaseTestingUtility | HBase 3.0.0废弃 | ⚠️ 仍存在 | TestingHBaseCluster | 应迁移到新测试框架 |
| **HBase** | MiniHBaseCluster | HBase 3.0.0废弃 | ⚠️ 仍存在 | TestingHBaseCluster | 应迁移 |
| **HBase** | Table.checkAndMutate方法 | HBase 4.0.0移除 | ⚠️ 仍存在 | CheckAndMutate类 | 应使用新接口 |
| **HBase** | CoprocessorRpcChannel | HBase 4.0.0移除 | ⚠️ 仍存在 | 不再支持低级别RPC | 即将移除 |
| **HBase** | MasterRegistry | HBase 2.5.0废弃 | ⚠️ 仍存在 | RpcConnectionRegistry | ZK替代方案 |
| **HBase** | QuotaRetriever | HBase 3.0.0废弃 | ⚠️ 仍存在 | Admin API | 应使用Admin获取配额 |
| **Hadoop** | CommonConfigurationKeys | 文档未明确废弃 | ⚠️ @Private标注 | CommonConfigurationKeysPublic | 应使用Public版本 |
| **Kafka** | 旧消费者API(KafkaConsumer旧构造) | 文档有说明 | 部分废弃 | 新配置方式 | 渐进迁移 |

---

## 五、官方Javadoc范围差异（收录范围不一致）

| 项目 | 官方Javadoc收录 | 代码仓实际范围 | 差异说明 |
|------|----------------|---------------|----------|
| **Iceberg** | 200+类(含core模块) | api模块139个接口 | ⚠️ Javadoc收录了实现类，用户应区分接口vs实现 |
| **HBase** | 400+类 | 343个@Public类 | ⚠️ Javadoc可能收录了一些Internal类 |
| **Hadoop** | ~800类 | 902个@Public类 | ✅ 基本一致 |

---

## 六、关键差异总结

### ❌ 稳定性争议（影响使用决策）

| 问题 | 影响 | 建议 |
|------|------|------|
| Hadoop DistributedFileSystem标注LimitedPrivate | 用户误用，稳定性无保证 | 使用FileSystem抽象，避免依赖DistributedFileSystem特定方法 |
| Hadoop Job类是Evolving | MapReduce核心API可能变化 | 关注版本迁移指南 |
| HDFS客户端81%是Evolving | 大部分HDFS API不稳定 | 仅依赖FileSystem核心方法(@Stable) |

### ⚠️ 新增API未及时更新文档

| 项目 | 新增API | 建议 |
|------|---------|------|
| Spark | TimeMode、Geometry/Geography | 关注Spark 4.x文档更新 |
| HBase | ConnectionRegistry系列 | 替代ZK连接，提前迁移 |
| Iceberg | View、Variant、geospatial | 查看最新Javadoc而非文档 |

### ⚠️ Deprecated未移除（应迁移）

| 项目 | 应迁移API | 紧迫程度 |
|------|----------|----------|
| HBase | HBaseTestingUtility→TestingHBaseCluster | HBase 4.0将移除 |
| HBase | Table.checkAndMutate→CheckAndMutate类 | HBase 4.0已移除方法 |
| HBase | CoprocessorRpcChannel | HBase 4.0移除，不支持 |
| Spark | Trigger.Once→AvailableNow | 已废弃，应迁移 |
| Hadoop | CommonConfigurationKeys→Public版本 | Private不应使用 |

### ✅ 正确设计（无需担心）

| 项目 | 说明 |
|------|------|
| Kafka internals包381类未公开 | 正确：内部实现隔离 |
| Iceberg api模块仅接口 | 正确：接口与实现分离 |
| Hadoop server包全部Private | 正确：服务端内部 |

---

## 七、各项目API数量对比

| 项目 | 代码仓@Public类 | 官方Javadoc公共类 | 稳定@Stable | 演进@Evolving | 内部@Private |
|------|----------------|-------------------|-------------|---------------|--------------|
| **Spark** | 69个 | ~65个 | 46个 | 5个 | internal包 |
| **Kafka** | 577个 | 577个 | ~100个 | ~15个 | 381个internals |
| **Iceberg** | 139个接口 | 200+含实现 | 大部分stable | View/Variants | core实现类 |
| **HBase** | 343个 | 400+ | 核心类stable | ConnectionRegistry | ~30个Impl |
| **Hadoop** | 902个 | ~800+ | MapReduce核心233 | HDFS 34个 | server全部 |

---

## 八、使用建议

1. **优先使用@Stable API**：Spark函数接口、Kafka核心客户端、Iceberg api模块、HBase核心操作类、Hadoop MapReduce核心
2. **避免@LimitedPrivate/@Private API**：DistributedFileSystem、CommonConfigurationKeys、各项目Impl实现类
3. **关注@Evolving API变化**：Kafka StreamsGroup、Spark TimeMode、Hadoop Job类
4. **迁移deprecated API**：HBase测试框架、Spark Trigger.Once、HBase checkAndMutate方法
5. **区分接口vs实现**：Iceberg api模块是接口，core模块是实现类不应直接使用