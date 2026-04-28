# Hadoop Java API 清单

> 基于Hadoop代码仓 + 官方文档生成

## 官方文档参考

| 文档类型 | URL |
|---------|-----|
| **官方文档** | https://hadoop.apache.org/docs/stable/ |
| **单节点集群** | https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html |
| **集群部署** | https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/ClusterSetup.html |
| **HDFS用户指南** | https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-hdfs/HdfsUserGuide.html |
| **MapReduce教程** | https://hadoop.apache.org/docs/stable/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html |
| **YARN** | https://hadoop.apache.org/docs/stable/hadoop-yarn/hadoop-yarn-site/YARN.html |

---

## API稳定性标注

Hadoop使用 `InterfaceAudience` 和 `InterfaceStability` 注解标注API稳定性：

| 标注 | 描述 |
|------|------|
| `@InterfaceAudience.Public` | 公共API，稳定且向后兼容 |
| `@InterfaceAudience.Limited` | 有限API，特定用途 |
| `@InterfaceAudience.Private` | 私有API，不保证兼容性 |
| `@InterfaceStability.Stable` | 稳定API，不会改变 |
| `@InterfaceStability.Evolving` | 进化API，可能会改变 |
| `@InterfaceStability.Unstable` | 不稳定API，无保证 |

---

## 1. Configuration API

**包路径:** `org.apache.hadoop.conf`

| 类名 | 类型 | 描述 |
|------|------|------|
| `Configuration` | 类 | Hadoop配置核心类 |
| `Configured` | 类 | 可配置基类 |
| `Configurable` | 接口 | 可配置接口 |
| `Reconfigurable` | 接口 | 可重配置接口 |
| `ReconfigurableBase` | 类 | 可重配置基类 |

---

## 2. FileSystem API

**包路径:** `org.apache.hadoop.fs`

### 核心类

| 类名 | 描述 |
|------|------|
| `FileSystem` | 文件系统抽象基类 |
| `Path` | 路径类 |
| `FSDataInputStream` | 文件系统输入流 |
| `FSDataOutputStream` | 文件系统输出流 |
| `FileStatus` | 文件状态 |
| `FileContext` | 文件上下文 |
| `AbstractFileSystem` | 抽象文件系统 |
| `BlockLocation` | 块位置 |
| `ContentSummary` | 内容摘要 |
| `FileChecksum` | 文件校验和 |
| `LocatedFileStatus` | 带位置的文件状态 |

### 本地文件系统

| 类名 | 描述 |
|------|------|
| `LocalFileSystem` | 本地文件系统 |
| `ChecksumFileSystem` | 校验文件系统 |
| `RawLocalFileSystem` | Raw本地文件系统 |

### 文件系统操作

| 方法 | 描述 |
|------|------|
| `open(Path)` | 打开文件读取 |
| `create(Path)` | 创建文件写入 |
| `append(Path)` | 追加文件 |
| `delete(Path, boolean)` | 删除文件/目录 |
| `rename(Path, Path)` | 重命名文件 |
| `listStatus(Path)` | 列出文件状态 |
| `mkdirs(Path)` | 创建目录 |
| `getFileStatus(Path)` | 获取文件状态 |
| `exists(Path)` | 检查文件是否存在 |
| `getBlockSize(Path)` | 获取块大小 |
| `getFileBlockLocations(Path, long, long)` | 获取块位置 |

---

## 3. Permission API

**包路径:** `org.apache.hadoop.fs.permission`

| 类名 | 描述 |
|------|------|
| `FsPermission` | 文件系统权限 |
| `FsAction` | 权限动作枚举 |
| `FsCreateModes` | 创建模式 |
| `AclEntry` | ACL条目 |
| `AclEntryScope` | ACL范围 |
| `AclEntryType` | ACL类型 |
| `AclStatus` | ACL状态 |

---

## 4. IO API

**包路径:** `org.apache.hadoop.io`

### Writable接口

| 接口名 | 描述 |
|--------|------|
| `Writable` | 可序列化接口 |
| `WritableComparable<T>` | 可比较Writable接口 |
| `WritableComparator` | Writable比较器 |
| `WritableFactories` | Writable工厂 |

### 基本类型Writable

| 类名 | 描述 |
|------|------|
| `Text` | Text类型 (UTF8 String) |
| `IntWritable` | Int类型 |
| `LongWritable` | Long类型 |
| `FloatWritable` | Float类型 |
| `DoubleWritable` | Double类型 |
| `BooleanWritable` | Boolean类型 |
| `ByteWritable` | Byte类型 |
| `ShortWritable` | Short类型 |
| `BytesWritable` | Bytes类型 |
| `NullWritable` | Null类型 |
| `VIntWritable` | Variable Int类型 |
| `VLongWritable` | Variable Long类型 |

### 数组类型Writable

| 类名 | 描述 |
|------|------|
| `ArrayWritable` | 数组Writable |
| `ArrayPrimitiveWritable` | 基本类型数组Writable |
| `TwoDArrayWritable` | 二维数组Writable |

### Map类型Writable

| 类名 | 描述 |
|------|------|
| `MapWritable` | Map Writable |
| `SortedMapWritable` | SortedMap Writable |

### 文件类型

| 类名 | 描述 |
|------|------|
| `SequenceFile` | SequenceFile文件格式 |
| `SequenceFile.Reader` | SequenceFile读取器 |
| `SequenceFile.Writer` | SequenceFile写入器 |
| `SequenceFile.Sorter` | SequenceFile排序器 |
| `MapFile` | MapFile文件格式 |
| `MapFile.Reader` | MapFile读取器 |
| `MapFile.Writer` | MapFile写入器 |
| `ArrayFile` | ArrayFile文件格式 |
| `SetFile` | SetFile文件格式 |
| `BloomMapFile` | BloomMapFile文件格式 |

### IO工具

| 类名 | 描述 |
|------|------|
| `IOUtils` | IO工具类 |
| `DataInputBuffer` | 数据输入缓冲 |
| `DataOutputBuffer` | 数据输出缓冲 |
| `NullOutput` | 空输出 |

### 压缩

| 类名 | 描述 |
|------|------|
| `CompressionCodec` | 压缩编码接口 |
| `CompressionCodecFactory` | 压缩编码工厂 |
| `Compressor` | 压缩器接口 |
| `Decompressor` | 解压器接口 |
| `CompressorStream` | 压缩流 |
| `DecompressorStream` | 解压流 |

---

## 5. IPC API

**包路径:** `org.apache.hadoop.ipc`

| 类名 | 描述 |
|------|------|
| `RPC` | RPC工具类 |
| `Server` | RPC服务端 |
| `Client` | RPC客户端 |
| `VersionedProtocol` | 版本化协议接口 |
| `ProtocolProxy<T>` | 协议代理 |
| `RemoteException` | 远程异常 |
| `RpcEngine` | RPC引擎接口 |
| `ProtobufRpcEngine` | Protobuf RPC引擎 |
| `WritableRpcEngine` | Writable RPC引擎 |

---

## 6. Security API

**包路径:** `org.apache.hadoop.security`

### 核心类

| 类名 | 描述 |
|------|------|
| `UserGroupInformation` | 用户组信息 |
| `Credentials` | 凭证类 |
| `AccessControlException` | 访问控制异常 |
| `Groups` | 组映射 |
| `SecurityUtil` | 安全工具 |
| `User` | 用户信息 |

### 认证方式

| 方式 | 描述 |
|------|------|
| `SIMPLE` | 简单认证 |
| `KERBEROS` | Kerberos认证 |
| `TOKEN` | Token认证 |

---

## 7. Token API

**包路径:** `org.apache.hadoop.security.token`

| 类名 | 描述 |
|------|------|
| `Token<T extends TokenIdentifier>` | Token类 |
| `TokenIdentifier` | Token标识符接口 |
| `DelegationTokenIdentifier` | 委托Token标识符 |
| `TokenSelector<T extends TokenIdentifier>` | Token选择器 |
| `TokenRenewer` | Token续期器 |
| `SecretManager<T extends TokenIdentifier>` | 密钥管理器 |

---

## 8. Crypto API

**包路径:** `org.apache.hadoop.crypto`

| 类名 | 描述 |
|------|------|
| `CryptoCodec` | 加密编解码器 |
| `CryptoInputStream` | 加密输入流 |
| `CryptoOutputStream` | 加密输出流 |
| `KeyProvider` | 密钥提供者接口 |
| `KeyProviderFactory` | 密钥提供者工厂 |

---

## 9. Util API

**包路径:** `org.apache.hadoop.util`

| 类名 | 描述 |
|------|------|
| `Tool` | Tool接口 |
| `ToolRunner` | Tool运行器 |
| `Progressable` | 进度报告接口 |
| `StringUtils` | 字符串工具 |
| `Shell` | Shell工具 |
| `ReflectionUtils` | 反射工具 |
| `NativeCodeLoader` | Native代码加载器 |
| `Daemon` | 守护线程 |
| `ExitUtil` | 退出工具 |
| `Options` | 选项解析 |

---

## 10. Net API

**包路径:** `org.apache.hadoop.net`

| 类名 | 描述 |
|------|------|
| `NetUtils` | 网络工具 |
| `NetworkTopology` | 网络拓扑 |
| `Node` | 节点接口 |
| `DNSToSwitchMapping` | DNS到交换机映射 |
| `CachedDNSToSwitchMapping` | 缓存DNS映射 |
| `ScriptBasedMapping` | 脚本基础映射 |

---

## 11. HDFS Client API

**包路径:** `org.apache.hadoop.hdfs`

### 核心类

| 类名 | 描述 |
|------|------|
| `DistributedFileSystem` | 分布式文件系统 |
| `DFSClient` | DFS客户端 |
| `DFSInputStream` | DFS输入流 |
| `DFSOutputStream` | DFS输出流 |
| `HdfsConfiguration` | HDFS配置 |

---

## 12. HDFS Protocol API

**包路径:** `org.apache.hadoop.hdfs.protocol`

### 文件状态

| 类名 | 描述 |
|------|------|
| `HdfsFileStatus` | HDFS文件状态 |
| `HdfsLocatedFileStatus` | HDFS带位置文件状态 |
| `LocatedBlocks` | 块位置列表 |
| `LocatedBlock` | 块位置 |
| `DatanodeInfo` | DataNode信息 |
| `DatanodeID` | DataNode ID |
| `DatanodeInfoWithStorage` | 带存储的DataNode信息 |

### 块相关

| 类名 | 描述 |
|------|------|
| `ExtendedBlock` | 扩展块 |
| `Block` | 块类 |
| `BlockLocalPathInfo` | 本地块路径 |
| `CorruptFileBlocks` | 损坏块 |

### 编码策略

| 类名 | 描述 |
|------|------|
| `ErasureCodingPolicy` | 纠删码策略 |
| `ErasureCodingPolicyInfo` | 纠删码策略信息 |
| `ECBlockGroup` | EC块组 |

### 存储策略

| 类名 | 描述 |
|------|------|
| `BlockStoragePolicy` | 块存储策略 |
| `StorageType` | 存储类型枚举 |

### 缓存相关

| 类名 | 描述 |
|------|------|
| `CacheDirectiveInfo` | 缓存指令信息 |
| `CachePoolInfo` | 缓存池信息 |

---

## 13. HDFS Security Token API

**包路径:** `org.apache.hadoop.hdfs.security.token`

| 类名 | 描述 |
|------|------|
| `DelegationTokenIdentifier` | HDFS委托Token |
| `BlockTokenIdentifier` | 块Token标识符 |
| `BlockTokenSecretManager` | 块Token密钥管理器 |

---

## 14. MapReduce API

**包路径:** `org.apache.hadoop.mapreduce`

### 核心类

| 类名 | 描述 |
|------|------|
| `Job` | Job类 |
| `Mapper<KEYIN,VALUEIN,KEYOUT,VALUEOUT>` | Mapper基类 |
| `Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>` | Reducer基类 |
| `InputFormat<K,V>` | InputFormat接口 |
| `OutputFormat<K,V>` | OutputFormat接口 |
| `InputSplit` | 输入分片接口 |
| `RecordReader<K,V>` | RecordReader接口 |
| `RecordWriter<K,V>` | RecordWriter接口 |
| `Partitioner<K,V>` | Partitioner接口 |
| `OutputCommitter` | OutputCommitter接口 |
| `JobContext` | Job上下文接口 |
| `TaskAttemptContext` | Task上下文接口 |
| `TaskInputOutputContext` | Task IO上下文 |

### Job管理

| 类名 | 描述 |
|------|------|
| `Cluster` | 集群类 |
| `JobStatus` | Job状态 |
| `JobID` | Job ID |
| `TaskID` | Task ID |
| `TaskAttemptID` | Task尝试ID |
| `Counters` | 计数器 |
| `Counter` | 计数器接口 |
| `CounterGroup` | 计数器组 |

---

## 15. MapReduce Input API

**包路径:** `org.apache.hadoop.mapreduce.lib.input`

| 类名 | 描述 |
|------|------|
| `FileInputFormat<K,V>` | 文件InputFormat基类 |
| `TextInputFormat` | Text InputFormat |
| `SequenceFileInputFormat<K,V>` | SequenceFile InputFormat |
| `KeyValueTextInputFormat` | KeyValue Text InputFormat |
| `NLineInputFormat` | N行 InputFormat |
| `CombineFileInputFormat<K,V>` | 组合文件 InputFormat |
| `CombineFileSplit` | 组合文件分片 |
| `MultipleInputs` | 多输入工具 |
| `DelegatingInputFormat` | 委派 InputFormat |
| `LineRecordReader` | 行 RecordReader |

---

## 16. MapReduce Output API

**包路径:** `org.apache.hadoop.mapreduce.lib.output`

| 类名 | 描述 |
|------|------|
| `FileOutputFormat<K,V>` | 文件OutputFormat基类 |
| `TextOutputFormat<K,V>` | Text OutputFormat |
| `SequenceFileOutputFormat<K,V>` | SequenceFile OutputFormat |
| `MapFileOutputFormat` | MapFile OutputFormat |
| `FileOutputCommitter` | 文件OutputCommitter |
| `LazyOutputFormat<K,V>` | 惰性 OutputFormat |
| `NullOutputFormat<K,V>` | Null OutputFormat |
| `FilterOutputFormat<K,V>` | 过滤 OutputFormat |

---

## 17. MapReduce Partition API

**包路径:** `org.apache.hadoop.mapreduce.lib.partition`

| 类名 | 描述 |
|------|------|
| `HashPartitioner<K,V>` | Hash Partitioner |
| `TotalOrderPartitioner<K,V>` | 全序 Partitioner |
| `KeyFieldBasedPartitioner<K,V>` | Key字段 Partitioner |
| `BinaryPartitioner` | 二进制 Partitioner |
| `InputSampler<K,V>` | 输入采样器 |

---

## 18. MapReduce Join API

**包路径:** `org.apache.hadoop.mapreduce.lib.join`

| 类名 | 描述 |
|------|------|
| `CompositeInputFormat<K,V>` | 组合 InputFormat |
| `CompositeRecordReader<K,V,X>` | 组合 RecordReader |
| `JoinRecordReader<K,V>` | Join RecordReader |
| `OuterJoinRecordReader<K,V>` | 外连接 RecordReader |
| `InnerJoinRecordReader<K,V>` | 内连接 RecordReader |
| `MultiFilterRecordReader<K,V>` | 多过滤 RecordReader |
| `TupleWritable` | Tuple Writable |

---

## 19. MapReduce Chain API

**包路径:** `org.apache.hadoop.mapreduce.lib.chain`

| 类名 | 描述 |
|------|------|
| `ChainMapper` | Chain Mapper |
| `ChainReducer` | Chain Reducer |

---

## 20. YARN API

**包路径:** `org.apache.hadoop.yarn.api`

### 协议接口

| 接口名 | 描述 |
|--------|------|
| `ApplicationClientProtocol` | Application客户端协议 |
| `ApplicationMasterProtocol` | AM协议 |
| `ContainerManagementProtocol` | 容器管理协议 |

### 常量

| 类名 | 描述 |
|------|------|
| `ApplicationConstants` | Application常量 |

---

## 21. YARN Records API

**包路径:** `org.apache.hadoop.yarn.api.records`

### ID类

| 类名 | 描述 |
|------|------|
| `ApplicationId` | Application ID |
| `ApplicationAttemptId` | Application尝试ID |
| `ContainerId` | Container ID |
| `NodeId` | Node ID |
| `ReservationId` | Reservation ID |

### Application相关

| 类名 | 描述 |
|------|------|
| `ApplicationReport` | Application报告 |
| `ApplicationSubmissionContext` | Application提交上下文 |
| `ApplicationResourceUsageReport` | 资源使用报告 |
| `ApplicationAttemptReport` | Application尝试报告 |

### Container相关

| 类名 | 描述 |
|------|------|
| `Container` | Container接口 |
| `ContainerLaunchContext` | Container启动上下文 |
| `ContainerStatus` | Container状态 |
| `ContainerReport` | Container报告 |

### Resource相关

| 类名 | 描述 |
|------|------|
| `Resource` | Resource类 |
| `ResourceInformation` | Resource信息 |
| `ResourceRequest` | Resource请求 |
| `ResourceOption` | Resource选项 |

### Node相关

| 类名 | 描述 |
|------|------|
| `NodeReport` | Node报告 |
| `NodeState` | Node状态枚举 |

### Queue相关

| 类名 | 描述 |
|------|------|
| `QueueInfo` | Queue信息 |
| `QueueUserACLInfo` | Queue用户ACL信息 |
| `QueueACL` | Queue ACL枚举 |

### Priority

| 类名 | 描述 |
|------|------|
| `Priority` | Priority类 |

### LocalResource

| 类名 | 描述 |
|------|------|
| `LocalResource` | LocalResource类 |
| `LocalResourceType` | LocalResource类型枚举 |
| `LocalResourceVisibility` | LocalResource可见性枚举 |

### Token

| 类名 | 描述 |
|------|------|
| `Token` | YARN Token类 |
| `NMToken` | NMToken类 |

---

## 22. YARN Client API

**包路径:** `org.apache.hadoop.yarn.client.api`

| 类名 | 描述 |
|------|------|
| `YarnClient` | YARN客户端 |
| `AMRMClient<T extends ContainerRequest>` | AM-RM客户端 |
| `NMClient` | NM客户端 |
| `AHSClient` | AHS客户端 |
| `SharedCacheClient` | 共享缓存客户端 |
| `YarnClientApplication` | YARN客户端Application |

---

## 23. YARN Async API

**包路径:** `org.apache.hadoop.yarn.client.api.async`

| 类名 | 描述 |
|------|------|
| `AMRMClientAsync<T extends ContainerRequest>` | 异步AM-RM客户端 |
| `NMClientAsync` | 异步NM客户端 |

---

## 24. YARN Configuration

**包路径:** `org.apache.hadoop.yarn.conf`

| 类名 | 描述 |
|------|------|
| `YarnConfiguration` | YARN配置类 |
| `HAUtil` | HA工具类 |

---

## 25. Client Modules

**包路径:** `org.apache.hadoop`

### 客户端聚合模块

| 模块 | 描述 |
|------|------|
| `hadoop-client` | 客户端聚合POM (暴露所有依赖) |
| `hadoop-client-api` | Shaded客户端API jar |
| `hadoop-client-runtime` | Shaded客户端运行时jar |
| `hadoop-client-minicluster` | MiniCluster测试支持 |

---

## 26. Annotations API

**包路径:** `org.apache.hadoop.classification`

| 类名 | 描述 |
|------|------|
| `InterfaceAudience` | API受众标注 |
| `InterfaceStability` | API稳定性标注 |
| `VisibleForTesting` | 测试可见标注 |

### InterfaceAudience

| 标注 | 描述 |
|------|------|
| `Public` | 公共API |
| `Limited` | 有限API |
| `Private` | 私有API |

### InterfaceStability

| 标注 | 描述 |
|------|------|
| `Stable` | 稳定API |
| `Evolving` | 进化API |
| `Unstable` | 不稳定API |

---

## 模块依赖关系

```
hadoop-common (公共API)
    ├── Configuration
    ├── FileSystem
    ├── IO (Writable, SequenceFile等)
    ├── Security (UGI, Token)
    └── Util (Tool, StringUtils等)
    ↓
┌───────────────────────────────────────┐
│  核心子系统                            │
│  ├── hadoop-hdfs (HDFS客户端)          │
│  ├── hadoop-mapreduce (MapReduce)      │
│  └── hadoop-yarn (YARN)                │
└───────────────────────────────────────┘
    ↓
hadoop-client (客户端聚合)
    ├── hadoop-client-api (Shaded API)
    ├── hadoop-client-runtime (Shaded Runtime)
    └── hadoop-client-minicluster (测试)
```

---

## 使用示例

### 文件系统操作

```java
Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(conf);

// 创建文件
FSDataOutputStream out = fs.create(new Path("/tmp/file.txt"));
out.writeUTF("Hello Hadoop");
out.close();

// 读取文件
FSDataInputStream in = fs.open(new Path("/tmp/file.txt"));
String content = in.readUTF();
in.close();

// 列出文件
FileStatus[] statuses = fs.listStatus(new Path("/tmp"));
for (FileStatus status : statuses) {
    System.out.println(status.getPath());
}
```

### MapReduce Job

```java
Configuration conf = new Configuration();
Job job = Job.getInstance(conf, "WordCount");

job.setMapperClass(WordCountMapper.class);
job.setReducerClass(WordCountReducer.class);

job.setOutputKeyClass(Text.class);
job.setOutputValueClass(IntWritable.class);

FileInputFormat.addInputPath(job, new Path("/input"));
FileOutputFormat.setOutputPath(job, new Path("/output"));

job.waitForCompletion(true);
```

### Mapper

```java
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        for (String w : words) {
            word.set(w);
            context.write(word, one);
        }
    }
}
```

### Reducer

```java
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) 
            throws IOException, InterruptedException {
        int sum = 0;
        for (IntWritable val : values) {
            sum += val.get();
        }
        context.write(key, new IntWritable(sum));
    }
}
```

### YARN Application

```java
YarnClient yarnClient = YarnClient.createYarnClient();
yarnClient.init(conf);
yarnClient.start();

// 创建Application
YarnClientApplication app = yarnClient.createApplication();
ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
appContext.setApplicationName("MyApp");

// 提交Application
ApplicationId appId = yarnClient.submitApplication(appContext);
System.out.println("Submitted application: " + appId);
```

### AM-RM Client

```java
AMRMClientAsync<ContainerRequest> amrmClient = AMRMClientAsync.createAMRMClientAsync(1000, new AMRMCallbackHandler());
amrmClient.init(conf);
amrmClient.start();

// 注册AM
amrmClient.registerApplicationMaster("localhost", 0, "");

// 请求Container
Resource capability = Resource.newInstance(1024, 1);
ContainerRequest request = new ContainerRequest(capability, null, null, Priority.newInstance(0));
amrmClient.addContainerRequest(request);
```

---

## 参考链接

- Hadoop官方文档: https://hadoop.apache.org/docs/stable
- Java API文档: https://hadoop.apache.org/docs/stable/api