# HDFS Java API Complete List

> 参考官方文档: https://hadoop.apache.org/docs/stable/api/
> 版本: Apache Hadoop 3.3.6
> 生成时间: 2026-05-18

---

## 目录

1. [DistributedFileSystem](#distributedfilesystem)
2. [HdfsConfiguration](#hdfsconfiguration)
3. [HdfsDataOutputStream](#hdfsdataoutputstream)
4. [HdfsDataInputStream](#hdfsdatainputstream)
5. [DFSClient](#dfsclient)

---

## DistributedFileSystem

**包路径**: `DistributedFileSystem`

**稳定性标注**: `@InterfaceAudience.LimitedPrivate`

**说明**: 标记为LimitedPrivate,仅限于MapReduce和HBase使用,普通应用应使用FileSystem接口

**功能**: HDFS的具体实现类,提供HDFS特有的操作如快照、纠删码、缓存管理等

| 序号 | 方法名 | 返回类型 | 参数 | 描述 | 状态 |
|------|--------|----------|------|------|------|
| 1 | `getScheme` | `String` | `` | 获取文件系统scheme | Stable |
| 2 | `getUri` | `URI` | `` | 获取文件系统URI | Stable |
| 3 | `initialize` | `void` | `URI, Configuration` | 初始化文件系统 | Stable |
| 4 | `provide` | `StorageStatistics` | `` |  | Stable |
| 5 | `getWorkingDirectory` | `Path` | `` | 获取工作目录 | Stable |
| 6 | `getDefaultBlockSize` | `long` | `` |  | Stable |
| 7 | `getDefaultReplication` | `short` | `` |  | Stable |
| 8 | `setWorkingDirectory` | `void` | `Path` | 设置工作目录 | Stable |
| 9 | `getHomeDirectory` | `Path` | `` | 获取用户主目录 | Stable |
| 10 | `getHedgedReadMetrics` | `DFSHedgedReadMetrics` | `` | 获取 Hedged Read 指标 | Stable |
| 11 | `getFileBlockLocations` | `BlockLocation[]` | `FileStatus, long, long` | 获取文件块位置 | Stable |
| 12 | `getFileBlockLocations` | `BlockLocation[]` | `Path, long, long` | 获取文件块位置 | Stable |
| 13 | `setVerifyChecksum` | `void` | `boolean` |  | Stable |
| 14 | `recoverLease` | `boolean` | `Path` | 恢复文件租约 | Stable |
| 15 | `open` | `FSDataInputStream` | `Path, int` | 打开文件读取 | Stable |
| 16 | `open` | `FSDataInputStream` | `PathHandle, int` | 打开文件读取 | Stable |
| 17 | `getErasureCodingPolicyName` | `String` | `FileStatus` |  | Stable |
| 18 | `append` | `FSDataOutputStream` | `Path, int, Progressable` | 追加文件 | Stable |
| 19 | `append` | `FSDataOutputStream` | `Path, int, Progressable, boolean` | 追加文件 | Stable |
| 20 | `append` | `FSDataOutputStream` | `Path, EnumSet<>, int, Progressable` | 追加文件 | Stable |
| 21 | `append` | `FSDataOutputStream` | `Path, EnumSet<>, int, Progressable, InetSocketAddress[]` | 追加文件 | Stable |
| 22 | `create` | `FSDataOutputStream` | `Path, FsPermission, boolean, int, short, long, Progressable` | 创建文件 | Stable |
| 23 | `create` | `HdfsDataOutputStream` | `Path, FsPermission, boolean, int, short, long, Progressable, InetSocketAddress[]` | 创建文件 | Stable |
| 24 | `create` | `FSDataOutputStream` | `Path, FsPermission, EnumSet<>, int, short, long, Progressable, ChecksumOpt` | 创建文件 | Stable |
| 25 | `createNonRecursive` | `FSDataOutputStream` | `Path, FsPermission, EnumSet<>, int, short, long, Progressable` |  | Stable |
| 26 | `setReplication` | `boolean` | `Path, short` | 设置文件副本数 | Stable |
| 27 | `setStoragePolicy` | `void` | `Path, String` | 设置存储策略 | Stable |
| 28 | `unsetStoragePolicy` | `void` | `Path` | 取消存储策略 | Stable |
| 29 | `getStoragePolicy` | `BlockStoragePolicySpi` | `Path` | 获取存储策略 | Stable |
| 30 | `getAllStoragePolicies` | `Collection<BlockStoragePolicy>` | `` | 获取所有存储策略 | Stable |
| 31 | `getBytesWithFutureGenerationStamps` | `long` | `` |  | Stable |
| 32 | `getStoragePolicies` | `BlockStoragePolicy[]` | `` | 获取所有存储策略 | Deprecated |
| 33 | `concat` | `void` | `Path, Path` | 连接文件 | Stable |
| 34 | `rename` | `boolean` | `Path, Path` | 重命名文件 | Stable |
| 35 | `rename` | `void` | `Path, Path, Options.Rename...` | 重命名文件 | Stable |
| 36 | `truncate` | `boolean` | `Path, long` | 截断文件 | Stable |
| 37 | `delete` | `boolean` | `Path, boolean` | 删除文件或目录 | Stable |
| 38 | `getContentSummary` | `ContentSummary` | `Path` | 获取内容摘要 | Stable |
| 39 | `getQuotaUsage` | `QuotaUsage` | `Path` | 获取配额使用情况 | Stable |
| 40 | `setQuota` | `void` | `Path, long, long` | 设置配额 | Stable |
| 41 | `setQuotaByStorageType` | `void` | `Path, StorageType, long` |  | Stable |
| 42 | `listStatus` | `FileStatus[]` | `Path` | 列出文件状态 | Stable |
| 43 | `listStatusIterator` | `RemoteIterator<FileStatus>` | `Path` |  | Stable |
| 44 | `hasNext` | `boolean` | `` |  | Stable |
| 45 | `batchedListStatusIterator` | `RemoteIterator<PartialListing<FileStatus>>` | `List<>` |  | Stable |
| 46 | `batchedListLocatedStatusIterator` | `RemoteIterator<PartialListing<LocatedFileStatus>>` | `List<>` |  | Stable |
| 47 | `hasNext` | `boolean` | `` |  | Stable |
| 48 | `mkdir` | `boolean` | `Path, FsPermission` |  | Stable |
| 49 | `mkdirs` | `boolean` | `Path, FsPermission` | 创建目录 | Stable |
| 50 | `close` | `void` | `` |  | Stable |
| 51 | `getClient` | `DFSClient` | `` |  | Stable |
| 52 | `getStatus` | `FsStatus` | `Path` |  | Stable |
| 53 | `getMissingBlocksCount` | `long` | `` |  | Stable |
| 54 | `getPendingDeletionBlocksCount` | `long` | `` |  | Stable |
| 55 | `getMissingReplOneBlocksCount` | `long` | `` |  | Stable |
| 56 | `getLowRedundancyBlocksCount` | `long` | `` |  | Stable |
| 57 | `getCorruptBlocksCount` | `long` | `` |  | Stable |
| 58 | `listCorruptFileBlocks` | `RemoteIterator<Path>` | `Path` | 列出损坏文件块 | Stable |
| 59 | `getDataNodeStats` | `DatanodeInfo[]` | `` |  | Stable |
| 60 | `getDataNodeStats` | `DatanodeInfo[]` | `DatanodeReportType` |  | Stable |
| 61 | `setSafeMode` | `boolean` | `SafeModeAction` | 设置安全模式 | Stable |
| 62 | `setSafeMode` | `boolean` | `SafeModeAction, boolean` | 设置安全模式 | Stable |
| 63 | `setSafeMode` | `boolean` | `HdfsConstants.SafeModeAction` | 设置安全模式 | Stable |
| 64 | `setSafeMode` | `boolean` | `HdfsConstants.SafeModeAction, boolean` | 设置安全模式 | Stable |
| 65 | `saveNamespace` | `boolean` | `long, long` | 保存命名空间 | Stable |
| 66 | `saveNamespace` | `void` | `` | 保存命名空间 | Stable |
| 67 | `rollEdits` | `long` | `` | 滚动编辑日志 | Stable |
| 68 | `restoreFailedStorage` | `boolean` | `String` | 恢复失败存储 | Stable |
| 69 | `refreshNodes` | `void` | `` | 刷新节点列表 | Stable |
| 70 | `finalizeUpgrade` | `void` | `` | 完成升级 | Stable |
| 71 | `upgradeStatus` | `boolean` | `` |  | Stable |
| 72 | `rollingUpgrade` | `RollingUpgradeInfo` | `RollingUpgradeAction` | 滚动升级操作 | Stable |
| 73 | `metaSave` | `void` | `String` | 保存元数据信息 | Stable |
| 74 | `getServerDefaults` | `FsServerDefaults` | `` |  | Stable |
| 75 | `getFileStatus` | `FileStatus` | `Path` | 获取文件状态 | Stable |
| 76 | `msync` | `void` | `` |  | Stable |
| 77 | `createSymlink` | `void` | `Path, Path, boolean` |  | Stable |
| 78 | `supportsSymlinks` | `boolean` | `` |  | Stable |
| 79 | `getFileLinkStatus` | `FileStatus` | `Path` |  | Stable |
| 80 | `getLinkTarget` | `Path` | `Path` |  | Stable |
| 81 | `getFileChecksum` | `FileChecksum` | `Path` |  | Stable |
| 82 | `getFileChecksum` | `FileChecksum` | `Path, long` |  | Stable |
| 83 | `setPermission` | `void` | `Path, FsPermission` | 设置文件权限 | Stable |
| 84 | `setOwner` | `void` | `Path, String, String` | 设置文件所有者 | Stable |
| 85 | `setTimes` | `void` | `Path, long, long` | 设置文件时间 | Stable |
| 86 | `getDelegationToken` | `Token<DelegationTokenIdentifier>` | `String` | 获取委托令牌 | Stable |
| 87 | `setBalancerBandwidth` | `void` | `long` |  | Stable |
| 88 | `getCanonicalServiceName` | `String` | `` |  | Stable |
| 89 | `isInSafeMode` | `boolean` | `` |  | Stable |
| 90 | `isSnapshotTrashRootEnabled` | `boolean` | `` |  | Stable |
| 91 | `allowSnapshot` | `void` | `Path` | 允许快照 | Stable |
| 92 | `disallowSnapshot` | `void` | `Path` | 禁用快照 | Stable |
| 93 | `createSnapshot` | `Path` | `Path, String` | 创建快照 | Stable |
| 94 | `renameSnapshot` | `void` | `Path, String, String` | 重命名快照 | Stable |
| 95 | `getSnapshottableDirListing` | `SnapshottableDirectoryStatus[]` | `` |  | Stable |
| 96 | `getSnapshotListing` | `SnapshotStatus[]` | `Path` |  | Stable |
| 97 | `deleteSnapshot` | `void` | `Path, String` | 删除快照 | Stable |
| 98 | `snapshotDiffReportListingRemoteIterator` | `RemoteIterator
      <SnapshotDiffReportListing>` | `Path, String, String` |  | Stable |
| 99 | `hasNext` | `boolean` | `` |  | Stable |
| 100 | `getSnapshotDiffReport` | `SnapshotDiffReport` | `Path, String, String` |  | Stable |
| 101 | `getSnapshotDiffReportListing` | `SnapshotDiffReportListing` | `Path, String, String, String, int` |  | Stable |
| 102 | `isFileClosed` | `boolean` | `Path` |  | Stable |
| 103 | `addCacheDirective` | `long` | `CacheDirectiveInfo` | 添加缓存指令 | Stable |
| 104 | `addCacheDirective` | `long` | `CacheDirectiveInfo, EnumSet<>` | 添加缓存指令 | Stable |
| 105 | `modifyCacheDirective` | `void` | `CacheDirectiveInfo` |  | Stable |
| 106 | `modifyCacheDirective` | `void` | `CacheDirectiveInfo, EnumSet<>` |  | Stable |
| 107 | `removeCacheDirective` | `void` | `long` | 移除缓存指令 | Stable |
| 108 | `listCacheDirectives` | `RemoteIterator<CacheDirectiveEntry>` | `CacheDirectiveInfo` | 列出缓存指令 | Stable |
| 109 | `hasNext` | `boolean` | `` |  | Stable |
| 110 | `addCachePool` | `void` | `CachePoolInfo` | 添加缓存池 | Stable |
| 111 | `modifyCachePool` | `void` | `CachePoolInfo` | 修改缓存池 | Stable |
| 112 | `removeCachePool` | `void` | `String` | 移除缓存池 | Stable |
| 113 | `listCachePools` | `RemoteIterator<CachePoolEntry>` | `` | 列出缓存池 | Stable |
| 114 | `modifyAclEntries` | `void` | `Path, List<>` | 修改ACL条目 | Stable |
| 115 | `removeAclEntries` | `void` | `Path, List<>` | 移除ACL条目 | Stable |
| 116 | `removeDefaultAcl` | `void` | `Path` | 移除默认ACL | Stable |
| 117 | `removeAcl` | `void` | `Path` | 移除所有ACL | Stable |
| 118 | `setAcl` | `void` | `Path, List<>` | 设置ACL | Stable |
| 119 | `getAclStatus` | `AclStatus` | `Path` | 获取ACL状态 | Stable |
| 120 | `createEncryptionZone` | `void` | `Path, String` | 创建加密区 | Stable |
| 121 | `getEZForPath` | `EncryptionZone` | `Path` | 获取路径的加密区 | Stable |
| 122 | `listEncryptionZones` | `RemoteIterator<EncryptionZone>` | `` | 列出加密区 | Stable |
| 123 | `reencryptEncryptionZone` | `void` | `Path, ReencryptAction` |  | Stable |
| 124 | `listReencryptionStatus` | `RemoteIterator<ZoneReencryptionStatus>` | `` |  | Stable |
| 125 | `getFileEncryptionInfo` | `FileEncryptionInfo` | `Path` |  | Stable |
| 126 | `provisionEZTrash` | `void` | `Path, FsPermission` |  | Stable |
| 127 | `provisionSnapshotTrash` | `Path` | `Path, FsPermission` |  | Stable |
| 128 | `setXAttr` | `void` | `Path, String, byte[], EnumSet<>` | 设置扩展属性 | Stable |
| 129 | `getXAttr` | `byte[]` | `Path, String` | 获取扩展属性 | Stable |
| 130 | `getXAttrs` | `Map<String, byte[]>` | `Path` | 批量获取扩展属性 | Stable |
| 131 | `getXAttrs` | `Map<String, byte[]>` | `Path, List<>` | 批量获取扩展属性 | Stable |
| 132 | `listXAttrs` | `List<String>` | `Path` | 列出扩展属性 | Stable |
| 133 | `removeXAttr` | `void` | `Path, String` | 移除扩展属性 | Stable |
| 134 | `access` | `void` | `Path, FsAction` |  | Stable |
| 135 | `getKeyProviderUri` | `URI` | `` |  | Stable |
| 136 | `getKeyProvider` | `KeyProvider` | `` |  | Stable |
| 137 | `getAdditionalTokenIssuers` | `DelegationTokenIssuer[]` | `` |  | Stable |
| 138 | `getInotifyEventStream` | `DFSInotifyEventInputStream` | `` | 获取inotify事件流 | Stable |
| 139 | `getInotifyEventStream` | `DFSInotifyEventInputStream` | `long` | 获取inotify事件流 | Stable |
| 140 | `setErasureCodingPolicy` | `void` | `Path, String` | 设置纠删码策略 | Stable |
| 141 | `satisfyStoragePolicy` | `void` | `Path` |  | Stable |
| 142 | `getErasureCodingPolicy` | `ErasureCodingPolicy` | `Path` | 获取纠删码策略 | Stable |
| 143 | `getAllErasureCodingPolicies` | `Collection<ErasureCodingPolicyInfo>` | `` |  | Stable |
| 144 | `getAllErasureCodingCodecs` | `Map<String, String>` | `` |  | Stable |
| 145 | `addErasureCodingPolicies` | `AddErasureCodingPolicyResponse[]` | `ErasureCodingPolicy[]` | 添加纠删码策略 | Stable |
| 146 | `removeErasureCodingPolicy` | `void` | `String` | 移除纠删码策略 | Stable |
| 147 | `enableErasureCodingPolicy` | `void` | `String` |  | Stable |
| 148 | `disableErasureCodingPolicy` | `void` | `String` |  | Stable |
| 149 | `unsetErasureCodingPolicy` | `void` | `Path` | 取消纠删码策略 | Stable |
| 150 | `getECTopologyResultForPolicies` | `ECTopologyVerifierResult` | `String...` |  | Stable |
| 151 | `getTrashRoot` | `Path` | `Path` |  | Stable |
| 152 | `getTrashRoots` | `Collection<FileStatus>` | `boolean` |  | Stable |
| 153 | `getThisBuilder` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 154 | `favoredNodes` | `HdfsDataOutputStreamBuilder` | `@Nonnull` |  | Stable |
| 155 | `syncBlock` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 156 | `lazyPersist` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 157 | `newBlock` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 158 | `noLocalWrite` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 159 | `noLocalRack` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 160 | `storagePolicyName` | `HdfsDataOutputStreamBuilder` | `@Nonnull` |  | Stable |
| 161 | `ecPolicyName` | `HdfsDataOutputStreamBuilder` | `@Nonnull` |  | Stable |
| 162 | `replicate` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 163 | `ignoreClientLocality` | `HdfsDataOutputStreamBuilder` | `` |  | Stable |
| 164 | `build` | `FSDataOutputStream` | `` |  | Stable |
| 165 | `createFile` | `HdfsDataOutputStreamBuilder` | `Path` |  | Stable |
| 166 | `listOpenFiles` | `RemoteIterator<OpenFileEntry>` | `` |  | Deprecated |
| 167 | `listOpenFiles` | `RemoteIterator<OpenFileEntry>` | `EnumSet<>` |  | Deprecated |
| 168 | `listOpenFiles` | `RemoteIterator<OpenFileEntry>` | `EnumSet<>, String` |  | Deprecated |
| 169 | `appendFile` | `HdfsDataOutputStreamBuilder` | `Path` |  | Stable |
| 170 | `hasPathCapability` | `boolean` | `Path, String` |  | Stable |
| 171 | `createMultipartUploader` | `MultipartUploaderBuilder` | `Path` |  | Stable |
| 172 | `getSlowDatanodeStats` | `DatanodeInfo[]` | `` |  | Stable |
| 173 | `getLocatedBlocks` | `LocatedBlocks` | `Path, long, long` | 获取定位块信息 | Stable |
| 174 | `getEnclosingRoot` | `Path` | `Path` |  | Stable |

**方法统计**: 174个方法 (4个已废弃)

---

## HdfsConfiguration

**包路径**: `HdfsConfiguration`

**稳定性标注**: `@InterfaceAudience.Public`

**说明**: Stable

**功能**: HDFS配置类,扩展Configuration添加HDFS特定配置

| 序号 | 方法名 | 返回类型 | 参数 | 描述 | 状态 |
|------|--------|----------|------|------|------|
| 1 | `init` | `void` | `` |  | Stable |
| 2 | `main` | `void` | `String[]` |  | Stable |

**方法统计**: 2个方法 (0个已废弃)

---

## HdfsDataOutputStream

**包路径**: `HdfsDataOutputStream`

**稳定性标注**: `@InterfaceAudience.Public`

**说明**: Stable

**功能**: HDFS数据输出流,扩展FSDataOutputStream

| 序号 | 方法名 | 返回类型 | 参数 | 描述 | 状态 |
|------|--------|----------|------|------|------|
| 1 | `getCurrentBlockReplication` | `int` | `` |  | Stable |
| 2 | `hsync` | `void` | `EnumSet<>` |  | Stable |

**方法统计**: 2个方法 (0个已废弃)

---

## HdfsDataInputStream

**包路径**: `HdfsDataInputStream`

**稳定性标注**: `@InterfaceAudience.Public`

**说明**: Stable

**功能**: HDFS数据输入流,扩展FSDataInputStream

| 序号 | 方法名 | 返回类型 | 参数 | 描述 | 状态 |
|------|--------|----------|------|------|------|
| 1 | `getWrappedStream` | `InputStream` | `` |  | Stable |
| 2 | `getCurrentDatanode` | `DatanodeInfo` | `` |  | Stable |
| 3 | `getCurrentBlock` | `ExtendedBlock` | `` |  | Stable |
| 4 | `getAllBlocks` | `List<LocatedBlock>` | `` |  | Stable |
| 5 | `getVisibleLength` | `long` | `` |  | Stable |
| 6 | `getReadStatistics` | `ReadStatistics` | `` |  | Stable |
| 7 | `clearReadStatistics` | `void` | `` |  | Stable |

**方法统计**: 7个方法 (0个已废弃)

---

## DFSClient

**包路径**: `DFSClient`

**稳定性标注**: `@InterfaceAudience.Private`

**说明**: Private,内部类不应直接使用

**功能**: HDFS客户端核心类,直接与NameNode和DataNode通信

| 序号 | 方法名 | 返回类型 | 参数 | 描述 | 状态 |
|------|--------|----------|------|------|------|
| 1 | `setDisabledStopDeadNodeDetectorThreadForTest` | `void` | `boolean` |  | Stable |
| 2 | `getConf` | `DfsClientConf` | `` |  | Stable |
| 3 | `getClientName` | `String` | `` |  | Stable |
| 4 | `getLeaseRenewer` | `LeaseRenewer` | `` |  | Stable |
| 5 | `putFileBeingWritten` | `void` | `String, DFSOutputStream` |  | Stable |
| 6 | `removeFileBeingWritten` | `void` | `String` |  | Stable |
| 7 | `isFilesBeingWrittenEmpty` | `boolean` | `` |  | Stable |
| 8 | `isClientRunning` | `boolean` | `` |  | Stable |
| 9 | `getNumOfFilesBeingWritten` | `int` | `` |  | Stable |
| 10 | `renewLease` | `boolean` | `` |  | Stable |
| 11 | `closeAllFilesBeingWritten` | `void` | `boolean` |  | Stable |
| 12 | `close` | `void` | `` |  | Stable |
| 13 | `closeOutputStreams` | `void` | `boolean` |  | Stable |
| 14 | `getBlockSize` | `long` | `String` |  | Stable |
| 15 | `getServerDefaults` | `FsServerDefaults` | `` |  | Stable |
| 16 | `getCanonicalServiceName` | `String` | `` |  | Stable |
| 17 | `getDelegationToken` | `Token<DelegationTokenIdentifier>` | `Text` | 获取委托令牌 | Stable |
| 18 | `renewDelegationToken` | `long` | `Token<>` |  | Deprecated |
| 19 | `cancelDelegationToken` | `void` | `Token<>` | 取消委托令牌 | Deprecated |
| 20 | `handleKind` | `boolean` | `Text` |  | Stable |
| 21 | `renew` | `long` | `Token<>, Configuration` |  | Stable |
| 22 | `cancel` | `void` | `Token<>, Configuration` |  | Stable |
| 23 | `isManaged` | `boolean` | `Token<>` |  | Stable |
| 24 | `reportBadBlocks` | `void` | `LocatedBlock[]` |  | Stable |
| 25 | `getRefreshReadBlkLocationsInterval` | `long` | `` |  | Stable |
| 26 | `getLocatedBlocks` | `LocatedBlocks` | `String, long` | 获取定位块信息 | Stable |
| 27 | `getLocatedBlocks` | `LocatedBlocks` | `String, long, long` | 获取定位块信息 | Stable |
| 28 | `getBlockLocations` | `BlockLocation[]` | `String, long, long` | 获取块位置 | Stable |
| 29 | `createWrappedInputStream` | `HdfsDataInputStream` | `DFSInputStream` |  | Stable |
| 30 | `createWrappedOutputStream` | `HdfsDataOutputStream` | `DFSOutputStream, FileSystem.Statistics` |  | Stable |
| 31 | `createWrappedOutputStream` | `HdfsDataOutputStream` | `DFSOutputStream, FileSystem.Statistics, long` |  | Stable |
| 32 | `open` | `DFSInputStream` | `String` | 打开文件读取 | Stable |
| 33 | `open` | `DFSInputStream` | `String, int, boolean, FileSystem.Statistics` | 打开文件读取 | Stable |
| 34 | `open` | `DFSInputStream` | `String, int, boolean` | 打开文件读取 | Stable |
| 35 | `open` | `DFSInputStream` | `HdfsPathHandle, int, boolean` | 打开文件读取 | Stable |
| 36 | `getNamenode` | `ClientProtocol` | `` |  | Stable |
| 37 | `create` | `OutputStream` | `String, boolean` | 创建文件 | Stable |
| 38 | `create` | `OutputStream` | `String, boolean, Progressable` | 创建文件 | Stable |
| 39 | `create` | `OutputStream` | `String, boolean, short, long` | 创建文件 | Stable |
| 40 | `create` | `OutputStream` | `String, boolean, short, long, Progressable` | 创建文件 | Stable |
| 41 | `create` | `OutputStream` | `String, boolean, short, long, Progressable, int` | 创建文件 | Stable |
| 42 | `create` | `DFSOutputStream` | `String, FsPermission, EnumSet<>, short, long, Progressable, int, ChecksumOpt` | 创建文件 | Stable |
| 43 | `create` | `DFSOutputStream` | `String, FsPermission, EnumSet<>, boolean, short, long, Progressable, int, ChecksumOpt` | 创建文件 | Stable |
| 44 | `create` | `DFSOutputStream` | `String, FsPermission, EnumSet<>, boolean, short, long, Progressable, int, ChecksumOpt, InetSocketAddress[]` | 创建文件 | Stable |
| 45 | `create` | `DFSOutputStream` | `String, FsPermission, EnumSet<>, boolean, short, long, Progressable, int, ChecksumOpt, InetSocketAddress[], String` | 创建文件 | Stable |
| 46 | `create` | `DFSOutputStream` | `String, FsPermission, EnumSet<>, boolean, short, long, Progressable, int, ChecksumOpt, InetSocketAddress[], String, String` | 创建文件 | Stable |
| 47 | `primitiveCreate` | `DFSOutputStream` | `String, FsPermission, EnumSet<>, boolean, short, long, Progressable, int, ChecksumOpt` |  | Stable |
| 48 | `createSymlink` | `void` | `String, String, boolean` |  | Stable |
| 49 | `getLinkTarget` | `String` | `String` |  | Stable |
| 50 | `append` | `HdfsDataOutputStream` | `String, int, EnumSet<>, Progressable, FileSystem.Statistics` | 追加文件 | Stable |
| 51 | `append` | `HdfsDataOutputStream` | `String, int, EnumSet<>, Progressable, FileSystem.Statistics, InetSocketAddress[]` | 追加文件 | Stable |
| 52 | `setReplication` | `boolean` | `String, short` | 设置文件副本数 | Stable |
| 53 | `setStoragePolicy` | `void` | `String, String` | 设置存储策略 | Stable |
| 54 | `unsetStoragePolicy` | `void` | `String` | 取消存储策略 | Stable |
| 55 | `getStoragePolicy` | `BlockStoragePolicy` | `String` | 获取存储策略 | Stable |
| 56 | `getStoragePolicies` | `BlockStoragePolicy[]` | `` | 获取所有存储策略 | Stable |
| 57 | `rename` | `boolean` | `String, String` | 重命名文件 | Deprecated |
| 58 | `concat` | `void` | `String, String` | 连接文件 | Stable |
| 59 | `rename` | `void` | `String, String, Options.Rename...` | 重命名文件 | Deprecated |
| 60 | `truncate` | `boolean` | `String, long` | 截断文件 | Stable |
| 61 | `delete` | `boolean` | `String` | 删除文件或目录 | Deprecated |
| 62 | `delete` | `boolean` | `String, boolean` | 删除文件或目录 | Deprecated |
| 63 | `exists` | `boolean` | `String` | 检查文件是否存在 | Stable |
| 64 | `listPaths` | `DirectoryListing` | `String, byte[]` |  | Stable |
| 65 | `listPaths` | `DirectoryListing` | `String, byte[], boolean` |  | Stable |
| 66 | `batchedListPaths` | `BatchedDirectoryListing` | `String[], byte[], boolean` |  | Stable |
| 67 | `getFileInfo` | `HdfsFileStatus` | `String` |  | Stable |
| 68 | `getLocatedFileInfo` | `HdfsLocatedFileStatus` | `String, boolean` |  | Stable |
| 69 | `isFileClosed` | `boolean` | `String` |  | Stable |
| 70 | `getFileLinkInfo` | `HdfsFileStatus` | `String` |  | Stable |
| 71 | `clearDataEncryptionKey` | `void` | `` |  | Stable |
| 72 | `newDataEncryptionKey` | `DataEncryptionKey` | `` |  | Stable |
| 73 | `getEncryptionKey` | `DataEncryptionKey` | `` |  | Stable |
| 74 | `getFileChecksumWithCombineMode` | `FileChecksum` | `String, long` |  | Stable |
| 75 | `getFileChecksum` | `MD5MD5CRC32FileChecksum` | `String, long` |  | Stable |
| 76 | `setPermission` | `void` | `String, FsPermission` | 设置文件权限 | Stable |
| 77 | `setOwner` | `void` | `String, String, String` | 设置文件所有者 | Stable |
| 78 | `getDiskStatus` | `FsStatus` | `` |  | Stable |
| 79 | `getStateAtIndex` | `long` | `long[], int` |  | Stable |
| 80 | `getMissingBlocksCount` | `long` | `` |  | Stable |
| 81 | `getMissingReplOneBlocksCount` | `long` | `` |  | Stable |
| 82 | `getPendingDeletionBlocksCount` | `long` | `` |  | Stable |
| 83 | `getLowRedundancyBlocksCount` | `long` | `` |  | Stable |
| 84 | `getCorruptBlocksCount` | `long` | `` |  | Stable |
| 85 | `getBytesInFutureBlocks` | `long` | `` |  | Stable |
| 86 | `listCorruptFileBlocks` | `CorruptFileBlocks` | `String, String` | 列出损坏文件块 | Stable |
| 87 | `datanodeReport` | `DatanodeInfo[]` | `DatanodeReportType` |  | Stable |
| 88 | `getDatanodeStorageReport` | `DatanodeStorageReport[]` | `DatanodeReportType` | 获取数据节点存储报告 | Stable |
| 89 | `setSafeMode` | `boolean` | `SafeModeAction` | 设置安全模式 | Stable |
| 90 | `setSafeMode` | `boolean` | `SafeModeAction, boolean` | 设置安全模式 | Stable |
| 91 | `createSnapshot` | `String` | `String, String` | 创建快照 | Stable |
| 92 | `deleteSnapshot` | `void` | `String, String` | 删除快照 | Stable |
| 93 | `renameSnapshot` | `void` | `String, String, String` | 重命名快照 | Stable |
| 94 | `getSnapshottableDirListing` | `SnapshottableDirectoryStatus[]` | `` |  | Stable |
| 95 | `getSnapshotListing` | `SnapshotStatus[]` | `String` |  | Stable |
| 96 | `allowSnapshot` | `void` | `String` | 允许快照 | Stable |
| 97 | `disallowSnapshot` | `void` | `String` | 禁用快照 | Stable |
| 98 | `getSnapshotDiffReport` | `SnapshotDiffReport` | `String, String, String` |  | Stable |
| 99 | `getSnapshotDiffReportListing` | `SnapshotDiffReportListing` | `String, String, String, byte[], int` |  | Stable |
| 100 | `addCacheDirective` | `long` | `CacheDirectiveInfo, EnumSet<>` | 添加缓存指令 | Stable |
| 101 | `modifyCacheDirective` | `void` | `CacheDirectiveInfo, EnumSet<>` |  | Stable |
| 102 | `removeCacheDirective` | `void` | `long` | 移除缓存指令 | Stable |
| 103 | `listCacheDirectives` | `RemoteIterator<CacheDirectiveEntry>` | `CacheDirectiveInfo` | 列出缓存指令 | Stable |
| 104 | `addCachePool` | `void` | `CachePoolInfo` | 添加缓存池 | Stable |
| 105 | `modifyCachePool` | `void` | `CachePoolInfo` | 修改缓存池 | Stable |
| 106 | `removeCachePool` | `void` | `String` | 移除缓存池 | Stable |
| 107 | `listCachePools` | `RemoteIterator<CachePoolEntry>` | `` | 列出缓存池 | Stable |
| 108 | `refreshNodes` | `void` | `` | 刷新节点列表 | Stable |
| 109 | `metaSave` | `void` | `String` | 保存元数据信息 | Stable |
| 110 | `setBalancerBandwidth` | `void` | `long` |  | Stable |
| 111 | `finalizeUpgrade` | `void` | `` | 完成升级 | Stable |
| 112 | `upgradeStatus` | `boolean` | `` |  | Stable |
| 113 | `mkdirs` | `boolean` | `String` | 创建目录 | Deprecated |
| 114 | `mkdirs` | `boolean` | `String, FsPermission, boolean` | 创建目录 | Deprecated |
| 115 | `primitiveMkdir` | `boolean` | `String, FsPermission` |  | Stable |
| 116 | `primitiveMkdir` | `boolean` | `String, FsPermission, boolean` |  | Stable |
| 117 | `setTimes` | `void` | `String, long, long` | 设置文件时间 | Stable |
| 118 | `getDefaultReadCachingStrategy` | `CachingStrategy` | `` |  | Stable |
| 119 | `getDefaultWriteCachingStrategy` | `CachingStrategy` | `` |  | Stable |
| 120 | `getClientContext` | `ClientContext` | `` |  | Stable |
| 121 | `modifyAclEntries` | `void` | `String, List<>` | 修改ACL条目 | Stable |
| 122 | `removeAclEntries` | `void` | `String, List<>` | 移除ACL条目 | Stable |
| 123 | `removeDefaultAcl` | `void` | `String` | 移除默认ACL | Stable |
| 124 | `removeAcl` | `void` | `String` | 移除所有ACL | Stable |
| 125 | `setAcl` | `void` | `String, List<>` | 设置ACL | Stable |
| 126 | `getAclStatus` | `AclStatus` | `String` | 获取ACL状态 | Stable |
| 127 | `createEncryptionZone` | `void` | `String, String` | 创建加密区 | Stable |
| 128 | `getEZForPath` | `EncryptionZone` | `String` | 获取路径的加密区 | Stable |
| 129 | `listEncryptionZones` | `RemoteIterator<EncryptionZone>` | `` | 列出加密区 | Stable |
| 130 | `reencryptEncryptionZone` | `void` | `String, ReencryptAction` |  | Stable |
| 131 | `listReencryptionStatus` | `RemoteIterator<ZoneReencryptionStatus>` | `` |  | Stable |
| 132 | `setErasureCodingPolicy` | `void` | `String, String` | 设置纠删码策略 | Stable |
| 133 | `unsetErasureCodingPolicy` | `void` | `String` | 取消纠删码策略 | Stable |
| 134 | `getECTopologyResultForPolicies` | `ECTopologyVerifierResult` | `String...` |  | Stable |
| 135 | `setXAttr` | `void` | `String, String, byte[], EnumSet<>` | 设置扩展属性 | Stable |
| 136 | `getXAttr` | `byte[]` | `String, String` | 获取扩展属性 | Stable |
| 137 | `getXAttrs` | `Map<String, byte[]>` | `String` | 批量获取扩展属性 | Stable |
| 138 | `getXAttrs` | `Map<String, byte[]>` | `String, List<>` | 批量获取扩展属性 | Stable |
| 139 | `listXAttrs` | `List<String>` | `String` | 列出扩展属性 | Stable |
| 140 | `removeXAttr` | `void` | `String, String` | 移除扩展属性 | Stable |
| 141 | `checkAccess` | `void` | `String, FsAction` |  | Stable |
| 142 | `getErasureCodingPolicies` | `ErasureCodingPolicyInfo[]` | `` | 获取所有纠删码策略 | Stable |
| 143 | `getErasureCodingCodecs` | `Map<String, String>` | `` |  | Stable |
| 144 | `addErasureCodingPolicies` | `AddErasureCodingPolicyResponse[]` | `ErasureCodingPolicy[]` | 添加纠删码策略 | Stable |
| 145 | `removeErasureCodingPolicy` | `void` | `String` | 移除纠删码策略 | Stable |
| 146 | `enableErasureCodingPolicy` | `void` | `String` |  | Stable |
| 147 | `disableErasureCodingPolicy` | `void` | `String` |  | Stable |
| 148 | `getInotifyEventStream` | `DFSInotifyEventInputStream` | `` | 获取inotify事件流 | Stable |
| 149 | `getInotifyEventStream` | `DFSInotifyEventInputStream` | `long` | 获取inotify事件流 | Stable |
| 150 | `newConnectedPeer` | `Peer` | `InetSocketAddress, Token<>, DatanodeID` |  | Stable |
| 151 | `newThread` | `Thread` | `Runnable` |  | Stable |
| 152 | `rejectedExecution` | `void` | `Runnable, ThreadPoolExecutor` |  | Stable |
| 153 | `getKeyProviderUri` | `URI` | `` |  | Stable |
| 154 | `getKeyProvider` | `KeyProvider` | `` |  | Stable |
| 155 | `setKeyProvider` | `void` | `KeyProvider` |  | Stable |
| 156 | `getSaslDataTransferClient` | `SaslDataTransferClient` | `` |  | Stable |
| 157 | `getErasureCodingPolicy` | `ErasureCodingPolicy` | `String` | 获取纠删码策略 | Stable |
| 158 | `satisfyStoragePolicy` | `void` | `String` |  | Stable |
| 159 | `listOpenFiles` | `RemoteIterator<OpenFileEntry>` | `` |  | Deprecated |
| 160 | `listOpenFiles` | `RemoteIterator<OpenFileEntry>` | `String` |  | Deprecated |
| 161 | `listOpenFiles` | `RemoteIterator<OpenFileEntry>` | `EnumSet<>` |  | Deprecated |
| 162 | `listOpenFiles` | `RemoteIterator<OpenFileEntry>` | `EnumSet<>, String` |  | Deprecated |
| 163 | `msync` | `void` | `` |  | Stable |
| 164 | `getHAServiceState` | `HAServiceProtocol.HAServiceState` | `` |  | Stable |
| 165 | `getDeadNodes` | `ConcurrentHashMap<DatanodeInfo, DatanodeInfo>` | `DFSInputStream` |  | Stable |
| 166 | `isDeadNode` | `boolean` | `DFSInputStream, DatanodeInfo` |  | Stable |
| 167 | `addNodeToDeadNodeDetector` | `void` | `DFSInputStream, DatanodeInfo` |  | Stable |
| 168 | `removeNodeFromDeadNodeDetector` | `void` | `DFSInputStream, DatanodeInfo` |  | Stable |
| 169 | `removeNodeFromDeadNodeDetector` | `void` | `DFSInputStream, LocatedBlocks` |  | Stable |
| 170 | `getDeadNodeDetector` | `DeadNodeDetector` | `` |  | Stable |
| 171 | `getLocatedBlockRefresher` | `LocatedBlocksRefresher` | `` |  | Stable |
| 172 | `addLocatedBlocksRefresh` | `void` | `DFSInputStream` |  | Stable |
| 173 | `removeLocatedBlocksRefresh` | `void` | `DFSInputStream` |  | Stable |
| 174 | `slowDatanodeReport` | `DatanodeInfo[]` | `` |  | Stable |
| 175 | `getEnclosingRoot` | `Path` | `String` |  | Stable |

**方法统计**: 175个方法 (12个已废弃)

---

## 总体统计

- **总类数**: 5
- **总方法数**: 360
- **已废弃方法**: 16
- **稳定方法**: 344

### API稳定性分布

| 状态 | 数量 | 占比 |
|------|------|------|
| Stable | 344 | 95.6% |
| Deprecated | 16 | 4.4% |

---

## 常用示例

### 1. 创建文件并写入数据
```java
Configuration conf = new Configuration();
FileSystem fs = FileSystem.get(conf);
Path path = new Path("/user/test/file.txt");

FSDataOutputStream out = fs.create(path);
out.writeUTF("Hello HDFS");
out.close();
```

### 2. 读取文件内容
```java
FileSystem fs = FileSystem.get(conf);
Path path = new Path("/user/test/file.txt");

FSDataInputStream in = fs.open(path);
String content = in.readUTF();
in.close();
```

### 3. 设置副本数
```java
FileSystem fs = FileSystem.get(conf);
fs.setReplication(new Path("/user/test/file.txt"), 3);
```

### 4. 创建快照
```java
DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
Path snapshotPath = dfs.createSnapshot(new Path("/user/test"), "snapshot1");
```

---

**文档生成完成**