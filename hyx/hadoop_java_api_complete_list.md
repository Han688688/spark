# Hadoop Java API Complete List

> 参考官方文档: https://hadoop.apache.org/docs/stable/api/
> 版本: Apache Hadoop Main 3.3.5 API
> 生成时间: 2026-04-29

---

## 1. HDFS Client API (org.apache.hadoop.fs)

### 1.1 FileSystem 接口
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

FileSystem是Hadoop文件系统的抽象基类，所有用户代码应使用FileSystem对象。

| 方法签名 | 描述 |
|---------|------|
| `FSDataOutputStream append(Path f)` | 追加到现有文件(可选操作) |
| `FSDataOutputStream append(Path f, int bufferSize)` | 追加到现有文件，指定缓冲区大小 |
| `FSDataOutputStream append(Path f, int bufferSize, Progressable progress)` | 追加到现有文件，抽象方法 |
| `FSDataOutputStreamBuilder appendFile(Path path)` | 创建追加文件的Builder |
| `boolean cancelDeleteOnExit(Path f)` | 取消关闭时删除路径的调度 |
| `void close()` | 关闭FileSystem实例 |
| `void concat(Path trg, Path[] psrcs)` | 连接现有文件 |
| `void copyFromLocalFile(Path src, Path dst)` | 从本地磁盘复制文件到FileSystem |
| `void copyFromLocalFile(boolean delSrc, Path src, Path dst)` | 从本地复制，可删除源文件 |
| `void copyFromLocalFile(boolean delSrc, boolean overwrite, Path src, Path dst)` | 从本地复制，可覆盖 |
| `void copyFromLocalFile(boolean delSrc, boolean overwrite, Path[] srcs, Path dst)` | 批量从本地复制 |
| `void copyToLocalFile(Path src, Path dst)` | 从远程文件系统复制到本地 |
| `void copyToLocalFile(boolean delSrc, Path src, Path dst)` | 复制到本地，可删除源 |
| `void copyToLocalFile(boolean delSrc, Path src, Path dst, boolean useRawLocalFileSystem)` | 复制到本地，指定是否使用原始本地文件系统 |
| `FSDataOutputStream create(Path f)` | 创建文件 |
| `FSDataOutputStream create(Path f, boolean overwrite)` | 创建文件，可覆盖 |
| `FSDataOutputStream create(Path f, boolean overwrite, int bufferSize)` | 创建文件，指定缓冲区大小 |
| `FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, Progressable progress)` | 创建文件，带进度报告 |
| `FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize)` | 创建文件，指定副本数和块大小 |
| `FSDataOutputStream create(Path f, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)` | 创建文件，完整参数，带进度 |
| `FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)` | 创建文件，抽象方法，带权限 |
| `FSDataOutputStream create(Path f, FsPermission permission, EnumSet<CreateFlag> flags, int bufferSize, short replication, long blockSize, Progressable progress)` | 创建文件，带CreateFlag |
| `FSDataOutputStream create(Path f, Progressable progress)` | 创建文件，带进度报告 |
| `FSDataOutputStream create(Path f, short replication)` | 创建文件，指定副本数 |
| `FSDataOutputStream create(Path f, short replication, Progressable progress)` | 创建文件，指定副本数和进度 |
| `FSDataOutputStreamBuilder createFile(Path path)` | 创建新文件的Builder |
| `boolean createNewFile(Path f)` | 创建新的零长度文件 |
| `Path createSnapshot(Path path)` | 创建快照，默认名称 |
| `Path createSnapshot(Path path, String snapshotName)` | 创建指定名称的快照 |
| `void createSymlink(Path target, Path link, boolean createParent)` | 创建符号链接 |
| `boolean delete(Path f)` | 删除文件，已废弃，使用delete(Path, boolean) |
| `boolean delete(Path f, boolean recursive)` | 删除文件/目录，抽象方法 |
| `boolean deleteOnExit(Path f)` | 标记关闭时删除 |
| `void deleteSnapshot(Path path, String snapshotName)` | 删除快照 |
| `boolean exists(Path f)` | 检查文件是否存在 |
| `BlockLocation[] getFileBlockLocations(Path p, long start, long len)` | 获取文件块位置 |
| `BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len)` | 获取文件块位置 |
| `FileStatus getFileStatus(Path f)` | 获取文件状态 |
| `FsStatus getStatus()` | 获取文件系统状态 |
| `FsStatus getStatus(Path p)` | 获取指定路径的文件系统状态 |
| `long getLength(Path f)` | 获取文件长度(已废弃) |
| `Path getHomeDirectory()` | 获取用户主目录 |
| `FileSystem[] getChildFileSystems()` | 获取子文件系统 |
| `String getCanonicalServiceName()` | 获取规范服务名称 |
| `String getScheme()` | 获取URI scheme |
| `URI getUri()` | 获取URI |
| `Path getWorkingDirectory()` | 获取工作目录 |
| `void initialize(URI uri, Configuration conf)` | 初始化文件系统 |
| `boolean isDirectory(Path f)` | 是否为目录(已废弃) |
| `boolean isFile(Path f)` | 是否为文件(已废弃) |
| `RemoteIterator<LocatedFileStatus> listFiles(Path f, boolean recursive)` | 列出文件 |
| `RemoteIterator<LocatedFileStatus> listLocatedStatus(Path f)` | 列出定位状态 |
| `FileStatus[] listStatus(Path f)` | 列出路径状态 |
| `FileStatus[] listStatus(Path[] files)` | 批量列出状态 |
| `FileStatus[] listStatus(Path f, PathFilter filter)` | 列出状态，带过滤器 |
| `FileStatus[] listStatus(Path[] files, PathFilter filter)` | 批量列出状态，带过滤器 |
| `boolean mkdirs(Path f)` | 创建目录(已废弃) |
| `boolean mkdirs(Path f, FsPermission permission)` | 创建目录，带权限 |
| `FSDataInputStream open(Path f)` | 打开文件读取 |
| `FSDataInputStream open(Path f, int bufferSize)` | 打开文件，指定缓冲区大小 |
| `boolean rename(Path src, Path dst)` | 重命名文件 |
| `void setWorkingDirectory(Path new_dir)` | 设置工作目录 |
| `Path resolvePath(Path p)` | 解析路径 |
| `void setPermission(Path p, FsPermission permission)` | 设置权限 |
| `void setOwner(Path p, String username, String groupname)` | 设置所有者 |
| `void setTimes(Path p, long mtime, long atime)` | 设置修改时间和访问时间 |
| `boolean truncate(Path f, long newLength)` | 截断文件 |
| `Token<?> getDelegationToken(String renewer)` | 获取委托令牌 |
| `boolean hasPathCapability(Path path, String capability)` | 检查路径能力 |

**静态方法**:
| 方法签名 | 描述 |
|---------|------|
| `static FileSystem get(Configuration conf)` | 获取文件系统 |
| `static FileSystem get(URI uri, Configuration conf)` | 根据URI获取文件系统 |
| `static FileSystem get(URI uri, Configuration conf, String user)` | 根据URI和用户获取 |
| `static FileSystem getLocal(Configuration conf)` | 获取本地文件系统 |
| `static void closeAll()` | 关闭所有缓存的文件系统实例 |
| `static void clearStatistics()` | 重置所有统计 |
| `static Map<String, Statistics> getStatistics()` | 获取统计信息 |

### 1.2 Path 类
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `String toString()` | 返回路径字符串 |
| `String getName()` | 返回路径最后组件名称 |
| `Path getParent()` | 返回父路径，根目录返回null |
| `int depth()` | 返回路径元素数量 |
| `boolean isAbsolute()` | 检查路径是否绝对 |
| `boolean isUriPathAbsolute()` | 检查URI路径是否绝对 |
| `boolean isRoot()` | 是否为根路径 |
| `URI toUri()` | 转换为URI |
| `FileSystem getFileSystem(Configuration conf)` | 获取此路径的文件系统 |
| `Path suffix(String suffix)` | 添加后缀到路径名称 |
| `int compareTo(Path o)` | 比较路径 |
| `boolean equals(Object o)` | 判断相等 |
| `int hashCode()` | 获取哈希值 |

**静态方法**:
| 方法签名 | 描述 |
|---------|------|
| `static Path getPathWithoutSchemeAndAuthority(Path path)` | 返回无scheme和authority的路径 |
| `static Path mergePaths(Path path1, Path path2)` | 合并两个路径 |
| `static boolean isWindowsAbsolutePath(String pathString, boolean slashed)` | 检查Windows绝对路径 |

### 1.3 FileStatus 类
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `Path getPath()` | 获取文件路径 |
| `long getLen()` | 获取文件长度(字节) |
| `boolean isFile()` | 是否为文件 |
| `boolean isDirectory()` | 是否为目录 |
| `boolean isDir()` | 是否为目录(已废弃) |
| `boolean isSymlink()` | 是否为符号链接 |
| `short getReplication()` | 获取副本数 |
| `long getBlockSize()` | 获取块大小 |
| `long getModificationTime()` | 获取修改时间 |
| `long getAccessTime()` | 获取访问时间 |
| `FsPermission getPermission()` | 获取权限 |
| `String getOwner()` | 获取所有者 |
| `String getGroup()` | 获取所属组 |
| `Path getSymlink()` | 获取符号链接目标 |
| `boolean hasAcl()` | 是否有ACL |
| `boolean isEncrypted()` | 是否加密 |
| `boolean isErasureCoded()` | 是否纠删码 |
| `boolean isSnapshotEnabled()` | 是否启用快照 |

### 1.4 FSDataInputStream
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `void seek(long desired)` | 定位到指定偏移量 |
| `long getPos()` | 获取当前位置 |
| `int read(long position, byte[] buffer, int offset, int length)` | 从指定位置读取 |
| `void readFully(long position, byte[] buffer)` | 完整读取到缓冲区 |
| `void readFully(long position, byte[] buffer, int offset, int length)` | 完整读取，指定偏移和长度 |
| `boolean seekToNewSource(long targetPos)` | 定位到新数据源 |
| `int read(ByteBuffer buf)` | 读取到ByteBuffer |
| `void setReadahead(Long readahead)` | 设置预读 |
| `void setDropBehind(Boolean dropBehind)` | 设置drop-behind |
| `void unbuffer()` | 减少缓冲 |
| `InputStream getWrappedStream()` | 获取包装的输入流 |
| `FileDescriptor getFileDescriptor()` | 获取文件描述符 |
| `boolean hasCapability(String capability)` | 检查能力 |
| `IOStatistics getIOStatistics()` | 获取IO统计 |

### 1.5 FSDataOutputStream
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `long getPos()` | 获取当前位置 |
| `void close()` | 关闭输出流 |
| `void hflush()` | 刷新客户端用户缓冲区 |
| `void hsync()` | 同步到磁盘设备 |
| `void setDropBehind(Boolean dropBehind)` | 设置drop-behind |
| `boolean hasCapability(String capability)` | 检查能力 |
| `AbortableResult abort()` | 中止流 |
| `IOStatistics getIOStatistics()` | 获取IO统计 |

### 1.6 DistributedFileSystem ⚠️
**稳定性标注**: `@InterfaceAudience.LimitedPrivate({"MapReduce", "HBase"})` 
**重要警告**: 此类为受限私有API，普通用户不应依赖，可能随时更改！

DistributedFileSystem是HDFS的具体实现，但标记为LimitedPrivate，仅限于MapReduce和HBase使用。普通应用程序应使用FileSystem接口。

---

## 2. MapReduce API (org.apache.hadoop.mapreduce)

### 2.1 Job 类 ⚠️
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Evolving`
**重要警告**: 标记为@Evolving，不保证向后兼容，后续版本可能更改！

| 方法签名 | 描述 |
|---------|------|
| `void setJobName(String name)` | 设置作业名称 |
| `String getJobName()` | 获取作业名称 |
| `void setInputFormatClass(Class<? extends InputFormat> cls)` | 设置输入格式类 |
| `void setOutputFormatClass(Class<? extends OutputFormat> cls)` | 设置输出格式类 |
| `void setMapperClass(Class<? extends Mapper> cls)` | 设置Mapper类 |
| `void setReducerClass(Class<? extends Reducer> cls)` | 设置Reducer类 |
| `void setPartitionerClass(Class<? extends Partitioner> cls)` | 设置Partitioner类 |
| `void setNumReduceTasks(int tasks)` | 设置Reducer任务数 |
| `void setCombinerClass(Class<? extends Reducer> cls)` | 设置Combiner类 |
| `void setJarByClass(Class<?> cls)` | 通过类设置Jar |
| `void setJar(String jar)` | 设置Jar路径 |
| `void addCacheFile(URI uri)` | 添加缓存文件 |
| `void addCacheArchive(URI uri)` | 添加缓存归档 |
| `void addFileToClassPath(Path file)` | 添加文件到classpath |
| `void addArchiveToClassPath(Path archive)` | 添加归档到classpath |
| `boolean waitForCompletion(boolean verbose)` | 等待作业完成 |
| `void submit()` | 提交作业 |
| `void killJob()` | 杀死作业 |
| `String getJobId()` | 获取作业ID |
| `JobID getID()` | 获取作业ID |
| `JobStatus getStatus()` | 获取作业状态 |
| `float getProgress()` | 获取进度 |
| `Configuration getConfiguration()` | 获取配置 |
| `Counters getCounters()` | 获取计数器 |
| `void setWorkingDirectory(Path dir)` | 设置工作目录 |
| `Path getWorkingDirectory()` | 获取工作目录 |
| `void setInputFormatClass(Class cls)` | 设置输入格式 |
| `void setOutputFormatClass(Class cls)` | 设置输出格式 |
| `void setMapOutputKeyClass(Class theClass)` | 设置Map输出键类 |
| `void setMapOutputValueClass(Class theClass)` | 设置Map输出值类 |
| `void setOutputKeyClass(Class theClass)` | 设置输出键类 |
| `void setOutputValueClass(Class theClass)` | 设置输出值类 |
| `void setSortComparatorClass(Class theClass)` | 设置排序比较器 |
| `void setGroupingComparatorClass(Class theClass)` | 设置分组比较器 |

### 2.2 Mapper 接口
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `void map(KEYIN key, VALUEIN value, Context context)` | Map方法，处理输入键值对 |
| `void setup(Context context)` | 初始化方法 |
| `void cleanup(Context context)` | 清理方法 |

### 2.3 Reducer 接口
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `void reduce(KEYIN key, Iterable<VALUEIN> values, Context context)` | Reduce方法，处理分组后的键值对 |
| `void setup(Context context)` | 初始化方法 |
| `void cleanup(Context context)` | 清理方法 |

### 2.4 Partitioner 接口
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `int getPartition(KEY key, VALUE value, int numReduceTasks)` | 获取分区号 |

### 2.5 InputFormat 接口
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `List<InputSplit> getSplits(JobContext context)` | 获取输入分片 |
| `RecordReader<K,V> createRecordReader(InputSplit split, TaskAttemptContext context)` | 创建记录读取器 |

### 2.6 OutputFormat 接口
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `RecordWriter<K,V> getRecordWriter(TaskAttemptContext context)` | 获取记录写入器 |
| `void checkOutputSpecs(JobContext context)` | 检查输出规范 |
| `OutputCommitter getOutputCommitter(TaskAttemptContext context)` | 获取输出提交器 |

### 2.7 Context 类 (Mapper.Context / Reducer.Context)
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Evolving`

| 方法签名 | 描述 |
|---------|------|
| `void write(KEY key, VALUE value)` | 写入键值对 |
| `Configuration getConfiguration()` | 获取配置 |
| `TaskAttemptID getTaskAttemptID()` | 获取任务尝试ID |
| `void setStatus(String status)` | 设置状态信息 |
| `String getStatus()` | 获取状态信息 |
| `float getProgress()` | 获取进度 |
| `void progress()` | 报告进度 |
| `Counter getCounter(Enum counterName)` | 获取计数器 |
| `Counter getCounter(String groupName, String counterName)` | 获取计数器 |
| `void incrementCounter(Enum key, long amount)` | 增加计数器(已废弃) |

---

## 3. YARN Client API (org.apache.hadoop.yarn.client.api)

### 3.1 YarnClient
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Evolving`

| 方法签名 | 描述 |
|---------|------|
| `static YarnClient createYarnClient()` | 创建YarnClient实例 |
| `void init(Configuration conf)` | 初始化 |
| `void start()` | 启动服务 |
| `void stop()` | 停止服务 |
| `ApplicationId submitApplication(ApplicationSubmissionContext appContext)` | 提交应用程序 |
| `void killApplication(ApplicationId applicationId)` | 杀死应用程序 |
| `ApplicationReport getApplicationReport(ApplicationId appId)` | 获取应用程序报告 |
| `List<ApplicationReport> getApplications()` | 获取所有应用程序列表 |
| `List<ApplicationReport> getApplications(Set<String> applicationTypes)` | 获取指定类型的应用程序 |
| `List<ApplicationReport> getApplications(EnumSet<YarnApplicationState> applicationStates)` | 获取指定状态的应用程序 |
| `List<NodeReport> getClusterNodes(EnumSet<NodeState> nodeStates)` | 获取集群节点报告 |
| `YarnClusterMetrics getYarnClusterMetrics()` | 获取集群指标 |
| `QueueInfo getQueueInfo(String queueName)` | 获取队列信息 |
| `List<QueueInfo> getAllQueues()` | 获取所有队列 |
| `List<QueueInfo> getRootQueueInfos()` | 获取根队列 |
| `List<QueueInfo> getChildQueueInfos(String parent)` | 获取子队列 |
| `Token<AMRMTokenIdentifier> getAMRMToken(ApplicationId appId)` | 获取AMRM令牌 |
| `Token getDelegationToken(Text renewer)` | 获取委托令牌 |
| `void cancelDelegationToken(Token dToken)` | 取消委托令牌 |
| `long renewDelegationToken(Token dToken)` | 更新委托令牌 |

### 3.2 ApplicationClientProtocol
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

YARN应用程序客户端协议接口，定义了客户端与ResourceManager之间的通信。

---

## 4. Configuration API (org.apache.hadoop.conf)

### 4.1 Configuration 类
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`

| 方法签名 | 描述 |
|---------|------|
| `String get(String name)` | 获取属性值 |
| `String get(String name, String defaultValue)` | 获取属性值，带默认值 |
| `int getInt(String name, int defaultValue)` | 获取int值 |
| `void setInt(String name, int value)` | 设置int值 |
| `long getLong(String name, long defaultValue)` | 获取long值 |
| `void setLong(String name, long value)` | 设置long值 |
| `boolean getBoolean(String name, boolean defaultValue)` | 获取boolean值 |
| `void setBoolean(String name, boolean value)` | 设置boolean值 |
| `float getFloat(String name, float defaultValue)` | 获取float值 |
| `void setFloat(String name, float value)` | 设置float值 |
| `double getDouble(String name, double defaultValue)` | 获取double值 |
| `Class<?> getClass(String name, Class<?> defaultValue)` | 获取Class |
| `void setClass(String name, Class<?> theClass, Class<?> xface)` | 设置Class |
| `Class<?>[] getClasses(String name, Class<?>... defaultValue)` | 获取Class数组 |
| `void addResource(String name)` | 添加资源(从classpath) |
| `void addResource(URL url)` | 添加资源(从URL) |
| `void addResource(Path file)` | 添加资源(从Path) |
| `void addResource(InputStream in)` | 添加资源(从流) |
| `void addResource(Configuration conf)` | 添加配置 |
| `Properties getProps()` | 获取所有属性 |
| `void writeXml(OutputStream out)` | 写XML格式 |
| `void writeXml(Writer out)` | 写XML格式 |
| `Map<String,String> getValByRegex(String regex)` | 正则匹配获取值 |
| `String getRaw(String name)` | 获取原始值(无变量展开) |
| `void set(String name, String value)` | 设置属性 |
| `void clear()` | 清除所有属性 |
| `Iterator<Map.Entry<String,String>> iterator()` | 迭代器 |
| `void setQuietMode(boolean quietmode)` | 设置安静模式 |
| `ClassLoader getClassLoader()` | 获取ClassLoader |
| `void setClassLoader(ClassLoader classLoader)` | 设置ClassLoader |
| `String[] getPropertySources(String name)` | 获取属性来源 |
| `Pattern getPattern(String name, Pattern defaultValue)` | 获取Pattern |
| `void setPattern(String name, Pattern pattern)` | 设置Pattern |
| `void setIfUnset(String name, String value)` | 如果未设置则设置 |
| `void setAllowNullValueProperties(boolean allowNullValueProperties)` | 允许null值 |
| `char[] getPassword(String name)` | 获取密码 |
| `Properties getAllPropertiesByTag(String tag)` | 标签获取属性 |
| `Properties getAllPropertiesByTags(List<String> tagList)` | 多标签获取属性 |

**静态方法**:
| 方法签名 | 描述 |
|---------|------|
| `static void addDefaultResource(String name)` | 添加默认资源 |
| `static void dumpConfiguration(Configuration config, Writer out)` | 导出配置 |
| `static void addDeprecation(String key, String newKey)` | 添加废弃警告 |
| `static void addDeprecation(String key, String newKey, String customMessage)` | 添加废弃警告，自定义消息 |

### 4.2 CommonConfigurationKeys ⚠️
**稳定性标注**: `@InterfaceAudience.Private`
**重要警告**: 此类为私有API，用户应使用CommonConfigurationKeysPublic！

用户应使用`CommonConfigurationKeysPublic`类来获取公共配置键。

---

## 5. Security API (org.apache.hadoop.security)

### 5.1 UserGroupInformation
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Evolving`

| 方法签名 | 描述 |
|---------|------|
| `static void loginUserFromKeytab(String user, String path)` | 从keytab登录用户 |
| `static UserGroupInformation loginUserFromKeytabAndReturnUGI(String user, String path)` | 从keytab登录并返回UGI |
| `static void loginUserFromSubject(Subject subject)` | 从Subject登录用户 |
| `static UserGroupInformation getCurrentUser()` | 获取当前用户 |
| `static UserGroupInformation getLoginUser()` | 获取登录用户 |
| `static boolean isSecurityEnabled()` | 是否启用安全模式 |
| `String getUserName()` | 获取用户名(完整principal名) |
| `String getShortUserName()` | 获取短用户名 |
| `String[] getGroupNames()` | 获取组名数组 |
| `List<String> getGroups()` | 获取组名列表 |
| `UserGroupInformation.AuthenticationMethod getAuthenticationMethod()` | 获取认证方法 |
| `boolean hasKerberosCredentials()` | 是否有Kerberos凭据 |
| `boolean isFromKeytab()` | 是否从keytab登录 |
| `T doAs(PrivilegedAction<T> action)` | 以此用户身份执行操作 |
| `T doAs(PrivilegedExceptionAction<T> action)` | 以此用户身份执行操作(可抛异常) |
| `UserGroupInformation getRealUser()` | 获取真实用户(代理用户场景) |
| `void checkTGTAndReloginFromKeytab()` | 检查并重新登录 |
| `void reloginFromKeytab()` | 从keytab重新登录 |
| `void reloginFromTicketCache()` | 从ticket cache重新登录 |
| `void logoutUserFromKeytab()` | 从keytab注销 |
| `Credentials getCredentials()` | 获取凭据 |
| `void addCredentials(Credentials credentials)` | 添加凭据 |
| `Collection<Token<?>> getTokens()` | 获取令牌集合 |
| `boolean addToken(Token<? extends TokenIdentifier> token)` | 添加令牌 |
| `boolean addToken(Text alias, Token<? extends TokenIdentifier> token)` | 添加命名令牌 |

**静态方法**:
| 方法签名 | 描述 |
|---------|------|
| `static UserGroupInformation createRemoteUser(String user)` | 创建远程用户 |
| `static UserGroupInformation createProxyUser(String user, UserGroupInformation realUser)` | 创建代理用户 |
| `static UserGroupInformation getUGIFromSubject(Subject subject)` | 从Subject获取UGI |
| `static UserGroupInformation getUGIFromTicketCache(String ticketCache, String user)` | 从ticket cache获取UGI |
| `static void setConfiguration(Configuration conf)` | 设置配置 |
| `static UserGroupInformation getBestUGI(String ticketCachePath, String user)` | 获取最佳UGI |

---

## 6. Common Utilities

### 6.1 IOUtils (org.apache.hadoop.io)
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Evolving`

| 方法签名 | 描述 |
|---------|------|
| `static void copyBytes(InputStream in, OutputStream out, int buffSize)` | 复制字节流 |
| `static void copyBytes(InputStream in, OutputStream out, int buffSize, boolean close)` | 复制字节流，可关闭 |
| `static void copyBytes(InputStream in, OutputStream out, Configuration conf)` | 复制字节流，使用配置 |
| `static void copyBytes(InputStream in, OutputStream out, Configuration conf, boolean close)` | 复制字节流，使用配置，可关闭 |
| `static void copyBytes(InputStream in, OutputStream out, long count, boolean close)` | 复制指定数量的字节 |
| `static void closeStream(Closeable stream)` | 关闭流(忽略异常) |
| `static void closeStreams(Closeable... streams)` | 关闭多个流(忽略异常) |
| `static void cleanup(Log log, Closeable... closeables)` | 清理(已废弃) |
| `static void cleanupWithLogger(Logger logger, Closeable... closeables)` | 带日志清理 |
| `static void closeSocket(Socket sock)` | 关闭Socket(忽略异常) |
| `static void readFully(InputStream in, byte[] buf, int off, int len)` | 完整读取 |
| `static void skipFully(InputStream in, long len)` | 完整跳过 |
| `static void fsync(File fileToSync)` | 同步到存储设备 |
| `static void fsync(FileChannel channel, boolean isDir)` | 同步文件通道 |
| `static byte[] readFullyToByteArray(DataInput in)` | 完整读取到byte数组 |
| `static void writeFully(WritableByteChannel bc, ByteBuffer buf)` | 完整写入ByteBuffer |
| `static void writeFully(FileChannel fc, ByteBuffer buf, long offset)` | 完整写入，指定偏移 |
| `static IOException wrapException(String path, String methodName, IOException exception)` | 包装异常 |
| `static List<String> listDirectory(File dir, FilenameFilter filter)` | 列出目录文件 |

### 6.2 FileUtil (org.apache.hadoop.fs)
**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Evolving`

| 方法签名 | 描述 |
|---------|------|
| `static boolean copy(File src, FileSystem dstFS, Path dst, boolean deleteSource, Configuration conf)` | 复制本地文件到FileSystem |
| `static boolean copy(FileSystem srcFS, Path src, File dst, boolean deleteSource, Configuration conf)` | 复制FileSystem文件到本地 |
| `static boolean copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource, Configuration conf)` | 复制FileSystem文件之间 |
| `static boolean copy(FileSystem srcFS, Path src, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf)` | 复制，可覆盖 |
| `static boolean copy(FileSystem srcFS, FileStatus srcStatus, FileSystem dstFS, Path dst, boolean deleteSource, boolean overwrite, Configuration conf)` | 复制文件/目录树 |
| `static boolean fullyDelete(File dir)` | 完全删除目录 |
| `static boolean fullyDelete(File dir, boolean tryGrantPermissions)` | 完全删除，尝试授权 |
| `static boolean fullyDeleteContents(File dir)` | 删除目录内容(不删除目录本身) |
| `static void fullyDeleteOnExit(File file)` | 退出时完全删除 |
| `static int chmod(String filename, String perm)` | 更改权限 |
| `static int chmod(String filename, String perm, boolean recursive)` | 更改权限，递归 |
| `static void setPermission(File f, FsPermission permission)` | 设置权限 |
| `static void setOwner(File file, String username, String groupname)` | 设置所有者 |
| `static boolean canRead(File f)` | 是否可读 |
| `static boolean canWrite(File f)` | 是否可写 |
| `static boolean canExecute(File f)` | 是否可执行 |
| `static void unZip(File inFile, File unzipDir)` | 解压ZIP |
| `static void unTar(File inFile, File untarDir)` | 解压TAR |
| `static int symLink(String target, String linkname)` | 创建符号链接 |
| `static String readLink(File f)` | 读取符号链接目标 |
| `static String makeShellPath(File file)` | 转换为shell路径 |
| `static String makeShellPath(String filename)` | 转换为shell路径 |
| `static String makeSecureShellPath(File file)` | 安全shell路径 |
| `static long getDU(File dir)` | 获取磁盘使用量 |
| `static Path[] stat2Paths(FileStatus[] stats)` | FileStatus转Path数组 |
| `static List<Path> getJarsInDirectory(String path)` | 获取目录中的jar文件 |
| `static FileSystem write(FileSystem fs, Path path, byte[] bytes)` | 写字节到文件 |
| `static FileSystem write(FileSystem fs, Path path, CharSequence charseq)` | 写文本到文件 |
| `static File createLocalTempFile(File basefile, String prefix, boolean isDeleteOnExit)` | 创建本地临时文件 |
| `static void replaceFile(File src, File target)` | 替换文件 |

---

## 7. 稳定性标注说明

### 7.1 InterfaceAudience 标注
| 标注 | 含义 | 使用建议 |
|-----|------|---------|
| `@Public` | 公共API，稳定对外 | 可以安全依赖 |
| `@LimitedPrivate` | 受限私有，仅限特定项目 | 仅限标注指定的项目使用 |
| `@Private` | 私有API，内部使用 | 不应依赖，可能随时更改 |

### 7.2 InterfaceStability 标注
| 标注 | 含义 | 使用建议 |
|-----|------|---------|
| `@Stable` | 稳定，保证向后兼容 | 可以安全依赖 |
| `@Evolving` | 演进中，可能变化 | 需关注版本变更 |
| `@Unstable` | 不稳定，可能移除 | 不应依赖 |

---

## 8. 重要稳定性争议标注

### 8.1 DistributedFileSystem
- **标注**: `@LimitedPrivate({"MapReduce", "HBase"})`
- **建议**: 普通用户不应直接依赖此类，应使用`FileSystem`接口
- **原因**: 这是HDFS的具体实现，Hadoop团队限制其使用范围

### 8.2 Job类
- **标注**: `@Evolving`
- **建议**: 不保证向后兼容，升级时需验证代码兼容性
- **原因**: MapReduce API仍在演进，可能引入新方法或改变现有方法

### 8.3 CommonConfigurationKeys
- **标注**: `@Private`
- **建议**: 使用`CommonConfigurationKeysPublic`替代
- **原因**: 内部配置键可能随时更改

### 8.4 UserGroupInformation
- **标注**: `@Evolving`
- **建议**: 关注版本更新时的API变更
- **原因**: 安全API可能随安全机制演进而变化

---

## 9. 方法数量统计

| 模块 | 类/接口 | 方法数量 |
|-----|---------|---------|
| HDFS Client | FileSystem | 175+ |
| HDFS Client | Path | 18 |
| HDFS Client | FileStatus | 31 |
| HDFS Client | FSDataInputStream | 22 |
| HDFS Client | FSDataOutputStream | 9 |
| MapReduce | Job | 40+ |
| MapReduce | Mapper | 3 |
| MapReduce | Reducer | 3 |
| MapReduce | Partitioner | 1 |
| MapReduce | InputFormat | 2 |
| MapReduce | OutputFormat | 3 |
| YARN Client | YarnClient | 18+ |
| Configuration | Configuration | 115+ |
| Security | UserGroupInformation | 54+ |
| Common Utilities | IOUtils | 19 |
| Common Utilities | FileUtil | 51+ |

**总计**: 约400+公共方法

---

## 10. 使用建议

1. **优先使用@Public/@Stable标注的API**
2. **避免使用@Private标注的类和方法**
3. **谨慎使用@Evolving标注的API，关注版本变更**
4. **使用接口而非具体实现类(如使用FileSystem而非DistributedFileSystem)**
5. **升级Hadoop版本前检查废弃方法和API变更**

---

*文档生成自 Apache Hadoop 3.3.5 官方API文档*