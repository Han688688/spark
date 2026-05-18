#!/usr/bin/env python3
"""
从Hadoop源码提取HDFS Java API文档 - 优化版本
"""
import re
import os

HADOOP_SRC = "/home/h00517772/hadoop"
OUTPUT_FILE = "/home/h00517772/spark/hyx/hdfs_java_api_complete_list.md"

# HDFS核心类及其描述
HDFS_CLASSES = {
    "DistributedFileSystem": {
        "file": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java",
        "description": "HDFS的具体实现类,提供HDFS特有的操作如快照、纠删码、缓存管理等",
        "stability": "@InterfaceAudience.LimitedPrivate",
        "note": "标记为LimitedPrivate,仅限于MapReduce和HBase使用,普通应用应使用FileSystem接口"
    },
    "HdfsConfiguration": {
        "file": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/HdfsConfiguration.java",
        "description": "HDFS配置类,扩展Configuration添加HDFS特定配置",
        "stability": "@InterfaceAudience.Public",
        "note": "Stable"
    },
    "HdfsDataOutputStream": {
        "file": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsDataOutputStream.java",
        "description": "HDFS数据输出流,扩展FSDataOutputStream",
        "stability": "@InterfaceAudience.Public",
        "note": "Stable"
    },
    "HdfsDataInputStream": {
        "file": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsDataInputStream.java",
        "description": "HDFS数据输入流,扩展FSDataInputStream",
        "stability": "@InterfaceAudience.Public",
        "note": "Stable"
    },
    "DFSClient": {
        "file": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSClient.java",
        "description": "HDFS客户端核心类,直接与NameNode和DataNode通信",
        "stability": "@InterfaceAudience.Private",
        "note": "Private,内部类不应直接使用"
    },
}

# 需要过滤的方法名
SKIP_METHODS = [
    'doCall', 'next', 'getPathName', 'checkPath', 'fixRelativePart',
    'makeQualified', 'getDFS', 'getStatistics', 'incrementReadOps',
    'incrementWriteOps', 'equals', 'hashCode', 'toString', 'getClass',
    'notify', 'notifyAll', 'wait', 'finalize', 'clone',
    'initDFSClient', 'getStorageStatistics'
]

def extract_public_methods(java_file):
    """从Java文件提取public方法及其注释"""
    methods = []
    
    if not os.path.exists(java_file):
        print(f"警告: 文件不存在 {java_file}")
        return methods
    
    with open(java_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # 使用正则表达式直接提取所有public方法
    # 匹配: public [static] [final] 返回类型 方法名(参数)
    pattern = r'public\s+(?:static\s+)?(?:final\s+)?(?:synchronized\s+)?([a-zA-Z0-9_<>,\[\]\s\.]+?)\s+(\w+)\s*\(([^)]*)\)'
    
    # 找到所有匹配
    matches = re.findall(pattern, content)
    
    # 向前查找每个方法的注释和注解
    lines = content.split('\n')
    
    for match in matches:
        return_type = match[0].strip()
        method_name = match[1]
        params = match[2].strip()
        
        # 过滤不需要的方法
        if method_name in SKIP_METHODS:
            continue
        
        # 过滤Object类的方法
        if method_name in ['equals', 'hashCode', 'toString', 'getClass']:
            continue
        
        # 简化参数显示
        params_display = simplify_params(params)
        
        # 在源码中查找该方法,检查是否有@Deprecated
        deprecated = False
        method_pattern = f'public.*{method_name}\\s*\\('
        method_match = re.search(method_pattern, content)
        if method_match:
            start_pos = method_match.start()
            # 检查前面的字符中是否有@Deprecated
            before_text = content[max(0, start_pos-200):start_pos]
            deprecated = '@Deprecated' in before_text
        
        # 获取描述
        description = get_method_description(method_name)
        
        methods.append({
            'name': method_name,
            'return_type': return_type,
            'params': params_display,
            'description': description,
            'deprecated': deprecated
        })
    
    return methods

def simplify_params(params_str):
    """简化参数显示,只保留类型"""
    if not params_str:
        return ""
    
    params = []
    # 分割参数
    param_parts = params_str.split(',')
    
    for param in param_parts:
        param = param.strip()
        if not param:
            continue
        
        # 提取类型(去掉final和参数名)
        parts = param.split()
        if len(parts) >= 2:
            # 过滤final关键字
            type_parts = [p for p in parts if p not in ['final', ' transient', 'volatile']]
            if type_parts:
                param_type = type_parts[0]
                # 清理泛型
                param_type = re.sub(r'<[^>]+>', '<>', param_type)
                params.append(param_type)
    
    return ', '.join(params)

def get_method_description(method_name):
    """根据方法名推断描述"""
    descriptions = {
        'getScheme': '获取文件系统scheme',
        'getUri': '获取文件系统URI',
        'initialize': '初始化文件系统',
        'getWorkingDirectory': '获取工作目录',
        'setWorkingDirectory': '设置工作目录',
        'getHomeDirectory': '获取用户主目录',
        'open': '打开文件读取',
        'create': '创建文件',
        'append': '追加文件',
        'delete': '删除文件或目录',
        'rename': '重命名文件',
        'exists': '检查文件是否存在',
        'listStatus': '列出文件状态',
        'mkdirs': '创建目录',
        'getFileStatus': '获取文件状态',
        'getFileBlockLocations': '获取文件块位置',
        'setReplication': '设置文件副本数',
        'setPermission': '设置文件权限',
        'setOwner': '设置文件所有者',
        'setTimes': '设置文件时间',
        'concat': '连接文件',
        'truncate': '截断文件',
        'recoverLease': '恢复文件租约',
        'createSnapshot': '创建快照',
        'deleteSnapshot': '删除快照',
        'renameSnapshot': '重命名快照',
        'setStoragePolicy': '设置存储策略',
        'getStoragePolicy': '获取存储策略',
        'unsetStoragePolicy': '取消存储策略',
        'getErasureCodingPolicy': '获取纠删码策略',
        'setErasureCodingPolicy': '设置纠删码策略',
        'addCacheDirective': '添加缓存指令',
        'removeCacheDirective': '移除缓存指令',
        'listCacheDirectives': '列出缓存指令',
        'addCachePool': '添加缓存池',
        'modifyCachePool': '修改缓存池',
        'removeCachePool': '移除缓存池',
        'listCachePools': '列出缓存池',
        'createEncryptionZone': '创建加密区',
        'getEncryptionZone': '获取加密区信息',
        'listEncryptionZones': '列出加密区',
        'getEZForPath': '获取路径的加密区',
        'setXAttr': '设置扩展属性',
        'getXAttr': '获取扩展属性',
        'removeXAttr': '移除扩展属性',
        'listXAttrs': '列出扩展属性',
        'getXAttrs': '批量获取扩展属性',
        'setAcl': '设置ACL',
        'getAclStatus': '获取ACL状态',
        'modifyAclEntries': '修改ACL条目',
        'removeAclEntries': '移除ACL条目',
        'removeDefaultAcl': '移除默认ACL',
        'removeAcl': '移除所有ACL',
        'getContentSummary': '获取内容摘要',
        'setQuota': '设置配额',
        'setStoragePolicy': '设置存储策略',
        'getStoragePolicies': '获取所有存储策略',
        'allowSnapshot': '允许快照',
        'disallowSnapshot': '禁用快照',
        'getHedgedReadMetrics': '获取 Hedged Read 指标',
        'getDelegationToken': '获取委托令牌',
        ' renewDelegationToken': '续期委托令牌',
        'cancelDelegationToken': '取消委托令牌',
        'getInotifyEventStream': '获取inotify事件流',
        'getLocatedBlocks': '获取定位块信息',
        'getAllStoragePolicies': '获取所有存储策略',
        'getDatanodeReport': '获取数据节点报告',
        'getDatanodeStorageReport': '获取数据节点存储报告',
        'setSafeMode': '设置安全模式',
        'saveNamespace': '保存命名空间',
        'rollEdits': '滚动编辑日志',
        'restoreFailedStorage': '恢复失败存储',
        'refreshNodes': '刷新节点列表',
        'finalizeUpgrade': '完成升级',
        'rollingUpgrade': '滚动升级操作',
        'metaSave': '保存元数据信息',
        'refreshUserToGroupsMappings': '刷新用户组映射',
        'refreshSuperUserGroupsConfiguration': '刷新超级用户组配置',
        'refreshCallQueue': '刷新调用队列',
        'getBlockLocations': '获取块位置',
        'getLocatedFileStatus': '获取定位文件状态',
        'listLocatedStatus': '列出定位状态',
        'listFiles': '列出文件',
        'listCorruptFileBlocks': '列出损坏文件块',
        'getQuotaUsage': '获取配额使用情况',
        'setErasureCodingPolicy': '设置纠删码策略',
        'unsetErasureCodingPolicy': '取消纠删码策略',
        'getErasureCodingPolicy': '获取纠删码策略',
        'getErasureCodingPolicies': '获取所有纠删码策略',
        'addErasureCodingPolicies': '添加纠删码策略',
        'removeErasureCodingPolicy': '移除纠删码策略',
    }
    
    return descriptions.get(method_name, '')

def generate_markdown():
    """生成完整的Markdown文档"""
    sections = []
    sections.append("# HDFS Java API Complete List")
    sections.append("")
    sections.append("> 参考官方文档: https://hadoop.apache.org/docs/stable/api/")
    sections.append("> 版本: Apache Hadoop 3.3.6")
    sections.append("> 生成时间: 2026-05-18")
    sections.append("")
    sections.append("---")
    sections.append("")
    sections.append("## 目录")
    sections.append("")
    
    # 生成目录
    for idx, class_name in enumerate(HDFS_CLASSES.keys(), 1):
        sections.append(f"{idx}. [{class_name}](#{class_name.lower()})")
    
    sections.append("")
    sections.append("---")
    sections.append("")
    
    total_methods = 0
    total_deprecated = 0
    
    for class_name, class_info in HDFS_CLASSES.items():
        methods = extract_public_methods(class_info['file'])
        
        if not methods:
            continue
        
        sections.append(f"## {class_name}")
        sections.append("")
        sections.append(f"**包路径**: `{class_info['file'].split('/')[-1].replace('.java', '')}`")
        sections.append("")
        sections.append(f"**稳定性标注**: `{class_info['stability']}`")
        sections.append("")
        
        if class_info['note']:
            sections.append(f"**说明**: {class_info['note']}")
            sections.append("")
        
        sections.append(f"**功能**: {class_info['description']}")
        sections.append("")
        sections.append("| 序号 | 方法名 | 返回类型 | 参数 | 描述 | 状态 |")
        sections.append("|------|--------|----------|------|------|------|")
        
        deprecated_count = 0
        for idx, method in enumerate(methods, 1):
            status = "Deprecated" if method['deprecated'] else "Stable"
            desc = method['description'] if method['description'] else ""
            sections.append(
                f"| {idx} | `{method['name']}` | `{method['return_type']}` | `{method['params']}` | {desc} | {status} |"
            )
            total_methods += 1
            if method['deprecated']:
                deprecated_count += 1
                total_deprecated += 1
        
        sections.append("")
        sections.append(f"**方法统计**: {len(methods)}个方法 ({deprecated_count}个已废弃)")
        sections.append("")
        sections.append("---")
        sections.append("")
    
    # 添加统计信息
    sections.append("## 总体统计")
    sections.append("")
    sections.append(f"- **总类数**: {len(HDFS_CLASSES)}")
    sections.append(f"- **总方法数**: {total_methods}")
    sections.append(f"- **已废弃方法**: {total_deprecated}")
    sections.append(f"- **稳定方法**: {total_methods - total_deprecated}")
    sections.append("")
    if total_methods > 0:
        sections.append("### API稳定性分布")
        sections.append("")
        sections.append("| 状态 | 数量 | 占比 |")
        sections.append("|------|------|------|")
        sections.append(f"| Stable | {total_methods - total_deprecated} | {(total_methods - total_deprecated)/total_methods*100:.1f}% |")
        sections.append(f"| Deprecated | {total_deprecated} | {total_deprecated/total_methods*100:.1f}% |")
        sections.append("")
    sections.append("---")
    sections.append("")
    
    # 添加使用示例
    sections.append("## 常用示例")
    sections.append("")
    sections.append("### 1. 创建文件并写入数据")
    sections.append("```java")
    sections.append("Configuration conf = new Configuration();")
    sections.append("FileSystem fs = FileSystem.get(conf);")
    sections.append("Path path = new Path(\"/user/test/file.txt\");")
    sections.append("")
    sections.append("FSDataOutputStream out = fs.create(path);")
    sections.append("out.writeUTF(\"Hello HDFS\");")
    sections.append("out.close();")
    sections.append("```")
    sections.append("")
    sections.append("### 2. 读取文件内容")
    sections.append("```java")
    sections.append("FileSystem fs = FileSystem.get(conf);")
    sections.append("Path path = new Path(\"/user/test/file.txt\");")
    sections.append("")
    sections.append("FSDataInputStream in = fs.open(path);")
    sections.append("String content = in.readUTF();")
    sections.append("in.close();")
    sections.append("```")
    sections.append("")
    sections.append("### 3. 设置副本数")
    sections.append("```java")
    sections.append("FileSystem fs = FileSystem.get(conf);")
    sections.append("fs.setReplication(new Path(\"/user/test/file.txt\"), 3);")
    sections.append("```")
    sections.append("")
    sections.append("### 4. 创建快照")
    sections.append("```java")
    sections.append("DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);")
    sections.append("Path snapshotPath = dfs.createSnapshot(new Path(\"/user/test\"), \"snapshot1\");")
    sections.append("```")
    sections.append("")
    sections.append("---")
    sections.append("")
    sections.append("**文档生成完成**")
    
    # 写入文件
    output = '\n'.join(sections)
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write(output)
    
    print(f"✅ 文档已生成: {OUTPUT_FILE}")
    print(f"✅ 总方法数: {total_methods}")
    print(f"✅ 已废弃方法: {total_deprecated}")
    print(f"✅ 稳定方法: {total_methods - total_deprecated}")

if __name__ == "__main__":
    generate_markdown()