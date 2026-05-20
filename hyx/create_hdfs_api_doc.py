#!/usr/bin/env python3
"""
从Hadoop源码提取HDFS Java API文档
"""
import re
import os
from pathlib import Path

HADOOP_SRC = "/home/h00517772/hadoop"
OUTPUT_FILE = "/home/h00517772/spark/hyx/hdfs_java_api_complete_list.md"

# HDFS核心类
HDFS_CLASSES = {
    "DistributedFileSystem": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DistributedFileSystem.java",
    "HdfsConfiguration": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/HdfsConfiguration.java",
    "DFSClient": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSClient.java",
    "DFSInputStream": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSInputStream.java",
    "DFSOutputStream": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/DFSOutputStream.java",
    "HdfsDataOutputStream": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsDataOutputStream.java",
    "HdfsDataInputStream": f"{HADOOP_SRC}/hadoop-hdfs-project/hadoop-hdfs-client/src/main/java/org/apache/hadoop/hdfs/client/HdfsDataInputStream.java",
}

def extract_methods(java_file):
    """从Java文件提取public方法"""
    methods = []
    
    if not os.path.exists(java_file):
        print(f"文件不存在: {java_file}")
        return methods
    
    with open(java_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    # 提取类注释和稳定性标注
    class_stability = "Stable"
    if '@InterfaceAudience.LimitedPrivate' in content:
        class_stability = "LimitedPrivate"
    elif '@InterfaceAudience.Private' in content.split('class')[0] if 'class' in content else '':
        class_stability = "Private"
    elif '@InterfaceStability.Evolving' in content:
        class_stability = "Evolving"
    elif '@InterfaceStability.Unstable' in content:
        class_stability = "Unstable"
    
    # 提取方法
    # 匹配模式: public [static] [返回类型] 方法名(参数) [throws 异常]
    pattern = r'(?:@\w+(?:\([^)]*\))?\s*)*public\s+(?:static\s+)?(?:final\s+)?(?:synchronized\s+)?(?:[\w<>?,\[\]\s]+)\s+(\w+)\s*\(([^)]*)\)(?:\s*throws\s+[\w,\s]+)?'
    
    matches = re.findall(pattern, content)
    
    for match in matches:
        method_name = match[0]
        params = match[1].strip()
        
        # 过滤掉一些不需要的方法
        if method_name in ['equals', 'hashCode', 'toString', 'getClass', 'notify', 'notifyAll', 'wait']:
            continue
        
        # 简化参数显示
        if params:
            params = simplify_params(params)
        
        methods.append({
            'name': method_name,
            'params': params,
            'stability': class_stability
        })
    
    return methods

def simplify_params(params_str):
    """简化参数类型显示"""
    # 移除参数名,只保留类型
    params = []
    for param in params_str.split(','):
        param = param.strip()
        if param:
            parts = param.split()
            if len(parts) >= 2:
                # 类型 + 参数名
                type_part = parts[-2] if parts[-1] == 'final' else parts[0]
                params.append(type_part)
            elif len(parts) == 1:
                params.append(parts[0])
    return ', '.join(params)

def extract_method_details(java_file):
    """提取方法的详细信息(返回类型、完整签名等)"""
    methods = []
    
    if not os.path.exists(java_file):
        return methods
    
    with open(java_file, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()
    
    lines = content.split('\n')
    
    i = 0
    while i < len(lines):
        line = lines[i]
        
        # 查找public方法声明
        if 'public ' in line and not line.strip().startswith('*') and not line.strip().startswith('//'):
            # 检查是否是方法(不是类/接口/枚举声明)
            if 'class ' in line or 'interface ' in line or 'enum ' in line or '@interface' in line:
                i += 1
                continue
            
            # 提取方法签名(可能跨多行)
            signature_lines = [line]
            j = i + 1
            while j < len(lines) and '{' not in lines[j-1] and not lines[j].strip().startswith('public'):
                signature_lines.append(lines[j])
                j += 1
            
            signature = ' '.join(signature_lines).strip()
            
            # 提取方法名和返回类型
            match = re.search(r'public\s+(?:static\s+)?(?:final\s+)?(?:synchronized\s+)?([\w<>?,\[\]\s]+)\s+(\w+)\s*\(', signature)
            if match:
                return_type = match.group(1).strip()
                method_name = match.group(2)
                
                # 过滤Object方法
                if method_name in ['equals', 'hashCode', 'toString', 'getClass', 'notify', 'notifyAll', 'wait']:
                    i += 1
                    continue
                
                # 提取参数
                param_match = re.search(r'\(([^)]*)\)', signature)
                params = param_match.group(1).strip() if param_match else ''
                
                # 检查稳定性
                stability = "Stable"
                # 向前查找注解
                for k in range(max(0, i-5), i):
                    if '@InterfaceStability.Evolving' in lines[k]:
                        stability = "Evolving"
                    elif '@InterfaceStability.Unstable' in lines[k]:
                        stability = "Unstable"
                    elif '@Deprecated' in lines[k]:
                        stability = "Deprecated"
                
                methods.append({
                    'return_type': return_type,
                    'name': method_name,
                    'params': simplify_params(params),
                    'signature': signature.split('{')[0].strip() if '{' in signature else signature,
                    'stability': stability
                })
        
        i += 1
    
    return methods

def generate_markdown():
    """生成Markdown文档"""
    md_content = []
    md_content.append("# HDFS Java API Complete List")
    md_content.append("")
    md_content.append("> 参考官方文档: https://hadoop.apache.org/docs/stable/api/")
    md_content.append("> 版本: Apache Hadoop 3.3.6")
    md_content.append("> 生成时间: 2026-05-18")
    md_content.append("")
    md_content.append("---")
    md_content.append("")
    
    total_methods = 0
    
    for class_name, java_file in HDFS_CLASSES.items():
        methods = extract_method_details(java_file)
        
        if not methods:
            continue
        
        md_content.append(f"## {class_name}")
        
        # 添加稳定性标注
        if class_name == "DistributedFileSystem":
            md_content.append("**稳定性标注**: `@InterfaceAudience.LimitedPrivate({\"MapReduce\", \"HBase\"})`")
            md_content.append("")
            md_content.append("**重要说明**: DistributedFileSystem是HDFS的具体实现类，标记为LimitedPrivate。普通应用程序应使用FileSystem接口，仅MapReduce和HBase等框架可直接使用此类。")
        else:
            md_content.append("**稳定性标注**: `@InterfaceAudience.Public` `@InterfaceStability.Stable`")
        
        md_content.append("")
        md_content.append("| 返回类型 | 方法名 | 参数 | 稳定性 |")
        md_content.append("|---------|--------|------|--------|")
        
        for method in methods:
            md_content.append(f"| `{method['return_type']}` | `{method['name']}` | `{method['params']}` | {method['stability']} |")
            total_methods += 1
        
        md_content.append("")
        md_content.append("---")
        md_content.append("")
    
    # 统计信息
    md_content.append("## API统计")
    md_content.append("")
    md_content.append(f"- **总方法数**: {total_methods}")
    md_content.append(f"- **核心类数**: {len(HDFS_CLASSES)}")
    md_content.append(f"- **覆盖范围**: HDFS客户端核心API")
    md_content.append("")
    md_content.append("---")
    md_content.append("")
    md_content.append("**文档生成完成**")
    
    # 写入文件
    with open(OUTPUT_FILE, 'w', encoding='utf-8') as f:
        f.write('\n'.join(md_content))
    
    print(f"文档已生成: {OUTPUT_FILE}")
    print(f"总方法数: {total_methods}")

if __name__ == "__main__":
    generate_markdown()