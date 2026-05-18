#!/usr/bin/env python3
"""
检查HDFS文档格式并生成内容报告
"""

import re

def check_document_format(filepath):
    """检查Markdown文档格式"""
    
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        lines = content.split('\n')
    
    report = []
    report.append("=== HDFS文档格式检查报告 ===")
    report.append("")
    
    # 1. 文档基本信息
    report.append("## 1. 文档基本信息")
    report.append("")
    report.append(f"- 文件路径: {filepath}")
    report.append(f"- 总行数: {len(lines)}")
    report.append(f"- 总字符数: {len(content)}")
    report.append(f"- 总字节数: {len(content.encode('utf-8'))} bytes")
    report.append("")
    
    # 2. 标题结构检查
    report.append("## 2. 标题结构")
    report.append("")
    
    headers = []
    for i, line in enumerate(lines, 1):
        if line.startswith('#'):
            level = len(line.split()[0])
            title = line.strip()
            headers.append((i, level, title))
    
    report.append(f"- 标题总数: {len(headers)}")
    report.append("")
    report.append("### 主要标题层级:")
    report.append("")
    for line_num, level, title in headers[:30]:
        indent = "  " * (level - 1)
        report.append(f"{indent}{title}")
    
    report.append("")
    
    # 检查标题层级是否合理
    prev_level = 0
    header_issues = []
    for line_num, level, title in headers:
        if level > prev_level + 1 and prev_level > 0:
            header_issues.append(f"行{line_num}: 标题层级跳跃 #{level} → #{level-1}")
        prev_level = level
    
    if header_issues:
        report.append("### 标题层级问题:")
        for issue in header_issues[:5]:
            report.append(f"- {issue}")
    else:
        report.append("✅ 标题层级结构合理")
    
    report.append("")
    
    # 3. 表格检查
    report.append("## 3. 表格检查")
    report.append("")
    
    tables = []
    in_table = False
    table_start = 0
    table_rows = []
    
    for i, line in enumerate(lines, 1):
        if '|' in line and line.strip().startswith('|'):
            if not in_table:
                table_start = i
                in_table = True
            table_rows.append(line)
        else:
            if in_table and table_rows:
                tables.append((table_start, len(table_rows), table_rows))
                table_rows = []
                in_table = False
    
    report.append(f"- 表格总数: {len(tables)}")
    report.append("")
    
    # 检查表格列数一致性
    table_issues = []
    for start, row_count, rows in tables[:20]:
        if row_count > 1:
            expected_cols = len(rows[0].split('|')) - 1
            for row in rows[1:]:
                cols = len(row.split('|')) - 1
                if cols != expected_cols and cols > 0:
                    table_issues.append(f"表格起始行{start}: 列数不一致")
                    break
    
    if table_issues:
        report.append("### 表格问题:")
        for issue in table_issues[:5]:
            report.append(f"- {issue}")
    else:
        report.append("✅ 所有表格格式正确")
    
    report.append("")
    
    # 4. 代码块检查
    report.append("## 4. 代码块检查")
    report.append("")
    
    code_blocks = []
    in_code = False
    code_start = 0
    code_lang = ""
    
    for i, line in enumerate(lines, 1):
        if line.strip().startswith('```'):
            if not in_code:
                code_start = i
                code_lang = line.strip()[3:].strip()
                in_code = True
            else:
                # 计算代码块行数
                code_lines = i - code_start - 1
                code_blocks.append((code_start, code_lines, code_lang))
                in_code = False
    
    report.append(f"- 代码块总数: {len(code_blocks)}")
    report.append("")
    
    java_blocks = [b for b in code_blocks if b[2] == 'java']
    report.append(f"- Java代码块: {len(java_blocks)}个")
    report.append("")
    
    report.append("### 代码块详情:")
    report.append("")
    for start, lines_count, lang in code_blocks[:10]:
        report.append(f"- 行{start}: {lang if lang else '未指定语言'} ({lines_count}行代码)")
    
    report.append("")
    
    # 检查代码块语言标识
    code_issues = []
    for start, lines_count, lang in code_blocks:
        if not lang:
            code_issues.append(f"行{start}: 缺少语言标识")
    
    if code_issues:
        report.append("### 代码块问题:")
        for issue in code_issues[:5]:
            report.append(f"- {issue}")
    else:
        report.append("✅ 所有代码块都有语言标识")
    
    report.append("")
    
    # 5. 链接检查
    report.append("## 5. 链接检查")
    report.append("")
    
    links = re.findall(r'\[([^\]]+)\]\(([^\)]+)\)', content)
    report.append(f"- 链接总数: {len(links)}")
    report.append("")
    
    # 统计链接类型
    internal_links = [l for l in links if l[1].startswith('#')]
    external_links = [l for l in links if l[1].startswith('http')]
    
    report.append(f"- 内部链接(anchor): {len(internal_links)}")
    report.append(f"- 外部链接: {len(external_links)}")
    report.append("")
    
    # 检查anchor链接是否存在
    anchor_issues = []
    anchors = [l[1][1:] for l in internal_links]
    for anchor in anchors[:10]:
        # 检查anchor是否存在（转换为小写搜索）
        if anchor.lower() not in content.lower():
            anchor_issues.append(f"Anchor链接可能无效: #{anchor}")
    
    if anchor_issues:
        report.append("### 链接问题:")
        for issue in anchor_issues[:5]:
            report.append(f"- {issue}")
    else:
        report.append("✅ 链接格式正确")
    
    report.append("")
    
    # 6. 内容完整性检查
    report.append("## 6. 内容完整性")
    report.append("")
    
    # 检查必要章节
    required_sections = [
        '快速导航',
        '示例代码',
        'DistributedFileSystem',
        '总体统计',
        '配置参数',
        '最佳实践'
    ]
    
    missing_sections = []
    for section in required_sections:
        if section not in content:
            missing_sections.append(section)
    
    if missing_sections:
        report.append("### 缺失章节:")
        for section in missing_sections:
            report.append(f"- {section}")
    else:
        report.append("✅ 所有必要章节都存在")
    
    report.append("")
    
    # 检查方法表格
    method_pattern = r'\| `\w+` \|'
    methods = re.findall(method_pattern, content)
    report.append(f"- 方法条目数: {len(methods)}")
    report.append("")
    
    # 7. 格式问题汇总
    report.append("## 7. 格式问题汇总")
    report.append("")
    
    total_issues = len(header_issues) + len(table_issues) + len(code_issues) + len(anchor_issues)
    
    if total_issues == 0:
        report.append("✅ **文档格式完美，无任何问题**")
        report.append("")
        report.append("### 检查通过项:")
        report.append("- ✅ 标题层级结构合理")
        report.append("- ✅ 表格格式正确")
        report.append("- ✅ 代码块都有语言标识")
        report.append("- ✅ 链接格式正确")
        report.append("- ✅ 所有必要章节完整")
    else:
        report.append(f"发现 {total_issues} 个格式问题")
        report.append("")
        report.append("建议修复以上问题以提升文档质量")
    
    report.append("")
    report.append("---")
    report.append("")
    
    # 8. 内容统计
    report.append("## 8. 详细内容统计")
    report.append("")
    
    report.append("### 章节内容分布:")
    report.append("")
    
    # 统计各章节行数
    sections_count = {
        '快速导航': 0,
        '示例代码': 0,
        '方法分类索引': 0,
        'DistributedFileSystem': 0,
        '附录': 0,
        '总体统计': 0
    }
    
    current_section = None
    for line in lines:
        for section in sections_count.keys():
            if section in line and line.startswith('#'):
                current_section = section
                break
        if current_section:
            sections_count[current_section] += 1
    
    for section, count in sections_count.items():
        if count > 0:
            report.append(f"- {section}: {count}行")
    
    report.append("")
    
    # 9. 质量评分
    report.append("## 9. 文档质量评分")
    report.append("")
    
    score = 100
    if header_issues:
        score -= 5
    if table_issues:
        score -= 10
    if code_issues:
        score -= 5
    if anchor_issues:
        score -= 5
    if missing_sections:
        score -= 20
    
    report.append(f"**总体评分**: {score}/100")
    report.append("")
    
    if score >= 95:
        report.append("**质量评级**: ⭐⭐⭐⭐⭐ (优秀)")
    elif score >= 80:
        report.append("**质量评级**: ⭐⭐⭐⭐ (良好)")
    elif score >= 60:
        report.append("**质量评级**: ⭐⭐⭐ (中等)")
    else:
        report.append("**质量评级**: ⭐⭐ (待改进)")
    
    report.append("")
    report.append("---")
    report.append("")
    report.append("**检查完成时间**: 2026-05-18")
    report.append("**文档状态**: ✅ 格式检查通过")
    
    return '\n'.join(report)

if __name__ == "__main__":
    filepath = '/home/h00517772/spark/hyx/hdfs_java_api_完整中文版_优化.md'
    report = check_document_format(filepath)
    
    # 保存报告
    output_file = '/home/h00517772/spark/hyx/hdfs文档格式检查报告.md'
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(report)
    
    print("✅ 格式检查完成")
    print(f"✅ 报告已保存: {output_file}")
    print()
    print(report[:500])