#!/bin/bash
echo "================================"
echo "API覆盖检测测试脚本"
echo "================================"

# 测试1: 运行分析
echo ""
echo "测试1: 运行API覆盖度分析"
python3 api_coverage_plugin.py 2>&1 | tail -15

echo ""
echo "================================"
echo "测试2: 查看生成的报告"
echo "================================"

if [ -f "results/api_coverage_analysis.json" ]; then
    echo "✓ JSON报告已生成"
    echo "大小: $(ls -lh results/api_coverage_analysis.json | awk '{print $5}')"
fi

if [ -f "results/API_Coverage_Report.md" ]; then
    echo "✓ Markdown报告已生成"
    echo "大小: $(ls -lh results/API_Coverage_Report.md | awk '{print $5}')"
    echo ""
    echo "报告前20行:"
    head -20 results/API_Coverage_Report.md
fi

echo ""
echo "================================"
echo "测试完成"
echo "================================"
