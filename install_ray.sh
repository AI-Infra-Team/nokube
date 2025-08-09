#!/bin/bash

# Ray 安装脚本
# 用于安装 Ray 分布式计算框架

set -e

echo "🚀 开始安装 Ray..."

# 检查 Python 版本
python_version=$(python3 --version 2>&1 | cut -d' ' -f2 | cut -d'.' -f1,2)
echo "📊 Python 版本: $python_version"

# 检查 pip 是否可用
if command -v pip3 &> /dev/null; then
    echo "✅ pip3 可用"
    PIP_CMD="pip3"
elif command -v pip &> /dev/null; then
    echo "✅ pip 可用"
    PIP_CMD="pip"
else
    echo "❌ pip 不可用，请先安装 pip"
    exit 1
fi

# 检查 uv 是否可用
if command -v uv &> /dev/null; then
    echo "✅ uv 可用，使用 uv 安装"
    UV_AVAILABLE=true
else
    echo "⚠️  uv 不可用，使用 pip 安装"
    UV_AVAILABLE=false
fi

# 安装 Ray
echo "📦 安装 Ray..."

if [ "$UV_AVAILABLE" = true ]; then
    # 使用 uv 安装
    echo "使用 uv 安装 Ray..."
    uv add ray[serve]
else
    # 使用 pip 安装
    echo "使用 pip 安装 Ray..."
    $PIP_CMD install "ray[serve]>=2.7.0"
fi

# 验证安装
echo "🔍 验证 Ray 安装..."

# 检查 Python 包
python3 -c "import ray; print(f'✅ Ray Python 包已安装，版本: {ray.__version__}')"

# 检查 CLI
if command -v ray &> /dev/null; then
    ray_version=$(ray --version 2>&1)
    echo "✅ Ray CLI 已安装: $ray_version"
else
    echo "❌ Ray CLI 未找到"
    echo "请尝试重新安装 Ray"
    exit 1
fi

echo "✅ Ray 安装完成！"
echo ""
echo "🎉 现在您可以使用以下命令："
echo "  nokube ray start          # 启动 Ray 集群"
echo "  nokube ray stop           # 停止 Ray 集群"
echo "  nokube ray status         # 查看 Ray 集群状态"
echo ""
echo "📊 Ray Dashboard 将在启动后可用："
echo "  http://localhost:8265" 