#!/bin/bash

# NoKube 安装脚本
# 支持多种安装方式

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# 打印带颜色的消息
print_info() {
    echo -e "${BLUE}[INFO]${NC} $1"
}

print_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查命令是否存在
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# 检查 Python 版本
check_python_version() {
    if command_exists python3; then
        python_version=$(python3 -c "import sys; print(f'{sys.version_info.major}.{sys.version_info.minor}')")
        required_version="3.8"
        
        if python3 -c "import sys; exit(0 if sys.version_info >= (3, 8) else 1)"; then
            print_success "Python $python_version 版本符合要求"
            return 0
        else
            print_error "Python 版本过低，需要 Python 3.8 或更高版本"
            return 1
        fi
    else
        print_error "未找到 Python3，请先安装 Python 3.8+"
        return 1
    fi
}

# 安装 uv
install_uv() {
    if command_exists uv; then
        print_info "uv 已安装"
        return 0
    fi
    
    print_info "正在安装 uv..."
    curl -LsSf https://astral.sh/uv/install.sh | sh
    
    # 重新加载 shell 配置
    if [ -f ~/.bashrc ]; then
        source ~/.bashrc
    fi
    
    if command_exists uv; then
        print_success "uv 安装成功"
        return 0
    else
        print_error "uv 安装失败"
        return 1
    fi
}

# 使用 uv 安装
install_with_uv() {
    print_info "使用 uv 安装 nokube..."
    
    if ! install_uv; then
        return 1
    fi
    
    # 使用 --system 参数进行全局安装
    uv pip install -e . --system
    print_success "使用 uv 安装完成"
}

# 使用 pip 安装
install_with_pip() {
    print_info "使用 pip 安装 nokube..."
    pip install -e .
    print_success "使用 pip 安装完成"
}

# 系统级安装
install_system() {
    print_info "系统级安装 nokube..."
    sudo pip install -e .
    print_success "系统级安装完成"
}

# 开发环境安装
install_dev() {
    print_info "安装开发环境..."
    
    if command_exists uv; then
        uv sync --extra dev
        print_success "使用 uv 安装开发环境完成"
    else
        pip install -e ".[dev]"
        print_success "使用 pip 安装开发环境完成"
    fi
}

# 验证安装
verify_installation() {
    print_info "验证安装..."
    
    if command_exists nokube; then
        print_success "nokube 安装成功！"
        echo
        echo "使用方法："
        echo "  nokube --help          # 查看帮助"
        echo "  nokube info            # 查看系统信息"
        echo "  nokube cluster list    # 列出集群"
        echo
        return 0
    else
        print_error "nokube 安装失败或命令不可用"
        return 1
    fi
}

# 显示帮助信息
show_help() {
    echo "NoKube 安装脚本"
    echo
    echo "用法: $0 [选项]"
    echo
    echo "选项:"
    echo "  -h, --help     显示此帮助信息"
    echo "  -u, --uv       使用 uv 安装（推荐）"
    echo "  -p, --pip      使用 pip 安装"
    echo "  -s, --system   系统级安装（需要 sudo）"
    echo "  -d, --dev      安装开发环境"
    echo "  -v, --verify   验证安装"
    echo
    echo "示例:"
    echo "  $0 -u          # 使用 uv 安装"
    echo "  $0 -p          # 使用 pip 安装"
    echo "  $0 -s          # 系统级安装"
    echo "  $0 -d          # 安装开发环境"
    echo "  $0 -v          # 验证安装"
}

# 主函数
main() {
    print_info "开始安装 NoKube..."
    
    # 检查 Python 版本
    if ! check_python_version; then
        exit 1
    fi
    
    # 检查是否在项目目录中
    if [ ! -f "pyproject.toml" ]; then
        print_error "请在 NoKube 项目根目录中运行此脚本"
        exit 1
    fi
    
    # 解析命令行参数
    case "${1:-}" in
        -h|--help)
            show_help
            exit 0
            ;;
        -u|--uv)
            install_with_uv
            ;;
        -p|--pip)
            install_with_pip
            ;;
        -s|--system)
            install_system
            ;;
        -d|--dev)
            install_dev
            ;;
        -v|--verify)
            verify_installation
            ;;
        "")
            # 默认使用 uv 安装
            print_info "未指定安装方式，默认使用 uv 安装"
            install_with_uv
            ;;
        *)
            print_error "未知选项: $1"
            show_help
            exit 1
            ;;
    esac
    
    # 验证安装
    if verify_installation; then
        print_success "安装完成！"
    else
        print_error "安装验证失败"
        exit 1
    fi
}

# 运行主函数
main "$@" 