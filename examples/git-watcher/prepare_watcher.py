#!/usr/bin/env python3
"""
Git Watcher 环境准备脚本
处理容器启动时的环境配置、依赖检查等任务
"""

import os
import sys
import subprocess
import shutil
import tempfile
from pathlib import Path

# 全局变量，稍后初始化
console = None


def install_basic_python_deps():
    """安装基础Python依赖（在导入rich之前）"""
    print("📦 安装基础 Python 依赖...")
    
    basic_deps = ['rich', 'pyyaml', 'requests']
    
    for dep in basic_deps:
        try:
            print(f"安装 {dep}...")
            result = subprocess.run(
                [sys.executable, '-m', 'pip', 'install', '--no-cache-dir', dep],
                capture_output=True,
                text=True,
                check=True
            )
            print(f"✅ {dep} 安装成功")
        except subprocess.CalledProcessError as e:
            print(f"❌ {dep} 安装失败: {e}")
            print(f"错误输出: {e.stderr}")
            # 继续尝试安装其他包
            continue


def init_rich_console():
    """初始化rich console"""
    global console
    try:
        from rich.console import Console
        console = Console()
        return True
    except ImportError:
        print("❌ 无法导入 rich，请检查安装")
        return False


def run_command(cmd, cwd=None, shell=True, check=True, show_progress=True, capture_output=False):
    """执行系统命令
    
    Args:
        cmd: 要执行的命令
        cwd: 工作目录
        shell: 是否使用shell
        check: 是否检查返回码
        show_progress: 是否显示实时输出进度（默认True），如果为True则不捕获输出
        capture_output: 是否捕获输出（默认False），为True时会忽略show_progress
    
    Returns:
        如果capture_output=True，返回subprocess.CompletedProcess对象
        如果show_progress=True，返回None
    """
    try:
        if console:
            console.print(f"[cyan]执行: {cmd}[/cyan]")
        else:
            print(f"执行: {cmd}")
        
        if capture_output:
            # 捕获输出模式：适用于需要处理输出结果的场景
            result = subprocess.run(
                cmd, 
                shell=shell, 
                cwd=cwd, 
                check=check,
                capture_output=True, 
                text=True
            )
            if result.stdout:
                if console:
                    console.print(f"[green]{result.stdout.strip()}[/green]")
                else:
                    print(result.stdout.strip())
            return result
        else:
            # 实时输出模式：适用于长时间运行的命令，如安装、编译等
            if show_progress:
                if console:
                    console.print(f"[yellow]⏳ 实时输出模式...[/yellow]")
                else:
                    print("⏳ 实时输出模式...")
            
            result = subprocess.run(
                cmd, 
                shell=shell, 
                cwd=cwd, 
                check=check,
                # 不捕获输出，直接显示到终端
                capture_output=False,
                text=True
            )
            
            if show_progress:
                if console:
                    console.print(f"[green]✅ 命令执行完成[/green]")
                else:
                    print("✅ 命令执行完成")
            
            return None  # 实时输出模式不返回结果
            
    except subprocess.CalledProcessError as e:
        if console:
            console.print(f"[red]❌ 命令执行失败: {cmd}[/red]")
            console.print(f"[red]错误码: {e.returncode}[/red]")
        else:
            print(f"❌ 命令执行失败: {cmd}")
            print(f"错误码: {e.returncode}")
        if check:
            raise
        return None


def safe_print(message, style=None):
    """安全打印函数，自动选择console或print"""
    if console:
        if style:
            console.print(message, style=style)
        else:
            console.print(message)
    else:
        print(message)


def install_system_deps():
    """安装系统依赖"""
    safe_print("📦 安装系统依赖...", style="blue")
    
    # 检查是否已安装
    deps_to_install = []
    
    if not shutil.which('git'):
        deps_to_install.append('git')
    
    if not os.path.exists('/etc/ssl/certs'):
        deps_to_install.append('ca-certificates')
        
    if not shutil.which('curl'):
        deps_to_install.append('curl')
    
    if deps_to_install:
        safe_print(f"需要安装: {', '.join(deps_to_install)}")
        
        # 更新包列表
        run_command("apt-get update")
        
        # 安装依赖
        install_cmd = f"apt-get install -y --no-install-recommends {' '.join(deps_to_install)}"
        run_command(install_cmd)
        
        # 更新证书（如果安装了ca-certificates）
        if 'ca-certificates' in deps_to_install:
            run_command("update-ca-certificates")
        
        # 清理
        run_command("rm -rf /var/lib/apt/lists/*")
        
        safe_print("✅ 系统依赖安装完成", style="green")
    else:
        safe_print("✅ 系统依赖已满足", style="green")


def setup_nokube_repo():
    """设置nokube代码仓库"""
    safe_print("📥 设置 nokube 代码仓库...", style="blue")
    
    nokube_dir = Path("/opt/nokube")
    repo_url = "https://github.com/AI-Infra-Team/nokube.git"
    
    if nokube_dir.exists():
        safe_print("🔄 更新现有仓库...")
        try:
            # 检查网络连接和代理设置
            safe_print("🌐 检查网络连接...")
            proxy_vars = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]
            active_proxies = [f"{k}={os.environ[k]}" for k in proxy_vars if k in os.environ]
            if active_proxies:
                safe_print(f"🔗 使用代理: {', '.join(active_proxies)}")
            else:
                safe_print("🔗 未设置代理")
            
            run_command("git pull", cwd=nokube_dir, check=False)
            safe_print("✅ 仓库更新完成", style="green")
        except Exception as e:
            safe_print(f"⚠️ 仓库更新失败，继续使用现有代码: {e}", style="yellow")
    else:
        safe_print("📦 克隆新仓库...")
        safe_print("⚠️ 代码仓库应该在容器启动脚本中克隆", style="yellow")
        safe_print("如果看到此消息，说明启动脚本可能有问题", style="yellow")
        # 这里不再尝试克隆，因为应该在启动脚本中完成
        if not nokube_dir.exists():
            raise RuntimeError("nokube代码目录不存在，请检查容器启动脚本")


def install_python_deps():
    """安装Python依赖"""
    safe_print("🐍 安装 Python 依赖...", style="blue")
    
    # git-watcher 需要的第三方库
    required_packages = [
        "gitpython",      # Git 操作
        "requests",       # HTTP 请求
        "pyyaml",        # YAML 解析
        "rich",          # 美化输出
    ]
    
    python_executable = sys.executable
    
    # 逐个安装依赖包
    for package in required_packages:
        try:
            safe_print(f"📦 安装 {package}...", style="cyan")
            run_command(f"{python_executable} -m pip install --no-cache-dir {package}")
            safe_print(f"✅ {package} 安装完成", style="green")
        except Exception as e:
            safe_print(f"❌ {package} 安装失败: {e}", style="red")
            raise
    
    safe_print("✅ 所有 Python 依赖安装完成", style="green")


def setup_directories():
    """创建必要的目录"""
    safe_print("📁 创建必要目录...", style="blue")
    
    directories = [
        "/app/data",
        "/app/repos", 
        "/app/config",
        "/tmp/git-watcher"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        safe_print(f"✅ 创建目录: {directory}")


def setup_config_from_env():
    """从环境变量设置配置文件"""
    safe_print("⚙️ 配置环境变量处理...", style="blue")
    
    config_path = os.environ.get('CONFIG_PATH', '/app/config/repos.yaml')
    safe_print(f"配置文件路径: {config_path}")
    
    # 确保配置目录存在
    config_dir = Path(config_path).parent
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # 如果配置文件不存在，尝试从环境变量创建
    if not Path(config_path).exists():
        safe_print("📝 配置文件不存在，尝试从环境变量创建...")
        
        # 尝试从REPOS_YAML环境变量获取
        repos_yaml = os.environ.get('REPOS_YAML')
        if repos_yaml:
            # 处理GitHub token替换
            github_token = os.environ.get('GITHUB_TOKEN', '')
            if github_token and '${GITHUB_TOKEN}' in repos_yaml:
                repos_yaml = repos_yaml.replace('${GITHUB_TOKEN}', github_token)
                safe_print("🔑 已替换GitHub token")
            
            # 写入配置文件
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(repos_yaml)
            safe_print(f"✅ 已从环境变量创建配置文件: {config_path}", style="green")
        else:
            safe_print("❌ 未找到 REPOS_YAML 环境变量", style="red")
            return False
    else:
        safe_print("✅ 配置文件已存在", style="green")
    
    # 显示配置文件内容预览（脱敏）
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # 脱敏处理
        preview_content = content
        if 'token:' in preview_content:
            import re
            preview_content = re.sub(r'token:\s*"[^"]*"', 'token: "***"', preview_content)
            preview_content = re.sub(r'token:\s*[^\s\n]+', 'token: ***', preview_content)
        
        # 只显示前20行
        lines = preview_content.split('\n')[:20]
        preview = '\n'.join(lines)
        if len(content.split('\n')) > 20:
            preview += '\n...(更多内容已省略)'
        
        safe_print("\n📋 配置文件内容预览:")
        if console:
            from rich.panel import Panel
            console.print(Panel(preview, title="repos.yaml", border_style="cyan"))
        else:
            print("=" * 50)
            print("repos.yaml 预览:")
            print("=" * 50)
            print(preview)
            print("=" * 50)
        
    except Exception as e:
        safe_print(f"⚠️ 无法读取配置文件预览: {e}", style="yellow")
    
    return True


def check_health():
    """健康检查"""
    safe_print("🏥 执行健康检查...", style="blue")
    
    checks = []
    
    # 检查Git
    if shutil.which('git'):
        checks.append(("Git", "✅"))
    else:
        checks.append(("Git", "❌"))
    
    # 检查Python
    try:
        result = run_command(f"{sys.executable} --version", check=False, capture_output=True)
        if result.returncode == 0:
            checks.append(("Python", f"✅ {result.stdout.strip()}"))
        else:
            checks.append(("Python", "❌"))
    except:
        checks.append(("Python", "❌"))
    
    # 检查配置文件
    config_path = os.environ.get('CONFIG_PATH', '/app/config/repos.yaml')
    if Path(config_path).exists():
        checks.append(("配置文件", f"✅ {config_path}"))
    else:
        checks.append(("配置文件", f"❌ {config_path}"))
    
    # 检查目录
    data_dir = Path("/app/data")
    if data_dir.exists():
        checks.append(("数据目录", f"✅ {data_dir}"))
    else:
        checks.append(("数据目录", f"❌ {data_dir}"))
    
    # 显示检查结果
    safe_print("\n🔍 健康检查结果:")
    for check_name, status in checks:
        safe_print(f"  {check_name}: {status}")
    
    # 检查是否有失败项
    failed_checks = [name for name, status in checks if "❌" in status]
    if failed_checks:
        safe_print(f"\n❌ 失败的检查项: {', '.join(failed_checks)}", style="red")
        return False
    else:
        safe_print("\n✅ 所有检查通过", style="green")
        return True


def main():
    """主函数"""
    print("🚀 Git Watcher 环境准备开始")
    
    try:
        # 第一步：安装基础Python依赖（包括rich）
        print("第1步：安装基础Python依赖...")
        install_basic_python_deps()
        
        # 第二步：初始化rich console
        print("第2步：初始化显示组件...")
        if not init_rich_console():
            print("❌ 无法初始化显示组件，继续使用基础显示")
        
        # 现在可以使用rich的功能了
        safe_print("🚀 Git Watcher 环境准备开始", style="bold blue")
        
        # 使用进度条（如果rich可用）
        if console:
            from rich.progress import Progress, SpinnerColumn, TextColumn
            
            with Progress(
                SpinnerColumn(),
                TextColumn("[progress.description]{task.description}"),
                console=console,
            ) as progress:
                
                # 系统依赖
                task = progress.add_task("安装系统依赖...", total=None)
                install_system_deps()
                progress.update(task, description="✅ 系统依赖")
                
                # 设置目录
                task = progress.add_task("创建目录...", total=None)
                setup_directories()
                progress.update(task, description="✅ 目录创建")
                
                # 代码仓库
                task = progress.add_task("设置代码仓库...", total=None)
                setup_nokube_repo()
                progress.update(task, description="✅ 代码仓库")
                
                # Python依赖
                task = progress.add_task("安装Python依赖...", total=None)
                install_python_deps()
                progress.update(task, description="✅ Python依赖")
                
                # 配置文件
                task = progress.add_task("配置环境...", total=None)
                config_ok = setup_config_from_env()
                if not config_ok:
                    progress.update(task, description="❌ 配置失败")
                    return 1
                progress.update(task, description="✅ 环境配置")
        else:
            # 没有rich，使用简单模式
            print("第3步：安装系统依赖...")
            install_system_deps()
            
            print("第4步：创建目录...")
            setup_directories()
            
            print("第5步：设置代码仓库...")
            setup_nokube_repo()
            
            print("第6步：安装Python依赖...")
            install_python_deps()
            
            print("第7步：配置环境...")
            config_ok = setup_config_from_env()
            if not config_ok:
                print("❌ 配置失败")
                return 1
        
        # 健康检查
        if not check_health():
            return 1
        
        safe_print("\n🎉 环境准备完成，可以启动监控器了！", style="bold green")
        return 0
        
    except KeyboardInterrupt:
        safe_print("\n🛑 用户中断", style="yellow")
        return 1
    except Exception as e:
        safe_print(f"\n❌ 环境准备失败: {e}", style="red")
        import traceback
        if console:
            console.print(traceback.format_exc(), style="dim red")
        else:
            print(traceback.format_exc())
        return 1


if __name__ == '__main__':
    sys.exit(main())
