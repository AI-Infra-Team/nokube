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
from rich.console import Console
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

console = Console()


def run_command(cmd, cwd=None, shell=True, check=True):
    """执行系统命令"""
    try:
        console.print(f"[cyan]执行: {cmd}[/cyan]")
        result = subprocess.run(
            cmd, 
            shell=shell, 
            cwd=cwd, 
            check=check,
            capture_output=True, 
            text=True
        )
        if result.stdout:
            console.print(f"[green]{result.stdout.strip()}[/green]")
        return result
    except subprocess.CalledProcessError as e:
        console.print(f"[red]命令执行失败: {cmd}[/red]")
        console.print(f"[red]错误: {e.stderr}[/red]")
        if check:
            raise
        return e


def install_system_deps():
    """安装系统依赖"""
    console.print("📦 安装系统依赖...", style="blue")
    
    # 检查是否已安装
    deps_to_install = []
    
    if not shutil.which('git'):
        deps_to_install.append('git')
    
    if not os.path.exists('/etc/ssl/certs'):
        deps_to_install.append('ca-certificates')
        
    if not shutil.which('curl'):
        deps_to_install.append('curl')
    
    if deps_to_install:
        console.print(f"需要安装: {', '.join(deps_to_install)}")
        
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
        
        console.print("✅ 系统依赖安装完成", style="green")
    else:
        console.print("✅ 系统依赖已满足", style="green")


def setup_nokube_repo():
    """设置nokube代码仓库"""
    console.print("📥 设置 nokube 代码仓库...", style="blue")
    
    nokube_dir = Path("/opt/nokube")
    repo_url = "https://github.com/AI-Infra-Team/nokube.git"
    
    if nokube_dir.exists():
        console.print("🔄 更新现有仓库...")
        try:
            # 检查网络连接和代理设置
            console.print("🌐 检查网络连接...")
            proxy_vars = ["http_proxy", "https_proxy", "HTTP_PROXY", "HTTPS_PROXY"]
            active_proxies = [f"{k}={os.environ[k]}" for k in proxy_vars if k in os.environ]
            if active_proxies:
                console.print(f"🔗 使用代理: {', '.join(active_proxies)}")
            else:
                console.print("🔗 未设置代理")
            
            run_command("git pull", cwd=nokube_dir, check=False)
            console.print("✅ 仓库更新完成", style="green")
        except Exception as e:
            console.print(f"⚠️ 仓库更新失败，继续使用现有代码: {e}", style="yellow")
    else:
        console.print("📦 克隆新仓库...")
        console.print("⚠️ 代码仓库应该在容器启动脚本中克隆", style="yellow")
        console.print("如果看到此消息，说明启动脚本可能有问题", style="yellow")
        # 这里不再尝试克隆，因为应该在启动脚本中完成
        if not nokube_dir.exists():
            raise RuntimeError("nokube代码目录不存在，请检查容器启动脚本")


def install_python_deps():
    """安装Python依赖"""
    console.print("🐍 安装 Python 依赖...", style="blue")
    
    nokube_dir = Path("/opt/nokube")
    if not nokube_dir.exists():
        raise RuntimeError("nokube目录不存在，请先设置代码仓库")
    
    # 安装项目依赖
    try:
        run_command("pip install --no-cache-dir -e .", cwd=nokube_dir)
        console.print("✅ Python 依赖安装完成", style="green")
    except Exception as e:
        console.print(f"❌ Python 依赖安装失败: {e}", style="red")
        raise


def setup_directories():
    """创建必要的目录"""
    console.print("📁 创建必要目录...", style="blue")
    
    directories = [
        "/app/data",
        "/app/repos", 
        "/app/config",
        "/tmp/git-watcher"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        console.print(f"✅ 创建目录: {directory}")


def setup_config_from_env():
    """从环境变量设置配置文件"""
    console.print("⚙️ 配置环境变量处理...", style="blue")
    
    config_path = os.environ.get('CONFIG_PATH', '/app/config/repos.yaml')
    console.print(f"配置文件路径: {config_path}")
    
    # 确保配置目录存在
    config_dir = Path(config_path).parent
    config_dir.mkdir(parents=True, exist_ok=True)
    
    # 如果配置文件不存在，尝试从环境变量创建
    if not Path(config_path).exists():
        console.print("📝 配置文件不存在，尝试从环境变量创建...")
        
        # 尝试从REPOS_YAML环境变量获取
        repos_yaml = os.environ.get('REPOS_YAML')
        if repos_yaml:
            # 处理GitHub token替换
            github_token = os.environ.get('GITHUB_TOKEN', '')
            if github_token and '${GITHUB_TOKEN}' in repos_yaml:
                repos_yaml = repos_yaml.replace('${GITHUB_TOKEN}', github_token)
                console.print("🔑 已替换GitHub token")
            
            # 写入配置文件
            with open(config_path, 'w', encoding='utf-8') as f:
                f.write(repos_yaml)
            console.print(f"✅ 已从环境变量创建配置文件: {config_path}", style="green")
        else:
            console.print("❌ 未找到 REPOS_YAML 环境变量", style="red")
            return False
    else:
        console.print("✅ 配置文件已存在", style="green")
    
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
        
        console.print("\n📋 配置文件内容预览:")
        console.print(Panel(preview, title="repos.yaml", border_style="cyan"))
        
    except Exception as e:
        console.print(f"⚠️ 无法读取配置文件预览: {e}", style="yellow")
    
    return True


def check_health():
    """健康检查"""
    console.print("🏥 执行健康检查...", style="blue")
    
    checks = []
    
    # 检查Git
    if shutil.which('git'):
        checks.append(("Git", "✅"))
    else:
        checks.append(("Git", "❌"))
    
    # 检查Python
    try:
        result = run_command(f"{sys.executable} --version", check=False)
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
    console.print("\n🔍 健康检查结果:")
    for check_name, status in checks:
        console.print(f"  {check_name}: {status}")
    
    # 检查是否有失败项
    failed_checks = [name for name, status in checks if "❌" in status]
    if failed_checks:
        console.print(f"\n❌ 失败的检查项: {', '.join(failed_checks)}", style="red")
        return False
    else:
        console.print("\n✅ 所有检查通过", style="green")
        return True


def main():
    """主函数"""
    console.print("🚀 Git Watcher 环境准备开始", style="bold blue")
    
    try:
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
        
        # 健康检查
        if not check_health():
            return 1
        
        console.print("\n🎉 环境准备完成，可以启动监控器了！", style="bold green")
        return 0
        
    except KeyboardInterrupt:
        console.print("\n🛑 用户中断", style="yellow")
        return 1
    except Exception as e:
        console.print(f"\n❌ 环境准备失败: {e}", style="red")
        import traceback
        console.print(traceback.format_exc(), style="dim red")
        return 1


if __name__ == '__main__':
    sys.exit(main())
