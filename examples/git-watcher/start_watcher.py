#!/usr/bin/env python3
"""
启动 Git 仓库监控服务
增强版，整合了环境准备和监控启动功能
"""

import sys
import os
import argparse
import signal
import time
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))

from git_watcher import GitWatcher
from rich.console import Console
from rich.panel import Panel
from rich.live import Live
from rich.table import Table

console = Console()

# 全局变量用于优雅关闭
watcher_instance = None


def signal_handler(signum, frame):
    """信号处理器，用于优雅关闭"""
    global watcher_instance
    console.print(f"\n🛑 收到信号 {signum}，正在优雅关闭...", style="yellow")
    if watcher_instance:
        watcher_instance.stop()
    sys.exit(0)


def run_prepare_if_needed():
    """如果需要，运行环境准备"""
    # 检查是否在容器环境中且需要准备
    if os.environ.get('CONTAINER_ENV') == 'true' or not Path('/opt/nokube').exists():
        console.print("🔧 检测到需要环境准备，运行 prepare_watcher.py...", style="blue")
        try:
            # 导入并运行prepare脚本
            current_dir = Path(__file__).parent
            prepare_script = current_dir / 'prepare_watcher.py'
            
            if prepare_script.exists():
                # 直接导入并运行
                import importlib.util
                spec = importlib.util.spec_from_file_location("prepare_watcher", prepare_script)
                prepare_module = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(prepare_module)
                
                result = prepare_module.main()
                if result != 0:
                    console.print("❌ 环境准备失败", style="red")
                    return False
                console.print("✅ 环境准备完成", style="green")
            else:
                console.print("⚠️ prepare_watcher.py 不存在，跳过环境准备", style="yellow")
        except Exception as e:
            console.print(f"❌ 环境准备失败: {e}", style="red")
            return False
    
    return True


def setup_config_file(config_file):
    """设置配置文件"""
    if not os.path.exists(config_file):
        console.print(f"📝 配置文件不存在: {config_file}，尝试从环境变量创建...", style="yellow")
        
        # 尝试从环境变量内联创建配置
        inline_b64 = os.environ.get('REPOS_YAML_B64')
        inline_text = os.environ.get('REPOS_YAML') or os.environ.get('REPOS_YAML_INLINE')
        
        if inline_b64 or inline_text:
            try:
                os.makedirs(os.path.dirname(config_file) or '.', exist_ok=True)
                if inline_b64:
                    import base64
                    data = base64.b64decode(inline_b64.encode('utf-8')).decode('utf-8')
                else:
                    data = inline_text
                
                # 处理GitHub token替换
                github_token = os.environ.get('GITHUB_TOKEN', '')
                if github_token and '${GITHUB_TOKEN}' in data:
                    data = data.replace('${GITHUB_TOKEN}', github_token)
                    console.print("🔑 已替换配置中的GitHub token", style="cyan")
                
                with open(config_file, 'w', encoding='utf-8') as f:
                    f.write(data)
                console.print(f"📝 已从环境变量写入配置: {config_file}", style="green")
                return True
            except Exception as e:
                console.print(f"❌ 写入内联配置失败: {e}", style="red")
                return False
        else:
            console.print("❌ 未找到环境变量 REPOS_YAML 或 REPOS_YAML_B64", style="red")
            console.print("请确保配置文件存在，或通过环境变量提供配置", style="yellow")
            return False
    
    return True


def show_startup_banner():
    """显示启动横幅"""
    banner = """
🎯 Git Repository Watcher
━━━━━━━━━━━━━━━━━━━━━━━━━━
自动监控Git仓库变更并部署配置
"""
    console.print(Panel(banner, style="bold blue", border_style="blue"))


def create_status_table(watcher, running_time=0):
    """创建状态表格"""
    table = Table(title=f"监控状态 (运行时间: {running_time}s)")
    table.add_column("仓库", style="cyan")
    table.add_column("分支", style="green") 
    table.add_column("路径", style="yellow")
    table.add_column("间隔(s)", style="blue")
    table.add_column("最后检查", style="magenta")
    table.add_column("状态", style="bold")
    
    for repo in watcher.config['repositories']:
        repo_name = repo['name']
        branch = repo.get('branch', 'main')
        path = repo.get('path', repo.get('config_path', '-'))
        interval = repo.get('check_interval', 300)
        
        last_check = watcher.state.get(repo_name, {}).get('last_check', '从未检查')
        if last_check != '从未检查':
            # 简化时间显示
            try:
                from datetime import datetime
                dt = datetime.fromisoformat(last_check.replace('Z', '+00:00'))
                last_check = dt.strftime('%H:%M:%S')
            except:
                pass
        
        status = "🟢 监控中" if watcher.running else "🔴 已停止"
        
        table.add_row(repo_name, branch, path, str(interval), last_check, status)
    
    return table


def main():
    global watcher_instance
    
    parser = argparse.ArgumentParser(description='启动 Git 仓库监控服务')
    default_config = os.environ.get('CONFIG_PATH', 'repos.yaml')
    parser.add_argument('--config', default=default_config, help='配置文件路径')
    parser.add_argument('--daemon', action='store_true', help='以守护进程模式运行')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='日志级别')
    parser.add_argument('--prepare', action='store_true', help='强制运行环境准备')
    parser.add_argument('--no-prepare', action='store_true', help='跳过环境准备')
    parser.add_argument('--status-update', type=int, default=30, help='状态更新间隔(秒)')
    
    args = parser.parse_args()
    
    # 注册信号处理器
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 设置日志级别
    os.environ['LOG_LEVEL'] = args.log_level
    
    # 显示启动横幅
    show_startup_banner()
    
    # 环境准备
    if args.prepare or (not args.no_prepare and not run_prepare_if_needed()):
        if args.prepare:
            if not run_prepare_if_needed():
                return 1
        else:
            return 1
    
    # 配置文件处理
    config_file = args.config
    console.print(f"📋 使用配置文件: {config_file}", style="cyan")
    
    if not setup_config_file(config_file):
        return 1
    
    try:
        # 创建监控器
        console.print("🔧 初始化监控器...", style="blue")
        watcher = GitWatcher(config_file)
        watcher_instance = watcher
        
        # 显示初始状态
        console.print("📊 显示初始状态...")
        watcher.show_status()
        
        if args.daemon:
            console.print("🔄 以守护进程模式运行", style="blue")
            try:
                import daemon
                with daemon.DaemonContext():
                    watcher.start()
            except ImportError:
                console.print("⚠️ daemon库未安装，使用前台模式", style="yellow")
                run_with_live_status(watcher, args.status_update)
        else:
            # 前台运行（带实时状态显示）
            run_with_live_status(watcher, args.status_update)
        
        return 0
        
    except KeyboardInterrupt:
        console.print("\n🛑 收到停止信号", style="yellow")
        return 0
    except Exception as e:
        console.print(f"❌ 启动失败: {e}", style="red")
        import traceback
        console.print(traceback.format_exc(), style="dim red")
        return 1


def run_with_live_status(watcher, status_update_interval):
    """运行监控器并显示实时状态"""
    global watcher_instance
    watcher_instance = watcher
    
    console.print("🚀 启动监控器...", style="green")
    
    # 在后台线程启动监控器
    import threading
    monitor_thread = threading.Thread(target=watcher.start, daemon=True)
    monitor_thread.start()
    
    # 等待启动
    time.sleep(2)
    
    start_time = time.time()
    last_status_update = 0
    
    try:
        with Live(console=console, refresh_per_second=0.5) as live:
            while watcher.running:
                current_time = time.time()
                running_time = int(current_time - start_time)
                
                # 定期更新状态显示
                if current_time - last_status_update >= status_update_interval:
                    table = create_status_table(watcher, running_time)
                    live.update(table)
                    last_status_update = current_time
                
                time.sleep(1)
                
    except KeyboardInterrupt:
        console.print("\n🛑 收到中断信号，正在关闭监控器...", style="yellow")
        watcher.stop()
        monitor_thread.join(timeout=10)
        console.print("✅ 监控器已优雅关闭", style="green")


if __name__ == '__main__':
    sys.exit(main()) 