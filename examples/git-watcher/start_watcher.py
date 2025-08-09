#!/usr/bin/env python3
"""
启动 Git 仓库监控服务
"""

import sys
import os
import argparse
from pathlib import Path

# 添加项目根目录到 Python 路径
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root / 'src'))

from git_watcher import GitWatcher
from rich.console import Console

console = Console()


def main():
    parser = argparse.ArgumentParser(description='启动 Git 仓库监控服务')
    parser.add_argument('--config', default='repos.yaml', help='配置文件路径')
    parser.add_argument('--daemon', action='store_true', help='以守护进程模式运行')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='日志级别')
    
    args = parser.parse_args()
    
    # 设置日志级别
    os.environ['LOG_LEVEL'] = args.log_level
    
    console.print("🚀 启动 Git 仓库监控服务", style="blue")
    
    # 检查配置文件
    config_file = args.config
    if not os.path.exists(config_file):
        console.print(f"❌ 配置文件不存在: {config_file}", style="red")
        console.print("请确保配置文件存在或使用 --config 参数指定", style="yellow")
        return 1
    
    try:
        # 创建监控器
        watcher = GitWatcher(config_file)
        
        # 显示初始状态
        watcher.show_status()
        
        if args.daemon:
            console.print("🔄 以守护进程模式运行", style="blue")
            # 这里可以实现守护进程逻辑
            import daemon
            with daemon.DaemonContext():
                watcher.start()
        else:
            # 直接运行
            watcher.start()
        
        return 0
        
    except KeyboardInterrupt:
        console.print("\n🛑 收到停止信号", style="yellow")
        return 0
    except Exception as e:
        console.print(f"❌ 启动失败: {e}", style="red")
        return 1


if __name__ == '__main__':
    sys.exit(main()) 