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
    # 默认从环境变量 CONFIG_PATH 读取，未设置则回退到 repos.yaml
    default_config = os.environ.get('CONFIG_PATH', 'repos.yaml')
    parser.add_argument('--config', default=default_config, help='配置文件路径')
    parser.add_argument('--daemon', action='store_true', help='以守护进程模式运行')
    parser.add_argument('--log-level', default='INFO', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR'], help='日志级别')
    
    args = parser.parse_args()
    
    # 设置日志级别
    os.environ['LOG_LEVEL'] = args.log_level

    # 检查配置文件
    config_file = args.config
    console.print("🚀 启动 Git 仓库监控服务", style="blue")
    console.print(f"使用配置: {config_file}", style="cyan")
    if not os.path.exists(config_file):
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
                with open(config_file, 'w', encoding='utf-8') as f:
                    f.write(data)
                console.print(f"📝 已从环境变量写入配置: {config_file}", style="green")
            except Exception as e:
                console.print(f"❌ 写入内联配置失败: {e}", style="red")
                return 1
        else:
            console.print(f"❌ 配置文件不存在: {config_file}", style="red")
            console.print("请确保配置文件存在，或通过环境变量 REPOS_YAML/REPOS_YAML_B64 提供配置，或使用 --config 指定路径", style="yellow")
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