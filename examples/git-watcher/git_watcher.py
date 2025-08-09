#!/usr/bin/env python3
"""
Git 仓库监控器
监控 Git 仓库中的 YAML 配置变更并自动部署
"""

import yaml
import time
import threading
import subprocess
import hashlib
import json
import os
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Any
from rich.console import Console
from rich.table import Table
from rich.panel import Panel

console = Console()


class GitWatcher:
    """Git 仓库监控器"""
    
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_config()
        self.state_file = self.config['global']['storage']['path']
        self.state = self._load_state()
        self.running = False
        self.threads = []
        
        # 确保数据目录存在
        os.makedirs(os.path.dirname(self.state_file), exist_ok=True)
    
    def _load_config(self) -> Dict:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except Exception as e:
            console.print(f"❌ 加载配置文件失败: {e}", style="red")
            raise
    
    def _load_state(self) -> Dict:
        """加载状态文件"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception:
                pass
        return {}
    
    def _save_state(self):
        """保存状态文件"""
        try:
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, indent=2)
        except Exception as e:
            console.print(f"⚠️  保存状态失败: {e}", style="yellow")
    
    def _get_repo_hash(self, repo_name: str, config_path: str) -> str:
        """获取仓库配置文件的哈希值"""
        try:
            # 克隆或更新仓库
            repo_dir = f"./repos/{repo_name}"
            repo_config = self._get_repo_config(repo_name)
            
            if not os.path.exists(repo_dir):
                self._clone_repo(repo_config)
            else:
                self._pull_repo(repo_config)
            
            # 计算配置文件哈希
            config_file = os.path.join(repo_dir, config_path)
            if os.path.exists(config_file):
                with open(config_file, 'rb') as f:
                    content = f.read()
                return hashlib.md5(content).hexdigest()
            
        except Exception as e:
            console.print(f"❌ 获取仓库哈希失败 {repo_name}: {e}", style="red")
        
        return ""
    
    def _get_repo_config(self, repo_name: str) -> Dict:
        """获取仓库配置"""
        for repo in self.config['repositories']:
            if repo['name'] == repo_name:
                return repo
        raise ValueError(f"仓库 {repo_name} 未找到")
    
    def _clone_repo(self, repo_config: Dict):
        """克隆仓库"""
        repo_name = repo_config['name']
        repo_url = repo_config['url']
        branch = repo_config.get('branch', 'main')
        repo_dir = f"./repos/{repo_name}"
        
        console.print(f"📥 克隆仓库: {repo_name}", style="blue")
        
        # 设置认证
        if repo_config.get('auth', {}).get('type') == 'ssh':
            # SSH 认证
            key_path = repo_config['auth']['key_path']
            env = os.environ.copy()
            env['GIT_SSH_COMMAND'] = f'ssh -i {key_path}'
        else:
            # HTTPS 认证
            token = os.environ.get('GITHUB_TOKEN')
            if token:
                repo_url = repo_url.replace('https://', f'https://{token}@')
            env = os.environ.copy()
        
        # 克隆仓库
        result = subprocess.run([
            'git', 'clone', '-b', branch, repo_url, repo_dir
        ], env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"克隆仓库失败: {result.stderr}")
    
    def _pull_repo(self, repo_config: Dict):
        """更新仓库"""
        repo_name = repo_config['name']
        repo_dir = f"./repos/{repo_name}"
        
        console.print(f"🔄 更新仓库: {repo_name}", style="blue")
        
        # 设置认证
        if repo_config.get('auth', {}).get('type') == 'ssh':
            key_path = repo_config['auth']['key_path']
            env = os.environ.copy()
            env['GIT_SSH_COMMAND'] = f'ssh -i {key_path}'
        else:
            env = os.environ.copy()
        
        # 拉取更新
        result = subprocess.run([
            'git', 'pull'
        ], cwd=repo_dir, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"更新仓库失败: {result.stderr}")
    
    def _check_repo_changes(self, repo_name: str) -> bool:
        """检查仓库是否有变更"""
        repo_config = self._get_repo_config(repo_name)
        config_path = repo_config['config_path']
        
        # 获取当前哈希
        current_hash = self._get_repo_hash(repo_name, config_path)
        
        # 获取上次哈希
        last_hash = self.state.get(repo_name, {}).get('last_hash', '')
        
        if current_hash != last_hash:
            console.print(f"🔄 检测到变更: {repo_name}", style="yellow")
            
            # 更新状态
            if repo_name not in self.state:
                self.state[repo_name] = {}
            
            self.state[repo_name]['last_hash'] = current_hash
            self.state[repo_name]['last_check'] = datetime.now().isoformat()
            self._save_state()
            
            return True
        
        return False
    
    def deploy_config_change(self, repo_name: str, config_data: Dict[str, Any], target_cluster: str):
        """部署配置变更"""
        try:
            console.print(f"🚀 部署配置变更: {repo_name} -> {target_cluster}", style="blue")
            
            # 根据目标集群选择部署方式
            if target_cluster == "ray-prod":
                deployment_target = 'ray'
            elif target_cluster == "ray-staging":
                deployment_target = 'ray'
            else:
                # 默认使用 Ray
                deployment_target = 'ray'
            
            # 创建部署器
            deployer = AutoDeployer()
            
            # 部署到目标集群
            success = deployer.deploy_to_target(repo_name, config_data, deployment_target)
            
            if success:
                console.print(f"✅ 配置变更部署成功: {repo_name}", style="green")
            else:
                console.print(f"❌ 配置变更部署失败: {repo_name}", style="red")
                
        except Exception as e:
            console.print(f"❌ 部署配置变更失败: {e}", style="red")
    
    def _send_notification(self, message: str):
        """发送通知"""
        try:
            webhook_config = self.config['global']['notifications']['webhook']
            if webhook_config.get('enabled'):
                # 这里可以集成 Slack、钉钉等通知
                console.print(f"📢 通知: {message}", style="cyan")
        except Exception as e:
            console.print(f"⚠️  发送通知失败: {e}", style="yellow")
    
    def _monitor_repo(self, repo_name: str):
        """监控单个仓库"""
        repo_config = self._get_repo_config(repo_name)
        check_interval = repo_config.get('check_interval', 300)
        
        console.print(f"👀 开始监控仓库: {repo_name} (间隔: {check_interval}s)", style="blue")
        
        while self.running:
            try:
                if self._check_repo_changes(repo_name):
                    self._deploy_config_change(repo_name)
                
                time.sleep(check_interval)
                
            except Exception as e:
                console.print(f"❌ 监控仓库失败 {repo_name}: {e}", style="red")
                time.sleep(60)  # 出错后等待1分钟再重试
    
    def start(self):
        """启动监控"""
        console.print("🚀 启动 Git 仓库监控器", style="blue")
        
        self.running = True
        
        # 为每个仓库创建监控线程
        for repo in self.config['repositories']:
            repo_name = repo['name']
            thread = threading.Thread(
                target=self._monitor_repo,
                args=(repo_name,),
                daemon=True
            )
            thread.start()
            self.threads.append(thread)
        
        console.print(f"✅ 监控 {len(self.threads)} 个仓库", style="green")
        
        try:
            # 主循环
            while self.running:
                time.sleep(1)
        except KeyboardInterrupt:
            console.print("\n🛑 收到停止信号", style="yellow")
            self.stop()
    
    def stop(self):
        """停止监控"""
        console.print("🛑 停止 Git 仓库监控器", style="blue")
        self.running = False
        
        # 等待所有线程结束
        for thread in self.threads:
            thread.join(timeout=5)
        
        console.print("✅ 监控器已停止", style="green")
    
    def show_status(self):
        """显示监控状态"""
        table = Table(title="Git 仓库监控状态")
        table.add_column("仓库名称", style="cyan")
        table.add_column("分支", style="green")
        table.add_column("配置文件", style="yellow")
        table.add_column("目标集群", style="blue")
        table.add_column("最后检查", style="magenta")
        table.add_column("状态", style="red")
        
        for repo in self.config['repositories']:
            repo_name = repo['name']
            branch = repo.get('branch', 'main')
            config_path = repo['config_path']
            target_cluster = repo.get('target_cluster', 'default')
            
            last_check = self.state.get(repo_name, {}).get('last_check', '从未检查')
            status = "✅ 正常" if repo_name in self.state else "❌ 未初始化"
            
            table.add_row(repo_name, branch, config_path, target_cluster, last_check, status)
        
        console.print(table)


if __name__ == '__main__':
    import sys
    
    config_file = sys.argv[1] if len(sys.argv) > 1 else 'repos.yaml'
    
    watcher = GitWatcher(config_file)
    
    try:
        watcher.start()
    except KeyboardInterrupt:
        watcher.stop() 