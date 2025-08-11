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

try:
    # 同目录导入
    from auto_deployer import AutoDeployer
    from config_scanner import ConfigScanner
except Exception:
    # 作为模块运行时的兜底（例如通过启动器调用）
    pass

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
        """获取仓库配置路径（文件或目录）的哈希值"""
        try:
            # 克隆或更新仓库
            repo_dir = f"./repos/{repo_name}"
            repo_config = self._get_repo_config(repo_name)
            
            if not os.path.exists(repo_dir):
                self._clone_repo(repo_config)
            else:
                self._pull_repo(repo_config)
            
            config_root = os.path.join(repo_dir, config_path)
            # 支持目录：聚合目录下所有 yaml/yml 文件
            if os.path.isdir(config_root):
                digest = hashlib.md5()
                for root, _, files in os.walk(config_root):
                    for fname in sorted(files):
                        if fname.endswith(('.yaml', '.yml')):
                            fpath = os.path.join(root, fname)
                            try:
                                with open(fpath, 'rb') as f:
                                    content = f.read()
                                digest.update(fpath.encode('utf-8'))
                                digest.update(content)
                            except Exception:
                                continue
                return digest.hexdigest()
            else:
                # 单文件
                if os.path.exists(config_root):
                    with open(config_root, 'rb') as f:
                        content = f.read()
                    return hashlib.md5(content).hexdigest()
            
        except Exception as e:
            console.print(f"❌ 获取仓库哈希失败 {repo_name}: {e}", style="red")
        
        return ""

    def _get_repo_head(self, repo_name: str) -> str:
        """获取仓库当前 HEAD 提交哈希（失败返回空字符串）"""
        try:
            repo_config = self._get_repo_config(repo_name)
            repo_dir = f"./repos/{repo_name}"
            if not os.path.exists(repo_dir):
                try:
                    self._clone_repo(repo_config)
                except Exception as e:
                    console.print(f"⚠️ 仓库克隆失败(跳过本轮): {repo_name} -> {e}", style="yellow")
                    return ""
            else:
                try:
                    self._pull_repo(repo_config)
                except Exception as e:
                    console.print(f"⚠️ 仓库更新失败(继续轮询): {repo_name} -> {e}", style="yellow")
            # 读取 HEAD 提交
            result = subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo_dir, capture_output=True, text=True)
            if result.returncode == 0:
                return (result.stdout or "").strip()
            console.print(f"⚠️ 获取 HEAD 失败: {repo_name} -> {result.stderr}", style="yellow")
            return ""
        except Exception as e:
            console.print(f"⚠️ 获取仓库 HEAD 失败: {repo_name} -> {e}", style="yellow")
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
            # SSH 认证：优先 key_path，否则支持内联 ssh_key（写入临时文件）
            auth_cfg = repo_config.get('auth', {})
            key_path = auth_cfg.get('key_path')
            if not key_path:
                inline_key = auth_cfg.get('ssh_key') or auth_cfg.get('ssh_key_inline')
                if inline_key:
                    repo_tmp_dir = os.path.join('/tmp', 'git-watcher', repo_name)
                    os.makedirs(repo_tmp_dir, exist_ok=True)
                    key_path = os.path.join(repo_tmp_dir, 'id_rsa')
                    try:
                        with open(key_path, 'w', encoding='utf-8') as kf:
                            kf.write(inline_key.strip() + ("\n" if not inline_key.endswith("\n") else ""))
                        os.chmod(key_path, 0o600)
                    except Exception as e:
                        raise RuntimeError(f"写入内联 SSH Key 失败: {e}")
                else:
                    raise ValueError("SSH 认证需要提供 auth.key_path 或 auth.ssh_key")
            env = os.environ.copy()
            env['GIT_SSH_COMMAND'] = f'ssh -i {key_path} -o StrictHostKeyChecking=no'
        else:
            # HTTPS 认证（优先直接 token，其次 token_path 文件；均不使用环境变量）
            auth_cfg = repo_config.get('auth', {})
            token = auth_cfg.get('token')
            if not token:
                token_path = auth_cfg.get('token_path', '/app/ssh/github-token')
                try:
                    if token_path and os.path.exists(token_path):
                        with open(token_path, 'r', encoding='utf-8') as tf:
                            token = tf.read().strip()
                except Exception:
                    token = None
            if token:
                # 直接临时拼接到 URL（注意避免日志打印）
                if repo_url.startswith('https://'):
                    repo_url = repo_url.replace('https://', f'https://{token}@', 1)
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
            auth_cfg = repo_config.get('auth', {})
            key_path = auth_cfg.get('key_path')
            if not key_path:
                inline_key = auth_cfg.get('ssh_key') or auth_cfg.get('ssh_key_inline')
                if inline_key:
                    repo_tmp_dir = os.path.join('/tmp', 'git-watcher', repo_name)
                    os.makedirs(repo_tmp_dir, exist_ok=True)
                    key_path = os.path.join(repo_tmp_dir, 'id_rsa')
                    try:
                        with open(key_path, 'w', encoding='utf-8') as kf:
                            kf.write(inline_key.strip() + ("\n" if not inline_key.endswith("\n") else ""))
                        os.chmod(key_path, 0o600)
                    except Exception as e:
                        raise RuntimeError(f"写入内联 SSH Key 失败: {e}")
                else:
                    raise ValueError("SSH 认证需要提供 auth.key_path 或 auth.ssh_key")
            env = os.environ.copy()
            env['GIT_SSH_COMMAND'] = f'ssh -i {key_path} -o StrictHostKeyChecking=no'
        else:
            # HTTPS 拉取（尽量复用已存在的远程凭据；不通过环境变量传递）
            env = os.environ.copy()
        
        # 拉取更新
        result = subprocess.run([
            'git', 'pull'
        ], cwd=repo_dir, env=env, capture_output=True, text=True)
        
        if result.returncode != 0:
            raise Exception(f"更新仓库失败: {result.stderr}")
    
    def _check_repo_changes(self, repo_name: str) -> bool:
        """检查仓库是否有变更：基于 HEAD 提交哈希判断"""
        head = self._get_repo_head(repo_name)
        # 失败或未能获取 commit，保持静默继续轮询
        if not head:
            # 仅更新时间戳，保留原状态
            if repo_name not in self.state:
                self.state[repo_name] = {}
            self.state[repo_name]['last_check'] = datetime.now().isoformat()
            self._save_state()
            return False

        last_commit = self.state.get(repo_name, {}).get('last_commit', '')
        if head != last_commit:
            console.print(f"🔄 检测到提交变更: {repo_name} {last_commit[:7]} -> {head[:7]}", style="yellow")
            if repo_name not in self.state:
                self.state[repo_name] = {}
            self.state[repo_name]['last_commit'] = head
            self.state[repo_name]['last_check'] = datetime.now().isoformat()
            self._save_state()
            return True
        # 无变更
        if repo_name not in self.state:
            self.state[repo_name] = {}
        self.state[repo_name]['last_check'] = datetime.now().isoformat()
        self._save_state()
        return False

    def _list_yaml_files(self, repo_name: str, path_value: str) -> list:
        """列出仓库路径下的 YAML 文件（支持文件或目录）"""
        repo_dir = f"./repos/{repo_name}"
        full_path = os.path.join(repo_dir, path_value)
        files = []
        if os.path.isdir(full_path):
            for root, _, fnames in os.walk(full_path):
                for fname in sorted(fnames):
                    if fname.endswith(('.yaml', '.yml')):
                        files.append(os.path.join(root, fname))
        else:
            if os.path.exists(full_path) and full_path.endswith(('.yaml', '.yml')):
                files.append(full_path)
        return files

    def _load_repo_configs(self, repo_name: str) -> list:
        """加载并解析仓库中的 YAML 配置，返回配置对象列表"""
        try:
            repo_config = self._get_repo_config(repo_name)
            path_value = repo_config.get('path') or repo_config.get('config_path')
            if not path_value:
                return []
            yaml_files = self._list_yaml_files(repo_name, path_value)
            configs = []
            scanner = ConfigScanner() if 'ConfigScanner' in globals() else None
            for fpath in yaml_files:
                try:
                    if scanner:
                        scanned = scanner.scan_config_file(fpath)
                        configs.extend(scanned.values())
                    else:
                        # 简单解析以兜底
                        with open(fpath, 'r', encoding='utf-8') as f:
                            documents = list(yaml.safe_load_all(f))
                        for doc in documents:
                            if isinstance(doc, dict) and 'kind' in doc:
                                configs.append(doc)
                except Exception:
                    continue
            return configs
        except Exception as e:
            console.print(f"❌ 加载仓库配置失败 {repo_name}: {e}", style="red")
            return []
    
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
            deployer = AutoDeployer() if 'AutoDeployer' in globals() else None
            
            # 部署到目标集群
            success = False
            if deployer:
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
            webhook_config = (
                self.config.get('global', {})
                .get('notifications', {})
                .get('webhook', {})
            )
            if webhook_config and webhook_config.get('enabled'):
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
                    # 解析配置并逐条部署
                    repo_cfg = self._get_repo_config(repo_name)
                    target_cluster = repo_cfg.get('target_cluster', 'default')
                    configs = self._load_repo_configs(repo_name)
                    if not configs:
                        console.print(f"⚠️ {repo_name} 未发现可部署配置", style="yellow")
                    for cfg in configs:
                        # cfg 可能来自扫描器（已拆解），也可能是原始 K8s 文档
                        self.deploy_config_change(repo_name, cfg, target_cluster)
                
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