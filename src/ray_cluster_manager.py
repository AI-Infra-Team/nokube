#!/usr/bin/env python3
"""
Ray 集群管理器
用于管理 Ray 分布式计算集群
支持远程部署到多个节点
"""

import subprocess
import time
import os
import tempfile
import shutil
from pathlib import Path
from typing import Optional, Dict, Any, List
from rich.console import Console
from rich.table import Table
from .cluster_manager import ClusterManager
from .ssh_manager import RemoteExecutor

console = Console()


class RayClusterManager(ClusterManager):
    """Ray 集群管理器"""
    
    def __init__(self):
        super().__init__()
        self.cluster_name = "ray"
        self.head_node = None
        self.worker_nodes = []
        self.remote_lib_path = Path(__file__).parent / "remote_lib"
        self.remote_executor = RemoteExecutor()
    
    def start_cluster(self, config: Dict[str, Any] = None, **kwargs) -> bool:
        """启动 Ray 集群"""
        try:
            if not config:
                console.print("❌ 需要提供集群配置", style="red")
                return False
            
            console.print("🚀 启动 Ray 集群", style="blue")
            
            # 解析配置
            nodes = config.get('nodes', [])
            if not nodes:
                console.print("❌ 配置中没有节点信息", style="red")
                return False
            
            # 找到 head 节点
            head_node = None
            worker_nodes = []
            
            for node in nodes:
                if node.get('role') == 'head':
                    head_node = node
                elif node.get('role') == 'worker':
                    worker_nodes.append(node)
            
            if not head_node:
                console.print("❌ 配置中没有 head 节点", style="red")
                return False
            
            # 启动 head 节点
            console.print(f"🎯 启动 head 节点: {head_node.get('name', 'unknown')}", style="blue")
            if not self._start_head_node(head_node):
                console.print("❌ Head 节点启动失败", style="red")
                return False
            
            time.sleep(5)

            # 启动 worker 节点
            if worker_nodes:
                console.print(f"🔧 启动 {len(worker_nodes)} 个 worker 节点", style="blue")
                for worker in worker_nodes:
                    if not self._start_worker_node(worker, head_node):
                        console.print(f"⚠️  Worker 节点 {worker.get('name')} 启动失败", style="yellow")
            
            console.print("✅ Ray 集群启动完成", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ 启动 Ray 集群失败: {e}", style="red")
            return False
    
    def _start_head_node(self, head_node: Dict[str, Any]) -> bool:
        """启动 head 节点"""
        try:
            ssh_url = head_node.get('ssh_url')
            if not ssh_url:
                console.print("❌ Head 节点缺少 ssh_url", style="red")
                return False
            
            # 解析 SSH 连接信息
            host, ssh_port = self._parse_ssh_url(ssh_url)
            
            # 获取用户信息
            users = head_node.get('users', [])
            if not users:
                console.print("❌ Head 节点缺少用户信息", style="red")
                return False
            
            # 使用第一个用户
            user = users[0]
            username = user.get('userid', 'root')
            password = user.get('password')
            
            # 上传远程执行库
            if not self.remote_executor.upload_remote_lib(host, ssh_port, username, password, str(self.remote_lib_path)):
                return False
            
            # 在远程节点上启动 head
            ray_config = head_node.get('ray_config', {})
            ray_port = ray_config.get('port', 10001)
            dashboard_port = ray_config.get('dashboard_port', 8265)
            num_cpus = ray_config.get('num_cpus', 4)
            object_store_memory = ray_config.get('object_store_memory', 1000000000)
            
            ray_args = [
                f"--port={ray_port}",
                f"--dashboard-port={dashboard_port}",
                f"--num-cpus={num_cpus}",
                f"--object-store-memory={object_store_memory}",
                f"--node-ip-address={host}"
            ]
            
            # Execute startup command with auto-generated logging
            node_name = head_node.get('name', 'ray-head')
            success = self.remote_executor.execute_ray_command_with_logging(
                host, ssh_port, username, password, "start-head", ray_args, 
                realtime_output=True, logtag=f"head_{node_name}"
            )
            
            if success:
                console.print("✅ Head 节点启动成功", style="green")
                console.print(f"  Dashboard: http://{host}:{dashboard_port}", style="cyan")
                self.head_node = head_node
                return True
            else:
                console.print("❌ Head 节点启动失败", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 启动 head 节点失败: {e}", style="red")
            return False
    
    def _start_worker_node(self, worker_node: Dict[str, Any], head_node: Dict[str, Any]) -> bool:
        """启动 worker 节点"""
        try:
            ssh_url = worker_node.get('ssh_url')
            if not ssh_url:
                console.print("❌ Worker 节点缺少 ssh_url", style="red")
                return False
            
            # 解析 SSH 连接信息
            host, ssh_port = self._parse_ssh_url(ssh_url)
            
            # 获取用户信息
            users = worker_node.get('users', [])
            if not users:
                console.print("❌ Worker 节点缺少用户信息", style="red")
                return False
            
            # 使用第一个用户
            user = users[0]
            username = user.get('userid', 'root')
            password = user.get('password')
            
            # 上传远程执行库
            if not self.remote_executor.upload_remote_lib(host, ssh_port, username, password, str(self.remote_lib_path)):
                return False
            
            # 获取 head 节点地址
            head_host, head_ssh_port = self._parse_ssh_url(head_node.get('ssh_url'))
            head_ray_port = head_node.get('ray_config', {}).get('port', 10001)
            head_address = f"{head_host}:{head_ray_port}"
            
            # 在远程节点上启动 worker
            ray_config = worker_node.get('ray_config', {})
            num_cpus = ray_config.get('num_cpus', 2)
            object_store_memory = ray_config.get('object_store_memory', 500000000)
            
            ray_args = [
                f"--head-address={head_address}",
                f"--num-cpus={num_cpus}",
                f"--object-store-memory={object_store_memory}",
            ]
            
            # Execute startup command with auto-generated logging
            worker_name = worker_node.get('name', 'worker')
            success = self.remote_executor.execute_ray_command_with_logging(
                host, ssh_port, username, password, "start-worker", ray_args,
                realtime_output=True, logtag=f"worker_{worker_name}"
            )
            
            if success:
                console.print(f"✅ Worker 节点 {worker_node.get('name')} 启动成功", style="green")
                self.worker_nodes.append(worker_node)
                return True
            else:
                console.print(f"❌ Worker 节点启动失败", style="red")
                return False
                
        except Exception as e:
            console.print(f"❌ 启动 worker 节点失败: {e}", style="red")
            return False
    
    def stop_cluster(self, config: Dict[str, Any] = None, **kwargs) -> bool:
        """停止 Ray 集群"""
        try:
            console.print("🛑 停止 Ray 集群", style="blue")
            
            if not config:
                console.print("❌ 需要提供集群配置", style="red")
                return False
            
            nodes = config.get('nodes', [])
            success_count = 0
            
            for node in nodes:
                ssh_url = node.get('ssh_url')
                if not ssh_url:
                    continue
                
                host, ssh_port = self._parse_ssh_url(ssh_url)
                
                # 获取用户信息
                users = node.get('users', [])
                if not users:
                    continue
                
                user = users[0]
                username = user.get('userid', 'root')
                password = user.get('password')
                
                # Execute stop command with auto-generated logging
                node_name = node.get('name', 'unknown')
                success = self.remote_executor.execute_ray_command_with_logging(
                    host, ssh_port, username, password, "stop", 
                    realtime_output=True, logtag=f"stop_{node_name}"
                )
                
                if success:
                    console.print(f"✅ 节点 {node.get('name')} 已停止", style="green")
                    success_count += 1
                else:
                    console.print(f"❌ 节点 {node.get('name')} 停止失败", style="red")
            
            console.print(f"✅ 已停止 {success_count}/{len(nodes)} 个节点", style="green")
            return success_count > 0
            
        except Exception as e:
            console.print(f"❌ 停止 Ray 集群失败: {e}", style="red")
            return False
    
    def show_status(self, config: Dict[str, Any] = None, **kwargs) -> None:
        """显示 Ray 集群状态"""
        try:
            console.print("📊 Ray 集群状态", style="blue")
            
            if not config:
                console.print("❌ 需要提供集群配置", style="red")
                return
            
            nodes = config.get('nodes', [])
            
            table = Table(title="Ray 集群节点状态")
            table.add_column("节点名称", style="cyan")
            table.add_column("角色", style="green")
            table.add_column("地址", style="yellow")
            table.add_column("状态", style="blue")
            
            for node in nodes:
                name = node.get('name', 'unknown')
                role = node.get('role', 'unknown')
                ssh_url = node.get('ssh_url', 'unknown')
                
                # 检查节点状态
                status = self._check_node_status(node)
                
                table.add_row(name, role, ssh_url, status)
            
            console.print(table)
            
        except Exception as e:
            console.print(f"❌ 获取状态失败: {e}", style="red")
    
    def _parse_ssh_url(self, ssh_url: str) -> tuple:
        """解析 SSH URL"""
        if ':' in ssh_url:
            host, port = ssh_url.rsplit(':', 1)
            return host, int(port)
        else:
            return ssh_url, 22
    
    def _check_node_status(self, node: Dict[str, Any]) -> str:
        """检查节点状态"""
        try:
            ssh_url = node.get('ssh_url')
            if not ssh_url:
                return "❌ 无地址"
            
            host, ssh_port = self._parse_ssh_url(ssh_url)
            
            # 获取用户信息
            users = node.get('users', [])
            if not users:
                return "❌ 无用户信息"
            
            user = users[0]
            username = user.get('userid', 'root')
            password = user.get('password')
            
            # 检查 Ray 状态
            status = self.remote_executor.check_ray_status(host, ssh_port, username, password)
            return status
                
        except Exception as e:
            return f"❓ 未知: {e}"
    
    def is_running(self, config: Dict[str, Any] = None) -> bool:
        """检查 Ray 集群是否正在运行"""
        if not config:
            return False
        
        nodes = config.get('nodes', [])
        running_nodes = 0
        
        for node in nodes:
            if self._check_node_status(node) == "✅ 运行中":
                running_nodes += 1
        
        return running_nodes > 0
    
    def get_config(self) -> Dict[str, Any]:
        """获取 Ray 集群配置"""
        return {
            "type": "ray",
            "head_node": self.head_node,
            "worker_nodes": self.worker_nodes
        }


if __name__ == '__main__':
    import sys
    
    manager = RayClusterManager()
    
    if len(sys.argv) < 2:
        console.print("用法: python ray_cluster_manager.py <command> [config_file]", style="red")
        sys.exit(1)
    
    command = sys.argv[1]
    config_file = sys.argv[2] if len(sys.argv) > 2 else None
    
    if command == 'start':
        if not config_file:
            console.print("❌ 需要提供配置文件", style="red")
            sys.exit(1)
        
        # 加载配置
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        manager.start_cluster(config)
    elif command == 'stop':
        if not config_file:
            console.print("❌ 需要提供配置文件", style="red")
            sys.exit(1)
        
        # 加载配置
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        manager.stop_cluster(config)
    elif command == 'status':
        if not config_file:
            console.print("❌ 需要提供配置文件", style="red")
            sys.exit(1)
        
        # 加载配置
        import yaml
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        
        manager.show_status(config)
    else:
        console.print(f"❌ 未知命令: {command}", style="red")
        sys.exit(1) 