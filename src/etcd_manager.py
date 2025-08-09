#!/usr/bin/env python3
"""
etcd 集群管理器
用于存储和管理 NoKube 集群列表
专注于 etcd 作为唯一存储后端
"""

import json
import os
import yaml
from typing import Dict, List, Optional, Any
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from src.config import get_config

console = Console()


class EtcdManager:
    """etcd 集群管理器"""
    
    def __init__(self, etcd_hosts: Optional[str] = None):
        # 获取全局配置
        self.global_config = get_config()
        
        # 优先使用传入的参数，否则使用配置文件
        if etcd_hosts:
            self.etcd_hosts = etcd_hosts
        else:
            self.etcd_hosts = self.global_config.get_etcd_hosts()
        
        self.clusters_key = f"{self.global_config.get_etcd_prefix()}/clusters"
        self.configs_key = f"{self.global_config.get_etcd_prefix()}/configs"
        self.timeout = self.global_config.get_etcd_timeout()
        self.username = self.global_config.get_etcd_username()
        self.password = self.global_config.get_etcd_password()
        
        # 检查 etcd 配置要求
        if not self.global_config.check_etcd_requirements():
            raise RuntimeError("etcd 配置不正确")
        
        self.client = None
        self._init_etcd_client()
    
    def _init_etcd_client(self):
        """初始化 etcd 客户端"""
        try:
            import etcd3
            host = self.etcd_hosts.split(',')[0].split(':')[0]
            port = int(self.etcd_hosts.split(',')[0].split(':')[1])
            
            # 构建连接参数
            client_kwargs = {
                'host': host, 
                'port': port, 
                'timeout': self.timeout
            }
            
            # 如果配置了认证信息，添加到参数中
            if self.username:
                client_kwargs['user'] = self.username
            if self.password:
                client_kwargs['password'] = self.password
            
            self.client = etcd3.client(**client_kwargs)
            
            # 测试连接
            self.client.status()
            console.print("✅ etcd 连接成功", style="green")
            
        except ImportError:
            console.print("❌ etcd3 未安装", style="red")
            console.print("   运行 'pip install etcd3' 安装 etcd3 客户端", style="red")
            raise RuntimeError("etcd3 client not available")
        except Exception as e:
            error_msg = str(e)
            if "connection failed" in error_msg.lower() or "refused" in error_msg.lower():
                console.print(f"❌ etcd 连接失败: 无法连接到 {self.etcd_hosts}", style="red")
                console.print("", style="red")
                console.print("💡 解决方案:", style="blue")
                console.print("   1. 启动 etcd 服务:", style="blue")
                console.print("      docker run -d --name etcd -p 2379:2379 \\", style="cyan")
                console.print("        quay.io/coreos/etcd:latest \\", style="cyan")
                console.print("        etcd --advertise-client-urls http://0.0.0.0:2379 \\", style="cyan")
                console.print("        --listen-client-urls http://0.0.0.0:2379", style="cyan")
                console.print("", style="blue")
                console.print("   2. 或配置正确的 etcd 地址:", style="blue")
                console.print("      nokube config init --etcd-host <your-etcd-host>", style="cyan")
                console.print("", style="blue")
                console.print("   3. 或查看配置帮助: nokube config example", style="blue")
            else:
                console.print(f"❌ etcd 连接失败: {e}", style="red")
            
            raise RuntimeError(f"etcd connection failed: {e}")
    
    def list_clusters(self) -> List[Dict[str, Any]]:
        """列出所有集群"""
        if not self.client:
            raise RuntimeError("etcd client not available")
            
        try:
            value, _ = self.client.get(self.clusters_key)
            if value:
                return json.loads(value.decode('utf-8'))
            return []
        except Exception as e:
            console.print(f"❌ 从 etcd 读取集群列表失败: {e}", style="red")
            raise RuntimeError(f"Failed to read clusters from etcd: {e}")
    
    def get_cluster(self, name: str) -> Optional[Dict[str, Any]]:
        """获取指定集群信息"""
        clusters = self.list_clusters()
        for cluster in clusters:
            if cluster.get('name') == name:
                return cluster
        return None
    
    def add_cluster(self, name: str, cluster_type: str, config: Dict[str, Any]) -> bool:
        """添加集群"""
        if not self.client:
            raise RuntimeError("etcd client not available")
            
        try:
            clusters = self.list_clusters()
            
            # 检查是否已存在
            for cluster in clusters:
                if cluster.get('name') == name:
                    console.print(f"⚠️  集群 {name} 已存在", style="yellow")
                    return False
            
            # 添加新集群
            new_cluster = {
                'name': name,
                'type': cluster_type,
                'config': config,
                'status': 'inactive',  # 默认未启动
                'created_at': self._get_timestamp(),
                'nodes': config.get('nodes', []),  # 节点列表
                'users': config.get('users', [])  # 用户列表
            }
            
            clusters.append(new_cluster)
            
            self.client.put(self.clusters_key, json.dumps(clusters).encode('utf-8'))
            console.print(f"✅ 集群 {name} 添加成功", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ 添加集群失败: {e}", style="red")
            return False
    
    def update_cluster(self, name: str, config: Dict[str, Any]) -> bool:
        """更新集群配置"""
        if not self.client:
            raise RuntimeError("etcd client not available")
            
        try:
            clusters = self.list_clusters()
            
            for cluster in clusters:
                if cluster.get('name') == name:
                    cluster['config'] = config
                    cluster['updated_at'] = self._get_timestamp()
                    
                    self.client.put(self.clusters_key, json.dumps(clusters).encode('utf-8'))
                    console.print(f"✅ 集群 {name} 更新成功", style="green")
                    return True
            
            console.print(f"❌ 集群 {name} 不存在", style="red")
            return False
            
        except Exception as e:
            console.print(f"❌ 更新集群失败: {e}", style="red")
            return False
    
    def delete_cluster(self, name: str) -> bool:
        """删除集群"""
        if not self.client:
            raise RuntimeError("etcd client not available")
            
        try:
            clusters = self.list_clusters()
            
            for i, cluster in enumerate(clusters):
                if cluster.get('name') == name:
                    del clusters[i]
                    
                    self.client.put(self.clusters_key, json.dumps(clusters).encode('utf-8'))
                    console.print(f"✅ 集群 {name} 删除成功", style="green")
                    return True
            
            console.print(f"❌ 集群 {name} 不存在", style="red")
            return False
            
        except Exception as e:
            console.print(f"❌ 删除集群失败: {e}", style="red")
            return False
    
    def set_cluster_status(self, name: str, status: str) -> bool:
        """设置集群状态"""
        if not self.client:
            raise RuntimeError("etcd client not available")
            
        try:
            clusters = self.list_clusters()
            
            for cluster in clusters:
                if cluster.get('name') == name:
                    cluster['status'] = status
                    cluster['updated_at'] = self._get_timestamp()
                    
                    self.client.put(self.clusters_key, json.dumps(clusters).encode('utf-8'))
                    console.print(f"✅ 集群 {name} 状态更新为 {status}", style="green")
                    return True
            
            console.print(f"❌ 集群 {name} 不存在", style="red")
            return False
            
        except Exception as e:
            console.print(f"❌ 更新集群状态失败: {e}", style="red")
            return False
    
    def get_cluster_config(self, name: str) -> Optional[Dict[str, Any]]:
        """获取集群配置"""
        cluster = self.get_cluster(name)
        return cluster.get('config') if cluster else None
    
    def show_clusters(self):
        """显示集群列表"""
        clusters = self.list_clusters()
        
        if not clusters:
            console.print("📊 暂无集群", style="blue")
            return
        
        table = Table(title="NoKube 集群列表")
        table.add_column("集群名称", style="cyan")
        table.add_column("类型", style="green")
        table.add_column("状态", style="yellow")
        table.add_column("创建时间", style="blue")
        table.add_column("更新时间", style="magenta")
        
        for cluster in clusters:
            name = cluster.get('name', 'unknown')
            cluster_type = cluster.get('type', 'unknown')
            status = cluster.get('status', 'unknown')
            created_at = cluster.get('created_at', 'unknown')
            updated_at = cluster.get('updated_at', 'unknown')
            
            # 格式化时间
            if created_at != 'unknown':
                created_at = self._format_timestamp(created_at)
            if updated_at != 'unknown':
                updated_at = self._format_timestamp(updated_at)
            
            table.add_row(name, cluster_type, status, created_at, updated_at)
        
        console.print(table)
    
    def _get_timestamp(self) -> str:
        """获取当前时间戳"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def _format_timestamp(self, timestamp: str) -> str:
        """格式化时间戳"""
        try:
            from datetime import datetime
            dt = datetime.fromisoformat(timestamp)
            return dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            return timestamp
    
    def import_clusters_from_yaml(self, yaml_file: str) -> bool:
        """从 YAML 文件导入集群配置（兼容旧格式）"""
        try:
            with open(yaml_file, 'r', encoding='utf-8') as f:
                clusters_data = yaml.safe_load(f)
            
            # 检查是否是新的单个集群配置格式
            if isinstance(clusters_data, dict) and 'clusters' not in clusters_data:
                # 单个集群配置文件，需要提供名称和类型
                console.print("⚠️  检测到单个集群配置文件，请使用 import_cluster_from_yaml 方法", style="yellow")
                return False
            
            # 检查是否是旧的 clusters.yaml 格式
            if isinstance(clusters_data, dict) and 'clusters' in clusters_data:
                console.print("⚠️  检测到 clusters.yaml 格式，已不再支持", style="yellow")
                console.print("请使用单个集群配置文件，或直接使用 add_cluster 方法", style="yellow")
                return False
            
            console.print("❌ 不支持的 YAML 文件格式", style="red")
            return False
            
        except Exception as e:
            console.print(f"❌ 导入集群配置失败: {e}", style="red")
            return False
    
    def export_clusters_to_yaml(self, yaml_file: str) -> bool:
        """导出集群配置到 YAML 文件"""
        try:
            clusters = self.list_clusters()
            
            export_data = {
                'clusters': clusters,
                'exported_at': self._get_timestamp()
            }
            
            with open(yaml_file, 'w', encoding='utf-8') as f:
                yaml.dump(export_data, f, default_flow_style=False, indent=2)
            
            console.print(f"✅ 集群配置已导出到 {yaml_file}", style="green")
            return True
            
        except Exception as e:
            console.print(f"❌ 导出集群配置失败: {e}", style="red")
            return False

    def start_cluster(self, name: str) -> bool:
        """启动集群"""
        try:
            cluster = self.get_cluster(name)
            if not cluster:
                console.print(f"❌ 集群 {name} 不存在", style="red")
                return False
            
            cluster_type = cluster.get('type', 'unknown')
            cluster_config = cluster.get('config', {})
            
            if cluster_type == 'ray':
                # 启动 Ray 集群
                console.print(f"🚀 启动 Ray 集群: {name}", style="blue")
                
                from src.ray_cluster_manager import RayClusterManager
                manager = RayClusterManager()
                
                # 使用配置启动集群
                success = manager.start_cluster(cluster_config)
                
                if success:
                    # 更新集群状态
                    self.set_cluster_status(name, 'running')
                    console.print(f"✅ Ray 集群 {name} 启动成功", style="green")
                    return True
                else:
                    console.print(f"❌ Ray 集群启动失败", style="red")
                    return False
            else:
                console.print("目前仅支持 ray 集群", style="yellow")
                return False
                
        except Exception as e:
            console.print(f"❌ 启动集群失败: {e}", style="red")
            return False


if __name__ == '__main__':
    import sys
    
    manager = EtcdManager()
    
    if len(sys.argv) < 2:
        console.print("用法: python etcd_manager.py <command> [args...]", style="red")
        sys.exit(1)
    
    command = sys.argv[1]
    
    if command == 'list':
        manager.show_clusters()
    elif command == 'add':
        if len(sys.argv) < 5:
            console.print("用法: python etcd_manager.py add <name> <type> <config_file>", style="red")
            sys.exit(1)
        
        name = sys.argv[2]
        cluster_type = sys.argv[3]
        config_file = sys.argv[4]
        
        with open(config_file, 'r') as f:
            config = json.load(f)
        
        manager.add_cluster(name, cluster_type, config)
    elif command == 'delete':
        if len(sys.argv) < 3:
            console.print("用法: python etcd_manager.py delete <name>", style="red")
            sys.exit(1)
        
        name = sys.argv[2]
        manager.delete_cluster(name)
    elif command == 'import':
        if len(sys.argv) < 3:
            console.print("用法: python etcd_manager.py import <yaml_file>", style="red")
            sys.exit(1)
        
        yaml_file = sys.argv[2]
        manager.import_clusters_from_yaml(yaml_file)
    elif command == 'export':
        if len(sys.argv) < 3:
            console.print("用法: python etcd_manager.py export <yaml_file>", style="red")
            sys.exit(1)
        
        yaml_file = sys.argv[2]
        manager.export_clusters_to_yaml(yaml_file)
    else:
        console.print(f"❌ 未知命令: {command}", style="red")
        sys.exit(1) 