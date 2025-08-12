#!/usr/bin/env python3
"""
Etcd Manager
Store and manage NoKube cluster metadata using etcd as the only backend
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
    """etcd manager"""
    
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

    # ---- 通用 KV 接口：供控制器/部署逻辑使用 ----
    def set_kv(self, key: str, value: Any) -> None:
        """将任意值写入 etcd 指定键。
        - 对 dict/list 统一序列化为 JSON；其余按字符串写入
        - 写入失败直接抛出异常
        """
        if not self.client:
            raise RuntimeError("etcd client not available")
        try:
            if isinstance(value, (dict, list)):
                data = json.dumps(value, ensure_ascii=False).encode('utf-8')
            elif isinstance(value, (bytes, bytearray)):
                data = bytes(value)
            else:
                data = str(value).encode('utf-8')
            self.client.put(key, data)
        except Exception as e:
            console.print(f"❌ 写入 etcd 失败: key={key} err={e}", style="red")
            raise RuntimeError(f"failed to set etcd key {key}: {e}")

    def get_kv(self, key: str) -> Any:
        """从 etcd 读取值；若内容为 JSON 则反序列化返回，否则返回字符串。
        - 读取失败直接抛出异常
        """
        if not self.client:
            raise RuntimeError("etcd client not available")
        try:
            value, _ = self.client.get(key)
            if value is None:
                return None
            raw = value.decode('utf-8')
            try:
                return json.loads(raw)
            except Exception:
                return raw
        except Exception as e:
            console.print(f"❌ 读取 etcd 失败: key={key} err={e}", style="red")
            raise RuntimeError(f"failed to get etcd key {key}: {e}")
    
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
            
            # 衍生元数据：为每个节点提取代理环境变量
            metadata = {
                'node_proxy_env': self._derive_node_proxy_env(config)
            }

            # 添加新集群
            new_cluster = {
                'name': name,
                'type': cluster_type,
                'config': config,
                'status': 'inactive',  # 默认未启动
                'created_at': self._get_timestamp(),
                'nodes': config.get('nodes', []),  # 节点列表
                'users': config.get('users', []),  # 用户列表
                'metadata': metadata,
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
                    # 计算配置变更摘要
                    old_config = cluster.get('config', {}) or {}
                    change_summary = self._summarize_config_changes(old_config, config)
                    cluster['config'] = config
                    cluster['updated_at'] = self._get_timestamp()
                    # 同步更新元数据（代理环境）
                    metadata = cluster.get('metadata', {})
                    if not isinstance(metadata, dict):
                        metadata = {}
                    metadata['node_proxy_env'] = self._derive_node_proxy_env(config)
                    metadata['change_summary'] = change_summary
                    cluster['metadata'] = metadata
                    
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
            # 将 etcd 中的元数据注入到配置，便于后续管理器读取
            if isinstance(cluster_config, dict):
                cluster_config = dict(cluster_config)  # 复制，避免影响存储
                cluster_config['_nokube_metadata'] = cluster.get('metadata', {}) or {}
            
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
                    
                    # 自动确保系统 config DaemonSet
                    try:
                        self._ensure_system_config_daemonset(cluster_config)
                    except Exception as e:
                        console.print(f"⚠️  系统 config DaemonSet 启动失败: {e}", style="yellow")
                    
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

    def _ensure_system_config_daemonset(self, cluster_config: Dict[str, Any]) -> None:
        """确保系统 config DaemonSet 在集群启动后自动运行"""
        try:
            # Get head node info from cluster config (same logic as RayClusterManager)
            nodes = cluster_config.get('nodes', []) or []
            head_node = None
            for node in nodes:
                if (node.get('role') or '').lower() == 'head':
                    head_node = node
                    break
            if head_node is None and nodes:
                head_node = nodes[0]  # fallback to first node
            
            if not head_node or not head_node.get('ssh_url'):
                console.print("⚠️  未找到 head 节点信息，跳过系统 config DaemonSet", style="yellow")
                return
            
            # Use RemoteExecutor to leverage existing remote_lib infrastructure
            from src.ssh_manager import RemoteExecutor
            from pathlib import Path
            
            ssh_url = str(head_node.get('ssh_url'))
            host = ssh_url.split(':')[0]
            port = int(ssh_url.split(':')[1]) if ':' in ssh_url else 22
            
            # Get user credentials (same logic as RayClusterManager)
            users = head_node.get("users", []) or []
            if not users:
                console.print("⚠️  head 节点缺少 users 信息，跳过系统 config DaemonSet", style="yellow")
                return
            
            user = users[0]
            username = user.get("userid", "root")
            password = user.get("password")
            
            # Get cluster metadata for proxy env
            cluster_meta = cluster_config.get("_nokube_metadata", {}) or {}
            
            # Merge proxy environment (same as RayClusterManager._merge_proxy_env logic)
            env = {}
            # Get node-level proxy config
            for key in ["http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"]:
                if key in head_node:
                    env[key] = str(head_node[key])
            # Get cluster-level proxy config from metadata
            node_proxy_env = cluster_meta.get("node_proxy_env", {}) or {}
            ssh_key = f"{host}:{port}"
            if ssh_key in node_proxy_env:
                env.update(node_proxy_env[ssh_key])
            
            # Ensure remote_lib is uploaded (reuse existing logic)
            remote_executor = RemoteExecutor()
            remote_lib_path = Path(__file__).resolve().parent / "remote_lib"
            
            if not remote_executor.upload_remote_lib(host, port, username, password, str(remote_lib_path), env=env):
                console.print("⚠️  remote_lib 上传失败，跳过系统 config DaemonSet", style="yellow")
                return
            
            # Execute Ray command to ensure system config DaemonSet
            success = remote_executor.execute_ray_command_with_logging(
                host, port, username, password, 
                command="ensure-system-config",
                env=env
            )
            
            if success:
                console.print("✅ 系统 config DaemonSet 已确保（通过 SSH）", style="green")
            else:
                console.print("⚠️  系统 config DaemonSet 启动失败", style="yellow")
                
        except Exception as e:
            console.print(f"⚠️  系统 config DaemonSet 启动失败: {e}", style="yellow")

    def _derive_node_proxy_env(self, config: Dict[str, Any]) -> Dict[str, Dict[str, str]]:
        """从集群配置中提取每个节点的代理环境映射，键为 ssh_url。

        仅收集存在的字段，支持大小写 http_proxy/https_proxy/no_proxy。
        """
        result: Dict[str, Dict[str, str]] = {}
        try:
            for node in config.get('nodes', []) or []:
                ssh_url = node.get('ssh_url')
                if not ssh_url:
                    continue
                proxy_cfg = node.get('proxy') or {}
                if not isinstance(proxy_cfg, dict) or not proxy_cfg:
                    continue
                env: Dict[str, str] = {}
                for key in (
                    'http_proxy', 'https_proxy', 'no_proxy',
                    'HTTP_PROXY', 'HTTPS_PROXY', 'NO_PROXY'
                ):
                    val = proxy_cfg.get(key)
                    if val:
                        env[key] = str(val)
                if env:
                    # 以 ssh_url 和 节点 name 双键保存，方便通过任一键查找
                    result[ssh_url] = env
                    node_name = node.get('name')
                    if node_name:
                        result[node_name] = env
        except Exception as _:
            pass
        return result

    def _summarize_config_changes(self, old: Dict[str, Any], new: Dict[str, Any]) -> Dict[str, Any]:
        """汇总配置变更，标记是否需要重启。

        关注字段：
        - 节点级：proxy(http/https/no_proxy) 变更（不需要重启）
        - 节点级：ray_config.port / dashboard_port 变更（需要重启）
        - 节点级：ssh_url 变化、新增/删除节点（需要重启）
        - 其余字段仅记录摘要
        """
        summary: Dict[str, Any] = {
            'requires_restart': False,
            'nodes': {}
        }
        try:
            old_nodes = old.get('nodes', []) or []
            new_nodes = new.get('nodes', []) or []
            # 使用 ssh_url 作为键；若缺失则使用 name
            def key_of(n: Dict[str, Any]) -> str:
                return n.get('ssh_url') or f"name:{n.get('name','')}"

            old_map = {key_of(n): n for n in old_nodes}
            new_map = {key_of(n): n for n in new_nodes}

            # 检查新增/删除
            removed = set(old_map.keys()) - set(new_map.keys())
            added = set(new_map.keys()) - set(old_map.keys())
            if removed or added:
                summary['requires_restart'] = True
                if removed:
                    summary['removed_nodes'] = list(sorted(removed))
                if added:
                    summary['added_nodes'] = list(sorted(added))

            # 检查相同键的节点变更
            for k in set(old_map.keys()) & set(new_map.keys()):
                o = old_map[k] or {}
                n = new_map[k] or {}
                node_changes: Dict[str, Any] = {}
                # 代理变更（不需重启）
                def proxy_env(d: Dict[str, Any]) -> Dict[str, str]:
                    p = d.get('proxy') or {}
                    env = {}
                    for kk in ('http_proxy','https_proxy','no_proxy','HTTP_PROXY','HTTPS_PROXY','NO_PROXY'):
                        if p.get(kk):
                            env[kk] = str(p.get(kk))
                    return env
                pe_old = proxy_env(o)
                pe_new = proxy_env(n)
                if pe_old != pe_new:
                    node_changes['proxy_changed'] = {'old': pe_old, 'new': pe_new}
                # 端口变更（需要重启）
                rc_old = (o.get('ray_config') or {})
                rc_new = (n.get('ray_config') or {})
                port_old = rc_old.get('port')
                port_new = rc_new.get('port')
                dash_old = rc_old.get('dashboard_port')
                dash_new = rc_new.get('dashboard_port')
                port_delta: Dict[str, Any] = {}
                if port_old != port_new:
                    port_delta['port'] = {'old': port_old, 'new': port_new}
                if dash_old != dash_new:
                    port_delta['dashboard_port'] = {'old': dash_old, 'new': dash_new}
                if port_delta:
                    node_changes['ray_port_changed'] = port_delta
                    summary['requires_restart'] = True
                if node_changes:
                    summary['nodes'][k] = node_changes
        except Exception:
            # 容错处理，出现异常时不阻断更新
            pass
        return summary


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