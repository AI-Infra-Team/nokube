#!/usr/bin/env python3
"""
Ray Cluster Manager
Start Ray head and worker processes on remote nodes via SSH.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from rich.console import Console
from .ssh_manager import RemoteExecutor

console = Console()


class RayClusterManager:
    """Ray cluster manager that starts head and worker nodes via SSH."""
    
    def __init__(self) -> None:
        self.cluster_name = "ray"
        self.remote_lib_path = Path(__file__).parent / "remote_lib"
        self.remote_executor = RemoteExecutor()

    # ---------------- Helpers -----------------
    def _parse_ssh_url(self, ssh_url: str) -> Tuple[str, int]:
        host = ssh_url
        port = 22
        try:
            if ":" in ssh_url:
                parts = ssh_url.rsplit(":", 1)
                host = parts[0]
                port = int(parts[1])
        except Exception:
            host, port = ssh_url, 22
        return host, port

    def _merge_proxy_env(self, node: Dict[str, Any], cluster_meta: Dict[str, Any]) -> Dict[str, str]:
        env: Dict[str, str] = {}
        try:
            # node.inline proxy
            proxy_cfg = node.get("proxy", {}) or {}
            for k in ("http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"):
                if proxy_cfg.get(k):
                    env[k] = str(proxy_cfg.get(k))
            
            # metadata override by node name / ssh_url
            meta_map = (cluster_meta or {}).get("node_proxy_env", {}) if isinstance(cluster_meta, dict) else {}
            name = node.get("name")
            ssh_url = node.get("ssh_url")
            for key in (ssh_url, name):
                if key and isinstance(meta_map.get(key), dict):
                    for k, v in meta_map[key].items():
                        if v is not None:
                            env[str(k)] = str(v)
        except Exception:
            pass
        return env

    def _get_head_and_workers(self, nodes: List[Dict[str, Any]]) -> Tuple[Optional[Dict[str, Any]], List[Dict[str, Any]]]:
        head: Optional[Dict[str, Any]] = None
        workers: List[Dict[str, Any]] = []
        for n in nodes:
            role = (n.get("role") or "").lower()
            if head is None and role == "head":
                head = n
            else:
                workers.append(n)
        if head is None and nodes:
            head = nodes[0]
            workers = nodes[1:]
        return head, workers

    # ---------------- Public API -----------------
    def start_cluster(self, config: Optional[Dict[str, Any]] = None, **_: Any) -> bool:
        try:
            if not isinstance(config, dict):
                console.print("❌ 无效的集群配置 (expect dict)", style="red")
                return False
            nodes = config.get("nodes", []) or []
            if not nodes:
                console.print("❌ 配置缺少 nodes", style="red")
                return False
            
            cluster_meta = config.get("_nokube_metadata", {}) or {}

            head, workers = self._get_head_and_workers(nodes)
            if not head:
                console.print("❌ 未找到 head 节点", style="red")
                return False
            
            # Start head
            if not self._start_head(head, cluster_meta):
                console.print("❌ Head 节点启动失败", style="red")
                return False
            
            # Start workers
            all_ok = True
            for w in workers:
                ok = self._start_worker(w, head, cluster_meta)
                if not ok:
                    all_ok = False
            return all_ok
        except Exception as e:
            console.print(f"❌ 启动 Ray 集群失败: {e}", style="red")
            return False
    
    # ---------------- Node starters -----------------
    def _start_head(self, head_node: Dict[str, Any], cluster_meta: Dict[str, Any]) -> bool:
        ssh_url = head_node.get("ssh_url")
        users = head_node.get("users", []) or []
        if not ssh_url or not users:
            console.print("❌ Head 节点缺少 ssh_url 或 users", style="red")
            return False
        user = users[0]
        username = user.get("userid", "root")
        password = user.get("password")
        host, port = self._parse_ssh_url(ssh_url)

        env = self._merge_proxy_env(head_node, cluster_meta)

        # Upload remote lib and ensure ray
        if not self.remote_executor.upload_remote_lib(host, port, username, password, str(self.remote_lib_path), env=env):
            return False
            
        # Configure docker daemon proxy if provided
        self.remote_executor.configure_docker_daemon_proxy(host, port, username, password, proxy_env=env)

        # Build ray head args
        ray_cfg = head_node.get("ray_config", {}) or {}
        ray_port = int(ray_cfg.get("port", 10001))
        dashboard_port = int(ray_cfg.get("dashboard_port", 8265))
        num_cpus = int(ray_cfg.get("num_cpus", 4))
        object_store_memory = int(ray_cfg.get("object_store_memory", 1_000_000_000))
        args = [
                f"--port={ray_port}",
                f"--dashboard-port={dashboard_port}",
                f"--num-cpus={num_cpus}",
                f"--object-store-memory={object_store_memory}",
            f"--node-ip-address={host}",
        ]

        ok = self.remote_executor.execute_ray_command_with_logging(
            host, port, username, password,
            command="start-head", ray_args=args, realtime_output=True,
            logtag=f"head_{head_node.get('name','head')}", env=env,
        )
        if ok:
                console.print("✅ Head 节点启动成功", style="green")
                console.print(f"  Dashboard: http://{host}:{dashboard_port}", style="cyan")
        return ok

    def _start_worker(self, worker_node: Dict[str, Any], head_node: Dict[str, Any], cluster_meta: Dict[str, Any]) -> bool:
        ssh_url = worker_node.get("ssh_url")
        users = worker_node.get("users", []) or []
        if not ssh_url or not users:
            console.print("❌ Worker 节点缺少 ssh_url 或 users", style="red")
            return False
        user = users[0]
        username = user.get("userid", "root")
        password = user.get("password")
        host, port = self._parse_ssh_url(ssh_url)

        env = self._merge_proxy_env(worker_node, cluster_meta)

        # Upload remote lib and ensure ray
        if not self.remote_executor.upload_remote_lib(host, port, username, password, str(self.remote_lib_path), env=env):
            return False
    
        # Build worker args
        head_host, _ = self._parse_ssh_url(head_node.get("ssh_url"))
        head_ray_port = int((head_node.get("ray_config", {}) or {}).get("port", 10001))
        args = [
            f"--head-address={head_host}:{head_ray_port}",
            f"--num-cpus={int((worker_node.get('ray_config') or {}).get('num_cpus', 2))}",
            f"--object-store-memory={int((worker_node.get('ray_config') or {}).get('object_store_memory', 500_000_000))}",
        ]

        ok = self.remote_executor.execute_ray_command_with_logging(
            host, port, username, password,
            command="start-worker", ray_args=args, realtime_output=True,
            logtag=f"worker_{worker_node.get('name','worker')}", env=env,
        )
        if ok:
            console.print(f"✅ Worker 节点 {worker_node.get('name','')} 启动成功", style="green")
        return ok


__all__ = ["RayClusterManager"]

