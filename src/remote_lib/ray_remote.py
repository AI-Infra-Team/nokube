#!/usr/bin/env python3
"""
Ray remote execution script
Start and manage Ray processes on remote nodes
"""

import os
import sys
import shutil
import time
import argparse
import json
import getpass
import logging
from pathlib import Path
from typing import Tuple, Optional, List, Dict

# Global logging setup
_log_file = None
_original_print = print

def setup_logging(log_file: str = None, enable_file_logging: bool = False):
    """Setup logging to both console and file"""
    global _log_file
    _log_file = log_file
    
    if enable_file_logging and log_file:
        # Ensure log directory exists
        log_dir = os.path.dirname(log_file)
        if log_dir and not os.path.exists(log_dir):
            os.makedirs(log_dir, exist_ok=True)
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(log_file, mode='a'),
                logging.StreamHandler(sys.stdout)
            ]
        )
        
        # Override print function to also write to log file
        def enhanced_print(*args, **kwargs):
            # Call original print
            _original_print(*args, **kwargs)
            
            # Also write to log file
            if _log_file:
                try:
                    with open(_log_file, 'a', encoding='utf-8') as f:
                        # Convert args to string like print does
                        sep = kwargs.get('sep', ' ')
                        end = kwargs.get('end', '\n')
                        message = sep.join(str(arg) for arg in args) + end
                        f.write(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}")
                        f.flush()
                except Exception:
                    pass  # Don't let logging errors break the main functionality
        
        # Replace the global print function
        import builtins
        builtins.print = enhanced_print

def log_info(message: str):
    """Log info message"""
    if _log_file:
        logging.info(message)
    else:
        print(message)

def log_error(message: str):
    """Log error message"""
    if _log_file:
        logging.error(message)
    else:
        print(message)

# 导入工具函数
try:
    from .utils import is_root, get_sudo_prefix, execute_command, check_command_exists, escape_shell_command
except ImportError:
    # 如果无法导入，使用本地实现
    def is_root() -> bool:
        return os.geteuid() == 0
    
    def get_sudo_prefix() -> str:
        return "sudo " if not is_root() else ""
    
    def execute_command(command: str, capture_output: bool = True, timeout: Optional[int] = None) -> Tuple[bool, str, str]:
        import subprocess
        if not is_root() and not command.startswith("sudo"):
            sudo_commands = ["mkdir", "rm", "chmod", "chown", "systemctl", "service", "apt", "yum", "dnf"]
            if any(cmd in command for cmd in sudo_commands):
                command = f"sudo {command}"
        
        try:
            cmd_parts = command.split()
            if not shutil.which(cmd_parts[0]):
                return False, "", f"Command not found: {cmd_parts[0]}"
            
            if capture_output:
                result = subprocess.run(
                    command.split(),
                    capture_output=True,
                    text=True,
                    timeout=timeout
                )
                return result.returncode == 0, result.stdout, result.stderr
            else:
                exit_code = os.system(command)
                return exit_code == 0, "", ""
                
        except Exception as e:
            return False, "", str(e)
    
    def check_command_exists(command: str) -> bool:
        return shutil.which(command) is not None
    
    def escape_shell_command(command: str) -> str:
        """转义 shell 命令中的特殊字符"""
        special_chars = ['[', ']', '*', '?', '{', '}', '(', ')', '|', '&', ';', '<', '>', '`', '$', '\\']
        escaped_command = command
        for char in special_chars:
            escaped_command = escaped_command.replace(char, f'\\{char}')
        return escaped_command


class RayRemoteManager:
    """Ray Remote Manager"""
    
    def __init__(self) -> None:
        self.ray_process = None
        self.pid_file = "/tmp/ray.pid"
        self.log_file = "/tmp/ray.log"
    
    def _install_ray_if_needed(self) -> bool:
        """Install Ray if needed"""
        try:
            # Check if Ray CLI exists in PATH (handles user installs at ~/.local/bin)
            ok, out, err = self.run_cmd_with_result("command -v ray")
            if ok and out.strip():
                print(f"✅ Ray is already installed at: {out.strip()}")
                return True
            # Fallback: try import ray in current python
            ok, out, err = self.run_cmd_with_result("python3 - <<'PY'\nimport ray, sys\nprint(getattr(ray, '__version__', 'unknown'))\nPY")
            if ok:
                print(f"✅ Ray python package present (version: {out.strip()})")
                return True
            
            print("📦 Ray not installed, starting installation...")
            
            # Check Python and pip
            success, stdout, stderr = self.run_cmd_with_result("python3 --version")
            if not success:
                print("❌ Python3 is not available")
                return False
            
            # Check pip using python -m pip
            success, stdout, stderr = self.run_cmd_with_result("python3 -m pip --version")
            if not success:
                print("❌ python3 -m pip is not available")
                return False
            
            # Install Ray to user site so CLI lands in ~/.local/bin
            print("🔧 Installing Ray...")
            install_cmd = "python3 -m pip install --user -U 'ray[default]'"
            print(install_cmd)
            success = self.run_cmd_with_progress(install_cmd)
            
            if success:
                print("✅ Ray installation successful")
                # Show where ray is located
                found, stdout, stderr = self.run_cmd_with_result("command -v ray")
                if found and stdout.strip():
                    print(f"🔎 ray found at: {stdout.strip()}")
                return True
            else:
                print("❌ Ray installation failed")
                return False
                
        except Exception as e:
            print(f"❌ Error occurred during Ray installation: {e}")
            return False
    
    def start_head(self, port: int = 10001, dashboard_port: int = 8265, num_cpus: int = 4, object_store_memory: int = 1000000000, node_ip_address: Optional[str] = None) -> bool:
        """Start Ray Head node"""
        try:
            print(f"🚀 Starting Ray Head node (Port: {port}, Dashboard: {dashboard_port})")
            
            # Check and install Ray
            if not self._install_ray_if_needed():
                print("❌ Unable to install Ray, startup failed")
                return False
            
            # Idempotent: if already running, treat as success
            try:
                if self.is_running():
                    print("✅ Ray head is already running; skipping start")
                    return True
            except Exception:
                pass
            # Show which ray and version on remote
            ok_path, out_path, _ = self.run_cmd_with_result("which ray")
            if ok_path and out_path.strip():
                print(f"🔎 which ray: {out_path.strip()}")
            ok_ver, out_ver, _ = self.run_cmd_with_result("ray --version")
            if ok_ver and out_ver.strip():
                print(f"📦 {out_ver.strip()}")
            
            # Build Ray startup command (prefer ray CLI)
            client_server_port = port + 1  # 避免与 GCS/Client 默认同端口冲突
            # 为 worker 指定不冲突的端口范围，避开 client_server_port
            min_worker_port = client_server_port + 1
            max_worker_port = min_worker_port + 999
            cmd = [
                "ray", "start", "--head",
                f"--port={port}",
                f"--ray-client-server-port={client_server_port}",
                f"--min-worker-port={min_worker_port}",
                f"--max-worker-port={max_worker_port}",
                f"--dashboard-port={dashboard_port}",
                f"--dashboard-host=0.0.0.0",
                f"--num-cpus={num_cpus}",
                f"--object-store-memory={object_store_memory}",
                "--temp-dir=/tmp/ray"
            ]
            if node_ip_address:
                cmd.insert(3, f"--node-ip-address={node_ip_address}")
            
            print(f"Executing command: {' '.join(cmd)}")
            res = self.run_cmd_with_progress(" ".join(cmd))

            if not res:
                # If failing because already running, still treat as success
                try:
                    if self.is_running():
                        print("✅ Ray appears to be already running; considering start successful")
                        return True
                except Exception:
                    pass
            return res
        except Exception as e:
            print(f"❌ Failed to start Ray Head node: {e}")
            return False
    
    def start_worker(self, head_address: str, num_cpus: int = 2, object_store_memory: int = 500000000) -> bool:
        """Start Ray Worker node"""
        try:
            print(f"🔧 Starting Ray Worker node (connecting to: {head_address})")
            
            # Check and install Ray
            if not self._install_ray_if_needed():
                print("❌ Unable to install Ray, startup failed")
                return False
            
            # Idempotent: if ray process already running, skip
            try:
                if self.is_running():
                    print("✅ Ray worker is already running; skipping start")
                    return True
            except Exception:
                pass
            # Show which ray and version on remote
            ok_path, out_path, _ = self.run_cmd_with_result("which ray")
            if ok_path and out_path.strip():
                print(f"🔎 which ray: {out_path.strip()}")
            ok_ver, out_ver, _ = self.run_cmd_with_result("ray --version")
            if ok_ver and out_ver.strip():
                print(f"📦 {out_ver.strip()}")
            
            # Build Ray startup command (prefer ray CLI)
            cmd = [
                "ray", "start",
                f"--address={head_address}",
            ]
            
            print(f"Executing command: {' '.join(cmd)}")
            ok = self.run_cmd_with_progress(" ".join(cmd))
            if not ok:
                try:
                    if self.is_running():
                        print("✅ Ray appears to be already running; considering start successful")
                        return True
                except Exception:
                    pass
            return ok
                
        except Exception as e:
            print(f"❌ Failed to start Ray Worker node: {e}")
            return False
    
    def stop(self) -> bool:
        """Stop Ray process"""
        try:
            print("🛑 Stopping Ray process")
            return self.run_cmd_with_progress("ray stop")
            
        except Exception as e:
            print(f"❌ Failed to stop Ray process: {e}")
            return False
    
    def status(self) -> bool:
        """Check Ray status"""
        try:
            # Directly run status (ray CLI) and return result
            return self.run_cmd_with_progress("ray status")
                
        except Exception as e:
            print(f"❓ Status check failed: {e}")
            return False
    
    def is_running(self) -> bool:
        """Check if Ray is running"""
        try:
            success, stdout, stderr = self.run_cmd_with_result("ray status")
            return success
            
        except Exception:
            return False
    
    def get_logs(self, lines: int = 100) -> str:
        """Get logs"""
        try:
            if os.path.exists(self.log_file):
                with open(self.log_file, 'r') as f:
                    log_lines = f.readlines()
                    return ''.join(log_lines[-lines:])
            else:
                return "Log file does not exist"
                
        except Exception as e:
            return f"Failed to get logs: {e}"

    # ---------------- NodeConfigActor (daemonset-like) -----------------
    # Keep method for backwards compatibility; prefer CLI subcommand
    def start_config_actor(self, interval_sec: int = 60, extra_env: Optional[dict] = None) -> bool:
        try:
            env = {**(extra_env or {})}
            # 直接调用 CLI 子命令。幂等逻辑在子命令内部完成（基于命名空间与具名 actor）
            return self.run_cmd_with_progress(
                f"python3 /tmp/remote_lib/ray_remote.py start-config-actor --interval {int(interval_sec)}",
                extra_env=env,
            )
        except Exception as e:
            print(f"❌ start_config_actor failed: {e}")
            return False

    def run_cmd_with_progress(self, command: str, timeout: Optional[int] = None, extra_env: Optional[dict] = None) -> bool:
        """Execute command with real-time progress display. Supports injecting env via extra_env."""
        # Check if sudo is needed
        if not is_root() and not command.startswith("sudo"):
            sudo_commands = ["mkdir", "rm", "chmod", "chown", "systemctl", "service", "apt", "yum", "dnf"]
            if any(cmd in command for cmd in sudo_commands):
                command = f"sudo {command}"
        
        try:
            # Build env: copy current environ and ensure ~/.local/bin is in PATH
            import subprocess, os
            env = os.environ.copy()
            if extra_env:
                env.update(extra_env)
            home_dir = env.get("HOME", os.path.expanduser("~"))
            local_bin = os.path.join(home_dir, ".local", "bin")
            current_path = env.get("PATH", "")
            if local_bin not in current_path.split(":"):
                env["PATH"] = f"{local_bin}:{current_path}" if current_path else local_bin
            
            print(f"🔧 Executing command: {command}")
            
            # Use subprocess.run for real-time output
            result = subprocess.run(command, text=True, timeout=timeout, shell=True, env=env)
            
            success = result.returncode == 0
            
            if success:
                print(f"✅ Command executed successfully (exit code: {result.returncode})")
            else:
                print(f"❌ Command failed (exit code: {result.returncode})")
            
            return success
                
        except subprocess.TimeoutExpired:
            print("❌ Command execution timeout")
            return False
        except Exception as e:
            print(f"❌ Command execution exception: {e}")
            return False

    def run_cmd_with_result(self, command: str, timeout: Optional[int] = None, extra_env: Optional[dict] = None) -> Tuple[bool, str, str]:
        """Execute command and capture output for result processing. Supports injecting env via extra_env."""
        # Check if sudo is needed
        if not is_root() and not command.startswith("sudo"):
            sudo_commands = ["mkdir", "rm", "chmod", "chown", "systemctl", "service", "apt", "yum", "dnf"]
            if any(cmd in command for cmd in sudo_commands):
                command = f"sudo {command}"
        
        try:
            import subprocess, os
            env = os.environ.copy()
            if extra_env:
                env.update(extra_env)
            home_dir = env.get("HOME", os.path.expanduser("~"))
            local_bin = os.path.join(home_dir, ".local", "bin")
            current_path = env.get("PATH", "")
            if local_bin not in current_path.split(":"):
                env["PATH"] = f"{local_bin}:{current_path}" if current_path else local_bin
            result = subprocess.run(command, capture_output=True, text=True, timeout=timeout, shell=True, env=env)
            
            success = result.returncode == 0
            return success, result.stdout, result.stderr
                
        except subprocess.TimeoutExpired:
            return False, "", "Command execution timeout"
        except Exception as e:
            return False, "", str(e)
    
    def ensure_system_config_daemonset(self) -> bool:
        """Ensure system config DaemonSet in Ray cluster"""
        try:
            # Import necessary modules from project root
            import sys
            import os
            
            # Add project src to path so we can import from src.*
            project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
            if project_root not in sys.path:
                sys.path.insert(0, project_root)
            
            import ray
            # Connect to local Ray cluster
            ray.init(ignore_reinit_error=True, namespace="nokube")
            
            # Import controller classes
            from src.ray_kube_controller import KubeControllerActor
            from src.actor_utils import ensure_actor
            
            # Get or create KubeControllerActor
            controller = ensure_actor(
                KubeControllerActor,
                "kube-controller", 
                namespace="nokube",
                detached=True,
                replace_existing=False,
                ctor_args=(),
                stop_method="stop",
                stop_timeout=10,
            )
            
            # Call apply with empty resources to trigger system config DaemonSet ensure
            ray.get(controller.apply.remote([]))
            print("✅ System config DaemonSet ensured successfully")
            return True
            
        except Exception as e:
            print(f"❌ Failed to ensure system config DaemonSet: {e}")
            import traceback
            traceback.print_exc()
            return False


def main() -> None:
    """Main function"""
    parser = argparse.ArgumentParser(description="Ray remote execution script")
    
    # Add global logging arguments
    parser.add_argument("--log-file", type=str, help="Log file path for output logging")
    parser.add_argument("--enable-file-logging", action="store_true", help="Enable file logging")
    
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # start-head command
    start_head_parser = subparsers.add_parser("start-head", help="Start Head node")
    start_head_parser.add_argument("--port", type=int, default=10001, help="Ray port")
    start_head_parser.add_argument("--dashboard-port", type=int, default=8265, help="Dashboard port")
    start_head_parser.add_argument("--num-cpus", type=int, default=4, help="Number of CPUs")
    start_head_parser.add_argument("--object-store-memory", type=int, default=1000000000, help="Object store memory")
    start_head_parser.add_argument("--node-ip-address", type=str, default=None, help="Bind node IP address explicitly")
    
    # start-worker command
    start_worker_parser = subparsers.add_parser("start-worker", help="Start Worker node")
    start_worker_parser.add_argument("--head-address", required=True, help="Head node address")
    start_worker_parser.add_argument("--num-cpus", type=int, default=2, help="Number of CPUs")
    start_worker_parser.add_argument("--object-store-memory", type=int, default=500000000, help="Object store memory")
    start_worker_parser.add_argument("--node-ip-address", type=str, default=None, help="Bind node IP address explicitly")
    
    # stop command
    subparsers.add_parser("stop", help="Stop Ray process")
    
    # status command
    subparsers.add_parser("status", help="Check Ray status")
    # start-config-actor command
    sca_parser = subparsers.add_parser("start-config-actor", help="Start NodeConfigActor on this node")
    sca_parser.add_argument("--interval", type=int, default=60, help="Sync interval seconds")
    
    # logs command
    logs_parser = subparsers.add_parser("logs", help="Get logs")
    logs_parser.add_argument("--lines", type=int, default=100, help="Number of log lines")
    
    # _check_ray command (internal use)
    subparsers.add_parser("_check_ray", help="Check if Ray is installed")
    
    # _install_ray command (internal use)
    subparsers.add_parser("_install_ray", help="Install Ray")
    
    # ensure-system-config command
    subparsers.add_parser("ensure-system-config", help="Ensure system config DaemonSet")
    
    args = parser.parse_args()
    
    # Setup logging if requested
    if args.log_file or args.enable_file_logging:
        log_file = args.log_file or "/tmp/ray_remote.log"
        setup_logging(log_file, args.enable_file_logging)
        print(f"📝 Logging enabled to: {log_file}")
    
    if not args.command:
        parser.print_help()
        return
    
    manager = RayRemoteManager()
    
    if args.command == "start-head":
        success = manager.start_head(
            port=args.port,
            dashboard_port=args.dashboard_port,
            num_cpus=args.num_cpus,
            object_store_memory=args.object_store_memory,
            node_ip_address=args.node_ip_address
        )
        sys.exit(0 if success else 1)
    
    elif args.command == "start-worker":
        success = manager.start_worker(
            head_address=args.head_address,
            num_cpus=args.num_cpus,
            object_store_memory=args.object_store_memory,
        )
        sys.exit(0 if success else 1)
    
    elif args.command == "stop":
        success = manager.stop()
        sys.exit(0 if success else 1)
    
    elif args.command == "status":
        success = manager.status()
        sys.exit(0 if success else 1)
    
    elif args.command == "logs":
        logs = manager.get_logs(args.lines)
        print(logs)
        sys.exit(0)
    
    elif args.command == "_check_ray":
        # Check if Ray is installed via CLI in PATH and print version
        mgr = RayRemoteManager()
        ok_path, out_path, _ = mgr.run_cmd_with_result("command -v ray")
        ok_ver, out_ver, err_ver = mgr.run_cmd_with_result("ray --version")
        if ok_ver:
            print("Ray is installed")
            if ok_path and out_path.strip():
                print(out_path.strip())
            if out_ver.strip():
                print(out_ver.strip())
            sys.exit(0)
        else:
            print("Ray is not installed")
            if err_ver:
                print(err_ver.strip())
            sys.exit(1)
    
    elif args.command == "_install_ray":
        # Install Ray
        success = manager._install_ray_if_needed()
        sys.exit(0 if success else 1)
    elif args.command == "start-config-actor":
        # Implement NodeConfigActor in this script
        # Lazy import to avoid overhead
        try:
            import os, socket, time, json, subprocess
            # Ensure required python libs and protobuf version (pin to avoid descriptor errors)
            pkgs = [
                "tenacity==6.1.0",
                "etcd3==0.12.0",
                "protobuf<=3.20.4",
            ]
            manager.run_cmd_with_progress("python3 -m pip install --user " + " ".join(f"'{p}'" for p in pkgs))
            try:
                import etcd3  # type: ignore
            except Exception:
                print("❌ etcd3 import still failing after install")
                sys.exit(1)

            import ray
            from ray.util.scheduling_strategies import NodeAffinitySchedulingStrategy

            NODE_NAME = socket.gethostname()
            INTERVAL = int(args.interval)
            ETCD_HOSTS = os.environ.get('NOKUBE_ETCD_HOSTS', '127.0.0.1:2379')
            ETCD_USER = os.environ.get('NOKUBE_ETCD_USER')
            ETCD_PASS = os.environ.get('NOKUBE_ETCD_PASS')
            ETCD_PREFIX = os.environ.get('NOKUBE_ETCD_PREFIX', '/nokube')

            def _get_etcd_client():
                host = ETCD_HOSTS.split(',')[0].split(':')[0]
                port = int(ETCD_HOSTS.split(',')[0].split(':')[1])
                kwargs = {'host': host, 'port': port}
                if ETCD_USER:
                    kwargs['user'] = ETCD_USER
                if ETCD_PASS:
                    kwargs['password'] = ETCD_PASS
                try:
                    return etcd3.client(**kwargs)
                except Exception:
                    return None

            def _mask(env_map: Dict[str, str]):
                items = []
                try:
                    from urllib.parse import urlparse
                    for key in ('http_proxy','https_proxy','no_proxy','HTTP_PROXY','HTTPS_PROXY','NO_PROXY'):
                        val = env_map.get(key)
                        if not val:
                            continue
                        if key.lower()=="no_proxy":
                            parts=[s.strip() for s in str(val).split(',') if s.strip()]
                            preview=','.join(parts[:5]); suf='' if len(parts)<=5 else f"(+{len(parts)-5})"
                            items.append(f"{key}={preview}{suf}")
                        else:
                            try:
                                p=urlparse(str(val)); host=p.hostname or ''; port=f":{p.port}" if p.port else ''
                                items.append(f"{key}={p.scheme}://{host}{port}")
                            except Exception:
                                items.append(f"{key}={val}")
                except Exception:
                    pass
                return items

            @ray.remote
            class _NodeCfg:
                def __init__(self, node_name: str, interval: int):
                    self.node_name=node_name
                    self.interval=interval
                    self.last_env={}
                    self.etcd=_get_etcd_client()

                def _fetch(self) -> Dict[str,str]:
                    try:
                        if not self.etcd:
                            return {}
                        key=f"{ETCD_PREFIX}/clusters"
                        val,_ = self.etcd.get(key)
                        if not val:
                            return {}
                        clusters=json.loads(val.decode('utf-8'))
                        for c in clusters:
                            meta=c.get('metadata') or {}
                            mapping=meta.get('node_proxy_env') or {}
                            if self.node_name in mapping and isinstance(mapping[self.node_name], dict):
                                env={}
                                for k in ('http_proxy','https_proxy','no_proxy','HTTP_PROXY','HTTPS_PROXY','NO_PROXY'):
                                    v=mapping[self.node_name].get(k)
                                    if v:
                                        env[k]=str(v)
                                return env
                        return {}
                    except Exception:
                        return {}

                def _apply(self, env_map: Dict[str,str]) -> bool:
                    try:
                        if env_map==self.last_env:
                            return True
                        daemon_dir='/etc/docker'
                        daemon_file=f"{daemon_dir}/daemon.json"
                        # read
                        r=subprocess.run(["bash","-lc",f"cat {daemon_file}"], capture_output=True, text=True)
                        try:
                            cur=json.loads(r.stdout) if r.returncode==0 and r.stdout.strip() else {}
                        except Exception:
                            cur={}
                        cur.setdefault('proxies',{})
                        pr=cur['proxies']
                        def n(key):
                            return env_map.get(key) or env_map.get(key.upper()) or env_map.get(key.lower())
                        hp=n('http_proxy'); sp=n('https_proxy'); np=n('no_proxy')
                        if hp: pr['http-proxy']=hp
                        elif 'http-proxy' in pr: pr.pop('http-proxy')
                        if sp: pr['https-proxy']=sp
                        elif 'https-proxy' in pr: pr.pop('https-proxy')
                        if np: pr['no-proxy']=np
                        elif 'no-proxy' in pr: pr.pop('no-proxy')
                        tmp='/tmp/daemon.json'; open(tmp,'w').write(json.dumps(cur,indent=2))
                        pref=[] if os.geteuid()==0 else ['sudo','-E','-n']
                        for c in [f"mkdir -p {daemon_dir}", f"install -o root -g root -m 644 {tmp} {daemon_file}", "systemctl restart docker || service docker restart || true"]:
                            subprocess.run(pref+["bash","-lc",c],text=True)
                        self.last_env=dict(env_map)
                        print("CFG | updated:", ', '.join(_mask(env_map)))
                        return True
                    except Exception as e:
                        print("CFG | apply failed:", e)
                        return False

                def run(self):
                    print(f"CFG | NodeConfigActor running on {self.node_name}, interval={self.interval}s")
                    while True:
                        env = self._fetch()
                        try:
                            masked = ', '.join(_mask(env)) if env else '(empty)'
                        except Exception:
                            masked = '(unprintable)'
                        print(f"CFG | current: {masked}")
                        if env:
                            if env != self.last_env:
                                self._apply(env)
                            else:
                                print("CFG | no change")
                        # no env -> just print current and sleep
                        time.sleep(self.interval)

            NS=os.environ.get('NOKUBE_NAMESPACE','nokube')
            # 优先 RAY_ADDRESS（由上游注入），否则 auto
            addr=os.environ.get('RAY_ADDRESS') or 'auto'
            ray.init(address=addr, namespace=NS)
            nid=ray.get_runtime_context().get_node_id()
            name=f"nokube-config-{NODE_NAME}"
            try:
                a=ray.get_actor(name, namespace=NS)
                print("CFG | actor already exists:", name)
            except Exception:
                a=_NodeCfg.options(name=name, lifetime="detached", scheduling_strategy=NodeAffinitySchedulingStrategy(node_id=nid, soft=True)).remote(NODE_NAME, INTERVAL)
                a.run.remote()
            print("CFG | ensure done for:", name)
            sys.exit(0)
        except Exception as e:
            print(f"❌ failed to start-config-actor: {e}")
            sys.exit(1)
    
    elif args.command == "ensure-system-config":
        # Ensure system config DaemonSet
        success = manager.ensure_system_config_daemonset()
        sys.exit(0 if success else 1)


if __name__ == "__main__":
    main() 