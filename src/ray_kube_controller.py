#!/usr/bin/env python3
"""
Ray Deployment/Pod/Container 模型
- DeploymentActor：管理 Pod 子 Actor
- PodActor：管理 Container 子 Actor
- ContainerActor：执行 docker run （最小实现）
"""

from typing import Any, Dict, List, Optional
import uuid
import re
import time
import subprocess
import threading
import sys
import json
import os
from urllib.parse import urlparse

import ray

# 等等类内部按需懒加载 etcd 依赖，避免 worker 导入期因缺包失败


def _short_id(length: int = 6) -> str:
    return uuid.uuid4().hex[:length]


def _safe_name(value: str) -> str:
    if not value:
        return "noname"
    return re.sub(r"[^a-zA-Z0-9_-]", "-", value)


def _compose_name(prefix: str, parts: List[str], max_len: int = 60) -> str:
    base = prefix + "-" + "-".join(_safe_name(p) for p in parts if p)
    if len(base) <= max_len:
        return base
    suffix = _short_id()
    # leave room for prefix + '-' + '-' + suffix
    budget = max_len - len(prefix) - 2 - len(suffix)
    segments = [p for p in parts if p]
    if budget <= 0 or not segments:
        return f"{prefix}-{suffix}"
    per = max(1, budget // len(segments))
    compact = "-".join(_safe_name(s)[:per] for s in segments)
    return f"{prefix}-{compact}-{suffix}"


def _compose_container_actor_name(ns: str, pod: str, container_name: str) -> str:
    """稳定的 ContainerActor 命名：基于 namespace/pod/container 三段。
    采用固定命名，便于分布式幂等与去重。"""
    return "ctr-" + "-".join([
        _safe_name(ns),
        _safe_name(pod),
        _safe_name(container_name),
    ])


def _workload_segment(name: Optional[str]) -> str:
    """返回工作负载段名；为空时使用 'standalone'。"""
    try:
        return _safe_name(name) if name else "standalone"
    except Exception:
        return "standalone"


def _log(prefix: str, message: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {prefix} | {message}", flush=True)


def _try_create_etcd():
    try:
        from src.etcd_manager import EtcdManager  # type: ignore
        return EtcdManager()
    except Exception:
        return None


def _resolve_node_proxy_env() -> Dict[str, str]:
    """解析当前 Actor 所在节点的代理环境（来自 etcd 元数据）。

    约定：在创建 Actor 前设置宿主节点标识到当前进程环境（NOKUBE_NODE_NAME 或 NOKUBE_NODE_SSH_URL），
    则在 etcd 的 metadata.node_proxy_env 中用该键查找对应的 env；
    否则返回空。
    """
    try:
        # 源自 Pod/Deployment 控制器可选注入的宿主节点键
        node_key = None
        # 允许通过容器 env 显式传入
        # 注意：此函数由 _build_env_dict 调用，此时容器 env 尚未合并，需从 self.container 读取
        # 因为这里是模块级函数，无法访问 self.container，改为读取进程环境中的 hint
        # 外层若有能力可预先导出到 os.environ
        # 优先使用节点 name
        node_key = os.environ.get("NOKUBE_NODE_NAME") or os.environ.get("NOKUBE_NODE_SSH_URL")
        if not node_key:
            return {}

        etcd = _try_create_etcd()
        if not etcd:
            return {}
        # 读取 clusters 列表，遍历 metadata.node_proxy_env
        clusters = etcd.list_clusters() or []
        for c in clusters:
            meta = (c or {}).get('metadata') or {}
            mapping = meta.get('node_proxy_env') or {}
            if not isinstance(mapping, dict):
                continue
            # 直接按 ssh_url 命中
            if node_key in mapping and isinstance(mapping[node_key], dict):
                # 仅返回常见代理键
                env = {}
                for key in ("http_proxy","https_proxy","no_proxy","HTTP_PROXY","HTTPS_PROXY","NO_PROXY"):
                    val = mapping[node_key].get(key)
                    if val:
                        env[key] = str(val)
                return env
        return {}
    except Exception:
        return {}


def _mask_proxy_items(env_map: Dict[str, str]) -> List[str]:
    """将代理相关环境变量脱敏为可读摘要。"""
    items: List[str] = []
    try:
        for key in ("http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"):
            val = env_map.get(key)
            if not val:
                continue
            if key.lower() == "no_proxy":
                parts = [s.strip() for s in str(val).split(',') if s.strip()]
                preview = ','.join(parts[:5])
                suffix = '' if len(parts) <= 5 else f" (+{len(parts)-5})"
                items.append(f"{key}={preview}{suffix}")
            else:
                try:
                    p = urlparse(str(val))
                    if p.scheme and (p.hostname or p.netloc):
                        host = p.hostname or ''
                        port = f":{p.port}" if p.port else ''
                        items.append(f"{key}={p.scheme}://{host}{port}")
                    else:
                        sval = str(val)
                        items.append(f"{key}={sval[:120]}{'' if len(sval) <= 120 else '…'}")
                except Exception:
                    items.append(f"{key}={val}")
    except Exception:
        pass
    return items


@ray.remote
class NodeConfigActor:
    """每节点常驻的配置 Actor：根据 etcd 元数据为本节点下发系统配置（如 Docker 代理）。"""

    def __init__(self, interval_sec: int = 60):
        self.interval_sec = max(10, int(interval_sec))
        self._stop = False
        self._thread: Optional[threading.Thread] = None
        self._etcd = _try_create_etcd()
        # 识别本节点名称（使用主机名）
        try:
            import socket
            self.node_name = socket.gethostname()
        except Exception:
            self.node_name = os.environ.get("NOKUBE_NODE_NAME", "")
        self._last_applied: Dict[str, str] = {}

    def _fetch_proxy_env(self) -> Dict[str, str]:
        try:
            if not self._etcd:
                return {}
            clusters = self._etcd.list_clusters() or []
            for c in clusters:
                meta = (c or {}).get('metadata') or {}
                mapping = meta.get('node_proxy_env') or {}
                if not isinstance(mapping, dict):
                    continue
                # 支持通过节点 name 直取
                for key in (self.node_name, os.environ.get("NOKUBE_NODE_SSH_URL", "")):
                    if key and key in mapping and isinstance(mapping[key], dict):
                        env = {}
                        for k in ("http_proxy","https_proxy","no_proxy","HTTP_PROXY","HTTPS_PROXY","NO_PROXY"):
                            v = mapping[key].get(k)
                            if v:
                                env[k] = str(v)
                        return env
            return {}
        except Exception:
            return {}

    def _ensure_docker_daemon_proxy(self, env_map: Dict[str, str]) -> bool:
        try:
            # 仅当有变更时应用
            if env_map == self._last_applied:
                return True

            http_p = env_map.get('http_proxy') or env_map.get('HTTP_PROXY')
            https_p = env_map.get('https_proxy') or env_map.get('HTTPS_PROXY')
            no_p = env_map.get('no_proxy') or env_map.get('NO_PROXY')
            if not any([http_p, https_p, no_p]):
                return True

            # 读取现有 /etc/docker/daemon.json
            import json as _json
            daemon_dir = "/etc/docker"
            daemon_file = f"{daemon_dir}/daemon.json"

            def _run(cmd: str) -> bool:
                try:
                    # 非 root 使用 sudo -E -n
                    prefix = [] if os.geteuid() == 0 else ["sudo","-E","-n"]
                    res = subprocess.run(prefix + ["bash","-lc", cmd], capture_output=True, text=True)
                    if res.returncode != 0:
                        _log("CFG", f"cmd fail code={res.returncode} err={(res.stderr or '').strip()[:200]}")
                        return False
                    return True
                except Exception as e:
                    _log("CFG", f"cmd exception: {e}")
                    return False

            # 读文件
            read = subprocess.run(["bash","-lc", f"cat {daemon_file}"], capture_output=True, text=True)
            try:
                current = _json.loads(read.stdout) if read.returncode == 0 and read.stdout.strip() else {}
            except Exception:
                current = {}

            if 'proxies' not in current or not isinstance(current['proxies'], dict):
                current['proxies'] = {}
            proxies = current['proxies']
            if http_p:
                proxies['http-proxy'] = http_p
            else:
                proxies.pop('http-proxy', None)
            if https_p:
                proxies['https-proxy'] = https_p
            else:
                proxies.pop('https-proxy', None)
            if no_p:
                proxies['no-proxy'] = no_p
            else:
                proxies.pop('no-proxy', None)

            tmp_json = "/tmp/daemon.json"
            data = _json.dumps(current, indent=2)
            with open(tmp_json, 'w') as f:
                f.write(data)

            # 安装并重启 docker
            steps = [
                f"mkdir -p {daemon_dir}",
                f"install -o root -g root -m 644 {tmp_json} {daemon_file}",
                "systemctl restart docker || service docker restart || true",
            ]
            ok_all = True
            for s in steps:
                if not _run(s):
                    ok_all = False
            if ok_all:
                self._last_applied = dict(env_map)
                masked = ', '.join(_mask_proxy_items(env_map))
                _log("CFG", f"docker daemon proxy updated: {masked}")
                return True
            return False
        except Exception as e:
            _log("CFG", f"apply daemon proxy failed: {e}")
            return False

    def _loop(self):
        backoff = self.interval_sec
        while not self._stop:
            try:
                env_map = self._fetch_proxy_env()
                if env_map:
                    self._ensure_docker_daemon_proxy(env_map)
            except Exception:
                pass
            for _ in range(self.interval_sec):
                if self._stop:
                    break
                time.sleep(1)

    def start(self) -> str:
        try:
            if not self._thread:
                self._thread = threading.Thread(target=self._loop, daemon=True)
                self._thread.start()
            _log("CFG", f"NodeConfigActor started on node={self.node_name}")
            return "Running"
        except Exception as e:
            return f"Error: {e}"

    def stop(self) -> str:
        try:
            self._stop = True
            if self._thread:
                self._thread.join(timeout=3)
        except Exception:
            pass
        return "Stopped"


@ray.remote
class ContainerActor:
    """容器级 Actor：最小实现，使用 docker SDK 运行容器。"""

    def __init__(self, pod_name: str, namespace: str, container: Dict[str, Any], actor_name: str, parent_deployment: Optional[str] = None):
        self.pod_name = pod_name
        self.namespace = namespace
        self.container = container
        self.actor_name = actor_name
        self.parent_deployment = parent_deployment or "standalone"
        self.status = "Pending"
        self.container_id: Optional[str] = None
        self.container_name: str = _safe_name(actor_name)
        self._proc: Optional[subprocess.Popen] = None
        self._io_thread: Optional[threading.Thread] = None
        self._stop_stream: bool = False

    def full_path(self, include_container: bool = False, include_namespace: bool = True) -> str:
        """返回容器/Pod 的稳定路径段：namespace/workload/pod[/container]
        - workload 为空时回退为 standalone
        - include_container=True 时追加容器名
        - include_namespace=False 时去掉开头 namespace 段
        """
        ns = _safe_name(self.namespace)
        workload = _workload_segment(self.parent_deployment)
        pod = _safe_name(self.pod_name)
        parts = [ns, workload, pod] if include_namespace else [workload, pod]
        if include_container:
            cname = _safe_name(self.container.get("name", "container"))
            parts.append(cname)
        return "/".join(parts)

    def start(self, attempts: int = 5, backoff_seconds: float = 3.0, stream_logs: bool = True) -> Dict[str, Any]:
        # 开头打印当前进程可见的代理环境摘要
        try:
            proxy_items = _mask_proxy_items(os.environ)
            if proxy_items:
                _log(f"CTR {self.actor_name}", f"proxy env: {', '.join(proxy_items)}")
        except Exception:
            pass
        def _sudo_prefix() -> List[str]:
            try:
                return [] if os.geteuid() == 0 else ["sudo", "-E", "-n"]
            except Exception:
                return ["sudo", "-E", "-n"]
        def _build_env_dict() -> Dict[str, str]:
            env_dict: Dict[str, str] = {}
            for env in self.container.get("env", []) or []:
                name = env.get("name")
                value = env.get("value")
                if name is not None and value is not None:
                    env_dict[name] = value
            # 合并来自集群元数据的节点代理环境
            try:
                proxy_env = _resolve_node_proxy_env()
                if proxy_env:
                    env_dict.update(proxy_env)
            except Exception:
                pass
            return env_dict

        def _build_ports_list() -> List[str]:
            port_args: List[str] = []
            for p in self.container.get("ports", []) or []:
                cport = p.get("containerPort")
                proto = (p.get("protocol") or "tcp").lower()
                if cport:
                    # 使用随机宿主端口发布容器端口
                    port_val = f"{cport}/{proto}" if proto in ("tcp", "udp") else str(cport)
                    port_args.extend(["-p", port_val])
            return port_args

        def _build_volume_args() -> List[str]:
            vol_args: List[str] = []
            # 将 K8s 风格 volumeMounts 映射到 docker -v host:container
            mounts = self.container.get("volumeMounts", []) or []
            # 以 namespace/workload/pod 解析到宿主路径，并为每个容器形成唯一 mount 点
            base = f"/var/lib/nokube/volumes/{self.full_path(include_container=False, include_namespace=True)}"
            for m in mounts:
                mpath = m.get("mountPath")
                vname = m.get("name")
                if not mpath or not vname:
                    continue
                pod_vol_dir = os.path.join(base, _safe_name(vname))
                try:
                    # 确保 pod 级数据目录存在；直接使用该目录进行挂载
                    os.makedirs(pod_vol_dir, exist_ok=True)
                except Exception:
                    pass
                host_dir = pod_vol_dir
                vol_args.extend(["-v", f"{host_dir}:{mpath}"])
            return vol_args

        def _build_final_cmd() -> List[str]:
            cmd = self.container.get("command") or []
            args = self.container.get("args") or []
            if isinstance(cmd, list):
                return cmd + (args if isinstance(args, list) else [])
            # 非列表命令，直接忽略 args 以避免歧义
            return cmd if cmd else []

        def _try_start() -> Dict[str, Any]:
            image = (self.container.get("image") or "").strip()
            if not image:
                raise RuntimeError("container.image 不能为空")

            # 优先 sudo + docker CLI（非交互）
            env_dict = _build_env_dict()
            port_args = _build_ports_list()
            volume_args = _build_volume_args()
            final_cmd = _build_final_cmd()

            run_args: List[str] = _sudo_prefix() + ["docker", "run", "--rm", "--name", self.container_name]
            for k, v in env_dict.items():
                run_args.extend(["-e", f"{k}={v}"])
            run_args.extend(port_args)
            run_args.extend(volume_args)
            run_args.append(image)
            run_args.extend(final_cmd)

            # 为 docker CLI 进程注入代理等环境变量（仅提取常见代理键）
            proc_env = dict(**os.environ)
            for key in ("http_proxy", "https_proxy", "no_proxy", "HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY"):
                if key in env_dict and env_dict.get(key) is not None:
                    proc_env[key] = str(env_dict.get(key))

            # 确保 no_proxy 至少包含本地常见地址，避免代理环路
            try:
                default_no_proxy = "localhost,127.0.0.1,::1"
                existing = proc_env.get('no_proxy') or proc_env.get('NO_PROXY') or ''
                if default_no_proxy not in existing:
                    merged = ','.join([p for p in [existing, default_no_proxy] if p])
                    # 保持大小写一致：优先 lower key
                    if 'no_proxy' in proc_env:
                        proc_env['no_proxy'] = merged
                    else:
                        proc_env['NO_PROXY'] = merged
            except Exception:
                pass
            # 执行并打印状态
            start_ts = time.time()
            # 先尝试移除同名遗留容器，避免名称冲突
            try:
                sp = _sudo_prefix()
                _log(f"CTR {self.actor_name}", f"docker rm -f {self.container_name}")
                pre = subprocess.run(sp + ["docker", "rm", "-f", self.container_name], capture_output=True, text=True)
                try:
                    stderr_tail = (pre.stderr or '').strip()[:300]
                    _log(f"CTR {self.actor_name}", f"pre-clean exit={pre.returncode} err={stderr_tail}")
                except Exception:
                    pass
            except Exception:
                pass
            try:
                # 前台运行并实时读取输出
                self._proc = subprocess.Popen(
                    run_args,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.STDOUT,
                    text=True,
                    env=proc_env,
                    bufsize=1,
                )
            except Exception as e:
                raise RuntimeError(str(e))

            # 启动 IO 线程，将容器输出透传到日志
            def _pump():
                try:
                    if self._proc and self._proc.stdout is not None:
                        for line in self._proc.stdout:
                            if self._stop_stream:
                                break
                            try:
                                _log(f"CTR {self.actor_name}", f"{line.rstrip()}")
                            except Exception:
                                pass
                except Exception:
                    pass
            self._stop_stream = False
            self._io_thread = threading.Thread(target=_pump, daemon=True)
            self._io_thread.start()

            # 简短等待，判断是否立即失败
            time.sleep(0.3)
            if self._proc.poll() is not None and self._proc.returncode is not None and self._proc.returncode != 0:
                dur_ms = int((time.time() - start_ts) * 1000)
                _log(f"CTR {self.actor_name}", f"docker run exited early code={self._proc.returncode} in {dur_ms}ms")
                raise RuntimeError(f"docker run exited: code={self._proc.returncode}")

            self.status = "Running"
            return {
                    "pod": self.pod_name,
                    "namespace": self.namespace,
                    "container": self.container.get("name"),
                    "actor": self.actor_name,
                    "status": self.status,
                    "image": image,
                    "container_id": self.container_id,
                    "container_name": self.container_name,
                }

        # 带重试与退避，避免权限/短暂不可用导致父 Pod 失败
        try_count = 0
        delay = backoff_seconds
        last_err: Optional[str] = None
        while try_count < attempts:
            try:
                return _try_start()
            except BaseException as e:
                msg = str(e)
                last_err = msg
                # 针对常见权限/连接错误重试，其余错误直接返回
                transient = any(
                    key in msg.lower()
                    for key in [
                        "permission denied",
                        "connection aborted",
                        "connection refused",
                        "temporary failure",
                        "timeout",
                    ]
                )
                try_count += 1
                if not transient or try_count >= attempts:
                    break
                time.sleep(delay)
                delay = min(delay * 2, 60.0)

        self.status = "Error"
        return {
            "pod": self.pod_name,
            "namespace": self.namespace,
            "container": self.container.get("name"),
            "actor": self.actor_name,
            "status": self.status,
            "error": (last_err or "unknown error"),
            "retries": try_count,
        }

    def stop(self) -> str:
        try:
            # 停止输出线程
            try:
                self._stop_stream = True
                if self._io_thread:
                    self._io_thread.join(timeout=2)
            except Exception:
                pass
            # 结束前台 docker 进程（如仍在）
            try:
                if self._proc and self._proc.poll() is None:
                    self._proc.terminate()
            except Exception:
                pass
            # 尝试用容器名强制删除（即使 --rm，失败也忽略）
            try:
                sudo_prefix = [] if os.geteuid() == 0 else ["sudo", "-E", "-n"]
            except Exception:
                sudo_prefix = ["sudo", "-E", "-n"]
            try:
                _log(f"CTR {self.actor_name}", f"docker rm -f {self.container_name}")
                proc = subprocess.run(sudo_prefix + ["docker", "rm", "-f", self.container_name], capture_output=True, text=True)
                try:
                    stderr_tail = (proc.stderr or '').strip()[:300]
                    _log(f"CTR {self.actor_name}", f"docker rm exit={proc.returncode} err={stderr_tail}")
                except Exception:
                    pass
            except Exception:
                pass
        except BaseException:
            pass
        self.status = "Stopped"
        return self.status

    def get_status(self) -> str:
        return self.status

    def alive(self) -> bool:
        """检查底层 docker 容器是否仍在运行。"""
        try:
            # 通过容器名探测
            try:
                sudo_prefix = [] if os.geteuid() == 0 else ["sudo", "-E", "-n"]
            except Exception:
                sudo_prefix = ["sudo", "-E", "-n"]
            proc = subprocess.run(
                sudo_prefix + [
                    "docker", "inspect", "-f", "{{.State.Running}}", self.container_name
                ],
                capture_output=True,
                text=True,
            )
            out = (proc.stdout or "").strip().lower()
            return proc.returncode == 0 and out == "true"
        except BaseException:
            return False



@ray.remote
class PodActor:
    """Pod 级 Actor，占位模拟运行容器；一容器一 Actor。"""

    def __init__(
        self,
        pod_name: str,
        namespace: str,
        containers: List[Dict[str, Any]],
        actor_name: str,
        spec_key: Optional[str] = None,
        parent_deployment: Optional[str] = None,
        volumes: Optional[List[Dict[str, Any]]] = None,
    ):
        self.pod_name = pod_name
        self.namespace = namespace
        self.containers = containers
        self.actor_name = actor_name
        self.status = "Pending"
        self.container_actors: List[Any] = []
        # 维护按容器名索引的单实例映射，避免重复创建
        self._actor_by_container: Dict[str, Any] = {}
        self._stop_flag = False
        self._threads: List[threading.Thread] = []
        self._status_thread: Optional[threading.Thread] = None
        self._container_status: Dict[str, Dict[str, Any]] = {}
        self.spec_key = spec_key
        self.deployment_name = parent_deployment or "standalone"
        self._etcd = _try_create_etcd()
        self._last_generation: Optional[int] = None
        # 已物化的卷缓存，避免重复创建
        self._materialized_volumes: Dict[str, bool] = {}
        # 初始卷（来自 K8s 资源 spec.volumes）
        self.initial_volumes: List[Dict[str, Any]] = list(volumes or [])

    def full_path(self, include_namespace: bool = True) -> str:
        """返回 Pod 的稳定路径段：namespace/workload/pod
        - workload 使用 deployment_name，为空回退为 standalone
        - include_namespace=False 时不含命名空间段
        """
        ns = _safe_name(self.namespace)
        workload = _workload_segment(self.deployment_name)
        pod = _safe_name(self.pod_name)
        parts = [ns, workload, pod] if include_namespace else [workload, pod]
        return "/".join(parts)

    def _supervise_container(self, container_spec: Dict[str, Any]):
        c_name = _safe_name(container_spec.get("name", "container"))
        backoff = 5.0
        from src.actor_utils import ensure_actor  # type: ignore
        ns = os.environ.get("NOKUBE_NAMESPACE", "nokube")
        while not self._stop_flag:
            # 每轮从最新 desired 列表提取此容器的当前期望 spec
            try:
                latest_spec = next((c for c in (self.containers or []) if _safe_name(c.get("name")) == c_name), None)
            except Exception:
                latest_spec = None
            if latest_spec is None:
                # 期望不存在该容器，等待并继续
                for _ in range(5):
                    if self._stop_flag:
                        break
                    time.sleep(1)
                continue
            # 如果已有同名容器 actor，优先进行存活检查，避免重复创建
            existing = self._actor_by_container.get(c_name)
            if existing is not None:
                try:
                    alive = bool(ray.get(existing.alive.remote(), timeout=3))
                except Exception:
                    alive = False
                if alive:
                    # 已在运行：延迟一段时间后复检
                    sleep_secs = 15.0
                    for _ in range(int(sleep_secs)):
                        if self._stop_flag:
                            break
                        time.sleep(1)
                    continue
                else:
                    # 旧实例已不在，尝试清理
                    try:
                        existing.stop.remote()
                    except Exception:
                        pass
                    try:
                        # 从记录中移除
                        self._actor_by_container.pop(c_name, None)
                        # 同步列表
                        self.container_actors = [a for a in self.container_actors if a is not existing]
                    except Exception:
                        pass

            # 创建/替换稳定命名的容器 actor，并尝试启动一次
            c_actor_name = _compose_container_actor_name(self.namespace, self.pod_name, c_name)
            try:
                actor = ensure_actor(
                    ContainerActor,
                    c_actor_name,
                    namespace=ns,
                    detached=False,
                    replace_existing=True,
                    ctor_args=(self.pod_name, self.namespace, latest_spec, c_actor_name, self.deployment_name),
                    stop_method="stop",
                    stop_timeout=10,
                )
                result = ray.get(actor.start.remote(attempts=3, backoff_seconds=2.0))
                self._container_status[c_name] = result
                if result.get("status") == "Running":
                    # 记录为该容器的唯一实例
                    self._actor_by_container[c_name] = actor
                    if actor not in self.container_actors:
                        self.container_actors.append(actor)
                    backoff = 10.0
                    sleep_secs = 15.0
                else:
                    # 失败时按 backoff 睡眠后重试
                    backoff = min(backoff * 2, 60.0)
                    sleep_secs = backoff
            except Exception as e:
                self._container_status[c_name] = {
                    "pod": self.pod_name,
                    "namespace": self.namespace,
                    "container": c_name,
                    "actor": c_actor_name,
                    "status": "Error",
                    "error": str(e),
                }
                backoff = min(backoff * 2, 60.0)
                sleep_secs = backoff

            # 睡眠后继续下一轮
            for _ in range(int(sleep_secs)):
                if self._stop_flag:
                    break
                time.sleep(1)

    def start(self) -> Dict[str, Any]:
        # 开头打印当前进程可见的代理环境摘要
        try:
            proxy_items = _mask_proxy_items(os.environ)
            if proxy_items:
                _log(f"POD {self.actor_name}", f"proxy env: {', '.join(proxy_items)}")
        except Exception:
            pass
        # 若存在外部规格存储（spec_key），优先在拉起容器前物化卷，避免容器启动时文件尚未就绪
        try:
            desired_volumes: List[Dict[str, Any]] = []
            if self.initial_volumes:
                desired_volumes.extend(self.initial_volumes)
            if self.spec_key and self._etcd is not None:
                data = self._etcd.get_kv(self.spec_key)
                if isinstance(data, dict):
                    desired_volumes.extend(data.get("volumes", []) or [])
            if desired_volumes:
                self._ensure_volumes(desired_volumes)
        except Exception:
            pass

        # 为每个容器启动监督线程：失败不退出，sleep 一会再重启
        self.status = "Running"
        for c in self.containers:
            t = threading.Thread(target=self._supervise_container, args=(c,), daemon=True)
            t.start()
            self._threads.append(t)
        # 启动状态打印线程，每 1s 打印一次子容器状态
        if not self._status_thread:
            self._status_thread = threading.Thread(target=self._status_loop, daemon=True)
            self._status_thread.start()
        # 初始返回当前可用的状态快照
        return self.get_status_snapshot()

    def stop(self) -> str:
        # 通知监督线程停止
        self._stop_flag = True
        for t in self._threads:
            try:
                t.join(timeout=2)
            except Exception:
                pass
        if self._status_thread:
            try:
                self._status_thread.join(timeout=2)
            except Exception:
                pass
        # 先停止容器级 Actor
        try:
            actors = list(self._actor_by_container.values()) if self._actor_by_container else self.container_actors
            if actors:
                try:
                    ray.get([a.stop.remote() for a in actors])
                except Exception:
                    pass
        except Exception:
            pass
        self.status = "Stopped"
        return self.status

    def get_status(self) -> str:
        return self.status

    def get_status_snapshot(self) -> Dict[str, Any]:
        return {
            "pod": self.pod_name,
            "namespace": self.namespace,
            "actor": self.actor_name,
            "status": self.status,
            "containers": list(self._container_status.values()),
        }

    def _status_loop(self):
        while not self._stop_flag:
            try:
                # 1) 拉取配置并协调（若启用 etcd）
                if self.spec_key and self._etcd is not None:
                    try:
                        data = self._etcd.get_kv(self.spec_key)
                        if isinstance(data, dict):
                            gen = int(data.get("generation", 0))
                            if self._last_generation is None or gen != self._last_generation:
                                desired_containers = data.get("containers", []) or []
                                desired_volumes = data.get("volumes", []) or []
                                # 物化声明式卷到宿主（仅支持 hostPath、ConfigMap、Secret 的最小子集）
                                self._ensure_volumes(desired_volumes)
                                # 变更摘要
                                cur_names = {c.get("name") for c in (self.containers or []) if c.get("name")}
                                des_names = {c.get("name") for c in desired_containers if c.get("name")}
                                added = sorted(list(des_names - cur_names))
                                removed = sorted(list(cur_names - des_names))
                                changed = []
                                for cname in (cur_names & des_names):
                                    cur = next((c for c in self.containers if c.get("name") == cname), {})
                                    des = next((c for c in desired_containers if c.get("name") == cname), {})
                                    if any([
                                        cur.get("image") != des.get("image"),
                                        (cur.get("args") or []) != (des.get("args") or []),
                                        (cur.get("command") or []) != (des.get("command") or []),
                                    ]):
                                        changed.append(cname)
                                _log(
                                    f"POD {self.actor_name}",
                                    f"spec change detected gen {self._last_generation} -> {gen} applied_at={data.get('applied_at')} added={added} removed={removed} changed={changed}",
                                )
                                self._reconcile_containers(desired_containers)
                                self._last_generation = gen
                    except Exception:
                        pass

                snap = self.get_status_snapshot()
                # 打印精简摘要，包含各容器状态
                summary = {
                    "pod": snap.get("pod"),
                    "ns": snap.get("namespace"),
                    "status": snap.get("status"),
                    "containers": [
                        {
                            "name": c.get("container"),
                            "status": c.get("status"),
                            "cid": c.get("container_id"),
                            "err": c.get("error"),
                        }
                        for c in snap.get("containers", [])
                    ],
                }
                _log(f"POD {self.actor_name}", json.dumps(summary, ensure_ascii=False))
            except Exception:
                pass
            time.sleep(1)

    def _ensure_volumes(self, volumes: List[Dict[str, Any]]):
        """在本节点物化卷内容，并为后续容器挂载准备 hostPath 路径。
        - 支持 ConfigMap/Secret 简化模式：从 etcd /nokube/{configmaps|secrets}/<ns>/<name> 读取数据，
          写入到 /var/lib/nokube/volumes/<ns>/<deploy>/<pod>/<volName>/ 下（pod 级唯一）。
        - 支持 hostPath 直透（确保目录存在）。
        """
        try:
            base = f"/var/lib/nokube/volumes/{self.full_path(include_namespace=True)}"
            os.makedirs(base, exist_ok=True)
            for v in volumes or []:
                vname = v.get("name")
                if not vname:
                    continue
                if self._materialized_volumes.get(vname):
                    continue
                target = os.path.join(base, _safe_name(vname))
                src: Dict[str, Any] = v or {}
                if src.get("configMap") and self._etcd is not None:
                    cm_name = (src.get("configMap") or {}).get("name")
                    if cm_name:
                        data = self._etcd.get_kv(f"/nokube/configmaps/{self.namespace}/{cm_name}") or {}
                        os.makedirs(target, exist_ok=True)
                        for k, val in (data or {}).items():
                            try:
                                with open(os.path.join(target, k), 'w', encoding='utf-8') as f:
                                    f.write(str(val))
                            except Exception:
                                pass
                        self._materialized_volumes[vname] = True
                elif src.get("secret") and self._etcd is not None:
                    sec_name = (src.get("secret") or {}).get("name")
                    if sec_name:
                        data = self._etcd.get_kv(f"/nokube/secrets/{self.namespace}/{sec_name}") or {}
                        os.makedirs(target, exist_ok=True)
                        for k, val in (data or {}).items():
                            try:
                                with open(os.path.join(target, k), 'w', encoding='utf-8') as f:
                                    f.write(str(val))
                            except Exception:
                                pass
                        self._materialized_volumes[vname] = True
                elif src.get("hostPath"):
                    host_path = (src.get("hostPath") or {}).get("path")
                    if host_path:
                        try:
                            os.makedirs(host_path, exist_ok=True)
                            self._materialized_volumes[vname] = True
                        except Exception:
                            pass
        except Exception:
            pass

    def _reconcile_containers(self, desired: List[Dict[str, Any]]):
        # 基于容器 name 做 diff
        current_by_name = {c.get("container"): c for c in self._container_status.values() if c.get("container")}
        desired_by_name = {c.get("name"): c for c in desired if c.get("name")}

        # 停掉删除/变更的容器 actor
        to_stop_names: List[str] = []
        for name, cstat in current_by_name.items():
            if name not in desired_by_name:
                to_stop_names.append(name)
            else:
                d = desired_by_name[name]
                if any([
                    cstat.get("image") != d.get("image"),
                    (cstat.get("status") != "Running"),
                ]):
                    to_stop_names.append(name)

        if to_stop_names:
            # 精确按容器名停止旧实例
            actors_to_stop: List[Any] = []
            for n in to_stop_names:
                actor = self._actor_by_container.get(n)
                if actor is not None:
                    actors_to_stop.append(actor)
            if actors_to_stop:
                try:
                    ray.get([a.stop.remote() for a in actors_to_stop], timeout=5)
                except Exception:
                    pass
            # 从记录中移除这些容器
            for n in to_stop_names:
                self._actor_by_container.pop(n, None)
                self._container_status.pop(n, None)
            # 同步列表视图
            try:
                self.container_actors = list(self._actor_by_container.values())
            except Exception:
                pass
        # 更新期望容器列表
        self.containers = desired


@ray.remote
class DeploymentActor:
    """Deployment 级 Actor：管理 Pod 子 Actor。"""

    def __init__(self, name: str, namespace: str, replicas: int, containers: List[Dict[str, Any]], volumes: Optional[List[Dict[str, Any]]] = None):
        self.name = name
        self.namespace = namespace
        self.replicas = replicas
        self.containers = containers
        self.volumes = volumes or []
        self.pods: Dict[str, Any] = {}
        self._stop_flag = False
        self._thread: Optional[threading.Thread] = None
        self._status_thread: Optional[threading.Thread] = None
        # etcd 存储 desired spec，Pod 从该 key 监听并自我协调
        self._etcd = _try_create_etcd()
        self._generation = 0
        self.spec_key = f"/nokube/deployments/{self.namespace}/{self.name}/spec"

    def start(self) -> Dict[str, Any]:
        # 开头打印当前进程可见的代理环境摘要
        try:
            proxy_items = _mask_proxy_items(os.environ)
            if proxy_items:
                _log(f"DEP {self.name}", f"proxy env: {', '.join(proxy_items)}")
        except Exception:
            pass
        # 发布初始 desired spec 到存储
        # 在启动 Pod/Container 子 Actor 前，将当前节点 name 注入进程环境，供子 Actor 解析代理环境
        try:
            # 如果外层控制器已知宿主节点名，可通过环境变量 NOKUBE_NODE_NAME 传递
            # 此处保持已有值，不覆盖
            if not os.environ.get("NOKUBE_NODE_NAME"):
                # 尝试从 etcd 的节点信息中推断（若可用）；此处简单保留已有约定，外层负责设置
                pass
        except Exception:
            pass
        self._publish_spec()
        futures = []
        from src.actor_utils import ensure_actor  # type: ignore
        ns = os.environ.get("NOKUBE_NAMESPACE", "nokube")
        for i in range(self.replicas):
            pod_name = f"{self.name}-pod-{i}"
            pod_actor_name = f"pod-{_safe_name(self.namespace)}-{_safe_name(self.name)}-{i}"
            # 幂等：pod actor 使用稳定名字，替换旧实例
            pod = ensure_actor(
                PodActor,
                pod_actor_name,
                namespace=ns,
                detached=False,
                replace_existing=True,
                ctor_args=(pod_name, self.namespace, self.containers, pod_actor_name, self.spec_key, self.name, getattr(self, 'volumes', [])),
                stop_method="stop",
                stop_timeout=10,
            )
            self.pods[pod_name] = pod
            futures.append(pod.start.remote())
        results = ray.get(futures) if futures else []
        # 启动监督线程，保持副本数与重建失败的 pod
        if not self._thread:
            self._thread = threading.Thread(target=self._supervise_loop, daemon=True)
            self._thread.start()
        if not self._status_thread:
            self._status_thread = threading.Thread(target=self._status_loop, daemon=True)
            self._status_thread.start()
        return {
            "deployment": self.name,
            "namespace": self.namespace,
            "replicas": self.replicas,
            "pods": results,
        }

    def stop(self) -> int:
        if not self.pods:
            return 0
        ray.get([p.stop.remote() for p in self.pods.values()])
        count = len(self.pods)
        self.pods.clear()
        self._stop_flag = True
        if self._thread:
            try:
                self._thread.join(timeout=2)
            except Exception:
                pass
        if self._status_thread:
            try:
                self._status_thread.join(timeout=2)
            except Exception:
                pass
        return count

    def run(self):
        """阻塞的运行接口，使 Dashboard 显示为运行中任务。"""
        if not self._thread:
            self._thread = threading.Thread(target=self._supervise_loop, daemon=True)
            self._thread.start()
        if not self._status_thread:
            self._status_thread = threading.Thread(target=self._status_loop, daemon=True)
            self._status_thread.start()
        # 阻塞直至停止
        while not self._stop_flag:
            time.sleep(5)

    def _supervise_loop(self):
        """保持 desired replicas，若 pod 死亡或异常则重建。"""
        backoff = 10.0
        while not self._stop_flag:
            try:
                # 确保副本数
                # 清理无响应的 pod，并补齐数量
                alive: Dict[str, Any] = {}
                for pod_name, actor in list(self.pods.items()):
                    try:
                        status = ray.get(actor.get_status.remote(), timeout=5)
                        # Running 或 Stopped 都保留，由 PodActor 内部做容器重启
                        alive[pod_name] = actor
                    except Exception:
                        # actor 掉线，需重建
                        pass
                self.pods = alive

                # 补齐副本
                idx = 0
                while len(self.pods) < self.replicas:
                    # 生成新的 pod
                    pod_name = f"{self.name}-pod-{idx}"
                    if pod_name in self.pods:
                        idx += 1
                        continue
                    pod_actor_name = f"pod-{_safe_name(self.namespace)}-{_safe_name(self.name)}-{idx}"
                    from src.actor_utils import ensure_actor  # type: ignore
                    ns = os.environ.get("NOKUBE_NAMESPACE", "nokube")
                    pod = ensure_actor(
                        PodActor,
                        pod_actor_name,
                        namespace=ns,
                        detached=False,
                        replace_existing=True,
                        ctor_args=(pod_name, self.namespace, self.containers, pod_actor_name, self.spec_key, self.name, getattr(self, 'volumes', [])),
                        stop_method="stop",
                        stop_timeout=10,
                    )
                    self.pods[pod_name] = pod
                    try:
                        # 异步启动，不阻塞监督循环
                        pod.start.remote()
                    except Exception:
                        pass
                    idx += 1

                backoff = 10.0
            except Exception:
                # 出错退避，避免频繁循环
                backoff = min(backoff * 2, 60.0)
            # 周期性检查
            for _ in range(int(backoff)):
                if self._stop_flag:
                    break
                time.sleep(1)

    def _status_loop(self):
        while not self._stop_flag:
            try:
                pod_summaries: List[Dict[str, Any]] = []
                for pod_name, actor in list(self.pods.items()):
                    try:
                        snap = ray.get(actor.get_status_snapshot.remote(), timeout=1)
                    except Exception:
                        snap = {"pod": pod_name, "namespace": self.namespace, "status": "Unknown"}
                    pod_summaries.append(snap)
                summary = {
                    "deployment": self.name,
                    "ns": self.namespace,
                    "replicas": self.replicas,
                    "pods": [
                        {
                            "pod": p.get("pod"),
                            "status": p.get("status"),
                            "containers": [
                                {"name": c.get("container"), "status": c.get("status")} for c in p.get("containers", [])
                            ],
                        }
                        for p in pod_summaries
                    ],
                }
                _log(f"DEP {self.name}", json.dumps(summary, ensure_ascii=False))
            except Exception:
                pass
            time.sleep(1)

    # --- Idempotent control API ---
    def set_spec(self, replicas: int, containers: List[Dict[str, Any]]):
        """更新期望副本与容器规格。"""
        self.replicas = int(replicas)
        self.containers = containers or []
        self._publish_spec()

    def snapshot(self) -> Dict[str, Any]:
        """返回当前部署快照。"""
        pod_summaries: List[Dict[str, Any]] = []
        for pod_name, actor in self.pods.items():
            try:
                pod_summaries.append(ray.get(actor.get_status_snapshot.remote(), timeout=5))
            except Exception:
                pod_summaries.append({"pod": pod_name, "namespace": self.namespace, "status": "Unknown"})
        return {
            "deployment": self.name,
            "namespace": self.namespace,
            "replicas": self.replicas,
            "pods": pod_summaries,
        }

    def reconcile(self):
        """立即协调，使当前状态尽快收敛至期望规格。
        简化实现：重建全部 Pod 以应用容器规格与副本数。
        """
        # 停止现有 pods
        try:
            if self.pods:
                try:
                    ray.get([p.stop.remote() for p in self.pods.values()], timeout=10)
                except Exception:
                    pass
        finally:
            self.pods.clear()

        # 重建期望数量的 pods
        from src.actor_utils import ensure_actor  # type: ignore
        ns = os.environ.get("NOKUBE_NAMESPACE", "nokube")
        for i in range(self.replicas):
            pod_name = f"{self.name}-pod-{i}"
            pod_actor_name = f"pod-{_safe_name(self.namespace)}-{_safe_name(self.name)}-{i}"
            pod = ensure_actor(
                PodActor,
                pod_actor_name,
                namespace=ns,
                detached=False,
                replace_existing=True,
                ctor_args=(pod_name, self.namespace, self.containers, pod_actor_name, self.spec_key),
                stop_method="stop",
                stop_timeout=10,
            )
            self.pods[pod_name] = pod
            try:
                pod.start.remote()
            except Exception:
                pass

    def _publish_spec(self):
        self._generation += 1
        if self._etcd is not None:
            try:
                # 添加 apply 时间戳，作为变更标识
                applied_at = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                payload = {"generation": self._generation, "applied_at": applied_at, "containers": self.containers, "volumes": getattr(self, 'volumes', [])}
                self._etcd.set_kv(self.spec_key, payload)
                _log(f"DEP {self.name}", f"publish spec gen={self._generation} replicas={self.replicas} containers={len(self.containers)} applied_at={applied_at}")
            except Exception:
                pass

@ray.remote
class DaemonSetActor:
    """DaemonSet Actor: Manages Pod Actors on each eligible node based on nodeSelector and nodeAffinity"""
    
    def __init__(self, name: str, namespace: str, spec: Dict[str, Any]):
        self.name = name
        self.namespace = namespace
        self.spec = spec
        self.running = False
        self.pod_actors: Dict[str, Any] = {}  # node -> pod_actor
        self._etcd = None
        
        # Parse template spec
        template_spec = spec.get("spec", {}).get("template", {}).get("spec", {})
        self.containers = template_spec.get("containers", [])
        self.volumes = template_spec.get("volumes", [])
        self.node_selector = template_spec.get("nodeSelector", {})
        self.node_affinity = template_spec.get("affinity", {}).get("nodeAffinity", {})
        
        # Initialize etcd connection
        try:
            from src.etcd_manager import EtcdManager as _EM  # type: ignore
            self._etcd = _EM()
        except Exception:
            self._etcd = None
    
    def start(self):
        """Start DaemonSet supervision"""
        try:
            if self.running:
                return {"status": "already_running"}
            
            print(f"🚀 Starting DaemonSet {self.name} in namespace {self.namespace}")
            self.running = True
            
            # Get eligible nodes
            eligible_nodes = self._get_eligible_nodes()
            print(f"📋 Eligible nodes for DaemonSet {self.name}: {eligible_nodes}")
            
            if not eligible_nodes:
                print(f"⚠️  No eligible nodes found for DaemonSet {self.name}")
                return {"status": "no_eligible_nodes"}
            
            # Start Pod Actor on each eligible node
            from src.actor_utils import ensure_actor  # type: ignore
            ray_ns = os.environ.get("NOKUBE_NAMESPACE", "nokube")
            
            started_pods = 0
            for node in eligible_nodes:
                pod_name = f"{self.name}-pod-{_safe_name(node)}"
                pod_actor_name = f"pod-{_safe_name(self.namespace)}-{_safe_name(self.name)}-node-{_safe_name(node)}"
                
                try:
                    print(f"🔧 Creating pod {pod_actor_name} on node {node}")
                    pod_actor = ensure_actor(
                        PodActor,
                        pod_actor_name,
                        namespace=ray_ns,
                        detached=True,
                        replace_existing=True,
                        ctor_args=(pod_name, self.namespace, self.containers, pod_actor_name, node, self.name, self.volumes),
                        stop_method="stop",
                        stop_timeout=10,
                    )
                    pod_actor.start.remote()
                    self.pod_actors[node] = pod_actor
                    started_pods += 1
                    print(f"✅ Started pod on node {node}: {pod_actor_name}")
                except Exception as e:
                    print(f"❌ Failed to start pod on node {node}: {e}")
                    import traceback
                    traceback.print_exc()
            
            print(f"🎯 DaemonSet {self.name} started {started_pods}/{len(eligible_nodes)} pods")
            return {"status": "started", "pods": started_pods, "total_nodes": len(eligible_nodes)}
            
        except Exception as e:
            print(f"❌ DaemonSet {self.name} start failed: {e}")
            import traceback
            traceback.print_exc()
            self.running = False
            return {"status": "failed", "error": str(e)}
    
    def stop(self):
        """Stop DaemonSet and all pods"""
        self.running = False
        
        # Stop all pod actors
        for node, pod_actor in self.pod_actors.items():
            try:
                ray.get(pod_actor.stop.remote())
                _log(f"DS {self.name}", f"Stopped pod on node {node}")
            except Exception as e:
                _log(f"DS {self.name}", f"Failed to stop pod on node {node}: {e}")
        
        self.pod_actors.clear()
        return {"status": "stopped"}
    
    def _get_eligible_nodes(self) -> List[str]:
        """Get list of eligible nodes based on nodeSelector and nodeAffinity"""
        # Get all nodes from etcd cluster metadata
        all_nodes = self._list_all_nodes()
        
        # Filter nodes based on nodeSelector and nodeAffinity
        eligible_nodes = []
        for node in all_nodes:
            if self._node_matches(node):
                eligible_nodes.append(node)
        
        return eligible_nodes
    
    def _list_all_nodes(self) -> List[str]:
        """List all available nodes from etcd cluster metadata"""
        try:
            if self._etcd:
                print(f"🔍 Querying etcd for cluster nodes...")
                clusters = self._etcd.list_clusters()
                print(f"📋 Found {len(clusters) if clusters else 0} clusters in etcd")
                
                for cluster in clusters:
                    if cluster.get('status') == 'running':
                        config = cluster.get('config', {})
                        nodes = config.get('nodes', [])
                        node_names = []
                        for node in nodes:
                            ssh_url = node.get('ssh_url', '')
                            if ssh_url:
                                # Extract hostname from ssh_url
                                host = ssh_url.split(':')[0]
                                node_names.append(host)
                        if node_names:
                            print(f"✅ Found nodes from etcd: {node_names}")
                            return node_names
                
                print("⚠️  No running clusters or nodes found in etcd")
            else:
                print("⚠️  No etcd connection available")
        except Exception as e:
            print(f"❌ Failed to query etcd for nodes: {e}")
        
        # Fallback to default node
        print("🔄 Using fallback node: default-node")
        return ["default-node"]
    
    def _node_matches(self, node_name: str) -> bool:
        """Check if node matches nodeSelector and nodeAffinity requirements"""
        # For now, simplified matching based on nodeName
        if self.node_selector:
            # Check if nodeSelector specifies nodeName or nokube/nodeName
            node_name_selector = self.node_selector.get('nodeName') or self.node_selector.get('nokube/nodeName')
            if node_name_selector and node_name_selector != node_name:
                return False
        
        # TODO: Implement more sophisticated nodeAffinity matching
        # For now, assume all nodes are eligible unless explicitly excluded
        return True


@ray.remote
class KubeControllerActor:
    """固定控制器：接收 K8s 资源，存储配置并管理 PodActor。"""

    def __init__(self, name: str = "nokube-kube-controller"):
        self.name = name
        self.pods: Dict[str, Any] = {}
        # 延迟导入判定 etcd 可用性
        try:
            from src.etcd_manager import EtcdManager as _EM  # type: ignore
            self.store_enabled = _EM is not None
        except Exception:
            self.store_enabled = False
        if self.store_enabled:
            try:
                from src.etcd_manager import EtcdManager as _EM2  # type: ignore
                self.etcd = _EM2()
            except Exception:
                self.etcd = None
                self.store_enabled = False
        else:
            self.etcd = None

    def apply(self, resources: List[Dict[str, Any]]) -> Dict[str, Any]:
        """应用一组 K8s 资源文档。"""
        configmaps = []
        secrets = []
        deployments = []
        pods = []

        for doc in resources:
            if not isinstance(doc, dict):
                continue
            kind = doc.get("kind")
            if kind == "ConfigMap":
                configmaps.append(doc)
            elif kind == "Secret":
                secrets.append(doc)
            elif kind == "Deployment":
                deployments.append(doc)
            elif kind == "Pod":
                pods.append(doc)

        # 存储 ConfigMap/Secret（如 etcd 可用）
        if self.store_enabled and self.etcd is not None:
            try:
                for cm in configmaps:
                    meta = cm.get("metadata", {})
                    ns = meta.get("namespace", "default")
                    name = meta.get("name", "")
                    self.etcd.set_kv(
                        f"/nokube/configmaps/{ns}/{name}",
                        cm.get("data", {}),
                    )
                for sec in secrets:
                    meta = sec.get("metadata", {})
                    ns = meta.get("namespace", "default")
                    name = meta.get("name", "")
                    self.etcd.set_kv(
                        f"/nokube/secrets/{ns}/{name}",
                        sec.get("data", {}),
                    )
            except Exception:
                # 存储失败忽略，不影响 Actor 生命周期
                pass

        started = []

        # 将 Deployment 归约为 Pod 级别（每副本一个 PodActor）
        for d in deployments:
            meta = d.get("metadata", {})
            ns = meta.get("namespace", "default")
            spec = d.get("spec", {})
            replicas = spec.get("replicas", 1)
            tmpl_spec = spec.get("template", {}).get("spec", {})
            containers = tmpl_spec.get("containers", [])
            base_name = meta.get("name", "deployment")
            for i in range(replicas):
                pod_name = f"{base_name}-pod-{i}-{_short_id()}"
                pod_actor_name = f"pod-{_safe_name(ns)}-{_safe_name(base_name)}-{i}-{_short_id()}"
                # 从 Deployment 传递模板 volumes
                volumes = tmpl_spec.get("volumes", []) or []
                actor = PodActor.options(name=pod_actor_name).remote(pod_name, ns, containers, pod_actor_name, None, base_name, volumes)
                self.pods[pod_name] = actor
                started.append(actor.start.remote())

        # 直接处理 Pod 资源
        for p in pods:
            meta = p.get("metadata", {})
            ns = meta.get("namespace", "default")
            spec = p.get("spec", {})
            containers = spec.get("containers", [])
            base_name = meta.get("name", "pod")
            pod_name = f"{base_name}-{_short_id()}"
            pod_actor_name = f"pod-{_safe_name(ns)}-{_safe_name(base_name)}-{_short_id()}"
            # 直接 Pod 资源也传递其 volumes
            volumes = spec.get("volumes", []) or []
            actor = PodActor.options(name=pod_actor_name).remote(pod_name, ns, containers, pod_actor_name, None, None, volumes)
            self.pods[pod_name] = actor
            started.append(actor.start.remote())

        results = ray.get(started) if started else []

        # Ensure system config: one PodActor per node (best-effort), no user YAML required
        try:
            print("🔧 Starting system config ensure...")
            etcd = _try_create_etcd()
            node_names: List[str] = []
            if etcd is not None:
                print("✅ Etcd connection available")
                try:
                    clusters = etcd.list_clusters() or []
                    print(f"📋 Found {len(clusters)} clusters")
                    for c in clusters:
                        cfg = (c or {}).get('config') or {}
                        for n in (cfg.get('nodes') or []):
                            nm = (n or {}).get('name')
                            if nm and nm not in node_names:
                                node_names.append(nm)
                except Exception as e:
                    print(f"⚠️  Failed to get nodes from etcd: {e}")
            else:
                print("⚠️  No etcd connection")
            if not node_names:
                node_names = ["default-node"]
            print(f"🎯 Target nodes: {node_names}")

            node_cfg_container = {
                "name": "node-config",
                "image": "python:3.10",
                "securityContext": {"privileged": True},
                "command": ["/bin/bash", "-lc"],
                "args": [
                    "echo CFG | node=$(hostname) http=$http_proxy https=$https_proxy no=$no_proxy; "
                    "while true; do "
                    "echo '[CONFIG]' $(date) 'proxy env:' http_proxy=$http_proxy https_proxy=$https_proxy no_proxy=$no_proxy; "
                    "sleep 300; "
                    "done"
                ],
                "volumeMounts": [{"name": "docker-etc", "mountPath": "/host/etc/docker"}],
            }
            node_cfg_vols = [{"name": "docker-etc", "persistentVolumeClaim": {"localPath": "/etc/docker"}}]

            from src.actor_utils import ensure_actor  # type: ignore
            ns_sys = "nokube-system"
            ds_name = "sys-nokube-node-config"
            ray_ns = os.environ.get("NOKUBE_NAMESPACE", "nokube")
            
            # Create system config DaemonSet spec
            ds_spec = {
                "metadata": {"name": ds_name, "namespace": ns_sys},
                "spec": {
                    "template": {
                        "spec": {
                            "containers": [node_cfg_container],
                            "volumes": node_cfg_vols
                        }
                    }
                }
            }
            
            # Create DaemonSetActor to manage system config
            ds_actor_name = f"daemonset-{_safe_name(ns_sys)}-{_safe_name(ds_name)}"
            ds_actor = ensure_actor(
                DaemonSetActor,
                ds_actor_name,
                namespace=ray_ns,
                detached=True,
                replace_existing=True,
                ctor_args=(ds_name, ns_sys, ds_spec),
                stop_method="stop",
                stop_timeout=10,
            )
            try:
                ds_actor.start.remote()
                print(f"✅ Started system config DaemonSet: {ds_actor_name}")
            except Exception as e:
                print(f"⚠️  Failed to start system config DaemonSet {ds_actor_name}: {e}")
        except Exception:
            pass
        return {
            "controller": self.name,
            "pods_started": len(results),
            "details": results,
            "configmaps": len(configmaps),
            "secrets": len(secrets),
        }

    def list_pods(self) -> List[str]:
        return list(self.pods.keys())

    def stop_all(self) -> int:
        futures = []
        for name, actor in self.pods.items():
            futures.append(actor.stop.remote())
        if futures:
            ray.get(futures)
        count = len(self.pods)
        self.pods.clear()
        return count


