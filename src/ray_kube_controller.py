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

    def __init__(self, pod_name: str, namespace: str, container: Dict[str, Any], actor_name: str):
        self.pod_name = pod_name
        self.namespace = namespace
        self.container = container
        self.actor_name = actor_name
        self.status = "Pending"
        self.container_id: Optional[str] = None
        self.container_name: str = _safe_name(actor_name)
        self._proc: Optional[subprocess.Popen] = None
        self._io_thread: Optional[threading.Thread] = None
        self._stop_stream: bool = False

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
            final_cmd = _build_final_cmd()

            run_args: List[str] = _sudo_prefix() + ["docker", "run", "--rm", "--name", self.container_name]
            for k, v in env_dict.items():
                run_args.extend(["-e", f"{k}={v}"])
            run_args.extend(port_args)
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
        self._etcd = _try_create_etcd()
        self._last_generation: Optional[int] = None

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
                    ctor_args=(self.pod_name, self.namespace, latest_spec, c_actor_name),
                    stop_method="stop",
                    stop_timeout=10,
                )
                result = ray.get(actor.start.remote(attempts=1, backoff_seconds=1.0))
                self._container_status[c_name] = result
                if result.get("status") == "Running":
                    # 记录为该容器的唯一实例
                    self._actor_by_container[c_name] = actor
                    if actor not in self.container_actors:
                        self.container_actors.append(actor)
                    backoff = 10.0
                    sleep_secs = 15.0
                else:
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

    def __init__(self, name: str, namespace: str, replicas: int, containers: List[Dict[str, Any]]):
        self.name = name
        self.namespace = namespace
        self.replicas = replicas
        self.containers = containers
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
                ctor_args=(pod_name, self.namespace, self.containers, pod_actor_name, self.spec_key),
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
                payload = {"generation": self._generation, "applied_at": applied_at, "containers": self.containers}
                self._etcd.set_kv(self.spec_key, payload)
                _log(f"DEP {self.name}", f"publish spec gen={self._generation} replicas={self.replicas} containers={len(self.containers)} applied_at={applied_at}")
            except Exception:
                pass

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
                actor = PodActor.options(name=pod_actor_name).remote(pod_name, ns, containers, pod_actor_name)
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
            actor = PodActor.options(name=pod_actor_name).remote(pod_name, ns, containers, pod_actor_name)
            self.pods[pod_name] = actor
            started.append(actor.start.remote())

        results = ray.get(started) if started else []
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


