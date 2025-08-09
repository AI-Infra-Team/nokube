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


def _log(prefix: str, message: str):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {prefix} | {message}", flush=True)


def _try_create_etcd():
    try:
        from src.etcd_manager import EtcdManager  # type: ignore
        return EtcdManager()
    except Exception:
        return None


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

    def start(self, attempts: int = 5, backoff_seconds: float = 3.0) -> Dict[str, Any]:
        def _build_env_dict() -> Dict[str, str]:
            env_dict: Dict[str, str] = {}
            for env in self.container.get("env", []) or []:
                name = env.get("name")
                value = env.get("value")
                if name is not None and value is not None:
                    env_dict[name] = value
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

            run_args: List[str] = ["sudo", "-n", "docker", "run", "-d"]
            for k, v in env_dict.items():
                run_args.extend(["-e", f"{k}={v}"])
            run_args.extend(port_args)
            run_args.append(image)
            run_args.extend(final_cmd)

            proc = subprocess.run(run_args, capture_output=True, text=True)
            if proc.returncode == 0:
                self.container_id = proc.stdout.strip()
                self.status = "Running"
                return {
                    "pod": self.pod_name,
                    "namespace": self.namespace,
                    "container": self.container.get("name"),
                    "actor": self.actor_name,
                    "status": self.status,
                    "image": image,
                    "container_id": self.container_id,
                }
            else:
                # 回退到 docker SDK（若 CLI 不可用）
                try:
                    import docker
                    client = docker.from_env()
                    run_cfg = {
                        "image": image,
                        "detach": True,
                        "environment": env_dict or None,
                        "ports": None,
                    }
                    # 将端口转为 SDK 形式，如 {"80/tcp": None}
                    ports_dict: Dict[str, Optional[int]] = {}
                    for p in self.container.get("ports", []) or []:
                        cport = p.get("containerPort")
                        proto = (p.get("protocol") or "TCP").lower()
                        if cport:
                            ports_dict[f"{cport}/{proto}"] = None
                    run_cfg["ports"] = ports_dict or None
                    if final_cmd:
                        run_cfg["command"] = final_cmd
                    c = client.containers.run(**run_cfg)
                    self.container_id = c.id
                    self.status = "Running"
                    return {
                        "pod": self.pod_name,
                        "namespace": self.namespace,
                        "container": self.container.get("name"),
                        "actor": self.actor_name,
                        "status": self.status,
                        "image": image,
                        "container_id": self.container_id,
                    }
                except Exception as e2:
                    err = proc.stderr.strip() or str(e2)
                    raise RuntimeError(err)

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
            if self.container_id:
                # 尝试 sudo docker rm -f
                proc = subprocess.run(["sudo", "-n", "docker", "rm", "-f", self.container_id], capture_output=True, text=True)
                if proc.returncode != 0:
                    # 回退到 SDK
                    try:
                        import docker
                        client = docker.from_env()
                        c = client.containers.get(self.container_id)
                        c.stop()
                        c.remove()
                    except Exception:
                        pass
        except BaseException:
            pass
        self.status = "Stopped"
        return self.status

    def get_status(self) -> str:
        return self.status


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
        while not self._stop_flag:
            # 每次尝试使用新的 actor 名，避免命名冲突
            c_actor_name = _compose_name("ctr", [self.namespace, self.pod_name, c_name, _short_id()])
            try:
                try:
                    actor = ContainerActor.options(name=c_actor_name).remote(
                        self.pod_name, self.namespace, container_spec, c_actor_name
                    )
                except Exception:
                    actor = ContainerActor.options(name=None).remote(
                        self.pod_name, self.namespace, container_spec, c_actor_name
                    )
                # 单次尝试，失败则由本监督循环重试
                result = ray.get(actor.start.remote(attempts=1, backoff_seconds=1.0))
                self._container_status[c_name] = result
                if result.get("status") == "Running":
                    # 成功后维持一段时间再复检（轻量心跳）
                    self.container_actors.append(actor)
                    backoff = 10.0
                    sleep_secs = 15.0
                else:
                    # 失败则指数退避，保持 Pod 存活
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

            # 睡眠后继续尝试，不退出
            for _ in range(int(sleep_secs)):
                if self._stop_flag:
                    break
                time.sleep(1)

    def start(self) -> Dict[str, Any]:
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
        if self.container_actors:
            try:
                ray.get([a.stop.remote() for a in self.container_actors])
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
        to_stop: List[Any] = []
        for name, cstat in current_by_name.items():
            if name not in desired_by_name:
                # 删除
                actor = next((a for a in self.container_actors if isinstance(a, ray.actor.ActorHandle)), None)
                if actor:
                    to_stop.append(actor)
            else:
                # 比较关键字段
                d = desired_by_name[name]
                if any([
                    cstat.get("image") != d.get("image"),
                ]):
                    actor = next((a for a in self.container_actors if isinstance(a, ray.actor.ActorHandle)), None)
                    if actor:
                        to_stop.append(actor)
        if to_stop:
            try:
                ray.get([a.stop.remote() for a in to_stop], timeout=5)
            except Exception:
                pass
            # 清空本地记录，待监督线程按新 spec 重建
            self.container_actors = []
            self._container_status = {}
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
        # 发布初始 desired spec 到存储
        self._publish_spec()
        futures = []
        for i in range(self.replicas):
            pod_name = f"{self.name}-pod-{i}"
            pod_actor_name = f"pod-{_safe_name(self.namespace)}-{_safe_name(self.name)}-{i}-{_short_id()}"
            pod = PodActor.options(name=pod_actor_name).remote(
                pod_name, self.namespace, self.containers, pod_actor_name, self.spec_key
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
                    pod_actor_name = f"pod-{_safe_name(self.namespace)}-{_safe_name(self.name)}-{idx}-{_short_id()}"
                    pod = PodActor.options(name=pod_actor_name).remote(
                        pod_name, self.namespace, self.containers, pod_actor_name
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
        for i in range(self.replicas):
            pod_name = f"{self.name}-pod-{i}"
            pod_actor_name = f"pod-{_safe_name(self.namespace)}-{_safe_name(self.name)}-{i}-{_short_id()}"
            pod = PodActor.options(name=pod_actor_name).remote(
                pod_name, self.namespace, self.containers, pod_actor_name, self.spec_key
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
        self.store_enabled = EtcdManager is not None
        if self.store_enabled:
            try:
                self.etcd = EtcdManager()
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


