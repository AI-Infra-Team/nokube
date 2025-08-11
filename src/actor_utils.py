#!/usr/bin/env python3
"""
通用 Actor 管理工具：提供分布式幂等的创建/替换，避免重复代码。

主要能力：
- stop_actor_if_possible: 调用远程 stop 并吞掉异常
- kill_actor_safely: ray.kill，no_restart=True，吞掉异常
- stop_and_kill_actor: 组合式停止与杀死
- ensure_actor: 幂等获取/替换具名 Actor（可选 detached/namespace）

适用：Deployment/Pod/Container/NodeConfig 等具名 Actor 启动路径。
"""

from typing import Any, Dict, Optional, Tuple
import os
import ray


def get_default_namespace() -> str:
    """获取默认命名空间，可由 NOKUBE_NAMESPACE 覆盖。"""
    return os.environ.get("NOKUBE_NAMESPACE", "nokube")


def stop_actor_if_possible(actor: Any, stop_method: str = "stop", timeout: int = 10) -> bool:
    try:
        if hasattr(actor, stop_method):
            try:
                ray.get(getattr(actor, stop_method).remote(), timeout=timeout)
            except Exception:
                # 忽略 stop 失败
                pass
        return True
    except Exception:
        return False


def kill_actor_safely(actor: Any, no_restart: bool = True) -> bool:
    try:
        ray.kill(actor, no_restart=no_restart)
        return True
    except Exception:
        return False


def stop_and_kill_actor(actor: Any, stop_method: str = "stop", timeout: int = 10, no_restart: bool = True) -> None:
    stop_actor_if_possible(actor, stop_method=stop_method, timeout=timeout)
    kill_actor_safely(actor, no_restart=no_restart)


def get_named_actor(name: str, namespace: Optional[str] = None) -> Optional[Any]:
    try:
        if namespace is None:
            return ray.get_actor(name)
        return ray.get_actor(name, namespace=namespace)
    except Exception:
        return None


def ensure_actor(
    actor_cls: Any,
    name: str,
    *,
    namespace: Optional[str] = None,
    detached: bool = False,
    replace_existing: bool = True,
    scheduling_strategy: Any = None,
    options_overrides: Optional[Dict[str, Any]] = None,
    ctor_args: Tuple[Any, ...] = (),
    ctor_kwargs: Optional[Dict[str, Any]] = None,
    stop_method: str = "stop",
    stop_timeout: int = 10,
) -> Any:
    """分布式幂等确保具名 Actor 存在。

    - 若已存在且 replace_existing=True，则先调用 stop 再 kill，然后重建
    - 若已存在且 replace_existing=False，则直接复用
    - 支持 detached/namespace/scheduling_strategy
    - 返回 ActorHandle
    """
    if ctor_kwargs is None:
        ctor_kwargs = {}
    ns = namespace
    opts: Dict[str, Any] = {"name": name}
    if detached:
        opts["lifetime"] = "detached"
        # 对 detached 建议总是明确 namespace
        opts["namespace"] = ns or get_default_namespace()
    elif ns is not None:
        # 非 detached 也允许显式命名空间
        opts["namespace"] = ns
    if scheduling_strategy is not None:
        opts["scheduling_strategy"] = scheduling_strategy
    if options_overrides:
        opts.update(options_overrides)

    # 查找现有同名 Actor
    existing = get_named_actor(name, namespace=opts.get("namespace"))
    if existing is not None:
        if replace_existing:
            stop_and_kill_actor(existing, stop_method=stop_method, timeout=stop_timeout, no_restart=True)
        else:
            return existing

    # 创建新的 Actor
    try:
        actor = actor_cls.options(**opts).remote(*ctor_args, **ctor_kwargs)
        return actor
    except Exception:
        # 若具名创建失败，尝试匿名降级
        try:
            opts.pop("name", None)
            actor = actor_cls.options(**opts).remote(*ctor_args, **ctor_kwargs)
            return actor
        except Exception as e:
            raise e


